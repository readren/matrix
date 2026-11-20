package readren.nexus
package cluster.channel

import cluster.channel.SequentialTransmitter.*
import cluster.misc.RingLength
import cluster.serialization.{ProtocolVersion, Serializer}

import java.lang.invoke.VarHandle
import java.nio.channels.AsynchronousSocketChannel
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.{ConcurrentLinkedQueue, RejectedExecutionException, TimeUnit}
import scala.util.control.NonFatal

/** Companion object of [[SequentialTransmitter]] exporting shared protocol definitions from [[TransmissionProtocol]] and defining internal coordination state helpers. */
object SequentialTransmitter {
	export TransmissionProtocol.*

	/** Maximum number of unserialized messages allowed in [[SequentialTransmitter.transmissionQueue]] (`2^28 - 1 = 268,435,455`), and bitmask isolating bits `0..27` of [[CoordinationState]]. */
	private inline val MsgQueueMaxLength = 0x0fff_ffff

	/** Bitmask isolating bits `28..30` of [[CoordinationState]], which encode the [[StageState]] (leaving bit `31` unused `0`). */
	private inline val StageStateMask = 7 << 28

	/** Encodes the mutually exclusive execution state stored in bits `28..30` of [[CoordinationState]]. */
	private[SequentialTransmitter] opaque type StageState = Int

	private object StageState {
		/** Neither transmission nor pruning is active (`0 << 28`), and `queuedCount` is always `0`. */
		val Idle: StageState = 0 << 28

		/** An underlying [[Transmitter]] transmission is active (`1 << 28`), and `queuedCount >= 0`. */
		val Transmitting: StageState = 1 << 28

		/** A [[SequentialTransmitter.pruneTo]] call or idle-transition prune owns the underlying [[Transmitter]] exclusively (`2 << 28`), and `queuedCount >= 0`. */
		val Pruning: StageState = 2 << 28
	}

	/** Bit-packed 31-bit coordination state stored in [[SequentialTransmitter.coordinationState]]: bits `0..27` hold `queuedCount`, bits `28..30` hold [[StageState]], and bit `31` remains `0`. */
	private[SequentialTransmitter] opaque type CoordinationState = Int

	private object CoordinationState {
		val Idle: CoordinationState = StageState.Idle
		val Pruning: CoordinationState = StageState.Pruning

		inline def fromInt(raw: Int): CoordinationState = {
			assert(raw >= 0 && (raw & StageStateMask) <= StageState.Pruning, s"Invalid CoordinationState bits: $raw")
			raw
		}

		extension (state: CoordinationState) {
			inline def toInt: Int = state
			inline def stageState: StageState = state & StageStateMask
			inline def queuedCount: Int = state & MsgQueueMaxLength
			inline def withStageState(next: StageState): CoordinationState = (state & ~StageStateMask) | next
			inline def withIncrementedQueue: CoordinationState = {
				assert(queuedCount < MsgQueueMaxLength, "CoordinationState queuedCount overflow")
				state + 1
			}
			inline def withDecrementedQueue: CoordinationState = {
				assert(queuedCount > 0, "CoordinationState queuedCount underflow")
				state - 1
			}
		}
	}

	extension (atomic: AtomicInteger) {
		inline def getCoordinationState: CoordinationState = CoordinationState.fromInt(atomic.get())
		inline def compareAndSetCoordinationState(expected: CoordinationState, update: CoordinationState): Boolean =
			atomic.compareAndSet(expected, update)
	}

	/** Trampoline handoff state between [[SequentialTransmitter.drainTransmissionLoop]] and [[SequentialTransmitter.completionAdapter]]: no active `transmitWithAttachment` call is on the stack. */
	private inline val HANDOFF_IDLE = 0
	/** Trampoline handoff state: a thread is currently inside `transmitter.transmitWithAttachment`. */
	private inline val HANDOFF_CALLING_TRANSMIT = 1
	/** Trampoline handoff state: `completionAdapter` completed before `transmitter.transmitWithAttachment` returned and staged [[SequentialTransmitter.pendingCompletionReport]]. */
	private inline val HANDOFF_CALLBACK_COMPLETED = 2

	/** Shared no-op function reference used as the default/reset value of [[SequentialTransmitter.onIdleHandler]]. */
	private val noOpIdle: () => Unit = () => ()

	/** Shared zero-allocation callback adapter used by [[SequentialTransmitter.transmit]] to forward the single-argument completion callback through [[SequentialTransmitter.transmitWithAttachment]] without allocating a wrapper closure per call. */
	private val invokeWithoutAttachment: (Report, Report => Unit) => Unit = (report, onComplete) => onComplete(report)

	/** Immutable envelope holding a queued message submission and its per-message transmission parameters in [[SequentialTransmitter.transmissionQueue]]. */
	private final class QueuedSubmission(
		val message: Any,
		val msgVersion: ProtocolVersion,
		val timeout: Long,
		val timeUnit: TimeUnit,
		val attachment: Any,
		val onComplete: (Report, Any) => Unit,
		val serializer: Serializer[Any]
	)
}

/** A thread-safe wrapper around [[Transmitter]] that allows multiple messages to be queued for sequential transmission in strict FIFO order.
 *
 * Uses a lock-free atomic [[SequentialTransmitter.CoordinationState]] cell to coordinate exclusive ownership of the underlying single-flight [[Transmitter]] across message submissions, completion callbacks, and quiescent buffer pruning, while preserving per-message protocol versions, timeouts, attachments, and serializers and delivering completion callbacks in strict FIFO order without stack recursion.
 *
 * @param channel The underlying channel for transmission.
 * @param context The transmission diagnostic context.
 * @param capacityOfBufferInRing The capacity of each circular storage buffer.
 * @param ringMaxLength The maximum number of buffers allowed in circular storage.
 * @param ringMinLengthOnIdle The minimum number of buffers retained on idle transitions.
 * @param streamTransferBufferCapacity The capacity of the direct buffer used to transfer streaming chunks.
 * @param maxGatheringBuffers The maximum number of [[java.nio.ByteBuffer]] entries submitted per gathering socket write call.
 * @tparam M The type of messages to transmit. */
class SequentialTransmitter[M](
	channel: AsynchronousSocketChannel,
	val context: Context,
	capacityOfBufferInRing: Int = DEFAULT_CAPACITY_FOR_BUFFERS_IN_RING,
	ringMaxLength: RingLength = DEFAULT_RING_MAX_LENGTH,
	val ringMinLengthOnIdle: RingLength = DEFAULT_RING_MIN_LENGTH_ON_IDLE,
	val streamTransferBufferCapacity: Int = DEFAULT_STREAM_TRANSFER_BUFFER_CAPACITY,
	val maxGatheringBuffers: Int = DEFAULT_MAX_GATHERING_BUFFERS
) {
	private val transmitter = new Transmitter(channel, context, capacityOfBufferInRing, ringMaxLength, streamTransferBufferCapacity, maxGatheringBuffers)

	/** Lock-free atomic [[SequentialTransmitter.CoordinationState]] packing [[SequentialTransmitter.StageState]] and `queuedCount` across caller threads, completion threads, and [[pruneTo]]. */
	private val coordinationState: AtomicInteger = new AtomicInteger(CoordinationState.Idle.toInt)

	/** Lock-free FIFO queue holding queued message submissions while [[transmitter]] is busy or pruning. */
	private val transmissionQueue: ConcurrentLinkedQueue[QueuedSubmission] = new ConcurrentLinkedQueue()

	/** Atomic holder for the composite callback invoked once when [[coordinationState]] transitions to [[SequentialTransmitter.CoordinationState.Idle]]. */
	private val onIdleHandler: AtomicReference[() => Unit] = new AtomicReference(noOpIdle)

	/** Coordinates inline or concurrent pre-return completion of `transmitter.transmitWithAttachment` with [[drainTransmissionLoop]] to guarantee $O(1)$ stack depth. */
	private val transmitHandoffState: AtomicInteger = new AtomicInteger(HANDOFF_IDLE)

	// Active single-flight metadata owned by the thread executing `drainTransmissionLoop` / `advanceAfterCompletion` while `StageState.Transmitting` holds.
	private var activeMessage: Any = null
	private var activeMsgVersion: ProtocolVersion = ProtocolVersion.NOT_SPECIFIED
	private var activeTimeout: Long = 0L
	private var activeTimeUnit: TimeUnit = TimeUnit.SECONDS
	private var activeAttachment: Any = null
	private var activeOnComplete: (Report, Any) => Unit = null
	private var activeSerializer: Serializer[Any] = null
	@volatile private var pendingCompletionReport: Report | Null = null

	/** Shared zero-allocation completion adapter passed to [[Transmitter.transmitWithAttachment]]. */
	private val completionAdapter: (Report, Any) => Unit = (report, _) => {
		pendingCompletionReport = report
		if transmitHandoffState.compareAndExchange(HANDOFF_CALLING_TRANSMIT, HANDOFF_CALLBACK_COMPLETED) == HANDOFF_IDLE then {
			val nextSubmission = advanceAfterCompletion()
			if nextSubmission ne null then drainTransmissionLoop(nextSubmission.nn)
		}
	}

	/** Enqueues or immediately starts transmitting a message through the underlying [[Transmitter]].
	 * @param message The message to transmit.
	 * @param msgVersion The protocol version for the message.
	 * @param timeout The transmission timeout.
	 * @param timeUnit The timeout unit.
	 * @param onTransmissionComplete Callback invoked in FIFO submission order when transmission completes.
	 * @param serializer The serializer for the message type.
	 * @throws java.util.concurrent.RejectedExecutionException if the transmitter is busy and the wait queue has reached [[SequentialTransmitter.MsgQueueMaxLength]]. */
	def transmit(message: M, msgVersion: ProtocolVersion, timeout: Long = 1, timeUnit: TimeUnit = TimeUnit.SECONDS)(onTransmissionComplete: Report => Unit)(using serializer: Serializer[M]): Unit = {
		transmitWithAttachment[Report => Unit](message, msgVersion, onTransmissionComplete, timeout, timeUnit)(invokeWithoutAttachment)
	}

	/** Enqueues or immediately starts transmitting a message with a user-supplied attachment passed through to the completion callback.
	 * @param message The message to transmit.
	 * @param msgVersion The protocol version for the message.
	 * @param attachment An arbitrary attachment passed through to `onTransmissionComplete`.
	 * @param timeout The transmission timeout.
	 * @param timeUnit The timeout unit.
	 * @param onTransmissionComplete Callback invoked in FIFO submission order with a [[SequentialTransmitter.Report]] and `attachment`.
	 * @param serializer The serializer for the message type.
	 * @tparam A The type of the attachment passed through.
	 * @throws java.util.concurrent.RejectedExecutionException if the transmitter is busy and the wait queue has reached [[SequentialTransmitter.MsgQueueMaxLength]]. */
	def transmitWithAttachment[A](
		message: M,
		msgVersion: ProtocolVersion,
		attachment: A,
		timeout: Long = 1,
		timeUnit: TimeUnit = TimeUnit.SECONDS
	)(
		onTransmissionComplete: (Report, A) => Unit
	)(using serializer: Serializer[M]): Unit = {
		var wasStateChangedConcurrently = true
		while wasStateChangedConcurrently do {
			val current = coordinationState.getCoordinationState
			val stage = current.stageState
			if stage == StageState.Idle then {
				assert(current.queuedCount == 0)
				if coordinationState.compareAndSetCoordinationState(current, current.withStageState(StageState.Transmitting)) then {
					wasStateChangedConcurrently = false
					activeMessage = message
					activeMsgVersion = msgVersion
					activeTimeout = timeout
					activeTimeUnit = timeUnit
					activeAttachment = attachment
					activeOnComplete = onTransmissionComplete.asInstanceOf[(Report, Any) => Unit]
					activeSerializer = serializer.asInstanceOf[Serializer[Any]]
					drainActiveTransmissionLoop()
				}
			} else if current.queuedCount < MsgQueueMaxLength then {
				if coordinationState.compareAndSetCoordinationState(current, current.withIncrementedQueue) then {
					wasStateChangedConcurrently = false
					transmissionQueue.add(
						new QueuedSubmission(
							message,
							msgVersion,
							timeout,
							timeUnit,
							attachment,
							onTransmissionComplete.asInstanceOf[(Report, Any) => Unit],
							serializer.asInstanceOf[Serializer[Any]]
						)
					)
				}
			} else throw new RejectedExecutionException(s"Transmission queue capacity ($MsgQueueMaxLength) exceeded")
		}
	}

	/** Prunes unused allocated buffers in the underlying [[Transmitter]] down to `targetLength` when quiescent (`Idle`).
	 * @param targetLength The desired target number of buffers to retain.
	 * @return The number of buffers retained after pruning (or current [[ringLength]] if a transmission or pruning pass is active). */
	def pruneTo(targetLength: RingLength): RingLength = {
		if coordinationState.compareAndSetCoordinationState(CoordinationState.Idle, CoordinationState.Pruning) then {
			val retained = transmitter.pruneTo(targetLength)
			releasePruningOwnership()
			retained
		} else {
			ringLength
		}
	}

	/** The total number of buffers currently allocated in the underlying [[Transmitter]] circular storage. */
	def ringLength: RingLength = transmitter.ringLength

	/** Programs an idle callback to be invoked once when no transmission is in progress and the queue is empty.
	 * @param onIdle The handler to execute when idle. */
	def triggerOnIdle(onIdle: () => Unit): Unit = {
		onIdleHandler.updateAndGet { prev =>
			if prev eq noOpIdle then onIdle
			else () => {
				prev()
				onIdle()
			}
		}
		if coordinationState.getCoordinationState == CoordinationState.Idle then onIdleHandler.getAndSet(noOpIdle)()
	}

	/** Loads `submission` into the active single-flight slots and runs [[drainActiveTransmissionLoop]]. */
	private def drainTransmissionLoop(submission: QueuedSubmission): Unit = {
		activeMessage = submission.message
		activeMsgVersion = submission.msgVersion
		activeTimeout = submission.timeout
		activeTimeUnit = submission.timeUnit
		activeAttachment = submission.attachment
		activeOnComplete = submission.onComplete
		activeSerializer = submission.serializer
		drainActiveTransmissionLoop()
	}

	/** Iteratively dispatches the active submission and any synchronously completing queued submissions in $O(1)$ stack depth. */
	private def drainActiveTransmissionLoop(): Unit = {
		var continueLoop = true
		while continueLoop do {
			continueLoop = false
			transmitHandoffState.set(HANDOFF_CALLING_TRANSMIT)
			transmitter.transmitWithAttachment(
				activeMessage,
				activeMsgVersion,
				activeAttachment,
				activeTimeout,
				activeTimeUnit
			)(completionAdapter)(using activeSerializer)
			if !transmitHandoffState.compareAndSet(HANDOFF_CALLING_TRANSMIT, HANDOFF_IDLE) then {
				transmitHandoffState.set(HANDOFF_IDLE)
				val nextSubmission = advanceAfterCompletion()
				if nextSubmission ne null then {
					val s = nextSubmission.nn
					activeMessage = s.message
					activeMsgVersion = s.msgVersion
					activeTimeout = s.timeout
					activeTimeUnit = s.timeUnit
					activeAttachment = s.attachment
					activeOnComplete = s.onComplete
					activeSerializer = s.serializer
					continueLoop = true
				}
			}
		}
	}

	/** Delivers the completed message's callback in FIFO order and then claims either the next queued submission or quiescent pruning and idle notification.
	 * @return The next [[SequentialTransmitter.QueuedSubmission]] to transmit, or `null` if the transmitter transitioned to idle (or handed off to an asynchronous continuation). */
	private def advanceAfterCompletion(): QueuedSubmission | Null = {
		val completedReport = pendingCompletionReport.nn
		val completedAttachment = activeAttachment
		val completedOnComplete = activeOnComplete
		pendingCompletionReport = null
		activeMessage = null
		activeMsgVersion = ProtocolVersion.NOT_SPECIFIED
		activeAttachment = null
		activeOnComplete = null
		activeSerializer = null

		var nextSubmission: QueuedSubmission | Null = null
		var callbackError: Throwable | Null = null
		try {
			completedOnComplete(completedReport, completedAttachment)
		} catch {
			case NonFatal(e) => callbackError = e
		}

		var transitionedToIdle = false
		var done = false
		while !done do {
			val current = coordinationState.getCoordinationState
			assert(current.stageState == StageState.Transmitting)
			if current.queuedCount > 0 then {
				if coordinationState.compareAndSetCoordinationState(current, current.withDecrementedQueue) then {
					nextSubmission = pollQueuedSubmission()
					done = true
				}
			} else if coordinationState.compareAndSetCoordinationState(current, CoordinationState.Pruning) then {
				transmitter.pruneTo(ringMinLengthOnIdle)
				var released = false
				while !released do {
					val curPruning = coordinationState.getCoordinationState
					assert(curPruning.stageState == StageState.Pruning)
					if curPruning.queuedCount == 0 then {
						if coordinationState.compareAndSetCoordinationState(curPruning, CoordinationState.Idle) then {
							transitionedToIdle = true
							released = true
						}
					} else if coordinationState.compareAndSetCoordinationState(curPruning, curPruning.withDecrementedQueue.withStageState(StageState.Transmitting)) then {
						nextSubmission = pollQueuedSubmission()
						released = true
					}
				}
				done = true
			}
		}
		if transitionedToIdle then onIdleHandler.getAndSet(noOpIdle)()
		if callbackError ne null then {
			if nextSubmission ne null then drainTransmissionLoop(nextSubmission.nn)
			throw callbackError.nn
		}
		nextSubmission
	}

	/** Releases [[StageState.Pruning]] after [[pruneTo]] completes, returning to [[CoordinationState.Idle]] or draining any submission enqueued concurrently during pruning. */
	private def releasePruningOwnership(): Unit = {
		var nextSubmission: QueuedSubmission | Null = null
		var becameIdle = false
		var released = false
		while !released do {
			val current = coordinationState.getCoordinationState
			assert(current.stageState == StageState.Pruning)
			if current.queuedCount == 0 then {
				if coordinationState.compareAndSetCoordinationState(current, CoordinationState.Idle) then {
					becameIdle = true
					released = true
				}
			} else if coordinationState.compareAndSetCoordinationState(current, current.withDecrementedQueue.withStageState(StageState.Transmitting)) then {
				nextSubmission = pollQueuedSubmission()
				released = true
			}
		}
		if becameIdle then onIdleHandler.getAndSet(noOpIdle)()
		if nextSubmission ne null then drainTransmissionLoop(nextSubmission.nn)
	}

	/** Polls the next [[SequentialTransmitter.QueuedSubmission]] after decrementing `queuedCount` in [[coordinationState]]. */
	private def pollQueuedSubmission(): QueuedSubmission = {
		var next = transmissionQueue.poll()
		while next eq null do {
			VarHandle.fullFence()
			next = transmissionQueue.poll()
		}
		next
	}
}
