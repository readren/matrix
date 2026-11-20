package readren.nexus
package cluster.channel

import cluster.channel.PipelinedTransmitter.*
import cluster.misc.{DualEndedCircularStorage, RingLength, VLQ}
import cluster.serialization.Serializer.{SerializationException, Writer}
import cluster.serialization.{ProtocolVersion, RecyclableIdentityIntMap, Serializer}

import readren.common.ByteSource

import java.lang.invoke.VarHandle
import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousSocketChannel, CompletionHandler}
import java.util as util
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.{ConcurrentLinkedQueue, RejectedExecutionException, TimeUnit}
import scala.util.control.NonFatal

/** Companion object of [[PipelinedTransmitter]] exporting shared protocol definitions from [[TransmissionProtocol]] and defining internal bit-packed state layout helpers. */
object PipelinedTransmitter {
	export TransmissionProtocol.*

	/** Maximum number of unserialized messages allowed in [[PipelinedTransmitter.transmissionQueue]] (`2^28 - 1 = 268,435,455`), and bitmask isolating bits `0..27` of [[CoordinationState]]. */
	private inline val MaxQueuedCount = 0x0fff_ffff

	/** Bitmask isolating bits `28..30` of [[CoordinationState]], which encode the [[JointStageState]] (leaving bit `31` unused `0`). */
	private inline val JointStageStateMask = 7 << 28

	/** Encodes the pre-shifted joint state of the two pipeline stages stored in bits `28..30` of [[CoordinationState]]. */
	private[PipelinedTransmitter] opaque type JointStageState = Int

	private object JointStageState {
		/** Neither serialization nor transmission is active (`0 << 28`), both stage rings are pruned to `ringMinLengthOnIdle`, and `queuedCount` is always `0`. */
		val Idle: JointStageState = 0 << 28

		/** Only serialization is active (`1 << 28`); the thread in [[PipelinedTransmitter.runSerializationLoop]] owns both serialization and transmission stage variables, and `queuedCount >= 0`. */
		val SerializingOnly: JointStageState = 1 << 28

		/** Only socket transmission is active (`2 << 28`); the active transmission chain owns transmission stage variables, [[PipelinedTransmitter.serializationStorage]] is pruned to `ringMinLengthOnIdle`, and `queuedCount` is always `0`. */
		val TransmittingOnly: JointStageState = 2 << 28

		/** Both stages are active in parallel (`3 << 28`); the thread in [[PipelinedTransmitter.runSerializationLoop]] owns serialization stage variables, the active transmission chain owns transmission stage variables, and `queuedCount >= 0`. */
		val Pipelined: JointStageState = 3 << 28

		/** Serialization of the next package has finished while the previous transmission is still in progress (`4 << 28`); neither stage can be claimed by callers, and only the finishing transmission thread entering [[PipelinedTransmitter.onTransmissionFinished]] will own both stages to swap and dispatch the staged package (`queuedCount >= 0`). */
		val HandoffPending: JointStageState = 4 << 28

		/** A [[PipelinedTransmitter.pruneTo]] caller temporarily owns both serialization and transmission stage variables (`5 << 28`); `queuedCount` starts at `0` and grows if messages arrive concurrently during pruning. */
		val Pruning: JointStageState = 5 << 28

		/** Wraps a pre-shifted integer bitmask as a [[JointStageState]]. */
		inline def fromInt(raw: Int): JointStageState = {
			assert((raw & ~JointStageStateMask) == 0 && raw >= 0 && raw <= Pruning, s"Invalid JointStageState bits: $raw")
			raw
		}

		extension (stageState: JointStageState) {
			/** Returns the underlying pre-shifted `Int` bitmask of this [[JointStageState]]. */
			inline def toInt: Int = stageState
		}
	}

	/** Bit-packed 31-bit coordination state stored in [[PipelinedTransmitter.coordinationState]]: bits `0..27` hold `queuedCount`, bits `28..30` hold [[JointStageState]], and bit `31` remains `0`.
	 *
	 * Joint State Ownership Table:
	 * | Joint Stage State | `queuedCount` | Serialization Variables Owner | Transmission Variables Owner | Allowed Transitions |
	 * | --- | --- | --- | --- | --- |
	 * | [[JointStageState.Idle]] | Always `0` | None (pruned) | None (pruned) | `transmitWithAttachment` -> `SerializingOnly`; `pruneTo` -> `Pruning` |
	 * | [[JointStageState.SerializingOnly]] | `>= 0` | `runSerializationLoop` holder | Same `runSerializationLoop` holder (swaps in Cases B/C) | `runSerializationLoop` Case B -> `TransmittingOnly` or Case C -> `Pipelined` |
	 * | [[JointStageState.TransmittingOnly]] | Always `0` | None (pruned) | Active transmission chain | `transmitWithAttachment` -> `Pipelined`; `onTransmissionFinished` Case D -> `Idle` |
	 * | [[JointStageState.Pipelined]] | `>= 0` | `runSerializationLoop` holder | Active transmission chain | `runSerializationLoop` Case A -> `HandoffPending`; `onTransmissionFinished` Case C -> `SerializingOnly` |
	 * | [[JointStageState.HandoffPending]] | `>= 0` | Finishing thread in `onTransmissionFinished` only | Finishing thread in `onTransmissionFinished` only (until it starts the next message) | `onTransmissionFinished` Case A -> `TransmittingOnly` or Case B -> `Pipelined` |
	 * | [[JointStageState.Pruning]] | `>= 0` (`0` on entry) | `pruneTo` caller | `pruneTo` caller | `pruneTo` -> `Idle` or `SerializingOnly` |
	 *
	 * Two-Line Intuition ("Who Takes What?"):
	 *  - Claiming work: A caller starts serializing only from [[JointStageState.Idle]] or [[JointStageState.TransmittingOnly]] (in `transmitWithAttachment`), or prunes only from [[JointStageState.Idle]] (in `pruneTo`); in all other joint stage states callers only increment [[CoordinationState.queuedCount]] and enqueue.
	 *  - Triggering transmission: Only the [[JointStageState.SerializingOnly]] holder can start transmission when idle (in `runSerializationLoop` Cases B/C), and only the finishing transmission thread in `onTransmissionFinished` can hand off a [[JointStageState.HandoffPending]] package directly into the next transmission. */
	private[PipelinedTransmitter] opaque type CoordinationState = Int

	private object CoordinationState {
		/** Completely quiescent coordination state (`0`): `queuedCount == 0` and [[JointStageState.Idle]]. */
		val Idle: CoordinationState = JointStageState.Idle

		/** Initial coordination state claimed atomically by [[PipelinedTransmitter.pruneTo]] to claim exclusive ownership of both serialization and transmission state (`queuedCount == 0` and [[JointStageState.Pruning]]). */
		val Pruning: CoordinationState = JointStageState.Pruning

		/** Converts a raw non-negative 31-bit integer read from [[PipelinedTransmitter.coordinationState]] into a [[CoordinationState]]. */
		inline def fromInt(raw: Int): CoordinationState = {
			assert(raw >= 0 && (raw & JointStageStateMask) <= JointStageState.Pruning, s"Invalid CoordinationState bits: $raw")
			raw
		}

		extension (state: CoordinationState) {
			/** Returns the raw 31-bit packed integer representation of this [[CoordinationState]]. */
			inline def toInt: Int = state

			/** Extracts the pre-shifted [[JointStageState]] from bits `28..30`. */
			inline def jointStageState: JointStageState = state & JointStageStateMask

			/** Extracts the unshifted count of unserialized queued messages from bits `0..27`. */
			inline def queuedCount: Int = state & MaxQueuedCount

			/** Returns a copy of this state with bits `28..30` replaced by `next` while preserving [[queuedCount]]. */
			inline def withJointStageState(next: JointStageState): CoordinationState =
				(state & ~JointStageStateMask) | next

			/** Returns a copy of this state with [[queuedCount]] incremented by `1` while preserving [[jointStageState]]. */
			inline def withIncrementedQueue: CoordinationState = {
				assert(queuedCount < MaxQueuedCount, "CoordinationState queuedCount overflow")
				state + 1
			}

			/** Returns a copy of this state with [[queuedCount]] decremented by `1` while preserving [[jointStageState]]. */
			inline def withDecrementedQueue: CoordinationState = {
				assert(queuedCount > 0, "CoordinationState queuedCount underflow")
				state - 1
			}
		}
	}

	extension (atomic: AtomicInteger) {
		/** Atomically reads the current [[CoordinationState]]. */
		inline def getCoordinationState: CoordinationState = CoordinationState.fromInt(atomic.get())

		/** Atomically sets the coordination state to `update` if the current value equals `expected`. */
		inline def compareAndSetCoordinationState(expected: CoordinationState, update: CoordinationState): Boolean =
			atomic.compareAndSet(expected, update)
	}

	/** Transmission-stage trailing stream step constant (`0`) indicating that no trailing stream is currently active. */
	private inline val STREAM_STEP_IDLE = 0

	/** Transmission-stage trailing stream step constant (`1`) indicating that the loop should poll and open the next queued [[readren.common.ByteSource]] or write the terminal `0x00` chunk header if none remain. */
	private inline val STREAM_STEP_NEXT_SOURCE = 1

	/** Transmission-stage trailing stream step constant (`2`) indicating that the loop should initiate a chunk read from [[PipelinedTransmitter.currentAsyncReader]] into [[PipelinedTransmitter.streamTransferBuffer]]. */
	private inline val STREAM_STEP_READ_SOURCE = 2

	/** Transmission-stage trailing stream step constant (`3`) indicating that an asynchronous [[ByteSource.AsyncReader.read]] call is currently in flight. */
	private inline val STREAM_STEP_AWAITING_READ = 3

	/** Transmission-stage trailing stream step constant (`4`) indicating that a chunk read has completed and its byte count in [[PipelinedTransmitter.transmissionCompletedReadBytes]] is ready to be processed. */
	private inline val STREAM_STEP_PROCESS_READ = 4

	/** Transmission-stage trailing stream step constant (`5`) indicating that [[ByteSource.AsyncReader.read]] failed with an exception stored in [[PipelinedTransmitter.transmissionStreamFailure]]. */
	private inline val STREAM_STEP_READ_FAILED = 5

	/** Transmission-stage trailing stream step constant (`6`) indicating that an asynchronous socket write of a stream chunk or terminal sentinel is currently in flight on [[PipelinedTransmitter.channel]]. */
	private inline val STREAM_STEP_AWAITING_WRITE = 6

	/** Transmission-stage trailing stream step constant (`7`) indicating that a trailing stream socket write completed and the loop should check for remaining buffer bytes, finalize the stream, or read the next chunk. */
	private inline val STREAM_STEP_PROCESS_WRITE = 7

	/** Transmission-stage trailing stream step constant (`8`) indicating that a trailing stream socket write failed with an exception stored in [[PipelinedTransmitter.transmissionStreamFailure]]. */
	private inline val STREAM_STEP_WRITE_FAILED = 8

	/** Shared no-op function reference used as the default/reset value of [[PipelinedTransmitter.onIdleHandler]].
	 * Accessed by any thread registering or firing idle callbacks. */
	private val noOpIdle: () => Unit = () => ()

	/** Shared zero-allocation callback adapter used by [[PipelinedTransmitter.transmit]] to forward the single-argument completion callback through [[PipelinedTransmitter.transmitWithAttachment]] as its attachment parameter without allocating a wrapper closure per message.
	 * Executed on the thread invoking the message's completion callback in [[PipelinedTransmitter.onTransmissionFinished]]. */
	private val invokeWithoutAttachment: (Report, Report => Unit) => Unit = (report, onComplete) => onComplete(report)

	/** Immutable envelope representing an unserialized message submission waiting in [[PipelinedTransmitter.transmissionQueue]].
	 * Allocated by a caller thread in [[PipelinedTransmitter.transmitWithAttachment]] when serialization ownership is already occupied, and consumed later by whichever thread (caller or NIO.2 completion thread) drains the next slot for serialization.
	 * @param message The application message to serialize and transmit.
	 * @param msgVersion The protocol version governing serialization of [[message]].
	 * @param timeout The maximum duration to wait for each individual socket write operation.
	 * @param timeUnit The time unit of [[timeout]].
	 * @param attachment The user-supplied attachment passed through to [[onComplete]].
	 * @param onComplete The completion callback invoked in FIFO submission order when transmission finishes or fails.
	 * @param serializer The type-erased [[cluster.serialization.Serializer]] instance used to encode [[message]]. */
	private final class MsgSubmission(
		val message: Any,
		val msgVersion: ProtocolVersion,
		val timeout: Long,
		val timeUnit: TimeUnit,
		val attachment: Any,
		val onComplete: (Report, Any) => Unit,
		val serializer: Serializer[Any]
	)
}

/** A thread-safe pipelined capability for sequentially serializing and transmitting messages over an [[java.nio.channels.AsynchronousSocketChannel]].
 *
 * In the serialization stage, the thread holding serialization ownership encodes a message's complete control package into an isolated [[cluster.misc.DualEndedCircularStorage]] ring ([[serializationStorage]]) before any bytes of that message are written to the socket, guaranteeing that control serialization failures never taint the wire. In the transmission stage, the active transmission chain (initiated on the handoff thread and driven through NIO.2 and [[readren.common.ByteSource.AsyncReader]] completion callbacks) writes a previously serialized control package as a single protocol frame using bounded gathering writes, followed by any out-of-line [[readren.common.ByteSource]] trailing streams. Because the two stages operate on swapped, isolated state compartments coordinated by [[coordinationState]], a serialization thread can serialize message `N+1` concurrently while NIO.2 socket I/O transmits message `N`, and additional submissions wait in a lock-free FIFO queue.
 *
 * @param channel The channel over which the messages will be transmitted.
 * @param context Transport diagnostic addressing context.
 * @param capacityOfBufferInRing The capacity of each buffer of the [[cluster.misc.DualEndedCircularStorage]].
 * @param ringMaxLength The maximum number of buffers allowed in each stage's circular storage.
 * @param ringMinLengthOnIdle The minimum number of buffers retained per stage on idle transitions.
 * @param streamTransferBufferCapacity The capacity of the direct buffer lazily allocated to transfer streaming chunks.
 * @param maxGatheringBuffers The maximum number of [[java.nio.ByteBuffer]] entries submitted per gathering socket write call.
 * @tparam M The type of messages transmitted by this instance. */
class PipelinedTransmitter[M](
	channel: AsynchronousSocketChannel,
	val context: Context,
	capacityOfBufferInRing: Int = DEFAULT_CAPACITY_FOR_BUFFERS_IN_RING,
	val ringMaxLength: RingLength = DEFAULT_RING_MAX_LENGTH,
	val ringMinLengthOnIdle: RingLength = DEFAULT_RING_MIN_LENGTH_ON_IDLE,
	val streamTransferBufferCapacity: Int = DEFAULT_STREAM_TRANSFER_BUFFER_CAPACITY,
	val maxGatheringBuffers: Int = DEFAULT_MAX_GATHERING_BUFFERS
) {
	assert(capacityOfBufferInRing >= Serializer.BUFFER_SIZE_REQUIRED_BY_MOST_DEMANDING_OPERATION)
	require(streamTransferBufferCapacity > 0, s"streamTransferBufferCapacity must be positive, got $streamTransferBufferCapacity")
	require(maxGatheringBuffers >= 2, s"maxGatheringBuffers must be at least 2, got $maxGatheringBuffers")

	/** Lock-free atomic [[CoordinationState]] cell packing the wait queue reservation count (`queuedCount`) and [[JointStageState]] across arbitrary caller threads and NIO.2 completion threads.
	 * The [[CoordinationState.Idle]] (`0`) value represents the completely quiescent state (`queuedCount == 0` and [[JointStageState.Idle]]). */
	private val coordinationState: AtomicInteger = new AtomicInteger(CoordinationState.Idle.toInt)

	/** Lock-free FIFO queue holding unserialized [[PipelinedTransmitter.MsgSubmission]] instances when serialization is already occupied and new messages arrive.
	 * Enqueued by caller threads in [[transmitWithAttachment]] and dequeued by whichever thread (caller or NIO.2 completion thread) claims a queued count slot while entering or continuing [[runSerializationLoop]]. */
	private val transmissionQueue: ConcurrentLinkedQueue[MsgSubmission] = new ConcurrentLinkedQueue()

	/** Atomic holder for the composite callback to execute when [[coordinationState]] transitions to [[CoordinationState.Idle]].
	 * Updated by caller threads in [[triggerOnIdle]] and consumed/reset to [[PipelinedTransmitter.noOpIdle]] by whichever thread transitions the pipeline to [[CoordinationState.Idle]]. */
	private val onIdleHandler: AtomicReference[() => Unit] = new AtomicReference(noOpIdle)

	// ====================================================================================================
	// Serialization Stage Isolated State
	// Concurrency invariant: Accessed and mutated exclusively by the single thread in `runSerializationLoop` while `JointStageState.SerializingOnly` or `JointStageState.Pipelined` holds, by the finishing thread in `onTransmissionFinished` while `JointStageState.HandoffPending` holds (during `handoffSerializedOutcomeToTransmissionStage` and Case A `serializationStorage.pruneTo`), or by the caller in `pruneTo` while `JointStageState.Pruning` holds, with the sole exception of the unlocked diagnostic read in `bufferInRingTotalCount`.
	// ====================================================================================================

	/** Circular direct-buffer ring into which the active serialization thread serializes the control package.
	 * Mutated by the thread executing [[runSerializationLoop]] (either a caller thread or an NIO.2 completion thread) and swapped with [[transmissionStorage]] in [[handoffSerializedOutcomeToTransmissionStage]]. */
	private var serializationStorage: DualEndedCircularStorage[ByteBuffer] = new DualEndedCircularStorage[ByteBuffer](() => ByteBuffer.allocateDirect(capacityOfBufferInRing), ringMaxLength)

	/** FIFO queue of non-empty out-of-line [[readren.common.ByteSource]] instances encountered while serializing the current control package.
	 * Populated by the serialization thread in [[serializationWriter.putByteSource]] and swapped with [[transmissionByteSources]] in [[handoffSerializedOutcomeToTransmissionStage]]. */
	private var serializationByteSources: util.ArrayDeque[ByteSource] = new util.ArrayDeque[ByteSource]()

	/** Reusable identity map for object-reference deduplication during control package serialization.
	 * Reset and mutated exclusively by the serialization thread inside [[serializationWriter.prepare]] and [[cluster.serialization.Serializer.serialize]]. */
	private val deduplicationMap: RecyclableIdentityIntMap = new RecyclableIdentityIntMap()

	/** Accumulated byte count of the serialized control package payload across all buffers in [[serializationStorage]] (excluding the frame header and package sentinel).
	 * Reset and incremented by the serialization thread during [[runSerializationLoop]] and copied to [[transmissionTotalControlBytes]] in [[handoffSerializedOutcomeToTransmissionStage]]. */
	private var serializationTotalControlBytes: Int = 0

	/** Indicates whether the final buffer of [[serializationStorage]] lacked sufficient remaining capacity to inline the package-terminating sentinel, requiring [[fillAndSendNextControlBatch]] to append [[sentinelBuffer]] separately during transmission.
	 * Set by the serialization thread in [[serializationWriter.finishControlPackage]] and copied to [[transmissionNeedsSeparateSentinel]] in [[handoffSerializedOutcomeToTransmissionStage]]. */
	private var serializationNeedsSeparateSentinel: Boolean = false

	/** Holds the non-delivery report when control package serialization fails (via [[cluster.serialization.Serializer.SerializationException]] or [[RingLengthExceededException]]), or `null` when serialization succeeded.
	 * Written by the serialization thread in [[runSerializationLoop]] and copied to [[transmissionFailureReport]] in [[handoffSerializedOutcomeToTransmissionStage]] so failures are reported in strict FIFO order via [[initiateTransmission]] and [[onTransmissionFinished]]. */
	private var serializationFailureReport: NotDelivered | Null = null

	/** The message currently being serialized or held ready in the serialization stage.
	 * Written by the serialization thread in [[runSerializationLoop]] and transferred to [[transmissionMessage]] in [[handoffSerializedOutcomeToTransmissionStage]]. */
	private var serializationMessage: Any = null

	/** The socket write timeout configured for the message in the serialization stage.
	 * Written by the serialization thread in [[runSerializationLoop]] and transferred to [[transmissionTimeout]] in [[handoffSerializedOutcomeToTransmissionStage]]. */
	private var serializationTimeout: Long = 0L

	/** The time unit associated with [[serializationTimeout]].
	 * Written by the serialization thread in [[runSerializationLoop]] and transferred to [[transmissionTimeUnit]] in [[handoffSerializedOutcomeToTransmissionStage]]. */
	private var serializationTimeUnit: TimeUnit = TimeUnit.SECONDS

	/** The user-supplied callback attachment for the message in the serialization stage.
	 * Written by the serialization thread in [[runSerializationLoop]] and transferred to [[transmissionAttachment]] in [[handoffSerializedOutcomeToTransmissionStage]]. */
	private var serializationAttachment: Any = null

	/** The completion callback for the message in the serialization stage.
	 * Written by the serialization thread in [[runSerializationLoop]] and transferred to [[transmissionOnComplete]] in [[handoffSerializedOutcomeToTransmissionStage]]. */
	private var serializationOnComplete: (Report, Any) => Unit = null

	// ====================================================================================================
	// Transmission Stage Isolated State
	// Concurrency invariant: Accessed and mutated exclusively by the active transmission chain (thread calling `initiateTransmission` through NIO.2 / AsyncReader completion callbacks up to `onTransmissionFinished`) while `JointStageState.TransmittingOnly`, `JointStageState.Pipelined`, or `JointStageState.HandoffPending` holds, by the single thread in `runSerializationLoop` while `JointStageState.SerializingOnly` holds (during `handoffSerializedOutcomeToTransmissionStage`), or by the caller in `pruneTo` while `JointStageState.Pruning` holds, with the sole exception of the unlocked diagnostic read in `bufferInRingTotalCount`.
	// ====================================================================================================

	/** Circular direct-buffer ring holding the serialized control package buffers currently being transmitted.
	 * Swapped in from [[serializationStorage]] during [[handoffSerializedOutcomeToTransmissionStage]] and drained across NIO.2 completion callbacks in [[fillAndSendNextControlBatch]] and [[onTransmissionFinished]]. */
	private var transmissionStorage: DualEndedCircularStorage[ByteBuffer] = new DualEndedCircularStorage[ByteBuffer](() => ByteBuffer.allocateDirect(capacityOfBufferInRing), ringMaxLength)

	/** FIFO queue of out-of-line [[readren.common.ByteSource]] instances awaiting trailing stream transmission after the control package completes.
	 * Swapped in from [[serializationByteSources]] during [[handoffSerializedOutcomeToTransmissionStage]] and drained in [[runTrailingStreamLoop]]. */
	private var transmissionByteSources: util.ArrayDeque[ByteSource] = new util.ArrayDeque[ByteSource]()

	/** Dedicated 5-byte buffer holding the VLQ-encoded single-frame length header ([[transmissionTotalControlBytes]]) for the transmitted control package.
	 * Populated by the thread starting transmission in [[initiateTransmission]] (via [[transmissionControlWriteHandler.putByte]]) and consumed by [[channel]] in the first gathering write batch. */
	private val frameHeaderBuffer: ByteBuffer = ByteBuffer.allocate(VLQ.INT_MAX_LENGTH)

	/** Dedicated 5-byte buffer holding the control package terminator sentinel (`0x00` or [[TransmissionProtocol.STREAM_FOLLOWS_SENTINEL_ENCODED]]) when [[transmissionNeedsSeparateSentinel]] is `true`.
	 * Populated in [[initiateTransmission]] and appended to the final gathering write batch in [[fillAndSendNextControlBatch]]. */
	private val sentinelBuffer: ByteBuffer = ByteBuffer.allocate(VLQ.INT_MAX_LENGTH)

	/** Reusable array of [[java.nio.ByteBuffer]] references (of length [[maxGatheringBuffers]]) passed to gathering [[java.nio.channels.AsynchronousSocketChannel.write]] calls on [[channel]] during control package transmission.
	 * Populated in [[fillAndSendNextControlBatch]] and advanced/cleared in [[transmissionControlWriteHandler]] on NIO.2 completion threads. */
	private val gatheringBatch: Array[ByteBuffer] = new Array[ByteBuffer](maxGatheringBuffers)

	/** Index of the first buffer in [[gatheringBatch]] that still has remaining unwritten bytes during the active gathering write batch.
	 * Reset to `0` in [[fillAndSendNextControlBatch]] and incremented in [[transmissionControlWriteHandler.completed]] on NIO.2 completion threads. */
	private var batchOffset: Int = 0

	/** Exclusive upper bound index (`<= maxGatheringBuffers`) of populated buffers in [[gatheringBatch]] for the active gathering write batch.
	 * Set in [[fillAndSendNextControlBatch]] and read in [[transmissionControlWriteHandler.completed]] on NIO.2 completion threads. */
	private var batchLimit: Int = 0

	/** Tracks whether [[transmissionStorage]] still has unread buffers behind its write end that have not yet been placed into [[gatheringBatch]].
	 * Initialized to `true` in [[initiateTransmission]] and updated via [[cluster.misc.DualEndedCircularStorage.advanceReadEnd]] in [[fillAndSendNextControlBatch]]. */
	private var transmissionHasUnreadStorageBuffers: Boolean = false

	/** Tracks whether the standalone [[sentinelBuffer]] still needs to be appended to [[gatheringBatch]] after all buffers in [[transmissionStorage]] have been placed into batches.
	 * Initialized to [[transmissionNeedsSeparateSentinel]] in [[initiateTransmission]] and cleared to `false` once [[sentinelBuffer]] is added to [[gatheringBatch]] in [[fillAndSendNextControlBatch]]. */
	private var transmissionSeparateSentinelPending: Boolean = false

	/** Lazily allocated direct buffer (of capacity [[streamTransferBufferCapacity]]) used during trailing stream transfers to copy chunks from [[readren.common.ByteSource.AsyncReader]] to [[channel]].
	 * Allocated on demand in [[ensureStreamTransferBuffer]] when the first message with a trailing stream enters transmission, and released (`null`) when the pipeline transitions to idle or [[pruneTo]] executes. */
	private var streamTransferBuffer: ByteBuffer | Null = null

	/** Dedicated 5-byte buffer holding the VLQ-encoded chunk length header (or terminal `0x00` / [[TransmissionProtocol.CORRUPTED_PACKAGE_SENTINEL_ENCODED]] sentinel) for trailing stream writes.
	 * Mutated by trailing stream methods (including [[trailingStreamTransmissionHandler.putByte]]) on the thread initiating the stream or on NIO.2 / [[readren.common.ByteSource.AsyncReader]] completion threads. */
	private val streamChunkHeaderBuffer: ByteBuffer = ByteBuffer.allocate(VLQ.INT_MAX_LENGTH)

	/** Two-element buffer array (`Array(streamChunkHeaderBuffer, streamTransferBuffer)`) used for gathering writes of trailing stream chunks.
	 * Slot `0` permanently points to [[streamChunkHeaderBuffer]]; slot `1` is bound to [[streamTransferBuffer]] in [[startTrailingStream]] and cleared to `null` when idle. */
	private val streamChunkBuffers: Array[ByteBuffer] = Array(streamChunkHeaderBuffer, null)

	/** Total payload byte count of the control package currently being transmitted.
	 * Copied from [[serializationTotalControlBytes]] in [[handoffSerializedOutcomeToTransmissionStage]] and read in [[initiateTransmission]]. */
	private var transmissionTotalControlBytes: Int = 0

	/** Indicates whether [[fillAndSendNextControlBatch]] must append [[sentinelBuffer]] after draining [[transmissionStorage]] during the active transmission.
	 * Copied from [[serializationNeedsSeparateSentinel]] in [[handoffSerializedOutcomeToTransmissionStage]] and read in [[initiateTransmission]]. */
	private var transmissionNeedsSeparateSentinel: Boolean = false

	/** Non-null when the message swapped into transmission failed control serialization and must immediately report failure without writing to [[channel]].
	 * Copied from [[serializationFailureReport]] in [[handoffSerializedOutcomeToTransmissionStage]] and consumed in [[initiateTransmission]]. */
	private var transmissionFailureReport: NotDelivered | Null = null

	/** The message currently undergoing transmission (or failure reporting).
	 * Copied from [[serializationMessage]] in [[handoffSerializedOutcomeToTransmissionStage]] and cleared in [[onTransmissionFinished]]. */
	private var transmissionMessage: Any = null

	/** Socket write timeout for the message currently in transmission.
	 * Copied from [[serializationTimeout]] in [[handoffSerializedOutcomeToTransmissionStage]] and passed to [[java.nio.channels.AsynchronousSocketChannel.write]] on [[channel]]. */
	private var transmissionTimeout: Long = 0L

	/** Time unit for [[transmissionTimeout]] during transmission.
	 * Copied from [[serializationTimeUnit]] in [[handoffSerializedOutcomeToTransmissionStage]] and passed to [[java.nio.channels.AsynchronousSocketChannel.write]] on [[channel]]. */
	private var transmissionTimeUnit: TimeUnit = TimeUnit.SECONDS

	/** User-supplied callback attachment for the message currently in transmission.
	 * Copied from [[serializationAttachment]] in [[handoffSerializedOutcomeToTransmissionStage]] and passed to [[transmissionOnComplete]] in [[onTransmissionFinished]]. */
	private var transmissionAttachment: Any = null

	/** Completion callback for the message currently in transmission.
	 * Copied from [[serializationOnComplete]] in [[handoffSerializedOutcomeToTransmissionStage]] and invoked in [[onTransmissionFinished]]. */
	private var transmissionOnComplete: (Report, Any) => Unit = null

	/** The [[readren.common.ByteSource]] currently being streamed in the transmission stage, or `null` when between sources or not streaming.
	 * Mutated in [[runTrailingStreamLoop]], [[handleByteSourceChunkRead]], and [[cleanupStream]] by the active transmission thread / completion handler. */
	private var currentStreamByteSource: ByteSource | Null = null

	/** The open [[readren.common.ByteSource.AsyncReader]] session for [[currentStreamByteSource]], or `null` when no reader session is open.
	 * Opened in [[runTrailingStreamLoop]] and closed/cleared in [[cleanupCurrentReader]] by the active transmission thread / completion handler. */
	private var currentAsyncReader: ByteSource.AsyncReader | Null = null

	/** Cumulative bytes read so far from [[currentAsyncReader]] for [[currentStreamByteSource]].
	 * Reset to `0L` when opening a source in [[runTrailingStreamLoop]] and incremented in [[handleByteSourceChunkRead]]. */
	private var currentStreamBytesRead: Long = 0L

	/** Declared byte size ([[readren.common.ByteSource.size]]) of [[currentStreamByteSource]], validated against [[currentStreamBytesRead]] to detect premature EOF or size drift.
	 * Set when polling the next source in [[runTrailingStreamLoop]]. */
	private var currentStreamDeclaredSize: Long = 0L

	/** Non-null [[TransmissionProtocol.ByteSourceProblem]] recorded when trailing stream reading fails and an in-band [[TransmissionProtocol.CORRUPTED_PACKAGE_SENTINEL_ENCODED]] is being written.
	 * Set in [[abortTrailingStream]] and delivered to [[onTransmissionFinished]] once the sentinel write completes in [[stepTrailingStream]]. */
	private var streamCancellationReason: NotDelivered | Null = null

	/** Lock-free work-stealing trampoline counter serializing all trailing stream state transitions across NIO.2 completion threads and [[readren.common.ByteSource.AsyncReader]] completion threads.
	 * A transition from `0` to `1` via [[java.util.concurrent.atomic.AtomicInteger.getAndIncrement]] in [[runTrailingStreamLoop]] grants exclusive ownership of [[stepTrailingStream]]; increments above `1` register pending continuation steps that the active owner drains iteratively in $O(1)$ stack space. */
	private val transmissionStreamWorkPermits: AtomicInteger = new AtomicInteger(0)

	/** Current step of the trailing stream state machine (`STREAM_STEP_*` constants in [[PipelinedTransmitter]]).
	 * Written before releasing or incrementing [[transmissionStreamWorkPermits]] and read by the single thread executing [[stepTrailingStream]]. */
	@volatile private var transmissionStreamStep: Int = STREAM_STEP_IDLE

	/** Number of bytes produced by the most recent [[readren.common.ByteSource.AsyncReader.read]] completion (or `-1` on EOF), staged for processing in [[PipelinedTransmitter.STREAM_STEP_PROCESS_READ]].
	 * Written by [[stepTrailingStream]] (on synchronous return) or [[byteSourceReadHandler.completed]] (before incrementing [[transmissionStreamWorkPermits]]) and read in [[stepTrailingStream]]. */
	@volatile private var transmissionCompletedReadBytes: Int = 0

	/** Failure exception captured from an asynchronous or synchronous [[readren.common.ByteSource.AsyncReader.read]] or socket write operation during trailing stream processing.
	 * Written before transitioning [[transmissionStreamStep]] to [[PipelinedTransmitter.STREAM_STEP_READ_FAILED]] or [[PipelinedTransmitter.STREAM_STEP_WRITE_FAILED]] and consumed in [[stepTrailingStream]]. */
	@volatile private var transmissionStreamFailure: Throwable | Null = null

	/** Tracks whether the active trailing stream socket write is transmitting a terminal sentinel (`0x00` or [[TransmissionProtocol.CORRUPTED_PACKAGE_SENTINEL_ENCODED]]) rather than a data chunk.
	 * Mutated exclusively inside [[stepTrailingStream]] (and helpers called by it) under [[transmissionStreamWorkPermits]] ownership. */
	private var transmissionWritingSentinel: Boolean = false

	// ====================================================================================================
	// Public API
	// ====================================================================================================

	/** Enqueues or immediately serializes and transmits a message through [[channel]], invoking the callback with a [[PipelinedTransmitter.Report]] upon completion.
	 * May be called concurrently from any thread. Passes `onComplete` directly as the attachment to [[transmitWithAttachment]] via [[PipelinedTransmitter.invokeWithoutAttachment]] to avoid allocating a wrapper closure per message.
	 * @param message The message to transmit.
	 * @param msgVersion The protocol version governing serialization of the message.
	 * @param timeout The maximum time to wait for each socket write operation.
	 * @param timeUnit The unit of time for the `timeout` parameter.
	 * @param onComplete A callback invoked in FIFO submission order when transmission completes or fails.
	 * @param serializer The serializer used to encode the message into outgoing bytes.
	 * @throws java.util.concurrent.RejectedExecutionException if serialization ownership is occupied and the wait queue has reached [[PipelinedTransmitter.MaxQueuedCount]]. */
	def transmit(message: M, msgVersion: ProtocolVersion, timeout: Long = 1, timeUnit: TimeUnit = TimeUnit.SECONDS)(onComplete: Report => Unit)(using serializer: Serializer[M]): Unit = {
		transmitWithAttachment[Report => Unit](message, msgVersion, onComplete, timeout, timeUnit)(invokeWithoutAttachment)
	}

	/** Enqueues or immediately serializes and transmits a message through [[channel]] with a user-supplied attachment passed through to the completion callback.
	 * May be called concurrently from any thread. If [[JointStageState]] is [[JointStageState.Idle]] or [[JointStageState.TransmittingOnly]], the calling thread atomically claims serialization ownership by transitioning to [[JointStageState.SerializingOnly]] or [[JointStageState.Pipelined]] respectively and serializes `message` immediately on the calling thread via [[runSerializationLoop]] (and also initiates socket transmission via [[initiateTransmission]] if no prior transmission is active when serialization finishes). Otherwise (in [[JointStageState.SerializingOnly]], [[JointStageState.Pipelined]], [[JointStageState.HandoffPending]], or [[JointStageState.Pruning]]), the calling thread atomically increments `queuedCount` in [[coordinationState]], appends a [[PipelinedTransmitter.MsgSubmission]] to [[transmissionQueue]], and returns immediately without blocking.
	 * @param message The message to transmit.
	 * @param msgVersion The protocol version governing serialization of the message.
	 * @param attachment An arbitrary attachment passed through to `onComplete`.
	 * @param timeout The maximum time to wait for each socket write operation.
	 * @param timeUnit The unit of time for the `timeout` parameter.
	 * @param onComplete A callback invoked in FIFO submission order with a [[PipelinedTransmitter.Report]] and `attachment`.
	 * @param serializer The serializer used to encode the message into outgoing bytes.
	 * @tparam A The type of the attachment passed through.
	 * @throws java.util.concurrent.RejectedExecutionException if serialization ownership is occupied and the wait queue has reached [[PipelinedTransmitter.MaxQueuedCount]]. */
	def transmitWithAttachment[A](
		message: M,
		msgVersion: ProtocolVersion,
		attachment: A,
		timeout: Long = 1,
		timeUnit: TimeUnit = TimeUnit.SECONDS
	)(
		onComplete: (Report, A) => Unit
	)(using serializer: Serializer[M]): Unit = {
		// Single atomic CAS loop on the caller thread: either claim serialization ownership directly (from Idle or TransmittingOnly) or reserve a queue slot (queuedCount -> queuedCount + 1).
		// Reserving queuedCount + 1 atomically in the same loop prevents any subsequent caller from observing a free serialization slot and overtaking this message.
		var wasStateChangedConcurrently = true
		while wasStateChangedConcurrently do {
			val cur = coordinationState.getCoordinationState
			val stageState = cur.jointStageState
			if stageState == JointStageState.Idle || stageState == JointStageState.TransmittingOnly then {
				assert(cur.queuedCount == 0)
				val nextStageState = if stageState == JointStageState.Idle then JointStageState.SerializingOnly else JointStageState.Pipelined
				if coordinationState.compareAndSetCoordinationState(cur, cur.withJointStageState(nextStageState)) then {
					wasStateChangedConcurrently = false
					// Fast path: serialization ownership was free; serialize immediately on the caller thread.
					runSerializationLoop(
						message,
						msgVersion,
						timeout,
						timeUnit,
						attachment,
						onComplete.asInstanceOf[(Report, Any) => Unit],
						serializer.asInstanceOf[Serializer[Any]]
					)
				}
			} else if cur.queuedCount < MaxQueuedCount then {
				if coordinationState.compareAndSetCoordinationState(cur, cur.withIncrementedQueue) then {
					wasStateChangedConcurrently = false
					// Slow path: serialization ownership is already occupied; enqueue the submission into `transmissionQueue` for whichever thread later claims it by decrementing `queuedCount` in `runSerializationLoop`, `onTransmissionFinished`, or `pruneTo`.
					transmissionQueue.add(
						new MsgSubmission(
							message,
							msgVersion,
							timeout,
							timeUnit,
							attachment,
							onComplete.asInstanceOf[(Report, Any) => Unit],
							serializer.asInstanceOf[Serializer[Any]]
						)
					)
				}
			} else throw new RejectedExecutionException(s"Transmission queue capacity ($MaxQueuedCount) exceeded")
		}
	}

	/** Programs an idle callback to be executed once when no transmission or serialization is in progress and [[transmissionQueue]] is empty.
	 * Executed on any caller thread. If the pipeline is already idle ([[coordinationState]] is [[CoordinationState.Idle]]) when this method is called, the callback is executed immediately on the calling thread; otherwise, it will be executed on the thread that completes the final active message in [[onTransmissionFinished]] or releases an empty [[JointStageState.Pruning]] phase in [[pruneTo]].
	 * @param onIdle The callback to execute when the pipeline becomes idle. */
	def triggerOnIdle(onIdle: () => Unit): Unit = {
		// Atomically compose the new callback with any previously registered unexecuted idle callback.
		onIdleHandler.updateAndGet { prev =>
			if prev eq noOpIdle then onIdle
			else () => {
				prev()
				onIdle()
			}
		}
		// If the pipeline is already quiescent, atomically claim and invoke the handler immediately.
		if coordinationState.getCoordinationState == CoordinationState.Idle then {
			val handler = onIdleHandler.getAndSet(noOpIdle)
			handler()
		}
	}

	/** Prunes unused allocated buffers in both [[serializationStorage]] and [[transmissionStorage]] via [[DualEndedCircularStorage.pruneTo]] down to `targetLength` per stage when the pipeline is completely idle, and releases [[streamTransferBuffer]].
	 * Executed on any caller thread invoking [[pruneTo]]. To prevent races with concurrent [[transmit]] or [[transmitWithAttachment]] calls during pruning, the caller atomically claims exclusive ownership of both serialization and transmission state by transitioning [[CoordinationState.Idle]] into [[CoordinationState.Pruning]] (`queuedCount == 0`, [[JointStageState.Pruning]]), prunes both rings, and then releases ownership (picking up any message submission that was enqueued while pruning was in progress by transitioning to [[JointStageState.SerializingOnly]]).
	 * @param targetLength The desired target number of buffers to retain per stage.
	 * @return The total number of buffers retained across both stages (or the current [[bufferInRingTotalCount]] if the pipeline was not idle). */
	def pruneTo(targetLength: RingLength): RingLength = {
		if coordinationState.compareAndSetCoordinationState(CoordinationState.Idle, CoordinationState.Pruning) then {
			val retained = RingLength(serializationStorage.pruneTo(targetLength) + transmissionStorage.pruneTo(targetLength))
			streamTransferBuffer = null
			streamChunkBuffers(1) = null
			var nextMsgSubmission: MsgSubmission | Null = null
			var becameIdle = false
			var released = false
			// Release temporary Pruning ownership; if a caller enqueued a message while we were pruning (queuedCount > 0), transition to SerializingOnly to process it.
			while !released do {
				val cur = coordinationState.getCoordinationState
				assert(cur.jointStageState == JointStageState.Pruning)
				if cur.queuedCount == 0 then {
					if coordinationState.compareAndSetCoordinationState(cur, CoordinationState.Idle) then {
						becameIdle = true
						released = true
					}
				} else if coordinationState.compareAndSetCoordinationState(cur, cur.withDecrementedQueue.withJointStageState(JointStageState.SerializingOnly)) then {
					nextMsgSubmission = pollQueuedMsgSubmission()
					released = true
				}
			}
			if becameIdle then onIdleHandler.getAndSet(noOpIdle)()
			if nextMsgSubmission ne null then {
				val t = nextMsgSubmission.nn
				runSerializationLoop(t.message, t.msgVersion, t.timeout, t.timeUnit, t.attachment, t.onComplete, t.serializer)
			}
			retained
		} else {
			bufferInRingTotalCount
		}
	}

	/** Returns the total number of buffers currently allocated across both [[serializationStorage]] and [[transmissionStorage]].
	 * May be called from any thread (reads `@volatile` buffer counts from both rings). */
	def bufferInRingTotalCount: RingLength = RingLength(serializationStorage.ringLength + transmissionStorage.ringLength)

	// ====================================================================================================
	// Pipeline Stage Orchestrators
	// ====================================================================================================

	/** Executes control-package serialization iteratively while the current thread retains serialization ownership (under [[JointStageState.SerializingOnly]] or [[JointStageState.Pipelined]]).
	 * Executed either by a caller thread (when [[transmitWithAttachment]] claims serialization ownership from [[JointStageState.Idle]] or [[JointStageState.TransmittingOnly]], or when [[pruneTo]] drains a concurrently queued message submission into [[JointStageState.SerializingOnly]]) or by an NIO.2 completion thread (at the end of [[onTransmissionFinished]] after draining a [[JointStageState.HandoffPending]] package while claiming a queued message submission into [[JointStageState.Pipelined]]).
	 * @param initialMessage The first message to serialize in this serialization loop.
	 * @param initialVersion The protocol version for `initialMessage`.
	 * @param initialTimeout The socket write timeout for `initialMessage`.
	 * @param initialTimeUnit The time unit for `initialTimeout`.
	 * @param initialAttachment The callback attachment for `initialMessage`.
	 * @param initialOnComplete The completion callback for `initialMessage`.
	 * @param initialSerializer The serializer for `initialMessage`. */
	private def runSerializationLoop(
		initialMessage: Any,
		initialVersion: ProtocolVersion,
		initialTimeout: Long,
		initialTimeUnit: TimeUnit,
		initialAttachment: Any,
		initialOnComplete: (Report, Any) => Unit,
		initialSerializer: Serializer[Any]
	): Unit = {
		var msg = initialMessage
		var ver = initialVersion
		var timeout = initialTimeout
		var timeUnit = initialTimeUnit
		var att = initialAttachment
		var onComplete = initialOnComplete
		var ser = initialSerializer
		var continue = true

		// Iterative loop on the active serialization thread: continues only when no transmission was in-flight (SerializingOnly), so this thread swapped and initiated transmission of the just-serialized message itself while another message submission was waiting in transmissionQueue (Case C below).
		while continue do {
			continue = false
			serializationTotalControlBytes = 0
			serializationNeedsSeparateSentinel = false
			serializationByteSources.clear()
			serializationWriter.prepare(ver)

			// Serialize the message completely into serializationStorage before any bytes of this message are sent to the socket.
			var failureReport: NotDelivered | Null = null
			try {
				ser.serialize(msg, serializationWriter)
			} catch {
				case se: SerializationException =>
					failureReport = SerializationProblem(msg, se)
				case NonFatal(e) =>
					failureReport = TransmissionFailure(msg, e)
			}

			if failureReport ne null then {
				// Serialization failed before touching the wire: discard any buffers written into `serializationStorage` and prune it back to `ringMinLengthOnIdle`.
				while serializationStorage.advanceReadEnd() do ()
				serializationStorage.writeEnd.clear()
				serializationByteSources.clear()
				serializationStorage.pruneTo(ringMinLengthOnIdle)
			} else {
				// Serialization succeeded: flip the final write-end buffer and inline or flag the package sentinel.
				serializationWriter.finishControlPackage(!serializationByteSources.isEmpty)
			}

			// Record the prepared package (or failure report) in serialization-stage slots. Routing failureReport through the standard serialization -> transmission handoff ensures callbacks always fire in strict FIFO submission order.
			serializationFailureReport = failureReport
			serializationMessage = msg
			serializationTimeout = timeout
			serializationTimeUnit = timeUnit
			serializationAttachment = att
			serializationOnComplete = onComplete

			var nextMsgSubmission: MsgSubmission | Null = null
			var mustTriggerTransmission = false
			var swappedForIdleTransmission = false
			// Atomic handoff at the end of serialization (executed on the active serialization thread):
			var wasStateChangedConcurrently = true
			while wasStateChangedConcurrently do {
				val cur = coordinationState.getCoordinationState
				val stageState = cur.jointStageState
				assert(stageState == JointStageState.Pipelined || stageState == JointStageState.SerializingOnly)
				// If transmission of the previous message is still in-flight (Pipelined), transition to HandoffPending and return.
				if stageState == JointStageState.Pipelined then {
					// Case A: Transmission is in-flight in parallel (`Pipelined`).
					// Transition to `HandoffPending` and return; the completing transmission thread will swap and transmit this package in `onTransmissionFinished` when the previous message completes.
					if coordinationState.compareAndSetCoordinationState(cur, cur.withJointStageState(JointStageState.HandoffPending)) then wasStateChangedConcurrently = false
				}
				// else (`SerializingOnly`: no transmission is in-flight)
				else {
					// At this point, under `SerializingOnly`, no other thread can access either the serialization-stage or transmission-stage variables, because no transmission chain is active and callers can neither start serialization, initiate transmission, nor enter `pruneTo`.

					// If not already swapped on a prior CAS attempt, hand off the serialization outcome to the transmission stage by swapping the buffer rings and byte-source queues (exposing the newly serialized buffers and sources for transmission and recycling the consumed transmission containers for the next serialization pass) and transferring the package metadata.
					if !swappedForIdleTransmission then {
						handoffSerializedOutcomeToTransmissionStage()
						swappedForIdleTransmission = true
					}
					// If there is no pending message to serialize next, then prune serializationStorage and transition to `TransmittingOnly` to initiate transmission.
					if cur.queuedCount == 0 then {
						// Case B: No transmission is in-flight (`SerializingOnly`) and `transmissionQueue` is empty.
						// Prune `serializationStorage` while still in `SerializingOnly` to uphold the pruned-serialization invariant of `TransmittingOnly`, then transition `coordinationState` to `TransmittingOnly` (`queuedCount == 0`) and schedule transmission initiation.
						serializationStorage.pruneTo(ringMinLengthOnIdle)
						// Transition to `TransmittingOnly` to allow callers to claim serialization on the pruned `serializationStorage`, and schedule initiation of transmission from `transmissionStorage`.
						if coordinationState.compareAndSetCoordinationState(cur, cur.withJointStageState(JointStageState.TransmittingOnly)) then {
							wasStateChangedConcurrently = false
							mustTriggerTransmission = true
						}
					}
					// else (a submission is pending) transition to `Pipelined`, decrement queue count, schedule transmission, and poll the next message.
					else {
						// Case C: No transmission is in-flight (`SerializingOnly`) and at least one more message is waiting in `transmissionQueue` (`queuedCount > 0`).
						// Transition to `Pipelined` and decrement `queuedCount`.
						if coordinationState.compareAndSetCoordinationState(cur, cur.withDecrementedQueue.withJointStageState(JointStageState.Pipelined)) then {
							wasStateChangedConcurrently = false
							// Set the order to trigger the transmission
							mustTriggerTransmission = true
							// Poll the next message. This causes this thread to loop and serialize the polled message.
							nextMsgSubmission = pollQueuedMsgSubmission()
						}
					}
				}
			}

			// If the order to trigger the transmission is set, then initiate the transmission before looping to serialize the next message.
			if mustTriggerTransmission then initiateTransmission()
			// If a message was polled, loop to serialize it in this same thread.
			if nextMsgSubmission ne null then {
				val t = nextMsgSubmission.nn
				msg = t.message
				ver = t.msgVersion
				timeout = t.timeout
				timeUnit = t.timeUnit
				att = t.attachment
				onComplete = t.onComplete
				ser = t.serializer
				continue = true
			}
		}
	}

	/** Initiates transmission processing for the package currently in [[transmissionStorage]] (or immediately completes with [[transmissionFailureReport]] if serialization failed).
	 * Executed either by the thread that completed [[runSerializationLoop]] (when transition occurred from [[JointStageState.SerializingOnly]] in Cases B/C) or by an NIO.2 completion thread inside [[onTransmissionFinished]] (when a staged package was claimed from [[JointStageState.HandoffPending]] in Cases A/B). */
	private def initiateTransmission(): Unit = {
		if transmissionFailureReport ne null then {
			// Serialization failed: report the failure in FIFO order without writing any bytes to the socket channel.
			val report = transmissionFailureReport.nn
			transmissionFailureReport = null
			onTransmissionFinished(report)
		} else {
			// Encode the single-frame VLQ length header covering the entire control package across all buffers in transmissionStorage.
			if transmissionTotalControlBytes > 0 then {
				frameHeaderBuffer.clear()
				VLQ.encodeUnsignedInt(transmissionTotalControlBytes, transmissionControlWriteHandler)
				frameHeaderBuffer.flip()
			} else {
				// If a serializer produced 0 payload bytes, omit the frame header so the sentinel byte alone is written.
				frameHeaderBuffer.clear()
				frameHeaderBuffer.limit(0)
			}

			// Prepare the standalone sentinel buffer only if the sentinel did not fit at the tail of the last storage buffer.
			if transmissionNeedsSeparateSentinel then {
				sentinelBuffer.clear()
				if transmissionByteSources.isEmpty then sentinelBuffer.put(0: Byte)
				else sentinelBuffer.put(STREAM_FOLLOWS_SENTINEL_ENCODED)
				sentinelBuffer.flip()
			}

			transmissionHasUnreadStorageBuffers = true
			transmissionSeparateSentinelPending = transmissionNeedsSeparateSentinel
			fillAndSendNextControlBatch()
		}
	}

	/** Finalizes transmission of the current message, resets [[transmissionStorage]], performs the atomic state handoff via [[coordinationState]] and [[transmissionQueue]], invokes the completed message's callback in FIFO order, and initiates any subsequent transmission or serialization.
	 * Executed on an NIO.2 completion thread (when control or trailing stream writes finish asynchronously) or on the serialization thread (when [[transmissionFailureReport]] short-circuits in [[initiateTransmission]] or when [[java.nio.channels.AsynchronousSocketChannel.write]] on [[channel]] fails/completes synchronously).
	 * @param report The final [[PipelinedTransmitter.Report]] ([[TransmissionProtocol.Delivered]] or [[TransmissionProtocol.NotDelivered]]) for the message that just finished transmission. */
	private def onTransmissionFinished(report: Report): Unit = {
		// Reset transmissionStorage read/write pointers so it is clean and ready to be swapped back as serialization storage or pruned.
		while transmissionStorage.advanceReadEnd() do ()
		transmissionStorage.writeEnd.clear()
		transmissionByteSources.clear()
		val completedMsg = transmissionMessage
		val completedOnComplete = transmissionOnComplete
		val completedAttachment = transmissionAttachment
		transmissionMessage = null
		transmissionAttachment = null
		transmissionOnComplete = null

		scribe.trace(s"Transmission progress of message `$completedMsg` from `${context.myAddress}` to `${context.showPeerAddress}`: completed with report=$report")

		var nextMsgSubmission: MsgSubmission | Null = null
		var startTransmission = false
		var transitionedToIdle = false
		var done = false

		// Atomic handoff at the end of transmission (executed on the finishing transmission thread):
		while !done do {
			val cur = coordinationState.getCoordinationState
			val stageState = cur.jointStageState
			assert(stageState == JointStageState.HandoffPending || stageState == JointStageState.Pipelined || stageState == JointStageState.TransmittingOnly)

			if stageState == JointStageState.HandoffPending then {
				// Another thread has already finished serializing the next message and transitioned to `HandoffPending`.
				// Because `HandoffPending` can only be exited by this finishing transmission thread, this thread exclusively owns both serialization and transmission stage variables and swaps their rings immediately.
				handoffSerializedOutcomeToTransmissionStage()
				var swapped = false
				while !swapped do {
					val c2 = coordinationState.getCoordinationState
					assert(c2.jointStageState == JointStageState.HandoffPending)
					if c2.queuedCount == 0 then {
						// Case A: `HandoffPending` and `transmissionQueue` is empty.
						// Uphold the pruned-serialization invariant of `TransmittingOnly` by pruning the newly swapped `serializationStorage` before transitioning to `TransmittingOnly`.
						serializationStorage.pruneTo(ringMinLengthOnIdle)
						if coordinationState.compareAndSetCoordinationState(c2, c2.withJointStageState(JointStageState.TransmittingOnly)) then {
							startTransmission = true
							swapped = true
						}
					} else {
						// Case B: `HandoffPending` and at least one unserialized message is waiting in `transmissionQueue` (`queuedCount > 0`).
						// Transition to `Pipelined`, decrement `queuedCount`, and poll the next message submission so this thread will both start transmission for message N+1 and serialize message N+2.
						if coordinationState.compareAndSetCoordinationState(c2, c2.withDecrementedQueue.withJointStageState(JointStageState.Pipelined)) then {
							startTransmission = true
							nextMsgSubmission = pollQueuedMsgSubmission()
							swapped = true
						}
					}
				}
				done = true
			} else if stageState == JointStageState.Pipelined then {
				// Case C: Another thread is actively serializing the next message (`Pipelined`).
				// Transition to `SerializingOnly` and exit; the active serialization thread will observe `SerializingOnly` when it finishes serializing (in `runSerializationLoop` Cases B/C) and initiate transmission itself.
				if coordinationState.compareAndSetCoordinationState(cur, cur.withJointStageState(JointStageState.SerializingOnly)) then done = true
			} else {
				// Case D: `TransmittingOnly` (which implies `queuedCount == 0` and `serializationStorage` is already pruned to `ringMinLengthOnIdle`).
				// Prune `transmissionStorage` and release the lazy `streamTransferBuffer`, then CAS to `CoordinationState.Idle`.
				assert(cur.queuedCount == 0)
				transmissionStorage.pruneTo(ringMinLengthOnIdle)
				streamTransferBuffer = null
				streamChunkBuffers(1) = null
				if coordinationState.compareAndSetCoordinationState(cur, CoordinationState.Idle) then {
					transitionedToIdle = true
					done = true
				}
			}
		}

		// If the pipeline just reached CoordinationState.Idle, fire any registered onIdle callback once.
		if transitionedToIdle then onIdleHandler.getAndSet(noOpIdle)()

		// Deliver the completed message's callback in strict FIFO order before starting the next message's transmission, and finally run serialization if a queued message submission was claimed in Case B.
		try {
			if startTransmission then {
				try completedOnComplete(report, completedAttachment)
				finally initiateTransmission()
			} else {
				completedOnComplete(report, completedAttachment)
			}
		} finally {
			if nextMsgSubmission ne null then {
				val t = nextMsgSubmission.nn
				runSerializationLoop(t.message, t.msgVersion, t.timeout, t.timeUnit, t.attachment, t.onComplete, t.serializer)
			}
		}
	}

	// ====================================================================================================
	// Stage Mechanics & Completion Handlers
	// ====================================================================================================

	/** Reusable [[cluster.serialization.Serializer.Writer]] bound to [[serializationStorage]] and [[serializationByteSources]].
	 * All methods on this object are executed exclusively by the thread currently holding serialization ownership inside [[runSerializationLoop]]. */
	private object serializationWriter extends Writer(deduplicationMap) {
		/** Protocol version governing the current serialization pass.
		 * Set by the serialization thread in [[prepare]]. */
		private var currentMsgVersion: ProtocolVersion = ProtocolVersion.NOT_SPECIFIED

		/** Cached reference to the current write-end buffer of [[serializationStorage]].
		 * Initialized in [[prepare]] and advanced in [[getBuffer]] by the serialization thread. */
		private var writeEndBuffer: ByteBuffer = null

		/** Resets writer state, deduplication table, escaping flag, and initial write-end buffer for a new serialization pass.
		 * Executed by the serialization thread at the start of each iteration in [[runSerializationLoop]].
		 * @param msgVersion The protocol version governing the message about to be serialized. */
		def prepare(msgVersion: ProtocolVersion): Unit = {
			currentMsgVersion = msgVersion
			resetDeduplication()
			writeEndBuffer = serializationStorage.writeEnd
			writeEndBuffer.clear()
		}

		/** Serializes an out-of-line [[readren.common.ByteSource]] by writing its declared byte size inline as an unsigned VLQ long and enqueueing non-empty sources into [[serializationByteSources]] for trailing stream transmission.
		 * Executed by the serialization thread during [[cluster.serialization.Serializer.serialize]].
		 * @param source The [[readren.common.ByteSource]] encountered in the message graph.
		 * @throws cluster.serialization.Serializer.SerializationException if [[readren.common.ByteSource.size]] is negative. */
		override def putByteSource(source: ByteSource): Unit = {
			val size = source.size
			if size < 0 then throw new SerializationException(writeEndBuffer.position(), s"ByteSource size cannot be negative ($size)")
			putUnsignedLongVlq(size)
			if size > 0 then serializationByteSources.add(source)
		}

		/** Obtains a [[java.nio.ByteBuffer]] having at least `minimumRemaining` bytes available, flipping full buffers, accumulating [[serializationTotalControlBytes]], and advancing [[serializationStorage]] via [[cluster.misc.DualEndedCircularStorage.advanceWriteEnd]] when needed.
		 * Executed by the serialization thread during [[cluster.serialization.Serializer.serialize]].
		 * @param minimumRemaining The minimum number of contiguous bytes required for the next primitive or chunk write.
		 * @return A [[java.nio.ByteBuffer]] positioned for writing with at least `minimumRemaining` bytes available.
		 * @throws cluster.channel.RingLengthExceededException if advancing [[serializationStorage]] would exceed [[ringMaxLength]]. */
		protected override def getBuffer(minimumRemaining: Int): ByteBuffer = {
			if writeEndBuffer.remaining() < minimumRemaining then {
				// Record the payload bytes written into the current buffer and flip it so gathering writes can read it directly during transmission.
				serializationTotalControlBytes += writeEndBuffer.position()
				writeEndBuffer.flip()
				serializationStorage.advanceWriteEnd().fold {
					throw new RingLengthExceededException(s"Message transmission exceeded maximum buffer capacity ($ringMaxLength buffers of $capacityOfBufferInRing bytes)")
				} { nextBuffer =>
					nextBuffer.clear()
					writeEndBuffer = nextBuffer
				}
			}
			writeEndBuffer
		}

		/** Returns the protocol version governing the active serialization pass.
		 * Executed by the serialization thread during [[cluster.serialization.Serializer.serialize]]. */
		override def governingVersion: ProtocolVersion = currentMsgVersion

		/** Returns the current write cursor position within the active [[writeEndBuffer]].
		 * Executed by the serialization thread during [[cluster.serialization.Serializer.serialize]]. */
		override def position: Int = writeEndBuffer.position()

		/** Finalizes the serialized control package in [[serializationStorage]] by adding the final buffer's payload bytes to [[serializationTotalControlBytes]] and either appending the package sentinel directly into [[writeEndBuffer]] (if capacity permits) or setting [[serializationNeedsSeparateSentinel]] to `true`.
		 * Executed by the serialization thread in [[runSerializationLoop]] immediately after [[cluster.serialization.Serializer.serialize]] returns normally.
		 * @param hasByteSources `true` if one or more non-empty [[readren.common.ByteSource]] instances were queued in [[serializationByteSources]] (requiring [[TransmissionProtocol.STREAM_FOLLOWS_SENTINEL_ENCODED]]), or `false` if a single `0x00` byte terminator suffices. */
		def finishControlPackage(hasByteSources: Boolean): Unit = {
			serializationTotalControlBytes += writeEndBuffer.position()
			val sentinelLength = if hasByteSources then STREAM_FOLLOWS_SENTINEL_ENCODED.length else 1
			if writeEndBuffer.remaining() >= sentinelLength then {
				// Inline the sentinel at the tail of the last content buffer so no extra ByteBuffer slot is needed during gathering writes.
				if hasByteSources then writeEndBuffer.put(STREAM_FOLLOWS_SENTINEL_ENCODED)
				else writeEndBuffer.put(0: Byte)
				writeEndBuffer.flip()
				serializationNeedsSeparateSentinel = false
			} else {
				// Not enough tail room in the last content buffer; fillAndSendNextControlBatch will append sentinelBuffer as an extra gathering buffer during transmission.
				writeEndBuffer.flip()
				serializationNeedsSeparateSentinel = true
			}
		}
	}

	/** Populates [[gatheringBatch]] with up to [[maxGatheringBuffers]] buffers (starting with [[frameHeaderBuffer]] on the first batch, followed by unread buffers from [[transmissionStorage]], and ending with [[sentinelBuffer]] when needed) and submits a non-blocking gathering [[java.nio.channels.AsynchronousSocketChannel.write]] on [[channel]].
	 * Executed by the thread initiating transmission in [[initiateTransmission]] for the first batch, and by an NIO.2 completion thread in [[transmissionControlWriteHandler.completed]] for subsequent batches when a message spans more than [[maxGatheringBuffers]] buffers. */
	private def fillAndSendNextControlBatch(): Unit = {
		batchOffset = 0
		var idx = 0
		// Include the single-frame VLQ length header at slot 0 of the first gathering batch.
		if frameHeaderBuffer.hasRemaining then {
			gatheringBatch(idx) = frameHeaderBuffer
			idx += 1
		}
		// Pull unread payload buffers from transmissionStorage up to maxGatheringBuffers.
		while idx < maxGatheringBuffers && transmissionHasUnreadStorageBuffers do {
			val readBuf = transmissionStorage.readEnd
			if readBuf.hasRemaining then {
				gatheringBatch(idx) = readBuf
				idx += 1
			}
			transmissionHasUnreadStorageBuffers = transmissionStorage.advanceReadEnd()
		}
		// Append the standalone sentinelBuffer at the end of the final batch if it is still pending and a slot remains in gatheringBatch.
		if !transmissionHasUnreadStorageBuffers && transmissionSeparateSentinelPending && idx < maxGatheringBuffers then {
			gatheringBatch(idx) = sentinelBuffer
			idx += 1
			transmissionSeparateSentinelPending = false
		}
		batchLimit = idx
		assert(batchLimit > 0)
		scribe.trace(s"Transmission progress of message `$transmissionMessage` from `${context.myAddress}` to `${context.showPeerAddress}`: sending gathering batch of $batchLimit buffers (controlBytes=$transmissionTotalControlBytes, hasMoreStorage=$transmissionHasUnreadStorageBuffers, separateSentinelPending=$transmissionSeparateSentinelPending)")
		try {
			channel.write(gatheringBatch, 0, batchLimit, transmissionTimeout, transmissionTimeUnit, null, transmissionControlWriteHandler)
		} catch {
			case NonFatal(e) =>
				clearBatchReferences()
				onTransmissionFinished(TransmissionFailure(transmissionMessage, e))
		}
	}

	/** NIO.2 [[java.nio.channels.CompletionHandler]] and [[cluster.misc.VLQ.ByteWriter]] governing bounded gathering socket writes and single-frame VLQ header encoding for the transmitted control package.
	 * [[transmissionControlWriteHandler.putByte]] is executed by the thread starting transmission in [[initiateTransmission]]; [[transmissionControlWriteHandler.completed]] and [[transmissionControlWriteHandler.failed]] are executed by an NIO.2 channel-group completion thread. */
	private object transmissionControlWriteHandler extends CompletionHandler[java.lang.Long, Null], VLQ.ByteWriter {
		/** Appends a single VLQ-encoded frame-length byte into [[frameHeaderBuffer]].
		 * Executed by the thread starting transmission inside [[initiateTransmission]].
		 * @param byte The VLQ-encoded byte to write. */
		override def putByte(byte: Byte): Unit = frameHeaderBuffer.put(byte)

		/** Advances [[batchOffset]] past fully drained buffers in [[gatheringBatch]], re-submitting partial batches, advancing to the next batch of [[transmissionStorage]], or transitioning to trailing stream / completion.
		 * Executed on an NIO.2 completion thread when a gathering [[java.nio.channels.AsynchronousSocketChannel.write]] operation on [[channel]] completes.
		 * @param bytesWritten The number of bytes written to [[channel]] during the completed write call.
		 * @param att Unused attachment (`null`). */
		override def completed(bytesWritten: java.lang.Long, att: Null): Unit = {
			// Advance batchOffset past all buffers in the current batch that were completely drained by the OS socket write.
			while batchOffset < batchLimit && !gatheringBatch(batchOffset).hasRemaining do {
				gatheringBatch(batchOffset) = null
				batchOffset += 1
			}
			if batchOffset < batchLimit then {
				// Partial gathering write: the OS send buffer accepted only a prefix of the current batch; re-submit the remaining slice of gatheringBatch.
				try {
					channel.write(gatheringBatch, batchOffset, batchLimit - batchOffset, transmissionTimeout, transmissionTimeUnit, null, this)
				} catch {
					case NonFatal(e) =>
						clearBatchReferences()
						onTransmissionFinished(TransmissionFailure(transmissionMessage, e))
				}
			} else {
				// Current batch of buffers is completely written.
				batchOffset = 0
				batchLimit = 0
				if transmissionHasUnreadStorageBuffers || transmissionSeparateSentinelPending then {
					// More payload buffers in transmissionStorage (or the standalone sentinelBuffer) remain to be sent for this single frame; send the next batch.
					fillAndSendNextControlBatch()
				} else if transmissionByteSources.isEmpty then {
					// Control package is complete and no out-of-line ByteSources were queued.
					onTransmissionFinished(Delivered)
				} else {
					// Control package ended with STREAM_FOLLOWS_SENTINEL; begin streaming queued ByteSource payloads.
					startTrailingStream()
				}
			}
		}

		/** Cleans up [[gatheringBatch]] and finishes transmission with a [[TransmissionProtocol.TransmissionFailure]] when a socket write fails.
		 * Executed on an NIO.2 completion thread.
		 * @param exc The I/O exception reported by [[channel]].
		 * @param att Unused attachment (`null`). */
		override def failed(exc: Throwable, att: Null): Unit = {
			clearBatchReferences()
			onTransmissionFinished(TransmissionFailure(transmissionMessage, exc))
		}
	}

	/** Initializes trailing stream state, binds the lazily allocated [[streamTransferBuffer]] (via [[ensureStreamTransferBuffer]]) into [[streamChunkBuffers]], and starts [[runTrailingStreamLoop]].
	 * Executed on an NIO.2 completion thread from [[transmissionControlWriteHandler.completed]] immediately after the control package's [[TransmissionProtocol.STREAM_FOLLOWS_SENTINEL_ENCODED]] has been written. */
	private def startTrailingStream(): Unit = {
		streamCancellationReason = null
		currentStreamByteSource = null
		currentAsyncReader = null
		currentStreamBytesRead = 0L
		currentStreamDeclaredSize = 0L
		transmissionCompletedReadBytes = 0
		transmissionStreamFailure = null
		transmissionWritingSentinel = false
		transmissionStreamStep = STREAM_STEP_NEXT_SOURCE
		streamChunkBuffers(1) = ensureStreamTransferBuffer()
		transmissionStreamWorkPermits.set(0)
		runTrailingStreamLoop()
	}

	/** Drives the trailing stream state machine iteratively across queued [[readren.common.ByteSource]] instances using [[transmissionStreamWorkPermits]] to serialize synchronous, inline-callback, and multi-threaded asynchronous completions in $O(1)$ stack depth.
	 * Executed initially on the NIO.2 completion thread from [[startTrailingStream]], and resumed on NIO.2 or [[readren.common.ByteSource.AsyncReader]] completion threads whenever a reader or socket write callback fires.
	 * @note Trampoline necessity vs. SPI contract: This trampoline removes any assumption about how [[readren.common.ByteSource.AsyncReader]] or [[channel]] dispatches completions, keeping stack depth $O(1)$ even if an `AsyncReader` or a custom/mock [[java.nio.channels.AsynchronousSocketChannel]] invokes [[java.nio.channels.CompletionHandler]] inline before returning `PENDING`. Without this trampoline, a direct tail-recursive/looping implementation would require [[readren.common.ByteSource.AsyncReader]] never to invoke `CompletionHandler` inline before returning [[readren.common.ByteSource.AsyncReader.PENDING]] (reporting synchronous completions strictly via the `Int` return value) and would rely on [[java.nio.channels.AsynchronousSocketChannel]] bounding consecutive inline callback depth. */
	private def runTrailingStreamLoop(): Unit = {
		if transmissionStreamWorkPermits.getAndIncrement() == 0 then {
			var terminalReport: Report | Null = null
			while {
				terminalReport = stepTrailingStream()
				if terminalReport ne null then {
					transmissionStreamWorkPermits.set(0)
					false
				} else {
					transmissionStreamWorkPermits.decrementAndGet() > 0
				}
			} do ()
			if terminalReport ne null then onTransmissionFinished(terminalReport.nn)
		}
	}

	/** Executes a single serialized transition of the trailing stream state machine according to [[transmissionStreamStep]].
	 * Executed exclusively by the single thread holding ownership of [[transmissionStreamWorkPermits]] inside [[runTrailingStreamLoop]].
	 * @return The terminal [[PipelinedTransmitter.Report]] when the trailing stream has completely finished or failed on the socket, or `null` when the stream remains in progress. */
	private def stepTrailingStream(): Report | Null = {
		(transmissionStreamStep: @scala.annotation.switch) match {
			case STREAM_STEP_NEXT_SOURCE =>
				if transmissionByteSources.isEmpty then {
					// All queued ByteSources have been completely streamed; write the terminal 0-length chunk header (0x00).
					streamChunkHeaderBuffer.clear()
					streamChunkHeaderBuffer.put(0: Byte)
					streamChunkHeaderBuffer.flip()
					sendStreamWrite(streamChunkBuffers, 0, 1, sentinelIsWritten = true)
				} else {
					// Poll the next queued ByteSource and open its AsyncReader session.
					val source = transmissionByteSources.poll()
					currentStreamByteSource = source
					currentStreamDeclaredSize = source.size
					currentStreamBytesRead = 0L
					var readerOpened = false
					try {
						currentAsyncReader = source.openAsyncReader()
						readerOpened = true
					} catch {
						case NonFatal(e) =>
							abortTrailingStream(e)
					}
					if readerOpened then {
						transmissionStreamStep = STREAM_STEP_READ_SOURCE
						transmissionStreamWorkPermits.incrementAndGet()
					}
				}
				null

			case STREAM_STEP_READ_SOURCE =>
				// Read the next chunk from currentAsyncReader into streamTransferBuffer.
				val transferBuf = streamTransferBuffer.nn
				transferBuf.clear()
				// Transition to STREAM_STEP_AWAITING_READ before calling read() so that an inline or fast-async callback in byteSourceReadHandler can safely overwrite transmissionStreamStep with STREAM_STEP_PROCESS_READ.
				transmissionStreamStep = STREAM_STEP_AWAITING_READ
				val readStatus = try {
					currentAsyncReader.nn.read(transferBuf, null, byteSourceReadHandler)
				} catch {
					case NonFatal(e) =>
						transmissionStreamFailure = e
						transmissionStreamStep = STREAM_STEP_READ_FAILED
						transmissionStreamWorkPermits.incrementAndGet()
						ByteSource.AsyncReader.PENDING
				}
				// If readStatus != PENDING, byteSourceReadHandler was not invoked; stage the synchronous result and schedule the next step on the trampoline.
				if readStatus != ByteSource.AsyncReader.PENDING then {
					transmissionCompletedReadBytes = readStatus
					transmissionStreamStep = STREAM_STEP_PROCESS_READ
					transmissionStreamWorkPermits.incrementAndGet()
				}
				null

			case STREAM_STEP_PROCESS_READ =>
				handleByteSourceChunkRead(transmissionCompletedReadBytes)
				null

			case STREAM_STEP_READ_FAILED =>
				val cause = transmissionStreamFailure.nn
				transmissionStreamFailure = null
				abortTrailingStream(cause)
				null

			case STREAM_STEP_PROCESS_WRITE =>
				if streamChunkBuffers(0).hasRemaining then {
					// Partial write in the 5-byte VLQ chunk header buffer; re-submit starting at slot 0.
					sendStreamWrite(streamChunkBuffers, 0, if transmissionWritingSentinel then 1 else 2, transmissionWritingSentinel)
					null
				} else if !transmissionWritingSentinel && streamChunkBuffers(1).nn.hasRemaining then {
					// Chunk header finished, but payload bytes remain in streamTransferBuffer (slot 1); re-submit slot 1 alone.
					sendStreamWrite(streamChunkBuffers, 1, 1, sentinelIsWritten = false)
					null
				} else if transmissionWritingSentinel then {
					// Terminal 0x00 chunk header or CORRUPTED_PACKAGE_SENTINEL has been completely written; finalize transmission.
					cleanupStream()
					transmissionStreamStep = STREAM_STEP_IDLE
					if streamCancellationReason ne null then streamCancellationReason.nn else Delivered
				} else {
					// Data chunk completely written; advance to reading the next chunk from currentAsyncReader.
					transmissionStreamStep = STREAM_STEP_READ_SOURCE
					transmissionStreamWorkPermits.incrementAndGet()
					null
				}

			case STREAM_STEP_WRITE_FAILED =>
				val exc = transmissionStreamFailure.nn
				transmissionStreamFailure = null
				cleanupStream()
				transmissionStreamStep = STREAM_STEP_IDLE
				TransmissionFailure(transmissionMessage, exc)

			case _ =>
				null
		}
	}

	/** Processes the result of reading a chunk from [[currentAsyncReader]] into [[streamTransferBuffer]], verifying declared byte bounds and either writing the VLQ-framed chunk to [[channel]] or advancing to the next [[readren.common.ByteSource]] on EOF.
	 * Executed exclusively by the trampoline owner thread inside [[stepTrailingStream]] when [[transmissionStreamStep]] is [[PipelinedTransmitter.STREAM_STEP_PROCESS_READ]].
	 * @param readBytes The number of bytes transferred into [[streamTransferBuffer]] (`> 0`), or `<= 0` on EOF. */
	private def handleByteSourceChunkRead(readBytes: Int): Unit = {
		if readBytes > 0 then {
			currentStreamBytesRead += readBytes
			if currentStreamBytesRead > currentStreamDeclaredSize then {
				// The ByteSource produced more bytes than its declared size in the control package; abort cleanly with CORRUPTED_PACKAGE_SENTINEL.
				abortTrailingStream(new java.io.IOException(s"ByteSource size drift: read $currentStreamBytesRead bytes exceeding declared $currentStreamDeclaredSize"))
			} else {
				// Flip the transfer buffer, encode the chunk length as a VLQ header, and write both buffers in a 2-buffer gathering write.
				streamTransferBuffer.nn.flip()
				streamChunkHeaderBuffer.clear()
				VLQ.encodeUnsignedInt(readBytes, trailingStreamTransmissionHandler)
				streamChunkHeaderBuffer.flip()
				sendStreamWrite(streamChunkBuffers, 0, 2, sentinelIsWritten = false)
			}
		} else {
			// Reached EOF on currentAsyncReader; verify that the total bytes read match the exact size declared in the control package.
			cleanupCurrentReader()
			if currentStreamBytesRead != currentStreamDeclaredSize then {
				abortTrailingStream(new java.io.EOFException(s"Premature EOF for ByteSource: expected $currentStreamDeclaredSize bytes, got $currentStreamBytesRead"))
			} else {
				currentStreamByteSource = null
				transmissionStreamStep = STREAM_STEP_NEXT_SOURCE
				transmissionStreamWorkPermits.incrementAndGet()
			}
		}
	}

	/** Aborts the active trailing stream at a clean chunk boundary by closing the reader, recording a [[TransmissionProtocol.ByteSourceProblem]], and writing [[TransmissionProtocol.CORRUPTED_PACKAGE_SENTINEL_ENCODED]] in place of the next chunk length header.
	 * Executed exclusively by the trampoline owner thread inside [[stepTrailingStream]]. Because the sentinel is emitted at a chunk header boundary, the remote [[Receiver]] can discard its staging sinks and skip the aborted package without closing the socket channel.
	 * @param cause The exception encountered while opening or reading from [[currentStreamByteSource]]. */
	private def abortTrailingStream(cause: Throwable): Unit = {
		cleanupStream()
		streamCancellationReason = ByteSourceProblem(transmissionMessage, cause)
		streamChunkHeaderBuffer.clear()
		streamChunkHeaderBuffer.put(CORRUPTED_PACKAGE_SENTINEL_ENCODED)
		streamChunkHeaderBuffer.flip()
		sendStreamWrite(streamChunkBuffers, 0, 1, sentinelIsWritten = true)
	}

	/** Submits an asynchronous socket write on [[channel]] for a trailing stream chunk or terminal stream sentinel, catching synchronous channel exceptions and routing them into [[transmissionStreamStep]].
	 * Executed exclusively by the trampoline owner thread inside [[stepTrailingStream]].
	 * @param buffers The buffer array ([[streamChunkBuffers]]) containing the chunk header and optional chunk payload.
	 * @param offset The starting index in `buffers` (`0` when header bytes remain, `1` when only payload bytes remain).
	 * @param length The number of buffers to write (`1` or `2`).
	 * @param sentinelIsWritten `true` if this write transmits the terminal `0x00` chunk header or [[TransmissionProtocol.CORRUPTED_PACKAGE_SENTINEL_ENCODED]]; `false` for normal data chunks. */
	private def sendStreamWrite(buffers: Array[ByteBuffer], offset: Int, length: Int, sentinelIsWritten: Boolean): Unit = {
		transmissionWritingSentinel = sentinelIsWritten
		transmissionStreamStep = STREAM_STEP_AWAITING_WRITE
		try {
			channel.write(buffers, offset, length, transmissionTimeout, transmissionTimeUnit, sentinelIsWritten, trailingStreamTransmissionHandler)
		} catch {
			case NonFatal(e) =>
				transmissionStreamFailure = e
				transmissionStreamStep = STREAM_STEP_WRITE_FAILED
				transmissionStreamWorkPermits.incrementAndGet()
		}
	}

	/** Completion handler passed to [[readren.common.ByteSource.AsyncReader.read]] for asynchronous chunk reads into [[streamTransferBuffer]].
	 * Executed on a [[readren.common.ByteSource.AsyncReader]] completion thread (e.g., an [[java.nio.channels.AsynchronousFileChannel]] worker thread) or inline during [[readren.common.ByteSource.AsyncReader.read]]. */
	private object byteSourceReadHandler extends CompletionHandler[Integer, Null] {
		/** Stages the completed read byte count into [[transmissionCompletedReadBytes]], transitions [[transmissionStreamStep]] to [[PipelinedTransmitter.STREAM_STEP_PROCESS_READ]], and schedules the trailing stream trampoline via [[runTrailingStreamLoop]].
		 * Executed on an [[readren.common.ByteSource.AsyncReader]] completion thread or inline before [[readren.common.ByteSource.AsyncReader.read]] returns [[readren.common.ByteSource.AsyncReader.PENDING]].
		 * @param readBytes The number of bytes read into [[streamTransferBuffer]], or `-1` on EOF.
		 * @param att Unused attachment (`null`). */
		override def completed(readBytes: Integer, att: Null): Unit = {
			transmissionCompletedReadBytes = readBytes.intValue()
			transmissionStreamStep = STREAM_STEP_PROCESS_READ
			runTrailingStreamLoop()
		}

		/** Stages the reader failure into [[transmissionStreamFailure]], transitions [[transmissionStreamStep]] to [[PipelinedTransmitter.STREAM_STEP_READ_FAILED]], and schedules the trailing stream trampoline via [[runTrailingStreamLoop]].
		 * Executed on an [[readren.common.ByteSource.AsyncReader]] completion thread or inline before [[readren.common.ByteSource.AsyncReader.read]] returns [[readren.common.ByteSource.AsyncReader.PENDING]].
		 * @param exc The I/O exception thrown by the asynchronous reader.
		 * @param att Unused attachment (`null`). */
		override def failed(exc: Throwable, att: Null): Unit = {
			transmissionStreamFailure = exc
			transmissionStreamStep = STREAM_STEP_READ_FAILED
			runTrailingStreamLoop()
		}
	}

	/** NIO.2 [[java.nio.channels.CompletionHandler]] and [[cluster.misc.VLQ.ByteWriter]] governing socket writes of trailing stream chunks, stream-terminating sentinels, and VLQ chunk-length header encoding.
	 * [[trailingStreamTransmissionHandler.putByte]] is executed by the trailing stream trampoline owner thread in [[handleByteSourceChunkRead]]; [[trailingStreamTransmissionHandler.completed]] and [[trailingStreamTransmissionHandler.failed]] are executed on an NIO.2 channel-group completion thread (or inline on the calling thread if the channel completes writes synchronously). */
	private object trailingStreamTransmissionHandler extends CompletionHandler[java.lang.Long, Boolean], VLQ.ByteWriter {
		/** Appends a single VLQ-encoded chunk-length byte into [[streamChunkHeaderBuffer]].
		 * Executed exclusively by the trailing stream trampoline owner thread inside [[handleByteSourceChunkRead]].
		 * @param byte The VLQ-encoded byte to write. */
		override def putByte(byte: Byte): Unit = streamChunkHeaderBuffer.put(byte)

		/** Transitions [[transmissionStreamStep]] to [[PipelinedTransmitter.STREAM_STEP_PROCESS_WRITE]] and schedules [[runTrailingStreamLoop]] to continue partial writes, finalize the stream, or read the next chunk.
		 * Executed on an NIO.2 completion thread.
		 * @param bytesWritten The number of bytes written to [[channel]] during the completed write operation.
		 * @param sentinelIsWritten `true` if this write operation was transmitting the terminal stream sentinel (`0x00` or abort sentinel); `false` if transmitting a data chunk. */
		override def completed(bytesWritten: java.lang.Long, sentinelIsWritten: Boolean): Unit = {
			transmissionStreamStep = STREAM_STEP_PROCESS_WRITE
			runTrailingStreamLoop()
		}

		/** Stages the socket failure in [[transmissionStreamFailure]], transitions [[transmissionStreamStep]] to [[PipelinedTransmitter.STREAM_STEP_WRITE_FAILED]], and schedules [[runTrailingStreamLoop]].
		 * Executed on an NIO.2 completion thread.
		 * @param exc The network I/O exception reported by [[channel]].
		 * @param sentinelIsWritten Whether the failed write was attempting to send a terminal sentinel. */
		override def failed(exc: Throwable, sentinelIsWritten: Boolean): Unit = {
			transmissionStreamFailure = exc
			transmissionStreamStep = STREAM_STEP_WRITE_FAILED
			runTrailingStreamLoop()
		}
	}

	// ====================================================================================================
	// Low-Level State & Cleanup Helpers
	// ====================================================================================================

	/** Swaps the buffer rings and byte-source queues (the filled: write/serializationStorage -> read/transmissionStorage; and the consumed: read/transmissionStorage -> write/serializationStorage), and transfers serialization package metadata into the transmission fields.
	 * Executed either by the serialization thread in [[runSerializationLoop]] (under [[JointStageState.SerializingOnly]]) or by the transmission completion thread in [[onTransmissionFinished]] (under [[JointStageState.HandoffPending]]).
	 * @note Safety invariant: Invoked only when the calling thread simultaneously holds exclusive ownership of both serialization and transmission stage variables, requiring zero internal synchronization inside [[cluster.misc.DualEndedCircularStorage]]. */
	private def handoffSerializedOutcomeToTransmissionStage(): Unit = {
		val tmpStorage = serializationStorage
		serializationStorage = transmissionStorage
		transmissionStorage = tmpStorage

		val tmpSources = serializationByteSources
		serializationByteSources = transmissionByteSources
		transmissionByteSources = tmpSources

		transmissionTotalControlBytes = serializationTotalControlBytes
		transmissionNeedsSeparateSentinel = serializationNeedsSeparateSentinel
		transmissionFailureReport = serializationFailureReport
		transmissionMessage = serializationMessage
		transmissionTimeout = serializationTimeout
		transmissionTimeUnit = serializationTimeUnit
		transmissionAttachment = serializationAttachment
		transmissionOnComplete = serializationOnComplete

		serializationFailureReport = null
		serializationMessage = null
		serializationAttachment = null
		serializationOnComplete = null
	}

	/** Polls the next [[MsgSubmission]] from [[transmissionQueue]] after a consumer thread has atomically decremented the queued count in [[coordinationState]].
	 * Executed by whichever thread (caller thread, NIO.2 completion thread, or [[pruneTo]] caller) won the CAS decrementing `queuedCount`. Because [[transmitWithAttachment]] increments `queuedCount` via CAS immediately before invoking [[java.util.concurrent.ConcurrentLinkedQueue.add]] on [[transmissionQueue]], there is a brief window where the queue node link is not yet visible; spinning with [[java.lang.invoke.VarHandle.fullFence]] resolves this memory visibility gap without locks.
	 * @return The non-null [[PipelinedTransmitter.MsgSubmission]] at the head of [[transmissionQueue]]. */
	private def pollQueuedMsgSubmission(): MsgSubmission = {
		var next = transmissionQueue.poll()
		while next eq null do {
			VarHandle.fullFence()
			next = transmissionQueue.poll()
		}
		next
	}

	/** Clears all buffer references in [[gatheringBatch]] and resets [[batchOffset]] and [[batchLimit]] upon write failure.
	 * Executed by the thread initiating [[java.nio.channels.AsynchronousSocketChannel.write]] on [[channel]] (on synchronous throw) or by an NIO.2 completion thread (on asynchronous failure). */
	private def clearBatchReferences(): Unit = {
		var i = 0
		while i < batchLimit do {
			gatheringBatch(i) = null
			i += 1
		}
		batchOffset = 0
		batchLimit = 0
	}

	/** Returns the direct [[streamTransferBuffer]] for trailing stream transfers, allocating it lazily on first use.
	 * Executed on the NIO.2 completion thread entering [[startTrailingStream]].
	 * @return The non-null direct [[java.nio.ByteBuffer]] of capacity [[streamTransferBufferCapacity]]. */
	private def ensureStreamTransferBuffer(): ByteBuffer = {
		val buf = streamTransferBuffer
		if buf ne null then buf
		else {
			val allocated = ByteBuffer.allocateDirect(streamTransferBufferCapacity)
			streamTransferBuffer = allocated
			allocated
		}
	}

	/** Closes the active [[currentAsyncReader]] via [[cleanupCurrentReader]], clears [[currentStreamByteSource]], and empties [[transmissionByteSources]].
	 * Executed on the transmission thread / completion handler when the trailing stream finishes, aborts, or encounters a socket error. */
	private def cleanupStream(): Unit = {
		cleanupCurrentReader()
		currentStreamByteSource = null
		transmissionByteSources.clear()
	}

	/** Closes [[currentAsyncReader]] via [[readren.common.ByteSource.AsyncReader.close]] if open, suppressing secondary close exceptions so primary stream or socket faults are not masked.
	 * Executed on the transmission thread / completion handler during source EOF, stream abort, or socket write failure. */
	private def cleanupCurrentReader(): Unit = {
		val reader = currentAsyncReader
		if reader ne null then {
			try reader.close() catch { case NonFatal(_) => () }
			currentAsyncReader = null
		}
	}
}
