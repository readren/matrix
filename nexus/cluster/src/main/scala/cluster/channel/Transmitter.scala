package readren.nexus
package cluster.channel

import cluster.channel.Transmitter.*
import cluster.misc.{DualEndedCircularStorage, RingLength, VLQ}
import cluster.serialization.Serializer.{SerializationException, Writer}
import cluster.serialization.{ProtocolVersion, RecyclableIdentityIntMap, Serializer}

import readren.common.ByteSource

import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousSocketChannel, CompletionHandler}
import java.util as util
import java.util.concurrent.TimeUnit
import scala.util.control.NonFatal

object Transmitter {
	export TransmissionProtocol.*

	class TransmissionInProgressException extends Exception // TODO Should it be a RuntimeException?

	/** Shared zero-allocation callback adapter used by [[Transmitter.transmit]] to forward the single-argument completion callback through [[Transmitter.transmitWithAttachment]] as its attachment parameter without allocating a wrapper closure per message. */
	private val invokeWithoutAttachment: (Report, Report => Unit) => Unit = (report, onComplete) => onComplete(report)
}

/** A capability for transmitting messages over an [[java.nio.channels.AsynchronousSocketChannel]].
 *
 * Note: The underlying [[java.nio.channels.AsynchronousSocketChannel]] is not thread-safe and supports only one transmission at a time. Concurrent attempts to transmit messages will result in undefined behavior.
 *
 * @param channel The channel over which the messages will be transmitted.
 * @param context Transport diagnostic addressing context.
 * @param capacityOfBufferInRing The capacity of each buffer of the [[misc.DualEndedCircularStorage]].
 * @param ringMaxLength The maximum number of buffers allowed in circular storage.
 * @param streamTransferBufferCapacity The capacity of the direct buffer used to transfer streaming chunks.
 * @param maxGatheringBuffers The maximum number of [[java.nio.ByteBuffer]] entries submitted per gathering socket write call. */
class Transmitter(
	channel: AsynchronousSocketChannel,
	context: Context,
	capacityOfBufferInRing: Int = Transmitter.DEFAULT_CAPACITY_FOR_BUFFERS_IN_RING,
	val ringMaxLength: RingLength = DEFAULT_RING_MAX_LENGTH,
	val streamTransferBufferCapacity: Int = Transmitter.DEFAULT_STREAM_TRANSFER_BUFFER_CAPACITY,
	val maxGatheringBuffers: Int = Transmitter.DEFAULT_MAX_GATHERING_BUFFERS
) {
	assert(capacityOfBufferInRing >= Serializer.BUFFER_SIZE_REQUIRED_BY_MOST_DEMANDING_OPERATION)
	require(ringMaxLength.toLong * capacityOfBufferInRing.toLong <= Int.MaxValue, s"Aggregate circular buffer capacity (${ringMaxLength.toLong * capacityOfBufferInRing.toLong} bytes) exceeds Int.MaxValue")
	require(streamTransferBufferCapacity > 0, s"streamTransferBufferCapacity must be positive, got $streamTransferBufferCapacity")
	require(maxGatheringBuffers >= 2, s"maxGatheringBuffers must be at least 2, got $maxGatheringBuffers")

	// The following reference variables and single-flight state fields are defined at class level rather than inside `transmitWithAttachment` to eliminate per-call object, handler, and closure allocations. This is valid because `Transmitter` enforces single-flight transmission via `transmissionInProgress` (whose volatile write-to-read edge also publishes reused state across consecutive calls from different threads).
	private val deduplicationMap: RecyclableIdentityIntMap = new RecyclableIdentityIntMap()
	/** Holds the VLQ-encoded byte length of the single control-package frame. */
	private val frameHeaderBuffer: ByteBuffer = ByteBuffer.allocate(VLQ.INT_MAX_LENGTH)
	private val frameHeaderWriter: VLQ.ByteWriter = (byte: Byte) => frameHeaderBuffer.put(byte)
	/** Holds the standalone package terminator sentinel (`0x00` for standalone packages, or [[STREAM_FOLLOWS_SENTINEL_ENCODED]] when trailing byte sources are pending) when it does not fit at the tail of the final [[circularStorage]] buffer. */
	private val sentinelBuffer: ByteBuffer = ByteBuffer.allocate(VLQ.INT_MAX_LENGTH)
	private val circularStorage: DualEndedCircularStorage[ByteBuffer] = new DualEndedCircularStorage[ByteBuffer](() => ByteBuffer.allocateDirect(capacityOfBufferInRing), ringMaxLength)
	/** Bounded array of [[java.nio.ByteBuffer]] entries passed to gathering [[channel.write]] calls: starts with [[frameHeaderBuffer]] on the first batch, followed by unread buffers from [[circularStorage]], and ends with [[sentinelBuffer]] when needed. */
	private val gatheringBatch: Array[ByteBuffer] = new Array[ByteBuffer](maxGatheringBuffers)
	/** Guards single-flight transmission: `true` while control packaging, gathering socket writes, or trailing streaming is in progress; reset to `false` before invoking `onComplete`. */
	@volatile private var transmissionInProgress: Boolean = false

	private val streamTransferBuffer: ByteBuffer = ByteBuffer.allocateDirect(streamTransferBufferCapacity)
	private val streamChunkHeaderBuffer: ByteBuffer = ByteBuffer.allocate(VLQ.INT_MAX_LENGTH)
	private val streamChunkBuffers: Array[ByteBuffer] = Array(streamChunkHeaderBuffer, streamTransferBuffer)
	/** @note Concurrency invariant: Reusable queue safely shared across transmissions because Transmitter enforces single-flight message transmission on this socket. Cleared on initiation, stream termination, and error cleanup. */
	private val pendingByteSources: util.ArrayDeque[ByteSource] = new util.ArrayDeque[ByteSource]()

	// Active single-flight per-message metadata, cleared in `completeTransmission` before releasing `transmissionInProgress` and invoking the callback.
	private var currentMessage: Any = null
	private var currentMsgVersion: ProtocolVersion = ProtocolVersion.NOT_SPECIFIED
	private var currentAttachment: Any = null
	private var currentOnComplete: (Report, Any) => Unit = null
	private var currentTimeout: Long = 0L
	private var currentTimeUnit: TimeUnit = TimeUnit.SECONDS

	// Control-package serialization and bounded gathering write state.
	/** Modified only by the calling thread during control serialization and cleared in [[completeTransmission]]. */
	private var writeEndBuffer: ByteBuffer = null
	private var totalControlBytes: Int = 0
	private var hasUnreadStorageBuffers: Boolean = false
	private var separateSentinelPending: Boolean = false
	private var batchOffset: Int = 0
	private var batchLimit: Int = 0

	// Trailing-stream state.
	@volatile private var cancellationReason: NotDelivered | Null = null
	private var currentStreamByteSource: ByteSource | Null = null
	private var currentAsyncReader: ByteSource.AsyncReader | Null = null
	private var currentStreamBytesRead: Long = 0L
	private var currentStreamDeclaredSize: Long = 0L

	// ====================================================================================================
	// Public API
	// ====================================================================================================

	/** Transmits a message through the channel, invoking the callback with a [[Transmitter.Report]] upon completion.
	 * @param message the message to transmit.
	 * @param msgVersion the version id of messages to produce.
	 * @param timeout The maximum time to wait for the first chunk of bytes and between chunks of bytes are transmitted.
	 * @param timeUnit The unit of time for the `timeout` parameter.
	 * @param onComplete A consumer callback that is invoked when the transmission completes either successfully or not, with a [[Transmitter.Report]].
	 * @param serializer The serializer to be used for serializing the message into outgoing bytes.
	 * @tparam M The type of the `message` to be sent.
	 * @throws TransmissionInProgressException if another transmission is already in flight on this transmitter. */
	inline def transmit[M](message: M, msgVersion: ProtocolVersion, timeout: Long = 1, timeUnit: TimeUnit = TimeUnit.SECONDS)(onComplete: Report => Unit)(using serializer: Serializer[M]): Unit = {
		transmitWithAttachment[M, Report => Unit](message, msgVersion, onComplete, timeout, timeUnit)(invokeWithoutAttachment)
	}

	/** Transmits a message through the channel with a user-supplied attachment passed through to the completion callback.
	 * @param message the message to transmit.
	 * @param msgVersion the version id of messages to produce.
	 * @param attachment an arbitrary attachment passed to [[onComplete]].
	 * @param timeout The maximum time to wait for the first chunk of bytes and between chunks of bytes are transmitted.
	 * @param timeUnit The unit of time for the `timeout` parameter.
	 * @param onComplete A consumer callback that is invoked when the transmission completes either successfully or not, with a [[Transmitter.Report]] and attachment.
	 * @param serializer The serializer to be used for serializing the message into outgoing bytes.
	 * @tparam M The type of the `message` to be sent.
	 * @tparam A The type of the attachment passed through.
	 * @throws TransmissionInProgressException if another transmission is already in flight on this transmitter. */
	def transmitWithAttachment[M, A](
		message: M,
		msgVersion: ProtocolVersion,
		attachment: A,
		timeout: Long = 1,
		timeUnit: TimeUnit = TimeUnit.SECONDS
	)(
		onComplete: (Report, A) => Unit
	)(using serializer: Serializer[M]): Unit = {
		/** Asserts single-flight transmission: `transmissionInProgress` is `true` while control packaging or trailing streaming is in progress. It remains `true` throughout trailing streaming until [[completeTransmission]] to prevent concurrent transmissions from corrupting wire state. */
		if transmissionInProgress then throw new TransmissionInProgressException
		transmissionInProgress = true

		currentMessage = message
		currentAttachment = attachment
		currentOnComplete = onComplete.asInstanceOf[(Report, Any) => Unit]
		currentTimeout = timeout
		currentTimeUnit = timeUnit

		pendingByteSources.clear()
		controlPackageTransmissionHandler.prepare(msgVersion)
		writeEndBuffer = circularStorage.writeEnd
		writeEndBuffer.clear()
		totalControlBytes = 0
		hasUnreadStorageBuffers = false
		separateSentinelPending = false
		batchOffset = 0
		batchLimit = 0
		cancellationReason = null

		val failureReport: NotDelivered | Null = try {
			serializer.serialize(message, controlPackageTransmissionHandler)
			null
		} catch {
			case se: SerializationException => SerializationProblem(message, se, aFragmentWasTransmitted = false)
			case NonFatal(e) => TransmissionFailure(message, e)
		}
		if failureReport eq null then {
			totalControlBytes += writeEndBuffer.position()
			val hasByteSources = !pendingByteSources.isEmpty
			val sentinelLength = if hasByteSources then STREAM_FOLLOWS_SENTINEL_ENCODED.length else 1
			if writeEndBuffer.remaining() >= sentinelLength then {
				// Inline the message-end or trailing-stream sentinel at the tail of the last content buffer so no extra ByteBuffer slot is needed during gathering writes.
				if hasByteSources then writeEndBuffer.put(STREAM_FOLLOWS_SENTINEL_ENCODED)
				else writeEndBuffer.put(0: Byte)
				writeEndBuffer.flip()
				separateSentinelPending = false
			} else {
				// Not enough tail room in the last content buffer; stage sentinelBuffer as an extra gathering buffer.
				writeEndBuffer.flip()
				separateSentinelPending = true
			}

			if totalControlBytes > 0 then {
				frameHeaderBuffer.clear()
				VLQ.encodeUnsignedInt(totalControlBytes, frameHeaderWriter)
				frameHeaderBuffer.flip()
			} else {
				// Omit the frame header when 0 payload bytes were produced so the sentinel alone is written.
				frameHeaderBuffer.clear()
				frameHeaderBuffer.limit(0)
			}

			if separateSentinelPending then {
				sentinelBuffer.clear()
				if hasByteSources then sentinelBuffer.put(STREAM_FOLLOWS_SENTINEL_ENCODED)
				else sentinelBuffer.put(0: Byte)
				sentinelBuffer.flip()
			}

			hasUnreadStorageBuffers = true
			fillAndSendNextControlBatch()
		} else {
			// No frames were sent to the channel, so call `onComplete` synchronously.
			completeTransmission(failureReport.nn)
		}
	}

	/** Prunes unused allocated buffers in circular storage down to a target length.
	 * @param targetLength The desired target number of buffers to retain.
	 * @return The number of buffers retained in circular storage after pruning. */
	def pruneTo(targetLength: RingLength): RingLength = circularStorage.pruneTo(targetLength)

	/** The total number of buffers currently allocated in circular storage. */
	def ringLength: RingLength = circularStorage.ringLength

	// ====================================================================================================
	// Transmission Lifecycle Orchestration
	// ====================================================================================================

	/** Completes the transmission lifecycle by capturing the active callback metadata, clearing single-flight state and references, releasing [[transmissionInProgress]], and invoking the completion callback.
	 * @param report The final [[Transmitter.Report]] indicating delivery success or the specific non-delivery failure. */
	private def completeTransmission(report: Report): Unit = {
		val completedMessage = currentMessage
		val completedAttachment = currentAttachment
		val completedOnComplete = currentOnComplete
		currentMessage = null
		currentMsgVersion = ProtocolVersion.NOT_SPECIFIED
		currentAttachment = null
		currentOnComplete = null
		writeEndBuffer = null
		cancellationReason = null
		currentStreamByteSource = null
		currentAsyncReader = null
		clearBatchReferences()
		resetCircularStorage()
		pendingByteSources.clear()
		transmissionInProgress = false
		scribe.trace(s"Transmission progress of message `$completedMessage` from `${context.myAddress}` to `${context.showPeerAddress}`: completed with report=$report")
		completedOnComplete(report, completedAttachment)
	}

	// ====================================================================================================
	// Control Package Serialization & Gathering Transmission Mechanics
	// ====================================================================================================

	/** Both,
	 *   - the [[Serializer.Writer]] instance used to serialize the complete control package into [[circularStorage]] before any socket write begins,
	 *   - and the delegate passed to [[channel.write]] to handle bounded gathering socket writes for the single-frame control package. */
	private object controlPackageTransmissionHandler extends Writer(deduplicationMap), CompletionHandler[java.lang.Long, Null] { thisTransmissionHandler =>
		/** Prepares this writer for a new serialization pass by updating [[currentMsgVersion]] and resetting deduplication and escaping state.
		 * @param msgVersion The protocol version governing the message about to be serialized. */
		def prepare(msgVersion: ProtocolVersion): Unit = {
			currentMsgVersion = msgVersion
			resetDeduplication()
		}

		override def governingVersion: ProtocolVersion = currentMsgVersion

		/** The current write position within the active write-end buffer. */
		override def position: Int = writeEndBuffer.position()

		/** @inheritdoc
		 * @throws cluster.serialization.Serializer.SerializationException if the [[ByteSource.size]] is negative. */
		override def putByteSource(source: ByteSource): Unit = {
			val size = source.size
			if size < 0 then throw new SerializationException(writeEndBuffer.position(), s"ByteSource size cannot be negative ($size)")
			putUnsignedLongVlq(size)
			if size > 0 then pendingByteSources.add(source)
		}

		/** Obtains a [[java.nio.ByteBuffer]] having at least `minimumRemaining` bytes available, flipping full buffers, accumulating [[totalControlBytes]], and advancing [[circularStorage]] when needed.
		 * @param minimumRemaining The minimum number of contiguous bytes required for the next serialization write.
		 * @return A [[java.nio.ByteBuffer]] positioned for writing with at least `minimumRemaining` bytes available.
		 * @throws cluster.channel.RingLengthExceededException if advancing [[circularStorage]] would exceed [[ringMaxLength]]. */
		protected override def getBuffer(minimumRemaining: Int): ByteBuffer = {
			if writeEndBuffer.remaining() < minimumRemaining then {
				// Record the payload bytes written into the current buffer and flip it so gathering writes can read it directly during transmission.
				totalControlBytes += writeEndBuffer.position()
				writeEndBuffer.flip()
				circularStorage.advanceWriteEnd().fold {
					throw new RingLengthExceededException(s"Message transmission exceeded maximum buffer capacity ($ringMaxLength buffers of $capacityOfBufferInRing bytes)")
				} { nextBuffer =>
					nextBuffer.clear()
					writeEndBuffer = nextBuffer
				}
			}
			writeEndBuffer
		}

		/** Advances [[batchOffset]] past completely drained buffers in [[gatheringBatch]], re-submitting partial batches, advancing to subsequent storage batches, or transitioning to trailing streaming or completion.
		 * @param bytesTransmitted The number of bytes written to [[channel]] during the completed operation.
		 * @param att Unused attachment (`null`). */
		override def completed(bytesTransmitted: java.lang.Long, att: Null): Unit = {
			// Advance batchOffset past all buffers in the current batch that were completely drained by the socket write.
			while batchOffset < batchLimit && !gatheringBatch(batchOffset).hasRemaining do {
				gatheringBatch(batchOffset) = null
				batchOffset += 1
			}
			if batchOffset < batchLimit then {
				// Partial gathering write: re-submit the remaining unwritten slice of gatheringBatch.
				try channel.write(gatheringBatch, batchOffset, batchLimit - batchOffset, currentTimeout, currentTimeUnit, null, thisTransmissionHandler) catch {
					case NonFatal(e) => completeTransmission(TransmissionFailure(currentMessage, e))
				}
			} else {
				// Current batch of buffers is completely written.
				batchOffset = 0
				batchLimit = 0
				if hasUnreadStorageBuffers || separateSentinelPending then {
					// More payload buffers in circularStorage (or the standalone sentinelBuffer) remain to be sent for this single frame; send the next batch.
					fillAndSendNextControlBatch()
				} else if pendingByteSources.isEmpty then {
					completeTransmission(Delivered)
				} else {
					// Transitions to streaming out-of-line `ByteSource` instances after the control package and STREAM_FOLLOWS_SENTINEL complete.
					transmitNextStreamChunk()
				}
			}
		}

		/** Delivers a [[Transmitter.TransmissionFailure]] when socket transmission of a control package batch fails.
		 * @param exc The network I/O exception that caused the batch write to fail.
		 * @param att Unused attachment (`null`). */
		override def failed(exc: Throwable, att: Null): Unit = {
			completeTransmission(TransmissionFailure(currentMessage, exc))
		}
	}

	/** Populates [[gatheringBatch]] with up to [[maxGatheringBuffers]] buffers (starting with [[frameHeaderBuffer]] on the first batch, followed by unread buffers from [[circularStorage]], and ending with [[sentinelBuffer]] when needed) and submits a gathering write on [[channel]]. */
	private def fillAndSendNextControlBatch(): Unit = {
		batchOffset = 0
		var idx = 0
		// Include the single-frame VLQ length header at slot 0 of the first gathering batch.
		if frameHeaderBuffer.hasRemaining then {
			gatheringBatch(idx) = frameHeaderBuffer
			idx += 1
		}
		// Pull unread payload buffers from circularStorage up to maxGatheringBuffers.
		while idx < maxGatheringBuffers && hasUnreadStorageBuffers do {
			val readBuf = circularStorage.readEnd
			if readBuf.hasRemaining then {
				gatheringBatch(idx) = readBuf
				idx += 1
			}
			hasUnreadStorageBuffers = circularStorage.advanceReadEnd()
		}
		// Append the standalone sentinelBuffer at the end of the final batch if it is still pending and a slot remains in gatheringBatch.
		if !hasUnreadStorageBuffers && separateSentinelPending && idx < maxGatheringBuffers then {
			gatheringBatch(idx) = sentinelBuffer
			idx += 1
			separateSentinelPending = false
		}
		batchLimit = idx
		assert(batchLimit > 0)
		scribe.trace(s"Transmission progress of message `$currentMessage` from `${context.myAddress}` to `${context.showPeerAddress}`: sending gathering batch of $batchLimit buffers (controlBytes=$totalControlBytes, hasMoreStorage=$hasUnreadStorageBuffers, separateSentinelPending=$separateSentinelPending)")
		try {
			channel.write(gatheringBatch, 0, batchLimit, currentTimeout, currentTimeUnit, null, controlPackageTransmissionHandler)
		} catch {
			case NonFatal(e) => completeTransmission(TransmissionFailure(currentMessage, e))
		}
	}

	// ====================================================================================================
	// Trailing Stream Mechanics & Completion Handlers
	// ====================================================================================================

	/** Reads and transmits the next chunk from the active byte source, advances to the next pending source, or sends the terminal 0-length chunk.
	 * @note Non-trampolined execution contract: Unlike `PipelinedTransmitter`, this single-flight `Transmitter` does not wrap `read`/`write` completions in an atomic trampoline loop. The synchronous loop in this method handles `readStatus != ByteSource.AsyncReader.PENDING` reads and source EOF transitions iteratively, while asynchronous continuation across socket writes relies on [[readren.common.ByteSource.AsyncReader]] reporting synchronous completions via its `Int` return value (never invoking [[java.nio.channels.CompletionHandler]] inline before returning [[readren.common.ByteSource.AsyncReader.PENDING]]) and on [[java.nio.channels.AsynchronousSocketChannel]] bounding consecutive inline callback depth. */
	private def transmitNextStreamChunk(): Unit = {
		var continueLoop = true
		while continueLoop do {
			if currentStreamByteSource == null then {
				if pendingByteSources.isEmpty then {
					streamChunkHeaderBuffer.clear()
					streamChunkHeaderBuffer.put(0: Byte)
					streamChunkHeaderBuffer.flip()
					sendStreamWrite(0, 1, sentinelIsWritten = true)
					return
				} else {
					val source = pendingByteSources.poll()
					currentStreamByteSource = source
					currentStreamDeclaredSize = source.size
					currentStreamBytesRead = 0L
					try {
						currentAsyncReader = source.openAsyncReader()
					} catch {
						case NonFatal(e) =>
							abortTrailingStream(e)
							return
					}
				}
			}

			streamTransferBuffer.clear()
			val readStatus = try {
				currentAsyncReader.read(streamTransferBuffer, null, byteSourceReadHandler)
			} catch {
				case NonFatal(e) =>
					abortTrailingStream(e)
					return
			}
			if readStatus != ByteSource.AsyncReader.PENDING then {
				if readStatus > 0 then {
					continueLoop = false
					handleByteSourceChunkRead(readStatus)
				} else {
					cleanupCurrentReader()
					if currentStreamBytesRead != currentStreamDeclaredSize then {
						continueLoop = false
						abortTrailingStream(new java.io.EOFException(s"Premature EOF for ByteSource: expected $currentStreamDeclaredSize bytes, got $currentStreamBytesRead"))
					} else {
						currentStreamByteSource = null
					}
				}
			} else {
				continueLoop = false
			}
		}
	}

	/** Processes bytes read from the active [[readren.common.ByteSource]], validating declared sizes and transmitting chunk frames.
	 * @param readBytes The number of bytes transferred into [[streamTransferBuffer]], or -1 if the source reached EOF. */
	private def handleByteSourceChunkRead(readBytes: Int): Unit = {
		if readBytes > 0 then {
			currentStreamBytesRead += readBytes
			if currentStreamBytesRead > currentStreamDeclaredSize then {
				abortTrailingStream(new java.io.IOException(s"ByteSource size drift: read $currentStreamBytesRead bytes exceeding declared $currentStreamDeclaredSize"))
			} else {
				streamTransferBuffer.flip()
				streamChunkHeaderBuffer.clear()
				VLQ.encodeUnsignedInt(readBytes, trailingStreamTransmissionHandler)
				streamChunkHeaderBuffer.flip()
				sendStreamWrite(0, 2, sentinelIsWritten = false)
			}
		} else {
			cleanupCurrentReader()
			if currentStreamBytesRead != currentStreamDeclaredSize then {
				abortTrailingStream(new java.io.EOFException(s"Premature EOF for ByteSource: expected $currentStreamDeclaredSize bytes, got $currentStreamBytesRead"))
			} else {
				currentStreamByteSource = null
				transmitNextStreamChunk()
			}
		}
	}

	/** Aborts the active trailing stream, cleans up resources, records the failure, and sends an in-band corrupted package sentinel to the peer.
	 * @param cause The root exception that caused trailing stream transmission to fail. */
	private def abortTrailingStream(cause: Throwable): Unit = {
		cleanupStream()
		cancellationReason = ByteSourceProblem(currentMessage, cause)
		streamChunkHeaderBuffer.clear()
		streamChunkHeaderBuffer.put(CORRUPTED_PACKAGE_SENTINEL_ENCODED)
		streamChunkHeaderBuffer.flip()
		sendStreamWrite(0, 1, sentinelIsWritten = true)
	}

	/** Submits an asynchronous socket write on [[channel]] for a trailing stream chunk or terminal sentinel, catching synchronous channel exceptions so stream cleanup and completion delivery are never bypassed.
	 * @param offset The starting index in [[streamChunkBuffers]].
	 * @param length The number of buffers to write (`1` or `2`).
	 * @param sentinelIsWritten Whether the write completes the trailing stream with a terminal sentinel. */
	private def sendStreamWrite(offset: Int, length: Int, sentinelIsWritten: Boolean): Unit = {
		try {
			channel.write(streamChunkBuffers, offset, length, currentTimeout, currentTimeUnit, sentinelIsWritten, trailingStreamTransmissionHandler)
		} catch {
			case NonFatal(e) =>
				cleanupStream()
				completeTransmission(TransmissionFailure(currentMessage, e))
		}
	}

	/** Delegate passed to [[currentAsyncReader]] to handle the feeding of the [[streamTransferBuffer]]. */
	private object byteSourceReadHandler extends CompletionHandler[Integer, Null] {
		/** Dispatches read chunk bytes into channel transmission upon asynchronous read completion.
		 * @param readBytes The number of bytes read from the active byte source, or -1 on EOF.
		 * @param att Unused attachment parameter. */
		override def completed(readBytes: Integer, att: Null): Unit = {
			handleByteSourceChunkRead(readBytes)
		}

		/** Aborts the trailing stream when an asynchronous read from the byte source fails.
		 * @param exc The I/O exception encountered while reading the byte source.
		 * @param att Unused attachment parameter. */
		override def failed(exc: Throwable, att: Null): Unit = {
			abortTrailingStream(exc)
		}
	}

	/** Delegate passed to [[channel.write]] to handle the consumption of the [[streamTransferBuffer]] and VLQ chunk-length header encoding. */
	private object trailingStreamTransmissionHandler extends CompletionHandler[java.lang.Long, Boolean], VLQ.ByteWriter { thisTransmissionHandler =>
		/** Appends a single VLQ-encoded chunk-length byte into [[streamChunkHeaderBuffer]]. */
		override def putByte(byte: Byte): Unit = streamChunkHeaderBuffer.put(byte)

		/** Handles completion of chunk frame transmission over the socket channel, advancing chunk headers or triggering next chunks.
		 * @param bytesWritten The number of bytes written to the channel during this operation.
		 * @param sentinelIsWritten Whether this write operation represented the terminal stream sentinel or peer abort sentinel. */
		override def completed(bytesWritten: java.lang.Long, sentinelIsWritten: Boolean): Unit = {
			if streamChunkBuffers(0).hasRemaining then {
				sendStreamWrite(0, if sentinelIsWritten then 1 else 2, sentinelIsWritten)
			} else if !sentinelIsWritten && streamChunkBuffers(1).hasRemaining then {
				sendStreamWrite(1, 1, sentinelIsWritten = false)
			} else if sentinelIsWritten then {
				cleanupStream()
				val reason = cancellationReason
				if reason ne null then completeTransmission(reason)
				else completeTransmission(Delivered)
			} else {
				transmitNextStreamChunk()
			}
		}

		/** Cleans up stream resources and delivers a [[Transmitter.TransmissionFailure]] upon socket write failure.
		 * @param exc The network I/O exception that caused the transmission write to fail.
		 * @param sentinelIsWritten Whether the failed write was attempting to transmit a terminal sentinel. */
		override def failed(exc: Throwable, sentinelIsWritten: Boolean): Unit = {
			cleanupStream()
			completeTransmission(TransmissionFailure(currentMessage, exc))
		}
	}

	// ====================================================================================================
	// Low-Level State & Cleanup Helpers
	// ====================================================================================================

	/** Clears buffer references from [[gatheringBatch]] up to [[batchLimit]] and resets batch tracking offsets. */
	private def clearBatchReferences(): Unit = {
		var i = 0
		while i < batchLimit do {
			gatheringBatch(i) = null
			i += 1
		}
		batchOffset = 0
		batchLimit = 0
	}

	/** Resets [[circularStorage]] read/write pointers so the ring is empty and ready for the next transmission. */
	private def resetCircularStorage(): Unit = {
		while circularStorage.advanceReadEnd() do ()
		circularStorage.writeEnd.clear()
	}

	/** Releases the active reader, clears stream references, and empties pending sources to avoid leaking references. */
	private def cleanupStream(): Unit = {
		cleanupCurrentReader()
		currentStreamByteSource = null
		pendingByteSources.clear()
	}

	/** Closes the active [[ByteSource.AsyncReader]] to release OS file handles or backing resources. */
	private def cleanupCurrentReader(): Unit = {
		if currentAsyncReader != null then {
			// Read-only handle disposal: Suppress secondary close errors because bytes were already read and errors must not mask cancellation or stream faults
			try currentAsyncReader.close() catch { case NonFatal(_) => () }
			currentAsyncReader = null
		}
	}
}
