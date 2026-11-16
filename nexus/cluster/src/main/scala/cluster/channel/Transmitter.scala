package readren.nexus
package cluster.channel

import cluster.channel.Transmitter.*
import cluster.misc.LoggingTools.*
import cluster.misc.{DualEndedCircularStorage, VLQ}
import cluster.serialization.Serializer.{SerializationException, Writer}
import cluster.serialization.{ProtocolVersion, RecyclableIdentityIntMap, Serializer}
import cluster.service.ChannelId
import cluster.service.Protocol.ContactAddress

import readren.common.{ByteSource, Maybe}

import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousSocketChannel, CompletionHandler}
import java.util.ArrayDeque
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.threadUnsafe
import scala.util.control.NonFatal

object Transmitter {
	export TransmissionProtocol.*

	class TransmissionInProgressException extends Exception // TODO Should it be a RuntimeException?

	/** Control-package coordination state: both serialization and asynchronous frame transmission are concurrently active. */
	private inline val SERIALIZING_AND_TRANSMITTING = 0
	/** Control-package coordination state: frame transmission drained all buffered frames so far and paused while serialization continues. */
	private inline val TRANSMISSION_PAUSED_WHILE_SERIALIZING = 1
	/** Control-package coordination state: serialization completed cleanly and frame transmission continues for remaining buffered data, end sentinel, or trailing stream. */
	private inline val SERIALIZATION_COMPLETED_AND_TRANSMITTING = 2
	/** Control-package coordination state: serialization failed after frames reached the channel; the corrupted-package sentinel must be transmitted before reporting completion. */
	private inline val SERIALIZATION_FAILED = 3
	/** Control-package coordination state: asynchronous channel transmission encountered an I/O error. */
	private inline val TRANSMISSION_FAILED = 4
}

/** A capability for transmitting messages over an [[java.nio.channels.AsynchronousSocketChannel]].
 *
 * Note: The underlying [[java.nio.channels.AsynchronousSocketChannel]] is not thread-safe and supports only one transmission at a time. Concurrent attempts to transmit messages will result in undefined behavior.
 *
 * @param channel The channel over which the messages will be transmitted.
 * @param context Transport diagnostic addressing context.
 * @param buffersCapacity The capacity of each buffer of the [[misc.DualEndedCircularStorage]], which determines the frames max size (frameMaxSize == buffersCapacity).
 * @param maxBuffers The maximum number of buffers allowed in circular storage.
 * @param streamTransferBufferCapacity The capacity of the direct buffer used to transfer streaming chunks. */
class Transmitter(
	channel: AsynchronousSocketChannel,
	context: Context,
	buffersCapacity: Int = Transmitter.DEFAULT_BUFFERS_CAPACITY,
	val maxBuffers: Int = DEFAULT_MAX_BUFFERS,
	val streamTransferBufferCapacity: Int = Transmitter.DEFAULT_STREAM_TRANSFER_BUFFER_CAPACITY
) {
	assert(buffersCapacity >= Serializer.BUFFER_SIZE_REQUIRED_BY_MOST_DEMANDING_OPERATION)
	require(streamTransferBufferCapacity > 0, s"streamTransferBufferCapacity must be positive, got $streamTransferBufferCapacity")

	// The following reference variables could be local to the `transmit` method but are defined as class fields to reuse the object they refer to. This is valid because `transmit` does not support concurrent calls due to the nature of `AsynchronousSocketChannel.write`.
	private val deduplicationMap: RecyclableIdentityIntMap = new RecyclableIdentityIntMap()
	private val frameHeaderBuffer: ByteBuffer = ByteBuffer.allocate(VLQ.INT_MAX_LENGTH)
	private val frameHeaderWriter: VLQ.ByteWriter = (byte: Byte) => frameHeaderBuffer.put(byte) 
	private val circularStorage: DualEndedCircularStorage[ByteBuffer] = new DualEndedCircularStorage[ByteBuffer](() => ByteBuffer.allocateDirect(buffersCapacity), maxBuffers)
	private val frameBuffer: Array[ByteBuffer] = new Array(2)
	/** Holds the coordination signal between the serialization (executed by the thread that called [[transmit]]) and the transmission callbacks (executed by an NIO2 thread). */
	private val coordinationCell: AtomicInteger = new AtomicInteger()

	private val streamTransferBuffer: ByteBuffer = ByteBuffer.allocateDirect(streamTransferBufferCapacity)
	private val streamChunkHeaderBuffer: ByteBuffer = ByteBuffer.allocate(VLQ.INT_MAX_LENGTH)
	private val streamChunkHeaderWriter: VLQ.ByteWriter = (byte: Byte) => streamChunkHeaderBuffer.put(byte)
	private val streamChunkBuffers: Array[ByteBuffer] = Array(streamChunkHeaderBuffer, streamTransferBuffer)
	/** @note Concurrency invariant: Reusable queue safely shared across transmissions because Transmitter enforces single-flight message transmission on this socket. Cleared on initiation, stream termination, and error cleanup. */
	private val pendingByteSources: ArrayDeque[ByteSource] = new ArrayDeque[ByteSource]()

	/** Prunes unused allocated buffers in circular storage down to a target count.
	 * @param targetCount The desired target number of buffers to retain.
	 * @return The number of pruned buffers actually freed. */
	def pruneTo(targetCount: Int): Int = circularStorage.pruneTo(targetCount)

	/** The total number of buffers currently allocated in circular storage. */
	def bufferCount: Int = circularStorage.bufferCount

	/** Consist of two buffers: the first for the frame header (which tells the size of the frame content in bytes), and the second for the frame content. */
	frameBuffer(0) = frameHeaderBuffer

	/** Transmits a message through the channel, invoking the callback with a [[Transmitter.Report]] upon completion.
	 * @param message the message to transmit.
	 * @param msgVersion the version id of messages to produce.
	 * @param behindTransmissionEnabled whether behind-transmission frame pipelining is enabled.
	 * @param timeout The maximum time to wait for the first chunk of bytes and between chunks of bytes are transmitted.
	 * @param timeUnit The unit of time for the `timeout` parameter.
	 * @param onComplete A consumer callback that is invoked when the transmission completes either successfully or not, with a [[Transmitter.Report]].
	 * @param serializer The serializer to be used for serializing the message into outgoing bytes.
	 * @tparam M The type of the `message` to be sent.
	 * @throws TransmissionInProgressException if another transmission is already in flight on this transmitter. */
	inline def transmit[M](message: M, msgVersion: ProtocolVersion, behindTransmissionEnabled: Boolean = true, timeout: Long = 1, timeUnit: TimeUnit = TimeUnit.SECONDS)(onComplete: Report => Unit)(using serializer: Serializer[M]): Unit = {
		transmitWithAttachment[M, Null](message, msgVersion, null, behindTransmissionEnabled, timeout, timeUnit) { (report, dummy) => onComplete(report) }
	}

	// assume that the thread that calls this method is named "main".
	/** Transmits a message through the channel with a user-supplied attachment passed through to the completion callback.
	 * @param message the message to transmit.
	 * @param msgVersion the version id of messages to produce.
	 * @param attachment an arbitrary attachment passed to [[onComplete]].
	 * @param behindTransmissionEnabled whether behind-transmission frame pipelining is enabled.
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
		behindTransmissionEnabled: Boolean = true,
		timeout: Long = 1,
		timeUnit: TimeUnit = TimeUnit.SECONDS
	)(
		onComplete: (Report, A) => Unit
	)(using serializer: Serializer[M]): Unit = {
		/** This variable is modified only within the thread that called this method ([[transmitWithAttachment]]). */
		var writeEndBuffer = circularStorage.writeEnd
		/** This variable is modified a single time and accessed only within the thread that called this method ([[transmitWithAttachment]]). */
		var behindTransmissionStarted = false
		/** This variable is modified one time only, from null to non-null, before transitioning [[coordinationCell]] to [[SERIALIZATION_FAILED]]. */
		@volatile var cancellationReason: NotDelivered | Null = null
		/** This variable is modified one time only, from null to non-null, before transitioning [[coordinationCell]] to [[TRANSMISSION_FAILED]]. */
		@volatile var ioFailure: Throwable | Null = null

		var currentStreamByteSource: ByteSource | Null = null
		var currentAsyncReader: ByteSource.AsyncReader | Null = null
		var currentStreamBytesRead: Long = 0L
		var currentStreamDeclaredSize: Long = 0L

		/** Sends a frame containing serialized message bytes from `contentBuffer` through the [[channel]].
		 * @param contentBuffer The buffer containing serialized message payload bytes.
		 * @param isIntermediateFrame When `true`, indicates that more frames follow for this message, so no terminator sentinel is appended; when `false`, indicates that this is the final frame and attempts to append the package terminator sentinel (either 0x00 for standalone packages, or [[STREAM_FOLLOWS_SENTINEL_ENCODED]] if trailing byte \ sources are pending) if `contentBuffer` has remaining capacity. */
		def sendFrame(contentBuffer: ByteBuffer, isIntermediateFrame: Boolean): Unit = {
			frameHeaderBuffer.clear()
			VLQ.encodeUnsignedInt(contentBuffer.position(), frameHeaderWriter)
			frameHeaderBuffer.flip()
			frameBuffer(1) = contentBuffer
			val sentinelIsIncluded = if isIntermediateFrame then {
				// send a single frame
				contentBuffer.flip()
				scribe.trace(s"Transmission progress of message `$message` from `${context.myAddress}` to `${context.showPeerAddress}: sending a frame with size=${frameHeaderBuffer.limit} + ${contentBuffer.limit}, contentBuffer=${show.drainingByteBuffer(contentBuffer)}")
				false
			} else if pendingByteSources.isEmpty then {
				if contentBuffer.hasRemaining then {
					// append the message-end sentinel value to the content buffer
					contentBuffer.put(0: Byte)
					// send the frame including the sentinel value (a trailing empty frame)
					contentBuffer.flip()
					scribe.trace(s"Transmission progress of message `$message` from `${context.myAddress}` to `${context.showPeerAddress}: sending the last frame with size=${frameHeaderBuffer.limit} + ${contentBuffer.limit - 1} + 1, contentBuffer=${show.drainingByteBuffer(contentBuffer)}")
					true
				} else {
					contentBuffer.flip()
					false
				}
			} else {
				if contentBuffer.remaining >= STREAM_FOLLOWS_SENTINEL_ENCODED.length then {
					// append the trailing-stream sentinel value to the content buffer
					contentBuffer.put(STREAM_FOLLOWS_SENTINEL_ENCODED)
					contentBuffer.flip()
					scribe.trace(s"Transmission progress of message `$message` from `${context.myAddress}` to `${context.showPeerAddress}: sending the last frame with trailing stream sentinel size=${frameHeaderBuffer.limit} + ${contentBuffer.limit - STREAM_FOLLOWS_SENTINEL_ENCODED.length} + ${STREAM_FOLLOWS_SENTINEL_ENCODED.length}")
					true
				} else {
					contentBuffer.flip()
					false
				}
			}
			channel.write(frameBuffer, 0, 2, timeout, timeUnit, sentinelIsIncluded, controlPackageTransmissionHandler)
		}

		/** Sends the standalone control package sentinel value when it was not appended to the last frame buffer.
		 * An empty frame indicates an uncorrupted package with no trailing stream, [[STREAM_FOLLOWS_SENTINEL_ENCODED]] indicates trailing stream payloads follow, and [[CORRUPTED_PACKAGE_SENTINEL_ENCODED]] indicates an aborted serialization.
		 * This distinction enables the deserializer to:
		 * 1. Detect failed serializations
		 * 2. Discard corrupted packets
		 * 3. Continue processing following messages without requiring channel restart. */
		def sendControlSentinelValue(): Unit = {
			scribe.trace(s"Transmission progress of message `$message` from `${context.myAddress}` to `${context.showPeerAddress}: sending control sentinel: cancellationReason=$cancellationReason, pendingByteSources=${pendingByteSources.size}.")
			frameHeaderBuffer.clear()
			if cancellationReason ne null then frameHeaderBuffer.put(CORRUPTED_PACKAGE_SENTINEL_ENCODED)
			else if !pendingByteSources.isEmpty then frameHeaderBuffer.put(STREAM_FOLLOWS_SENTINEL_ENCODED)
			else frameHeaderBuffer.put(0: Byte)
			frameHeaderBuffer.flip()
			channel.write(frameBuffer, 0, 1, timeout, timeUnit, true, controlPackageTransmissionHandler)
		}

		/** Completes the transmission lifecycle by resetting in-flight frame state, clearing pending byte sources, and invoking the completion callback.
		 * @param report The final [[Transmitter.Report]] indicating delivery success or the specific non-delivery failure. */
		def completeTransmission(report: Report): Unit = {
			frameBuffer(1) = null
			pendingByteSources.clear()
			scribe.trace(s"Transmission progress of message `$message` from `${context.myAddress}` to `${context.showPeerAddress}: completed with report=$report")
			onComplete(report, attachment)
		}

		/** Records a channel I/O failure on the transmission path and completes transmission only if serialization is not concurrently active. */
		def handleAsyncChannelFailure(exc: Throwable): Unit = {
			ioFailure = exc
			if coordinationCell.getAndSet(TRANSMISSION_FAILED) != SERIALIZING_AND_TRANSMITTING then completeTransmission(TransmissionFailure(message, exc))
		}

		/** Closes the active [[ByteSource.AsyncReader]] to release OS file handles or backing resources. */
		def cleanupCurrentReader(): Unit = {
			if currentAsyncReader != null then {
				// Read-only handle disposal: Suppress secondary close errors because bytes were already read and errors must not mask cancellation or stream faults
				try currentAsyncReader.close() catch { case NonFatal(_) => () }
				currentAsyncReader = null
			}
		}

		/** Releases the active reader, clears stream references, and empties pending sources to avoid leaking references. */
		def cleanupStream(): Unit = {
			cleanupCurrentReader()
			currentStreamByteSource = null
			pendingByteSources.clear()
		}

		/** Aborts the active trailing stream, cleans up resources, records the failure, and sends an in-band corrupted package sentinel to the peer.
		 * @param cause The root exception that caused trailing stream transmission to fail. */
		def abortTrailingStream(cause: Throwable): Unit = {
			cleanupStream()
			cancellationReason = ByteSourceProblem(message, cause)
			streamChunkHeaderBuffer.clear()
			streamChunkHeaderBuffer.put(CORRUPTED_PACKAGE_SENTINEL_ENCODED)
			streamChunkHeaderBuffer.flip()
			channel.write(streamChunkBuffers, 0, 1, timeout, timeUnit, true, trailingStreamTransmissionHandler)
		}

		/** Delegate passed to [[currentAsyncReader]] to handle the feeding of the [[streamTransferBuffer]].
		 * @note Marked [[@threadUnsafe]] because Transmitter enforces single-flight transmissions; only the single thread entering the streaming phase can evaluate this lazy val, and subsequent invocations are published via NIO.2 happens-before edges. */
		@threadUnsafe lazy val byteSourceReadHandler: CompletionHandler[Integer, Null] = new CompletionHandler[Integer, Null] {
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

		/** Processes bytes read from the active [[readren.common.ByteSource]], validating declared sizes and transmitting chunk frames.
		 * @param readBytes The number of bytes transferred into [[streamTransferBuffer]], or -1 if the source reached EOF. */
		def handleByteSourceChunkRead(readBytes: Int): Unit = {
			if readBytes > 0 then {
				currentStreamBytesRead += readBytes
				if currentStreamBytesRead > currentStreamDeclaredSize then {
					abortTrailingStream(new java.io.IOException(s"ByteSource size drift: read $currentStreamBytesRead bytes exceeding declared $currentStreamDeclaredSize"))
				} else {
					streamTransferBuffer.flip()
					streamChunkHeaderBuffer.clear()
					VLQ.encodeUnsignedInt(readBytes, streamChunkHeaderWriter)
					streamChunkHeaderBuffer.flip()
					channel.write(streamChunkBuffers, 0, 2, timeout, timeUnit, false, trailingStreamTransmissionHandler)
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

		/** Reads and transmits the next chunk from the active byte source, advances to the next pending source, or sends the terminal 0-length chunk. */
		def transmitNextStreamChunk(): Unit = {
			var continueLoop = true
			while continueLoop do {
				if currentStreamByteSource == null then {
					if pendingByteSources.isEmpty then {
						streamChunkHeaderBuffer.clear()
						streamChunkHeaderBuffer.put(0: Byte)
						streamChunkHeaderBuffer.flip()
						channel.write(streamChunkBuffers, 0, 1, timeout, timeUnit, true, trailingStreamTransmissionHandler)
						return
					} else {
						val source = pendingByteSources.poll()
						currentStreamByteSource = source
						currentStreamDeclaredSize = source.size
						currentStreamBytesRead = 0L
						try {
							currentAsyncReader = source.openAsyncReader()
						} catch {
							case scala.util.control.NonFatal(e) =>
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

		/** Delegate passed to [[channel.write]] to handle the consumption of the [[streamTransferBuffer]].
		 * @note Marked [[@threadUnsafe]] because the single-flight transmission pipeline guarantees initialization is never contested, and subsequent chunk writes are ordered across completion callbacks. */
		@threadUnsafe lazy val trailingStreamTransmissionHandler: CompletionHandler[java.lang.Long, Boolean] = new CompletionHandler[java.lang.Long, Boolean] { thisTransmissionHandler =>
			/** Handles completion of chunk frame transmission over the socket channel, advancing chunk headers or triggering next chunks.
			 * @param bytesWritten The number of bytes written to the channel during this operation.
			 * @param sentinelIsWritten Whether this write operation represented the terminal stream sentinel or peer abort sentinel. */
			override def completed(bytesWritten: java.lang.Long, sentinelIsWritten: Boolean): Unit = {
				if streamChunkBuffers(0).hasRemaining then {
					channel.write(streamChunkBuffers, 0, if sentinelIsWritten then 1 else 2, timeout, timeUnit, sentinelIsWritten, thisTransmissionHandler)
				} else if !sentinelIsWritten && streamChunkBuffers(1).hasRemaining then {
					channel.write(streamChunkBuffers, 1, 1, timeout, timeUnit, false, thisTransmissionHandler)
				} else if sentinelIsWritten then {
					cleanupStream()
					if cancellationReason ne null then completeTransmission(cancellationReason)
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
				completeTransmission(TransmissionFailure(message, exc))
			}
		}

		/** Asserts single-flight transmission: frameBuffer(1) is non-null while control packaging or trailing streaming is in progress.
		 * It remains non-null throughout trailing streaming until completeTransmission to prevent concurrent transmissions from corrupting wire state. */
		if frameBuffer(1) ne null then throw new TransmissionInProgressException

		pendingByteSources.clear()
		deduplicationMap.reset()

		/** Both,
		 *   - the [[Serializer.Writer]] instance used to serialize the control package of the message into the [[circularStorage]] buffers,
		 *   - and the delegate passed to [[channel.write]] to handle the consumption of those buffers.\
		 *
		 * Note that coordination is needed because the serialization is executed by the thread that called the [[transmit]] method, and the buffers consumption handling is executed by an NIO2 thread.\
		 * TODO consider moving this object outside the transmit method to allow re-usage. It would avoid an allocation per call to `transmit` but introduces a lot of mutable fields, which may require careful handling to ensure thread safety and correctness. */
		object controlPackageTransmissionHandler extends Writer(deduplicationMap), CompletionHandler[java.lang.Long, Boolean] { thisTransmissionHandler =>
			override val governingVersion: ProtocolVersion = msgVersion

			/** The current write position within the active write-end buffer. */
			override def position: Int = writeEndBuffer.position

			/** @inheritdoc
			 * @throws cluster.serialization.Serializer.SerializationException if the [[ByteSource.size]] is negative. */
			override def putByteSource(source: ByteSource): Unit = {
				val size = source.size
				if size < 0 then throw new SerializationException(writeEndBuffer.position, s"ByteSource size cannot be negative ($size)")
				putUnsignedLongVlq(size)
				if size > 0 then pendingByteSources.add(source)
			}

			private inline def completeOrSendCorruptedSentinel(sentinelWasIncluded: Boolean): Unit = {
				if sentinelWasIncluded then completeTransmission(cancellationReason.nn)
				else try sendControlSentinelValue() catch {
					case NonFatal(e) => handleAsyncChannelFailure(e)
				}
			}

			private def dispatchRemainingAfterCompletion(hasBufferToSend: Boolean, sentinelWasIncluded: Boolean): Unit = {
				if hasBufferToSend then try sendFrame(circularStorage.readEnd, circularStorage.hasPendingBuffersBehind) catch {
					case NonFatal(e) => handleAsyncChannelFailure(e)
				} else if sentinelWasIncluded then {
					// Transitions to streaming out-of-line `ByteSource` instances or completes transmission if no streams are pending.
					if pendingByteSources.isEmpty then completeTransmission(Delivered)
					else transmitNextStreamChunk()
				} else try sendControlSentinelValue() catch {
					case NonFatal(e) => handleAsyncChannelFailure(e)
				}
			}

			/** Obtains a [[java.nio.ByteBuffer]] having at least `minimumRemaining` bytes available, advancing circular storage when needed.
			 * @param minimumRemaining The minimum number of contiguous bytes required for the next serialization write.
			 * @return A [[java.nio.ByteBuffer]] positioned for writing with at least `minimumRemaining` bytes available.
			 * @throws cluster.channel.BuffersCountExceededException if advancing circular storage exceeds maximum capacity. */
			protected override def getBuffer(minimumRemaining: Int): ByteBuffer = {
				if behindTransmissionStarted && coordinationCell.get() == TRANSMISSION_FAILED then throw ioFailure.nn
				if writeEndBuffer.remaining() < minimumRemaining then {
					circularStorage.advanceWriteEnd().fold {
						throw new BuffersCountExceededException(s"Message transmission exceeded maximum buffer capacity ($maxBuffers buffers of $buffersCapacity bytes)")
					} { nextBuffer =>
						nextBuffer.clear()
						if !behindTransmissionStarted && behindTransmissionEnabled then {
							assert(writeEndBuffer eq circularStorage.readEnd)
							coordinationCell.set(SERIALIZING_AND_TRANSMITTING)
							sendFrame(writeEndBuffer, true)
							// Set after sendFrame returns so a synchronous channel.write throw leaves behindTransmissionStarted false and completes synchronously.
							behindTransmissionStarted = true
						}
						writeEndBuffer = nextBuffer
					}
				}
				writeEndBuffer
			}

			/** Handles asynchronous socket write completion for control package frames, advancing read buffers or initiating trailing streams.
			 * @param bytesTransmitted The number of bytes written to the channel during this operation.
			 * @param sentinelWasIncluded Whether this write operation included the package-terminating sentinel byte(s). */
			override def completed(bytesTransmitted: java.lang.Long, sentinelWasIncluded: Boolean): Unit = {
				scribe.trace(s"Transmission progress of message `$message` from `${context.myAddress}` to `${context.showPeerAddress}: bytesTransmitted=$bytesTransmitted, sentinelWasIncluded=$sentinelWasIncluded`, remaining=${frameBuffer(0).remaining()}+${if sentinelWasIncluded then 0 else frameBuffer(1).remaining()}")
				if frameBuffer(0).hasRemaining then try channel.write(frameBuffer, 0, if (frameBuffer(1) ne null) && frameBuffer(1).hasRemaining then 2 else 1, timeout, timeUnit, sentinelWasIncluded, thisTransmissionHandler) catch {
					case NonFatal(e) => handleAsyncChannelFailure(e)
				} else if (frameBuffer(1) ne null) && frameBuffer(1).hasRemaining then try channel.write(frameBuffer, 1, 1, timeout, timeUnit, sentinelWasIncluded, thisTransmissionHandler) catch {
					case NonFatal(e) => handleAsyncChannelFailure(e)
				} else {
					coordinationCell.get() match {
						case SERIALIZATION_COMPLETED_AND_TRANSMITTING =>
							dispatchRemainingAfterCompletion(circularStorage.advanceReadEnd(), sentinelWasIncluded)
						// This point may be reached only if behind transmission is enabled.
						// If serialization failed, dispatch the corrupted-package sentinel before completing with failure.
						case SERIALIZATION_FAILED =>
							completeOrSendCorruptedSentinel(sentinelWasIncluded)
						// This point may be reached only if behind transmission is enabled and the transmission of a frame completed before the caller thread finished serializing the full message.
						case SERIALIZING_AND_TRANSMITTING =>
							// If the read-end catches up to the write-end, pause behind transmission; otherwise advance to the next buffer and transmit it through the channel.
							val didAdvance = circularStorage.advanceReadEnd()
							if didAdvance && circularStorage.hasPendingBuffersBehind then try sendFrame(circularStorage.readEnd, true) catch {
								case NonFatal(e) => handleAsyncChannelFailure(e)
							} else { // Behind transmission is paused only when serialization is slower than transmission.
								coordinationCell.compareAndExchange(SERIALIZING_AND_TRANSMITTING, TRANSMISSION_PAUSED_WHILE_SERIALIZING) match {
									case SERIALIZING_AND_TRANSMITTING => () // Paused cleanly; the caller will resume final frame or corrupted sentinel dispatch.
									// Serialization finished or failed concurrently after advanceReadEnd(); handle the final state here instead of pausing.
									case SERIALIZATION_COMPLETED_AND_TRANSMITTING => dispatchRemainingAfterCompletion(didAdvance, sentinelWasIncluded)
									case SERIALIZATION_FAILED => completeOrSendCorruptedSentinel(sentinelWasIncluded)
									case unexpected => throw new AssertionError(s"Unexpected coordinationCell after CAS failure in NIO2 completion handler: $unexpected")
								}
							}
						case unexpected =>
							throw new AssertionError(s"Unexpected coordinationCell in NIO2 completion handler: $unexpected")
					}
				}
			}

			/** Delivers a [[Transmitter.TransmissionFailure]] when socket transmission of a control package frame fails.
			 * @param exc The network I/O exception that caused the frame write to fail.
			 * @param dummy Unused completion status parameter. */
			override def failed(exc: Throwable, dummy: Boolean): Unit = {
				handleAsyncChannelFailure(exc)
			}
		}

		/** Transitions [[coordinationCell]] after concurrent serialization ends, resuming dispatch from the caller if transmission paused or reporting failure if transmission already errored. */
		inline def finishHandoffFromCaller(targetState: Int)(inline pausedWriteOp: Unit): Unit = {
			coordinationCell.compareAndExchange(SERIALIZING_AND_TRANSMITTING, targetState) match {
				case SERIALIZING_AND_TRANSMITTING => () // Transmitter is still active and will act on `targetState` when its current write completes.
				case TRANSMISSION_PAUSED_WHILE_SERIALIZING =>
					// Transitions [[coordinationCell]] to `targetState` and executes `writeOp` synchronously from the caller thread when transmission is paused or was never started asynchronously.
					coordinationCell.set(targetState)
					try pausedWriteOp catch {
						case NonFatal(e) => completeTransmission(TransmissionFailure(message, e))
					}
				case TRANSMISSION_FAILED => completeTransmission(TransmissionFailure(message, ioFailure.nn))
				case unexpected => throw new AssertionError(s"Unexpected coordinationCell after serialization ended: $unexpected")
			}
		}

		writeEndBuffer.clear()
		var failureReport: NotDelivered | Null = null
		try {
			serializer.serialize(message, controlPackageTransmissionHandler)
		} catch {
			case se: SerializationException => failureReport = SerializationProblem(message, se, behindTransmissionStarted)
			case NonFatal(e) => failureReport = TransmissionFailure(message, e)
		}
		if failureReport eq null then {
			// If behind transmission started, notify the transmitter or resume from here if it already paused.
			if behindTransmissionStarted then finishHandoffFromCaller(SERIALIZATION_COMPLETED_AND_TRANSMITTING)(sendFrame(circularStorage.readEnd, circularStorage.hasPendingBuffersBehind))
			// Otherwise, initiate transmission sending the first frame.
			else {
				coordinationCell.set(SERIALIZATION_COMPLETED_AND_TRANSMITTING)
				try sendFrame(circularStorage.readEnd, circularStorage.hasPendingBuffersBehind) catch {
					case NonFatal(e) => completeTransmission(TransmissionFailure(message, e))
				}
			}
		} else {
			val report = failureReport.nn
			// If behind transmission started, record the cancellation reason and signal abort (or send the corrupted sentinel from here if transmission already paused).
			if behindTransmissionStarted then {
				cancellationReason = report
				finishHandoffFromCaller(SERIALIZATION_FAILED)(sendControlSentinelValue())
			} else {
				// No frames were sent to the channel, so call `onComplete` synchronously.
				completeTransmission(report)
			}
		}
	}
}
