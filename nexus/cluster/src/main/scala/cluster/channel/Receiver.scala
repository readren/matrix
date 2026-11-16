package readren.nexus
package cluster
package channel

import cluster.channel.Receiver.*
import cluster.misc.LoggingTools.*
import cluster.misc.{DualEndedCircularStorage, VLQ}
import cluster.serialization.{AllocationLimits, Deserializer, ProtocolVersion, RecyclableObjectBuffer}
import cluster.service.ChannelId
import cluster.service.Protocol.ContactAddress

import readren.common.{ByteSink, ByteSource, Maybe}
import scribe.LogFeature

import java.net.SocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousSocketChannel, CompletionHandler}
import java.util.ArrayDeque
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal

object Receiver {
	private inline val FRAME_HEADER_MAX_SIZE = VLQ.INT_MAX_LENGTH

	/** Default buffer capacity (8 KiB) for each direct circular storage buffer. */
	inline val DEFAULT_BUFFERS_CAPACITY = 8192
	/** Default maximum number of buffers in the [[DualEndedCircularStorage]]. */
	inline val DEFAULT_MAX_BUFFERS = 128

	/** Default capacity (64 KiB) for the direct streaming transfer buffer. */
	inline val DEFAULT_STREAM_TRANSFER_BUFFER_CAPACITY = 65536

	type Lazy[A <: Matchable] = misc.Lazy[A, Fault]

	sealed trait Fault {
		/** Generates diagnostic features for structured logging of this fault. */
		def logFeatures: Seq[LogFeature] = List(this.toString)

		/** Combines an explanatory message with this fault's log features for logging output.
		 * @param message An explanatory description of the error context.
		 * @return A sequence of [[scribe.LogFeature]] entries including the message and fault details. */
		def scribeContent(message: String): Seq[LogFeature] = message +: logFeatures
	}

	case class ChannelClosedByPeer(missingBytesAccordingToLastFrame: Int) extends Fault

	case class DeserializerAndFrameMismatch(origin: LengthMismatchException) extends Fault
	
	case class TheDeserializerHasNotConsumedTheWholePackage(remainingBytes: Int, deserializerResult: Any) extends Fault

	case class DeserializationProblem(problem: Throwable) extends Fault {
		override def logFeatures: Seq[LogFeature] = List(this.toString, problem)
	}

	case class ReceptionFailure(myAddress: ContactAddress, peerContactAddress: Maybe[ContactAddress], channelId: ChannelId, cause: Throwable) extends Fault {
		override def logFeatures: Seq[LogFeature] = List(this.toString, cause)
	}

	/** The [[Deserializer]] expected more bytes than the contained in the received package (a sequence of frames finalized with an empty frame). */
	class LengthMismatchException(message: String = "Unexpected end of package") extends RuntimeException(message)

	/** The received bytes are less than the package size. A package is a sequence of frames finalized with an empty one. */
	class UnexpectedBufferEnd extends RuntimeException("The buffer was consumed before the package end.")

	private enum SentinelFound {
		case NONE, UNTAINTED, TAINTED, STREAM_FOLLOWS, SPLIT
	}
	
	trait Context {
		/** The local contact address representing this node. */
		def myAddress: ContactAddress

		/** The optional contact address representing the remote peer, or [[readren.common.Maybe.empty]] if unknown. */
		def oPeerAddress: Maybe[ContactAddress]

		/** The unique identifier of the communication channel. */
		def channelId: ChannelId

		/** Formats the peer address as a human-readable string for diagnostic logs. */
		def showPeerAddress: String = oPeerAddress.fold("not-accesible")(_.toString)
	}
}

/** A capability for receiving messages over an [[java.nio.channels.AsynchronousSocketChannel]].
 *
 * Note: The underlying [[java.nio.channels.AsynchronousSocketChannel]] is not thread-safe and supports only one reception at a time. Concurrent attempts to receive messages will result in undefined behavior.
 *
 * Vocabulary: A "package" is a sequence of frames followed by a sentinel.
 *
 * @param channel The channel that connects the peer with us.
 * @param context Diagnostic addressing context for local and peer endpoints.
 * @param buffersCapacity The capacity of each buffer of the [[misc.DualEndedCircularStorage]].
 * @param maxBuffers The maximum number of buffers allowed in circular storage.
 * @param minBuffersOnIdle The minimum number of buffers retained when circular storage is pruned during idle transitions.
 * @param allocationLimits The security bounds governing maximum permissible allocations during deserialization.
 * @param byteSinkAllocator Allocator responsible for provisioning [[readren.common.ByteSink]] instances for out-of-line streams.
 * @param streamTransferBufferCapacity The capacity of the direct buffer used to transfer streaming chunks. */
class Receiver(
	channel: AsynchronousSocketChannel,
	context: Context,
	buffersCapacity: Int = Receiver.DEFAULT_BUFFERS_CAPACITY,
	val maxBuffers: Int = DEFAULT_MAX_BUFFERS,
	val minBuffersOnIdle: Int = 2,
	val allocationLimits: AllocationLimits = AllocationLimits.Default,
	val byteSinkAllocator: ByteSink.Allocator = ByteSink.DefaultAllocator,
	val streamTransferBufferCapacity: Int = Receiver.DEFAULT_STREAM_TRANSFER_BUFFER_CAPACITY
) {
	private val buffersInitialLimit = buffersCapacity - FRAME_HEADER_MAX_SIZE
	require(streamTransferBufferCapacity > 0, s"streamTransferBufferCapacity must be positive, got $streamTransferBufferCapacity")
	private val streamTransferBuffer: ByteBuffer = ByteBuffer.allocateDirect(streamTransferBufferCapacity)
	/** @note Concurrency invariant: Reusable queue safely shared across inbound message receptions because Receiver enforces single-flight message processing per socket channel. */
	private val pendingByteSinks: ArrayDeque[ByteSink] = new ArrayDeque[ByteSink]()
	/** Staging sinks that have already been filled and polled from [[pendingByteSinks]] during the active message's trailing stream, retained until the message completes so they can be discarded if a subsequent stream in the same package aborts or faults. */
	private val completedByteSinks: ArrayDeque[ByteSink] = new ArrayDeque[ByteSink]()

	assert(buffersCapacity > FRAME_HEADER_MAX_SIZE)
	require(maxBuffers.toLong * buffersCapacity.toLong <= Int.MaxValue, s"Aggregate circular buffer capacity (${maxBuffers.toLong * buffersCapacity.toLong} bytes) exceeds Int.MaxValue")
	private val totalBufferCapacity: Int = maxBuffers * buffersCapacity
	private val circularStorage: DualEndedCircularStorage[ByteBuffer] = new DualEndedCircularStorage[ByteBuffer](() => ByteBuffer.allocateDirect(buffersCapacity), maxBuffers)

	/** The total number of buffers currently allocated in circular storage. */
	def bufferCount: Int = circularStorage.bufferCount
	private val deduplicationBuffer: RecyclableObjectBuffer = new RecyclableObjectBuffer()

	/** Isolated staging buffer holding pipelined bytes that arrived in [[streamTransferBuffer]] past the terminal chunk sentinel of a trailing stream.
	 * Pipelined TCP streams may pack the beginning of subsequent messages into the same socket read that delivers the stream's final chunk sentinel. Slicing or retaining these leftover bytes in [[streamTransferBuffer]] directly is unsafe because if the subsequent message also contains a trailing stream, [[streamTransferBuffer]] will be immediately cleared and reused for the next stream's network reads, clobbering the unconsumed header bytes in-place. Staging leftover bytes in this dedicated, isolated buffer decouples downstream message framing from stream buffer recycling, eliminates cross-stream memory aliasing, and guarantees that circular storage ingestion proceeds deterministically without premature ring distribution. */
	private var streamTransferLeftover: ByteBuffer | Null = null

	/** Intercepts read requests intended for circular storage, transparently feeding staged pipelined bytes before delegating to the underlying socket [[channel]].
	 * When a trailing stream terminates, any leftover bytes staged in [[streamTransferLeftover]] are siphoned into the target circular storage buffer (`dst`) through standard [[java.nio.channels.CompletionHandler.completed]] dispatches. Once [[streamTransferLeftover]] is completely drained, this interceptor falls back to physical socket reads via [[java.nio.channels.AsynchronousByteChannel.read]]. Virtualizing socket reads ensures that pipelined bytes re-enter circular storage strictly through the standard framing loop, preserving frame header decoding, split-header continuation, buffer limit extensions, and read-pointer advancement invariants without ad-hoc buffer slicing.
	 * @param dst The destination circular storage buffer to receive incoming bytes.
	 * @param timeout The maximum duration allowed for the physical read operation.
	 * @param timeUnit The time unit of the `timeout` argument.
	 * @param att The attachment object passed to the completion handler.
	 * @param completionHandler The callback notified when bytes have been transferred into `dst` or upon physical I/O failure. */
	private def readChannel(dst: ByteBuffer, timeout: Long, timeUnit: TimeUnit, att: ByteBuffer, completionHandler: CompletionHandler[Integer, ByteBuffer]): Unit = {
		if streamTransferLeftover != null && streamTransferLeftover.hasRemaining then {
			// Siphon available leftover bytes into dst up to the remaining capacity of dst.
			val toCopy = math.min(dst.remaining(), streamTransferLeftover.remaining())
			val slice = streamTransferLeftover.slice()
			slice.limit(toCopy)
			dst.put(slice)
			streamTransferLeftover.position(streamTransferLeftover.position() + toCopy)
			// Release reference immediately once fully drained to enable prompt garbage collection.
			if !streamTransferLeftover.hasRemaining then streamTransferLeftover = null
			// Simulate an asynchronous channel read completion with the exact byte count copied.
			completionHandler.completed(toCopy, att)
		} else {
			streamTransferLeftover = null
			channel.read(dst, timeout, timeUnit, att, completionHandler)
		}
	}

	/** A [[misc.VLQ.ByteReader]] that reads the bytes from the [[java.nio.ByteBuffer]] it is attached to, consuming its content.
	 * Design note: Defining this object here (as a member of [[Receiver]] instead of locally - at the usage site) is not neat, but it prevents the creation of a [[misc.VLQ.ByteReader]] instance for each frame received from the channel. */
	private object frameHeaderReader extends VLQ.ByteReader {
		private var bufferPositionedAtFrameHeader: ByteBuffer | Null = null

		/** Attaches this reader to the provided buffer.
		 * - Buffer must already be positioned at the header start.
		 * - Actual mutation (consumption) of the buffer will happen when the `readByte()` method is called by the VLQ decoder method.
		 * @param bufferPositionedAtFrameHeader A [[java.nio.ByteBuffer]] that contains and points to the frame header to be read. */
		def attachTo(bufferPositionedAtFrameHeader: ByteBuffer): Unit = {
			this.bufferPositionedAtFrameHeader = bufferPositionedAtFrameHeader
		}

		/** Reads the next byte from the attached buffer, advancing its position. */
		override def readByte(): Byte =
			bufferPositionedAtFrameHeader.get()
	}

	/** A [[misc.VLQ.ByteReader]] that reads the bytes from [[java.nio.ByteBuffer]] it is attached to, without mutating it.
	 * Design note: Defining this object here (as a member of [[Receiver]] instead of locally - at the usage site) is not neat, but it prevents the creation of a [[misc.VLQ.ByteReader]] instance for each frame received from the channel. */
	private object frameHeaderFetcher extends VLQ.BoundedByteReader {
		private var backingBuffer: ByteBuffer | Null = null
		private var startingReadPosition: Int = 0
		private var readPosition: Int = 0
		private var limit: Int = 0

		/** The total number of bytes inspected by this reader since it was last attached. */
		inline def numberOfBytesRead: Int = readPosition - startingReadPosition

		/** Attaches this reader to the provided [[java.nio.ByteBuffer]] at the specified position with the specified limit.
		 * The buffer is not mutated by this method nor the [[readByte]]
		 * @param bufferWithHeader A [[java.nio.ByteBuffer]] that contains the frame header to be read.
		 * @param headerPosition The absolute position of the frame header within the provided buffer.
		 * @param limit The absolute limit within the buffer beyond which reading must not proceed. */
		def attachTo(bufferWithHeader: ByteBuffer, headerPosition: Int, limit: Int): Unit = {
			backingBuffer = bufferWithHeader
			startingReadPosition = headerPosition
			readPosition = headerPosition
			this.limit = limit
		}

		/** Checks whether more uninspected bytes remain before the limit. */
		override def hasMoreBytes: Boolean = readPosition < limit

		/** Reads the next byte of the attached buffer without mutating it. */
		override def readByte(): Byte = {
			val byte = backingBuffer.get(readPosition)
			readPosition += 1
			byte
		}
	}

	/** Starts consuming bytes received or being received through the [[channel]] and uses them as the source for the provided [[cluster.serialization.Deserializer]].
	 * The `onComplete` callback is invoked when the `deserializer` completes its operation. The deserializer begins \
	 * processing once sufficient bytes have been received through the channel.
	 *
	 * @param msgVersion The version of the received message. This value depends on the project version of the peer.
	 * @param attachment An attachment of type `A` that is passed through to the `onComplete` callback.
	 * @param timeout The maximum time to wait for the first chunk of bytes and between chunks of bytes.
	 * @param timeUnit The unit of time for the `timeout` parameter.
	 * @param onComplete A callback that is invoked with either the deserialized message of type `M` or a [[Receiver.Fault]] object, along with the provided `attachment`.
	 * @param deserializer An implicit [[cluster.serialization.Deserializer]] used to deserialize the incoming bytes into a message of type `M`.
	 * @tparam M The type of the message to deserialize.
	 * @tparam A The type of the attachment passed through to the completion callback.
	 * @throws java.nio.channels.ReadPendingException if a read operation is already in flight on this channel.
	 * @throws java.nio.channels.ShutdownChannelGroupException if the underlying channel group has shut down. */
	def receiveWithAttachment[M, A](msgVersion: ProtocolVersion, attachment: A, timeout: Long, timeUnit: TimeUnit)(onComplete: (M | Fault, A) => Unit)(using deserializer: Deserializer[M]): Unit = {
		deduplicationBuffer.reset()
		pendingByteSinks.clear()
		completedByteSinks.clear()

		/** Implementation note: To optimize memory usage, this object can be moved outside the [[receiveWithAttachment]] method. This avoids repeated memory allocations but introduces mutable fields, which may require careful handling to ensure thread safety and correctness. */
		object handler extends Deserializer.Reader(deduplicationBuffer, allocationLimits), CompletionHandler[Integer, ByteBuffer] { thisHandler =>

			var remainingContentBytesUntilNextFrameHeader = 0
			var readEndBuffer: ByteBuffer = circularStorage.readEnd
			var nextFrameHeaderPosRelativeToReadEndBufferBase = 0
			var packageContentBytesCommitted: Int = 0
			var currentPackageContentBytesBound: Int = Int.MaxValue

			/** The protocol version governing deserialization of this message. */
			override def governingVersion: ProtocolVersion = msgVersion

			/** The current read position within the active circular storage buffer. */
			override def position: Int = readEndBuffer.position

			/** The remaining content bytes bound for the current package. */
			override def remainingContentBytes: Int = currentPackageContentBytesBound

			/** @inheritdoc
			 * @throws cluster.serialization.Deserializer.NegativeLengthException if the decoded stream length is negative.
			 * @throws cluster.serialization.Deserializer.AllocationThresholdExceededException if the declared length exceeds \
			 * [[allocationLimits.maxByteSourceBytes]].
			 * @throws LengthMismatchException if the [[cluster.serialization.Deserializer]] tries to read more bytes than \
			 * contained in the package.
			 * @throws UnexpectedBufferEnd if all received bytes were consumed and the package was not fully read. */
			override def readByteSource(): ByteSource = {
				val size = readUnsignedLongVlq()
				if size < 0 then throw new Deserializer.NegativeLengthException(position, size, "ByteSource")
				if size == 0L then readren.common.BufferByteSource(Array.emptyByteArray)
				else {
					if size > thisHandler.allocationLimits.maxByteSourceBytes then {
						throw new Deserializer.AllocationThresholdExceededException(position, size, thisHandler.allocationLimits.maxByteSourceBytes, "ByteSource")
					}
					val sink = byteSinkAllocator.allocate(size)
					pendingByteSinks.add(sink)
					sink.byteSource
				}
			}

			/** Peeks the next byte without advancing the read position, skipping frame headers if encountered.
			 * @throws LengthMismatchException if the [[cluster.serialization.Deserializer]] tries to read more bytes than \
			 * contained in the package. A package is a sequence of frames finalized with an empty frame.
			 * @throws UnexpectedBufferEnd if all received bytes were consumed and the package was not fully read. */
			override def peekByte: Byte = {
				skipFrameHeader()
				readEndBuffer.get(readEndBuffer.position)
			}

			/** Reads the next content byte, advancing the read position and skipping frame headers if encountered.
			 * @throws LengthMismatchException if the [[cluster.serialization.Deserializer]] tries to read more bytes than \
			 * contained in the package.
			 * @throws UnexpectedBufferEnd if all received bytes were consumed and the package was not fully read. */
			override def readByte(): Byte = {
				skipFrameHeader()
				readEndBuffer.get()
			}

			/** Advances circular storage read-end to the next buffer or throws [[UnexpectedBufferEnd]] if no buffers remain.
			 * @throws UnexpectedBufferEnd if all received bytes were consumed and the package was not fully read. */
			private def advanceReadEndOrFail(): Unit = {
				if circularStorage.advanceReadEnd() then {
					nextFrameHeaderPosRelativeToReadEndBufferBase -= readEndBuffer.limit
					readEndBuffer = circularStorage.readEnd
				}
				else throw new UnexpectedBufferEnd()
			}

			/** Skips frame headers and buffer boundaries such that the next call to [[readEndBuffer.get()]] returns a content byte. 
			 * @throws LengthMismatchException if the [[cluster.serialization.Deserializer]] tries to read more bytes than \
			 * contained in the package. A package is a sequence of frames finalized with an empty frame.
			 * @throws UnexpectedBufferEnd if all received bytes were consumed and the package was not fully read. */
			private def skipFrameHeader(): Unit = {
				if readEndBuffer.remaining == 0 then advanceReadEndOrFail()

				if readEndBuffer.position == nextFrameHeaderPosRelativeToReadEndBufferBase then {
					// Invariant: readEndBuffer has at least FRAME_HEADER_MAX_SIZE remaining; completed() ensures this across buffer boundaries by extending the buffer limit into reserved capacity.
					frameHeaderReader.attachTo(readEndBuffer)
					val currentFrameContentLength = VLQ.decodeUnsignedInt(frameHeaderReader)
					if currentFrameContentLength == 0 then throw new LengthMismatchException()
					nextFrameHeaderPosRelativeToReadEndBufferBase = readEndBuffer.position() + currentFrameContentLength
					if readEndBuffer.remaining == 0 then advanceReadEndOrFail()
				}
			}

			/** Reads a 16-bit signed integer, skipping frame headers and crossing buffer boundaries if necessary. */
			override def readShort(): Short = {
				skipFrameHeader()
				val bytesUntilHeader = nextFrameHeaderPosRelativeToReadEndBufferBase - readEndBuffer.position
				if bytesUntilHeader >= 2 && readEndBuffer.remaining >= 2 then readEndBuffer.getShort
				else super.readShort()
			}

			/** Reads a 32-bit signed integer, skipping frame headers and crossing buffer boundaries if necessary. */
			override def readInt(): Int = {
				skipFrameHeader()
				val bytesUntilHeader = nextFrameHeaderPosRelativeToReadEndBufferBase - readEndBuffer.position
				if bytesUntilHeader >= 4 && readEndBuffer.remaining >= 4 then readEndBuffer.getInt
				else super.readInt()
			}

			/** Reads a 64-bit signed integer, skipping frame headers and crossing buffer boundaries if necessary. */
			override def readLong(): Long = {
				skipFrameHeader()
				val bytesUntilHeader = nextFrameHeaderPosRelativeToReadEndBufferBase - readEndBuffer.position
				if bytesUntilHeader >= 8 && readEndBuffer.remaining >= 8 then readEndBuffer.getLong
				else super.readLong()
			}

			/** Reads a 32-bit floating point number, skipping frame headers and crossing buffer boundaries if necessary. */
			override def readFloat(): Float = {
				skipFrameHeader()
				val bytesUntilHeader = nextFrameHeaderPosRelativeToReadEndBufferBase - readEndBuffer.position
				if bytesUntilHeader >= 4 && readEndBuffer.remaining >= 4 then readEndBuffer.getFloat
				else super.readFloat()
			}

			/** Reads a 64-bit floating point number, skipping frame headers and crossing buffer boundaries if necessary. */
			override def readDouble(): Double = {
				skipFrameHeader()
				val bytesUntilHeader = nextFrameHeaderPosRelativeToReadEndBufferBase - readEndBuffer.position
				if bytesUntilHeader >= 8 && readEndBuffer.remaining >= 8 then readEndBuffer.getDouble
				else super.readDouble()
			}

			/** Reads a contiguous sequence of bytes into the destination array, skipping frame headers across buffers.
			 * @param destination The array into which bytes are written.
			 * @param offset The starting offset within the destination array.
			 * @param length The number of bytes to read into the destination array. */
			override def readBytes(destination: Array[Byte], offset: Int, length: Int): Unit = {
				var currOffset = offset
				var remaining = length
				while remaining > 0 do {
					skipFrameHeader()
					val bytesUntilHeader = nextFrameHeaderPosRelativeToReadEndBufferBase - readEndBuffer.position
					val available = math.min(readEndBuffer.remaining, bytesUntilHeader)
					val chunk = math.min(remaining, available)
					readEndBuffer.get(destination, currOffset, chunk)
					currOffset += chunk
					remaining -= chunk
				}
			}

			/** Deserializes the control package frames into a message instance or returns a diagnostic [[Receiver.Fault]].
			 * @param writeEndBuffer The buffer containing the final bytes of the received control package.
			 * @param sentinelPos The buffer position where the package-terminating sentinel was identified.
			 * @param totalContentBytes The total number of content payload bytes received across all package frames.
			 * @return Either the successfully deserialized message of type `M`, or a [[Receiver.Fault]] describing the failure. */
			private def deserializePackage(writeEndBuffer: ByteBuffer, sentinelPos: Int, totalContentBytes: Int): M | Fault = {
				scribe.trace(s"Reception progress at `${context.myAddress}` from `${context.showPeerAddress}`: before deserialization: sentinelPos=$sentinelPos, nextFrameHeaderPosRelativeToReadEndBufferBase=$nextFrameHeaderPosRelativeToReadEndBufferBase, readEndPos=${readEndBuffer.position()}, circularStorage=${show.circularStorage(circularStorage, false).end}")
				currentPackageContentBytesBound = totalContentBytes
				try {
					skipFrameHeader()
					val message = deserializer.deserialize(thisHandler)
					// Catch up readEndBuffer to writeEndBuffer if all bytes in previous buffers were consumed
					while (readEndBuffer ne writeEndBuffer) && !readEndBuffer.hasRemaining do {
						if circularStorage.advanceReadEnd() then {
							nextFrameHeaderPosRelativeToReadEndBufferBase -= readEndBuffer.limit
							readEndBuffer = circularStorage.readEnd
						}
					}
					// if the deserializer consumed all the bytes in the package, return a successful outcome.
					if (readEndBuffer eq writeEndBuffer) && readEndBuffer.position() == sentinelPos then message
					// else return a faulty outcome
					else {
						var notConsumedBytesAccumulator = 0
						if readEndBuffer ne writeEndBuffer then {
							notConsumedBytesAccumulator += readEndBuffer.remaining()
							while circularStorage.advanceReadEnd() && (circularStorage.readEnd ne writeEndBuffer) do {
								notConsumedBytesAccumulator += circularStorage.readEnd.remaining()
							}
							notConsumedBytesAccumulator += sentinelPos - writeEndBuffer.position()
						} else {
							notConsumedBytesAccumulator += sentinelPos - readEndBuffer.position()
						}
						TheDeserializerHasNotConsumedTheWholePackage(notConsumedBytesAccumulator, message)
					}
				} catch {
					case lme: LengthMismatchException => DeserializerAndFrameMismatch(lme)
					case scala.util.control.NonFatal(e) => DeserializationProblem(e)
				} finally {
					currentPackageContentBytesBound = Int.MaxValue
				}
			}

			/** Prepares circular storage for the next message reception cycle by compacting unconsumed bytes or advancing the write-end.
			 * @param writeEndBuffer The active write-end buffer after receiving the package sentinel.
			 * @param sentinelPos The position of the package sentinel within `writeEndBuffer`.
			 * @param posOfFirstRemainingByteOfTheWriteEndBuffer The position immediately following the last received byte. */
			inline private def prepareCircularStorageForNextReception(writeEndBuffer: ByteBuffer, sentinelPos: Int, posOfFirstRemainingByteOfTheWriteEndBuffer: Int): Unit = {
				val nextPackageHeaderPos = sentinelPos + 1
				while circularStorage.advanceReadEnd() do ()
				writeEndBuffer.position(nextPackageHeaderPos)
				writeEndBuffer.limit(posOfFirstRemainingByteOfTheWriteEndBuffer)
				writeEndBuffer.compact()
				writeEndBuffer.limit(buffersInitialLimit)
			}

			/** Consumes trailing stream chunks following a control package until a 0-length terminal chunk or error sentinel is encountered.
			 *
			 * ==Framing Bounds & Security==
			 * - Each chunk begins with a VLQ-encoded length header bounded to at most 5 continuation bytes (`chunkHeaderShift < 28`). Longer sequences trigger a fatal [[LengthMismatchException]] to reject malicious or desynchronized streams.
			 * - Chunk lengths must be non-negative and are bounded by the remaining expected stream length, preventing stream inflation while allowing arbitrary chunk sizes across network transports.
			 *
			 * ==Sink Dispatch Contract==
			 * - Supports both synchronous and asynchronous [[readren.common.ByteSink]] implementations: when [[readren.common.ByteSink.write]] returns non-negative bytes written, synchronous completion proceeds inline without trampoline recursion; when [[readren.common.ByteSink.PENDING]] is returned, parsing yields until the asynchronous completion callback resumes buffer processing.
			 *
			 * ==Socket Teardown Invariant==
			 * - Any mid-stream framing defect, sink I/O failure, draining control fault, or byte count mismatch immediately closes the underlying socket channel and unlinks all staging files, preventing desynchronized bytes from contaminating subsequent package deserialization.
			 * - A clean in-band peer stream abort (`CORRUPTED_PACKAGE_SENTINEL` at a chunk boundary without local or framing faults) discards staging sinks and skips the aborted package while keeping the socket channel open for subsequent messages.
			 * @param outcome The outcome of deserializing the control package, which may be a valid message `M` or a [[Receiver.Fault]].
			 * @param initialSource The buffer containing leftover unconsumed bytes immediately following the stream-follows sentinel. */
			private def receiveTrailingStream(outcome: M | Fault, initialSource: ByteBuffer): Unit = {
				var isDrainingFault: Boolean = outcome.isInstanceOf[Fault]
				var currentSink: ByteSink | Null = if !isDrainingFault then pendingByteSinks.peek() else null
				var currentSinkBytesRemaining: Long = if currentSink != null then currentSink.expectedSize else 0L
				var currentChunkBytesRemaining: Int = 0
				var totalStreamedBytes: Long = 0L
				val expectedTotalBytes: Long = if !isDrainingFault then {
					var sum = 0L
					val it = pendingByteSinks.iterator()
					while it.hasNext do sum += it.next().expectedSize
					sum
				} else 0L
				var streamFinished: Boolean = false
				var streamAbortedByPeer: Boolean = false
				var streamProtocolFault: Fault | Null = null

				var chunkHeaderValue: Int = 0
				var chunkHeaderShift: Int = 0
				var currentActiveSource: ByteBuffer = initialSource
				var sinkToSealOnComplete: ByteSink | Null = null
				val streamTrampoline: AtomicInteger = new AtomicInteger(0)
				@volatile var awaitingSinkWrite: Boolean = false
				@volatile var sinkWriteCompleted: Boolean = false
				@volatile var sinkWriteFailed: Throwable | Null = null
				@volatile var awaitingChannelRead: Boolean = false
				@volatile var channelReadCompleted: Boolean = false
				@volatile var channelReadResult: Int = 0
				@volatile var channelReadFailed: Throwable | Null = null

				/** Closes and discards all pending, active, and previously sealed staging sinks of the current message to delete temporary files from disk upon reception faults, peer aborts, or framing mismatches. */
				def cleanupStagingSinks(): Unit = {
					if sinkToSealOnComplete != null then {
						// Rollback teardown: Suppress secondary discard errors to prevent masking primary failure causes
						try sinkToSealOnComplete.discard() catch { case NonFatal(_) => () }
						sinkToSealOnComplete = null
					}
					if currentSink != null then {
						// Rollback teardown: Suppress secondary discard errors to prevent masking primary failure causes
						try currentSink.discard() catch { case NonFatal(_) => () }
						currentSink = null
					}
					while !pendingByteSinks.isEmpty do {
						val sink = pendingByteSinks.poll()
						// Rollback loop: Suppress secondary discard errors to ensure all pending staging sinks are unlinked from disk
						try sink.discard() catch { case NonFatal(_) => () }
					}
					while !completedByteSinks.isEmpty do {
						val sink = completedByteSinks.poll()
						// Rollback loop: Suppress secondary discard errors to ensure already-sealed staging sinks of an aborted multi-stream package are unlinked from disk
						try sink.discard() catch { case NonFatal(_) => () }
					}
				}

				/** Finalizes trailing stream reception, safely isolating pipelined leftover bytes and completing the reception lifecycle.
				 * @param leftoverSource The buffer holding unconsumed bytes following the terminal chunk sentinel, or `null`. */
				def finishTrailingStream(leftoverSource: ByteBuffer | Null): Unit = {
					// Catch up the read-end of circular storage: any buffers preceding writeEnd from previous reads are marked consumed.
					while circularStorage.advanceReadEnd() do ()
					val target = circularStorage.writeEnd
					val existingStaged = streamTransferLeftover
					val stagedRem = if existingStaged != null then existingStaged.remaining() else 0
					if (leftoverSource ne target) || stagedRem > 0 then {
						// Case 1: The trailing stream read beyond the initial packet into [[streamTransferBuffer]] (or leftover bytes from an earlier stream remain in [[streamTransferLeftover]]).
						// Any unconsumed pipelined bytes in `leftoverSource` followed by any remaining bytes in [[streamTransferLeftover]] belong to subsequent messages.
						// Stage them in chronological order into an isolated heap buffer ([[streamTransferLeftover]]) and reset circular storage's write target to empty.
						val leftoverRem = if leftoverSource != null then leftoverSource.remaining() else 0
						if leftoverRem + stagedRem > 0 then {
							val staged = ByteBuffer.allocate(leftoverRem + stagedRem)
							if leftoverRem > 0 then staged.put(leftoverSource.nn)
							if stagedRem > 0 then staged.put(existingStaged.nn)
							staged.flip()
							streamTransferLeftover = staged
						} else {
							streamTransferLeftover = null
						}
						target.clear()
						target.limit(buffersInitialLimit)
					} else {
						// Case 2: The entire stream was contained in the initial `writeEndBuffer` (`initialSource`) and `streamTransferLeftover` is empty.
						// Calling compact() shifts unconsumed pipelined bytes from [position, limit) to [0, remaining) in-place without allocation.
						target.compact()
						// Given all buffers initially respect `buffersInitialLimit` and at least the 5-byte STREAM_FOLLOWS_SENTINEL plus terminal chunk header (>= 6 bytes) are consumed before the previous `target.compact()` call, `target.position()` cannot exceed `buffersInitialLimit`.
						assert(target.position <= buffersInitialLimit)
						target.limit(buffersInitialLimit)
						streamTransferLeftover = null
					}
					val curTarget = circularStorage.writeEnd
					// Prune circular storage down to minBuffersOnIdle only when no pipelined bytes are waiting in target or streamTransferLeftover.
					if curTarget.position == 0 && streamTransferLeftover == null then circularStorage.pruneTo(minBuffersOnIdle)

					if isDrainingFault || streamProtocolFault != null then {
						// Socket teardown upon local or protocol fault: Suppress secondary close exceptions to ensure staging sink cleanup and onComplete callback execute with primary fault preserved
						try channel.close() catch { case NonFatal(_) => () }
						cleanupStagingSinks()
						val faultToReport = if streamProtocolFault != null then streamProtocolFault else outcome
						onComplete(faultToReport, attachment)
					} else if streamAbortedByPeer then {
						cleanupStagingSinks()
						scribe.warn(s"Reception progress at `${context.myAddress}` from `${context.showPeerAddress}`: A package with an aborted trailing stream was skipped")
						receiveWithAttachment[M, A](msgVersion, attachment, timeout, timeUnit)(onComplete)
					} else if totalStreamedBytes != expectedTotalBytes || !pendingByteSinks.isEmpty || currentSink != null then {
						// Socket teardown upon framing length mismatch: Suppress secondary close exceptions to ensure cleanupStagingSinks and DeserializerAndFrameMismatch delivery execute reliably
						try channel.close() catch { case NonFatal(_) => () }
						cleanupStagingSinks()
						onComplete(DeserializerAndFrameMismatch(new LengthMismatchException()), attachment)
					} else {
						pendingByteSinks.clear()
						completedByteSinks.clear()
						onComplete(outcome, attachment)
					}
				}

				/** Asynchronous completion handler for chunk writes dispatched to staging [[readren.common.ByteSink]] instances. */
				object sinkWriteHandler extends CompletionHandler[Integer, Null] {
					/** Records completion of an asynchronous sink write and schedules the trailing stream trampoline.
					 * @param written The number of bytes written to the sink.
					 * @param att Unused attachment parameter. */
					override def completed(written: Integer, att: Null): Unit = {
						sinkWriteCompleted = true
						scheduleStreamIngestion()
					}

					/** Records failure of an asynchronous sink write and schedules the trailing stream trampoline.
					 * @param exc The I/O exception thrown by the sink write.
					 * @param att Unused attachment parameter. */
					override def failed(exc: Throwable, att: Null): Unit = {
						sinkWriteFailed = exc
						scheduleStreamIngestion()
					}
				}

				/** Asynchronous completion handler for channel reads into [[streamTransferBuffer]] during trailing stream ingestion. */
				object streamReader extends CompletionHandler[Integer, ByteBuffer] {
					/** Records the number of stream bytes read into [[streamTransferBuffer]] (or `-1` on EOF) and schedules the trailing stream trampoline.
					 * @param bytesRead The number of bytes read from the channel, or -1 if the channel reached EOF.
					 * @param buf The transfer buffer into which stream bytes were read. */
					override def completed(bytesRead: Integer, buf: ByteBuffer): Unit = {
						channelReadResult = bytesRead.intValue()
						channelReadCompleted = true
						scheduleStreamIngestion()
					}

					/** Records a network read failure during trailing stream ingestion and schedules the trailing stream trampoline.
					 * @param exc The network I/O exception encountered while reading stream bytes.
					 * @param buf The transfer buffer passed to the channel read. */
					override def failed(exc: Throwable, buf: ByteBuffer): Unit = {
						channelReadFailed = exc
						scheduleStreamIngestion()
					}
				}

				/** Executes a single serialized step of trailing stream ingestion under [[streamTrampoline]] ownership, returning `0` while in progress, `1` when the stream has finished or faulted, `2` on peer EOF during a stream read, or `3` on a socket read exception. */
				def stepStreamIngestion(): Int = {
					if awaitingSinkWrite then {
						val failExc = sinkWriteFailed
						if failExc != null then {
							sinkWriteFailed = null
							awaitingSinkWrite = false
							if sinkToSealOnComplete != null then {
								// Best-effort rollback: Suppress secondary discard errors so primary write failure exc is preserved in ReceptionFailure
								try sinkToSealOnComplete.discard() catch { case NonFatal(_) => () }
								sinkToSealOnComplete = null
							}
							isDrainingFault = true
							streamProtocolFault = ReceptionFailure(context.myAddress, context.oPeerAddress, context.channelId, failExc)
						} else if sinkWriteCompleted then {
							sinkWriteCompleted = false
							awaitingSinkWrite = false
							if sinkToSealOnComplete != null then {
								try sinkToSealOnComplete.seal() catch {
									case NonFatal(e) =>
										streamProtocolFault = ReceptionFailure(context.myAddress, context.oPeerAddress, context.channelId, e)
								}
								sinkToSealOnComplete = null
							}
						} else {
							return 0
						}
					}

					if awaitingChannelRead then {
						if channelReadFailed != null then {
							awaitingChannelRead = false
							return 3
						} else if channelReadCompleted then {
							val bytesRead = channelReadResult
							channelReadCompleted = false
							awaitingChannelRead = false
							if bytesRead == -1 then {
								return 2
							} else {
								streamTransferBuffer.flip()
								currentActiveSource = streamTransferBuffer
							}
						} else {
							return 0
						}
					}

					val source = currentActiveSource
					while !awaitingSinkWrite && source.hasRemaining && !streamFinished && (streamProtocolFault == null) do {
						// State 1: Decode the next chunk length header (VLQ32) across buffer boundaries.
						if currentChunkBytesRemaining == 0 then {
							var headerComplete = false
							while source.hasRemaining && !headerComplete do {
								val b = source.get()
								// Security guard: Reject VLQ sequences exceeding 5 continuation bytes (32-bit unsigned) to prevent integer overflow and busy-spins.
								if chunkHeaderShift > 28 || (chunkHeaderShift == 28 && (b & 0x80) != 0) then {
									streamProtocolFault = DeserializerAndFrameMismatch(new LengthMismatchException("Chunk header VLQ continuation length exceeds maximum 5 bytes"))
									headerComplete = true
								} else {
									chunkHeaderValue |= (b & 0x7f) << chunkHeaderShift
									chunkHeaderShift += 7
									if (b & 0x80) == 0 then headerComplete = true
								}
							}
							if headerComplete && (streamProtocolFault == null) then {
								val chunkLen = chunkHeaderValue
								chunkHeaderValue = 0
								chunkHeaderShift = 0
								if chunkLen == 0 then {
									// Terminal sentinel (0x00): Trailing stream completed uncorrupted.
									streamFinished = true
								} else if chunkLen == Transmitter.CORRUPTED_PACKAGE_SENTINEL.toInt then {
									// In-band peer abort sentinel: Sender failed during serialization.
									streamAbortedByPeer = true
									streamFinished = true
								} else if chunkLen < 0 || (!isDrainingFault && (totalStreamedBytes + chunkLen > expectedTotalBytes)) then {
									// Framing error: Reject negative lengths or chunk lengths exceeding declared stream size.
									streamProtocolFault = DeserializerAndFrameMismatch(new LengthMismatchException(s"Invalid chunk length: $chunkLen"))
								} else {
									// Valid chunk length header: Set remaining chunk bytes to consume.
									currentChunkBytesRemaining = chunkLen
								}
							}
						}

						// State 2: Stream chunk payload bytes into the active staging ByteSink.
						if currentChunkBytesRemaining > 0 && source.hasRemaining && (streamProtocolFault == null) then {
							val available = math.min(source.remaining(), currentChunkBytesRemaining)
							if isDrainingFault || currentSink == null then {
								// Draining mode: The control package already faulted; drain and discard remaining stream bytes to clear channel state.
								if !isDrainingFault && currentSink == null then {
									streamProtocolFault = DeserializerAndFrameMismatch(new LengthMismatchException())
								}
								source.position(source.position() + available)
								currentChunkBytesRemaining -= available
								totalStreamedBytes += available
							} else {
								// Normal ingestion: Write at most what the current sink expects.
								val toWrite = math.min(available.toLong, currentSinkBytesRemaining).toInt
								val slice = source.slice()
								slice.limit(toWrite)
								source.position(source.position() + toWrite)
								totalStreamedBytes += toWrite
								currentSinkBytesRemaining -= toWrite
								currentChunkBytesRemaining -= toWrite

								// When current sink reaches its declared capacity, advance to the next queued sink and stage current for sealing.
								val sinkForThisWrite = currentSink
								if currentSinkBytesRemaining == 0L then {
									sinkToSealOnComplete = sinkForThisWrite
									val polled = pendingByteSinks.poll()
									if polled != null then completedByteSinks.add(polled)
									currentSink = pendingByteSinks.peek()
									currentSinkBytesRemaining = if currentSink != null then currentSink.expectedSize else 0L
								}

								// Arm awaitingSinkWrite before calling write() so that inline or concurrent callbacks on sinkWriteHandler are serialized by streamTrampoline.
								awaitingSinkWrite = true
								val writeStatus = try {
									sinkForThisWrite.write(slice, null, sinkWriteHandler)
								} catch {
									case NonFatal(e) =>
										awaitingSinkWrite = false
										if sinkToSealOnComplete != null then {
											try sinkToSealOnComplete.discard() catch { case NonFatal(_) => () }
											sinkToSealOnComplete = null
										}
										isDrainingFault = true
										streamProtocolFault = ReceptionFailure(context.myAddress, context.oPeerAddress, context.channelId, e)
										0
								}
								if awaitingSinkWrite && writeStatus != ByteSink.PENDING then {
									// Synchronous completion without invoking sinkWriteHandler: seal completed sink immediately and continue inline.
									awaitingSinkWrite = false
									if sinkToSealOnComplete != null then {
										try sinkToSealOnComplete.seal() catch {
											case NonFatal(e) =>
												streamProtocolFault = ReceptionFailure(context.myAddress, context.oPeerAddress, context.channelId, e)
										}
										sinkToSealOnComplete = null
									}
								}
							}
						}
					}

					if awaitingSinkWrite then {
						0
					} else if streamFinished || streamProtocolFault != null then {
						1
					} else {
						// Current buffer drained and no sink write is pending: Read next chunk into streamTransferBuffer via readChannel so any bytes staged in streamTransferLeftover are consumed before reading from the socket.
						streamTransferBuffer.clear()
						awaitingChannelRead = true
						readChannel(streamTransferBuffer, timeout, timeUnit, streamTransferBuffer, streamReader)
						0
					}
				}

				/** Drives [[stepStreamIngestion]] iteratively using [[streamTrampoline]] to serialize synchronous, inline-callback, and multi-threaded asynchronous sink/channel completions in $O(1)$ stack depth and guarantee single execution of [[finishTrailingStream]]. */
				def scheduleStreamIngestion(): Unit = {
					if streamTrampoline.getAndIncrement() == 0 then {
						var terminalAction = 0
						var terminalReadError: Throwable | Null = null
						while {
							terminalAction = stepStreamIngestion()
							if terminalAction != 0 then {
								terminalReadError = channelReadFailed
								channelReadFailed = null
								streamTrampoline.set(0)
								false
							} else {
								streamTrampoline.decrementAndGet() > 0
							}
						} do ()
						(terminalAction: @scala.annotation.switch) match {
							case 1 =>
								finishTrailingStream(currentActiveSource)
							case 2 =>
								// Socket teardown upon peer disconnection: Suppress secondary close exceptions to ensure staging sink cleanup and ChannelClosedByPeer delivery execute reliably
								try channel.close() catch { case NonFatal(_) => () }
								cleanupStagingSinks()
								onComplete(ChannelClosedByPeer(currentChunkBytesRemaining), attachment)
							case 3 =>
								// Socket teardown upon read failure: Suppress secondary close exceptions to ensure staging sink cleanup and ReceptionFailure delivery execute reliably
								try channel.close() catch { case NonFatal(_) => () }
								cleanupStagingSinks()
								onComplete(ReceptionFailure(context.myAddress, context.oPeerAddress, context.channelId, terminalReadError.nn), attachment)
							case _ => ()
						}
					}
				}

				scheduleStreamIngestion()
			}

			/** Handles completion of a socket channel read into `writeEndBuffer`, parsing frame headers and package sentinels.
			 * @param bytesReceived The number of bytes read from the socket channel, or -1 on peer disconnect.
			 * @param writeEndBuffer The circular storage buffer receiving incoming bytes. */
			override def completed(bytesReceived: Integer, writeEndBuffer: ByteBuffer): Unit = {
				scribe.trace(s"Reception progress at `${context.myAddress}` from `${context.showPeerAddress}`: bytesReceived=$bytesReceived, remainingContentBytesUntilNextFrameHeader=$remainingContentBytesUntilNextFrameHeader, writeEndBuffer=${show.fillingByteBuffer(writeEndBuffer)}")
				if bytesReceived == -1 then {
					packageContentBytesCommitted = 0
					onComplete(ChannelClosedByPeer(remainingContentBytesUntilNextFrameHeader), attachment)
				} else {
					val posAfterLastReceivedByte = writeEndBuffer.position

					var nextFrameHeaderPos = remainingContentBytesUntilNextFrameHeader
					var currentBufferContentBytes = 0
					var sentinelFound = SentinelFound.NONE
					var lastDecodedContentLength = 0L
					// Find, within the write-end buffer, either, the package's sentinel, a split frame-header, or the position of the first frame-header (of the package) whose first byte was not already received and written to the write-end buffer.
					try {
						while nextFrameHeaderPos < posAfterLastReceivedByte && sentinelFound == SentinelFound.NONE do {
							frameHeaderFetcher.attachTo(writeEndBuffer, nextFrameHeaderPos, posAfterLastReceivedByte)
							val nextFrameContentLength = VLQ.tryToDecodeUnsignedInt(frameHeaderFetcher)
							lastDecodedContentLength = nextFrameContentLength
							if nextFrameContentLength == 0L then sentinelFound = SentinelFound.UNTAINTED
							else if nextFrameContentLength == -1L then sentinelFound = SentinelFound.SPLIT
							else if nextFrameContentLength == Transmitter.CORRUPTED_PACKAGE_SENTINEL then sentinelFound = SentinelFound.TAINTED
							else if nextFrameContentLength == Transmitter.STREAM_FOLLOWS_SENTINEL then sentinelFound = SentinelFound.STREAM_FOLLOWS
							else if nextFrameContentLength > totalBufferCapacity then sentinelFound = SentinelFound.TAINTED
							else {
								val frameContentLength = nextFrameContentLength.toInt
								currentBufferContentBytes += frameContentLength
								nextFrameHeaderPos += frameContentLength + frameHeaderFetcher.numberOfBytesRead
							}
						}
					} catch {
						case NonFatal(e) =>
							packageContentBytesCommitted = 0
							// Best-effort channel close upon framing scan failure: Suppress secondary close exceptions so ReceptionFailure is always delivered.
							try channel.close() catch { case NonFatal(_) => () }
							onComplete(ReceptionFailure(context.myAddress, context.oPeerAddress, context.channelId, e), attachment)
							return
					}

					// Invariants here: `sentinelFound != None || nextFrameHeaderPos >= posAfterLastReceivedByte`

					// If a whole package was received followed by an untainted sentinel (0x00):
					// - If no out-of-line byte sources were queued, deliver the deserialized message and exit the reception cycle.
					// - If out-of-line byte sources were expected but not signaled via STREAM_FOLLOWS, treat as a protocol framing mismatch, discard all staging sinks, and deliver DeserializerAndFrameMismatch.
					if sentinelFound eq SentinelFound.UNTAINTED then {
						writeEndBuffer.flip()
						val totalPackageContentBytes = packageContentBytesCommitted + currentBufferContentBytes
						packageContentBytesCommitted = 0
						val outcome = deserializePackage(writeEndBuffer, nextFrameHeaderPos, totalPackageContentBytes)
						prepareCircularStorageForNextReception(writeEndBuffer, nextFrameHeaderPos, posAfterLastReceivedByte)
						if writeEndBuffer.position == 0 then circularStorage.pruneTo(minBuffersOnIdle)
						if !pendingByteSinks.isEmpty then {
							while !pendingByteSinks.isEmpty do {
								val sink = pendingByteSinks.poll()
								// Rollback cleanup: Suppress secondary discard errors to guarantee all pending staging sinks are unlinked from disk
								try sink.discard() catch { case NonFatal(_) => () }
							}
							onComplete(DeserializerAndFrameMismatch(new LengthMismatchException()), attachment)
						} else {
							onComplete(outcome, attachment)
						}
					}
					// If a whole package was received (written to the buffers) that is followed by a trailing stream sentinel, deserialize the control package and begin streaming payload chunks.
					else if sentinelFound eq SentinelFound.STREAM_FOLLOWS then {
						writeEndBuffer.flip()
						val totalPackageContentBytes = packageContentBytesCommitted + currentBufferContentBytes
						packageContentBytesCommitted = 0
						val sentinelPos = nextFrameHeaderPos
						val sentinelLen = frameHeaderFetcher.numberOfBytesRead
						val trailingStreamStartPos = sentinelPos + sentinelLen
						val outcome = deserializePackage(writeEndBuffer, sentinelPos, totalPackageContentBytes)
						writeEndBuffer.position(trailingStreamStartPos)
						writeEndBuffer.limit(posAfterLastReceivedByte)
						receiveTrailingStream(outcome, writeEndBuffer)
					}
					// If a whole package was received (written to the buffers) that is followed by a tainted sentinel, discard the package and start receiving the next.
					else if sentinelFound eq SentinelFound.TAINTED then {
						packageContentBytesCommitted = 0
						while circularStorage.advanceReadEnd() do ()
						val sentinelLen = if lastDecodedContentLength == Transmitter.CORRUPTED_PACKAGE_SENTINEL then Transmitter.CORRUPTED_PACKAGE_SENTINEL_ENCODED.length else frameHeaderFetcher.numberOfBytesRead
						writeEndBuffer.position(math.min(nextFrameHeaderPos + sentinelLen, posAfterLastReceivedByte))
						writeEndBuffer.limit(posAfterLastReceivedByte)
						writeEndBuffer.compact()
						writeEndBuffer.limit(buffersInitialLimit)
						if writeEndBuffer.position == 0 then circularStorage.pruneTo(minBuffersOnIdle)
						scribe.warn(s"Reception progress at `${context.myAddress}` from `${context.showPeerAddress}`: A corrupted package was skipped")
						receiveWithAttachment[M, A](msgVersion, attachment, timeout, timeUnit)(onComplete)
					}
					// Invariants here: `sentinelFound == SPLIT || nextFrameHeaderPos >= posAfterLastReceivedByte`

					// if the next frame header was partially received, receive more bytes into the current write-end buffer. 	
					else if sentinelFound eq SentinelFound.SPLIT then {
						val necessarySpaceToStoreTheMissingPart = FRAME_HEADER_MAX_SIZE - frameHeaderFetcher.numberOfBytesRead
						// If the write-end buffer has not enough remaining space to store the missing part of the header, increase its limit using the reserved capacity. There is no risk of including part of the following frame header because, for a header to be split, it has to be longer than one byte, and therefore the frame size is greater than the reserved space.
						if writeEndBuffer.remaining() < necessarySpaceToStoreTheMissingPart then {
							assert(nextFrameHeaderPos <= buffersInitialLimit)
							writeEndBuffer.limit(nextFrameHeaderPos + FRAME_HEADER_MAX_SIZE)
						}
						readChannel(writeEndBuffer, timeout, timeUnit, writeEndBuffer, thisHandler)
					}
					// Invariants here: `nextFrameHeaderPos >= posAfterLastReceivedByte`

					// If the current write-end buffer has available capacity and its reserved space was not used, continue writing received data into the current write-end buffer.
					else if writeEndBuffer.hasRemaining && writeEndBuffer.limit == buffersInitialLimit then {
						readChannel(writeEndBuffer, timeout, timeUnit, writeEndBuffer, thisHandler)
					}
					// Else, advance the write-end to the next buffer.
					else {
						writeEndBuffer.flip()
						remainingContentBytesUntilNextFrameHeader = nextFrameHeaderPos - writeEndBuffer.limit
						packageContentBytesCommitted += currentBufferContentBytes
						circularStorage.advanceWriteEnd().fold {
							packageContentBytesCommitted = 0
							onComplete(ReceptionFailure(context.myAddress, context.oPeerAddress, context.channelId, new BuffersCountExceededException(s"Message reception exceeded maximum buffer capacity ($maxBuffers buffers of $buffersCapacity bytes)")), attachment)
						} { nextWriteBuffer =>
							nextWriteBuffer.clear()
							nextWriteBuffer.limit(buffersInitialLimit)
							readChannel(nextWriteBuffer, timeout, timeUnit, nextWriteBuffer, thisHandler)
						}
					}
				}
			}

			/** Delivers a [[Receiver.ReceptionFailure]] to the completion callback when a socket channel read operation fails.
			 * @param exc The network I/O exception that caused the read to fail.
			 * @param writeEndBuffer The buffer that was targeted by the failed read operation. */
			override def failed(exc: Throwable, writeEndBuffer: ByteBuffer): Unit = {
				packageContentBytesCommitted = 0
				onComplete(ReceptionFailure(context.myAddress, context.oPeerAddress, context.channelId, exc), attachment)
			}

		}

		val writeEndBuffer = circularStorage.writeEnd
		if writeEndBuffer.position > 0 then {
			// Case 2 continuation: Unconsumed pipelined bytes were shifted to the start of writeEndBuffer via compact().
			// Immediately trigger the framing state machine with 0 new bytes without issuing an unnecessary socket read.
			handler.completed(0, writeEndBuffer)
		} else {
			writeEndBuffer.limit(buffersInitialLimit)
			// Case 1 continuation or clean idle state: Delegate to `readChannel`, which transparently siphons from `streamTransferLeftover` if staged bytes exist, or issues a physical asynchronous socket read if circular storage is empty.
			readChannel(writeEndBuffer, timeout, timeUnit, writeEndBuffer, handler)
		}
	}

	/** Receives a message of type `M` from the channel without an attachment, invoking the completion callback upon completion.
	 * @param msgVersion The protocol version expected for the received message.
	 * @param timeout The maximum time to wait for the first chunk of bytes and between chunks of bytes.
	 * @param timeUnit The unit of time for the `timeout` parameter.
	 * @param onComplete A callback invoked with either the deserialized message of type `M` or a [[Receiver.Fault]].
	 * @param deserializer An implicit [[cluster.serialization.Deserializer]] used to deserialize incoming bytes into `M`.
	 * @tparam M The expected message type.
	 * @throws java.nio.channels.ReadPendingException if a read operation is already in flight on this channel.
	 * @throws java.nio.channels.ShutdownChannelGroupException if the underlying channel group has shut down. */
	inline def receive[M](msgVersion: ProtocolVersion, timeout: Long, timeUnit: TimeUnit)(inline onComplete: M | Fault => Unit)(using deserializer: Deserializer[M]): Unit = {
		receiveWithAttachment[M, Null](msgVersion, null, timeout, timeUnit) { (outcome, dummy) => onComplete(outcome) }
	}

	/** Receives a message of type `M` lazily, deferring socket read initiation until explicitly triggered.
	 * @param msgVersion The protocol version expected for the received message.
	 * @param timeout The maximum time to wait for the first chunk of bytes and between chunks of bytes.
	 * @param timeUnit The unit of time for the `timeout` parameter.
	 * @param deserializer An implicit [[cluster.serialization.Deserializer]] used to deserialize incoming bytes into `M`.
	 * @tparam M The expected message type.
	 * @return A [[Receiver.Lazy]] instance whose [[Receiver.Lazy.trigger]] method initiates reception. */
	def receivesLazily[M <: Matchable](msgVersion: ProtocolVersion, timeout: Long, timeUnit: TimeUnit)(using deserializer: Deserializer[M]): Lazy[M] = new Lazy[M] {
		/** Initiates reception of the message and registers the completion callback.
		 * @param onComplete A callback invoked with either the deserialized message or a [[Receiver.Fault]]. */
		override def trigger(onComplete: M | Fault => Unit): Unit = {
			receiveWithAttachment[M, Unit](msgVersion, (), timeout, timeUnit) { (outcome, dummy) => onComplete(outcome) }
		}
	}

	/** Receives a message of type `M` eagerly, returning a [[scala.concurrent.Future]] completed when the message arrives.
	 * @param msgVersion The protocol version expected for the received message.
	 * @param timeout The maximum time to wait for the first chunk of bytes and between chunks of bytes.
	 * @param timeUnit The unit of time for the `timeout` parameter.
	 * @param deserializer An implicit [[cluster.serialization.Deserializer]] used to deserialize incoming bytes into `M`.
	 * @tparam M The expected message type.
	 * @return A [[scala.concurrent.Future]] yielding either the deserialized message `M` or a [[Receiver.Fault]]. */
	def receiveFuture[M](msgVersion: ProtocolVersion, timeout: Long, timeUnit: TimeUnit)(using deserializer: Deserializer[M]): Future[M | Fault] = {
		val promise = Promise[M | Fault]()
		receiveWithAttachment[M, Unit](msgVersion, (), timeout, timeUnit) { (outcome, dummy) => promise.success(outcome) }
		promise.future
	}
}
