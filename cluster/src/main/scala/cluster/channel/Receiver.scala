package readren.matrix
package cluster
package channel

import cluster.channel.Receiver.*
import cluster.misc.{DualEndedCircularBuffer, VLQ}
import cluster.service.ProtocolVersion

import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousSocketChannel, CompletionHandler}
import java.util.concurrent.TimeUnit
import java.util.function.{BiConsumer, Consumer}
import scala.concurrent.{Future, Promise}

object Receiver {
	private inline val FRAME_HEADER_MAX_SIZE = VLQ.INT_MAX_LENGTH

	type Lazy[A] = misc.Lazy[A, Fault]

	sealed trait Fault

	case class FrameAndEndOfStreamMismatch(missingBytesAccordingToLastFrame: Int) extends Fault

	case class DeserializerAndFrameMismatch(origin: LengthMismatchException) extends Fault

	case class DeserializationProblem(problem: Throwable) extends Fault

	case class ReceptionFailure(cause: Throwable) extends Fault

	/** The [[Deserializer]] expected more bytes than the contained in the received package (a sequence of frames finalized with an empty frame). */
	class LengthMismatchException extends RuntimeException("Unexpected end of package")
	/** The received bytes are less than the package size. A package is a sequence of frames finalized with an empty one. */
	class UnexpectedBufferEnd extends RuntimeException("The buffer was consumed before the package end.")
}

/** A capability for receiving messages over an [[AsynchronousSocketChannel]].
 *
 * Note: The underlying [[AsynchronousSocketChannel]] is not thread-safe and supports only one reception at a time. Concurrent attempts to receive messages will result in undefined behavior.
 *
 * @param channel the channel that connects the peer with us.
 * @param buffersCapacity the capacity of each storage unit of the [[DualEndedCircularBuffer]]. */
class Receiver(channel: AsynchronousSocketChannel, buffersCapacity: Int = 8192) {
	private val buffersInitialLimit = buffersCapacity - FRAME_HEADER_MAX_SIZE

	assert(buffersCapacity >= FRAME_HEADER_MAX_SIZE + Deserializer.CONSECUTIVE_CONTENT_BYTES_REQUIRED_BY_MOST_DEMANDING_OPERATION)
	private val buffers: DualEndedCircularBuffer[ByteBuffer] = new DualEndedCircularBuffer[ByteBuffer](() => ByteBuffer.allocateDirect(buffersCapacity))
	private val continuousBuffer: ByteBuffer = ByteBuffer.allocate(Deserializer.CONSECUTIVE_CONTENT_BYTES_REQUIRED_BY_MOST_DEMANDING_OPERATION)

	private object frameHeaderReader extends VLQ.ByteReader {
		private var bufferPointingToFrameHeader: ByteBuffer | Null = null

		/** @param bufferPointingToFrameHeader a [[ByteBuffer]] that contains and points to the frame header to be read. */
		def reset(bufferPointingToFrameHeader: ByteBuffer): Unit = {
			this.bufferPointingToFrameHeader = bufferPointingToFrameHeader
		}

		override def readByte(): Byte =
			bufferPointingToFrameHeader.get()
	}

	private object frameHeaderFetcher extends VLQ.ByteReader {
		private val bytes: Array[Byte] = new Array(VLQ.INT_MAX_LENGTH)
		private var readPosition: Int = 0

		inline def numberOfBytesRead: Int = readPosition

		/** @param headerContainingBuffer a [[ByteBuffer]] that contains the frame header to be read.
		 * @param headerPosition the absolute position of the frame header within the provided buffer. */
		def reset(headerContainingBuffer: ByteBuffer, headerPosition: Int): Unit = {
			headerContainingBuffer.get(headerPosition, bytes)
			readPosition = 0
		}

		override def readByte(): Byte = {
			val byte = bytes(readPosition)
			readPosition += 1
			byte
		}
	}

	/**
	 * Starts consuming bytes received or being received through the [[channel]] and uses them as the source for the provided [[Deserializer]].
	 * The `onComplete` callback is invoked when the `deserializer` completes its operation. The deserializer begins processing once sufficient bytes have been received through the channel.
	 *
	 * @param msgVersion the version of the received message. This value depends on the project version of the peer.
	 * @param attachment An attachment of type `A` that is passed through to the `onComplete` callback.
	 * @param timeout The maximum time to wait for the first chunk of bytes and between chunks of bytes.
	 * @param timeUnit The unit of time for the `timeout` parameter.
	 * @param onComplete A `BiConsumer` callback that is invoked with either the deserialized message of type `M` or a `Fault` object, along with the provided `attachment`.
	 * @param deserializer An implicit `Deserializer[M]` used to deserialize the incoming bytes into a message of type `M`.
	 */
	def receiveWithAttachment[M, A](msgVersion: ProtocolVersion, attachment: A, timeout: Long, timeUnit: TimeUnit)(onComplete: BiConsumer[M | Fault, A])(using deserializer: Deserializer[M]): Unit = {
		continuousBuffer.position(continuousBuffer.limit)

		/** Implementation note: To optimize memory usage, this object can be moved outside the [[receiveWithAttachment]] method. This avoids repeated memory allocations but introduces mutable fields, which may require careful handling to ensure thread safety and correctness. */
		object handler extends Deserializer.Reader, CompletionHandler[Integer, ByteBuffer] { thisHandler =>

			@volatile var remainingContentBytesUntilNextFrameHeader = 0
			var readEndBuffer: ByteBuffer = buffers.readEnd
			var nextFrameHeaderPosRelativeToReadEndBufferBase = 0

			override def versionToDeserializeFrom: ProtocolVersion = msgVersion

			override def position: Int = readEndBuffer.position

			/** @throws LengthMismatchException if the [[Deserializer]] tries to read more bytes than the contained in the package. A package is a sequence of frames finalized with an empty frame.
			 * @throws UnexpectedBufferEnd if all the received bytes were consumed and the package was not fully read. */
			override def peekByte: Byte = {
				if continuousBuffer.hasRemaining then continuousBuffer.get(continuousBuffer.position)
				else {
					skipFrameHeader()
					readEndBuffer.get(readEndBuffer.position)
				}
			}

			/** @throws UnexpectedBufferEnd if all the received bytes were consumed and the package was not fully read. */
			private def advanceReadEnd(): Unit = {
				if buffers.advanceReadEnd() then {
					nextFrameHeaderPosRelativeToReadEndBufferBase -= readEndBuffer.limit
					readEndBuffer = buffers.readEnd
				}
				else throw new UnexpectedBufferEnd()
			}

			/** @throws LengthMismatchException if the [[Deserializer]] tries to read more bytes than the contained in the package. A package is a sequence of frames finalized with an empty frame.
			 * @throws UnexpectedBufferEnd if all the received bytes were consumed and the package was not fully read.*/
			private def skipFrameHeader(): Unit = {
				if readEndBuffer.remaining == 0 then advanceReadEnd()

				if readEndBuffer.position == nextFrameHeaderPosRelativeToReadEndBufferBase then {
					assert(readEndBuffer.remaining >= FRAME_HEADER_MAX_SIZE) // this is ensured by the `completed` method by extending the buffer limit (using the reserved capacity).
					frameHeaderReader.reset(readEndBuffer)
					val currentFrameContentLength = VLQ.decodeUnsignedInt(frameHeaderReader)
					if currentFrameContentLength == 0 then throw new LengthMismatchException()
					nextFrameHeaderPosRelativeToReadEndBufferBase = readEndBuffer.position() + currentFrameContentLength
					if readEndBuffer.remaining == 0 then advanceReadEnd()
				}
			}

			/** @throws LengthMismatchException if the [[Deserializer]] tries to read more bytes than the contained in the package. A package is a sequence of frames finalized with an empty frame.
			 * @throws UnexpectedBufferEnd if all the received bytes were consumed and the package was not fully read. */
			override def getContentBytes(maxBytesToConsume: Int): ByteBuffer = {
				val continuousBufferRemaining = continuousBuffer.remaining
				if continuousBufferRemaining == 0 && readEndBuffer.position + maxBytesToConsume < nextFrameHeaderPosRelativeToReadEndBufferBase && maxBytesToConsume <= readEndBuffer.remaining then readEndBuffer
				else {
					if continuousBufferRemaining > 0 then continuousBuffer.compact()
					else continuousBuffer.clear()
					var count, updatedMaxBytesToConsume = maxBytesToConsume - continuousBufferRemaining
					while count > 0 do {
						skipFrameHeader()
						continuousBuffer.put(readEndBuffer.get)
						count -= 1
					}
					continuousBuffer.flip()
				}
			}

			override def completed(bytesReceived: Integer, writeEndBuffer: ByteBuffer): Unit = {
				if bytesReceived == -1 then onComplete.accept(FrameAndEndOfStreamMismatch(remainingContentBytesUntilNextFrameHeader), attachment)
				else {
					val writeEndPosition = writeEndBuffer.position

					var nextFrameHeaderPos = remainingContentBytesUntilNextFrameHeader
					while nextFrameHeaderPos <= writeEndPosition - FRAME_HEADER_MAX_SIZE do {
						frameHeaderFetcher.reset(writeEndBuffer, nextFrameHeaderPos)
						val nextFrameContentLength = VLQ.decodeUnsignedInt(frameHeaderFetcher)
						if nextFrameContentLength == 0 then {
							writeEndBuffer.flip()
							val outcome: M | Fault =
								try {
									skipFrameHeader() // this line is not necessary but its presence avoids the use of the continuous buffer when the deserializer reads the first byte.
									deserializer.deserialize(thisHandler)
								} catch {
									case lme: LengthMismatchException => DeserializerAndFrameMismatch(lme)
									case scala.util.control.NonFatal(e) => DeserializationProblem(e)
								}
							writeEndBuffer.compact()
							onComplete.accept(outcome, attachment)
							return
						}
						nextFrameHeaderPos += nextFrameContentLength + frameHeaderFetcher.numberOfBytesRead
					}
					// Invariants here: `nextFrameHeaderPos > writeEndPosition - FRAME_HEADER_MAX_SIZE`

					// If the current write-end buffer has available capacity, continue writing received data into the current write-end buffer.
					if writeEndBuffer.hasRemaining then {
						// If the frame header will be split, avoid the splitting in advance by increasing the limit of the current write-end buffer (using the reserved capacity).
						if buffersInitialLimit > nextFrameHeaderPos && nextFrameHeaderPos > buffersInitialLimit - FRAME_HEADER_MAX_SIZE then {
							writeEndBuffer.limit(nextFrameHeaderPos + FRAME_HEADER_MAX_SIZE)
						}
						channel.read(writeEndBuffer, timeout, timeUnit, writeEndBuffer, thisHandler)
					}
					// Else, if a fragment of the frame header is written at the end of the current write-end buffer, increase the limit (using the reserved capacity) and write the missing part in the same buffer.
					else if nextFrameHeaderPos < buffersInitialLimit then {
						writeEndBuffer.limit(nextFrameHeaderPos + FRAME_HEADER_MAX_SIZE)
						channel.read(writeEndBuffer, timeout, timeUnit, writeEndBuffer, thisHandler)
					}
					// Else, advance the write-end to the next buffer.
					else {
						writeEndBuffer.flip()
						remainingContentBytesUntilNextFrameHeader = nextFrameHeaderPos - writeEndBuffer.limit
						val nextWriteBuffer = buffers.advanceWriteEnd()
						nextWriteBuffer.clear()
						nextWriteBuffer.limit(buffersInitialLimit)
						channel.read(nextWriteBuffer, timeout, timeUnit, nextWriteBuffer, thisHandler)
					}
				}
			}

			override def failed(exc: Throwable, writeEndBuffer: ByteBuffer): Unit = {
				onComplete.accept(ReceptionFailure(exc), attachment)
			}

		}

		val writeEndBuffer = buffers.writeEnd
		channel.read(writeEndBuffer, timeout, timeUnit, writeEndBuffer, handler)
	}

	/** Same as [[receiveWithAttachment]] but without attachment. */
	inline def receive[M](msgVersion: ProtocolVersion, timeout: Long, timeUnit: TimeUnit)(inline onComplete: M | Fault => Unit)(using deserializer: Deserializer[M]): Unit = {
		receiveWithAttachment[M, Null](msgVersion, null, timeout, timeUnit) { (outcome, dummy) => onComplete(outcome) }
	}

	/** Like [[receive]] but changing the continuation style from passing to returned.
	 * Also, the computation is deferred until explicitly triggered (with [[util.Lazy.trigger]]. */
	def receivesLazily[M](msgVersion: ProtocolVersion, timeout: Long, timeUnit: TimeUnit)(using deserializer: Deserializer[M]): Lazy[M] = new Lazy[M] {
		override def trigger(onComplete: Consumer[M | Fault]): Unit = {
			receiveWithAttachment[M, Unit](msgVersion, (), timeout, timeUnit) { (outcome, dummy) => onComplete.accept(outcome) }
		}
	}

	/** Like [[receive]] but changing the continuation style from passing to returned.
	 * Differs from [[receivesLazily]] in that the computation is eagerly started.
	 */
	def receive[M](msgVersion: ProtocolVersion, timeout: Long, timeUnit: TimeUnit)(using deserializer: Deserializer[M]): Future[M | Fault] = {
		val promise = Promise[M | Fault]()
		receiveWithAttachment[M, Unit](msgVersion, (), timeout, timeUnit) { (outcome, dummy) => promise.success(outcome) }
		promise.future
	}
}
