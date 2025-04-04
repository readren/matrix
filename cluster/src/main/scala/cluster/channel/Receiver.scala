package readren.matrix
package cluster
package channel

import cluster.channel.Receiver.*
import cluster.misc.{DualEndedCircularStorage, VLQ}
import cluster.service.ProtocolVersion

import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousSocketChannel, CompletionHandler}
import java.util.concurrent.TimeUnit
import scala.concurrent.{Future, Promise}

object Receiver {
	private inline val FRAME_HEADER_MAX_SIZE = VLQ.INT_MAX_LENGTH

	type Lazy[A] = misc.Lazy[A, Fault]

	sealed trait Fault

	case class ChannelClosedByPeer(missingBytesAccordingToLastFrame: Int) extends Fault

	case class DeserializerAndFrameMismatch(origin: LengthMismatchException) extends Fault
	
	case class TheDeserializerHasNotConsumedTheWholePackage(remainingBytes: Int, deserializerResult: Any) extends Fault

	case class DeserializationProblem(problem: Throwable) extends Fault

	case class ReceptionFailure(cause: Throwable) extends Fault

	/** The [[Deserializer]] expected more bytes than the contained in the received package (a sequence of frames finalized with an empty frame). */
	class LengthMismatchException extends RuntimeException("Unexpected end of package")

	/** The received bytes are less than the package size. A package is a sequence of frames finalized with an empty one. */
	class UnexpectedBufferEnd extends RuntimeException("The buffer was consumed before the package end.")

	private enum SentinelFound {
		case NONE, UNTAINTED, TAINTED, SPLIT
	}
}

/** A capability for receiving messages over an [[AsynchronousSocketChannel]].
 *
 * Note: The underlying [[AsynchronousSocketChannel]] is not thread-safe and supports only one reception at a time. Concurrent attempts to receive messages will result in undefined behavior.
 *
 * @param channel the channel that connects the peer with us.
 * @param buffersCapacity the capacity of each buffer of the [[DualEndedCircularStorage]]. */
class Receiver(channel: AsynchronousSocketChannel, buffersCapacity: Int = 8192, maxBytesToCompact: Int = 256) {
	private val buffersInitialLimit = buffersCapacity - FRAME_HEADER_MAX_SIZE

	assert(buffersCapacity >= FRAME_HEADER_MAX_SIZE + Deserializer.CONSECUTIVE_CONTENT_BYTES_REQUIRED_BY_MOST_DEMANDING_OPERATION)
	private val circularStorage: DualEndedCircularStorage[ByteBuffer] = new DualEndedCircularStorage[ByteBuffer](() => ByteBuffer.allocateDirect(buffersCapacity))
	private val continuousBuffer: ByteBuffer = ByteBuffer.allocate(Deserializer.CONSECUTIVE_CONTENT_BYTES_REQUIRED_BY_MOST_DEMANDING_OPERATION)

	/** A [[VLQ.ByteReader]] that reads the bytes from the [[ByteBuffer]] it is attached to.
	 * Design note: Defining this object here is not neat but it prevents the creation of a [[VLQ.ByteReader]] instance for each frame received from the channel. */
	private object frameHeaderReader extends VLQ.ByteReader {
		private var bufferPositionedAtFrameHeader: ByteBuffer | Null = null

		/** Attaches this reader to the provided buffer.
		 *  - Buffer must already be positioned at the header start.
		 *  - Actual mutation (consumption) of the buffer will happen when the `readByte()` method is called by the VLQ decoder method..
		 * @param bufferPositionedAtFrameHeader a [[ByteBuffer]] that contains and points to the frame header to be read.
		 */
		def attachTo(bufferPositionedAtFrameHeader: ByteBuffer): Unit = {
			this.bufferPositionedAtFrameHeader = bufferPositionedAtFrameHeader
		}

		override def readByte(): Byte =
			bufferPositionedAtFrameHeader.get()
	}

	/** A [[VLQ.ByteReader]] that reads the bytes from an internal array whose content is copied from a [[ByteBuffer]] without mutating it.
	 * Design note: Defining this object here is not neat but it prevents the creation of a [[VLQ.ByteReader]] instance for each frame received from the channel. */
	private object frameHeaderFetcher extends VLQ.BoundedByteReader {
		private var backingBuffer: ByteBuffer | Null = null
		private var startingReadPosition: Int = 0
		private var readPosition: Int = 0
		private var limit: Int = 0

		inline def numberOfBytesRead: Int = readPosition - startingReadPosition

		/** Attaches this reader to the provided [[ByteBuffer]] at the specified position with the specified limit.
		 * The buffer is not mutated by this method nor the [[readByte]]
		 * @param bufferWithHeader a [[ByteBuffer]] that contains the frame header to be read.
		 * @param headerPosition the absolute position of the frame header within the provided buffer. */
		def attachTo(bufferWithHeader: ByteBuffer, headerPosition: Int, limit: Int): Unit = {
			backingBuffer = bufferWithHeader
			startingReadPosition = headerPosition
			readPosition = headerPosition
			this.limit = limit
		}

		override def hasMoreBytes: Boolean = readPosition < limit

		override def readByte(): Byte = {
			val byte = backingBuffer.get(readPosition)
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
	 * @param onComplete A callback that is invoked with either the deserialized message of type `M` or a `Fault` object, along with the provided `attachment`.
	 * @param deserializer An implicit `Deserializer[M]` used to deserialize the incoming bytes into a message of type `M`.
	 */
	def receiveWithAttachment[M, A](msgVersion: ProtocolVersion, attachment: A, timeout: Long, timeUnit: TimeUnit)(onComplete: (M | Fault, A) => Unit)(using deserializer: Deserializer[M]): Unit = {
		continuousBuffer.position(continuousBuffer.limit)

		/** Implementation note: To optimize memory usage, this object can be moved outside the [[receiveWithAttachment]] method. This avoids repeated memory allocations but introduces mutable fields, which may require careful handling to ensure thread safety and correctness. */
		object handler extends Deserializer.Reader, CompletionHandler[Integer, ByteBuffer] { thisHandler =>

			var remainingContentBytesUntilNextFrameHeader = 0
			var readEndBuffer: ByteBuffer = circularStorage.readEnd
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
				if circularStorage.advanceReadEnd() then {
					nextFrameHeaderPosRelativeToReadEndBufferBase -= readEndBuffer.limit
					readEndBuffer = circularStorage.readEnd
				}
				else throw new UnexpectedBufferEnd()
			}

			/** @throws LengthMismatchException if the [[Deserializer]] tries to read more bytes than the contained in the package. A package is a sequence of frames finalized with an empty frame.
			 * @throws UnexpectedBufferEnd if all the received bytes were consumed and the package was not fully read. */
			private def skipFrameHeader(): Unit = {
				if readEndBuffer.remaining == 0 then advanceReadEnd()

				if readEndBuffer.position == nextFrameHeaderPosRelativeToReadEndBufferBase then {
					assert(readEndBuffer.remaining >= FRAME_HEADER_MAX_SIZE) // this is ensured by the `completed` method by extending the buffer limit (using the reserved capacity).
					frameHeaderReader.attachTo(readEndBuffer)
					val currentFrameContentLength = VLQ.decodeUnsignedInt(frameHeaderReader)
					if currentFrameContentLength == 0 then throw new LengthMismatchException()
					nextFrameHeaderPosRelativeToReadEndBufferBase = readEndBuffer.position() + currentFrameContentLength
					if readEndBuffer.remaining == 0 then advanceReadEnd()
				}
			}

			/** @param maxBytesToConsume the number of bytes that will be consumed from the returned [[ByteBuffer]]. This value can be greater than number of bytes that will be read, but for efficiency it is preferably it be exactly the same.
			 *  @throws LengthMismatchException if the [[Deserializer]] tries to read more bytes than the contained in the package. A package is a sequence of frames finalized with an empty frame.
			 * @throws UnexpectedBufferEnd if all the received bytes were consumed and the package was not fully read. */
			override def getContentBytes(maxBytesToConsume: Int): ByteBuffer = {
				val continuousBufferRemaining = continuousBuffer.remaining
				if continuousBufferRemaining == 0 && readEndBuffer.position + maxBytesToConsume < nextFrameHeaderPosRelativeToReadEndBufferBase && maxBytesToConsume <= readEndBuffer.remaining then readEndBuffer
				else {
					if continuousBufferRemaining > 0 then continuousBuffer.compact()
					else continuousBuffer.clear()
					var count = maxBytesToConsume - continuousBufferRemaining
					while count > 0 do {
						skipFrameHeader()
						continuousBuffer.put(readEndBuffer.get)
						count -= 1
					}
					continuousBuffer.flip()
				}
			}

			inline private def deserializePackage(writeEndBuffer: ByteBuffer, sentinelPos: Int): M | Fault = {
				try {
					skipFrameHeader() // this line is not necessary but its presence avoids the use of the continuous buffer when the deserializer reads the first byte.
					val message = deserializer.deserialize(thisHandler)
					// if the deserializer consumed all the bytes in the package, return a successful outcome.
					if (readEndBuffer eq writeEndBuffer) && readEndBuffer.position() == sentinelPos then message
					// else return a faulty outcome
					else {
						var notConsumedBytesAccumulator = 0
						while circularStorage.readEnd ne writeEndBuffer do {
							notConsumedBytesAccumulator += circularStorage.readEnd.remaining()
							circularStorage.advanceReadEnd()
						}
						notConsumedBytesAccumulator += sentinelPos - writeEndBuffer.position()
						TheDeserializerHasNotConsumedTheWholePackage(notConsumedBytesAccumulator, message)
					}
				} catch {
					case lme: LengthMismatchException => DeserializerAndFrameMismatch(lme)
					case scala.util.control.NonFatal(e) => DeserializationProblem(e)
				}
			}

			/** Prepare the `circularStorage` for the next call to the `receiveWithAttachment` method. */
			inline private def prepareStorageForNextCall(writeEndBuffer: ByteBuffer, sentinelPos: Int, posOfFirstRemainingByteOfTheWriteEndBuffer: Int): Unit = {
				val nextPackageHeaderPos = sentinelPos + 1
				if posOfFirstRemainingByteOfTheWriteEndBuffer > nextPackageHeaderPos + maxBytesToCompact then {
					// consume the sentinel
					readEndBuffer.get().ensuring(_ == 0)
					// use the next buffer to store subsequently received bytes (the next time the `receiveWithAttachment` method is called)
					circularStorage.advanceWriteEnd()
				} else {
					// move the bytes corresponding to the next package to the beginning of the write-end buffer and reuse it for subsequently received bytes (the next time the `receive` method is called)
					writeEndBuffer.position(nextPackageHeaderPos)
					writeEndBuffer.limit(posOfFirstRemainingByteOfTheWriteEndBuffer)
					writeEndBuffer.compact()
					writeEndBuffer.limit(buffersInitialLimit)
				}
			}


			override def completed(bytesReceived: Integer, writeEndBuffer: ByteBuffer): Unit = {
				if bytesReceived == -1 then onComplete(ChannelClosedByPeer(remainingContentBytesUntilNextFrameHeader), attachment)
				else {
					val posAfterLastReceivedByte = writeEndBuffer.position

					// Find, within the write-end buffer, the package's sentinel or the position of the first frame (of the package) whose header was not already received and written to the write-end buffer.
					var nextFrameHeaderPos = remainingContentBytesUntilNextFrameHeader
					var sentinelFound = SentinelFound.NONE
					while nextFrameHeaderPos < posAfterLastReceivedByte && sentinelFound == SentinelFound.NONE do {
						frameHeaderFetcher.attachTo(writeEndBuffer, nextFrameHeaderPos, posAfterLastReceivedByte)
						val nextFrameContentLength = VLQ.tryToDecodeUnsignedInt(frameHeaderFetcher)
						if nextFrameContentLength == 0L then sentinelFound = SentinelFound.UNTAINTED
						else if nextFrameContentLength == -1L then sentinelFound = SentinelFound.SPLIT
						else if nextFrameContentLength == (Transmitter.CORRUPTED_PACKAGE_SENTINEL & 0xffff_ffffL) then sentinelFound = SentinelFound.TAINTED
						else nextFrameHeaderPos += nextFrameContentLength.toInt + frameHeaderFetcher.numberOfBytesRead
					}
					// Invariants here: `sentinelFound != None || nextFrameHeaderPos >= posAfterLastReceivedByte`

					// If a whole package was received (written to the buffers) and is not marked as corrupt, deserialize its content, compact the write-end buffer, call the onComplete call-back with the result, and exit the reception cycle.
					if sentinelFound == SentinelFound.UNTAINTED then {
						writeEndBuffer.flip()
						val outcome = deserializePackage(writeEndBuffer, nextFrameHeaderPos)
						prepareStorageForNextCall(writeEndBuffer, nextFrameHeaderPos, posAfterLastReceivedByte)
						onComplete(outcome, attachment)
					}
					// If a whole package was received (written to the buffers) and is marked as corrupt, discard it and start receiving the next package.
					else if sentinelFound == SentinelFound.TAINTED then {
						while circularStorage.advanceReadEnd() do ()
						writeEndBuffer.position(nextFrameHeaderPos + Transmitter.CORRUPTED_PACKAGE_SENTINEL_ENCODED.length)
						writeEndBuffer.limit(posAfterLastReceivedByte)
						writeEndBuffer.compact()
						writeEndBuffer.limit(buffersInitialLimit)
						scribe.warn("A corrupted package was skipped")
						receiveWithAttachment[M, A](msgVersion, attachment, timeout, timeUnit)(onComplete)
					}
					// Invariants here: `sentinelFound == SPLIT || nextFrameHeaderPos >= posAfterLastReceivedByte`

					// If the current write-end buffer has available capacity, continue writing received data into the current write-end buffer.
					else if writeEndBuffer.hasRemaining then {
						// If the frame header will be split, avoid the splitting in advance by increasing the limit of the current write-end buffer (using the reserved capacity).
						if buffersInitialLimit > nextFrameHeaderPos && nextFrameHeaderPos > buffersInitialLimit - FRAME_HEADER_MAX_SIZE then {
							writeEndBuffer.limit(nextFrameHeaderPos + FRAME_HEADER_MAX_SIZE)
						}
						channel.read(writeEndBuffer, timeout, timeUnit, writeEndBuffer, thisHandler)
					}
					// Invariants here: `sentinelFound == SPLIT || nextFrameHeaderPos >= buffersInitialLimit`

					// Else, if a fragment of the frame header is written at the end of the current write-end buffer, increase the limit (using the reserved capacity) and write the missing part in the same buffer.
					else if nextFrameHeaderPos < buffersInitialLimit then {
						assert(sentinelFound == SentinelFound.SPLIT)
						writeEndBuffer.limit(nextFrameHeaderPos + FRAME_HEADER_MAX_SIZE)
						channel.read(writeEndBuffer, timeout, timeUnit, writeEndBuffer, thisHandler)
					}
					// Else, advance the write-end to the next buffer.
					else {
						writeEndBuffer.flip()
						remainingContentBytesUntilNextFrameHeader = nextFrameHeaderPos - writeEndBuffer.limit
						val nextWriteBuffer = circularStorage.advanceWriteEnd()
						nextWriteBuffer.clear()
						nextWriteBuffer.limit(buffersInitialLimit)
						channel.read(nextWriteBuffer, timeout, timeUnit, nextWriteBuffer, thisHandler)
					}
				}
			}

			override def failed(exc: Throwable, writeEndBuffer: ByteBuffer): Unit = {
				onComplete(ReceptionFailure(exc), attachment)
			}

		}

		val writeEndBuffer = circularStorage.writeEnd
		channel.read(writeEndBuffer, timeout, timeUnit, writeEndBuffer, handler)
	}

	/** Same as [[receiveWithAttachment]] but without attachment. */
	inline def receive[M](msgVersion: ProtocolVersion, timeout: Long, timeUnit: TimeUnit)(inline onComplete: M | Fault => Unit)(using deserializer: Deserializer[M]): Unit = {
		receiveWithAttachment[M, Null](msgVersion, null, timeout, timeUnit) { (outcome, dummy) => onComplete(outcome) }
	}

	/** Like [[receive]] but changing the continuation style from passing to returned.
	 * Also, the computation is deferred until explicitly triggered (with [[util.Lazy.trigger]]. */
	def receivesLazily[M](msgVersion: ProtocolVersion, timeout: Long, timeUnit: TimeUnit)(using deserializer: Deserializer[M]): Lazy[M] = new Lazy[M] {
		override def trigger(onComplete: M | Fault => Unit): Unit = {
			receiveWithAttachment[M, Unit](msgVersion, (), timeout, timeUnit) { (outcome, dummy) => onComplete(outcome) }
		}
	}

	/** Like [[receive]] but changing the continuation style from passing to returned.
	 * Differs from [[receivesLazily]] in that the computation is eagerly started.
	 */
	def receiveFuture[M](msgVersion: ProtocolVersion, timeout: Long, timeUnit: TimeUnit)(using deserializer: Deserializer[M]): Future[M | Fault] = {
		val promise = Promise[M | Fault]()
		receiveWithAttachment[M, Unit](msgVersion, (), timeout, timeUnit) { (outcome, dummy) => promise.success(outcome) }
		promise.future
	}
}
