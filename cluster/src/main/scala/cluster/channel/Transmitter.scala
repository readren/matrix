package readren.matrix
package cluster.channel

import cluster.channel.Transmitter.*
import cluster.misc.LoggingTools.*
import cluster.misc.{DualEndedCircularStorage, VLQ}
import cluster.serialization.Serializer.{SerializationException, Writer}
import cluster.serialization.{ProtocolVersion, Serializer}
import cluster.service.Protocol.ContactAddress

import readren.taskflow.Maybe

import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousSocketChannel, CompletionHandler}
import java.util.concurrent.TimeUnit

object Transmitter {
	sealed trait Report

	/** Report received by the `onComplete` callback passed to the [[transmit]] method when the serialization and transmission was successful. */
	case object Delivered extends Report
	sealed trait NotDelivered extends Report
	/**
	 * A report received by the `onComplete` callback passed to the [[transmit]] method when the transmission fails due to a serialization exception.
	 *
	 * @param rootMessage The complete message that was intended for serialization and transmission.
	 * @param problem The exception thrown by the serializer.
	 * @param aFragmentWasTransmitted Indicates whether a portion of the message was transmitted before the failure occurred. 
	 *                                - `true` if the issue was detected after a fragment of the message was already transmitted.
	 *                                - `false` if no bytes were transmitted because the issue was detected before transmission began.
	 */
	case class SerializationProblem(rootMessage: Any, problem: SerializationException, aFragmentWasTransmitted: Boolean) extends NotDelivered

	case class TransmissionFailure(rootMessage: Any, cause: Throwable) extends NotDelivered

	class TransmissionInProgressException extends Exception
	
	/** Special reserved value (max unsigned int value) of a frame length that the [[Transmitter]] uses for the sentinel of a corrupted package.
	 *  - Written in place of a frame length when the previous frames are the product of an aborted serialization.  
	 *  - Acts as an invalid/poison value for downstream processing. */
	val CORRUPTED_PACKAGE_SENTINEL: Int = 0xffff_fffe // = -2 in two complement
	val CORRUPTED_PACKAGE_SENTINEL_ENCODED: Array[Byte] = {
		val array = new Array[Byte](VLQ.INT_MAX_LENGTH)
		val writer: VLQ.ByteWriter = new VLQ.ByteWriter {
			var pos = 0
			override def putByte(byte: Byte): Unit = {
				array(pos) = byte
				pos += 1
			}
		}
		VLQ.encodeUnsignedInt(CORRUPTED_PACKAGE_SENTINEL, writer)
		array
	}

	trait Context {
		def myAddress: ContactAddress
		def oPeerAddress: Maybe[ContactAddress]
		def showPeerAddress: String = oPeerAddress.fold("unknown")(_.toString)
	}
}

/**
 * A capability for transmitting messages over an [[AsynchronousSocketChannel]].
 * 
 * Note: The underlying [[AsynchronousSocketChannel]] is not thread-safe and supports only one transmission at a time. Concurrent attempts to transmit messages will result in undefined behavior
 * 
 * @param channel the channel over which the messages will be transmitted.
 * @param buffersCapacity the capacity of each buffer of the [[DualEndedCircularStorage]], which determines the frames max size (frameMaxSize == buffersCapacity)..
 */
class Transmitter(channel: AsynchronousSocketChannel, context: Context, buffersCapacity: Int = 8192) {
	assert(buffersCapacity >= Serializer.BUFFER_SIZE_REQUIRED_BY_MOST_DEMANDING_OPERATION)
	private val frameHeaderBuffer: ByteBuffer = ByteBuffer.allocate(VLQ.INT_MAX_LENGTH)
	private val frameHeaderWriter: VLQ.ByteWriter = (byte: Byte) => frameHeaderBuffer.put(byte) 
	private val circularStorage: DualEndedCircularStorage[ByteBuffer] = new DualEndedCircularStorage[ByteBuffer](() => ByteBuffer.allocateDirect(buffersCapacity))
	private val frameBuffer: Array[ByteBuffer] = new Array(2)

	/** Consist of two buffers: the first for the frame header (which tells the size of the frame content in bytes), and the second for the frame content. */
	frameBuffer(0) = frameHeaderBuffer

	/**
	 * 
	 * @param message the message to transmit.
	 * @param msgVersion the version id of messages to produce.
	 * @param timeout The maximum time to wait for the first chunk of bytes and between chunks of bytes are transmitted.
	 * @param timeUnit The unit of time for the `timeout` parameter.
	 * @param onComplete A consumer callback that is invoked when the transmission completes either successfully or not, with a [[Transmitter.Report]].
	 * @param serializer The serializer to be used for serializing the message into outgoing bytes.
	 * @tparam M The type of the `message` to be sent.
	 */
	inline def transmit[M](message: M, msgVersion: ProtocolVersion, behindTransmissionEnabled: Boolean = true, timeout: Long = 1, timeUnit: TimeUnit = TimeUnit.SECONDS)(onComplete: Report => Unit)(using serializer: Serializer[M]): Unit = {
		transmitWithAttachment[M, Null](message, msgVersion, null, behindTransmissionEnabled, timeout, timeUnit) { (report, dummy) => onComplete(report) }
	}

	// assume that the thread that calls this method is named "main".
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
		/** This variable is modified only within the thread that called this method ([[transmitWithAttachment]]). */
		var behindTransmissionStarted = false
		/** This variable is modified one time only, from false to true. */
		@volatile var behindTransmissionStopped = false
		/** This variable is modified one time only, from false to true. */
		@volatile var isSerializationCompleted = false
		/** This variable is modified one time only, from null to non-null. */
		@volatile var cancellationReason: SerializationProblem | Null = null

		/** Sends a frame with the content of the provided [[ByteBuffer]] through the channel.
		 * @param isCorruptedOrMayHaveAFramesAfter if `false` and the `contentBuffer` has space remaining, then a trailing sentinel (an empty frame) is appended to the transmitted data. Otherwise, a single frame is transmitted. */
		def sendFrame(contentBuffer: ByteBuffer, isCorruptedOrMayHaveAFramesAfter: Boolean): Unit = {
			frameHeaderBuffer.clear()
			VLQ.encodeUnsignedInt(contentBuffer.position(), frameHeaderWriter)
			frameHeaderBuffer.flip()
			frameBuffer(1) = contentBuffer
			val sentinelIsIncluded = if isCorruptedOrMayHaveAFramesAfter || !contentBuffer.hasRemaining then {
				// send a single frame
				contentBuffer.flip()
				scribe.trace(s"Transmission progress of message `$message` from `${context.myAddress}` to `${context.showPeerAddress}: sending a frame with size=${frameHeaderBuffer.limit} + ${contentBuffer.limit}, contentBuffer=${show.drainingByteBuffer(contentBuffer)}")
				false
			} else {
				// append the message-end sentinel value to the content buffer
				contentBuffer.put(0: Byte)
				// send the frame including the sentinel value (a trailing empty frame)
				contentBuffer.flip()
				scribe.trace(s"Transmission progress of message `$message` from `${context.myAddress}` to `${context.showPeerAddress}: sending the last frame with size=${frameHeaderBuffer.limit} + ${contentBuffer.limit - 1} + 1, contentBuffer=${show.drainingByteBuffer(contentBuffer)}")
				true
			}
			channel.write(frameBuffer, 0, 2, timeout, timeUnit, sentinelIsIncluded, handler)
		}

		/** Send the sentinel value, which is an empty frame when `forCorruptedPackage` is false, or a special value to indicate a serialization a failure.
		 * This distinction enables the deserializer to:
		 * 1. Detect failed serializations
		 * 2. Discard corrupted packets
		 * 3. Continue processing the following messages without requiring channel restart.
		 * */
		def sendSentinelValue(forCorruptedPackage: Boolean): Unit = {
			scribe.trace(s"Transmission progress of message `$message` from `${context.myAddress}` to `${context.showPeerAddress}: sending sentinel: forCorruptedPackage=$forCorruptedPackage.")
			frameHeaderBuffer.clear()
			if forCorruptedPackage then frameHeaderBuffer.put(CORRUPTED_PACKAGE_SENTINEL_ENCODED)
			else frameHeaderBuffer.put(0: Byte)
			frameHeaderBuffer.flip()
			channel.write(frameBuffer, 0, 1, timeout, timeUnit, true, handler)
		}

		inline def onCompleteWrapper(report: Report): Unit = {
			frameBuffer(1) = null
			scribe.trace(s"Transmission progress of message `$message` from `${context.myAddress}` to `${context.showPeerAddress}: completed with report=$report")
			onComplete(report, attachment)
		}

		if frameBuffer(1) ne null then throw new TransmissionInProgressException

		/** Implementation note: To optimize memory usage, this object can be moved outside the [[transmit]] method. This avoids repeated memory allocations but introduces mutable fields, which may require careful handling to ensure thread safety and correctness. */
		object handler extends Writer, CompletionHandler[java.lang.Long, Boolean] { thisHandler =>
			override val governingVersion: ProtocolVersion = msgVersion

			override def position: Int = writeEndBuffer.position

			/** This method is called by the serializer only; therefore, it runs within the thread that called the [[transmitWithAttachment]] method. */
			override def getBuffer(minimumRemaining: Int): ByteBuffer = {
				if writeEndBuffer.remaining() < minimumRemaining then {
					val nextBuffer = circularStorage.advanceWriteEnd()
					nextBuffer.clear()
					if !behindTransmissionStarted && behindTransmissionEnabled then {
						assert(writeEndBuffer eq circularStorage.readEnd)
						behindTransmissionStarted = true
						sendFrame(writeEndBuffer, false)
					}
					writeEndBuffer = nextBuffer
				}
				writeEndBuffer
			}

			// executed sequentially by a thread of the NIO2 group.
			override def completed(bytesTransmitted: java.lang.Long, sentinelWasIncluded: Boolean): Unit = {
				scribe.trace(s"Transmission progress of message `$message` from `${context.myAddress}` to `${context.showPeerAddress}: bytesTransmitted=$bytesTransmitted, sentinelWasIncluded=$sentinelWasIncluded`, remaining=${frameBuffer(0).remaining()}+${if sentinelWasIncluded then 0 else frameBuffer(1).remaining()}")
				if frameBuffer(0).hasRemaining then channel.write(frameBuffer, 0, 2, timeout, timeUnit, sentinelWasIncluded, thisHandler)
				else if frameBuffer(1).hasRemaining then channel.write(frameBuffer, 1, 2, timeout, timeUnit, sentinelWasIncluded, thisHandler)
				else {
					if isSerializationCompleted then {
						if circularStorage.advanceReadEnd() then sendFrame(circularStorage.readEnd, circularStorage.hasPendingBuffersBehind)
						else if sentinelWasIncluded then onCompleteWrapper(Delivered)
						else sendSentinelValue(false)
					}
					// This point may be reached only if behind transmission is enabled.
					// If the serialization failed, write the corrupted-package-sentinel before completing with the failure
					else if cancellationReason ne null then {
						if sentinelWasIncluded then onCompleteWrapper(cancellationReason)
						else sendSentinelValue(true)
					}
					// This point may be reached only if behind transmission is enabled and the transmission of the first frame took less time than the serialization of the whole message to be completed by the thread that called the `transmit` method.
					else if !behindTransmissionStopped then {
						// if the read-end catches up to the write-end, stop the behind transmission; else fill the frame buffer with the content of the read-end buffer and transmit it through the channel.
						val aPendingBufferIsBehind = circularStorage.advanceReadEnd() && circularStorage.hasPendingBuffersBehind // behind transmission is stopped only when serialization is slower than transmission.
						behindTransmissionStopped = !aPendingBufferIsBehind
						if aPendingBufferIsBehind || isSerializationCompleted then sendFrame(circularStorage.readEnd, aPendingBufferIsBehind)
					}
				}
			}

			override def failed(exc: Throwable, dummy: Boolean): Unit = {
				onCompleteWrapper(TransmissionFailure(message, exc))
			}
		}

		writeEndBuffer.clear()
		try {
			serializer.serialize(message, handler)
			isSerializationCompleted = true
			if !behindTransmissionStarted || behindTransmissionStopped then sendFrame(circularStorage.readEnd, circularStorage.hasPendingBuffersBehind)
		} catch {
			case se: SerializationException =>
				if behindTransmissionStarted then cancellationReason = SerializationProblem(message, se, true)
				else onCompleteWrapper(SerializationProblem(message, se, false))
			case scala.util.control.NonFatal(e) =>
				onCompleteWrapper(TransmissionFailure(message, e))
		}
	}
}