package readren.matrix
package cluster.channel

import cluster.channel.Serializer.{SerializationException, Writer}
import cluster.channel.Transmitter.*
import cluster.misc.{DualEndedCircularBuffer, VLQ}
import cluster.service.ProtocolVersion

import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousSocketChannel, CompletionHandler}
import java.util
import java.util.concurrent.TimeUnit
import scala.annotation.tailrec

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
}

/**
 * A capability for transmitting messages over an [[AsynchronousSocketChannel]].
 * 
 * Note: The underlying [[AsynchronousSocketChannel]] is not thread-safe and supports only one transmission at a time. Concurrent attempts to transmit messages will result in undefined behavior
 * 
 * @param channel the channel over which the messages will be transmitted.
 * @param buffersCapacity the capacity of each storage unit of the [[DualEndedCircularBuffer]]
 */
class Transmitter(channel: AsynchronousSocketChannel, buffersCapacity: Int = 8192) {
	assert(buffersCapacity >= Serializer.BUFFER_SIZE_REQUIRED_BY_MOST_DEMANDING_OPERATION)
	private val frameHeaderBuffer: ByteBuffer = ByteBuffer.allocate(VLQ.INT_MAX_LENGTH)
	private val frameHeaderWriter: VLQ.ByteWriter = (byte: Byte) => frameHeaderBuffer.put(byte) 
	private val contentBuffers: DualEndedCircularBuffer[ByteBuffer] = new DualEndedCircularBuffer[ByteBuffer](() => ByteBuffer.allocateDirect(buffersCapacity))
	private val frameBuffer: Array[ByteBuffer] = new Array(2)

	/** Consist of two buffers: the first for the frame header (which tells the size of the frame content in bytes), and the second for the frame content. */
	frameBuffer(0) = frameHeaderBuffer

	/**
	 * 
	 * @param message the message to transmit
	 * @param msgVersion the version id of messages to produce.
	 * @param timeout The maximum time to wait for the first chunk of bytes and between chunks of bytes are transmitted.
	 * @param timeUnit The unit of time for the `timeout` parameter.
	 * @param onComplete A consumer callback that is invoked when the transmission completes either successfully or not, with a [[Transmitter.Report]].
	 * @param serializer The serializer to be used for serializing the message into outgoing bytes.
	 * @tparam M The type of the `message` to be sent.
	 */
	inline def transmit[M](message: M, msgVersion: ProtocolVersion, timeout: Long = 1, timeUnit: TimeUnit = TimeUnit.SECONDS)(onComplete: Report => Unit)(using serializer: Serializer[M]): Unit = {
		transmitWithAttachment[M, Null](message, msgVersion, null, timeout, timeUnit) { (report, dummy) => onComplete(report) }
	}

	// assume that the thread that calls this method is named "main".
	def transmitWithAttachment[M, A](
		message: M,
		msgVersion: ProtocolVersion,
		attachment: A,
		timeout: Long = 1,
		timeUnit: TimeUnit = TimeUnit.SECONDS
	)(
		onComplete: (Report, A) => Unit
	)(using serializer: Serializer[M]): Unit = {
		/** This variable is modified only within the thread that called this method ([[transmitWithAttachment]]). */
		var writeEndBuffer = contentBuffers.writeEnd
		/** This variable is modified only within the thread that called this method ([[transmitWithAttachment]]). */
		var behindTransmissionStarted = false
		/** This variable is modified one time only, from false to true. */
		@volatile var behindTransmissionStopped = false
		/** This variable is modified one time only, from false to true. */
		@volatile var isSerializationCompleted = false
		/** This variable is modified one time only, from null to non-null. */
		@volatile var cancellationReason: SerializationProblem | Null = null

		/** Fills the frame-buffer with the content of the current read-end buffer */
		def fillFrame(): Unit = {
			val readEnd = contentBuffers.readEnd
			frameBuffer(1) = readEnd
			frameHeaderBuffer.clear()
			VLQ.encodeUnsignedInt(readEnd.limit, frameHeaderWriter)
		}

		/**
		 * Sends a frame with the content of the provided [[ByteBuffer]] through the channel. 
		 * This method is called within the thread that called the [[transmitWithAttachment]] method. */
		def sendFrame(contentBuffer: ByteBuffer): Unit = {
			frameHeaderBuffer.clear()
			VLQ.encodeUnsignedInt(contentBuffer.limit, frameHeaderWriter)
			frameBuffer(1) = contentBuffer
			channel.write(frameBuffer, 0, 2, timeout, timeUnit, false, handler)
		}

		inline def onCompleteWrapper(report: Report): Unit = {
			frameBuffer(1) = null
			onComplete(report, attachment)
		}

		if frameBuffer(1) != null then throw new TransmissionInProgressException

		/** Implementation note: To optimize memory usage, this object can be moved outside the [[transmit]] method. This avoids repeated memory allocations but introduces mutable fields, which may require careful handling to ensure thread safety and correctness. */
		object handler extends Writer, CompletionHandler[java.lang.Long, Boolean] { thisHandler =>
			override def versionToSerializeAs: ProtocolVersion = msgVersion

			override def position: Int = writeEndBuffer.position

			/** This method is called by the serializer only, therefore it runs within the thread that called the [[transmitWithAttachment]] method. */
			override def getBuffer(minimumRemaining: Int): ByteBuffer = {
				if writeEndBuffer.remaining() < minimumRemaining then {
					writeEndBuffer.flip()
					val nextBuffer = contentBuffers.advanceWriteEnd()
					nextBuffer.clear()
					if !behindTransmissionStarted then {
						behindTransmissionStarted = true
						sendFrame(writeEndBuffer)
					}
					writeEndBuffer = nextBuffer
				}
				writeEndBuffer
			}

			// executed sequentially by a thread of the NIO2 group.
			@tailrec
			override def completed(bytesTransmitted: java.lang.Long, sentinelWasSent: Boolean): Unit = {
				if cancellationReason != null then onCompleteWrapper(cancellationReason)
				else {
					if frameBuffer(0).hasRemaining then channel.write(frameBuffer, 0, 2, timeout, timeUnit, false, thisHandler)
					else if frameBuffer(1).hasRemaining then channel.write(frameBuffer, 1, 2, timeout, timeUnit, false, thisHandler)
					else {
						if isSerializationCompleted then {
							if contentBuffers.advanceReadEnd() then {
								fillFrame()
								completed(bytesTransmitted, false)
							} else if sentinelWasSent then onCompleteWrapper(Delivered)
							else {
								// Send the sentinel value, which is an empty frame.
								frameHeaderBuffer.clear()
								frameHeaderBuffer.put(0:Byte)
								channel.write(frameBuffer, 0, 1, timeout, timeUnit, true, thisHandler)
							}
						} else if !behindTransmissionStopped then {
							// this point is reached only when the time that takes to serialize the whole message is greater than the time to transmit the content of the first buffer.
							val aPendingStorageIsBehind = contentBuffers.advanceReadEnd() && contentBuffers.hasPendingStoragesBehind // behind transmission is stopped only when serialization is slower than transmission.
							behindTransmissionStopped = !aPendingStorageIsBehind
							if aPendingStorageIsBehind || isSerializationCompleted then {
								fillFrame()
								completed(bytesTransmitted, false)
							}
						}
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
			writeEndBuffer.flip()
			isSerializationCompleted = true
			if !behindTransmissionStarted || behindTransmissionStopped then sendFrame(contentBuffers.readEnd)
		} catch {
			case se: SerializationException =>
				if behindTransmissionStarted then cancellationReason = SerializationProblem(message, se, true)
				else onCompleteWrapper(SerializationProblem(message, se, false))
			case scala.util.control.NonFatal(e) =>
				onCompleteWrapper(TransmissionFailure(message, e))
		}
	}
}