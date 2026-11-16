package readren.nexus
package cluster.channel

import cluster.misc.VLQ
import cluster.serialization.Serializer.SerializationException
import cluster.service.ChannelId
import cluster.service.Protocol.ContactAddress

import readren.common.Maybe

/** Shared wire framing sentinels, configuration defaults, diagnostic context, and completion report types used across channel transmitter implementations. */
object TransmissionProtocol {
	/** Outcome report delivered to the completion callback of a message transmission operation. */
	sealed trait Report

	/** Report delivered when message serialization and channel transmission complete successfully. */
	case object Delivered extends Report

	/** Marker trait for transmission reports indicating that the message was not delivered completely. */
	sealed trait NotDelivered extends Report

	/** Report delivered when transmission fails due to a [[cluster.serialization.Serializer.SerializationException]].
	 * @param rootMessage The complete message that was intended for serialization and transmission.
	 * @param problem The exception thrown by the serializer.
	 * @param aFragmentWasTransmitted Indicates whether a fragment of the message reached the channel before the serialization failure occurred (`false` by default for serialize-before-transmit pipelines). */
	case class SerializationProblem(rootMessage: Any, problem: SerializationException, aFragmentWasTransmitted: Boolean = false) extends NotDelivered

	/** Report delivered when transmission fails due to a channel I/O error or buffer capacity exhaustion.
	 * @param rootMessage The complete message that was intended for transmission.
	 * @param cause The underlying exception that caused transmission to fail. */
	case class TransmissionFailure(rootMessage: Any, cause: Throwable) extends NotDelivered

	/** Report delivered when reading an out-of-line [[readren.common.ByteSource]] fails, encounters premature EOF, or suffers size drift during trailing stream transmission.
	 * @param rootMessage The complete message whose trailing stream failed.
	 * @param cause The underlying I/O or framing exception encountered while reading the [[readren.common.ByteSource]]. */
	case class ByteSourceProblem(rootMessage: Any, cause: Throwable) extends NotDelivered

	/** Pre-encodes a 32-bit unsigned sentinel value into its fixed 5-byte [[cluster.misc.VLQ]] representation at class initialization time.
	 * @param sentinelValue The unsigned 32-bit sentinel value (stored in a `Long`) to encode.
	 * @return A 5-byte array containing the [[cluster.misc.VLQ]] encoding of `sentinelValue`. */
	private def encodeSentinel(sentinelValue: Long): Array[Byte] = {
		val array = new Array[Byte](VLQ.INT_MAX_LENGTH)
		val writer: VLQ.ByteWriter = new VLQ.ByteWriter {
			var pos = 0
			override def putByte(byte: Byte): Unit = {
				array(pos) = byte
				pos += 1
			}
		}
		VLQ.encodeUnsignedInt(sentinelValue.toInt, writer)
		array
	}

	/** Special reserved frame-length value (`-2` in 32-bit two's complement) used as the in-band sentinel of a corrupted or aborted package. */
	inline val CORRUPTED_PACKAGE_SENTINEL = 0xffff_fffeL

	/** Pre-encoded 5-byte unsigned VLQ representation of [[TransmissionProtocol.CORRUPTED_PACKAGE_SENTINEL]]. */
	val CORRUPTED_PACKAGE_SENTINEL_ENCODED: Array[Byte] = encodeSentinel(CORRUPTED_PACKAGE_SENTINEL)

	/** Special reserved frame-length value (`-3` in 32-bit two's complement) indicating that a control package is immediately followed by a concatenated trailing stream of bulk payloads. */
	inline val STREAM_FOLLOWS_SENTINEL = 0xffff_fffdL

	/** Pre-encoded 5-byte unsigned VLQ representation of [[TransmissionProtocol.STREAM_FOLLOWS_SENTINEL]]. */
	val STREAM_FOLLOWS_SENTINEL_ENCODED: Array[Byte] = encodeSentinel(STREAM_FOLLOWS_SENTINEL)

	/** Default buffer capacity (8 KiB) for each direct circular storage buffer. */
	inline val DEFAULT_BUFFERS_CAPACITY = 8192

	/** Default maximum number of buffers in each [[cluster.misc.DualEndedCircularStorage]] ring. */
	inline val DEFAULT_MAX_BUFFERS = 128

	/** Default minimum number of buffers retained in each [[cluster.misc.DualEndedCircularStorage]] ring on idle transitions. */
	inline val DEFAULT_MIN_BUFFERS_ON_IDLE = 2

	/** Default capacity (64 KiB) for the direct streaming transfer buffer. */
	inline val DEFAULT_STREAM_TRANSFER_BUFFER_CAPACITY = 65536

	/** Default maximum number of [[java.nio.ByteBuffer]] instances submitted in a single gathering socket write call. */
	inline val DEFAULT_MAX_GATHERING_BUFFERS = 16

	/** Diagnostic addressing context associated with a communication channel. */
	trait Context {
		/** The local contact address representing this node. */
		def myAddress: ContactAddress

		/** The optional contact address representing the remote peer, or [[readren.common.Maybe.empty]] if unknown. */
		def oPeerAddress: Maybe[ContactAddress]

		/** The unique identifier of the communication channel. */
		def channelId: ChannelId

		/** Formats the peer address as a human-readable string for diagnostic logs. */
		def showPeerAddress: String = oPeerAddress.fold("unknown")(_.toString)
	}
}
