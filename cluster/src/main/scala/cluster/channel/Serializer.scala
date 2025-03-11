package readren.matrix
package cluster
package channel

import cluster.channel.Serializer.Writer
import cluster.misc.VLQ

import readren.matrix.cluster.service.ProtocolVersion

import java.nio.ByteBuffer

object Serializer {

	/** The number of consecutive content bytes required by the operation that calls [[Writer.getBuffer]] with the greatest argument. There are two: [[Writer.putLong]] and [[Writer.putDouble]]. */
	inline val BUFFER_SIZE_REQUIRED_BY_MOST_DEMANDING_OPERATION = 9

	/** A special value written by the [[Writer.write]] method before every back reference.
	 *
	 * The chosen value intentionally coincides with the only byte value that the variable-length-quantities produced by [[util.VLQ.encodeInt]] and [[util.VLQ.encodeLong]] never start with.
	 *
	 * The semantic of this header is nullified escaping it with another byte with the same value. */
	val BACK_REFERENCE_HEADER: Byte = 0x40.toByte

	sealed trait Outcome

	case object Success extends Outcome

	case class Unsupported(position: Int, explanation: String) extends Outcome

	abstract class Writer extends VLQ.ByteWriter {

		/** Memorizes the components that were already serialized associated with the order in which they were serialized.
		 * //TODO consider implementing a map that reuses the entries to avoid the cost of memory allocation and reclaim. */
		private val refs: java.util.IdentityHashMap[AnyRef, Int] = new java.util.IdentityHashMap()

		/** Flag that determines if the first byte produced by the serialization of the next component should be escaped if it is equal to [[BACK_REFERENCE_HEADER]].
		 * This flag is set by the [[write]] method before calling [[writeFull]] when the argument is not deduplicated (is the first occurrence of it within the context of the serialization of the composite that contains it).
		 * This flag checked and unconditionally cleared by all member methods (of [[Writer]]) that write something to the backing buffer. */
		private var haveToEscape: Boolean = false

		/** The version ID of the message type that the serializer should produce.
		 * This is required to support coexistence and interaction between different versions of the same component in a distributed system.
		 * It ensures that the serializer generates a serialized representation of the message that is compatible with the specified version, accounting for changes in the message structure (e.g., added or removed fields) across versions. */
		def versionToSerializeAs: ProtocolVersion

		/** The implementation must return a [[ByteBuffer]] with at least `minimumRemaining` bytes of remaining space to write.
		 * The caller must not write more than the provided `minimumRemaining` bytes. */
		def getBuffer(minimumRemaining: Int): ByteBuffer
		
		/** The implementation must return the position of the backing buffer.
		 * This method is used for diagnostic purposes only. */
		def position: Int

		def putBoolean(boolean: Boolean): Unit = {
			getBuffer(1).put(if boolean then 1: Byte else 0: Byte)
		}

		def putByte(byte: Byte): Unit = {
			if haveToEscape && byte == BACK_REFERENCE_HEADER then {
				val buffer = getBuffer(1 + 1)
				buffer.put(BACK_REFERENCE_HEADER)
				buffer.put(byte)
			} else getBuffer(1).put(byte)
			haveToEscape = false
		}

		def putChar(char: Char): Unit = {
			if haveToEscape && (char >>> (16 - 8)) == BACK_REFERENCE_HEADER then {
				val buffer = getBuffer(2 + 1)
				buffer.put(BACK_REFERENCE_HEADER)
				buffer.putChar(char)
			} else getBuffer(2).putChar(char)
			haveToEscape = false
		}

		def putShort(short: Short): Unit = {
			if haveToEscape && (short >>> (16 - 8)) == BACK_REFERENCE_HEADER then {
				val buffer = getBuffer(2 + 1)
				buffer.put(BACK_REFERENCE_HEADER)
				buffer.putShort(short)
			}
			getBuffer(2).putShort(short)
			haveToEscape = false
		}

		def putInt(int: Int): Unit = {
			if haveToEscape && (int >>> (32 - 8)) == BACK_REFERENCE_HEADER then {
				val buffer = getBuffer(4 + 1)
				buffer.put(BACK_REFERENCE_HEADER)
				buffer.putInt(int)
			} else getBuffer(4).putInt(int)
			haveToEscape = false
		}

		def putLong(long: Long): Unit = {
			if haveToEscape && (long >>> (64 - 8)) == BACK_REFERENCE_HEADER then {
				val buffer = getBuffer(8 + 1)
				buffer.put(BACK_REFERENCE_HEADER)
				buffer.putLong(long)
			} else getBuffer(8).putLong(long)
			haveToEscape = false
		}

		def putFloat(float: Float): Unit = {
			if haveToEscape && (java.lang.Float.floatToRawIntBits(float) >>> (32 - 8)) == BACK_REFERENCE_HEADER then {
				val buffer = getBuffer(4 + 1)
				buffer.put(BACK_REFERENCE_HEADER)
				buffer.putFloat(float)
			} else getBuffer(8).putFloat(float)
			haveToEscape = false
		}

		def putDouble(double: Double): Unit = {
			if haveToEscape && (java.lang.Double.doubleToRawLongBits(double) >>> (64 - 8)) == BACK_REFERENCE_HEADER then {
				val buffer = getBuffer(8 + 1)
				buffer.put(BACK_REFERENCE_HEADER)
				buffer.putDouble(double)
			} else getBuffer(8).putDouble(double)
			haveToEscape = false
		}

		inline def putIntVlq(int: Int): Unit = {
			VLQ.encodeInt(int, this)
		}

		inline def putLongVlq(long: Long): Unit = {
			VLQ.encodeLong(long, this)
		}

		inline def putUnsignedIntVlq(unsignedInt: Int): Unit = {
			VLQ.encodeUnsignedInt(unsignedInt, this)
		}

		inline def putUnsignedLongVlq(unsignedLong: Long): Unit = {
			VLQ.encodeUnsignedLong(unsignedLong, this)
		}

		def putBytes(bytes: Array[Byte]): Unit = {
			if haveToEscape && bytes(0) == BACK_REFERENCE_HEADER then {
				getBuffer(1).put(BACK_REFERENCE_HEADER)
			}
			// TODO optimize
			for i <- bytes.indices do getBuffer(1).put(bytes(i))
			haveToEscape = false
		}

		/** Unconditionally serializes the provided `value` to the backing buffer using the provided [[Serializer]].
		 * No deduplication is applied to provided object, but its fields are deduplicated if their corresponding serializers determine so. */
		inline def writeFull[M](value: M)(using sM: Serializer[M]): Outcome =
			sM.serialize(value, this)

		/**
		 * Either, serializes the object referenced by `ref` to the backing buffer using the provided [[Serializer]], or writes a back-reference if the object has already been serialized within the context of the containing composite.
		 *
		 * This method checks whether the object has already been serialized. If it has, a back-reference to the previously serialized object is written. Otherwise, the object is serialized using the provided [[Serializer]].
		 *
		 * **Important:** This method must be paired with [[Deserializer.Reader.read]] on the deserializer side. Failing to do so will result in the deserialization of this component and all subsequent composites failing.
		 */
		def write[R <: AnyRef](ref: R)(using sR: Serializer[R]): Serializer.Outcome = {
			val nextRefKey = refs.size + 1
			val refKey = refs.computeIfAbsent(ref, r => nextRefKey)
			if refKey == nextRefKey then {
				haveToEscape = true
				writeFull(ref)
			} else {
				putByte(BACK_REFERENCE_HEADER)
				putUnsignedIntVlq(refKey)
				Serializer.Success
			}
		}
	}

	extension (previous: Outcome) {
		inline def andThen[A](a: A, writer: Writer)(using serializer: Serializer[A]): Outcome = {
			previous match {
				case Success => serializer.serialize(a, writer)
				case unsupported => unsupported
			}
		}
	}
}

trait Serializer[A] { self =>
	def serialize(message: A, writer: Writer): Serializer.Outcome
}