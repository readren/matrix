package readren.nexus
package cluster.serialization

import cluster.misc.VLQ
import cluster.serialization.Serializer.Writer

import java.nio.ByteBuffer
import scala.compiletime.summonFrom
import scala.deriving.Mirror

object Serializer {

	/** The number of consecutive content bytes required by the operation that calls [[Writer.getBuffer]] with the greatest argument. There are two: [[Writer.putLong]] and [[Writer.putDouble]]. */
	inline val BUFFER_SIZE_REQUIRED_BY_MOST_DEMANDING_OPERATION = 9

	/** A special value written by the [[Writer.write]] method before every back reference.
	 *
	 * The chosen value intentionally coincides with the only byte value that the variable-length-quantities produced by [[util.VLQ.encodeInt]] and [[util.VLQ.encodeLong]] never start with.
	 *
	 * The semantic of this header is nullified escaping it with another byte with the same value. */
	val BACK_REFERENCE_HEADER: Byte = 0x40.toByte

	class SerializationException(val position: Int, explanation: String = null, cause: Throwable = null) extends RuntimeException(explanation, cause) {
		def this(position: Int, cause: Throwable) = this(position, null, cause)
	}

	abstract class Writer extends VLQ.ByteWriter {

		/** Memorizes the components that were already serialized associated with the order in which they were serialized.
		 * //TODO consider implementing a map that reuses the entries to avoid the cost of memory allocation and reclaim. */
		private val refs: java.util.IdentityHashMap[AnyRef, Int] = new java.util.IdentityHashMap()

		/** Flag that determines if the first byte produced by the serialization of the next component should be escaped if it is equal to [[BACK_REFERENCE_HEADER]].
		 * This flag is set by the [[write]] method before calling [[writeFull]] when the argument is not deduplicated (is the first occurrence of it within the context of the serialization of the composite that contains it).
		 * This flag checked and unconditionally cleared by all member methods (of [[Writer]]) that write something to the backing buffer. */
		private var haveToEscape: Boolean = false

		/** The version to serialize with. 
		 * Exposed here because the serialized representation format depends on the rules defined by this specific version.
		 * This is required to support coexistence and interaction between different versions of the same component in a distributed system.
		 * It ensures that the serializer generates a serialized representation of the message that is compatible with the specified version, accounting for changes in the message structure (e.g., added or removed fields) across versions. */
		val governingVersion: ProtocolVersion

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
		 * No deduplication is applied to the provided object, but its fields are deduplicated if their corresponding serializers determine so. */
		inline def writeFull[M](value: M)(using sM: Serializer[M]): Unit =
			sM.serialize(value, this)

		/**
		 * Either, serializes the object referenced by `ref` to the backing buffer using the provided [[Serializer]], or writes a back-reference if the object has already been serialized within the context of the containing composite.
		 *
		 * This method checks whether the object has already been serialized. If it has, a back-reference to the previously serialized object is written. Otherwise, the object is serialized using the provided [[Serializer]].
		 *
		 * __Important__: This method must be paired with [[Deserializer.Reader.read]] on the deserializer side. Failing to do so will result in the deserialization of this component and all subsequent composites failing.
		 */
		// TODO use [[IsValueOrReferenceTest]] like in the deserializer
		def write[R <: AnyRef](ref: R)(using sR: Serializer[R]): Unit = {
			val nextRefKey = refs.size + 1
			val refKey = refs.computeIfAbsent(ref, r => nextRefKey)
			if refKey == nextRefKey then {
				haveToEscape = true
				writeFull(ref)
			} else {
				putByte(BACK_REFERENCE_HEADER)
				putUnsignedIntVlq(refKey)
			}
		}
	}

	def apply[A](using serializer: Serializer[A]): Serializer[A] = serializer

	inline def derive[A](inline mode: NestedSumMatchMode): Serializer[A] = ${ SerializerDerivation.deriveSerializerImpl[A]('mode) }

}

/**
 * Serializes values of type `A` to a binary format.
 *
 * @note DESIGN: This is invariant (not contravariant) to prevent ambiguous implicit resolution and ensure correct discriminator handling for sum vs. product types. */
trait Serializer[A] { self =>
	def serialize(message: A, writer: Writer): Unit
}