package readren.nexus
package cluster.serialization

import cluster.misc.VLQ
import cluster.serialization.Deserializer.Reader

import java.nio.ByteBuffer
import scala.deriving.Mirror

object Deserializer {
	/** The number of consecutive content bytes required by the operation that calls [[Reader.getContentBytes]] with the greatest argument. There are two: [[Reader.readLong]] and [[Reader.readDouble]]. */
	inline val CONSECUTIVE_CONTENT_BYTES_REQUIRED_BY_MOST_DEMANDING_OPERATION = 8

	class DeserializationException(val position: Int, explanation: String = null, cause: Throwable = null) extends RuntimeException(explanation, cause) {
		def this(position: Int, cause: Throwable) = this(position, null, cause)
	}

	sealed abstract class PayloadCorruptedException(position: Int, explanation: String, cause: Throwable = null)
		extends DeserializationException(position, explanation, cause)

	final class NegativeLengthException(position: Int, val length: Long, val targetType: String)
		extends PayloadCorruptedException(position, s"Negative length $length decoded for $targetType at position $position.")

	final class LengthExceedsRemainingBytesException(position: Int, val requestedLength: Long, val remainingBytes: Int, val targetType: String)
		extends PayloadCorruptedException(position, s"Requested capacity $requestedLength for $targetType exceeds remaining package bytes ($remainingBytes) at position $position.")

	final class AllocationThresholdExceededException(position: Int, val requestedLength: Long, val maxAllowed: Long, val targetType: String)
		extends PayloadCorruptedException(position, s"Requested capacity $requestedLength for $targetType exceeds maximum safe allocation threshold ($maxAllowed) at position $position.")

	final class InvalidDiscriminatorException(position: Int, val discriminator: Int, val targetSumType: String)
		extends PayloadCorruptedException(position, s"Invalid discriminator $discriminator for sum type $targetSumType at position $position.")

	final class InvalidReferenceKeyException(position: Int, val refKey: Int, val tableSize: Int)
		extends PayloadCorruptedException(position, s"Invalid reference key $refKey (table size: $tableSize) at position $position.")

	abstract class Reader(
		protected val refs: RecyclableObjectBuffer = new RecyclableObjectBuffer(),
		val allocationLimits: AllocationLimits = AllocationLimits.Default
	) extends VLQ.ByteReader {

		/** Resets the reference deduplication buffer across composite message frames. */
		def resetDeduplication(): Unit = refs.reset()


		/** The version to deserialize with.
		 * Exposed here because the serialized representation format depends on the rules defined by this specific version.
		 * This is required to support coexistence and interaction between different versions of the same component in a distributed system.
		 * It ensures that the deserializer can correctly interpret the serialized representation of the message, accounting for changes in the message structure (e.g., added or removed fields) across versions. */
		def governingVersion: ProtocolVersion

		/** The implementation should return the position of the backing [[ByteBuffer]] from where the bytes are read.
		 * This method is used for diagnostic only. */
		def position: Int

		/** Returns the remaining content or buffer bytes available in the current package, or Int.MaxValue if unbounded. */
		def remainingContentBytes: Int = Int.MaxValue

		/** Validates requested length for array or raw byte allocations against remaining bytes and allocation limits.
		 * @throws NegativeLengthException if length < 0
		 * @throws LengthExceedsRemainingBytesException if length * elementByteSize > remainingContentBytes
		 * @throws AllocationThresholdExceededException if length or total bytes exceed configured limits */
		def validateArrayLength(length: Int, elementByteSize: Int, targetType: String): Int = {
			if length < 0 then throw new Deserializer.NegativeLengthException(position, length, targetType)
			val totalElementBytes = length.toLong * elementByteSize
			if totalElementBytes > remainingContentBytes then {
				throw new Deserializer.LengthExceedsRemainingBytesException(position, length, remainingContentBytes, targetType)
			}
			val maxElementsByBytes = allocationLimits.maxAllocationBytes / math.max(1, elementByteSize)
			val effectiveMaxElements = math.min(allocationLimits.maxCollectionElements, maxElementsByBytes)
			if length > effectiveMaxElements || totalElementBytes > allocationLimits.maxAllocationBytes then {
				throw new Deserializer.AllocationThresholdExceededException(position, length, effectiveMaxElements, targetType)
			}
			length
		}

		/** Validates requested count for collection builders against remaining bytes and allocation limits.
		 * @throws NegativeLengthException if count < 0
		 * @throws LengthExceedsRemainingBytesException if count * minElementByteSize > remainingContentBytes
		 * @throws AllocationThresholdExceededException if count exceeds allocationLimits.maxCollectionElements */
		def validateCollectionLength(count: Int, minElementByteSize: Int, targetType: String): Int = {
			if count < 0 then throw new Deserializer.NegativeLengthException(position, count, targetType)
			if minElementByteSize > 0 && (count.toLong * minElementByteSize > remainingContentBytes) then {
				throw new Deserializer.LengthExceedsRemainingBytesException(position, count, remainingContentBytes, targetType)
			}
			if count > allocationLimits.maxCollectionElements then {
				throw new Deserializer.AllocationThresholdExceededException(position, count, allocationLimits.maxCollectionElements, targetType)
			}
			count
		}

		/** Validates back-reference keys against the deduplication table size.
		 * @throws InvalidReferenceKeyException if refKey < 0 or refKey >= refs.size */
		def validateReferenceKey(refKey: Int): Int = {
			if refKey < 0 || refKey >= refs.size then {
				throw new Deserializer.InvalidReferenceKeyException(position, refKey, refs.size)
			}
			refKey
		}

		/** The implementation must return a [[ByteBuffer]] with at least `maxBytesToConsume` of content bytes starting from its current position. */
		def getContentBytes(maxBytesToConsume: Int): ByteBuffer

		/** The implementation must return the same byte as `getContentBytes(1).get()` would, but without consuming it.
		 * @throws LengthMismatchException if the [[Deserializer]] tries to read more bytes than the contained in the package. A package is a sequence of frames finalized with an empty frame.
		 * @throws UnexpectedBufferEnd if all the received bytes were consumed and the package was not fully read.
		 * */
		def peekByte: Byte

		/** Consumes the next byte if it is equal to the provided one.
		 *
		 * @return `true` if the next byte matches and is consumed; false otherwise.
		 * @throws LengthMismatchException if the [[Deserializer]] tries to read more bytes than the contained in the package. A package is a sequence of frames finalized with an empty frame.
		 * @throws UnexpectedBufferEnd if all the received bytes were consumed and the package was not fully read.
		 * */
		inline def consumeByteIfEqualTo(byte: Byte): Boolean = {
			if peekByte == byte then {
				getContentBytes(1).get()
				true
			} else false
		}
		
		inline def readBoolean(): Boolean = {
			getContentBytes(1).get() != 0
		}

		/** Reads the next content [[Byte]] from the backing buffer consuming it.
		 *
		 * @throws LengthMismatchException if the [[Deserializer]] tries to read more bytes than the contained in the package. A package is a sequence of frames finalized with an empty frame.
		 * @throws UnexpectedBufferEnd if all the received bytes were consumed and the package was not fully read.
		 * */
		inline def readByte(): Byte = {
			getContentBytes(1).get()
		}

		/** Reads the next content [[Short]] from the backing buffer consuming it.
		 *
		 * @throws LengthMismatchException if the [[Deserializer]] tries to read more bytes than the contained in the package. A package is a sequence of frames finalized with an empty frame.
		 * @throws UnexpectedBufferEnd if all the received bytes were consumed and the package was not fully read.
		 * @throws FrameMisalignmentException if a frame boundary is touched.
		 * */
		inline def readShort(): Short = {
			getContentBytes(2).getShort
		}

		/** Reads the next content [[Int]] from the backing buffer consuming it.
		 *
		 * @throws LengthMismatchException if the [[Deserializer]] tries to read more bytes than the contained in the package. A package is a sequence of frames finalized with an empty frame.
		 * @throws UnexpectedBufferEnd if all the received bytes were consumed and the package was not fully read.
		 * @throws FrameMisalignmentException if a frame boundary is touched.
		 * */
		inline def readInt(): Int = {
			getContentBytes(4).getInt
		}

		/** Reads the next content [[Int]] in VLQ format, from the backing buffer consuming it.
		 * @throws LengthMismatchException if the [[Deserializer]] tries to read more bytes than the contained in the package. A package is a sequence of frames finalized with an empty frame.
		 * @throws UnexpectedBufferEnd if all the received bytes were consumed and the package was not fully read.
		 */
		inline def readIntVlq(): Int = {
			VLQ.decodeInt(this)
		}

		/** Reads the next content unsigned [[Int]] in VLQ format, from the backing buffer consuming it.
		 * @throws LengthMismatchException if the [[Deserializer]] tries to read more bytes than the contained in the package. A package is a sequence of frames finalized with an empty frame.
		 * @throws UnexpectedBufferEnd if all the received bytes were consumed and the package was not fully read.
		 */
		inline def readUnsignedIntVlq(): Int = {
			VLQ.decodeUnsignedInt(this)
		}

		/** Reads the next content [[Long]] from the backing buffer consuming it.
		 *
		 * @throws LengthMismatchException if the end-of-stream is reached.
		 * @throws FrameMisalignmentException if a frame boundary is touched.
		 * */
		inline def readLong(): Long = {
			getContentBytes(8).getLong
		}

		/** Reads the next content [[Long]] in VLQ format, from the backing buffer consuming it.
		 * @throws LengthMismatchException if the [[Deserializer]] tries to read more bytes than the contained in the package. A package is a sequence of frames finalized with an empty frame.
		 * @throws UnexpectedBufferEnd if all the received bytes were consumed and the package was not fully read.
		 */
		inline def readLongVlq(): Long = {
			VLQ.decodeLong(this)
		}

		/** Reads the next content unsigned [[Long]] in VLQ format, from the backing buffer consuming it.
		 * @throws LengthMismatchException if the [[Deserializer]] tries to read more bytes than the contained in the package. A package is a sequence of frames finalized with an empty frame.
		 * @throws UnexpectedBufferEnd if all the received bytes were consumed and the package was not fully read.
		 */
		inline def readUnsignedLongVlq(): Long = {
			VLQ.decodeUnsignedLong(this)
		}

		/** Reads the next content [[Float]] from the backing buffer consuming it.
		 *
		 * @throws LengthMismatchException if the end-of-stream is reached.
		 * @throws UnexpectedBufferEnd if all the received bytes were consumed and the package was not fully read.
		 * @throws FrameMisalignmentException if a frame boundary is touched.
		 * */
		inline def readFloat(): Float = {
			getContentBytes(4).getFloat
		}

		/** Reads the next content [[Double]] from the backing buffer consuming it.
		 *
		 * @throws LengthMismatchException if the end-of-stream is reached.
		 * @throws UnexpectedBufferEnd if all the received bytes were consumed and the package was not fully read.
		 * @throws FrameMisalignmentException if a frame boundary is touched.
		 * */
		inline def readDouble(): Double = {
			getContentBytes(8).getDouble
		}

		/** Reads the next `length` content bytes from the backing buffer into `destination` starting at `offset`.
		 *
		 * @throws LengthMismatchException if the end-of-stream is reached.
		 * @throws UnexpectedBufferEnd if all the received bytes were consumed and the package was not fully read.
		 * @throws FrameMisalignmentException if a frame boundary is touched.
		 * */
		def readBytes(destination: Array[Byte], offset: Int, length: Int): Unit = {
			var currOffset = offset
			var remaining = length
			while remaining > 0 do {
				val buffer = getContentBytes(1)
				val chunk = math.min(remaining, buffer.remaining)
				buffer.get(destination, currOffset, chunk)
				currOffset += chunk
				remaining -= chunk
			}
		}

		/** Reads the next `howMany` content bytes from the backing buffer consuming it.
		 *
		 * @throws LengthMismatchException if the end-of-stream is reached.
		 * @throws UnexpectedBufferEnd if all the received bytes were consumed and the package was not fully read.
		 * @throws FrameMisalignmentException if a frame boundary is touched.
		 * */
		def readBytes(howMany: Int): Array[Byte] = {
			validateArrayLength(howMany, 1, "Array[Byte]")
			val array = new Array[Byte](howMany)
			readBytes(array, 0, howMany)
			array
		}

		/** Unconditionally deserializes an instance of the specified type from the backing buffer using the provided [[Deserializer]].
		 *
		 * @return an instance of `A`; or a [[Problem]] if the deserialization failed.
		 * @throws LengthMismatchException if the end-of-stream is reached.
		 * @throws UnexpectedBufferEnd if all the received bytes were consumed and the package was not fully read.
		 * @throws FrameMisalignmentException if a frame boundary is touched.
		 * */
		inline def readFull[A](using deserializer: Deserializer[A]): A = {
			deserializer.deserialize(this)
		}

		/**
		 * Either deserializes an object from the backing buffer using the provided [[Deserializer]] or returns an already deserialized object, depending on whether the next content byte is a back-reference header.
		 *
		 * This method checks the next byte in the backing buffer. If it is a back-reference header, the method consumes the back-reference and returns the corresponding already deserialized object. Otherwise, it deserializes a new object from the buffer.
		 *
		 * Important: This method must be paired with [[Serializer.Writer.write]] on the serializer side.
		 * Failing to do so will result in the deserialization of this component and all subsequent composites failing.
		 * @throws LengthMismatchException if the end-of-stream is reached.
		 * @throws UnexpectedBufferEnd if all the received bytes were consumed and the package was not fully read.
		 * @throws FrameMisalignmentException if a frame boundary is touched.
		 */
		def read[R](using deserializer: Deserializer[R], ivR: ValueOrReferenceTest[R]): R = {
			if ivR.isValueType then readFull[R]
			else if !consumeByteIfEqualTo(Serializer.BACK_REFERENCE_HEADER) || peekByte == Serializer.BACK_REFERENCE_HEADER then {
				val ref = readFull[R]
				refs.add(ref.asInstanceOf[AnyRef])
				ref
			} else {
				val refKey = validateReferenceKey(readIntVlq())
				refs(refKey).asInstanceOf[R]
			}
		}
	}

	def apply[A](using deserializer: Deserializer[A]): Deserializer[A] = deserializer

	/** Derives a [[Deserializer]] for a singleton, product, or sealed sum type `A` using [[NestedSumMatchMode.FLAT]] for nested sum types. */
	inline def derive[A]: Deserializer[A] = ${ DeserializerDerivation.deriveDeserializerImpl[A]('{ NestedSumMatchMode.FLAT }) }

	/** Derives a [[Deserializer]] for a singleton, product, or sealed sum type `A` using the specified [[NestedSumMatchMode]]. */
	inline def derive[A](inline mode: NestedSumMatchMode): Deserializer[A] = ${ DeserializerDerivation.deriveDeserializerImpl[A]('mode) }

}

/**
 * Deserializes values of type `A` from a binary format.
 *
 * @note DESIGN: This is invariant (not contravariant) to prevent ambiguous implicit resolution and ensure correct discriminator handling for sum vs. product types. */
trait Deserializer[A] { self =>

	def deserialize(reader: Reader): A

	def map[B](f: A => B): Deserializer[B] = (reader: Reader) => {
		f(self.deserialize(reader))
	}

	def flatMap[B](f: A => Deserializer[B]): Deserializer[B] = (reader: Reader) => {
		f(self.deserialize(reader)).deserialize(reader)
	}
}
