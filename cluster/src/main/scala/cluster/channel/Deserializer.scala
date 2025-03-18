package readren.matrix
package cluster.channel

import cluster.channel.Deserializer.{Problem, Reader}
import cluster.misc.VLQ
import cluster.service.ProtocolVersion

import java.nio.ByteBuffer
import scala.language.experimental.erasedDefinitions
import scala.util.NotGiven

object Deserializer {
	/** The number of consecutive content bytes required by the operation that calls [[Reader.getContentBytes]] with the greatest argument. There are two: [[Reader.readLong]] and [[Reader.readDouble]]. */
	inline val CONSECUTIVE_CONTENT_BYTES_REQUIRED_BY_MOST_DEMANDING_OPERATION = 8


	sealed trait Problem

	case class Failure(position: Int, cause: Throwable) extends Problem

	case class Mismatch(position: Int, explanation: String) extends Problem

	/** The [[Deserializer]] expected more bytes than the contained in the received frames. */
	class LengthMismatchException extends Exception

	/** The frame-header is not aligned with the boundaries of the serialized parts. */
	class AlignmentMismatchException extends Exception

	extension [A](previous: Problem | A) {

		inline def map[B](inline f: A => B): Problem | B = {
			previous match {
				case problemA: Problem => problemA
				case a: A => f(a)
			}
		}
	}

	trait ValueOrReferenceTest[T] {
		def isValueType: Boolean
	}

	given whenReference[T](using ev: T <:< AnyRef): ValueOrReferenceTest[T] with {
		def isValueType = false
	}

	given whenValue[T](using ev: T <:< AnyVal): ValueOrReferenceTest[T] with {
		def isValueType = true
	}


	abstract class Reader extends VLQ.ByteReader {

		/** Memorizes the components that were already deserialized in the order they were serialized. */
		private val refs: scala.collection.mutable.ArrayBuffer[Any] = scala.collection.mutable.ArrayBuffer.empty


		/** The version ID of the message type that the deserializer should expect.
		 * This is required to support coexistence and interaction between different versions of the same component in a distributed system.
		 * It ensures that the deserializer can correctly interpret the serialized representation of the message, accounting for changes in the message structure (e.g., added or removed fields) across versions. */
		def versionToDeserializeFrom: ProtocolVersion

		/** The implementation should return the position of the backing [[ByteBuffer]] from where the bytes are read.
		 * This method is used for diagnostic only. */
		def position: Int

		/** The implementation must return a [[ByteBuffer]] with at least `maxBytesToConsume` of content bytes starting from its current position. */
		def getContentBytes(maxBytesToConsume: Int): ByteBuffer

		/** The implementation must return the same byte as `getContentBytes(1).get()` would, but without consuming it. */
		def peekByte: Byte

		/** Consumes the next byte if it is equal to the provided one.
		 *
		 * @return `true` if the next byte matches and is consumed; false otherwise. */
		inline def consumeByteIfEqualTo(byte: Byte): Boolean = {
			if peekByte == byte then {
				getContentBytes(1).get()
				true
			} else false
		}

		/** Reads the next content [[Byte]] from the backing buffer consuming it.
		 *
		 * @throws LengthMismatchException if the end-of-stream is reached.
		 * */
		inline def readByte(): Byte = {
			getContentBytes(1).get()
		}

		/** Reads the next content [[Short]] from the backing buffer consuming it.
		 *
		 * @throws LengthMismatchException if the end-of-stream is reached.
		 * @throws AlignmentMismatchException if a frame boundary is touched.
		 * */
		inline def readShort(): Short = {
			getContentBytes(2).getShort
		}

		/** Reads the next content [[Int]] from the backing buffer consuming it.
		 *
		 * @throws LengthMismatchException if the end-of-stream is reached.
		 * @throws AlignmentMismatchException if a frame boundary is touched.
		 * */
		inline def readInt(): Int = {
			getContentBytes(4).getInt
		}

		inline def readIntVlq(): Int = {
			VLQ.decodeInt(this)
		}

		inline def readUnsignedIntVlq(): Int = {
			VLQ.decodeUnsignedInt(this)
		}

		/** Reads the next content [[Long]] from the backing buffer consuming it.
		 *
		 * @throws LengthMismatchException if the end-of-stream is reached.
		 * @throws AlignmentMismatchException if a frame boundary is touched.
		 * */
		inline def readLong(): Long = {
			getContentBytes(8).getLong
		}

		inline def readLongVlq(): Long = {
			VLQ.decodeLong(this)
		}

		inline def readUnsignedLongVlq(): Long = {
			VLQ.decodeUnsignedLong(this)
		}

		/** Reads the next content [[Float]] from the backing buffer consuming it.
		 *
		 * @throws LengthMismatchException if the end-of-stream is reached.
		 * @throws AlignmentMismatchException if a frame boundary is touched.
		 * */
		inline def readFloat(): Float = {
			getContentBytes(4).getFloat
		}

		/** Reads the next content [[Double]] from the backing buffer consuming it.
		 *
		 * @throws LengthMismatchException if the end-of-stream is reached.
		 * @throws AlignmentMismatchException if a frame boundary is touched.
		 * */
		inline def readDouble(): Double = {
			getContentBytes(8).getDouble
		}

		/** Reads the next `howMany` content bytes from the backing buffer consuming it.
		 *
		 * @throws LengthMismatchException if the end-of-stream is reached.
		 * @throws AlignmentMismatchException if a frame boundary is touched.
		 * */
		def readBytes(howMany: Int): Array[Byte] = {
			val array = new Array[Byte](howMany)
			// TODO optimize
			for i <- 0 to howMany do array(i) = getContentBytes(1).get()
			array
		}

		/** Unconditionally deserializes an instance of the specified type from the backing buffer using the provided [[Deserializer]].
		 *
		 * @return an instance of `A`; or a [[Problem]] if the deserialization failed. */
		inline def readFull[A](using deserializer: Deserializer[A]): A | Problem = {
			deserializer.deserialize(this)
		}

		/**
		 * Either deserializes an object from the backing buffer using the provided [[Deserializer]] or returns an already deserialized object, depending on whether the next content byte is a back-reference header.
		 *
		 * This method checks the next byte in the backing buffer. If it is a back-reference header, the method consumes the back-reference and returns the corresponding already deserialized object. Otherwise, it deserializes a new object from the buffer.
		 *
		 * **Important:** This method must be paired with [[Serializer.Writer.write]] on the serializer side.
		 * Failing to do so will result in the deserialization of this component and all subsequent composites failing.
		 */
		def read[R](using deserializer: Deserializer[R], ivR: ValueOrReferenceTest[R]): R | Problem = {
			// if next byte isn't a back reference header or is an escape then we have no back reference here.
			if !consumeByteIfEqualTo(Serializer.BACK_REFERENCE_HEADER) || peekByte == Serializer.BACK_REFERENCE_HEADER || ivR.isValueType then {
				val ref = readFull[R]
				refs.addOne(ref)
				ref
			} else {
				val refKey = readUnsignedIntVlq()
				refs(refKey).asInstanceOf[R]
			}
		}

		/** Helps to implement a [[Deserializer]] for a concrete class provided the [[Deserializer]]s for its fields. */
		inline def compose1[A <: AnyRef, Z](inline f: A => Z)(using dA: Deserializer[A]): Z | Problem = {
			read[A] match {
				case problemA: Problem => problemA
				case a: A => f(a)
			}
		}

		/** Helps to implement a [[Deserializer]] for a concrete class provided the [[Deserializer]]s for its fields. */
		inline def compose2[A <: AnyRef, B <: AnyRef, Z](inline f: (A, B) => Z)(using dA: Deserializer[A], dB: Deserializer[B]): Problem | Z = {
			read[A] match {
				case problemA: Problem => problemA
				case a: A => read[B] match {
					case problemB: Problem => problemB
					case b: B => f(a, b)
				}
			}
		}

		/** Helps to implement a [[Deserializer]] for a concrete class provided the [[Deserializer]]s for its fields. */
		inline def compose3[A <: AnyRef, B <: AnyRef, C <: AnyRef, Z](inline f: (A, B, C) => Z)(using dA: Deserializer[A], dB: Deserializer[B], dC: Deserializer[C]): Problem | Z = {
			read[A] match {
				case problemA: Problem => problemA
				case a: A => read[B] match {
					case problemB: Problem => problemB
					case b: B => read[C] match {
						case problemC: Problem => problemC
						case c: C => f(a, b, c)
					}
				}
			}
		}

		/** Helps to implement a [[Deserializer]] for a concrete class provided the [[Deserializer]]s for its fields. */
		inline def compose4[A <: AnyRef, B <: AnyRef, C <: AnyRef, D <: AnyRef, Z](inline f: (A, B, C, D) => Z)(using dA: Deserializer[A], dB: Deserializer[B], dC: Deserializer[C], dD: Deserializer[D]): Problem | Z = {
			read[A] match {
				case problemA: Problem => problemA
				case a: A => read[B] match {
					case problemB: Problem => problemB
					case b: B => read[C] match {
						case problemC: Problem => problemC
						case c: C => read[D] match {
							case problemD: Problem => problemD
							case d: D => f(a, b, c, d)
						}
					}
				}
			}
		}
	}
}

trait Deserializer[A] { self =>
	def deserialize(reader: Reader): Problem | A

	def map[B](f: A => B): Deserializer[B] = new Deserializer[B] {
		override def deserialize(reader: Reader): Problem | B = {
			self.deserialize(reader) match {
				case problem: Deserializer.Problem => problem
				case a: A @unchecked => f(a)
			}
		}
	}

	def flatMap[B](f: A => Deserializer[B]): Deserializer[B] = new Deserializer[B] {
		override def deserialize(reader: Reader): Problem | B = {
			self.deserialize(reader) match {
				case problem: Deserializer.Problem => problem
				case a: A @unchecked => f(a).deserialize(reader)
			}
		}
	}
}
