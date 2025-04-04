package readren.matrix
package cluster.misc

import VLQ.*
import munit.ScalaCheckSuite
import org.scalacheck.Prop

import java.nio.ByteBuffer

class VLQTest extends ScalaCheckSuite  {

	class Buffer extends ByteWriter, ByteReader {
		private val backingBuffer = ByteBuffer.allocate(1024)

		override def putByte(byte: Byte): Unit = backingBuffer.put(byte)

		override def readByte(): Byte = backingBuffer.get()

		def flip(): Unit = backingBuffer.flip()

		def clear(): Unit = backingBuffer.clear()

		def hasRemaining: Boolean = backingBuffer.hasRemaining

		def position: Int = backingBuffer.position()

		def get(position: Int, destination: Array[Byte]): Unit = backingBuffer.get(position, destination)
	}


	property("check `vlqToLong` and `longToVlq` are inverses") {
		val buffer = new Buffer
		def check(expected: Long): Boolean = {
			buffer.clear()
			encodeLong(expected, buffer)
			buffer.flip()
			expected == decodeLong(buffer)
		}
		List[Long](0, 0x39, -0x39, 0x40, -0x40, Long.MinValue, Long.MaxValue).forall(check)
			&& Prop.forAll(check)
	}

	property("check `vlqToInt` and `intToVlq` are inverses") {
		val buffer = new Buffer

		def check(expected: Int): Boolean = {
			buffer.clear()
			encodeInt(expected, buffer)
			buffer.flip()
			expected == decodeInt(buffer)
		}

		List[Int](0, 0x39, -0x39, 0x40, -0x40, Integer.MIN_VALUE, Integer.MAX_VALUE).forall(check)
			&& Prop.forAll(check)
	}

	property("check `vlqToPositiveLong` and `positiveLongToVlq` are inverses") {
		val buffer = new Buffer
		def check(expected: Long): Boolean = {
			buffer.clear()
			encodeUnsignedLong(expected, buffer)
			buffer.flip()
			expected == decodeUnsignedLong(buffer)
		}
		List[Long](0, 0x7f, 0x80, Long.MaxValue).forall(check)
			&& Prop.forAll(check)
	}

	property("check `vlqToPositiveInt` and `positiveIntToVlq` are inverses") {
		val buffer = new Buffer

		def check(expected: Int): Boolean = {
			buffer.clear()
			encodeUnsignedInt(expected, buffer)
			buffer.flip()
			expected == decodeUnsignedInt(buffer)
		}

		List[Int](0, 0x7f, 0x80, Integer.MIN_VALUE, Integer.MAX_VALUE).forall(check)
			&& Prop.forAll(check)
	}


	test("just for watching") {
		val buffer = new Buffer

		encodeUnsignedLong(0, buffer)
		encodeUnsignedLong(1, buffer)
		encodeUnsignedLong(10, buffer)
		encodeUnsignedLong(0x40, buffer)
		encodeUnsignedLong(100, buffer)
		encodeUnsignedLong(127, buffer)
		encodeUnsignedLong(128, buffer)
		encodeUnsignedLong(1000, buffer)
		encodeUnsignedLong(10000, buffer)
		encodeUnsignedLong(100000, buffer)
		encodeUnsignedLong(1000000, buffer)
		encodeUnsignedLong(10000000, buffer)
		encodeUnsignedLong(100000000, buffer)
		encodeUnsignedLong(1000000000, buffer)
		encodeUnsignedLong(Integer.MAX_VALUE, buffer)
		encodeUnsignedLong(10000000000L, buffer)
		encodeUnsignedLong(100000000000L, buffer)
		encodeUnsignedLong(1000000000000L, buffer)
		encodeUnsignedLong(10000000000000L, buffer)
		encodeUnsignedLong(100000000000000L, buffer)
		encodeUnsignedLong(1000000000000000L, buffer)
		encodeUnsignedLong(10000000000000000L, buffer)
		encodeUnsignedLong(100000000000000000L, buffer)
		encodeUnsignedLong(1000000000000000000L, buffer)
		encodeUnsignedLong(Long.MaxValue, buffer)
		encodeUnsignedLong(0x80_00_00_00_00_00_00_00L, buffer)
		encodeUnsignedLong(0x80_00_00_00_00_00_00_01L, buffer)
		encodeUnsignedLong(0xff_ff_ff_ff_ff_ff_ff_ffL, buffer)

		buffer.flip()

		while buffer.hasRemaining do {
			val p0 = buffer.position
			val value = java.lang.Long.toUnsignedString(decodeUnsignedLong(buffer))
			val length = buffer.position - p0
			val bytes = new Array[Byte](length)
			buffer.get(p0, bytes)
			println(f"${length}%2d: ${value}%21s, ${bytes.map(byte => (f"${byte}%02X").takeRight(2)).mkString("_")}")
		}

		buffer.clear()

		// Encode an integer
		encodeLong(0, buffer)
		encodeLong(1, buffer)
		encodeLong(-1, buffer)
		encodeLong(2, buffer)
		encodeLong(-2, buffer)
		encodeLong(0x3f, buffer)
		encodeLong(-0x3f, buffer)
		encodeLong(0x40, buffer)
		encodeLong(-0x40, buffer)
		encodeLong(100, buffer)
		encodeLong(-100, buffer)
		encodeLong(0x7f, buffer)
		encodeLong(-0x7f, buffer)
		encodeLong(0x80, buffer)
		encodeLong(-0x80, buffer)
		encodeLong(1000, buffer)
		encodeLong(-1000, buffer)
		encodeLong(10000, buffer)
		encodeLong(-10000, buffer)
		encodeLong(100000, buffer)
		encodeLong(-100000, buffer)
		encodeLong(1000000, buffer)
		encodeLong(-1000000, buffer)
		encodeLong(10000000, buffer)
		encodeLong(-10000000, buffer)
		encodeLong(100000000, buffer)
		encodeLong(-100000000, buffer)
		encodeLong(1000000000, buffer)
		encodeLong(-1000000000, buffer)
		encodeLong(Integer.MAX_VALUE, buffer)
		encodeLong(Integer.MIN_VALUE, buffer)
		encodeLong(10000000000L, buffer)
		encodeLong(-10000000000L, buffer)
		encodeLong(100000000000L, buffer)
		encodeLong(-100000000000L, buffer)
		encodeLong(1000000000000L, buffer)
		encodeLong(-1000000000000L, buffer)
		encodeLong(10000000000000L, buffer)
		encodeLong(-10000000000000L, buffer)
		encodeLong(100000000000000L, buffer)
		encodeLong(-100000000000000L, buffer)
		encodeLong(1000000000000000L, buffer)
		encodeLong(-1000000000000000L, buffer)
		encodeLong(10000000000000000L, buffer)
		encodeLong(-10000000000000000L, buffer)
		encodeLong(100000000000000000L, buffer)
		encodeLong(-100000000000000000L, buffer)
		encodeLong(1000000000000000000L, buffer)
		encodeLong(-1000000000000000000L, buffer)
		encodeLong(Long.MaxValue, buffer)
		encodeLong(Long.MinValue, buffer)

		// Prepare the buffer for reading
		buffer.flip()

		// Decode the integers
		while buffer.hasRemaining do {
			val p0 = buffer.position
			val value = decodeLong(buffer)
			val length = buffer.position - p0
			val bytes = new Array[Byte](length)
			buffer.get(p0, bytes)
			println(f"${length}%2d: ${value}% 21d, ${bytes.map(byte => (f"${byte}%02X").takeRight(2)).mkString("_")}")
		}
	}


}
