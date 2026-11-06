package readren.nexus
package cluster.serialization

import cluster.serialization.CommonSerializers.given
import cluster.serialization.InvariantIterableFactory.given
import munit.FunSuite
import readren.common.Maybe

import java.nio.ByteBuffer
import scala.collection.mutable.ListBuffer

class TupleSerializationSuite extends FunSuite {

	final val DEFAULT_VERSION = ProtocolVersion(1)

	private def roundTrip[T](
		value: T,
		bufferCapacity: Int = 1024
	)(using serializer: Serializer[T], deserializer: Deserializer[T]): (T, Array[Byte]) = {
		val buffer = ByteBuffer.allocate(bufferCapacity)
		val writer = new Serializer.Writer {
			override val governingVersion: ProtocolVersion = DEFAULT_VERSION
			override def getBuffer(minimumRemaining: Int): ByteBuffer = buffer
			override def position: Int = buffer.position()
		}
		val reader = new Deserializer.Reader {
			override val governingVersion: ProtocolVersion = DEFAULT_VERSION
			override def position: Int = buffer.position()
			override def getContentBytes(maxBytesToConsume: Int): ByteBuffer = buffer
			override def peekByte: Byte = buffer.get(buffer.position())
		}

		serializer.serialize(value, writer)
		buffer.flip()
		val writtenBytes = new Array[Byte](buffer.remaining())
		buffer.duplicate().get(writtenBytes)
		val decoded = deserializer.deserialize(reader)
		assertEquals(buffer.remaining(), 0)
		(decoded, writtenBytes)
	}

	test("EmptyTuple produces 0 bytes and round-trips correctly") {
		val (decoded, bytes) = roundTrip(EmptyTuple)
		assertEquals(bytes.length, 0)
		assertEquals(decoded, EmptyTuple)
	}

	test("Single element tuple (1-tuple)") {
		val single = Tuple1(42)
		val (decoded, bytes) = roundTrip(single)
		assertEquals(decoded, single)
		assertEquals(bytes.length, 4) // 4 bytes for Int
	}

	test("Standard 2-tuple syntax") {
		val pair = (100, "hello")
		val (decoded, _) = roundTrip(pair)
		assertEquals(decoded, pair)
	}

	test("Heterogeneous multi-element tuple with product wire format (no discriminators)") {
		val tuple = (42: Int, "matrix": String, true: Boolean, 9999999999L: Long, 3.14159: Double)
		val (decoded, bytes) = roundTrip(tuple)
		assertEquals(decoded, tuple)

		// Calculate expected payload size: Int (4) + String (VLQ len 6 + 6 bytes UTF-8) + Boolean (1) + Long (8) + Double (8)
		val stringBytes = "matrix".getBytes("UTF-8")
		val expectedSize = 4 + (1 + stringBytes.length) + 1 + 8 + 8
		assertEquals(bytes.length, expectedSize)
	}

	test("Arbitrary arity tuple beyond 22 elements (arity 25)") {
		val bigTuple = (
			1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
			11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
			21, 22, 23, 24, "tail-value"
		)
		val (decoded, _) = roundTrip(bigTuple)
		assertEquals(decoded, bigTuple)
	}

	test("Strict left-to-right deserialization evaluation order") {
		val readLog = ListBuffer[String]()

		case class First(value: Int)
		case class Second(value: Int)
		case class Third(value: Int)

		given Serializer[First] = (msg, writer) => writer.putInt(msg.value)
		given Serializer[Second] = (msg, writer) => writer.putInt(msg.value)
		given Serializer[Third] = (msg, writer) => writer.putInt(msg.value)

		given Deserializer[First] = reader => {
			readLog += "first"
			First(reader.readInt())
		}
		given Deserializer[Second] = reader => {
			readLog += "second"
			Second(reader.readInt())
		}
		given Deserializer[Third] = reader => {
			readLog += "third"
			Third(reader.readInt())
		}

		val original = (First(1), Second(2), Third(3))

		val buffer = ByteBuffer.allocate(64)
		val writer = new Serializer.Writer {
			override val governingVersion: ProtocolVersion = DEFAULT_VERSION
			override def getBuffer(minimumRemaining: Int): ByteBuffer = buffer
			override def position: Int = buffer.position()
		}
		val reader = new Deserializer.Reader {
			override val governingVersion: ProtocolVersion = DEFAULT_VERSION
			override def position: Int = buffer.position()
			override def getContentBytes(maxBytesToConsume: Int): ByteBuffer = buffer
			override def peekByte: Byte = buffer.get(buffer.position())
		}

		summon[Serializer[(First, Second, Third)]].serialize(original, writer)
		buffer.flip()

		val decoded = summon[Deserializer[(First, Second, Third)]].deserialize(reader)
		assertEquals(readLog.toList, List("first", "second", "third"))
		assertEquals(decoded._1.value, 1)
		assertEquals(decoded._2.value, 2)
		assertEquals(decoded._3.value, 3)
	}

	test("Nested composite types inside tuples") {
		case class User(name: String, age: Int)
		given Serializer[User] = Serializer.derive[User]
		given Deserializer[User] = Deserializer.derive[User]

		val nested: ((Int, String), User, Maybe[String], List[Int]) = (
			(1, "nested-first"),
			User("Alice", 30),
			Maybe.some("present"),
			List(10, 20, 30)
		)

		val (decoded, _) = roundTrip(nested)
		assertEquals(decoded._1, nested._1)
		assertEquals(decoded._2, nested._2)
		assertEquals(decoded._3, nested._3)
		assertEquals(decoded._4, nested._4)
	}

	test("Case class containing tuple fields derived via Serializer.derive and Deserializer.derive") {
		case class Telemetry(
			id: Long,
			coordinates: (Int, Int, Int),
			metadata: (String, Boolean)
		)

		given Serializer[Telemetry] = Serializer.derive[Telemetry]
		given Deserializer[Telemetry] = Deserializer.derive[Telemetry]

		val telemetry = Telemetry(999L, (10, 20, 30), ("active", true))
		val (decoded, _) = roundTrip(telemetry)
		assertEquals(decoded, telemetry)
	}

	test("Explicit derivation via Serializer.derive and Deserializer.derive on tuple types") {
		given Serializer[(Int, String)] = Serializer.derive[(Int, String)]
		given Deserializer[(Int, String)] = Deserializer.derive[(Int, String)]

		val sample = (77, "derived-tuple")
		val (decoded, _) = roundTrip(sample)
		assertEquals(decoded, sample)
	}

}
