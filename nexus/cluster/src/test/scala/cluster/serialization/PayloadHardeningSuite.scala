package readren.nexus
package cluster.serialization

import cluster.misc.VLQ
import cluster.serialization.CommonSerializers.given
import cluster.serialization.Deserializer.*

import java.nio.ByteBuffer
import munit.FunSuite

class PayloadHardeningSuite extends FunSuite {

	sealed trait TestEvent
	case object StartEvent extends TestEvent
	case class StopEvent(code: Int) extends TestEvent

	given Serializer[TestEvent] = Serializer.derive[TestEvent]
	given Deserializer[TestEvent] = Deserializer.derive[TestEvent]

	sealed trait TreeSum
	sealed trait NestedBranch extends TreeSum
	case object NestedLeafA extends NestedBranch
	case class NestedLeafB(data: String) extends NestedBranch
	case object OtherLeaf extends TreeSum

	given Deserializer[TreeSum] = Deserializer.derive[TreeSum](NestedSumMatchMode.TREE)

	private def createTestReader(
		buffer: ByteBuffer,
		remainingBytesOverride: Int = -1,
		limits: AllocationLimits = AllocationLimits.Default
	): Deserializer.Reader = {
		new Deserializer.Reader(allocationLimits = limits) {
			override def governingVersion: ProtocolVersion = ProtocolVersion.OF_THIS_PROJECT
			override def position: Int = buffer.position()
			override def getContentBytes(maxBytesToConsume: Int): ByteBuffer = buffer
			override def peekByte: Byte = buffer.get(buffer.position())
			override def remainingContentBytes: Int =
				if remainingBytesOverride >= 0 then remainingBytesOverride else buffer.remaining()
		}
	}

	test("ByteArray deserializer rejects length exceeding remaining content bytes") {
		val buffer = ByteBuffer.allocate(32)
		val writer = new VLQ.ByteWriter {
			override def putByte(byte: Byte): Unit = buffer.put(byte)
		}
		VLQ.encodeUnsignedInt(1024, writer) // length 1024, but only ~27 bytes remain
		buffer.flip()

		val reader = createTestReader(buffer)
		intercept[LengthExceedsRemainingBytesException] {
			summon[Deserializer[Array[Byte]]].deserialize(reader)
		}
	}

	test("String deserializer rejects length exceeding remaining content bytes") {
		val buffer = ByteBuffer.allocate(16)
		val writer = new VLQ.ByteWriter {
			override def putByte(byte: Byte): Unit = buffer.put(byte)
		}
		VLQ.encodeUnsignedInt(500, writer)
		buffer.flip()

		val reader = createTestReader(buffer)
		intercept[LengthExceedsRemainingBytesException] {
			summon[Deserializer[String]].deserialize(reader)
		}
	}

	test("Array[Int] deserializer rejects length exceeding safe allocation limit") {
		val buffer = ByteBuffer.allocate(100)
		val writer = new VLQ.ByteWriter {
			override def putByte(byte: Byte): Unit = buffer.put(byte)
		}
		VLQ.encodeUnsignedInt(50, writer)
		buffer.flip()

		// Restrict limits to 10 elements max
		val strictLimits = new AllocationLimits(maxAllocationBytes = 64, maxCollectionElements = 10)
		val reader = createTestReader(buffer, remainingBytesOverride = 1000, limits = strictLimits)

		intercept[AllocationThresholdExceededException] {
			summon[Deserializer[Array[Int]]].deserialize(reader)
		}
	}

	test("Iterable deserializer rejects count exceeding maxCollectionElements") {
		val buffer = ByteBuffer.allocate(100)
		val writer = new VLQ.ByteWriter {
			override def putByte(byte: Byte): Unit = buffer.put(byte)
		}
		VLQ.encodeUnsignedInt(25, writer)
		buffer.flip()

		val strictLimits = new AllocationLimits(maxCollectionElements = 20)
		val reader = createTestReader(buffer, remainingBytesOverride = 1000, limits = strictLimits)

		intercept[AllocationThresholdExceededException] {
			summon[Deserializer[List[Int]]].deserialize(reader)
		}
	}

	test("Map deserializer rejects negative count") {
		val buffer = ByteBuffer.allocate(32)
		val writer = new VLQ.ByteWriter {
			override def putByte(byte: Byte): Unit = buffer.put(byte)
		}
		VLQ.encodeInt(-5, writer)
		buffer.flip()

		val reader = createTestReader(buffer)
		intercept[NegativeLengthException] {
			summon[Deserializer[Map[String, Int]]].deserialize(reader)
		}
	}

	test("Macro-derived sum type throws InvalidDiscriminatorException on unknown tag") {
		val buffer = ByteBuffer.allocate(32)
		val writer = new VLQ.ByteWriter {
			override def putByte(byte: Byte): Unit = buffer.put(byte)
		}
		VLQ.encodeUnsignedInt(99, writer) // Unknown discriminator tag
		buffer.flip()

		val reader = createTestReader(buffer)
		val ex = intercept[InvalidDiscriminatorException] {
			summon[Deserializer[TestEvent]].deserialize(reader)
		}
		assertEquals(ex.discriminator, 99)
	}

	test("Reader.read rejects invalid reference key out of bounds") {
		val buffer = ByteBuffer.allocate(32)
		val writer = new VLQ.ByteWriter {
			override def putByte(byte: Byte): Unit = buffer.put(byte)
		}
		buffer.put(Serializer.BACK_REFERENCE_HEADER)
		VLQ.encodeInt(42, writer) // Table is empty, key 42 is invalid
		buffer.flip()

		val reader = createTestReader(buffer)
		intercept[InvalidReferenceKeyException] {
			reader.read[String]
		}
	}

	test("Reader.readBytes rejects length exceeding remaining bytes") {
		val buffer = ByteBuffer.allocate(10)
		val reader = createTestReader(buffer)
		intercept[LengthExceedsRemainingBytesException] {
			reader.readBytes(50)
		}
	}

	test("TREE-mode sum type throws InvalidDiscriminatorException on unknown nested tag") {
		val buffer = ByteBuffer.allocate(32)
		val writer = new VLQ.ByteWriter {
			override def putByte(byte: Byte): Unit = buffer.put(byte)
		}
		VLQ.encodeUnsignedInt(0, writer)  // branch 0 -> NestedBranch
		VLQ.encodeUnsignedInt(88, writer) // unknown nested discriminator in NestedBranch
		buffer.flip()

		val reader = createTestReader(buffer)
		val ex = intercept[InvalidDiscriminatorException] {
			summon[Deserializer[TreeSum]].deserialize(reader)
		}
		assertEquals(ex.discriminator, 88)
	}

	test("Zero-wire-footprint collection rejects count exceeding maxCollectionElements") {
		val buffer = ByteBuffer.allocate(32)
		val writer = new VLQ.ByteWriter {
			override def putByte(byte: Byte): Unit = buffer.put(byte)
		}
		VLQ.encodeUnsignedInt(150, writer) // 150 elements of 0-byte wire size
		buffer.flip()

		val strictLimits = new AllocationLimits(maxCollectionElements = 100)
		val reader = createTestReader(buffer, remainingBytesOverride = 0, limits = strictLimits)
		intercept[AllocationThresholdExceededException] {
			summon[Deserializer[List[EmptyTuple]]].deserialize(reader)
		}
	}

	test("Receiver constructor rejects aggregate buffer capacity exceeding Int.MaxValue") {
		intercept[IllegalArgumentException] {
			new cluster.channel.Receiver(null, null, buffersCapacity = 1024 * 1024, maxBuffers = 3000)
		}
	}

	test("DelegateConfig rejects aggregate buffer capacity exceeding Int.MaxValue") {
		intercept[IllegalArgumentException] {
			new cluster.service.ParticipantService.DelegateConfig(frameCapacity = 1024 * 1024, maxBuffers = 3000)
		}
	}
}
