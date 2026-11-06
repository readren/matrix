package readren.nexus
package cluster.serialization

import cluster.serialization.CommonSerializers.given
import cluster.serialization.Deserializer.Reader
import cluster.serialization.Serializer.Writer
import cluster.serialization.ValueOrReferenceTest.given

import java.nio.ByteBuffer
import munit.FunSuite

class ReferenceDeduplicationSuite extends FunSuite {

	case class Item(name: String) derives CanEqual

	given itemSerializer: Serializer[Item] = (item: Item, writer: Writer) => {
		val bytes = item.name.getBytes(java.nio.charset.StandardCharsets.UTF_8)
		writer.putUnsignedIntVlq(bytes.length)
		writer.putBytes(bytes)
	}

	given itemDeserializer: Deserializer[Item] = (reader: Reader) => {
		val len = reader.readUnsignedIntVlq()
		val bytes = reader.readBytes(len)
		Item(new String(bytes, java.nio.charset.StandardCharsets.UTF_8))
	}

	case class RawBytePayload(header: Byte, value: Int) derives CanEqual

	given rawBytePayloadSerializer: Serializer[RawBytePayload] = (payload: RawBytePayload, writer: Writer) => {
		writer.putByte(payload.header)
		writer.putInt(payload.value)
	}

	given rawBytePayloadDeserializer: Deserializer[RawBytePayload] = (reader: Reader) => {
		val header = reader.readByte()
		val value = reader.readInt()
		RawBytePayload(header, value)
	}

	case object SingletonEntity derives CanEqual

	given singletonSerializer: Serializer[SingletonEntity.type] = (message: SingletonEntity.type, writer: Writer) => ()

	given singletonDeserializer: Deserializer[SingletonEntity.type] = (reader: Reader) => SingletonEntity

	private def createWriterAndReader(bufferCapacity: Int = 8192): (ByteBuffer, Writer, Reader) = {
		val buffer = ByteBuffer.allocate(bufferCapacity)
		val writer = new Writer {
			override val governingVersion: ProtocolVersion = ProtocolVersion(1)
			override def getBuffer(minimumRemaining: Int): ByteBuffer = buffer
			override def position: Int = buffer.position()
		}
		val reader = new Reader {
			override val governingVersion: ProtocolVersion = ProtocolVersion(1)
			override def position: Int = buffer.position()
			override def getContentBytes(maxBytesToConsume: Int): ByteBuffer = {
				if buffer.remaining() < 1 then throw new Deserializer.DeserializationException(buffer.position(), "Unexpected end of buffer")
				buffer
			}
			override def peekByte: Byte = {
				if buffer.remaining() < 1 then throw new Deserializer.DeserializationException(buffer.position(), "Unexpected end of buffer")
				buffer.get(buffer.position())
			}
		}
		(buffer, writer, reader)
	}

	test("Shared reference deduplication preserves object identity and succeeds") {
		val (buffer, writer, reader) = createWriterAndReader()
		val shared = Item("shared-item")

		writer.write(shared)
		writer.write(shared)

		buffer.flip()

		val first = reader.read[Item]
		val second = reader.read[Item]

		assertEquals(first, shared)
		assertEquals(second, shared)
		assert(first eq second, "Expected deserialized instances to share object identity")
	}

	test("Back-reference encoding does not collide with 0x40 when refKey reaches 64") {
		val (buffer, writer, reader) = createWriterAndReader()
		val items = (0 to 64).map(i => Item(s"item-$i")).toList

		// Write 65 distinct items (refKey 0 to 64 in 0-based, or 1 to 65 in 1-based)
		for item <- items do {
			writer.write(item)
		}

		// Re-write the item whose refKey is 64
		// In 1-based indexing, the 64th item (index 63 in 0-indexed list) has refKey = 64
		// In 0-based indexing, the 65th item (index 64 in 0-indexed list) has refKey = 64
		val targetItem = items(64) // refKey 64 under 0-based indexing
		writer.write(targetItem)

		buffer.flip()

		// Read all 65 original items
		val readItems = items.indices.map { i =>
			val readItem = reader.read[Item]
			assertEquals(readItem, items(i))
			readItem
		}

		// Read the repeated item
		val repeated = reader.read[Item]
		assertEquals(repeated, targetItem)
		assert(repeated eq readItems(64), "Expected repeated item to match the deserialized instance identity")
	}

	test("Reading value types must not pollute or increment reference deduplication table") {
		val (buffer, writer, reader) = createWriterAndReader()
		val item = Item("first-item")

		writer.write(item)
		// Int value written directly
		writer.putInt(42)
		// Repeat reference
		writer.write(item)

		buffer.flip()

		val readItem1 = reader.read[Item]
		assertEquals(readItem1, item)

		// Reading value type via reader.read
		val readVal = reader.read[Int]
		assertEquals(readVal, 42)

		// Reading repeated reference
		val readItem2 = reader.read[Item]
		assertEquals(readItem2, item)
		assert(readItem1 eq readItem2, "Expected repeated reference to resolve to identical object")
	}

	test("Leading byte equal to 0x40 is escaped and correctly deserialized") {
		val (buffer, writer, reader) = createWriterAndReader()
		val payload = RawBytePayload(Serializer.BACK_REFERENCE_HEADER, 12345)

		writer.write(payload)
		writer.write(payload)

		buffer.flip()

		val read1 = reader.read[RawBytePayload]
		val read2 = reader.read[RawBytePayload]

		assertEquals(read1, payload)
		assertEquals(read2, payload)
		assert(read1 eq read2, "Expected identity match for escaped payload")
	}

	test("RecyclableIdentityIntMap: putIfAbsent, object identity, growth, and reset") {
		val map = new RecyclableIdentityIntMap(initialCapacity = 16)
		assertEquals(map.size, 0)
		assert(map.isEmpty)

		val a1 = new String("dup")
		val a2 = new String("dup")
		assert(a1 ne a2)

		assertEquals(map.putIfAbsent(a1, 100), 100)
		assertEquals(map.putIfAbsent(a1, 999), 100) // already present
		assertEquals(map.size, 1)

		assertEquals(map.putIfAbsent(a2, 200), 200) // distinct object identity
		assertEquals(map.size, 2)

		// Test expansion across threshold
		val extraKeys = (1 to 100).map(_ => new Object()).toArray
		for i <- extraKeys.indices do {
			assertEquals(map.putIfAbsent(extraKeys(i), i + 1000), i + 1000)
		}
		assertEquals(map.size, 102)

		// Verify retrieval of early keys after rehash
		assertEquals(map.putIfAbsent(a1, -1), 100)
		assertEquals(map.putIfAbsent(a2, -1), 200)

		// In-place reset
		map.reset()
		assertEquals(map.size, 0)
		assert(map.isEmpty)

		// Re-populate without reallocating
		assertEquals(map.putIfAbsent(a1, 55), 55)
		assertEquals(map.size, 1)
	}

	test("RecyclableObjectBuffer: append, get, growth, and reset") {
		val buf = new RecyclableObjectBuffer(initialCapacity = 16)
		assertEquals(buf.size, 0)
		assert(buf.isEmpty)

		val objs = (0 until 100).map(i => s"obj-$i").toArray
		for i <- objs.indices do {
			val idx = buf.add(objs(i))
			assertEquals(idx, i)
		}
		assertEquals(buf.size, 100)

		for i <- objs.indices do {
			assert(buf(i) eq objs(i))
		}

		// In-place reset
		buf.reset()
		assertEquals(buf.size, 0)
		assert(buf.isEmpty)

		// Out of bounds after reset
		intercept[IndexOutOfBoundsException] {
			buf(0)
		}

		// Re-append
		val newObj = "fresh"
		val idx0 = buf.add(newObj)
		assertEquals(idx0, 0)
		assert(buf(0) eq newObj)
	}

	test("Deduplication table reset across multiple sequential messages reuses allocated tables") {
		val sharedMap = new RecyclableIdentityIntMap(32)
		val sharedBuf = new RecyclableObjectBuffer(32)

		val buffer = ByteBuffer.allocate(4096)
		val writer = new Writer(sharedMap) {
			override val governingVersion: ProtocolVersion = ProtocolVersion(1)
			override def getBuffer(minimumRemaining: Int): ByteBuffer = buffer
			override def position: Int = buffer.position()
		}
		val reader = new Reader(sharedBuf) {
			override val governingVersion: ProtocolVersion = ProtocolVersion(1)
			override def position: Int = buffer.position()
			override def getContentBytes(maxBytesToConsume: Int): ByteBuffer = {
				if buffer.remaining() < 1 then throw new Deserializer.DeserializationException(buffer.position(), "Unexpected end of buffer")
				buffer
			}
			override def peekByte: Byte = {
				if buffer.remaining() < 1 then throw new Deserializer.DeserializationException(buffer.position(), "Unexpected end of buffer")
				buffer.get(buffer.position())
			}
		}

		val itemA = Item("msgA-item")
		val itemB = Item("msgB-item")

		// Message 1
		writer.resetDeduplication()
		reader.resetDeduplication()
		buffer.clear()

		writer.write(itemA)
		writer.write(itemA)
		buffer.flip()

		val readA1 = reader.read[Item]
		val readA2 = reader.read[Item]
		assertEquals(readA1, itemA)
		assert(readA1 eq readA2)

		// Message 2 on same writer/reader with reset
		writer.resetDeduplication()
		reader.resetDeduplication()
		buffer.clear()

		writer.write(itemB)
		writer.write(itemB)
		buffer.flip()

		val readB1 = reader.read[Item]
		val readB2 = reader.read[Item]
		assertEquals(readB1, itemB)
		assert(readB1 eq readB2)
	}

}
