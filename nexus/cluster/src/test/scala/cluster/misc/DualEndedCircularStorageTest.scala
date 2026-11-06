package readren.nexus
package cluster.misc

import munit.FunSuite
import java.nio.ByteBuffer

class DualEndedCircularStorageTest extends FunSuite {

	test("initial state has one buffer and matching read and write ends") {
		val storage = new DualEndedCircularStorage[ByteBuffer](() => ByteBuffer.allocate(100), maxBuffers = 4)
		assertEquals(storage.bufferCount, 1)
		assert(!storage.hasPendingBuffersBehind)
		assert(storage.readEnd eq storage.writeEnd)
	}

	test("advancing write end allocates new buffers up to maxBuffers") {
		val storage = new DualEndedCircularStorage[ByteBuffer](() => ByteBuffer.allocate(100), maxBuffers = 3)
		assertEquals(storage.bufferCount, 1)

		val b2 = storage.advanceWriteEnd()
		assertEquals(storage.bufferCount, 2)
		assert(storage.hasPendingBuffersBehind)

		val b3 = storage.advanceWriteEnd()
		assertEquals(storage.bufferCount, 3)

		intercept[DualEndedCircularStorage.StorageCapacityExceededException] {
			storage.advanceWriteEnd()
		}
		assertEquals(storage.bufferCount, 3)
	}

	test("advancing read end frees buffers for write reuse without exceeding maxBuffers") {
		val storage = new DualEndedCircularStorage[ByteBuffer](() => ByteBuffer.allocate(100), maxBuffers = 2)
		val b1 = storage.writeEnd
		val b2 = storage.advanceWriteEnd()
		assertEquals(storage.bufferCount, 2)
		assert(storage.hasPendingBuffersBehind)

		// advancing readEnd moves read pointer to b2
		assert(storage.advanceReadEnd())
		assert(!storage.hasPendingBuffersBehind)
		assert(storage.readEnd eq b2)

		// advanceWriteEnd now loops to b1 instead of allocating because b1 was consumed
		val reused = storage.advanceWriteEnd()
		assert(reused eq b1)
		assertEquals(storage.bufferCount, 2)
	}

	test("pruneTo reduces buffer count when quiescent") {
		val storage = new DualEndedCircularStorage[ByteBuffer](() => ByteBuffer.allocate(100), maxBuffers = 6)
		storage.advanceWriteEnd()
		storage.advanceWriteEnd()
		storage.advanceWriteEnd()
		assertEquals(storage.bufferCount, 4)

		// While in-flight buffers exist (readEnd ne writeEnd), pruning must be a no-op
		assertEquals(storage.pruneTo(2), 4)

		// Consume all buffers so readEnd catches up to writeEnd
		while storage.advanceReadEnd() do ()
		assert(!storage.hasPendingBuffersBehind)

		// Quiescent: pruneTo(2) should unlink excess buffers down to 2
		val prunedCount = storage.pruneTo(2)
		assertEquals(prunedCount, 2)
		assertEquals(storage.bufferCount, 2)

		// Ring must remain fully circular and valid
		val b1 = storage.writeEnd
		val b2 = storage.advanceWriteEnd()
		assert(b1 ne b2)
		// Catching up to readEnd: third advance should allocate again up to maxBuffers
		val b3 = storage.advanceWriteEnd()
		assertEquals(storage.bufferCount, 3)
	}

	test("pruneTo enforces a minimum floor of 1") {
		val storage = new DualEndedCircularStorage[ByteBuffer](() => ByteBuffer.allocate(100), maxBuffers = 4)
		storage.advanceWriteEnd()
		assertEquals(storage.bufferCount, 2)
		assert(storage.advanceReadEnd())

		assertEquals(storage.pruneTo(0), 1)
		assertEquals(storage.bufferCount, 1)
		assert(storage.readEnd eq storage.writeEnd)
	}
}
