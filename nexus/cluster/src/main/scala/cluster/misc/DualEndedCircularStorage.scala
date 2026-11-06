package readren.nexus
package cluster.misc

import java.util.function.Supplier

object DualEndedCircularStorage {
	inline val DEFAULT_MAX_BUFFERS = 128

	class StorageCapacityExceededException(val allocatedBuffers: Int, val maxAllowed: Int)
		extends RuntimeException(s"DualEndedCircularStorage capacity limit reached: allocated $allocatedBuffers buffers, max allowed is $maxAllowed")
}

/**
 * A circular FIFO buffer-like storage designed to help a writer write continuously without worrying about buffer bounds.
 * It automatically manages underlying buffers (e.g., `ByteBuffer`) and grows dynamically as needed.
 * The circular storage provides two ends: a **write end** for appending new data and a **read end** for consuming the oldest unread data.
 *
 * @param bufferSupplier A supplier function that provides new buffers (e.g., `ByteBuffer`).
 * @param maxBuffers The maximum number of buffers that may be allocated in this circular storage ring.
 * @tparam T The type of the buffers used to hold data in the circular storage.
 */
class DualEndedCircularStorage[T](bufferSupplier: Supplier[T], val maxBuffers: Int = DualEndedCircularStorage.DEFAULT_MAX_BUFFERS) {

	private class Node {
		val buffer: T = bufferSupplier.get
		var next: Node = this
	}

	/**
	 * The write-end points to the buffer where new data is appended.
	 * Data written here will be consumed later, potentially at a different pace, from the read end.
	 */
	@volatile private var writeEndNode: Node = new Node
	/** The read-end points to the buffer that contains the oldest data. */
	@volatile private var readEndNode: Node = writeEndNode
	private var _bufferCount: Int = 1

	/** Returns the total number of buffers currently allocated in this circular storage ring. */
	def bufferCount: Int = _bufferCount

	/**
	 * Returns the buffer at the write-end, where new information is appended.
	 * Data appended here will be consumed later, potentially at a different pace, from the read end.
	 *
	 * @return The buffer at the write-end.
	 */
	inline def writeEnd: T = writeEndNode.buffer

	/**
	 * Returns the buffer that contains the oldest unread information.
	 *
	 * @return The buffer at the read end.
	 */
	inline def readEnd: T = readEndNode.buffer

	/**
	 * Checks if there are any buffers, that are not fully read, behind the write end.
	 *
	 * @return `true` if there is a non-fully-read buffer behind the write end, or `false` otherwise.
	 */
	inline def hasPendingBuffersBehind: Boolean = readEndNode ne writeEndNode

	/**
	 * Marks the buffer at the read end as fully read and, if the read-end buffer isn't the write-end buffer, advances the read-end.
	 * Must be called to indicate that the buffer at the read end was fully read and becomes available for writing again.
	 * @return true if the `readEnd` was advanced; `false` if not advanced (because the read-end buffer is the write-end buffer).
	 */
	inline def advanceReadEnd(): Boolean = {
		if readEndNode ne writeEndNode then {
			readEndNode = readEndNode.next
			true
		} else false
	}

	/**
	 * Advances the write-end to the next buffer. If the write-end catches up to the read end, the circular storage grows by adding a new buffer in between.
	 *
	 * @return The buffer at the new write-end position.
	 */
	def advanceWriteEnd(): T = {
		val nextNode = writeEndNode.next
		if nextNode eq readEndNode then {
			if _bufferCount >= maxBuffers then throw new DualEndedCircularStorage.StorageCapacityExceededException(_bufferCount, maxBuffers)
			val newNode = new Node
			newNode.next = nextNode
			writeEndNode.next = newNode
			writeEndNode = newNode
			_bufferCount += 1
		} else writeEndNode = nextNode
		writeEndNode.buffer
	}

	/**
	 * Prunes the number of allocated buffers down to `targetCount` if the storage is currently quiescent
	 * (`readEndNode eq writeEndNode`), releasing detached buffers for garbage collection.
	 *
	 * @param targetCount the desired number of buffers to retain in the ring (must be at least 1).
	 * @return the number of buffers in the ring after pruning.
	 */
	def pruneTo(targetCount: Int): Int = {
		val effectiveTarget = math.max(1, targetCount)
		if _bufferCount > effectiveTarget && (readEndNode eq writeEndNode) then {
			var current = writeEndNode
			for _ <- 1 until effectiveTarget do {
				current = current.next
			}
			current.next = writeEndNode
			_bufferCount = effectiveTarget
		}
		_bufferCount
	}

	/**
	 * Returns an iterator over the buffers in the circular storage.
	 * The iterator starts from the read-end and traverses the buffers until the write-end inclusive.
	 *
	 * @return An iterator over the buffers in the circular storage.
	 */
	def iterator: Iterator[T] = new Iterator[T] {
		private var current = readEndNode

		override def hasNext: Boolean =
			current ne null

		override def next(): T = {
			if current eq null then throw new NoSuchElementException("No more buffers")
			val buf = current.buffer
			current = if current eq writeEndNode then null else current.next
			buf
		}
	}
}

