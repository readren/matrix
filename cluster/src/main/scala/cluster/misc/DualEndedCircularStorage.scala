package readren.matrix
package cluster.misc

import java.util
import java.util.function.Supplier


/**
 * A circular FIFO buffer-like storage designed to help a writer write continuously without worrying about buffer bounds.
 * It automatically manages underlying buffers (e.g., `ByteBuffer`) and grows dynamically as needed.
 * The circular storage provides two ends: a **write end** for appending new data and a **read end** for consuming the oldest unread data.
 *
 * @param bufferSupplier A supplier function that provides new buffers (e.g., `ByteBuffer`).
 * @tparam T The type of the buffers used to hold data in the circular storage.
 */
class DualEndedCircularStorage[T](bufferSupplier: Supplier[T]) {

	private class Node {
		val buffer: T = bufferSupplier.get
		var next: Node = this
	}

	/**
	 * The write end points to the current buffer unit where new data is appended.
	 * Data written here will be consumed later, potentially at a different pace, from the read end.
	 */
	@volatile private var writeEndNode: Node = new Node
	/** The read end points to the buffer that contains the oldest data. */
	@volatile private var readEndNode: Node = writeEndNode

	/**
	 * Returns the buffer at the write end, where new information is appended.
	 * Data appended here will be consumed later, potentially at a different pace, from the read end.
	 *
	 * @return The buffer at the write end.
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
	 * Advances the write end to the next buffer. If the write end catches up to the read end, the circular storage grows by adding a new buffer in between.
	 *
	 * @return The buffer at the new write end position.
	 */
	def advanceWriteEnd(): T = {
		val nextNode = writeEndNode.next
		if nextNode eq readEndNode then {
			val newNode = new Node
			newNode.next = nextNode
			writeEndNode.next = newNode
			writeEndNode = newNode
		} else writeEndNode = writeEndNode.next
		writeEndNode.buffer
	}

}

