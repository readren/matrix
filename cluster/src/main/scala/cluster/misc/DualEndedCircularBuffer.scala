package readren.matrix
package cluster.misc

import java.util
import java.util.function.Supplier


/**
 * A circular FIFO buffer designed to help a writer write continuously without worrying about storage bounds.
 * It automatically manages underlying storage (e.g., `ByteBuffer`) and grows dynamically as needed.
 * The buffer provides two ends: a **write end** for appending new data and a **read end** for consuming the oldest unread data.
 *
 * @param storageSupplier A supplier function that provides new storage units (e.g., `ByteBuffer`).
 * @tparam T The type of storage units used to hold data in the buffer.
 */
class DualEndedCircularBuffer[T](storageSupplier: Supplier[T]) {

	private class Node {
		val storage: T = storageSupplier.get
		var next: Node = this
	}

	/**
	 * The write end points to the current storage unit where new data is appended.
	 * Data written here will be consumed later, potentially at a different pace, from the read end.
	 */
	@volatile private var writeEndNode: Node = new Node
	/** The read end points to the storage unit that contains the oldest data. */
	@volatile private var readEndNode: Node = writeEndNode

	/**
	 * Returns the storage unit at the write end, where new information is appended.
	 * Data appended here will be consumed later, potentially at a different pace, from the read end.
	 *
	 * @return The storage unit at the write end.
	 */
	inline def writeEnd: T = writeEndNode.storage

	/**
	 * Returns the storage unit that contains the oldest unread information.
	 *
	 * @return The storage unit at the read end.
	 */
	inline def readEnd: T = readEndNode.storage

	/**
	 * Checks if there are any storage units, that are not fully read, behind the write end.
	 *
	 * @return `true` if there is a non-fully-read storage unit behind the write end, or `false` otherwise.
	 */
	inline def hasPendingStoragesBehind: Boolean = readEndNode ne writeEndNode

	/**
	 * Marks the storage unit at the read end as fully read and, if the read-end storage isn't the write-end storage, advances the read-end.
	 * Must be called to indicate that the storage unit at the read end was fully read and becomes available for writing again.
	 * @return true if the `readEnd` was advanced; `false` if not advanced (because the read-end storage is the write-end storage).
	 */
	inline def advanceReadEnd(): Boolean = {
		if readEndNode ne writeEndNode then {
			readEndNode = readEndNode.next
			true
		} else false
	}

	/**
	 * Advances the write end to the next storage unit. If the write end catches up to the read end, the buffer grows by adding a new storage unit in between.
	 *
	 * @return The storage unit at the new write end position.
	 */
	def advanceWriteEnd(): T = {
		val nextNode = writeEndNode.next
		if nextNode eq readEndNode then {
			val newNode = new Node
			newNode.next = nextNode
			writeEndNode.next = newNode
			writeEndNode = newNode
		} else writeEndNode = writeEndNode.next
		writeEndNode.storage
	}

}

