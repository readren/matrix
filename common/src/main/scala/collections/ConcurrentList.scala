package readren.common.collections

import ConcurrentList.Node

@deprecated("not used")
object ConcurrentList {
	abstract class Node {
		type Self <: Node

		var next: Self | Null = null

		private[ConcurrentList] var removed: Boolean = false

		inline def isRemoved: Boolean = removed
		
		final def remove(): Unit = this.synchronized {
			removed = true
		}
	}
}

/**
 * A concurrent singly-linked list with circular navigation.
 * Constant time [[add]] and [[Node.remove]] operations.
 * @tparam A the type of the elements
 */
@deprecated("not used")
final class ConcurrentList[A <: Node {type Self = A}] { thisConcurrentList =>

	private var head: A | Null = null

	def add(freeNode: A): Unit = {
		assert((freeNode.next eq null) && !freeNode.removed)
		thisConcurrentList.synchronized {
			freeNode.next = head
			head = freeNode
		}
	}

	/**
	 * Retrieves the element that follows the given element in this list, treating the list as circular (i.e., the tail connects to the head with a `null` in between).
	 *
	 *		- If the provided `element` is `null`, the [[head]] element is returned.
	 *		- Returns `null` if the list is empty or the provided element is the last of the list.
	 *		- Handles cases where the provided `element` has been removed from the list:
	 *			- Returns the first non-removed element that followed the provided `element`.
	 *			- If no such element exists, returns `null`.
	 *
	 * Thread-safety: The method synchronizes access to both the provided `element` and its next element to ensure safe concurrent usage. The list itself is synchronized only when the [[head]] is involved (which happens when the provided element is `null` or the last of the list).
	 *
	 * Deadlock Safety: Nodes are always locked in a consistent order: the current element is locked before accessing or locking its successor (nextElem).
	 *
	 * Constant time removal at the cost of navigation overhead: The traversal logic simultaneously skips and removes logically deleted elements (isRemoved). This ensures the list structure remains clean with minimal overhead.
	 * // TODO to skip removed elements requires a nested synchronization. Surely removing the [[Node.removed]] field would improve the navigation speed at the cost a removing at O(n) instead of O(1). Consider changing this.
	 *
	 *
	 * @param element The element whose successor is to be retrieved.
	 * @return The next non-removed element after the provided `element`, or `null` if it is the last non-removed element of the list or the list is empty.
	 */
	def nextOf(element: A | Null): A | Null = {
		if element eq null then updatedHead()
		else element.synchronized {
			var nextElem = element.next
			while nextElem ne null do nextElem.synchronized {
				if nextElem.removed then {
					nextElem = nextElem.next
					element.next = nextElem
				} else return nextElem
			}
			null
		}
	}

	/** Updates and gets the [[head]] of this list. */
	private def updatedHead(): A | Null = thisConcurrentList.synchronized {
		while head ne null do head.synchronized {
			if head.removed then head = head.next
			else return head
		}
		null
	}

	inline def circularIterator: CircularIterator = new CircularIterator

	/** A never ending circular iterator over the elements of this list that restarts at the [[head]] when advancing after the last element.
	 * Instances of this class are not thread-safe. */
	final class CircularIterator {
		private var currentNode: A | Null = null

		/** @return the next element of the circularized list or `null` if the list is empty. */
		inline def next(): A | Null = {
			currentNode = nextOf(currentNode)
			if currentNode ne null then currentNode
			else updatedHead()
		}

		inline def current: A | Null = currentNode
	}
}


