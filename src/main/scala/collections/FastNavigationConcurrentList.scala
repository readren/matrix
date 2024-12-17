package readren.matrix
package collections

import collections.FastNavigationConcurrentList.Node

object FastNavigationConcurrentList {
	abstract class Node {
		type Self <: Node

		var next: Self | Null = null

	}
}


/**
 * A concurrent singly-linked list with circular navigation (see problem below).
 * Constant time [[add]].
 * Faster navigation than [[ConcurrentList]] but when the current element is concurrently removed all the following ones are skipped.
 * Slow remove.
 * This clas was created as an alternative to [[ConcurrentList]] with improved navigation speed, but it is not being used because of the "skips elements when the current element is concurrently removed" problem that affect both [[nextOf]] and [[CircularIterator]]   
 * @tparam A the type of the elements
 */
final class FastNavigationConcurrentList[A <: Node {type Self = A}] { thisConcurrentList =>

	private var head: A | Null = null

	def add(freeNode: A): Unit = {
		assert(freeNode.next == null)
		thisConcurrentList.synchronized {
			freeNode.next = head
			head = freeNode
		}
	}

	/**
	 * Retrieves the element that follows the given element in this list, treating the list as circular (i.e., the tail connects to the head with a `null` in between).
	 *
	 *		- If the provided `element` is `null`, the [[head]] element is returned.
	 *		- Returns `null` if the list is empty or the provided element was removed or is the last of the list.
	 *
	 * Thread-safety: The method synchronizes access to both the provided `element` and its next element to ensure safe concurrent usage. The list itself is synchronized only when the [[head]] is involved (which happens when the provided element is `null` or the last of the list).
	 *
	 * Deadlock Safety: No nested synchronizations.
	 *
	 * Constant time removal at the cost of navigation overhead: The traversal logic simultaneously skips and removes logically deleted elements (isRemoved). This ensures the list structure remains clean with minimal overhead.
	 * // TODO This method skips removed elements which requires nested synchronizations. Surely removing the [[Node.isRemoved]] field would improve the navigation speed at the cost of a much slower removal. Consider changing this.
	 *
	 *
	 * @param element The element whose successor is to be retrieved.
	 * @return The next non-removed element after the provided `element`, or `null` if it is the last non-removed element of the list or the list is empty.
	 */
	def nextOf(element: A | Null): A | Null = {
		if element == null then getHead
		else element.synchronized {
			element.next
		}
	}

	/** Gets the [[head]] of this list. */
	private def getHead: A | Null = {
		thisConcurrentList.synchronized(head)
	}

	/** Removes the provided element from this list.
	 * Supports concurrency but is not efficient when successive elements are removed simultaneously.
	 * 
	 * Deadlock Safety: This method nests two synchronizations in consistent order: an element and then its successor, or this [[FastNavigationConcurrentList]] instance and then the head element. */
	def remove(element: A): Boolean = {
		var previousNode: A | Null = null
		var currentNode = getHead
		while currentNode != null do {
			var haveToRestart = false
			if currentNode eq element then {
				if previousNode == null then {
					thisConcurrentList.synchronized {
						if head eq element then {
							head.synchronized {
								element.next = null
								head = head.next
							}
							return true
						}
					}
					haveToRestart = true
				} else {
					previousNode.synchronized {
						if previousNode.next eq element then {
							element.synchronized {
								previousNode.next = element.next
								element.next = null
							}
							return true
						}
					}
					haveToRestart = true
				}
			}
			if haveToRestart then {
				previousNode = null
				currentNode = getHead

			} else {
				previousNode = currentNode
				currentNode = currentNode.synchronized(currentNode.next)
			}
		}
		false
	}

	inline def circularIterator: CircularIterator = new CircularIterator

	/** A never ending circular iterator over the elements of this list that restarts at the [[head]] when advancing after the last element.
	 * Instances of this class are not thread-safe. */
	final class CircularIterator {
		private var currentNode: A | Null = null

		/** @return the next element of the circularized list or `null` if the list is empty. */
		inline def next(): A | Null = {
			currentNode = nextOf(currentNode)
			if currentNode != null then currentNode
			else getHead
		}

		inline def current: A | Null = currentNode
	}
}


