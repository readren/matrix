package readren.sequencer
package providers


import providers.MinHeapPriorityQueue.Element

import java.util
import scala.reflect.ClassTag

object MinHeapPriorityQueue {

	/** */
	trait Element {
		var scheduledTime: MilliTime = 0L
		/** The index of this instance in the array-based min-heap.
		 * Only accessed within the scheduling thread. */
		var heapIndex: Int = -1
	}
}


/**
 * A min-heap based priority queue specialized for scheduling time measured in milliseconds.
 * The specialization avoids the boxing of the value on which the sort criteria is based.
 * @tparam E the type of the [[Element]] instances that this queue accepts.           
 */
class MinHeapPriorityQueue[E <: Element](initialCapacity: Int = 16)(using ctP: ClassTag[E | Null]) {
	private var heap: Array[E | Null] = new Array(initialCapacity)
	private var heapSize: Int = 0

	inline def size: Int = heapSize

	inline def peek: E | Null = heap(0)

	/** Adds the provided element to this min-heap based priority queue. */
	def add(element: E): Unit = {
		val holeIndex = heapSize
		if holeIndex >= heap.length then grow()
		heapSize = holeIndex + 1
		if holeIndex == 0 then {
			heap(0) = element
			element.heapIndex = 0
		}
		else siftUp(holeIndex, element)
	}

	/**
	 * Polls the element that was peeked: replaces first element with last and sifts it down.
	 * Assumes the provided element is the one returned by the last call to [[peek]].
	 * @param peekedElement the [[ScheduleImpl]] to remove and return.
	 */
	def finishPoll(peekedElement: E): Unit = {
		heapSize -= 1
		val s = heapSize
		val replacement = heap(s).asInstanceOf[E]
		heap(s) = null
		if s != 0 then siftDown(0, replacement)
		peekedElement.heapIndex = -1
	}

	/** Removes the provided element from this queue.
	 * @return true if the element was removed; false if it is not contained by this queue. */
	def remove(element: E): Boolean = {
		val elemIndex = indexOf(element)
		if elemIndex < 0 then return false
		element.heapIndex = -1
		heapSize -= 1
		val s = heapSize
		val replacement = heap(s).asInstanceOf[E]
		heap(s) = null
		if s != elemIndex then {
			siftDown(elemIndex, replacement)
			if heap(elemIndex) eq replacement then siftUp(elemIndex, replacement)
		}
		true
	}

	inline def indexOf(element: E): Int = element.heapIndex

	inline def apply(index: Int): E | Null = heap(index)

	def clear(): Unit = {
		var index = heapSize
		while index > 0 do {
			index -= 1
			heap(index) = null
		}
		heapSize = 0
	}

	/**
	 * Replaces the element at position `holeIndex` of the heap-based array with the `providedElement` and rearranges it and its parents as necessary to ensure that all parents are less than or equal to their children.
	 * Note that for the entire heap to satisfy the min-heap property, the `providedElement` must be less than or equal to the children of `holeIndex`.
	 * Sifts element added at bottom up to its heap-ordered spot.
	 */
	private def siftUp(holeIndex: Int, providedElement: E): Unit = {
		var gapIndex = holeIndex
		var zero = 0
		while gapIndex > zero do {
			val parentIndex = (gapIndex - 1) >>> 1
			val parent = heap(parentIndex)
			if providedElement.scheduledTime >= parent.scheduledTime then zero = Int.MaxValue
			else {
				heap(gapIndex) = parent
				parent.heapIndex = gapIndex
				gapIndex = parentIndex
			}
		}
		heap(gapIndex) = providedElement
		providedElement.heapIndex = gapIndex
	}

	/**
	 * Replaces the element that is currently at position `holeIndex` of the heap-based array with the `providedElement` and rearranges the elements in the subtree rooted at `holeIndex` such that the subtree conform to the min-heap property.
	 * Sifts element added at top down to its heap-ordered spot.
	 */
	private def siftDown(holeIndex: Int, providedElement: E): Unit = {
		var gapIndex = holeIndex
		var half = heapSize >>> 1
		while gapIndex < half do {
			var childIndex = (gapIndex << 1) + 1
			var child = heap(childIndex)
			val rightIndex = childIndex + 1
			if rightIndex < heapSize && child.scheduledTime > heap(rightIndex).scheduledTime then {
				childIndex = rightIndex
				child = heap(childIndex)
			}
			if providedElement.scheduledTime <= child.scheduledTime then half = 0
			else {
				heap(gapIndex) = child
				child.heapIndex = gapIndex
				gapIndex = childIndex
			}
		}
		heap(gapIndex) = providedElement
		providedElement.heapIndex = gapIndex
	}

	/**
	 * Resizes the heap array.
	 */
	private def grow(): Unit = {
		val oldCapacity = heap.length
		var newCapacity = oldCapacity + (oldCapacity >> 1) // grow 50%

		if newCapacity < 0 then newCapacity = Integer.MAX_VALUE // overflow

		heap = util.Arrays.copyOf[E | Null](heap, newCapacity)
	}
}
