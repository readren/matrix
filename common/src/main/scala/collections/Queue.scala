package readren.common
package collections

import scala.annotation.nowarn
import scala.collection.AbstractIterator


@deprecated("not used")
object Queue {
	trait Node {
		type Self <: Node

		private[Queue] var next: Self | Null = null
	}
}

/** A queue that saves allocations by requiring the elements to extend the [[Node]] trait.
 * @see [[ConcurrentQueue]] for a thread-save version */
@deprecated("not used")
class Queue[N <: Queue.Node {type Self = N}] extends Queue.Node {
	type Self = N

	private var head: N | Null = null
	private var tail: N | Null = null

	def enqueue(node: N): Unit = {
		assert(node.next eq null)
		val t = tail
		if t eq null then
			assert(head eq null)
			head = node
			tail = node
		else
			t.next = node
			tail = node
	}

	def dequeue(): N | Null = {
		val node = head
		if node ne null then {
			head = node.next
			node.next = null
			if head eq null then tail = null
		}
		node
	}

	inline def nextOf(a: N): N | Null = a.next

	def iterator: Iterator[N] = new AbstractIterator[N] {
		private var nextA: N | Null = head
		
		override def hasNext: Boolean = nextA ne null

		override def next(): N = {
			val n = nextA
			if n eq null then throw new NoSuchElementException()
			else {
				nextA = n.next
				n.nn
			}
		}
	}
}
