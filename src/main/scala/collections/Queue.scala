package readren.matrix
package collections

import scala.annotation.nowarn
import scala.collection.AbstractIterator

object Queue {
	abstract class Node {
		type Self <: Node

		private[Queue] var next: Self | Null = null
	}
}
class Queue[A <: Queue.Node { type Self = A }] extends Queue.Node {
	type Self = A

	private var head: A | Null = null
	private var tail: A | Null = null
	
	def enqueue(a: A): Unit = {
		assert(a.next == null)
		if tail == null then {
			head = a
			tail = a
		} else {	
			tail.next = a
			tail = a
		} 
	}
	
	def dequeue(): A | Null = {
		val a = head
		if a != null then  {
			a.next = null
			head = a.next
			if head == null then tail = null
		}
		a
	}
	
	inline def nextOf(a: A): A | Null = a.next
	
	def iterator: Iterator[A] = new AbstractIterator[A] {
		private var nextA: A | Null = head 
		
		override def hasNext: Boolean = nextA != null

		override def next(): A = {
			nextA match {
				case null =>
					throw new NoSuchElementException()
				case a: A @nowarn =>
					nextA = a.next
					a
			}
		}
	}
}
