package readren.matrix
package msgbuffers

import readren.taskflow.Maybe

import java.net.URI
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.AbstractIterator

/**
 * @param owner the [[Reactant]] that owns this [[Inbox]] */
class ConcurrentUnboundedFifo[M](owner: Reactant[M]) extends Receiver[M], Inbox[M] {
	private val queue = new ConcurrentLinkedQueue[M]()

	/** This mark is necessary because [[ConcurrentLinkedQueue.isEmpty]] may return true after calling [[ConcurrentLinkedQueue.offer]], and we need to be sure if a message maybe pending. */
	@volatile private var aMsgWasSubmitted: Boolean = false

	override val uri: URI = {
		val mu = owner.admin.matrix.uri
		URI(mu.getScheme, null, mu.getHost, mu.getPort, mu.getPath + owner.path, null, null)
	}

	override def submit(message: M): Unit = {
		aMsgWasSubmitted = true
		queue.offer(message)
		owner.thereIsAPendingMsg()
	}

	override def withdraw(): Maybe[M] = {
		aMsgWasSubmitted = false
		Maybe.apply(queue.poll())
	}

	override def maybeNonEmpty: Boolean = {
		if aMsgWasSubmitted then {
			aMsgWasSubmitted = false
			true
		} else !queue.isEmpty 
	}

	override def size: Int = {
		queue.size()
	}
	
	override def iterator: Iterator[M] = new AbstractIterator[M] {
		private val javaIterator = queue.iterator()

		override def hasNext: Boolean = javaIterator.hasNext

		override def next(): M = javaIterator.next()
	}
}
