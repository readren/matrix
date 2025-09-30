package readren.matrix
package msgbuffers

import core.{Inbox, Reactant, Receiver}

import readren.common.Maybe
import readren.sequencer.Doer

import java.net.URI
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.AbstractIterator

/**
 * @param owner the [[Reactant]] that owns this [[Inbox]] */
class ConcurrentUnboundedFifo[M](owner: Reactant[M, ?]) extends Receiver[M], Inbox[M] {
	private val queue = new ConcurrentLinkedQueue[M]()

	/** Incremented before a message is added, and decremented after a message is withdrawn. Therefore, its value tend to be equal to the queue's actual size but may be greater.
	 * This mark is necessary because [[ConcurrentLinkedQueue.isEmpty]] is not accurate. There is a time window during which it returns true after [[ConcurrentLinkedQueue.offer]] was called. */
	private val atomicSize: AtomicInteger = new AtomicInteger(0)

	override val uri: URI = {
		val mu = owner.matrix.uri
		URI(mu.getScheme, null, mu.getHost, mu.getPort, mu.getPath + owner.path, null, null)
	}

	override def submit(message: M): Unit = {
		if atomicSize.getAndIncrement() == 0 then {
			queue.offer(message)
			owner.doer.execute(owner.onInboxBecomesNonempty())
		} else queue.offer(message)
	}

	override def withdraw(): Maybe[M] = {
		val maybeMsg = Maybe.apply(queue.poll)
		maybeMsg.foreach(_ => atomicSize.decrementAndGet())
		maybeMsg
	}

	override def maybeNonEmpty: Boolean = {
		atomicSize.get > 0
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
