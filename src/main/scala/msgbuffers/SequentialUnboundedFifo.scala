package readren.matrix
package msgbuffers

import readren.taskflow.Maybe

import java.net.URI
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.{AbstractIterator, mutable}

/**
 * @param owner the [[Reactant]] that owns this [[Inbox]] */
class SequentialUnboundedFifo[M](owner: Reactant[M]) extends Receiver[M], Inbox[M] { thisFifoInbox =>

	/** Should be accessed only within the [[admin]] */
	private val queue: java.util.ArrayDeque[M] = new java.util.ArrayDeque()
	
	val admin: MatrixAdmin = owner.admin

	override val uri: URI = {
		val mu = admin.matrix.uri
		URI(mu.getScheme, null, mu.getHost, mu.getPort, mu.getPath + owner.path, null, null)
	}

	override def submit(message: M): Unit = {
		admin.queueForSequentialExecution {
			if queue.isEmpty then {
				if owner.onInboxBecomesNonempty(message) then queue.add(message)
			} else queue.add(message)
		}
	}

	override def withdraw(): Maybe[M] = {
		admin.checkWithin()
		if queue.isEmpty then Maybe.empty
		else Maybe.some(queue.pollFirst())
	}

	override def maybeNonEmpty: Boolean = {
		admin.checkWithin()
		!queue.isEmpty
	}
	
	override def size: Int = {
		queue.size
	}

	override def iterator: Iterator[M] = {
		new AbstractIterator[M] {
			private val javaIterator = queue.iterator()
			
			override def hasNext: Boolean = javaIterator.hasNext

			override def next(): M = javaIterator.next()
		}
	}
}
