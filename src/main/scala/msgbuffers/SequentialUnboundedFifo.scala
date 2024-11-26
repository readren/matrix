package readren.matrix
package msgbuffers

import readren.taskflow.Maybe

import java.net.URI
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable

/**
 * @param owner the [[Reactant]] that owns this [[Inbox]] */
class SequentialUnboundedFifo[M](owner: Reactant[M]) extends Receiver[M], Inbox[M] { thisFifoInbox =>

	/** Should be accessed only within the [[admin]] */
	private val queue: mutable.ArrayDeque[M] = mutable.ArrayDeque.empty
	
	val admin: MatrixAdmin = owner.admin

	override val uri: URI = {
		val mu = admin.matrix.uri
		URI(mu.getScheme, null, mu.getHost, mu.getPort, mu.getPath + owner.path, null, null)
	}

	override def submit(message: M): Unit = {
		admin.queueForSequentialExecution {
			if queue.isEmpty then {
				if owner.onInboxNotEmpty(message) then queue.append(message) 
			} else queue.append(message)
		}
	}

	override def withdraw(): Maybe[M] = {
		admin.checkWithin()
		if queue.isEmpty then Maybe.empty
		else Maybe.some(queue.removeHead())
	}

	override def maybeNonEmpty: Boolean = {
		admin.checkWithin()
		queue.nonEmpty
	}
	
	override def size: Int = {
		queue.size
	}

	override def iterator: Iterator[M] = {
		queue.iterator
	}
}
