package readren.matrix


import readren.taskflow.Maybe

import java.net.URI
import scala.collection.mutable

/**
 * @param owner the [[Reactant]] that owns this [[Inbox]] */
class FifoInbox[M](owner: Reactant[M]) extends Receiver[M], Inbox[M] { thisFifoInbox =>
	
	/** Should be accessed only within the [[admin]] */
	private val queue: mutable.ArrayDeque[M] = mutable.ArrayDeque.empty

	val admin: MatrixAdmin = owner.admin

	override val uri: URI = {
		val mu = admin.matrix.uri
		URI(mu.getScheme, null, mu.getHost, mu.getPort, mu.getPath + owner.path, null, null)
	}

	override def submit(message: M): Unit = {
		admin.queueForSequentialExecution {
			if owner.isReady then {
				assert(queue.isEmpty)
				owner.stimulate(message)
			} else {
				queue.append(message)
			}
		}
	}

	override def withdraw(): Maybe[M] = {
		admin.checkWithin()
		if queue.isEmpty then Maybe.empty
		else Maybe.some(queue.removeHead())
	}

	override def nonEmpty: Boolean = {
		admin.checkWithin()
		queue.nonEmpty
	}

}
