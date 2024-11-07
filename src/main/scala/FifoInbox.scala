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
			val wasEmpty = queue.isEmpty
			if owner.isIdle then {
				assert(wasEmpty) 
				owner.stimulate(message)
			} else {
				queue.append(message)
			}
		}
	}

	override def withdraw(): Maybe[M] = {
		if queue.isEmpty then Maybe.empty
		else Maybe.some(queue.removeHead())
	}

	override def isEmpty: Boolean = queue.isEmpty

}
