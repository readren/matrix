package readren.matrix


import scala.collection.mutable
import readren.taskflow.{Doer, Maybe}

import scala.compiletime.uninitialized

/**
 * @param owner the [[Reactant]] that owns this [[Inbox]] */
class FifoInbox[M](owner: Reactant[M]) extends Receiver[M], Inbox[M] { thisFifoInbox =>
	
	/** Should be accessed only within the [[admin]] */
	private val queue: mutable.ArrayDeque[M] = mutable.ArrayDeque.empty

	val admin: MatrixAdmin = owner.admin
	
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
}
