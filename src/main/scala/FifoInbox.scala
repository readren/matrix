package readren.matrix


import scala.collection.mutable
import readren.taskflow.Doer

class FifoInbox[M](adminDoer: Doer) extends Inbox[M] {
	
	/** Should be accessed only within the [[adminDoer]] */
	private val queue: mutable.ArrayDeque[M] = mutable.ArrayDeque.empty

	override def submit(m: M): Unit = {
		adminDoer.queueForSequentialExecution(queue.append(m))
	}

	override def withdraw(withdrawer: Doer): withdrawer.Task[M] = {
		adminDoer.Task
			.mine(() => queue.removeHead())
			.onBehalfOf(withdrawer)
	}
}
