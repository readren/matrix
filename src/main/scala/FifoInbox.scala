package readren.matrix


import scala.collection.mutable
import readren.taskflow.TaskDomain

class FifoInbox[M](adminDomain: TaskDomain) extends Inbox[M] {
	
	/** Should be accessed only within the [[adminDomain]] */
	private val queue: mutable.ArrayDeque[M] = mutable.ArrayDeque.empty

	override def submit(m: M): Unit = {
		adminDomain.queueForSequentialExecution(queue.append(m))
	}

	override def withdraw(withdrawingTaskDomain: TaskDomain): withdrawingTaskDomain.Task[M] = {
		withdrawingTaskDomain.Task.foreign(adminDomain) {
			adminDomain.Task.mine(() => queue.removeHead())
		}
	}
}
