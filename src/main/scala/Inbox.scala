package readren.matrix

import readren.taskflow.TaskDomain

object Inbox {
}

/** @tparam M type of the message this inbox receives. */
trait Inbox[M] {

	def submit(m: M): Unit
	def withdraw(withdrawingTaskDomain: TaskDomain): withdrawingTaskDomain.Task[M]
//	def owner: Doer | Null
}
