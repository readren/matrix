package readren.matrix

import readren.taskflow.TaskDomain

import BehaviorKind.*

object BehaviorKind {
	type SerialNumber = Int
}

trait BehaviorKind {
	/** Should be thread-safe  */
	def createInbox[M](taskDomain: TaskDomain): Inbox[M]
	/** Called withing the progenitor's [[TaskDomain]]. */
	def createDoer[M](id: Doer.SerialNumber, inbox: Inbox[M]): Doer
}
