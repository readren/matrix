package readren.matrix

import readren.taskflow.Doer

object BehaviorKind {
	type SerialNumber = Int
}

trait BehaviorKind {
	/** Should be thread-safe  */
	def createInbox[M](doer: Doer): Inbox[M]
	/** Called withing the progenitor's [[Doer]]. */
	def createReactant[M](id: Reactant.SerialNumber, inbox: Inbox[M]): Reactant
}
