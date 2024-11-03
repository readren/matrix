package readren.matrix

import readren.taskflow.Doer

object ReactantKind {
	type SerialNumber = Int
}

trait ReactantKind {
	/** Should be thread-safe  */
	def createInbox[M](admin: MatrixAdmin): InboxBackend[M]
	/** Called withing the progenitor's [[Doer]]. */
	def createReactant[M](id: Reactant.SerialNumber, progenitor: Progenitor, admin: MatrixAdmin, inbox: Inbox[M]): Reactant[M]
}
