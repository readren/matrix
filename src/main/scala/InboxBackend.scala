package readren.matrix

import readren.taskflow.Maybe

trait InboxBackend[M] extends Inbox[M] {

	/** The [[MatrixAdmin]] assigned to this inbox and the reactant that owns it. All mutable the mutable members of this inbox instance should be accessed within this [[MatrixAdmin]]. 
	 * */
	val admin: MatrixAdmin

	/** Withdraws the next pending message. */
	def withdraw(): admin.Duty[Maybe[M]]

	/** Called one time a moment after this instance is created. */
	def setOwner(reactant: Reactant[M], asker: MatrixAdmin): Unit

}

object InboxBackend {
	def union[A, B](inbox: InboxBackend[A], inboxB: InboxBackend[B]): InboxBackend[A|B] = {
		???
	}
}
