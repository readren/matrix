package readren.matrix

import readren.taskflow.Maybe

trait Inbox[+M] {

	//	/** The [[MatrixAdmin]] assigned to this inbox, which should be the same that are assigned to the [[Reactant]] that owns it.
	//	 * All mutable the mutable members of this inbox instance should be accessed within this [[MatrixAdmin]]. 
	//	 * */
	//	val admin: MatrixAdmin

	/** Withdraws the next pending message.
	 * Should be called withing the [[MatrixAdmin]] of the owning [[Reactant]] only. */
	def withdraw(): Maybe[M]


	/** Checks if there are no pending messages.
	 * Should be called withing the [[MatrixAdmin]] of the owning [[Reactant]] only. */
	def isEmpty: Boolean
}
