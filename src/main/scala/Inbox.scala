package readren.matrix

import readren.taskflow.Doer

object Inbox {
}

/** @tparam M type of the message this inbox receives. */
trait Inbox[M] {

	def submit(m: M): Unit
	def withdraw(withdrawer: Doer): withdrawer.Task[M]
//	def reactant: Reactant | Null
}
