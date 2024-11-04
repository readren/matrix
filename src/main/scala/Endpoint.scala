package readren.matrix

import readren.taskflow.Doer


/** A facade that only exposes the functionality intended for message suppliers (in opposition to message consumers).
 * @tparam M type of the message this inbox receives. */
trait Endpoint[-M] {

	def tell(message: M): Unit
	
	def ask[R](question: M & Endpoint[R]): Unit
	def ask[R](question: M, replyInbox: Inbox[R]): Unit
	def ask[R](question: M, doer: Doer): doer.Duty[R]
}
