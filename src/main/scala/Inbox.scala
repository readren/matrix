package readren.matrix

import readren.taskflow.Doer

object Inbox {
	trait Question[R] {
		val replyInbox: Inbox[R]
	}
}

/** A facade of [[InboxBackend]] that only exposes the functionality intended for message suppliers (in opposition to message consumers).
 * @tparam M type of the message this inbox receives. */
trait Inbox[-M] {

	def submit(message: M): Unit
	
//	def ask[R](message: M & Inbox[R]): Unit
//	def ask[R](message: M, replyInbox: Inbox[R]): Unit
}
