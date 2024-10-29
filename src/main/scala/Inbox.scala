package readren.matrix

import readren.taskflow.Doer

object Inbox {
}

/** A facade of [[InboxBackend]] that only exposes the functionality intended for message suppliers (in opposition to message consumers).
 * @tparam M type of the message this inbox receives. */
trait Inbox[M] {

	def submit(message: M): Unit
}
