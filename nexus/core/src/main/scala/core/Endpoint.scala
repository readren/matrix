package readren.nexus
package core

import java.net.URI

/** An input terminal through which the owner receives messages of a specified type.
 * A [[Receptor]] is provided to whoever the owner wants to receive messages from.
 *
 * @tparam M type of the message this [[Receptor]] accepts. */
sealed trait Receptor[-M] {

	def tell(message: M): Unit

	def isLocal: Boolean

	//	def ask[R](question: M & Receptor[R]): Unit
//	def ask[R](question: M, replyInbox: Inbox[R]): Unit
//	def ask[R](question: M, doer: Doer): doer.Duty[R]
}

case class LocalReceptor[U, M <: U](inqueue: Inqueue[U]) extends Receptor[M] {
	override def tell(message: M): Unit = inqueue.submit(message)
	override def isLocal: true = true
}

case class RemoteReceptor[U, M <: U](uri: URI) extends Receptor[M] {
	override def tell(message: M): Unit = ??? // TODO
	override def isLocal: false = false
}