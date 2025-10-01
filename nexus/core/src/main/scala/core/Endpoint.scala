package readren.nexus
package core

import java.net.URI

/** A facade that only exposes the functionality intended for message suppliers (in opposition to message consumers).
 * @tparam M type of the message this inbox receives. */
sealed trait Endpoint[-M] {

	def tell(message: M): Unit

	def isLocal: Boolean
	
//	def ask[R](question: M & Endpoint[R]): Unit
//	def ask[R](question: M, replyInbox: Inbox[R]): Unit
//	def ask[R](question: M, doer: Doer): doer.Duty[R]
}

case class LocalEndpoint[U, M<: U](receiver: Receiver[U]) extends Endpoint[M] {
	override def tell(message: M): Unit = receiver.submit(message)
	override def isLocal: true = true
}
case class RemoteEndpoint[U, M<: U](uri: URI) extends Endpoint[M] {
	override def tell(message: M): Unit = ??? // TODO
	override def isLocal: false = false
}