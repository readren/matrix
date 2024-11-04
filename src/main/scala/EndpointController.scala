package readren.matrix
import readren.taskflow.Doer

class EndpointController[M, D >: M](receiver: Receiver[D]) extends Endpoint[M] {
	override def tell(message: M): Unit = receiver.submit(message)

	override def ask[R](question: M & Endpoint[R]): Unit = ???

	override def ask[R](question: M, replyInbox: Inbox[R]): Unit = ???

	override def ask[R](question: M, doer: Doer): doer.Duty[R] = ???
}
