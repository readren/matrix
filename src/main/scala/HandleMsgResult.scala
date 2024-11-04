package readren.matrix

sealed trait HandleMsgResult[M] {
	def map[U](f: Behavior[M] => Behavior[U]): HandleMsgResult[U]
}

case class ContinueWith[M](behavior: Behavior[M]) extends HandleMsgResult[M] {
	override def map[U](f: Behavior[M] => Behavior[U]): HandleMsgResult[U] = ContinueWith(f(behavior))
}

sealed trait WithSameBehavior extends HandleMsgResult[Nothing] {
	override def map[U](f: Behavior[Nothing] => Behavior[U]): HandleMsgResult[U] = this.asInstanceOf[HandleMsgResult[U]]	
}

case object Continue extends WithSameBehavior

case object Stop extends WithSameBehavior

case object Restart extends WithSameBehavior

case class Error(exceptionHandlerError: Throwable, originalCause: Throwable) extends WithSameBehavior