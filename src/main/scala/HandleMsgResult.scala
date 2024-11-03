package readren.matrix

sealed trait HandleMsgResult[M] {
	def map[U](f: Behavior[M] => Behavior[U]): HandleMsgResult[U]
}

case class ContinueWith[M](behavior: Behavior[M]) extends HandleMsgResult[M] {
	override def map[U](f: Behavior[M] => Behavior[U]): HandleMsgResult[U] = ContinueWith(f(behavior))
}

sealed trait HandlingFailed extends HandleMsgResult[Nothing] {
	override def map[U](f: Behavior[Nothing] => Behavior[U]): HandleMsgResult[U] = this.asInstanceOf[HandleMsgResult[U]]	
}

case object Stop extends HandlingFailed

case object Restart extends HandlingFailed

case object Ignore extends HandlingFailed

case class Error(exceptionHandlerError: Throwable, originalCause: Throwable) extends HandlingFailed