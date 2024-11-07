package readren.matrix

sealed trait HandleMsgResult[-M] {
	def map[U, S <: M](f: Behavior[S] => Behavior[U]): HandleMsgResult[U]
}

case class ContinueWith[-M](behavior: Behavior[M]) extends HandleMsgResult[M] {
	override def map[U, S <: M](f: Behavior[S] => Behavior[U]): HandleMsgResult[U] = ContinueWith(f(behavior))
}

sealed trait WithSameBehavior extends HandleMsgResult[Nothing] {
	override def map[U, S <: Nothing](f: Behavior[Nothing] => Behavior[U]): HandleMsgResult[U] = this.asInstanceOf[HandleMsgResult[U]]
}

case object Continue extends WithSameBehavior

case object Stop extends WithSameBehavior

/**	CAUTION: if `stopChildren` is set to false, be sure to avoid unintentional recreation of them in the behavior setup.  
 *  @param stopChildren if true children will be stopped before the restart. */
case class Restart(stopChildren: Boolean = true) extends WithSameBehavior

case class RestartWith[-M](stopChildren: Boolean)(val behavior: Behavior[M]) extends HandleMsgResult[M] {
	override def map[U, S <: M](f: Behavior[S] => Behavior[U]): HandleMsgResult[U] = RestartWith(stopChildren)(f(behavior))
}

case class Error(exceptionHandlerError: Throwable, originalCause: Throwable) extends WithSameBehavior