package readren.matrix

sealed trait HandleMsgResult[-M] {
	def map[U, S <: M](f: Behavior[S] => Behavior[U]): HandleMsgResult[U]
}

case class ContinueWith[-M](behavior: Behavior[M]) extends HandleMsgResult[M] {
	override def map[U, S <: M](f: Behavior[S] => Behavior[U]): HandleMsgResult[U] = ContinueWith(f(behavior))
}

sealed trait WithSameBehavior extends HandleMsgResult[Any] {
	override def map[U, S <: Any](f: Behavior[S] => Behavior[U]): HandleMsgResult[U] = this
}

case object Continue extends WithSameBehavior

case object Stop extends WithSameBehavior

/** Restart the reactant after stopping all children. */
case object Restart extends WithSameBehavior

/** Restart the reactant without stopping any of its child reactants.
 *  @param behavior the behavior that the reactant will have after the restart. Note that the consequent [[RestartReceived]] signal will be handled by the reactant's current behavior, while the [[Restarted]] signal will be handled by the specified behavior. */
case class RestartWith[-M](behavior: Behavior[M]) extends HandleMsgResult[M] {
	override def map[U, S <: M](f: Behavior[S] => Behavior[U]): HandleMsgResult[U] = RestartWith(f(behavior))
}

case class Error(exceptionHandlerError: Throwable, originalCause: Throwable) extends WithSameBehavior