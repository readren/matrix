package readren.matrix

sealed trait HandleResult[-M] {
	def map[U, S <: M](f: Behavior[S] => Behavior[U]): HandleResult[U]
}

case class ContinueWith[-M](behavior: Behavior[M]) extends HandleResult[M] {
	override def map[U, S <: M](f: Behavior[S] => Behavior[U]): HandleResult[U] = ContinueWith(f(behavior))
}

sealed trait WithSameBehavior extends HandleResult[Any] {
	override def map[U, S <: Any](f: Behavior[S] => Behavior[U]): HandleResult[U] = this
}

case object Continue extends WithSameBehavior

case object Stop extends WithSameBehavior

/** Restart the reactant after stopping all children. */
case object Restart extends WithSameBehavior

/** Restart the reactant without stopping any of its child reactants.
 * Note that if the [[Behavior.handleSignal]] of the current behavior responds with [[Stop]] or [[Error]] to the [[RestartReceived]] signal then the restart is canceled and the [[Reactant]] is stopped instead. 
 *  @param behavior the behavior that the reactant will have after the restart. Note that the consequent [[RestartReceived]] signal will be handled by the reactant's current behavior, while the [[Restarted]] signal will be handled by the specified behavior. */
case class RestartWith[-M](behavior: Behavior[M]) extends HandleResult[M] {
	override def map[U, S <: M](f: Behavior[S] => Behavior[U]): HandleResult[U] = RestartWith(f(behavior))
}

case class Error(exceptionHandlerError: Throwable, originalCause: Throwable) extends WithSameBehavior

case object Unhandled extends WithSameBehavior