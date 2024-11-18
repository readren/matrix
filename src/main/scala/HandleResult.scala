package readren.matrix

sealed trait HandleResult[-A] {
	def mapMsg[B, A1 <: A](f: Behavior[A1] => Behavior[B]): HandleResult[B]

	def mapBehavior[A1 <: A, B[a] <: Behavior[a]](f: Behavior[A1] => B[A1]): HandleResult[A1]
}

case class ContinueWith[-A](behavior: Behavior[A]) extends HandleResult[A] {
	override def mapMsg[B, A1 <: A](f: Behavior[A1] => Behavior[B]): HandleResult[B] = ContinueWith(f(behavior))

	override def mapBehavior[A1 <: A, B[a] <: Behavior[a]](f: Behavior[A1] => B[A1]): HandleResult[A1] = ContinueWith(f(behavior))
}

sealed trait WithSameBehavior extends HandleResult[Any] {
	override def mapMsg[U, S <: Any](f: Behavior[S] => Behavior[U]): HandleResult[U] = this

	override def mapBehavior[A1 <: Any, B[a] <: Behavior[a]](f: Behavior[A1] => B[A1]): HandleResult[A1] = this
}

case object Continue extends WithSameBehavior

case object Stop extends WithSameBehavior

/** Restart the reactant after stopping all children. */
case object Restart extends WithSameBehavior

/** Restart the reactant without stopping any of its child reactants.
 * Note that if the [[Behavior.handleSignal]] of the current behavior responds with [[Stop]] or [[Error]] to the [[RestartReceived]] signal then the restart is canceled and the [[Reactant]] is stopped instead. 
 * @param behavior the behavior that the reactant will have after the restart. Note that the consequent [[RestartReceived]] signal will be handled by the reactant's current behavior, while the [[Restarted]] signal will be handled by the specified behavior. */
case class RestartWith[-A](behavior: Behavior[A]) extends HandleResult[A] {
	override def mapMsg[B, A1 <: A](f: Behavior[A1] => Behavior[B]): HandleResult[B] = RestartWith(f(behavior))

	override def mapBehavior[A1 <: A, B[a] <: Behavior[a]](f: Behavior[A1] => B[A1]): HandleResult[A1] = ContinueWith(f(behavior))
}

case object Unhandled extends WithSameBehavior