package readren.nexus
package core

sealed trait HandleResult[-A] {
	def mapBehavior[B, A1 <: A](f: Behavior[A1] => Behavior[B]): HandleResult[B]

	@deprecated("not needed anymore", "0.1.0")
	def adaptBehavior[A1 <: A, B[a] <: Behavior[a]](f: Behavior[A1] => B[A1]): HandleResult[A1]
}

case class ContinueWith[-A](behavior: Behavior[A]) extends HandleResult[A] {
	override def mapBehavior[B, A1 <: A](f: Behavior[A1] => Behavior[B]): HandleResult[B] = ContinueWith(f(behavior))

	override def adaptBehavior[A1 <: A, B[a] <: Behavior[a]](f: Behavior[A1] => B[A1]): HandleResult[A1] = ContinueWith(f(behavior))
}

sealed trait WithSameBehavior extends HandleResult[Any] {
	override def mapBehavior[U, S <: Any](f: Behavior[S] => Behavior[U]): HandleResult[U] = this

	override def adaptBehavior[A1 <: Any, B[a] <: Behavior[a]](f: Behavior[A1] => B[A1]): HandleResult[A1] = this
}

case object Continue extends WithSameBehavior

case object Stop extends WithSameBehavior

/** Restart the spuron after stopping all children. */
case object Restart extends WithSameBehavior

/** Restart the spuron without stopping any of its child spurons.
 * Note that if the [[Behavior.handle]] of the current behavior responds with [[Stop]] to the [[RestartReceived]] signal then the restart is canceled and the [[SpuronCore]] is stopped instead.
 * @param behavior the behavior that the spuron will have after the restart. Note that the consequent [[RestartReceived]] signal will be handled by the spuron's current behavior, while the [[Restarted]] signal will be handled by the specified behavior. */
case class RestartWith[-A](behavior: Behavior[A]) extends HandleResult[A] {
	override def mapBehavior[B, A1 <: A](f: Behavior[A1] => Behavior[B]): HandleResult[B] = RestartWith(f(behavior))

	override def adaptBehavior[A1 <: A, B[a] <: Behavior[a]](f: Behavior[A1] => B[A1]): HandleResult[A1] = ContinueWith(f(behavior))
}

case object Unhandled extends WithSameBehavior