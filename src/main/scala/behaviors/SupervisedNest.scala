package readren.matrix
package behaviors

import core.{Behavior, HandleResult, Stop}

object SupervisedNest {
	def defaultCatcher[A]: PartialFunction[Throwable, HandleResult[A]] = {
		case scala.util.control.NonFatal(e) => Stop
	}
}

/** Note that the type parameter would be contravariant if this class was immutable. Change it if contravariance is necessary. */
class SupervisedNest[A](
	var backingBehavior: Behavior[A],
	var baseCatcher: PartialFunction[Throwable, HandleResult[A]]
) extends Behavior[A] {

	private def update(
		newBackingBehavior: Behavior[A],
		newBaseCatcher: PartialFunction[Throwable, HandleResult[A]],
	): this.type = {
		backingBehavior = newBackingBehavior
		baseCatcher = newBaseCatcher
		this
	}

	override def handle(message: A): HandleResult[A] = {
		try backingBehavior.handle(message).mapBehavior(bA => update(bA, baseCatcher))
		catch baseCatcher
	}

	def withCatcher(extendingCatcher: PartialFunction[Throwable, HandleResult[A]]): SupervisedNest[A] =
		update(backingBehavior, extendingCatcher.orElse(baseCatcher))

}