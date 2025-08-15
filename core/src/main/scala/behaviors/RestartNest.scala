package readren.matrix
package behaviors

import core.*

/** Note that the type parameter would be contravariant if this class was immutable. Change it if contravariance is necessary. */
private class RestartNest[A](var nestedBehavior: Behavior[A], initializer: () => Behavior[A], cleaner: () => Unit) extends Behavior[A] {
	private def update(newBehavior: Behavior[A]): this.type = {
		this.nestedBehavior = newBehavior
		this
	}

	override def handle(message: A): HandleResult[A] = {
		nestedBehavior.handle(message) match {
			case Restart =>
				cleaner()
				ContinueWith(update(initializer()))
			case rw: RestartWith[A] => rw
			case hmrA => hmrA.mapBehavior(bA => update(bA))
		}
	}
}