package readren.matrix
package behaviors

import core.{Behavior, ContinueWith, HandleResult, Restart, RestartWith}


inline def restartNest[A](initializer: () => Behavior[A])(cleaner: () => Unit): Behavior[A] =
	new RestartNest(initializer(), initializer, cleaner)

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