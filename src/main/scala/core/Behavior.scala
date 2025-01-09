package readren.matrix
package core

import scala.reflect.TypeTest

trait Behavior[-A] {
	def handle(message: A): HandleResult[A]
}

object Behavior {

	inline def ignore: Behavior[Any] = IgnoreAllBehavior

	inline def factory[A](inline handler: A => HandleResult[A]): Behavior[A] =
		(message: A) => handler(message)
	
	inline def superviseNest[A](behavior: Behavior[A]): SupervisedNest[A] =
		new SupervisedNest[A](behavior, defaultCatcher)

	inline def restartNest[A](initializer: () => Behavior[A])(cleaner: () => Unit): Behavior[A] =
		new RestartNest(initializer(), initializer, cleaner)

	def unitedNest[A, B](bA: Behavior[A], bB: Behavior[B])(using ttA: TypeTest[A | B, A], ttB: TypeTest[A | B, B]): Behavior[A | B] =
		new UnitedNest(bA, bB)

	def defaultCatcher[A]: PartialFunction[Throwable, HandleResult[A]] = {
		case scala.util.control.NonFatal(e) => Stop
	}

	def unreachable: Nothing = throw new AssertionError("unreachable")
}

object IgnoreAllBehavior extends Behavior[Any] {
	override def handle(message: Any): HandleResult[Any] = Continue
}

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

class RestartNest[A](var nestedBehavior: Behavior[A], initializer: () => Behavior[A], cleaner: () => Unit) extends Behavior[A] {
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

class UnitedNest[A, B](
	var bA: Behavior[A],
	var bB: Behavior[B]
)(using ttA: TypeTest[A | B, A], ttB: TypeTest[A | B, B]) extends Behavior[A | B] { thisUnitedBehavior =>

	def update(nbA: Behavior[A], nbB: Behavior[B]): this.type = {
		this.bA = nbA
		this.bB = nbB
		this
	}

	override def handle(message: A | B): HandleResult[A | B] = {
		message match {
			case ttA(a) =>
				val hrA = bA.handle(a)
				if hrA eq Unhandled then ttB.unapply(message) match {
					case Some(b) => handleB(b)
					case None => Unhandled
				} else {
					hrA.mapMsg { nbA => update(nbA, bB) }
				}

			case ttB(b) => handleB(b)
		}
	}

	private def handleB(b: B): HandleResult[A | B] =
		bB.handle(b).mapMsg { nmbB =>
			update(bA, nmbB)
		}

	/** Useful when both behaviors handle [[Signal]]. */
	def combineSignals(hrA: HandleResult[A], hrB: HandleResult[B]): HandleResult[A | B] = {
		(hrA, hrB) match {
			case (Unhandled, Unhandled) => Unhandled
			case (Continue | Unhandled, Continue | Unhandled) => Continue
			case (Stop, _) | (_, Stop) => Stop
			case (Restart, _) | (_, Restart) => Restart
			case (RestartWith(nbA), hsrB) => hsrB match {
				case Continue | Unhandled => RestartWith(update(nbA, bB))
				case ContinueWith(nbB) => RestartWith(update(nbA, nbB))
				case RestartWith(nbB) => RestartWith(update(nbA, nbB))
				case _ => Behavior.unreachable
			}
			case (hsrA, RestartWith(nbB)) => hsrA match {
				case Continue | Unhandled => RestartWith(update(bA, nbB))
				case ContinueWith(nba) => RestartWith(update(nba, nbB))
				case _ => Behavior.unreachable
			}
			case (ContinueWith(nbA), hsrB) => hsrB match {
				case Continue | Unhandled => ContinueWith(update(nbA, bB))
				case ContinueWith(nbB) => ContinueWith(update(nbA, nbB))
				case _ => Behavior.unreachable
			}
			case (hsrA, ContinueWith(nbB)) => hsrA match {
				case Continue | Unhandled => ContinueWith(update(bA, nbB))
				case _ => Behavior.unreachable
			}
		}
	}
}




