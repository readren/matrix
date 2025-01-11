package readren.matrix
package behaviors

import core.{Behavior, HandleResult, Unhandled}

import scala.reflect.TypeTest


inline def unitedNest[A, B](bA: Behavior[A], bB: Behavior[B])(using ttA: TypeTest[A | B, A], ttB: TypeTest[A | B, B]): Behavior[A | B] =
	new UnitedNest(bA, bB)

private class UnitedNest[A, B](
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

//	/** Useful when both behaviors handle [[Signal]]. */
//	def combineSignals(hrA: HandleResult[A], hrB: HandleResult[B]): HandleResult[A | B] = {
//		(hrA, hrB) match {
//			case (Unhandled, Unhandled) => Unhandled
//			case (Continue | Unhandled, Continue | Unhandled) => Continue
//			case (Stop, _) | (_, Stop) => Stop
//			case (Restart, _) | (_, Restart) => Restart
//			case (RestartWith(nbA), hsrB) => hsrB match {
//				case Continue | Unhandled => RestartWith(update(nbA, bB))
//				case ContinueWith(nbB) => RestartWith(update(nbA, nbB))
//				case RestartWith(nbB) => RestartWith(update(nbA, nbB))
//				case _ => unreachable
//			}
//			case (hsrA, RestartWith(nbB)) => hsrA match {
//				case Continue | Unhandled => RestartWith(update(bA, nbB))
//				case ContinueWith(nba) => RestartWith(update(nba, nbB))
//				case _ => unreachable
//			}
//			case (ContinueWith(nbA), hsrB) => hsrB match {
//				case Continue | Unhandled => ContinueWith(update(nbA, bB))
//				case ContinueWith(nbB) => ContinueWith(update(nbA, nbB))
//				case _ => unreachable
//			}
//			case (hsrA, ContinueWith(nbB)) => hsrA match {
//				case Continue | Unhandled => ContinueWith(update(bA, nbB))
//				case _ => unreachable
//			}
//		}
//	}
//
//	def unreachable: Nothing = throw new AssertionError("unreachable")

}