package readren.matrix

import readren.taskflow.deriveToString

import scala.reflect.TypeTest

trait Behavior[-M] {
	def handleMessage(message: M): HandleResult[M] = Continue

	def handleSignal(signal: Signal): HandleResult[M] = Continue
}

object Behavior {

	inline def union[A, B](bA: Behavior[A], bB: Behavior[B])(signalResultCombiner: (HandleResult[A], HandleResult[B]) => HandleResult[A | B])(using ttA: TypeTest[A | B, A], ttB: TypeTest[A | B, B]): Behavior[A | B] =
		new Union(bA, bB)(signalResultCombiner)

	final class Union[A, B](bA: Behavior[A], bB: Behavior[B])(signalResultCombiner: (HandleResult[A], HandleResult[B]) => HandleResult[A | B])(using ttA: TypeTest[A | B, A], ttB: TypeTest[A | B, B]) extends Behavior[A | B] { thisUnionBehavior =>
		override def handleMessage(message: A | B): HandleResult[A | B] = {
			message match {
				case ttA(a) =>
					bA.handleMessage(a).map { nba =>
						if nba eq bA then thisUnionBehavior else union(nba, bB)(signalResultCombiner)
					}

				case ttB(b) =>
					bB.handleMessage(b).map { nbb =>
						if nbb eq bB then thisUnionBehavior else union(bA, nbb)(signalResultCombiner)
					}
			}
		}

		override def handleSignal(signal: Signal): HandleResult[A | B] =
			if signal.isInitialization then { // TODO analyze if the order should be the opposite.
				val hsrB = bB.handleSignal(signal)
				val hsrA = bA.handleSignal(signal)
				signalResultCombiner(hsrA, hsrB)
			} else {
				val hsrA = bA.handleSignal(signal)
				val hsrB = bB.handleSignal(signal)
				signalResultCombiner(hsrA, hsrB)
			}

		override def toString: String = deriveToString[Union[A, B]](this)
	}
}



