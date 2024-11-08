package readren.matrix

import readren.taskflow.deriveToString

import scala.reflect.TypeTest

trait Behavior[-M] {
	def handleMessage(message: M): HandleMsgResult[M]

	def handleSignal(signal: Signal): Unit
}

object Behavior {

	inline def union[A, B](bA: Behavior[A], bB: Behavior[B])(using ttA: TypeTest[A | B, A], ttB: TypeTest[A | B, B]): Behavior[A | B] =
		new Union(bA, bB)
	
	final class Union[A, B](bA: Behavior[A], bB: Behavior[B])(using ttA: TypeTest[A | B, A], ttB: TypeTest[A | B, B]) extends Behavior[A | B] { thisUnionBehavior =>
		def handleMessage(message: A | B): HandleMsgResult[A | B] = {
			message match {
				case ttA(a) =>
					bA.handleMessage(a).map { nba =>
						if nba eq bA then thisUnionBehavior else union(nba, bB)
					}

				case ttB(b) =>
					bB.handleMessage(b).map { nbb =>
						if nbb eq bB then thisUnionBehavior else union(bA, nbb)
					}
			}
		}
		
		def handleSignal(signal: Signal): Unit =
			if signal.isInitialization then { // TODO analyze if the order should be the opposite.
				bB.handleSignal(signal)
				bA.handleSignal(signal)
			} else {
				bA.handleSignal(signal)
				bB.handleSignal(signal)
			}

		override def toString: String = deriveToString[Union[A, B]](this)
	}
}



