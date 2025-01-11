package readren.matrix
package behaviors

import core.{Behavior, Continue, ContinueWith, HandleResult}

import scala.language.experimental.saferExceptions
import scala.reflect.Typeable


trait CheckedBehavior[-A, M <: Exception : Typeable] {

	def handleChecked(message: A)(using ctM: CanThrow[M]): CheckedBehavior[A, M] | HandleResult[A]

}

object CheckedBehavior {
	/**
	 * Converts a [[CheckedBehavior]] to a [[Behavior]] that generates the [[CanThrow]] capabilities with the help of the provided `handlerRecovery` function. 
	 * Implementation note: The `inline` modifier is necessary to allow the compiler to know the concrete type of `M` at compile-time.
	 * Otherwise, the {{{case m: M => ...}}} part would be unchecked, and we can't use [[TypeTest]] here because the current version of scala (2.6.2) only generates capabilities for catch clauses of the form {{ case ex: Ex => }}
	 * Additionally, the `safer` method must reside within `makeSafe` to preserve the visibility and proper scoping of `M`.
	 */
	inline def makeSafe[A, M <: Exception](
		checkedBehavior: CheckedBehavior[A, M]
	)(
		inline handlerRecovery: M => CheckedBehavior[A, M] | HandleResult[A]
	): Behavior[A] = {
		def safer(checkedBehavior: CheckedBehavior[A, M]): Behavior[A] =
			(message: A) => {
				val next =
					try checkedBehavior.handleChecked(message)
					catch {
						case m: M => handlerRecovery(m)
					}
				next match {
					case hr: HandleResult[A @unchecked] => hr
					case cb: CheckedBehavior[A @unchecked, M @unchecked] =>
						if cb eq checkedBehavior then Continue else ContinueWith(safer(cb))
				}
			}

		safer(checkedBehavior)
	}


	inline def factory[A, M <: Exception : Typeable, S <: Exception : Typeable](
		inline msgHandler: A => CheckedBehavior[A, M] | HandleResult[A]
	): CheckedBehavior[A, M] =
		new CheckedBehavior[A, M] {
			override def handleChecked(message: A)(using ctM: CanThrow[M]): CheckedBehavior[A, M] | HandleResult[A] =
				msgHandler(message)
		}
}
