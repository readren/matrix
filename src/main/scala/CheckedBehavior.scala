package readren.matrix

import scala.language.experimental.saferExceptions
import scala.reflect.Typeable


trait CheckedBehavior[-A, M <: Exception : Typeable] {

	def handleChecked(message: A)(using ctM: CanThrow[M]): CheckedBehavior[A, M] | HandleResult[A]

}

object CheckedBehavior {

	inline def makeSafe[A, M <: Exception](
		checkedBehavior: CheckedBehavior[A, M]
	)(
		inline handlerRecovery: M => CheckedBehavior[A, M] | HandleResult[A]
	): Behavior[A] = {
		class Safer(checkedBehavior: CheckedBehavior[A, M]) extends Behavior[A] { thisSafer =>

			override def handle(message: A): HandleResult[A] = {
				val next =
					try checkedBehavior.handleChecked(message)
					catch {
						case m: M => handlerRecovery(m)
					}
				next match {
					case hr: HandleResult[A @unchecked] =>
						hr
					case cb: CheckedBehavior[A @unchecked, M @unchecked] =>
						if cb eq checkedBehavior then Continue else ContinueWith(Safer(cb))
				}

			}
		}
		new Safer(checkedBehavior)
	}


	inline def factory[A, M <: Exception : Typeable, S <: Exception : Typeable](
		inline msgHandler: A => CheckedBehavior[A, M] | HandleResult[A]
	): CheckedBehavior[A, M] =
		new CheckedBehavior[A, M] {
			override def handleChecked(message: A)(using ctM: CanThrow[M]): CheckedBehavior[A, M] | HandleResult[A] =
				msgHandler(message)
		}
}
