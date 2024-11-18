package readren.matrix

import scala.language.experimental.saferExceptions
import scala.reflect.Typeable


trait CheckedBehavior[-A, M <: Exception : Typeable, S <: Exception : Typeable] {

	def handleMsgChecked(message: A)(using ctM: CanThrow[M]): CheckedBehavior[A, M, S] | HandleResult[A]

	def handleSignalChecked(signal: Signal)(using ctS: CanThrow[S]): CheckedBehavior[A, M, S] | HandleResult[A]
}

object CheckedBehavior {

	inline def makeSafe[A, M <: Exception, S <: Exception](
		checkedBehavior: CheckedBehavior[A, M, S]
	)(
		inline msgHandlerRecovery: M => CheckedBehavior[A, M, S] | HandleResult[A]
	)(
		inline signalHandlerRecovery: S => CheckedBehavior[A, M, S] | HandleResult[A]
	): Behavior[A] = {
		class Safer(checkedBehavior: CheckedBehavior[A, M, S]) extends Behavior[A] { thisSafer =>

			override def handleMsg(message: A): HandleResult[A] = {
				val next =
					try checkedBehavior.handleMsgChecked(message)
					catch {
						case m: M => msgHandlerRecovery(m)
					}
				next match {
					case hr: HandleResult[A @unchecked] =>
						hr
					case cb: CheckedBehavior[A @unchecked, M @unchecked, S @unchecked] =>
						if cb eq checkedBehavior then Continue else ContinueWith(Safer(cb))
				}

			}

			override def handleSignal(signal: Signal): HandleResult[A] = {
				val next =
					try checkedBehavior.handleSignalChecked(signal)
					catch {
						case s: S => signalHandlerRecovery(s)
					}
				next match {
					case hr: HandleResult[A @unchecked] =>
						hr
					case cb: CheckedBehavior[A @unchecked, M @unchecked, S @unchecked] =>
						if cb eq checkedBehavior then Continue else ContinueWith(Safer(cb))
				}
			}
		}
		new Safer(checkedBehavior)
	}


	inline def factory[A, M <: Exception : Typeable, S <: Exception : Typeable](
		inline msgHandler: A => CheckedBehavior[A, M, S] | HandleResult[A]
	)(
		inline signalHandler: Signal => CheckedBehavior[A, M, S] | HandleResult[A]
	): CheckedBehavior[A, M, S] =
		new CheckedBehavior[A, M, S] {
			override def handleMsgChecked(message: A)(using ctM: CanThrow[M]): CheckedBehavior[A, M, S] | HandleResult[A] =
				msgHandler(message)

			override def handleSignalChecked(signal: Signal)(using ctS: CanThrow[S]): CheckedBehavior[A, M, S] | HandleResult[A] =
				signalHandler(signal)
		}
}
