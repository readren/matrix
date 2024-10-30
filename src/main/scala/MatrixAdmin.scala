package readren.matrix

import readren.taskflow.{Doer, Maybe}

import scala.util.{Failure, Success, Try}
import constants.*

object MatrixAdmin {

	sealed trait ProcessMsgResult

	case class Continue[M](nextBehavior: Behavior[M])

	case object Stop extends ProcessMsgResult

	case object Restart extends ProcessMsgResult

	case object Ignore extends ProcessMsgResult
}

class MatrixAdmin(val assistant: Doer.Assistant, msgHandlingDoersManager: MsgHandlingDoersManager) extends Doer(assistant) { thisAdmin =>



	/** Should be called within this [[MatrixAdmin]]. */
	def stimulate[M](reactant: Reactant[M], stimulator: InboxBackend[M]): Unit = {
		assert(reactant.isIdle && (reactant.admin eq thisAdmin) && (stimulator.admin eq thisAdmin))

		def processMsg(message: M, currentBehavior: Behavior[M]): thisAdmin.Task[Boolean] = {
			val msgHandlingDoer: MsgHandlingDoer = msgHandlingDoersManager.pickMsgHandlingDoer()
			msgHandlingDoer.processes { currentBehavior.handle(message) }
				.onBehalfOf(thisAdmin)
				.map { nextBehavior =>
					reactant.setBehavior(nextBehavior)
					false
				}
				.recover(reactant.handleException)
		}

		def consumeNextPendingMessages(): Task[Boolean] = {

			// Build a task that, if there is a pending message, withdraws it, handles it, updates the reactant's behavior, and returns `Maybe.empty`; else returns Maybe.some(()).
			val handlesMessageAndUpdatesBehavior: thisAdmin.Task[Maybe[Boolean]] =
				thisAdmin.Task.mine { () =>
					// withdraw the next pending message considering all the inboxes the reactant has.
					reactant.withdrawNextMessage()
				}.flatMap { oNextMessage =>
					oNextMessage.fold {
						// if no pending message to process, return Maybe.some(false)
						thisAdmin.Task.immediate(SuccessSomeFalse)
					} { nextMessage =>
						// if a message was withdrawn, handle it, update the `nextBehavior` variable, and return Maybe.empty
						processMsg(nextMessage, reactant.currentBehavior).map { haveToAbort =>
							if haveToAbort then SomeTrue
							else Maybe.empty
						}
					}
				}

			// repeat the `handlesMessageAndUpdatesBehavior` task until pending messages are exhausted.
			handlesMessageAndUpdatesBehavior.repeatedUntilSome() { (count, continueStopOrAbort) =>
				continueStopOrAbort.fold(Maybe.empty) { stopOrAbort =>
					if stopOrAbort then {
						// if the cycle was aborted, set the mark on the reactant that the message handling was aborted
						reactant.abortionCompleted = true
						SomeSuccessTrue
					}
					else {
						// if pending messages are exhausted, mark the reactant as idle and exit the loop.
						reactant.isIdle = true
						SomeSuccessFalse
					}
				}
			}
		}

		reactant.isIdle = false

		// build a task that handles all pending messages updating the behavior and then set the reactant's idle mark to true.
		val handlesAllPendingMessages: thisAdmin.Task[Boolean] =
			stimulator.withdraw().castTypePath(thisAdmin).flatMap { oFirstMessage =>
				processMsg(oFirstMessage.get, reactant.currentBehavior).flatMap { haveToAbort =>
					if haveToAbort then thisAdmin.Task.immediate(SuccessTrue)
					else consumeNextPendingMessages()
				}
			}

		// Nothing happens until this point where the task built above is executed.
		handlesAllPendingMessages.attemptAndForgetHandlingErrors()(reportFailure)
	}

}
