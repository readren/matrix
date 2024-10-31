package readren.matrix

import readren.taskflow.{Doer, Maybe}

import scala.util.{Failure, Success, Try}
import constants.*

object MatrixAdmin {


}

class MatrixAdmin(val assistant: Doer.Assistant, msgHandlerExecutorsManager: MsgHandlerExecutorsManager) extends Doer(assistant) { thisAdmin =>


	/** Should be called within this [[MatrixAdmin]]. */
	def stimulate[M](reactant: Reactant[M], stimulator: InboxBackend[M]): Unit = {
		assert(reactant.isIdle && (reactant.admin eq thisAdmin) && (stimulator.admin eq thisAdmin))

		def processMsg(message: M, currentBehavior: Behavior[M]): thisAdmin.Task[Boolean] = {
			val msgHandlerExecutor: MsgHandlerExecutor = msgHandlerExecutorsManager.pickExecutor()
			msgHandlerExecutor.executeMsgHandler(currentBehavior, message)
				.onBehalfOf(thisAdmin)
				.map {
					case ContinueWith(nextBehavior) =>
						reactant.setBehavior(nextBehavior)
						false
					case Ignore =>
						false
					case Stop =>
						reactant.markForTermination()
						true
					case Restart =>
						reactant.markForRestart()
						true
				}
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
			handlesMessageAndUpdatesBehavior.repeatedUntilSome() { (count, hungryExhaustedOrAborted) =>
				hungryExhaustedOrAborted.fold(Maybe.empty) { wasAborted =>
					if wasAborted then {
						// if the cycle was aborted, exit the loop.
						SomeSuccessTrue
					}
					else {
						// if pending messages are exhausted, mark the reactant as idle and exit the loop.
						reactant.setIsIdleState(true)
						SomeSuccessFalse
					}
				}
			}
		}

		reactant.setIsIdleState(false)

		// build a task that handles all pending messages updating the behavior and then set the reactant's idle mark to true.
		val handlesAllPendingMessages: thisAdmin.Task[Boolean] =
			stimulator.withdraw().castTypePath(thisAdmin).flatMap { oFirstMessage =>
				processMsg(oFirstMessage.get, reactant.currentBehavior).flatMap { haveToAbort =>
					if haveToAbort then thisAdmin.Task.immediate(SuccessTrue)
					else consumeNextPendingMessages()
				}
			}.andThen { result =>
				if result ne SuccessFalse then {
					if result.isInstanceOf[Failure[?]] then reactant.markForTermination()
					else assert(result eq SuccessTrue)
					troubleShoot()
				}

			}

		// Nothing happens until this point where the task built above is executed.
		handlesAllPendingMessages.attemptAndForgetHandlingErrors()(reportFailure)
	}

	private def troubleShoot(): Unit = {
		???
	}

}
