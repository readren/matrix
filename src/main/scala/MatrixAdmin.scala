package readren.matrix

import constants.*

import readren.taskflow.Doer.ExceptionReport
import readren.taskflow.{Doer, Maybe}

import scala.util.{Failure, Success, Try}

object MatrixAdmin {


}

class MatrixAdmin(val assistant: Doer.Assistant, msgHandlerExecutorService: MsgHandlerExecutorService) extends Doer(assistant) { thisAdmin =>


	/** Should be called within this [[MatrixAdmin]]. */
	def stimulate[M](reactant: Reactant[M], stimulator: InboxBackend[M]): Unit = {
		assert(reactant.isIdle && (reactant.admin eq thisAdmin) && (stimulator.admin eq thisAdmin))

		def processMsg(message: M, currentBehavior: Behavior[M]): thisAdmin.Duty[Boolean] = {
			inline val clarityVsEfficiency = false
			val handlesMessage: thisAdmin.Duty[ProcessMsgResult[M]] =
				if clarityVsEfficiency then {
					// clarity version
					val covenant = new thisAdmin.Covenant[ProcessMsgResult[M]]
					msgHandlerExecutorService.executeMsgHandler(currentBehavior, message) { pmc => covenant.fulfill(pmc)() }
					covenant
				} else {
					// a bit more efficient version: creates one less object.
					var result: Maybe[ProcessMsgResult[M]] = Maybe.empty
					var observer: Maybe[ProcessMsgResult[M] => Unit] = Maybe.empty
					msgHandlerExecutorService.executeMsgHandler(currentBehavior, message) { pmc =>
						thisAdmin.queueForSequentialExecution {
							observer.fold {
								result = Maybe.some(pmc)
							}(_(pmc))
						}
					}
					new thisAdmin.Duty[ProcessMsgResult[M]] {
						override def engage(onComplete: ProcessMsgResult[M] => Unit): Unit =
							result.fold {
								observer = Maybe.some(onComplete)
							}(onComplete)
					}
				}

			handlesMessage.map[Boolean] {
				case cw: ContinueWith[M @unchecked] =>
					reactant.setBehavior(cw.behavior)
					false
				case Ignore =>
					false
				case Restart =>
					reactant.markForRestart()
					true
				case Stop =>
					reactant.markForTermination()
					true
				case Error(exceptionHandlerError, originalCause) =>
					thisAdmin.reportFailure(new ExceptionReport(s"The error handler of the behavior [$currentBehavior] terminated abruptly when it tried to handle [$originalCause], which was throw when handling the message [$message]", exceptionHandlerError))
					true
			}
		}

		def consumeNextPendingMessages(): Duty[Boolean] = {

			// Build a duty that, if there is a pending message, withdraws it, handles it, updates the reactant's behavior, and returns `Maybe.empty`; else returns Maybe.some(()).
			val handlesMessageAndUpdatesBehavior: thisAdmin.Duty[Maybe[Boolean]] =
				thisAdmin.Duty.mine { () =>
					// withdraw the next pending message considering all the inboxes the reactant has.
					reactant.withdrawNextMessage()
				}.flatMap { oNextMessage =>
					oNextMessage.fold {
						// if no pending message to process, return Maybe.some(false)
						thisAdmin.Duty.ready(SomeFalse)
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
						SomeTrue
					}
					else {
						// if pending messages are exhausted, mark the reactant as idle and exit the loop.
						reactant.setIsIdleState(true)
						SomeFalse
					}
				}
			}
		}

		reactant.setIsIdleState(false)

		// build a duty that handles all pending messages updating the behavior and then set the reactant's idle mark to true.
		val handlesAllPendingMessages: thisAdmin.Duty[Boolean] =
			stimulator.withdraw().castTypePath(thisAdmin).flatMap { oFirstMessage =>
				processMsg(oFirstMessage.get, reactant.currentBehavior).flatMap { haveToAbort =>
					if haveToAbort then thisAdmin.Duty.ready(true)
					else consumeNextPendingMessages()
				}
			}.andThen { haveToAbort =>
				if haveToAbort then {
					troubleShoot(reactant)
				}
			}

		// Nothing happens until this point where the duty built above is executed.
		handlesAllPendingMessages.triggerAndForget()
	}

	private def troubleShoot[M](reactant: Reactant[M]): Unit = {

		???
	}

}
