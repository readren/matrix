package readren.matrix

import constants.*

import readren.taskflow.Doer.ExceptionReport
import readren.taskflow.{Doer, Maybe}

object MatrixAdmin {
	private enum Decision {
		case continue, restart, stop
	}
}

class MatrixAdmin(val assistant: Doer.Assistant, msgHandlerExecutorService: MsgHandlerExecutorService) extends Doer(assistant) { thisAdmin =>

	import MatrixAdmin.*

	/** Should be called within this [[MatrixAdmin]]. */
	def stimulate[M](reactant: Reactant[M], stimulator: InboxBackend[M]): Unit = {
		assert(reactant.isIdle && (reactant.admin eq thisAdmin) && (stimulator.admin eq thisAdmin))

		def handleMsg(message: M, currentBehavior: Behavior[M]): thisAdmin.Duty[Decision] = {
			inline val clarityVsEfficiency = false
			val handlesMessage: thisAdmin.Duty[HandleMsgResult[M]] =
				if clarityVsEfficiency then {
					// clarity version
					val covenant = new thisAdmin.Covenant[HandleMsgResult[M]]
					msgHandlerExecutorService.executeMsgHandler(currentBehavior, message) { hmr => covenant.fulfill(hmr)() }
					covenant
				} else {
					// a bit more efficient version: creates one less object.
					var result: Maybe[HandleMsgResult[M]] = Maybe.empty
					var observer: Maybe[HandleMsgResult[M] => Unit] = Maybe.empty
					msgHandlerExecutorService.executeMsgHandler(currentBehavior, message) { hmr =>
						thisAdmin.queueForSequentialExecution {
							observer.fold {
								result = Maybe.some(hmr)
							}(_(hmr))
						}
					}
					new thisAdmin.Duty[HandleMsgResult[M]] {
						override def engage(onComplete: HandleMsgResult[M] => Unit): Unit =
							result.fold {
								observer = Maybe.some(onComplete)
							}(onComplete)
					}
				}

			handlesMessage.map {
				case cw: ContinueWith[M @unchecked] =>
					reactant.setBehavior(cw.behavior)
					Decision.continue
				case Ignore =>
					Decision.continue
				case Restart =>
					Decision.restart
				case Stop =>
					Decision.stop
				case Error(exceptionHandlerError, originalCause) =>
					thisAdmin.reportFailure(new ExceptionReport(s"The error handler of the behavior [$currentBehavior] terminated abruptly when it tried to handle [$originalCause], which was throw when handling the message [$message]", exceptionHandlerError))
					Decision.stop
			}
		}

		def processNextPendingMessages(): Duty[Decision] = {

			// Build a duty that, if there is no pending messages, returns `Maybe.some(continue)`;
			// else if there is a pending message, withdraws it, handles it, and if everything went OK updates the reactant's behavior, and returns `Maybe.empty`;
			// else (there were problems) returns `Maybe.some(decision)` where decision != continue.
			val processMessage: thisAdmin.Duty[Maybe[Decision]] =

				// withdraw the next pending message considering all the inboxes the reactant has.
				reactant.withdrawNextMessage()
					.castTypePath(thisAdmin)
					.flatMap { oNextMessage =>
						oNextMessage.fold {
							// if no pending message to process, return Maybe.some(continue) to exit the loop gracefully
							thisAdmin.Duty.ready(Maybe.some(Decision.continue))
						} { nextMessage =>
							// if a message was withdrawn, handle it, and return Maybe.empty to continue in the loop unless there were trouble.
							handleMsg(nextMessage, reactant.getCurrentBehavior).map { decision =>
								if decision == Decision.continue then Maybe.empty
								else Maybe.some(decision)
							}
						}
					}

			// repeat the `handlesMessageAndUpdatesBehavior` task until pending messages are exhausted.
			processMessage.repeatedUntilSome() { (count, hungryExhaustedOrAborted) => hungryExhaustedOrAborted }
		}

		reactant.setIsIdleState(false)

		// build a duty that handles all pending messages updating the behavior and then set the reactant's idle mark to true.
		val processAllPendingMessages: thisAdmin.Duty[Unit] =
			stimulator.withdraw().castTypePath(thisAdmin).flatMap { oFirstMessage =>
				handleMsg(oFirstMessage.get, reactant.getCurrentBehavior).flatMap { decision =>
					if decision eq Decision.continue then processNextPendingMessages()
					else thisAdmin.Duty.ready(decision)
				}
			}.foreach {
				case Decision.continue => reactant.setIsIdleState(true)
				case Decision.restart => reactant.restart()
				case Decision.stop => reactant.stop()
			}

		// Nothing happens until this point where the duty built above is executed.
		processAllPendingMessages.triggerAndForget()
	}
}
