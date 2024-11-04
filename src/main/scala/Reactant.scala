package readren.matrix

import readren.taskflow.Doer.ExceptionReport
import readren.taskflow.Maybe

object Reactant {
	type SerialNumber = Long

	private enum Decision {
		case continue, restart, stop
	}
}

/**
 *
 * @param admin the [[MatrixAdmin]] assigned to this instance.
 * @param msgHandlerExecutorService the [[ExecutorService]] used for executing the [[Behavior.handleMessage]] method. 
 * @tparam M the type of the messages this reactant understands.
 */
abstract class Reactant[M](val admin: MatrixAdmin, msgHandlerExecutorService: MsgHandlerExecutorService, initialBehavior: Behavior[M]) {

	import Reactant.Decision

	//	/** Identifies this [[Reactant]] among its siblings (those created by the same [[Progenitor]]). */
	//	val serialNumber: Reactant.SerialNumber
	//
	//	/** The progenitor of this instance. */
	//	val progenitor: Progenitor

	/** should be accessed within the [[admin]]. */
	private var idleState = true
	private var currentBehavior: Behavior[M] = initialBehavior

	/** should be called within the [[admin]]. */
	def isIdle: Boolean = idleState

	/** is called within the [[admin]]. */
	protected def withdrawNextMessage(): Maybe[M]

	//	def ask[Q, R <: M](question: Q, inbox: InboxBackend[R]): Unit

	/** should be called within the [[admin]]. */
	protected def restart(): Unit

	/** should be called within the [[admin]]. */
	protected def stop(): Unit


	/** Should be called within this [[MatrixAdmin]] and only if this reactant is idle. */
	def stimulate(firstMessage: M): Unit = {
		assert(idleState)

		def handleMsg(message: M, behavior: Behavior[M]): admin.Duty[Decision] = {
			inline val clarityVsEfficiency = false
			val handlesMessage: admin.Duty[HandleMsgResult[M]] =
				if clarityVsEfficiency then {
					// clarity version
					val covenant = new admin.Covenant[HandleMsgResult[M]]
					msgHandlerExecutorService.executeMsgHandler(behavior, message) { hmr => covenant.fulfill(hmr)() }
					covenant
				} else {
					// a bit more efficient version: creates one less object.
					var result: Maybe[HandleMsgResult[M]] = Maybe.empty
					var observer: Maybe[HandleMsgResult[M] => Unit] = Maybe.empty
					msgHandlerExecutorService.executeMsgHandler(behavior, message) { hmr =>
						admin.queueForSequentialExecution {
							observer.fold {
								result = Maybe.some(hmr)
							}(_(hmr))
						}
					}
					new admin.Duty[HandleMsgResult[M]] {
						override def engage(onComplete: HandleMsgResult[M] => Unit): Unit =
							result.fold {
								observer = Maybe.some(onComplete)
							}(onComplete)
					}
				}

			handlesMessage.map {
				case cw: ContinueWith[M @unchecked] =>
					currentBehavior = cw.behavior
					Decision.continue
				case Continue =>
					Decision.continue
				case Restart =>
					Decision.restart
				case Stop =>
					Decision.stop
				case Error(exceptionHandlerError, originalCause) =>
					admin.reportFailure(new ExceptionReport(s"The error handler of the behavior [$behavior] terminated abruptly when it tried to handle [$originalCause], which was throw when handling the message [$message]", exceptionHandlerError))
					Decision.stop
			}
		}

		def processNextPendingMessages(): admin.Duty[Decision] = {

			// Build a duty that, if there is no pending messages, returns `Maybe.some(continue)`;
			// else if there is a pending message, withdraws it, handles it, and if everything went OK updates the reactant's behavior, and returns `Maybe.empty`;
			// else (there were problems) returns `Maybe.some(decision)` where decision != continue.
			val processMessage: admin.Duty[Maybe[Decision]] =

				// withdraw the next pending message considering all the inboxes the reactant has.
				withdrawNextMessage().fold {
					// if no pending message to process, return Maybe.some(continue) to exit the loop gracefully
					admin.Duty.ready(Maybe.some(Decision.continue))
				} { nextMessage =>
					// if a message was withdrawn, handle it, and return Maybe.empty to continue in the loop unless there were trouble.
					handleMsg(nextMessage, currentBehavior).map { decision =>
						if decision == Decision.continue then Maybe.empty
						else Maybe.some(decision)
					}
				}


			// repeat the `handlesMessageAndUpdatesBehavior` task until pending messages are exhausted.
			processMessage.repeatedUntilSome() { (count, hungryExhaustedOrAborted) => hungryExhaustedOrAborted }
		}

		idleState = false

		// build a duty that handles all pending messages updating the behavior and then set the reactant's idle mark to true.
		val processAllPendingMessages: admin.Duty[Unit] =
			handleMsg(firstMessage, currentBehavior).flatMap { decision =>
				if decision eq Decision.continue then processNextPendingMessages()
				else admin.Duty.ready(decision)
			}.foreach {
				case Decision.continue => idleState = true
				case Decision.restart => restart()
				case Decision.stop => stop()
			}

		// Nothing happens until this point where the duty built above is executed.
		processAllPendingMessages.triggerAndForget()
	}
}
