package readren.matrix

import Reactant.SerialNumber

import readren.taskflow.Doer.ExceptionReport
import readren.taskflow.Maybe
import java.net.URI
import scala.annotation.tailrec

object Reactant {
	type SerialNumber = Int

	private enum Decision {
		case continue, restart, stop
	}
}

/**
 *
 * @param admin the [[MatrixAdmin]] assigned to this instance.
 * @param oMsgHandlerExecutorService the [[ExecutorService]] used for executing the [[Behavior.handleMessage]] method.
 * @tparam U the type of the messages this reactant understands.
 */
abstract class Reactant[U](
	serialNumber: SerialNumber,
	progenitor: Spawner[MatrixAdmin],
	canSpawn: Boolean,
	val admin: MatrixAdmin,
	initialBehavior: Behavior[U],
	oMsgHandlerExecutorService: Maybe[MsgHandlerExecutorService]
) {

	import Reactant.Decision

	//	/** Identifies this [[Reactant]] among its siblings (those created by the same [[Progenitor]]). */
	//	val serialNumber: Reactant.SerialNumber
	//
	//	/** The progenitor of this instance. */
	//	val progenitor: Progenitor

	/** should be accessed within the [[admin]]. */
	private var idleState = true
	private var currentBehavior: Behavior[U] = initialBehavior

	val oSpawner: Maybe[Spawner[admin.type]] =
		if canSpawn then Maybe.some(new Spawner(Maybe.some(this), admin, serialNumber))
		else Maybe.empty

	val endpointProvider: EndpointProvider[U]

	/** should be called within the [[admin]]. */
	def isIdle: Boolean = idleState

	/** is called within the [[admin]]. */
	protected def withdrawNextMessage(): Maybe[U]

	val path: String = {
		val parentPath = progenitor.owner.fold(java.lang.StringBuilder())(pr => java.lang.StringBuilder(pr.path))
		parentPath.append('/').append(serialNumber).toString
	}

	final def restart(): admin.Duty[Unit] = ???

	/** thread-safe */
	final def stop(): admin.Duty[Unit] = {
		def stopMe(): admin.Duty[Unit] = {
			// execute the preStop
			oMsgHandlerExecutorService.fold(admin.Duty.ready(currentBehavior.handleSignal(PreStop))) { msgHandlerExecutorService =>
				val covenant = new admin.Covenant[Unit]
				msgHandlerExecutorService.executeSignalHandler(currentBehavior, PreStop)(() => covenant.fulfill(())())
				covenant
			}.foreach { _ =>
				// remove myself form progenitor children
				progenitor.removeChild(serialNumber)
				
				// TODO notify parent
			}
		}

		admin.Duty.mine { () => oSpawner }
			.flatMap { oSpawner =>
				oSpawner.fold(stopMe()) { spawner =>
					spawner.stopChildren().flatMap(_ => stopMe())
				}
			}
	}

	/** Should be called within this [[MatrixAdmin]] and only if this reactant is idle. */
	final def stimulate(firstMessage: U): Unit = {
		assert(idleState)

		def mapHmrToDecision(message: U, behavior: Behavior[U])(hmr: HandleMsgResult[U]): Decision = hmr match {
			case cw: ContinueWith[U @unchecked] =>
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

		inline def processMessagesWithSpecifiedExecutor(msgHandlerExecutorService: MsgHandlerExecutorService): Unit = {

			def handleMsg(message: U, behavior: Behavior[U]): admin.Duty[Decision] = {
				val covenant = new admin.Covenant[HandleMsgResult[U]]
				msgHandlerExecutorService.executeMsgHandler(behavior, message) { hmr => covenant.fulfill(hmr)() }
				covenant.map(mapHmrToDecision(message, behavior))
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

			// build a duty that handles all pending messages updating the behavior and then set the reactant's idle mark to true.
			val processAllPendingMessages: admin.Duty[Unit] =
				handleMsg(firstMessage, currentBehavior).flatMap { decision =>
					if decision eq Decision.continue then processNextPendingMessages()
					else admin.Duty.ready(decision)
				}.flatMap {
					case Decision.continue =>
						idleState = true
						admin.Duty.ready(())
					case Decision.restart => restart()
					case Decision.stop => stop()
				}

			// Nothing happens until this point where the duty built above is executed.
			processAllPendingMessages.triggerAndForget()
		}

		inline def processMessagesWithAdmin(): Unit = {

			def handleMsg(message: U, behavior: Behavior[U]): Decision = {
				mapHmrToDecision(message, behavior)(behavior.handleMessage(message))
			}

			@tailrec
			def processNextPendingMessages(): Decision = {
				withdrawNextMessage().fold(Decision.continue) { message =>
					val decision = handleMsg(message, currentBehavior)
					if decision eq Decision.continue then processNextPendingMessages()
					else decision
				}
			}

			val firstDecision = handleMsg(firstMessage, currentBehavior)
			val finalDecision =
				if firstDecision eq Decision.continue then processNextPendingMessages()
				else firstDecision
			finalDecision match {
				case Decision.continue => idleState = true
				case Decision.restart => restart()
				case Decision.stop => stop()
			}
		}

		idleState = false

		oMsgHandlerExecutorService.fold(processMessagesWithAdmin())(processMessagesWithSpecifiedExecutor)
	}
}
