package readren.matrix

import readren.taskflow.Doer.ExceptionReport
import readren.taskflow.Maybe

import scala.annotation.tailrec

object Reactant {
	type SerialNumber = Int

	private sealed trait Decision[+U]
	private case object ToContinue extends Decision[Nothing]
	private case class ToRestart[U](stopChildren: Boolean, restartBehavior: Behavior[U]) extends Decision[U]
	private case object ToStop extends Decision[Nothing]
}

import Reactant.*

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


	//	/** Identifies this [[Reactant]] among its siblings (those created by the same [[Progenitor]]). */
	//	val serialNumber: Reactant.SerialNumber
	//
	//	/** The progenitor of this instance. */
	//	val progenitor: Progenitor

	/** should be accessed within the [[admin]]. */
	private var idleState = false
	private var currentBehavior: Behavior[U] = initialBehavior
	private var isMarkedToStop = false
	private val stopCovenant = new admin.Covenant[Unit]

	val oSpawner: Maybe[Spawner[admin.type]] =
		if canSpawn then Maybe.some(new Spawner(Maybe.some(this), admin, serialNumber))
		else Maybe.empty

	val endpointProvider: EndpointProvider[U]

	val path: String = {
		val parentPath = progenitor.owner.fold(java.lang.StringBuilder())(pr => java.lang.StringBuilder(pr.path))
		parentPath.append('/').append(serialNumber).toString
	}

	/** @return true if there is no pending messages to process. */
	protected def noPendingMsg: Boolean

	/** is called within the [[admin]]. */
	protected def withdrawNextMessage(): Maybe[U]

	/** should be called within the [[admin]]. */
	inline def isIdle: Boolean = idleState

	{	// send Started signal
		admin.Duty.mineFlat{ ()=> executeSignalHandler(Started).foreach(_ => sleepUntilNextMessageArrives()) }
			.triggerAndForget()
	}


	/** should be called within the [[admin]]. */
	private final def sleepUntilNextMessageArrives(): Unit = {
		assert(!isIdle)
		if noPendingMsg then idleState = true
		else stimulate(withdrawNextMessage().get)
	}

	/** should be called within the [[admin]]. */
	private final def executeSignalHandler(signal: Signal): admin.Duty[Unit] = {
		oMsgHandlerExecutorService.fold(admin.Duty.ready(currentBehavior.handleSignal(signal))) { msgHandlerExecutorService =>
			val covenant = new admin.Covenant[Unit]
			msgHandlerExecutorService.executeSignalHandler(currentBehavior, signal)(() => covenant.fulfill(())())
			covenant
		}
	}

	/** should be called within the [[admin]]. */
	private final def restart(stopChildren: Boolean, restartBehavior: Behavior[U]): admin.Duty[Unit] = {
		def restartMe(): admin.Duty[Unit] = {
			// send RestartReceived signal
			executeSignalHandler(RestartReceived).andThen { _ =>
				// change the behavior before signaling with Restarted
				currentBehavior = restartBehavior
				// send the Restarted signal
				executeSignalHandler(Restarted)
				sleepUntilNextMessageArrives()
				// TODO notify parent
			}
		}

		idleState = false
		if stopChildren then {
			oSpawner.fold(restartMe()) { spawner =>
				spawner.stopChildren().flatMap(_ => restartMe())
			}
		} else restartMe()
	}

	/**
	 * Instructs to stop this [[Reactant]].
	 * Supports being called at any moment.
	 * thread-safe
	 * @return a [[Duty]] that completes when this [[Reactant]] is fully stopped. */
	final def stop(): admin.Duty[Unit] = {
		isMarkedToStop = true
		if isIdle then {
			idleState = false
			stopInternal()
		} else stopCovenant
	}

	/**
	 * Stops this [[Reactant]].
	 * Should be called within the [[admin]].
	 * @return a [[Duty]] that completes when this [[Reactant]] is fully stopped. */
	private final def stopInternal(): admin.Duty[Unit] = {
		/** should be called within the [[admin]]. */
		def stopMe(): admin.Duty[Unit] = {
			// execute the StopReceived
			executeSignalHandler(StopReceived).foreach { _ =>
				// remove myself form progenitor children
				progenitor.removeChild(serialNumber)
				stopCovenant.fulfill(())()

				// TODO notify parent
			}
		}

		idleState = false
		oSpawner.fold(stopMe()) { spawner =>
			spawner.stopChildren().flatMap(_ => stopMe())
		}
	}

	/** Should be called within this [[MatrixAdmin]] and only if this reactant is idle. */
	final def stimulate(firstMessage: U): Unit = {
		assert(idleState && !isMarkedToStop)

		def mapHmrToDecision(message: U, behavior: Behavior[U])(hmr: HandleMsgResult[U]): Decision[U] = hmr match {
			case cw: ContinueWith[U @unchecked] =>
				currentBehavior = cw.behavior
				ToContinue
			case Continue =>
				ToContinue
			case Stop =>
				ToStop
			case Restart(stopChildren) =>
				ToRestart(stopChildren, initialBehavior)
			case rw: RestartWith[U] =>
				ToRestart[U](rw.stopChildren, rw.behavior)
			case Error(exceptionHandlerError, originalCause) =>
				admin.reportFailure(new ExceptionReport(s"The error handler of the behavior [$behavior] terminated abruptly when it tried to handle [$originalCause], which was throw when handling the message [$message]", exceptionHandlerError))
				ToStop
		}

		inline def processMessagesWithSpecifiedExecutor(msgHandlerExecutorService: MsgHandlerExecutorService): Unit = {

			/** should be called within the admin */
			def handleMsg(message: U, behavior: Behavior[U]): admin.Duty[Decision[U]] = {
				val covenant = new admin.Covenant[HandleMsgResult[U]]
				msgHandlerExecutorService.executeMsgHandler(behavior, message) { hmr => covenant.fulfill(hmr)() }
				covenant.map(mapHmrToDecision(message, behavior))
			}

			/** should be called within the admin */
			def processNextPendingMessages(): admin.Duty[Decision[U]] = {

				// Build a duty that, if there is no pending messages, returns `Maybe.some(continue)`;
				// else if there is a pending message, withdraws it, handles it, and if everything went OK updates the reactant's behavior, and returns `Maybe.empty`;
				// else (there were problems) returns `Maybe.some(decision)` where decision != continue.
				val processMessage: admin.Duty[Maybe[Decision[U]]] =

					// withdraw the next pending message considering all the inboxes the reactant has.
					withdrawNextMessage().fold {
						// if no pending message to process, return Maybe.some(continue) to exit the loop gracefully
						admin.Duty.ready(Maybe.some(ToContinue))
					} { nextMessage =>
						// if a message was withdrawn, handle it, and return Maybe.empty to continue in the loop unless there were trouble.
						handleMsg(nextMessage, currentBehavior).map { decision =>
							if isMarkedToStop then Maybe.some(ToStop)
							else if decision == ToContinue then Maybe.empty
							else Maybe.some(decision)
						}
					}


				// repeat the `handlesMessageAndUpdatesBehavior` task until pending messages are exhausted.
				processMessage.repeatedUntilSome() { (count, hungryExhaustedOrAborted) => hungryExhaustedOrAborted }
			}

			// build a duty that handles all pending messages updating the behavior and then set the reactant's idle mark to true.
			val processAllPendingMessages: admin.Duty[Unit] =
				handleMsg(firstMessage, currentBehavior).flatMap { decision =>
					if isMarkedToStop then admin.Duty.ready(ToStop)
					else if decision eq ToContinue then processNextPendingMessages()
					else admin.Duty.ready(decision)
				}.flatMap {
					case ToContinue =>
						idleState = true
						admin.Duty.ready(())
					case ToStop =>
						stopInternal()
					case tr: ToRestart[U @unchecked] =>
						restart(tr.stopChildren, tr.restartBehavior)
				}

			// Nothing happens until this point where the duty built above is executed.
			processAllPendingMessages.triggerAndForget()
		}

		inline def processMessagesWithAdmin(): Unit = {

			/** should be called within the admin */
			def handleMsg(message: U, behavior: Behavior[U]): Decision[U] = {
				mapHmrToDecision(message, behavior)(behavior.handleMessage(message))
			}

			/** should be called within the admin */
			@tailrec
			def processNextPendingMessages(): Decision[U] = {
				withdrawNextMessage().fold(ToContinue) { message =>
					val decision = handleMsg(message, currentBehavior)
					if isMarkedToStop then ToStop
					else if decision eq ToContinue then processNextPendingMessages()
					else decision
				}
			}

			val firstDecision = handleMsg(firstMessage, currentBehavior)
			val finalDecision =
				if isMarkedToStop then ToStop
				else if firstDecision eq ToContinue then processNextPendingMessages()
				else firstDecision
			finalDecision match {
				case ToContinue => idleState = true
				case ToStop => stopInternal()
				case tr: ToRestart[U @unchecked] => restart(tr.stopChildren, tr.restartBehavior)
			}
		}

		// First line of the outer method.
		idleState = false
		oMsgHandlerExecutorService.fold(processMessagesWithAdmin())(processMessagesWithSpecifiedExecutor)
	}
}
