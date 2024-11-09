package readren.matrix

import readren.taskflow.Doer.ExceptionReport
import readren.taskflow.Maybe

import scala.annotation.tailrec
import scala.collection.MapView
import scala.compiletime.uninitialized

object Reactant {
	type SerialNumber = Int

	private sealed trait Decision[+U]

	private case object ToContinue extends Decision[Nothing]

	private case class ToRestart[U](stopChildren: Boolean, restartBehaviorBuilder: Reactant[U] => Behavior[U]) extends Decision[U]

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
	override val admin: MatrixAdmin,
	initialBehaviorBuilder: Reactant[U] => Behavior[U],
	oMsgHandlerExecutorService: Maybe[MsgHandlerExecutorService]
) extends ReactantRelay[U] {

	/** should be accessed within the [[admin]]. */
	private var idleState = false
	private var isMarkedToStop = false
	private val stopCovenant = new admin.Covenant[Unit]

	private var oSpawner: Maybe[Spawner[admin.type]] = Maybe.empty
	/** Should be accessed withing the [[admin]] */
	private var childrenRelays: MapView[Long, ReactantRelay[?]] = MapView.empty
	override val endpointProvider: EndpointProvider[U]

	override val path: String = {
		val parentPath = progenitor.owner.fold(java.lang.StringBuilder())(pr => java.lang.StringBuilder(pr.path))
		parentPath.append('/').append(serialNumber).toString
	}

	/** Should be the last field to be initialized, in order to ensure that the `initialBehaviorBuilder` is executed with the [[Reactant]] fully initialized. */
	private var currentBehavior: Behavior[U] = uninitialized

	/**
	 * Should be called only once and within the [[admin]].
	 * Design note: This method is necessary to initialize the objects referenced by this [[Reactant]] that also need a reference to this [[Reactant]] after it is sufficiently initialized (e.g., [[currentBehavior]]). */
	def initialize(): this.type = { // send Started signal after all the vals and vars have been initialized
		assert(currentBehavior == null)
		currentBehavior = initialBehaviorBuilder(this)
		executeSignalHandler(Started).foreach(_ => sleepUntilNextMessageArrives())
			.triggerAndForget()
		this
	}

	/** Should be called withing the [[admin]]. */
	private def spawner: Spawner[admin.type] = oSpawner.fold {
		val spawner = new Spawner[admin.type](Maybe.some(this), admin, serialNumber)
		oSpawner = Maybe.some(spawner)
		childrenRelays = spawner.childrenView.filterNot(_._2.isMarkedToStop)
		spawner
	}(alreadyBuiltSpawner => alreadyBuiltSpawner)

	/** Should be called withing the [[admin]]. */
	override def spawn[A](childReactantFactory: ReactantFactory)(initialChildBehaviorBuilder: Reactant[A] => Behavior[A]): admin.Duty[ReactantRelay[A]] = {
		spawner.createReactant[A](childReactantFactory, initialChildBehaviorBuilder)
	}

	/** Should be called withing the [[admin]]. */
	override def getChildren: MapView[Long, ReactantRelay[?]] = childrenRelays

	/** @return true if there is no pending messages to process. */
	protected def noPendingMsg: Boolean

	/** is called within the [[admin]]. */
	protected def withdrawNextMessage(): Maybe[U]

	/** should be called within the [[admin]]. */
	inline def isIdle: Boolean = idleState

	/** should be called within the [[admin]]. */
	private final def sleepUntilNextMessageArrives(): Unit = {
		assert(!isIdle)
		if noPendingMsg then idleState = true
		else stimulate(withdrawNextMessage().get)
	}

	/** should be called within the [[admin]]. */
	private final def executeSignalHandler(signal: Signal): admin.Duty[HandleResult[U]] = {
		oMsgHandlerExecutorService.fold(admin.Duty.ready(currentBehavior.handleSignal(signal))) { msgHandlerExecutorService =>
			val covenant = new admin.Covenant[HandleResult[U]]
			msgHandlerExecutorService.executeSignalHandler(currentBehavior, signal)(hr => covenant.fulfill(hr)())
			covenant
		}
	}

	/** should be called within the [[admin]]. */
	private final def restart(stopChildren: Boolean, restartBehaviorBuilder: Reactant[U] => Behavior[U]): admin.Duty[Unit] = {
		def restartMe(): admin.Duty[Unit] = {
			// send RestartReceived signal
			executeSignalHandler(RestartReceived).foreach {
				case Stop => // if the `handleSignal` responds `Stop` to the `RestartReceived` signal, then the restart is canceled and the reactant is stopped instead.
					stopInternal()
				case error@Error(exceptionHandlerError, originalCause) => // if the `handleSignal` responds `Error` to the `RestartReceived` signal, then the restart is canceled and the reactant is stopped instead.
					admin.reportFailure(new ExceptionReport(s"The error handler of the behavior [$currentBehavior] terminated abruptly when it tried to handle [$originalCause], which was throw when handling the signal [$RestartReceived]", exceptionHandlerError))
					stopInternal()
				case _ =>
					// change the behavior before signaling with Restarted
					currentBehavior = restartBehaviorBuilder(this)
					// send the Restarted signal
					executeSignalHandler(Restarted)
					admin.Duty.ready(sleepUntilNextMessageArrives())
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

	override def stopDuty: admin.Duty[Unit] = stopCovenant.duty

	/**
	 * Instructs to stop this [[Reactant]].
	 * Supports being called at any moment.
	 * thread-safe
	 * @return a [[Duty]] that completes when this [[Reactant]] is fully stopped. */
	override final def stop(): admin.Duty[Unit] = {
		admin.Duty.mineFlat { () =>
			if isIdle then {
				idleState = false
				stopInternal()
			} else {
				isMarkedToStop = true
				stopCovenant
			}
		}
	}

	/**
	 * Stops this [[Reactant]].
	 * Should be called within the [[admin]].
	 * @return a [[Duty]] that completes when this [[Reactant]] is fully stopped. */
	private final def stopInternal(): admin.Duty[Unit] = {
		/** should be called within the [[admin]]. */
		def stopMe(): admin.Duty[Unit] = {
			// execute the StopReceived
			executeSignalHandler(StopReceived).foreach { _ => // note that the result of the `signalHandler` is ignored.
				// remove myself form progenitor children
				progenitor.removeChild(serialNumber)
				stopCovenant.fulfill(())()

				// TODO notify parent
			}
		}

		idleState = false
		isMarkedToStop = true
		oSpawner.fold(stopMe()) { spawner =>
			spawner.stopChildren().flatMap(_ => stopMe())
		}
	}

	/** Should be called within this [[MatrixAdmin]] and only if this reactant is idle. */
	final def stimulate(firstMessage: U): Unit = {
		assert(idleState && !isMarkedToStop)

		def mapHmrToDecision(message: U, behavior: Behavior[U])(hmr: HandleResult[U]): Decision[U] = hmr match {
			case cw: ContinueWith[U @unchecked] =>
				currentBehavior = cw.behavior
				ToContinue
			case Continue =>
				ToContinue
			case Stop =>
				ToStop
			case Restart =>
				ToRestart(true, initialBehaviorBuilder)
			case rw: RestartWith[U] =>
				ToRestart[U](false, _ => rw.behavior)
			case Unhandled =>
				// TODO log it  
				ToContinue
			case Error(exceptionHandlerError, originalCause) =>
				admin.reportFailure(new ExceptionReport(s"The error handler of the behavior [$behavior] terminated abruptly when it tried to handle [$originalCause], which was throw when handling the message [$message]", exceptionHandlerError))
				ToStop
		}

		inline def processMessagesWithSpecifiedExecutor(msgHandlerExecutorService: MsgHandlerExecutorService): Unit = {

			/** should be called within the admin */
			def handleMsg(message: U, behavior: Behavior[U]): admin.Duty[Decision[U]] = {
				val covenant = new admin.Covenant[HandleResult[U]]
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
						restart(tr.stopChildren, tr.restartBehaviorBuilder)
				}
			// Nothing happens until here where the duty built above is executed.
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
				case ToStop => stopInternal().triggerAndForget(true)
				case tr: ToRestart[U @unchecked] => restart(tr.stopChildren, tr.restartBehaviorBuilder).triggerAndForget(true)
			}
		}

		// First line of the outer method.
		idleState = false
		oMsgHandlerExecutorService.fold(processMessagesWithAdmin())(processMessagesWithSpecifiedExecutor)
	}
}
