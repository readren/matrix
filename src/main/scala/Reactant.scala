package readren.matrix

import readren.taskflow.Maybe

import scala.annotation.tailrec
import scala.collection.MapView
import scala.compiletime.uninitialized

object Reactant {
	type SerialNumber = Int

	private sealed trait Decision[+U]

	private object ToContinue extends Decision[Nothing]

	private class ToRestart[U](val stopChildren: Boolean, val restartBehaviorBuilder: ReactantRelay[U] => Behavior[U]) extends Decision[U]

	private object ToStop extends Decision[Nothing]
}

import Reactant.*

/**
 *
 * @param admin the [[MatrixAdmin]] assigned to this instance.
 * @tparam U the type of the messages this reactant understands.
 */
abstract class Reactant[U](
	val serial: SerialNumber,
	progenitor: Spawner[MatrixAdmin],
	override val admin: MatrixAdmin,
	isSignalTest: IsSignalTest[U],
	initialBehaviorBuilder: ReactantRelay[U] => Behavior[U]
) extends ReactantRelay[U] { thisReactant =>

	/**
	 * The initial state is `false` (not ready).
	 * Is set to `true` after consuming all the pending messages (of this reactant's [[Inbox]]) if the result of [[Behavior.handle]] for the last message returned [[Continue]] or [[ContinueWith]]; and the stop process was not started (e.g. the [[stopWasStarted]] is false).
	 * Is set to `false` when [[stopWasStarted]] is set to `true` or after [[onInboxBecomesNonempty]] is called.
	 * Its purpose is to avoid consuming messages while this [[Reactant]] is starting, restarting, or stopping.
	 * It is set to true only by the [[beReadyToProcess()]] method, which ensures all pending messages are processed before the transition to `true`.
	 * This flag should be the only one that determines when "inbox becomes nonempty" notifications (calls to [[onInboxBecomesNonempty]]) are ignored in order to compensate the ignored notifications when it is set to `true`.
	 * Should be accessed within the [[admin]].
	 * */
	private var isReadyToProcessMsg: Boolean = false

	/** Tells if this [[Reactant]] was marked to be stopped.
	 * Is set to true by the [[Reactant.stop]] method which can be called at any moment .
	 * It can't be cleared. Once it is true it will remain true forever (until it is garbage-collected).
	 * It is volatile to achieve its only purpose: to avoid processing the next pending messages after [[stop]] was called from outside the [[admin]] thread; otherwise the [[stopWasStarted]] would be sufficient. */
	@volatile private var isMarkedToStop: Boolean = false

	/** Tells if the stop process was already started.
	 * Is set to true by the [[Reactant.selfStop]] method which is called withing the [[admin]].
	 * It can't be cleared. Once it is true it will remain true forever (until it is garbage-collected).
	 * Its purpose is to avoid the [[processMessages()]] be called after the stop process has started.
	 * Should be accessed within the [[admin]] only.
	 * */
	private var stopWasStarted = false

	private val stopCovenant = new admin.Covenant[Unit]

	private var oSpawner: Maybe[Spawner[admin.type]] = Maybe.empty
	/** Should be accessed withing the [[admin]] */
	private var childrenRelays: MapView[Long, ReactantRelay[?]] = MapView.empty

	override val endpointProvider: EndpointProvider[U]

	override val path: String = {
		val parentPath = progenitor.owner.fold(java.lang.StringBuilder())(pr => java.lang.StringBuilder(pr.path))
		parentPath.append('/').append(serial).toString
	}

	/** Should be the last field to be initialized, in order to ensure that the `initialBehaviorBuilder` is executed with the [[Reactant]] fully initialized. */
	private var currentBehavior: Behavior[U] = uninitialized

	protected val inbox: Inbox[U]

	/**
	 * Should be called only once and within the [[admin]].
	 * Design note: This method is necessary to initialize the objects referenced by this [[Reactant]] that also need a reference to this [[Reactant]] after it is sufficiently initialized (e.g., [[currentBehavior]]). */
	def initialize(): admin.Duty[this.type] = { // send Started signal after all the vals and vars have been initialized
		admin.checkWithin()
		assert(currentBehavior == null)
		selfStart(false, initialBehaviorBuilder).map(_ => thisReactant) // TODO considerar hacer que selfStart devuelva Duty[this.type] para evitar este 'map`  del final. Esto requiere que selfStop, selfRestar, stayIdleUntilNextMessageArrive, y otros que ahora devuelven Duty[Unit] también hagan lo mismo.
	}

	/** Starts or restarts this [[Reactant]].
	 * Should be called only once and within the [[admin]].
	 * */
	private def selfStart(comesFromRestart: Boolean, behaviorBuilder: ReactantRelay[U] => Behavior[U]): admin.Duty[Unit] = {
		admin.checkWithin()
		currentBehavior = behaviorBuilder(thisReactant)
		val handleResult = handleSignal(if comesFromRestart then isSignalTest.restarted else isSignalTest.started)
		mapHrToDecision(handleResult) match {
			case ToContinue =>
				if !stopWasStarted then beReadyToProcess()
				admin.dutyReadyUnit
			case ToStop =>
				selfStop()
			case tr: ToRestart[U @unchecked] =>
				selfRestart(tr.stopChildren, tr.restartBehaviorBuilder)
		}
	}

	/** Should be called withing the [[admin]]. */
	override def spawn[A](childReactantFactory: ReactantFactory)(initialChildBehaviorBuilder: ReactantRelay[A] => Behavior[A])(using isSignalTest: IsSignalTest[A]): admin.Duty[ReactantRelay[A]] = {
		admin.checkWithin()
		oSpawner.fold {
				val spawner = new Spawner[admin.type](Maybe.some(thisReactant), admin, serial)
				oSpawner = Maybe.some(spawner)
				childrenRelays = spawner.childrenView
				spawner
			}(alreadyBuiltSpawner => alreadyBuiltSpawner)
			.createReactant[A](childReactantFactory, isSignalTest, initialChildBehaviorBuilder)
	}

	/** The children of this [[Reactant]] by serial number.
	 *
	 * Should be called withing the [[admin]]. */
	override def children: MapView[Long, ReactantRelay[?]] = {
		admin.checkWithin()
		childrenRelays
	}

	/** should be called within the [[admin]]. */
	private final def selfRestart(stopChildren: Boolean, restartBehaviorBuilder: ReactantRelay[U] => Behavior[U]): admin.Duty[Unit] = {
		admin.checkWithin()

		def restartMe(): admin.Duty[Unit] = {
			// send RestartReceived signal
			val hr = handleSignal(isSignalTest.restartReceived)
			mapHrToDecision(hr) match {
				case ToContinue => selfStart(true, restartBehaviorBuilder)
				case ToStop =>
					// if the `handleSignal` responds `Stop` to the `RestartReceived` signal, then the restart is canceled and the reactant is stopped instead, which provokes the signal handler be called again with a `StopReceived` signal.
					selfStop()
				case tr: ToRestart[U @unchecked] =>
					// if the `handleSignal` responds `Restart` or `RestartWith` to the `RestartReceived` signal, then the restart is adapted to the new restart settings: stops children if they were not, and replaces the restartBehaviorBuilder for the new one. The signal handler is NOT called again.
					val stopsChildrenIfInstructed =
						if tr.stopChildren && !stopChildren then {
							oSpawner.fold(admin.dutyReadyUnit) { spawner =>
								spawner.stopChildren()
							}
						}
						else admin.dutyReadyUnit
					stopsChildrenIfInstructed.flatMap(_ => selfStart(true, tr.restartBehaviorBuilder))
			}
		}

		if stopChildren then {
			oSpawner.fold(restartMe()) { spawner =>
				spawner.stopChildren().flatMap(_ => restartMe())
			}
		} else restartMe()
	}

	override def isMarkedToBeStopped: Boolean = isMarkedToStop

	override def stopDuty: admin.Duty[Unit] = stopCovenant.duty

	override def watchChild[CSM <: U](childSerial: SerialNumber, childStoppedSignal: CSM): Boolean = {
		admin.checkWithin()
		oSpawner.fold(false) { spawner =>
			Maybe(spawner.childrenView.getOrElse(childSerial, null)).fold(false) { child =>
				watch(child, childStoppedSignal)
				true
			}
		}
	}

	override def watch[CSM <: U](watchedReactant: ReactantRelay[?], stoppedSignal: CSM): Unit = {
		watchedReactant.stopDuty.onBehalfOf(admin).foreach { _ =>
			// if a stop of this reactant is in progress, ignore the notification.
			if thisReactant.stopWasStarted then ()
			else {
				val hr = thisReactant.currentBehavior.handle(stoppedSignal)
				mapHrToDecision(hr) match {
					case ToContinue => ()
					case ToStop => selfStop()
					case tr: ToRestart[U @unchecked] => selfRestart(tr.stopChildren, tr.restartBehaviorBuilder).triggerAndForget(true)
				}
			}
		}.triggerAndForget(true)
	}


	override final def stop(): admin.Duty[Unit] = {
		// Note that if [[stop]] is called simultaneously from many threads, the [[selfStop]] duty might be triggered more than once, but that is not harmful because it discards repetitions.
		// As far as this "if" is concerned, mutations of the `isMarkedToStop` flag do not need to be atomic.
		if !isMarkedToStop then {
			isMarkedToStop = true
			admin.queueForSequentialExecution(selfStop())
		}
		stopCovenant.duty
	}

	/**
	 * Stops this [[Reactant]].
	 * Should be called within the [[admin]].
	 * Supports being called more than one time.
	 * It is not necessary to trigger the execution of the returned [[Duty]] to start the stop process. The result can be ignored.
	 * @return a [[Duty]] that completes when this [[Reactant]] is fully stopped. */
	private final def selfStop(): admin.Duty[Unit] = {
		admin.checkWithin()

		/** should be called within the [[admin]]. */
		def stopMe(): Unit = {
			// execute the signal handler and ignore its result
			handleSignal(isSignalTest.stopReceived)
			// remove myself form progenitor children
			progenitor.admin.queueForSequentialExecution {
				progenitor.removeChild(thisReactant.serial)
				stopCovenant.fulfill(())()
			}
			// TODO notify parent
		}

		if !stopWasStarted then {
			stopWasStarted = true
			isReadyToProcessMsg = false
			oSpawner.fold(stopMe()) { spawner =>
				spawner.stopChildren().trigger(true)(_ => stopMe())
			}
		}
		stopCovenant.duty
	}

	private inline def handleSignal(signal: Option[U]): HandleResult[U] = {
		signal.fold(Continue)(currentBehavior.handle)
	}


	private final def mapHrToDecision(hr: HandleResult[U]): Decision[U] = {
		admin.checkWithin()
		hr match {
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
		}
	}

	/** Sets the "ready to process messages" flag of this reactant after processing all messages that were submitted to the [[Receiver]] but are not jet visible in the [[Inbox]].
	 * This is the only method that sets the [[isReadyToProcessMsg]] flag to true.
	 * Since the "inbox becomes nonempty" notifications from the [[Receiver]] are ignored while its value was false, the transition to `true` should be done after ensuring all pending messages were processed.
	 * Should be called withing the [[admin]] only. */
	private def beReadyToProcess(): Unit = {
		admin.checkWithin()
		assert(!stopWasStarted && !isReadyToProcessMsg)
		if inbox.maybeNonEmpty then admin.queueForSequentialExecution {
			if !stopWasStarted then {
				inbox.withdraw().fold(beReadyToProcess())(processMessages)
			}
		} else isReadyToProcessMsg = true
	}

	/** Should be called by the [[Receiver]] whenever it receives a message while the [[Inbox]] is empty.
	 * Differs from the other variant in that this method is designed for implementations of the [[Receiver]] which queue messages concurrently (not within the [[admin]] of this reactant).
	 * Should be called within the [[admin]] only.
	 * */
	final def onInboxBecomesNonempty(): Unit = {
		admin.checkWithin()
		if isReadyToProcessMsg then {
			isReadyToProcessMsg = false
			inbox.withdraw().fold(beReadyToProcess())(processMessages)
		}
	}

	/** Should be called by the [[Receiver]] whenever it receives a message while the [[Inbox]] is empty.
	 * Differs from the other variant in that this method is designed for implementations of the [[Receiver]] which queue messages within the [[admin]] of this reactant.
	 * Should be called within the [[admin]] only.
	 * @param firstMsg the message received while the inbox was empty.
	 * @return true if the received message was not processed and should be queued in the inbox; false if it was processed. */
	final def onInboxBecomesNonempty(firstMsg: U): Boolean = {
		admin.checkWithin()
		if isReadyToProcessMsg then {
			isReadyToProcessMsg = false
			processMessages(firstMsg)
			false
		} else true
	}

	/** Process the received message and all the pending messages queue in the [[Reactant.inbox]]
	 * Should be called within the admin */
	private final def processMessages(firstMessage: U): Unit = {
		admin.checkWithin()

		inline def handleMsg(message: U, behavior: Behavior[U]): Decision[U] = mapHrToDecision(behavior.handle(message))

		@tailrec
		def processPendingMessages(): Decision[U] = {
			inbox.withdraw().fold(ToContinue) { message =>
				val decision = handleMsg(message, currentBehavior)
				if isMarkedToStop then ToStop
				else if decision eq ToContinue then processPendingMessages()
				else decision
			}
		}

		// First line of the outer method.
		val firstDecision = handleMsg(firstMessage, currentBehavior)
		val finalDecision =
			if isMarkedToStop then ToStop
			else if firstDecision eq ToContinue then processPendingMessages()
			else firstDecision
		finalDecision match {
			case ToContinue => beReadyToProcess()
			case ToStop => selfStop()
			case tr: ToRestart[U @unchecked] => selfRestart(tr.stopChildren, tr.restartBehaviorBuilder).triggerAndForget(true)
		}
	}


	override def diagnose: admin.Duty[ReactantDiagnostic] =
		admin.Duty.mine { () =>
			ReactantDiagnostic(thisReactant.isReadyToProcessMsg, thisReactant.isMarkedToStop, thisReactant.stopWasStarted, inbox.maybeNonEmpty, inbox.size, inbox.iterator)
		}
}
