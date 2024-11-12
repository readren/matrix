package readren.matrix

import readren.taskflow.Maybe

import java.util.concurrent.atomic.AtomicBoolean
import scala.annotation.tailrec
import scala.collection.MapView
import scala.compiletime.uninitialized

object Reactant {
	type SerialNumber = Int

	private sealed trait Decision[+U]

	private case object ToContinue extends Decision[Nothing]

	private case class ToRestart[U](stopChildren: Boolean, restartBehaviorBuilder: ReactantRelay[U] => Behavior[U]) extends Decision[U]

	private case object ToStop extends Decision[Nothing]
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
	initialBehaviorBuilder: ReactantRelay[U] => Behavior[U]
) extends ReactantRelay[U] { thisReactant =>

	override protected val isReadyToProcessAMsg: AtomicBoolean = AtomicBoolean(false)
	override protected val isMarkedToStop: AtomicBoolean = AtomicBoolean(false)
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

	/** @return true if there is a message queue to be processed. */
	protected def aMsgIsPending: Boolean

	/** is called within the [[admin]]. */
	protected def withdrawNextMessage(): Maybe[U]

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
		val handleResult = currentBehavior.handleSignal(if comesFromRestart then Restarted else Started)
		mapHrToDecision(handleResult) match {
			case ToContinue =>
				stayIdleUntilNextMsgArrives()
				admin.dutyReadyUnit
			case ToStop =>
				selfStop()
			case tr: ToRestart[U @unchecked] =>
				selfRestart(tr.stopChildren, tr.restartBehaviorBuilder)
		}
	}

	/** Should be called withing the [[admin]]. */
	override def spawn[A](childReactantFactory: ReactantFactory)(initialChildBehaviorBuilder: ReactantRelay[A] => Behavior[A]): admin.Duty[ReactantRelay[A]] = {
		admin.checkWithin()
		oSpawner.fold {
				val spawner = new Spawner[admin.type](Maybe.some(thisReactant), admin, serial)
				oSpawner = Maybe.some(spawner)
				childrenRelays = spawner.childrenView
				spawner
			}(alreadyBuiltSpawner => alreadyBuiltSpawner)
			.createReactant[A](childReactantFactory, initialChildBehaviorBuilder)
	}

	/** The children of this [[Reactant]] by serial number.
	 *
	 * Should be called withing the [[admin]]. */
	override def children: MapView[Long, ReactantRelay[?]] = {
		admin.checkWithin()
		childrenRelays
	}

	/** should be called within the [[admin]]. */
	private final def stayIdleUntilNextMsgArrives(): Unit = {
		admin.checkWithin()
		isReadyToProcessAMsg.set(true) // it's necessary that this line be before the next if
		if aMsgIsPending then stimulate(withdrawNextMessage().get)
	}

	/** should be called within the [[admin]]. */
	private final def selfRestart(stopChildren: Boolean, restartBehaviorBuilder: ReactantRelay[U] => Behavior[U]): admin.Duty[Unit] = {
		admin.checkWithin()

		def restartMe(): admin.Duty[Unit] = {
			// send RestartReceived signal
			val hr = currentBehavior.handleSignal(RestartReceived)
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

	override def stopDuty: admin.Duty[Unit] = stopCovenant.duty

	def watch(childSerial: SerialNumber): Boolean = {
		admin.checkWithin()
		oSpawner.fold(false) { spawner =>
			Maybe(spawner.childrenView.getOrElse(childSerial, null)).fold(false) { child =>
				child.stopDuty.onBehalfOf(admin).foreach { _ =>
					// if a stop of this reactant is in progress, ignore the notification.
					if thisReactant.isMarkedToStop.get then ()
					else {
						val hr = currentBehavior.handleSignal(ChildStopped(child.serial))
						mapHrToDecision(hr) match {
							case ToContinue => ()
							case ToStop => selfStop().triggerAndForget(true)
							case tr: ToRestart[U @unchecked] => selfRestart(tr.stopChildren, tr.restartBehaviorBuilder).triggerAndForget(true)
						}
					}
				}.triggerAndForget(true)
				true
			}
		}
	}

	/**
	 * Instructs to stop this [[Reactant]].
	 * Supports being called at any moment.
	 * If this covenant is processing a message when this method is called, the process of that single message will continue but no other message will be processed after it.
	 * thread-safe
	 * @return a [[Duty]] that completes when this [[Reactant]] is fully stopped. */
	override final def stop(): admin.Duty[Unit] = {
		isReadyToProcessAMsg.set(false)
		// the order of the previous and next line is important.
		if isMarkedToStop.getAndSet(true) then stopCovenant.duty
		else admin.Duty.mineFlat { () => selfStop() }
	}

	/**
	 * Stops this [[Reactant]].
	 * Should be called within the [[admin]].
	 * @return a [[Duty]] that completes when this [[Reactant]] is fully stopped. */
	private final def selfStop(): admin.Duty[Unit] = {
		admin.checkWithin()

		/** should be called within the [[admin]]. */
		def stopMe(): admin.Duty[Unit] = {
			// execute the signal handler and ignore its result
			currentBehavior.handleSignal(StopReceived)
			// remove myself form progenitor children
			progenitor.admin.Duty.mine { () => progenitor.removeChild(thisReactant.serial) }
				.onBehalfOf(thisReactant.admin)
				.foreach(_ => stopCovenant.fulfill(())())

			// TODO notify parent
		}

		isReadyToProcessAMsg.set(false)
		isMarkedToStop.set(true) // TODO  es necesaria esta linea?
		oSpawner.fold(stopMe()) { spawner =>
			spawner.stopChildren().flatMap(_ => stopMe())
		}
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

	/** Should be called within this [[MatrixAdmin]] and only if this reactant is idle. */
	final def stimulate(firstMessage: U): Unit = {
		admin.checkWithin()
		if !isReadyToProcessAMsg.getAndSet(false) then return

		inline def handleMsg(message: U, behavior: Behavior[U]): Decision[U] = mapHrToDecision(behavior.handleMsg(message))

		/** should be called within the admin */
		@tailrec
		def processNextPendingMessages(): Decision[U] = {
			admin.checkWithin()
			withdrawNextMessage().fold(ToContinue) { message =>
				val decision = handleMsg(message, currentBehavior)
				if isMarkedToStop.get then ToStop
				else if decision eq ToContinue then processNextPendingMessages()
				else decision
			}
		}

		// First line of the outer method.
		val firstDecision = handleMsg(firstMessage, currentBehavior)
		val finalDecision =
			if isMarkedToStop.get then ToStop
			else if firstDecision eq ToContinue then processNextPendingMessages()
			else firstDecision
		finalDecision match {
			case ToContinue => isReadyToProcessAMsg.set(true)
			case ToStop => selfStop().triggerAndForget(true)
			case tr: ToRestart[U @unchecked] => selfRestart(tr.stopChildren, tr.restartBehaviorBuilder).triggerAndForget(true)
		}
	}
}
