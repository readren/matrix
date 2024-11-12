package readren.matrix

import readren.taskflow.Maybe

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
 * @param oHandlerExecutorService the [[ExecutorService]] used for executing [[Behavior.handleMsg]], [[Behavior.handleSignal]], and [[initialBehaviorBuilder]].
 * @tparam U the type of the messages this reactant understands.
 */
abstract class Reactant[U](
	val serial: SerialNumber,
	progenitor: Spawner[MatrixAdmin],
	override val admin: MatrixAdmin,
	initialBehaviorBuilder: ReactantRelay[U] => Behavior[U],
	oHandlerExecutorService: Maybe[HandlerExecutorService[U]]
) extends ReactantRelay[U] { thisReactant =>

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
		parentPath.append('/').append(serial).toString
	}

	/** Should be the last field to be initialized, in order to ensure that the `initialBehaviorBuilder` is executed with the [[Reactant]] fully initialized. */
	private var currentBehavior: Behavior[U] = uninitialized

	/**
	 * Should be called only once and within the [[admin]].
	 * Design note: This method is necessary to initialize the objects referenced by this [[Reactant]] that also need a reference to this [[Reactant]] after it is sufficiently initialized (e.g., [[currentBehavior]]). */
	def initialize(): admin.Duty[this.type] = { // send Started signal after all the vals and vars have been initialized
		admin.checkWithin()
		assert(!idleState)
		assert(currentBehavior == null)
		selfStart(false, initialBehaviorBuilder).map(_ => thisReactant) // TODO considerar hacer que selfStart devuelva Duty[this.type] para evitar este 'map`  del final. Esto requiere que selfStop, selfRestar, stayIdleUntilNextMessageArrive, y otros que ahora devuelven Duty[Unit] también hagan lo mismo.
	}

	/** Starts or restarts this [[Reactant]].
	 * Should be called only once and within the [[admin]].
	 * */
	private def selfStart(comesFromRestart: Boolean, behaviorBuilder: ReactantRelay[U] => Behavior[U]): admin.Duty[Unit] = {
		admin.checkWithin()
		executeBehaviorBuilder(behaviorBuilder)
			.flatMap { initialBehavior =>
				currentBehavior = initialBehavior
				executeSignalHandler(if comesFromRestart then Restarted else Started)
					.map(mapHrToDecision)
					.flatMap {
						case ToContinue => admin.Duty.ready(stayIdleUntilNextMessageArrives())
						case ToStop => selfStop()
						case tr: ToRestart[U @unchecked] => selfRestart(tr.stopChildren, tr.restartBehaviorBuilder)
					}
			}
	}

	/** Should be called withing the [[admin]]. */
	private def spawner: Spawner[admin.type] = oSpawner.fold {
		admin.checkWithin()
		val spawner = new Spawner[admin.type](Maybe.some(thisReactant), admin, serial)
		oSpawner = Maybe.some(spawner)
		childrenRelays = spawner.childrenView.filterNot(_._2.isMarkedToStop) // Note that the `isMarkedToStop` should be accessed withing the child's admin, and here we aren't. So this collection may contain reactants that are already marked to stop.
		spawner
	}(alreadyBuiltSpawner => alreadyBuiltSpawner)

	/** Should be called withing the [[admin]] when [[oHandlerExecutorService]] is empty. */
	override def spawn[A](childReactantFactory: ReactantFactory)(initialChildBehaviorBuilder: ReactantRelay[A] => Behavior[A]): admin.Duty[ReactantRelay[A]] = {
		if oHandlerExecutorService.isEmpty then {
			admin.checkWithin()
			spawner.createReactant[A](childReactantFactory, initialChildBehaviorBuilder)
		} else {
			MatrixAdmin.checkOutside()
			admin.Duty.mineFlat { () =>
				spawner.createReactant[A](childReactantFactory, initialChildBehaviorBuilder)
			}
		}
	}

	/** The children of this [[Reactant]] by serial number.
	 * Does not include those children whose [[isMarkedToStop]] flag is set unless it was set recently. So it may contain some reactants that are already marked to stop.
	 * Should be called withing the [[admin]]. */
	override def getChildren: MapView[Long, ReactantRelay[?]] = {
		admin.checkWithin()
		childrenRelays
	}

	/** @return true if there is a message is queue to be processed. */
	protected def aMsgIsPending: Boolean

	/** is called within the [[admin]]. */
	protected def withdrawNextMessage(): Maybe[U]

	/** should be called within the [[admin]]. */
	inline def isIdle: Boolean = {
		admin.checkWithin()
		idleState
	}

	/** should be called within the [[admin]]. */
	private final def stayIdleUntilNextMessageArrives(): Unit = {
		admin.checkWithin()
		assert(!idleState)
		idleState = true
		if aMsgIsPending then stimulate(withdrawNextMessage().get)
	}

	/** should be called within the [[admin]]. */
	private final def executeSignalHandler(signal: Signal): admin.Duty[HandleResult[U]] = {
		admin.checkWithin()
		oHandlerExecutorService.fold(admin.Duty.ready(currentBehavior.handleSignal(signal))) { handlerExecutorService =>
			val covenant = new admin.Covenant[HandleResult[U]]
			handlerExecutorService.executeSignalHandler(currentBehavior, signal)(hr => covenant.fulfill(hr)())
			covenant
		}
	}

	/** should be called within the [[admin]]. */
	private final def executeBehaviorBuilder(behaviorBuilder: ReactantRelay[U] => Behavior[U]): admin.Duty[Behavior[U]] = {
		admin.checkWithin()
		oHandlerExecutorService.fold(admin.Duty.ready(behaviorBuilder(thisReactant))) { handlerExecutorService =>
			val covenant = new admin.Covenant[Behavior[U]]
			handlerExecutorService.executeBehaviorBuilder(behaviorBuilder, thisReactant)(behavior => covenant.fulfill(behavior)())
			covenant
		}
	}

	/** should be called within the [[admin]]. */
	private final def selfRestart(stopChildren: Boolean, restartBehaviorBuilder: ReactantRelay[U] => Behavior[U]): admin.Duty[Unit] = {
		admin.checkWithin()

		def restartMe(): admin.Duty[Unit] = {
			// send RestartReceived signal
			executeSignalHandler(RestartReceived)
				.map(mapHrToDecision)
				.flatMap {
					case ToContinue => selfStart(true, restartBehaviorBuilder)
					case ToStop => selfStop() // if the `handleSignal` responds `Stop` to the `RestartReceived` signal, then the restart is canceled and the reactant is stopped instead.
					case tr: ToRestart[U @unchecked] =>
						// if the `handleSignal` responds `Restart` or `RestartWith` to the `RestartReceived` signal, then the restart is adapted to the new restart settings: stops children if they were not, and replaces the restartBehaviorBuilder for the new one.
						val stopsChildrenIfInstructed =
							if tr.stopChildren && !stopChildren then {
								oSpawner.fold(admin.dutyReadyUnit) { spawner =>
									spawner.stopChildren()
								}
							}
							else admin.dutyReadyUnit
						stopsChildrenIfInstructed.flatMap(_ => selfStart(true, tr.restartBehaviorBuilder))

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

	def watch(childSerial: SerialNumber): Boolean = {
		admin.checkWithin()
		oSpawner.fold(false) { spawner =>
			Maybe(spawner.childrenView.getOrElse(childSerial, null)).fold(false) { child =>
				child.stopDuty.onBehalfOf(admin).foreach { _ =>
						// if a stop of this reactant is in progress, ignore the notification.
						if thisReactant.isMarkedToStop then () // TODO corregir: el objetivo de este if (ignorar notificaciones cuando el reactant ya inició el stopping process) no se logra cuando oMsgHandlerExecutorService está definido porque la marca `isMarkedToStop` se pondría en true luego de que la ejecución asincrónica termine y para entonces el hilo de otra notificaciones ya pudo haver evaluado la condición de este if.
						else executeSignalHandler(ChildStopped(child.serial))
							.map(mapHrToDecision)
							.map {
								case ToContinue =>
									()
								case ToStop =>
									selfStop()
										.triggerAndForget(true)
								case tr: ToRestart[U @unchecked] =>
									selfRestart(tr.stopChildren, tr.restartBehaviorBuilder)
										.triggerAndForget(true)
							}.triggerAndForget(true)
					}
					.triggerAndForget(true)
				true
			}
		}
	}

	/**
	 * Instructs to stop this [[Reactant]].
	 * Supports being called at any moment.
	 * thread-safe
	 * @return a [[Duty]] that completes when this [[Reactant]] is fully stopped. */
	override final def stop(): admin.Duty[Unit] = {
		admin.Duty.mineFlat { () =>
			if isIdle then {
				idleState = false
				selfStop()
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
	private final def selfStop(): admin.Duty[Unit] = {
		admin.checkWithin()

		/** should be called within the [[admin]]. */
		def stopMe(): admin.Duty[Unit] = {
			admin.checkWithin()
			// execute the StopReceived
			executeSignalHandler(StopReceived).flatMap { _ => // note that the result of the `signalHandler` is ignored.
				// remove myself form progenitor children
				progenitor.admin.Duty.mine { () => progenitor.removeChild(serial) }
					.onBehalfOf(admin)
					.foreach(_ => stopCovenant.fulfill(())())

				// TODO notify parent
			}
		}

		idleState = false
		isMarkedToStop = true
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
		assert(idleState && !isMarkedToStop, s"isIdle=$isIdle, isMarkedToStop=$isMarkedToStop")

		/** should be called within the admin */
		inline def processMessagesWithSpecifiedExecutor(handlerExecutorService: HandlerExecutorService[U]): Unit = {

			/** should be called within the admin */
			def handleMsg(message: U, behavior: Behavior[U]): admin.Duty[Decision[U]] = {
				val covenant = new admin.Covenant[HandleResult[U]]
				handlerExecutorService.executeMsgHandler(behavior, message) { hmr => covenant.fulfill(hmr)() }
				covenant.map(mapHrToDecision)
			}

			/** should be called within the admin */
			def processNextPendingMessages(): admin.Duty[Decision[U]] = {

				// Build a duty that, if there is no pending messages, returns `Maybe.some(continue)`;
				// else if there is a pending message, withdraws it, handles it, and if everything went OK updates the reactant's behavior, and returns `Maybe.empty`;
				// else (there were problems) returns `Maybe.some(decision)` where decision != continue.
				val processMessage: admin.Duty[Maybe[Decision[U]]] =

					// withdraw the next pending message considering all the inboxes the reactant has.
					admin.Duty.mineFlat { () =>
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
						admin.dutyReadyUnit
					case ToStop =>
						selfStop()
					case tr: ToRestart[U @unchecked] =>
						selfRestart(tr.stopChildren, tr.restartBehaviorBuilder)
				}
			// Nothing happens until here where the duty built above is executed.
			processAllPendingMessages.triggerAndForget()
		}

		/** should be called within the admin */
		inline def processMessagesWithAdmin(): Unit = {
			/** should be called within the admin */
			def handleMsg(message: U, behavior: Behavior[U]): Decision[U] = {
				admin.checkWithin()
				mapHrToDecision(behavior.handleMsg(message))
			}

			/** should be called within the admin */
			@tailrec
			def processNextPendingMessages(): Decision[U] = {
				admin.checkWithin()
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
				case ToStop => selfStop().triggerAndForget(true)
				case tr: ToRestart[U @unchecked] => selfRestart(tr.stopChildren, tr.restartBehaviorBuilder).triggerAndForget(true)
			}
		}

		// First line of the outer method.
		idleState = false
		oHandlerExecutorService.fold(processMessagesWithAdmin())(processMessagesWithSpecifiedExecutor)
	}
}
