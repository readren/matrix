package readren.matrix
package core

import readren.taskflow.Maybe

import java.util
import scala.annotation.tailrec
import scala.collection.MapView
import scala.compiletime.uninitialized
import scala.runtime.AbstractFunction1

object Reactant {
	type SerialNumber = Int

	private sealed trait Decision[+U]

	private object ToContinue extends Decision[Nothing]

	private class ToRestart[U](val stopChildren: Boolean, val restartBehaviorBuilder: ReactantRelay[U] => Behavior[U]) extends Decision[U]

	private object ToStop extends Decision[Nothing]
}

import core.Reactant.*

/**
 *
 * @param doer the [[MatrixDoer]] assigned to this instance.
 * @tparam U the type of the messages this reactant understands.
 */
abstract class Reactant[U](
	val serial: SerialNumber,
	progenitor: Spawner[MatrixDoer],
	override val doer: MatrixDoer,
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
	 * Should be accessed within the [[doer]].
	 * */
	private var isReadyToProcessMsg: Boolean = false

	/** Tells if this [[Reactant]] was marked to be stopped.
	 * Is set to true by the [[Reactant.stop]] method which can be called at any moment .
	 * It can't be cleared. Once it is true it will remain true forever (until it is garbage-collected).
	 * It is volatile to achieve its only purpose: to avoid processing the next pending messages after [[stop]] was called from outside the [[doer]] thread; otherwise the [[stopWasStarted]] would be sufficient. */
	@volatile private var isMarkedToStop: Boolean = false

	/** Tells if the stop process was already started.
	 * Is set to true by the [[Reactant.selfStop]] method which is called withing the [[doer]].
	 * It can't be cleared. Once it is true it will remain true forever (until it is garbage-collected).
	 * Its purpose is to avoid the [[processMessages()]] be called after the stop process has started.
	 * Should be accessed within the [[doer]] only.
	 * */
	private var stopWasStarted = false

	private val stopCovenant = new doer.Covenant[Unit]

	private var oSpawner: Maybe[Spawner[doer.type]] = Maybe.empty
	/** Should be accessed withing the [[doer]] */
	private var childrenRelays: MapView[Long, ReactantRelay[?]] = MapView.empty

	override val endpointProvider: EndpointProvider[U]

	override val path: String = {
		val parentPath = progenitor.owner.fold(java.lang.StringBuilder())(pr => java.lang.StringBuilder(pr.path))
		parentPath.append('/').append(serial).toString
	}

	/** Should be the last field to be initialized, in order to ensure that the `initialBehaviorBuilder` is executed with the [[Reactant]] fully initialized. */
	private var currentBehavior: Behavior[U] = uninitialized

	protected val inbox: Inbox[U]

	/** Contains the observers subscribed to the [[Reactant.stopCovenant]] of other [[Reactant]] instances that were not unsubscribed calling [[WatchSubscription.unsubscribe()]].
	 * @see [[watch]]. */
	private val activeWatchSubscriptions: util.IdentityHashMap[ReactantRelay[?], List[WatchSubscription]] = new util.IdentityHashMap()

	/**
	 * Should be called only once and within the [[doer]].
	 * Design note: This method is necessary to initialize the objects referenced by this [[Reactant]] that also need a reference to this [[Reactant]] after it is sufficiently initialized (e.g., [[currentBehavior]]). */
	def initialize(): doer.Duty[this.type] = { // send Started signal after all the vals and vars have been initialized
		doer.checkWithin()
		assert(currentBehavior == null)
		selfStart(false, initialBehaviorBuilder).map(_ => thisReactant) // TODO considerar hacer que selfStart devuelva Duty[this.type] para evitar este 'map`  del final. Esto requiere que selfStop, selfRestar, stayIdleUntilNextMessageArrive, y otros que ahora devuelven Duty[Unit] también hagan lo mismo.
	}

	/** Starts or restarts this [[Reactant]].
	 * Should be called only once and within the [[doer]].
	 * */
	private def selfStart(comesFromRestart: Boolean, behaviorBuilder: ReactantRelay[U] => Behavior[U]): doer.Duty[Unit] = {
		doer.checkWithin()
		currentBehavior = behaviorBuilder(thisReactant)
		val handleResult = handleSignal(if comesFromRestart then isSignalTest.restarted else isSignalTest.started)
		mapHrToDecision(handleResult) match {
			case ToContinue =>
				if !stopWasStarted then beReadyToProcess()
				doer.dutyReadyUnit
			case ToStop =>
				selfStop()
			case tr: ToRestart[U @unchecked] =>
				selfRestart(tr.stopChildren, tr.restartBehaviorBuilder)
		}
	}

	/** Should be called withing the [[doer]]. */
	override def spawn[A](childReactantFactory: ReactantFactory, doerAssistantProviderRef: Matrix.DoerAssistantProviderRef[?])(initialChildBehaviorBuilder: ReactantRelay[A] => Behavior[A])(using isSignalTest: IsSignalTest[A]): doer.Duty[ReactantRelay[A]] = {
		doer.checkWithin()
		oSpawner.fold {
				val spawner = new Spawner[doer.type](Maybe.some(thisReactant), doer, serial)
				oSpawner = Maybe.some(spawner)
				childrenRelays = spawner.childrenView
				spawner
			}(alreadyBuiltSpawner => alreadyBuiltSpawner)
			.createReactant[A](childReactantFactory, doerAssistantProviderRef, isSignalTest, initialChildBehaviorBuilder)
	}

	/** The children of this [[Reactant]] by serial number.
	 *
	 * Should be called withing the [[doer]]. */
	override def children: MapView[Long, ReactantRelay[?]] = {
		doer.checkWithin()
		childrenRelays
	}

	/** should be called within the [[doer]]. */
	private final def selfRestart(stopChildren: Boolean, restartBehaviorBuilder: ReactantRelay[U] => Behavior[U]): doer.Duty[Unit] = {
		doer.checkWithin()

		def restartMe(): doer.Duty[Unit] = {
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
							oSpawner.fold(doer.dutyReadyUnit) { spawner =>
								spawner.stopChildren()
							}
						}
						else doer.dutyReadyUnit
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

	override def stopDuty: doer.SubscriptableDuty[Unit] = stopCovenant.subscriptableDuty

	override def watch[CSM <: U](watchedReactant: ReactantRelay[?], stoppedSignal: CSM, univocally: Boolean = true, subscriptionCompleted: Maybe[doer.Covenant[Unit]]): Maybe[WatchSubscription] = {
		doer.checkWithin()
		if stopWasStarted then Maybe.empty
		else {
			object observer extends AbstractFunction1[Unit, Unit], WatchSubscription {
				private def work(): Unit = {
					// ignore the notification if a stop of this reactant is in progress or the subscription is not active.
					if !stopWasStarted && activeWatchSubscriptions.getOrDefault(watchedReactant, Nil).contains(observer) then {
						mapHrToDecision(currentBehavior.handle(stoppedSignal)) match {
							case ToContinue => ()
							case ToStop => selfStop()
							case tr: ToRestart[U @unchecked] => selfRestart(tr.stopChildren, tr.restartBehaviorBuilder).triggerAndForget(true)
						}
					}
				}

				override def apply(unit: Unit): Unit = {
					if watchedReactant.doer eq thisReactant.doer then work()
					else doer.executeSequentially(work())
				}

				override def unsubscribe(): Unit = {
					doer.checkWithin()
					// first remove the observer from the active subscription maintained locally in order to ignore the notification it could catch until the subscription is undone.   
					activeWatchSubscriptions.computeIfPresent(watchedReactant, (_, list) => list.filterNot(_ eq observer))
					// then undo the subscription, which may be asynchronous. 
					if watchedReactant.doer.assistant eq thisReactant.doer.assistant then watchedReactant.stopDuty.unsubscribe(observer)
					else watchedReactant.doer.executeSequentially(watchedReactant.stopDuty.unsubscribe(observer))
				}
			}
			// first, add the observer to the active subscriptions record.
			activeWatchSubscriptions.compute(
				watchedReactant,
				(_, list) =>
					if list == null then List(observer)
					else if univocally then {
						list.foreach(_.unsubscribe())
						List(observer)
					} else observer :: list
			)
			// and then, make the subscription
			if watchedReactant.doer.assistant eq thisReactant.doer.assistant then {
				watchedReactant.stopDuty.subscribe(observer)
				subscriptionCompleted.foreach(_.fulfill((), true)())
			} else watchedReactant.doer.executeSequentially {
				watchedReactant.stopDuty.subscribe(observer)
				subscriptionCompleted.foreach(_.fulfill((), false)())
			}
			Maybe.some(observer)
		}
	}

	override final def stop(): doer.Duty[Unit] = {
		// Note that if [[stop]] is called simultaneously from many threads, the [[selfStop]] duty might be triggered more than once, but that is not harmful because it discards repetitions.
		// As far as this "if" is concerned, mutations of the `isMarkedToStop` flag do not need to be atomic.
		if !isMarkedToStop then {
			isMarkedToStop = true
			doer.executeSequentially(selfStop())
		}
		stopCovenant.subscriptableDuty
	}

	/**
	 * Stops this [[Reactant]].
	 * Should be called within the [[doer]].
	 * Supports being called more than one time.
	 * It is not necessary to trigger the execution of the returned [[Duty]] to start the stop process. The result can be ignored.
	 * @return a [[Duty]] that completes when this [[Reactant]] is fully stopped. */
	private final def selfStop(): doer.Duty[Unit] = {
		doer.checkWithin()

		/** should be called within the [[doer]]. */
		def stopMe(): Unit = {
			// execute the signal handler and ignore its result
			handleSignal(isSignalTest.stopReceived)
			// remove myself form progenitor children
			progenitor.doer.executeSequentially {
				progenitor.removeChild(thisReactant.serial)
				stopCovenant.fulfill(())()
			}
			// TODO notify parent
		}

		if !stopWasStarted then {
			stopWasStarted = true
			isReadyToProcessMsg = false
			activeWatchSubscriptions.forEach { (k, v) => v.foreach(_.unsubscribe()) }
			oSpawner.fold(stopMe()) { spawner =>
				spawner.stopChildren().trigger(true)(_ => stopMe())
			}
		}
		stopCovenant.subscriptableDuty
	}

	private inline def handleSignal(signal: Option[U]): HandleResult[U] = {
		signal.fold(Continue)(currentBehavior.handle)
	}


	private final def mapHrToDecision(hr: HandleResult[U]): Decision[U] = {
		doer.checkWithin()
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
	 * Should be called withing the [[doer]] only. */
	private def beReadyToProcess(): Unit = {
		doer.checkWithin()
		assert(!stopWasStarted && !isReadyToProcessMsg)
		if inbox.maybeNonEmpty then doer.executeSequentially {
			if !stopWasStarted then {
				inbox.withdraw().fold(beReadyToProcess())(processMessages)
			}
		} else isReadyToProcessMsg = true
	}

	/** Should be called by the [[Receiver]] whenever it receives a message while the [[Inbox]] is empty.
	 * Differs from the other variant in that this method is designed for implementations of the [[Receiver]] which queue messages concurrently (not within the [[doer]] of this reactant).
	 * Should be called within the [[doer]] only.
	 * */
	final def onInboxBecomesNonempty(): Unit = {
		doer.checkWithin()
		if isReadyToProcessMsg then {
			isReadyToProcessMsg = false
			inbox.withdraw().fold(beReadyToProcess())(processMessages)
		}
	}

	/** Should be called by the [[Receiver]] whenever it receives a message while the [[Inbox]] is empty.
	 * Differs from the other variant in that this method is designed for implementations of the [[Receiver]] which queue messages within the [[doer]] of this reactant.
	 * Should be called within the [[doer]] only.
	 * @param firstMsg the message received while the inbox was empty.
	 * @return true if the received message was not processed and should be queued in the inbox; false if it was processed. */
	final def onInboxBecomesNonempty(firstMsg: U): Boolean = {
		doer.checkWithin()
		if isReadyToProcessMsg then {
			isReadyToProcessMsg = false
			processMessages(firstMsg)
			false
		} else true
	}

	/** Process the received message and all the pending messages queue in the [[Reactant.inbox]]
	 * Should be called within the doer */
	private final def processMessages(firstMessage: U): Unit = {
		doer.checkWithin()

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


	override def diagnose: doer.Duty[ReactantDiagnostic] =
		doer.Duty.mine { () =>
			val childrenDiagnostic = children.map(_._2.staleDiagnose).toArray
			ReactantDiagnostic(thisReactant.isReadyToProcessMsg, thisReactant.isMarkedToStop, thisReactant.stopWasStarted, inbox.size, inbox.iterator, childrenDiagnostic)
		}

	override def staleDiagnose: ReactantDiagnostic =
		val childrenDiagnostic = childrenRelays.map(_._2.staleDiagnose).toArray
		ReactantDiagnostic(thisReactant.isReadyToProcessMsg, thisReactant.isMarkedToStop, thisReactant.stopWasStarted, inbox.size, Iterator.empty, childrenDiagnostic)
}
