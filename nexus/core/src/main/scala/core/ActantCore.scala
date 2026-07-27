package readren.nexus
package core

import readren.common.Maybe
import readren.sequencer.Doer

import java.util
import java.util.function.{BiConsumer, BiFunction}
import scala.annotation.{tailrec, threadUnsafe}
import scala.collection.MapView
import scala.collection.mutable.ArrayBuffer
import scala.compiletime.uninitialized
import scala.util.control.NonFatal

object ActantCore {
	type SerialNumber = Int

	private sealed trait Decision[+U]

	private object ToContinue extends Decision[Nothing]

	private object ToStop extends Decision[Nothing]
}

import core.ActantCore.*

/**
 * @param serial identifies a [[ActantCore]] among its siblings.
 * @param progenitor the [[Spawner]] that created this [[ActantCore]]. The progenitor of a [[ActantCore]] knows the set of its children, and every [[ActantCore]] knows its progenitor.
 * @param doer the [[Doer]] instance assigned to this [[ActantCore]].
 * @param isSignalTest knows which [[Signal]]s does this [[ActantCore]] understand. In other words, knows which concrete [[Signal]] types are assignable to `U`. This information is obtained from the `U` type parameter at compile time.
 * @param initialBehaviorBuilder a builder of the [[Behavior]] that the created [[ActantCore]] will host when is born.
 * @tparam U the type of the messages this actant understands.
 */
abstract class ActantCore[U, D <: Doer](
	val serial: SerialNumber,
	override val doer: D,
	progenitor: Spawner[?],
	isSignalTest: IsSignalTest[U],
	initialBehaviorBuilder: Actant[U, D] => Behavior[U]
) extends Actant[U, D] { thisActant =>

	private class ToRestart(val stopChildren: Boolean, val restartBehaviorBuilder: Actant[U, D] => Behavior[U]) extends Decision[U]

	/** the nexus this [[Actant]] is part of */
	val nexus: Nexus = progenitor.owner match {
		case ab: Nexus => ab
		case r: ActantCore[?, ?] => r.nexus
	}
	
	/**
	 * The initial state is `false` (not ready).
	 * Is set to `true` after consuming all the pending messages (of this actant's [[Inbox]]) if the result of [[Behavior.handle]] for the last message returned [[Continue]] or [[ContinueWith]]; and the stop process was not started (e.g. the [[stopWasStarted]] is false).
	 * Is set to `false` when [[stopWasStarted]] is set to `true` or after [[onInboxBecomesNonempty]] is called.
	 * Its purpose is to avoid consuming messages while this [[ActantCore]] is starting, restarting, or stopping.
	 * It is set to true only by the [[beReadyToProcess()]] method, which ensures all pending messages are processed before the transition to `true`.
	 * This flag should be the only one that determines when "inbox becomes nonempty" notifications (calls to [[onInboxBecomesNonempty]]) are ignored in order to compensate the ignored notifications when it is set to `true`.
	 * Should be accessed within the [[doer]].
	 * */
	private var isReadyToProcessMsg: Boolean = false

	/** Tells if this [[ActantCore]] was marked to be stopped.
	 * Is set to true by the [[ActantCore.stop]] method which can be called at any moment .
	 * It can't be cleared. Once it is true it will remain true forever (until it is garbage-collected).
	 * It is volatile to achieve its only purpose: to avoid processing the next pending messages after [[stop]] was called from outside the [[doer]] thread; otherwise the [[stopWasStarted]] would be sufficient. */
	@volatile private var isMarkedToStop: Boolean = false

	/** Tells if the stop process was already started.
	 * Is set to true by the [[ActantCore.selfStop]] method which is called withing the [[doer]].
	 * It can't be cleared. Once it is true it will remain true forever (until it is garbage-collected).
	 * Its purpose is to avoid the [[processMessages()]] be called after the stop process has started.
	 * Should be accessed within the [[doer]] only.
	 * */
	private var stopWasStarted = false

	private val stopCaptor = new doer.Captor[Unit]

	private var maybeSpawner: Maybe[Spawner[doer.type]] = Maybe.empty
	/** Should be accessed withing the [[doer]] */
	private var childrenGates: MapView[Long, Actant[?, ?]] = MapView.empty

	override val receptorProvider: ReceptorProvider[U]

	override val path: String = {
		val parentPath = java.lang.StringBuilder(progenitor.owner.path)
		parentPath.append('/').append(serial).toString
	}

	/** Should be the last field to be initialized, in order to ensure that the `initialBehaviorBuilder` is executed with the [[ActantCore]] fully initialized. */
	private var currentBehavior: Behavior[U] = uninitialized

	protected val inbox: Inbox[U]

	/** Contains the observers subscribed to the [[ActantCore.stopCaptor]] of other [[ActantCore]] instances that were not unsubscribed calling [[WatchSubscription.unsubscribe()]].
	 * @see [[watch]]. */
	@threadUnsafe private lazy val activeWatchSubscriptions: util.IdentityHashMap[Actant[?, ?], List[WatchSubscription]] = new util.IdentityHashMap()

	/**
	 * Should be called only once and within the [[doer]].
	 * Design note: This method is necessary to initialize the objects referenced by this [[ActantCore]] that also need a reference to this [[ActantCore]] after it is sufficiently initialized (e.g., [[currentBehavior]]). */
	def initialize(): doer.Capturer[this.type] = { // send Started signal after all the vals and vars have been initialized
		doer.checkWithin()
		assert(currentBehavior eq null)
		selfStart(false, initialBehaviorBuilder).map(_ => thisActant) // TODO considerar hacer que selfStarts devuelva Task[this.type] para evitar este 'map`  del final. Esto requiere que selfStop, selfRestar, stayIdleUntilNextMessageArrive, y otros que ahora devuelven Task[Unit] también hagan lo mismo.
	}

	/** Starts or restarts this [[ActantCore]].
	 * Should be called only once and within the [[doer]].
	 * */
	private def selfStart(comesFromRestart: Boolean, behaviorBuilder: Actant[U, D] => Behavior[U]): doer.Capturer[Unit] = {
		doer.checkWithin()
		currentBehavior = behaviorBuilder(thisActant)
		val handleResult = handleSignal(if comesFromRestart then isSignalTest.restarted else isSignalTest.started)
		mapHrToDecision(handleResult) match {
			case ToContinue =>
				if !stopWasStarted then beReadyToProcess()
				doer.Capturer_unit
			case ToStop =>
				selfStop()
			case tr: ToRestart =>
				selfRestart(tr.stopChildren, tr.restartBehaviorBuilder)
		}
	}

	/** Should be called withing the [[doer]]. */
	override def spawn[V, CD <: Doer](
		childActantFactory: ActantFactory,
		childDoer: CD
	)(
		initialChildBehaviorBuilder: Actant[V, CD] => Behavior[V]
	)(
		using isSignalTest: IsSignalTest[V]
	): doer.Capturer[Actant[V, CD]] = {
		doer.checkWithin()
		maybeSpawner.fold {
				val spawner = new Spawner[doer.type](thisActant, doer, serial)
				maybeSpawner = Maybe(spawner)
				childrenGates = spawner.childrenView
				spawner
			}(alreadyBuiltSpawner => alreadyBuiltSpawner)
			.createsActant[V, CD](childActantFactory, childDoer, isSignalTest, initialChildBehaviorBuilder)
	}

	/** The children of this [[ActantCore]] by serial number.
	 *
	 * Calls must be within the [[doer]]. */
	override def children: MapView[Long, Actant[?, ?]] = {
		doer.checkWithin()
		childrenGates
	}

	/** Calls must be within the [[doer]]. */
	private final def selfRestart(stopChildren: Boolean, restartBehaviorBuilder: Actant[U, D] => Behavior[U]): doer.Capturer[Unit] = {
		doer.checkWithin()

		def restartMe(): doer.Capturer[Unit] = {
			// send RestartReceived signal
			val hr = handleSignal(isSignalTest.restartReceived)
			mapHrToDecision(hr) match {
				case ToContinue => selfStart(true, restartBehaviorBuilder)
				case ToStop =>
					// if the `handleSignal` responds `Stop` to the `RestartReceived` signal, then the restart is canceled and the actant is stopped instead, which provokes the signal handler be called again with a `StopReceived` signal.
					selfStop()
				case tr: ToRestart =>
					// if the `handleSignal` responds `Restart` or `RestartWith` to the `RestartReceived` signal, then the restart is adapted to the new restart settings: stops children if they were not, and replaces the restartBehaviorBuilder for the new one. The signal handler is NOT called again.
					val stopsChildrenIfInstructed =
						if tr.stopChildren && !stopChildren then {
							maybeSpawner.fold(doer.Capturer_unit) { spawner =>
								spawner.stopChildren()
							}
						}
						else doer.Capturer_unit
					stopsChildrenIfInstructed.flatMap(_ => selfStart(true, tr.restartBehaviorBuilder))
			}
		}

		if stopChildren then {
			maybeSpawner.fold(restartMe()) { spawner =>
				spawner.stopChildren().flatMap(_ => restartMe())
			}
		} else restartMe()
	}

	override def isMarkedToBeStopped: Boolean = isMarkedToStop

	override def stopCapturer: doer.Capturer[Unit] = stopCaptor

	override def watch[SS <: U](watchedActant: Actant[?, ?], stoppedSignalBuilder: (Unit | Throwable) => SS, univocally: Boolean, maybeSubscriptionCompletedCapturer: Maybe[doer.Captor[Unit]]): Maybe[WatchSubscription] = {
		doer.checkWithin()
		if stopWasStarted then Maybe.empty
		else {
			class Eye extends watchedActant.doer.MonoObserver[Unit], (WatchSubscription => Unit), WatchSubscription, Runnable, BiFunction[Actant[?, ?], List[WatchSubscription], List[WatchSubscription] | Null] { thisEye =>
				/** Holds the subscription handle returned when subscribing to the watched [[Actant]]'s stopped capturer. This allows us to cancel the subscription directly rather than using the deprecated callback-based unsubscribe. */
				private var watchedActantStoppedSubscription: watchedActant.doer.Subscription | Null = null
				private var thisEyeWasRemoved: Boolean = false

				override def run(): Unit = {
					val was = watchedActant.stopCapturer.subscribeSync(thisEye)
					if watchedActant.doer eq thisActant.doer then {
						if stopWasStarted then was.unsubscribeSync() else watchedActantStoppedSubscription = was
						maybeSubscriptionCompletedCapturer.foreach(_.captureSync(()))
					} else thisActant.doer.run {
						if stopWasStarted then was.unsubscribeSync() else watchedActantStoppedSubscription = was
						maybeSubscriptionCompletedCapturer.foreach(_.captureSync(()))
					}
				}

				/** on watched actant stopped normally */
				override def onSuccess(u: Unit): Unit = {
					if watchedActant.doer eq thisActant.doer then handleStopSignal(u)
					else doer.run(handleStopSignal(u))
				}

				/** on watched actant stopped abruptly */
				override def onError(e: Throwable): Unit = {
					if watchedActant.doer eq thisActant.doer then handleStopSignal(e)
					else doer.run(handleStopSignal(e))
				}

				/** Apply this [[Actant]]'s [[Behavior]] to the provided stop signal */
				private def handleStopSignal(cause: Unit | Throwable): Unit = {
					watchedActantStoppedSubscription = null
					thisEyeWasRemoved = false
					activeWatchSubscriptions.computeIfPresent(watchedActant, thisEye)
					// ignore the notification if a stop of this actant is in progress or the subscription is not active.
					if thisEyeWasRemoved && !stopWasStarted then {
						val stoppedSignal = stoppedSignalBuilder(cause)
						mapHrToDecision(currentBehavior.handle(stoppedSignal)) match {
							case ToContinue => ()
							case ToStop => selfStop()
							case tr: ToRestart => selfRestart(tr.stopChildren, tr.restartBehaviorBuilder).triggerAndForget(true)
						}
					}
				}

				override def unsubscribe(): Unit = {
					doer.checkWithin()
					// First, remove the observer from the active subscription maintained locally in order to ignore the notification it could catch until the subscription is undone.
					if !stopWasStarted then activeWatchSubscriptions.computeIfPresent(watchedActant, thisEye)
					// Then, undo the subscription to the watched actant's stopped-capturer.
					val was = watchedActantStoppedSubscription
					if was != null then {
						if watchedActant.doer eq thisActant.doer then was.unsubscribeSync()
						else watchedActant.doer.run(was.unsubscribeSync())
					}
				}

				/** @return the received [[List]] with all the occurrences of [[thisEye]] removed, or `null` if resulting [[List]] would be empty. */
				override def apply(actant: Actant[?, ?], watchSubscriptions: List[WatchSubscription]): List[WatchSubscription] | Null = {
					watchSubscriptions match {
						case Nil => null
						case head :: tail =>
							if head eq thisEye then {
								thisEyeWasRemoved = true
								apply(actant, tail)
							} else {
								val newTail = apply(actant, tail)
								if newTail eq null then List(head) else head :: newTail
							}
					}
				}

				override def apply(ws: WatchSubscription): Unit = ws.unsubscribe()
			}
			// first, create an eye to observe the watched actant.
			val eye = new Eye
			val removedEyes = if univocally then ArrayBuffer.empty[List[WatchSubscription]] else null
			// second, add the eye to the active subscriptions' registry.
			activeWatchSubscriptions.compute(
				watchedActant,
				(_, list) =>
					if list eq null then List(eye)
					else if univocally then {
						removedEyes.addOne(list)
						List(eye)
					} else eye :: list
			)
			// third, if univocally is true, unsubscribe all previous WatchSubscription to the same watchedActant.
			if univocally then {
				var i = removedEyes.size
				while i > 0 do {
					i -= 1
					removedEyes(i).foreach(eye)
				}
			}

			// and finally, subscribe the eye to the watched actant's stopped-capturer, and store the returned Subscription in the activeWatchSubscriptions map.
			if watchedActant.doer eq thisActant.doer then eye.run()
			else watchedActant.doer.executeSequentially(eye)
			Maybe(eye)
		}
	}

	override final def stop(): doer.Capturer[Unit] = {
		// Note that if [[stop]] is called simultaneously from many threads, the [[selfStop]] task might be triggered more than once, but that is not harmful because it discards repetitions.
		// As far as this "if" is concerned, mutations of the `isMarkedToStop` flag do not need to be atomic.
		if !isMarkedToStop then {
			isMarkedToStop = true
			doer.run(selfStop())
		}
		stopCaptor
	}

	/**
	 * Stops this [[ActantCore]].
	 * Should be called within the [[doer]].
	 * Supports being called more than one time.
	 * @return a [[Task]] that completes when this [[ActantCore]] is fully stopped. */
	private final def selfStop(): doer.Capturer[Unit] = {
		doer.checkWithin()

		/** should be called within the [[doer]]. */
		def stopMe(): Unit = {
			// execute the signal handler and ignore its result
			handleSignal(isSignalTest.stopReceived)
			// remove myself form progenitor children
			progenitor.doer.run {
				progenitor.removeChild(thisActant.serial)
				stopCaptor.capture(())
			}
			// TODO notify parent
		}

		if !stopWasStarted then {
			stopWasStarted = true
			isReadyToProcessMsg = false
			val undoWatchSubscriptionsAndStopMe = new BiConsumer[Actant[?, ?], List[WatchSubscription]] with doer.MonoObserver[Array[Unit]] {
				override def accept(a: Actant[?, ?], ws: List[WatchSubscription]): Unit = ws.foreach(_.unsubscribe())

				override def onSuccess(a: Array[Unit]): Unit = stopMe()

				override def onError(e: Throwable): Unit = stopMe()
			}
			activeWatchSubscriptions.forEach(undoWatchSubscriptionsAndStopMe)
			maybeSpawner.fold(stopMe()) { spawner =>
				spawner.stopChildren().triggerSync(undoWatchSubscriptionsAndStopMe)
			}
			activeWatchSubscriptions.clear()
		}
		stopCaptor
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
				ToRestart(false, _ => rw.behavior)
			case Unhandled =>
				// TODO log it
				ToContinue
		}
	}

	/** Sets the "ready to process messages" flag of this actant after processing all messages that were submitted to the [[Inqueue]] but are not jet visible in the [[Inbox]].
	 * This is the only method that sets the [[isReadyToProcessMsg]] flag to true.
	 * Since the "inbox becomes nonempty" notifications from the [[Inqueue]] are ignored while its value was false, the transition to `true` should be done after ensuring all pending messages were processed.
	 * Should be called withing the [[doer]] only. */
	private def beReadyToProcess(): Unit = {
		doer.checkWithin()
		assert(!stopWasStarted && !isReadyToProcessMsg)
		if inbox.maybeNonEmpty then doer.run {
			if !stopWasStarted then {
				inbox.withdraw().fold(beReadyToProcess())(processMessages)
			}
		} else isReadyToProcessMsg = true
	}

	/** Should be called by the [[Inqueue]] whenever it receives a message while the [[Inbox]] is empty.
	 * Differs from the other variant in that this method is designed for implementations of the [[Inqueue]] which queue messages concurrently (not within the [[doer]] of this actant).
	 * Should be called within the [[doer]] only.
	 * */
	final def onInboxBecomesNonempty(): Unit = {
		doer.checkWithin()
		if isReadyToProcessMsg then {
			isReadyToProcessMsg = false
			inbox.withdraw().fold(beReadyToProcess())(processMessages)
		}
	}

	/** Should be called by the [[Inqueue]] whenever it receives a message while the [[Inbox]] is empty.
	 * Differs from the other variant in that this method is designed for implementations of the [[Inqueue]] which queue messages within the [[doer]] of this actant.
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

	/** Process the received message and all the pending messages queue in the [[ActantCore.inbox]]
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
			case tr: ToRestart => selfRestart(tr.stopChildren, tr.restartBehaviorBuilder).triggerAndForget(true)
		}
	}


	override def diagnose: doer.Capturer[ActantDiagnostic] =
		doer.Capturer_defer { () =>
			for childrenDiagnostics <- doer.Capturer_sequenceToArray(children.values.map(_.diagnose.onBehalfOf(doer)))
				yield ActantDiagnostic(thisActant.isReadyToProcessMsg, thisActant.isMarkedToStop, thisActant.stopWasStarted, inbox.size, inbox.iterator, childrenDiagnostics)
		}

	override def staleDiagnose: ActantDiagnostic =
		val childrenDiagnostic = try childrenGates.map(_._2.staleDiagnose).toArray catch {
			case NonFatal(e) => Array.empty[ActantDiagnostic]
		}
		ActantDiagnostic(thisActant.isReadyToProcessMsg, thisActant.isMarkedToStop, thisActant.stopWasStarted, inbox.size, Iterator.empty, childrenDiagnostic)
}
