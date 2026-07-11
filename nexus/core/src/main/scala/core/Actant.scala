package readren.nexus
package core

import readren.common.Maybe
import readren.sequencer.Doer

import scala.collection.MapView

/** The facade of a [[ActantCore]] */
abstract class Actant[-U, +D <: Doer] extends Procreative {

	val serial: ActantCore.SerialNumber
	val doer: D
	val receptorProvider: ReceptorProvider[U]
	/** The [[Nexus]] this [[Actant]] instance is part of. */
	val nexus: Nexus

	export nexus.provideDoer

	/** Indicates whether this [[ActantCore]] was marked to be stopped, which does not necessarily mean that the stop process has already started.
	 *
	 * This method is thread-safe. */
	def isMarkedToBeStopped: Boolean

	/** Creates a child [[ActantCore]] backed by the specified [[Doer]]
	 * Calls must be within the [[doer]]. */
	def spawn[V, CD <: Doer](
		childFactory: ActantFactory,
		childDoer: CD
	)(
		initialChildBehaviorBuilder: Actant[V, CD] => Behavior[V]
	)(
		using isSignalTest: IsSignalTest[V]
	): doer.Capturer[Actant[V, CD]]

	/** Calls must be within the [[doer]]. */
	def children: MapView[Long, Actant[?, ?]]

	/**
	 * Instructs to stop this [[ActantCore]].
	 * Supports being called from anywhere at any moment and many times.
	 * If this actant is processing a message when this method is called, the process of that single message will continue but no other message will be processed after it.
	 * TODO consider adding a parameter of type `U` to this method to allow the actants that are [[watch]]ing this [[Actant]] receive a clue of the cause. This would be convenient to:
	 *   - Avoid the necessity for the watched [[Actant]] to have a [[Receptor]] of the [[watch]]ing [[Actant]].
	 *   - Simplify the watcher's logic given [[Signal]]s skip the messages queue and, therefore, any message intended to inform the cause may arrive after the stop signal, forcing the watcher to remember the situation during the gap.
	 *
	 * This method is thread-safe.
	 * @return a [[Task]] that completes when this [[ActantCore]] is fully stopped. */
	def stop(): doer.Capturer[Unit]

	/** A [[SubscriptableTask]] that completes when this [[ActantCore]] is fully stopped (after the [[StopReceived]] signal was handled and this [[ActantCore]] was removed from its progenitor's children list).
	 *
	 * This task is the same as the returned by the [[stop]] method.
	 *
	 * This method is thread-safe but some methods of the returned [[SubscriptableTask]] require being called within the [[doer]]. */
	def stopCapturer: doer.Capturer[Unit]

	/** Registers this [[ActantCore]] to be notified with the specified signal when the given `watchedActant` is fully stopped.
	 *
	 * **Note:** The `watchedActant` does not send the notification to this [[ActantCore]]'s [[Inqueue]] (via the inbox) as a regular message.
	 * Instead, the notification behaves like a signal: an execution of `behavior.handle(childStoppedSignal)` is queued directly in the task queue of this [[ActantCore.doer]]'s executor.
	 * Consequently, the [[Behavior.handler]] will handle the notification before processing any messages pending in the inbox if this actant uses a concurrent message buffer (e.g., [[ConcurrentUnboundedFifo]]).
	 * **Note:** This method may return before the subscription is completed. The optional `subscriptionCompleted` parameter may be used to know when that happens.  
	 * **Usage:** This method must be called within the [[doer]].
	 *
	 * @param watchedActant The [[ActantCore]] to be observed.
	 * @param stoppedSignalBuilder A builder for the signal to be passed to the `[[Behavior.handle]]` method of this [[ActantCore]]'s behavior after the `watchedActant` is fully stopped.
	 * @param univocally      When `true`, any existing subscriptions to the `watchedActant` are cleared. This mode avoids redundant subscriptions that might occur after a restart.
	 *                        When `false`, the behavior must handle potential duplicate subscriptions after a restart. This mode is useful when two [[Behaviors]] combined with
	 *                        [[Behavior.unitedNest]] watch the same [[ActantCore]].
	 * @param subscriptionCompleted An optional [[Doer.Captor]] that will be fulfilled when the subscription process completes.
	 * @return A [[WatchSubscription]] that can be used to cancel the subscription, if needed.
	 */
	def watch[SS <: U](watchedActant: Actant[?, ?], stoppedSignalBuilder: (Unit | Throwable) => SS, univocally: Boolean = true, subscriptionCompleted: Maybe[doer.Captor[Unit]] = Maybe.empty): Maybe[WatchSubscription]

	/** Provides diagnostic information about the current instance.
	 * The different nested [[ActantDiagnostic]] are build by different [[Doer]] instances so they may be inconsistent. */
	def diagnose: doer.Capturer[ActantDiagnostic]

	/** Provides diagnostic information about the current instance that may be stale due to cache visibility issues across processor cores. */
	@deprecated
	def staleDiagnose: ActantDiagnostic
}
