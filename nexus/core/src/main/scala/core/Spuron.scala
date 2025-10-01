package readren.nexus
package core

import readren.common.Maybe
import readren.sequencer.Doer

import scala.collection.MapView

/** The facade of a [[SpuronCore]] */
abstract class Spuron[-U, +D <: Doer] extends Procreative {

	val serial: SpuronCore.SerialNumber
	val doer: D
	val endpointProvider: EndpointProvider[U]
	/** The [[Nexus]] this [[Spuron]] instance is part of. */
	val nexus: Nexus

	export nexus.provideDoer

	/** Indicates whether this [[SpuronCore]] was marked to be stopped, which does not necessarily mean that the stop process has already started.
	 *
	 * This method is thread-safe. */
	def isMarkedToBeStopped: Boolean

	/** Creates a child [[SpuronCore]] backed by the specified [[Doer]]
	 * Calls must be within the [[doer]]. */
	def spawns[V, CD <: Doer](
		childFactory: SpuronFactory,
		childDoer: CD
	)(
		initialChildBehaviorBuilder: Spuron[V, CD] => Behavior[V]
	)(
		using isSignalTest: IsSignalTest[V]
	): doer.Duty[Spuron[V, CD]]

	/** Calls must be within the [[doer]]. */
	def children: MapView[Long, Spuron[?, ?]]

	/**
	 * Instructs to stop this [[SpuronCore]].
	 * Supports being called from anywhere at any moment and many times.
	 * If this spuron is processing a message when this method is called, the process of that single message will continue but no other message will be processed after it.
	 * It is not necessary to trigger the execution of the returned [[Duty]] to start the stop process. The result can be ignored.
	 *
	 * This method is thread-safe.
	 * @return a [[Duty]] that completes when this [[SpuronCore]] is fully stopped. */
	def stop(): doer.Duty[Unit]

	/** A [[SubscriptableDuty]] that completes when this [[SpuronCore]] is fully stopped (after the [[StopReceived]] signal was handled and this [[SpuronCore]] was removed from its progenitor's children list).
	 *
	 * This duty is the same as the returned by the [[stop]] method.
	 *
	 * This method is thread-safe but some methods of the returned [[SubscriptableDuty]] require being called within the [[doer]]. */
	def stopDuty: doer.SubscriptableDuty[Unit]

	/** Registers this [[SpuronCore]] to be notified with the specified signal when the given `watchedSpuron` is fully stopped.
	 *
	 * **Note:** The `watchedSpuron` does not send the notification to this [[SpuronCore]]'s [[Receiver]] (via the inbox) as a regular message.
	 * Instead, the notification behaves like a signal: an execution of `behavior.handle(childStoppedSignal)` is queued directly in the task queue of this [[SpuronCore.doer]]'s executor.
	 * Consequently, the [[Behavior.handler]] will handle the notification before processing any messages pending in the inbox if this spuron uses a concurrent message buffer (e.g., [[ConcurrentUnboundedFifo]]).
	 * **Note:** This method may return before the subscription is completed. The optional `subscriptionCompleted` parameter may be used to know when that happens.  
	 * **Usage:** This method must be called within the [[doer]].
	 *
	 * @param watchedSpuron The [[SpuronCore]] to be observed.
	 * @param stoppedSignal   The signal to be passed to the `[[Behavior.handle]]` method of this [[SpuronCore]]'s behavior after the `watchedSpuron` is fully stopped.
	 * @param univocally      When `true`, any existing subscriptions to the `watchedSpuron` are cleared. This mode avoids redundant subscriptions that might occur after a restart.
	 *                        When `false`, the behavior must handle potential duplicate subscriptions after a restart. This mode is useful when two [[Behaviors]] combined with
	 *                        [[Behavior.unitedNest]] watch the same [[SpuronCore]].
	 * @param subscriptionCompleted An optional [[Doer.Covenant]] that will be fulfilled when the subscription process completes.
	 * @return A [[WatchSubscription]] that can be used to cancel the subscription, if needed.
	 */
	def watch[SS <: U](watchedSpuron: Spuron[?, ?], stoppedSignal: SS, univocally: Boolean = true, subscriptionCompleted: Maybe[doer.Covenant[Unit]] = Maybe.empty): Maybe[WatchSubscription]

	/** Provides diagnostic information about the current instance. */
	def diagnoses: doer.Duty[SpuronDiagnostic]

	/** Provides diagnostic information about the current instance that may be stale due to cache visibility issues across processor cores. */
	def staleDiagnose: SpuronDiagnostic
}
