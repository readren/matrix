package readren.matrix
package core

import readren.taskflow.{Doer, Maybe}

import scala.collection.MapView

abstract class ReactantRelay[-U] {

	/** thread-safe */
	val serial: Reactant.SerialNumber
	/** thread-safe */
	val doer: MatrixDoer
	/** thread-safe */
	val endpointProvider: EndpointProvider[U]
	/** thread-safe */
	val path: String

	
	/** Indicates whether this [[Reactant]] was marked to be stopped, which does not necessarily mean that the stop process has already started.
	 * 
	 * This method is thread-safe. */
	def isMarkedToBeStopped: Boolean
	
	/** Should be called withing the [[doer]]. */
	def spawn[V](
		childFactory: ReactantFactory,
		childDoer: MatrixDoer = doer.matrix.provideDefaultDoer
	)(
		initialChildBehaviorBuilder: ReactantRelay[V] => Behavior[V]
	)(
		using isSignalTest: IsSignalTest[V]
	): doer.Duty[ReactantRelay[V]]

	/** Should be called within the [[doer]]. */
	def children: MapView[Long, ReactantRelay[?]]

	/**
	 * Instructs to stop this [[Reactant]].
	 * Supports being called from anywhere at any moment and many times.
	 * If this reactant is processing a message when this method is called, the process of that single message will continue but no other message will be processed after it.
	 * It is not necessary to trigger the execution of the returned [[Duty]] to start the stop process. The result can be ignored.
	 * 
	 * This method is thread-safe.
	 * @return a [[Duty]] that completes when this [[Reactant]] is fully stopped. */
	def stop(): doer.Duty[Unit]
	
	/** A [[SubscriptableDuty]] that completes when this [[Reactant]] is fully stopped (after the [[StopReceived]] signal was handled and this [[Reactant]] was removed from its progenitor's children list).
	 * 
	 * This duty is the same as the returned by the [[stop]] method.
	 * 
 	 * This method is thread-safe but some methods of the returned [[SubscriptableDuty]] require being called withing the [[doer]]. */
	def stopDuty: doer.SubscriptableDuty[Unit]

	/** Registers this [[Reactant]] to be notified with the specified signal when the given `watchedReactant` is fully stopped.
	 *
	 * **Note:** The `watchedReactant` does not send the notification to this [[Reactant]]'s [[Receiver]] (via the inbox) as a regular message.
	 * Instead, the notification behaves like a signal: an execution of `behavior.handle(childStoppedSignal)` is queued directly in the task queue of this [[Reactant.doer]]'s executor.
	 * Consequently, the [[Behavior.handler]] will handle the notification before processing any messages pending in the inbox if this reactant uses a concurrent message buffer (e.g., [[ConcurrentUnboundedFifo]]).
	 * **Note:** This method may return before the subscription is completed. The optional `subscriptionCompleted` parameter may be used to know when that happens.  
	 * **Usage:** This method must be called within the [[doer]].
	 *
	 * @param watchedReactant The [[Reactant]] to be observed.
	 * @param stoppedSignal   The signal to be passed to the `[[Behavior.handle]]` method of this [[Reactant]]'s behavior after the `watchedReactant` is fully stopped.
	 * @param univocally      When `true`, any existing subscriptions to the `watchedReactant` are cleared. This mode avoids redundant subscriptions that might occur after a restart.
	 *                        When `false`, the behavior must handle potential duplicate subscriptions after a restart. This mode is useful when two [[Behaviors]] combined with
	 *                        [[Behavior.unitedNest]] watch the same [[Reactant]].
	 * @param subscriptionCompleted An optional [[Doer.Covenant]] that will be fulfilled when the subscription process completes.
	 * @return A [[WatchSubscription]] that can be used to cancel the subscription, if needed.
	 */
	def watch[SS <: U](watchedReactant: ReactantRelay[?], stoppedSignal: SS, univocally: Boolean = true, subscriptionCompleted: Maybe[doer.Covenant[Unit]] = Maybe.empty): Maybe[WatchSubscription]
	
	/** Provides diagnostic information about the current instance. */
	def diagnose: doer.Duty[ReactantDiagnostic]

	/** Provides diagnostic information about the current instance that may be stale due to cache visibility issues across processor cores. */
	def staleDiagnose: ReactantDiagnostic
}
