package readren.matrix

import Reactant.SerialNumber

import readren.taskflow.{Doer, Maybe}

import java.util.concurrent.atomic.AtomicBoolean
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
	def spawn[A](
		childReactantFactory: ReactantFactory,
		doerAssistantProviderRef: Matrix.DoerAssistantProviderRef[?] = doer.matrix.defaultDoerAssistantProviderRef
	)(
		initialChildBehaviorBuilder: ReactantRelay[A] => Behavior[A]
	)(
		using isSignalTest: IsSignalTest[A]
	): doer.Duty[ReactantRelay[A]]

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

	/** Registers this [[Reactant]] to be notified with the provided signal when the specified [[Reactant]] is fully stopped.
	 * The notification is not sent to this [[Reactant]]'s [[Receiver]] as a regular messages. It behaves like signals: an execution of {{{behavior.handle(childStoppedSignal)}}} is queued directly in the task-queue of this [[Reactant.doer]]'s executor.
	 * 
	 * Should be called within the [[doer]].
	 * @param watchedReactant the [[Reactant]] to be watched.
	 * @param stoppedSignal the signal that the [[Behavior.handle]] method of this [[Reactant]]'s behavior will receive after the `watchedReactant` is fully stopped.
	 * @param univocally when `true`, existing subscriptions to the `watchedReactant` are undone. This mode prevents undesired repeated subscriptions after a restart.
	 *                   when `false`, the behavior should be responsible to avoid repeated subscriptions after a restart. This mode is particularly useful when two [[Behaviors]] united with [[Behavior.unitedNest]] watch the same [[Reactant]].
	 * @return a [[WatchSubscription]] that may be used to undo the subscription. */
	def watch[SS <: U](watchedReactant: ReactantRelay[?], stoppedSignal: SS, univocally: Boolean = true): Maybe[WatchSubscription]
	
	/** Provides diagnostic information about the current instance. */
	def diagnose: doer.Duty[ReactantDiagnostic]

	/** Provides diagnostic information about the current instance that may be stale due to cache visibility issues across processor cores. */
	def staleDiagnose: ReactantDiagnostic
}
