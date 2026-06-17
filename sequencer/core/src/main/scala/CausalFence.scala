package readren.sequencer

import Doer.{CausalAnchorArrival, RollbackApplication}

import readren.common.Maybe

/** A fence that serializes non-failing state transitions with causal fulfillment semantics.
 * It enforces causal ordering; lineage continues indefinitely.
 *
 * Each state transition attempt is causally anchored to the previous one, ensuring that each update observes the latest committed state, and that all updates are fulfilled in causal order.
 * Speculative updates may be rolled back before becoming visible.
 *
 * Core idea: CausalFence[A] is a causal sequencing primitive. It maintains a queue of state updaters implemented with a chain of [[Covenant]]s, whose tail is [[lastEnqueuedCovenant]]. The tail represents the latest step in a causal chain. Each [[advance]] and [[causalAnchor]] call enqueues a new [[Covenant]], chained to the previous one with a subscription to its completion.
 *
 * This fence provides **causal fulfillment semantics**:
 * - Each transition yields a committed, visible state. Rolled-back transitions fulfill with the previous state.
 * - The causal chain is never broken: all updates are fulfilled and causally ordered.
 * - The committed lineage includes all transitions.
 * - Transitions cannot fail — they either commit a new state or retain the previous one.
 *
 * Fundamental Invariants
 * - Single updater invariant: At most one `primaryStateUpdater` (the function passed to [[advance]]) is in progress at a time. Subsequent advances wait until the previous one completes.
 * - Advance ordering invariant: The `primaryStateUpdater` passed to [[advance]] runs after all consumers that subscribed to the previous [[Covenant]] in the queue (via earlier [[advance]] or [[causalAnchor]]) have completed their synchronous part.
 * - Anchor freshness invariant: - A consumer subscribed to [[causalAnchor]] observes the same state that a `primaryStateUpdater` would receive if [[advance]] were invoked at that moment. Ordering is guaranteed relative to the last completed advance at subscription time, but not relative to advances invoked later.
 * - Rollback integrity invariant: For speculative advances, derived updates must not be composed into the primary updater, otherwise rollback semantics are broken.
 *
 * Invariants related to derived state:
 * - Game-changing invariant: Immediately after an [[advance]] or [[causalAnchor]] call, there are no pending advances other than the one just created. The returned [[Covenant]] (seen as [[LatchingTask]]) is a fresh tail. Any immediate synchronous subscription to it is guaranteed to be the first subscriber in its list. Therefore, when the covenant fulfills, that consumer sees the up‑to‑date state deterministically. This invariant eliminates the race where another [[advance]] could sneak in and publish a newer state before or while the synchronous consumer runs. The consumer is deterministically ordered before any updater that follows in the causal chain.
 * - Anchor specificity invariant: Derived updates that require strict ordering relative to a particular primary transition must anchor to that transition’s [[Covenant]] (the one returned by [[advance]]), not to a generic tail snapshot (as the returned by [[causalAnchor]]).
 * - Deterministic derived ordering invariant: When derived updates have dependencies, their execution order must be enforced by anchoring the dependent update and deriving prerequisites synchronously from the anchored state, or by composing into the primary updater if not speculative.
 * - Rollback integrity invariant: No derived side-effect may commit externally during a speculative advance before the primary step is safely committed; otherwise rollback can leave the system in an inconsistent state.
 * - Idempotence/compensation invariant: Any derived side-effect that can be re-run or rolled back must be idempotent or have a compensating action to preserve causal correctness under retries or rollback.
 *
 * Invariants inherited from [[Covenant]]:
 * - Sequential consumer invariant: The [[LatchingTask]] returned by [[advance]] and [[causalAnchor]] is a [[Covenant]] and therefore the subscribed consumers are invoked in registration order. The synchronous part of each consumer runs to completion before the next begins.
 * @param initialState the initial committed state, already visible and causally anchored. */
class CausalFence[A, D <: Doer](val doer: D)(initialState: A) {
	private var lastCommittedCovenant: doer.Covenant[A] = new doer.Covenant(Maybe(initialState))
	private var lastEnqueuedCovenant: doer.Covenant[A] = lastCommittedCovenant

	/** Provides asynchronous rollback capability for speculative updates.
	 *
	 * Used within [[advanceSpeculatively]] to discard an in-flight update before it becomes visible.
	 * Rollback is only effective if invoked before the update is committed.
	 * Rollback is always non-failing and fulfills the update with the previous state.
	 * The returned [[LatchingTask]] completes with the same state that becomes visible.
	 */
	trait RollbackAccessor[B <: A] {
		/** Attempts to roll back the speculative update, restoring the previous visible state.
		 *
		 * The rollback is applied if "invoked" before the update it targets has completed.
		 * The quotes around "invoked" reflect that, when `isWithinDoSerEx` is false, the rollback attempt is scheduled asynchronously and may race with the update's completion.
		 *
		 * Regardless of timing, the [[LatchingTask]] originally returned by [[advanceSpeculatively]] is fulfilled:
		 * - With the previous state if rollback succeeds
		 * - With the committed state if rollback was too late
		 *
		 * The callback receives the resulting state and a flag indicating whether rollback was applied (`false`) or too late (`true`).
		 *
		 * @param isWithinDoSerEx whether the caller is executing within the Doer's sequential executor.
		 * @param onCompleted a callback invoked with the resulting state and a flag indicating whether rollback was too late.
		 * @return the [[LatchingTask]] originally returned by the [[advanceSpeculatively]]-like method that created this [[RollbackAccessor]] instance, now fulfilled with either the committed or rolled-back state.
		 */
		def rollback(isWithinDoSerEx: Boolean = doer.isInSequence, onCompleted: (A | B, RollbackApplication) => Unit = (_, _) => ()): doer.LatchingTask[A | B]
	}

	/** @return true if the queue of primary state updaters is empty. */
	def isEmpty: Boolean = lastEnqueuedCovenant eq lastCommittedCovenant

	/** The state to which the most recent step transitioned into.
	 *
	 * Rolled-back transitions yield the previous state.
	 *
	 * Must be called within this [[Doer]].
	 *
	 * @return the last transition result.
	 */
	def committedState: A = {
		doer.checkWithin()
		lastCommittedCovenant.maybeResult.get
	}

	/** Returns a [[LatchingTask]] that yields the currently visible state.
	 *
	 * This reflects the state to which the most recent completed step transitioned into.
	 * Rolled-back transitions yield the previous state.
	 * The returned [[LatchingTask]] is always already fulfilled.
	 *
	 * Must be called within this [[Doer]].
	 * @return a [[LatchingTask]] yielding the currently visible state.
	 */
	def committed: doer.LatchingTask[A] = {
		doer.checkWithin()
		lastCommittedCovenant
	}

	/**
	 * Returns a [[LatchingTask]] that yields the same state an updater would see if [[advance]] were invoked at this moment.
	 * This provides a causal checkpoint suitable for synchronous consumers that need to derive state deterministically.\
	 * The provided state consumer, along with any synchronous subscribers to the returned [[LatchingTask]], will be executed when the anchored link of the causal chain is reached, receiving the state at that link.\
	 * **Temporal window of causal safety:**
	 * The causal guarantee holds only during the synchronous execution of a consumer synchronously subscribed to the returned [[LatchingTask]].\
	 * Methods that rely on causal visibility are safe only within the body of that consumer; once the consumer has returned, deferred or later code is no longer causally anchored.\
	 * @param stateConsumer optional callback invoked when the anchored link is reached. Executed within this [[Doer]]’s sequential executor before any consumer subscribed to the returned [[LatchingTask]]. The first parameter is the primary state; the second indicates whether the link was already reached when this method was invoked: [[ARRIVED_BEFORE]] if so, or [[ARRIVED_AFTER]] if not.
	 * @return a [[LatchingTask]] yielding the state that the next update will be causally anchored to — i.e. the same state an updater would see if [[advance]] were called at this moment.
	 * @note When derived updates (secondary state depending on primary state) have causal dependencies among themselves, deterministic order must be enforced by other means: either anchor only the dependent update and derive prerequisites synchronously from the anchored state, or — if derived updates are fast and the advance is not speculative — compose them into the `primaryStateUpdater` passed to [[advance]] or [[advanceIf]]. Composition is unsafe for speculative advances, because rollback during the derived update phase could succeed when it should not.\
	 * Independent subscriptions to [[causalAnchor]] are appropriate only for derived updates that are order‑independent.\
	 * Implementation note: The returned [[Covenant]] (exposed as a [[LatchingTask]]) participates in the causal chain by linking forward from the current tail. This ensures that immediate synchronous subscriptions are registered before fulfillment, guaranteeing deterministic observation of the up‑to‑date state. */
	def causalAnchor(stateConsumer: (A, CausalAnchorArrival) => Unit = (_, _) => ()): doer.LatchingTask[A] = {
		doer.checkWithin()
		val lec = lastEnqueuedCovenant
		val lcc = lastCommittedCovenant
		if lec eq lcc then {
			stateConsumer(lcc.maybeResult.get, Doer.ARRIVED_BEFORE)
			lec
		} else {
			val thisStepCovenant = doer.Covenant[A]()
			lastEnqueuedCovenant = thisStepCovenant
			lec.subscribeSync(a => thisStepCovenant.fulfillUnsafe(a, stateConsumer))
			thisStepCovenant
		}
	}

	/** Enqueues an asynchronous non-speculative primary-state updater.\
	 * Rollback is not supported in this method. The updater function is defined with a second parameter of type `Null` to match the internal speculative signature, allowing reuse without introducing an extra closure.\
	 * **Temporal window of causal safety:**
	 * The causal guarantee holds from the moment the `primaryStateUpdater` function is invoked until the [[LatchingTask]] returned by this method and all the consumers synchronously subscribed to it have returned.\
	 * Is worth mentioning that the provided updater will be executed after all the consumers previously and synchronously subscribed to the [[LatchingTask]] returned by [[causalAnchor]] and [[advance]]-like methods have completed.\
	 * @param primaryStateUpdater a function that computes the next state from the current one
	 * @return a [[LatchingTask]] that will be fulfilled with the new state once the update completes.
	 * @note CAUTION: The execution of consumers that are subscribed to obsolete instances of [[LatchingTask]] is not causally ordered.
	 * So, avoid memorizing [[LatchingTask]] instances returned by [[causalAnchor]] or [[advance]]-like methods; always subscribe to the instance returned by [[causalAnchor]] to ensure causal ordering of the consumers executions.
	 * Obsolete are those instances returned by methods of this [[CausalFence]] before the last call to an [[advance]]-like method. */
	inline def advance[B <: A](inline primaryStateUpdater: A => doer.Task[A | B]): doer.LatchingTask[A | B] =
		step((a, _) => Maybe(primaryStateUpdater(a)), false)


	/** Like [[advance]], but the update may be synchronously canceled by the provided updater returning [[Maybe.empty]]
	 * If the [[primaryStateUpdater]] returns some state, it is committed.
	 * If it returns [[Maybe.empty]], the update is canceled and the previous state is retained.
	 * @param primaryStateUpdater a partial function that computes the next state from the current one; the second argument is always `null`
	 * @return a [[LatchingTask]] that yields the updated state */
	inline def advanceIf[B <: A](inline primaryStateUpdater: A => Maybe[doer.Task[B]]): doer.LatchingTask[A | B] = {
		step((a, _) => primaryStateUpdater(a), false)
	}

	/** Enqueues an asynchronous speculative primary-state updater.\
	 * The provided [[RollbackAccessor]] allows the update to be withdrawn before it becomes visible.\
	 * All updates fulfill successfully, even when rolled back.\
	 * **Temporal window of causal safety:**
	 * The causal guarantee holds from the moment the `primaryStateUpdater` function is invoked until the [[LatchingTask]] returned by this method and all the consumers synchronously subscribed to it have returned.\
	 * Is worth mentioning that the provided updater will be executed after all the consumers previously and synchronously subscribed to the [[LatchingTask]] returned by [[causalAnchor]] and [[advance]]-like methods have completed.\
	 * @param primaryStateUpdater a function that computes the next state from the current one, with rollback control
	 * @return a [[LatchingTask]] that yields the updated or rolled-back state
	 * @note CAUTION: The execution of consumers that are subscribed to obsolete instances of [[LatchingTask]] is not causally ordered.
	 * So, avoid memorizing [[LatchingTask]] instances returned by [[causalAnchor]] or [[advance]]-like methods; always subscribe to the instance returned by [[causalAnchor]] to ensure causal ordering of the consumers executions.
	 * Obsolete are those instances returned by methods of this [[CausalFence]] before the last call to an [[advance]]-like method. */
	inline def advanceSpeculatively[B <: A](inline primaryStateUpdater: (A, RollbackAccessor[B]) => doer.Task[A | B]): doer.LatchingTask[A | B] =
		step[B]((a, rba) => Maybe(primaryStateUpdater(a, rba)), true)

	/** Like [[advanceSpeculatively]], but the update may be synchronously canceled by the provided updater returning [[Maybe.empty]]
	 * If the [[primaryStateUpdater]] returns some state, it is committed.
	 * If it returns [[Maybe.empty]], the update is canceled and the previous state is retained.
	 * @param primaryStateUpdater a partial function that computes the next state from the current one, with rollback control
	 * @return a [[LatchingTask]] that yields the updated state
	 * */
	inline def advanceSpeculativelyIf[B <: A](primaryStateUpdater: (A, RollbackAccessor[B]) => Maybe[doer.Task[A | B]]): doer.LatchingTask[A | B] =
		step(primaryStateUpdater, true)

	/** Internal method that performs the actual state transition.\
	 * Handles both speculative and non-speculative updates depending on the `isSpeculative` flag.\
	 * The rollback accessor is instantiated only when needed to avoid unnecessary allocations. */
	private def step[B <: A](primaryStateUpdater: (A, RollbackAccessor[B]) => Maybe[doer.Task[A | B]], isSpeculative: Boolean): doer.LatchingTask[A | B] = {
		doer.checkWithin()
		val previousStepCovenant = lastEnqueuedCovenant
		val thisStepCovenant = doer.Covenant[A | B]()
		lastEnqueuedCovenant = thisStepCovenant

		previousStepCovenant.subscribeSync { previousState =>
			val rba: RollbackAccessor[B] =
				if isSpeculative then (isWithinDoSerEx: Boolean, onCompleted: (A | B, RollbackApplication) => Unit) => thisStepCovenant.fulfill(previousState, isWithinDoSerEx, onCompleted)
				else null.asInstanceOf[RollbackAccessor[B]]
			primaryStateUpdater(previousState, rba)
				.fold {
					lastCommittedCovenant = thisStepCovenant
					thisStepCovenant.fulfillUnsafe(previousState)
				} { newStateProviderTask =>
					newStateProviderTask.subscribeSync { newState =>
						lastCommittedCovenant = thisStepCovenant
						thisStepCovenant.fulfillUnsafe(newState)
					}
				}
		}
		thisStepCovenant
	}

	/** Enqueues a synchronous non-speculative primary-state updater.\
	 * The update is applied synchronously and always fulfills with a committed state:
	 * - The [[primaryStateUpdater]] is applied to the previous state.
	 * - The resulting state is committed immediately.
	 * @param primaryStateUpdater a total function that produces the next state from the current one.
	 * @return a [[LatchingTask]] that is always fulfilled with the committed state.
	 */
	inline def jump[B <: A](inline primaryStateUpdater: A => A | B): doer.LatchingTask[A | B] =
		jumpIf[B](a => Maybe(primaryStateUpdater(a)))

	/** Like [[jump]], but the update may be canceled by the provided updater returning [[Maybe.empty]].\
	 * If the [[primaryStateUpdater]] returns some state, it is committed.
	 * If it returns [[Maybe.empty]], the update is canceled and the previous state is retained.
	 * @param primaryStateUpdater a partial function that produces a new state from the previous one
	 * @return a [[LatchingTask]] that is always fulfilled with the committed state
	 */
	def jumpIf[B <: A](primaryStateUpdater: A => Maybe[A | B]): doer.LatchingTask[A | B] = {
		doer.checkWithin()
		val previousStepCovenant = lastEnqueuedCovenant
		val thisStepCovenant = doer.Covenant[A | B]()
		lastEnqueuedCovenant = thisStepCovenant

		previousStepCovenant.subscribeSync { previousState =>
			primaryStateUpdater(previousState)
				.fold {
					lastCommittedCovenant = thisStepCovenant
					thisStepCovenant.fulfillUnsafe(previousState)
				} { newState =>
					lastCommittedCovenant = thisStepCovenant
					thisStepCovenant.fulfillUnsafe(newState)
				}
		}
		thisStepCovenant
	}

	override def toString: String = {
		s"CausalFence(lastEnqueuedCovenant=${lastEnqueuedCovenant.toString}, lastCommittedCovenant=${lastCommittedCovenant.toString})"
	}
}