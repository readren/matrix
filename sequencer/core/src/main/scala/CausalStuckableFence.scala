package readren.sequencer

import Doer.{CausalAnchorArrival, RollbackApplication}

import readren.common.Maybe

import scala.util.{Failure, Success, Try}

/** A fence that enforces causal ordering and may become stuck; once stuck, progression halts: subsequent update attempts are skipped and the returned [[LatchingTask]] yield the same halting state.\
 * Each state transition attempt is causally anchored to the previous one, ensuring that each update observes the latest visible state, and that all updates are fulfilled in causal order. Speculative updates may be rolled back before becoming visible.\
 * Differs from [[CausalFence]] in that it may become stuck on specific states. Once a stuckable state is committed (currently [[Failure]]), the fence halts: subsequent transition attempts are skipped and yield the same stuck state. A transition to a stuck state becomes the final committed state, and no further transitions are accepted.\
 * Core idea: CausalStuckableFence[A] is a causal sequencing primitive with stuckness semantics. It maintains a single tail commitment ([[lastEnqueuedCommitment]]) that represents the latest step in a causal chain. Each [[advanceIf]] and [[causalAnchor]] call enqueues a new [[Commitment]], chained to the previous one, unless the fence is already stuck.\
 * This fence provides **causal fulfillment semantics with stuckness**:
 * - Each successful transition yields a committed, visible state. Rolled-back transitions fulfill with the previous state.
 * - The causal chain is never broken: all updates are fulfilled and causally ordered until a stuck state is reached.
 * - The committed lineage includes all transitions to non-stuck states, and ends in a stuck one.
 * - If a transition to a stuck state occurs, the stuck state is committed and the fence halts — subsequent update attempts are skipped and yield the same stuck state.
 *
 * **Fundamental Invariants:**
 * - Single updater invariant: At most one `primaryStateUpdater` (the function passed to [[advanceIf]]) is in progress at a time. Subsequent advances wait until the previous one completes.
 * - Advance ordering invariant: The `primaryStateUpdater` passed to [[advanceIf]] runs after all consumers that subscribed to the previous [[Commitment]] in the queue (via earlier [[advanceIf]] or [[causalAnchor]]) have completed their synchronous part.
 * - Anchor freshness invariant: A consumer subscribed to [[causalAnchor]] observes the same state that a `primaryStateUpdater` would receive if [[advanceIf]] were invoked at that moment. Ordering is guaranteed relative to the last completed advance at subscription time, but not relative to advances invoked later. If the fence is stuck, the anchor yields the stuck state deterministically.
 * - Stuckness invariant: Once a stuck state is committed, no further transitions are accepted. All subsequent advances or anchors yield the same stuck state.
 * - Rollback integrity invariant: For speculative advances, derived updates must not be composed into the primary updater, otherwise rollback semantics are broken.
 *
 * **Invariants related to derived state:**
 * - Game-changing invariant: Immediately after an [[advanceIf]] or [[causalAnchor]] call (if not stuck), there are no pending advances other than the one just created. The returned [[Commitment]] (seen as [[LatchingTask]]) is a fresh tail. Any immediate synchronous subscription to it is guaranteed to be the first subscriber in its list. Therefore, when the commitment fulfills, that consumer sees the up‑to‑date state deterministically. This invariant eliminates the race where another [[advanceIf]] could sneak in and publish a newer state before or while the synchronous consumer runs. The consumer is deterministically ordered before any updater that follows in the causal chain.
 * - Anchor specificity invariant: Derived updates that require strict ordering relative to a particular primary transition must anchor to that transition’s [[Commitment]] (the one returned by [[advanceIf]]), not to a generic tail snapshot (as returned by [[causalAnchor]]).
 * - Deterministic derived ordering invariant: When derived updates have dependencies, their execution order must be enforced by anchoring the dependent update and deriving prerequisites synchronously from the anchored state, or by composing into the primary updater if not speculative.
 * - Rollback integrity invariant: No derived side effect may commit externally during a speculative advance before the primary step is safely committed; otherwise rollback can leave the system in an inconsistent state.
 * - Idempotence/compensation invariant: Any derived side effect that can be re-run or rolled back must be idempotent or have a compensating action to preserve causal correctness under retries or rollback.
 *
 * **Invariants inherited from [[Commitment]]:**
 * - Sequential consumer invariant: The [[LatchingTask]] returned by [[advanceIf]] and [[causalAnchor]] is a [[Commitment]] and therefore the subscribed consumers are invoked in registration order. The synchronous part of each consumer runs to completion before the next begins.\
 * @param initialState the initial state, already visible and committed (may be stuck if it is a stuckable state). */
class CausalStuckableFence[A, D <: Doer](val doer: D)(initialState: Try[A]) {
	private var lastCommittedCommitment: doer.Commitment[A] = doer.Commitment(Maybe(initialState))
	private var lastEnqueuedCommitment: doer.Commitment[A] = lastCommittedCommitment

	/** Provides asynchronous rollback capability for speculative updates.\
	 * A [[RollbackAccessor]] is passed to speculative state transitions to allow them to cancel themselves before becoming visible.\
	 * Rollback is only effective if invoked before the update is committed.\
	 * If rollback is invoked too late, the accessor still complete the [[LatchingTask]] with the committed state and signals that rollback was ineffective. */
	trait RollbackAccessor[B <: A] {
		/** Attempts to roll back the speculative update, restoring the previous visible state.\
		 * The rollback is applied if "invoked" before the update it targets has completed.\
		 * The quotes around "invoked" reflect that, when `isWithinDoSerEx` is false, the rollback attempt is scheduled asynchronously and may race with the update's completion.\
		 * Regardless of timing, the [[LatchingTask]] originally returned by [[advanceSpeculativelyIf]] is fulfilled:
		 * - With the previous state if rollback succeeds.
		 * - With the committed state if rollback was too late.\
		 * The callback receives the resulting state and a flag indicating whether rollback was applied (`false`) or too late (`true`).\
		 * @param isWithinDoSerEx whether the caller is executing within the Doer's sequential executor.
		 * @param onCompleted a callback invoked with the resulting state and a flag indicating whether rollback was too late.
		 * @return the [[LatchingTask]] originally returned by the [[advanceSpeculatively]]-like method that created this [[RollbackAccessor]], now fulfilled with either the committed or rolled-back state. */
		def rollback(isWithinDoSerEx: Boolean = doer.isInSequence, onCompleted: (Try[A | B], RollbackApplication) => Unit = (_, _) => ()): doer.LatchingVenture[A | B]
	}

	/** The state to which the most recent step transitioned into, or a [[Failure]] if the transition failed.\
	 * Rolled-back transitions yield the previous state.\
	 * Must be called within this [[Doer]].\
	 * @return the last transition result */
	inline def committedState: Try[A] = {
		doer.checkWithin()
		lastCommittedCommitment.maybeResult.get
	}

	/** Returns a [[LatchingTask]] that yields the currently visible state or failure.\
	 * This reflects the state to which the most recent step transitioned into, or a [[Failure]] if the transition failed.\
	 * Rolled-back transitions yield the previous state.\
	 * The returned [[LatchingTask]] is always already completed.\
	 * Must be called within this [[Doer]].\
	 * @return a [[LatchingTask]] yielding the currently visible state or failure. */
	def committed: doer.LatchingVenture[A] = {
		doer.checkWithin()
		lastCommittedCommitment
	}

	/** Returns a [[LatchingTask]] that yields the same state an updater would see if [[advanceIf]] were invoked at this moment.\
	 * This provides a causal checkpoint suitable for synchronous consumers that need to derive state deterministically.\
	 * The returned [[LatchingTask]] is backed by a fresh [[Commitment]] that forwards from the current tail [[Commitment]].\
	 * This ensures that immediate synchronous subscriptions to the returned [[LatchingTask]] are registered before completion, making them the first subscribers on the fresh [[Covenant]] and guaranteeing deterministic observation of the up‑to‑date state.\
	 * **Temporal window of causal safety:**\
	 * The causal guarantee holds only during the synchronous execution of a consumer subscribed to the returned [[LatchingTask]].\
	 * Calls to methods that rely on causal visibility are safe only within the body of that consumer; once the consumer has returned, deferred or later code is no longer causally anchored.\
	 * @note When derived updates (those done to secondary state that derives from the primary state) have causal dependencies among themselves, you must enforce deterministic order by other means: use causal derivation functions (anchor only the dependent update and derive prerequisites synchronously from the anchored state), or, if derived updates are fast and the advance is not speculative, compose them into the `primaryStateUpdater` passed to [[advanceIf]] or [[advanceIf]]. Composition is not safe for speculative advances, because rollback during the derived update phase could succeed despite it shouldn’t.\
	 * Independent subscriptions to [[causalAnchor]] are appropriate only for derived updates that are order‑independent.\
	 * @param stateConsumer optional callback invoked when the anchored link is reached. Executed within this [[Doer]]’s sequential executor before any consumer subscribed to the returned [[LatchingDuty]]. The first parameter is the primary state; the second indicates whether the link was already reached when this method was invoked: [[ARRIVED_BEFORE]] if so, or [[ARRIVED_AFTER]] if not.
	 * @return a [[LatchingTask]] yielding the state that the next update will be causally anchored to — i.e. the same state an updater would see if [[advanceIf]] were called at this moment. */
	def causalAnchor(stateConsumer: (Try[A], CausalAnchorArrival) => Unit = (_, _) => ()): doer.LatchingVenture[A] = {
		doer.checkWithin()
		val lec = lastEnqueuedCommitment
		val lcc = lastCommittedCommitment
		if lec eq lcc then {
			stateConsumer(lcc.maybeResult.get, Doer.ARRIVED_BEFORE)
			lec
		} else {
			val thisStepCommitment = doer.Commitment[A]()
			lastEnqueuedCommitment = thisStepCommitment
			lec.engage(a => thisStepCommitment.completeUnsafe(a, stateConsumer))
			thisStepCommitment
		}
	}

	/** Enqueues an asynchronous non-speculative primary-state updater.\
	 * If this [[CausalStuckableFence]] gets stuck (because a previous update failed) before the provided updater is executed, it is skipped and the returned [[LatchingTask]] is completed with the same failure.\
	 * Rollback is not supported in this method. The updater function is defined with a second parameter of type `Null` to match the internal speculative signature, allowing reuse without introducing an extra closure.\
	 * **Temporal window of causal safety:**\
	 * The causal guarantee holds from the moment the `primaryStateUpdater` function is invoked until the [[LatchingDuty]] returned by this method and all the consumers synchronously subscribed to it have returned.\
	 * Is worth mentioning that the provided updater will be executed after all the consumers previously and synchronously subscribed to the [[LatchingDuty]] returned by [[causalAnchor]] and [[advance]]-like methods have completed.\
	 * @param primaryStateUpdater a function that computes the next state from the current one
	 * @return a [[LatchingTask]] that will be fulfilled with the new state once the update completes.
	 * @note CAUTION: The execution of consumers that are subscribed to obsolete instances of [[LatchingDuty]] is not causally ordered.\
	 * So, avoid memorizing [[LatchingDuty]] instances returned by [[causalAnchor]] or [[advance]]-like methods; always subscribe to the instance returned by [[causalAnchor]] to ensure causal ordering of the consumers executions.
	 * Obsolete are those instances returned by methods of this [[CausalFence]] before the last call to an [[advance]]-like method. */
	inline def advance[B <: A](inline primaryStateUpdater: A => doer.Venture[A | B]): doer.LatchingVenture[A | B] =
		step((a, _) => Maybe(primaryStateUpdater(a)), false)

	/** Like [[advance]], but the update may be synchronously canceled by the provided updater returning [[Maybe.empty]].\
	 * If the [[primaryStateUpdater]] returns some state, it is commited.\
	 * If it returns [[Maybe.empty]], the update is canceled and the previous state is retained.\
	 * @param primaryStateUpdater a partial function that computes the next state from the current one; the second argument is always `null`
	 * @return a [[LatchingTask]] that yields the updated state */
	inline def advanceIf[B <: A](inline primaryStateUpdater: A => Maybe[doer.Venture[A | B]]): doer.LatchingVenture[A | B] = {
		step((a, _) => primaryStateUpdater(a), false)
	}

	/** Enqueues an asynchronous speculative primary-state updater.\
	 * The provided [[RollbackAccessor]] allows the update to be withdrawn before it becomes visible.\
	 * If the previous step failed, this transition is skipped and the returned [[LatchingTask]] is completed with the same failure.\
	 * If rollback is invoked before visibility, the update is canceled and the previous state is kept.\
	 * Only successful transitions update the committed state.\
	 * **Temporal window of causal safety:**\
	 * The causal guarantee holds from the moment the `primaryStateUpdater` function is invoked until the [[LatchingDuty]] returned by this method and all the consumers synchronously subscribed to it have returned.\
	 * @param primaryStateUpdater a function that computes the next state from the current one, with rollback control
	 * @return a [[LatchingTask]] that yields the updated or rolled-back state */
	inline def advanceSpeculatively[B <: A](inline primaryStateUpdater: (A, RollbackAccessor[B]) => doer.Venture[A | B]): doer.LatchingVenture[A | B] =
		advanceSpeculativelyIf[B]((a, rba) => Maybe(primaryStateUpdater(a, rba)))

	/** Attempts a speculative state transition anchored to the previous one.\
	 * If the previous step failed, this transition is skipped and the returned [[LatchingTask]] is completed with the same failure.\
	 * If rollback is invoked before visibility, the update is canceled and the previous state is kept.\
	 * Only successful transitions update the committed state.\
	 * @param primaryStateUpdater a function that computes the next state from the current one, with rollback capability
	 * @param isWithinDoSerEx whether the caller is executing within the Doer's sequential executor
	 * @return a [[LatchingTask]] that will be completed with the new state if not rolled-back in time, the previous state if rolled-back in time, or the previous failure due to which the update was skipped. */
	def advanceSpeculativelyIf[B <: A](primaryStateUpdater: (A, RollbackAccessor[B]) => Maybe[doer.Venture[A | B]], isWithinDoSerEx: Boolean = doer.isInSequence): doer.LatchingVenture[A | B] = {
		if isWithinDoSerEx then step(primaryStateUpdater, true)
		else {
			val commitment = doer.Commitment[A]()
			doer.run(commitment.completeWith(step(primaryStateUpdater, true), true))
			commitment
		}
	}

	/** Internal method that performs the actual state transition.\
	 * Handles both deterministic and speculative updates depending on the `isSpeculative` flag.\
	 * If the previous step failed, the update is not executed and the [[LatchingTask]] corresponding to this step is completed with the same failure. The rollback accessor is instantiated only when needed to avoid unnecessary allocations.\
	 * Only successful transitions update the committed state.\
	 * @param primaryStateUpdater the transition function, optionally accepting a [[RollbackAccessor]]
	 * @param isSpeculative whether the update is speculative and may be rolled back
	 * @return a [[LatchingTask]] that will be completed with the new state if not rolled-back in time, the previous state if rolled-back in time, or the previous failure due to which the update was skipped. */
	private def step[B <: A](primaryStateUpdater: (A, RollbackAccessor[B]) => Maybe[doer.Venture[A | B]], isSpeculative: Boolean): doer.LatchingVenture[A | B] = {
		val previousStepCommitment = lastEnqueuedCommitment
		val thisStepCommitment = doer.Commitment[A]()
		lastEnqueuedCommitment = thisStepCommitment

		previousStepCommitment.subscribe {
			case success@Success(previousState) =>
				val rba: RollbackAccessor[B] =
					if isSpeculative then (isWithinDoSerEx: Boolean, onCompleted: (Try[A | B], RollbackApplication) => Unit) => thisStepCommitment.fulfill(previousState, isWithinDoSerEx, onCompleted)
					else null.asInstanceOf[RollbackAccessor[B]]
				primaryStateUpdater(previousState, rba)
					.fold {
						lastCommittedCommitment = thisStepCommitment
						thisStepCommitment.completeUnsafe(success)
					} {
						_.engage { thisStepResult =>
							lastCommittedCommitment = thisStepCommitment
							thisStepCommitment.completeUnsafe(thisStepResult)
						}
					}
			case Failure(e) =>
				thisStepCommitment.break(e, true)
		}
		thisStepCommitment
	}

	override def toString: String = {
		s"CausalStuckableFence(lastEnqueuedCovenant=${lastEnqueuedCommitment.toString}, lastCommittedCovenant=${lastCommittedCommitment.toString})"
	}
}