package readren.sequencer

import CausalFence.{ARRIVED_AFTER, ARRIVED_BEFORE, ROLLBACK_APPLIED, ROLLBACK_IGNORED, CausalAnchorArrival, RollbackApplication}
import Doer.{ANOTHER_AFTER, ANOTHER_BEFORE, THE_PROVIDED}

import readren.common.{Maybe, Trial}

import scala.util.Failure
import scala.util.control.NonFatal


object CausalFence {
	/** Information about the application of a rollback.
	 *		- [[ROLLBACK_APPLIED]] if the rollback was applied.
	 *		- [[ROLLBACK_IGNORED]] if the rollback was ignored because it was attempted to late. */
	type RollbackApplication = OriginId
	final inline val ROLLBACK_APPLIED = THE_PROVIDED
	final inline val ROLLBACK_IGNORED = ANOTHER_BEFORE

	/** Informs about the timing of the arrival to an anchored link of a causal chain:
	 *		- [[ARRIVED_BEFORE]] the transition corresponding to the link completed before the anchoring.
	 *		- [[ARRIVED_AFTER]] the transition corresponding to the link completed after the anchoring.
	 * */
	type CausalAnchorArrival = OriginId
	final inline val ARRIVED_BEFORE = ANOTHER_BEFORE
	final inline val ARRIVED_AFTER = THE_PROVIDED
}

/** A fence that serializes (enqueues for serial execution) non-failing state transitions with causal fulfillment semantics with stuckness.\
 * Its goal is to enforce causal ordering: each state transition attempt is causally anchored to the previous one, ensuring that each update observes the latest visible state, and that all updates are fulfilled in causal order. Speculative updates may be rolled back before becoming visible.\
 * Becomes stuck on a failure state when a state transition fails. Once stuck, progression halts: subsequent update attempts are skipped and the returned [[doer.LatchingTask]] yield the same halting state.\
 * Core idea: [[CausalFence]] is a causal sequencing primitive with stuckness semantics.
 * - It maintains a single tail [[doer.Covenant]] ([[lastEnqueuedCovenant]]) that represents the latest step in a causal chain. Each [[advanceIf]] and [[causalAnchor]] call enqueues a new [[doer.Covenant]], chained to the previous one, unless the fence is already stuck.\
 * - Each successful transition yields a committed, visible state. Rolled-back transitions fulfill with the previous state.
 * - The causal chain is never broken: all updates are fulfilled and causally ordered until a stuck state is reached.
 * - The committed lineage includes all transitions to non-stuck states, and ends in a stuck one.
 * - If a transition to a stuck state occurs, the stuck state is committed and the fence halts — subsequent update attempts are skipped and yield the same stuck state.
 *
 * Fundamental Invariants:
 * - Single updater invariant: At most one `primaryStateUpdater` (the function passed to [[advance]]) is in progress at a time. Subsequent advances wait until the previous one completes.
 * - Advance ordering invariant: The `primaryStateUpdater` passed to [[advance]] runs after all consumers that subscribed to the previous [[Covenant]] in the queue (via earlier [[advance]] or [[causalAnchor]]) have completed their synchronous part.
 * - Anchor freshness invariant: A consumer subscribed to [[causalAnchor]] observes the same state that a `primaryStateUpdater` would receive if [[advance]] were invoked at that moment. Ordering is guaranteed relative to the last completed advance at subscription time, but not relative to advances invoked later. If the fence is stuck, the anchor yields the stuck state deterministically.
 * - Stuckness invariant: Once a stuck state is committed, no further transitions are accepted. All subsequent advances or anchors yield the same stuck state.
 * - Rollback integrity invariant: For speculative advances, derived updates must not be composed into the primary updater, otherwise rollback semantics are broken.
 *
 * Invariants related to derived state:
 * - Game-changing invariant: Immediately after an [[advance]] or [[causalAnchor]] call (if not stuck), there are no pending advances other than the one just created. The returned [[Covenant]] (seen as [[LatchingTask]]) is a fresh tail. Any immediate synchronous subscription to it is guaranteed to be the first subscriber in its list. Therefore, when the [[Covenant]] fulfills, that consumer sees the up‑to‑date state deterministically. This invariant eliminates the race where another [[advance]] could sneak in and publish a newer state before or while the synchronous consumer runs. The consumer is deterministically ordered before any updater that follows in the causal chain.
 * - Anchor specificity invariant: Derived updates that require strict ordering relative to a particular primary transition must anchor to that transition’s [[Covenant]] (the one returned by [[advance]]), not to a generic tail snapshot (as the returned by [[causalAnchor]]).
 * - Deterministic derived ordering invariant: When derived updates have dependencies, their execution order must be enforced by anchoring the dependent update and deriving prerequisites synchronously from the anchored state, or by composing into the primary updater if not speculative.
 * - Rollback integrity invariant: No derived side effect may commit externally during a speculative advance before the primary step is safely committed; otherwise rollback can leave the system in an inconsistent state.
 * - Idempotence/compensation invariant: Any derived side effect that can be re-run or rolled back must be idempotent or have a compensating action to preserve causal correctness under retries or rollback.
 *
 * Invariants inherited from [[doer.Covenant]]:
 * - Sequential consumer invariant: The [[doer.LatchingTask]] returned by [[advanceIf]] and [[causalAnchor]] is a [[doer.Commitment]] and therefore the subscribed consumers are invoked in registration order. The synchronous part of each consumer runs to completion before the next begins.\
 * @param initialState the initial state, already visible and committed. Can not be failure. */
class CausalFence[A, D <: Doer](val doer: D)(initialState: A) {
	private var lastCommittedCovenant: doer.Covenant[A] = new doer.Covenant(Trial.success(initialState))
	private var lastEnqueuedCovenant: doer.Covenant[A] = lastCommittedCovenant

	/** Provides asynchronous rollback capability for speculative updates.\
	 * Used within [[advanceSpeculatively]] to discard an in-flight update before it becomes visible.\
	 * Rollback is only effective if invoked before the update is committed.\
	 * Rollback is always non-failing and fulfills the update with the previous state.\
	 * If rollback is invoked too late, the accessor still complete the [[doer.LatchingTask]] with the committed state and signals that rollback was ineffective.\
	 * The returned [[doer.LatchingTask]] completes with the same state that becomes visible. */
	trait RollbackAccessor[B <: A] {
		/** Attempts to roll back the speculative update, restoring the previous visible state.\
		 * The rollback is applied if "invoked" before the update it targets has completed.\
		 * The quotes around "invoked" reflect that, when `isWithinDoSerEx` is false, the rollback attempt is scheduled asynchronously and may race with the update's completion.\
		 * Regardless of timing, the [[doer.LatchingTask]] originally returned by [[advanceSpeculatively]] is fulfilled:
		 * - With the previous state if rollback succeeds.
		 * - With the committed state if rollback was too late.\
		 * The callback receives the resulting state and a flag indicating whether rollback was applied (`false`) or too late (`true`).\
		 * @param isWithinDoSerEx whether the caller is executing within the Doer's sequential executor.
		 * @param completionObserver optional observer of the actual primary state when the transition is completed or rolled back. This observer is notified within this [[Doer]]’s sequential executor before any consumer subscribed to the returned [[LatchingTask]]. The first parameter is the primary state; the second indicates whether the rollback was applied or ignored: [[ROLLBACK_APPLIED]] if so, or [[ROLLBACK_IGNORED]] if not.
		 * @return the [[LatchingTask]] originally returned by the [[advanceSpeculatively]]-like method that created this [[RollbackAccessor]], now fulfilled with either the committed or rolled-back state. */
		def rollback(isWithinDoSerEx: Boolean = doer.isInSequence, completionObserver: CompletionObserver[A | B] = CompletionIgnorer): doer.LatchingTask[A | B]
	}

	/** @return true if the queue of primary state updaters is empty. */
	def isEmpty: Boolean = lastEnqueuedCovenant eq lastCommittedCovenant

	/** The state to which the most recent step transitioned into.\
	 * Rolled-back transitions yield the previous state.\
	 * Must be called within this [[Doer]].\
	 * @return the last transition result, which is always defined. */
	def committedState: Trial[A] = {
		doer.checkWithin()
		lastCommittedCovenant.maybeResult
	}

	/** Returns a [[doer.LatchingTask]] that yields the currently visible state.\
	 * This reflects the state to which the most recent step transitioned into.\
	 * Rolled-back transitions yield the previous state.\
	 * The returned [[doer.LatchingTask]] is always already completed.\
	 * Must be called within this [[Doer]].\
	 * @return a [[doer.LatchingTask]] yielding the currently visible state. */
	def committed: doer.LatchingTask[A] = {
		doer.checkWithin()
		lastCommittedCovenant
	}


	/** Returns a [[LatchingTask]] that yields the same state an updater would see if [[advance]] were invoked at this moment.\
	 * This provides a causal checkpoint suitable for synchronous consumers that need to derive state deterministically.\
	 * The returned [[LatchingTask]] is backed by a fresh [[Commitment]] that forwards from the current tail [[Commitment]].\
	 * This ensures that immediate synchronous subscriptions to the returned [[LatchingTask]] are registered before completion, making them the first subscribers on the fresh [[Covenant]] and guaranteeing deterministic observation of the up‑to‑date state.\
	 * Temporal window of causal safety: The causal guarantee holds only during the synchronous execution of a consumer synchronously subscribed to the returned [[LatchingTask]]. Code that rely on causal visibility is safe only within the body of that consumer. Once the consumer has returned, deferred or later code is no longer causally anchored.\
	 * @param completionObserver optional observer of the actual primary state when the anchored link is successfully reached. This observer is notified within this [[Doer]]’s sequential executor before any consumer subscribed to the returned [[LatchingTask]]. The first parameter is the primary state; the second indicates whether the link was already reached when this method was invoked: [[ARRIVED_BEFORE]] if so, or [[ARRIVED_AFTER]] if not.
	 * @return a [[doer.LatchingTask]] yielding the state that the next update will be causally anchored to — i.e. the same state an updater would see if [[advance]] were called at this moment.
	 * @note When derived updates (those done to secondary state that derives from the primary state) have causal dependencies among themselves, you must enforce deterministic order by other means: use causal derivation functions (anchor only the dependent update and derive prerequisites synchronously from the anchored state), or, if derived updates are fast and the advance is not speculative, compose them into the `primaryStateUpdater` passed to [[advanceIf]] or [[advanceIf]]. Composition is not safe for speculative advances, because rollback during the derived update phase could succeed when it should not.\
	 * Independent subscriptions to [[causalAnchor]] are appropriate only for derived updates that are order‑independent. */
	def causalAnchor(completionObserver: CompletionObserver[A] = CompletionIgnorer): doer.LatchingTask[A] = { // TODO Consolidate the callbacks into a trait.
		doer.checkWithin()
		val lec = lastEnqueuedCovenant
		val lcc = lastCommittedCovenant
		if lec eq lcc then {
			lcc.maybeResult.fold(throw IllegalStateException())(e => completionObserver.onError(e, ARRIVED_BEFORE))(a => completionObserver.onSuccess(a, ARRIVED_BEFORE))
			lec
		} else {
			val thisStepCovenant = doer.Covenant[A]()
			lastEnqueuedCovenant = thisStepCovenant
			lec.subscribeSyncCallbacks(
				a =>
					lastCommittedCovenant = thisStepCovenant
					thisStepCovenant.fulfillSync(a, completionObserver),
				e =>
					lastCommittedCovenant = thisStepCovenant
					thisStepCovenant.breakSync(e, completionObserver)
			)
			thisStepCovenant
		}
	}

	/** Enqueues an asynchronous non-speculative primary-state updater.\
	 * If this [[CausalFence]] gets stuck (because a previous update failed) before the provided updater is executed, it is skipped and the returned [[doer.LatchingTask]] is completed with the same failure.\
	 * Rollback is not supported in this method. The updater function is defined with a second parameter of type `Null` to match the internal speculative signature, allowing reuse without introducing an extra closure.\
	 * Temporal window of causal safety:
	 * The causal guarantee holds from the moment the `primaryStateUpdater` function is invoked until the [[doer.LatchingTask]] returned by this method and all the consumers synchronously subscribed to it have returned.\
	 * Is worth mentioning that the provided updater will be executed after all the consumers previously and synchronously subscribed to the [[doer.LatchingTask]] returned by [[causalAnchor]] and [[advance]]-like methods have completed.\
	 * @param primaryStateUpdater a function that computes the next state from the current one
	 * @return a [[doer.LatchingTask]] that will be fulfilled with the new state once the update completes.
	 * @note CAUTION: The execution of consumers that are subscribed to obsolete instances of [[doer.LatchingTask]] is not causally ordered.\
	 * So, avoid memorizing [[doer.LatchingTask]] instances returned by [[causalAnchor]] or [[advance]]-like methods; always subscribe to the instance returned by [[causalAnchor]] to ensure causal ordering of the consumers executions.
	 * Obsolete are those instances returned by methods of this [[CausalFence]] before the last call to an [[advance]]-like method. */
	inline def advance[B <: A](inline primaryStateUpdater: A => doer.Observable[A | B], isGuarded: Boolean = false): doer.LatchingTask[A | B] =
		step((a, _) => Maybe(primaryStateUpdater(a)), false, isGuarded)


	/** Like [[advance]], but the update may be synchronously canceled by the provided updater returning [[Maybe.empty]].\
	 * If the [[primaryStateUpdater]] returns some state, it is committed.\
	 * If it returns [[Maybe.empty]], the update is canceled and the previous state is retained.\
	 * @param primaryStateUpdater a partial function that computes the next state from the current one; the second argument is always `null`
	 * @return a [[LatchingTask]] that yields the updated state */
	inline def advanceIf[B <: A](inline primaryStateUpdater: A => Maybe[doer.Observable[B]], isGuarded: Boolean = false): doer.LatchingTask[A | B] = {
		step((a, _) => primaryStateUpdater(a), false, isGuarded)
	}

	/** Enqueues an asynchronous speculative primary-state updater.\
	 * The provided [[RollbackAccessor]] allows the update to be withdrawn before it becomes visible.\
	 * If the previous step failed, this transition is skipped and the returned [[LatchingTask]] is completed with the same failure.\
	 * If rollback is invoked before visibility, the update is canceled and the previous state is kept.\
	 * Only successful transitions update the committed state.\
	 * Temporal window of causal safety:\
	 * The causal guarantee holds from the moment the `primaryStateUpdater` function is invoked until the [[LatchingTask]] returned by this method and all the consumers synchronously subscribed to it have returned.\
	 * Is worth mentioning that the provided updater will be executed after all the consumers previously and synchronously subscribed to the [[LatchingTask]] returned by [[causalAnchor]] and [[advance]]-like methods have completed.\
	 * @param primaryStateUpdater a function that computes the next state from the current one, with rollback control
	 * @return a [[LatchingTask]] that yields the updated or rolled-back state.
	 * @note CAUTION: The execution of consumers that are subscribed to obsolete instances of [[LatchingTask]] is not causally ordered.
	 * So, avoid memorizing [[LatchingTask]] instances returned by [[causalAnchor]] or [[advance]]-like methods; always subscribe to the instance returned by [[causalAnchor]] to ensure causal ordering of the consumers executions.
	 * Obsolete are those instances returned by methods of this [[CausalFence]] before the last call to an [[advance]]-like method. */
	inline def advanceSpeculatively[B <: A](inline primaryStateUpdater: (A, RollbackAccessor[B]) => doer.Observable[A | B], isGuarded: Boolean = false): doer.LatchingTask[A | B] =
		step[B]((a, rba) => Maybe(primaryStateUpdater(a, rba)), true, isGuarded)

	/** Like [[advanceSpeculatively]], but the update may be synchronously canceled by the provided updater returning [[Maybe.empty]]
	 * If the [[primaryStateUpdater]] returns some state, it is committed.
	 * If it returns [[Maybe.empty]], the update is canceled and the previous state is retained.
	 * If a previous step failed, this transition is skipped and the returned [[LatchingTask]] is completed with the same failure.\
	 * If rollback is invoked before visibility, the update is canceled and the previous state is kept.\
	 * Only successful transitions update the committed state.\
	 * @param primaryStateUpdater a function that computes the next state from the current one, with rollback capability
	 * @return a [[LatchingTask]] that will be completed with the new state if not rolled-back in time, the previous state if rolled-back in time, or the previous failure due to which the update was skipped. */
	inline def advanceSpeculativelyIf[B <: A](primaryStateUpdater: (A, RollbackAccessor[B]) => Maybe[doer.Observable[A | B]], isGuarded: Boolean = false): doer.LatchingTask[A | B] =
		step(primaryStateUpdater, true, isGuarded)

	/** Internal method that performs the actual state transition.\
	 * Handles both speculative and non-speculative updates depending on the `isSpeculative` flag.\
	 * If the previous step failed, the update is not executed and the [[doer.LatchingTask]] corresponding to this step is completed with the same failure. The rollback accessor is instantiated only when needed to avoid unnecessary allocations.\
	 * Only successful transitions update the committed state.\
	 * The rollback accessor is instantiated only when needed to avoid unnecessary allocations.\
	 * @param primaryStateUpdater the transition function, optionally accepting a [[RollbackAccessor]]
	 * @param isSpeculative whether the update is speculative and may be rolled back.
	 * @param isGuarded If true, any non-fatal exception thrown by the provided `primaryStateUpdater` function is caught sticking this fence synchronously. If false, exceptions throw by it are not handled.
	 * @return a [[doer.LatchingTask]] that will be completed with the new state if not rolled-back in time, the previous state if rolled-back in time, or the previous failure due to which the update was skipped. */
	private def step[B <: A](primaryStateUpdater: (A, RollbackAccessor[B]) => Maybe[doer.Observable[A | B]], isSpeculative: Boolean, isGuarded: Boolean): doer.LatchingTask[A | B] = {
		doer.checkWithin()
		val previousStepCovenant = lastEnqueuedCovenant
		val thisStepCovenant = doer.Covenant[A | B]()
		lastEnqueuedCovenant = thisStepCovenant

		previousStepCovenant.triggerSync(new doer.MonoObserver[A] with RollbackAccessor[B] {
			private var maybePreviousState: Maybe[A] = Maybe.empty

			override def rollback(isWithinDoSerEx: Boolean, completionObserver: CompletionObserver[A | B]): doer.LatchingTask[A | B] = {
				thisStepCovenant.fulfill(maybePreviousState.get, isWithinDoSerEx, completionObserver)
			}

			override def onSuccess(previousState: A): Unit = {
				maybePreviousState = Maybe(previousState)

				var maybeCaughtError: Maybe[Throwable] = Maybe.empty
				val maybeNextStateProviderMono =
					if isGuarded then try primaryStateUpdater(previousState, this) catch {
						case NonFatal(e) =>
							maybeCaughtError = Maybe(e)
							Maybe.empty
					} else primaryStateUpdater(previousState, this)

				maybeCaughtError.fold {
					maybeNextStateProviderMono.fold {
						lastCommittedCovenant = thisStepCovenant
						thisStepCovenant.fulfillSync(previousState)
					} { nextStateProviderTask =>
						nextStateProviderTask.triggerSync(new doer.MonoObserver[A | B] {
							override def onSuccess(newState: A | B): Unit = {
								lastCommittedCovenant = thisStepCovenant
								thisStepCovenant.fulfillSync(newState)
							}

							override def onError(failure: Throwable): Unit = {
								lastCommittedCovenant = thisStepCovenant
								thisStepCovenant.breakSync(failure)
							}
						})
					}
				} { caughtError =>
					lastCommittedCovenant = thisStepCovenant
					thisStepCovenant.breakSync(caughtError)
				}
			}

			override def onError(e: Throwable): Unit = {
				lastCommittedCovenant = thisStepCovenant
				thisStepCovenant.breakSync(e)
			}
		})
		thisStepCovenant
	}

	/** Enqueues a synchronous non-speculative primary-state updater.\
	 * The update is applied synchronously and always fulfills with a committed state:
	 * - The [[primaryStateUpdater]] is applied to the previous state.
	 * - The resulting state is committed immediately.
	 * @param primaryStateUpdater a total function that produces the next state from the current one.
	 * @return a [[LatchingTask]] that is always fulfilled with the committed state.
	 */
	inline def jump[B <: A](inline primaryStateUpdater: A => A | B, isGuarded: Boolean = false): doer.LatchingTask[A | B] =
		jumpIf[B](a => Maybe(primaryStateUpdater(a)), isGuarded)

	/** Like [[jump]], but the update may be canceled by the provided updater returning [[Maybe.empty]].\
	 * If the [[primaryStateUpdater]] returns some state, it is committed.
	 * If it returns [[Maybe.empty]], the update is canceled and the previous state is retained.
	 * @param primaryStateUpdater a partial function that produces a new state from the previous one
	 * @return a [[LatchingTask]] that is always fulfilled with the committed state
	 */
	def jumpIf[B <: A](primaryStateUpdater: A => Maybe[A | B], isGuarded: Boolean): doer.LatchingTask[A | B] = {
		doer.checkWithin()
		val previousStepCovenant = lastEnqueuedCovenant
		val thisStepCovenant = doer.Covenant[A | B]()
		lastEnqueuedCovenant = thisStepCovenant

		previousStepCovenant.triggerSync(new doer.MonoObserver[A] {
			override def onSuccess(previousState: A): Unit = {
				var maybeCaughtError: Maybe[Throwable] = Maybe.empty
				val maybeNextState = if isGuarded then try primaryStateUpdater(previousState) catch {
					case NonFatal(caughtError) =>
						maybeCaughtError = Maybe(caughtError)
						Maybe.empty
				} else primaryStateUpdater(previousState)
				maybeCaughtError.fold {
					maybeNextState.fold {
						lastCommittedCovenant = thisStepCovenant
						thisStepCovenant.fulfillSync(previousState)
					} { nextState =>
						lastCommittedCovenant = thisStepCovenant
						thisStepCovenant.fulfillSync(nextState)
					}
				} { caughtError =>
					lastCommittedCovenant = thisStepCovenant
					thisStepCovenant.breakSync(caughtError)
				}
			}

			override def onError(e: Throwable): Unit = {
				lastCommittedCovenant = thisStepCovenant
				thisStepCovenant.breakSync(e)
			}
		})
		thisStepCovenant
	}

	override def toString: String = {
		s"CausalFence(lastEnqueuedCovenant=${lastEnqueuedCovenant.toString}, lastCommittedCovenant=${lastCommittedCovenant.toString})"
	}
}