package readren.sequencer

import readren.common.Maybe

/**
 * A coordination primitive that manages the convergence of multiple concurrent executions into a single, stable, terminal result.
 *
 * This class implements a **Monotonic Convergence** pattern. It maintains a single stable [[doer.Covenant]] for each ongoing competition.
 * An execution is started by calling [[contend]].
 * A new competition is created when [[contend]] is called and no competition exists.
 * The competition has an incumbent execution.
 * When the incumbent execution completes, the competition is ended and the stable [[doer.Covenant]] is fulfilled.
 *
 * The provided `arbitrator` function acts as both a participant and an arbitrator, deciding whether the existing `incumbent` remains the leader of the competition or is superseded by its own execution.
 * Convergence is reached only when the incumbency completes its execution without being unseated.
 *
 * @tparam R The type of the result produced by the participating executions.
 * @tparam D The singleton type of the [[Doer]] that runs the participating executions.
 * @param doer The [[Doer]] that runs the participating executions.
 */
final class ResultIncrementalCoalescing[R, D <: Doer](val doer: D) {
	/** The stable [[doer.Covenant]] returned by all the calls to [[contend]] that participate in the ongoing [[Competition]]. */
	private var maybeFinalResult: Maybe[doer.Covenant[R]] = Maybe.empty
	/** The [[doer.LatchingDuty]] that yields the result of the execution currently authorized to fulfill the [[finalResult]] of the ongoing [[Competition]]. */
	private var incumbent: doer.LatchingDuty[R] | Null = null

	/**
	 * Enters a new execution into the ongoing competition.
	 * A new competition is started if none is ongoing, in which case the `arbitrator` function receives an empty incumbent.
	 *
	 * This method is the entry point for a "contender" It uses the `arbitrator` function to determine if this contender should displace the current [[incumbent]].
	 *
	 * @param arbitrator A function that receives the current [[incumbent]] (if any) and returns a [[doer.LatchingDuty]] that yields the result of the execution that should hold the title.
	 * If it returns the provided incumbent, the new contender "loses."
	 * If it returns another [[doer.LatchingDuty]] instance, the execution that fulfills it becomes the new incumbent and "wins" the right to fulfill the stable [[doer.Covenant]] of the competition result.
	 * CAUTION: If the [[doer.LatchingDuty]] returned by this function depends on a recursive call to [[contend]], then the `arbitrator` function passed to it must not return the incumbent or a deadlock occurs.
	 * @param isWithinDoSerEx A flag indicating if the call is already executing within the [[doer]]'s sequential context.
	 * @return A [[doer.LatchingDuty]] that will eventually yield the result of whichever execution completes while being the competition's incumbent.
	 * @note The `arbitrator` function is intentionally a parameter of this method rather than of the constructor.
	 * Placing it in the constructor would make the competition's arbitration invariance structurally explicit — a single policy governing all contenders for the lifetime of the instance.
	 * However, in practice, arbitration logic typically depends on both instance-level state and contextual parameters available at the call site, making a closure the most natural and readable expression of the policy.
	 * Placing `arbitrator` in the constructor would require artificially packaging that context into a state type `S` and threading it through, adding indirection without semantic gain.
	 * The per-call design also keeps the arbitration logic co-located with the contention site, where all relevant context is in scope and immediately visible to the reader.
	 */
	def contend(arbitrator: Maybe[doer.LatchingDuty[R]] => doer.LatchingDuty[R], isWithinDoSerEx: Boolean = doer.isInSequence): doer.LatchingDuty[R] = {
		if isWithinDoSerEx then {

			def supersedeWith(chosenWinner: doer.LatchingDuty[R], finalResult: doer.Covenant[R]): Unit = {
				incumbent = chosenWinner
				chosenWinner.subscribe { result =>
					if chosenWinner eq incumbent then {
						incumbent = null
						maybeFinalResult = Maybe.empty
						finalResult.fulfillUnsafe(result)
					}
				}
			}

			val chosenWinner = arbitrator(Maybe(incumbent))
			maybeFinalResult.fold {
				val finalResult = new doer.Covenant[R]
				maybeFinalResult = Maybe(finalResult)
				supersedeWith(chosenWinner, finalResult)
				finalResult
			} { finalResult =>
				if chosenWinner ne incumbent then supersedeWith(chosenWinner, finalResult)
				finalResult
			}
		} else {
			val joiningCovenant = doer.Covenant[R]()
			doer.run(contend(arbitrator, true).subscribe(r => joiningCovenant.fulfillUnsafe(r)))
			joiningCovenant
		}
	}

	/** A curried version of [[contend]]. */
	inline def contend(isWithinDoSerEx: Boolean)(arbitrator: Maybe[doer.LatchingDuty[R]] => doer.LatchingDuty[R]): doer.LatchingDuty[R] =
		contend(arbitrator, isWithinDoSerEx)
}
