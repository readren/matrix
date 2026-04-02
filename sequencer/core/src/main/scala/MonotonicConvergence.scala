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
 * The provided `resolve` function acts as both a participant and an arbitrator, deciding whether the existing `incumbent` remains the leader of the competition or is superseded by its own execution.
 * Convergence is reached only when the incumbency completes its execution without being unseated.
 *
 * @tparam R The type of the result produced by the participating executions.
 * @tparam D The singleton type of the [[Doer]] that runs the participating executions.
 * @param doer The [[Doer]] that runs the participating executions.
 */
final class MonotonicConvergence[R, D <: Doer](val doer: D) {
	/** The stable [[doer.Covenant]] returned by all the calls to [[contend]] that participate in the ongoing [[Competition]]. */
	private var maybeFinalResult: Maybe[doer.Covenant[R]] = Maybe.empty
	/** The [[doer.LatchingDuty]] that yields the result of the execution currently authorized to fulfill the [[finalResult]] of the ongoing [[Competition]]. */
	private var incumbent: doer.LatchingDuty[R] | Null = null

	/**
	 * Enters a new execution into the ongoing competition.
	 * A new competition is started if none is ongoing, in which case the `resolve` function receives an empty incumbent.
	 *
	 * This method is the entry point for a "contender" It uses the `resolve` function to determine if this contender should displace the current [[incumbent]].
	 *
	 * @param resolve A function that receives the current [[incumbent]] (if any) and returns a [[doer.LatchingDuty]] that yields the result of the execution that should hold the title.
	 * If it returns the provided incumbent, the new contender "loses."
	 * If it returns another [[doer.LatchingDuty]] instance, that execution that fulfills it becomes the new incumbent and "wins" the right to fulfill the stable [[doer.Covenant]] of the competition result.
	 * CAUTION: If the [[doer.LatchingDuty]] returned by this function depends on a recursive call to [[contend]], then the `resolve` function passed to it must not return the incumbent or a deadlock occurs.
	 * @param isWithinDoer A flag indicating if the call is already executing within the [[doer]]'s sequential context.
	 * @return A [[doer.LatchingDuty]] that will eventually yield the result of whichever execution completes while being the competition's incumbent.
	 */
	def contend(resolve: Maybe[doer.LatchingDuty[R]] => doer.LatchingDuty[R], isWithinDoSerEx: Boolean = doer.isInSequence): doer.LatchingDuty[R] = {
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

			val chosenWinner = resolve(Maybe(incumbent))
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
			doer.run(contend(resolve, true).subscribe(r => joiningCovenant.fulfillUnsafe(r)))
			joiningCovenant
		}
	}
}
