package readren.sequencer

import readren.common.Maybe

/**
 * A coordination primitive that manages the convergence of multiple concurrent executions into a single, stable, terminal result.
 *
 * This class implements a **Monotonic Convergence** pattern. It maintains a single stable [[doer.Covenant]] for each ongoing competition.
 * An execution is started by calling [[contend]].
 * There is at most one competition per `parameter` value.
 * A new competition is created when [[contend]] is called and no competition exists for the provided parameter.
 * Every competition has an incumbent execution.
 * When the incumbent execution completes, the competition is ended and the stable [[doer.Covenant]] is fulfilled.
 *
 * The provided `arbitrator` function acts as both a participant and an arbitrator, deciding whether the existing `incumbent` remains the leader of the competition or is superseded by its own execution.
 * Convergence is reached only when the incumbency completes its execution without being unseated.
 *
 * @see [[ResultIncrementalCoalescing]] for a simpler implementation without groping.
 *
 * @tparam P The type of the parameter by which competitions are grouped.
 * @tparam R The type of the result produced by the participating executions.
 * @tparam D The singleton type of the [[Doer]] that runs the participating executions.
 * @param doer The [[Doer]] that runs the participating executions.
 */
final class ResultIncrementalCoalescingGrouped[P, R, D <: Doer](val doer: D) {

	/**
	 * Represents the internal state of an ongoing convergence process.
	 *
	 * @param finalResult The stable [[doer.Covenant]] returned by all the calls to [[contend]] that participate in this [[Competition]].
	 * @param incumbent   The [[doer.LatchingDuty]] that yields the result of the execution currently authorized to fulfill the [[finalResult]] of this [[Competition]].
	 */
	private final class Competition(
		val finalResult: doer.Covenant[R],
		var incumbent: doer.LatchingDuty[R] | Null
	)

	private val activeCompetitions: java.util.HashMap[P, Competition] = new java.util.HashMap()

	private val createCompetition: java.util.function.Function[P, Competition] =
		_ => new Competition(doer.Covenant[R](), null)

	/**
	 * Enters a new execution into the ongoing competition for a specific parameter.
	 * A new competition is started if none is ongoing for the given parameter, in which case the `arbitrator` function receives an empty incumbent.
	 *
	 * This method is the entry point for a "contender." It uses the `arbitrator` function to determine if this new entry should displace the current [[incumbent]].
	 *
	 * @param parameter      The key used to group competing executions.
	 * @param arbitrator        A function that receives the current [[incumbent]] (if any) and returns a [[doer.LatchingDuty]] that yields the result of the execution that should hold the title.
	 * If it returns the provided incumbent, the new contender "loses."
	 * If it returns another [[doer.LatchingDuty]] instance, the execution that fulfills it becomes the new incumbent and "wins" the right to fulfill the stable [[doer.Covenant]] of the competition result.
	 * @param isWithinDoSerEx   A flag indicating if the call is already executing within the [[doer]]'s sequential context.
	 * @return A [[doer.LatchingDuty]] that will eventually yield the result of whichever execution completes while being the competition's incumbent.
	 */
	def contend(
		parameter: P,
		arbitrator: (parameter: P, incumbent: Maybe[doer.LatchingDuty[R]]) => doer.LatchingDuty[R],
		isWithinDoSerEx: Boolean = doer.isInSequence
	): doer.LatchingDuty[R] = {

		if isWithinDoSerEx then {
			// Access or create the state for this specific parameter
			val competition = activeCompetitions.computeIfAbsent(parameter, createCompetition)
			val maybeIncumbent = Maybe(competition.incumbent)

			// The arbitrator function determines the winner of this contention
			val chosenWinner = arbitrator(parameter, maybeIncumbent)

			// Check for a change in incumbency (Monotonic transition)
			if maybeIncumbent.fold(true)(_ ne chosenWinner) then {
				// Unseat the previous incumbent
				competition.incumbent = chosenWinner

				// Subscribe to the chosen winner's completion
				chosenWinner.subscribe { result =>
					// The Incumbency Guard: A winner only fulfills the final result if it has not been displaced by a newer contender's arbitrator logic in the meantime.
					if chosenWinner eq competition.incumbent then {
						competition.finalResult.fulfillUnsafe(result)
						// Cleanup: The convergence for this parameter is complete
						activeCompetitions.remove(parameter)
					}
				}
			}
			competition.finalResult
		} else {
			// If called from outside the doer, marshal the request into the sequence
			val joiningCovenant = doer.Covenant[R]()
			doer.run(joiningCovenant.fulfillWith(contend(parameter, arbitrator, true)))
			joiningCovenant
		}
	}
}