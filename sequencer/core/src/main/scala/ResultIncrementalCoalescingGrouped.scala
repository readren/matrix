package readren.sequencer

import readren.common.Maybe

/**
 * A coordination primitive that manages the convergence of multiple concurrent executions into a single, stable, terminal result.
 *
 * This class implements a **Monotonic Convergence** pattern. It maintains a single stable [[doer.Captor]] for each ongoing competition.
 * An execution is started by calling [[contend]].
 * There is at most one competition per `parameter` value.
 * A new competition is created when [[contend]] is called and no competition exists for the provided parameter.
 * Every competition has an incumbent execution.
 * When the incumbent execution completes, the competition is ended and the stable [[doer.Captor]] is fulfilled.
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
	 * The stable [[doer.Capturer]] returned by all the calls to [[contend]] that participate in this [[Competition]].
	 * Manages the internal state of an ongoing convergence process.
	 *
	 * The [[doer.Capturer]] that yields the result of the execution currently authorized to fulfill the [[finalResult]] of this [[Competition]].
	 */
	private final class Competition extends doer.Captor[R] {
		/** The [[doer.Capturer]] that yields the result of the execution currently authorized to fulfill the [[finalResult]] of this [[Competition]]. */
		var incumbent: doer.Capturer[R] | Null = null
		/** The [[Subscription]] to the [[incumbent]]. */
		var maybeIncumbentSubscription: Maybe[doer.Subscription] = Maybe.empty
	}

	private val activeCompetitions: java.util.HashMap[P, Competition] = new java.util.HashMap()

	private val createCompetition: java.util.function.Function[P, Competition] = _ => new Competition()

	/**
	 * Enters a new execution into the ongoing competition for a specific parameter.
	 * A new competition is started if none is ongoing for the given parameter, in which case the `arbitrator` function receives an empty incumbent.
	 *
	 * This method is the entry point for a "contender." It uses the `arbitrator` function to determine if this new entry should displace the current [[incumbent]].
	 *
	 * @param parameter      The key used to group competing executions.
	 * @param arbitrator        A function that receives the current [[incumbent]] (if any) and returns a [[doer.Capturer]] that yields the result of the execution that should hold the title.
	 * If it returns the provided incumbent, the new contender "loses."
	 * If it returns another [[doer.Capturer]] instance, the execution that fulfills it becomes the new incumbent and "wins" the right to fulfill the stable [[doer.Captor]] of the competition result.
	 * @param isWithinDoSerEx   A flag indicating if the call is already executing within the [[doer]]'s sequential context.
	 * @return A [[doer.Capturer]] that will eventually yield the result of whichever execution completes while being the competition's incumbent.
	 */
	def contend(
		parameter: P,
		arbitrator: (parameter: P, incumbent: Maybe[doer.Capturer[R]]) => doer.Capturer[R],
		isWithinDoSerEx: Boolean = doer.isInSequence
	): doer.Capturer[R] = {

		if isWithinDoSerEx then {
			// Access or create the state for this specific parameter
			val competition = activeCompetitions.computeIfAbsent(parameter, createCompetition)
			val maybeIncumbent = Maybe(competition.incumbent)

			// The arbitrator function determines the winner of this contention
			val chosenWinner = arbitrator(parameter, maybeIncumbent)


			val isNewIncumbent = maybeIncumbent.fold(true) { currentIncumbent =>
				if chosenWinner eq currentIncumbent then false
				else {
					// Unsubscribe the unseated contender.
					val mis = competition.maybeIncumbentSubscription
					competition.maybeIncumbentSubscription = Maybe.empty
					mis.foreach(_.unsubscribeSync())
					true
				}
			}

			// If the competition is brand new or its current incumbent must be unseated
			if isNewIncumbent then {
				// Set the chosen winner as the incumbent
				competition.incumbent = chosenWinner

				// Subscribe to the chosen winner's completion
				val subscription = chosenWinner.subscribeSync(new doer.MonoObserver[R] { // TODO optimize
					override def onSuccess(result: R): Unit = {
						// The Incumbency Guard: A winner only fulfills the final result if it has not been displaced by a newer contender's arbitrator logic in the meantime.
						if chosenWinner eq competition.incumbent then {
							// Cleanup: The convergence for this parameter is complete
							competition.incumbent = null
							competition.maybeIncumbentSubscription = Maybe.empty
							activeCompetitions.remove(parameter)

							competition.captureSync(result)
						}
					}

					override def onError(e: Throwable): Unit = {
						// The Incumbency Guard: A winner only fulfills the final result if it has not been displaced by a newer contender's arbitrator logic in the meantime.
						if chosenWinner eq competition.incumbent then {
							// Cleanup: The convergence for this parameter is complete
							competition.incumbent = null
							competition.maybeIncumbentSubscription = Maybe.empty
							activeCompetitions.remove(parameter)

							competition.trapSync(e)
						}
					}
				})
				if competition.incumbent eq chosenWinner then competition.maybeIncumbentSubscription = Maybe(subscription)

			}
			competition
		} else {
			// If called from outside the doer, marshal the request into the sequence
			new doer.Captor[R] with doer.MonoObserver[R] with Runnable {
				doer.run(this)

				override def run(): Unit = contend(parameter, arbitrator, true).triggerSync(this)

				override def onSuccess(a: R): Unit = captureSync(a)

				override def onError(e: Throwable): Unit = trapSync(e)

			}
		}
	}
}