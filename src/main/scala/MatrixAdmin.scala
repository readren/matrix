package readren.matrix

import readren.taskflow.{Doer, Maybe}

import scala.util.{Failure, Success, Try}

object MatrixAdmin {
	val SomeUnit: Maybe[Unit] = Maybe.some(())
	val SuccessUnit: Success[Unit] = Success(())
	val SuccessSomeUnit: Success[Maybe[Unit]] = Success(SomeUnit)
	val SomeSuccessUnit: Maybe[Success[Unit]] = Maybe.some(SuccessUnit)
}

class MatrixAdmin(val assistant: Doer.Assistant) extends Doer(assistant) { thisAdmin =>
	import MatrixAdmin.*

	def pickDoer(): Doer = ???

	/** Should be called within this [[MatrixAdmin]]. */
	def stimulate[M](reactant: Reactant[M], stimulator: InboxBackend[M]): Unit = {
		assert(reactant.isIdle && (reactant.admin eq thisAdmin) && (stimulator.admin eq thisAdmin))
		reactant.isIdle = false
		// pick the doer that will handle the messages
		val msgHandlingDoer = pickDoer()

		// build a task that handles all pending messages updating the behavior and then set the reactant's idle mark to true.
		val handlesAllPendingMessages: msgHandlingDoer.Task[Unit] = stimulator.withdraw()
			.onBehalfOf(msgHandlingDoer)
			.foreach { firstMessage =>
				// Note that this variable is initialized within the `msgHandlingDoer` but is accessed and mutated exclusively within `thisAdmin`.
				var nextBehavior = reactant.currentBehavior.handle(firstMessage.get)

				// Build a task that, if there is a pending message, withdraws it, handles it, updates the reactant's behavior, and returns `Maybe.empty`; else returns Maybe.some(()).
				val handlesMessageAndUpdatesBehavior: thisAdmin.Task[Maybe[Unit]] = thisAdmin.Task.mine { () =>
					// update the behavior
					reactant.setBehavior(nextBehavior)
					// withdraw the next pending message considering all the inboxes the reactant has.
					reactant.withdrawNextMessage()
				}.flatMap { (oNextMessage: Maybe[M]) =>
					oNextMessage.fold {
						// if no pending message to process, return Maybe.some(())
						thisAdmin.Task.immediate(SuccessSomeUnit)
					} { nextMessage =>
						// if a message was withdrawn, handle it, update the `nextBehavior` variable, and return Maybe.empty
						msgHandlingDoer.Task.mine { () => nextBehavior.handle(nextMessage) }
							.onBehalfOf(thisAdmin)
							.map { newBehavior =>
								nextBehavior = newBehavior
								Maybe.empty
							}
					}
				}
				// repeat the `handlesMessageAndUpdatesBehavior` task until pending messages are exhausted.
				handlesMessageAndUpdatesBehavior.repeatedUntilSome() { (count, noMorePendingMessages) =>
					// if pending messages are exhausted, mark the reactant as idle and exit the loop.
					if noMorePendingMessages.isDefined then {
						reactant.isIdle = true
						assert(nextBehavior eq reactant.currentBehavior)
						SomeSuccessUnit
					}
					// if a message was withdrawn, repeat the loop.
					else Maybe.empty
				}
			}
		// execute the task that handles all pending messages updating the behavior
		handlesAllPendingMessages.attemptAndForgetHandlingErrors()(reportFailure)
	}

}
