package readren.sequencer

import scala.collection.mutable
import readren.sequencer.Doer

/**
 * Coalesces concurrent requests by sharing the result of the in-flight inquire triggered for a specific parameter.
 *
 * This implements a '''First-In-Flight-Wins''' strategy:
 *  - If an equivalent request is already being processed, new callers subscribe to the existing [[LatchingTask]] handle.
 *  - Once that initial execution completes, the handle is removed, and the result is delivered to all concurrent subscribers.
 *
 * This is intended for stateless or point-in-time inquiries where any result retrieved after the request is enqueued is considered sufficient for all concurrent callers in that coalesced group.
 */
final class CoalescedInquire[P, R, D <: Doer](val doer: D, inquirer: P => doer.LatchingDuty[R]) {
	private val inFlight: mutable.Map[P, doer.LatchingDuty[R]] = mutable.Map.empty

	def getOrStart(params: P, isWithinDoer: Boolean = doer.isInSequence): doer.LatchingDuty[R] = {
		if isWithinDoer then inFlight.getOrElse(params, inquirer(params).andThen(_ => inFlight.remove(params)))
		else {
			val covenant = doer.Covenant[R]()
			doer.run {
				covenant.fulfillWith(getOrStart(params, true))
			}
			covenant
		}
	}
}
