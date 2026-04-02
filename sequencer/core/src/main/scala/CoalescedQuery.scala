package readren.sequencer

import readren.common.Maybe

import scala.collection.mutable
import scala.util.Failure
import scala.util.Try
import scala.util.control.NonFatal

/**
 * Coalesces concurrent requests by sharing the result of the in-flight query triggered for a specific parameter.
 *
 * This implements a '''First-In-Flight-Wins''' strategy:
 *  - If an equivalent request is already being processed, new callers subscribe to the existing [[LatchingTask]] handle.
 *  - Once that initial execution completes, the handle is removed, and the result is delivered to all concurrent subscribers.
 *
 * This is intended for stateless or point-in-time inquiries where any result retrieved after the request is enqueued is considered sufficient for all concurrent callers in that coalesced group.
 */
final class CoalescedQuery[P, R, D <: Doer](val doer: D, querier: P => doer.LatchingTask[R]) {
	private val inFlight: mutable.Map[P, doer.LatchingTask[R]] = mutable.Map.empty

	def getOrStart(params: P, isWithinDoer: Boolean = doer.isInSequence): doer.LatchingTask[R] = {
		if isWithinDoer then {
			inFlight.get(params) match {
				case Some(lt) =>
					lt
				case None =>
					try {
						val lt = querier(params)
						inFlight.put(params, lt)
						lt.andThen(_ => inFlight.remove(params))
						lt
					} catch {
						case NonFatal(e) => doer.LatchingTask_ready(Failure(e))
					}
			}
		} else {
			val commitment = doer.Commitment[R]()
			doer.run {
				commitment.completeWith(getOrStart(params, true))
			}
			commitment
		}
	}
}