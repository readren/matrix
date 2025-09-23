package readren.sequencer
package providers

import providers.CooperativeWorkersDp.DoerFacade

trait CooperativeWorkersDpWithSchedulerCompanion {
	/** Facade of the concrete type of the [[Doer]] instances provided by [[CooperativeWorkersWithAsyncSchedulerDp]].
	 * Note that this trait is extending [[DoerFacade]] which is an abstract class. See [[DoerFacade]] to see why. */
	trait SchedulingDoerFacade extends DoerFacade, SchedulingExtension, LoopingExtension {
		override type Schedule <: ScheduleFacade
	}

	/** IMPORTANT: Represents a unique entity where equality and hash code must be based on identity. */
	trait ScheduleFacade {
		def initialDelay: MilliDuration

		def interval: MilliDuration

		def isFixedRate: Boolean

		/** Exposes the time at which the routine is expected to be run.
		 * Note that this value is updated by the scheduling thread and its last update may not be visible from other threads. Use for diagnostics only. */
		def scheduledTime: MilliTime

		/** A flag that tells if this instance was ever passed to the [[SchedulingDoerFacade.scheduleSequentially]] method.
		 * This flag is never cleared. */
		def wasActivated: Boolean

		/** An instance becomes canceled when either, it is passed to [[SchedulingDoerFacade.cancel]] or [[SchedulingDoerFacade.cancelAll]] is called after it [[wasActivated]]. */
		def isCanceled: Boolean
	}
}
