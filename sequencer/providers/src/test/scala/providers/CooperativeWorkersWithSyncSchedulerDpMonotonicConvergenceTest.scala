package readren.sequencer
package providers

import readren.sequencer.MonotonicConvergenceTest

/** Tests if the [[MonotonicConvergence]] works correctly using the [[Doer]] provided by [[CooperativeWorkersWithSyncSchedulerDp]].
 */
class CooperativeWorkersWithSyncSchedulerDpMonotonicConvergenceTest extends MonotonicConvergenceTest[CooperativeWorkersWithSyncSchedulerDp.SchedulingDoerFacade] { thisSuite =>

	override type DP = CooperativeWorkersWithSyncSchedulerDp

	/** The implementation should build an instance of the [[DoerProvider]] implementation under test. */
	override protected def buildDoerProvider: DP = new CooperativeWorkersWithSyncSchedulerDp(applyMemoryFence = false) {
		override type Tag = String

		override def tagFromText(text: String): String = text

		override protected def onUnhandledException(doer: Doer, exception: Throwable): Unit = {
			if doer.isInSequence then {
				scribe.error(s"Unhandled exception:", exception)
			}
		}

		override protected def onFailureReported(doer: Doer, failure: Throwable): Unit = {
			if doer.isInSequence then {
				scribe.debug(s"Failure reported: ${failure.getMessage}")
			}
		}
	}

	/** The implementation should release the specified [[DoerProvider]]. */
	override protected def releaseDoerProvider(doerProvider: DP): Unit = {
		doerProvider.shutdown()
	}
}
