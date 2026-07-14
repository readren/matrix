package readren.sequencer
package providers

import readren.sequencer.ResultIncrementalCoalescingTest

/** Tests if the [[ResultIncrementalCoalescing]] works correctly using the [[Doer]] provided by [[CooperativeFlatPollingSchedulerDp]].
 */
class CooperativeFlatPollingSchedulerDpResultIncrementalCoalescingTest extends ResultIncrementalCoalescingTest[CooperativeFlatPollingSchedulerDp.SchedulingDoerFacade] { thisSuite =>

	override type DP = CooperativeFlatPollingSchedulerDp

	/** The implementation should build an instance of the [[DoerProvider]] implementation under test. */
	override protected def buildDoerProvider: DP = new CooperativeFlatPollingSchedulerDp(applyMemoryFence = false) {
		override type Tag = String

		override def tagFromText(text: String): String = text

		override protected def onUnhandledException(doer: Doer, exception: Throwable): Unit = {
			if doer.isInSequence then {
				scribe.error(s"Unhandled exception:", exception)
			}
		}
	}

	/** The implementation should release the specified [[DoerProvider]]. */
	override protected def releaseDoerProvider(doerProvider: DP): Unit = {
		doerProvider.shutdown()
	}
}
