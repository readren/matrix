package readren.sequencer
package providers

/** Tests if the [[ResultIncrementalCoalescing]] works correctly using the [[Doer]] provided by [[CooperativeFlatPollingSchedulerDp]].
 */
class CooperativeFlatPollingSchedulerDpResultIncrementalCoalescingTest extends ResultIncrementalCoalescingTest[CooperativeFlatPollingSchedulerDp.SchedulingDoerFacade] { thisSuite =>

	override type DP = CooperativeFlatPollingSchedulerDp

	override protected def buildDoerProvider: DP = new CooperativeFlatPollingSchedulerDp(applyMemoryFence = false) {
		override type Tag = String

		override def tagFromText(text: String): String = text

		override protected def onUnhandledException(doer: Doer, exception: Throwable): Unit = {
			if doer.isInSequence then {
				scribe.error(s"Unhandled exception:", exception)
			}
		}
	}

	override protected def releaseDoerProvider(doerProvider: DP): Unit = {
		doerProvider.shutdown()
	}
}
