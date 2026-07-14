package readren.sequencer
package providers

import readren.sequencer.SchedulingDoerProviderTest

/** Tests if the [[Doer]] with [[SchedulingExtension]] instances provided by [[CooperativeShardedPollingSchedulerDp]] satisfy the [[Doer]] and [[SchedulingExtension]] invariants.
 */
class CooperativeShardedPollingSchedulerDpTest extends SchedulingDoerProviderTest[CooperativeShardedPollingSchedulerDp.SchedulingDoerFacade] { thisSuite =>

	override type DP = CooperativeShardedPollingSchedulerDp

	/** The implementation should build an instance of the [[DoerProvider]] implementation under test. */
	override protected def buildDoerProvider: DP = new CooperativeShardedPollingSchedulerDp(applyMemoryFence = false, threadFactory = new TestThreadFactory) {
		override type Tag = String

		override def tagFromText(text: String): Tag = text

		override protected def onUnhandledException(doer: Doer, exception: Throwable): Unit = thisSuite.onUnhandledException(doer, exception)
	}

	override def scalaCheckTestParameters: org.scalacheck.Test.Parameters =
		super.scalaCheckTestParameters.withMinSuccessfulTests(100)

	/** The implementation should release the specified [[DoerProvider]].
	 * The implementation may assume that the provided instance was created calling [[buildDoerProvider]]. */
	override protected def releaseDoerProvider(doerProvider: DP): Unit = {
		scribe.debug(s"Provider diagnostics:", doerProvider.diagnose(new StringBuilder()).toString())
		doerProvider.shutdown()
	}
}
