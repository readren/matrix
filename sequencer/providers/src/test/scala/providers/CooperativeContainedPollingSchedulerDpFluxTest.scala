package readren.sequencer
package providers

import readren.sequencer.FluxDoerProviderTest

/** Tests if the [[Doer]] with [[FluxExtension]] instances provided by [[CooperativeContainedPollingSchedulerDp]] satisfy the [[FluxExtension]] invariants.
 */
class CooperativeContainedPollingSchedulerDpFluxTest extends FluxDoerProviderTest[CooperativeContainedPollingSchedulerDp.SchedulingDoerFacade] { thisSuite =>

	override type DP = CooperativeContainedPollingSchedulerDp

	override protected def buildDoerProvider: DP = new CooperativeContainedPollingSchedulerDp(applyMemoryFence = false, threadFactory = new TestThreadFactory) {
		override type Tag = String

		override def tagFromText(text: String): Tag = text

		override protected def onUnhandledException(doer: Doer, exception: Throwable): Unit = thisSuite.onUnhandledException(doer, exception)
	}

	override def scalaCheckTestParameters: org.scalacheck.Test.Parameters =
		super.scalaCheckTestParameters.withMinSuccessfulTests(100)

	override protected def releaseDoerProvider(doerProvider: DP): Unit = {
		scribe.debug(s"Provider diagnostics:", doerProvider.diagnose(new StringBuilder()).toString())
		doerProvider.shutdown()
	}
}
