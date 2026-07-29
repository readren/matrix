package readren.sequencer
package providers

/** Tests if the [[Doer]] with [[FluxExtension]] instances provided by [[CooperativeLocalPollingSchedulerDp]] satisfy the [[FluxExtension]] invariants.
 */
class CooperativeLocalPollingSchedulerDpFluxTest extends FluxDoerProviderTest[CooperativeLocalPollingSchedulerDp.SchedulingDoerFacade] { thisSuite =>

	override type DP = CooperativeLocalPollingSchedulerDp

	override protected def buildDoerProvider: DP = new CooperativeLocalPollingSchedulerDp(applyMemoryFence = false, threadFactory = new TestThreadFactory) {
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
