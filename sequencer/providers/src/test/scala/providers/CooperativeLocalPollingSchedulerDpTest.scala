package readren.sequencer
package providers

import providers.CooperativeLocalPollingSchedulerDp.*

class CooperativeLocalPollingSchedulerDpTest extends DoerProviderTestBase[SchedulingDoerFacade]
	with VanillaDoerTests[SchedulingDoerFacade]
	with MonoTests[SchedulingDoerFacade]
	with FluxDoerTests[SchedulingDoerFacade]
	with CausalFenceTests[SchedulingDoerFacade]
	with ResultIncrementalCoalescingTests[SchedulingDoerFacade]
	with LoopingDoerTests[SchedulingDoerFacade]
	with ScheduledMonoTests[SchedulingDoerFacade]
	with ScheduledFluxTests[SchedulingDoerFacade]
	with CooperativeWorkersChildTests[SchedulingDoerFacade] { thisSuite =>

	override type DP = CooperativeLocalPollingSchedulerDp

	override protected def buildDoerProvider(poolSize: Int): DP = new CooperativeLocalPollingSchedulerDp(applyMemoryFence = false, threadPoolSize = poolSize, threadFactory = new TestThreadFactory) {
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
