package readren.sequencer
package providers

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

class StandardSchedulingDpTest extends DoerProviderTestBase[StandardSchedulingDp.ProvidedDoerFacade]
	with VanillaDoerTests[StandardSchedulingDp.ProvidedDoerFacade]
	with MonoTests[StandardSchedulingDp.ProvidedDoerFacade]
	with FluxDoerTests[StandardSchedulingDp.ProvidedDoerFacade]
	with CausalFenceTests[StandardSchedulingDp.ProvidedDoerFacade]
	with ResultIncrementalCoalescingDoerTests[StandardSchedulingDp.ProvidedDoerFacade]
	with LoopingDoerTests[StandardSchedulingDp.ProvidedDoerFacade]
	with ScheduledMonoTests[StandardSchedulingDp.ProvidedDoerFacade]
	with ScheduledFluxTests[StandardSchedulingDp.ProvidedDoerFacade] { thisSuite =>

	override type DP = StandardSchedulingDp

	override protected def buildDoerProvider(poolSize: Int): DP = new StandardSchedulingDp() {
		override type Tag = String

		override def tagFromText(text: String): Tag = text

		override protected def onUnhandledException(doer: Doer, exception: Throwable): Unit = thisSuite.onUnhandledException(doer, exception)
	}

	override def scalaCheckTestParameters: org.scalacheck.Test.Parameters =
		super.scalaCheckTestParameters.withMinSuccessfulTests(100)

	override val munitTimeout: Duration = scala.concurrent.duration.Duration(30, "seconds")

	override protected def releaseDoerProvider(doerProvider: DP): Unit = {
		doerProvider.shutdown()
		val diagnostic = doerProvider.shutdownNow(9, TimeUnit.SECONDS)
		if !diagnostic._1 then println(s"Shutdown terminated forcefully after await timeout. Running Runnables per doer tag: ${diagnostic._2}")
	}
}
