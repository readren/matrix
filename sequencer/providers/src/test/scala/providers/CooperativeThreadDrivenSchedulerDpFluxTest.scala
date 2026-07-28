package readren.sequencer
package providers

import readren.sequencer.FluxDoerProviderTest

/** Tests if the [[Doer]] with [[FluxExtension]] instances provided by [[CooperativeThreadDrivenSchedulerDp]] satisfy the [[FluxExtension]] invariants.
 */
class CooperativeThreadDrivenSchedulerDpFluxTest extends FluxDoerProviderTest[CooperativeThreadDrivenSchedulerDp.SchedulingDoerFacade] { thisSuite =>

	override type DP = CooperativeThreadDrivenSchedulerDp

	override protected def buildDoerProvider: DP = new CooperativeThreadDrivenSchedulerDp(applyMemoryFence = false, threadFactory = new TestThreadFactory) {
		override type Tag = String

		override def tagFromText(text: String): Tag = text

		override protected def onUnhandledException(doer: Doer, exception: Throwable): Unit = thisSuite.onUnhandledException(doer, exception)
	}

	override def scalaCheckTestParameters: org.scalacheck.Test.Parameters =
		super.scalaCheckTestParameters.withMinSuccessfulTests(100)

	override protected def releaseDoerProvider(doerProvider: DP): Unit =
		doerProvider.shutdown()
}
