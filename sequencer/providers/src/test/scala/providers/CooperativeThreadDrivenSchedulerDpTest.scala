package readren.sequencer
package providers

import readren.sequencer.SchedulingDoerProviderTest

/** Tests if the [[Doer]] with [[SchedulingExtension]] instances provided by [[StandardSchedulingDp]] satisfy the [[Doer]] and [[SchedulingExtension]] invariants.
 */
class CooperativeThreadDrivenSchedulerDpTest extends SchedulingDoerProviderTest[CooperativeThreadDrivenSchedulerDp.SchedulingDoerFacade] { thisSuite =>

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