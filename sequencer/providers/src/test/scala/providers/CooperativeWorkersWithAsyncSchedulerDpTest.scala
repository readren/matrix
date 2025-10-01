package readren.sequencer
package providers

import readren.sequencer.SchedulingDoerProviderTest

/** Tests if the [[Doer]] with [[SchedulingExtension]] instances provided by [[StandardSchedulingDp]] satisfy the [[Doer]] and [[SchedulingExtension]] invariants.
 */
class CooperativeWorkersWithAsyncSchedulerDpTest extends SchedulingDoerProviderTest[CooperativeWorkersWithAsyncSchedulerDp.SchedulingDoerFacade] { thisSuite =>

	override type DP = CooperativeWorkersWithAsyncSchedulerDp

	/** The implementation should build an instance of the [[DoerProvider]] implementation under test. */
	override protected def buildDoerProvider: DP = new CooperativeWorkersWithAsyncSchedulerDp(applyMemoryFence = false) {
		override type Tag = String

		override def tagFromText(text: String): Tag = text

		override protected def onUnhandledException(doer: Doer, exception: Throwable): Unit = thisSuite.onUnhandledException(doer, exception)

		override protected def onFailureReported(doer: Doer, failure: Throwable): Unit = thisSuite.onFailureReported(doer, failure)
	}

	/** The implementation should release the specified [[DoerProvider]].
	 * The implementation may assume that the provided instance was created calling [[buildDoerProvider]]. */
	override protected def releaseDoerProvider(doerProvider: DP): Unit =
		doerProvider.shutdown()
}