package readren.sequencer
package providers

import readren.sequencer.ScheduledDoerTestEffectAbstractSuite

/** Tests if the [[Doer]] with [[SchedulingExtension]] instances provided by [[StandardSchedulingDp]] satisfy the [[Doer]] and [[SchedulingExtension]] invariants.
 */
class CooperativeWorkersSchedulingDpTest extends ScheduledDoerTestEffectAbstractSuite[CooperativeWorkersSchedulingDp.SchedulingDoerFacade] { thisSuite =>

	/** The [[DoerProvider]] whose provided [[Doer]] instances are tested. */
	private val doerProvider = new CooperativeWorkersSchedulingDp() {
		override protected def onUnhandledException(doer: Doer, exception: Throwable): Unit = thisSuite.onUnhandledException(doer, exception)

		override protected def onFailureReported(doer: Doer, failure: Throwable): Unit = thisSuite.onFailureReported(doer, failure)
	}

	override protected def buildDoer: CooperativeWorkersSchedulingDp.SchedulingDoerFacade =
		doerProvider.provide("test-doer")

	/** Clean up resources after tests. */
	override def afterAll(): Unit = {
		doerProvider.shutdown()
	}
}