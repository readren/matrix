package readren.sequencer
package providers

import readren.sequencer.ScheduledDoerTestEffectAbstractSuite

/** Tests if the [[Doer]] with [[SchedulingExtension]] instances provided by [[StandardSchedulingDp]] satisfy the [[Doer]] and [[SchedulingExtension]] invariants.
 */
class StandardSchedulingDpTest extends ScheduledDoerTestEffectAbstractSuite[StandardSchedulingDp.ProvidedDoerFacade] { thisSuite =>

	/** The [[DoerProvider]] whose provided [[Doer]] instances are tested. */
	private val doerProvider = new StandardSchedulingDp() {
		override protected def onUnhandledException(doer: Doer, exception: Throwable): Unit = thisSuite.onUnhandledException(doer, exception)

		override protected def onFailureReported(doer: Doer, failure: Throwable): Unit = thisSuite.onFailureReported(doer, failure)
	}

	override protected def buildDoer(tag: String): StandardSchedulingDp.ProvidedDoerFacade =
		doerProvider.provide("test-doer")

	/** Clean up resources after tests. */
	override def afterAll(): Unit = {
		println("Shutting down...")
		doerProvider.shutdown()
	}
}