package readren.sequencer
package providers

import readren.sequencer.ScheduledDoerTestEffectAbstractSuite

/** Tests if the [[Doer]] with [[SchedulingExtension]] instances provided by [[StandardSchedulingDp]] satisfy the [[Doer]] and [[SchedulingExtension]] invariants.
 */
class StandardSchedulingDpTest extends ScheduledDoerTestEffectAbstractSuite[StandardSchedulingDp.ProvidedDoerFacade] { thisSuite =>

	override type DP = StandardSchedulingDp

	/** The implementation should build an instance of the [[DoerProvider]] implementation under test. */
	override protected def buildDoerProvider: DP = new StandardSchedulingDp() {
		override protected def onUnhandledException(doer: Doer, exception: Throwable): Unit = thisSuite.onUnhandledException(doer, exception)

		override protected def onFailureReported(doer: Doer, failure: Throwable): Unit = thisSuite.onFailureReported(doer, failure)
	}

	/** The implementation should release the specified [[DoerProvider]].
	 * The implementation may assume that the provided instance was created calling [[buildDoerProvider]]. */
	override protected def releaseDoerProvider(doerProvider: DP): Unit = {
		// TODO correct this class' type parameter to avoid this instanceOf
		doerProvider.shutdown()
	}
}