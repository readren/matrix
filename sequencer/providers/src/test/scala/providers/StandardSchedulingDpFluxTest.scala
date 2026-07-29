package readren.sequencer
package providers

/** Tests if the [[Doer]] with [[FluxExtension]] instances provided by [[StandardSchedulingDp]] satisfy the [[FluxExtension]] invariants.
 */
class StandardSchedulingDpFluxTest extends FluxDoerProviderTest[StandardSchedulingDp.ProvidedDoerFacade] { thisSuite =>

	override type DP = StandardSchedulingDp

	override protected def buildDoerProvider: DP = new StandardSchedulingDp() {
		override type Tag = String

		override def tagFromText(text: String): Tag = text

		override protected def onUnhandledException(doer: Doer, exception: Throwable): Unit = thisSuite.onUnhandledException(doer, exception)
	}

	override protected def releaseDoerProvider(doerProvider: DP): Unit = {
		doerProvider.shutdown()
	}
}
