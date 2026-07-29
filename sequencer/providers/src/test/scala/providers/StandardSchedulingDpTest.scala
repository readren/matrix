package readren.sequencer
package providers

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

/** Tests if the [[Doer]] with [[SchedulingExtension]] instances provided by [[StandardSchedulingDp]] satisfy the [[Doer]] and [[SchedulingExtension]] invariants.
 */
class StandardSchedulingDpTest extends SchedulingDoerProviderTest[StandardSchedulingDp.ProvidedDoerFacade] { thisSuite =>

	override type DP = StandardSchedulingDp

	/** The implementation should build an instance of the [[DoerProvider]] implementation under test. */
	override protected def buildDoerProvider: DP = new StandardSchedulingDp() {
		override type Tag = String

		override def tagFromText(text: String): Tag = text

		override protected def onUnhandledException(doer: Doer, exception: Throwable): Unit = thisSuite.onUnhandledException(doer, exception)
	}

	override def scalaCheckTestParameters: org.scalacheck.Test.Parameters =
		super.scalaCheckTestParameters.withMinSuccessfulTests(100)

	override val munitTimeout: Duration = scala.concurrent.duration.Duration(30, "seconds")

	/** The implementation should release the specified [[DoerProvider]].
	 * The implementation may assume that the provided instance was created calling [[buildDoerProvider]]. */
	override protected def releaseDoerProvider(doerProvider: DP): Unit = {
		doerProvider.shutdown()
		// doerProvider.awaitTermination(10, TimeUnit.SECONDS)
		val diagnostic = doerProvider.shutdownNow(9, TimeUnit.SECONDS)
		if !diagnostic._1 then println(s"Shutdown terminated forcefully after await timeout. Running Runnables per doer tag: ${diagnostic._2}")
	}
}