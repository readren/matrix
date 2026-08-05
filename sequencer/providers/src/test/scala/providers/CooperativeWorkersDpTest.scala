package readren.sequencer
package providers

import providers.CooperativeWorkersDp.*

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

class CooperativeWorkersDpTest extends DoerProviderTestBase[DoerFacade]
	with VanillaDoerTests[DoerFacade]
	with MonoTests[DoerFacade]
	with FluxDoerTests[DoerFacade]
	with CausalFenceTests[DoerFacade]
	with ResultIncrementalCoalescingTests[DoerFacade]
	with LoopingDoerTests[DoerFacade]
	with CooperativeWorkersChildTests[DoerFacade] { thisSuite =>

	override type DP = CooperativeWorkersDp.Impl

	override protected def buildDoerProvider(poolSize: Int): DP = new CooperativeWorkersDp.Impl(
		applyMemoryFence = false,
		threadPoolSize = poolSize,
		unhandledExceptionReporter = (doer, exception) => thisSuite.onUnhandledException(doer, exception),
		threadFactory = new TestThreadFactory
	)

	override protected def releaseDoerProvider(doerProvider: DP): Unit = {
		scribe.debug(s"Provider diagnostics:", doerProvider.diagnose(new StringBuilder()).toString())
		doerProvider.shutdown()
		doerProvider.awaitTermination(5, TimeUnit.SECONDS)
	}

	override val munitTimeout: Duration = scala.concurrent.duration.Duration(60, "seconds")
}
