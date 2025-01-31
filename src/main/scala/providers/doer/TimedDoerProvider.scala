package readren.matrix
package providers.doer

import core.{AbstractMatrix, MatrixDoer}
import providers.ShutdownAble
import providers.assistant.TimedAssistantProvider
import timed.MatrixTimedDoer

import java.util.concurrent.{Executors, ThreadFactory}

class TimedDoerProvider(
	applyMemoryFence: Boolean = true,
	threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
	failureReporter: Throwable => Unit = _.printStackTrace(),
	threadFactory: ThreadFactory = Executors.defaultThreadFactory()
) extends AssistantBasedDoerProvider[MatrixTimedDoer] {
	override protected val assistantProvider: TimedAssistantProvider = new TimedAssistantProvider(applyMemoryFence, threadPoolSize, failureReporter, threadFactory)

	override def provide(matrix: AbstractMatrix): MatrixTimedDoer = {
		val doerId = matrix.genDoerId()
		new MatrixTimedDoer(doerId, assistantProvider.provide(doerId), matrix)
	}	
}