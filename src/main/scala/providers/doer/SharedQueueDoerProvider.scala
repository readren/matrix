package readren.matrix
package providers.doer

import core.{AbstractMatrix, MatrixDoer}
import providers.ShutdownAble
import providers.assistant.SharedQueueDoerAssistantProvider

import java.util.concurrent.{Executors, ThreadFactory}

object SharedQueueDoerProvider {
	class ProvidedDoer(
		override val id: MatrixDoer.Id,
		override val assistant: SharedQueueDoerAssistantProvider.DoerAssistant,
		override val matrix: AbstractMatrix
	) extends MatrixDoer {
		override type Assistant = SharedQueueDoerAssistantProvider.DoerAssistant
	}
}

class SharedQueueDoerProvider(
	applyMemoryFence: Boolean = true,
	threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
	failureReporter: Throwable => Unit = _.printStackTrace(),
	threadFactory: ThreadFactory = Executors.defaultThreadFactory()
) extends AssistantBasedDoerProvider[SharedQueueDoerProvider.ProvidedDoer, SharedQueueDoerAssistantProvider.DoerAssistant] {
	override protected val assistantProvider = new SharedQueueDoerAssistantProvider(applyMemoryFence, threadPoolSize, failureReporter, threadFactory)

	override def provide(matrix: AbstractMatrix): SharedQueueDoerProvider.ProvidedDoer = {
		val doerId = matrix.genDoerId()
		new SharedQueueDoerProvider.ProvidedDoer(doerId, assistantProvider.provide(doerId), matrix)
	}
}