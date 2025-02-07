package readren.matrix
package providers.doer

import core.{AbstractMatrix, MatrixDoer}
import providers.ShutdownAble
import providers.assistant.BalancedDoerAssistantProvider

import java.util.concurrent.*

object BalancedDoerProvider {
	class ProvidedDoer(
		override val id: MatrixDoer.Id,
		override val assistant: BalancedDoerAssistantProvider.AssistantImpl,
		override val matrix: AbstractMatrix
	) extends MatrixDoer {
		override type Assistant = BalancedDoerAssistantProvider.AssistantImpl
	}
}

class BalancedDoerProvider(
	threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
	failureReporter: Throwable => Unit = _.printStackTrace(),
	threadFactory: ThreadFactory = Executors.defaultThreadFactory(),
	queueFactory: () => BlockingQueue[Runnable] = () => new LinkedBlockingQueue[Runnable]()
) extends AssistantBasedDoerProvider[MatrixDoer, BalancedDoerAssistantProvider.AssistantImpl] {
	override protected val assistantProvider = new BalancedDoerAssistantProvider(threadPoolSize, failureReporter, threadFactory, queueFactory)

	override def provide(matrix: AbstractMatrix): MatrixDoer = {
		val doerId = matrix.genDoerId()
		new BalancedDoerProvider.ProvidedDoer(doerId, assistantProvider.provide(doerId), matrix)
	}
}