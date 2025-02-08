package readren.matrix
package providers.doer

import core.{AbstractMatrix, MatrixDoer}
import providers.ShutdownAble
import providers.assistant.SimpleDoerAssistantProvider

import java.util.concurrent.{BlockingQueue, Executors, LinkedBlockingQueue, ThreadFactory}

object SimpleDoerProvider {
	class ProvidedDoer(
		override val id: MatrixDoer.Id,
		override val assistant: SimpleDoerAssistantProvider.AssistantImpl,
		override val matrix: AbstractMatrix
	) extends MatrixDoer {
		override type Assistant = SimpleDoerAssistantProvider.AssistantImpl
	}
}

class SimpleDoerProvider(
	threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
	failureReporter: Throwable => Unit = _.printStackTrace(),
	threadFactory: ThreadFactory = Executors.defaultThreadFactory(),
	queueFactory: () => BlockingQueue[Runnable] = () => new LinkedBlockingQueue[Runnable]()
) extends AssistantBasedDoerProvider[SimpleDoerProvider.ProvidedDoer, SimpleDoerAssistantProvider.AssistantImpl] {
	override protected val assistantProvider = new SimpleDoerAssistantProvider(threadPoolSize, failureReporter, threadFactory, queueFactory)

	override def provide(matrix: AbstractMatrix): SimpleDoerProvider.ProvidedDoer = {
		val doerId = matrix.genDoerId()
		new SimpleDoerProvider.ProvidedDoer(doerId, assistantProvider.provide(doerId), matrix)
	}	
}