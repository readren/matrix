package readren.matrix
package providers.doer

import core.{AbstractMatrix, MatrixDoer}
import providers.ShutdownAble
import providers.assistant.RoundRobinDap

import java.util.concurrent.{BlockingQueue, Executors, LinkedBlockingQueue, ThreadFactory}

object RoundRobinDoerProvider {
	class ProvidedDoer(
		override val id: MatrixDoer.Id,
		override val assistant: RoundRobinDap.AssistantImpl,
		override val matrix: AbstractMatrix
	) extends MatrixDoer {
		override type Assistant = RoundRobinDap.AssistantImpl
	}
}

/** A [[Matrix.DoerProvider]] that provides pinned instances of [[MatrixDoer]].
 * The provided instances use a [[Doer.Assistant]] provided by a [[RoundRobinDap]]. */
class RoundRobinDoerProvider(
	threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
	failureReporter: Throwable => Unit = _.printStackTrace(),
	threadFactory: ThreadFactory = Executors.defaultThreadFactory(),
	queueFactory: () => BlockingQueue[Runnable] = () => new LinkedBlockingQueue[Runnable]()
) extends AssistantBasedDoerProvider[RoundRobinDoerProvider.ProvidedDoer, RoundRobinDap.AssistantImpl] {
	override protected val assistantProvider = new RoundRobinDap(threadPoolSize, failureReporter, threadFactory, queueFactory)

	override def provide(matrix: AbstractMatrix): RoundRobinDoerProvider.ProvidedDoer = {
		val doerId = matrix.genDoerId()
		new RoundRobinDoerProvider.ProvidedDoer(doerId, assistantProvider.provide(doerId), matrix)
	}	
}