package readren.matrix
package providers.doer

import core.{AbstractMatrix, MatrixDoer}
import providers.ShutdownAble
import providers.assistant.LeastLoadedFixedWorkerDap

import java.util.concurrent.*

object LeastLoadedFixedWorkerDoerProvider {
	class ProvidedDoer(
		override val id: MatrixDoer.Id,
		override val assistant: LeastLoadedFixedWorkerDap.AssistantImpl,
		override val matrix: AbstractMatrix
	) extends MatrixDoer {
		override type Assistant = LeastLoadedFixedWorkerDap.AssistantImpl
	}
}

/** A [[Matrix.DoerProvider]] that provides pinned instances of [[MatrixDoer]].
 * The provided instances use a [[Doer.Assistant]] provided by a [[LeastLoadedFixedWorkerDap]]. */
class LeastLoadedFixedWorkerDoerProvider(
	threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
	failureReporter: Throwable => Unit = _.printStackTrace(),
	threadFactory: ThreadFactory = Executors.defaultThreadFactory(),
	queueFactory: () => BlockingQueue[Runnable] = () => new LinkedBlockingQueue[Runnable]()
) extends AssistantBasedDoerProvider[LeastLoadedFixedWorkerDoerProvider.ProvidedDoer, LeastLoadedFixedWorkerDap.AssistantImpl] {
	override protected val assistantProvider = new LeastLoadedFixedWorkerDap(threadPoolSize, failureReporter, threadFactory, queueFactory)

	override def provide(matrix: AbstractMatrix): LeastLoadedFixedWorkerDoerProvider.ProvidedDoer = {
		val doerId = matrix.genDoerId()
		new LeastLoadedFixedWorkerDoerProvider.ProvidedDoer(doerId, assistantProvider.provide(doerId), matrix)
	}
}