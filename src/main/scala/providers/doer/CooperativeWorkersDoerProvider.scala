package readren.matrix
package providers.doer

import core.{AbstractMatrix, MatrixDoer}
import providers.ShutdownAble
import providers.assistant.CooperativeWorkersDap

import java.util.concurrent.{Executors, ThreadFactory}

object CooperativeWorkersDoerProvider {
	class ProvidedDoer(
		override val id: MatrixDoer.Id,
		override val assistant: CooperativeWorkersDap.DoerAssistant,
		override val matrix: AbstractMatrix
	) extends MatrixDoer {
		override type Assistant = CooperativeWorkersDap.DoerAssistant
	}
}

/** A [[Matrix.DoerProvider]] that provides non-pinned instances of [[MatrixDoer]].
 * The provided instances use a [[Doer.Assistant]] provided by a [[CooperativeWorkersDap]]. */
class CooperativeWorkersDoerProvider(
	applyMemoryFence: Boolean = true,
	threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
	failureReporter: Throwable => Unit = _.printStackTrace(),
	threadFactory: ThreadFactory = Executors.defaultThreadFactory()
) extends AssistantBasedDoerProvider[CooperativeWorkersDoerProvider.ProvidedDoer, CooperativeWorkersDap.DoerAssistant] {
	override protected val assistantProvider = new CooperativeWorkersDap(applyMemoryFence, threadPoolSize, failureReporter, threadFactory)

	override def provide(matrix: AbstractMatrix): CooperativeWorkersDoerProvider.ProvidedDoer = {
		val doerId = matrix.genDoerId()
		new CooperativeWorkersDoerProvider.ProvidedDoer(doerId, assistantProvider.provide(doerId), matrix)
	}
}