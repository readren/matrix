package readren.matrix
package providers.doer

import core.{AbstractMatrix, MatrixDoer}
import providers.ShutdownAble
import providers.assistant.CooperativeWorkersDap
import providers.assistant.DoerAssistantProvider.Tag

import java.util.concurrent.{Executors, ThreadFactory}

object CooperativeWorkersDoerProvider {
	class ProvidedDoer(
		override val tag: Tag,
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
		val tag = matrix.genTag()
		new CooperativeWorkersDoerProvider.ProvidedDoer(tag, assistantProvider.provide(tag), matrix)
	}
}