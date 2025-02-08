package readren.matrix
package providers.doer

import core.{AbstractMatrix, MatrixDoer}
import providers.assistant.SchedulingAssistantProvider

import readren.taskflow.SchedulingExtension

import java.util.concurrent.{Executors, ThreadFactory}

object SchedulingDoerProvider {
	class ProvidedDoer(
		override val id: MatrixDoer.Id,
		override val assistant: SchedulingAssistantProvider.SchedulingAssistant,
		override val matrix: AbstractMatrix
	) extends MatrixDoer, SchedulingExtension {
		override type Assistant = SchedulingAssistantProvider.SchedulingAssistant
	}
}

class SchedulingDoerProvider(
	applyMemoryFence: Boolean = true,
	threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
	failureReporter: Throwable => Unit = _.printStackTrace(),
	threadFactory: ThreadFactory = Executors.defaultThreadFactory()
) extends AssistantBasedDoerProvider[SchedulingDoerProvider.ProvidedDoer, SchedulingAssistantProvider.SchedulingAssistant] {
	override protected val assistantProvider: SchedulingAssistantProvider = new SchedulingAssistantProvider(applyMemoryFence, threadPoolSize, failureReporter, threadFactory)

	override def provide(matrix: AbstractMatrix): SchedulingDoerProvider.ProvidedDoer = {
		val doerId = matrix.genDoerId()
		new SchedulingDoerProvider.ProvidedDoer(doerId, assistantProvider.provide(doerId), matrix)
	}	
}
