package readren.matrix
package providers.doer

import core.{AbstractMatrix, MatrixDoer}
import providers.assistant.SchedulingAssistantProvider
import providers.doer.SchedulingDoerProvider.ProvidedDoer

import readren.taskflow.SchedulingExtension

import java.util.concurrent.{Executors, ThreadFactory}

object SchedulingDoerProvider {
	class ProvidedDoer(id: MatrixDoer.Id, anAssistant: SchedulingAssistantProvider.SchedulingAssistant, matrix: AbstractMatrix) extends MatrixDoer(id, anAssistant, matrix), SchedulingExtension {
		override type SchedulingAssistant = SchedulingAssistantProvider.SchedulingAssistant
		override val schedulingAssistant: SchedulingAssistant = anAssistant
	}
}

class SchedulingDoerProvider(
	applyMemoryFence: Boolean = true,
	threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
	failureReporter: Throwable => Unit = _.printStackTrace(),
	threadFactory: ThreadFactory = Executors.defaultThreadFactory()
) extends AssistantBasedDoerProvider[ProvidedDoer, SchedulingAssistantProvider.SchedulingAssistant] {
	override protected val assistantProvider: SchedulingAssistantProvider = new SchedulingAssistantProvider(applyMemoryFence, threadPoolSize, failureReporter, threadFactory)

	override def provide(matrix: AbstractMatrix): ProvidedDoer = {
		val doerId = matrix.genDoerId()
		new ProvidedDoer(doerId, assistantProvider.provide(doerId), matrix)
	}	
}
