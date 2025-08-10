package readren.matrix
package providers.doer

import core.{AbstractMatrix, MatrixDoer}
import providers.assistant.DoerAssistantProvider.Tag
import providers.assistant.SchedulingDap

import readren.sequencer.SchedulingExtension

import java.util.concurrent.{Executors, ThreadFactory}

object SchedulingDoerProvider {
	class ProvidedDoer(
		override val tag: Tag,
		override val assistant: SchedulingDap.SchedulingAssistant,
		override val matrix: AbstractMatrix
	) extends MatrixDoer, SchedulingExtension {
		override type Assistant = SchedulingDap.SchedulingAssistant
	}
}

/** A [[Matrix.DoerProvider]] that provides non-pinned instances of [[MatrixDoer]] that support scheduling operations.
 * The provided instances use a [[Doer.Assistant]] provided by a [[SchedulingDap]]. */
class SchedulingDoerProvider(
	applyMemoryFence: Boolean = true,
	threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
	failureReporter: Throwable => Unit = _.printStackTrace(),
	threadFactory: ThreadFactory = Executors.defaultThreadFactory()
) extends AssistantBasedDoerProvider[SchedulingDoerProvider.ProvidedDoer, SchedulingDap.SchedulingAssistant] {
	override protected val assistantProvider: SchedulingDap = new SchedulingDap(applyMemoryFence, threadPoolSize, failureReporter, threadFactory)

	override def provide(matrix: AbstractMatrix): SchedulingDoerProvider.ProvidedDoer = {
		val tag = matrix.genTag()
		new SchedulingDoerProvider.ProvidedDoer(tag, assistantProvider.provide(tag), matrix)
	}	
}
