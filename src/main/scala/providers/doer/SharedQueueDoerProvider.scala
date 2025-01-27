package readren.matrix
package providers.doer

import providers.ShutdownAble
import providers.assistant.SharedQueueDoerAssistantProvider

import java.util.concurrent.{Executors, ThreadFactory}

class SharedQueueDoerProvider(
	applyMemoryFence: Boolean = true,
	threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
	failureReporter: Throwable => Unit = _.printStackTrace(),
	threadFactory: ThreadFactory = Executors.defaultThreadFactory()
) extends AssistantBasedDoerProvider {
	override protected val assistantProvider = new SharedQueueDoerAssistantProvider(applyMemoryFence, threadPoolSize, failureReporter, threadFactory)
}