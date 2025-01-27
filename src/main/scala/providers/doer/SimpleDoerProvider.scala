package readren.matrix
package providers.doer

import providers.ShutdownAble
import providers.assistant.SimpleDoerAssistantProvider

import java.util.concurrent.{BlockingQueue, Executors, LinkedBlockingQueue, ThreadFactory}

class SimpleDoerProvider(
	threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
	failureReporter: Throwable => Unit = _.printStackTrace(),
	threadFactory: ThreadFactory = Executors.defaultThreadFactory(),
	queueFactory: () => BlockingQueue[Runnable] = () => new LinkedBlockingQueue[Runnable]()
) extends AssistantBasedDoerProvider {
	override protected val assistantProvider = new SimpleDoerAssistantProvider(threadPoolSize, failureReporter, threadFactory, queueFactory)
}