package readren.matrix
package providers.doer

import providers.ShutdownAble
import providers.assistant.BalancedDoerAssistantProvider

import java.util.concurrent.*

class BalancedDoerProvider(
	threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
	failureReporter: Throwable => Unit = _.printStackTrace(),
	threadFactory: ThreadFactory = Executors.defaultThreadFactory(),
	queueFactory: () => BlockingQueue[Runnable] = () => new LinkedBlockingQueue[Runnable]()
) extends AssistantBasedDoerProvider {
	override protected val assistantProvider = new BalancedDoerAssistantProvider(threadPoolSize, failureReporter, threadFactory, queueFactory)
}