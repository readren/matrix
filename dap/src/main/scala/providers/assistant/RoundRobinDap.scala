package readren.matrix
package providers.assistant

import providers.ShutdownAble
import providers.assistant.DoerAssistantProvider.Tag
import providers.assistant.RoundRobinDap.AssistantImpl

import readren.sequencer.Doer

import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger

object RoundRobinDap {
	private val currentAssistant: ThreadLocal[AssistantImpl] = new ThreadLocal

	class AssistantImpl(
		val index: Int,
		failureReporter: Throwable => Unit = _.printStackTrace(),
		threadFactory: ThreadFactory = Executors.defaultThreadFactory(),
		queueFactory: () => BlockingQueue[Runnable] = () => new LinkedBlockingQueue[Runnable]()
	) extends Doer.Assistant { thisAssistant =>

		val doSiThEx: ThreadPoolExecutor = {
			val tf: ThreadFactory = (r: Runnable) => threadFactory.newThread { () =>
				currentAssistant.set(thisAssistant)
				r.run()
			}

			new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, queueFactory(), tf)
		}

		override def executeSequentially(runnable: Runnable): Unit = doSiThEx.execute(runnable)

		override def current: AssistantImpl = currentAssistant.get

		override def reportFailure(cause: Throwable): Unit = failureReporter(cause)
	}
}

/** A [[Doer.Assistant]] provider in which the provided [[Doer.Assistant]]s are created when the instance of this class is created. One assistant per worker in the pool.
 * The [[provide]] method just gives one of them in a round-robin fashion.
 * How it works:
 * 		- every call to [[provide]] returns one of the already created [[Doer.Assistant]] instances in a round-robin fashion..
 *		- When an assistant (provided by this provider) starts having pending tasks it is enqueued in a queue.
 * Effective for abundant, short-lived, or evenly-loaded [[Doer]]s.
 */
class RoundRobinDap(
	threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
	failureReporter: Throwable => Unit = _.printStackTrace(),
	threadFactory: ThreadFactory = Executors.defaultThreadFactory(),
	queueFactory: () => BlockingQueue[Runnable] = () => new LinkedBlockingQueue[Runnable]()
) extends DoerAssistantProvider[RoundRobinDap.AssistantImpl], ShutdownAble { thisProvider =>

	private val switcher = new AtomicInteger(0)

	private val assistants: IArray[AssistantImpl] = IArray.tabulate(threadPoolSize) { index => new AssistantImpl(index, failureReporter, threadFactory, queueFactory) }


	override def provide(tag: Tag): AssistantImpl =
		assistants(switcher.getAndIncrement() % assistants.length)

	override def shutdown(): Unit = {
		for assistant <- assistants do {
			assistant.doSiThEx.shutdown()
		}
	}

	override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = {
		// TODO subtract already waited time
		assistants.forall(_.doSiThEx.awaitTermination(timeout, unit))
	}

	override def diagnose(sb: StringBuilder): StringBuilder = {
		var totalCompletedTaskCount: Long = 0
		sb.append("<<<\n")
		for (assistant, i) <- assistants.zipWithIndex do {
			sb.append(i).append(") ")
			sb.append(" queue.size=").append(assistant.doSiThEx.getQueue.size)
			sb.append(", activeCount=").append(assistant.doSiThEx.getActiveCount)
			sb.append(", taskCount=").append(assistant.doSiThEx.getTaskCount)
			sb.append(", completedTaskCount=").append(assistant.doSiThEx.getCompletedTaskCount)
			// sb.append(", largestPoolSize=").append(info.executor.getLargestPoolSize)
			sb.append(", isTerminating=").append(assistant.doSiThEx.isTerminating)
			sb.append(", isTerminated=").append(assistant.doSiThEx.isTerminated)
			sb.append(", isShutdown=").append(assistant.doSiThEx.isShutdown)
			sb.append('\n')
			// info.lastRunnable.foreach(r => sb.append("Last runnable:\n").append(r.toString).append('\n'))
			totalCompletedTaskCount += assistant.doSiThEx.getCompletedTaskCount
		}
		sb.append("totalCompletedTasks=").append(totalCompletedTaskCount)
		sb.append("\n>>>\n")
		sb
	}

}
