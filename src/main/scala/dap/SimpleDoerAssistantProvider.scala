package readren.matrix
package dap

import core.{Matrix, MatrixDoer}
import dap.SimpleDoerAssistantProvider.currentAssistant

import readren.taskflow.Doer

import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger

object SimpleDoerAssistantProvider {
	private val currentAssistant: ThreadLocal[Doer.Assistant] = new ThreadLocal
}

class SimpleDoerAssistantProvider(
	threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
	failureReporter: Throwable => Unit = _.printStackTrace(),
	threadFactory: ThreadFactory = Executors.defaultThreadFactory(),
	queueFactory: () => BlockingQueue[Runnable] = () => new LinkedBlockingQueue[Runnable]()
) extends Matrix.DoerAssistantProvider, ShutdownAble { thisProvider =>

	private val switcher = new AtomicInteger(0)

	private val assistants: IArray[Assistant] = IArray.tabulate(threadPoolSize) { index => new Assistant(index) }
	

	override def provide(serial: MatrixDoer.Id): Doer.Assistant =
		assistants(switcher.getAndIncrement() % assistants.length)

	private class Assistant(val index: Int) extends Doer.Assistant { thisAssistant =>

		val doSiThEx: ThreadPoolExecutor = {
			val tf: ThreadFactory = (r: Runnable) => threadFactory.newThread { () =>
				currentAssistant.set(thisAssistant)
				r.run()
			}

			new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, queueFactory(), tf)
		}
		
		override def executeSequentially(runnable: Runnable): Unit = doSiThEx.execute(runnable)

		override def current: Doer.Assistant = currentAssistant.get

		override def reportFailure(cause: Throwable): Unit = failureReporter(cause)
	}

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
