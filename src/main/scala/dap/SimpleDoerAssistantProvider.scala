package readren.matrix
package dap

import core.{Matrix, MatrixDoer}

import readren.taskflow.Doer

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.*

class SimpleDoerAssistantProvider(
	threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
	failureReporter: Throwable => Unit = _.printStackTrace(),
	threadFactory: ThreadFactory = Executors.defaultThreadFactory(),
	queueFactory: () => BlockingQueue[Runnable] = () => new LinkedBlockingQueue[Runnable]()
) extends Matrix.DoerAssistantProvider, ShutdownAble { thisProvider =>

	private class Entry(val assistant: Doer.Assistant, val executor: ThreadPoolExecutor)

	private val switcher = new AtomicInteger(0)

	/**
	 * The array with all the [[MatrixDoer]] instances of this [[Matrix]]. */
	private val matrixDoers: IArray[Entry] = {
		IArray.tabulate(threadPoolSize) { index =>
			val doSiThEx = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, queueFactory())
			Entry(new Assistant(doSiThEx), doSiThEx)
		}
	}

	override def provide(serial: MatrixDoer.Id): Doer.Assistant =
		matrixDoers(switcher.getAndIncrement() % matrixDoers.length).assistant

	private class Assistant(doSiThEx: ExecutorService) extends Doer.Assistant {

		override def queueForSequentialExecution(runnable: Runnable): Unit = doSiThEx.execute(runnable)

		override def reportFailure(cause: Throwable): Unit = cause.printStackTrace()
	}

	override def shutdown(): Unit = {
		for md <- matrixDoers do {
			md.executor.shutdown()
		}
	}

	override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = {
		// TODO subtract already waited time
		matrixDoers.forall(_.executor.awaitTermination(timeout, unit))
	}

	override def diagnose(sb: StringBuilder): StringBuilder = {
		var totalCompletedTaskCount: Long = 0
		sb.append("<<<\n")
		for (info, i) <- matrixDoers.zipWithIndex do {
			sb.append(i).append(") ")
			sb.append(" queue.size=").append(info.executor.getQueue.size)
			sb.append(", activeCount=").append(info.executor.getActiveCount)
			sb.append(", taskCount=").append(info.executor.getTaskCount)
			sb.append(", completedTaskCount=").append(info.executor.getCompletedTaskCount)
			// sb.append(", largestPoolSize=").append(info.executor.getLargestPoolSize)
			sb.append(", isTerminating=").append(info.executor.isTerminating)
			sb.append(", isTerminated=").append(info.executor.isTerminated)
			sb.append(", isShutdown=").append(info.executor.isShutdown)
			sb.append('\n')
			// info.lastRunnable.foreach(r => sb.append("Last runnable:\n").append(r.toString).append('\n'))
			totalCompletedTaskCount += info.executor.getCompletedTaskCount
		}
		sb.append("totalCompletedTasks=").append(totalCompletedTaskCount)
		sb.append("\n>>>\n")
		sb
	}

}
