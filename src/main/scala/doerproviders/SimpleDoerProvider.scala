package readren.matrix
package doerproviders

import readren.taskflow.Doer

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ExecutorService, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

class SimpleDoerProvider(owner: Matrix[SimpleDoerProvider]) extends Matrix.DoerProvider, ShutdownAble { thisProvider =>

	private class Entry(val doer: MatrixDoer, val executor: ThreadPoolExecutor)

	private val serialSequencer = new AtomicInteger(0)

	/**
	 * The array with all the [[MatrixDoer]] instances of this [[Matrix]]. */
	private val matrixDoers: IArray[Entry] = {
		val availableProcessors = Runtime.getRuntime.availableProcessors()
		IArray.tabulate(availableProcessors) { index =>
			val doerId = index + 1
			val queue = new LinkedBlockingQueue[Runnable]()
			val doSiThEx = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, queue)
			val doer = new MatrixDoer(doerId, new Assistant(doSiThEx), owner)
			Entry(doer, doSiThEx)
		}
	}

	override def provide(): MatrixDoer =
		matrixDoers(serialSequencer.getAndIncrement() % matrixDoers.length).doer

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
