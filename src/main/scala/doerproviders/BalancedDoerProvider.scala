package readren.matrix
package doerproviders

import Matrix.DoerProvider

import readren.taskflow.Doer

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{BlockingQueue, ExecutorService, ThreadFactory, ThreadPoolExecutor, TimeUnit}

class BalancedDoerProvider(matrix: AbstractMatrix, threadFactory: ThreadFactory, threadPoolSize: Int, failureReporter: Throwable => Unit, queueFactory: () => BlockingQueue[Runnable]) extends DoerProvider, ShutdownAble {
	override type Doer = MatrixDoer

	private val executors = Array.tabulate[Executor](threadPoolSize)(index => new Executor(index))

	private val serialSequencer = new AtomicInteger(0)

	private class Executor(index: Int) extends ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, queueFactory(), threadFactory)

	private class Assistant(doSiThEx: ExecutorService) extends Doer.Assistant {
		override def queueForSequentialExecution(runnable: Runnable): Unit = doSiThEx.execute(runnable)

		override def reportFailure(cause: Throwable): Unit = failureReporter(cause)
	}

	override def provide(): MatrixDoer = {
		val serial = serialSequencer.getAndIncrement()
		val executorsWithShortestWorkQueue = findExecutorsWithShortestWorkQueue()
		val pickedExecutor =
			if executorsWithShortestWorkQueue.tail == Nil then executorsWithShortestWorkQueue.head
			else {
				val pickedExecutorIndex = serial % executorsWithShortestWorkQueue.size
				executorsWithShortestWorkQueue(pickedExecutorIndex)
			}
		new MatrixDoer(serial, new Assistant(pickedExecutor), matrix)
	}

	private def findExecutorsWithShortestWorkQueue(): List[Executor] = {
		var shortestSize = Integer.MAX_VALUE
		var result: List[Executor] = Nil

		for executor <- executors do {
			val queueSize = executor.getQueue.size()
			if queueSize < shortestSize then {
				result = List(executor)
				shortestSize = queueSize
			}
			else if queueSize == shortestSize then result = executor :: result
		}
		result
	}

	override def shutdown(): Unit = {
		for executor <- executors do {
			executor.shutdown()
		}
	}

	override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = {
		// TODO subtract already waited time
		executors.forall(_.awaitTermination(timeout, unit))
	}

	override def diagnose(sb: StringBuilder): StringBuilder = {
		var totalCompletedTaskCount: Long = 0
		sb.append(this.getClass.getSimpleName)
		sb.append('\n')
		for (executor, i) <- executors.zipWithIndex do {
			sb.append('\t').append(i).append(") ")
			sb.append(" queue.size=").append(executor.getQueue.size)
			sb.append(", activeCount=").append(executor.getActiveCount)
			sb.append(", taskCount=").append(executor.getTaskCount)
			sb.append(", completedTaskCount=").append(executor.getCompletedTaskCount)
			// sb.append(", largestPoolSize=").append(executor.getLargestPoolSize)
			sb.append(", isTerminating=").append(executor.isTerminating)
			sb.append(", isTerminated=").append(executor.isTerminated)
			sb.append(", isShutdown=").append(executor.isShutdown)
			sb.append('\n')
			// info.lastRunnable.foreach(r => sb.append("Last runnable:\n").append(r.toString).append('\n'))
			totalCompletedTaskCount += executor.getCompletedTaskCount
		}
		sb.append("\ttotalCompletedTasks=").append(totalCompletedTaskCount)
		sb
	}
}
