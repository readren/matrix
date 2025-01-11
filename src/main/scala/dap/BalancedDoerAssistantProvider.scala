package readren.matrix
package dap

import core.Matrix.DoerAssistantProvider
import core.MatrixDoer
import dap.BalancedDoerAssistantProvider.currentAssistant

import readren.taskflow.Doer

import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger

object BalancedDoerAssistantProvider {
	private val currentAssistant: ThreadLocal[Doer.Assistant] = new ThreadLocal()
}

class BalancedDoerAssistantProvider(
	threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
	failureReporter: Throwable => Unit = _.printStackTrace(),
	threadFactory: ThreadFactory = Executors.defaultThreadFactory(),
	queueFactory: () => BlockingQueue[Runnable] = () => new LinkedBlockingQueue[Runnable]()
) extends DoerAssistantProvider, ShutdownAble {

	private val assistants = Array.tabulate[Assistant](threadPoolSize)(index => new Assistant(index))

	private val switcher = new AtomicInteger(0)

	private class Assistant(val index: Int) extends Doer.Assistant { thisAssistant =>
		val doSiThEx: ThreadPoolExecutor = {
			val tf: ThreadFactory = (r: Runnable) => threadFactory.newThread { () =>
				currentAssistant.set(thisAssistant)
				r.run()
			}
			new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, queueFactory(), tf)
		}
		
		override def queueForSequentialExecution(runnable: Runnable): Unit = doSiThEx.execute(runnable)

		override def current: Doer.Assistant = currentAssistant.get
		
		override def reportFailure(cause: Throwable): Unit = failureReporter(cause)
	}

	override def provide(serial: MatrixDoer.Id): Doer.Assistant = {
		val assistantsWithShortestWorkQueue = findExecutorsWithShortestWorkQueue()
		val pickedAssistant =
			if assistantsWithShortestWorkQueue.tail == Nil then assistantsWithShortestWorkQueue.head
			else {
				val pickedExecutorIndex = switcher.getAndIncrement() % assistantsWithShortestWorkQueue.size
				assistantsWithShortestWorkQueue(pickedExecutorIndex)
			}
		pickedAssistant
	}

	private def findExecutorsWithShortestWorkQueue(): List[Assistant] = {
		var shortestSize = Integer.MAX_VALUE
		var result: List[Assistant] = Nil

		for assistant <- assistants do {
			val executor = assistant.doSiThEx
			val queueSize = executor.getQueue.size()
			if queueSize < shortestSize then {
				result = List(assistant)
				shortestSize = queueSize
			}
			else if queueSize == shortestSize then result = assistant :: result
		}
		result
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
		sb.append(this.getClass.getSimpleName)
		sb.append('\n')
		for assistant <- assistants do {
			val executor = assistant.doSiThEx
			sb.append('\t').append(assistant.index).append(") ")
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
