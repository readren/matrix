package readren.sequencer
package providers

import providers.DoerProvider.Tag
import providers.ShutdownAble

import readren.sequencer.Doer

import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger


object LeastLoadedFixedWorkerDp {
	class Impl(
		threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
		failureReporter: (Doer, Throwable) => Unit = DefaultDoerFaultReporter(true),
		threadFactory: ThreadFactory = Executors.defaultThreadFactory(),
		queueFactory: () => BlockingQueue[Runnable] = () => new LinkedBlockingQueue[Runnable]()
	) extends LeastLoadedFixedWorkerDp(threadPoolSize, threadFactory, queueFactory) {
		/** Called when a [[Runnable]] passed to the [[Doer.executeSequentially]] method of a provided [[Doer]] throws an exception. */
		override protected def onUnhandledException(doer: Doer, exception: Throwable): Unit = ???

		/** Called when the [[Doer.reportFailure]] method of a provided [[Doer]] is called. */
		override protected def onFailureReported(doer: Doer, failure: Throwable): Unit = failureReporter(doer, failure)
	}
}

/** A [[Doer]] provider with a non-dynamic thread-load balancing mechanism.
 * How it works:
 * - Manages a fixed number of [[Doer]] instances equal to the thread pool size, each of which owns a thread-worker.
 * - A call to the [[provide]] method returns the [[Doer]] instance with the shortest task queue at the moment of the call.
 * - All tasks submitted to a [[Doer]] are executed by the same thread-worker.
 * Effective for short-living doers that are created when the work is demanded.
 * Not suited for long-living doers. */
abstract class LeastLoadedFixedWorkerDp(
	threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
	threadFactory: ThreadFactory = Executors.defaultThreadFactory(),
	queueFactory: () => BlockingQueue[Runnable] = () => new LinkedBlockingQueue[Runnable]()
) extends DoerProvider[Doer], ShutdownAble {

	private val doers = Array.tabulate[ProvidedDoer](threadPoolSize)(index => new ProvidedDoer(s"LeastLoadedFixedWorker#$index"))

	private val switcher = new AtomicInteger(0)

	private val doerThreadLocal: ThreadLocal[ProvidedDoer] = new ThreadLocal()

	/** @return the [[ProvidedDoer]] that is currently associated to the current [[Thread]], if any. */
	override def currentDoer: ProvidedDoer | Null = doerThreadLocal.get

	class ProvidedDoer(
		override val tag: Tag,
		failureReporter: Throwable => Unit = _.printStackTrace(),
		threadFactory: ThreadFactory = Executors.defaultThreadFactory(),
		queueFactory: () => BlockingQueue[Runnable] = () => new LinkedBlockingQueue[Runnable]()
	) extends Doer { thisDoer =>
		override type Tag = DoerProvider.Tag
		val doSiThEx: ThreadPoolExecutor = {
			val tf: ThreadFactory = (r: Runnable) => threadFactory.newThread { () =>
				doerThreadLocal.set(thisDoer)
				r.run()
			}
			new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, queueFactory(), tf)
		}

		override def executeSequentially(runnable: Runnable): Unit = doSiThEx.execute(runnable)

		override def current: ProvidedDoer = currentDoer

		override def reportFailure(cause: Throwable): Unit = failureReporter(cause)
	}

	override def provide(tag: Tag): ProvidedDoer = {
		val doersWithShortestWorkQueue = findExecutorsWithShortestWorkQueue()
		val pickedDoer =
			if doersWithShortestWorkQueue.tail == Nil then doersWithShortestWorkQueue.head
			else {
				val pickedExecutorIndex = switcher.getAndIncrement() % doersWithShortestWorkQueue.size
				doersWithShortestWorkQueue(pickedExecutorIndex)
			}
		pickedDoer
	}

	private def findExecutorsWithShortestWorkQueue(): List[ProvidedDoer] = {
		var shortestSize = Integer.MAX_VALUE
		var result: List[ProvidedDoer] = Nil

		for doer <- doers do {
			val executor = doer.doSiThEx
			val queueSize = executor.getQueue.size()
			if queueSize < shortestSize then {
				result = List(doer)
				shortestSize = queueSize
			}
			else if queueSize == shortestSize then result = doer :: result
		}
		result
	}

	override def shutdown(): Unit = {
		for doer <- doers do {
			doer.doSiThEx.shutdown()
		}
	}

	override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = {
		// TODO subtract already waited time
		doers.forall(_.doSiThEx.awaitTermination(timeout, unit))
	}

	override def diagnose(sb: StringBuilder): StringBuilder = {
		var totalCompletedTaskCount: Long = 0
		sb.append(this.getClass.getSimpleName)
		sb.append('\n')
		for doer <- doers do {
			val executor = doer.doSiThEx
			sb.append('\t').append(doer.tag).append(") ")
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
