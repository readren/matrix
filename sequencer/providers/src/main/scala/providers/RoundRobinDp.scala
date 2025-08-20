package readren.sequencer
package providers

import providers.DoerProvider.Tag
import providers.ShutdownAble

import readren.sequencer.Doer

import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger

object RoundRobinDp {
	
	class Impl(
		threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
		failureReporter: (Doer, Throwable) => Unit = DefaultDoerFaultReporter(true),
		threadFactory: ThreadFactory = Executors.defaultThreadFactory(),
		queueFactory: () => BlockingQueue[Runnable] = () => new LinkedBlockingQueue[Runnable]()
	) extends RoundRobinDp(threadPoolSize, threadFactory, queueFactory) {
		/** Called when a [[Runnable]] passed to the [[Doer.executeSequentially]] method of a provided [[Doer]] throws an exception. */
		override protected def onUnhandledException(doer: Doer, exception: Throwable): Unit = ??? // TODO if the Runnable passed to `executeSequentially` should be wrapped to notice about unhandled exceptions and expose them calling this method which would be implemented similar to `onFailureReported`.   

		/** Called when the [[Doer.reportFailure]] method of a provided [[Doer]] is called. */
		override protected def onFailureReported(doer: Doer, failure: Throwable): Unit = failureReporter(doer, failure)
	}
}

/** A [[Doer]] provider in which the provided [[Doer]]s are created when the instance of this class is created. One [[Doer]] instance per worker in the pool.
 * The [[provide]] method just gives one of them in a round-robin fashion.
 * How it works:
 * 		- every call to [[provide]] returns one of the already created [[Doer]] instances in a round-robin fashion.
 *		- When a [[Doer]] instance (provided by this provider) starts having pending tasks it is enqueued in a queue.
 * Effective for abundant, short-lived, or evenly-loaded [[Doer]]s.
 */
abstract class RoundRobinDp(
	threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
	threadFactory: ThreadFactory = Executors.defaultThreadFactory(),
	queueFactory: () => BlockingQueue[Runnable] = () => new LinkedBlockingQueue[Runnable]()
) extends DoerProvider[Doer], ShutdownAble { thisProvider =>

	private val switcher = new AtomicInteger(0)

	private val doers: IArray[ProvidedDoer] = IArray.tabulate(threadPoolSize) { index => new ProvidedDoer(s"round-robin#$index") }

	private val doerThreadLocal: ThreadLocal[ProvidedDoer] = new ThreadLocal

	/** @return the [[ProvidedDoer]] that is currently associated to the current [[Thread]], if any. */
	inline def currentDoer: ProvidedDoer | Null = doerThreadLocal.get

	override def provide(tag: Tag): ProvidedDoer =
		doers(switcher.getAndIncrement() % doers.length)


	class ProvidedDoer(override val tag: Tag) extends Doer { thisDoer =>

		override type Tag = DoerProvider.Tag

		private[RoundRobinDp] val doSiThEx: ThreadPoolExecutor = {
			val tf: ThreadFactory = (r: Runnable) => threadFactory.newThread { () =>
				doerThreadLocal.set(thisDoer)
				r.run()
			}

			new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, queueFactory(), tf)
		}

		override def executeSequentially(runnable: Runnable): Unit = doSiThEx.execute(runnable)

		override def current: ProvidedDoer = currentDoer

		override def reportFailure(cause: Throwable): Unit = onFailureReported(thisDoer, cause)
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
		sb.append("<<<\n")
		for (doer, i) <- doers.zipWithIndex do {
			sb.append(i).append(") ")
			sb.append(" queue.size=").append(doer.doSiThEx.getQueue.size)
			sb.append(", activeCount=").append(doer.doSiThEx.getActiveCount)
			sb.append(", taskCount=").append(doer.doSiThEx.getTaskCount)
			sb.append(", completedTaskCount=").append(doer.doSiThEx.getCompletedTaskCount)
			// sb.append(", largestPoolSize=").append(info.executor.getLargestPoolSize)
			sb.append(", isTerminating=").append(doer.doSiThEx.isTerminating)
			sb.append(", isTerminated=").append(doer.doSiThEx.isTerminated)
			sb.append(", isShutdown=").append(doer.doSiThEx.isShutdown)
			sb.append('\n')
			// info.lastRunnable.foreach(r => sb.append("Last runnable:\n").append(r.toString).append('\n'))
			totalCompletedTaskCount += doer.doSiThEx.getCompletedTaskCount
		}
		sb.append("totalCompletedTasks=").append(totalCompletedTaskCount)
		sb.append("\n>>>\n")
		sb
	}

}
