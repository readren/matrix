package readren.sequencer
package providers

import providers.CooperativeWorkersTieredDp.TieredDoerFacade

import readren.common.Maybe

import java.util.concurrent.{ConcurrentLinkedQueue, Executors, ThreadFactory}

object CooperativeWorkersTieredDp {
	/** Facade of the concrete type of the [[Doer]] instances provided by [[CooperativeWorkersTieredDp]].
	 * */
	trait TieredDoerFacade extends CooperativeWorkersDp.DoerFacade {
		def hasHighPriority: Boolean
	}

	class Impl(
		applyMemoryFence: Boolean = true,
		threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
		failureReporter: (Doer, Throwable) => Unit = DefaultDoerFaultReporter(true),
		unhandledExceptionReporter: (Doer, Throwable) => Unit = DefaultDoerFaultReporter(false),
		threadFactory: ThreadFactory = Executors.defaultThreadFactory()
	) extends CooperativeWorkersTieredDp(applyMemoryFence, threadPoolSize, threadFactory) {
		override type Tag = String

		override def tagFromText(text: String): Tag = text

		/** Called when a [[Runnable]] passed to the [[Doer.executeSequentially]] method of a provided [[Doer]] throws an exception. */
		override protected def onUnhandledException(doer: Doer, exception: Throwable): Unit = unhandledExceptionReporter(doer, exception)

		/** Called when the [[Doer.reportFailure]] method of a provided [[Doer]] is called. */
		override protected def onFailureReported(doer: Doer, failure: Throwable): Unit = failureReporter(doer, failure)
	}

}

/** Like [[CooperativeWorkersDp]] but overloads the [[provide]] method with a version that allows to specify the execution priority of the provided [[Doer]].
 * Only two levels of priority are supported: regular a high. */
abstract class CooperativeWorkersTieredDp(
	applyMemoryFence: Boolean = true,
	threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
	threadFactory: ThreadFactory = Executors.defaultThreadFactory()
) extends CooperativeWorkersDp(applyMemoryFence, threadPoolSize, threadFactory), DoerProvider[TieredDoerFacade] {

	/** Queue of [[TieredDoerImpl]] with pending tasks (are waiting to be assigned to a [[Worker]] in order to process them.
	 *
	 * Invariant: this queue contains no duplicate elements due to the [[DoerImpl.queueForSequentialExecution]] logic.
	 * TODO create and use an implementation of concurrent non-blocking queue that minimizes dynamic memory allocation. */
	protected val queuedPriorityDoers = new ConcurrentLinkedQueue[DoerImpl]()

	protected class TieredDoerImpl(tag: Tag, override val hasHighPriority: Boolean) extends DoerImpl(tag), TieredDoerFacade { thisDoer =>

		override def enqueueMyself(): Unit = {
			if thisDoer.hasHighPriority then queuedPriorityDoers.offer(thisDoer)
			else queuedDoers.offer(thisDoer)
		}
	}

	override protected def pollNextDoer(): DoerImpl | Null = {
		val next = queuedPriorityDoers.poll()
		if next ne null then next
		else queuedDoers.poll()
	}

	override def provide(tag: Tag): TieredDoerFacade = {
		startAllWorkersIfNotAlready()
		new TieredDoerImpl(tag, false)
	}

	override def currentDoer: Maybe[TieredDoerFacade] = super.currentDoer.asInstanceOf[Maybe[TieredDoerFacade]]

	def provide(tag: Tag, withHighPriority: Boolean): TieredDoerFacade = new TieredDoerImpl(tag, withHighPriority)
}
