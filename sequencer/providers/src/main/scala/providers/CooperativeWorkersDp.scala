package readren.sequencer
package providers

import providers.CooperativeWorkersDp.*
import providers.ShutdownAble

import readren.common.CompileTime.getTypeName
import readren.common.Maybe

import java.lang.invoke.VarHandle
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger

object CooperativeWorkersDp {
	type TaskQueue = ConcurrentLinkedQueue[Runnable]

	enum State {
		case notStarted, keepRunning, shutdownWhenAllWorkersSleep, terminated
	}

	inline val debugEnabled = false

	/** Facade of the concrete type of the [[Doer]] instances provided by [[CooperativeWorkersDp]].
	 *
	 * Design note: to reduce class-metadata of extending classes, this facade was defined as an abstract class that extends [[AbstractDoer]] instead of a trait that extends [[Doer]].
	 * If this design causes type-hierarchy problems, define it as a trait that extends [[Doer]] instead of [[AbstractDoer]].
	 * */
	abstract class DoerFacade extends AbstractDoer {
		/** Exposes the number of routines that are waiting to be executed sequentially. */
		def numOfPendingTasks: Int
	}


	class Impl(
		applyMemoryFence: Boolean = true,
		threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
		failureReporter: (Doer, Throwable) => Unit = DefaultDoerFaultReporter(true),
		unhandledExceptionReporter: (Doer, Throwable) => Unit = DefaultDoerFaultReporter(false),
		threadFactory: ThreadFactory = Executors.defaultThreadFactory()
	) extends CooperativeWorkersDp(applyMemoryFence, threadPoolSize, threadFactory) {
		override type Tag = String

		override def tagFromText(text: String): Tag = text

		/** Called when a [[Runnable]] passed to the [[Doer.executeSequentially]] method of a provided [[Doer]] throws an exception. */
		override protected def onUnhandledException(doer: Doer, exception: Throwable): Unit = unhandledExceptionReporter(doer, exception)

		/** Called when the [[Doer.reportFailure]] method of a provided [[Doer]] is called. */
		override protected def onFailureReported(doer: Doer, failure: Throwable): Unit = failureReporter(doer, failure)
	}
}

/** A [[DoerProvider]] in which the task queue of the provided [[Doer]]s is processed by any worker of the pool, the first that gets free.
 * How it works:
 * 		- every call to [[provide]] returns a new [[Doer]] instance.
 *		- When an [[Doer]] instance (provided by this provider) starts having pending tasks, it is enqueued in a queue.
 *		- When a thread-worker of the pool gets free it polls a doer from said queue and processes all the pending tasks that the thread-worker can sse before continuing with the next doer from the queue.
 *		- After processing all the visible task, if there is a non-visible one, the doer is enqueue back.	
 *		- If the queue is empty the thread-worker goes to sleep.
 *		- When a doer is enqueued (because its tasks-queue transitions from empty to nonempty) a single sleeping thread-worker is awakened if there is some.
 * Effective for all purposes. Shines when the processor demand of long-living doers is very variant.   		
 * @param applyMemoryFence Determines whether memory fences are applied to ensure that store operations made by a task happen before load operations performed by successive tasks enqueued to the same [[Doer]]. 
 * The application of memory fences is optional because no test case has been devised to demonstrate their necessity. Apparently, the ordering constraints are already satisfied by the surrounding code.
 */
abstract class CooperativeWorkersDp(
	applyMemoryFence: Boolean = true,
	threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
	threadFactory: ThreadFactory = Executors.defaultThreadFactory()
) extends DoerProvider[DoerFacade], ShutdownAble { thisProvider =>

	private val state: AtomicInteger = new AtomicInteger(State.notStarted.ordinal)

	/** Queue of [[DoerImpl]] with pending tasks.
	 * We say that a [[DoerImpl]] has pending tasks if its [[DoerImpl.taskQueueSize]] is greater than zero, which means that it has tasks waiting to be executed and therefore it should be assigned to a [[Worker]] in order to process them.
	 *
	 * Invariant: this queue contains no duplicate elements due to the [[DoerImpl.queueForSequentialExecution]] logic.
	 * TODO create and use an implementation of concurrent non-blocking queue that minimizes dynamic memory allocation. */
	protected val queuedDoers = new ConcurrentLinkedQueue[DoerImpl]()

	private val workers: Array[Worker] = Array.tabulate(threadPoolSize)(buildWorker)

	private val runningWorkersLatch: CountDownLatch = new CountDownLatch(workers.length)
	/** Usually equal to the number of workers whose [[Worker.isSleeping]] flag is set, but may be temporarily greater. Never smaller.
	 * Invariant: {{{ workers.count(_.isSleeping) <= sleepingWorkersCount.get <= workers.length }}} */
	private val sleepingWorkersCount = AtomicInteger(0)

	private val workerThreadLocal: ThreadLocal[Runnable] = new ThreadLocal()
	private val doerThreadLocal: ThreadLocal[DoerFacade | Null] = new ThreadLocal()

	protected def buildWorker(index: Int): Worker = new Worker(index)

	/** @return the [[CooperativeWorkersDp.Worker]] that owns the current [[Thread]], if any.
	 *  Exposed for testing only. */
	inline private[providers] def currentWorker: Runnable | Null = workerThreadLocal.get

	/** @return the [[DoerFacade]] that is currently associated to the current [[Thread]], if any. */
	override def currentDoer: Maybe[DoerFacade] = Maybe(doerThreadLocal.get)

	protected open class DoerImpl(override val tag: Tag) extends DoerFacade { thisDoer =>

		override type Tag = thisProvider.Tag

		private val taskQueue: TaskQueue = new ConcurrentLinkedQueue[Runnable]
		private val taskQueueSize: AtomicInteger = new AtomicInteger(0)
		@volatile protected var firstTaskInQueue: Runnable = null
		/** Remembers the index of the worker that executed this doer's tasks the last time. This allows reusing the same worker if available, to take advantage of CPU-core local cache. */
		private[CooperativeWorkersDp] var lastTimeWorkerIndex = 0

		override def numOfPendingTasks: Int = taskQueueSize.get

		override def executeSequentially(task: Runnable): Unit = {
			if enqueueTask(task) && !wakeUpASleepingWorkerIfAny(thisDoer) then enqueueMyself()
		}

		/** Enqueues a [[Runnable]] to this [[DoerImpl]] queue.
		 * @return true if the queue transitioned from empty to non-empty thanx to this call. */
		inline def enqueueTask(task: Runnable): Boolean = {
			if taskQueueSize.getAndIncrement() > 0 then {
				taskQueue.offer(task)
				false
			} else {
				firstTaskInQueue = task
				true
			}
		}

		/** Enqueues this [[DoerImpl]] in the queue of [[DoerImpl]] instances that have pending tasks. */
		protected def enqueueMyself(): Unit = {
			queuedDoers.offer(thisDoer)
		}

		override def current: Maybe[DoerFacade] = Maybe(doerThreadLocal.get)

		override def reportFailure(cause: Throwable): Unit = onFailureReported(thisDoer, cause)

		/** Executes all the pending tasks that are visible from the calling [[Worker.thread]].
		 * Assumes that [[taskQueueSize]] is greater than zero because, for this method to be called, this [[DoerImpl]] should have been added to the [[queuedDoers]], which happens when the [[taskQueueSize]] transitions from zero to one.
		 *
		 * Note: The [[taskQueueSize]] is decremented not immediately after polling a task from the [[taskQueue]] but only after the task is executed.
		 * This ensures that calls to [[executeSequentially]] by other threads while the worker is executing the task see a [[taskQueueSize]] greater than zero and, therefore, impeding two tasks of the same doer being executed simultaneously. In other words: avoiding the violation of the constraint that prevents two workers from being assigned to the same [[DoerImpl]] instance simultaneously.
		 *
		 * If at least one pending task remains unconsumed — typically because it is not yet visible from the [[Worker.thread]] — this [[DoerImpl]] is enqueued into the [[queuedDoers]] queue to be assigned to a worker at a later time.
		 * @param worker the [[Worker]] that called this method and owns the current [[Thread]].
		 */
		private[CooperativeWorkersDp] final def executePendingTasks(worker: Worker): Int = {
			doerThreadLocal.set(thisDoer)
			if debugEnabled then assert(taskQueueSize.get > 0)
			var processedTasksCounter: Int = 0
			var taskQueueSizeIsPositive = true
			if applyMemoryFence then VarHandle.loadLoadFence()
			try {
				var task = firstTaskInQueue
				firstTaskInQueue = null
				if task == null then task = taskQueue.poll()
				while task != null do {
					task.run()
					processedTasksCounter += 1
					// the `taskQueueSize` must be decremented after (not before) running the task to avoid that other thread executing `executeSequentially` to enqueue this doer into `queuedDoers` allowing the worst problem to occur: two workers assigned to the same [[DoerImpl]].
					taskQueueSizeIsPositive = taskQueueSize.decrementAndGet() > 0
					task = if taskQueueSizeIsPositive then taskQueue.poll() else null
				}
			} catch {
				case uncaught: Throwable =>
					// Do the taskQueueSize update skipped in the while loop due to the exception.
					taskQueueSizeIsPositive = taskQueueSize.decrementAndGet() > 0
					try {
						// Notify the user about the uncaught exception, protected from exceptions.
						onUnhandledException(thisDoer, uncaught)
					} finally {
						doerThreadLocal.remove()
						// Start the worker in a new thread and exit this method abruptly so that the worker terminates the current one.
						worker.startInANewThread()
					}
					// Rethrow the uncaught exception to terminate the thread abruptly skipping the `runningWorkersLatch` decremental.
					throw uncaught
			} finally {
				if applyMemoryFence then VarHandle.storeStoreFence()
				// if there are pending tasks, enqueue this doer back into the queue of doers with pending tasks.
				if taskQueueSizeIsPositive then {
					if debugEnabled then assert(!queuedDoers.contains(thisDoer))
					enqueueMyself()
				}
				// Note that, for efficiency, the `doerThreadLocal` entry corresponding to this worker thread is not cleared here as expected because it will be overwritten before anything that could access it is executed. It is overwritten either in the next call to `executePendingTasks` if there is a Doer with pending tasks, in `tryToSleep` if no Doer has visible pending tasks, or in the unhandled exception catcher.
			}
			processedTasksCounter
		}

		def diagnose(sb: StringBuilder): StringBuilder = {
			sb.append(f"(tag=$tag, taskQueueSize=${taskQueueSize.get}%3d)")
		}

		override def toString: String = s"${getTypeName[DoerImpl]}(tag=$tag)"
	}

	/** @return `true` if a worker was awakened.
	 * The provided [[DoerImpl]] will be assigned to the awakened worker.
	 * Asumes the provided [[DoerImpl]] is and will not be enqueued in [[queuedDoers]], which ensures it will not be assigned to any other worker simultaneously. */
	protected def wakeUpASleepingWorkerIfAny(stimulator: DoerImpl): Boolean = {
		if debugEnabled then assert(!queuedDoers.contains(stimulator))
		if sleepingWorkersCount.get > 0 then {
			val startingWorkerIndex = stimulator.lastTimeWorkerIndex
			if workers(startingWorkerIndex).wakeUpIfSleeping(stimulator) then true
			else {
				var workerIndex = startingWorkerIndex - 1
				while workerIndex != startingWorkerIndex do {
					if workerIndex <= 0 then workerIndex = workers.length
					workerIndex -= 1
					if workers(workerIndex).wakeUpIfSleeping(stimulator) then return true
				}
				false
			}
		} else false
	}

	protected def wakeUpAWorkerIfAllSleeping(): Unit = {
		if sleepingWorkersCount.get == workers.length then {
			workers(0).wakeUpIfSleeping(null)
		}
	}

	protected open class Worker(val index: Int) extends Runnable { thisWorker =>

		/** The [[Thread]] that executes this worker. */
		private var thread: Thread = threadFactory.newThread(thisWorker)

		/** This field is updated only within a synchronized block on this [[Worker]]'s intrinsic lock. */
		private var keepRunning: Boolean = true

		/** Set to `true` just before calling [[ReentrantLock.wait]] and to `false` just after (the second only if [[keepRunning]] is `true`).
		 * This field is updated within a synchronized block on this [[Worker]]'s intrinsic lock. */
		private var isSleeping: Boolean = false

		/** Usually equal to [[isSleeping]] but may be temporarily true when [[isSleeping]] is false. Not the opposite.
		 * This field is updated exclusively within this worker [[thread]]. */
		@volatile private var potentiallySleeping: Boolean = false

		/** Tracks the number of times the [[tryToSleep]] method was called but returned without putting the worker to sleep.
		 * [[tryToSleep]] avoids sleeping when all other workers are either sleeping or attempting to sleep, leaving this worker as the only one awake, in order to process any pending task that was enqueued after, or was not visible during, the last call to [[areAllOtherWorkersNotCompletelyAsleep]].
		 * This field is updated exclusively within this worker [[thread]]. */
		private var refusedTriesToSleepsCounter: Int = 0

		/**
		 * A [[DoerImpl]] instance that jumps the [[queuedDoers]].
		 * Should not be modified by any thread other than the [[thread]] of this worker unless this worker is sleeping.
		 * Is set by [[wakeUpIfSleeping]] while this worker is sleeping, and by [[run]] after calling [[DoerImpl.executePendingTasks()]] if the task-queue was not completely emptied;
		 * is read by this worker after it is awakened;
		 * and is cleared by this worker after it is awakened. */
		private var queueJumper: DoerImpl | Null = null

		/**
		 * Remember the greatest value that [[refusedTriesToSleepsCounter]] reached before it has been reset because a pending task becomes visible.
		 * Used for diagnostic only to calibrate the limit of [[refusedTriesToSleepsCounter]] at which the worker can safely go to sleep.
		 * This field is updated exclusively within this worker [[thread]]. */
		private var maxTriesToSleepThatWereReset = 0
		/** Tracks the number of times this worker was awakened.
		 * Used for diagnostic only.
		 * This field is updated exclusively within this worker [[thread]]. */
		private var awakensCounter: Int = 0
		/** Set to `true` after exiting the main loop gracefully because [[keepRunning]] is false.
		 * Used for diagnostic only.
		 * This field is updated exclusively within this worker [[thread]]. */
		private var isStopped: Boolean = false
		/** Tracks the number of times this worker completed the main loop.
		 * Used for diagnostic only.
		 * This field is updated exclusively within this worker [[thread]]. */
		private var completedMainLoopsCounter: Int = 0
		private var processedTasksCounter: Int = 0

		inline def start(): Unit = thread.start()

		/** Worker main loop. */
		override def run(): Unit = {
			workerThreadLocal.set(thisWorker)
			while keepRunning do {
				val assignedDoer: DoerImpl | Null =
					if queueJumper != null then queueJumper
					else pollNextDoer()
				queueJumper = null
				if assignedDoer == null then tryToSleep()
				else {
					if refusedTriesToSleepsCounter > maxTriesToSleepThatWereReset then maxTriesToSleepThatWereReset = refusedTriesToSleepsCounter
					refusedTriesToSleepsCounter = 0
					assignedDoer.lastTimeWorkerIndex = this.index
					processedTasksCounter += assignedDoer.executePendingTasks(thisWorker)
					completedMainLoopsCounter += 1
				}
			}
			// Note that the latch counter is decremented only when the worker terminates gracefully.
			runningWorkersLatch.countDown()
			isStopped = true
		}

		/** Starts this [[Worker]] in a new [[Thread]].
		 * Assumes that the current thread is going to terminate. */
		private[CooperativeWorkersDp] def startInANewThread(): Unit = {
			val newThread = threadFactory.newThread(this)
			thread = newThread
			thread.start()
		}

		/** Puts the [[thread]] into sleep unless it is the last [[Worker]]'s thread (of this [[CooperativeWorkersDp]] workers pool) that entered into this method and there is a possibility that there is pending but not yet visible work to do. */
		private def tryToSleep(): Unit = {
			doerThreadLocal.set(null)
			val sleepingCounter = sleepingWorkersCount.incrementAndGet()
			// The purpose of this `if` is to avoid all workers go to sleep when maybe there is work to do.
			// Refuse to sleep if all other workers' threads are also inside this method (tryToSleep) and either:
			// - this worker (the one that incremented the counter to the top) haven't checked that no new task were enqueued during N consecutive main loops since all other workers' threads are inside this method; (this is necessary to avoid all workers go to sleep if a [[DoerImpl]] was enqueued into `enqueuedDoers` by an external thread and the addition is still not visible from the threads of the workers that were awake)
			// - or all other worker's thread haven't reached the point inside this method where the `isSleeping` member is set to true; (this is necessary to avoid the rare situation where all workers' threads are inside this method fated to sleep but none have still entered the synchronized block, which causes calls to `wakeUpASleepingWorkerIfAny` by external threads during the interval to return false and not awake any worker, which causes the tasks enqueued during that interval never be executed unless another task is enqueued after a worker enters said synchronous block, which may not happen)
			// The value of N should be greater than one in order to process any task enqueued between the last check and now. The chosen value of "number of workers" may be more than necessary but extra main loops are not harmful.
			// If I am the last worker entering this section and another worker's thread is leaving the sleeping state
			if sleepingCounter == workers.length && (refusedTriesToSleepsCounter <= workers.length || areAllOtherWorkersNotCompletelyAsleep) then {
				// TODO analyze if a memory barrier is necessary here (or in the main loop) to force the visibility from workers' threads of elements enqueued into `queuedDoers`.
				sleepingWorkersCount.getAndDecrement()
				refusedTriesToSleepsCounter += 1
			} else {
				potentiallySleeping = true
				if sleepingCounter == workers.length && state.get() != State.keepRunning.ordinal && sleepingWorkersCount.get() == workers.length then stopAllWorkers()
				else {
					var isAwakened = false
					thisWorker.synchronized {
						isSleeping = true
						lull(thisWorker, workers.length - sleepingCounter)
						// if a spurious wakeup occur then act as if the worker was awakened with `wakeUpIfSleeping(null)`, unless it was simultaneously stopped (very unlikely to occur if it is possible at all), in which case act as if the worker was stopped while sleeping.
						if keepRunning == isSleeping then isSleeping = !keepRunning
						isAwakened = !isSleeping
					}
					if isAwakened then {
						potentiallySleeping = false
						sleepingWorkersCount.getAndDecrement()
						refusedTriesToSleepsCounter = 0

						awakensCounter += 1
					}
				}
			}
		}

		/** Wakes up this [[Worker]] if it is currently sleeping.
		 * @param stimulator the [[DoerImpl]] to be assigned to this worker upon awakening. Assumes it has not already been assigned to another [[Worker]].
		 * @return `true` if this worker was sleeping and has been awakened, otherwise `false`.
		 */
		def wakeUpIfSleeping(stimulator: DoerImpl | Null): Boolean = {
			if potentiallySleeping then {
				thisWorker.synchronized {
					if isSleeping then {
						queueJumper = stimulator
						isSleeping = false
						thisWorker.notify()
						true
					} else false
				}
			} else false
		}

		private def areAllOtherWorkersNotCompletelyAsleep: Boolean = {
			var workerIndex = workers.length
			var allTheTraversedWorkersAreNotCompletelyAsleep = true
			while workerIndex > 0 && allTheTraversedWorkersAreNotCompletelyAsleep do {
				workerIndex -= 1
				if workerIndex != thisWorker.index then {
					val worker = workers(workerIndex)
					allTheTraversedWorkersAreNotCompletelyAsleep = !worker.potentiallySleeping || !worker.synchronized(worker.isSleeping)
				}
			}
			allTheTraversedWorkersAreNotCompletelyAsleep
		}

		def isAsleep: Boolean = {
			thisWorker.potentiallySleeping && thisWorker.synchronized(thisWorker.isSleeping)
		}

		def stop(): Unit = thisWorker.synchronized {
			keepRunning = false
			thisWorker.notify()
		}

		def diagnose(sb: StringBuilder): StringBuilder = {
			sb.append(f"index=$index%4d, keepRunning=$keepRunning%5b, isStopped=$isStopped%5b, isSleeping=$isSleeping%5b, potentiallySleeping=$potentiallySleeping%5b, maxTriesToSleepThatWereReset=$maxTriesToSleepThatWereReset, awakensCounter=$awakensCounter, processedTaskCounter=$processedTasksCounter, completedMainLoopsCounter=$completedMainLoopsCounter, queueJumper=${queueJumper ne null}%5b")
		}
	}

	/** Puts the specified [[Worker]] to wait until it is awakened.
	 * Called within a synchronization block on the specified [[Worker]]. */
	protected def lull(worker: Worker, numberOfNonSleepingWorkers: Int): Unit = {
		worker.wait() // TODO analyse if the interrupted exception should be handled
	}

	/** Polls the next [[Doer]] from the [[queuedDoers]]. */
	protected def pollNextDoer(): DoerImpl | Null = queuedDoers.poll()

	protected inline def startAllWorkersIfNotAlready(): Unit = {
		if state.compareAndSet(State.notStarted.ordinal, State.keepRunning.ordinal) then {
			workers.foreach(_.start())
		}
	}

	private def stopAllWorkers(): Unit = {
		var workerIndex = workers.length
		while workerIndex > 0 do {
			workerIndex -= 1
			workers(workerIndex).stop()
		}
	}

	override def provide(tag: Tag): DoerFacade = {
		startAllWorkersIfNotAlready()
		new DoerImpl(tag)
	}

	/**
	 * Makes this [[DoerProvider]] to shut down when all the workers are sleeping.
	 * Invocation has no additional effect if already shut down.
	 *
	 * <p>This method does not wait. Use [[awaitTermination]] to do that.
	 *
	 * @throws SecurityException @inheritDoc
	 */
	override def shutdown(): Unit = {
		if state.compareAndSet(State.keepRunning.ordinal, State.shutdownWhenAllWorkersSleep.ordinal) && workers.forall(_.isAsleep) then stopAllWorkers()
	}


	override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = {
		runningWorkersLatch.await(timeout, unit)
	}

	override def diagnose(sb: StringBuilder): StringBuilder = {
		sb.append(getTypeName[CooperativeWorkersDp])
		sb.append('\n')
		sb.append(s"\tstate=${State.fromOrdinal(state.get)}\n")
		sb.append(s"\trunningWorkersLatch=${runningWorkersLatch.getCount}\n")
		sb.append("\tqueuedDoers: ")
		val doersIterator = queuedDoers.iterator()
		while doersIterator.hasNext do {
			val doer = doersIterator.next()
			doer.diagnose(sb)
			sb.append(", ")
		}

		sb.append("\n\tworkers:\n")
		for workerIndex <- workers.indices do {
			sb.append("\t\t")
			workers(workerIndex).diagnose(sb)
			sb.append('\n')
		}
		sb.append("\n")
	}
}
