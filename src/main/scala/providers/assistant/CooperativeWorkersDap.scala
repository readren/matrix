package readren.matrix
package providers.assistant

import core.{Matrix, MatrixDoer}
import providers.ShutdownAble
import providers.assistant.CooperativeWorkersDap.*
import providers.doer.AssistantBasedDoerProvider.DoerAssistantProvider

import readren.taskflow.Doer

import java.lang.invoke.VarHandle
import java.util
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger

object CooperativeWorkersDap {
	type TaskQueue = ConcurrentLinkedQueue[Runnable]

	enum State {
		case keepRunning, shutdownWhenAllWorkersSleep, terminated
	}

	inline val debugEnabled = false

	private val workerIndexThreadLocal: ThreadLocal[Int] = ThreadLocal.withInitial(() => -1)
	private val doerAssistantThreadLocal: ThreadLocal[Doer.Assistant] = new ThreadLocal()

	def currentWorkerIndex: Int = workerIndexThreadLocal.get
	// val UNSAFE: Unsafe = Unsafe.getUnsafe

	trait DoerAssistant extends Doer.Assistant {
		def id: MatrixDoer.Id
		/** Exposes the number of [[Runnable]]s that are in the task-queue waiting to be executed sequentially. */
		def numOfPendingTasks: Int
	}
}

/** A [[Doer.Assistant]] provider in which the task queue of the provided [[Doer.Assistant]]s is processed by any worker of the pool, the first that gets free.
 * How it works:
 * 		- every call to [[provide]] returns a new [[Doer.Assistant]] instance.
 *		- When an assistant (provided by this provider) starts having pending tasks it is enqueued in a queue.
 *		- When a thread-worker of the pool gets free it polls an assistant from said queue and processes all the pending tasks that the thread-worker can sse before continuing with the next assistant from the queue.
 *		- After processing all the visible task, if there is a non-visible one, the assistant is enqueue back.	
 *		- If the queue is empty the thread-worker goes to sleep.
 *		- When an assistant is enqueued (because its tasks-queue transitions from empty to nonempty) a single sleeping thread-worker is awakened if there is some.
 * Effective for all purposes. Shines when the processor demand of long-living doers is very variant.   		
 * @param applyMemoryFence Determines whether memory fences are applied to ensure that store operations made by a task happen before load operations performed by successive tasks enqueued to the same [[Doer.Assistant]]. 
 * The application of memory fences is optional because no test case has been devised to demonstrate their necessity. Apparently, the ordering constraints are already satisfied by the surrounding code.
 */
class CooperativeWorkersDap(
	applyMemoryFence: Boolean = true,
	threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
	failureReporter: Throwable => Unit = _.printStackTrace(),
	threadFactory: ThreadFactory = Executors.defaultThreadFactory()
) extends DoerAssistantProvider[DoerAssistant], ShutdownAble { thisSharedQueueDoerAssistantProvider =>

	private val state: AtomicInteger = new AtomicInteger(State.keepRunning.ordinal)

	/** Queue of [[DoerAssistantImpl]] with pending tasks that are waiting to be assigned to a [[Worker]] in order to process them.
	 *
	 * Invariant: this queue contains no duplicate elements due to the [[DoerAssistantImpl.queueForSequentialExecution]] logic.
	 * TODO try using my own implementation of concurrent queue that avoids dynamic memory allocation. */
	private val queuedDoersAssistants = new ConcurrentLinkedQueue[DoerAssistantImpl]()

	private val workers: Array[Worker] = Array.tabulate(threadPoolSize)(Worker.apply)

	private val runningWorkersLatch: CountDownLatch = new CountDownLatch(workers.length)
	/** Usually equal to the number of workers whose [[Worker.isSleeping]] flag is set, but may be temporarily greater. Never smaller.
	 * Invariant: {{{ workers.count(_.isSleeping) <= sleepingWorkersCount.get <= workers.length }}} */
	private val sleepingWorkersCount = AtomicInteger(0)

	/** Tracks the worker that was awakened during the last call to [[wakeUpASleepingWorkerIfAny]], with the sole purpose of distributing the load more evenly across physical processors.
	 * This variable is exclusive to the [[wakeUpASleepingWorkerIfAny]] method and is not intended to be accessed from anywhere else. */
	private var lastAwakenedWorkerIndex = 0

	{
		workers.foreach(_.start())
	}

	protected class DoerAssistantImpl(override val id: MatrixDoer.Id) extends DoerAssistant { thisDoerAssistant =>
		private val taskQueue: TaskQueue = new ConcurrentLinkedQueue[Runnable]
		private val taskQueueSize: AtomicInteger = new AtomicInteger(0)
		@volatile private var firstTaskInQueue: Runnable = null

		override def numOfPendingTasks: Int = taskQueueSize.get

		override def executeSequentially(task: Runnable): Unit = {
			if taskQueueSize.getAndIncrement() == 0 then {
				firstTaskInQueue = task
				if !wakeUpASleepingWorkerIfAny(thisDoerAssistant) then {
					if debugEnabled then assert(!queuedDoersAssistants.contains(thisDoerAssistant))
					queuedDoersAssistants.offer(thisDoerAssistant)
				}
			} else {
				taskQueue.offer(task)
			}
		}

		override def current: Doer.Assistant = doerAssistantThreadLocal.get()

		override def reportFailure(cause: Throwable): Unit = failureReporter(cause)


		/** Executes all the pending tasks that are visible from the calling [[Worker.thread]].
		 *
		 * Note: The [[taskQueueSize]] is decremented not immediately after polling a task from the [[taskQueue]] but only after the task is executed.
		 * This ensures that another thread invoking `queuedForSequentialExecution` does not call [[wakeUpASleepingWorkerIfAny]] passing this [[DoerAssistantImpl]] or enqueue this [[DoerAssistantImpl]] into the [[queuedDoersAssistants]] queue,
		 * which would violate the constraint that prevents two workers from being assigned to the same [[DoerAssistantImpl]] instance simultaneously.
		 *
		 * If at least one pending task remains unconsumed — typically because it is not yet visible from the [[Worker.thread]] — this [[DoerAssistantImpl]] is enqueued into the [[queuedDoersAssistants]] queue to be assigned to a worker at a later time.
		 */
		final def executePendingTasks(): Int = {
			doerAssistantThreadLocal.set(thisDoerAssistant)
			if debugEnabled then assert(taskQueueSize.get > 0)
			var processedTasksCounter: Int = 0
			var taskQueueSizeIsPositive = true
			var aDecrementIsPending = false
			if applyMemoryFence then VarHandle.loadLoadFence()
			try {
				var task = firstTaskInQueue
				firstTaskInQueue = null
				if task	== null then task = taskQueue.poll()
				while task != null && taskQueueSizeIsPositive do {
					aDecrementIsPending = true
					task.run()
					processedTasksCounter += 1
					aDecrementIsPending = false
					// the `taskQueueSize` must be decremented after (not before) running the task to avoid that other thread executing `queuedForSequentialExecution` to enqueue this SchedulingAssistantImpl into `queuedDoersAssistants` allowing the worst problem to occur: two workers assigned to the same SchedulingAssistantImpl.
					taskQueueSizeIsPositive = taskQueueSize.decrementAndGet() > 0
					if taskQueueSizeIsPositive then task = taskQueue.poll()
				}
			} finally {
				if applyMemoryFence then VarHandle.storeStoreFence()
				if aDecrementIsPending then taskQueueSizeIsPositive = taskQueueSize.decrementAndGet() > 0
				if taskQueueSizeIsPositive then {
					if debugEnabled then assert(!queuedDoersAssistants.contains(thisDoerAssistant))
					queuedDoersAssistants.offer(thisDoerAssistant)
				}
			}
			processedTasksCounter
		}


		def diagnose(sb: StringBuilder): StringBuilder = {
			sb.append(f"(id=$id, taskQueueSize=${taskQueueSize.get}%3d)")
		}

		override def toString: String = s"${utils.CompileTime.getTypeName[DoerAssistantImpl]}(id=$id)"
	}

	/** @return `true` if a worker was awakened.
	 * The provided [[DoerAssistantImpl]] will be assigned to the awakened worker.
	 * Asumes the provided [[DoerAssistantImpl]] is and will not be enqueued in [[queuedDoersAssistants]], which ensures it will not be assigned to any other worker simultaneously. */
	private def wakeUpASleepingWorkerIfAny(stimulator: DoerAssistantImpl): Boolean = {
		if debugEnabled then assert(!queuedDoersAssistants.contains(stimulator))
		if sleepingWorkersCount.get > 0 then {
			var workerIndex = lastAwakenedWorkerIndex - 1
			while workerIndex != lastAwakenedWorkerIndex do {
				if workerIndex <= 0 then workerIndex = workers.length
				workerIndex -= 1
				if workers(workerIndex).wakeUpIfSleeping(stimulator) then return {
					lastAwakenedWorkerIndex = workerIndex
					true
				}
			}
			false
		} else false
	}

	private class Worker(val index: Int) extends Runnable { thisWorker =>

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
		 * [[tryToSleep]] avoids sleeping when all other workers are either sleeping or attempting to sleep, leaving this worker as the only one awake, in order to process any pending task that was enqueued after, or was not visible during, the last call to [[findALoadedAndFreeDoerAssistant]].
		 * This field is updated exclusively within this worker [[thread]]. */
		private var refusedTriesToSleepsCounter: Int = 0

		/**
		 * A [[DoerAssistantImpl]] instance that jumps the queue established by the [[circularIterator]] that determines the order in which the [[DoerAssistantImpl]] instances are assigned to this worker.
		 * Should not be modified by any thread other than the [[thread]] of this worker unless this worker is sleeping.
		 * Is set by [[wakeUpIfSleeping]] while this worker is sleeping, and by [[run]] after calling [[DoerAssistantImpl.executePendingTasks()]] if the task-queue was not completely emptied;
		 * is read by this worker after it is awakened;
		 * and is cleared by this worker after it is awakened. */
		private var queueJumper: DoerAssistantImpl | Null = null

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
			workerIndexThreadLocal.set(index)
			while keepRunning do {
				val assignedDoerAssistant: DoerAssistantImpl | Null =
					if queueJumper != null then queueJumper
					else queuedDoersAssistants.poll()
				queueJumper = null
				if assignedDoerAssistant == null then tryToSleep()
				else {
					if refusedTriesToSleepsCounter > maxTriesToSleepThatWereReset then maxTriesToSleepThatWereReset = refusedTriesToSleepsCounter
					refusedTriesToSleepsCounter = 0
					try {
						processedTasksCounter += assignedDoerAssistant.executePendingTasks()
						completedMainLoopsCounter += 1
					}
					catch { // TODO analyze if clarity would suffer too much if [[SchedulingAssistantImpl.executePendingTasks]] accepted a partial function with this catch removing the necessity of this try-catch.
						case e: Throwable =>
							if thread.getUncaughtExceptionHandler == null && Thread.getDefaultUncaughtExceptionHandler == null then failureReporter(e)
							// Let the current thread to terminate abruptly, create a new one, and start it with the same Runnable (this worker).
							thisWorker.synchronized {
								thread = threadFactory.newThread(this)
								// Memorize the assigned SchedulingAssistantImpl such that the new thread be assigned to the same [[SchedulingAssistantImpl]]. It will continue with the task after the one that threw the exception.
								queueJumper = assignedDoerAssistant
							}
							thread.start()
							// Terminate the thread abruptly to skip the `runningWorkersLatch` decremental.
							throw e
					}
				}
			}
			// Note that only decrements when the worker terminates gracefully.
			runningWorkersLatch.countDown()
			isStopped = true
		}

		/** Should be called immediately before the main-loop's exit condition evaluation. */
		private def tryToSleep(): Unit = {
			val sleepingCounter = sleepingWorkersCount.incrementAndGet()
			// The purpose of this `if` is to avoid all workers go to sleep when maybe there is work to do.
			// Refuse to sleep if all other workers' threads are also inside this method (tryToSleep) and either:
			// - this worker (the one that incremented the counter to the top) haven't checked that no new task were enqueued during N consecutive main loops since all other workers' threads are inside this method; (this is necessary to avoid all workers go to sleep if a DoerAssistants was enqueued into `enqueuedDoerAssistants` by an external thread and is still not visible from the threads of the workers that were awake)
			// - or all other worker's thread haven't reached the point inside this method where the `isSleeping` member is set to true; (this is necessary to avoid the rare situation where all workers' threads are inside this method fated to sleep but none have still entered the synchronized block, which causes calls to `wakeUpASleepingWorkerIfAny` by external threads during the interval to return false and not awake any worker, which causes the tasks enqueued during that interval never be executed unless another task is enqueued after a worker enters said synchronous block, which may not happen)
			// The value of N should be greater than one in order to process any task enqueued between the last check and now. The chosen value of "number of workers" may be more than necessary but extra main loops are not harmful.
			// If other worker's thread is leaving the sleeping state
			if sleepingCounter == workers.length && (refusedTriesToSleepsCounter <= workers.length || areAllOtherWorkersNotCompletelyAsleep) then {
				// TODO analyze if a memory barrier is necessary here (or in the main loop) to force the the visibility from workers' threads of elements enqueued into `queuedDoersAssistants`.
				sleepingWorkersCount.getAndDecrement()
				refusedTriesToSleepsCounter += 1
			} else {
				potentiallySleeping = true
				if sleepingCounter == workers.length && state.get() != State.keepRunning.ordinal && sleepingWorkersCount.get() == workers.length then stopAllWorkers()
				else {
					var isAwakened = false
					thisWorker.synchronized {
						isSleeping = true
						thisWorker.wait() // TODO analyse if the interrupted exception should be handled
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
		 * @param stimulator the [[DoerAssistantImpl]] to be assigned to this worker upon awakening,
		 *                   provided it has not already been assigned to another [[Worker]].
		 * @return `true` if this worker was sleeping and has been awakened, otherwise `false`.
		 */
		def wakeUpIfSleeping(stimulator: DoerAssistantImpl): Boolean = {
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
			sb.append(f"index=$index%4d, keepRunning=$keepRunning%5b, isStopped=$isStopped%5b, isSleeping=$isSleeping%5b, potentiallySleeping=$potentiallySleeping%5b, maxTriesToSleepThatWereReset=$maxTriesToSleepThatWereReset, awakensCounter=$awakensCounter, processedTaskCounter=$processedTasksCounter, completedMainLoopsCounter=$completedMainLoopsCounter, queueJumper=${queueJumper != null}%5b")
		}
	}

	private def stopAllWorkers(): Unit = {
		var workerIndex = workers.length
		while workerIndex > 0 do {
			workerIndex -= 1
			workers(workerIndex).stop()
		}
	}

	override def provide(serial: MatrixDoer.Id): DoerAssistant = {
		new DoerAssistantImpl(serial)
	}

	/**
	 * Makes this [[Matrix.DoerAssistantProvider]] to shut down when all the workers are sleeping.
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
		sb.append(utils.CompileTime.getTypeName[CooperativeWorkersDap])
		sb.append('\n')
		sb.append(s"\tstate=${State.fromOrdinal(state.get)}\n")
		sb.append(s"\trunningWorkersLatch=${runningWorkersLatch.getCount}\n")
		sb.append("\tqueuedDoersAssistants: ")
		val doersAssistantsIterator = queuedDoersAssistants.iterator()
		while doersAssistantsIterator.hasNext do {
			val doerAssistant = doersAssistantsIterator.next()
			doerAssistant.diagnose(sb)
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
