package readren.matrix
package providers.assistant

import core.MatrixDoer
import providers.ShutdownAble
import providers.assistant.SchedulingAssistantProvider.*
import providers.doer.AssistantBasedDoerProvider.DoerAssistantProvider

import readren.taskflow.SchedulingExtension.NanoDuration
import readren.taskflow.{Doer, SchedulingExtension}

import java.lang.invoke.VarHandle
import java.util
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import scala.annotation.tailrec
import scala.collection.mutable

object SchedulingAssistantProvider {
	type TaskQueue = ConcurrentLinkedQueue[Runnable]
	/** A nano time based on the [[System.nanoTime]] method. */
	type NanoTime = Long
	/** A duration in nanoseconds. */
	type Duration = Long

	inline val INITIAL_DELAYED_TASK_QUEUE_CAPACITY = 16

	enum State {
		case keepRunning, shutdownWhenAllWorkersSleep, terminated
	}

	inline val debugEnabled = false

	private val workerIndexThreadLocal: ThreadLocal[Int] = ThreadLocal.withInitial(() => -1)
	private val doerAssistantThreadLocal: ThreadLocal[Doer.Assistant] = new ThreadLocal()

	def currentWorkerIndex: Int = workerIndexThreadLocal.get
	// val UNSAFE: Unsafe = Unsafe.getUnsafe
}

/** A dynamically balanced [[Doer.Assistant]] provider.
 * How it works:
 * 		- every call to [[provide]] returns a new [[Doer.Assistant]] instance.
 *		- When an assistant (provided by this provider) starts having pending tasks it is enqueued in a queue.
 *		- When a thread-worker of the pool gets free it polls an assistant from said queue and processes all the pending messages that the thread-worker can sse before continuing with the next assistant from the queue.
 *		- After processing all the visible task, if there is a non-visible one, the assistant is enqueue back.	
 *		- If the queue is empty the thread-worker goes to sleep.
 *		- When an assistant is enqueued (because its tasks-queue transitions from empty to nonempty) a single sleeping thread-worker is awakened if there are ones.
 * Effective for all purposes. Shines when the processor demand of long-living doers is very variant.   		
 * @param applyMemoryFence Determines whether memory fences are applied to ensure that store operations made by a task happen before load operations performed by successive tasks enqueued to the same [[Doer.Assistant]]. 
 * The application of memory fences is optional because no test case has been devised to demonstrate their necessity. Apparently, the ordering constraints are already satisfied by the surrounding code.
 */
class SchedulingAssistantProvider(
	applyMemoryFence: Boolean = true,
	threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
	failureReporter: Throwable => Unit = _.printStackTrace(),
	threadFactory: ThreadFactory = Executors.defaultThreadFactory()
) extends DoerAssistantProvider, ShutdownAble { thisSharedQueueDoerProvider =>

	override type ProvidedAssistant = DoerAssistant

	private val state: AtomicInteger = new AtomicInteger(State.keepRunning.ordinal)

	/** Queue of [[DoerAssistant]] with pending tasks that are waiting to be assigned to a [[Worker]] in order to process them.
	 *
	 * Invariant: this queue contains no duplicate elements due to the [[DoerAssistant.queueForSequentialExecution]] logic.
	 * TODO try using my own implementation of concurrent queue that avoids dynamic memory allocation. */
	private val queuedDoersAssistants = new ConcurrentLinkedQueue[DoerAssistant]()

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

	class DoerAssistant(val id: MatrixDoer.Id) extends Doer.Assistant, SchedulingExtension.Assistant { thisDoerAssistant =>
		private val taskQueue: TaskQueue = new ConcurrentLinkedQueue[Runnable]
		private val taskQueueSize: AtomicInteger = new AtomicInteger(0)
		@volatile private var firstTaskInQueue: Runnable = null

		inline def hasPendingTasks: Boolean = taskQueueSize.get > 0

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
		 * This ensures that another thread invoking `queuedForSequentialExecution` does not call [[wakeUpASleepingWorkerIfAny]] passing this [[DoerAssistant]] or enqueue this [[DoerAssistant]] into the [[queuedDoersAssistants]] queue,
		 * which would violate the constraint that prevents two workers from being assigned to the same [[DoerAssistant]] instance simultaneously.
		 *
		 * If at least one pending task remains unconsumed — typically because it is not yet visible from the [[Worker.thread]] — this [[DoerAssistant]] is enqueued into the [[queuedDoersAssistants]] queue to be assigned to a worker at a later time.
		 */
		private[SchedulingAssistantProvider] final def executePendingTasks(): Int = {
			doerAssistantThreadLocal.set(thisDoerAssistant)
			if debugEnabled then assert(taskQueueSize.get > 0)
			var processedTasksCounter: Int = 0
			var taskQueueSizeIsPositive = true
			var aDecrementIsPending = false
			if applyMemoryFence then VarHandle.loadLoadFence()
			try {
				var task = firstTaskInQueue
				firstTaskInQueue = null
				if task == null then task = taskQueue.poll()
				while task != null && taskQueueSizeIsPositive do {
					aDecrementIsPending = true
					task.run()
					processedTasksCounter += 1
					aDecrementIsPending = false
					// the `taskQueueSize` must be decremented after (not before) running the task to avoid that other thread executing `queuedForSequentialExecution` to enqueue this DoerAssistant into `queuedDoersAssistants` allowing the worst problem to occur: two workers assigned to the same DoerAssistant.
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

		override type Schedule = ScheduleImpl

		override def newDelaySchedule(delay: NanoDuration): Schedule =
			new ScheduleImpl(delay, 0L, false)

		override def newFixedRateSchedule(initialDelay: NanoDuration, interval: NanoDuration): Schedule =
			new ScheduleImpl(initialDelay, interval, true)

		override def newFixedDelaySchedule(initialDelay: NanoDuration, delay: NanoDuration): Schedule =
			new ScheduleImpl(initialDelay, delay, false)

		override def scheduleSequentially(schedule: Schedule, originalRunnable: Runnable): Unit = {
			val currentTime = System.nanoTime()
			assert(!schedule.isActive)
			schedule.isActive = true

			object fixedDelayWrapper extends Runnable {
				override def run(): Unit = {
					if schedule.isActive then {
						originalRunnable.run()
						// TODO analyze if the following lines must be in a `finally` block whose `try`'s body is `originalRunnable.run()`
						scheduler.schedule(schedule, System.nanoTime() + schedule.interval)
					}
				}
			}

			object fixedRateWrapper extends Runnable {
				override def run(): Unit = {
					@tailrec
					def loop(currentTime: NanoTime): Unit = {
						if schedule.isActive then {
							schedule.startingTime = currentTime
							schedule.numOfSkippedExecutions = (currentTime - schedule.scheduledTime) / schedule.interval
							originalRunnable.run()
							// TODO analyze if the following lines must be in a `finally` block whose `try`'s body is `originalRunnable.run()`
							val nextTime = schedule.scheduledTime + schedule.interval * (1L + schedule.numOfSkippedExecutions)
							val newCurrentTime = System.nanoTime()
							if nextTime <= newCurrentTime then {
								schedule.scheduledTime = nextTime
								loop(newCurrentTime)
							} else scheduler.schedule(schedule, nextTime)
						}
					}

					loop(System.nanoTime())
				}
			}

			schedule.runnable =
				if schedule.interval > 0 then {
					if schedule.isFixedRate then fixedRateWrapper
					else fixedDelayWrapper
				} else originalRunnable
			scheduler.schedule(schedule, currentTime + schedule.initialDelay)
		}

		override def cancel(schedule: Schedule): Unit = scheduler.cancel(schedule)

		override def cancelAll(): Unit = scheduler.cancelAllBelongingTo(thisDoerAssistant)

		/** An instance becomes active when is passed to the [[scheduleSequentially]] method.
		 * An instances becomes inactive when it is passed to the [[cancel]] method or when [[cancelAll]] is called. */
		override def isActive(schedule: Schedule): Boolean = schedule.isActive


		/** IMPORTANT: Represents a unique entity where equality and hash code must be based on identity. */
		class ScheduleImpl(val initialDelay: NanoTime, val interval: Duration, val isFixedRate: Boolean) {
			/** The [[Runnable]] that this [[TimersExtension.Assistant.Schedule]] schedules. */
			private[SchedulingAssistantProvider] var runnable: Runnable | Null = null
			/** Exposes the time the [[Runnable]] is expected to be run.
			 * Updated after the [[Runnable]] execution is completed. */
			var scheduledTime: NanoTime = 0L
			/** The index of this instance in the array-based min-heap. */
			private[SchedulingAssistantProvider] var heapIndex: Int = -1
			/** Exposes the number of executions of the [[Runnable]] that were skipped before the current one due to processing power saturation or negative `initialDelay`.
			 * It is calculated based on the scheduled interval, and the difference between the actual [[startingTime]] and the scheduled time:
			 * {{{ (actualTime - scheduledTime) / interval }}}
			 * Updated before the [[Runnable]] is run.
			 * The value of this variable is used after the [[runnable]]'s execution completes to calculate the [[scheduledTime]]; therefore, the [[runnable]] may modify it to affect the resulting [[scheduledTime]] and therefore when it's next execution will be.
			 * Intended to be accessed only within the thread that is currently running the [[Runnable]] that is scheduled by this instance. */
			var numOfSkippedExecutions: Long = 0
			/** Exposes the [[System.nanoTime]] when the current execution started.
			 * The [[numOfSkippedExecutions]] is calculated based on this time.
			 * Updated before the [[Runnable]] is run.
			 * Intended to be accessed only within the thread that is currently running the [[Runnable]] that is scheduled by this instance. */
			var startingTime: NanoTime = 0L
			/** An instance becomes enabled when the [[scheduledTime]] is reached, and it's [[runnable]] is enqueued in [[thisDoerAssistant.taskQueue]].
			 * An instance becomes disabled after the [[runnable]] execution finishes and the */
			private[SchedulingAssistantProvider] var isEnabled = false
			/** An instance becomes active when is passed to the [[thisDoerAssistant.scheduleSequentially]] method.
			 * An instances becomes inactive when it is passed to the [[thisDoerAssistant.cancel]] method or when [[thisDoerAssistant.cancelAll]] is called.
			 *
			 * Implementation note: This var may be replaced with {{{ def isActive = !isEnabled && heapIndex < 0}}} but that would require both [[isEnabled]] and [[heapIndex]] to be @volatile. */
			@volatile private[SchedulingAssistantProvider] var isActive = false

			inline def owner: thisDoerAssistant.type = thisDoerAssistant
		}
	}

	/** @return `true` if a worker was awakened.
	 * The provided [[DoerAssistant]] will be assigned to the awakened worker.
	 * Asumes the provided [[DoerAssistant]] is and will not be enqueued in [[queuedDoersAssistants]], which ensures it will not be assigned to any other worker simultaneously. */
	private def wakeUpASleepingWorkerIfAny(stimulator: DoerAssistant): Boolean = {
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
		 * A [[DoerAssistant]] instance that jumps the queue established by the [[circularIterator]] that determines the order in which the [[DoerAssistant]] instances are assigned to this worker.
		 * Should not be modified by any thread other than the [[thread]] of this worker unless this worker is sleeping.
		 * Is set by [[wakeUpIfSleeping]] while this worker is sleeping, and by [[run]] after calling [[DoerAssistant.executePendingTasks()]] if the task-queue was not completely emptied;
		 * is read by this worker after it is awakened;
		 * and is cleared by this worker after it is awakened. */
		private var queueJumper: DoerAssistant | Null = null

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
				val assignedDoerAssistant: DoerAssistant | Null =
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
					catch { // TODO analyze if clarity would suffer too much if [[DoerAssistant.executePendingTasks]] accepted a partial function with this catch removing the necessity of this try-catch.
						case e: Throwable =>
							if thread.getUncaughtExceptionHandler == null && Thread.getDefaultUncaughtExceptionHandler == null then failureReporter(e)
							// Let the current thread to terminate abruptly, create a new one, and start it with the same Runnable (this worker).
							thisWorker.synchronized {
								thread = threadFactory.newThread(this)
								// Memorize the assigned DoerAssistant such that the new thread be assigned to the same [[DoerAssistant]]. It will continue with the task after the one that threw the exception.
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
		 * @param stimulator the [[DoerAssistant]] to be assigned to this worker upon awakening,
		 *                   provided it has not already been assigned to another [[Worker]].
		 * @return `true` if this worker was sleeping and has been awakened, otherwise `false`.
		 */
		def wakeUpIfSleeping(stimulator: DoerAssistant): Boolean = {
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

	override def provide(serial: MatrixDoer.Id): ProvidedAssistant = {
		new DoerAssistant(serial)
	}

	private object scheduler extends Runnable {
		private val commandsQueue = new ConcurrentLinkedQueue[Runnable]()
		private var heap: Array[DoerAssistant#ScheduleImpl | Null] = Array.fill(INITIAL_DELAYED_TASK_QUEUE_CAPACITY)(null)
		private var size: Int = 0
		private var enabledSchedulesByAssistant: util.HashMap[DoerAssistant, mutable.HashSet[DoerAssistant#ScheduleImpl]] = new util.HashMap()

		private var isRunning = true
		private val lock = new ReentrantLock()
		/**
		 * Condition signalled when a command is enqueued into [[commandsQueue]].
		 */
		private val commandPending = lock.newCondition()

		private val timeWaitingThread: Thread = threadFactory.newThread(this)
		timeWaitingThread.start()

		def schedule(schedule: DoerAssistant#ScheduleImpl, scheduleTime: NanoTime): Unit = {
			signal { () =>
				if schedule.isEnabled then {
					schedule.isEnabled = false
					enabledSchedulesByAssistant.computeIfPresent(schedule.owner, (_, enabledSchedules) => enabledSchedules.subtractOne(schedule))
				}
				schedule.scheduledTime = scheduleTime
				enqueue(schedule)
			}
		}

		def cancel(schedule: DoerAssistant#ScheduleImpl): Unit = {
			signal { () =>
				schedule.isActive = false
				if schedule.isEnabled then {
					schedule.isEnabled = false
					enabledSchedulesByAssistant.computeIfPresent(schedule.owner, (_, enabledSchedules) => enabledSchedules.subtractOne(schedule))
				} else remove(schedule)
			}
		}

		def cancelAllBelongingTo(assistant: DoerAssistant): Unit = {
			signal { () =>
				var index = size
				while index > 0 do {
					index -= 1
					val schedule = heap(index)
					if schedule.owner eq assistant then {
						schedule.isActive = false
						remove(schedule)
					}
				}

				enabledSchedulesByAssistant.remove(assistant).foreach { schedule =>
					schedule.isActive = false
					schedule.isEnabled = false
				}
			}
		}

		def stop(): Unit = {
			signal(() => isRunning = false)
		}

		private def signal(command: Runnable): Unit = {
			commandsQueue.offer(command)
			lock.lock()
			try commandPending.signal()
			finally lock.unlock()
		}

		override def run(): Unit = {
			while isRunning do {
				var command: Runnable | Null = commandsQueue.poll()
				while command != null do {
					command.run()
					command = commandsQueue.poll()
				}

				var earlierSchedule = peek
				var currentNanoTime = System.nanoTime()
				while earlierSchedule != null && earlierSchedule.scheduledTime <= currentNanoTime do {
					finishPoll(earlierSchedule)
					earlierSchedule.isEnabled = true
					enabledSchedulesByAssistant.merge(earlierSchedule.owner, mutable.HashSet(earlierSchedule), (_, enabledSchedules) => enabledSchedules.addOne(earlierSchedule))
					earlierSchedule.owner.executeSequentially(earlierSchedule.runnable)
					currentNanoTime = System.nanoTime()
					earlierSchedule = peek
				}
				lock.lock()
				try {
					if earlierSchedule == null then commandPending.await()
					else {
						val delay = earlierSchedule.scheduledTime - currentNanoTime
						earlierSchedule = null // do not keep unnecessary references while waiting to avoid unnecessary memory retention
						commandPending.awaitNanos(delay)
					}
				}
				finally lock.unlock()
			}
			commandsQueue.clear() // do not keep unnecessary references while waiting to avoid unnecessary memory retention
			for i <- 0 until size do heap(i).isActive = false
			heap = null // do not keep unnecessary references while waiting to avoid unnecessary memory retention
			enabledSchedulesByAssistant.forEach { (_, enabledSchedules) =>
				enabledSchedules.foreach { schedule =>
					schedule.isActive = false
					schedule.isEnabled = false
				}
			}
			enabledSchedulesByAssistant = null // do not keep unnecessary references while waiting to avoid unnecessary memory retention
		}

		private inline def peek: DoerAssistant#ScheduleImpl | Null = heap(0)

		/** Adds the provided element to this min-heap based priority queue. */
		private def enqueue(element: DoerAssistant#ScheduleImpl): Unit = {
			val holeIndex = size
			if holeIndex >= heap.length then grow()
			size = holeIndex + 1
			if holeIndex == 0 then {
				heap(0) = element
				element.heapIndex = 0
			}
			else siftUp(holeIndex, element)
		}

		/**
		 * Polls the element that was peeked: replaces first element with last and sifts it down.
		 * Assumes the provided element is the same as the returned by [[peek]].
		 * @param peekedElement the [[ScheduleImpl]] to remove and return.
		 */
		private def finishPoll(peekedElement: DoerAssistant#ScheduleImpl): DoerAssistant#ScheduleImpl = {
			size -= 1;
			val s = size
			val replacement = heap(s)
			heap(s) = null
			if s != 0 then siftDown(0, replacement)
			peekedElement.heapIndex = -1
			peekedElement
		}

		/** Removes the provided element from this queue.
		 * @return true if the element was removed; false if it is not contained by this queue. */
		private def remove(element: DoerAssistant#ScheduleImpl): Boolean = {
			val elemIndex = indexOf(element)
			if elemIndex < 0 then return false
			element.heapIndex = -1
			size -= 1
			val s = size
			val replacement = heap(s)
			heap(s) = null
			if s != elemIndex then {
				siftDown(elemIndex, replacement)
				if heap(elemIndex) eq replacement then siftUp(elemIndex, replacement)
			}
			true
		}

		private inline def indexOf(task: DoerAssistant#ScheduleImpl): Int = task.heapIndex

		/**
		 * Replaces the element at position `holeIndex` of the heap-based array with the `providedElement` and rearranges it and its parents as necessary to ensure that all parents are less than or equal to their children.
		 * Note that for the entire heap to satisfy the min-heap property, the `providedElement` must be less than or equal to the children of `holeIndex`.
		 * Sifts element added at bottom up to its heap-ordered spot.
		 */
		private def siftUp(holeIndex: Int, providedElement: DoerAssistant#ScheduleImpl): Unit = {
			var gapIndex = holeIndex
			var zero = 0
			while (gapIndex > zero) {
				val parentIndex = (gapIndex - 1) >>> 1
				val parent = heap(parentIndex)
				if providedElement.scheduledTime >= parent.scheduledTime then zero = Int.MaxValue
				else {
					heap(gapIndex) = parent
					parent.heapIndex = gapIndex
					gapIndex = parentIndex
				}
			}
			heap(gapIndex) = providedElement
			providedElement.heapIndex = gapIndex
		}

		/**
		 * Replaces the element that is currently at position `holeIndex` of the heap-based array with the `providedElement` and rearranges the elements in the subtree rooted at `holeIndex` such that the subtree conform to the min-heap property.
		 * Sifts element added at top down to its heap-ordered spot.
		 */
		private def siftDown(holeIndex: Int, providedElement: DoerAssistant#ScheduleImpl): Unit = {
			var gapIndex = holeIndex
			var half = size >>> 1
			while gapIndex < half do {
				var childIndex = (gapIndex << 1) + 1
				var child = heap(childIndex)
				val rightIndex = childIndex + 1
				if rightIndex < size && child.scheduledTime > heap(rightIndex).scheduledTime then {
					childIndex = rightIndex
					child = heap(childIndex)
				}
				if providedElement.scheduledTime <= child.scheduledTime then half = 0
				else {
					heap(gapIndex) = child
					child.heapIndex = gapIndex
					gapIndex = childIndex
				}
			}
			heap(gapIndex) = providedElement
			providedElement.heapIndex = gapIndex
		}

		/**
		 * Resizes the heap array.
		 */
		private def grow(): Unit = {
			val oldCapacity = heap.length
			var newCapacity = oldCapacity + (oldCapacity >> 1) // grow 50%

			if newCapacity < 0 then newCapacity = Integer.MAX_VALUE // overflow

			heap = util.Arrays.copyOf(heap, newCapacity)
		}
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
		scheduler.stop()
		if state.compareAndSet(State.keepRunning.ordinal, State.shutdownWhenAllWorkersSleep.ordinal) && workers.forall(_.isAsleep) then stopAllWorkers()
	}


	override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = {
		runningWorkersLatch.await(timeout, unit)
	}

	override def diagnose(sb: StringBuilder): StringBuilder = {
		sb.append(thisSharedQueueDoerProvider.getClass.getSimpleName)
		sb.append('\n')
		sb.append(s"\tstate=${State.fromOrdinal(state.get)}\n")
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
