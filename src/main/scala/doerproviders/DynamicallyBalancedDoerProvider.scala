package readren.matrix
package doerproviders

import collections.ConcurrentList
import doerproviders.SharedQueueDoerProvider.{State, TaskQueue, debugEnabled, doerAssistantThreadLocal}

import readren.taskflow.Doer

import java.util
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, ThreadFactory, TimeUnit}
import scala.annotation.tailrec

object DynamicallyBalancedDoerProvider {
	type TaskQueue = ConcurrentLinkedQueue[Runnable]

	enum State {
		case keepRunning, shutdownWhenAllWorkersSleep, terminated
	}

	inline val debugEnabled = false

	val doerAssistantThreadLocal: ThreadLocal[Doer.Assistant] = new ThreadLocal()
}

class DynamicallyBalancedDoerProvider(owner: AbstractMatrix, threadFactory: ThreadFactory, threadPoolSize: Int, failureReporter: Throwable => Unit) extends Matrix.DoerProvider, ShutdownAble { thisSharedQueueDoerProvider =>
	private val serialSequencer = new AtomicLong(0)

	private val state: AtomicInteger = new AtomicInteger(State.keepRunning.ordinal)

	private val doersAssistants: ConcurrentList[DoerAssistant] = new ConcurrentList

	private val workers: Array[Worker] = Array.tabulate(threadPoolSize)(Worker.apply)

	private val assignedDoerAssistantByWorkerIndex = Array.fill[DoerAssistant | Null](workers.length)(null)

	private val runningWorkersLatch: CountDownLatch = new CountDownLatch(workers.length)
	/** Usually equal to the number of workers that whose [[Worker.isSleeping]] flat is set, but may be temporarily greater. Never smaller.
	 * Invariant: {{{ workers.count(_.isSleeping) <= sleepingWorkersCount.get <= workers.length }}} */
	private val sleepingWorkersCount = AtomicInteger(0)


	{
		workers.foreach(_.start())
	}

	private class DoerAssistant(val id: MatrixDoer.Id) extends ConcurrentList.Node, Doer.Assistant { thisDoerAssistant =>
		override type Self = DoerAssistant
		private val taskQueue: TaskQueue = new ConcurrentLinkedQueue[Runnable]
		private val taskQueueSize: AtomicInteger = new AtomicInteger(0)

		inline def hasPendingTasks: Boolean = taskQueueSize.get > 0

		override def queueForSequentialExecution(task: Runnable): Unit = {
			if taskQueueSize.getAndIncrement() == 0 then {
				taskQueue.offer(task)
				wakeUpASleepingWorkerIfAny(thisDoerAssistant)
			} else {
				taskQueue.offer(task)
			}
		}

		override def reportFailure(cause: Throwable): Unit = failureReporter(cause)

		/** Executes all the pending tasks that are visible from the calling [[Worker.thread]].
		 *
		 * Note: The [[taskQueueSize]] is decremented not immediately after polling a task from the [[taskQueue]] but only after the task is executed.
		 * This prevents that [[queueForSequentialExecution]] to awake another worker before the stimulating [[DoerAssistant]] becomes free to be assigned to said worker.
		 *
		 * If at least one pending task remains unconsumed — typically because it is not yet visible from the [[Worker.thread]] — this [[DoerAssistant]] is enqueued into the [[queuedDoersAssistants]] queue to be assigned to a worker at a later time.
		 */
		@tailrec
		final def executePendingTasks(): Unit = {
			doerAssistantThreadLocal.set(this)

			val task = taskQueue.poll()
			if task != null then {
				task.run()
				taskQueueSize.decrementAndGet()
				executePendingTasks()
			}
		}

		/** Should be called within a synchronized block on the [[assignedDoerAssistantByWorkerIndex]] array's intrinsic lock.
		 * @return `true` if this [[DoerAssistant]] is not assigned to a [[Worker]] */
		def isFree: Boolean = {
			var workerIndex = workers.length
			while workerIndex > 0 do {
				workerIndex -= 1
				if assignedDoerAssistantByWorkerIndex(workerIndex) eq this then return false
			}
			true

		}

		def diagnose(sb: StringBuilder): StringBuilder = {
			sb.append(f"(id=$id, taskQueueSize=${taskQueueSize.get}%3d)")
		}
	}

	/** @return `true` if a worker was awakened.
	 * The provided [[DoerAssistant]] will be assigned to the awakened worker if no other [[Worker]] was assigned to it before. */
	private def wakeUpASleepingWorkerIfAny(stimulator: DoerAssistant): Boolean = {
		if sleepingWorkersCount.get > 0 then {
			var workerIndex = workers.length
			while workerIndex > 0 do {
				workerIndex -= 1
				if workers(workerIndex).wakeUpIfSleeping(stimulator) then return true
			}
			false
		} else false
	}

	private class Worker(val index: Int) extends Runnable { thisWorker =>

		/** The [[Thread]] that executes this worker. */
		private var thread: Thread = threadFactory.newThread(this)

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

		/** Used by [[findALoadedAndFreeDoerAssistant()]] method to iterate through the [[doersAssistants]] list and remember where to continue in the following calls. */
		private val circularIterator = doersAssistants.circularIterator

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

		inline def start(): Unit = thread.start()

		/** Worker main loop. */
		override def run(): Unit = {
			while keepRunning do {
				val assignedDoerAssistant: DoerAssistant | Null =
					assignedDoerAssistantByWorkerIndex.synchronized {
						val unassignedDoerAssistant =
							if queueJumper != null && queueJumper.isFree then queueJumper
							else findALoadedAndFreeDoerAssistant()
						assignedDoerAssistantByWorkerIndex(index) = unassignedDoerAssistant
						unassignedDoerAssistant
					}
				queueJumper = null
				if assignedDoerAssistant == null then tryToSleep()
				else {
					if refusedTriesToSleepsCounter > maxTriesToSleepThatWereReset then maxTriesToSleepThatWereReset = refusedTriesToSleepsCounter
					refusedTriesToSleepsCounter = 0
					try {
						assignedDoerAssistant.executePendingTasks()
						completedMainLoopsCounter += 1
					}
					catch {
						case e: Throwable =>
							if thread.getUncaughtExceptionHandler == null && Thread.getDefaultUncaughtExceptionHandler == null then failureReporter(e)
							// Let the current thread to terminate abruptly, create a new one, and start it with the same Runnable (this worker).
							thisWorker.synchronized {
								thread = threadFactory.newThread(this)
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

		/** Starting after the last [[DoerInstance]] returned by the previous call to this method, looks in the [[doersAssistants]] list for a [[DoerAssistant]] that is not assigned to a worker and whose task-queue is not empty.
		 * Should be called within a synchronized block on the [[DynamicallyBalancedDoerProvider.assignedDoerAssistantByWorkerIndex]] array's intrinsic lock. */
		private def findALoadedAndFreeDoerAssistant(): DoerAssistant | Null = {
			val firstCandidate = circularIterator.next()
			if firstCandidate != null then {
				var candidate = firstCandidate

				// This is an example of why removing do-while from scala was a bad idea.
				while true do {
					if candidate.isFree && candidate.hasPendingTasks then return candidate
					candidate = circularIterator.next()
					if candidate eq firstCandidate then return null
				}
			}
			null
		}

		/** Should be called immediately before the main-loop's exit condition evaluation. */
		private def tryToSleep(): Unit = {
			val sleepingCounter = sleepingWorkersCount.incrementAndGet()
			// refuse to sleep if this worker is the only worker awake and haven't checked that no new task were enqueued during N consecutive main loops since all other workers went to sleep. The value of N should be greater than zero in order to process any task enqueued between the last check and now. The chosen value of "number of workers" may be more than the necessary but are free.
			if sleepingCounter == workers.length && (refusedTriesToSleepsCounter <= workers.length || areAllOtherWorkersNotCompletelyAsleep) then {
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
						if debugEnabled then assert(keepRunning != isSleeping)
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
			sb.append(f"index=$index%4d, keepRunning=$keepRunning%5b, isStopped=$isStopped%5b, isSleeping=$isSleeping%5b, potentiallySleeping=${potentiallySleeping}%5b, maxTriesToSleepThatWereReset=$maxTriesToSleepThatWereReset, awakensCounter=$awakensCounter, completedMainLoopsCounter=$completedMainLoopsCounter, queueJumper=${queueJumper != null}%5b")
		}
	}

	private def stopAllWorkers(): Unit = {
		var workerIndex = workers.length
		while workerIndex > 0 do {
			workerIndex -= 1
			workers(workerIndex).stop()
		}
	}

	override def provide(): MatrixDoer = {
		val serial = serialSequencer.getAndIncrement()
		val newDoerAssistant = new DoerAssistant(serial)
		doersAssistants.add(newDoerAssistant)
		new MatrixDoer(serial, newDoerAssistant, owner)
	}

	/**
	 * Makes this [[Matrix.DoerProvider]] to shut down when all the workers are sleeping.
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
		sb.append("AutoBalancedDoerProvider:\n")
		sb.append(s"\tstate=${State.fromOrdinal(state.get)}\n")
		sb.append(s"\tsleepingWorkersCount=${sleepingWorkersCount.get}\n")
		sb.append("\tdoers' assistants with pending tasks: ")
		var doerAssistant = doersAssistants.nextOf(null)
		while doerAssistant != null do {
			if doerAssistant.hasPendingTasks then {
				doerAssistant.diagnose(sb)
				sb.append(", ")
			}
			doerAssistant = doersAssistants.nextOf(doerAssistant)
		}

		sb.append("\n\tworkers:\n")
		for workerIndex <- workers.indices do {
			sb.append("\t\t")
			workers(workerIndex).diagnose(sb)
			sb.append(s"\t\tassigned to\t")
			val assignedDoerAssistant = assignedDoerAssistantByWorkerIndex(workerIndex)
			if assignedDoerAssistant != null then assignedDoerAssistant.diagnose(sb) else sb.append("nothing")
			sb.append('\n')
		}
		sb.append("\n")
	}
}
