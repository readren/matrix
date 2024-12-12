package readren.matrix
package doerproviders

import doerproviders.AutoBalancedDoerProvider.{State, TaskQueue}

import readren.taskflow.Doer

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, ThreadFactory, TimeUnit}
import scala.annotation.tailrec
import scala.util.Try

object AutoBalancedDoerProvider {
	type TaskQueue = ConcurrentLinkedQueue[Runnable]

	enum State {
		case keepRunning, shutdownWhenAllWorkersSleep, terminated
	}
}

class AutoBalancedDoerProvider(owner: AbstractMatrix, threadFactory: ThreadFactory, threadPoolSize: Int, failureReporter: Throwable => Unit) extends Matrix.DoerProvider, ShutdownAble {
	private val serialSequencer = new AtomicLong(0)
	private val state: AtomicInteger = new AtomicInteger(State.keepRunning.ordinal)

	@deprecated("no es usado por ahora")
	private val doersAssistants = new ConcurrentLinkedQueue[DoerAssistant]()
	/** Contains the instances of [[DoerAssistant]] that might be unassigned (not assigned to a worker) and loaded (having pending tasks).
	 *
	 * <s>Invariant: this queue contains no duplicate elements due to the [[DoerAssistant.queueForSequentialExecution]] logic.
	 * Invariant: the size of this queue is less than or equal to the size of [[doersAssistants]] (the number of [[DoerAssistant]] instances created by [[pick]]).</s>
	 * TODO consider implementing an array based concurrent queue to avoid the overhead of dynamic allocation. In order to be efficient it should use separate locks for offer and poll. */
	private val doersAssistantsThatMightBeLoadedAndUnassigned = new ConcurrentLinkedQueue[DoerAssistant]()

	private val workers: Array[Worker] = Array.tabulate(threadPoolSize)(Worker.apply)
	private val assignedDoerAssistantByWorkerIndex = Array.fill[DoerAssistant | Null](workers.length)(null)
	private val runningWorkersLatch: CountDownLatch = new CountDownLatch(workers.length)

	{
		workers.foreach(_.start())
	}

	private class DoerAssistant extends Doer.Assistant { thisDoerAssistant =>
		private val taskQueue: TaskQueue = new ConcurrentLinkedQueue[Runnable]
		private val taskQueueSize: AtomicInteger = new AtomicInteger(0)

		override def queueForSequentialExecution(task: Runnable): Unit = {
			if taskQueueSize.getAndIncrement() == 0 then {
				taskQueue.offer(task)
				if !wakeUpASleepingWorkerIfAny(thisDoerAssistant) then doersAssistantsThatMightBeLoadedAndUnassigned.offer(thisDoerAssistant)
			} else {
				taskQueue.offer(task)
			}
		}

		override def reportFailure(cause: Throwable): Unit = failureReporter(cause)

		/** @return `true` if the taskQueue was not completely emptied */
		@tailrec
		final def executePendingTasks(): Boolean = {
			val task = taskQueue.poll
			if task != null then {
				taskQueueSize.decrementAndGet()
				task.run()
				executePendingTasks()
			} else taskQueueSize.get() > 0
		}

		def diagnose(sb: StringBuilder): StringBuilder = {
			sb.append(s"taskQueueSize=${taskQueueSize.get}\n")
		}
	}


	/** @return `true` if a worker was awakened.
	 * The provided [[DoerAssistant]] will be assigned to the awakened worker if no other [[Worker]] was assigned to it before. */
	private def wakeUpASleepingWorkerIfAny(stimulator: DoerAssistant): Boolean = {
		var workerIndex = workers.length
		while workerIndex > 0 do {
			workerIndex -= 1
			if workers(workerIndex).wakeUpIfSleeping(stimulator) then return true
		}
		false
	}

	private class Worker(val index: Int) extends Runnable { thisWorker =>

		private var thread: Thread = threadFactory.newThread(this)
		/** Usually equal to [[isSleeping]] but may be temporarily true when [[isSleeping]] is false. Not the opposite. */
		private val potentiallySleeping: AtomicBoolean = new AtomicBoolean(false)
		/** Should be accessed within the lock of this worker. */
		private var isSleeping: Boolean = false

		/** Should be accessed by this worker [[thread]] only. */
		private var keepRunning: Boolean = true

		/** Should be set by other threads withing this worker lock.
		 * Is set by another worker while this worker is sleeping just before waking this worker up; is read by this worker after it is awakened; and is cleared by this worker after it is awakened. */
		private var wakeUpStimulator: DoerAssistant | Null = null

		inline def start(): Unit = thread.start()

		override def run(): Unit = {
			while keepRunning do {
				val assignedDoerAssistant: DoerAssistant | Null =
					assignedDoerAssistantByWorkerIndex.synchronized {
						val unassignedDoerAssistant =
							if wakeUpStimulator != null && assignedDoerAssistantByWorkerIndex.doesNotContain(wakeUpStimulator) then wakeUpStimulator
							else findALoadedAndFreeDoerAssistant()
						assignedDoerAssistantByWorkerIndex(index) = unassignedDoerAssistant
						unassignedDoerAssistant
					}
				wakeUpStimulator = null
				if assignedDoerAssistant == null then sleep()
				else {
					try if assignedDoerAssistant.executePendingTasks() then wakeUpStimulator = assignedDoerAssistant
					catch {
						case e: Throwable =>
							if thread.getUncaughtExceptionHandler == null && Thread.getDefaultUncaughtExceptionHandler == null then failureReporter(e)
							// Let the current thread to terminate abruptly, create a new one, and start it with the same Runnable (this worker).
							thisWorker.synchronized {
								thread = threadFactory.newThread(this)
								// Memorize the assigned Doer.Assistant such that the new thread will continue with the task after the one that threw the exception.
								wakeUpStimulator = assignedDoerAssistant
							}
							thread.start()
							throw e
					}
				}
			}
			runningWorkersLatch.countDown()
		}

		@tailrec
		private def findALoadedAndFreeDoerAssistant(): DoerAssistant | Null = {
			val candidate = doersAssistantsThatMightBeLoadedAndUnassigned.poll()
			if candidate == null then null
			else if assignedDoerAssistantByWorkerIndex.doesNotContain(candidate) then candidate
			else findALoadedAndFreeDoerAssistant()
		}

		private def sleep(): Unit = thisWorker.synchronized {
			potentiallySleeping.set(true)
			isSleeping = true
			if state.get() != State.keepRunning.ordinal then terminateIfAllWorkersAreSleeping()
			thisWorker.wait() // TODO analyse if the interrupted exception should be handled
		}

		/** Awakens this [[Worker]] if it was sleeping.
		 * @return `true` if this worker was sleeping, in which case it is awakened.
		 * The provided [[DoerAssistant]] will be assigned to this worker if no other [[Worker]] was assigned to it before. */
		def wakeUpIfSleeping(stimulator: DoerAssistant): Boolean = {
			if potentiallySleeping.get then thisWorker.synchronized {
				if isSleeping then {
					isSleeping = false
					potentiallySleeping.set(false)
					wakeUpStimulator = stimulator
					thisWorker.notify()
					true
				} else false
			} else false
		}

		def stop(): Unit = thisWorker.synchronized {
			keepRunning = false
			thisWorker.notify()
		}

		def diagnose(sb: StringBuilder): StringBuilder = {
			sb.append(f"index=$index%4d, keepRunning=$keepRunning%5b, isSleeping=$isSleeping%5b, potentiallySleeping=${potentiallySleeping.get}%5b, wakeupStimulator=${wakeUpStimulator!=null}%5b\n")
		}
	}

	override def pick(): MatrixDoer = {
		val serial = serialSequencer.getAndIncrement()
		val doerAssistant = new DoerAssistant
		doersAssistants.offer(doerAssistant)
		new MatrixDoer(serial, doerAssistant, owner)
	}

	private def terminateIfAllWorkersAreSleeping(): Unit = assignedDoerAssistantByWorkerIndex.synchronized {
		var workerIndex = workers.length
		while workerIndex > 0 do {
			workerIndex -= 1
			if assignedDoerAssistantByWorkerIndex(workerIndex) != null then return
		}
		// reaches here if all workers are sleeping
		while workerIndex < workers.length do {
			workers(workerIndex).stop()
			workerIndex += 1
		}
	}

	/**
	 * Makes this [[Matrix.DoerProvider]] to shutdown when all the workers are sleeping.
	 * Invocation has no additional effect if already shut down.
	 *
	 * <p>This method does not wait. Use [[awaitTermination]] to do that.
	 *
	 * @throws SecurityException @inheritDoc
	 */
	override def shutdown(): Unit =
		state.compareAndSet(State.keepRunning.ordinal, State.shutdownWhenAllWorkersSleep.ordinal)

	override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = {
		runningWorkersLatch.await(timeout, unit)
	}

	override def diagnose(sb: StringBuilder): StringBuilder = {
		sb.append("AutoBalancedDoerProvider:\n")
		sb.append(s"state=${State.fromOrdinal(state.get)}\n")
		sb.append("doersAssistantsThatMightBeLoadedAndUnassigned:\n")
		var doersAssistantsIterator = doersAssistantsThatMightBeLoadedAndUnassigned.iterator()
		while doersAssistantsIterator.hasNext do {
			val doerAssistant = doersAssistantsIterator.next()
			sb.append('\t')
			doerAssistant.diagnose(sb)
		}

		sb.append("doersAssistants:\n")
		doersAssistantsIterator = doersAssistants.iterator() // TODO uncomment
		while doersAssistantsIterator.hasNext do {
			val doerAssistant = doersAssistantsIterator.next()
			sb.append('\t')
			doerAssistant.diagnose(sb)
		}

		sb.append("workers:\n")
		for worker <- workers do {
			sb.append('\t')
			worker.diagnose(sb)
		}
		sb.append("\n")
	}


	extension (assignments: assignedDoerAssistantByWorkerIndex.type) {
		private def doesNotContain(doerAssistant: DoerAssistant): Boolean = {
			var workerIndex = workers.length
			while workerIndex > 0 do {
				workerIndex -= 1
				if assignments(workerIndex) eq doerAssistant then return false
			}
			true
		}
	}
}
