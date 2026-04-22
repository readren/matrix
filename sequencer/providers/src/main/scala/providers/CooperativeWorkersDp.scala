package readren.sequencer
package providers

import providers.CooperativeWorkersDp.*
import providers.ShutdownAble

import readren.common.CompileTime.getTypeName
import readren.common.Maybe
import Doer.ExecutionSerial

import java.lang.invoke.VarHandle
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger

object CooperativeWorkersDp {
	type TaskQueue = ConcurrentLinkedQueue[Runnable]

	enum State {
		case notStarted, keepRunning, shutdownWhenAllWorkersSleep, terminated
	}

	/** Facade of the concrete type of the [[Doer]] instances provided by [[CooperativeWorkersDp]].
	 *
	 * Design note: to reduce class-metadata of extending classes, this facade was defined as an abstract class that extends [[AbstractDoer]] instead of a trait that extends [[Doer]].
	 * If this design causes type-hierarchy problems, define it as a trait that extends [[Doer]] instead of [[AbstractDoer]].
	 * */
	abstract class DoerFacade extends AbstractDoer {
		/** Exposes the number of routines that are waiting to be executed sequentially. */
		def numOfPendingTasks: Int
	}


	final class Impl(
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
	/** Knows how many [[Worker]]s are in the sleep zone. Usually equal to the number of workers whose [[Worker.isSleeping]] flag is set, but may be temporarily greater. Never smaller.
	 * Invariant: {{{ workers.count(_.isSleeping) <= sleepZonePopulation.get <= workers.length }}} */
	private val sleepZonePopulation = AtomicInteger(0)

	private val workerThreadLocal: ThreadLocal[Runnable] = new ThreadLocal()
	private val doerThreadLocal: ThreadLocal[DoerFacade | Null] = new ThreadLocal()

	protected def buildWorker(index: Int): Worker = new Worker(index)

	/** @return the [[CooperativeWorkersDp.Worker]] that owns the current [[Thread]], if any.
	 *  Exposed for testing only. */
	inline private[providers] def currentWorker: Runnable | Null = workerThreadLocal.get

	/** @return the [[DoerFacade]] that is currently associated to the current [[Thread]], if any.
	 *
	 * @note Extensions may down-cast the return type provided the conditions mentioned in the [[DoerProvider.provide]]'s note are met. */
	override def currentDoer: Maybe[DoerFacade] = Maybe(doerThreadLocal.get)

	protected open class DoerImpl(override val tag: Tag) extends DoerFacade { thisDoer =>

		override type Tag = thisProvider.Tag

		private val taskQueue: TaskQueue = new ConcurrentLinkedQueue[Runnable]
		private val taskQueueSize: AtomicInteger = new AtomicInteger(0)
		@volatile protected var firstTaskInQueue: Runnable = null
		/** Remembers the index of the worker that executed this doer's tasks the last time. This allows reusing the same worker if available, to take advantage of CPU-core local cache. */
		private[CooperativeWorkersDp] var lastTimeWorkerIndex = 0
		private var executionSequencer: Int = 0 

		override def numOfPendingTasks: Int = taskQueueSize.get

		override def executeSequentially(task: Runnable): Unit = {
			if enqueueTask(task) then {
				// assert(!queuedDoers.contains(thisDoer))
				enqueueMyself()
				wakeUpASleepingWorkerIfAny(lastTimeWorkerIndex)
			}
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

		override def currentExecutionSerial: ExecutionSerial = executionSequencer

		override def currentlyRunningDoer: Maybe[DoerFacade] = Maybe(doerThreadLocal.get)

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
		private[CooperativeWorkersDp] final def executePendingTasks(worker: Worker): Unit = {
			doerThreadLocal.set(thisDoer)
			// assert(taskQueueSize.get > 0)
			var taskQueueSizeIsPositive = true
			if applyMemoryFence then VarHandle.loadLoadFence()
			try {
				var task = firstTaskInQueue
				firstTaskInQueue = null
				if task == null then task = taskQueue.poll()
				while task != null do {
					executionSequencer += 1
					task.run()
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
					// assert(!queuedDoers.contains(thisDoer))
					enqueueMyself()
				}
				// Note that, for efficiency, the `doerThreadLocal` entry corresponding to this worker thread is not cleared here as expected because it will be overwritten before anything that could access it is executed. It is overwritten either in the next call to `executePendingTasks` if there is a Doer with pending tasks, in `tryToSleep` if no Doer has visible pending tasks, or in the unhandled exception catcher.
			}
		}

		def diagnose(sb: StringBuilder): StringBuilder = {
			sb.append(f"(tag=$tag, taskQueueSize=${taskQueueSize.get}%3d)")
		}

		override def toString: String = s"${getTypeName[DoerImpl]}(tag=$tag)"
	}

	/** @return `true` if a [[Worker]] is awakened.
	 * The provided [[DoerImpl]] will be assigned to the awakened worker.
	 * Asumes the provided [[DoerImpl]] is and will not be enqueued in [[queuedDoers]], which ensures it will not be assigned to any other worker simultaneously.
	 * @param startingWorkerIndex the index of the worker to start the search with. */
	protected def wakeUpASleepingWorkerIfAny(startingWorkerIndex: Int): Boolean = {
		if sleepZonePopulation.get > 0 then {
			var workerIndex = startingWorkerIndex
			while true do {
				if workers(workerIndex).wakeUpIfSleeping() then return true
				if workerIndex == 0 then workerIndex = workers.length
				workerIndex -= 1
				if workerIndex == startingWorkerIndex then return false
			}
		}
		false
	}

	protected def wakeUpAWorkerIfAllSleeping(): Unit = {
		if sleepZonePopulation.get == workers.length then {
			workers(0).wakeUpIfSleeping()
		}
	}

	protected open class Worker(val index: Int) extends Runnable { thisWorker =>

		/** The [[Thread]] that executes this worker. */
		private var thread: Thread = threadFactory.newThread(thisWorker)

		/** This field is updated only within a synchronized block on this [[Worker]]'s intrinsic lock. */
		private var keepRunning: Boolean = true

		/** Set to `true` just before calling [[ReentrantLock.wait]] and to `false` just after (the second only if [[keepRunning]] is `true`).
		 * This field is updated within a synchronized block on this [[Worker]]'s intrinsic lock. */
		@volatile private var isSleeping: Boolean = false

		/** Usually equal to [[isSleeping]] but may be temporarily true when [[isSleeping]] is false. Not the opposite.
		 * This field is updated exclusively within this worker [[thread]]. */
		@volatile private var potentiallySleeping: Boolean = false

		/** Tracks the number of times the [[tryToSleep]] method was called but returned without putting the worker to sleep due to a salient [[Doer]].
		 * This field is updated exclusively within this worker [[thread]]. */
		private var salientDoerCounter: Int = 0

		/** Tracks the number of times this worker was awakened.
		 * Used for diagnostic only.
		 * This field is updated exclusively within this worker [[thread]]. */
		private var awakeningCounter: Int = 0

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
			workerThreadLocal.set(thisWorker)
			while keepRunning do {
				var assignedDoer: DoerImpl | Null = pollNextDoer()
				if assignedDoer == null then assignedDoer = tryToSleep()
				if assignedDoer != null then {
					assignedDoer.lastTimeWorkerIndex = this.index
					assignedDoer.executePendingTasks(thisWorker)
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

		/** Puts this [[Worker]] to sleep unless a salient [[DoerImpl]] is seen in the [[queuedDoers]], in which case such [[DoerImpl]] is immediately returned. */
		private def tryToSleep(): DoerImpl = {
			doerThreadLocal.set(null)
			val sleepZonePopulationAtEntry = sleepZonePopulation.incrementAndGet() // Sleep zone begin
			potentiallySleeping = true
			val salientDoer = thisWorker.synchronized {
				val salientDoer = pollNextDoer()
				if salientDoer == null then {
					isSleeping = true
					// Shutdown is a terminal action. We use the exact, O(N) `allOtherWorkersAreSleeping` check to avoid false positives. If a worker mistakenly terminates, thread capacity is permanently lost.
					if state.get() != State.keepRunning.ordinal && allOtherWorkersAreSleeping(index) then keepRunning = false
					else if sleepZonePopulationAtEntry < workers.length then thisWorker.wait()
					else lull(thisWorker)
					isSleeping = !keepRunning
				}
				salientDoer
			}
			if keepRunning then {
				potentiallySleeping = false
				sleepZonePopulation.getAndDecrement() // Sleep zone end
				if salientDoer == null then awakeningCounter += 1 else salientDoerCounter += 1
			} else stopAllWorkers(index)
			salientDoer
		}

		/** Wakes up this [[Worker]] if it is currently sleeping.
		 * @return `true` if this worker was awakened, otherwise `false`. */
		def wakeUpIfSleeping(): Boolean = {
			if potentiallySleeping then {
				thisWorker.synchronized {
					if isSleeping then {
						thisWorker.notify()
						true
					} else false
				}
			} else false
		}

		def isAsleep: Boolean = thisWorker.isSleeping

		def stop(): Unit = thisWorker.synchronized {
			keepRunning = false
			thisWorker.notify()
		}

		def diagnose(sb: StringBuilder): StringBuilder = {
			sb.append(f"index=$index%4d, keepRunning=$keepRunning%5b, isStopped=$isStopped%5b, isSleeping=$isSleeping%5b, potentiallySleeping=$potentiallySleeping%5b, awakeningCounter=$awakeningCounter, salientDoer=$salientDoerCounter, completedMainLoopsCounter=$completedMainLoopsCounter")
		}
	}

	/** Puts the specified [[Worker]] to sleep.
	 * Called when the last [[Worker]] to enter the sleep zone (where workers go after finding the [[queuedDoers]] queue empty) must be put to sleep.
	 * 
	 * Note: The condition used to determine the "last" worker (`sleepZonePopulationAtEntry == workers.length`) is an O(1) approximation. 
	 * If a false positive occurs (i.e., a worker calls `lull` while another is still active), it safely degrades to a timed wait instead of an infinite wait, ensuring scheduled tasks are monitored without incurring the O(N) cost of checking all workers' precise states.
	 *
	 * The default implementation puts the specified [[Worker]] to wait without timeout until it is awakened.
 	 * The intention of this method is to allow extensions to add scheduling support. See [[CooperativeWorkersWithPollingSchedulerDp.determineWaitDurationFor]] for an example.
	 * @param worker the last [[Worker]] to enter the sleep zone.
	 */
	protected def lull(worker: Worker): Unit = 
		worker.wait()
	

	/** Polls the next [[Doer]] from the [[queuedDoers]]. */
	protected def pollNextDoer(): DoerImpl | Null =
		queuedDoers.poll()

	protected inline def startAllWorkersIfNotAlready(): Unit = {
		if state.compareAndSet(State.notStarted.ordinal, State.keepRunning.ordinal) then {
			workers.foreach(_.start())
		}
	}

	private def allOtherWorkersAreSleeping(workerIndex: Int): Boolean = {
		var otherWorkerIndex = workerIndex
		while true do {
			if otherWorkerIndex == 0 then otherWorkerIndex = workers.length
			otherWorkerIndex -= 1
			if otherWorkerIndex == workerIndex then return true
			if !workers(otherWorkerIndex).isAsleep then return false
		}
		true
	}

	private def stopAllWorkers(startingWorkerIndex: Int): Unit = {
		var workerIndex = startingWorkerIndex
		while true do {
			workers(workerIndex).stop()
			if workerIndex == 0 then workerIndex = workers.length
			workerIndex -= 1
			if workerIndex == startingWorkerIndex then return
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
		if state.compareAndSet(State.keepRunning.ordinal, State.shutdownWhenAllWorkersSleep.ordinal) && workers.forall(_.isAsleep) then stopAllWorkers(0)
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
