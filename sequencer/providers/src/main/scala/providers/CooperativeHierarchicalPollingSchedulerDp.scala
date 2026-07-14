package readren.sequencer
package providers

import providers.CooperativeHierarchicalPollingSchedulerDp.{NOT_ACTIVATED, ScheduleFacade, SchedulingDoerFacade}

import readren.common.CompileTime.getTypeName
import readren.common.{Maybe, deriveToString}

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Executors, ThreadFactory}
import scala.annotation.threadUnsafe

object CooperativeHierarchicalPollingSchedulerDp extends CooperativeSchedulerDpCompanion {
	final class Impl(
		applyMemoryFence: Boolean = true,
		threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
		unhandledExceptionReporter: (Doer, Throwable) => Unit = DefaultDoerUnhandledExceptionReporter(),
		threadFactory: ThreadFactory = Executors.defaultThreadFactory(),
		clock: MonotonicClock = new NanoTimeBasedMilliClock
	) extends CooperativeHierarchicalPollingSchedulerDp(applyMemoryFence, threadPoolSize, threadFactory, clock) {

		override type Tag = String

		override def tagFromText(text: String): Tag = text

		override protected def onUnhandledException(doer: Doer, exception: Throwable): Unit = unhandledExceptionReporter(doer, exception)
	}

	inline def NOT_ACTIVATED: Long = Long.MaxValue
}

/** Adds scheduling features to the [[CooperativeWorkersDp]] using a hierarchical, two-level priority queue design.
 *
 * === Architecture ===
 * To minimize global lock contention and optimize cancellation operations, the scheduler uses two levels of queues:
 * 1. '''Per-Doer Queue''': Each [[SchedulingDoerImpl]] maintains its own private [[MinHeapPriorityQueue[ScheduleImpl]]] (`doerPriorityQueue`) containing all scheduled tasks belonging to that doer.
 * 2. '''Global Queue''': A single, shared [[MinHeapPriorityQueue[SchedulingDoerImpl]]] (`priorityQueue`) tracks active doers, sorted by the scheduled time of their earliest pending task.
 *
 * === Lock Contention and Complexity ===
 * - '''Locking''': Operations like scheduling or cancelling primarily synchronize on the individual [[SchedulingDoerImpl]]. Global synchronization on the provider (`thisProvider`) is only acquired when the doer's earliest scheduled task changes.
 * - '''Deadlock Avoidance''': To prevent lock-ordering deadlocks between worker threads polling timers (which start with the provider lock) and client threads scheduling tasks (which start with the doer lock), the worker polling loop temporarily releases the provider lock before acquiring a doer's lock.
 * - '''Complexity''': Operations like [[SchedulingDoerImpl.cancelAll]] do not require scanning all schedules across the entire provider. Instead, they clear the doer's private queue and perform a single $O(\log D)$ heap removal from the global queue, where $D$ is the number of active doers.
 *
 * @param applyMemoryFence Determines whether memory fences are applied to ensure that store operations made by a task happen before load operations performed by successive tasks enqueued to the same [[Doer]].
 */
abstract class CooperativeHierarchicalPollingSchedulerDp(
	applyMemoryFence: Boolean = true,
	threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
	threadFactory: ThreadFactory = Executors.defaultThreadFactory(),
	clock: MonotonicClock = new NanoTimeBasedMilliClock,
) extends CooperativeWorkersDp(applyMemoryFence, threadPoolSize, threadFactory), DoerProvider[SchedulingDoerFacade] { thisProvider =>

	/**
	 * Note that the scheduled-time is initialized to the first time point when the timer is activated, and updated to the next time point every time the routine is executed.
	 * IMPORTANT: Represents a unique entity where equality and hash code must be based on identity. */
	private class ScheduleImpl(val owner: SchedulingDoerImpl, override val initialDelay: MilliDuration, override val interval: MilliDuration, override val isFixedRate: Boolean) extends MinHeapPriorityQueue.Element, ScheduleFacade {
		/** Memorizes the serial number produced when this [[ScheduleImpl]] instance was activated (applied to [[SchedulingDoerImpl.scheduleSequentially]]). */
		val activationSerial: AtomicLong = AtomicLong(NOT_ACTIVATED)

		override def wasActivated: Boolean = activationSerial.get() != NOT_ACTIVATED

		/** The routine whose execution is scheduled by this [[ScheduleImpl]].
		 * Initialized when this instance is activated, which happens when it is passed to the [[scheduleSequentially]] method. */
		var runnable: Runnable | Null = null

		@volatile var isCanceled = false

		/** Executes the routine associated to this [[ScheduleImpl]] */
		inline def execute(): Unit = runnable.run()

		/** Adds this [[ScheduleImpl]] instance to the [[owner]]'s private queue, and if needed, updates the [[owner]]'s position in the global [[headsPriorityQueue]]. */
		def program(scheduledTime: MilliTime): Unit = {
			this.scheduledTime = scheduledTime
			val owerPriorityQueue = owner.doerPriorityQueue
			owner.synchronized {
				if isCanceled then return
				val oldEarliestSchedule = owerPriorityQueue.peek
				owerPriorityQueue.add(this)
				val newEarliest = owerPriorityQueue.peek.scheduledTime
				if (oldEarliestSchedule eq null) || newEarliest != oldEarliestSchedule.scheduledTime then {
					thisProvider.synchronized {
						if owner.heapIndex >= 0 then headsPriorityQueue.remove(owner)
						owner.scheduledTime = newEarliest
						headsPriorityQueue.add(owner)
						val next = headsPriorityQueue.peek
						earliestScheduledTime = if next eq null then clock.MaxValue else next.scheduledTime
					}
					wakeUpASleepingWorkerIfAny(owner.lastTimeWorkerIndex)
				}
			}
		}

		override def toString: String =
			s"ScheduleImpl(owner=${owner.tag}, ïnitialDelay=$initialDelay, interval=$interval, isFixedRate=$isFixedRate, scheduledTime: $scheduledTime, wasActivated=$wasActivated)"
	}

	/** The global priority queue used to track and sort active [[SchedulingDoerImpl]] instances by their next earliest scheduled-time. */
	private val headsPriorityQueue = new MinHeapPriorityQueue[SchedulingDoerImpl]()

	/** Memorizes the earliest scheduled-time across all active doers, or [[clock.MaxValue]] if none are active.
	 * Used to optimize sleep/wake state checks without querying the priority queue structure directly under lock. */
	@volatile private var earliestScheduledTime: MilliTime = clock.MaxValue

	/** Exposes the number of times that [[lull]] was called that didn't put the [[Worker]] to sleep. */
	var skippedLullsCounter: Int = 0

	override def provide(tag: Tag): SchedulingDoerFacade = {
		startAllWorkersIfNotAlready()
		new SchedulingDoerImpl(tag)
	}

	override def currentDoer: Maybe[SchedulingDoerFacade] = super.currentDoer.asInstanceOf[Maybe[SchedulingDoerFacade]]

	private class SchedulingDoerImpl(aTag: Tag) extends DoerImpl(aTag), SchedulingDoerFacade, MinHeapPriorityQueue.Element { thisDoer =>

		override type Schedule = ScheduleImpl
		override type Delay = ScheduleImpl

		@threadUnsafe lazy val doerPriorityQueue = new MinHeapPriorityQueue[ScheduleImpl]()
		private val lastActivationSerial: AtomicLong = AtomicLong(Long.MinValue)
		@volatile private var activationSerialAtLastCancelAll = Long.MinValue

		override def newDelaySchedule(delay: MilliDuration): Delay =
			new ScheduleImpl(thisDoer, delay, 0L, false)

		override def newFixedRateSchedule(initialDelay: MilliDuration, interval: MilliDuration): Schedule =
			new ScheduleImpl(thisDoer, initialDelay, interval, true)

		override def newFixedDelaySchedule(initialDelay: MilliDuration, delay: MilliDuration): Schedule =
			new ScheduleImpl(thisDoer, initialDelay, delay, false)

		override def scheduleSequentially(schedule: Schedule, routine: Schedule => Unit): Unit = {
			val activationTime = clock.currentTimeRoundedUp
			val activationSerial = lastActivationSerial.incrementAndGet()
			if !schedule.activationSerial.compareAndSet(NOT_ACTIVATED, activationSerial) then
				throw new IllegalStateException(s"The ${getTypeName[Schedule]} instance `$schedule` was already used before and can't be used twice.")
			if !schedule.isCanceled then {
				schedule.runnable = new Runnable {
					override def run(): Unit = {
						if !schedule.isCanceled && schedule.activationSerial.get > activationSerialAtLastCancelAll then {
							routine(schedule)
							if schedule.interval > 0 && !schedule.isCanceled && schedule.activationSerial.get > activationSerialAtLastCancelAll then {
								val base = if schedule.isFixedRate then schedule.scheduledTime else clock.currentTimeRoundedUp
								schedule.program(base + schedule.interval)
							}
						}
					}
				}
				schedule.program(activationTime + schedule.initialDelay)
			}
		}

		/** @inheritdoc
		 * This implementation removes the routine executions corresponding to the provided [[Schedule]] from the schedule.
		 * If called near its scheduled-time from outside this [[Doer]]'s current thread, the routine may be executed a single time during this method execution, but not after this method returns.
		 * If called within the [[Thread]] assigned to this [[Doer]], it is ensured that no more execution of the routine can occur. */
		override def cancel(schedule: Schedule): Unit = {
			schedule.isCanceled = true
			thisDoer.synchronized {
				if doerPriorityQueue.remove(schedule) then {
					val earliest = doerPriorityQueue.peek
					thisProvider synchronized {
						if thisDoer.heapIndex >= 0 then {
							headsPriorityQueue.remove(thisDoer)
							if earliest ne null then {
								thisDoer.scheduledTime = earliest.scheduledTime
								headsPriorityQueue.add(thisDoer)
							}
							val next = headsPriorityQueue.peek
							earliestScheduledTime = if next eq null then clock.MaxValue else next.scheduledTime
						}
					}
				}
			}
		}

		/** @inheritdoc
		 * This implementation removes all the scheduled executions corresponding to this [[Doer]] from its schedule.
		 * If called near a scheduled-time from outside this [[Doer]] current thread, some [[Runnable]]s may be executed a single time during this method execution, but not after this method returns.
		 * If called within the [[Thread]] assigned to this [[Doer]], it is ensured that no more execution of scheduled [[Runnable]]s can occur. */
		override def cancelAll(): Unit = {
			activationSerialAtLastCancelAll = lastActivationSerial.get
			thisDoer.synchronized {
				var i = doerPriorityQueue.size
				while i > 0 do {
					i -= 1
					val schedule = doerPriorityQueue(i)
					schedule.isCanceled = true
				}
				doerPriorityQueue.clear()
				thisProvider synchronized {
					if thisDoer.heapIndex >= 0 then {
						headsPriorityQueue.remove(thisDoer)
						val next = headsPriorityQueue.peek
						earliestScheduledTime = if next eq null then clock.MaxValue else next.scheduledTime
					}
				}
			}
		}

		/** @inheritdoc
		 * An instance becomes active when is passed to the [[scheduleSequentially]] method.
		 * An instance becomes inactive when it is passed to the [[cancel]] method or when [[cancelAll]] is called. */
		override def wasActivated(schedule: Schedule): Boolean =
			schedule.activationSerial.get != NOT_ACTIVATED

		/** @return true if the [[Schedule]] was canceled, even if it was not activated.
		 * Note that [[cancelAll]] does not cancel [[Schedule]] instances that weren't activated. */
		override def isCanceled(schedule: ScheduleImpl): Boolean =
			schedule.isCanceled || schedule.activationSerial.get <= activationSerialAtLastCancelAll
	}

	override def lull(worker: Worker): Unit = {
		val est = earliestScheduledTime
		if est == clock.MaxValue then clock.suspend(worker)
		else {
			val durationUntilEarliestScheduledTime = est - clock.currentTimeRoundedDown
			if durationUntilEarliestScheduledTime > 0 then clock.suspend(worker, durationUntilEarliestScheduledTime)
			else skippedLullsCounter += 1
		}
	}

	override def pollNextDoer(worker: Worker): DoerImpl | Null = {
		val currentMilliTime = clock.currentTimeRoundedDown
		if earliestScheduledTime - currentMilliTime > 0 then queuedDoers.poll()
		else {
			val urgedDoer = pollDoerWithEarliestElapsedSchedule(currentMilliTime)
			if urgedDoer ne null then urgedDoer
			else queuedDoers.poll()
		}
	}

	/** Polls the [[SchedulingDoerImpl]] instance that both, its [[SchedulingDoerImpl.runnablesQueue]] was empty, and has the [[ScheduleImpl]] with the earliest elapsed schedule-time. */
	private def pollDoerWithEarliestElapsedSchedule(currentTime: MilliTime): DoerImpl | Null = {
		var maybeAwakenedDoer: SchedulingDoerImpl | Null = null
		while maybeAwakenedDoer eq null do { // outer loop
			val firstDoer = thisProvider.synchronized {
				val firstDoer: SchedulingDoerImpl = headsPriorityQueue.peek
				if firstDoer eq null then {
					earliestScheduledTime = clock.MaxValue
					return null
				} else if firstDoer.scheduledTime - currentTime > 0 then {
					earliestScheduledTime = firstDoer.scheduledTime
					return null
				} else {
					headsPriorityQueue.finishPoll(firstDoer)
					firstDoer
				}
			}
			// After the previous line, the `headsPriorityQueue` may suffer concurrent changes, so we have to consider all possibilities below.
			// Enqueue all the `Runnable`s of the elapsed schedules owned by the `firstDoer`.
			firstDoer.synchronized {
				while { // inner loop
					val firstDoerFirstSchedule = firstDoer.doerPriorityQueue.peek
					// If the `firstDoer` has no schedule (because it was consumed concurrently by other worker), exit the inner loop to continue with the next one.
					if firstDoerFirstSchedule eq null then false // exit inner loop
					// If the `firstDoerFirstSchedule` is not expired (because the expired one was consumed concurrently by other worker), update the `headsPriorityQueue` and exit the inner loop to continue with the next one
					else if firstDoerFirstSchedule.scheduledTime - currentTime > 0 then {
						// Add the `firstDoer` back to the `headsPriorityQueue` with the updated time, unless it was concurrently added back by `ScheduleImpl.program`.
						thisProvider.synchronized {
							// if the `firstDoer` was not concurrently added back to the `headsPriorityQueue`, add it and update the `earliestScheduleTime`.
							if firstDoer.heapIndex < 0 then {
								firstDoer.scheduledTime = firstDoerFirstSchedule.scheduledTime
								headsPriorityQueue.add(firstDoer)
								val nextDoer = headsPriorityQueue.peek
								earliestScheduledTime = if nextDoer eq null then clock.MaxValue else nextDoer.scheduledTime
							}
							// else, if it was concurrently added back with an expired time, reposition it to the correct future time.
							else if firstDoer.scheduledTime != firstDoerFirstSchedule.scheduledTime then {
								headsPriorityQueue.remove(firstDoer)
								firstDoer.scheduledTime = firstDoerFirstSchedule.scheduledTime
								headsPriorityQueue.add(firstDoer)
								val newEarliestDoer = headsPriorityQueue.peek
								earliestScheduledTime = if newEarliestDoer eq null then clock.MaxValue else newEarliestDoer.scheduledTime
							}
						}
						false // exit inner loop
					}
					// If the `firstDoerFirstSchedule` has elapsed, poll it and enqueue its runnable into the `firstDoer`'s particular priority queue.
					else {
						// Poll the elapsed schedule from the `firstDoer` particular priority-queue.
						firstDoer.doerPriorityQueue.finishPoll(firstDoerFirstSchedule)
						// Enqueue the elapsed schedule's runnable into the runnables-queue of the doer that owns it (the `firstDoer`); and, if it is awakened (the queue transitions from empty to non-empty), memorize it in `maybeAwakenedDoer`.
						if firstDoer.enqueueRunnable(firstDoerFirstSchedule.runnable) then {
							// This point is reached at most a single time per each call to this method (`pollDoerWithEarliestElapsedSchedule`).
							maybeAwakenedDoer = firstDoer
						}
						true // do inner loop again to consume other elapsed schedules owned by the same doer.
					}
				} do ()
			}
		}
		maybeAwakenedDoer
	}

	override def diagnose(sb: StringBuilder): StringBuilder = {
		sb.append(getTypeName[CooperativeHierarchicalPollingSchedulerDp]).append('\n')
		sb.append(s"\tskippedLullsCounter = $skippedLullsCounter\n")
		thisProvider synchronized {
			sb.append(s"\tearliestScheduledTime = $earliestScheduledTime\n")
			sb.append(s"\tglobal priorityQueue: size = ${headsPriorityQueue.size}\n")
			var i = 0
			while i < headsPriorityQueue.size do {
				val doer = headsPriorityQueue(i)
				if doer ne null then sb.append(s"\t\t${doer.tag} -> earliestScheduledTime = ${doer.scheduledTime}\n")
				i += 1
			}
		}
		super.diagnose(sb)
	}
}
