package readren.sequencer
package providers

import providers.CooperativeFlatPollingSchedulerDp.{NOT_ACTIVATED, ScheduleFacade, SchedulingDoerFacade}

import readren.common.CompileTime.getTypeName
import readren.common.{Maybe, deriveToString}

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Executors, ThreadFactory}


object CooperativeFlatPollingSchedulerDp extends CooperativeSchedulerDpCompanion {
	final class Impl(
		applyMemoryFence: Boolean = true,
		threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
		unhandledExceptionReporter: (Doer, Throwable) => Unit = DefaultDoerUnhandledExceptionReporter(),
		threadFactory: ThreadFactory = Executors.defaultThreadFactory(),
		clock: MonotonicClock = new NanoTimeBasedMilliClock,
		trackSleepTime: Boolean = false
	) extends CooperativeFlatPollingSchedulerDp(applyMemoryFence, threadPoolSize, threadFactory, clock, trackSleepTime) {

		override type Tag = String

		override def tagFromText(text: String): Tag = text

		/** Called when a routine passed to the [[Doer.executeSequentially]] method of a provided [[Doer]] throws an exception. */
		override protected def onUnhandledException(doer: Doer, exception: Throwable): Unit = unhandledExceptionReporter(doer, exception)
	}

	inline def NOT_ACTIVATED: Long= Long.MaxValue
}


/** Adds scheduling features to the [[CooperativeWorkersDp]].
 * The scheduling information is operated by the current thread by means of atomic and synchronization primitives.
 * @param applyMemoryFence Determines whether memory fences are applied to ensure that store operations made by a task happen before load operations performed by successive tasks enqueued to the same [[Doer]].
 * The application of memory fences is optional because no test case has been devised to demonstrate their necessity. Apparently, the ordering constraints are already satisfied by the surrounding code.
 * TODO Define a variant of this class in which each [[Doer]] also has its own [[MinHeapPriorityQueue]], and the shared one ([[priorityQueue]]) contains only the earliest [[MinHeapPriorityQueue.Element]] of each [[Doer]]'s [[MinHeapPriorityQueue]]. This would simplify the cancelAll operation and would minimize blocking.
 */
abstract class CooperativeFlatPollingSchedulerDp(
	applyMemoryFence: Boolean = true,
	threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
	threadFactory: ThreadFactory = Executors.defaultThreadFactory(),
	clock: MonotonicClock = new NanoTimeBasedMilliClock,
	trackSleepTime: Boolean = false
) extends CooperativeWorkersDp(applyMemoryFence, threadPoolSize, threadFactory, trackSleepTime), DoerProvider[SchedulingDoerFacade] { thisProvider =>

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

		/** Adds this [[ScheduleImpl]] instance to the [[priorityQueue]] at the specified time. */
		def program(scheduledTime: MilliTime): Unit = {
			this.scheduledTime = scheduledTime
			val earliestChanged = thisProvider synchronized {
				val oldEarliest = earliestScheduledTime
				priorityQueue.add(this)
				val newEarliest = priorityQueue.peek.scheduledTime
				earliestScheduledTime = newEarliest
				oldEarliest - newEarliest > 0
			}
			if earliestChanged then wakeUpASleepingWorkerIfAny(owner.lastTimeWorkerIndex)
		}

		override def toString: String =
			s"ScheduleImpl(owner=${owner.tag}, ïnitialDelay=$initialDelay, interval=$interval, isFixedRate=$isFixedRate, scheduledTime: $scheduledTime, wasActivated=$wasActivated)"
	}

	/** The priority queue used to memorize the [[ScheduleImpl]] instances and sort them by its next scheduled-time. */
	private val priorityQueue = new MinHeapPriorityQueue[ScheduleImpl]()

	/** Memorizes the earliest scheduled-time of all the active [[ScheduleImpl]] instances if any. Defaults to [[clock.MaxValue]] if none is active ([[priorityQueue]] is empty).
	 * The only purpose of this variable is to improve efficiency by minimizing synchronized accesses to [[priorityQueue.peek]].
	 * Updates should occur within synchronized sections on this provider to avoid race conditions. */
	@volatile private var earliestScheduledTime: MilliTime = clock.MaxValue


	/** Exposes the number of times that [[lull]] was called that didn't put the [[Worker]] to sleep. */
	var skippedLullsCounter: Int = 0

	override def provide(tag: Tag): SchedulingDoerFacade = {
		startAllWorkersIfNotAlready()
		new SchedulingDoerImpl(tag)
	}

	override def currentDoer: Maybe[SchedulingDoerFacade] = super.currentDoer.asInstanceOf[Maybe[SchedulingDoerFacade]]

	private class SchedulingDoerImpl(aTag: Tag) extends DoerImpl(aTag), SchedulingDoerFacade { thisDoer =>

		override type Schedule = ScheduleImpl
		override type Delay = ScheduleImpl

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
			thisProvider synchronized {
				priorityQueue.remove(schedule)
				val next = priorityQueue.peek
				earliestScheduledTime = if next eq null then clock.MaxValue else next.scheduledTime
			}
		}

		/** @inheritdoc
		 * This implementation removes all the scheduled executions corresponding to this [[Doer]] from its schedule.
		 * If called near a scheduled-time from outside this [[Doer]] current thread, some [[Runnable]]s may be executed a single time during this method execution, but not after this method returns.
		 * If called within the [[Thread]] assigned to this [[Doer]], it is ensured that no more execution of scheduled [[Runnable]]s can occur. */
		override def cancelAll(): Unit = {
			activationSerialAtLastCancelAll = lastActivationSerial.get // after this line is executed, no routine of already activated schedules belonging to this SchedulingDoerImpl is executed.
			thisProvider synchronized {
				var index = priorityQueue.size
				while index > 0 do {
					index -= 1
					val schedule = priorityQueue(index)
					if schedule.owner eq thisDoer then {
						schedule.isCanceled = true
						if priorityQueue.remove(schedule) && index < priorityQueue.size then index += 1
					}
				}
				val next = priorityQueue.peek
				earliestScheduledTime = if next eq null then clock.MaxValue else next.scheduledTime
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
			val urgedDoer = pollDoerWithEarliestExpiredTimer(currentMilliTime)
			if urgedDoer ne null then urgedDoer
			else queuedDoers.poll()
		}
	}

	/** Polls the [[SchedulingDoerImpl]] instance that both, its [[SchedulingDoerImpl.runnablesQueue]] was empty, and has the [[ScheduleImpl]] with the earliest elapsed schedule-time. */
	private def pollDoerWithEarliestExpiredTimer(currentTime: MilliTime): DoerImpl | Null = thisProvider synchronized {
		var maybeAwakenedDoer: DoerImpl | Null = null
		while true do {
			val earliestToExpire: ScheduleImpl = priorityQueue.peek
			if earliestToExpire eq null then {
				earliestScheduledTime = clock.MaxValue
				return maybeAwakenedDoer
			} else if earliestToExpire.scheduledTime - currentTime > 0 then {
				earliestScheduledTime = earliestToExpire.scheduledTime
				return maybeAwakenedDoer
			} else if (maybeAwakenedDoer ne null) && (earliestToExpire.owner ne maybeAwakenedDoer) then return maybeAwakenedDoer
			else {
				priorityQueue.finishPoll(earliestToExpire)
				val owner = earliestToExpire.owner
				if owner.enqueueRunnable(earliestToExpire.runnable) then maybeAwakenedDoer = owner
			}
		}
		maybeAwakenedDoer
	}

	override def diagnose(sb: StringBuilder): StringBuilder = {
		sb.append(getTypeName[CooperativeFlatPollingSchedulerDp]).append('\n')
		sb.append("\tskippedLullsCounter = ").append(skippedLullsCounter).append('\n')
		super.diagnose(sb)
	}
}
