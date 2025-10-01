package readren.sequencer
package providers

import providers.CooperativeWorkersWithSyncSchedulerDp.{ScheduleFacade, SchedulingDoerFacade}

import readren.common.CompileTime.getTypeName
import readren.common.{Maybe, deriveToString}

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Executors, ThreadFactory}


object CooperativeWorkersWithSyncSchedulerDp extends CooperativeWorkersDpWithSchedulerCompanion {
	class Impl(
		applyMemoryFence: Boolean = true,
		threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
		failureReporter: (Doer, Throwable) => Unit = DefaultDoerFaultReporter(true),
		unhandledExceptionReporter: (Doer, Throwable) => Unit = DefaultDoerFaultReporter(false),
		threadFactory: ThreadFactory = Executors.defaultThreadFactory(),
		clock: MonotonicClock = new NanoTimeBasedMilliClock
	) extends CooperativeWorkersWithSyncSchedulerDp(applyMemoryFence, threadPoolSize, threadFactory) {

		override type Tag = String

		override def tagFromText(text: String): Tag = text

		/** Called when a routine passed to the [[Doer.executeSequentially]] method of a provided [[Doer]] throws an exception. */
		override protected def onUnhandledException(doer: Doer, exception: Throwable): Unit = unhandledExceptionReporter(doer, exception)

		/** Called when the [[Doer.reportFailure]] method of a provided [[Doer]] is called. */
		override protected def onFailureReported(doer: Doer, failure: Throwable): Unit = failureReporter(doer, failure)
	}
}


abstract class CooperativeWorkersWithSyncSchedulerDp(
	applyMemoryFence: Boolean = true,
	threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
	threadFactory: ThreadFactory = Executors.defaultThreadFactory(),
	clock: MonotonicClock = new NanoTimeBasedMilliClock,
) extends CooperativeWorkersDp, DoerProvider[SchedulingDoerFacade] { thisProvider =>

	/** IMPORTANT: Represents a unique entity where equality and hash code must be based on identity. */
	private class ScheduleImpl(val owner: SchedulingDoerImpl, override val initialDelay: MilliDuration, override val interval: MilliDuration, override val isFixedRate: Boolean) extends MinHeapPriorityQueue.Element, ScheduleFacade {
		val activationSerial: AtomicLong = AtomicLong(Long.MaxValue)

		override def wasActivated: Boolean = activationSerial.get() != Long.MaxValue

		/** The routine whose execution is scheduled by this [[ScheduleImpl]].
		 * Initialized when this instance is activated, which happens when it is passed to the [[scheduleSequentially]] method. */
		var runnable: Runnable | Null = null
		/** Knows if the [[scheduler.enqueuedSchedulesByDoer]] collection contains this instance.
		 * Its purpose is to improve efficiency by avoiding unnecessary manipulations of said collection.
		 * Only accessed within the scheduling thread. */
		var isTriggered = false
		@volatile var isCanceled = false

		/** Executes the routine associated to this [[ScheduleImpl]] */
		inline def execute(): Unit = runnable.run()

		def program(scheduledTime: MilliTime): Unit = {
			this.scheduledTime = scheduledTime
			thisProvider synchronized {
				priorityQueue.add(this)
				earliestScheduledTime = priorityQueue.peek.scheduledTime
			}
		}

		override def toString: String =
			s"ScheduleImpl(owner=${owner.tag}, ïnitialDelay=$initialDelay, $interval=$interval, isFixedRate=$isFixedRate, scheduledTime: $scheduledTime, wasActivated=$wasActivated, isTriggered=$isTriggered)"
	}

	/** The priority queue used to memorize the [[ScheduleImpl]] instances and sort them by its next scheduled-time. */
	private val priorityQueue = new MinHeapPriorityQueue[ScheduleImpl]()

	/** Memorizes the earliest scheduled-time of all the [[ScheduleImpl]] instances.
	 * The only purpose of this variable is to improve efficiency by minimizing calls to [[priorityQueue.peek]].
	 * Updates should occur within synchronized sections to avoid race conditions.
	 * Note that the scheduled-time is initialized to the first time point when the timer is activated, and updated to the next time point every time the routine is executed. */
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

		private val lastActivationSerial: AtomicLong = AtomicLong(Long.MinValue)
		@volatile private var activationSerialAtLastCancelAll = Long.MinValue

		override def newDelaySchedule(delay: MilliDuration): Schedule =
			new ScheduleImpl(thisDoer, delay, 0L, false)

		override def newFixedRateSchedule(initialDelay: MilliDuration, interval: MilliDuration): Schedule =
			new ScheduleImpl(thisDoer, initialDelay, interval, true)

		override def newFixedDelaySchedule(initialDelay: MilliDuration, delay: MilliDuration): Schedule =
			new ScheduleImpl(thisDoer, initialDelay, delay, false)

		override def scheduleSequentially(schedule: Schedule, routine: Schedule => Unit): Unit = {
			val activationTime = clock.currentTimeTimeRoundedUp
			val activationSerial = lastActivationSerial.incrementAndGet()
			if !schedule.activationSerial.compareAndSet(Long.MaxValue, activationSerial) then
				throw new IllegalStateException(s"The ${getTypeName[Schedule]} instance `$schedule` was already used before and can't be used twice.")
			if !schedule.isCanceled then {
				schedule.runnable = new Runnable {
					override def run(): Unit = {
						if !schedule.isCanceled && schedule.activationSerial.get > activationSerialAtLastCancelAll then {
							routine(schedule)
							if schedule.interval > 0 && !schedule.isCanceled && schedule.activationSerial.get > activationSerialAtLastCancelAll then {
								val base = if schedule.isFixedRate then schedule.scheduledTime else clock.currentTimeTimeRoundedUp
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
				priorityQueue.clear()
			}
		}

		/** @inheritdoc
		 * This implementation removes all the scheduled executions corresponding to this [[Doer]] from its schedule.
		 * If called near a scheduled-time from outside this [[Doer]] current thread, some [[Runnable]]s may be executed a single time during this method execution, but not after this method returns.
		 * If called within the [[Thread]] assigned to this [[Doer]], it is ensured that no more execution of scheduled [[Runnable]]s can occur. */
		override def cancelAll(): Unit = {
			activationSerialAtLastCancelAll = lastActivationSerial.get
			thisProvider synchronized {
				var index = priorityQueue.size
				while index > 0 do {
					index -= 1
					val schedule = priorityQueue(index)
					if schedule.owner eq thisDoer then {
						schedule.isCanceled = true
						priorityQueue.remove(schedule)
					}
				}
			}
		}

		/** @inheritdoc
		 * An instance becomes active when is passed to the [[scheduleSequentially]] method.
		 * An instance becomes inactive when it is passed to the [[cancel]] method or when [[cancelAll]] is called. */
		override def wasActivated(schedule: Schedule): Boolean =
			schedule.activationSerial.get != Long.MaxValue

		/** @return true if the [[Schedule]] was cancelled, even if it was not activated.
		 * Note that [[cancelAll]] does not cancel [[Schedule]] instances that weren't activated. */
		override def isCanceled(schedule: ScheduleImpl): Boolean =
			schedule.isCanceled || schedule.activationSerial.get <= activationSerialAtLastCancelAll
	}

	override def lull(worker: Worker): Unit = {
		val durationUntilEarliestScheduledTime = earliestScheduledTime - clock.currentTimeRoundedDown
		if durationUntilEarliestScheduledTime > 0 then worker.wait(durationUntilEarliestScheduledTime)
		else skippedLullsCounter += 1
	}

	override def pollNextDoer(): DoerImpl | Null = {
		val currentMilliTime = clock.currentTimeRoundedDown
		if earliestScheduledTime - currentMilliTime > 0 then queuedDoers.poll()
		else {
			val scheduledTaskDoer = pollDoerWithEarliestExpiredTimer(currentMilliTime)
			if scheduledTaskDoer ne null then scheduledTaskDoer
			else queuedDoers.poll()
		}
	}

	private def pollDoerWithEarliestExpiredTimer(currentTime: MilliTime): DoerImpl | Null = thisProvider synchronized {
		while true do {
			val earliestToExpire: ScheduleImpl = priorityQueue.peek
			if earliestToExpire eq null then {
				earliestScheduledTime = clock.MaxValue
				return null
			} else if earliestToExpire.scheduledTime - currentTime > 0 then {
				earliestScheduledTime = earliestToExpire.scheduledTime
				return null
			} else {
				priorityQueue.finishPoll(earliestToExpire)
				if earliestToExpire.owner.enqueueTask(earliestToExpire.runnable) then {
					val next = priorityQueue.peek
					earliestScheduledTime = if next eq null then clock.MaxValue else next.scheduledTime
					return earliestToExpire.owner
				}
			}
		}
		null
	}

	override def diagnose(sb: StringBuilder): StringBuilder = {
		sb.append(getTypeName[CooperativeWorkersWithSyncSchedulerDp]).append('\n')
		sb.append("\tskippedLullsCounter = ").append(skippedLullsCounter).append('\n')
		super.diagnose(sb)
	}
}
