package readren.sequencer
package providers

import providers.CooperativeLocalPollingSchedulerDp.{NOT_ACTIVATED, ScheduleFacade, SchedulingDoerFacade}

import readren.common.CompileTime.getTypeName
import readren.common.Maybe

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Executors, ThreadFactory}

object CooperativeLocalPollingSchedulerDp extends CooperativeSchedulerDpCompanion {
	final class Impl(
		applyMemoryFence: Boolean = true,
		threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
		unhandledExceptionReporter: (Doer, Throwable) => Unit = DefaultDoerUnhandledExceptionReporter(),
		threadFactory: ThreadFactory = Executors.defaultThreadFactory(),
		clock: MonotonicClock = new NanoTimeBasedMilliClock,
		trackSleepTime: Boolean = false
	) extends CooperativeLocalPollingSchedulerDp(applyMemoryFence, threadPoolSize, threadFactory, clock, trackSleepTime) {

		override type Tag = String

		override def tagFromText(text: String): Tag = text

		override protected def onUnhandledException(doer: Doer, exception: Throwable): Unit = unhandledExceptionReporter(doer, exception)
	}

	inline def NOT_ACTIVATED: Long = Long.MaxValue
}

/** Adds scheduling features to the [[CooperativeWorkersDp]] by partitioning scheduling heaps per worker thread
 * and using thread-local priorities queue mutations to eliminate cross-thread locking overhead. */
abstract class CooperativeLocalPollingSchedulerDp(
	applyMemoryFence: Boolean = true,
	threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
	threadFactory: ThreadFactory = Executors.defaultThreadFactory(),
	clock: MonotonicClock = new NanoTimeBasedMilliClock,
	trackSleepTime: Boolean = false
) extends CooperativeWorkersDp(applyMemoryFence, threadPoolSize, threadFactory, trackSleepTime), DoerProvider[SchedulingDoerFacade] { thisProvider =>

	/** Schedule representation managed by the thread-local priority queues of this provider. */
	private class ScheduleImpl(val owner: SchedulingDoerImpl, override val initialDelay: MilliDuration, override val interval: MilliDuration, override val isFixedRate: Boolean) extends MinHeapPriorityQueue.Element, ScheduleFacade {
		/** Monotonically increasing sequence number indicating when this schedule was activated. \
		 * Holds [[CooperativeLocalPollingSchedulerDp.NOT_ACTIVATED]] if the schedule has not been activated. */
		val activationSerial: AtomicLong = AtomicLong(NOT_ACTIVATED)

		override def wasActivated: Boolean = activationSerial.get() != NOT_ACTIVATED

		var runnable: Runnable | Null = null

		@volatile var isCanceled = false

		inline def execute(): Unit = runnable.run()

		/** Enqueues this schedule to the thread-local priority queue of the calling worker thread. \
		 * Updates the worker's earliest scheduled execution time. */
		def program(scheduledTime: MilliTime): Unit = {
			this.scheduledTime = scheduledTime
			val w = currentWorker
			if w != null then {
				val worker = w.asInstanceOf[Worker]
				val workerIndex = worker.index
				val queue = workerPriorityQueues(workerIndex)
				queue.add(this)
				earliestScheduledTimes(workerIndex) = queue.peek.scheduledTime
			}
		}

		override def toString: String =
			s"ScheduleImpl(owner=${owner.tag}, initialDelay=$initialDelay, interval=$interval, isFixedRate=$isFixedRate, scheduledTime: $scheduledTime, wasActivated=$wasActivated)"
	}

	/** Priority queues containing pending schedules, partitioned by worker thread index. \
	 * Mutation operations require no lock synchronization since each index is strictly mutated \
	 * only by its corresponding worker thread. */
	private val workerPriorityQueues = Array.tabulate(threadPoolSize)(_ => new MinHeapPriorityQueue[ScheduleImpl]())

	/** Earliest scheduled execution times for each worker thread, used to calculate timed sleep durations. */
	private val earliestScheduledTimes = Array.fill(threadPoolSize)(clock.MaxValue)

	override def provide(tag: Tag): SchedulingDoerFacade = {
		startAllWorkersIfNotAlready()
		new SchedulingDoerImpl(tag)
	}

	override def currentDoer: Maybe[SchedulingDoerFacade] = super.currentDoer.asInstanceOf[Maybe[SchedulingDoerFacade]]

	private class SchedulingDoerImpl(aTag: Tag) extends DoerImpl(aTag), SchedulingDoerFacade { thisDoer =>

		override type Schedule = ScheduleImpl
		override type Delay = ScheduleImpl

		/** Global generator for unique, monotonically increasing schedule activation serial numbers. */
		private val lastActivationSerial: AtomicLong = AtomicLong(Long.MinValue)

		/** The serial number threshold at the time of the last [[cancelAll]] invocation. \
		 * Any schedule with an activation serial less than or equal to this threshold is treated as canceled. */
		@volatile private var activationSerialAtLastCancelAll = Long.MinValue

		override def newDelaySchedule(delay: MilliDuration): Delay =
			new ScheduleImpl(thisDoer, delay, 0L, false)

		override def newFixedRateSchedule(initialDelay: MilliDuration, interval: MilliDuration): Schedule =
			new ScheduleImpl(thisDoer, initialDelay, interval, true)

		override def newFixedDelaySchedule(initialDelay: MilliDuration, delay: MilliDuration): Schedule =
			new ScheduleImpl(thisDoer, initialDelay, delay, false)

		override def scheduleSequentially(schedule: Schedule, routine: Schedule => Unit): Unit = {
			val activationSerial = lastActivationSerial.incrementAndGet()
			if !schedule.activationSerial.compareAndSet(NOT_ACTIVATED, activationSerial) then
				throw new IllegalStateException(s"The ${getTypeName[Schedule]} instance `$schedule` was already used before and can't be used twice.")

			if currentWorker ne null then {
				scheduleSequentiallyInternal(schedule, routine, activationSerial)
			} else {
				thisDoer.executeSequentially { () =>
					scheduleSequentiallyInternal(schedule, routine, activationSerial)
				}
			}
		}

		/** Configures and enqueues the schedule on the worker thread. \
		 * Bypasses the duplicate activation check to prevent re-activation errors when delegated. */
		private def scheduleSequentiallyInternal(schedule: Schedule, routine: Schedule => Unit, activationSerial: Long): Unit = {
			val activationTime = clock.currentTimeRoundedUp
			if !schedule.isCanceled && activationSerial > activationSerialAtLastCancelAll then {
				schedule.runnable = new Runnable {
					override def run(): Unit = {
						if !schedule.isCanceled && schedule.activationSerial.get > activationSerialAtLastCancelAll then {
							thisDoer.executeSequentially { () =>
								if !schedule.isCanceled && schedule.activationSerial.get > activationSerialAtLastCancelAll then {
									routine(schedule)
									if schedule.interval > 0 && !schedule.isCanceled && schedule.activationSerial.get > activationSerialAtLastCancelAll then {
										val base = if schedule.isFixedRate then schedule.scheduledTime else clock.currentTimeRoundedUp
										schedule.program(base + schedule.interval)
									}
								}
							}
						}
					}
				}
				schedule.program(activationTime + schedule.initialDelay)
			}
		}

		override def cancel(schedule: Schedule): Unit = {
			schedule.isCanceled = true
		}

		override def cancelAll(): Unit = {
			activationSerialAtLastCancelAll = lastActivationSerial.get
		}

		override def wasActivated(schedule: Schedule): Boolean =
			schedule.activationSerial.get != NOT_ACTIVATED

		override def isCanceled(schedule: ScheduleImpl): Boolean =
			schedule.isCanceled || schedule.activationSerial.get <= activationSerialAtLastCancelAll
	}

	override def lull(worker: Worker, numberOfWorkersOutsideTheSleepZone: Int): Unit = {
		val est = earliestScheduledTimes(worker.index)
		if est == clock.MaxValue then clock.suspend(worker)
		else {
			val duration = est - clock.currentTimeRoundedDown
			if duration > 0 then clock.suspend(worker, duration)
		}
	}

	override def pollNextDoer(worker: Worker): DoerImpl | Null = {
		val currentTime = clock.currentTimeRoundedDown
		val workerIndex = worker.index
		val workerQueue = workerPriorityQueues(workerIndex)

		val earliestSchedule = workerQueue.peek
		if earliestSchedule eq null then {
			earliestScheduledTimes(workerIndex) = clock.MaxValue
			queuedDoers.poll()
		} else if earliestSchedule.scheduledTime - currentTime > 0 then {
			earliestScheduledTimes(workerIndex) = earliestSchedule.scheduledTime
			queuedDoers.poll()
		} else {
			val urgedDoer = pollExpiredSchedules(workerIndex, workerQueue, currentTime)
			if urgedDoer ne null then urgedDoer
			else queuedDoers.poll()
		}
	}

	/** Removes and enqueues the [[ScheduleImpl.runnable]] of expired schedules in the worker's priority queue, until a [[ScheduleImpl.owner]] is awakened and a succeeding schedule does not belong to it. \
	 * @return the awakened [[DoerImpl]] if any. */
	private def pollExpiredSchedules(workerIndex: Int, workerQueue: MinHeapPriorityQueue[ScheduleImpl], currentTime: MilliTime): DoerImpl | Null = {
		var maybeAwakenedDoer: DoerImpl | Null = null

		var earliestSchedule = workerQueue.peek
		while true do {
			workerQueue.finishPoll(earliestSchedule)
			val scheduleOwner = earliestSchedule.owner
			if scheduleOwner.enqueueRunnable(earliestSchedule.runnable) then maybeAwakenedDoer = scheduleOwner
			earliestSchedule = workerQueue.peek
			if earliestSchedule eq null then {
				earliestScheduledTimes(workerIndex) = clock.MaxValue
				return maybeAwakenedDoer
			}
			if earliestSchedule.scheduledTime - currentTime > 0 || (maybeAwakenedDoer ne null) && (earliestSchedule.owner ne maybeAwakenedDoer) then {
				earliestScheduledTimes(workerIndex) = earliestSchedule.scheduledTime
				return maybeAwakenedDoer
			}
		}
		maybeAwakenedDoer
	}
}
