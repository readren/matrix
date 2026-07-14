package readren.sequencer
package providers

import providers.CooperativeShardedPollingSchedulerDp.{NOT_ACTIVATED, ScheduleFacade, SchedulingDoerFacade}

import readren.common.CompileTime.getTypeName
import readren.common.Maybe

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Executors, ThreadFactory}

object CooperativeShardedPollingSchedulerDp extends CooperativeSchedulerDpCompanion {
	final class Impl(
		applyMemoryFence: Boolean = true,
		threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
		unhandledExceptionReporter: (Doer, Throwable) => Unit = DefaultDoerUnhandledExceptionReporter(),
		threadFactory: ThreadFactory = Executors.defaultThreadFactory(),
		clock: MonotonicClock = new NanoTimeBasedMilliClock
	) extends CooperativeShardedPollingSchedulerDp(applyMemoryFence, threadPoolSize, threadFactory, clock) {

		override type Tag = String

		override def tagFromText(text: String): Tag = text

		override protected def onUnhandledException(doer: Doer, exception: Throwable): Unit = unhandledExceptionReporter(doer, exception)
	}

	inline def NOT_ACTIVATED: Long = Long.MaxValue
}

/** Adds scheduling features to the [[CooperativeWorkersDp]] by partitioning scheduling heaps per worker thread. \
 * Each worker thread maintains its own [[MinHeapPriorityQueue]] of schedules, avoiding global provider lock contention. */
abstract class CooperativeShardedPollingSchedulerDp(
	applyMemoryFence: Boolean = true,
	threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
	threadFactory: ThreadFactory = Executors.defaultThreadFactory(),
	clock: MonotonicClock = new NanoTimeBasedMilliClock,
) extends CooperativeWorkersDp(applyMemoryFence, threadPoolSize, threadFactory), DoerProvider[SchedulingDoerFacade] { thisProvider =>

	/** Schedule representation managed by the sharded priority queues of this provider. */
	private class ScheduleImpl(val owner: SchedulingDoerImpl, override val initialDelay: MilliDuration, override val interval: MilliDuration, override val isFixedRate: Boolean) extends MinHeapPriorityQueue.Element, ScheduleFacade {
		/** Monotonically increasing sequence number indicating when this schedule was activated. \
		 * Holds [[CooperativeShardedPollingSchedulerDp.NOT_ACTIVATED]] if the schedule has not been activated. */
		val activationSerial: AtomicLong = AtomicLong(NOT_ACTIVATED)

		override def wasActivated: Boolean = activationSerial.get() != NOT_ACTIVATED

		var runnable: Runnable | Null = null

		@volatile var isCanceled = false

		inline def execute(): Unit = runnable.run()

		/** Enqueues this schedule to the sharded priority queue of its assigned worker thread. \
		 * Wakes up the worker if the new scheduled time is earlier than the worker's current earliest. */
		def program(scheduledTime: MilliTime): Unit = {
			this.scheduledTime = scheduledTime
			val workerIndex = owner.assignedWorkerIndex
			val worker = workers(workerIndex)
			val earliestChanged = worker.synchronized {
				val oldEarliest = earliestScheduledTimes(workerIndex)
				val queue = workerPriorityQueues(workerIndex)
				queue.add(this)
				val newEarliest = queue.peek.scheduledTime
				earliestScheduledTimes(workerIndex) = newEarliest
				val changed = oldEarliest - newEarliest > 0
				changed
			}
			if earliestChanged then worker.wakeUpIfSleeping()
		}

		override def toString: String =
			s"ScheduleImpl(owner=${owner.tag}, initialDelay=$initialDelay, interval=$interval, isFixedRate=$isFixedRate, scheduledTime: $scheduledTime, wasActivated=$wasActivated)"
	}

	/** Priority queues containing pending schedules, partitioned by worker thread index. \
	 * Access is synchronized on the corresponding worker monitor. */
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

		/** The index of the worker thread assigned to process schedules for this doer. */
		val assignedWorkerIndex: Int = (aTag.hashCode % threadPoolSize).abs

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

		override def cancel(schedule: Schedule): Unit = {
			schedule.isCanceled = true
			val workerIndex = assignedWorkerIndex
			val worker = workers(workerIndex)
			worker.synchronized {
				val queue = workerPriorityQueues(workerIndex)
				val removed = queue.remove(schedule)
				val next = queue.peek
				val oldEarliest = earliestScheduledTimes(workerIndex)
				earliestScheduledTimes(workerIndex) = if next eq null then clock.MaxValue else next.scheduledTime
			}
		}

		override def cancelAll(): Unit = {
			activationSerialAtLastCancelAll = lastActivationSerial.get
			val workerIndex = assignedWorkerIndex
			val worker = workers(workerIndex)
			worker.synchronized {
				val queue = workerPriorityQueues(workerIndex)
				val toRemove = new scala.collection.mutable.ListBuffer[ScheduleImpl]()
				var i = 0
				while i < queue.size do {
					val schedule = queue(i)
					if (schedule ne null) && (schedule.owner eq thisDoer) then {
						toRemove += schedule
					}
					i += 1
				}
				for schedule <- toRemove do {
					schedule.isCanceled = true
					queue.remove(schedule)
				}
				val next = queue.peek
				val oldEarliest = earliestScheduledTimes(workerIndex)
				earliestScheduledTimes(workerIndex) = if next eq null then clock.MaxValue else next.scheduledTime
			}
		}


		override def wasActivated(schedule: Schedule): Boolean =
			schedule.activationSerial.get != NOT_ACTIVATED

		override def isCanceled(schedule: ScheduleImpl): Boolean =
			schedule.isCanceled || schedule.activationSerial.get <= activationSerialAtLastCancelAll
	}

	override protected def shouldSleepIndefinitely(worker: Worker): Boolean = {
		val queue = workerPriorityQueues(worker.index)
		worker.synchronized {
			queue.size == 0
		}
	}

	override def lull(worker: Worker): Unit = {
		val myIndex = worker.index
		val est = earliestScheduledTimes(myIndex)
		if est == clock.MaxValue then {
			clock.suspend(worker)
		} else {
			val duration = est - clock.currentTimeRoundedDown
			if duration > 0 then clock.suspend(worker, duration)
		}
	}

	override def pollNextDoer(worker: Worker): DoerImpl | Null = {
		val myIndex = worker.index
		val myQueue = workerPriorityQueues(myIndex)
		val currentMilliTime = clock.currentTimeRoundedDown

		val earliest = worker.synchronized {
			val res = myQueue.peek
			if res eq null then {
				earliestScheduledTimes(myIndex) = clock.MaxValue
			} else if res.scheduledTime - currentMilliTime > 0 then {
				earliestScheduledTimes(myIndex) = res.scheduledTime
			}
			res
		}

		val resDoer = if earliest eq null then {
			queuedDoers.poll()
		} else if earliest.scheduledTime - currentMilliTime > 0 then {
			queuedDoers.poll()
		} else {
			val urgedDoer = pollExpiredSchedules(myIndex, currentMilliTime)
			if urgedDoer ne null then urgedDoer
			else queuedDoers.poll()
		}
		resDoer
	}

	/** Removes and enqueues all expired schedules in the worker's priority queue. \
	 * Returns the first awakened [[DoerImpl]] if any. */
	private def pollExpiredSchedules(workerIndex: Int, currentTime: MilliTime): DoerImpl | Null = {
		val worker = workers(workerIndex)
		val myQueue = workerPriorityQueues(workerIndex)
		var maybeAwakenedDoer: DoerImpl | Null = null

		worker.synchronized {
			var earliest = myQueue.peek
			while (earliest ne null) && earliest.scheduledTime - currentTime <= 0 && ((maybeAwakenedDoer eq null) || (earliest.owner eq maybeAwakenedDoer)) do {
				myQueue.finishPoll(earliest)
				val owner = earliest.owner
				val enqueued = owner.enqueueRunnable(earliest.runnable)
				if enqueued then {
					maybeAwakenedDoer = owner
				}
				earliest = myQueue.peek
			}
			val next = myQueue.peek
			earliestScheduledTimes(workerIndex) = if next eq null then clock.MaxValue else next.scheduledTime
		}

		maybeAwakenedDoer
	}
}
