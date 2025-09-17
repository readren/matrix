package readren.sequencer
package providers

import DoerProvider.Tag

import readren.common.CompileTime.getTypeName
import readren.common.deriveToString
import readren.sequencer.SchedulingExtension
import SchedulingExtension.MilliDuration
import providers.CooperativeWorkersDp.*
import providers.CooperativeWorkersSchedulingDp.*

import java.util
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable
import scala.language.adhocExtensions

object CooperativeWorkersSchedulingDp {
	/** A time based on the [[System.nanoTime]] method converted to milliseconds. */
	type MilliTime = Long
	/** A time based on the [[System.nanoTime]]. */
	type NanoTime = Long

	inline val INITIAL_DELAYED_TASK_QUEUE_CAPACITY = 16

	/** Facade of the concrete type of the [[Doer]] instances provided by [[CooperativeWorkersSchedulingDp]].
	 * Note that this trait is extending [[DoerFacade]] which is an abstract class. See [[DoerFacade]] to see why. */
	trait SchedulingDoerFacade extends DoerFacade, SchedulingExtension, LoopingExtension {
		override type Schedule <: ScheduleFacade
	}

	/** IMPORTANT: Represents a unique entity where equality and hash code must be based on identity. */
	trait ScheduleFacade {
		def initialDelay: MilliDuration

		def interval: MilliDuration

		def isFixedRate: Boolean

		/** Exposes the time the routine is expected to be run.
		 * Note that this value is updated by the scheduling thread and its last update may not be visible from other threads. Use for diagnostics only. */
		def scheduledTime: MilliTime

		/** A flag that tells if this instance was ever passed to the [[SchedulingDoerFacade.scheduleSequentially]] method.
		 * This flag is never cleared. */
		def wasActivated: Boolean

		/** An instance becomes canceled when either, it is passed to [[SchedulingDoerFacade.cancel]] or [[SchedulingDoerFacade.cancelAll]] is called after it [[wasActivated]]. */
		def isCanceled: Boolean
	}

	inline def nanosToMillisRoundedUp(nanos: Long): Long = (nanos + 999_999) / 1_000_000

	inline def nanosToMillisRoundedDown(nanos: Long): Long = nanos / 1_000_000

	class Impl(
		applyMemoryFence: Boolean = true,
		threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
		failureReporter: (Doer, Throwable) => Unit = DefaultDoerFaultReporter(true),
		unhandledExceptionReporter: (Doer, Throwable) => Unit = DefaultDoerFaultReporter(false),
		threadFactory: ThreadFactory = Executors.defaultThreadFactory()
	) extends CooperativeWorkersSchedulingDp(applyMemoryFence, threadPoolSize, threadFactory) {
		/** Called when a routine passed to the [[Doer.executeSequentially]] method of a provided [[Doer]] throws an exception. */
		override protected def onUnhandledException(doer: Doer, exception: Throwable): Unit = unhandledExceptionReporter(doer, exception)

		/** Called when the [[Doer.reportFailure]] method of a provided [[Doer]] is called. */
		override protected def onFailureReported(doer: Doer, failure: Throwable): Unit = failureReporter(doer, failure)
	}
}

/** Adds scheduling features to the [[CooperativeWorkersDp]].
 * Scheduling is managed by a dedicated single thread, which operates independently and does not contribute to the thread-pool size.
 * @param applyMemoryFence Determines whether memory fences are applied to ensure that store operations made by a task happen before load operations performed by successive tasks enqueued to the same [[Doer]].
 * The application of memory fences is optional because no test case has been devised to demonstrate their necessity. Apparently, the ordering constraints are already satisfied by the surrounding code.
 */
abstract class CooperativeWorkersSchedulingDp(
	applyMemoryFence: Boolean = true,
	threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
	threadFactory: ThreadFactory = Executors.defaultThreadFactory()
) extends CooperativeWorkersDp, DoerProvider[SchedulingDoerFacade] { thisSchedulingDoerProvider =>

	override def provide(tag: Tag): SchedulingDoerFacade = {
		new SchedulingDoerImpl(tag)
	}

	override def currentDoer: SchedulingDoerFacade | Null = super.currentDoer.asInstanceOf[SchedulingDoerFacade | Null]

	private class SchedulingDoerImpl(aTag: Tag) extends DoerImpl(aTag), SchedulingDoerFacade { thisSchedulingDoer =>

		override type Schedule = ScheduleImpl

		override def newDelaySchedule(delay: MilliDuration): Schedule =
			new ScheduleImpl(delay, 0L, false)

		override def newFixedRateSchedule(initialDelay: MilliDuration, interval: MilliDuration): Schedule =
			new ScheduleImpl(initialDelay, interval, true)

		override def newFixedDelaySchedule(initialDelay: MilliDuration, delay: MilliDuration): Schedule =
			new ScheduleImpl(initialDelay, delay, false)

		override def scheduleSequentially(schedule: Schedule, routine: Schedule => Unit): Unit = {
			if schedule.activated.getAndSet(true) then throw new IllegalStateException(s"The ${getTypeName[Schedule]} instance `$schedule` was already used before and can't be used twice.")
			else if !schedule.isCanceled then {
				val activationTime = nanosToMillisRoundedUp(System.nanoTime)
				schedule.routine =
					if schedule.interval <= 0 then routine
					else {
						if schedule.isFixedRate then { (s: Schedule) =>
							if !s.isCanceled then {
								routine(s)
								if !s.isCanceled then scheduler.scheduleRelativeToPrevious(s, schedule.interval)
							}

						} else { (s: Schedule) =>
							if !s.isCanceled then {
								routine(s)
								if !s.isCanceled then {
									val completionTime = nanosToMillisRoundedUp(System.nanoTime())
									scheduler.schedule(s, completionTime + s.interval)
								}
							}
						}
					}
				scheduler.schedule(schedule, activationTime + schedule.initialDelay)
			}
		}

		/** @inheritdoc
		 * This implementation removes the routine executions corresponding to the provided [[Schedule]] from the schedule.
		 * If called near its scheduled time from outside this [[Doer]]'s current thread, the routine may be executed a single time during this method execution, but not after this method returns.
		 * If called within this [[Doer]]'s current thread, it is ensured that no more execution of the routine can occur. */
		override def cancel(schedule: Schedule): Unit =
			scheduler.cancel(schedule)


		/** @inheritdoc
		 * This implementation removes all the scheduled executions corresponding to this [[Doer]] from its schedule.
		 * If called near a scheduled time from outside this [[Doer]] current thread, some [[Runnable]]s may be executed a single time during this method execution, but not after this method returns.
		 * If called within this [[Doer]] current thread, it is ensured that no more execution of scheduled [[Runnable]]s can occur. */
		override def cancelAll(): Unit = scheduler.cancelAllBelongingTo(thisSchedulingDoer)

		/** @inheritdoc
		 * An instance becomes active when is passed to the [[scheduleSequentially]] method.
		 * An instance becomes inactive when it is passed to the [[cancel]] method or when [[cancelAll]] is called. */
		override def wasActivated(schedule: Schedule): Boolean = schedule.activated.get()

		/** @return true if the [[Schedule]] was cancelled, even if it was not activated.
		 * Note that [[cancelAll]] does not cancel [[Schedule]] instances that weren't activated. */
		override def isCanceled(schedule: ScheduleImpl): Boolean = schedule.isCanceled

		/** IMPORTANT: Represents a unique entity where equality and hash code must be based on identity. */
		class ScheduleImpl(override val initialDelay: MilliDuration, override val interval: MilliDuration, override val isFixedRate: Boolean) extends ScheduleFacade {
			/** The routine whose execution is scheduled by this [[ScheduleImpl]].
			 * Initialized when this instance is activated, which happens when it is passed to the [[scheduleSequentially]] method. */
			var routine: (this.type => Unit) | Null = null
			/** @inheritdoc
			 * Only accessed within the scheduling thread. */
			var scheduledTime: MilliTime = 0L
			/** The index of this instance in the array-based min-heap.
			 * Only accessed within the scheduling thread. */
			var heapIndex: Int = -1
			/** Knows if the [[scheduler.enqueuedSchedulesByDoer]] collection contains this instance.
			 * Its purpose is to improve efficiency by avoiding unnecessary manipulations of said collection.
			 * Only accessed within the scheduling thread. */
			var isTriggered = false
			@volatile var isCanceled = false
			val activated: AtomicBoolean = AtomicBoolean(false)

			override def wasActivated: Boolean = activated.get

			inline def owner: thisSchedulingDoer.type = thisSchedulingDoer

			/** Executes the routine associated to this [[ScheduleImpl]] */
			inline def execute(): Unit = routine(this)

			override def toString: String = deriveToString(this) + s" scheduledTime: $scheduledTime"
		}
	}


	/**
	 * The `scheduler` object is responsible for managing the scheduling and execution of delayed and periodic tasks  for all [[SchedulingDoerFacade]] instances provided by this [[CooperativeWorkersSchedulingDp]].
	 *
	 * == Responsibilities ==
	 *   - Maintains a min-heap priority queue of scheduled tasks, ordered by their next scheduled execution time.
	 *   - Handles scheduling, cancellation, and rescheduling of tasks, including fixed-rate and fixed-delay periodic executions.
	 *   - Ensures that scheduled tasks are executed at (or as close as possible to) their intended times, using a dedicated thread.
	 *   - Supports cancellation of individual schedules or all schedules belonging to a specific [[SchedulingDoerImpl]].
	 *   - Tracks enabled (i.e., currently executing or about to execute) schedules separately from the heap for efficient cancellation.
	 *   - Provides diagnostic information about the scheduler's state.
	 *
	 * == Threading ==
	 *   - The scheduler runs on a dedicated thread, created via the provided [[threadFactory]].
	 *   - All modifications to the scheduling queue and enabled schedules are performed on this thread, with external requests (such as schedule, cancel, or cancelAll) being enqueued as commands via a synchronized queue.
	 *   - This design ensures thread safety and avoids race conditions between scheduling operations and task execution.
	 *
	 * == Heap Management ==
	 *   - The scheduler uses an array-based binary min-heap to efficiently manage the next task to execute.
	 *   - The heap is dynamically resized as needed.
	 *   - Each scheduled task (a [[ScheduleImpl]]) tracks its index in the heap for O(1) removal.
	 *
	 * == Lifecycle ==
	 *   - The scheduler thread is started upon construction and runs until [[shutdown]] is called.
	 *   - On shutdown, all scheduled and enabled tasks are deactivated, and resources are released.
	 *
	 * == Usage ==
	 *   - Not intended for direct use; all interactions should go through the [[SchedulingDoerFacade]] API.
	 */
	private object scheduler extends Runnable {
		private val commandsQueue = new util.ArrayDeque[Runnable]()
		private var heap: Array[SchedulingDoerImpl#ScheduleImpl | Null] = Array.fill(INITIAL_DELAYED_TASK_QUEUE_CAPACITY)(null)
		private var heapSize: Int = 0
		/** A register that knows the instances of [[SchedulingDoerImpl.ScheduleImpl]] that are not in the [[heap]] because they were triggered and still not rescheduled (still not added to the [[heap]] again after the routine completes and [[schedule]] or [[scheduleRelativeToPrevious]] is called again).
		 * We say that a schedule is triggered when its [[SchedulingDoerImpl.ScheduleImpl.routine]] is enqueued for execution in the [[CooperativeWorkersDp.DoerImpl.taskQueue]].
		 * A schedule is triggered only after its [[SchedulingDoerImpl.ScheduleImpl.scheduledTime]] is reached.
		 * This register is necessary to implement the [[cancelAllBelongingTo]] method because enqueued instances are not in the [[heap]].
		 * The goal of this register is to know all the [[Schedule]] instances that are activated but not contained in the [[heap]]. */
		private var triggeredSchedulesByDoer: util.HashMap[SchedulingDoerImpl, mutable.HashSet[SchedulingDoerImpl#ScheduleImpl]] = new util.HashMap()

		private var isRunning = true

		private val timeWaitingThread: Thread = threadFactory.newThread(this)
		timeWaitingThread.start()

		def numOfEnabledSchedules: Int = {
			var accum = 0
			triggeredSchedulesByDoer.forEach((_, set) => accum += set.size)
			accum
		}

		/** Schedules a single execution of the routine associated to the specified [[Schedule]] at a specified time. */
		def schedule(schedule: SchedulingDoerImpl#ScheduleImpl, scheduleTime: MilliTime): Unit = {
			signal { () =>
				removeFromRegister(schedule)
				if !schedule.isCanceled then {
					schedule.scheduledTime = scheduleTime
					if scheduleTime * 1_000_000 <= System.nanoTime() then triggeredForExecution(schedule)
					else appoint(schedule)
				}
			}
		}

		/** Schedules a single execution of the routine associated to the specified [[Shedule]] at a time relative to the previous schedule time. */
		def scheduleRelativeToPrevious(schedule: SchedulingDoerImpl#ScheduleImpl, interval: MilliDuration): Unit = {
			signal { () =>
				removeFromRegister(schedule)
				if !schedule.isCanceled then {
					schedule.scheduledTime += interval
					if schedule.scheduledTime * 1_000_000 <= System.nanoTime() then triggeredForExecution(schedule)
					else appoint(schedule)
				}
			}
		}

		def cancel(schedule: SchedulingDoerImpl#ScheduleImpl): Unit = {
			schedule.isCanceled = true
			signal { () =>
				if !removeFromRegister(schedule) then remove(schedule)
			}
		}

		/** Cancels oll the activated [[Schedule]] instances corresponding to a [[Doer]]. */
		def cancelAllBelongingTo(doer: SchedulingDoerImpl): Unit = {
			signal { () =>
				var index = heapSize
				while index > 0 do {
					index -= 1
					val schedule = heap(index)
					if schedule.owner eq doer then {
						schedule.isCanceled = true
						remove(schedule)
					}
				}

				val enabledSchedules = triggeredSchedulesByDoer.remove(doer)
				if enabledSchedules ne null then enabledSchedules.foreach { schedule =>
					schedule.isCanceled = true
					schedule.isTriggered = false
				}
			}
		}

		/** Stops the scheduling thread. */
		def stop(): Unit = {
			signal(() =>
				isRunning = false
			)
		}

		/** TODO this design is inefficient because requires the creation of a [[Runnable]] instance every call. Consider an improvement. */
		private def signal(command: Runnable): Unit = {
			this.synchronized {
				commandsQueue.offer(command)
				this.notify()
			}
		}

		/** Removes a [[Schedule]] from the register that knows which [[Schedule]] instances were enqueued and still not scheduled again.
		 * @return true if the [[Schedule]] was in the register. */
		private def removeFromRegister(schedule: SchedulingDoerImpl#ScheduleImpl): Boolean = {
			if schedule.isTriggered then {
				schedule.isTriggered = false
				if triggeredSchedulesByDoer.computeIfPresent(schedule.owner, (_, enabledSchedules) => enabledSchedules.subtractOne(schedule)).isEmpty then
					triggeredSchedulesByDoer.remove(schedule.owner)
				true
			} else false
		}

		private def triggeredForExecution(es: SchedulingDoerImpl#ScheduleImpl): Unit = {
			es.isTriggered = true
			// Memorize which schedule instances were triggered for execution. Necessary to support `cancelAll`.
			triggeredSchedulesByDoer.compute(
				es.owner,
				(_, enqueuedSchedules) => if enqueuedSchedules eq null then mutable.HashSet(es) else enqueuedSchedules.addOne(es)
			)
			es.owner.executeSequentially(() => es.execute())
		}

		/** Main loop of the scheduling thread. */
		override def run(): Unit = {
			while isRunning do {
				// execute all pending commands
				var command: Runnable | Null = this.synchronized(commandsQueue.poll())
				while command ne null do {
					command.run()
					command = this.synchronized(commandsQueue.poll())
				}

				var earlierSchedule = peek
				val currentTime = nanosToMillisRoundedDown(System.nanoTime())
				while (earlierSchedule ne null) && earlierSchedule.scheduledTime <= currentTime do {
					val es = earlierSchedule.asInstanceOf[SchedulingDoerImpl#ScheduleImpl]
					finishPoll(es)
					triggeredForExecution(es)
					earlierSchedule = peek
				}
				this.synchronized {
					if isRunning && commandsQueue.isEmpty then {
						if earlierSchedule eq null then this.wait()
						else {
							val delay = earlierSchedule.scheduledTime - currentTime
							earlierSchedule = null // do not keep unnecessary references while waiting to avoid unnecessary memory retention
							this.wait(delay)
						}
					}
				}
			}
			// Reached when stopped.
			this.synchronized(commandsQueue.clear()) // do not keep unnecessary references after stopped to avoid unnecessary memory retention
			for i <- 0 until heapSize do heap(i).isCanceled = true
			heap = null // do not keep unnecessary references after stopped to avoid unnecessary memory retention
			triggeredSchedulesByDoer.forEach { (_, enabledSchedules) =>
				enabledSchedules.foreach { schedule =>
					schedule.isCanceled = true
					schedule.isTriggered = false
				}
			}
			triggeredSchedulesByDoer = null // do not keep unnecessary references while waiting to avoid unnecessary memory retention
		}

		private inline def peek: SchedulingDoerImpl#ScheduleImpl | Null = heap(0)

		/** Adds the provided element to this min-heap based priority queue. */
		private def appoint(element: SchedulingDoerImpl#ScheduleImpl): Unit = {
			val holeIndex = heapSize
			if holeIndex >= heap.length then grow()
			heapSize = holeIndex + 1
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
		private def finishPoll(peekedElement: SchedulingDoerImpl#ScheduleImpl): SchedulingDoerImpl#ScheduleImpl = {
			heapSize -= 1
			val s = heapSize
			val replacement = heap(s)
			heap(s) = null
			if s != 0 then siftDown(0, replacement)
			peekedElement.heapIndex = -1
			peekedElement
		}

		/** Removes the provided element from this queue.
		 * @return true if the element was removed; false if it is not contained by this queue. */
		private def remove(element: SchedulingDoerImpl#ScheduleImpl): Boolean = {
			val elemIndex = indexOf(element)
			if elemIndex < 0 then return false
			element.heapIndex = -1
			heapSize -= 1
			val s = heapSize
			val replacement = heap(s)
			heap(s) = null
			if s != elemIndex then {
				siftDown(elemIndex, replacement)
				if heap(elemIndex) eq replacement then siftUp(elemIndex, replacement)
			}
			true
		}

		private inline def indexOf(element: SchedulingDoerImpl#ScheduleImpl): Int = element.heapIndex

		/**
		 * Replaces the element at position `holeIndex` of the heap-based array with the `providedElement` and rearranges it and its parents as necessary to ensure that all parents are less than or equal to their children.
		 * Note that for the entire heap to satisfy the min-heap property, the `providedElement` must be less than or equal to the children of `holeIndex`.
		 * Sifts element added at bottom up to its heap-ordered spot.
		 */
		private def siftUp(holeIndex: Int, providedElement: SchedulingDoerImpl#ScheduleImpl): Unit = {
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
		private def siftDown(holeIndex: Int, providedElement: SchedulingDoerImpl#ScheduleImpl): Unit = {
			var gapIndex = holeIndex
			var half = heapSize >>> 1
			while gapIndex < half do {
				var childIndex = (gapIndex << 1) + 1
				var child = heap(childIndex)
				val rightIndex = childIndex + 1
				if rightIndex < heapSize && child.scheduledTime > heap(rightIndex).scheduledTime then {
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

		def diagnose(sb: StringBuilder): StringBuilder = {
			sb.append("\t\tisRunning=").append(isRunning).append('\n')
			sb.append("\t\theapSize=").append(scheduler.heapSize).append('\n')
			sb.append("\t\tnumOfEnabledSchedules=").append(scheduler.numOfEnabledSchedules).append('\n')
		}
	}

	/**
	 * Makes this [[CooperativeWorkersSchedulingDp]] to shut down when all the workers are sleeping.
	 * Invocation has no additional effect if already shut down.
	 *
	 * <p>This method does not wait. Use [[awaitTermination]] to do that.
	 *
	 * @throws SecurityException @inheritDoc
	 */
	override def shutdown(): Unit = {
		scheduler.stop()
		super.shutdown()
	}

	override def diagnose(sb: StringBuilder): StringBuilder = {
		sb.append(getTypeName[CooperativeWorkersSchedulingDp]).append('\n')
		sb.append("\tscheduler:\n")
		scheduler.diagnose(sb)
		super.diagnose(sb)
	}
}
