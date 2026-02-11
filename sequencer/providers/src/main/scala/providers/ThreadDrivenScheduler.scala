package readren.sequencer
package providers

import providers.ThreadDrivenScheduler.{INITIAL_DELAYED_TASK_QUEUE_CAPACITY, Plan}

import readren.common.deriveToString

import java.util
import java.util.concurrent.*
import scala.collection.mutable
import scala.reflect.ClassTag

object ThreadDrivenScheduler {


	inline val INITIAL_DELAYED_TASK_QUEUE_CAPACITY = 16


	/** IMPORTANT: Represents a unique entity where equality and hash code must be based on identity. */
	abstract class Plan[+D <: Doer](val owner: D) extends MinHeapPriorityQueue.Element {
		val initialDelay: MilliDuration
		val interval: MilliDuration
		val isFixedRate: Boolean
		/** The [[Runnable]] whose execution is scheduled by this [[ScheduleImpl]].
		 * Initialized when this instance is activated, which happens when it is passed to the [[scheduleSequentially]] method. */
		var runnable: Runnable | Null = null
		/** Knows if the [[scheduler.enqueuedSchedulesByDoer]] collection contains this instance.
		 * Its purpose is to improve efficiency by avoiding unnecessary manipulations of said collection.
		 * Only accessed within the scheduling thread. */
		var isTriggered = false
		@volatile var isCanceled = false

		override def toString: String = deriveToString(this) + s" scheduledTime: $scheduledTime"
	}
}


/**
 * Manages the scheduling of the execution of tasks within [[Doer]] instances.
 * Similar to [[java.util.concurrent.ScheduledThreadPoolExecutor]] but the execution is donde by a [[Doer]] instead of an [[java.util.concurrent.ExecutorService]].
 *
 * == Responsibilities ==
 *   - Maintains a min-heap priority queue of scheduled tasks, ordered by their next scheduled execution time.
 *   - Handles scheduling, cancellation, and rescheduling of tasks, including fixed-rate and fixed-delay periodic executions.
 *   - Ensures that scheduled tasks are executed at (or as close as possible after) their intended times, using a dedicated thread.
 *   - Supports cancellation of individual schedules or all schedules belonging to a specific [[Doer]].
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
 *   - Each scheduled task (a [[Plan]]) tracks its index in the heap for O(1) removal.
 *
 * == Lifecycle ==
 *   - The scheduler thread is started upon construction and runs until [[shutdown]] is called.
 *   - On shutdown, all scheduled and enabled tasks are deactivated, and resources are released.
 *
 * @tparam D the type of [[Doer]] that executes the scheduled routines.
 * @tparam P the type of the [[Plan]] instances that this scheduler accepts.           
 */
class ThreadDrivenScheduler[D <: Doer, P <: Plan[D]](threadFactory: ThreadFactory)(using ctP: ClassTag[P | Null]) extends Runnable {
	private val commandsQueue = new util.ArrayDeque[Runnable]()

	private val priorityQueue: MinHeapPriorityQueue[Plan[D]] = new MinHeapPriorityQueue[Plan[D]](INITIAL_DELAYED_TASK_QUEUE_CAPACITY)
	/** A register that knows the instances of [[Plan]] that are not in the [[heap]] because they were triggered and still not rescheduled (still not added to the [[heap]] again after the routine completes and [[schedule]] or [[scheduleRelativeToPrevious]] is called again).
	 * We say that a schedule is triggered when its [[Plan.runnable]] is passed to [[Doer.executeSequentially]].
	 * A schedule is triggered only after its [[Plan.scheduledTime]] is reached.
	 * This register is necessary to implement the [[cancelAllBelongingTo]] method because enqueued instances are not in the [[heap]].
	 * The goal of this register is to know all the [[Plan]] instances that are activated but not contained in the [[heap]]. */
	private var triggeredSchedulesByDoer: util.HashMap[D, mutable.HashSet[P]] = new util.HashMap()

	private var isRunning = true

	private val timeWaitingThread: Thread = threadFactory.newThread(this)
	timeWaitingThread.start()

	/** Schedules a single execution of the routine associated to the specified [[Plan]] at a specified time. */
	def schedule(plan: P, scheduleTime: MilliTime): Unit = {
		signal { () =>
			removeFromRegister(plan)
			if !plan.isCanceled then {
				plan.scheduledTime = scheduleTime
				if scheduleTime * 1_000_000 <= System.nanoTime() then triggerExecutionOf(plan)
				else priorityQueue.add(plan)
			}
		}
	}

	/** Schedules a single execution of the routine associated to the specified [[Plan]] at a time relative to the previous schedule time. */
	def scheduleRelativeToPrevious(schedule: P, interval: MilliDuration): Unit = {
		signal { () =>
			removeFromRegister(schedule)
			if !schedule.isCanceled then {
				schedule.scheduledTime += interval
				if schedule.scheduledTime * 1_000_000 <= System.nanoTime() then triggerExecutionOf(schedule)
				else priorityQueue.add(schedule)
			}
		}
	}

	def cancel(schedule: P): Unit = {
		schedule.isCanceled = true
		signal { () =>
			if !removeFromRegister(schedule) then priorityQueue.remove(schedule)
		}
	}

	/** Cancels oll the activated [[Plan]] instances corresponding to a [[Doer]]. */
	def cancelAllBelongingTo(doer: D): Unit = {
		signal { () =>
			var index = priorityQueue.size
			while index > 0 do {
				index -= 1
				val schedule = priorityQueue(index).asInstanceOf[P]
				if schedule.owner eq doer then {
					schedule.isCanceled = true
					priorityQueue.remove(schedule)
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

	/** TODO this design is inefficient because it blocks the [[timeWaitingThread]] and requires the creation of a [[Runnable]] instance for the command in every call. Consider an improvement.
	 * TODO An alternative would be to use a tiered Doer (see [[CooperativeWorkersTieredDp]]) instead of a dedicated thread to manage the scheduling. This would eliminate the [[timeWaitingThread]] and maybe synchronization blocking.
	 * */
	private def signal(command: Runnable): Unit = {
		this.synchronized {
			commandsQueue.offer(command)
			this.notify()
		}
	}

	/** Removes a [[Plan]] from the register that knows which [[Plan]] instances were enqueued and still not scheduled again.
	 * @return true if the [[Plan]] was in the register. */
	private def removeFromRegister(schedule: P): Boolean = {
		if schedule.isTriggered then {
			schedule.isTriggered = false
			val triggeredSchedules = triggeredSchedulesByDoer.get(schedule.owner)
			if triggeredSchedules ne null then triggeredSchedules.remove(schedule)
			if triggeredSchedules.isEmpty then triggeredSchedulesByDoer.remove(schedule.owner)
			true
		} else false
	}

	/** Triggers the execution of the routine corresponding to the specified [[Plan]], and registers that the [[Plan]] was triggered and still not rescheduled. */
	private def triggerExecutionOf(plan: P): Unit = {
		plan.isTriggered = true
		// Memorize which schedule instances were triggered for execution. Necessary to support `cancelAll`.
		val triggeredSchedules = triggeredSchedulesByDoer.get(plan.owner)
		if triggeredSchedules eq null then triggeredSchedulesByDoer.put(plan.owner, mutable.HashSet(plan))
		else triggeredSchedules.addOne(plan)
		plan.owner.executeSequentially(plan.runnable)
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

			var earlierSchedule = priorityQueue.peek
			val currentTime = nanosToMillisRoundedDown(System.nanoTime())
			while (earlierSchedule ne null) && earlierSchedule.scheduledTime <= currentTime do {
				val plan = earlierSchedule.asInstanceOf[P]
				priorityQueue.finishPoll(plan)
				triggerExecutionOf(plan)
				earlierSchedule = priorityQueue.peek
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
		for i <- 0 until priorityQueue.size do priorityQueue(i).isCanceled = true
		priorityQueue.clear() // do not keep unnecessary references after stopped to avoid unnecessary memory retention
		triggeredSchedulesByDoer.forEach { (_, enabledSchedules) =>
			enabledSchedules.foreach { schedule =>
				schedule.isCanceled = true
				schedule.isTriggered = false
			}
		}
		triggeredSchedulesByDoer = null // do not keep unnecessary references while waiting to avoid unnecessary memory retention
	}

	def diagnose(sb: StringBuilder): StringBuilder = {
		sb.append("\t\tisRunning=").append(isRunning).append('\n')
		sb.append("\t\tpriorityQueueSize=").append(priorityQueue.size).append('\n')
	}
}
