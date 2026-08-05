package readren.sequencer
package providers

import providers.ThreadDrivenScheduler.{INITIAL_HEAP_QUEUE_CAPACITY, Plan}

import readren.common.deriveToString

import java.util
import java.util.concurrent.*
import scala.collection.mutable
import scala.reflect.ClassTag

object ThreadDrivenScheduler {

	inline val INITIAL_HEAP_QUEUE_CAPACITY = 16

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

/** Manages the scheduling of the execution of tasks within [[Doer]] instances. \
 * Similar to [[java.util.concurrent.ScheduledThreadPoolExecutor]] but the execution is done by a [[Doer]] instead of an [[java.util.concurrent.ExecutorService]].
 *
 * == Responsibilities ==
 *   - Maintains a min-heap priority queue of scheduled tasks, ordered by their next scheduled execution time.
 *   - Handles scheduling, cancellation, and rescheduling of tasks, including fixed-rate and fixed-delay periodic executions.
 *   - Ensures that scheduled tasks are executed at (or as close as possible after) their intended times, using a dedicated thread.
 *   - Supports cancellation of individual schedules or all schedules belonging to a specific [[Doer]].
 *   - Tracks enabled (i.e., currently executing or about to execute) schedules separately from the heap for efficient cancellation.
 *   - Provides diagnostic information about the scheduler's state.
 *
 * == Threading and Concurrency ==
 *   - The scheduler runs on a dedicated thread, created via the provided `threadFactory`.
 *   - All modifications to the scheduling queue and enabled schedules are performed on this thread, with external requests (such as schedule, cancel, or cancelAll) being enqueued as custom [[Command]] instances via a synchronized queue.
 *   - This design ensures thread safety and avoids deadlocks by ensuring that the scheduling thread never holds outer locks while executing tasks.
 *   - To minimize synchronization overhead, the scheduling thread batch-drains all pending [[Command]]s in a single lock acquisition per iteration.
 *
 * == Heap Management ==
 *   - The scheduler uses an array-based binary min-heap to efficiently manage the next task to execute.
 *   - The heap is dynamically resized as needed.
 *   - Each scheduled task (a [[Plan]]) tracks its index in the heap for O(1) removal.
 *
 * == Lifecycle ==
 *   - The scheduler thread is started upon construction and runs until [[stop]] is called.
 *   - On shutdown, all scheduled and enabled tasks are deactivated, and resources are released.
 *
 * @tparam D the type of [[Doer]] that executes the scheduled routines.
 * @tparam P the type of the [[Plan]] instances that this scheduler accepts. */
class ThreadDrivenScheduler[D <: Doer, P <: Plan[D]](threadFactory: ThreadFactory, trackSleepTime: Boolean = false)(using ctP: ClassTag[P | Null]) extends Runnable { thisScheduler =>

	sealed trait Command {
		/** Executed by the scheduling thread to apply state modifications. */
		def execute(): Unit
	}

	final class Schedule(plan: P, scheduledTime: MilliTime) extends Command {
		override def execute(): Unit = {
			removeFromRegister(plan)
			if !plan.isCanceled then {
				plan.scheduledTime = scheduledTime
				if scheduledTime * 1_000_000 <= System.nanoTime() then triggerExecutionOf(plan)
				else priorityQueue.add(plan)
			}
		}
	}

	private inline def Schedule(trap: Nothing): Any = trap

	final class ScheduleRelative(plan: P, interval: MilliDuration) extends Command {
		override def execute(): Unit = {
			removeFromRegister(plan)
			if !plan.isCanceled then {
				plan.scheduledTime += interval
				if plan.scheduledTime * 1_000_000 <= System.nanoTime() then triggerExecutionOf(plan)
				else priorityQueue.add(plan)
			}
		}
	}

	private inline def ScheduleRelative(trap: Nothing): Any = trap

	final class Cancel(plan: P) extends Command {
		override def execute(): Unit = {
			if !removeFromRegister(plan) then priorityQueue.remove(plan)
		}
	}

	private inline def Cancel(trap: Nothing): Any = trap

	final class CancelAll(doer: D) extends Command {
		override def execute(): Unit = {
			// Note: We do not maintain a separate registry of triggered-but-not-yet-executed tasks. Instead, cancellation of already triggered tasks is handled lazily/safely via activationSerial checks inside the execution runnables.
			var index = priorityQueue.size
			while index > 0 do {
				index -= 1
				val schedule = priorityQueue(index).asInstanceOf[P]
				if schedule.owner eq doer then {
					schedule.isCanceled = true
					// Removing an element from the min-heap replaces it with the last element of the heap and bubbles it. If the replacement bubbles down, it stays at or below `index` (handled by repeating the check at `index`). If it bubbles up, it moves to a parent index < `index` (handled by the backward iteration of the loop). Therefore, incrementing the index cursor when the removed element was not the last element is sufficient to visit and check all elements without missing any and without allocating temporary lists.
					if priorityQueue.remove(schedule) && index < priorityQueue.size then index += 1
				}
			}
		}
	}

	private inline def CancelAll(trap: Nothing): Any = trap

	case object Stop extends Command {
		override def execute(): Unit = {
			isRunning = false
		}
	}

	private val commandsQueue = new util.ArrayDeque[Command]()

	private val priorityQueue = new MinHeapPriorityQueue[Plan[D]](INITIAL_HEAP_QUEUE_CAPACITY)

	@volatile var totalSleepTimeNanos: Long = 0L

	private var isRunning = true

	private val timeWaitingThread: Thread = threadFactory.newThread(this)
	timeWaitingThread.start()

	/** Schedules a single execution of the routine associated to the specified [[Plan]] at a specified time. */
	def schedule(plan: P, scheduleTime: MilliTime): Unit = {
		signal(new Schedule(plan, scheduleTime))
	}

	/** Schedules a single execution of the routine associated to the specified [[Plan]] at a time relative to the previous schedule time. */
	def scheduleRelativeToPrevious(schedule: P, interval: MilliDuration): Unit = {
		signal(new ScheduleRelative(schedule, interval))
	}

	def cancel(schedule: P): Unit = {
		schedule.isCanceled = true
		signal(new Cancel(schedule))
	}

	/** Cancels all the activated [[Plan]] instances corresponding to a [[Doer]]. */
	def cancelAllBelongingTo(doer: D): Unit = {
		signal(new CancelAll(doer))
	}

	/** Stops the scheduling thread. */
	def stop(): Unit = {
		signal(Stop)
	}

	/** TODO this design is inefficient because it blocks the caller. Consider an improvement.
	 * TODO An alternative would be to use a tiered Doer (see [[CooperativeWorkersTieredDp]]) instead of a dedicated thread to manage the scheduling. This would eliminate the [[timeWaitingThread]] and maybe synchronization blocking.
	 * */
	private def signal(command: Command): Unit = {
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
			true
		} else false
	}

	/** Triggers the execution of the routine corresponding to the specified [[Plan]], and registers that the [[Plan]] was triggered and still not rescheduled. */
	private def triggerExecutionOf(plan: P): Unit = {
		plan.isTriggered = true
		plan.owner.executeSequentially(plan.runnable)
	}

	/** Main loop of the scheduling thread. */
	override def run(): Unit = {
		while isRunning do {
			val localCommands = this.synchronized {
				if commandsQueue.isEmpty then null
				else {
					val arr = commandsQueue.toArray(new Array[Command](commandsQueue.size()))
					commandsQueue.clear()
					arr
				}
			}
			if localCommands ne null then {
				var i = 0
				while i < localCommands.length do {
					localCommands(i).execute()
					i += 1
				}
			}

			var earlierSchedule = priorityQueue.peek
			val currentTime = nanosToMillisRoundedDown(System.nanoTime())
			while (earlierSchedule ne null) && earlierSchedule.scheduledTime <= currentTime do {
				val plan = earlierSchedule.asInstanceOf[P]
				priorityQueue.finishPoll(plan)
				triggerExecutionOf(plan)
				earlierSchedule = priorityQueue.peek
			}
			val sleepStart = if trackSleepTime then System.nanoTime() else 0L
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
			if trackSleepTime then totalSleepTimeNanos += (System.nanoTime() - sleepStart)
		}
		// Reached when stopped.
		this.synchronized(commandsQueue.clear()) // do not keep unnecessary references after stopped to avoid unnecessary memory retention
		for i <- 0 until priorityQueue.size do priorityQueue(i).isCanceled = true
		priorityQueue.clear() // do not keep unnecessary references after stopped to avoid unnecessary memory retention
	}

	def diagnose(sb: StringBuilder): StringBuilder = {
		sb.append("\t\tisRunning=").append(isRunning).append('\n')
		sb.append("\t\tpriorityQueueSize=").append(priorityQueue.size).append('\n')
	}
}
