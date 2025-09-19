package readren.sequencer
package providers

import providers.ThreadDrivenScheduler.{INITIAL_DELAYED_TASK_QUEUE_CAPACITY, Plan, nanosToMillisRoundedDown}

import readren.common.deriveToString

import java.util
import java.util.concurrent.*
import scala.collection.mutable
import scala.reflect.ClassTag

object ThreadDrivenScheduler {


	inline val INITIAL_DELAYED_TASK_QUEUE_CAPACITY = 16


	/** IMPORTANT: Represents a unique entity where equality and hash code must be based on identity. */
	abstract class Plan[+D <: Doer](val owner: D) {
		val initialDelay: MilliDuration
		val interval: MilliDuration
		val isFixedRate: Boolean
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

		/** Executes the routine associated to this [[ScheduleImpl]] */
		inline def execute(): Unit = routine(this)

		override def toString: String = deriveToString(this) + s" scheduledTime: $scheduledTime"
	}

	inline def nanosToMillisRoundedUp(nanos: Long): Long = (nanos + 999_999) / 1_000_000

	inline def nanosToMillisRoundedDown(nanos: Long): Long = nanos / 1_000_000

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
	private var heap: Array[P | Null] = new Array(INITIAL_DELAYED_TASK_QUEUE_CAPACITY)
	private var heapSize: Int = 0
	/** A register that knows the instances of [[Plan]] that are not in the [[heap]] because they were triggered and still not rescheduled (still not added to the [[heap]] again after the routine completes and [[schedule]] or [[scheduleRelativeToPrevious]] is called again).
	 * We say that a schedule is triggered when its [[Plan.routine]] is passed to [[Doer.executeSequentially]].
	 * A schedule is triggered only after its [[Plan.scheduledTime]] is reached.
	 * This register is necessary to implement the [[cancelAllBelongingTo]] method because enqueued instances are not in the [[heap]].
	 * The goal of this register is to know all the [[Plan]] instances that are activated but not contained in the [[heap]]. */
	private var triggeredSchedulesByDoer: util.HashMap[D, mutable.HashSet[P]] = new util.HashMap()

	private var isRunning = true

	private val timeWaitingThread: Thread = threadFactory.newThread(this)
	timeWaitingThread.start()

	def numOfEnabledSchedules: Int = {
		var accum = 0
		triggeredSchedulesByDoer.forEach((_, set) => accum += set.size)
		accum
	}

	/** Schedules a single execution of the routine associated to the specified [[Plan]] at a specified time. */
	def schedule(schedule: P, scheduleTime: MilliTime): Unit = {
		signal { () =>
			removeFromRegister(schedule)
			if !schedule.isCanceled then {
				schedule.scheduledTime = scheduleTime
				if scheduleTime * 1_000_000 <= System.nanoTime() then triggeredForExecution(schedule)
				else appoint(schedule)
			}
		}
	}

	/** Schedules a single execution of the routine associated to the specified [[Plan]] at a time relative to the previous schedule time. */
	def scheduleRelativeToPrevious(schedule: P, interval: MilliDuration): Unit = {
		signal { () =>
			removeFromRegister(schedule)
			if !schedule.isCanceled then {
				schedule.scheduledTime += interval
				if schedule.scheduledTime * 1_000_000 <= System.nanoTime() then triggeredForExecution(schedule)
				else appoint(schedule)
			}
		}
	}

	def cancel(schedule: P): Unit = {
		schedule.isCanceled = true
		signal { () =>
			if !removeFromRegister(schedule) then remove(schedule)
		}
	}

	/** Cancels oll the activated [[Plan]] instances corresponding to a [[Doer]]. */
	def cancelAllBelongingTo(doer: D): Unit = {
		signal { () =>
			var index = heapSize
			while index > 0 do {
				index -= 1
				val schedule = heap(index).asInstanceOf[P]
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

	/** TODO this design is inefficient because it blocks and requires the creation of a [[Runnable]] instance for the command in every call. Consider an improvement. May be using an array of reusable commands.
	 * TODO Another more simple path would be to use a Doer instead of a dedicated thread to manage the scheduling. It also creates an object for the command, but no extra thread is required and no synchronization blocking. The problem is that the commands execution would be queued after regular tasks (unless Doer prioritization is added).
	 * TODO Another more complex path that avoids blocking would be to use
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
			if triggeredSchedulesByDoer.computeIfPresent(schedule.owner, (_, enabledSchedules) => enabledSchedules.subtractOne(schedule)).isEmpty then
				triggeredSchedulesByDoer.remove(schedule.owner)
			true
		} else false
	}

	private def triggeredForExecution(es: P): Unit = {
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
				val es = earlierSchedule.asInstanceOf[P]
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

	private inline def peek: P | Null = heap(0)

	/** Adds the provided element to this min-heap based priority queue. */
	private def appoint(element: P): Unit = {
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
	private def finishPoll(peekedElement: P): P = {
		heapSize -= 1
		val s = heapSize
		val replacement = heap(s).asInstanceOf[P]
		heap(s) = null
		if s != 0 then siftDown(0, replacement)
		peekedElement.heapIndex = -1
		peekedElement
	}

	/** Removes the provided element from this queue.
	 * @return true if the element was removed; false if it is not contained by this queue. */
	private def remove(element: P): Boolean = {
		val elemIndex = indexOf(element)
		if elemIndex < 0 then return false
		element.heapIndex = -1
		heapSize -= 1
		val s = heapSize
		val replacement = heap(s).asInstanceOf[P]
		heap(s) = null
		if s != elemIndex then {
			siftDown(elemIndex, replacement)
			if heap(elemIndex) eq replacement then siftUp(elemIndex, replacement)
		}
		true
	}

	private inline def indexOf(element: P): Int = element.heapIndex

	/**
	 * Replaces the element at position `holeIndex` of the heap-based array with the `providedElement` and rearranges it and its parents as necessary to ensure that all parents are less than or equal to their children.
	 * Note that for the entire heap to satisfy the min-heap property, the `providedElement` must be less than or equal to the children of `holeIndex`.
	 * Sifts element added at bottom up to its heap-ordered spot.
	 */
	private def siftUp(holeIndex: Int, providedElement: P): Unit = {
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
	private def siftDown(holeIndex: Int, providedElement: P): Unit = {
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

		heap = util.Arrays.copyOf[P | Null](heap, newCapacity)
	}

	def diagnose(sb: StringBuilder): StringBuilder = {
		sb.append("\t\tisRunning=").append(isRunning).append('\n')
		sb.append("\t\theapSize=").append(heapSize).append('\n')
		sb.append("\t\tnumOfEnabledSchedules=").append(numOfEnabledSchedules).append('\n')
	}
}
