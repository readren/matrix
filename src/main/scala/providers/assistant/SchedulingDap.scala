package readren.matrix
package providers.assistant

import core.MatrixDoer
import providers.assistant.SchedulingDap.*
import providers.assistant.CooperativeWorkersDap.*
import providers.doer.AssistantBasedDoerProvider

import readren.taskflow.SchedulingExtension.NanoDuration
import readren.taskflow.SchedulingExtension

import java.util
import java.util.concurrent.*
import java.util.concurrent.locks.ReentrantLock
import scala.annotation.tailrec
import scala.collection.mutable

object SchedulingDap {
	/** A nano time based on the [[System.nanoTime]] method. */
	type NanoTime = Long

	inline val INITIAL_DELAYED_TASK_QUEUE_CAPACITY = 16

	trait SchedulingAssistant extends DoerAssistant, SchedulingExtension.Assistant {
		override type Schedule <: AbstractSchedule
	}

	/** IMPORTANT: Represents a unique entity where equality and hash code must be based on identity. */
	trait AbstractSchedule {
		def initialDelay: NanoTime

		def interval: NanoDuration

		def isFixedRate: Boolean

		/** Exposes the time the [[Runnable]] is expected to be run.
		 * Updated after the [[Runnable]] execution is completed. */
		def scheduledTime: NanoTime

		/** Exposes the number of executions of the [[Runnable]] that were skipped before the current one due to processing power saturation or negative `initialDelay`.
		 * It is calculated based on the scheduled interval, and the difference between the actual [[startingTime]] and the scheduled time:
		 * {{{ (actualTime - scheduledTime) / interval }}}
		 * Updated before the [[Runnable]] is run.
		 * The value of this variable is used after the [[runnable]]'s execution completes to calculate the [[scheduledTime]]; therefore, the [[runnable]] may modify it to affect the resulting [[scheduledTime]] and therefore when it's next execution will be.
		 * Intended to be accessed only within the thread that is currently running the [[Runnable]] that is scheduled by this instance. */
		def numOfSkippedExecutions: Long

		/** Exposes the [[System.nanoTime]] when the current execution started.
		 * The [[numOfSkippedExecutions]] is calculated based on this time.
		 * Updated before the [[Runnable]] is run.
		 * Intended to be accessed only within the thread that is currently running the [[Runnable]] that is scheduled by this instance. */
		def startingTime: NanoTime

		/** An instance becomes enabled when the [[scheduledTime]] is reached, and it's [[runnable]] is enqueued in [[thisSchedulingAssistant.taskQueue]].
		 * An instance becomes disabled after the [[runnable]] execution finishes and the */
		def isEnabled: Boolean

		def enabledTime: NanoTime

		/** An instance becomes active when is passed to the [[thisSchedulingAssistant.scheduleSequentially]] method.
		 * An instances becomes inactive when it is passed to the [[thisSchedulingAssistant.cancel]] method or when [[thisSchedulingAssistant.cancelAll]] is called.
		 *
		 * Implementation note: This var may be replaced with {{{ def isActive = !isEnabled && heapIndex < 0}}} but that would require both [[isEnabled]] and [[heapIndex]] to be @volatile. */
		def isActive: Boolean
	}

}

/** A dynamically balanced [[Doer.Assistant]] provider with scheduling support.
 * This assistant provider is like [[CooperativeWorkersDap]] but with scheduling capabilities added.
 * Scheduling is managed by a dedicated single thread, which operates independently and does not contribute to the thread-pool size.
 * @param applyMemoryFence Determines whether memory fences are applied to ensure that store operations made by a task happen before load operations performed by successive tasks enqueued to the same [[Doer.Assistant]]. 
 * The application of memory fences is optional because no test case has been devised to demonstrate their necessity. Apparently, the ordering constraints are already satisfied by the surrounding code.
 */
class SchedulingDap(
	applyMemoryFence: Boolean = true,
	threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
	failureReporter: Throwable => Unit = _.printStackTrace(),
	threadFactory: ThreadFactory = Executors.defaultThreadFactory()
) extends CooperativeWorkersDap, AssistantBasedDoerProvider.DoerAssistantProvider[SchedulingAssistant] { thisSchedulingAssistantProvider =>


	override def provide(serial: MatrixDoer.Id): SchedulingAssistant = {
		new SchedulingAssistantImpl(serial)
	}

	private class SchedulingAssistantImpl(anId: MatrixDoer.Id) extends DoerAssistantImpl(anId), SchedulingAssistant { thisSchedulingAssistant =>

		override type Schedule = ScheduleImpl

		override def newDelaySchedule(delay: NanoDuration): Schedule =
			new ScheduleImpl(delay, 0L, false)

		override def newFixedRateSchedule(initialDelay: NanoDuration, interval: NanoDuration): Schedule =
			new ScheduleImpl(initialDelay, interval, true)

		override def newFixedDelaySchedule(initialDelay: NanoDuration, delay: NanoDuration): Schedule =
			new ScheduleImpl(initialDelay, delay, false)

		override def scheduleSequentially(schedule: Schedule, originalRunnable: Runnable): Unit = {
			val currentTime = System.nanoTime()
			assert(!schedule.isActive)
			schedule.isActive = true

			object fixedDelayWrapper extends Runnable {
				override def run(): Unit = {
					if schedule.isActive then {
						originalRunnable.run()
						// TODO analyze if the following lines must be in a `finally` block whose `try`'s body is `originalRunnable.run()`
						scheduler.schedule(schedule, System.nanoTime() + schedule.interval)
					}
				}
			}

			object fixedRateWrapper extends Runnable {
				override def run(): Unit = {
					@tailrec
					def loop(currentTime: NanoTime): Unit = {
						if schedule.isActive then {
							schedule.startingTime = currentTime
							schedule.numOfSkippedExecutions = (currentTime - schedule.scheduledTime) / schedule.interval
							originalRunnable.run()
							// TODO analyze if the following lines must be in a `finally` block whose `try`'s body is `originalRunnable.run()`
							val nextTime = schedule.scheduledTime + schedule.interval * (1L + schedule.numOfSkippedExecutions)
							val updatedCurrentTime = System.nanoTime()
							if nextTime <= updatedCurrentTime then {
								schedule.scheduledTime = nextTime
								loop(updatedCurrentTime)
							} else scheduler.schedule(schedule, nextTime)
						}
					}

					loop(System.nanoTime())
				}
			}
			schedule.runnable =
				if schedule.interval > 0 then {
					if schedule.isFixedRate then fixedRateWrapper
					else fixedDelayWrapper
				} else originalRunnable
			scheduler.schedule(schedule, currentTime + schedule.initialDelay, true)
		}

		override def cancel(schedule: Schedule): Unit = scheduler.cancel(schedule)

		override def cancelAll(): Unit = scheduler.cancelAllBelongingTo(thisSchedulingAssistant)

		/** An instance becomes active when is passed to the [[scheduleSequentially]] method.
		 * An instances becomes inactive when it is passed to the [[cancel]] method or when [[cancelAll]] is called. */
		override def isActive(schedule: Schedule): Boolean = schedule.isActive


		/** IMPORTANT: Represents a unique entity where equality and hash code must be based on identity. */
		class ScheduleImpl(override val initialDelay: NanoTime, override val interval: NanoDuration, override val isFixedRate: Boolean) extends AbstractSchedule {
			/** The [[Runnable]] that this [[TimersExtension.Assistant.Schedule]] schedules. */
			var runnable: Runnable | Null = null
			var scheduledTime: NanoTime = 0L
			/** The index of this instance in the array-based min-heap. */
			var heapIndex: Int = -1
			var numOfSkippedExecutions: Long = 0
			var startingTime: NanoTime = 0L
			var isEnabled = false
			var enabledTime: NanoTime = 0L
			@volatile var isActive = false

			inline def owner: thisSchedulingAssistant.type = thisSchedulingAssistant

			override def toString: String = readren.taskflow.deriveToString(this)
		}
	}

	private object scheduler extends Runnable {
		private val commandsQueue = new util.ArrayDeque[Runnable]()
		private var heap: Array[SchedulingAssistantImpl#ScheduleImpl | Null] = Array.fill(INITIAL_DELAYED_TASK_QUEUE_CAPACITY)(null)
		private var heapSize: Int = 0
		/** Know the instances of [[SchedulingAssistantImpl#ScheduleImpl]] that are enabled, which is necessary to implement the [[SchedulingAssistant.cancelAll()]] method because enabled instances are not in the [[heap]]. */
		private var enabledSchedulesByAssistant: util.HashMap[SchedulingAssistantImpl, mutable.HashSet[SchedulingAssistantImpl#ScheduleImpl]] = new util.HashMap()

		private var isRunning = true
		private val lock = new ReentrantLock()
		/**
		 * Condition signalled when a command is enqueued into [[commandsQueue]].
		 */
		private val commandPending = lock.newCondition()

		private val timeWaitingThread: Thread = threadFactory.newThread(this)
		timeWaitingThread.start()

		def numOfEnabledSchedules: Int = {
			var accum = 0
			enabledSchedulesByAssistant.forEach((_, set) => accum += set.size)
			accum
		}

		def schedule(schedule: SchedulingAssistantImpl#ScheduleImpl, scheduleTime: NanoTime, fresh: Boolean = false): Unit = {
			signal { () =>
				if schedule.isEnabled then {
					schedule.isEnabled = false
					enabledSchedulesByAssistant.computeIfPresent(schedule.owner, (_, enabledSchedules) => enabledSchedules.subtractOne(schedule))
				}
				if schedule.isActive then {
					schedule.scheduledTime = scheduleTime
					enqueue(schedule)
				}
			}
		}

		def cancel(schedule: SchedulingAssistantImpl#ScheduleImpl): Unit = {
			schedule.isActive = false
			signal { () =>
				if schedule.isEnabled then {
					schedule.isEnabled = false
					if enabledSchedulesByAssistant.computeIfPresent(schedule.owner, (_, enabledSchedules) => enabledSchedules.subtractOne(schedule)).isEmpty then
						enabledSchedulesByAssistant.remove(schedule.owner)
				} else remove(schedule)
			}
		}

		def cancelAllBelongingTo(assistant: SchedulingAssistantImpl): Unit = {
			signal { () =>
				var index = heapSize
				while index > 0 do {
					index -= 1
					val schedule = heap(index)
					if schedule.owner eq assistant then {
						schedule.isActive = false
						remove(schedule)
					}
				}

				val enabledSchedules = enabledSchedulesByAssistant.remove(assistant)
				if enabledSchedules != null then enabledSchedules.foreach { schedule =>
					schedule.isActive = false
					schedule.isEnabled = false
				}
			}
		}

		def stop(): Unit = {
			signal(() =>
				isRunning = false
			)
		}

		private def signal(command: Runnable): Unit = {
			lock.lock()
			commandsQueue.offer(command)
			commandPending.signal()
			lock.unlock()
		}

		override def run(): Unit = {
			while isRunning do {
				lock.lock()
				var command: Runnable | Null = commandsQueue.poll()
				lock.unlock()
				while command != null do {
					command.run()
					lock.lock()
					command = commandsQueue.poll()
					lock.unlock()
				}

				var earlierSchedule = peek
				var currentNanoTime = System.nanoTime()
				while earlierSchedule != null && earlierSchedule.scheduledTime <= currentNanoTime do {
					finishPoll(earlierSchedule)
					earlierSchedule.isEnabled = true
					earlierSchedule.enabledTime = currentNanoTime
					enabledSchedulesByAssistant.compute(
						earlierSchedule.owner,
						(_, enabledSchedules) => if enabledSchedules == null then mutable.HashSet(earlierSchedule) else enabledSchedules.addOne(earlierSchedule)
					)
					earlierSchedule.owner.executeSequentially(earlierSchedule.runnable)
					currentNanoTime = System.nanoTime()
					earlierSchedule = peek
				}
				lock.lock()
				try {
					if isRunning && commandsQueue.isEmpty then {
						if earlierSchedule == null then commandPending.await()
						else {
							val delay = earlierSchedule.scheduledTime - currentNanoTime
							earlierSchedule = null // do not keep unnecessary references while waiting to avoid unnecessary memory retention
							commandPending.awaitNanos(delay)
						}
					}
				} catch {
					case _: InterruptedException => isRunning = false
				}
				finally lock.unlock()
			}
			lock.lock()
			commandsQueue.clear() // do not keep unnecessary references while waiting to avoid unnecessary memory retention
			lock.unlock()
			for i <- 0 until heapSize do heap(i).isActive = false
			heap = null // do not keep unnecessary references while waiting to avoid unnecessary memory retention
			enabledSchedulesByAssistant.forEach { (_, enabledSchedules) =>
				enabledSchedules.foreach { schedule =>
					schedule.isActive = false
					schedule.isEnabled = false
				}
			}
			enabledSchedulesByAssistant = null // do not keep unnecessary references while waiting to avoid unnecessary memory retention
		}

		private inline def peek: SchedulingAssistantImpl#ScheduleImpl | Null = heap(0)

		/** Adds the provided element to this min-heap based priority queue. */
		private def enqueue(element: SchedulingAssistantImpl#ScheduleImpl): Unit = {
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
		private def finishPoll(peekedElement: SchedulingAssistantImpl#ScheduleImpl): SchedulingAssistantImpl#ScheduleImpl = {
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
		private def remove(element: SchedulingAssistantImpl#ScheduleImpl): Boolean = {
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

		private inline def indexOf(task: SchedulingAssistantImpl#ScheduleImpl): Int = task.heapIndex

		/**
		 * Replaces the element at position `holeIndex` of the heap-based array with the `providedElement` and rearranges it and its parents as necessary to ensure that all parents are less than or equal to their children.
		 * Note that for the entire heap to satisfy the min-heap property, the `providedElement` must be less than or equal to the children of `holeIndex`.
		 * Sifts element added at bottom up to its heap-ordered spot.
		 */
		private def siftUp(holeIndex: Int, providedElement: SchedulingAssistantImpl#ScheduleImpl): Unit = {
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
		private def siftDown(holeIndex: Int, providedElement: SchedulingAssistantImpl#ScheduleImpl): Unit = {
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
	 * Makes this [[Matrix.DoerAssistantProvider]] to shut down when all the workers are sleeping.
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
		sb.append(utils.CompileTime.getTypeName[SchedulingDap]).append('\n')
		sb.append("\tscheduler:\n")
		scheduler.diagnose(sb)
		super.diagnose(sb)
	}
}
