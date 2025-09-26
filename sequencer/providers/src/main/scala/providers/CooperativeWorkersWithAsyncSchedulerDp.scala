package readren.sequencer
package providers

import DoerProvider.Tag
import providers.CooperativeWorkersDp.*
import providers.CooperativeWorkersWithAsyncSchedulerDp.*

import readren.common.CompileTime.getTypeName
import readren.common.Maybe

import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicBoolean
import scala.language.adhocExtensions

object CooperativeWorkersWithAsyncSchedulerDp extends CooperativeWorkersDpWithSchedulerCompanion {

	class Impl(
		applyMemoryFence: Boolean = true,
		threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
		failureReporter: (Doer, Throwable) => Unit = DefaultDoerFaultReporter(true),
		unhandledExceptionReporter: (Doer, Throwable) => Unit = DefaultDoerFaultReporter(false),
		threadFactory: ThreadFactory = Executors.defaultThreadFactory()
	) extends CooperativeWorkersWithAsyncSchedulerDp(applyMemoryFence, threadPoolSize, threadFactory) {
		/** Called when a routine passed to the [[Doer.executeSequentially]] method of a provided [[Doer]] throws an exception. */
		override protected def onUnhandledException(doer: Doer, exception: Throwable): Unit = unhandledExceptionReporter(doer, exception)

		/** Called when the [[Doer.reportFailure]] method of a provided [[Doer]] is called. */
		override protected def onFailureReported(doer: Doer, failure: Throwable): Unit = failureReporter(doer, failure)
	}
}

/** Adds scheduling features to the [[CooperativeWorkersDp]].
 * The scheduling is managed by a [[ThreadDrivenScheduler]] which uses a dedicated thread that operates independently and does not contribute to the thread-pool size.
 * @param applyMemoryFence Determines whether memory fences are applied to ensure that store operations made by a task happen before load operations performed by successive tasks enqueued to the same [[Doer]].
 * The application of memory fences is optional because no test case has been devised to demonstrate their necessity. Apparently, the ordering constraints are already satisfied by the surrounding code.
 */
abstract class CooperativeWorkersWithAsyncSchedulerDp(
	applyMemoryFence: Boolean = true,
	threadPoolSize: Int = Runtime.getRuntime.availableProcessors(),
	threadFactory: ThreadFactory = Executors.defaultThreadFactory()
) extends CooperativeWorkersDp, DoerProvider[SchedulingDoerFacade] { thisSchedulingDoerProvider =>

	private val scheduler = new ThreadDrivenScheduler[SchedulingDoerImpl, SchedulingDoerImpl#ScheduleImpl](threadFactory)

	override def provide(tag: Tag): SchedulingDoerFacade = {
		startAllWorkersIfNotAlready()
		new SchedulingDoerImpl(tag)
	}

	override def currentDoer: Maybe[SchedulingDoerFacade] = super.currentDoer.asInstanceOf[Maybe[SchedulingDoerFacade]]

	private class SchedulingDoerImpl(aTag: Tag) extends DoerImpl(aTag), SchedulingDoerFacade { thisSchedulingDoer =>

		override type Schedule = ScheduleImpl

		override def newDelaySchedule(delay: MilliDuration): Schedule =
			new ScheduleImpl(delay, 0L, false)

		override def newFixedRateSchedule(initialDelay: MilliDuration, interval: MilliDuration): Schedule =
			new ScheduleImpl(initialDelay, interval, true)

		override def newFixedDelaySchedule(initialDelay: MilliDuration, delay: MilliDuration): Schedule =
			new ScheduleImpl(initialDelay, delay, false)

		override def scheduleSequentially(schedule: Schedule, routine: Schedule => Unit): Unit = {
			val activationTime = nanosToMillisRoundedUp(System.nanoTime)
			if schedule.activated.getAndSet(true) then throw new IllegalStateException(s"The ${getTypeName[Schedule]} instance `$schedule` was already used before and can't be used twice.")
			else if !schedule.isCanceled then {
				schedule.runnable = new Runnable {
					override def run(): Unit = {
						if !schedule.isCanceled then {
							routine(schedule)
							if schedule.interval > 0 && !schedule.isCanceled then {
								if schedule.isFixedRate then scheduler.scheduleRelativeToPrevious(schedule, schedule.interval)
								else scheduler.schedule(schedule, nanosToMillisRoundedUp(System.nanoTime()) + schedule.interval)
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
		class ScheduleImpl(override val initialDelay: MilliDuration, override val interval: MilliDuration, override val isFixedRate: Boolean) extends ThreadDrivenScheduler.Plan[SchedulingDoerImpl](thisSchedulingDoer), ScheduleFacade {
			val activated: AtomicBoolean = AtomicBoolean(false)

			override def wasActivated: Boolean = activated.get
		}
	}



	/**
	 * Makes this [[CooperativeWorkersWithAsyncSchedulerDp]] to shut down when all the workers are sleeping.
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
		sb.append(getTypeName[CooperativeWorkersWithAsyncSchedulerDp]).append('\n')
		sb.append("\tscheduler:\n")
		scheduler.diagnose(sb)
		super.diagnose(sb)
	}
}
