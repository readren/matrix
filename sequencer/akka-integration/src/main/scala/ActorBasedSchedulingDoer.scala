package readren.sequencer.akka

import ActorBasedDoer.Procedure

import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{Behavior, Scheduler}
import readren.common.CompileTime.getTypeName
import readren.common.Maybe
import readren.sequencer.{MilliDuration, SchedulingExtension}

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.FiniteDuration
import scala.reflect.Typeable

object ActorBasedSchedulingDoer {

	abstract class Plan {
		private[ActorBasedSchedulingDoer] val wasActivated: AtomicBoolean = new AtomicBoolean(false)
		@volatile private[ActorBasedSchedulingDoer] var isCanceled: Boolean = false
	}

	case class SingleTime(delay: MilliDuration) extends Plan

	case class FixedRate(initialDelay: MilliDuration, interval: MilliDuration) extends Plan

	case class FixedDelay(initialDelay: MilliDuration, delay: MilliDuration) extends Plan

	/** A [[Behavior]] factory that provides access to an [[ActorBasedSchedulingDoer]] whose DoSiThEx (doer single thread executor) is the actor corresponding to the provided [[ActorContext]]. */
	def setup[A: Typeable](ctxA: ActorContext[A], timerScheduler: TimerScheduler[A])(frontier: ActorBasedSchedulingDoer => Behavior[A]): Behavior[A] = {
		val doer = buildActorBasedSchedulingDoer(ctxA.asInstanceOf[ActorContext[Procedure]], timerScheduler.asInstanceOf[TimerScheduler[Procedure]])
		val behaviorA = frontier(doer)
		val interceptor = ActorBasedDoer.buildProcedureInterceptor[A](doer)
		Behaviors.intercept(() => interceptor)(behaviorA).narrow
	}

	private def buildActorBasedSchedulingDoer[A >: Procedure](ctx: ActorContext[A], timerScheduler: TimerScheduler[A]): ActorBasedSchedulingDoer = {
		val actorBasedDoer = ActorBasedDoer.buildDoer(ctx)
		new ActorBasedSchedulingDoer {
			override val tag: Tag = ctx.self.path
			
			override def executeSequentially(runnable: Runnable): Unit =
				actorBasedDoer.executeSequentially(runnable)

			override def currentlyRunningDoer: Maybe[ActorBasedDoer] =
				Maybe.apply(ActorBasedDoer.currentDoerThreadLocal.get)

			override def reportFailure(cause: Throwable): Unit =
				actorBasedDoer.reportFailurePortal(cause)

			override def akkaScheduler: Scheduler =
				actorBasedDoer.akkaScheduler

			override type Schedule = Plan

			override def newDelaySchedule(delay: MilliDuration): SingleTime = SingleTime(delay)

			override def newFixedRateSchedule(initialDelay: MilliDuration, interval: MilliDuration): FixedRate = FixedRate(initialDelay, interval)

			override def newFixedDelaySchedule(initialDelay: MilliDuration, delay: MilliDuration): FixedDelay = FixedDelay(initialDelay, delay)

			override def scheduleSequentially(schedule: Schedule, routine: Schedule => Unit): Unit = {
				if schedule.wasActivated.getAndSet(true) then throw new IllegalStateException(s"The ${getTypeName[Schedule]} instance `$schedule` was already used before and can't be used twice.")
				else if !schedule.isCanceled then {
					val runnable: Runnable = () => routine(schedule)
					schedule match {
						case SingleTime(delay) => timerScheduler.startSingleTimer(schedule, Procedure(runnable), FiniteDuration(delay, TimeUnit.MILLISECONDS))
						case FixedRate(initialDelay, interval) => timerScheduler.startTimerAtFixedRate(schedule, Procedure(runnable), FiniteDuration(initialDelay, TimeUnit.MILLISECONDS), FiniteDuration(interval, TimeUnit.MILLISECONDS))
						case FixedDelay(initialDelay, delay) => timerScheduler.startTimerWithFixedDelay(schedule, Procedure(runnable), FiniteDuration(initialDelay, TimeUnit.MILLISECONDS), FiniteDuration(delay, TimeUnit.MILLISECONDS))
					}
				}
			}

			override def cancel(schedule: Schedule): Unit = {
				schedule.isCanceled = true
				timerScheduler.cancel(schedule)
			}

			override def cancelAll(): Unit = {
				timerScheduler.cancelAll()
			}

			override def wasActivated(schedule: Schedule): Boolean =
				schedule.wasActivated.get

			override def isCanceled(schedule: Schedule): Boolean = {
				schedule.isCanceled || schedule.wasActivated.get && !timerScheduler.isTimerActive(schedule)
			}

		}
	}
}

/** A [[Doer]], extended with scheduling and akka-actor related operations, whose DoSiThEx (doer single thread executor) is an akka-actor. */
abstract class ActorBasedSchedulingDoer extends ActorBasedDoer, SchedulingExtension  