package readren.sequencer

import SchedulingExtension.MilliDuration

import readren.common.Maybe
import readren.sequencer.Doer

import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}


object SchedulingExtension {

	/** A duration in milliseconds. */
	type MilliDuration = Long
}

/** Extends the [[Doer]] trait and its [[Duty]] and [[Task]] inner traits with scheduling operations.
 *
 * The abstract methods specifies what an instance of [[Doer]] extended with the [[SchedulingExtension]] requires to exist.
 *
 * Design note: Why not avoid the vulnerability "using the same instance of Schedule in two calls to scheduleSequentially is illegal" by making Schedule only describe the schedule, and representing the execution plan by a separate trait Plan, with instances returned by the scheduleSequentially operation?
 * Because this would require operations on Duty and Task that use scheduleSequentially to include an instance of Plan along with the result.
 * This would necessitate a tuple, which not only requires additional memory allocation but also complicates the chaining of operations.
 * Wait! There is a way. See [[Schedule]]
 *
 *
 * @define notReusableDuty CAUTION: the [[Duty]] instance returned by this method should not be reused. It is mutable because it depends on an instance of [[SchedulingExtension.Schedule]] which mutate when [[schedule]] is executed. // TODO avoid this limitation enforcing the implementation of Schedule be immutable or apparently immutable.
 * @define notReusableTask CAUTION: the [[Task]] instance returned by this method should not be reused. It is mutable because it depends on an instance of [[SchedulingExtension.Schedule]] which mutate when [[schedule]] is executed. // TODO avoid this limitation enforcing the implementation of Schedule be immutable or apparently immutable.
 * */
trait SchedulingExtension { thisSchedulingExtension: Doer =>


	/** Represents an execution schedule.
	 * It is tied to the [[Runnable]] passed along it to the [[schedule]] method. This means that it is mutable and, therefore, non referentially transparent and illegal to use the same instance in more than one call to [[schedule]].
	 * Given all the operations added to [[Duty]] and [[Task]] by this extension ([[SchedulingExtension]]) rely explicitly or implicitly on a [[Schedule]] instance, they all are also not referentially transparent.
	 * TODO: avoid the limitation of using the same instance in more than one call to [[schedule]], by enforcing [[Schedule]] to be referentially transparent. This change requires that instances of [[Schedule]] instances to be associated to all the [[Runnable]]s that accompany it in a calls to [[schedule]], and that the `cancel` method to apply to all of them. */
	type Schedule

	/** Creates a [[Schedule]] for a single time execution after a delay.
	 * @param delay duration before the execution.
	 * @return a [[Schedule]] instance intended solely as an argument for a single call to the [[schedule]] method. */
	def newDelaySchedule(delay: MilliDuration): Schedule

	/** Creates a [[Schedule]] for a fixed rate repeated execution after an initial delay.
	 * @param initialDelay duration before the first execution.
	 * @param interval duration between the scheduled time of the executions.
	 * @return a [[Schedule]] instance intended solely as an argument for a single call to the [[schedule]] method. */
	def newFixedRateSchedule(initialDelay: MilliDuration, interval: MilliDuration): Schedule

	/** Creates a [[Schedule]] for a fixed delay repeated execution after an initial delay.
	 * @param initialDelay duration before the first execution.
	 * @param delay duration between the end of an execution and the scheduled start of the next.
	 * @return a [[Schedule]] instance intended solely as an argument for a single call to the [[schedule]] method. */
	def newFixedDelaySchedule(initialDelay: MilliDuration, delay: MilliDuration): Schedule

	/** Programs the execution of the provided [[Runnable]] according to the provided [[Schedule]].
	 * The implementation must ensure mutual sequentiality of the execution of [[Runnable]]s passed to both, this method and [[executeSequentially]].
	 * @param schedule determines when the provided [[runnable]] will be run.
	 * @param runnable the [[Runnable]] to be run according to the provided [[schedule]].
	 * The implementation should not throw non-fatal exceptions. */
	def scheduleSequentially(schedule: Schedule, runnable: Runnable): Unit

	/**
	 * The implementation should stop the [[Schedule]] from triggering any executions. Even if it was not activated jet.
	 * When this method is called within the thread currently assigned to this [[Doer]], the [[SchedulingExtension]] implementation should ensure that the [[Schedule]] will not trigger any execution after this method is called.
	 * If, on the contrary, this method is executed by a thread other than the one currently assigned to this [[Doer]], the [[SchedulingExtension]] implementation should strive to prevent the [[Schedule]] from triggering any execution except possibly a single one if this method is called close to its scheduled time.
	 * The implementation should not throw non-fatal exceptions. */
	def cancel(schedule: Schedule): Unit

	/**
	 * The implementation should [[cancel]] all the active instances of [[Schedule]] of this [[Doer]]. See [[isActive]].
	 * The implementation should not throw non-fatal exceptions. */
	def cancelAll(): Unit

	/** @return true if the [[Schedule]] was used in a call to [[scheduleSequentially]] and is not cancelled nor elapsed (only instances returned by [[newDelaySchedule]] can elapse). */
	def isActive(schedule: Schedule): Boolean

	inline def schedule(schedule: Schedule)(runnable: Runnable): Unit =
		scheduleSequentially(schedule, runnable)

	//// Duty extension ////

	extension [A](thisDuty: Duty[A]) {

		/** Returns a [[Duty]] that behaves the same as `thisDuty`, but its execution starts only after the delay specified by the provided schedule, measured from the moment it is triggered.
		 * If the provided [[Schedule]] is a fixed rate/delay, triggering the returned [[Duty]] causes `thisDuty` to execute repeatedly until the `schedule` is canceled.
		 * $notReusableDuty */
		inline def scheduled(schedule: Schedule): Duty[A] =
			new Scheduled(thisDuty, schedule)


		/** Returns a [[Duty]] that behaves the same as `thisDuty`, but its execution starts only after the provided `delay`, measured from the moment it is triggered.
		 * $notReusableDuty */
		inline def delayed(delay: MilliDuration): Duty[A] =
			scheduled(newDelaySchedule(delay))

		/** Returns a [[Duty]] that behaves the same as `thisDuty`, but its execution starts only after the provided `delay`, measured from the moment it is triggered.
		 * $notReusableDuty */
		inline def delayed(delay: FiniteDuration): Duty[A] =
			delayed(delay.toMillis)

		/** Like [[Duty.map]] but the function application is scheduled.
		 * Note that what is scheduled is the start of the function application, not the execution of `thisDuty`. The schedule's delay occurs after the execution of `thisDuty` and before the application of `f` to its result.
		 * If the provided `schedule` is a fixed rate then triggering the returned duty causes the function application be executed periodically until the `schedule` is canceled.
		 * Is equivalent to {{{ thisDuty.flatMap(a => Duty.ready(a).map(f).scheduled(schedule)) }}} but more efficient.
		 * $notReusableDuty*/
		def mapScheduled[B](aSchedule: Schedule)(f: A => B): Duty[B] = new Duty[B] {
			override def engage(onComplete: B => Unit): Unit = {
				thisDuty.engagePortal { a => schedule(aSchedule) { () => onComplete(f(a)) } }
			}
		}

		/** Like [[Duty.map]] but the function application is delayed.
		 * Note that what is delayed is the start of function application, not the start of the execution of `thisDuty`. The delay occurs after the execution of `thisDuty` and before the application of `f` to its result.
		 * Is equivalent to {{{ thisDuty.flatMap(a => Duty.ready(a).map(f).delayed(delay)) }}} but more efficient.
		 * $notReusableDuty*/
		inline def mapDelayed[B](delay: MilliDuration)(f: A => B): Duty[B] =
			mapScheduled(newDelaySchedule(delay))(f)

		/** Like [[Duty.map]] but the function application is delayed.
		 * Note that what is delayed is the start of function application, not the start of the execution of `thisDuty`. The delay occurs after the execution of `thisDuty` and before the application of `f` to its result.
		 * Is equivalent to {{{ thisDuty.flatMap(a => Duty.ready(a).map(f).delayed(delay)) }}} but more efficient.
		 * $notReusableDuty */
		inline def mapDelayed[B](delay: FiniteDuration)(f: A => B): Duty[B] =
			mapDelayed(delay.toMillis)(f)


		/** Like [[Duty.flatMap]] but the function application is scheduled.
		 * Note that what is scheduled is the function application, not the execution of `thisDuty`. The schedule's delay occurs after the execution of `thisDuty` and before the application of `f` to its result.
		 * If the provided `schedule` is a fixed rate then triggering the returned duty causes the function application be executed periodically until the `schedule` is canceled.
		 * Is equivalent to {{{ thisDuty.flatMap(a => Duty.ready(a).flatMap(f).scheduled(schedule)) }}} but more efficient.
		 * $notReusableDuty*/
		def flatMapScheduled[B](aSchedule: Schedule)(f: A => Duty[B]): Duty[B] = new Duty[B] {
			override def engage(onComplete: B => Unit): Unit = {
				thisDuty.engagePortal { a => schedule(aSchedule) { () => f(a).engagePortal(onComplete) } }
			}
		}

		/** Like [[Duty.flatMap]] but the function application is delayed.
		 * Note that what is delayed is the function application, not the execution of `thisDuty`. The delay occurs after the execution of `thisDuty` and before the application of `f` to its result.
		 * Is equivalent to {{{ thisDuty.flatMap(a => Duty.ready(a).flatMap(f).delayed(delay)) }}} but more efficient.
		 * $notReusableDuty */
		def flatMapDelayed[B](delay: FiniteDuration)(f: A => Duty[B]): Duty[B] =
			flatMapScheduled(newDelaySchedule(delay.toMillis))(f)

		/**
		 * Returns a [[Duty]] that behaves the same as `thisDuty` but wraps its result in [[Maybe.some]] if the duty's execution takes less time than the delay of the provided [[Schedule]].
		 * Otherwise, the result is [[Maybe.empty]].
		 * $notReusableDuty
		 *
		 * Canceling the `schedule` within the [[Doer]] `DoSiThEx` before the delay elapses effectively removes the time constraint, treating the timeout's delay as infinite.
		 * The provided [[Schedule]] may be a fixed rate one, in which case triggering the returned duty would cause an inert operation be executed periodically until the schedule is cancelled.
		 *
		 * @param schedule a [[Schedule]] whose delay is the maximum duration allowed for the duty to complete before returning [[Maybe.empty]].
		 * @return a [[Duty]] that wraps the result of this duty with [[Maybe.some]] if `thisDuty` completes within the timeout, or completes with [[Maybe.empty]] when the timeout elapses.
		 */
		inline def timeLimited(schedule: Schedule): Duty[Maybe[A]] = {
			new TimeLimited[A, Maybe[A]](thisDuty, schedule, identity)
		}

		/**
		 * Returns a [[Duty]] that behaves the same as `thisDuty` but wraps its result in [[Maybe.some]] if the duty's execution takes less time than the provided `timeout`.
		 * Otherwise, the result is [[Maybe.empty]].
		 * $notReusableDuty
		 *
		 * @param timeout the maximum duration allowed for the duty to complete before returning [[Maybe.empty]].
		 * @return a [[Duty]] that wraps the result of this duty with [[Maybe.some]] if the task completes within the timeout, or completes with [[Maybe.empty]] when the timeout elapses.
		 */
		inline def timeLimited(timeout: FiniteDuration): Duty[Maybe[A]] =
			new TimeLimited[A, Maybe[A]](thisDuty, newDelaySchedule(timeout.toMillis), identity)

		/**
		 * Returns a [[Duty]] that behaves the same as `thisDuty` but retries its execution if it does not complete within the delay of the specified [[Schedule]].
		 * The duty will be retried until it completes within the `timeout` or the maximum number of retries (`maxRetries`) is reached, whichever occurs first.
		 * If this duty has side effects, they will be performed once for the initial execution and once for each retry, resulting in a total of one plus the number of retries.
		 * $notReusableDuty
		 *
		 * @param timeout    the maximum duration to allow for each execution of the duty before it is retried.
		 * @param maxRetries the maximum number of retries allowed.
		 * @return           a [[Task]] that produces [[Maybe[A]]] indicating the result of the task execution, or [[Maybe.empty]] if it fails to complete within the allowed retries.
		 */
		def retriedWhileTimeout(timeout: Schedule, maxRetries: Int): Duty[Maybe[A]] = {
			thisDuty.timeLimited(timeout).repeatedUntilSome(Integer.MAX_VALUE) { (retries, result) =>
				result.fold {
					if retries < maxRetries then Maybe.empty
					else Maybe.some(Maybe.empty)
				}(a => Maybe.some(Maybe.some(a)))
			}
		}
	}

	final class Scheduled[A, B](duty: Duty[A], aSchedule: Schedule) extends Duty[A] {
		override def engage(onComplete: A => Unit): Unit =
			schedule(aSchedule)(() => duty.engagePortal(onComplete))
	}

	/** Used by [[timeLimited]] and [[timeBounded]].
	 */
	final class TimeLimited[A, B](duty: Duty[A], timeout: Schedule, f: Maybe[A] => B) extends Duty[B] {
		override def engage(onComplete: B => Unit): Unit = {
			var hasElapsed = false
			var hasCompleted = false
			duty.trigger(true) { a =>
				if (!hasElapsed) {
					cancel(timeout)
					hasCompleted = true
					onComplete(f(Maybe.some(a)))
				}
			}
			schedule(timeout) {
				() =>
					if (!hasCompleted) {
						hasElapsed = true
						onComplete(f(Maybe.empty))
					}
			}
		}
	}

	extension (companion: Duty.type) {
		/** Creates a [[Duty]] that does nothing for the specified `duration`.
		 * Is equivalent to both {{{delay(duration)(()=>()) }}} and {{{Duty.unit.delayed(duration)}}}
		 * $notReusableDuty */
		inline def waits(duration: MilliDuration): Duty[Unit] =
			delay(duration)(()=>())
		
		/** @return a [[Duty]] that executes a supplier and yields its result according to a [[Schedule]] since it is triggered.
		 * If the schedule is a fixed rate/delay then the supplier and down-chained operations are executed repeatedly until the schedule is canceled.
		 * $notReusableDuty */
		inline def schedule[A](schedule: Schedule)(supplier: () => A): Duty[A] =
			new Book(schedule, supplier)

		/** $notReusableDuty
		 *  @return a [[Duty]] that executes a supplier and yields its result after a delay since it is triggered. */
		inline def delay[A](duration: MilliDuration)(supplier: () => A): Duty[A] =
			new Book(newDelaySchedule(duration), supplier)

		/** $notReusableDuty
		 *  @return a [[Duty]] that executes a supplier and yields its result after a delay since it is triggered. */
		inline def delay[A](duration: FiniteDuration)(supplier: () => A): Duty[A] =
			delay(duration.toMillis)(supplier)
	}

	final class Book[A](aSchedule: Schedule, supplier: () => A) extends Duty[A] {
		override def engage(onComplete: A => Unit): Unit =
			schedule(aSchedule)(() => onComplete(supplier()))
	}

	//// Task extension ////

	extension [A](thisTask: Task[A]) {

		/** Like [[Duty.scheduled]] but for [[Task]]s.
		 * $notReusableTask */
		def appointed(aSchedule: Schedule): Task[A] = new Task[A] {
			override def engage(onComplete: Try[A] => Unit): Unit = {
				schedule(aSchedule)(() => thisTask.engagePortal(onComplete))
			}
		}

		/** Returns a [[Task]] that behaves the same as `thisTask`, but its execution starts only after the provided `delay`, measured from the moment it is triggered.
		 * $notReusableTask */
		inline def postponed(delay: MilliDuration): Task[A] =
			appointed(newDelaySchedule(delay))

		/** Returns a [[Task]] that behaves the same as `thisTask`, but its execution starts only after the provided `delay`, measured from the moment it is triggered. */
		inline def postponed(delay: FiniteDuration): Task[A] =
			postponed(delay.toMillis)

		/** Like [[Task.map]] but the function application is scheduled.
		 * Note that what is scheduled is the start of the function application, not the execution of `thisTask`. The schedule's delay occurs after the execution of `thisTask` and only if it is successful.
		 * If the provided `schedule` is a fixed rate then triggering the returned task causes the function application be executed periodically until the `schedule` is canceled.
		 * Is equivalent to {{{ thisTask.map(a => Task.successful(a).map(f).appointed(schedule)) }}} but more efficient.
		 * $notReusableTask */
		def mapAppointed[B](aSchedule: Schedule)(f: A => B): Task[B] = new Task[B] {
			override def engage(onComplete: Try[B] => Unit): Unit = {
				thisTask.engagePortal {
					case Success(a) => schedule(aSchedule) { () => onComplete(Try(f(a))) }
					case fe@Failure(e) => onComplete(fe.asInstanceOf[Failure[B]])	
				}
			}
		}

		/** Like [[Task.map]] but the function application is delayed.
		 * Note that what is delayed is the start of function application, not the start of the execution of `thisTask`. The delay occurs after the execution of `thisTask` and before the application of `f` to its result.
		 * Is equivalent to {{{ thisTask.map(a => Task.successful(a).map(f).appointed(schedule))  }}} but more efficient.
		 * $notReusableTask */
		inline def mapPostponed[B](delay: MilliDuration)(f: A => B): Task[B] =
			mapAppointed(newDelaySchedule(delay))(f)

		/** Like [[Task.map]] but the function application is delayed.
		 * Note that what is delayed is the start of function application, not the start of the execution of `thisTask`. The delay occurs after the execution of `thisTask` and before the application of `f` to its result.
		 * Is equivalent to {{{ thisTask.map(a => Task.successful(a).map(f).appointed(schedule))  }}} but more efficient.
		 * $notReusableTask */
		inline def mapPostponed[B](delay: FiniteDuration)(f: A => B): Task[B] =
			mapPostponed(delay.toMillis)(f)

		/**
		 * Returns a [[Task]] that behaves the same as `thisTask` but wraps its result in [[Maybe.some]] if the task's execution takes less time than the delay of the provided [[Schedule]].
		 * Otherwise, the result is [[Maybe.empty]].
		 *
		 * Canceling the `schedule` within the [[Doer]] `DoSiThEx` before the delay elapses effectively removes the time constraint, treating the timeout's delay as infinite.
		 * The provided [[Schedule]] may be a fixed rate one, in which case triggering the returned task would cause an inert operation be executed periodically until the schedule is cancelled.
		 * $notReusableTask
		 *
		 * @param schedule a [[Schedule]] whose delay is the maximum duration allowed for the `thisTask` to complete before returning [[Maybe.empty]].
		 * @return a [[Duty]] that wraps the result of this duty with [[Maybe.some]] if the task completes within the timeout, or completes with [[Maybe.empty]] when the timeout elapses.
		 */
		def timeBounded(schedule: Schedule): Task[Maybe[A]] = new Task[Maybe[A]] {
			override def engage(onComplete: Try[Maybe[A]] => Unit): Unit = {
				val timeLimitedDuty = new TimeLimited[Try[A], Try[Maybe[A]]](thisTask, schedule, mtA => mtA.fold(Success(Maybe.empty))(_.map(Maybe.some)))
				timeLimitedDuty.engagePortal(onComplete)
			}
		}

		/**
		 * Returns a [[Task]] that behaves the same as `thisTask` but wraps its result in [[Maybe.some]] if the task's execution takes less time than `timeout`.
		 * Otherwise, the result is [[Maybe.empty]].
		 * $notReusableTask
		 *
		 * @param timeout the maximum duration allowed for the `thisTask` to complete before returning [[Maybe.empty]].
		 * @return a [[Task]] that wraps the result of this task with [[Maybe.some]] if the task completes within the timeout, or completes with [[Maybe.empty]] when the timeout elapses.
		 */
		def timeBounded(timeout: FiniteDuration): Task[Maybe[A]] =
			timeBounded(newDelaySchedule(timeout.toMillis))

		/**
		 * Returns a [[Task]] that behaves the same as `thisTask` but retries its execution if it does not complete within the specified `timeout`.
		 * The task will be retried until it completes within the `timeout` or the maximum number of retries (`maxRetries`) is reached, whichever occurs first.
		 * If this task has side effects, they will be performed once for the initial execution and once for each retry, resulting in a total of one plus the number of retries.
		 * $notReusableTask
		 *
		 * @param timeout the maximum duration to allow for each execution of the task before it is retried.
		 * @param maxRetries the maximum number of retries allowed.
		 * @return a [[Task]] that produces [[Maybe[A]]] indicating the result of the task execution, or [[Maybe.empty]] if it fails to complete within the allowed retries
		 */
		def reiteratedWhileTimeout(timeout: FiniteDuration, maxRetries: Int): Task[Maybe[A]] = {
			thisTask.timeBounded(newDelaySchedule(timeout.toMillis)).reiteratedHardyUntilSome[Maybe[A]](Integer.MAX_VALUE) { (retries, result) =>
				result match {
					case Success(mA) =>
						mA.fold {
							if retries < maxRetries then Maybe.empty
							else Maybe.some(Success(Maybe.empty))
						}(a => Maybe.some(Success(Maybe.some(a))))
					case Failure(cause) =>
						Maybe.some(Failure(cause))
				}
			}
		}
	}

	/** Truco para agregar operaciones al objeto [[AmigoFutures.Task]]. Para que funcione se requiere que esta clase esté importada. */
	extension (companion: Task.type) {

		/** Creates a [[Task]] that does nothing for the specified `duration`.
		 * $notReusableTask*/
		inline def sleeps(duration: MilliDuration): Task[Unit] = {
			Task.unit.postponed(duration)
		}
		
		/** Creates a [[Task]] that does nothing for the specified `duration`.
		 * $notReusableTask */
		inline def sleeps(duration: FiniteDuration): Task[Unit] = {
			Task.unit.postponed(duration)
		}

		def appoint[A](aSchedule: Schedule)(supplier: () => A): Task[A] = new Task[A] {
			override def engage(onComplete: Try[A] => Unit): Unit = {
				schedule(aSchedule) { () =>
					try onComplete(Success(supplier()))
					catch {
						case NonFatal(e) => onComplete(Failure(e))
					}
				}
			}
		}

		/** Creates a task, let's call it "loop", which when executed builds a task using `taskBuilder` and triggers it. If it takes more time than the given `timeout` to complete, triggers it again.
		 * This cycle repeats until the execution time of the task does not exceed the `timeout`, or the retries are exhausted.
		 * The execution of the loop task will complete when:
		 * - the time taken by the task to complete is within the `timeout`, in which case the result of the loop task will be `Some(taskResult)`,
		 * - the retries are exhausted, in which case the result of the loop task will be `None`.
		 * $notReusableTask */
		def retryWhileTimeout[A](maxRetries: Int, timeout: FiniteDuration)(taskBuilder: Int => Task[Try[A]]): Task[Maybe[A]] = {
			companion.attemptUntilRight[Unit, A](maxRetries) { attemptsAlreadyMade =>
				val task: Task[Try[A]] = taskBuilder(attemptsAlreadyMade)
				task.timeBounded(timeout).transform {
					case Success(mtA) =>
						mtA.fold(Success(Left(()))) {
							case Success(a) => Success(Right(a))
							case Failure(falla) => Failure(falla)
						}

					case Failure(falla) => Failure(falla);
				}
			}.map {
				case Right(a) => Maybe.some(a);
				case Left(_) => Maybe.empty
			}
		}

		/** Creates a task that repeatedly executes the given task as long as its result is [[Maybe.empty]] and the specified `maxNumberOfExecutions` is not exceeded; waiting for the specified `pause` between the end of one execution and the start of the next.
		 * $notReusableTask */
		inline def reiterateDelayedWhileEmpty[A](maxNumberOfExecutions: Int, pause: Schedule)(task: Task[Maybe[Try[A]]]): Task[Maybe[A]] =
			new DelayedLoop[A](maxNumberOfExecutions, pause)(task)
	}

	final class DelayedLoop[A](maxNumberOfExecutions: Int, delay: Schedule)(task: Task[Maybe[Try[A]]]) extends Task[Maybe[A]] {
		override def engage(onComplete: Try[Maybe[A]] => Unit): Unit = {
			def loop(remainingExecutions: Int): Unit = {
				task.trigger(true) {
					case Success(mtA) =>
						mtA.fold {
							if (remainingExecutions > 1) {
								schedule(delay) { () => loop(remainingExecutions - 1) }
							} else
								onComplete(Success(Maybe.empty))
						} {
							case Success(a) => onComplete(Success(Maybe.some(a)))
							case Failure(e) => onComplete(Failure(e))
						}
					case Failure(e) => onComplete(Failure(e))
				}
			}

			if (maxNumberOfExecutions <= 0) {
				onComplete(Success(Maybe.empty))
			} else {
				loop(maxNumberOfExecutions)
			}
		}
	}
}
