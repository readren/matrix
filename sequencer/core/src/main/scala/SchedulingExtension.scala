package readren.sequencer

import SchedulingExtension.MilliDuration

import readren.common.{Maybe, castTo}
import readren.sequencer.Doer

import scala.annotation.targetName
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
 * In this API, up-chain refers to the Duty instance that encapsulates all prior computation steps. It serves as the source context for operations like map, which extend the chain with new logic.
 *
 * // TODO avoid the following limitation (which break referential transparency) enforcing the implementation of Schedule be immutable. That would require an internal mapping between each schedule instance and all its activations. The cancellation of a schedule instance would cancel all the associated activations. 
 * @define notReusableDuty CAUTION: the [[Duty]] instance returned by this method should not be reused. It is mutable because it depends on an instance of [[SchedulingExtension.Schedule]] which mutate when [[schedule]] is executed.
 * @define notReusableTask CAUTION: the [[Task]] instance returned by this method should not be reused. It is mutable because it depends on an instance of [[SchedulingExtension.Schedule]] which mutate when [[schedule]] is executed.
 * */
trait SchedulingExtension { thisSchedulingExtension: Doer =>


	/** Represents an execution schedule.
	 * It is tied to the routine passed along it to the [[schedule]] method. This means that it is mutable and, therefore, non referentially transparent and illegal to use the same instance in more than one call to [[schedule]].
	 * Given all the operations added to [[Duty]] and [[Task]] by this extension ([[SchedulingExtension]]) rely explicitly or implicitly on a [[Schedule]] instance, they all are also not referentially transparent.
	 * TODO: avoid the limitation of using the same instance in more than one call to [[schedule]], by enforcing [[Schedule]] to be referentially transparent. This change requires that instances of [[Schedule]] instances to be associated to all the routines that accompanied it in a calls to [[schedule]], and that the `cancel` method to apply to all of them. */
	type Schedule <: AnyRef

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

	/** Programs the execution of the provided `routine` according to the provided [[Schedule]].
	 * The implementation must ensure mutual sequentiality of the execution of routines passed to both, this method and [[executeSequentially]].
	 * @param schedule determines when the provided `routine` will be executed.
	 * @param routine the routine to execute according to the provided [[schedule]].
	 * The implementation should not throw non-fatal exceptions. */
	def scheduleSequentially(schedule: Schedule, routine: Schedule => Unit): Unit

	/**
	 * The implementation should stop the [[Schedule]] from triggering any executions. Even if it was not activated jet.
	 * When this method is called within the thread currently assigned to this [[Doer]], the [[SchedulingExtension]] implementation should ensure that the [[Schedule]] will not trigger any execution after this method is called.
	 * If, on the contrary, this method is executed by a thread other than the one currently assigned to this [[Doer]], the [[SchedulingExtension]] implementation should strive to prevent the [[Schedule]] from triggering any execution except possibly a single one if this method is called close to its scheduled time.
	 * The implementation should not throw non-fatal exceptions. */
	def cancel(schedule: Schedule): Unit

	/**
	 * The implementation should [[cancel]] all the [[Schedule]] instances of this [[Doer]] that were activated. See [[wasActivated]].
	 * The implementation should not throw non-fatal exceptions. */
	def cancelAll(): Unit

	/** @return true if the [[Schedule]] was used in a call to [[scheduleSequentially]], even if it is cancelled. */
	def wasActivated(schedule: Schedule): Boolean

	/** @return true if the [[Schedule]] was cancelled, even if it was not activated.
	 * An [[Schedule]] instance becomes canceled when either, it is passed to the [[cancel]] method, or the [[cancelAll]] method is called after it [[wasActivated]]. */
	def isCanceled(schedule: Schedule): Boolean

	inline def schedule(schedule: Schedule)(routine: Schedule => Unit): Unit =
		scheduleSequentially(schedule, routine)

	//// DUTY ////

	//// Duty instance operations  ////

	extension [A](thisDuty: Duty[A]) {

		/** Returns a [[Duty]] that triggers the up-chain [[Duty]] according to a [[Schedule]].
		 * The [[Schedule]] is activated when the returned [[Duty]] is executed.
		 * For periodic schedules (e.g., fixed-rate or fixed-delay), the up-chain [[Duty]] is executed repeatedly, yielding each result, until the schedule is canceled.
		 *
		 * $notReusableDuty */
		@targetName("scheduledDuty")
		inline def scheduled(schedule: Schedule): Duty[A] =
			new ScheduledDuty(thisDuty, schedule)

		/** Returns a [[Duty]] that triggers the up-chain [[Duty]] after a delay measured from the moment the returned [[Duty]] is executed. */
		@targetName("delayedDuty")
		inline def delayed(delay: MilliDuration): Duty[A] =
			new DelayedDuty(thisDuty, delay)

		/** Like [[Duty.map]] but the function application is scheduled.
		 * Note that what is scheduled is the function application, not the execution of the up-chain [[Duty]]. The provided [[Schedule]] is activated only after the up-chain duty has completed.
		 * For periodic schedules (e.g., fixed-rate or fixed-delay), the up-chain [[Duty]] is executed repeatedly, yielding each result, until the schedule is canceled.
		 * Is equivalent to {{{ thisDuty.flatMap(a => Duty_schedules(schedule)(_ => f(a)) }}} but more efficient.
		 *
		 * $notReusableDuty */
		inline def scheduledMap[B](aSchedule: Schedule)(f: A => B): Duty[B] =
			new ScheduledMap(thisDuty, aSchedule, f)

		/** Like [[Duty.map]] but the function application is delayed.
		 * Note that what is delayed is the function application, not the execution of the up-chain [[Duty]]. The delay occurs only after the up-chain [[Duty]] is completed.
		 * Is equivalent to {{{ thisDuty.flatMap(a => Duty_delays(delay)(_ => f(a)) }}} but more efficient.
		 * */
		inline def delayedMap[B](delay: MilliDuration)(f: A => B): Duty[B] =
			new DelayedMap(thisDuty, delay, f)

		/** Like [[Duty.flatMap]] but the function application is scheduled.
		 * Note that what is scheduled is the function application, not the execution of the up-chain [[Duty]]. The provided [[Schedule]] is activated only after the up-chain duty has completed.
		 * If the provided [[Schedule]] schedules more than one execution (fixed-rate or fixed-delay) then the function application will be executed multiple times according to the [[Schedule]] until it is canceled.
		 * Is equivalent to {{{ thisDuty.flatMap(a => Duty_schedulesFlat(schedule)(_ => f(a)) }}} but more efficient.
		 *
		 * $notReusableDuty */
		inline def scheduledFlatMap[B](aSchedule: Schedule)(f: A => Duty[B]): Duty[B] =
			new ScheduledFlatMap[A, B](thisDuty, aSchedule, f)

		/** Like [[Duty.flatMap]] but the function application is delayed.
		 * Note that what is delayed is the function application, not the execution of the up-chain [[Duty]]. The delay occurs only after the up-chain [[Duty]] is completed.
		 * Is equivalent to {{{ thisDuty.flatMap(a => Duty_delaysFlat(delay)(_ => f(a)) }}} but more efficient.
		 * */
		inline def delayedFlatMap[B](delay: MilliDuration)(f: A => Duty[B]): Duty[B] =
			new DelayedFlatMap(thisDuty, delay, f)

		/**
		 * Returns a [[Duty]] that waits for the up-chain [[Duty]] to yield a result, but only for a limited time.
		 * The time limit is determined by the initial delay of the provided [[Schedule]].
		 * If the up-chain [[Duty]] yields a result within the time limit, the returned [[Duty]] yields that result wrapped in [[Maybe.some]].
		 * If the time limit is exceeded, the returned [[Duty]] yields [[Maybe.empty]] immediately and does not wait for the up-chain result.
		 * The up-chain [[Duty]] is executed regardless and may complete in the background after the timeout.
		 * The [[Schedule]] is activated when the returned [[Duty]] is executed and canceled when it completes. Therefore, fixed-rate and fixed-delay kind schedules are worthless.
		 * If the [[Schedule]] is cancelled before the time limit, then the returned [[Duty]] waits the up-chain [[Duty]] completion forever, ensuring a non-empty result (provided there is one).
		 *
		 * $notReusableDuty
		 *
		 * @param schedule a [[Schedule]] whose initial delay is the maximum time to wait for a result, measured from the start of the returned [[Duty]]'s execution.
		 *                 If the up-chain [[Duty]] yields a result before this time elapses, the result is wrapped in [[Maybe.some]]. If the timer expires first, the returned [[Duty]] yields [[Maybe.empty]] immediately without waiting any more.
		 * @return a [[Duty]] that yields [[Maybe.some]] containing the result of the up-chain [[Duty]] if it completes within the time limit; otherwise, yields [[Maybe.empty]] as soon as the timeout is reached.                
		 */
		inline def timeLimited(schedule: Schedule): Duty[Maybe[A]] = {
			new TimeLimitedDuty[A](thisDuty.engageEta, 0, schedule)
		}

		/**
		 * Returns a [[Duty]] that waits for the up-chain [[Duty]] to yield a result, but only for a limited time.
		 * If the up-chain [[Duty]] yields a result within the time limit, the returned [[Duty]] yields that result wrapped in [[Maybe.some]].
		 * If the time limit is exceeded, the returned [[Duty]] yields [[Maybe.empty]] immediately and does not wait for the up-chain result.
		 * The up-chain [[Duty]] is executed regardless and may complete in the background after the timeout.
		 *
		 * @param limit the maximum time to wait for a result, measured from the start of the returned [[Duty]]'s execution. If the up-chain [[Duty]] yields a result before this time elapses, the result is wrapped in [[Maybe.some]]. If the timer expires first, the returned [[Duty]] yields [[Maybe.empty]] immediately without waiting any more.
		 * @return a [[Duty]] that yields [[Maybe.some]] containing the result of the up-chain [[Duty]] if it completes within the time limit; otherwise, yields [[Maybe.empty]] as soon as the timeout is reached.                
		 */
		inline def timeLimited(limit: MilliDuration): Duty[Maybe[A]] = {
			new TimeLimitedDuty[A](thisDuty.engageEta, limit, null)
		}

		/**
		 * Repeats the up-chain [[Duty]] whenever its execution duration exceeds a specified limit, up to a maximum number of retries.
		 * Each retry is triggered immediately after the previous attempt times out, with no delay between retries.
		 * The up-chain [[Duty]] is not cancelled when it times out; it continues executing in the background even as retries begin.
		 * The time limit is best-effort: it does not forcibly interrupt the up-chain [[Duty]], but determines whether a retry should be initiated.
		 * If the up-chain [[Duty]] has side effects, they will occur once per attempt, resulting in a total of one plus the number of retries.
		 * Equivalent to the [[Task]]'s [[reattemptedOnTimeout]] method but for [[Duty]].
		 *
		 * @param limit      the maximum duration allowed for each execution of the up-chain [[Duty]] before triggering a retry.
		 * @param maxRetries the maximum number of retries permitted after the initial attempt.
		 * @return a [[Duty]] that yields [[Maybe.some]] containing the result of the up-chain [[Duty]] if any attempt completes within the time limit; otherwise, yields [[Maybe.empty]] as soon as the final attempt times out.
		 */
		def retriedOnTimeout(limit: MilliDuration, maxRetries: Int): Duty[Maybe[A]] = {
			thisDuty.timeLimited(limit).flatMap(_.fold {
				if maxRetries > 0 then retriedOnTimeout(limit, maxRetries - 1)
				else Duty_ready(Maybe.empty)
			} { r =>
				Duty_Ready(Maybe.some(r))
			})
		}
	}

	//// Duty factory methods ////

	/** Builds a [[Duty]] that, once executed, does nothing but yields a value of `()` after the specified duration.
	 * The delay period begins when the returned [[Duty]] is started, not when it is built.
	 * This is equivalent to both `Duty_unit.delayed(duration)` and `Duty_delays(duration)(_ => ())`.
	 *
	 * @param duration the time to wait before the [[Duty]] yields its result.
	 * @return a new [[Duty]] that will yield a value of `()` after the specified delay.
	 */
	inline def Duty_sleeps(duration: MilliDuration): Duty[Unit] =
		Duty_unit.delayed(duration)

	/**
	 * Builds a [[Duty]] that schedules the execution of a supplier function according to a specified [[Schedule]] and yields the supplier’s result for each scheduled execution.
	 * The schedule is activated only when the returned [[Duty]] is started, not when it is constructed.
	 * For periodic schedules (e.g., fixed-rate or fixed-delay), the supplier is executed repeatedly, yielding each result, until the schedule is canceled.
	 *
	 * $notReusableDuty
	 * @param schedule the [[Schedule]] controlling when the supplier function is executed.
	 * @param supplier the function that produces a value of type [[A]] for each scheduled execution.
	 * @return a [[Duty]] that yields the supplier’s result(s) according to the specified [[Schedule]].
	 */
	inline def Duty_schedules[A](schedule: Schedule)(supplier: Schedule => A): Duty[A] =
		new DelayedSupplierDuty(0, schedule, supplier)

	/**
	 * Builds a [[Duty]] that schedules the execution of a [[Duty]] builder according to a specified [[Schedule]] and yields the results of the [[Duty]] produced by the builder for each scheduled execution.
	 * The schedule is activated only when the returned [[Duty]] is started, not when it is constructed.
	 * For periodic schedules (e.g., fixed-rate or fixed-delay), the builder is executed repeatedly, producing a new [[Duty]] for each execution, and the results of each produced [[Duty]] are yielded until the schedule is canceled.
	 * This [[Duty]] is not reusable and can only be executed once.
	 *
	 * @param schedule the [[Schedule]] controlling when the [[Duty]] builder is executed.
	 * @param builder  the function that produces a new [[Duty[A]]] for each scheduled execution.
	 * @return a [[Duty]] that yields the results of the [[Duty]] produced by the builder according to the specified [[Schedule]].
	 */
	inline def Duty_schedulesFlat[A](schedule: Schedule)(builder: Schedule => Duty[A]): Duty[A] =
		new DelayedSupplierFlatDuty(0, schedule, builder)

	/**
	 * Builds a [[Duty]] that waits for a specified duration before executing a supplier function and yielding its result.
	 * The delay begins only when the returned [[Duty]] is started, not when it is constructed.
	 * The supplier is executed once after the delay, and its result is what the returned [[Duty]] yields.
	 *
	 * @param duration the duration to wait before executing the supplier function.
	 * @param supplier the function that produces a value of type [[A]] after the delay.
	 * @return a [[Duty]] that yields the supplier’s result after the specified duration.
	 */
	inline def Duty_delays[A](duration: MilliDuration)(supplier: Schedule => A): Duty[A] =
		new DelayedSupplierDuty(duration, null, supplier)

	/**
	 * Builds a [[Duty]] that waits for a specified duration before executing a [[Duty]] builder and yielding the result of the produced [[Duty]].
	 * The delay begins only when the returned [[Duty]] is started, not when it is constructed.
	 * The builder is executed once after the delay, producing a [[Duty]] whose result is yielded by the returned [[Duty]].
	 *
	 * @param duration the duration to wait before executing the [[Duty]] builder.
	 * @param builder  the function that produces a new [[Duty[A]]] after the delay.
	 * @return a [[Duty]] that yields the result of the [[Duty]] produced by the builder after the specified duration.
	 */
	inline def Duty_delaysFlat[A](duration: MilliDuration)(builder: Schedule => Duty[A]): Duty[A] =
		new DelayedSupplierFlatDuty(duration, null, builder)

	/**
	 * Builds a [[Duty]] that executes a supplier function and yields its result if the execution duration is less than a specified limit.
	 * If the execution exceeds the limit, the supplier is retried immediately, up to a maximum number of retries.
	 * The supplier is not cancelled when it times out; it continues executing in the background even as retries begin.
	 * The time limit is best-effort: it does not forcibly interrupt the supplier function, but determines whether a retry should be initiated.
	 * If the supplier has side effects, they will occur once per attempt, resulting in a total of one plus the number of retries.
	 * The supplier receives the number of failed attempts as a parameter, allowing it to adjust its behavior based on prior timeouts.
	 *
	 * @param limit         the maximum duration allowed for each execution of the supplier function before triggering a retry.
	 * @param maxRetries    the maximum number of retries permitted after the initial attempt.
	 * @param supplier          the supplier function that produces a value of type [[A]], taking the number of failed attempts as an input.
	 * @return a [[Duty]] that yields [[Maybe.some]] containing the result of the supplier function if any attempt completes within the time limit; otherwise, yields [[Maybe.empty]] as soon as the final attempt times out.
	 */
	def Duty_retryOnTimeout[A](limit: MilliDuration, maxRetries: Int, supplier: (failedAttempts: Int) => A): Duty[Maybe[A]] = {
		def loop(failedAttempts: Int): Duty[Maybe[A]] = {
			TimeLimitedDuty[A](_(supplier(failedAttempts)), limit, null)
				.flatMap(_.fold {
					if failedAttempts >= maxRetries then Duty_ready(Maybe.empty)
					else loop(failedAttempts + 1)
				} { a =>
					Duty_ready(Maybe.some(a))
				})
		}

		loop(0)
	}

	//// Duty implementation classes ////

	/** $notReusableDuty */
	final class ScheduledDuty[A](duty: Duty[A], aSchedule: Schedule) extends AbstractDuty[A] {
		override def engage(onComplete: A => Unit): Unit =
			schedule(aSchedule)(_ => duty.engagePortal(onComplete))
	}

	/** $notReusableDuty */
	final class ScheduledMap[A, B](duty: Duty[A], aSchedule: Schedule, f: A => B) extends AbstractDuty[B] {
		override def engage(onComplete: B => Unit): Unit = {
			duty.engagePortal { a =>
				schedule(aSchedule) { _ => onComplete(f(a)) }
			}
		}
	}

	/** $notReusableDuty */
	final class ScheduledFlatMap[A, B](duty: Duty[A], aSchedule: Schedule, f: A => Duty[B]) extends AbstractDuty[B] {
		override def engage(onComplete: B => Unit): Unit = {
			duty.engagePortal { a =>
				schedule(aSchedule) { _ => f(a).engagePortal(onComplete) }
			}
		}
	}

	final class DelayedDuty[A](duty: Duty[A], delay: MilliDuration) extends AbstractDuty[A] {
		override protected def engage(onComplete: A => Unit): Unit =
			schedule(newDelaySchedule(delay)) { _ => duty.engagePortal(onComplete) }
	}

	final class DelayedMap[A, B](duty: Duty[A], delay: MilliDuration, f: A => B) extends AbstractDuty[B] {
		override protected def engage(onComplete: B => Unit): Unit =
			duty.engagePortal { a =>
				schedule(newDelaySchedule(delay)) { _ => onComplete(f(a)) }
			}
	}

	final class DelayedFlatMap[A, B](duty: Duty[A], delay: MilliDuration, f: A => Duty[B]) extends AbstractDuty[B] {
		override protected def engage(onComplete: B => Unit): Unit =
			duty.engagePortal { a =>
				schedule(newDelaySchedule(delay)) { _ => f(a).engagePortal(onComplete) }
			}
	}

	/**
	 * Caution: This [[Duty]] is reusable only when limit2 is null.
	 */
	final class TimeLimitedDuty[A](duty: (A => Unit) => Unit, limit1: MilliDuration, limit2: Schedule | Null) extends AbstractDuty[Maybe[A]] {
		override def engage(onComplete: Maybe[A] => Unit): Unit = {
			val timer: Schedule = limit2 match {
				case s: Schedule @unchecked => s
				case _ => newDelaySchedule(limit1)
			}
			var hasElapsed = false
			var hasCompleted = false
			schedule(timer) { _ =>
				cancel(timer)
				if (!hasCompleted) {
					hasElapsed = true
					onComplete(Maybe.empty)
				}
			}
			duty { a =>
				if (!hasElapsed) {
					cancel(timer)
					hasCompleted = true
					onComplete(Maybe.some(a))
				}
			}
		}
	}

	/**
	 * Caution: This [[Duty]] is reusable only when limit2 is null.
	 */
	final class DelayedSupplierDuty[A](limit1: MilliDuration, limit2: Schedule | Null, supplier: Schedule => A) extends AbstractDuty[A] {
		override def engage(onComplete: A => Unit): Unit = {
			val timer: Schedule = limit2 match {
				case s: Schedule @unchecked => s
				case _ => newDelaySchedule(limit1)
			}
			schedule(timer)(_ => onComplete(supplier(timer)))
		}
	}

	/**
	 * Caution: This [[Duty]] is reusable only when limit2 is null.
	 */
	final class DelayedSupplierFlatDuty[A](limit1: MilliDuration, limit2: Schedule | Null, supplier: Schedule => Duty[A]) extends AbstractDuty[A] {
		override def engage(onComplete: A => Unit): Unit = {
			val timer: Schedule = limit2 match {
				case s: Schedule @unchecked => s
				case _ => newDelaySchedule(limit1)
			}
			schedule(timer)(_ => supplier(timer).engagePortal(onComplete))
		}
	}

	//// TASK ////

	//// Task instance operations ////

	extension [A](thisTask: Task[A]) {

		/** Like [[Duty.scheduled]] but for [[Task]]s.
		 * $notReusableTask */
		@targetName("scheduledTask")
		def scheduled(schedule: Schedule): Task[A] =
			new ScheduledTask(thisTask, schedule)

		/** Like [[Duty.delayed]] but for [[Task]]s.
		 * $notReusableTask */
		@targetName("delayedTask")
		inline def delayed(delay: MilliDuration): Task[A] =
			scheduled(newDelaySchedule(delay))

		/** Like [[Task.transform]] but the function application is scheduled.
		 * Note that what is scheduled is the function application, not the execution of the up-chain [[Task]]. The provided [[Schedule]] is activated only after the up-chain [[Task]] has completed.
		 * For periodic schedules (e.g., fixed-rate or fixed-delay), the up-chain [[Task]] is executed repeatedly, yielding each result, until the schedule is canceled.
		 * Is equivalent to {{{ thisTask.transformWith(tryA => Task_schedules(schedule)(_ => f(tryA)) }}} but more efficient.
		 * $notReusableDuty */
		def scheduledTransform[B](schedule: Schedule)(f: Try[A] => Try[B]): Task[B] =
			new ScheduledTransform[A, B](thisTask, schedule, f)

		/** Like [[Task.transform]] but the function application is delayed.
		 * Note that what is delayed is the function application, not the execution of the up-chain [[Task]]. The delay occurs only after the up-chain [[Task]] is completed.
		 * Is equivalent to {{{ thisTask.transformWith(tryA => Task_delays(delay)(_ => f(tryA)) }}} but more efficient.
		 * */
		inline def delayedTransform[B](delay: MilliDuration)(f: Try[A] => Try[B]): Task[B] =
			new DelayedTransform(thisTask, delay, f)

		/** Like [[Task.transformWith]] but the function application is scheduled.
		 * Note that what is scheduled is the function application, not the execution of the up-chain [[Task]]. The provided [[Schedule]] is activated only after the up-chain [[Task]] has completed.
		 * For periodic schedules (e.g., fixed-rate or fixed-delay), the up-chain [[Task]] is executed repeatedly, yielding each result, until the schedule is canceled.
		 * Is equivalent to {{{ thisTask.transformWith(tryA => Task_schedulesFlat(schedule)(_ => f(tryA)) }}} but more efficient.
		 * $notReusableDuty */
		inline def scheduledTransformWith[B](schedule: Schedule)(f: Try[A] => Task[B]): Task[B] =
			new ScheduledTransformWith(thisTask, schedule, f)

		/** Like [[Task.transformWith]] but the function application is delayed.
		 * Note that what is delayed is the function application, not the execution of the up-chain [[Task]]. The delay occurs only after the up-chain [[Task]] is completed.
		 * Is equivalent to {{{ thisTask.transformWith(tryA => Task_delaysFlat(delay)(_ => f(tryA)) }}} but more efficient.
		 * */
		inline def delayedTransformWith[B](delay: MilliDuration, f: Try[A] => Task[B]): Task[B] =
			new DelayedTransformWith(thisTask, delay, f)

		/**
		 * Returns a [[Task]] that waits for the up-chain [[Task]] to yield a result, but only for a limited time.
		 * The time limit is determined by the initial delay of the provided [[Schedule]].
		 * If the up-chain [[Task]] yields a result within the time limit, the returned [[Task]] yields that result wrapped in [[Maybe.some]].
		 * If the time limit is exceeded, the returned [[Task]] yields [[Maybe.empty]] immediately and does not wait for the up-chain result.
		 * The up-chain [[Task]] is executed regardless and may complete in the background after the timeout.
		 * The [[Schedule]] is activated when the returned [[Task]] is executed and canceled when it completes. Therefore, fixed-rate and fixed-delay kind schedules are worthless.
		 * If the [[Schedule]] is cancelled before the time limit, then the returned [[Task]] waits the up-chain [[Task]] completion forever, ensuring a non-empty result (provided there is one).
		 *
		 * @param schedule a [[Schedule]] whose initial delay is the maximum time to wait for a result, measured from the start of the returned [[Duty]]'s execution.
		 *                 If the up-chain [[Duty]] yields a result before this time elapses, the result is wrapped in [[Maybe.some]]. If the timer expires first, the returned [[Duty]] yields [[Maybe.empty]] immediately without waiting any more.
		 * @return a [[Duty]] that will complete with [[Maybe.some]] wrapping the result if it is available within the time limit, or with [[Maybe.empty]] otherwise.
		 */
		inline def timeBounded(schedule: Schedule): Task[Maybe[A]] =
			new TimeLimitedTask[A](thisTask.engageEta, 0, schedule)

		/**
		 * Returns a [[Task]] that waits for the up-chain [[Task]] to yield a result, but only for a limited time.
		 * If the up-chain [[Task]] yields a result within the time limit, the returned [[Task]] yields that result wrapped in [[Maybe.some]].
		 * If the time limit is exceeded, the returned [[Task]] yields [[Maybe.empty]] immediately, ignoring the future up-chain result.
		 * The up-chain [[Task]] is executed regardless and may complete in the background after the timeout.
		 * $notReusableTask
		 *
		 * @param limit the maximum time to wait for a result, measured from the start of the returned [[Duty]]'s execution. If the up-chain [[Duty]] yields a result before this time elapses, the result is wrapped in [[Maybe.some]]. If the timer expires first, the returned [[Duty]] yields [[Maybe.empty]] immediately without waiting any more.
		 * @return a [[Duty]] that will complete with [[Maybe.some]] wrapping the result if it is available within the timeout, or with [[Maybe.empty]] if the timeout elapses first.
		 */
		inline def timeBounded(limit: MilliDuration): Task[Maybe[A]] =
			new TimeLimitedTask[A](thisTask.engageEta, limit, null)


		/**
		 * Repeats the up-chain [[Task]] whenever its execution duration exceeds a specified limit, up to a maximum number of retries. 
		 * Each retry is triggered immediately after the previous attempt times out, with no delay between retries.
		 * The up-chain [[Task]] is not cancelled when it times out; it continues executing in the background even as retries begin.
		 * The time limit is best-effort: it does not forcibly interrupt the up-chain [[Task]], but determines whether a retry should be initiated.
		 * If the up-chain [[Task]] has side effects, they will occur once per attempt, resulting in a total of one plus the number of retries.
		 * Equivalent to the [[Duty]]'s [[retriedOnTimeout]] method but for [[Task]].
		 *
		 * @param limit      the maximum duration allowed for each execution of the up-chain [[Task]] before triggering a retry.
		 * @param maxRetries the maximum number of retries permitted after the initial attempt.
		 * @return a [[Task]] that yields [[Maybe.some]] containing the result of the up-chain [[Task]] if any attempt completes within the time limit; otherwise, yields [[Maybe.empty]] as soon as the final attempt times out.
		 */
		def reattemptedOnTimeout(limit: MilliDuration, maxRetries: Int): Task[Maybe[A]] = {
			thisTask.timeBounded(limit).flatMap(_.fold {
				if maxRetries > 0 then reattemptedOnTimeout(limit, maxRetries - 1)
				else Task_successful(Maybe.empty)
			} { r =>
				Task_successful(Maybe.some(r))
			})
		}
	}

	//// Task factory methods ////

	/** Builds a [[Task]] that, once executed, does nothing but yields a value of `()` after the specified duration.
	 * The delay period begins when the returned [[Task]] is started, not when it is built.
	 * This is equivalent to both `Duty_unit.delayed(duration)` and `Duty_delay(duration)(() => ())`.
	 *
	 * @param duration the time to wait before the [[Task]] yields its result.
	 * @return a new [[Task]] that will yield a value of `()` after the specified delay.
	 */
	inline def Task_sleeps(duration: MilliDuration): Task[Unit] =
		Task_unit.delayed(duration)

	/**
	 * Builds a [[Task]] that schedules the execution of a supplier function according to a specified [[Schedule]] and yields the supplier’s result for each scheduled execution.
	 * The schedule is activated only when the returned [[Task]] is started, not when it is constructed.
	 * For periodic schedules (e.g., fixed-rate or fixed-delay), the supplier is executed repeatedly, yielding each result, until the schedule is canceled.
	 *
	 * $notReusableDuty
	 * @param schedule the [[Schedule]] controlling when the supplier function is executed.
	 * @param supplier the function that produces a value of type [[A]] for each scheduled execution.
	 * @return a [[Task]] that yields the supplier’s result(s) according to the specified [[Schedule]].
	 */
	inline def Task_schedules[A](schedule: Schedule)(supplier: Schedule => Try[A]): Task[A] =
		new DelayedSupplierTask(0, schedule, supplier)

	/**
	 * Builds a [[Task]] that schedules the execution of a [[Task]] builder according to a specified [[Schedule]] and yields the results of the [[Task]] produced by the builder for each scheduled execution.
	 * The schedule is activated only when the returned [[Task]] is started, not when it is constructed.
	 * For periodic schedules (e.g., fixed-rate or fixed-delay), the builder is executed repeatedly, producing a new [[Task]] for each execution, and the results of each produced [[Task]] are yielded until the schedule is canceled.
	 * This [[Task]] is not reusable and can only be executed once.
	 *
	 * @param schedule the [[Schedule]] controlling when the [[Task]] builder is executed.
	 * @param builder  the function that produces a new [[Task[A]]] for each scheduled execution.
	 * @return a [[Task]] that yields the results of the [[Task]] produced by the builder according to the specified [[Schedule]].
	 */
	inline def Task_schedulesFlat[A](schedule: Schedule)(builder: Schedule => Task[A]): Task[A] =
		new DelayedSupplierFlatTask(0, schedule, builder)

	/**
	 * Builds a [[Task]] that waits for a specified duration before executing a supplier function and yielding its result.
	 * The delay begins only when the returned [[Task]] is started, not when it is constructed.
	 * The supplier is executed once after the delay, and its result is what the returned [[Task]] yields.
	 *
	 * @param duration the duration to wait before executing the supplier function.
	 * @param supplier the function that produces a result after the delay.
	 * @return a [[Task]] that yields the supplier’s result after the specified duration.
	 */
	inline def Task_delays[A](duration: MilliDuration)(supplier: Schedule => Try[A]): Task[A] =
		new DelayedSupplierTask(duration, null, supplier)

	/**
	 * Builds a [[Task]] that waits for a specified duration before executing a [[Task]] builder and yielding the result of the produced [[Task]].
	 * The delay begins only when the returned [[Task]] is started, not when it is constructed.
	 * The builder is executed once after the delay, producing a [[Task]] whose result is yielded by the returned [[Task]].
	 *
	 * @param duration the duration to wait before executing the [[Task]] builder.
	 * @param builder  the function that produces a new [[Task]] after the delay.
	 * @return a [[Task]] that yields the result of the [[Task]] produced by the builder after the specified duration.
	 */
	inline def Task_delaysFlat[A](duration: MilliDuration)(builder: Schedule => Task[A]): Task[A] =
		new DelayedSupplierFlatTask(duration, null, builder)

	/**
	 * Builds a [[Task]] that executes a supplier function and yields its result if the execution duration is less than a specified limit.
	 * If the execution exceeds the limit, the supplier is retried immediately, up to a maximum number of retries.
	 * The supplier is not cancelled when it times out; it continues executing in the background even as retries begin.
	 * The time limit is best-effort: it does not forcibly interrupt the supplier function, but determines whether a retry should be initiated.
	 * If the supplier has side effects, they will occur once per attempt, resulting in a total of one plus the number of retries.
	 * The supplier receives the number of failed attempts as a parameter, allowing it to adjust its behavior based on prior timeouts.
	 *
	 * @param limit         the maximum duration allowed for each execution of the supplier function before triggering a retry.
	 * @param maxRetries    the maximum number of retries permitted after the initial attempt.
	 * @param supplier          the supplier function that produces a value of type [[A]], taking the number of failed attempts as an input.
	 * @return a [[Task]] that yields [[Maybe.some]] containing the result of the supplier function if any attempt completes within the time limit; otherwise, yields [[Maybe.empty]] as soon as the final attempt times out.
	 */
	def Task_retryOnTimeout[A](limit: MilliDuration, maxRetries: Int, supplier: Int => Try[A]): Task[Maybe[A]] = {
		def loop(failedAttempts: Int): Task[Maybe[A]] = {
			TimeLimitedTask[A](_(supplier(failedAttempts)), limit, null)
				.flatMap(_.fold {
					if failedAttempts >= maxRetries then Task_successful(Maybe.empty)
					else loop(failedAttempts + 1)
				} { a =>
					Task_successful(Maybe.some(a))
				})
		}

		loop(0)
	}

	//// Task implementation classes ////

	/** $notReusableDuty */
	final class ScheduledTask[A](task: Task[A], aSchedule: Schedule) extends AbstractTask[A] {
		override def engage(onComplete: Try[A] => Unit): Unit = {
			schedule(aSchedule)(_ => task.engagePortal(onComplete))
		}
	}

	/** $notReusableDuty */
	final class ScheduledTransform[A, B](task: Task[A], aSchedule: Schedule, f: Try[A] => Try[B]) extends AbstractTask[B] {
		override def engage(onComplete: Try[B] => Unit): Unit = {
			task.engagePortal { tryA =>
				schedule(aSchedule) { _ =>
					val tryB =
						try f(tryA)
						catch {
							case NonFatal(e) => Failure(e)
						}
					onComplete(tryB)
				}
			}
		}
	}

	/** $notReusableDuty */
	final class ScheduledTransformWith[A, B](taskA: Task[A], aSchedule: Schedule, f: Try[A] => Task[B]) extends AbstractTask[B] {
		override def engage(onComplete: Try[B] => Unit): Unit = {
			taskA.engagePortal { tryA =>
				schedule(aSchedule) { _ =>
					val taskB =
						try f(tryA)
						catch {
							case NonFatal(e) => Task_failed(e)
						}
					taskB.engagePortal(onComplete)
				}
			}
		}
	}

	final class DelayedTask[A](task: Task[A], delay: MilliDuration) extends AbstractTask[A] {
		override def engage(onComplete: Try[A] => Unit): Unit = {
			schedule(newDelaySchedule(delay)) { _ => task.engagePortal(onComplete) }
		}
	}

	final class DelayedTransform[A, B](task: Task[A], delay: MilliDuration, f: Try[A] => Try[B]) extends AbstractTask[B] {
		override protected def engage(onComplete: Try[B] => Unit): Unit =
			task.engagePortal { tryA =>
				schedule(newDelaySchedule(delay)) { _ =>
					val tryB =
						try f(tryA)
						catch {
							case NonFatal(e) => Failure(e)
						}
					onComplete(tryB)
				}
			}
	}

	final class DelayedTransformWith[A, B](task: Task[A], delay: MilliDuration, f: Try[A] => Task[B]) extends AbstractTask[B] {
		override protected def engage(onComplete: Try[B] => Unit): Unit =
			task.engagePortal { tryA =>
				schedule(newDelaySchedule(delay)) { _ =>
					val taskB =
						try f(tryA)
						catch {
							case NonFatal(e) => Task_failed(e)
						}
					taskB.engagePortal(onComplete)
				}
			}
	}

	/**
	 * This [[Task]] is reusable only when limit2 is null.
	 */
	final class TimeLimitedTask[A](upChain: (Try[A] => Unit) => Unit, limit1: MilliDuration, limit2: Schedule | Null) extends AbstractTask[Maybe[A]] {
		override def engage(onComplete: Try[Maybe[A]] => Unit): Unit = {
			val timer: Schedule = limit2 match {
				case s: Schedule @unchecked => s
				case _ => newDelaySchedule(limit1)
			}
			var hasElapsed = false
			var hasCompleted = false
			schedule(timer) { _ =>
				cancel(timer)
				if (!hasCompleted) {
					hasElapsed = true
					onComplete(Success(Maybe.empty))
				}
			}
			val consumer: Try[A] => Unit = tryA =>
				if (!hasElapsed) {
					cancel(timer)
					hasCompleted = true
					tryA match {
						case Success(a) => onComplete(Success(Maybe.some(a)))
						case f: Failure[A] => onComplete(f.castTo[Maybe[A]])
					}
				}
			upChain(consumer)
		}
	}

	/**
	 * Caution: This [[Task]] is reusable only when limit2 is null.
	 */
	final class DelayedSupplierTask[A](limit1: MilliDuration, limit2: Schedule | Null, supplier: Schedule => Try[A]) extends AbstractTask[A] {
		override def engage(onComplete: Try[A] => Unit): Unit = {
			val timer: Schedule = limit2 match {
				case s: Schedule @unchecked => s
				case _ => newDelaySchedule(limit1)
			}
			schedule(timer)(_ => onComplete(supplier(timer)))
		}
	}

	/**
	 * Caution: This [[Task]] is reusable only when limit2 is null.
	 */
	final class DelayedSupplierFlatTask[A](limit1: MilliDuration, limit2: Schedule | Null, supplier: Schedule => Task[A]) extends AbstractTask[A] {
		override def engage(onComplete: Try[A] => Unit): Unit = {
			val timer: Schedule = limit2 match {
				case s: Schedule @unchecked => s
				case _ => newDelaySchedule(limit1)
			}
			schedule(timer)(_ => supplier(timer).engagePortal(onComplete))
		}
	}
}
