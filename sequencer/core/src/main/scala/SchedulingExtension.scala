package readren.sequencer

import readren.common.{Maybe, castTo}
import readren.sequencer.Doer

import scala.annotation.targetName
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}


/** Extends the [[Doer]] trait and its [[Task]] and [[Task]] inner traits with scheduling operations.
 *
 * The abstract methods specifies what an instance of [[Doer]] extended with the [[SchedulingExtension]] requires to exist.
 *
 * Design note: Why not avoid the vulnerability "using the same instance of Schedule in two calls to scheduleSequentially is illegal" by making Schedule only describe the schedule, and representing the execution plan by a separate trait Plan, with instances returned by the scheduleSequentially operation?
 * Because this would require operations on Task and Task that use scheduleSequentially to include an instance of Plan along with the result.
 * This would necessitate a tuple, which not only requires additional memory allocation but also complicates the chaining of operations.
 * Wait! There is a way. See [[Schedule]]
 *
 * In this API, up-chain refers to the Task instance that encapsulates all prior computation steps. It serves as the source context for operations like map, which extend the chain with new logic.
 *
 * // TODO avoid the following limitation (which break referential transparency) enforcing the implementation of Schedule be immutable. That would require an internal mapping between each schedule instance and all its activations. The cancellation of a schedule instance would cancel all the associated activations.
 * @define notReusableTask CAUTION: the [[Task]] instance returned by this method should not be reused. It is mutable because it depends on an instance of [[SchedulingExtension.Schedule]] which mutate when [[schedule]] is executed.
 * @define notReusableTask CAUTION: the [[Task]] instance returned by this method should not be reused. It is mutable because it depends on an instance of [[SchedulingExtension.Schedule]] which mutate when [[schedule]] is executed.
 * */
trait SchedulingExtension { thisSchedulingExtension: Doer =>


	/** Represents an execution schedule.
	 * It is tied to the routine passed along it to the [[schedule]] method. This means that it is mutable and, therefore, non referentially transparent and illegal to use the same instance in more than one call to [[schedule]].
	 * Given all the operations added to [[Task]] and [[Venture]] by this extension ([[SchedulingExtension]]) rely explicitly or implicitly on a [[Schedule]] instance, they all are also not referentially transparent.
	 * TODO: avoid the limitation of using the same instance in more than one call to [[schedule]], by enforcing [[Schedule]] to be referentially transparent. This change requires that instances of [[Schedule]] instances to be associated to all the routines that accompanied it in a calls to [[schedule]], and that the `cancel` method to apply to all of them. */
	type Schedule <: AnyRef

	/** Creates a [[Schedule]] for a single time execution after a delay.
	 * If the delay is non-positive, the execution would be as soon as possible.
	 * @param delay duration before the execution.
	 * @return a [[Schedule]] instance intended solely as an argument for a single call to the [[schedule]] method. */
	def newDelaySchedule(delay: MilliDuration): Schedule

	/** Creates a [[Schedule]] for a fixed rate repeated execution after an initial delay.
	 * @param initialDelay duration before the first execution. If non-positive, the first execution would be ASAP.
	 * @param interval duration between the scheduled time of the executions. Should be positive.
	 * @return a [[Schedule]] instance intended solely as an argument for a single call to the [[schedule]] method. */
	def newFixedRateSchedule(initialDelay: MilliDuration, interval: MilliDuration): Schedule

	/** Creates a [[Schedule]] for a fixed delay repeated execution after an initial delay.
	 * @param initialDelay duration before the first execution. If non-positive, the first execution would be ASAP.
	 * @param delay duration between the end of an execution and the scheduled start of the next. Should be positive.
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

	//// TASK ////

	//// Task instance operations  ////

	extension [A](thisTask: Task[A]) {

		/** Returns a [[Task]] that triggers the up-chain [[Task]] according to a [[Schedule]].
		 * The [[Schedule]] is activated when the returned [[Task]] is executed.
		 * For periodic schedules (e.g., fixed-rate or fixed-delay), the up-chain [[Task]] is executed repeatedly, yielding each result, until the schedule is canceled.
		 *
		 * $notReusableTask */
		@targetName("scheduledTask")
		inline def scheduled(schedule: Schedule): Task[A] =
			new ScheduledTask(thisTask, schedule)

		/** Returns a [[Task]] that triggers the up-chain [[Task]] after a delay measured from the moment the returned [[Task]] is executed. */
		@targetName("delayedTask")
		inline def delayed(delay: MilliDuration): Task[A] =
			new DelayedTask(thisTask, delay)

		/** Like [[Task.map]] but the function application is scheduled.
		 * Note that what is scheduled is the function application, not the execution of the up-chain [[Task]]. The provided [[Schedule]] is activated only after the up-chain task has completed.
		 * For periodic schedules (e.g., fixed-rate or fixed-delay), the up-chain [[Task]] is executed repeatedly, yielding each result, until the schedule is canceled.
		 * Is equivalent to {{{ thisTask.flatMap(a => Task_schedules(schedule)(_ => f(a)) }}} but more efficient.
		 *
		 * $notReusableTask */
		inline def scheduledMap[B](aSchedule: Schedule)(f: A => B): Task[B] =
			new ScheduledMap(thisTask, aSchedule, f)

		/** Like [[Task.map]] but the function application is delayed.
		 * Note that what is delayed is the function application, not the execution of the up-chain [[Task]]. The delay occurs only after the up-chain [[Task]] is completed.
		 * Is equivalent to {{{ thisTask.flatMap(a => Task_delays(delay)(_ => f(a)) }}} but more efficient.
		 * */
		inline def delayedMap[B](delay: MilliDuration)(f: A => B): Task[B] =
			new DelayedMap(thisTask, delay, f)

		/** Like [[Task.flatMap]] but the function application is scheduled.
		 * Note that what is scheduled is the function application, not the execution of the up-chain [[Task]]. The provided [[Schedule]] is activated only after the up-chain task has completed.
		 * If the provided [[Schedule]] schedules more than one execution (fixed-rate or fixed-delay) then the function application will be executed multiple times according to the [[Schedule]] until it is canceled.
		 * Is equivalent to {{{ thisTask.flatMap(a => Task_schedulesFlat(schedule)(_ => f(a)) }}} but more efficient.
		 *
		 * $notReusableTask */
		inline def scheduledFlatMap[B](aSchedule: Schedule)(f: A => Task[B]): Task[B] =
			new ScheduledFlatMap[A, B](thisTask, aSchedule, f)

		/** Like [[Task.flatMap]] but the function application is delayed.
		 * Note that what is delayed is the function application, not the execution of the up-chain [[Task]]. The delay occurs only after the up-chain [[Task]] is completed.
		 * Is equivalent to {{{ thisTask.flatMap(a => Task_delaysFlat(delay)(_ => f(a)) }}} but more efficient.
		 * */
		inline def delayedFlatMap[B](delay: MilliDuration)(f: A => Task[B]): Task[B] =
			new DelayedFlatMap(thisTask, delay, f)

		/**
		 * Returns a [[Task]] that waits for the up-chain [[Task]] to yield a result, but only for a limited time.
		 * The time limit is determined by the initial delay of the provided [[Schedule]].
		 * If the up-chain [[Task]] yields a result within the time limit, the returned [[Task]] yields that result wrapped in [[Maybe.some]].
		 * If the time limit is exceeded, the returned [[Task]] yields [[Maybe.empty]] immediately and does not wait for the up-chain result.
		 * The up-chain [[Task]] is executed regardless and may complete in the background after the timeout.
		 * The [[Schedule]] is activated when the returned [[Task]] is executed and canceled when it completes. Therefore, fixed-rate and fixed-delay kind schedules are worthless.
		 * If the [[Schedule]] is cancelled before the time limit, then the returned [[Task]] waits the up-chain [[Task]] completion forever, ensuring a non-empty result (provided there is one).
		 *
		 * $notReusableTask
		 *
		 * @param schedule a [[Schedule]] whose initial delay is the maximum time to wait for a result, measured from the start of the returned [[Task]]'s execution.
		 *                 If the up-chain [[Task]] yields a result before this time elapses, the result is wrapped in [[Maybe.some]]. If the timer expires first, the returned [[Task]] yields [[Maybe.empty]] immediately without waiting any more.
		 * @return a [[Task]] that yields [[Maybe.some]] containing the result of the up-chain [[Task]] if it completes within the time limit; otherwise, yields [[Maybe.empty]] as soon as the timeout is reached.
		 */
		inline def timeLimited(schedule: Schedule): Task[Maybe[A]] = {
			new TimeLimitedTask[A](thisTask.subscribeSync, 0, schedule)
		}

		/**
		 * Returns a [[Task]] that waits for the up-chain [[Task]] to yield a result, but only for a limited time.
		 * If the up-chain [[Task]] yields a result within the time limit, the returned [[Task]] yields that result wrapped in [[Maybe.some]].
		 * If the time limit is exceeded, the returned [[Task]] yields [[Maybe.empty]] immediately and does not wait for the up-chain result.
		 * The up-chain [[Task]] is executed regardless and may complete in the background after the timeout.
		 *
		 * @param limit the maximum time to wait for a result, measured from the start of the returned [[Task]]'s execution. If the up-chain [[Task]] yields a result before this time elapses, the result is wrapped in [[Maybe.some]]. If the timer expires first, the returned [[Task]] yields [[Maybe.empty]] immediately without waiting any more.
		 * @return a [[Task]] that yields [[Maybe.some]] containing the result of the up-chain [[Task]] if it completes within the time limit; otherwise, yields [[Maybe.empty]] as soon as the timeout is reached.
		 */
		inline def timeLimited(limit: MilliDuration): Task[Maybe[A]] = {
			new TimeLimitedTask[A](thisTask.subscribeSync, limit, null)
		}

		/**
		 * Repeats the up-chain [[Task]] whenever its execution duration exceeds a specified limit, up to a maximum number of retries.
		 * Each retry is triggered immediately after the previous attempt times out, with no delay between retries.
		 * The up-chain [[Task]] is not cancelled when it times out; it continues executing in the background even as retries begin.
		 * The time limit is best-effort: it does not forcibly interrupt the up-chain [[Task]], but determines whether a retry should be initiated.
		 * If the up-chain [[Task]] has side effects, they will occur once per attempt, resulting in a total of one plus the number of retries.
		 * Equivalent to the [[Venture]]'s [[reattemptedOnTimeout]] method but for [[Task]].
		 *
		 * @param limit      the maximum duration allowed for each execution of the up-chain [[Task]] before triggering a retry.
		 * @param maxRetries the maximum number of retries permitted after the initial attempt.
		 * @return a [[Task]] that yields [[Maybe.some]] containing the result of the up-chain [[Task]] if any attempt completes within the time limit; otherwise, yields [[Maybe.empty]] as soon as the final attempt times out.
		 */
		def retriedOnTimeout(limit: MilliDuration, maxRetries: Int): Task[Maybe[A]] = {
			thisTask.timeLimited(limit).flatMap(_.fold {
				if maxRetries > 0 then retriedOnTimeout(limit, maxRetries - 1)
				else Task_ready(Maybe.empty)
			} { r =>
				Task_ready(Maybe(r))
			})
		}
	}

	//// Task factory methods ////

	/** Builds a [[Task]] that, once executed, does nothing but yields a value of `()` after the specified duration.
	 * The delay period begins when the returned [[Task]] is started, not when it is built.
	 * This is equivalent to both `Task_unit.delayed(duration)` and `Task_delays(duration)(_ => ())`.
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
	 * $notReusableTask
	 * @param schedule the [[Schedule]] controlling when the supplier function is executed.
	 * @param supplier the function that produces a value of type [[A]] for each scheduled execution.
	 * @return a [[Task]] that yields the supplier’s result(s) according to the specified [[Schedule]].
	 */
	inline def Task_schedules[A](schedule: Schedule)(supplier: Schedule => A): Task[A] =
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
	 * @param supplier the function that produces a value of type [[A]] after the delay.
	 * @return a [[Task]] that yields the supplier’s result after the specified duration.
	 */
	inline def Task_delays[A](duration: MilliDuration)(supplier: Schedule => A): Task[A] =
		new DelayedSupplierTask(duration, null, supplier)

	/**
	 * Builds a [[Task]] that waits for a specified duration before executing a [[Task]] builder and yielding the result of the produced [[Task]].
	 * The delay begins only when the returned [[Task]] is started, not when it is constructed.
	 * The builder is executed once after the delay, producing a [[Task]] whose result is yielded by the returned [[Task]].
	 *
	 * @param duration the duration to wait before executing the [[Task]] builder.
	 * @param builder  the function that produces a new [[Task[A]]] after the delay.
	 * @return a [[Task]] that yields the result of the [[Task]] produced by the builder after the specified duration.
	 */
	inline def Task_delaysFlat[A](duration: MilliDuration)(builder: Schedule => Task[A]): Task[A] =
		new DelayedSupplierFlatTask(duration, null, builder)

	/**
	 * Builds a [[Task]] that executes a supplier function and yields its result if the execution duration is less than a specified limit.
	 * If the execution exceeds the limit, the supplier is retried immediately, up to a maximum number of retries.
	 * The supplier is not stopped when it times out; it continues executing in the background even as retries begin.
	 * The time limit is best-effort: it does not forcibly interrupt the supplier function, but determines whether a retry should be initiated.
	 * If the supplier has side effects, they will occur once per attempt, resulting in a total of one plus the number of retries.
	 * The supplier receives the number of failed attempts as a parameter, allowing it to adjust its behavior based on prior timeouts.
	 *
	 * @param limit         the maximum duration allowed for each execution of the supplier function before triggering a retry.
	 * @param maxRetries    the maximum number of retries permitted after the initial attempt.
	 * @param supplier          the supplier function that produces a value of type [[A]], taking the number of failed attempts as an input.
	 * @return a [[Task]] that yields [[Maybe.some]] containing the result of the supplier function if any attempt completes within the time limit; otherwise, yields [[Maybe.empty]] as soon as the final attempt times out.
	 */
	def Task_retryOnTimeout[A](limit: MilliDuration, maxRetries: Int, supplier: (failedAttempts: Int) => A): Task[Maybe[A]] = {
		def loop(failedAttempts: Int): Task[Maybe[A]] = {
			TimeLimitedTask[A](_(supplier(failedAttempts)), limit, null)
				.flatMap(_.fold {
					if failedAttempts >= maxRetries then Task_ready(Maybe.empty)
					else loop(failedAttempts + 1)
				} { a =>
					Task_ready(Maybe(a))
				})
		}

		loop(0)
	}

	//// Task implementation classes ////

	/** $notReusableTask */
	final class ScheduledTask[A](task: Task[A], aSchedule: Schedule) extends AbstractTask[A] {
		override def subscribeSync(monoObserver: MonoObserver[A]): Subscription = {
			// Returns subscription that guards schedule trigger and inner task completion
			new Subscription {
				private var active = true
				private var innerSub: Subscription = Subscription_empty

				{
					schedule(aSchedule) { _ =>
						if active then {
							innerSub = task.subscribeSync(monoObserver)
						}
					}
				}

				override def unsubscribe(): Unit = {
					checkWithin()
					active = false
					innerSub.unsubscribe()
				}
			}
		}
	}

	/** $notReusableTask */
	final class ScheduledMap[A, B](task: Task[A], aSchedule: Schedule, f: A => B) extends AbstractTask[B] {
		override def subscribeSync(monoObserver: MonoObserver[B]): Subscription = {
			// Returns subscription that propagates unsubscription to underlying task
			new Subscription {
				private var active = true
				private var innerSub: Subscription = Subscription_empty

				{
					innerSub = task.subscribeSync(new MonoObserver[A] {
						override def onSuccess(a: A): Unit = {
							if active then {
								schedule(aSchedule) { _ =>
									if active then monoObserver.onSuccess(f(a))
								}
							}
						}

						override def onError(ex: Throwable): Unit = {
							if active then monoObserver.onError(ex)
						}
					})
				}

				override def unsubscribe(): Unit = {
					checkWithin()
					active = false
					innerSub.unsubscribe()
				}
			}
		}
	}

	/** $notReusableTask */
	final class ScheduledFlatMap[A, B](task: Task[A], aSchedule: Schedule, f: A => Task[B]) extends AbstractTask[B] {
		override def subscribeSync(monoObserver: MonoObserver[B]): Subscription = {
			// Returns subscription that handles unsubscription from both outer and inner task
			new Subscription {
				private var active = true
				private var innerSub: Subscription = Subscription_empty

				{
					innerSub = task.subscribeSync(new MonoObserver[A] {
						override def onSuccess(a: A): Unit = {
							if active then {
								schedule(aSchedule) { _ =>
									if active then {
										innerSub = f(a).subscribeSync(monoObserver)
									}
								}
							}
						}

						override def onError(ex: Throwable): Unit = {
							if active then monoObserver.onError(ex)
						}
					})
				}

				override def unsubscribe(): Unit = {
					checkWithin()
					active = false
					innerSub.unsubscribe()
				}
			}
		}
	}

	final class DelayedTask[A](task: Task[A], delay: MilliDuration) extends AbstractTask[A] {
		override def subscribeSync(monoObserver: MonoObserver[A]): Subscription = {
			// Returns subscription that cancels schedule callback or inner task subscription
			new Subscription {
				private var active = true
				private var innerSub: Subscription = Subscription_empty

				{
					schedule(newDelaySchedule(delay)) { _ =>
						if active then {
							innerSub = task.subscribeSync(monoObserver)
						}
					}
				}

				override def unsubscribe(): Unit = {
					checkWithin()
					active = false
					innerSub.unsubscribe()
				}
			}
		}
	}

	final class DelayedMap[A, B](task: Task[A], delay: MilliDuration, f: A => B) extends AbstractTask[B] {
		override def subscribeSync(monoObserver: MonoObserver[B]): Subscription = {
			// Returns subscription that cancels schedule or propagates upstream
			new Subscription {
				private var active = true
				private var innerSub: Subscription = Subscription_empty

				{
					innerSub = task.subscribeSync(new MonoObserver[A] {
						override def onSuccess(a: A): Unit = {
							if active then {
								schedule(newDelaySchedule(delay)) { _ =>
									if active then monoObserver.onSuccess(f(a))
								}
							}
						}

						override def onError(ex: Throwable): Unit = {
							if active then monoObserver.onError(ex)
						}
					})
				}

				override def unsubscribe(): Unit = {
					checkWithin()
					active = false
					innerSub.unsubscribe()
				}
			}
		}
	}

	final class DelayedFlatMap[A, B](task: Task[A], delay: MilliDuration, f: A => Task[B]) extends AbstractTask[B] {
		override def subscribeSync(monoObserver: MonoObserver[B]): Subscription = {
			// Returns subscription that propagates cancel to both delayed outer and inner task
			new Subscription {
				private var active = true
				private var innerSub: Subscription = Subscription_empty

				{
					innerSub = task.subscribeSync(new MonoObserver[A] {
						override def onSuccess(a: A): Unit = {
							if active then {
								schedule(newDelaySchedule(delay)) { _ =>
									if active then {
										innerSub = f(a).subscribeSync(monoObserver)
									}
								}
							}
						}

						override def onError(ex: Throwable): Unit = {
							if active then monoObserver.onError(ex)
						}
					})
				}

				override def unsubscribe(): Unit = {
					checkWithin()
					active = false
					innerSub.unsubscribe()
				}
			}
		}
	}

	/**
	 * Caution: This [[Task]] is reusable only when limit2 is null.
	 */
	final class TimeLimitedTask[A](task: (A => Unit) => Unit, limit1: MilliDuration, limit2: Schedule | Null) extends AbstractTask[Maybe[A]] {
		override def subscribeSync(monoObserver: MonoObserver[Maybe[A]]): Subscription = {
			val timer: Schedule = if limit2 eq null then newDelaySchedule(limit1) else limit2.asInstanceOf[Schedule]
			var hasElapsed = false
			var hasCompleted = false
			// Returns subscription that stops timer and ignores task callback
			new Subscription {
				private var active = true

				{
					schedule(timer) { _ =>
						if active then {
							cancel(timer)
							if !hasCompleted then {
								hasElapsed = true
								monoObserver.onSuccess(Maybe.empty)
							}
						}
					}
					task { a =>
						if active && !hasElapsed then {
							cancel(timer)
							hasCompleted = true
							monoObserver.onSuccess(Maybe(a))
						}
					}
				}

				override def unsubscribe(): Unit = {
					checkWithin()
					active = false
					cancel(timer)
				}
			}
		}
	}

	/**
	 * Caution: This [[Task]] is reusable only when limit2 is null.
	 */
	final class DelayedSupplierTask[A](limit1: MilliDuration, limit2: Schedule | Null, supplier: Schedule => A) extends AbstractTask[A] {
		override def subscribeSync(monoObserver: MonoObserver[A]): Subscription = {
			val timer: Schedule = if limit2 eq null then newDelaySchedule(limit1) else limit2.asInstanceOf[Schedule]
			// Returns subscription that ignores supplier callback and cancels timer
			new Subscription {
				private var active = true
				{
					schedule(timer)(_ => if active then monoObserver.onSuccess(supplier(timer)))
				}

				override def unsubscribe(): Unit = {
					checkWithin()
					active = false
					cancel(timer)
				}
			}
		}
	}

	/**
	 * Caution: This [[Task]] is reusable only when limit2 is null.
	 */
	final class DelayedSupplierFlatTask[A](limit1: MilliDuration, limit2: Schedule | Null, supplier: Schedule => Task[A]) extends AbstractTask[A] {
		override def subscribeSync(monoObserver: MonoObserver[A]): Subscription = {
			val timer: Schedule = if limit2 eq null then newDelaySchedule(limit1) else limit2.asInstanceOf[Schedule]
			// Returns subscription that propagates unsubscription to inner task
			new Subscription {
				private var active = true
				private var innerSub: Subscription = Subscription_empty
				{
					schedule(timer)(_ => if active then innerSub = supplier(timer).subscribeSync(monoObserver))
				}

				override def unsubscribe(): Unit = {
					checkWithin()
					active = false
					cancel(timer)
					innerSub.unsubscribe()
				}
			}
		}
	}

	//// VENTURE ////

	//// Task instance operations ////

	extension [A](thisVenture: Venture[A]) {

		/** Like [[Task.scheduled]] but for [[Venture]]s.
		 * $notReusableTask */
		@targetName("scheduledVenture")
		def scheduled(schedule: Schedule): Venture[A] =
			new ScheduledVenture(thisVenture, schedule)

		/** Like [[Task.delayed]] but for [[Venture]]s.
		 * $notReusableTask */
		@targetName("delayedVenture")
		inline def delayed(delay: MilliDuration): Venture[A] =
			scheduled(newDelaySchedule(delay))

		/** Like [[Venture.transform]] but the function application is scheduled.
		 * Note that what is scheduled is the function application, not the execution of the up-chain [[Venture]]. The provided [[Schedule]] is activated only after the up-chain [[Venture]] has completed.
		 * For periodic schedules (e.g., fixed-rate or fixed-delay), the up-chain [[Venture]] is executed repeatedly, yielding each result, until the schedule is canceled.
		 * Is equivalent to {{{ thisVenture.transformWith(tryA => Venture_schedules(schedule)(_ => f(tryA)) }}} but more efficient.
		 * $notReusableTask */
		def scheduledTransform[B](schedule: Schedule)(f: Try[A] => Try[B]): Venture[B] =
			new ScheduledTransform[A, B](thisVenture, schedule, f)

		/** Like [[Venture.transform]] but the function application is delayed.
		 * Note that what is delayed is the function application, not the execution of the up-chain [[Venture]]. The delay occurs only after the up-chain [[Venture]] is completed.
		 * Is equivalent to {{{ thisVenture.transformWith(tryA => Venture_delays(delay)(_ => f(tryA)) }}} but more efficient.
		 * */
		inline def delayedTransform[B](delay: MilliDuration)(f: Try[A] => Try[B]): Venture[B] =
			new DelayedTransform(thisVenture, delay, f)

		/** Like [[Venture.transformWith]] but the function application is scheduled.
		 * Note that what is scheduled is the function application, not the execution of the up-chain [[Venture]]. The provided [[Schedule]] is activated only after the up-chain [[Venture]] has completed.
		 * For periodic schedules (e.g., fixed-rate or fixed-delay), the up-chain [[Venture]] is executed repeatedly, yielding each result, until the schedule is canceled.
		 * Is equivalent to {{{ thisVenture.transformWith(tryA => Venture_schedulesFlat(schedule)(_ => f(tryA)) }}} but more efficient.
		 * $notReusableTask */
		inline def scheduledTransformWith[B](schedule: Schedule)(f: Try[A] => Venture[B]): Venture[B] =
			new ScheduledTransformWith(thisVenture, schedule, f)

		/** Like [[Venture.transformWith]] but the function application is delayed.
		 * Note that what is delayed is the function application, not the execution of the up-chain [[Venture]]. The delay occurs only after the up-chain [[Venture]] is completed.
		 * Is equivalent to {{{ thisVenture.transformWith(tryA => Venture_delaysFlat(delay)(_ => f(tryA)) }}} but more efficient.
		 * */
		inline def delayedTransformWith[B](delay: MilliDuration, f: Try[A] => Venture[B]): Venture[B] =
			new DelayedTransformWith(thisVenture, delay, f)

		/**
		 * Returns a [[Venture]] that waits for the up-chain [[Venture]] to yield a result, but only for a limited time.
		 * The time limit is determined by the initial delay of the provided [[Schedule]].
		 * If the up-chain [[Venture]] yields a result within the time limit, the returned [[Venture]] yields that result wrapped in [[Maybe.some]].
		 * If the time limit is exceeded, the returned [[Venture]] yields [[Maybe.empty]] immediately and does not wait for the up-chain result.
		 * The up-chain [[Venture]] is executed regardless and may complete in the background after the timeout.
		 * The [[Schedule]] is activated when the returned [[Venture]] is executed and canceled when it completes. Therefore, fixed-rate and fixed-delay kind schedules are worthless.
		 * If the [[Schedule]] is cancelled before the time limit, then the returned [[Venture]] waits the up-chain [[Venture]] completion forever, ensuring a non-empty result (provided there is one).
		 *
		 * @param schedule a [[Schedule]] whose initial delay is the maximum time to wait for a result, measured from the start of the returned [[Task]]'s execution.
		 *                 If the up-chain [[Task]] yields a result before this time elapses, the result is wrapped in [[Maybe.some]]. If the timer expires first, the returned [[Task]] yields [[Maybe.empty]] immediately without waiting any more.
		 * @return a [[Task]] that will complete with [[Maybe.some]] wrapping the result if it is available within the time limit, or with [[Maybe.empty]] otherwise.
		 */
		inline def timeBounded(schedule: Schedule): Venture[Maybe[A]] =
			new TimeLimitedVenture[A](thisVenture.subscribeSync, 0, schedule)

		/**
		 * Returns a [[Venture]] that waits for the up-chain [[Venture]] to yield a result, but only for a limited time.
		 * If the up-chain [[Venture]] yields a result within the time limit, the returned [[Venture]] yields that result wrapped in [[Maybe.some]].
		 * If the time limit is exceeded, the returned [[Venture]] yields [[Maybe.empty]] immediately, ignoring the future up-chain result.
		 * The up-chain [[Venture]] is executed regardless and may complete in the background after the timeout.
		 * $notReusableTask
		 *
		 * @param limit the maximum time to wait for a result, measured from the start of the returned [[Task]]'s execution. If the up-chain [[Task]] yields a result before this time elapses, the result is wrapped in [[Maybe.some]]. If the timer expires first, the returned [[Task]] yields [[Maybe.empty]] immediately without waiting any more.
		 * @return a [[Task]] that will complete with [[Maybe.some]] wrapping the result if it is available within the timeout, or with [[Maybe.empty]] if the timeout elapses first.
		 */
		inline def timeBounded(limit: MilliDuration): Venture[Maybe[A]] =
			new TimeLimitedVenture[A](thisVenture.subscribeSync, limit, null)


		/**
		 * Repeats the up-chain [[Venture]] whenever its execution duration exceeds a specified limit, up to a maximum number of retries.
		 * Each retry is triggered immediately after the previous attempt times out, with no delay between retries.
		 * The up-chain [[Venture]] is not cancelled when it times out; it continues executing in the background even as retries begin.
		 * The time limit is best-effort: it does not forcibly interrupt the up-chain [[Venture]], but determines whether a retry should be initiated.
		 * If the up-chain [[Venture]] has side effects, they will occur once per attempt, resulting in a total of one plus the number of retries.
		 * Equivalent to the [[Task]]'s [[retriedOnTimeout]] method but for [[Venture]].
		 *
		 * @param limit      the maximum duration allowed for each execution of the up-chain [[Venture]] before triggering a retry.
		 * @param maxRetries the maximum number of retries permitted after the initial attempt.
		 * @return a [[Venture]] that yields [[Maybe.some]] containing the result of the up-chain [[Venture]] if any attempt completes within the time limit; otherwise, yields [[Maybe.empty]] as soon as the final attempt times out.
		 */
		def reattemptedOnTimeout(limit: MilliDuration, maxRetries: Int): Venture[Maybe[A]] = {
			thisVenture.timeBounded(limit).flatMap(_.fold {
				if maxRetries > 0 then reattemptedOnTimeout(limit, maxRetries - 1)
				else Venture_successful(Maybe.empty)
			} { r =>
				Venture_successful(Maybe(r))
			})
		}
	}

	//// Task factory methods ////

	/** Builds a [[Venture]] that, once executed, does nothing but yields a value of `()` after the specified duration.
	 * The delay period begins when the returned [[Venture]] is started, not when it is built.
	 * This is equivalent to both `Task_unit.delayed(duration)` and `Task_delay(duration)(() => ())`.
	 *
	 * @param duration the time to wait before the [[Venture]] yields its result.
	 * @return a new [[Venture]] that will yield a value of `()` after the specified delay.
	 */
	inline def Venture_sleeps(duration: MilliDuration): Venture[Unit] =
		Venture_unit.delayed(duration)

	/**
	 * Builds a [[Venture]] that schedules the execution of a supplier function according to a specified [[Schedule]] and yields the supplier’s result for each scheduled execution.
	 * The schedule is activated only when the returned [[Venture]] is started, not when it is constructed.
	 * For periodic schedules (e.g., fixed-rate or fixed-delay), the supplier is executed repeatedly, yielding each result, until the schedule is canceled.
	 *
	 * $notReusableTask
	 * @param schedule the [[Schedule]] controlling when the supplier function is executed.
	 * @param supplier the function that produces a value of type [[A]] for each scheduled execution.
	 * @return a [[Venture]] that yields the supplier’s result(s) according to the specified [[Schedule]].
	 */
	inline def Venture_schedules[A](schedule: Schedule)(supplier: Schedule => Try[A]): Venture[A] =
		new DelayedSupplierVenture(0, schedule, supplier)

	/**
	 * Builds a [[Venture]] that schedules the execution of a [[Venture]] builder according to a specified [[Schedule]] and yields the results of the [[Venture]] produced by the builder for each scheduled execution.
	 * The schedule is activated only when the returned [[Venture]] is started, not when it is constructed.
	 * For periodic schedules (e.g., fixed-rate or fixed-delay), the builder is executed repeatedly, producing a new [[Venture]] for each execution, and the results of each produced [[Venture]] are yielded until the schedule is canceled.
	 * This [[Venture]] is not reusable and can only be executed once.
	 *
	 * @param schedule the [[Schedule]] controlling when the [[Venture]] builder is executed.
	 * @param builder  the function that produces a new [[Venture[A]]] for each scheduled execution.
	 * @return a [[Venture]] that yields the results of the [[Venture]] produced by the builder according to the specified [[Schedule]].
	 */
	inline def Venture_schedulesFlat[A](schedule: Schedule)(builder: Schedule => Venture[A]): Venture[A] =
		new DelayedSupplierFlatVenture(0, schedule, builder)

	/**
	 * Builds a [[Venture]] that waits for a specified duration before executing a supplier function and yielding its result.
	 * The delay begins only when the returned [[Venture]] is started, not when it is constructed.
	 * The supplier is executed once after the delay, and its result is what the returned [[Venture]] yields.
	 *
	 * @param duration the duration to wait before executing the supplier function.
	 * @param supplier the function that produces a result after the delay.
	 * @return a [[Venture]] that yields the supplier’s result after the specified duration.
	 */
	inline def Venture_delays[A](duration: MilliDuration)(supplier: Schedule => Try[A]): Venture[A] =
		new DelayedSupplierVenture(duration, null, supplier)

	/**
	 * Builds a [[Venture]] that waits for a specified duration before executing a [[Venture]] builder and yielding the result of the produced [[Venture]].
	 * The delay begins only when the returned [[Venture]] is started, not when it is constructed.
	 * The builder is executed once after the delay, producing a [[Venture]] whose result is yielded by the returned [[Venture]].
	 *
	 * @param duration the duration to wait before executing the [[Venture]] builder.
	 * @param builder  the function that produces a new [[Venture]] after the delay.
	 * @return a [[Venture]] that yields the result of the [[Venture]] produced by the builder after the specified duration.
	 */
	inline def Venture_delaysFlat[A](duration: MilliDuration)(builder: Schedule => Venture[A]): Venture[A] =
		new DelayedSupplierFlatVenture(duration, null, builder)

	/**
	 * Builds a [[Venture]] that executes a supplier function and yields its result if the execution duration is less than a specified limit.
	 * If the execution exceeds the limit, the supplier is retried immediately, up to a maximum number of retries.
	 * The supplier is not stopped when it times out; it continues executing in the background even as retries begin.
	 * The time limit is best-effort: it does not forcibly interrupt the supplier function, but determines whether a retry should be initiated.
	 * If the supplier has side effects, they will occur once per attempt, resulting in a total of one plus the number of retries.
	 * The supplier receives the number of failed attempts as a parameter, allowing it to adjust its behavior based on prior timeouts.
	 *
	 * @param limit         the maximum duration allowed for each execution of the supplier function before triggering a retry.
	 * @param maxRetries    the maximum number of retries permitted after the initial attempt.
	 * @param supplier          the supplier function that produces a value of type [[A]], taking the number of failed attempts as an input.
	 * @return a [[Venture]] that yields [[Maybe.some]] containing the result of the supplier function if any attempt completes within the time limit; otherwise, yields [[Maybe.empty]] as soon as the final attempt times out.
	 */
	def Venture_retryOnTimeout[A](limit: MilliDuration, maxRetries: Int, supplier: Int => Try[A]): Venture[Maybe[A]] = {
		def loop(failedAttempts: Int): Venture[Maybe[A]] = {
			TimeLimitedVenture[A](_(supplier(failedAttempts)), limit, null)
				.flatMap(_.fold {
					if failedAttempts >= maxRetries then Venture_successful(Maybe.empty)
					else loop(failedAttempts + 1)
				} { a =>
					Venture_successful(Maybe(a))
				})
		}

		loop(0)
	}

	//// Task implementation classes ////

	/** $notReusableTask */
	final class ScheduledVenture[A](venture: Venture[A], aSchedule: Schedule) extends AbstractVenture[A] {
		override def subscribeSync(monoObserver: MonoObserver[Try[A]]): Subscription = {
			// Changed to return a Subscription to support the modernized cancel/unsubscribe flow.
			// The returned subscription cancels the scheduled task and handles inner subscription cancellation.
			new Subscription {
				private var active = true
				private var innerSub: Subscription = Subscription_empty

				{
					schedule(aSchedule) { _ =>
						if active then {
							innerSub = venture.subscribeSync(monoObserver)
						}
					}
				}

				override def unsubscribe(): Unit = {
					checkWithin()
					active = false
					innerSub.unsubscribe()
				}
			}
		}
	}

	/** $notReusableTask */
	final class ScheduledTransform[A, B](venture: Venture[A], aSchedule: Schedule, f: Try[A] => Try[B]) extends AbstractVenture[B] {
		override def subscribeSync(monoObserver: MonoObserver[Try[B]]): Subscription = {
			// Changed to return a Subscription to support the modernized cancel/unsubscribe flow.
			// Propagates the unsubscribe call back to the underlying upstream venture.
			new Subscription {
				private var active = true
				private var innerSub: Subscription = Subscription_empty

				{
					innerSub = venture.subscribeSync(new MonoObserver[Try[A]] {
						override def onSuccess(tryA: Try[A]): Unit = {
							if active then {
								schedule(aSchedule) { _ =>
									if active then {
										val tryB =
											try f(tryA)
											catch {
												case NonFatal(e) => Failure(e)
											}
										monoObserver.onSuccess(tryB)
									}
								}
							}
						}

						override def onError(ex: Throwable): Unit = {
							if active then {
								monoObserver.onSuccess(Failure(ex))
							}
						}
					})
				}

				override def unsubscribe(): Unit = {
					checkWithin()
					active = false
					innerSub.unsubscribe()
				}
			}
		}
	}

	/** $notReusableTask */
	final class ScheduledTransformWith[A, B](ventureA: Venture[A], aSchedule: Schedule, f: Try[A] => Venture[B]) extends AbstractVenture[B] {
		override def subscribeSync(monoObserver: MonoObserver[Try[B]]): Subscription = {
			// Changed to return a Subscription to support the modernized cancel/unsubscribe flow.
			// Propagates cancellation to both the outer (upstream) venture and the inner venture.
			new Subscription {
				private var active = true
				private var innerSub: Subscription = Subscription_empty

				{
					innerSub = ventureA.subscribeSync(new MonoObserver[Try[A]] {
						override def onSuccess(tryA: Try[A]): Unit = {
							if active then {
								schedule(aSchedule) { _ =>
									if active then {
										val ventureB =
											try f(tryA)
											catch {
												case NonFatal(e) => Venture_failed(e)
											}
										innerSub = ventureB.subscribeSync(monoObserver)
									}
								}
							}
						}

						override def onError(ex: Throwable): Unit = {
							if active then {
								monoObserver.onSuccess(Failure(ex))
							}
						}
					})
				}

				override def unsubscribe(): Unit = {
					checkWithin()
					active = false
					innerSub.unsubscribe()
				}
			}
		}
	}

	final class DelayedVenture[A](venture: Venture[A], delay: MilliDuration) extends AbstractVenture[A] {
		override def subscribeSync(monoObserver: MonoObserver[Try[A]]): Subscription = {
			// Changed to return a Subscription to support the modernized cancel/unsubscribe flow.
			// It guards the delay timer and propagates cancellation down to the inner venture.
			new Subscription {
				private var active = true
				private var innerSub: Subscription = Subscription_empty

				{
					schedule(newDelaySchedule(delay)) { _ =>
						if active then {
							innerSub = venture.subscribeSync(monoObserver)
						}
					}
				}

				override def unsubscribe(): Unit = {
					checkWithin()
					active = false
					innerSub.unsubscribe()
				}
			}
		}
	}

	final class DelayedTransform[A, B](venture: Venture[A], delay: MilliDuration, f: Try[A] => Try[B]) extends AbstractVenture[B] {
		override def subscribeSync(monoObserver: MonoObserver[Try[B]]): Subscription = {
			// Changed to return a Subscription to support the modernized cancel/unsubscribe flow.
			// Propagates cancellation upstream to the source venture and guards scheduled callback execution.
			new Subscription {
				private var active = true
				private var innerSub: Subscription = Subscription_empty

				{
					innerSub = venture.subscribeSync(new MonoObserver[Try[A]] {
						override def onSuccess(tryA: Try[A]): Unit = {
							if active then {
								schedule(newDelaySchedule(delay)) { _ =>
									if active then {
										val tryB =
											try f(tryA)
											catch {
												case NonFatal(e) => Failure(e)
											}
										monoObserver.onSuccess(tryB)
									}
								}
							}
						}

						override def onError(ex: Throwable): Unit = {
							if active then {
								monoObserver.onSuccess(Failure(ex))
							}
						}
					})
				}

				override def unsubscribe(): Unit = {
					checkWithin()
					active = false
					innerSub.unsubscribe()
				}
			}
		}
	}

	final class DelayedTransformWith[A, B](venture: Venture[A], delay: MilliDuration, f: Try[A] => Venture[B]) extends AbstractVenture[B] {
		override def subscribeSync(monoObserver: MonoObserver[Try[B]]): Subscription = {
			// Changed to return a Subscription to support the modernized cancel/unsubscribe flow.
			// Propagates cancellation to both upstream and inner ventures during delay.
			new Subscription {
				private var active = true
				private var innerSub: Subscription = Subscription_empty

				{
					innerSub = venture.subscribeSync(new MonoObserver[Try[A]] {
						override def onSuccess(tryA: Try[A]): Unit = {
							if active then {
								schedule(newDelaySchedule(delay)) { _ =>
									if active then {
										val ventureB =
											try f(tryA)
											catch {
												case NonFatal(e) => Venture_failed(e)
											}
										innerSub = ventureB.subscribeSync(monoObserver)
									}
								}
							}
						}

						override def onError(ex: Throwable): Unit = {
							if active then {
								monoObserver.onSuccess(Failure(ex))
							}
						}
					})
				}

				override def unsubscribe(): Unit = {
					checkWithin()
					active = false
					innerSub.unsubscribe()
				}
			}
		}
	}

	/**
	 * This [[Venture]] is reusable only when limit2 is null.
	 */
	final class TimeLimitedVenture[A](upChain: (Try[A] => Unit) => Unit, limit1: MilliDuration, limit2: Schedule | Null) extends AbstractVenture[Maybe[A]] {
		override def subscribeSync(monoObserver: MonoObserver[Try[Maybe[A]]]): Subscription = {
			val timer: Schedule = if limit2 eq null then newDelaySchedule(limit1) else limit2.asInstanceOf[Schedule]
			var hasElapsed = false
			var hasCompleted = false
			// Changed to return a Subscription to support the modernized cancel/unsubscribe flow.
			// The returned subscription cancels the delay timer and ignores any further callbacks.
			new Subscription {
				private var active = true

				{
					schedule(timer) { _ =>
						if active then {
							cancel(timer)
							if !hasCompleted then {
								hasElapsed = true
								monoObserver.onSuccess(Success(Maybe.empty))
							}
						}
					}
					upChain { tryA =>
						if active && !hasElapsed then {
							cancel(timer)
							hasCompleted = true
							tryA match {
								case Success(a) => monoObserver.onSuccess(Success(Maybe(a)))
								case f: Failure[A] => monoObserver.onSuccess(f.castTo[Maybe[A]])
							}
						}
					}
				}

				override def unsubscribe(): Unit = {
					checkWithin()
					active = false
					cancel(timer)
				}
			}
		}
	}

	/**
	 * Caution: This [[Venture]] is reusable only when limit2 is null.
	 */
	final class DelayedSupplierVenture[A](limit1: MilliDuration, limit2: Schedule | Null, supplier: Schedule => Try[A]) extends AbstractVenture[A] {
		override def subscribeSync(monoObserver: MonoObserver[Try[A]]): Subscription = {
			val timer: Schedule = if limit2 eq null then newDelaySchedule(limit1) else limit2.asInstanceOf[Schedule]
			// Changed to return a Subscription to support the modernized cancel/unsubscribe flow.
			// The returned subscription cancels the scheduler timer and ignores the supplier callback if inactive.
			new Subscription {
				private var active = true
				{
					schedule(timer)(_ => if active then monoObserver.onSuccess(supplier(timer)))
				}

				override def unsubscribe(): Unit = {
					checkWithin()
					active = false
					cancel(timer)
				}
			}
		}
	}

	/**
	 * Caution: This [[Venture]] is reusable only when limit2 is null.
	 */
	final class DelayedSupplierFlatVenture[A](limit1: MilliDuration, limit2: Schedule | Null, supplier: Schedule => Venture[A]) extends AbstractVenture[A] {
		override def subscribeSync(monoObserver: MonoObserver[Try[A]]): Subscription = {
			val timer: Schedule = if limit2 eq null then newDelaySchedule(limit1) else limit2.asInstanceOf[Schedule]
			// Changed to return a Subscription to support the modernized cancel/unsubscribe flow.
			// The returned subscription cancels the scheduler timer and propagates cancellation to the inner venture.
			new Subscription {
				private var active = true
				private var innerSub: Subscription = Subscription_empty
				{
					schedule(timer)(_ => if active then innerSub = supplier(timer).subscribeSync(monoObserver))
				}

				override def unsubscribe(): Unit = {
					checkWithin()
					active = false
					cancel(timer)
					innerSub.unsubscribe()
				}
			}
		}
	}
}
