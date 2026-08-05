package readren.sequencer

import SchedulingExtension.{DELAY, FIXED_DELAY, FIXED_RATE, ScheduleKind}

import readren.common.Maybe
import readren.sequencer.Doer

import scala.annotation.targetName
import scala.util.control.NonFatal


object SchedulingExtension {
	opaque type ScheduleKind <: Int = Int
	val DELAY: ScheduleKind = 1
	val FIXED_RATE: ScheduleKind = 2
	val FIXED_DELAY: ScheduleKind = 3
}

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
trait SchedulingExtension extends SchedulingDoerFluxPart { thisDoer: Doer =>


	/** Represents an execution schedule.
	 * It is tied to the routine passed along it to the [[schedule]] method. This means that it is mutable and, therefore, non referentially transparent and illegal to use the same instance in more than one call to [[schedule]].
	 * Given all the operations added to [[Mono]] by this extension ([[SchedulingExtension]]) rely explicitly or implicitly on a [[Schedule]] instance, they all are also not referentially transparent.
	 * TODO: avoid the limitation of using the same instance in more than one call to [[schedule]], by enforcing [[Schedule]] to be referentially transparent. This change requires that instances of [[Schedule]] to be associated to all the routines that accompanied it in a calls to [[schedule]], and that the `cancel` method to apply to all of them. */
	type Schedule <: AnyRef
	type Delay <: Schedule

	trait TimedSubscription extends Subscription {
		def schedule: Schedule
	}

	trait TimedTask[+A] extends Task[A] {
		override def subscribeSync(downChainObserver: MonoObserver[A]): TimedSubscription

		inline final def andOnSubscription(action: Schedule => Unit): TimedTask[A] = new Task_OnSubscription[A](this, action)
	}

	/** Creates a [[Delay]] for a single time execution after a delay.
	 * If the delay is non-positive, the execution would be as soon as possible.
	 * @param delay duration before the execution.
	 * @return a [[Delay]] instance intended solely as an argument for a single call to the [[schedule]] method. */
	def newDelaySchedule(delay: MilliDuration): Delay

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

	/** @return true if the [[Schedule]] was used in a call to [[scheduleSequentially]], even if it is canceled. */
	def wasActivated(schedule: Schedule): Boolean

	/** @return true if the [[Schedule]] was cancelled, even if it was not activated.
	 * An [[Schedule]] instance becomes canceled when either, it is passed to the [[cancel]] method, or the [[cancelAll]] method is called after it [[wasActivated]]. */
	def isCanceled(schedule: Schedule): Boolean

	inline def schedule(schedule: Schedule)(routine: Schedule => Unit): Unit =
		scheduleSequentially(schedule, routine)

	//// Task extension methods ////

	extension [A](thisTask: Task[A]) {

		/** Returns a [[Task]] that triggers the up-chain [[Task]] after a delay measured from the moment the returned [[Task]] is executed. */
		// @targetName("delayedTask")
		inline def delayed(delay: MilliDuration): TimedTask[A] = {
			new Task_Delayed(thisTask, delay)
		}

		/**
		 * Returns a [[Task]] that waits for the up-chain [[Mono]] to yield a result, but only for a limited time.
		 * If the up-chain [[Task]] yields a result within the time limit, the returned [[Task]] yields that result wrapped in [[Maybe.some]].
		 * If the time limit is exceeded, the up-chain [[Mono]] is canceled and the returned [[Task]] yields [[Maybe.empty]] immediately (does not wait for the up-chain result).
		 *
		 * @param limit the maximum time to wait for a result, measured from the start of the returned [[Task]]'s execution. If the up-chain [[Task]] yields a result before this time elapses, the result is wrapped in [[Maybe.some]]. If the timer expires first, the returned [[Task]] yields [[Maybe.empty]] immediately without waiting any more.
		 * @return a [[Task]] that yields [[Maybe.some]] containing the result of the up-chain [[Task]] if it completes within the time limit; otherwise, yields [[Maybe.empty]] upon limit elapses.
		 */
		inline def timeLimited(limit: MilliDuration): TimedTask[Maybe[A]] = {
			new Task_TimeLimited[A](thisTask, limit)
		}

		/**
		 * Repeats the up-chain [[Task]] whenever its execution duration exceeds a specified limit, up to a maximum number of retries.\
		 * Each retry is triggered immediately after the previous attempt times out, with no delay between retries.\
		 * The timed-out up-chain [[Subscription]]s are canceled.
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

	/** Builds a [[Task]] that, once executed, does nothing but yields a value of `()` after the specified duration.\
	 * The delay period begins whenever the returned [[Task]] is started, not when it is built.\
	 * This is equivalent to both `Task_unit.delayed(duration)` and `Task_delays(duration)(_ => ())`.\
	 *
	 * @param duration the time to wait before the [[Task]] yields its result.
	 * @return a new [[Task]] that will yield a value of `()` after the specified delay. */
	inline def Task_sleeps(duration: MilliDuration): TimedTask[Unit] =
		Task_unit.delayed(duration)

	/**
	 * Builds a [[Task]] that waits for a specified duration before executing a supplier function and yielding its result.\
	 * The delay begins only whenever the returned [[Task]] is started, not when it is constructed.\
	 * The supplier is executed once after the delay, and its result is what the returned [[Task]] yields.\
	 *
	 * @param duration the duration to wait before executing the supplier function.
	 * @param supplier the function that produces a value of type [[A]] after the delay.
	 * @return a [[Task]] that yields the supplier’s result after the specified duration. */
	inline def Task_delays[A](duration: MilliDuration)(supplier: TimedSubscription => A): TimedTask[A] =
		new Task_DelaysSupplier(duration, supplier)

	/**
	 * Builds a [[Task]] that waits for a specified duration before executing a [[Task]] builder and yielding the result of the produced [[Task]].\
	 * The delay begins only whenever the returned [[Task]] is started, not when it is constructed.\
	 * The builder is executed once after the delay, producing a [[Task]] whose result is yielded by the returned [[Task]].\
	 *
	 * @param duration the duration to wait before executing the [[Task]] builder.
	 * @param builder  the function that produces a new [[Task[A]]] after the delay.
	 * @return a [[Task]] that yields the result of the [[Task]] produced by the builder after the specified duration. */
	inline def Task_delaysFlat[A](duration: MilliDuration)(builder: TimedSubscription => Task[A]): TimedTask[A] =
		new Task_DelaysSupplierFlat(duration, builder)

	/**
	 * Builds a [[Task]] that executes a supplier function and yields its result if the execution duration is less than a specified limit.\
	 * If the execution exceeds the limit, the supplier is retried immediately, up to a maximum number of retries.\
	 * The supplier is not stopped when it times out; it continues executing in the background even as retries begin.\
	 * The time limit is best-effort: it does not forcibly interrupt the supplier function, but determines whether a retry should be initiated.\
	 * If the supplier has side effects, they will occur once per attempt, resulting in a total of one plus the number of retries.\
	 * The supplier receives the number of failed attempts as a parameter, allowing it to adjust its behavior based on prior timeouts.
	 *
	 * @param limit         the maximum duration allowed for each execution of the supplier function before triggering a retry.
	 * @param maxAttempts    the maximum number of attempts permitted.
	 * @param supplier          the supplier function that takes the number of failed attempts and produces the [[Task]] to be time limited.
	 * @return a [[Task]] that yields [[Maybe.some]] containing the result of the supplier function if any attempt completes within the time limit; otherwise, yields [[Maybe.empty]] as soon as the final attempt times out. */
	def Task_retriesOnTimeout[A](limit: MilliDuration, maxAttempts: Int, supplier: (attemptsDone: Int) => Task[A]): Task[Maybe[A]] = {
		def loop(attemptsDone: Int): Task[Maybe[A]] = {
			if attemptsDone >= maxAttempts then Task_ready(Maybe.empty)
			else {
				var maybeFailure: Maybe[Throwable] = Maybe.empty
				val maybeTaskA = try Maybe(supplier(attemptsDone)) catch {
					case NonFatal(e) =>
						maybeFailure = Maybe(e)
						Maybe.empty
				}
				maybeFailure.fold {
					maybeTaskA.get.timeLimited(limit).flatMap { maybeA =>
						maybeA.fold(loop(attemptsDone + 1)) { a => Task_ready(maybeA) }
					}
				}(Task_fail)
			}
		}

		loop(0)
	}

	//// Task operations implementation classes ////

	protected inline def buildSchedule(kind: ScheduleKind, initialDelay: MilliDuration, loopDelay: MilliDuration): Schedule = {
		kind match {
			case DELAY => newDelaySchedule(initialDelay)
			case FIXED_RATE => newFixedRateSchedule(initialDelay, loopDelay)
			case FIXED_DELAY => newFixedDelaySchedule(initialDelay, loopDelay)
		}
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_Delayed(trap: Nothing): Any = trap

	final class Task_Delayed[A](monoA: Mono[A], delay: MilliDuration) extends AbstractTask[A], TimedTask[A] {
		override def subscribeSync(downChainObserver: MonoObserver[A]): TimedSubscription = {
			new TimedSubscription with MonoObserver[A] with (Schedule => Unit) {
				private val aSchedule = newDelaySchedule(delay)
				private var isActive = true
				private var maybeUpChainSubscription: Maybe[Subscription] = Maybe.empty

				override def schedule: Schedule = aSchedule

				{ // Constructor
					thisDoer.schedule(aSchedule)(this)
				}

				override def apply(aSchedule: Schedule): Unit = {
					if isActive then {
						val ucs = monoA.subscribeSync(this)
						if isActive then maybeUpChainSubscription = Maybe(ucs)
					}
				}

				override def onSuccess(a: A): Unit = {
					if isActive then {
						maybeUpChainSubscription = Maybe.empty
						downChainObserver.onSuccess(a)
					}
				}

				override def onError(e: Throwable): Unit = {
					if isActive then {
						maybeUpChainSubscription = Maybe.empty
						downChainObserver.onError(e)
					}
				}

				override def unsubscribeSync(): Unit = {
					if isActive then {
						isActive = false
						cancel(aSchedule)
						val ucs = maybeUpChainSubscription
						maybeUpChainSubscription = Maybe.empty
						ucs.foreach(_.unsubscribeSync())
					}
				}
			}
		}
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_TimeLimited(trap: Nothing): Any = trap

	final class Task_TimeLimited[A](monoA: Mono[A], limit: MilliDuration) extends AbstractTask[Maybe[A]], TimedTask[Maybe[A]] {
		override def subscribeSync(downChainObserver: MonoObserver[Maybe[A]]): TimedSubscription = {
			new TimedSubscription with MonoObserver[A] with (Schedule => Unit) { thisSubOb =>
				private val timer: Schedule = newDelaySchedule(limit)
				private var isActive = true
				private var maybeUpChainSubscription: Maybe[Subscription] = Maybe.empty

				override def schedule: Schedule = timer

				{
					val upChainSubscription = monoA.subscribeSync(thisSubOb)
					if isActive then {
						maybeUpChainSubscription = Maybe(upChainSubscription)
						thisDoer.schedule(timer)(thisSubOb)
					}
				}

				override def apply(timer: Schedule): Unit = {
					if isActive then {
						maybeUpChainSubscription.foreach(_.unsubscribeSync())
						maybeUpChainSubscription = Maybe.empty
						downChainObserver.onSuccess(Maybe.empty)
					}
				}

				override def onSuccess(a: A): Unit = {
					if isActive then {
						cancel(timer)
						maybeUpChainSubscription = Maybe.empty
						downChainObserver.onSuccess(Maybe(a))
					}
				}

				override def onError(e: Throwable): Unit = {
					if isActive then {
						cancel(timer)
						maybeUpChainSubscription = Maybe.empty
						downChainObserver.onError(e)
					}
				}

				override def unsubscribeSync(): Unit = {
					if isActive then {
						isActive = false
						cancel(timer)
						val ucs = maybeUpChainSubscription
						maybeUpChainSubscription = Maybe.empty
						ucs.foreach(_.unsubscribeSync())
					}
				}
			}
		}
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_OnSubscription(trap: Nothing): Any = trap

	final class Task_OnSubscription[+A](taskA: TimedTask[A], action: Schedule => Unit) extends AbstractTask[A], TimedTask[A] {

		override def subscribeSync(downChainObserver: MonoObserver[A]): TimedSubscription = {
			val upChainSubscription = taskA.subscribeSync(downChainObserver)
			try {
				action(upChainSubscription.schedule)
				upChainSubscription
			} catch {
				case scala.util.control.NonFatal(e) =>
					upChainSubscription.unsubscribeSync()
					throw e
			}
		}
	}

	//// Capturer extension methods ////

	extension [A](thisCapturer: Capturer[A]) {

		/** Returns a [[Capturer]] that subscribes to the up-chain [[Capturer]] after a delay determined by the provided [[Delay]]. */
		@targetName("delayedLatchingTask")
		inline def delayed(delay: Delay): Capturer[A] = {
			new Capturer_Delayed(thisCapturer, delay)
		}

		/**
		 * Returns a [[Capturer]] that waits for the up-chain [[Capturer]] to yield a result, but only for a limited time determined by the provided [[Delay]].
		 * If the up-chain [[Capturer]] yields a result within the time limit, the returned [[Capturer]] yields that result wrapped in [[Maybe.some]].
		 * If the time limit is exceeded, the up-chain [[Capturer]] is canceled and the returned [[Capturer]] yields [[Maybe.empty]] immediately.
		 */
		inline def timeLimited(delay: Delay): Capturer[Maybe[A]] = {
			new Capturer_TimeLimited[A](thisCapturer, delay)
		}
	}

	/** Builds a [[Capturer]] that schedules the execution of a supplier function after a specified delay.\
	 * The delay begins immediately when this method is called (hot/eager start).\
	 * The supplier is executed once after the delay, and its result is what the returned [[Capturer]] yields.
	 *
	 * @param delay the schedule delay that determines when the supplier function will be executed.
	 * @param supplier the function that produces a value of type [[A]] after the delay.
	 * @return a [[Capturer]] that yields the supplier’s result. */
	def Capturer_delay[A](delay: Delay)(supplier: Schedule => A): Capturer[A] = new Captor[A] with (Schedule => Unit) {
		schedule(delay)(this)

		override def apply(schedule: Schedule): Unit = {
			val maybeA = try Maybe(supplier(schedule)) catch {
				case NonFatal(e) =>
					trapSync(e)
					Maybe.empty
			}
			maybeA.foreach(a => captureSync(a))
		}
	}

	/** Builds a [[Capturer]] that waits for a specified delay before executing a [[Capturer]] builder and yielding the result of the produced [[Capturer]].\
	 * The delay begins immediately when this method is called (hot/eager start).\
	 * The builder is executed once after the delay, producing a [[Capturer]] whose result is yielded by the returned [[Capturer]].
	 *
	 * @param delay the schedule delay that determines when the [[Capturer]] builder will be executed.
	 * @param builder the function that produces a new [[Capturer[A]]] after the delay.
	 * @return a [[Capturer]] that yields the result of the [[Capturer]] produced by the builder. */
	def Capturer_delayFlat[A](delay: Delay)(builder: Schedule => Capturer[A]): Capturer[A] = new Captor[A] with (Schedule => Unit) {
		schedule(delay)(this)

		override def apply(schedule: Schedule): Unit = {
			val maybeCapturerA = try Maybe(builder(schedule)) catch {
				case NonFatal(e) =>
					trapSync(e)
					Maybe.empty
			}
			maybeCapturerA.foreach(capturerA => seizeWithSync(capturerA))
		}
	}

	//// Capturer operations implementation classes ////

	/** $suppressSyntheticCompanionObject */
	private inline def Capturer_Delayed(trap: Nothing): Any = trap

	final class Capturer_Delayed[A](capturer: Capturer[A], delay: Delay) extends Captor[A], (Schedule => Unit), MonoObserver[A] {
		private var isActive = true
		private var maybeUpChainSubscription: Maybe[Subscription] = Maybe.empty

		{ // Constructor
			thisDoer.schedule(delay)(this)
		}

		override def apply(schedule: Schedule): Unit = {
			if isActive then {
				val upChainSubscription = capturer.subscribeSync(this)
				if isActive then {
					maybeUpChainSubscription = Maybe(upChainSubscription)
				}
			}
		}

		override def onSuccess(a: A): Unit = {
			isActive = false
			captureSync(a)
		}

		override def onError(e: Throwable): Unit = {
			isActive = false
			trapSync(e)
		}
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Capturer_TimeLimited(trap: Nothing): Any = trap

	final class Capturer_TimeLimited[A](capturer: Capturer[A], delay: Delay) extends Captor[Maybe[A]], (Schedule => Unit), MonoObserver[A] {
		private var isActive = true
		private var maybeUpChainSubscription: Maybe[Subscription] = Maybe.empty

		{ // Constructor
			val upChainSubscription = capturer.subscribeSync(this)
			if isActive then {
				maybeUpChainSubscription = Maybe(upChainSubscription)
				thisDoer.schedule(delay)(this)
			}
		}

		override def apply(schedule: Schedule): Unit = {
			if isActive then {
				isActive = false
				maybeUpChainSubscription.foreach(_.unsubscribeSync())
				captureSync(Maybe.empty)
			}
		}

		override def onSuccess(a: A): Unit = {
			if isActive then {
				isActive = false
				cancel(delay)
				captureSync(Maybe.some(a))
			}
		}

		override def onError(e: Throwable): Unit = {
			if isActive then {
				isActive = false
				cancel(delay)
				trapSync(e)
			}
		}
	}

	//// Flux operations implementation classes ////

	/** $suppressSyntheticCompanionObject */
	private inline def Task_DelaysSupplier(trap: Nothing): Any = trap

	final class Task_DelaysSupplier[A](delay: MilliDuration, supplier: TimedSubscription => A) extends AbstractTask[A], TimedTask[A] {
		override def subscribeSync(downChainObserver: MonoObserver[A]): TimedSubscription = {
			new TimedSubscription with (Schedule => Unit) {
				private val aSchedule: Delay = newDelaySchedule(delay)
				private var isActive = true

				override def schedule: Schedule = aSchedule

				{ // Constructor
					thisDoer.schedule(aSchedule)(this)
				}

				override def apply(schedule: Schedule): Unit = {
					if isActive then {
						val maybeA = try Maybe(supplier(this)) catch {
							case NonFatal(e) =>
								isActive = false
								downChainObserver.onError(e)
								Maybe.empty
						}

						maybeA.foreach(downChainObserver.onSuccess)
					}
				}

				override def unsubscribeSync(): Unit = {
					if isActive then {
						isActive = false
						cancel(aSchedule)
					}
				}
			}
		}
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_DelaysSupplierFlat(trap: Nothing): Any = trap

	final class Task_DelaysSupplierFlat[A](delay: MilliDuration, supplier: TimedSubscription => Task[A]) extends AbstractTask[A], TimedTask[A] {
		override def subscribeSync(downChainObserver: MonoObserver[A]): TimedSubscription = {
			new TimedSubscription with (Schedule => Unit) {
				private val aSchedule: Schedule = newDelaySchedule(delay)
				private var isActive = true
				private var maybeInnerSubscription: Maybe[Subscription] = Maybe.empty

				override def schedule: Schedule = aSchedule

				{ // Constructor
					thisDoer.schedule(aSchedule)(this)
				}

				override def apply(schedule: Schedule): Unit = {
					if isActive then {
						val maybeTaskA = try Maybe(supplier(this)) catch {
							case NonFatal(e) =>
								isActive = false
								downChainObserver.onError(e)
								Maybe.empty
						}
						maybeTaskA.foreach { taskA =>
							val innerSubscription = taskA.subscribeSync(new MonoObserver[A] {
								override def onSuccess(a: A): Unit = {
									maybeInnerSubscription = Maybe.empty
									downChainObserver.onSuccess(a)
								}

								override def onError(e: Throwable): Unit = {
									maybeInnerSubscription = Maybe.empty
									downChainObserver.onError(e)
								}
							})
							if isActive then maybeInnerSubscription = Maybe(innerSubscription)
						}
					}
				}

				override def unsubscribeSync(): Unit = {
					if isActive then {
						isActive = false
						cancel(aSchedule)
						val mis = maybeInnerSubscription
						maybeInnerSubscription = Maybe.empty
						mis.foreach(_.unsubscribeSync())
					}
				}
			}
		}
	}
}
