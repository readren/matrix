package readren.sequencer

import readren.common.{Maybe, castTo, deriveToString}

import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

trait LoopingExtension { thisDoer: Doer =>
	
	//// DUTY INSTANCE OPERATIONS 
	extension [A] (thisDuty: Duty[A]) {
		/**
		 * Repeats this duty until applying the received function yields [[Maybe.some]].
		 * ===Detailed description===
		 * Creates a [[Duty]] that, when executed, it will:
		 * - execute this duty producing the result `a`
		 * - apply `condition` to `(completedCycles, a)`. If the evaluation finishes with:
		 *      - `some(b)`, completes with `b`
		 *      - `empty`, goes back to the first step.
		 *
		 * $threadSafe
		 *
		 * @param condition function that decides if the loop continues or not based on:
		 *  - the number of already completed cycles,
		 *  - and the result of the last execution of the `dutyA`.
		 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
		 * The loop ends when this function returns a [[Maybe.some]]. Its content will be the final result.
		 * @return a new [[Duty]] that, when executed, repeatedly executes this duty and applies the `condition` to the duty's result until the function's result is [[Maybe.some]]. The result of this duty is the contents of said [[Maybe]].
		 */
		inline def repeatedUntilSome[B](condition: (Int, A) => Maybe[B], maxRecursionDepthPerExecutor: Int = 9): Duty[B] =
			new Duty_RepeatUntilSome(thisDuty, condition, maxRecursionDepthPerExecutor)

		/**
		 * Like [[repeatedUntilSome]] but the condition is a [[PartialFunction]] instead of a function that returns [[Maybe]].
		 */
		inline def repeatedUntilDefined[B](pf: PartialFunction[(Int, A), B], maxRecursionDepthPerExecutor: Int = 9): Duty[B] =
			repeatedUntilSome(Function.untupled(Maybe.liftPartialFunction(pf)), maxRecursionDepthPerExecutor)

		/**
		 * Repeats this [[Duty]] while the given function returns [[Maybe.empty]].
		 * ===Detailed behavior===
		 * Returns a [[Duty]] that, when executed, it will:
		 *  - Apply the `condition` function to `(n, s0)` where `n` is the number of already completed evaluations of it (starts with zero).
		 *  - If the evaluation returns:
		 *      - `some(b)`, completes with `b`.
		 *      - `empty`, executes the `dutyA` and goes back to the first step replacing `s0` with the result.
		 *
		 * $threadSafe
		 *
		 * @param s0 the value passed as second parameter to `condition` the first time it is evaluated.
		 * @param condition determines whether a new cycle should be performed based on the number of times it has already been evaluated and either the result of the previous cycle or `da0` if no cycle has been done yet.
		 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
		 * @tparam S a supertype of `A`
		 * @tparam B the type of the result of this duty.
		 */
		inline def repeatedWhileEmpty[S >: A, B](s0: S, condition: (Int, S) => Maybe[B], maxRecursionDepthPerExecutor: Int = 9): Duty[B] =
			new Duty_RepeatWhileEmpty[S, B](thisDuty, s0, condition, maxRecursionDepthPerExecutor)

		/**
		 * Returns a duty that, when executed, repeatedly executes this [[Duty]] while a [[PartialFunction]] is undefined.
		 * ===Detailed behavior===
		 * Returns a [[Duty]] that, when executed, it will:
		 *  - Check if the partial function is defined in `(n, s0)` where `n` is the number of already completed evaluations of it (starts with zero).
		 *  - If it is undefined, executes the `dutyA` and goes back to the first step replacing `s0` with the result.
		 *  - If it is defined, evaluates it and completes with the result.
		 *
		 * $threadSafe
		 *
		 * @param s0 the value passed as second parameter to `condition` the first time it is evaluated.
		 * @param pf determines whether a new cycle should be performed based on the number of times it has already been evaluated and either the result of the previous cycle or `da0` if no cycle has been done yet.
		 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
		 * @tparam S a supertype of `A`
		 * @tparam B the type of the result of this duty.
		 */
		inline def repeatedWhileUndefined[S >: A, B](s0: S, pf: PartialFunction[(Int, S), B], maxRecursionDepthPerExecutor: Int = 9): Duty[B] =
			repeatedWhileEmpty(s0, Function.untupled(Maybe.liftPartialFunction(pf)), maxRecursionDepthPerExecutor)

	}
	
	//// DUTY FACTORY METHODS 

	/** Creates a new [[Duty]] that, when executed, repeatedly constructs a duty and executes it while a condition returns [[Right]].
	 * ==Detailed behavior:==
	 * Gives a new [[Duty]] that, when executed, it will:
	 *  - Apply the function `condition` to `(completedCycles, a0)`, and if it returns:
	 *		- a `Left(b)`, completes with `b`.
	 *  	- a `Right(taskA)`, executes the `taskA` goes back to the first step, replacing `a0` with the result.
	 *
	 * $threadSafe
	 *
	 * @param a0 the initial iteration state.
	 * @param condition function that, based on the `completedExecutionsCounter` and the iteration's state `a`, determines if the loop should end or otherwise creates the [[Duty]] to execute in the next iteration.
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 * @tparam A the type of the state passed from an iteration to the next.
	 * @tparam B the type of the result of created [[Duty]]
	 */
	def Duty_whileRightRepeat[A, B](a0: A, condition: (Int, A) => Either[B, Duty[A]], maxRecursionDepthPerExecutor: Int = 9): Duty[B] =
		new Duty_WhileRightRepeat[A, B](a0, condition, maxRecursionDepthPerExecutor)

	/** Creates a new [[Duty]] that, when executed, repeatedly constructs and executes tasks until the `condition` is met.
	 * ===Detailed behavior:===
	 * Gives a new [[Duty]] that, when executed, it will:
	 * - Apply the function `condition` to `(n,a0)` where n is the number of cycles already done. Then executes the resulting `task` and if its result is:
	 *			- `Left(tryB)`, completes with `tryB`.
	 *			- `Right(a1)`, goes back to the first step replacing `a0` with `a1`.
	 *
	 * $threadSafe
	 *
	 * @param a0 the initial iteration state.
	 * @param condition function that, based on the `completedExecutionsCounter` and the iteration's state (which starts with `a0`), determines if the loop should end or otherwise creates the [[Duty]] to execute in the next iteration.
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 * @tparam A the type of the state passed from an iteration to the next.
	 * @tparam B the type of the result of created [[Duty]]
	 */
	inline def Duty_repeatUntilLeft[A, B](a0: A, condition: (Int, A) => Duty[Either[B, A]], maxRecursionDepthPerExecutor: Int = 9): Duty[B] =
		new Duty_RepeatUntilLeft(a0, condition, maxRecursionDepthPerExecutor)

	/** Creates a new [[Duty]] that, when executed, repeatedly constructs and executes tasks until it succeeds or `maxRetries` is reached.
	 * ===Detailed behavior:===
	 * When the returned [[Task]] is executed, it will:
	 * 		- Apply the function `taskBuilder` to the number of tries that were already done.
	 * 		- Then executes the returned task and if the result is:
	 *				- `Right(b)`, completes with `Success(b)`.
	 *				- `Left(a)`, compares the retries counter against `maxRetries` and if:
	 *					- `retriesCounter >= maxRetries`, completes with `Left(a)`
	 *					- `retriesCounter < maxRetries`, increments the `retriesCounter` (which starts at zero) and goes back to the first step.
	 *
	 * $threadSafe
	 *
	 * @param maxRetries the maximum number of retries. Note that N retries is equivalent to N+1 attempts. So, a value of zero retries is one attempt.
	 * @param taskBuilder function to construct tasks, taking the retry count as input.
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 */
	inline def Duty_retryUntilRight[A, B](maxRetries: Int, taskBuilder: Int => Duty[Either[A, B]], maxRecursionDepthPerExecutor: Int = 9): Duty[Either[A, B]] =
		new Duty_RetryUntilRight[A, B](maxRetries, taskBuilder, maxRecursionDepthPerExecutor)

	/** Returns a new [[Duty]] that, when executed:
	 * 	- creates and executes a control task and, depending on its result, either:
	 *		- completes.
	 *		- or creates and executes an interleaved task and then goes back to the first step.
	 * WARNING: the execution of the returned duty will never end if the control duty always returns [[Right]].
	 *
	 * $threadSafe
	 *
	 * @param a0 2nd argument passed to `controlTaskBuilder` in the first cycle.
	 * @param b0 3rd argument passed to `controlTaskBuilder` in the first cycle.
	 * @param controlTaskBuilder the function that builds the control duty. It takes three parameters:
	 * - the number of already executed interleaved duties.
	 * - the result of the control duty in the previous cycle or `a0` in the first cycle.
	 * - the result of the interleaved duty in the previous cycle or `b0` in the first cycle.
	 * @param interleavedDutyBuilder the function that builds the interleaved duties. It takes two parameters:
	 *		- the number of already executed interleaved duties.
	 *		- the result of the control duty in the current cycle.
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 * */

	def Duty_repeatInterleavedUntilLeftDuty[A, B, R](a0: A, b0: B, controlTaskBuilder: (Int, A, B) => Duty[Either[R, A]], interleavedDutyBuilder: (Int, A) => Duty[B], maxRecursionDepthPerExecutor: Int = 9): Duty[R] = {

		Duty_repeatUntilLeft[(A, B), R](
			(a0, b0),
			(completedCycles, ab) =>
				controlTaskBuilder(completedCycles, ab._1, ab._2).flatMap {
					case Left(r) =>
						Duty_ready(Left(r))

					case Right(a) =>
						interleavedDutyBuilder(completedCycles, a).map(b => Right((a, b)))
				},
			maxRecursionDepthPerExecutor
		)
	}

	//// DUTY IMPLEMENTATION CLASSES

	/**
	 * A [[Duty]] that executes the received duty until applying the received function yields [[Maybe.some]].
	 * ===Detailed description===
	 * A [[Duty]] that, when executed, it will:
	 *		- execute the `dutyA` producing the result `a`
	 *		- apply `condition` to `(completedCycles, a)`. If the evaluation finishes with:
	 *			- `some(b)`, completes with `b`
	 *			- `empty`, goes back to the first step.
	 *
	 * @param dutyA the duty to be repeated.
	 * @param condition function that decides if the loop continues or not based on:
	 *		- the number of already completed cycles,
	 *		- and the result of the last execution of the `dutyA`.
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 * The loop ends when this function returns a [[Maybe.some]]. Its content will be the final result of this task.
	 */
	final class Duty_RepeatUntilSome[+A, +B](dutyA: Duty[A], condition: (Int, A) => Maybe[B], maxRecursionDepthPerExecutor: Int) extends AbstractDuty[B] {
		override def engage(onComplete: B => Unit): Unit = {
			def loop(completedCycles: Int, recursionDepth: Int): Unit = {
				dutyA.engagePortal { a =>
					condition(completedCycles, a).fold {
						if (recursionDepth < maxRecursionDepthPerExecutor) {
							loop(completedCycles + 1, recursionDepth + 1)
						} else {
							execute(loop(completedCycles + 1, 0))
						}
					}(onComplete)
				}
			}

			loop(0, 0)
		}

		override def toString: String = deriveToString[Duty_RepeatUntilSome[A, B]](this)
	}

	/**
	 * Duty that, when executed, repeatedly executes a duty while a condition returns [[Maybe.empty]].
	 * ===Detailed behavior:===
	 * When this [[Duty]] is executed, it will:
	 *  - Apply the `condition` function to `(n, a0)` where `n` is the number of already completed evaluations.
	 *  - If the evaluation returns:
	 *  	- `some(b)`, completes with `b`.
	 *  	- `empty`, executes the `dutyA` and repeats the condition.
	 *
	 * @param dutyA the task to be repeated
	 * @param a0 the value passed as the second parameter to `condition` the first time it is evaluated.
	 * @param condition determines if a new cycle should be performed based on the number of already completed cycles and the last result of `dutyA` or `a0` if no cycle has been completed.
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 */
	final class Duty_RepeatWhileEmpty[+A, +B](dutyA: Duty[A], a0: A, condition: (Int, A) => Maybe[B], maxRecursionDepthPerExecutor: Int) extends AbstractDuty[B] {
		override def engage(onComplete: B => Unit): Unit = {
			def loop(completedCycles: Int, lastDutyResult: A, recursionDepth: Int): Unit = {
				condition(completedCycles, lastDutyResult).fold {
					dutyA.engagePortal { newA =>
						if recursionDepth < maxRecursionDepthPerExecutor then loop(completedCycles + 1, newA, recursionDepth + 1)
						else execute(loop(completedCycles + 1, newA, 0))
					}
				}(onComplete)
			}

			loop(0, a0, 0)
		}

		override def toString: String = deriveToString[Duty_RepeatWhileEmpty[A, B]](this)
	}

	/**
	 * Duty that, when executed, repeatedly constructs and executes duties as long as the `condition` is met.
	 * ===Detailed behavior:===
	 * When this [[Duty]] is executed, it will:
	 *  - Apply the function `checkAndBuild` to `(n, a0)` where `n` is the number of completed cycles.
	 *  	- If it returns a `Left(b)`, completes with `b`.
	 *  	- If it returns `Right(dutyA)`, executes `dutyA` and repeats the cycle replacing `a0` with the result.
	 *
	 * @param a0 the initial value used in the first call to `checkAndBuild`.
	 * @param checkAndBuild function that takes completed cycles count and last duty result, returning an `Either[B, Duty[A]]`.
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 */
	final class Duty_WhileRightRepeat[+A, +B](a0: A, checkAndBuild: (Int, A) => Either[B, Duty[A]], maxRecursionDepthPerExecutor: Int) extends AbstractDuty[B] {
		override def engage(onComplete: B => Unit): Unit = {
			def loop(completedCycles: Int, lastDutyResult: A, recursionDepth: Int): Unit = {
				checkAndBuild(completedCycles, lastDutyResult) match {
					case Left(b) => onComplete(b)
					case Right(dutyA) =>
						dutyA.engagePortal { newA =>
							if recursionDepth < maxRecursionDepthPerExecutor then loop(completedCycles + 1, newA, recursionDepth + 1)
							else execute(loop(completedCycles + 1, newA, 0))
						}
				}
			}

			loop(0, a0, 0)
		}

		override def toString: String = deriveToString[Duty_WhileRightRepeat[A, B]](this)
	}

	/**
	 * Duty that, when executed, repeatedly constructs and executes duties until the result is [[Left]] or a failure occurs.
	 * ===Detailed behavior:===
	 * When this [[Duty]] is executed, it will:
	 *  - Apply the function `buildAndCheck` to `(n, a0)` where `n` ìs the number of completed cycles. Then executes the built duty and, if the result is:
	 *  	- `Left(b)`, completes with `b`.
	 *  	- `Right(a1)`, repeats the cycle replacing `a0` with `a1`.
	 *
	 * @param a0 the initial value used in the first call to `buildAndCheck`.
	 * @param buildAndCheck function that takes completed cycles count and last duty result, and returns a new duty that yields an `Either[B, A]`.
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 */
	final class Duty_RepeatUntilLeft[+A, +B](a0: A, buildAndCheck: (Int, A) => Duty[Either[B, A]], maxRecursionDepthPerExecutor: Int) extends AbstractDuty[B] {
		override def engage(onComplete: B => Unit): Unit = {
			def loop(executionsCounter: Int, lastDutyResult: A, recursionDepth: Int): Unit = {
				val duty = buildAndCheck(executionsCounter, lastDutyResult)
				duty.engagePortal {
					case Left(b) => onComplete(b)
					case Right(a) =>
						if recursionDepth < maxRecursionDepthPerExecutor then loop(executionsCounter + 1, a, recursionDepth + 1)
						else execute(loop(executionsCounter + 1, a, 0))
				}
			}

			loop(0, a0, 0)
		}

		override def toString: String = deriveToString[Duty_RepeatUntilLeft[A, B]](this)
	}

	/** Task that, when executed, repeatedly constructs and executes tasks until the result is [[Right]] or the `maxRetries` is reached.
	 * ===Detailed behavior:===
	 * When it is executed, it will:
	 *  - Apply the function `taskBuilder` to the number of tries that were already done. If the evaluation completes:
	 *  	- Abruptly, completes with the cause.
	 *  		- Normally, executes the returned task and if the result is:
	 *  			- `Failure(cause)`, completes with `Failure(cause)`.
	 *  			- `Success(Right(b))`, completes with `Success(b)`.
	 *  			- `Success(Left(a))`, compares the retries counter against `maxRetries` and if:
	 *  				- `retriesCounter >= maxRetries`, completes with `Left(a)`
	 *  				- `retriesCounter < maxRetries`, increments the `retriesCounter` (which starts at zero) and goes back to the first step.
	 */
	final class Duty_RetryUntilRight[+A, +B](maxRetries: Int, taskBuilder: Int => Duty[Either[A, B]], maxRecursionDepthPerExecutor: Int) extends AbstractDuty[Either[A, B]] {
		override def engage(onComplete: Either[A, B] => Unit): Unit = {
			/**
			 * @param attemptsAlreadyMade the number attempts already made.
			 * @param recursionDepth the number of recursions that may have been performed in the current executor in the worst case scenario where all calls are synchronous. */
			def loop(attemptsAlreadyMade: Int, recursionDepth: Int): Unit = {
				val task: Duty[Either[A, B]] = taskBuilder(attemptsAlreadyMade)

				task.engagePortal {
					case rb@(_: Right[A, B]) =>
						onComplete(rb)
					case la@Left(a) =>
						if (attemptsAlreadyMade >= maxRetries) {
							onComplete(la)
						} else if (recursionDepth < maxRecursionDepthPerExecutor) {
							loop(attemptsAlreadyMade + 1, recursionDepth + 1)
						} else {
							execute(loop(attemptsAlreadyMade + 1, 0))
						}
				}
			}

			loop(0, 0)
		}

		override def toString: String = deriveToString[Duty_RetryUntilRight[A, B]](this)
	}

	//// TASK INSTANCE OPERATIONS ////
	
	extension [A](thisTask: Task[A]) {

		/**
		 * Repeats this task until applying the received function yields [[Maybe.some]].
		 * ===Detailed description===
		 * Creates a [[Task]] that, when executed, it will:
		 *		- execute this task producing the result `tryA`
		 *		- apply `condition` to `(completedCycles, tryA)`. If the evaluation finishes:
		 * 			- abruptly, completes with the cause.
		 * 			- normally with `some(tryB)`, completes with `tryB`
		 * 			- normally with `empty`, goes back to the first step.
		 *
		 * $threadSafe
		 *
		 * @param condition function that decides if the loop continues or not based on:
		 * 	- the number of already completed cycles,
		 * 	- and the result of the last execution of the `taskA`.
		 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
		 * The loop ends when this function returns a [[Maybe.some]]. Its content will be the final result.
		 *
		 * $isExecutedByDoSiThEx $unhandledErrorsArePropagatedToTaskResult
		 * @return a new [[Task]] that, when executed, repeatedly executes this task and applies the `condition` to the task's result until the function's result is [[Maybe.some]]. The result of this task is the contents of said [[Maybe]] unless any execution of the `taskA` or `condition` terminates abruptly in which case this task result is the cause.
		 * */
		inline def reiteratedHardyUntilSome[B](condition: (Int, Try[A]) => Maybe[Try[B]], maxRecursionDepthPerExecutor: Int = 9): Task[B] =
			new Task_ReiterateHardyUntilSome(thisTask, condition, maxRecursionDepthPerExecutor)

		/**
		 * Creates a new [[Task]] that is executed repeatedly until either it fails or applying a condition to: its result and the number of already completed cycles, yields [[Maybe.some]].
		 * ===Detailed description===
		 * Creates a [[Task]] that, when executed, it will:
		 *		- execute this task and, if its results is:
		 * 			- `Failure(cause)`, completes with the same failure.
		 * 			- `Success(a)`, applies the `condition` to `(completedCycles, a)`. If the evaluation finishes:
		 * 				- abruptly, completes with the cause.
		 * 				- normally with `some(tryB)`, completes with `tryB`
		 * 				- normally with `empty`, goes back to the first step.
		 *
		 * $threadSafe
		 *
		 * @param condition function that decides if the loop continues or not based on:
		 * 	- the number of already completed cycles,
		 * 	- and the result of the last execution of the `taskA`.
		 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
		 * The loop ends when this function returns a [[Maybe.some]]. Its content will be the final result.
		 *
		 * $isExecutedByDoSiThEx $unhandledErrorsArePropagatedToTaskResult
		 * @return a new [[Task]] that, when executed, repeatedly executes this task and applies the `condition` to the task's result until the function's result is [[Maybe.some]]. The result of this task is the contents of said [[Maybe]] unless any execution of the `taskA` or `condition` terminates abruptly in which case this task result is the cause.
		 * */
		inline def reiteratedUntilSome[B](condition: (Int, A) => Maybe[Try[B]], maxRecursionDepthPerExecutor: Int = 9): Task[B] =
			reiteratedHardyUntilSome((completedCycles, tryA) =>
				tryA match {
					case Success(a) => condition(completedCycles, a)
					case f: Failure[A] => Maybe.some(f.castTo[B])
				},
				maxRecursionDepthPerExecutor
			)


		/** Like [[repeatedHardlyUntilSome]] but the condition is a [[PartialFunction]] instead of a function that returns [[Maybe]]. */
		inline def reiteratedHardyUntilDefined[B](pf: PartialFunction[(Int, Try[A]), Try[B]], maxRecursionDepthPerExecutor: Int = 9): Task[B] =
			reiteratedHardyUntilSome(Function.untupled(Maybe.liftPartialFunction(pf)), maxRecursionDepthPerExecutor)

		/**
		 * Repeats this [[Task]] while the given function returns [[Maybe.empty]].
		 * ===Detailed behavior:===
		 * Returns a [[Task]] that, when executed, it will:
		 *  - Apply the `condition` function to `(n, ts0)` where `n` is the number of already completed evaluations of it (starts with zero).
		 *  - If the evaluation finishes:
		 *  	- Abruptly, completes with the cause.
		 *  	- Normally returning `some(b)`, completes with `b`.
		 *  	- Normally returning `empty`, executes the `taskA` and goes back to the first step replacing `ts0` with the result.
		 *
		 * $threadSafe
		 *
		 * @param ts0 the value passed as second parameter to `condition` the first time it is evaluated.
		 * @param condition determines whether a new cycle should be performed based on the number of times it has already been evaluated and either the result of the previous cycle or `ta0` if no cycle has been done yet. $isExecutedByDoSiThEx $unhandledErrorsArePropagatedToTaskResult
		 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
		 * @tparam S a supertype of `A`
		 * @tparam B the type of the result of this task.
		 */
		inline def reiteratedWhileEmpty[S >: A, B](ts0: Try[S], condition: (Int, Try[S]) => Maybe[B], maxRecursionDepthPerExecutor: Int = 9): Task[B] =
			new Task_ReiterateHardyWhileEmpty[S, B](thisTask, ts0, condition, maxRecursionDepthPerExecutor)

		/**
		 * Returns a task that, when executed, repeatedly executes this [[Task]] while a [[PartialFunction]] is undefined.
		 * ===Detailed behavior:===
		 * Returns a [[Task]] that, when executed, it will:
		 *  - Check if the partial function is defined in `(n, ts0)` where `n` is the number of already completed evaluations of it (starts with zero). If it:
		 *		- fails, completes with the cause.
		 *  	- is undefined, executes the `taskA` and goes back to the first step replacing `ts0` with the result.
		 *  	- is defined, evaluates it and if it finishes:
		 *			- abruptly, completes with the cause.
		 *			- normally, completes with the result.
		 *
		 * $threadSafe
		 *
		 * @param ts0 the value passed as second parameter to `condition` the first time it is evaluated.
		 * @param pf determines whether a new cycle should be performed based on the number of times it has already been evaluated and either the result of the previous cycle or `ta0` if no cycle has been done yet. $isExecutedByDoSiThEx $unhandledErrorsArePropagatedToTaskResult
		 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
		 * @tparam S a supertype of `A`
		 * @tparam B the type of the result of this task.
		 */
		inline def reiteratedWhileUndefined[S >: A, B](ts0: Try[S], pf: PartialFunction[(Int, Try[S]), B], maxRecursionDepthPerExecutor: Int = 9): Task[B] = {
			reiteratedWhileEmpty(ts0, Function.untupled(Maybe.liftPartialFunction(pf)), maxRecursionDepthPerExecutor)
		}
	}
	
	
	//// TASK FACTORY METHODS ////


	/** Creates a new [[Task]] that, when executed, repeatedly constructs a task and executes it while a condition returns [[Right]].
	 * ==Detailed behavior:==
	 * Gives a new [[Task]] that, when executed, it will:
	 *  - Apply the function `condition` to `(completedCycles, tryA0)`, and if it finishes:
	 *  	- Abruptly, completes with the cause.
	 *		- Normally, returning a `Left(tryB)`, completes with `tryB`.
	 *  	- Normally, returning a `Right(taskA)`, executes the `taskA` and goes back to the first step, replacing `tryA0` with the result.
	 *
	 * $threadSafe
	 *
	 * @param tryA0 the initial iteration state.
	 * @param condition function that, based on the `completedExecutionsCounter` and the iteration's state `tryA`, determines if the loop should end or otherwise creates the [[Task]] to execute in the next iteration.
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 * @tparam A the type of the state passed from an iteration to the next.
	 * @tparam B the type of the result of created [[Task]]
	 */
	inline final def Task_whileRightReiterateHardy[A, B](tryA0: Try[A], condition: (Int, Try[A]) => Either[Try[B], Task[A]], maxRecursionDepthPerExecutor: Int = 9): Task[B] =
		new Task_WhileRightReiterateHardy[A, B](tryA0, condition, maxRecursionDepthPerExecutor);

	/** Creates a new [[Task]] that, when executed, repeatedly constructs a task and executes it as long as the `condition` is met.
	 * ==Detailed behavior:==
	 * Gives a new [[Task]] that, when executed, it will:
	 *  - Apply the function `condition` to `(completedCycles, a0)`, and if it finishes:
	 *  	- Abruptly, completes with the cause.
	 *		- Normally, returning a `Left(tryB)`, completes with `tryB`.
	 *  	- Normally, returning a `Right(taskA)`, executes the `taskA`, and if it terminates with:
	 *  		- Failure(cause), completes with the cause.
	 *  		- Success(a1), goes back to the first step, replacing `a0` with `a1`.
	 *
	 * $threadSafe
	 *
	 * @param a0 the initial iteration state.
	 * @param condition function that, based on the `completedExecutionsCounter` and the iteration's state `a`, determines if the loop should end or otherwise creates the [[Task]] to execute in the next iteration.
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 * @tparam A the type of the state passed from an iteration to the next.
	 * @tparam B the type of the result of created [[Task]]
	 */
	final def Task_whileRightReiterate[A, B](a0: A, condition: (Int, A) => Either[Try[B], Task[A]], maxRecursionDepthPerExecutor: Int = 9): Task[B] = {
		Task_whileRightReiterateHardy[A, B](
			Success(a0),
			(completedCycles, tryA) =>
				tryA match {
					case Success(a) => condition(completedCycles, a)
					case Failure(cause) => Left(Failure(cause))
				},
			maxRecursionDepthPerExecutor
		)
	}

	@deprecated("lo hice como ejercicio")
	private def Task_reiterateUntilLeft2[A, B](tryA0: Try[A], condition: (Int, Try[A]) => Task[Either[Try[B], A]], maxRecursionDepthPerExecutor: Int = 9): Task[B] =
		Task_whileRightReiterateHardy[Either[Try[B], A], B](
			tryA0.map(Right(_)),
			(completedExecutionsCounter, previousState) =>
				previousState match {
					case Success(Right(x)) => Right(condition(completedExecutionsCounter, Success(x)))
					case Success(Left(x)) => Left(x)
					case Failure(cause) => Right(condition(completedExecutionsCounter, Failure(cause)))
				},
			maxRecursionDepthPerExecutor
		)

	/** Creates a new [[Task]] that, when executed, repeatedly constructs and executes tasks as long as the `condition` is met.
	 * ===Detailed behavior:===
	 * Gives a new [[Task]] that, when executed, it will:
	 * - Try to apply the function `condition` to `(n,a0)` where n is the number of cycles already done. If the evaluation completes:
	 *		- Abruptly, completes with the cause.
	 *		- Normally, returning a `task`, executes the `task` and if its result is:
	 *			- `Failure(cause)`, completes with that `Failure(cause)`.
	 *			- `Success(Left(tryB))`, completes with `tryB`.
	 *			- `Success(Right(a1))`, goes back to the first step replacing `a0` with `a1`.
	 *
	 * $threadSafe
	 * @param a0 the initial iteration state.
	 * @param condition function that, based on the `completedExecutionsCounter` and the iteration's state (which starts with `a0`), determines if the loop should end or otherwise creates the [[Duty]] to execute in the next iteration.
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 * @tparam A the type of the state passed from an iteration to the next.
	 * @tparam B the type of the result of created [[Duty]]
	 * */
	inline final def Task_reiterateUntilLeft[A, B](a0: A, condition: (Int, A) => Task[Either[Try[B], A]], maxRecursionDepthPerExecutor: Int = 9): Task[B] =
		new Task_ReiterateUntilLeft(a0, condition, maxRecursionDepthPerExecutor)


	/** Creates a new [[Task]] that, when executed, repeatedly constructs and executes tasks until it succeed or the `maxRetries` is reached.
	 * ===Detailed behavior:===
	 * When the returned [[Task]] is executed, it will:
	 *		- Try to apply the function `taskBuilder` to the number of tries that were already done. If the evaluation completes:
	 *  		- Abruptly, completes with the cause.
	 *  		- Normally, executes the returned task and if the result is:
	 *				- `Failure(cause)`, completes with `Failure(cause)`.
	 *				- `Success(Right(b))`, completes with `Success(b)`.
	 *				- `Success(Left(a))`, compares the retries counter against `maxRetries` and if:
	 *					- `retriesCounter >= maxRetries`, completes with `Left(a)`
	 *					- `retriesCounter < maxRetries`, increments the `retriesCounter` (which starts at zero) and goes back to the first step.
	 *
	 * $threadSafe
	 * @param maxRetries the maximum number of retries. Note that N retries is equivalent to N+1 attempts. So, a value of zero retries is one attempt.
	 * @param taskBuilder function to construct tasks, taking the retry count as input.
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 * */
	inline final def Task_attemptUntilRight[A, B](maxRetries: Int, taskBuilder: Int => Task[Either[A, B]], maxRecursionDepthPerExecutor: Int = 9): Task[Either[A, B]] =
		new Task_AttemptUntilRight[A, B](maxRetries, taskBuilder, maxRecursionDepthPerExecutor)

	@deprecated("No se usa. Lo hice como ejercicio")
	private def Task_attemptUntilRight2[A, B](maxRetries: Int, taskBuilder: Int => Task[Either[A, B]], maxRecursionDepthPerExecutor: Int = 9): Task[Either[A, B]] =
		Task_reiterateUntilLeft[Null, Either[A, B]](
			null,
			(triesCounter, _) =>
				taskBuilder(triesCounter).map {
					case rb@Right(b) => Left(Success(rb))
					case Left(a) => Right(null)
				},
			maxRecursionDepthPerExecutor
		)


	/** Returns a new [[Task]] that, when executed:
	 * 	- creates and executes a control task and, depending on its result, either:
	 *		- completes.
	 *		- or creates and executes an interleaved task and then goes back to the first step.
	 * WARNING: the execution of the returned task will never end if the control task always returns [[Right]].
	 *
	 * $threadSafe
	 *
	 * @param a0 2nd argument passed to `controlTaskBuilder` in the first cycle.
	 * @param b0 3rd argument passed to `controlTaskBuilder` in the first cycle.
	 * @param controlTaskBuilder the function that builds the control tasks. It takes three parameters:
	 * - the number of already executed interleaved tasks.
	 * - the result of the control task in the previous cycle or `a0` in the first cycle.
	 * - the result of the interleaved task in the previous cycle or `b0` in the first cycle.
	 * @param interleavedTaskBuilder the function that builds the interleaved tasks. It takes two parameters:
	 * - the number of already executed interleaved tasks.
	 *  - the result of the control task in the current cycle.
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 * */
	final def Task_reiterateInterleavedUntilLeft[A, B, R](a0: A, b0: B, controlTaskBuilder: (Int, A, B) => Task[Either[Try[R], A]], interleavedTaskBuilder: (Int, A) => Task[B], maxRecursionDepthPerExecutor: Int = 9): Task[R] = {
		Task_reiterateUntilLeft[(A, B), R](
			(a0, b0),
			(completedCycles, ab) =>
				controlTaskBuilder(completedCycles, ab._1, ab._2).flatMap {
					case Left(tryR) =>
						Task_successful(Left(tryR))

					case Right(a) =>
						interleavedTaskBuilder(completedCycles, a).map(b => Right((a, b)))
				},
			maxRecursionDepthPerExecutor
		)
	}

	//// TASK IMPLEMENTATION CLASSES ////

	/**
	 * A [[Task]] that executes the received task until applying the received function yields [[Maybe.some]].
	 * ===Detailed description===
	 * A [[Task]] that, when executed, it will:
	 *		- execute the `taskA` producing the result `tryA`
	 *		- apply `condition` to `(completedCycles, tryA)`. If the evaluation finishes:
	 *			- abruptly, completes with the cause.
	 *			- normally with `some(tryB)`, completes with `tryB`
	 *			- normally with `empty`, goes back to the first step.
	 *
	 * $onCompleteExecutedByDoSiThEx
	 *
	 * @param taskA the task to be repeated.
	 * @param condition function that decides if the loop continues or not based on:
	 *		- the number of already completed cycles,
	 *		- and the result of the last execution of the `taskA`.
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 * The loop ends when this function returns a [[Maybe.some]]. Its content will be the final result of this task.
	 *
	 * $isExecutedByDoSiThEx $unhandledErrorsArePropagatedToTaskResult
	 * */
	final class Task_ReiterateHardyUntilSome[+A, +B](taskA: Task[A], condition: (Int, Try[A]) => Maybe[Try[B]], maxRecursionDepthPerExecutor: Int) extends AbstractTask[B] {

		override def engage(onComplete: Try[B] => Unit): Unit = {
			/**
			 * @param completedCycles number of already completed cycles.
			 * @param recursionDepth the number of recursions that may have been performed in the current executor in the worst case more synchronous scenario. */
			def loop(completedCycles: Int, recursionDepth: Int): Unit = {
				taskA.engagePortal { tryA =>
					val conditionResult: Maybe[Try[B]] =
						try condition(completedCycles, tryA)
						catch {
							case NonFatal(cause) => Maybe.some(Failure(cause))
						}
					conditionResult.fold {
						if (recursionDepth < maxRecursionDepthPerExecutor) {
							loop(completedCycles + 1, recursionDepth + 1)
						} else {
							execute(loop(completedCycles + 1, 0))
						}
					}(onComplete)
				}
			}

			loop(0, 0)
		}

		override def toString: String = deriveToString[Task_ReiterateHardyUntilSome[A, B]](this)
	}

	/**
	 * Task that, when executed, repeatedly executes a task while a condition return [[Maybe.empty]].
	 * ===Detailed behavior:===
	 * When this [[Task]] is executed, it will:
	 *  - Try to apply the `condition` function to `(n, ta0)` where `n` is the number of already completed evaluations of it.
	 *  - If the evaluation completes:
	 *  	- Abruptly, completes with the cause.
	 *  	- Normally, returning `some(b)`, completes with `b`.
	 *  	- Normally, returning `empty`, executes the `taskA` and goes back to the first step replacing `ta0` with the result.
	 *
	 * $onCompleteExecutedByDoSiThEx
	 *
	 * @param taskA the task to be repeated
	 * @param ta0 the value passed as second parameter `condition` the first time it is evaluated.
	 * @param condition determines whether a new cycle should be performed based on the number of times it has already been evaluated and either the result of the previous cycle or `ta0` if no cycle has been done yet. $isExecutedByDoSiThEx $unhandledErrorsArePropagatedToTaskResult
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 * @tparam A the type of the result of the repeated task `taskA`.
	 * @tparam B the type of the result of this task.
	 */
	final class Task_ReiterateHardyWhileEmpty[+A, +B](taskA: Task[A], ta0: Try[A], condition: (Int, Try[A]) => Maybe[B], maxRecursionDepthPerExecutor: Int) extends AbstractTask[B] {
		override def engage(onComplete: Try[B] => Unit): Unit = {
			/**
			 * @param completedCycles number of already completed cycles.
			 * @param lastTaskResult the result of the last task execution.
			 * @param recursionDepth the number of recursions that may have been performed in the current executor in the worst case more synchronous scenario. */
			def loop(completedCycles: Int, lastTaskResult: Try[A], recursionDepth: Int): Unit = {
				val conditionResult: Maybe[Try[B]] =
					try {
						condition(completedCycles, lastTaskResult)
							.fold(Maybe.empty)(b => Maybe.some(Success(b)))
					}
					catch {
						case NonFatal(cause) => Maybe.some(Failure(cause))
					}

				conditionResult.fold {
					taskA.engagePortal { newTryA =>
						if recursionDepth < maxRecursionDepthPerExecutor then loop(completedCycles + 1, newTryA, recursionDepth + 1)
						else execute(loop(completedCycles + 1, newTryA, 0))
					}
				}(onComplete)
			}

			loop(0, ta0, 0)
		}

		override def toString: String = deriveToString[Task_ReiterateHardyWhileEmpty[A, B]](this)
	}


	/** Task that, when executed, repeatedly constructs and executes tasks as long as the `condition` is met.
	 * ===Detailed behavior:===
	 * When this [[Task]] is executed, it will:
	 *  - Try to apply the function `checkAndBuild` to `(n, tryA0)` where `n` is the number of already completed cycles, and if it completes:
	 *  	- Abruptly, completes with the cause.
	 *		- Normally, returning a `Left(tryB)`, completes with `tryB`.
	 *  	- Normally, returning a `Right(taskA)`, executes the `taskA` and goes back to the first step replacing `tryA0` with the result.
	 *
	 * @param tryA0 the initial value wrapped in a `Try`, used in the first call to `checkAndBuild`.
	 * @param checkAndBuild a function that takes the number of already completed cycles and the last task result wrapped in a `Try`, returning an `Either[Try[B], Task[A]]` indicating the next action to perform.	$isExecutedByDoSiThEx $unhandledErrorsArePropagatedToTaskResult *
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 */
	final class Task_WhileRightReiterateHardy[+A, +B](tryA0: Try[A], checkAndBuild: (Int, Try[A]) => Either[Try[B], Task[A]], maxRecursionDepthPerExecutor: Int) extends AbstractTask[B] {
		override def engage(onComplete: Try[B] => Unit): Unit = {
			/**
			 * @param completedCycles number of already completed cycles, which consist of a task creation and its execution.
			 * @param lastTaskResult the result of the last task execution.
			 * @param recursionDepth the number of recursions that may have been performed in the current executor in the worst case scenario where all calls are synchronous. */
			def loop(completedCycles: Int, lastTaskResult: Try[A], recursionDepth: Int): Unit = {
				val tryBOrTaskA =
					try checkAndBuild(completedCycles, lastTaskResult)
					catch {
						case NonFatal(cause) => Left(Failure(cause));
					}
				tryBOrTaskA match {
					case Left(tryB) =>
						onComplete(tryB);
					case Right(taskA) =>
						taskA.engagePortal { newTryA =>
							if (recursionDepth < maxRecursionDepthPerExecutor) loop(completedCycles + 1, newTryA, recursionDepth + 1)
							else execute(loop(completedCycles + 1, newTryA, 0));
						}
				}
			}

			loop(0, tryA0, 0)
		}

		override def toString: String = deriveToString[Task_WhileRightReiterateHardy[A, B]](this)
	}

	/** Task that, when executed, repeatedly constructs and executes tasks until the result is [[Left]] or failed.
	 * ===Detailed behavior:===
	 * When this [[Task]] is executed, it will:
	 *  - Try to apply the function `buildAndCheck` to `(n, a0)` where n is the number of already completed cycles. If the evaluation completes:
	 *  	- Abruptly, completes with the cause.
	 *  		- Normally, returning a `task`, executes the `task` and if its result is:
	 *  			- `Failure(cause)`, completes with that `Failure(cause)`.
	 *  			- `Success(Left(tryB))`, completes with `tryB`.
	 *  			- `Success(Right(a1))`, goes back to the first step replacing `a0` with `a1`.
	 *
	 * $onCompleteExecutedByDoSiThEx
	 *
	 * @param a0 the initial value used in the first call to `buildAndCheck`.
	 * @param buildAndCheck a function that takes the number of already completed cycles and the last task result, returning a new task that yields an `Either[Try[B], A]` indicating the next action to perform. $isExecutedByDoSiThEx $unhandledErrorsArePropagatedToTaskResult
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 */
	final class Task_ReiterateUntilLeft[+A, +B](a0: A, buildAndCheck: (Int, A) => Task[Either[Try[B], A]], maxRecursionDepthPerExecutor: Int) extends AbstractTask[B] {
		override def engage(onComplete: Try[B] => Unit): Unit = {
			/**
			 * @param executionsCounter number of already completed cycles, which consist of a task creation and its execution.
			 * @param lastTaskResult the result of the last task execution.
			 * @param recursionDepth the number of recursions that may have been performed in the current executor in the worst case scenario where all calls are synchronous. */
			def loop(executionsCounter: Int, lastTaskResult: A, recursionDepth: Int): Unit = {
				val task: Task[Either[Try[B], A]] =
					try buildAndCheck(executionsCounter, lastTaskResult)
					catch {
						case NonFatal(e) => Task_successful(Left(Failure(e)))
					}
				task.engagePortal {
					case Success(Right(a)) =>
						if (recursionDepth < maxRecursionDepthPerExecutor) loop(executionsCounter + 1, a, recursionDepth + 1)
						else execute(loop(executionsCounter + 1, a, 0));
					case Success(Left(tryB)) =>
						onComplete(tryB)
					case Failure(e) =>
						onComplete(Failure(e))
				}
			}

			loop(0, a0, 0)
		}

		override def toString: String = deriveToString[Task_ReiterateUntilLeft[A, B]](this)
	}

	/** Task that, when executed, repeatedly constructs and executes tasks until the result is [[Right]] or the `maxRetries` is reached.
	 * ===Detailed behavior:===
	 * When it is executed, it will:
	 *  - Try to apply the function `taskBuilder` to the number of tries that were already done. If the evaluation completes:
	 *  	- Abruptly, completes with the cause.
	 *  		- Normally, executes the returned task and if the result is:
	 *  			- `Failure(cause)`, completes with `Failure(cause)`.
	 *  			- `Success(Right(b))`, completes with `Success(b)`.
	 *  			- `Success(Left(a))`, compares the retries counter against `maxRetries` and if:
	 *  				- `retriesCounter >= maxRetries`, completes with `Left(a)`
	 *  				- `retriesCounter < maxRetries`, increments the `retriesCounter` (which starts at zero) and goes back to the first step.
	 */
	final class Task_AttemptUntilRight[+A, +B](maxRetries: Int, taskBuilder: Int => Task[Either[A, B]], maxRecursionDepthPerExecutor: Int) extends AbstractTask[Either[A, B]] {
		override def engage(onComplete: Try[Either[A, B]] => Unit): Unit = {
			/**
			 * @param attemptsAlreadyMade the number attempts already made.
			 * @param recursionDepth the number of recursions that may have been performed in the current executor in the worst case scenario where all calls are synchronous. */
			def loop(attemptsAlreadyMade: Int, recursionDepth: Int): Unit = {
				val task: Task[Either[A, B]] =
					try taskBuilder(attemptsAlreadyMade)
					catch {
						case NonFatal(cause) => Task_failed(cause)
					}
				task.engagePortal {
					case success@Success(aOrB) =>
						aOrB match {
							case _: Right[A, B] =>
								onComplete(success)
							case Left(a) =>
								if (attemptsAlreadyMade >= maxRetries) {
									onComplete(success)
								} else if (recursionDepth < maxRecursionDepthPerExecutor) {
									loop(attemptsAlreadyMade + 1, recursionDepth + 1)
								} else {
									execute(loop(attemptsAlreadyMade + 1, 0))
								}
						}
					case failure: Failure[Either[A, B]] =>
						onComplete(failure);
				}
			}

			loop(0, 0)
		}

		override def toString: String = deriveToString[Task_AttemptUntilRight[A, B]](this)
	}
}
