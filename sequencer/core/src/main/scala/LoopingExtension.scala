package readren.sequencer

import readren.common.{Maybe, castTo, deriveToString}

import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

trait LoopingExtension { thisDoer: Doer =>

	//// TASK INSTANCE OPERATIONS
	extension [A](thisTask: Task[A]) {
		/**
		 * Repeats this task until applying the received function yields [[Maybe.some]].
		 * ===Detailed description===
		 * Creates a [[Task]] that, when executed, it will:
		 * - execute this task producing the result `a`
		 * - apply `condition` to `(completedCycles, a)`. If the evaluation finishes with:
		 *      - `some(b)`, completes with `b`
		 *      - `empty`, goes back to the first step.
		 *
		 * $threadSafe
		 *
		 * @param condition function that decides if the loop continues or not based on:
		 *  - the number of already completed cycles,
		 *  - and the result of the last execution of the `taskA`.
		 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
		 * The loop ends when this function returns a [[Maybe.some]]. Its content will be the final result.
		 * @return a new [[Task]] that, when executed, repeatedly executes this task and applies the `condition` to the task's result until the function's result is [[Maybe.some]]. The result of this task is the contents of said [[Maybe]].
		 */
		inline def repeatedUntilSome[B](condition: (Int, A) => Maybe[B], maxRecursionDepthPerExecutor: Int = 9): Task[B] =
			new Task_RepeatUntilSome(thisTask, condition, maxRecursionDepthPerExecutor)

		/**
		 * Like [[repeatedUntilSome]] but the condition is a [[PartialFunction]] instead of a function that returns [[Maybe]].
		 */
		inline def repeatedUntilDefined[B](pf: PartialFunction[(Int, A), B], maxRecursionDepthPerExecutor: Int = 9): Task[B] =
			repeatedUntilSome(Function.untupled(Maybe.liftPartialFunction(pf)), maxRecursionDepthPerExecutor)

		/**
		 * Repeats this [[Task]] while the given function returns [[Maybe.empty]].
		 * ===Detailed behavior===
		 * Returns a [[Task]] that, when executed, it will:
		 *  - Apply the `condition` function to `(n, s0)` where `n` is the number of already completed evaluations of it (starts with zero).
		 *  - If the evaluation returns:
		 *      - `some(b)`, completes with `b`.
		 *      - `empty`, executes the `taskA` and goes back to the first step replacing `s0` with the result.
		 *
		 * $threadSafe
		 *
		 * @param s0 the value passed as second parameter to `condition` the first time it is evaluated.
		 * @param condition determines whether a new cycle should be performed based on the number of times it has already been evaluated and either the result of the previous cycle or `da0` if no cycle has been done yet.
		 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
		 * @tparam S a supertype of `A`
		 * @tparam B the type of the result of this task.
		 */
		inline def repeatedWhileEmpty[S >: A, B](s0: S, condition: (Int, S) => Maybe[B], maxRecursionDepthPerExecutor: Int = 9): Task[B] =
			new Task_RepeatWhileEmpty[S, B](thisTask, s0, condition, maxRecursionDepthPerExecutor)

		/**
		 * Returns a task that, when executed, repeatedly executes this [[Task]] while a [[PartialFunction]] is undefined.
		 * ===Detailed behavior===
		 * Returns a [[Task]] that, when executed, it will:
		 *  - Check if the partial function is defined in `(n, s0)` where `n` is the number of already completed evaluations of it (starts with zero).
		 *  - If it is undefined, executes the `taskA` and goes back to the first step replacing `s0` with the result.
		 *  - If it is defined, evaluates it and completes with the result.
		 *
		 * $threadSafe
		 *
		 * @param s0 the value passed as second parameter to `condition` the first time it is evaluated.
		 * @param pf determines whether a new cycle should be performed based on the number of times it has already been evaluated and either the result of the previous cycle or `da0` if no cycle has been done yet.
		 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
		 * @tparam S a supertype of `A`
		 * @tparam B the type of the result of this task.
		 */
		inline def repeatedWhileUndefined[S >: A, B](s0: S, pf: PartialFunction[(Int, S), B], maxRecursionDepthPerExecutor: Int = 9): Task[B] =
			repeatedWhileEmpty(s0, Function.untupled(Maybe.liftPartialFunction(pf)), maxRecursionDepthPerExecutor)

	}

	//// TASK FACTORY METHODS

	/** Creates a new [[Task]] that, when executed, repeatedly constructs a task and executes it while a condition returns [[Right]].
	 * ==Detailed behavior:==
	 * Gives a new [[Task]] that, when executed, it will:
	 *  - Apply the function `condition` to `(completedCycles, a0)`, and if it returns:
	 *		- a `Left(b)`, completes with `b`.
	 *  	- a `Right(taskA)`, executes the `taskA` goes back to the first step, replacing `a0` with the result.
	 *
	 * $threadSafe
	 *
	 * @param a0 the initial iteration state.
	 * @param condition function that, based on the `completedExecutionsCounter` and the iteration's state `a`, determines if the loop should end or otherwise creates the [[Task]] to execute in the next iteration.
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 * @tparam A the type of the state passed from an iteration to the next.
	 * @tparam B the type of the result of created [[Task]]
	 */
	def Task_whileRightRepeat[A, B](a0: A, condition: (Int, A) => Either[B, Task[A]], maxRecursionDepthPerExecutor: Int = 9): Task[B] =
		new Task_WhileRightRepeat[A, B](a0, condition, maxRecursionDepthPerExecutor)

	/** Creates a new [[Task]] that, when executed, repeatedly constructs and executes tasks until the `condition` is met.
	 * ===Detailed behavior:===
	 * Gives a new [[Task]] that, when executed, it will:
	 * - Apply the function `condition` to `(n,a0)` where n is the number of cycles already done. Then executes the resulting `task` and if its result is:
	 *			- `Left(tryB)`, completes with `tryB`.
	 *			- `Right(a1)`, goes back to the first step replacing `a0` with `a1`.
	 *
	 * $threadSafe
	 *
	 * @param a0 the initial iteration state.
	 * @param condition function that, based on the `completedExecutionsCounter` and the iteration's state (which starts with `a0`), determines if the loop should end or otherwise creates the [[Task]] to execute in the next iteration.
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 * @tparam A the type of the state passed from an iteration to the next.
	 * @tparam B the type of the result of created [[Task]]
	 */
	inline def Task_repeatUntilLeft[A, B](a0: A, condition: (Int, A) => Task[Either[B, A]], maxRecursionDepthPerExecutor: Int = 9): Task[B] =
		new Task_RepeatUntilLeft(a0, condition, maxRecursionDepthPerExecutor)

	/** Creates a new [[Task]] that, when executed, repeatedly constructs and executes tasks until it succeeds or `maxRetries` is reached.
	 * ===Detailed behavior:===
	 * When the returned [[Venture]] is executed, it will:
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
	inline def Task_retryUntilRight[A, B](maxRetries: Int, taskBuilder: Int => Task[Either[A, B]], maxRecursionDepthPerExecutor: Int = 9): Task[Either[A, B]] =
		new Task_RetryUntilRight[A, B](maxRetries, taskBuilder, maxRecursionDepthPerExecutor)

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
	 * @param controlTaskBuilder the function that builds the control task. It takes three parameters:
	 * - the number of already executed interleaved duties.
	 * - the result of the control task in the previous cycle or `a0` in the first cycle.
	 * - the result of the interleaved task in the previous cycle or `b0` in the first cycle.
	 * @param interleavedTaskBuilder the function that builds the interleaved duties. It takes two parameters:
	 *		- the number of already executed interleaved duties.
	 *		- the result of the control task in the current cycle.
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 * */

	def Task_repeatInterleavedUntilLeftTask[A, B, R](a0: A, b0: B, controlTaskBuilder: (Int, A, B) => Task[Either[R, A]], interleavedTaskBuilder: (Int, A) => Task[B], maxRecursionDepthPerExecutor: Int = 9): Task[R] = {

		Task_repeatUntilLeft[(A, B), R](
			(a0, b0),
			(completedCycles, ab) =>
				controlTaskBuilder(completedCycles, ab._1, ab._2).flatMap {
					case Left(r) =>
						Task_ready(Left(r))

					case Right(a) =>
						interleavedTaskBuilder(completedCycles, a).map(b => Right((a, b)))
				},
			maxRecursionDepthPerExecutor
		)
	}

	//// TASK IMPLEMENTATION CLASSES

	/**
	 * A [[Task]] that executes the received task until applying the received function yields [[Maybe.some]].
	 * ===Detailed description===
	 * A [[Task]] that, when executed, it will:
	 *		- execute the `taskA` producing the result `a`
	 *		- apply `condition` to `(completedCycles, a)`. If the evaluation finishes with:
	 *			- `some(b)`, completes with `b`
	 *			- `empty`, goes back to the first step.
	 *
	 * @param taskA the task to be repeated.
	 * @param condition function that decides if the loop continues or not based on:
	 *		- the number of already completed cycles,
	 *		- and the result of the last execution of the `taskA`.
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 * The loop ends when this function returns a [[Maybe.some]]. Its content will be the final result of this task.
	 */
	final class Task_RepeatUntilSome[+A, +B](taskA: Task[A], condition: (Int, A) => Maybe[B], maxRecursionDepthPerExecutor: Int) extends AbstractTask[B] {
		override def subscribe(onComplete: B => Unit): Subscription = {
			// Returns a Subscription that supports cancellation by setting a volatile flag
			// and unsubscribing from the active task cycle.
			new Subscription {
				private var active = true
				private var currentSub: Subscription = Subscription_empty

				def loop(completedCycles: Int, recursionDepth: Int): Unit = {
					if active then {
						currentSub = taskA.subscribe { a =>
							if active then {
								condition(completedCycles, a).fold {
									if recursionDepth < maxRecursionDepthPerExecutor then {
										loop(completedCycles + 1, recursionDepth + 1)
									} else {
										run(loop(completedCycles + 1, 0))
									}
								}(onComplete)
							}
						}
					}
				}

				override def unsubscribe(): Unit = {
					checkWithin()
					active = false
					currentSub.unsubscribe()
				}

				loop(0, 0)
			}
		}

		override def toString: String = deriveToString[Task_RepeatUntilSome[A, B]](this)
	}

	/**
	 * Task that, when executed, repeatedly executes a task while a condition returns [[Maybe.empty]].
	 * ===Detailed behavior:===
	 * When this [[Task]] is executed, it will:
	 *  - Apply the `condition` function to `(n, a0)` where `n` is the number of already completed evaluations.
	 *  - If the evaluation returns:
	 *  	- `some(b)`, completes with `b`.
	 *  	- `empty`, executes the `taskA` and repeats the condition.
	 *
	 * @param taskA the task to be repeated
	 * @param a0 the value passed as the second parameter to `condition` the first time it is evaluated.
	 * @param condition determines if a new cycle should be performed based on the number of already completed cycles and the last result of `taskA` or `a0` if no cycle has been completed.
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 */
	final class Task_RepeatWhileEmpty[+A, +B](taskA: Task[A], a0: A, condition: (Int, A) => Maybe[B], maxRecursionDepthPerExecutor: Int) extends AbstractTask[B] {
		override def subscribe(onComplete: B => Unit): Subscription = {
			// Returns a Subscription that supports cancellation by setting a volatile flag
			// and unsubscribing from the active task cycle.
			new Subscription {
				private var active = true
				private var currentSub: Subscription = Subscription_empty

				def loop(completedCycles: Int, lastTaskResult: A, recursionDepth: Int): Unit = {
					if active then {
						condition(completedCycles, lastTaskResult).fold {
							currentSub = taskA.subscribe { newA =>
								if active then {
									if recursionDepth < maxRecursionDepthPerExecutor then loop(completedCycles + 1, newA, recursionDepth + 1)
									else run(loop(completedCycles + 1, newA, 0))
								}
							}
						}(onComplete)
					}
				}

				override def unsubscribe(): Unit = {
					checkWithin()
					active = false
					currentSub.unsubscribe()
				}

				loop(0, a0, 0)
			}
		}

		override def toString: String = deriveToString[Task_RepeatWhileEmpty[A, B]](this)
	}

	/**
	 * Task that, when executed, repeatedly constructs and executes duties as long as the `condition` is met.
	 * ===Detailed behavior:===
	 * When this [[Task]] is executed, it will:
	 *  - Apply the function `checkAndBuild` to `(n, a0)` where `n` is the number of completed cycles.
	 *  	- If it returns a `Left(b)`, completes with `b`.
	 *  	- If it returns `Right(taskA)`, executes `taskA` and repeats the cycle replacing `a0` with the result.
	 *
	 * @param a0 the initial value used in the first call to `checkAndBuild`.
	 * @param checkAndBuild function that takes completed cycles count and last task result, returning an `Either[B, Task[A]]`.
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 */
	final class Task_WhileRightRepeat[+A, +B](a0: A, checkAndBuild: (Int, A) => Either[B, Task[A]], maxRecursionDepthPerExecutor: Int) extends AbstractTask[B] {
		override def subscribe(onComplete: B => Unit): Subscription = {
			// Returns a Subscription that supports cancellation by setting a volatile flag
			// and unsubscribing from the active task cycle.
			new Subscription {
				private var active = true
				private var currentSub: Subscription = Subscription_empty

				def loop(completedCycles: Int, lastTaskResult: A, recursionDepth: Int): Unit = {
					if active then {
						checkAndBuild(completedCycles, lastTaskResult) match {
							case Left(b) => onComplete(b)
							case Right(taskA) =>
								currentSub = taskA.subscribe { newA =>
									if active then {
										if recursionDepth < maxRecursionDepthPerExecutor then loop(completedCycles + 1, newA, recursionDepth + 1)
										else run(loop(completedCycles + 1, newA, 0))
									}
								}
						}
					}
				}

				override def unsubscribe(): Unit = {
					checkWithin()
					active = false
					currentSub.unsubscribe()
				}

				loop(0, a0, 0)
			}
		}

		override def toString: String = deriveToString[Task_WhileRightRepeat[A, B]](this)
	}

	/**
	 * Task that, when executed, repeatedly constructs and executes duties until the result is [[Left]] or a failure occurs.
	 * ===Detailed behavior:===
	 * When this [[Task]] is executed, it will:
	 *  - Apply the function `buildAndCheck` to `(n, a0)` where `n` ìs the number of completed cycles. Then executes the built task and, if the result is:
	 *  	- `Left(b)`, completes with `b`.
	 *  	- `Right(a1)`, repeats the cycle replacing `a0` with `a1`.
	 *
	 * @param a0 the initial value used in the first call to `buildAndCheck`.
	 * @param buildAndCheck function that takes completed cycles count and last task result, and returns a new task that yields an `Either[B, A]`.
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 */
	final class Task_RepeatUntilLeft[+A, +B](a0: A, buildAndCheck: (Int, A) => Task[Either[B, A]], maxRecursionDepthPerExecutor: Int) extends AbstractTask[B] {
		override def subscribe(onComplete: B => Unit): Subscription = {
			// Returns a Subscription that supports cancellation by setting a volatile flag
			// and unsubscribing from the active task cycle.
			new Subscription {
				private var active = true
				private var currentSub: Subscription = Subscription_empty

				def loop(executionsCounter: Int, lastTaskResult: A, recursionDepth: Int): Unit = {
					if active then {
						val task = buildAndCheck(executionsCounter, lastTaskResult)
						currentSub = task.subscribe {
							case Left(b) =>
								if active then onComplete(b)
							case Right(a) =>
								if active then {
									if recursionDepth < maxRecursionDepthPerExecutor then loop(executionsCounter + 1, a, recursionDepth + 1)
									else run(loop(executionsCounter + 1, a, 0))
								}
						}
					}
				}

				override def unsubscribe(): Unit = {
					checkWithin()
					active = false
					currentSub.unsubscribe()
				}

				loop(0, a0, 0)
			}
		}

		override def toString: String = deriveToString[Task_RepeatUntilLeft[A, B]](this)
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
	final class Task_RetryUntilRight[+A, +B](maxRetries: Int, taskBuilder: Int => Task[Either[A, B]], maxRecursionDepthPerExecutor: Int) extends AbstractTask[Either[A, B]] {
		override def subscribe(onComplete: Either[A, B] => Unit): Subscription = {
			// Returns a Subscription that supports cancellation by setting a volatile flag
			// and unsubscribing from the active task cycle.
			new Subscription {
				private var active = true
				private var currentSub: Subscription = Subscription_empty

				def loop(attemptsAlreadyMade: Int, recursionDepth: Int): Unit = {
					if active then {
						val task: Task[Either[A, B]] = taskBuilder(attemptsAlreadyMade)
						currentSub = task.subscribe {
							case rb@(_: Right[A, B]) =>
								if active then onComplete(rb)
							case la@Left(a) =>
								if active then {
									if attemptsAlreadyMade >= maxRetries then {
										onComplete(la)
									} else if recursionDepth < maxRecursionDepthPerExecutor then {
										loop(attemptsAlreadyMade + 1, recursionDepth + 1)
									} else {
										run(loop(attemptsAlreadyMade + 1, 0))
									}
								}
						}
					}
				}

				override def unsubscribe(): Unit = {
					checkWithin()
					active = false
					currentSub.unsubscribe()
				}

				loop(0, 0)
			}
		}

		override def toString: String = deriveToString[Task_RetryUntilRight[A, B]](this)
	}

	//// TASK INSTANCE OPERATIONS ////

	extension [A](thisVenture: Venture[A]) {

		/**
		 * Repeats this task until applying the received function yields [[Maybe.some]].
		 * ===Detailed description===
		 * Creates a [[Venture]] that, when executed, it will:
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
		 * $isExecutedByDoSerEx $unhandledErrorsArePropagatedToTaskResult
		 * @return a new [[Venture]] that, when executed, repeatedly executes this task and applies the `condition` to the task's result until the function's result is [[Maybe.some]]. The result of this task is the contents of said [[Maybe]] unless any execution of the `taskA` or `condition` terminates abruptly in which case this task result is the cause.
		 * */
		inline def reiteratedHardyUntilSome[B](condition: (Int, Try[A]) => Maybe[Try[B]], maxRecursionDepthPerExecutor: Int = 9): Venture[B] =
			new Venture_ReiterateHardyUntilSome(thisVenture, condition, maxRecursionDepthPerExecutor)

		/**
		 * Creates a new [[Venture]] that is executed repeatedly until either it fails or applying a condition to: its result and the number of already completed cycles, yields [[Maybe.some]].
		 * ===Detailed description===
		 * Creates a [[Venture]] that, when executed, it will:
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
		 * $isExecutedByDoSerEx $unhandledErrorsArePropagatedToTaskResult
		 * @return a new [[Venture]] that, when executed, repeatedly executes this task and applies the `condition` to the task's result until the function's result is [[Maybe.some]]. The result of this task is the contents of said [[Maybe]] unless any execution of the `taskA` or `condition` terminates abruptly in which case this task result is the cause.
		 * */
		inline def reiteratedUntilSome[B](condition: (Int, A) => Maybe[Try[B]], maxRecursionDepthPerExecutor: Int = 9): Venture[B] =
			reiteratedHardyUntilSome((completedCycles, tryA) =>
				tryA match {
					case Success(a) => condition(completedCycles, a)
					case f: Failure[A] => Maybe(f.castTo[B])
				},
				maxRecursionDepthPerExecutor
			)


		/** Like [[repeatedHardlyUntilSome]] but the condition is a [[PartialFunction]] instead of a function that returns [[Maybe]]. */
		inline def reiteratedHardyUntilDefined[B](pf: PartialFunction[(Int, Try[A]), Try[B]], maxRecursionDepthPerExecutor: Int = 9): Venture[B] =
			reiteratedHardyUntilSome(Function.untupled(Maybe.liftPartialFunction(pf)), maxRecursionDepthPerExecutor)

		/**
		 * Repeats this [[Venture]] while the given function returns [[Maybe.empty]].
		 * ===Detailed behavior:===
		 * Returns a [[Venture]] that, when executed, it will:
		 *  - Apply the `condition` function to `(n, ts0)` where `n` is the number of already completed evaluations of it (starts with zero).
		 *  - If the evaluation finishes:
		 *  	- Abruptly, completes with the cause.
		 *  	- Normally returning `some(b)`, completes with `b`.
		 *  	- Normally returning `empty`, executes the `taskA` and goes back to the first step replacing `ts0` with the result.
		 *
		 * $threadSafe
		 *
		 * @param ts0 the value passed as second parameter to `condition` the first time it is evaluated.
		 * @param condition determines whether a new cycle should be performed based on the number of times it has already been evaluated and either the result of the previous cycle or `ta0` if no cycle has been done yet. $isExecutedByDoSerEx $unhandledErrorsArePropagatedToTaskResult
		 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
		 * @tparam S a supertype of `A`
		 * @tparam B the type of the result of this task.
		 */
		inline def reiteratedWhileEmpty[S >: A, B](ts0: Try[S], condition: (Int, Try[S]) => Maybe[B], maxRecursionDepthPerExecutor: Int = 9): Venture[B] =
			new Venture_ReiterateHardyWhileEmpty[S, B](thisVenture, ts0, condition, maxRecursionDepthPerExecutor)

		/**
		 * Returns a task that, when executed, repeatedly executes this [[Venture]] while a [[PartialFunction]] is undefined.
		 * ===Detailed behavior:===
		 * Returns a [[Venture]] that, when executed, it will:
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
		 * @param pf determines whether a new cycle should be performed based on the number of times it has already been evaluated and either the result of the previous cycle or `ta0` if no cycle has been done yet. $isExecutedByDoSerEx $unhandledErrorsArePropagatedToTaskResult
		 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
		 * @tparam S a supertype of `A`
		 * @tparam B the type of the result of this task.
		 */
		inline def reiteratedWhileUndefined[S >: A, B](ts0: Try[S], pf: PartialFunction[(Int, Try[S]), B], maxRecursionDepthPerExecutor: Int = 9): Venture[B] = {
			reiteratedWhileEmpty(ts0, Function.untupled(Maybe.liftPartialFunction(pf)), maxRecursionDepthPerExecutor)
		}
	}
	
	
	//// TASK FACTORY METHODS ////


	/** Creates a new [[Venture]] that, when executed, repeatedly constructs a task and executes it while a condition returns [[Right]].
	 * ==Detailed behavior:==
	 * Gives a new [[Venture]] that, when executed, it will:
	 *  - Apply the function `condition` to `(completedCycles, tryA0)`, and if it finishes:
	 *  	- Abruptly, completes with the cause.
	 *		- Normally, returning a `Left(tryB)`, completes with `tryB`.
	 *  	- Normally, returning a `Right(taskA)`, executes the `taskA` and goes back to the first step, replacing `tryA0` with the result.
	 *
	 * $threadSafe
	 *
	 * @param tryA0 the initial iteration state.
	 * @param condition function that, based on the `completedExecutionsCounter` and the iteration's state `tryA`, determines if the loop should end or otherwise creates the [[Venture]] to execute in the next iteration.
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 * @tparam A the type of the state passed from an iteration to the next.
	 * @tparam B the type of the result of created [[Venture]]
	 */
	inline final def Venture_whileRightReiterateHardy[A, B](tryA0: Try[A], condition: (Int, Try[A]) => Either[Try[B], Venture[A]], maxRecursionDepthPerExecutor: Int = 9): Venture[B] =
		new Venture_WhileRightReiterateHardy[A, B](tryA0, condition, maxRecursionDepthPerExecutor);

	/** Creates a new [[Venture]] that, when executed, repeatedly constructs a task and executes it as long as the `condition` is met.
	 * ==Detailed behavior:==
	 * Gives a new [[Venture]] that, when executed, it will:
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
	 * @param condition function that, based on the `completedExecutionsCounter` and the iteration's state `a`, determines if the loop should end or otherwise creates the [[Venture]] to execute in the next iteration.
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 * @tparam A the type of the state passed from an iteration to the next.
	 * @tparam B the type of the result of created [[Venture]]
	 */
	final def Venture_whileRightReiterate[A, B](a0: A, condition: (Int, A) => Either[Try[B], Venture[A]], maxRecursionDepthPerExecutor: Int = 9): Venture[B] = {
		Venture_whileRightReiterateHardy[A, B](
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
	private def Venture_reiterateUntilLeft2[A, B](tryA0: Try[A], condition: (Int, Try[A]) => Venture[Either[Try[B], A]], maxRecursionDepthPerExecutor: Int = 9): Venture[B] =
		Venture_whileRightReiterateHardy[Either[Try[B], A], B](
			tryA0.map(Right(_)),
			(completedExecutionsCounter, previousState) =>
				previousState match {
					case Success(Right(x)) => Right(condition(completedExecutionsCounter, Success(x)))
					case Success(Left(x)) => Left(x)
					case Failure(cause) => Right(condition(completedExecutionsCounter, Failure(cause)))
				},
			maxRecursionDepthPerExecutor
		)

	/** Creates a new [[Venture]] that, when executed, repeatedly constructs and executes tasks as long as the `condition` is met.
	 * ===Detailed behavior:===
	 * Gives a new [[Venture]] that, when executed, it will:
	 * - Try to apply the function `condition` to `(n,a0)` where n is the number of cycles already done. If the evaluation completes:
	 *		- Abruptly, completes with the cause.
	 *		- Normally, returning a `task`, executes the `task` and if its result is:
	 *			- `Failure(cause)`, completes with that `Failure(cause)`.
	 *			- `Success(Left(tryB))`, completes with `tryB`.
	 *			- `Success(Right(a1))`, goes back to the first step replacing `a0` with `a1`.
	 *
	 * $threadSafe
	 * @param a0 the initial iteration state.
	 * @param condition function that, based on the `completedExecutionsCounter` and the iteration's state (which starts with `a0`), determines if the loop should end or otherwise creates the [[Task]] to execute in the next iteration.
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 * @tparam A the type of the state passed from an iteration to the next.
	 * @tparam B the type of the result of created [[Task]]
	 * */
	inline final def Venture_reiterateUntilLeft[A, B](a0: A, condition: (Int, A) => Venture[Either[Try[B], A]], maxRecursionDepthPerExecutor: Int = 9): Venture[B] =
		new Venture_ReiterateUntilLeft(a0, condition, maxRecursionDepthPerExecutor)


	/** Creates a new [[Venture]] that, when executed, repeatedly constructs and executes tasks until it succeed or the `maxRetries` is reached.
	 * ===Detailed behavior:===
	 * When the returned [[Venture]] is executed, it will:
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
	 * @param ventureBuilder function to construct tasks, taking the retry count as input.
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 * */
	inline final def Venture_attemptUntilRight[A, B](maxRetries: Int, ventureBuilder: Int => Venture[Either[A, B]], maxRecursionDepthPerExecutor: Int = 9): Venture[Either[A, B]] =
		new Venture_AttemptUntilRight[A, B](maxRetries, ventureBuilder, maxRecursionDepthPerExecutor)

	@deprecated("No se usa. Lo hice como ejercicio")
	private def Venture_attemptUntilRight2[A, B](maxRetries: Int, ventureBuilder: Int => Venture[Either[A, B]], maxRecursionDepthPerExecutor: Int = 9): Venture[Either[A, B]] =
		Venture_reiterateUntilLeft[Null, Either[A, B]](
			null,
			(triesCounter, _) =>
				ventureBuilder(triesCounter).map {
					case rb@Right(b) => Left(Success(rb))
					case Left(a) => Right(null)
				},
			maxRecursionDepthPerExecutor
		)


	/** Returns a new [[Venture]] that, when executed:
	 * 	- creates and executes a control task and, depending on its result, either:
	 *		- completes.
	 *		- or creates and executes an interleaved task and then goes back to the first step.
	 * WARNING: the execution of the returned task will never end if the control task always returns [[Right]].
	 *
	 * $threadSafe
	 *
	 * @param a0 2nd argument passed to `controlTaskBuilder` in the first cycle.
	 * @param b0 3rd argument passed to `controlTaskBuilder` in the first cycle.
	 * @param controlVentureBuilder the function that builds the control tasks. It takes three parameters:
	 * - the number of already executed interleaved tasks.
	 * - the result of the control task in the previous cycle or `a0` in the first cycle.
	 * - the result of the interleaved task in the previous cycle or `b0` in the first cycle.
	 * @param interleavedVentureBuilder the function that builds the interleaved tasks. It takes two parameters:
	 * - the number of already executed interleaved tasks.
	 *  - the result of the control task in the current cycle.
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 * */
	final def Venture_reiterateInterleavedUntilLeft[A, B, R](a0: A, b0: B, controlVentureBuilder: (Int, A, B) => Venture[Either[Try[R], A]], interleavedVentureBuilder: (Int, A) => Venture[B], maxRecursionDepthPerExecutor: Int = 9): Venture[R] = {
		Venture_reiterateUntilLeft[(A, B), R](
			(a0, b0),
			(completedCycles, ab) =>
				controlVentureBuilder(completedCycles, ab._1, ab._2).flatMap {
					case Left(tryR) =>
						Venture_successful(Left(tryR))

					case Right(a) =>
						interleavedVentureBuilder(completedCycles, a).map(b => Right((a, b)))
				},
			maxRecursionDepthPerExecutor
		)
	}

	//// TASK IMPLEMENTATION CLASSES ////

	/**
	 * A [[Venture]] that executes the received task until applying the received function yields [[Maybe.some]].
	 * ===Detailed description===
	 * A [[Venture]] that, when executed, it will:
	 *		- execute the `taskA` producing the result `tryA`
	 *		- apply `condition` to `(completedCycles, tryA)`. If the evaluation finishes:
	 *			- abruptly, completes with the cause.
	 *			- normally with `some(tryB)`, completes with `tryB`
	 *			- normally with `empty`, goes back to the first step.
	 *
	 * $onCompleteExecutedByDoSerEx
	 *
	 * @param ventureA the task to be repeated.
	 * @param condition function that decides if the loop continues or not based on:
	 *		- the number of already completed cycles,
	 *		- and the result of the last execution of the `taskA`.
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 * The loop ends when this function returns a [[Maybe.some]]. Its content will be the final result of this task.
	 *
	 * $isExecutedByDoSerEx $unhandledErrorsArePropagatedToTaskResult
	 * */
	final class Venture_ReiterateHardyUntilSome[+A, +B](ventureA: Venture[A], condition: (Int, Try[A]) => Maybe[Try[B]], maxRecursionDepthPerExecutor: Int) extends AbstractVenture[B] {

		override def subscribe(onComplete: Try[B] => Unit): Subscription = {
			// Returns a Subscription that supports cancellation by setting a flag
			// and unsubscribing from the active venture cycle.
			new Subscription {
				private var active = true
				private var currentSub: Subscription = Subscription_empty

				def loop(completedCycles: Int, recursionDepth: Int): Unit = {
					if active then {
						currentSub = ventureA.subscribe { tryA =>
							if active then {
								val conditionResult: Maybe[Try[B]] =
									try condition(completedCycles, tryA)
									catch {
										case NonFatal(cause) => Maybe(Failure(cause))
									}
								conditionResult.fold {
									if recursionDepth < maxRecursionDepthPerExecutor then {
										loop(completedCycles + 1, recursionDepth + 1)
									} else {
										run(loop(completedCycles + 1, 0))
									}
								}(onComplete)
							}
						}
					}
				}

				override def unsubscribe(): Unit = {
					checkWithin()
					active = false
					currentSub.unsubscribe()
				}

				loop(0, 0)
			}
		}

		override def toString: String = deriveToString[Venture_ReiterateHardyUntilSome[A, B]](this)
	}

	/**
	 * Task that, when executed, repeatedly executes a task while a condition return [[Maybe.empty]].
	 * ===Detailed behavior:===
	 * When this [[Venture]] is executed, it will:
	 *  - Try to apply the `condition` function to `(n, ta0)` where `n` is the number of already completed evaluations of it.
	 *  - If the evaluation completes:
	 *  	- Abruptly, completes with the cause.
	 *  	- Normally, returning `some(b)`, completes with `b`.
	 *  	- Normally, returning `empty`, executes the `taskA` and goes back to the first step replacing `ta0` with the result.
	 *
	 * $onCompleteExecutedByDoSerEx
	 *
	 * @param ventureA the task to be repeated
	 * @param ta0 the value passed as second parameter `condition` the first time it is evaluated.
	 * @param condition determines whether a new cycle should be performed based on the number of times it has already been evaluated and either the result of the previous cycle or `ta0` if no cycle has been done yet. $isExecutedByDoSerEx $unhandledErrorsArePropagatedToTaskResult
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 * @tparam A the type of the result of the repeated task `taskA`.
	 * @tparam B the type of the result of this task.
	 */
	final class Venture_ReiterateHardyWhileEmpty[+A, +B](ventureA: Venture[A], ta0: Try[A], condition: (Int, Try[A]) => Maybe[B], maxRecursionDepthPerExecutor: Int) extends AbstractVenture[B] {
		override def subscribe(onComplete: Try[B] => Unit): Subscription = {
			// Returns a Subscription that supports cancellation by setting a flag
			// and unsubscribing from the active venture cycle.
			new Subscription {
				private var active = true
				private var currentSub: Subscription = Subscription_empty

				def loop(completedCycles: Int, lastVentureResult: Try[A], recursionDepth: Int): Unit = {
					if active then {
						val conditionResult: Maybe[Try[B]] =
							try {
								condition(completedCycles, lastVentureResult)
									.fold(Maybe.empty)(b => Maybe(Success(b)))
							}
							catch {
								case NonFatal(cause) => Maybe(Failure(cause))
							}

						conditionResult.fold {
							currentSub = ventureA.subscribe { newTryA =>
								if active then {
									if recursionDepth < maxRecursionDepthPerExecutor then loop(completedCycles + 1, newTryA, recursionDepth + 1)
									else run(loop(completedCycles + 1, newTryA, 0))
								}
							}
						}(onComplete)
					}
				}

				override def unsubscribe(): Unit = {
					checkWithin()
					active = false
					currentSub.unsubscribe()
				}

				loop(0, ta0, 0)
			}
		}

		override def toString: String = deriveToString[Venture_ReiterateHardyWhileEmpty[A, B]](this)
	}


	/** Task that, when executed, repeatedly constructs and executes tasks as long as the `condition` is met.
	 * ===Detailed behavior:===
	 * When this [[Venture]] is executed, it will:
	 *  - Try to apply the function `checkAndBuild` to `(n, tryA0)` where `n` is the number of already completed cycles, and if it completes:
	 *  	- Abruptly, completes with the cause.
	 *		- Normally, returning a `Left(tryB)`, completes with `tryB`.
	 *  	- Normally, returning a `Right(taskA)`, executes the `taskA` and goes back to the first step replacing `tryA0` with the result.
	 *
	 * @param tryA0 the initial value wrapped in a `Try`, used in the first call to `checkAndBuild`.
	 * @param checkAndBuild a function that takes the number of already completed cycles and the last task result wrapped in a `Try`, returning an `Either[Try[B], Task[A]]` indicating the next action to perform.	$isExecutedByDoSerEx $unhandledErrorsArePropagatedToTaskResult *
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 */
	final class Venture_WhileRightReiterateHardy[+A, +B](tryA0: Try[A], checkAndBuild: (Int, Try[A]) => Either[Try[B], Venture[A]], maxRecursionDepthPerExecutor: Int) extends AbstractVenture[B] {
		override def subscribe(onComplete: Try[B] => Unit): Subscription = {
			// Returns a Subscription that supports cancellation by setting a flag
			// and unsubscribing from the active venture cycle.
			new Subscription {
				private var active = true
				private var currentSub: Subscription = Subscription_empty

				def loop(completedCycles: Int, lastVentureResult: Try[A], recursionDepth: Int): Unit = {
					if active then {
						val tryBOrVentureA =
							try checkAndBuild(completedCycles, lastVentureResult)
							catch {
								case NonFatal(cause) => Left(Failure(cause));
							}
						tryBOrVentureA match {
							case Left(tryB) =>
								if active then onComplete(tryB);
							case Right(ventureA) =>
								currentSub = ventureA.subscribe { newTryA =>
									if active then {
										if recursionDepth < maxRecursionDepthPerExecutor then loop(completedCycles + 1, newTryA, recursionDepth + 1)
										else run(loop(completedCycles + 1, newTryA, 0));
									}
								}
						}
					}
				}

				override def unsubscribe(): Unit = {
					checkWithin()
					active = false
					currentSub.unsubscribe()
				}

				loop(0, tryA0, 0)
			}
		}

		override def toString: String = deriveToString[Venture_WhileRightReiterateHardy[A, B]](this)
	}

	/** Task that, when executed, repeatedly constructs and executes tasks until the result is [[Left]] or failed.
	 * ===Detailed behavior:===
	 * When this [[Venture]] is executed, it will:
	 *  - Try to apply the function `buildAndCheck` to `(n, a0)` where n is the number of already completed cycles. If the evaluation completes:
	 *  	- Abruptly, completes with the cause.
	 *  		- Normally, returning a `task`, executes the `task` and if its result is:
	 *  			- `Failure(cause)`, completes with that `Failure(cause)`.
	 *  			- `Success(Left(tryB))`, completes with `tryB`.
	 *  			- `Success(Right(a1))`, goes back to the first step replacing `a0` with `a1`.
	 *
	 * $onCompleteExecutedByDoSerEx
	 *
	 * @param a0 the initial value used in the first call to `buildAndCheck`.
	 * @param buildAndCheck a function that takes the number of already completed cycles and the last task result, returning a new task that yields an `Either[Try[B], A]` indicating the next action to perform. $isExecutedByDoSerEx $unhandledErrorsArePropagatedToTaskResult
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 */
	final class Venture_ReiterateUntilLeft[+A, +B](a0: A, buildAndCheck: (Int, A) => Venture[Either[Try[B], A]], maxRecursionDepthPerExecutor: Int) extends AbstractVenture[B] {
		override def subscribe(onComplete: Try[B] => Unit): Subscription = {
			// Returns a Subscription that supports cancellation by setting a flag
			// and unsubscribing from the active venture cycle.
			new Subscription {
				private var active = true
				private var currentSub: Subscription = Subscription_empty

				def loop(executionsCounter: Int, lastVentureResult: A, recursionDepth: Int): Unit = {
					if active then {
						val venture: Venture[Either[Try[B], A]] =
							try buildAndCheck(executionsCounter, lastVentureResult)
							catch {
								case NonFatal(e) => Venture_successful(Left(Failure(e)))
							}
						currentSub = venture.subscribe {
							case Success(Right(a)) =>
								if active then {
									if recursionDepth < maxRecursionDepthPerExecutor then loop(executionsCounter + 1, a, recursionDepth + 1)
									else run(loop(executionsCounter + 1, a, 0));
								}
							case Success(Left(tryB)) =>
								if active then onComplete(tryB)
							case Failure(e) =>
								if active then onComplete(Failure(e))
						}
					}
				}

				override def unsubscribe(): Unit = {
					checkWithin()
					active = false
					currentSub.unsubscribe()
				}

				loop(0, a0, 0)
			}
		}

		override def toString: String = deriveToString[Venture_ReiterateUntilLeft[A, B]](this)
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
	final class Venture_AttemptUntilRight[+A, +B](maxRetries: Int, ventureBuilder: Int => Venture[Either[A, B]], maxRecursionDepthPerExecutor: Int) extends AbstractVenture[Either[A, B]] {
		override def subscribe(onComplete: Try[Either[A, B]] => Unit): Subscription = {
			// Returns a Subscription that supports cancellation by setting a flag
			// and unsubscribing from the active venture cycle.
			new Subscription {
				private var active = true
				private var currentSub: Subscription = Subscription_empty

				def loop(attemptsAlreadyMade: Int, recursionDepth: Int): Unit = {
					if active then {
						val venture: Venture[Either[A, B]] =
							try ventureBuilder(attemptsAlreadyMade)
							catch {
								case NonFatal(cause) => Venture_failed(cause)
							}
						currentSub = venture.subscribe {
							case success@Success(aOrB) =>
								if active then {
									aOrB match {
										case _: Right[A, B] =>
											onComplete(success)
										case Left(a) =>
											if attemptsAlreadyMade >= maxRetries then {
												onComplete(success)
											} else if recursionDepth < maxRecursionDepthPerExecutor then {
												loop(attemptsAlreadyMade + 1, recursionDepth + 1)
											} else {
												run(loop(attemptsAlreadyMade + 1, 0))
											}
									}
								}
							case failure: Failure[Either[A, B]] =>
								if active then onComplete(failure);
						}
					}
				}

				override def unsubscribe(): Unit = {
					checkWithin()
					active = false
					currentSub.unsubscribe()
				}

				loop(0, 0)
			}
		}

		override def toString: String = deriveToString[Venture_AttemptUntilRight[A, B]](this)
	}
}
