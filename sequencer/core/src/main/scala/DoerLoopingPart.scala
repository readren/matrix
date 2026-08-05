package readren.sequencer

import readren.common.{Maybe, deriveToString}

import scala.util.control.NonFatal

trait DoerLoopingPart { thisDoer: Doer =>

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
		 * @param isGuarded determines whether non-fatal exceptions thrown by `condition` are propagated to the result (true) or are left unhandled (false).
		 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
		 * The loop ends when this function returns a [[Maybe.some]]. Its content will be the final result.
		 * @return a new [[Task]] that, when executed, repeatedly executes this task and applies the `condition` to the task's result until the function's result is [[Maybe.some]]. The result of this task is the contents of said [[Maybe]].
		 */
		inline def repeatedUntilSome[B](condition: (Int, A) => Maybe[B], isGuarded: Boolean = false, maxRecursionDepthPerExecutor: Int = 9): Task[B] =
			new Task_RepeatUntilSome(thisTask, condition, isGuarded, maxRecursionDepthPerExecutor)

		/**
		 * Like [[repeatedUntilSome]] but the condition is a [[PartialFunction]] instead of a function that returns [[Maybe]].
		 */
		inline def repeatedUntilDefined[B](pf: PartialFunction[(Int, A), B], isGuarded: Boolean = false, maxRecursionDepthPerExecutor: Int = 9): Task[B] =
			repeatedUntilSome(Function.untupled(Maybe.liftPartialFunction(pf)), isGuarded, maxRecursionDepthPerExecutor)

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
		 * @param isGuarded determines whether non-fatal exceptions thrown by `condition` are propagated to the result (true) or are left unhandled (false).
		 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
		 * @tparam S a supertype of `A`
		 * @tparam B the type of the result of this task.
		 */
		inline def repeatedWhileEmpty[S >: A, B](s0: S, condition: (Int, S) => Maybe[B], isGuarded: Boolean = false, maxRecursionDepthPerExecutor: Int = 9): Task[B] =
			new Task_RepeatWhileEmpty[S, B](thisTask, s0, condition, isGuarded, maxRecursionDepthPerExecutor)

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
		 * @param isGuarded determines whether non-fatal exceptions thrown by `pf` are propagated to the result (true) or are left unhandled (false).
		 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
		 * @tparam S a supertype of `A`
		 * @tparam B the type of the result of this task.
		 */
		inline def repeatedWhileUndefined[S >: A, B](s0: S, pf: PartialFunction[(Int, S), B], isGuarded: Boolean = false, maxRecursionDepthPerExecutor: Int = 9): Task[B] =
			repeatedWhileEmpty(s0, Function.untupled(Maybe.liftPartialFunction(pf)), isGuarded, maxRecursionDepthPerExecutor)

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
	 * @param isGuarded determines whether non-fatal exceptions thrown by `condition` are propagated to the result (true) or are left unhandled (false).
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 * @tparam A the type of the state passed from an iteration to the next.
	 * @tparam B the type of the result of created [[Task]]
	 */
	def Task_whileRightRepeat[A, B](a0: A, condition: (Int, A) => Either[B, Task[A]], isGuarded: Boolean = false, maxRecursionDepthPerExecutor: Int = 9): Task[B] =
		new Task_WhileRightRepeat[A, B](a0, condition, isGuarded, maxRecursionDepthPerExecutor)

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
	 * @param isGuarded determines whether non-fatal exceptions thrown by `condition` are propagated to the result (true) or are left unhandled (false).
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 * @tparam A the type of the state passed from an iteration to the next.
	 * @tparam B the type of the result of created [[Task]]
	 */
	inline def Task_repeatUntilLeft[A, B](a0: A, condition: (Int, A) => Task[Either[B, A]], isGuarded: Boolean = false, maxRecursionDepthPerExecutor: Int = 9): Task[B] =
		new Task_RepeatUntilLeft(a0, condition, isGuarded, maxRecursionDepthPerExecutor)

	/** Creates a new [[Task]] that, when executed, repeatedly constructs and executes tasks until it succeeds or `maxRetries` is reached.
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
	 * @param isGuarded determines whether non-fatal exceptions thrown by `taskBuilder` are propagated to the result (true) or are left unhandled (false).
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 */
	inline def Task_retryUntilRight[A, B](maxRetries: Int, taskBuilder: Int => Task[Either[A, B]], isGuarded: Boolean = false, maxRecursionDepthPerExecutor: Int = 9): Task[Either[A, B]] =
		new Task_RetryUntilRight[A, B](maxRetries, taskBuilder, isGuarded, maxRecursionDepthPerExecutor)

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
	def Task_repeatInterleavedUntilLeft[A, B, R](a0: A, b0: B, controlTaskBuilder: (Int, A, B) => Task[Either[R, A]], interleavedTaskBuilder: (Int, A) => Task[B], isGuarded: Boolean = false, maxRecursionDepthPerExecutor: Int = 9): Task[R] = {

		Task_repeatUntilLeft[(A, B), R](
			(a0, b0),
			(completedCycles, ab) =>
				controlTaskBuilder(completedCycles, ab._1, ab._2).flatMap {
					case Left(r) =>
						Task_ready(Left(r))

					case Right(a) =>
						val interleavedTask =
							if isGuarded then try interleavedTaskBuilder(completedCycles, a) catch {
								case NonFatal(e) => Task_fail(e)
							} else interleavedTaskBuilder(completedCycles, a)

						interleavedTask.map(b => Right((a, b)))
				},
			isGuarded,
			maxRecursionDepthPerExecutor
		)
	}

	//// TASK IMPLEMENTATION CLASSES

	/** $suppressSyntheticCompanionObject */
	private inline def Task_RepeatUntilSome(trap: Nothing): Any = trap

	final class Task_RepeatUntilSome[+A, +B](upChainTask: Task[A], condition: (Int, A) => Maybe[B], isGuarded: Boolean, maxRecursionDepthPerExecutor: Int) extends AbstractTask[B] {
		override def subscribeSync(downChainObserver: MonoObserver[B]): Subscription = {
			new Subscription {
				private var isActive = true
				private var maybeInnerSubscription: Maybe[Subscription] = Maybe.empty

				def loop(completedCycles: Int, recursionDepth: Int): Unit = {
					if isActive then {
						val innerSubscription = upChainTask.subscribeSync(new MonoObserver[A] {
							override def onSuccess(a: A): Unit = {
								if isActive then {
									maybeInnerSubscription = Maybe.empty
									val maybeB =
										if isGuarded then try condition(completedCycles, a) catch {
											case NonFatal(e) =>
												isActive = false
												downChainObserver.onError(e)
												Maybe.empty
										} else condition(completedCycles, a)

									if isActive then {
										maybeB.fold {
											if recursionDepth < maxRecursionDepthPerExecutor then {
												loop(completedCycles + 1, recursionDepth + 1)
											} else {
												run(loop(completedCycles + 1, 0))
											}
										} { b =>
											isActive = false
											downChainObserver.onSuccess(b)
										}
									}
								}
							}

							override def onError(ex: Throwable): Unit = {
								if isActive then {
									isActive = false
									maybeInnerSubscription = Maybe.empty
									downChainObserver.onError(ex)
								}
							}
						})
						if isActive then maybeInnerSubscription = Maybe(innerSubscription)
					}
				}

				override def unsubscribeSync(): Unit = {
					checkWithin()
					if isActive then {
						isActive = false
						val mis = maybeInnerSubscription
						maybeInnerSubscription = Maybe.empty
						mis.foreach(_.unsubscribeSync())
					}
				}

				loop(0, 0)
			}
		}

		override def toString: String = deriveToString[Task_RepeatUntilSome[A, B]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_RepeatWhileEmpty(trap: Nothing): Any = trap

	final class Task_RepeatWhileEmpty[+A, +B](upChainTask: Task[A], a0: A, condition: (Int, A) => Maybe[B], isGuarded: Boolean, maxRecursionDepthPerExecutor: Int) extends AbstractTask[B] {
		override def subscribeSync(downChainObserver: MonoObserver[B]): Subscription = {
			new Subscription {
				private var isActive = true
				private var maybeInnerSubscription: Maybe[Subscription] = Maybe.empty

				def loop(completedCycles: Int, lastTaskResult: A, recursionDepth: Int): Unit = {
					if isActive then {
						val maybeB =
							if isGuarded then try condition(completedCycles, lastTaskResult) catch {
								case NonFatal(e) =>
									isActive = false
									downChainObserver.onError(e)
									Maybe.empty
							} else condition(completedCycles, lastTaskResult)

						if isActive then {
							maybeB.fold {
								val innerSubscription = upChainTask.subscribeSync(new MonoObserver[A] {
									override def onSuccess(newA: A): Unit = {
										if isActive then {
											maybeInnerSubscription = Maybe.empty
											if recursionDepth < maxRecursionDepthPerExecutor then loop(completedCycles + 1, newA, recursionDepth + 1)
											else run(loop(completedCycles + 1, newA, 0))
										}
									}

									override def onError(ex: Throwable): Unit = {
										if isActive then {
											isActive = false
											maybeInnerSubscription = Maybe.empty
											downChainObserver.onError(ex)
										}
									}
								})
								if isActive then maybeInnerSubscription = Maybe(innerSubscription)
							}(downChainObserver.onSuccess)
						}
					}
				}

				override def unsubscribeSync(): Unit = {
					checkWithin()
					if isActive then {
						isActive = false
						val mis = maybeInnerSubscription
						maybeInnerSubscription = Maybe.empty
						mis.foreach(_.unsubscribeSync())
					}
				}

				loop(0, a0, 0)
			}
		}

		override def toString: String = deriveToString[Task_RepeatWhileEmpty[A, B]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_WhileRightRepeat(trap: Nothing): Any = trap

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
	final class Task_WhileRightRepeat[+A, +B](a0: A, checkAndBuild: (Int, A) => Either[B, Task[A]], isGuarded: Boolean, maxRecursionDepthPerExecutor: Int) extends AbstractTask[B] {
		override def subscribeSync(downChainObserver: MonoObserver[B]): Subscription = {
			new Subscription {
				private var isActive = true
				private var maybeInnerSubscription: Maybe[Subscription] = Maybe.empty

				def loop(completedCycles: Int, lastTaskResult: A, recursionDepth: Int): Unit = {
					if isActive then {
						val decision: Either[B, Task[A]] =
							if isGuarded then try checkAndBuild(completedCycles, lastTaskResult) catch {
								case NonFatal(e) =>
									isActive = false
									downChainObserver.onError(e)
									Right(Task_fail(e))
							} else checkAndBuild(completedCycles, lastTaskResult)

						if isActive then decision match {
							case Left(b) => downChainObserver.onSuccess(b)
							case Right(taskA) =>
								val innerSubscription = taskA.subscribeSync(new MonoObserver[A] {
									override def onSuccess(newA: A): Unit = {
										if isActive then {
											maybeInnerSubscription = Maybe.empty
											if recursionDepth < maxRecursionDepthPerExecutor then loop(completedCycles + 1, newA, recursionDepth + 1)
											else run(loop(completedCycles + 1, newA, 0))
										}
									}

									override def onError(ex: Throwable): Unit = {
										if isActive then {
											isActive = false
											maybeInnerSubscription = Maybe.empty
											downChainObserver.onError(ex)
										}
									}
								})
								if isActive then maybeInnerSubscription = Maybe(innerSubscription)
						}
					}
				}

				override def unsubscribeSync(): Unit = {
					checkWithin()
					if isActive then {
						isActive = false
						val mis = maybeInnerSubscription
						maybeInnerSubscription = Maybe.empty
						mis.foreach(_.unsubscribeSync())
					}
				}

				loop(0, a0, 0)
			}
		}

		override def toString: String = deriveToString[Task_WhileRightRepeat[A, B]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_RepeatUntilLeft(trap: Nothing): Any = trap

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
	final class Task_RepeatUntilLeft[+A, +B](a0: A, buildAndCheck: (Int, A) => Task[Either[B, A]], isGuarded: Boolean, maxRecursionDepthPerExecutor: Int) extends AbstractTask[B] {
		override def subscribeSync(downChainObserver: MonoObserver[B]): Subscription = {
			new Subscription {
				private var isActive = true
				private var maybeInnerSubscription: Maybe[Subscription] = Maybe.empty

				def loop(executionsCounter: Int, lastTaskResult: A, recursionDepth: Int): Unit = {
					if isActive then {
						val taskBorA =
							if isGuarded then {
								try buildAndCheck(executionsCounter, lastTaskResult) catch {
									case NonFatal(e) => Task_fail(e)
								}
							}
							else buildAndCheck(executionsCounter, lastTaskResult)
						val innerSubscription = taskBorA.subscribeSync(new MonoObserver[Either[B, A]] {
							override def onSuccess(res: Either[B, A]): Unit = {
								if isActive then {
									maybeInnerSubscription = Maybe.empty
									res match {
										case Left(b) => downChainObserver.onSuccess(b)
										case Right(a) =>
											if recursionDepth < maxRecursionDepthPerExecutor then loop(executionsCounter + 1, a, recursionDepth + 1)
											else run(loop(executionsCounter + 1, a, 0))
									}
								}
							}

							override def onError(ex: Throwable): Unit = {
								if isActive then {
									isActive = false
									maybeInnerSubscription = Maybe.empty
									downChainObserver.onError(ex)
								}
							}
						})
						if isActive then maybeInnerSubscription = Maybe(innerSubscription)
					}
				}

				override def unsubscribeSync(): Unit = {
					checkWithin()
					if isActive then {
						isActive = false
						val mis = maybeInnerSubscription
						maybeInnerSubscription = Maybe.empty
						mis.foreach(_.unsubscribeSync())
					}
				}

				loop(0, a0, 0)
			}
		}

		override def toString: String = deriveToString[Task_RepeatUntilLeft[A, B]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_RetryUntilRight(trap: Nothing): Any = trap

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
	final class Task_RetryUntilRight[+A, +B](maxRetries: Int, taskBuilder: Int => Task[Either[A, B]], isGuarded: Boolean, maxRecursionDepthPerExecutor: Int) extends AbstractTask[Either[A, B]] {
		override def subscribeSync(downChainObserver: MonoObserver[Either[A, B]]): Subscription = {
			new Subscription {
				private var isActive = true
				private var maybeInnerSubscription: Maybe[Subscription] = Maybe.empty

				def loop(attemptsAlreadyMade: Int, recursionDepth: Int): Unit = {
					if isActive then {
						val taskAorB: Task[Either[A, B]] =
							if isGuarded then {
								try taskBuilder(attemptsAlreadyMade) catch {
									case NonFatal(cause) => Task_fail(cause)
								}
							} else taskBuilder(attemptsAlreadyMade)
						val innerSubscription = taskAorB.subscribeSync(new MonoObserver[Either[A, B]] {
							override def onSuccess(aOrB: Either[A, B]): Unit = {
								if isActive then {
									maybeInnerSubscription = Maybe.empty
									aOrB match {
										case _: Right[A, B] => downChainObserver.onSuccess(aOrB)
										case la@Left(a) =>
											if attemptsAlreadyMade >= maxRetries then downChainObserver.onSuccess(la)
											else if recursionDepth < maxRecursionDepthPerExecutor then loop(attemptsAlreadyMade + 1, recursionDepth + 1)
											else run(loop(attemptsAlreadyMade + 1, 0))
									}
								}
							}

							override def onError(ex: Throwable): Unit = {
								if isActive then {
									isActive = false
									maybeInnerSubscription = Maybe.empty
									downChainObserver.onError(ex)
								}
							}
						})
						if isActive then maybeInnerSubscription = Maybe(innerSubscription)
					}
				}

				override def unsubscribeSync(): Unit = {
					checkWithin()
					if isActive then {
						isActive = false
						val mis = maybeInnerSubscription
						maybeInnerSubscription = Maybe.empty
						mis.foreach(_.unsubscribeSync())
					}
				}

				loop(0, 0)
			}
		}

		override def toString: String = deriveToString[Task_RetryUntilRight[A, B]](this)
	}
}
