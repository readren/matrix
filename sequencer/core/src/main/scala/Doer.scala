package readren.sequencer

import Doer.ExceptionReport

import scala.annotation.tailrec
import scala.collection.IterableFactory
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.reflect.ClassTag
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object Doer {
	/** Specifies what an instance of [[Doer]] requires to exist. */
	trait Assistant {
		/**
		 * Executes the provided [[Runnable]] in the orden of submission after all the ones that were provided before to this [[Assistant]] instance have been completed.
		 * The implementation should queue all the [[Runnable]]s this method receives while they are being executed sequentially. The thread that executes them can change as long as sequentiality and happens-before relationship are guaranteed.
		 * From now on the executor of the queued [[Runnable]] instances will be called "the doer's single-thread executor", or DoSiThEx for short, despite more than one thread may be involved.
		 * If the call is executed within the current DoSiThEx's [[Thread]], the [[Runnable]]'s execution must not start until the DoSiThEx completes its current execution and all the previously queued ones.
		 * The implementation should not throw non-fatal exceptions.
		 * The implementation should be thread-safe.
		 *
		 * All the deferred actions preformed by the [[Duty]] and [[Task]] operations are executed by calling this method unless the particular operation documentation says otherwise. That includes not only the call-back functions like `onComplete` but also all the functions, procedures, predicates, and by-name parameters they receive.
		 * */
		def executeSequentially(runnable: Runnable): Unit

		/**
		 * The implementation should return the [[Assistant]] instance corresponding to the DoSiThEx corresponding to the current [[java.lang.Thread]] if it knows it, or null if it doesn't.
		 * The implementation should know, at least, if the current [[java.lang.Thread]] corresponds to this [[Assistant]], and return this instance in that case. */
		def current: Assistant | Null

		/**
		 * @return true if the current [[java.lang.Thread]] is the currently assigned to this [[Assistant]] (the thread used to execute the [[Runnable]] passed to [[executeSequentially]]); or false otherwise. */
		inline def isWithinDoSiThEx: Boolean = this eq current

		/**
		 * The implementation should report the received [[Throwable]] somehow. Preferably including a description that identifies the provider of the DoSiThEx used by [[executeSequentially]] and mentions that the error was thrown by a deferred procedure programmed by means of a [[Task]].
		 * The implementation should not throw non-fatal exceptions.
		 * The implementation should be thread-safe.
		 * */
		def reportFailure(cause: Throwable): Unit
	}

	class ExceptionReport(message: String, cause: Throwable) extends RuntimeException(message, cause)
}

abstract class AbstractDoer extends Doer

/**
 * A '''Doer''' encloses [[Duty]] and [[Task]] instances, enforcing '''sequential execution''' of tasks and duties.
 * This sequentiality is '''scoped to the duties and tasks enclosed by the same instance of Doer'''. Specifically:
 *  - Duties and tasks created by the same '''Doer''' instance will execute sequentially relative to each other.
 *  - Duties and tasks created by different '''Doer''' instances are '''independent''' and may execute concurrently or in any order.
 *
 * == Execution of Routines ==
 * All routines (functions, procedures, predicates, or by-name parameters) passed to the operations of [[Duty]] and [[Task]]
 * (including callbacks like `onComplete`) are also executed sequentially with respect to the duties and tasks enclosed by
 * the same '''Doer''' instance. This ensures that all operations associated with a single '''Doer''' instance maintain sequential
 * consistency, unless explicitly documented otherwise in the method's documentation.
 *
 * == Key Points ==
 * - Sequential execution is '''instance-specific''': Each '''Doer''' instance manages its own sequence of tasks and duties.
 * - Routines passed to tasks and duties (e.g., callbacks) are executed in the same sequential scope as the enclosing '''Doer''' instance.
 * - Tasks and duties across different '''Doer''' instances are '''independent''' and may run concurrently.
 * ==Note:==
 * At the time of writing, almost all the operations and classes in this source file are thread-safe and may function properly on any kind of execution context. The only exceptions are the classes [[CombinedTask]] and [[Commitment]], which could be enhanced to support concurrency. However, given that a design goal was to allow [[Task]] and the functions their operators receive to close over variables in code sections guaranteed to be executed solely by the DoSiThEx (doer's single-threaded executor), the effort and cost of making them concurrent would be unnecessary.
 * See [[Doer.Assistant.executeSequentially()]].
 *
 * @define DoSiThEx DoSiThEx (doer's single-thread executor)
 * @define onCompleteExecutedByDoSiThEx The `onComplete` callback passed to `engage` is always, with no exception, executed by this $DoSiThEx. This is part of the contract of the [[Task]] trait.
 * @define threadSafe This method is thread-safe.
 * @define isExecutedByDoSiThEx Executed within the DoSiThEx (doer's single-thread executor).
 * @define unhandledErrorsArePropagatedToTaskResult The call to this routine is guarded with try-catch. If it throws a non-fatal exception it will be caught and the [[Task]] will complete with a [[Failure]] containing the error.
 * @define unhandledErrorsAreReported The call to this routine is guarded with a try-catch. If the evaluation throws a non-fatal exception it will be caught and reported with [[Doer.Assistant.reportFailure()]].
 * @define notGuarded CAUTION: The call to this function is NOT guarded with a try-catch. If its evaluation terminates abruptly the duty will never complete. The same occurs with all routines received by [[Duty]] operations. This is one of the main differences with [[Task]] operation.
 * @define maxRecursionDepthPerExecutor Maximum recursion depth per executor. Once this limit is reached, the recursion continues in a new executor. The result does not depend on this parameter as long as no [[java.lang.StackOverflowError]] occurs.
 * @define isWithinDoSiThEx indicates whether the caller is certain that this method is being executed within the $DoSiThEx. If there is no such certainty, the caller should set this parameter to `false` (or don't specify a value). This flag is useful for those [[Task]]s whose first action can or must be executed in the DoSiThEx, as it informs them that they can immediately execute said action synchronously. This method is thread-safe when this parameter value is false.
 */
trait Doer { thisDoer =>

	/** The type of the assistant of this [[Doer]] instance.  */
	type Assistant <: Doer.Assistant

	/** The assistant of this [[Doer]] instance. */
	val assistant: Assistant

	/**
	 * Queues the execution of the specified procedure in the task-queue of this $DoSiThEx. See [[Doer.Assistant.executeSequentially]]
	 * If the call is executed by the DoSiThEx the [[Runnable]]'s execution will not start until the DoSiThEx completes its current execution and gets free to start a new one.
	 *
	 * All the deferred actions preformed by the [[Task]] operations are executed by calling this method unless the particular operation documentation says otherwise. That includes not only the call-back functions like `onComplete` but also all the functions, procedures, predicates, and by-name parameters they receive as.
	 * This function only makes sense to call:
	 *		- from an action that is not executed by this DoSiThEx (the callback of a [[Future]], for example);
	 *		- or to avoid a stack overflow by continuing the recursion in a new execution.
	 *
	 * ==Note:==
	 * Is more efficient than the functionally equivalent: {{{ Task.mine(runnable.run).attemptAndForget(); }}}.
	 */
	inline def executeSequentially(inline procedure: => Unit): Unit = {
		${ DoerMacros.executeSequentiallyImpl('assistant, 'procedure) }
	}

	inline def reportFailure(cause: Throwable): Unit =
		${ DoerMacros.reportFailureImpl('assistant, 'cause) }

	/**
	 * An [[ExecutionContext]] that uses the $DoSiThEx. See [[Doer.assistant.executeSequentially]] */
	object ownSingleThreadExecutionContext extends ExecutionContext {
		def execute(runnable: Runnable): Unit = executeSequentially(runnable.run())

		def reportFailure(cause: Throwable): Unit = thisDoer.reportFailure(cause)
	}

	/////////////// DUTY ///////////////

	abstract class AbstractDuty[+A] extends Duty[A]


	/**
	 * Encapsulates one or more chained actions and provides operations to declaratively build complex duties from simpler ones.
	 * This tool eliminates the need for state variables that determine the decision-making flow, as the code structure itself indicates the execution order.
	 *
	 * This tool was created to simplify the implementation of an actor that needs to perform multiple duties simultaneously (as opposed to a finite state machine), because the overhead of separating them into different actors would be significant.
	 *
	 * Instances of [[Duty]] whose result is always the same follow the monadic laws. However, if the result depends on mutable variables or timing, these laws may be broken.
	 * For example, if a duty `d` closes over a mutable variable from the environment that affects its execution result, the equality of two supposedly equivalent expressions like {{{t.flatMap(f).flatMap(g) == t.flatMap(a => f(a).flatMap(g))}}} could be compromised. This would depend on the timing of when the variable is mutated—specifically if the mutation occurs between the start and end of execution.
	 * This does not mean that [[Duty]] implementations (and the routines their operations receive) must avoid closing over mutable variables altogether. Rather, it highlights that if strict adherence to monadic laws is required by your business logic, you should ensure that the mutable variable is not modified during task execution.
	 * For deterministic behavior, it's sufficient that any closed-over mutable variable is only mutated and accessed by actions executed in sequence. This is why the contract centralizes execution in the $DoSiThEx: to maintain determinism, even when closing over mutable variables, provided they are mutated and accessed solely within the $DoSiThEx.
	 *
	 * Design note: [[Duty]] is a member of [[Doer]] to ensure that access to [[Duty]] instances is restricted to the section of code where the owning [[Doer]] is exposed.
	 *
	 * CAUTION: Unlike [[Task]], [[Duty]] does NOT support failures. And unlike [[Task]], the call to routines received by its operations is not guarded with a try-catch. Therefore, unlike [[Task]], any unhandled exception thrown during the execution of a [[Duty]] will break the expected flow and the duty will never complete.
	 * It is recommended to use [[Task]] instead of [[Duty]] unless efficiency is a concern.
	 *
	 * @tparam A the type of result obtained when executing this duty.
	 */
	trait Duty[+A] { thisDuty =>
		/**
		 * This method performs the actions represented by the duty and calls `onComplete` within the $DoSiThEx context when the task finishes, regardless of whether it succeeds or fails.
		 *
		 * The implementation may assume this method is invoked within the $DoSiThEx.
		 *
		 * The implementation must respect the following exception-handling rules:
		 * - no exception thrown by the provided callback must be caught.
		 * - any other non-fatal exception throw by this method must be caught and propagated to the result or reported using [[Doer.assistant.reportFailure]] if propagation is not feasible.
		 * In the case of [[Task]] this includes non-fatal exceptions originated from routines passed in the class constructor that this method executes, including those captured over a closure.
		 * [[Duty]], on the other hand, assumes that these routines never throw exceptions. If an exception is thrown, the stack of the corresponding task will be completely unwound. 
		 * It is crucial to ensure that exceptions thrown by the onComplete callback are not caught, as this could suppress issues within the callback, preventing the execution of code expected to run and making it extremely difficult to diagnose the cause of a never-completing [[Duty]] or [[Task]].
		 *
		 * This method is the sole primitive operation of this trait; all other methods are derived from it.
		 *
		 * @param onComplete The callback that must be invoked upon the completion of this task. The implementation should call this callback
		 * within the $DoSiThEx context.
		 *
		 * The implementation may assume that `onComplete` will either terminate normally or
		 * fatally, but will not throw non-fatal exceptions.
		 */
		protected def engage(onComplete: A => Unit): Unit

		/** A bridge to access the [[engage]] method from macros in [[DoerMacros]] and sibling classes. */
		private[sequencer] inline final def engagePortal(onComplete: A => Unit): Unit = engage(onComplete)

		/**
		 * Triggers the execution of this [[Duty]].
		 *
		 * @param isWithinDoSiThEx $isWithinDoSiThEx
		 * @param onComplete called when the execution of this duty completes.
		 * Must not throw non-fatal exceptions: `onComplet` must either terminate normally or fatally, but never with a non-fatal exception.
		 * Note that if it terminates abruptly the `onComplete` will never be called.
		 * $isExecutedByDoSiThEx
		 */
		inline final def trigger(inline isWithinDoSiThEx: Boolean = assistant.isWithinDoSiThEx)(inline onComplete: A => Unit): Unit = {
			${ DoerMacros.triggerImpl('isWithinDoSiThEx, 'assistant, 'thisDuty, 'onComplete) }
		}

		/** Triggers the execution of this [[Task]] ignoring the result.
		 *
		 * $threadSafe
		 *
		 * @param isWithinDoSiThEx $isWithinDoSiThEx
		 */
		inline final def triggerAndForget(isWithinDoSiThEx: Boolean = assistant.isWithinDoSiThEx): Unit =
			trigger(isWithinDoSiThEx)(_ => {})


		/**
		 * Processes the result of this [[Duty]] once it is completed for its side effects.
		 * Is equivalent to {{{map[Unit](consumer)}}}
		 * @param consumer called with this task result when it completes. $isExecutedByDoSiThEx $notGuarded
		 */
		inline final def foreach(consumer: A => Unit): Duty[Unit] = new Map(thisDuty, consumer)

		/**
		 * Transforms this [[Duty]] by applying the given function to the result.
		 * ===Detailed behavior===
		 * Creates a [[Duty]] that, when executed, will trigger the execution of this duty and apply `f` to its result.
		 *
		 * $threadSafe
		 *
		 * @param f a function that transforms the result of this task, when it is successful.
		 *
		 * $isExecutedByDoSiThEx
		 *
		 * $notGuarded
		 */
		inline final def map[B](f: A => B): Duty[B] = new Map(thisDuty, f)

		/**
		 * Composes this [[Duty]] with a second one that is built from the result of this one.
		 * ===Detailed behavior===
		 * Creates a [[Duty]] that, when executed, it will:
		 *		- Trigger the execution of this task and applies the `taskBBuilder` function to its result.
		 *		- Then triggers the execution of the built duty and completes with its result.
		 *
		 * $threadSafe
		 *
		 * @param f a function that receives the result of this task and returns the task to be executed next.
		 *
		 * $isExecutedByDoSiThEx
		 *
		 * $notGuarded
		 */
		inline final def flatMap[B](f: A => Duty[B]): Duty[B] = new FlatMap(thisDuty, f)

		/** Applies the side-effecting function to the result of this duty without affecting the propagated value.
		 * The result of the provided function is always ignored and therefore not propagated in any way.
		 * This method allows to enforce many callbacks to receive the same value and to be executed in the order they are chained.
		 * It's worth mentioning that the side-effecting function is executed before triggering the next duty in the chain.
		 * ===Detailed description===
		 * Returns a duty that, when executed, it will:
		 *		- trigger the execution of this duty,
		 *		- then apply the received function to this duty's result,
		 *		- and finally complete with the result of this task (ignoring the functions result).
		 *
		 * $threadSafe
		 *
		 * @param sideEffect a side-effecting function. The call to this function is wrapped in a try-catch block; however, unlike most other operators, unhandled non-fatal exceptions are not propagated to the result of the returned task. $isExecutedByDoSiThEx
		 */
		def andThen(sideEffect: A => Any): Duty[A] = new AndThen[A](thisDuty, sideEffect)

		/** Wraps this [[Duty]] into a [[Task]].
		 * Together with [[Task.toDuty]] this method allow to mix duties and task in the same chain.
		 * ===Detailed behavior===
		 * Creates a [[Task]] that, when executed, triggers the execution of this [[Duty]] and completes with its result, which will always be successful.
		 * @return a [[Task]] whose result is the result of this task wrapped inside a [[Success]]. */
		inline final def toTask: Task[A] = new ToTask(thisDuty)

		/**
		 * Triggers the execution of this [[Task]] and returns a [[Future]] containing its result wrapped inside a [[Success]].
		 *
		 * Is equivalent to {{{ transform(Success.apply).toFuture(isWithinDoSiThEx) }}}
		 *
		 * Useful when failures need to be propagated to the next for-expression (or for-binding).
		 *
		 * @param isWithinDoSiThEx $isWithinDoSiThEx
		 * @return a [[Future]] that will be completed when this [[Task]] is completed.
		 */
		def toFutureHardy(isWithinDoSiThEx: Boolean = assistant.isWithinDoSiThEx): Future[A] = {
			val promise = Promise[A]()
			trigger(isWithinDoSiThEx)(a => promise.success(a))
			promise.future
		}

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
		 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
		 * @param condition function that decides if the loop continues or not based on:
		 *  - the number of already completed cycles,
		 *  - and the result of the last execution of the `dutyA`.
		 * The loop ends when this function returns a [[Maybe.some]]. Its content will be the final result.
		 * @return a new [[Duty]] that, when executed, repeatedly executes this duty and applies the `condition` to the duty's result until the function's result is [[Maybe.some]]. The result of this duty is the contents of said [[Maybe]].
		 */
		final inline def repeatedUntilSome[B](maxRecursionDepthPerExecutor: Int = 9)(condition: (Int, A) => Maybe[B]): Duty[B] =
			new RepeatUntilSome(thisDuty, maxRecursionDepthPerExecutor)(condition)

		/**
		 * Like [[repeatedUntilSome]] but the condition is a [[PartialFunction]] instead of a function that returns [[Maybe]].
		 */
		final inline def repeatedUntilDefined[B](maxRecursionDepthPerExecutor: Int = 9)(pf: PartialFunction[(Int, A), B]): Duty[B] =
			repeatedUntilSome(maxRecursionDepthPerExecutor)(Function.untupled(Maybe.liftPartialFunction(pf)))

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
		 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
		 * @param condition determines whether a new cycle should be performed based on the number of times it has already been evaluated and either the result of the previous cycle or `da0` if no cycle has been done yet.
		 * @tparam S a supertype of `A`
		 * @tparam B the type of the result of this duty.
		 */
		final inline def repeatedWhileEmpty[S >: A, B](s0: S, maxRecursionDepthPerExecutor: Int = 9)(condition: (Int, S) => Maybe[B]): Duty[B] =
			new RepeatWhileEmpty[S, B](thisDuty, s0, maxRecursionDepthPerExecutor)(condition)

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
		 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
		 * @param pf determines whether a new cycle should be performed based on the number of times it has already been evaluated and either the result of the previous cycle or `da0` if no cycle has been done yet.
		 * @tparam S a supertype of `A`
		 * @tparam B the type of the result of this duty.
		 */
		final inline def repeatedWhileUndefined[S >: A, B](s0: S, maxRecursionDepthPerExecutor: Int = 9)(pf: PartialFunction[(Int, S), B]): Duty[B] =
			repeatedWhileEmpty(s0, maxRecursionDepthPerExecutor)(Function.untupled(Maybe.liftPartialFunction(pf)))

		/**
		 * Wraps this duty into another that belongs to another [[Doer]].
		 * Useful to chain [[Duty]]'s operations that involve different [[Doer]]s.
		 * ===Detailed behavior===
		 * Returns a duty that belongs to the specified [[Doer]] that, when executed, it will execute this duty within this [[Doer]]'s $DoSiThEx and complete with the same result but within the specified [[Doer]]'s DoSiThEx.
		 *
		 * $threadSafe
		 *
		 * @param otherDoer the [[Doer]] to which the returned duty will belong.
		 */
		def onBehalfOf(otherDoer: Doer): otherDoer.Duty[A] =
			otherDoer.Duty.foreign(thisDoer)(this)

		/** Casts the type-path of this [[Duty]] to the received [[Doer]].
		 * This operation does nothing at runtime. It merely satisfies the compiler to allow operations on [[Duty]]s with distinct type paths.
		 */
		def castTypePath[E <: Doer](doer: E): doer.Duty[A] = {
			assert(thisDoer eq doer)
			this.asInstanceOf[doer.Duty[A]]
		}
	}

	object Duty {

		/** A [[Duty]] that yields [[Unit]]. */
		val unit: Duty[Unit] = ready(())

		/** Creates a [[Duty]] whose execution never ends.
		 * $threadSafe
		 *
		 * @return a [[Duty]] whose execution never ends.
		 * */
		val never: Duty[Nothing] = NotEver

		/** Creates a [[Duty]] whose result is calculated at the call site even before the duty is constructed.
		 * $threadSafe
		 *
		 * @param a the already calculated result of the returned [[Duty]]. */
		inline def ready[A](a: A): Duty[A] = new Ready(a)

		/** Creates a [[Duty]] whose result is the value returned by the provided supplier.
		 * ===Detailed behavior===
		 * Creates a duty that, when executed, evaluates the `supplier` within the $DoSiThEx. If the evaluation finishes:
		 *		- abruptly, will never complete.
		 *		- normally, completes with the evaluation's result.
		 *
		 * $$threadSafe
		 *
		 * @param supplier the supplier of the result. $isExecutedByDoSiThEx $notGuarded
		 * @return the task described in the method description.
		 */
		inline def mine[A](supplier: () => A): Duty[A] = new Mine(supplier)

		/** Creates a [[Duty]] whose result is the result of the [[Duty]] created by the provided supplier.
		 * Is equivalent to: {{{mine(supplier).flatMap(identity)}}} but slightly more efficient
		 * ===Detailed behavior===
		 * Creates a duty that, when executed:
		 *		- evaluates the `supplier` within the $DoSiThEx;
		 *		- then triggers the execution of the returned Duty;
		 *		- finally completes with the result of executed duty.
		 *
		 * $$threadSafe
		 *
		 * @param supplier the supplier of the duty whose execution will give the result. $isExecutedByDoSiThEx $notGuarded
		 * @return the duty described in the method description.
		 */
		inline def mineFlat[A](supplier: () => Duty[A]): Duty[A] = new MineFlat(supplier)

		/** Creates a [[Duty]] that, when executed, triggers the execution of a duty that belongs to another [[Doer]] within that [[Doer]]'s $DoSiThEx; and completes with the same result as the `foreignDuty` but within this [[Doer]]'s DoSiThEx.
		 * Useful to start a process in a foreign [[Doer]] and access its result as if it were executed sequentially.
		 * $threadSafe
		 *
		 * @param foreignDoer the [[Doer]] to whom the `foreignTask` belongs.
		 * @return a duty that belongs to this [[Doer]] that completes when the `foreignDuty` is completed by the `foreignDoer`. */
		inline def foreign[A](foreignDoer: Doer)(foreignDuty: foreignDoer.Duty[A]): Duty[A] = {
			if foreignDoer eq thisDoer then foreignDuty.asInstanceOf[thisDoer.Duty[A]]
			else new DelegateTo[A](foreignDoer, foreignDuty)
		}

		/**
		 * Creates a [[Duty]] that, when executed, simultaneously triggers the execution of two tasks and returns their results combined by the provided function.
		 * Given the single-thread nature of [[Doer]] this operation only has sense when the provided duties involve foreign duties/tasks or alien duties/tasks.
		 * ===Detailed behavior===
		 * Creates a new [[Duty]] that, when executed:
		 *		- triggers the execution of both: `dutyA` and `dutyB`
		 *		- when both are completed, completes with the value that results of applying the function `f` to their results.
		 *
		 * $threadSafe
		 *
		 * @param dutyA a task
		 * @param dutyB a task
		 * @param f the function that combines the results of the `taskA` and `taskB`. $isExecutedByDoSiThEx $unhandledErrorsArePropagatedToTaskResult
		 * @return the task described in the method description.
		 */
		inline def combine[A, B, C](dutyA: Duty[A], dutyB: Duty[B])(f: (A, B) => C): Duty[C] =
			new ForkJoin(dutyA, dutyB, f)

		/**
		 * Creates a [[Duty]] that, when executed, simultaneously triggers the execution of all the [[Duty]]s in the provided iterable, and completes with a collection containing their results in the same order if all are successful, or a failure if any is faulty.
		 * This overload is only convenient for very small lists. For large ones it is not efficient and also may cause stack-overflow when the task is executed.
		 * Use the other overload for large lists or other kind of iterables.
		 *
		 * $threadSafe
		 *
		 * @param duties the `Iterable` of duties that the returned task will trigger simultaneously to combine their results.
		 * @tparam A the result type of all the duties
		 * @return the duty described in the method description.
		 * */
		def sequence[A](duties: List[Duty[A]]): Duty[List[A]] = {
			@tailrec
			def loop(incompleteResult: Duty[List[A]], remainingDuties: List[Duty[A]]): Duty[List[A]] = {
				remainingDuties match {
					case Nil =>
						incompleteResult
					case head :: tail =>
						val lessIncompleteResult = combine(head, incompleteResult) { (a, as) => a :: as }
						loop(lessIncompleteResult, tail)
				}
			}

			duties.reverse match {
				case Nil => ready(Nil)
				case lastDuty :: previousDuties => loop(lastDuty.map(List(_)), previousDuties);
			}
		}

		/**
		 * Creates a [[Duty]] that, when executed, simultaneously triggers the execution of all the [[Duty]]s in the received iterable, and completes with a collection containing their results in the same order.
		 * This overload accepts any [[Iterable]] and is more efficient than the other (above). Especially for large iterables.
		 * $threadSafe
		 *
		 * @param factory the [[IterableFactory]] needed to build the [[Iterable]] that will contain the results. Note that most [[Iterable]] implementations' companion objects are an [[IterableFactory]].
		 * @param duties the `Iterable` of duties that the returned task will trigger simultaneously to combine their results.
		 * @tparam A the result type of all the duties
		 * @tparam C the higher-kinded type of the `Iterable` of duties.
		 * @tparam To the type of the `Iterable` that will contain the results.
		 * @return the duty described in the method description.
		 * */
		def sequence[A: ClassTag, C[x] <: Iterable[x], To[_]](factory: IterableFactory[To], duties: C[Duty[A]]): Duty[To[A]] = {
			sequenceToArray(duties).map { array =>
				val builder = factory.newBuilder[A]
				var index = 0
				while index < array.length do {
					builder.addOne(array(index))
					index += 1
				}
				builder.result()
			}
		}

		/** Like [[sequence]] but the resulting collection's higher-kinded type `To` is fixed to [[Array]]. */
		inline def sequenceToArray[A: ClassTag, C[x] <: Iterable[x]](duties: C[Duty[A]]): Duty[Array[A]] = new SequenceDuty[A, C](duties)

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
		 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
		 * @param condition function that, based on the `completedExecutionsCounter` and the iteration's state `a`, determines if the loop should end or otherwise creates the [[Duty]] to execute in the next iteration.
		 * @tparam A the type of the state passed from an iteration to the next.
		 * @tparam B the type of the result of created [[Duty]]
		 */
		def whileRightRepeat[A, B](a0: A, maxRecursionDepthPerExecutor: Int = 9)(condition: (Int, A) => Either[B, Duty[A]]): Duty[B] =
			new WhileRightRepeat[A, B](a0, maxRecursionDepthPerExecutor)(condition)

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
		 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
		 * @param condition function that, based on the `completedExecutionsCounter` and the iteration's state (which starts with `a0`), determines if the loop should end or otherwise creates the [[Duty]] to execute in the next iteration.
		 * @tparam A the type of the state passed from an iteration to the next.
		 * @tparam B the type of the result of created [[Duty]]
		 */
		inline def repeatUntilLeft[A, B](a0: A, maxRecursionDepthPerExecutor: Int = 9)(condition: (Int, A) => Duty[Either[B, A]]): Duty[B] =
			new RepeatUntilLeft(a0, maxRecursionDepthPerExecutor)(condition)

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
		 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
		 * @param taskBuilder function to construct tasks, taking the retry count as input.
		 */
		inline def retryUntilRight[A, B](maxRetries: Int, maxRecursionDepthPerExecutor: Int = 9)(taskBuilder: Int => Duty[Either[A, B]]): Duty[Either[A, B]] =
			new RetryUntilRight[A, B](maxRetries, maxRecursionDepthPerExecutor)(taskBuilder)

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
		 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
		 * @param controlTaskBuilder the function that builds the control duty. It takes three parameters:
		 * - the number of already executed interleaved duties.
		 * - the result of the control duty in the previous cycle or `a0` in the first cycle.
		 * - the result of the interleaved duty in the previous cycle or `b0` in the first cycle.
		 * @param interleavedDutyBuilder the function that builds the interleaved duties. It takes two parameters:
		 *		- the number of already executed interleaved duties.
		 *		- the result of the control duty in the current cycle.
		 * */

		def repeatInterleavedUntilLeftDuty[A, B, R](a0: A, b0: B, maxRecursionDepthPerExecutor: Int = 9)(controlTaskBuilder: (Int, A, B) => Duty[Either[R, A]])(interleavedDutyBuilder: (Int, A) => Duty[B]): Duty[R] = {

			repeatUntilLeft[(A, B), R]((a0, b0), maxRecursionDepthPerExecutor) { (completedCycles, ab) =>
				controlTaskBuilder(completedCycles, ab._1, ab._2).flatMap {
					case Left(r) =>
						Duty.ready(Left(r))

					case Right(a) =>
						interleavedDutyBuilder(completedCycles, a).map(b => Right((a, b)))
				}
			}
		}

	}

	final class Map[A, B](cA: Duty[A], f: A => B) extends AbstractDuty[B] {
		override def engage(onComplete: B => Unit): Unit = cA.engagePortal { a => onComplete(f(a)) }

		override def toString: String = deriveToString[Map[A, B]](this)
	}

	final class FlatMap[A, B](cA: Duty[A], f: A => Duty[B]) extends AbstractDuty[B] {
		override def engage(onComplete: B => Unit): Unit = cA.engagePortal { a => f(a).engagePortal(onComplete) }

		override def toString: String = deriveToString[FlatMap[A, B]](this)
	}

	final class AndThen[A](dutyA: Duty[A], sideEffect: A => Any) extends AbstractDuty[A] {
		override def engage(onComplete: A => Unit): Unit =
			dutyA.engagePortal { a =>
				sideEffect(a)
				onComplete(a)
			}

		override def toString: String = deriveToString[AndThen[A]](this)
	}

	final class ToTask[A](cA: Duty[A]) extends AbstractTask[A] {
		override def engage(onComplete: Try[A] => Unit): Unit = cA.engagePortal(onComplete.compose(Success.apply))

		override def toString: String = deriveToString[ToTask[A]](this)
	}

	object NotEver extends AbstractDuty[Nothing] {
		override def engage(onComplete: Nothing => Unit): Unit = ()

		override def toString: String = "NotEver"
	}

	final class Ready[A](a: A) extends AbstractDuty[A] {
		override def engage(onComplete: A => Unit): Unit = onComplete(a)

		override def toFutureHardy(isWithinDoSiThEx: Boolean = assistant.isWithinDoSiThEx): Future[A] = Future.successful(a)

		override def toString: String = deriveToString[Ready[A]](this)
	}

	final class Mine[A](supplier: () => A) extends AbstractDuty[A] {
		override def engage(onComplete: A => Unit): Unit = onComplete(supplier())

		override def toString: String = deriveToString[Mine[A]](this)
	}

	final class MineFlat[A](supplier: () => Duty[A]) extends AbstractDuty[A] {
		override def engage(onComplete: A => Unit): Unit = supplier().engagePortal(onComplete)

		override def toString: String = deriveToString[MineFlat[A]](this)
	}

	final class DelegateTo[A](foreignDoer: Doer, foreignDuty: foreignDoer.Duty[A]) extends AbstractDuty[A] {
		override def engage(onComplete: A => Unit): Unit = foreignDuty.trigger()(a => executeSequentially(onComplete(a)))

		override def toString: String = deriveToString[DelegateTo[A]](this)
	}

	/** A [[Duty]] that, when executed:
	 *		- triggers the execution of both: `taskA` and `taskB`;
	 *		- when both are completed, completes with the result of applying the function `f` to their results.
	 *
	 * $threadSafe
	 * $onCompleteExecutedByDoSiThEx
	 */
	final class ForkJoin[+A, +B, +C](dutyA: Duty[A], dutyB: Duty[B], f: (A, B) => C) extends AbstractDuty[C] {
		override def engage(onComplete: C => Unit): Unit = {
			var ma: Maybe[A] = Maybe.empty;
			var mb: Maybe[B] = Maybe.empty;
			dutyA.engagePortal { a =>
				mb.fold {
					ma = Maybe.some(a)
				} { b => onComplete(f(a, b)) }
			}
			dutyB.engagePortal { b =>
				ma.fold {
					mb = Maybe.some(b)
				} { a => onComplete(f(a, b)) }
			}
		}

		override def toString: String = deriveToString[ForkJoin[A, B, C]](this)
	}

	/** @see [[Duty.sequenceToArray]] */
	class SequenceDuty[A: ClassTag, C[x] <: Iterable[x]](duties: C[Duty[A]]) extends AbstractDuty[Array[A]] {
		override def engage(onComplete: Array[A] => Unit): Unit = {
			val size = duties.size
			val array = Array.ofDim[A](size)
			if size == 0 then onComplete(array)
			else {
				val dutyIterator = duties.iterator
				var completedCounter: Int = 0
				var index = 0
				while index < size do {
					val duty = dutyIterator.next
					val dutyIndex = index
					duty.engagePortal { a =>
						array(dutyIndex) = a
						completedCounter += 1
						if completedCounter == size then onComplete(array)
					}
					index += 1
				}
			}
		}
	}


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
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 * @param condition function that decides if the loop continues or not based on:
	 *		- the number of already completed cycles,
	 *		- and the result of the last execution of the `dutyA`.
	 * The loop ends when this function returns a [[Maybe.some]]. Its content will be the final result of this task.
	 */
	final class RepeatUntilSome[+A, +B](dutyA: Duty[A], maxRecursionDepthPerExecutor: Int = 9)(condition: (Int, A) => Maybe[B]) extends AbstractDuty[B] {
		override def engage(onComplete: B => Unit): Unit = {
			def loop(completedCycles: Int, recursionDepth: Int): Unit = {
				dutyA.engagePortal { a =>
					condition(completedCycles, a).fold {
						if (recursionDepth < maxRecursionDepthPerExecutor) {
							loop(completedCycles + 1, recursionDepth + 1)
						} else {
							executeSequentially(loop(completedCycles + 1, 0))
						}
					}(onComplete)
				}
			}

			loop(0, 0)
		}

		override def toString: String = deriveToString[RepeatUntilSome[A, B]](this)
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
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 * @param condition determines if a new cycle should be performed based on the number of already completed cycles and the last result of `dutyA` or `a0` if no cycle has been completed.
	 */
	final class RepeatWhileEmpty[+A, +B](dutyA: Duty[A], a0: A, maxRecursionDepthPerExecutor: Int = 9)(condition: (Int, A) => Maybe[B]) extends AbstractDuty[B] {
		override def engage(onComplete: B => Unit): Unit = {
			def loop(completedCycles: Int, lastDutyResult: A, recursionDepth: Int): Unit = {
				condition(completedCycles, lastDutyResult).fold {
					dutyA.engagePortal { newA =>
						if recursionDepth < maxRecursionDepthPerExecutor then loop(completedCycles + 1, newA, recursionDepth + 1)
						else executeSequentially(loop(completedCycles + 1, newA, 0))
					}
				}(onComplete)
			}

			loop(0, a0, 0)
		}

		override def toString: String = deriveToString[RepeatWhileEmpty[A, B]](this)
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
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 * @param checkAndBuild function that takes completed cycles count and last duty result, returning an `Either[B, Duty[A]]`.
	 */
	final class WhileRightRepeat[+A, +B](a0: A, maxRecursionDepthPerExecutor: Int = 9)(checkAndBuild: (Int, A) => Either[B, Duty[A]]) extends AbstractDuty[B] {
		override def engage(onComplete: B => Unit): Unit = {
			def loop(completedCycles: Int, lastDutyResult: A, recursionDepth: Int): Unit = {
				checkAndBuild(completedCycles, lastDutyResult) match {
					case Left(b) => onComplete(b)
					case Right(dutyA) =>
						dutyA.engagePortal { newA =>
							if recursionDepth < maxRecursionDepthPerExecutor then loop(completedCycles + 1, newA, recursionDepth + 1)
							else executeSequentially(loop(completedCycles + 1, newA, 0))
						}
				}
			}

			loop(0, a0, 0)
		}

		override def toString: String = deriveToString[WhileRightRepeat[A, B]](this)
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
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 * @param buildAndCheck function that takes completed cycles count and last duty result, and returns a new duty that yields an `Either[B, A]`.
	 */
	final class RepeatUntilLeft[+A, +B](a0: A, maxRecursionDepthPerExecutor: Int = 9)(buildAndCheck: (Int, A) => Duty[Either[B, A]]) extends AbstractDuty[B] {
		override def engage(onComplete: B => Unit): Unit = {
			def loop(executionsCounter: Int, lastDutyResult: A, recursionDepth: Int): Unit = {
				val duty = buildAndCheck(executionsCounter, lastDutyResult)
				duty.engagePortal {
					case Left(b) => onComplete(b)
					case Right(a) =>
						if recursionDepth < maxRecursionDepthPerExecutor then loop(executionsCounter + 1, a, recursionDepth + 1)
						else executeSequentially(loop(executionsCounter + 1, a, 0))
				}
			}

			loop(0, a0, 0)
		}

		override def toString: String = deriveToString[RepeatUntilLeft[A, B]](this)
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
	final class RetryUntilRight[+A, +B](maxRetries: Int, maxRecursionDepthPerExecutor: Int = 9)(taskBuilder: Int => Duty[Either[A, B]]) extends AbstractDuty[Either[A, B]] {
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
							executeSequentially(loop(attemptsAlreadyMade + 1, 0))
						}
				}
			}

			loop(0, 0)
		}

		override def toString: String = deriveToString[RetryUntilRight[A, B]](this)
	}

	////////////// COVENANT ///////////////

	/** A [[Duty]] that allows to subscribe/unsubscribe consumers of its result. */
	abstract class SubscriptableDuty[+A] extends AbstractDuty[A] {
		/**
		 * Subscribes a consumer of the result of this [[Duty]].
		 *
		 * The subscription is automatically removed after this [[Duty]] is completed and the received consumer is executed.
		 *
		 * If this [[Duty]] is already completed when this method is called, the method executes the provided consumer synchronously and does not make a subscription.
		 *
		 * CAUTION: This method does not prevent duplicate subscriptions.
		 * CAUTION: Should be called within the $DoSiThEx */
		def subscribe(onComplete: A => Unit): Unit

		/**
		 * Removes a subscription made with [[subscribe]].
		 *
		 * CAUTION: Should be called within the $DoSiThEx */
		def unsubscribe(onComplete: A => Unit): Unit

		/** @return `true` if the provided consumer was already subscribed. */
		def isAlreadySubscribed(onComplete: A => Unit): Boolean
	}

	/** A covenant to complete a [[SubscriptableDuty]].
	 * The [[SubscriptableDuty]] that this [[Covenant]] promises to fulfill is itself. This duty is completed when this [[Covenant]] is fulfilled, either immediately by calling [[fulfill]], or after the completion of a specified duty by calling [[fulfillWith]].
	 *
	 * [[Covenant]] is to [[Duty]] as [[Commitment]] is to [[Task]], and as [[scala.concurrent.Promise]] is to [[scala.concurrent.Future]]
	 * */
	final class Covenant[A] extends SubscriptableDuty[A] { thisCovenant =>
		private var oResult: Maybe[A] = Maybe.empty;
		private var firstOnCompleteObserver: (A => Unit) | Null = null
		private var onCompletedObservers: List[A => Unit] = Nil;

		/** The [[SubscriptableDuty]] this [[Covenant]] promises to fulfill. */
		inline def subscriptableDuty: SubscriptableDuty[A] = thisCovenant

		/** CAUTION: Should be called within the $DoSiThEx
		 *  @return true if this [[Covenant]] was fulfilled; or false if it is still pending. */
		inline def isCompleted: Boolean = {
			assert(assistant.isWithinDoSiThEx)
			this.oResult.isDefined
		}

		/** CAUTION: Should be called within the $DoSiThEx
		 *  @return true if this [[Covenant]] is still pending; or false if it was completed. */
		inline def isPending: Boolean = {
			assert(assistant.isWithinDoSiThEx)
			this.oResult.isEmpty
		}

		protected override def engage(onComplete: A => Unit): Unit = subscribe(onComplete)

		override def subscribe(onComplete: A => Unit): Unit = {
			assert(assistant.isWithinDoSiThEx)
			oResult.fold {
				if firstOnCompleteObserver eq null then firstOnCompleteObserver = onComplete
				else onCompletedObservers = onComplete :: onCompletedObservers
			}(onComplete)
		}

		override def unsubscribe(onComplete: A => Unit): Unit = {
			assert(assistant.isWithinDoSiThEx)
			if firstOnCompleteObserver eq onComplete then {
				if onCompletedObservers.isEmpty then firstOnCompleteObserver = null
				else {
					firstOnCompleteObserver = onCompletedObservers.head
					onCompletedObservers = onCompletedObservers.tail
				}
			} else onCompletedObservers = onCompletedObservers.filterNot(_ ne onComplete)
		}

		override def isAlreadySubscribed(onComplete: A => Unit): Boolean = {
			assert(assistant.isWithinDoSiThEx)
			(firstOnCompleteObserver eq onComplete) || onCompletedObservers.exists(_ eq onComplete)
		}

		/** Provokes that the [[Duty]] that this [[Covenant]] promises to complete to be completed with the received `result`.
		 *
		 * @param result the result of the [[Duty]] this [[Covenant]] promised to complete . */
		def fulfill(result: A, isWithinDoSiThEx: Boolean = assistant.isWithinDoSiThEx)(onAlreadyCompleted: A => Unit = _ => ()): this.type = {
			if isWithinDoSiThEx then {
				assert(assistant.isWithinDoSiThEx)
				fulfillHere(result)(onAlreadyCompleted)
			}
			else executeSequentially(fulfillHere(result)(onAlreadyCompleted))
			this;
		}

		/** Provokes that the [[Duty]] that this [[Covenant]] promises to complete to be completed with the received `result`.
		 * CAUTION: Should be called within the $DoSiThEx
		 * @param result the result of the [[Duty]] this [[Covenant]] promised to complete . */
		private def fulfillHere(result: A)(onAlreadyCompleted: A => Unit = _ => ()): this.type = {
			oResult.fold {
				this.oResult = Maybe.some(result);
				this.onCompletedObservers.foreach(_(result));
				this.onCompletedObservers = Nil // Clean the observers list to help the garbage collector.
				if firstOnCompleteObserver ne null then {
					firstOnCompleteObserver(result);
					firstOnCompleteObserver = null // Clean the observers list to help the garbage collector.
				}
			}(onAlreadyCompleted)
			this
		}

		/** Triggers the execution of the specified [[Duty]] and completes the [[Duty]] that this [[Covenant]] promises to fulfill with the result of the specified duty once it finishes. */
		def fulfillWith(dutyA: Duty[A], isWithinDoSiThEx: Boolean = assistant.isWithinDoSiThEx)(onAlreadyCompleted: A => Unit = _ => ()): this.type = {
			if (dutyA ne this)
				dutyA.trigger(isWithinDoSiThEx)(result => fulfillHere(result)(onAlreadyCompleted));
			this
		}
	}


	///////////// TASK //////////////

	abstract class AbstractTask[+A] extends Task[A]


	/**
	 * A hardy version of [[Duty]].
	 * Advantages of [[Task]] compared to [[Duty]]:
	 *		- results are wrapped withing a [[Try]] which allow the support of failed results.
	 *		- the call to the routines received by the operations are guarded with a try-catch, which allows to propagate failures through [[Task]] chains.
	 *		- can encapsulate a [[Future]] making interoperability with them easier.
	 *
	 * Design note: [[Task]] and [[Duty]] are a member of [[Doer]] to ensure that the access to instances of them is restricted to the section of code where the owning [[Doer]] is exposed.
	 *
	 * @tparam A the type of the result obtained when executing this task.
	 */
	trait Task[+A] extends Duty[Try[A]] { thisTask =>

		/** Triggers the execution of this [[Task]] and returns a [[Future]] of its result.
		 *
		 * @param isWithinDoSiThEx $isWithinDoSiThEx
		 * @return a [[Future]] that will be completed when this [[Task]] is completed.
		 */
		def toFuture(isWithinDoSiThEx: Boolean = assistant.isWithinDoSiThEx): Future[A] = {
			val promise = Promise[A]()
			trigger(isWithinDoSiThEx)(promise.complete)
			promise.future
		}

		/** Triggers the execution of this [[Task]] noticing faulty results.
		 *
		 * @param isWithinDoSiThEx $isWithinDoSiThEx
		 * @param errorHandler called when the execution of this task completes with a failure. $isExecutedByDoSiThEx $unhandledErrorsAreReported
		 */
		def triggerAndForgetHandlingErrors(errorHandler: Throwable => Unit, isWithinDoSiThEx: Boolean = assistant.isWithinDoSiThEx): Unit =
			trigger(isWithinDoSiThEx) {
				case Failure(e) =>
					try errorHandler(e) catch {
						case NonFatal(cause) => reportFailure(cause)
					}
				case _ => ()
			}

		/**
		 * Processes the result of the task once it is completed, for its side effects.
		 * ===Detailed behavior===
		 * A [[Task]] that, when executed, it will:
		 *		- trigger the execution of `taskA` and if the result is:
		 *			- a [[Failure]], completes with that failure without calling `consumer`.	
		 *			- `Success(a)`, tries to apply the `consumer` to `a`. If the evaluation finishes:
		 *				- abruptly, completes with a [[Failure]] containing the cause.
		 *				- normally, completes with `Success(())`.
		 *
		 * @param consumer called with this task result when it completes, if it ever does.
		 */
		inline def consume(consumer: Try[A] => Unit): Task[Unit] =
			new Consume[A](thisTask, consumer)

		/**
		 * Processes the result of the task once it is completed successfully, for its side effects.
		 * WARNING: `consumer` will not be called if this task completes with a failure.
		 *
		 * @param consumer called with this task result when it completes, if it ever does.
		 * */
		final def foreach(consumer: A => Unit): Task[Unit] =
			consume {
				case Success(a) => consumer(a)
				case _ => ()
			}

		/**
		 * Transform this task by applying the given function to the result. Analogous to [[Future.transform]]
		 * ===Detailed description===
		 * Creates a [[Task]] that, when executed, triggers the execution of this task and applies the received `resultTransformer` to its result. If the evaluation finishes:
		 *		- abruptly, completes with the cause.
		 *		- normally, completes with the result of the evaluation.
		 *
		 * $threadSafe
		 *
		 * @param resultTransformer applied to the result of the `originalTask` to obtain the result of this task.
		 *
		 * $isExecutedByDoSiThEx
		 *
		 * $unhandledErrorsArePropagatedToTaskResult
		 */
		inline final def transform[B](resultTransformer: Try[A] => Try[B]): Task[B] = new Transform(thisTask, resultTransformer)


		/**
		 * Composes this [[Task]] with a second one that is built from the result of this one. Analogous to [[Future.transformWith]].
		 * ===Detailed behavior===
		 * Creates a [[Task]] that, when executed, it will:
		 *		- Trigger the execution of this task and apply the `taskBBuilder` function to its result. If the evaluation finishes:
		 *			- abruptly, completes with the cause.
		 *			- normally with `taskB`, triggers the execution of `taskB` and completes with its result.
		 *
		 * $threadSafe
		 *
		 * @param taskBBuilder a function that receives the result of `taskA` and builds the task to be executed next. $isExecutedByDoSiThEx $unhandledErrorsArePropagatedToTaskResult
		 */
		inline final def transformWith[B](taskBBuilder: Try[A] => Task[B]): Task[B] =
			new Compose(thisTask, taskBBuilder)


		/**
		 * Transforms this task by applying the given function to the result if it is successful. Analogous to [[Future.map]].
		 * See [[recover]] and [[toDuty]] if you want to transform the failures; and [[transform]] if you want to transform both, successful and failed ones.
		 * ===Detailed behavior===
		 * Creates a [[Task]] that, when executed, it will trigger the execution of this task and, if the result is:
		 *		- `Failure(e)`, completes with that failure.
		 *		- `Success(a)`, apply `f` to `a` and if the evaluation finishes:
		 *			- abruptly with `cause`, completes with `Failure(cause)`.
		 *			- normally with value `b`, completes with `Success(b)`.
		 *
		 * $threadSafe
		 *
		 * @param f a function that transforms the result of this task, when it is successful. $isExecutedByDoSiThEx $unhandledErrorsArePropagatedToTaskResult
		 */
		inline final def map[B](f: A => B): Task[B] = transform(_ map f)

		/**
		 * Composes this [[Task]] with a second one that is built from the result of this one, but only when this one is successful. Analogous to [[Future.flatMap]].
		 * ===Detailed behavior===
		 * Creates a [[Task]] that, when executed, it will:
		 *		- Trigger the execution of this task and if the result is:
		 *			- `Failure(e)`, completes with that failure.
		 *			- `Success(a)`, applies the `taskBBuilder` function to `a`. If the evaluation finishes:
		 *				- abruptly, completes with the cause.
		 *				- normally with `taskB`, triggers the execution of `taskB` and completes with its result.
		 *
		 * $threadSafe
		 *
		 * @param taskBBuilder a function that receives the result of `taskA`, when it is a [[Success]], and returns the task to be executed next. $isExecutedByDoSiThEx $unhandledErrorsArePropagatedToTaskResult
		 */
		final def flatMap[B](taskBBuilder: A => Task[B]): Task[B] = transformWith {
			case Success(a) => taskBBuilder(a);
			case fail@Failure(_) => Task.ready(fail.castTo[B]);
		}

		/** Needed to support filtering and case matching in for-compressions. The for-expressions (or for-bindings) after the filter are not executed if the [[predicate]] is not satisfied.
		 * Detailed behavior: Gives a [[Task]] that, when executed, it will:
		 *		- executes this [[Task]] and, if the result is a:
		 *			- [[Failure]], completes with that failure.
		 *			- [[Success]], applies the `predicate` to its content and if the evaluation finishes:
		 *				- abruptly, completes with the cause.
		 *				- normally with a `false`, completes with a [[Failure]] containing a [[NoSuchElementException]].
		 *				- normally with a `true`, completes with the result of this task.
		 *
		 * $threadSafe
		 *
		 * @param predicate a predicate that determines which values are propagated to the following for-bindings.
		 * */
		inline final def withFilter(predicate: A => Boolean): Task[A] = new WithFilter(thisTask, predicate)

		/** Applies the side-effecting function to the result of this task without affecting the propagated value.
		 * The result of the provided function is always ignored and therefore not propagated in any way.
		 * This method allows to enforce many callbacks to receive the same value and to be executed in the order they are chained.
		 * It's worth mentioning that the side-effecting function is executed before triggering the next duty in the chain.
		 * ===Detailed description===
		 * Returns a task that, when executed:
		 *		- first executes this task;
		 *		- second applies the received function to the result and, if the evaluation finishes:
		 *			- normally, completes with the result of this task.
		 *			- abruptly with a non-fatal exception, reports the failure cause to [[Doer.Assistant.reportFailure]] and completes with the result of this task.
		 *			- abruptly with a fatal exception, never completes.
		 *
		 * $threadSafe
		 *
		 * @param sideEffect a side-effecting function. The call to this function is wrapped in a try-catch block; however, unlike most other operators, unhandled non-fatal exceptions are not propagated to the result of the returned task. $isExecutedByDoSiThEx
		 */
		final override def andThen(sideEffect: Try[A] => Any): Task[A] = {
			transform { tryA =>
				try sideEffect(tryA)
				catch {
					case NonFatal(e) => reportFailure(e)
				}
				tryA
			}
		}

		/**
		 * Wraps this [[Task]] into a [[Duty]] applying the given function to transform failure results into successful ones. This is like [[map]] but for the throwable; and like [[recover]] but with a complete function.
		 * Together with [[Duty.toTask]] this method allow to mix duties and task in the same chain.
		 *
		 * @param exceptionHandler the complete function to apply to the result of this task if it is a [[Failure]]. $isExecutedByDoSiThEx */
		inline final def toDuty[B >: A](exceptionHandler: Throwable => B): Duty[B] = new ToDuty[A, B](thisTask, exceptionHandler)

		/** Transforms this task applying the given partial function to failure results. This is like map but for the throwable; and like [[toDuty]] but with a partial function. Analogous to [[Future.recover]].
		 * ===detailed description===
		 * Returns a new [[Task]] that, when executed, executes this task and if the result is:
		 * 		- a [[Success]] or a [[Failure]] for which `pf` is not defined, completes with the same result.
		 *		- a [[Failure]] for which `pf` is defined, applies `pf` to it and if the evaluation finishes:
		 *			- abruptly, completes with the cause.
		 *			- normally, completes with the result of the evaluation.
		 *
		 * $threadSafe
		 *
		 * @param pf the [[PartialFunction]] to apply to the result of this task if it is a [[Failure]]. $isExecutedByDoSiThEx $unhandledErrorsArePropagatedToTaskResult
		 */
		final def recover[B >: A](pf: PartialFunction[Throwable, B]): Task[B] =
			transform {
				_.recover(pf)
			}

		/** Composes this task with a second one that is built from the result of this one, but only when said result is a [[Failure]] for which the given partial function is defined. This is like flatMap but for the exception. Analogous to [[Future.recoverWith]].
		 * ===detailed description===
		 * Returns a new [[Task]] that, when executed, executes this task and if the result is:
		 * 		- a [[Success]] or a [[Failure]] for which `pf` is not defined, completes with the same result.
		 *		- a [[Failure]] for which `pf` is defined, applies `pf` to it and if the evaluation finishes:
		 *			- abruptly, completes with the cause.
		 *			- normally returning a [[Task]], triggers the execution of said task and completes with its same result.
		 *
		 * $threadSafe
		 *
		 * @param pf the [[PartialFunction]] to apply to the result of this task, if it is a [[Failure]], to build the second task. $isExecutedByDoSiThEx $unhandledErrorsArePropagatedToTaskResult
		 */
		final def recoverWith[B >: A](pf: PartialFunction[Throwable, Task[B]]): Task[B] = {
			transformWith[B] {
				case Failure(t) => pf.applyOrElse(t, (e: Throwable) => new Immediate(Failure(e)));
				case sa@Success(_) => new Immediate(sa);
			}
		}

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
		 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
		 * @param condition function that decides if the loop continues or not based on:
		 * 	- the number of already completed cycles,
		 * 	- and the result of the last execution of the `taskA`.
		 * The loop ends when this function returns a [[Maybe.some]]. Its content will be the final result.
		 *
		 * $isExecutedByDoSiThEx $unhandledErrorsArePropagatedToTaskResult
		 * @return a new [[Task]] that, when executed, repeatedly executes this task and applies the `condition` to the task's result until the function's result is [[Maybe.some]]. The result of this task is the contents of said [[Maybe]] unless any execution of the `taskA` or `condition` terminates abruptly in which case this task result is the cause.
		 * */
		inline final def reiteratedHardyUntilSome[B](maxRecursionDepthPerExecutor: Int = 9)(condition: (Int, Try[A]) => Maybe[Try[B]]): Task[B] =
			new ReiterateHardyUntilSome(thisTask, maxRecursionDepthPerExecutor)(condition)

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
		 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
		 * @param condition function that decides if the loop continues or not based on:
		 * 	- the number of already completed cycles,
		 * 	- and the result of the last execution of the `taskA`.
		 * The loop ends when this function returns a [[Maybe.some]]. Its content will be the final result.
		 *
		 * $isExecutedByDoSiThEx $unhandledErrorsArePropagatedToTaskResult
		 * @return a new [[Task]] that, when executed, repeatedly executes this task and applies the `condition` to the task's result until the function's result is [[Maybe.some]]. The result of this task is the contents of said [[Maybe]] unless any execution of the `taskA` or `condition` terminates abruptly in which case this task result is the cause.
		 * */
		inline final def reiteratedUntilSome[B](maxRecursionDepthPerExecutor: Int = 9)(condition: (Int, A) => Maybe[Try[B]]): Task[B] =
			reiteratedHardyUntilSome(maxRecursionDepthPerExecutor) { (completedCycles, tryA) =>
				tryA match {
					case Success(a) => condition(completedCycles, a)
					case f: Failure[A] => Maybe.some(f.castTo[B])
				}
			}


		/** Like [[repeatedHardlyUntilSome]] but the condition is a [[PartialFunction]] instead of a function that returns [[Maybe]]. */
		inline final def reiteratedHardyUntilDefined[B](maxRecursionDepthPerExecutor: Int = 9)(pf: PartialFunction[(Int, Try[A]), Try[B]]): Task[B] =
			reiteratedHardyUntilSome(maxRecursionDepthPerExecutor)(Function.untupled(Maybe.liftPartialFunction(pf)))

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
		 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
		 * @param condition determines whether a new cycle should be performed based on the number of times it has already been evaluated and either the result of the previous cycle or `ta0` if no cycle has been done yet. $isExecutedByDoSiThEx $unhandledErrorsArePropagatedToTaskResult
		 * @tparam S a supertype of `A`
		 * @tparam B the type of the result of this task.
		 */
		inline final def reiteratedWhileEmpty[S >: A, B](ts0: Try[S], maxRecursionDepthPerExecutor: Int = 9)(condition: (Int, Try[S]) => Maybe[B]): Task[B] =
			new ReiterateHardyWhileEmpty[S, B](thisTask, ts0, maxRecursionDepthPerExecutor: Int)(condition)

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
		 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
		 * @param pf determines whether a new cycle should be performed based on the number of times it has already been evaluated and either the result of the previous cycle or `ta0` if no cycle has been done yet. $isExecutedByDoSiThEx $unhandledErrorsArePropagatedToTaskResult
		 * @tparam S a supertype of `A`
		 * @tparam B the type of the result of this task.
		 */
		inline final def reiteratedWhileUndefined[S >: A, B](ts0: Try[S], maxRecursionDepthPerExecutor: Int = 9)(pf: PartialFunction[(Int, Try[S]), B]): Task[B] = {
			reiteratedWhileEmpty(ts0, maxRecursionDepthPerExecutor)(Function.untupled(Maybe.liftPartialFunction(pf)));
		}

		/**
		 * Wraps this task into another that belongs to other [[Doer]].
		 * Useful to chain [[Task]]'s operations that involve different [[Doer]]s.
		 * ===Detailed behavior===
		 * Returns a task that belongs to the specified [[Doer]] that, when executed, it will execute this task within this [[Doer]]'s $DoSiThEx and complete with the same result but within specified [[Doers]]'s DoSiThEx.
		 * CAUTION: Avoid closing over the same mutable variable from two transformations applied to Task instances belonging to different [[Doer]]s.
		 * Remember that all routines (e.g., functions, procedures, predicates, and callbacks) provided to [[Task]] methods are executed by the $DoSiThEx of the [[Doer]] that owns the [[Task]] instance on which the method is called.
		 * Therefore, calling [[trigger]] on the returned task will execute the `onComplete` passed to it within the $DoSiThEx of the `otherDoer`.
		 *
		 * $threadSafe
		 *
		 * @param otherDoer the [[Doer]] to which the returned task will belong.
		 * */
		override def onBehalfOf(otherDoer: Doer): otherDoer.Task[A] =
			otherDoer.Task.foreign(thisDoer)(this)

		/** Casts the type-path of this [[Task]] to the received [[Doer]]. Usar con cautela.
		 * Esta operación no hace nada en tiempo de ejecución. Solo engaña al compilador para evitar que chille cuando se opera con [[Task]]s que tienen distinto type-path pero se sabe que corresponden al mismo actor ejecutor.
		 * Se decidió hacer que [[Task]] sea un inner class del actor ejecutor para detectar en tiempo de compilación cuando se usa una instancia fuera de dicho actor.
		 * Usar el type-path checking para detectar en tiempo de compilación cuando una [[Task]] está siendo usada fuera del actor ejecutor es muy valioso, pero tiene un costo: El chequeo de type-path es más estricto de lo necesario para este propósito y, por ende, el compilador reportará errores de tipo en situaciones donde se sabe que el actor ejecutor es el correcto. Esta operación ([[castTypePath()]]) está para tratar esos casos.
		 */
		override def castTypePath[E <: Doer](doer: E): doer.Task[A] = {
			assert(thisDoer eq doer);
			this.asInstanceOf[doer.Task[A]]
		}
	}

	object Task {

		/** A [[Task]] yields [[Unit]]. */
		val unit: Task[Unit] = successful(())

		/** A [[Task]] whose execution never ends. */
		val never: Task[Nothing] = Never

		/** Creates a [[Task]] whose result is calculated at the call site even before the task is constructed. The result of its execution is always the provided value.
		 *
		 * $threadSafe
		 *
		 * @param tryA the value that the returned task will give as result every time it is executed.
		 * @return the task described in the method description.
		 */
		inline final def ready[A](tryA: Try[A]): Task[A] = new Immediate(tryA);

		/** Creates a [[Task]] that always succeeds with a result that is calculated at the call site even before the task is constructed. The result of its execution is always a [[Success]] with the provided value.
		 *
		 * $threadSafe
		 *
		 * @param a the value contained in the [[Success]] that the returned task will give as result every time it is executed.
		 * @return the task described in the method description.
		 */
		inline final def successful[A](a: A): Task[A] = new Immediate(Success(a));

		/** Creates a [[Task]] that always fails with a result that is calculated at the call site even before the task is constructed. The result of its execution is always a [[Failure]] with the provided [[Throwable]]
		 *
		 * $threadSafe
		 *
		 * @param throwable the exception contained in the [[Failure]] that the returned task will give as result every time it is executed.
		 * @return the task described in the method description.
		 */
		inline final def failed[A](throwable: Throwable): Task[A] = new Immediate(Failure(throwable));

		/**
		 * Creates a task whose result is the result of the provided supplier.
		 * ===Detailed behavior===
		 * Creates a task that, when executed, evaluates the `resultSupplier` within the $DoSiThEx. If the evaluation finishes:
		 *		- abruptly, completes with a [[Failure]] with the cause.
		 *		- normally, completes with the evaluation's result.
		 *
		 * $$threadSafe
		 *
		 * @param supplier the supplier of the result. $isExecutedByDoSiThEx $unhandledErrorsArePropagatedToTaskResult
		 * @return the task described in the method description.
		 */
		inline final def own[A](supplier: () => Try[A]): Task[A] = new Own(supplier);

		/**
		 * Creates a task whose result is the result of applying [[Successful.apply]] to the result of the provided supplier as long as the evaluation of the supplier finishes normally; otherwise its result is a failure with the cause.
		 * ===Detailed behavior===
		 * Creates a task that, when executed, evaluates the `resultSupplier` within the $DoSiThEx. If it finishes:
		 *		- abruptly, completes with a [[Failure]] containing the cause.
		 *		- normally, completes with a [[Success]] containing the evaluation's result.
		 *
		 * Is equivalent to {{{ own { () => Success(resultSupplier()) } }}}
		 *
		 * $threadSafe
		 *
		 * @param supplier La acción que estará encapsulada en la Task creada. $isExecutedByDoSiThEx $unhandledErrorsArePropagatedToTaskResult
		 * @return the task described in the method description.
		 */
		inline final def mine[A](supplier: () => A): Task[A] = new Own(() => Success(supplier()));

		/**
		 * Creates a task whose result is the result of the task returned by the provided supplier.
		 * Is equivalent to: {{{own(supplier).flatMap(identity)}}} but slightly more efficient.
		 * ===Detailed behavior===
		 * Creates a task that, when executed, evaluates the `supplier` within the $DoSiThEx. If the evaluation finishes:
		 *		- abruptly, completes with a [[Failure]] with the cause.
		 *		- normally, triggers the execution of the returned task and completes with its result.
		 *
		 * $$threadSafe
		 *
		 * @param supplier the supplier of the result. $isExecutedByDoSiThEx $unhandledErrorsArePropagatedToTaskResult
		 * @return the task described in the method description.
		 */
		inline def ownFlat[A](supplier: () => Task[A]): Task[A] = new OwnFlat(supplier)

		/** Create a [[Task]] whose result will be the result of the provided [[Future]] when it completes.
		 * Useful to access the result of a process that was already started in an alien executor as if it were executed sequentially.
		 *
		 * $threadSafe
		 *
		 * @param future the future to wait for.
		 * @return the task described in the method description.
		 */
		inline final def wait[A](future: Future[A]): Task[A] = new Wait(future);

		/** Creates a [[Task]] whose result will be the result of the [[Future]] returned by the provided supplier.
		 * Useful to start a process in an alien executor and access its result as if it were executed sequentially.
		 * The alien executor may be the $DoSiThEx of this [[Doer]].
		 *
		 * $threadSafe
		 *
		 * @param supplier a function that starts the process and return a [[Future]] of its result. $isExecutedByDoSiThEx $unhandledErrorsArePropagatedToTaskResult
		 * @return the task described in the method description.
		 */
		inline final def alien[A](supplier: () => Future[A]): Task[A] = new Alien(supplier);

		/**
		 * Creates a [[Task]] that, when executed, triggers the execution the `foreignTask` (a task that belongs to another [[Doer]]) within that [[Doer]]'s $DoSiThEx; and completes with the same result as the foreign task but within this [[Doer]]'s DoSiThEx.
		 * Useful to start a process in a foreign [[Doer]] and access its result as if it were executed sequentially.
		 *
		 * $threadSafe
		 *
		 * @param foreignDoer the [[Doer]] to whom the `foreignTask` belongs.
		 * @return a task that belongs to this [[Doer]] and completes when the `foreignTask` is completed by the `foreignDoer`. */
		inline final def foreign[A](foreignDoer: Doer)(foreignTask: foreignDoer.Task[A]): Task[A] = {
			if foreignDoer eq thisDoer then foreignTask.asInstanceOf[thisDoer.Task[A]]
			else new Foreign(foreignDoer)(foreignTask)
		}

		/**
		 * Creates a [[Task]] that simultaneously triggers the execution of two tasks and returns their results combined with the received function.
		 * Given the single thread nature of [[Doer]] this operation only has sense when the received tasks are a chain of actions that involve timers, foreign, or alien tasks.
		 * ===Detailed behavior===
		 * Creates a new [[Task]] that, when executed:
		 *		- triggers the execution of both: `taskA` and `taskB`
		 *		- when both are completed, whether normal or abruptly, the function `f` is applied to their results and if the evaluation finishes:
		 *			- abruptly, completes with a [[Failure]] containing the cause.
		 *			- normally, completes with the evaluation's result.
		 *
		 * $threadSafe
		 *
		 * @param taskA a task
		 * @param taskB a task
		 * @param f the function that combines the results of the `taskA` and `taskB`. $isExecutedByDoSiThEx $unhandledErrorsArePropagatedToTaskResult
		 * @return the task described in the method description.
		 */
		inline final def combine[A, B, C](taskA: Task[A], taskB: Task[B])(f: (Try[A], Try[B]) => Try[C]): Task[C] =
			new CombinedTask(taskA, taskB, f)

		/**
		 * Creates a task that, when executed, simultaneously triggers the execution of all the [[Task]]s in the received list, and completes with a list containing their results in the same order.
		 * This overload only accepts [[List]]s and is only convenient when the list is small. For large ones it is not efficient and also may cause stack-overflow when the task is executed.
		 * Use the other overload for large lists or other kind of iterables.
		 *
		 * $threadSafe
		 *
		 * @param tasks the list of [[Task]]s that the returned task will trigger simultaneously to combine their results.
		 * @return the task described in the method description.
		 *
		 * */
		final def sequence[A](tasks: List[Task[A]]): Task[List[A]] = {
			@tailrec
			def loop(incompleteResult: Task[List[A]], remainingTasks: List[Task[A]]): Task[List[A]] = {
				remainingTasks match {
					case Nil =>
						incompleteResult
					case head :: tail =>
						val lessIncompleteResult = combine(incompleteResult, head) { (tla, ta) =>
							for {
								la <- tla
								a <- ta
							} yield a :: la
						}
						loop(lessIncompleteResult, tail)
				}
			}

			tasks.reverse match {
				case Nil => successful(Nil)
				case lastTask :: previousTasks => loop(lastTask.map(List(_)), previousTasks);
			}
		}

		/**
		 * Creates a task that, when executed, simultaneously triggers the execution of all the [[Task]]s in the received list, and completes with a list containing their results in the same order if all are successful, or a Failure if anyone is faulty.
		 * This overload accepts any [[Iterable]] and is more efficient than the other (above). Especially for large iterables.
		 * $threadSafe
		 *
		 * @param factory the [[IterableFactory]] needed to build the [[Iterable]] that will contain the results. Note that most [[Iterable]] implementations' companion objects are an [[IterableFactory]].
		 * @param tasks the `Iterable` of tasks that the returned task will trigger simultaneously to combine their results.
		 * @tparam A the result type of all the tasks.
		 * @tparam C the higher-kinded type of the `Iterable` of tasks.
		 * @tparam To the type of the `Iterable` that will contain the results.
		 * @return the task described in the method description.
		 * */
		def sequence[A: ClassTag, C[x] <: Iterable[x], To[x] <: Iterable[x]](factory: IterableFactory[To], tasks: C[Task[A]]): Task[To[A]] = {
			sequenceToArray(tasks).map { array =>
				val builder = factory.newBuilder[A]
				var index = 0
				while index < array.length do {
					builder.addOne(array(index))
					index += 1
				}
				builder.result()
			}
		}

		/** Like [[sequence]] but the resulting collection's higher-kinded type `To` is fixed to [[Array]]. */
		inline def sequenceToArray[A: ClassTag, C[x] <: Iterable[x]](tasks: C[Task[A]]): Task[Array[A]] = new SequenceTask[A, C](tasks)


		/**
		 * Creates a task that, when executed, simultaneously triggers the execution of all the [[Task]]s in the received list, and completes with a list containing their results, successful or not, in the same order.
		 * $threadSafe
		 *
		 * @param tasks the `Iterable` of tasks that the returned task will trigger simultaneously to combine their results.
		 * @param factory the [[IterableFactory]] needed to build the [[Iterable]] that will contain the results. Note that most [[Iterable]] implementations' companion objects are an [[IterableFactory]].
		 * @tparam A the result type of all the tasks.
		 * @tparam C the higher-kinded type of the `Iterable` of tasks.
		 * @tparam To the type of the `Iterable` that will contain the results.
		 * @return the task described in the method description.
		 * */
		def sequenceHardy[A: ClassTag, C[x] <: Iterable[x], To[x] <: Iterable[x]](factory: IterableFactory[To], tasks: C[Task[A]]): Task[To[Try[A]]] = {
			sequenceHardyToArray(tasks).map { array =>
				val builder = factory.newBuilder[Try[A]]
				var index = 0
				while index < array.length do {
					builder.addOne(array(index))
					index += 1
				}
				builder.result()
			}
		}

		/** Like [[sequenceHardy]] but the resulting collection's higher-kinded type `To` is fixed to [[Array]]. */
		inline def sequenceHardyToArray[A: ClassTag, C[x] <: Iterable[x]](tasks: C[Task[A]]): Task[Array[Try[A]]] = new SequenceHardy[A, C](tasks)

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
		 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
		 * @param condition function that, based on the `completedExecutionsCounter` and the iteration's state `tryA`, determines if the loop should end or otherwise creates the [[Task]] to execute in the next iteration.
		 * @tparam A the type of the state passed from an iteration to the next.
		 * @tparam B the type of the result of created [[Task]]
		 */
		inline final def whileRightReiterateHardy[A, B](tryA0: Try[A], maxRecursionDepthPerExecutor: Int = 9)(condition: (Int, Try[A]) => Either[Try[B], Task[A]]): Task[B] =
			new WhileRightReiterateHardy[A, B](tryA0, maxRecursionDepthPerExecutor)(condition);

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
		 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
		 * @param condition function that, based on the `completedExecutionsCounter` and the iteration's state `a`, determines if the loop should end or otherwise creates the [[Task]] to execute in the next iteration.
		 * @tparam A the type of the state passed from an iteration to the next.
		 * @tparam B the type of the result of created [[Task]]
		 */
		final def whileRightReiterate[A, B](a0: A, maxRecursionDepthPerExecutor: Int = 9)(condition: (Int, A) => Either[Try[B], Task[A]]): Task[B] = {
			whileRightReiterateHardy[A, B](Success(a0), maxRecursionDepthPerExecutor) { (completedCycles, tryA) =>
				tryA match {
					case Success(a) => condition(completedCycles, a)
					case Failure(cause) => Left(Failure(cause))
				}
			}
		}

		@deprecated("lo hice como ejercicio")
		private def reiterateUntilLeft2[A, B](tryA0: Try[A], maxRecursionDepthPerExecutor: Int = 9)(condition: (Int, Try[A]) => Task[Either[Try[B], A]]): Task[B] =
			whileRightReiterateHardy[Either[Try[B], A], B](tryA0.map(Right(_)), maxRecursionDepthPerExecutor)((completedExecutionsCounter, previousState) =>
				previousState match {
					case Success(Right(x)) => Right(condition(completedExecutionsCounter, Success(x)))
					case Success(Left(x)) => Left(x)
					case Failure(cause) => Right(condition(completedExecutionsCounter, Failure(cause)))
				}
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
		 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
		 * @param condition function that, based on the `completedExecutionsCounter` and the iteration's state (which starts with `a0`), determines if the loop should end or otherwise creates the [[Duty]] to execute in the next iteration.
		 * @tparam A the type of the state passed from an iteration to the next.
		 * @tparam B the type of the result of created [[Duty]]
		 * */
		inline final def reiterateUntilLeft[A, B](a0: A, maxRecursionDepthPerExecutor: Int = 9)(condition: (Int, A) => Task[Either[Try[B], A]]): Task[B] =
			new ReiterateUntilLeft(a0, maxRecursionDepthPerExecutor)(condition)


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
		 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
		 * @param taskBuilder function to construct tasks, taking the retry count as input.
		 * */
		inline final def attemptUntilRight[A, B](maxRetries: Int, maxRecursionDepthPerExecutor: Int = 9)(taskBuilder: Int => Task[Either[A, B]]): Task[Either[A, B]] =
			new AttemptUntilRight[A, B](maxRetries, maxRecursionDepthPerExecutor)(taskBuilder)

		@deprecated("No se usa. Lo hice como ejercicio")
		private def attemptUntilRight2[A, B](maxRetries: Int, maxRecursionDepthPerExecutor: Int = 9)(taskBuilder: Int => Task[Either[A, B]]): Task[Either[A, B]] =
			reiterateUntilLeft[Null, Either[A, B]](null, maxRecursionDepthPerExecutor) { (triesCounter, _) =>
				taskBuilder(triesCounter).map {
					case rb@Right(b) => Left(Success(rb))
					case Left(a) => Right(null)
				}
			}


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
		 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
		 * @param controlTaskBuilder the function that builds the control tasks. It takes three parameters:
		 * - the number of already executed interleaved tasks.
		 * - the result of the control task in the previous cycle or `a0` in the first cycle.
		 * - the result of the interleaved task in the previous cycle or `b0` in the first cycle.
		 * @param interleavedTaskBuilder the function that builds the interleaved tasks. It takes two parameters:
		 * - the number of already executed interleaved tasks.
		 *  - the result of the control task in the current cycle.
		 * */
		final def reiterateInterleavedUntilLeft[A, B, R](a0: A, b0: B, maxRecursionDepthPerExecutor: Int = 9)(controlTaskBuilder: (Int, A, B) => Task[Either[Try[R], A]])(interleavedTaskBuilder: (Int, A) => Task[B]): Task[R] = {

			reiterateUntilLeft[(A, B), R]((a0, b0), maxRecursionDepthPerExecutor) { (completedCycles, ab) =>
				controlTaskBuilder(completedCycles, ab._1, ab._2).flatMap {
					case Left(tryR) =>
						Task.successful(Left(tryR))

					case Right(a) =>
						interleavedTaskBuilder(completedCycles, a).map(b => Right((a, b)))

				}
			}
		}
	}

	/** A [[Task]] that never completes.
	 *
	 * $onCompleteExecutedByDoSiThEx
	 * */
	object Never extends AbstractTask[Nothing] {
		override def engage(onComplete: Try[Nothing] => Unit): Unit = ()
	}

	/**
	 * @param taskA the [[Task]] from where this duty takes its result.
	 * @param exceptionHandler converts the faulty results of the `taskA` to instances of `B`.
	 *
	 * $notGuarded */
	final class ToDuty[A, B >: A](taskA: Task[A], exceptionHandler: Throwable => B) extends AbstractDuty[B] {
		override def engage(onComplete: B => Unit): Unit = {
			taskA.engagePortal { tryA =>
				val b = tryA match {
					case Success(a) => a
					case Failure(exception) => exceptionHandler(exception)
				}
				onComplete(b)
			}
		}

		override def toString: String = deriveToString[ToDuty[A, B]](this)
	}


	/** A [[Task]] whose result is calculated at the call site even before the task is constructed. The result of its execution is always the provided value.
	 *
	 * $threadSafe
	 *
	 * @param tryA the value that the returned task will give as result every time it is executed.
	 * @return the task described in the method description.
	 * $onCompleteExecutedByDoSiThEx
	 */
	final class Immediate[+A](tryA: Try[A]) extends AbstractTask[A] {
		override def engage(onComplete: Try[A] => Unit): Unit =
			onComplete(tryA)

		override def toFuture(isWithinDoSiThEx: Boolean = assistant.isWithinDoSiThEx): Future[A] =
			Future.fromTry(tryA)

		override def toFutureHardy(isWithinDoSiThEx: Boolean = assistant.isWithinDoSiThEx): Future[Try[A]] =
			Future.successful(tryA)

		override def toString: String = deriveToString[Immediate[A]](this)
	}

	/** A [[Task]] that, when executed, calls the `supplier` within the $DoSiThEx and if it finishes:
	 *  - abruptly, completes with the cause.
	 *  - normally, completes with the result.
	 *
	 * $onCompleteExecutedByDoSiThEx
	 *
	 * @param supplier builds the result of this [[Task]]. $isExecutedByDoSiThEx $unhandledErrorsArePropagatedToTaskResult
	 */
	final class Own[+A](supplier: () => Try[A]) extends AbstractTask[A] {
		override def engage(onComplete: Try[A] => Unit): Unit = {
			val result =
				try supplier()
				catch {
					case NonFatal(e) => Failure(e)
				}
			onComplete(result)
		}

		override def toString: String = deriveToString[Own[A]](this)
	}

	final class OwnFlat[+A](supplier: () => Task[A]) extends AbstractTask[A] {
		override def engage(onComplete: Try[A] => Unit): Unit = {
			val taskA =
				try supplier()
				catch {
					case NonFatal(e) => Task.failed(e)
				}
			taskA.engagePortal(onComplete)
		}
	}

	/** A [[Task]] that just waits the completion of the specified [[Future]]. The result of the task, when executed, is the result of the received [[Future]].
	 *
	 * $onCompleteExecutedByDoSiThEx
	 *
	 * @param future the future to wait for.
	 */
	final class Wait[+A](future: Future[A]) extends AbstractTask[A] {
		override def engage(onComplete: Try[A] => Unit): Unit =
			future.onComplete(onComplete)(using ownSingleThreadExecutionContext)

		override def toString: String = deriveToString[Wait[A]](this)
	}

	/** A [[Task]] that, when executed, triggers the execution of a process in an alien executor and waits its result, successful or not.
	 * The process executor may be the $DoSiThEx but if that is the intention [[Own]] would be more appropriate.
	 *
	 * $onCompleteExecutedByDoSiThEx
	 *
	 * @param builder a function that starts the process and return a [[Future]] of its result. $isExecutedByDoSiThEx $unhandledErrorsArePropagatedToTaskResult
	 * */
	final class Alien[+A](builder: () => Future[A]) extends AbstractTask[A] {
		override def engage(onComplete: Try[A] => Unit): Unit = {
			val future =
				try builder()
				catch {
					case NonFatal(e) => Future.failed(e)
				}
			future.onComplete(onComplete)(using ownSingleThreadExecutionContext)
		}

		override def toString: String = deriveToString[Alien[A]](this)
	}

	final class Foreign[+A](foreignDoer: Doer)(foreignTask: foreignDoer.Task[A]) extends AbstractTask[A] {
		override def engage(onComplete: Try[A] => Unit): Unit = {
			foreignTask.trigger() { tryA => executeSequentially(onComplete(tryA)) }
		}

		override def toString: String = deriveToString[Foreign[A]](this)
	}

	/**
	 * A [[Task]] that consumes the results of the received task for its side effects.
	 * ===Detailed behavior===
	 * A [[Task]] that, when executed, it will:
	 *		- trigger the execution of `taskA` and tries to apply the `consumer` to its result. If the evaluation finishes:
	 *			- abruptly, completes with a [[Failure]] containing the cause.
	 *			- normally, completes with `Success(())`.
	 */
	final class Consume[A](taskA: Task[A], consumer: Try[A] => Unit) extends AbstractTask[Unit] {
		override def engage(onComplete: Try[Unit] => Unit): Unit = {
			taskA.engagePortal { tryA =>
				val tryConsumerResult =
					try Success(consumer(tryA))
					catch {
						case NonFatal(cause) => Failure(cause)
					}
				onComplete(tryConsumerResult)
			}
		}

		override def toString: String = deriveToString[Consume[A]](this)
	}

	/**
	 * A [[Task]] that transforms the received task by converting successful results to `Failure(new NoSuchElementException())` if it does not satisfy the received `predicate`.
	 * Needed to support filtering and case matching in for-compressions. The for-expressions (or for-bindings) after the filter are not executed if the [[predicate]] is not satisfied.
	 * ===Detailed behavior===
	 * A [[Task]] that, when executed, it will:
	 *		- trigger the execution of `taskA` and, if the result is a:
	 *			- [[Failure]], completes with that failure.
	 *			- [[Success]], tries to apply the `predicate` to its content. If the evaluation finishes:
	 *				- abruptly, completes with the cause.
	 *				- normally with a `false`, completes with a [[Failure]] containing a [[NoSuchElementException]].
	 *				- normally with a `true`, completes with the result of this task.
	 *
	 * $onCompleteExecutedByDoSiThEx
	 *
	 * */
	final class WithFilter[A](taskA: Task[A], predicate: A => Boolean) extends AbstractTask[A] {
		override def engage(onComplete: Try[A] => Unit): Unit = {
			taskA.engagePortal {
				case sa@Success(a) =>
					val predicateResult =
						try {
							if predicate(a) then sa
							else Failure(new NoSuchElementException(s"Task filter predicate is not satisfied for $a"))
						} catch {
							case NonFatal(cause) =>
								Failure(cause)
						}
					onComplete(predicateResult)

				case f@Failure(_) =>
					onComplete(f)
			}
		}

		override def toString: String = deriveToString[WithFilter[A]](this)
	}


	/**
	 * A [[Task]] that transform the result of another task.
	 * ===Detailed description===
	 * A [[Task]] that, when executed, triggers the execution of the `originalTask` and applies the received `resultTransformer` to its results. If the evaluation finishes:
	 *		- abruptly, completes with the cause.
	 *		- normally, completes with the result of the evaluation.
	 *
	 * $onCompleteExecutedByDoSiThEx
	 *
	 * @param resultTransformer applied to the result of the `originalTask` to obtain the result of this task.
	 *
	 * $isExecutedByDoSiThEx
	 *
	 * $unhandledErrorsArePropagatedToTaskResult
	 */
	final class Transform[+A, +B](originalTask: Task[A], resultTransformer: Try[A] => Try[B]) extends AbstractTask[B] {
		override def engage(onComplete: Try[B] => Unit): Unit = {
			originalTask.engagePortal { tryA =>
				val tryB =
					try resultTransformer(tryA)
					catch {
						case NonFatal(e) => Failure(e);
					}
				onComplete(tryB)
			};
		}

		override def toString: String = deriveToString[Transform[A, B]](this)
	}

	/**
	 * A [[Task]] that composes two [[Task]]s where the second is build from the result of the first.
	 * ===Detailed behavior===
	 * A [[Task]] which, when executed, it will:
	 *		- Trigger the execution of `taskA` and apply the `taskBBuilder` function to its result. If the evaluation finishes:
	 *			- abruptly, completes with the cause.
	 *			- normally with `taskB`, triggers the execution of `taskB` and completes with its result.
	 *
	 * $threadSafe
	 *
	 * @param taskA the task that is executed first.
	 * @param taskBBuilder a function that receives the result of the execution of `taskA` and builds the task to be executed next. $isExecutedByDoSiThEx $unhandledErrorsArePropagatedToTaskResult
	 */
	final class Compose[+A, +B](taskA: Task[A], taskBBuilder: Try[A] => Task[B]) extends AbstractTask[B] {
		override def engage(onComplete: Try[B] => Unit): Unit = {
			taskA.engagePortal { tryA =>
				val taskB =
					try taskBBuilder(tryA)
					catch {
						case NonFatal(e) => Task.failed(e)
					}
				taskB.engagePortal(onComplete)
			}
		}

		override def toString: String = deriveToString[Compose[A, B]](this)
	}


	/** A [[Task]] that, when executed:
	 *		- triggers the execution of both: `taskA` and `taskB`;
	 *		- when both are completed, whether normal or abruptly, the function `f` is applied to their results and if the evaluation finishes:
	 *			- abruptly, completes with the cause.
	 *			- normally, completes with the result.
	 *
	 * $threadSafe
	 * $onCompleteExecutedByDoSiThEx
	 */
	final class CombinedTask[+A, +B, +C](taskA: Task[A], taskB: Task[B], f: (Try[A], Try[B]) => Try[C]) extends AbstractTask[C] {
		override def engage(onComplete: Try[C] => Unit): Unit = {
			var ota: Maybe[Try[A]] = Maybe.empty;
			var otb: Maybe[Try[B]] = Maybe.empty;
			taskA.engagePortal { tryA =>
				otb.fold {
					ota = Maybe.some(tryA)
				} { tryB =>
					val tryC =
						try f(tryA, tryB)
						catch {
							case NonFatal(e) => Failure(e)
						}
					onComplete(tryC)
				}
			}
			taskB.engagePortal { tryB =>
				ota.fold {
					otb = Maybe.some(tryB)
				} { tryA =>
					val tryC =
						try f(tryA, tryB)
						catch {
							case NonFatal(e) => Failure(e)
						}
					onComplete(tryC)
				}
			}
		}

		override def toString: String = deriveToString[CombinedTask[A, B, C]](this)
	}

	/** @see [[Task.sequenceToArray]] */
	final class SequenceTask[A: ClassTag, C[x] <: Iterable[x]](tasks: C[Task[A]]) extends AbstractTask[Array[A]] {
		override def engage(onComplete: Try[Array[A]] => Unit): Unit = {
			val size = tasks.size
			val array = Array.ofDim[A](size)
			if size == 0 then onComplete(Success(array))
			else {
				val taskIterator = tasks.iterator
				var completedCounter: Int = 0
				var index = 0
				while index < size do {
					val task = taskIterator.next
					val taskIndex = index
					task.engagePortal {
						case Success(a) =>
							array(taskIndex) = a
							completedCounter += 1
							if completedCounter == size then onComplete(Success(array))

						case failure: Failure[A] =>
							onComplete(failure.asInstanceOf[Failure[Array[A]]])
					}
					index += 1
				}
			}
		}
	}

	final class SequenceHardy[A: ClassTag, C[x] <: Iterable[x]](tasks: C[Task[A]]) extends AbstractTask[Array[Try[A]]] {
		override def engage(onComplete: Try[Array[Try[A]]] => Unit): Unit = {
			val size = tasks.size
			val array = Array.ofDim[Try[A]](size)
			if size == 0 then onComplete(Success(array))
			else {
				val taskIterator = tasks.iterator
				var completedCounter: Int = 0
				var index = 0
				while index < size do {
					val task = taskIterator.next
					val taskIndex = index
					task.engagePortal { tryA =>
						array(taskIndex) = tryA
						completedCounter += 1
						if completedCounter == size then onComplete(Success(array))
					}
					index += 1
				}
			}
		}
	}

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
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 * @param condition function that decides if the loop continues or not based on:
	 *		- the number of already completed cycles,
	 *		- and the result of the last execution of the `taskA`.
	 * The loop ends when this function returns a [[Maybe.some]]. Its content will be the final result of this task.
	 *
	 * $isExecutedByDoSiThEx $unhandledErrorsArePropagatedToTaskResult
	 * */
	final class ReiterateHardyUntilSome[+A, +B](taskA: Task[A], maxRecursionDepthPerExecutor: Int = 9)(condition: (Int, Try[A]) => Maybe[Try[B]]) extends AbstractTask[B] {

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
							executeSequentially(loop(completedCycles + 1, 0))
						}
					}(onComplete)
				}
			}

			loop(0, 0)
		}

		override def toString: String = deriveToString[ReiterateHardyUntilSome[A, B]](this)
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
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 * @param condition determines whether a new cycle should be performed based on the number of times it has already been evaluated and either the result of the previous cycle or `ta0` if no cycle has been done yet. $isExecutedByDoSiThEx $unhandledErrorsArePropagatedToTaskResult
	 * @tparam A the type of the result of the repeated task `taskA`.
	 * @tparam B the type of the result of this task.
	 */
	final class ReiterateHardyWhileEmpty[+A, +B](taskA: Task[A], ta0: Try[A], maxRecursionDepthPerExecutor: Int = 9)(condition: (Int, Try[A]) => Maybe[B]) extends AbstractTask[B] {
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
						else executeSequentially(loop(completedCycles + 1, newTryA, 0))
					}
				}(onComplete)
			}

			loop(0, ta0, 0)
		}

		override def toString: String = deriveToString[ReiterateHardyWhileEmpty[A, B]](this)
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
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 * @param checkAndBuild a function that takes the number of already completed cycles and the last task result wrapped in a `Try`, returning an `Either[Try[B], Task[A]]` indicating the next action to perform.	$isExecutedByDoSiThEx $unhandledErrorsArePropagatedToTaskResult *
	 */
	final class WhileRightReiterateHardy[+A, +B](tryA0: Try[A], maxRecursionDepthPerExecutor: Int = 9)(checkAndBuild: (Int, Try[A]) => Either[Try[B], Task[A]]) extends AbstractTask[B] {
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
							else executeSequentially(loop(completedCycles + 1, newTryA, 0));
						}
				}
			}

			loop(0, tryA0, 0)
		}

		override def toString: String = deriveToString[WhileRightReiterateHardy[A, B]](this)
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
	 * @param maxRecursionDepthPerExecutor $maxRecursionDepthPerExecutor
	 * @param buildAndCheck a function that takes the number of already completed cycles and the last task result, returning a new task that yields an `Either[Try[B], A]` indicating the next action to perform. $isExecutedByDoSiThEx $unhandledErrorsArePropagatedToTaskResult
	 */
	final class ReiterateUntilLeft[+A, +B](a0: A, maxRecursionDepthPerExecutor: Int = 9)(buildAndCheck: (Int, A) => Task[Either[Try[B], A]]) extends AbstractTask[B] {
		override def engage(onComplete: Try[B] => Unit): Unit = {
			/**
			 * @param executionsCounter number of already completed cycles, which consist of a task creation and its execution.
			 * @param lastTaskResult the result of the last task execution.
			 * @param recursionDepth the number of recursions that may have been performed in the current executor in the worst case scenario where all calls are synchronous. */
			def loop(executionsCounter: Int, lastTaskResult: A, recursionDepth: Int): Unit = {
				val task: Task[Either[Try[B], A]] =
					try buildAndCheck(executionsCounter, lastTaskResult)
					catch {
						case NonFatal(e) => Task.successful(Left(Failure(e)))
					}
				task.engagePortal {
					case Success(Right(a)) =>
						if (recursionDepth < maxRecursionDepthPerExecutor) loop(executionsCounter + 1, a, recursionDepth + 1)
						else executeSequentially(loop(executionsCounter + 1, a, 0));
					case Success(Left(tryB)) =>
						onComplete(tryB)
					case Failure(e) =>
						onComplete(Failure(e))
				}
			}

			loop(0, a0, 0)
		}

		override def toString: String = deriveToString[ReiterateUntilLeft[A, B]](this)
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
	final class AttemptUntilRight[+A, +B](maxRetries: Int, maxRecursionDepthPerExecutor: Int = 9)(taskBuilder: Int => Task[Either[A, B]]) extends AbstractTask[Either[A, B]] {
		override def engage(onComplete: Try[Either[A, B]] => Unit): Unit = {
			/**
			 * @param attemptsAlreadyMade the number attempts already made.
			 * @param recursionDepth the number of recursions that may have been performed in the current executor in the worst case scenario where all calls are synchronous. */
			def loop(attemptsAlreadyMade: Int, recursionDepth: Int): Unit = {
				val task: Task[Either[A, B]] =
					try taskBuilder(attemptsAlreadyMade)
					catch {
						case NonFatal(cause) => Task.failed(cause)
					}
				task.engagePortal {
					case success@Success(aOrB) =>
						aOrB match {
							case _: Right[A, B] =>
								onComplete(success)
							case Left(a) =>
								if (attemptsAlreadyMade >= maxRetries) {
									onComplete(success);
								} else if (recursionDepth < maxRecursionDepthPerExecutor) {
									loop(attemptsAlreadyMade + 1, recursionDepth + 1);
								} else {
									executeSequentially(loop(attemptsAlreadyMade + 1, 0))
								}
						}
					case failure: Failure[Either[A, B]] =>
						onComplete(failure);
				}
			}

			loop(0, 0)
		}

		override def toString: String = deriveToString[AttemptUntilRight[A, B]](this)
	}

	////////////// COMMITMENT ///////////////

	/** A [[Task]] that allows to subscribe/unsubscribe consumers of its result. */
	abstract class SubscriptableTask[+A] extends AbstractTask[A] {
		/**
		 * Subscribes a consumer of the result of this [[Task]].
		 *
		 * The subscription is automatically removed after this [[Task]] is completed and the received consumer is executed.
		 *
		 * If this [[Task]] is already completed when this method is called, the method executes the provided consumer synchronously and does not make a subscription.
		 *
		 * CAUTION: This method does not prevent duplicate subscriptions.
		 * CAUTION: Should be called within the $DoSiThEx */
		def subscribe(onComplete: Try[A] => Unit): Unit

		/**
		 * Removes a subscription made with [[subscribe]].
		 *
		 * CAUTION: Should be called within the $DoSiThEx */
		def unsubscribe(onComplete: Try[A] => Unit): Unit

		/** @return `true` if the provided consumer was already subscribed. */
		def isAlreadySubscribed(onComplete: Try[A] => Unit): Boolean
	}

	/** A commitment to complete a [[SubscriptableTask]].
	 * The [[SubscriptableTask]] this [[Commitment]] promises to complete is itself. This task's will complete when this [[Commitment]] is fulfilled or broken. That could be done immediately calling [[fulfill]], [[break]], [[complete]], or in a deferred manner by calling [[completeWith]].
	 *
	 * Analogous to [[scala.concurrent.Promise]] but for a [[Task]] instead of a [[scala.concurrent.Future]].
	 * */
	final class Commitment[A] extends SubscriptableTask[A] { thisCommitment =>
		private var oResult: Maybe[Try[A]] = Maybe.empty;
		private var firstOnCompleteObserver: (Try[A] => Unit) | Null = null
		private var onCompletedObservers: List[Try[A] => Unit] = Nil;

		/** The [[SubscriptableTask]] this [[Commitment]] promises to fulfill. */
		inline def subscriptableTask: SubscriptableTask[A] = thisCommitment

		/** CAUTION: should be called within the $DoSiThEx
		 * @return true if this [[Commitment]] was either fulfilled or broken; or false if it is still pending. */
		def isCompleted: Boolean = {
			assert(assistant.isWithinDoSiThEx)
			oResult.isDefined
		}

		/** CAUTION: should be called within the $DoSiThEx
		 * @return true if this [[Commitment]] is still pending; or false if it was completed. */
		def isPending: Boolean = {
			assert(assistant.isWithinDoSiThEx)
			oResult.isEmpty
		}

		protected override def engage(onComplete: Try[A] => Unit): Unit = subscribe(onComplete)

		override def subscribe(onComplete: Try[A] => Unit): Unit = {
			assert(assistant.isWithinDoSiThEx)
			oResult.fold {
				if firstOnCompleteObserver eq null then firstOnCompleteObserver = onComplete
				else onCompletedObservers = onComplete :: onCompletedObservers
			}(onComplete)
		}

		override def unsubscribe(onComplete: Try[A] => Unit): Unit = {
			assert(assistant.isWithinDoSiThEx)
			if firstOnCompleteObserver eq onComplete then {
				if onCompletedObservers.isEmpty then firstOnCompleteObserver = null
				else {
					firstOnCompleteObserver = onCompletedObservers.head
					onCompletedObservers = onCompletedObservers.tail
				}
			}
			else onCompletedObservers = onCompletedObservers.filterNot(_ ne onComplete)
		}

		override def isAlreadySubscribed(onComplete: Try[A] => Unit): Boolean = {
			assert(assistant.isWithinDoSiThEx)
			(firstOnCompleteObserver eq onComplete) || onCompletedObservers.exists(_ eq onComplete)
		}

		/** Provokes that the [[subscriptableTask]] that this [[Commitment]] promises to complete to be completed with the received `result`.
		 *
		 * @param result the result that the [[subscriptableTask]] this [[Commitment]] promised to complete . */
		def complete(result: Try[A], isWithinDoSiThEx: Boolean = assistant.isWithinDoSiThEx)(onAlreadyCompleted: Try[A] => Unit = _ => ()): this.type = {
			if isWithinDoSiThEx then completeHere(result)(onAlreadyCompleted)
			else executeSequentially(completeHere(result)(onAlreadyCompleted))
			thisCommitment
		}

		/** Provokes that the [[subscriptableTask]] that this [[Commitment]] promises to complete to be completed with the received `result`.
		 *
		 * Caution: Should be called within the $DoSiThEx
		 * @param result the result that the [[subscriptableTask]] this [[Commitment]] promised to complete . */
		def completeHere(result: Try[A])(onAlreadyCompleted: Try[A] => Unit = _ => ()): this.type = {
			assert(assistant.isWithinDoSiThEx)
			oResult.fold {
				oResult = Maybe.some(result);
				onCompletedObservers.foreach(_(result));
				onCompletedObservers = Nil; // unbind the observers list to help the garbage collector
				if firstOnCompleteObserver ne null then {
					firstOnCompleteObserver(result)
					firstOnCompleteObserver = null // unbind the observer reference to help the garbage collector
				}
			} { value =>
				try onAlreadyCompleted(value)
				catch {
					case NonFatal(cause) => reportFailure(cause)
				}
			}
			thisCommitment
		}


		/** Provokes that the [[subscriptableTask]] this [[Commitment]] promises to complete to be fulfilled (completed successfully) with the received `result`. */
		def fulfill(result: A, isWithinDoSiThEx: Boolean = assistant.isWithinDoSiThEx)(onAlreadyCompleted: Try[A] => Unit = _ => ()): this.type =
			if isWithinDoSiThEx then completeHere(Success(result))(onAlreadyCompleted)
			else complete(Success(result))(onAlreadyCompleted)

		/** Provokes that the [[subscriptableTask]] this [[Commitment]] promises to complete to be broken (completed with failure) with the received `cause`. */
		def break(cause: Throwable, isWithinDoSiThEx: Boolean = assistant.isWithinDoSiThEx)(onAlreadyCompleted: Try[A] => Unit = _ => ()): this.type = {
			if isWithinDoSiThEx then completeHere(Failure(cause))(onAlreadyCompleted)
			else complete(Failure(cause))(onAlreadyCompleted)
		}

		/** Programs the completion of the [[subscriptableTask]] this [[Commitment]] promises to complete to be completed with the result of the received [[Task]] when it is completed. */
		def completeWith(otherTask: Task[A], isWithinDoSiThEx: Boolean = assistant.isWithinDoSiThEx)(onAlreadyCompleted: Try[A] => Unit = _ => ()): this.type = {
			if (otherTask ne thisCommitment)
				otherTask.trigger(isWithinDoSiThEx)(result => completeHere(result)(onAlreadyCompleted));
			thisCommitment
		}
	}

	//////////////// Flow //////////////////////

	object Flow {
		def lift[A, B](f: A => B): Flow[A, B] =
			(a: A) => Duty.ready(f(a))

		def wrap[A, B](builder: A => Duty[B]): Flow[A, B] =
			(a: A) => builder(a)
	}

	trait Flow[A, B] { thisFlow =>

		protected def flush(a: A): Duty[B]

		inline def apply(a: A, inline isWithinDoSiThEx: Boolean = assistant.isWithinDoSiThEx)(onComplete: B => Unit): Unit = {
			def work(): Unit = flush(a).engagePortal(onComplete)

			if isWithinDoSiThEx then work()
			else executeSequentially(work())
		}

		/** Connects this flow output with the input of the received one. */
		def to[C](next: Flow[B, C]): Flow[A, C] =
			(a: A) => thisFlow.flush(a).flatMap(b => next.flush(b))

		/** Connects the received flow output with the input of this one. */
		def from[Z](previous: Flow[Z, A]): Flow[Z, B] =
			(z: Z) => previous.flush(z).flatMap(a => thisFlow.flush(a))
	}
}