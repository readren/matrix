package readren.sequencer

import readren.common.{Maybe, castTo, deriveToString}
import Doer.{successFalse, successTrue, successUnit}

import scala.annotation.{tailrec, threadUnsafe}
import scala.collection.{IterableFactory, mutable}
import scala.compiletime.erasedValue
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.reflect.ClassTag
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object Doer {

	/** Wraps exception passed to [[Doer.reportFailure]] when [[Doer.reportPanicException]] is called.
	 * [[Doer.reportPanicException]] is called by [[Doer.ownSingleThreadExecutionContext.reportFailure]], few [[Doer.Task]] operations like [[Doer.Task.andThen]] that can't propagate failures, and most [[Commitment]] operations. */
	class PanicException(message: String, cause: Throwable) extends RuntimeException(message, cause)

	val successUnit: Success[Unit] = Success(())
	val successTrue: Success[true] = Success(true)
	val successFalse: Success[false] = Success(false)
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
 * See [[Doer.executeSequentially()]].
 *
 * @define DoSiThEx DoSiThEx (doer's single-thread executor)
 * @define onCompleteExecutedByDoSiThEx The `onComplete` callback passed to `engage` is always, with no exception, executed by this $DoSiThEx. This is part of the contract of the [[Task]] trait.
 * @define threadSafe This method is thread-safe.
 * @define isExecutedByDoSiThEx Executed within the DoSiThEx (doer's single-thread executor).
 * @define unhandledErrorsArePropagatedToTaskResult The call to this routine is guarded with try-catch. If it throws a non-fatal exception it will be caught and the [[Task]] will complete with a [[Failure]] containing the error.
 * @define unhandledErrorsAreReported The call to this routine is guarded with a try-catch. If the evaluation throws a non-fatal exception it will be caught and reported with [[Doer.reportFailure()]].
 * @define notGuarded CAUTION: The call to this function is NOT guarded with a try-catch. If its evaluation terminates abruptly the duty will never complete. The same occurs with all routines received by [[Duty]] operations. This is one of the main differences with [[Task]] operation.
 * @define maxRecursionDepthPerExecutor Maximum recursion depth per executor. Once this limit is reached, the recursion continues in a new executor. The result does not depend on this parameter as long as no [[java.lang.StackOverflowError]] occurs.
 * @define isWithinDoSiThEx indicates whether the call to this method is within this [[Doer]]'s sequential executor. If there is no such certainty the call site should either, not specify a value in order to use the default (which is the result of [[Doer.isInSequence]]), or specify `false` to force deferred execution.
 */
trait Doer { thisDoer =>

	/** Type of the tag attached to [[Doer]] instances. */
	type Tag

	/** Convenience tag attached to this [[Doer]] instance.
	 * This tag is not necessary and even not used by [[Doer]] operations. It exists solely for convenience in tracking and debugging. */
	val tag: Tag

	/**
	 * Specifies what an instance of [[Doer]] requires to execute its operations.
	 * Executes the provided [[Runnable]] in the order of submission (after all the ones that were submitted before to this [[Doer]] instance have been completed).
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
	 * The implementation should return the [[Doer]] instance that the current [[java.lang.Thread]] is currently running if it knows it, or null if it doesn't.
	 * The implementation should know, at least, if the current [[java.lang.Thread]] corresponds to this [[Doer]], and return this instance in that case. */
	def current: Maybe[Doer]

	/**
	 * @return true if the current [[java.lang.Thread]] is the one that is currently assigned to this [[Doer]]. Calling this method within the thread with which the [[Runnable]]s passed to this instance's [[executeSequentially]] or [[execute]] methods is executed, always returns `true`. */
	inline def isInSequence: Boolean = current.exists(_ eq thisDoer)

	/** Asserts that the current [[java.lang.Thread]] is the one that is currently assigned to this [[Doer]] instance for sequential execution of its operations. */
	inline def checkWithin(): Unit = {
		assert(isInSequence, s"The current thread does not correspond to this Doer: expected=$thisDoer, current=$current, tag=$tag")
	}

	/**
	 * Called by few [[Task]] and most [[Commitment]] operations when an operand function terminates abruptly and the nature of the operation does not allow to propagate the failure to the result.
	 * Examples of such operations are [[Task.andThen]], [[Task.triggerAndForgetHandlingErrors]], [[Task_wait]], [[Task_alien]], and [[Commitment.completeHere]].
	 * The implementation should report the received [[Throwable]] somehow. Preferably including a description that identifies the provider of the DoSiThEx used by [[executeSequentially]] and mentions that the error was thrown by a deferred procedure programmed by means of a [[Task]].
	 * The implementation should not throw non-fatal exceptions.
	 * This method is called within the thread assigned to this [[Doer]].
	 * */
	protected def reportFailure(cause: Throwable): Unit

	private[sequencer] inline def reportFailurePortal(cause: Throwable): Unit = reportFailure(cause)

	/**
	 * Queues an execution of the specified procedure in the task-queue of this $DoSiThEx. See [[Doer.executeSequentially]]
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
	inline def execute(inline procedure: => Unit): Unit = {
		${ DoerMacros.executeSequentiallyImpl('thisDoer, 'procedure) }
	}

	protected inline def reportPanicException(exception: Throwable): Unit =
		${ DoerMacros.reportPanicExceptionImpl('thisDoer, 'exception) }

	/**
	 * An [[ExecutionContext]] that executes in sequence with this [[Doer]]. See [[Doer.executeSequentially]] */
	@threadUnsafe lazy val ownSingleThreadExecutionContext: ExecutionContext = new ExecutionContext {
		def execute(runnable: Runnable): Unit = thisDoer.executeSequentially(runnable)

		def reportFailure(cause: Throwable): Unit = {
			if isInSequence then thisDoer.reportPanicException(cause)
			else executeSequentially(() => thisDoer.reportPanicException(cause))
		}
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
	 * For example, if a duty `d` closes over a mutable variable from the environment that affects its execution result, the equality of two supposedly equivalent expressions like {{{t.flatMap(f).flatMap(g) == t.flatMap(a => f(a).flatMap(g))}}} could be compromised. This would depend on the timing of when the variable is mutated — specifically when the mutation occurs between the start and end of execution.
	 * This does not mean that [[Duty]] implementations (and the routines their operations receive) must avoid closing over mutable variables altogether. Rather, it highlights that if strict adherence to monadic laws is required by your business logic, you should ensure that the mutable variable is not modified during task execution.
	 * For deterministic behavior, it's sufficient that any closed-over mutable variable is only mutated and accessed by actions executed in sequence. This is why the contract centralizes execution in the $DoSiThEx: to maintain determinism, even when closing over mutable variables, provided they are mutated and accessed solely within the $DoSiThEx.
	 *
	 * Design note: [[Duty]] and [[Task]] are defined with inner [[Doer]] traits to leverage Scala's path-dependent type checking. This ensures, at compile time, that all operand functions passed to [[Duty]] or [[Task]] operations, owned by the same [[Doer]], are executed sequentially, helping detect contract violations.
	 * While path-dependent type checking is valuable for enforcing this contract, it has a drawback: the compiler's type-path checks are overly strict, requiring compatible singleton types for references, whereas we only need to verify that the [[Duty]] instances correspond to the same [[Doer]].
	 * As a result, the compiler may flag type errors in cases where the contract is not violated, which is undesirable.
	 * The [[castTypePath()]] method mitigates these false positives.
	 *
	 * CAUTION: Unlike [[Task]], [[Duty]] does NOT support failures. And unlike [[Task]], the invocation of function operands received by its operations is not guarded with a try-catch. Therefore, unlike [[Task]], any unhandled exception thrown during an execution of a [[Duty]] will break the expected flow and the duty will never complete.
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
		 * - any other non-fatal exception throw by this method must be caught and propagated to the result or reported using [[Doer.reportFailure]] if propagation is not feasible.
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

		/** The eta-conversion of the [[engage]] method. */
		@threadUnsafe private[sequencer] lazy val engageEta: (A => Unit) => Unit = engage

		/** A bridge to access the [[engage]] method from macros in [[DoerMacros]] and sibling classes. */
		private[sequencer] inline final def engagePortal(onComplete: A => Unit): Unit = engage(onComplete)

		/**
		 * Triggers an execution of this [[Duty]].
		 *
		 * @param isWithinDoSiThEx $isWithinDoSiThEx
		 * @param onComplete called when the triggered execution completes.
		 * Must not throw non-fatal exceptions: `onComplete` must either terminate normally or fatally, but never with a non-fatal exception.
		 * Note that if it terminates abruptly the `onComplete` will never be called.
		 * $isExecutedByDoSiThEx
		 */
		inline final def trigger(inline isWithinDoSiThEx: Boolean = isInSequence)(inline onComplete: A => Unit): Unit = {
			${ DoerMacros.triggerImpl('isWithinDoSiThEx, 'thisDoer, 'thisDuty, 'onComplete) }
		}

		/** Triggers an execution of this [[Task]] ignoring the result.
		 *
		 * $threadSafe
		 *
		 * @param isWithinDoSiThEx $isWithinDoSiThEx
		 */
		inline final def triggerAndForget(isWithinDoSiThEx: Boolean = isInSequence): Unit =
			trigger(isWithinDoSiThEx)(_ => {})

		/**
		 * Triggers and execution of this [[Duty]] and processes its result once it is completed for its side effects.
		 * Is equivalent to {{{trigger(isInSequence)(consumer)}}}
		 * @param consumer called with this task result when it completes. $isExecutedByDoSiThEx $notGuarded
		 */
		inline final def foreach(consumer: A => Unit): Unit = trigger(isInSequence)(consumer)

		/**
		 * Transforms this [[Duty]] by applying the given function to the result.
		 * ===Detailed behavior===
		 * Creates a [[Duty]] that, when executed, will trigger an execution of this duty and apply `f` to its result.
		 *
		 * $threadSafe
		 *
		 * @param f a function that transforms the result of this task, when it is successful.
		 *
		 * $isExecutedByDoSiThEx
		 *
		 * $notGuarded
		 */
		inline final def map[B](f: A => B): Duty[B] = new Duty_Map(thisDuty, f)

		/**
		 * Composes this [[Duty]] with a second one that is built from the result of this one.
		 * ===Detailed behavior===
		 * Creates a [[Duty]] that, when executed, it will:
		 *		- Trigger an execution of this duty and applies the `taskBBuilder` function to its result.
		 *		- Then triggers an execution of the built duty and completes with its result.
		 *
		 * $threadSafe
		 *
		 * @param f a function that receives the result of this task and returns the task to be executed next.
		 *
		 * $isExecutedByDoSiThEx
		 *
		 * $notGuarded
		 */
		inline final def flatMap[B](f: A => Duty[B]): Duty[B] = new Duty_FlatMap(thisDuty, f)

		/** Applies the side-effecting function to the result of this duty without affecting the propagated value.
		 * The result of the provided function is always ignored and therefore not propagated in any way.
		 * This method allows to enforce many callbacks to receive the same value and to be executed in the order they are chained.
		 * It's worth mentioning that the side-effecting function is executed before triggering the next duty in the chain.
		 * ===Detailed description===
		 * Returns a duty that, when executed, it will:
		 *		- trigger an execution of this duty,
		 *		- then apply the received function to this duty's result,
		 *		- and finally complete with the result of this task (ignoring the functions result).
		 *
		 * $threadSafe
		 *
		 * @param sideEffect a side-effecting function. The call to this function is wrapped in a try-catch block; however, unlike most other operators, unhandled non-fatal exceptions are not propagated to the result of the returned task. $isExecutedByDoSiThEx
		 */
		def andThen(sideEffect: A => Any): Duty[A] = new Duty_AndThen[A](thisDuty, sideEffect)

		/** Wraps this [[Duty]] into a [[Task]].
		 * Together with [[Task.toDuty]] this method allow to mix duties and task in the same chain.
		 * ===Detailed behavior===
		 * Creates a [[Task]] that, when executed, triggers an execution of this [[Duty]] and completes with its result, which will always be successful.
		 * @return a [[Task]] whose result is the result of this task wrapped inside a [[Success]]. */
		inline final def toTask: Task[A] = new Duty_ToTask(thisDuty)

		/**
		 * Triggers an execution of this [[Task]] and returns a [[Future]] containing its result wrapped inside a [[Success]].
		 *
		 * Is equivalent to {{{ transform(Success.apply).toFuture(isWithinDoSiThEx) }}}
		 *
		 * Useful when failures need to be propagated to the next for-expression (or for-binding).
		 *
		 * @param isWithinDoSiThEx $isWithinDoSiThEx
		 * @return a [[Future]] that will be completed when this [[Task]] is completed.
		 */
		def toFutureHardy(isWithinDoSiThEx: Boolean = isInSequence): Future[A] = {
			val promise = Promise[A]()
			trigger(isWithinDoSiThEx)(a => promise.success(a))
			promise.future
		}

		/**
		 * Wraps this [[Duty]] into another that belongs to another [[Doer]].
		 * Useful to chain [[Duty]]'s operations that involve different [[Doer]] instances.
		 * ===Detailed behavior===
		 * Returns a [[Duty]] that belongs to the provided [[Doer]]. When it is triggered, it will trigger this duty within this [[Doer]] and, when completed, make the returned [[Duty]] to yield the result.
		 * CAUTION: Avoid closing over the same mutable variable from two operand functions applied to [[Duty]] instances belonging to different [[Doer]]s.
		 * Remember that all function operands provided to [[Task]] methods are executed within the [[Doer]] that owns it.
		 * Therefore, calling [[trigger]] on the returned [[Duty]] will execute the `onComplete` passed to it within the `otherDoer`.
		 *
		 * $threadSafe
		 *
		 * @param otherDoer the [[Doer]] to which the returned [[Duty]] will belong.
		 */
		def onBehalfOf(otherDoer: Doer): otherDoer.Duty[A] =
			otherDoer.Duty_foreign(thisDoer)(this)

		/** Casts the singleton type of the [[Doer]] instance that owns this [[Duty]] to the singleton-type of the provided [[Doer]].
		 * This operation does nothing at runtime. It only tricks the compiler to prevent it from complaining when operating with [[Duty]]s that correspond to the same [[Doer]] instance but have different type-paths.
		 * CAUTION: Use it only if you are sure that the provided [[Doer]] instance is the one that owns this [[Duty]].
		 * Design note: It was decided to make [[Duty]] (and [[Task]]) an inner class of the [[Doer]] to take advantage of type-path checking to detect when the contract "all operand functions passed to [[Duty]] (and [[Task]]) operations owned by the same [[Doer]] are executed in sequence" might be violated, at compile time.
		 * Using type-path checking to detect contract violations is very valuable but it comes at a cost, because the type-path check done by the compiler is stricter than necessary -- it checks that the singleton type of the references involved be compatible, and we only need to check that the involved [[Duty]]s correspond to the same [[Doer]] instance.
		 * Therefore, the compiler will report type errors in situations the contract is not violated, which is not what we want.
		 * This operation ([[castTypePath()]]) is intended to handle those cases.
		 */
		def castTypePath[E <: Doer](doer: E): doer.Duty[A] = {
			assert(thisDoer eq doer)
			this.asInstanceOf[doer.Duty[A]]
		}
	}


	/** An idea on how to optimise duty chains. It works (at least compiles) but the benefit is mild because it does not avoid the creation of the uplink Duty, only skips it.
	 * For a considerable benefit, macros that analyze and replace code are necessary.
	 * TODO: delete or continue improving */
	@deprecated("Is just a draft of an idea")
	private inline def map_JustAnIdea[A, B, D <: Duty[A]](inline duty: D)(inline f: A => B): Duty[B] = {
		inline erasedValue[D] match {
			case _: Duty_Map[x, A] =>
				val upLink: Duty_Map[x, A] = duty.asInstanceOf[Duty_Map[x, A]]
				new Duty_Map[x, B](upLink.cA, f.compose(upLink.f))
			case _ =>
				new Duty_Map[A, B](duty, f)
		}
	}

	/** A [[Duty]] that yields [[Unit]].
	 * CAUTION: This @threadUnsafe lazy val does not guarantee a unique instance under concurrent access. Its use is only safe for logic that depends on the value's data, not its object identity (eq/ne). */
	@threadUnsafe lazy val Duty_unit: Duty[Unit] = Duty_ready(())

	/** A [[Duty]] that yields `true`.
	 * CAUTION: This @threadUnsafe lazy val does not guarantee a unique instance under concurrent access. Its use is only safe for logic that depends on the value's data, not its object identity (eq/ne). */
	@threadUnsafe lazy val Duty_true: Duty[true] = Duty_ready(true)

	/** A [[Duty]] that yields `false`.
	 * CAUTION: This @threadUnsafe lazy val does not guarantee a unique instance under concurrent access. Its use is only safe for logic that depends on the value's data, not its object identity (eq/ne). */
	@threadUnsafe lazy val Duty_false: Duty[false] = Duty_ready(false)

	/** Creates a [[Duty]] whose execution never ends.
	 * $threadSafe
	 *
	 * @return a [[Duty]] whose execution never ends.
	 * */
	@threadUnsafe lazy val Duty_never: Duty[Nothing] = Duty_NotEver()

	/** Creates a [[Duty]] whose result is calculated at the call site even before the duty is constructed.
	 * $threadSafe
	 *
	 * @param a the already calculated result of the returned [[Duty]]. */
	inline def Duty_ready[A](a: A): Duty[A] = new Duty_Ready(a)

	/** Creates a [[Duty]] that yields the value returned by the provided supplier.
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
	inline def Duty_mine[A](supplier: () => A): Duty[A] = new Duty_Mine(supplier)

	/** Creates a [[Duty]] that yields what the [[Duty]] created by the provided supplier yields.
	 * Is equivalent to: {{{mine(supplier).flatMap(identity)}}} but slightly more efficient
	 * ===Detailed behavior===
	 * Creates a duty that, when executed:
	 *		- evaluates the `supplier` within the $DoSiThEx;
	 *		- then triggers an execution of the returned Duty;
	 *		- finally completes with the result of executed duty.
	 *
	 * $$threadSafe
	 *
	 * @param supplier the supplier of the duty whose execution will give the result. $isExecutedByDoSiThEx $notGuarded
	 * @return the duty described in the method description.
	 */
	inline def Duty_mineFlat[A](supplier: () => Duty[A]): Duty[A] = new Duty_MineFlat(supplier)

	/** Creates a [[Duty]] that yields, in sequence with this doer, what the `foreignDuty` yields.
	 * When triggered, the `foreignDuty` is executed within the `foreignDoer`, and its result is supplied to the created [[Duty]] in sequence with this [[Doer]].
	 * Useful to start a process in a foreign [[Doer]] and access its result as if it were executed sequentially.
	 * $threadSafe
	 *
	 * @param foreignDoer the [[Doer]] to whom the `foreignTask` belongs.
	 * @return a duty that belongs to this [[Doer]] that completes when the `foreignDuty` is completed by the `foreignDoer`. */
	inline def Duty_foreign[A](foreignDoer: Doer)(foreignDuty: foreignDoer.Duty[A]): Duty[A] = {
		if foreignDoer eq thisDoer then foreignDuty.asInstanceOf[thisDoer.Duty[A]]
		else new Duty_Foreign[A](foreignDoer, foreignDuty)
	}

	/**
	 * Creates a [[Duty]] that yields the result of applying the bifunction `f` to what the provided duties yield.
	 * When executed, simultaneously triggers and execution of each duty and returns their results combined by the provided function.
	 * Given the single-thread nature of [[Doer]] this operation only has sense when the provided duties involve foreign duties/tasks or alien duties/tasks.
	 * ===Detailed behavior===
	 * Creates a new [[Duty]] that, when executed:
	 *		- triggers an execution for both: `dutyA` and `dutyB`
	 *		- when both are completed, completes with the value that results of applying the function `f` to their results.
	 *
	 * $threadSafe
	 *
	 * @param dutyA a task
	 * @param dutyB a task
	 * @param f the function that combines the results of the `taskA` and `taskB`. $isExecutedByDoSiThEx $unhandledErrorsArePropagatedToTaskResult
	 * @return the task described in the method description.
	 */
	inline def Duty_combine[A, B, C](dutyA: Duty[A], dutyB: Duty[B])(f: (A, B) => C): Duty[C] =
		new Duty_Combined(dutyA, dutyB, f)

	/**
	 * Creates a [[Duty]] that, when executed, simultaneously triggers an execution for each [[Duty]]s in the provided iterable, and completes with a collection containing their results in the same order if all are successful, or a failure if any is faulty.
	 * This overload is only convenient for very small lists. For large ones it is not efficient and also may cause stack-overflow when the task is executed.
	 * Use the other overload for large lists or other kind of iterables.
	 *
	 * $threadSafe
	 *
	 * @param duties the `Iterable` of duties that the returned task will trigger simultaneously to combine their results.
	 * @tparam A the result type of all the duties
	 * @return the duty described in the method description.
	 * */
	def Duty_sequence[A](duties: List[Duty[A]]): Duty[List[A]] = {
		@tailrec
		def loop(incompleteResult: Duty[List[A]], remainingDuties: List[Duty[A]]): Duty[List[A]] = {
			remainingDuties match {
				case Nil =>
					incompleteResult
				case head :: tail =>
					val lessIncompleteResult = Duty_combine(head, incompleteResult) { (a, as) => a :: as }
					loop(lessIncompleteResult, tail)
			}
		}

		duties.reverse match {
			case Nil => Duty_ready(Nil)
			case lastDuty :: previousDuties => loop(lastDuty.map(List(_)), previousDuties);
		}
	}

	/**
	 * Creates a [[Duty]] that, when executed, simultaneously triggers an execution for each [[Duty]]s in the received iterable, and completes with a collection containing their results in the same order.
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
	def Duty_sequence[A: ClassTag, C[x] <: Iterable[x], To[_]](factory: IterableFactory[To], duties: C[Duty[A]]): Duty[To[A]] = {
		Duty_sequenceToArray(duties).map { array =>
			val builder = factory.newBuilder[A]
			var index = 0
			while index < array.length do {
				builder.addOne(array(index))
				index += 1
			}
			builder.result()
		}
	}

	/** Like [[Duty_sequence]] but the resulting collection's higher-kinded type `To` is fixed to [[Array]]. */
	inline def Duty_sequenceToArray[A: ClassTag, C[x] <: Iterable[x]](duties: C[Duty[A]]): Duty[Array[A]] = new Duty_Sequence[A, C](duties)



	final class Duty_Map[A, B](val cA: Duty[A], val f: A => B) extends AbstractDuty[B] {
		override def engage(onComplete: B => Unit): Unit =
			cA.engagePortal { a => onComplete(f(a)) }

		override def toString: String = deriveToString[Duty_Map[A, B]](this)
	}

	final class Duty_FlatMap[A, B](cA: Duty[A], f: A => Duty[B]) extends AbstractDuty[B] {
		override def engage(onComplete: B => Unit): Unit = cA.engagePortal { a => f(a).engagePortal(onComplete) }

		override def toString: String = deriveToString[Duty_FlatMap[A, B]](this)
	}

	final class Duty_AndThen[A](dutyA: Duty[A], sideEffect: A => Any) extends AbstractDuty[A] {
		override def engage(onComplete: A => Unit): Unit =
			dutyA.engagePortal { a =>
				sideEffect(a)
				onComplete(a)
			}

		override def toString: String = deriveToString[Duty_AndThen[A]](this)
	}

	final class Duty_ToTask[A](cA: Duty[A]) extends AbstractTask[A] {
		override def engage(onComplete: Try[A] => Unit): Unit = cA.engagePortal(onComplete.compose(Success.apply))

		override def toString: String = deriveToString[Duty_ToTask[A]](this)
	}

	class Duty_NotEver extends AbstractDuty[Nothing] {
		override def engage(onComplete: Nothing => Unit): Unit = ()

		override def toString: String = "NotEver"
	}

	final class Duty_Ready[A](a: A) extends AbstractDuty[A] {
		override def engage(onComplete: A => Unit): Unit = onComplete(a)

		override def toFutureHardy(isWithinDoSiThEx: Boolean = isInSequence): Future[A] = Future.successful(a)

		override def toString: String = deriveToString[Duty_Ready[A]](this)
	}

	final class Duty_Mine[A](supplier: () => A) extends AbstractDuty[A] {
		override def engage(onComplete: A => Unit): Unit = onComplete(supplier())

		override def toString: String = deriveToString[Duty_Mine[A]](this)
	}

	final class Duty_MineFlat[A](supplier: () => Duty[A]) extends AbstractDuty[A] {
		override def engage(onComplete: A => Unit): Unit = supplier().engagePortal(onComplete)

		override def toString: String = deriveToString[Duty_MineFlat[A]](this)
	}

	final class Duty_Foreign[A](foreignDoer: Doer, foreignDuty: foreignDoer.Duty[A]) extends AbstractDuty[A] {
		override def engage(onComplete: A => Unit): Unit = foreignDuty.trigger()(a => thisDoer.execute(onComplete(a)))

		override def toString: String = deriveToString[Duty_Foreign[A]](this)
	}

	/** A [[Duty]] that, when executed:
	 *		- triggers an execution for each: `taskA` and `taskB`;
	 *		- when both are completed, completes with the result of applying the function `f` to their results.
	 *
	 * $threadSafe
	 * $onCompleteExecutedByDoSiThEx
	 */
	final class Duty_Combined[+A, +B, +C](dutyA: Duty[A], dutyB: Duty[B], f: (A, B) => C) extends AbstractDuty[C] {
		override def engage(onComplete: C => Unit): Unit = {
			var ma: Maybe[A] = Maybe.empty
			var mb: Maybe[B] = Maybe.empty
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

		override def toString: String = deriveToString[Duty_Combined[A, B, C]](this)
	}

	/** @see [[Duty_sequenceToArray]] */
	final class Duty_Sequence[A: ClassTag, C[x] <: Iterable[x]](duties: C[Duty[A]]) extends AbstractDuty[Array[A]] {
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

		/** @return `true` if the provided consumer was already subscribed still not executed. */
		def isAlreadySubscribed(onComplete: A => Unit): Boolean
	}

	/** A [[SubscriptableDuty]] whose completion is controlled externally..
	 *
	 * A [[Covenant]] is itself a [[SubscriptableDuty]], but unlike ordinary duties, its completion is not determined by internal logic.
	 * Instead, it exposes methods such as [[fulfill]] and [[fulfillWith]] to allow external code to complete it.
	 *
	 * [[Covenant]] is to [[Duty]] as [[Commitment]] is to [[Task]], and as [[scala.concurrent.Promise]] is to [[scala.concurrent.Future]]
	 * */
	final class Covenant[A] private[Doer](fixedResult: Maybe[A]) extends SubscriptableDuty[A] { thisCovenant =>
		private var oResult: Maybe[A] = fixedResult
		private var firstOnCompleteObserver: (A => Unit) | Null = null
		private var onCompletedObservers: List[A => Unit] = Nil

		def this() = this(Maybe.empty)

		inline def maybeResult: Maybe[A] = {
			assert(isInSequence)
			oResult
		}

		/** The [[SubscriptableDuty]] whose completion is controlled by this [[Covenant]].
		 *
		 * Provided to mimic containment semantics, allowing external code to treat this [[Covenant]] as if it exposed a separate [[SubscriptableDuty]] field.
		 * @return this [[Covenant]] as a [[SubscriptableDuty]]. */
		inline def subscriptableDuty: SubscriptableDuty[A] = thisCovenant

		/** CAUTION: Should be called within the $DoSiThEx
		 *  @return true if this [[Covenant]] was fulfilled; or false if it is still pending. */
		inline def isCompleted: Boolean = {
			assert(isInSequence)
			this.oResult.isDefined
		}

		/** CAUTION: Should be called within the $DoSiThEx
		 *  @return true if this [[Covenant]] is still pending; or false if it was completed. */
		inline def isPending: Boolean = {
			assert(isInSequence)
			this.oResult.isEmpty
		}

		protected override def engage(onComplete: A => Unit): Unit = subscribe(onComplete)

		override def subscribe(onComplete: A => Unit): Unit = {
			assert(isInSequence)
			oResult.fold {
				if firstOnCompleteObserver eq null then firstOnCompleteObserver = onComplete
				else onCompletedObservers = onComplete :: onCompletedObservers
			}(onComplete)
		}

		override def unsubscribe(onComplete: A => Unit): Unit = {
			assert(isInSequence)
			if firstOnCompleteObserver eq onComplete then {
				if onCompletedObservers.isEmpty then firstOnCompleteObserver = null
				else {
					firstOnCompleteObserver = onCompletedObservers.head
					onCompletedObservers = onCompletedObservers.tail
				}
			} else onCompletedObservers = onCompletedObservers.filterNot(_ ne onComplete)
		}

		override def isAlreadySubscribed(onComplete: A => Unit): Boolean = {
			assert(isInSequence)
			(firstOnCompleteObserver eq onComplete) || onCompletedObservers.exists(_ eq onComplete)
		}

		/** Fulfills this [[Covenant]] with the given `result`, unless it has already been fulfilled at the time the fulfillment is performed.
		 *
		 * Fulfillment is performed:
		 * - Synchronously (before this method returns) if `isWithinDoSiThEx` is true.
		 * - Asynchronously as soon as possible otherwise.
		 *
		 * This method delegates to [[fulfillHere]], scheduling it within this [[Doer]]'s sequential executor if not already executing within it.
		 *
		 * @param result the value to complete this [[Covenant]] with.
		 * @param isWithinDoSiThEx $isWithinDoSiThEx
		 * @param onCompleted optional callback invoked with the final result and a boolean indicating whether this [[Covenant]] was already completed.
		 *                    If `true`, the result is a previously set one; if `false`, the result is the one provided here.
		 */
		inline def fulfill(result: A, isWithinDoSiThEx: Boolean = isInSequence)(onCompleted: (A, Boolean) => Unit = (_, _) => ()): this.type = {
			if isWithinDoSiThEx then fulfillHere(result)(onCompleted)
			else {
				execute(fulfillHere(result)(onCompleted))
				this
			}
		}


		/** Fulfills this [[Covenant]] with the given `result`, unless it has already been fulfilled.
		 *
		 * This method must be called from within this [[Doer]]'s sequential executor.
		 *
		 * If this [[Covenant]] is not yet fulfilled, the provided `result` becomes its final value and is made immediately visible to all subscribers.
		 * If it is already fulfilled, the provided `result` is ignored.
		 *
		 * @param result the value to fulfill this [[Covenant]] with.
		 * @param onCompleted optional callback invoked synchronously (before this method returns) with the final result and a boolean indicating whether this [[Covenant]] was already fulfilled.
		 *                    If `true`, the result is a previously set one; if `false`, the result is the one provided here.
		 */
		def fulfillHere(result: A)(onCompleted: (A, Boolean) => Unit = (_, _) => ()): this.type = {
			oResult.fold {
				this.oResult = Maybe.some(result)
				onCompleted(result, false)
				this.onCompletedObservers.foreach(_(result))
				this.onCompletedObservers = Nil // Clean the observers list to help the garbage collector.
				if firstOnCompleteObserver ne null then {
					firstOnCompleteObserver(result)
					firstOnCompleteObserver = null // Clean the observers list to help the garbage collector.
				}
			}(previousResult => onCompleted(previousResult, true))
			this
		}

		/** Wires this [[Covenant]] to be completed with the result of a [[SubscriptableDuty]].
		 *
		 * Arranges this [[Covenant]] to be fulfilled if `fulfillingDuty` completes, unless it was fulfilled before.
		 * Always one, and only one, of the two callback is invoked:
		 *		- `onAlreadyCompleted` if this [[Covenant]] was already fulfilled when the subscription is done.
		 *		- `onCompletedLater` if this [[Covenant]] is fulfilled after the subscription is done.
		 * The subscription is synchronic if `isWithinDoSiThEx` is true, and asynchronic ASAP otherwise.
		 *
		 * @param fulfillingDuty the [[SubscriptableDuty]] whose result will be used to complete this [[Covenant]].
		 * @param isWithinDoSiThEx $isWithinDoSiThEx
		 * @param onCompletedLater invoked (within this [[Doer]]) when this [[Covenant]] is fulfilled, but only if that happens after the subscription is done. The boolean parameter tells whenever this [[Covenant]] was already completed when `fulfillingDuty` completes. `false` means the completión was due to the completion of `fulfillingDuty`. `true` means the completion was due to other means.
		 * @param onAlreadyCompleted invoked (within this [[Doer]]) if this [[Covenant]] was already fulfilled when the subscription is done. The subscription is immediate when `isWithinDoSiThEx` is true, and deferred otherwise.
		 * @throws IllegalArgumentException if `fulfillingDuty` is the same instance as this [[Covenant]].
		 */
		def fulfillWith(fulfillingDuty: SubscriptableDuty[A], isWithinDoSiThEx: Boolean = isInSequence, onCompletedLater: (A, Boolean) => Unit = (_, _) => (), onAlreadyCompleted: A => Unit = _ => ()): this.type = {
			if fulfillingDuty eq this then throw IllegalArgumentException("A Covenant can't be fulfilled with itself.")
			if isWithinDoSiThEx then fulfillHereWith(fulfillingDuty, onCompletedLater, onAlreadyCompleted)
			else execute(fulfillHereWith(fulfillingDuty, onCompletedLater, onAlreadyCompleted))
			this
		}

		private inline def fulfillHereWith(fulfillingDuty: SubscriptableDuty[A], onCompletedLater: (A, Boolean) => Unit = (_, _) => (), onAlreadyCompleted: A => Unit = _ => ()): Unit = {
			oResult.fold(
				fulfillingDuty.subscribe(result => fulfillHere(result)(onCompletedLater))
			)(onAlreadyCompleted)
		}
	}


	/** Creates an already fulfilled [[Covenant]].
	 * @param immediateResult the immediate result that this [[Covenant]] yields. */
	def Covenant_ready[A](immediateResult: A): Covenant[A] =
		Covenant(Maybe.some(immediateResult))

	/** Triggers an execution of the given [[Duty]] and returns a [[Covenant]] that will be completed with the result of the triggered execution if it completes before this [[Covenant]] is completed by other means.
	 *
	 * This method initiates an execution of the given [[Duty]] and wires its result to a newly created [[Covenant]].
	 * The returned [[Covenant]] acts as a completion handle for the execution triggered by this method, and can be used to observe or react to its result.
	 *
	 * @param duty the [[Duty]] to be triggered.
	 * @param isWithinDoSiThEx $isWithinDoSiThEx
	 * @param onFulfilled invoked when the returned [[Covenant]] is fulfilled. The boolean parameters tells whenever it was already fulfilled before the execution triggered by this method of given [[Duty]] completes or not.
	 * @return a [[Covenant]] that will be completed with the result of the execution triggered by this method.
	 */
	def Covenant_triggerAndWire[A](duty: Duty[A], isWithinDoSiThEx: Boolean = isInSequence, onFulfilled: (A, Boolean) => Unit = (_: A, _: Boolean) => ()): Covenant[A] = {
		val covenant = new Covenant[A]()
		duty.trigger(isWithinDoSiThEx)(result => covenant.fulfillHere(result)(onFulfilled))
		covenant
	}

	/** A causal fence that serializes non-failing state transitions with visibility-only semantics.
	 *
	 * Built on [[Duty]] and [[Covenant]], this fence ensures that each update observes the latest visible state, and that all updates are fulfilled in causal order.
	 * Speculative updates may be rolled back before becoming visible.
	 *
	 * All updates are non-failing: even rolled-back updates fulfill with the prior state.
	 */
	class CausalVisibilityFence[A](initialState: A) {
		private var lastCommittedCovenant: Covenant[A] = Covenant_ready(initialState)
		private var lastEnqueuedCovenant: Covenant[A] = lastCommittedCovenant

		/** Provides rollback capability for speculative updates.
		 *
		 * Used within [[advanceSpeculatively]] to discard an in-flight update before it becomes visible.
		 * Rollback is only effective if invoked before the update is committed.
		 * Rollback is always non-failing and fulfills the update with the previous state.
		 * The returned [[SubscriptableDuty]] completes with the same state that becomes visible.
		 */
		trait RollbackAccessor {
			/** Attempts to roll back the speculative update, restoring the previous visible state.
			 *
			 * The rollback is applied if "invoked" before the update it targets has completed.
			 * The quotes around "invoked" reflect that, when `isWithinDoSiThEx` is false, the rollback attempt is scheduled asynchronously and may race with the update's completion.
			 *
			 * Regardless of timing, the [[SubscriptableDuty]] originally returned by [[advanceSpeculatively]] is fulfilled:
			 * - With the previous state if rollback succeeds
			 * - With the committed state if rollback was too late
			 *
			 * The callback receives the resulting state and a flag indicating whether rollback was applied (`false`) or too late (`true`).
			 *
			 * @param isWithinDoSiThEx whether the caller is executing within the Doer's sequential executor
			 * @param onCompleted a callback invoked with the resulting state and a flag indicating whether rollback was too late
			 * @return the [[SubscriptableDuty]] originally returned by [[advanceSpeculatively]], now fulfilled with either the committed or rolled-back state
			 */
			def rollback(isWithinDoSiThEx: Boolean = isInSequence, onCompleted: (A, Boolean) => Unit = (_, _) => ()): SubscriptableDuty[A]
		}

		/** Returns a [[SubscriptableDuty]] that yields the state which will become visible when the last enqueued update in flight — at the time of this call — completes.
		 *
		 * This represents the causal anchor for the subsequent transition: the state that the next update will be causally anchored to.
		 * The returned [[SubscriptableDuty]] may or may not be fulfilled yet.
		 *
		 * @param isWithinDoSiThEx whether the caller is executing within the Doer's sequential executor.
		 * @return a [[SubscriptableDuty]] yielding the state that will become visible when the update in flight - as of this call - completes.
		 */
		def causalAnchor(isWithinDoSiThEx: Boolean = isInSequence): SubscriptableDuty[A] = {
			if isWithinDoSiThEx then lastEnqueuedCovenant
			else {
				val wrapper = Covenant[A]()
				execute(wrapper.fulfillWith(lastEnqueuedCovenant, true))
				wrapper
			}
		}


		/** The state to which the most recent step transitioned into.
		 *
		 * Rolled-back transitions yield the previous state.
		 *
		 * Must be called within this [[Doer]].
		 *
		 * @return the last transition result.
		 */
		inline def committedNow: A = lastCommittedCovenant.maybeResult.get

		/** Returns a [[SubscriptableDuty]] that yields the currently visible state.
		 *
		 * This reflects the state to which the most recent step transitioned into.
		 * Rolled-back transitions yield the previous state.
		 *
		 * @param isWithinDoSiThEx whether the caller is executing within the Doer's sequential executor
		 * @return a [[SubscriptableDuty]] yielding the currently visible state.
		 */
		def committedAsync(isWithinDoSiThEx: Boolean = isInSequence): SubscriptableDuty[A] = {
			if isWithinDoSiThEx then lastCommittedCovenant
			else {
				val wrapper = Covenant[A]()
				execute(wrapper.fulfillHere(committedNow)())
				wrapper
			}
		}

		/** Advances the state with a non-speculative update.
		 *
		 * The updater function is defined with a second parameter of type `Null` to match the internal speculative signature,
		 * allowing reuse without introducing an extra closure. Rollback is not supported in this method.
		 *
		 * @param stateUpdater a function that computes the next state from the current one; the second argument is always `null`
		 * @param isWithinDoSiThEx whether the caller is executing within the Doer's sequential executor
		 * @return a [[SubscriptableDuty]] that yields the updated state
		 */
		def advance(stateUpdater: (A, Null) => Duty[A], isWithinDoSiThEx: Boolean = isInSequence): SubscriptableDuty[A] = {
			val suAdapter = stateUpdater.asInstanceOf[(A, RollbackAccessor | Null) => Duty[A]]
			if isWithinDoSiThEx then step(suAdapter, false)
			else {
				val covenant = Covenant[A]()
				execute(covenant.fulfillWith(step(suAdapter, false), true))
				covenant
			}
		}

		/** Advances the state with a rollback-aware speculative update.
		 *
		 * The provided [[RollbackAccessor]] allows the update to be withdrawn before it becomes visible.
		 * All updates fulfill successfully, even when rolled back.
		 *
		 * @param stateUpdater a function that computes the next state from the current one, with rollback control
		 * @param isWithinDoSiThEx whether the caller is executing within the Doer's sequential executor
		 * @return a [[SubscriptableDuty]] that yields the updated or rolled-back state
		 */
		def advanceSpeculatively(stateUpdater: (A, RollbackAccessor) => Duty[A], isWithinDoSiThEx: Boolean = isInSequence): SubscriptableDuty[A] = {
			if isWithinDoSiThEx then step(stateUpdater, true)
			else {
				val covenant = Covenant[A]()
				execute(covenant.fulfillWith(step(stateUpdater, true), true))
				covenant
			}
		}

		/** Internal method that performs the actual state transition.
		 *
		 * Handles both speculative and non-speculative updates depending on the `isSpeculative` flag.
		 * The rollback accessor is instantiated only when needed to avoid unnecessary allocations.
		 */
		private def step(stateUpdater: (A, RollbackAccessor | Null) => Duty[A], isSpeculative: Boolean): SubscriptableDuty[A] = {
			val previousStepCovenant = lastEnqueuedCovenant
			val thisStepCovenant = Covenant[A]()
			lastEnqueuedCovenant = thisStepCovenant

			previousStepCovenant.subscribe { previousState =>
				val rba =
					if isSpeculative then new RollbackAccessor {
						override def rollback(isWithinDoSiThEx: Boolean = isInSequence, onCompleted: (A, Boolean) => Unit): SubscriptableDuty[A] =
							thisStepCovenant.fulfill(previousState, isWithinDoSiThEx)(onCompleted)
					} else null
				stateUpdater(previousState, rba).trigger(true) { newState =>
					lastCommittedCovenant = thisStepCovenant
					thisStepCovenant.fulfillHere(newState)()
				}
			}
			thisStepCovenant
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
	 * // TODO: Explore defining Task as `type Task[+A] = Duty[Try[A]]`. This could eliminate redundant Task implementation classes by reusing Duty's implementations, but it may cause IDE issues since Task operations would need to be defined as extension methods.  
	 * @tparam A the type of the result obtained when executing this task.
	 */
	trait Task[+A] extends Duty[Try[A]] { thisTask =>

		/** Triggers an execution of this [[Task]] and returns a [[Future]] of its result.
		 *
		 * @param isWithinDoSiThEx $isWithinDoSiThEx
		 * @return a [[Future]] that will be completed when this [[Task]] is completed.
		 */
		def toFuture(isWithinDoSiThEx: Boolean = isInSequence): Future[A] = {
			val promise = Promise[A]()
			trigger(isWithinDoSiThEx)(promise.complete)
			promise.future
		}

		/** Triggers an execution of this [[Task]] noticing faulty results.
		 *
		 * @param isWithinDoSiThEx $isWithinDoSiThEx
		 * @param errorHandler called when the triggered execution completes with a failure. $isExecutedByDoSiThEx $unhandledErrorsAreReported
		 */
		def triggerAndForgetHandlingErrors(errorHandler: Throwable => Unit, isWithinDoSiThEx: Boolean = isInSequence): Unit =
			trigger(isWithinDoSiThEx) {
				case Failure(e) =>
					try errorHandler(e) catch {
						case NonFatal(cause) => reportPanicException(cause)
					}
				case _ => ()
			}

		/**
		 * Triggers this [[Task]] and, once it is completed, processes its result for its side effects.
		 * @param consumer called with this task result when it completes, if it ever does.
		 */
		inline def consume(inline consumer: Try[A] => Unit): Unit =
			trigger(isInSequence) { tryA =>
				try consumer(tryA)
				catch {
					case NonFatal(e) => thisDoer.reportPanicException(e)
				}
			}

		/** Triggers this [[Task]] and once it is completed successfully processes its result for its side effects.
		 * WARNING: `consumer` won't be called if this task completes with a failure.
		 *
		 * @param consumer called with this task result when it completes successfully, if it ever does.
		 * */
		inline def foreach(inline consumer: A => Unit): Unit =
			consume {
				case Success(a) => consumer(a)
				case _ => ()
			}

		/**
		 * Transform this task by applying the given function to the result. Analogous to [[Future.transform]]
		 * ===Detailed description===
		 * Creates a [[Task]] that, when executed, triggers an execution of this task and applies the received `resultTransformer` to its result. If the evaluation finishes:
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
		inline final def transform[B](resultTransformer: Try[A] => Try[B]): Task[B] = new Task_Transform(thisTask, resultTransformer)


		/**
		 * Composes this [[Task]] with a second one that is built from the result of this one. Analogous to [[Future.transformWith]].
		 * ===Detailed behavior===
		 * Creates a [[Task]] that, when executed, it will:
		 *		- Trigger an execution of this task and apply the `taskBBuilder` function to its result. If the evaluation finishes:
		 *			- abruptly, completes with the cause.
		 *			- normally with `taskB`, triggers an execution of `taskB` and completes with its result.
		 *
		 * $threadSafe
		 *
		 * @param taskBBuilder a function that receives the result of `taskA` and builds the task to be executed next. $isExecutedByDoSiThEx $unhandledErrorsArePropagatedToTaskResult
		 */
		inline final def transformWith[B](taskBBuilder: Try[A] => Task[B]): Task[B] =
			new Task_TransformWith(thisTask, taskBBuilder)


		/**
		 * Transforms this task by applying the given function to the result if it is successful. Analogous to [[Future.map]].
		 * See [[recover]] and [[toDuty]] if you want to transform the failures; and [[transform]] if you want to transform both, successful and failed ones.
		 * ===Detailed behavior===
		 * Creates a [[Task]] that, when executed, it will trigger an execution of this task and, if the result is:
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
		 *		- Trigger an execution of this task and if the result is:
		 *			- `Failure(e)`, completes with that failure.
		 *			- `Success(a)`, applies the `taskBBuilder` function to `a`. If the evaluation finishes:
		 *				- abruptly, completes with the cause.
		 *				- normally with `taskB`, triggers an execution of `taskB` and completes with its result.
		 *
		 * $threadSafe
		 *
		 * @param taskBBuilder a function that receives the result of `taskA`, when it is a [[Success]], and returns the task to be executed next. $isExecutedByDoSiThEx $unhandledErrorsArePropagatedToTaskResult
		 */
		final def flatMap[B](taskBBuilder: A => Task[B]): Task[B] = transformWith {
			case Success(a) => taskBBuilder(a);
			case fail@Failure(_) => Task_ready(fail.castTo[B]);
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
		inline final def withFilter(predicate: A => Boolean): Task[A] = new Task_WithFilter(thisTask, predicate)

		/** Applies the side-effecting function to the result of this task without affecting the propagated value.
		 * The result of the provided function is always ignored and therefore not propagated in any way.
		 * This method allows to enforce many callbacks to receive the same value and to be executed in the order they are chained.
		 * It's worth mentioning that the side-effecting function is executed before triggering the next duty in the chain.
		 * ===Detailed description===
		 * Returns a task that, when executed:
		 *		- first executes this task;
		 *		- second applies the received function to the result and, if the evaluation finishes:
		 *			- normally, completes with the result of this task.
		 *			- abruptly with a non-fatal exception, reports the failure cause to [[Doer.reportFailure]] and completes with the result of this task.
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
					case NonFatal(e) => reportPanicException(e)
				}
				tryA
			}
		}

		/**
		 * Wraps this [[Task]] into a [[Duty]] applying the given function to transform failure results into successful ones. This is like [[map]] but for the throwable; and like [[recover]] but with a complete function.
		 * Together with [[Duty.toTask]] this method allow to mix duties and task in the same chain.
		 *
		 * @param exceptionHandler the complete function to apply to the result of this task if it is a [[Failure]]. $isExecutedByDoSiThEx */
		inline final def toDuty[B >: A](exceptionHandler: Throwable => B): Duty[B] = new Task_ToDuty[A, B](thisTask, exceptionHandler)

		/** @return a [[Duty]] that yields the result of this [[Task]]. */
		final def toDutyHardy: Duty[Try[A]] = new AbstractDuty[Try[A]] {
			override protected def engage(onComplete: Try[A] => Unit): Unit =
				thisTask.engage(onComplete)
		}

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
		 *			- normally returning a [[Task]], triggers an execution of said task and completes with its same result.
		 *
		 * $threadSafe
		 *
		 * @param pf the [[PartialFunction]] to apply to the result of this task, if it is a [[Failure]], to build the second task. $isExecutedByDoSiThEx $unhandledErrorsArePropagatedToTaskResult
		 */
		final def recoverWith[B >: A](pf: PartialFunction[Throwable, Task[B]]): Task[B] = {
			transformWith[B] {
				case Failure(t) => pf.applyOrElse(t, (e: Throwable) => new Task_Ready(Failure(e)));
				case sa@Success(_) => new Task_Ready(sa);
			}
		}

		/**
		 * Wraps this [[Task]] into another that belongs to other [[Doer]].
		 * Useful to chain [[Task]]'s operations that involve different [[Doer]] instances.
		 * ===Detailed behavior===
		 * Returns a [[Task]] that belongs to the provided [[Doer]]. When it is triggered, it will trigger this [[Task]] within this [[Doer]] and, when completed, make the returned [[Task]] to yield the result.
		 * CAUTION: Avoid closing over the same mutable variable from two transformations applied to Task instances belonging to different [[Doer]]s.
		 * Remember that all routines (e.g., functions, procedures, predicates, and callbacks) provided to [[Task]] methods are executed by the $DoSiThEx of the [[Doer]] that owns the [[Task]] instance on which the method is called.
		 * Therefore, calling [[trigger]] on the returned task will execute the `onComplete` passed to it within the $DoSiThEx of the `otherDoer`.
		 *
		 * $threadSafe
		 *
		 * @param otherDoer the [[Doer]] to which the returned [[Task]] will belong.
		 * */
		override def onBehalfOf(otherDoer: Doer): otherDoer.Task[A] =
			otherDoer.Task_foreign(thisDoer)(this)

		/** Casts the singleton type of the [[Doer]] instance that owns this [[Task]] to the singleton-type of the received [[Doer]].
		 * This operation does nothing at runtime. It only tricks the compiler to prevent it from complaining when operating with [[Task]]s that correspond to the same [[Doer]] instance but have different type-paths.
		 * CAUTION: Use it only if you are sure that the provided [[Doer]] instance is the one that owns this [[Task]].
		 * Design note: It was decided to make [[Task]] (and [[Duty]]) an inner class of the [[Doer]] to take advantage of type-path checking to detect when the contract "all operand functions passed to [[Task]] (and [[Duty]]) operations owned by the same [[Doer]] are executed in sequence" might be violated, at compile time.
		 * Using type-path checking to detect contract violations is very valuable but it comes at a cost, because the type-path check done by the compiler is stricter than necessary -- it checks that the singleton type of the references involved be compatible, and we only need to check that the involved [[Task]]s correspond to the same [[Doer]] instance.
		 * Therefore, the compiler will report type errors in situations the contract is not violated, which is not what we want.
		 * This operation ([[castTypePath()]]) is intended to handle those cases.
		 */
		override def castTypePath[E <: Doer](doer: E): doer.Task[A] = {
			assert(thisDoer eq doer)
			this.asInstanceOf[doer.Task[A]]
		}
	}

	/** An always successful ready [[Task]] that yields [[Unit]].
	 * Equivalent to {{{Task_successful[Unit](())}}}
	 *
	 * CAUTION: This @threadUnsafe lazy val does not guarantee a unique instance under concurrent access. Its use is only safe for logic that depends on the value's data, not its object identity (eq/ne). */
	@threadUnsafe lazy val Task_unit: Task[Unit] = Task_Ready(successUnit)

	/** An always successful ready [[Task]] that yields [[true]].
	 * Equivalent to {{{Task_successful[true](true}}}
	 *
	 * CAUTION: This @threadUnsafe lazy val does not guarantee a unique instance under concurrent access. Its use is only safe for logic that depends on the value's data, not its object identity (eq/ne). */
	@threadUnsafe lazy val Task_true: Task[true] = Task_Ready(successTrue)

	/** An always successful ready [[Task]] that yields [[false]].
	 * Equivalent to {{{Task_successful[false](false)}}}
	 *
	 * CAUTION: This @threadUnsafe lazy val does not guarantee a unique instance under concurrent access. Its use is only safe for logic that depends on the value's data, not its object identity (eq/ne). */
	@threadUnsafe lazy val Task_false: Task[false] = Task_Ready(successFalse)

	/** A [[Task]] whose execution never ends. */
	@threadUnsafe lazy val Task_never: Task[Nothing] = Task_Never()

	/** Creates a [[Task]] whose result is calculated at the call site even before the task is constructed. The result of its execution is always the provided value.
	 *
	 * $threadSafe
	 *
	 * @param tryA the value that the returned task will give as result every time it is executed.
	 * @return the task described in the method description.
	 */
	inline final def Task_ready[A](tryA: Try[A]): Task[A] = new Task_Ready(tryA)

	/** Creates a ready [[Task]] that always succeeds with a result that is calculated at the call site even before the task is constructed. The result of its execution is always a [[Success]] with the provided value.
	 *
	 * $threadSafe
	 *
	 * @param a the value contained in the [[Success]] that the returned task will give as result every time it is executed.
	 * @return the task described in the method description.
	 */
	inline final def Task_successful[A](a: A): Task[A] = new Task_Ready(Success(a))

	/** Creates a [[Task]] that always fails with a result that is calculated at the call site even before the task is constructed. The result of its execution is always a [[Failure]] with the provided [[Throwable]]
	 *
	 * $threadSafe
	 *
	 * @param throwable the exception contained in the [[Failure]] that the returned task will give as result every time it is executed.
	 * @return the task described in the method description.
	 */
	inline final def Task_failed[A](throwable: Throwable): Task[A] = new Task_Ready(Failure(throwable))

	/** Transforms a [[Duty]] to a [[Task]] */
	def Task_fromDuty[A](duty: Duty[Try[A]]): Task[A] =
		(onComplete: Try[A] => Unit) => duty.engagePortal(onComplete)
		
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
	inline final def Task_own[A](supplier: () => Try[A]): Task[A] = new Task_Own(supplier)

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
	inline final def Task_mine[A](supplier: () => A): Task[A] = new Task_Own(() => Success(supplier()))

	/**
	 * Creates a task whose result is the result of the task returned by the provided supplier.
	 * Is equivalent to: {{{own(supplier).flatMap(identity)}}} but slightly more efficient.
	 * ===Detailed behavior===
	 * Creates a task that, when executed, evaluates the `supplier` within the $DoSiThEx. If the evaluation finishes:
	 *		- abruptly, completes with a [[Failure]] with the cause.
	 *		- normally, triggers an execution of the returned task and completes with its result.
	 *
	 * $$threadSafe
	 *
	 * @param supplier the supplier of the result. $isExecutedByDoSiThEx $unhandledErrorsArePropagatedToTaskResult
	 * @return the task described in the method description.
	 */
	inline def Task_ownFlat[A](supplier: () => Task[A]): Task[A] = new Task_OwnFlat(supplier)

	/** Create a [[Task]] whose result will be the result of the provided [[Future]] when it completes.
	 * Useful to access the result of a process that was already started in an alien executor as if it were executed sequentially.
	 *
	 * $threadSafe
	 *
	 * @param future the future to wait for.
	 * @return the task described in the method description.
	 */
	inline final def Task_wait[A](future: Future[A]): Task[A] = new Task_Wait(future)

	/** Creates a [[Task]] whose result will be the result of the [[Future]] returned by the provided supplier.
	 * Useful to start a process in an alien executor and access its result as if it were executed sequentially.
	 * The alien executor may be the $DoSiThEx of this [[Doer]].
	 *
	 * $threadSafe
	 *
	 * @param supplier a function that starts the process and return a [[Future]] of its result. $isExecutedByDoSiThEx $unhandledErrorsArePropagatedToTaskResult
	 * @return the task described in the method description.
	 */
	inline final def Task_alien[A](supplier: () => Future[A]): Task[A] = new Task_Alien(supplier)

	/**
	 * Creates a [[Task]] that, when executed, triggers an execution of the `foreignTask` (a task that belongs to another [[Doer]]) within that [[Doer]]'s $DoSiThEx; and completes with the result within this [[Doer]]'s DoSiThEx.
	 * Useful to start a process in a foreign [[Doer]] and access its result as if it were executed sequentially.
	 *
	 * $threadSafe
	 *
	 * @param foreignDoer the [[Doer]] to whom the `foreignTask` belongs.
	 * @return a task that belongs to this [[Doer]] and completes when the `foreignTask` is completed by the `foreignDoer`. */
	inline final def Task_foreign[A](foreignDoer: Doer)(foreignTask: foreignDoer.Task[A]): Task[A] = {
		if foreignDoer eq thisDoer then foreignTask.asInstanceOf[thisDoer.Task[A]]
		else new Task_Foreign(foreignDoer, foreignTask)
	}

	/**
	 * Creates a [[Task]] that simultaneously triggers an execution for each of two tasks and returns their results combined with the received function.
	 * Given the single thread nature of [[Doer]] this operation only has sense when the received tasks are a chain of actions that involve timers, foreign, or alien tasks.
	 * ===Detailed behavior===
	 * Creates a new [[Task]] that, when executed:
	 *		- triggers an execution of each: `taskA` and `taskB`
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
	inline final def Task_combine[A, B, C](taskA: Task[A], taskB: Task[B])(f: (Try[A], Try[B]) => Try[C]): Task[C] =
		new Task_Combined(taskA, taskB, f)

	/**
	 * Creates a task that, when executed, simultaneously triggers an execution for each [[Task]]s in the received list, and completes with a list containing their results in the same order.
	 * This overload only accepts [[List]]s and is only convenient when the list is small. For large ones it is not efficient and also may cause stack-overflow when the task is executed.
	 * Use the other overload for large lists or other kind of iterables.
	 *
	 * $threadSafe
	 *
	 * @param tasks the list of [[Task]]s that the returned task will trigger simultaneously to combine their results.
	 * @return the task described in the method description.
	 *
	 * */
	final def Task_sequence[A](tasks: List[Task[A]]): Task[List[A]] = {
		@tailrec
		def loop(incompleteResult: Task[List[A]], remainingTasks: List[Task[A]]): Task[List[A]] = {
			remainingTasks match {
				case Nil =>
					incompleteResult
				case head :: tail =>
					val lessIncompleteResult = Task_combine(incompleteResult, head) { (tla, ta) =>
						for {
							la <- tla
							a <- ta
						} yield a :: la
					}
					loop(lessIncompleteResult, tail)
			}
		}

		tasks.reverse match {
			case Nil => Task_successful(Nil)
			case lastTask :: previousTasks => loop(lastTask.map(List(_)), previousTasks);
		}
	}

	/**
	 * Creates a task that, when executed, simultaneously triggers and execution for each [[Task]]s in the received list, and completes with a list containing their results in the same order if all are successful, or a Failure if anyone is faulty.
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
	def Task_sequence[A: ClassTag, C[x] <: Iterable[x], To[x] <: Iterable[x]](factory: IterableFactory[To], tasks: C[Task[A]]): Task[To[A]] = {
		Task_sequenceToArray(tasks).map { array =>
			val builder = factory.newBuilder[A]
			var index = 0
			while index < array.length do {
				builder.addOne(array(index))
				index += 1
			}
			builder.result()
		}
	}

	/** Like [[Task_sequence]] but the resulting collection's higher-kinded type `To` is fixed to [[Array]]. */
	inline def Task_sequenceToArray[A: ClassTag, C[x] <: Iterable[x]](tasks: C[Task[A]]): Task[Array[A]] = new Task_Sequence[A, C](tasks)


	/**
	 * Creates a [[Duty]] that, when executed, simultaneously triggers an execution for each [[Task]]s in the received list, and completes with a list containing their results, successful or not, in the same order.
	 *
	 * $threadSafe
	 * TODO change return type to [[Duty]] to better expose the fact that always yields a successful result
	 *
	 * @param tasks the `Iterable` of tasks that the returned task will trigger simultaneously to combine their results.
	 * @param factory the [[IterableFactory]] needed to build the [[Iterable]] that will contain the results. Note that most [[Iterable]] implementations' companion objects are an [[IterableFactory]].
	 * @tparam A the result type of all the tasks.
	 * @tparam C the higher-kinded type of the `Iterable` of tasks.
	 * @tparam To the type of the `Iterable` that will contain the results.
	 * @return the successful duty described in the method description.
	 * */
	def Duty_sequenceTasks[A: ClassTag, C[x] <: Iterable[x], To[x] <: Iterable[x]](factory: IterableFactory[To], tasks: C[Task[A]]): Duty[To[Try[A]]] = {
		Duty_sequenceTasksToArray(tasks).map { array =>
			val builder = factory.newBuilder[Try[A]]
			var index = 0
			while index < array.length do {
				builder.addOne(array(index))
				index += 1
			}
			builder.result()
		}
	}

	/** Like [[Duty_sequenceTasks]] but the resulting collection's higher-kinded type `To` is fixed to [[Array]].
	 * TODO change return type to [[Duty]] to better expose the fact that always yields a successful result
	 * */
	inline def Duty_sequenceTasksToArray[A: ClassTag, C[x] <: Iterable[x]](tasks: C[Task[A]]): Duty[Array[Try[A]]] =
		new Task_SequenceHardy[A, C](tasks)

	/** A [[Task]] that never completes.
	 *
	 * $onCompleteExecutedByDoSiThEx
	 * */
	final class Task_Never extends AbstractTask[Nothing] {
		override def engage(onComplete: Try[Nothing] => Unit): Unit = ()
	}

	/**
	 * @param taskA the [[Task]] from where this duty takes its result.
	 * @param exceptionHandler converts the faulty results of the `taskA` to instances of `B`.
	 *
	 * $notGuarded */
	final class Task_ToDuty[A, B >: A](taskA: Task[A], exceptionHandler: Throwable => B) extends AbstractDuty[B] {
		override def engage(onComplete: B => Unit): Unit = {
			taskA.engagePortal { tryA =>
				val b = tryA match {
					case Success(a) => a
					case Failure(exception) => exceptionHandler(exception)
				}
				onComplete(b)
			}
		}

		override def toString: String = deriveToString[Task_ToDuty[A, B]](this)
	}


	/** A [[Task]] whose result is calculated at the call site even before the task is constructed. The result of its execution is always the provided value.
	 *
	 * $threadSafe
	 *
	 * @param tryA the value that the returned task will give as result every time it is executed.
	 * @return the task described in the method description.
	 * $onCompleteExecutedByDoSiThEx
	 */
	final class Task_Ready[+A](tryA: Try[A]) extends AbstractTask[A] {
		override def engage(onComplete: Try[A] => Unit): Unit =
			onComplete(tryA)

		override def toFuture(isWithinDoSiThEx: Boolean = isInSequence): Future[A] =
			Future.fromTry(tryA)

		override def toFutureHardy(isWithinDoSiThEx: Boolean = isInSequence): Future[Try[A]] =
			Future.successful(tryA)

		override def toString: String = deriveToString[Task_Ready[A]](this)
	}

	/** A [[Task]] that, when executed, calls the `supplier` within the $DoSiThEx and if it finishes:
	 *  - abruptly, completes with the cause.
	 *  - normally, completes with the result.
	 *
	 * $onCompleteExecutedByDoSiThEx
	 *
	 * @param supplier builds the result of this [[Task]]. $isExecutedByDoSiThEx $unhandledErrorsArePropagatedToTaskResult
	 */
	final class Task_Own[+A](supplier: () => Try[A]) extends AbstractTask[A] {
		override def engage(onComplete: Try[A] => Unit): Unit = {
			val result =
				try supplier()
				catch {
					case NonFatal(e) => Failure(e)
				}
			onComplete(result)
		}

		override def toString: String = deriveToString[Task_Own[A]](this)
	}

	final class Task_OwnFlat[+A](supplier: () => Task[A]) extends AbstractTask[A] {
		override def engage(onComplete: Try[A] => Unit): Unit = {
			val taskA =
				try supplier()
				catch {
					case NonFatal(e) => Task_failed(e)
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
	final class Task_Wait[+A](future: Future[A]) extends AbstractTask[A] {
		override def engage(onComplete: Try[A] => Unit): Unit = {
			// Note that passing the `onComplete` operand directly to the `future.onComplete` method would break the error management contract: "exceptions thrown by the `onComplete` operand passed to `engage` should not be caught".
			future.onComplete { tryA =>
				thisDoer.execute(onComplete(tryA))
			}(using ownSingleThreadExecutionContext)
		}

		override def toString: String = deriveToString[Task_Wait[A]](this)
	}

	/** A [[Task]] that yields the resul of [[Future]] produced by a given builder.
	 * The builder is executed within this [[Doer]] but the process that completes the produced [[Future]] may run in any thread.
	 * The process executor may be the $DoSiThEx but if that is the intention [[Task_Own]] would be more appropriate.
	 *
	 * $onCompleteExecutedByDoSiThEx
	 *
	 * @param builder a function that starts the process and return a [[Future]] of its result. $isExecutedByDoSiThEx $unhandledErrorsArePropagatedToTaskResult
	 * */
	final class Task_Alien[+A](builder: () => Future[A]) extends AbstractTask[A] {
		override def engage(onComplete: Try[A] => Unit): Unit = {
			val future =
				try builder()
				catch {
					case NonFatal(e) => Future.failed(e)
				}
			// Note that passing the `onComplete` operand directly to the `future.onComplete` method would break the error management contract: "exceptions thrown by the `onComplete` operand passed to `engage` should not be caught".
			future.onComplete { tryA =>
				thisDoer.execute(onComplete(tryA))
			}(using ownSingleThreadExecutionContext)
		}

		override def toString: String = deriveToString[Task_Alien[A]](this)
	}

	final class Task_Foreign[+A](foreignDoer: Doer, foreignTask: foreignDoer.Task[A]) extends AbstractTask[A] {
		override def engage(onComplete: Try[A] => Unit): Unit = {
			foreignTask.trigger(false) { tryA => execute(onComplete(tryA)) }
		}

		override def toString: String = deriveToString[Task_Foreign[A]](this)
	}

	/**
	 * A [[Task]] that consumes the results of the received task for its side effects.
	 * ===Detailed behavior===
	 * A [[Task]] that, when executed, it will:
	 *		- trigger an execution of `taskA` and tries to apply the `consumer` to its result. If the evaluation finishes:
	 *			- abruptly, completes with a [[Failure]] containing the cause.
	 *			- normally, completes with `Success(())`.
	 */
	final class Task_Consume[A](taskA: Task[A], consumer: Try[A] => Unit) extends AbstractTask[Unit] {
		override def engage(onComplete: Try[Unit] => Unit): Unit = {
			taskA.engagePortal { tryA =>
				val tryConsumerResult =
					try {
						consumer(tryA)
						successUnit
					}
					catch {
						case NonFatal(cause) => Failure(cause)
					}
				onComplete(tryConsumerResult)
			}
		}

		override def toString: String = deriveToString[Task_Consume[A]](this)
	}

	/**
	 * A [[Task]] that transforms the received task by converting successful results to `Failure(new NoSuchElementException())` if it does not satisfy the received `predicate`.
	 * Needed to support filtering and case matching in for-compressions. The for-expressions (or for-bindings) after the filter are not executed if the [[predicate]] is not satisfied.
	 * ===Detailed behavior===
	 * A [[Task]] that, when executed, it will:
	 *		- trigger an execution of `taskA` and, if the result is a:
	 *			- [[Failure]], completes with that failure.
	 *			- [[Success]], tries to apply the `predicate` to its content. If the evaluation finishes:
	 *				- abruptly, completes with the cause.
	 *				- normally with a `false`, completes with a [[Failure]] containing a [[NoSuchElementException]].
	 *				- normally with a `true`, completes with the result of this task.
	 *
	 * $onCompleteExecutedByDoSiThEx
	 *
	 * */
	final class Task_WithFilter[A](taskA: Task[A], predicate: A => Boolean) extends AbstractTask[A] {
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

		override def toString: String = deriveToString[Task_WithFilter[A]](this)
	}


	/**
	 * A [[Task]] that transform the result of another task.
	 * ===Detailed description===
	 * A [[Task]] that, when executed, triggers an execution of the `originalTask` and applies the received `resultTransformer` to its results. If the evaluation finishes:
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
	final class Task_Transform[+A, +B](originalTask: Task[A], resultTransformer: Try[A] => Try[B]) extends AbstractTask[B] {
		override def engage(onComplete: Try[B] => Unit): Unit = {
			originalTask.engagePortal { tryA =>
				val tryB =
					try resultTransformer(tryA)
					catch {
						case NonFatal(e) => Failure(e);
					}
				onComplete(tryB)
			}
		}

		override def toString: String = deriveToString[Task_Transform[A, B]](this)
	}

	/**
	 * A [[Task]] that composes two [[Task]]s where the second is build from the result of the first.
	 * ===Detailed behavior===
	 * A [[Task]] which, when executed, it will:
	 *		- Trigger an execution of `taskA` and apply the `taskBBuilder` function to its result. If the evaluation finishes:
	 *			- abruptly, completes with the cause.
	 *			- normally with `taskB`, triggers an execution of `taskB` and completes with its result.
	 *
	 * $threadSafe
	 *
	 * @param taskA the task that is executed first.
	 * @param taskBBuilder a function that receives the result of an execution of `taskA` and builds the task to be executed next. $isExecutedByDoSiThEx $unhandledErrorsArePropagatedToTaskResult
	 */
	final class Task_TransformWith[+A, +B](taskA: Task[A], taskBBuilder: Try[A] => Task[B]) extends AbstractTask[B] {
		override def engage(onComplete: Try[B] => Unit): Unit = {
			taskA.engagePortal { tryA =>
				val taskB =
					try taskBBuilder(tryA)
					catch {
						case NonFatal(e) => Task_failed(e)
					}
				taskB.engagePortal(onComplete)
			}
		}

		override def toString: String = deriveToString[Task_TransformWith[A, B]](this)
	}


	/** A [[Task]] that, when executed:
	 *		- triggers an execution for each: `taskA` and `taskB`;
	 *		- when both are completed, whether normal or abruptly, the function `f` is applied to their results and if the evaluation finishes:
	 *			- abruptly, completes with the cause.
	 *			- normally, completes with the result.
	 *
	 * $threadSafe
	 * $onCompleteExecutedByDoSiThEx
	 */
	final class Task_Combined[+A, +B, +C](taskA: Task[A], taskB: Task[B], f: (Try[A], Try[B]) => Try[C]) extends AbstractTask[C] {
		override def engage(onComplete: Try[C] => Unit): Unit = {
			var ota: Maybe[Try[A]] = Maybe.empty
			var otb: Maybe[Try[B]] = Maybe.empty
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

		override def toString: String = deriveToString[Task_Combined[A, B, C]](this)
	}

	/** @see [[Task_sequenceToArray]] */
	final class Task_Sequence[A: ClassTag, C[x] <: Iterable[x]](tasks: C[Task[A]]) extends AbstractTask[Array[A]] {
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

	final class Task_SequenceHardy[A: ClassTag, C[x] <: Iterable[x]](tasks: C[Task[A]]) extends AbstractDuty[Array[Try[A]]] {
		override def engage(onComplete: Array[Try[A]] => Unit): Unit = {
			val size = tasks.size
			val array = Array.ofDim[Try[A]](size)
			if size == 0 then onComplete(array)
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
						if completedCounter == size then onComplete(array)
					}
					index += 1
				}
			}
		}
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

		/** @return `true` if the provided consumer was already subscribed and still not executed. */
		def isAlreadySubscribed(onComplete: Try[A] => Unit): Boolean
	}

	/** A [[SubscriptableTask]] whose completion is controlled externally.
	 *
	 * A [[Commitment]] is itself a [[SubscriptableTask]], but unlike ordinary tasks, its completion is not determined by internal logic.
	 * Instead, it exposes methods such as [[complete]], [[fulfill]], [[break]], and [[completeWith]] to allow external code to complete it.
	 *
	 * Analogous to [[scala.concurrent.Promise]] but for [[Task]]s instead of a [[scala.concurrent.Future]]s.
	 */
	final class Commitment[A] private[Doer](fixedResult: Maybe[Try[A]]) extends SubscriptableTask[A] { thisCommitment =>
		private var oResult: Maybe[Try[A]] = fixedResult
		private var firstOnCompleteObserver: (Try[A] => Unit) | Null = null
		private var onCompletedObservers: List[Try[A] => Unit] = Nil

		def this() = this(Maybe.empty)

		inline def maybeResult: Maybe[Try[A]] = {
			assert(isInSequence)
			oResult
		}

		/** The [[SubscriptableTask]] whose completion is controlled by this [[Commitment]].
		 *
		 * Provided to mimic containment semantics, allowing external code to treat this [[Commitment]] as if it exposed a separate [[SubscriptableTask]] field.
		 * @return this [[Commitment]] as a [[SubscriptableTask]] */
		inline def subscriptableTask: SubscriptableTask[A] = thisCommitment

		/** CAUTION: should be called within the $DoSiThEx
		 * @return true if this [[Commitment]] was either fulfilled or broken; or false if it is still pending. */
		inline def isCompleted: Boolean = {
			assert(isInSequence)
			oResult.isDefined
		}

		/** CAUTION: should be called within the $DoSiThEx
		 * @return true if this [[Commitment]] is still pending; or false if it was completed. */
		inline def isPending: Boolean = {
			assert(isInSequence)
			oResult.isEmpty
		}

		protected override def engage(onComplete: Try[A] => Unit): Unit =
			subscribe(onComplete)

		override def subscribe(onComplete: Try[A] => Unit): Unit = {
			assert(isInSequence)
			oResult.fold {
				if firstOnCompleteObserver eq null then firstOnCompleteObserver = onComplete
				else onCompletedObservers = onComplete :: onCompletedObservers
			}(onComplete)
		}

		override def unsubscribe(onComplete: Try[A] => Unit): Unit = {
			assert(isInSequence)
			if firstOnCompleteObserver eq onComplete then {
				if onCompletedObservers.isEmpty then firstOnCompleteObserver = null
				else {
					firstOnCompleteObserver = onCompletedObservers.head
					onCompletedObservers = onCompletedObservers.tail
				}
			}
			else onCompletedObservers = onCompletedObservers.filterNot(_ eq onComplete)
		}

		override def isAlreadySubscribed(onComplete: Try[A] => Unit): Boolean = {
			assert(isInSequence)
			(firstOnCompleteObserver eq onComplete) || onCompletedObservers.exists(_ eq onComplete)
		}

		/** Completes this [[Commitment]] with the given `result`, unless it has already been completed at the time the completion is performed.
		 *
		 * Completion is performed:
		 * - Synchronously (before this method returns) if `isWithinDoSiThEx` is true.
		 * - Asynchronously as soon as possible otherwise.
		 *
		 * This method delegates to [[completeHere]], scheduling it within this [[Doer]]'s sequential executor if not already executing within it.
		 *
		 * @param result the value to complete this [[Commitment]] with.
		 * @param isWithinDoSiThEx $isWithinDoSiThEx
		 * @param onCompleted optional callback invoked with the final result and a boolean indicating whether this [[Commitment]] was already completed.
		 *                    If `true`, the result is a previously set one; if `false`, the result is the one provided here.
		 */
		inline def complete(result: Try[A], isWithinDoSiThEx: Boolean = isInSequence)(onCompleted: (Try[A], Boolean) => Unit = (_, _) => ()): this.type = {
			if isWithinDoSiThEx then completeHere(result)(onCompleted)
			else {
				execute(completeHere(result)(onCompleted))
				thisCommitment
			}
		}

		/** Completes this [[Commitment]] with the given `result`, unless it has already been completed.
		 *
		 * This method must be called from within this [[Doer]]'s sequential executor.
		 *
		 * If this [[Commitment]] is not yet completed, the provided `result` becomes its final value and is made immediately visible to all subscribers.
		 * If it is already completed, the provided `result` is ignored.
		 *
		 * @param result the value to complete this [[Commitment]] with.
		 * @param onCompleted optional callback invoked synchronously (before this method returns) with the final result and a boolean indicating whether this [[Commitment]] was already completed.
		 *                    If `true`, the result is a previously set one; if `false`, the result is the one provided here.
		 */
		def completeHere(result: Try[A])(onCompleted: (Try[A], Boolean) => Unit = (_, _) => ()): this.type = {
			assert(isInSequence)
			oResult.fold {
				oResult = Maybe.some(result)
				onCompleted(result, false)
				onCompletedObservers.foreach(_(result))
				onCompletedObservers = Nil // unbind the observers list to help the garbage collector
				if firstOnCompleteObserver ne null then {
					firstOnCompleteObserver(result)
					firstOnCompleteObserver = null // unbind the observer reference to help the garbage collector
				}
			} { value =>
				try onCompleted(value, true)
				catch {
					case NonFatal(cause) => reportPanicException(cause)
				}
			}
			thisCommitment
		}

		/** Fulfills this [[Commitment]] with the given `result`, unless it has already been completed at the time the fulfillment is performed.
		 *
		 * Fulfillment is performed:
		 * - Synchronously (before this method returns) if `isWithinDoSiThEx` is true.
		 * - Asynchronously as soon as possible otherwise.
		 *
		 * This method delegates to [[completeHere]], scheduling it within this [[Doer]]'s sequential executor if not already executing within it.
		 *
		 * @param result the value to complete this [[Commitment]] with.
		 * @param isWithinDoSiThEx $isWithinDoSiThEx
		 * @param onCompleted optional callback invoked with the final result and a boolean indicating whether this [[Commitment]] was already completed.
		 *                    If `true`, the result is a previously set one; if `false`, the result is the one provided here.
		 */
		inline def fulfill(result: A, isWithinDoSiThEx: Boolean = isInSequence)(onCompleted: (Try[A], Boolean) => Unit = (_, _) => ()): this.type = {
			if isWithinDoSiThEx then completeHere(Success(result))(onCompleted)
			else {
				execute(completeHere(Success(result))(onCompleted))
				this
			}
		}

		/** Breaks this [[Commitment]] with the given `excuse`, unless it has already been completed at the time the fulfillment is performed.
		 *
		 * Fulfillment is performed:
		 * - Synchronously (before this method returns) if `isWithinDoSiThEx` is true.
		 * - Asynchronously as soon as possible otherwise.
		 *
		 * This method delegates to [[completeHere]], scheduling it within this [[Doer]]'s sequential executor if not already executing within it.
		 *
		 * @param excuse the [[Failure]] to break this [[Commitment]] with.
		 * @param isWithinDoSiThEx $isWithinDoSiThEx
		 * @param onCompleted optional callback invoked with the final result and a boolean indicating whether this [[Commitment]] was already completed.
		 *                    If `true`, the result is a previously set one; if `false`, the result is the one provided here.
		 */
		inline def break(excuse: Throwable, isWithinDoSiThEx: Boolean = isInSequence)(onCompleted: (Try[A], Boolean) => Unit = (_, _) => ()): this.type = {
			if isWithinDoSiThEx then completeHere(Failure(excuse))(onCompleted)
			else {
				execute(completeHere(Failure(excuse))(onCompleted))
				this
			}
		}

		/** Wires this [[Commitment]] to the completion of a [[SubscriptableTask]].
		 *
		 * Arranges this [[Commitment]] to be completed if `completingTask` completes, unless it was completed before.
		 * Always one, and only one, of the two callback is invoked:
		 *		- `onAlreadyCompleted` if this [[Commitment]] was already completed when the subscription is done.
		 *		- `onCompletedLater` if this [[Commitment]] is completed after the subscription is done.
		 * The subscription is synchronic if `isWithinDoSiThEx` is true, and asynchronic ASAP otherwise.
		 *
		 * @param completingTask the [[SubscriptableTask]] whose result will be used to complete this [[Commitment]].
		 * @param isWithinDoSiThEx informs if this method was called within this [[Doer]].
		 * @param onCompletedLater invoked (within this [[Doer]]) when this [[Commitment]] is completed, but only if that happens after the subscription is done. The boolean parameter tells whenever this [[Commitment]] was already completed when `completingTask` completes. `false` means the completión was due to the completion of `completingTask`. `true` means the completion was due to other means.
		 * @param onAlreadyCompleted invoked (within this [[Doer]]) if this [[Commitment]] was already fulfilled when the subscription is done. The subscription is immediate when `isWithinDoSiThEx` is true, and deferred otherwise.
		 * @throws IllegalArgumentException if `completingTask` is the same instance as this [[Commitment]].
		 */
		def completeWith(completingTask: SubscriptableTask[A], isWithinDoSiThEx: Boolean = isInSequence, onCompletedLater: (Try[A], Boolean) => Unit = (_, _) => (), onAlreadyCompleted: Try[A] => Unit = _ => ()): this.type = {
			if completingTask eq this then throw IllegalArgumentException("A Commitment can't be fulfilled with itself.")
			if isWithinDoSiThEx then completeHereWith(completingTask, onCompletedLater, onAlreadyCompleted)
			else execute(completeHereWith(completingTask, onCompletedLater, onAlreadyCompleted))
			this
		}

		private inline def completeHereWith(completingTask: SubscriptableTask[A], onCompletedLater: (Try[A], Boolean) => Unit = (_, _) => (), onAlreadyCompleted: Try[A] => Unit = _ => ()): Unit = {
			oResult.fold(
				completingTask.subscribe(result => completeHere(result)(onCompletedLater))
			)(onAlreadyCompleted)
		}
	}

	/** Creates a [[Commitment]] that is already completed.
	 * @param immediateResult the immediate result that this [[Commitment]] yields. */
	def Commitment_ready[A](immediateResult: Try[A]): Commitment[A] =
		Commitment(Maybe.some(immediateResult))

	/** Triggers the given [[Task]] and returns a [[Commitment]] that will be completed with the result of the triggered execution unless this [[Commitment]] is completed before by other means.
	 *
	 * This method triggers an execution of the given [[Task]] and wires its result to a newly created [[Commitment]].
	 * The returned [[Commitment]] acts as a completion handle for the execution triggered by this method, and can be used to observe or react to its result.
	 *
	 * @param task the [[Task]] to be triggered.
	 * @param isWithinDoSiThEx true if triggering occurs within the current [[Doer]] sequence.
	 * @param onCompleted invoked when the returned [[Commitment]] is completed. The boolean parameters tells whenever it was already completed before the execution triggered by this method of the given [[Task]] completes or not.
	 * @return a [[Commitment]] that will be completed with the result of the execution triggered by this method.
	 */
	def Commitment_triggerAndWire[A](task: Task[A], isWithinDoSiThEx: Boolean = isInSequence, onCompleted: (Try[A], Boolean) => Unit = (_: Try[A], _: Boolean) => ()): Commitment[A] = {
		val commitment = new Commitment[A]()
		task.trigger(isWithinDoSiThEx)(result => commitment.completeHere(result)(onCompleted))
		commitment
	}


	/** A causal durability fence that serializes state transitions with rollback-aware semantics and durability guarantees.
	 *
	 * Each call to [[advance]] or [[advanceSpeculatively]] attempts a state transition causally anchored to the previous one — regardless of whether that step succeeded or failed.
	 * Transitions are executed sequentially and complete the [[SubscriptableTask]] returned by the corresponding call.
	 * If the previous step failed, the new transition is skipped and the corresponding [[SubscriptableTask]] is completed with the same failure.
	 * Only successful transitions update the committed state.
	 *
	 * This fence ensures:
	 *   - Causal sequencing of state transitions
	 *   - Durability semantics via [[Task]] and [[Commitment]]
	 *   - Speculative updates with rollback capability
	 *   - Deterministic fulfillment of the [[SubscriptableTask]] returned by each call
	 *   - Visibility is only advanced on successful completion
	 *
	 * @param initialState the initial state, already visible and committed
	 */
	class CausalDurabilityFence[A](initialState: Try[A]) {
		private var lastCommittedCommitment: Commitment[A] = Commitment_ready(initialState)
		private var lastEnqueuedCommitment: Commitment[A] = lastCommittedCommitment

		/** Provides rollback capability for speculative updates.
		 *
		 * A [[RollbackAccessor]] is passed to speculative state transitions to allow them to cancel themselves before becoming visible.
		 * Rollback is only effective if invoked before the update is committed.
		 * If rollback is invoked too late, the accessor still complete the [[SubscriptableTask]] with the committed state
		 * and signals that rollback was ineffective.
		 */
		trait RollbackAccessor {
			/** Attempts to roll back the speculative update, restoring the previous visible state.
			 *
			 * The rollback is applied if "invoked" before the update it targets has completed.
			 * The quotes around "invoked" reflect that, when `isWithinDoSiThEx` is false, the rollback attempt is scheduled asynchronously and may race with the update's completion.
			 *
			 * Regardless of timing, the [[SubscriptableTask]] originally returned by [[advanceSpeculatively]] is fulfilled:
			 * - With the previous state if rollback succeeds
			 * - With the committed state if rollback was too late
			 *
			 * The callback receives the resulting state and a flag indicating whether rollback was applied (`false`) or too late (`true`).
			 *
			 * @param isWithinDoSiThEx whether the caller is executing within the Doer's sequential executor
			 * @param onCompleted a callback invoked with the resulting state and a flag indicating whether rollback was too late
			 * @return the [[SubscriptableTask]] originally returned by [[advanceSpeculatively]], now fulfilled with either the committed or rolled-back state
			 */
			def rollback(isWithinDoSiThEx: Boolean = isInSequence, onCompleted: (Try[A], Boolean) => Unit = (_, _) => ()): SubscriptableTask[A]
		}

		/** Returns a [[SubscriptableTask]] that yields the state which will become visible when the last enqueued update in flight — at the time of this call — completes.
		 *
		 * This represents the causal anchor for the subsequent transition: the state that the next update will be causally anchored to.
		 * The returned [[SubscriptableTask]] may or may not be fulfilled yet.
		 *
		 * @param isWithinDoSiThEx whether the caller is executing within the Doer's sequential executor
		 * @return a [[SubscriptableTask]] yielding the state that will become visible when the update in flight at the time of this call completes
		 */
		def causalAnchor(isWithinDoSiThEx: Boolean = isInSequence): SubscriptableTask[A] = {
			if isWithinDoSiThEx then lastEnqueuedCommitment
			else {
				val wrapper = Commitment[A]()
				execute(wrapper.completeWith(lastEnqueuedCommitment, true))
				wrapper
			}
		}

		/** The state to which the most recent step transitioned into, or a [[Failure]] if the transition failed.
		 *
		 * Rolled-back transitions yield the previous state.
		 *
		 * Must be called within this [[Doer]].
		 *
		 * @return the last transition result
		 */
		inline def committedNow: Try[A] = lastCommittedCommitment.maybeResult.get

		/** Returns a [[SubscriptableTask]] that yields the currently visible state or failure.
		 *
		 * This reflects the state to which the most recent step transitioned into, or a [[Failure]] if the transition failed.
		 * Rolled-back transitions yield the previous state.
		 *
		 * @param isWithinDoSiThEx whether the caller is executing within the Doer's sequential executor
		 * @return a [[SubscriptableTask]] yielding the currently visible state or failure.
		 */
		def committedAsync(isWithinDoSiThEx: Boolean = isInSequence): SubscriptableTask[A] = {
			if isWithinDoSiThEx then lastCommittedCommitment
			else {
				val wrapper = Commitment[A]()
				execute(wrapper.completeWith(lastCommittedCommitment, true))
				wrapper
			}
		}

		/** Attempts a deterministic state transition anchored to the previous enqueued step.
		 *
		 * If the previous step failed, this transition is skipped and the [[SubscriptableTask]] corresponding to this call is completed with the same failure. Only successful transitions update the committed state.
		 *
		 * @param stateUpdater a function that computes the next state from the current one
		 * @param isWithinDoSiThEx whether the caller is executing within the Doer's sequential executor
		 * @return a [[SubscriptableTask]] that will be fulfilled with the new state once the update completes, or the previous failure due to which the update was skipped.
		 */
		def advance(stateUpdater: (A, Null) => Task[A], isWithinDoSiThEx: Boolean = isInSequence): SubscriptableTask[A] = {
			val suAdapter = stateUpdater.asInstanceOf[(A, RollbackAccessor | Null) => Task[A]]
			if isWithinDoSiThEx then step(suAdapter, false)
			else {
				val commitment = Commitment[A]()
				execute(commitment.completeWith(step(suAdapter, false), true))
				commitment
			}
		}


		/** Attempts a speculative state transition anchored to the previous one.
		 *
		 * If the previous step failed, this transition is skipped and the returned [[SubscriptableTask]] is completed with the same failure.
		 * If rollback is invoked before visibility, the update is canceled and the previous state is kept.
		 * Only successful transitions update the committed state.
		 *
		 * @param stateUpdater a function that computes the next state from the current one, with rollback capability
		 * @param isWithinDoSiThEx whether the caller is executing within the Doer's sequential executor
		 * @return a [[SubscriptableTask]] that will be completed with the new state if not rolled-back in time, the previous state if rolled-back in time, or the previous failure due to which the update was skipped.
		 */
		def advanceSpeculatively(stateUpdater: (A, RollbackAccessor) => Task[A], isWithinDoSiThEx: Boolean = isInSequence): SubscriptableTask[A] = {
			if isWithinDoSiThEx then step(stateUpdater, true)
			else {
				val covenant = Commitment[A]()
				execute(covenant.completeWith(step(stateUpdater, true), true))
				covenant
			}
		}

		/** Internal method that performs the actual state transition.
		 *
		 * Handles both deterministic and speculative updates depending on the `isSpeculative` flag.
		 * If the previous step failed, the update is not executed and the [[SubscriptableTask]] corresponding to this step
		 * is completed with the same failure. The rollback accessor is instantiated only when needed to avoid unnecessary allocations.
		 * Only successful transitions update the committed state.
		 *
		 * @param stateUpdater the transition function, optionally accepting a [[RollbackAccessor]]
		 * @param isSpeculative whether the update is speculative and may be rolled back
		 * @return a [[SubscriptableTask]] that will be completed with the new state if not rolled-back in time, the previous state if rolled-back in time, or the previous failure due to which the update was skipped.
		 */
		private def step(stateUpdater: (A, RollbackAccessor | Null) => Task[A], isSpeculative: Boolean): SubscriptableTask[A] = {
			val previousStepCommitment = lastEnqueuedCommitment
			val thisStepCommitment = Commitment[A]()
			lastEnqueuedCommitment = thisStepCommitment

			previousStepCommitment.subscribe {
				case Success(previousState) =>
					val rba =
						if isSpeculative then new RollbackAccessor {
							override def rollback(isWithinDoSiThEx: Boolean = isInSequence, onCompleted: (Try[A], Boolean) => Unit): SubscriptableTask[A] =
								thisStepCommitment.fulfill(previousState, isWithinDoSiThEx)(onCompleted)
						} else null
					stateUpdater(previousState, rba).trigger(true) { thisStepResult =>
						lastCommittedCommitment = thisStepCommitment
						thisStepCommitment.completeHere(thisStepResult)()
					}
				case Failure(e) =>
					thisStepCommitment.break(e, true)()
			}
			thisStepCommitment
		}
	}

	//////////////// Flow //////////////////////

	def Flow_lift[A, B](f: A => B): Flow[A, B] =
		(a: A) => Duty_ready(f(a))

	def Flow_wrap[A, B](builder: A => Duty[B]): Flow[A, B] =
		(a: A) => builder(a)

	trait Flow[A, B] { thisFlow =>

		protected def flush(a: A): Duty[B]

		inline def apply(a: A, inline isWithinDoSiThEx: Boolean = isInSequence)(onComplete: B => Unit): Unit = {
			def work(): Unit = flush(a).engagePortal(onComplete)

			if isWithinDoSiThEx then work()
			else execute(work())
		}

		/** Connects this flow output with the input of the received one. */
		def to[C](next: Flow[B, C]): Flow[A, C] =
			(a: A) => thisFlow.flush(a).flatMap(b => next.flush(b))

		/** Connects the received flow output with the input of this one. */
		def from[Z](previous: Flow[Z, A]): Flow[Z, B] =
			(z: Z) => previous.flush(z).flatMap(a => thisFlow.flush(a))
	}
}