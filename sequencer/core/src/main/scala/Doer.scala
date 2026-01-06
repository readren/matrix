package readren.sequencer

import Doer.*

import readren.common.{Maybe, castTo, deriveToString}

import scala.annotation.{tailrec, threadUnsafe}
import scala.collection.{IterableFactory, mutable}
import scala.compiletime.erasedValue
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.reflect.ClassTag
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object Doer {

	val assertionsEnabled: Boolean = classOf[Doer].desiredAssertionStatus()

	/** Wraps exception passed to [[Doer.reportFailure]] when [[Doer.reportPanicException]] is called.
	 * [[Doer.reportPanicException]] is called by [[Doer.ownSingleThreadExecutionContext.reportFailure]], few [[Doer.Task]] operations like [[Doer.Task.andThen]] that can't propagate failures, and most [[Commitment]] operations. */
	class PanicException(message: String, cause: Throwable) extends RuntimeException(message, cause)

	/** Information about the responsible for the completion and origin of the value with which a [[Covenant]]/[[Commitment]] is completed:
	 *		- [[THE_PROVIDED]] if completed by the invoked completion method with the provided value.
	 *		- [[ANOTHER_BEFORE]] if completed by other means before the completion method was invoked.
	 *		- [[ANOTHER_AFTER]] if completed by other menas after the completion method was invoked.
	 * TODO when a new version of scala is released (newer than 3.7.4), check if it supports making these types aliases opaque without causing obscure errors in unrelated code like the [[LoopingExtension]] despite it does not reference them. */
	type ResultOrigin = Int
	/** Completed by something else after the [[Doer.Covenant.fulfillWith]]/[[Doer.Commitment.completeWith]] was invoked. */
	inline val ANOTHER_AFTER = 0
	/** Information about the responsible for the completion and origin of the value with which a [[Covenant]]/[[Commitment]] is completed with an immediate value.
	 *		- [[THE_PROVIDED]] if completed by the invoked completion method with the provided value.
	 *		- [[ANOTHER_BEFORE]] if completed by other means before the completion method was invoked.
	 * TODO when a new version of scala is released (newer than 3.7.4), check if it supports making these types aliases opaque without causing obscure errors in unrelated code like the [[LoopingExtension]] despite it does not reference them. */
	type ImmediateResultOrigin = ResultOrigin
	/** Completed by something else before the [[Doer.Covenant]]/[[Doer.Commitment]] completion method was invoked. */
	final inline val ANOTHER_BEFORE = 1
	/** Completed by the [[Doer.Covenant]]/[[Doer.Commitment]] completion method was invoked. */
	final inline val THE_PROVIDED = 2

	/** Information about the application of a rollback.
	 *		- [[ROLLBACK_APPLIED]] if the rollback was applied.
	 *		- [[ROLLBACK_IGNORED]] if the rollback was ignored because it was attempted to late. */
	type RollbackApplication = Int
	final inline val ROLLBACK_APPLIED = THE_PROVIDED
	final inline val ROLLBACK_IGNORED = ANOTHER_BEFORE

	/** Informs about the timing of the arrival to an anchored link of a causal chain:
	 *		- [[ARRIVED_BEFORE]] the transition corresponding to the link completed before the anchoring.
	 *		- [[ARRIVED_AFTER]] the transition corresponding to the link completed after the anchoring.
	 * */
	type CausalAnchorArrival = Int
	final inline val ARRIVED_BEFORE = ANOTHER_BEFORE
	final inline val ARRIVED_AFTER = ANOTHER_AFTER

	//// Convenient constants ////

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
	 * TODO add a [[Doer]] field that exposes the serial number of the current invocation of [[executeSequentially]]. This will allow users to notice whether something is executed in the same or other to [[executeSequentially]] invocation.
	 * */
	def executeSequentially(runnable: Runnable): Unit


	/**
	 * The implementation should return the [[Doer]] instance that the current [[java.lang.Thread]] is currently running if it knows it, or null if it doesn't.
	 * The implementation should know, at least, if the current [[java.lang.Thread]] corresponds to this [[Doer]], and return this instance in that case. */
	def current: Maybe[Doer]

	/**
	 * @return true if the current [[java.lang.Thread]] is the one that is currently assigned to this [[Doer]]. Calling this method within the thread with which the [[Runnable]]s passed to this instance's [[executeSequentially]] or [[execute]] methods is executed, always returns `true`. */
	inline def isInSequence: Boolean = current.value eq thisDoer

	/** Asserts that the current [[java.lang.Thread]] is the one that is currently assigned to this [[Doer]] instance for sequential execution of its operations. */
	inline def checkWithin(): Unit = {
		if Doer.assertionsEnabled && !isInSequence then throw new AssertionError(checkWithinMsg())
	}

	final def checkWithinMsg(): String = s"The current thread does not correspond to this Doer: expected=${thisDoer.tag}, current=${current.fold("unknown")(_.tag)}."

	/**
	 * Called by few [[Task]] and most [[Commitment]] operations when an operand function terminates abruptly and the nature of the operation does not allow to propagate the failure to the result.
	 * Examples of such operations are [[Task.andThen]], [[Task.triggerAndForgetHandlingErrors]], [[Task_wait]], [[Task_alien]], and [[Commitment.completeUnsafe]].
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
	 * A producer of a result whose executions are serialized with all the executions of this and other instances of [[Duty]]/[[Task]] that belong to this [[Doer]].
	 * The order of execution is the same as the order in which the executions were triggered (see [[trigger]]), provided they are triggered within the same [[Doer]] or [[Thread]].
	 *
	 * A [[Duty]] can encapsulate one or more chained actions and provides operations to declaratively build complex duties from simpler ones.
	 * This tool simplifies the implementation of a handler that manages multiple simultaneous processes that interact with each other using a single sequential actor. How? By eliminating the need for state variables that determine the decision-making flow, as the code structure itself indicates the execution order.
	 *
	 * Instances of [[Duty]] whose result is always the same follow the monadic laws. However, if the result depends on the execution (because it depends on mutable variables or time), these laws may be broken.
	 * For example, if the [[Duty.engage]] implementation closes over mutable variables (either directly or through any of the function operands that its factory or the operations used to construct it receives) from the environment that affects its execution result, then the equality of two supposedly equivalent expressions like {{{duty.flatMap(f).flatMap(g) == duty.flatMap(a => f(a).flatMap(g))}}} could be compromised. This would depend on the timing of when the variables are mutated — specifically when the mutations occur between the start and end of the duty's execution.
	 * This does not mean that [[Duty.engage]] implementations must avoid closing over mutable variables altogether. Rather, it highlights that if strict adherence to monadic laws is required by your business logic, you should ensure that the mutable variable is not modified during the execution of the involved [[Duty]] instances.
	 * If the goal is just deterministic behavior, it's sufficient that any closed-over mutable variable is only mutated and accessed by actions executed sequentially in a determined order. This is why the contract enforces serialized execution of actions in the order at which the actions were triggered: to maintain determinism, even when closing over mutable variables, provided they are mutated and accessed solely within the actions in said ordered sequence and those actions are deterministic.
	 *
	 * If you require to ensure monadic laws are followed, use [[LatchedDuty]]/[[LatchedTask]] instead.
	 *
	 * Design note: [[Duty]] and [[Task]] are defined as inner traits of [[Doer]] to leverage Scala's path-dependent type checking. This avoids that [[Duty]]/[[Task]] instances that belong to different [[Doer]] instances to be inadvertently composed together without the adapters needed to ensure sequential execution of the component actions.
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
		 * This method performs the actions represented by the duty and calls `onComplete` within the $DoSiThEx when the task finishes.
		 *
		 * The implementation may assume this method is invoked within the $DoSiThEx.
		 *
		 * The implementation must respect the following exception-handling rules:
		 * - no exception thrown by the provided callback must be caught.
		 * - any non-fatal exception throw by this method must be either caught and propagated to the result or reported using [[Doer.reportFailure]] if propagation is not feasible.
		 * In the case of [[Task]] this includes non-fatal exceptions originated in function operands passed to its factory, including those captured over a closure.
		 * [[Duty]], on the other hand, assumes that function operands never throw exceptions. If an exception is thrown, the stack of the corresponding duty execution will be completely unwound.
		 * It is crucial to ensure that exceptions thrown by the onComplete callback are not caught, as this could suppress issues within the callback, preventing the execution of code expected to run and making it extremely difficult to diagnose the cause of a never-completing [[Duty]] or [[Task]].
		 *
		 * This method is the sole primitive operation of this trait; all other methods are derived from it.
		 *
		 * @param onComplete The callback that must be invoked upon the completion of this task. The implementation should call this callback within the $DoSiThEx.
		 *
		 * The implementation may assume that `onComplete` will either terminate normally or fatally, but will not throw non-fatal exceptions.
		 */
		protected def engage(onComplete: A => Unit): Unit

		/** The eta-conversion of the [[engage]] method. TODO: replace all eta-expansion of `engage` with this function. */
		@threadUnsafe private[sequencer] lazy val engageEta: (A => Unit) => Unit = engage

		/** A bridge to access the [[engage]] method from macros in [[DoerMacros]] and sibling classes. */
		private[sequencer] inline final def engagePortal(onComplete: A => Unit): Unit = engage(onComplete)

		/**
		 * Initiates an execution of this [[Duty]]/[[Task]] and subscribes the provided call-back as a consumer of the execution result.
		 * Each invocation of this method triggers a new execution.
		 * Note: Executions triggered on [[Duty]] instances whose completion depends on other executions (e.g., a pending [[Covenant]]) will not complete until those dependent executions have themselves been triggered and completed.
		 * @param isWithinDoSiThEx $isWithinDoSiThEx
		 * @param onComplete Invoked when the triggered execution completes. The call-back must not throw non-fatal exceptions. It must either terminate normally or fail fatally, but never with a non-fatal exception.
		 * $isExecutedByDoSiThEx
		 */
		inline final def trigger(inline isWithinDoSiThEx: Boolean = isInSequence)(inline onComplete: A => Unit): Unit = {
			${ DoerMacros.triggerImpl('isWithinDoSiThEx, 'thisDoer, 'thisDuty, 'onComplete) }
		}

		/** Triggers an execution of this [[Duty]] ignoring the result.
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
		def foreach(consumer: A => Unit): Unit = trigger(isInSequence)(consumer)

		/**
		 * Transforms this [[Duty]] by applying the provided function to the result of this [[Duty]].
		 * ===Detailed behavior===
		 * Creates a [[Duty]] that yields the result of applying the provided function to the results of this [[Duty]].
		 *
		 * $threadSafe
		 *
		 * @param f a function that transforms the result of this [[Duty]].
		 *
		 * $isExecutedByDoSiThEx
		 *
		 * $notGuarded
		 */
		def map[B](f: A => B): Duty[B] = new Duty_Map(thisDuty, f)

		/**
		 * Transforms this [[Duty]] by applying the provided function to the result of this [[Duty]] and then executing the [[Duty]] returned by said function.
		 * ===Detailed behavior===
		 * Creates a [[Duty]] that, when executed, it will:
		 *		- Execute this [[Duty]] and apply the provided function to its result.
		 *		- Then executes the [[Duty]] built in the previous step by the provided funcion and completes with its result.
		 *
		 * $threadSafe
		 *
		 * @param f a function that is applied to the result of this [[Duty]] execution, to build a [[Duty]] that is executed next to produce the result that the [[Duty]] returned by this method yields.
		 *
		 * $isExecutedByDoSiThEx
		 *
		 * $notGuarded
		 */
		def flatMap[B](f: A => Duty[B]): Duty[B] = new Duty_FlatMap(thisDuty, f)

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
	inline def Duty_ready[A](a: A): Duty[A] = new ReadyDuty(a)

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

	/** Creates a [[Duty]] that triggers the execution of the provided [[Duty]] by another [[Doer]], and yields its result.
	 * When triggered, the `foreignDuty` is executed within the `foreignDoer` (in sequence with whatever the `foreignDoer` is doing), and its result is supplied by the created [[Duty]] in sequence with this [[Doer]].
	 * Useful to start a process in a another [[Doer]] and access its result sequentially.
	 * $threadSafe
	 *
	 * @param foreignDoer the [[Doer]] to whom the `foreignDuty` belongs.
	 * @param foreignDuty the [[Duty]] to be executed by the `foreignDoer`. Its result will be yielded by the returned [[Duty]] in sequence with this [[Doer]].
	 * @return a [[Duty]] that produces what the `foreignDuty` produces, but the result is yielded in sequence with this [[Doer]]. */
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
			object vars {
				var aIsCompleted: Boolean = false
				var bIsCompleted: Boolean = false
				var maybeA: AnyRef | Null = null
				var maybeB: AnyRef | Null = null
			}
			dutyA.engagePortal { a =>
				if vars.bIsCompleted then onComplete(f(a, vars.maybeB.asInstanceOf[B]))
				else {
					vars.aIsCompleted = true
					vars.maybeA = a.asInstanceOf[AnyRef]
				}
			}
			dutyB.engagePortal { b =>
				if vars.aIsCompleted then onComplete(f(vars.maybeA.asInstanceOf[A], b))
				else {
					vars.bIsCompleted = true
					vars.maybeB = b.asInstanceOf[AnyRef]
				}
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

	////////////// LATCHED DUTY ///////////////

	/**
	 * A [[Duty]] whose result, once completion occurs, is latched exactly once and subsequently delivered deterministically to present and future subscribers.
	 * Specifically, a [[Duty]] that:
	 *		- Is completed a single time and caches the result so that, once completed, subscribing a consumer executes the call-back immediately. Note that linking a down-chain subscribes the first link as consumer.
	 * 		- The monadic laws are always upheld. Before completion, they can’t be observed because no result exists yet; after completion, they can be observed in the cached result.
	 *		- Allows to subscribe/unsubscribe consumers of its completion result dynamically.
	 *		- The source of determination may be intrinsic from the start (e.g. {{{ Covenant[String]().fulfillWith(anIntrinsicallyDeterminedDuty) }}}) or external (e.g. {{{ Covenant[String]().fulfill(someValueDeterminedExternally) }}}); the concrete result value is realized only at completion.
	 * The timing and outcome of completion are not specified by this class. That behavior is delegated to subclasses; see [[Covenant]].
	 * @note Triggering (calling [[trigger]]) on a pending [[LatchedDuty]] does not trigger the execution of the subscribed consumers, but just subscribes the `onComplete` call-back passed to [[trigger]] as a consumer of the future result.
	 * // TODO make [[LatchedDuty]] be a covariant trait, add a concrete covariant subclass named ReadyDuty, and move the fields and implementations that depend on them to [[Covenant]]. If I am right, doing that will allow to implement [[map]] and [[flatMap]] such that they don't require to be called within the [[Doer]].
	 * */
	sealed abstract class LatchedDuty[+A] extends AbstractDuty[A] {

		def maybeResult: Maybe[A]

		override protected def engage(onComplete: A => Unit): Unit =
			subscribe(onComplete)

		/**
		 * Subscribes a consumer of the result of this [[LatchedDuty]].
		 *
		 * The subscription is automatically removed after and execution of this [[LatchedDuty]] has completed and the received consumer is executed.
		 *
		 * If this [[LatchedDuty]] is already fulfilled when this method is called, the provided consumer is invoked synchronously and no subscription occurs.
		 * Otherwise, the provided consumer is schedule to run upon completion in subscription orden (after sequentially running all the previously subscribed result consumers).
		 *
		 *
		 * @note CAUTION: This method does not prevent duplicate subscriptions.
		 * @note CAUTION: Must be called within the $DoSiThEx */
		def subscribe(consumer: A => Unit): Unit

		/**
		 * Removes a subscription done with [[subscribe]].
		 *
		 * @note CAUTION: Must be called within the $DoSiThEx */
		def unsubscribe(onComplete: A => Unit): Unit

		/** @note CAUTION: Must be called within the $DoSiThEx
		 *  @return true if this [[LatchedDuty]] was fulfilled; or false if it is still pending. */
		def isCompleted: Boolean

		/** @note CAUTION: Must be called within the $DoSiThEx
		 *  @return true if this [[LatchedDuty]] is still pending; or false if it was completed. */
		def isPending: Boolean

		inline def intoDuty: Duty[A] = this

		/**
		 * Transforms this [[LatchedDuty]] by applying the given function to the result of this [[LatchedDuty]].
		 * ===Detailed behavior===
		 * Creates a [[LatchedDuty]] that yields the result of applying the provided function to the results of this [[LatchedDuty]].
		 * @note CAUTION: Must be called within the $DoSiThEx
		 * @note The override is necessary to specialize the return type, and implementation is necessary (can't leave the method abstract) because [[Covenant]] is invariant.
		 * @param f a function that transforms the result of this [[Duty]].
		 *
		 * $isExecutedByDoSiThEx
		 *
		 * $notGuarded
		 */
		override def map[B](f: A => B): LatchedDuty[B] = {
			this match {
				case rd: ReadyDuty[A] => rd.map(f)
				case c: Covenant[A @unchecked] => c.map(f)
			}
		}


		/**
		 * Transforms this [[LatchedDuty]] by applying the provided function to the result of this [[LatchedDuty]] and then subscribing-to the [[LatchedDuty]] returned by said function.
		 * The returned [[LatchedDuty]] will be already fulfilled if, and only if, this [[LatchedDuty]] is already fulfilled.
		 * @note CAUTION: Must be called within the $DoSiThEx
		 * @param f a function that is applied to the result of this [[Duty]] execution to return a [[Duty]] that is executed next to produce the result that the [[Duty]] returned by this method yields.
		 *
		 * $isExecutedByDoSiThEx
		 *
		 * $notGuarded
		 */
		def flatMap[B](f: A => LatchedDuty[B]): LatchedDuty[B]

		/** Returns this [[LatchedDuty]] after subscribing the provided side-effecting procedure to it.
		 * If this [[LatchedDuty]] is already fulfilled, the provided side-effecting procedure is executed synchronously (before this method returns).
		 * Otherwise, the provided side-effecting procedure is scheduled to run upon completion in subscription order (after sequentially running all the previously subscribed result consumers).
		 * Note that the implicit subscription done when chaining an operation to this one occurs after the subscription of the provided side-effecting procedure, se they are ran after the provided side-effecting procedure.
		 * @note CAUTION: Must be called within the $DoSiThEx
		 * */
		override def andThen(sideEffect: A => Any): LatchedDuty[A] = {
			subscribe(a => sideEffect(a))
			this
		}
	}

	final class ReadyDuty[+A](a: A) extends LatchedDuty[A] {
		override def engage(onComplete: A => Unit): Unit = onComplete(a)

		override def subscribe(consumer: A => Unit): Unit = consumer(a)

		override def unsubscribe(onComplete: A => Unit): Unit = ()

		override def maybeResult: Maybe[A] = Maybe(a)

		override def isCompleted: Boolean = true

		override def isPending: Boolean = false

		override def foreach(consumer: A => Unit): Unit = consumer(a)

		override def map[B](f: A => B): ReadyDuty[B] = ReadyDuty(f(a))

		override def flatMap[B](f: A => LatchedDuty[B]): LatchedDuty[B] = Covenant[B]().fulfillWith(f(a))

		override def toFutureHardy(isWithinDoSiThEx: Boolean = isInSequence): Future[A] = Future.successful(a)

		override def toString: String = deriveToString[ReadyDuty[A]](this)
	}

	/** Creates an already completed [[LatchedDuty]].
	 * @param immediateResult the immediate result that this [[LatchedDuty]] yields. */
	inline def LatchedDuty_ready[A](immediateResult: A): ReadyDuty[A] = {
		ReadyDuty(immediateResult)
	}

	/** An already completed [[LatchedDuty]] that yields [[Unit]].
	 * CAUTION: This @threadUnsafe lazy val does not guarantee a unique instance under concurrent access. Its use is only safe for logic that depends on the value's data, not its object identity (eq/ne). */
	@threadUnsafe lazy val LatchedDuty_unit: LatchedDuty[Unit] = LatchedDuty_ready(())

	/** An already completed [[LatchedDuty]] that yields `true`.
	 * CAUTION: This @threadUnsafe lazy val does not guarantee a unique instance under concurrent access. Its use is only safe for logic that depends on the value's data, not its object identity (eq/ne). */
	@threadUnsafe lazy val LatchedDuty_true: LatchedDuty[Boolean] = LatchedDuty_ready(true)

	/** An already completed [[LatchedDuty]] that yields `false`.
	 * CAUTION: This @threadUnsafe lazy val does not guarantee a unique instance under concurrent access. Its use is only safe for logic that depends on the value's data, not its object identity (eq/ne). */
	@threadUnsafe lazy val LatchedDuty_false: LatchedDuty[Boolean] = LatchedDuty_ready(false)

	/** Like [[Duty_sequenceTasksToArray]] but eager.
	 * */
	inline def LatchedDuty_sequenceTasksToArray[A: ClassTag, C[x] <: Iterable[x]](tasks: C[Task[A]], isWithinDoSiThEx: Boolean = isInSequence): LatchedDuty[Array[Try[A]]] =
		Covenant_triggerAndWire(Duty_sequenceTasksToArray(tasks), isWithinDoSiThEx)

	//// COVENANT /////

	/** A [[LatchedDuty]] with dynamic control of its completion (the execution of the subscribed consumers).
	 *
	 * It exposes methods such as [[fulfill]] and [[fulfillWith]] to allow external code to complete it.
	 *
	 * [[Covenant]] is to [[Duty]] as [[Commitment]] is to [[Task]], and as [[scala.concurrent.Promise]] is to [[scala.concurrent.Future]]
	 * */
	final class Covenant[A](fixedResult: Maybe[A]) extends LatchedDuty[A], Subscriptable[A] { thisCovenant =>
		protected var oResult: Maybe[A] = fixedResult

		def this() = this(Maybe.empty)

		override def maybeResult: Maybe[A] = {
			checkWithin()
			oResult
		}

		override def subscribe(consumer: A => Unit): Unit = {
			checkWithin()
			oResult.fold {
				super.subscribe(consumer)
			}(consumer)
		}

		override def unsubscribe(onComplete: A => Unit): Unit = {
			checkWithin()
			if oResult.isEmpty then super.unsubscribe(onComplete)
		}

		override def isCompleted: Boolean = {
			checkWithin()
			this.oResult.isDefined
		}

		override def isPending: Boolean = {
			checkWithin()
			this.oResult.isEmpty
		}

		override def foreach(consumer: A => Unit): Unit = {
			if isInSequence then {
				oResult.fold(subscribe(consumer))(consumer)
			} else {
				execute(oResult.fold(subscribe(consumer))(consumer))
			}
		}

		override def map[B](f: A => B): LatchedDuty[B] = {
			if isInSequence then {
				oResult.fold {
					val covenant = Covenant[B]()
					this.subscribe(a => covenant.fulfillUnsafe(f(a)))
					covenant
				} { a =>
					ReadyDuty[B](f(a))
				}
			} else {
				val covenant = Covenant[B]()
				this.trigger(false)(a => covenant.fulfillUnsafe(f(a)))
				covenant
			}
		}

		override def flatMap[B](f: A => LatchedDuty[B]): LatchedDuty[B] = {
			inline def consumer(a: A, f: A => LatchedDuty[B], c: Covenant[B]): Unit = f(a).subscribe(b => c.fulfillUnsafe(b))

			val covenant = Covenant[B]()
			if isInSequence then {
				oResult.fold(this.subscribe(a => consumer(a, f, covenant)))(a => consumer(a, f, covenant))
			} else {
				this.trigger(false)(a => consumer(a, f, covenant))
			}
			covenant
		}

		/** The [[LatchedDuty]] whose completion is controlled by this [[Covenant]].
		 *
		 * Provided to mimic containment semantics, allowing external code to treat this [[Covenant]] as if it exposed a separate [[LatchedDuty]] field.
		 * @return this [[Covenant]] as a [[LatchedDuty]]. */
		inline def latchedDuty: LatchedDuty[A] = thisCovenant


		/** Fulfills this [[Covenant]] with the given `result`, unless it has already been fulfilled at the time the fulfillment is performed.
		 *
		 * Fulfillment is performed:
		 * - Synchronously (before this method returns) if `isWithinDoSiThEx` is true.
		 * - Asynchronously as soon as possible otherwise.
		 *
		 * This method delegates to [[fulfillUnsafe]], scheduling it within this [[Doer]]'s sequential executor if not already executing within it.
		 *
		 * @param result the value to complete this [[Covenant]] with.
		 * @param isWithinDoSiThEx $isWithinDoSiThEx
		 * @param onCompleted optional callback invoked with the final result and a boolean indicating whether this [[Covenant]] was already completed.
		 *                    If [[ANOTHER_BEFORE]], the result is a previously set one; if [[THE_PROVIDED]], the result is the one provided here.
		 */
		inline def fulfill(result: A, isWithinDoSiThEx: Boolean = isInSequence, onCompleted: (A, ImmediateResultOrigin) => Unit = (_, _) => ()): this.type = {
			if isWithinDoSiThEx then fulfillUnsafe(result, onCompleted)
			else {
				execute(fulfillUnsafe(result, onCompleted))
				this
			}
		}


		/** Fulfills this [[Covenant]] with the given `result`, unless it has already been fulfilled.
		 *
		 * If this [[Covenant]] is not yet fulfilled, the provided `result` becomes its final value and is made immediately visible to all subscribers.
		 * If it is already fulfilled, the provided `result` is ignored.
		 *
		 * CAUTION: This method must be called within this [[Doer]].
		 * CAUTION: Deep synchronous chains of [[flatMap]] over immediately-fulfilled [[LatchedDuty]] instances during ongoing fulfillment can form a synchronous recursion (fulfill → subscribe-immediate → fulfill → …) that overflows the stack. // TODO consider the trampoline solutions discussed with copilot in the session "causal anchoring dilema", near the end.
		 * CAUTION: Too many subscriptions may overflow the stack upon fulfillment. // TODO consider implementing a list with a pointer to the tail to avoid the recursion loop of the current implementation.
		 *
		 * @param result the value to fulfill this [[Covenant]] with.
		 * @param onCompleted optional callback invoked synchronously (before this method returns) with the fulfilling value and information about its origin:
		 *                    - [[THE_PROVIDED]] if this [[Covenant]] was completed by this method call with the provided value;
		 *                    - [[ANOTHER_BEFORE]] if this [[Covenant]] was already completed when this method was called.
		 */
		def fulfillUnsafe(result: A, onCompleted: (A, ImmediateResultOrigin) => Unit = (_, _) => ()): this.type = {
			oResult.fold {
				this.oResult = Maybe(result)
				// First run the consumers in subscriptions order
				if firstOnCompleteObserver ne null then {
					firstOnCompleteObserver(result)
					firstOnCompleteObserver = null // Nullify the reference to help the garbage collector.
					if this.onCompletedObservers.nonEmpty then {
						def loop(head: A => Unit, tail: List[A => Unit]): Unit = {
							if tail.nonEmpty then loop(tail.head, tail.tail)
							head(result)
						}

						loop(this.onCompletedObservers.head, this.onCompletedObservers.tail)
						this.onCompletedObservers = Nil // Clean the observers list to help the garbage collector.
					}
				}
				// Finally call the provided call-back.
				onCompleted(result, THE_PROVIDED)
			}(previousResult => onCompleted(previousResult, ANOTHER_BEFORE))
			this
		}

		/** Wires this [[Covenant]] to be completed with the result of a [[Duty]].
		 *
		 * Arranges this [[Covenant]] to be fulfilled if `fulfillingDuty` completes, unless it was fulfilled before.
		 * Always one, and only one, of the two callback is invoked:
		 *		- `onAlreadyCompleted` if this [[Covenant]] was already fulfilled when the subscription is done.
		 *		- `onCompletedLater` if this [[Covenant]] is fulfilled after the subscription is done.
		 * The subscription is synchronic if `isWithinDoSiThEx` is true, and asynchronic ASAP otherwise.
		 *
		 * @param fulfillingDuty the [[Duty]] whose result will be used to complete this [[Covenant]].
		 * @param isWithinDoSiThEx $isWithinDoSiThEx
		 * @param onCompleted optional callback invoked when this [[Covenant]] is fulfilled. The first parameter is the fulfilling value and the second informs about its origin. Invoked within this [[Doer]] sequential executor.
		 * @throws IllegalArgumentException if `fulfillingDuty` is the same instance as this [[Covenant]].
		 */
		def fulfillWith(fulfillingDuty: Duty[A], isWithinDoSiThEx: Boolean = isInSequence, onCompleted: (A, ResultOrigin) => Unit = (_, _) => ()): this.type = {
			if fulfillingDuty eq this then throw IllegalArgumentException("A Covenant can't be fulfilled with itself.")
			if isWithinDoSiThEx then oResult.fold(fulfillingDuty.engagePortal(result => fulfillUnsafe(result, onCompleted)))(result => onCompleted(result, ANOTHER_BEFORE))
			else execute(fulfillWith(fulfillingDuty, true, onCompleted))
			this
		}

		override def toString(): String = {
			s"Covenant(oResult=$oResult, subscriptions=[${if firstOnCompleteObserver eq null then "" else s"$firstOnCompleteObserver, ${onCompletedObservers.mkString(", ")}"}])"
		}
	}


	/** Creates an already completed [[Covenant]].
	 * @param immediateResult the immediate result that this [[Covenant]] yields. */
	inline def Covenant_ready[A](immediateResult: A): Covenant[A] =
		Covenant(Maybe(immediateResult))

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
	inline def Covenant_triggerAndWire[A](duty: Duty[A], inline isWithinDoSiThEx: Boolean = isInSequence, onFulfilled: (A, ImmediateResultOrigin) => Unit = (_: A, _: ImmediateResultOrigin) => ()): Covenant[A] = {
		val covenant = new Covenant[A]()
		duty.trigger(isWithinDoSiThEx)(result => covenant.fulfillUnsafe(result, onFulfilled))
		covenant
	}

	//// Causal Fence ////

	/** A fence that serializes non-failing state transitions with causal fulfillment semantics.
	 * It enforces causal ordering; lineage continues indefinitely.
	 *
	 * Each state transition attempt is causally anchored to the previous one, ensuring that each update observes the latest committed state, and that all updates are fulfilled in causal order.
	 * Speculative updates may be rolled back before becoming visible.
	 *
	 * Core idea: CausalFence[A] is a causal sequencing primitive. It maintains a single tail covenant ([[lastEnqueuedCovenant]]) that represents the latest step in a causal chain. Each [[advance]] and [[causalAnchor]] call enqueues a new [[Covenant]], chained to the previous one.
	 *
	 * This fence provides **causal fulfillment semantics**:
	 * - Each transition yields a committed, visible state. Rolled-back transitions fulfill with the previous state.
	 * - The causal chain is never broken: all updates are fulfilled and causally ordered.
	 * - The committed lineage includes all transitions.
	 * - Transitions cannot fail — they either commit a new state or retain the previous one.
	 *
	 * Fundamental Invariants
	 * - Single updater invariant: At most one `primaryStateUpdater` (the function passed to [[advance]]) is in progress at a time. Subsequent advances wait until the previous one completes.
	 * - Advance ordering invariant: The `primaryStateUpdater` passed to [[advance]] runs after all consumers that subscribed to the previous [[Covenant]] in the queue (via earlier [[advance]] or [[causalAnchor]]) have completed their synchronous part.
	 * - Anchor freshness invariant: - A consumer subscribed to [[causalAnchor]] observes the same state that a `primaryStateUpdater`` would receive if [[advance]] were invoked at that moment. Ordering is guaranteed relative to the last completed advance at subscription time, but not relative to advances invoked later.
	 * - Rollback integrity invariant: For speculative advances, derived updates must not be composed into the primary updater, otherwise rollback semantics are broken.
	 *
	 * Invariants related to derived state:
	 * - Game-changing invariant: Immediately after an [[advance]] or [[causalAnchor]] call, there are no pending advances other than the one just created. The returned [[Covenant]] (seen as [[LatchedDuty]]) is a fresh tail.  Any immediate synchronous subscription to it is guaranteed to be the first subscriber in its list. Therefore, when the covenant fulfills, that consumer sees the up‑to‑date state deterministically. This invariant eliminates the race where another [[advance]] could sneak in and publish a newer state before or while the synchronous consumer runs. The consumer is deterministically ordered before any updater that follows in the causal chain.
	 * - Anchor specificity invariant: Derived updates that require strict ordering relative to a particular primary transition must anchor to that transition’s [[Covenant]] (the one returned by [[advance]]), not to a generic tail snapshot (as the returned by [[causalAnchor]]).
	 * - Deterministic derived ordering invariant: When derived updates have dependencies, their execution order must be enforced by anchoring the dependent update and deriving prerequisites synchronously from the anchored state, or by composing into the primary updater if not speculative.
	 * - Rollback integrity invariant: No derived side-effect may commit externally during a speculative advance before the primary step is safely committed; otherwise rollback can leave the system in an inconsistent state.
	 * - Idempotence/compensation invariant: Any derived side-effect that can be re-run or rolled back must be idempotent or have a compensating action to preserve causal correctness under retries or rollback.
	 *
	 * Invariants inherited from [[Covenant]]:
	 * - Sequential consumer invariant: The [[LatchedDuty]] returned by [[advance]] and [[causalAnchor]] is a [[Covenant]] and therefore the subscribed consumers are invoked in registration order. The synchronous part of each consumer runs to completion before the next begins.
	 * @param initialState the initial committed state, already visible and causally anchored.
	 */
	class CausalFence[A](initialState: A) {
		private var lastCommittedCovenant: Covenant[A] = Covenant_ready(initialState)
		private var lastEnqueuedCovenant: Covenant[A] = lastCommittedCovenant

		/** Provides rollback capability for speculative updates.
		 *
		 * Used within [[advanceSpeculatively]] to discard an in-flight update before it becomes visible.
		 * Rollback is only effective if invoked before the update is committed.
		 * Rollback is always non-failing and fulfills the update with the previous state.
		 * The returned [[LatchedDuty]] completes with the same state that becomes visible.
		 */
		trait RollbackAccessor {
			/** Attempts to roll back the speculative update, restoring the previous visible state.
			 *
			 * The rollback is applied if "invoked" before the update it targets has completed.
			 * The quotes around "invoked" reflect that, when `isWithinDoSiThEx` is false, the rollback attempt is scheduled asynchronously and may race with the update's completion.
			 *
			 * Regardless of timing, the [[LatchedDuty]] originally returned by [[advanceSpeculatively]] is fulfilled:
			 * - With the previous state if rollback succeeds
			 * - With the committed state if rollback was too late
			 *
			 * The callback receives the resulting state and a flag indicating whether rollback was applied (`false`) or too late (`true`).
			 *
			 * @param isWithinDoSiThEx whether the caller is executing within the Doer's sequential executor
			 * @param onCompleted a callback invoked with the resulting state and a flag indicating whether rollback was too late
			 * @return the [[LatchedDuty]] originally returned by [[advanceSpeculatively]], now fulfilled with either the committed or rolled-back state
			 */
			def rollback(isWithinDoSiThEx: Boolean = isInSequence, onCompleted: (A, RollbackApplication) => Unit = (_, _) => ()): LatchedDuty[A]
		}

		/** Provides the tail of the causal chain at call time.
		 *
		 * Unlike [[causalAnchor]], this method does not guarantee deterministic observation of the up‑to‑date state. Subscribers to the returned [[LatchedDuty]] will observe the progression until that tail, but synchronous [[advance]]s that install a newer tail before the subscriber executes are not observed.
		 *
		 * @return a [[LatchedDuty]] representing the tail of the causal chain at call time.
		 */
		def causalChainTail(isWithinDoSiThEx: Boolean = isInSequence): LatchedDuty[A] = {
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
		inline def committedState: A = {
			checkWithin()
			lastCommittedCovenant.maybeResult.get
		}

		/** Returns a [[LatchedDuty]] that yields the currently visible state.
		 *
		 * This reflects the state to which the most recent completed step transitioned into.
		 * Rolled-back transitions yield the previous state.
		 * The returned [[LatchedDuty]] is always already fulfilled.
		 *
		 * @param isWithinDoSiThEx whether the caller is executing within the Doer's sequential executor
		 * @return a [[LatchedDuty]] yielding the currently visible state.
		 */
		def committed(isWithinDoSiThEx: Boolean = isInSequence): LatchedDuty[A] = {
			if isWithinDoSiThEx then lastCommittedCovenant
			else {
				val wrapper = Covenant[A]()
				execute(wrapper.fulfillUnsafe(committedState))
				wrapper
			}
		}

		/**
		 * Returns a [[LatchedDuty]] that yields the same state an updater would see if [[advance]] were invoked at this moment.
		 * This provides a causal checkpoint suitable for synchronous consumers that need to derive state deterministically.
		 *
		 * The provided state consumer, along with any synchronous subscribers to the returned [[LatchedDuty]], will be executed when the anchored link of the causal chain is reached, receiving the state at that link.
		 *
		 * **Temporal window of causal safety:**
		 * The causal guarantee holds only during the synchronous execution of a consumer subscribed to the returned [[LatchedDuty]].
		 * Methods that rely on causal visibility are safe only within the body of that consumer; once the consumer has returned, deferred or later code is no longer causally anchored.
		 *
		 * @note When derived updates (secondary state depending on primary state) have causal dependencies among themselves, deterministic order must be enforced by other means: either anchor only the dependent update and derive prerequisites synchronously from the anchored state, or — if derived updates are fast and the advance is not speculative — compose them into the `primaryStateUpdater` passed to [[advance]] or [[advanceIf]]. Composition is unsafe for speculative advances, because rollback during the derived update phase could succeed when it should not.
		 *
		 * Independent subscriptions to [[causalAnchor]] are appropriate only for derived updates that are order‑independent.
		 *
		 * Implementation note: The returned [[Covenant]] (exposed as a [[LatchedDuty]]) participates in the causal chain by linking forward from the current tail. This ensures that immediate synchronous subscriptions are registered before fulfillment, guaranteeing deterministic observation of the up‑to‑date state.
		 *
		 * @param stateConsumer optional callback invoked when the anchored link is reached. Executed within this [[Doer]]’s sequential executor before any consumer subscribed to the returned [[LatchedDuty]]. The first parameter is the primary state; the second indicates whether the link was already reached when this method was invoked: [[ARRIVED_BEFORE]] if so, or [[ARRIVED_AFTER]] if not.
		 * @return a [[LatchedDuty]] yielding the state that the next update will be causally anchored to — i.e. the same state an updater would see if [[advance]] were called at this moment.
		 */
		def causalAnchor(stateConsumer: (A, CausalAnchorArrival) => Unit = (_, _) => ()): LatchedDuty[A] = {
			checkWithin()
			val previousStepCovenant = lastEnqueuedCovenant
			val thisStepCovenant = Covenant[A]()
			lastEnqueuedCovenant = thisStepCovenant
			thisStepCovenant.fulfillWith(previousStepCovenant, true, stateConsumer)
		}

		/** Advances the state with a non-speculative asynchronous update causally anchored to the previous update.
		 *
		 * Is worth mentioning that the provided function will be executed after all the consumers subscribed (before the call to this method) to both, the [[LatchedDuty]] returned by both [[causalAnchor]] and the one returned by previous calls to [[advance]]-like methods, have completed.
		 *
		 * Caution: The execution of consumers that are subscribed to obsolete instances of [[LatchedDuty]] is not causally ordered.
		 * So, avoid memorizing the [[LatchedDuty]] instances returned by [[causalAnchor]] or [[advance]]-like methods; always subscribe to the instance returned by [[causalAnchor]] to ensure causal ordering of the consumers executions.
		 * Obsolete are those instances returned by methods of this [[CausalFence]] before the last call to an [[advance]]-like method).
		 *
		 * The updater function is defined with a second parameter of type `Null` to match the internal speculative signature, allowing reuse without introducing an extra closure.
		 * Rollback is not supported in this method.
		 *
		 * **Temporal window of causal safety:**
		 * The causal guarantee holds from the moment the `primaryStateUpdater` function is invoked until the [[LatchedDuty]] it returns has completed.
		 * Calls to methods that rely on causal visibility are safe only within this window; after completion, the causal fence is closed and subsequent calls are no longer anchored.
		 *
		 * @param primaryStateUpdater a function that computes the next state from the current one
		 * @param isWithinDoSiThEx whether the caller is executing within the Doer's sequential executor
		 * @return a [[LatchedDuty]] that will be fulfilled with the new state once the update completes.
		 */
		inline def advance(inline primaryStateUpdater: (A, Null) => Duty[A], inline isWithinDoSiThEx: Boolean = isInSequence): LatchedDuty[A] =
			advanceIf((a, _) => Maybe(primaryStateUpdater(a, null)), isWithinDoSiThEx)


		/** Like [[advance]], but the update may be synchronously canceled by returning [[Maybe.empty]]
		 * If the [[primaryStateUpdater]] returns some state, it is committed.
		 * If it returns [[Maybe.empty]], the update is canceled and the previous state is retained.
		 * @param primaryStateUpdater a partial function that computes the next state from the current one; the second argument is always `null`
		 * @param isWithinDoSiThEx whether the caller is executing within the Doer's sequential executor
		 * @return a [[LatchedDuty]] that yields the updated state
		 * */
		def advanceIf(primaryStateUpdater: (A, Null) => Maybe[Duty[A]], isWithinDoSiThEx: Boolean = isInSequence): LatchedDuty[A] = {
			val suAdapter = primaryStateUpdater.asInstanceOf[(A, RollbackAccessor | Null) => Maybe[LatchedDuty[A]]]
			if isWithinDoSiThEx then step(suAdapter, false)
			else {
				val covenant = Covenant[A]()
				execute(covenant.fulfillWith(step(suAdapter, false), true))
				covenant
			}
		}

		/** Advances the state with a rollback-aware speculative asynchronous update causally anchored to the previous update.
		 *
		 * The provided [[RollbackAccessor]] allows the update to be withdrawn before it becomes visible.
		 * All updates fulfill successfully, even when rolled back.
		 *
		 * @param primaryStateUpdater a function that computes the next state from the current one, with rollback control
		 * @param isWithinDoSiThEx whether the caller is executing within the Doer's sequential executor
		 * @return a [[LatchedDuty]] that yields the updated or rolled-back state
		 */
		inline def advanceSpeculatively(inline primaryStateUpdater: (A, RollbackAccessor) => Duty[A], inline isWithinDoSiThEx: Boolean = isInSequence): LatchedDuty[A] = {
			advanceSpeculativelyIf(
				(a, rba) => Maybe(primaryStateUpdater(a, rba)),
				isWithinDoSiThEx
			)
		}

		/** Like [[advanceSpeculatively]], but the update may be synchronously canceled by returning [[Maybe.empty]]
		 * If the [[primaryStateUpdater]] returns some state, it is committed.
		 * If it returns [[Maybe.empty]], the update is canceled and the previous state is retained.
		 * @param primaryStateUpdater a partial function that computes the next state from the current one, with rollback control
		 * @param isWithinDoSiThEx whether the caller is executing within the Doer's sequential executor
		 * @return a [[LatchedDuty]] that yields the updated state
		 * */
		def advanceSpeculativelyIf(primaryStateUpdater: (A, RollbackAccessor) => Maybe[Duty[A]], isWithinDoSiThEx: Boolean = isInSequence): LatchedDuty[A] =
			if isWithinDoSiThEx then step(primaryStateUpdater, true)
			else {
				val covenant = Covenant[A]()
				execute(covenant.fulfillWith(step(primaryStateUpdater, true), true))
				covenant
			}

		/** Internal method that performs the actual state transition.
		 *
		 * Handles both speculative and non-speculative updates depending on the `isSpeculative` flag.
		 * The rollback accessor is instantiated only when needed to avoid unnecessary allocations.
		 */
		private def step(primaryStateUpdater: (A, RollbackAccessor | Null) => Maybe[Duty[A]], isSpeculative: Boolean): LatchedDuty[A] = {
			val previousStepCovenant = lastEnqueuedCovenant
			val thisStepCovenant = Covenant[A]()
			lastEnqueuedCovenant = thisStepCovenant

			previousStepCovenant.subscribe { previousState =>
				val rba =
					if isSpeculative then new RollbackAccessor {
						override def rollback(isWithinDoSiThEx: Boolean = isInSequence, onCompleted: (A, RollbackApplication) => Unit): LatchedDuty[A] =
							thisStepCovenant.fulfill(previousState, isWithinDoSiThEx, onCompleted)
					} else null
				primaryStateUpdater(previousState, rba)
					.fold {
						lastCommittedCovenant = thisStepCovenant
						thisStepCovenant.fulfillUnsafe(previousState)
					} { newStateProviderDuty =>
						newStateProviderDuty.engagePortal { newState =>
							lastCommittedCovenant = thisStepCovenant
							thisStepCovenant.fulfillUnsafe(newState)
						}
					}
			}
			thisStepCovenant
		}

		/** Advances the state with a synchronous, non-speculative update causally anchored to the previous one.
		 *
		 * The update is applied synchronously and always fulfills with a committed state:
		 * - The [[primaryStateUpdater]] is applied to the previous state.
		 * - The resulting state is committed immediately.
		 *
		 * Rollback machinery is not required because the update is synchronous and may be canceled
		 * by returning [[Maybe.empty]].
		 *
		 * @param primaryStateUpdater a total function that produces the next state from the current one
		 * @return a [[LatchedDuty]] that is always fulfilled with the committed state
		 */

		inline def jump(inline primaryStateUpdater: A => A): LatchedDuty[A] =
			jumpIf(a => Maybe(primaryStateUpdater(a)))

		/** Like [[jump]], but the update may be canceled by returning [[Maybe.empty]].
		 *
		 * If the [[primaryStateUpdater]] returns some state, it is committed.
		 * If it returns [[Maybe.empty]], the update is canceled and the previous state is retained.
		 *
		 * @param primaryStateUpdater a partial function that produces a new state from the previous one
		 * @return a [[LatchedDuty]] that is always fulfilled with the committed state
		 */
		def jumpIf(primaryStateUpdater: A => Maybe[A]): LatchedDuty[A] = {
			val previousStepCovenant = lastEnqueuedCovenant
			val thisStepCovenant = Covenant[A]()
			lastEnqueuedCovenant = thisStepCovenant

			previousStepCovenant.subscribe { previousState =>
				primaryStateUpdater(previousState)
					.fold {
						lastCommittedCovenant = thisStepCovenant
						thisStepCovenant.fulfillUnsafe(previousState)
					} { newState =>
						lastCommittedCovenant = thisStepCovenant
						thisStepCovenant.fulfillUnsafe(newState)
					}
			}
			thisStepCovenant
		}

		override def toString: String = {
			s"CausalFence(lastEnqueuedCovenant=${lastEnqueuedCovenant.toString}, lastCommittedCovenant=${lastCommittedCovenant.toString})"
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
		 * Transform this [[Task]] by applying the given function to the result. Analogous to [[Future.transform]]
		 * ===Detailed description===
		 * Creates a [[Task]] that yields the result of applying the provided function to the results of this [[Task]].
		 * If the evaluation of the provided function finishes:
		 *		- abruptly, completes with the cause.
		 *		- normally, completes with the result of the evaluation.
		 *
		 * $threadSafe
		 *
		 * @param resultTransformer applied to the result of this [[Task]] to obtain the result of the returned [[Task]].
		 *
		 * $isExecutedByDoSiThEx
		 *
		 * $unhandledErrorsArePropagatedToTaskResult
		 */
		def transform[B](resultTransformer: Try[A] => Try[B]): Task[B] = new Task_Transform(thisTask, resultTransformer)


		/**
		 * Transforms this [[Task]] by applying the provided function to the result of this [[Task]] and then executing the [[Task]] returned by said function.  
		 * ===Detailed behavior===
		 * Creates a [[Task]] that, when executed, it will:
		 *		- Execute this [[Task]] and apply the provided function to its result.
		 *		- Then executes the [[Task]] built in the previous step by the provided function and completes with its result.
		 *
		 * $threadSafe
		 *
		 * @param taskBBuilder a function that is applied to the result of this [[Task]] execution, to build a [[Task]] that is executed next to produce the result that the [[Task]] returned by this method yields.
		 *
		 * $isExecutedByDoSiThEx
		 */
		inline def transformWith[B](taskBBuilder: Try[A] => Task[B]): Task[B] =
			new Task_TransformWith(thisTask, taskBBuilder)


		/**
		 * Transforms this [[Task]] by applying the given function to the result if it is successful. Analogous to [[Future.map]].
		 *
		 * Equivalent to {{{ transform(_ map f) }}} but more efficient (creates one less closure).
		 *
		 * See [[recover]] and [[toDuty]] if you want to transform the failures; and [[transform]] if you want to transform both, successful and failed ones.
		 * ===Detailed behavior===
		 * Creates a [[Task]] that yields the result of applying the provided function to successful results of this [[Task]].
		 * If the evaluation of the provided function finishes:
		 *		- abruptly, completes with the cause.
		 *		- normally, completes with the successful result.
		 *
		 * $threadSafe
		 *
		 * @param f a function that transforms successful results of this task. $isExecutedByDoSiThEx $unhandledErrorsArePropagatedToTaskResult
		 */
		def map[B](f: A => B): Task[B] = new Task_Map(this, f)

		/**
		 * Composes this [[Task]] with a second one that is built from the result of this one, but only when this one is successful. Analogous to [[Future.flatMap]].
		 * ===Detailed behavior===
		 * Creates a [[Task]] that, when executed, it will:
		 *		- Trigger an execution of this [[Task]] and if the result is:
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
		override def andThen(sideEffect: Try[A] => Any): Task[A] = {
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

	/** Creates a [[Task]] that triggers the execution of the provided [[Task]] by another [[Doer]], and yields its result.
	 * When triggered, the `foreignTask` is executed within the `foreignDoer` (in sequence with whatever the `foreignDoer` is doing), and its result is supplied by the created [[Task]] in sequence with this [[Doer]].
	 * Useful to start a process in a another [[Doer]] and access its result sequentially.
	 * $threadSafe
	 *
	 * @param foreignDoer the [[Doer]] to whom the `foreignTask` belongs.
	 * @param foreignTask the [[Task]] to be executed by the `foreignDoer`. Its result will be yielded by the returned [[Task]] in sequence with this [[Doer]].
	 * @return a [[Task]] that produces what the `foreignTask` produces, but the result is yielded in sequence with this [[Doer]]. */
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
	 * */
	inline def Duty_sequenceTasksToArray[A: ClassTag, C[x] <: Iterable[x]](tasks: C[Task[A]]): Duty[Array[Try[A]]] =
		new Duty_SequenceHardy[A, C](tasks)

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
	 * A [[Task]] that, when executed, triggers an execution of the `originalTask` and applies the provided `resultTransformer` to its results. If the evaluation finishes:
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
	 * A [[Task]] that transform the result of another task.
	 * ===Detailed description===
	 * A [[Task]] that executes the `originalTask` and if the result is a:
	 *		- [[Failure]], completes with that failure.
	 *		- [[Success]], applies the provided `resultTransformer` to its [[Success.value]]. If the evaluation finishes:
	 *			- abruptly, completes with the cause.
	 *			- normally, completes with the result of the evaluation.
	 *
	 * $onCompleteExecutedByDoSiThEx
	 *
	 * @param resultTransformer applied to the result of the `originalTask` to obtain the result of this task.
	 *
	 * $isExecutedByDoSiThEx
	 *
	 * $unhandledErrorsArePropagatedToTaskResult
	 */
	final class Task_Map[+A, +B](originalTask: Task[A], resultTransformer: A => B) extends AbstractTask[B] {
		override def engage(onComplete: Try[B] => Unit): Unit = {
			originalTask.engagePortal {
				case success: Success[A] =>
					val tryB =
						try Success(resultTransformer(success.value))
						catch {
							case NonFatal(e) => Failure(e);
						}
					onComplete(tryB)
				case failure: Failure[A] =>
					onComplete(failure.castTo[B])
			}
		}

		override def toString: String = deriveToString[Task_Map[A, B]](this)
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
					ota = Maybe(tryA)
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
					otb = Maybe(tryB)
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

	final class Duty_SequenceHardy[A: ClassTag, C[x] <: Iterable[x]](tasks: C[Task[A]]) extends AbstractDuty[Array[Try[A]]] {
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


	////////////// LATCHED TASK ///////////////

	/**
	 * A [[Task]] whose result, once completion occurs, is latched exactly once and subsequently delivered deterministically to present and future subscribers.
	 * Specifically, a [[Task]] that:
	 *		- Is completed a single time and caches the result so that, once completed, subscribing a consumer executes the call-back immediately. Note that linking a down-chain subscribes the first link as consumer.
	 *		- The monadic laws are always upheld. Before completion, they can’t be observed because no result exists yet; after completion, they can be observed in the cached result.
	 *		- Allows to subscribe/unsubscribe consumers of its completion result dynamically.
	 *		- The source of determination may be intrinsic from the start (e.g. {{{ Commitment[String]().fulfillWith(anIntrinsicallyDeterminedTask) }}}) or external (e.g. {{{ Commitment[String]().fulfill(someValueDeterminedExternally) }}}); the concrete result value is realized only at completion.
	 * The timing and outcome of completion are not specified by this class. That behavior is delegated to subclasses; see [[Commitment]].
	 * @note Triggering (calling [[trigger]]) on a pending [[LatchedTask]] does not trigger the execution of the subscribed consumers, but just subscribes the `onComplete` call-back passed to [[trigger]] as a consumer of the future result.
	 * */
	class LatchedTask[A] private[Doer](fixedResult: Maybe[Try[A]]) extends AbstractTask[A], Subscriptable[Try[A]] {
		protected var oResult: Maybe[Try[A]] = fixedResult

		def this() = this(Maybe.empty)

		inline def maybeResult: Maybe[Try[A]] = {
			checkWithin()
			oResult
		}

		protected override def engage(onComplete: Try[A] => Unit): Unit =
			subscribe(onComplete)

		/**
		 * Subscribes a consumer of the result of this [[LatchedTask]].
		 *
		 * The subscription is automatically removed after this [[LatchedTask]] is completed and the received consumer is executed.
		 *
		 * If this [[LatchedTask]] is already completed when this method is called, the provided consumer is invoked synchronously and no subscription occurs.
		 * Otherwise, the provided consumer is schedule to run upon completion in subscription orden (after sequentially running all the previously subscribed result consumers).
		 *
		 * @note CAUTION: This method does not prevent duplicate subscriptions.
		 * @note CAUTION: Must be called within the $DoSiThEx */
		override def subscribe(consumer: Try[A] => Unit): Unit = {
			checkWithin()
			oResult.fold {
				super.subscribe(consumer)
			}(consumer)
		}

		/** @note CAUTION: should be called within the $DoSiThEx
		 * @return true if this [[Commitment]] was either fulfilled or broken; or false if it is still pending. */
		def isCompleted: Boolean = {
			checkWithin()
			oResult.isDefined
		}

		/** @note CAUTION: should be called within the $DoSiThEx
		 * @return true if this [[Commitment]] is still pending; or false if it was completed. */
		def isPending: Boolean = {
			checkWithin()
			oResult.isEmpty
		}

		inline def intoTask: Task[A] = this


		/**
		 * Transform this [[LatchedTask]] by applying the given function to the result of this [[LatchedTask]]. Analogous to [[Future.transform]]
		 * ===Detailed description===
		 * Creates a [[LatchedTask]] that yields the result of applying the provided function to the result of this [[LatchedTask]].
		 * If the evaluation of the provided function finishes:
		 *		- abruptly, completes with the cause.
		 *		- normally, completes with the result of the evaluation.
		 *
		 * $threadSafe
		 *
		 * @param resultTransformer the function applied to the result of this [[LatchedTask]] to obtain the result of the returned [[LatchedTask]].
		 *
		 * $isExecutedByDoSiThEx
		 *
		 * $unhandledErrorsArePropagatedToTaskResult
		 */
		override def transform[B](resultTransformer: Try[A] => Try[B]): LatchedTask[B] = {
			def fBis(tryA: Try[A]): Try[B] = {
				try resultTransformer(tryA)
				catch {
					case NonFatal(e) => Failure(e)
				}
			}
			oResult.fold {
				val commitment = Commitment[B]()
				this.subscribe { tryA => commitment.completeUnsafe(fBis(tryA)) }
				commitment
			} { tryA =>
				LatchedTask[B](Maybe(fBis(tryA)))
			}
		}


		/**
		 * Transforms this [[LatchedTask]] by applying the provided function to the result of this [[LatchedTask]] and then subscribing to the [[LatchedTask]] returned by said function.
		 * The returned [[LatchedTask]] will be already completed if, and only if, this [[LatchedTask]] is already completed.
		 *
		 * @param f a function that is applied to the result of this [[LatchedTask]] execution, to build a [[LatchedTask]] that is executed next to produce the result that the [[LatchedTask]] returned by this method yields.
		 *
		 * $isExecutedByDoSiThEx
		 *
		 * $unhandledErrorsArePropagatedToTaskResult
		 */
		def transformWith[B](f: Try[A] => LatchedTask[B]): LatchedTask[B] = {
			checkWithin()
			val commitment = Commitment[B]()
			val observer: Try[A] => Unit = tryA =>
				val latchedB =
					try f(tryA)
					catch {
						case NonFatal(e) => LatchedTask[B](Maybe(Failure(e)))
					}
				latchedB.subscribe { tryB =>
					commitment.completeUnsafe(tryB)
				}
			oResult.fold(this.subscribe(observer))(observer)
			commitment
		}

		/**
		 * Transforms this [[LatchedDuty]] by applying the given function to the result if it is successful. Analogous to [[Future.map]].
		 * Equivalent to {{{ transform(_ map f) }}} but more efficient (one less closure allocation).
		 * See [[recover]] and [[toDuty]] if you want to transform the failures; and [[transform]] if you want to transform both, successful and failed ones.
		 * ===Detailed behavior===
		 * Creates a [[LatchedTask]] that yields the result of applying the provided function to the result of this [[LatchedTask]].
		 * If the evaluation of the provided function finishes:
		 *		- abruptly, completes with that failure.
		 *		- normally, apply `f` to `a` and if the evaluation finishes:
		 *			- abruptly with `cause`, completes with `Failure(cause)`.
		 *			- normally with value `b`, completes with `Success(b)`.
		 *
		 * @param f a function that transforms the result of this task, when it is successful.
		 *
		 *          $isExecutedByDoSiThEx
		 *
		 *          $unhandledErrorsArePropagatedToTaskResult
		 */
		override def map[B](f: A => B): LatchedTask[B] = {
			checkWithin()

			def fBis(tryA: Try[A]): Try[B] = {
				tryA match {
					case success: Success[A] =>
						try Success(f(success.value))
						catch {
							case NonFatal(e) => Failure(e)
						}
					case failure: Failure[A] =>
						failure.castTo[B]
				}
			}

			oResult.fold {
				val commitment = Commitment[B]()
				this.subscribe { tryA => commitment.completeUnsafe(fBis(tryA)) }
				commitment
			} { tryA =>
				LatchedTask[B](Maybe(fBis(tryA)))
			}
		}

		/**
		 * Transforms this [[LatchedTask]] by applying the provided function to the result of this [[LatchedTask]] and then subscribing-to the [[LatchedTask]] returned by said function.
		 * The returned [[LatchedTask]] will be already completed if, and only if, this [[LatchedTask]] is already completed.
		 *
		 * $threadSafe
		 *
		 * @param f a function that is applied to the result of this [[Task]] execution to return a [[Task]] that is executed next to produce the result that the [[Task]] returned by this method yields.
		 *
		 * $isExecutedByDoSiThEx
		 *
		 * $notGuarded
		 */
		def flatMap[B](f: A => LatchedTask[B]): LatchedTask[B] = {
			checkWithin()
			val commitment = Commitment[B]()
			oResult.fold {
				this.subscribe {
					case Success(a) => f(a).subscribe(b => commitment.completeUnsafe(b))
					case failure: Failure[A] => commitment.completeUnsafe(failure.castTo[B])
				}
			} {
				case Success(a) => f(a).subscribe(b => commitment.completeUnsafe(b))
				case failure: Failure[A] => commitment.completeUnsafe(failure.castTo[B])
			}
			commitment
		}

		/** Returns this [[LatchedTask]] after subscribing the provided side-effecting procedure to it.
		 * If this [[LatchedTask]] is already completed, the provided side-effecting procedure is executed synchronously (before this method returns).
		 * Otherwise, the provided side-effecting procedure is scheduled to run upon completion in subscription order (after sequentially running all the previously subscribed result consumers).
		 * Note that the implicit subscription done when chaining an operation to this one occurs after the subscription of the provided side-effecting procedure, se they are ran after the provided side-effecting procedure.
		 * @note CAUTION: Must be called within the $DoSiThEx
		 * */
		override def andThen(sideEffect: Try[A] => Any): LatchedTask[A] = {
			subscribe(tryA => sideEffect(tryA))
			this
		}

	}


	/** Creates an already completed [[LatchedTask]].
	 * @param immediateResult the immediate result that this [[LatchedTask]] yields. */
	def LatchedTask_ready[A](immediateResult: Try[A]): LatchedTask[A] =
		LatchedTask(Maybe(immediateResult))

	/** An already completed [[LatchedTask]] that yields [[Unit]].
	 * CAUTION: This @threadUnsafe lazy val does not guarantee a unique instance under concurrent access. Its use is only safe for logic that depends on the value's data, not its object identity (eq/ne). */
	@threadUnsafe lazy val LatchedTask_unit: LatchedTask[Unit] = LatchedTask_ready(Doer.successUnit)

	/** An already completed [[LatchedTask]] that yields `true`.
	 * CAUTION: This @threadUnsafe lazy val does not guarantee a unique instance under concurrent access. Its use is only safe for logic that depends on the value's data, not its object identity (eq/ne). */
	@threadUnsafe lazy val LatchedTask_true: LatchedTask[true] = LatchedTask_ready(Doer.successTrue)

	/** An already completed [[LatchedTask]] that yields `false`.
	 * CAUTION: This @threadUnsafe lazy val does not guarantee a unique instance under concurrent access. Its use is only safe for logic that depends on the value's data, not its object identity (eq/ne). */
	@threadUnsafe lazy val LatchedTask_false: LatchedTask[false] = LatchedTask_ready(Doer.successFalse)

	////////////// COMMITMENT ///////////////

	/** A [[LatchedTask]] with dynamic control of its completion (the execution of the subscribed consumers).
	 *
	 * It exposes methods such as [[complete]] and [[completeWith]] to allow external code to complete it.
	 *
	 * Analogous to [[scala.concurrent.Promise]] but for [[Task]]s instead of a [[scala.concurrent.Future]]s.
	 */
	final class Commitment[A] private[Doer](fixedResult: Maybe[Try[A]]) extends LatchedTask[A](fixedResult) { thisCommitment =>

		def this() = this(Maybe.empty)

		/** The [[LatchedTask]] whose completion is controlled by this [[Commitment]].
		 *
		 * Provided to mimic containment semantics, allowing external code to treat this [[Commitment]] as if it exposed a separate [[LatchedTask]] field.
		 * @return this [[Commitment]] as a [[LatchedTask]] */
		inline def latchedTask: LatchedTask[A] = thisCommitment

		/** Completes this [[Commitment]] with the given `result`, unless it has already been completed at the time the completion is performed.
		 *
		 * Completion is performed:
		 * - Synchronously (before this method returns) if `isWithinDoSiThEx` is true.
		 * - Asynchronously as soon as possible otherwise.
		 *
		 * This method delegates to [[completeUnsafe]], scheduling it within this [[Doer]]'s sequential executor if not already executing within it.
		 *
		 * @param result the value to complete this [[Commitment]] with.
		 * @param isWithinDoSiThEx $isWithinDoSiThEx
		 * @param onCompleted optional callback invoked with the final result and a boolean indicating whether this [[Commitment]] was already completed.
		 *                    If `true`, the result is a previously set one; if `false`, the result is the one provided here.
		 */
		inline def complete(result: Try[A], isWithinDoSiThEx: Boolean = isInSequence, onCompleted: (Try[A], Boolean) => Unit = (_, _) => ()): this.type = {
			if isWithinDoSiThEx then completeUnsafe(result, onCompleted)
			else {
				execute(completeUnsafe(result, onCompleted))
				thisCommitment
			}
		}

		/** Completes this [[Commitment]] with the given `result`, unless it has already been completed.
		 *
		 * If this [[Commitment]] is not yet completed, the provided `result` becomes its final value and is made immediately visible to all subscribers.
		 * If it is already completed, the provided `result` is ignored.
		 *
		 * @note CAUTION: must be called from within this [[Doer]].
		 * @param result the value to complete this [[Commitment]] with.
		 * @param onCompleted optional callback invoked synchronously (before this method returns) with the final result and a boolean indicating whether this [[Commitment]] was already completed.
		 *                    If `true`, the result is a previously set one; if `false`, the result is the one provided here.
		 */
		def completeUnsafe(result: Try[A], onCompleted: (Try[A], Boolean) => Unit = (_, _) => ()): this.type = {
			checkWithin()
			oResult.fold {
				oResult = Maybe(result)
				// First run the consumers in subscriptions order
				if firstOnCompleteObserver ne null then {
					try firstOnCompleteObserver(result)
					catch {
						case NonFatal(e) => reportPanicException(e)
					}
					firstOnCompleteObserver = null // unbind the observer reference to help the garbage collector
					if onCompletedObservers.nonEmpty then {
						def loop(head: Try[A] => Unit, tail: List[Try[A] => Unit]): Unit = {
							if tail.nonEmpty then loop(tail.head, tail.tail)
							try head(result)
							catch {
								case NonFatal(e) => reportPanicException(e)
							}
						}

						loop(this.onCompletedObservers.head, this.onCompletedObservers.tail)
						this.onCompletedObservers = Nil // Clean the observers list to help the garbage collector.
					}
				}
				// Finally call the provided call-back.
				try onCompleted(result, false)
				catch {
					case NonFatal(e) => reportPanicException(e)
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
		 * This method delegates to [[completeUnsafe]], scheduling it within this [[Doer]]'s sequential executor if not already executing within it.
		 *
		 * @param result the value to complete this [[Commitment]] with.
		 * @param isWithinDoSiThEx $isWithinDoSiThEx
		 * @param onCompleted optional callback invoked with the final result and a boolean indicating whether this [[Commitment]] was already completed.
		 *                    If `true`, the result is a previously set one; if `false`, the result is the one provided here.
		 */
		inline def fulfill(result: A, isWithinDoSiThEx: Boolean = isInSequence, onCompleted: (Try[A], Boolean) => Unit = (_, _) => ()): this.type = {
			if isWithinDoSiThEx then completeUnsafe(Success(result), onCompleted)
			else {
				execute(completeUnsafe(Success(result), onCompleted))
				this
			}
		}

		/** Breaks this [[Commitment]] with the given `excuse`, unless it has already been completed at the time the fulfillment is performed.
		 *
		 * Fulfillment is performed:
		 * - Synchronously (before this method returns) if `isWithinDoSiThEx` is true.
		 * - Asynchronously as soon as possible otherwise.
		 *
		 * This method delegates to [[completeUnsafe]], scheduling it within this [[Doer]]'s sequential executor if not already executing within it.
		 *
		 * @param excuse the [[Failure]] to break this [[Commitment]] with.
		 * @param isWithinDoSiThEx $isWithinDoSiThEx
		 * @param onCompleted optional callback invoked with the final result and a boolean indicating whether this [[Commitment]] was already completed.
		 *                    If `true`, the result is a previously set one; if `false`, the result is the one provided here.
		 */
		inline def break(excuse: Throwable, isWithinDoSiThEx: Boolean = isInSequence, onCompleted: (Try[A], Boolean) => Unit = (_, _) => ()): this.type = {
			if isWithinDoSiThEx then completeUnsafe(Failure(excuse), onCompleted)
			else {
				execute(completeUnsafe(Failure(excuse), onCompleted))
				this
			}
		}

		/** Wires this [[Commitment]] to the completion of a [[LatchedTask]].
		 *
		 * Arranges this [[Commitment]] to be completed if `completingTask` completes, unless it was completed before.
		 * Always one, and only one, of the two callback is invoked:
		 *		- `onAlreadyCompleted` if this [[Commitment]] was already completed when the subscription is done.
		 *		- `onCompletedLater` if this [[Commitment]] is completed after the subscription is done.
		 * The subscription is synchronic if `isWithinDoSiThEx` is true, and asynchronic ASAP otherwise.
		 *
		 * @param completingTask the [[Task]] whose result will be used to complete this [[Commitment]].
		 * @param isWithinDoSiThEx informs if this method was called within this [[Doer]].
		 * @param onCompletedLater invoked (within this [[Doer]]) when this [[Commitment]] is completed, but only if that happens after the subscription is done. The boolean parameter tells whenever this [[Commitment]] was already completed when `completingTask` completes. `false` means the completión was due to the completion of `completingTask`. `true` means the completion was due to other means.
		 * @param onAlreadyCompleted invoked (within this [[Doer]]) if this [[Commitment]] was already fulfilled when the subscription is done. The subscription is immediate when `isWithinDoSiThEx` is true, and deferred otherwise.
		 * @throws IllegalArgumentException if `completingTask` is the same instance as this [[Commitment]].
		 */
		def completeWith(completingTask: Task[A], isWithinDoSiThEx: Boolean = isInSequence, onCompletedLater: (Try[A], Boolean) => Unit = (_, _) => (), onAlreadyCompleted: Try[A] => Unit = _ => ()): this.type = {
			if completingTask eq this then throw IllegalArgumentException("A Commitment can't be fulfilled with itself.")
			if isWithinDoSiThEx then oResult.fold(completingTask.engagePortal(result => completeUnsafe(result, onCompletedLater)))(onAlreadyCompleted)
			else execute(completeWith(completingTask, true, onCompletedLater, onAlreadyCompleted))
			this
		}
	}

	/** Creates an already completed [[Commitment]].
	 * @param immediateResult the immediate result that this [[LatchedTask]] yields. */
	def Commitment_ready[A](immediateResult: Try[A]): Commitment[A] =
		Commitment(Maybe(immediateResult))

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
		task.trigger(isWithinDoSiThEx)(result => commitment.completeUnsafe(result, onCompleted))
		commitment
	}

	/** A fence that enforces causal ordering and may become stuck; once stuck, progression halts: subsequent update attempts are skipped and the returned [[LatchedTask]] yield the same halting state.
	 *
	 * Each state transition attempt is causally anchored to the previous one, ensuring that each update observes the latest visible state, and that all updates are fulfilled in causal order. Speculative updates may be rolled back before becoming visible.
	 *
	 * Differs from [[CausalFence]] in that it may become stuck on specific states. Once a stuckable state is committed (currently [[Failure]]), the fence halts: subsequent transition attempts are skipped and yield the same stuck state. A transition to a stuck state becomes the final committed state, and no further transitions are accepted.
	 *
	 * Core idea: CausalStuckableFence[A] is a causal sequencing primitive with stuckness semantics. It maintains a single tail commitment ([[lastEnqueuedCommitment]]) that represents the latest step in a causal chain. Each [[advance]] and [[causalAnchor]] call enqueues a new [[Commitment]], chained to the previous one, unless the fence is already stuck.
	 *
	 * This fence provides **causal fulfillment semantics with stuckness**:
	 * - Each successful transition yields a committed, visible state. Rolled-back transitions fulfill with the previous state.
	 * - The causal chain is never broken: all updates are fulfilled and causally ordered until a stuck state is reached.
	 * - The committed lineage includes all transitions to non-stuck states, and ends in a stuck one.
	 * - If a transition to a stuck state occurs, the stuck state is committed and the fence halts — subsequent update attempts are skipped and yield the same stuck state.
	 *
	 * Fundamental Invariants
	 * - Single updater invariant: At most one `primaryStateUpdater` (the function passed to [[advance]]) is in progress at a time. Subsequent advances wait until the previous one completes.
	 * - Advance ordering invariant: The `primaryStateUpdater` passed to [[advance]] runs after all consumers that subscribed to the previous [[Commitment]] in the queue (via earlier [[advance]] or [[causalAnchor]]) have completed their synchronous part.
	 * - Anchor freshness invariant: A consumer subscribed to [[causalAnchor]] observes the same state that a `primaryStateUpdater` would receive if [[advance]] were invoked at that moment. Ordering is guaranteed relative to the last completed advance at subscription time, but not relative to advances invoked later. If the fence is stuck, the anchor yields the stuck state deterministically.
	 * - Stuckness invariant: Once a stuck state is committed, no further transitions are accepted. All subsequent advances or anchors yield the same stuck state.
	 * - Rollback integrity invariant: For speculative advances, derived updates must not be composed into the primary updater, otherwise rollback semantics are broken.
	 *
	 * Invariants related to derived state:
	 * - Game-changing invariant: Immediately after an [[advance]] or [[causalAnchor]] call (if not stuck), there are no pending advances other than the one just created. The returned [[Commitment]] (seen as [[LatchedTask]]) is a fresh tail. Any immediate synchronous subscription to it is guaranteed to be the first subscriber in its list. Therefore, when the commitment fulfills, that consumer sees the up‑to‑date state deterministically. This invariant eliminates the race where another [[advance]] could sneak in and publish a newer state before or while the synchronous consumer runs. The consumer is deterministically ordered before any updater that follows in the causal chain.
	 * - Anchor specificity invariant: Derived updates that require strict ordering relative to a particular primary transition must anchor to that transition’s [[Commitment]] (the one returned by [[advance]]), not to a generic tail snapshot (as returned by [[causalAnchor]]).
	 * - Deterministic derived ordering invariant: When derived updates have dependencies, their execution order must be enforced by anchoring the dependent update and deriving prerequisites synchronously from the anchored state, or by composing into the primary updater if not speculative.
	 * - Rollback integrity invariant: No derived side-effect may commit externally during a speculative advance before the primary step is safely committed; otherwise rollback can leave the system in an inconsistent state.
	 * - Idempotence/compensation invariant: Any derived side-effect that can be re-run or rolled back must be idempotent or have a compensating action to preserve causal correctness under retries or rollback.
	 *
	 * Invariants inherited from [[Commitment]]:
	 * - Sequential consumer invariant: The [[LatchedTask]] returned by [[advance]] and [[causalAnchor]] is a [[Commitment]] and therefore the subscribed consumers are invoked in registration order. The synchronous part of each consumer runs to completion before the next begins.
	 *
	 * @param initialState the initial state, already visible and committed (may be stuck if it is a stuckable state).
	 */

	class CausalStuckableFence[A](initialState: Try[A]) {
		private var lastCommittedCommitment: Commitment[A] = Commitment_ready(initialState)
		private var lastEnqueuedCommitment: Commitment[A] = lastCommittedCommitment

		/** Provides rollback capability for speculative updates.
		 *
		 * A [[RollbackAccessor]] is passed to speculative state transitions to allow them to cancel themselves before becoming visible.
		 * Rollback is only effective if invoked before the update is committed.
		 * If rollback is invoked too late, the accessor still complete the [[LatchedTask]] with the committed state
		 * and signals that rollback was ineffective.
		 */
		trait RollbackAccessor {
			/** Attempts to roll back the speculative update, restoring the previous visible state.
			 *
			 * The rollback is applied if "invoked" before the update it targets has completed.
			 * The quotes around "invoked" reflect that, when `isWithinDoSiThEx` is false, the rollback attempt is scheduled asynchronously and may race with the update's completion.
			 *
			 * Regardless of timing, the [[LatchedTask]] originally returned by [[advanceSpeculatively]] is fulfilled:
			 * - With the previous state if rollback succeeds
			 * - With the committed state if rollback was too late
			 *
			 * The callback receives the resulting state and a flag indicating whether rollback was applied (`false`) or too late (`true`).
			 *
			 * @param isWithinDoSiThEx whether the caller is executing within the Doer's sequential executor
			 * @param onCompleted a callback invoked with the resulting state and a flag indicating whether rollback was too late
			 * @return the [[LatchedTask]] originally returned by [[advanceSpeculatively]], now fulfilled with either the committed or rolled-back state
			 */
			def rollback(isWithinDoSiThEx: Boolean = isInSequence, onCompleted: (Try[A], Boolean) => Unit = (_, _) => ()): LatchedTask[A]
		}

		/** Provides the tail of the causal chain at call time.
		 *
		 * Unlike [[causalAnchor]], this method does not guarantee deterministic observation of the up‑to‑date state. Subscribers to the returned [[LatchedTask]] will observe the progression until that tail, but synchronous [[advance]]s that install a newer tail before the subscriber executes are not observed.
		 *
		 * @return a [[LatchedTask]] representing the tail of the causal chain at call time.
		 */
		def causalChainTail(isWithinDoSiThEx: Boolean = isInSequence): LatchedTask[A] = {
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
		inline def committedState: Try[A] = lastCommittedCommitment.maybeResult.get

		/** Returns a [[LatchedTask]] that yields the currently visible state or failure.
		 *
		 * This reflects the state to which the most recent step transitioned into, or a [[Failure]] if the transition failed.
		 * Rolled-back transitions yield the previous state.
		 * The returned [[LatchedTask]] is always already completed.
		 *
		 * @param isWithinDoSiThEx whether the caller is executing within the Doer's sequential executor
		 * @return a [[LatchedTask]] yielding the currently visible state or failure.
		 */
		def committed(isWithinDoSiThEx: Boolean = isInSequence): LatchedTask[A] = {
			if isWithinDoSiThEx then lastCommittedCommitment
			else {
				val wrapper = Commitment[A]()
				execute(wrapper.completeWith(lastCommittedCommitment, true))
				wrapper
			}
		}

		/**
		 * Returns a [[LatchedTask]] that yields the same state an updater would see if [[advance]] were invoked at this moment.
		 * This provides a causal checkpoint suitable for synchronous consumers that need to derive state deterministically.
		 *
		 * The returned [[LatchedTask]] is backed by a fresh [[Commitment]] that forwards from the current tail [[Commitment]].
		 * This ensures that immediate synchronous subscriptions to the returned [[LatchedTask]] are registered before completion, making them the first subscribers on the fresh [[Covenant]] and guaranteeing deterministic observation of the up‑to‑date state.
		 *
		 * **Temporal window of causal safety:**
		 * The causal guarantee holds only during the synchronous execution of a consumer subscribed to the returned [[LatchedTask]].
		 * Calls to methods that rely on causal visibility are safe only within the body of that consumer; once the consumer has returned, deferred or later code is no longer causally anchored.
		 *
		 * @note When derived updates (those done to secondary state that derives from the primary state) have causal dependencies among themselves, you must enforce deterministic order by other means: use causal derivation functions (anchor only the dependent update and derive prerequisites synchronously from the anchored state), or, if derived updates are fast and the advance is not speculative, compose them into the `primaryStateUpdater` passed to [[advance]] or [[advanceIf]]. Composition is not safe for speculative advances, because rollback during the derived update phase could succeed despite it shouldn’t.
		 *
		 * Independent subscriptions to [[causalAnchor]] are appropriate only for derived updates that are order‑independent.
		 *
		 * @param onCompletedLater optional call-cack that is invoked only if a transition was in progress (the tail [[Commitment]] is pending) when this method is called. Useful to discriminate the timing. The boolean parameter is always false and should be ignored. This call-back is called within this [[Doer]] sequential executor before any subscriber to the returned [[LatchedTask]]. Mutually exclusive with the other call-back parameter.
		 * @param onAlreadyCompleted optional call-back tha is invoked only if the last transition is complete when this method is called. Useful to discriminate the timing. This call-back is called within this [[Doer]] sequential executor before any subscriber to the returned [[LatchedTask]]. Mutually exclusive with the other call-back parameter.
		 * @return a [[LatchedTask]] yielding the state that the next update will be causally anchored to — i.e. the same state an updater would see if [[advance]] were called at this moment.
		 */
		def causalAnchor(onCompletedLater: (Try[A], Boolean) => Unit = (_, _) => (), onAlreadyCompleted: Try[A] => Unit = _ => ()): LatchedTask[A] = {
			checkWithin()
			val previousStepCommitment = lastEnqueuedCommitment
			val thisStepCommitment = Commitment[A]()
			lastEnqueuedCommitment = thisStepCommitment
			thisStepCommitment.completeWith(previousStepCommitment, true, onCompletedLater, onAlreadyCompleted)
		}


		/** Attempts a deterministic asynchronous state transition causally anchored to the previous one.
		 *
		 * If this [[CausalStuckableFence]] is stuck, because a previous update failed, the provided updater is not executed and the returned [[LatchedTask]] is completed with the same failure.
		 *
		 * Is worth mentioning that the provided function will be executed after all the consumers subscribed (before the call to this method) to both, the [[LatchedTask]] returned by both [[causalAnchor]] and the one returned by previous calls to [[advance]]-like methods, have completed.
		 *
		 * Caution: The execution of consumers that are subscribed to obsolete instances of [[LatchedTask]] is not causally ordered.
		 * So, avoid memorizing the [[LatchedTask]] instances returned by [[causalAnchor]] or [[advance]]-like methods; always subscribe to the instance returned by [[causalAnchor]] to ensure causal ordering of the consumers executions.
		 * Obsolete are those instances returned by methods of this [[CausalStuckableFence]] before the last call to an [[advance]]-like method).
		 *
		 * The updater function is defined with a second parameter of type `Null` to match the internal speculative signature, allowing reuse without introducing an extra closure.
		 * Rollback is not supported in this method.
		 *
		 * **Temporal window of causal safety:**
		 * The causal guarantee holds from the moment the `primaryStateUpdater` function is invoked until the [[LatchedTask]] it returns has completed.
		 * Calls to methods that rely on causal visibility are safe only within this window; after completion, the causal fence is closed and subsequent calls are no longer anchored.
		 *
		 * @param primaryStateUpdater a function that computes the next state from the current one
		 * @param isWithinDoSiThEx whether the caller is executing within the Doer's sequential executor
		 * @return a [[LatchedTask]] that will be fulfilled with the new state once the update completes, or the previous failure due to which the update was skipped.
		 */
		def advance(primaryStateUpdater: (A, Null) => Maybe[Task[A]], isWithinDoSiThEx: Boolean = isInSequence): LatchedTask[A] = {
			val suAdapter = primaryStateUpdater.asInstanceOf[(A, RollbackAccessor | Null) => Maybe[LatchedTask[A]]]
			if isWithinDoSiThEx then step(suAdapter, false)
			else {
				val commitment = Commitment[A]()
				execute(commitment.completeWith(step(suAdapter, false), true))
				commitment
			}
		}

		/** Attempts a speculative state transition anchored to the previous one.
		 *
		 * If the previous step failed, this transition is skipped and the returned [[LatchedTask]] is completed with the same failure.
		 * If rollback is invoked before visibility, the update is canceled and the previous state is kept.
		 * Only successful transitions update the committed state.
		 *
		 * @param primaryStateUpdater a function that computes the next state from the current one, with rollback capability
		 * @param isWithinDoSiThEx whether the caller is executing within the Doer's sequential executor
		 * @return a [[LatchedTask]] that will be completed with the new state if not rolled-back in time, the previous state if rolled-back in time, or the previous failure due to which the update was skipped.
		 */
		def advanceSpeculatively(primaryStateUpdater: (A, RollbackAccessor) => Maybe[Task[A]], isWithinDoSiThEx: Boolean = isInSequence): LatchedTask[A] = {
			if isWithinDoSiThEx then step(primaryStateUpdater, true)
			else {
				val covenant = Commitment[A]()
				execute(covenant.completeWith(step(primaryStateUpdater, true), true))
				covenant
			}
		}

		/** Internal method that performs the actual state transition.
		 *
		 * Handles both deterministic and speculative updates depending on the `isSpeculative` flag.
		 * If the previous step failed, the update is not executed and the [[LatchedTask]] corresponding to this step
		 * is completed with the same failure. The rollback accessor is instantiated only when needed to avoid unnecessary allocations.
		 * Only successful transitions update the committed state.
		 *
		 * @param primaryStateUpdater the transition function, optionally accepting a [[RollbackAccessor]]
		 * @param isSpeculative whether the update is speculative and may be rolled back
		 * @return a [[LatchedTask]] that will be completed with the new state if not rolled-back in time, the previous state if rolled-back in time, or the previous failure due to which the update was skipped.
		 */
		private def step(primaryStateUpdater: (A, RollbackAccessor | Null) => Maybe[Task[A]], isSpeculative: Boolean): LatchedTask[A] = {
			val previousStepCommitment = lastEnqueuedCommitment
			val thisStepCommitment = Commitment[A]()
			lastEnqueuedCommitment = thisStepCommitment

			previousStepCommitment.subscribe {
				case success@Success(previousState) =>
					val rba =
						if isSpeculative then new RollbackAccessor {
							override def rollback(isWithinDoSiThEx: Boolean = isInSequence, onCompleted: (Try[A], Boolean) => Unit): LatchedTask[A] =
								thisStepCommitment.fulfill(previousState, isWithinDoSiThEx, onCompleted)
						} else null
					primaryStateUpdater(previousState, rba)
						.fold {
							lastCommittedCommitment = thisStepCommitment
							thisStepCommitment.completeUnsafe(success)
						} {
							_.engagePortal { thisStepResult =>
							lastCommittedCommitment = thisStepCommitment
								thisStepCommitment.completeUnsafe(thisStepResult)
						}
					}
				case Failure(e) =>
					thisStepCommitment.break(e, true)
			}
			thisStepCommitment
		}
	}

	//// Coalesced Inquire/Query ////

	/** Coalesces in-flight inquires. */
	class CoalescedInquire[P, R](inquire: P => LatchedDuty[R]) {
		private val inFlight: mutable.Map[P, LatchedDuty[R]] = mutable.Map.empty

		def getOrStart(params: P, isWithinDoer: Boolean = isInSequence): LatchedDuty[R] = {
			if isWithinDoer then inFlight.getOrElse(params, inquire(params).andThen(_ => inFlight.remove(params)))
			else {
				val covenant = Covenant[R]()
				execute {
					covenant.fulfillWith(getOrStart(params, true))
				}
				covenant
			}
		}
	}

	/** Coalesces in-flight queries. */
	class CoalescedQuery[P, R](query: P => LatchedTask[R]) {
		private val inFlight: mutable.Map[P, LatchedTask[R]] = mutable.Map.empty

		def getOrStart(params: P, isWithinDoer: Boolean = isInSequence): LatchedTask[R] = {
			if isWithinDoer then {
				inFlight.get(params) match {
					case Some(lt) =>
						lt
					case None =>
						try {
							val lt = query(params)
							inFlight.put(params, lt)
							lt.andThen(_ => inFlight.remove(params))
							lt
						} catch {
							case NonFatal(e) => LatchedTask_ready(Failure(e))
						}
				}
			} else {
				val commitment = Commitment[R]()
				execute {
					commitment.completeWith(getOrStart(params, true))
				}
				commitment
			}
		}
	}

	//////////////// Pure ////////////////////

	/** A [[Duty]] whose result does not depend on the execution. TODO find a better name */
	type PureDuty[+A] = Duty[A]

	/** A [[Task]]  whose result does not depend on the execution. */
	type PureTask[+A] = Task[A]

	//////////////// Dirty ////////////////////

	/** A [[Duty]] that is expected to produce a different result at each execution. TODO find a better name */
	type DirtyDuty[+A] = Duty[A]

	/** A [[Task]] that is expected to produce a different result at each execution. */
	type DirtyTask[+A] = Task[A]


	//////////////// Stream ////////////////////

	/** A [[Duty]] that produces more than one result (calls [[Duty.engage]] multiple times. */
	type StreamDuty[+A] = Duty[A]

	/** A [[Task]] that produces more than one result (calls [[Duty.engage]] multiple times. */
	type StreamTask[+A] = Task[A]

	def StreamDuty_source[A]: Source[A] = new Source[A]


	final class Source[A] extends AbstractDuty[A] with Subscriptable[A] {

		override protected def engage(onComplete: A => Unit): Unit =
			subscribe(onComplete)

		def push(a: A): Unit = {
			// Run the consumers in subscriptions order
			if firstOnCompleteObserver ne null then {
				firstOnCompleteObserver(a)
				firstOnCompleteObserver = null // Nullify the reference to help the garbage collector.
			}
			if this.onCompletedObservers.nonEmpty then {
				def loop(head: A => Unit, tail: List[A => Unit]): Unit = {
					if tail.nonEmpty then loop(tail.head, tail.tail)
					head(a)
				}

				loop(this.onCompletedObservers.head, this.onCompletedObservers.tail)
				this.onCompletedObservers = Nil // Clean the observers list to help the garbage collector.
			}
		}

		inline def intoDuty: Duty[A] = this
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


	//////////////// Subscriptable ////////////////////

	trait Subscriptable[A] {
		protected var firstOnCompleteObserver: (A => Unit) | Null = null
		protected var onCompletedObservers: List[A => Unit] = Nil

		/**
		 * Subscribes a consumer of the result of this producer.
		 *
		 * The subscription is automatically removed after and execution of this producer has completed and the received consumer is executed.
		 *
		 * The provided consumer is schedule to run upon completion in subscription orden (after sequentially running all the previously subscribed consumers).
		 *
		 * @note CAUTION: This method does not prevent duplicate subscriptions.
		 * @note CAUTION: Must be called within the $DoSiThEx */
		def subscribe(consumer: A => Unit): Unit = {
			checkWithin()
			if firstOnCompleteObserver eq null then firstOnCompleteObserver = consumer
			else onCompletedObservers = consumer :: onCompletedObservers
		}

		/**
		 * Removes a subscription done with [[subscribe]].
		 *
		 * @note CAUTION: Must be called within the $DoSiThEx */
		def unsubscribe(onComplete: A => Unit): Unit = {
			checkWithin()
			if firstOnCompleteObserver eq onComplete then {
				if onCompletedObservers.isEmpty then firstOnCompleteObserver = null
				else {
					firstOnCompleteObserver = onCompletedObservers.head
					onCompletedObservers = onCompletedObservers.tail
				}
			} else onCompletedObservers = onCompletedObservers.filterNot(_ ne onComplete)
		}


		/** @note CAUTION: Must be called within the $DoSiThEx
		 * @return `true` if the provided consumer is currently subscribed. */
		def isSubscribed(onComplete: A => Unit): Boolean = {
			checkWithin()
			(firstOnCompleteObserver eq onComplete) || onCompletedObservers.exists(_ eq onComplete)
		}
	}
}