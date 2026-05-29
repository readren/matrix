package readren.sequencer

import Doer.*

import readren.common.*

import scala.annotation.{publicInBinary, tailrec, targetName, threadUnsafe}
import scala.collection.IterableFactory
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.reflect.ClassTag
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object Doer {

	type ExecutionSerial = Int

	val assertionsEnabled: Boolean = classOf[Doer].desiredAssertionStatus()

	/** Wraps exception passed to [[Doer.reportFailure]] when [[Doer.reportPanicException]] is called.
	 * [[Doer.reportPanicException]] is called by [[Doer.ownSingleThreadExecutionContext.reportFailure]], few [[Doer.Venture]] operations like [[Doer.Venture.andThen]] that can't propagate failures, and most [[Doer.Covenant]]/[[Doer.Commitment]] operations. */
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
	/** Completed by the [[Doer.Covenant]]/[[Doer.Commitment]] completion method to which the `onCompleted` call-back that received this constant was provided. */
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
 * A [[Doer]] encloses computational primitives, enforcing sequential execution of them.
 * This sequentiality is scoped to the primitives enclosed by the same instance of [[Doer]]. Specifically:
 *  - Primitives created by the same [[Doer]] instance will execute sequentially relative to each other.
 *  - Primitives created by different [[Doer]] instances are independent and may execute concurrently or in any order.
 *
 * == Execution of Routines ==
 * All routines (functions, procedures, predicates, or by-name parameters) passed to the primitive operations (including callbacks like `onComplete`) are also executed sequentially relative primitives enclosed by the same '''Doer''' instance. This ensures that all operations associated with a single '''Doer''' instance maintain sequential consistency, unless explicitly documented otherwise in the method's documentation.
 *
 * == Key Points ==
 * - Sequential execution is instance-specific: Each [[Doer]] instance manages its own sequence of actions.
 * - Routines passed to primitive's operations are executed in the same sequential scope as the [[Doer]] instance that owns the primitive.
 * - Primitives across different [[Doer]] instances are '''independent''' and may run concurrently.
 * ==Note:==
 * See [[Doer.executeSequentially()]].
 *
 * @define DoSerEx DoSerEx (doer's serial executor)
 * @define onCompleteExecutedByDoSerEx The `onComplete` callback passed to `engage` is always, with no exception, executed by this $DoSerEx. This is part of the contract of the [[Venture]] trait.
 * @define threadSafe This method is thread-safe.
 * @define isExecutedByDoSerEx This function is executed within the DoSerEx (doer's serial executor).
 * @define unhandledErrorsArePropagatedToVentureResult The call to this routine is guarded with try-catch. If it throws a non-fatal exception it will be caught and the [[Venture]] will complete with a [[Failure]] containing the error.
 * @define unhandledErrorsAreReported The call to this routine is guarded with a try-catch. If the evaluation throws a non-fatal exception it will be caught and reported with [[Doer.reportFailure()]].
 * @define notGuarded CAUTION: The call to this function is NOT guarded with a try-catch. If its evaluation terminates abruptly the duty will never complete. The same occurs with all routines received by [[Duty]] operations. This is one of the main differences with [[Venture]] operation.
 * @define maxRecursionDepthPerExecutor Maximum recursion depth per executor. Once this limit is reached, the recursion continues in a new executor. The result does not depend on this parameter as long as no [[java.lang.StackOverflowError]] occurs.
 * @define isWithinDoSerEx indicates whether the call to this method is within this [[Doer]]'s sequential executor. If there is no such certainty the call site should either, not specify a value in order to use the default (which is the result of [[Doer.isInSequence]]), or specify `false` to force deferred execution.
 * @define suppressSyntheticCompanionObject Suppresses the generation of the synthetic companion object. This dummy definition creates a name collision to prevent the compiler from generating a module for universal apply, thereby avoiding the bytecode overhead of a lazy-initialized nested module. By requiring a [[Nothing]] parameter, this method is made uncallable, ensuring any inadvertent use is caught at compile-time.
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
	 * From now on the executor of the queued [[Runnable]] instances will be called "the doer's serial executor", or DoSerEx for short, despite more than one thread may be involved.
	 * If the call is executed within the current DoSerEx's [[Thread]], the [[Runnable]]'s execution must not start until the DoSerEx completes its current execution and all the previously queued ones.
	 * The implementation should not throw non-fatal exceptions.
	 * The implementation should be thread-safe.
	 *
	 * All the deferred actions preformed by the [[Duty]] and [[Venture]] operations are executed by calling this method unless the particular operation documentation says otherwise. That includes not only the call-back functions like `onComplete` but also all the functions, procedures, predicates, and by-name parameters they receive.
	 * @note Implementations must set their associated provider's thread-local to `this` before invoking `body`, and clear it (restore to `null`) once `body` returns or throws. Failure to uphold this contract will cause [[DoerProvider.currentDoer]] to return a value of the wrong type at runtime, as the cast in that method relies on it. */
	def executeSequentially(runnable: Runnable): Unit

	/** The [[ExecutionSerial]] of the most recently executed [[Runnable]] on this [[Doer]].
	 *
	 * This serial is incremented immediately before each [[Runnable]] passed to [[executeSequentially]] is run.
	 * It enables those [[Runnable]]s to observe their relative execution order and distinguish whether two operations occur during the same or different executions.
	 */
	def currentExecutionSerial: ExecutionSerial

	/**
	 * The implementation should return the [[Doer]] instance that the current [[java.lang.Thread]] is currently running if it knows it, or null if it doesn't.
	 * The implementation should know, at least, if the current [[java.lang.Thread]] corresponds to this [[Doer]], and return this instance in that case. */
	def currentlyRunningDoer: Maybe[Doer]

	/**
	 * @return true if the current [[java.lang.Thread]] is the one that is currently assigned to this [[Doer]]. Calling this method within the thread with which the [[Runnable]]s passed to this instance's [[executeSequentially]] or [[run]] methods is executed, always returns `true`. */
	inline def isInSequence: Boolean = currentlyRunningDoer.value eq thisDoer

	/** Asserts that the current [[java.lang.Thread]] is the one that is currently assigned to this [[Doer]] instance for sequential execution of its operations. */
	inline def checkWithin(): Unit = {
		if Doer.assertionsEnabled && !isInSequence then throw new AssertionError(checkWithinMsg())
	}

	final def checkWithinMsg(): String = s"The current thread does not correspond to this Doer: expected=${thisDoer.tag}, current=${currentlyRunningDoer.fold("unknown")(_.tag)}."

	/**
	 * Called by few [[Venture]] and most [[Commitment]] operations when an operand function terminates abruptly and the nature of the operation does not allow to propagate the failure to the result.
	 * Examples of such operations are [[Venture.andThen]], [[Venture.triggerAndForgetHandlingErrors]], [[Venture_wait]], [[Venture_alien]], and [[Commitment.completeUnsafe]].
	 * The implementation should report the received [[Throwable]] somehow. Preferably including a description that identifies the provider of the DoSerEx used by [[executeSequentially]] and mentions that the error was thrown by a deferred procedure programmed by means of a [[Venture]].
	 * The implementation should not throw non-fatal exceptions.
	 * This method is called within the thread assigned to this [[Doer]].
	 * */
	protected def reportFailure(cause: Throwable): Unit

	private[sequencer] inline final def reportFailurePortal(cause: Throwable): Unit = reportFailure(cause)

	/**
	 * Queues an execution of the specified procedure in the tasks-queue of this $DoSerEx. See [[Doer.executeSequentially]]
	 * If the call is executed by the $DoSerEx the [[Runnable]]'s execution will not start until the DoSerEx completes its current execution and gets free to start a new one.
	 *
	 * All the deferred actions preformed by the [[Duty]]/[[Venture]] operations are executed by calling this method unless the particular operation documentation says otherwise. That includes not only the call-back functions like `onComplete` but also all the functions, procedures, predicates, and by-name parameters they receive as.
	 * This function only makes sense to call:
	 *		- from an action that is not executed by this $DoSerEx (the callback of a [[Future]], for example);
	 *		- or to avoid a stack overflow by continuing the recursion in a new execution.
	 * @see [[submit]] and [[submitHardy]] if the result of the execution is relevant.
	 * @note Is more efficient than the functionally equivalent: `Duty_mine(() => procedure).triggerAndForget()`.
	 */
	inline def run(inline procedure: => Unit): Unit = {
		${ DoerMacros.executeSequentiallyImpl('thisDoer, 'procedure) }
	}

	//// WIRABLE SOFT ////

	/** Abstracts over how a supplier is wired into an effect type `F` within the context of this [[Doer]].
	 *
	 * Instances determine the strategy by which a supplier `() => A` is captured or scheduled into `F[A]`. This may happen eagerly (e.g. queuing execution via [[Doer.run]] immediately) or lazily (e.g. deferring until the effect is engaged).
	 *
	 * @tparam A the type of value produced by the supplier
	 * @tparam F the effect type into which the supplier is wired
	 */
	trait WirableSoft[A, F[_]] {
		/** Wires the given supplier into an instance of `F[A]`.\
		 * The wiring strategy is defined by the implementing instance: it may schedule execution immediately via [[Doer.run]], defer it until the effect is engaged, or apply any other strategy consistent with this [[Doer]]'s execution model.
		 * @param supplier the computation to wire into `F`
		 * @return an `M[A]` that captures or schedules the supplier according to this instance's strategy */
		inline def wire(supplier: () => A): F[A]

		inline def wireFlat(supplier: () => F[A]): F[A]
	}

	/** Wires the given supplier into effect type `F` using the [[WirableSoft]] instance corresponding to `F`. The [[WirableSoft]] instances for effect types defined in [[Doer]] are provided by [[Doer]] itself.
	 * @param supplier the computation to submit
	 * @param wirable the type-class instance that determines how the supplier is wired
	 * @tparam A the type of value produced
	 * @tparam F the effect type into which the supplier is wired
	 * @return an `F[A]` wired according to the [[WirableSoft]] instance in scope */
	inline def submit[A, F[_]](supplier: () => A)(using wirable: WirableSoft[A, F]): F[A] =
		wirable.wire(supplier)

	inline def submitFlat[A, F[_]](supplier: () => F[A])(using wirable: WirableSoft[A, F]): F[A] =
		wirable.wireFlat(supplier)

	inline given [A] =>WirableSoft[A, Duty] {
		override inline def wire(supplier: () => A): Duty[A] =
			new Duty_Mine(supplier)

		override inline def wireFlat(supplier: () => Duty[A]): Duty[A] = {
			new Duty_MineFlat(supplier)
		}
	}

	inline given [A] =>WirableSoft[A, LatchingDuty] {
		override inline def wire(supplier: () => A): LatchingDuty[A] =
			Covenant_mine(supplier)

		override inline def wireFlat(supplier: () => LatchingDuty[A]): LatchingDuty[A] =
			Covenant_mineFlat(supplier)
	}

	//// WIRABLE HARDY ////

	/** Like [[WirableSoft]] but for hardy suppliers (the value is wrapped with [[Try]]). */
	trait WirableHardy[A, F[_]] {
		inline def wire(inline supplier: () => Try[A]): F[A]

		inline def wireFlat(inline supplier: () => F[A]): F[A]
	}

	/** Wires the given supplier into an effect type `F` using the [[WirableSoft]] instance corresponding to `F`. The [[WirableSoft]] instances for effect types defined in [[Doer]] are provided by [[Doer]] itself.
	 * @param supplier the computation to submit
	 * @param wirable the type-class instance that determines how the supplier is wired
	 * @tparam A the type of value produced
	 * @tparam F the effect type into which the supplier is wired
	 * @return an `F[A]` wired according to the [[WirableSoft]] instance in scope
	 */
	inline def submitHardy[A, F[_]](inline supplier: () => Try[A])(using wirable: WirableHardy[A, F]): F[A] = {
		wirable.wire(supplier)
	}

	inline given [A] =>WirableHardy[A, Venture] {
		override inline def wire(inline supplier: () => Try[A]): Venture[A] =
			new Venture_Own(supplier)

		override inline def wireFlat(inline supplier: () => Venture[A]): Venture[A] =
			new Venture_OwnFlat(supplier)
	}

	inline given [A] =>WirableHardy[A, LatchingVenture] {
		override inline def wire(inline supplier: () => Try[A]): LatchingVenture[A] = {
			val commitment = new Commitment[A]
			run(commitment.complete(supplier()))
			commitment
		}

		override inline def wireFlat(inline supplier: () => LatchingVenture[A]): LatchingVenture[A] = {
			val commitment = new Commitment[A]
			run(supplier().engage(tryA => commitment.complete(tryA)))
			commitment
		}
	}

	//// EXCEPTION HANDLING ////

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

	/** A lazy computation owned by this [[Doer]]. Executions are serialized across all [[Duty]] instances of the same [[Doer]]. Each execution may produce a different result if the computation depends on mutable state.\
	 * Executions are performed in the order they were triggered.\
	 * A [[Duty]] can encapsulate one or more chained actions and provides operations to declaratively build complex duties from simpler ones.\
	 * This tool simplifies the implementation of a handler that manages multiple simultaneous processes that interact with each other using a single sequential actor. How? By eliminating the need for state variables that determine the decision-making flow, as the code structure itself indicates the execution order.\
	 * Instances of [[Duty]] whose result is always the same follow the monadic laws. However, if the result depends on the execution (because it depends on mutable variables or time), these laws may be broken.\
	 * For example, if the [[Duty.engage]] implementation closes over mutable variables (either directly or through any of the function operands that its factory or the operations used to construct it receives) from the environment that affects its execution result, then the equality of two supposedly equivalent expressions like {{{duty.flatMap(f).flatMap(g) == duty.flatMap(a => f(a).flatMap(g))}}} could be compromised. This would depend on the timing of when the variables are mutated — specifically when the mutations occur between the start and end of the duty's execution.\
	 * This does not mean that [[Duty.engage]] implementations must avoid closing over mutable variables altogether. Rather, it highlights that if strict adherence to monadic laws is required by your business logic, you should ensure that the mutable variable is not modified during the execution of the involved [[Duty]] instances.\
	 * If the goal is just deterministic behavior, it's sufficient that any closed-over mutable variable is only mutated and accessed by actions executed sequentially in a determined order. This is why the contract enforces serialized execution of actions in the order at which the actions were triggered: to maintain determinism, even when closing over mutable variables, provided they are mutated and accessed solely within the actions in said ordered sequence and those actions are deterministic.\
	 * If you require to ensure monadic laws are followed, use [[LatchingDuty]]/[[LatchingVenture]] instead.\
	 * Design note: [[Duty]] and [[Venture]] are defined as inner traits of [[Doer]] to leverage Scala's path-dependent type checking. This avoids that [[Duty]]/[[Venture]] instances that belong to different [[Doer]] instances to be inadvertently composed together without the adapters needed to ensure sequential execution of the component actions.\
	 * While path-dependent type checking is valuable for enforcing this contract, it has a drawback: the compiler's type-path checks are overly strict, requiring compatible singleton types for references, whereas we only need to verify that the [[Duty]] instances correspond to the same [[Doer]].\
	 * As a result, the compiler may flag type errors in cases where the contract is not violated, which is undesirable.\
	 * The [[castTypePath()]] method mitigates these false positives.\
	 * CAUTION: Unlike [[Venture]], [[Duty]] is strict (non-short-circuiting) and does NOT support failures. And unlike [[Venture]], the invocation of function operands received by its operations is not guarded with a try-catch. Therefore, unlike [[Venture]], any unhandled exception thrown during an execution of a [[Duty]] will break the expected flow and the duty will never complete.\
	 * It is recommended to use [[Venture]] instead of [[Duty]] unless efficiency is a concern.\
	 * @tparam A the type of result obtained when executing this duty. */
	trait Duty[+A] { thisDuty =>
		/** This method performs the actions represented by the duty and calls `onComplete` within the $DoSerEx when the [[Duty]] finishes.\
		 * CAUTION: This method is intended to be used by extensions of [[Duty]] only. Use [[trigger]] or [[foreach]] instead.
		 * The implementation may assume this method is invoked within the $DoSerEx.\
		 * The implementation must respect the following exception-handling rules:
		 * - no exception thrown by the provided callback must be caught.
		 * - any non-fatal exception throw by this method must be either caught and propagated to the result or reported using [[Doer.reportFailure]] if propagation is not feasible.\
		 * In the case of [[Venture]] this includes non-fatal exceptions originated in function operands passed to its factory, including those captured over a closure.
		 * [[Duty]], on the other hand, assumes that function operands never throw exceptions. If an exception is thrown, the stack of the corresponding duty execution will be completely unwound.\
		 * It is crucial to ensure that exceptions thrown by the onComplete callback are not caught, as this could suppress issues within the callback, preventing the execution of code expected to run and making it extremely difficult to diagnose the cause of a never-completing [[Duty]] or [[Venture]].\
		 * This method is the sole primitive operation of this trait; all other methods are derived from it.\
		 * @param onComplete The callback that must be invoked upon the completion of this [[Duty]]. The implementation should call this callback within the $DoSerEx.\
		 * The implementation may assume that `onComplete` will either terminate normally or fatally, but will not throw non-fatal exceptions. */
		def engage(onComplete: A => Unit): Unit

		/** Initiates an execution of this [[Duty]] and subscribes the provided call-back as a consumer of the execution result.
		 * Each invocation of this method triggers a new execution.
		 * Note: Executions triggered on [[Duty]] instances whose completion depends on other executions (e.g., a pending [[Covenant]]) will not complete until those dependent executions have themselves been triggered and completed.
		 * @param isWithinDoSerEx $isWithinDoSerEx
		 * @param onComplete Invoked when the triggered execution completes. The call-back must not throw non-fatal exceptions. It must either terminate normally or fail fatally, but never with a non-fatal exception.
		 * $isExecutedByDoSerEx */
		inline final def trigger(inline isWithinDoSerEx: Boolean = isInSequence)(inline onComplete: A => Unit): Unit = {
			${ DoerMacros.triggerImpl('isWithinDoSerEx, 'thisDoer, 'thisDuty, 'onComplete) }
		}

		/** Triggers an execution of this [[Duty]] ignoring the result.
		 *
		 * $threadSafe
		 *
		 * @param isWithinDoSerEx $isWithinDoSerEx */
		inline final def triggerAndForget(isWithinDoSerEx: Boolean = isInSequence): Unit =
			trigger(isWithinDoSerEx)(_ => {})

		/** Triggers an execution of this [[Duty]] and then invokes the provided consumer passing the result.
		 *
		 * Is equivalent to {{{trigger(isInSequence)(consumer)}}}
		 *
		 * $threadSafe
		 * @param consumer called with this [[Duty]] result when it completes.
		 *
		 * $isExecutedByDoSerEx
		 *
		 * $notGuarded */
		def foreach(consumer: A => Unit): Unit = trigger(isInSequence)(consumer)

		/**
		 * Creates a new [[Duty]] that yields the result of applying the provided function to the result of this [[Duty]].
		 *
		 * $threadSafe
		 *
		 * @param f a function that is applied to the result of this [[Duty]] to produce the final result.
		 *
		 * $isExecutedByDoSerEx
		 *
		 * $notGuarded
		 */
		def map[B](f: A => B): Duty[B] = new Duty_Map(thisDuty, f)

		/**
		 * Creates a new [[Duty]] that yields the result of executing an intermediate [[Duty]] produced by applying the provided function to the final result.
		 *
		 * $threadSafe
		 *
		 * @param f a function that is applied to the result of this [[Duty]] to produce an intermediate [[Duty]] that is then executed to produce the final result.
		 *
		 * $isExecutedByDoSerEx
		 *
		 * $notGuarded
		 */
		inline def flatMap[B](f: A => Duty[B]): Duty[B] = new Duty_FlatMap(thisDuty, f)

		/** Creates a new [[Duty]] that yields exactly the same result (same identity) as this [[Duty]] but executes the provided side-effecting function before yielding it.
		 *
		 * $threadSafe
		 *
		 * @param sideEffect a function that is applied to the result of this [[Duty]] for its side effects.
		 */
		def andThen(sideEffect: A => Unit): Duty[A] = new Duty_AndThen[A](thisDuty, sideEffect)

		/** Creates a new always successful [[Venture]] that shields the result of this [[Duty]].
		 *
		 * This operation allows a [[Duty]] to participate in a [[Venture]]'s short-circuiting context without itself being a short-circuit trigger.
		 *
		 * Together with [[reconcile]] this method allow to mix [[Duty]] and [[Venture]] operations in the same chain.
		 * @return a [[Venture]] whose result is the result of this [[Duty]] wrapped inside a [[Success]]. */
		def succeed: Venture[A] = new Venture_fromDuty(thisDuty)

		/** Starts an execution of this [[Duty]] and returns a successful [[Future]] that yields the result. */
		def toFutureHardy(isWithinDoSerEx: Boolean = isInSequence): Future[A] = {
			val promise = Promise[A]()
			trigger(isWithinDoSerEx)(a => promise.success(a))
			promise.future
		}

		/**
		 * Wraps this [[Duty]] into another that belongs to another [[Doer]].
		 * Useful to chain [[Duty]]'s operations that involve different [[Doer]] instances.
		 * ===Detailed behavior===
		 * Returns a [[Duty]] that belongs to the provided [[Doer]]. When it is triggered, it will trigger this duty within this [[Doer]] and, when completed, make the returned [[Duty]] to yield the result.
		 * CAUTION: Avoid closing over the same mutable variable from two operand functions applied to [[Duty]] instances belonging to different [[Doer]]s.
		 * Remember that all function operands provided to [[Venture]] methods are executed within the [[Doer]] that owns it.
		 * Therefore, calling [[trigger]] on the returned [[Duty]] will execute the `onComplete` passed to it within the `otherDoer`.
		 *
		 * $threadSafe
		 *
		 * @param otherDoer the [[Doer]] to which the returned [[Duty]] will belong.
		 */
		def onBehalfOf(otherDoer: Doer): otherDoer.Duty[A] =
			otherDoer.Duty_foreign(thisDoer)(this)

		/** Casts the singleton type of the [[Doer]] instance that owns this [[Duty]] to the singleton-type of the provided [[Doer]].
		 * This operation does nothing at runtime. It only tricks the compiler to prevent it from complaining when operating with references to the same [[Doer]] instance but through different type-paths.
		 * CAUTION: Use it only if you are sure that the provided [[Doer]] instance is the one that owns this [[Duty]].
		 *
		 * Design note: It was decided to make [[Duty]] (and [[Venture]]) an inner class of the [[Doer]] to take advantage of type-path checking to detect when the contract "all operand functions passed to [[Duty]] (and [[Venture]]) operations owned by the same [[Doer]] are executed in sequence" might be violated, at compile time.
		 * Using type-path checking to detect contract violations is very valuable, but it comes at a cost, because the type-path check done by the compiler is stricter than necessary -- it checks that the singleton type of the references involved be compatible, and we only need to check that the involved [[Duty]] instances belong to the same [[Doer]] instance.
		 * Therefore, the compiler will report type errors in situations the contract is not violated, which is not what we want.
		 * This operation ([[castTypePath()]]) is intended to handle those cases.
		 */
		def castTypePath[E <: Doer](doer: E): doer.Duty[A] = {
			assert(thisDoer eq doer)
			this.asInstanceOf[doer.Duty[A]]
		}
	}

	//// Duty's factory methods ////

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
	@threadUnsafe lazy val Duty_never: Duty[Nothing] = new Duty_NotEver()

	/** Creates a [[Duty]] whose result is calculated at the call site even before the duty is constructed.
	 * $threadSafe
	 *
	 * @param a the already calculated result of the returned [[Duty]]. */
	inline def Duty_ready[A](a: A): Duty[A] = new Duty_Ready(a)

	/** Creates a [[Duty]] that yields the value returned by the provided supplier.
	 * ===Detailed behavior===
	 * Creates a duty that, when executed, evaluates the `supplier` within the $DoSerEx. If the evaluation finishes:
	 *		- abruptly, will never complete.
	 *		- normally, completes with the evaluation's result.
	 *
	 * $$threadSafe
	 * TODO rename to Duty_own
	 *
	 * @param supplier the supplier of the result. $isExecutedByDoSerEx $notGuarded
	 * @return the [[Duty]] described in the method description.
	 */
	inline def Duty_mine[A](supplier: () => A): Duty[A] = new Duty_Mine(supplier)

	/** Creates a [[Duty]] that yields what the [[Duty]] created by the provided supplier yields.
	 * Is equivalent to: {{{Duty_mine(supplier).flatMap(identity)}}} but slightly more efficient
	 * ===Detailed behavior===
	 * Creates a duty that, when executed:
	 *		- evaluates the `supplier` within the $DoSerEx;
	 *		- then triggers an execution of the returned Duty;
	 *		- finally completes with the result of executed duty.
	 *
	 * $$threadSafe
	 * TODO rename to Duty_ownFlat
	 *
	 * @param supplier the supplier of the duty whose execution will give the result. $isExecutedByDoSerEx $notGuarded
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
	 * Given the serial-execution nature of [[Doer]] this operation only has sense when the provided [[Duty]]s involves foreign ([[Duty_foreign]]) or alien ([[Venture_alien]]) actions.
	 * ===Detailed behavior===
	 * Creates a new [[Duty]] that, when executed:
	 *		- triggers an execution for both: `dutyA` and `dutyB`
	 *		- when both are completed, completes with the value that results of applying the function `f` to their results.
	 *
	 * $threadSafe
	 *
	 * @param dutyA a [[Duty]]
	 * @param dutyB a [[Duty]]
	 * @param f the function that combines the results of the two [[Duty]] instances. $isExecutedByDoSerEx $unhandledErrorsArePropagatedToVentureResult
	 * @return the [[Duty]] described in the method description.
	 */
	inline def Duty_combine[A, B, C](dutyA: Duty[A], dutyB: Duty[B])(f: (A, B) => C): Duty[C] =
		new Duty_Combined(dutyA, dutyB, f)

	/**
	 * Creates a [[Duty]] that, when executed, simultaneously triggers an execution for each [[Duty]]s in the provided iterable, and completes with a collection containing their results in the same order if all are successful, or a failure if any is faulty.
	 * This overload is only convenient for very small lists. For large ones it is not efficient and also may cause stack-overflow when the [[Duty]] is executed.
	 * Use the other overload for large lists or other kind of iterables.
	 *
	 * $threadSafe
	 *
	 * @param duties the `Iterable` of duties that the returned [[Duty]] will trigger simultaneously to combine their results.
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
	 * @param duties the `Iterable` of duties that the returned [[Duty]] will trigger simultaneously to combine their results.
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

	//// Concrete implementations of [[Duty]] used internally ////

	/** $suppressSyntheticCompanionObject */
	private inline def Duty_FromVenture(trap: Nothing): Any = trap

	final class Duty_FromVenture[A, B >: A](ventureA: Venture[A], exceptionHandler: Throwable => B) extends AbstractDuty[B] {
		override def engage(onComplete: B => Unit): Unit = {
			ventureA.engage { tryA =>
				val b = tryA match {
					case Success(a) => a
					case Failure(exception) => exceptionHandler(exception)
				}
				onComplete(b)
			}
		}

		override def toString: String = deriveToString[Duty_FromVenture[A, B]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Duty_Map(trap: Nothing): Any = trap

	final class Duty_Map[A, B](cA: Duty[A], f: A => B) extends AbstractDuty[B] {
		override def engage(onComplete: B => Unit): Unit =
			cA.engage { a => onComplete(f(a)) }

		override def toString: String = deriveToString[Duty_Map[A, B]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Duty_FlatMap(trap: Nothing): Any = trap

	final class Duty_FlatMap[A, B](cA: Duty[A], f: A => Duty[B]) extends AbstractDuty[B] {
		override def engage(onComplete: B => Unit): Unit = cA.engage { a => f(a).engage(onComplete) }

		override def toString: String = deriveToString[Duty_FlatMap[A, B]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Duty_AndThen(trap: Nothing): Any = trap

	final class Duty_AndThen[A](dutyA: Duty[A], sideEffect: A => Unit) extends AbstractDuty[A] {
		override def engage(onComplete: A => Unit): Unit =
			dutyA.engage { a =>
				sideEffect(a)
				onComplete(a)
			}

		override def toString: String = deriveToString[Duty_AndThen[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Duty_NotEver(trap: Nothing): Any = trap

	class Duty_NotEver extends AbstractDuty[Nothing] {
		override def engage(onComplete: Nothing => Unit): Unit = ()

		override def toString: String = deriveToString[Duty_NotEver](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Duty_Ready(trap: Nothing): Any = trap

	final class Duty_Ready[A](a: A) extends AbstractDuty[A] {
		override def engage(onComplete: A => Unit): Unit = onComplete(a)

		override def toFutureHardy(isWithinDoSiThEx: Boolean = isInSequence): Future[A] = Future.successful(a)

		override def toString: String = deriveToString[Duty_Ready[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Duty_Mine(trap: Nothing): Any = trap

	final class Duty_Mine[A](supplier: () => A) extends AbstractDuty[A] {
		override def engage(onComplete: A => Unit): Unit = onComplete(supplier())

		override def toString: String = deriveToString[Duty_Mine[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Duty_MineFlat(trap: Nothing): Any = trap

	final class Duty_MineFlat[A](supplier: () => Duty[A]) extends AbstractDuty[A] {
		override def engage(onComplete: A => Unit): Unit = supplier().engage(onComplete)

		override def toString: String = deriveToString[Duty_MineFlat[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Duty_Foreign(trap: Nothing): Any = trap

	final class Duty_Foreign[A](foreignDoer: Doer, foreignDuty: foreignDoer.Duty[A]) extends AbstractDuty[A] {
		override def engage(onComplete: A => Unit): Unit = foreignDuty.trigger()(a => thisDoer.run(onComplete(a)))

		override def toString: String = deriveToString[Duty_Foreign[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Duty_Combined(trap: Nothing): Any = trap

	final class Duty_Combined[+A, +B, +C](dutyA: Duty[A], dutyB: Duty[B], f: (A, B) => C) extends AbstractDuty[C] {
		override def engage(onComplete: C => Unit): Unit = {
			object vars {
				var aIsCompleted: Boolean = false
				var bIsCompleted: Boolean = false
				var maybeA: AnyRef | Null = null
				var maybeB: AnyRef | Null = null
			}
			dutyA.engage { a =>
				if vars.bIsCompleted then onComplete(f(a, vars.maybeB.asInstanceOf[B]))
				else {
					vars.aIsCompleted = true
					vars.maybeA = a.asInstanceOf[AnyRef]
				}
			}
			dutyB.engage { b =>
				if vars.aIsCompleted then onComplete(f(vars.maybeA.asInstanceOf[A], b))
				else {
					vars.bIsCompleted = true
					vars.maybeB = b.asInstanceOf[AnyRef]
				}
			}
		}

		override def toString: String = deriveToString[Duty_Combined[A, B, C]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Duty_Sequence(trap: Nothing): Any = trap

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
					val duty = dutyIterator.next()
					val dutyIndex = index
					duty.engage { a =>
						array(dutyIndex) = a
						completedCounter += 1
						if completedCounter == size then onComplete(array)
					}
					index += 1
				}
			}
		}
	}

	////////////// ONCE ///////////////

	/** A [Duty] that remembers the result of the execution that completes first, and all the others produce the same result as the first.
	 * Once the first completion occurs the result is subsequently delivered deterministically to present and future subscribers.
	 * Specifically, a [[Duty]] that:
	 *		- Is completed a single time and caches the result so that, once completed, subscribing a consumer executes the call-back immediately. Note that linking a down-chain subscribes the first link as consumer.
	 * 		- The monadic laws are always upheld. Before completion, they can’t be observed because no result exists yet; after completion, they can be observed in the cached result.
	 *		- Allows to subscribe/unsubscribe consumers of its completion result dynamically.
	 *		- The source of determination may be intrinsic from the start (e.g. {{{ Covenant[String]().fulfillWith(anIntrinsicallyDeterminedDuty) }}}) or external (e.g. {{{ Covenant[String]().fulfill(someValueDeterminedExternally) }}}); the concrete result value is realized only at completion.
	 * The timing and outcome of completion are not specified by this class. That behavior is delegated to subclasses; see [[Covenant]].
	 * @note Triggering (calling [[trigger]]) on a pending [[LatchingDuty]] does not trigger the execution of the subscribed consumers, but just subscribes the `onComplete` call-back passed to [[trigger]] as a consumer of the future result.
	 * */
	sealed abstract class LatchingDuty[+A] extends AbstractDuty[A], Idempotent[A] {

		/** @inheritdoc
		 * @note The override is necessary to specialize the return type; and the implementation is necessary (can't leave the method abstract) because [[Covenant]] is invariant.
		 * */
		override def succeed: LatchingVenture[A] = {
			this match {
				case c: Covenant[A] @unchecked => c.succeed
				case rd: ReadyDuty[A] => rd.succeed
			}
		}

		inline def asDuty: Duty[A] = this

		/**
		 * Transforms this [[LatchingDuty]] by applying the given function to the result of this [[LatchingDuty]].
		 * ===Detailed behavior===
		 * Creates a [[LatchingDuty]] that yields the result of applying the provided function to the results of this [[LatchingDuty]].
		 * @note CAUTION: Must be called within the $DoSerEx
		 * @note The override is necessary to specialize the return type, and implementation is necessary (can't leave the method abstract) because [[Covenant]] is invariant.
		 * @param f a function that transforms the result of this [[Duty]].
		 *
		 * $isExecutedByDoSerEx
		 *
		 * $notGuarded
		 */
		override def map[B](f: A => B): LatchingDuty[B] = {
			this match {
				case rd: ReadyDuty[A] => rd.map(f)
				case c: Covenant[A @unchecked] => c.map(f)
			}
		}


		/**
		 * Transforms this [[LatchingDuty]] by applying the provided function to the result of this [[LatchingDuty]] and then subscribing-to the [[LatchingDuty]] returned by said function.
		 * The returned [[LatchingDuty]] will be already fulfilled if, and only if, this [[LatchingDuty]] is already fulfilled.
		 * @note CAUTION: Must be called within the $DoSerEx
		 * @param f a function that is applied to the result of this [[Duty]] execution to return a [[Duty]] that is executed next to produce the result that the [[Duty]] returned by this method yields.
		 *
		 * $isExecutedByDoSerEx
		 *
		 * $notGuarded
		 */
		def flatMap[B](f: A => LatchingDuty[B]): LatchingDuty[B]

		/** Creates a new [[LatchingDuty]] that yields exactly the same result (same identity) as this [[LatchingDuty]] but executes the provided side-effecting function before yielding it.
		 *
		 * $threadSafe
		 *
		 * @param sideEffect a function that is applied to the result of this [[LatchingDuty]] for its side effects. */
		override def andThen(sideEffect: A => Unit): LatchingDuty[A] = {
			subscribe(sideEffect)
			this
		}
	}

	//// Once factory methods ////

	/** Creates an already completed [[LatchingDuty]].
	 * @param immediateResult the immediate result that this [[LatchingDuty]] yields. */
	inline def LatchingDuty_ready[A](immediateResult: A): ReadyDuty[A] =
		new ReadyDuty(immediateResult)

	/** An already completed [[LatchingDuty]] that yields [[Unit]].
	 * CAUTION: This @threadUnsafe lazy val does not guarantee a unique instance under concurrent access. Its use is only safe for logic that depends on the value's data, not its object identity (eq/ne). */
	@threadUnsafe lazy final val LatchingDuty_unit: ReadyDuty[Unit] = ReadyDuty(())

	/** An already completed [[LatchingDuty]] that yields `true`.
	 * CAUTION: This @threadUnsafe lazy val does not guarantee a unique instance under concurrent access. Its use is only safe for logic that depends on the value's data, not its object identity (eq/ne). */
	@threadUnsafe lazy final val LatchingDuty_true: ReadyDuty[Boolean] = ReadyDuty(true)

	/** An already completed [[LatchingDuty]] that yields `false`.
	 * CAUTION: This @threadUnsafe lazy val does not guarantee a unique instance under concurrent access. Its use is only safe for logic that depends on the value's data, not its object identity (eq/ne). */
	@threadUnsafe lazy final val LatchingDuty_false: ReadyDuty[Boolean] = ReadyDuty(false)

	/** Like [[Duty_sequenceVenturesToArray]] but eager (instead of lazy). */
	inline def LatchingDuty_sequenceVenturesToArray[A: ClassTag, C[x] <: Iterable[x]](ventures: C[Venture[A]], isWithinDoSerEx: Boolean = isInSequence): LatchingDuty[Array[Try[A]]] =
		Covenant_triggerAndWire(Duty_sequenceVenturesToArray(ventures), isWithinDoSerEx)

	//// READY DUTY ////

	/** A [[LatchingDuty]] that is fulfilled since its inception. */
	final class ReadyDuty[+A](val value: A) extends LatchingDuty[A] {

		override def engage(onComplete: A => Unit): Unit =
			onComplete(value)

		override def succeed: ReadyVenture[A] =
			ReadyVenture(Success(value))

		override val maybeResult: Maybe[A] =
			Maybe(value)

		override def subscribe(consumer: A => Unit): Unit = {
			checkWithin()
			consumer(value)
		}

		override def unsubscribe(onComplete: A => Unit): Unit =
			()

		override def isSubscribed(onComplete: A => Unit): Boolean =
			false

		override def foreach(consumer: A => Unit): Unit = {
			checkWithin()
			consumer(value)
		}

		override def map[B](f: A => B): ReadyDuty[B] = {
			checkWithin()
			ReadyDuty(f(value))
		}

		override def flatMap[B](f: A => LatchingDuty[B]): LatchingDuty[B] = {
			checkWithin()
			f(value)
		}

		override def toFutureHardy(isWithinDoSerEx: Boolean = isInSequence): Future[A] =
			Future.successful(value)

		override def toString: String = deriveToString[ReadyDuty[A]](this)
	}

	/** Creates a [[ReadyDuty]] that yields the provided value.
	 * @note Also suppresses the generation of the synthetic companion object. */
	inline final def ReadyDuty[A](a: A): ReadyDuty[A] = new ReadyDuty(a)

	//// COVENANT /////

	/** A [[LatchingDuty]] with dynamic control of its completion (the execution of the subscribed consumers).
	 *
	 * It exposes methods such as [[fulfill]] and [[fulfillWith]] to allow external code to complete it.
	 *
	 * [[Covenant]] is to [[Duty]] as [[Commitment]] is to [[Venture]], and as [[scala.concurrent.Promise]] is to [[scala.concurrent.Future]]
	 * */
	final class Covenant[A](initialResult: Maybe[A]) extends LatchingDuty[A], SubscriptionHub[A] {
		private var oResult: Maybe[A] = initialResult

		def this() = this(Maybe.empty)

		override def engage(onComplete: A => Unit): Unit =
			oResult.fold(attach(onComplete))(onComplete)

		override def succeed: LatchingVenture[A] = {
			oResult.fold {
				val commitment = new Commitment[A]
				subscribe(a => commitment.completeUnsafe(Success(a)))
				commitment
			} { a =>
				ReadyVenture(Success(a))
			}
		}

		override def maybeResult: Maybe[A] = {
			checkWithin()
			oResult
		}

		override def subscribe(consumer: A => Unit): Unit = {
			checkWithin()
			oResult.fold(attach(consumer))(consumer)
		}

		override def unsubscribe(consumer: A => Unit): Unit = {
			checkWithin()
			detach(consumer)
		}

		override def isSubscribed(onComplete: A => Unit): Boolean =
			isAttached(onComplete)

		override def foreach(consumer: A => Unit): Unit = {
			checkWithin()
			oResult.fold(subscribe(consumer))(consumer)
		}

		override def map[B](f: A => B): LatchingDuty[B] = {
			checkWithin()
			oResult.fold {
				val covenant = Covenant[B]()
				this.subscribe(a => covenant.fulfillUnsafe(f(a)))
				covenant
			} { a =>
				new ReadyDuty[B](f(a))
			}
		}

		override def flatMap[B](f: A => LatchingDuty[B]): LatchingDuty[B] = {
			checkWithin()
			oResult.fold {
				val covenant = Covenant[B]()
				this.subscribe(a => f(a).subscribe(b => covenant.fulfillUnsafe(b)))
				covenant
			}(f)
		}

		/** The [[LatchingDuty]] whose completion is controlled by this [[Covenant]].
		 *
		 * Provided to mimic containment semantics, allowing external code to treat this [[Covenant]] as if it exposed a separate [[LatchingDuty]] field.
		 * @return this [[Covenant]] as a [[LatchingDuty]]. */
		inline def asLatchingDuty: LatchingDuty[A] = this


		/** Fulfills this [[Covenant]] with the given `result`, unless it has already been fulfilled at the time the fulfillment is performed.
		 *
		 * Fulfillment is performed:
		 * - Synchronously (before this method returns) if `isWithinDoSerEx` is true.
		 * - Asynchronously as soon as possible otherwise.
		 *
		 * This method delegates to [[fulfillUnsafe]], scheduling it within this [[Doer]]'s sequential executor if not already executing within it.
		 * TODO rename to `complete`
		 * @param result the value to complete this [[Covenant]] with.
		 * @param isWithinDoSerEx $isWithinDoSerEx
		 * @param onCompleted optional callback invoked with the final result and information about its origin: [[ANOTHER_BEFORE]] if the result is a previously set one; [[THE_PROVIDED]] if the result is the one provided here.
		 */
		inline def fulfill(result: A, isWithinDoSerEx: Boolean = isInSequence, onCompleted: (A, ImmediateResultOrigin) => Unit = (_, _) => ()): this.type = {
			if isWithinDoSerEx then fulfillUnsafe(result, onCompleted)
			else {
				run(fulfillUnsafe(result, onCompleted))
				this
			}
		}


		/** Fulfills this [[Covenant]] with the given `result`, unless it has already been fulfilled.
		 *
		 * If this [[Covenant]] is not yet fulfilled, the provided `result` becomes its final value and is made immediately visible to all subscribers.
		 * If it is already fulfilled, the provided `result` is ignored.
		 *
		 * CAUTION: This method must be called within this [[Doer]].
		 * CAUTION: Deep synchronous chains of [[flatMap]] over immediately-fulfilled [[LatchingDuty]] instances during ongoing fulfillment can form a synchronous recursion (fulfill → subscribe-immediate → fulfill → …) that overflows the stack. // TODO consider the trampoline solutions discussed with copilot in the session "causal anchoring dilema", near the end.
		 * CAUTION: Too many subscriptions may overflow the stack upon fulfillment. // TODO consider implementing a list with a pointer to the tail to avoid the recursion loop of the current implementation.
		 *
		 * TODO rename to `completeUnsafe`
		 * @param result the value to fulfill this [[Covenant]] with.
		 * @param onCompleted optional callback invoked synchronously (before this method returns) with the fulfilling value and information about its origin:
		 *                    - [[THE_PROVIDED]] if this [[Covenant]] was completed by this method call with the provided value;
		 *                    - [[ANOTHER_BEFORE]] if this [[Covenant]] was already completed when this method was called.
		 */
		def fulfillUnsafe(result: A, onCompleted: (A, ImmediateResultOrigin) => Unit = (_, _) => ()): this.type = {
			oResult.fold {
				// First, set the result.
				this.oResult = Maybe(result)
				// Second, run the consumers in subscriptions order
				this.capture(result)
				// Finally. call the provided call-back.
				try onCompleted(result, THE_PROVIDED)
				catch {
					case NonFatal(e) => reportPanicException(e)
				}
			}(previousResult => onCompleted(previousResult, ANOTHER_BEFORE))
			this
		}

		/** Wires this [[Covenant]] to be completed with the result of a [[Duty]].
		 *
		 * Arranges this [[Covenant]] to be fulfilled if `fulfillingDuty` completes, unless it was fulfilled before.
		 * Always one, and only one, of the two callback is invoked:
		 *		- `onAlreadyCompleted` if this [[Covenant]] was already fulfilled when the subscription is done.
		 *		- `onCompletedLater` if this [[Covenant]] is fulfilled after the subscription is done.
		 * The subscription is synchronic if `isWithinDoSerEx` is true, and asynchronic ASAP otherwise.
		 *
		 * TODO rename to `completeWith`
		 * @param fulfillingDuty the [[Duty]] whose result will be used to complete this [[Covenant]].
		 * @param isWithinDoSerEx $isWithinDoSerEx
		 * @param onCompleted optional callback invoked when this [[Covenant]] is fulfilled. The first parameter is the fulfilling value and the second informs about its origin. Invoked within this [[Doer]] sequential executor.
		 * @throws IllegalArgumentException if `fulfillingDuty` is the same instance as this [[Covenant]].
		 */
		def fulfillWith(fulfillingDuty: Duty[A], isWithinDoSerEx: Boolean = isInSequence, onCompleted: (A, ResultOrigin) => Unit = (_, _) => ()): this.type = {
			if fulfillingDuty eq this then throw IllegalArgumentException("A Covenant can't be fulfilled with itself.")
			if isWithinDoSerEx then {
				oResult.fold {
					fulfillingDuty.engage(result => fulfillUnsafe(result, onCompleted))
				} { result =>
					try onCompleted(result, ANOTHER_BEFORE)
					catch {
						case NonFatal(e) => reportPanicException(e)
					}
				}
			}
			else run(fulfillWith(fulfillingDuty, true, onCompleted))
			this
		}

		/** @note removing the empty parameters list causes an obscure compilation error in [[Covenant.trigger]] call sites after a full project re-build. */
		override def toString(): String = {
			s"Covenant(oResult=$oResult, subscriptions=[${if firstOnCompleteObserver eq null then "" else s"$firstOnCompleteObserver, ${onCompletedObservers.mkString(", ")}"}])"
		}
	}

	//// COVENANT FACTORY METHODS ////

	/** Creates a new pending [[Covenant]] */
	inline def Covenant[A](): Covenant[A] =
		new Covenant()

	/** Creates a [[Covenant]] that will fulfill with the result of executing the provided supplier within the $DoSerEx.
	 * @param supplier a supplier function that is executed within the $DoSerEx and returns the value to fulfill the created [[Covenant]] with. */
	def Covenant_mine[A](supplier: () => A): Covenant[A] = {
		val covenant = new Covenant[A]
		run {
			covenant.fulfillUnsafe(supplier())
		}
		covenant
	}

	/** Creates a [[Covenant]] that is wired to the [[LatchingDuty]] resulting of executing the provided supplier within the $DoSerEx.
	 * @param supplier a supplier function that is executed within the $DoSerEx to return the [[LatchingDuty]] to which the created [[Covenant]] is wired. */
	def Covenant_mineFlat[A](supplier: () => LatchingDuty[A]): Covenant[A] = {
		val covenant = new Covenant[A]
		run {
			supplier().engage(a => covenant.fulfillUnsafe(a))
		}
		covenant
	}

	/** Triggers an execution of the given [[Duty]] and returns a [[Covenant]] that will be completed with the result of the triggered execution if it completes before this [[Covenant]] is completed by other means.
	 *
	 * This method initiates an execution of the given [[Duty]] and wires its result to a newly created [[Covenant]].
	 * The returned [[Covenant]] acts as a completion handle for the execution triggered by this method, and can be used to observe or react to its result.
	 *
	 * @param duty the [[Duty]] to be triggered.
	 * @param isWithinDoSerEx $isWithinDoSerEx
	 * @param onFulfilled The first parameter is the fulfilling value and the second informs about its origin. Invoked within this [[Doer]] sequential executor.
	 * @return a [[Covenant]] that will be completed with the result of the execution triggered by this method.
	 */
	inline def Covenant_triggerAndWire[A](duty: Duty[A], inline isWithinDoSerEx: Boolean = isInSequence, onFulfilled: (A, ImmediateResultOrigin) => Unit = (_: A, _: ImmediateResultOrigin) => ()): Covenant[A] = {
		val covenant = new Covenant[A]()
		duty.trigger(isWithinDoSerEx)(result => covenant.fulfillUnsafe(result, onFulfilled))
		covenant
	}



	///////////// VENTURE //////////////

	/** A hardy and short-circuiting version of [[Duty]].\
	 * Advantages of [[Venture]] compared to [[Duty]]:
	 *		- results are wrapped withing a [[Try]] which allows the support of failed results.
	 *		- the call to the routines received by the operations are guarded with a try-catch, which allows to propagate failures through [[Venture]] chains.
	 *		- can encapsulate a [[Future]] making interoperability with them easier.
	 * @param A the type of the result obtained when executing this [[Venture]]. */
	type Venture[+A] = AbstractVenture[A]


	/** A hardy and short-circuiting version of [[Duty]].\
	 * Design note:
	 * - The use of mixins to define the hardy side of the hierarchy was explored in Duty3.scala and discarded due to extra allocation in many fundamental operations.
	 * - Defining [[Venture]] as `opaque type Venture[+A] = Duty[Try[A]]` was explored but discarded due to bugs in the scala compiler. See https://github.com/scala/scala3/issues/25594. This will eliminate many redundant [[Venture]] implementation classes by reusing [[Duty]]'s counterpart, but it may cause IDE issues since [[Venture]] operations would need to be defined as extension methods.
	 * @tparam A the type of the result obtained when executing this [[Venture]]. */
	abstract class AbstractVenture[+A] extends AbstractDuty[Try[A]] { thisVenture =>

		/** Removes short-circuit semantics by reifying both the successful and failed outcomes as a [[scala.util.Try]] value within a strict [[Duty]].\
		 * Together with [[Duty.succeed]] this method allow to mix duties and ventures in the same chain. */
		def reconcile: Duty[Try[A]] = thisVenture

		/** Triggers an execution of this [[Venture]] and returns a [[Future]] of its result.\
		 * @param isWithinDoSerEx $isWithinDoSerEx
		 * @return a [[Future]] that will be completed when this [[Venture]] is completed. */
		def toFuture(isWithinDoSerEx: Boolean = isInSequence): Future[A] = {
			val promise = Promise[A]()
			thisVenture.trigger(isWithinDoSerEx)(promise.complete)
			promise.future
		}

		/** Triggers an execution of this [[Venture]] noticing faulty results.\
		 * @param isWithinDoSerEx $isWithinDoSerEx
		 * @param errorHandler called when the triggered execution completes with a failure. $isExecutedByDoSerEx $unhandledErrorsAreReported */
		inline def triggerAndForgetHandlingErrors(inline errorHandler: Throwable => Unit, inline isWithinDoSerEx: Boolean = isInSequence): Unit =
			thisVenture.trigger(isWithinDoSerEx) {
				case Failure(e) =>
					try errorHandler(e) catch {
						case NonFatal(cause) => reportPanicException(cause)
					}
				case _ => ()
			}

		/** Triggers this [[Venture]] and, once it is completed, processes its result for its side effects.
		 * Differs from [[trigger]] in that it catches non-fatal exceptions thrown by the provided consumer function and reports them with [[reportPanicException]].
		 * @param onComplete called with this [[Venture]] result when it completes, if it ever does. */
		inline def triggerHardy(inline onComplete: Try[A] => Unit, inline isWithinDoSerEx: Boolean = isInSequence): Unit =
			thisVenture.trigger(isWithinDoSerEx) { tryA =>
				try onComplete(tryA)
				catch {
					case NonFatal(e) => thisDoer.reportPanicException(e)
				}
			}

		/** Triggers this [[Venture]] and once it is completed successfully processes its result for its side effects.\
		 * WARNING: `consumer` won't be called if this [[Venture]] completes with a failure.
		 * @param consumer called with this [[Venture]] result when it completes successfully, if it ever does. */
		@targetName("foreach_venture")
		def foreach(consumer: A => Unit): Unit =
			thisVenture.triggerHardy {
				case Success(a) => consumer(a)
				case _ => ()
			}

		/** Transform this [[Venture]] by applying the given function to the result. Analogous to [[Future.transform]].\
		 * **Detailed description:**
		 * Creates a [[Venture]] that yields the result of applying the provided function to the results of this [[Venture]].
		 * If the evaluation of the provided function finishes:
		 * - abruptly, completes with the cause.
		 * - normally, completes with the result of the evaluation.
		 *
		 * $threadSafe
		 *
		 * @param f applied to the result of this [[Venture]] to obtain the result of the returned [[Venture]].
		 *
		 * $isExecutedByDoSerEx
		 *
		 * $unhandledErrorsArePropagatedToVentureResult
		 */
		def transform[B](f: Try[A] => Try[B]): Venture[B] =
			new Venture_Transform(thisVenture, f)


		/** Transforms this [[Venture]] by applying the provided function to the result of this [[Venture]] and then executing the [[Venture]] returned by said function.\
		 * **Detailed behavior**:
		 * Creates a [[Venture]] that, when executed, it will:
		 * - Execute this [[Venture]] and apply the provided function to its result.
		 * - Then executes the [[Venture]] built in the previous step by the provided function and completes with its result.\
		 * $threadSafe\
		 * @param f a function that is applied to the result of this [[Venture]] execution, to build a [[Venture]] that is executed next to produce the result that the [[Venture]] returned by this method yields.\
		 * $isExecutedByDoSerEx */
		inline def transformWith[B](f: Try[A] => Venture[B]): Venture[B] =
			new Venture_TransformWith(thisVenture, f)


		/** Transforms this [[Venture]] by applying the given function to the result if it is successful. Analogous to [[Future.map]].\
		 * Equivalent to {{{ transform(_ map f) }}} but more efficient (creates one less closure).\
		 * See [[recover]] and [[toDuty]] if you want to transform the failures; and [[transform]] if you want to transform both, successful and failed ones.\
		 * **Detailed behavior:**
		 * Creates a [[Venture]] that yields the result of applying the provided function to successful results of this [[Venture]].
		 * If the evaluation of the provided function finishes:
		 * - abruptly, completes with the cause.
		 * - normally, completes with the successful result.\
		 * $threadSafe\
		 * @param f a function that transforms successful results of this [[Venture]]. $isExecutedByDoSerEx $unhandledErrorsArePropagatedToVentureResult */
		def map[B](f: A => B): Venture[B] =
			new Venture_Map(thisVenture, f)

		/** Composes this [[Venture]] with a second one that is built from the result of this one, but only when this one is successful. Analogous to [[Future.flatMap]].\
		 * **Detailed behavior:**
		 * Creates a [[Venture]] that, when executed, it will:
		 *  - Trigger an execution of this [[Venture]] and if the result is:
		 *    - `Failure(e)`, completes with that failure.
		 *    - `Success(a)`, applies the function to `a`. If the evaluation finishes:
		 *      - abruptly, completes with the cause.
		 *      - normally with `ventureB`, triggers an execution of `ventureB` and completes with its result.\
		 * $threadSafe\
		 * @param f a function that receives the result of `ventureA`, when it is a [[Success]], and returns the [[Venture]] to be executed next. $isExecutedByDoSerEx $unhandledErrorsArePropagatedToVentureResult */
		inline def flatMap[B](f: A => Venture[B]): Venture[B] =
			new Venture_FlatMap(thisVenture, f)

		/** Needed to support filtering and case matching in for-compressions. The for-expressions (or for-bindings) after the filter are not executed if the [[predicate]] is not satisfied.\
		 * **Detailed behavior:** Gives a [[Venture]] that, when executed, it will:
		 *  - executes this [[Venture]] and, if the result is a:
		 *    - [[Failure]], completes with that failure.
		 *    - [[Success]], applies the `predicate` to its content and if the evaluation finishes:
		 *      - abruptly, completes with the cause.
		 *      - normally with a `false`, completes with a [[Failure]] containing a [[NoSuchElementException]].
		 *      - normally with a `true`, completes with the result of this [[Venture]].\
		 * $threadSafe\
		 * @param predicate a predicate that determines which values are propagated to the following for-bindings. */
		def withFilter(predicate: A => Boolean): Venture[A] =
			new Venture_WithFilter(thisVenture, predicate)

		/** Applies the side-effecting function to the result of this [[Venture]] without affecting the propagated value.
		 * The result of the provided function is always ignored and therefore not propagated in any way.
		 * This method allows to enforce many callbacks to receive the same value and to be executed in the order they are chained.
		 * It's worth mentioning that the side-effecting function is executed before triggering the next duty in the chain.\
		 * **Detailed description:**
		 * Returns a [[Venture]] that, when executed:
		 *  - first executes this [[Venture]];
		 *  - second applies the received function to the result and, if the evaluation finishes:
		 *    - normally, completes with the result of this [[Venture]].
		 *    - abruptly with a non-fatal exception, reports the failure cause to [[Doer.reportFailure]] and completes with the result of this [[Venture]].
		 *    - abruptly with a fatal exception, never completes.\
		 * $threadSafe\
		 * @param sideEffect a side-effecting function. The call to this function is wrapped in a try-catch block; however, unlike most other operators, unhandled non-fatal exceptions are not propagated to the result of the returned [[Venture]]. $isExecutedByDoSerEx */
		override def andThen(sideEffect: Try[A] => Unit): Venture[A] =
			new Venture_AndThen(thisVenture, sideEffect)

		/** Wraps this [[Venture]] into a [[Duty]] applying the given function to transform failure results into successful ones. This is like [[map]] but for the throwable; and like [[recover]] but with a complete function.
		 * Together with [[Duty.succeed]] this method allow to mix duties and ventures in the same chain. *
		 * @param exceptionHandler a complete function to apply to the result of this [[Venture]] if it is a [[Failure]].\
		 * $isExecutedByDoSerEx\
		 * $notGuarded\
		 * @return a [[Duty]] that yields the result of this [[Venture]]. */
		inline final def reconcile[B >: A](exceptionHandler: Throwable => B): Duty[B] =
			new Duty_FromVenture[A, B](thisVenture, exceptionHandler)

		/** @return a [[Duty]] that yields the result of this [[Venture]]. */
		inline final def asHardyDuty: Duty[Try[A]] =
			thisVenture

		/** Transforms this [[Venture]] applying the given partial function to failure results. This is like map but for the throwable; and like [[reconcile]] but with a partial function. Analogous to [[Future.recover]].\
		 * **detailed description:**
		 * Returns a new [[Venture]] that, when executed, executes this [[Venture]] and if the result is:
		 *  - a [[Success]] or a [[Failure]] for which `pf` is not defined, completes with the same result.
		 *  - a [[Failure]] for which `pf` is defined, applies `pf` to it and if the evaluation finishes:
		 *    - abruptly, completes with the cause.
		 *    - normally, completes with the result of the evaluation.\
		 * $threadSafe\
		 * @param pf the [[PartialFunction]] to apply to the result of this [[Venture]] if it is a [[Failure]]. $isExecutedByDoSerEx $unhandledErrorsArePropagatedToVentureResult */
		def recover[B >: A](pf: PartialFunction[Throwable, B]): Venture[B] =
			transform(_.recover(pf))

		/** Composes this [[Venture]] with a second one that is built from the result of this one, but only when said result is a [[Failure]] for which the given partial function is defined. This is like flatMap but for the exception. Analogous to [[Future.recoverWith]].\
		 * **detailed description:**
		 * Returns a new [[Venture]] that, when executed, executes this [[Venture]] and if the result is:
		 *  - a [[Success]] or a [[Failure]] for which `pf` is not defined, completes with the same result.
		 *  - a [[Failure]] for which `pf` is defined, applies `pf` to it and if the evaluation finishes:
		 *    - abruptly, completes with the cause.
		 *    - normally returning a [[Venture]], triggers an execution of said [[Venture]] and completes with its same result.\
		 * $threadSafe\
		 * @param pf the [[PartialFunction]] to apply to the result of this [[Venture]], if it is a [[Failure]], to build the second [[Venture]]. $isExecutedByDoSerEx $unhandledErrorsArePropagatedToVentureResult */
		def recoverWith[B >: A](pf: PartialFunction[Throwable, Venture[B]]): Venture[B] = {
			transformWith[B] {
				case Failure(t) => pf.applyOrElse(t, (e: Throwable) => new ReadyVenture[B](Failure(e)));
				case sa: Success[A] => new ReadyVenture[B](sa);
			}
		}

		/** Wraps this [[Venture]] into another that belongs to other [[Doer]].\
		 * Useful to chain [[Venture]]'s operations that involve different [[Doer]] instances.\
		 * **Detailed behavior:**
		 * Returns a [[Venture]] that belongs to the provided [[Doer]]. When it is triggered, it will trigger this [[Venture]] within this [[Doer]] and, when completed, make the returned [[Venture]] to yield the result.\
		 * CAUTION: Avoid closing over the same mutable variable from two transformations applied to [[Venture]] instances belonging to different [[Doer]]s.\
		 * Remember that all routines (e.g., functions, procedures, predicates, and callbacks) provided to [[Venture]] methods are executed by the $DoSerEx of the [[Doer]] that owns the [[Venture]] instance on which the method is called.
		 * Therefore, calling [[trigger]] on the returned [[Venture]] will execute the `onComplete` passed to it within the $DoSerEx of the `otherDoer`.\
		 * $threadSafe\
		 * @param otherDoer the [[Doer]] to which the returned [[Venture]] will belong. */
		override def onBehalfOf(otherDoer: Doer): otherDoer.Venture[A] =
			otherDoer.Venture_foreign(thisDoer)(this)

		/** Casts the singleton type of the [[Doer]] instance that owns this [[Venture]] to the singleton-type of the received [[Doer]].\
		 * This operation does nothing at runtime. It only tricks the compiler to prevent it from complaining when operating with [[Venture]]s that correspond to the same [[Doer]] instance but have different type-paths.\
		 * CAUTION: Use it only if you are sure that the provided [[Doer]] instance is the one that owns this [[Venture]].\
		 * Design note: It was decided to make [[Venture]] (and [[Duty]]) an inner class of the [[Doer]] to take advantage of type-path checking to detect when the contract "all operand functions passed to [[Venture]] (and [[Duty]]) operations owned by the same [[Doer]] are executed in sequence" might be violated, at compile time.\
		 * Using type-path checking to detect contract violations is very valuable but it comes at a cost, because the type-path check done by the compiler is stricter than necessary -- it checks that the singleton type of the references involved be compatible, and we only need to check that the involved [[Venture]]s correspond to the same [[Doer]] instance.
		 * Therefore, the compiler will report type errors in situations the contract is not violated, which is not what we want.
		 * This operation ([[castTypePath()]]) is intended to handle those cases. */
		override def castTypePath[E <: Doer](doer: E): doer.Venture[A] = {
			assert(thisDoer eq doer)
			this.asInstanceOf[doer.Venture[A]]
		}
	}

	/** An always successful ready [[Venture]] that yields [[Unit]].\
	 * Equivalent to {{{Venture_successful[Unit](())}}}\
	 * CAUTION: This @threadUnsafe lazy val does not guarantee a unique instance under concurrent access. Its use is only safe for logic that depends on the value's data, not its object identity (eq/ne). */
	@threadUnsafe lazy val Venture_unit: Venture[Unit] = Venture_ready(successUnit)

	/** An always successful ready [[Venture]] that yields [[true]].\
	 * Equivalent to {{{Venture_successful[true](true}}}\
	 * CAUTION: This @threadUnsafe lazy val does not guarantee a unique instance under concurrent access. Its use is only safe for logic that depends on the value's data, not its object identity (eq/ne). */
	@threadUnsafe lazy val Venture_true: Venture[true] = Venture_ready(successTrue)

	/** An always successful ready [[Venture]] that yields [[false]].\
	 * Equivalent to {{{Venture_successful[false](false)}}}\
	 * CAUTION: This @threadUnsafe lazy val does not guarantee a unique instance under concurrent access. Its use is only safe for logic that depends on the value's data, not its object identity (eq/ne). */
	@threadUnsafe lazy val Venture_false: Venture[false] = Venture_ready(successFalse)

	/** A [[Venture]] whose execution never ends. */
	@threadUnsafe lazy val Venture_never: Venture[Nothing] = new Venture_Never()

	/** Creates a [[Venture]] whose result is calculated at the call site even before the returned [[Venture]] is constructed. The result of its execution is always the provided value.\
	 * $threadSafe
	 * @param tryA the value that the returned [[Venture]] will give as result every time it is executed.
	 * @return the [[Venture]] described in the method description. */
	inline final def Venture_ready[A](tryA: Try[A]): Venture[A] = new Venture_Ready(tryA)

	/** Creates a ready [[Venture]] that always succeeds with a result that is calculated at the call site even before the [[Venture]] is constructed. The result of its execution is always a [[Success]] with the provided value.\
	 * $threadSafe
	 * @param a the value contained in the [[Success]] that the returned [[Venture]] will give as result every time it is executed.
	 * @return the [[Venture]] described in the method description. */
	inline final def Venture_successful[A](a: A): Venture[A] = Venture_ready(Success(a))

	/** Creates a [[Venture]] that always fails with a result that is calculated at the call site even before the [[Venture]] is constructed. The result of its execution is always a [[Failure]] with the provided [[Throwable]].\
	 * $threadSafe
	 * @param throwable the exception contained in the [[Failure]] that the returned [[Venture]] will give as result every time it is executed.
	 * @return the [[Venture]] described in the method description. */
	inline final def Venture_failed[A](throwable: Throwable): Venture[A] = Venture_ready(Failure(throwable))

	/** Transforms a [[Duty]] to a [[Venture]] */
	def Venture_fromDuty[A](duty: Duty[Try[A]]): Venture[A] =
		(onComplete: Try[A] => Unit) => duty.engage(onComplete)

	/** Creates a [[Venture]] whose result is the result of the provided supplier.\
	 * **Detailed behavior:**
	 * Creates a [[Venture]] that, when executed, evaluates the `resultSupplier` within the $DoSerEx. If the evaluation finishes:
	 *  - abruptly, completes with a [[Failure]] with the cause.
	 *  - normally, completes with the evaluation's result.\
	 * $$threadSafe
	 * @param supplier the supplier of the result. $isExecutedByDoSerEx $unhandledErrorsArePropagatedToVentureResult
	 * @return the [[Venture]] described in the method description. */
	inline final def Venture_own[A](supplier: () => Try[A]): Venture[A] = new Venture_Own(supplier)

	/** Creates a [[Venture]] whose result is the result of applying [[Successful.apply]] to the result of the provided supplier as long as the evaluation of the supplier finishes normally; otherwise its result is a failure with the cause.\
	 * **Detailed behavior:**
	 * Creates a [[Venture]] that, when executed, evaluates the `resultSupplier` within the $DoSerEx. If it finishes:
	 *  - abruptly, completes with a [[Failure]] containing the cause.
	 *  - normally, completes with a [[Success]] containing the evaluation's result.\
	 * Is equivalent to {{{ own { () => Success(resultSupplier()) } }}}\
	 * $threadSafe
	 * @param supplier the action that supplies the successful result of [[Venture]]. $isExecutedByDoSerEx $unhandledErrorsArePropagatedToVentureResult
	 * @return the [[Venture]] described in the method description. */
	inline final def Venture_mine[A](supplier: () => A): Venture[A] = new Venture_Own(() => Success(supplier()))

	/** Creates a [[Venture]] whose result is the result of the [[Venture]] returned by the provided supplier.\
	 * Is equivalent to: {{{own(supplier).flatMap(identity)}}} but slightly more efficient.\
	 * **Detailed behavior:**
	 * Creates a [[Venture]] that, when executed, evaluates the `supplier` within the $DoSerEx. If the evaluation finishes:
	 *  - abruptly, completes with a [[Failure]] with the cause.
	 *  - normally, triggers an execution of the returned [[Venture]] and completes with its result.\
	 * $threadSafe
	 * @param supplier the supplier of the result. $isExecutedByDoSerEx $unhandledErrorsArePropagatedToVentureResult
	 * @return the [[Venture]] described in the method description. */
	inline def Venture_ownFlat[A](supplier: () => Venture[A]): Venture[A] = new Venture_OwnFlat(supplier)

	/** Create a [[Venture]] whose result will be the result of the provided [[Future]] when it completes.\
	 * Useful to access the result of a process that was already started in an alien executor as if it were executed sequentially.\
	 * $threadSafe
	 * @param future the future to wait for.
	 * @return the [[Venture]] described in the method description. */
	inline final def Venture_wait[A](future: Future[A]): Venture[A] = new Venture_Wait(future)

	/** Creates a [[Venture]] whose result will be the result of the [[Future]] returned by the provided supplier.\
	 * Useful to start a process in an alien executor and access its result as if it were executed sequentially.\
	 * The alien executor may be the $DoSerEx of this [[Doer]].\
	 * $threadSafe
	 * @param supplier a function that starts the process and return a [[Future]] of its result. $isExecutedByDoSerEx $unhandledErrorsArePropagatedToVentureResult
	 * @return the [[Venture]] described in the method description. */
	inline final def Venture_alien[A](supplier: () => Future[A]): Venture[A] = new Venture_Alien(supplier)

	/** Creates a [[Venture]] that triggers the execution of the provided [[Venture]] by another [[Doer]], and yields its result.\
	 * When triggered, the `foreignVenture` is executed within the `foreignDoer` (in sequence with whatever the `foreignDoer` is doing), and its result is supplied by the created [[Venture]] in sequence with this [[Doer]].\
	 * Useful to start a process in a another [[Doer]] and access its result sequentially.\
	 * $threadSafe
	 * @param foreignDoer the [[Doer]] to whom the `foreignVenture` belongs.
	 * @param foreignVenture the [[Venture]] to be executed by the `foreignDoer`. Its result will be yielded by the returned [[Venture]] in sequence with this [[Doer]].
	 * @return a [[Venture]] that produces what the `foreignVenture` produces, but the result is yielded in sequence with this [[Doer]]. */
	inline final def Venture_foreign[A](foreignDoer: Doer)(foreignVenture: foreignDoer.Venture[A]): Venture[A] = {
		if foreignDoer eq thisDoer then foreignVenture.asInstanceOf[thisDoer.Venture[A]]
		else new Venture_Foreign(foreignDoer, foreignVenture)
	}

	/** Creates a [[Venture]] that simultaneously triggers an execution for each of two [[Venture]] instances and returns their results combined with the received function.\
	 * Given the serial-execution nature of [[Doer]] this operation only has sense when the received [[Venture]]s are a chain of actions that involve timers, foreign, or alien actions.\
	 * **Detailed behavior:**
	 * Creates a new [[Venture]] that, when executed:
	 *  - triggers an execution for each [[Venture]] and when both are completed, whether normally or abruptly, the function `f` is applied to their results; and, if the evaluation finishes:
	 *    - abruptly, completes with a [[Failure]] containing the cause.
	 *    - normally, completes with the evaluation's result.\
	 * $threadSafe
	 * @param ventureA a [[Venture]]
	 * @param ventureB a [[Venture]]
	 * @param f the function that combines the results. $isExecutedByDoSerEx $unhandledErrorsArePropagatedToVentureResult
	 * @return the [[Venture]] described in the method description. */
	inline final def Venture_combine[A, B, C](ventureA: Venture[A], ventureB: Venture[B])(f: (Try[A], Try[B]) => Try[C]): Venture[C] =
		new Venture_Combined(ventureA, ventureB, f)

	/** Creates a [[Venture]] that, when executed, simultaneously triggers an execution for each [[Venture]]s in the received list, and completes with a list containing their results in the same order.\
	 * This overload only accepts [[List]]s and is only convenient when the list is small. For large ones it is not efficient and also may cause stack-overflow when the [[Venture]] is executed.\
	 * Use the other overload for large lists or other kind of iterables.\
	 * $threadSafe
	 * @param ventures the list of [[Venture]]s that the returned [[Venture]] will trigger simultaneously to combine their results.
	 * @return the [[Venture]] described in the method description. */
	final def Venture_sequence[A](ventures: List[Venture[A]]): Venture[List[A]] = {
		@tailrec
		def loop(incompleteResult: Venture[List[A]], remainingVentures: List[Venture[A]]): Venture[List[A]] = {
			remainingVentures match {
				case Nil =>
					incompleteResult
				case head :: tail =>
					val lessIncompleteResult = Venture_combine(incompleteResult, head) { (tla, ta) =>
						for {
							la <- tla
							a <- ta
						} yield a :: la
					}
					loop(lessIncompleteResult, tail)
			}
		}

		ventures.reverse match {
			case Nil => Venture_successful(Nil)
			case lastVenture :: previousVentures => loop(lastVenture.map(List(_)), previousVentures);
		}
	}

	/** Creates a [[Venture]] that, when executed, simultaneously triggers and execution for each [[Venture]]s in the received list, and completes with a list containing their results in the same order if all are successful, or a Failure if anyone is faulty.\
	 * This overload accepts any [[Iterable]] and is more efficient than the other (above). Especially for large iterables.\
	 * $threadSafe
	 * @param factory the [[IterableFactory]] needed to build the [[Iterable]] that will contain the results. Note that most [[Iterable]] implementations' companion objects are an [[IterableFactory]].
	 * @param ventures the `Iterable` of [[Venture]] instances that the returned [[Venture]] will trigger simultaneously to combine their results.
	 * @tparam A the result type of all the [[Venture]] instances.
	 * @tparam C the higher-kinded type of the [[Iterable]] of [[Venture]]s.
	 * @tparam To the type of the [[Iterable]] that will contain the results.
	 * @return the [[Venture]] described in the method description. */
	def Venture_sequence[A: ClassTag, C[x] <: Iterable[x], To[x] <: Iterable[x]](factory: IterableFactory[To], ventures: C[Venture[A]]): Venture[To[A]] = {
		Venture_sequenceToArray(ventures).map { array =>
			val builder = factory.newBuilder[A]
			var index = 0
			while index < array.length do {
				builder.addOne(array(index))
				index += 1
			}
			builder.result()
		}
	}

	/** Like [[Venture_sequence]] but the resulting collection's higher-kinded type `To` is fixed to [[Array]]. */
	inline def Venture_sequenceToArray[A: ClassTag, C[x] <: Iterable[x]](ventures: C[Venture[A]]): Venture[Array[A]] = new Venture_Sequence[A, C](ventures)


	/** Creates a [[Duty]] that, when executed, simultaneously triggers an execution for each [[Venture]] in the received [[Iterable]], and completes with an [[Iterable]] containing their results, successful or not, in the same order.\
	 * $threadSafe \
	 * TODO change return type to [[Duty]] to better expose the fact that always yields a successful result
	 * @param ventures the [[Iterable]] of [[Venture]]s that the returned [[Duty]] will trigger simultaneously to combine their results.
	 * @param factory the [[IterableFactory]] needed to build the [[Iterable]] that will contain the results. Note that most [[Iterable]] implementations' companion objects are an [[IterableFactory]].
	 * @tparam A the result type of all the provided [[Venture]]s.
	 * @tparam C the higher-kinded type of the [[Iterable]] of [[Venture]]s.
	 * @tparam To the higher-kinded type of the [[Iterable]] that will contain the results.
	 * @return the successful duty described in the method description. */
	def Duty_sequenceVentures[A: ClassTag, C[x] <: Iterable[x], To[x] <: Iterable[x]](factory: IterableFactory[To], ventures: C[Venture[A]]): Duty[To[Try[A]]] = {
		Duty_sequenceVenturesToArray(ventures).map { array =>
			val builder = factory.newBuilder[Try[A]]
			var index = 0
			while index < array.length do {
				builder.addOne(array(index))
				index += 1
			}
			builder.result()
		}
	}

	/** Like [[Duty_sequenceVentures]] but the resulting collection's higher-kinded type `To` is fixed to [[Array]]. */
	inline def Duty_sequenceVenturesToArray[A: ClassTag, C[x] <: Iterable[x]](ventures: C[Venture[A]]): Duty[Array[Try[A]]] =
		new Duty_SequenceHardy[A, C](ventures)

	//// Venture concrete implementations used internally ////

	/** $suppressSyntheticCompanionObject */
	private inline def Venture_Never(trap: Nothing): Any = trap

	/** A [[Venture]] that never completes.\
	 * $onCompleteExecutedByDoSerEx */
	final class Venture_Never extends AbstractVenture[Nothing] {
		override def engage(onComplete: Try[Nothing] => Unit): Unit = ()

		override def toString: String = deriveToString[Venture_Never](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Venture_fromDuty(trap: Nothing): Any = trap

	final class Venture_fromDuty[A](cA: Duty[A]) extends AbstractVenture[A] {
		override def engage(onComplete: Try[A] => Unit): Unit = cA.engage(onComplete.compose(Success.apply))

		override def toString: String = deriveToString[Venture_fromDuty[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Venture_Ready(trap: Nothing): Any = trap

	final class Venture_Ready[A](tryA: Try[A]) extends AbstractVenture[A] {
		override def engage(onComplete: Try[A] => Unit): Unit = onComplete(tryA)

		override def toString: String = deriveToString[Venture_Ready[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Venture_Own(trap: Nothing): Any = trap

	final class Venture_Own[+A](supplier: () => Try[A]) extends AbstractVenture[A] {
		override def engage(onComplete: Try[A] => Unit): Unit = {
			val result =
				try supplier()
				catch {
					case NonFatal(e) => Failure(e)
				}
			onComplete(result)
		}

		override def toString: String = deriveToString[Venture_Own[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def TVenture_OwnFlat(trap: Nothing): Any = trap

	final class Venture_OwnFlat[+A](supplier: () => Venture[A]) extends AbstractVenture[A] {
		override def engage(onComplete: Try[A] => Unit): Unit = {
			val venturesA =
				try supplier()
				catch {
					case NonFatal(e) => Venture_failed(e)
				}
			venturesA.engage(onComplete)
		}

		override def toString: String = deriveToString[Venture_OwnFlat[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Venture_Wait(trap: Nothing): Any = trap

	final class Venture_Wait[+A](future: Future[A]) extends AbstractVenture[A] {
		override def engage(onComplete: Try[A] => Unit): Unit = {
			// Note that passing the `onComplete` operand directly to the `future.onComplete` method would break the error management contract: "exceptions thrown by the `onComplete` operand passed to `engage` should not be caught".
			future.onComplete { tryA =>
				thisDoer.run(onComplete(tryA))
			}(using ownSingleThreadExecutionContext)
		}

		override def toString: String = deriveToString[Venture_Wait[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Venture_Alien(trap: Nothing): Any = trap

	final class Venture_Alien[+A](builder: () => Future[A]) extends AbstractVenture[A] {
		override def engage(onComplete: Try[A] => Unit): Unit = {
			val future =
				try builder()
				catch {
					case NonFatal(e) => Future.failed(e)
				}
			// Note that passing the `onComplete` operand directly to the `future.onComplete` method would break the error management contract: "exceptions thrown by the `onComplete` operand passed to `engage` should not be caught".
			future.onComplete { tryA =>
				thisDoer.run(onComplete(tryA))
			}(using ownSingleThreadExecutionContext)
		}

		override def toString: String = deriveToString[Venture_Alien[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Venture_Foreign(trap: Nothing): Any = trap

	final class Venture_Foreign[+A](foreignDoer: Doer, foreignVenture: foreignDoer.Venture[A]) extends AbstractVenture[A] {
		override def engage(onComplete: Try[A] => Unit): Unit =
			foreignVenture.trigger(false) { tryA => run(onComplete(tryA)) }

		override def toString: String = deriveToString[Venture_Foreign[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Venture_Consume(trap: Nothing): Any = trap

	final class Venture_Consume[A](ventureA: Venture[A], consumer: Try[A] => Unit) extends AbstractVenture[Unit] {
		override def engage(onComplete: Try[Unit] => Unit): Unit = {
			ventureA.engage { tryA =>
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

		override def toString: String = deriveToString[Venture_Consume[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Venture_WithFilter(trap: Nothing): Any = trap

	final class Venture_WithFilter[A](ventureA: Venture[A], predicate: A => Boolean) extends AbstractVenture[A] {
		override def engage(onComplete: Try[A] => Unit): Unit = {
			ventureA.engage {
				case sa@Success(a) =>
					val predicateResult =
						try {
							if predicate(a) then sa
							else Failure(new NoSuchElementException(s"Venture filter predicate is not satisfied for $a"))
						} catch {
							case NonFatal(cause) =>
								Failure(cause)
						}
					onComplete(predicateResult)

				case f@Failure(_) =>
					onComplete(f)
			}
		}

		override def toString: String = deriveToString[Venture_WithFilter[A]](this)
	}


	/** $suppressSyntheticCompanionObject */
	private inline def Venture_Transform(trap: Nothing): Any = trap

	final class Venture_Transform[+A, +B](originalVenture: Venture[A], f: Try[A] => Try[B]) extends AbstractVenture[B] {
		override def engage(onComplete: Try[B] => Unit): Unit =
			originalVenture.engage { tryA => onComplete(tryA.reifyBack(f)) }

		override def toString: String = deriveToString[Venture_Transform[A, B]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Venture_Map(trap: Nothing): Any = trap

	final class Venture_Map[+A, +B](originalVenture: Venture[A], f: A => B) extends AbstractVenture[B] {
		override def engage(onComplete: Try[B] => Unit): Unit =
			originalVenture.engage { tryA => onComplete(tryA.mapFast(f)) }

		override def toString: String = deriveToString[Venture_Map[A, B]](this)
	}


	/** $suppressSyntheticCompanionObject */
	private inline def Venture_FlatMap(trap: Nothing): Any = trap

	final class Venture_FlatMap[+A, +B](ventureA: Venture[A], f: A => Venture[B]) extends AbstractVenture[B] {
		override def engage(onComplete: Try[B] => Unit): Unit = {
			ventureA.engage {
				case Success(a) =>
					val maybeVentureB = try Maybe(f(a)) catch {
						case NonFatal(e) =>
							onComplete(Failure(e))
							Maybe.empty
					}
					maybeVentureB.foreach(_.engage(onComplete))
				case failure: Failure[A] =>
					onComplete(failure.castTo[B])
			}
		}

		override def toString: String = deriveToString[Venture_FlatMap[A, B]](this)
	}


	/** $suppressSyntheticCompanionObject */
	private inline def Venture_TransformWith(trap: Nothing): Any = trap

	final class Venture_TransformWith[+A, +B](ventureA: Venture[A], f: Try[A] => Venture[B]) extends AbstractVenture[B] {
		override def engage(onComplete: Try[B] => Unit): Unit = {
			ventureA.engage(tryA =>
				tryA.reify(e =>
					onComplete(Failure(e))
				)(tryA =>
					f(tryA).engage(onComplete)
				)
			)
		}

		override def toString: String = deriveToString[Venture_TransformWith[A, B]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Venture_AndThen(trap: Nothing): Any = trap

	final class Venture_AndThen[+A](ventureA: Venture[A], consumer: Try[A] => Unit) extends AbstractVenture[A] {
		override def engage(onComplete: Try[A] => Unit): Unit = {
			ventureA.engage { tryA =>
				try consumer(tryA)
				catch {
					case NonFatal(e) => reportPanicException(e)
				}
				onComplete(tryA)
			}
		}

		override def toString: String = deriveToString[Venture_AndThen[A]](this)
	}


	/** $suppressSyntheticCompanionObject */
	private inline def Venture_Combined(trap: Nothing): Any = trap

	final class Venture_Combined[+A, +B, +C](ventureA: Venture[A], ventureB: Venture[B], f: (Try[A], Try[B]) => Try[C]) extends AbstractVenture[C] {
		override def engage(onComplete: Try[C] => Unit): Unit = {
			var ota: Maybe[Try[A]] = Maybe.empty
			var otb: Maybe[Try[B]] = Maybe.empty
			ventureA.engage { tryA =>
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
			ventureB.engage { tryB =>
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

		override def toString: String = deriveToString[Venture_Combined[A, B, C]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Venture_Sequence(trap: Nothing): Any = trap

	final class Venture_Sequence[A: ClassTag, C[x] <: Iterable[x]](ventures: C[Venture[A]]) extends AbstractVenture[Array[A]] {
		override def engage(onComplete: Try[Array[A]] => Unit): Unit = {
			val size = ventures.size
			val array = Array.ofDim[A](size)
			if size == 0 then onComplete(Success(array))
			else {
				val venturesIterator = ventures.iterator
				var completedCounter: Int = 0
				var index = 0
				while index < size do {
					val venture = venturesIterator.next()
					val ventureIndex = index
					venture.engage {
						case Success(a) =>
							array(ventureIndex) = a
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

	/** $suppressSyntheticCompanionObject */
	private inline def Duty_SequenceHardy(trap: Nothing): Any = trap

	final class Duty_SequenceHardy[A: ClassTag, C[x] <: Iterable[x]](ventures: C[Venture[A]]) extends AbstractDuty[Array[Try[A]]] {
		override def engage(onComplete: Array[Try[A]] => Unit): Unit = {
			val size = ventures.size
			val array = Array.ofDim[Try[A]](size)
			if size == 0 then onComplete(array)
			else {
				val venturesIterator = ventures.iterator
				var completedCounter: Int = 0
				var index = 0
				while index < size do {
					val venture = venturesIterator.next()
					val ventureIndex = index
					venture.engage { tryA =>
						array(ventureIndex) = tryA
						completedCounter += 1
						if completedCounter == size then onComplete(array)
					}
					index += 1
				}
			}
		}
	}


	////////////// EVER ///////////////

	/** A [[Venture]] that remembers the result of the execution that completes first, and all the others produce the same result as the first.\
	 * Once the first completion occurs the result is subsequently delivered deterministically to present and future subscribers.\
	 * Specifically, a [[Venture]] that:
	 *  - Is completed a single time and caches the result so that, once completed, subscribing a consumer executes the call-back immediately. Note that linking a down-chain subscribes the first link as consumer.
	 *  - The monadic laws are always upheld. Before completion, they can’t be observed because no result exists yet; after completion, they can be observed in the cached result.
	 *  - Allows to subscribe/unsubscribe consumers of its completion result dynamically.
	 *  - The source of determination may be intrinsic from the start (e.g. {{{ Covenant[String]().fulfillWith(anIntrinsicallyDeterminedVenture) }}}) or external (e.g. {{{ Covenant[String]().fulfill(someValueDeterminedExternally) }}}); the concrete result value is realized only at completion.\
	 * The timing and outcome of completion are not specified by this class. That behavior is delegated to subclasses; see [[Covenant]].
	 * @note Triggering (calling [[trigger]]) on a pending [[LatchingVenture]] does not trigger the execution of the subscribed consumers, but just subscribes the `onComplete` call-back passed to [[trigger]] as a consumer of the future result. */
	sealed abstract class LatchingVenture[+A] extends AbstractVenture[A], Idempotent[Try[A]] { thisLatchingVenture =>

		inline def asVenture: Venture[A] = this

		override def reconcile: LatchingDuty[Try[A]] = {
			maybeResult.fold {
				val covenant = new Covenant[Try[A]]
				subscribe(tryA => covenant.fulfillUnsafe(tryA))
				covenant
			} { tryA => ReadyDuty(tryA) }
		}

		override def withFilter(predicate: A => Boolean): LatchingVenture[A] = {
			thisLatchingVenture match {
				case commitment: Commitment[A] @unchecked => commitment.withFilter(predicate)
				case ready: ReadyVenture[A] => ready.withFilter(predicate)
			}
		}

		/** Transform this [[LatchingVenture]] by applying the given function to the result of this [[LatchingVenture]]. Analogous to [[Future.transform]]
		 * **Detailed description:**
		 * Creates a [[LatchingVenture]] that yields the result of applying the provided function to the result of this [[LatchingVenture]].
		 * If the evaluation of the provided function finishes:
		 *  - abruptly, completes with the cause.
		 *  - normally, completes with the result of the evaluation.\
		 * $threadSafe
		 * @param f the function applied to the result of this [[LatchingVenture]] to obtain the result of the returned [[LatchingVenture]].\
		 * $isExecutedByDoSerEx \
		 * $unhandledErrorsArePropagatedToVentureResult */
		override def transform[B](f: Try[A] => Try[B]): LatchingVenture[B] = {
			thisLatchingVenture match {
				case commitment: Commitment[A] @unchecked => commitment.transform(f)
				case ready: ReadyVenture[A] => ready.transform(f)
			}
		}


		/** Transforms this [[LatchingVenture]] by applying the provided function to the result of this [[LatchingVenture]] and then subscribing to the [[LatchingVenture]] returned by said function.\
		 * The returned [[LatchingVenture]] will be already completed if, and only if, this [[LatchingVenture]] is already completed.\
		 * @param f a function that is applied to the result of this [[LatchingVenture]] execution, to build a [[LatchingVenture]] that is executed next to produce the result that the [[LatchingVenture]] returned by this method yields.\
		 * $isExecutedByDoSerEx \
		 * $unhandledErrorsArePropagatedToVentureResult */
		def transformWith[B](f: Try[A] => LatchingVenture[B]): LatchingVenture[B] = {
			thisLatchingVenture match {
				case commitment: Commitment[A] @unchecked => commitment.transformWith(f)
				case ready: ReadyVenture[A] => ready.transformWith(f)
			}
		}

		/** Transforms this [[LatchingDuty]] by applying the given function to the result if it is successful. Analogous to [[Future.map]].\
		 * Equivalent to {{{ transform(_ map f) }}} but more efficient (one less closure allocation).\
		 * See [[recover]] and [[reconcile]] if you want to transform the failures; and [[transform]] if you want to transform both, successful and failed ones.\
		 * **Detailed behavior:**
		 * Creates a [[LatchingVenture]] that yields the result of applying the provided function to the result of this [[LatchingVenture]].
		 * If the evaluation of the provided function finishes:
		 *  - abruptly, completes with that failure.
		 *  - normally, apply `f` to `a` and if the evaluation finishes:
		 *    - abruptly with `cause`, completes with `Failure(cause)`.
		 *    - normally with value `b`, completes with `Success(b)`.\
		 * @param f a function that transforms the result of this [[Venture]], when it is successful.\
		 * $isExecutedByDoSerEx \
		 * $unhandledErrorsArePropagatedToVentureResult */
		override def map[B](f: A => B): LatchingVenture[B] = {
			thisLatchingVenture match {
				case commitment: Commitment[A] @unchecked => commitment.map(f)
				case ready: ReadyVenture[A] => ready.map(f)
			}
		}

		/** Transforms this [[LatchingVenture]] by applying the provided function to the result of this [[LatchingVenture]] and then subscribing-to the [[LatchingVenture]] returned by said function.\
		 * The returned [[LatchingVenture]] will be already completed if, and only if, this [[LatchingVenture]] is already completed.\
		 * $threadSafe \
		 * @param f a function that is applied to the result of this [[Venture]] execution to return a [[Venture]] that is executed next to produce the result that the [[Venture]] returned by this method yields.\
		 * $isExecutedByDoSerEx \
		 * $notGuarded */
		def flatMap[B](f: A => LatchingVenture[B]): LatchingVenture[B] = {
			thisLatchingVenture match {
				case commitment: Commitment[A] @unchecked => commitment.flatMap(f)
				case ready: ReadyVenture[A] => ready.flatMap(f)
			}
		}

		/** Returns this [[LatchingVenture]] after subscribing the provided side-effecting procedure to it.\
		 * If this [[LatchingVenture]] is already completed, the provided side-effecting procedure is executed synchronously (before this method returns).\
		 * Otherwise, the provided side-effecting procedure is scheduled to run upon completion in subscription order (after sequentially running all the previously subscribed result consumers).\
		 * Note that the implicit subscription done when chaining an operation to this one occurs after the subscription of the provided side-effecting procedure, se they are ran after the provided side-effecting procedure.
		 * @note CAUTION: Must be called within the $DoSerEx */
		override final def andThen(sideEffect: Try[A] => Unit): LatchingVenture[A] = {
			thisLatchingVenture.subscribe(sideEffect)
			thisLatchingVenture
		}
	}

	//// LATCHING TASK FACTORY METHODS ////

	/** Creates a [[LatchingVenture]] that is already completed if the provided value is defined, or is pending otherwise. */
	inline final def LatchingVenture[A](fixedResult: Maybe[Try[A]]): LatchingVenture[A] =
		fixedResult.fold(Commitment())(tryA => new ReadyVenture(tryA))

	/** Creates an already completed [[LatchingVenture]].
	 * @param immediateResult the immediate result that this [[LatchingVenture]] yields. */
	inline final def LatchingVenture_ready[A](immediateResult: Try[A]): LatchingVenture[A] =
		LatchingVenture(Maybe(immediateResult))

	/** An already completed [[LatchingVenture]] that yields [[Unit]].\
	 * CAUTION: This @threadUnsafe lazy val does not guarantee a unique instance under concurrent access. Its use is only safe for logic that depends on the value's data, not its object identity (eq/ne). */
	@threadUnsafe lazy final val LatchingVenture_unit: ReadyVenture[Unit] = ReadyVenture(Doer.successUnit)

	/** An already completed [[LatchingVenture]] that yields `true`.\
	 * CAUTION: This @threadUnsafe lazy val does not guarantee a unique instance under concurrent access. Its use is only safe for logic that depends on the value's data, not its object identity (eq/ne). */
	@threadUnsafe lazy final val LatchingVenture_true: ReadyVenture[true] = ReadyVenture(Doer.successTrue)

	/** An already completed [[LatchingVenture]] that yields `false`.\
	 * CAUTION: This @threadUnsafe lazy val does not guarantee a unique instance under concurrent access. Its use is only safe for logic that depends on the value's data, not its object identity (eq/ne). */
	@threadUnsafe lazy final val LatchingVenture_false: ReadyVenture[false] = ReadyVenture(Doer.successFalse)

	//// READY TASK ////

	/** A [[LatchingVenture]] that is fulfilled since its inception. */
	final class ReadyVenture[+A](val value: Try[A]) extends LatchingVenture[A] { thisReadyVenture =>

		override def engage(onComplete: Try[A] => Unit): Unit =
			onComplete(value)

		override def maybeResult: Maybe[Try[A]] =
			Maybe(value)

		override def subscribe(consumer: Try[A] => Unit): Unit = {
			checkWithin()
			consumer(value)
		}

		override def unsubscribe(onComplete: Try[A] => Unit): Unit =
			()

		override def isSubscribed(onComplete: Try[A] => Unit): Boolean =
			false

		override def toFutureHardy(isWithinDoSerEx: Boolean = isInSequence): Future[Try[A]] =
			Future.successful(value)

		override def withFilter(predicate: A => Boolean): ReadyVenture[A] = {
			value.foldAndThenReify(
				_ => thisReadyVenture,
				e => ReadyVenture(Failure(e)),
				a => if predicate(a) then thisReadyVenture else ReadyVenture(Failure(new NoSuchElementException(s"ReadyVenture filter predicate is not satisfied for $a")))
			)
		}


		override def transform[B](f: Try[A] => Try[B]): ReadyVenture[B] = {
			checkWithin()
			ReadyVenture(f(thisReadyVenture.value))
		}

		override def transformWith[B](f: Try[A] => LatchingVenture[B]): LatchingVenture[B] = {
			checkWithin()
			f(thisReadyVenture.value)
		}

		override def map[B](f: A => B): ReadyVenture[B] = {
			checkWithin()
			ReadyVenture(thisReadyVenture.value.map(f))
		}

		override def flatMap[B](f: A => LatchingVenture[B]): LatchingVenture[B] = {
			thisReadyVenture.value match {
				case success: Success[A] =>
					checkWithin()
					f(success.value)
				case failure: Failure[A] =>
					ReadyVenture(failure.castTo[B])
			}
		}
	}

	inline final def ReadyVenture[A](tryA: Try[A]): ReadyVenture[A] = new ReadyVenture(tryA)

	////////////// COMMITMENT ///////////////

	/** A [[LatchingVenture]] with dynamic control of its completion (the execution of the subscribed consumers).\
	 * It exposes methods such as [[complete]] and [[completeWith]] to allow external code to complete it.\
	 * Analogous to [[scala.concurrent.Promise]] but for [[Venture]]s instead of a [[scala.concurrent.Future]]s. */
	final class Commitment[A] @publicInBinary private[Doer](initialResult: Maybe[Try[A]]) extends LatchingVenture[A], SubscriptionHub[Try[A]] { thisCommitment =>
		private var oResult: Maybe[Try[A]] = initialResult

		def this() = this(Maybe.empty)

		/** The [[LatchingVenture]] whose completion is controlled by this [[Commitment]].\
		 * Provided to mimic containment semantics, allowing external code to treat this [[Commitment]] as if it exposed a separate [[LatchingVenture]] field.\
		 * @return this [[Commitment]] as a [[LatchingVenture]] */
		inline def asLatchingVenture: LatchingVenture[A] = thisCommitment

		override def engage(onComplete: Try[A] => Unit): Unit =
			oResult.fold(attach(onComplete))(onComplete)

		override def maybeResult: Maybe[Try[A]] = {
			checkWithin()
			oResult
		}

		override def subscribe(consumer: Try[A] => Unit): Unit = {
			checkWithin()
			oResult.fold(attach(consumer))(consumer)
		}

		override def unsubscribe(onComplete: Try[A] => Unit): Unit = {
			checkWithin()
			detach(onComplete)
		}

		override def isSubscribed(onComplete: Try[A] => Unit): Boolean = {
			checkWithin()
			isAttached(onComplete)
		}

		override def withFilter(predicate: A => Boolean): LatchingVenture[A] = {
			checkWithin()
			thisCommitment.maybeResult.fold {
				val commitment = new Commitment[A]
				thisCommitment.engage { tryA =>
					tryA.foldAndThenReify(
						commitment.completeUnsafe(_),
						e => commitment.completeUnsafe(Failure(e)),
						a => if predicate(a) then commitment.completeUnsafe(Success(a)) else commitment.completeUnsafe(Failure(new NoSuchElementException(s"LatchingVenture filter predicate is not satisfied for $a")))
					)
				}
				commitment
			} { tryA =>
				tryA.foldAndThenReify(
					_ => thisCommitment,
					e => ReadyVenture(Failure(e)),
					a => if predicate(a) then thisCommitment else ReadyVenture(Failure(new NoSuchElementException(s"LatchingVenture filter predicate is not satisfied for $a")))
				)
			}
		}

		override def transform[B](f: Try[A] => Try[B]): LatchingVenture[B] = {
			checkWithin()
			thisCommitment.maybeResult.fold {
				val commitment = Commitment[B]()
				thisCommitment.engage { tryA => commitment.completeUnsafe(tryA.reifyBack(f)) }
				commitment
			} { tryA =>
				ReadyVenture[B](tryA.reifyBack(f))
			}
		}

		override def transformWith[B](f: Try[A] => LatchingVenture[B]): LatchingVenture[B] = {
			checkWithin()
			thisCommitment.maybeResult.fold {
				val commitment = new Commitment[B]
				thisCommitment.engage(tryA =>
					tryA.reify(e =>
						commitment.completeUnsafe(Failure(e))
					)(tryA =>
						f(tryA).engage(tryB => commitment.completeUnsafe(tryB))
					)
				)
				commitment
			}(_.reify(e => ReadyVenture(Failure(e)))(f))
		}

		override def map[B](f: A => B): LatchingVenture[B] = {
			checkWithin()
			thisCommitment.maybeResult.fold {
				val commitment = new Commitment[B]
				thisCommitment.engage { tryA => commitment.completeUnsafe(tryA.mapFast(f)) }
				commitment
			} { tryA =>
				ReadyVenture(tryA.mapFast(f))
			}
		}

		override def flatMap[B](f: A => LatchingVenture[B]): LatchingVenture[B] = {
			checkWithin()
			thisCommitment.maybeResult.fold {
				val commitment = new Commitment[B]
				thisCommitment.engage {
					case success: Success[A] =>
						val maybeVentureB = try Maybe(f(success.value)) catch {
							case NonFatal(e) =>
								commitment.completeUnsafe(Failure(e))
								Maybe.empty
						}
						maybeVentureB.foreach(_.engage(tryB => commitment.completeUnsafe(tryB)))
					case failure: Failure[A] =>
						commitment.completeUnsafe(failure.castTo[B])
				}
				commitment
			}(_.foldAndThenReify(ReadyVenture(_), e => ReadyVenture(Failure(e)), f))
		}

		/** Completes this [[Commitment]] with the given `result`, unless it has already been completed at the time the completion is performed.\
		 * Completion is performed:
		 *  - Synchronously (before this method returns) if `isWithinDoSerEx` is true.
		 *  - Asynchronously as soon as possible otherwise.\
		 * This method delegates to [[completeUnsafe]], scheduling it within this [[Doer]]'s sequential executor if not already executing within it.\
		 * @param result the value to complete this [[Commitment]] with.
		 * @param isWithinDoSerEx $isWithinDoSerEx
		 * @param onCompleted optional callback invoked with the final result and information about its origin: [[ANOTHER_BEFORE]] if the result is a previously set one; [[THE_PROVIDED]] if the result is the one provided here. */
		inline def complete(result: Try[A], inline isWithinDoSerEx: Boolean = isInSequence, onCompleted: (Try[A], ImmediateResultOrigin) => Unit = (_: Try[A], _: ImmediateResultOrigin) => ()): thisCommitment.type = {
			if isWithinDoSerEx then completeUnsafe(result, onCompleted)
			else {
				run(completeUnsafe(result, onCompleted))
				thisCommitment
			}
		}

		/** Completes this [[Commitment]] with the given `result`, unless it has already been completed.\
		 * If this [[Commitment]] is not yet completed, the provided `result` becomes its final value and is made immediately visible to all subscribers.\
		 * If it is already completed, the provided `result` is ignored.\
		 * @note CAUTION: must be called from within this [[Doer]].
		 * @param result the value to complete this [[Commitment]] with.
		 * @param onCompleted optional callback invoked synchronously (before this method returns) with the fulfilling value and information about its origin:
		 *  - [[THE_PROVIDED]] if this [[Covenant]] was completed by this method call with the provided value;
		 *  - [[ANOTHER_BEFORE]] if this [[Covenant]] was already completed when this method was called. */
		def completeUnsafe(result: Try[A], onCompleted: (Try[A], ImmediateResultOrigin) => Unit = (_: Try[A], _: ImmediateResultOrigin) => ()): thisCommitment.type = {
			checkWithin()
			oResult.fold {
				// First, set the result.
				this.oResult = Maybe(result)
				// Second, run the consumers in subscriptions order
				this.capture(result)
				// Finally. call the provided call-back.
				try onCompleted(result, THE_PROVIDED)
				catch {
					case NonFatal(e) => reportPanicException(e)
				}
			} { value =>
				try onCompleted(value, ANOTHER_BEFORE)
				catch {
					case NonFatal(cause) => reportPanicException(cause)
				}
			}
			thisCommitment
		}

		/** Fulfills this [[Commitment]] with the given `result`, unless it has already been completed at the time the fulfillment is performed.\
		 * Fulfillment is performed:
		 *  - Synchronously (before this method returns) if `isWithinDoSerEx` is true.
		 *  - Asynchronously as soon as possible otherwise.\
		 * This method delegates to [[completeUnsafe]], scheduling it within this [[Doer]]'s sequential executor if not already executing within it.\
		 * @param result the value to complete this [[Commitment]] with.
		 * @param isWithinDoSerEx $isWithinDoSerEx
		 * @param onCompleted optional callback invoked with the final result and a boolean indicating whether this [[Commitment]] was already completed.\
		 * If `true`, the result is a previously set one; if `false`, the result is the one provided here.
		 */
		inline def fulfill(result: A, inline isWithinDoSerEx: Boolean = isInSequence, onCompleted: (Try[A], ImmediateResultOrigin) => Unit = (_, _) => ()): this.type =
			complete(Success(result), isWithinDoSerEx, onCompleted)

		/** Breaks this [[Commitment]] with the given `excuse`, unless it has already been completed at the time the fulfillment is performed.\
		 * Fulfillment is performed:
		 *  - Synchronously (before this method returns) if `isWithinDoSerEx` is true.
		 *  - Asynchronously as soon as possible otherwise.\
		 * This method delegates to [[completeUnsafe]], scheduling it within this [[Doer]]'s sequential executor if not already executing within it.\
		 * @param excuse the [[Failure]] to break this [[Commitment]] with.
		 * @param isWithinDoSerEx $isWithinDoSerEx
		 * @param onCompleted optional callback invoked with the final result and a boolean indicating whether this [[Commitment]] was already completed.\
		 * If `true`, the result is a previously set one; if `false`, the result is the one provided here. */
		inline def break(excuse: Throwable, inline isWithinDoSerEx: Boolean = isInSequence, onCompleted: (Try[A], ImmediateResultOrigin) => Unit = (_: Try[A], _: ImmediateResultOrigin) => ()): thisCommitment.type =
			complete(Failure(excuse), isWithinDoSerEx, onCompleted)

		/** Wires this [[Commitment]] to the completion of a [[LatchingVenture]].\
		 * Arranges this [[Commitment]] to be completed if `completingVenture`` completes, unless it was completed before.\
		 * Always one, and only one, of the two callback is invoked:
		 *  - `onAlreadyCompleted` if this [[Commitment]] was already completed when the subscription is done.
		 *  - `onCompletedLater` if this [[Commitment]] is completed after the subscription is done.
		 * The subscription is synchronic if `isWithinDoSerEx` is true, and asynchronic ASAP otherwise.\
		 * @param completingVenture the [[Venture]] whose result will be used to complete this [[Commitment]].
		 * @param isWithinDoSerEx informs if this method was called within this [[Doer]].
		 * @param onCompleted optional callback invoked when this [[Commitment]] is completed. The first parameter is the completing value and the second informs about its origin. Invoked within this [[Doer]] sequential executor.
		 * @throws IllegalArgumentException if `completingVenture` is the same instance as this [[Commitment]]. */
		def completeWith(completingVenture: Venture[A], isWithinDoSerEx: Boolean = isInSequence, onCompleted: (Try[A], ResultOrigin) => Unit = (_: Try[A], _: ResultOrigin) => ()): thisCommitment.type = {
			if completingVenture eq this then throw IllegalArgumentException("A Commitment can't be fulfilled with itself.")
			if isWithinDoSerEx then {
				oResult.fold {
					completingVenture.engage(result => completeUnsafe(result, onCompleted))
				} { tryA =>
					try onCompleted(tryA, ANOTHER_BEFORE)
					catch {
						case NonFatal(e) => reportPanicException(e)
					}
				}
			}
			else run(completeWith(completingVenture, true, onCompleted))
			this
		}
	}

	inline def Commitment[A](fixedResult: Maybe[Try[A]] = Maybe.empty): Commitment[A] =
		new Commitment(fixedResult)

	/** Creates a [[Commitment]] that will fulfill with the result of executing the provided supplier within the $DoSerEx.\
	 * @param supplier a supplier function that is executed within the $DoSerEx and returns the value to fulfill the created [[Commitment]] with. */
	def Commitment_own[A](supplier: () => Try[A]): Commitment[A] = {
		val commitment = new Commitment[A]
		run(commitment.completeUnsafe(supplier()))
		commitment
	}

	/** Creates a [[Commitment]] that is wired to the [[LatchingVenture]] resulting of executing the provided supplier within the $DoSerEx.\
	 * @param supplier a supplier function that is executed within the $DoSerEx to return the [[LatchingVenture]] to which the created [[Commitment]] is wired. */
	def Commitment_ownFlat[A](supplier: () => LatchingVenture[A]): Commitment[A] = {
		val commitment = new Commitment[A]
		run(supplier().subscribe(a => commitment.completeUnsafe(a)))
		commitment
	}

	/** Triggers the given [[Venture]] and returns a [[Commitment]] that will be completed with the result of the triggered execution unless this [[Commitment]] is completed before by other means.\
	 * This method triggers an execution of the given [[Venture]] and wires its result to a newly created [[Commitment]].\
	 * The returned [[Commitment]] acts as a completion handle for the execution triggered by this method, and can be used to observe or react to its result.\
	 * @param venture the [[Venture]] to be triggered.
	 * @param isWithinDoSerEx true if triggering occurs within the current [[Doer]] sequence.
	 * @param onCompleted  The first parameter is the fulfilling value and the second informs about its origin. Invoked within this [[Doer]] sequential executor.
	 * @return a [[Commitment]] that will be completed with the result of the execution triggered by this method. */
	inline def Commitment_triggerAndWire[A](venture: Venture[A], inline isWithinDoSerEx: Boolean = isInSequence, onCompleted: (Try[A], ResultOrigin) => Unit = (_: Try[A], _: ResultOrigin) => ()): Commitment[A] = {
		val commitment = Commitment[A]()
		venture.trigger(isWithinDoSerEx)(result => commitment.completeUnsafe(result, onCompleted))
		commitment
	}

	//////////////// Flow //////////////////////

	def Flow_lift[A, B](f: A => B): Flow[A, B] =
		(a: A) => Duty_ready(f(a))

	def Flow_wrap[A, B](builder: A => Duty[B]): Flow[A, B] =
		(a: A) => builder(a)

	trait Flow[A, B] { thisFlow =>

		protected def flush(a: A): Duty[B]

		inline def apply(a: A, inline isWithinDoSerEx: Boolean = isInSequence)(onComplete: B => Unit): Unit = {
			def work(): Unit = flush(a).engage(onComplete)

			if isWithinDoSerEx then work()
			else run(work())
		}

		/** Connects this flow output with the input of the received one. */
		def to[C](next: Flow[B, C]): Flow[A, C] =
			(a: A) => thisFlow.flush(a).flatMap(b => next.flush(b))

		/** Connects the received flow output with the input of this one. */
		def from[Z](previous: Flow[Z, A]): Flow[Z, B] =
			(z: Z) => previous.flush(z).flatMap(a => thisFlow.flush(a))
	}


	//////////////// Subscriptable producer ////////////////////

	/** Memorizes a value latched exactly once, allowing multiple consumers to subscribe.\
	 * @tparam A The type of the result obtained when the process completes. */
	trait Idempotent[+A] {

		/** @return the result if completed.
		 * @note CAUTION: Must be called within the $DoSerEx */
		def maybeResult: Maybe[A]

		/** @return true if this [[LatchingDuty]] was fulfilled; or false if it is still pending.
		 * @note CAUTION: Must be called within the $DoSerEx */
		inline def isCompleted: Boolean = maybeResult.isDefined

		/** @return true if this [[LatchingDuty]] is still pending; or false if it was completed.
		 * @note CAUTION: Must be called within the $DoSerEx */
		inline def isPending: Boolean = maybeResult.isEmpty

		/** Subscribes a consumer of the result of this producer.\
		 * The subscription is automatically removed after an execution of this producer has completed and the received consumer is executed.\
		 * If this producer is already fulfilled when this method is called, the provided consumer is invoked synchronously and no subscription occurs.\
		 * Otherwise, the provided consumer is schedule to run upon completion in subscription orden (after sequentially running all the previously subscribed result consumers).\
		 * @note CAUTION: This method does not prevent duplicate subscriptions.
		 * @note CAUTION: Must be called within the $DoSerEx */
		def subscribe(consumer: A => Unit): Unit

		/** Removes a subscription done with [[subscribe]].\
		 * @note CAUTION: Must be called within the $DoSerEx */
		def unsubscribe(onComplete: A => Unit): Unit

		/** @return `true` if the provided consumer is currently subscribed.
		 * @note CAUTION: Must be called within the $DoSerEx */
		def isSubscribed(onComplete: A => Unit): Boolean
	}

	/** A mixin trait that maintains a list of observers subscribed to a future result.\
	 * @tparam A The type of the result obtained when the associated process completes. */
	trait SubscriptionHub[A] {
		protected var firstOnCompleteObserver: (A => Unit) | Null = null
		protected var onCompletedObservers: List[A => Unit] = Nil // TODO Change the collection to one with a efficient iterator from first to last appended.

		protected def attach(consumer: A => Unit): Unit = {
			if firstOnCompleteObserver eq null then firstOnCompleteObserver = consumer
			else onCompletedObservers = consumer :: onCompletedObservers
		}

		protected def detach(onComplete: A => Unit): Unit = {
			if firstOnCompleteObserver eq onComplete then {
				if onCompletedObservers.isEmpty then firstOnCompleteObserver = null
				else {
					firstOnCompleteObserver = onCompletedObservers.head
					onCompletedObservers = onCompletedObservers.tail
				}
			} else onCompletedObservers = onCompletedObservers.filterNot(_ ne onComplete)
		}


		protected def isAttached(onComplete: A => Unit): Boolean = {
			(firstOnCompleteObserver eq onComplete) || onCompletedObservers.exists(_ eq onComplete)
		}

		/** Apply the provided value to each consumers in subscriptions order and then clear all the subscriptions.\
		 * @note CAUTION: Must be called within the $DoSerEx */
		protected def capture(a: A): Unit = {
			if firstOnCompleteObserver ne null then {
				try {
					firstOnCompleteObserver.nn(a)
					if onCompletedObservers.nonEmpty then {
						// TODO change this implementation to one that does not cause StackOverflow when onCompleteObserver is big..
						def loop(head: A => Unit, tail: List[A => Unit]): Unit = {
							if tail.nonEmpty then loop(tail.head, tail.tail)
							head(a)
						}

						loop(onCompletedObservers.head, onCompletedObservers.tail)
					}
				} catch {
					case NonFatal(e) => reportPanicException(e)
				} finally {
					firstOnCompleteObserver = null // unbind the observer reference to help the garbage collector
					onCompletedObservers = Nil // Clean the observers list to help the garbage collector.
				}
			}
		}
	}
}