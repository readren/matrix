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
 * @define onCompleteExecutedByDoSerEx The `onComplete` callback passed to `subscribe` is always, with no exception, executed by this $DoSerEx. This is part of the contract of the [[Venture]] trait.
 * @define threadSafe This method is thread-safe.
 * @define isExecutedByDoSerEx This function is executed within the DoSerEx (doer's serial executor).
 * @define unhandledErrorsArePropagatedToVentureResult The call to this routine is guarded with try-catch. If it throws a non-fatal exception it will be caught and the [[Venture]] will complete with a [[Failure]] containing the error.
 * @define unhandledErrorsAreReported The call to this routine is guarded with a try-catch. If the evaluation throws a non-fatal exception it will be caught and reported with [[Doer.reportFailure()]].
 * @define notGuarded CAUTION: The call to this function is NOT guarded with a try-catch. If its evaluation terminates abruptly the task will never complete. The same occurs with all routines received by [[Task]] operations. This is one of the main differences with [[Venture]] operation.
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
	 * All the deferred actions preformed by the [[Task]] and [[Venture]] operations are executed by calling this method unless the particular operation documentation says otherwise. That includes not only the call-back functions like `onComplete` but also all the functions, procedures, predicates, and by-name parameters they receive.
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
	 * All the deferred actions preformed by the [[Task]]/[[Venture]] operations are executed by calling this method unless the particular operation documentation says otherwise. That includes not only the call-back functions like `onComplete` but also all the functions, procedures, predicates, and by-name parameters they receive as.
	 * This function only makes sense to call:
	 *		- from an action that is not executed by this $DoSerEx (the callback of a [[Future]], for example);
	 *		- or to avoid a stack overflow by continuing the recursion in a new execution.
	 * @see [[submit]] and [[submitHardy]] if the result of the execution is relevant.
	 * @note Is more efficient than the functionally equivalent: `Task_mine(() => procedure).triggerAndForget()`.
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

	inline given [A] =>WirableSoft[A, Task] {
		override inline def wire(supplier: () => A): Task[A] =
			new Task_Mine(supplier)

		override inline def wireFlat(supplier: () => Task[A]): Task[A] = {
			new Task_MineFlat(supplier)
		}
	}

	inline given [A] =>WirableSoft[A, LatchingTask] {
		override inline def wire(supplier: () => A): LatchingTask[A] =
			Covenant_mine(supplier)

		override inline def wireFlat(supplier: () => LatchingTask[A]): LatchingTask[A] =
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
			run(supplier().subscribe(tryA => commitment.complete(tryA)))
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

	trait Observable[+A] {
		def subscribe(consumer: A => Unit): Unit
	}

	/////////////// TASK ///////////////

	abstract class AbstractTask[+A] extends Task[A]

	/** A lazy computation owned by this [[Doer]]. Executions are serialized across all [[Task]] instances of the same [[Doer]]. Each execution may produce a different result if the computation depends on mutable state.\
	 * Executions are performed in the order they were triggered.\
	 * A [[Task]] can encapsulate one or more chained actions and provides operations to declaratively build complex duties from simpler ones.\
	 * This tool simplifies the implementation of a handler that manages multiple simultaneous processes that interact with each other using a single sequential actor. How? By eliminating the need for state variables that determine the decision-making flow, as the code structure itself indicates the execution order.\
	 * Instances of [[Task]] whose result is always the same follow the monadic laws. However, if the result depends on the execution (because it depends on mutable variables or time), these laws may be broken.\
	 * For example, if the [[Task.subscribe]] implementation closes over mutable variables (either directly or through any of the function operands that its factory or the operations used to construct it receives) from the environment that affects its execution result, then the equality of two supposedly equivalent expressions like {{{task.flatMap(f).flatMap(g) == task.flatMap(a => f(a).flatMap(g))}}} could be compromised. This would depend on the timing of when the variables are mutated — specifically when the mutations occur between the start and end of the task's execution.\
	 * This does not mean that [[Task.subscribe]] implementations must avoid closing over mutable variables altogether. Rather, it highlights that if strict adherence to monadic laws is required by your business logic, you should ensure that the mutable variable is not modified during the execution of the involved [[Task]] instances.\
	 * If the goal is just deterministic behavior, it's sufficient that any closed-over mutable variable is only mutated and accessed by actions executed sequentially in a determined order. This is why the contract enforces serialized execution of actions in the order at which the actions were triggered: to maintain determinism, even when closing over mutable variables, provided they are mutated and accessed solely within the actions in said ordered sequence and those actions are deterministic.\
	 * If you require to ensure monadic laws are followed, use [[LatchingTask]]/[[LatchingVenture]] instead.\
	 * Design note: [[Task]] and [[Venture]] are defined as inner traits of [[Doer]] to leverage Scala's path-dependent type checking. This avoids that [[Task]]/[[Venture]] instances that belong to different [[Doer]] instances to be inadvertently composed together without the adapters needed to ensure sequential execution of the component actions.\
	 * While path-dependent type checking is valuable for enforcing this contract, it has a drawback: the compiler's type-path checks are overly strict, requiring compatible singleton types for references, whereas we only need to verify that the [[Task]] instances correspond to the same [[Doer]].\
	 * As a result, the compiler may flag type errors in cases where the contract is not violated, which is undesirable.\
	 * The [[castTypePath()]] method mitigates these false positives.\
	 * CAUTION: Unlike [[Venture]], [[Task]] is strict (non-short-circuiting) and does NOT support failures. And unlike [[Venture]], the invocation of function operands received by its operations is not guarded with a try-catch. Therefore, unlike [[Venture]], any unhandled exception thrown during an execution of a [[Task]] will break the expected flow and the task will never complete.\
	 * It is recommended to use [[Venture]] instead of [[Task]] unless efficiency is a concern.\
	 * @tparam A the type of result obtained when executing this task. */
	trait Task[+A] extends Observable[A] { thisTask =>
		/** This method performs the actions represented by the task and calls `onComplete` within the $DoSerEx when the [[Task]] finishes.\
		 * CAUTION: This method is intended to be used by extensions of [[Task]] only. Use [[trigger]] or [[foreach]] instead.
		 * The implementation may assume this method is invoked within the $DoSerEx.\
		 * The implementation must respect the following exception-handling rules:
		 * - no exception thrown by the provided callback must be caught.
		 * - any non-fatal exception throw by this method must be either caught and propagated to the result or reported using [[Doer.reportFailure]] if propagation is not feasible.\
		 * In the case of [[Venture]] this includes non-fatal exceptions originated in function operands passed to its factory, including those captured over a closure.
		 * [[Task]], on the other hand, assumes that function operands never throw exceptions. If an exception is thrown, the stack of the corresponding task execution will be completely unwound.\
		 * It is crucial to ensure that exceptions thrown by the onComplete callback are not caught, as this could suppress issues within the callback, preventing the execution of code expected to run and making it extremely difficult to diagnose the cause of a never-completing [[Task]] or [[Venture]].\
		 * This method is the sole primitive operation of this trait; all other methods are derived from it.\
		 * @param onComplete The callback that must be invoked upon the completion of this [[Task]]. The implementation should call this callback within the $DoSerEx.\
		 * The implementation may assume that `onComplete` will either terminate normally or fatally, but will not throw non-fatal exceptions. */
		override def subscribe(onComplete: A => Unit): Unit

		// override def subscribe(consumer: A => Unit): Unit = subscribe(consumer)

		/** Initiates an execution of this [[Task]] and subscribes the provided call-back as a consumer of the execution result.
		 * Each invocation of this method triggers a new execution.
		 * Note: Executions triggered on [[Task]] instances whose completion depends on other executions (e.g., a pending [[Covenant]]) will not complete until those dependent executions have themselves been triggered and completed.
		 * @param isWithinDoSerEx $isWithinDoSerEx
		 * @param onComplete Invoked when the triggered execution completes. The call-back must not throw non-fatal exceptions. It must either terminate normally or fail fatally, but never with a non-fatal exception.
		 * $isExecutedByDoSerEx */
		inline final def trigger(inline isWithinDoSerEx: Boolean = isInSequence)(inline onComplete: A => Unit): Unit = {
			${ DoerMacros.triggerImpl('isWithinDoSerEx, 'thisDoer, 'thisTask, 'onComplete) }
		}

		/** Triggers an execution of this [[Task]] ignoring the result.
		 *
		 * $threadSafe
		 *
		 * @param isWithinDoSerEx $isWithinDoSerEx */
		inline final def triggerAndForget(isWithinDoSerEx: Boolean = isInSequence): Unit =
			trigger(isWithinDoSerEx)(_ => {})

		/** Triggers an execution of this [[Task]] and then invokes the provided consumer passing the result.
		 *
		 * Is equivalent to {{{trigger(isInSequence)(consumer)}}}
		 *
		 * $threadSafe
		 * @param consumer called with this [[Task]] result when it completes.
		 *
		 * $isExecutedByDoSerEx
		 *
		 * $notGuarded */
		def foreach(consumer: A => Unit): Unit = trigger(isInSequence)(consumer)

		/**
		 * Creates a new [[Task]] that yields the result of applying the provided function to the result of this [[Task]].
		 *
		 * $threadSafe
		 *
		 * @param f a function that is applied to the result of this [[Task]] to produce the final result.
		 *
		 * $isExecutedByDoSerEx
		 *
		 * $notGuarded
		 */
		def map[B](f: A => B): Task[B] = new Task_Map(thisTask, f)

		/**
		 * Creates a new [[Task]] that yields the result of executing an intermediate [[Task]] produced by applying the provided function to the final result.
		 *
		 * $threadSafe
		 *
		 * @param f a function that is applied to the result of this [[Task]] to produce an intermediate [[Task]] that is then executed to produce the final result.
		 *
		 * $isExecutedByDoSerEx
		 *
		 * $notGuarded
		 */
		inline def flatMap[B](f: A => Task[B]): Task[B] = new Task_FlatMap(thisTask, f)

		/** Creates a new [[Task]] that yields exactly the same result (same identity) as this [[Task]] but executes the provided side-effecting function before yielding it.
		 *
		 * $threadSafe
		 *
		 * @param sideEffect a function that is applied to the result of this [[Task]] for its side effects.
		 */
		def andThen(sideEffect: A => Unit): Task[A] = new Task_AndThen[A](thisTask, sideEffect)

		/** Creates a new always successful [[Venture]] that shields the result of this [[Task]].
		 *
		 * This operation allows a [[Task]] to participate in a [[Venture]]'s short-circuiting context without itself being a short-circuit trigger.
		 *
		 * Together with [[reconcile]] this method allow to mix [[Task]] and [[Venture]] operations in the same chain.
		 * @return a [[Venture]] whose result is the result of this [[Task]] wrapped inside a [[Success]]. */
		def succeed: Venture[A] = new Venture_fromTask(thisTask)

		/** Starts an execution of this [[Task]] and returns a successful [[Future]] that yields the result. */
		def toFutureHardy(isWithinDoSerEx: Boolean = isInSequence): Future[A] = {
			val promise = Promise[A]()
			trigger(isWithinDoSerEx)(a => promise.success(a))
			promise.future
		}

		/**
		 * Wraps this [[Task]] into another that belongs to another [[Doer]].
		 * Useful to chain [[Task]]'s operations that involve different [[Doer]] instances.
		 * ===Detailed behavior===
		 * Returns a [[Task]] that belongs to the provided [[Doer]]. When it is triggered, it will trigger this task within this [[Doer]] and, when completed, make the returned [[Task]] to yield the result.
		 * CAUTION: Avoid closing over the same mutable variable from two operand functions applied to [[Task]] instances belonging to different [[Doer]]s.
		 * Remember that all function operands provided to [[Venture]] methods are executed within the [[Doer]] that owns it.
		 * Therefore, calling [[trigger]] on the returned [[Task]] will execute the `onComplete` passed to it within the `otherDoer`.
		 *
		 * $threadSafe
		 *
		 * @param otherDoer the [[Doer]] to which the returned [[Task]] will belong.
		 */
		def onBehalfOf(otherDoer: Doer): otherDoer.Task[A] =
			otherDoer.Task_foreign(thisDoer)(this)

		/** Casts the singleton type of the [[Doer]] instance that owns this [[Task]] to the singleton-type of the provided [[Doer]].
		 * This operation does nothing at runtime. It only tricks the compiler to prevent it from complaining when operating with references to the same [[Doer]] instance but through different type-paths.
		 * CAUTION: Use it only if you are sure that the provided [[Doer]] instance is the one that owns this [[Task]].
		 *
		 * Design note: It was decided to make [[Task]] (and [[Venture]]) an inner class of the [[Doer]] to take advantage of type-path checking to detect when the contract "all operand functions passed to [[Task]] (and [[Venture]]) operations owned by the same [[Doer]] are executed in sequence" might be violated, at compile time.
		 * Using type-path checking to detect contract violations is very valuable, but it comes at a cost, because the type-path check done by the compiler is stricter than necessary -- it checks that the singleton type of the references involved be compatible, and we only need to check that the involved [[Task]] instances belong to the same [[Doer]] instance.
		 * Therefore, the compiler will report type errors in situations the contract is not violated, which is not what we want.
		 * This operation ([[castTypePath()]]) is intended to handle those cases.
		 */
		def castTypePath[E <: Doer](doer: E): doer.Task[A] = {
			assert(thisDoer eq doer)
			this.asInstanceOf[doer.Task[A]]
		}
	}

	//// Task's factory methods ////

	/** A [[Task]] that yields [[Unit]].
	 * CAUTION: This @threadUnsafe lazy val does not guarantee a unique instance under concurrent access. Its use is only safe for logic that depends on the value's data, not its object identity (eq/ne). */
	@threadUnsafe lazy val Task_unit: Task[Unit] = Task_ready(())

	/** A [[Task]] that yields `true`.
	 * CAUTION: This @threadUnsafe lazy val does not guarantee a unique instance under concurrent access. Its use is only safe for logic that depends on the value's data, not its object identity (eq/ne). */
	@threadUnsafe lazy val Task_true: Task[true] = Task_ready(true)

	/** A [[Task]] that yields `false`.
	 * CAUTION: This @threadUnsafe lazy val does not guarantee a unique instance under concurrent access. Its use is only safe for logic that depends on the value's data, not its object identity (eq/ne). */
	@threadUnsafe lazy val Task_false: Task[false] = Task_ready(false)

	/** Creates a [[Task]] whose execution never ends.
	 * $threadSafe
	 *
	 * @return a [[Task]] whose execution never ends.
	 * */
	@threadUnsafe lazy val Task_never: Task[Nothing] = new Task_NotEver()

	/** Creates a [[Task]] whose result is calculated at the call site even before the task is constructed.
	 * $threadSafe
	 *
	 * @param a the already calculated result of the returned [[Task]]. */
	inline def Task_ready[A](a: A): Task[A] = new Task_Ready(a)

	/** Creates a [[Task]] that yields the value returned by the provided supplier.
	 * ===Detailed behavior===
	 * Creates a task that, when executed, evaluates the `supplier` within the $DoSerEx. If the evaluation finishes:
	 *		- abruptly, will never complete.
	 *		- normally, completes with the evaluation's result.
	 *
	 * $$threadSafe
	 * TODO rename to Task_own
	 *
	 * @param supplier the supplier of the result. $isExecutedByDoSerEx $notGuarded
	 * @return the [[Task]] described in the method description.
	 */
	inline def Task_mine[A](supplier: () => A): Task[A] = new Task_Mine(supplier)

	/** Creates a [[Task]] that yields what the [[Task]] created by the provided supplier yields.
	 * Is equivalent to: {{{Task_mine(supplier).flatMap(identity)}}} but slightly more efficient
	 * ===Detailed behavior===
	 * Creates a task that, when executed:
	 *		- evaluates the `supplier` within the $DoSerEx;
	 *		- then triggers an execution of the returned Task;
	 *		- finally completes with the result of executed task.
	 *
	 * $$threadSafe
	 * TODO rename to Task_ownFlat
	 *
	 * @param supplier the supplier of the task whose execution will give the result. $isExecutedByDoSerEx $notGuarded
	 * @return the task described in the method description.
	 */
	inline def Task_mineFlat[A](supplier: () => Task[A]): Task[A] = new Task_MineFlat(supplier)

	/** Creates a [[Task]] that triggers the execution of the provided [[Task]] by another [[Doer]], and yields its result.
	 * When triggered, the `foreignTask` is executed within the `foreignDoer` (in sequence with whatever the `foreignDoer` is doing), and its result is supplied by the created [[Task]] in sequence with this [[Doer]].
	 * Useful to start a process in a another [[Doer]] and access its result sequentially.
	 * $threadSafe
	 *
	 * @param foreignDoer the [[Doer]] to whom the `foreignTask` belongs.
	 * @param foreignTask the [[Task]] to be executed by the `foreignDoer`. Its result will be yielded by the returned [[Task]] in sequence with this [[Doer]].
	 * @return a [[Task]] that produces what the `foreignTask` produces, but the result is yielded in sequence with this [[Doer]]. */
	inline def Task_foreign[A](foreignDoer: Doer)(foreignTask: foreignDoer.Task[A]): Task[A] = {
		if foreignDoer eq thisDoer then foreignTask.asInstanceOf[thisDoer.Task[A]]
		else new Task_Foreign[A](foreignDoer, foreignTask)
	}

	/**
	 * Creates a [[Task]] that yields the result of applying the bifunction `f` to what the provided duties yield.
	 * When executed, simultaneously triggers and execution of each task and returns their results combined by the provided function.
	 * Given the serial-execution nature of [[Doer]] this operation only has sense when the provided [[Task]]s involves foreign ([[Task_foreign]]) or alien ([[Venture_alien]]) actions.
	 * ===Detailed behavior===
	 * Creates a new [[Task]] that, when executed:
	 *		- triggers an execution for both: `taskA` and `taskB`
	 *		- when both are completed, completes with the value that results of applying the function `f` to their results.
	 *
	 * $threadSafe
	 *
	 * @param taskA a [[Task]]
	 * @param taskB a [[Task]]
	 * @param f the function that combines the results of the two [[Task]] instances. $isExecutedByDoSerEx $unhandledErrorsArePropagatedToVentureResult
	 * @return the [[Task]] described in the method description.
	 */
	inline def Task_combine[A, B, C](taskA: Task[A], taskB: Task[B])(f: (A, B) => C): Task[C] =
		new Task_Combined(taskA, taskB, f)

	/**
	 * Creates a [[Task]] that, when executed, simultaneously triggers an execution for each [[Task]]s in the provided iterable, and completes with a collection containing their results in the same order if all are successful, or a failure if any is faulty.
	 * This overload is only convenient for very small lists. For large ones it is not efficient and also may cause stack-overflow when the [[Task]] is executed.
	 * Use the other overload for large lists or other kind of iterables.
	 *
	 * $threadSafe
	 *
	 * @param duties the `Iterable` of duties that the returned [[Task]] will trigger simultaneously to combine their results.
	 * @tparam A the result type of all the duties
	 * @return the task described in the method description.
	 * */
	def Task_sequence[A](duties: List[Task[A]]): Task[List[A]] = {
		@tailrec
		def loop(incompleteResult: Task[List[A]], remainingDuties: List[Task[A]]): Task[List[A]] = {
			remainingDuties match {
				case Nil =>
					incompleteResult
				case head :: tail =>
					val lessIncompleteResult = Task_combine(head, incompleteResult) { (a, as) => a :: as }
					loop(lessIncompleteResult, tail)
			}
		}

		duties.reverse match {
			case Nil => Task_ready(Nil)
			case lastTask :: previousDuties => loop(lastTask.map(List(_)), previousDuties);
		}
	}

	/**
	 * Creates a [[Task]] that, when executed, simultaneously triggers an execution for each [[Task]]s in the received iterable, and completes with a collection containing their results in the same order.
	 * This overload accepts any [[Iterable]] and is more efficient than the other (above). Especially for large iterables.
	 * $threadSafe
	 *
	 * @param factory the [[IterableFactory]] needed to build the [[Iterable]] that will contain the results. Note that most [[Iterable]] implementations' companion objects are an [[IterableFactory]].
	 * @param duties the `Iterable` of duties that the returned [[Task]] will trigger simultaneously to combine their results.
	 * @tparam A the result type of all the duties
	 * @tparam C the higher-kinded type of the `Iterable` of duties.
	 * @tparam To the type of the `Iterable` that will contain the results.
	 * @return the task described in the method description.
	 * */
	def Task_sequence[A: ClassTag, C[x] <: Iterable[x], To[_]](factory: IterableFactory[To], duties: C[Task[A]]): Task[To[A]] = {
		Task_sequenceToArray(duties).map { array =>
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
	inline def Task_sequenceToArray[A: ClassTag, C[x] <: Iterable[x]](duties: C[Task[A]]): Task[Array[A]] = new Task_Sequence[A, C](duties)

	//// Concrete implementations of [[Task]] used internally ////

	/** $suppressSyntheticCompanionObject */
	private inline def Task_FromVenture(trap: Nothing): Any = trap

	final class Task_FromVenture[A, B >: A](ventureA: Venture[A], exceptionHandler: Throwable => B) extends AbstractTask[B] {
		override def subscribe(onComplete: B => Unit): Unit = {
			ventureA.subscribe { tryA =>
				val b = tryA match {
					case Success(a) => a
					case Failure(exception) => exceptionHandler(exception)
				}
				onComplete(b)
			}
		}

		override def toString: String = deriveToString[Task_FromVenture[A, B]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_Map(trap: Nothing): Any = trap

	final class Task_Map[A, B](cA: Task[A], f: A => B) extends AbstractTask[B] {
		override def subscribe(onComplete: B => Unit): Unit =
			cA.subscribe { a => onComplete(f(a)) }

		override def toString: String = deriveToString[Task_Map[A, B]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_FlatMap(trap: Nothing): Any = trap

	final class Task_FlatMap[A, B](cA: Task[A], f: A => Task[B]) extends AbstractTask[B] {
		override def subscribe(onComplete: B => Unit): Unit = cA.subscribe { a => f(a).subscribe(onComplete) }

		override def toString: String = deriveToString[Task_FlatMap[A, B]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_AndThen(trap: Nothing): Any = trap

	final class Task_AndThen[A](taskA: Task[A], sideEffect: A => Unit) extends AbstractTask[A] {
		override def subscribe(onComplete: A => Unit): Unit =
			taskA.subscribe { a =>
				sideEffect(a)
				onComplete(a)
			}

		override def toString: String = deriveToString[Task_AndThen[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_NotEver(trap: Nothing): Any = trap

	class Task_NotEver extends AbstractTask[Nothing] {
		override def subscribe(onComplete: Nothing => Unit): Unit = ()

		override def toString: String = deriveToString[Task_NotEver](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_Ready(trap: Nothing): Any = trap

	final class Task_Ready[A](a: A) extends AbstractTask[A] {
		override def subscribe(onComplete: A => Unit): Unit = onComplete(a)

		override def toFutureHardy(isWithinDoSiThEx: Boolean = isInSequence): Future[A] = Future.successful(a)

		override def toString: String = deriveToString[Task_Ready[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_Mine(trap: Nothing): Any = trap

	final class Task_Mine[A](supplier: () => A) extends AbstractTask[A] {
		override def subscribe(onComplete: A => Unit): Unit = onComplete(supplier())

		override def toString: String = deriveToString[Task_Mine[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_MineFlat(trap: Nothing): Any = trap

	final class Task_MineFlat[A](supplier: () => Task[A]) extends AbstractTask[A] {
		override def subscribe(onComplete: A => Unit): Unit = supplier().subscribe(onComplete)

		override def toString: String = deriveToString[Task_MineFlat[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_Foreign(trap: Nothing): Any = trap

	final class Task_Foreign[A](foreignDoer: Doer, foreignTask: foreignDoer.Task[A]) extends AbstractTask[A] {
		override def subscribe(onComplete: A => Unit): Unit = foreignTask.trigger()(a => thisDoer.run(onComplete(a)))

		override def toString: String = deriveToString[Task_Foreign[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_Combined(trap: Nothing): Any = trap

	final class Task_Combined[+A, +B, +C](taskA: Task[A], taskB: Task[B], f: (A, B) => C) extends AbstractTask[C] {
		override def subscribe(onComplete: C => Unit): Unit = {
			object vars {
				var aIsCompleted: Boolean = false
				var bIsCompleted: Boolean = false
				var maybeA: AnyRef | Null = null
				var maybeB: AnyRef | Null = null
			}
			taskA.subscribe { a =>
				if vars.bIsCompleted then onComplete(f(a, vars.maybeB.asInstanceOf[B]))
				else {
					vars.aIsCompleted = true
					vars.maybeA = a.asInstanceOf[AnyRef]
				}
			}
			taskB.subscribe { b =>
				if vars.aIsCompleted then onComplete(f(vars.maybeA.asInstanceOf[A], b))
				else {
					vars.bIsCompleted = true
					vars.maybeB = b.asInstanceOf[AnyRef]
				}
			}
		}

		override def toString: String = deriveToString[Task_Combined[A, B, C]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_Sequence(trap: Nothing): Any = trap

	/** @see [[Task_sequenceToArray]] */
	final class Task_Sequence[A: ClassTag, C[x] <: Iterable[x]](duties: C[Task[A]]) extends AbstractTask[Array[A]] {
		override def subscribe(onComplete: Array[A] => Unit): Unit = {
			val size = duties.size
			val array = Array.ofDim[A](size)
			if size == 0 then onComplete(array)
			else {
				val taskIterator = duties.iterator
				var completedCounter: Int = 0
				var index = 0
				while index < size do {
					val task = taskIterator.next()
					val taskIndex = index
					task.subscribe { a =>
						array(taskIndex) = a
						completedCounter += 1
						if completedCounter == size then onComplete(array)
					}
					index += 1
				}
			}
		}
	}

	////////////// ONCE ///////////////

	/** A [Task] that remembers the result of the execution that completes first, and all the others produce the same result as the first.
	 * Once the first completion occurs the result is subsequently delivered deterministically to present and future subscribers.
	 * Specifically, a [[Task]] that:
	 *		- Is completed a single time and caches the result so that, once completed, subscribing a consumer executes the call-back immediately. Note that linking a down-chain subscribes the first link as consumer.
	 * 		- The monadic laws are always upheld. Before completion, they can’t be observed because no result exists yet; after completion, they can be observed in the cached result.
	 *		- Allows to subscribe/unsubscribe consumers of its completion result dynamically.
	 *		- The source of determination may be intrinsic from the start (e.g. {{{ Covenant[String]().fulfillWith(anIntrinsicallyDeterminedTask) }}}) or external (e.g. {{{ Covenant[String]().fulfill(someValueDeterminedExternally) }}}); the concrete result value is realized only at completion.
	 * The timing and outcome of completion are not specified by this class. That behavior is delegated to subclasses; see [[Covenant]].
	 * @note Triggering (calling [[trigger]]) on a pending [[LatchingTask]] does not trigger the execution of the subscribed consumers, but just subscribes the `onComplete` call-back passed to [[trigger]] as a consumer of the future result.
	 * */
	sealed abstract class LatchingTask[+A] extends AbstractTask[A], Latching[A] {

		/** @inheritdoc
		 * @note The override is necessary to specialize the return type; and the implementation is necessary (can't leave the method abstract) because [[Covenant]] is invariant.
		 * */
		override def succeed: LatchingVenture[A] = {
			this match {
				case c: Covenant[A] @unchecked => c.succeed
				case rd: ReadyTask[A] => rd.succeed
			}
		}

		inline def asTask: Task[A] = this

		/**
		 * Transforms this [[LatchingTask]] by applying the given function to the result of this [[LatchingTask]].
		 * ===Detailed behavior===
		 * Creates a [[LatchingTask]] that yields the result of applying the provided function to the results of this [[LatchingTask]].
		 * @note CAUTION: Must be called within the $DoSerEx
		 * @note The override is necessary to specialize the return type, and implementation is necessary (can't leave the method abstract) because [[Covenant]] is invariant.
		 * @param f a function that transforms the result of this [[Task]].
		 *
		 * $isExecutedByDoSerEx
		 *
		 * $notGuarded
		 */
		override def map[B](f: A => B): LatchingTask[B] = {
			this match {
				case rd: ReadyTask[A] => rd.map(f)
				case c: Covenant[A @unchecked] => c.map(f)
			}
		}


		/**
		 * Transforms this [[LatchingTask]] by applying the provided function to the result of this [[LatchingTask]] and then subscribing-to the [[LatchingTask]] returned by said function.
		 * The returned [[LatchingTask]] will be already fulfilled if, and only if, this [[LatchingTask]] is already fulfilled.
		 * @note CAUTION: Must be called within the $DoSerEx
		 * @param f a function that is applied to the result of this [[Task]] execution to return a [[Task]] that is executed next to produce the result that the [[Task]] returned by this method yields.
		 *
		 * $isExecutedByDoSerEx
		 *
		 * $notGuarded
		 */
		def flatMap[B](f: A => LatchingTask[B]): LatchingTask[B]

		/** Creates a new [[LatchingTask]] that yields exactly the same result (same identity) as this [[LatchingTask]] but executes the provided side-effecting function before yielding it.
		 *
		 * $threadSafe
		 *
		 * @param sideEffect a function that is applied to the result of this [[LatchingTask]] for its side effects. */
		override def andThen(sideEffect: A => Unit): LatchingTask[A] = {
			subscribe(sideEffect)
			this
		}
	}

	//// Once factory methods ////

	/** Creates an already completed [[LatchingTask]].
	 * @param immediateResult the immediate result that this [[LatchingTask]] yields. */
	inline def LatchingTask_ready[A](immediateResult: A): ReadyTask[A] =
		new ReadyTask(immediateResult)

	/** An already completed [[LatchingTask]] that yields [[Unit]].
	 * CAUTION: This @threadUnsafe lazy val does not guarantee a unique instance under concurrent access. Its use is only safe for logic that depends on the value's data, not its object identity (eq/ne). */
	@threadUnsafe lazy final val LatchingTask_unit: ReadyTask[Unit] = ReadyTask(())

	/** An already completed [[LatchingTask]] that yields `true`.
	 * CAUTION: This @threadUnsafe lazy val does not guarantee a unique instance under concurrent access. Its use is only safe for logic that depends on the value's data, not its object identity (eq/ne). */
	@threadUnsafe lazy final val LatchingTask_true: ReadyTask[Boolean] = ReadyTask(true)

	/** An already completed [[LatchingTask]] that yields `false`.
	 * CAUTION: This @threadUnsafe lazy val does not guarantee a unique instance under concurrent access. Its use is only safe for logic that depends on the value's data, not its object identity (eq/ne). */
	@threadUnsafe lazy final val LatchingTask_false: ReadyTask[Boolean] = ReadyTask(false)

	/** Like [[Task_sequenceVenturesToArray]] but eager (instead of lazy). */
	inline def LatchingTask_sequenceVenturesToArray[A: ClassTag, C[x] <: Iterable[x]](ventures: C[Venture[A]], isWithinDoSerEx: Boolean = isInSequence): LatchingTask[Array[Try[A]]] =
		Covenant_triggerAndWire(Task_sequenceVenturesToArray(ventures), isWithinDoSerEx)

	//// READY TASK ////

	/** A [[LatchingTask]] that is fulfilled since its inception. */
	final class ReadyTask[+A](val value: A) extends LatchingTask[A] {

		override def subscribe(onComplete: A => Unit): Unit =
			onComplete(value)

		override def succeed: ReadyVenture[A] =
			ReadyVenture(Success(value))

		override val maybeResult: Maybe[A] =
			Maybe(value)

		override def unsubscribe(onComplete: A => Unit): Unit =
			()

		override def isSubscribed(onComplete: A => Unit): Boolean =
			false

		override def foreach(consumer: A => Unit): Unit = {
			checkWithin()
			consumer(value)
		}

		override def map[B](f: A => B): ReadyTask[B] = {
			checkWithin()
			ReadyTask(f(value))
		}

		override def flatMap[B](f: A => LatchingTask[B]): LatchingTask[B] = {
			checkWithin()
			f(value)
		}

		override def toFutureHardy(isWithinDoSerEx: Boolean = isInSequence): Future[A] =
			Future.successful(value)

		override def toString: String = deriveToString[ReadyTask[A]](this)
	}

	/** Creates a [[ReadyTask]] that yields the provided value.
	 * @note Also suppresses the generation of the synthetic companion object. */
	inline final def ReadyTask[A](a: A): ReadyTask[A] = new ReadyTask(a)

	//// COVENANT /////

	/** A [[LatchingTask]] with dynamic control of its completion (the execution of the subscribed consumers).
	 *
	 * It exposes methods such as [[fulfill]] and [[fulfillWith]] to allow external code to complete it.
	 *
	 * [[Covenant]] is to [[Task]] as [[Commitment]] is to [[Venture]], and as [[scala.concurrent.Promise]] is to [[scala.concurrent.Future]]
	 * */
	final class Covenant[A](initialResult: Maybe[A]) extends LatchingTask[A], SubscriptionHub[A] {
		private var oResult: Maybe[A] = initialResult

		def this() = this(Maybe.empty)

		override def subscribe(onComplete: A => Unit): Unit =
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

		override def map[B](f: A => B): LatchingTask[B] = {
			checkWithin()
			oResult.fold {
				val covenant = Covenant[B]()
				this.subscribe(a => covenant.fulfillUnsafe(f(a)))
				covenant
			} { a =>
				new ReadyTask[B](f(a))
			}
		}

		override def flatMap[B](f: A => LatchingTask[B]): LatchingTask[B] = {
			checkWithin()
			oResult.fold {
				val covenant = Covenant[B]()
				this.subscribe(a => f(a).subscribe(b => covenant.fulfillUnsafe(b)))
				covenant
			}(f)
		}

		/** The [[LatchingTask]] whose completion is controlled by this [[Covenant]].
		 *
		 * Provided to mimic containment semantics, allowing external code to treat this [[Covenant]] as if it exposed a separate [[LatchingTask]] field.
		 * @return this [[Covenant]] as a [[LatchingTask]]. */
		inline def asLatchingTask: LatchingTask[A] = this


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
		 * CAUTION: Deep synchronous chains of [[flatMap]] over immediately-fulfilled [[LatchingTask]] instances during ongoing fulfillment can form a synchronous recursion (fulfill → subscribe-immediate → fulfill → …) that overflows the stack. // TODO consider the trampoline solutions discussed with copilot in the session "causal anchoring dilema", near the end.
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

		/** Wires this [[Covenant]] to be completed with the result of a [[Task]].
		 *
		 * Arranges this [[Covenant]] to be fulfilled if `fulfillingTask` completes, unless it was fulfilled before.
		 * Always one, and only one, of the two callback is invoked:
		 *		- `onAlreadyCompleted` if this [[Covenant]] was already fulfilled when the subscription is done.
		 *		- `onCompletedLater` if this [[Covenant]] is fulfilled after the subscription is done.
		 * The subscription is synchronic if `isWithinDoSerEx` is true, and asynchronic ASAP otherwise.
		 *
		 * TODO rename to `completeWith`
		 * @param fulfillingTask the [[Task]] whose result will be used to complete this [[Covenant]].
		 * @param isWithinDoSerEx $isWithinDoSerEx
		 * @param onCompleted optional callback invoked when this [[Covenant]] is fulfilled. The first parameter is the fulfilling value and the second informs about its origin. Invoked within this [[Doer]] sequential executor.
		 * @throws IllegalArgumentException if `fulfillingTask` is the same instance as this [[Covenant]].
		 */
		def fulfillWith(fulfillingTask: Task[A], isWithinDoSerEx: Boolean = isInSequence, onCompleted: (A, ResultOrigin) => Unit = (_, _) => ()): this.type = {
			if fulfillingTask eq this then throw IllegalArgumentException("A Covenant can't be fulfilled with itself.")
			if isWithinDoSerEx then {
				oResult.fold {
					fulfillingTask.subscribe(result => fulfillUnsafe(result, onCompleted))
				} { result =>
					try onCompleted(result, ANOTHER_BEFORE)
					catch {
						case NonFatal(e) => reportPanicException(e)
					}
				}
			}
			else run(fulfillWith(fulfillingTask, true, onCompleted))
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

	/** Creates a [[Covenant]] that is wired to the [[LatchingTask]] resulting of executing the provided supplier within the $DoSerEx.
	 * @param supplier a supplier function that is executed within the $DoSerEx to return the [[LatchingTask]] to which the created [[Covenant]] is wired. */
	def Covenant_mineFlat[A](supplier: () => LatchingTask[A]): Covenant[A] = {
		val covenant = new Covenant[A]
		run {
			supplier().subscribe(a => covenant.fulfillUnsafe(a))
		}
		covenant
	}

	/** Triggers an execution of the given [[Task]] and returns a [[Covenant]] that will be completed with the result of the triggered execution if it completes before this [[Covenant]] is completed by other means.
	 *
	 * This method initiates an execution of the given [[Task]] and wires its result to a newly created [[Covenant]].
	 * The returned [[Covenant]] acts as a completion handle for the execution triggered by this method, and can be used to observe or react to its result.
	 *
	 * @param task the [[Task]] to be triggered.
	 * @param isWithinDoSerEx $isWithinDoSerEx
	 * @param onFulfilled The first parameter is the fulfilling value and the second informs about its origin. Invoked within this [[Doer]] sequential executor.
	 * @return a [[Covenant]] that will be completed with the result of the execution triggered by this method.
	 */
	inline def Covenant_triggerAndWire[A](task: Task[A], inline isWithinDoSerEx: Boolean = isInSequence, onFulfilled: (A, ImmediateResultOrigin) => Unit = (_: A, _: ImmediateResultOrigin) => ()): Covenant[A] = {
		val covenant = new Covenant[A]()
		task.trigger(isWithinDoSerEx)(result => covenant.fulfillUnsafe(result, onFulfilled))
		covenant
	}



	///////////// VENTURE //////////////

	/** A hardy and short-circuiting version of [[Task]].\
	 * Advantages of [[Venture]] compared to [[Task]]:
	 *		- results are wrapped withing a [[Try]] which allows the support of failed results.
	 *		- the call to the routines received by the operations are guarded with a try-catch, which allows to propagate failures through [[Venture]] chains.
	 *		- can encapsulate a [[Future]] making interoperability with them easier.
	 * @param A the type of the result obtained when executing this [[Venture]]. */
	type Venture[+A] = AbstractVenture[A]


	/** A hardy and short-circuiting version of [[Task]].\
	 * Design note:
	 * - The use of mixins to define the hardy side of the hierarchy was explored in Task3.scala and discarded due to extra allocation in many fundamental operations.
	 * - Defining [[Venture]] as `opaque type Venture[+A] = Task[Try[A]]` was explored but discarded due to bugs in the scala compiler. See https://github.com/scala/scala3/issues/25594. This will eliminate many redundant [[Venture]] implementation classes by reusing [[Task]]'s counterpart, but it may cause IDE issues since [[Venture]] operations would need to be defined as extension methods.
	 * @tparam A the type of the result obtained when executing this [[Venture]]. */
	abstract class AbstractVenture[+A] extends AbstractTask[Try[A]] { thisVenture =>

		/** Removes short-circuit semantics by reifying both the successful and failed outcomes as a [[scala.util.Try]] value within a strict [[Task]].\
		 * Together with [[Task.succeed]] this method allow to mix duties and ventures in the same chain. */
		def reconcile: Task[Try[A]] = thisVenture

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
		 * See [[recover]] and [[toTask]] if you want to transform the failures; and [[transform]] if you want to transform both, successful and failed ones.\
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
		 * It's worth mentioning that the side-effecting function is executed before triggering the next task in the chain.\
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

		/** Wraps this [[Venture]] into a [[Task]] applying the given function to transform failure results into successful ones. This is like [[map]] but for the throwable; and like [[recover]] but with a complete function.
		 * Together with [[Task.succeed]] this method allow to mix duties and ventures in the same chain. *
		 * @param exceptionHandler a complete function to apply to the result of this [[Venture]] if it is a [[Failure]].\
		 * $isExecutedByDoSerEx\
		 * $notGuarded\
		 * @return a [[Task]] that yields the result of this [[Venture]]. */
		inline final def reconcile[B >: A](exceptionHandler: Throwable => B): Task[B] =
			new Task_FromVenture[A, B](thisVenture, exceptionHandler)

		/** @return a [[Task]] that yields the result of this [[Venture]]. */
		inline final def asHardyTask: Task[Try[A]] =
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
		 * Design note: It was decided to make [[Venture]] (and [[Task]]) an inner class of the [[Doer]] to take advantage of type-path checking to detect when the contract "all operand functions passed to [[Venture]] (and [[Task]]) operations owned by the same [[Doer]] are executed in sequence" might be violated, at compile time.\
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

	/** Transforms a [[Task]] to a [[Venture]] */
	def Venture_fromTask[A](task: Task[Try[A]]): Venture[A] =
		(onComplete: Try[A] => Unit) => task.subscribe(onComplete)

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


	/** Creates a [[Task]] that, when executed, simultaneously triggers an execution for each [[Venture]] in the received [[Iterable]], and completes with an [[Iterable]] containing their results, successful or not, in the same order.\
	 * $threadSafe \
	 * TODO change return type to [[Task]] to better expose the fact that always yields a successful result
	 * @param ventures the [[Iterable]] of [[Venture]]s that the returned [[Task]] will trigger simultaneously to combine their results.
	 * @param factory the [[IterableFactory]] needed to build the [[Iterable]] that will contain the results. Note that most [[Iterable]] implementations' companion objects are an [[IterableFactory]].
	 * @tparam A the result type of all the provided [[Venture]]s.
	 * @tparam C the higher-kinded type of the [[Iterable]] of [[Venture]]s.
	 * @tparam To the higher-kinded type of the [[Iterable]] that will contain the results.
	 * @return the successful task described in the method description. */
	def Task_sequenceVentures[A: ClassTag, C[x] <: Iterable[x], To[x] <: Iterable[x]](factory: IterableFactory[To], ventures: C[Venture[A]]): Task[To[Try[A]]] = {
		Task_sequenceVenturesToArray(ventures).map { array =>
			val builder = factory.newBuilder[Try[A]]
			var index = 0
			while index < array.length do {
				builder.addOne(array(index))
				index += 1
			}
			builder.result()
		}
	}

	/** Like [[Task_sequenceVentures]] but the resulting collection's higher-kinded type `To` is fixed to [[Array]]. */
	inline def Task_sequenceVenturesToArray[A: ClassTag, C[x] <: Iterable[x]](ventures: C[Venture[A]]): Task[Array[Try[A]]] =
		new Task_SequenceHardy[A, C](ventures)

	//// Venture concrete implementations used internally ////

	/** $suppressSyntheticCompanionObject */
	private inline def Venture_Never(trap: Nothing): Any = trap

	/** A [[Venture]] that never completes.\
	 * $onCompleteExecutedByDoSerEx */
	final class Venture_Never extends AbstractVenture[Nothing] {
		override def subscribe(onComplete: Try[Nothing] => Unit): Unit = ()

		override def toString: String = deriveToString[Venture_Never](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Venture_fromTask(trap: Nothing): Any = trap

	final class Venture_fromTask[A](cA: Task[A]) extends AbstractVenture[A] {
		override def subscribe(onComplete: Try[A] => Unit): Unit = cA.subscribe(onComplete.compose(Success.apply))

		override def toString: String = deriveToString[Venture_fromTask[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Venture_Ready(trap: Nothing): Any = trap

	final class Venture_Ready[A](tryA: Try[A]) extends AbstractVenture[A] {
		override def subscribe(onComplete: Try[A] => Unit): Unit = onComplete(tryA)

		override def toString: String = deriveToString[Venture_Ready[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Venture_Own(trap: Nothing): Any = trap

	final class Venture_Own[+A](supplier: () => Try[A]) extends AbstractVenture[A] {
		override def subscribe(onComplete: Try[A] => Unit): Unit = {
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
		override def subscribe(onComplete: Try[A] => Unit): Unit = {
			val venturesA =
				try supplier()
				catch {
					case NonFatal(e) => Venture_failed(e)
				}
			venturesA.subscribe(onComplete)
		}

		override def toString: String = deriveToString[Venture_OwnFlat[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Venture_Wait(trap: Nothing): Any = trap

	final class Venture_Wait[+A](future: Future[A]) extends AbstractVenture[A] {
		override def subscribe(onComplete: Try[A] => Unit): Unit = {
			// Note that passing the `onComplete` operand directly to the `future.onComplete` method would break the error management contract: "exceptions thrown by the `onComplete` operand passed to `subscribe` should not be caught".
			future.onComplete { tryA =>
				thisDoer.run(onComplete(tryA))
			}(using ownSingleThreadExecutionContext)
		}

		override def toString: String = deriveToString[Venture_Wait[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Venture_Alien(trap: Nothing): Any = trap

	final class Venture_Alien[+A](builder: () => Future[A]) extends AbstractVenture[A] {
		override def subscribe(onComplete: Try[A] => Unit): Unit = {
			val future =
				try builder()
				catch {
					case NonFatal(e) => Future.failed(e)
				}
			// Note that passing the `onComplete` operand directly to the `future.onComplete` method would break the error management contract: "exceptions thrown by the `onComplete` operand passed to `subscribe` should not be caught".
			future.onComplete { tryA =>
				thisDoer.run(onComplete(tryA))
			}(using ownSingleThreadExecutionContext)
		}

		override def toString: String = deriveToString[Venture_Alien[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Venture_Foreign(trap: Nothing): Any = trap

	final class Venture_Foreign[+A](foreignDoer: Doer, foreignVenture: foreignDoer.Venture[A]) extends AbstractVenture[A] {
		override def subscribe(onComplete: Try[A] => Unit): Unit =
			foreignVenture.trigger(false) { tryA => run(onComplete(tryA)) }

		override def toString: String = deriveToString[Venture_Foreign[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Venture_Consume(trap: Nothing): Any = trap

	final class Venture_Consume[A](ventureA: Venture[A], consumer: Try[A] => Unit) extends AbstractVenture[Unit] {
		override def subscribe(onComplete: Try[Unit] => Unit): Unit = {
			ventureA.subscribe { tryA =>
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
		override def subscribe(onComplete: Try[A] => Unit): Unit = {
			ventureA.subscribe {
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
		override def subscribe(onComplete: Try[B] => Unit): Unit =
			originalVenture.subscribe { tryA => onComplete(tryA.reifyBack(f)) }

		override def toString: String = deriveToString[Venture_Transform[A, B]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Venture_Map(trap: Nothing): Any = trap

	final class Venture_Map[+A, +B](originalVenture: Venture[A], f: A => B) extends AbstractVenture[B] {
		override def subscribe(onComplete: Try[B] => Unit): Unit =
			originalVenture.subscribe { tryA => onComplete(tryA.mapFast(f)) }

		override def toString: String = deriveToString[Venture_Map[A, B]](this)
	}


	/** $suppressSyntheticCompanionObject */
	private inline def Venture_FlatMap(trap: Nothing): Any = trap

	final class Venture_FlatMap[+A, +B](ventureA: Venture[A], f: A => Venture[B]) extends AbstractVenture[B] {
		override def subscribe(onComplete: Try[B] => Unit): Unit = {
			ventureA.subscribe {
				case Success(a) =>
					val maybeVentureB = try Maybe(f(a)) catch {
						case NonFatal(e) =>
							onComplete(Failure(e))
							Maybe.empty
					}
					maybeVentureB.foreach(_.subscribe(onComplete))
				case failure: Failure[A] =>
					onComplete(failure.castTo[B])
			}
		}

		override def toString: String = deriveToString[Venture_FlatMap[A, B]](this)
	}


	/** $suppressSyntheticCompanionObject */
	private inline def Venture_TransformWith(trap: Nothing): Any = trap

	final class Venture_TransformWith[+A, +B](ventureA: Venture[A], f: Try[A] => Venture[B]) extends AbstractVenture[B] {
		override def subscribe(onComplete: Try[B] => Unit): Unit = {
			ventureA.subscribe(tryA =>
				tryA.reify(e =>
					onComplete(Failure(e))
				)(tryA =>
					f(tryA).subscribe(onComplete)
				)
			)
		}

		override def toString: String = deriveToString[Venture_TransformWith[A, B]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Venture_AndThen(trap: Nothing): Any = trap

	final class Venture_AndThen[+A](ventureA: Venture[A], consumer: Try[A] => Unit) extends AbstractVenture[A] {
		override def subscribe(onComplete: Try[A] => Unit): Unit = {
			ventureA.subscribe { tryA =>
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
		override def subscribe(onComplete: Try[C] => Unit): Unit = {
			var ota: Maybe[Try[A]] = Maybe.empty
			var otb: Maybe[Try[B]] = Maybe.empty
			ventureA.subscribe { tryA =>
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
			ventureB.subscribe { tryB =>
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
		override def subscribe(onComplete: Try[Array[A]] => Unit): Unit = {
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
					venture.subscribe {
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
	private inline def Task_SequenceHardy(trap: Nothing): Any = trap

	final class Task_SequenceHardy[A: ClassTag, C[x] <: Iterable[x]](ventures: C[Venture[A]]) extends AbstractTask[Array[Try[A]]] {
		override def subscribe(onComplete: Array[Try[A]] => Unit): Unit = {
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
					venture.subscribe { tryA =>
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
	sealed abstract class LatchingVenture[+A] extends AbstractVenture[A], Latching[Try[A]] { thisLatchingVenture =>

		inline def asVenture: Venture[A] = this

		override def reconcile: LatchingTask[Try[A]] = {
			maybeResult.fold {
				val covenant = new Covenant[Try[A]]
				subscribe(tryA => covenant.fulfillUnsafe(tryA))
				covenant
			} { tryA => ReadyTask(tryA) }
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

		/** Transforms this [[LatchingTask]] by applying the given function to the result if it is successful. Analogous to [[Future.map]].\
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

		override def subscribe(onComplete: Try[A] => Unit): Unit =
			onComplete(value)

		override def maybeResult: Maybe[Try[A]] =
			Maybe(value)

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

		override def subscribe(onComplete: Try[A] => Unit): Unit =
			oResult.fold(attach(onComplete))(onComplete)

		override def maybeResult: Maybe[Try[A]] = {
			checkWithin()
			oResult
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
				thisCommitment.subscribe { tryA =>
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
				thisCommitment.subscribe { tryA => commitment.completeUnsafe(tryA.reifyBack(f)) }
				commitment
			} { tryA =>
				ReadyVenture[B](tryA.reifyBack(f))
			}
		}

		override def transformWith[B](f: Try[A] => LatchingVenture[B]): LatchingVenture[B] = {
			checkWithin()
			thisCommitment.maybeResult.fold {
				val commitment = new Commitment[B]
				thisCommitment.subscribe(tryA =>
					tryA.reify(e =>
						commitment.completeUnsafe(Failure(e))
					)(tryA =>
						f(tryA).subscribe(tryB => commitment.completeUnsafe(tryB))
					)
				)
				commitment
			}(_.reify(e => ReadyVenture(Failure(e)))(f))
		}

		override def map[B](f: A => B): LatchingVenture[B] = {
			checkWithin()
			thisCommitment.maybeResult.fold {
				val commitment = new Commitment[B]
				thisCommitment.subscribe { tryA => commitment.completeUnsafe(tryA.mapFast(f)) }
				commitment
			} { tryA =>
				ReadyVenture(tryA.mapFast(f))
			}
		}

		override def flatMap[B](f: A => LatchingVenture[B]): LatchingVenture[B] = {
			checkWithin()
			thisCommitment.maybeResult.fold {
				val commitment = new Commitment[B]
				thisCommitment.subscribe {
					case success: Success[A] =>
						val maybeVentureB = try Maybe(f(success.value)) catch {
							case NonFatal(e) =>
								commitment.completeUnsafe(Failure(e))
								Maybe.empty
						}
						maybeVentureB.foreach(_.subscribe(tryB => commitment.completeUnsafe(tryB)))
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
					completingVenture.subscribe(result => completeUnsafe(result, onCompleted))
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
		(a: A) => Task_ready(f(a))

	def Flow_wrap[A, B](builder: A => Task[B]): Flow[A, B] =
		(a: A) => builder(a)

	trait Flow[A, B] { thisFlow =>

		protected def flush(a: A): Task[B]

		inline def apply(a: A, inline isWithinDoSerEx: Boolean = isInSequence)(onComplete: B => Unit): Unit = {
			def work(): Unit = flush(a).subscribe(onComplete)

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

	/** Facade of a [[Task]] that, once completed, memorizes its result forever. All subscribers receive the same memorized result, even after completion.\
	 * @tparam A The type of the value. */
	trait Latching[+A] extends Observable[A] {

		/** @return the result if completed.
		 * @note CAUTION: Must be called within the $DoSerEx */
		def maybeResult: Maybe[A]

		/** @return true if this [[LatchingTask]] was fulfilled; or false if it is still pending.
		 * @note CAUTION: Must be called within the $DoSerEx */
		inline def isCompleted: Boolean = maybeResult.isDefined

		/** @return true if this [[LatchingTask]] is still pending; or false if it was completed.
		 * @note CAUTION: Must be called within the $DoSerEx */
		inline def isPending: Boolean = maybeResult.isEmpty

		/** Subscribes a consumer of the result of this producer.\
		 * The subscription is automatically removed after an execution of this producer has completed and the received consumer is executed.\
		 * If this producer is already fulfilled when this method is called, the provided consumer is invoked synchronously and no subscription occurs.\
		 * Otherwise, the provided consumer is schedule to run upon completion in subscription orden (after sequentially running all the previously subscribed result consumers).\
		 * @note CAUTION: This method does not prevent duplicate subscriptions.
		 * @note CAUTION: Must be called within the $DoSerEx */
		override def subscribe(consumer: A => Unit): Unit

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