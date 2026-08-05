package readren.sequencer

import Doer.*

import readren.common.*

import scala.annotation.unchecked.uncheckedVariance
import scala.annotation.{targetName, threadUnsafe}
import scala.collection.IterableFactory
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.reflect.ClassTag
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object Doer {

	type ExecutionSerial = Int

	val assertionsEnabled: Boolean = classOf[Doer].desiredAssertionStatus()


	/** Information about the responsible for the completion and origin of the value with which a [[Captor]] is completed:
	 *		- [[THE_PROVIDED]] if completed by the invoked completion method with the provided value.
	 *		- [[ANOTHER_BEFORE]] if completed by other means before the completion method was invoked.
	 *		- [[ANOTHER_AFTER]] if completed by other menas after the completion method was invoked.
	 * TODO when a new version of scala is released (newer than 3.7.4), check if it supports making these types aliases opaque without causing obscure errors in unrelated code like the [[DoerLoopingPart]] despite it does not reference them. */
	type ResultOrigin = OriginId
	/** Completed by something else after the [[Doer.Captor.seizeWith]] was invoked. */
	inline val ANOTHER_AFTER = 0
	/** Information about the responsible for the completion and origin of the value with which a [[Captor]] is completed with an immediate value.
	 *		- [[THE_PROVIDED]] if completed by the invoked completion method with the provided value.
	 *		- [[ANOTHER_BEFORE]] if completed by other means before the completion method was invoked.
	 * TODO when a new version of scala is released (newer than 3.7.4), check if it supports making these types aliases opaque without causing obscure errors in unrelated code like the [[DoerLoopingPart]] despite it does not reference them. */
	type ImmediateResultOrigin = ResultOrigin
	/** Completed by something else before the [[Doer.Captor]] completion method was invoked. */
	final inline val ANOTHER_BEFORE = 1
	/** Completed by the [[Doer.Captor]] completion method to which the `onCompleted` call-back that received this constant was provided. */
	final inline val THE_PROVIDED = 2

	final def checkWithinMsg(thisDoer: Doer): String = s"The current thread does not correspond to this Doer: expected=${thisDoer.tag}, current=${thisDoer.currentlyRunningDoer.fold("unknown")(_.tag)}."

	//// Convenient constants ////

	val successUnit: Success[Unit] = Success(())
	val successTrue: Success[true] = Success(true)
	val successFalse: Success[false] = Success(false)
}

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
 * - Primitives across different [[Doer]] instances are independent and may run concurrently.
 * ==Note:==
 * See [[Doer.executeSequentially()]].
 *
 * @define DoSerEx DoSerEx (doer's serial executor)
 * @define onCompleteExecutedByDoSerEx The `onComplete` callback passed to `subscribe` is always, with no exception, executed by this $DoSerEx. This is part of the contract of the [[Mono]] and [[Flux]] hierachies.
 * @define threadSafe This method is thread-safe.
 * @define isExecutedByDoSerEx This function is executed within the DoSerEx (doer's serial executor).
 * @define notGuarded CAUTION! The call to this function is NOT guarded with a try-catch. If its evaluation terminates abruptly the task will never complete. The same occurs with all routines received by not guarded [[Task]] operations.
 * @define maxRecursionDepthPerExecutor Maximum recursion depth per executor. Once this limit is reached, the recursion continues in a new executor. The result does not depend on this parameter as long as no [[java.lang.StackOverflowError]] occurs.
 * @define isWithinDoSerEx indicates whether the call to this method is within this [[Doer]]'s sequential executor. If there is no such certainty the call site should either, not specify a value in order to use the default (which is the result of [[Doer.isInSequence]]), or specify `false` to force deferred execution.
 * @define suppressSyntheticCompanionObject Suppresses the generation of the synthetic companion object. This dummy definition creates a name collision to prevent the compiler from generating a module for universal apply, thereby avoiding the bytecode overhead of a lazy-initialized nested module. By requiring a [[Nothing]] parameter, this method is made uncallable, ensuring any inadvertent use is caught at compile-time.
 */
trait Doer extends DoerCorePart, DoerTaskOpsPart, DoerFluxPart, DoerLoopingPart { thisDoer =>
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
	 * All the deferred actions preformed by the [[Mono]] operations are executed by calling this method unless the particular operation documentation says otherwise. That includes not only the call-back functions like `onComplete` but also all the functions, procedures, predicates, and by-name parameters they receive.
	 * @note Implementations must set their associated provider's thread-local to `this` before invoking `body`, and clear it (restore to `null`) once `body` returns or throws. Failure to uphold this contract will cause [[DoerProvider.currentDoer]] to return a value of the wrong type at runtime, as the cast in that method relies on it. */
	def executeSequentially(runnable: Runnable): Unit

	/** Like [[executeSequentially]] but with asynchronous stack traces. */
	def executeSequentiallyWithAST(runnable: Runnable): Unit = {
		val dispatchTrace = new Throwable("Asynchronously dispatched here")

		val wrappedRunnable = new Runnable {
			def run(): Unit = {
				try {
					runnable.run()
				} catch {
					case t: Throwable =>
						t.addSuppressed(dispatchTrace)
						throw t
				}
			}
		}

		executeSequentially(wrappedRunnable) // Enqueue the wrapped runnable instead
	}

	/** The [[ExecutionSerial]] of the most recently executed [[Runnable]] on this [[Doer]].
	 *
	 * This serial is incremented immediately before each [[Runnable]] passed to [[executeSequentially]] is run.
	 * It enables those [[Runnable]]s to observe their relative execution order and distinguish whether two operations occur during the same or different executions, allowing the user to build defensive measures againts stack-overflow due to recursive calls to [[Mono.subscribeSync]]. */
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
		if Doer.assertionsEnabled && !isInSequence then throw new AssertionError(checkWithinMsg(thisDoer))
	}


	/**
	 * Queues an execution of the specified procedure in the tasks-queue of this $DoSerEx. See [[Doer.executeSequentially]]
	 * If the call is executed by the $DoSerEx the [[Runnable]]'s execution will not start until the DoSerEx completes its current execution and gets free to start a new one.
	 *
	 * All the deferred actions preformed by the [[Mono]] operations are executed by calling this method unless the particular operation documentation says otherwise. That includes not only the call-back functions like `onComplete` but also all the functions, procedures, predicates, and by-name parameters they receive as.
	 * This function only makes sense to call:
	 *		- from an action that is not executed by this $DoSerEx (the callback of a [[Future]], for example);
	 *		- or to avoid a stack overflow by continuing the recursion in a new execution.
	 * @see [[submit]] and [[submitHardy]] if the result of the execution is relevant.
	 * @note Is more efficient than the functionally equivalent: `Task_mine(() => procedure).subscribeAndForget()`.
	 */
	inline def run(inline procedure: => Unit): Unit = {
		${ DoerMacros.runImpl('thisDoer, 'procedure) }
	}

	//// WIRABLE SOFT ////

	/** Abstracts over how a supplier is wired into an effect type `F` within the context of this [[Doer]].
	 *
	 * Instances determine the strategy by which a supplier `() => A` is captured or scheduled into `F[A]`. This may happen eagerly (e.g. queuing execution via [[Doer.run]] immediately) or lazily (e.g. deferring until the effect is engaged).
	 *
	 * @tparam A the type of value produced by the supplier
	 * @tparam F the effect type into which the supplier is wired
	 */
	trait Wirable[A, F[_]] {
		/** Wires the given supplier into an instance of `F[A]`.\
		 * The wiring strategy is defined by the implementing instance: it may schedule execution immediately via [[Doer.run]], defer it until the effect is engaged, or apply any other strategy consistent with this [[Doer]]'s execution model.
		 * @param supplier the computation to wire into `F`
		 * @return an `M[A]` that captures or schedules the supplier according to this instance's strategy */
		inline def wire(supplier: () => A): F[A]

		inline def wireFlat(supplier: () => F[A]): F[A]

		inline def wireGuarded(supplier: () => A): F[A]

		inline def wireFlatGuarded(supplier: () => F[A]): F[A]
	}

	/** Wires the given supplier into effect type `F` using the [[Wirable]] instance corresponding to `F`. The [[Wirable]] instances for effect types defined in [[Doer]] are provided by [[Doer]] itself.
	 * @param supplier the computation to submit
	 * @param wirable the type-class instance that determines how the supplier is wired
	 * @tparam A the type of value produced
	 * @tparam F the effect type into which the supplier is wired
	 * @return an `F[A]` wired according to the [[Wirable]] instance in scope */
	inline def submit[A, F[_]](supplier: () => A)(using wirable: Wirable[A, F]): F[A] =
		wirable.wire(supplier)

	inline def submitGuarded[A, F[_]](supplier: () => A)(using wirable: Wirable[A, F]): F[A] =
		wirable.wireGuarded(supplier)

	inline def submitFlat[A, F[_]](supplier: () => F[A])(using wirable: Wirable[A, F]): F[A] =
		wirable.wireFlat(supplier)

	inline def submitFlatGuarded[A, F[_]](supplier: () => F[A])(using wirable: Wirable[A, F]): F[A] =
		wirable.wireFlatGuarded(supplier)


	inline given [A] =>Wirable[A, Task] {
		override inline def wire(supplier: () => A): Task[A] = new Task_Apply(supplier)

		override inline def wireFlat(supplier: () => Task[A]): Task[A] = new Task_Defers(supplier)

		override inline def wireGuarded(supplier: () => A): Task[A] = new Task_ApplyGuarded(supplier)

		override inline def wireFlatGuarded(supplier: () => Task[A]): Task[A] = new Task_DefersGuarded(supplier)
	}

	inline given [A] =>Wirable[A, Capturer] {
		override inline def wire(supplier: () => A): Capturer[A] = Captor_apply(supplier, false)

		override inline def wireFlat(supplier: () => Capturer[A]): Capturer[A] = Captor_defer(supplier, false)

		override inline def wireGuarded(supplier: () => A): Capturer[A] = Captor_apply(supplier, true)

		override inline def wireFlatGuarded(supplier: () => Capturer[A]): Capturer[A] = Captor_defer(supplier, true)
	}

	//// EXCEPTION HANDLING ////

	/** An [[ExecutionContext]] that executes within this [[Doer]]'s serial executor.\
	 * Useful to execute the functional operands of [[Future]] operations serially with this [[Doer]] primitives' operations.\
	 * Internally, it is used by operations that handle a [[Future]]. */
	@threadUnsafe lazy val ownSerialExecutionContext: ExecutionContext = new ExecutionContext {
		def execute(runnable: Runnable): Unit = thisDoer.executeSequentially(runnable)

		override def reportFailure(cause: Throwable): Unit = throw cause
	}
}

abstract class AbstractDoer extends Doer

/** A [[Doer]] extended with [[SchedulingExtension]] capabilities. */
type SchedulingDoer = Doer & SchedulingExtension
