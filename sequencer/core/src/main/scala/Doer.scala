package readren.sequencer

import Doer.*

import readren.common.*

import scala.annotation.unchecked.uncheckedVariance
import scala.annotation.{tailrec, targetName, threadUnsafe}
import scala.collection.IterableFactory
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.reflect.ClassTag
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object Doer {

	type ExecutionSerial = Int

	val assertionsEnabled: Boolean = classOf[Doer].desiredAssertionStatus()



	/** Information about the responsible for the completion and origin of the value with which a [[Covenant]]/[[Commitment]] is completed:
	 *		- [[THE_PROVIDED]] if completed by the invoked completion method with the provided value.
	 *		- [[ANOTHER_BEFORE]] if completed by other means before the completion method was invoked.
	 *		- [[ANOTHER_AFTER]] if completed by other menas after the completion method was invoked.
	 * TODO when a new version of scala is released (newer than 3.7.4), check if it supports making these types aliases opaque without causing obscure errors in unrelated code like the [[LoopingExtension]] despite it does not reference them. */
	type ResultOrigin = OriginId
	/** Completed by something else after the [[Doer.Covenant.fulfillWith]] was invoked. */
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

	final def checkWithinMsg(thisDoer: Doer): String = s"The current thread does not correspond to this Doer: expected=${thisDoer.tag}, current=${thisDoer.currentlyRunningDoer.fold("unknown")(_.tag)}."

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
 * @define notGuarded CAUTION! The call to this function is NOT guarded with a try-catch. If its evaluation terminates abruptly the task will never complete. The same occurs with all routines received by [[Task]] operations. This is one of the main differences with [[Venture]] operation.
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
	 * It enables those [[Runnable]]s to observe their relative execution order and distinguish whether two operations occur during the same or different executions, allowing the user to build defensive measures againts stack-overflow due to recursive calls to [[Observable.subscribeSync]]. */
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
	 * All the deferred actions preformed by the [[Task]]/[[Venture]] operations are executed by calling this method unless the particular operation documentation says otherwise. That includes not only the call-back functions like `onComplete` but also all the functions, procedures, predicates, and by-name parameters they receive as.
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

		override inline def wireGuarded(supplier: () => A): Task[A] = ??? // TODO

		override inline def wireFlatGuarded(suplier: () => Task[A]): Task[A] = ??? // TODO
	}

	inline given [A] =>Wirable[A, LatchingTask] {
		override inline def wire(supplier: () => A): LatchingTask[A] = Covenant_from(supplier)

		override inline def wireFlat(supplier: () => LatchingTask[A]): LatchingTask[A] = Covenant_defer(supplier)

		override inline def wireGuarded(supplier: () => A): LatchingTask[A] = ??? // TODO

		override inline def wireFlatGuarded(suplier: () => LatchingTask[A]): LatchingTask[A] = ??? // TODO
	}

	//// EXCEPTION HANDLING ////

	/** An [[ExecutionContext]] that executes in sequence with this [[Doer]].\
	 * Useful to execute [[Future]] operations within this [[Doer]]'s DoSerEx.\
	 * Internally, it is used by operations that handle a [[Future]]. */
	@threadUnsafe lazy val ownSingleThreadExecutionContext: ExecutionContext = new ExecutionContext {
		def execute(runnable: Runnable): Unit = thisDoer.executeSequentially(runnable)

		override def reportFailure(cause: Throwable): Unit = throw cause
	}

	//// PRIMITIVES ////

	/** Modernized subscription handle returned upon subscribing to an asynchronous primitive.
	 * Added as part of the bi-convergent convergence plan to support safe cancellation.
	 * @note CAUTION: Must be called within the single-thread Execution Context of the owning Doer (DoSerEx). */
	trait Subscription {
		def unsubscribe(): Unit
	}

	/** An empty subscription that performs no action upon unsubscription. */
	@threadUnsafe lazy val Subscription_empty: Subscription = () => ()

	/** Observer of single result computations. */
	trait MonoObserver[-A] {
		/** The implementation should never throw a non-fatal exception. It may either terminate normally or fatally though. */
		def onSuccess(a: A): Unit

		/** The implementation should never throw a non-fatal exception. It may either terminate normally or fatally though. */
		def onError(e: Throwable): Unit
	}

	@threadUnsafe lazy val MonoObserver_ignore: MonoObserver[Any] = new MonoObserver[Any] {
		override def onSuccess(value: Any): Unit = ()

		override def onError(ex: Throwable): Unit = ()
	}

	inline def MonoObserver_fromCallbacks[A](inline success: A => Unit, inline error: Throwable => Unit): MonoObserver[A] = {
		class Local extends MonoObserver[A] {
			override def onSuccess(a: A): Unit = success(a)

			override def onError(e: Throwable): Unit = error(e)
		}
		new Local
	}

	/** A single result computation with observable result.
	 * TODO rename to "Mono" */
	trait Observable[+A] { thisMono =>
		/** Subscribes an [[Observer]] to the result of this [[Observable]] and returns a [[Subscription]] that can be used to cancel.
		 * This method is the sole primitive operation of this trait; all other methods are derived from it.\
		 * @param downChainMono The observer to be notified upon the completion of this [[Observable]]. The implementation should notify within the $DoSerEx.\
		 * The implementation may assume that `onComplete` will either terminate normally or fatally, but will not throw non-fatal exceptions. */
		def subscribeSync(downChainMono: MonoObserver[A]): Subscription

		inline final def subscribeSyncCallbacks(inline success: A => Unit, inline error: Throwable => Unit): Subscription = subscribeSync(MonoObserver_fromCallbacks(success, error))

		inline final def subscribe(inline isWithinDoSerEx: Boolean = isInSequence)(downChainObserver: MonoObserver[A]): Subscription = {
			if isWithinDoSerEx then {
				checkWithin()
				subscribeSync(downChainObserver)
			} else {
				class Junction extends Subscription with Runnable {
					private var isActive = true
					private var maybeUpChainSubscription: Maybe[Subscription] = Maybe.empty

					{
						executeSequentially(this)
					}

					override def run(): Unit = {
						if isActive then {
							val upChainSubscription = subscribeSync(downChainObserver)
							if isActive then maybeUpChainSubscription = Maybe(upChainSubscription)
							else upChainSubscription.unsubscribe()
						}
					}

					override def unsubscribe(): Unit = {
						checkWithin()
						isActive = false
						maybeUpChainSubscription.foreach(_.unsubscribe())
					}
				}
				new Junction
			}
		}

		inline def subscribe(inline isWithinDoSerEx: Boolean)(inline onComplete: A | Throwable => Unit): Subscription = {
			class AdaptedJunction extends Subscription with MonoObserver[A] with Runnable {
				private var isActive = true
				private var maybeUpChainSubscription: Maybe[Subscription] = Maybe.empty

				override def run(): Unit = {
					if isActive then {
						val upChainSubscription = subscribeSync(this)
						if isActive then maybeUpChainSubscription = Maybe(upChainSubscription)
						else upChainSubscription.unsubscribe()
					}
				}

				override def onSuccess(a: A): Unit = onComplete(a)

				override def onError(e: Throwable): Unit = onComplete(e)

				override def unsubscribe(): Unit = {
					checkWithin()
					isActive = false
					maybeUpChainSubscription.foreach(_.unsubscribe())
				}
			}
			val junction = new AdaptedJunction
			if isWithinDoSerEx then junction.run() else executeSequentially(junction)
			junction
		}

		inline final def subscribeCallbacks(inline isWithinDoSerEx: Boolean = isInSequence)(inline success: A => Unit, inline error: Throwable => Unit): Subscription = subscribe(isWithinDoSerEx)(MonoObserver_fromCallbacks(success, error))

		inline final def subscribeHardy(inline isWithinDoSerEx: Boolean = isInSequence)(inline onComplete: Try[A] => Unit): Subscription = subscribeCallbacks(isWithinDoSerEx)(a => onComplete(Success(a)), e => onComplete(Failure(e)))

		inline final def subscribeAndForget(inline isWithingDoSerEx: Boolean = isInSequence): Subscription = subscribe(isWithingDoSerEx)(MonoObserver_ignore)

		/** Like [[subscribeSync]] but does not return a [[Subscription]]. /
		 * The default implementation calls [[subscribeSync]], but some subclasses have a more efficient implementation.
		 * TODO override this method in as many primitives as possible.
		 * @param downChainObserver The observer to be notified upon the completion of this [[Observable]]. The implementation should notify within the $DoSerEx.\
		 * The implementation may assume that the [[MonoObserver]] methods either terminate normally or fatally, but will not throw non-fatal exceptions. */
		def triggerSync(downChainObserver: MonoObserver[A]): Unit = subscribeSync(downChainObserver)

		inline def triggerSyncCallbacks(inline success: A => Unit, inline error: Throwable => Unit): Unit = triggerSync(MonoObserver_fromCallbacks(success, error))

		/** Enqueues an uncancelable execution of this [[Observable]] ignoring the result .
		 *
		 * $threadSafe
		 *
		 * @param isWithinDoSerEx $isWithinDoSerEx */
		inline final def trigger(inline isWithinDoSerEx: Boolean = isInSequence)(downChainObserver: MonoObserver[A]): Unit = {
			if isWithinDoSerEx then {
				checkWithin()
				triggerSync(downChainObserver)
			} else thisDoer.run(triggerSync(downChainObserver))
		}

		@targetName("triggerCallback")
		inline final def triggerCallbacks(inline isWithinDoSerEx: Boolean = isInSequence)(inline success: A => Unit, inline error: Throwable => Unit): Unit = trigger(isWithinDoSerEx)(MonoObserver_fromCallbacks(success, error))

		inline final def triggerHardy(inline isWithinDoSerEx: Boolean = isInSequence)(inline onComplete: Try[A] => Unit): Unit = triggerCallbacks(isWithinDoSerEx)(a => onComplete(Success(a)), e => onComplete(Failure(e)))

		/** Enqueues an execution of this [[Observable]] ignoring the result.
		 *
		 * $threadSafe
		 *
		 * @param isWithinDoSerEx $isWithinDoSerEx */
		inline final def triggerAndForget(inline isWithinDoSerEx: Boolean = isInSequence): Unit = trigger(isWithinDoSerEx)(MonoObserver_ignore)

		/** Enqueues an execution of this [[Task]] and then invokes the provided consumer passing the result.
		 *
		 * Is equivalent to {{{subscribe(isInSequence)(consumer)}}}
		 *
		 * $threadSafe
		 * @param consumer called with this [[Observable]] result when it completes.
		 *
		 * $isExecutedByDoSerEx
		 *
		 * $notGuarded */
		def foreach(consumer: A => Unit): Unit

		/** Creates a new [[Observable]] that yields exactly the same result (same identity) as this [[Observable]] but executes the provided side-effecting function before yielding it.\
		 * $threadSafe
		 * @param onSuccess a function that is applied to the successful result of this [[Observable]] for its side effects before subscribers.
		 * @param onError a function that is applied to the failure result of this [[Observable]] for its side effects before subscribers. */
		def andThen(onSuccess: A => Unit, onError: Throwable => Unit = _ => ()): Observable[A]

		def reconcile: Observable[Try[A]]

		def withFilter(p: A => Boolean): Observable[A]

		def withFilterGuarded(p: A => Boolean): Observable[A]

		/**
		 * Creates a new [[Observable]] that yields the result of applying the provided function to the result of this [[Observable]].
		 *
		 * $threadSafe
		 *
		 * @param f a function that is applied to the result of this [[Observable]] to produce the final result.
		 *
		 * $isExecutedByDoSerEx
		 *
		 * $notGuarded
		 */
		def map[B](f: A => B): Observable[B]

		def mapGuarded[B](f: A => B): Observable[B]

		/**
		 * Creates a new [[Observable]] that yields the result of executing an intermediate [[Observable]] produced by applying the provided function to the final result.
		 *
		 * $threadSafe
		 *
		 * @param f a function that is applied to the result of this [[Observable]] to produce an intermediate [[Observable]] that is then executed to produce the final result.
		 *
		 * $isExecutedByDoSerEx
		 *
		 * $notGuarded
		 */
		def flatMap[B](f: A => Observable[B]): Observable[B]

		def flatMapGuarded[B](f: A => Observable[B]): Observable[B]

		def transform[B](f: Try[A] => Try[B]): Observable[B]

		def transformWith[B](f: Try[A] => Observable[B]): Observable[B]

		def recover[B >: A](pf: Throwable => Maybe[B]): Observable[B]

		def recoverWith[B >: A](pf: Throwable => Maybe[Observable[B]]): Observable[B]

		def toFuture(isWithinDoSerEx: Boolean = isInSequence): Future[A]

		/**
		 * Wraps this [[Observable]] into another that belongs to another [[Doer]].\
		 * Useful to chain [[Observable]]'s operations that involve different [[Doer]] instances.\
		 * ===Detailed behavior===
		 * Returns a [[Observable]] that belongs to the provided [[Doer]]. When it is triggered, it will trigger this task within this [[Doer]] and, when completed, make the returned [[Observable]] to yield the result.\
		 * CAUTION: Avoid closing over the same mutable variable from two operand functions applied to [[Observable]] instances belonging to different [[Doer]]s.\
		 * Remember that all function operands provided to [[Observable]] methods are executed within the [[Doer]] that owns it. Therefore, calling [[triggerCallbacks]] on the returned [[Observable]] will execute the `onComplete` passed to it within the `otherDoer`.\
		 *
		 * $threadSafe
		 *
		 * @param otherDoer the [[Doer]] to which the returned [[Task]] will belong.
		 */
		def onBehalfOf(otherDoer: Doer): otherDoer.Observable[A]

		/** Casts the singleton type of the [[Doer]] instance that owns this [[Task]] to the singleton-type of the provided [[Doer]].
		 * This operation does nothing at runtime. It only tricks the compiler to prevent it from complaining when operating with references to the same [[Doer]] instance but through different type-paths.
		 * CAUTION: Use it only if you are sure that the provided [[Doer]] instance is the one that owns this [[Task]].
		 *
		 * Design note: It was decided to make [[Task]] (and [[Venture]]) an inner class of the [[Doer]] to take advantage of type-path checking to detect when the contract "all operand functions passed to [[Task]] (and [[Venture]]) operations owned by the same [[Doer]] are executed in sequence" might be violated, at compile time.
		 * Using type-path checking to detect contract violations is very valuable, but it comes at a cost, because the type-path check done by the compiler is stricter than necessary -- it checks that the singleton type of the references involved be compatible, and we only need to check that the involved [[Task]] instances belong to the same [[Doer]] instance.
		 * Therefore, the compiler will report type errors in situations the contract is not violated, which is not what we want.
		 * This operation ([[castTypePath()]]) is intended to handle those cases.
		 */
		def castTypePath[E <: Doer](doer: E): doer.Observable[A] = {
			assert(thisDoer eq doer)
			this.asInstanceOf[doer.Observable[A]]
		}

		def guarded: GuardedMono[A]
	}

	trait GuardedMono[+A] {
		def withFilter(p: A => Boolean): Observable[A]

		def map[B](f: A => B): Observable[B]

		def flatMap[B](f: A => Observable[B]): Observable[B]
		/*
		def recover[B >: A](pf: Throwable => Maybe[B]): Observable[B]
		def recoverWith[B >: A](pf: Throwable => Maybe[Observable[B]]): Observable[B]
		def transform[B](f: Try[A] => Try[B]): Observable[B]
		def transformWith[B](f: Try[A] => Observable[B]): Observable[B]
		*/
	}

	/////////////// TASK ///////////////

	/** A lazy computation owned by this [[Doer]]. Executions are serialized across all [[Task]] instances of the same [[Doer]]. Each execution may produce a different result if the computation depends on mutable state.\
	 * Executions are performed in the order they were triggered.\
	 * A [[Task]] can encapsulate one or more chained actions and provides operations to declaratively build complex duties from simpler ones.\
	 * This tool simplifies the implementation of a handler that manages multiple simultaneous processes that interact with each other using a single sequential actor. How? By eliminating the need for state variables that determine the decision-making flow, as the code structure itself indicates the execution order.\
	 * Instances of [[Task]] whose result is always the same follow the monadic laws. However, if the result depends on the execution (because it depends on mutable variables or time), these laws may be broken.\
	 * For example, if the [[Task.subscribeSyncCallbacks]] implementation closes over mutable variables (either directly or through any of the function operands that its factory or the operations used to construct it receives) from the environment that affects its execution result, then the equality of two supposedly equivalent expressions like {{{task.flatMap(f).flatMap(g) == task.flatMap(a => f(a).flatMap(g))}}} could be compromised. This would depend on the timing of when the variables are mutated — specifically when the mutations occur between the start and end of the task's execution.\
	 * This does not mean that [[Task.subscribeSyncCallbacks]] implementations must avoid closing over mutable variables altogether. Rather, it highlights that if strict adherence to monadic laws is required by your business logic, you should ensure that the mutable variable is not modified during the execution of the involved [[Task]] instances.\
	 * If the goal is just deterministic behavior, it's sufficient that any closed-over mutable variable is only mutated and accessed by actions executed sequentially in a determined order. This is why the contract enforces serialized execution of actions in the order at which the actions were triggered: to maintain determinism, even when closing over mutable variables, provided they are mutated and accessed solely within the actions in said ordered sequence and those actions are deterministic.\
	 * If you require to ensure monadic laws are followed, use [[LatchingTask]]/[[LatchingVenture]] instead.\
	 * Design note: [[Task]] and [[Venture]] are defined as inner traits of [[Doer]] to leverage Scala's path-dependent type checking. This avoids that [[Task]]/[[Venture]] instances that belong to different [[Doer]] instances to be inadvertently composed together without the adapters needed to ensure sequential execution of the component actions.\
	 * While path-dependent type checking is valuable for enforcing this contract, it has a drawback: the compiler's type-path checks are overly strict, requiring compatible singleton types for references, whereas we only need to verify that the [[Task]] instances correspond to the same [[Doer]].\
	 * As a result, the compiler may flag type errors in cases where the contract is not violated, which is undesirable.\
	 * To bypass these path-dependent restrictions when composing tasks across Doer boundaries (or when types cannot be fully proven stable by the compiler), see the trigger implementations in the macro definition, which projects types using the general projected type `Doer#Task`.\
	 * @tparam A the type of the result obtained when executing this [[Task]]. */
	trait Task[+A] extends Observable[A] { thisTask =>

		override def foreach(consumer: A => Unit): Unit = {
			checkWithin()
			triggerSyncCallbacks(consumer, _ => ())
		}

		/** Creates a new [[Task]] that yields exactly the same result (same identity) as this [[Task]] but executes the provided side-effecting function before yielding it.
		 *
		 * $threadSafe
		 *
		 * @param onSuccess a function that is applied to the result of this [[Task]] for its side effects.
		 */
		override def andThen(onSuccess: A => Unit, onError: Throwable => Unit = _ => ()): Task[A] = new Task_AndThen[A](thisTask, onSuccess, onError)

		override def reconcile: Task[Try[A]] = new AbstractTask[Try[A]] {
			override def subscribeSync(downChainMono: MonoObserver[Try[A]]): Subscription = {
				new Subscription with MonoObserver[A] {
					private val upChainSubscription = thisTask.subscribeSync(this)

					override def onSuccess(a: A): Unit = downChainMono.onSuccess(Success(a))

					override def onError(e: Throwable): Unit = downChainMono.onSuccess(Failure(e))

					override def unsubscribe(): Unit = upChainSubscription.unsubscribe()
				}
			}
		}

		override def withFilter(p: A => Boolean): Task[A] = new Task_WithFilter(thisTask, p, false)

		override def withFilterGuarded(p: A => Boolean): Task[A] = new Task_WithFilter(thisTask, p, true)

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
		override def map[B](f: A => B): Task[B] = new Task_Map(thisTask, f, false)

		override def mapGuarded[B](f: A => B): Task[B] = new Task_Map(thisTask, f, true)

		override def recover[B >: A](pf: Throwable => Maybe[B]): Task[B] = new Task_Recover(thisTask, pf, false)

		override def transform[B](f: Try[A] => Try[B]): Task[B] = new Task_Transform(thisTask, f, false)

		/**
		 * Creates a new [[Task]] that yields the result of executing an intermediate [[Observable]] produced by applying the provided function to the final of this [[Task]].
		 *
		 * $threadSafe
		 *
		 * @param f a function that is applied to the result of this [[Task]] to produce an intermediate [[Task]] that is then executed to produce the final result.
		 *
		 * $isExecutedByDoSerEx
		 *
		 * $notGuarded
		 */
		override def flatMap[B](f: A => Observable[B]): Task[B] = new Task_FlatMap(thisTask, f, false)

		override def flatMapGuarded[B](f: A => Observable[B]): Task[B] = new Task_FlatMap(thisTask, f, true)

		override def recoverWith[B >: A](pf: Throwable => Maybe[Observable[B]]): Task[B] = new Task_RecoverWith(thisTask, pf, false)

		override def transformWith[B](f: Try[A] => Observable[B]): Task[B] = new Task_TransformWith(thisTask, f, false)

		override def toFuture(isWithinDoSerEx: Boolean = isInSequence): Future[A] = {
			val promise = Promise[A]()
			subscribe(isWithinDoSerEx)(new MonoObserver[A] {
				override def onSuccess(a: A): Unit = promise.success(a)

				override def onError(e: Throwable): Unit = promise.failure(e)
			})
			promise.future
		}

		/**
		 * Wraps this [[Task]] into another that belongs to another [[Doer]].
		 * Useful to chain [[Task]]'s operations that involve different [[Doer]] instances.
		 * ===Detailed behavior===
		 * Returns a [[Task]] that belongs to the provided [[Doer]]. When it is triggered, it will trigger this task within this [[Doer]] and, when completed, make the returned [[Task]] to yield the result.
		 * CAUTION: Avoid closing over the same mutable variable from two operand functions applied to [[Task]] instances belonging to different [[Doer]]s.
		 * Remember that all function operands provided to [[Venture]] methods are executed within the [[Doer]] that owns it.
		 * Therefore, calling [[triggerCallbacks]] on the returned [[Task]] will execute the `onComplete` passed to it within the `otherDoer`.
		 *
		 * $threadSafe
		 *
		 * @param otherDoer the [[Doer]] to which the returned [[Task]] will belong.
		 */
		override def onBehalfOf(otherDoer: Doer): otherDoer.Task[A] =
			otherDoer.Task_from(thisDoer)(this)

		/** Casts the singleton type of the [[Doer]] instance that owns this [[Task]] to the singleton-type of the provided [[Doer]].
		 * This operation does nothing at runtime. It only tricks the compiler to prevent it from complaining when operating with references to the same [[Doer]] instance but through different type-paths.
		 * CAUTION: Use it only if you are sure that the provided [[Doer]] instance is the one that owns this [[Task]].
		 *
		 * Design note: It was decided to make [[Task]] (and [[Venture]]) an inner class of the [[Doer]] to take advantage of type-path checking to detect when the contract "all operand functions passed to [[Task]] (and [[Venture]]) operations owned by the same [[Doer]] are executed in sequence" might be violated, at compile time.
		 * Using type-path checking to detect contract violations is very valuable, but it comes at a cost, because the type-path check done by the compiler is stricter than necessary -- it checks that the singleton type of the references involved be compatible, and we only need to check that the involved [[Task]] instances belong to the same [[Doer]] instance.
		 * Therefore, the compiler will report type errors in situations the contract is not violated, which is not what we want.
		 * This operation ([[castTypePath()]]) is intended to handle those cases.
		 */
		override def castTypePath[E <: Doer](doer: E): doer.Task[A] = {
			assert(thisDoer eq doer)
			this.asInstanceOf[doer.Task[A]]
		}

		override def guarded: GuardedTask[A] = new GuardedTask(thisTask)
	}

	abstract class AbstractTask[+A] extends Task[A]

	final class GuardedTask[+A](val underlying: Task[A]) extends GuardedMono[A] {
		override def withFilter(p: A => Boolean): Task[A] = new Task_WithFilter(underlying, p, true)

		override def map[B](f: A => B): Task[B] = new Task_Map(underlying, f, true)

		override def flatMap[B](f: A => Observable[B]): Task[B] = new Task_FlatMap(underlying, f, true)

		def recover[B >: A](pf: Throwable => Maybe[B]): Task[B] = new Task_Recover(underlying, pf, true)

		def recoverWith[B >: A](pf: Throwable => Maybe[Observable[B]]): Task[B] = new Task_RecoverWith(underlying, pf, true)

		def transform[B](f: Try[A] => Try[B]): Task[B] = new Task_Transform(underlying, f, true)

		def transformWith[B](f: Try[A] => Observable[B]): Task[B] = new Task_TransformWith(underlying, f, true)
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
	@threadUnsafe lazy val Task_never: Task[Nothing] = new Task_Never()

	/** Creates a [[Task]] whose result is calculated at the call site even before the task is constructed.
	 * $threadSafe
	 *
	 * @param a the already calculated result of the returned [[Task]]. */
	inline def Task_ready[A](a: A): Task[A] = new Task_Ready(a)

	inline def Task_fail(e: Throwable): Task[Nothing] = new Task_Fail(e)

	/** Creates a [[Task]] that lazily executes the provided supplier and yields the value it returns.
	 * ===Detailed behavior===
	 * Creates a task that, when executed, evaluates the `supplier` within the $DoSerEx. If the evaluation finishes:
	 *		- abruptly, will never complete.
	 *		- normally, completes with the evaluation's result.
	 *
	 * $$threadSafe
	 *
	 * @param supplier the supplier of the result. $isExecutedByDoSerEx $notGuarded
	 * @return the [[Task]] described in the method description.
	 */
	inline def Task_apply[A](supplier: () => A): Task[A] = new Task_Apply(supplier)

	/** Creates a [[Task]] that lazily executes the provided [[Task]] supplier and yields whatever the produced [[Task]] yields.
	 * Is equivalent to: {{{Task_apply(supplier).flatMap(identity)}}} but slightly more efficient
	 * ===Detailed behavior===
	 * Creates a task that, when executed:
	 *		- evaluates the `supplier` within the $DoSerEx;
	 *		- then triggers an execution of the returned Task;
	 *		- finally completes with the result of executed task.
	 *
	 * $$threadSafe
	 *
	 * @param supplier the supplier of the task whose execution will give the result. $isExecutedByDoSerEx $notGuarded
	 * @return the task described in the method description.
	 */
	inline def Task_defers[A](supplier: () => Task[A]): Task[A] = new Task_Defers(supplier)

	/** Creates a [[Task]] that wraps a [[Observable]]. */
	def Task_from[A](mono: Observable[A]): Task[A] = {
		mono match {
			case task: Task[A] @unchecked => task
			case _ => new Task_FromMono[A](mono)
		}
	}

	/** Creates a [[Task]] that synchronously wraps a [[Observable]] of another [[Doer]]. Its result will be yielded by the returned [[Task]] in sequence with this [[Doer]].
	 * Useful to delegate work to another [[Doer]] and access its result safely.
	 * $threadSafe
	 *
	 * @param foreignDoer the [[Doer]] to whom the `foreignMono` belongs.
	 * @param foreignMono the [[Observable]] to subscribed to whenever the returned [[Task]] is executed.
	 * @return a [[Task]] that yields what the `foreignMono` yields to subscribers, but the result is yielded in sequence with this [[Doer]]. */
	def Task_from[A](foreignDoer: Doer)(foreignMono: foreignDoer.Observable[A]): Task[A] = {
		if foreignDoer ne thisDoer then new Task_FromForeign[A](foreignDoer, foreignMono)
		else foreignMono match {
			case ft: foreignDoer.Task[A] @unchecked => ft.asInstanceOf[Task[A]]
			case _ => new Task_FromMono(foreignMono.asInstanceOf[Observable[A]])
		}
	}

	/** Create a [[Task]] whose result will be the result of the provided [[Future]] when it completes.\
	 * Useful to access the result of a process that was already started in an alien executor as if it were executed sequentially.\
	 * $threadSafe
	 * @param future the future to wait for.
	 * @return the [[Task]] described in the method description. */
	inline final def Task_from[A](future: Future[A]): Task[A] = new Task_FromFuture(future)

	/** Creates a [[Task]] whose result will be the result of the [[Future]] returned by the provided supplier.\
	 * Useful to start a process in an alien executor and access its result as if it were executed sequentially.\
	 * The alien executor may be the $DoSerEx of this [[Doer]].\
	 * $threadSafe
	 * @param supplier a function that starts the process and return a [[Future]] of its result. $isExecutedByDoSerEx $unhandledErrorsArePropagatedToVentureResult
	 * @return the [[Task]] described in the method description. */
	inline final def Task_from[A](supplier: () => Future[A], isGuarded: Boolean = false): Task[A] = new Task_FromFutureSupplier(supplier, isGuarded)


	/**
	 * Creates a [[Task]] that yields the result of applying the bifunction `f` to what the provided duties yield.
	 * When executed, simultaneously triggers and execution of each task and returns their results combined by the provided function.
	 * Given the serial-execution nature of [[Doer]] this operation only has sense when the provided [[Task]]s involves foreign ([[Task_from]]) or alien ([[Task_from]]) actions.
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
	inline def Task_combine[A, B, C](taskA: Task[A], taskB: Task[B])(f: (A, B) => C): Task[C] = new Task_Combined(taskA, taskB, f)

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
	inline def Task_sequenceToArray[A: ClassTag, C[x] <: Iterable[x]](monos: C[Observable[A]]): Task[Array[A]] = new Task_Sequence[A, C](monos)

	/** Creates a [[Task]] that, when executed, simultaneously triggers an execution for each [[Task]] in the received [[Iterable]], and completes with an [[Iterable]] containing their results, successful or not, wrapped with [[Try]], in the same order.\
	 * The result, if any, is always successful.
	 * $threadSafe \
	 * @param ventures the [[Iterable]] of [[Venture]]s that the returned [[Task]] will trigger simultaneously to combine their results.
	 * @param factory the [[IterableFactory]] needed to build the [[Iterable]] that will contain the results. Note that most [[Iterable]] implementations' companion objects are an [[IterableFactory]].
	 * @tparam A the result type of all the provided [[Venture]]s.
	 * @tparam C the higher-kinded type of the [[Iterable]] of [[Venture]]s.
	 * @tparam To the higher-kinded type of the [[Iterable]] that will contain the results.
	 * @return the successful task described in the method description. */
	def Task_sequenceHardy[A: ClassTag, C[x] <: Iterable[x], To[x] <: Iterable[x]](factory: IterableFactory[To], ventures: C[Task[A]]): Task[To[Try[A]]] = {
		Task_sequenceHardyToArray(ventures).map { array =>
			val builder = factory.newBuilder[Try[A]]
			var index = 0
			while index < array.length do {
				builder.addOne(array(index))
				index += 1
			}
			builder.result()
		}
	}

	/** Like [[Task_sequenceHardy]] but the resulting collection's higher-kinded type `To` is fixed to [[Array]]. */
	inline def Task_sequenceHardyToArray[A: ClassTag, C[x] <: Iterable[x]](ventures: C[Observable[A]]): Task[Array[Try[A]]] =
		new Task_SequenceHardyToArray[A, C](ventures)

	//// Concrete implementations of [[Task]] returned by instance methods ////

	/** $suppressSyntheticCompanionObject */
	private inline def Task_AndThen(trap: Nothing): Any = trap

	final class Task_AndThen[+A](upChainMono: Observable[A], onSuccessCbf: A => Unit, onErrorCbf: Throwable => Unit) extends AbstractTask[A] {
		override def subscribeSync(downChainObserver: MonoObserver[A]): Subscription = {
			upChainMono.subscribeSync(new MonoObserver[A] { // TODO Optimize
				override def onSuccess(a: A): Unit = {
					onSuccessCbf(a)
					downChainObserver.onSuccess(a)
				}

				override def onError(ex: Throwable): Unit = {
					onErrorCbf(ex)
					downChainObserver.onError(ex)
				}
			})
		}

		override def toString: String = deriveToString[Task_AndThen[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_WithFilter(trap: Nothing): Any = trap

	final class Task_WithFilter[+A](upChainMono: Observable[A], p: A => Boolean, isGuarded: Boolean) extends AbstractTask[A] {
		override def subscribeSync(downChainObserver: MonoObserver[A]): Subscription = {
			upChainMono.subscribeSync(new MonoObserver[A] { // TODO Optimize
				override def onSuccess(a: A): Unit = {
					val pass =
						if isGuarded then {
							try (if p(a) then 1 else 0) catch {
								case NonFatal(e) =>
									downChainObserver.onError(e)
									-1
							}
						} else if p(a) then 1 else 0
					if pass == 1 then downChainObserver.onSuccess(a)
					else if pass == 0 then downChainObserver.onError(new NoSuchElementException)
				}

				override def onError(e: Throwable): Unit = downChainObserver.onError(e)
			})
		}

		override def toString: String = deriveToString[Task_WithFilter[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_Map(trap: Nothing): Any = trap

	final class Task_Map[+A, +B](upChainMono: Observable[A], f: A => B, isGuarded: Boolean) extends AbstractTask[B] {
		override def subscribeSync(downChainObserver: MonoObserver[B]): Subscription = {
			// Propagates the subscription upstream while mapping success values
			upChainMono.subscribeSync(new MonoObserver[A] { // TODO Optimize
				override def onSuccess(a: A): Unit = {
					if isGuarded then {
						val maybeB = try Maybe(f(a)) catch {
							case NonFatal(e) =>
								downChainObserver.onError(e)
								Maybe.empty
						}
						maybeB.foreach(downChainObserver.onSuccess)
					}
					else downChainObserver.onSuccess(f(a))
				}

				override def onError(ex: Throwable): Unit = downChainObserver.onError(ex)
			})
		}

		override def toString: String = deriveToString[Task_Map[A, B]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_Recover(trap: Nothing): Any = trap

	final class Task_Recover[-A, +B >: A](upChainMono: Observable[A], pf: Throwable => Maybe[B], isGuarded: Boolean) extends AbstractTask[B] {
		override def subscribeSync(downChainObserver: MonoObserver[B]): Subscription = {
			upChainMono.subscribeSync(new MonoObserver[A] { // TODO Optimize
				override def onSuccess(a: A): Unit = downChainObserver.onSuccess(a)

				override def onError(e1: Throwable): Unit = {
					val maybeMaybeB = if isGuarded then try Maybe(pf(e1)) catch {
						case NonFatal(e2) =>
							downChainObserver.onError(e2)
							Maybe.empty
					} else Maybe(pf(e1))
					maybeMaybeB.foreach(_.fold(downChainObserver.onError(e1))(downChainObserver.onSuccess))
				}
			})
		}

		override def toString: String = deriveToString[Task_Recover[A, B]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_Transform(trap: Nothing): Any = trap

	final class Task_Transform[-A, B](upChainMono: Task[A], f: Try[A] => Try[B], isGuarded: Boolean) extends AbstractTask[B] {

		override def subscribeSync(downChainMono: MonoObserver[B]): Subscription = {
			upChainMono.subscribeSync(new MonoObserver[A] { // TODO Optimize
				override def onSuccess(a: A): Unit = handle(Success(a))

				override def onError(e: Throwable): Unit = handle(Failure(e))

				private def handle(tryA: Try[A]): Unit = {
					val tryB = if isGuarded then try f(tryA) catch {
						case NonFatal(e) => Failure(e)
					} else f(tryA)
					tryB match {
						case Success(b) => downChainMono.onSuccess(b)
						case Failure(ex) => downChainMono.onError(ex)
					}
				}
			})
		}

		override def toString: String = deriveToString[Task_Transform[A, B]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_FlatMap(trap: Nothing): Any = trap

	/** TODO This class is very similar to [[DefaultCaptor_FlatMap]]. Consider removing duplication by exteinding a common super class. */
	final class Task_FlatMap[+A, +B](upChainMono: Observable[A], f: A => Observable[B], isGuarded: Boolean) extends AbstractTask[B] {
		override def subscribeSync(downChainObserver: MonoObserver[B]): Subscription = {
			new Subscription with MonoObserver[A] {
				private var isActive = true
				private var maybeInnerSubscription: Maybe[Subscription] = Maybe.empty
				private var maybeUpChainSubscription: Maybe[Subscription] = Maybe.empty
				{
					val upChainSubscription = upChainMono.subscribeSync(this)
					if isActive then maybeUpChainSubscription = Maybe(upChainSubscription)
				}

				override def onSuccess(a: A): Unit = {
					if isActive then {
						maybeUpChainSubscription = Maybe.empty
						val maybeInnerMonoB =
							if isGuarded then {
								try Maybe(f(a)) catch {
									case NonFatal(e) =>
										downChainObserver.onError(e)
										Maybe.empty
								}
							} else Maybe(f(a))
						maybeInnerMonoB.foreach { innerMonoB =>
							val innerSubscription = innerMonoB.subscribeSync(downChainObserver)
							if isActive then maybeInnerSubscription = Maybe(innerSubscription)
						}
					}
				}

				override def onError(e: Throwable): Unit = {
					if isActive then {
						isActive = false
						maybeUpChainSubscription = Maybe.empty
						downChainObserver.onError(e)
					}
				}

				override def unsubscribe(): Unit = {
					checkWithin()
					isActive = false
					val mus = maybeUpChainSubscription
					val mis = maybeInnerSubscription
					maybeUpChainSubscription = Maybe.empty
					maybeInnerSubscription = Maybe.empty
					mus.foreach(_.unsubscribe())
					mis.foreach(_.unsubscribe())
				}
			}
		}

		override def toString: String = deriveToString[Task_FlatMap[A, B]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_RecoverWith(trap: Nothing): Any = trap

	final class Task_RecoverWith[-A, +B >: A](upChainMono: Observable[A], pf: Throwable => Maybe[Observable[B]], isGuarded: Boolean) extends AbstractTask[B] {
		override def subscribeSync(downChainObserver: MonoObserver[B]): Subscription = {
			new Subscription with MonoObserver[A] {
				private var isActive = true
				private var maybeInnerSubscription: Maybe[Subscription] = Maybe.empty
				private var maybeUpChainSubscription: Maybe[Subscription] = Maybe.empty
				{ // Constructor
					val upChainSubscription = upChainMono.subscribeSync(this)
					if isActive then maybeUpChainSubscription = Maybe(upChainSubscription)
				}

				override def onSuccess(a: A): Unit = {
					if isActive then {
						isActive = false
						maybeUpChainSubscription = Maybe.empty
						downChainObserver.onSuccess(a)
					}
				}

				override def onError(e1: Throwable): Unit = {
					if isActive then {
						maybeUpChainSubscription = Maybe.empty
						val maybeMaybeMonoB =
							if isGuarded then {
								try Maybe(pf(e1)) catch {
									case NonFatal(e2) =>
										downChainObserver.onError(e2)
										Maybe.empty
								}
							} else Maybe(pf(e1))

						if isActive then maybeMaybeMonoB.foreach { maybeMonoB =>
							maybeMonoB.fold(downChainObserver.onError(e1)) { monoB =>
								val innerSubscription = monoB.subscribeSync(downChainObserver)
								if isActive then maybeInnerSubscription = Maybe(innerSubscription)
							}
						}
					}
				}

				override def unsubscribe(): Unit = {
					checkWithin()
					isActive = false
					val up = maybeUpChainSubscription
					val inner = maybeInnerSubscription
					maybeUpChainSubscription = Maybe.empty
					maybeInnerSubscription = Maybe.empty
					up.foreach(_.unsubscribe())
					inner.foreach(_.unsubscribe())
				}
			}
		}

		override def toString: String = deriveToString[Task_RecoverWith[A, B]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_TransformWith(trap: Nothing): Any = trap

	final class Task_TransformWith[+A, +B](upChainMono: Observable[A], f: Try[A] => Observable[B], isGuarded: Boolean) extends AbstractTask[B] {
		override def subscribeSync(downChainObserver: MonoObserver[B]): Subscription = {
			new Subscription with MonoObserver[A] {
				private var isActive = true
				private var maybeUpChainSubscription: Maybe[Subscription] = Maybe.empty
				private var maybeInnerSubscription: Maybe[Subscription] = Maybe.empty

				{ // Constructor
					val upChainSubscription = upChainMono.subscribeSync(this)
					if isActive then maybeUpChainSubscription = Maybe(upChainSubscription)
				}

				override def onSuccess(a: A): Unit = handle(Success(a))

				override def onError(e: Throwable): Unit = handle(Failure(e))

				private def handle(tryA: Try[A]): Unit = {
					if isActive then {
						maybeUpChainSubscription = Maybe.empty
						val maybeMonoB = if isGuarded then try Maybe(f(tryA)) catch {
							case NonFatal(e) =>
								isActive = false
								maybeUpChainSubscription = Maybe.empty
								downChainObserver.onError(e)
								Maybe.empty
						} else Maybe(f(tryA))
						maybeMonoB.foreach { monoB =>
							val innerSubscription = monoB.subscribeSync(downChainObserver)
							if isActive then maybeInnerSubscription = Maybe(innerSubscription)
						}
					}
				}

				override def unsubscribe(): Unit = {
					checkWithin()
					isActive = false
					val mus = maybeUpChainSubscription
					maybeUpChainSubscription = Maybe.empty
					mus.foreach(_.unsubscribe())
					val mis = maybeInnerSubscription
					maybeInnerSubscription = Maybe.empty
					mis.foreach(_.unsubscribe())
				}
			}
		}

		override def toString: String = deriveToString[Task_TransformWith[A, B]](this)
	}

	//// Concrete implementations of [[Task]] returned by factory methods ////

	/** $suppressSyntheticCompanionObject */
	private inline def Task_Never(trap: Nothing): Any = trap

	class Task_Never extends AbstractTask[Nothing] {
		override def subscribeSync(downChainObserver: MonoObserver[Nothing]): Subscription = {
			// Nothing is ever emitted, so returns empty subscription
			Subscription_empty
		}

		override def toString: String = deriveToString[Task_Never](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_Ready(trap: Nothing): Any = trap

	final class Task_Ready[+A](a: A) extends AbstractTask[A] {
		override def subscribeSync(downChainObserver: MonoObserver[A]): Subscription = {
			// Completes immediately, so returns empty subscription
			downChainObserver.onSuccess(a)
			Subscription_empty
		}

		override def toFuture(isWithinDoSiThEx: Boolean = isInSequence): Future[A] = Future.successful(a)

		override def toString: String = deriveToString[Task_Ready[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_Fail(trap: Nothing): Any = trap

	final class Task_Fail(e: Throwable) extends AbstractTask[Nothing] {
		override def subscribeSync(downChainObserver: MonoObserver[Nothing]): Subscription = {
			downChainObserver.onError(e)
			Subscription_empty
		}

		override def toFuture(isWithinDoSiThEx: Boolean = isInSequence): Future[Nothing] = Future.failed(e)

		override def toString: String = deriveToString[Task_Fail](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_Apply(trap: Nothing): Any = trap

	final class Task_Apply[+A](supplier: () => A) extends AbstractTask[A] {
		override def subscribeSync(downChainObserver: MonoObserver[A]): Subscription = {
			// Completes immediately, so returns empty subscription
			downChainObserver.onSuccess(supplier())
			Subscription_empty
		}

		override def toString: String = deriveToString[Task_Apply[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_Defers(trap: Nothing): Any = trap

	final class Task_Defers[+A](supplier: () => Task[A]) extends AbstractTask[A] {
		override def subscribeSync(downChainObserver: MonoObserver[A]): Subscription = {
			// Propagates the inner subscription directly
			supplier().subscribeSync(downChainObserver)
		}

		override def toString: String = deriveToString[Task_Defers[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_FromMono(trap: Nothing): Any = trap

	final class Task_FromMono[+A](monoA: Observable[A]) extends AbstractTask[A] {
		override def subscribeSync(downChainObserver: MonoObserver[A]): Subscription = monoA.subscribeSync(downChainObserver)

		override def toString: String = deriveToString[Task_FromMono[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_FromForeign(trap: Nothing): Any = trap

	final class Task_FromForeign[+A](foreignDoer: Doer, foreignMono: foreignDoer.Observable[A]) extends AbstractTask[A] {
		override def subscribeSync(downChainSubscripton: MonoObserver[A]): Subscription = {
			new Subscription with MonoObserver[A] with Runnable {
				@volatile private var isActive = true
				private var maybeForeignSubscription: Maybe[foreignDoer.Subscription] = Maybe.empty

				{ // Constructor
					foreignDoer.executeSequentially(this)
				}

				override def run(): Unit = {
					if isActive then {
						val foreignSubscription = foreignMono.subscribeSync(this.asInstanceOf[foreignDoer.MonoObserver[A]])
						// Note: Unlike single-threaded tasks (such as [[Task_FlatMap]]), we do not perform defensive checks to guarantee the clearing of maybeForeignSubscription because a failure to clear the reference is very rare and only results in a transient, minor memory leak (which is reclaimed once the delegating subscription is garbage collected), the performance and complexity cost of such optimization is not justified here.
						maybeForeignSubscription = Maybe(foreignSubscription)
					}
				}

				override def onSuccess(a: A): Unit = { // runs in foreignDoer
					if isActive then {
						maybeForeignSubscription = Maybe.empty
						thisDoer.run {
							if isActive then {
								isActive = false
								downChainSubscripton.onSuccess(a)
							}
						}
					}
				}

				override def onError(e: Throwable): Unit = { // runs in foreignDoer
					if isActive then {
						maybeForeignSubscription = Maybe.empty
						thisDoer.run {
							if isActive then {
								isActive = false
								downChainSubscripton.onError(e)
							}
						}
					}
				}

				override def unsubscribe(): Unit = {
					if isActive then {
						isActive = false
						foreignDoer.run {
							val mfs = maybeForeignSubscription
							maybeForeignSubscription = Maybe.empty
							mfs.foreach(_.unsubscribe())
						}
					}
				}
			}
		}

		override def toString: String = deriveToString[Task_FromForeign[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_FromFuture(trap: Nothing): Any = trap

	final class Task_FromFuture[+A](future: Future[A]) extends AbstractTask[A] {
		override def subscribeSync(downChainObserver: MonoObserver[A]): Subscription = {
			new Subscription with (Try[A] => Unit) {
				private var active = true
				{ // Constructor
					future.onComplete(this)(using ownSingleThreadExecutionContext)
				}

				override def apply(tryA: Try[A]): Unit = {
					if active then tryA match {
						case Success(a) => downChainObserver.onSuccess(a)
						case Failure(ex) => downChainObserver.onError(ex)
					}
				}

				override def unsubscribe(): Unit = {
					checkWithin()
					active = false
				}
			}
		}

		override def toString: String = deriveToString[Task_FromFuture[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_FromFutureSupplier(trap: Nothing): Any = trap

	final class Task_FromFutureSupplier[+A](supplier: () => Future[A], isGuarded: Boolean) extends AbstractTask[A] {
		override def subscribeSync(downChainObserver: MonoObserver[A]): Subscription = {
			new Subscription with (Try[A] => Unit) {
				private var active = true
				{ // Constructor
					val future =
						if isGuarded then try supplier() catch {
							case NonFatal(e) => Future.failed(e)
						} else supplier()
					future.onComplete(this)(using ownSingleThreadExecutionContext)
				}

				override def apply(tryA: Try[A]): Unit = {
					if active then tryA match {
						case Success(a) => downChainObserver.onSuccess(a)
						case Failure(ex) => downChainObserver.onError(ex)
					}
				}

				override def unsubscribe(): Unit = {
					checkWithin()
					active = false
				}
			}
		}

		override def toString: String = deriveToString[Task_FromFutureSupplier[A]](this)
	}


	/** $suppressSyntheticCompanionObject */
	private inline def Task_Combined(trap: Nothing): Any = trap

	final class Task_Combined[+A, +B, +C](taskA: Task[A], taskB: Task[B], f: (A, B) => C) extends AbstractTask[C] {
		override def subscribeSync(downChainObserver: MonoObserver[C]): Subscription = new Subscription with MonoObserver[A] {
			private var isActive = true
			private var maybeA: Maybe[A] = Maybe.empty
			private var maybeB: Maybe[B] = Maybe.empty
			private var maybeSubscriptionA: Maybe[Subscription] = Maybe.empty
			private var maybeSubscriptionB: Maybe[Subscription] = Maybe.empty

			{ // Constructor
				val subscriptionA = taskA.subscribeSync(this)

				if isActive then {
					if maybeA.isEmpty then maybeSubscriptionA = Maybe(subscriptionA)
					val subscriptionB = taskB.subscribeSync(new MonoObserver[B] {
						override def onSuccess(b: B): Unit = {
							if isActive then {
								maybeSubscriptionB = Maybe.empty
								maybeA.fold {
									maybeB = Maybe(b)
								} { a =>
									isActive = false
									downChainObserver.onSuccess(f(a, b))
								}
							}
						}

						override def onError(e: Throwable): Unit = {
							if isActive then {
								isActive = false
								maybeSubscriptionB = Maybe.empty
								downChainObserver.onError(e)
								maybeSubscriptionA.foreach(_.unsubscribe())
							}
						}
					})
					if isActive && maybeB.isEmpty then maybeSubscriptionB = Maybe(subscriptionB)
				}
			}

			override def onSuccess(a: A): Unit = {
				if isActive then {
					maybeSubscriptionA = Maybe.empty
					maybeB.fold {
						maybeA = Maybe(a)
					} { b =>
						isActive = false
						downChainObserver.onSuccess(f(a, b))
					}
				}
			}

			override def onError(e: Throwable): Unit = {
				if isActive then {
					isActive = false
					maybeSubscriptionA = Maybe.empty
					downChainObserver.onError(e)
					maybeSubscriptionB.foreach(_.unsubscribe())
				}
			}

			override def unsubscribe(): Unit = {
				checkWithin()
				if isActive then {
					isActive = false
					val msa = maybeSubscriptionA
					val msb = maybeSubscriptionB
					maybeSubscriptionA = Maybe.empty
					maybeSubscriptionB = Maybe.empty
					msa.foreach(_.unsubscribe())
					msb.foreach(_.unsubscribe())
				}
			}
		}

		override def toString: String = deriveToString[Task_Combined[A, B, C]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_Sequence(trap: Nothing): Any = trap

	/** @see [[Task_sequenceToArray]] */
	final class Task_Sequence[A: ClassTag, +C[x] <: Iterable[x]](monos: C[Observable[A]]) extends AbstractTask[Array[A]] {
		override def subscribeSync(downChainObserver: MonoObserver[Array[A]]): Subscription = {
			val size = monos.size
			val array = Array.ofDim[A](size)
			if size == 0 then {
				downChainObserver.onSuccess(array)
				Subscription_empty
			} else new Subscription {
				private var completedCounter: Int = 0
				private var isActive = true
				private val subscriptions = new Array[Subscription](size)

				{ // Constructor
					var index = 0
					val monosIterator = monos.iterator
					while index < size && isActive do {
						val mono = monosIterator.next()
						val monoIndex = index
						val subscription = mono.subscribeSync(new MonoObserver[A] { // TODO this allocation could be avoided if the MonoObserver propagated the subscription id/index.
							override def onSuccess(a: A): Unit = {
								if isActive then {
									array(monoIndex) = a
									completedCounter += 1
									if completedCounter == size then {
										isActive = false
										downChainObserver.onSuccess(array)
									}
								}
							}

							override def onError(ex: Throwable): Unit = {
								if isActive then {
									isActive = false
									downChainObserver.onError(ex)
									unsubscribeAll()
								}
							}
						})
						if isActive then subscriptions(monoIndex) = subscription
						index += 1
					}
				}

				private final def unsubscribeAll(): Unit = {
					var index = 0
					while index < size do {
						val sub = subscriptions(index)
						if sub != null then {
							subscriptions(index) = null
							sub.unsubscribe()
						}
						index += 1
					}
				}

				override def unsubscribe(): Unit = {
					checkWithin()
					if isActive then {
						isActive = false
						unsubscribeAll()
					}
				}
			}
		}
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_SequenceHardy(trap: Nothing): Any = trap

	final class Task_SequenceHardyToArray[A: ClassTag, C[x] <: Iterable[x]](monos: C[Observable[A]]) extends AbstractTask[Array[Try[A]]] {
		override def subscribeSync(monoObserver: MonoObserver[Array[Try[A]]]): Subscription = {
			val size = monos.size
			val array = Array.ofDim[Try[A]](size)
			if size == 0 then {
				monoObserver.onSuccess(array)
				Subscription_empty
			} else new Subscription {
				private val monosSubscriptions = new Array[Subscription](size)
				private var completedCounter: Int = 0
				private var index: Int = 0

				{ // constructor
					val monosIterator = monos.iterator
					while index < size do {
						val mono = monosIterator.next()
						val ventureIndex = index

						val monoSubscription = new Subscription {
							private var active = true
							private var innerSub: Subscription = Subscription_empty
							{
								innerSub = mono.subscribeSync(new MonoObserver[A] {
									override def onSuccess(a: A): Unit = {
										if active then {
											array(ventureIndex) = Success(a)
											completedCounter += 1
											if completedCounter == size then monoObserver.onSuccess(array)
										}
									}

									override def onError(ex: Throwable): Unit = {
										if active then {
											array(ventureIndex) = Failure(ex)
											completedCounter += 1
											if completedCounter == size then monoObserver.onSuccess(array)
										}
									}
								})
							}

							override def unsubscribe(): Unit = {
								active = false
								innerSub.unsubscribe()
							}
						}
						monosSubscriptions(index) = monoSubscription
						index += 1
					}
				}

				override def unsubscribe(): Unit = {
					var i = 0
					while i < size do {
						val s = monosSubscriptions(i)
						if s != null then s.unsubscribe()
						i += 1
					}
				}
			}
		}
	}

	////////////// Capturer ///////////////

	/** A [[Observable]] whose completion is externally controlled and, upon completion, exposes the same result to all present and future subscribers.\
	 * Useful to capture the result of a computation that already started.\
	 * Once completed, subscribing a consumer executes the call-back immediately. Note that linking a down-chain subscribes the first link as consumer.
	 * The monadic laws are always upheld. Before completion, they can’t be observed because no result exists yet; after completion, they can be observed in the cached result.\
	 * Allows dynamic subscription and unsubscription.
	 * TODO rename to "Capturer" */
	sealed abstract class LatchingTask[+A] extends Observable[A] {

		inline def asMono: Observable[A] = this

		/** Subscribes a consumer of the captured value.\
		 * The subscription is automatically removed after a value was captured and the received consumer is executed.\
		 * If a value was already captured when this method is called, the provided consumer is invoked synchronously and no subscription occurs.\
		 * Otherwise, the provided consumer is schedule to run upon the capture occurs in subscription orden (after sequentially running all the previously subscribed captured value consumers).\
		 * @note CAUTION: This method does not prevent duplicate subscriptions.
		 * @note CAUTION: Must be called within the $DoSerEx */
		override def subscribeSync(monoObserver: MonoObserver[A]): Subscription

		/** @return a [[Trial]] with the captured value if the target computation completed successfully, a [[Throwable]] if failed, or [[Trial.empty]] if pending.
		 * @note CAUTION: Must be called within the $DoSerEx */
		def maybeResult: Trial[A]

		/** @return true if this the target computation completed, successfully or not.
		 * @note CAUTION: Must be called within the $DoSerEx */
		inline def isCompleted: Boolean = maybeResult.isDefined

		/** @return true if this [[LatchingTask]] is still pending; or false if it was completed.
		 * @note CAUTION: Must be called within the $DoSerEx */
		inline def isPending: Boolean = maybeResult.isEmpty

		inline def asLatchingTask: LatchingTask[A] = this // TODO is this necessary?

		override def andThen(onSuccess: A => Unit, onError: Throwable => Unit): LatchingTask[A] = {
			triggerSyncCallbacks(onSuccess, onError)
			this
		}

		override def reconcile: LatchingTask[Try[A]]

		override def withFilter(predicate: A => Boolean): LatchingTask[A]

		override def withFilterGuarded(predicate: A => Boolean): LatchingTask[A]

		override def map[B](f: A => B): LatchingTask[B]

		override def mapGuarded[B](f: A => B): LatchingTask[B]

		override def flatMap[B](f: A => Observable[B]): Observable[B]

		@targetName("flatMapTask")
		def flatMap[B](f: A => Task[B]): Task[B]

		@targetName("flatMapCapturer")
		def flatMap[B](f: A => LatchingTask[B]): LatchingTask[B]

		override def flatMapGuarded[B](f: A => Observable[B]): Observable[B]

		@targetName("flatMapGuardedTask")
		def flatMapGuarded[B](f: A => Task[B]): Task[B]

		@targetName("flatMapGuardedCapturer")
		def flatMapGuarded[B](f: A => LatchingTask[B]): LatchingTask[B]

		override def transform[B](f: Try[A] => Try[B]): LatchingTask[B]

		override def transformWith[B](f: Try[A] => Observable[B]): Observable[B]

		@targetName("transformWithCapturer")
		def transformWith[B](f: Try[A] => LatchingTask[B]): LatchingTask[B]

		@targetName("transformWithTask")
		def transformWith[B](f: Try[A] => Task[B]): Task[B]

		override def recover[B >: A](pf: Throwable => Maybe[B]): LatchingTask[B]

		override def recoverWith[B >: A](pf: Throwable => Maybe[Observable[B]]): Observable[B]

		@targetName("recoverWithCapturer")
		def recoverWith[B >: A](pf: Throwable => Maybe[LatchingTask[B]]): LatchingTask[B]

		@targetName("recoverWithTask")
		def recoverWith[B >: A](pf: Throwable => Maybe[Task[B]]): Task[B]

		override def onBehalfOf(otherDoer: Doer): otherDoer.LatchingTask[A]

		override def castTypePath[E <: Doer](doer: E): doer.LatchingTask[A] = {
			assert(thisDoer eq doer)
			this.asInstanceOf[doer.LatchingTask[A]]
		}

		override def guarded: GuardedCapturer[A] = new GuardedCapturer[A](this)
	}

	final class GuardedCapturer[+A](val underlying: LatchingTask[A]) extends GuardedMono[A] {

		override def withFilter(predicate: A => Boolean): LatchingTask[A] = underlying.withFilterGuarded(predicate)

		override def map[B](f: A => B): LatchingTask[B] = underlying.mapGuarded(f)

		override def flatMap[B](f: A => Observable[B]): Observable[B] = underlying.flatMapGuarded(f)

		@targetName("flatMapCapturer")
		def flatMap[B](f: A => LatchingTask[B]): LatchingTask[B] = underlying.flatMapGuarded(f)

		@targetName("flatMapTask")
		def flatMap[B](f: A => Task[B]): Task[B] = underlying.flatMapGuarded(f)

		/*
		override def transform[B](f: Try[A] => Try[B]): LatchingTask[B] = ???

		override def transformWith[B](f: Try[A] => Observable[B]): Observable[B] = ???

		@targetName("transformWithCapturer")
		def transformWith[B](f: Try[A] => LatchingTask[B]): LatchingTask[B] = ???

		@targetName("transformWithTask")
		def transformWith[B](f: Try[A] => Task[B]): Task[B] = ???

		override def recover[B >: A](pf: Throwable => Maybe[B]): LatchingTask[B] = ???

		override def recoverWith[B >: A](pf: Throwable => Maybe[Observable[B]]): Observable[B] = ???

		@targetName("recoverWithCapturer")
		def recoverWith[B >: A](pf: Throwable => Maybe[LatchingTask[B]]): LatchingTask[B] = ???

		@targetName("recoverWithTask")
		def recoverWith[B >: A](pf: Throwable => Maybe[Task[B]]): Task[B] = ???
		*/
	}

	/** A partial implementation of [[LatchingTask]]. Implements everything except [[state]].
	 * Implementation note: Despite extending [[Muxer]], the [[Muxer]] state is actually a component of this class. The composition is implemented by extending [[Muxer]], instead of holding it as a component, to avoid an allocation. Also, given the [[Muxer]] pseudo-component is not public (none of its methods are public), it should not prevent variance on this class. */
	abstract class DefaultCapturer[+A] extends LatchingTask[A], Muxer[A @uncheckedVariance, MonoObserver] { thisCaptor =>
		protected def state: Trial[A]

		override def subscribeSync(monoObserver: MonoObserver[A]): Subscription = {
			state.fold {
				addTarget(monoObserver)
				new Subscription {
					override def unsubscribe(): Unit = {
						checkWithin()
						removeAllMatching(monoObserver)
					}
				}
			} { ex =>
				monoObserver.onError(ex)
				Subscription_empty
			} { a =>
				monoObserver.onSuccess(a)
				Subscription_empty
			}
		}

		override def triggerSync(downChainObserver: MonoObserver[A]): Unit = {
			state.fold {
				addTarget(downChainObserver)
			} { ex =>
				downChainObserver.onError(ex)
			} { a =>
				downChainObserver.onSuccess(a)
			}
		}

		override def maybeResult: Trial[A] = {
			checkWithin()
			state
		}

		override def foreach(consumer: A => Unit): Unit = {
			checkWithin()
			state.fold {
				triggerSyncCallbacks(consumer, _ => ())
			}(_ => ())(consumer)
		}

		override def reconcile: LatchingTask[Try[A]] = {
			state.fold {
				new DefaultCaptor[Try[A]] with MonoObserver[A] {
					thisCaptor.triggerSync(this)

					override def onSuccess(a: A): Unit = this.fulfillSync(Success(a))

					override def onError(e: Throwable): Unit = this.fulfillSync(Failure(e))
				}
			} { e => ReadyTask(Failure(e))
			} { a => ReadyTask(Success(a))
			}
		}

		override def withFilter(predicate: A => Boolean): LatchingTask[A] = {
			checkWithin()
			state.fold[LatchingTask[A]] {
				new DefaultCaptor[A] with MonoObserver[A] {
					thisCaptor.triggerSync(this)

					override def onSuccess(a: A): Unit = {
						if predicate(a) then fulfillSync(a)
						else breakSync(new NoSuchElementException("Covenant filter predicate is not satisfied"))
					}

					override def onError(ex: Throwable): Unit = breakSync(ex)
				}
			} { ex => new Failed(ex) } { a =>
				if predicate(a) then this
				else new Failed(new NoSuchElementException("Covenant filter predicate is not satisfied"))
			}
		}

		override def withFilterGuarded(predicate: A => Boolean): LatchingTask[A] = {
			checkWithin()
			state.fold[LatchingTask[A]] {
				new DefaultCaptor[A] with MonoObserver[A] {
					thisCaptor.triggerSync(this)

					override def onSuccess(a: A): Unit = {
						if predicate(a) then fulfillSync(a)
						else breakSync(new NoSuchElementException("Covenant filter predicate is not satisfied"))
					}

					override def onError(ex: Throwable): Unit = breakSync(ex)
				}
			} { ex => new Failed(ex) } { a =>
				if predicate(a) then this
				else new Failed(new NoSuchElementException("Covenant filter predicate is not satisfied"))
			}
		}

		override def map[B](f: A => B): LatchingTask[B] = {
			checkWithin()
			state.fold[LatchingTask[B]] {
				new DefaultCaptor[B] with MonoObserver[A] {
					thisCaptor.triggerSync(this)

					override def onSuccess(a: A): Unit = fulfillSync(f(a))

					override def onError(ex: Throwable): Unit = breakSync(ex)
				}
			} { ex => new Failed(ex) } { a => new ReadyTask[B](f(a)) }
		}

		override def mapGuarded[B](f: A => B): LatchingTask[B] = {
			checkWithin()
			state.fold[LatchingTask[B]] {
				new DefaultCaptor[B] with MonoObserver[A] {
					thisCaptor.triggerSync(this)

					override def onSuccess(a: A): Unit = {
						val maybeB = try Maybe(f(a)) catch {
							case NonFatal(e) =>
								breakSync(e)
								Maybe.empty
						}
						maybeB.foreach(b => fulfillSync(b))
					}

					override def onError(ex: Throwable): Unit = breakSync(ex)
				}
			} { ex => new Failed(ex) } { a =>
				try {
					new ReadyTask[B](f(a))
				} catch {
					case NonFatal(e) => new Failed(e)
				}
			}
		}

		override def flatMap[B](f: A => Observable[B]): Observable[B] = {
			checkWithin()
			state.fold {
				new DefaultCaptor_FlatMap[A, B](this, f, false)
			} { ex => Task_fail(ex) } { a => f(a) }
		}

		@targetName("flatMapCapturer")
		override def flatMap[B](f: A => LatchingTask[B]): LatchingTask[B] = {
			checkWithin()
			state.fold[LatchingTask[B]] {
				new DefaultCaptor[B] with MonoObserver[A] {
					thisCaptor.triggerSync(this)

					override def onSuccess(a: A): Unit = {
						f(a).subscribeSync(new MonoObserver[B] {
							override def onSuccess(b: B): Unit = fulfillSync(b)

							override def onError(ex: Throwable): Unit = breakSync(ex)
						})
					}

					override def onError(e: Throwable): Unit = breakSync(e)
				}
			} { ex => new Failed(ex) } { a => f(a) }
		}

		@targetName("flatMapTask")
		override def flatMap[B](f: A => Task[B]): Task[B] = {
			checkWithin()
			state.fold[Task[B]] {
				new DefaultCaptor_FlatMap[A, B](this, f, false)
			} { ex => Task_fail(ex) } { a => f(a) }
		}

		override def flatMapGuarded[B](f: A => Observable[B]): Observable[B] = {
			checkWithin()
			state.fold[Observable[B]] {
				new DefaultCaptor_FlatMap[A, B](this, f, true)
			} { ex => Task_fail(ex) } { a =>
				try {
					f(a)
				} catch {
					case NonFatal(e) => Task_fail(e)
				}
			}
		}

		@targetName("flatMapGuardedCapturer")
		override def flatMapGuarded[B](f: A => LatchingTask[B]): LatchingTask[B] = {
			checkWithin()
			state.fold[LatchingTask[B]] {
				new DefaultCaptor[B] with MonoObserver[A] {
					thisCaptor.triggerSync(this)

					override def onSuccess(a: A): Unit = {
						val next = try Maybe(f(a)) catch {
							case NonFatal(e) =>
								breakSync(e)
								Maybe.empty
						}
						next.foreach(_.subscribeSync(new MonoObserver[B] {
							override def onSuccess(b: B): Unit = fulfillSync(b)

							override def onError(ex: Throwable): Unit = breakSync(ex)
						}))
					}

					override def onError(ex: Throwable): Unit = breakSync(ex)
				}
			} { ex => new Failed(ex) } { a =>
				try {
					f(a)
				} catch {
					case NonFatal(e) => new Failed(e)
				}
			}
		}

		@targetName("flatMapGuardedTask")
		override def flatMapGuarded[B](f: A => Task[B]): Task[B] = {
			checkWithin()
			state.fold[Task[B]] {
				new DefaultCaptor_FlatMap[A, B](this, f, true)
			} { ex => Task_fail(ex) } { a =>
				try {
					f(a)
				} catch {
					case NonFatal(e) => Task_fail(e)
				}
			}
		}

		override def transform[B](f: Try[A] => Try[B]): LatchingTask[B] = {
			checkWithin()
			state.fold[LatchingTask[B]] {
				new DefaultCaptor[B] with MonoObserver[A] {
					thisCaptor.triggerSync(this)

					override def onSuccess(a: A): Unit = {
						// try {
						f(Success(a)) match {
							case Success(b) => fulfillSync(b)
							case Failure(e) => breakSync(e)
						}
						// } catch {
						//	case NonFatal(e) => breakSync(e)
						// }
					}

					override def onError(ex: Throwable): Unit = {
						// try {
						f(Failure(ex)) match {
							case Success(b) => fulfillSync(b)
							case Failure(e) => breakSync(e)
						}
						//} catch {
						//	case NonFatal(e) => breakSync(e)
						//}
					}
				}
			} { ex =>
				//try {
				f(Failure(ex)) match {
					case Success(b) => ReadyTask(b)
					case Failure(e) => new Failed(e)
				}
				//} catch {
				//	case NonFatal(e) => new Failed(e)
				//}
			} { a =>
				//try {
				f(Success(a)) match {
					case Success(b) => ReadyTask(b)
					case Failure(e) => new Failed(e)
				}
				//} catch {
				//	case NonFatal(e) => new Failed(e)
				//}
			}
		}

		override def transformWith[B](f: Try[A] => Observable[B]): Observable[B] = {
			checkWithin()
			state.fold[Observable[B]] {
				new DefaultCaptor_TransformWith[A, B](this, f)
			} { ex =>
				/*try*/ f(Failure(ex)) /*catch {
					case NonFatal(e) => new Failed(e)
				}*/
			} { a =>
				/*try*/ f(Success(a)) /*catch {
					case NonFatal(e) => new Failed(e)
				}*/
			}
		}

		@targetName("transformWithCapturer")
		override def transformWith[B](f: Try[A] => LatchingTask[B]): LatchingTask[B] = {
			checkWithin()
			state.fold[LatchingTask[B]] {
				new DefaultCaptor[B] with MonoObserver[A] {
					thisCaptor.subscribeSync(this)

					override def onSuccess(a: A): Unit = handle(Success(a))

					override def onError(ex: Throwable): Unit = handle(Failure(ex))

					private def handle(tryA: Try[A]): Unit = {
						f(tryA).triggerSync(new MonoObserver[B] {
							override def onSuccess(b: B): Unit = fulfillSync(b)

							override def onError(ex: Throwable): Unit = breakSync(ex)
						})
					}
				}
			} { ex =>
				// try {
				f(Failure(ex))
				//} catch {
				//	case NonFatal(e) => new Failed(e)
				//}
			} { a =>
				//try {
				f(Success(a))
				//} catch {
				//	case NonFatal(e) => new Failed(e)
				//}
			}
		}

		@targetName("transformWithTask")
		override def transformWith[B](f: Try[A] => Task[B]): Task[B] = {
			checkWithin()
			state.fold[Task[B]] {
				new DefaultCaptor_TransformWith[A, B](this, f)
			} { ex =>
				f(Failure(ex))
			} { a =>
				f(Success(a))
			}
		}

		override def recover[B >: A](pf: Throwable => Maybe[B]): LatchingTask[B] = {
			checkWithin()
			state.fold[LatchingTask[B]] {
				new DefaultCaptor[B] with MonoObserver[A] {
					thisCaptor.triggerSync(this)

					override def onSuccess(a: A): Unit = fulfillSync(a)

					override def onError(ex: Throwable): Unit = {
						// try {
						pf(ex).fold(breakSync(ex))(b => fulfillSync(b))
						//} catch {
						//	case NonFatal(e) => breakSync(e)
						//}
					}
				}
			} { ex =>
				/*try*/ pf(ex).fold[LatchingTask[B]](thisCaptor)(ReadyTask) /*catch {
					case NonFatal(e) => new Failed(e)
				}*/
			} { a => thisCaptor }
		}

		override def recoverWith[B >: A](pf: Throwable => Maybe[Observable[B]]): Observable[B] = {
			checkWithin()
			state.fold[Observable[B]] {
				new DefaultCaptor_RecoverWith[A, B](thisCaptor, pf)
			} { ex =>
				/*try*/ pf(ex).fold[Observable[B]](new Failed(ex))(identity) /*catch {
					case NonFatal(e) => new Failed(e)
				}*/
			} { a => Task_ready(a) }
		}

		@targetName("recoverWithCapturer")
		override def recoverWith[B >: A](pf: Throwable => Maybe[LatchingTask[B]]): LatchingTask[B] = {
			checkWithin()
			state.fold[LatchingTask[B]] {
				new DefaultCaptor[B] with MonoObserver[A] {
					thisCaptor.triggerSync(this)

					override def onSuccess(a: A): Unit = fulfillSync(a)

					override def onError(ex: Throwable): Unit = {
						pf(ex).fold {
							breakSync(ex)
						} { mb =>
							mb.subscribeSync(new MonoObserver[B] {
								override def onSuccess(b: B): Unit = fulfillSync(b)

								override def onError(e: Throwable): Unit = breakSync(e)
							})
						}
					}
				}
			} { ex =>
				pf(ex).fold[LatchingTask[B]](thisCaptor)(identity)
			} { a => thisCaptor }
		}

		@targetName("recoverWithTask")
		override def recoverWith[B >: A](pf: Throwable => Maybe[Task[B]]): Task[B] = {
			checkWithin()
			state.fold[Task[B]] {
				new DefaultCaptor_RecoverWith[A, B](thisCaptor, pf)
			} { ex =>
				pf(ex).fold[Task[B]](Task_fail(ex))(identity)
			} { a =>
				Task_ready(a)
			}
		}

		override def toFuture(isWithinDoSerEx: Boolean): Future[A] = {
			state.fold {
				val promise = Promise[A]()
				subscribe(isWithinDoSerEx)(new MonoObserver[A] {
					override def onSuccess(a: A): Unit = promise.success(a)

					override def onError(e: Throwable): Unit = promise.failure(e)
				})
				promise.future
			} { ex => Future.failed(ex) } { a => Future.successful(a) }
		}

		override def onBehalfOf(otherDoer: Doer): otherDoer.LatchingTask[A] = {
			state.fold {
				otherDoer.LatchingTask_from(thisDoer)(this)
			} { ex => otherDoer.Failed(ex) } { a => new otherDoer.ReadyTask(a) }
		}
	}

	/** A [[LatchingTask]] that captures a value derived from an observed result. */
	abstract class ChainedCapturer[-A, +B] extends DefaultCapturer[B], MonoObserver[A]

	/** A [[LatchingTask]] that captures the first value it observes. */
	abstract class DirectlyChainedCaptor[A] extends ChainedCapturer[A, A] {
		private var theState: Trial[A] = Trial.empty

		override protected def state: Trial[A] = theState

		override def onSuccess(a: A): Unit = theState = Trial.success(a)

		override def onError(e: Throwable): Unit = theState = Trial.failure(e)
	}

	//// Capturer factory methods ////

	/** Creates an already completed [[LatchingTask]].
	 * @param immediateResult the immediate result that this [[LatchingTask]] yields. */
	inline def LatchingTask_ready[A](immediateResult: A): ReadyTask[A] =
		new ReadyTask(immediateResult)

	inline def LatchingTask_failed(error: Throwable): Failed = new Failed(error)

	/** An already completed [[LatchingTask]] that yields [[Unit]].
	 * CAUTION: This @threadUnsafe lazy val does not guarantee a unique instance under concurrent access. Its use is only safe for logic that depends on the value's data, not its object identity (eq/ne). */
	@threadUnsafe lazy final val LatchingTask_unit: ReadyTask[Unit] = ReadyTask(())

	/** An already completed [[LatchingTask]] that yields `true`.
	 * CAUTION: This @threadUnsafe lazy val does not guarantee a unique instance under concurrent access. Its use is only safe for logic that depends on the value's data, not its object identity (eq/ne). */
	@threadUnsafe lazy final val LatchingTask_true: ReadyTask[Boolean] = ReadyTask(true)

	/** An already completed [[LatchingTask]] that yields `false`.
	 * CAUTION: This @threadUnsafe lazy val does not guarantee a unique instance under concurrent access. Its use is only safe for logic that depends on the value's data, not its object identity (eq/ne). */
	@threadUnsafe lazy final val LatchingTask_false: ReadyTask[Boolean] = ReadyTask(false)

	def LatchingTask_from[A](mono: Observable[A]): LatchingTask[A] = {
		mono match {
			case capturer: LatchingTask[A] => capturer
			case _ =>
				val covenant = new Covenant[A]() // TODO optimize
				mono.subscribeSync(new MonoObserver[A] {
					override def onSuccess(a: A): Unit = covenant.fulfillSync(a)

					override def onError(e: Throwable): Unit = covenant.breakSync(e)
				})
				covenant
		}
	}

	def LatchingTask_apply[A](supplier: () => A): LatchingTask[A] = {
		val captor = new Covenant[A]() // TODO optimize
		run(captor.fulfill(supplier()))
		captor
	}

	def LatchingTask_defer[A](supplier: () => LatchingTask[A]): LatchingTask[A] = {
		val captor = new Covenant[A]() // TODO optimize
		run {
			supplier().subscribeSync(new MonoObserver[A] {
				override def onSuccess(a: A): Unit = captor.fulfillSync(a)

				override def onError(e: Throwable): Unit = captor.breakSync(e)
			})
		}
		captor
	}

	/** Creates a [[LatchingTask]] that subscribes to an [[Observable]] of another [[Doer]].
	 * $threadSafe
	 *
	 * @param foreignDoer the [[Doer]] to whom the `foreignMono` belongs.
	 * @param foreignMono the [[Observable]] to subscribe to. Its result will be memorized and yielded by the returned [[LatchingTask]] in sequence with this [[Doer]].
	 * @return a [[Task]] that produces what the `foreignMono` produces, but the result is yielded in sequence with this [[Doer]]. */
	def LatchingTask_from[A](foreignDoer: Doer)(foreignMono: foreignDoer.Observable[A]): LatchingTask[A] = {
		if foreignDoer ne thisDoer then {
			val covenant = new Covenant[A]() // TODO optimize
			foreignDoer.run {
				foreignMono.subscribeSync(new foreignDoer.MonoObserver[A] {
					override def onSuccess(a: A): Unit = covenant.fulfill(a, false)

					override def onError(e: Throwable): Unit = covenant.break(e, false)
				})
			}
			covenant
		} else foreignMono match {
			case ft: foreignDoer.LatchingTask[A] @unchecked => ft.asInstanceOf[LatchingTask[A]]
			case _ => LatchingTask_from(foreignMono.asInstanceOf[Observable[A]])
		}
	}

	/** Create a [[LatchingTask]] whose result will be the result of the provided [[Future]] when it completes.\
	 * Useful to start a process in this [[Doer]]'s DoSerEx derived from a value produced by other process.\
	 * $threadSafe
	 * @param future the future to wait for.
	 * @return the [[LatchingTask]] described in the method description. */
	final def LatchingTask_from[A](future: Future[A]): LatchingTask[A] = {
		new DefaultCaptor[A] with (Try[A] => Unit) {
			future.onComplete(this)(using ownSingleThreadExecutionContext)

			override def apply(tryA: Try[A]): Unit = tryA match {
				case Success(a) => fulfillSync(a)
				case Failure(e) => breakSync(e)
			}
		}
	}

	/** Creates a [[LatchingTask]] whose result will be the result of the [[Future]] returned by the provided supplier.\
	 * Useful to start a process in an alien executor and continue it within this [[Doer]]'s DoSerEx.\
	 * The alien executor may be the $DoSerEx of this [[Doer]].\
	 * $threadSafe
	 * @param supplier a function that starts the process and return a [[Future]] of its result. $isExecutedByDoSerEx $unhandledErrorsArePropagatedToVentureResult
	 * @return the [[LatchingTask]] described in the method description. */
	final def LatchingTask_from[A](supplier: () => Future[A]): LatchingTask[A] = {
		new DefaultCaptor[A] with (Try[A] => Unit) {
			run(supplier().onComplete(this)(using ownSingleThreadExecutionContext))

			override def apply(tryA: Try[A]): Unit = tryA match {
				case Success(a) => fulfillSync(a)
				case Failure(e) => breakSync(e)
			}
		}
	}

	inline def LatchingTask_sequenceToArray[A: ClassTag, C[x] <: Iterable[x]](monos: C[Observable[A]], inline isWithinDoSerEx: Boolean = isInSequence): LatchingTask[Array[A]] = {
		Covenant_triggerAndWire(Task_sequenceToArray(monos)) // TODO optimize
	}

	/** Like [[Task_sequenceHardyToArray]] but eager (instead of lazy). */
	inline def LatchingTask_sequenceVenturesToArray[A: ClassTag, C[x] <: Iterable[x]](ventures: C[Observable[A]], inline isWithinDoSerEx: Boolean = isInSequence): LatchingTask[Array[Try[A]]] =
		Covenant_triggerAndWire(Task_sequenceHardyToArray(ventures), isWithinDoSerEx)

	//// Keeper ////

	/** Creates a [[ReadyTask]] that yields the provided value.
	 * @note Also suppresses the generation of the synthetic companion object. */
	inline final def ReadyTask[A](a: A): ReadyTask[A] = new ReadyTask(a)

	/** A [[LatchingTask]] that is fulfilled since its inception.
	 * TODO rename to Keeper */
	final class ReadyTask[+A](val value: A) extends LatchingTask[A] { thisKeeper =>

		override def subscribeSync(downChainObserver: MonoObserver[A]): Subscription = {
			// Returns empty subscription since it completes synchronously
			downChainObserver.onSuccess(value)
			Subscription_empty
		}

		override def triggerSync(downChainObserver: MonoObserver[A]): Unit = downChainObserver.onSuccess(value)

		override val maybeResult: Trial[A] = Trial.success(value)

		override def foreach(consumer: A => Unit): Unit = {
			checkWithin()
			consumer(value)
		}

		override def reconcile: ReadyTask[Try[A]] = ReadyTask(Success(value))

		override def withFilter(predicate: A => Boolean): LatchingTask[A] = {
			if predicate(value) then thisKeeper
			else Failed(new NoSuchElementException)
		}

		override def withFilterGuarded(predicate: A => Boolean): LatchingTask[A] = {
			try {
				if predicate(value) then thisKeeper
				else Failed(new NoSuchElementException)
			} catch {
				case NonFatal(e) => Failed(e)
			}
		}

		override def map[B](f: A => B): ReadyTask[B] = {
			checkWithin()
			ReadyTask(f(value))
		}

		override def mapGuarded[B](f: A => B): LatchingTask[B] = {
			checkWithin()
			try ReadyTask(f(value)) catch {
				case NonFatal(e) => Failed(e)
			}
		}

		@targetName("flatMapCapturer")
		override def flatMap[B](f: A => LatchingTask[B]): LatchingTask[B] = {
			checkWithin()
			f(value)
		}

		@targetName("flatMapTask")
		override def flatMap[B](f: A => Task[B]): Task[B] = {
			checkWithin()
			f(value)
		}

		override def flatMap[B](f: A => Observable[B]): Observable[B] = {
			checkWithin()
			f(value)
		}

		override def flatMapGuarded[B](f: A => Observable[B]): Observable[B] = {
			checkWithin()
			try f(value) catch {
				case NonFatal(e) => Failed(e)
			}
		}

		@targetName("flatMapGuardedCapturer")
		override def flatMapGuarded[B](f: A => LatchingTask[B]): LatchingTask[B] = {
			checkWithin()
			try f(value) catch {
				case NonFatal(e) => Failed(e)
			}
		}

		@targetName("flatMapGuardedTask")
		override def flatMapGuarded[B](f: A => Task[B]): Task[B] = {
			checkWithin()
			try f(value) catch {
				case NonFatal(e) => Task_fail(e)
			}
		}

		override def transform[B](f: Try[A] => Try[B]): LatchingTask[B] = {
			checkWithin()
			f(Success(value)) match {
				case Success(b) => ReadyTask(b)
				case Failure(e) => Failed(e)
			}
		}

		override def transformWith[B](f: Try[A] => Observable[B]): Observable[B] = {
			checkWithin()
			f(Success(value))
		}

		@targetName("transformWithCapturer")
		override def transformWith[B](f: Try[A] => LatchingTask[B]): LatchingTask[B] = {
			checkWithin()
			f(Success(value))
		}

		@targetName("transformWithTask")
		override def transformWith[B](f: Try[A] => Task[B]): Task[B] = {
			checkWithin()
			f(Success(value))
		}

		override def recover[B >: A](pf: Throwable => Maybe[B]): ReadyTask[B] = thisKeeper

		override def recoverWith[B >: A](pf: Throwable => Maybe[Observable[B]]): ReadyTask[B] = thisKeeper

		@targetName("recoverWithCapturer")
		override def recoverWith[B >: A](pf: Throwable => Maybe[LatchingTask[B]]): LatchingTask[B] = thisKeeper

		@targetName("recoverWithTask")
		override def recoverWith[B >: A](pf: Throwable => Maybe[Task[B]]): Task[B] = Task_ready(value)

		override def toFuture(isWithinDoSerEx: Boolean = isInSequence): Future[A] = Future.successful(value)

		override def onBehalfOf(otherDoer: Doer): otherDoer.ReadyTask[A] = new otherDoer.ReadyTask(value)

		override def toString: String = deriveToString[ReadyTask[A]](thisKeeper)
	}

	//// Failed ////

	/** Creates a [[Failed]] that yields the provided value.
	 * @note Also suppresses the generation of the synthetic companion object. */
	inline final def Failed(exception: Throwable): Failed = new Failed(exception)

	final class Failed(val exception: Throwable) extends LatchingTask[Nothing] { thisFailed =>
		override def maybeResult: Trial[Nothing] = Trial.failure(exception)

		override def subscribeSync(downChainObserver: MonoObserver[Nothing]): Subscription = {
			downChainObserver.onError(exception)
			Subscription_empty
		}

		override def triggerSync(downChainObserver: MonoObserver[Nothing]): Unit = downChainObserver.onError(exception)

		override def foreach(consumer: Nothing => Unit): Unit = ()

		override def reconcile: LatchingTask[Try[Nothing]] = ReadyTask(Failure(exception))

		override def withFilter(predicate: Nothing => Boolean): Failed = thisFailed

		override def withFilterGuarded(predicate: Nothing => Boolean): Failed = thisFailed

		override def map[B](f: Nothing => B): Failed = thisFailed

		override def mapGuarded[B](f: Nothing => B): Failed = thisFailed

		override def flatMap[B](f: Nothing => Observable[B]): Failed = thisFailed

		@targetName("flatMapCapturer")
		override def flatMap[B](f: Nothing => LatchingTask[B]): Failed = thisFailed

		@targetName("flatMapTask")
		override def flatMap[B](f: Nothing => Task[B]): Task[B] = Task_fail(exception)

		override def flatMapGuarded[B](f: Nothing => Observable[B]): Failed = thisFailed

		@targetName("flatMapGuardedCapturer")
		override def flatMapGuarded[B](f: Nothing => LatchingTask[B]): Failed = thisFailed

		@targetName("flatMapGuardedTask")
		override def flatMapGuarded[B](f: Nothing => Task[B]): Task[B] = Task_fail(exception)

		// override def reconcile: LatchingTask[Try[Nothing]] = ReadyTask(Failure(exception))

		override def transform[B](f: Try[Nothing] => Try[B]): LatchingTask[B] = {
			checkWithin()
			f(Failure(exception)) match {
				case Success(b) => ReadyTask(b)
				case Failure(ex) => new Failed(ex)
			}
		}

		override def transformWith[B](f: Try[Nothing] => Observable[B]): Observable[B] = {
			checkWithin()
			f(Failure(exception))
		}

		@targetName("transformWithCapturer")
		override def transformWith[B](f: Try[Nothing] => LatchingTask[B]): LatchingTask[B] = {
			checkWithin()
			f(Failure(exception))
		}

		@targetName("transformWithTask")
		override def transformWith[B](f: Try[Nothing] => Task[B]): Task[B] = {
			checkWithin()
			f(Failure(exception))
		}

		override def recover[B >: Nothing](pf: Throwable => Maybe[B]): LatchingTask[B] = {
			checkWithin()
			pf(exception).fold(thisFailed)(ReadyTask)
		}

		override def recoverWith[B >: Nothing](pf: Throwable => Maybe[Observable[B]]): Observable[B] = {
			checkWithin()
			pf(exception).fold(thisFailed)(identity)
		}

		@targetName("recoverWithCapturer")
		override def recoverWith[B >: Nothing](pf: Throwable => Maybe[LatchingTask[B]]): LatchingTask[B] = {
			checkWithin()
			pf(exception).fold(thisFailed)(identity)
		}

		@targetName("recoverWithTask")
		override def recoverWith[B >: Nothing](pf: Throwable => Maybe[Task[B]]): Task[B] = {
			checkWithin()
			pf(exception).fold(Task_fail(exception))(identity)
		}

		override def toFuture(isWithinDoSerEx: Boolean): Future[Nothing] = Future.failed(exception)

		override def onBehalfOf(otherDoer: Doer): otherDoer.Failed = otherDoer.Failed(exception)

		override def toString: String = deriveToString[Failed](thisFailed)
	}

	//// CAPTOR /////

	/** A non-instantiable complete implementation of [[LatchingTask]]. */
	abstract class DefaultCaptor[A](initialState: Trial[A] = Trial.empty) extends DefaultCapturer[A] { thisCaptor =>
		protected var theState: Trial[A] = initialState

		override protected def state: Trial[A] = theState

		/** Sets this [[DefaultCaptor]] captured value with the given successful `result`, unless it has already been set.
		 *
		 * If this [[DefaultCaptor]] captured value is not yet set, the provided `result` becomes its final value and is made immediately visible to all subscribers.
		 * If it is already set, the provided `result` is ignored.
		 *
		 * CAUTION: This method must be called within this [[Doer]].
		 * CAUTION: Deep synchronous chains of [[flatMap]] over immediately-fulfilled [[LatchingTask]] instances during ongoing fulfillment can form a synchronous recursion (fulfill → subscribe-immediate → fulfill → …) that overflows the stack. This could be avoided in the library but is not worth. The user can prevent it easily with the help of [[Doer.currentExecutionSerial]].
		 *
		 * TODO rename to `captureSuccessSync`
		 * @param result the value to set this [[DefaultCaptor]] capture value with.
		 * @param completionObserver optional synchronous observer of the actual captured value and information about its origin:
		 *   - [[THE_PROVIDED]] if the captured value was set by this method call with the provided value;
		 *   - [[ANOTHER_BEFORE]] if the captured value was already set when this method was called. */
		final def fulfillSync(result: A, completionObserver: CompletionObserver[A] = CompletionIgnorer): this.type = {
			theState.fold {
				theState = Trial.success(result)
				foreachTarget(_.onSuccess(result))
				clearRegistry()
				completionObserver.onSuccess(result, THE_PROVIDED)
			} { ex =>
				completionObserver.onError(ex, ANOTHER_BEFORE)
				// Ignore or do nothing if failed
			} { previousResult =>
				completionObserver.onSuccess(previousResult, ANOTHER_BEFORE)
			}
			this
		}

		/** Sets this [[DefaultCaptor]] captured value with the given failure `excuse`, unless it has already been set.
		 *
		 * If this [[DefaultCaptor]] is not yet fulfilled, the provided `result` becomes its final value and is made immediately visible to all subscribers.
		 * If it is already fulfilled, the provided `result` is ignored.
		 *
		 * CAUTION: This method must be called within this [[Doer]].
		 * CAUTION: Deep synchronous chains of [[flatMap]] over immediately-fulfilled [[LatchingTask]] instances during ongoing fulfillment can form a synchronous recursion (fulfill → subscribe-immediate → fulfill → …) that overflows the stack. // TODO consider the trampoline solutions discussed with copilot in the session "causal anchoring dilema", near the end.
		 *
		 * TODO rename to `captureFailureSync`
		 * @param excuse the value to fulfill this [[DefaultCaptor]] with.
		 * @param completionObserver optional synchronous observer of the actual captured value and information about its origin:
		 *   - [[THE_PROVIDED]] if the captured value was set by this method call with the provided value;
		 *   - [[ANOTHER_BEFORE]] if the captured value was already set when this method was called. */
		final def breakSync(excuse: Throwable, completionObserver: CompletionObserver[A] = CompletionIgnorer): this.type = {
			theState.fold {
				theState = Trial.failure(excuse)
				foreachTarget(_.onError(excuse))
				clearRegistry()
				completionObserver.onError(excuse, THE_PROVIDED)
			} { ex =>
				completionObserver.onError(ex, ANOTHER_BEFORE)
			} { a =>
				completionObserver.onSuccess(a, ANOTHER_BEFORE)
			}
			this
		}

		def fulfillWithSync(completingMono: Observable[A], completionObserver: CompletionObserver[A] = CompletionIgnorer): this.type = {
			if completingMono eq this then throw IllegalArgumentException("A Covenant can't be fulfilled with itself.")
			state.fold {
				completingMono.triggerSync(new MonoObserver[A] {
					override def onSuccess(result: A): Unit = {
						state.fold {
							theState = Trial.success(result)
							foreachTarget(_.onSuccess(result))
							clearRegistry()
							completionObserver.onSuccess(result, THE_PROVIDED)
						} { ex =>
							completionObserver.onError(ex, ANOTHER_AFTER)
						} { a1 =>
							completionObserver.onSuccess(a1, ANOTHER_AFTER)
						}
					}

					override def onError(ex: Throwable): Unit = {
						state.fold {
							theState = Trial.failure(ex)
							foreachTarget(_.onError(ex))
							clearRegistry()
							completionObserver.onError(ex, THE_PROVIDED)
						} { prevEx =>
							completionObserver.onError(prevEx, ANOTHER_AFTER)
						} { a1 =>
							completionObserver.onSuccess(a1, ANOTHER_AFTER)
						}
					}
				})
			} { ex =>
				completionObserver.onError(ex, ANOTHER_BEFORE)
			} { result =>
				completionObserver.onSuccess(result, ANOTHER_BEFORE)
			}
			this
		}
	}

	/** A [[LatchingTask]] with dynamic control of its completion (the execution of the subscribed consumers).
	 *
	 * It exposes methods such as [[fulfill]] and [[fulfillWith]] to allow external code to complete it.
	 *
	 * [[Covenant]] is to [[LatchingTask]] as [[scala.concurrent.Promise]] is to [[scala.concurrent.Future]] */
	final class Covenant[A](initialState: Trial[A] = Trial.empty) extends DefaultCaptor[A](initialState) {

		/** @param completionObserver optional synchronous observer of the actual completion result and origin. The `originId` parameter indicates the [[ImmediateResultOrigin]]. */
		inline def fulfill(result: A, inline isWithinDoSerEx: Boolean = isInSequence, completionObserver: CompletionObserver[A] = CompletionIgnorer): this.type = {
			if isWithinDoSerEx then fulfillSync(result, completionObserver)
			else {
				run(fulfillSync(result, completionObserver))
				this
			}
		}

		/** @param completionObserver optional synchronous observer of the actual completion result and origin. The `originId` parameter indicates the [[ImmediateResultOrigin]]. */
		inline def break(excuse: Throwable, inline isWithinDoSerEx: Boolean = isInSequence, completionObserver: CompletionObserver[A] = CompletionIgnorer): this.type = {
			if isWithinDoSerEx then breakSync(excuse, completionObserver)
			else {
				run(breakSync(excuse, completionObserver))
				this
			}
		}

		/** @param completionObserver optional synchronous observer of the actual completion result and origin. The `originId` parameter indicates the [[ImmediateResultOrigin]]. */
		def completeSync(result: Try[A], completionObserver: CompletionObserver[A] = CompletionIgnorer): this.type = {
			result match {
				case Success(a) =>
					fulfillSync(a, completionObserver)
				case Failure(ex) =>
					breakSync(ex, completionObserver)
			}
		}

		/** @param completionObserver optional synchronous observer of the actual completion result and origin. The `originId` parameter indicates the [[ImmediateResultOrigin]]. */
		inline def complete(result: Try[A], inline isWithinDoSerEx: Boolean = isInSequence, completionObserver: CompletionObserver[A] = CompletionIgnorer): this.type = {
			if isWithinDoSerEx then {
				checkWithin()
				completeSync(result, completionObserver)
			} else {
				run(completeSync(result, completionObserver))
				this
			}
		}

		/** @param completionObserver optional observer of the actual completion result and origin. The `originId` parameter indicates the [[ResultOrigin]]. */
		inline def fulfillWith(completingMono: Observable[A], inline isWithinDoSerEx: Boolean = isInSequence, completionObserver: CompletionObserver[A] = CompletionIgnorer): this.type = {
			if isWithinDoSerEx then fulfillWithSync(completingMono, completionObserver)
			else {
				if completingMono eq this then throw IllegalArgumentException("A Covenant can't be fulfilled with itself.")
				run(fulfillWithSync(completingMono, completionObserver))
				this
			}
		}

		override def toString(): String = {
			s"Captor(state=$state, isRegistryEmpty=$isRegistryEmpty)"
		}
	}

	/** TODO This class is very similar to [[Task_FlatMap]]. Consider removing duplication by extending a common super class. */
	final class DefaultCaptor_FlatMap[+A, B](upChainMono: Observable[A], f: A => Observable[B], isGuarded: Boolean) extends AbstractTask[B] {
		private var monoBMemory: Maybe[Observable[B]] = Maybe.empty

		override def subscribeSync(downChainObserver: MonoObserver[B]): Subscription = {
			new Subscription with MonoObserver[A] {
				private var isActive = true
				private var maybeUpChainSubscription: Maybe[Subscription] = Maybe.empty
				private var maybeInnerSubscription: Maybe[Subscription] = Maybe.empty

				{ // Constructor
					val upChainSubscription = upChainMono.subscribeSync(this)
					if isActive then maybeUpChainSubscription = Maybe(upChainSubscription)
				}

				override def onSuccess(a: A): Unit = {
					if isActive then {
						maybeUpChainSubscription = Maybe.empty
						monoBMemory.fold {
							val maybeInnerMonoB = if isGuarded then try Maybe(f(a)) catch {
								case NonFatal(e) =>
									isActive = false
									downChainObserver.onError(e)
									Maybe.empty
							} else Maybe(f(a))
							maybeInnerMonoB.foreach { mb =>
								monoBMemory = Maybe(mb)
								if isActive then {
									val innerSubscription = mb.subscribeSync(downChainObserver)
									if isActive then maybeInnerSubscription = Maybe(innerSubscription)
								}
							}
						} { mb =>
							val innerSubscription = mb.subscribeSync(downChainObserver)
							if isActive then maybeInnerSubscription = Maybe(innerSubscription)
						}
					}
				}

				override def onError(ex: Throwable): Unit = {
					if isActive then {
						isActive = false
						maybeUpChainSubscription = Maybe.empty
						downChainObserver.onError(ex)
					}
				}

				override def unsubscribe(): Unit = {
					if isActive then {
						isActive = false
						val mus = maybeUpChainSubscription
						val mis = maybeInnerSubscription
						maybeUpChainSubscription = Maybe.empty
						maybeInnerSubscription = Maybe.empty
						mus.foreach(_.unsubscribe())
						mis.foreach(_.unsubscribe())
					}
				}
			}
		}
	}

	final class DefaultCaptor_TransformWith[A, B](upChainMono: Observable[A], f: Try[A] => Observable[B]) extends AbstractTask[B] {
		private var monoBMemory: Maybe[Observable[B]] = Maybe.empty

		override def subscribeSync(downChainObserver: MonoObserver[B]): Subscription = {
			new Subscription with MonoObserver[A] {
				private var isActive = true
				private var maybeUpChainSubscription: Maybe[Subscription] = Maybe.empty
				private var maybeInnerSubscription: Maybe[Subscription] = Maybe.empty

				{ // Constructor
					val upChainSubscription = upChainMono.subscribeSync(this)
					if isActive then maybeUpChainSubscription = Maybe(upChainSubscription)
				}

				override def onSuccess(a: A): Unit = handle(Success(a))

				override def onError(ex: Throwable): Unit = handle(Failure(ex))

				private def handle(tryA: Try[A]): Unit = {
					if isActive then {
						maybeUpChainSubscription = Maybe.empty
						val monoB = monoBMemory.getOrElse {
							val monoB = f(tryA)
							monoBMemory = Maybe(monoB)
							monoB
						}
						if isActive then {
							val innerSubscription = monoB.subscribeSync(downChainObserver)
							if isActive then maybeInnerSubscription = Maybe(innerSubscription)
						}
					}
				}

				override def unsubscribe(): Unit = {
					if isActive then {
						isActive = false
						val mus = maybeUpChainSubscription
						val mis = maybeInnerSubscription
						maybeUpChainSubscription = Maybe.empty
						maybeInnerSubscription = Maybe.empty
						mus.foreach(_.unsubscribe())
						mis.foreach(_.unsubscribe())
					}
				}
			}
		}
	}

	final class DefaultCaptor_RecoverWith[A, B >: A](upChainMono: Observable[A], pf: Throwable => Maybe[Observable[B]]) extends AbstractTask[B] {
		private var maybeMonoBMemory: Maybe[Maybe[Observable[B]]] = Maybe.empty

		override def subscribeSync(downChainObserver: MonoObserver[B]): Subscription = {
			new Subscription with MonoObserver[A] {
				private var isActive = true
				private var maybeUpChainSubscription: Maybe[Subscription] = Maybe.empty
				private var maybeInnerSubscription: Maybe[Subscription] = Maybe.empty

				{ // Constructor
					val upChainSubscription = upChainMono.subscribeSync(this)
					if isActive then maybeUpChainSubscription = Maybe(upChainSubscription)
				}

				override def onSuccess(a: A): Unit = {
					if isActive then {
						maybeUpChainSubscription = Maybe.empty
						downChainObserver.onSuccess(a)
					}
				}

				override def onError(ex: Throwable): Unit = {
					if isActive then {
						maybeUpChainSubscription = Maybe.empty
						val maybeMonoB = maybeMonoBMemory.getOrElse {
							val maybeMonoB = pf(ex)
							maybeMonoBMemory = Maybe(maybeMonoB)
							maybeMonoB
						}
						if isActive then {
							maybeMonoB.fold {
								isActive = false
								downChainObserver.onError(ex)
							} { monoB =>
								if isActive then {
									val innerSubscription = monoB.subscribeSync(downChainObserver)
									if isActive then maybeInnerSubscription = Maybe(innerSubscription)
								}
							}
						}
					}
				}

				override def unsubscribe(): Unit = {
					if isActive then {
						isActive = false
						val mus = maybeUpChainSubscription
						val mis = maybeInnerSubscription
						maybeUpChainSubscription = Maybe.empty
						maybeInnerSubscription = Maybe.empty
						mus.foreach(_.unsubscribe())
						mis.foreach(_.unsubscribe())
					}
				}
			}
		}
	}

	//// COVENANT FACTORY METHODS ////

	/** Creates a new pending [[Covenant]] */
	inline def Covenant[A](): Covenant[A] =
		new Covenant()

	/** Creates a [[Covenant]] that will fulfill with the result of executing the provided supplier within the $DoSerEx.
	 * @param supplier a supplier function that is executed within the $DoSerEx and returns the value to fulfill the created [[Covenant]] with. */
	def Covenant_from[A](supplier: () => A): Covenant[A] = {
		val covenant = new Covenant[A] // TODO optimize
		run {
			covenant.fulfillSync(supplier())
		}
		covenant
	}

	/** Creates a [[Covenant]] that is wired to the [[LatchingTask]] resulting of executing the provided supplier within the $DoSerEx.
	 * @param supplier a supplier function that is executed within the $DoSerEx to return the [[LatchingTask]] to which the created [[Covenant]] is wired. */
	def Covenant_defer[A](supplier: () => LatchingTask[A]): Covenant[A] = {
		val covenant = new Covenant[A] // TODO optimize
		run {
			supplier().subscribeSync(new MonoObserver[A] {
				override def onSuccess(a: A): Unit = covenant.fulfillSync(a)

				override def onError(ex: Throwable): Unit = ()
			})
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
	 * @param completionObserver optional synchronous observer of the actual completion result and origin. The `originId` parameter indicates the [[ImmediateResultOrigin]].
	 * @return a [[Covenant]] that will be completed with the result of the execution triggered by this method.
	 */
	inline def Covenant_triggerAndWire[A](
		task: Task[A], inline isWithinDoSerEx: Boolean = isInSequence,
		completionObserver: CompletionObserver[A] = CompletionIgnorer,
	): Covenant[A] = {
		val covenant = new Covenant[A]() // TODO optimize
		task.subscribeCallbacks(isWithinDoSerEx)(a => covenant.fulfillSync(a, completionObserver), e => covenant.breakSync(e, completionObserver))
		covenant
	}



	///////////// VENTURE //////////////

	/** A hardy and short-circuiting version of [[Task]].\
	 * Advantages of [[Venture]] compared to [[Task]]:
	 *		- results are wrapped withing a [[Try]] which allows the support of failed results.
	 *		- the call to the routines received by the operations are guarded with a try-catch, which allows to propagate failures through [[Venture]] chains.
	 *		- can encapsulate a [[Future]] making interoperability with them easier.
	 * @param A the type of the result obtained when executing this [[Venture]]. */
	@deprecated
	type Venture[+A] = Task[A]
	@deprecated
	type LatchingVenture[+A] = LatchingTask[A]
	@deprecated
	type ReadyVenture[+A] = ReadyTask[A]
	@deprecated
	type Commitment[A] = Covenant[A]

	@deprecated
	inline def ReadyVenture[A](tryA: Try[A]): ReadyTask[A] = tryA match {
		case Success(a) => ReadyTask(a)
		case Failure(ex) => new Failed(ex).asInstanceOf[ReadyTask[A]]
	}

	@deprecated
	inline def Commitment[A](): Covenant[A] = new Covenant[A]()




	////////////// EVER ///////////////

	inline final def LatchingVenture[A](fixedResult: Maybe[Try[A]]): LatchingTask[A] =
		fixedResult.fold(new Covenant[A]())(tryA => LatchingVenture_ready(tryA))

	inline final def LatchingVenture_ready[A](immediateResult: Try[A]): LatchingTask[A] = immediateResult match {
		case Success(a) => ReadyTask(a)
		case Failure(ex) => new Failed(ex).asInstanceOf[LatchingTask[A]]
	}

	@threadUnsafe lazy final val LatchingVenture_unit: LatchingTask[Unit] = LatchingTask_unit
	@threadUnsafe lazy final val LatchingVenture_true: LatchingTask[Boolean] = LatchingTask_true
	@threadUnsafe lazy final val LatchingVenture_false: LatchingTask[Boolean] = LatchingTask_false

	inline def Commitment[A](fixedResult: Maybe[Try[A]]): Commitment[A] = {
		val c = new Covenant[A]()
		fixedResult.foreach(tryA => c.completeSync(tryA))
		c
	}

	def Commitment_own[A](supplier: () => Try[A]): Commitment[A] = {
		val commitment = new Commitment[A]()
		run(commitment.completeSync(supplier()))
		commitment
	}

	def Commitment_ownFlat[A](supplier: () => LatchingVenture[A]): Commitment[A] = {
		val commitment = new Commitment[A]()
		run(supplier().subscribeSync(new MonoObserver[A] {
			override def onSuccess(a: A): Unit = commitment.fulfillSync(a)

			override def onError(ex: Throwable): Unit = commitment.completeSync(Failure(ex))
		}))
		commitment
	}

	inline def Commitment_triggerAndWire[A](
		venture: Venture[A],
		inline isWithinDoSerEx: Boolean = isInSequence,
		onSuccess: (A, ResultOrigin) => Unit = (_: A, _: ResultOrigin) => (),
		onError: (Throwable, ResultOrigin) => Unit = (_, _) => ()
	): Commitment[A] = {
		val commitment = Commitment[A]()
		venture.subscribeCallbacks(isWithinDoSerEx)(a => commitment.fulfillSync(a), e => commitment.breakSync(e))
		commitment
	}


	//////////////// Flow //////////////////////

	def Flow_lift[A, B](f: A => B): Flow[A, B] =
		(a: A) => Task_ready(f(a))

	def Flow_wrap[A, B](builder: A => Task[B]): Flow[A, B] =
		(a: A) => builder(a)

	trait Flow[A, B] { thisFlow =>

		protected def flush(a: A): Task[B]

		inline def apply(a: A, inline isWithinDoSerEx: Boolean = isInSequence)(onSuccess: B => Unit, onError: Throwable => Unit): Unit = {
			def work(): Unit = flush(a).subscribeSyncCallbacks(onSuccess, onError)

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

	///////////////////////////////////////////
	//// Targets registry and broadcasting ////
	///////////////////////////////////////////


	trait TargetProxy[-A, +T[-_] <: AnyRef] {
		def target: T[A]
	}

	/** Convenient conjunction of [[TargetProxy]] and [[Subscription]] due to its ubiquity. */
	trait ObservingSubscription[-A, +T[-_] <: AnyRef] extends Subscription, TargetProxy[A, T]

	/** A registry of targets capable of applying any operation to all of them.\
	 * **Maintenance note:** None of the methods in [[Muxer]] can be public. This restriction ensures that subclasses can extend [[Muxer]] and bypass the variance limitations without exposing type-unsafe operations to external clients. */
	trait Muxer[A, T[-_] <: AnyRef] {
		type Target = T[A] | TargetProxy[A, T]

		private var maybeFirstTarget: Maybe[Target] = Maybe.empty
		private var maybeFollowingTargets: Maybe[Array[Target]] = Maybe.empty
		private var followingTargetsSize: Int = 0
		private var recursionDepth: Int = 0

		protected def addTarget(target: Target): Unit = {
			if maybeFirstTarget.isEmpty && followingTargetsSize == 0 then {
				maybeFirstTarget = Maybe(target)
			} else {
				maybeFollowingTargets.fold {
					val followingTargets = new Array[Target](8)
					followingTargets(0) = target
					followingTargetsSize = 1
					maybeFollowingTargets = Maybe(followingTargets)
				} { followingTargets =>
					val fts = followingTargetsSize
					val newFollowingTargets =
						if fts < followingTargets.length then followingTargets
						else {
							val expanded = new Array[Target](fts * 2)
							System.arraycopy(followingTargets, 0, expanded, 0, fts)
							maybeFollowingTargets = Maybe(expanded)
							expanded
						}
					newFollowingTargets(fts) = target
					followingTargetsSize = fts + 1
				}
			}
		}

		protected def removeAllMatching(target: Target): Int = {
			var removedCount = 0
			maybeFirstTarget = maybeFirstTarget.flatMap { firstTarget =>
				if firstTarget ne target then Maybe(firstTarget)
				else {
					removedCount += 1
					Maybe.empty
				}
			}
			maybeFollowingTargets.foreach { followingTargets =>
				var index = followingTargetsSize
				while index > 0 do {
					index -= 1
					if followingTargets(index) eq target then {
						removedCount += 1
						followingTargets(index) = null
					}
				}
			}
			if recursionDepth == 0 then removeHoles()

			removedCount
		}

		private def removeHoles(): Maybe[Array[Target]] = {

			if maybeFirstTarget.isEmpty then {
				maybeFirstTarget = maybeFollowingTargets.flatMap { followingTargets =>
					// Search for the first non-null and not matching entry in the array.
					var index = 0
					var targetAtIndex: Target | Null = null
					while index < followingTargetsSize && {
						targetAtIndex = followingTargets(index)
						(targetAtIndex eq null)
					} do index += 1
					// If none found then the registry is empty
					if index == followingTargetsSize then Maybe.empty
					// else, remove it from the array and make it be the first target
					else {
						followingTargets(index) = null
						Maybe(targetAtIndex)
					}
				}
			}

			maybeFollowingTargets.flatMap { followingTargets =>
				val initialSize = followingTargetsSize
				var insertIndex = 0
				var readIndex = 0
				while readIndex < initialSize do {
					val element = followingTargets(readIndex)
					if element ne null then {
						if insertIndex != readIndex then {
							followingTargets(insertIndex) = element
							followingTargets(readIndex) = null
						}
						insertIndex += 1
					}
					readIndex += 1
				}
				followingTargetsSize = insertIndex
				if insertIndex == 0 then Maybe.empty else Maybe(followingTargets)
			}
		}

		protected def countAllMatching(target: Target): Int = {
			var counter = 0
			maybeFirstTarget.foreach { firstTarget =>
				if firstTarget eq target then counter += 1
			}
			maybeFollowingTargets.foreach { followingTargets =>
				var index = followingTargetsSize
				while index > 0 do {
					index -= 1
					if followingTargets(index) eq target then counter += 1
				}
			}
			counter
		}

		protected inline def foreachTarget(inline consumer: T[A] => Unit): Unit = {
			recursionDepth += 1
			maybeFirstTarget.foreach {
				case proxy: TargetProxy[A, ?] @unchecked => consumer(proxy.target.asInstanceOf[T[A]])
				case direct: T[A] @unchecked => consumer(direct)
			}
			// CRITICAL: The cast is necessary to bypass an invalid Scala 3 compiler optimization during the inline expansion. Because Entry is a Union Type, its runtime allocation is a raw JVM Object array (Object[]). However, if an Observer implementation happens to extend a trait like java.io.Serializable, the Scala 3 compiler will try to optimize this inline closure by implicitly downcasting the entire array container to a Serializable[] array. Since an Object[] cannot be downcast to a Serializable[], the JVM explodes with a ClassCastException. Forcing an AnyRef array view strips away this aggressive optimization and keeps it as a safe, generic pointer array.
			maybeFollowingTargets.asInstanceOf[Maybe[IArray[AnyRef]]].foreach { followingTargets =>
				var i = 0
				val size = followingTargetsSize
				while i < size do {
					followingTargets(i) match {
						case null => // do nothing
						case proxy: TargetProxy[A, ?] @unchecked => consumer(proxy.target.asInstanceOf[T[A]])
						case direct: T[A] @unchecked => consumer(direct)
					}
					i += 1
				}
			}

			recursionDepth -= 1
			if recursionDepth == 0 then removeHoles()
		}

		protected def isRegistryEmpty: Boolean = maybeFirstTarget.isEmpty

		protected def clearRegistry(): Unit = {
			maybeFollowingTargets.foreach { followingTargets =>
				var index = followingTargetsSize
				while index > 0 do {
					index -= 1
					followingTargets(index) = null
				}
			}
			maybeFirstTarget = Maybe.empty
			maybeFollowingTargets = Maybe.empty
			followingTargetsSize = 0
		}
	}

}