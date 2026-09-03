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
trait DoerCorePart { thisDoer: Doer & DoerTaskOpsPart =>


	//// PRIMITIVES ////

	/** Modernized subscription handle returned upon subscribing to an asynchronous primitive.
	 * Added as part of the bi-convergent convergence plan to support safe cancellation.
	 * @note CAUTION: Must be called within the single-thread Execution Context of the owning Doer (DoSerEx). */
	trait Subscription {
		def unsubscribeSync(): Unit

		final def unsubscribe(): Unit = run(unsubscribeSync())
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

	trait MonoTransformer[-A, +B] {
		def mapSuccess(a: A): B

		def mapError(e: Throwable): B
	}

	/** A single result computation with observable result. */
	trait Mono[+A] { thisMono =>
		/** Subscribes an [[Observer]] to the result of this [[Mono]] and returns a [[Subscription]] that can be used to cancel.
		 * This method is the sole primitive operation of this trait; all other methods are derived from it.\
		 * @param downChainObserver The observer to be notified upon the completion of this [[Mono]]. The implementation should notify within the $DoSerEx.\
		 * The implementation may assume that `onComplete` will either terminate normally or fatally, but will not throw non-fatal exceptions. */
		def subscribeSync(downChainObserver: MonoObserver[A]): Subscription

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
							else upChainSubscription.unsubscribeSync()
						}
					}

					override def unsubscribeSync(): Unit = {
						checkWithin()
						isActive = false
						maybeUpChainSubscription.foreach(_.unsubscribeSync())
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
						else upChainSubscription.unsubscribeSync()
					}
				}

				override def onSuccess(a: A): Unit = onComplete(a)

				override def onError(e: Throwable): Unit = onComplete(e)

				override def unsubscribeSync(): Unit = {
					checkWithin()
					isActive = false
					maybeUpChainSubscription.foreach(_.unsubscribeSync())
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
		 * @param downChainObserver The observer to be notified upon the completion of this [[Mono]]. The implementation should notify within the $DoSerEx.\
		 * The implementation may assume that the [[MonoObserver]] methods either terminate normally or fatally, but will not throw non-fatal exceptions. */
		def triggerSync(downChainObserver: MonoObserver[A]): Unit = subscribeSync(downChainObserver)

		inline def triggerSyncCallbacks(inline success: A => Unit, inline error: Throwable => Unit): Unit = triggerSync(MonoObserver_fromCallbacks(success, error))

		/** Enqueues an uncancelable execution of this [[Mono]] ignoring the result .
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

		/** Enqueues an execution of this [[Mono]] ignoring the result.
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
		 * @param consumer called with this [[Mono]] result when it completes.
		 *
		 * $isExecutedByDoSerEx
		 *
		 * $notGuarded */
		def foreach(consumer: A => Unit): Unit

		/** Creates a new [[Mono]] that yields exactly the same result (same identity) as this [[Mono]] but executes the provided side-effecting function before yielding it.\
		 * $threadSafe
		 * @param onSuccess a function that is applied to the successful result of this [[Mono]] for its side effects before subscribers.
		 * @param onError a function that is applied to the failure result of this [[Mono]] for its side effects before subscribers. */
		def andThen(onSuccess: A => Unit, onError: Throwable => Unit = _ => ()): Mono[A]

		def andThen(monoObserver: MonoObserver[A]): Mono[A]

		def reconcile: Mono[Try[A]]

		def withFilter(p: A => Boolean): Mono[A]

		def withFilterGuarded(p: A => Boolean): Mono[A]

		/**
		 * Creates a new [[Mono]] that yields the result of applying the provided function to the result of this [[Mono]].
		 *
		 * $threadSafe
		 *
		 * @param f a function that is applied to the result of this [[Mono]] to produce the final result.
		 *
		 * $isExecutedByDoSerEx
		 *
		 * $notGuarded
		 */
		def map[B](f: A => B): Mono[B]

		def mapGuarded[B](f: A => B): Mono[B]

		/**
		 * Creates a new [[Mono]] that yields the result of executing an intermediate [[Mono]] produced by applying the provided function to the final result.
		 *
		 * $threadSafe
		 *
		 * @param f a function that is applied to the result of this [[Mono]] to produce an intermediate [[Mono]] that is then executed to produce the final result.
		 *
		 * $isExecutedByDoSerEx
		 *
		 * $notGuarded
		 */
		def flatMap[B](f: A => Mono[B]): Mono[B]

		def flatMapGuarded[B](f: A => Mono[B]): Mono[B]

		def transform[B](transformer: MonoTransformer[A, Try[B]]): Mono[B]

		def transformWith[B](transformer: MonoTransformer[A, Mono[B]]): Mono[B]
		
		def recover[B >: A](pf: Throwable => Maybe[B]): Mono[B]

		def recoverWith[B >: A](pf: Throwable => Maybe[Mono[B]]): Mono[B]

		def toFuture(isWithinDoSerEx: Boolean = isInSequence): Future[A]

		/**
		 * Wraps this [[Mono]] into another that belongs to another [[Doer]].\
		 * Useful to chain [[Mono]]'s operations that involve different [[Doer]] instances.\
		 * ===Detailed behavior===
		 * Returns a [[Mono]] that belongs to the provided [[Doer]]. When it is triggered, it will trigger this task within this [[Doer]] and, when completed, make the returned [[Mono]] to yield the result.\
		 * CAUTION: Avoid closing over the same mutable variable from two operand functions applied to [[Mono]] instances belonging to different [[Doer]]s.\
		 * Remember that all function operands provided to [[Mono]] methods are executed within the [[Doer]] that owns it. Therefore, calling [[triggerCallbacks]] on the returned [[Mono]] will execute the `onComplete` passed to it within the `otherDoer`.\
		 *
		 * $threadSafe
		 *
		 * @param otherDoer the [[Doer]] to which the returned [[Task]] will belong.
		 */
		def onBehalfOf(otherDoer: Doer): otherDoer.Mono[A]

		/** Casts the singleton type of the [[Doer]] instance that owns this [[Task]] to the singleton-type of the provided [[Doer]].
		 * This operation does nothing at runtime. It only tricks the compiler to prevent it from complaining when operating with references to the same [[Doer]] instance but through different type-paths.
		 * CAUTION: Use it only if you are sure that the provided [[Doer]] instance is the one that owns this [[Task]].
		 *
		 * Design note: It was decided to make [[Mono]] an inner class of the [[Doer]] to take advantage of type-path checking to detect when the contract "all operand functions passed to [[Mono]] operations owned by the same [[Doer]] are executed in sequence" might be violated, at compile time.
		 * Using type-path checking to detect contract violations is very valuable, but it comes at a cost, because the type-path check done by the compiler is stricter than necessary -- it checks that the singleton type of the references involved be compatible, and we only need to check that the involved [[Task]] instances belong to the same [[Doer]] instance.
		 * Therefore, the compiler will report type errors in situations the contract is not violated, which is not what we want.
		 * This operation ([[castTypePath()]]) is intended to handle those cases.
		 */
		def castTypePath[E <: Doer](doer: E): doer.Mono[A] = {
			assert(thisDoer eq doer)
			this.asInstanceOf[doer.Mono[A]]
		}

		def guarded: GuardedMono[A]
	}

	trait GuardedMono[+A] {
		def withFilter(p: A => Boolean): Mono[A]

		def map[B](f: A => B): Mono[B]

		def flatMap[B](f: A => Mono[B]): Mono[B]

		def recover[B >: A](pf: Throwable => Maybe[B]): Mono[B]

		def recoverWith[B >: A](pf: Throwable => Maybe[Mono[B]]): Mono[B]

		def transform[B](transformer: MonoTransformer[A, Try[B]]): Mono[B]

		def transformWith[B](transformer: MonoTransformer[A, Mono[B]]): Mono[B]
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
	 * If you require to ensure monadic laws are followed, use [[Capture]] instead.\
	 * Design note: [[Task]] and [[Capture]] are defined as inner traits of [[Doer]] to leverage Scala's path-dependent type checking. This avoids that [[Task]]/[[Capture]] instances that belong to different [[Doer]] instances to be inadvertently composed together without the adapters needed to ensure sequential execution of the component actions.\
	 * While path-dependent type checking is valuable for enforcing this contract, it has a drawback: the compiler's type-path checks are overly strict, requiring compatible singleton types for references, whereas we only need to verify that the [[Task]] instances correspond to the same [[Doer]].\
	 * As a result, the compiler may flag type errors in cases where the contract is not violated, which is undesirable.\
	 * To bypass these path-dependent restrictions when composing tasks across Doer boundaries (or when types cannot be fully proven stable by the compiler), see the trigger implementations in the macro definition, which projects types using the general projected type `Doer#Task`.\
	 * @tparam A the type of the result obtained when executing this [[Task]]. */
	trait Task[+A] extends Mono[A] { thisTask =>

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
		override def andThen(onSuccess: A => Unit, onError: Throwable => Unit = _ => ()): Task[A] = new Task_AndThen[A](thisTask, MonoObserver_fromCallbacks(onSuccess, onError))

		override def andThen(monoObserver: MonoObserver[A]): Task[A] = new Task_AndThen[A](thisTask, monoObserver)

		override def reconcile: Task[Try[A]] = new Task_Reconcile(thisTask)

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

		override def transform[B](transformer: MonoTransformer[A, Try[B]]): Task[B] = new Task_Transform(thisTask, transformer, false)

		/**
		 * Creates a new [[Task]] that yields the result of executing an intermediate [[Mono]] produced by applying the provided function to the final of this [[Task]].
		 *
		 * $threadSafe
		 *
		 * @param f a function that is applied to the result of this [[Task]] to produce an intermediate [[Task]] that is then executed to produce the final result.
		 *
		 * $isExecutedByDoSerEx
		 *
		 * $notGuarded
		 */
		override def flatMap[B](f: A => Mono[B]): Task[B] = new Task_FlatMap(thisTask, f, false)

		override def flatMapGuarded[B](f: A => Mono[B]): Task[B] = new Task_FlatMap(thisTask, f, true)

		override def recoverWith[B >: A](pf: Throwable => Maybe[Mono[B]]): Task[B] = new Task_RecoverWith(thisTask, pf, false)

		override def transformWith[B](transformer: MonoTransformer[A, Mono[B]]): Task[B] = new Task_TransformWith(thisTask, transformer, false)

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
		 * Remember that all functional operands passed to [[Mono]] methods are executed within the [[Doer]] that owns it.
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
		 * Design note: It was decided to make [[Mono]] an inner class of the [[Doer]] to take advantage of type-path checking to detect when the contract "all operand functions passed to [[Mono]] operations owned by the same [[Doer]] are executed in sequence" might be violated, at compile time.
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

		override def flatMap[B](f: A => Mono[B]): Task[B] = new Task_FlatMap(underlying, f, true)

		def recover[B >: A](pf: Throwable => Maybe[B]): Task[B] = new Task_Recover(underlying, pf, true)

		def recoverWith[B >: A](pf: Throwable => Maybe[Mono[B]]): Task[B] = new Task_RecoverWith(underlying, pf, true)

		def transform[B](transformer: MonoTransformer[A, Try[B]]): Task[B] = new Task_Transform(underlying, transformer, true)

		def transformWith[B](transformer: MonoTransformer[A, Mono[B]]): Task[B] = new Task_TransformWith(underlying, transformer, true)
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
	inline def Task_apply[A](supplier: () => A, isGuarded: Boolean = false): Task[A] = if isGuarded then new Task_ApplyGuarded(supplier) else new Task_Apply(supplier)

	/** Creates a [[Task]] that lazily executes the provided [[Mono]] supplier and yields whatever the produced [[Mono]] yields.
	 * Is equivalent to: {{{Task_apply(supplier).flatMap(identity)}}} but slightly more efficient
	 * ===Detailed behavior===
	 * Creates a task that, when executed:
	 *		- evaluates the `supplier` within the $DoSerEx;
	 *		- then triggers an execution of the returned [[Mono]];
	 *		- finally completes with the result of executed task.
	 *
	 * $$threadSafe
	 *
	 * @param supplier the supplier of the [[Mono]] whose execution will give the result. $isExecutedByDoSerEx $notGuarded
	 * @return the task described in the method description. */
	inline def Task_defers[A](supplier: () => Mono[A], isGuarded: Boolean = false): Task[A] = if isGuarded then new Task_DefersGuarded(supplier) else new Task_Defers(supplier)

	/** Adapts a [[Mono]] belonging to this [[Doer]] into a [[Task]].\
	 * If `mono` is already a [[Task]], it is returned directly without wrapping overhead, preserving its lazy, re-executable behavior.\
	 * If `mono` is a [[Capture]], the returned [[Task]] yields its single, memoized result whenever executed.\
	 * $threadSafe
	 * @param mono the [[Mono]] belonging to this [[Doer]] to adapt into a [[Task]].
	 * @return a [[Task]] representing the computation of `mono`. */
	def Task_from[A](mono: Mono[A]): Task[A] = {
		mono match {
			case task: Task[A] @unchecked => task
			case _ => new Task_FromMono[A](mono)
		}
	}

	/** Adapts a [[Mono]] belonging to another [[Doer]] into a [[Task]] sequenced on this [[Doer]].\
	 * When the returned [[Task]] is executed, subscription is dispatched sequentially to `foreignDoer`.\
	 * If `foreignMono` is a [[Task]], each execution of the returned [[Task]] triggers a new execution on `foreignDoer`.\
	 * If `foreignMono` is a [[Capture]], executions of the returned [[Task]] observe its single, memoized outcome.\
	 * When `foreignMono` completes (success or failure), its result is dispatched back into this [[Doer]]'s sequential timeline.\
	 * If unsubscription occurs on this [[Doer]], the unsubscription request is dispatched sequentially to `foreignDoer`.\
	 * If `foreignDoer` is identical to this [[Doer]], the mono is adapted directly without foreign bridging overhead.\
	 * $threadSafe
	 * @param foreignDoer the [[Doer]] that owns `foreignMono`.
	 * @param foreignMono the [[Mono]] to subscribe to when the returned [[Task]] is executed.
	 * @return a [[Task]] that yields the outcome of `foreignMono` within this [[Doer]]'s sequential timeline. */
	def Task_from[A](foreignDoer: Doer)(foreignMono: foreignDoer.Mono[A]): Task[A] = {
		if foreignDoer ne thisDoer then new Task_FromForeign[A](foreignDoer, foreignMono)
		else foreignMono match {
			case ft: foreignDoer.Task[A] @unchecked => ft.asInstanceOf[Task[A]]
			case _ => new Task_FromMono(foreignMono.asInstanceOf[Mono[A]])
		}
	}

	/** Adapts an existing, eagerly executing [[Future]] into a [[Task]] sequenced on this [[Doer]].\
	 * Because `future` is already running or completed, all executions of the returned [[Task]] await and observe the same single outcome rather than restarting the operation.\
	 * When `future` completes (success or failure), its result is dispatched back into this [[Doer]]'s sequential timeline.\
	 * $threadSafe
	 * @param future the [[Future]] whose completion will be awaited.
	 * @return a [[Task]] that yields the outcome of `future` within this [[Doer]]'s sequential timeline. */
	inline final def Task_from[A](future: Future[A]): Task[A] = new Task_FromFuture(future)

	/** Creates a lazy [[Task]] that invokes `supplier` upon each execution to start a new [[Future]].\
	 * Each execution of the returned [[Task]] evaluates `supplier` to trigger a fresh asynchronous computation.\
	 * When the produced [[Future]] completes (success or failure), its result is dispatched back into this [[Doer]]'s sequential timeline.\
	 * $threadSafe
	 * @param supplier a function evaluated within this [[Doer]]'s sequential timeline that starts an asynchronous process and returns its [[Future]]. $isExecutedByDoSerEx
	 * @param isGuarded determines whether non-fatal exceptions thrown by `supplier` are captured as task failures (`true`) or left unhandled (`false`).
	 * @return a [[Task]] that evaluates `supplier` and yields the outcome of the resulting [[Future]] within this [[Doer]]'s sequential timeline. */
	inline final def Task_from[A](supplier: () => Future[A], isGuarded: Boolean = false): Task[A] = new Task_FromFutureSupplier(supplier, isGuarded)


	/** Creates a [[Task]] that subscribes to two [[Mono]] instances and combines their results using a bifunction.\
	 * When the returned [[Task]] is executed, subscriptions to `monoA` and `monoB` are initiated.\
	 * Given the serial execution nature of [[Doer]], concurrency between the monos only occurs if one or both involve foreign or alien actions (such as [[Task_from]]).\
	 * If either mono fails, the overall [[Task]] immediately fails with that error and cancels the remaining active peer mono.\
	 * When both monos succeed, `f` is evaluated within this [[Doer]]'s sequential timeline to produce the final result.\
	 * $threadSafe
	 * @param monoA the first [[Mono]] to combine.
	 * @param monoB the second [[Mono]] to combine.
	 * @param isGuarded determines whether non-fatal exceptions thrown by `f` are captured as task failures (`true`) or left unhandled (`false`).
	 * @param f the function that combines the successful outcomes of `monoA` and `monoB`. $isExecutedByDoSerEx
	 * @return a [[Task]] that yields the combined result of `monoA` and `monoB`. */
	inline def Task_combine[A, B, C](monoA: Mono[A], monoB: Mono[B], isGuarded: Boolean = false)(f: (A, B) => C): Task[C] = new Task_Combined(monoA, monoB, f, isGuarded)

	/** Creates a [[Task]] that subscribes to each [[Mono]] in the provided [[Iterable]] and yields a target collection containing their results in the original input order.\
	 * When the returned [[Task]] is executed, subscriptions to all elements in `monos` are initiated.\
	 * Given the serial execution nature of [[Doer]], concurrency only occurs among monos involving foreign or alien actions (such as [[Task_from]]).\
	 * If any mono fails, the overall [[Task]] immediately fails with that error and cancels all active subscriptions.\
	 * If `monos` is empty, the task completes immediately with an empty collection.\
	 * $threadSafe
	 * @param factory the [[IterableFactory]] used to build the target collection.
	 * @param monos the [[Iterable]] of [[Mono]] instances to sequence.
	 * @tparam A the result type of each [[Mono]].
	 * @tparam C the higher-kinded type of the input collection.
	 * @tparam To the higher-kinded type of the resulting collection.
	 * @return a [[Task]] that yields a collection containing the successful results of all monos in the original input order. */
	def Task_sequence[A: ClassTag, C[x] <: Iterable[x], To[_]](factory: IterableFactory[To], monos: C[Mono[A]]): Task[To[A]] = {
		Task_sequenceToArray(monos).map { array =>
			val builder = factory.newBuilder[A]
			var index = 0
			while index < array.length do {
				builder.addOne(array(index))
				index += 1
			}
			builder.result()
		}
	}

	/** Like [[Task_sequence]], but returns an [[Array]] directly without [[IterableFactory]] conversion overhead.\
	 * $threadSafe
	 * @param monos the [[Iterable]] of [[Mono]] instances to sequence.
	 * @tparam A the result type of each [[Mono]].
	 * @tparam C the higher-kinded type of the input collection.
	 * @return a [[Task]] yielding an [[Array]] with the results of all monos in the original input order. */
	inline def Task_sequenceToArray[A: ClassTag, C[x] <: Iterable[x]](monos: C[Mono[A]]): Task[Array[A]] = new Task_Sequence[A, C](monos)

	/** Creates a [[Task]] that subscribes to each [[Mono]] in the provided [[Iterable]] and yields a target collection containing their [[Try]] outcomes in the original input order.\
	 * The returned [[Task]] always completes successfully once every mono finishes, capturing individual failures into [[scala.util.Failure]] without aborting.\
	 * When the returned [[Task]] is executed, subscriptions to all elements in `monos` are initiated.\
	 * Given the serial execution nature of [[Doer]], concurrency only occurs among monos involving foreign or alien actions (such as [[Task_from]]).\
	 * If `monos` is empty, the task completes immediately with an empty collection.\
	 * $threadSafe
	 * @param factory the [[IterableFactory]] used to build the target collection.
	 * @param monos the [[Iterable]] of [[Mono]] instances to sequence.
	 * @tparam A the value type of each [[Mono]].
	 * @tparam C the higher-kinded type of the input collection.
	 * @tparam To the higher-kinded type of the resulting collection.
	 * @return a [[Task]] that yields a collection of [[Try]] outcomes for all monos in the original input order. */
	def Task_sequenceHardy[A: ClassTag, C[x] <: Iterable[x], To[x] <: Iterable[x]](factory: IterableFactory[To], monos: C[Mono[A]]): Task[To[Try[A]]] = {
		Task_sequenceHardyToArray(monos).map { array =>
			val builder = factory.newBuilder[Try[A]]
			var index = 0
			while index < array.length do {
				builder.addOne(array(index))
				index += 1
			}
			builder.result()
		}
	}

	/** Like [[Task_sequenceHardy]], but returns an [[Array]] directly without [[IterableFactory]] conversion overhead.\
	 * $threadSafe
	 * @param monos the [[Iterable]] of [[Mono]] instances to sequence.
	 * @tparam A the value type of each [[Mono]].
	 * @tparam C the higher-kinded type of the input collection.
	 * @return a [[Task]] yielding an [[Array]] of [[Try]] outcomes for all monos in the original input order. */
	inline def Task_sequenceHardyToArray[A: ClassTag, C[x] <: Iterable[x]](monos: C[Mono[A]]): Task[Array[Try[A]]] =
		new Task_SequenceHardyToArray[A, C](monos)


	////////////// Capture ///////////////

	/** A [[Mono]] that memoizes a single computation outcome and shares it with all present and future subscribers.\
	 * If the outcome is already resolved, new subscribers are notified synchronously upon subscription.\
	 * If the outcome is still pending, subscribers are queued and notified in sequence when resolution occurs.\
	 * Unlike lazy [[Task]] instances, [[Capture]] caches its final outcome, ensuring that derived monadic operations strictly uphold monadic laws.\
	 * Allows dynamic subscription and unsubscription prior to completion. */
	sealed abstract class Capture[+A] extends Mono[A] {

		inline def asMono: Mono[A] = this

		/** Subscribes a [[MonoObserver]] of the captured value.\
		 * The subscription is automatically removed after a value was captured and the received consumer is executed.\
		 * If a value was already captured when this method is called, the provided consumer is invoked synchronously and no subscription occurs.\
		 * Otherwise, the provided [[MonoObserver]] is scheduled to run when the capture occurs in subscription order (after sequentially running all the previously subscribed captured value consumers).\
		 * @note CAUTION: This method does not prevent duplicate subscriptions.
		 * @note CAUTION: Must be called within the $DoSerEx */
		override def subscribeSync(monoObserver: MonoObserver[A]): Subscription

		/** @return a [[Trial]] with the captured value if the target computation completed successfully, a [[Throwable]] if failed, or [[Trial.empty]] if pending.
		 * @note CAUTION: Must be called within the $DoSerEx */
		def maybeResult: Trial[A]

		/** @return true if the target computation completed, successfully or not.
		 * @note CAUTION: Must be called within the $DoSerEx */
		inline def isCompleted: Boolean = maybeResult.isDefined

		/** @return true if this [[Capture]] is still pending; or false if it was completed.
		 * @note CAUTION: Must be called within the $DoSerEx */
		inline def isPending: Boolean = maybeResult.isEmpty

		override def andThen(onSuccess: A => Unit, onError: Throwable => Unit): Capture[A] = {
			triggerSyncCallbacks(onSuccess, onError)
			this
		}

		override def andThen(monoObserver: MonoObserver[A]): Capture[A] = {
			triggerSync(monoObserver)
			this
		}

		override def reconcile: Capture[Try[A]]

		override def withFilter(predicate: A => Boolean): Capture[A]

		override def withFilterGuarded(predicate: A => Boolean): Capture[A]

		override def map[B](f: A => B): Capture[B]

		override def mapGuarded[B](f: A => B): Capture[B]

		override def flatMap[B](f: A => Mono[B]): Mono[B]

		@targetName("flatMapTask")
		def flatMap[B](f: A => Task[B]): Task[B]

		@targetName("flatMapCapture")
		def flatMap[B](f: A => Capture[B]): Capture[B]

		override def flatMapGuarded[B](f: A => Mono[B]): Mono[B]

		@targetName("flatMapGuardedTask")
		def flatMapGuarded[B](f: A => Task[B]): Task[B]

		@targetName("flatMapGuardedCapture")
		def flatMapGuarded[B](f: A => Capture[B]): Capture[B]

		override def transform[B](transformer: MonoTransformer[A, Try[B]]): Capture[B]

		override def transformWith[B](transformer: MonoTransformer[A, Mono[B]]): Mono[B]

		@targetName("transformWithCapture")
		def transformWith[B](transformer: MonoTransformer[A, Capture[B]]): Capture[B]

		@targetName("transformWithTask")
		def transformWith[B](transformer: MonoTransformer[A, Task[B]]): Task[B]

		override def recover[B >: A](pf: Throwable => Maybe[B]): Capture[B]

		override def recoverWith[B >: A](pf: Throwable => Maybe[Mono[B]]): Mono[B]

		@targetName("recoverWithCapture")
		def recoverWith[B >: A](pf: Throwable => Maybe[Capture[B]]): Capture[B]

		@targetName("recoverWithTask")
		def recoverWith[B >: A](pf: Throwable => Maybe[Task[B]]): Task[B]

		override def onBehalfOf(otherDoer: Doer): otherDoer.Capture[A]

		override def castTypePath[E <: Doer](doer: E): doer.Capture[A] = {
			assert(thisDoer eq doer)
			this.asInstanceOf[doer.Capture[A]]
		}

		override def guarded: GuardedCapture[A] = new GuardedCapture[A](this)
	}

	final class GuardedCapture[+A](val underlying: Capture[A]) extends GuardedMono[A] {

		override def withFilter(predicate: A => Boolean): Capture[A] = underlying.withFilterGuarded(predicate)

		override def map[B](f: A => B): Capture[B] = underlying.mapGuarded(f)

		override def flatMap[B](f: A => Mono[B]): Mono[B] = underlying.flatMapGuarded(f)

		@targetName("flatMapCapture")
		def flatMap[B](f: A => Capture[B]): Capture[B] = underlying.flatMapGuarded(f)

		@targetName("flatMapTask")
		def flatMap[B](f: A => Task[B]): Task[B] = underlying.flatMapGuarded(f)


		override def transform[B](transformer: MonoTransformer[A, Try[B]]): Capture[B] = ???

		override def transformWith[B](transformer: MonoTransformer[A, Mono[B]]): Mono[B] = ???

		@targetName("transformWithCapture")
		def transformWith[B](transformer: MonoTransformer[A, Capture[B]]): Capture[B] = ???

		@targetName("transformWithTask")
		def transformWith[B](transformer: MonoTransformer[A, Task[B]]): Task[B] = ???

		override def recover[B >: A](pf: Throwable => Maybe[B]): Capture[B] = ???

		override def recoverWith[B >: A](pf: Throwable => Maybe[Mono[B]]): Mono[B] = ???

		@targetName("recoverWithCapture")
		def recoverWith[B >: A](pf: Throwable => Maybe[Capture[B]]): Capture[B] = ???

		@targetName("recoverWithTask")
		def recoverWith[B >: A](pf: Throwable => Maybe[Task[B]]): Task[B] = ???

	}

	/** A partial implementation of [[Capture]]. Implements everything except [[state]].
	 * Implementation note: Despite extending [[Muxer]], the [[Muxer]] state is actually a component of this class. The composition is implemented by extending [[Muxer]], instead of holding it as a component, to avoid an allocation. Also, given the [[Muxer]] pseudo-component is not public (none of its methods are public), it should not prevent variance on this class. */
	abstract class DefaultCapture[+A] extends Capture[A], Muxer[A @uncheckedVariance, MonoObserver] { thisCaptor =>
		protected def state: Trial[A]

		override def subscribeSync(monoObserver: MonoObserver[A]): Subscription = {
			state.fold {
				addTarget(monoObserver)
				new Subscription {
					override def unsubscribeSync(): Unit = {
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

		override def reconcile: Capture[Try[A]] = {
			state.fold {
				new Captor[Try[A]] with MonoObserver[A] {
					thisCaptor.triggerSync(this)

					override def onSuccess(a: A): Unit = this.captureSync(Success(a))

					override def onError(e: Throwable): Unit = this.captureSync(Failure(e))
				}
			} { e => Keeper(Failure(e))
			} { a => Keeper(Success(a))
			}
		}

		override def withFilter(predicate: A => Boolean): Capture[A] = {
			checkWithin()
			state.fold[Capture[A]] {
				new Captor[A] with MonoObserver[A] {
					thisCaptor.triggerSync(this)

					override def onSuccess(a: A): Unit = {
						if predicate(a) then captureSync(a)
						else trapSync(new NoSuchElementException("Captor filter predicate is not satisfied"))
					}

					override def onError(ex: Throwable): Unit = trapSync(ex)
				}
			} { ex => new Failed(ex) } { a =>
				if predicate(a) then this
				else new Failed(new NoSuchElementException("Captor filter predicate is not satisfied"))
			}
		}

		override def withFilterGuarded(predicate: A => Boolean): Capture[A] = {
			checkWithin()
			state.fold[Capture[A]] {
				new Captor[A] with MonoObserver[A] {
					thisCaptor.triggerSync(this)

					override def onSuccess(a: A): Unit = {
						if predicate(a) then captureSync(a)
						else trapSync(new NoSuchElementException("Captor filter predicate is not satisfied"))
					}

					override def onError(ex: Throwable): Unit = trapSync(ex)
				}
			} { ex => new Failed(ex) } { a =>
				if predicate(a) then this
				else new Failed(new NoSuchElementException("Captor filter predicate is not satisfied"))
			}
		}

		override def map[B](f: A => B): Capture[B] = {
			checkWithin()
			state.fold[Capture[B]] {
				new Captor[B] with MonoObserver[A] {
					thisCaptor.triggerSync(this)

					override def onSuccess(a: A): Unit = captureSync(f(a))

					override def onError(ex: Throwable): Unit = trapSync(ex)
				}
			} { ex => new Failed(ex) } { a => new Keeper[B](f(a)) }
		}

		override def mapGuarded[B](f: A => B): Capture[B] = {
			checkWithin()
			state.fold[Capture[B]] {
				new Captor[B] with MonoObserver[A] {
					thisCaptor.triggerSync(this)

					override def onSuccess(a: A): Unit = {
						val maybeB = try Maybe(f(a)) catch {
							case NonFatal(e) =>
								trapSync(e)
								Maybe.empty
						}
						maybeB.foreach(b => captureSync(b))
					}

					override def onError(ex: Throwable): Unit = trapSync(ex)
				}
			} { ex => new Failed(ex) } { a =>
				try {
					new Keeper[B](f(a))
				} catch {
					case NonFatal(e) => new Failed(e)
				}
			}
		}

		override def flatMap[B](f: A => Mono[B]): Mono[B] = {
			checkWithin()
			state.fold {
				new Captor_FlatMap[A, B](this, f, false)
			} { ex => Task_fail(ex) } { a => f(a) }
		}

		@targetName("flatMapCapture")
		override def flatMap[B](f: A => Capture[B]): Capture[B] = {
			checkWithin()
			state.fold[Capture[B]] {
				new Captor[B] with MonoObserver[A] {
					thisCaptor.triggerSync(this)

					override def onSuccess(a: A): Unit = {
						f(a).subscribeSync(new MonoObserver[B] {
							override def onSuccess(b: B): Unit = captureSync(b)

							override def onError(ex: Throwable): Unit = trapSync(ex)
						})
					}

					override def onError(e: Throwable): Unit = trapSync(e)
				}
			} { ex => new Failed(ex) } { a => f(a) }
		}

		@targetName("flatMapTask")
		override def flatMap[B](f: A => Task[B]): Task[B] = {
			checkWithin()
			state.fold[Task[B]] {
				new Captor_FlatMap[A, B](this, f, false)
			} { ex => Task_fail(ex) } { a => f(a) }
		}

		override def flatMapGuarded[B](f: A => Mono[B]): Mono[B] = {
			checkWithin()
			state.fold[Mono[B]] {
				new Captor_FlatMap[A, B](this, f, true)
			} { ex => Task_fail(ex) } { a =>
				try {
					f(a)
				} catch {
					case NonFatal(e) => Task_fail(e)
				}
			}
		}

		@targetName("flatMapGuardedCapture")
		override def flatMapGuarded[B](f: A => Capture[B]): Capture[B] = {
			checkWithin()
			state.fold[Capture[B]] {
				new Captor[B] with MonoObserver[A] {
					thisCaptor.triggerSync(this)

					override def onSuccess(a: A): Unit = {
						val next = try Maybe(f(a)) catch {
							case NonFatal(e) =>
								trapSync(e)
								Maybe.empty
						}
						next.foreach(_.subscribeSync(new MonoObserver[B] {
							override def onSuccess(b: B): Unit = captureSync(b)

							override def onError(ex: Throwable): Unit = trapSync(ex)
						}))
					}

					override def onError(ex: Throwable): Unit = trapSync(ex)
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
				new Captor_FlatMap[A, B](this, f, true)
			} { ex => Task_fail(ex) } { a =>
				try {
					f(a)
				} catch {
					case NonFatal(e) => Task_fail(e)
				}
			}
		}

		override def transform[B](transformer: MonoTransformer[A, Try[B]]): Capture[B] = {
			checkWithin()
			state.fold[Capture[B]] {
				new Captor[B] with MonoObserver[A] {
					thisCaptor.triggerSync(this)

					override def onSuccess(a: A): Unit = {
						// try {
						transformer.mapSuccess(a) match {
							case Success(b) => captureSync(b)
							case Failure(e) => trapSync(e)
						}
						// } catch {
						//	case NonFatal(e) => breakSync(e)
						// }
					}

					override def onError(ex: Throwable): Unit = {
						// try {
						transformer.mapError(ex) match {
							case Success(b) => captureSync(b)
							case Failure(e) => trapSync(e)
						}
						//} catch {
						//	case NonFatal(e) => breakSync(e)
						//}
					}
				}
			} { ex =>
				//try {
				transformer.mapError(ex) match {
					case Success(b) => Keeper(b)
					case Failure(e) => new Failed(e)
				}
				//} catch {
				//	case NonFatal(e) => new Failed(e)
				//}
			} { a =>
				//try {
				transformer.mapSuccess(a) match {
					case Success(b) => Keeper(b)
					case Failure(e) => new Failed(e)
				}
				//} catch {
				//	case NonFatal(e) => new Failed(e)
				//}
			}
		}

		override def transformWith[B](f: MonoTransformer[A, Mono[B]]): Mono[B] = {
			checkWithin()
			state.fold[Mono[B]] {
				new Captor_TransformWith[A, B](this, f, false)
			} { ex =>
				/*try*/ f.mapError(ex) /*catch {
					case NonFatal(e) => new Failed(e)
				}*/
			} { a =>
				/*try*/ f.mapSuccess(a) /*catch {
					case NonFatal(e) => new Failed(e)
				}*/
			}
		}

		@targetName("transformWithCapture")
		override def transformWith[B](f: MonoTransformer[A, Capture[B]]): Capture[B] = {
			checkWithin()
			state.fold[Capture[B]] {
				new Captor[B] with MonoObserver[A] {
					thisCaptor.subscribeSync(this)

					override def onSuccess(a: A): Unit = handle(f.mapSuccess(a))

					override def onError(ex: Throwable): Unit = handle(f.mapError(ex))

					private def handle(capture: Capture[B]): Unit = {
						capture.triggerSync(new MonoObserver[B] {
							override def onSuccess(b: B): Unit = captureSync(b)

							override def onError(ex: Throwable): Unit = trapSync(ex)
						})
					}
				}
			} { ex =>
				// try {
				f.mapError(ex)
				//} catch {
				//	case NonFatal(e) => new Failed(e)
				//}
			} { a =>
				//try {
				f.mapSuccess(a)
				//} catch {
				//	case NonFatal(e) => new Failed(e)
				//}
			}
		}

		@targetName("transformWithTask")
		override def transformWith[B](f: MonoTransformer[A, Task[B]]): Task[B] = {
			checkWithin()
			state.fold[Task[B]] {
				new Captor_TransformWith[A, B](this, f, false)
			} { ex =>
				f.mapError(ex)
			} { a =>
				f.mapSuccess(a)
			}
		}

		override def recover[B >: A](pf: Throwable => Maybe[B]): Capture[B] = {
			checkWithin()
			state.fold[Capture[B]] {
				new Captor[B] with MonoObserver[A] {
					thisCaptor.triggerSync(this)

					override def onSuccess(a: A): Unit = captureSync(a)

					override def onError(ex: Throwable): Unit = {
						// try {
						pf(ex).fold(trapSync(ex))(b => captureSync(b))
						//} catch {
						//	case NonFatal(e) => breakSync(e)
						//}
					}
				}
			} { ex =>
				/*try*/ pf(ex).fold[Capture[B]](thisCaptor)(Keeper) /*catch {
					case NonFatal(e) => new Failed(e)
				}*/
			} { a => thisCaptor }
		}

		override def recoverWith[B >: A](pf: Throwable => Maybe[Mono[B]]): Mono[B] = {
			checkWithin()
			state.fold[Mono[B]] {
				new Captor_RecoverWith[A, B](thisCaptor, pf, false)
			} { ex =>
				/*try*/ pf(ex).fold[Mono[B]](new Failed(ex))(identity) /*catch {
					case NonFatal(e) => new Failed(e)
				}*/
			} { a => Task_ready(a) }
		}

		@targetName("recoverWithCapture")
		override def recoverWith[B >: A](pf: Throwable => Maybe[Capture[B]]): Capture[B] = {
			checkWithin()
			state.fold[Capture[B]] {
				new Captor[B] with MonoObserver[A] {
					thisCaptor.triggerSync(this)

					override def onSuccess(a: A): Unit = captureSync(a)

					override def onError(ex: Throwable): Unit = {
						pf(ex).fold {
							trapSync(ex)
						} { mb =>
							mb.subscribeSync(new MonoObserver[B] {
								override def onSuccess(b: B): Unit = captureSync(b)

								override def onError(e: Throwable): Unit = trapSync(e)
							})
						}
					}
				}
			} { ex =>
				pf(ex).fold[Capture[B]](thisCaptor)(identity)
			} { a => thisCaptor }
		}

		@targetName("recoverWithTask")
		override def recoverWith[B >: A](pf: Throwable => Maybe[Task[B]]): Task[B] = {
			checkWithin()
			state.fold[Task[B]] {
				new Captor_RecoverWith[A, B](thisCaptor, pf, false)
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

		override def onBehalfOf(otherDoer: Doer): otherDoer.Capture[A] = {
			state.fold {
				otherDoer.Capture_from(thisDoer)(this)
			} { ex => otherDoer.Failed(ex) } { a => new otherDoer.Keeper(a) }
		}
	}

	/** A [[Capture]] that captures a value derived from an observed result. */
	abstract class ChainedCapture[-A, +B] extends DefaultCapture[B], MonoObserver[A]

	/** A [[Capture]] that captures the first value it observes. */
	abstract class DirectlyChainedCaptor[A] extends ChainedCapture[A, A] {
		private var theState: Trial[A] = Trial.empty

		override protected def state: Trial[A] = theState

		override def onSuccess(a: A): Unit = theState = Trial.success(a)

		override def onError(e: Throwable): Unit = theState = Trial.failure(e)
	}

	//// Capture factory methods ////

	/** An already completed [[Capture]] that yields [[Unit]].
	 * CAUTION: This @threadUnsafe lazy val does not guarantee a unique instance under concurrent access. Its use is only safe for logic that depends on the value's data, not its object identity (eq/ne). */
	@threadUnsafe lazy final val Capture_unit: Keeper[Unit] = Keeper(())

	/** An already completed [[Capture]] that yields `true`.
	 * CAUTION: This @threadUnsafe lazy val does not guarantee a unique instance under concurrent access. Its use is only safe for logic that depends on the value's data, not its object identity (eq/ne). */
	@threadUnsafe lazy final val Capture_true: Keeper[Boolean] = Keeper(true)

	/** An already completed [[Capture]] that yields `false`.
	 * CAUTION: This @threadUnsafe lazy val does not guarantee a unique instance under concurrent access. Its use is only safe for logic that depends on the value's data, not its object identity (eq/ne). */
	@threadUnsafe lazy final val Capture_false: Keeper[Boolean] = Keeper(false)

	inline def Capture_ready[A](a: A): Keeper[A] = new Keeper(a)

	def Capture_from[A](mono: Mono[A]): Capture[A] = {
		mono match {
			case capture: Capture[A] => capture
			case _ =>
				new Captor[A]() with MonoObserver[A] {
					mono.triggerSync(this)

					override def onSuccess(a: A): Unit = captureSync(a)

					override def onError(e: Throwable): Unit = trapSync(e)
				}
		}
	}

	inline def Capture_apply[A](inline supplier: () => A, inline isGuarded: Boolean = false): Capture[A] = {
		class CR extends Captor[A] with Runnable {
			executeSequentially(this)

			override def run(): Unit = {
				if isGuarded then {
					val maybeA = try Maybe(supplier()) catch {
						case NonFatal(e) =>
							trapSync(e)
							Maybe.empty
					}
					maybeA.foreach(captureSync(_))
				} else captureSync(supplier())
			}
		}
		new CR
	}

	inline def Capture_defer[A](inline supplier: () => Capture[A], inline isGuarded: Boolean = false): Capture[A] = {
		class COR extends Captor[A] with MonoObserver[A] with Runnable {
			executeSequentially(this)

			override def run(): Unit = {
				if isGuarded then {
					val maybeCaptureA = try Maybe(supplier()) catch {
						case NonFatal(e) =>
							trapSync(e)
							Maybe.empty
					}
					maybeCaptureA.foreach(_.triggerSync(this))
				} else supplier().triggerSync(this)
			}

			override def onSuccess(a: A): Unit = captureSync(a)

			override def onError(e: Throwable): Unit = trapSync(e)
		}
		new COR
	}

	/** Adapts a [[Mono]] belonging to another [[Doer]] into a [[Capture]] sequenced on this [[Doer]].\
	 * Its result will be memoized and yielded by the returned [[Capture]] in sequence with this [[Doer]].\
	 * $threadSafe
	 * @param foreignDoer the [[Doer]] that owns `foreignMono`.
	 * @param foreignMono the [[Mono]] to subscribe to.
	 * @return a [[Capture]] that yields the memoized outcome of `foreignMono` within this [[Doer]]'s sequential timeline. */
	def Capture_from[A](foreignDoer: Doer)(foreignMono: foreignDoer.Mono[A]): Capture[A] = {
		if foreignDoer ne thisDoer then {
			new Captor[A] with foreignDoer.MonoObserver[A] with Runnable {
				foreignDoer.executeSequentially(this)

				override def run(): Unit = foreignMono.triggerSync(this)

				override def onSuccess(a: A): Unit = thisDoer.run(captureSync(a))

				override def onError(e: Throwable): Unit = thisDoer.run(trapSync(e))
			}
		} else foreignMono match {
			case ft: foreignDoer.Capture[A] @unchecked => ft.asInstanceOf[Capture[A]]
			case _ => Capture_from(foreignMono.asInstanceOf[Mono[A]])
		}
	}

	/** Creates a [[Capture]] whose result will be the result of the provided [[Future]] when it completes.\
	 * Useful to start a process in this [[Doer]]'s DoSerEx derived from a value produced by another process.\
	 * $threadSafe
	 * @param future the future to wait for.
	 * @return the [[Capture]] described in the method description. */
	final def Capture_from[A](future: Future[A]): Capture[A] = {
		new Captor[A] with (Try[A] => Unit) {
			future.onComplete(this)(using ownSerialExecutionContext)

			override def apply(tryA: Try[A]): Unit = tryA match {
				case Success(a) => captureSync(a)
				case Failure(e) => trapSync(e)
			}
		}
	}

	/** Creates a [[Capture]] whose result will be the result of the [[Future]] returned by the provided supplier.\
	 * Useful to start a process in an alien executor and continue it within this [[Doer]]'s DoSerEx.\
	 * The alien executor may be the $DoSerEx of this [[Doer]].\
	 * $threadSafe
	 * @param supplier a function that starts the process and returns a [[Future]] of its result. $isExecutedByDoSerEx
	 * @return the [[Capture]] described in the method description. */
	final def Capture_from[A](supplier: () => Future[A]): Capture[A] = {
		new Captor[A] with (Try[A] => Unit) {
			run(supplier().onComplete(this)(using ownSerialExecutionContext))

			override def apply(tryA: Try[A]): Unit = tryA match {
				case Success(a) => captureSync(a)
				case Failure(e) => trapSync(e)
			}
		}
	}

	inline def Capture_sequenceToArray[A: ClassTag, C[x] <: Iterable[x]](monos: C[Mono[A]], inline isWithinDoSerEx: Boolean = isInSequence): Capture[Array[A]] = {
		Captor_triggerAndWire(Task_sequenceToArray(monos)) // TODO optimize
	}

	/** Like [[Task_sequenceHardyToArray]] but eager (instead of lazy). */
	inline def Capture_sequenceHardyToArray[A: ClassTag, C[x] <: Iterable[x]](monos: C[Mono[A]], inline isWithinDoSerEx: Boolean = isInSequence): Capture[Array[Try[A]]] = {
		Captor_triggerAndWire(Task_sequenceHardyToArray(monos), isWithinDoSerEx) // TODO optimize
	}

	//// Keeper ////

	/** Creates a [[Keeper]] that yields the provided value.
	 * @note Also suppresses the generation of the synthetic companion object. */
	inline final def Keeper[A](a: A): Keeper[A] = new Keeper(a)

	/** A [[Capture]] that is fulfilled since its inception. */
	final class Keeper[+A](val value: A) extends Capture[A] { thisKeeper =>

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

		override def reconcile: Keeper[Try[A]] = Keeper(Success(value))

		override def withFilter(predicate: A => Boolean): Capture[A] = {
			if predicate(value) then thisKeeper
			else Failed(new NoSuchElementException)
		}

		override def withFilterGuarded(predicate: A => Boolean): Capture[A] = {
			try {
				if predicate(value) then thisKeeper
				else Failed(new NoSuchElementException)
			} catch {
				case NonFatal(e) => Failed(e)
			}
		}

		override def map[B](f: A => B): Keeper[B] = {
			checkWithin()
			Keeper(f(value))
		}

		override def mapGuarded[B](f: A => B): Capture[B] = {
			checkWithin()
			try Keeper(f(value)) catch {
				case NonFatal(e) => Failed(e)
			}
		}

		@targetName("flatMapCapture")
		override def flatMap[B](f: A => Capture[B]): Capture[B] = {
			checkWithin()
			f(value)
		}

		@targetName("flatMapTask")
		override def flatMap[B](f: A => Task[B]): Task[B] = {
			checkWithin()
			f(value)
		}

		override def flatMap[B](f: A => Mono[B]): Mono[B] = {
			checkWithin()
			f(value)
		}

		override def flatMapGuarded[B](f: A => Mono[B]): Mono[B] = {
			checkWithin()
			try f(value) catch {
				case NonFatal(e) => Failed(e)
			}
		}

		@targetName("flatMapGuardedCapture")
		override def flatMapGuarded[B](f: A => Capture[B]): Capture[B] = {
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

		override def transform[B](f: MonoTransformer[A, Try[B]]): Capture[B] = {
			checkWithin()
			f.mapSuccess(value) match {
				case Success(b) => Keeper(b)
				case Failure(e) => Failed(e)
			}
		}

		override def transformWith[B](f: MonoTransformer[A, Mono[B]]): Mono[B] = {
			checkWithin()
			f.mapSuccess(value)
		}

		@targetName("transformWithCapture")
		override def transformWith[B](f: MonoTransformer[A, Capture[B]]): Capture[B] = {
			checkWithin()
			f.mapSuccess(value)
		}

		@targetName("transformWithTask")
		override def transformWith[B](f: MonoTransformer[A, Task[B]]): Task[B] = {
			checkWithin()
			f.mapSuccess(value)
		}

		override def recover[B >: A](pf: Throwable => Maybe[B]): Keeper[B] = thisKeeper

		override def recoverWith[B >: A](pf: Throwable => Maybe[Mono[B]]): Keeper[B] = thisKeeper

		@targetName("recoverWithCapture")
		override def recoverWith[B >: A](pf: Throwable => Maybe[Capture[B]]): Capture[B] = thisKeeper

		@targetName("recoverWithTask")
		override def recoverWith[B >: A](pf: Throwable => Maybe[Task[B]]): Task[B] = Task_ready(value)

		override def toFuture(isWithinDoSerEx: Boolean = isInSequence): Future[A] = Future.successful(value)

		override def onBehalfOf(otherDoer: Doer): otherDoer.Keeper[A] = new otherDoer.Keeper(value)

		override def toString: String = deriveToString[Keeper[A]](thisKeeper)
	}

	//// Failed ////

	/** Creates a [[Failed]] that yields the provided value.
	 * @note Also suppresses the generation of the synthetic companion object. */
	inline final def Failed(exception: Throwable): Failed = new Failed(exception)

	final class Failed(val exception: Throwable) extends Capture[Nothing] { thisFailed =>
		override def maybeResult: Trial[Nothing] = Trial.failure(exception)

		override def subscribeSync(downChainObserver: MonoObserver[Nothing]): Subscription = {
			downChainObserver.onError(exception)
			Subscription_empty
		}

		override def triggerSync(downChainObserver: MonoObserver[Nothing]): Unit = downChainObserver.onError(exception)

		override def foreach(consumer: Nothing => Unit): Unit = ()

		override def reconcile: Capture[Try[Nothing]] = Keeper(Failure(exception))

		override def withFilter(predicate: Nothing => Boolean): Failed = thisFailed

		override def withFilterGuarded(predicate: Nothing => Boolean): Failed = thisFailed

		override def map[B](f: Nothing => B): Failed = thisFailed

		override def mapGuarded[B](f: Nothing => B): Failed = thisFailed

		override def flatMap[B](f: Nothing => Mono[B]): Failed = thisFailed

		@targetName("flatMapCapture")
		override def flatMap[B](f: Nothing => Capture[B]): Failed = thisFailed

		@targetName("flatMapTask")
		override def flatMap[B](f: Nothing => Task[B]): Task[B] = Task_fail(exception)

		override def flatMapGuarded[B](f: Nothing => Mono[B]): Failed = thisFailed

		@targetName("flatMapGuardedCapture")
		override def flatMapGuarded[B](f: Nothing => Capture[B]): Failed = thisFailed

		@targetName("flatMapGuardedTask")
		override def flatMapGuarded[B](f: Nothing => Task[B]): Task[B] = Task_fail(exception)

		// override def reconcile: Capture[Try[Nothing]] = Keeper(Failure(exception))

		override def transform[B](f: MonoTransformer[Nothing, Try[B]]): Capture[B] = {
			checkWithin()
			f.mapError(exception) match {
				case Success(b) => Keeper(b)
				case Failure(ex) => new Failed(ex)
			}
		}

		override def transformWith[B](f: MonoTransformer[Nothing, Mono[B]]): Mono[B] = {
			checkWithin()
			f.mapError(exception)
		}

		@targetName("transformWithCapture")
		override def transformWith[B](f: MonoTransformer[Nothing, Capture[B]]): Capture[B] = {
			checkWithin()
			f.mapError(exception)
		}

		@targetName("transformWithTask")
		override def transformWith[B](f: MonoTransformer[Nothing, Task[B]]): Task[B] = {
			checkWithin()
			f.mapError(exception)
		}

		override def recover[B >: Nothing](pf: Throwable => Maybe[B]): Capture[B] = {
			checkWithin()
			pf(exception).fold(thisFailed)(Keeper)
		}

		override def recoverWith[B >: Nothing](pf: Throwable => Maybe[Mono[B]]): Mono[B] = {
			checkWithin()
			pf(exception).fold(thisFailed)(identity)
		}

		@targetName("recoverWithCapture")
		override def recoverWith[B >: Nothing](pf: Throwable => Maybe[Capture[B]]): Capture[B] = {
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

	/** A mutable [[Capture]] that acts as a single-assignment completion promise and rendezvous point.\
	 * A [[Captor]] starts in a pending state and can be completed with a value via [[captureSync]]/[[capture]], a failure via [[trapSync]]/[[trap]], a [[scala.util.Try]] via [[seizeSync]]/[[seize]], or wired to another [[Mono]] via [[seizeWithSync]]/[[seizeWith]].\
	 * The first completion transition fixes the outcome, notifies all registered observers in subscription order, and clears the observer registry.\
	 * Subsequent completion attempts are ignored, preserving the initial outcome.\
	 * Observers subscribed before completion are queued and notified upon resolution; observers subscribed after completion receive the memoized outcome synchronously.\
	 * Synchronous completion methods (`captureSync`, `trapSync`, `seizeSync`, `seizeWithSync`) must be invoked within the owning [[Doer]]'s sequential timeline ($DoSerEx), whereas asynchronous variants (`capture`, `trap`, `seize`, `seizeWith`) are thread-safe.\
	 * @param initialState the initial completion state of the captor; defaults to [[Trial.empty]] (pending). */
	open class Captor[A](initialState: Trial[A] = Trial.empty) extends DefaultCapture[A] { thisCaptor =>
		protected var theState: Trial[A] = initialState

		override protected def state: Trial[A] = theState

		/** Sets this [[Captor]]'s captured value with the given successful `result`, unless it has already been set.
		 *
		 * If this [[Captor]]'s captured value is not yet set, the provided `result` becomes its final value and is made immediately visible to all subscribers.
		 * If it is already set, the provided `result` is ignored.
		 *
		 * CAUTION: This method must be called within this [[Doer]].
		 * CAUTION: Deep synchronous chains of [[flatMap]] over immediately-fulfilled [[Capture]] instances during ongoing fulfillment can form a synchronous recursion (fulfill → subscribe-immediate → fulfill → …) that overflows the stack. This could be avoided in the library but is not worth. The user can prevent it easily with the help of [[Doer.currentExecutionSerial]].
		 *
		 * @param result the value to fulfill this [[Captor]] with.
		 * @param completionObserver optional synchronous observer of the actual captured value and information about its origin:
		 *   - [[THE_PROVIDED]] if the captured value was set by this method call with the provided value;
		 *   - [[ANOTHER_BEFORE]] if the captured value was already set when this method was called. */
		final def captureSync(result: A, completionObserver: CompletionObserver[A] = CompletionIgnorer): this.type = {
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

		/** Sets this [[Captor]]'s captured value with the given failure `excuse`, unless it has already been set.
		 *
		 * If this [[Captor]] is not yet fulfilled, the provided `excuse` becomes its final value and is made immediately visible to all subscribers.
		 * If it is already fulfilled, the provided `excuse` is ignored.
		 *
		 * CAUTION: This method must be called within this [[Doer]].
		 * CAUTION: Deep synchronous chains of [[flatMap]] over immediately-fulfilled [[Capture]] instances during ongoing fulfillment can form a synchronous recursion (fulfill → subscribe-immediate → fulfill → …) that overflows the stack. // TODO consider the trampoline solutions discussed with copilot in the session "causal anchoring dilema", near the end.
		 *
		 * @param excuse the exception to fail this [[Captor]] with.
		 * @param completionObserver optional synchronous observer of the actual captured value and information about its origin:
		 *   - [[THE_PROVIDED]] if the captured value was set by this method call with the provided value;
		 *   - [[ANOTHER_BEFORE]] if the captured value was already set when this method was called. */
		final def trapSync(excuse: Throwable, completionObserver: CompletionObserver[A] = CompletionIgnorer): this.type = {
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

		final def seizeWithSync(completingMono: Mono[A], completionObserver: CompletionObserver[A] = CompletionIgnorer): this.type = {
			if completingMono eq this then throw IllegalArgumentException("A Captor can't be fulfilled with itself.")
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

		/** @param completionObserver optional synchronous observer of the actual completion result and origin. The `originId` parameter indicates the [[ImmediateResultOrigin]]. */
		inline def capture(result: A, inline isWithinDoSerEx: Boolean = isInSequence, completionObserver: CompletionObserver[A] = CompletionIgnorer): this.type = {
			if isWithinDoSerEx then captureSync(result, completionObserver)
			else {
				run(captureSync(result, completionObserver))
				this
			}
		}

		/** @param completionObserver optional synchronous observer of the actual completion result and origin. The `originId` parameter indicates the [[ImmediateResultOrigin]]. */
		inline def trap(excuse: Throwable, inline isWithinDoSerEx: Boolean = isInSequence, completionObserver: CompletionObserver[A] = CompletionIgnorer): this.type = {
			if isWithinDoSerEx then trapSync(excuse, completionObserver)
			else {
				run(trapSync(excuse, completionObserver))
				this
			}
		}

		/** @param completionObserver optional synchronous observer of the actual completion result and origin. The `originId` parameter indicates the [[ImmediateResultOrigin]]. */
		def seizeSync(result: Try[A], completionObserver: CompletionObserver[A] = CompletionIgnorer): this.type = {
			result match {
				case Success(a) =>
					captureSync(a, completionObserver)
				case Failure(ex) =>
					trapSync(ex, completionObserver)
			}
		}

		/** @param completionObserver optional synchronous observer of the actual completion result and origin. The `originId` parameter indicates the [[ImmediateResultOrigin]]. */
		inline def seize(result: Try[A], inline isWithinDoSerEx: Boolean = isInSequence, completionObserver: CompletionObserver[A] = CompletionIgnorer): this.type = {
			if isWithinDoSerEx then {
				checkWithin()
				seizeSync(result, completionObserver)
			} else {
				run(seizeSync(result, completionObserver))
				this
			}
		}

		/** @param completionObserver optional observer of the actual completion result and origin. The `originId` parameter indicates the [[ResultOrigin]]. */
		inline def seizeWith(completingMono: Mono[A], inline isWithinDoSerEx: Boolean = isInSequence, completionObserver: CompletionObserver[A] = CompletionIgnorer): this.type = {
			if isWithinDoSerEx then seizeWithSync(completingMono, completionObserver)
			else {
				if completingMono eq this then throw IllegalArgumentException("A Captor can't be fulfilled with itself.")
				run(seizeWithSync(completingMono, completionObserver))
				this
			}
		}

		override def toString(): String = {
			s"Captor(state=$state, isRegistryEmpty=$isRegistryEmpty)"
		}
	}

	/** TODO This class is very similar to [[Task_FlatMap]]. Consider removing duplication by extending a common super class. */
	final class Captor_FlatMap[+A, B](upChainMono: Mono[A], f: A => Mono[B], isGuarded: Boolean) extends AbstractTask[B] {
		private var fResultMemory: Maybe[Mono[B]] = Maybe.empty

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
						if fResultMemory.isEmpty then {
							val maybeInnerMonoB =
								if isGuarded then try Maybe(f(a)) catch {
									case NonFatal(e) =>
										isActive = false
										downChainObserver.onError(e)
										Maybe.empty
								} else Maybe(f(a))
							fResultMemory = maybeInnerMonoB
						}
						if isActive then fResultMemory.foreach { innerMono =>
							val innerSubscription = innerMono.subscribeSync(downChainObserver)
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

				override def unsubscribeSync(): Unit = {
					if isActive then {
						isActive = false
						val mus = maybeUpChainSubscription
						val mis = maybeInnerSubscription
						maybeUpChainSubscription = Maybe.empty
						maybeInnerSubscription = Maybe.empty
						mus.foreach(_.unsubscribeSync())
						mis.foreach(_.unsubscribeSync())
					}
				}
			}
		}
	}

	final class Captor_TransformWith[A, B](upChainMono: Mono[A], f: MonoTransformer[A, Mono[B]], isGuarded: Boolean) extends AbstractTask[B] {
		private var fResultMemory: Maybe[Mono[B]] = Maybe.empty

		override def subscribeSync(downChainObserver: MonoObserver[B]): Subscription = {
			new Subscription with MonoObserver[A] {
				private var isActive = true
				private var maybeUpChainSubscription: Maybe[Subscription] = Maybe.empty
				private var maybeInnerSubscription: Maybe[Subscription] = Maybe.empty

				{ // Constructor
					val upChainSubscription = upChainMono.subscribeSync(this)
					if isActive then maybeUpChainSubscription = Maybe(upChainSubscription)
				}

				override def onSuccess(a: A): Unit = handle(f.mapSuccess(a))

				override def onError(ex: Throwable): Unit = handle(f.mapError(ex))

				private inline def handle(inline momoBuilder: => Mono[B]): Unit = {
					if isActive then {
						maybeUpChainSubscription = Maybe.empty
						if fResultMemory.isEmpty then {
							val maybeInnerMono =
								if isGuarded then try Maybe(momoBuilder) catch {
									case NonFatal(e) =>
										isActive = false
										downChainObserver.onError(e)
										Maybe.empty
								} else Maybe(momoBuilder)
							fResultMemory = maybeInnerMono
						}

						if isActive then fResultMemory.foreach { innerMono =>
							val innerSubscription = innerMono.subscribeSync(downChainObserver)
							if isActive then maybeInnerSubscription = Maybe(innerSubscription)
						}
					}
				}

				override def unsubscribeSync(): Unit = {
					if isActive then {
						isActive = false
						val mus = maybeUpChainSubscription
						val mis = maybeInnerSubscription
						maybeUpChainSubscription = Maybe.empty
						maybeInnerSubscription = Maybe.empty
						mus.foreach(_.unsubscribeSync())
						mis.foreach(_.unsubscribeSync())
					}
				}
			}
		}
	}

	final class Captor_RecoverWith[A, B >: A](upChainMono: Mono[A], pf: Throwable => Maybe[Mono[B]], isGuarded: Boolean) extends AbstractTask[B] {
		private var pfResultIsEmpty: Boolean = true
		private var pfResultMemory: Maybe[Mono[B]] = Maybe.empty

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
						if pfResultIsEmpty then {
							if isGuarded then try {
								pfResultMemory = pf(ex)
								pfResultIsEmpty = false
							} catch {
								case NonFatal(e) =>
									isActive = false
									downChainObserver.onError(e)
									Maybe.empty
							} else {
								pfResultMemory = pf(ex)
								pfResultIsEmpty = false
							}
						}
						if isActive then pfResultMemory.fold(downChainObserver.onError(ex)) { innerMono =>
							val innerSubscription = innerMono.subscribeSync(downChainObserver)
							if isActive then maybeInnerSubscription = Maybe(innerSubscription)
						}
					}
				}

				override def unsubscribeSync(): Unit = {
					isActive = false
					val mus = maybeUpChainSubscription
					val mis = maybeInnerSubscription
					maybeUpChainSubscription = Maybe.empty
					maybeInnerSubscription = Maybe.empty
					mus.foreach(_.unsubscribeSync())
					mis.foreach(_.unsubscribeSync())
				}
			}
		}
	}

	//// COVENANT FACTORY METHODS ////

	/** Creates a new pending [[Captor]] */
	inline def Captor[A](): Captor[A] = new Captor()

	/** Creates a [[Captor]] that will fulfill with the result of executing the provided supplier within the $DoSerEx.
	 * @param supplier a supplier function that is executed within the $DoSerEx and returns the value to fulfill the created [[Captor]] with. */
	inline def Captor_apply[A](inline supplier: () => A, inline isGuarded: Boolean = false): Captor[A] = {
		class CR extends Captor[A]() with Runnable {
			{ // Constructor
				executeSequentially(this)
			}

			override def run(): Unit = {
				if isGuarded then {
					val maybeA = try Maybe(supplier()) catch {
						case NonFatal(e) =>
							trapSync(e)
							Maybe.empty
					}
					maybeA.foreach(captureSync(_))
				} else {
					captureSync(supplier())
				}
			}
		}
		new CR
	}

	/** Creates a [[Captor]] that is wired to the [[Capture]] resulting from executing the provided supplier within the $DoSerEx.
	 * @param supplier a supplier function that is executed within the $DoSerEx to return the [[Capture]] to which the created [[Captor]] is wired. */
	inline def Captor_defer[A](inline supplier: () => Capture[A], inline isGuarded: Boolean = false): Captor[A] = {
		class CRO extends Captor[A] with Runnable with MonoObserver[A] {
			{ // Constructor
				executeSequentially(this)
			}

			override def run(): Unit = {
				if isGuarded then {
					val maybeCaptureA = try Maybe(supplier()) catch {
						case NonFatal(e) =>
							trapSync(e)
							Maybe.empty
					}
					maybeCaptureA.foreach(_.triggerSync(this))
				} else {
					supplier().triggerSync(this)
				}
			}

			override def onSuccess(a: A): Unit = captureSync(a)

			override def onError(e: Throwable): Unit = trapSync(e)
		}
		new CRO
	}

	/** Triggers an execution of the given [[Task]] and returns a [[Captor]] that will be completed with the result of the triggered execution if it completes before this [[Captor]] is completed by other means.
	 *
	 * This method initiates an execution of the given [[Task]] and wires its result to a newly created [[Captor]].
	 * The returned [[Captor]] acts as a completion handle for the execution triggered by this method, and can be used to observe or react to its result.
	 *
	 * @param task the [[Task]] to be triggered.
	 * @param isWithinDoSerEx $isWithinDoSerEx
	 * @param completionObserver optional synchronous observer of the actual completion result and origin. The `originId` parameter indicates the [[ImmediateResultOrigin]].
	 * @return a [[Captor]] that will be completed with the result of the execution triggered by this method.
	 */
	inline def Captor_triggerAndWire[A](
		task: Task[A],
		inline isWithinDoSerEx: Boolean = isInSequence,
		completionObserver: CompletionObserver[A] = CompletionIgnorer,
	): Captor[A] = {
		class CO extends Captor[A] with MonoObserver[A] {
			{ // Constructor
				task.trigger(isWithinDoSerEx)(this)
			}

			override def onSuccess(a: A): Unit = captureSync(a, completionObserver)

			override def onError(e: Throwable): Unit = trapSync(e, completionObserver)
		}
		new CO
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
						targetAtIndex eq null
					} do index += 1
					// If none found then the registry is empty
					if index == followingTargetsSize then Maybe.empty
					// else, remove it from the array and make it be the first target
					else {
						followingTargets(index) = null
						Maybe(targetAtIndex.asInstanceOf[Target])
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
