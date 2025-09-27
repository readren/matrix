package readren.matrix
package cluster.misc

import scala.reflect.Typeable

/**
 * Represents a deferred computation that produces a value of type `A` or a `Fault`.
 * The computation is not executed until explicitly triggered using the `trigger` method.
 * This trait supports composition and transformation of asynchronous computations through methods
 * like [[map]], [[flatMap]], [[transform]], and [[transformWith]].
 *
 * @tparam A The type of the value produced by the computation.
 */
trait Lazy[A <: Matchable, Fault <: Matchable : Typeable] { thisLazy =>

	/**
	 * Triggers the deferred computation and invokes the `onComplete` callback with the result.
	 *
	 * @param onComplete A callback function that accepts either the computed value (`A`) or a `Fault`.
	 */
	def trigger(onComplete: A | Fault => Unit): Unit

	/**
	 * Transforms the result of this computation using the function `f`.
	 * If the computation produces a `Fault`, the `Fault` is propagated without applying `f`.
	 *
	 * **Important**: The provided `f` function must not throw non-fatal exceptions. If it does,
	 * the behavior is undefined, and the exception may propagate unexpectedly. If exception handling
	 * is required, the caller should wrap the `f` function in a `try-catch` block.
	 *
	 * @param f A function that transforms the result of type `A` into a value of type `B`.
	 * @tparam B The type of the transformed value.
	 * @return A new `Lazy[B]` representing the transformed computation.
	 */
	def map[B <: Matchable](f: A => B): Lazy[B, Fault] = new Lazy[B, Fault] {
		override def trigger(onComplete: B | Fault => Unit): Unit = thisLazy.trigger {
			case fault: Fault => onComplete(fault)
			case a: A @unchecked => onComplete(f(a))
		}
	}

	/**
	 * Transforms the result of this computation into a new `Lazy[B]` using the function `f`.
	 * If the computation produces a `Fault`, the `Fault` is propagated without applying `f`.
	 *
	 * **Important**: The provided `f` function must not throw non-fatal exceptions. If it does,
	 * the behavior is undefined, and the exception may propagate unexpectedly. If exception handling
	 * is required, the caller should wrap the `f` function in a `try-catch` block.
	 *
	 * @param f A function that transforms the result of type `A` into a new `Lazy[B]`.
	 * @tparam B The type of the transformed computation.
	 * @return A new `Lazy[B]` representing the transformed computation.
	 */
	def flatMap[B <: Matchable](f: A => Lazy[B, Fault]): Lazy[B, Fault] = new Lazy[B, Fault] {
		override def trigger(onComplete: B | Fault => Unit): Unit =
			thisLazy.trigger {
				case fault: Fault => onComplete(fault)
				case a: A @unchecked => f(a).trigger(onComplete)
			}
	}

	/**
	 * Transforms the result of this computation (either `A` or `Fault`) into a new result (`B` or `Fault`)
	 * using the function `f`.
	 *
	 * **Important**: The provided `f` function must not throw non-fatal exceptions. If it does,
	 * the behavior is undefined, and the exception may propagate unexpectedly. If exception handling
	 * is required, the caller should wrap the `f` function in a `try-catch` block.
	 *
	 * @param f A function that transforms the result (`A` or `Fault`) into a new result (`B` or `Fault`).
	 * @tparam B The type of the transformed result.
	 * @return A new `Lazy[B]` representing the transformed computation.
	 */
	def transform[B <: Matchable](f: A | Fault => B | Fault): Lazy[B, Fault] = new Lazy[B, Fault] {
		override def trigger(onComplete: B | Fault => Unit): Unit =
			thisLazy.trigger { aOrFault =>
				onComplete(f(aOrFault))
			}
	}

	/**
	 * Transforms the result of this computation (either `A` or `Fault`) into a new `Lazy[B]` using the function `next`.
	 *
	 * **Important**: The provided `next` function must not throw non-fatal exceptions. If it does,
	 * the behavior is undefined, and the exception may propagate unexpectedly. If exception handling
	 * is required, the caller should wrap the `next` function in a `try-catch` block.
	 *
	 * @param next A function that transforms the result (`A` or `Fault`) into a new `Lazy[B]`.
	 * @tparam B The type of the transformed computation.
	 * @return A new `Lazy[B]` representing the transformed computation.
	 */
	def transformWith[B <: Matchable](next: A | Fault => Lazy[B, Fault]): Lazy[B, Fault] = new Lazy[B, Fault] {
		override def trigger(onComplete: B | Fault => Unit): Unit =
			thisLazy.trigger {
				case fault: Fault => next(fault).trigger(onComplete)
				case a: A @unchecked => next(a).trigger(onComplete)
			}
	}

	/**
	 * Attaches a side effect to this computation. The side effect is executed **before** the completion handler.
	 *
	 * **Important**: The provided `sideEffect` function must not throw non-fatal exceptions. If it does,
	 * the behavior is undefined, and the exception may propagate unexpectedly. If exception handling
	 * is required, the caller should wrap the `sideEffect` function in a `try-catch` block.
	 *
	 * @param sideEffect A function that performs a side effect based on the result of this computation.
	 * @return A new `Lazy[A]` representing the original computation with the side effect attached.
	 */
	def andThen(sideEffect: A | Fault => Unit): Lazy[A, Fault] = new Lazy[A, Fault] {
		override def trigger(onComplete: A | Fault => Unit): Unit =
			thisLazy.trigger { result =>
				sideEffect(result)
				onComplete(result)
			}
	}

	/**
	 * Attaches a side effect to this computation. The side effect is executed **after** the completion handler.
	 *
	 * **Important**: The provided `sideEffect` function must not throw non-fatal exceptions. If it does,
	 * the behavior is undefined, and the exception may propagate unexpectedly. If exception handling
	 * is required, the caller should wrap the `sideEffect` function in a `try-catch` block.
	 *
	 * @param sideEffect A function that performs a side effect based on the result of this computation.
	 * @return A new `Lazy[A]` representing the original computation with the side effect attached.
	 */
	def andFinally(sideEffect: A | Fault => Unit): Lazy[A, Fault] = new Lazy[A, Fault] {
		override def trigger(onComplete: A | Fault => Unit): Unit =
			thisLazy.trigger { result =>
				onComplete(result)
				sideEffect(result)
			}
	}
}