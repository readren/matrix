package readren.common

import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

extension [A](thisTry: Try[A]) {
	/** Equivalent to [[Try.map]] but avoiding the by-name parameter overhead. */
	def mapFast[B](f: A => B): Try[B] = {
		thisTry match {
			case success: Success[A] =>
				try Success(f(success.value))
				catch {
					case NonFatal(e) => Failure(e)
				}
			case failure: Failure[A] =>
				failure.castTo[B]
		}
	}

	/** Equivalent to [[Try.flatMap]] but avoiding the by-name parameter overhead. */
	def flatMapFast[B](f: A => Try[B]): Try[B] = {
		thisTry match {
			case success: Success[A] =>
				try f(success.value)
				catch {
					case NonFatal(e) => Failure(e)
				}
			case failure: Failure[A] =>
				failure.castTo[B]
		}
	}

	/** Reifies the evaluation of `f` back into the [[Try]] container.
	 * In other words, applies the provided function to this [[Try]] instance, capturing any [[NonFatal]] exceptions thrown by the function and returning them as a [[Failure]].
	 *
	 * Equivalent to {{{reify(Failure.apply)(f)}}} */
	def reifyBack[B](reifiedFunc: Try[A] => Try[B]): Try[B] = {
		try reifiedFunc(thisTry)
		catch {
			case NonFatal(e) => Failure(e)
		}
	}

	/** Reifies the evaluation of `f`.
	 *
	 * In other words, applies the provided function to this [[Try]] instance collapsing nonfatal exceptions into results of the same type. */
	inline def reify[B](inline onException: Throwable => B)(reifiedFunc: Try[A] => B): B = {
		try reifiedFunc(thisTry)
		catch {
			case NonFatal(e) => onException(e)
		}
	}

	/**
	 * A composite operation that first folds this [[Try]] and then reifies the evaluation of the success-path function.
	 *
	 * @param onFailure Handles the case where this [[Try]] is already a [[Failure]].
	 * @param onException Handles the case when the reified function throws a nonfatal exception.
	 * @param reifiedFunc The function applied when this [[Try]] is a [[Success]] and whose evaluation is being reified.
	 */
	inline def foldAndThenReify[B, M[x <: B]](inline onFailure: Failure[B] => M[B], inline onException: Throwable => M[B], inline reifiedFunc: A => M[B]): M[B] = {
		thisTry match {
			case success: Success[A] =>
				try reifiedFunc(success.value)
				catch {
					case NonFatal(e) => onException(e)
				}

			case failure: Failure[A] => onFailure(failure.castTo[B])
		}
	}
}