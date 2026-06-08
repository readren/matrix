package readren.common

import scala.compiletime.asMatchable

final class Trial[+A](val raw: AnyRef | Null) extends AnyVal {

	inline def isEmpty: Boolean = raw eq null

	inline def isDefined: Boolean = raw ne null

	inline def isSuccess: Boolean = isDefined && !raw.isInstanceOf[Trial.Failure]

	inline def isFailure: Boolean = isDefined && raw.isInstanceOf[Trial.Failure]

	inline def get: A = {
		if isFailure then throw new NoSuchElementException("Trial is a failure")
		else if isEmpty then throw new NoSuchElementException("Trial is empty")
		else raw.asInstanceOf[A]
	}

	inline def failure: Throwable = {
		if isSuccess then throw new NoSuchElementException("Trial is not a failure")
		else if isEmpty then throw new NoSuchElementException("Trial is empty")
		else raw.asInstanceOf[Trial.Failure].exception
	}

	inline def toMaybe: Maybe[A] = {
		if isSuccess then Maybe.some(raw.asInstanceOf[A])
		else Maybe.empty
	}

	inline def fold[B](inline ifEmpty: => B)(inline onFailure: Throwable => B)(inline onSuccess: A => B): B = {
		if isEmpty then ifEmpty
		else {
			raw.asMatchable match {
				case Trial.Failure(ex) => onFailure(ex)
				case _ => onSuccess(raw.asInstanceOf[A])
			}
		}
	}

	override def toString: String =
		if isEmpty then "empty"
		else {
			raw.asMatchable match {
				case Trial.Failure(ex) => s"failure($ex)"
				case _ => s"success($raw)"
			}
		}
}

object Trial {
	final case class Failure(exception: Throwable)

	val empty: Trial[Nothing] = new Trial(null)

	inline def success[A](value: A): Trial[A] = {
		val ref = value.asInstanceOf[AnyRef | Null]
		if ref eq null then throw new IllegalArgumentException("Trial.success cannot wrap null")
		else new Trial(ref)
	}

	inline def failure(ex: Throwable): Trial[Nothing] =
		new Trial(Failure(ex))

	given [A, B] =>CanEqual[A, B] => CanEqual[Trial[A], Trial[B]] = CanEqual.derived
}
