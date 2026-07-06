package readren.common

import scala.compiletime.asMatchable

final class Trial[+A](val raw: AnyRef | Null) extends AnyVal {

	inline def isEmpty: Boolean = raw eq null

	inline def isDefined: Boolean = raw ne null

	inline def isSuccess: Boolean = isDefined && !raw.isInstanceOf[Trial.Failure_Private]

	inline def isFailure: Boolean = isDefined && raw.isInstanceOf[Trial.Failure_Private]

	//	inline def get: A = {
	//		if isFailure then throw new NoSuchElementException("Trial is a failure")
	//		else if isEmpty then throw new NoSuchElementException("Trial is empty")
	//		else raw.asInstanceOf[A]
	//	}
	//
	//	inline def failure: Throwable = {
	//		if isSuccess then throw new NoSuchElementException("Trial is not a failure")
	//		else if isEmpty then throw new NoSuchElementException("Trial is empty")
	//		else raw.asInstanceOf[Trial.Failure].exception
	//	}
	//
	//	inline def toMaybe: Maybe[A] = {
	//		if isSuccess then Maybe.some(raw.asInstanceOf[A])
	//		else Maybe.empty
	//	}

	inline def getOrElse[B >: A](inline ifEmptyOrError: => B): B = if isSuccess then raw.asInstanceOf[A] else ifEmptyOrError

	inline def exists(inline predicate: A => Boolean): Boolean = isSuccess && predicate(raw.asInstanceOf[A])

	inline def is(other: AnyRef): Boolean =
		this.raw eq other

	inline def isEqualTo[A1 >: A](other: Trial[A1])(using CanEqual[A, A1]): Boolean = {
		if raw eq null then other.raw eq null
		else this.raw.equals(other.raw)
	}

	override def equals(other: Any): Boolean = {
		other.asMatchable match {
			case otherTrial: Trial[?] =>
				if raw eq null then otherTrial.raw eq null
				else raw.equals(otherTrial.raw)
			case _ => false
		}
	}

	/** @return `true` if [[isDefined]] and the contained value equals the specified one. */
	inline def containsNonStrict[A1 >: A](elem: A1): Boolean = {
		if isSuccess then raw.equals(elem.asInstanceOf[AnyRef])
		else false
	}

	/** @return `true` if [[isDefined]] and the contained value equals the specified one. */
	inline def contains[A1 >: A](elem: A1)(using CanEqual[A, A1]): Boolean = {
		if isSuccess then raw.equals(elem.asInstanceOf[AnyRef])
		else false
	}

	inline def fold[B](inline ifEmpty: => B)(inline onFailure: Throwable => B)(inline onSuccess: A => B): B = {
		if isEmpty then ifEmpty
		else {
			raw.asMatchable match {
				case Trial.Failure_Private(ex) => onFailure(ex)
				case _ => onSuccess(raw.asInstanceOf[A])
			}
		}
	}

	inline def foreach(inline consumer: A => Unit): Unit = if isSuccess then consumer(raw.asInstanceOf[A])

	inline def map[B <: AnyRef](inline f: A => B): Trial[B] = {
		if isSuccess then new Trial[B](f(raw.asInstanceOf[A]))
		else this.asInstanceOf[Trial[B]]
	}

	inline def flatMap[B](inline f: A => Trial[B]): Trial[B] = {
		if isSuccess then f(raw.asInstanceOf[A])
		else this.asInstanceOf[Trial[B]]
	}

	override def toString: String = {
		if isEmpty then "empty"
		else {
			raw.asMatchable match {
				case Trial.Failure_Private(ex) => s"failure($ex)"
				case _ => s"success($raw)"
			}
		}
	}
}

object Trial {
	/** Not private due to a compiler bug. Checked at v2.8.4 */
	final case class Failure_Private(exception: Throwable)

	val empty: Trial[Nothing] = new Trial(null)

	inline def success[A](value: A): Trial[A] = {
		val ref = value.asInstanceOf[AnyRef | Null]
		if ref eq null then throw new IllegalArgumentException("Trial.success cannot wrap null")
		else new Trial(ref)
	}

	inline def failure(ex: Throwable): Trial[Nothing] = new Trial(Failure_Private(ex))

	given [A, B] =>CanEqual[A, B] => CanEqual[Trial[A], Trial[B]] = CanEqual.derived
}
