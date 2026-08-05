package readren.common

import scala.compiletime.asMatchable

final class Trial[+A](val raw: AnyRef) extends AnyVal {

	inline def isEmpty: Boolean = raw eq Trial.EMPTY_INTERNAL

	inline def isDefined: Boolean = raw ne Trial.EMPTY_INTERNAL

	inline def isSuccess: Boolean = isDefined && !raw.isInstanceOf[Trial.Failure_Internal]

	inline def isFailure: Boolean = isDefined && raw.isInstanceOf[Trial.Failure_Internal]

	inline def getOrElse[B >: A](inline ifEmptyOrError: => B): B = if isSuccess then raw.asInstanceOf[A] else ifEmptyOrError

	inline def exists(inline predicate: A => Boolean): Boolean = isSuccess && predicate(raw.asInstanceOf[A])

	inline def is(other: AnyRef): Boolean =
		this.raw eq other

	inline def isEqualTo[A1 >: A](other: Trial[A1])(using CanEqual[A, A1]): Boolean = {
		(raw eq other.raw) || ((raw ne null) && raw.equals(other.raw))
	}

	override def equals(other: Any): Boolean = {
		other.asMatchable match {
			case otherTrial: Trial[?] => (raw eq otherTrial.raw) || ((raw ne null) && raw.equals(otherTrial.raw))
			case _ => false
		}
	}

	/** @return `true` if [[isDefined]] and the contained value equals the specified one. */
	inline def containsNonStrict[A1 >: A](elem: A1): Boolean = {
		if isSuccess then {
			val elemAsAnyRef = elem.asInstanceOf[AnyRef]
			(raw eq elemAsAnyRef) || ((raw ne null) && raw.equals(elemAsAnyRef))
		} else false
	}

	/** @return `true` if [[isDefined]] and the contained value equals the specified one. */
	inline def contains[A1 >: A](elem: A1)(using CanEqual[A, A1]): Boolean = containsNonStrict(elem)

	inline def fold[B](inline ifEmpty: => B)(inline onFailure: Throwable => B)(inline onSuccess: A => B): B = {
		if isEmpty then ifEmpty
		else {
			raw.asMatchable match {
				case Trial.Failure_Internal(ex) => onFailure(ex)
				case _ => onSuccess(raw.asInstanceOf[A])
			}
		}
	}

	inline def foreach(inline consumer: A => Unit): Unit = if isSuccess then consumer(raw.asInstanceOf[A])

	inline def map[B](inline f: A => B): Trial[B] = {
		if isSuccess then Trial.success(f(raw.asInstanceOf[A]))
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
				case Trial.Failure_Internal(ex) => s"failure($ex)"
				case _ => s"success($raw)"
			}
		}
	}
}

object Trial {
	/** Never access this class. Is public to avoid synthetic accessors. */
	final case class Failure_Internal(exception: Throwable)

	/** Never access this reference. Is public to avoid synthetic accessors. */
	final val EMPTY_INTERNAL = new AnyRef

	val empty: Trial[Nothing] = new Trial(EMPTY_INTERNAL)

	inline def success[A](value: A): Trial[A] = {
		val ref = value.asInstanceOf[AnyRef]
		if ref eq EMPTY_INTERNAL then throw new IllegalArgumentException("Trial.success cannot wrap EMPTY")
		else new Trial(ref)
	}

	inline def failure(ex: Throwable): Trial[Nothing] = new Trial(Failure_Internal(ex))

	given [A, B] =>CanEqual[A, B] => CanEqual[Trial[A], Trial[B]] = CanEqual.derived
}
