package readren.common

import scala.compiletime.asMatchable

/** TODO: try defining it as `opaque type Maybe[+A] = A | Null` instead. */
final class Maybe[+A](private val value: AnyRef | Null) extends AnyVal {

	inline def isEmpty: Boolean = value eq null

	inline def isDefined: Boolean = value ne null

	inline def get: A =
		if isEmpty then throw new NoSuchElementException("Maybe.get on empty Maybe")
		else value.asInstanceOf[A]

	inline def foreach(inline f: A => Unit): Unit =
		if isDefined then f(value.asInstanceOf[A])

	inline def map[B](f: A => B): Maybe[B] =
		if isEmpty then Maybe.empty else Maybe.some(f(get))

	inline def flatMap[B](f: A => Maybe[B]): Maybe[B] =
		if isEmpty then Maybe.empty else f(value.asInstanceOf[A])

	inline def fold[B](inline ifEmpty: => B)(inline f: A => B): B =
		if isEmpty then ifEmpty else f(value.asInstanceOf[A])

	inline def getOrElse[B >: A](default: B): B =
		if isEmpty then default else value.asInstanceOf[A]

	inline def exists(inline predicate: A => Boolean): Boolean =
		isDefined && predicate(value.asInstanceOf[A])

	inline def isEqualTo[A1>:A](other: Maybe[A1])(using CanEqual[A, A1]): Boolean = {
		other.fold(this.isEmpty)(this.contains)
	}

	override def equals(other: Any): Boolean = {
		other.asMatchable match {
			case omb: Maybe[?] => this.fold(omb.isEmpty)(_.equals(omb.value))
			case _ => false
		}
	}

	/** @return `true` if [[isDefined]] and the contained value equals the specified one. */
	inline def containsNonStrict[A1 >: A](elem: A1): Boolean =
		if isEmpty then false else value.equals(elem.asInstanceOf[AnyRef])

	/** @return `true` if [[isDefined]] and the contained value equals the specified one. */
	inline def contains[A1 >: A](elem: A1)(using CanEqual[A, A1]): Boolean =
		if isEmpty then false else value.equals(elem.asInstanceOf[AnyRef])

	override def toString: String =
		if isEmpty then "empty" else s"some($value)"
}

object Maybe {

	val empty: Maybe[Nothing] = new Maybe(null)

	inline def apply[A](a: A | Null): Maybe[A] =
		new Maybe(a.asInstanceOf[AnyRef | Null])

	inline def some[A](a: A): Maybe[A] =
		val aRef = a.asInstanceOf[AnyRef | Null]
		if aRef eq null then throw new IllegalArgumentException("Maybe.some cannot wrap null")
		else new Maybe(aRef)

	def liftPartialFunction[A, B](pf: PartialFunction[A, B]): A => Maybe[B] =
		(a: A) => if pf.isDefinedAt(a) then some(pf.apply(a)) else empty

	given [A, B] => CanEqual[A, B] => CanEqual[Maybe[A], Maybe[B]] = CanEqual.derived
}