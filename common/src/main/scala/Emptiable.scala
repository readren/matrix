package readren.common

import scala.compiletime.asMatchable

/** An alternative to [[Maybe]] that supports nullables and nesting. */
final class Emptiable[+A](val value: AnyRef | Null) extends AnyVal {

	inline def isEmpty: Boolean = value eq null

	inline def isDefined: Boolean = value ne null

	inline def get: A = value.asInstanceOf[A]

	inline def foreach(inline f: A => Unit): Unit = if isDefined then f(value.asInstanceOf[A])

	inline def map[B](inline f: A => B): Emptiable[B] = if isEmpty then Emptiable.empty else Emptiable.some(f(get))

	inline def flatMap[B](inline f: A => Emptiable[B]): Emptiable[B] = if isEmpty then Emptiable.empty else f(value.asInstanceOf[A])

	inline def fold[B](inline ifEmpty: => B)(inline f: A => B): B = if isEmpty then ifEmpty else f(value.asInstanceOf[A])

	inline def orElse[B >: A](inline maybeB: => Emptiable[B]): Emptiable[B] = if isEmpty then maybeB else this

	inline def getOrElse[B >: A](inline default: => B): B = if isEmpty then default else value.asInstanceOf[A]

	inline def exists(inline predicate: A => Boolean): Boolean = isDefined && predicate(value.asInstanceOf[A])

	inline def is(other: AnyRef): Boolean = this.value eq other

	inline def isEqualTo[A1 >: A](other: Emptiable[A1])(using CanEqual[A, A1]): Boolean = {
		if this.value eq null then other.value eq null
		else this.value.equals(other.value)
	}

	override def equals(other: Any): Boolean = {
		other.asMatchable match {
			case omb: Emptiable[?] =>
				if this.value eq null then omb.value eq null
				else this.value.equals(omb.value)
			case _ => false
		}
	}

	/** @return `true` if [[isDefined]] and the contained value equals the specified one. */
	inline def containsNonStrict[A1 >: A](elem: A1): Boolean = if isEmpty then false else value.equals(elem.asInstanceOf[AnyRef])

	/** @return `true` if [[isDefined]] and the contained value equals the specified one. */
	inline def contains[A1 >: A](elem: A1)(using CanEqual[A, A1]): Boolean = if isEmpty then false else value.equals(elem.asInstanceOf[AnyRef])

	override def toString: String = if isEmpty then "empty" else s"some($value)"
}

object Emptiable {

	val empty: Emptiable[Nothing] = new Emptiable(null)

	inline def apply[A](inline a: A | Null): Emptiable[A] = ${ EmptiableMacros.applyImpl('a) }

	inline def some[A](inline a: A): Emptiable[A] = ${ EmptiableMacros.someImpl('a) }

	def liftPartialFunction[A, B](pf: PartialFunction[A, B]): A => Emptiable[B] = (a: A) => if pf.isDefinedAt(a) then Emptiable.some(pf.apply(a)) else empty

	given [A, B] =>CanEqual[A, B] => CanEqual[Emptiable[A], Emptiable[B]] = CanEqual.derived
}