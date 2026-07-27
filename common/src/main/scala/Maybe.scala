package readren.common

import scala.compiletime.asMatchable

opaque type Maybe[+A] = A | Null

object Maybe {

	extension [A](self: Maybe[A]) {

		inline def value: AnyRef = self.asInstanceOf[AnyRef]

		inline def isEmpty: Boolean = value eq null

		inline def isDefined: Boolean = value ne null

		inline def get: A = self.asInstanceOf[A]

		inline def foreach(inline f: A => Unit): Unit = if isDefined then f(get)

		inline def map[B](inline f: A => B): Maybe[B] = if isEmpty then null else f(get)

		inline def flatMap[B](inline f: A => Maybe[B]): Maybe[B] = if isEmpty then null else f(get)

		inline def fold[B](inline ifEmpty: => B)(inline f: A => B): B = if isEmpty then ifEmpty else f(get)

		inline def orElse[B >: A](inline maybeB: => Maybe[B]): Maybe[B] = if isEmpty then maybeB else self

		inline def getOrElse[B >: A](inline default: => B): B = if isEmpty then default else get

		inline def exists(inline predicate: A => Boolean): Boolean = isDefined && predicate(get)

		inline def is(other: AnyRef): Boolean = value eq other

		inline def isEqualTo[A1 >: A](other: Maybe[A1])(using CanEqual[A, A1]): Boolean = {
			if self.value eq null then other.value eq null
			else self.value.equals(other.value)
		}

		inline def ==(other: AnyRef): Boolean = {
			other.asMatchable match {
				case omb: Maybe[?] =>
					if this.value eq null then omb.value eq null
					else this.value.equals(omb.value)
				case _ => false
			}
		}

		/** @return `true` if [[isDefined]] and the contained value equals the specified one. */
		inline def containsNonStrict[A1 >: A](elem: A1): Boolean = if isEmpty then false else self.asInstanceOf[AnyRef].equals(elem.asInstanceOf[AnyRef])

		/** @return `true` if [[isDefined]] and the contained value equals the specified one. */
		inline def contains[A1 >: A](elem: A1)(using CanEqual[A, A1]): Boolean = if isEmpty then false else self.asInstanceOf[AnyRef].equals(elem.asInstanceOf[AnyRef])
	}

	inline def empty: Maybe[Nothing] = null

	/** Creates a [[Maybe]] wrapping the specified value. \
	 * May require an explicit non-nullable type parameter to compile. \
	 * Warning: Avoid passing opaque types that represent nullable values (such as `T | Null`), as [[Maybe]] cannot encode nested nullability. \
	 * @param a the value to wrap into a [[Maybe]].
	 * @tparam A a non-nullable type.
	 * @return a [[Maybe]] containing `a`. */
	inline def apply[A](inline a: A | Null): Maybe[A] = ${ MaybeMacros.applyImpl('a) }

	/** Creates a [[Maybe]] containing a strictly non-null value.\
	 * Throws [[IllegalArgumentException]] at runtime if `a` evaluates to `null`. \
	 * Warning: Avoid passing opaque types that represent nullable values (such as `T | Null`), as passing `null` will cause a runtime exception.
	 * @param a the non-null value to wrap into a [[Maybe]].
	 * @tparam A a non-nullable type.
	 * @return a [[Maybe]] containing `a`. */
	inline def some[A](inline a: A): Maybe[A] = ${ MaybeMacros.someImpl('a) }

	/** Lifts a [[PartialFunction]] into a function returning [[Maybe]]. \
	 * Warning: Avoid lifting functions whose result type `B` represents nullable values (such as `T | Null`), as [[Maybe]] cannot encode nested nullability.
	 * @param pf the partial function to lift.
	 * @tparam B a non-nullable type.
	 * @return a function `A => Maybe[B]` returning [[Maybe.empty]] when `pf` is undefined. */
	inline def liftPartialFunction[A, B](inline pf: PartialFunction[A, B]): A => Maybe[B] = ${ MaybeMacros.liftPartialFunctionImpl[A, B]('pf) }

	given [A, B] =>CanEqual[A, B] => CanEqual[Maybe[A], Maybe[B]] = CanEqual.derived
}