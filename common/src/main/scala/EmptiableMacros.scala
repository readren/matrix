package readren.common

import scala.quoted.Quotes
import scala.quoted.*

object EmptiableMacros {
	def applyImpl[A: Type](a: Expr[A | Null])(using Quotes): Expr[Emptiable[A]] = {
		checkNotMaybe[A]
		'{ new Emptiable($a.asInstanceOf[AnyRef]) }
	}

	def someImpl[A: Type](a: Expr[A])(using Quotes): Expr[Emptiable[A]] = {
		checkNotMaybe[A]
		'{
			val ref = $a.asInstanceOf[AnyRef | Null]
			if ref eq null then throw new IllegalArgumentException("The argument of `Emptiable.some` cannot be `null`")
			else new Emptiable(ref)
		}
	}

	private def checkNotMaybe[A: Type](using Quotes): Unit = {
		import quotes.reflect.*

		val widened = TypeRepr.of[A].dealias.widen
		if widened <:< TypeRepr.of[Maybe[Any]] then report.errorAndAbort("The type argument of `Emptiable` factory methods cannot be a `Maybe` because `Maybe.empty` evaluates to raw `null` at runtime, corrupting the empty state.")
	}
}
