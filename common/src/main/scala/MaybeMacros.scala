package readren.common

import scala.annotation.tailrec
import scala.quoted.*

object MaybeMacros {

	def applyImpl[A: Type](a: Expr[A | Null])(using Quotes): Expr[Maybe[A]] = {
		import quotes.reflect.*

		abortIfNullableType[A]
		abortIfNested(a.asTerm.tpe.dealias.widen, false, "The argument passed to `Maybe.apply` cannot have a nullable type (e.g. `T | Null` or `Maybe[T]`)")

		'{ $a.asInstanceOf[Maybe[A]] }
	}

	def someImpl[A: Type](a: Expr[A])(using Quotes): Expr[Maybe[A]] = {
		import quotes.reflect.*

		abortIfNullableType[A]

		val aTerm = a.asTerm
		if isLiteralNull(aTerm) then report.errorAndAbort("The argument passed to `Maybe.some` cannot be `null`.")
		val aTermType = aTerm.tpe.dealias.widen
		if aTermType <:< TypeRepr.of[Null] then report.errorAndAbort("The argument passed to `Maybe.some` cannot have `Null` type.")
		abortIfNested(aTermType, true, "The argument passed to `Maybe.some` cannot have a nullable type (e.g. `T | Null` or `Maybe[T]`).")

		'{
			val ref = $a.asInstanceOf[AnyRef | Null]
			if ref eq null then throw new IllegalArgumentException("Maybe.some cannot wrap null")
			else $a.asInstanceOf[Maybe[A]]
		}
	}

	def liftPartialFunctionImpl[A: Type, B: Type](pf: Expr[PartialFunction[A, B]])(using Quotes): Expr[A => Maybe[B]] = {
		abortIfNullableType[B]

		'{ (a: A) => if $pf.isDefinedAt(a) then Maybe.apply($pf.apply(a)) else Maybe.empty }
	}

	private def abortIfNullableType[A: Type](using Quotes): Unit = {
		import quotes.reflect.*

		val tpe = TypeRepr.of[A].dealias.widen
		if tpe <:< TypeRepr.of[Null] then report.errorAndAbort("The type argument of `Maybe` cannot be `Null`.")
		abortIfNested(tpe, true, "`Maybe`'s type argument cannot be a nullable type (e.g. `T | Null`, `Maybe[T]`)")
	}

	private def abortIfNested(using Quotes)(tpe: quotes.reflect.TypeRepr, prohibitUnionWithNull: Boolean, message: String): Unit = {
		import quotes.reflect.*

		tpe.dealias.widen match {
			case OrType(lhs, rhs) =>
				if prohibitUnionWithNull && (lhs <:< TypeRepr.of[Null] || rhs <:< TypeRepr.of[Null]) then report.errorAndAbort(message)
				abortIfNested(lhs, prohibitUnionWithNull, message)
				abortIfNested(rhs, prohibitUnionWithNull, message)

			case widened =>
				if widened <:< TypeRepr.of[Maybe[Any]] then report.errorAndAbort(message)
		}
	}

	@tailrec
	private def isLiteralNull(using Quotes)(term: quotes.reflect.Term): Boolean = {
		import quotes.reflect.*
		term match {
			case Literal(NullConstant()) => true
			case Inlined(_, _, body) => isLiteralNull(body)
			case Typed(expr, _) => isLiteralNull(expr)
			case _ => false
		}
	}
}