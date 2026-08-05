package readren.common

import scala.annotation.tailrec
import scala.quoted.*

object MaybeMacros {

	def applyImpl[A: Type](a: Expr[A | Null])(using Quotes): Expr[Maybe[A]] = {
		abortIfNullableType[A]

		'{ $a.asInstanceOf[Maybe[A]] }
	}

	def someImpl[A: Type](a: Expr[A])(using Quotes): Expr[Maybe[A]] = {
		import quotes.reflect.*

		abortIfNullableType[A]

		val aTerm = a.asTerm
		if isLiteralNull(aTerm) then report.errorAndAbort("The argument passed to `Maybe.some` cannot be `null`.")
		val aTermType = aTerm.tpe.dealias.widen
		if aTermType <:< TypeRepr.of[Null] then report.errorAndAbort("The argument passed to `Maybe.some` cannot have `Null` type.")
		checkIfNullable(aTermType, false, s"Based on its type, the argument passed to `Maybe.some` is potentially `null` (or `Maybe.empty`), which is illegal. The argument's declared type is: `${aTermType.show.replace("Maybe$package.Maybe", "Maybe")}`")

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

	def mapImpl[A: Type, B: Type](self: Expr[Maybe[A]], f: Expr[A => B])(using Quotes): Expr[Maybe[B]] = {
		abortIfNullableType[B]

		'{ if $self.isEmpty then Maybe.empty else $f($self.get).asInstanceOf[Maybe[B]] }
	}

	def flatMapImpl[A: Type, B: Type](self: Expr[Maybe[A]], f: Expr[A => Maybe[B]])(using Quotes): Expr[Maybe[B]] = {
		abortIfNullableType[B]

		'{ if $self.isEmpty then Maybe.empty else $f($self.get) }
	}

	private def abortIfNullableType[A: Type](using Quotes): Unit = {
		import quotes.reflect.*

		val tpe = TypeRepr.of[A].dealias.widen
		if tpe <:< TypeRepr.of[Null] && !(tpe =:= TypeRepr.of[Nothing]) then report.errorAndAbort("The type argument of `Maybe` cannot be `Null`.")
		checkIfNullable(tpe, true, s"`Maybe`'s type argument cannot be a nullable type (e.g. `T | Null`, `Maybe[T]`); and it is (`${tpe.show}`).")
	}

	private def checkIfNullable(using Quotes)(tpe: quotes.reflect.TypeRepr, abortOrWarn: Boolean, message: String): Unit = {
		import quotes.reflect.*

		tpe.dealias.widen match {
			case OrType(lhs, rhs) =>
				if (lhs <:< TypeRepr.of[Null]) && !(lhs =:= TypeRepr.of[Nothing]) || (rhs <:< TypeRepr.of[Null]) && !(rhs =:= TypeRepr.of[Nothing]) then if abortOrWarn then report.errorAndAbort(message) else report.warning(message)
				checkIfNullable(lhs, abortOrWarn, message)
				checkIfNullable(rhs, abortOrWarn, message)

			case widened =>
				if (widened <:< TypeRepr.of[Maybe[Any]]) && !(widened =:= TypeRepr.of[Nothing]) then if abortOrWarn then report.errorAndAbort(message) else report.warning(message)
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