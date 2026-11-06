package readren.common

import scala.annotation.tailrec

object ToolsMacro {

	import scala.quoted.*

	def deriveToStringImpl[T: Type](productExpr: Expr[T])(using Quotes): Expr[String] = {
		import quotes.reflect.*

		// Get the type and symbol of T
		val productSymbol = TypeRepr.of[T].typeSymbol
		val primaryConstructor = productSymbol.primaryConstructor
		// Ensure that T is a class with a primary constructor
		if !productSymbol.isClassDef || !primaryConstructor.exists then {
			report.errorAndAbort("`deriveToString` can only be applied to classes with a primary constructor")
		}

		val productTerm = productExpr.asTerm

		@tailrec
		def unwrap(term: Term): Term = term match {
			case Inlined(_, Nil, body) => unwrap(body)
			case Typed(expr, _) => unwrap(expr)
			case _ => term
		}

		val unwrappedTerm = unwrap(productTerm)
		unwrappedTerm match {
			case This(_) => // valid target
			case _ =>
				report.errorAndAbort(
					s"`deriveToString` must only be called with `this` (or an enclosing self-type alias) inside a class definition, but received: `${unwrappedTerm.show}`"
				)
		}

		// Create a new StringBuilder instance
		var stringBuilderExpr = '{ new StringBuilder(${ Expr[String](productSymbol.name) }) }

		for {
			paramsList <- primaryConstructor.paramSymss
			if paramsList.forall(_.isTerm) // skips type parameters
		} do {
			stringBuilderExpr = '{ $stringBuilderExpr.append('(') }
			var isFirst = true
			// The next line can be replaced by `for param <- paramsList if !param.info.isFunctionType do {` if the experimental compiler option is set.
			for param <- paramsList if !TypeRepr.of[T].memberType(param).isFunctionType do {
				val paramName = param.name
				val select = Select.unique(unwrappedTerm, paramName)
				val argValueExpr: Expr[Any] = select.asExpr

				if isFirst then isFirst = false else stringBuilderExpr = '{ $stringBuilderExpr.append(", ") }
				stringBuilderExpr = '{ $stringBuilderExpr.append(${ Expr(paramName) }).append('=').append($argValueExpr.toString) }
			}
			stringBuilderExpr = '{ $stringBuilderExpr.append(')') }
		}
		'{ $stringBuilderExpr.toString }
	}
}
