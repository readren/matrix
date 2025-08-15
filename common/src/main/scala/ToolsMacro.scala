package readren.common

object ToolsMacro {

	import scala.quoted.*

	def deriveToStringImpl[T: Type](productExpr: Expr[T])(using Quotes): Expr[String] = {
		import quotes.reflect.*

		// Get the type and symbol of T
		val productSymbol = TypeRepr.of[T].typeSymbol
		val primaryConstructor = productSymbol.primaryConstructor
		// Ensure that T is a class with a primary constructor
		if !productSymbol.isClassDef || !primaryConstructor.exists then
			report.errorAndAbort("`deriveToString` can only be applied to classes with a primary constructor")

		val productTerm = productExpr.asTerm
		// Create a new StringBuilder instance
		var stringBuilderExpr = '{ new StringBuilder(${ Expr[String](productSymbol.name) }) }

		for {
			paramsList <- primaryConstructor.paramSymss
			if paramsList.forall(_.isTerm) // skips type parameters
		} do {
			stringBuilderExpr = '{ $stringBuilderExpr.append('(') }
			var isFirst = true
			for param <- paramsList if !param.info.isFunctionType do {
				val paramName = param.name
				val select = Select.unique(productTerm, paramName)
				val argValueExpr: Expr[Any] = select.asExpr

				if isFirst then isFirst = false else stringBuilderExpr = '{ $stringBuilderExpr.append(", ") }
				stringBuilderExpr = '{ $stringBuilderExpr.append(${ Expr(paramName) }).append('=').append($argValueExpr.toString) }
			}
			stringBuilderExpr = '{ $stringBuilderExpr.append(')') }
		}
		'{ $stringBuilderExpr.toString }
	}
}
