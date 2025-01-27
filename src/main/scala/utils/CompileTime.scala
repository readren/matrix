package readren.matrix
package utils

import scala.quoted.*

object CompileTime {

	inline def getTypeName[T]: String = ${ getTypeNameImpl[T] }

	private def getTypeNameImpl[T: Type](using Quotes): Expr[String] = {
		import quotes.reflect.*
		val typeRepr = TypeRepr.of[T]
		val typeName = typeRepr.typeSymbol.name
		Expr(typeName)
	}
}