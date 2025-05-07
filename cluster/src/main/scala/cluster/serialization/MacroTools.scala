package readren.matrix
package cluster.serialization

import scala.quoted.{Expr, Quotes, Type}

object MacroTools {
	inline def printMacroExpansion[T](inline expr: T): T = ${ printMacroExpansionImpl[T]('expr) }
	
	private def printMacroExpansionImpl[T: Type](expr: Expr[T])(using quotes: Quotes): Expr[T] = {
		import quotes.reflect.*
		val pos = Position.ofMacroExpansion
		println(s"""\nMacro expansion of `${pos.sourceCode.fold(Type.show[T])(identity)}` at ${pos.sourceFile}:${pos.startLine}:${pos.startColumn} <<<<""")
		println(expr.show)
		println(s"\n>>>>")
		expr
	}
}
