package readren.matrix
package cluster.serialization

import scala.quoted.{Expr, Quotes, Type}

object MacroTools {
	inline def doAndPrintCode[T](inline expr: T): T = ${ doAndPrintCodeImpl[T]('expr) }
	
	private def doAndPrintCodeImpl[T: Type](expr: Expr[T])(using quotes: Quotes): Expr[T] = {
		import quotes.reflect.*
		val pos = Position.ofMacroExpansion
		println(s"""\nCode of `${pos.sourceCode.fold(Type.show[T])(identity)}` at ${pos.sourceFile}:${pos.startLine}:${pos.startColumn} <<<<""")
//		println(expr.show)
		println(expr.asTerm.show(using Printer.TreeShortCode))
		println(s"\n>>>>")
		expr
	}
}
