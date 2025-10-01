package readren.nexus
package cluster.serialization

import scala.quoted.{Expr, Quotes, Type}

object MacroTools {

	/**
	 * Behaves like the identity function at runtime but prints the code of the received argument at compile time.
	 */
	inline def showCode[T](inline expr: T): T = ${ showCodeImpl[T]('expr) }
	
	private def showCodeImpl[T: Type](expr: Expr[T])(using quotes: Quotes): Expr[T] = {
		import quotes.reflect.*
		val pos = Position.ofMacroExpansion
		println(s"""\nCode of `${pos.sourceCode.fold(Type.show[T])(identity)}` at ${pos.sourceFile}:${pos.startLine}:${pos.startColumn} <<<<""")
//		println(expr.show)
		println(expr.asTerm.show(using Printer.TreeShortCode))
		println(s"\n>>>>")
		expr
	}
}
