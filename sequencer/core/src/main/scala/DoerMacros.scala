package readren.sequencer

import readren.common.Maybe
import scala.quoted.{Expr, Quotes, Type}

object DoerMacros {


	inline def sourceInfo(inline onComplete: Any): String = {
		${ sourceInfoImpl('onComplete) }
	}

	def sourceInfoImpl(onCompleteExpr: Expr[Any])(using quotes: Quotes): Expr[String] = {
		import quotes.reflect.*
		val pos: Position = onCompleteExpr.asTerm.pos
		Expr(s".subscribe(${Printer.TreeShortCode.show(onCompleteExpr.asTerm)}) } @ ${pos.sourceFile.name}:${pos.startLine + 1}")
	}

	def runImpl(doerExpr: Expr[Doer], procedureExpr: Expr[Unit])(using quotes: Quotes): Expr[Unit] = {
		import quotes.reflect.*

		// Capture the source code location
		val pos: Position = procedureExpr.asTerm.pos
		// Build source info text.
		val sourceInfo = Expr(s"{ ${procedureExpr.asTerm.show} } @ ${pos.sourceFile.name}:${pos.startLine + 1}")
		//		val sourceInfo = Expr(s"{ ${pos.sourceCode.getOrElse("not available")} } @ ${pos.sourceFile.name}:${pos.startLine + 1}")

		val runnable: Expr[Runnable] = '{
			new Runnable {
				override def run(): Unit = $procedureExpr

				override def toString: String = $sourceInfo
			}
		}
		// Call the `executeSequentially` method with the new wrapped `Runnable`
		'{ $doerExpr.executeSequentially($runnable) }
	}


}
