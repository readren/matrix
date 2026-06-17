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

	def executeSequentiallyImpl(doerExpr: Expr[Doer], procedureExpr: Expr[Unit])(using quotes: Quotes): Expr[Unit] = {
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

	def reportPanicExceptionImpl(doerExpr: Expr[Doer], panicExceptionExpr: Expr[Throwable])(using quotes: Quotes): Expr[Unit] = {
		import quotes.reflect.*
		// Capture the source code location.
		val pos = Position.ofMacroExpansion
		// Get the source code snippet from the source file at the specific line.
		val snippet = pos.sourceCode.getOrElse("Source code not available")
		// Build exception message.
		val message = Expr(s"Reported at ${pos.sourceFile.name}:${pos.startLine + 1} => $snippet")

		'{ $doerExpr.reportFailurePortal(new Doer.PanicException($message, $panicExceptionExpr)) }
	}
}
