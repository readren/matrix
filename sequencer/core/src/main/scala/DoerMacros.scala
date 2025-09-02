package readren.sequencer

import scala.quoted.{Expr, Quotes, Type}

object DoerMacros {


	def triggerImpl[A: Type](isWithinDoSiThExExpr: Expr[Boolean], doerExpr: Expr[Doer], dutyExpr: Expr[Doer#Duty[A]], onCompleteExpr: Expr[A => Unit])(using quotes: Quotes): Expr[Unit] = {
		import quotes.reflect.*

		def runnable: Expr[Runnable] = {
			val pos: Position = onCompleteExpr.asTerm.pos
			val sourceInfo: Expr[String] = Expr(s".engage(${Printer.TreeShortCode.show(onCompleteExpr.asTerm)}) } @ ${pos.sourceFile.name}:${pos.startLine + 1}")

			'{
				new Runnable {
					override def run(): Unit = $dutyExpr.engagePortal($onCompleteExpr)

					override def toString: String = s"{ ${$dutyExpr.toString}${$sourceInfo}"
				}
			}
		}

		isWithinDoSiThExExpr.value match {
			case Some(isWithinDoSiThEx) =>
				if isWithinDoSiThEx then '{
					assert($doerExpr.isInSequence, s"The current thread does not correspond to this doer: this=${$doerExpr}, current=${$doerExpr.current}")
					$dutyExpr.engagePortal($onCompleteExpr)
				}
				else '{ $doerExpr.executeSequentially($runnable) }

			case None =>
				'{
					if $isWithinDoSiThExExpr then {
						assert($doerExpr.isInSequence, s"The current thread does not correspond to this doer: this=${$doerExpr}, current=${$doerExpr.current}")
						$dutyExpr.engagePortal($onCompleteExpr)
					}
					else $doerExpr.executeSequentially($runnable)
				}
		}
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
