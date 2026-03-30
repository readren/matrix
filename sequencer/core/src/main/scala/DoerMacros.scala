package readren.sequencer

import scala.quoted.{Expr, Quotes, Type}

object DoerMacros {


	def triggerImpl[A: Type](isWithinDoSerExExpr: Expr[Boolean], doerExpr: Expr[Doer], dutyExpr: Expr[Doer#Duty[A]], onCompleteExpr: Expr[A => Unit])(using quotes: Quotes): Expr[Unit] = {
		import quotes.reflect.*

		def runnable: Expr[Runnable] = {
			val pos: Position = onCompleteExpr.asTerm.pos
			val sourceInfo: Expr[String] = Expr(s".engage(${Printer.TreeShortCode.show(onCompleteExpr.asTerm)}) } @ ${pos.sourceFile.name}:${pos.startLine + 1}")

			'{
				new Runnable {
					override def run(): Unit = $dutyExpr.engagePortal($onCompleteExpr)

					override def toString: String = s"${$dutyExpr.toString}${$sourceInfo}"
				}
			}
		}

		isWithinDoSerExExpr.value match {
			case Some(isWithinDoSerEx) =>
				if isWithinDoSerEx then '{
					$doerExpr.checkWithin()
					$dutyExpr.engagePortal($onCompleteExpr)
				}
				else '{ $doerExpr.executeSequentially($runnable) }

			case None =>
				'{
					if $isWithinDoSerExExpr then {
						$doerExpr.checkWithin()
						$dutyExpr.engagePortal($onCompleteExpr)
					}
					else $doerExpr.executeSequentially($runnable)
				}
		}
	}


	/** This is a hacky version of triggerImpl that bypasses the path-dependent type checking.
	 * It is used by the [[Doer.trigger]] macro to avoid the path-dependent type checking.
	 * @note Not currently in use, but retained to demonstrate how to bypass path-dependent type checking. This serves as a template to replace [[triggerImpl]] if the compiler eventually disallows type projections on abstract types.
	 */
	def triggerImpl_hacked[A: Type](isWithinDoSerExExpr: Expr[Boolean], doerExpr: Expr[Doer], dutyExpr: Expr[Any], onCompleteExpr: Expr[A => Unit])(using quotes: Quotes): Expr[Unit] = {
		import quotes.reflect.*

		/** Builds a `Term` corresponding to `dutyExpr.engagePortal(onCompleteExpr)`. */
		def engagePortalCall: Term = {
			val dutyTerm = dutyExpr.asTerm
			val engagePortalSymbol = dutyTerm.tpe.typeSymbol.memberMethod("engagePortal").head
			Apply(Select(dutyTerm, engagePortalSymbol), List(onCompleteExpr.asTerm))
		}

		/** Builds an `Expr[String]` corresponding to `dutyExpr.toString`. */
		def dutyToStringExpr: Expr[String] = {
			val dutyTerm = dutyExpr.asTerm
			val toStringSymbol = dutyTerm.tpe.typeSymbol.memberMethod("toString").head
			Apply(Select(dutyTerm, toStringSymbol), Nil).asExprOf[String]
		}

		def runnable: Expr[Runnable] = {
			val pos: Position = onCompleteExpr.asTerm.pos
			val sourceInfo: Expr[String] = Expr(s".engage(${Printer.TreeShortCode.show(onCompleteExpr.asTerm)}) } @ ${pos.sourceFile.name}:${pos.startLine + 1}")
			val engageCall = engagePortalCall.asExprOf[Unit]
			val dts = dutyToStringExpr

			'{
				new Runnable {
					override def run(): Unit = $engageCall

					override def toString: String = s"${$dts}${$sourceInfo}"
				}
			}
		}

		isWithinDoSerExExpr.value match {
			case Some(isWithinDoSerEx) =>
				if isWithinDoSerEx then {
					val engageCall = engagePortalCall.asExprOf[Unit]
					'{
						$doerExpr.checkWithin()
						$engageCall
					}
				}
				else '{ $doerExpr.executeSequentially($runnable) }

			case None =>
				val engageCall = engagePortalCall.asExprOf[Unit]
				'{
					if $isWithinDoSerExExpr then {
						$doerExpr.checkWithin()
						$engageCall
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
