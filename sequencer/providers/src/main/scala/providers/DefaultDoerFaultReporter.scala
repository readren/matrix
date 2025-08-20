package readren.sequencer
package providers

import readren.common.CompileTime

/** @param isPanic determines the message reported: true for panic exceptions, and false for unhandled exceptions. */
class DefaultDoerFaultReporter(isPanic: Boolean) extends ((Doer, Throwable) => Unit) {
	override def apply(doer: Doer, e: Throwable): Unit = {
		val message =
			if isPanic then s"A side-effect-only operand function passed to an operation of the doer tagged with `${doer.tag}` terminated abruptly. Chained operations are not affected but the side-effect will be incomplete."
			else s"The doer tagged with `${doer.tag}` encountered an unhandled exception, which propagated to the top of the stack without being caught. As a result, the ${CompileTime.getTypeName[DoerProvider[?]]} restarted the worker that was executing it with a fresh thread."
		scribe.error(message, e)
	}
}

