package readren.sequencer
package providers

import readren.common.CompileTime

class DefaultDoerUnhandledExceptionReporter extends ((Doer, Throwable) => Unit) {
	override def apply(doer: Doer, e: Throwable): Unit = {
		val message = s"The doer tagged with `${doer.tag}` encountered an unhandled exception, which propagated to the top of the stack without being caught. As a result, the ${CompileTime.getTypeName[DoerProvider[?]]} restarted the worker that was executing it with a fresh thread."
		scribe.error(message, e)
	}
}

