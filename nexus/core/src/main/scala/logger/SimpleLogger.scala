package readren.nexus
package logger

import core.{Logger, Inqueue}

import scribe.mdc.MDC

import java.net.URI

class SimpleLogger(aLevel: Logger.Level) extends Logger {
	
	override val level: Logger.Level = aLevel
	
	private val scribeLevel = aLevel match {
		case Logger.Level.debug => scribe.Level.Debug
		case Logger.Level.info => scribe.Level.Debug
		case Logger.Level.warn => scribe.Level.Debug
		case Logger.Level.error => scribe.Level.Debug
	}

	override val destination: Inqueue[String] = new Inqueue[String] {
		val mdc: scribe.mdc.MDC = summon[scribe.mdc.MDC]
		override def submit(message: String): Unit = scribe.log(scribeLevel, mdc, message)

		override def uri: URI = ???
	}
}
