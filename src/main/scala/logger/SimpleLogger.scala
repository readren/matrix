package readren.matrix
package logger

import core.{Logger, Receiver}

import java.net.URI

class SimpleLogger(aLevel: Logger.Level) extends Logger {
	
	override val level: Logger.Level = aLevel
	
	override val destination: Receiver[String] = new Receiver[String] {
		override def submit(message: String): Unit = println(message)

		override def uri: URI = ???
	}
}
