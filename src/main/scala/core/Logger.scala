package readren.matrix
package core

import core.Logger.Level

object Logger {
	enum Level { case debug, info, warn, error }
}

trait Logger {
	
	val level: Level
	
	val destination: Receiver[String]
	
	inline def debug(inline text: String): Unit = {
		if level.ordinal <= Level.debug.ordinal then destination.submit(text)   
	}
	inline def info(inline text: String): Unit = {
		if level.ordinal <= Level.info.ordinal then destination.submit(text)
	}
	inline def warn(inline text: String): Unit = {
		if level.ordinal <= Level.warn.ordinal then destination.submit(text)
	}
	inline def error(inline text: String): Unit = {
		if level.ordinal <= Level.error.ordinal then destination.submit(text)
	}
}
