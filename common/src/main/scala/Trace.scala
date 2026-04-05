package readren.common

import scribe.format.FormatBlock
import scribe.mdc.MDC
import scribe.output.{EmptyOutput, LogOutput, TextOutput}
import scribe.{LogFeature, LogRecord, Logger}

object Trace {
	opaque type Context = List[String | (() => String)]

	object format extends FormatBlock {
		override def format(record: LogRecord): LogOutput =
			record.data.get("traceCtx") match {
				case Some(ctx) => TextOutput(StringBuilder(128, " ").appendTrace(ctx().asInstanceOf[Context]).append('|').result())
				case None => EmptyOutput
			}
	}

	extension (sb: StringBuilder) {
		def appendTrace(tc: Context): sb.type = {
			if tc.isEmpty then sb
			else {
				val text = tc.head match {
					case s: String => s
					case f: (() => String) => f()
				}
				val tail = tc.tail
				if tail.isEmpty then sb.append(text)
				else appendTrace(tail).append("/").append(text)
			}
		}
	}

	inline def step[A](node: String | (() => String))(inline block: Context ?=> A)(using ctx: Context): A =
		block(using node :: ctx)

	inline def init[A](root: String | (() => String))(inline block: Context ?=> A): A =
		block(using root :: Nil)

	inline def logger(using ctx: Context): Logger =
		Logger.root.set("traceCtx", ctx)

	inline def trace(features: LogFeature*)(using ctx: Context): Unit =
		logger.trace(features *)

	inline def debug(features: LogFeature*)(using ctx: Context): Unit =
		logger.debug(features *)

	inline def info(features: LogFeature*)(using ctx: Context): Unit =
		logger.info(features *)

	inline def warn(features: LogFeature*)(using ctx: Context): Unit =
		logger.warn(features *)

	inline def error(features: LogFeature*)(using ctx: Context): Unit =
		logger.error(features *)

	inline def fatal(features: LogFeature*)(using ctx: Context): Unit =
		logger.fatal(features *)
}
