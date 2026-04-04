package readren.common

import scribe.format.FormatBlock
import scribe.mdc.MDC
import scribe.output.{EmptyOutput, LogOutput, TextOutput}
import scribe.{LogFeature, LogRecord, Logger}

//	override def toString: String = stack.reverseIterator.mkString(">>")



object Trace {
	opaque type TraceContext = List[String]

	object format extends FormatBlock {
		override def format(record: LogRecord): LogOutput = {
			record.data.get("traceCtx") match {
				case Some(ctx) => TextOutput(StringBuilder().appendTrace(ctx().asInstanceOf[TraceContext]).result())
				case None => EmptyOutput
			}
		}
	}

	extension (sb: StringBuilder) {
		def appendTrace(tc: TraceContext): sb.type = {
			tc match {
				case Nil => sb
				case head :: Nil => sb.append(head)
				case head :: tail => appendTrace(tail).append("/").append(head)
			}
		}
	}

	inline def step[A](node: String)(inline block: TraceContext ?=> A)(using ctx: TraceContext): A = {
		val localCtx = node :: ctx
		block(using localCtx)
	}

	inline def init[A](root: String)(inline block: TraceContext ?=> A): A = {
		block(using root :: Nil)
	}

	inline def logger(using ctx: TraceContext): Logger =
		Logger.root.set("traceCtx", ctx)

	inline def trace(features: LogFeature*)(using ctx: TraceContext): Unit =
		logger.trace(features *)

	inline def debug(features: LogFeature*)(using ctx: TraceContext): Unit =
		logger.debug(features *)

	inline def info(features: LogFeature*)(using ctx: TraceContext): Unit =
		logger.info(features *)

	inline def warn(features: LogFeature*)(using ctx: TraceContext): Unit =
		logger.warn(features *)

	inline def error(features: LogFeature*)(using ctx: TraceContext): Unit =
		logger.error(features *)

	inline def fatal(features: LogFeature*)(using ctx: TraceContext): Unit =
		logger.fatal(features *)
}
