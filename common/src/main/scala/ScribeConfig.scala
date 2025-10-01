package readren.common

import scribe.*
import scribe.file.*
import scribe.format.*
import scribe.format.FormatBlock.{ClassAndMethodName, ColumnNumber, LineNumber}
import scribe.output.{LogOutput, TextOutput}

import java.io.File

object ScribeConfig {
	object myPosition extends FormatBlock {
		override def format(record: LogRecord): LogOutput = {
			val line = if (record.line.nonEmpty) {
				s":${LineNumber.format(record).plainText}"
			} else {
				""
			}
			val column = if (record.column.nonEmpty) {
				s":${ColumnNumber.format(record).plainText}"
			} else {
				""
			}
			val fileName = record.fileName
			new TextOutput(s"at ${ClassAndMethodName.format(record).plainText}($fileName$line$column)")
		}
	}

	val mySourceLinkFormatter: Formatter = Formatter.fromBlocks(
		groupBySecond(
			cyan(bold(dateFull)),
			space,
			italic(threadName),
			space,
			levelColored,
			space,
			green(myPosition),
			newLine
		),
		multiLine(messages),
		multiLine(mdcMultiLine)
	)


	/**
	 * Initialize the Scribe logging configuration.
	 *
	 * @param useSimpleFormatter If true, use Formatter.simple (default). If false, use mySourceLinkFormatter.
	 * @param deleteLogFilesOnLaunch If true, delete previous log files at launch.
	 */
	def init(useSimpleFormatter: Boolean = true, deleteLogFilesOnLaunch: Boolean = false): Unit = {
		val formatter = if useSimpleFormatter then formatter"${format.timeStamp} ${format.messages}${format.mdc}"
		else mySourceLinkFormatter

		if deleteLogFilesOnLaunch then {
			// Delete previous log files at launch
			val logsDir = new File("logs")
			if (logsDir.exists && logsDir.isDirectory) {
				logsDir.listFiles().filter(f => f.isFile && f.getName.startsWith("app-") && f.getName.endsWith(".log")).foreach(_.delete())
			}
		}

		Logger.root
			.clearHandlers()
			.withMinimumLevel(Level.Trace)
			.withHandler(
				minimumLevel = Some(Level.Trace),
				formatter = formatter
			)
			.withHandler(
				minimumLevel = Some(Level.Trace),
				writer = FileWriter("logs" / ("app-" % year % "-" % month % "-" % day % ".log")),
				formatter = formatter
			)
			.replace()

		// Set debug level for Transmitter class
		Logger("readren.nexus.cluster.channel.Transmitter").withMinimumLevel(Level.Debug).replace()

		// Set debug level for Receiver class
		Logger("readren.nexus.cluster.channel.Receiver").withMinimumLevel(Level.Debug).replace()

		Thread.setDefaultUncaughtExceptionHandler((t: Thread, e: Throwable) => scribe.error(s"Uncaught exception in thread ${t.getName}:", e))
	}
} 