package readren.matrix.cluster.service

import scribe.*
import scribe.file.*
import scribe.format.*
import scribe.format.FormatBlock.{ClassAndMethodName, ColumnNumber, LineNumber}
import scribe.output.{LogOutput, TextOutput}

import java.io.File

object ScribeTestConfig {
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

  def init(deleteLogFilesOnLaunch: Boolean = false): Unit = {
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
        formatter = mySourceLinkFormatter
      )
      .withHandler(
        minimumLevel = Some(Level.Trace),
        writer = FileWriter("logs" / ("app-" % year % "-" % month % "-" % day % ".log")),
        formatter = mySourceLinkFormatter
      )
      .replace()

    // Set debug level for Transmitter class
    Logger("readren.matrix.cluster.channel.Transmitter").withMinimumLevel(Level.Debug).replace()
    
    // Set debug level for Receiver class
    Logger("readren.matrix.cluster.channel.Receiver").withMinimumLevel(Level.Debug).replace()

    Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler {
      override def uncaughtException(t: Thread, e: Throwable): Unit =
        scribe.error(s"Uncaught exception in thread ${t.getName}:", e)
    })
  }
} 