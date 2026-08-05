package readren.sequencer

import GeneratorsForDoerTests.FaultyValue

import java.util.concurrent.{Executors, ThreadFactory}

class TestThreadFactory extends ThreadFactory {
	private val defaultFactory = Executors.defaultThreadFactory()

	// Pattern to match test-generated random exception messages: "Type: [random9char]"
	private val simulatedMessagePattern = ".*: \\[[a-zA-Z]{9}\\]".r

	private def isSimulatedException(e: Throwable): Boolean = {
		e.isInstanceOf[FaultyValue[?]] || ((e.getMessage ne null) && e.getMessage.startsWith("Simulated"))
	}

	override def newThread(r: Runnable): Thread = {
		val newThread = defaultFactory.newThread(r)
		newThread.setUncaughtExceptionHandler((thread, exception) => {
			if (!isSimulatedException(exception)) {
				val defaultHandler = Thread.getDefaultUncaughtExceptionHandler
				if (defaultHandler != null) {
					defaultHandler.uncaughtException(thread, exception)
				} else {
					exception.printStackTrace()
				}
			}
			// If it is simulated, we suppress the printing (do nothing)
		})
		newThread
	}

}
