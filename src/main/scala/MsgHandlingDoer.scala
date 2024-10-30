package readren.matrix

import readren.taskflow.Doer

import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

class MsgHandlingDoer(assistant: Doer.Assistant) extends Doer(assistant) {

	@volatile private var queuedExecutionsCounter: Int = 0
	private var ongoingExecutionsCounter: Int = 0

	inline def load: Int = {
		val queuedCounter = queuedExecutionsCounter
		queuedCounter + queuedCounter + ongoingExecutionsCounter
	}

	/** Returns a [[Task]] that, when executed, does the received procedure. Similar to [[Task.mine]] */
	inline def processes[A](procedure: => A): Task[A] = {
		queuedExecutionsCounter += 1
		new Monitored[A](procedure)
	}

	final class Monitored[A](procedure: => A) extends Task[A] {
		override protected def engage(onComplete: Try[A] => Unit): Unit = {
			ongoingExecutionsCounter += 1
			val result =
				try Success(procedure)
				catch {
					case NonFatal(cause) => Failure(cause)
				}
				finally {
					ongoingExecutionsCounter -= 1
					queuedExecutionsCounter -= 1
				}
			onComplete(result)
		}
	}
}
