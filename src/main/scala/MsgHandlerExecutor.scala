package readren.matrix

import readren.taskflow.Doer

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

@deprecated("See the MsgHandlerExecutorManager's deprecation comment")
class MsgHandlerExecutor(override protected val assistant: Doer.Assistant) extends Doer {

	@volatile private var queuedExecutionsCounter: Int = 0
	private var ongoingExecutionsCounter: Int = 0

	inline def load: Int = {
		val queuedCounter = queuedExecutionsCounter
		queuedCounter + queuedCounter + ongoingExecutionsCounter
	}

	/** Returns a [[Task]] that, when executed, runs the received procedure. Similar to [[Task.mine]] */
	inline def executeMsgHandler[M](currentBehavior: Behavior[M], message: M): Task[HandleResult[M]] = {
		queuedExecutionsCounter += 1 // TODO avoid race condition here. Either with AtomicInteger or mutating it within an assigned MatrixAdmin. 
		new Monitored[M](currentBehavior, message)
	}

	final class Monitored[M](behavior: Behavior[M], message: M) extends Task[HandleResult[M]] {
		override protected def engage(onComplete: Try[HandleResult[M]] => Unit): Unit = {
			ongoingExecutionsCounter += 1
			val result =
				try Success(behavior.handle(message))
				finally {
					ongoingExecutionsCounter -= 1
					queuedExecutionsCounter -= 1
				}
			onComplete(result)
		}
		
		override def toString: String = readren.taskflow.deriveToString[Monitored[M]](this)
	}
}
