package readren.matrix

import java.util.concurrent.ForkJoinPool
import scala.util.control.NonFatal

class MsgHandlerExecutorService {

	val forkJoinPool:  ForkJoinPool = new ForkJoinPool()

	def executeMsgHandler[M](behavior: Behavior[M], message: M)(onComplete: ProcessMsgResult[M] => Unit): Unit = {
		 
		forkJoinPool.execute(new Runnable {
			override def run(): Unit = {
				val result: ProcessMsgResult[M] =
					try ContinueWith[M](behavior.handle(message))
					catch {
						// TODO: analyze if only catching non-fatal exceptions is OK. Note that fatal-exception cause the task to never complete.
						case NonFatal(cause) =>
							try behavior.handleException(cause)
							catch {
								case NonFatal(e) => Error(e, cause)
							}
					}
				onComplete(result)
			}
		})
	}
}
