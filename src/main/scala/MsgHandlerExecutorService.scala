package readren.matrix

import java.util.concurrent.ForkJoinPool
import scala.util.control.NonFatal

class MsgHandlerExecutorService {

	val forkJoinPool:  ForkJoinPool = new ForkJoinPool()

	def executeMsgHandler[M](behavior: Behavior[M], message: M)(onComplete: HandleMsgResult[M] => Unit): Unit = {
		 
		forkJoinPool.execute(() => onComplete(behavior.handleMessage(message)))
	}
}
