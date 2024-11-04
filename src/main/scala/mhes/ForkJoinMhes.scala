package readren.matrix
package mhes

import java.util.concurrent.ForkJoinPool
import scala.util.control.NonFatal

class ForkJoinMhes extends MsgHandlerExecutorService {

	val forkJoinPool:  ForkJoinPool = new ForkJoinPool()

	override def executeMsgHandler[M](behavior: Behavior[M], message: M)(onComplete: HandleMsgResult[M] => Unit): Unit = {
		 
		forkJoinPool.execute(() => onComplete(behavior.handleMessage(message)))
	}
}
