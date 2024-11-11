package readren.matrix
package mhes

import java.util.concurrent.ForkJoinPool
import scala.util.control.NonFatal

class ForkJoinMhes[U] extends HandlerExecutorService[U] {

	val forkJoinPool:  ForkJoinPool = new ForkJoinPool()

	override def executeMsgHandler(behavior: Behavior[U], message: U)(onComplete: HandleResult[U] => Unit): Unit = {
		 
		forkJoinPool.execute(() => onComplete(behavior.handleMsg(message)))
	}

	override def executeSignalHandler(behavior: Behavior[U], signal: Signal)(onComplete: HandleResult[U] => Unit): Unit = {
		forkJoinPool.execute { () =>
			onComplete(behavior.handleSignal(signal))
		}
	}

	override def executeBehaviorBuilder(builder: ReactantRelay[U] => Behavior[U], reactant: ReactantRelay[U])(onComplete: Behavior[U] => Unit): Unit = {
		forkJoinPool.execute { () =>
			onComplete(builder(reactant))
		}
	}
}
