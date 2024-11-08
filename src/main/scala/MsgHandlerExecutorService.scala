package readren.matrix

trait MsgHandlerExecutorService {

	def executeMsgHandler[U](behavior: Behavior[U], message: U)(onComplete: HandleResult[U] => Unit): Unit

	def executeSignalHandler[U](behavior: Behavior[U], signal: Signal)(onComplete: HandleResult[U] => Unit): Unit

}
