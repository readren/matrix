package readren.matrix

trait MsgHandlerExecutorService {

	def executeMsgHandler[U](behavior: Behavior[U], message: U)(onComplete: HandleMsgResult[U] => Unit): Unit

	def executeSignalHandler[U](behavior: Behavior[U], signal: Signal)(onComplete: () => Unit): Unit

}
