package readren.matrix

trait MsgHandlerExecutorService {

	def executeMsgHandler[M](behavior: Behavior[M], message: M)(onComplete: HandleMsgResult[M] => Unit): Unit
		 
}
