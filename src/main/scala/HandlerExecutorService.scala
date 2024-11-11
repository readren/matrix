package readren.matrix

trait HandlerExecutorService[U] {
	// TODO consolidate the three methods in a single more generic one.
	def executeMsgHandler(behavior: Behavior[U], message: U)(onComplete: HandleResult[U] => Unit): Unit

	def executeSignalHandler(behavior: Behavior[U], signal: Signal)(onComplete: HandleResult[U] => Unit): Unit
	
	def executeBehaviorBuilder(builder: ReactantRelay[U] => Behavior[U], reactant: ReactantRelay[U])(onComplete: Behavior[U] => Unit): Unit

}
