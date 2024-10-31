package readren.matrix

trait Behavior[M] {
	val kind: BehaviorKind
	
	def handle(message: M): Behavior[M]

	/** should be called sequentially with the [[handle]] method. */
	def handleException(throwable: Throwable): ProcessMsgResult[M]
}
