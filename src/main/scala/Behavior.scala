package readren.matrix

trait Behavior[-M] {
	val kind: BehaviorKind
	
	def handle(message: M): Behavior[M]
}
