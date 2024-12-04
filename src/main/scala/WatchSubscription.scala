package readren.matrix

trait WatchSubscription {
	/** Undo this subscription instantly.
	 * Should be called within the [[MatrixDoer]] of the watching [[Reactant]]. */
	def unsubscribe(): Unit
}
