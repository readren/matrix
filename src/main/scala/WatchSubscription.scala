package readren.matrix

trait WatchSubscription {
	/** Undo this subscription instantly.
	 * Should be called within the [[MatrixAdmin]] of the watching [[Reactant]]. */
	def unsubscribe(): Unit
}
