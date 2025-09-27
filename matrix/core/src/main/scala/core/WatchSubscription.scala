package readren.matrix
package core

trait WatchSubscription {
	/** Undo this subscription instantly.
	 * Should be called within the [[Doer]] of the watching [[Reactant]]. */
	def unsubscribe(): Unit
}
