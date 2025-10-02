package readren.nexus
package core

trait WatchSubscription {
	/** Undo this subscription instantly.
	 * Should be called within the [[Doer]] of the watching [[ActantCore]]. */
	def unsubscribe(): Unit
}
