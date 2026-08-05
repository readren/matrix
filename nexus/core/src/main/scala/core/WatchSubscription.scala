package readren.nexus
package core

import readren.sequencer.Doer

/** TODO consider removing this class and use [[Doer.Subcription]] instead. */
trait WatchSubscription {
	/** Undo this subscription instantly.
	 * Should be called within the [[Doer]] of the watching [[ActantCore]]. */
	def unsubscribe(): Unit
}
