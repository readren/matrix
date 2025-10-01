package readren.nexus
package core

import readren.common.Maybe

/**
 * Implementation that are intended for a [[SpuronCore]] should, whenever this inbox starts to have a pending message, clear the "owner is ready to process" state and immediately after that should also call the [[SpuronCore.onInboxBecomesNonempty()]] method passing the first pending message.
 * Design note: The responsibility of knowing if the owner is ready is delegated from the owner to the inbox in order to allow the [[Inbox]] implementation decide how to implement the atomicity of this boolean.
 *  */
trait Inbox[+M] {

	/** Withdraws the next pending message.
	 * The implementation may assume that this method is called withing the [[Doer]] of the owning [[SpuronCore]] only. */
	def withdraw(): Maybe[M]


	/** Checks if there are no pending messages.
	 * Should be called withing the [[Doer]] of the owning [[SpuronCore]] only. */
	def maybeNonEmpty: Boolean


	/** 
	 * Exposes the pending messages for diagnostic.
	 * Should be called within the [[Doer]] of the owning [[SpuronCore]] only.
	 * The thread-safety of the returned [[Iterator]] depends on this [[Inbox]] implementation. To ensure correct usage, use it withing said [[Doer]] only.
	 * The returned [[Iterator]] may be weakly consistent. */
	def iterator: Iterator[M]

	/** The number of pending messages.
	 * Used for diagnostic only.
	 * Should be called withing the [[Doer]] of the owning [[SpuronCore]] only.
	 * */
	def size: Int
}
