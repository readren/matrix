package readren.matrix

import readren.taskflow.Maybe

trait Inbox[+M] {

	/**
	 * Sets the "is ready to process messages" state of the owner of this [[Inbox]].
	 * Mutations of said state should be atomic from the viewpoint of this [[Inbox]].
	 * Implementation that are intended for a [[Reactant]]:
	 * 		- may assume that:
	 *			- this method is called within the [[MatrixAdmin]] of the owning [[Reactant]];
	 *			- the reactant never reads this state directly;
	 *			- will call passing a `true` only after calling [[withdraw]] and only if it returned [[Maybe.empty]];
	 *		- and should:
	 *			- set the ready state to false when it (the inbox) starts to have a pending message while the ready state is `true`, and immediately after that should also call the [[Reactant.processMessages()]] method passing the first pending message.   	
	 *		
	 * The initial state should be `false` (not ready).
	 * Design note: The responsibility of knowing if the owner is ready is delegated from the owner to the inbox in order to allow the [[Inbox]] implementation decide how to implement the atomicity of this boolean. */
	def setOwnerReadyToProcessState(isReady: Boolean): Unit

	/** Withdraws the next pending message.
	 * The implementation may assume that this method is called withing the [[MatrixAdmin]] of the owning [[Reactant]] only. */
	def withdraw(): Maybe[M]


	/** Checks if there are no pending messages.
	 * Should be called withing the [[MatrixAdmin]] of the owning [[Reactant]] only. */
	@deprecated("not used for the moment")
	def nonEmpty: Boolean
}
