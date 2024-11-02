package readren.matrix

import readren.taskflow.Maybe

object Reactant {
	type SerialNumber = Long
}

/** @param serialNumber identifies this [[Reactant]] among its siblings (those created by the same [[Progenitor]]).
 *  @param admin the [[MatrixAdmin]] assigned to this [[Reactant]].
 *  @param initialBehavior the behavior of this [[Reactant]] */
trait Reactant[M](val serialNumber: Reactant.SerialNumber, val progenitor: Progenitor, val admin: MatrixAdmin, initialBehavior: Behavior[M]) {
	
	/** should be accessed within the [[admin]]. */
	def isIdle: Boolean
	
	def setIsIdleState(newState: Boolean): Unit

	/** should be called within the [[admin]]. */
	def markForTermination(): Unit

	/** should be called within the [[admin]]. */
	def markForRestart(): Unit

	/** should be called within the [[admin]]. */
	def withdrawNextMessage(): Maybe[M]

	/** should be called within the [[admin]]. */
	def setBehavior(behavior: Behavior[M]): Unit

	/** should be called within the [[admin]]. */
	def currentBehavior: Behavior[M]
}
