package readren.matrix

import readren.taskflow.Maybe

object Reactant {
	type SerialNumber = Long
}

/** @param serialNumber identifies this [[Reactant]] among its siblings (those created by the same [[Progenitor]]).
 *  @param admin the [[MatrixAdmin]] assigned to this [[Reactant]].
 *  @param initialBehavior the behavior of this [[Reactant]] */
trait Reactant[M](val serialNumber: Reactant.SerialNumber, val admin: MatrixAdmin, initialBehavior: Behavior[M]) {
	
	/** should be called within the [[admin]]. */
	var isIdle: Boolean 

	def withdrawNextMessage(): Maybe[M]
	
	def setBehavior(behavior: Behavior[M]): Unit
	
	def currentBehavior: Behavior[M]	
}
