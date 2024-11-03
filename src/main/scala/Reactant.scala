package readren.matrix

import readren.taskflow.Maybe

object Reactant {
	type SerialNumber = Long
}

trait Reactant[M] {

//	/** Identifies this [[Reactant]] among its siblings (those created by the same [[Progenitor]]). */
//	val serialNumber: Reactant.SerialNumber
//
//	/** The progenitor of this instance. */
//	val progenitor: Progenitor

	/** The [[MatrixAdmin]] assigned to this instance. */
	val admin: MatrixAdmin

	/** should be called within the [[admin]]. */
	def isIdle: Boolean

	/** should be called within the [[admin]]. */
	def setIsIdleState(newState: Boolean): Unit

	/** should be called within the [[admin]]. */
	def setBehavior(behavior: Behavior[M]): Unit

	/** should be called within the [[admin]]. */
	def getCurrentBehavior: Behavior[M]

	/** should be called within the [[admin]]. */
	def withdrawNextMessage(): admin.Duty[Maybe[M]]

//	def ask[Q, R <: M](question: Q, inbox: InboxBackend[R]): Unit

	/** should be called within the [[admin]]. */
	def restart(): Unit

	/** should be called within the [[admin]]. */
	def stop(): Unit

}
