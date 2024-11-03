package readren.matrix

import readren.taskflow.Maybe

class RegularReactant[M](val serialNumber: Reactant.SerialNumber, val admin: MatrixAdmin, progenitor: Progenitor, inbox: InboxBackend[M], initialBehavior: Behavior[M])
	extends Reactant[M] {

	private var idleState = true
	private var currentBehavior = initialBehavior
	
	/** should be called within the [[admin]]. */
	def isIdle: Boolean = idleState

	/** should be called within the [[admin]]. */
	def setIsIdleState(newState: Boolean): Unit = idleState = newState

	/** should be called within the [[admin]]. */
	def setBehavior(behavior: Behavior[M]): Unit = currentBehavior = behavior

	/** should be called within the [[admin]]. */
	def getCurrentBehavior: Behavior[M] = currentBehavior

	/** should be called within the [[admin]]. */
	def withdrawNextMessage(): admin.Duty[Maybe[M]] = inbox.withdraw().castTypePath(admin)

	/** should be called within the [[admin]]. */
	def restart(): Unit = ???

	/** should be called within the [[admin]]. */
	def stop(): Unit = ???
}
