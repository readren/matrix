package readren.matrix

import Reactant.SerialNumber

import readren.taskflow.Maybe

class RegularReactant[M](
	val serialNumber: SerialNumber,
	admin: MatrixAdmin,
	msgHandlerExecutorService: MsgHandlerExecutorService,
	progenitor: Progenitor,
	inbox: Inbox[M],
	initialBehavior: Behavior[M]
) extends Reactant[M](admin, msgHandlerExecutorService, initialBehavior) {


	/** should be called within the [[admin]]. */
	protected def withdrawNextMessage(): Maybe[M] = inbox.withdraw()

	/** should be called within the [[admin]]. */
	protected def restart(): Unit = ???

	/** should be called within the [[admin]]. */
	protected def stop(): Unit = ???
}
