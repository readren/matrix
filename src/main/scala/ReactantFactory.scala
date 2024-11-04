package readren.matrix

import readren.taskflow.Doer

object ReactantFactory {
	type SerialNumber = Int
}

trait ReactantFactory {
	/** Being a type member allows the creation of the [[MsgBuffer]] and [[Reactant]] instances in two steps without losing type safety.
	 * The two-step creation avoids the need to return a tuple. */
	type MsgBuffer[m] <: Receiver[m]

	/** Creates the pending messages buffer for a [[Reactant]]. */
	def createMsgBuffer[M](reactantAdmin: MatrixAdmin): MsgBuffer[M]

	/** Creates a new [[Reactant]] */
	def createReactant[M](
		id: Reactant.SerialNumber,
		progenitor: Progenitor,
		reactantAdmin: MatrixAdmin,
		msgBuffer: MsgBuffer[M]
	): Reactant[M]
}
