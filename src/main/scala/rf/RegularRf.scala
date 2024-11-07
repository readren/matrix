package readren.matrix
package rf

import readren.taskflow.Maybe

class RegularRf[U](canSpawn: Boolean) extends ReactantFactory[U] {
	type MsgBuffer = FifoInbox[U]

	override protected def createMsgBuffer(reactant: Reactant[U]): MsgBuffer = new FifoInbox[U](reactant)

	override protected def createEndpointProvider(msgBuffer: FifoInbox[U]): EndpointProvider[U] = new EndpointProvider[U](msgBuffer)

	override def createReactant(
		id: Reactant.SerialNumber,
		progenitor: Spawner[MatrixAdmin],
		admin: MatrixAdmin,
		initialBehavior: Behavior[U]		
	): Reactant[U] = {
		new Reactant[U](id, progenitor, canSpawn, admin, initialBehavior, Maybe.empty) {

			private val fifoInbox = createMsgBuffer(this)

			override val endpointProvider: EndpointProvider[U] = createEndpointProvider(fifoInbox)

			override def withdrawNextMessage(): Maybe[U] = fifoInbox.withdraw()

			override def noPendingMsg: Boolean = fifoInbox.isEmpty
		}
	}
}
