package readren.matrix
package rf

import mhes.ForkJoinMhes

import readren.taskflow.Maybe

object RegularRf extends ReactantFactory {
	override type MsgBuffer[U] = FifoInbox[U]

	override protected def createMsgBuffer[U](reactant: Reactant[U]): MsgBuffer[U] = new FifoInbox[U](reactant)

	override protected def createEndpointProvider[U](msgBuffer: FifoInbox[U]): EndpointProvider[U] = new EndpointProvider[U](msgBuffer)

	override def createReactant[U](
		id: Reactant.SerialNumber,
		progenitor: Spawner[MatrixAdmin],
		admin: MatrixAdmin,
		initialBehaviorBuilder: ReactantRelay[U] => Behavior[U]		
	): admin.Duty[Reactant[U]] = {
		admin.Duty.mineFlat { () =>
			new Reactant[U](id, progenitor, admin, initialBehaviorBuilder) {

				private val fifoInbox = createMsgBuffer(this)

				override val endpointProvider: EndpointProvider[U] = createEndpointProvider(fifoInbox)

				override def withdrawNextMessage(): Maybe[U] = fifoInbox.withdraw()

				override def aMsgIsPending: Boolean = fifoInbox.nonEmpty
			}.initialize().castTypePath(admin)
		}
	}
}
