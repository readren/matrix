package readren.matrix
package rf

abstract class TemplateRf[MS[u] <: Inbox[u] & Receiver[u]] extends ReactantFactory {
	/** Design note: Allows delegating the construction to subsidiary methods without losing type safety. */
	type MsgBuffer[u] = MS[u]

	/** Creates the pending messages buffer needed by the [[createReactant]] method. */
	protected def createMsgBuffer[U](reactant: Reactant[U]): MsgBuffer[U]

	protected def createEndpointProvider[U](msgBuffer: MsgBuffer[U]): EndpointProvider[U] = new EndpointProvider[U](msgBuffer)

	override def createReactant[U](
		id: Reactant.SerialNumber,
		progenitor: Spawner[MatrixAdmin],
		admin: MatrixAdmin,
		isSignalTest: IsSignalTest[U],		
		initialBehaviorBuilder: ReactantRelay[U] => Behavior[U]
	): admin.Duty[Reactant[U]] = {
		admin.Duty.mineFlat { () =>
			new Reactant[U](id, progenitor, admin, isSignalTest, initialBehaviorBuilder) {

				override protected val inbox: MsgBuffer[U] = createMsgBuffer(this)

				override val endpointProvider: EndpointProvider[U] = createEndpointProvider(inbox)

			}.initialize().castTypePath(admin)
		}
	}
}
