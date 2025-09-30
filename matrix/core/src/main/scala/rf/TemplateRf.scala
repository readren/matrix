package readren.matrix
package rf

import core.*

import readren.sequencer.Doer

abstract class TemplateRf[MS[u] <: Inbox[u] & Receiver[u]] extends ReactantFactory {
	/** Design note: Allows delegating the construction to subsidiary methods without losing type safety. */
	type MsgBuffer[u] = MS[u]

	/** Creates the pending messages buffer needed by the [[createsReactant]] method. */
	protected def createMsgBuffer[U](reactant: Reactant[U, ?]): MsgBuffer[U]

	protected def createEndpointProvider[U](msgBuffer: MsgBuffer[U]): EndpointProvider[U] = new EndpointProvider[U](msgBuffer)

	override def createsReactant[U, D <: Doer](
		serial: Reactant.SerialNumber,
		progenitor: Spawner[?],
		reactantDoer: D,
		isSignalTest: IsSignalTest[U],
		initialBehaviorBuilder: ReactantRelay[U, D] => Behavior[U]
	): reactantDoer.Duty[Reactant[U, D]] = {
		reactantDoer.Duty_mineFlat { () =>
			new Reactant[U, D](serial, reactantDoer, progenitor, isSignalTest, initialBehaviorBuilder) {
				
				override protected val inbox: MsgBuffer[U] = createMsgBuffer(this)

				override val endpointProvider: EndpointProvider[U] = createEndpointProvider(inbox)

			}.initialize().castTypePath(reactantDoer)
		}
	}
}
