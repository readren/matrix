package readren.nexus
package rf

import core.*

import readren.sequencer.Doer

abstract class SpuronFactoryTemplate[MS[u] <: Inbox[u] & Receiver[u]] extends SpuronFactory {
	/** Design note: Allows delegating the construction to subsidiary methods without losing type safety. */
	type MsgBuffer[u] = MS[u]

	/** Creates the pending messages buffer needed by the [[createsSpuron]] method. */
	protected def createMsgBuffer[U, D <: Doer](spuron: SpuronCore[U, D]): MsgBuffer[U]

	protected def createEndpointProvider[U](msgBuffer: MsgBuffer[U]): EndpointProvider[U] = new EndpointProvider[U](msgBuffer)

	override def createsSpuron[U, D <: Doer](
		serial: SpuronCore.SerialNumber,
		progenitor: Spawner[?],
		spuronDoer: D,
		isSignalTest: IsSignalTest[U],
		initialBehaviorBuilder: Spuron[U, D] => Behavior[U]
	): spuronDoer.Duty[SpuronCore[U, D]] = {
		spuronDoer.Duty_mineFlat { () =>
			new SpuronCore[U, D](serial, spuronDoer, progenitor, isSignalTest, initialBehaviorBuilder) {
				
				override protected val inbox: MsgBuffer[U] = createMsgBuffer(this)

				override val endpointProvider: EndpointProvider[U] = createEndpointProvider(inbox)

			}.initialize().castTypePath(spuronDoer)
		}
	}
}
