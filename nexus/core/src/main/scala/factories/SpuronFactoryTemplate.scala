package readren.nexus
package factories

import core.*

import readren.sequencer.Doer

abstract class ActantFactoryTemplate[MS[u] <: Inbox[u] & Inqueue[u]] extends ActantFactory {
	/** Design note: Allows delegating the construction to subsidiary methods without losing type safety. */
	type MsgBuffer[u] = MS[u]

	/** Creates the pending messages buffer needed by the [[createsActant]] method. */
	protected def createMsgBuffer[U, D <: Doer](actant: ActantCore[U, D]): MsgBuffer[U]

	protected def createReceptorProvider[U](msgBuffer: MsgBuffer[U]): ReceptorProvider[U] = new ReceptorProvider[U](msgBuffer)

	override def createsActant[U, D <: Doer](
		serial: ActantCore.SerialNumber,
		progenitor: Spawner[?],
		actantDoer: D,
		isSignalTest: IsSignalTest[U],
		initialBehaviorBuilder: Actant[U, D] => Behavior[U]
	): actantDoer.Duty[ActantCore[U, D]] = {
		actantDoer.Duty_mineFlat { () =>
			new ActantCore[U, D](serial, actantDoer, progenitor, isSignalTest, initialBehaviorBuilder) {
				
				override protected val inbox: MsgBuffer[U] = createMsgBuffer(this)

				override val receptorProvider: ReceptorProvider[U] = createReceptorProvider(inbox)

			}.initialize().castTypePath(actantDoer)
		}
	}
}
