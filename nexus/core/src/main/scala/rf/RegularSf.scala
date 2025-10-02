package readren.nexus
package rf

import core.ActantCore
import msgbuffers.ConcurrentUnboundedFifo

import readren.sequencer.Doer

object RegularSf extends ActantFactoryTemplate[ConcurrentUnboundedFifo] {
	override protected def createMsgBuffer[U, D <: Doer](actant: ActantCore[U, D]): MsgBuffer[U] = new ConcurrentUnboundedFifo[U](actant)
}
