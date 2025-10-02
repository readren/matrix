package readren.nexus
package rf

import core.ActantCore
import msgbuffers.SequentialUnboundedFifo

import readren.sequencer.Doer

object SequentialMsgBufferSf extends ActantFactoryTemplate[SequentialUnboundedFifo] {
	override protected def createMsgBuffer[U, D <: Doer](actant: ActantCore[U, D]): MsgBuffer[U] = new SequentialUnboundedFifo[U](actant)
}
