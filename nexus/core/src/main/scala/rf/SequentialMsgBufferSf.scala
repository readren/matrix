package readren.nexus
package rf

import core.SpuronCore
import msgbuffers.SequentialUnboundedFifo

import readren.sequencer.Doer

object SequentialMsgBufferSf extends SpuronFactoryTemplate[SequentialUnboundedFifo] {
	override protected def createMsgBuffer[U, D <: Doer](spuron: SpuronCore[U, D]): MsgBuffer[U] = new SequentialUnboundedFifo[U](spuron)
}
