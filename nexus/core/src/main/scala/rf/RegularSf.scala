package readren.nexus
package rf

import core.SpuronCore
import msgbuffers.ConcurrentUnboundedFifo

import readren.sequencer.Doer

object RegularSf extends SpuronFactoryTemplate[ConcurrentUnboundedFifo] {
	override protected def createMsgBuffer[U, D <: Doer](spuron: SpuronCore[U, D]): MsgBuffer[U] = new ConcurrentUnboundedFifo[U](spuron)
}
