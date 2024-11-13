package readren.matrix
package rf

import msgbuffers.ConcurrentUnboundedFifo

object RegularRf extends TemplateRf[ConcurrentUnboundedFifo] {
	override protected def createMsgBuffer[U](reactant: Reactant[U]): MsgBuffer[U] = new ConcurrentUnboundedFifo[U](reactant)
}
