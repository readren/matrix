package readren.matrix
package rf

import msgbuffers.SequentialUnboundedFifo

object SequentialMsgBufferRf extends TemplateRf[SequentialUnboundedFifo] {
	override protected def createMsgBuffer[U](reactant: Reactant[U]): MsgBuffer[U] = new SequentialUnboundedFifo[U](reactant)
}
