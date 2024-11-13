package readren.matrix
package rf

import msgbuffers.SerializedUnboundedFifo

object SynchronousMsgBufferRf extends TemplateRf[SerializedUnboundedFifo] {
	override protected def createMsgBuffer[U](reactant: Reactant[U]): MsgBuffer[U] = new SerializedUnboundedFifo[U](reactant)
}
