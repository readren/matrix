package readren.nexus
package factories

import core.ActantCore
import msgbuffers.SequentialUnboundedFifo

import readren.sequencer.Doer

/** Creates [[core.ActantCore]] instances whose [[core.Inqueue]] is unbounded and mutates in sequence with its [[core.Actant.doer]].
 * This helps when accessing/modifying its the [[core/Inqueue]] is necessary. */
object SequentialInqueueAf extends ActantFactoryTemplate[SequentialUnboundedFifo] {
	override protected def createMsgBuffer[U, D <: Doer](actant: ActantCore[U, D]): MsgBuffer[U] = new SequentialUnboundedFifo[U](actant)
}
