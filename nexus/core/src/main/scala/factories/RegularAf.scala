package readren.nexus
package factories

import core.ActantCore
import msgbuffers.ConcurrentUnboundedFifo

import readren.sequencer.Doer

/** Creates [[core.ActantCore]] instances whose [[core.Inqueue]] is unbounded and mutates concurrently (not in sequence with its [[core.Actant.doer]]). */
object RegularAf extends ActantFactoryTemplate[ConcurrentUnboundedFifo] {
	override protected def createMsgBuffer[U, D <: Doer](actant: ActantCore[U, D]): MsgBuffer[U] = new ConcurrentUnboundedFifo[U](actant)
}
