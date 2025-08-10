package readren.matrix
package core

import providers.assistant.DoerAssistantProvider.Tag

import readren.sequencer.AbstractDoer

abstract class MatrixDoer extends AbstractDoer {
	val tag: Tag
	val matrix: AbstractMatrix

	val dutyReadyUnit: Duty[Unit] = Duty.ready(())

	inline def checkWithin(): Unit = {
		assert(assistant.isWithinDoSiThEx, s"The current thread does not correspond to the assistant of this MatrixDoer: expected=$assistant, current=${assistant.current}, tag=$tag")
	}
}
