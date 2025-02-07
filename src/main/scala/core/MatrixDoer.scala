package readren.matrix
package core

import readren.taskflow.{AbstractDoer, Doer}

object MatrixDoer {
	type Id = Long
}

abstract class MatrixDoer extends AbstractDoer {
	val id: MatrixDoer.Id
	val matrix: AbstractMatrix

	val dutyReadyUnit: Duty[Unit] = Duty.ready(())

	inline def checkWithin(): Unit = {
		assert(assistant.isWithinDoSiThEx, s"The current thread does not correspond to the assistant of this MatrixDoer: expected=$assistant, current=${assistant.current}")
	}
}
