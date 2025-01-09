package readren.matrix
package core

import readren.taskflow.{AbstractDoer, Doer}

object MatrixDoer {

	type Id = Long
	
	inline val checkWeAreWithingTheDoerIsEnabled = true

	val doerIdThreadLocal: ThreadLocal[Id] =
		if checkWeAreWithingTheDoerIsEnabled then ThreadLocal.withInitial[Id](() => -1)
		else null

	inline def checkOutside(): Unit = {
		inline if MatrixDoer.checkWeAreWithingTheDoerIsEnabled then {
			val idOnThread = MatrixDoer.doerIdThreadLocal.get()
			assert(idOnThread <= 0, s"expected<=0, onThread=$idOnThread")
		}
	}
}

class MatrixDoer(val id: MatrixDoer.Id, anAssistant: Doer.Assistant, val matrix: AbstractMatrix) extends AbstractDoer {

	override protected val assistant: Doer.Assistant = {
		if MatrixDoer.checkWeAreWithingTheDoerIsEnabled then {
			new Doer.Assistant {
				private val backingAssistant = anAssistant

				override def queueForSequentialExecution(runnable: Runnable): Unit = {
					backingAssistant.queueForSequentialExecution { () =>
						MatrixDoer.doerIdThreadLocal.set(id)
						runnable.run()
					}
				}

				override def reportFailure(cause: Throwable): Unit = backingAssistant.reportFailure(cause)
			}
		} else anAssistant
	}

	val dutyReadyUnit: Duty[Unit] = Duty.ready(())

	inline def checkWithin(): Unit = {
		inline if MatrixDoer.checkWeAreWithingTheDoerIsEnabled then {
			val idOnThread = MatrixDoer.doerIdThreadLocal.get()
			assert(idOnThread == id, s"expected=$id, onThread=$idOnThread")
		}
	}
}
