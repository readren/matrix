package readren.matrix
package core

import readren.taskflow.{AbstractDoer, Doer}

object MatrixDoer {

	type Id = Long
	
	inline val checkWeAreWithingTheDoerIsEnabled = false

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

	override val assistant: Doer.Assistant = {
		if MatrixDoer.checkWeAreWithingTheDoerIsEnabled then {
			new Doer.Assistant { thisAssistant =>
				private val backingAssistant = anAssistant

				override def queueForSequentialExecution(runnable: Runnable): Unit = {
					backingAssistant.queueForSequentialExecution { () =>
						MatrixDoer.doerIdThreadLocal.set(id)
						runnable.run()
					}
				}
				
				override def current: Doer.Assistant = anAssistant.current

				override def reportFailure(cause: Throwable): Unit = backingAssistant.reportFailure(cause)
			}
		} else anAssistant
	}

	val dutyReadyUnit: Duty[Unit] = Duty.ready(())

	inline def checkWithin(): Unit = {
		inline if MatrixDoer.checkWeAreWithingTheDoerIsEnabled then {
			val idOnThread = MatrixDoer.doerIdThreadLocal.get()
			assert(idOnThread == id, s"The current thread is not executing this MatrixDoer: expectedId=$id, onThread=$idOnThread")
		} else assert(assistant.isCurrentAssistant, s"The current thread does not correspond to the assistant of this MatrixDoer: expected=$assistant, current=${assistant.current}")
	}
}
