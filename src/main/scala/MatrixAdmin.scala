package readren.matrix

import readren.taskflow.Doer

object MatrixAdmin {

	inline val checkWeAreWithingTheAdminIsEnabled = true

	val adminIdThreadLocal: ThreadLocal[Int] =
		if checkWeAreWithingTheAdminIsEnabled then ThreadLocal.withInitial[Int](() => -1)
		else null

	inline def checkOutside(): Unit = {
		inline if MatrixAdmin.checkWeAreWithingTheAdminIsEnabled then {
			val idOnThread = MatrixAdmin.adminIdThreadLocal.get()
			assert(idOnThread <= 0, s"expected<=0, onThread=$idOnThread")
		}
	}
}

class MatrixAdmin(val id: Int, anAssistant: Doer.Assistant, val matrix: Matrix) extends Doer {

	override protected val assistant: Doer.Assistant = {
		if MatrixAdmin.checkWeAreWithingTheAdminIsEnabled then {
			new Doer.Assistant {
				private val backingAssistant = anAssistant

				override def queueForSequentialExecution(runnable: Runnable): Unit = {
					backingAssistant.queueForSequentialExecution { () =>
						MatrixAdmin.adminIdThreadLocal.set(id)
						runnable.run()
					}
				}

				override def reportFailure(cause: Throwable): Unit = backingAssistant.reportFailure(cause)
			}
		} else anAssistant
	}

	val dutyReadyUnit: Duty[Unit] = Duty.ready(())

	inline def checkWithin(): Unit = {
		inline if MatrixAdmin.checkWeAreWithingTheAdminIsEnabled then {
			val idOnThread = MatrixAdmin.adminIdThreadLocal.get()
			assert(idOnThread == id, s"expected=$id, onThread=$idOnThread")
		}
	}

	inline def checkOutside(): Unit = {
		inline if MatrixAdmin.checkWeAreWithingTheAdminIsEnabled then {
			val idOnThread = MatrixAdmin.adminIdThreadLocal.get()
			assert(idOnThread <= 0, s"expected<=0, onThread=$idOnThread")
		}
	}
}
