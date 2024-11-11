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

class MatrixAdmin(val id: Int, anAssistant: Doer.Assistant, val matrix: Matrix) extends Doer(anAssistant) {

	val dutyReadyUnit: Duty[Unit] = Duty.ready(())
	
	inline def checkWithin(): Unit = {
		inline if MatrixAdmin.checkWeAreWithingTheAdminIsEnabled then {
			val idOnThread = MatrixAdmin.adminIdThreadLocal.get()
			assert( idOnThread == id, s"expected=$id, onThread=$idOnThread")
		}
	}

	inline def checkOutside(): Unit = {
		inline if MatrixAdmin.checkWeAreWithingTheAdminIsEnabled then {
			val idOnThread = MatrixAdmin.adminIdThreadLocal.get()
			assert(idOnThread <= 0, s"expected<=0, onThread=$idOnThread")
		}
	}
}
