package readren.matrix

import readren.taskflow.Doer


object Matrix {
	trait Aide {
		def reportFailure(cause: Throwable): Unit
		def buildDoerAssistantForAdmin(): Doer.Assistant
	}
}

class Matrix(name: String, aide: Matrix.Aide) { thisMatrix =>

	import Matrix.*

//	private val msgHandlerExecutorService = new MsgHandlerExecutorService

	private val matrixAdmins: IArray[MatrixAdmin] = {
		val availableProcessors = Runtime.getRuntime.availableProcessors()
		val doerAssistant = aide.buildDoerAssistantForAdmin()
		IArray.fill(availableProcessors)(new MatrixAdmin(doerAssistant))
	}
	
	val progenitor: Progenitor = new Progenitor(matrixAdmins(0), 0, matrixAdmins) {}

}
