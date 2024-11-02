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

	private val msgHandlerExecutorService = new MsgHandlerExecutorService

	private val adminDoers: IArray[MatrixAdmin] = {
		val availableProcessors = Runtime.getRuntime.availableProcessors()
		IArray.fill(availableProcessors) {
			new MatrixAdmin(aide.buildDoerAssistantForAdmin(), msgHandlerExecutorService)
		}
	}
	
	val progenitor: Progenitor = new Progenitor(0, adminDoers) {}

}
