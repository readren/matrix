package readren.matrix

import readren.taskflow.Doer


object Matrix {
	trait Aide extends MsgHandlerExecutorsManager.Aide {
		def reportFailure(cause: Throwable): Unit
		def buildDoerAssistantForAdmin(): Doer.Assistant
	}
}

class Matrix(name: String, aide: Matrix.Aide) { thisMatrix =>

	import Matrix.*

	private val msgHandlingDoersManager = new MsgHandlerExecutorsManager(aide)

	private val adminDoers: IArray[MatrixAdmin] = {
		val availableProcessors = Runtime.getRuntime.availableProcessors()
		IArray.fill(availableProcessors) {
			new MatrixAdmin(aide.buildDoerAssistantForAdmin(), msgHandlingDoersManager)
		}
	}
	
	val progenitor: Progenitor = new Progenitor(0, adminDoers) {}

}
