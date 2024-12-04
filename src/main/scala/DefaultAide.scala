package readren.matrix

import readren.taskflow.Doer

import java.util.concurrent.Executors

class DefaultAide extends Matrix.Aide { thisAide =>

	override def reportFailure(cause: Throwable): Unit = cause.printStackTrace()
	
	override def buildDoerAssistant(doerId: Int): Doer.Assistant = new Doer.Assistant {
		private val doSiThEx = Executors.newSingleThreadExecutor()

		override def queueForSequentialExecution(runnable: Runnable): Unit = doSiThEx.execute(runnable)

		override def reportFailure(cause: Throwable): Unit = thisAide.reportFailure(cause)
	}
}
