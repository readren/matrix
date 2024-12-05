package readren.matrix
package doerproviders

import readren.taskflow.Doer

import java.util.concurrent.Executors

class SimpleDoerProvider(owner: Matrix[SimpleDoerProvider]) extends Matrix.DoerProvider { thisProvider =>

	/**
	 * The array with all the [[MatrixDoer]] instances of this [[Matrix]]. */
	private val matrixDoers: IArray[MatrixDoer] = {
		val availableProcessors = Runtime.getRuntime.availableProcessors()
		IArray.tabulate(availableProcessors) { index =>
			val doerId = index + 1
			new MatrixDoer(doerId, buildDoerAssistant(doerId), owner)
		}
	}

	override def pick(serial: Int): MatrixDoer = matrixDoers(serial % matrixDoers.length)

	private def buildDoerAssistant(doerId: Int): Doer.Assistant = new Doer.Assistant {
		private val doSiThEx = Executors.newSingleThreadExecutor()

		override def queueForSequentialExecution(runnable: Runnable): Unit = doSiThEx.execute(runnable)

		override def reportFailure(cause: Throwable): Unit = cause.printStackTrace()
	}
}
