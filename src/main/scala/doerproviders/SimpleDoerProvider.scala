package readren.matrix
package doerproviders

import readren.taskflow.Doer

import java.util.concurrent.{ExecutorService, Executors, TimeUnit}
import scala.util.Try

class SimpleDoerProvider(owner: Matrix[SimpleDoerProvider]) extends Matrix.DoerProvider { thisProvider =>

	private class Entry(val doer: MatrixDoer, val executor: ExecutorService)
	
	/**
	 * The array with all the [[MatrixDoer]] instances of this [[Matrix]]. */
	private val matrixDoers: IArray[Entry] = {
		val availableProcessors = Runtime.getRuntime.availableProcessors()
		IArray.tabulate(availableProcessors) { index =>
			val doerId = index + 1
			val doSiThEx = Executors.newSingleThreadExecutor()
			val doer = new MatrixDoer(doerId, new Assistant(doSiThEx), owner)
			Entry(doer, doSiThEx)
		}
	}

	override def pick(serial: Int): MatrixDoer = matrixDoers(serial % matrixDoers.length).doer

	private class Assistant(doSiThEx: ExecutorService) extends Doer.Assistant {

		override def queueForSequentialExecution(runnable: Runnable): Unit = doSiThEx.execute(runnable)

		override def reportFailure(cause: Throwable): Unit = cause.printStackTrace()
	}
	
	def shutdown(): Unit = {
		for md <- matrixDoers do {
			md.executor.shutdown()
		}
	}

	def awaitTermination(timeout: Long, unit: TimeUnit): Try[Unit] = {
		Try {
			for md <- matrixDoers do {
				md.executor.awaitTermination(timeout, unit)
			}
		}
	}
}
