package readren.matrix
package pruebas

import readren.taskflow.Doer

import java.util.concurrent.{CompletableFuture, ExecutorService, Executors, TimeUnit}
import scala.collection.mutable.ArrayBuffer

object Shared {

	class MatrixAide extends Matrix.Aide { thisMatrixAide =>
		override def reportFailure(cause: Throwable): Unit = cause.printStackTrace()

		private val executors: ArrayBuffer[ExecutorService] = ArrayBuffer.empty

		override def buildDoerAssistantForAdmin(adminId: Int): Doer.Assistant = new Doer.Assistant {

			private val doSiThEx = {
				val newExecutor = Executors.newSingleThreadExecutor()
				executors.addOne(newExecutor)
				newExecutor
			}

			override def queueForSequentialExecution(runnable: Runnable): Unit = doSiThEx.execute(runnable)

			override def reportFailure(cause: Throwable): Unit = thisMatrixAide.reportFailure(cause)
		}

		def shutdown(): CompletableFuture[Void] = {
			CompletableFuture.runAsync(
					() => executors.foreach(_.shutdown()),
					CompletableFuture.delayedExecutor(100, TimeUnit.MILLISECONDS)
					)
				.thenRunAsync(
					() => (),
					CompletableFuture.delayedExecutor(100, TimeUnit.MILLISECONDS)
					)
		}
	}	
	
}
