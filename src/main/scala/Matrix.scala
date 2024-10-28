package readren.matrix

import readren.taskflow.TaskDomain

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable

object Matrix {
	trait Aide {
		def reportFailure(cause: Throwable): Unit
	}


}

class Matrix(name: String, aide: Matrix.Aide) { thisMatrix =>

	import Matrix.*

	private val inboxSerialNumberSequencer: AtomicInteger = new AtomicInteger(0)

	private val taskDomains: IArray[TaskDomain] = {
		val availableProcessors = Runtime.getRuntime.availableProcessors()
		IArray.fill(availableProcessors) {
			val assistant = new TaskDomain.Assistant {
				private val doSiThEx = Executors.newSingleThreadExecutor()

				override def queueForSequentialExecution(runnable: Runnable): Unit = doSiThEx.execute(runnable)

				override def reportFailure(cause: Throwable): Unit = aide.reportFailure(cause)
			}
			new TaskDomain(assistant) {}
		}
	}

	val progenitor: Progenitor = new Progenitor(
		0,
		new Progenitor.Aide {
			override val taskDomains: IArray[TaskDomain] = thisMatrix.taskDomains

			override def pickTaskDomain(serialNumber: Int): TaskDomain =
				taskDomains(serialNumber % taskDomains.length)
		}) {}
}
