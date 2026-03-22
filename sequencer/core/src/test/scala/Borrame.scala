package readren.sequencer

import Doer.ExecutionSerial

import readren.common.Maybe

import scala.util.{Failure, Success, Try}

object Borrame {
	val currentDoer: ThreadLocal[Doer] = new ThreadLocal()
	val doer = new Doer {
		override def executeSequentially(runnable: Runnable): Unit = runnable.run()

		override protected def reportFailure(cause: Throwable): Unit = println(s"Cause: $cause")

		override def currentlyRunningDoer: Maybe[Doer] = Maybe(currentDoer.get())

		override type Tag = String
		override val tag: Tag = "unique"

		override def currentExecutionSerial: ExecutionSerial = 0
	}

	@main
	def main(): Unit = {
		val task1: doer.Task[Int] = doer.Task_ready(Success(1))
		val task2 = task1.map(x => (x * 2).toString)
		pepe(task2.reconcile)
	}

	def pepe(duty: doer.Duty[Try[String]]): Unit = {
		duty.map {
			case Success(v) => println(v.reverse)
			case Failure(e) => println(e)
		}
		duty.trigger(false)(println)
	}

}
