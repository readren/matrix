package readren.sequencer.akka

import akka.actor.typed.{ActorRef, Scheduler}
import akka.util.Timeout
import readren.sequencer.Doer
import scala.util.{Failure, Success}

object ActorExtension {
	trait Aide extends Doer.Assistant {
		def akkaScheduler: Scheduler
	}
}

/** Extends [[Doer]] with akka-actor related operations. */
trait ActorExtension { thisActorExtension: Doer =>
	override type Assistant <: ActorExtension.Aide

	extension [A](target: ActorRef[A]) {
		/** Creates a [[Task]] that sends the provided message to the `target`. */
		def says(message: A): Task[Unit] = Task.mine(() => target ! message)

		/** Note: The type parameter is required for the compiler to know the type parameter of the resulting [[Task]]. */
		def queries[B](messageBuilder: ActorRef[B] => A)(using timeout: Timeout): Task[B] = {
			import akka.actor.typed.scaladsl.AskPattern.*
			Task.wait(target.ask[B](messageBuilder)(using timeout, assistant.akkaScheduler))
		}
	}

	extension [A](task: Task[A]) {

		/**
		 * Triggers the execution of this task and sends the result to the `destination`.
		 *
		 * @param destination the [[ActorRef]] of the actor to send the result to.
		 * @param isWithinDoSiThEx $isRunningInDoSiThEx
		 * @param errorHandler called if the execution of this task completed with failure.
		 */
		def triggerAndSend(destination: ActorRef[A], isWithinDoSiThEx: Boolean = assistant.isWithinDoSiThEx)(errorHandler: Throwable => Unit): Unit = {
			task.trigger(isWithinDoSiThEx) {
				case Success(r) => destination ! r;
				case Failure(e) => errorHandler(e)
			}
		}
	}

} 
