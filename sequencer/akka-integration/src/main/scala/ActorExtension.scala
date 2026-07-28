package readren.sequencer.akka

import akka.actor.ActorPath
import akka.actor.typed.{ActorRef, Scheduler}
import akka.util.Timeout
import readren.sequencer.Doer

import scala.util.{Failure, Success}


/** Extends [[Doer]] with akka-actor related operations. */
trait ActorExtension { thisActorExtension: Doer =>
	override type Tag = ActorPath

	def akkaScheduler: Scheduler

	extension [A](target: ActorRef[A]) {
		/** Creates a [[Task]] that sends the provided message to the `target`. */
		def says(message: A): Task[Unit] = Task_apply(() => target ! message)

		/** Note: The type parameter is required for the compiler to know the type parameter of the resulting [[Task]]. */
		def queries[B](messageBuilder: ActorRef[B] => A)(using timeout: Timeout): Task[B] = {
			import akka.actor.typed.scaladsl.AskPattern.*
			Task_from(target.ask[B](messageBuilder)(using timeout, akkaScheduler))
		}
	}

	extension [A](taskA: Task[A]) {

		/**
		 * Triggers the execution of this task and sends the result to the `destination`.
		 *
		 * @param destination the [[ActorRef]] of the actor to send the result to.
		 * @param isWithinDoSerEx tells if the call is within the doer's serial executor.
		 * @param errorHandler called if the execution of this task completed with failure.
		 */
		inline def subscribeAndSend(destination: ActorRef[A], inline isWithinDoSerEx: Boolean = isInSequence)(inline errorHandler: Throwable => Unit): Unit = {
			taskA.subscribeCallbacks(isWithinDoSerEx)(
				r => destination ! r,
				e => errorHandler(e)
			)
		}
	}

} 
