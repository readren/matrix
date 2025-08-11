package readren.sequencer.akka

import akka.actor.typed.scaladsl.AskPattern.*
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.*
import akka.util.Timeout

import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext
import scala.io.StdIn
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

object Prueba {
	case class RunProcedure(procedure: () => Unit) extends AnyVal

	given Timeout = Timeout(3, TimeUnit.SECONDS)

	@main def probando(): Unit = {
		val system = ActorSystem(apply4(), "Prueba")

		given ExecutionContext = system.executionContext

		given Scheduler = system.scheduler

		for {
			Respuesta(replyTo1, text1) <- system.ask[Respuesta](ref => Pregunta(ref, "Hola"))
			_ = system.log.info(s"resultado: $text1")
			Respuesta(replyTo2, text2) <- replyTo1.ask[Respuesta](ref => Pregunta(ref, "¿Qué tal?"))
		} do {
			system.log.info(s"resultado: $text2")
		}


		StdIn.readLine()
		system.terminate()
	}

	sealed case class Pregunta(replyTo: ActorRef[Respuesta], text: String) {
		println(s"creando pregunta: replyTo=$replyTo, text=$text")
	}
	
	class Pepe extends Pregunta(null, null)

	case class Respuesta(replyTo: ActorRef[Pregunta], text: String)

	def apply1(): Behavior[Pregunta] = {
		Behaviors.setup[Pregunta] { ctx =>

			Behaviors.receiveMessage {
				case Pregunta(replyTo, "Hola") =>
					replyTo ! Respuesta(ctx.self, "Hola también")
					Behaviors.same

				case Pregunta(replyTo, "¿Qué tal?") =>
					replyTo ! Respuesta(ctx.self, "Muy bien, ¿y vos?")
					Behaviors.same
					
				case _ => throw AssertionError()	
			}
		}
	}

	def apply2(): Behavior[Pregunta] = {
		Behaviors.setup[Pregunta | RunProcedure] { ctx =>
			given Scheduler = ctx.system.scheduler;

			given ejecutadoPorEsteActorVar: ExecutionContext = new ExecutionContext {
				def execute(runnable: Runnable): Unit =
					ctx.asScala.self ! RunProcedure(() => runnable.run())

				def reportFailure(cause: Throwable): Unit =
					ctx.asScala.log.error("Reporte de computación asincrónica fallida en actor {}", ctx.asScala.self, cause);
			}

			extension [Question](refA: ActorRef[Question]) {
				def preguntar[Answer](questionBuilder: ActorRef[Answer] => Question)(onResponse: Answer => Unit)(using timeout: Timeout): Unit = {
					refA.ask[Answer](questionBuilder).onComplete {
						case Success(answer) => onResponse(answer)
						case Failure(e) => throw e
					}
				}
			}


			Behaviors.receiveMessage {
				case RunProcedure(procedure) =>
					procedure()
					Behaviors.same

				case Pregunta(replyTo, "Hola") =>
					ctx.log.info("received: Hola")

					replyTo.preguntar[Pregunta](ref => Respuesta(ref, "Hola también")) { pregunta =>
						if pregunta.text == "¿Qué tal?" then {
							ctx.log.info("received: ¿Qué tal?")
							pregunta.replyTo ! Respuesta(ctx.self, "Muy bien, y vos?")
						} else {
							ctx.log.info(s"pregunta inesperada: $pregunta")
						}
					}
					Behaviors.same

				case unhandled => ctx.log.info(s"unhandled message: $unhandled")
					Behaviors.same

			}
		}.narrow
	}

	def apply30(): Behavior[Pregunta] = {
		Behaviors.setup { ctx =>
			ActorBasedDoer.setup[Pregunta](ctx) { doer =>
				import doer.*

				Behaviors.receiveMessage {
					case Pregunta(replyTo1, "Hola") =>
						val task = for {
							case Pregunta(replyTo2, "¿Qué tal?") <- replyTo1.queries[Pregunta](ref => Respuesta(ref, "Hola también"))
							_ <- replyTo2.says(Respuesta(null, "Muy bien, ¿y vos?"))
						} yield ()
						task.trigger(true) { x => ctx.log.info(s"resultado final: $x") }
						Behaviors.same
				}
			}
		}
	}

	def apply31(): Behavior[Pregunta] = {
		Behaviors.setup { ctx =>
			ActorBasedDoer.setup[Pregunta](ctx) { doer =>
				import doer.*

				val flow = Flow.wrap[ActorRef[Respuesta], Try[Unit]] { replyTo1 =>
					for {
						case Pregunta(replyTo2, "¿Qué tal?") <- replyTo1.queries[Pregunta](ref => Respuesta(ref, "Hola también"))
						_ <- replyTo2.says(Respuesta(null, "Muy bien, ¿y vos?"))
					} yield ()					
				}
				Behaviors.receiveMessage {
					case Pregunta(replyTo1, "Hola") =>
						flow.apply(replyTo1, true) { x => ctx.log.info(s"resultado final: $x") }
						Behaviors.same
				}
			}
		}
	}
	
	def apply4(): Behavior[Pregunta] = {
		Behaviors.setup { ctx =>
			ActorBasedDoer.setup[Pregunta](ctx) { doer =>
				import doer.*

				val paso2 = Behaviors.receiveMessage[Pregunta] {
					case Pregunta(replyTo2, "¿Qué tal?") =>
						val task = for {
							_ <- Task.mine(() => ctx.log.info("sigue funcionando"))
							_ <- replyTo2.says(Respuesta(null, "Muy bien, ¿y vos?"))
						} yield ()
						task.trigger(true)(rf => ctx.log.info(s"resultado final: $rf"))
						Behaviors.same
						
					case x => println(s"unhandled message: $x")
						Behaviors.unhandled
				}

				val paso1 = Behaviors.receiveMessage[Pregunta] {
					case Pregunta(replyTo1, "Hola") =>
						replyTo1 ! Respuesta(ctx.self, "Hola también")
						paso2
						
					case x => println(s"unhandled message: $x")
						Behaviors.unhandled
				}

				paso1
			}
		}
	}

	def build[A, B](behaviorA: Behavior[A], bHandler: B => Unit)(using ctAuB: ClassTag[A | B], ctA: ClassTag[A], ctB: ClassTag[B]): Behavior[A | B] = {
		def interceptor() = new BehaviorInterceptor[A | B, A] {

			override def aroundReceive(ctx: TypedActorContext[A | B], msg: A | B, target: BehaviorInterceptor.ReceiveTarget[A]): Behavior[A] = msg match {
				case b: B => bHandler(b)
					Behaviors.same
				case a: A => target(ctx, a)
			}
		}

		Behaviors.intercept[A | B, A](interceptor)(behaviorA)
	}
}

