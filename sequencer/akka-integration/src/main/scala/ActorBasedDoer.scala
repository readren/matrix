package readren.sequencer.akka

import ActorExtension.Aide

import akka.actor.typed.*
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import readren.sequencer.AbstractDoer

import scala.reflect.Typeable

object ActorBasedDoer {

	private[akka] val currentAssistant: ThreadLocal[Aide] = new ThreadLocal()

	private[sequencer] case class Procedure(runnable: Runnable)

	/** A [[Behavior]] factory that provides access to an [[ActorBasedDoer]] whose DoSiThEx (doer single thread executor) is the actor corresponding to the provided [[ActorContext]]. */
	def setup[A: Typeable](ctxA: ActorContext[A])(frontier: ActorBasedDoer => Behavior[A]): Behavior[A] = {
		val aide = buildAide(ctxA.asInstanceOf[ActorContext[Procedure]])
		val doer: ActorBasedDoer = new ActorBasedDoer(aide);
		val behaviorA = frontier(doer)
		val interceptor = buildProcedureInterceptor[A](aide)
		Behaviors.intercept(() => interceptor)(behaviorA).narrow
	}


	private[sequencer] def buildAide[A >: Procedure](ctx: ActorContext[A]): Aide = new Aide {
		override def executeSequentially(runnable: Runnable): Unit = ctx.self ! Procedure(runnable)

		override def current: Aide = currentAssistant.get

		override def reportFailure(cause: Throwable): Unit = ctx.log.error("""Error occurred while the actor "{}" was executing a Runnable within a Task.""", ctx.self, cause)

		override def akkaScheduler: Scheduler = ctx.system.scheduler
	}

	def buildProcedureInterceptor[A](aide: Aide): BehaviorInterceptor[A | Procedure, A] =
		new BehaviorInterceptor[Any, A](classOf[Any]) {
			override def aroundReceive(ctxU: TypedActorContext[Any], message: Any, target: BehaviorInterceptor.ReceiveTarget[A]): Behavior[A] = {
				currentAssistant.set(aide)
				try {
					message match {
						case procedure: Procedure =>
							procedure.runnable.run()
							Behaviors.same // TODO: analyze if returning `same` may cause problems in edge cases

						case a: A @unchecked =>
							target(ctxU, a)
					}
				} finally currentAssistant.remove()
			}
		}.asInstanceOf[BehaviorInterceptor[A | Procedure, A]]
}

/** A [[Doer]], extended with akka-actor related operations, whose DoSiThEx (doer single thread executor) is an akka-actor. */
class ActorBasedDoer(aide: Aide) extends AbstractDoer, ActorExtension {
	override type Assistant = Aide
	override val assistant: Assistant = aide
}