package readren.sequencer.akka

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorSystem, Behavior}

object PruebaScheduling {

	case class Tick(incitingTickId: List[Int])

	@main def runScheduling(): Unit = {
		val system = ActorSystem(buildBehavior(), "Prueba")
	}

	def buildBehavior(): Behavior[Tick] = {
		Behaviors.setup { actorCtx =>
			actorCtx.self.tell(Tick(Nil))

			Behaviors.withTimers { timerScheduler =>
				ActorBasedSchedulingDoer.setup(actorCtx, timerScheduler) { actorBasedSchedulingDoer =>
					var ticksCounter: Int = 0
					Behaviors.receiveMessage {
						case Tick(incitingTickId) =>
							ticksCounter += 1
							if ticksCounter > 1000 then {
								actorBasedSchedulingDoer.cancelAll()
								println("Cancel all was executed")
								Behaviors.stopped
							} else {
								val schedule = actorBasedSchedulingDoer.newFixedRateSchedule((ticksCounter % 1000) * 1, 10)
								var repetitions = 0
								actorBasedSchedulingDoer.scheduleSequentially(schedule) { () =>
									println(f"counter=$ticksCounter%4d, repetitions=$repetitions%2d, thread=${Thread.currentThread().getId}%3d, incitingId=$incitingTickId")
									actorCtx.self.tell(Tick(ticksCounter :: incitingTickId))
									repetitions += 1
								}
								Behaviors.same
							}
					}
				}
			}
		}
	}
}
