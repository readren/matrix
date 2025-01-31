package readren.matrix
package pruebas

import core.Matrix.DoerProviderDescriptor
import core.{Continue, Matrix, Started, Stop}
import providers.doer.SchedulingDoerProvider
import rf.RegularRf

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

object PruebaScheduling {


	@main def runPruebaTimed(): Unit = {

		case object Tick

		val schedulingDpd = new DoerProviderDescriptor[SchedulingDoerProvider.ProvidedDoer, SchedulingDoerProvider]("schedulingDpd") {
			override def build(owner: Matrix.DoerProvidersManager): SchedulingDoerProvider = new SchedulingDoerProvider(false)
		}
		val aide = new AideImpl(schedulingDpd)

		val matrix = new Matrix("scheduled", aide)
		println(s"Matrix created")

		val timedDoer = matrix.provideDoer(schedulingDpd)

		if false then {
			@volatile var inside = false
			
			var count = 0
			val schedule = timedDoer.newFixedRateSchedule(1_000_000_000, 1_000)
			timedDoer.scheduleSequentially(schedule) { () =>
				assert(!inside)
				inside = true
				count += 1
				println(s"count=$count, numOfSkippedExecutions=${schedule.numOfSkippedExecutions}, diff=${(schedule.startingTime - schedule.scheduledTime) / 100}, thread=${Thread.currentThread().getId}")
				inside = false
			}

		} else {
			matrix.spawns[Started.type | Tick.type](RegularRf, timedDoer) { reactant =>
				val selfEndpoint = reactant.endpointProvider.local[Tick.type]
				val interval = FiniteDuration(1, TimeUnit.SECONDS)
				var counter: Int = 0
				{
					case m@(Started | Tick) =>
						counter += 1
						if counter > 10000 then {
							timedDoer.cancelAll()
							Stop
						} else {
							println(s"The $m number $counter was received, thread=${Thread.currentThread().getId}")
							val schedule: timedDoer.Schedule = timedDoer.newFixedRateSchedule((counter % 1000) * 1_000_000, 1_000_000_000)
							timedDoer.scheduleSequentially(schedule) { () =>
								println(s"counter=$counter, diff=${schedule.startingTime - schedule.scheduledTime}, thread=${Thread.currentThread().getId}")
								selfEndpoint.tell(Tick)
							}
							//					timedDoer.Duty.delay(interval) { () =>
							//							selfEndpoint.tell(Tick)
							//							selfEndpoint.tell(Tick)
							//						}
							//						.triggerAndForget()
							Continue
						}
				}
			}.triggerAndForget()
		}
	}
}
