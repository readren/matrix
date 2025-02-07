package readren.matrix
package pruebas

import core.Matrix.DoerProviderDescriptor
import core.{Continue, Matrix, Started, Stop}
import providers.doer.SchedulingDoerProvider
import rf.RegularRf

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

object PruebaScheduling {


	@main def runPruebaScheduling(): Unit = {

		case class Tick(incitingId: List[Int])

		val schedulingDpd = new DoerProviderDescriptor[SchedulingDoerProvider.ProvidedDoer, SchedulingDoerProvider]("schedulingDpd") {
			override def build(owner: Matrix.DoerProvidersManager): SchedulingDoerProvider = new SchedulingDoerProvider(false)
		}
		val aide = new AideImpl(schedulingDpd)

		val matrix = new Matrix("scheduled", aide)
		println(s"Matrix created")

		val schedulingDoer = matrix.provideDoer(schedulingDpd)

		if false then {
			@volatile var inside = false
			
			var count = 0
			val schedule = schedulingDoer.newFixedRateSchedule(1_000_000_000, 1_000)
			schedulingDoer.scheduleSequentially(schedule) { () =>
				assert(!inside)
				inside = true
				count += 1
				println(s"count=$count, numOfSkippedExecutions=${schedule.numOfSkippedExecutions}, diff=${(schedule.startingTime - schedule.scheduledTime) / 100}, thread=${Thread.currentThread().getId}")
				inside = false
			}

		} else {
			val diagnosticScheduler = new Scheduler

			matrix.spawns[Started.type | Tick](RegularRf, schedulingDoer) { reactant =>
				val selfEndpoint = reactant.endpointProvider.local[Tick]
				val interval = FiniteDuration(1, TimeUnit.SECONDS)
				var counter: Int = 0
				{
					case Started =>
						selfEndpoint.tell(Tick(List.empty))
						Continue

					case Tick(incitingId) =>
						counter += 1
						if counter > 1000 then {
							schedulingDoer.cancelAll()
							println("cancelAll executed")
							Stop
						} else {
//							println(s"The Tick number $counter was received, thread=${Thread.currentThread().getId}")
							val schedule: schedulingDoer.Schedule = schedulingDoer.newFixedRateSchedule((counter % 1000) * 1_000_000, 10_000_000)
							var repetitions = 0
							schedulingDoer.scheduleSequentially(schedule) { () =>
								println(f"counter=$counter%4d, repetitions=$repetitions%2d, enableDelay=${schedule.enabledTime - schedule.scheduledTime}%9d, runDelay=${schedule.startingTime - schedule.enabledTime}%9d, thread=${Thread.currentThread().getId}%3d, numOfPendingTasks=${schedulingDoer.schedulingAssistant.numOfPendingTasks}%3d, incitingId=$incitingId")
								selfEndpoint.tell(Tick(counter :: incitingId))
								repetitions += 1
							}
							//					timedDoer.Duty.delay(interval) { () =>
							//							selfEndpoint.tell(Tick)
							//							selfEndpoint.tell(Tick)
							//						}
							//						.triggerAndForget()
							Continue
						}
				}
			}.trigger() { parent =>
				parent.stopDuty.trigger() { _ =>
					schedulingDoer.scheduleSequentially(schedulingDoer.newDelaySchedule(100_000_000L)) { () =>
						matrix.doerProvidersManager.shutdown()
						println("shutdown executed")

						diagnosticScheduler.schedule(4000, TimeUnit.MILLISECONDS) { () =>

							try {
								val sb = new StringBuilder
								sb.append("\n<<< Inspector <<<\n")
								sb.append(
									s"""Parent's diagnostic: ${parent.staleDiagnose}
									   |SchedulingDoer's diagnostic: ${matrix.doerProvidersManager.diagnose(sb)}
									   |""".stripMargin

								)
								sb.append("\n>>> Inspector >>>\n")
								println(sb)
							} catch {
								case e: Throwable =>
									e.printStackTrace()
									throw e
							}
						}

					}
				}
			}
		}
	}
}
