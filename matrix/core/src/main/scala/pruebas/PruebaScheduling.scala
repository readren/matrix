package readren.matrix
package pruebas

import core.{Continue, Matrix, Stop}
import providers.descriptor.{DefaultAsyncSchedulingDpd, DefaultSyncSchedulingDpd}
import rf.RegularRf
import utils.DefaultAide

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

object PruebaScheduling {


	@main def runPruebaScheduling(): Unit = {

		case class Tick(incitingId: List[Int])

		val matrix = new Matrix("scheduled", DefaultAide)
		println(s"Matrix created")

		val schedulingDoer = matrix.provideDoer("scheduling-doer", DefaultSyncSchedulingDpd)

		if false then {
			@volatile var inside = false

			var counter = 0
			val schedule = schedulingDoer.newFixedRateSchedule(1_000, 1_000)
			schedulingDoer.schedule(schedule) { _ =>
				assert(!inside)
				inside = true
				counter += 1
				println(f"counter=$counter%4d, thread=${Thread.currentThread().getId}%3d, numOfPendingTasks=${schedulingDoer.numOfPendingTasks}%3d")
				inside = false
			}

		} else {
			val diagnosticScheduler = new Scheduler

			matrix.spawns[Tick](RegularRf, schedulingDoer) { reactant =>
				val selfEndpoint = reactant.endpointProvider.local[Tick]
				selfEndpoint.tell(Tick(List.empty))
				val interval = FiniteDuration(1, TimeUnit.SECONDS)
				var counter: Int = 0
				{
					case Tick(incitingId) =>
						counter += 1
						if counter > 1000 then {
							schedulingDoer.cancelAll()
							println("cancelAll executed")
							Stop
						} else {
							val schedule: schedulingDoer.Schedule = schedulingDoer.newFixedRateSchedule(counter % 10, 10)
							var repetitions = 0
							schedulingDoer.schedule(schedule) { _ =>
								println(f"counter=$counter%4d, repetitions=$repetitions%2d, thread=${Thread.currentThread().getId}%3d, numOfPendingTasks=${schedulingDoer.numOfPendingTasks}%3d, incitingId=$incitingId")
								selfEndpoint.tell(Tick(counter :: incitingId))
								repetitions += 1
							}
							Continue
						}
				}
			}.trigger() { parent =>
				parent.stopDuty.trigger() { _ =>
					println(s"Diagnostics:\n${matrix.doerProvidersManager.diagnose(new StringBuilder())}")

					matrix.doerProvidersManager.shutdown()
					println("shutdown executed")

					diagnosticScheduler.fixedRate(0, 4000, TimeUnit.MILLISECONDS) { () =>

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
