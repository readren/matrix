package readren.nexus
package interactive

import core.{Continue, NexusTyped, Stop}
import factories.RegularAf

import readren.sequencer.manager.ShutdownAbleDpm
import readren.sequencer.manager.descriptors.{DefaultCooperativeWorkersDpd, DefaultSyncSchedulingDpd}

import java.net.URI
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

object PruebaScheduling {


	@main def runPruebaScheduling(): Unit = {

		case class Tick(incitingId: List[Int])

		val uri = new URI(null, "localhost", null, null)
		val manager = new ShutdownAbleDpm
		val rootDoer = manager.provideDoer(DefaultCooperativeWorkersDpd, "root")
		val nexus = new NexusTyped(uri, rootDoer, manager)
		println(s"Nexus created")

		val schedulingDoer = nexus.provideDoer(DefaultSyncSchedulingDpd, "scheduling-doer")

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

			nexus.createsActant[Tick, schedulingDoer.type](RegularAf, schedulingDoer) { actant =>
				val tickSelfReceptor = actant.receptorProvider.local[Tick]
				tickSelfReceptor.tell(Tick(List.empty))
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
								tickSelfReceptor.tell(Tick(counter :: incitingId))
								repetitions += 1
							}
							Continue
						}
				}
			}.trigger() { parent =>
				parent.stopDuty.trigger() { _ =>
					println(s"Diagnostics:\n${manager.diagnose(new StringBuilder())}")

					manager.shutdown()
					println("shutdown executed")

					diagnosticScheduler.fixedRate(0, 4000, TimeUnit.MILLISECONDS) { () =>

						try {
							val sb = new StringBuilder
							sb.append("\n<<< Inspector <<<\n")
							sb.append(
								s"""Parent's diagnostic: ${parent.staleDiagnose}
								   |SchedulingDoer's diagnostic: ${manager.diagnose(sb)}
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
