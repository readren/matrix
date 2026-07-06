package readren.nexus
package interactive

import core.{Continue, NexusTyped, Stop}
import factories.RegularAf

import readren.sequencer.manager.ShutdownAbleDpm
import readren.sequencer.manager.descriptors.{DefaultCooperativeWorkersDpd, DefaultPollingSchedulingDpd}
import readren.sequencer.providers.{CooperativeWorkersDp, CooperativeWorkersWithPollingSchedulerDp}

import java.net.URI
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

object PruebaScheduling {


	@main def runPruebaScheduling(): Unit = {

		case class Tick(incitingId: List[Int])

		val uri = new URI(null, "localhost", null, null)
		val manager = new ShutdownAbleDpm
		val rootDoer: CooperativeWorkersDp.DoerFacade = manager.provideDoer(DefaultCooperativeWorkersDpd, "root")
		val nexus = new NexusTyped(uri, rootDoer, manager)
		println(s"Nexus created")

		val schedulingDoer: CooperativeWorkersWithPollingSchedulerDp.SchedulingDoerFacade = nexus.provideDoer(DefaultPollingSchedulingDpd, "scheduling-doer")

		if false then {
			@volatile var inside = false

			var counter = 0
			val schedule = schedulingDoer.newFixedRateSchedule(1_000, 1_000)
			schedulingDoer.schedule(schedule) { _ =>
				assert(!inside)
				inside = true
				counter += 1
				println(f"counter=$counter%4d, thread=${Thread.currentThread().threadId}%3d, numOfPendingRunnables=${schedulingDoer.numOfPendingRunnables}%3d")
				inside = false
			}

		} else {
			val diagnosticScheduler = new Scheduler

			nexus.createActant[Tick, schedulingDoer.type](RegularAf, schedulingDoer) { actant =>
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
								println(f"counter=$counter%4d, repetitions=$repetitions%2d, thread=${Thread.currentThread().threadId}%3d, numOfPendingRunnables=${schedulingDoer.numOfPendingRunnables}%3d, incitingId=$incitingId")
								tickSelfReceptor.tell(Tick(counter :: incitingId))
								repetitions += 1
							}
							Continue
						}
				}
			}.triggerCallbacks(false)(
				parent => {
					nexus.doer.checkWithin()
					parent.stopCapturer.triggerCallbacks(false)(
						_ => {
							parent.doer.checkWithin()
							println(s"Diagnostics:\n${manager.diagnose(new StringBuilder())}")

							manager.shutdown()
							println("shutdown executed")

							diagnosticScheduler.fixedRate(0, 4000, TimeUnit.MILLISECONDS) { () =>

								parent.diagnose.foreach { parentDiagnostic =>
									try {
										val sb = new StringBuilder
										sb.append("\n<<< Inspector <<<\n")
										sb.append(
											s"""Parent's diagnostic: $parentDiagnostic
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
						},
						error => throw new Exception(error)
					)
				},
				error => throw new Exception(error)
			)
		}
	}
}
