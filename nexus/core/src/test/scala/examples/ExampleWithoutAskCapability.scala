package readren.nexus
package examples

import core.*
import factories.RegularAf

import readren.sequencer.manager.ShutdownAbleDpm
import readren.sequencer.manager.descriptors.DefaultCooperativeWorkersDpd

import java.net.URI

object ExampleWithoutAskCapability {
	private sealed trait CalcCmd

	private case class Sum(a: Int, b: Int, replyTo: Receptor[SumResult]) extends CalcCmd

	private case class SumResult(result: Int)

	@main def runExample(): Unit = {
		val uri = new URI(null, "localhost", null, null)
		val manager = new ShutdownAbleDpm
		val rootDoer = manager.provideDoer(DefaultCooperativeWorkersDpd, "root")
		val nexus = new NexusTyped(uri, rootDoer, manager)

		val calculatorDoer = nexus.provideDoer(DefaultCooperativeWorkersDpd, "calculator")
		nexus.createsActant[CalcCmd, calculatorDoer.type](RegularAf, calculatorDoer)(_ => {
				case Sum(a, b, replyTo) =>
					replyTo.tell(SumResult(a + b))
					Continue
			}).flatMap { calculator =>
				val receptorForCalculator = calculator.receptorProvider.local[CalcCmd]

				val userDoer = calculator.provideDoer(DefaultCooperativeWorkersDpd, "user")
				nexus.createsActant[Started.type | SumResult, userDoer.type](RegularAf, userDoer) { user =>
					val userReceptor = user.receptorProvider.local[SumResult]

					{
						case _: Started.type =>
							receptorForCalculator.tell(Sum(3, 7, userReceptor))
							Continue

						case SumResult(result) =>
							println(s"3 + 7 = $result")
							Stop
					}
				}

			}
			.flatMap { user => user.stopDuty.onBehalfOf(nexus.doer) }
			.trigger() { _ =>
				manager.shutdown()
			}


	}
}