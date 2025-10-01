package readren.nexus
package examples

import core.*
import rf.RegularSf

import readren.sequencer.manager.ShutdownAbleDpm
import readren.sequencer.manager.descriptors.DefaultCooperativeWorkersDpd

import java.net.URI

object ExampleWithoutAskCapability {
	private sealed trait CalcCmd

	private case class Sum(a: Int, b: Int, replyTo: Endpoint[SumResult]) extends CalcCmd

	private case class SumResult(result: Int)

	@main def runExample(): Unit = {
		val uri = new URI(null, "localhost", null, null)
		val manager = new ShutdownAbleDpm
		val rootDoer = manager.provideDoer(DefaultCooperativeWorkersDpd, "root")
		val nexus = new NexusTyped(uri, rootDoer, manager)

		val calculatorDoer = nexus.provideDoer(DefaultCooperativeWorkersDpd, "calculator")
		nexus.spawns[CalcCmd, calculatorDoer.type](RegularSf, calculatorDoer)(_ => {
				case Sum(a, b, replyTo) =>
					replyTo.tell(SumResult(a + b))
					Continue
			}).flatMap { calculatorSpuron =>
				val endpointForCalculator = calculatorSpuron.endpointProvider.local[CalcCmd]

				val userDoer = calculatorSpuron.provideDoer(DefaultCooperativeWorkersDpd, "user")
				nexus.spawns[Started.type | SumResult, userDoer.type](RegularSf, userDoer) { userSpuron =>
					val userEndpoint = userSpuron.endpointProvider.local[SumResult]

					{
						case _: Started.type =>
							endpointForCalculator.tell(Sum(3, 7, userEndpoint))
							Continue

						case SumResult(result) =>
							println(s"3 + 7 = $result")
							Stop
					}
				}

			}
			.flatMap { userSpuron => userSpuron.stopDuty.onBehalfOf(nexus.doer) }
			.trigger() { _ =>
				manager.shutdown()
			}


	}
}