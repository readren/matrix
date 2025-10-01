package readren.matrix
package examples

import core.*
import rf.RegularRf

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
		val matrix = new Matrix(uri, rootDoer, manager)

		val calculatorDoer = matrix.provideDoer(DefaultCooperativeWorkersDpd, "calculator")
		matrix.spawns[CalcCmd, calculatorDoer.type](RegularRf, calculatorDoer)(_ => {
				case Sum(a, b, replyTo) =>
					replyTo.tell(SumResult(a + b))
					Continue
			}).flatMap { calculatorReactant =>
				val endpointForCalculator = calculatorReactant.endpointProvider.local[CalcCmd]

				val userDoer = calculatorReactant.provideDoer(DefaultCooperativeWorkersDpd, "user")
				matrix.spawns[Started.type | SumResult, userDoer.type](RegularRf, userDoer) { userReactant =>
					val userEndpoint = userReactant.endpointProvider.local[SumResult]

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
			.flatMap { userReactant => userReactant.stopDuty.onBehalfOf(matrix.doer) }
			.trigger() { _ =>
				manager.shutdown()
			}


	}
}