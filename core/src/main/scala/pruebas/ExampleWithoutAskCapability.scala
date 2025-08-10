package readren.matrix
package pruebas

import core.*
import rf.RegularRf
import utils.DefaultAide

object ExampleWithoutAskCapability {
	private sealed trait CalcCmd

	private case class Sum(a: Int, b: Int, replyTo: Endpoint[SumResult]) extends CalcCmd

	private case class SumResult(result: Int)

	@main def runExample(): Unit = {
		val matrix = new Matrix("example", DefaultAide)

		matrix.spawns[CalcCmd](RegularRf)(calculatorRelay => {
				case Sum(a, b, replyTo) =>
					replyTo.tell(SumResult(a + b))
					Continue
			}).flatMap { calculatorReactant =>
				val endpointForCalculator = calculatorReactant.endpointProvider.local[CalcCmd]

				matrix.spawns[Started.type | SumResult](RegularRf) { userReactant =>
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
				matrix.doerProvidersManager.shutdown()
			}


	}
}