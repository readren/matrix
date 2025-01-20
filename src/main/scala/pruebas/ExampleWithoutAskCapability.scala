package readren.matrix
package pruebas

import core.*
import dap.SharedQueueDoerAssistantProvider
import rf.RegularRf

object ExampleWithoutAskCapability {
	sealed trait CalcCmd

	case class Sum(a: Int, b: Int, replyTo: Endpoint[SumResult]) extends CalcCmd

	case class SumResult(result: Int)

	@main def runExample(): Unit = {
		val aide = new AideImpl((owner: Matrix.DoerAssistantProviderManager) => new SharedQueueDoerAssistantProvider(false))
		val matrix = new Matrix("example", aide)

		matrix.spawn[CalcCmd](RegularRf)(calculatorRelay => {
			case Sum(a, b, replyTo) =>
				replyTo.tell(SumResult(a + b))
				Continue
		}
		).flatMap { calculatorReactant =>
			val endpointForCalculator = calculatorReactant.endpointProvider.local[CalcCmd]

			matrix.spawn[Started.type | SumResult](RegularRf) { userReactant =>
				val userEndpoint = userReactant.endpointProvider.local[SumResult]

				{
					case _: Started.type => endpointForCalculator.tell(Sum(3, 7, userEndpoint))
						Continue

					case SumResult(result) =>
						println(s"3 + 7 = $result")
						Stop
				}
			}

		}.triggerAndForget()
		
		matrix.doerAssistantProviderManager.shutdown()
	}
}