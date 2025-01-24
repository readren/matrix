package readren.matrix
package pruebas

import core.*
import core.Matrix.DapKind
import dap.{SharedQueueDoerAssistantProvider, SimpleDoerAssistantProvider}
import rf.RegularRf

object ExampleWithoutAskCapability {
	private sealed trait CalcCmd

	private case class Sum(a: Int, b: Int, replyTo: Endpoint[SumResult]) extends CalcCmd

	private case class SumResult(result: Int)

	private object sharedQueueDapKind extends DapKind[SharedQueueDoerAssistantProvider]("sharedQueue") {
		override def build(owner: Matrix.DapManager): SharedQueueDoerAssistantProvider = new SharedQueueDoerAssistantProvider(false)
	}

	@main def runExample(): Unit = {
		val aide = new AideImpl(sharedQueueDapKind)
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

		matrix.dapManager.shutdown()
	}
}