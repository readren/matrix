package readren.matrix
package pruebas

import core.*
import core.Matrix.DoerProviderDescriptor
import providers.doer.SharedQueueDoerProvider
import rf.RegularRf

object ExampleWithoutAskCapability {
	private sealed trait CalcCmd

	private case class Sum(a: Int, b: Int, replyTo: Endpoint[SumResult]) extends CalcCmd

	private case class SumResult(result: Int)

	object sharedQueueDpd extends DoerProviderDescriptor[SharedQueueDoerProvider.ProvidedDoer]("sharedQueue") {
		override def build(owner: Matrix.DoerProvidersManager): SharedQueueDoerProvider = new SharedQueueDoerProvider
	}

	@main def runExample(): Unit = {
		val aide = new AideImpl(sharedQueueDpd)
		val matrix = new Matrix("example", aide)

		matrix.spawns[CalcCmd](RegularRf)(calculatorRelay => {
			case Sum(a, b, replyTo) =>
				replyTo.tell(SumResult(a + b))
				Continue
		}
		).flatMap { calculatorReactant =>
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

		}.triggerAndForget()

		matrix.doerProvidersManager.shutdown()
	}
}