package readren.matrix
package pruebas

import behaviors.Inquisitive.*
import core.*
import core.Matrix.DoerProviderDescriptor
import providers.doer.SharedQueueDoerProvider
import rf.RegularRf

object ExampleWithAskCapability {
	sealed trait CalcCmd

	case class Sum(a: Int, b: Int, replyTo: Endpoint[SumResult], questionId: QuestionId) extends CalcCmd, Question[SumResult]

	case class SumResult(result: Int, toQuestion: QuestionId) extends Answer


	object sharedQueueDpd extends DoerProviderDescriptor[SharedQueueDoerProvider.ProvidedDoer]("sharedQueue") {
		override def build(owner: Matrix.DoerProvidersManager): Matrix.DoerProvider[SharedQueueDoerProvider.ProvidedDoer] = new SharedQueueDoerProvider
	}
	
	@main def runAskCapabilityExample(): Unit = {

		val aide = new AideImpl(sharedQueueDpd)

		val matrix = new Matrix("example", aide)

		matrix.spawns[CalcCmd](RegularRf)(calculatorRelay => {
			case Sum(a, b, replyTo, questionId) =>
				replyTo.tell(SumResult(a + b, questionId))
				Continue
		}).flatMap { calculatorReactant =>
			val calculatorEndpoint = calculatorReactant.endpointProvider.local[CalcCmd]

			matrix.spawns[Started.type | SumResult](RegularRf) { userReactant =>
				val userEndpoint = userReactant.endpointProvider.local[SumResult]

				behaviors.inquisitiveNest(userReactant)(new Behavior[Started.type] {
					override def handle(message: Started.type): HandleResult[Started.type] =
						calculatorEndpoint.ask(questionId => Sum(3, 7, userEndpoint, questionId))
							.trigger(true) { answer =>
								println(s"3 + 7 = ${answer.result}")
								userReactant.stop()
							}
						Continue
				})()
			}

		}.triggerAndForget()

		matrix.doerProvidersManager.shutdown()
	}
}