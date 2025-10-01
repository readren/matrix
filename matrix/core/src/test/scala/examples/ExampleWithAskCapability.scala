package readren.matrix
package examples

import behaviors.Inquisitive.*
import core.*
import rf.RegularRf

import readren.sequencer.manager.ShutdownAbleDpm
import readren.sequencer.manager.descriptors.DefaultCooperativeWorkersDpd

import java.net.URI

object ExampleWithAskCapability {
	sealed trait CalcCmd

	case class Sum(a: Int, b: Int, replyTo: Endpoint[SumResult], questionId: QuestionId) extends CalcCmd, Question[SumResult]

	case class SumResult(result: Int, toQuestion: QuestionId) extends Answer


	@main def runAskCapabilityExample(): Unit = {

		val uri = new URI(null, "localhost", null, null)
		val manager = new ShutdownAbleDpm
		val rootDoer = manager.provideDoer(DefaultCooperativeWorkersDpd, "root")
		val matrix = new Matrix(uri, rootDoer, manager)

		val calculatorDoer = matrix.provideDoer(DefaultCooperativeWorkersDpd, "calculator")
		matrix.spawns[CalcCmd, calculatorDoer.type](RegularRf, calculatorDoer)(_ => {
				case Sum(a, b, replyTo, questionId) =>
					replyTo.tell(SumResult(a + b, questionId))
					Continue
			})
			.flatMap { calculator =>
				val calculatorEndpoint = calculator.endpointProvider.local[CalcCmd]

				val userDoer = calculator.provideDoer(DefaultCooperativeWorkersDpd, "user")
				matrix.spawns[Started.type | SumResult, userDoer.type](RegularRf, userDoer) { userReactant =>
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

			}
			.flatMap { userReactant => userReactant.stopDuty.onBehalfOf(matrix.doer) }
			.trigger() { _ =>
				manager.shutdown()
			}
	}
}