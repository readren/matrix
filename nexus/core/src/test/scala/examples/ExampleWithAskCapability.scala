package readren.nexus
package examples

import behaviors.Inquisitive.*
import core.*
import rf.RegularSf

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
		val nexus = new NexusTyped(uri, rootDoer, manager)

		val calculatorDoer = nexus.provideDoer(DefaultCooperativeWorkersDpd, "calculator")
		nexus.spawns[CalcCmd, calculatorDoer.type](RegularSf, calculatorDoer)(_ => {
				case Sum(a, b, replyTo, questionId) =>
					replyTo.tell(SumResult(a + b, questionId))
					Continue
			})
			.flatMap { calculator =>
				val calculatorEndpoint = calculator.endpointProvider.local[CalcCmd]

				val userDoer = calculator.provideDoer(DefaultCooperativeWorkersDpd, "user")
				nexus.spawns[Started.type | SumResult, userDoer.type](RegularSf, userDoer) { userSpuron =>
					val userEndpoint = userSpuron.endpointProvider.local[SumResult]

					behaviors.inquisitiveNest(userSpuron)(new Behavior[Started.type] {
						override def handle(message: Started.type): HandleResult[Started.type] =
							calculatorEndpoint.ask(questionId => Sum(3, 7, userEndpoint, questionId))
								.trigger(true) { answer =>
									println(s"3 + 7 = ${answer.result}")
									userSpuron.stop()
								}
							Continue
					})()
				}

			}
			.flatMap { userSpuron => userSpuron.stopDuty.onBehalfOf(nexus.doer) }
			.trigger() { _ =>
				manager.shutdown()
			}
	}
}