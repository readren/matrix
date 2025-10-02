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

	case class Sum(a: Int, b: Int, replyTo: Receptor[SumResult], questionId: QuestionId) extends CalcCmd, Question[SumResult]

	case class SumResult(result: Int, toQuestion: QuestionId) extends Answer


	@main def runAskCapabilityExample(): Unit = {

		val uri = new URI(null, "localhost", null, null)
		val manager = new ShutdownAbleDpm
		val rootDoer = manager.provideDoer(DefaultCooperativeWorkersDpd, "root")
		val nexus = new NexusTyped(uri, rootDoer, manager)

		val calculatorDoer = nexus.provideDoer(DefaultCooperativeWorkersDpd, "calculator")
		nexus.createsActant[CalcCmd, calculatorDoer.type](RegularSf, calculatorDoer)(_ => {
				case Sum(a, b, replyTo, questionId) =>
					replyTo.tell(SumResult(a + b, questionId))
					Continue
			})
			.flatMap { calculator =>
				val calculatorReceptor = calculator.receptorProvider.local[CalcCmd]

				val userDoer = calculator.provideDoer(DefaultCooperativeWorkersDpd, "user")
				nexus.createsActant[Started.type | SumResult, userDoer.type](RegularSf, userDoer) { user =>
					val userReceptor = user.receptorProvider.local[SumResult]

					behaviors.inquisitiveNest(user)(new Behavior[Started.type] {
						override def handle(message: Started.type): HandleResult[Started.type] =
							calculatorReceptor.ask(questionId => Sum(3, 7, userReceptor, questionId))
								.trigger(true) { answer =>
									println(s"3 + 7 = ${answer.result}")
									user.stop()
								}
							Continue
					})()
				}

			}
			.flatMap { user => user.stopDuty.onBehalfOf(nexus.doer) }
			.trigger() { _ =>
				manager.shutdown()
			}
	}
}