package readren.matrix
package dap

import munit.ScalaCheckEffectSuite
import readren.taskflow.Doer

import scala.collection.immutable.ArraySeq
import scala.concurrent.{ExecutionContext, Future, Promise}


class DoerAssistantProviderTest extends ScalaCheckEffectSuite {

	private val provider = new SharedQueueDoerAssistantProvider

	class AssistantData(serial: Long) {
		val assistant: Doer.Assistant = provider.provide(serial)
		val promise: Promise[Unit] = Promise()
		var counter: Int = 0
		var previousTaskWorkerIndex: Int = -1
		var workerIndexChangesCounter: Int = 0
	}

	private def testVisibility(provider: Matrix.DoerAssistantProvider): Future[ArraySeq[Unit]] = {
		val assistantsData = ArraySeq.tabulate(1000)(AssistantData(_))

		for expected <- 0 to 1000 do {

			for doerIndex <- assistantsData.indices yield {
				val doerData = assistantsData(doerIndex)
				doerData.assistant.queueForSequentialExecution { () =>
					val currentWorkerIndex = SharedQueueDoerAssistantProvider.workerIndexThreadLocal.get()
					if doerData.previousTaskWorkerIndex != currentWorkerIndex then {
						doerData.workerIndexChangesCounter += 1
						doerData.previousTaskWorkerIndex = currentWorkerIndex
					}
					if doerData.counter != expected then doerData.promise.failure(new AssertionError(s"expected: $expected, found: $doerData.counter"))
					doerData.counter = doerData.counter + 1
				}
			}
		}
		for doerData <- assistantsData do doerData.assistant.queueForSequentialExecution(() => doerData.promise.success(()))

		given ExecutionContext = ExecutionContext.global

		Future.sequence(assistantsData.map(_.promise.future))
		//	.andThen { _ => for doerData <- doersData do println(s"workerIndexChangesCounter=${doerData.workerIndexChangesCounter}") }

	}

	test("Tasks should see updates made by previous tasks enqueued into the same assistant") {

		testVisibility(new SimpleDoerAssistantProvider)
		testVisibility(new BalancedDoerAssistantProvider)
		testVisibility(new SharedQueueDoerAssistantProvider)
	}
}
