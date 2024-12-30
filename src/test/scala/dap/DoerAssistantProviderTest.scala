package readren.matrix
package dap

import munit.ScalaCheckEffectSuite
import readren.taskflow.Doer

import java.util.concurrent.TimeUnit
import scala.collection.immutable.ArraySeq
import scala.concurrent.{ExecutionContext, Future, Promise}


class DoerAssistantProviderTest extends ScalaCheckEffectSuite {

	private val NUMBER_OF_TASK_ENQUEUED_PER_ASSISTANT = 1000
	private val NUMBER_OF_ASSISTANTS = 1000

	private def testVisibility(provider: Matrix.DoerAssistantProvider & ShutdownAble, minimumThreadSwaps: Int): Future[Any] = {

		class Counter {
			var count: Int = 0
			// Padding to ensure the counter occupies a distinct cache line
			val p1, p2, p3, p4, p5, p6, p7, p8: Long = 0
		}
		class AssistantData(serial: Long) {
			val assistant: Doer.Assistant = provider.provide(serial)
			val promise: Promise[Int] = Promise()
			var counter: Counter | Null = null
			var previousTaskWorkerIndex: Int = -1
			var workerIndexChangesCounter: Int = 0
		}

		val assistantsData = ArraySeq.tabulate(NUMBER_OF_ASSISTANTS)(AssistantData(_))

		for expected <- 0 until NUMBER_OF_TASK_ENQUEUED_PER_ASSISTANT do {

			for assistantDataIndex <- assistantsData.indices yield {
				val assistantData = assistantsData(assistantDataIndex)
				assistantData.assistant.queueForSequentialExecution { () =>
					val currentWorkerIndex = SharedQueueDoerAssistantProvider.workerIndexThreadLocal.get()
					if assistantData.previousTaskWorkerIndex != currentWorkerIndex then {
						assistantData.workerIndexChangesCounter += 1
						assistantData.previousTaskWorkerIndex = currentWorkerIndex
					}
					// allocate the counters on different threads to encourage allocation on a memory regions separated from the one where the assistantData object is stored.
					if assistantData.counter == null then {
						// allocate large unrelated objects between counters to avoid
						val dummy = new Array[Long](1024)
						assistantData.counter = new Counter
					}
					if assistantData.counter.count != expected then assistantData.promise.failure(new AssertionError(s"expected: $expected, found: $assistantData.counter"))
					assistantData.counter.count = assistantData.counter.count + 1
				}
			}
		}
		for assistantData <- assistantsData do assistantData.assistant.queueForSequentialExecution { () =>
			assistantData.promise.success(assistantData.workerIndexChangesCounter)
		}

		given ExecutionContext = ExecutionContext.global

		Future.sequence(assistantsData.map(_.promise.future))
			.map { threadSwapsByAssistant => assert(threadSwapsByAssistant.sum() >= minimumThreadSwaps * NUMBER_OF_ASSISTANTS) }
			.andThen { _ =>
				provider.shutdown()
				provider.awaitTermination(1, TimeUnit.SECONDS)
			}
			// .andThen { _ => for assistant <- assistantsData do println(s"workerIndexChangesCounter=${assistant.workerIndexChangesCounter}") }

	}

	test("SharedQueueDoerAssistantProvider: Tasks should see updates made by previous tasks enqueued into the same assistant") {

		testVisibility(new SharedQueueDoerAssistantProvider(false), NUMBER_OF_TASK_ENQUEUED_PER_ASSISTANT/2)
	}
	test("BalancedDoerAssistantProvider: Tasks should see updates made by previous tasks enqueued into the same assistant") {

		testVisibility(new BalancedDoerAssistantProvider, 0)
	}
	test("SimpleDoerAssistant: Tasks should see updates made by previous tasks enqueued into the same assistant") {

		testVisibility(new SimpleDoerAssistantProvider, 0)
	}
}
