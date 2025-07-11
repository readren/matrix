package readren.matrix
package providers.assistant

import providers.ShutdownAble

import munit.ScalaCheckEffectSuite
import readren.taskflow.Doer

import java.util.concurrent.TimeUnit
import scala.collection.immutable.ArraySeq
import scala.concurrent.{ExecutionContext, Future, Promise}


class DoerAssistantProviderTest extends ScalaCheckEffectSuite {

	private val NUMBER_OF_TASK_ENQUEUED_PER_ASSISTANT = 10
	private val NUMBER_OF_ASSISTANTS = 10000

	private def testVisibility(provider: DoerAssistantProvider[Doer.Assistant] & ShutdownAble, minimumThreadSwaps: Int): Future[Any] = {

		class Counter {
			var count: Int = 0
			// Padding to ensure the counter occupies a distinct cache line
			val p1, p2, p3, p4, p5, p6, p7, p8: Long = 0
		}
		class AssistantData(serial: Long) {
			val assistant: Doer.Assistant = provider.provide(serial)
			var failed: Boolean = false
			val promise: Promise[Int] = Promise()
			var counter: Counter | Null = null
			var previousTaskWorker: Runnable | Null = null
			var workerChangesCounter: Int = 0
		}

		val assistantsData = ArraySeq.tabulate(NUMBER_OF_ASSISTANTS)(AssistantData(_))

		for expected <- 0 until NUMBER_OF_TASK_ENQUEUED_PER_ASSISTANT do {

			for assistantDataIndex <- assistantsData.indices yield {
				val assistantData = assistantsData(assistantDataIndex)
				assistantData.assistant.executeSequentially { () =>
					// Track the number of times this code is executed by a different worker thread than the previous time.
					val currentWorker = CooperativeWorkersDap.currentWorker
					if currentWorker ne assistantData.previousTaskWorker then {
						assistantData.workerChangesCounter += 1
						assistantData.previousTaskWorker = currentWorker
					}
					// allocate the counters on different threads to encourage allocation on memory regions separated from the one where the assistantData object is stored.
					if assistantData.counter eq null then {
						// allocate large unrelated objects between counters to avoid
						val dummy = new Array[Long](1024)
						assistantData.counter = new Counter
					}
					if assistantData.counter.count != expected then {
						assistantData.failed = true
						assistantData.promise.failure(new AssertionError(s"expected: $expected, found: $assistantData.counter"))
					}
					assistantData.counter.count = assistantData.counter.count + 1
				}
			}
		}
		for assistantData <- assistantsData do assistantData.assistant.executeSequentially { () =>
			if !assistantData.failed then assistantData.promise.success(assistantData.workerChangesCounter)
		}

		given ExecutionContext = ExecutionContext.global

		Future.sequence(assistantsData.map(_.promise.future))
			.map { threadSwapsByAssistant => assert(threadSwapsByAssistant.sum >= minimumThreadSwaps * NUMBER_OF_ASSISTANTS, s"${threadSwapsByAssistant.sum} >= ${minimumThreadSwaps * NUMBER_OF_ASSISTANTS}") }
			.andThen { _ =>
				provider.shutdown()
				provider.awaitTermination(1, TimeUnit.SECONDS)
			}
//			.andThen { _ => for index <- assistantsData.indices do println(s"$index: workerIndexChangesCounter=${assistantsData(index).workerChangesCounter}") }
			.andThen { _ => println(s"total worker swaps: ${assistantsData.map(_.workerChangesCounter).sum}") }

	}

	test("CooperativeWorkersDap: Tasks should see updates made by previous tasks enqueued into the same assistant") {

		testVisibility(new CooperativeWorkersDap(false), NUMBER_OF_TASK_ENQUEUED_PER_ASSISTANT/20)
	}
	test("SchedulingDap: Tasks should see updates made by previous tasks enqueued into the same assistant") {

		testVisibility(new SchedulingDap(false), NUMBER_OF_TASK_ENQUEUED_PER_ASSISTANT/20)
	}
	test("LeastLoadedFixedWorkerDap: Tasks should see updates made by previous tasks enqueued into the same assistant") {

		testVisibility(new LeastLoadedFixedWorkerDap, 0)
	}
	test("RoundRobinDap: Tasks should see updates made by previous tasks enqueued into the same assistant") {

		testVisibility(new RoundRobinDap, 0)
	}
}
