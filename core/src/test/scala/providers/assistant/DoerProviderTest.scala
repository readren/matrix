package readren.matrix
package providers.assistant

import providers.ShutdownAble

import munit.ScalaCheckEffectSuite
import readren.sequencer.Doer

import java.util.concurrent.TimeUnit
import scala.collection.immutable.ArraySeq
import scala.concurrent.{ExecutionContext, Future, Promise}


class DoerProviderTest extends ScalaCheckEffectSuite {

	private val NUMBER_OF_TASK_ENQUEUED_PER_DOER = 10
	private val NUMBER_OF_DOERS = 10000

	private def testVisibility(provider: DoerProvider[Doer] & ShutdownAble, minimumThreadSwaps: Int): Future[Any] = {

		class Counter {
			var count: Int = 0
			// Padding to ensure the counter occupies a distinct cache line
			val p1, p2, p3, p4, p5, p6, p7, p8: Long = 0
		}
		class DoerData(serial: Long) {
			val doer: Doer = provider.provide(serial.toString)
			var failed: Boolean = false
			val promise: Promise[Int] = Promise()
			var counter: Counter | Null = null
			var previousTaskWorker: Runnable | Null = null
			var workerChangesCounter: Int = 0
		}

		val doersData = ArraySeq.tabulate(NUMBER_OF_DOERS)(DoerData(_))

		for expected <- 0 until NUMBER_OF_TASK_ENQUEUED_PER_DOER do {

			for doerDataIndex <- doersData.indices yield {
				val doerData = doersData(doerDataIndex)
				doerData.doer.executeSequentially { () =>
					// Track the number of times this code is executed by a different worker thread than the previous time.
					val currentWorker = CooperativeWorkersDap.currentWorker
					if currentWorker ne doerData.previousTaskWorker then {
						doerData.workerChangesCounter += 1
						doerData.previousTaskWorker = currentWorker
					}
					// allocate the counters on different threads to encourage allocation on memory regions separated from the one where the doerData object is stored.
					if doerData.counter eq null then {
						// allocate large unrelated objects between counters to avoid
						val dummy = new Array[Long](1024)
						doerData.counter = new Counter
					}
					if doerData.counter.count != expected then {
						doerData.failed = true
						doerData.promise.failure(new AssertionError(s"expected: $expected, found: $doerData.counter"))
					}
					doerData.counter.count = doerData.counter.count + 1
				}
			}
		}
		for doerData <- doersData do doerData.doer.executeSequentially { () =>
			if !doerData.failed then doerData.promise.success(doerData.workerChangesCounter)
		}

		given ExecutionContext = ExecutionContext.global

		Future.sequence(doersData.map(_.promise.future))
			.map { threadSwapsByDoer => assert(threadSwapsByDoer.sum >= minimumThreadSwaps * NUMBER_OF_DOERS, s"${threadSwapsByDoer.sum} >= ${minimumThreadSwaps * NUMBER_OF_DOERS}") }
			.andThen { _ =>
				provider.shutdown()
				provider.awaitTermination(1, TimeUnit.SECONDS)
			}
//			.andThen { _ => for index <- doersData.indices do println(s"$index: workerIndexChangesCounter=${doersData(index).workerChangesCounter}") }
			.andThen { _ => println(s"total worker swaps: ${doersData.map(_.workerChangesCounter).sum}") }

	}

	test("CooperativeWorkersDap: Tasks should see updates made by previous tasks enqueued into the same doer") {

		testVisibility(new CooperativeWorkersDap(false), NUMBER_OF_TASK_ENQUEUED_PER_DOER/20)
	}
	test("SchedulingDap: Tasks should see updates made by previous tasks enqueued into the same doer") {

		testVisibility(new SchedulingDap(false), NUMBER_OF_TASK_ENQUEUED_PER_DOER/20)
	}
	test("LeastLoadedFixedWorkerDap: Tasks should see updates made by previous tasks enqueued into the same doer") {

		testVisibility(new LeastLoadedFixedWorkerDap, 0)
	}
	test("RoundRobinDap: Tasks should see updates made by previous tasks enqueued into the same doer") {

		testVisibility(new RoundRobinDap, 0)
	}
}
