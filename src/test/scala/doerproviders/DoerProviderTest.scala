package readren.matrix
package doerproviders

import munit.ScalaCheckEffectSuite

import java.util.concurrent.Executors
import scala.collection.immutable.ArraySeq
import scala.concurrent.{ExecutionContext, Future, Promise}


class DoerProviderTest extends ScalaCheckEffectSuite {

	private val matrix = new Matrix[SharedQueueDoerProvider]("underTest", new Matrix.Aide[SharedQueueDoerProvider] {
		override def buildLogger(owner: Matrix[SharedQueueDoerProvider]): Logger = new SimpleLogger(Logger.Level.info)

		override def buildDoerProvider(owner: Matrix[SharedQueueDoerProvider]): SharedQueueDoerProvider = new SharedQueueDoerProvider(owner, Executors.defaultThreadFactory(), Runtime.getRuntime.availableProcessors(), _.printStackTrace())
	})
	private val provider = matrix.doerProvider

	test("Tasks should see updates made by previous tasks across multiple Assistants") {

		class DoerData {
			val doer: provider.Doer = provider.provide()
			val promise: Promise[Unit] = Promise()
			var counter: Int = 0
			var previousTaskWorkerIndex: Int = -1
			var workerIndexChangesCounter: Int = 0
		}

		val doersData = ArraySeq.fill(320)(new DoerData)

		for expected <- 0 to 10000 do {

			for doerIndex <- doersData.indices yield {
				val doerData = doersData(doerIndex)
				val doer = doerData.doer
				doer.queueForSequentialExecution {
					val currentWorkerIndex = SharedQueueDoerProvider.workerIndexThreadLocal.get()
					if doerData.previousTaskWorkerIndex != currentWorkerIndex then {
						doerData.workerIndexChangesCounter += 1
						doerData.previousTaskWorkerIndex = currentWorkerIndex
					}
					if doerData.counter != expected then doerData.promise.failure(new AssertionError(s"expected: $expected, found: $doerData.counter"))
					doerData.counter = doerData.counter + 1
				}
			}
		}
		for doerData <- doersData do doerData.doer.queueForSequentialExecution(doerData.promise.success(()))

		given ExecutionContext = ExecutionContext.global
		Future.sequence(doersData.map(_.promise.future)).andThen { _ =>
			for doerData <- doersData do println(s"workerIndexChangesCounter=${doerData.workerIndexChangesCounter}")
		}
	}
}
