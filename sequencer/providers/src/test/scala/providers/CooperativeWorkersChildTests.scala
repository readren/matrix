package readren.sequencer
package providers

import providers.CooperativeWorkersDp.*

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.concurrent.Promise
import scala.reflect.ClassTag

/** Trait containing tests for [[DoerProvider]] implementations that extend [[CooperativeWorkersDp]].
 */
trait CooperativeWorkersChildTests[D <: DoerFacade : ClassTag] { self: DoerProviderTestBase[D] =>

	override type DP <: CooperativeWorkersDp

	test("CooperativeWorkersDp should provide unique doer instances") {
		val p = getSharedDoerProvider
		val doer1 = p.provide(p.tagFromText("test-doer-1"))
		val doer2 = p.provide(p.tagFromText("test-doer-2"))

		assert(doer1.ne(doer2), "Different doer instances should be provided")
		assert(doer1.tag == p.tagFromText("test-doer-1"), "doer1 tag match")
		assert(doer2.tag == p.tagFromText("test-doer-2"), "doer2 tag match")
	}

	test("Doer should track pending runnables correctly") {
		val doer = getSharedDoer
		val latch = new CountDownLatch(1)
		val slowLatch = new CountDownLatch(1)

		// Initially no pending tasks
		assert(doer.numOfPendingRunnables == 0, "Initially should have no pending runnables")

		// Submit a slow runnable
		doer.executeSequentially { () =>
			slowLatch.await(2, TimeUnit.SECONDS)
			latch.countDown()
		}

		// Give it a moment to be queued
		Thread.sleep(10)

		// Should have at least one pending runnable
		assert(doer.numOfPendingRunnables >= 1, "Should have at least one pending runnable")

		// Release the slow runnable
		slowLatch.countDown()
		assert(latch.await(5, TimeUnit.SECONDS), "Runnable should complete")

		// Should have no pending tasks after completion
		Thread.sleep(10)
		assert(doer.numOfPendingRunnables == 0, "Should have no pending runnable after completion")
	}

	test("Provider should handle empty runnable submission") {
		val doer = getSharedDoer

		// Submit an empty runnable (no-op)
		doer.executeSequentially { () =>
			// Empty runnable
		}

		// Give it a moment to process
		Thread.sleep(50)

		// Should have no pending tasks
		assert(doer.numOfPendingRunnables == 0, "Should have no pending runnables after empty runnable")
	}

	//// SHUTDOWN & CONCURRENCY TESTS ////

	test("Provider should shutdown gracefully") {
		given scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

		org.scalacheck.effect.PropF.forAllNoShrinkF(
			org.scalacheck.Gen.choose(1, 9),
			org.scalacheck.Gen.choose(1, 20),
			org.scalacheck.Gen.choose(1, 40)
		) { (poolSize: Int, numberOfDoers: Int, numberOfRunnables: Int) =>
			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			val provider = buildDoerProvider(poolSize)

			val doers = IArray.tabulate(numberOfDoers)(i => provider.provide(provider.tagFromText(s"shutdown-test-$i")))
			val numberOfCompletedRunnables = java.util.concurrent.atomic.AtomicInteger(0)

			for i <- 0 until numberOfRunnables do {
				val doer = doers(i % numberOfDoers)
				doer.executeSequentially { () =>
					Thread.sleep(1)
					numberOfCompletedRunnables.getAndIncrement()
				}
			}

			releaseDoerProvider(provider)
			val termination = provider.awaitTermination(2, TimeUnit.SECONDS)

			def diagnostic: String = s"\nDiagnostic:\nTest sample: poolSize=$poolSize, numberOfDoers=$numberOfDoers, numberOfRunnables=$numberOfRunnables\nProvider state:\n${provider.diagnose(new StringBuilder)}"

			if termination then {
				if numberOfCompletedRunnables.get == numberOfRunnables then promise.trySuccess(())
				else break(s"All runnables should be completed and only ${numberOfCompletedRunnables.get}/$numberOfRunnables are. Diagnostic:\n$diagnostic")
			} else break(s"Provider should shutdown within timeout. Diagnostic:\n$diagnostic")

			promise.future
		}
	}

	test("Provider should handle shutdown while runnables are running") {
		val testProvider = buildDoerProvider()
		val doer = testProvider.provide(testProvider.tagFromText("shutdown-running-test"))
		val latch = new CountDownLatch(1)
		val runnablesStarted = new AtomicBoolean(false)

		// Submit a long-running runnable
		doer.executeSequentially { () =>
			runnablesStarted.set(true)
			Thread.sleep(200)
			latch.countDown()
		}

		// Wait for runnable to start
		Thread.sleep(50)
		assert(runnablesStarted.get, "Runnable should have started")

		// Shutdown while a runnable is running
		releaseDoerProvider(testProvider)
		assert(latch.await(1, TimeUnit.SECONDS), "Running runnables should complete")
	}

	test("No pending runnables when workers go to sleep (race condition test)") {
		for threadPoolSize <- 1 to 4 do {
			val testProvider = new CooperativeWorkersDp.Impl(
				applyMemoryFence = false,
				threadPoolSize = threadPoolSize
			)
			val doers = Array.tabulate(threadPoolSize)(i => testProvider.provide(s"race-doer-$i"))

			val iterations = 100000
			for i <- 1 to iterations do {
				val latch = new CountDownLatch(threadPoolSize)

				// Enqueue one runnable per doer to occupy all workers.
				for j <- 0 until threadPoolSize do {
					doers(j).executeSequentially { () =>
						latch.countDown()
					}
				}

				// Wait for the tasks to finish.
				// As soon as this unblocks successfully, all workers are finishing their tasks and will poll an empty queue, transitioning to sleep. The next iteration will immediately enqueue new tasks, maximizing the probability of hitting the tryToSleep window.
				val success = latch.await(1, TimeUnit.SECONDS)
				if !success then {
					val diagnostic = testProvider.diagnose(new StringBuilder)
					assert(false, s"Runnable at iteration $i where not executed. Thread pool size: $threadPoolSize. Provider state:\n$diagnostic")
				}
			}

			val diagnostic = testProvider.diagnose(new StringBuilder)
			println(s"Provider state:\n$diagnostic")

			testProvider.shutdown()
			assert(testProvider.awaitTermination(1, TimeUnit.SECONDS), "Provider did not terminate")
		}
	}

	test("Worker sleep/wakeup race condition under concurrent task submission") {
		val p = getSharedDoerProvider
		val doer = p.provide(p.tagFromText("race-test-doer"))
		val iterations = 1000
		val latch = new CountDownLatch(iterations)
		val rand = new scala.util.Random()

		val threads = for (_ <- 0 until 4) yield new Thread {
			override def run(): Unit = {
				for (_ <- 0 until iterations / 4) {
					Thread.sleep(rand.nextInt(3)) // random delay up to 2ms
					doer.executeSequentially { () =>
						latch.countDown()
					}
				}
			}
		}

		threads.foreach(_.start())
		val completed = latch.await(10, TimeUnit.SECONDS)
		threads.foreach(_.join())
		assert(completed, "Worker threads hung or a deadlock occurred during concurrent task submission!")
	}
}
