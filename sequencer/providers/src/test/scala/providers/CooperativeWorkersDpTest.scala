package readren.sequencer
package providers

import providers.CooperativeWorkersDp.*

import munit.ScalaCheckEffectSuite
import readren.common.ScribeConfig

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.compiletime.uninitialized
import scala.concurrent.Promise

/** Test suite for [[CooperativeWorkersDp]] trait using fixed samples instead of property-based testing.
 *
 * This suite tests the core functionality of the [[CooperativeWorkersDp]] including:
 * - Basic doer provision and task execution
 * - Concurrency and thread safety
 * - Shutdown and termination behavior
 * - Exception handling and failure reporting
 * - Memory visibility and happens-before guarantees
 */
class CooperativeWorkersDpTest extends ScalaCheckEffectSuite {

	/** The [[DoerProvider]] being tested. */
	private var doerProvider: CooperativeWorkersDp.Impl = uninitialized

	private var sharedDoer: CooperativeWorkersDp.DoerFacade = uninitialized

	override def beforeAll(): Unit = {
		ScribeConfig.init(deleteLogFilesOnLaunch = true)
		doerProvider = new CooperativeWorkersDp.Impl(
			applyMemoryFence = false,
			failureReporter = (doer, failure) => scribe.debug(s"Failure reported: ${failure.getMessage}"),
			unhandledExceptionReporter = (doer, exception) => scribe.debug(s"Unhandled exception: ${exception.getMessage}")
		)
		sharedDoer = doerProvider.provide("shared-doer")
	}

	override def afterAll(): Unit = {
		doerProvider.shutdown()
		doerProvider.awaitTermination(5, TimeUnit.SECONDS)
	}

	/** Breaks the `promise` that this test will succeed. */
	protected def break[P](message: String)(using promise: Promise[P]): Unit =
		promise.tryFailure(new AssertionError(message))


	//// BASIC FUNCTIONALITY TESTS ////

	test("CooperativeWorkersDp should provide unique doer instances") {
		val doer1 = doerProvider.provide("test-doer-1")
		val doer2 = doerProvider.provide("test-doer-2")

		assert(doer1 ne doer2, "Different doer instances should be provided")
		assert(doer1.tag == "test-doer-1")
		assert(doer2.tag == "test-doer-2")
	}

	test("Doer should track pending tasks correctly") {
		val doer = sharedDoer
		val latch = new CountDownLatch(1)
		val slowLatch = new CountDownLatch(1)

		// Initially no pending tasks
		assert(doer.numOfPendingTasks == 0, "Initially should have no pending tasks")

		// Submit a slow task
		doer.executeSequentially { () =>
			slowLatch.await(2, TimeUnit.SECONDS)
			latch.countDown()
		}

		// Give it a moment to be queued
		Thread.sleep(10)

		// Should have at least one pending task
		assert(doer.numOfPendingTasks >= 1, "Should have at least one pending task")

		// Release the slow task
		slowLatch.countDown()
		assert(latch.await(5, TimeUnit.SECONDS), "Task should complete")

		// Should have no pending tasks after completion
		Thread.sleep(10)
		assert(doer.numOfPendingTasks == 0, "Should have no pending tasks after completion")
	}

	test("`Doer.execute` executes in a decoupled manner.") {
		val doer = sharedDoer

		val promise = Promise[Unit]()

		given Promise[Unit] = promise

		var mutable = 1

		val duty = doer.Duty_mine { () =>
			println("start")

			def m12(): Unit = {
				println("executing 12")
				if mutable != 1 then break(s"An execute was not decoupled 1: mutable=$mutable")
				mutable = 2
			}

			doer.run(m12())

			inline def m23(): Unit = {
				println("executing 23")
				if mutable != 2 then break(s"An execute was not decoupled 2: mutable=$mutable")
				mutable = 3
			}

			doer.run(m23())

			def m34(): Unit = {
				println("executing 34")
				if mutable != 3 then break(s"An execute was not decoupled 3: mutable=$mutable")
				mutable = 4
			}

			doer.run(m34())

			def end(): Unit = {
				println("executing end")
				if mutable != 4 then break(s"An execute was not decoupled 4: mutable=$mutable")
				promise.trySuccess(())
			}

			doer.run(end())
			if mutable != 1 then break(s"An execute was not decoupled 0: mutable=$mutable")

			println("completed")
		}

		duty.triggerAndForget(false)
		promise.future.map(identity)(using scala.concurrent.ExecutionContext.global)
	}

	test("Doer should execute tasks sequentially") {
		val doer = sharedDoer
		val results = new AtomicInteger(0)
		val executionOrder = new AtomicInteger(0)
		val latch = new CountDownLatch(3)

		// Submit three tasks that should execute in order
		doer.executeSequentially { () =>
			results.set(1)
			executionOrder.set(1)
			latch.countDown()
		}

		doer.executeSequentially { () =>
			results.set(2)
			executionOrder.set(2)
			latch.countDown()
		}

		doer.executeSequentially { () =>
			results.set(3)
			executionOrder.set(3)
			latch.countDown()
		}

		// Wait for all tasks to complete
		assert(latch.await(50, TimeUnit.MILLISECONDS), s"All tasks should complete within timeout. ${doerProvider.asInstanceOf[ShutdownAble].diagnose(new StringBuilder)}")
		assert(results.get == 3, "Last task should set result to 3")
		assert(executionOrder.get == 3, "Last task should set execution order to 3")
	}

	test("Tasks should see memory updates from previous tasks in the same doer") {
		val doer = sharedDoer
		var sharedCounter = 0
		val latch = new CountDownLatch(5)

		// Submit multiple tasks that increment the shared counter
		for i <- 0 until 5 do {
			doer.executeSequentially { () =>
				val currentValue = sharedCounter
				sharedCounter = currentValue + 1
				latch.countDown()
			}
		}

		assert(latch.await(5, TimeUnit.SECONDS), "All tasks should complete")
		assert(sharedCounter == 5, "Counter should be incremented 5 times")
	}

	//// EDGE CASE TESTS ////

	test("Provider should handle empty task submission") {
		val doer = sharedDoer

		// Submit an empty task (no-op)
		doer.executeSequentially { () =>
			// Empty task
		}

		// Give it a moment to process
		Thread.sleep(50)

		// Should have no pending tasks
		assert(doer.numOfPendingTasks == 0, "Should have no pending tasks after empty task")
	}

	//// SHUTDOWN TESTS ////

	test("Provider should shutdown gracefully") {
		val testProvider = new CooperativeWorkersDp.Impl(
			applyMemoryFence = true,
			threadPoolSize = 2
		)

		val doer = testProvider.provide("shutdown-test")
		val latch = new CountDownLatch(1)

		// Submit a task
		doer.executeSequentially { () =>
			Thread.sleep(50)
			latch.countDown()
		}

		// Wait for task to complete
		assert(latch.await(1, TimeUnit.SECONDS), "Task should complete")

		// Shutdown the provider
		testProvider.shutdown()
		val terminated = testProvider.awaitTermination(2, TimeUnit.SECONDS)

		assert(terminated, "Provider should terminate within timeout")
	}

	test("Provider should handle shutdown while tasks are running") {
		val testProvider = new CooperativeWorkersDp.Impl(
			applyMemoryFence = true,
			threadPoolSize = 2
		)

		val doer = testProvider.provide("shutdown-running-test")
		val latch = new CountDownLatch(1)
		val taskStarted = new AtomicBoolean(false)

		// Submit a long-running task
		doer.executeSequentially { () =>
			taskStarted.set(true)
			Thread.sleep(200)
			latch.countDown()
		}

		// Wait for task to start
		Thread.sleep(50)
		assert(taskStarted.get, "Task should have started")

		// Shutdown while task is running
		testProvider.shutdown()
		val terminated = testProvider.awaitTermination(3, TimeUnit.SECONDS)

		assert(terminated, "Provider should terminate even with running tasks")
		assert(latch.await(1, TimeUnit.SECONDS), "Running task should complete")
	}

	test("Busy-spin livelock reproduction") {
		var timedOut = false
		var i = 1
		val doer = doerProvider.provide("livelock-repro-shared")

		// Try up to 1000 times to guarantee hitting the race condition window
		while i <= 1000 && !timedOut do {
			// scribe.debug(s"Begin try #$i")
			val promise = Promise[Unit]()
			val latch = new CountDownLatch(3)

			// 1. Simulate the end of the previous test EXACTLY as MUnit handles it.
			// The Duty executes asynchronously, avoiding the artificial Thread.sleep
			// which gave the main thread too much of a head start.
			val duty = doer.Duty_mine { () =>
				doer.run {
					scribe.debug(s"#$i: test A first execute")
				}
				doer.run {
					scribe.debug(s"#$i: test A second execute")
					promise.trySuccess(())
				}
			}
			duty.triggerAndForget(false)

			// 2. Main thread waits for the test boundary promise to resolve
			scala.concurrent.Await.result(promise.future, scala.concurrent.duration.Duration.Inf)

			scribe.debug(s"#$i: after await")
			// 3. Widen the race window with a variable micro-delay fuzz
			// This perfectly simulates MUnit's invisible framework overhead between tests.
			var busy = 0
			val target = scala.util.Random.nextInt(500)
			while busy < target do busy += 1

			// 4. Main thread immediately enqueues new tasks, racing with the worker's queue exit
			doer.run {
				// scribe.debug(s"#$i: test B first execute")
				latch.countDown()
			}
			doer.run {
				// scribe.debug(s"#$i: test B second execute")
				latch.countDown()
			}
			doer.run {
				// scribe.debug(s"#$i: test C third execute")
				latch.countDown()
			}

			// If the race causes the infinite busy-spin livelock, the 50ms will expire.
			if !latch.await(50, TimeUnit.MILLISECONDS) then {
				timedOut = true
			}
			i += 1
		}

		assert(!timedOut, "The busy-spin livelock caused a timeout")
	}

}
