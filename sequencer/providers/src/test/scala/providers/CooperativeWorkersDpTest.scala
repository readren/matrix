package readren.sequencer
package providers

import providers.CooperativeWorkersDp.*

import munit.ScalaCheckEffectSuite
import org.scalacheck.effect.PropF
import readren.common.ScribeConfig

import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.annotation.tailrec
import scala.concurrent.{Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success, Try}
import scala.compiletime.uninitialized

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

	override def beforeAll(): Unit = {
		ScribeConfig.init(deleteLogFilesOnLaunch = true)
		doerProvider = new CooperativeWorkersDp.Impl(
			applyMemoryFence = false,
			failureReporter = (doer, failure) => scribe.debug(s"Failure reported: ${failure.getMessage}"),
			unhandledExceptionReporter = (doer, exception) => scribe.debug(s"Unhandled exception: ${exception.getMessage}")
		)
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
		val doer = doerProvider.provide("pending-tasks-test")
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

	//// EDGE CASE TESTS ////

	test("Provider should handle empty task submission") {
		val doer = doerProvider.provide("pending-tasks-test")

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


}
