package readren.sequencer
package providers

import providers.CooperativeWorkersDp.*

import munit.ScalaCheckEffectSuite
import org.scalacheck.Gen
import org.scalacheck.effect.PropF
import readren.common.ScribeConfig

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.compiletime.uninitialized
import scala.concurrent.{ExecutionContext, Promise}

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
	private var sharedDoerProvider: CooperativeWorkersDp.Impl = uninitialized

	private var sharedDoer: CooperativeWorkersDp.DoerFacade = uninitialized

	override def beforeAll(): Unit = {
		ScribeConfig.init(deleteLogFilesOnLaunch = true)
		sharedDoerProvider = new CooperativeWorkersDp.Impl(
			applyMemoryFence = false,
			failureReporter = (doer, failure) => scribe.debug(s"Failure reported: ${failure.getMessage}"),
			unhandledExceptionReporter = (doer, exception) => scribe.debug(s"Unhandled exception: ${exception.getMessage}")
		)
		sharedDoer = sharedDoerProvider.provide("shared-doer")
	}

	override def afterAll(): Unit = {
		sharedDoerProvider.shutdown()
		sharedDoerProvider.awaitTermination(5, TimeUnit.SECONDS)
	}

	/** Breaks the `promise` that this test will succeed. */
	protected def break[P](message: String)(using promise: Promise[P]): Unit =
		promise.tryFailure(new AssertionError(message))


	//// BASIC FUNCTIONALITY TESTS ////

	test("CooperativeWorkersDp should provide unique doer instances") {
		val doer1 = sharedDoerProvider.provide("test-doer-1")
		val doer2 = sharedDoerProvider.provide("test-doer-2")

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
		assert(latch.await(50, TimeUnit.MILLISECONDS), s"All tasks should complete within timeout. ${sharedDoerProvider.asInstanceOf[ShutdownAble].diagnose(new StringBuilder)}")
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

	test("Worker threads should be reused efficiently") {
		val numberOfTasksPerDoer = 999
		val numberOfDoers = 9
		val latch = new CountDownLatch(numberOfTasksPerDoer * numberOfDoers)
		val threadIds = new java.util.concurrent.ConcurrentHashMap[Long, Int]()

		// Submit multiple tasks in different doers and collect thread IDs
		val doers = Array.tabulate[Doer](numberOfDoers)(i => sharedDoerProvider.provide(s"$i"))
		for taskNumber <- 0 until numberOfTasksPerDoer do {
			for doer <- doers do {
				doer.executeSequentially { () =>
					threadIds.compute(Thread.currentThread().threadId, (threadId, rep) => if rep eq null then 1 else rep + 1)
					latch.countDown()
				}
			}
		}

		assert(latch.await(1, TimeUnit.SECONDS), "All tasks should complete")

		// Should have used multiple threads (concurrent execution)
		val uniqueThreads = threadIds.size
		assert(uniqueThreads > 1, s"Should use multiple threads, used: $uniqueThreads")
		println(threadIds)
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
		given ExecutionContext = ExecutionContext.global

		PropF.forAllNoShrinkF (Gen.choose(1, 9), Gen.choose(1, 20), Gen.choose(1, 40)) { (poolSize: Int, numberOfDoers: Int, numberOfTasks: Int) =>
			// println(s"Begin: poolSize=$poolSize, numberOfDoers=$numberOfDoers, numberOfTasks=$numberOfTasks")
			val promise = Promise[Unit]()
			val provider = new CooperativeWorkersDp.Impl(applyMemoryFence = false, threadPoolSize = poolSize)

			val doers = IArray.tabulate(numberOfDoers)(i => provider.provide(s"shutdown-test-$i"))
			val numberOfCompletedTask = AtomicInteger(0)

			// Submit a tasks
			for i <- 0 until numberOfTasks do {
				val doer = doers(i % numberOfDoers)
				doer.executeSequentially { () =>
					Thread.sleep(1)
					numberOfCompletedTask.getAndIncrement()
				}
			}

			// Shutdown the provider
			provider.shutdown()
			val termination = provider.awaitTermination(2, TimeUnit.SECONDS)
			def diagnostic: String = s"\nDiagnostic:\nTest sample: poolSize=$poolSize, numberOfDoers=$numberOfDoers, numberOfTasks=$numberOfTasks\nProvider state:\n${provider.diagnose(new StringBuilder)}"
			if termination then {
				if numberOfCompletedTask.get < numberOfTasks then promise.tryFailure(new AssertionError(s"All tasks should be completed and only $numberOfCompletedTask/$numberOfTasks are.$diagnostic"))
			} else promise.tryFailure(new AssertionError(s"Provider should shutdown within timeout.$diagnostic"))

			promise.trySuccess(())
			promise.future
		}
	}

	test("Provider should handle shutdown while tasks are running") {
		val testProvider = new CooperativeWorkersDp.Impl(
			applyMemoryFence = false,
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

		assert(terminated, s"Provider should terminate even with running tasks: ${testProvider.diagnose(new StringBuilder)}")
		assert(latch.await(1, TimeUnit.SECONDS), "Running task should complete")
	}

	test("No pending tasks when workers go to sleep (race condition test)") {
		for threadPoolSize <- 1 to 4 do {
			val testProvider = new CooperativeWorkersDp.Impl(
				applyMemoryFence = false,
				threadPoolSize = threadPoolSize
			)
			val doers = Array.tabulate(threadPoolSize)(i => testProvider.provide(s"race-doer-$i"))

			val iterations = 100000
			for i <- 1 to iterations do {
				val latch = new CountDownLatch(threadPoolSize)

				// Enqueue one task per doer to occupy all workers.
				for j <- 0 until threadPoolSize do {
					doers(j).executeSequentially { () =>
						latch.countDown()
					}
				}

				// Wait for the tasks to finish.
				// As soon as this unblocks successfuly, all workers are finishing their tasks and will poll an empty queue, transitioning to sleep. The next iteration will immediately enqueue new tasks, maximizing the probability of hitting the tryToSleep window.
				val success = latch.await(1, TimeUnit.SECONDS)
				if !success then {
					val diagnostic = testProvider.diagnose(new StringBuilder)
					assert(false, s"Tasks at iteration $i where not executed. Thread pool size: $threadPoolSize. Provider state:\n$diagnostic")
				}
			}

			val diagnostic = testProvider.diagnose(new StringBuilder)
			println(s"Provider state:\n$diagnostic")

			testProvider.shutdown()
			assert(testProvider.awaitTermination(1, TimeUnit.SECONDS), "Provider did not terminate")
		}
	}

}
