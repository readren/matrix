package readren.sequencer

import CausalFence.{ROLLBACK_APPLIED, ROLLBACK_IGNORED, RollbackApplication}
import GeneratorsForDoerTests.{*, given}

import org.scalacheck.effect.PropF
import org.scalacheck.{Arbitrary, Gen}
import readren.common.Maybe

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/** Trait containing tests for vanilla [[Doer]] primitives, concurrency, exception handling, Task, Captor, and CausalFence.
 */
trait VanillaDoerTests[D <: Doer : ClassTag] { self: DoerProviderTestBase[D] =>

	////////// DOER INFRASTRUCTURE ////////

	test("`Doer.execute` executes in a decoupled manner.") {
		val generators = getGenerators
		import generators.*

		val promise = Promise[Unit]()

		given Promise[Unit] = promise

		var mutable = 1

		val task = doer.Task_apply { () =>

			def m12(): Unit = {
				if mutable != 1 then break(s"An execute was not decoupled 1: mutable=$mutable")
				mutable = 2
			}

			doer.run(m12())

			inline def m23(): Unit = {
				if mutable != 2 then break(s"An execute was not decoupled 2: mutable=$mutable")
				mutable = 3
			}

			doer.run(m23())

			def m34(): Unit = {
				if mutable != 3 then break(s"An execute was not decoupled 3: mutable=$mutable")
				mutable = 4
			}

			doer.run(m34())

			def end(): Unit = {
				if mutable != 4 then break(s"An execute was not decoupled 4: mutable=$mutable")
				promise.trySuccess(())
			}

			doer.run(end())
			if mutable != 1 then break(s"An execute was not decoupled 0: mutable=$mutable")
		}

		task.triggerAndForget(false)
		gate
	}

	test("Doer should execute runnables sequentially") {
		val doer = getSharedDoer
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
		assert(latch.await(50, TimeUnit.MILLISECONDS), "All runnables should complete within timeout")
		assert(results.get == 3, "Last venture should set result to 3")
		assert(executionOrder.get == 3, "Last venture should set execution order to 3")
	}

	//// CONCURRENCY TESTS ////

	test("Multiple doers should execute runnables concurrently") {
		assert(Runtime.getRuntime.availableProcessors() >= 3)

		val doer1 = buildDoer("doer-1")
		val doer2 = buildDoer("doer-2")
		val doer3 = buildDoer("doer-3")

		val startLatch = new CountDownLatch(3)
		val endLatch = new CountDownLatch(3)

		// Submit tasks to different doers simultaneously
		doer1.executeSequentially { () =>
			startLatch.countDown()
			println(s"Doer1 start:${System.currentTimeMillis()}")
			Thread.sleep(100)
			endLatch.countDown()
			println(s"Doer1 end:${System.currentTimeMillis()}")
		}

		doer2.executeSequentially { () =>
			startLatch.countDown()
			println(s"Doer2 start:${System.currentTimeMillis()}")
			Thread.sleep(100)
			endLatch.countDown()
			println(s"Doer3 end:${System.currentTimeMillis()}")
		}

		doer3.executeSequentially { () =>
			startLatch.countDown()
			println(s"Doer3 start:${System.currentTimeMillis()}")
			Thread.sleep(100)
			endLatch.countDown()
			println(s"Doer3 end:${System.currentTimeMillis()}")
		}

		// If tasks were truly concurrent, they should start without waiting any other to finish.
		assert(startLatch.await(90, TimeUnit.MILLISECONDS), "All runnables should start soon.")

		// If tasks were truly concurrent, total time should be close to 100ms, not 300ms
		assert(endLatch.await(250, TimeUnit.MILLISECONDS), "All runnables should complete")
	}

	test("Runnables should see memory updates from previous runnable in the same doer") {
		val doer = getSharedDoer
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

		assert(latch.await(5, TimeUnit.SECONDS), "All runnables should complete")
		assert(sharedCounter == 5, "Counter should be incremented 5 times")
	}

	test("Worker threads should be reused efficiently") {
		val numberOfVenturesPerDoer = 999
		val numberOfDoers = 9
		val latch = new CountDownLatch(numberOfVenturesPerDoer * numberOfDoers)
		val threadIds = new java.util.concurrent.ConcurrentLinkedQueue[Long]()

		// Submit multiple tasks in different doers and collect thread IDs
		val doers = Array.tabulate[Doer](numberOfDoers)(i => buildDoer(s"$i"))
		for ventureNumber <- 0 until numberOfVenturesPerDoer do {
			for doer <- doers do {
				doer.executeSequentially { () =>
					threadIds.add(Thread.currentThread().threadId)
					latch.countDown()
				}
			}
		}

		assert(latch.await(1, TimeUnit.SECONDS), "All runnables should complete")

		// Should have used multiple threads (concurrent execution)
		val uniqueThreads = threadIds.toArray.toSet.size
		assert(uniqueThreads > 1, s"Should use multiple threads, used: $uniqueThreads")
	}

	//// EXCEPTION HANDLING TESTS ////

	test("Doer should handle exceptions in runnables gracefully") {
		val doer = getSharedDoer
		val latch = new CountDownLatch(2)
		val exceptionCaught = new AtomicBoolean(false)

		// Submit a Runnable that throws an exception
		doer.executeSequentially { () =>
			throw new RuntimeException("Test exception")
		}

		// Submit a Runnable that should still execute after the exception
		doer.executeSequentially { () =>
			exceptionCaught.set(true)
			latch.countDown()
		}

		// Submit another normal venture
		doer.executeSequentially { () =>
			latch.countDown()
		}

		assert(latch.await(5, TimeUnit.SECONDS), "`Runnable` after exception should still execute")
		assert(exceptionCaught.get, "`Runnable` after exception should have executed")
	}

	test("The DoerProvider.onUnhandledException handler should be called immediately when the Runnable passed to executeSequentially throws an exception.") {
		val mainDoer = getSharedDoer

		PropF.forAllNoShrinkF { (exception: Throwable) =>
			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			observingUnhandledExceptionsDo { () =>
				mainDoer.executeSequentially { () => throw exception }
				mainDoer.executeSequentially { () => if !promise.isCompleted then break(s"The next Runnable was executed before the onUnhandledException: $exception") }
				breakAfterWaiting(999, s"No notification of the exception $exception until 999 milliseconds after applying the operation. Waiting aborted.")

			} { (doer, e) =>
				if e ne exception then break(s"An unexpected exception was caught: $e != $exception")
				else if doer ne mainDoer then break(s"Correct doer should be captured: ${doer.tag} != ${mainDoer.tag}")
				else promise.trySuccess(())
			}
		}
	}

	test("The `DoerProvider` should notify uncaught exceptions thrown by the Runnable passed to `Doer.executeSequentially` before executing the next enqueued Runnable") {
		val mainDoer = getSharedDoer

		PropF.forAllNoShrinkF { (exception: Throwable) =>

			val promise = Promise[Unit]()
			val wasCaught = new java.util.concurrent.atomic.AtomicBoolean(false)

			given Promise[Unit] = promise

			observingUnhandledExceptionsDo { () =>
				mainDoer.executeSequentially(() => throw exception)
				mainDoer.executeSequentially { () =>
					if wasCaught.get() then promise.trySuccess(()) else break("The uncaught exception was not notified")
				}

				breakAfterWaiting(999, s"No notification of the exception $exception until 990 milliseconds after applying the operation. Waiting aborted.")

			} { (d, t) =>
				if t eq exception then wasCaught.set(true) else break(s"an unexpected exception was uncaught $t")
			}
		}
	}

	//// STRESS TESTS ////

	test("Provider should handle high load") {
		val doer = getSharedDoer
		val runnablesCount = 100
		val latch = new CountDownLatch(runnablesCount)
		val results = new AtomicInteger(0)

		// Submit many tasks
		for _ <- 1 to runnablesCount do {
			doer.executeSequentially { () =>
				results.incrementAndGet()
				latch.countDown()
			}
		}

		assert(latch.await(10, TimeUnit.SECONDS), "All runnables should complete")
		assert(results.get == runnablesCount, s"All $runnablesCount runnables should have executed")
	}

	test("Provider should handle multiple doers with high load") {
		val doerCount = 10
		val runnablesPerDoer = 20
		val latch = new CountDownLatch(doerCount * runnablesPerDoer)
		val results = new AtomicInteger(0)

		// Create multiple doers and submit tasks to each
		for doerIndex <- 1 to doerCount do {
			val doer = buildDoer(s"stress-doer-$doerIndex")
			for _ <- 1 to runnablesPerDoer do {
				doer.executeSequentially { () =>
					results.incrementAndGet()
					latch.countDown()
				}
			}
		}

		assert(latch.await(15, TimeUnit.SECONDS), "All runnables should complete")
		assert(results.get == doerCount * runnablesPerDoer, s"All ${doerCount * runnablesPerDoer} runnables should have executed")
	}

	//// EDGE CASE TESTS ////

	test("Provider should handle rapid venture submission") {
		val doer = getSharedDoer
		val latch = new CountDownLatch(50)
		val results = new AtomicInteger(0)

		// Submit tasks rapidly without waiting
		for _ <- 1 to 50 do {
			doer.executeSequentially { () =>
				results.incrementAndGet()
				latch.countDown()
			}
		}

		assert(latch.await(5, TimeUnit.SECONDS), "All rapid runnables should complete")
		assert(results.get == 50, "All 50 rapid runnables should have executed")
	}

	test("Provider should maintain venture ordering under concurrent submission") {
		val doer = getSharedDoer
		val runnablesCount = 20
		val latch = new CountDownLatch(runnablesCount)
		val executionOrder = new java.util.concurrent.ConcurrentLinkedQueue[Int]()

		// Submit tasks from multiple threads
		val futures = for i <- 1 to runnablesCount yield {
			Future {
				doer.executeSequentially { () =>
					executionOrder.add(i)
					latch.countDown()
				}
			}
		}

		// Wait for all tasks to complete
		Future.sequence(futures)
		assert(latch.await(5, TimeUnit.SECONDS), "All runnables should complete")

		// Verify that tasks were executed in some order
		val orderList = executionOrder.toArray.toList
		assert(orderList.size == runnablesCount, s"All $runnablesCount runnables should have been executed")
		assert(orderList.toSet.size == runnablesCount, "All venture IDs should be unique")
	}
}
