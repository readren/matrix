package readren.sequencer

import CausalFence.{ROLLBACK_APPLIED, ROLLBACK_IGNORED, RollbackApplication}
import GeneratorsForDoerTests.{*, given}
import SchedulingExtension.{DELAY, FIXED_DELAY, FIXED_RATE}

import munit.ScalaCheckEffectSuite
import org.scalacheck.effect.PropF
import org.scalacheck.{Arbitrary, Gen, Prop}
import readren.common.{Maybe, ScribeConfig, Trial}
import readren.sequencer.CausalFence

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.compiletime.uninitialized
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}


/** Abstract test suite for testing [[DoerProvider]] implementations that provide [[Doer]] instances extended with [[SchedulingExtension]] and [[LoopingExtension]].
 *
 * This suite checks if the instances provided by a [[DoerProvider]] implementation respect the contract of [[Doer]] with [[SchedulingExtension]] and [[LoopingExtension]], without being tied to a specific implementation.
 * The idea is that the test suites of [[DoerProvider]] extend this abstract class to verify that the [[Doer]] & [[SchedulingExtension]] & [[LoopingExtension]] instances that the [[DoerProvider]] provides satisfy all the invariants checked here by this testing class.
 *
 * @tparam D The type of Doer being tested, must extend both [[Doer]] with [[SchedulingExtension]] and [[LoopingExtension]].
 */
abstract class SchedulingDoerProviderTest[D <: Doer & SchedulingExtension & LoopingExtension : ClassTag] extends ScalaCheckEffectSuite {

	type DP <: DoerProvider[D]

	@volatile private var unhandledExceptionObserver: Null | ((Doer, Throwable) => Unit) = null

	private var sharedDoerProvider: DP = uninitialized
	private var sharedDoer: D = uninitialized
	private var sharedGenerators: GeneratorsForDoerTests[D] = uninitialized

	@volatile private var observingSession: Int = 0

	/** The implementation should build an instance of the [[DoerProvider]] implementation under test. */
	protected def buildDoerProvider: DP

	/** The implementation should release the specified [[DoerProvider]].
	 * The implementation may assume that the provided instance was obtained calling [[buildDoerProvider]]. */
	protected def releaseDoerProvider(doerProvider: DP): Unit

	/**
	 * This method should be invoked by the [[DoerProvider]] instances returned by [[buildDoerProvider]] whenever their [[DoerProvider.onUnhandledException]] callback is triggered.
	 * The extending class is responsible for ensuring this linkage.
	 */
	protected def onUnhandledException(doer: Doer, exception: Throwable): Unit = {
		if doer.isInSequence then {
			if unhandledExceptionObserver ne null then unhandledExceptionObserver(doer, exception)
			// scribe.debug(s"#$observingSession: unhandled exception logged: $exception")

			// scribe.error(s"Unhandled exception:", exception)
		} else {
			val trace = new RuntimeException(exception)
			scribe.error(s"TEST FAILED - DO NOT IGNORE: `onUnhandledException` was called outside the provided doer's thread.", trace)
		}
	}

	//// Suite lifecycle ////

	// override def scalaCheckInitialSeed = "VGtbAPL-x8B3LNaFTqrChP5DoBPGiOpWnmcpQoAYzhN="

	override val munitTimeout: Duration = scala.concurrent.duration.Duration(15, "seconds")

	/** Creates the shared instances that depend on the abstract methods of this class in a deferred way to ensure the concrete subclass is fully constructed before said methods are invoked. */
	override def beforeAll(): Unit = {
		ScribeConfig.init(deleteLogFilesOnLaunch = true)

		val sharedDoerProvider = buildDoerProvider
		this.sharedDoerProvider = sharedDoerProvider
		val sharedDoer = sharedDoerProvider.provide(sharedDoerProvider.tagFromText("main-doer"))
		this.sharedDoer = sharedDoer
		val sharedGenerators = GeneratorsForDoerTests(sharedDoer, sharedDoerProvider)
		this.sharedGenerators = sharedGenerators
	}

	/** Clean up resources after tests. */
	override def afterAll(): Unit = {
		println("Shutting down...")
		releaseDoerProvider(getSharedDoerProvider)
	}

	override def beforeEach(context: BeforeEach): Unit = {
		println(s"[START] ${context.test.name}")
		super.beforeEach(context)
	}

	override def afterEach(context: AfterEach): Unit = {
		println(s"[DONE]  ${context.test.name}")
		super.afterEach(context)
	}

	//// Shared instance's getters ////

	/** Gets the shared instance of the [[DoerProvider]] implementation under test. */
	protected def getSharedDoerProvider: DP = sharedDoerProvider

	/** Builds an instance of [[Doer]] using the shared [[DoerProvider]]. */
	protected def buildDoer(tag: String): D = {
		val provider = sharedDoerProvider
		provider.provide(provider.tagFromText(tag))
	}

	/** Gets the shared instance of [[Doer]] provided by the shared doer provider. */
	protected def getSharedDoer: D = sharedDoer

	/** Get the shared instance of [[GeneratorsForDoerTests]] built using the [[DoerProvider]] and [[Doer]] instances returned by [[getSharedDoerProvider]] and [[getSharedDoer]] respectively. */
	protected def getGenerators: GeneratorsForDoerTests[D] = sharedGenerators

	//// UTILITIES ////

	/** Thrown by [[break]] */
	class BreakException(cause: Throwable) extends RuntimeException(cause)

	/** Breaks the `promise` if it wasn't already completed. */
	protected def break[P](message: String)(using promise: Promise[P]): Nothing = {
		val error = new AssertionError(message)
		promise.tryFailure(error)
		throw new BreakException(error)
	}

	protected def gate[P](using promise: Promise[P]): Future[P] = {
		promise.future.map { result =>
			// println(s"gating to next promise in thread ${Thread.currentThread().getName}")
			result
		}(using scala.concurrent.ExecutionContext.Implicits.global)
	}

	/** Waits the promise to complete or the specified duration, what happens first. In the second case the promise is broken with the specified message.
	 * @return the [[Future]] view of the provided [[Promise]]. */
	protected def breakAfterWaiting[P](duration: Int, message: String)(using promise: Promise[P]): Future[P] = {
		val latch = CountDownLatch(1)
		promise.future.andThen(_ => latch.countDown())(using scala.concurrent.ExecutionContext.Implicits.global)
		if !latch.await(duration, TimeUnit.MILLISECONDS) then break(message)
		gate
	}

	/** Executes the provided `supplier` observing the calls to the [[onUnhandledException]] method until its completion. */
	protected def observingUnhandledExceptionsDo[R, P](supplier: () => Future[R])(onUnhandledException: (Doer, Throwable) => Unit)(using promise: Promise[P]): Future[R] = {
		if unhandledExceptionObserver ne null then break("Nesting `observingAsyncUnhandledExceptionsDo` is not supported")
		observingSession += 1
		unhandledExceptionObserver = onUnhandledException
		try {
			supplier().andThen { tryR =>
				unhandledExceptionObserver = null
			}
		} catch {
			case e: Throwable =>
				unhandledExceptionObserver = null
				Future.failed(e)
		}
	}

	//// UNDER DEVELOPMENT

	test("joker") {
		// Test with fixed-delay schedule
		// Verify supplier is called multiple times with delay between completions
		// Verify each result is yielded
		val generators = getGenerators

		true
	}

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

	test("Doer should execute ventures sequentially") {
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
		val doer1 = buildDoer("doer-1")
		val doer2 = buildDoer("doer-2")
		val doer3 = buildDoer("doer-3")

		val latch = new CountDownLatch(3)
		val startTime = System.currentTimeMillis()
		val executionTimes = new AtomicInteger(0)

		// Submit tasks to different doers simultaneously
		doer1.executeSequentially { () =>
			Thread.sleep(100)
			executionTimes.incrementAndGet()
			latch.countDown()
		}

		doer2.executeSequentially { () =>
			Thread.sleep(100)
			executionTimes.incrementAndGet()
			latch.countDown()
		}

		doer3.executeSequentially { () =>
			Thread.sleep(100)
			executionTimes.incrementAndGet()
			latch.countDown()
		}

		assert(latch.await(400, TimeUnit.MILLISECONDS), "All runnables should complete")
		val endTime = System.currentTimeMillis()
		val totalTime = endTime - startTime

		// If tasks were truly concurrent, total time should be close to 100ms, not 300ms
		assert(totalTime < 250, s"Ventures should execute concurrently, total time: ${totalTime}ms")
		assert(executionTimes.get == 3, "All runnables should have executed")
	}

	test("Ventures should see memory updates from previous runnable in the same doer") {
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

		assert(latch.await(5, TimeUnit.SECONDS), "Venture after exception should still execute")
		assert(exceptionCaught.get, "Venture after exception should have executed")
	}

	test("The DoerProvider.onUnhandledException handler should be called immediately when the Runnable passed to executeSequentially throws an exception.") {
		val mainDoer = getSharedDoer

		PropF.forAllNoShrinkF { (exception: Throwable) =>
			// println(s"Begin: exception = $exception")
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
			val wasCaught = new java.util.concurrent.atomic.AtomicBoolean(false) // REVISION#1: Why have you changed to use AtomicBoolean? Isn't this variable always accessed from the same thread?

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

		// Verify that tasks were executed in some order (not necessarily submission order due to concurrency)
		val orderList = executionOrder.toArray.toList
		assert(orderList.size == runnablesCount, s"All $runnablesCount runnables should have been executed")
		assert(orderList.toSet.size == runnablesCount, "All venture IDs should be unique")
	}


	////////// TASK //////////

	// Custom equality for Task based on the result
	private def checkEquality[A](doer: Doer)(task1: doer.Task[A], task2: doer.Task[A], clue: => Any = "duties yield different results")(using CanEqual[A, A]): Future[Unit] = {
		val promise = Promise[Unit]()

		given Promise[Unit] = promise

		task1.subscribe(false)(new doer.MonoObserver[A] {
			override def onSuccess(a1: A): Unit = {
				task2.subscribeSync(new doer.MonoObserver[A] {
					override def onSuccess(a2: A): Unit = if a1 == a2 then promise.trySuccess(()) else break(s"$a1 is not equal to $a2")

					override def onError(e2: Throwable): Unit = break(s"$a1 is not equal to $e2")
				})
			}

			override def onError(e1: Throwable): Unit = {
				task2.subscribeSync(new doer.MonoObserver[A] {
					override def onSuccess(a2: A): Unit = break(s"$e1 is not equal to $a2")

					override def onError(e2: Throwable): Unit = if e1 ==== e2 then promise.trySuccess(()) else break(s"$e1 is not equal to $e2")
				})
			}
		})
		promise.future
	}

	// Monadic left identity law: Task.ready(x).flatMap(f) == f(x)
	test("Task: left identity") {
		val generators = getGenerators
		import generators.*
		PropF.forAllF { (x: Int, f: Int => Task[Int]) =>
			val left: doer.Task[Int] = Task_ready(x).flatMap(f)
			val right: doer.Task[Int] = f(x)
			checkEquality(doer)(left, right)
		}
	}

	// Monadic right identity law: m.flatMap(Task.ready) == m
	test("Task: right identity") {
		val generators = getGenerators
		import generators.*
		PropF.forAllF { (m: Task[Int]) =>
			val left = m.flatMap(Task_ready)
			val right = m
			checkEquality(doer)(left, right)
		}
	}

	// Monadic associativity law: m.flatMap(f).flatMap(g) == m.flatMap(x => f(x).flatMap(g))
	test("Task: associativity") {
		val generators = getGenerators
		import generators.*

		PropF.forAllF { (m: Task[Int], f: Int => Task[Int], g: Int => Task[Int]) =>
			val leftAssoc = m.flatMap(f).flatMap(g)
			val rightAssoc = m.flatMap(x => f(x).flatMap(g))
			checkEquality(doer)(leftAssoc, rightAssoc)
		}
	}

	// Functor: `m.map(f) == m.flatMap(a => ready(f(a)))`
	test("Task: can be transformed with map") {
		val generators = getGenerators
		import generators.*

		PropF.forAllF { (m: Task[Int], f: Int => String) =>
			val left = m.map(f)
			val right = m.flatMap(a => Task_ready(f(a)))
			checkEquality(doer)(left, right)
		}
	}

	// Recovery: `failedVenture.recover(f) == if f.isDefinedAt(e) then successful(f(e)) else failed(e)` where e is the exception thrown by failedVenture
	test("Task: can be recovered from non fatal failure") {
		val generators = getGenerators
		import generators.*

		PropF.forAllF { (thrownException: Throwable, f: Function[Throwable, Int]) =>
			val int = f(thrownException)
			val fIsDefined = int % 2 == 0

			if NonFatal(thrownException) then {
				val leftTask = Task_fail(thrownException).recover { e =>
					if fIsDefined then Maybe(int) else Maybe.empty
				}
				val rightVenture = if fIsDefined then Task_ready(int) else Task_fail(thrownException)
				checkEquality(doer)(leftTask, rightVenture)
			} else Future.successful(()) // TODO check fatal ones
		}
	}

	test("Task: any pair of task can be combined") {
		val generators = getGenerators
		import generators.*

		PropF.forAllF { (taskA: Task[Int], taskB: Task[Int], f: (Int, Int) => Int) =>
			import scala.concurrent.ExecutionContext.Implicits.global
			val combinedTask = Task_combine(taskA, taskB)(f)

			val fCombined = combinedTask.toFuture().transform(scala.util.Success.apply)
			val fA = taskA.toFuture().transform(scala.util.Success.apply)
			val fB = taskB.toFuture().transform(scala.util.Success.apply)

			for {
				combinedResult <- fCombined
				taskAResult <- fA
				taskBResult <- fB
			} yield {
				val expected = for {
					a <- taskAResult
					b <- taskBResult
				} yield f(a, b)
				import readren.sequencer.GeneratorsForDoerTests.====
				assert(combinedResult ==== expected || (taskAResult.isFailure && combinedResult ==== taskAResult) || (taskBResult.isFailure && combinedResult ==== taskBResult), s"combinedResult: $combinedResult != expected: $expected")
			}
		}
	}

	test("Task: `doer.Task.foreign(foreignDoer)(foreignTask)` should complete in the `doer`'s thread") {
		val generators = getGenerators
		import generators.*
		PropF.forAllNoShrinkF {
			for {
				expectedInt <- smallIntGen
				expectedResult <- genTryFrom[Int](expectedInt, "expectedResult")
				foreignTask <- foreignDoerGenerators(true).genTaskFrom(expectedResult)
			} yield (expectedInt, expectedResult, foreignTask)
		} { case (expectedInt, expectedResult, foreignTask) =>
			// println(s"Begin: foreignTask: $foreignTask")
			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			doer.Task_from(foreignDoer)(foreignTask).subscribeSync(new MonoObserver[Int] {
				override def onSuccess(a: Int): Unit = {
					if a != expectedInt || expectedResult.isFailure then break(s"Unexpected result: Success($a) != $expectedResult")
					else if !doer.isInSequence then break(s"The observer wasn't executed within the DoSerEx")
					else promise.trySuccess(())
				}

				override def onError(e: Throwable): Unit = {
					if expectedResult.fold(_ ne e, _ => true) then break(s"Unexpected result: Failure($e) != $expectedResult")
					else if !doer.isInSequence then break(s"The observer wasn't executed within the DoSerEx")
					else promise.trySuccess(())
				}
			})
			promise.future
		}
	}

	//// TASK EXCEPTION HANDLING ////

	test("Task exception handling: if a function operand passed to a Task operation throws an exception then, non-fatal exceptions should be propagated or uncaught depending on the operation is guarded or not.") {
		val generators = getGenerators
		import generators.*

		PropF.forAllNoShrinkF(
			for {
				successfulTask <- genSuccessfulTask[Int]()
				failingTaskException <- throwableArbitrary.arbitrary
				failingTask <- genFailingTaskFrom(failingTaskException)
				expectedUnhandledException <- throwableArbitrary.arbitrary
			} yield (successfulTask, failingTaskException, failingTask, expectedUnhandledException)
		) { case (successfulTask, failingTaskException, failingTask, expectedUnhandledException) =>
			// println(s"Begin: successfulTask=$successfulTask, failingTaskException=$failingTaskException, failingTask=$failingTask, expectedException: $expectedUnhandledException")

			/** Do the test for a single operation */
			def check[R](opName: String, operatedTask: Task[R], shouldPropagateNonFatales: Boolean = false): Future[Unit] = {
				// Apply the operation to the random task and trigger the execution passing a faulty on-complete callback.
				val promise = Promise[Unit]()

				given Promise[Unit] = promise

				observingUnhandledExceptionsDo { () =>
					// Apply the operation to the random venture.
					operatedTask.trigger()(new MonoObserver[R] {
						override def onSuccess(operationResult: R): Unit = {
							break(s"`$opName`: Completed successfully despite an exception was thrown: $expectedUnhandledException")
						}

						override def onError(actualException: Throwable): Unit = {
							if actualException ==== failingTaskException then promise.trySuccess(())
							else if !NonFatal(expectedUnhandledException) then {
								scribe.error(actualException)
								break(s"`$opName` incorrectly caught the fatal exception [$expectedUnhandledException] (and propagated [$actualException])")
							}
							else if shouldPropagateNonFatales then {
								if (actualException ne expectedUnhandledException) && (actualException.getCause ne expectedUnhandledException) then break(s"`$opName` caught the non fatal exception [$expectedUnhandledException] but propagated another one: [$actualException]")
								else promise.trySuccess(())

							} else break(s"`$opName` incorrectly caught [$expectedUnhandledException] (and propagated [$actualException]).")
						}
					})

					breakAfterWaiting(999, s"`$opName`: No notification of the exception until 999 milliseconds after applying the operation. Waiting aborted.")

				} { (doer, unCaughtException) =>
					// For the exception to be uncaught it should be fatal or the operation be non-guarded.
					if unCaughtException ne expectedUnhandledException then break(s"`$opName`: An unexpected exception was uncaught: $unCaughtException")
					else if shouldPropagateNonFatales && NonFatal(expectedUnhandledException) then break(s"`$opName` had not caught the non-fatal exception [$unCaughtException]")
					else promise.trySuccess(())
				}
			}

			def f0[A](): A = throw expectedUnhandledException

			def f1[A, B](a: A): B = throw expectedUnhandledException

			def f2[A, B, C](a: A, b: B): C = throw expectedUnhandledException

			for {
				_ <- check("apply", Task_apply(f0))
				_ <- check("defer", Task_defers(f0))
				_ <- check("fromForeign", Task_from(foreignDoer)(foreignDoer.Task_apply(f0)))
				_ <- check("fromFutureBuilder", Task_from(f0, false), false)
				_ <- check("fromFutureBuilderGuarded", Task_from(f0, true), true)

				_ <- check("combine1", Task_combine(successfulTask, failingTask)(f2))
				_ <- check("combine2", Task_combine(failingTask, successfulTask)(f2))

				_ <- check("withFilter", successfulTask.withFilter(f1))
				_ <- check("withFilterGuarded", successfulTask.withFilterGuarded(f1), true)

				_ <- check("andThen1", successfulTask.andThen(f1))
				_ <- check("andThen2", failingTask.andThen(_ => (), f1))

				_ <- check("map", successfulTask.map(f1))
				_ <- check("mapGuarded", successfulTask.mapGuarded(f1), true)

				_ <- check("flatMap", successfulTask.flatMap(f1))
				_ <- check("flatMapGuarded", successfulTask.flatMapGuarded(f1), true)

				_ <- check("transform1", successfulTask.transform(f1))
				_ <- check("transform2", failingTask.transform(f1))
				_ <- check("guarded.transform3", successfulTask.guarded.transform(f1), true)
				_ <- check("guarded.transform4", failingTask.guarded.transform(f1), true)

				_ <- check("transformWith1", successfulTask.transformWith(f1))
				_ <- check("transformWith2", failingTask.transformWith(f1))
				_ <- check("guarded.transformWith1", successfulTask.guarded.transformWith(f1), true)
				_ <- check("guarded.transformWith2", failingTask.guarded.transformWith(f1), true)

				_ <- check("recover", failingTask.recover(f1))
				_ <- check("guarded.recover", failingTask.guarded.recover(f1), true)

				_ <- check("recoverWith", failingTask.recoverWith(f1))
				_ <- check("guarded.recoverWith", failingTask.guarded.recoverWith(f1), true)

				_ <- check("repeatedUntilSome1", successfulTask.repeatedUntilSome(f2))
				_ <- check("repeatedUntilSome2", failingTask.repeatedUntilSome(f2))

				_ <- check("repeatedWhileEmpty1", successfulTask.repeatedWhileEmpty(Success(0), f2))
				_ <- check("repeatedWhileEmpty2", failingTask.repeatedWhileEmpty(Success(0), f2))

				_ <- check("repeatedWhileUndefined1", successfulTask.repeatedWhileUndefined(Success(0), { case (a, b) => f2(a, b) }))
				_ <- check("repeatedWhileUndefined2", failingTask.repeatedWhileUndefined(Success(0), { case (a, b) => f2(a, b) }))
			} yield ()
		}
	}

	test("Task exception handling: `subscribe` should not catch exceptions thrown by the passed `MonoObserver` methods") {
		val generators = getGenerators
		import generators.*

		PropF.forAllNoShrinkF { (task1: Task[Int], task2: Task[Int], thrownException: Throwable, future: Future[Int]) =>


			def check[R](opName: String, operatedTask: Task[R]): Future[Unit] = {
				val promise = Promise[Unit]()

				given Promise[Unit] = promise

				observingUnhandledExceptionsDo { () =>
					// Trigger the execution passing a faulty on-complete callback.
					operatedTask.trigger(false)(new MonoObserver[R] {
						override def onSuccess(value: R): Unit = throw thrownException

						override def onError(actualException: Throwable): Unit = throw thrownException
					})

					breakAfterWaiting(999, s"$opName: No notification of the exception until 999 milliseconds after applying the operation. Waiting aborted.")

				} { (d, t) =>
					// For the exception to be unhandled, it should be fatal and the operation of the kind that does not handle them.
					if (d eq doer) && (t eq thrownException) then promise.trySuccess(())
				}(using promise)
			}

			val randomInt = thrownException.getMessage.hashCode()
			val smallNonNegativeInt = randomInt % 9
			val randomBool = (randomInt % 2) == 0
			val randomTryInt = if randomBool then Success(randomInt) else Failure(thrownException)
			// println(s"Begin: venture=$task, exception=$exception, randomInt=$randomInt, randomBool=$randomBool")

			for {
				_ <- check("ready", Task_ready(randomInt))
				_ <- check("fail", Task_fail(thrownException))
				_ <- check("apply", Task_apply(() => randomInt))
				_ <- check("defer", Task_apply(() => task1))
				_ <- check("fromForeign", Task_from(foreignDoer)(foreignDoer.Task_apply(() => randomInt)))
				_ <- check("fromFuture", Task_from(future))
				_ <- check("fromFutureDeferred", Task_from(() => future))

				_ <- check("withFilter", task1.withFilter(_ => randomBool))
				_ <- check("withFilterGuarded", task1.withFilterGuarded(_ => randomBool))

				_ <- check("andThen1", task1.andThen(_ => (), _ => ()))

				_ <- check("map", task1.map(identity))
				_ <- check("mapGuarded", task1.mapGuarded(identity))

				_ <- check("flatMap", task1.flatMap(_ => task2))
				_ <- check("flatMapGuarded", task1.flatMapGuarded(_ => task2))

				_ <- check("transform", task1.transform(identity))
				_ <- check("guarded.transform", task1.guarded.transform(identity))

				_ <- check("transformWith", task1.transformWith(_ => task2))
				_ <- check("guarded.transformWith", task1.guarded.transformWith(_ => task2))

				_ <- check("recover", task1.recover { _ => if randomBool then Maybe(randomInt) else Maybe.empty })
				_ <- check("guarded.recover", task1.guarded.recover { _ => if randomBool then Maybe(randomInt) else Maybe.empty })

				_ <- check("recoverWith", task1.recoverWith { _ => if randomBool then Maybe(task2) else Maybe.empty })
				_ <- check("guarded.recoverWith", task1.guarded.recoverWith { _ => if randomBool then Maybe(task2) else Maybe.empty })

				_ <- check("repeatedUntilSome", task1.repeatedUntilSome { (n, i) => if n > smallNonNegativeInt then Maybe(randomTryInt) else Maybe.empty })
				_ <- check("repeatedWhileEmpty", task1.repeatedWhileEmpty(Success(0), (n, tryInt) => if n > smallNonNegativeInt then Maybe(randomTryInt) else Maybe.empty))
				_ <- check("repeatedWhileUndefined", task1.repeatedWhileUndefined(Success(0), { case (n, tryInt) if n > smallNonNegativeInt => randomInt }))
			} yield ()
		}
	}


	//// CAPTOR ////

	test("Captor: `Captor.seize(v)` should trigger a single execution, passing `v`, of each subscribed consumers it has wired.") {
		val generators = getGenerators
		import generators.*
		PropF.forAllF(
			for {
				expectedSuccessfulResult <- smallIntGen
				expectedResult <- genTryFrom(expectedSuccessfulResult, "expected successful result")
				numberOfPendingSubscriptions <- Gen.choose(1, 17)
			} yield (expectedSuccessfulResult, expectedResult, numberOfPendingSubscriptions)
		) { case (expectedSuccessfulResult, expectedResult, numberOfPendingSubscriptions) =>
			// println(s"Begin: int: $int, f1(int): ${f1(int)}")
			val promise = Promise[Unit]()
			val testedCaptor = doer.Captor[Int]()
			checkCaptor[doer.type](doer, testedCaptor, promise, expectedSuccessfulResult, expectedResult, numberOfPendingSubscriptions, () => testedCaptor.seize(expectedResult))
			gate(using promise)
		}
	}

	test("Captor: `Captor.seizeWith(task)` should trigger a single execution, passing what `task` shields, of each subscribed consumers it has wired.") {
		val generators = getGenerators
		import generators.{taskArbitrary, *}
		PropF.forAllF(
			for {
				expectedSuccessfulResult <- smallIntGen
				expectedResult <- genTryFrom(expectedSuccessfulResult, "expected successful result")
				numberOfPendingSubscriptions <- Gen.choose(1, 17)
			} yield (expectedSuccessfulResult, expectedResult, numberOfPendingSubscriptions)
		) { case (expectedSuccessfulResult, expectedResult, numberOfPendingSubscriptions) =>
			// println(s"Begin: int: $int, task: $task, f1(int): ${f1(int)}")
			val promise = Promise[Unit]()

			val testedCaptor = doer.Captor[Int]()
			val completingCaptor = doer.Captor[Int]()
			testedCaptor.seizeWith(completingCaptor)
			checkCaptor[doer.type](doer, testedCaptor, promise, expectedSuccessfulResult, expectedResult, numberOfPendingSubscriptions, () => completingCaptor.seize(expectedResult))

			gate(using promise)
		}
	}

	private def checkCaptor[DD <: Doer](doer: DD, testedCaptor: doer.Captor[Int], promise: Promise[Unit], expectedSuccessfulResult: Int, expectedResult: Try[Int], numberOfPendingSubscriptions: Int, capture: () => Unit): Unit = {
		given Promise[Unit] = promise

		import doer.*

		val notifiedObserversCdl = CountDownLatch(numberOfPendingSubscriptions)
		for index <- 0 until numberOfPendingSubscriptions do {
			testedCaptor.subscribe(false)(new MonoObserver[Int] {
				override def onSuccess(a: Int): Unit = {
					if expectedResult.fold(_ => true, _ != a) then break(s"the observer #$index received an unexpected value: Success($a) != $expectedResult")
					notifiedObserversCdl.countDown()
				}

				override def onError(e: Throwable): Unit = {
					if expectedResult.fold(_ ne e, _ => true) then break(s"the observer #$index received an unexpected value: Failure($e) != $expectedResult")
					notifiedObserversCdl.countDown()
				}
			})
		}

		doer.executeSequentially { () =>
			try {
				if !testedCaptor.isPending then break("`isPending` returned false despite no capturing was done")
				if testedCaptor.isCompleted then break("`isCompleted` returned true despite no capturing was done")
				capture()
				if testedCaptor.isPending then break("`isPending` returned true despite the capturing was done")
				if !testedCaptor.isCompleted then break("`isCompleted` returned false despite the capturing was done")
			} catch {
				case cause: Throwable => promise.tryFailure(cause)
			}
		}

		if notifiedObserversCdl.await(99, TimeUnit.MILLISECONDS) then promise.trySuccess(())
		else break(s"At least one observer has not been notified")
	}

	//// CAUSAL FENCE

	test("CausalFence: multiple stepped advances should serialize and commit in order") {
		val generators = getGenerators
		import generators.*

		PropF.forAllNoShrinkF { (initial: Int, updater: Int => Capturer[Int]) =>
			// println(s"Begin: initial=$initial, updater=$updater)")
			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			val fence = CausalFence[Int, doer.type](doer)(initial)

			def loop(currentValue: Int, repetition: Int): Unit = {
				if repetition == 9 then promise.trySuccess(())
				else {
					updater(currentValue).triggerHardy(true) { expectedNextState =>
						val advanceCapturer = fence.advance[Int] { previousValue =>
							if previousValue != currentValue then break(s"repetition #$repetition mismatch")
							updater(previousValue)
						}
						advanceCapturer.triggerCallbacks(true)(
							actualNextSuccessfulState => {
								if expectedNextState.fold(_ => true, _ != actualNextSuccessfulState) then break(s"Expected: $expectedNextState, got: Success($actualNextSuccessfulState)")
								else loop(actualNextSuccessfulState, repetition + 1)
							},
							actualNextFaultyState => {
								if expectedNextState.fold(_ !=== actualNextFaultyState, _ => true) then break(s"Expected: $expectedNextState, got: Failure($actualNextFaultyState)")
								else promise.trySuccess(())
							}
						)
					}
				}
			}

			run(loop(initial, 0))
			gate
		}
	}

	test("CausalFence: multiple simultaneous advances should serialize and commit in order") {
		val generators = getGenerators
		import generators.*

		PropF.forAllNoShrinkF(
			smallIntGen,
			Gen.function1[Int, Capturer[Int]](genSuccessfulCapturer[Int]())
		) { (initial, updater) =>
			val promise = Promise[Unit]()
			given Promise[Unit] = promise

			run {
				val fence = CausalFence[Int, doer.type](doer)(initial)

				val actualSteps = for i <- 0 to 9 yield fence.advance(updater)

				def loop(previousState: Int, repetition: Int): Task[List[Int]] = {
					if repetition > 9 then Task_ready(Nil)
					else for {
						nextState <- updater(previousState)
						followingStates <- loop(nextState, repetition + 1)
					} yield nextState :: followingStates
				}

				val expectedResultsTask = loop(initial, 0)

				for {
					actualResults <- doer.Task_sequenceToArray(actualSteps)
					expectedResults <- expectedResultsTask
				} do {
					// println(s"expected:${expectedResults.mkString(", ")}, actual:${actualResults.mkString(", ")}")
					if actualResults.toList != expectedResults then break(s"expected:${expectedResults.mkString(", ")}, actual:${actualResults.mkString(", ")}")
					else promise.trySuccess(())
				}
			}
			gate
		}
	}

	/**
	 * Test invariants of [[Doer.CausalFence]] ensuring that synchronous consumers of the [[Doer.Capturer]] returned by [[Doer.CausalFence.advance]] observe the up‑to‑date state deterministically.
	 *
	 * Unique checks in this test:
	 *  - Consumers subscribed immediately (synchronously) to the [[Doer.Capturer]] returned by [[Doer.CausalFence.advance]] must be executed strictly in order of subscription, before any other consumer, and even before the updaters passed to subsequent calls to [[advance]].
	 *
	 *  - A consumer subscribed immediately (synchronously) to the [[Doer.Capturer]] returned by [[causalAnchor]] must observe either the state to which the last advance transitioned to, or a state produced earlier, but never an later one.
	 *
	 *  - Game‑changing invariant: Immediately after an [[Doer.CausalFence.advance]] call, there are no other advances in flight except the one just created. The returned [[Doer.Captor]] (seen as [[Doer.Capturer]]) is the new tail, and any immediate synchronous subscription to it is guaranteed to be the first subscriber in its list. Therefore, when the Captor fulfills, that consumer sees the up‑to‑date state deterministically, free of concurrent updates to the primary state.
	 *
	 * The test constructs multiple paths that repeatedly advance the fence up to a top serial number, failing if any consumer observes stale state, incorrect ordering, or out‑of‑sequence execution.
	 * TODO This is a cheating variant of the following test. The cheat avoids the stack-overflow bug mentioned in [[Doer.Captor.captureSync]]'s documentation. Remove this test and keep the following one when appropriate.
	 */
	test("CausalFence - synchronous consumer ordering and anchor freshness: synchronous consumers see up‑to‑date state deterministically - using hoping tasks to avoid stack overflow (much faster than the version that uses random delays below)") {
		val generators = getGenerators
		import generators.*

		/** Build an [[Mono]] that yields the provided `serial` hopping (= calling [[Doer.executeSequentially]]) the specified times.
		 * // TODO: Using this method is cheating. Replace it with a randomly generated successful Mono. */
		def buildHopingMono(serial: Int, hops: Int): Mono[Int] = {
			if hops <= 0 then Task_ready(serial)
			else Captor[Int]().seizeWith(buildHopingMono(serial, hops - 1), false)
		}

		type PrimaryState = (pathId: Int, serial: Int)
		val initialState: PrimaryState = (0, 0)
		PropF.forAllF(
			for {
				swarmSize <- Gen.choose(1, 9)
				// The head and tail of hopsList are generated separately to ensure the list is non-empty even when scalacheck is shrinking the sample.
				hopsHead <- Gen.choose(0, 9)
				hopsTail <- Gen.listOfN(99, Gen.choose(0, 9))
			} yield (swarmSize, hopsHead, hopsTail)
		) { (swarmSize: Int, hopsHead: Int, hopsTail: List[Int]) =>
			val hopsList = hopsHead :: hopsTail
			val topSerial = hopsList.size

			// println(s"Begin: swarmSize=$swarmSize, topSerial=$topSerial, hopsList=$hopsList")

			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			val fence = CausalFence[PrimaryState, doer.type](doer)(initialState)
			var derivedSerial: Int = 0

			def path(pathId: Int): Capturer[PrimaryState] = {
				var hasAdvanced = false
				for {
					nextState <- {
						// Do a state transition that increments the `serial` field and keeps the `path` field invariant.
						fence.advanceIf { (previous: PrimaryState) =>
							if previous.serial >= topSerial then Maybe.empty
							else {
								val commitedAtStart: PrimaryState = fence.committedState.getOrElse(break(s"Unexpected failing state at updater start: ${fence.committedState}"))
								val mono = buildHopingMono(previous.serial + 1, hopsList(previous.serial))
									.map(newSerial => (pathId, newSerial))
									.andThen { nextState =>
										val committedAtEnd = fence.committedState.getOrElse(break(s"Unexpected failing state at updater end: ${fence.committedState}"))
										if commitedAtStart.serial != committedAtEnd.serial then break(s"In the interval between the updater passed to `advance` is called and the Mono it returns completes, no other updater is started; and that is not happening.")
									}
								hasAdvanced = true
								Maybe.some(mono)
							}
						}
					}
					anchoredState <- {
						val committedState = fence.committedState.getOrElse(break(s"The Capturer returned by advanceIf yielded an unexpected failing state: ${fence.committedState}"))
						if hasAdvanced && nextState.pathId != pathId then break(s"A consumer subscribed to the Capturer returned by `advance` should see the state to which the advance transitioned to; and is not happening: pathId=$pathId, actual: ${nextState.pathId}")
						else if derivedSerial > nextState.serial then break(s"Consumers subscribed immediately (in a synchronously coupled manner) to the `Capturer` returned by `advance`, should be executed in order of subscription before any other consumer, even before the updaters passed to subsequent calls to advance; and is not happening.")
						else if nextState.serial != committedState.serial then break(s"A consumer subscribed immediately (in a synchronously coupled manner) to the Capturer returned by `advance` should see the up-to-date state; and is not happening: current=$nextState, commited=$committedState")
						else derivedSerial = nextState.serial
						fence.causalAnchor()
					}
					recursiveState <- {
						val committedState = fence.committedState.getOrElse(break(s"The Capturer returned by causalAnchor yielded an unexpected failing state: ${fence.committedState}"))
						if anchoredState.serial != committedState.serial then break(s"A consumer subscribed immediately (in a synchronously coupled manner) to the `Capturer` returned by `causalAnchor` should see the the up-to-date state; and is not happening: current=$anchoredState, commited=${fence.committedState}")
						if nextState.serial < topSerial then path(pathId)
						else fence.committed
					}
				} yield {
					val committedState = fence.committedState.getOrElse(break(s"The Capturer returned by `committed` yielded an unexpected failing state: ${fence.committedState}"))
					if recursiveState.serial != committedState.serial then break(s"followingState=$recursiveState, commited=${fence.committedState}")
					recursiveState
				}
			}

			val swarm: Seq[Mono[PrimaryState]] = Seq.tabulate(swarmSize) { n => doer.Capturer_defer(() => path(n)) }
			val checks = for array <- doer.Task_sequenceToArray(swarm) yield promise.trySuccess(())
			checks.triggerAndForget()
			gate
		}

	}


	/**
	 * Test invariants of [[Doer.CausalFence]] ensuring that synchronous consumers of the [[Doer.Capturer]] returned by [[Doer.CausalFence.advance]] observe the up‑to‑date state deterministically.
	 *
	 * Unique checks in this test:
	 *  - Consumers subscribed immediately (synchronously) to the [[Doer.Capturer]] returned by [[Doer.CausalFence.advance]] must be executed strictly in order of subscription, before any other consumer, and even before the updaters passed to subsequent calls to [[advance]].
	 *
	 *  - A consumer subscribed immediately (synchronously) to the [[Doer.Capturer]] returned by [[causalAnchor]] must observe either the state to which the last advance transitioned to, or a state produced earlier, but never an later one.
	 *
	 *  - Game‑changing invariant: Immediately after an [[Doer.CausalFence.advance]] call, there are no other advances in flight except the one just created. The returned [[Doer.Captor]] (seen as [[Doer.Capturer]]) is the new tail, and any immediate synchronous subscription to it is guaranteed to be the first subscriber in its list. Therefore, when the Captor fulfills, that consumer sees the up‑to‑date state deterministically, free of concurrent updates to the primary state.
	 *
	 * The test constructs multiple paths that repeatedly advance the fence up to a top serial number, failing if any consumer observes stale state, incorrect ordering, or out‑of‑sequence execution.\
	 * A non-cheating variant of the previous test using random Monos. It unveils the stack-overflow bug mentioned in [[Doer.Captor.captureSync]]'s documentation.
	 * TODO stack-overflows when many consecutive synchronous transitions occur. Consider a solution. See note in [[Doer.Captor.captureSync]]. If this problem deserves a solution, add the generation of samples whose transitions are all synchronic to verify if the solution works.
	 */
	test("CausalFence - synchronous consumer ordering and anchor freshness: synchronous consumers see up‑to‑date state deterministically - using random delays (very slow)") {
		val generators = getGenerators
		import generators.*

		type PrimaryState = (pathId: Int, serial: Int)
		val initialState: PrimaryState = (0, 0)
		val topSerial = 99 // Note that incrementing this number causes stack overflow when syncOnly == true. See note in `Captor.captureSync`.
		PropF.forAllF(Gen.choose(1, 9), Gen.oneOf(true, false)) { (swarmSize: Int, syncOnly: Boolean) =>
			// println(s"Begin: swarmSize=$swarmSize, syncOnly=$syncOnly")

			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			val fence = CausalFence[PrimaryState, doer.type](doer)(initialState)
			var derivedSerial: Int = 0

			def path(pathId: Int): Capturer[PrimaryState] = {
				for {
					nextState <- {
						// Do a state transition that increments the `serial` field and keeps the `path` field invariant.
						fence.advance { (previous: PrimaryState) =>
							val commitedAtStart: PrimaryState = fence.committedState.getOrElse(break(s"Unexpected failing state at updater start: ${fence.committedState}"))
							val monoGenerator: Gen[Mono[Int]] = genSuccessfulMonoFrom(previous.serial + 1, syncOnly)
							val randomMono: Mono[Int] = monoGenerator.sample.get
							// val delay = Gen.choose(-1, 1).sample.get
							// val task = if delay > 0 then randomTask.delayed(delay) else randomTask
							randomMono.map(newSerial => (pathId, newSerial))
								.andThen { nextState =>
									val committedAtEnd = fence.committedState.getOrElse(break(s"Unexpected failing state at updater end: ${fence.committedState}"))
									if commitedAtStart.serial != committedAtEnd.serial then break(s"In the interval between the updater passed to `advance` is called and the Task it returns completes, no other updater is started; and that is not happening.")
								}
						}
					}
					anchoredState <- {
						val committedState = fence.committedState.getOrElse(break(s"The Capturer returned by advanceIf yielded an unexpected failing state: ${fence.committedState}"))
						if nextState.pathId != pathId then break(s"A consumer subscribed to the Capturer returned by `advance` should see the state to which the advance transitioned to; and is not happening: pathId=$pathId, actual: ${nextState.pathId}")
						else if derivedSerial > nextState.serial then break(s"Consumers subscribed immediately (in a synchronously coupled manner) to the `Capturer` returned by `advance`, should be executed in order of subscription before any other consumer, even before the updaters passed to subsequent calls to advance; and is not happening.")
						else if nextState.serial != committedState.serial then break(s"A consumer subscribed immediately (in a synchronously coupled manner) to the Capturer returned by `advance` should see the up-to-date state; and is not happening: current=$nextState, commited=$committedState")
						else derivedSerial = nextState.serial
						fence.causalAnchor()
					}
					recursiveState <- {
						val committedState = fence.committedState.getOrElse(break(s"The Capturer returned by causalAnchor yielded an unexpected failing state: ${fence.committedState}"))
						if anchoredState.serial != committedState.serial then break(s"A consumer subscribed immediately (in a synchronously coupled manner) to the `Capturer` returned by `causalAnchor` should see the the up-to-date state; and is not happening: current=$anchoredState, commited=${fence.committedState}")
						if nextState.serial < topSerial then path(pathId)
						else fence.committed
					}
				} yield {
					val committedState = fence.committedState.getOrElse(break(s"The Capturer returned by `committed` yielded an unexpected failing state: ${fence.committedState}"))
					if recursiveState.serial != committedState.serial then break(s"followingState=$recursiveState, commited=${fence.committedState}")
					recursiveState
				}
			}

			val swarm: Seq[Mono[PrimaryState]] = Seq.tabulate(swarmSize) { n => Capturer_defer(() => path(n)) }
			val checks = for array <- doer.Task_sequenceToArray(swarm) yield promise.trySuccess(())
			checks.triggerAndForget()
			gate
		}
	}

	test("CausalFence: `advance` should either, skip transitions if failed, or commit updated state if successful") {
		val generators = getGenerators
		import generators.*

		PropF.forAllNoShrinkF { (expectedState0: Int, firstUpdater: Int => Mono[Int], secondUpdater: Int => Mono[Int]) =>
			// println(s"initial: $initial")
			val promise = Promise[Unit]()
			given Promise[Unit] = promise

			run {
				val fence = CausalFence[Int, doer.type](doer)(expectedState0)

				fence.advance(firstUpdater)

				fence.advance(secondUpdater).subscribeSyncCallbacks(
					actualState2 => firstUpdater(expectedState0).subscribeSyncCallbacks(
						expectedState1 => secondUpdater(expectedState1).subscribeSyncCallbacks(
							expectedState2 => {
								if actualState2 != expectedState2 then break(s"Unexpected final state: actual=$actualState2, expected=$expectedState2")
								else promise.trySuccess(())
							},
							expectedFailure2 => break(s"Unexpected final state: actual=$actualState2, expected=$expectedFailure2")
						),
						expectedFailure1 => break(s"Unexpected final state: actual=$actualState2, expected=$expectedFailure1")
					),
					actualFailure2 => firstUpdater(expectedState0).subscribeSyncCallbacks(
						expectedState1 => secondUpdater(expectedState1).subscribeSyncCallbacks(
							expectedState2 => break(s"Unexpected final state: actual=$actualFailure2, expected=$expectedState2"),
							expectedFailure2 => {
								if actualFailure2 !=== expectedFailure2 then break(s"Unexpected final state: actual=$actualFailure2, expected=$expectedFailure2")
								else promise.trySuccess(())
							},
						),
						expectedFailure1 => {
							if actualFailure2 !=== expectedFailure1 then break(s"Unexpected final state: actual=$actualFailure2, expected=$expectedFailure1")
							else promise.trySuccess(())
						}
					)
				)				
			}
			gate
		}
	}


	test("CausalFence: rollback before commit should be applied") {
		val generators = getGenerators
		import generators.*

		type PrimaryState = Int
		PropF.forAllNoShrinkF { (expectedInitialState: PrimaryState, updater: PrimaryState => Mono[PrimaryState]) =>
			val promise = Promise[Unit]()
			given Promise[Unit] = promise

			run {
				val fence = CausalFence[PrimaryState, doer.type](doer)(expectedInitialState)

				fence.advanceSpeculatively { (actualInitialState, rba) =>
					def doTheRollback(): Unit = {
						rba.rollback(
							true,
							new CompletionObserver[PrimaryState] {
								override def onSuccess(actualFinalSuccessState: PrimaryState, rollbackApplication: OriginId): Unit = {
									if rollbackApplication != ROLLBACK_APPLIED then break("The rollback's CompletionObserver was told that the rollback was not applied despite it should")
									else if actualFinalSuccessState != expectedInitialState then break("The rollback's CompletionObserver was told that the previous state wasn't restored despite it should")
								}

								override def onError(e: Throwable, rollbackApplication: OriginId): Unit = break("The rollback's CompletionObserver was told that the previous state is a failure despite it isn't")
							}
						)
					}

					updater(actualInitialState).andThen(
						_ => doTheRollback(),
						_ => doTheRollback()
					)
				}.subscribeSyncCallbacks(
					actualFinalSuccessState => if actualFinalSuccessState != expectedInitialState then break("The `Capturer` returned by `advanceSpeculatively` yielded an unexpected value"),
					_ => break("The `Capturer` returned by `advanceSpeculatively` received a sticking/failure state despite it shouldn't")
				)

				fence.causalAnchor(new CompletionObserver[PrimaryState] {
					override def onSuccess(actualSuccessfulState: PrimaryState, application: RollbackApplication): Unit = {
						if actualSuccessfulState != expectedInitialState then break("The `causalAnchor`'s CompletionObserver received an unexpected value")
					}

					override def onError(e: Throwable, originId: OriginId): Unit = break("The causal anchor`s CompletionObserver received a sticking/failure state despite it shouldn't")
				}
				).subscribeSyncCallbacks(
					actualSuccessfulState => if actualSuccessfulState != expectedInitialState then break("The `Capturer` returned by `causalAnchor` yielded an unexpected value"),
					_ => break("The `Capturer` returned by `causalAnchor` captured an error and it shouldn't")
				)

				fence.committed.subscribeSyncCallbacks(
					actualCommitted => {
						if actualCommitted != expectedInitialState then break("The `Capturer` returned by `committed` captured an unexpected value")
						else promise.trySuccess(())
					},
					_ => break("The `Capturer` returned by `committed` captured an error and it shouldn't")
				)
			}
			gate
		}
	}

	test("Generators: random functions are deterministic") {
		val generators = getGenerators
		import generators.*

		type PrimaryState = Int
		PropF.forAllNoShrinkF { (state0: PrimaryState, updater: PrimaryState => Mono[PrimaryState]) =>
			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			updater(state0).triggerHardy(false) { state1A =>
				updater(state0).triggerHardy(true) { state1B =>
					if state1A !=== state1B then break("not deterministic")
					else promise.trySuccess(())
				}

			}
			gate
		}
	}

	test("CausalFence: rollback after commit should be ignored") {
		val generators = getGenerators
		import generators.*

		type PrimaryState = Int
		PropF.forAllNoShrinkF { (initialState: PrimaryState, updater: PrimaryState => Mono[PrimaryState]) =>
			// println(s"Begin: initialState=$initialState, updater=$updater")
			val promise = Promise[Unit]()
			given Promise[Unit] = promise

			doer.executeSequentially { () =>
				try {
					updater(initialState).triggerHardy(false) { expectedFinalState =>
						println(s"Begin: expectedFinalState=$expectedFinalState")

						val fence = CausalFence[PrimaryState, doer.type](doer)(initialState)

						fence.advanceSpeculatively { (actualInitialState, rba) =>
							def doTheRollback(): Unit = {
								// ensure `rollback` is called after the venture returned by primaryStateUpdater is fulfilled.
								run {
									rba.rollback(
										true,
										new CompletionObserver[PrimaryState] {
											override def onSuccess(actualFinalSuccessState: PrimaryState, rollbackApplication: OriginId): Unit = {
												if rollbackApplication != ROLLBACK_IGNORED then break("The rollback's CompletionObserver was told that the rollback wasn't ignored despite it should")
												else if expectedFinalState.fold(_ => true, _ != actualFinalSuccessState) then break("The rollback's CompletionObserver received an unexpected primary state")
											}

											override def onError(actualFinalFailureState: Throwable, rollbackApplication: OriginId): Unit = {
												if rollbackApplication != ROLLBACK_IGNORED then break("The rollback's CompletionObserver was told that the rollback wasn't ignored despite it should")
												else if expectedFinalState.fold(_ !=== actualFinalFailureState, _ => true) then break("The rollback's CompletionObserver received an unexpected primary state")
											}
										}
									)
								}
							}

							updater(actualInitialState).andThen(
								_ => doTheRollback(),
								_ => doTheRollback()
							)
						}.triggerHardy(true) { actualFinalState =>
							if actualFinalState !=== expectedFinalState then break(s"The `Capturer` returned by `advanceSpeculatively` received an unexpected state")

							fence.causalAnchor(new CompletionObserver[PrimaryState] {
								override def onSuccess(actualFinalSuccessState: PrimaryState, originId: OriginId): Unit = {
									// println(s"causalAnchor's onSuccess originId = $originId")
									if expectedFinalState.fold(_ => true, _ != actualFinalSuccessState) then break("The `causalAnchor`'s CompletionObserver received an unexpected value")
								}

								override def onError(actualFinalErrorState: Throwable, originId: OriginId): Unit = {
									// println(s"causalAnchor's onError originId = $originId")
									if expectedFinalState.fold(_ !=== actualFinalErrorState, _ => true) then break("The `causalAnchor`'s CompletionObserver received an unexpected value")
								}
							}).subscribeHardy(true) { actualAnchor =>
								if actualAnchor !=== expectedFinalState then break("The capturer returned by `causalAnchor` yielded an unexpected value")
							}

							fence.committed.subscribeHardy(true) { actualCommitedState =>
								if actualCommitedState !=== expectedFinalState then break(s"The capturer returned by `committed` yielded an unexpected value: expected=$expectedFinalState, got=$actualCommitedState")
								else promise.trySuccess(())
							}
						}
					}
				} catch {
					case cause: Throwable => promise.tryFailure(cause)
				}
			}
			gate
		}
	}	

	//// SCHEDULING ////

	//// scheduling infrastructure ////

	test("Scheduling: `SchedulingExtension.schedule` should fail if called with the same `Schedule` instance twice") {
		val generators = getGenerators
		import generators.*

		Prop.forAllNoShrink(Gen.choose(1, 5), Gen.choose(1, 5)) { (delay: Int, interval: Int) =>
			val delaySchedule = doer.newDelaySchedule(delay)
			val fixedRateSchedule = doer.newFixedRateSchedule(delay, interval)
			val fixedDelaySchedule = doer.newFixedRateSchedule(delay, interval)
			doer.schedule(delaySchedule)(_ => ())
			doer.schedule(fixedRateSchedule)(_ => ())
			doer.schedule(fixedDelaySchedule)(_ => ())
			assert(
				intercept[IllegalStateException] {
					doer.schedule(delaySchedule)(_ => ())
				}.getMessage.contains("twice"),
				"No exception thrown despite the same delay schedule was used twice"
			)
			assert(intercept[IllegalStateException] {
				doer.schedule(fixedRateSchedule)(_ => ())
			}.getMessage.contains("twice"), "No exception thrown despite the same fixed rate schedule was used twice")
			assert(intercept[IllegalStateException] {
				doer.schedule(fixedDelaySchedule)(_ => ())
			}.getMessage.contains("twice"), "No exception thrown despite the same fixed delay schedule was used twice")
			doer.cancelAll()
		}
	}

	//// Scheduling factory methods ////

	test("Scheduling: a schedule with a shorter delay programmed after a schedule with a longer delay must wake up the worker and execute on time") {
		val doer = getSharedDoer
		val latch = new CountDownLatch(1)
		val task1Started = new AtomicBoolean(false)
		val task2Completed = new AtomicBoolean(false)

		doer.schedule(doer.newDelaySchedule(100))(s => task1Started.set(true))
		doer.schedule(doer.newDelaySchedule(10))(s => {
			task2Completed.set(true)
			latch.countDown()
		})

		val completed = latch.await(80, TimeUnit.MILLISECONDS)
		assert(completed, "Task with shorter delay did not execute on time (the worker did not wake up early enough)")
		assert(task2Completed.get(), "Task with shorter delay should have completed")
		assert(!task1Started.get(), "Task with longer delay should not have started yet")
		doer.cancelAll()
	}

	test("Scheduling Task: `Task.schedules(newDelaySchedule(delay))(supplier)` should execute the supplier after the delay") {
		val generators = getGenerators
		import generators.*

		PropF.forAllNoShrinkF(Gen.choose(1, 15)) { (delay: Int) =>
			val startNano = System.nanoTime()
			val task = doer.Task_schedules(DELAY, delay, 0)((_, delay * 2))
				.map { case (schedule, x) =>
					val actualDelay = System.nanoTime - startNano
					// println(s"-------> actual delay: ${actualDelay/1000} micros, expected: $delay millis, error: ${actualDelay/1000_000-delay} schedule: $schedule")
					assert(x == delay * 2, s"found: $x, expected: ${x * 2}")
					assert(actualDelay >= delay * 1_000_000, s"actual: $actualDelay, expected: $delay, schedule: $schedule")
				}
			task.toFuture()
		}
	}

	test("Scheduling Task: `Task.schedules(FIXED_RATE, ...)(supplier)` should execute both, the `supplier` and down-chained operations, repeatedly according to the specified specified period until cancellation") {
		val generators = getGenerators
		import generators.*

		PropF.forAllNoShrinkF(Gen.choose(1, 10), Gen.choose(1, 10)) { (initialDelay: Int, interval: Int) =>
			val repetitions = 10 - interval
			// println(s"\nBegin: initialDelay = $initialDelay, interval = $interval, repetitions = $repetitions")
			val promise = Promise[(doer.TimedSubscription, Int)]()
			val startMilli = System.currentTimeMillis()
			var counter: Int = 0
			val task = doer.Task_schedules(FIXED_RATE, initialDelay, interval)((_, counter))
				.andThen { case (timedSub, supplierResult) =>
					// println(s"supplierResult = $supplierResult/$repetitions")
					if !doer.wasActivated(timedSub.schedule) then promise.tryFailure(new AssertionError("The `wasActivated` method returned false for a schedule that was activated"))
					if supplierResult == repetitions then {
						timedSub.unsubscribe()
						promise.trySuccess((timedSub, supplierResult))
					} else if supplierResult > repetitions then {
						promise.tryFailure(new AssertionError("The supplier was execute despite the schedule was canceled in the previous supplier's execution."))
					} else counter += 1
				}
			task.triggerAndForget()
			promise.future.map { case (timedSub, supplyResult) =>
				val actualDelay = System.currentTimeMillis() - startMilli
				val expectedDelay = interval * repetitions + initialDelay
				// println(s"counter = $counter/$repetitions, actualDelay = $actualDelay, expectedDelay = $expectedDelay, active = ${doer.isActive(schedule)}")
				assertEquals(supplyResult, repetitions)
				assert(actualDelay + 1 >= expectedDelay)
				assert(doer.isCanceled(timedSub.schedule))
			}
		}
	}

	//// Scheduling instance operations ////

	test("Scheduling Task: `task.scheduled(DELAY, delay, 0)` should preserve the original task's result and postpone its execution the specified `delay`") {
		val generators = getGenerators
		import generators.*

		PropF.forAllNoShrinkF(genSuccessfulTask[Int](), Gen.choose(1, 10)) { (task: Task[Int], testDelay: Int) =>
			(for {
				directResult <- task
				startTime = System.currentTimeMillis()
				delayedResult <- task.scheduled(DELAY, testDelay, 0)
			} yield {
				val actualDelay = System.currentTimeMillis() - startTime
				assertEquals(directResult, delayedResult)
				assert(actualDelay + 1 >= testDelay, s"Execution was not delayed enough. Expected at least ${testDelay}ms, got ${actualDelay}ms")
			}).toFuture()
		}
	}

	test("Scheduling Task: `task.scheduled(FIXED_DELAY, initialDelay, period)` should execute the `task` (up-chained operations) repeatedly according to the specified period until cancellation") {
		val generators = getGenerators
		import generators.*

		PropF.forAllNoShrinkF(
			Gen.choose(1, 10),
			Gen.choose(1, 5),
			genSuccessfulTask[Int]()
		) { (initialDelay: Int, interval: Int, task: Task[Int]) =>
			val repetitions = 5 - interval
			// println(s"\nBegin: initialDelay = $initialDelay, interval = $interval, repetitions = $repetitions")
			val testCompletion = doer.Captor[Unit]()
			var counter: Int = 0
			var maybeCheckSubscription: Maybe[Subscription] = Maybe.empty 
			val check = for {
				directResult <- task
				startMilli = System.currentTimeMillis()
				scheduledResult <- task.scheduled(FIXED_DELAY, initialDelay, interval)
			} yield {
				if scheduledResult != directResult then testCompletion.trap(new AssertionError(s"the scheduled result differs from the original"))
				val actualDelay = System.currentTimeMillis() - startMilli
				val expectedDelay = interval * counter + initialDelay
				if actualDelay + 1 < expectedDelay then testCompletion.trap(new AssertionError(s"Execution was not delayed enough. Expected at least ${expectedDelay}ms, got ${actualDelay}ms"))
				// println(s"period = $interval, counter = $counter/$repetitions, actualDelay = $actualDelay, expectedDelay = $expectedDelay, active = ${doer.isActive(schedule)}")
				if counter == repetitions then {
					testCompletion.capture(())
					maybeCheckSubscription.foreach(_.unsubscribe())
				} else counter += 1
			}
			maybeCheckSubscription = Maybe(check.subscribeAndForget())
			testCompletion.toFuture()
		}
	}

	test("Scheduling Task: `task.scheduled(DELAY, ...)` should be cancellable after the schedule was activated.") {
		val generators = getGenerators
		import generators.*

		PropF.forAllNoShrinkF(genSuccessfulTask[Int](), Gen.choose(1, 5)) { (task: Task[Int], delay: Int) =>
			// println(s"Begin: delay: $delay, task: $task")
			val promise = Promise[Unit]()
			given Promise[Unit] = promise

			var wasCanceled = false
			var hasCompleted = false
			var maybeSchedule: Maybe[doer.Schedule] = Maybe.empty
			val scheduledTask: doer.TimedTask[Int] = task.scheduled(DELAY, delay, 0).onSubscription { s =>
				if !doer.wasActivated(s) then break(s"The schedule should be activated after subscription")
				maybeSchedule = Maybe(s)
			}
			val subscription = scheduledTask.subscribe(false)(new MonoObserver[Int] {
				override def onSuccess(value: Int): Unit = {
					hasCompleted = true
					if wasCanceled then {
						break(s"The task completed (onSuccess) despite it was cancelled: isActive=${doer.wasActivated(maybeSchedule.get)}")
					}
				}

				override def onError(ex: Throwable): Unit = {
					hasCompleted = true
					if wasCanceled then {
						break(s"The task completed (onError) despite it was cancelled: isActive=${doer.wasActivated(maybeSchedule.get)}")
					}
				}
			})
			val cancelsAndWaits: Task[Unit] = for {
				_ <- Task_apply[Unit] { () =>
					if doer.isCanceled(maybeSchedule.get) && !hasCompleted then break("The schedule got canceled before canceling it")
					doer.cancel(maybeSchedule.get)
					wasCanceled = true
					if !doer.isCanceled(maybeSchedule.get) then break("The schedule remains not canceled after being canceled.")
				}
				_ <- doer.Task_sleeps(delay)

			} yield () // println("cancelsAndWaits completed successfully")
			cancelsAndWaits.trigger(false)(new MonoObserver[Unit] {
				override def onSuccess(value: Unit): Unit = promise.trySuccess(value)

				override def onError(ex: Throwable): Unit = promise.tryFailure(ex)
			})
			gate
		}
	}


	test("Scheduling Capturer: `capturer.delayed(delay)` should be cancellable before the schedule is activated.") {
		val generators = getGenerators
		import generators.*

		PropF.forAllNoShrinkF(genCapturer[Int](), Gen.choose(1, 5)) { (capturer: Capturer[Int], duration: Int) =>
			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			run {
				val delay: doer.Delay = doer.newDelaySchedule(duration)
				doer.cancel(delay)
				val scheduledCapturer = capturer.delayed(delay)
				scheduledCapturer.trigger()(new MonoObserver[Int] {
					override def onSuccess(value: Int): Unit = break(s"The task completed (onSuccess) despite it was cancelled: isActive=${doer.wasActivated(delay)}")

					override def onError(ex: Throwable): Unit = break(s"The task completed (onError) despite it was cancelled: isActive=${doer.wasActivated(delay)}")
				})
				if !doer.isCanceled(delay) then break("The schedule says it is not canceled despite it was.")
				doer.schedule(doer.newDelaySchedule(1))(_ => promise.trySuccess(()))
			}
			gate
		}
	}

	test("Scheduling Task: `task.onSubscription` should run side effects immediately on subscription") {
		val generators = getGenerators
		import generators.*

		PropF.forAllNoShrinkF(genSuccessfulTask[Int](), Gen.choose(1, 5)) { (task: Task[Int], delay: Int) =>
			val promise = Promise[doer.Schedule]()

			given Promise[doer.Schedule] = promise

			val timedTask = task.scheduled(DELAY, delay, 0).onSubscription { schedule =>
				promise.trySuccess(schedule)
			}

			val subscription = timedTask.subscribe(false)(new MonoObserver[Int] {
				override def onSuccess(value: Int): Unit = ()

				override def onError(ex: Throwable): Unit = ()
			})

			promise.future.map { schedule =>
				assert(schedule ne null)
			}
		}
	}

	//// Task factory methods ////

	test("Scheduling Task.scheduled: should compose correctly with other Task operations") {
		val generators = getGenerators
		import generators.*

		PropF.forAllNoShrinkF(genSuccessfulTask[Int](), smallIntGen, Gen.function1[Int, String](Gen.alphaLowerStr)) { (task: Task[Int], delay: Int, f: Int => String) =>
			//			def f(i: Int): String = i.toString.reverse

			val testDelay = Math.abs(delay % 5) + 1 // 1-5ms
			// println(s"Begin: testDelay = $testDelay")

			// Test composition with map
			val scheduledMapped: Task[String] = task.scheduled(DELAY, testDelay, 0).map(f)
			val mappedScheduled: Task[String] = task.map(f).scheduled(DELAY, testDelay, 0)

			// Test composition with flatMap
			val scheduledFlatMapped: Task[String] = task.scheduled(DELAY, testDelay, 0).flatMap(x => Task_ready(f(x)))
			val flatMappedScheduled: Task[String] = task.flatMap(x => Task_ready(f(x))).scheduled(DELAY, testDelay, 0)

			val checks =
				for {
					_ <- Task_combine(scheduledMapped, mappedScheduled) { (a, b) =>
						assert(a == b, "scheduled.map should equal map.scheduled")
					}
					_ <- Task_combine(scheduledFlatMapped, flatMappedScheduled) { (scheduledFlat, flatMapped) =>
						assert(scheduledFlat == flatMapped, "scheduled.flatMap should equal flatMap.scheduled")
					}
				} yield ()
			checks.toFuture()
		}
	}

	//// Doer wide cancellation ////

	test("Scheduling: when `doer.cancelAll()` is called within the thread currently assigned to `doer`, then no scheduled [[Runnable]]s should be executed, even if called near its scheduled time.") {
		val generators = getGenerators
		import generators.*
		val maxDuration = 5
		PropF.forAllNoShrinkF(
			Gen.nonEmptyListOf(for {
				schedule <- genSchedule(doer, maxDuration)
			} yield schedule),
			Gen.choose(1, maxDuration)
		) { (samples: List[doer.Schedule], cancelDelay: Int) =>
			// println(s"Begin: cancelDelay: $cancelDelay, samples: $samples")

			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			var cancelAllWasCalled = false
			// Activate all the sample schedules and check if their routine is executed after `cancelAll` was called.
			doer.run {
				for sample <- samples do {
					doer.schedule(sample) { s =>
						if cancelAllWasCalled then break(s"A schedule's routine was executed despite `cancelAll` was cancelled: schedule: $sample, isActive=${doer.wasActivated(sample)}")
					}
				}
			}

			// With another Doer instance, schedule the execution of `doer.cancelAll` within the doer and wait enough time for the routines be executed before considering the test as passed.
			val otherDoer = buildDoer("other")
			otherDoer.schedule(otherDoer.newDelaySchedule(cancelDelay)) { _ =>
				doer.run {
					doer.cancelAll()
					cancelAllWasCalled = true
					otherDoer.schedule(otherDoer.newDelaySchedule(maxDuration))(_ => promise.trySuccess(()))
				}
			}

			gate
		}
	}

	test("Scheduling: when `doer.cancelAll()` is called outside the thread currently assigned to `doer`, the scheduled [[Runnable]]s may be executed at most one time and only if called near its scheduled time.") {
		val generators = getGenerators
		import generators.*
		val maxDelay = 5
		PropF.forAllNoShrinkF(
			Gen.choose(1, maxDelay),
			Gen.nonEmptyListOf(Gen.choose(1, maxDelay)),
			Gen.oneOf(true, false)
		) { (cancelDelay: Int, delays: List[Int], useCpuSaturator) =>
			// println(s"Begin: cancelDelay: $cancelDelay, delays: $delays")

			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			@volatile var cancelAllWasCalled = false
			@volatile var cancelNanoTime: Long = 0
			@volatile var maxDistanceBetweenCancellationAndExecutionInNanos: Long = 0


			val saturationStopper = if useCpuSaturator then CpuSaturator.startSaturation() else new Runnable {
				override def run(): Unit = ()
			}

			for delayMillis <- delays do {
				val schedule = doer.newFixedRateSchedule(delayMillis, 1)
				var executionsAfterCancelAllCounter = 0
				val activationNanoTime: Long = System.nanoTime()
				doer.schedule(schedule) { s =>
					val actualExecutionNanoTime = System.nanoTime()
					if cancelAllWasCalled then {
						if executionsAfterCancelAllCounter > 0 then break(s"A schedule routine was executed more than once after `cancelAll` was called.")
						val distanceBetweenCancellationAndExecutionInNanos = actualExecutionNanoTime - cancelNanoTime
						if distanceBetweenCancellationAndExecutionInNanos > maxDistanceBetweenCancellationAndExecutionInNanos then maxDistanceBetweenCancellationAndExecutionInNanos = distanceBetweenCancellationAndExecutionInNanos
						// if cancelAll was called and either, a previous execution occurred or the distance between cancellation and expected execution is large enough, break the promise.
						if distanceBetweenCancellationAndExecutionInNanos > 500_000 then { // 500 micro
							val message = s"A schedule's routine was executed despite `cancelAll` was called: previousExecutionsCounter: $executionsAfterCancelAllCounter, distanceBetweenCancellationAndExecutionInMicros: ${distanceBetweenCancellationAndExecutionInNanos / 1_000}, delay: $delayMillis, cancelTime: $cancelNanoTime, schedule: $schedule, isActive=${doer.wasActivated(schedule)}"
							break(message)
						}
						executionsAfterCancelAllCounter += 1
					}
				}
			}

			// With another Doer instance, schedule the execution of `doer.cancelAll` outside the doer and wait enough time for the routines be executed before considering the test as passed.
			val otherDoer = buildDoer("other")
			otherDoer.schedule(otherDoer.newDelaySchedule(cancelDelay)) { cancelSchedule =>
				doer.cancelAll()
				cancelNanoTime = System.nanoTime()
				cancelAllWasCalled = true
				otherDoer.schedule(otherDoer.newDelaySchedule(maxDelay)) { checkSchedule =>
					promise.trySuccess(())
					// if maxDistanceBetweenCancellationAndExecutionInNanos == 0 then println(s"No executions after cancellation: VERY GOOD, is CPU saturated=$useCpuSaturator")
					// else println(s"is CPU saturated=$useCpuSaturator, maxDistanceBetweenCancellationAndExecutionInMicros = ${maxDistanceBetweenCancellationAndExecutionInNanos / 1_000}")
				}
			}

			saturationStopper.run()
			gate
		}
	}

	//// Task_schedules factory method

	test("Task_schedules: The task returned by `Task_schedules(newDelaySchedule(delay))(body)` should execute `body` and yield its result once after the delay.") {
		// Test with a schedule that only executes once (e.g., single delay)
		// Verify supplier is called exactly once and task yields the result

		val generators = getGenerators
		import generators.*
		PropF.forAllNoShrinkF(
			Gen.choose(1, 5)
		) { (expectedDelay: Int) =>
			// println(s"Begin: upChain: $upChain, delays: $delays")

			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			val latch = new CountDownLatch(2)

			val startTime = System.nanoTime()
			val task = doer.Task_schedules(DELAY, expectedDelay, 0) { s =>
				val actualDelay = System.nanoTime() - startTime
				if actualDelay < expectedDelay * 1_000_000 then break("The execution occurred sooner than expected")
				else latch.countDown()
			}
			task.triggerAndForget()
			if latch.await(expectedDelay * 2 + 5, TimeUnit.MILLISECONDS) then break("The routine was executed more than one time")
			else promise.trySuccess(())
			gate
		}
	}

	test("Task_schedules: The task returned by `Task_schedules(newFixedRateSchedule(initialDelay, interval))(body)` should execute `body` and yield its result repeatedly after the instants determined by the schedule.") {
		val generators = getGenerators
		val REPETITIONS = 4
		var testExecutionsCounter = 0
		import generators.*
		PropF.forAllNoShrinkF(
			Gen.choose(-1, 10),
			Gen.choose(1, 5)
		) { (expectedInitialDelay: Int, expectedPeriod: Int) =>
			// scribe.debug(s"Begin: $expectedInitialDelay, $expectedPeriod")

			val EXECUTION_DELAY_MARGIN_MILLIS = if testExecutionsCounter < 5 then 100 else 50
			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			val latch = new CountDownLatch(REPETITIONS)

			var executionsCounter = 0
			var maybeSchedule: Maybe[doer.Schedule] = Maybe.empty
			val startTime = System.nanoTime()
			val task = doer.Task_schedules(FIXED_RATE, expectedInitialDelay, expectedPeriod) { s =>
				maybeSchedule = Maybe(s.schedule)
				val actualDurationNanos = System.nanoTime() - startTime
				val expectedDurationMillis = expectedInitialDelay + executionsCounter * expectedPeriod
				val differenceMicros = actualDurationNanos / 1000 - expectedDurationMillis * 1000
				// scribe.debug(f"difference: $differenceMicros%6d actual: ${actualDurationNanos/1000}%6d, expected: ${expectedDurationMillis*1000}%6d")
				if differenceMicros < 0 then break(s"The #$executionsCounter execution occurred sooner than expected")
				else if differenceMicros > EXECUTION_DELAY_MARGIN_MILLIS * 1_000 then break(s"The #$executionsCounter execution occurred later than expected after $testExecutionsCounter successful tests")
				else latch.countDown()
				executionsCounter += 1
			}
			task.triggerAndForget()
			if latch.await(expectedInitialDelay + expectedPeriod * REPETITIONS + EXECUTION_DELAY_MARGIN_MILLIS, TimeUnit.MILLISECONDS) then promise.trySuccess(())
			else break(s"The number of executions ($executionsCounter) within the provided time is less than the expected")
			doer.cancel(maybeSchedule.get)
			testExecutionsCounter += 1
			gate
		}
	}
}
