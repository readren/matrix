package readren.sequencer

import GeneratorsForDoerTests.{*, given}

import munit.ScalaCheckEffectSuite
import org.scalacheck.Test.Parameters
import org.scalacheck.effect.PropF
import org.scalacheck.{Arbitrary, Gen, Prop}
import readren.common.{Maybe, ScribeConfig}
import readren.sequencer.{CausalFence, CausalStuckableFence}

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
	@volatile private var reportedFailuresObserver: Null | ((Doer, Throwable) => Unit) = null

	private var sharedDoerProvider: DP = uninitialized
	private var sharedDoer: D = uninitialized
	private var sharedGenerators: GeneratorsForDoerTests[D] = uninitialized

	@volatile private var observingSession: Int = 0

	/** Executions that start more than this number or nanos after [[SchedulingExtension.cancelAll]] was called outside the [[Doer]]'s thread will fail the test. */
	protected val schedulerMaximumToleratedNanosBetweenCancellationAndExecution: Long

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
			val trace = new Exception(exception)
			scribe.error(s"TEST FAILED - DO NOT IGNORE: `onUnhandledException` was called outside the provided doer's thread.", trace)
		}
	}

	/**
	 * This method should be invoked by the [[DoerProvider]] instances returned by [[buildDoerProvider]] whenever their [[DoerProvider.onFailureReported]] callback is triggered.
	 * The extending class is responsible for ensuring this linkage.
	 */
	protected def onFailureReported(doer: Doer, failure: Throwable): Unit = {
		if doer.isInSequence then {
			if reportedFailuresObserver ne null then reportedFailuresObserver(doer, failure)
			// scribe.debug(s"#$observingSession: failure reported at #$observingSession: ${failure.getMessage}")
		} else {
			val trace = new Exception(failure)
			scribe.error(s"TEST FAILED - DO NOT IGNORE: `onFailureReported` was called outside the provided doer's thread.", trace)
		}
	}


	//// Suite lifecycle ////

	override val munitTimeout: Duration = scala.concurrent.duration.Duration(240, "seconds")

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

	/** Breaks the `promise` if it wasn't already completed. */
	protected def break[P](message: String)(using promise: Promise[P]): Unit =
		promise.tryFailure(new AssertionError(message))

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
		promise.future.andThen(_ => latch.countDown())
		latch.await(duration, TimeUnit.MILLISECONDS)
		break(message)
		gate
	}

	/** Executes the provided `supplier` observing the calls to the [[onUnhandledException]] and [[onFailureReported]] methods during its execution. */
	protected def observingUnhandledAndReportedExceptionsDo[R, P](supplier: () => R)(onUnhandledException: (Doer, Throwable) => Unit)(onFailureReported: (Doer, Throwable) => Unit)(using promise: Promise[P]): R = {
		if (unhandledExceptionObserver ne null) || (reportedFailuresObserver ne null) then break("Nesting `observingUnhandledAndReportedExceptionsDo` is not supported")
		observingSession += 1
		unhandledExceptionObserver = onUnhandledException
		reportedFailuresObserver = onFailureReported
		// scribe.debug(s"Session #$observingSession opened")
		val r = supplier()
		// scribe.debug(s"Session #$observingSession closed")
		reportedFailuresObserver = null
		unhandledExceptionObserver = null
		r
	}

	//// UNDER DEVELOPMENT

	test("joker") {
		// Test with fixed-delay schedule
		// Verify supplier is called multiple times with delay between completions
		// Verify each result is yielded
		val generators = getGenerators
		import generators.*

		true
	}

	/**
	 * Test invariants of [[Doer.CausalFence]] ensuring that synchronous consumers of the [[Doer.LatchingTask]] returned by [[Doer.CausalFence.advance]] observe the up‑to‑date state deterministically.
	 *
	 * Unique checks in this test:
	 *  - Consumers subscribed immediately (synchronously) to the [[Doer.LatchingTask]] returned by [[Doer.CausalFence.advance]] must be executed strictly in order of subscription, before any other consumer, and even before the updaters passed to subsequent calls to [[advance]].
	 *
	 *  - A consumer subscribed immediately (synchronously) to the [[Doer.LatchingTask]] returned by [[causalAnchor]] must observe either the state to which the last advance transitioned to, or a state produced earlier, but never an later one.
	 *
	 *  - Game‑changing invariant: Immediately after an [[Doer.CausalFence.advance]] call, there are no other advances in flight except the one just created. The returned [[Doer.Covenant]] (seen as [[Doer.LatchingTask]]) is the new tail, and any immediate synchronous subscription to it is guaranteed to be the first subscriber in its list. Therefore, when the Covenant fulfills, that consumer sees the up‑to‑date state deterministically, free of concurrent updates to the primary state.
	 *
	 * The test constructs multiple paths that repeatedly advance the fence up to a top serial number, failing if any consumer observes stale state, incorrect ordering, or out‑of‑sequence execution.
	 * // TODO removing delay causes stack overflow. Look for a solution for this test, and consider a solution at the library level. See note in [[Doer.Covenant.fulfillUnsafe]].
	 */
	test("CausalFence - synchronous consumer ordering and anchor freshness: synchronous consumers see up‑to‑date state deterministically - using hoping duties (much faster than the version that uses random delays)") {
		val generators = getGenerators
		import generators.*

		def buildTask(serial: Int, hops: Int): Task[Int] = {
			if hops <= 0 then Task_ready(serial)
			else Covenant[Int]().fulfillWith(buildTask(serial, hops - 1), false)
		}

		type PrimaryState = (pathId: Int, serial: Int)
		val initialState: PrimaryState = (0, 0)
		PropF.forAllF(
			for {
				swarmSizeMinusOne <- Gen.choose(1, 9)
				// The head and tail of hopsList are generated separately to ensure the list is non-empty even when scalacheck is shrinking the sample.
				hopsHead <- Gen.choose(0, 9)
				hopsTail <- Gen.listOfN(99, Gen.choose(0, 9))
			} yield (swarmSizeMinusOne, hopsHead, hopsTail)
		) { (swarmSizeMinusOne: Int, hopsHead: Int, hopsTail: List[Int]) =>
			val hopsList = hopsHead :: hopsTail
			val topSerial = hopsList.size
			val swarmSize = Math.min(1 + swarmSizeMinusOne, topSerial)

			println(s"Begin: swarmSize=$swarmSize, topSerial=$topSerial, hopsList=$hopsList")

			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			val fence = CausalFence[PrimaryState, doer.type](doer)(initialState)
			var derivedSerial: Int = 0
			var advanceCallSerial = 0

			def path(pathId: Int): LatchingTask[PrimaryState] = {
				val advanceName = s"advance$advanceCallSerial${advanceCallSerial + 1}"
				advanceCallSerial += 1
				for {
					nextState <- {
						fence.advanceIf { (previous: PrimaryState) =>
							if previous.serial >= topSerial then Maybe.empty
							else {
								val commitedAtStart = fence.committedState
								val task = buildTask(previous.serial + 1, hopsList(previous.serial))
									.map(newSerial => (pathId, newSerial))
									.andThen { nextState =>
										if commitedAtStart.serial != fence.committedState.serial then break(s"In the interval between the updater passed to `advance` is called and the Task it returns completes, no other updater is started; and that is not happening.")
									}
								Maybe.some(task)
							}
						}
					}
					anchoredState <- {
						val committedState = fence.committedState
						if nextState.pathId != pathId && nextState.serial < topSerial then break(s"A consumer subscribed to the LatchingTask returned by `advance` should see the state to which the advance transitioned to; and is not happening: pathId=$pathId, actual: ${nextState.pathId}")
						else if derivedSerial > nextState.serial then break(s"Consumers subscribed immediately (in a synchronously coupled manner) to the `LatchingTask` returned by `advance`, should be executed in order of subscription before any other consumer, even before the updaters passed to subsequent calls to advance; and is not happening.")
						else if nextState.serial != fence.committedState.serial then break(s"A consumer subscribed immediately (in a synchronously coupled manner) to the LatchingTask returned by `advance` should see the up-to-date state; and is not happening: current=$nextState, commited=$committedState")
						else derivedSerial = nextState.serial
						fence.causalAnchor()
					}
					recursiveState <- {
						if anchoredState.serial != fence.committedState.serial then break(s"A consumer subscribed immediately (in a synchronously coupled manner) to the `LatchingTask` returned by `causalAnchor` should see the the up-to-date state; and is not happening: current=$anchoredState, commited=${fence.committedState}")
						if nextState.serial < topSerial then path(pathId)
						else fence.committed
					}
				} yield recursiveState
			}

			val swarm: Seq[Task[PrimaryState]] = Seq.tabulate(swarmSize) { n => doer.Task_mineFlat(() => path(n)) }
			val checks = for array <- doer.Task_sequenceToArray(swarm) yield promise.trySuccess(())
			checks.triggerAndForget()

			gate
		}

	}


	/**
	 * Test invariants of [[CausalFence]] ensuring that synchronous consumers of the [[Doer.LatchingTask]] returned by [[Doer.CausalFence.advance]] observe the up‑to‑date state deterministically.
	 *
	 * Unique checks in this test:
	 *  - Consumers subscribed immediately (synchronously) to the [[Doer.LatchingTask]] returned by [[Doer.CausalFence.advance]] must be executed strictly in order of subscription, before any other consumer, and even before the updaters passed to subsequent calls to [[advance]].
	 *
	 *  - A consumer subscribed immediately (synchronously) to the [[Doer.LatchingTask]] returned by [[Doer.CausalFence.causalAnchor]] must observe either the state to which the last advance transitioned to, or a state produced earlier, but never an later one.
	 *
	 *  - Game‑changing invariant: Immediately after an [[advance]] call, there are no other advances in flight except the one just created. The returned [[Doer.Covenant]] (seen as [[Doer.LatchingTask]]) is the new tail, and any immediate synchronous subscription to it is guaranteed to be the first subscriber in its list. Therefore, when the Covenant fulfills, that consumer sees the up‑to‑date state deterministically, free of concurrent updates to the primary state.
	 *
	 * The test constructs multiple paths that repeatedly advance the fence up to a top serial number, failing if any consumer observes stale state, incorrect ordering, or out‑of‑sequence execution.
	 * // TODO removing delay causes stack overflow. Look for a solution for this test, and consider a solution at the library level. See note in [[Doer.Covenant.fulfillUnsafe]].
	 */
	test("CausalFence - synchronous consumer ordering and anchor freshness: synchronous consumers see up‑to‑date state deterministically - using random delays (very slow)") {
		val generators = getGenerators
		import generators.*

		type PrimaryState = (pathId: Int, serial: Int)
		val initialState: PrimaryState = (0, 0)
		val topSerial = 99
		PropF.forAllF(Gen.choose(1, 9)) { (swarmSize: Int) =>
			println(s"Begin: swarmSize=$swarmSize")

			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			val fence = CausalFence[PrimaryState, doer.type](doer)(initialState)
			var derivedSerial: Int = 0

			def path(pathId: Int): LatchingTask[PrimaryState] = {
				for {
					nextState <- {
						fence.advance { (previous: PrimaryState) =>
							val commitedAtStart = fence.committedState
							val taskGenerator: Gen[Task[Int]] = genTask(previous.serial + 1)
							val randomTask: Task[Int] = taskGenerator.sample.get
							val delay = Gen.choose(-1, 1).sample.get
							val task = if delay > 0 then randomTask.delayed(delay) else randomTask
							task.map(newSerial => (pathId, newSerial))
								.andThen { nextState =>
									if commitedAtStart.serial != fence.committedState.serial then break(s"In the interval between the updater passed to `advance` is called and the Task it returns completes, no other updater is started; and that is not happening.")
								}
						}
					}
					anchoredState <- {
						if nextState.pathId != pathId then break(s"A consumer subscribed to the LatchingTask returned by `advance` should see the state to which the advance transitioned to; and is not happening: $pathId, actual: ${nextState.pathId}")
						else if derivedSerial > nextState.serial then break(s"Consumers subscribed immediately (in a synchronously coupled manner) to the `LatchingTask` returned by `advance`, should be executed in order of subscription before any other consumer, even before the updaters passed to subsequent calls to advance; and is not happening.")
						else if nextState.serial != fence.committedState.serial then break(s"A consumer subscribed immediately (in a synchronously coupled manner) to the LatchingTask returned by `advance` should see the up-to-date state; and is not happening: current=$nextState, commited=${fence.committedState}")
						else derivedSerial = nextState.serial
						fence.causalAnchor()
					}
					followingState <- {
						if anchoredState.serial != fence.committedState.serial then break(s"A consumer subscribed immediately (in a synchronously coupled manner) to the `LatchingTask` returned by `causalAnchor` should see the the up-to-date state; and is not happening: current=$anchoredState, commited=${fence.committedState}")
						if nextState.serial <= topSerial then path(pathId)
						else fence.committed
					}
				} yield {
					if followingState.serial != fence.committedState.serial then break(s"followingState=$followingState, commited=${fence.committedState}")
					followingState
				}
			}

			val swarm: Seq[Task[PrimaryState]] = Seq.tabulate(swarmSize) { n => Task_mineFlat(() => path(n)) }
			val checks = for array <- doer.Task_sequenceToArray(swarm) yield promise.trySuccess(())
			checks.triggerAndForget()
			gate
		}
	}

	////////// DOER INFRASTRUCTURE ////////

	test("`Doer.execute` executes in a decoupled manner.") {
		val generators = getGenerators
		import generators.*

		val promise = Promise[Unit]()

		given Promise[Unit] = promise

		var mutable = 1

		val task = doer.Task_mine { () =>
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

		// Submit a venture that throws an exception
		doer.executeSequentially { () =>
			throw new RuntimeException("Test exception")
		}

		// Submit a venture that should still execute after the exception
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

	test("Doer should call onFailureReported when the operand passed to `Venture.andThen` fails") {
		val mainDoer = getSharedDoer

		PropF.forAllF { (throwable: Throwable) =>
			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			observingUnhandledAndReportedExceptionsDo { () =>
				// Submit a venture that uses Venture.andThen which will cause a failure report
				val venture = mainDoer.Venture_successful(0).andThen(_ => throw throwable)
				venture.trigger() { _ =>
					if NonFatal(throwable) then break(s"The failure report should be done before the venture that produced it completes.")
					else break("The operation completed despite the operand thew a fatal exception")
				}

				breakAfterWaiting(9, "No notification of the exception until 9 milliseconds after applying the operation. Waiting aborted.")
			} { (doer, exception) =>
					if NonFatal(exception) then break(s"A non fatal exception was uncaught despite it should: $exception")
					else promise.trySuccess(())
			} { (doer, failure) =>
				if failure.getCause ne throwable then break(s"An unexpected failure was reported: $failure")
				else if doer ne mainDoer then break(s"An unexpected doer was associated to the failure report: ${doer.tag}")
				else promise.trySuccess(())
			}
		}
	}

	test("Doer should call onUnhandledException when venture throws uncaught exception") {
		val mainDoer = getSharedDoer

		PropF.forAllF { (exception: Throwable) =>
			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			observingUnhandledAndReportedExceptionsDo { () =>
				// Submit a venture that throws an uncaught exception
				mainDoer.executeSequentially { () =>
					throw exception
				}
				breakAfterWaiting(999, "No notification of the exception until 999 milliseconds after applying the operation. Waiting aborted.")

			} { (doer, e) =>
				if e ne exception then break(s"The thrown exception should be captured: $e")
				else if doer ne mainDoer then break(s"Correct doer should be captured: ${doer.tag}")
				else promise.trySuccess(())
			} { (doer, failure) =>
				break(s"Unexpected failure report: $failure")
			}
		}
	}

	test("The `DoerProvider` should notify uncaught exceptions thrown by the Runnable passed to `Doer.executeSequentially` before executing the next enqueued Runnable") {
		val mainDoer = getSharedDoer

		PropF.forAllNoShrinkF { (exception: Throwable) =>

			val promise = Promise[Unit]()
			var wasCaught = false

			given Promise[Unit] = promise

			observingUnhandledAndReportedExceptionsDo { () =>
				mainDoer.executeSequentially(() => throw exception)
				mainDoer.executeSequentially { () =>
					if wasCaught then promise.trySuccess(()) else break("The uncaught exception was not notified")
				}

				breakAfterWaiting(999, s"No notification of the exception $exception until 990 milliseconds after applying the operation. Waiting aborted.")

			} { (d, t) =>
				if t eq exception then wasCaught = true else break(s"an unexpected exception was uncaught $t")
			} { (d, t) =>
				break(s"an unexpected exception was reported")
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
	private def checkEquality[A](doer: Doer)(task1: doer.Task[A], task2: doer.Task[A], clue: => Any = "duties yield different results"): Future[Unit] = {
		// println(s"Begin: task1=$task1, task2=$task2")
		for {
			a1 <- task1.toFutureHardy()
			a2 <- task2.toFutureHardy()
		} yield {
			// println(s"$try1 ==== $try2")
			assertEquals(a1, a2, clue)
		}
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

	test("Task: any pair of duties can be combined") {
		val generators = getGenerators
		import generators.*

		PropF.forAllF { (taskA: Task[Int], taskB: Task[Int], f: (Int, Int) => Int) =>
			val combinedTask = Task_combine(taskA, taskB)(f)

			for {
				combinedResult <- combinedTask.toFutureHardy()
				taskAResult <- taskA.toFutureHardy()
				taskBResult <- taskB.toFutureHardy()
			} yield {
				assert(combinedResult == f(taskAResult, taskBResult))
			}
		}
	}

	test("Task: `doer.Task.foreign(foreignDoer)(foreignTask)` should complete in the `doer`'s thread") {
		val generators = getGenerators
		import generators.*
		PropF.forAllNoShrinkF {
			for {
				taskResult <- intGen
				foreignTask <- foreignDoerGenerators(true).genTask(taskResult)
			} yield (taskResult, foreignTask)
		} { case (taskResult, foreignTask) =>
			// println(s"Begin: foreignTask: $foreignTask")

			doer.Task_foreign(foreignDoer)(foreignTask)
				.map { int => int == taskResult && doer.isInSequence && !foreignDoer.isInSequence }
				.succeed
				.map(assert(_))
				.toFuture()
		}
	}

	test("`Task.subscribe` should not catch exceptions thrown by `onComplete`") {
		val generators = getGenerators
		import generators.*

		PropF.forAllNoShrinkF { (task: Task[Int], exception: Throwable, randomInt: Int) =>
			val smallNonNegativeInt = math.abs(randomInt % 10)
			// scribe.debug(s"Begin: task=$task, exception=${exception.getMessage}, randomInt=$randomInt, smallNonNegativeInt=$smallNonNegativeInt")

			/** Do the test for a single operation */
			def check[R](opName: String, operatedTask: Task[R]): Future[Unit] = {
				// scribe.debug(s"checking operation: $opName")
				// Apply the operation to the random task and trigger the execution passing a faulty on-complete callback.
				val promise = Promise[Unit]()

				given Promise[Unit] = promise

				observingUnhandledAndReportedExceptionsDo { () =>
					operatedTask.trigger() { r =>
						// scribe.debug(s"#$observingSession: about to throw the exception --- $isInSequence")
						Thread.sleep(1)
						throw exception
						//promise.trySuccess(null)
					}

					breakAfterWaiting(999, s"$opName: No notification of the exception until 999 milliseconds after applying the operation. Waiting aborted.")

				} { (d, t) =>
					if (d eq doer) && (t eq exception) then promise.trySuccess(()) else break(s"$opName: An unexpected exception was throw: $t")
				} { (d, t) =>
					/* if (d eq doer) && ((t eq exception) || (t.getCause eq exception)) then */ break(s"$opName: An exception was caught and reported despite the operation should not catch nor report them")
				}(using promise)
			}

			for {
				_ <- check("factory", task)
				_ <- check("map", task.map(identity))
				_ <- check("flatMap", task.flatMap(_ => task))
				_ <- check("andThen", task.andThen(_ => ()))
				_ <- check("toVenture", task.succeed)
				_ <- check("repeatedUntilSome", task.repeatedUntilSome { (n, i) => if n > smallNonNegativeInt then Maybe(randomInt) else Maybe.empty })
				_ <- check("repeatedUntilDefined", task.repeatedUntilDefined { case (n, tryInt) if n > smallNonNegativeInt => tryInt })
				_ <- check("repeatedWhileNone", task.repeatedWhileEmpty(Success(0), (n, tryInt) => if n > smallNonNegativeInt then Maybe(randomInt) else Maybe.empty))
				_ <- check("repeatedWhileUndefined", task.repeatedWhileUndefined(Success(0), { case (n, tryInt) if n > smallNonNegativeInt => randomInt }))
			} yield ()
		}
	}

	////////// Venture /////////////

	// Custom equality for Venture based on the result of attempt
	private def checkEquality[A](doer: Doer)(venture1: doer.Venture[A], venture2: doer.Venture[A]): Future[Unit] = {
		val futureEquality = for {
			try1 <- venture1.toFutureHardy()
			try2 <- venture2.toFutureHardy()
		} yield {
			// println(s"$try1 ==== $try2")
			try1 ==== try2
		}
		futureEquality.map(assert(_))
	}

	// Monadic left identity law: Venture.successful(x).flatMap(f) == f(x)
	test("Venture: left identity") {
		val generators = getGenerators
		import generators.*

		PropF.forAllF { (x: Int, f: Int => Venture[Int]) =>
			val sx = Venture_successful(x)
			val left = Venture_successful(x).flatMap(f)
			val right = f(x)
			checkEquality(doer)(left, right)
		}
	}

	// Monadic right identity law: m.flatMap(Venture.successful) == m
	test("Venture: right identity") {
		val generators = getGenerators
		import generators.*

		PropF.forAllF { (m: Venture[Int]) =>
			val left = m.flatMap(Venture_successful)
			val right = m
			checkEquality(doer)(left, right)
		}
	}

	// Monadic associativity law: m.flatMap(f).flatMap(g) == m.flatMap(x => f(x).flatMap(g))
	test("Venture: associativity") {
		val generators = getGenerators
		import generators.*

		PropF.forAllF { (m: Venture[Int], f: Int => Venture[Int], g: Int => Venture[Int]) =>
			val leftAssoc = m.flatMap(f).flatMap(g)
			val rightAssoc = m.flatMap(x => f(x).flatMap(g))
			checkEquality(doer)(leftAssoc, rightAssoc)
		}
	}

	// Functor: `m.map(f) == m.flatMap(a => unit(f(a)))`
	test("Venture: can be transformed with map") {
		val generators = getGenerators
		import generators.*

		PropF.forAllF { (m: Venture[Int], f: Int => String) =>
			val left = m.map(f)
			val right = m.flatMap(a => Venture_successful(f(a)))
			checkEquality(doer)(left, right)
		}
	}

	// Recovery: `failedVenture.recover(f) == if f.isDefinedAt(e) then successful(f(e)) else failed(e)` where e is the exception thrown by failedVenture
	test("Venture: can be recovered from failure") {
		val generators = getGenerators
		import generators.*

		PropF.forAllF { (e: Throwable, f: PartialFunction[Throwable, Int]) =>
			if NonFatal(e) then {
				val leftVenture = Venture_failed[Int](e).recover(f)
				val rightVenture = if f.isDefinedAt(e) then Venture_successful(f(e)) else Venture_failed(e)
				checkEquality(doer)(leftVenture, rightVenture)
			} else Future.successful(())
		}
	}

	test("Venture: any can be combined") {
		val generators = getGenerators
		import generators.*

		PropF.forAllF { (ventureA: Venture[Int], ventureB: Venture[Int], f: (Try[Int], Try[Int]) => Try[Int]) =>
			val combinedVenture = Venture_combine(ventureA, ventureB)(f)

			for {
				combinedResult <- combinedVenture.toFutureHardy()
				ventureAResult <- ventureA.toFutureHardy()
				ventureBResult <- ventureB.toFutureHardy()
			} yield {
				assert(combinedResult ==== f(ventureAResult, ventureBResult))
			}
		}
	}

	test("Venture: `doer.Venture_foreign(foreignDoer)(foreignVenture)` should complete in the `doer`'s thread") {
		val generators = getGenerators
		import generators.*
		PropF.forAllNoShrinkF {
			for {
				expectedVentureResult <- intGen
				foreignVenture <- foreignDoerGenerators(true).genVenture(expectedVentureResult, s"foreignVenture.arbitrary")
			} yield (expectedVentureResult, foreignVenture)
		} { case (ventureResult, foreignVenture) =>
			// println(s"Begin: taskResult: $taskResult, foreignVenture: $foreignVenture")

			doer.Venture_foreign(foreignDoer)(foreignVenture)
				.transform { tryInt =>
					assert(tryInt.fold[Boolean](_.getMessage.contains(ventureResult.toString), _ == ventureResult))
					assert(doer.isInSequence)
					assert(!foreignDoer.isInSequence)
					Success(())
				}.toFuture()
		}
	}

	test("Venture: if a function operand passed to a Venture's operation throws an exception then, if the exception isn't fatal, the venture should complete with a [[Failure]] containing that exception; and if it is fatal, the venture should not complete and instead the `DoerProvider.onUnhandledException` method should be called passing the exception.") {
		val generators = getGenerators
		import generators.*

		PropF.forAllNoShrinkF(
			for {i <- intGen; venture <- genVenture(i, "")} yield venture,
			throwableArbitrary.arbitrary
		) { case (anyVenture: Venture[Int], exception: Throwable) =>
			// println(s"Begin: anyVenture: $anyVenture, exception: $exception")

			/** Do the test for a single operation */
			def check[R](opName: String, operatedVenture: Venture[R], shouldCatchAndReportNonFatalExceptions: Boolean = false): Future[Unit] = {
				// Apply the operation to the random task and trigger the execution passing a faulty on-complete callback.
				val promise = Promise[Unit]()

				given Promise[Unit] = promise

				observingUnhandledAndReportedExceptionsDo { () =>
					// Apply the operation to the random venture.
					operatedVenture.trigger() { operationResult =>
						// If the venture completed then the result should be a Failure containing the exception, and the exception should be non-fatal.
						if !NonFatal(exception) then break(s"$opName: Completed despite a fatal exception was thrown")
						else if operationResult.fold(e => (e ne exception) && (e.getCause ne exception), _ => true) then break(s"$opName: Completed with an unexpected result: $operationResult")
						else promise.trySuccess(())
					}

					breakAfterWaiting(999, s"$opName: No notification of the exception until 999 milliseconds after applying the operation. Waiting aborted.")

				} { (d, t) =>
					// For the exception to be uncaught it should be fatal.
					if t ne exception then break(s"$opName: An unexpected exception was uncaught.")
					else if NonFatal(exception) then break(s"$opName: An exception was not handled despite it is non-fatal")
					else promise.trySuccess(())

				} { (d, t) =>
					// For the exception to be reported, it should be fatal and the operation of the kind that catches and report them.
					if (t ne exception) && (t.getCause ne exception) then break(s"$opName: An unexpected exception was caught and reported.")
					else if !NonFatal(exception) then break(s"$opName: An exception was reported despite it is fatal")
					else if shouldCatchAndReportNonFatalExceptions then promise.trySuccess(())
					else break(s"$opName: A fatal exception was caught and reported despite this operation should not catch nor report them.")
				}(using promise)
			}


			val successfulVenture = anyVenture.recover { case cause => exception.getMessage.hashCode }
			val failingVenture = anyVenture.map { x => throw new FaultyValue(x, "for recover") }

			def f0[A](): A = throw exception

			def f1[A, B](a: A): B = throw exception

			def f2[A, B, C](a: A, b: B): C = throw exception

			for {
				_ <- check("own", Venture_own(f0))
				_ <- check("ownFlat", Venture_ownFlat(f0))
				_ <- check("foreign", Venture_foreign(foreignDoer)(foreignDoer.Venture_own(f0)))
				_ <- check("alien", Venture_alien(f0))
				_ <- check("combine", Venture_combine(anyVenture, anyVenture)(f2))
				_ <- check("map", successfulVenture.map(f1))
				_ <- check("andThen", anyVenture.andThen(f1), true)
				_ <- check("flatMap", successfulVenture.flatMap(f1))
				_ <- check("withFilter", successfulVenture.withFilter(f1))
				_ <- check("transform", anyVenture.transform(f1))
				_ <- check("transformWith", anyVenture.transformWith(f1))
				_ <- check("recover", failingVenture.recover { case x => f1(x) }) // the `map` is to ensure that the upstream venture completes abruptly to avoid the tested operation be skipped.
				_ <- check("recoverWith", failingVenture.recoverWith { case x => f1(x) })
				_ <- check("reiteratedHardyUntilSome", anyVenture.reiteratedHardyUntilSome(f2))
				_ <- check("reiteratedUntilSome", successfulVenture.reiteratedUntilSome(f2))
				_ <- check("reiteratedUntilDefined", anyVenture.reiteratedHardyUntilDefined { case (a, b) => f2(a, b) })
				_ <- check("reiteratedWhileNone", anyVenture.reiteratedWhileEmpty(Success(0), f2))
				_ <- check("reiteratedWhileUndefined", anyVenture.reiteratedWhileUndefined(Success(0), { case (a, b) => f2(a, b) }))
			} yield ()
		}
	}

	test("`Venture.subscribe` should not catch exceptions thrown by the `onComplete` operand") {
		val generators = getGenerators
		import generators.*

		PropF.forAllF { (venture1: Venture[Int], venture2: Venture[Int], exception: Throwable, future: Future[Int]) =>


			def check[R](opName: String, operatedVenture: Venture[R]): Future[Unit] = {
				val promise = Promise[Unit]()

				given Promise[Unit] = promise

				observingUnhandledAndReportedExceptionsDo { () =>
					// Trigger the execution passing a faulty on-complete callback.
					operatedVenture.trigger()(tryR => throw exception)

					breakAfterWaiting(999, s"$opName: No notification of the exception until 999 milliseconds after applying the operation. Waiting aborted.")

				} { (d, t) =>
					// For the exception to be unhandled, it should be fatal and the operation of the kind that does not handle them.
					if (d eq doer) && (t eq exception) then promise.trySuccess(())
				} { (d, t) =>
					// For the exception to be reported, it should be fatal and the operation of the kind that catches and report them.
					if (d eq doer) && ((t eq exception) || (t.getCause eq exception)) then break(s"$opName: A fatal exception was caught and reported despite $opName should not catch nor report them.")

				}(using promise)
			}

			val randomInt = exception.getMessage.hashCode()
			val smallNonNegativeInt = randomInt % 9
			val randomBool = (randomInt % 2) == 0
			val randomTryInt = if randomBool then Success(randomInt) else Failure(exception)
			// println(s"Begin: venture=$task, exception=$exception, randomInt=$randomInt, randomBool=$randomBool")

			for {
				_ <- check("factory", venture1)
				_ <- check("ownFlat", Venture_ownFlat(() => venture1))
				_ <- check("foreign", Venture_foreign(foreignDoer)(foreignDoer.Venture_mine(() => randomInt)))
				_ <- check("alien", Venture_alien(() => future))
				_ <- check("map", venture1.map(identity))
				_ <- check("flatMap", venture1.flatMap(_ => venture2))
				_ <- check("withFilter", venture1.withFilter(_ => randomBool))
				_ <- check("andThen", venture1.andThen(_ => ()))
				_ <- check("transform", venture1.transform(identity))
				_ <- check("transformWith", venture1.transformWith(_ => venture2))
				_ <- check("recover", venture1.recover { case x if randomBool => randomInt })
				_ <- check("recoverWith", venture1.recoverWith { case x if randomBool => venture2 })
				_ <- check("reiteratedHardyUntilSome", venture1.reiteratedHardyUntilSome { (n, tryInt) => if n > smallNonNegativeInt then Maybe(randomTryInt) else Maybe.empty })
				_ <- check("reiteratedUntilSome", venture1.reiteratedUntilSome { (n, i) => if n > smallNonNegativeInt then Maybe(randomTryInt) else Maybe.empty })
				_ <- check("reiteratedHardyUntilDefined", venture1.reiteratedHardyUntilDefined { case (n, tryInt) if n > smallNonNegativeInt => tryInt })
				_ <- check("reiteratedWhileEmpty", venture1.reiteratedWhileEmpty(Success(0), (n, tryInt) => if n > smallNonNegativeInt then Maybe(randomTryInt) else Maybe.empty))
				_ <- check("reiteratedWhileUndefined", venture1.reiteratedWhileUndefined(Success(0), { case (n, tryInt) if n > smallNonNegativeInt => randomInt }))
			} yield ()
		}
	}


	//// COVENANT ////

	test("Covenant: `covenant.fulfill(int)` should trigger the execution of all the down-chains and subscriptions it has passing `int`") {
		val generators = getGenerators
		import generators.*
		PropF.forAllF { (int: Int, f1: Int => Int, f2: Int => Task[Int]) =>
			// println(s"Begin: int: $int, f1(int): ${f1(int)}")
			val promise = Promise[Unit]()
			val testedCovenant = doer.Covenant[Int]()
			checkCovenant[doer.type](doer, testedCovenant, promise, int, f1, f2)
			testedCovenant.fulfill(int)
			gate(using promise)
		}
	}

	test("Covenant: `covenant.fulfillWith(task)` should tigger the execution of all the down-chains and subscriptions it has passing what `task` shields") {
		val generators = getGenerators
		import generators.{taskArbitrary, *}
		PropF.forAllF(
			for {
				int <- intGen
				task <- genTask(int)
			} yield (int, task),
			Gen.function1[Int, Int](intGen),
			Gen.function1[Int, Task[Int]](taskArbitrary[Int].arbitrary)
		) { case ((int, task), f1, f2) =>
			// println(s"Begin: int: $int, task: $task, f1(int): ${f1(int)}")
			val promise = Promise[Unit]()

			val testedCovenant = doer.Covenant[Int]()
			val subscriptableTask = Covenant_triggerAndWire[Int](doer.Task_delays(1)(_ => int))
			checkCovenant[doer.type](doer, testedCovenant, promise, int, f1, f2)
			testedCovenant.fulfillWith(subscriptableTask)
			gate(using promise)
		}
	}

	private def checkCovenant[DD <: Doer](doer: DD, testedCovenant: doer.Covenant[Int], promise: Promise[Unit], anInt: Int, f1: Int => Int, f2: Int => doer.Task[Int]): Unit = {
		given Promise[Unit] = promise

		import doer.*
		val subscriptionAwareCovenant = doer.Covenant[Int]()
		val subscriptionOnCompleteCallBack: Int => Unit = x => subscriptionAwareCovenant.fulfill(x, true, (y, b) => if b == Doer.ANOTHER_BEFORE then break(s"`subscriptionAwareCovenant` was already  fulfilled with $y"))
		val checks = for {
			_ <- Task_mine { () =>
				if testedCovenant.isSubscribed(subscriptionOnCompleteCallBack) then break("`isAlreadySubscribed` returned true despite no subscription was done")
				testedCovenant.subscribe(subscriptionOnCompleteCallBack)
				if !testedCovenant.isSubscribed(subscriptionOnCompleteCallBack) && testedCovenant.isPending then break("`isAlreadySubscribed` returned false despite the subscription was done")
				if !testedCovenant.isPending then break("`isPending` returned false despite no fulfillment was done")
				if testedCovenant.isCompleted then break("`isCompleted` returned true despite no fulfillment was done")
			}
			_ <- testedCovenant.andThen { x =>
				if x != anInt then break("the covenant completed with a different value than the fulfillment")
				if testedCovenant.isPending then break("`isPending` returned true despite the fulfillment was done")
				if !testedCovenant.isCompleted then break("`isCompleted` returned false despite the fulfillment was done")
			}
			rSubscription <- subscriptionAwareCovenant
			rMap <- testedCovenant.map(f1)
			rFlatMap <- testedCovenant.flatMap(f2)
			f2Result <- f2(anInt)
		} yield {
			if rSubscription != anInt then break("the chained subscription received a different value than the fulfillment")
			else if rMap != f1(anInt) then break("the chained map yielded a different value than the expected one")
			else if rFlatMap != f2Result then break("the chained flatMap yielded a different value than the expected one")
			else promise.trySuccess(())
		}
		checks.triggerAndForget()
	}

	//// COMMITMENT ////

	test("Commitment: `commitment.complete(a)` should trigger the execution of all the down-chains and subscriptions it has, passing `a`") {
		val generators = getGenerators
		import generators.*
		PropF.forAllNoShrinkF(
			for {
				nat <- Gen.choose(1, 9)
				tryNat <- genTry(nat, s"sampleNat / sampleTryNat")
			} yield (nat, tryNat),
			Gen.function1[Int, Int](intGen).faulted(),
			Gen.function1[Int, Venture[Int]](intGen.flatMap(i => genVenture(i, s"sampleInt / f2Result"))).faulted()
		) { case ((nat, tryNat), f1, f2) =>

			/** The promise that this test will succeed. */
			val promise = Promise[Unit]()
			val testedCommitment = doer.Commitment[Int]()
			checksCommitment[D](doer, testedCommitment, promise, nat, tryNat, f1, f2)(() => testedCommitment.complete(tryNat))
			gate(using promise)
		} // .check(Parameters.default.withMinSuccessfulTests(500))
	}

	test("Commitment: `commitment.completeWith(venture)` should trigger the execution of all the down-chains and subscriptions it has, passing what `venture` yields") {
		val generators = getGenerators
		import generators.*
		PropF.forAllNoShrinkF(
			for {
				nat <- Gen.choose(1, 9)
				tryNat <- genTry(nat, s"sampleNat / sampleTryNat")
				venture <- genVentureFromTry(tryNat, "sampleNat / sampleVenture")
			} yield (nat, tryNat, venture),
			Gen.function1[Int, Int](intGen).faulted(),
			Gen.function1[Int, Venture[Int]](intGen.flatMap(i => genVenture(i, s"sampleInt / f2Result"))).faulted()
		) { case ((nat, tryNat, venture), f1, f2) =>

			/** The promise that this test will succeed. */
			val promise = Promise[Unit]()
			val testedCommitment = doer.Commitment[Int]()
			val subscriptableVenture = Commitment_triggerAndWire(doer.Venture_delays(1)(_ => tryNat))
			checksCommitment[D](doer, testedCommitment, promise, nat, tryNat, f1, f2)(() => testedCommitment.completeWith(subscriptableVenture))
			gate(using promise)
		} // .check(Parameters.default.withMinSuccessfulTests(500))
	}

	private def checksCommitment[DD <: Doer & SchedulingExtension & LoopingExtension](
		doer: DD,
		testedCommitment: doer.Commitment[Int],
		promise: Promise[Unit],
		nat: Int,
		expectedOutcome: Try[Int],
		f1: Int => Int,
		f2: Int => doer.Venture[Int]
	)(
		completer: () => Unit
	): Unit = {
		import doer.*

		given Promise[Unit] = promise

		extension (venture: Venture[Int]) {
			/** @return a [[Venture]] like this one but mapping failures containing a [[FaultyValue]] exception to the value contained in that exception.
			 * This tool helps to check that failures ar also correctly propagated. */
			def regenerated: Venture[Int] = venture.recover { case fv: FaultyValue[Int] @unchecked => -fv.value }
		}

		val tryF1AtNat = Try(f1(nat))
		val tryF1AtNegNat = Try(f1(-nat))
		// println(s"\nBegin: nat: $nat, expectedOutcome: $expectedOutcome, Try(f1(nat)): $tryF1AtNat, Try(f1(-nat)): $tryF1AtNegNat")


		var completeWasNotCalled = true
		// The commitment that the `completionObserver` will see the completion of the `testedCommitment`.
		val completionSeenCommitment = doer.Commitment[Int]()
		val completionObserver: Try[Int] => Unit =
			x => completionSeenCommitment.complete(x, true, (y, b) => if b == Doer.ANOTHER_BEFORE then break(s"`subscriptionAwareCommitment` was already completed with $y"))

		// The venture that checks what this test verifies.
		val checks: doer.Venture[Unit] = {
			Venture_ownFlat(() => f2(nat)).transformWith { f2AtNatResult =>
				Venture_ownFlat(() => f2(-nat)).transformWith { f2AtNegNatResult =>
					if testedCommitment.isSubscribed(completionObserver) then break("`isAlreadySubscribed` returned true despite no subscription was done")
					testedCommitment.subscribe(completionObserver)
					if !testedCommitment.isSubscribed(completionObserver) && testedCommitment.isPending then break("`isAlreadySubscribed` returned false despite the subscription was done and the commitment is still pending.")
					if !testedCommitment.isPending && completeWasNotCalled then break("`isPending` returned false despite no completion was done")
					if testedCommitment.isCompleted && completeWasNotCalled then break("`isCompleted` returned true despite no completion was done")
					testedCommitment.transformWith { testedCommitmentOutcome =>
						if testedCommitment.isPending then break("`isPending` returned true despite `complete` was called")
						if !testedCommitment.isCompleted then break("`isCompleted` returned false despite `complete` was called")

						completionSeenCommitment.transformWith { completionSeenCommitmentOutcome =>
							testedCommitment.regenerated.map(f1).transformWith { rMap =>
								testedCommitment.regenerated.flatMap(f2).transform { rFlatMap =>
									// Check that the value yielded to all the down-chains and subscriptions is the correct.
									if !(testedCommitmentOutcome ==== expectedOutcome) then break("the commitment completed with a different value than the provided to `complete`")
									else if !(completionSeenCommitmentOutcome ==== expectedOutcome) then break("the chained subscription received a different value than the provided to `complete`")
									else if expectedOutcome.isSuccess && !(rMap ==== tryF1AtNat) || expectedOutcome.isFailure && !(rMap ==== tryF1AtNegNat) then break("the chained map yielded a different value than the expected one")
									else if expectedOutcome.isSuccess && !(rFlatMap ==== f2AtNatResult) || expectedOutcome.isFailure && !(rFlatMap ==== f2AtNegNatResult) then break("the chained flatMap yielded a different value than the expected one")
									else promise.trySuccess(())
									Success(())
								}
							}
						}
					}
				}
			}
		}
		checks.trigger() { r =>
			if r.isFailure then break(s"The test is wrong. This should not happen: `checks` yielded $r")
		}
		if nat == 1 then {
			completeWasNotCalled = false
			completer()
		} else {
			doer.schedule(doer.newDelaySchedule(nat)) { _ =>
				completeWasNotCalled = false
				completer()
			}
		}
	}

	//// CAUSAL FENCE

	test("CausalFence: `advance` should fulfill with updated state and preserve causal sequencing") {
		val generators = getGenerators
		import generators.*

		PropF.forAllNoShrinkF { (initial: Int, updater: Int => Task[Int]) =>
			// println(s"initial: $initial")
			val promise = Promise[Unit]()
			given Promise[Unit] = promise

			run {
				val fence = CausalFence[Int, doer.type](doer)(initial)

				for {
					expectedUpdate <- Covenant_triggerAndWire(updater(initial), false)
					anchorBefore <- fence.causalAnchor()
					committedBefore <- fence.committed
					update <- fence.advance { (a: Int) =>
						if a != initial then break(s"The first state received by the updates mismatch")
						Covenant_triggerAndWire(updater(a))
					}
					committedAfter <- fence.committed
				} do {
					// println(s"yield: expectedUpdate: $expectedUpdate, anchor: $anchor, commitedBefore: $committedBefore, update: $update, committedAfter: $committedAfter")
					if committedBefore != initial then break("Initial committed state mismatch")
					else if anchorBefore != initial then break("Anchor did not reflect initial state")
					else if update != expectedUpdate then break("The committed state yield by the `advance` method does not match the expected")
					else if committedAfter != expectedUpdate then break("The committed state yield by the `commitedAsync` method does not match expected.")
					else promise.trySuccess(())
				}
			}
			gate
		}
	}

	test("CausalFence: `advanceSpeculatively` before commit should fulfill with rollback or committed state") {
		val generators = getGenerators
		import generators.*

		PropF.forAllNoShrinkF { (initial: Int, updater: Int => Task[Int]) =>
			val promise = Promise[Unit]()
			given Promise[Unit] = promise

			run {
				val fence = CausalFence[Int, doer.type](doer)(initial)
				for {
					anchor <- fence.causalAnchor()
					committedBefore <- fence.committed
					state <- fence.advanceSpeculatively { (a, rba) =>
						if a != initial then break("Speculative update received wrong anchor")
						Covenant_triggerAndWire(updater(a).andThen { _ =>
							rba.rollback(
								true,
								(v, rollbackApplication) =>
									if rollbackApplication == Doer.ROLLBACK_IGNORED then break("Rollback was too late")
									else if v != initial then break("Rollback did not restore initial state")
							)
						})
					}
					committedAfter <- fence.committed
				} do {
					if anchor != initial then break("Anchor mismatch")
					else if committedBefore != initial then break("Initial committed state mismatch")
					else if committedAfter != initial then break("Rollback did not restore committed state")
					else promise.trySuccess(())
				}
			}
			gate
		}
	}

	test("CausalFence: rollback after commit should be ignored") {
		val generators = getGenerators
		import generators.*

		PropF.forAllNoShrinkF { (initial: String, updater: String => Task[String]) =>
			val promise = Promise[Unit]()
			given Promise[Unit] = promise

			run {
				val fence = CausalFence[String, doer.type](doer)(initial)
				for {
					state <- fence.advanceSpeculatively { (a, rba) =>
						Covenant_triggerAndWire(
							updater(a)
								.map(new String(_)) // this line is needed because the random updater function may return a task that yields the argument.
								// ensure `rollback` is called after the task returned by primaryStateUpdater is fulfilled.
								.andThen { x =>
									doer.run {
										rba.rollback(true, (v, rollbackApplication) =>
											if rollbackApplication == Doer.ROLLBACK_IGNORED then promise.trySuccess(())
											else break("Rollback should have been rejected")
										)
									}
								}
						)
					}
				} do if state eq initial then break(s"Rollback incorrectly restored state: initial:`$initial`, state:`$state`")
			}
			gate
		}
	}

	test("CausalFence: multiple stepped advances should serialize and commit in order") {
		val generators = getGenerators
		import generators.*

		PropF.forAllNoShrinkF { (initial: Int, updater: Int => Task[Int]) =>
			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			val fence = CausalFence[Int, doer.type](doer)(initial)

			def loop(expectedState: Int, repetition: Int): Unit = {
				if repetition == 9 then promise.trySuccess(())
				else {
					fence.advanceSpeculatively { (previousState, rba) =>
						if previousState != expectedState then break(s"repetition #$repetition mismatch")
						Covenant_triggerAndWire(updater(previousState))
					}.trigger(false)(newState => loop(newState, repetition + 1))
				}
			}

			run(loop(initial, 0))
			gate
		}
	}

	test("CausalFence: multiple simultaneous advances should serialize and commit in order") {
		val generators = getGenerators
		import generators.*

		PropF.forAllNoShrinkF { (initial: Int, updater: Int => Task[Int]) =>
			val promise = Promise[Unit]()
			given Promise[Unit] = promise

			run {
				val fence = CausalFence[Int, doer.type](doer)(initial)

				val actualSteps = for i <- 0 to 9 yield fence.advanceSpeculatively { (previousState, rba) => Covenant_triggerAndWire(updater(previousState)) }

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

	//// CAUSAL STUCKABLE FENCE

	test("CausalStuckableFence: `advance` should skip transition if failed, or commit updated state if successful") {
		val generators = getGenerators
		import generators.*

		PropF.forAllNoShrinkF { (initial: Try[Int], updater: Int => Venture[Int]) =>
			// println(s"initial: $initial")
			val promise = Promise[Unit]()
			given Promise[Unit] = promise

			run {
				val fence = CausalStuckableFence[Int, doer.type](doer)(initial)

				fence.causalAnchor().trigger(false) { anchorBefore =>
					if !(anchorBefore ==== initial) then break("Anchor before transition and previous state mismatch")
				}

				fence.committed.subscribe { commitedBefore =>
					if !(commitedBefore ==== initial) then break("Commited state before transition and previous state mismatch")
				}

				fence.advanceIf { (previousState: Int) =>
					initial match {
						case Failure(e) => break("A transition from a failed fence wasn't skipped")
						case Success(initialState) => if previousState != initialState then break("Previous and initial state mismatch")
					}
					Maybe(Commitment_triggerAndWire(updater(previousState)))
				}.trigger(false) { actualState =>
					initial match {
						case failure: Failure[Int] =>
							if !(actualState ==== failure) then break("A transition attempt when the fence is failed changed the fence's failure")
							else promise.trySuccess(())
						case Success(initialState) =>
							updater(initialState).trigger(true) { expectedState =>
								if !(actualState ==== expectedState) then break("Actual and expected state mismatch")

								fence.committed.trigger(true) { stateAfter =>
									if stateAfter ==== expectedState then promise.trySuccess(())
									else break("Commited state after transition is not the expected")
								}
							}
					}
				}
			}
			gate
		}
	}

	test("CausalStuckableFence: `advanceSpeculatively` should fulfill with rollback or committed state") {
		val generators = getGenerators
		import generators.*

		PropF.forAllNoShrinkF { (initial: Int, updater: Int => Venture[Int]) =>
			val promise = Promise[Unit]()
			given Promise[Unit] = promise

			run {
				val fence = CausalStuckableFence[Int, doer.type](doer)(Success(initial))
				for {
					anchor <- fence.causalAnchor().asHardyTask
					committedBefore <- fence.committed.asHardyTask
					state <- fence.advanceSpeculativelyIf { (previousState, rba) =>
						if previousState != initial then break("Speculative update received wrong previous state")
						Maybe(Commitment_triggerAndWire(
							updater(previousState).andThen { expectedResult =>
								rba.rollback(
									true,
									(v, wasTooLate) =>
										if wasTooLate == Doer.ROLLBACK_IGNORED then break("Rollback was too late")
										else if !(v ==== Success(initial)) then break("Rollback did not restore initial state")
								)
							}
						))
					}.asHardyTask
					committedAfter <- fence.committed.asHardyTask
				} do {
					if !(anchor ==== Success(initial)) then break("Initial anchor mismatch")
					else if !(committedBefore ==== Success(initial)) then break("Initial committed state mismatch")
					else if !(state ==== Success(initial)) then break("Rollback did not restore committed state")
					else if !(committedAfter ==== Success(initial)) then break("Commited after did not match commited state")
					else promise.trySuccess(())
				}
			}
			gate
		}
	}

	test("CausalStuckableFence: rollback after commit should be ignored") {
		val generators = getGenerators
		import generators.*

		PropF.forAllNoShrinkF { (initial: String, updater: String => Venture[String]) =>
			println(s"Begin: initial=$initial")
			val promise = Promise[Unit]()
			given Promise[Unit] = promise

			run {
				val fence = CausalStuckableFence[String, doer.type](doer)(Success(initial))
				for {
					actualResult <- fence.advanceSpeculativelyIf { (a: String, rba) =>
						println(s"updater called")
						Maybe(Commitment_triggerAndWire(
							updater(a)
								.map(new String(_)) // this line is needed because the random updater function may return a venture that yields the argument.
								.andThen { x =>
									println(s"updater about to complete")
									// ensure `rollback` is called after the venture returned by primaryStateUpdater is fulfilled.
									doer.run {
										println(s"about to rollback")
										rba.rollback(true, (actualResult, wasTooLate) =>
											if wasTooLate == Doer.ROLLBACK_IGNORED then promise.trySuccess(())
											else break("Rollback should have been ignored")
										)
									}
								}
						))
					}.asHardyTask
				} do {
					println(s"transition done")
					actualResult match {
						case Success(actualState) if actualState eq initial => break(s"Rollback incorrectly restored state: initial:`$initial`, state:`$actualResult`")
						case _ => // do nothing
					}
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
		}
	}

	//// Task instance operations ////

	test("Scheduling Task: `Task.schedule(newDelaySchedule(delay))(supplier)` should execute the supplier after the delay") {
		val generators = getGenerators
		import generators.*

		PropF.forAllNoShrinkF(Gen.choose(1, 15)) { (delay: Int) =>
			val schedule = doer.newDelaySchedule(delay)
			val startNano = System.nanoTime()
			val task = doer.Task_schedules(schedule)(_ => delay * 2)
				.map { x =>
					val actualDelay = System.nanoTime - startNano
					// println(s"-------> actual delay: ${actualDelay/1000} micros, expected: $delay millis, error: ${actualDelay/1000_000-delay} schedule: $schedule")
					assert(x == delay * 2, s"found: $x, expected: ${x * 2}")
					assert(actualDelay >= delay * 1_000_000, s"actual: $actualDelay, expected: $delay, schedule: $schedule")
				}
			task.toFutureHardy()
		}
	}

	test("Scheduling Task: `Task.schedule(newFixedRateSchedule)(supplier)` should execute both, the `supplier` and down-chained operations, repeatedly according to the specified specified period until cancellation") {
		val generators = getGenerators
		import generators.*

		PropF.forAllNoShrinkF(Gen.choose(1, 10), Gen.choose(1, 10)) { (initialDelay: Int, interval: Int) =>
			val repetitions = 10 - interval
			// println(s"\nBegin: initialDelay = $initialDelay, interval = $interval, repetitions = $repetitions")
			val schedule = doer.newFixedRateSchedule(initialDelay, interval)
			val promise = Promise[Int]()
			val startMilli = System.currentTimeMillis()
			var counter: Int = 0
			val task = doer.Task_schedules[Int](schedule)(_ => counter)
				.andThen { supplierResult =>
					// println(s"supplierResult = $supplierResult/$repetitions")
					if !doer.wasActivated(schedule) then promise.tryFailure(new AssertionError("The `wasActivated` method returned false for a schedule that was activated"))
					if supplierResult == repetitions then {
						doer.cancel(schedule)
						promise.trySuccess(supplierResult)
					} else if supplierResult > repetitions then {
						promise.tryFailure(new AssertionError("The supplier was execute despite the schedule was canceled in the previous supplier's execution."))
					} else counter += 1
				}
			task.triggerAndForget()
			promise.future.map { supplyResult =>
				val actualDelay = System.currentTimeMillis() - startMilli
				val expectedDelay = interval * repetitions + initialDelay
				// println(s"counter = $counter/$repetitions, actualDelay = $actualDelay, expectedDelay = $expectedDelay, active = ${doer.isActive(schedule)}")
				assertEquals(supplyResult, repetitions)
				assert(actualDelay >= expectedDelay)
				assert(doer.isCanceled(schedule))
			}
		}
	}

	test("Scheduling Task: `task.scheduled(newDelaySchedule(delay))` should preserve the original task's result and postpone its execution the specified `delay`") {
		val generators = getGenerators
		import generators.{taskArbitrary, *}

		PropF.forAllNoShrinkF(taskArbitrary[Int].arbitrary, Gen.choose(1, 10)) { (task: Task[Int], testDelay: Int) =>
			val schedule = doer.newDelaySchedule(testDelay)
			(for {
				directResult <- task
				startTime = System.currentTimeMillis()
				delayedResult <- task.scheduled(schedule)
			} yield {
				val actualDelay = System.currentTimeMillis() - startTime
				assertEquals(directResult, delayedResult)
				assert(actualDelay >= testDelay, s"Execution was not delayed enough. Expected at least ${testDelay}ms, got ${actualDelay}ms")
			}).toFutureHardy()
		}
	}

	test("Scheduling Task: `task.scheduled(newFixedDelaySchedule(initialDelay, period))` should execute the `task` (up-chained operations) repeatedly according to the specified period until cancellation") {
		val generators = getGenerators
		import generators.{taskArbitrary, *}

		PropF.forAllNoShrinkF(
			Gen.choose(1, 10),
			Gen.choose(1, 5),
			taskArbitrary[Int].arbitrary
		) { (initialDelay: Int, interval: Int, task: Task[Int]) =>
			val repetitions = 5 - interval
			// println(s"\nBegin: initialDelay = $initialDelay, interval = $interval, repetitions = $repetitions")
			val schedule = doer.newFixedDelaySchedule(initialDelay, interval)
			val commitment = doer.Commitment[Unit]()
			var counter: Int = 0
			val check = for {
				directResult <- task
				startMilli = System.currentTimeMillis()
				scheduledResult <- task.scheduled(schedule)
			} yield {
				if scheduledResult != directResult then commitment.break(new AssertionError(s"the scheduled result differs from the original"))
				val actualDelay = System.currentTimeMillis() - startMilli
				val expectedDelay = interval * counter + initialDelay
				if actualDelay < expectedDelay then commitment.break(new AssertionError(s"Execution was not delayed enough. Expected at least ${expectedDelay}ms, got ${actualDelay}ms"))
				// println(s"period = $interval, counter = $counter/$repetitions, actualDelay = $actualDelay, expectedDelay = $expectedDelay, active = ${doer.isActive(schedule)}")
				if counter == repetitions then {
					commitment.fulfill(())
					doer.cancel(schedule)
				} else counter += 1
			}
			check.triggerAndForget()
			commitment.toFuture()
		}
	}

	test("Scheduling Task: `task.scheduled(schedule)` should be cancellable after the schedule was activated.") {
		val generators = getGenerators
		import generators.{taskArbitrary, *}

		PropF.forAllNoShrinkF(taskArbitrary[Int].arbitrary, Gen.choose(1, 5)) { (task: Task[Int], delay: Int) =>
			// println(s"Begin: delay: $delay, task: $task")
			val schedule = doer.newDelaySchedule(delay)
			val scheduledTask = task.scheduled(schedule)

			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			var wasCanceled = false
			var hasCompleted = false
			scheduledTask.trigger() { _ =>
				hasCompleted = true
				if wasCanceled then {
					break(s"The task completed despite it was cancelled: isActive=${doer.wasActivated(schedule)}")
				}
				// println(s"-----> wasCanceled: $wasCanceled, schedule: $schedule")
			}
			val cancelsAndWaits = for {
				_ <- Venture_mine[Unit] { () =>
					if doer.isCanceled(schedule) && !hasCompleted then break("The schedule got canceled before canceling it")
					//					if !doer.isActive(schedule) && !hasCompleted then commitment.break(new AssertionError("The schedule got canceled before canceling it"))()
					doer.cancel(schedule)
					wasCanceled = true
					if !doer.isCanceled(schedule) then break("The schedule remains not canceled after being canceled.")
				}
				_ <- doer.Venture_sleeps(delay)

			} yield () // println("cancelsAndWaits completed successfully")
			cancelsAndWaits.trigger()(promise.tryComplete(_))
			gate
		}
	}


	test("Scheduling Task: `task.scheduled(schedule)` should be cancellable before the schedule is activated.") {
		val generators = getGenerators
		import generators.{taskArbitrary, *}

		PropF.forAllNoShrinkF(taskArbitrary[Int].arbitrary, Gen.choose(1, 5)) { (task: Task[Int], delay: Int) =>
			val schedule = doer.newDelaySchedule(delay)
			val scheduledTask = task.scheduled(schedule)
			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			doer.cancel(schedule)
			scheduledTask.trigger() { _ =>
				break(s"The task completed despite it was cancelled: isActive=${doer.wasActivated(schedule)}")
			}
			if !doer.isCanceled(schedule) then break("The schedule says it is not canceled despite it was.")
			doer.schedule(doer.newDelaySchedule(1))(_ => promise.trySuccess(()))
			gate
		}
	}

	//// Task factory methods ////

	test("Scheduling Task.scheduled: should compose correctly with other Task operations") {
		val generators = getGenerators
		import generators.*

		PropF.forAllNoShrinkF { (task: Task[Int], delay: Int, f: Int => String) =>
			//			def f(i: Int): String = i.toString.reverse

			val testDelay = Math.abs(delay % 5) + 1 // 1-5ms
			// println(s"Begin: testDelay = $testDelay")

			// Test composition with map
			val scheduledMapped: Task[String] = task.scheduled(doer.newDelaySchedule(testDelay)).map(f)
			val mappedScheduled: Task[String] = task.map(f).scheduled(doer.newDelaySchedule(testDelay))

			// Test composition with flatMap
			val scheduledFlatMapped: Task[String] = task.scheduled(doer.newDelaySchedule(testDelay)).flatMap(x => Task_ready(f(x)))
			val flatMappedScheduled: Task[String] = task.flatMap(x => Task_ready(f(x))).scheduled(doer.newDelaySchedule(testDelay))

			val checks =
				for {
					_ <- Task_combine(scheduledMapped, mappedScheduled) { (a, b) =>
						assert(a == b, "scheduled.map should equal map.scheduled")
					}
					_ <- Task_combine(scheduledFlatMapped, flatMappedScheduled) { (scheduledFlat, flatMapped) =>
						assert(scheduledFlat == flatMapped, "scheduled.flatMap should equal flatMap.scheduled")
					}
				} yield ()
			checks.toFutureHardy()
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
			println(s"Begin: cancelDelay: $cancelDelay, samples: $samples")

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
			Gen.nonEmptyListOf(Gen.choose(1, maxDelay))
		) { (cancelDelay: Int, delays: List[Int]) =>
			// println(s"Begin: cancelDelay: $cancelDelay, delays: $delays")

			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			@volatile var cancelAllWasCalled = false
			@volatile var cancelNanoTime: Long = 0
			@volatile var maxDistanceBetweenCancellationAndExecutionInNanos: Long = 0

			for delayMillis <- delays do {
				val schedule = doer.newFixedRateSchedule(delayMillis, 1)
				var executionsCounter = 0
				val activationNanoTime: Long = System.nanoTime()
				doer.schedule(schedule) { s =>
					val actualExecutionNanoTime = System.nanoTime()
					if cancelAllWasCalled then {
						val distanceBetweenCancellationAndExecutionInNanos = actualExecutionNanoTime - cancelNanoTime
						if distanceBetweenCancellationAndExecutionInNanos > maxDistanceBetweenCancellationAndExecutionInNanos then maxDistanceBetweenCancellationAndExecutionInNanos = distanceBetweenCancellationAndExecutionInNanos
						// if cancelAll was called and either, a previous execution occurred or the distance between cancellation and expected execution is large enough, break the promise.
						if distanceBetweenCancellationAndExecutionInNanos > schedulerMaximumToleratedNanosBetweenCancellationAndExecution then {
							val message = s"A schedule's routine was executed despite cancelAll was called: previousExecutionsCounter: $executionsCounter, distanceBetweenCancellationAndExecutionInMicros: ${distanceBetweenCancellationAndExecutionInNanos / 1_000}, delay: $delayMillis, cancelTime: $cancelNanoTime, schedule: $schedule, isActive=${doer.wasActivated(schedule)}"
							break(message)
						}
					}
					executionsCounter += 1
				}
			}

			// With another Doer instance, schedule the execution of `doer.cancelAll` outside the doer and wait enough time for the routines be executed before considering the test as passed.
			val otherDoer = buildDoer("other")
			otherDoer.schedule(otherDoer.newDelaySchedule(cancelDelay)) { _ =>
				cancelNanoTime = System.nanoTime()
				cancelAllWasCalled = true
				doer.cancelAll()
				otherDoer.schedule(otherDoer.newDelaySchedule(maxDelay)) { _ =>
					promise.trySuccess(())
					if maxDistanceBetweenCancellationAndExecutionInNanos == 0 then println("No executions after cancellation: VERY GOOD")
					else println(s"maxDistanceBetweenCancellationAndExecutionInMicros = ${maxDistanceBetweenCancellationAndExecutionInNanos / 1_000}")
				}
			}

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

			val schedule = doer.newDelaySchedule(expectedDelay)
			val startTime = System.nanoTime()
			val task = doer.Task_schedules(schedule) { s =>
				if s ne schedule then break(s"The schedule passed to the routine should be the same as the one passed to the `Task_schedules` factory method.")
				else {
					val actualDelay = System.nanoTime() - startTime
					if actualDelay < expectedDelay * 1_000_000 then break("The execution occurred sooner than expected")
					else latch.countDown()
				}
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

			val schedule = doer.newFixedRateSchedule(expectedInitialDelay, expectedPeriod)
			val startTime = System.nanoTime()
			var executionsCounter = 0
			val task = doer.Task_schedules(schedule) { s =>
				if s ne schedule then break(s"The schedule passed to the routine should be the same as the one passed to the `Task_schedules` factory method.")
				else {
					val actualDurationNanos = System.nanoTime() - startTime
					val expectedDurationMillis = expectedInitialDelay + executionsCounter * expectedPeriod
					val differenceMicros = actualDurationNanos / 1000 - expectedDurationMillis * 1000
					// scribe.debug(f"difference: $differenceMicros%6d actual: ${actualDurationNanos/1000}%6d, expected: ${expectedDurationMillis*1000}%6d")
					if differenceMicros < 0 then break(s"The #$executionsCounter execution occurred sooner than expected")
					else if differenceMicros > EXECUTION_DELAY_MARGIN_MILLIS * 1_000 then break(s"The #$executionsCounter execution occurred later than expected after $testExecutionsCounter successful tests")
					else latch.countDown()
				}
				executionsCounter += 1
			}
			task.triggerAndForget()
			if latch.await(expectedInitialDelay + expectedPeriod * REPETITIONS + EXECUTION_DELAY_MARGIN_MILLIS, TimeUnit.MILLISECONDS) then promise.trySuccess(())
			else break(s"The number of executions within the provided time is less than the expected")
			doer.cancel(schedule)
			testExecutionsCounter += 1
			gate
		}
	}
}
