package readren.sequencer

import GeneratorsForDoerTests.{*, given}

import munit.ScalaCheckEffectSuite
import org.scalacheck.Test.Parameters
import org.scalacheck.effect.PropF
import org.scalacheck.{Arbitrary, Gen, Prop}
import readren.common.{Maybe, ScribeConfig}
import readren.sequencer

import java.util
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

	private var sharedDoerProviderFixture: Fixture[DP] = uninitialized
	private var sharedDoerFixture: Fixture[D] = uninitialized
	private var sharedGeneratorsFixture: Fixture[GeneratorsForDoerTests[D]] = uninitialized

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

	/**
	 * Creates instances of the classes under test that can be re-used by many test.
	 * Specifically, creates the instance of [[DoerProvider]] that [[getSharedDoerProvider]] returns, and the instance of [[Doer]] that [[getSharedDoer]] returns. */
	override def munitFixtures: Seq[Fixture[?]] = {
		ScribeConfig.init(deleteLogFilesOnLaunch = true)

		val sharedDoerProvider = buildDoerProvider
		sharedDoerProviderFixture = new Fixture[DP]("shared-doer-provider") {
			override def apply(): DP = sharedDoerProvider
		}
		val sharedDoer = sharedDoerProvider.provide(sharedDoerProvider.tagFromText("main-doer"))
		sharedDoerFixture = new Fixture[D]("main-doer") {
			override def apply(): D = sharedDoer
		}
		val sharedGenerators = GeneratorsForDoerTests(sharedDoer, sharedDoerProvider)
		sharedGeneratorsFixture = new Fixture[GeneratorsForDoerTests[D]]("generators") {
			override def apply(): GeneratorsForDoerTests[D] = sharedGenerators
		}
		List(sharedDoerProviderFixture, sharedDoerFixture)
	}

	/** Clean up resources after tests. */
	override def afterAll(): Unit = {
		println("Shutting down...")
		releaseDoerProvider(getSharedDoerProvider)
	}


	//// Shared instance's getters ////

	/** Gets the shared instance of the [[DoerProvider]] implementation under test. */
	protected def getSharedDoerProvider: DP = sharedDoerProviderFixture()

	/** Builds an instance of [[Doer]] using the shared [[DoerProvider]]. */
	protected def buildDoer(tag: String): D = {
		val provider = sharedDoerProviderFixture()
		provider.provide(provider.tagFromText(tag))
	}

	/** Gets the shared instance of [[Doer]] provided by the shared doer provider. */
	protected def getSharedDoer: D = sharedDoerFixture()

	/** Get the shared instance of [[GeneratorsForDoerTests]] built using the [[DoerProvider]] and [[Doer]] instances returned by [[getSharedDoerProvider]] and [[getSharedDoer]] respectively. */
	protected def getGenerators: GeneratorsForDoerTests[D] = sharedGeneratorsFixture()

	//// UTILITIES ////

	/** Breaks the `promise` if it wasn't already completed. */
	protected def break[P](message: String)(using promise: Promise[P]): Unit =
		promise.tryFailure(new AssertionError(message))

	/** Waits the promise to complete or the specified duration, what happens first. In the second case the promise is broken with the specified message.
	 * @return the [[Future]] view of the provided [[Promise]]. */
	protected def breakAfterWaiting[P](duration: Int, message: String)(using promise: Promise[P]): Future[P] = {
		val latch = CountDownLatch(1)
		promise.future.andThen(_ => latch.countDown())
		latch.await(duration, TimeUnit.MILLISECONDS)
		break(message)
		promise.future
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
		import generators.given

		true
	}

	/**
	 * Test invariants of [[CausalFence]] ensuring that synchronous consumers of the [[LatchedDuty]] returned by [[advance]] observe the up‑to‑date state deterministically.
	 *
	 * Unique checks in this test:
	 *  - Consumers subscribed immediately (synchronously) to the [[LatchedDuty]] returned by [[advance]] must be executed strictly in order of subscription, before any other consumer, and even before the updaters passed to subsequent calls to [[advance]].
	 *
	 *  - A consumer subscribed immediately (synchronously) to the [[LatchedDuty]] returned by [[causalAnchor]] must observe either the state to which the last advance transitioned to, or a state produced earlier, but never an later one.
	 *
	 *  - Game‑changing invariant: Immediately after an [[advance]] call, there are no other advances in flight except the one just created. The returned [[Covenant]] (seen as [[LatchedDuty]]) is the new tail, and any immediate synchronous subscription to it is guaranteed to be the first subscriber in its list. Therefore, when the Covenant fulfills, that consumer sees the up‑to‑date state deterministically, free of concurrent updates to the primary state.
	 *
	 * The test constructs multiple paths that repeatedly advance the fence up to a top serial number, failing if any consumer observes stale state, incorrect ordering, or out‑of‑sequence execution.
	 * // TODO removing delay causes stack overflow. Look for a solution for this test, and consider a solution at the library level. See note in [[Doer.Covenant.fulfillUnsafe]].
	 */
	test("CausalFence - synchronous consumer ordering and anchor freshness: synchronous consumers see up‑to‑date state deterministically - using hoping duties (much faster than the version that uses random delays)") {
		val generators = getGenerators
		import generators.{*, given}

		def buildDuty(serial: Int, hops: Int): Duty[Int] = {
			if hops <= 0 then Duty_ready(serial)
			else Covenant[Int]().fulfillWith(buildDuty(serial, hops - 1), false)
		}

		val initialState = (0, 0)
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

			val promise = Promise[Unit]

			given Promise[Unit] = promise

			type PrimaryState = (pathId: Int, serial: Int)

			val fence = CausalFence[PrimaryState](initialState)
			var derivedSerial: Int = 0
			var advanceCallSerial = 0

			def path(pathId: Int): LatchedDuty[PrimaryState] = {
				val advanceName = s"advance${advanceCallSerial}${advanceCallSerial + 1}"
				advanceCallSerial += 1
				for {
					nextState <- {
						fence.advanceIf(
							(previous, _) => {
								if previous.serial >= topSerial then Maybe.empty
								else {
									val commitedAtStart = fence.committedState
									val duty = buildDuty(previous.serial + 1, hopsList(previous.serial))
										.map(newSerial => (pathId, newSerial))
										.andThen { nextState =>
											if commitedAtStart.serial != fence.committedState.serial then break(s"In the interval between the updater passed to `advance` is called and the Duty it returns completes, no other updater is started; and that is not happening.")
										}
									Maybe.some(duty)
								}
							},
							isInSequence
						)
					}
					anchoredState <- {
						val committedState = fence.committedState
						if nextState.pathId != pathId && nextState.serial < topSerial then break(s"A consumer subscribed to the LatchedDuty returned by `advance` should see the state to which the advance transitioned to; and is not happening: pathId=$pathId, actual: ${nextState.pathId}")
						else if derivedSerial > nextState.serial then break(s"Consumers subscribed immediately (in a synchronously coupled manner) to the `LatchedDuty` returned by `advance`, should be executed in order of subscription before any other consumer, even before the updaters passed to subsequent calls to advance; and is not happening.")
						else if nextState.serial != fence.committedState.serial then break(s"A consumer subscribed immediately (in a synchronously coupled manner) to the LatchedDuty returned by `advance` should see the up-to-date state; and is not happening: current=$nextState, commited=$committedState")
						else derivedSerial = nextState.serial
						fence.causalAnchor()
					}
					latestState <- {
						if anchoredState.serial != fence.committedState.serial then break(s"A consumer subscribed immediately (in a synchronously coupled manner) to the `LatchedDuty` returned by `causalAnchor` should see the the up-to-date state; and is not happening: current=$anchoredState, commited=${fence.committedState}")
						fence.causalChainTail()
					}
					recursiveState <- {
						if nextState.serial < topSerial then path(pathId)
						else fence.committed(true)
					}
				} yield recursiveState
			}

			val swarm: Seq[LatchedDuty[PrimaryState]] = Seq.tabulate(swarmSize) { n => path(n) }
			val checks = for array <- Duty_sequenceToArray(swarm) yield promise.trySuccess(())
			checks.triggerAndForget()

			promise.future
		}

	}


	/**
	 * Test invariants of [[CausalFence]] ensuring that synchronous consumers of the [[LatchedDuty]] returned by [[advance]] observe the up‑to‑date state deterministically.
	 *
	 * Unique checks in this test:
	 *  - Consumers subscribed immediately (synchronously) to the [[LatchedDuty]] returned by [[advance]] must be executed strictly in order of subscription, before any other consumer, and even before the updaters passed to subsequent calls to [[advance]].
	 *
	 *  - A consumer subscribed immediately (synchronously) to the [[LatchedDuty]] returned by [[causalAnchor]] must observe either the state to which the last advance transitioned to, or a state produced earlier, but never an later one.
	 *
	 *  - Game‑changing invariant: Immediately after an [[advance]] call, there are no other advances in flight except the one just created. The returned [[Covenant]] (seen as [[LatchedDuty]]) is the new tail, and any immediate synchronous subscription to it is guaranteed to be the first subscriber in its list. Therefore, when the Covenant fulfills, that consumer sees the up‑to‑date state deterministically, free of concurrent updates to the primary state.
	 *
	 * The test constructs multiple paths that repeatedly advance the fence up to a top serial number, failing if any consumer observes stale state, incorrect ordering, or out‑of‑sequence execution.
	 * // TODO removing delay causes stack overflow. Look for a solution for this test, and consider a solution at the library level. See note in [[Doer.Covenant.fulfillUnsafe]].
	 */
	test("CausalFence - synchronous consumer ordering and anchor freshness: synchronous consumers see up‑to‑date state deterministically - using random delays (very slow)") {
		val generators = getGenerators
		import generators.{*, given}

		val topSerial = 99
		val initialState = (0, 0)
		PropF.forAllF(Gen.choose(1, 9)) { (swarmSize: Int) =>
			println(s"Begin: swarmSize=$swarmSize")

			val promise = Promise[Unit]

			given Promise[Unit] = promise

			type PrimaryState = (pathId: Int, serial: Int)

			val fence = CausalFence[PrimaryState](initialState)
			var derivedSerial: Int = 0

			def path(pathId: Int): LatchedDuty[PrimaryState] = {
				for {
					nextState <- {
						fence.advance(
							(previous, _) => {
								val commitedAtStart = fence.committedState
								val dutyGenerator: Gen[Duty[Int]] = genDuty(previous.serial + 1)
								val randomDuty: Duty[Int] = dutyGenerator.sample.get
								val delay = Gen.choose(-1, 1).sample.get
								val duty = if delay > 0 then randomDuty.delayed(delay) else randomDuty
								duty.map(newSerial => (pathId, newSerial))
									.andThen { nextState =>
										if commitedAtStart.serial != fence.committedState.serial then break(s"In the interval between the updater passed to `advance` is called and the Duty it returns completes, no other updater is started; and that is not happening.")
									}
							},
							isInSequence
						)
					}
					anchoredState <- {
						if nextState.pathId != pathId then break(s"A consumer subscribed to the LatchedDuty returned by `advance` should see the state to which the advance transitioned to; and is not happening: $pathId, actual: ${nextState.pathId}")
						else if derivedSerial > nextState.serial then break(s"Consumers subscribed immediately (in a synchronously coupled manner) to the `LatchedDuty` returned by `advance`, should be executed in order of subscription before any other consumer, even before the updaters passed to subsequent calls to advance; and is not happening.")
						else if nextState.serial != fence.committedState.serial then break(s"A consumer subscribed immediately (in a synchronously coupled manner) to the LatchedDuty returned by `advance` should see the up-to-date state; and is not happening: current=$nextState, commited=${fence.committedState}")
						else derivedSerial = nextState.serial
						fence.causalAnchor()
					}
					latestState <- {
						if anchoredState.serial != fence.committedState.serial then break(s"A consumer subscribed immediately (in a synchronously coupled manner) to the `LatchedDuty` returned by `causalAnchor` should see the the up-to-date state; and is not happening: current=$anchoredState, commited=${fence.committedState}")
						fence.causalChainTail()
					}
					followingState <- {
						if nextState.serial <= topSerial then path(pathId)
						else fence.committed(true)
					}
				} yield {
					if followingState.serial != fence.committedState.serial then break(s"followingState=$followingState, commited=${fence.committedState}")
					followingState
				}
			}

			val swarm: Seq[LatchedDuty[PrimaryState]] = Seq.tabulate(swarmSize) { n => path(n) }
			val checks = for array <- Duty_sequenceToArray(swarm) yield promise.trySuccess(())
			checks.triggerAndForget()
			promise.future
		}
	}

	////////// DOER INFRASTRUCTURE ////////

	test("`Doer.execute` executes in a decoupled manner.") {
		val generators = getGenerators
		import generators.{*, given}

		val promise = Promise[Unit]

		given Promise[Unit] = promise

		var mutable = 1

		val duty = Duty_mine { () =>
			println("start")

			def m12(): Unit = {
				println("executing 12")
				if mutable != 1 then break(s"An execute was not decoupled 1: mutable=$mutable")
				mutable = 2
			}

			execute(m12())

			inline def m23(): Unit = {
				println("executing 23")
				if mutable != 2 then break(s"An execute was not decoupled 2: mutable=$mutable")
				mutable = 3
			}

			execute(m23())

			def m34(): Unit = {
				println("executing 34")
				if mutable != 3 then break(s"An execute was not decoupled 3: mutable=$mutable")
				mutable = 4
			}

			execute(m34())

			def end(): Unit = {
				println("executing end")
				if mutable != 4 then break(s"An execute was not decoupled 4: mutable=$mutable")
				promise.trySuccess(())
			}

			execute(end())
			if mutable != 1 then break(s"An execute was not decoupled 0: mutable=$mutable")

			println("completed")
		}

		duty.triggerAndForget(false)
		promise.future
	}

	test("Doer should execute tasks sequentially") {
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
		assert(latch.await(50, TimeUnit.MILLISECONDS), "All tasks should complete within timeout")
		assert(results.get == 3, "Last task should set result to 3")
		assert(executionOrder.get == 3, "Last task should set execution order to 3")
	}

	//// CONCURRENCY TESTS ////

	test("Multiple doers should execute tasks concurrently") {
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

		assert(latch.await(400, TimeUnit.MILLISECONDS), "All tasks should complete")
		val endTime = System.currentTimeMillis()
		val totalTime = endTime - startTime

		// If tasks were truly concurrent, total time should be close to 100ms, not 300ms
		assert(totalTime < 250, s"Tasks should execute concurrently, total time: ${totalTime}ms")
		assert(executionTimes.get == 3, "All tasks should have executed")
	}

	test("Tasks should see memory updates from previous tasks in the same doer") {
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

		assert(latch.await(5, TimeUnit.SECONDS), "All tasks should complete")
		assert(sharedCounter == 5, "Counter should be incremented 5 times")
	}

	test("Worker threads should be reused efficiently") {
		val latch = new CountDownLatch(10)
		val threadIds = new java.util.concurrent.ConcurrentLinkedQueue[Long]()

		// Submit multiple tasks in different doers and collect thread IDs
		val doers = Array.tabulate[Doer](16)(i => buildDoer(s"$i"))
		for doer <- doers do {
			doer.executeSequentially { () =>
				threadIds.add(Thread.currentThread().threadId)
				latch.countDown()
			}
		}

		assert(latch.await(5, TimeUnit.SECONDS), "All tasks should complete")

		// Should have used multiple threads (concurrent execution)
		val uniqueThreads = threadIds.toArray.toSet.size
		assert(uniqueThreads > 1, s"Should use multiple threads, used: $uniqueThreads")
	}

	//// EXCEPTION HANDLING TESTS ////

	test("Doer should handle exceptions in tasks gracefully") {
		val doer = getSharedDoer
		val latch = new CountDownLatch(2)
		val exceptionCaught = new AtomicBoolean(false)

		// Submit a task that throws an exception
		doer.executeSequentially { () =>
			throw new RuntimeException("Test exception")
		}

		// Submit a task that should still execute after the exception
		doer.executeSequentially { () =>
			exceptionCaught.set(true)
			latch.countDown()
		}

		// Submit another normal task
		doer.executeSequentially { () =>
			latch.countDown()
		}

		assert(latch.await(5, TimeUnit.SECONDS), "Tasks after exception should still execute")
		assert(exceptionCaught.get, "Task after exception should have executed")
	}

	test("Doer should call onFailureReported when the operand passed to `Task.andThen` fails") {
		val mainDoer = getSharedDoer

		PropF.forAllF { (throwable: Throwable) =>
			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			observingUnhandledAndReportedExceptionsDo { () =>
				// Submit a task that uses Task.andThen which will cause a failure report
				mainDoer.Task_unit.andThen(_ => throw throwable).trigger() { _ =>
					if NonFatal(throwable) then break(s"The failure report should be done before the task that produced it completes.")
					else break("The operation completed despite the operand thew a fatal exception")
				}

				breakAfterWaiting(9, "No notification of the exception until 9 milliseconds after applying the operation. Waiting aborted.")
			} {
				(doer, exception) =>
					if NonFatal(exception) then break(s"A non fatal exception was uncaught despite it should: $exception")
					else promise.trySuccess(())
			} { (doer, failure) =>
				if failure.getCause ne throwable then break(s"An unexpected failure was reported: $failure")
				else if doer ne mainDoer then break(s"An unexpected doer was associated to the failure report: ${doer.tag}")
				else promise.trySuccess(())
			}
		}
	}

	test("Doer should call onUnhandledException when task throws uncaught exception") {
		val mainDoer = getSharedDoer

		PropF.forAllF { (exception: Throwable) =>
			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			observingUnhandledAndReportedExceptionsDo { () =>
				// Submit a task that throws an uncaught exception
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

	test("Provider should handle high task load") {
		val doer = getSharedDoer
		val taskCount = 100
		val latch = new CountDownLatch(taskCount)
		val results = new AtomicInteger(0)

		// Submit many tasks
		for _ <- 1 to taskCount do {
			doer.executeSequentially { () =>
				results.incrementAndGet()
				latch.countDown()
			}
		}

		assert(latch.await(10, TimeUnit.SECONDS), "All tasks should complete")
		assert(results.get == taskCount, s"All $taskCount tasks should have executed")
	}

	test("Provider should handle multiple doers with high load") {
		val doerCount = 10
		val tasksPerDoer = 20
		val latch = new CountDownLatch(doerCount * tasksPerDoer)
		val results = new AtomicInteger(0)

		// Create multiple doers and submit tasks to each
		for doerIndex <- 1 to doerCount do {
			val doer = buildDoer(s"stress-doer-$doerIndex")
			for _ <- 1 to tasksPerDoer do {
				doer.executeSequentially { () =>
					results.incrementAndGet()
					latch.countDown()
				}
			}
		}

		assert(latch.await(15, TimeUnit.SECONDS), "All tasks should complete")
		assert(results.get == doerCount * tasksPerDoer, s"All ${doerCount * tasksPerDoer} tasks should have executed")
	}

	//// EDGE CASE TESTS ////

	test("Provider should handle rapid task submission") {
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

		assert(latch.await(5, TimeUnit.SECONDS), "All rapid tasks should complete")
		assert(results.get == 50, "All 50 rapid tasks should have executed")
	}

	test("Provider should maintain task ordering under concurrent submission") {
		val doer = getSharedDoer
		val taskCount = 20
		val latch = new CountDownLatch(taskCount)
		val executionOrder = new java.util.concurrent.ConcurrentLinkedQueue[Int]()

		// Submit tasks from multiple threads
		val futures = for i <- 1 to taskCount yield {
			Future {
				doer.executeSequentially { () =>
					executionOrder.add(i)
					latch.countDown()
				}
			}
		}

		// Wait for all tasks to complete
		Future.sequence(futures)
		assert(latch.await(5, TimeUnit.SECONDS), "All tasks should complete")

		// Verify that tasks were executed in some order (not necessarily submission order due to concurrency)
		val orderList = executionOrder.toArray.toList
		assert(orderList.size == taskCount, s"All $taskCount tasks should have been executed")
		assert(orderList.toSet.size == taskCount, "All task IDs should be unique")
	}


	////////// DUTY //////////

	// Custom equality for Duty based on the result
	private def checkEquality[A](doer: Doer)(duty1: doer.Duty[A], duty2: doer.Duty[A], clue: => Any = "duties yield different results"): Future[Unit] = {
		// println(s"Begin: duty1=$duty1, duty2=$duty2")
		for {
			a1 <- duty1.toFutureHardy()
			a2 <- duty2.toFutureHardy()
		} yield {
			// println(s"$try1 ==== $try2")
			assertEquals(a1, a2, clue)
		}
	}

	// Monadic left identity law: Duty.ready(x).flatMap(f) == f(x)
	test("Duty: left identity") {
		val generators = getGenerators
		import generators.{*, given}
		PropF.forAllF { (x: Int, f: Int => Duty[Int]) =>
			val left: doer.Duty[Int] = Duty_ready(x).flatMap(f)
			val right: doer.Duty[Int] = f(x)
			checkEquality(doer)(left, right)
		}
	}

	// Monadic right identity law: m.flatMap(Duty.ready) == m
	test("Duty: right identity") {
		val generators = getGenerators
		import generators.{*, given}
		PropF.forAllF { (m: Duty[Int]) =>
			val left = m.flatMap(Duty_ready)
			val right = m
			checkEquality(doer)(left, right)
		}
	}

	// Monadic associativity law: m.flatMap(f).flatMap(g) == m.flatMap(x => f(x).flatMap(g))
	test("Duty: associativity") {
		val generators = getGenerators
		import generators.{*, given}

		PropF.forAllF { (m: Duty[Int], f: Int => Duty[Int], g: Int => Duty[Int]) =>
			val leftAssoc = m.flatMap(f).flatMap(g)
			val rightAssoc = m.flatMap(x => f(x).flatMap(g))
			checkEquality(doer)(leftAssoc, rightAssoc)
		}
	}

	// Functor: `m.map(f) == m.flatMap(a => ready(f(a)))`
	test("Duty: can be transformed with map") {
		val generators = getGenerators
		import generators.{*, given}

		PropF.forAllF { (m: Duty[Int], f: Int => String) =>
			val left = m.map(f)
			val right = m.flatMap(a => Duty_ready(f(a)))
			checkEquality(doer)(left, right)
		}
	}

	test("Duty: any pair of duties can be combined") {
		val generators = getGenerators
		import generators.{*, given}

		PropF.forAllF { (dutyA: Duty[Int], dutyB: Duty[Int], f: (Int, Int) => Int) =>
			val combinedDuty = Duty_combine(dutyA, dutyB)(f)

			for {
				combinedResult <- combinedDuty.toFutureHardy()
				dutyAResult <- dutyA.toFutureHardy()
				dutyBResult <- dutyB.toFutureHardy()
			} yield {
				assert(combinedResult == f(dutyAResult, dutyBResult))
			}
		}
	}

	test("Duty: `doer.Duty.foreign(foreignDoer)(foreignDuty)` should complete in the `doer`'s thread") {
		val generators = getGenerators
		import generators.{*, given}
		PropF.forAllNoShrinkF {
			for {
				dutyResult <- intGen
				foreignDuty <- foreignDoerGenerators(true).genDuty(dutyResult)
			} yield (dutyResult, foreignDuty)
		} { case (dutyResult, foreignDuty) =>
			// println(s"Begin: foreignDuty: $foreignDuty")

			doer.Duty_foreign(foreignDoer)(foreignDuty)
				.map { int => int == dutyResult && doer.isInSequence && !foreignDoer.isInSequence }
				.toTask
				.map(assert(_))
				.toFuture()
		}
	}

	test("`Duty.engage` should not catch exceptions thrown by `onComplete`") {
		val generators = getGenerators
		import generators.{*, given}

		PropF.forAllNoShrinkF { (duty: Duty[Int], exception: Throwable, randomInt: Int) =>
			val smallNonNegativeInt = math.abs(randomInt % 10)
			// scribe.debug(s"Begin: duty=$duty, exception=${exception.getMessage}, randomInt=$randomInt, smallNonNegativeInt=$smallNonNegativeInt")

			/** Do the test for a single operation */
			def check[R](opName: String, operatedDuty: Duty[R]): Future[Unit] = {
				// scribe.debug(s"checking operation: $opName")
				// Apply the operation to the random duty and trigger the execution passing a faulty on-complete callback.
				val promise = Promise[Unit]()

				given Promise[Unit] = promise

				observingUnhandledAndReportedExceptionsDo { () =>
					operatedDuty.trigger() { r =>
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
				_ <- check("factory", duty)
				_ <- check("map", duty.map(identity))
				_ <- check("flatMap", duty.flatMap(_ => duty))
				_ <- check("andThen", duty.andThen(_ => ()))
				_ <- check("toTask", duty.toTask)
				_ <- check("repeatedUntilSome", duty.repeatedUntilSome { (n, i) => if n > smallNonNegativeInt then Maybe(randomInt) else Maybe.empty })
				_ <- check("repeatedUntilDefined", duty.repeatedUntilDefined { case (n, tryInt) if n > smallNonNegativeInt => tryInt })
				_ <- check("repeatedWhileNone", duty.repeatedWhileEmpty(Success(0), (n, tryInt) => if n > smallNonNegativeInt then Maybe(randomInt) else Maybe.empty))
				_ <- check("repeatedWhileUndefined", duty.repeatedWhileUndefined(Success(0), { case (n, tryInt) if n > smallNonNegativeInt => randomInt }))
			} yield ()
		}
	}

	////////// TASK /////////////

	// Custom equality for Task based on the result of attempt
	private def checkEquality[A](doer: Doer)(task1: doer.Task[A], task2: doer.Task[A]): Future[Unit] = {
		val futureEquality = for {
			try1 <- task1.toFutureHardy()
			try2 <- task2.toFutureHardy()
		} yield {
			// println(s"$try1 ==== $try2")
			try1 ==== try2
		}
		futureEquality.map(assert(_))
	}

	//	private def evalNow[A](task: Task[A]): Try[A] = {
	//		Await.result(task.toFutureHardy(), new FiniteDuration(1, TimeUnit.MINUTES))
	//	}


	// Monadic left identity law: Task.successful(x).flatMap(f) == f(x)
	test("Task: left identity") {
		val generators = getGenerators
		import generators.{*, given}

		PropF.forAllF { (x: Int, f: Int => Task[Int]) =>
			val sx = Task_successful(x)
			val left = Task_successful(x).flatMap(f)
			val right = f(x)
			checkEquality(doer)(left, right)
		}
	}

	// Monadic right identity law: m.flatMap(Task.successful) == m
	test("Task: right identity") {
		val generators = getGenerators
		import generators.{*, given}

		PropF.forAllF { (m: Task[Int]) =>
			val left = m.flatMap(Task_successful)
			val right = m
			checkEquality(doer)(left, right)
		}
	}

	// Monadic associativity law: m.flatMap(f).flatMap(g) == m.flatMap(x => f(x).flatMap(g))
	test("Task: associativity") {
		val generators = getGenerators
		import generators.{*, given}

		PropF.forAllF { (m: Task[Int], f: Int => Task[Int], g: Int => Task[Int]) =>
			val leftAssoc = m.flatMap(f).flatMap(g)
			val rightAssoc = m.flatMap(x => f(x).flatMap(g))
			checkEquality(doer)(leftAssoc, rightAssoc)
		}
	}

	// Functor: `m.map(f) == m.flatMap(a => unit(f(a)))`
	test("Task: can be transformed with map") {
		val generators = getGenerators
		import generators.{*, given}

		PropF.forAllF { (m: Task[Int], f: Int => String) =>
			val left = m.map(f)
			val right = m.flatMap(a => Task_successful(f(a)))
			checkEquality(doer)(left, right)
		}
	}

	// Recovery: `failedTask.recover(f) == if f.isDefinedAt(e) then successful(f(e)) else failed(e)` where e is the exception thrown by failedTask
	test("Task: can be recovered from failure") {
		val generators = getGenerators
		import generators.{*, given}

		PropF.forAllF { (e: Throwable, f: PartialFunction[Throwable, Int]) =>
			if NonFatal(e) then {
				val leftTask = Task_failed[Int](e).recover(f)
				val rightTask = if f.isDefinedAt(e) then Task_successful(f(e)) else Task_failed(e)
				checkEquality(doer)(leftTask, rightTask)
			} else Future.successful(())
		}
	}

	test("Task: any can be combined") {
		val generators = getGenerators
		import generators.{*, given}

		PropF.forAllF { (taskA: Task[Int], taskB: Task[Int], f: (Try[Int], Try[Int]) => Try[Int]) =>
			val combinedTask = Task_combine(taskA, taskB)(f)

			for {
				combinedResult <- combinedTask.toFutureHardy()
				taskAResult <- taskA.toFutureHardy()
				taskBResult <- taskB.toFutureHardy()
			} yield {
				assert(combinedResult ==== f(taskAResult, taskBResult))
			}
		}
	}

	test("Task: `doer.Task.foreign(foreignDoer)(foreignTask)` should complete in the `doer`'s thread") {
		val generators = getGenerators
		import generators.{*, given}
		PropF.forAllNoShrinkF {
			for {
				taskResult <- intGen
				foreignTask <- foreignDoerGenerators(true).genTask(taskResult, s"foreignTask.arbitrary")
			} yield (taskResult, foreignTask)
		} { case (taskResult, foreignTask) =>
			// println(s"Begin: taskResult: $taskResult, foreignTask: $foreignTask")

			doer.Task_foreign(foreignDoer)(foreignTask)
				.transform { tryInt =>
					assert(tryInt.fold[Boolean](_.getMessage.contains(taskResult.toString), _ == taskResult))
					assert(doer.isInSequence)
					assert(!foreignDoer.isInSequence)
					Success(())
				}.toFuture()
		}
	}

	test("Task: if a function operand passed to a Task's operation throws an exception then, if the exception isn't fatal, the task should complete with a [[Failure]] containing that exception; and if it is fatal, the task should not complete and instead the `DoerProvider.onUnhandledException` method should be called passing the exception.") {
		val generators = getGenerators
		import generators.{*, given}

		PropF.forAllNoShrinkF(
			for {i <- intGen; task <- genTask(i, "")} yield task,
			throwableArbitrary.arbitrary
		) { case (anyTask: Task[Int], exception: Throwable) =>
			// println(s"Begin: anyTask: $anyTask, exception: $exception")

			/** Do the test for a single operation */
			def check[R](opName: String, operatedTask: Task[R], shouldCatchAndReportNonFatalExceptions: Boolean = false): Future[Unit] = {
				// Apply the operation to the random duty and trigger the execution passing a faulty on-complete callback.
				val promise = Promise[Unit]()

				given Promise[Unit] = promise

				observingUnhandledAndReportedExceptionsDo { () =>
					// Apply the operation to the random task.
					operatedTask.trigger() { operationResult =>
						// If the task completed then the result should be a Failure containing the exception, and the exception should be non-fatal.
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


			val successfulTask = anyTask.recover { case cause => exception.getMessage.hashCode }
			val failingTask = anyTask.map { x => throw new FaultyValue(x, "for recover") }

			def f0[A](): A = throw exception

			def f1[A, B](a: A): B = throw exception

			def f2[A, B, C](a: A, b: B): C = throw exception

			for {
				_ <- check("own", Task_own(f0))
				_ <- check("ownFlat", Task_ownFlat(f0))
				_ <- check("foreign", Task_foreign(foreignDoer)(foreignDoer.Task_own(f0)))
				_ <- check("alien", Task_alien(f0))
				_ <- check("combine", Task_combine(anyTask, anyTask)(f2))
				_ <- check("map", successfulTask.map(f1))
				_ <- check("andThen", anyTask.andThen(f1), true)
				_ <- check("flatMap", successfulTask.flatMap(f1))
				_ <- check("withFilter", successfulTask.withFilter(f1))
				_ <- check("transform", anyTask.transform(f1))
				_ <- check("transformWith", anyTask.transformWith(f1))
				_ <- check("recover", failingTask.recover { case x => f1(x) }) // the `map` is to ensure that the upstream task completes abruptly to avoid the tested operation be skipped.
				_ <- check("recoverWith", failingTask.recoverWith { case x => f1(x) })
				_ <- check("reiteratedHardyUntilSome", anyTask.reiteratedHardyUntilSome(f2))
				_ <- check("reiteratedUntilSome", successfulTask.reiteratedUntilSome(f2))
				_ <- check("reiteratedUntilDefined", anyTask.reiteratedHardyUntilDefined { case (a, b) => f2(a, b) })
				_ <- check("reiteratedWhileNone", anyTask.reiteratedWhileEmpty(Success(0), f2))
				_ <- check("reiteratedWhileUndefined", anyTask.reiteratedWhileUndefined(Success(0), { case (a, b) => f2(a, b) }))
			} yield ()
		}
	}

	test("`Task.engage` should not catch exceptions thrown by the `onComplete` operand") {
		val generators = getGenerators
		import generators.{*, given}

		PropF.forAllF { (task1: Task[Int], task2: Task[Int], exception: Throwable, future: Future[Int]) =>


			def check[R](opName: String, operatedTask: Task[R]): Future[Unit] = {
				val promise = Promise[Unit]()

				given Promise[Unit] = promise

				observingUnhandledAndReportedExceptionsDo { () =>
					// Trigger the execution passing a faulty on-complete callback.
					operatedTask.trigger()(tryR => throw exception)

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
			// println(s"Begin: task=$task, exception=$exception, randomInt=$randomInt, randomBool=$randomBool")

			for {
				_ <- check("factory", task1)
				_ <- check("ownFlat", Task_ownFlat(() => task1))
				_ <- check("foreign", Task_foreign(foreignDoer)(foreignDoer.Task_mine(() => randomInt)))
				_ <- check("alien", Task_alien(() => future))
				_ <- check("map", task1.map(identity))
				_ <- check("flatMap", task1.flatMap(_ => task2))
				_ <- check("withFilter", task1.withFilter(_ => randomBool))
				_ <- check("andThen", task1.andThen(_ => ()))
				_ <- check("transform", task1.transform(identity))
				_ <- check("transformWith", task1.transformWith(_ => task2))
				_ <- check("recover", task1.recover { case x if randomBool => randomInt })
				_ <- check("recoverWith", task1.recoverWith { case x if randomBool => task2 })
				_ <- check("reiteratedHardyUntilSome", task1.reiteratedHardyUntilSome { (n, tryInt) => if n > smallNonNegativeInt then Maybe(randomTryInt) else Maybe.empty })
				_ <- check("reiteratedUntilSome", task1.reiteratedUntilSome { (n, i) => if n > smallNonNegativeInt then Maybe(randomTryInt) else Maybe.empty })
				_ <- check("reiteratedHardyUntilDefined", task1.reiteratedHardyUntilDefined { case (n, tryInt) if n > smallNonNegativeInt => tryInt })
				_ <- check("reiteratedWhileEmpty", task1.reiteratedWhileEmpty(Success(0), (n, tryInt) => if n > smallNonNegativeInt then Maybe(randomTryInt) else Maybe.empty))
				_ <- check("reiteratedWhileUndefined", task1.reiteratedWhileUndefined(Success(0), { case (n, tryInt) if n > smallNonNegativeInt => randomInt }))
			} yield ()
		}
	}


	//// COVENANT ////

	test("Covenant: `covenant.fulfill(int)` should trigger the execution of all the down-chains and subscriptions it has passing `int`") {
		val generators = getGenerators
		import generators.{*, given}
		PropF.forAllF { (int: Int, f1: Int => Int, f2: Int => Duty[Int]) =>
			// println(s"Begin: int: $int, f1(int): ${f1(int)}")
			val promise = Promise[Unit]
			val testedCovenant = doer.Covenant[Int]
			checkCovenant[doer.type](doer, testedCovenant, promise, int, f1, f2)
			testedCovenant.fulfill(int)
			promise.future
		}
	}

	test("Covenant: `covenant.fulfillWith(duty)` should tigger the execution of all the down-chains and subscriptions it has passing what `duty` shields") {
		val generators = getGenerators
		import generators.{*, given}
		PropF.forAllF(
			for {
				int <- intGen
				duty <- genDuty(int)
			} yield (int, duty),
			Gen.function1[Int, Int](intGen),
			Gen.function1[Int, Duty[Int]](dutyArbitrary[Int].arbitrary)
		) { case ((int, duty), f1, f2) =>
			// println(s"Begin: int: $int, duty: $duty, f1(int): ${f1(int)}")
			val promise = Promise[Unit]

			val testedCovenant = doer.Covenant[Int]()
			val subscriptableDuty = Covenant_triggerAndWire[Int](doer.Duty_delays(1)(_ => int))
			checkCovenant[doer.type](doer, testedCovenant, promise, int, f1, f2)
			testedCovenant.fulfillWith(subscriptableDuty)
			promise.future
		}
	}

	private def checkCovenant[DD <: Doer](doer: DD, testedCovenant: doer.Covenant[Int], promise: Promise[Unit], anInt: Int, f1: Int => Int, f2: Int => doer.Duty[Int]): Unit = {
		given Promise[Unit] = promise

		import doer.*
		val subscriptionAwareCovenant = doer.Covenant[Int]
		val subscriptionOnCompleteCallBack: Int => Unit = x => subscriptionAwareCovenant.fulfill(x, true, (y, b) => if b == Doer.ANOTHER_BEFORE then break(s"`subscriptionAwareCovenant` was already  fulfilled with $y"))
		val checks = for {
			_ <- Duty_mine { () =>
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
		import generators.{*, given}
		PropF.forAllNoShrinkF(
			for {
				nat <- Gen.choose(1, 9)
				tryNat <- genTry(nat, s"sampleNat / sampleTryNat")
			} yield (nat, tryNat),
			Gen.function1[Int, Int](intGen).faulted(),
			Gen.function1[Int, Task[Int]](intGen.flatMap(i => genTask(i, s"sampleInt / f2Result"))).faulted()
		) { case ((nat, tryNat), f1, f2) =>

			/** The promise that this test will succeed. */
			val promise = Promise[Unit]
			val testedCommitment = Commitment[Int]
			checksCommitment[doer.type](doer, testedCommitment, promise, nat, tryNat, f1, f2)(() => testedCommitment.complete(tryNat))
			promise.future
		}.check(Parameters.default.withMinSuccessfulTests(500))

	}

	test("Commitment: `commitment.completeWith(task)` should trigger the execution of all the down-chains and subscriptions it has, passing what `task` yields") {
		val generators = getGenerators
		import generators.{*, given}
		PropF.forAllNoShrinkF(
			for {
				nat <- Gen.choose(1, 9)
				tryNat <- genTry(nat, s"sampleNat / sampleTryNat")
				task <- genTaskFromTry(tryNat, "sampleNat / sampleTask")
			} yield (nat, tryNat, task),
			Gen.function1[Int, Int](intGen).faulted(),
			Gen.function1[Int, Task[Int]](intGen.flatMap(i => genTask(i, s"sampleInt / f2Result"))).faulted()
		) { case ((nat, tryNat, task), f1, f2) =>

			/** The promise that this test will succeed. */
			val promise = Promise[Unit]

			val testedCommitment = Commitment[Int]()
			val subscriptableTask = Commitment_triggerAndWire(doer.Task_delays(1)(_ => tryNat))
			checksCommitment[doer.type](doer, testedCommitment, promise, nat, tryNat, f1, f2)(() => testedCommitment.completeWith(subscriptableTask))
			promise.future
		}.check(Parameters.default.withMinSuccessfulTests(500))
	}

	private def checksCommitment[DD <: Doer & SchedulingExtension & LoopingExtension](doer: DD, testedCommitment: doer.Commitment[Int], promise: Promise[Unit], nat: Int, expectedOutcome: Try[Int], f1: Int => Int, f2: Int => doer.Task[Int])(completer: () => Unit): Unit = {
		import doer.*

		given Promise[Unit] = promise

		extension (task: Task[Int]) {
			/** @return a [[Task]] like this one but mapping failures containing a [[FaultyValue]] exception to the value contained in that exception.
			 * This tool helps to check that failures ar also correctly propagated. */
			def regenerated: Task[Int] = task.recover { case fv: FaultyValue[Int] @unchecked => -fv.value }
		}

		val tryF1AtNat = Try(f1(nat))
		val tryF1AtNegNat = Try(f1(-nat))
		// println(s"\nBegin: nat: $nat, expectedOutcome: $expectedOutcome, Try(f1(nat)): $tryF1AtNat, Try(f1(-nat)): $tryF1AtNegNat")


		var completeWasNotCalled = true
		// The commitment that the `completionObserver` will see the completion of the `testedCommitment`.
		val completionSeenCommitment = doer.Commitment[Int]
		val completionObserver: Try[Int] => Unit =
			x => completionSeenCommitment.complete(x, true, (y, b) => if b then break(s"`subscriptionAwareCommitment` was already completed with $y"))

		// The task that checks what this test verifies.
		val checks: doer.Task[Unit] = {
			Task_ownFlat(() => f2(nat)).transformWith { f2AtNatResult =>
				Task_ownFlat(() => f2(-nat)).transformWith { f2AtNegNatResult =>
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
		checks.trigger() { r => if r.isFailure then break(s"The test is wrong. This should not happen: `checks` yielded $r") }
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
		import generators.{*, given}

		PropF.forAllNoShrinkF { (initial: Int, updater: Int => Duty[Int]) =>
			// println(s"initial: $initial")
			val promise = Promise[Unit]

			given Promise[Unit] = promise

			val fence: CausalFence[Int] = CausalFence(initial)

			val checks: LatchedDuty[Unit] = for {
				expectedUpdate <- Covenant_triggerAndWire(updater(initial))
				anchorBefore <- fence.causalAnchor()
				committedBefore <- fence.committed(true)
				update <- fence.advance { (a, _) =>
					if a != initial then break(s"The first state received by the updates mismatch")
					Covenant_triggerAndWire(updater(a))
				}
				committedAfter <- fence.committed(true)
			} yield {
				// println(s"yield: expectedUpdate: $expectedUpdate, anchor: $anchor, commitedBefore: $committedBefore, update: $update, committedAfter: $committedAfter")
				if committedBefore != initial then break("Initial committed state mismatch")
				else if anchorBefore != initial then break("Anchor did not reflect initial state")
				else if update != expectedUpdate then break("The committed state yield by the `advance` method does not match the expected")
				else if committedAfter != expectedUpdate then break("The committed state yield by the `commitedAsync` method does not match expected.")
				else promise.trySuccess(())
				()
			}

			checks.triggerAndForget()
			promise.future
		}
	}

	test("CausalFence: `advanceSpeculatively` should fulfill with rollback or committed state") {
		val generators = getGenerators
		import generators.{*, given}

		PropF.forAllNoShrinkF { (initial: Int, updater: Int => Duty[Int]) =>
			val promise = Promise[Unit]

			given Promise[Unit] = promise

			val fence = CausalFence(initial)

			val checks = for {
				anchor <- fence.causalAnchor()
				committedBefore <- fence.committed()
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
				committedAfter <- fence.committed()
			} yield {
				if anchor != initial then break("Anchor mismatch")
				else if committedBefore != initial then break("Initial committed state mismatch")
				else if committedAfter != initial then break("Rollback did not restore committed state")
				else promise.trySuccess(())
			}

			checks.triggerAndForget()
			promise.future
		}
	}

	test("CausalFence: rollback after commit should be ignored") {
		val generators = getGenerators
		import generators.{*, given}

		PropF.forAllNoShrinkF { (initial: String, updater: String => Duty[String]) =>
			val promise = Promise[Unit]

			given Promise[Unit] = promise

			val fence = CausalFence[String](initial)

			val checks = for {
				state <- fence.advanceSpeculatively { (a, rba) =>
					Covenant_triggerAndWire(
						updater(a)
							.map(new String(_)) // this line is needed because the random updater function may return a duty that yields the argument.
							// ensure `rollback` is called after the duty returned by primaryStateUpdater is fulfilled.
							.andThen { x =>
								doer.execute {
									rba.rollback(true, (v, rollbackApplication) =>
										if rollbackApplication == Doer.ROLLBACK_IGNORED then promise.trySuccess(())
										else break("Rollback should have been rejected")
									)
								}
							}
					)
				}
			} yield {
				if state eq initial then break(s"Rollback incorrectly restored state: initial:`$initial`, state:`$state`")
			}

			checks.triggerAndForget()
			promise.future
		}
	}

	test("CausalFence: multiple stepped advances should serialize and commit in order") {
		val generators = getGenerators
		import generators.{*, given}

		PropF.forAllNoShrinkF { (initial: Int, updater: Int => Duty[Int]) =>
			val promise = Promise[Unit]

			given Promise[Unit] = promise

			val fence = CausalFence[Int](initial)

			def loop(expectedState: Int, repetition: Int): Unit = {
				if repetition == 9 then promise.trySuccess(())
				else {
					fence.advanceSpeculatively { (previousState, rba) =>
						if previousState != expectedState then break(s"repetition #$repetition mismatch")
						Covenant_triggerAndWire(updater(previousState))
					}.trigger(false)(newState => loop(newState, repetition + 1))
				}
			}

			loop(initial, 0)
			promise.future
		}
	}

	test("CausalFence: multiple simultaneous advances should serialize and commit in order") {
		val generators = getGenerators
		import generators.{*, given}

		PropF.forAllNoShrinkF { (initial: Int, updater: Int => Duty[Int]) =>
			val promise = Promise[Unit]

			given Promise[Unit] = promise

			val fence = CausalFence[Int](initial)
			val actualSteps = for i <- 0 to 9 yield fence.advanceSpeculatively { (previousState, rba) => Covenant_triggerAndWire(updater(previousState)) }

			def loop(previousState: Int, repetition: Int): Duty[List[Int]] = {
				if repetition > 9 then Duty_ready(Nil)
				else for {
					nextState <- updater(previousState)
					followingStates <- loop(nextState, repetition + 1)
				} yield nextState :: followingStates
			}

			val expectedResultsDuty = loop(initial, 0)

			val checks = for {
				actualResults <- Duty_sequenceToArray(actualSteps)
				expectedResults <- expectedResultsDuty
			} yield {
				// println(s"expected:${expectedResults.mkString(", ")}, actual:${actualResults.mkString(", ")}")
				if actualResults.toList != expectedResults then break(s"expected:${expectedResults.mkString(", ")}, actual:${actualResults.mkString(", ")}")
				else promise.trySuccess(())
			}
			checks.triggerAndForget()
			promise.future
		}
	}

	//// CAUSAL STUCKABLE FENCE

	test("CausalStuckableFence: `advance` should skip transition if failed, or commit updated state if successful") {
		val generators = getGenerators
		import generators.{*, given}

		PropF.forAllNoShrinkF { (initial: Try[Int], updater: Int => Task[Int]) =>
			// println(s"initial: $initial")
			val promise = Promise[Unit]

			given Promise[Unit] = promise

			val fence: CausalStuckableFence[Int] = CausalStuckableFence(initial)

			fence.causalAnchor().trigger(false) { anchorBefore =>
				if !(anchorBefore ==== initial) then break("Anchor before transition and previous state mismatch")
			}

			fence.committed(false).trigger(false) { commitedBefore =>
				if !(commitedBefore ==== initial) then break("Commited state before transition and previous state mismatch")
			}

			fence.advance { (previousState, _) =>
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

							fence.committed(true).trigger(true) { stateAfter =>
								if stateAfter ==== expectedState then promise.trySuccess(())
								else break("Commited state after transition is not the expected")
							}
						}
				}
			}

			promise.future
		}
	}

	test("CausalStuckableFence: `advanceSpeculatively` should fulfill with rollback or committed state") {
		val generators = getGenerators
		import generators.{*, given}

		PropF.forAllNoShrinkF { (initial: Int, updater: Int => Task[Int]) =>
			val promise = Promise[Unit]

			given Promise[Unit] = promise

			val fence = CausalStuckableFence(Success(initial))

			val checks = for {
				anchor <- fence.causalAnchor().toDutyHardy
				committedBefore <- fence.committed().toDutyHardy
				state <- fence.advanceSpeculatively { (previousState, rba) =>
					if previousState != initial then break("Speculative update received wrong previous state")
					Maybe(Commitment_triggerAndWire(
						updater(previousState).andThen { expectedResult =>
							rba.rollback(
								true,
								(v, wasTooLate) =>
									if wasTooLate then break("Rollback was too late")
									else if !(v ==== Success(initial)) then break("Rollback did not restore initial state")
							)
						}
					))
				}.toDutyHardy
				committedAfter <- fence.committed().toDutyHardy
			} yield {
				if !(anchor ==== Success(initial)) then break("Initial anchor mismatch")
				else if !(committedBefore ==== Success(initial)) then break("Initial committed state mismatch")
				else if !(state ==== Success(initial)) then break("Rollback did not restore committed state")
				else if !(committedAfter ==== Success(initial)) then break("Commited after did not match commited state")
				else promise.trySuccess(())
			}

			checks.triggerAndForget()
			promise.future
		}
	}

	test("CausalStuckableFence: rollback after commit should be ignored") {
		val generators = getGenerators
		import generators.{*, given}

		PropF.forAllNoShrinkF { (initial: String, updater: String => Task[String]) =>
			println(s"Begin: initial=$initial")
			val promise = Promise[Unit]

			given Promise[Unit] = promise

			val fence = CausalStuckableFence[String](Success(initial))

			val checks = for {
				actualResult <- fence.advanceSpeculatively { (a, rba) =>
					println(s"updater called")
					Maybe(Commitment_triggerAndWire(
						updater(a)
							.map(new String(_)) // this line is needed because the random updater function may return a task that yields the argument.
							.andThen { x =>
								println(s"updater about to complete")
								// ensure `rollback` is called after the task returned by primaryStateUpdater is fulfilled.
								doer.execute {
									println(s"about to rollback")
									rba.rollback(true, (actualResult, wasTooLate) =>
										if wasTooLate then promise.trySuccess(())
										else break("Rollback should have been ignored")
									)
								}
							}
					))
				}.toDutyHardy
			} yield {
				println(s"transition done")
				actualResult match {
					case Success(actualState) if actualState eq initial => break(s"Rollback incorrectly restored state: initial:`$initial`, state:`$actualResult`")
					case _ => // do nothing
				}
			}

			checks.triggerAndForget()
			promise.future
		}
	}


	//// SCHEDULING ////

	//// scheduling infrastructure ////

	test("Scheduling: `SchedulingExtension.schedule` should fail if called with the same `Schedule` instance twice") {
		val generators = getGenerators
		import generators.{*, given}

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

	//// Duty instance operations ////

	test("Scheduling Duty: `Duty.schedule(newDelaySchedule(delay))(supplier)` should execute the supplier after the delay") {
		val generators = getGenerators
		import generators.{*, given}

		PropF.forAllNoShrinkF(Gen.choose(1, 15)) { (delay: Int) =>
			val schedule = doer.newDelaySchedule(delay)
			val startNano = System.nanoTime()
			val duty = doer.Duty_schedules(schedule)(_ => delay * 2)
				.map { x =>
					val actualDelay = System.nanoTime - startNano
					// println(s"-------> actual delay: ${actualDelay/1000} micros, expected: $delay millis, error: ${actualDelay/1000_000-delay} schedule: $schedule")
					assert(x == delay * 2, s"found: $x, expected: ${x * 2}")
					assert(actualDelay >= delay * 1_000_000, s"actual: $actualDelay, expected: $delay, schedule: $schedule")
				}
			duty.toFutureHardy()
		}
	}

	test("Scheduling Duty: `Duty.schedule(newFixedRateSchedule)(supplier)` should execute both, the `supplier` and down-chained operations, repeatedly according to the specified specified period until cancellation") {
		val generators = getGenerators
		import generators.{*, given}

		PropF.forAllNoShrinkF(Gen.choose(1, 10), Gen.choose(1, 10)) { (initialDelay: Int, interval: Int) =>
			val repetitions = 10 - interval
			// println(s"\nBegin: initialDelay = $initialDelay, interval = $interval, repetitions = $repetitions")
			val schedule = doer.newFixedRateSchedule(initialDelay, interval)
			val promise = Promise[Int]()
			val startMilli = System.currentTimeMillis()
			var counter: Int = 0
			val duty = doer.Duty_schedules[Int](schedule)(_ => counter)
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
			duty.triggerAndForget()
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

	test("Scheduling Duty: `duty.scheduled(newDelaySchedule(delay))` should preserve the original duty's result and postpone its execution the specified `delay`") {
		val generators = getGenerators
		import generators.{*, given}

		PropF.forAllNoShrinkF(dutyArbitrary[Int].arbitrary, Gen.choose(1, 10)) { (duty: Duty[Int], testDelay: Int) =>
			val schedule = doer.newDelaySchedule(testDelay)
			(for {
				directResult <- duty
				startTime = System.currentTimeMillis()
				delayedResult <- duty.scheduled(schedule)
			} yield {
				val actualDelay = System.currentTimeMillis() - startTime
				assertEquals(directResult, delayedResult)
				assert(actualDelay >= testDelay, s"Execution was not delayed enough. Expected at least ${testDelay}ms, got ${actualDelay}ms")
			}).toFutureHardy()
		}
	}

	test("Scheduling Duty: `duty.scheduled(newFixedDelaySchedule(initialDelay, period))` should execute the `duty` (up-chained operations) repeatedly according to the specified period until cancellation") {
		val generators = getGenerators
		import generators.{*, given}

		PropF.forAllNoShrinkF(
			Gen.choose(1, 10),
			Gen.choose(1, 5),
			dutyArbitrary[Int].arbitrary
		) { (initialDelay: Int, interval: Int, duty: Duty[Int]) =>
			val repetitions = 5 - interval
			// println(s"\nBegin: initialDelay = $initialDelay, interval = $interval, repetitions = $repetitions")
			val schedule = doer.newFixedDelaySchedule(initialDelay, interval)
			val commitment = Commitment[Unit]()
			var counter: Int = 0
			val check = for {
				directResult <- duty
				startMilli = System.currentTimeMillis()
				scheduledResult <- duty.scheduled(schedule)
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

	test("Scheduling Duty: `duty.scheduled(schedule)` should be cancellable after the schedule was activated.") {
		val generators = getGenerators
		import generators.{*, given}

		PropF.forAllNoShrinkF(dutyArbitrary[Int].arbitrary, Gen.choose(1, 5)) { (duty: Duty[Int], delay: Int) =>
			// println(s"Begin: delay: $delay, duty: $duty")
			val schedule = doer.newDelaySchedule(delay)
			val scheduledDuty = duty.scheduled(schedule)

			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			var wasCanceled = false
			var hasCompleted = false
			scheduledDuty.trigger() { _ =>
				hasCompleted = true
				if wasCanceled then {
					break(s"The duty completed despite it was cancelled: isActive=${doer.wasActivated(schedule)}")
				}
				// println(s"-----> wasCanceled: $wasCanceled, schedule: $schedule")
			}
			val cancelsAndWaits = for {
				_ <- Task_mine[Unit] { () =>
					if doer.isCanceled(schedule) && !hasCompleted then break("The schedule got canceled before canceling it")
					//					if !doer.isActive(schedule) && !hasCompleted then commitment.break(new AssertionError("The schedule got canceled before canceling it"))()
					doer.cancel(schedule)
					wasCanceled = true
					if !doer.isCanceled(schedule) then break("The schedule remains not canceled after being canceled.")
				}
				_ <- doer.Task_sleeps(delay)

			} yield () // println("cancelsAndWaits completed successfully")
			cancelsAndWaits.trigger()(promise.tryComplete(_))
			promise.future
		}
	}


	test("Scheduling Duty: `duty.scheduled(schedule)` should be cancellable before the schedule is activated.") {
		val generators = getGenerators
		import generators.{*, given}

		PropF.forAllNoShrinkF(dutyArbitrary[Int].arbitrary, Gen.choose(1, 5)) { (duty: Duty[Int], delay: Int) =>
			val schedule = doer.newDelaySchedule(delay)
			val scheduledDuty = duty.scheduled(schedule)
			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			doer.cancel(schedule)
			scheduledDuty.trigger() { _ =>
				break(s"The duty completed despite it was cancelled: isActive=${doer.wasActivated(schedule)}")
			}
			if !doer.isCanceled(schedule) then break("The schedule says it is not canceled despite it was.")
			doer.schedule(doer.newDelaySchedule(1))(_ => promise.trySuccess(()))
			promise.future
		}
	}

	//// Duty factory methods ////

	test("Scheduling Duty.scheduled: should compose correctly with other Duty operations") {
		val generators = getGenerators
		import generators.{*, given}

		PropF.forAllNoShrinkF { (duty: Duty[Int], delay: Int, f: Int => String) =>
			//			def f(i: Int): String = i.toString.reverse

			val testDelay = Math.abs(delay % 5) + 1 // 1-5ms
			// println(s"Begin: testDelay = $testDelay")

			// Test composition with map
			val scheduledMapped: Duty[String] = duty.scheduled(doer.newDelaySchedule(testDelay)).map(f)
			val mappedScheduled: Duty[String] = duty.map(f).scheduled(doer.newDelaySchedule(testDelay))

			// Test composition with flatMap
			val scheduledFlatMapped: Duty[String] = duty.scheduled(doer.newDelaySchedule(testDelay)).flatMap(x => Duty_ready(f(x)))
			val flatMappedScheduled: Duty[String] = duty.flatMap(x => Duty_ready(f(x))).scheduled(doer.newDelaySchedule(testDelay))

			val checks =
				for {
					_ <- Duty_combine(scheduledMapped, mappedScheduled) { (a, b) =>
						assert(a == b, "scheduled.map should equal map.scheduled")
					}
					_ <- Duty_combine(scheduledFlatMapped, flatMappedScheduled) { (scheduledFlat, flatMapped) =>
						assert(scheduledFlat == flatMapped, "scheduled.flatMap should equal flatMap.scheduled")
					}
				} yield ()
			checks.toFutureHardy()
		}
	}

	//// Doer wide cancellation ////

	test("Scheduling: when `doer.cancelAll()` is called within the thread currently assigned to `doer`, then no scheduled [[Runnable]]s should be executed, even if called near its scheduled time.") {
		val generators = getGenerators
		import generators.{*, given}
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
			doer.execute {
				for sample <- samples do {
					doer.schedule(sample) { s =>
						if cancelAllWasCalled then break(s"A schedule's routine was executed despite `cancelAll` was cancelled: schedule: $sample, isActive=${doer.wasActivated(sample)}")
					}
				}
			}

			// With another Doer instance, schedule the execution of `doer.cancelAll` within the doer and wait enough time for the routines be executed before considering the test as passed.
			val otherDoer = buildDoer("other")
			otherDoer.schedule(otherDoer.newDelaySchedule(cancelDelay)) { _ =>
				doer.execute {
					doer.cancelAll()
					cancelAllWasCalled = true
					otherDoer.schedule(otherDoer.newDelaySchedule(maxDuration))(_ => promise.trySuccess(()))
				}
			}

			promise.future
		}
	}

	test("Scheduling: when `doer.cancelAll()` is called outside the thread currently assigned to `doer`, the scheduled [[Runnable]]s may be executed at most one time and only if called near its scheduled time.") {
		val generators = getGenerators
		import generators.{*, given}
		val maxDelay = 5
		PropF.forAllNoShrinkF(
			Gen.choose(1, maxDelay),
			Gen.nonEmptyListOf(Gen.choose(1, maxDelay))
		) { (cancelDelay: Int, delays: List[Int]) =>
			// println(s"Begin: cancelDelay: $cancelDelay, delays: $delays")

			val promise = Promise[Unit]

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

			promise.future
		}
	}

	//// Duty_schedules factory method

	test("Duty_schedules: The duty returned by `Duty_schedules(newDelaySchedule(delay))(body)` should execute `body` and yield its result once after the delay.") {
		// Test with a schedule that only executes once (e.g., single delay)
		// Verify supplier is called exactly once and duty yields the result

		val generators = getGenerators
		import generators.{*, given}
		PropF.forAllNoShrinkF(
			Gen.choose(1, 5)
		) { (expectedDelay: Int) =>
			// println(s"Begin: upChain: $upChain, delays: $delays")

			val promise = Promise[Unit]

			given Promise[Unit] = promise

			val latch = new CountDownLatch(2)

			val schedule = doer.newDelaySchedule(expectedDelay)
			val startTime = System.nanoTime()
			val duty = doer.Duty_schedules(schedule) { s =>
				if s ne schedule then break(s"The schedule passed to the routine should be the same as the one passed to the `Duty_schedules` factory method.")
				else {
					val actualDelay = System.nanoTime() - startTime
					if actualDelay < expectedDelay * 1_000_000 then break("The execution occurred sooner than expected")
					else latch.countDown()
				}
			}
			duty.triggerAndForget()
			if latch.await(expectedDelay * 2 + 5, TimeUnit.MILLISECONDS) then break("The routine was executed more than one time")
			else promise.trySuccess(())
			promise.future
		}
	}

	test("Duty_schedules: The duty returned by `Duty_schedules(newFixedRateSchedule(initialDelay, interval))(body)` should execute `body` and yield its result repeatedly after the instants determined by the schedule.") {
		val generators = getGenerators
		val REPETITIONS = 4
		var testExecutionsCounter = 0
		import generators.{*, given}
		PropF.forAllNoShrinkF(
			Gen.choose(-1, 10),
			Gen.choose(1, 5)
		) { (expectedInitialDelay: Int, expectedPeriod: Int) =>
			// scribe.debug(s"Begin: $expectedInitialDelay, $expectedPeriod")

			val EXECUTION_DELAY_MARGIN_MILLIS = if testExecutionsCounter < 5 then 100 else 50
			val promise = Promise[Unit]

			given Promise[Unit] = promise

			val latch = new CountDownLatch(REPETITIONS)

			val schedule = doer.newFixedRateSchedule(expectedInitialDelay, expectedPeriod)
			val startTime = System.nanoTime()
			var executionsCounter = 0
			val duty = doer.Duty_schedules(schedule) { s =>
				if s ne schedule then break(s"The schedule passed to the routine should be the same as the one passed to the `Duty_schedules` factory method.")
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
			duty.triggerAndForget()
			if latch.await(expectedInitialDelay + expectedPeriod * REPETITIONS + EXECUTION_DELAY_MARGIN_MILLIS, TimeUnit.MILLISECONDS) then promise.trySuccess(())
			else break(s"The number of executions within the provided time is less than the expected")
			doer.cancel(schedule)
			testExecutionsCounter += 1
			promise.future
		}
	}
}
