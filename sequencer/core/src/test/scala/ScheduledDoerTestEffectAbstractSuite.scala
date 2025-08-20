package readren.sequencer

import GeneratorsForDoerTests.{*, given}

import munit.ScalaCheckEffectSuite
import org.scalacheck.Test.Parameters
import org.scalacheck.effect.PropF
import org.scalacheck.{Arbitrary, Gen, Prop}
import readren.common.Maybe
import readren.sequencer

import scala.collection.mutable
import scala.compiletime.uninitialized
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}


/** Abstract test suite for testing [[DoerProvider]]s that provide [[Doer]] instances extended with [[SchedulingExtension]].
 *
 * This suite provides comprehensive testing of the [[Doer]] with [[SchedulingExtension]] functionality without being tied to a specific infrastructure ([[DoerProvider]]) implementation.
 * The idea is that the test suites of [[DoerProvider]] extend this testing class to verify that the [[Doer & SchedulingExtension]] instances that the [[DoerProvider]] provides satisfy all the invariants checked here by this testing class.
 *
 * @tparam D The type of Doer being tested, must extend both [[Doer]] and [[SchedulingExtension]].
 */
abstract class ScheduledDoerTestEffectAbstractSuite[D <: Doer & SchedulingExtension] extends ScalaCheckEffectSuite {

	/** Remembers the exceptions that were unhandled in the DoSiThEx's thread.
	 * CAUTION: this variable should be accessed within the DoSiThEx thread only. */
	private val unhandledExceptions: mutable.Set[String] = mutable.Set.empty

	/** Remembers the exceptions that were reported through reportFailure. */
	private val reportedExceptions: mutable.Set[String] = mutable.Set.empty


	/** The implementation should return an instance of [[D]]. */
	protected def buildDoer: D

	/** The extending class should call this method whenever the routine passed to [[Doer.executeSequentially]] or [[SchedulingExtension.scheduleSequentially]] terminates abruptly, passing the unhandled exception. */
	protected def onUnhandledException(doer: Doer, exception: Throwable): Unit = {
		if doer eq fixture() then {
			doer.execute {
				unhandledExceptions.addOne(exception.getMessage)
			}
		}
		scribe.error(s"Unhandled exception:", exception)
	}

	/** The extending class should call this method whenever [[Doer.reportFailure]] is called, passing the received exception. */
	protected def onFailureReported(doer: Doer, failure: Throwable): Unit = {
		// println(s"Reporting failure to munit: ${failure.getMessage}")
		// Note: In a real test environment, this would report to munitExecutionContext
		// For now, we just log it to our reportedExceptions set
		if doer eq fixture() then {
			doer.execute {
				val exceptionToReport = if failure.isInstanceOf[Doer.PanicException] then failure.getCause else failure
				reportedExceptions.addOne(exceptionToReport.getMessage)
			}
		}
	}

	private var fixture: Fixture[D] = uninitialized

	private def getDoer: D = fixture()

	override def munitFixtures: Seq[Fixture[?]] = {
		val doerUnderTest = buildDoer
		fixture = new Fixture[D]("infrastructure") {
			override def apply(): D = doerUnderTest
		}
		List(fixture)
	}

	/** The Doer instance to be tested. Must be provided by the concrete test class. */
	//	protected val doer: D

	/** Provides access to the doer's functionality. */
	//	import doer.*

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


	test("Duty: when `doer.cancelAll()` is called outside the thread currently assigned to `doer`, the scheduled [[Runnable]]s may be executed at most one time and only if called near its scheduled time.") {
		val generators = GeneratorsForDoerTests(getDoer)
		import generators.{*, given}
		val maxDuration = 5
		PropF.forAllNoShrinkF(dutyArbitrary[Int].arbitrary, Gen.choose(1, 5), Gen.nonEmptyListOf(Gen.choose(1, maxDuration))) { (duty: Duty[Int], cancelDelay: Int, delays: List[Int]) =>
			println(s"Begin: duty: $duty, delays: $delays")

			val commitment = doer.Commitment[Unit]()
			@volatile var allWereCancelled = false

			var startNanoTime: Long = 0
			@volatile var cancelNanoTime: Long = 0
			val duties: List[doer.Duty[Int]] =
				for delayMillis <- delays yield {
					val schedule = doer.newFixedRateSchedule(delayMillis, 1)
					var executionsCounter = 0
					duty.andThen(_ => startNanoTime = System.nanoTime())
						.scheduled(schedule)
						.andThen { _ =>
							val expectedExecutionNanoTime = startNanoTime + delayMillis * 1_000_000
							if allWereCancelled && (executionsCounter > 0 || math.abs(cancelNanoTime - expectedExecutionNanoTime) > 2_000) then {
								val distanceMicros = (cancelNanoTime - expectedExecutionNanoTime) / 1000
								commitment.break(new AssertionError(s"A duty completed despite all were cancelled: executionsCounter: $executionsCounter, distanceMicros: $distanceMicros, delay: $delayMillis, cancelTime: $cancelNanoTime, expectedExecutionTime: $expectedExecutionNanoTime, isActive=${doer.isActive(schedule)}"))()
							}
							executionsCounter += 1
						}
				}
			doer.Duty.sequenceToArray(duties).triggerAndForget()

			val otherDoer = buildDoer
			val waits = for {
				_ <- otherDoer.Task.appoint(otherDoer.newDelaySchedule(cancelDelay)) { () =>
					doer.cancelAll()
					cancelNanoTime = System.nanoTime()
					allWereCancelled = true
				}
				_ <- otherDoer.Task.appoint(otherDoer.newDelaySchedule(maxDuration + 1)) { () =>
					commitment.fulfill(())()
				}
			} yield ()
			waits.triggerAndForgetHandlingErrors(e => commitment.break(e)())

			commitment.toFuture()
		}
	}


	test("Duty: when `doer.cancelAll()` is called within the thread currently assigned to `doer`, no scheduled [[Runnable]]s should be executed, even if called near its scheduled time.") {
		val generators = GeneratorsForDoerTests(getDoer)
		import generators.{*, given}
		val maxDuration = 5
		PropF.forAllNoShrinkF(dutyArbitrary[Int].arbitrary, Gen.nonEmptyListOf(genSchedule(doer, maxDuration))) { (duty: Duty[Int], schedules: List[doer.Schedule]) =>
			println(s"Begin: duty: $duty, schedules: $schedules")

			val commitment = doer.Commitment[Unit]()
			@volatile var allWereCancelled = false

			val duties: List[doer.Duty[Int]] =
				for schedule <- schedules yield {
					val scheduledDuty = duty.scheduled(schedule)
					scheduledDuty.andThen { _ =>
						if allWereCancelled then {
							commitment.break(new AssertionError(s"A duty completed despite all were cancelled: isActive=${doer.isActive(schedule)}"))()
						}
					}
				}
			doer.Duty.sequenceToArray(duties).triggerAndForget()

			val otherDoer = buildDoer
			val waits = for {
				_ <- doer.Task.mine { () =>
					doer.cancelAll()
					allWereCancelled = true
				}.onBehalfOf(otherDoer)
				_ <- otherDoer.Task.appoint(otherDoer.newDelaySchedule(maxDuration + 1)) { () =>
					commitment.fulfill(())()
				}
			} yield ()
			waits.triggerAndForgetHandlingErrors(e => commitment.break(e)())

			commitment.toFuture()
		}
	}


	// Monadic left identity law: Duty.ready(x).flatMap(f) == f(x)
	test("Duty: left identity") {
		val generators = GeneratorsForDoerTests(getDoer)
		import generators.{*, given}
		PropF.forAllF { (x: Int, f: Int => Duty[Int]) =>
			val left: doer.Duty[Int] = Duty.ready(x).flatMap(f)
			val right: doer.Duty[Int] = f(x)
			checkEquality(doer)(left, right)
		}
	}

	// Monadic right identity law: m.flatMap(Duty.ready) == m
	test("Duty: right identity") {
		val generators = GeneratorsForDoerTests(getDoer)
		import generators.{*, given}
		PropF.forAllF { (m: Duty[Int]) =>
			val left = m.flatMap(Duty.ready)
			val right = m
			checkEquality(doer)(left, right)
		}
	}

	// Monadic associativity law: m.flatMap(f).flatMap(g) == m.flatMap(x => f(x).flatMap(g))
	test("Duty: associativity") {
		val generators = GeneratorsForDoerTests(getDoer)
		import generators.{*, given}

		PropF.forAllF { (m: Duty[Int], f: Int => Duty[Int], g: Int => Duty[Int]) =>
			val leftAssoc = m.flatMap(f).flatMap(g)
			val rightAssoc = m.flatMap(x => f(x).flatMap(g))
			checkEquality(doer)(leftAssoc, rightAssoc)
		}
	}

	// Functor: `m.map(f) == m.flatMap(a => ready(f(a)))`
	test("Duty: can be transformed with map") {
		val generators = GeneratorsForDoerTests(getDoer)
		import generators.{*, given}

		PropF.forAllF { (m: Duty[Int], f: Int => String) =>
			val left = m.map(f)
			val right = m.flatMap(a => Duty.ready(f(a)))
			checkEquality(doer)(left, right)
		}
	}

	test("Duty: any pair of duties can be combined") {
		val generators = GeneratorsForDoerTests(getDoer)
		import generators.{*, given}

		PropF.forAllF { (dutyA: Duty[Int], dutyB: Duty[Int], f: (Int, Int) => Int) =>
			val combinedDuty = Duty.combine(dutyA, dutyB)(f)

			for {
				combinedResult <- combinedDuty.toFutureHardy()
				dutyAResult <- dutyA.toFutureHardy()
				dutyBResult <- dutyB.toFutureHardy()
			} yield {
				assert(combinedResult == f(dutyAResult, dutyBResult))
			}
		}
	}

	test("Duty: Duty.schedule(newDelaySchedule(delay))(supplier) executes the supplier after the delay") {
		val generators = GeneratorsForDoerTests(getDoer)
		import generators.{*, given}

		PropF.forAllNoShrinkF(Gen.choose(1, 15)) { (delay: Int) =>
			val schedule = doer.newDelaySchedule(delay)
			val startNano = System.nanoTime()
			val duty = Duty.schedule(schedule)(() => delay * 2)
				.map { x =>
					val actualDelay = System.nanoTime - startNano
					// println(s"-------> actual delay: ${actualDelay/1000} micros, expected: $delay millis, error: ${actualDelay/1000_000-delay} schedule: $schedule")
					assert(x == delay * 2, s"found: $x, expected: ${x * 2}")
					assert(actualDelay >= delay * 1_000_000, s"actual: $actualDelay, expected: $delay, schedule: $schedule")
				}
			duty.toFutureHardy()
		}
	}

	test("Duty: `Duty.schedule(newFixedRateSchedule)(supplier)` should execute both, the `supplier` and down-chained operations, repeatedly according to the specified specified period until cancellation") {
		val generators = GeneratorsForDoerTests(getDoer)
		import generators.{*, given}

		PropF.forAllNoShrinkF(Gen.choose(1, 10), Gen.choose(1, 10)) { (initialDelay: Int, interval: Int) =>
			val repetitions = 10 - interval
			// println(s"\nBegin: initialDelay = $initialDelay, interval = $interval, repetitions = $repetitions")
			val schedule = doer.newFixedRateSchedule(initialDelay, interval)
			val covenant = Covenant[Int]()
			val startMilli = System.currentTimeMillis()
			var counter: Int = 0
			val duty = Duty.schedule[Int](schedule)(() => counter)
				.andThen { supplierResult =>
					// println(s"supplierResult = $supplierResult/$repetitions")
					if supplierResult == repetitions then {
						doer.cancel(schedule)
						covenant.fulfill(supplierResult)()
					} else counter += 1
				}
			duty.triggerAndForget()
			covenant
				.map { sr =>
					val actualDelay = System.currentTimeMillis() - startMilli
					val expectedDelay = interval * repetitions + initialDelay
					// println(s"counter = $counter/$repetitions, actualDelay = $actualDelay, expectedDelay = $expectedDelay, active = ${doer.isActive(schedule)}")
					assertEquals(sr, repetitions)
					assert(actualDelay >= expectedDelay)
					assert(!doer.isActive(schedule))
				}
				.toFutureHardy()
		}
	}

	test("Duty: `duty.scheduled(newDelaySchedule(delay))` should preserve the original duty's result and postpone its execution the specified `delay`") {
		val generators = GeneratorsForDoerTests(getDoer)
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

	test("Duty: `duty.scheduled(newFixedDelaySchedule)` should execute the `duty` (up-chained operations) repeatedly according to the specified period until cancellation") {
		val generators = GeneratorsForDoerTests(getDoer)
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
				if scheduledResult != directResult then commitment.break(new AssertionError(s"the scheduled result differs from the original"))()
				val actualDelay = System.currentTimeMillis() - startMilli
				val expectedDelay = interval * counter + initialDelay
				if actualDelay < expectedDelay then commitment.break(new AssertionError(s"Execution was not delayed enough. Expected at least ${expectedDelay}ms, got ${actualDelay}ms"))()
				// println(s"period = $interval, counter = $counter/$repetitions, actualDelay = $actualDelay, expectedDelay = $expectedDelay, active = ${doer.isActive(schedule)}")
				if counter == repetitions then {
					commitment.fulfill(())()
					doer.cancel(schedule)
				} else counter += 1
			}
			check.triggerAndForget()
			commitment.toFuture()
		}
	}

	test("Duty: `duty.scheduled(schedule)` should be cancellable when the schedule is active.") {
		val generators = GeneratorsForDoerTests(getDoer)
		import generators.{*, given}

		val prop = PropF.forAllNoShrinkF(dutyArbitrary[Int].arbitrary, Gen.choose(1, 5)) { (duty: Duty[Int], delay: Int) =>
			// println(s"Begin: delay: $delay, duty: $duty")
			val schedule = doer.newDelaySchedule(delay)
			val scheduledDuty = duty.scheduled(schedule)
			val commitment = Commitment[Unit]()
			var wasCanceled = false
			var hasCompleted = false
			scheduledDuty.trigger() { _ =>
				hasCompleted = true
				if wasCanceled then {
					commitment.break(new AssertionError(s"The duty completed despite it was cancelled: isActive=${doer.isActive(schedule)}"))()
				}
				// println(s"-----> wasCanceled: $wasCanceled, schedule: $schedule")
			}
			val cancelsAndWaits = for {
				_ <- Task.mine[Unit] { () =>
					assert(doer.isActive(schedule) || hasCompleted, "The schedule got canceled before canceling it")
					//					if !doer.isActive(schedule) && !hasCompleted then commitment.break(new AssertionError("The schedule got canceled before canceling it"))()
					doer.cancel(schedule)
					wasCanceled = true
					assert(!doer.isActive(schedule))
				}
				_ <- Task.sleeps(delay)

			} yield () // println("cancelsAndWaits completed successfully")
			commitment.completeWith(cancelsAndWaits)(x => println("was already completed with x"))
			commitment.toFuture()
		}
		prop.check(Parameters.default.withMinSuccessfulTests(100))
	}


	test("Duty: `duty.scheduled(schedule)` should be cancellable before the schedule is active.") {
		val generators = GeneratorsForDoerTests(getDoer)
		import generators.{*, given}

		PropF.forAllNoShrinkF(dutyArbitrary[Int].arbitrary, Gen.choose(1, 5)) { (duty: Duty[Int], delay: Int) =>
			val schedule = doer.newDelaySchedule(delay)
			val scheduledDuty = duty.scheduled(schedule)
			val commitment = Commitment[Unit]()
			doer.cancel(schedule)
			scheduledDuty.trigger() { _ =>
				commitment.break(new AssertionError(s"The duty completed despite it was cancelled: isActive=${doer.isActive(schedule)}"))()
			}
			assert(!doer.isActive(schedule))
			commitment.completeWith(Task.sleeps(delay + 1))()
			commitment.toFuture()
		}
	}


	test("Duty.scheduled: should compose correctly with other Duty operations") {
		val generators = GeneratorsForDoerTests(getDoer)
		import generators.{*, given}

		PropF.forAllNoShrinkF { (duty: Duty[Int], delay: Int, f: Int => String) =>
			//			def f(i: Int): String = i.toString.reverse

			val testDelay = Math.abs(delay % 5) + 1 // 1-5ms
			// println(s"Begin: testDelay = $testDelay")

			// Test composition with map
			val scheduledMapped: Duty[String] = duty.scheduled(doer.newDelaySchedule(testDelay)).map(f)
			val mappedScheduled: Duty[String] = duty.map(f).scheduled(doer.newDelaySchedule(testDelay))

			// Test composition with flatMap
			val scheduledFlatMapped: Duty[String] = duty.scheduled(doer.newDelaySchedule(testDelay)).flatMap(x => Duty.ready(f(x)))
			val flatMappedScheduled: Duty[String] = duty.flatMap(x => Duty.ready(f(x))).scheduled(doer.newDelaySchedule(testDelay))

			val checks =
				for {
					_ <- Duty.combine(scheduledMapped, mappedScheduled) { (a, b) =>
						assert(a == b, "scheduled.map should equal map.scheduled")
					}
					_ <- Duty.combine(scheduledFlatMapped, flatMappedScheduled) { (scheduledFlat, flatMapped) =>
						assert(scheduledFlat == flatMapped, "scheduled.flatMap should equal flatMap.scheduled")
					}
				} yield ()
			checks.toFutureHardy()
		}
	}

	test("SchedulingExtension.schedule: should fail if called with the same `Schedule` instance twice") {
		val generators = GeneratorsForDoerTests(getDoer)
		import generators.{*, given}

		Prop.forAllNoShrink(Gen.choose(1, 5), Gen.choose(1, 5)) { (delay: Int, interval: Int) =>
			val delaySchedule = doer.newDelaySchedule(delay)
			val fixedRateSchedule = doer.newFixedRateSchedule(delay, interval)
			val fixedDelaySchedule = doer.newFixedRateSchedule(delay, interval)
			doer.schedule(delaySchedule)(() => ())
			doer.schedule(fixedRateSchedule)(() => ())
			doer.schedule(fixedDelaySchedule)(() => ())
			assert(intercept[IllegalStateException] {
				doer.schedule(delaySchedule)(() => ())
			}.getMessage.contains(delaySchedule.toString))
			assert(intercept[IllegalStateException] {
				doer.schedule(fixedRateSchedule)(() => ())
			}.getMessage.contains(fixedRateSchedule.toString))
			assert(intercept[IllegalStateException] {
				doer.schedule(fixedDelaySchedule)(() => ())
			}.getMessage.contains(fixedDelaySchedule.toString))
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
		val generators = GeneratorsForDoerTests(getDoer)
		import generators.{*, given}

		PropF.forAllF { (x: Int, f: Int => Task[Int]) =>
			val sx = Task.successful(x)
			val left = Task.successful(x).flatMap(f)
			val right = f(x)
			checkEquality(doer)(left, right)
		}
	}

	// Monadic right identity law: m.flatMap(Task.successful) == m
	test("Task: right identity") {
		val generators = GeneratorsForDoerTests(getDoer)
		import generators.{*, given}

		PropF.forAllF { (m: Task[Int]) =>
			val left = m.flatMap(Task.successful)
			val right = m
			checkEquality(doer)(left, right)
		}
	}

	// Monadic associativity law: m.flatMap(f).flatMap(g) == m.flatMap(x => f(x).flatMap(g))
	test("Task: associativity") {
		val generators = GeneratorsForDoerTests(getDoer)
		import generators.{*, given}

		PropF.forAllF { (m: Task[Int], f: Int => Task[Int], g: Int => Task[Int]) =>
			val leftAssoc = m.flatMap(f).flatMap(g)
			val rightAssoc = m.flatMap(x => f(x).flatMap(g))
			checkEquality(doer)(leftAssoc, rightAssoc)
		}
	}

	// Functor: `m.map(f) == m.flatMap(a => unit(f(a)))`
	test("Task: can be transformed with map") {
		val generators = GeneratorsForDoerTests(getDoer)
		import generators.{*, given}

		PropF.forAllF { (m: Task[Int], f: Int => String) =>
			val left = m.map(f)
			val right = m.flatMap(a => Task.successful(f(a)))
			checkEquality(doer)(left, right)
		}
	}

	// Recovery: `failedTask.recover(f) == if f.isDefinedAt(e) then successful(f(e)) else failed(e)` where e is the exception thrown by failedTask
	test("Task: can be recovered from failure") {
		val generators = GeneratorsForDoerTests(getDoer)
		import generators.{*, given}

		PropF.forAllF { (e: Throwable, f: PartialFunction[Throwable, Int]) =>
			if NonFatal(e) then {
				val leftTask = Task.failed[Int](e).recover(f)
				val rightTask = if f.isDefinedAt(e) then Task.successful(f(e)) else Task.failed(e);
				checkEquality(doer)(leftTask, rightTask)
			} else Future.successful(())
		}
	}

	test("Task: any can be combined") {
		val generators = GeneratorsForDoerTests(getDoer)
		import generators.{*, given}

		PropF.forAllF { (taskA: Task[Int], taskB: Task[Int], f: (Try[Int], Try[Int]) => Try[Int]) =>
			val combinedTask = Task.combine(taskA, taskB)(f)

			for {
				combinedResult <- combinedTask.toFutureHardy()
				taskAResult <- taskA.toFutureHardy()
				taskBResult <- taskB.toFutureHardy()
			} yield {
				assert(combinedResult ==== f(taskAResult, taskBResult))
			}
		}
	}


	test("if a Task's transformation throws an exception the task should complete with that exception if it is non-fatal, or never complete if it is fatal.") {
		val generators = GeneratorsForDoerTests(getDoer)
		import generators.{*, given}
		def sleep1ms: Task[Unit] = doer.Task.alien { () =>
			Future {
				Thread.sleep(1)
			}(using global)
		}

		PropF.forAllF { (task: Task[Int], exception: Throwable) =>

			/** Do the test for a single operation */
			def check(operation: Task[Int] => Task[Int]): Future[Boolean] = {
				// Apply the operation to the random task. The `recover` is to ensure that the random task completes successfully in order for the operation to always be evaluated.
				val operationResult = operation {
					task.recover { case cause => exception.getMessage.hashCode }
				}.toFutureHardy()

				// Build a task that completes with `true` as soon as the `exception` is found among the unhandled exceptions logged in the `unhandledExceptions` set; or `false` if it isn't found after 99 milliseconds.
				val exceptionWasNotHandled: Future[Boolean] = sleep1ms
					// Check if the thread of DoSiThEx was terminated abruptly due to an unhandled exception
					.flatMap { _ =>
						Task.mine { () => unhandledExceptions.remove(exception.getMessage) }
					}
					// repeat the previous until the exception is found among the unhandled exceptions but no more than 99 times.
					.reiteratedUntilSome() { (tries, theExceptionWasFoundAmongTheUnhandledOnes) =>
						if theExceptionWasFoundAmongTheUnhandledOnes then Maybe.some(Success(true))
						else if tries > 99 then Maybe.some(Success(false))
						else Maybe.empty
					}.toFuture()

				// Depending on the kind of exception, fatal or not, the check is very different.
				if NonFatal(exception) then {
					// When the exception is non-fatal the task should complete with a Failure containing the exception thrown by the transformation.
					val nonFatalWasHandled: Future[Boolean] = operationResult.map {
						case Failure(e) if e.equals(exception) => true
						case Failure(wrapper) if wrapper.getCause eq exception => true
						case result =>
							throw new AssertionError(s"The task completed but with an unexpected result. Expected: ${Failure(exception)}, Actual: $result")
					}
					Future.firstCompletedOf(List(nonFatalWasHandled, exceptionWasNotHandled.map { wasNotHandled =>
						assert(!wasNotHandled, "A non fatal was not handled despite is should.")
						false
					}))
				} else {
					// When the exception is fatal it should remain unhandled, causing the doSiThEx thread to terminate. Consequently, the exception will be logged in the unhandledExceptions set, and the task and associated Future will never complete.
					// The following future will complete with `false` if the fatal exception was handled. Otherwise, it will never complete.
					val fatalWasHandled = operationResult.map { result =>
						// println(s"Was handled somehow: result=$result")
						throw new AssertionError(s"The task completed despite it shouldn't. Result=$result")
					}


					// The result of only one of the two futures, `fatalWasHandled` and `exceptionWasNotHandled`, is enough to know if the check is passed. So get the result of the one that completes first.
					Future.firstCompletedOf(List(fatalWasHandled, exceptionWasNotHandled.map { wasNotHandled =>
						assert(wasNotHandled, "The task handled the fatal exception despite it shouldn't");
						true
					}))
				}

			}

			def f0[A](): A = throw exception

			def f1[A, B](a: A): B = throw exception

			def f2[A, B, C](a: A, b: B): C = throw exception

			// println(s"Begin: task=$task, exception=$exception")

			for {
				foreachTestResult <- check(_.foreach(_ => throw exception).map(_ => 0))
				mapTestResult <- check(_.map(f1))
				flatMapTestResult <- check(_.flatMap(f1))
				withFilterTestResult <- check(_.withFilter(f1))
				transformTestResult <- check(_.transform(f1))
				transformWithTestResult <- check(_.transformWith(f1))
				recoverTestResult <- check(_.transform { _ => Failure(new Exception("for recover")) }.recover { case x => f1(x) })
				recoverWithTestResult <- check(_.transform { _ => Failure(new Exception("for recoverWith")) }.recoverWith { case x => f1(x) })
				repeatedHardyUntilSomeTestResult <- check(_.reiteratedHardyUntilSome()(f2))
				repeatedUntilSomeTestResult <- check(_.reiteratedUntilSome()(f2))
				repeatedUntilDefinedTestResult <- check(_.reiteratedHardyUntilDefined() { case (a, b) => f2(a, b) })
				repeatedWhileNoneTestResult <- check(_.reiteratedWhileEmpty(Success(0))(f2))
				repeatedWhileUndefinedTestResult <- check(_.reiteratedWhileUndefined(Success(0)) { case (a, b) => f2(a, b) })
				ownTestResult <- check(_.flatMap(_ => Task.own(f0)))
				ownFlatTestResult <- check(_.flatMap(_ => Task.ownFlat(f0)))
				alienTestResult <- check(_.flatMap(_ => Task.alien(f0)))
			} yield
				assert(
					foreachTestResult
						&& mapTestResult
						&& flatMapTestResult
						&& withFilterTestResult
						&& transformTestResult
						&& transformWithTestResult
						&& recoverTestResult
						&& recoverWithTestResult
						&& repeatedHardyUntilSomeTestResult
						&& repeatedUntilSomeTestResult
						&& repeatedUntilDefinedTestResult
						&& repeatedWhileNoneTestResult
						&& repeatedWhileUndefinedTestResult
						&& ownTestResult
						&& ownFlatTestResult
						&& alienTestResult,
					s"""
					   |foreach: $foreachTestResult
					   |map: $mapTestResult
					   |flatMap: $flatMapTestResult
					   |withFilter: $foreachTestResult
					   |transform: $transformTestResult
					   |transformWith: $transformWithTestResult
					   |recover: $recoverTestResult
					   |recoverWith: $recoverWithTestResult
					   |repeatedHardyUntilSome: $repeatedHardyUntilSomeTestResult
					   |repeatedUntilSome: $repeatedUntilSomeTestResult
					   |repeatedUntilDefined: $repeatedUntilDefinedTestResult
					   |repeatedWhileNone: $repeatedWhileNoneTestResult
					   |repeatedWhileUndefined: $repeatedWhileUndefinedTestResult
					   |own: $ownTestResult
					   |ownFlat: $ownFlatTestResult
					   |alien: $alienTestResult""".stripMargin
				)
			// TODO add a test to check if Task.andThen effect-full function is guarded.
		}
	}

	test("`Task.engage` should either report or not catch exceptions thrown by `onComplete`") {
		val generators = GeneratorsForDoerTests(getDoer)
		import generators.{*, given}
		def sleep1ms: Task[Unit] = doer.Task.alien { () =>
			Future {
				Thread.sleep(1)
			}(using global)
		}

		PropF.forAllF { (task: Task[Int], exception: Throwable) =>

			/** Do the test for a single operation */
			def check[R](operation: Task[Int] => Task[R]): Future[Boolean] = {
				// Apply the operation to the random task and trigger the execution passing a faulty on-complete callback.
				operation(task).trigger()(tryR => throw exception)

				// Build and execute a task that completes with `true` as soon as the `exception` is found among the unhandled exceptions logged in the `unhandledExceptions` set; or `false` if it isn't found after 99 milliseconds.
				sleep1ms
					// Check if the exception was reported or unhandled
					.flatMap { _ =>
						Task.mine { () => unhandledExceptions.remove(exception.getMessage) || reportedExceptions.remove(exception.getMessage) }
					}
					// repeat the previous until the exception is found among the unhandled exceptions but no more than 99 times.
					.reiteratedUntilSome() { (tries, theExceptionWasFoundAmongTheUnhandledOnes) =>
						if theExceptionWasFoundAmongTheUnhandledOnes then {
							// println(s"The exception was found among the unhandled or reported exceptions")
							Maybe.some(Success(true))
						}
						else if tries > 99 then {
							// println(s"The exception was NOT found among the unhandled/reported exceptions after $tries retries. Waiting aborted.")
							Maybe.some(Success(false))
						}
						else {
							// println(s"The exception was NOT found among the unhandled/reported exceptions after $tries retries. Wait more time.")
							Maybe.empty
						}
					}.toFuture()
			}

			val randomInt = exception.getMessage.hashCode()
			val smallNonNegativeInt = randomInt % 9
			val randomBool = (randomInt % 2) == 0
			val randomTryInt = if randomBool then Success(randomInt) else Failure(exception)
			// println(s"Begin: task=$task, exception=$exception, randomInt=$randomInt, randomBool=$randomBool")

			for {
				factoryTestResult <- check(identity)
				foreachTestResult <- check(_.foreach(_ => ()))
				mapTestResult <- check(_.map(identity))
				flatMapTestResult <- check(_.flatMap(_ => task))
				withFilterTestResult <- check(_.withFilter(_ => randomBool))
				consumeTestResult <- check(_.consume(_ => ()))
				andThenTestResult <- check(_.andThen(_ => ()))
				transformTestResult <- check(_.transform(identity))
				transformWithTestResult <- check(_.transformWith(_ => task))
				recoverTestResult <- check(_.recover { case x if randomBool => randomInt })
				recoverWithTestResult <- check(_.recoverWith { case x if randomBool => task })
				repeatedHardyUntilSomeTestResult <- check(_.reiteratedHardyUntilSome() { (n, tryInt) => if n > smallNonNegativeInt then Maybe.some(randomTryInt) else Maybe.empty })
				repeatedUntilSomeTestResult <- check(_.reiteratedUntilSome() { (n, i) => if n > smallNonNegativeInt then Maybe.some(randomTryInt) else Maybe.empty })
				repeatedUntilDefinedTestResult <- check(_.reiteratedHardyUntilDefined() { case (n, tryInt) if n > smallNonNegativeInt => tryInt })
				repeatedWhileNoneTestResult <- check(_.reiteratedWhileEmpty(Success(0)) { (n, tryInt) => if n > smallNonNegativeInt then Maybe.some(randomTryInt) else Maybe.empty })
				repeatedWhileUndefinedTestResult <- check(_.reiteratedWhileUndefined(Success(0)) { case (n, tryInt) if n > smallNonNegativeInt => randomInt })
			} yield
				assert(
					factoryTestResult
						&& foreachTestResult
						&& mapTestResult
						&& flatMapTestResult
						&& withFilterTestResult
						&& consumeTestResult
						&& andThenTestResult
						&& transformTestResult
						&& transformWithTestResult
						&& recoverTestResult
						&& recoverWithTestResult
						&& repeatedHardyUntilSomeTestResult
						&& repeatedUntilSomeTestResult
						&& repeatedUntilDefinedTestResult
						&& repeatedWhileNoneTestResult
						&& repeatedWhileUndefinedTestResult,
					s"""
					   |factory: $factoryTestResult
					   |foreach: $foreachTestResult
					   |map: $mapTestResult
					   |flatMap: $flatMapTestResult
					   |withFilter: $foreachTestResult
					   |consume: $consumeTestResult
					   |andThen: $andThenTestResult
					   |transform: $transformTestResult
					   |transformWith: $transformWithTestResult
					   |recover: $recoverTestResult
					   |recoverWith: $recoverWithTestResult
					   |repeatedHardyUntilSome: $repeatedHardyUntilSomeTestResult
					   |repeatedUntilSome: $repeatedUntilSomeTestResult
					   |repeatedUntilDefined: $repeatedUntilDefinedTestResult
					   |repeatedWhileNone: $repeatedWhileNoneTestResult
					   |repeatedWhileUndefined: $repeatedWhileUndefinedTestResult
				""".stripMargin
				)
		}
	}

	test("`Duty.engage` should either report or not catch exceptions thrown by `onComplete`") {
		val generators = GeneratorsForDoerTests(getDoer)
		import generators.{*, given}
		def sleep1ms: Task[Unit] = doer.Task.alien { () =>
			Future {
				Thread.sleep(1)
			}(using global)
		}

		PropF.forAllF { (duty: Duty[Int], exception: Throwable) =>

			/** Do the test for a single operation */
			def check[R](operation: Duty[Int] => Duty[R]): Future[Boolean] = {
				// Apply the operation to the random duty and trigger the execution passing a faulty on-complete callback.
				operation(duty).trigger()(r => throw exception)

				// Build and execute a duty that completes with `true` as soon as the `exception` is found among the unhandled exceptions logged in the `unhandledExceptions` set; or `false` if it isn't found after 99 milliseconds.
				sleep1ms
					// Check if the exception was reported or unhandled
					.flatMap { _ =>
						Task.mine { () => unhandledExceptions.remove(exception.getMessage) || reportedExceptions.remove(exception.getMessage) }
					}
					// repeat the previous until the exception is found among the unhandled exceptions but no more than 99 times.
					.reiteratedUntilSome() { (tries, theExceptionWasFoundAmongTheUnhandledOnes) =>
						if theExceptionWasFoundAmongTheUnhandledOnes then {
							// println(s"The exception was found among the unhandled or reported exceptions")
							Maybe.some(Success(true))
						}
						else if tries > 99 then {
							// println(s"The exception was NOT found among the unhandled/reported exceptions after $tries retries. Waiting aborted.")
							Maybe.some(Success(false))
						}
						else {
							// println(s"The exception was NOT found among the unhandled/reported exceptions after $tries retries. Wait more time.")
							Maybe.empty
						}
					}.toFuture()
			}

			val randomInt = exception.getMessage.hashCode()
			val smallNonNegativeInt = randomInt % 9
			// println(s"Begin: duty=$duty, exception=$exception")

			for {
				factoryTestResult <- check(identity)
				foreachTestResult <- check(_.foreach(_ => ()))
				mapTestResult <- check(_.map(identity))
				flatMapTestResult <- check(_.flatMap(_ => duty))
				andThenTestResult <- check(_.andThen(_ => ()))
				repeatedUntilSomeTestResult <- check(_.repeatedUntilSome() { (n, i) => if n > smallNonNegativeInt then Maybe.some(randomInt) else Maybe.empty })
				repeatedUntilDefinedTestResult <- check(_.repeatedUntilDefined() { case (n, tryInt) if n > smallNonNegativeInt => tryInt })
				repeatedWhileNoneTestResult <- check(_.repeatedWhileEmpty(Success(0)) { (n, tryInt) => if n > smallNonNegativeInt then Maybe.some(randomInt) else Maybe.empty })
				repeatedWhileUndefinedTestResult <- check(_.repeatedWhileUndefined(Success(0)) { case (n, tryInt) if n > smallNonNegativeInt => randomInt })
			} yield
				assert(
					factoryTestResult
						&& foreachTestResult
						&& mapTestResult
						&& flatMapTestResult
						&& andThenTestResult
						&& repeatedUntilSomeTestResult
						&& repeatedUntilDefinedTestResult
						&& repeatedWhileNoneTestResult
						&& repeatedWhileUndefinedTestResult,
					s"""
					   |factory: $factoryTestResult
					   |foreach: $foreachTestResult
					   |map: $mapTestResult
					   |flatMap: $flatMapTestResult
					   |andThen: $andThenTestResult
					   |repeatedUntilSome: $repeatedUntilSomeTestResult
					   |repeatedUntilDefined: $repeatedUntilDefinedTestResult
					   |repeatedWhileNone: $repeatedWhileNoneTestResult
					   |repeatedWhileUndefined: $repeatedWhileUndefinedTestResult
					   |""".stripMargin
				)
		}
	}
}
