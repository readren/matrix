package readren.sequencer

import Doer.ExceptionReport
import DoerTestEffect.currentDoer
import SchedulingExtension.*

import munit.ScalaCheckEffectSuite
import org.scalacheck.effect.PropF
import org.scalacheck.{Arbitrary, Gen}
import readren.common.Maybe
import readren.sequencer

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, ScheduledFuture, TimeUnit}
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object ScheduledDoerTestEffect {
	val currentDoer: ThreadLocal[Doer] = new ThreadLocal()
}

class ScheduledDoerTestEffect extends ScalaCheckEffectSuite {

	/** Remembers the exceptions that were unhandled in the DoSiThEx's thread.
	 * CAUTION: this variable should be accessed within the DoSiThEx thread only. */
	private val unhandledExceptions = mutable.Set.empty[String]
	private val reportedExceptions = mutable.Set.empty[String]

	private object doer extends Doer, SchedulingExtension { thisDoer =>
		override type Tag = String
		
		val tag: Tag = "testing doer"
		
		private val doSiThEx = Executors.newSingleThreadScheduledExecutor()

		private val sequencer: AtomicInteger = new AtomicInteger(0)

		override def executeSequentially(runnable: Runnable): Unit = {
			val id = sequencer.addAndGet(1)
			// println(s"queuedForSequentialExecution: pre execute; id=$id, thread=${Thread.currentThread().getName}; runnable=$runnable")
			doSiThEx.execute(() => {
				currentDoer.set(thisDoer)
				// println(s"queuedForSequentialExecution: pre run; id=$id; thread=${Thread.currentThread().getName}")
				try {
					runnable.run()
					// println(s"queuedForSequentialExecution: run completed normally; id=$id; thread=${Thread.currentThread().getName}")
				}
				catch {
					case cause: Throwable =>
						// println(s"queuedForSequentialExecution: run completed abruptly with: $cause; id=$id; thread=${Thread.currentThread().getName}")
						unhandledExceptions.addOne(cause.getMessage);
						throw cause;
				} finally {
					currentDoer.remove()
					// println(s"queuedForSequentialExecution: finally; id=$id; thread=${Thread.currentThread().getName}")
				}
			})

		}

		override def current: Doer = currentDoer.get

		override def reportFailure(failure: Throwable): Unit = {
			// println(s"Reporting failure to munit: ${failure.getMessage}")
			munitExecutionContext.reportFailure(failure)
			doSiThEx.execute { () =>
				val exceptionToReport = if failure.isInstanceOf[ExceptionReport] then failure.getCause else failure
				reportedExceptions.addOne(exceptionToReport.getMessage)
			}
		}

		//// SCHEDULING EXTENSION

		sealed abstract class TSchedule {
			var scheduledFuture: ScheduledFuture[?] | Null = null
			@volatile var isActive: Boolean = false
			@volatile var canceled: Boolean = false
		}

		case class TDelaySchedule(delay: MilliDuration) extends TSchedule

		case class TFixedRateSchedule(initialDelay: MilliDuration, interval: MilliDuration) extends TSchedule

		case class TFixedDelaySchedule(initialDelay: MilliDuration, delay: MilliDuration) extends TSchedule

		/** needed to support [[cancelAll]]. */
		private val activeSchedules: mutable.Set[TSchedule] = mutable.Set.empty

		override type Schedule = TSchedule

		override def newDelaySchedule(delay: MilliDuration): TDelaySchedule = TDelaySchedule(delay)

		override def newFixedRateSchedule(initialDelay: MilliDuration, interval: MilliDuration): TFixedRateSchedule = TFixedRateSchedule(initialDelay, interval)

		override def newFixedDelaySchedule(initialDelay: MilliDuration, delay: MilliDuration): Schedule = TFixedDelaySchedule(initialDelay, delay)

		override def scheduleSequentially(schedule: Schedule, runnable: Runnable): Unit = {
			assert(!schedule.isActive)
			if schedule.canceled then return
			activeSchedules.addOne(schedule)
			schedule.scheduledFuture = schedule match {
				case TDelaySchedule(delay) =>
					val wrapper: Runnable = () =>
						if schedule.isActive then {
							currentDoer.set(this)
							runnable.run()
							currentDoer.remove()
							activeSchedules.remove(schedule)
							schedule.isActive = false
						}
					doSiThEx.schedule(wrapper, delay, TimeUnit.MILLISECONDS)

				case TFixedRateSchedule(initialDelay, interval) =>
					val wrapper: Runnable = () => if schedule.isActive then runnable.run()
					doSiThEx.scheduleAtFixedRate(wrapper, initialDelay, interval, TimeUnit.MILLISECONDS)

				case TFixedDelaySchedule(initialDelay, delay) =>
					val wrapper: Runnable = () => if schedule.isActive then runnable.run()
					doSiThEx.scheduleWithFixedDelay(wrapper, initialDelay, delay, TimeUnit.MILLISECONDS)
			}
			schedule.isActive = true
		}

		override def cancel(schedule: Schedule): Unit = {
			schedule.canceled = true
			if schedule.isActive then {
				schedule.scheduledFuture.cancel(false)
				activeSchedules.remove(schedule)
				schedule.isActive = false
			}
		}

		override def cancelAll(): Unit = {
			for schedule <- activeSchedules do {
				schedule.canceled = true
				if schedule.isActive then {
					schedule.scheduledFuture.cancel(false)
					schedule.isActive = false
				}
			}
			activeSchedules.clear()
		}

		override def isActive(schedule: Schedule): Boolean = schedule.isActive
	}

	import doer.*

	private val shared = new DoerTestShared[doer.type](doer)
	import shared.{*, given}

	////////// DUTY //////////

	// Custom equality for Duty based on the result
	private def checkEquality[A](duty1: Duty[A], duty2: Duty[A], clue: => Any = "duties yield different results"): Future[Unit] = {
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
		PropF.forAllF { (x: Int, f: Int => Duty[Int]) =>
			val left = Duty.ready(x).flatMap(f)
			val right = f(x)
			checkEquality(left, right)
		}
	}

	// Monadic right identity law: m.flatMap(Duty.ready) == m
	test("Duty: right identity") {
		PropF.forAllF { (m: Duty[Int]) =>
			val left = m.flatMap(Duty.ready)
			val right = m
			checkEquality(left, right)
		}
	}

	// Monadic associativity law: m.flatMap(f).flatMap(g) == m.flatMap(x => f(x).flatMap(g))
	test("Duty: associativity") {
		PropF.forAllF { (m: Duty[Int], f: Int => Duty[Int], g: Int => Duty[Int]) =>
			val leftAssoc = m.flatMap(f).flatMap(g)
			val rightAssoc = m.flatMap(x => f(x).flatMap(g))
			checkEquality(leftAssoc, rightAssoc)
		}
	}

	// Functor: `m.map(f) == m.flatMap(a => ready(f(a)))`
	test("Duty: can be transformed with map") {
		PropF.forAllF { (m: Duty[Int], f: Int => String) =>
			val left = m.map(f)
			val right = m.flatMap(a => Duty.ready(f(a)))
			checkEquality(left, right)
		}
	}

	test("Duty: any pair of duties can be combined") {
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
		PropF.forAllF(Gen.choose(1, 5)) { (delay: Int) =>
			val schedule = doer.newDelaySchedule(delay)
			val startMilli = System.currentTimeMillis()
			val duty = Duty.schedule(schedule)(() => delay * 2)
				.map(x => assert(x == delay * 2 && System.currentTimeMillis() > startMilli + delay))
			duty.toFutureHardy()
		}
	}

	test("Duty: `Duty.schedule(newFixedRateSchedule)(supplier)` should execute both, the `supplier` and down-chained operations, repeatedly according to the specified specified period until cancellation") {
		PropF.forAllF(Gen.choose(1, 10), Gen.choose(1, 10)) { (initialDelay: Int, interval: Int) =>
			val repetitions = 10 - interval
			// println(s"\nBegin: initialDelay = $initialDelay, interval = $interval, repetitions = $repetitions")
			val schedule = newFixedRateSchedule(initialDelay, interval)
			val covenant = Covenant[Int]()
			val startMilli = System.currentTimeMillis()
			var counter: Int = 0
			val duty = Duty.schedule[Int](schedule)(() => counter)
				.andThen { supplierResult =>
					// println(s"supplierResult = $supplierResult/$repetitions")
					if supplierResult == repetitions then {
						cancel(schedule)
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
					assert(!isActive(schedule))
				}
				.toFutureHardy()
		}
	}

	test("Duty: `duty.scheduled(newDelaySchedule(delay))` should preserve the original duty's result and postpone its execution the specified `delay`") {
		PropF.forAllF(dutyArbitrary[Int].arbitrary, Gen.choose(1, 10)) { (duty: Duty[Int], testDelay: Int) =>
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
		PropF.forAllF(
			Gen.choose(1, 10),
			Gen.choose(1, 5),
			dutyArbitrary[Int].arbitrary
		) { (initialDelay: Int, interval: Int, duty: Duty[Int]) =>
			val repetitions = 5 - interval
			// println(s"\nBegin: initialDelay = $initialDelay, interval = $interval, repetitions = $repetitions")
			val schedule = newFixedDelaySchedule(initialDelay, interval)
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
					cancel(schedule)
				} else counter += 1
			}
			check.triggerAndForget()
			commitment.toFuture()
		}
	}

	test("Duty: `duty.scheduled(schedule)` should be cancellable when the schedule is active.") {
		PropF.forAllF(dutyArbitrary[Int].arbitrary, Gen.choose(1, 5)) { (duty: Duty[Int], delay: Int) =>
			val schedule = doer.newDelaySchedule(delay)
			val scheduledDuty = duty.scheduled(schedule)
			val commitment = Commitment[Unit]()
			scheduledDuty.trigger() { _ =>
				commitment.break(new AssertionError(s"The duty completed despite it was cancelled: isActive=${isActive(schedule)}"))()
			}
			val cancelsAndWaits = for {
				_ <- Duty.mine[Unit] { () =>
					assert(isActive(schedule))
					cancel(schedule)
					assert(!isActive(schedule))
				}
				_ <- Duty.delay(delay)(() => ())

			} yield ()
			commitment.completeWith(cancelsAndWaits.toTask)(x => println("was already completed with x"))
			commitment.toFuture()
		}
	}

	test("Duty: `duty.scheduled(schedule)` should be cancellable before the schedule is active.") {
		PropF.forAllF(dutyArbitrary[Int].arbitrary, Gen.choose(1, 5)) { (duty: Duty[Int], delay: Int) =>
			val schedule = doer.newDelaySchedule(delay)
			val scheduledDuty = duty.scheduled(schedule)
			val commitment = Commitment[Unit]()
			cancel(schedule)
			scheduledDuty.trigger() { _ =>
				commitment.break(new AssertionError(s"The duty completed despite it was cancelled: isActive=${isActive(schedule)}"))()
			}
			assert(!isActive(schedule))
			commitment.completeWith(Task.sleeps(delay + 1))()
			commitment.toFuture()
		}
	}


	test("Duty.scheduled: should compose correctly with other Duty operations") {
		PropF.forAllNoShrinkF { (duty: Duty[Int], delay: Int) =>
			def f(i:Int):String = i.toString.reverse
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


	////////// TASK /////////////

	// Custom equality for Task based on the result of attempt
	private def checkEquality[A](task1: Task[A], task2: Task[A]): Future[Unit] = {
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
		PropF.forAllF { (x: Int, f: Int => Task[Int]) =>
			val sx = Task.successful(x)
			val left = Task.successful(x).flatMap(f)
			val right = f(x)
			checkEquality(left, right)
		}
	}

	// Monadic right identity law: m.flatMap(Task.successful) == m
	test("Task: right identity") {
		PropF.forAllF { (m: Task[Int]) =>
			val left = m.flatMap(Task.successful)
			val right = m
			checkEquality(left, right)
		}
	}

	// Monadic associativity law: m.flatMap(f).flatMap(g) == m.flatMap(x => f(x).flatMap(g))
	test("Task: associativity") {
		PropF.forAllF { (m: Task[Int], f: Int => Task[Int], g: Int => Task[Int]) =>
			val leftAssoc = m.flatMap(f).flatMap(g)
			val rightAssoc = m.flatMap(x => f(x).flatMap(g))
			checkEquality(leftAssoc, rightAssoc)
		}
	}

	// Functor: `m.map(f) == m.flatMap(a => unit(f(a)))`
	test("Task: can be transformed with map") {
		PropF.forAllF { (m: Task[Int], f: Int => String) =>
			val left = m.map(f)
			val right = m.flatMap(a => Task.successful(f(a)))
			checkEquality(left, right)
		}
	}

	// Recovery: `failedTask.recover(f) == if f.isDefinedAt(e) then successful(f(e)) else failed(e)` where e is the exception thrown by failedTask
	test("Task: can be recovered from failure") {
		PropF.forAllF { (e: Throwable, f: PartialFunction[Throwable, Int]) =>
			if NonFatal(e) then {
				val leftTask = Task.failed[Int](e).recover(f)
				val rightTask = if f.isDefinedAt(e) then Task.successful(f(e)) else Task.failed(e);
				checkEquality(leftTask, rightTask)
			} else Future.successful(())
		}
	}

	test("Task: any can be combined") {
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

	private val sleep1ms = Task.alien { () =>
		Future {
			Thread.sleep(1)
		}(using global)
	}


	test("if a Task's transformation throws an exception the task should complete with that exception if it is non-fatal, or never complete if it is fatal.") {
		PropF.forAllF { (task: Task[Int], exception: Throwable) =>

			/** Do the test for a single operation */
			def check(operation: Task[Int] => Task[Int]): Future[Boolean] = {
				// Apply the operation to the random task. The `recover` is to ensure that the random task completes successfully in order for the operation to always be evaluated.
				val future = operation {
					task.recover { case cause => exception.getMessage.hashCode }
				}.toFutureHardy()

				// Build a task that completes with `true` as soon as the `exception` is found among the unhandled exceptions logged in the `unhandledExceptions` set; or `false` if it isn't found after 99 milliseconds.
				val exceptionWasNotHandled = sleep1ms
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
					val nonFatalWasHandled: Future[Boolean] = future.map {
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
					val fatalWasHandled = future.map { result =>
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
					   |withFilter: $withFilterTestResult
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
					   |withFilter: $withFilterTestResult
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