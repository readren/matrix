package readren.sequencer

import GeneratorsForDoerTests.{*, given}

import org.scalacheck.effect.PropF
import org.scalacheck.{Arbitrary, Gen, Prop}
import readren.common.Maybe

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Promise
import scala.reflect.ClassTag

/** Trait containing tests for [[Doer]] implementations extended with [[SchedulingExtension]].
 */
trait ScheduledMonoTests[D <: SchedulingDoer : ClassTag] { self: DoerProviderTestBase[D] =>

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
			val task = doer.Task_delays(delay)((_, delay * 2))
				.map { case (schedule, x) =>
					val actualDelay = System.nanoTime - startNano
					assert(x == delay * 2, s"found: $x, expected: ${x * 2}")
					assert(actualDelay >= delay * 1_000_000, s"actual: $actualDelay, expected: $delay, schedule: $schedule")
				}
			task.toFuture()
		}
	}

	//// Scheduling instance operations ////

	test("Scheduling Task: `task.delayed(delay)` should preserve the original task's result and postpone its execution the specified `delay`") {
		val generators = getGenerators
		import generators.*

		PropF.forAllNoShrinkF(genSuccessfulTask[Int](), Gen.choose(1, 10)) { (task: Task[Int], testDelay: Int) =>
			(for {
				directResult <- task
				startTime = System.currentTimeMillis()
				delayedResult <- task.delayed(testDelay)
			} yield {
				val actualDelay = System.currentTimeMillis() - startTime
				assertEquals(directResult, delayedResult)
				assert(actualDelay + 1 >= testDelay, s"Execution was not delayed enough. Expected at least ${testDelay}ms, got ${actualDelay}ms")
			}).toFuture()
		}
	}

	test("Scheduling Task: `task.delayed(delay)` should be cancellable after the schedule was activated.") {
		val generators = getGenerators
		import generators.*

		PropF.forAllNoShrinkF(genSuccessfulTask[Int](), Gen.choose(1, 5)) { (task: Task[Int], delay: Int) =>
			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			var wasCanceled = false
			var hasCompleted = false
			var maybeSchedule: Maybe[doer.Schedule] = Maybe.empty
			val scheduledTask: doer.TimedTask[Int] = task.delayed(delay).andOnSubscription { s =>
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

			} yield ()
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

			val timedTask = task.delayed(delay).andOnSubscription { schedule =>
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

	test("Scheduling Task.delayed: should compose correctly with other Task operations") {
		val generators = getGenerators
		import generators.*

		PropF.forAllNoShrinkF(genSuccessfulTask[Int](), smallIntGen, Gen.function1[Int, String](Gen.alphaLowerStr)) { (task: Task[Int], delay: Int, f: Int => String) =>
			val testDelay = Math.abs(delay % 5) + 1

			val scheduledMapped: Task[String] = task.delayed(testDelay).map(f)
			val mappedScheduled: Task[String] = task.map(f).delayed(testDelay)

			val scheduledFlatMapped: Task[String] = task.delayed(testDelay).flatMap(x => Task_ready(f(x)))
			val flatMappedScheduled: Task[String] = task.flatMap(x => Task_ready(f(x))).delayed(testDelay)

			val checks =
				for {
					_ <- Task_combine(scheduledMapped, mappedScheduled) { (a, b) =>
						assert(a == b, "delayed.map should equal map.delayed")
					}
					_ <- Task_combine(scheduledFlatMapped, flatMappedScheduled) { (scheduledFlat, flatMapped) =>
						assert(scheduledFlat == flatMapped, "delayed.flatMap should equal flatMap.delayed")
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
		val otherDoer = buildDoer("other")

		PropF.forAllNoShrinkF(
			Gen.nonEmptyListOf(for {
				schedule <- genSchedule(doer, maxDuration)
			} yield schedule),
			Gen.choose(1, maxDuration)
		) { (samples: List[doer.Schedule], cancelDelay: Int) =>
			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			var cancelAllWasCalled = false
			doer.run {
				for sample <- samples do {
					doer.schedule(sample) { s =>
						if cancelAllWasCalled then break(s"A schedule's routine was executed despite `cancelAll` was cancelled: schedule: $sample, isActive=${doer.wasActivated(sample)}")
					}
				}
			}

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
		val otherDoer = buildDoer("other")

		PropF.forAllNoShrinkF(
			Gen.choose(1, maxDelay),
			Gen.nonEmptyListOf(Gen.choose(1, maxDelay)),
			Gen.oneOf(true, false)
		) { (cancelDelay: Int, delays: List[Int], useCpuSaturator) =>
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
						if distanceBetweenCancellationAndExecutionInNanos > 500_000 then {
							val message = s"A schedule's routine was executed despite `cancelAll` was called: previousExecutionsCounter: $executionsAfterCancelAllCounter, distanceBetweenCancellationAndExecutionInMicros: ${distanceBetweenCancellationAndExecutionInNanos / 1_000}, delay: $delayMillis, cancelTime: $cancelNanoTime, schedule: $schedule, isActive=${doer.wasActivated(schedule)}"
							break(message)
						}
						executionsAfterCancelAllCounter += 1
					}
				}
			}

			otherDoer.schedule(otherDoer.newDelaySchedule(cancelDelay)) { cancelSchedule =>
				doer.cancelAll()
				cancelNanoTime = System.nanoTime()
				cancelAllWasCalled = true
				otherDoer.schedule(otherDoer.newDelaySchedule(maxDelay)) { checkSchedule =>
					promise.trySuccess(())
				}
			}

			saturationStopper.run()
			gate
		}
	}

	test("Task_schedules: The task returned by `Task_schedules(newDelaySchedule(delay))(body)` should execute `body` and yield its result once after the delay.") {
		val generators = getGenerators
		import generators.*
		PropF.forAllNoShrinkF(
			Gen.choose(1, 5)
		) { (expectedDelay: Int) =>
			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			val latch = new CountDownLatch(2)

			val startTime = System.nanoTime()
			val task = doer.Task_delays(expectedDelay) { s =>
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
}
