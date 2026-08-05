package readren.sequencer

import GeneratorsForDoerTests.{*, given}
import SchedulingExtension.{DELAY, FIXED_DELAY, FIXED_RATE}

import org.scalacheck.effect.PropF
import org.scalacheck.{Arbitrary, Gen, Prop}
import readren.common.Maybe

import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag

/** Trait containing tests for [[Doer]] implementations extended with [[SchedulingDoerFluxPart]].
 */
trait ScheduledFluxTests[D <: SchedulingDoer : ClassTag] { self: DoerProviderTestBase[D] =>

	test("Scheduled Flux: `Flux_schedules(FIXED_RATE, ...)(supplier)` should execute both, the `supplier` and down-chained operations, repeatedly according to the specified period until cancellation") {
		val generators = getGenerators
		import generators.*

		PropF.forAllNoShrinkF(Gen.choose(1, 10), Gen.choose(1, 10)) { (initialDelay: Int, interval: Int) =>
			val repetitions = 10 - interval
			val promise = Promise[(doer.TimedSubscription, Int)]()

			given Promise[(doer.TimedSubscription, Int)] = promise

			val startMilli = System.currentTimeMillis()
			var counter: Int = 0
			doer.Flux_schedules(FIXED_RATE, initialDelay, interval)((_, counter))
				.subscribe(false)(new doer.FluxObserver[(doer.TimedSubscription, Int)] {
					override def onNext(elem: (doer.TimedSubscription, Int), index: Int): Unit = {
						val (timedSub, supplierResult) = elem
						if !doer.wasActivated(timedSub.schedule) then break("The `wasActivated` method returned false for a schedule that was activated")
						if supplierResult == repetitions then {
							timedSub.unsubscribeSync()
							promise.trySuccess((timedSub, supplierResult))
						} else if supplierResult > repetitions then {
							break("The supplier was execute despite the schedule was canceled in the previous supplier's execution.")
						} else counter += 1
					}

					override def onError(ex: Throwable): Unit = break(s"Unexpected error: $ex")

					override def onComplete(): Unit = if !promise.isCompleted then break(s"Unexpected completion")
				})
			promise.future.map { case (timedSub, supplyResult) =>
				val actualDelay = System.currentTimeMillis() - startMilli
				val expectedDelay = interval * repetitions + initialDelay
				assertEquals(supplyResult, repetitions)
				assert(actualDelay + 1 >= expectedDelay)
				assert(doer.isCanceled(timedSub.schedule))
			}
		}
	}

	test("Scheduled Flux: The `TimedFlux` returned by `Flux_schedules(newFixedRateSchedule(initialDelay, interval))(body)` should execute `body` and yield its result repeatedly after the instants determined by the schedule.") {
		val generators = getGenerators
		val REPETITIONS = 4
		var testExecutionsCounter = 0
		import generators.*
		PropF.forAllNoShrinkF(
			Gen.choose(-1, 50),
			Gen.choose(1, 50)
		) { (expectedInitialDelay: Int, expectedPeriod: Int) =>
			print(f"\nBegin: initialDelay=$expectedInitialDelay%3d, period=$expectedPeriod%3d ")

			val EXECUTION_DELAY_MARGIN_MILLIS = if testExecutionsCounter < 5 then 100 else 50
			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			val latch = new CountDownLatch(REPETITIONS)

			@volatile var executionsCounter = 0
			System.gc()
			val startTime = System.nanoTime()
			val timedFlux = doer.Flux_schedules(FIXED_RATE, expectedInitialDelay, expectedPeriod) { ts =>
				val actualDurationNanos = System.nanoTime() - startTime
				val expectedDurationMillis = expectedInitialDelay + executionsCounter * expectedPeriod
				val differenceMicros = actualDurationNanos / 1000 - expectedDurationMillis * 1000
				print(f"| tickIndex=$executionsCounter%3d, difference=$differenceMicros%9d micros")
				if differenceMicros < 0 then break(s"The #$executionsCounter execution occurred sooner than expected")
				else if differenceMicros > EXECUTION_DELAY_MARGIN_MILLIS * 1_000 then break(s"The #$executionsCounter execution occurred later than expected after $testExecutionsCounter successful tests")
				latch.countDown()
				executionsCounter += 1
			}
			val subscription = timedFlux.subscribeAndForget(false)
			if latch.await(expectedInitialDelay + expectedPeriod * REPETITIONS + EXECUTION_DELAY_MARGIN_MILLIS, TimeUnit.MILLISECONDS) then promise.trySuccess(())
			else break(s"The number of executions ($executionsCounter) within the provided time is less than the expected ($REPETITIONS): initialDelay=$expectedInitialDelay, period=$expectedPeriod")
			subscription.unsubscribe()
			testExecutionsCounter += 1
			gate
		}
	}
}
