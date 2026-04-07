package readren.sequencer

import GeneratorsForDoerTests.{*, given}
import munit.ScalaCheckEffectSuite
import org.scalacheck.effect.PropF
import org.scalacheck.{Arbitrary, Gen, Prop}
import readren.common.{Maybe, ScribeConfig}

import scala.compiletime.uninitialized
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag
import scala.util.control.NonFatal

/** Abstract test suite for testing [[ResultIncrementalCoalescing]].
 *
 * This suite checks if the primitive respects its convergence properties.
 *
 * @tparam D The type of Doer being tested.
 */
abstract class ResultIncrementalCoalescingTest[D <: Doer & SchedulingExtension & LoopingExtension : ClassTag] extends ScalaCheckEffectSuite {

	type DP <: DoerProvider[D]

	private var sharedDoerProvider: DP = uninitialized
	private var sharedDoer: D = uninitialized
	private var sharedGenerators: GeneratorsForDoerTests[D] = uninitialized

	/** The implementation should build an instance of the [[DoerProvider]] implementation under test. */
	protected def buildDoerProvider: DP

	/** The implementation should release the specified [[DoerProvider]]. */
	protected def releaseDoerProvider(doerProvider: DP): Unit

	//// Suite lifecycle ////

	override val munitTimeout: Duration = scala.concurrent.duration.Duration(240, "seconds")

	override def beforeAll(): Unit = {
		ScribeConfig.init(deleteLogFilesOnLaunch = true)

		val sharedDoerProvider = buildDoerProvider
		this.sharedDoerProvider = sharedDoerProvider
		val sharedDoer = sharedDoerProvider.provide(sharedDoerProvider.tagFromText("mc-main-doer"))
		this.sharedDoer = sharedDoer
		val sharedGenerators = GeneratorsForDoerTests(sharedDoer, sharedDoerProvider)
		this.sharedGenerators = sharedGenerators
	}

	override def afterAll(): Unit = {
		releaseDoerProvider(sharedDoerProvider)
	}

	//// Shared instance's getters ////

	private def getSharedDoerProvider: DP = sharedDoerProvider

	private def getSharedDoer: D = sharedDoer

	private def getGenerators: GeneratorsForDoerTests[D] = sharedGenerators

	/** Breaks the `promise` if it wasn't already completed. */
	private def break[P](message: String)(using promise: Promise[P]): Unit =
		promise.tryFailure(new AssertionError(message))

	private final def gate[P](using promise: Promise[P]): Future[P] = {
		promise.future.map { result => result }(using scala.concurrent.ExecutionContext.Implicits.global)
	}

	test("MonotonicConvergence - first contender starts competition and wins") {
		val generators = getGenerators
		import generators.*
		PropF.forAllNoShrinkF {
			for {
				expectedResult <- intGen
				contender <- genDuty(expectedResult)
			} yield (expectedResult, doer.Covenant_triggerAndWire(contender): doer.LatchingDuty[Int])
		} { case (expectedResult, contender) =>

			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			doer.run {
				val mc = new ResultIncrementalCoalescing[Int, doer.type](doer)
				val resultDuty = mc.contend { maybeIncumbent =>
					maybeIncumbent.fold {
						contender
					} { _ =>
						break("First contender should see empty incumbent")
						contender
					}
				}
				resultDuty.subscribe { result =>
					if result == expectedResult then promise.trySuccess(())
					else promise.tryFailure(new AssertionError(s"Expected 42, got $result"))
				}
			}
			gate
		}
	}

	test("MonotonicConvergence - second contender supersedes the first") {
		val generators = getGenerators
		import generators.*
		PropF.forAllNoShrinkF {
			for {
				expectedResult1 <- intGen
				expectedResult2 <- intGen
				firstContender0 <- genDuty(expectedResult1)
				secondContender0 <- genDuty(expectedResult2)
			} yield (
				expectedResult2,
				doer.Covenant_triggerAndWire(firstContender0): doer.LatchingDuty[Int],
				doer.Covenant_triggerAndWire(secondContender0): doer.LatchingDuty[Int]
			)
		} { case (expectedResult2, firstContender, secondContender) =>
			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			doer.run {
				val mc = new ResultIncrementalCoalescing[Int, doer.type](doer)

				val firstResultDuty = mc.contend { _ => firstContender }

				val secondResultDuty = mc.contend { maybeIncumbent =>
					maybeIncumbent.fold {
						secondContender
					} { incumbent =>
						if incumbent ne firstContender then break("Incumbent should be the first covenant")
						secondContender
					}
				}

				secondResultDuty.subscribe { result =>
					if result == expectedResult2 then promise.trySuccess(())
					else break(s"Expected $expectedResult2, got $result")
				}
			}

			gate
		}
	}

	test("MonotonicConvergence - second contender yields to the first") {
		val generators = getGenerators
		import generators.*
		PropF.forAllNoShrinkF {
			for {
				expectedResult1 <- intGen
				firstContender0 <- genDuty(expectedResult1)
			} yield (
				expectedResult1,
				doer.Covenant_triggerAndWire(firstContender0): doer.LatchingDuty[Int]
			)
		} { case (expectedResult1, firstContender) =>
			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			doer.run {
				val mc = new ResultIncrementalCoalescing[Int, doer.type](doer)

				// To test yielding, the FIRST contender must be evaluated and complete on its own,
				// so we use the generated one. The second contender yields simply by returning the incumbent.
				// However, if the first already completed, yielding is technically starting a new competition!
				// But yielding returning the incumbent means returning the one passed in `maybeIncumbent`.
				// To guarantee the competition doesn't close before `mc.contend` executes, we delay `mc.contend(2)`
				// OR we make sure firstContender is delayed. Actually, we can just use `trySuccess` if the result matches!
				val firstResultDuty = mc.contend { _ => firstContender }

				val secondResultDuty = mc.contend { maybeIncumbent =>
					maybeIncumbent.fold {
						firstContender
					} { incumbent =>
						incumbent
					}
				}

				secondResultDuty.subscribe { result =>
					if result == expectedResult1 then promise.trySuccess(())
					else break(s"Expected $expectedResult1, got $result")
				}
			}

			gate
		}
	}

	test("MonotonicConvergence - new competition starts after previous completes") {
		val generators = getGenerators
		import generators.*
		PropF.forAllNoShrinkF {
			for {
				expectedResult1 <- intGen
				expectedResult2 <- intGen
				firstContender0 <- genDuty(expectedResult1)
				secondContender0 <- genDuty(expectedResult2)
			} yield (
				expectedResult1,
				doer.Covenant_triggerAndWire(firstContender0): doer.LatchingDuty[Int],
				expectedResult2,
				doer.Covenant_triggerAndWire(secondContender0): doer.LatchingDuty[Int]
			)
		} { case (expectedResult1, firstContender, expectedResult2, secondContender) =>
			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			doer.run {
				val mc = new ResultIncrementalCoalescing[Int, doer.type](doer)

				val firstResultDuty = mc.contend { _ => firstContender }

				firstResultDuty.subscribe { _ =>
					doer.run {
						val newResultDuty = mc.contend { maybeIncumbent =>
							maybeIncumbent.fold {
								secondContender
							} { _ =>
								break("Should start a new competition after previous completed")
								secondContender
							}
						}

						newResultDuty.subscribe { result =>
							if result == expectedResult2 then promise.trySuccess(())
							else break(s"Expected $expectedResult2, got $result")
						}
					}
				}
			}

			gate
		}
	}
}
