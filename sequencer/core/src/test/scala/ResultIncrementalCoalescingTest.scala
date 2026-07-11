package readren.sequencer

import GeneratorsForDoerTests.{*, given}

import munit.ScalaCheckEffectSuite
import org.scalacheck.Gen
import org.scalacheck.effect.PropF
import readren.common.{Maybe, ScribeConfig}

import scala.compiletime.uninitialized
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag

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
				successfulResult <- smallIntGen
				result <- genTryFrom(successfulResult, "expected-result", 50)
				contender <- genTaskFrom(result)
			} yield (successfulResult, result, doer.Captor_triggerAndWire(contender): doer.Capturer[Int])
		} { (successfulResult, expectedResult, contender) =>

			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			doer.run {
				val ric = new ResultIncrementalCoalescing[Int, doer.type](doer)
				val resultCapturer = ric.contend { maybeIncumbent =>
					if maybeIncumbent.isDefined then break("First contender should see empty incumbent")
					contender
				}
				resultCapturer.triggerSyncCallbacks(
					actualResult => {
						if actualResult == successfulResult then promise.trySuccess(())
						else break(s"Expected $successfulResult, got $actualResult")
					}, actualError => {
						if expectedResult.fold(_ eq actualError, _ => false) then promise.trySuccess(())
						else break(s"Expected $expectedResult, got Failure($actualError)")
					}
				)
			}
			gate
		}
	}

	test("MonotonicConvergence - second contender supersedes the first") {
		val generators = getGenerators
		import generators.*

		PropF.forAllNoShrinkF {
			for {
				contenderATask <- genTask[Int]()
				expectedResultB <- genTry[Int]
				contenderB <- genCapturerFrom(expectedResultB)
				bool <- Gen.oneOf(true, false)
			} yield (
				contenderATask,
				expectedResultB,
				contenderB,
				bool
			)
		} { case (contenderATask, expectedResultB, contenderB, bool) =>
			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			doer.run {
				val ric = new ResultIncrementalCoalescing[Int, doer.type](doer)

				val contenderA = new Captor[Int]()
				val firstResultCapturer = ric.contend { maybeIncumbent =>
					if maybeIncumbent.isDefined then break("First contender should see empty incumbent")
					contenderA
				}
				val secondResultCapturer = ric.contend { maybeIncumbent =>
					if maybeIncumbent.fold(true)(_ ne contenderA) then break("Second contender should see contenderA as incumbent")
					contenderB
				}
				if secondResultCapturer ne firstResultCapturer then break(s"The returned Captor is not stable")

				if bool then contenderA.seizeWithSync(contenderATask)
				secondResultCapturer.triggerSyncCallbacks(
					actualResultB => {
						if expectedResultB.fold(_ => true, _ != actualResultB) then break(s"expected $expectedResultB, got Success($actualResultB)")
						else promise.trySuccess(())
					}, actualErrorB => {
						if expectedResultB.fold(_ ne actualErrorB, _ => true) then break(s"expected $expectedResultB, got Failure($actualErrorB)")
						else promise.trySuccess(())
					}
				)
				if !bool then contenderA.seizeWithSync(contenderATask)
			}
			gate
		}
	}

	test("MonotonicConvergence - second contender yields to the first") {
		val generators = getGenerators
		import generators.*
		PropF.forAllNoShrinkF {
			for {
				expectedResultA <- genTry[Int]
				contenderATask <- genTaskFrom(expectedResultA)
				bool <- Gen.oneOf(true, false)
			} yield (
				expectedResultA,
				contenderATask,
				bool
			)
		} { (expectedResultA, contenderATask, bool) =>
			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			doer.run {
				val ric = new ResultIncrementalCoalescing[Int, doer.type](doer)

				val contenderA = new Captor[Int]()
				val firstResultCapturer = ric.contend { maybeIncumbent =>
					if maybeIncumbent.isDefined then break("First contender should see empty incumbent")
					contenderA
				}

				val secondResultCapturer = ric.contend { maybeIncumbent =>
					maybeIncumbent.fold {
						break("Second contender should see contenderA as incumbent")
						Keeper(0)
					}(identity)
				}
				if secondResultCapturer ne firstResultCapturer then break(s"The returned Captor is not stable")

				if bool then contenderA.seizeWithSync(contenderATask)
				secondResultCapturer.triggerSyncCallbacks(
					actualResult => {
						if expectedResultA.fold(_ => true, _ != actualResult) then break(s"Expected $expectedResultA, got Success($actualResult)")
						else promise.trySuccess(())
					}, actualError => {
						if expectedResultA.fold(_ ne actualError, _ => true) then break(s"Expected $expectedResultA, got Failure($actualError)")
						else promise.trySuccess(())
					}
				)
				if !bool then contenderA.seizeWithSync(contenderATask)
			}

			gate
		}
	}

	test("MonotonicConvergence - new competition starts after previous completes") {
		val generators = getGenerators
		import generators.*
		PropF.forAllNoShrinkF {
			for {
				contenderA <- genCapturer[Int]()
				expectedResultB <- genTry[Int]
				contenderB <- genCapturerFrom(expectedResultB)
			} yield (
				contenderA,
				expectedResultB,
				contenderB
			)
		} { (contenderA, expectedResultB, contenderB) =>
			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			doer.run {
				val ric = new ResultIncrementalCoalescing[Int, doer.type](doer)

				val firstResultCapturer = ric.contend { maybeIncumbent =>
					if maybeIncumbent.isDefined then break("First contender should see empty incumbent")
					contenderA
				}

				contenderA.subscribe(true) {
					_ => {
						val newResultTask = ric.contend(
							maybeIncumbent => {
								if maybeIncumbent.isDefined then break("Should start a new competition after previous completed")
								contenderB
							}
						)

						newResultTask.subscribeCallbacks(true)(
							actualResultB => {
								if expectedResultB.fold(_ => false, _ == actualResultB) then promise.trySuccess(())
								else break(s"Expected $expectedResultB, got $actualResultB")
							},
							actualErrorB => {
								if expectedResultB.fold(_ eq actualErrorB, _ => false) then promise.trySuccess(())
								else break(s"Expected $expectedResultB, got $actualErrorB")
							}
						)
					}
				}
			}

			gate
		}
	}
}
