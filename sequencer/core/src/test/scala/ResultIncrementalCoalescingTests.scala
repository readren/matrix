package readren.sequencer

import GeneratorsForDoerTests.{*, given}
import org.scalacheck.Gen
import org.scalacheck.effect.PropF
import readren.common.Maybe

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag

/** Trait containing tests for [[ResultIncrementalCoalescing]].
 *
 * This trait checks if the primitive respects its convergence properties.
 *
 * @tparam D The type of Doer being tested.
 */
trait ResultIncrementalCoalescingTests[D <: Doer : ClassTag] { self: DoerProviderTestBase[D] =>

	test("ResultIncrementalCoalescing - first contender starts competition and wins") {
		val generators = getGenerators
		import generators.*
		PropF.forAllNoShrinkF {
			for {
				successfulResult <- smallIntGen
				result <- genTryFrom(successfulResult, "expected-result", 50)
				contender <- genTaskFrom(result)
			} yield (successfulResult, result, doer.Captor_triggerAndWire(contender): doer.Capture[Int])
		} { (successfulResult, expectedResult, contender) =>

			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			doer.run {
				val ric = new ResultIncrementalCoalescing[Int, doer.type](doer)
				val resultCapture = ric.contend { maybeIncumbent =>
					if maybeIncumbent.isDefined then break("First contender should see empty incumbent")
					contender
				}
				resultCapture.triggerSyncCallbacks(
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

	test("ResultIncrementalCoalescing - second contender supersedes the first") {
		val generators = getGenerators
		import generators.*

		PropF.forAllNoShrinkF {
			for {
				contenderATask <- genTask[Int]()
				expectedResultB <- genTry[Int]
				contenderB <- genCaptureFrom(expectedResultB)
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
				val firstResultCapture = ric.contend { maybeIncumbent =>
					if maybeIncumbent.isDefined then break("First contender should see empty incumbent")
					contenderA
				}
				val secondResultCapture = ric.contend { maybeIncumbent =>
					if maybeIncumbent.fold(true)(_ ne contenderA) then break("Second contender should see contenderA as incumbent")
					contenderB
				}
				if secondResultCapture ne firstResultCapture then break(s"The returned Captor is not stable")

				if bool then contenderA.seizeWithSync(contenderATask)
				secondResultCapture.triggerSyncCallbacks(
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

	test("ResultIncrementalCoalescing - second contender yields to the first") {
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
				val firstResultCapture = ric.contend { maybeIncumbent =>
					if maybeIncumbent.isDefined then break("First contender should see empty incumbent")
					contenderA
				}

				val secondResultCapture = ric.contend { maybeIncumbent =>
					maybeIncumbent.fold {
						break("Second contender should see contenderA as incumbent")
						Keeper(0)
					}(identity)
				}
				if secondResultCapture ne firstResultCapture then break(s"The returned Captor is not stable")

				if bool then contenderA.seizeWithSync(contenderATask)
				secondResultCapture.triggerSyncCallbacks(
					actualResultA => {
						if expectedResultA.fold(_ => true, _ != actualResultA) then break(s"Expected $expectedResultA, got Success($actualResultA)")
						else promise.trySuccess(())
					}, actualErrorA => {
						if expectedResultA.fold(_ ne actualErrorA, _ => true) then break(s"Expected $expectedResultA, got Failure($actualErrorA)")
						else promise.trySuccess(())
					}
				)
				if !bool then contenderA.seizeWithSync(contenderATask)
			}

			gate
		}
	}

	test("ResultIncrementalCoalescing - new competition starts after previous completes") {
		val generators = getGenerators
		import generators.*
		PropF.forAllNoShrinkF {
			for {
				contenderA <- genCapture[Int]()
				expectedResultB <- genTry[Int]
				contenderB <- genCaptureFrom(expectedResultB)
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

				val firstResultCapture = ric.contend { maybeIncumbent =>
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
