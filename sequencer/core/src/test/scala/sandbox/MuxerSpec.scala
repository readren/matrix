package readren.sequencer
package sandbox

import sandbox.DoerSandbox2.ExecutionSerial

import munit.ScalaCheckSuite
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll
import readren.common.Maybe

import scala.collection.mutable

class MuxerSpec extends ScalaCheckSuite {

	val sandbox = new DoerSandbox2 {
		override type Tag = String
		override val tag: Tag = "sandbox"

		override def executeSequentially(runnable: Runnable): Unit = runnable.run()

		override def currentExecutionSerial: ExecutionSerial = 0

		override def currentlyRunningDoer: Maybe[DoerSandbox2] = Maybe.empty
	}
	import sandbox.*

	class MockTarget[-A](val id: Int) {
		override def toString: String = s"Target($id)"
	}

	def Target_from(int: Int): MockTarget[Int] = new MockTarget[Int](int)

	class IntMuxer extends Muxer[Int, MockTarget] {
		override def addTarget(target: this.Target): Unit = super.addTarget(target)

		override def countAllMatching(target: this.Target): Int = super.countAllMatching(target)

		override def removeAllMatching(target: this.Target): Int = super.removeAllMatching(target)

		def foreachEntry(consumer: MockTarget[Int] => Unit): Unit = foreachTarget(consumer)
	}

	def samplesListGen(size: Int): Gen[List[MockTarget[Int]]] = {
		Gen.listOfN(size, Gen.choose(0, 1000).map(Target_from))
	}

	test("addTarget updates containment states and count properly") {
		val muxer = new IntMuxer()
		val firstTarget = Target_from(1)
		muxer.addTarget(firstTarget)
		assertEquals(muxer.countAllMatching(firstTarget), 1)

		val secondTarget = Target_from(2)
		muxer.addTarget(secondTarget)
		assertEquals(muxer.countAllMatching(secondTarget), 1)

		muxer.addTarget(firstTarget)
		assertEquals(muxer.countAllMatching(firstTarget), 2)

		assertEquals(muxer.removeAllMatching(secondTarget), 1)
		assertEquals(muxer.countAllMatching(secondTarget), 0)

		assertEquals(muxer.countAllMatching(firstTarget), 2)
		assertEquals(muxer.removeAllMatching(firstTarget), 2)
		assertEquals(muxer.countAllMatching(firstTarget), 0)
	}

	test("foreachTarget iterates strictly over populated elements, ignoring uninitialized padding") {
		val muxer = new IntMuxer()
		val items = List(Target_from(1), Target_from(2), Target_from(3))

		items.foreach(muxer.addTarget)

		var collected = List.empty[MockTarget[Int]]
		muxer.foreachEntry(target => collected = collected :+ target)

		assertEquals(collected, items)
	}

	test("removeAllMatching safely handles mutations without lingering or leaked references") {
		val muxer = new IntMuxer()
		val item1 = Target_from(1)
		val item2 = Target_from(2)
		val item3 = Target_from(3)

		muxer.addTarget(item1)
		muxer.addTarget(item2)
		muxer.addTarget(item3)

		val removedCount = muxer.removeAllMatching(item1)

		var remaining = List.empty[MockTarget[Int]]
		muxer.foreachEntry(target => remaining = remaining :+ target)

		assertEquals(removedCount, 1)
		assertEquals(muxer.countAllMatching(item1), 0)
		assertEquals(remaining, List(item2, item3))
	}

	property("Property-Based Invariant: Arbitrary registration sequences preserve exact tracking arrays") {
		forAll(samplesListGen(17)) { samples =>
			val muxer = new IntMuxer()

			samples.foreach(muxer.addTarget)

			var iteratedTargets = List.empty[MockTarget[Int]]
			muxer.foreachEntry(target => iteratedTargets = iteratedTargets :+ target)

			assertEquals(iteratedTargets, samples)
		}
	}

	property("Property-Based Invariant: Execution order perfectly tracks subscription sequences through expansion (>8) and random multi-removals") {
		val sampleDataGen = Gen.choose(15, 40).flatMap { size =>
			samplesListGen(size)
		}

		forAll(sampleDataGen, Gen.long) { (samples, seed) =>
			val muxer = new IntMuxer()

			samples.foreach(muxer.addTarget)

			var iteratedInitialTargets = List.empty[MockTarget[Int]]
			muxer.foreachEntry(target => iteratedInitialTargets = iteratedInitialTargets :+ target)

			assertEquals(iteratedInitialTargets, samples)

			val distinctTargets = samples.distinct

			val random = new scala.util.Random(seed)
			val targetsShuffled = random.shuffle(distinctTargets)

			var totalRemovedCount = 0
			targetsShuffled.foreach { targetObserver =>
				val matchingCount = muxer.countAllMatching(targetObserver)
				val removedCount = muxer.removeAllMatching(targetObserver)
				assertEquals(matchingCount, removedCount)
				totalRemovedCount += removedCount

				var remainingCount = 0
				val remainingBuffer = mutable.Buffer.empty[MockTarget[Int]]
				muxer.foreachEntry { target =>
					remainingBuffer.addOne(target)
					remainingCount += 1
				}
				assertEquals(remainingCount, samples.size - totalRemovedCount)
			}

			var finalCount = 0
			muxer.foreachEntry(_ => finalCount += 1)

			assertEquals(totalRemovedCount, samples.size)
			assert(finalCount == 0)
		}
	}
}
