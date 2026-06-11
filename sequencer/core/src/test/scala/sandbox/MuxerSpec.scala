package readren.sequencer
package sandbox

import munit.ScalaCheckSuite
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

import scala.collection.mutable

class MuxerSpec extends ScalaCheckSuite {

	val sandbox = new DoerSandbox2

	import sandbox.*

	type Observer[T] = List[T]

	def Observer_from(int: Int): Observer[Int] = List(int)

	class IntMuxer extends Muxer[Int, Observer]

	def keyGen(universeSize: Int): Gen[String] = Gen.choose(0, universeSize - 1).map(offset => ('A' + offset).toChar.toString)

	def samplesListGen(size: Int, keysUniverseSize: Int): Gen[List[(key: Key, observer: Observer[Int], isKeyed: Boolean)]] = {
		for keys <- Gen.listOfN(size, keyGen(keysUniverseSize)) yield {
			for (key, index) <- keys.zipWithIndex yield {
				val observer = Observer_from(index)
				(key, observer, key(0) % 2 == 0)
			}
		}
	}

	def Entry_fromSample[M <: Muxer[Int, Observer]](muxer: M)(sample: (key: Key, observer: Observer[Int], isKeyed: Boolean)): muxer.Entry =
		if sample.isKeyed then (sample.key, sample.observer) else sample.observer: muxer.Entry

	private def extractObserver(muxer: Muxer[Int, Observer])(entry: muxer.Entry): Observer[Int] = entry match {
		case obs: Observer[Int] => obs
		case (_, obs: Observer[Int]) => obs
	}

	test("addEntry updates containment states and count properly") {
		val muxer = new IntMuxer()
		val firstObs = Observer_from(1)
		muxer.addEntry(firstObs)
		assertEquals(muxer.countAllMatching(firstObs), 1)

		val secondObs = Observer_from(2)
		muxer.addEntry(("x", secondObs))
		assertEquals(muxer.countAllMatching(secondObs), 0)
		assertEquals(muxer.countAllMatching("x"), 1)

		muxer.addEntry(firstObs)
		assertEquals(muxer.countAllMatching(firstObs), 2)

		muxer.addEntry(("x", secondObs))
		assertEquals(muxer.countAllMatching("x"), 2)
		assertEquals(muxer.countAllMatching(secondObs), 0)

		assertEquals(muxer.removeAllMatching(secondObs), 0)
		assertEquals(muxer.removeAllMatching("x"), 2)
		assertEquals(muxer.countAllMatching("x"), 0)

		assertEquals(muxer.countAllMatching(firstObs), 2)
		assertEquals(muxer.removeAllMatching(firstObs), 2)
		assertEquals(muxer.countAllMatching(firstObs), 0)
	}

	test("foreachEntry iterates strictly over populated elements, ignoring uninitialized padding") {
		val muxer = new IntMuxer()
		val items = List(Observer_from(1), Observer_from(2), Observer_from(3))

		items.foreach(muxer.addEntry)

		var collected = List.empty[Observer[Int]]
		muxer.foreachEntry(obs => collected = collected :+ obs)

		assertEquals(collected, items)
	}

	test("removeAllMatching safely handles mutations without lingering or leaked references") {
		val muxer = new IntMuxer()
		val item1 = Observer_from(1)
		val item2 = Observer_from(2)
		val item3 = Observer_from(3)

		muxer.addEntry(item1)
		muxer.addEntry(item2)
		muxer.addEntry(item3)

		val removedCount = muxer.removeAllMatching(item1)

		var remaining = List.empty[Observer[Int]]
		muxer.foreachEntry(obs => remaining = remaining :+ obs)

		assertEquals(removedCount, 1)
		assertEquals(muxer.countAllMatching(item1), 0)
		assertEquals(remaining, List(item2, item3))
	}

	property("Property-Based Invariant: Arbitrary registration sequences preserve exact tracking arrays") {
		forAll(samplesListGen(17, 9)) { samples =>
			val muxer = new IntMuxer()

			val entries = samples.map(Entry_fromSample[muxer.type](muxer))
			val expectedObservers = entries.map(extractObserver(muxer))

			entries.foreach(muxer.addEntry)

			var iteratedObservers = List.empty[Observer[Int]]
			muxer.foreachEntry(obs => iteratedObservers = iteratedObservers :+ obs)

			assertEquals(iteratedObservers, expectedObservers)
		}
	}

	property("Property-Based Invariant: Execution order perfectly tracks subscription sequences through expansion (>8) and random multi-removals") {
		// Using Gen.flatMap to generate perfectly sized test fixtures without cropping
		val sampleDataGen = Gen.choose(15, 40).flatMap { size =>
			samplesListGen(size, size / 2)
		}

		forAll(sampleDataGen, Gen.long) { (samples, seed) =>
			scribe.info(s"Begin: samples=$samples")
			val muxer = new IntMuxer()
			// Construct unique entries based on the exact dynamic size bounds
			val entries = samples.map(Entry_fromSample[muxer.type](muxer))

			entries.foreach(muxer.addEntry)

			val expectedInitialObservers = entries.map(extractObserver(muxer))
			var iteratedInitialObservers = List.empty[Observer[Int]]
			muxer.foreachEntry(obs => iteratedInitialObservers = iteratedInitialObservers :+ obs)

			assertEquals(iteratedInitialObservers, expectedInitialObservers)

			// Obtain all the distinct EntryId instances in the sample data.
			val entriesIds = samples.map[muxer.EntryId] { sample =>
				if sample.isKeyed then sample.key else sample.observer
			}.distinct

			val random = new scala.util.Random(seed)
			val entriesIdsShuffled = random.shuffle(entriesIds)

			var totalRemovedCount = 0
			// Perform random multi-removals
			entriesIdsShuffled.foreach { targetEntryId =>

				val matchingCount = muxer.countAllMatching(targetEntryId)
				val removedCount = muxer.removeAllMatching(targetEntryId)
				assertEquals(matchingCount, removedCount, s"Muxer matching count vs removed count mismatch for EntryId=$targetEntryId: matching=$matchingCount, removedCount=$removedCount")
				totalRemovedCount += removedCount

				var remainingCount = 0
				val remainingBuffer = mutable.Buffer.empty[muxer.Entry]
				muxer.foreachEntry { observer =>
					remainingBuffer.addOne(observer)
					remainingCount += 1
				}
				assertEquals(remainingCount, samples.size - totalRemovedCount, s"Muxer remaining count vs expected count mismatch for ID=$targetEntryId: remainingCount=$remainingCount, expectedCount=${samples.size - totalRemovedCount}")
				scribe.info(s"After removing EntryId=$targetEntryId, the remaining observers are $remainingBuffer")
			}

			var finalCount = 0
			muxer.foreachEntry(_ => finalCount += 1)

			assertEquals(totalRemovedCount, samples.size)
			assert(finalCount == 0)
		}
	}
}
