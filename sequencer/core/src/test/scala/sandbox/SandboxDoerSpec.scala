package readren.sequencer
package sandbox

import munit.ScalaCheckEffectSuite
import org.scalacheck.Prop
import readren.common.Maybe

object TestSandboxDoer extends SandboxDoer

class SandboxDoerSpec extends ScalaCheckEffectSuite {
	import TestSandboxDoer.*

	private def makeTask[A](value: A): Task[A] = new Task[A] {
		override def subscribe(onComplete: Consumer[A]): Unit = onComplete(value)
	}

	test("Keyed subscription lifecycle and auto-unsubscribe") {
		val capturer = new Captor[Int]()
		val array = new CaptorArray[Int](IArray(capturer))
		val key1 = new AnyRef()
		var callCount = 0
		var lastValue = -1

		// Subscribe with key1
		array.subscribe({ (v, index) =>
			callCount += 1
			lastValue = v
		}, key1)

		// Complete the capturer
		capturer.capture(42)
		assertEquals(callCount, 1)
		assertEquals(lastValue, 42)

		// Unsubscribe with key1
		array.unsubscribe(key1)

		// Complete with another value (though Captor is single-write, we can test with a new one)
		val capturer2 = new Captor[Int]()
		val array2 = new CaptorArray[Int](IArray(capturer2))
		var callCount2 = 0

		array2.subscribe((_, _) => callCount2 += 1, key1)
		array2.unsubscribe(key1)
		capturer2.capture(100)
		assertEquals(callCount2, 0) // unsubscribed, should not be called
	}

	test("Auto-unsubscribe on duplicate key subscription") {
		val capturer = new Captor[Int]()
		val array = new CaptorArray[Int](IArray(capturer))
		val key = new AnyRef()
		var count1 = 0
		var count2 = 0

		array.subscribe((_, _) => count1 += 1, key)
		array.subscribe((_, _) => count2 += 1, key) // should unsubscribe the first one

		capturer.capture(99)
		assertEquals(count1, 0)
		assertEquals(count2, 1)
	}

	test("FlatMappedCapturerMatrix - maybeResult and lazy subscription propagation") {
		val captorOuter = new Captor[Int]()
		val arrayOuter = new CaptorArray[Int](IArray(captorOuter))

		val captorInner1 = new Captor[String]()
		val arrayInner1 = new CaptorArray[String](IArray(captorInner1))

		val matrix = arrayOuter.flatMap[String] {
			case 1 => arrayInner1
			case _ => new KeeperArray[String](IArray("other"))
		}

		// Before completion: maybeResult(0, 0) should be empty
		assertEquals(matrix.maybeResult(0, 0), Maybe.empty)

		// Complete outer
		captorOuter.capture(1)
		// Inner not completed yet, should be empty
		assertEquals(matrix.maybeResult(0, 0), Maybe.empty)

		// Complete inner
		captorInner1.capture("hello")
		// Now it should return Maybe("hello")
		assertEquals(matrix.maybeResult(0, 0), Maybe("hello"))
	}

	test("FlattenedToSequentialArray - independent sequence per key subscription") {
		val captor1 = new Captor[Int]()
		val captor2 = new Captor[Int]()
		val matrix = new FlatMappedCapturerMatrix[Int, Int](
			new CaptorArray[Int](IArray(captor1)),
			x => new CaptorArray[Int](IArray(captor2))
		)
		val seqArray = matrix.flattenToSequential

		var sub1List = List[(Int, Int)]()
		var sub2List = List[(Int, Int)]()

		val key1 = new AnyRef()
		val key2 = new AnyRef()

		seqArray.subscribe((v, idx) => sub1List = sub1List :+ (v, idx), key1)
		seqArray.subscribe((v, idx) => sub2List = sub2List :+ (v, idx), key2)

		// Complete cell 1
		captor1.capture(10)
		captor2.capture(20)

		// Both should receive value 20 with sequential index 0
		assertEquals(sub1List, List((20, 0)))
		assertEquals(sub2List, List((20, 0)))
	}

	test("FlattenedToInnerArray and FlattenedToOuterArray coordinates") {
		val captorOuter = new Captor[Int]()
		val captorInner = new Captor[String]()
		val matrix = new FlatMappedCapturerMatrix[Int, String](
			new CaptorArray[Int](IArray(captorOuter)),
			x => new CaptorArray[String](IArray(captorInner))
		)

		var innerCoords = List[(String, Int)]()
		var outerCoords = List[(String, Int)]()

		matrix.flattenToInner.subscribe((v, idx) => innerCoords = innerCoords :+ (v, idx))
		matrix.flattenToOuter.subscribe((v, idx) => outerCoords = outerCoords :+ (v, idx))

		captorOuter.capture(1)
		captorInner.capture("a")

		assertEquals(innerCoords, List(("a", 0))) // innerIndex is 0
		assertEquals(outerCoords, List(("a", 0))) // outerIndex is 0
	}

	test("Property-based test: MappedKeyedCapturerArray correctness") {
		Prop.forAll { (nums: List[Int]) =>
			val keeperArray = new KeeperArray(IArray.from(nums))
			val mapped = keeperArray.map(_ * 2)

			var collected = List[Int]()
			mapped.subscribe((v, idx) => collected = collected :+ v)

			collected == nums.map(_ * 2)
		}
	}

	test("StreamEmitter basic emissions, completion, and error") {
		val emitter = new StreamEmitter[Int]()
		var collected = List[Int]()
		var completed = false
		var error: Option[Throwable] = None

		emitter.subscribe(
			onNext = (v, idx) => collected = collected :+ v,
			onError = ex => error = Some(ex),
			onComplete = () => completed = true
		)

		emitter.emit(1)
		emitter.emit(2)
		assertEquals(collected, List(1, 2))
		assertEquals(completed, false)

		emitter.end()
		assertEquals(completed, true)
		assertEquals(error, None)

		// Subsequent subscription receives completion immediately
		var completed2 = false
		emitter.subscribe(
			onNext = (_, _) => (),
			onError = _ => (),
			onComplete = () => completed2 = true
		)
		assertEquals(completed2, true)

		// Check failure propagation
		val emitterErr = new StreamEmitter[Int]()
		var errorErr: Option[Throwable] = None
		emitterErr.subscribe(
			onNext = (_, _) => (),
			onError = ex => errorErr = Some(ex),
			onComplete = () => ()
		)
		val testEx = new Exception("test")
		emitterErr.fail(testEx)
		assertEquals(errorErr, Some(testEx))

		// Subsequent subscription receives failure immediately
		var errorErr2: Option[Throwable] = None
		emitterErr.subscribe(
			onNext = (_, _) => (),
			onError = ex => errorErr2 = Some(ex),
			onComplete = () => ()
		)
		assertEquals(errorErr2, Some(testEx))
	}

	test("map propagation of completion and error") {
		val emitter = new StreamEmitter[Int]()
		val mapped = emitter.map(_ * 10)
		var collected = List[Int]()
		var completed = false
		var error: Option[Throwable] = None

		mapped.subscribe(
			onNext = (v, idx) => collected = collected :+ v,
			onError = ex => error = Some(ex),
			onComplete = () => completed = true
		)

		emitter.emit(1)
		emitter.emit(2)
		assertEquals(collected, List(10, 20))
		assertEquals(completed, false)

		emitter.end()
		assertEquals(completed, true)

		val emitterErr = new StreamEmitter[Int]()
		val mappedErr = emitterErr.map(_ * 10)
		var errorErr: Option[Throwable] = None
		mappedErr.subscribe(
			onNext = (_, _) => (),
			onError = ex => errorErr = Some(ex),
			onComplete = () => ()
		)
		val testEx = new Exception("test")
		emitterErr.fail(testEx)
		assertEquals(errorErr, Some(testEx))
	}

	test("scan propagation") {
		val emitter = new StreamEmitter[Int]()
		val scanned = emitter.scan(0)(_ + _)
		var collected = List[Int]()
		var completed = false

		scanned.subscribe(
			onNext = (v, idx) => collected = collected :+ v,
			onError = _ => (),
			onComplete = () => completed = true
		)

		emitter.emit(1)
		emitter.emit(2)
		emitter.emit(3)
		assertEquals(collected, List(1, 3, 6))
		assertEquals(completed, false)

		emitter.end()
		assertEquals(completed, true)
	}

	test("take(n) early completion") {
		val emitter = new StreamEmitter[Int]()
		val taken = emitter.take(2)
		var collected = List[Int]()
		var completed = false

		taken.subscribe(
			onNext = (v, idx) => collected = collected :+ v,
			onError = _ => (),
			onComplete = () => completed = true
		)

		emitter.emit(1)
		assertEquals(completed, false)
		emitter.emit(2)
		// Should complete immediately when taking limit is reached
		assertEquals(completed, true)
		assertEquals(collected, List(1, 2))

		// Subsequent emissions from emitter are ignored by take(2)
		emitter.emit(3)
		assertEquals(collected, List(1, 2))
	}

	test("takeWhile early completion") {
		val emitter = new StreamEmitter[Int]()
		val taken = emitter.takeWhile(_ < 3)
		var collected = List[Int]()
		var completed = false

		taken.subscribe(
			onNext = (v, idx) => collected = collected :+ v,
			onError = _ => (),
			onComplete = () => completed = true
		)

		emitter.emit(1)
		emitter.emit(2)
		assertEquals(completed, false)
		emitter.emit(3) // doesn't satisfy predicate, should complete immediately
		assertEquals(completed, true)
		assertEquals(collected, List(1, 2))

		emitter.emit(1)
		assertEquals(collected, List(1, 2))
	}

	test("buffer flushing trailing elements on complete") {
		val emitter = new StreamEmitter[Int]()
		val buffered = emitter.buffer[Int](3)
		var collected = List[IArray[Int]]()
		var completed = false

		buffered.subscribe(
			onNext = (v, idx) => collected = collected :+ v,
			onError = _ => (),
			onComplete = () => completed = true
		)

		emitter.emit(1)
		emitter.emit(2)
		assertEquals(collected.map(_.toList), Nil)

		emitter.emit(3)
		assertEquals(collected.map(_.toList), List(List(1, 2, 3)))

		emitter.emit(4)
		emitter.emit(5)
		assertEquals(collected.map(_.toList), List(List(1, 2, 3)))

		emitter.end()
		// Completing should flush the remaining (4, 5) and trigger complete
		assertEquals(completed, true)
		assertEquals(collected.map(_.toList), List(List(1, 2, 3), List(4, 5)))
	}

	test("zip coordinate-aligned completion and error propagation") {
		val leftEmitter = new StreamEmitter[Int]()
		val rightEmitter = new StreamEmitter[String]()
		val zipped = leftEmitter.zip(rightEmitter)((a, b) => s"$a-$b")

		var collected = List[String]()
		var completed = false

		zipped.subscribe(
			onNext = (v, idx) => collected = collected :+ v,
			onError = _ => (),
			onComplete = () => completed = true
		)

		leftEmitter.emit(10) // left: Map(0 -> 10)
		rightEmitter.emit("a") // match: 10-a
		assertEquals(collected, List("10-a"))
		assertEquals(completed, false)

		leftEmitter.emit(20)
		leftEmitter.end()
		assertEquals(completed, false)

		rightEmitter.emit("b")
		assertEquals(collected, List("10-a", "20-b"))
		assertEquals(completed, true)
	}

	test("zip error propagation") {
		val leftEmitter = new StreamEmitter[Int]()
		val rightEmitter = new StreamEmitter[String]()
		val zipped = leftEmitter.zip(rightEmitter)((a, b) => s"$a-$b")

		var error: Option[Throwable] = None
		zipped.subscribe(
			onNext = (_, _) => (),
			onError = ex => error = Some(ex),
			onComplete = () => ()
		)

		val testEx = new Exception("zip-err")
		leftEmitter.fail(testEx)
		assertEquals(error, Some(testEx))
	}

	test("TaskArray map and mapWithIndex") {
		val t1 = makeTask(1)
		val t2 = makeTask(2)
		val taskArray = TaskArray_fromTasks(IArray(t1, t2))

		var collectedMap = List[Int]()
		var completedMap = false
		taskArray.map(_ * 10).subscribe(
			onNext = (v, idx) => collectedMap = collectedMap :+ v,
			onError = _ => (),
			onComplete = () => completedMap = true
		)
		assertEquals(collectedMap, List(10, 20))
		assertEquals(completedMap, true)

		var collectedMapWithIdx = List[(Int, Int)]()
		taskArray.mapWithIndex((v, idx) => v + idx).subscribe(
			onNext = (v, idx) => collectedMapWithIdx = collectedMapWithIdx :+ (v, idx),
			onError = _ => (),
			onComplete = () => ()
		)
		assertEquals(collectedMapWithIdx, List((1, 0), (3, 1)))
	}

	test("TaskArray flatMap and flatMapWithIndex") {
		val t1 = makeTask(1)
		val t2 = makeTask(2)
		val taskArray = TaskArray_fromTasks(IArray(t1, t2))

		// flatMap to TaskArray
		val flatMapped = taskArray.flatMap[Int] { x =>
			TaskArray_fromTasks(IArray(makeTask(x * 10), makeTask(x * 100)))
		}

		var collected = List[Int]()
		var completed = false
		flatMapped.subscribe(
			onNext = (v, up, down) => collected = collected :+ v,
			onError = _ => (),
			onComplete = () => completed = true
		)
		assertEquals(collected, List(10, 100, 20, 200))
		assertEquals(completed, true)
	}

	test("TaskArray scan") {
		val t1 = makeTask(1)
		val t2 = makeTask(2)
		val t3 = makeTask(3)
		val taskArray = TaskArray_fromTasks(IArray(t1, t2, t3))

		var collected = List[Int]()
		var completed = false
		taskArray.scan(0)(_ + _).subscribe(
			onNext = (v, idx) => collected = collected :+ v,
			onError = _ => (),
			onComplete = () => completed = true
		)
		assertEquals(collected, List(1, 3, 6))
		assertEquals(completed, true)
	}

	test("TaskArray buffer") {
		val t1 = makeTask(1)
		val t2 = makeTask(2)
		val t3 = makeTask(3)
		val taskArray = TaskArray_fromTasks(IArray(t1, t2, t3))

		var collected = List[IArray[Int]]()
		var completed = false
		taskArray.buffer[Int](2).subscribe(
			onNext = (v, idx) => collected = collected :+ v,
			onError = _ => (),
			onComplete = () => completed = true
		)
		assertEquals(collected.map(_.toList), List(List(1, 2), List(3)))
		assertEquals(completed, true)
	}

	test("TaskArray zip") {
		val t1 = makeTask(1)
		val t2 = makeTask(2)
		val left = TaskArray_fromTasks(IArray(t1, t2))

		val t3 = makeTask("a")
		val t4 = makeTask("b")
		val right = TaskArray_fromTasks(IArray(t3, t4))

		var collected = List[String]()
		var completed = false
		left.zip(right)((a, b) => s"$a-$b").subscribe(
			onNext = (v, idx) => collected = collected :+ v,
			onError = _ => (),
			onComplete = () => completed = true
		)
		assertEquals(collected, List("1-a", "2-b"))
		assertEquals(completed, true)
	}

	test("TaskArray take and takeWhile") {
		val t1 = makeTask(1)
		val t2 = makeTask(2)
		val t3 = makeTask(3)
		val taskArray = TaskArray_fromTasks(IArray(t1, t2, t3))

		var collectedTake = List[Int]()
		var completedTake = false
		taskArray.take(2).subscribe(
			onNext = (v, idx) => collectedTake = collectedTake :+ v,
			onError = _ => (),
			onComplete = () => completedTake = true
		)
		assertEquals(collectedTake, List(1, 2))
		assertEquals(completedTake, true)

		var collectedTakeWhile = List[Int]()
		var completedTakeWhile = false
		taskArray.takeWhile(_ < 3).subscribe(
			onNext = (v, idx) => collectedTakeWhile = collectedTakeWhile :+ v,
			onError = _ => (),
			onComplete = () => completedTakeWhile = true
		)
		assertEquals(collectedTakeWhile, List(1, 2))
		assertEquals(completedTakeWhile, true)
	}
}
