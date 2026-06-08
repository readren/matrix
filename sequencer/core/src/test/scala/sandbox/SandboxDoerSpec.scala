package readren.sequencer
package sandbox

import munit.ScalaCheckEffectSuite
import org.scalacheck.Prop
import readren.common.{Maybe, Trial}

object TestSandboxDoer extends SandboxDoer

class SandboxDoerSpec extends ScalaCheckEffectSuite {

	import TestSandboxDoer.*

	private def makeTask[A](value: A): Task[A] = new Task[A] {
		override def subscribe(observer: Observer[A]): Unit = {
			observer.onNext(value, NOT_APPLICABLE_INDEX, NOT_APPLICABLE_INDEX)
			observer.onComplete()
		}
	}

	test("Keyed subscription lifecycle and auto-unsubscribe") {
		val capturer = new Captor[Int]()
		val array = new CaptorArray[Int](IArray(capturer))
		val key1 = new AnyRef()
		var callCount = 0
		var lastValue = -1
		var lastUpChain = -1
		var lastDownChain = -1

		// Subscribe with key1
		array.keyedSubscribeCallbacks(
			onNextCallback = (v, up, down) => {
				callCount += 1
				lastValue = v
				lastUpChain = up
				lastDownChain = down
			},
			onErrorCallback = _ => (),
			onCompleteCallback = () => (),
			key = key1
		)

		// Complete the capturer
		capturer.capture(42)
		assertEquals(callCount, 1)
		assertEquals(lastValue, 42)
		assertEquals(lastUpChain, 0)
		assertEquals(lastDownChain, 0)

		// Unsubscribe with key1
		array.unsubscribe(key1)

		// Complete with another value (though Captor is single-write, we can test with a new one)
		val capturer2 = new Captor[Int]()
		val array2 = new CaptorArray[Int](IArray(capturer2))
		var callCount2 = 0

		array2.keyedSubscribeCallbacks((_, _, _) => callCount2 += 1, _ => (), () => (), key1)
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

		array.keyedSubscribeCallbacks((_, _, _) => count1 += 1, _ => (), () => (), key)
		array.keyedSubscribeCallbacks((_, _, _) => count2 += 1, _ => (), () => (), key) // should unsubscribe the first one

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

		var sub1List = List[(Int, Int, Int)]()
		var sub2List = List[(Int, Int, Int)]()

		val key1 = new AnyRef()
		val key2 = new AnyRef()

		seqArray.keyedSubscribeCallbacks((v, up, down) => sub1List = sub1List :+ (v, up, down), _ => (), () => (), key1)
		seqArray.keyedSubscribeCallbacks((v, up, down) => sub2List = sub2List :+ (v, up, down), _ => (), () => (), key2)

		// Complete cell 1
		captor1.capture(10)
		captor2.capture(20)

		// Both should receive value 20 with sequential coordinates (0, 0)
		assertEquals(sub1List, List((20, 0, 0)))
		assertEquals(sub2List, List((20, 0, 0)))
	}

	test("FlattenedToInnerArray and FlattenedToOuterArray coordinates") {
		val captorOuter = new Captor[Int]()
		val captorInner = new Captor[String]()
		val matrix = new FlatMappedCapturerMatrix[Int, String](
			new CaptorArray[Int](IArray(captorOuter)),
			x => new CaptorArray[String](IArray(captorInner))
		)

		var innerCoords = List[(String, Int, Int)]()
		var outerCoords = List[(String, Int, Int)]()

		matrix.flattenToInner.keyedSubscribeCallbacks((v, up, down) => innerCoords = innerCoords :+ (v, up, down), _ => (), () => (), null)
		matrix.flattenToOuter.keyedSubscribeCallbacks((v, up, down) => outerCoords = outerCoords :+ (v, up, down), _ => (), () => (), null)

		captorOuter.capture(1)
		captorInner.capture("a")

		assertEquals(innerCoords, List(("a", 0, 0)))
		assertEquals(outerCoords, List(("a", 0, 0)))
	}

	test("Property-based test: MappedKeyedCapturerArray correctness") {
		Prop.forAll { (nums: List[Int]) =>
			val keeperArray = new KeeperArray(IArray.from(nums))
			val mapped = keeperArray.map(_ * 2)

			var collected = List[Int]()
			mapped.foreachWithIndex((v, idx) => collected = collected :+ v)

			collected == nums.map(_ * 2)
		}
	}

	test("StreamEmitter basic emissions, completion, and error") {
		val emitter = new StreamEmitter[Int]()
		var collected = List[(Int, Int, Int)]()
		var completed = false
		var error: Option[Throwable] = None

		emitter.subscribeCallbacks(
			onNextCallback = (v, up, down) => collected = collected :+ (v, up, down),
			onErrorCallback = ex => error = Some(ex),
			onCompleteCallback = () => completed = true
		)

		emitter.emit(1)
		emitter.emit(2)
		assertEquals(collected, List((1, 0, 0), (2, 0, 1)))
		assertEquals(completed, false)

		emitter.end()
		assertEquals(completed, true)
		assertEquals(error, None)

		// Subsequent subscription receives completion immediately
		var completed2 = false
		emitter.subscribeCallbacks(
			onNextCallback = (_, _, _) => (),
			onErrorCallback = _ => (),
			onCompleteCallback = () => completed2 = true
		)
		assertEquals(completed2, true)

		// Check failure propagation
		val emitterErr = new StreamEmitter[Int]()
		var errorErr: Option[Throwable] = None
		emitterErr.subscribeCallbacks(
			onNextCallback = (_, _, _) => (),
			onErrorCallback = ex => errorErr = Some(ex),
			onCompleteCallback = () => ()
		)
		val testEx = new Exception("test")
		emitterErr.fail(testEx)
		assertEquals(errorErr, Some(testEx))

		// Subsequent subscription receives failure immediately
		var errorErr2: Option[Throwable] = None
		emitterErr.subscribeCallbacks(
			onNextCallback = (_, _, _) => (),
			onErrorCallback = ex => errorErr2 = Some(ex),
			onCompleteCallback = () => ()
		)
		assertEquals(errorErr2, Some(testEx))
	}

	test("map propagation of completion and error") {
		val emitter = new StreamEmitter[Int]()
		val mapped = emitter.map(_ * 10)
		var collected = List[(Int, Int, Int)]()
		var completed = false
		var error: Option[Throwable] = None

		mapped.subscribeCallbacks(
			onNextCallback = (v, up, down) => collected = collected :+ (v, up, down),
			onErrorCallback = ex => error = Some(ex),
			onCompleteCallback = () => completed = true
		)

		emitter.emit(1)
		emitter.emit(2)
		assertEquals(collected, List((10, 0, 0), (20, 0, 1)))
		assertEquals(completed, false)

		emitter.end()
		assertEquals(completed, true)

		val emitterErr = new StreamEmitter[Int]()
		val mappedErr = emitterErr.map(_ * 10)
		var errorErr: Option[Throwable] = None
		mappedErr.subscribeCallbacks(
			onNextCallback = (_, _, _) => (),
			onErrorCallback = ex => errorErr = Some(ex),
			onCompleteCallback = () => ()
		)
		val testEx = new Exception("test")
		emitterErr.fail(testEx)
		assertEquals(errorErr, Some(testEx))
	}

	test("scan propagation") {
		val emitter = new StreamEmitter[Int]()
		val scanned = emitter.scan(0)(_ + _)
		var collected = List[(Int, Int, Int)]()
		var completed = false

		scanned.subscribeCallbacks(
			onNextCallback = (v, up, down) => collected = collected :+ (v, up, down),
			onErrorCallback = _ => (),
			onCompleteCallback = () => completed = true
		)

		emitter.emit(1)
		emitter.emit(2)
		emitter.emit(3)
		assertEquals(collected, List((1, 0, 0), (3, 0, 1), (6, 0, 2)))
		assertEquals(completed, false)

		emitter.end()
		assertEquals(completed, true)
	}

	test("take(n) early completion") {
		val emitter = new StreamEmitter[Int]()
		val taken = emitter.take(2)
		var collected = List[(Int, Int, Int)]()
		var completed = false

		taken.subscribeCallbacks(
			onNextCallback = (v, up, down) => collected = collected :+ (v, up, down),
			onErrorCallback = _ => (),
			onCompleteCallback = () => completed = true
		)

		emitter.emit(1)
		assertEquals(completed, false)
		emitter.emit(2)
		// Should complete immediately when taking limit is reached
		assertEquals(completed, true)
		assertEquals(collected, List((1, 0, 0), (2, 0, 1)))

		// Subsequent emissions from emitter are ignored by take(2)
		emitter.emit(3)
		assertEquals(collected, List((1, 0, 0), (2, 0, 1)))
	}

	test("takeWhile early completion") {
		val emitter = new StreamEmitter[Int]()
		val taken = emitter.takeWhile(_ < 3)
		var collected = List[(Int, Int, Int)]()
		var completed = false

		taken.subscribeCallbacks(
			onNextCallback = (v, up, down) => collected = collected :+ (v, up, down),
			onErrorCallback = _ => (),
			onCompleteCallback = () => completed = true
		)

		emitter.emit(1)
		emitter.emit(2)
		assertEquals(completed, false)
		emitter.emit(3) // doesn't satisfy predicate, should complete immediately
		assertEquals(completed, true)
		assertEquals(collected, List((1, 0, 0), (2, 0, 1)))

		emitter.emit(1)
		assertEquals(collected, List((1, 0, 0), (2, 0, 1)))
	}

	test("buffer flushing trailing elements on complete") {
		val emitter = new StreamEmitter[Int]()
		val buffered = emitter.buffer[Int](3)
		var collected = List[(IArray[Int], Int, Int)]()
		var completed = false

		buffered.subscribeCallbacks(
			onNextCallback = (v, up, down) => collected = collected :+ (v, up, down),
			onErrorCallback = _ => (),
			onCompleteCallback = () => completed = true
		)

		emitter.emit(1)
		emitter.emit(2)
		assertEquals(collected.map(c => (c._1.toList, c._2, c._3)), Nil)

		emitter.emit(3)
		assertEquals(collected.map(c => (c._1.toList, c._2, c._3)), List((List(1, 2, 3), 0, 0)))

		emitter.emit(4)
		emitter.emit(5)
		assertEquals(collected.map(c => (c._1.toList, c._2, c._3)), List((List(1, 2, 3), 0, 0)))

		emitter.end()
		// Completing should flush the remaining (4, 5) and trigger complete
		assertEquals(completed, true)
		assertEquals(collected.map(c => (c._1.toList, c._2, c._3)), List((List(1, 2, 3), 0, 0), (List(4, 5), 0, 1)))
	}

	test("zip coordinate-aligned completion and error propagation") {
		val leftEmitter = new StreamEmitter[Int]()
		val rightEmitter = new StreamEmitter[String]()
		val zipped = leftEmitter.zip(rightEmitter)((a, b) => s"$a-$b")

		var collected = List[(String, Int, Int)]()
		var completed = false

		zipped.subscribeCallbacks(
			onNextCallback = (v, up, down) => collected = collected :+ (v, up, down),
			onErrorCallback = _ => (),
			onCompleteCallback = () => completed = true
		)

		leftEmitter.emit(10) // left: Map(0 -> 10)
		rightEmitter.emit("a") // match: 10-a
		assertEquals(collected, List(("10-a", 0, 0)))
		assertEquals(completed, false)

		leftEmitter.emit(20)
		leftEmitter.end()
		assertEquals(completed, false)

		rightEmitter.emit("b")
		assertEquals(collected, List(("10-a", 0, 0), ("20-b", 0, 1)))
		assertEquals(completed, true)
	}

	test("zip error propagation") {
		val leftEmitter = new StreamEmitter[Int]()
		val rightEmitter = new StreamEmitter[String]()
		val zipped = leftEmitter.zip(rightEmitter)((a, b) => s"$a-$b")

		var error: Option[Throwable] = None
		zipped.subscribeCallbacks(
			onNextCallback = (_, _, _) => (),
			onErrorCallback = ex => error = Some(ex),
			onCompleteCallback = () => ()
		)

		val testEx = new Exception("zip-err")
		leftEmitter.fail(testEx)
		assertEquals(error, Some(testEx))
	}

	test("TaskArray map and mapWithIndex") {
		val t1 = makeTask(1)
		val t2 = makeTask(2)
		val taskArray = TaskArray_fromTasks(IArray(t1, t2))

		var collectedMap = List[(Int, Int, Int)]()
		var completedMap = false
		taskArray.map(_ * 10).subscribeCallbacks(
			onNextCallback = (v, up, down) => collectedMap = collectedMap :+ (v, up, down),
			onErrorCallback = _ => (),
			onCompleteCallback = () => completedMap = true
		)
		assertEquals(collectedMap, List((10, 0, 0), (20, 0, 1)))
		assertEquals(completedMap, true)

		var collectedMapWithIdx = List[(Int, Int, Int)]()
		taskArray.mapWithIndex((v, idx) => v + idx).subscribeCallbacks(
			onNextCallback = (v, up, down) => collectedMapWithIdx = collectedMapWithIdx :+ (v, up, down),
			onErrorCallback = _ => (),
			onCompleteCallback = () => ()
		)
		assertEquals(collectedMapWithIdx, List((1, 0, 0), (3, 0, 1)))
	}

	test("TaskArray flatMap and flatMapWithIndex") {
		val t1 = makeTask(1)
		val t2 = makeTask(2)
		val taskArray = TaskArray_fromTasks(IArray(t1, t2))

		// flatMap to TaskArray
		val flatMapped = taskArray.flatMap[Int] { x =>
			TaskArray_fromTasks(IArray(makeTask(x * 10), makeTask(x * 100)))
		}

		var collected = List[(Int, Int, Int)]()
		var completed = false
		flatMapped.subscribe(
			onNext = (v, up, down) => collected = collected :+ (v, up, down),
			onError = _ => (),
			onComplete = () => completed = true
		)
		assertEquals(collected, List((10, 0, 0), (100, 0, 1), (20, 1, 0), (200, 1, 1)))
		assertEquals(completed, true)
	}

	test("TaskArray scan") {
		val t1 = makeTask(1)
		val t2 = makeTask(2)
		val t3 = makeTask(3)
		val taskArray = TaskArray_fromTasks(IArray(t1, t2, t3))

		var collected = List[(Int, Int, Int)]()
		var completed = false
		taskArray.scan(0)(_ + _).subscribeCallbacks(
			onNextCallback = (v, up, down) => collected = collected :+ (v, up, down),
			onErrorCallback = _ => (),
			onCompleteCallback = () => completed = true
		)
		assertEquals(collected, List((1, 0, 0), (3, 0, 1), (6, 0, 2)))
		assertEquals(completed, true)
	}

	test("TaskArray buffer") {
		val t1 = makeTask(1)
		val t2 = makeTask(2)
		val t3 = makeTask(3)
		val taskArray = TaskArray_fromTasks(IArray(t1, t2, t3))

		var collected = List[(IArray[Int], Int, Int)]()
		var completed = false
		taskArray.buffer[Int](2).subscribeCallbacks(
			onNextCallback = (v, up, down) => collected = collected :+ (v, up, down),
			onErrorCallback = _ => (),
			onCompleteCallback = () => completed = true
		)
		assertEquals(collected.map(c => (c._1.toList, c._2, c._3)), List((List(1, 2), 0, 0), (List(3), 0, 1)))
		assertEquals(completed, true)
	}

	test("TaskArray zip") {
		val t1 = makeTask(1)
		val t2 = makeTask(2)
		val left = TaskArray_fromTasks(IArray(t1, t2))

		val t3 = makeTask("a")
		val t4 = makeTask("b")
		val right = TaskArray_fromTasks(IArray(t3, t4))

		var collected = List[(String, Int, Int)]()
		var completed = false
		left.zip(right)((a, b) => s"$a-$b").subscribeCallbacks(
			onNextCallback = (v, up, down) => collected = collected :+ (v, up, down),
			onErrorCallback = _ => (),
			onCompleteCallback = () => completed = true
		)
		assertEquals(collected, List(("1-a", 0, 0), ("2-b", 0, 1)))
		assertEquals(completed, true)
	}

	test("TaskArray take and takeWhile") {
		val t1 = makeTask(1)
		val t2 = makeTask(2)
		val t3 = makeTask(3)
		val taskArray = TaskArray_fromTasks(IArray(t1, t2, t3))

		var collectedTake = List[(Int, Int, Int)]()
		var completedTake = false
		taskArray.take(2).subscribeCallbacks(
			onNextCallback = (v, up, down) => collectedTake = collectedTake :+ (v, up, down),
			onErrorCallback = _ => (),
			onCompleteCallback = () => completedTake = true
		)
		assertEquals(collectedTake, List((1, 0, 0), (2, 0, 1)))
		assertEquals(completedTake, true)

		var collectedTakeWhile = List[(Int, Int, Int)]()
		var completedTakeWhile = false
		taskArray.takeWhile(_ < 3).subscribeCallbacks(
			onNextCallback = (v, up, down) => collectedTakeWhile = collectedTakeWhile :+ (v, up, down),
			onErrorCallback = _ => (),
			onCompleteCallback = () => completedTakeWhile = true
		)
		assertEquals(collectedTakeWhile, List((1, 0, 0), (2, 0, 1)))
		assertEquals(completedTakeWhile, true)
	}

	private def subscribeHelper[A](observable: Observable[A])(onNext: A => Unit, onError: Throwable => Unit = _ => (), onComplete: () => Unit = () => ()): Unit = {
		val onNextLocal = onNext
		val onErrorLocal = onError
		val onCompleteLocal = onComplete
		observable.subscribe(new Observer[A] {
			override def onNext(value: A, upChain: Int, downChain: Int): Unit = onNextLocal(value)

			override def onError(ex: Throwable): Unit = onErrorLocal(ex)

			override def onComplete(): Unit = onCompleteLocal()
		})
	}

	test("Task - mapGuarded success and failure propagation") {
		var successVal = -1
		var failureEx: Throwable = null

		val taskSuccess = makeTask(42).mapGuarded(identity)
		subscribeHelper(taskSuccess)(v => successVal = v, ex => failureEx = ex)
		assertEquals(successVal, 42)
		assert(failureEx == null)

		val testEx = new Exception("guarded-err")
		val taskFailure = makeTask(42).mapGuarded[Int](_ => throw testEx)
		subscribeHelper(taskFailure)(v => successVal = v, ex => failureEx = ex)
		assertEquals(successVal, 42) // unchanged
		assertEquals(failureEx, testEx)
	}

	test("Task.guarded decorator success and failure propagation") {
		val taskSuccess = makeTask(10).guarded.map(identity)
		var resSuccess = -1
		subscribeHelper(taskSuccess)(t => resSuccess = t)
		assertEquals(resSuccess, 10)

		val testEx = new Exception("task-err")
		val taskFailure = makeTask(5).guarded.map[Int](_ => throw testEx)
		var failed = false
		taskFailure.subscribe(new Observer[Int] {
			override def onNext(value: Int, up: Int, down: Int): Unit = ()
			override def onError(ex: Throwable): Unit = {
				assertEquals(ex, testEx)
				failed = true
			}
			override def onComplete(): Unit = ()
		})
		assert(failed)
	}

	test("Task - mapGuarded and flatMapGuarded monadic operations") {
		val t = makeTask(5)

		// mapGuarded success
		var mapRes = 0
		var mapErr: Throwable = null
		subscribeHelper(t.mapGuarded(_ * 2))(mapRes = _, mapErr = _)
		assertEquals(mapRes, 10)
		assert(mapErr == null)

		// mapGuarded throwing exception
		val testEx = new Exception("map-err")
		subscribeHelper(t.mapGuarded[Int](_ => throw testEx))(mapRes = _, mapErr = _)
		assertEquals(mapErr, testEx)

		// flatMapGuarded success
		var flatMapRes = 0
		subscribeHelper(t.flatMapGuarded(x => makeTask(x + 10)))(flatMapRes = _, _ => ())
		assertEquals(flatMapRes, 15)

		// flatMapGuarded failure propagation
		var flatMapErr: Throwable = null
		subscribeHelper(t.flatMapGuarded[Int](x => new Task[Int] {
			override def subscribe(observer: Observer[Int]): Unit = observer.onError(testEx)
		}))(onNext = _ => (), onError = flatMapErr = _)
		assertEquals(flatMapErr, testEx)
	}

	test("Keeper - mapGuarded and guarded") {
		val keeper = new Keeper(42)
		assertEquals(keeper.maybeValue, Maybe(42))
		assert(keeper.isCompleted)
		assert(!keeper.isPending)

		var successVal = 0
		var errorVal: Throwable = null
		subscribeHelper(keeper)(successVal = _, errorVal = _)
		assertEquals(successVal, 42)
		assert(errorVal == null)

		// mapGuarded
		val mapped = keeper.mapGuarded((x: Int) => x * 2)
		var mappedVal = 0
		subscribeHelper(mapped)(mappedVal = _, _ => ())
		assertEquals(mappedVal, 84)

		val testEx = new Exception("keeper-err")
		val failed = keeper.mapGuarded[Int](_ => throw testEx)
		subscribeHelper(failed)(onNext = _ => (), onError = errorVal = _)
		assertEquals(errorVal, testEx)
	}

	test("Captor - mapGuarded, guarded, and lifecycle") {
		val captor = new Captor[Int]()
		assert(!captor.isCompleted)
		assert(captor.isPending)
		assertEquals(captor.maybeValue, Maybe.empty)

		var successVal = 0
		var errorVal: Throwable = null
		subscribeHelper(captor)(successVal = _, errorVal = _)

		// capture success
		captor.capture(100)
		assert(captor.isCompleted)
		assertEquals(successVal, 100)
		assert(errorVal == null)

		// subsequent subscription should receive it immediately
		var successVal2 = 0
		subscribeHelper(captor)(successVal2 = _, _ => ())
		assertEquals(successVal2, 100)

		// check subscription removal/key matching
		val captor2 = new Captor[Int]()
		val key = new AnyRef()
		var count1 = 0
		var count2 = 0
		captor2.subscribe(
			new Observer[Int] {
				override def onNext(v: Int, up: Int, down: Int): Unit = count1 += 1

				override def onError(ex: Throwable): Unit = ()

				override def onComplete(): Unit = ()
			},
			key,
			upChain = 0,
			downChain = 0
		)
		captor2.subscribe(
			new Observer[Int] {
				override def onNext(v: Int, up: Int, down: Int): Unit = count2 += 1

				override def onError(ex: Throwable): Unit = ()

				override def onComplete(): Unit = ()
			},
			key, // should auto-unsubscribe key from count1
			upChain = 0,
			downChain = 0
		)
		captor2.capture(200)
		assertEquals(count1, 0)
		assertEquals(count2, 1)
	}

	test("Captor - mapGuarded/flatMapGuarded propagation") {
		val captor = new Captor[Int]()
		val mapped = captor.mapGuarded((x: Int) => x * 3)
		var mappedVal = 0
		subscribeHelper(mapped)(mappedVal = _, _ => ())

		val flatMapped = captor.flatMapGuarded((x: Int) => new Keeper(x + 5))
		var flatMappedVal = 0
		subscribeHelper(flatMapped)(flatMappedVal = _, _ => ())

		captor.capture(10)
		assertEquals(mappedVal, 30)
		assertEquals(flatMappedVal, 15)
	}

	test("Task - observer errors propagate out of mapGuarded") {
		val task = makeTask(5)
		val testEx = new Exception("observer-err")
		interceptMessage[Exception]("observer-err") {
			subscribeHelper(task.mapGuarded(_ * 2))(
				onNext = _ => throw testEx,
				onError = _ => ()
			)
		}
	}

	test("Captor - observer errors propagate out of mapGuarded/capture") {
		val captor = new Captor[Int]()
		val mapped = captor.mapGuarded((x: Int) => x * 2)
		val testEx = new Exception("observer-err")
		subscribeHelper(mapped)(
			onNext = _ => throw testEx,
			onError = _ => ()
		)
		interceptMessage[Exception]("observer-err") {
			captor.capture(10)
		}
	}

	test("ObservableArray.fold - complete fold and early termination") {
		val emitter = new StreamEmitter[Int]()
		val foldV = emitter.foldWhile(0)((sum, x) => Maybe.some(sum + x))

		var result = -1
		subscribeHelper(foldV)(result = _, _ => ())

		emitter.emit(1)
		emitter.emit(2)
		emitter.emit(3)
		emitter.end()
		assertEquals(result, 6)

		// Early termination fold
		val emitter2 = new StreamEmitter[Int]()
		val foldV2 = emitter2.foldWhile(0)((sum, x) => if (sum + x <= 5) Maybe.some(sum + x) else Maybe.empty)

		var result2 = -1
		subscribeHelper(foldV2)(result2 = _, _ => ())

		emitter2.emit(2)
		emitter2.emit(3) // sum is 5
		assertEquals(result2, -1) // not complete yet
		emitter2.emit(2) // sum would be 7 > 5 -> terminates early
		assertEquals(result2, 5) // completed early with previous valid state
	}

	test("ObservableArray.fold - error propagation") {
		val emitter = new StreamEmitter[Int]()
		val testEx = new Exception("fold-err")
		val foldV = emitter.foldWhile(0)((sum, x) => throw testEx)

		var caughtEx: Throwable = null
		subscribeHelper(foldV)(_ => (), caughtEx = _)

		emitter.emit(1)
		assertEquals(caughtEx, testEx)
	}

	test("Task.guarded for-comprehension non-sticky behavior") {
		val tA = makeTask(5)
		val tB = makeTask(10)

		var evaluatedA = false
		var evaluatedYield = false

		val testEx = new Exception("untrusted-generator-err")

		// Case 1: Exception thrown in guarded generator's transition lambda is caught
		val pipeline1 = for {
			a <- tA.guarded
			b <- {
				evaluatedA = true
				throw testEx
				tB
			}
		} yield {
			evaluatedYield = true
			a + b
		}

		var caughtEx: Throwable = null
		subscribeHelper(pipeline1)(_ => (), caughtEx = _)

		assert(evaluatedA)
		assert(!evaluatedYield)
		assertEquals(caughtEx, testEx)

		// Case 2: Exception thrown in subsequent generator (which is plain Task) is NOT caught
		val pipeline2 = for {
			a <- tA.guarded
			b <- tB
		} yield {
			throw testEx
		}

		interceptMessage[Exception]("untrusted-generator-err") {
			subscribeHelper(pipeline2)(_ => (), _ => ())
		}
	}

	test("Failed - direct construction and map/flatMap propagation") {
		val testEx = new Exception("failed-keeper-err")
		val failed = new Failed(testEx)
		assertEquals(failed.trial, Trial.failure(testEx))
		assertEquals(failed.maybeValue, Maybe.empty)
		assert(failed.isCompleted)
		assert(!failed.isPending)

		var caughtEx: Throwable = null
		subscribeHelper(failed)(_ => (), caughtEx = _)
		assertEquals(caughtEx, testEx)

		val mapped = failed.map((x: Nothing) => 42)
		assertEquals(mapped, failed)

		val flatMapped = failed.flatMap((x: Nothing) => makeTask(42))
		assertEquals(flatMapped, failed)
	}
}

