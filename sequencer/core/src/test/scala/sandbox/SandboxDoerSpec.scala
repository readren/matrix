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

		// Both should receive value 20 with sequential coordinates (NOT_APPLICABLE_INDEX, 0)
		assertEquals(sub1List, List((20, NOT_APPLICABLE_INDEX, 0)))
		assertEquals(sub2List, List((20, NOT_APPLICABLE_INDEX, 0)))
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

		assertEquals(innerCoords, List(("a", NOT_APPLICABLE_INDEX, 0)))
		assertEquals(outerCoords, List(("a", NOT_APPLICABLE_INDEX, 0)))
	}

	test("Property-based test: MappedKeyedCapturerArray correctness") {
		Prop.forAll { (nums: List[Int]) =>
			val keeperArray = new KeeperArray(IArray.from(nums))
			val mapped = keeperArray.map(_ * 2)

			var collected = List[Int]()
			mapped.foreach(v => collected = collected :+ v)

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
		assertEquals(collected, List((1, NOT_APPLICABLE_INDEX, 0), (2, NOT_APPLICABLE_INDEX, 1)))
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
		assertEquals(collected, List((10, NOT_APPLICABLE_INDEX, 0), (20, NOT_APPLICABLE_INDEX, 1)))
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
		assertEquals(collected, List((1, NOT_APPLICABLE_INDEX, 0), (3, NOT_APPLICABLE_INDEX, 1), (6, NOT_APPLICABLE_INDEX, 2)))
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
		assertEquals(collected, List((1, NOT_APPLICABLE_INDEX, 0), (2, NOT_APPLICABLE_INDEX, 1)))

		// Subsequent emissions from emitter are ignored by take(2)
		emitter.emit(3)
		assertEquals(collected, List((1, NOT_APPLICABLE_INDEX, 0), (2, NOT_APPLICABLE_INDEX, 1)))
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
		assertEquals(collected, List((1, NOT_APPLICABLE_INDEX, 0), (2, NOT_APPLICABLE_INDEX, 1)))

		emitter.emit(1)
		assertEquals(collected, List((1, NOT_APPLICABLE_INDEX, 0), (2, NOT_APPLICABLE_INDEX, 1)))
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
		assertEquals(collected.map(c => (c._1.toList, c._2, c._3)), List((List(1, 2, 3), NOT_APPLICABLE_INDEX, 0)))

		emitter.emit(4)
		emitter.emit(5)
		assertEquals(collected.map(c => (c._1.toList, c._2, c._3)), List((List(1, 2, 3), NOT_APPLICABLE_INDEX, 0)))

		emitter.end()
		// Completing should flush the remaining (4, 5) and trigger complete
		assertEquals(completed, true)
		assertEquals(collected.map(c => (c._1.toList, c._2, c._3)), List((List(1, 2, 3), NOT_APPLICABLE_INDEX, 0), (List(4, 5), NOT_APPLICABLE_INDEX, 1)))
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
		assertEquals(collected, List(("10-a", NOT_APPLICABLE_INDEX, 0)))
		assertEquals(completed, false)

		leftEmitter.emit(20)
		leftEmitter.end()
		assertEquals(completed, false)

		rightEmitter.emit("b")
		assertEquals(collected, List(("10-a", NOT_APPLICABLE_INDEX, 0), ("20-b", NOT_APPLICABLE_INDEX, 1)))
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

	test("TaskArray map and mapWithCoords") {
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

		var collectedMapWithCoords = List[(Int, Int, Int)]()
		taskArray.mapWithCoords((v, up, down) => v + down).subscribeCallbacks(
			onNextCallback = (v, up, down) => collectedMapWithCoords = collectedMapWithCoords :+ (v, up, down),
			onErrorCallback = _ => (),
			onCompleteCallback = () => ()
		)
		assertEquals(collectedMapWithCoords, List((1, 0, 0), (3, 0, 1)))
	}

	test("TaskArray flatMap and flatMapWithCoords") {
		val t1 = makeTask(1)
		val t2 = makeTask(2)
		val taskArray = TaskArray_fromTasks(IArray(t1, t2))

		// flatMap to TaskArray
		val flatMapped = taskArray.flatMap[Int] { x =>
			TaskArray_fromTasks(IArray(makeTask(x * 10), makeTask(x * 100)))
		}

		// flatMapWithCoords to TaskArray
		val flatMappedCoords = taskArray.flatMapWithCoords[Int] { (x, up, down) =>
			TaskArray_fromTasks(IArray(makeTask(x * 10 + up), makeTask(x * 100 + down)))
		}

		var collectedCoords = List[(Int, Int, Int)]()
		flatMappedCoords.subscribe(
			onNext = (v, up, down) => collectedCoords = collectedCoords :+ (v, up, down),
			onError = _ => (),
			onComplete = () => ()
		)
		assertEquals(collectedCoords, List((10, 0, 0), (100, 0, 1), (20, 1, 0), (201, 1, 1)))

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
		captor2.subscribeWithCoords(
			new Observer[Int] {
				override def onNext(v: Int, up: Int, down: Int): Unit = count1 += 1

				override def onError(ex: Throwable): Unit = ()

				override def onComplete(): Unit = ()
			},
			key,
			upChain = 0,
			downChain = 0
		)
		captor2.subscribeWithCoords(
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

	test("ObservableStream.fold - complete fold and early termination") {
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

	test("ObservableStream.fold - error propagation") {
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

	test("ObservableStream - empty factory method") {
		var nextCount = 0
		var errorCount = 0
		var completeCount = 0

		val stream = Flux.empty[Int]
		stream.subscribeCallbacks(
			onNextCallback = (v, up, down) => nextCount += 1,
			onErrorCallback = _ => errorCount += 1,
			onCompleteCallback = () => completeCount += 1
		)

		assertEquals(nextCount, 0)
		assertEquals(errorCount, 0)
		assertEquals(completeCount, 1)
	}

	test("ObservableStream - apply and fromIterable factory methods") {
		var receivedFromIterable = List.empty[Int]
		var completedFromIterable = false
		val streamIterable = Flux.fromIterable(List(1, 2, 3))

		streamIterable.subscribeCallbacks(
			onNextCallback = (v, up, down) => receivedFromIterable = receivedFromIterable :+ v,
			onErrorCallback = _ => (),
			onCompleteCallback = () => completedFromIterable = true
		)

		assertEquals(receivedFromIterable, List(1, 2, 3))
		assertEquals(completedFromIterable, true)

		var receivedApply = List.empty[String]
		var completedApply = false
		val streamApply = Flux("a", "b")

		streamApply.subscribeCallbacks(
			onNextCallback = (v, up, down) => receivedApply = receivedApply :+ v,
			onErrorCallback = _ => (),
			onCompleteCallback = () => completedApply = true
		)

		assertEquals(receivedApply, List("a", "b"))
		assertEquals(completedApply, true)
	}

	test("ObservableStream - generateKeyed factory method and synchronous cancellation") {
		val key = new AnyRef()
		var count = 0
		val stream = Flux.generateKeyed(() => {
			count += 1
			count
		})

		val received = scala.collection.mutable.Buffer[Int]()
		stream.keyedSubscribe(new Observer[Int] {
			override def onNext(v: Int, upChain: Int, downChain: Int): Unit = {
				received += v
				if v >= 5 then stream.unsubscribe(key)
			}

			override def onError(ex: Throwable): Unit = ()

			override def onComplete(): Unit = ()
		}, key)

		assertEquals(received.toList, List(1, 2, 3, 4, 5))
		assertEquals(count, 5)
	}

	test("ObservableStream - unfold factory method") {
		var received = List.empty[String]
		var completed = false
		val stream = Flux.unfold(0)(s => if s < 3 then Maybe((s.toString, s + 1)) else Maybe.empty)

		stream.subscribeCallbacks(
			onNextCallback = (v, up, down) => received = received :+ v,
			onErrorCallback = _ => (),
			onCompleteCallback = () => completed = true
		)

		assertEquals(received, List("0", "1", "2"))
		assertEquals(completed, true)
	}

	test("ObservableStream - fromIterableGuarded handles failure in hasNext") {
		val badIterable = new Iterable[Int] {
			override def iterator: Iterator[Int] = new Iterator[Int] {
				override def hasNext: Boolean = throw new RuntimeException("bad-has-next")

				override def next(): Int = 42
			}
		}

		var nextCount = 0
		var caughtEx: Throwable = null
		var completed = false

		Flux.fromIterableGuarded(badIterable).subscribeCallbacks(
			onNextCallback = (v, up, down) => nextCount += 1,
			onErrorCallback = ex => caughtEx = ex,
			onCompleteCallback = () => completed = true
		)

		assertEquals(nextCount, 0)
		assert(caughtEx != null)
		assertEquals(caughtEx.getMessage, "bad-has-next")
		assertEquals(completed, false)
	}

	test("ObservableStream - fromIterableGuarded handles failure in next") {
		val badIterable = new Iterable[Int] {
			override def iterator: Iterator[Int] = new Iterator[Int] {
				private var count = 0

				override def hasNext: Boolean = count < 2

				override def next(): Int = {
					count += 1
					if count == 1 then 10 else throw new RuntimeException("bad-next")
				}
			}
		}

		var received = List.empty[Int]
		var caughtEx: Throwable = null
		var completed = false

		Flux.fromIterableGuarded(badIterable).subscribeCallbacks(
			onNextCallback = (v, up, down) => received = received :+ v,
			onErrorCallback = ex => caughtEx = ex,
			onCompleteCallback = () => completed = true
		)

		assertEquals(received, List(10))
		assert(caughtEx != null)
		assertEquals(caughtEx.getMessage, "bad-next")
		assertEquals(completed, false)
	}

	test("Single-slot caching: MappedObservableStream supports multiple subscriptions with fallback") {
		val emitter = new StreamEmitter[Int]()
		val mapped = emitter.map(_ * 2)

		var list1 = List[Int]()
		var list2 = List[Int]()
		var completed1 = false
		var completed2 = false

		mapped.subscribe(new Observer[Int] {
			override def onNext(value: Int, upChain: Int, downChain: Int): Unit = list1 = list1 :+ value

			override def onError(ex: Throwable): Unit = ()

			override def onComplete(): Unit = completed1 = true
		})

		mapped.subscribe(new Observer[Int] {
			override def onNext(value: Int, upChain: Int, downChain: Int): Unit = list2 = list2 :+ value

			override def onError(ex: Throwable): Unit = ()

			override def onComplete(): Unit = completed2 = true
		})

		emitter.emit(10)
		emitter.emit(20)
		emitter.end()

		assertEquals(list1, List(20, 40))
		assertEquals(list2, List(20, 40))
		assertEquals(completed1, true)
		assertEquals(completed2, true)
	}

	test("Single-slot caching: ScannedObservableStream supports multiple subscriptions with state separation") {
		val emitter = new StreamEmitter[Int]()
		val scanned = emitter.scan(0)(_ + _)

		var list1 = List[Int]()
		var list2 = List[Int]()
		var completed1 = false
		var completed2 = false

		scanned.subscribe(new Observer[Int] {
			override def onNext(value: Int, upChain: Int, downChain: Int): Unit = list1 = list1 :+ value

			override def onError(ex: Throwable): Unit = ()

			override def onComplete(): Unit = completed1 = true
		})

		scanned.subscribe(new Observer[Int] {
			override def onNext(value: Int, upChain: Int, downChain: Int): Unit = list2 = list2 :+ value

			override def onError(ex: Throwable): Unit = ()

			override def onComplete(): Unit = completed2 = true
		})

		emitter.emit(1)
		emitter.emit(2)
		emitter.end()

		assertEquals(list1, List(1, 3))
		assertEquals(list2, List(1, 3))
		assertEquals(completed1, true)
		assertEquals(completed2, true)
	}

	test("Single-slot caching: FlatMappedObservableMatrix supports multiple subscriptions and delegates inner completions") {
		val emitter = new StreamEmitter[Int]()
		val flatMapped = emitter.flatMap(x => Flux(x, x + 1))

		var list1 = List[(Int, Int, Int)]()
		var list2 = List[(Int, Int, Int)]()
		var completed1 = false
		var completed2 = false

		flatMapped.subscribe(new Observer[Int] {
			override def onNext(value: Int, upChain: Int, downChain: Int): Unit = list1 = list1 :+ (value, upChain, downChain)

			override def onError(ex: Throwable): Unit = ()

			override def onComplete(): Unit = completed1 = true
		})

		flatMapped.subscribe(new Observer[Int] {
			override def onNext(value: Int, upChain: Int, downChain: Int): Unit = list2 = list2 :+ (value, upChain, downChain)

			override def onError(ex: Throwable): Unit = ()

			override def onComplete(): Unit = completed2 = true
		})

		emitter.emit(10)
		emitter.end()

		val expected = List((10, 0, 0), (11, 0, 1))
		assertEquals(list1, expected)
		assertEquals(list2, expected)
		assertEquals(completed1, true)
		assertEquals(completed2, true)
	}

	test("Single-slot caching: Keyed stream multiple subscriptions and unsubscription works correctly") {
		class TestKeyedSource extends KeyedCapturerArray[Int] {
			var activeObserver: Observer[Int] = scala.compiletime.uninitialized
			var secondaryObserver: Observer[Int] = scala.compiletime.uninitialized
			var activeKey: Key = scala.compiletime.uninitialized

			override def keyedSubscribe(observer: Observer[Int], key: Key): Unit = {
				if activeObserver == null then {
					activeObserver = observer
					activeKey = key
				} else {
					secondaryObserver = observer
				}
			}

			override def unsubscribe(key: Key): Unit = {
				if key eq activeKey then {
					activeObserver = null
					activeKey = null
				}
			}

			override def isSubscribed(key: Key): Boolean = key eq activeKey

			override def subscribe(observer: Observer[Int]): Unit = keyedSubscribe(observer, null)
		}

		val source = new TestKeyedSource()
		val key1 = new AnyRef()
		val key2 = new AnyRef()

		val mapped = source.map(_ * 2)

		var list1 = List[Int]()
		var list2 = List[Int]()

		mapped.keyedSubscribe(new Observer[Int] {
			override def onNext(value: Int, up: Int, down: Int): Unit = list1 = list1 :+ value

			override def onError(ex: Throwable): Unit = ()

			override def onComplete(): Unit = ()
		}, key1)

		mapped.keyedSubscribe(new Observer[Int] {
			override def onNext(value: Int, up: Int, down: Int): Unit = list2 = list2 :+ value

			override def onError(ex: Throwable): Unit = ()

			override def onComplete(): Unit = ()
		}, key2)

		source.activeObserver.onNext(5, -1, 0)
		source.secondaryObserver.onNext(5, -1, 0)

		assertEquals(list1, List(10))
		assertEquals(list2, List(10))

		mapped.unsubscribe(key1)
		assert(source.activeObserver == null)

		source.secondaryObserver.onNext(10, -1, 1)
		assertEquals(list2, List(10, 20))
	}

	// ====================================================================
	// Area 1 — 2D Coordinate Integrity Tests
	// ====================================================================

	test("1.1 flatMap matrix coordinates: outer downChain becomes matrix upChain") {
		val emitter = new StreamEmitter[Int]()
		val matrix = emitter.flatMap(x => Flux(x, x + 1))

		var collected = List[(Int, Int, Int)]()
		var completed = false
		matrix.subscribe(new Observer[Int] {
			override def onNext(value: Int, upChain: Int, downChain: Int): Unit = collected = collected :+ (value, upChain, downChain)

			override def onError(ex: Throwable): Unit = ()

			override def onComplete(): Unit = completed = true
		})

		emitter.emit(10) // outer downChain = 0 → matrix upChain = 0; inner emits (10, 0, 0), (11, 0, 1)
		emitter.emit(20) // outer downChain = 1 → matrix upChain = 1; inner emits (20, 1, 0), (21, 1, 1)
		emitter.end()

		assertEquals(collected, List((10, 0, 0), (11, 0, 1), (20, 1, 0), (21, 1, 1)))
		assertEquals(completed, true)
	}

	test("1.2 flattenToInner projects inner coordinate, discards outer") {
		val emitter = new StreamEmitter[Int]()
		// inner streams: x → [x, x+1, x+2]
		val flattened = emitter.flatMap(x => Flux(x, x + 1, x + 2)).flattenToInner

		var collected = List[(Int, Int, Int)]()
		flattened.subscribeCallbacks(
			onNextCallback = (v, up, down) => collected = collected :+ (v, up, down),
			onCompleteCallback = () => ()
		)

		emitter.emit(10)
		emitter.emit(20)
		emitter.end()

		// flattenToInner on ObservableMatrix uses NOT_APPLICABLE_INDEX for upChain
		// downChain keeps the inner position
		assertEquals(collected, List(
			(10, NOT_APPLICABLE_INDEX, 0), (11, NOT_APPLICABLE_INDEX, 1), (12, NOT_APPLICABLE_INDEX, 2),
			(20, NOT_APPLICABLE_INDEX, 0), (21, NOT_APPLICABLE_INDEX, 1), (22, NOT_APPLICABLE_INDEX, 2)
		))
	}

	test("1.3 flattenToOuter projects outer coordinate, discards inner") {
		val emitter = new StreamEmitter[Int]()
		val flattened = emitter.flatMap(x => Flux(x, x + 1)).flattenToOuter

		var collected = List[(Int, Int, Int)]()
		flattened.subscribeCallbacks(
			onNextCallback = (v, up, down) => collected = collected :+ (v, up, down),
			onCompleteCallback = () => ()
		)

		emitter.emit(10)
		emitter.emit(20)
		emitter.end()

		// flattenToOuter on ObservableMatrix: upChain = NOT_APPLICABLE_INDEX, downChain = matrix upChain (the outer position)
		assertEquals(collected, List(
			(10, NOT_APPLICABLE_INDEX, 0), (11, NOT_APPLICABLE_INDEX, 0),
			(20, NOT_APPLICABLE_INDEX, 1), (21, NOT_APPLICABLE_INDEX, 1)
		))
	}

	test("1.4 flattenToSequential emits monotonic counter, ignoring matrix coordinates") {
		val emitter = new StreamEmitter[Int]()
		val flattened = emitter.flatMap(x => Flux(x, x + 1)).flattenToSequential

		var collected = List[(Int, Int, Int)]()
		flattened.subscribeCallbacks(
			onNextCallback = (v, up, down) => collected = collected :+ (v, up, down),
			onCompleteCallback = () => ()
		)

		emitter.emit(10)
		emitter.emit(20)
		emitter.end()

		assertEquals(collected, List(
			(10, NOT_APPLICABLE_INDEX, 0), (11, NOT_APPLICABLE_INDEX, 1),
			(20, NOT_APPLICABLE_INDEX, 2), (21, NOT_APPLICABLE_INDEX, 3)
		))
	}

	test("1.5 flattenMap receives correct matrix coords and emits user-defined index") {
		val emitter = new StreamEmitter[Int]()
		val matrix = emitter.flatMap(x => Flux(x, x + 1))
		// flattenMap transforms value and produces (newValue, newIndex)
		val flattened = matrix.flattenMap[String]((v, up, down) => (s"$v@$up,$down", up * 10 + down))

		var collected = List[(String, Int, Int)]()
		flattened.subscribeCallbacks(
			onNextCallback = (v, up, down) => collected = collected :+ (v, up, down),
			onCompleteCallback = () => ()
		)

		emitter.emit(10)
		emitter.emit(20)
		emitter.end()

		// Matrix coords: (10, up=0, down=0), (11, up=0, down=1), (20, up=1, down=0), (21, up=1, down=1)
		// flattenMap output: upChain = NOT_APPLICABLE_INDEX, downChain = up*10+down
		assertEquals(collected, List(
			("10@0,0", NOT_APPLICABLE_INDEX, 0),
			("11@0,1", NOT_APPLICABLE_INDEX, 1),
			("20@1,0", NOT_APPLICABLE_INDEX, 10),
			("21@1,1", NOT_APPLICABLE_INDEX, 11)
		))
	}

	test("1.6 zip coordinate alignment with interleaved arrival") {
		val left = new StreamEmitter[Int]()
		val right = new StreamEmitter[String]()
		val zipped = left.zip(right)((a, b) => s"$a-$b")

		var collected = List[(String, Int, Int)]()
		var completed = false
		zipped.subscribeCallbacks(
			onNextCallback = (v, up, down) => collected = collected :+ (v, up, down),
			onCompleteCallback = () => completed = true
		)

		// Interleave: right first, then left
		right.emit("a") // downChain=0 on right, buffered
		right.emit("b") // downChain=1 on right, buffered
		left.emit(1) // downChain=0 on left, matches right's downChain=0
		left.emit(2) // downChain=1 on left, matches right's downChain=1

		left.end()
		right.end()

		// zip matches by downChain index; output preserves the match index as downChain
		assertEquals(collected, List(("1-a", NOT_APPLICABLE_INDEX, 0), ("2-b", NOT_APPLICABLE_INDEX, 1)))
		assertEquals(completed, true)
	}

	test("1.7 mapWithCoords on TaskArray receives correct (0, elementIndex) coordinates") {
		val taskArray = TaskArray_fromTasks(IArray(makeTask(10), makeTask(20), makeTask(30)))
		// mapWithCoords receives (value, upChain=0, downChain=elementIndex)
		val mapped = taskArray.mapWithCoords((v, up, down) => v + up * 1000 + down)

		var collected = List[(Int, Int, Int)]()
		mapped.subscribeCallbacks(
			onNextCallback = (v, up, down) => collected = collected :+ (v, up, down),
			onCompleteCallback = () => ()
		)

		// Each task at index i gets up=0, down=i. Value = original + 0*1000 + i
		assertEquals(collected, List((10, 0, 0), (21, 0, 1), (32, 0, 2)))
	}

	test("1.8 flatMapWithCoords on TaskArray → matrix → flattenToSequential round-trip") {
		val taskArray = TaskArray_fromTasks(IArray(makeTask(10), makeTask(20)))

		// flatMapWithCoords: each element (v, up, down) produces a TaskArray of 2 elements encoding the matrix coords
		val matrix: TaskMatrix[String] = taskArray.flatMapWithCoords[String] { (v, up, down) =>
			TaskArray_fromTasks(IArray(makeTask(s"$v:$up,$down,inner0"), makeTask(s"$v:$up,$down,inner1")))
		}

		val flattened = matrix.flattenToSequential
		var collected = List[(String, Int, Int)]()
		flattened.subscribeCallbacks(
			onNextCallback = (v, up, down) => collected = collected :+ (v, up, down),
			onCompleteCallback = () => ()
		)

		// TaskMatrix flatten uses NOT_APPLICABLE_INDEX for upChain, sequential counter for downChain
		assertEquals(collected.map(_._2).distinct, List(NOT_APPLICABLE_INDEX)) // all upChain = -1
		assertEquals(collected.map(_._3), List(0, 1, 2, 3)) // sequential counter
		// Values encode original coords: first task at (up=0, down=0), second at (up=0, down=1)
		assertEquals(collected.map(_._1), List("10:0,0,inner0", "10:0,0,inner1", "20:0,1,inner0", "20:0,1,inner1"))
	}

	// ====================================================================
	// Area 2 — Single-Slot Caching / Delegate-Fallback Verification
	// ====================================================================

	test("2.1 MappedObservableStream: two subscribers receive identical coordinates") {
		val emitter = new StreamEmitter[Int]()
		val mapped = emitter.map(_ * 2)

		var coords1 = List[(Int, Int, Int)]()
		var coords2 = List[(Int, Int, Int)]()

		mapped.subscribe(new Observer[Int] {
			override def onNext(value: Int, upChain: Int, downChain: Int): Unit = coords1 = coords1 :+ (value, upChain, downChain)

			override def onError(ex: Throwable): Unit = ()

			override def onComplete(): Unit = ()
		})

		mapped.subscribe(new Observer[Int] {
			override def onNext(value: Int, upChain: Int, downChain: Int): Unit = coords2 = coords2 :+ (value, upChain, downChain)

			override def onError(ex: Throwable): Unit = ()

			override def onComplete(): Unit = ()
		})

		emitter.emit(5)
		emitter.emit(10)
		emitter.end()

		assertEquals(coords1, List((10, NOT_APPLICABLE_INDEX, 0), (20, NOT_APPLICABLE_INDEX, 1)))
		assertEquals(coords2, List((10, NOT_APPLICABLE_INDEX, 0), (20, NOT_APPLICABLE_INDEX, 1)))
		assertEquals(coords1, coords2)
	}

	test("2.2 ScannedObservableStream: late subscriber starts from initial state, not accumulated") {
		val emitter = new StreamEmitter[Int]()
		val scanned = emitter.scan(0)(_ + _)

		var list1 = List[Int]()
		var completed1 = false

		scanned.subscribe(new Observer[Int] {
			override def onNext(value: Int, upChain: Int, downChain: Int): Unit = list1 = list1 :+ value

			override def onError(ex: Throwable): Unit = ()

			override def onComplete(): Unit = completed1 = true
		})

		// Emit some values before second subscription
		emitter.emit(1) // scan state: 0+1=1
		emitter.emit(2) // scan state: 1+2=3

		assertEquals(list1, List(1, 3))

		// Late subscriber — should start from initial state (0), not from 3
		var list2 = List[Int]()
		var completed2 = false

		scanned.subscribe(new Observer[Int] {
			override def onNext(value: Int, upChain: Int, downChain: Int): Unit = list2 = list2 :+ value

			override def onError(ex: Throwable): Unit = ()

			override def onComplete(): Unit = completed2 = true
		})

		emitter.emit(10) // sub1 scan: 3+10=13; sub2 scan: 0+10=10

		assertEquals(list1, List(1, 3, 13))
		assertEquals(list2, List(10)) // independent state, started from 0

		emitter.end()
		assertEquals(completed1, true)
		assertEquals(completed2, true)
	}

	test("2.3 BufferedObservableStream: two subscribers receive same buffer chunks with identical coordinates") {
		val emitter = new StreamEmitter[Int]()
		val buffered = emitter.buffer[Int](2)

		var chunks1 = List[(List[Int], Int, Int)]()
		var chunks2 = List[(List[Int], Int, Int)]()
		var completed1 = false
		var completed2 = false

		buffered.subscribe(new Observer[IArray[Int]] {
			override def onNext(value: IArray[Int], upChain: Int, downChain: Int): Unit = chunks1 = chunks1 :+ (value.toList, upChain, downChain)

			override def onError(ex: Throwable): Unit = ()

			override def onComplete(): Unit = completed1 = true
		})

		buffered.subscribe(new Observer[IArray[Int]] {
			override def onNext(value: IArray[Int], upChain: Int, downChain: Int): Unit = chunks2 = chunks2 :+ (value.toList, upChain, downChain)

			override def onError(ex: Throwable): Unit = ()

			override def onComplete(): Unit = completed2 = true
		})

		emitter.emit(1)
		emitter.emit(2) // flush chunk [1,2]
		emitter.emit(3)
		emitter.emit(4) // flush chunk [3,4]
		emitter.emit(5)
		emitter.end() // flush partial chunk [5]

		val expected = List((List(1, 2), NOT_APPLICABLE_INDEX, 0), (List(3, 4), NOT_APPLICABLE_INDEX, 1), (List(5), NOT_APPLICABLE_INDEX, 2))
		assertEquals(chunks1, expected)
		assertEquals(chunks2, expected)
		assertEquals(completed1, true)
		assertEquals(completed2, true)
	}

	test("2.4 FlatMappedObservableMatrix: two subscribers track inner completions independently") {
		val emitter = new StreamEmitter[Int]()
		// each outer element produces a stream of variable length
		val matrix = emitter.flatMap { x =>
			if x == 1 then Flux(10, 11)
			else Flux(20, 21, 22)
		}

		var list1 = List[(Int, Int, Int)]()
		var list2 = List[(Int, Int, Int)]()
		var completed1 = false
		var completed2 = false

		matrix.subscribe(new Observer[Int] {
			override def onNext(value: Int, upChain: Int, downChain: Int): Unit = list1 = list1 :+ (value, upChain, downChain)

			override def onError(ex: Throwable): Unit = ()

			override def onComplete(): Unit = completed1 = true
		})

		matrix.subscribe(new Observer[Int] {
			override def onNext(value: Int, upChain: Int, downChain: Int): Unit = list2 = list2 :+ (value, upChain, downChain)

			override def onError(ex: Throwable): Unit = ()

			override def onComplete(): Unit = completed2 = true
		})

		emitter.emit(1) // outer downChain=0 → upChain=0
		emitter.emit(2) // outer downChain=1 → upChain=1
		emitter.end()

		val expected = List((10, 0, 0), (11, 0, 1), (20, 1, 0), (21, 1, 1), (22, 1, 2))
		assertEquals(list1, expected)
		assertEquals(list2, expected)
		assertEquals(completed1, true)
		assertEquals(completed2, true)
	}

	test("2.5 ZippedObservableStream: two subscribers with independent match buffers") {
		val left = new StreamEmitter[Int]()
		val right = new StreamEmitter[String]()
		val zipped = left.zip(right)((a, b) => s"$a-$b")

		var list1 = List[(String, Int, Int)]()
		var list2 = List[(String, Int, Int)]()
		var completed1 = false
		var completed2 = false

		zipped.subscribe(new Observer[String] {
			override def onNext(value: String, upChain: Int, downChain: Int): Unit = list1 = list1 :+ (value, upChain, downChain)

			override def onError(ex: Throwable): Unit = ()

			override def onComplete(): Unit = completed1 = true
		})

		zipped.subscribe(new Observer[String] {
			override def onNext(value: String, upChain: Int, downChain: Int): Unit = list2 = list2 :+ (value, upChain, downChain)

			override def onError(ex: Throwable): Unit = ()

			override def onComplete(): Unit = completed2 = true
		})

		// Interleaved arrival
		right.emit("a") // right downChain=0, buffered
		left.emit(1) // left downChain=0, matches right 0
		left.emit(2) // left downChain=1, buffered
		right.emit("b") // right downChain=1, matches left 1

		left.end()
		right.end()

		val expected = List(("1-a", NOT_APPLICABLE_INDEX, 0), ("2-b", NOT_APPLICABLE_INDEX, 1))
		assertEquals(list1, expected)
		assertEquals(list2, expected)
		assertEquals(completed1, true)
		assertEquals(completed2, true)
	}

	// ====================================================================
	// Area 3 — Keyed Stream Lifecycle Tests
	// ====================================================================

	test("3.1 CaptorArray: unsubscribe(key) halts emissions for that key only") {
		val c1 = new Captor[Int]()
		val c2 = new Captor[Int]()
		val array = new CaptorArray[Int](IArray(c1, c2))

		val k1 = new AnyRef()
		val k2 = new AnyRef()
		var k1Values = List[(Int, Int, Int)]()
		var k2Values = List[(Int, Int, Int)]()

		array.keyedSubscribeCallbacks((v, up, down) => k1Values = k1Values :+ (v, up, down), _ => (), () => (), k1)
		array.keyedSubscribeCallbacks((v, up, down) => k2Values = k2Values :+ (v, up, down), _ => (), () => (), k2)

		// Unsubscribe k1 before any captures
		array.unsubscribe(k1)

		c1.capture(10)
		c2.capture(20)

		// k1 should have received nothing; k2 should have received both
		assertEquals(k1Values, Nil)
		assertEquals(k2Values, List((10, 0, 0), (20, 0, 1)))
	}

	test("3.2 MappedKeyedCapturerArray: key delegation through map operator") {
		val c1 = new Captor[Int]()
		val array = new CaptorArray[Int](IArray(c1))
		val mapped = array.map(_ * 3)

		val k1 = new AnyRef()
		val k2 = new AnyRef()
		var k1Values = List[Int]()
		var k2Values = List[Int]()

		mapped.keyedSubscribeCallbacks((v, _, _) => k1Values = k1Values :+ v, _ => (), () => (), k1)
		mapped.keyedSubscribeCallbacks((v, _, _) => k2Values = k2Values :+ v, _ => (), () => (), k2)

		// Unsubscribe k1 through the mapped array — should propagate to source
		mapped.unsubscribe(k1)
		assert(!array.isSubscribed(k1), "k1 should be unsubscribed from source array after mapped.unsubscribe")

		c1.capture(10)

		assertEquals(k1Values, Nil)
		assertEquals(k2Values, List(30))
	}

	test("3.3 CaptorArray: duplicate key auto-unsubscribes previous observer") {
		val c1 = new Captor[Int]()
		val array = new CaptorArray[Int](IArray(c1))
		val key = new AnyRef()

		var observerA_values = List[Int]()
		var observerB_values = List[Int]()

		array.keyedSubscribeCallbacks((v, _, _) => observerA_values = observerA_values :+ v, _ => (), () => (), key)
		// Re-subscribing with same key auto-unsubscribes observer A
		array.keyedSubscribeCallbacks((v, _, _) => observerB_values = observerB_values :+ v, _ => (), () => (), key)

		c1.capture(42)

		assertEquals(observerA_values, Nil)
		assertEquals(observerB_values, List(42))
	}

	test("3.4 FlattenedToSequentialArray: unsubscribe(key) propagates through matrix to all captors") {
		val outerCaptor1 = new Captor[Int]()
		val outerCaptor2 = new Captor[Int]()
		val outerArray = new CaptorArray[Int](IArray(outerCaptor1, outerCaptor2))

		val innerCaptor1 = new Captor[String]()
		val innerCaptor2 = new Captor[String]()

		val matrix = outerArray.flatMap[String] { x =>
			new CaptorArray[String](IArray(if x == 1 then innerCaptor1 else innerCaptor2))
		}

		val seqArray = matrix.flattenToSequential
		val key = new AnyRef()
		var received = List[(String, Int, Int)]()

		seqArray.keyedSubscribeCallbacks((v, up, down) => received = received :+ (v, up, down), _ => (), () => (), key)

		// Complete first outer captor → triggers inner subscription
		outerCaptor1.capture(1)
		// Complete the inner captor for it
		innerCaptor1.capture("first")
		assertEquals(received, List(("first", NOT_APPLICABLE_INDEX, 0)))

		// Now unsubscribe mid-stream (before second outer completes)
		seqArray.unsubscribe(key)

		// Complete second outer and its inner — observer should NOT receive anything
		outerCaptor2.capture(2)
		innerCaptor2.capture("second")
		assertEquals(received, List(("first", NOT_APPLICABLE_INDEX, 0))) // unchanged
	}

	test("3.5 generateKeyed: second subscription continues from supplier's captured state") {
		var count = 0
		val stream = Flux.generateKeyed(() => {
			count += 1
			count
		})

		// First subscriber: take 3 values then cancel
		val key1 = new AnyRef()
		val received1 = scala.collection.mutable.Buffer[Int]()
		stream.keyedSubscribe(new Observer[Int] {
			override def onNext(v: Int, upChain: Int, downChain: Int): Unit = {
				received1 += v
				if v >= 3 then stream.unsubscribe(key1)
			}

			override def onError(ex: Throwable): Unit = ()

			override def onComplete(): Unit = ()
		}, key1)

		assertEquals(received1.toList, List(1, 2, 3))
		assertEquals(count, 3)

		// Second subscriber: continues from where the supplier's state was left (count=3)
		val key2 = new AnyRef()
		val received2 = scala.collection.mutable.Buffer[Int]()
		stream.keyedSubscribe(new Observer[Int] {
			override def onNext(v: Int, upChain: Int, downChain: Int): Unit = {
				received2 += v
				if v >= 6 then stream.unsubscribe(key2)
			}

			override def onError(ex: Throwable): Unit = ()

			override def onComplete(): Unit = ()
		}, key2)

		assertEquals(received2.toList, List(4, 5, 6)) // continues from count=3
		assertEquals(count, 6)
	}
}

