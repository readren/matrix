package readren.sequencer
package sandbox

import sandbox.DoerSandbox2.ExecutionSerial

import munit.ScalaCheckEffectSuite
import org.scalacheck.Prop
import readren.common.Maybe

import scala.reflect.ClassTag

class DoerSandbox2Spec extends ScalaCheckEffectSuite {

	val sandbox = new DoerSandbox2 {
		override type Tag = String
		override val tag: Tag = "Sandbox"

		override def executeSequentially(runnable: Runnable): Unit = runnable.run()

		override def currentExecutionSerial: ExecutionSerial = 0

		override def currentlyRunningDoer: Maybe[DoerSandbox2] = Maybe.empty

		override def reportFailure(cause: Throwable): Unit = throw cause
	}

	import sandbox.*
	
	private def makeTask[A](value: A): Task[A] = new Task[A] {
		override def subscribe(observer: MonoObserver[A]): Subscription = {
			observer.onSuccess(value)
			Subscription_empty
		}
	}

	test("Task operations - map, flatMap, mapGuarded, flatMapGuarded, guarded") {
		val t = makeTask(10)
		var mapResult = 0
		t.map(_ * 2).subscribeCallbacks(v => mapResult = v)
		assertEquals(mapResult, 20)

		var flatMapResult = ""
		t.flatMap(v => makeTask(s"value: $v")).subscribeCallbacks(v => flatMapResult = v)
		assertEquals(flatMapResult, "value: 10")

		val tGuarded = makeTask(10)
		var errorResult: Throwable | Null = null
		tGuarded.mapGuarded[Int] { v =>
			if v > 5 then {
				throw new Exception("guarded-err")
			}
			v
		}.subscribeCallbacks(v => (), err => errorResult = err)
		assert(errorResult ne null)
		assertEquals(errorResult.nn.getMessage, "guarded-err")
	}

	test("Capturer and Captor - map, flatMap, mapGuarded, flatMapGuarded, flatMapCapturer") {
		// Keeper (ready success path)
		val keeper = new Keeper(5)
		var keeperMap = 0
		keeper.map(_ * 2).subscribeCallbacks(v => keeperMap = v)
		assertEquals(keeperMap, 10)

		// Failed (ready failure path)
		val ex = new Exception("fail")
		val failed = new Failed(ex)
		var failedErr: Throwable | Null = null
		failed.subscribeCallbacks(_ => (), err => failedErr = err)
		assertEquals(failedErr, ex)

		// Captor lifecycle (pending, subscription, capturing, unsubscription)
		val captor = new Captor[Int]()
		var captorResult = 0
		val sub = captor.subscribeCallbacks(v => captorResult = v)
		assertEquals(captor.isCompleted, false)
		assertEquals(captor.isPending, true)
		captor.capture(100)
		assertEquals(captor.isCompleted, true)
		assertEquals(captorResult, 100)

		// Unsubscription before capture
		val captor2 = new Captor[Int]()
		var captorResult2 = 0
		val sub2 = captor2.subscribeCallbacks(v => captorResult2 = v)
		sub2.unsubscribe()
		captor2.capture(200)
		assertEquals(captorResult2, 0)
	}

	test("Captor operators - unsubscription propagation upstream") {
		val captor = new Captor[Int]()
		val mapped = captor.map(_ * 2)
		var result = 0
		val sub = mapped.subscribeCallbacks(v => result = v)
		sub.unsubscribe()
		captor.capture(50)
		assertEquals(result, 0)
	}

	test("Flux factories - empty, apply, fromIterable, generate, generateStatefully") {
		// Flux.empty
		var completed = false
		Flux.empty[Int].subscribe(new FluxObserver[Int] {
			override def onNext(v: Int, index: Int): Unit = ()

			override def onError(ex: Throwable): Unit = ()

			override def onComplete(): Unit = completed = true
		})
		assertEquals(completed, true)

		// Flux.apply
		var list = List[Int]()
		Flux(1, 2, 3).subscribe(new FluxObserver[Int] {
			override def onNext(v: Int, index: Int): Unit = list = list :+ v

			override def onError(ex: Throwable): Unit = ()

			override def onComplete(): Unit = ()
		})
		assertEquals(list, List(1, 2, 3))

		// Flux.generateStatefully
		var genList = List[Int]()
		var genError = false
		val fluxGen = Flux.generateStatefully[Int] { () =>
			var state = 0
			index => {
				if index >= 3 then {
					throw new RuntimeException("done")
				}
				val v = state
				state += 2
				v
			}
		}
		fluxGen.subscribe(new FluxObserver[Int] {
			override def onNext(v: Int, index: Int): Unit = genList = genList :+ v

			override def onError(ex: Throwable): Unit = genError = true

			override def onComplete(): Unit = ()
		})
		assertEquals(genList, List(0, 2, 4))
		assertEquals(genError, true)
	}

	test("Flux operations - map, mapWithIndex, flatMap, scan, buffer, zip, take, takeWhile") {
		// map / mapWithIndex
		var mappedList = List[Int]()
		Flux(1, 2).map(_ * 10).subscribe(new FluxObserver[Int] {
			override def onNext(v: Int, index: Int): Unit = mappedList = mappedList :+ v

			override def onError(ex: Throwable): Unit = ()

			override def onComplete(): Unit = ()
		})
		assertEquals(mappedList, List(10, 20))

		// scan
		var scannedList = List[Int]()
		Flux(1, 2, 3).scan(0)((state, el, idx) => state + el).subscribe(new FluxObserver[Int] {
			override def onNext(v: Int, index: Int): Unit = scannedList = scannedList :+ v

			override def onError(ex: Throwable): Unit = ()

			override def onComplete(): Unit = ()
		})
		assertEquals(scannedList, List(1, 3, 6))

		// buffer
		var bufferedList = List[IArray[Int]]()
		Flux(1, 2, 3, 4).buffer(2).subscribe(new FluxObserver[IArray[Int]] {
			override def onNext(v: IArray[Int], index: Int): Unit = bufferedList = bufferedList :+ v

			override def onError(ex: Throwable): Unit = ()

			override def onComplete(): Unit = ()
		})
		assertEquals(bufferedList.map(_.toList), List(List(1, 2), List(3, 4)))

		// take / takeWhile
		var takenList = List[Int]()
		Flux(1, 2, 3, 4).take(2).subscribe(new FluxObserver[Int] {
			override def onNext(v: Int, index: Int): Unit = takenList = takenList :+ v

			override def onError(ex: Throwable): Unit = ()

			override def onComplete(): Unit = ()
		})
		assertEquals(takenList, List(1, 2))

		// zip
		var zippedList = List[Int]()
		Flux(1, 2).zip(Flux(10, 20))((left, right, idx) => left + right).subscribe(new FluxObserver[Int] {
			override def onNext(v: Int, index: Int): Unit = zippedList = zippedList :+ v

			override def onError(ex: Throwable): Unit = ()

			override def onComplete(): Unit = ()
		})
		assertEquals(zippedList, List(11, 22))
	}

	test("Tensor operations - flattenInner, flattenOuter, flattenSequential") {
		val outerEmitter = new StreamEmitter[Int]()
		val innerEmitter1 = new StreamEmitter[String]()
		val innerEmitter2 = new StreamEmitter[String]()

		val tensor = outerEmitter.flatMap {
			case 1 => innerEmitter1
			case 2 => innerEmitter2
			case _ => Flux.empty[String]
		}

		var innerList = List[String]()
		var outerList = List[String]()
		var seqList = List[String]()

		tensor.flattenInner.subscribe(new FluxObserver[String] {
			override def onNext(v: String, index: Int): Unit = innerList = innerList :+ v

			override def onError(ex: Throwable): Unit = ()

			override def onComplete(): Unit = ()
		})

		tensor.flattenOuter.subscribe(new FluxObserver[String] {
			override def onNext(v: String, index: Int): Unit = outerList = outerList :+ v

			override def onError(ex: Throwable): Unit = ()

			override def onComplete(): Unit = ()
		})

		tensor.flattenSequential.subscribe(new FluxObserver[String] {
			override def onNext(v: String, index: Int): Unit = seqList = seqList :+ v

			override def onError(ex: Throwable): Unit = ()

			override def onComplete(): Unit = ()
		})

		outerEmitter.emit(1)
		innerEmitter1.emit("a")
		innerEmitter1.emit("b")

		outerEmitter.emit(2)
		innerEmitter2.emit("x")

		assertEquals(innerList, List("a", "b", "x"))
		assertEquals(outerList, List("a", "b", "x"))
		assertEquals(seqList, List("a", "b", "x"))
	}

	test("FlatMap SubscriptionTracker & array resizing verification") {
		val outerEmitter = new StreamEmitter[Int]()
		val innerEmitters = Array.fill(12)(new StreamEmitter[String]())

		val tensor = outerEmitter.flatMap(idx => if idx < innerEmitters.length then innerEmitters(idx) else Flux.empty[String])
		var collected = List[String]()

		tensor.flattenSequential.subscribe(new FluxObserver[String] {
			override def onNext(v: String, index: Int): Unit = collected = collected :+ v

			override def onError(ex: Throwable): Unit = ()

			override def onComplete(): Unit = ()
		})

		var i = 0
		while i < 10 do {
			outerEmitter.emit(i)
			innerEmitters(i).emit(s"val$i")
			i += 1
		}

		assertEquals(collected.length, 10)
		assertEquals(collected(9), "val9")
	}

	test("Property-based test: Task/Capturer mapping identity and composition") {
		Prop.forAll { (n: Int) =>
			val keeper = new Keeper(n)
			var mapVal = 0
			keeper.map(identity).subscribeCallbacks(v => mapVal = v)

			val f = (x: Int) => x + 5
			val g = (x: Int) => x * 2
			var composedVal = 0
			keeper.map(f).map(g).subscribeCallbacks(v => composedVal = v)

			var directComposedVal = 0
			keeper.map(x => g(f(x))).subscribeCallbacks(v => directComposedVal = v)

			mapVal == n && composedVal == directComposedVal
		}
	}

	test("Muxer/ObserverNest - duplicate observer subscriptions can be unsubscribed individually") {
		// 1. Test with StreamEmitter
		val emitter = new StreamEmitter[Int]()
		var count = 0
		val obs = new FluxObserver[Int] {
			override def onNext(v: Int, index: Int): Unit = count += 1

			override def onError(ex: Throwable): Unit = ()

			override def onComplete(): Unit = ()
		}

		val sub1 = emitter.subscribe(obs)
		val sub2 = emitter.subscribe(obs)

		// Both are active, should get double events
		emitter.emit(10)
		assertEquals(count, 2)

		// Unsubscribe one, other should still be active
		sub1.unsubscribe()
		emitter.emit(20)
		assertEquals(count, 3)

		// Unsubscribe the second one, none should be active
		sub2.unsubscribe()
		emitter.emit(30)
		assertEquals(count, 3)

		// 2. Test with Captor
		val captor = new Captor[Int]()
		var successCount = 0
		val monoObs = new MonoObserver[Int] {
			override def onSuccess(v: Int): Unit = successCount += 1

			override def onError(ex: Throwable): Unit = ()
		}

		val capSub1 = captor.subscribe(monoObs)
		val capSub2 = captor.subscribe(monoObs)

		// Unsubscribe first slot subscription (capSub1)
		capSub1.unsubscribe()

		// Complete the captor. Since capSub2 is still active, it should receive the success callback.
		captor.capture(42)
		assertEquals(successCount, 1)

		// 3. Test with SpareSlotCapturerOp (map)
		val captor2 = new Captor[Int]()
		val mapped = captor2.map(_ * 2)
		var mappedSuccessCount = 0
		val mappedObs = new MonoObserver[Int] {
			override def onSuccess(v: Int): Unit = mappedSuccessCount += 1

			override def onError(ex: Throwable): Unit = ()
		}

		val mapSub1 = mapped.subscribe(mappedObs)
		val mapSub2 = mapped.subscribe(mappedObs)

		// Unsubscribe second slot subscription (mapSub2)
		mapSub2.unsubscribe()

		// Complete upstream captor. This propagates to the mapped op, which propagates to active observers.
		captor2.capture(5)
		assertEquals(mappedSuccessCount, 1)
	}

	test("SpareSlotFluxOp / SpareSlotTensorOp - resetState is called on unsubscribe to prevent state leakage") {
		// 1. Verify SpareSlotFluxOp state reset on unsubscribe (using Flux.take)
		val emitter = new StreamEmitter[Int]()
		val takeFlux = emitter.take(3)

		var list1 = List[Int]()
		val sub1 = takeFlux.subscribe(new FluxObserver[Int] {
			override def onNext(v: Int, index: Int): Unit = list1 = list1 :+ v

			override def onError(ex: Throwable): Unit = ()

			override def onComplete(): Unit = ()
		})

		emitter.emit(10)
		emitter.emit(20)
		assertEquals(list1, List(10, 20))

		// Unsubscribe before completion
		sub1.unsubscribe()

		// Resubscribe to the same takeFlux
		var list2 = List[(Int, Int)]()
		val sub2 = takeFlux.subscribe(new FluxObserver[Int] {
			override def onNext(v: Int, index: Int): Unit = list2 = list2 :+ (v, index)

			override def onError(ex: Throwable): Unit = ()

			override def onComplete(): Unit = ()
		})

		// If state was reset, we should get all 3 items (starts from index 0)
		emitter.emit(30)
		emitter.emit(40)
		emitter.emit(50)
		emitter.emit(60) // this shouldn't be received because we only take 3

		assertEquals(list2, List((30, 2), (40, 3), (50, 4)))

		// 2. Verify SpareSlotTensorOp state reset on unsubscribe (using flattenSequential)
		val outerEmitter = new StreamEmitter[Int]()
		val innerEmitter1 = new StreamEmitter[String]()
		val innerEmitter2 = new StreamEmitter[String]()

		val tensor = outerEmitter.flatMap {
			case 1 => innerEmitter1
			case 2 => innerEmitter2
			case _ => Flux.empty[String]
		}

		val flatSeq = tensor.flattenSequential
		var listSeq1 = List[String]()

		val seqSub1 = flatSeq.subscribe(new FluxObserver[String] {
			override def onNext(v: String, index: Int): Unit = listSeq1 = listSeq1 :+ v

			override def onError(ex: Throwable): Unit = ()

			override def onComplete(): Unit = ()
		})

		outerEmitter.emit(1)
		innerEmitter1.emit("a")
		innerEmitter1.emit("b")
		assertEquals(listSeq1, List("a", "b"))

		// Unsubscribe before outer complete
		seqSub1.unsubscribe()

		// Resubscribe to flatSeq
		var listSeq2 = List[(String, Int)]()
		val seqSub2 = flatSeq.subscribe(new FluxObserver[String] {
			override def onNext(v: String, index: Int): Unit = listSeq2 = listSeq2 :+ (v, index)

			override def onError(ex: Throwable): Unit = ()

			override def onComplete(): Unit = ()
		})

		// If state was reset, index starts back at 0
		outerEmitter.emit(2)
		innerEmitter2.emit("x")
		assertEquals(listSeq2, List(("x", 0)))
	}

	test("SpareSlotCapturerOp - mapping completed operators returns Keeper or Failed directly") {
		val captor = new Captor[Int]()
		val mapped = captor.map(_ * 2)

		// Complete upstream captor, which eagerly completes the mapped operator
		captor.capture(5)
		assert(mapped.isCompleted)

		// Map/flatMap the completed SpareSlotCapturerOp
		val mapped2 = mapped.map(_ + 10)
		val flatMapped = mapped.flatMap(v => new Keeper(s"val$v"))
		val failedMapped = mapped.mapGuarded[Int](_ => throw new RuntimeException("err"))

		// Assert that the returned instances are Keeper / Failed directly
		assert(mapped2.isInstanceOf[Keeper[?]], s"Expected Keeper, obtained ${mapped2.getClass.getName}")
		assert(flatMapped.isInstanceOf[Keeper[?]], s"Expected Keeper, obtained ${flatMapped.getClass.getName}")
		assert(failedMapped.isInstanceOf[Failed], s"Expected Failed, obtained ${failedMapped.getClass.getName}")

		var successVal = 0
		mapped2.subscribeCallbacks(v => successVal = v)
		assertEquals(successVal, 20)
	}

	test("Guarded operations - downstream observer exceptions are not caught by the operator try-catch") {
		val captor = new Captor[Int]()
		val mapped = captor.mapGuarded(_ * 2)

		var errorObserved = false
		val throwingObs = new MonoObserver[Int] {
			override def onSuccess(v: Int): Unit = throw new RuntimeException("downstream-error")

			override def onError(ex: Throwable): Unit = errorObserved = true
		}

		mapped.subscribe(throwingObs)

		// Complete upstream captor. This triggers onSuccess on throwingObs.
		// Since throwingObs throws a RuntimeException in onSuccess, it should NOT trigger its own onError.
		try {
			captor.capture(10)
		} catch {
			case ex: RuntimeException if ex.getMessage == "downstream-error" => // expected to propagate
		}

		assert(!errorObserved, "Downstream onSuccess exception should not have triggered onError on the observer")
	}

	test("Non-guarded operations - exceptions in f propagate directly") {
		// 1. Task.map
		val t = makeTask(10)
		val mappedTask = t.map(_ => throw new RuntimeException("nonguarded-err"))
		try {
			mappedTask.subscribeCallbacks(_ => ())
			fail("Expected RuntimeException")
		} catch {
			case ex: RuntimeException if ex.getMessage == "nonguarded-err" => // ok
		}

		// 2. Captor.flatMap
		val captor = new Captor[Int]()
		val flatMapped = captor.flatMap[Int]((_: Int) => (throw new RuntimeException("nonguarded-flat-err")): Mono[Int])
		flatMapped.subscribe(new MonoObserver[Int] {
			override def onSuccess(v: Int): Unit = ()

			override def onError(ex: Throwable): Unit = fail("Should not have caught the exception")
		})

		try {
			captor.capture(5)
			fail("Expected RuntimeException to propagate")
		} catch {
			case ex: RuntimeException if ex.getMessage == "nonguarded-flat-err" => // ok
		}
	}

	test("Task operators - spare slot reuse and delegate fallback") {
		var upstreamSubscribed = 0
		var upstreamUnsubscribed = 0
		val customTask = new Task[Int] {
			override def subscribe(observer: MonoObserver[Int]): Subscription = {
				upstreamSubscribed += 1
				new Subscription {
					override def unsubscribe(): Unit = {
						upstreamUnsubscribed += 1
					}
				}
			}
		}

		val mapped = customTask.map(_ * 2)

		// 1. First subscription uses primary slot
		val obs1 = new MonoObserver[Int] {
			override def onSuccess(v: Int): Unit = ()

			override def onError(ex: Throwable): Unit = ()
		}
		val sub1 = mapped.subscribe(obs1)
		assertEquals(upstreamSubscribed, 1)
		assert(sub1 eq mapped, "First subscription should return the operator itself")

		// 2. Second concurrent subscription falls back to delegate
		val obs2 = new MonoObserver[Int] {
			override def onSuccess(v: Int): Unit = ()

			override def onError(ex: Throwable): Unit = ()
		}
		val sub2 = mapped.subscribe(obs2)
		assertEquals(upstreamSubscribed, 2)
		assert(sub2 ne mapped, "Second subscription should return a delegate subscription")

		// 3. Early unsubscription propagates correctly
		sub1.unsubscribe()
		assertEquals(upstreamUnsubscribed, 1)

		sub2.unsubscribe()
		assertEquals(upstreamUnsubscribed, 2)
	}

	test("Task_FlatMap - spare slot reuse, delegate fallback and inner subscription unsubscription") {
		var upstreamSubscribed = 0
		var upstreamUnsubscribed = 0
		val customTask = new Task[Int] {
			override def subscribe(observer: MonoObserver[Int]): Subscription = {
				upstreamSubscribed += 1
				// Trigger onSuccess immediately to go into flatMap inner subscription
				observer.onSuccess(10)
				new Subscription {
					override def unsubscribe(): Unit = {
						upstreamUnsubscribed += 1
					}
				}
			}
		}

		var innerSubscribed = 0
		var innerUnsubscribed = 0
		val innerTask = new Task[String] {
			override def subscribe(observer: MonoObserver[String]): Subscription = {
				innerSubscribed += 1
				new Subscription {
					override def unsubscribe(): Unit = {
						innerUnsubscribed += 1
					}
				}
			}
		}

		val flatMapped = customTask.flatMap(_ => innerTask)

		// 1. First subscription
		val obs1 = new MonoObserver[String] {
			override def onSuccess(v: String): Unit = ()

			override def onError(ex: Throwable): Unit = ()
		}
		val sub1 = flatMapped.subscribe(obs1)
		assertEquals(upstreamSubscribed, 1)
		assertEquals(innerSubscribed, 1)
		assert(sub1 eq flatMapped, "First subscription should return the operator itself")

		// Unsubscribing flatMapped should unsubscribe from the inner subscription
		sub1.unsubscribe()
		assertEquals(innerUnsubscribed, 1)
	}

	test("Capturer.flatMap(Task) - static type verification and chaining") {
		val captor = new Captor[Int]()

		// This should compile because flatMap(A => Task[B]) returns Task[B]
		// and mapGuarded/guarded are available on Task[B]
		val mappedTask: Task[String] = captor
			.flatMap(v => makeTask(s"value: $v"))
			.mapGuarded(s => s + "!")
			.guarded

		var result = ""
		mappedTask.subscribeCallbacks(v => result = v)

		// Complete the captor to trigger the pipeline
		captor.capture(100)
		assertEquals(result, "value: 100!")
	}

	test("Task and Capturer factories - Task_apply and Task_defer") {
		// 1. Task_apply success (guarded & non-guarded)
		var t1Called = 0
		val t1 = Task_apply(() => {
			t1Called += 1
			100
		}, isGuarded = false)
		var t1Res = 0
		t1.subscribeCallbacks(v => t1Res = v)
		assertEquals(t1Called, 1)
		assertEquals(t1Res, 100)

		var t2Called = 0
		val t2 = Task_apply(() => {
			t2Called += 1
			200
		}, isGuarded = true)
		var t2Res = 0
		t2.subscribeCallbacks(v => t2Res = v)
		assertEquals(t2Called, 1)
		assertEquals(t2Res, 200)

		// 2. Task_apply error (guarded)
		val err = new RuntimeException("apply-guarded-err")
		val t3 = Task_apply[Int](() => {
			throw err
		}, isGuarded = true)
		var t3Err: Throwable | Null = null
		t3.subscribeCallbacks(_ => (), ex => t3Err = ex)
		assertEquals(t3Err, err)

		// 3. Task_defer success (guarded & non-guarded)
		var t4Called = 0
		val t4 = Task_defer(() => {
			t4Called += 1
			makeTask(300)
		}, isGuarded = false)
		var t4Res = 0
		t4.subscribeCallbacks(v => t4Res = v)
		assertEquals(t4Called, 1)
		assertEquals(t4Res, 300)

		var t5Called = 0
		val t5 = Task_defer(() => {
			t5Called += 1
			makeTask(400)
		}, isGuarded = true)
		var t5Res = 0
		t5.subscribeCallbacks(v => t5Res = v)
		assertEquals(t5Called, 1)
		assertEquals(t5Res, 400)

		// 4. Task_defer error (guarded)
		val deferErr = new RuntimeException("defer-guarded-err")
		val t6 = Task_defer[Int](() => {
			throw deferErr
		}, isGuarded = true)
		var t6Err: Throwable | Null = null
		t6.subscribeCallbacks(_ => (), ex => t6Err = ex)
		assertEquals(t6Err, deferErr)
	}

	test("Task and Capturer factories - Capturer_apply and Capturer_defer") {
		// 1. Capturer_apply success (guarded & non-guarded)
		var c1Called = 0
		val c1 = Capturer_apply(() => {
			c1Called += 1
			10
		}, isGuarded = false)
		var c1Res = 0
		c1.subscribeCallbacks(v => c1Res = v)
		assertEquals(c1Called, 1)
		assertEquals(c1Res, 10)

		var c2Called = 0
		val c2 = Capturer_apply(() => {
			c2Called += 1
			20
		}, isGuarded = true)
		var c2Res = 0
		c2.subscribeCallbacks(v => c2Res = v)
		assertEquals(c2Called, 1)
		assertEquals(c2Res, 20)

		// 2. Capturer_apply error (guarded)
		val err = new RuntimeException("capturer-apply-guarded-err")
		val c3 = Capturer_apply[Int](() => {
			throw err
		}, isGuarded = true)
		var c3Err: Throwable | Null = null
		c3.subscribeCallbacks(_ => (), ex => c3Err = ex)
		assertEquals(c3Err, err)

		// 3. Capturer_defer success (guarded & non-guarded)
		var c4Called = 0
		val c4 = Capturer_defer(() => {
			c4Called += 1
			new Keeper(30)
		}, isGuarded = false)
		var c4Res = 0
		c4.subscribeCallbacks(v => c4Res = v)
		assertEquals(c4Called, 1)
		assertEquals(c4Res, 30)

		var c5Called = 0
		val c5 = Capturer_defer(() => {
			c5Called += 1
			new Keeper(40)
		}, isGuarded = true)
		var c5Res = 0
		c5.subscribeCallbacks(v => c5Res = v)
		assertEquals(c5Called, 1)
		assertEquals(c5Res, 40)

		// 4. Capturer_defer error (guarded)
		val deferErr = new RuntimeException("capturer-defer-guarded-err")
		val c6 = Capturer_defer[Int](() => {
			throw deferErr
		}, isGuarded = true)
		var c6Err: Throwable | Null = null
		c6.subscribeCallbacks(_ => (), ex => c6Err = ex)
		assertEquals(c6Err, deferErr)
	}

	test("Task and Capturer factories - asynchronous cancellation verification") {
		// Use a custom DoerSandbox2 instance that executes sequentially with queuing (asynchronous behavior)
		class AsyncSandbox extends DoerSandbox2 {
			override type Tag = String
			override val tag: Tag = "AsyncSandbox"
			var queue = List[Runnable]()

			override def executeSequentially(runnable: Runnable): Unit = {
				queue = queue :+ runnable
			}

			override def currentExecutionSerial: ExecutionSerial = 0

			override def currentlyRunningDoer: Maybe[DoerSandbox2] = Maybe.empty

			override def reportFailure(cause: Throwable): Unit = throw cause

			def runPending(): Unit = {
				val q = queue
				queue = Nil
				q.foreach(_.run())
			}
		}
		val asyncSandbox = new AsyncSandbox

		// 1. Task_apply cancellation
		var taskApplyCalled = 0
		val tApply = asyncSandbox.Task_apply(() => {
			taskApplyCalled += 1
			999
		}, isGuarded = false)
		var tApplyRes = 0
		val subApply = tApply.subscribeCallbacks(v => tApplyRes = v)

		// Before running, unsubscribe
		subApply.unsubscribe()
		asyncSandbox.runPending()

		assertEquals(taskApplyCalled, 0)
		assertEquals(tApplyRes, 0)

		// 2. Task_defer cancellation
		var taskDeferCalled = 0
		val tDefer = asyncSandbox.Task_defer(() => {
			taskDeferCalled += 1
			asyncSandbox.Task_succeed(888)
		}, isGuarded = false)
		var tDeferRes = 0
		val subDefer = tDefer.subscribeCallbacks(v => tDeferRes = v)

		subDefer.unsubscribe()
		asyncSandbox.runPending()

		assertEquals(taskDeferCalled, 0)
		assertEquals(tDeferRes, 0)

		// 3. Capturer_apply cancellation
		var capApplyCalled = 0
		val cApply = asyncSandbox.Capturer_apply(() => {
			capApplyCalled += 1
			777
		}, isGuarded = false)
		var cApplyRes = 0

		// Subscribe to register a target observer
		val subCapApply = cApply.subscribeCallbacks(v => cApplyRes = v)
		// Since first subscription returns the capturer itself:
		assertEquals(subCapApply eq cApply, true)

		// Unsubscribe before executeSequentially runs
		subCapApply.unsubscribe()
		asyncSandbox.runPending()

		assertEquals(capApplyCalled, 0)
		assertEquals(cApplyRes, 0)

		// 4. Capturer_defer cancellation
		var capDeferCalled = 0
		val cDefer = asyncSandbox.Capturer_defer(() => {
			capDeferCalled += 1
			new asyncSandbox.Keeper(666)
		}, isGuarded = false)
		var cDeferRes = 0

		val subCapDefer = cDefer.subscribeCallbacks(v => cDeferRes = v)
		assertEquals(subCapDefer eq cDefer, true)

		subCapDefer.unsubscribe()
		asyncSandbox.runPending()

		assertEquals(capDeferCalled, 0)
		assertEquals(cDeferRes, 0)
	}
}

