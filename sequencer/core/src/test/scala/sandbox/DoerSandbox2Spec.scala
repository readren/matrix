package readren.sequencer
package sandbox

import sandbox.DoerSandbox2.{ANOTHER_AFTER, ExecutionSerial, ResultOrigin}

import munit.ScalaCheckEffectSuite
import org.scalacheck.Prop
import readren.common.Maybe

import scala.collection.mutable
import scala.reflect.ClassTag

class DoerSandbox2Spec extends ScalaCheckEffectSuite {

	private var forceSandboxInSequence = false

	def runInSequence[T](thunk: => T): T = {
		val prev = forceSandboxInSequence
		forceSandboxInSequence = true
		try {
			thunk
		} finally {
			forceSandboxInSequence = prev
		}
	}

	private val sandbox = new DoerSandbox2 {
		override type Tag = String
		override val tag: Tag = "Sandbox"

		override def executeSequentially(runnable: Runnable): Unit = {
			val previous = forceSandboxInSequence
			forceSandboxInSequence = true
			try {
				runnable.run()
			} finally {
				forceSandboxInSequence = previous
			}
		}

		override def currentExecutionSerial: ExecutionSerial = 0

		override def currentlyRunningDoer: Maybe[DoerSandbox2] = {
			if forceSandboxInSequence then Maybe(this) else Maybe.empty
		}
	}

	import sandbox.*

	private def makeTask[A](value: A): Task[A] = new Task[A] {
		override def subscribeSync(observer: MonoObserver[A]): Subscription = {
			observer.onSuccess(value)
			Subscription_empty
		}
	}

	test("Task operations - map, flatMap, mapGuarded, flatMapGuarded, guarded") {
		runInSequence {
			val t = makeTask(10)
			var mapResult = 0
			t.map(_ * 2).subscribeSyncCallbacks(v => mapResult = v)
			assertEquals(mapResult, 20)

			var flatMapResult = ""
			t.flatMap(v => makeTask(s"value: $v")).subscribeSyncCallbacks(v => flatMapResult = v)
			assertEquals(flatMapResult, "value: 10")

			val tGuarded = makeTask(10)
			var errorResult: Throwable | Null = null
			tGuarded.mapGuarded[Int] { v =>
				if v > 5 then {
					throw new Exception("guarded-err")
				}
				v
			}.subscribeSyncCallbacks(v => (), err => errorResult = err)
			assert(errorResult ne null)
			assertEquals(errorResult.nn.getMessage, "guarded-err")
		}
	}

	test("Capturer and Captor - map, flatMap, mapGuarded, flatMapGuarded, flatMapCapturer") {
		runInSequence {
			// Keeper (ready success path)
			val keeper = new Keeper(5)
			var keeperMap = 0
			keeper.map(_ * 2).subscribeSyncCallbacks(v => keeperMap = v)
			assertEquals(keeperMap, 10)

			// Failed (ready failure path)
			val ex = new Exception("fail")
			val failed = new Failed(ex)
			var failedErr: Throwable | Null = null
			failed.subscribeSyncCallbacks(_ => (), err => failedErr = err)
			assertEquals(failedErr, ex)

			// Captor lifecycle (pending, subscription, capturing, unsubscription)
			val captor = new Captor[Int]()
			var captorResult = 0
			val sub = captor.subscribeSyncCallbacks(v => captorResult = v)
			assertEquals(captor.isCompleted, false)
			assertEquals(captor.isPending, true)
			captor.capture(100)
			assertEquals(captor.isCompleted, true)
			assertEquals(captorResult, 100)

			// Unsubscription before capture
			val captor2 = new Captor[Int]()
			var captorResult2 = 0
			val sub2 = captor2.subscribeSyncCallbacks(v => captorResult2 = v)
			sub2.unsubscribe()
			captor2.capture(200)
			assertEquals(captorResult2, 0)
		}
	}

	test("Captor operators - unsubscription propagation upstream") {
		runInSequence {
			val captor = new Captor[Int]()
			val mapped = captor.map(_ * 2)
			var result = 0
			val sub = mapped.subscribeSyncCallbacks(v => result = v)
			sub.unsubscribe()
			captor.capture(50)
			assertEquals(result, 0)
		}
	}

	test("Flux factories - empty, apply, fromIterable, generate, generateStatefully") {
		runInSequence {
			// Flux.empty
			var completed = false
			Flux_empty.subscribeSync(new FluxObserver[Int] {
				override def onNext(v: Int, index: Int): Unit = ()

				override def onError(ex: Throwable): Unit = ()

				override def onComplete(): Unit = completed = true
			})
			assertEquals(completed, true)

			// Flux.apply
			var list = List[Int]()
			Flux_apply(1, 2, 3).subscribeSync(new FluxObserver[Int] {
				override def onNext(v: Int, index: Int): Unit = list = list :+ v

				override def onError(ex: Throwable): Unit = ()

				override def onComplete(): Unit = ()
			})
			assertEquals(list, List(1, 2, 3))

			// Flux.generateStatefully
			var genList = List[Int]()
			var genError = false
			val fluxGen = Flux_generateStatefully[Int] { () =>
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
			fluxGen.subscribeSync(new FluxObserver[Int] {
				override def onNext(v: Int, index: Int): Unit = genList = genList :+ v

				override def onError(ex: Throwable): Unit = genError = true

				override def onComplete(): Unit = ()
			})
			assertEquals(genList, List(0, 2, 4))
			assertEquals(genError, true)
		}
	}

	test("Flux operations - map, mapWithIndex, flatMap, scan, buffer, zip, take, takeWhile") {
		runInSequence {
			// map / mapWithIndex
			var mappedList = List[Int]()
			Flux_apply(1, 2).map(_ * 10).subscribeSync(new FluxObserver[Int] {
				override def onNext(v: Int, index: Int): Unit = mappedList = mappedList :+ v

				override def onError(ex: Throwable): Unit = ()

				override def onComplete(): Unit = ()
			})
			assertEquals(mappedList, List(10, 20))

			// scan
			var scannedList = List[Int]()
			Flux_apply(1, 2, 3).scan(0)((state, el, idx) => state + el).subscribeSync(new FluxObserver[Int] {
				override def onNext(v: Int, index: Int): Unit = scannedList = scannedList :+ v

				override def onError(ex: Throwable): Unit = ()

				override def onComplete(): Unit = ()
			})
			assertEquals(scannedList, List(1, 3, 6))

			// buffer
			var bufferedList = List[IArray[Int]]()
			Flux_apply(1, 2, 3, 4).buffer(2).subscribeSync(new FluxObserver[IArray[Int]] {
				override def onNext(v: IArray[Int], index: Int): Unit = bufferedList = bufferedList :+ v

				override def onError(ex: Throwable): Unit = ()

				override def onComplete(): Unit = ()
			})
			assertEquals(bufferedList.map(_.toList), List(List(1, 2), List(3, 4)))

			// take / takeWhile
			var takenList = List[Int]()
			Flux_apply(1, 2, 3, 4).take(2).subscribeSync(new FluxObserver[Int] {
				override def onNext(v: Int, index: Int): Unit = takenList = takenList :+ v

				override def onError(ex: Throwable): Unit = ()

				override def onComplete(): Unit = ()
			})
			assertEquals(takenList, List(1, 2))

			// zip
			var zippedList = List[Int]()
			Flux_apply(1, 2).zip(Flux_apply(10, 20))((left, right, idx) => left + right).subscribeSync(new FluxObserver[Int] {
				override def onNext(v: Int, index: Int): Unit = zippedList = zippedList :+ v

				override def onError(ex: Throwable): Unit = ()

				override def onComplete(): Unit = ()
			})
			assertEquals(zippedList, List(11, 22))
		}
	}

	test("Tensor operations - flattenInner, flattenOuter, flattenSequential") {
		runInSequence {
			val outerEmitter = new StreamEmitter[Int]()
			val innerEmitter1 = new StreamEmitter[String]()
			val innerEmitter2 = new StreamEmitter[String]()

			val tensor = outerEmitter.flatMap {
				case 1 => innerEmitter1
				case 2 => innerEmitter2
				case _ => Flux_empty
			}

			var innerList = List[String]()
			var outerList = List[String]()
			var seqList = List[String]()

			tensor.flattenInner.subscribeSync(new FluxObserver[String] {
				override def onNext(v: String, index: Int): Unit = innerList = innerList :+ v

				override def onError(ex: Throwable): Unit = ()

				override def onComplete(): Unit = ()
			})

			tensor.flattenOuter.subscribeSync(new FluxObserver[String] {
				override def onNext(v: String, index: Int): Unit = outerList = outerList :+ v

				override def onError(ex: Throwable): Unit = ()

				override def onComplete(): Unit = ()
			})

			tensor.flattenSequential.subscribeSync(new FluxObserver[String] {
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
	}

	test("FlatMap SubscriptionTracker & array resizing verification") {
		runInSequence {
			val outerEmitter = new StreamEmitter[Int]()
			val innerEmitters = Array.fill(12)(new StreamEmitter[String]())

			val tensor = outerEmitter.flatMap(idx => if idx < innerEmitters.length then innerEmitters(idx) else Flux_empty)
			var collected = List[String]()

			tensor.flattenSequential.subscribeSync(new FluxObserver[String] {
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
	}

	test("Property-based test: Task/Capturer mapping identity and composition") {
		runInSequence {
			Prop.forAll { (n: Int) =>
				val keeper = new Keeper(n)
				var mapVal = 0
				keeper.map(identity).subscribeSyncCallbacks(v => mapVal = v)

				val f = (x: Int) => x + 5
				val g = (x: Int) => x * 2
				var composedVal = 0
				keeper.map(f).map(g).subscribeSyncCallbacks(v => composedVal = v)

				var directComposedVal = 0
				keeper.map(x => g(f(x))).subscribeSyncCallbacks(v => directComposedVal = v)

				mapVal == n && composedVal == directComposedVal
			}
		}
	}

	test("Muxer/ObserverNest - duplicate observer subscriptions can be unsubscribed individually") {
		runInSequence {
			// 1. Test with StreamEmitter
			val emitter = new StreamEmitter[Int]()
			var count = 0
			val obs = new FluxObserver[Int] {
				override def onNext(v: Int, index: Int): Unit = count += 1

				override def onError(ex: Throwable): Unit = ()

				override def onComplete(): Unit = ()
			}

			val sub1 = emitter.subscribeSync(obs)
			val sub2 = emitter.subscribeSync(obs)

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

			val capSub1 = captor.subscribeSync(monoObs)
			val capSub2 = captor.subscribeSync(monoObs)

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

			val mapSub1 = mapped.subscribeSync(mappedObs)
			val mapSub2 = mapped.subscribeSync(mappedObs)

			// Unsubscribe second slot subscription (mapSub2)
			mapSub2.unsubscribe()

			// Complete upstream captor. This propagates to the mapped op, which propagates to active observers.
			captor2.capture(5)
			assertEquals(mappedSuccessCount, 1)
		}
	}

	test("SpareSlotFluxOp / SpareSlotTensorOp - resetState is called on unsubscribe to prevent state leakage") {
		runInSequence {
			// 1. Verify SpareSlotFluxOp state reset on unsubscribe (using Flux.take)
			val emitter = new StreamEmitter[Int]()
			val takeFlux = emitter.take(3)

			var list1 = List[Int]()
			val sub1 = takeFlux.subscribeSync(new FluxObserver[Int] {
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
			val sub2 = takeFlux.subscribeSync(new FluxObserver[Int] {
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
				case _ => Flux_empty
			}

			val flatSeq = tensor.flattenSequential
			var listSeq1 = List[String]()

			val seqSub1 = flatSeq.subscribeSync(new FluxObserver[String] {
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
			val seqSub2 = flatSeq.subscribeSync(new FluxObserver[String] {
				override def onNext(v: String, index: Int): Unit = listSeq2 = listSeq2 :+ (v, index)

				override def onError(ex: Throwable): Unit = ()

				override def onComplete(): Unit = ()
			})

			// If state was reset, index starts back at 0
			outerEmitter.emit(2)
			innerEmitter2.emit("x")
			assertEquals(listSeq2, List(("x", 0)))
		}
	}

	test("SpareSlotCapturerOp - mapping completed operators returns Keeper or Failed directly") {
		runInSequence {
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
			mapped2.subscribeSyncCallbacks(v => successVal = v)
			assertEquals(successVal, 20)
		}
	}

	test("Guarded operations - downstream observer exceptions are not caught by the operator try-catch") {
		runInSequence {
			val captor = new Captor[Int]()
			val mapped = captor.mapGuarded(_ * 2)

			var errorObserved = false
			val throwingObs = new MonoObserver[Int] {
				override def onSuccess(v: Int): Unit = throw new RuntimeException("downstream-error")

				override def onError(ex: Throwable): Unit = errorObserved = true
			}

			mapped.subscribeSync(throwingObs)

			// Complete upstream captor. This triggers onSuccess on throwingObs.
			// Since throwingObs throws a RuntimeException in onSuccess, it should NOT trigger its own onError.
			try {
				captor.capture(10)
			} catch {
				case ex: RuntimeException if ex.getMessage == "downstream-error" => // expected to propagate
			}

			assert(!errorObserved, "Downstream onSuccess exception should not have triggered onError on the observer")
		}
	}

	test("Non-guarded operations - exceptions in f propagate directly") {
		runInSequence {
			// 1. Task.map
			val t = makeTask(10)
			val mappedTask = t.map(_ => throw new RuntimeException("nonguarded-err"))
			try {
				mappedTask.subscribeSyncCallbacks(_ => ())
				fail("Expected RuntimeException")
			} catch {
				case ex: RuntimeException if ex.getMessage == "nonguarded-err" => // ok
			}

			// 2. Captor.flatMap
			val captor = new Captor[Int]()
			val flatMapped = captor.flatMap[Int]((_: Int) => (throw new RuntimeException("nonguarded-flat-err")): Mono[Int])
			flatMapped.subscribeSync(new MonoObserver[Int] {
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
	}

	test("Task operators - spare slot reuse and delegate fallback") {
		runInSequence {
			var upstreamSubscribed = 0
			var upstreamUnsubscribed = 0
			val customTask = new Task[Int] {
				override def subscribeSync(observer: MonoObserver[Int]): Subscription = {
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
			val sub1 = mapped.subscribeSync(obs1)
			assertEquals(upstreamSubscribed, 1)
			assert(sub1 eq mapped, "First subscription should return the operator itself")

			// 2. Second concurrent subscription falls back to delegate
			val obs2 = new MonoObserver[Int] {
				override def onSuccess(v: Int): Unit = ()

				override def onError(ex: Throwable): Unit = ()
			}
			val sub2 = mapped.subscribeSync(obs2)
			assertEquals(upstreamSubscribed, 2)
			assert(sub2 ne mapped, "Second subscription should return a delegate subscription")

			// 3. Early unsubscription propagates correctly
			sub1.unsubscribe()
			assertEquals(upstreamUnsubscribed, 1)

			sub2.unsubscribe()
			assertEquals(upstreamUnsubscribed, 2)
		}
	}

	test("Task_FlatMap - spare slot reuse, delegate fallback and inner subscription unsubscription") {
		runInSequence {
			var upstreamSubscribed = 0
			var upstreamUnsubscribed = 0
			val customTask = new Task[Int] {
				override def subscribeSync(observer: MonoObserver[Int]): Subscription = {
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
				override def subscribeSync(observer: MonoObserver[String]): Subscription = {
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
			val sub1 = flatMapped.subscribeSync(obs1)
			assertEquals(upstreamSubscribed, 1)
			assertEquals(innerSubscribed, 1)
			assert(sub1 eq flatMapped, "First subscription should return the operator itself")

			// Unsubscribing flatMapped should unsubscribe from the inner subscription
			sub1.unsubscribe()
			assertEquals(innerUnsubscribed, 1)
		}
	}

	test("Capturer.flatMap(Task) - static type verification and chaining") {
		runInSequence {
			val captor = new Captor[Int]()

			// This should compile because flatMap(A => Task[B]) returns Task[B]
			// and mapGuarded/guarded are available on Task[B]
			val mappedTask: Task[String] = captor
				.flatMap(v => makeTask(s"value: $v"))
				.mapGuarded(s => s + "!")
				.guarded

			var result = ""
			mappedTask.subscribeSyncCallbacks(v => result = v)

			// Complete the captor to trigger the pipeline
			captor.capture(100)
			assertEquals(result, "value: 100!")
		}
	}

	test("Captor - captureWith completion path, failure propagation, and ANOTHER_AFTER") {
		runInSequence {
			// 1. Success propagation
			val source1 = new Captor[Int]()
			val target1 = new Captor[Int]()
			target1.captureWith(source1)
			assertEquals(target1.isCompleted, false)

			source1.capture(99)
			assertEquals(target1.isCompleted, true)

			var successResult = 0
			target1.subscribeSyncCallbacks(v => successResult = v)
			assertEquals(successResult, 99)

			// 2. Failure propagation
			val source2 = new Captor[Int]()
			val target2 = new Captor[Int]()
			target2.captureWith(source2)

			val exception = new RuntimeException("source-err")
			source2.fail(exception)
			assertEquals(target2.isCompleted, true)

			var caughtErr: Throwable | Null = null
			target2.subscribeSyncCallbacks(_ => (), ex => caughtErr = ex)
			assertEquals(caughtErr, exception)

			// 3. ANOTHER_AFTER propagation
			val source3 = new Captor[Int]()
			val target3 = new Captor[Int]()
			var completedVal = 0
			var completedOrigin = -1
			target3.captureWith(source3, onCompleted = new CompletionObserver[Int] {
				override def onSuccess(v: Int, origin: ResultOrigin): Unit = {
					completedVal = v
					completedOrigin = origin
				}

				override def onError(ex: Throwable, origin: ResultOrigin): Unit = ()
			})

			// Complete the target first, before the source completes
			target3.capture(55)

			// Now complete the source (triggers the async callback)
			source3.capture(99)

			// The target's onSuccess should have been called with the target's original completed value (55)
			// and the origin should be ANOTHER_AFTER
			assertEquals(completedVal, 55)
			assertEquals(completedOrigin, ANOTHER_AFTER)
		}
	}

	test("Task and Capturer factories - Task_apply and Task_defer") {
		runInSequence {
			// 1. Task_apply success (guarded & non-guarded)
			var t1Called = 0
			val t1 = Task_apply(() => {
				t1Called += 1
				100
			}, isGuarded = false)
			var t1Res = 0
			t1.subscribeSyncCallbacks(v => t1Res = v)
			assertEquals(t1Called, 1)
			assertEquals(t1Res, 100)

			var t2Called = 0
			val t2 = Task_apply(() => {
				t2Called += 1
				200
			}, isGuarded = true)
			var t2Res = 0
			t2.subscribeSyncCallbacks(v => t2Res = v)
			assertEquals(t2Called, 1)
			assertEquals(t2Res, 200)

			// 2. Task_apply error (guarded)
			val err = new RuntimeException("apply-guarded-err")
			val t3 = Task_apply[Int](() => {
				throw err
			}, isGuarded = true)
			var t3Err: Throwable | Null = null
			t3.subscribeSyncCallbacks(_ => (), ex => t3Err = ex)
			assertEquals(t3Err, err)

			// 3. Task_defer success (guarded & non-guarded)
			var t4Called = 0
			val t4 = Task_defer(() => {
				t4Called += 1
				makeTask(300)
			}, isGuarded = false)
			var t4Res = 0
			t4.subscribeSyncCallbacks(v => t4Res = v)
			assertEquals(t4Called, 1)
			assertEquals(t4Res, 300)

			var t5Called = 0
			val t5 = Task_defer(() => {
				t5Called += 1
				makeTask(400)
			}, isGuarded = true)
			var t5Res = 0
			t5.subscribeSyncCallbacks(v => t5Res = v)
			assertEquals(t5Called, 1)
			assertEquals(t5Res, 400)

			// 4. Task_defer error (guarded)
			val deferErr = new RuntimeException("defer-guarded-err")
			val t6 = Task_defer[Int](() => {
				throw deferErr
			}, isGuarded = true)
			var t6Err: Throwable | Null = null
			t6.subscribeSyncCallbacks(_ => (), ex => t6Err = ex)
			assertEquals(t6Err, deferErr)
		}
	}

	test("Task and Capturer factories - Capturer_apply and Capturer_defer") {
		runInSequence {
			// 1. Capturer_apply success (guarded & non-guarded)
			var c1Called = 0
			val c1 = Capturer_apply(() => {
				c1Called += 1
				10
			}, isGuarded = false)
			var c1Res = 0
			c1.subscribeSyncCallbacks(v => c1Res = v)
			assertEquals(c1Called, 1)
			assertEquals(c1Res, 10)

			var c2Called = 0
			val c2 = Capturer_apply(() => {
				c2Called += 1
				20
			}, isGuarded = true)
			var c2Res = 0
			c2.subscribeSyncCallbacks(v => c2Res = v)
			assertEquals(c2Called, 1)
			assertEquals(c2Res, 20)

			// 2. Capturer_apply error (guarded)
			val err = new RuntimeException("capturer-apply-guarded-err")
			val c3 = Capturer_apply[Int](() => {
				throw err
			}, isGuarded = true)
			var c3Err: Throwable | Null = null
			c3.subscribeSyncCallbacks(_ => (), ex => c3Err = ex)
			assertEquals(c3Err, err)

			// 3. Capturer_defer success (guarded & non-guarded)
			var c4Called = 0
			val c4 = Capturer_defer(() => {
				c4Called += 1
				new Keeper(30)
			}, isGuarded = false)
			var c4Res = 0
			c4.subscribeSyncCallbacks(v => c4Res = v)
			assertEquals(c4Called, 1)
			assertEquals(c4Res, 30)

			var c5Called = 0
			val c5 = Capturer_defer(() => {
				c5Called += 1
				new Keeper(40)
			}, isGuarded = true)
			var c5Res = 0
			c5.subscribeSyncCallbacks(v => c5Res = v)
			assertEquals(c5Called, 1)
			assertEquals(c5Res, 40)

			// 4. Capturer_defer error (guarded)
			val deferErr = new RuntimeException("capturer-defer-guarded-err")
			val c6 = Capturer_defer[Int](() => {
				throw deferErr
			}, isGuarded = true)
			var c6Err: Throwable | Null = null
			c6.subscribeSyncCallbacks(_ => (), ex => c6Err = ex)
			assertEquals(c6Err, deferErr)
		}
	}

	test("Task and Capturer factories - asynchronous cancellation verification") {
		runInSequence {
			// Use a custom DoerSandbox2 instance that executes sequentially with queuing (asynchronous behavior)
			class AsyncSandbox extends DoerSandbox2 {
				override type Tag = String
				override val tag: Tag = "AsyncSandbox"
				private var queue = List[Runnable]()
				var forceInSequence = false

				override def executeSequentially(runnable: Runnable): Unit = {
					queue = queue :+ runnable
				}

				override def currentExecutionSerial: ExecutionSerial = 0

				override def currentlyRunningDoer: Maybe[DoerSandbox2] = {
					if forceInSequence then Maybe(this) else Maybe.empty
				}

				def runPending(): Unit = {
					val q = queue
					queue = Nil
					q.foreach { r =>
						val previous = forceInSequence
						forceInSequence = true
						try {
							r.run()
						} finally {
							forceInSequence = previous
						}
					}
				}
			}

			val asyncSandbox = new AsyncSandbox
			asyncSandbox.forceInSequence = true
			try {


				val failIfExecuted: Any => Unit = _ => fail("Part of a canceled subscription was executed")
				// 1. Task_apply cancellation
				{
					var executionsCounter = 0
					val task = asyncSandbox.Task_apply(() => {
						executionsCounter += 1
						999
					})
					val subscription = task.subscribeSyncCallbacks(v => assertEquals(v, 999))
					subscription.unsubscribe()
					asyncSandbox.runPending()
					assertEquals(executionsCounter, 1)
				}

				// 2. Task_defer cancellation
				{
					var factoryExecutionsCounter = 0
					val captor = new asyncSandbox.Captor[Int]()
					val task = asyncSandbox.Task_defer(() => {
						factoryExecutionsCounter += 1
						captor
					})
					val subscription1 = task.subscribeSyncCallbacks(failIfExecuted)
					var completionsCounter = 0
					val subscription2 = task.subscribeSyncCallbacks(v => {
						assertEquals(v, 888)
						completionsCounter += 1
					})
					val subscription3 = task.subscribeSyncCallbacks(v => {
						assertEquals(v, 888)
						completionsCounter += 1
					})
					subscription1.unsubscribe()
					captor.capture(888)
					subscription3.unsubscribe()
					asyncSandbox.runPending()
					assertEquals(factoryExecutionsCounter, 3)
					assertEquals(completionsCounter, 2)
				}
				// 3. Capturer_apply cancellation
				var capApplyCalled = 0
				val cApply = asyncSandbox.Capturer_apply(() => {
					capApplyCalled += 1
					777
				}, isGuarded = false)
				var cApplyRes = 0

				// Subscribe to register a target observer
				val subCapApply = cApply.subscribeSyncCallbacks(v => cApplyRes = v)
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

				val subCapDefer = cDefer.subscribeSyncCallbacks(v => cDeferRes = v)
				assertEquals(subCapDefer eq cDefer, true)

				subCapDefer.unsubscribe()
				asyncSandbox.runPending()

				assertEquals(capDeferCalled, 0)
				assertEquals(cDeferRes, 0)

			} finally {
				asyncSandbox.forceInSequence = false
			}
		}
	}

	test("Task and Capturer factories - un-guarded error propagation") {
		runInSequence {
			// Non-guarded Task_apply should propagate exception synchronously on subscribe
			val t = Task_apply[Int](() => throw new RuntimeException("unguarded-panic"), isGuarded = false)
			try {
				t.subscribeSyncCallbacks(_ => ())
				fail("Expected RuntimeException to propagate synchronously")
			} catch {
				case ex: RuntimeException if ex.getMessage == "unguarded-panic" => // ok
			}

			// Non-guarded Capturer_apply should propagate exception synchronously during construction
			try {
				Capturer_apply[Int](() => throw new RuntimeException("unguarded-panic"), isGuarded = false)
				fail("Expected RuntimeException to propagate synchronously during construction")
			} catch {
				case ex: RuntimeException if ex.getMessage == "unguarded-panic" => // ok
			}
		}
	}
	test("Task factories - laziness and reusability") {
		runInSequence {
			var evaluations = 0
			val t = Task_apply(() => {
				evaluations += 1
				evaluations
			})

			var res1 = 0
			var res2 = 0
			t.subscribeSyncCallbacks(v => res1 = v)
			t.subscribeSyncCallbacks(v => res2 = v)

			assertEquals(evaluations, 2)
			assertEquals(res1, 1)
			assertEquals(res2, 2)
		}
	}
	test("Task factories - succeed and fail") {
		runInSequence {
			var successVal = 0
			Task_succeed(42).subscribeSyncCallbacks(v => successVal = v)
			assertEquals(successVal, 42)
			val err = new RuntimeException("failed-task")
			var caughtErr: Throwable | Null = null
			Task_fail(err).subscribeSyncCallbacks(_ => (), ex => caughtErr = ex)
			assertEquals(caughtErr, err)
		}
	}

	test("Flux_fromMonosSequentially - success, out-of-order, error, and cancellation") {
		runInSequence {
			// 1. Success case with normal completion
			val m1 = makeTask(10)
			val m2 = makeTask(20)
			val flux = Flux_fromMonosSequentially(IArray(m1, m2))
			val list = mutable.ListBuffer[(Int, Int)]()
			var completed = false
			flux.subscribeSync(new FluxObserver[Int] {
				override def onNext(v: Int, index: Int): Unit = list.append((v, index))

				override def onError(ex: Throwable): Unit = ()

				override def onComplete(): Unit = completed = true
			})
			assertEquals(list.toList, List((10, 0), (20, 1)))
			assertEquals(completed, true)

			// 2. Out-of-order completion
			val captor1 = new Captor[Int]()
			val captor2 = new Captor[Int]()
			val fluxOO = Flux_fromMonosSequentially(IArray(captor1, captor2))
			val listOO = mutable.ListBuffer[(Int, Int)]()
			fluxOO.subscribeSync(new FluxObserver[Int] {
				override def onNext(v: Int, index: Int): Unit = listOO.append((v, index))

				override def onError(ex: Throwable): Unit = ()

				override def onComplete(): Unit = ()
			})
			// Complete captor2 first
			captor2.capture(200)
			// Then complete captor1
			captor1.capture(100)
			assertEquals(listOO.toList, List((200, 0), (100, 1)))

			// 3. Error case and cancellation of remaining monos
			var cancelled = false
			val subTask = new Task[Int] {
				override def subscribeSync(observer: MonoObserver[Int]): Subscription = {
					new Subscription {
						override def unsubscribe(): Unit = {
							cancelled = true
						}
					}
				}
			}
			val failedMono = Task_fail(new RuntimeException("failed-mono"))
			val fluxErr = Flux_fromMonosSequentially(IArray(subTask, failedMono))
			var gotError = false
			fluxErr.subscribeSync(new FluxObserver[Int] {
				override def onNext(v: Int, index: Int): Unit = ()

				override def onError(ex: Throwable): Unit = {
					gotError = true
				}

				override def onComplete(): Unit = ()
			})
			assertEquals(gotError, true)
			assertEquals(cancelled, true)
		}
	}

	test("Flux_fromMonos - success, out-of-order, error, and concurrent subscriber isolation") {
		runInSequence {
			// 1. Success case
			val m1 = makeTask(10)
			val m2 = makeTask(20)
			val flux = Flux_fromMonos(IArray(m1, m2))
			val list = mutable.ListBuffer[(Int, Int)]()
			var completed = false
			flux.subscribeSync(new FluxObserver[Int] {
				override def onNext(v: Int, index: Int): Unit = list.append((v, index))

				override def onError(ex: Throwable): Unit = ()

				override def onComplete(): Unit = completed = true
			})
			assertEquals(list.toList, List((10, 0), (20, 1)))
			assertEquals(completed, true)

			// 2. Out-of-order completion
			val captor1 = new Captor[Int]()
			val captor2 = new Captor[Int]()
			val fluxOO = Flux_fromMonos(IArray(captor1, captor2))
			val listOO = mutable.ListBuffer[(Int, Int)]()
			fluxOO.subscribeSync(new FluxObserver[Int] {
				override def onNext(v: Int, index: Int): Unit = listOO.append((v, index))

				override def onError(ex: Throwable): Unit = ()

				override def onComplete(): Unit = ()
			})
			// Complete captor2 first
			captor2.capture(200)
			// Then complete captor1
			captor1.capture(100)
			assertEquals(listOO.toList, List((200, 1), (100, 0))) // original indices preserved

			// 3. Error case and cancellation of remaining monos
			var cancelled = false
			val subTask = new Task[Int] {
				override def subscribeSync(observer: MonoObserver[Int]): Subscription = {
					new Subscription {
						override def unsubscribe(): Unit = {
							cancelled = true
						}
					}
				}
			}
			val failedMono = Task_fail(new RuntimeException("failed-mono"))
			val fluxErr = Flux_fromMonos(IArray(subTask, failedMono))
			var gotError = false
			fluxErr.subscribeSync(new FluxObserver[Int] {
				override def onNext(v: Int, index: Int): Unit = ()

				override def onError(ex: Throwable): Unit = {
					gotError = true
				}

				override def onComplete(): Unit = ()
			})
			assertEquals(gotError, true)
			assertEquals(cancelled, true)

			// 4. Concurrent subscriber isolation
			val captor = new Captor[Int]()
			val fluxIso = Flux_fromMonos(IArray(captor))

			var res1 = 0
			var res2 = 0
			val sub1 = fluxIso.subscribeSyncCallbacks((v, _) => res1 = v)
			val sub2 = fluxIso.subscribeSyncCallbacks((v, _) => res2 = v)

			// Unsubscribe sub1
			sub1.unsubscribe()

			// Complete the captor: only sub2 should get the value
			captor.capture(500)
			assertEquals(res1, 0)
			assertEquals(res2, 500)
		}
	}

	test("Mono and Flux - foreach, foreachWithCoords, trigger, triggerAndForget, subscribeAndForget inside and outside sequence context") {
		// 1. Within sequence context (using runInSequence)
		runInSequence {
			val captor = new Captor[Int]()
			var monoForeachResult = 0
			captor.foreach(v => monoForeachResult = v)
			captor.capture(123)
			assertEquals(monoForeachResult, 123)

			var monoTriggerResult = 0
			val captorTrigger = new Captor[Int]()
			captorTrigger.trigger(isWithinDoSerEx = true)(new MonoObserver[Int] {
				override def onSuccess(v: Int): Unit = monoTriggerResult = v

				override def onError(ex: Throwable): Unit = ()
			})
			captorTrigger.capture(456)
			assertEquals(monoTriggerResult, 456)

			val captorForget = new Captor[Int]()
			captorForget.triggerAndForget(isWithinDoSerEx = true)
			captorForget.capture(789)

			val emitter = new StreamEmitter[Int]()
			var fluxForeachResult = List[Int]()
			var fluxForeachCoordsResult = List[(Int, Int)]()

			emitter.foreach(v => fluxForeachResult = fluxForeachResult :+ v)
			emitter.foreachWithCoords((v, idx) => fluxForeachCoordsResult = fluxForeachCoordsResult :+ (v, idx))

			var fluxTriggerResult = List[Int]()
			emitter.trigger(isWithinDoSerEx = true)(new FluxObserver[Int] {
				override def onNext(v: Int, index: Int): Unit = fluxTriggerResult = fluxTriggerResult :+ v

				override def onError(ex: Throwable): Unit = ()

				override def onComplete(): Unit = ()
			})

			emitter.triggerAndForget(isWithinDoSerEx = true)
			emitter.subscribeAndForget(isWithinDoSerEx = true)

			emitter.emit(10)
			emitter.emit(20)

			assertEquals(fluxForeachResult, List(10, 20))
			assertEquals(fluxForeachCoordsResult, List((10, 0), (20, 1)))
			assertEquals(fluxTriggerResult, List(10, 20))
		}

		// 2. Outside sequence context (without runInSequence)
		// Thread-confined calls must fail with AssertionError when assertions are enabled.
		val captor2 = new Captor[Int]()
		intercept[AssertionError] {
			captor2.foreach(v => ())
		}

		intercept[AssertionError] {
			captor2.trigger(isWithinDoSerEx = true)(new MonoObserver[Int] {
				override def onSuccess(v: Int): Unit = ()

				override def onError(ex: Throwable): Unit = ()
			})
		}

		intercept[AssertionError] {
			captor2.triggerAndForget(isWithinDoSerEx = true)
		}

		val emitter2 = new StreamEmitter[Int]()
		intercept[AssertionError] {
			emitter2.foreach(v => ())
		}

		intercept[AssertionError] {
			emitter2.foreachWithCoords((v, idx) => ())
		}

		intercept[AssertionError] {
			emitter2.trigger(isWithinDoSerEx = true)(new FluxObserver[Int] {
				override def onNext(v: Int, index: Int): Unit = ()

				override def onError(ex: Throwable): Unit = ()

				override def onComplete(): Unit = ()
			})
		}

		intercept[AssertionError] {
			emitter2.triggerAndForget(isWithinDoSerEx = true)
		}

		intercept[AssertionError] {
			emitter2.subscribeAndForget(isWithinDoSerEx = true)
		}

		// Thread-safe gateway calls (with isWithinDoSerEx = false) must NOT fail outside sequence context
		var monoThreadSafeResult = 0
		val captor3 = new Captor[Int]()
		captor3.trigger(isWithinDoSerEx = false)(new MonoObserver[Int] {
			override def onSuccess(v: Int): Unit = monoThreadSafeResult = v

			override def onError(ex: Throwable): Unit = ()
		})
		captor3.capture(111, isWithinDoSerEx = false)
		assertEquals(monoThreadSafeResult, 111)

		val captor4 = new Captor[Int]()
		intercept[AssertionError] {
			captor4.capture(222, isWithinDoSerEx = true)
		}
	}
}


