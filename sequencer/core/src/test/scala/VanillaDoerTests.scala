package readren.sequencer

import CausalFence.{ROLLBACK_APPLIED, ROLLBACK_IGNORED, RollbackApplication}
import GeneratorsForDoerTests.{*, given}

import org.scalacheck.effect.PropF
import org.scalacheck.{Arbitrary, Gen}
import readren.common.Maybe

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/** Trait containing tests for vanilla [[Doer]] primitives, concurrency, exception handling, Task, Captor, and CausalFence.
 */
trait VanillaDoerTests[D <: Doer : ClassTag] { self: DoerProviderTestBase[D] =>

	////////// DOER INFRASTRUCTURE ////////

	test("`Doer.execute` executes in a decoupled manner.") {
		val generators = getGenerators
		import generators.*

		val promise = Promise[Unit]()

		given Promise[Unit] = promise

		var mutable = 1

		val task = doer.Task_apply { () =>

			def m12(): Unit = {
				if mutable != 1 then break(s"An execute was not decoupled 1: mutable=$mutable")
				mutable = 2
			}

			doer.run(m12())

			inline def m23(): Unit = {
				if mutable != 2 then break(s"An execute was not decoupled 2: mutable=$mutable")
				mutable = 3
			}

			doer.run(m23())

			def m34(): Unit = {
				if mutable != 3 then break(s"An execute was not decoupled 3: mutable=$mutable")
				mutable = 4
			}

			doer.run(m34())

			def end(): Unit = {
				if mutable != 4 then break(s"An execute was not decoupled 4: mutable=$mutable")
				promise.trySuccess(())
			}

			doer.run(end())
			if mutable != 1 then break(s"An execute was not decoupled 0: mutable=$mutable")
		}

		task.triggerAndForget(false)
		gate
	}

	test("Doer should execute ventures sequentially") {
		val doer = getSharedDoer
		val results = new AtomicInteger(0)
		val executionOrder = new AtomicInteger(0)
		val latch = new CountDownLatch(3)

		// Submit three tasks that should execute in order
		doer.executeSequentially { () =>
			results.set(1)
			executionOrder.set(1)
			latch.countDown()
		}

		doer.executeSequentially { () =>
			results.set(2)
			executionOrder.set(2)
			latch.countDown()
		}

		doer.executeSequentially { () =>
			results.set(3)
			executionOrder.set(3)
			latch.countDown()
		}

		// Wait for all tasks to complete
		assert(latch.await(50, TimeUnit.MILLISECONDS), "All runnables should complete within timeout")
		assert(results.get == 3, "Last venture should set result to 3")
		assert(executionOrder.get == 3, "Last venture should set execution order to 3")
	}

	//// CONCURRENCY TESTS ////

	test("Multiple doers should execute runnables concurrently") {
		assert(Runtime.getRuntime.availableProcessors() >= 3)

		val doer1 = buildDoer("doer-1")
		val doer2 = buildDoer("doer-2")
		val doer3 = buildDoer("doer-3")

		val startLatch = new CountDownLatch(3)
		val endLatch = new CountDownLatch(3)

		// Submit tasks to different doers simultaneously
		doer1.executeSequentially { () =>
			startLatch.countDown()
			println(s"Doer1 start:${System.currentTimeMillis()}")
			Thread.sleep(100)
			endLatch.countDown()
			println(s"Doer1 end:${System.currentTimeMillis()}")
		}

		doer2.executeSequentially { () =>
			startLatch.countDown()
			println(s"Doer2 start:${System.currentTimeMillis()}")
			Thread.sleep(100)
			endLatch.countDown()
			println(s"Doer3 end:${System.currentTimeMillis()}")
		}

		doer3.executeSequentially { () =>
			startLatch.countDown()
			println(s"Doer3 start:${System.currentTimeMillis()}")
			Thread.sleep(100)
			endLatch.countDown()
			println(s"Doer3 end:${System.currentTimeMillis()}")
		}

		// If tasks were truly concurrent, they should start without waiting any other to finish.
		assert(startLatch.await(90, TimeUnit.MILLISECONDS), "All runnables should start soon.")

		// If tasks were truly concurrent, total time should be close to 100ms, not 300ms
		assert(endLatch.await(250, TimeUnit.MILLISECONDS), "All runnables should complete")
	}

	test("Ventures should see memory updates from previous runnable in the same doer") {
		val doer = getSharedDoer
		var sharedCounter = 0
		val latch = new CountDownLatch(5)

		// Submit multiple tasks that increment the shared counter
		for i <- 0 until 5 do {
			doer.executeSequentially { () =>
				val currentValue = sharedCounter
				sharedCounter = currentValue + 1
				latch.countDown()
			}
		}

		assert(latch.await(5, TimeUnit.SECONDS), "All runnables should complete")
		assert(sharedCounter == 5, "Counter should be incremented 5 times")
	}

	test("Worker threads should be reused efficiently") {
		val numberOfVenturesPerDoer = 999
		val numberOfDoers = 9
		val latch = new CountDownLatch(numberOfVenturesPerDoer * numberOfDoers)
		val threadIds = new java.util.concurrent.ConcurrentLinkedQueue[Long]()

		// Submit multiple tasks in different doers and collect thread IDs
		val doers = Array.tabulate[Doer](numberOfDoers)(i => buildDoer(s"$i"))
		for ventureNumber <- 0 until numberOfVenturesPerDoer do {
			for doer <- doers do {
				doer.executeSequentially { () =>
					threadIds.add(Thread.currentThread().threadId)
					latch.countDown()
				}
			}
		}

		assert(latch.await(1, TimeUnit.SECONDS), "All runnables should complete")

		// Should have used multiple threads (concurrent execution)
		val uniqueThreads = threadIds.toArray.toSet.size
		assert(uniqueThreads > 1, s"Should use multiple threads, used: $uniqueThreads")
	}

	//// EXCEPTION HANDLING TESTS ////

	test("Doer should handle exceptions in runnables gracefully") {
		val doer = getSharedDoer
		val latch = new CountDownLatch(2)
		val exceptionCaught = new AtomicBoolean(false)

		// Submit a Runnable that throws an exception
		doer.executeSequentially { () =>
			throw new RuntimeException("Test exception")
		}

		// Submit a Runnable that should still execute after the exception
		doer.executeSequentially { () =>
			exceptionCaught.set(true)
			latch.countDown()
		}

		// Submit another normal venture
		doer.executeSequentially { () =>
			latch.countDown()
		}

		assert(latch.await(5, TimeUnit.SECONDS), "`Runnable` after exception should still execute")
		assert(exceptionCaught.get, "`Runnable` after exception should have executed")
	}

	test("The DoerProvider.onUnhandledException handler should be called immediately when the Runnable passed to executeSequentially throws an exception.") {
		val mainDoer = getSharedDoer

		PropF.forAllNoShrinkF { (exception: Throwable) =>
			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			observingUnhandledExceptionsDo { () =>
				mainDoer.executeSequentially { () => throw exception }
				mainDoer.executeSequentially { () => if !promise.isCompleted then break(s"The next Runnable was executed before the onUnhandledException: $exception") }
				breakAfterWaiting(999, s"No notification of the exception $exception until 999 milliseconds after applying the operation. Waiting aborted.")

			} { (doer, e) =>
				if e ne exception then break(s"An unexpected exception was caught: $e != $exception")
				else if doer ne mainDoer then break(s"Correct doer should be captured: ${doer.tag} != ${mainDoer.tag}")
				else promise.trySuccess(())
			}
		}
	}

	test("The `DoerProvider` should notify uncaught exceptions thrown by the Runnable passed to `Doer.executeSequentially` before executing the next enqueued Runnable") {
		val mainDoer = getSharedDoer

		PropF.forAllNoShrinkF { (exception: Throwable) =>

			val promise = Promise[Unit]()
			val wasCaught = new java.util.concurrent.atomic.AtomicBoolean(false)

			given Promise[Unit] = promise

			observingUnhandledExceptionsDo { () =>
				mainDoer.executeSequentially(() => throw exception)
				mainDoer.executeSequentially { () =>
					if wasCaught.get() then promise.trySuccess(()) else break("The uncaught exception was not notified")
				}

				breakAfterWaiting(999, s"No notification of the exception $exception until 990 milliseconds after applying the operation. Waiting aborted.")

			} { (d, t) =>
				if t eq exception then wasCaught.set(true) else break(s"an unexpected exception was uncaught $t")
			}
		}
	}

	//// STRESS TESTS ////

	test("Provider should handle high load") {
		val doer = getSharedDoer
		val runnablesCount = 100
		val latch = new CountDownLatch(runnablesCount)
		val results = new AtomicInteger(0)

		// Submit many tasks
		for _ <- 1 to runnablesCount do {
			doer.executeSequentially { () =>
				results.incrementAndGet()
				latch.countDown()
			}
		}

		assert(latch.await(10, TimeUnit.SECONDS), "All runnables should complete")
		assert(results.get == runnablesCount, s"All $runnablesCount runnables should have executed")
	}

	test("Provider should handle multiple doers with high load") {
		val doerCount = 10
		val runnablesPerDoer = 20
		val latch = new CountDownLatch(doerCount * runnablesPerDoer)
		val results = new AtomicInteger(0)

		// Create multiple doers and submit tasks to each
		for doerIndex <- 1 to doerCount do {
			val doer = buildDoer(s"stress-doer-$doerIndex")
			for _ <- 1 to runnablesPerDoer do {
				doer.executeSequentially { () =>
					results.incrementAndGet()
					latch.countDown()
				}
			}
		}

		assert(latch.await(15, TimeUnit.SECONDS), "All runnables should complete")
		assert(results.get == doerCount * runnablesPerDoer, s"All ${doerCount * runnablesPerDoer} runnables should have executed")
	}

	//// EDGE CASE TESTS ////

	test("Provider should handle rapid venture submission") {
		val doer = getSharedDoer
		val latch = new CountDownLatch(50)
		val results = new AtomicInteger(0)

		// Submit tasks rapidly without waiting
		for _ <- 1 to 50 do {
			doer.executeSequentially { () =>
				results.incrementAndGet()
				latch.countDown()
			}
		}

		assert(latch.await(5, TimeUnit.SECONDS), "All rapid runnables should complete")
		assert(results.get == 50, "All 50 rapid runnables should have executed")
	}

	test("Provider should maintain venture ordering under concurrent submission") {
		val doer = getSharedDoer
		val runnablesCount = 20
		val latch = new CountDownLatch(runnablesCount)
		val executionOrder = new java.util.concurrent.ConcurrentLinkedQueue[Int]()

		// Submit tasks from multiple threads
		val futures = for i <- 1 to runnablesCount yield {
			Future {
				doer.executeSequentially { () =>
					executionOrder.add(i)
					latch.countDown()
				}
			}
		}

		// Wait for all tasks to complete
		Future.sequence(futures)
		assert(latch.await(5, TimeUnit.SECONDS), "All runnables should complete")

		// Verify that tasks were executed in some order
		val orderList = executionOrder.toArray.toList
		assert(orderList.size == runnablesCount, s"All $runnablesCount runnables should have been executed")
		assert(orderList.toSet.size == runnablesCount, "All venture IDs should be unique")
	}

	////////// TASK //////////

	private def checkEquality[A](doer: Doer)(task1: doer.Task[A], task2: doer.Task[A], clue: => Any = "duties yield different results")(using CanEqual[A, A]): Future[Unit] = {
		val promise = Promise[Unit]()

		given Promise[Unit] = promise

		task1.subscribe(false)(new doer.MonoObserver[A] {
			override def onSuccess(a1: A): Unit = {
				task2.subscribeSync(new doer.MonoObserver[A] {
					override def onSuccess(a2: A): Unit = if a1 == a2 then promise.trySuccess(()) else break(s"$a1 is not equal to $a2")

					override def onError(e2: Throwable): Unit = break(s"$a1 is not equal to $e2")
				})
			}

			override def onError(e1: Throwable): Unit = {
				task2.subscribeSync(new doer.MonoObserver[A] {
					override def onSuccess(a2: A): Unit = break(s"$e1 is not equal to $a2")

					override def onError(e2: Throwable): Unit = if e1 ==== e2 then promise.trySuccess(()) else break(s"$e1 is not equal to $e2")
				})
			}
		})
		promise.future
	}

	test("Task: left identity") {
		val generators = getGenerators
		import generators.*
		PropF.forAllF { (x: Int, f: Int => Task[Int]) =>
			val left: doer.Task[Int] = Task_ready(x).flatMap(f)
			val right: doer.Task[Int] = f(x)
			checkEquality(doer)(left, right)
		}
	}

	test("Task: right identity") {
		val generators = getGenerators
		import generators.*
		PropF.forAllF { (m: Task[Int]) =>
			val left = m.flatMap(Task_ready)
			val right = m
			checkEquality(doer)(left, right)
		}
	}

	test("Task: associativity") {
		val generators = getGenerators
		import generators.*

		PropF.forAllF { (m: Task[Int], f: Int => Task[Int], g: Int => Task[Int]) =>
			val leftAssoc = m.flatMap(f).flatMap(g)
			val rightAssoc = m.flatMap(x => f(x).flatMap(g))
			checkEquality(doer)(leftAssoc, rightAssoc)
		}
	}

	test("Task: can be transformed with map") {
		val generators = getGenerators
		import generators.*

		PropF.forAllF { (m: Task[Int], f: Int => String) =>
			val left = m.map(f)
			val right = m.flatMap(a => Task_ready(f(a)))
			checkEquality(doer)(left, right)
		}
	}

	test("Task: can be recovered from non fatal failure") {
		val generators = getGenerators
		import generators.*

		PropF.forAllF { (thrownException: Throwable, f: Function[Throwable, Int]) =>
			val int = f(thrownException)
			val fIsDefined = int % 2 == 0

			if NonFatal(thrownException) then {
				val leftTask = Task_fail(thrownException).recover { e =>
					if fIsDefined then Maybe(int) else Maybe.empty
				}
				val rightVenture = if fIsDefined then Task_ready(int) else Task_fail(thrownException)
				checkEquality(doer)(leftTask, rightVenture)
			} else Future.successful(())
		}
	}

	test("Task: any pair of task can be combined") {
		val generators = getGenerators
		import generators.*

		PropF.forAllF { (taskA: Task[Int], taskB: Task[Int], f: (Int, Int) => Int) =>
			import scala.concurrent.ExecutionContext.Implicits.global
			val combinedTask = Task_combine(taskA, taskB)(f)

			val fCombined = combinedTask.toFuture().transform(scala.util.Success.apply)
			val fA = taskA.toFuture().transform(scala.util.Success.apply)
			val fB = taskB.toFuture().transform(scala.util.Success.apply)

			for {
				combinedResult <- fCombined
				taskAResult <- fA
				taskBResult <- fB
			} yield {
				val expected = for {
					a <- taskAResult
					b <- taskBResult
				} yield f(a, b)
				import readren.sequencer.GeneratorsForDoerTests.====
				assert(combinedResult ==== expected || (taskAResult.isFailure && combinedResult ==== taskAResult) || (taskBResult.isFailure && combinedResult ==== taskBResult), s"combinedResult: $combinedResult != expected: $expected")
			}
		}
	}

	test("Task: `doer.Task.foreign(foreignDoer)(foreignTask)` should complete in the `doer`'s thread") {
		val generators = getGenerators
		import generators.*
		PropF.forAllNoShrinkF {
			for {
				expectedInt <- smallIntGen
				expectedResult <- genTryFrom[Int](expectedInt, "expectedResult")
				foreignTask <- foreignDoerGenerators(true).genTaskFrom(expectedResult)
			} yield (expectedInt, expectedResult, foreignTask)
		} { case (expectedInt, expectedResult, foreignTask) =>
			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			doer.Task_from(foreignDoer)(foreignTask).subscribeSync(new MonoObserver[Int] {
				override def onSuccess(a: Int): Unit = {
					if a != expectedInt || expectedResult.isFailure then break(s"Unexpected result: Success($a) != $expectedResult")
					else if !doer.isInSequence then break(s"The observer wasn't executed within the DoSerEx")
					else promise.trySuccess(())
				}

				override def onError(e: Throwable): Unit = {
					if expectedResult.fold(_ ne e, _ => true) then break(s"Unexpected result: Failure($e) != $expectedResult")
					else if !doer.isInSequence then break(s"The observer wasn't executed within the DoSerEx")
					else promise.trySuccess(())
				}
			})
			promise.future
		}
	}

	//// TASK EXCEPTION HANDLING ////

	test("Task exception handling: if a function operand passed to a Task operation throws an exception then, non-fatal exceptions should be propagated or uncaught depending on the operation is guarded or not.") {
		forAllTaskOperandExceptions { (successfulTaskParam, failingTaskExceptionParam, failingTaskParam, expectedUnhandledExceptionParam) =>
			val generators = getGenerators
			import generators.*

			def check[R](opName: String, operatedTask: Task[R], shouldPropagateNonFatales: Boolean = false): Future[Unit] = {
				val promise = Promise[Unit]()
				checkTaskOperandExceptionHandling(opName, operatedTask, failingTaskExceptionParam, expectedUnhandledExceptionParam, shouldPropagateNonFatales)(using promise)
			}

			def f0[A](): A = throw expectedUnhandledExceptionParam

			def f1[A, B](a: A): B = throw expectedUnhandledExceptionParam

			def f2[A, B, C](a: A, b: B): C = throw expectedUnhandledExceptionParam

			val sTask = successfulTaskParam.asInstanceOf[Task[Int]]
			val fTask = failingTaskParam.asInstanceOf[Task[Int]]

			for {
				_ <- check("apply", Task_apply(f0))
				_ <- check("defer", Task_defers(f0))
				_ <- check("fromForeign", Task_from(foreignDoer)(foreignDoer.Task_apply(f0)))
				_ <- check("fromFutureBuilder", Task_from(f0, false), false)
				_ <- check("fromFutureBuilderGuarded", Task_from(f0, true), true)

				_ <- check("combine1", Task_combine(sTask, fTask)(f2))
				_ <- check("combine2", Task_combine(fTask, sTask)(f2))

				_ <- check("withFilter", sTask.withFilter(f1))
				_ <- check("withFilterGuarded", sTask.withFilterGuarded(f1), true)

				_ <- check("andThen1", sTask.andThen(f1))
				_ <- check("andThen2", fTask.andThen(_ => (), f1))

				_ <- check("map", sTask.map(f1))
				_ <- check("mapGuarded", sTask.mapGuarded(f1), true)

				_ <- check("flatMap", sTask.flatMap(f1))
				_ <- check("flatMapGuarded", sTask.flatMapGuarded(f1), true)

				_ <- check("transform1", sTask.transform(f1))
				_ <- check("transform2", fTask.transform(f1))
				_ <- check("guarded.transform3", sTask.guarded.transform(f1), true)
				_ <- check("guarded.transform4", fTask.guarded.transform(f1), true)

				_ <- check("transformWith1", sTask.transformWith(f1))
				_ <- check("transformWith2", fTask.transformWith(f1))
				_ <- check("guarded.transformWith1", sTask.guarded.transformWith(f1), true)
				_ <- check("guarded.transformWith2", fTask.guarded.transformWith(f1), true)

				_ <- check("recover", fTask.recover(f1))
				_ <- check("guarded.recover", fTask.guarded.recover(f1), true)

				_ <- check("recoverWith", fTask.recoverWith(f1))
				_ <- check("guarded.recoverWith", fTask.guarded.recoverWith(f1), true)
			} yield ()
		}
	}

	test("Task exception handling: `subscribe` should not catch exceptions thrown by the passed `MonoObserver` methods") {
		forAllSubscribeExceptions { (task1Param, task2Param, thrownExceptionParam, futureParam) =>
			val generators = getGenerators
			import generators.*

			def check[R](opName: String, operatedTask: Task[R]): Future[Unit] = {
				val promise = Promise[Unit]()
				checkMonoObserverExceptionNotCaught(opName, operatedTask, thrownExceptionParam)(using promise)
			}

			val t1 = task1Param.asInstanceOf[Task[Int]]
			val t2 = task2Param.asInstanceOf[Task[Int]]

			val randomInt = thrownExceptionParam.getMessage.hashCode()
			val smallNonNegativeInt = randomInt % 9
			val randomBool = (randomInt % 2) == 0
			val randomTryInt = if randomBool then Success(randomInt) else Failure(thrownExceptionParam)

			for {
				_ <- check("ready", Task_ready(randomInt))
				_ <- check("fail", Task_fail(thrownExceptionParam))
				_ <- check("apply", Task_apply(() => randomInt))
				_ <- check("defer", Task_apply(() => t1))
				_ <- check("fromForeign", Task_from(foreignDoer)(foreignDoer.Task_apply(() => randomInt)))
				_ <- check("fromFuture", Task_from(futureParam))
				_ <- check("fromFutureDeferred", Task_from(() => futureParam))

				_ <- check("withFilter", t1.withFilter(_ => randomBool))
				_ <- check("withFilterGuarded", t1.withFilterGuarded(_ => randomBool))

				_ <- check("andThen1", t1.andThen(_ => (), _ => ()))

				_ <- check("map", t1.map(identity))
				_ <- check("mapGuarded", t1.mapGuarded(identity))

				_ <- check("flatMap", t1.flatMap(_ => t2))
				_ <- check("flatMapGuarded", t1.flatMapGuarded(_ => t2))

				_ <- check("transform", t1.transform(identity))
				_ <- check("guarded.transform", t1.guarded.transform(identity))

				_ <- check("transformWith", t1.transformWith(_ => t2))
				_ <- check("guarded.transformWith", t1.guarded.transformWith(_ => t2))

				_ <- check("recover", t1.recover { _ => if randomBool then Maybe(randomInt) else Maybe.empty })
				_ <- check("guarded.recover", t1.guarded.recover { _ => if randomBool then Maybe(randomInt) else Maybe.empty })

				_ <- check("recoverWith", t1.recoverWith { _ => if randomBool then Maybe(t2) else Maybe.empty })
				_ <- check("guarded.recoverWith", t1.guarded.recoverWith { _ => if randomBool then Maybe(t2) else Maybe.empty })
			} yield ()
		}
	}

	//// CAPTOR ////

	test("Captor: `Captor.seize(v)` should trigger a single execution, passing `v`, of each subscribed consumers it has wired.") {
		val generators = getGenerators
		import generators.*
		PropF.forAllF(
			for {
				expectedSuccessfulResult <- smallIntGen
				expectedResult <- genTryFrom(expectedSuccessfulResult, "expected successful result")
				numberOfPendingSubscriptions <- Gen.choose(1, 17)
			} yield (expectedSuccessfulResult, expectedResult, numberOfPendingSubscriptions)
		) { case (expectedSuccessfulResult, expectedResult, numberOfPendingSubscriptions) =>
			val promise = Promise[Unit]()
			val testedCaptor = doer.Captor[Int]()
			checkCaptor[doer.type](doer, testedCaptor, promise, expectedSuccessfulResult, expectedResult, numberOfPendingSubscriptions, () => testedCaptor.seize(expectedResult))
			gate(using promise)
		}
	}

	test("Captor: `Captor.seizeWith(task)` should trigger a single execution, passing what `task` shields, of each subscribed consumers it has wired.") {
		val generators = getGenerators
		import generators.{taskArbitrary, *}
		PropF.forAllF(
			for {
				expectedSuccessfulResult <- smallIntGen
				expectedResult <- genTryFrom(expectedSuccessfulResult, "expected successful result")
				numberOfPendingSubscriptions <- Gen.choose(1, 17)
			} yield (expectedSuccessfulResult, expectedResult, numberOfPendingSubscriptions)
		) { case (expectedSuccessfulResult, expectedResult, numberOfPendingSubscriptions) =>
			val promise = Promise[Unit]()

			val testedCaptor = doer.Captor[Int]()
			val completingCaptor = doer.Captor[Int]()
			testedCaptor.seizeWith(completingCaptor)
			checkCaptor[doer.type](doer, testedCaptor, promise, expectedSuccessfulResult, expectedResult, numberOfPendingSubscriptions, () => completingCaptor.seize(expectedResult))

			gate(using promise)
		}
	}

	private def checkCaptor[DD <: Doer](doer: DD, testedCaptor: doer.Captor[Int], promise: Promise[Unit], expectedSuccessfulResult: Int, expectedResult: Try[Int], numberOfPendingSubscriptions: Int, capture: () => Unit): Unit = {
		given Promise[Unit] = promise

		import doer.*

		val notifiedObserversCdl = CountDownLatch(numberOfPendingSubscriptions)
		for index <- 0 until numberOfPendingSubscriptions do {
			testedCaptor.subscribe(false)(new MonoObserver[Int] {
				override def onSuccess(a: Int): Unit = {
					if expectedResult.fold(_ => true, _ != a) then break(s"the observer #$index received an unexpected value: Success($a) != $expectedResult")
					notifiedObserversCdl.countDown()
				}

				override def onError(e: Throwable): Unit = {
					if expectedResult.fold(_ ne e, _ => true) then break(s"the observer #$index received an unexpected value: Failure($e) != $expectedResult")
					notifiedObserversCdl.countDown()
				}
			})
		}

		doer.executeSequentially { () =>
			try {
				if !testedCaptor.isPending then break("`isPending` returned false despite no capturing was done")
				if testedCaptor.isCompleted then break("`isCompleted` returned true despite no capturing was done")
				capture()
				if testedCaptor.isPending then break("`isPending` returned true despite the capturing was done")
				if !testedCaptor.isCompleted then break("`isCompleted` returned false despite the capturing was done")
			} catch {
				case cause: Throwable => promise.tryFailure(cause)
			}
		}

		if notifiedObserversCdl.await(99, TimeUnit.MILLISECONDS) then promise.trySuccess(())
		else break(s"At least one observer has not been notified")
	}

	//// CAUSAL FENCE

	test("CausalFence: multiple stepped advances should serialize and commit in order") {
		val generators = getGenerators
		import generators.*

		PropF.forAllNoShrinkF { (initial: Int, updater: Int => Capturer[Int]) =>
			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			val fence = CausalFence[Int, doer.type](doer)(initial)

			def loop(currentValue: Int, repetition: Int): Unit = {
				if repetition == 9 then promise.trySuccess(())
				else {
					updater(currentValue).triggerHardy(true) { expectedNextState =>
						val advanceCapturer = fence.advance[Int] { previousValue =>
							if previousValue != currentValue then break(s"repetition #$repetition mismatch")
							updater(previousValue)
						}
						advanceCapturer.triggerCallbacks(true)(
							actualNextSuccessfulState => {
								if expectedNextState.fold(_ => true, _ != actualNextSuccessfulState) then break(s"Expected: $expectedNextState, got: Success($actualNextSuccessfulState)")
								else loop(actualNextSuccessfulState, repetition + 1)
							},
							actualNextFaultyState => {
								if expectedNextState.fold(_ !=== actualNextFaultyState, _ => true) then break(s"Expected: $expectedNextState, got: Failure($actualNextFaultyState)")
								else promise.trySuccess(())
							}
						)
					}
				}
			}

			run(loop(initial, 0))
			gate
		}
	}

	test("CausalFence: multiple simultaneous advances should serialize and commit in order") {
		val generators = getGenerators
		import generators.*

		PropF.forAllNoShrinkF(
			smallIntGen,
			Gen.function1[Int, Capturer[Int]](genSuccessfulCapturer[Int]())
		) { (initial, updater) =>
			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			run {
				val fence = CausalFence[Int, doer.type](doer)(initial)

				val actualSteps = for i <- 0 to 9 yield fence.advance(updater)

				def loop(previousState: Int, repetition: Int): Task[List[Int]] = {
					if repetition > 9 then Task_ready(Nil)
					else for {
						nextState <- updater(previousState)
						followingStates <- loop(nextState, repetition + 1)
					} yield nextState :: followingStates
				}

				val expectedResultsTask = loop(initial, 0)

				for {
					actualResults <- doer.Task_sequenceToArray(actualSteps)
					expectedResults <- expectedResultsTask
				} do {
					if actualResults.toList != expectedResults then break(s"expected:${expectedResults.mkString(", ")}, actual:${actualResults.mkString(", ")}")
					else promise.trySuccess(())
				}
			}
			gate
		}
	}

	test("CausalFence - synchronous consumer ordering and anchor freshness: synchronous consumers see up‑to‑date state deterministically - using hoping tasks to avoid stack overflow (much faster than the version that uses random delays below)") {
		val generators = getGenerators
		import generators.*

		def buildHopingMono(serial: Int, hops: Int): Mono[Int] = {
			if hops <= 0 then Task_ready(serial)
			else Captor[Int]().seizeWith(buildHopingMono(serial, hops - 1), false)
		}

		type PrimaryState = (pathId: Int, serial: Int)
		val initialState: PrimaryState = (0, 0)
		PropF.forAllF(
			for {
				swarmSize <- Gen.choose(1, 9)
				hopsHead <- Gen.choose(0, 9)
				hopsTail <- Gen.listOfN(99, Gen.choose(0, 9))
			} yield (swarmSize, hopsHead, hopsTail)
		) { (swarmSize: Int, hopsHead: Int, hopsTail: List[Int]) =>
			val hopsList = hopsHead :: hopsTail
			val topSerial = hopsList.size

			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			val fence = CausalFence[PrimaryState, doer.type](doer)(initialState)
			var derivedSerial: Int = 0

			def path(pathId: Int): Capturer[PrimaryState] = {
				var hasAdvanced = false
				for {
					nextState <- {
						fence.advanceIf { (previous: PrimaryState) =>
							if previous.serial >= topSerial then Maybe.empty
							else {
								val commitedAtStart: PrimaryState = fence.committedState.getOrElse(break(s"Unexpected failing state at updater start: ${fence.committedState}"))
								val mono = buildHopingMono(previous.serial + 1, hopsList(previous.serial))
									.map(newSerial => (pathId, newSerial))
									.andThen { nextState =>
										val committedAtEnd = fence.committedState.getOrElse(break(s"Unexpected failing state at updater end: ${fence.committedState}"))
										if commitedAtStart.serial != committedAtEnd.serial then break(s"In the interval between the updater passed to `advance` is called and the Mono it returns completes, no other updater is started; and that is not happening.")
									}
								hasAdvanced = true
								Maybe.some(mono)
							}
						}
					}
					anchoredState <- {
						val committedState = fence.committedState.getOrElse(break(s"The Capturer returned by advanceIf yielded an unexpected failing state: ${fence.committedState}"))
						if hasAdvanced && nextState.pathId != pathId then break(s"A consumer subscribed to the Capturer returned by `advance` should see the state to which the advance transitioned to; and is not happening: pathId=$pathId, actual: ${nextState.pathId}")
						else if derivedSerial > nextState.serial then break(s"Consumers subscribed immediately (in a synchronously coupled manner) to the `Capturer` returned by `advance`, should be executed in order of subscription before any other consumer, even before the updaters passed to subsequent calls to advance; and is not happening.")
						else if nextState.serial != committedState.serial then break(s"A consumer subscribed immediately (in a synchronously coupled manner) to the Capturer returned by `advance` should see the up-to-date state; and is not happening: current=$nextState, commited=$committedState")
						else derivedSerial = nextState.serial
						fence.causalAnchor()
					}
					recursiveState <- {
						val committedState = fence.committedState.getOrElse(break(s"The Capturer returned by causalAnchor yielded an unexpected failing state: ${fence.committedState}"))
						if anchoredState.serial != committedState.serial then break(s"A consumer subscribed immediately (in a synchronously coupled manner) to the `Capturer` returned by `causalAnchor` should see the the up-to-date state; and is not happening: current=$anchoredState, commited=${fence.committedState}")
						if nextState.serial < topSerial then path(pathId)
						else fence.committed
					}
				} yield {
					val committedState = fence.committedState.getOrElse(break(s"The Capturer returned by `committed` yielded an unexpected failing state: ${fence.committedState}"))
					if recursiveState.serial != committedState.serial then break(s"followingState=$recursiveState, commited=${fence.committedState}")
					recursiveState
				}
			}

			val swarm: Seq[Mono[PrimaryState]] = Seq.tabulate(swarmSize) { n => doer.Capturer_defer(() => path(n)) }
			val checks = for array <- doer.Task_sequenceToArray(swarm) yield promise.trySuccess(())
			checks.triggerAndForget()
			gate
		}
	}

	test("CausalFence - synchronous consumer ordering and anchor freshness: synchronous consumers see up‑to‑date state deterministically - using random delays (very slow)") {
		val generators = getGenerators
		import generators.*

		type PrimaryState = (pathId: Int, serial: Int)
		val initialState: PrimaryState = (0, 0)
		val topSerial = 99
		PropF.forAllF(Gen.choose(1, 9), Gen.oneOf(true, false)) { (swarmSize: Int, syncOnly: Boolean) =>
			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			val fence = CausalFence[PrimaryState, doer.type](doer)(initialState)
			var derivedSerial: Int = 0

			def path(pathId: Int): Capturer[PrimaryState] = {
				for {
					nextState <- {
						fence.advance { (previous: PrimaryState) =>
							val commitedAtStart: PrimaryState = fence.committedState.getOrElse(break(s"Unexpected failing state at updater start: ${fence.committedState}"))
							val monoGenerator: Gen[Mono[Int]] = genSuccessfulMonoFrom(previous.serial + 1, syncOnly)
							val randomMono: Mono[Int] = monoGenerator.sample.get
							randomMono.map(newSerial => (pathId, newSerial))
								.andThen { nextState =>
									val committedAtEnd = fence.committedState.getOrElse(break(s"Unexpected failing state at updater end: ${fence.committedState}"))
									if commitedAtStart.serial != committedAtEnd.serial then break(s"In the interval between the updater passed to `advance` is called and the Task it returns completes, no other updater is started; and that is not happening.")
								}
						}
					}
					anchoredState <- {
						val committedState = fence.committedState.getOrElse(break(s"The Capturer returned by advanceIf yielded an unexpected failing state: ${fence.committedState}"))
						if nextState.pathId != pathId then break(s"A consumer subscribed to the Capturer returned by `advance` should see the state to which the advance transitioned to; and is not happening: pathId=$pathId, actual: ${nextState.pathId}")
						else if derivedSerial > nextState.serial then break(s"Consumers subscribed immediately (in a synchronously coupled manner) to the `Capturer` returned by `advance`, should be executed in order of subscription before any other consumer, even before the updaters passed to subsequent calls to advance; and is not happening.")
						else if nextState.serial != committedState.serial then break(s"A consumer subscribed immediately (in a synchronously coupled manner) to the Capturer returned by `advance` should see the up-to-date state; and is not happening: current=$nextState, commited=$committedState")
						else derivedSerial = nextState.serial
						fence.causalAnchor()
					}
					recursiveState <- {
						val committedState = fence.committedState.getOrElse(break(s"The Capturer returned by causalAnchor yielded an unexpected failing state: ${fence.committedState}"))
						if anchoredState.serial != committedState.serial then break(s"A consumer subscribed immediately (in a synchronously coupled manner) to the `Capturer` returned by `causalAnchor` should see the the up-to-date state; and is not happening: current=$anchoredState, commited=${fence.committedState}")
						if nextState.serial < topSerial then path(pathId)
						else fence.committed
					}
				} yield {
					val committedState = fence.committedState.getOrElse(break(s"The Capturer returned by `committed` yielded an unexpected failing state: ${fence.committedState}"))
					if recursiveState.serial != committedState.serial then break(s"followingState=$recursiveState, commited=${fence.committedState}")
					recursiveState
				}
			}

			val swarm: Seq[Mono[PrimaryState]] = Seq.tabulate(swarmSize) { n => Capturer_defer(() => path(n)) }
			val checks = for array <- doer.Task_sequenceToArray(swarm) yield promise.trySuccess(())
			checks.triggerAndForget()
			gate
		}
	}

	test("CausalFence: `advance` should either, skip transitions if failed, or commit updated state if successful") {
		val generators = getGenerators
		import generators.*

		PropF.forAllNoShrinkF { (expectedState0: Int, firstUpdater: Int => Mono[Int], secondUpdater: Int => Mono[Int]) =>
			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			run {
				val fence = CausalFence[Int, doer.type](doer)(expectedState0)

				fence.advance(firstUpdater)

				fence.advance(secondUpdater).subscribeSyncCallbacks(
					actualState2 => firstUpdater(expectedState0).subscribeSyncCallbacks(
						expectedState1 => secondUpdater(expectedState1).subscribeSyncCallbacks(
							expectedState2 => {
								if actualState2 != expectedState2 then break(s"Unexpected final state: actual=$actualState2, expected=$expectedState2")
								else promise.trySuccess(())
							},
							expectedFailure2 => break(s"Unexpected final state: actual=$actualState2, expected=$expectedFailure2")
						),
						expectedFailure1 => break(s"Unexpected final state: actual=$actualState2, expected=$expectedFailure1")
					),
					actualFailure2 => firstUpdater(expectedState0).subscribeSyncCallbacks(
						expectedState1 => secondUpdater(expectedState1).subscribeSyncCallbacks(
							expectedState2 => break(s"Unexpected final state: actual=$actualFailure2, expected=$expectedState2"),
							expectedFailure2 => {
								if actualFailure2 !=== expectedFailure2 then break(s"Unexpected final state: actual=$actualFailure2, expected=$expectedFailure2")
								else promise.trySuccess(())
							},
						),
						expectedFailure1 => {
							if actualFailure2 !=== expectedFailure1 then break(s"Unexpected final state: actual=$actualFailure2, expected=$expectedFailure1")
							else promise.trySuccess(())
						}
					)
				)
			}
			gate
		}
	}

	test("CausalFence: rollback before commit should be applied") {
		val generators = getGenerators
		import generators.*

		type PrimaryState = Int
		PropF.forAllNoShrinkF { (expectedInitialState: PrimaryState, updater: PrimaryState => Mono[PrimaryState]) =>
			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			run {
				val fence = CausalFence[PrimaryState, doer.type](doer)(expectedInitialState)

				fence.advanceSpeculatively { (actualInitialState, rba) =>
					def doTheRollback(): Unit = {
						rba.rollback(
							true,
							new CompletionObserver[PrimaryState] {
								override def onSuccess(actualFinalSuccessState: PrimaryState, rollbackApplication: OriginId): Unit = {
									if rollbackApplication != ROLLBACK_APPLIED then break("The rollback's CompletionObserver was told that the rollback was not applied despite it should")
									else if actualFinalSuccessState != expectedInitialState then break("The rollback's CompletionObserver was told that the previous state wasn't restored despite it should")
								}

								override def onError(e: Throwable, rollbackApplication: OriginId): Unit = break("The rollback's CompletionObserver was told that the previous state is a failure despite it isn't")
							}
						)
					}

					updater(actualInitialState).andThen(
						_ => doTheRollback(),
						_ => doTheRollback()
					)
				}.subscribeSyncCallbacks(
					actualFinalSuccessState => if actualFinalSuccessState != expectedInitialState then break("The `Capturer` returned by `advanceSpeculatively` yielded an unexpected value"),
					_ => break("The `Capturer` returned by `advanceSpeculatively` received a sticking/failure state despite it shouldn't")
				)

				fence.causalAnchor(new CompletionObserver[PrimaryState] {
					override def onSuccess(actualSuccessfulState: PrimaryState, application: RollbackApplication): Unit = {
						if actualSuccessfulState != expectedInitialState then break("The `causalAnchor`'s CompletionObserver received an unexpected value")
					}

					override def onError(e: Throwable, originId: OriginId): Unit = break("The causal anchor`s CompletionObserver received a sticking/failure state despite it shouldn't")
				}
				).subscribeSyncCallbacks(
					actualSuccessfulState => if actualSuccessfulState != expectedInitialState then break("The `Capturer` returned by `causalAnchor` yielded an unexpected value"),
					_ => break("The `Capturer` returned by `causalAnchor` captured an error and it shouldn't")
				)

				fence.committed.subscribeSyncCallbacks(
					actualCommitted => {
						if actualCommitted != expectedInitialState then break("The `Capturer` returned by `committed` captured an unexpected value")
						else promise.trySuccess(())
					},
					_ => break("The `Capturer` returned by `committed` captured an error and it shouldn't")
				)
			}
			gate
		}
	}

	test("Generators: random functions are deterministic") {
		val generators = getGenerators
		import generators.*

		type PrimaryState = Int
		PropF.forAllNoShrinkF { (state0: PrimaryState, updater: PrimaryState => Mono[PrimaryState]) =>
			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			updater(state0).triggerHardy(false) { state1A =>
				updater(state0).triggerHardy(true) { state1B =>
					if state1A !=== state1B then break("not deterministic")
					else promise.trySuccess(())
				}

			}
			gate
		}
	}

	test("CausalFence: rollback after commit should be ignored") {
		val generators = getGenerators
		import generators.*

		type PrimaryState = Int
		PropF.forAllNoShrinkF { (initialState: PrimaryState, updater: PrimaryState => Mono[PrimaryState]) =>
			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			doer.executeSequentially { () =>
				try {
					updater(initialState).triggerHardy(false) { expectedFinalState =>
						println(s"Begin: expectedFinalState=$expectedFinalState")

						val fence = CausalFence[PrimaryState, doer.type](doer)(initialState)

						fence.advanceSpeculatively { (actualInitialState, rba) =>
							def doTheRollback(): Unit = {
								run {
									rba.rollback(
										true,
										new CompletionObserver[PrimaryState] {
											override def onSuccess(actualFinalSuccessState: PrimaryState, rollbackApplication: OriginId): Unit = {
												if rollbackApplication != ROLLBACK_IGNORED then break("The rollback's CompletionObserver was told that the rollback wasn't ignored despite it should")
												else if expectedFinalState.fold(_ => true, _ != actualFinalSuccessState) then break("The rollback's CompletionObserver received an unexpected primary state")
											}

											override def onError(actualFinalFailureState: Throwable, rollbackApplication: OriginId): Unit = {
												if rollbackApplication != ROLLBACK_IGNORED then break("The rollback's CompletionObserver was told that the rollback wasn't ignored despite it should")
												else if expectedFinalState.fold(_ !=== actualFinalFailureState, _ => true) then break("The rollback's CompletionObserver received an unexpected primary state")
											}
										}
									)
								}
							}

							updater(actualInitialState).andThen(
								_ => doTheRollback(),
								_ => doTheRollback()
							)
						}.triggerHardy(true) { actualFinalState =>
							if actualFinalState !=== expectedFinalState then break(s"The `Capturer` returned by `advanceSpeculatively` received an unexpected state")

							fence.causalAnchor(new CompletionObserver[PrimaryState] {
								override def onSuccess(actualFinalSuccessState: PrimaryState, originId: OriginId): Unit = {
									if expectedFinalState.fold(_ => true, _ != actualFinalSuccessState) then break("The `causalAnchor`'s CompletionObserver received an unexpected value")
								}

								override def onError(actualFinalErrorState: Throwable, originId: OriginId): Unit = {
									if expectedFinalState.fold(_ !=== actualFinalErrorState, _ => true) then break("The `causalAnchor`'s CompletionObserver received an unexpected value")
								}
							}).subscribeHardy(true) { actualAnchor =>
								if actualAnchor !=== expectedFinalState then break("The capturer returned by `causalAnchor` yielded an unexpected value")
							}

							fence.committed.subscribeHardy(true) { actualCommitedState =>
								if actualCommitedState !=== expectedFinalState then break(s"The capturer returned by `committed` yielded an unexpected value: expected=$expectedFinalState, got=$actualCommitedState")
								else promise.trySuccess(())
							}
						}
					}
				} catch {
					case cause: Throwable => promise.tryFailure(cause)
				}
			}
			gate
		}
	}
}
