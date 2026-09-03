package readren.sequencer

import GeneratorsForDoerTests.{throwableArbitrary, *}

import org.scalacheck.effect.PropF
import org.scalacheck.{Arbitrary, Gen}
import readren.common.Maybe

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

trait MonoTests[D <: Doer : ClassTag] { self: DoerProviderTestBase[D] =>

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

			class T[A, B] extends MonoTransformer[A, B] {
				override def mapSuccess(a: A): B = throw expectedUnhandledExceptionParam

				override def mapError(e: Throwable): B = throw expectedUnhandledExceptionParam
			} 
			
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

				_ <- check("transform1", sTask.transform(new T))
				_ <- check("transform2", fTask.transform(new T))
				_ <- check("guarded.transform3", sTask.guarded.transform(new T), true)
				_ <- check("guarded.transform4", fTask.guarded.transform(new T), true)

				_ <- check("transformWith1", sTask.transformWith(new T))
				_ <- check("transformWith2", fTask.transformWith(new T))
				_ <- check("guarded.transformWith1", sTask.guarded.transformWith(new T), true)
				_ <- check("guarded.transformWith2", fTask.guarded.transformWith(new T), true)

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

			class T[A, B] extends MonoTransformer[A, B] {
				override def mapSuccess(a: A): B = throw thrownExceptionParam

				override def mapError(e: Throwable): B = throw thrownExceptionParam
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

				_ <- check("transform", t1.transform(new T))
				_ <- check("guarded.transform", t1.guarded.transform(new T))

				_ <- check("transformWith", t1.transformWith(new T))
				_ <- check("guarded.transformWith", t1.guarded.transformWith(new T))

				_ <- check("recover", t1.recover { _ => if randomBool then Maybe(randomInt) else Maybe.empty })
				_ <- check("guarded.recover", t1.guarded.recover { _ => if randomBool then Maybe(randomInt) else Maybe.empty })

				_ <- check("recoverWith", t1.recoverWith { _ => if randomBool then Maybe(t2) else Maybe.empty })
				_ <- check("guarded.recoverWith", t1.guarded.recoverWith { _ => if randomBool then Maybe(t2) else Maybe.empty })
			} yield ()
		}
	}

	//// CAPTOR ////


	test("Capture: functional arguments passed to operations must be executed at most once") {
		val generators = getGenerators
		import generators.*

		PropF.forAllF(
			for {
				successfulInteger <- smallIntGen
				thrownException <- throwableArbitrary.arbitrary
				successfulCapture <- genSuccessfulCaptureFrom(successfulInteger)
				failingCapture <- genFailingCaptureFrom(thrownException).map(_.asInstanceOf[Capture[Int]])
			} yield (successfulInteger, thrownException, successfulCapture, failingCapture)
		) { case (successfulInteger, thrownException, successCapture, failureCapture) =>
			def checkOperation[Result](operationName: String)(buildSpiedCapture: Runnable => Capture[Result]): Future[Unit] = {
				val promise = Promise[Unit]()

				given Promise[Unit] = promise

				doer.run {
					val executionCounter = new AtomicInteger(0)
					val spiedCapture = buildSpiedCapture(() => executionCounter.incrementAndGet())

					spiedCapture.subscribe(false)(new MonoObserver[Result] {
						override def onSuccess(firstResult: Result): Unit = {
							spiedCapture.subscribeSync(new MonoObserver[Result] {
								override def onSuccess(secondResult: Result): Unit = checkExecutionCount()

								override def onError(secondException: Throwable): Unit = break(s"$operationName: second observer received unexpected error: $secondException")
							})
						}

						override def onError(firstException: Throwable): Unit = {
							spiedCapture.subscribeSync(new MonoObserver[Result] {
								override def onSuccess(secondResult: Result): Unit = break(s"$operationName: second observer succeeded despite failure")

								override def onError(secondException: Throwable): Unit = checkExecutionCount()
							})
						}

						private def checkExecutionCount(): Unit = {
							val totalExecutions = executionCounter.get()
							if totalExecutions > 1 then break(s"$operationName: functional operand was executed $totalExecutions times (expected at most 1)")
							else promise.trySuccess(())
						}
					})
				}

				promise.future
			}

			for {
				// Factory methods
				_ <- checkOperation("Capture_apply")(spy => Capture_apply(() => {
					spy.run();
					successfulInteger
				}))
				_ <- checkOperation("Capture_applyGuarded")(spy => Capture_apply(() => {
					spy.run();
					successfulInteger
				}, isGuarded = true))
				_ <- checkOperation("Capture_defer")(spy => Capture_defer(() => {
					spy.run();
					successCapture
				}))
				_ <- checkOperation("Capture_deferGuarded")(spy => Capture_defer(() => {
					spy.run();
					successCapture
				}, isGuarded = true))
				_ <- checkOperation("Capture_fromFutureBuilder")(spy => Capture_from(() => {
					spy.run();
					Future.successful(successfulInteger)
				}))
				_ <- checkOperation("Capture_fromForeign")(spy => Capture_from(foreignDoer)(foreignDoer.Capture_apply(() => {
					spy.run();
					successfulInteger
				})))

				// Operations on successful Capture
				_ <- checkOperation("successfulCapture.andThen")(spy => successCapture.andThen(successValue => spy.run(), failureCause => spy.run()))
				_ <- checkOperation("successfulCapture.withFilter")(spy => successCapture.withFilter(integerValue => {
					spy.run();
					integerValue > 0
				}))
				_ <- checkOperation("successfulCapture.withFilterGuarded")(spy => successCapture.withFilterGuarded(integerValue => {
					spy.run();
					integerValue > 0
				}))
				_ <- checkOperation("successfulCapture.map")(spy => successCapture.map(integerValue => {
					spy.run();
					integerValue + 1
				}))
				_ <- checkOperation("successfulCapture.mapGuarded")(spy => successCapture.mapGuarded(integerValue => {
					spy.run();
					integerValue + 1
				}))
				_ <- checkOperation("successfulCapture.flatMap")(spy => successCapture.flatMap(integerValue => {
					spy.run();
					Keeper(integerValue + 1)
				}))
				_ <- checkOperation("successfulCapture.flatMapGuarded")(spy => successCapture.flatMapGuarded(integerValue => {
					spy.run();
					Keeper(integerValue + 1)
				}))
				_ <- checkOperation("successfulCapture.transform")(spy => successCapture.transform(new MonoTransformer[Int, scala.util.Try[Int]] {
					def mapSuccess(a: Int): Try[Int] = {
						spy.run();
						scala.util.Success(a)
					}

					def mapError(e: Throwable): Try[Int] = {
						spy.run();
						scala.util.Failure(e)
					}
				}))
				_ <- checkOperation("successfulCapture.transformWith")(spy => successCapture.transformWith(new MonoTransformer[Int, Capture[Int]] {
					def mapSuccess(integerValue: Int): Capture[Int] = {
						spy.run();
						Keeper(integerValue + 1)
					}

					def mapError(failureCause: Throwable): Capture[Int] = {
						spy.run();
						Failed(failureCause)
					}
				}))

				// Guarded Capture operations on successful Capture
				_ <- checkOperation("successfulCapture.guarded.withFilter")(spy => successCapture.guarded.withFilter(integerValue => {
					spy.run();
					integerValue > 0
				}))
				_ <- checkOperation("successfulCapture.guarded.map")(spy => successCapture.guarded.map(integerValue => {
					spy.run();
					integerValue + 1
				}))
				_ <- checkOperation("successfulCapture.guarded.flatMap")(spy => successCapture.guarded.flatMap(integerValue => {
					spy.run();
					Keeper(integerValue + 1)
				}))

				// Operations on failing Capture
				_ <- checkOperation("failingCapture.andThen")(spy => failureCapture.andThen(successValue => spy.run(), failureCause => spy.run()))
				_ <- checkOperation("failingCapture.transform")(spy => failureCapture.transform(new MonoTransformer[Int, scala.util.Try[Int]] {
					def mapSuccess(a: Int): Try[Int] = {
						spy.run();
						scala.util.Success(a)
					}

					def mapError(e: Throwable): Try[Int] = {
						spy.run();
						scala.util.Failure(e)
					}
				}))
				_ <- checkOperation("failingCapture.transformWith")(spy => failureCapture.transformWith(new MonoTransformer[Int, Capture[Int]] {
					def mapSuccess(integerValue: Int): Capture[Int] = {
						spy.run();
						Keeper(integerValue + 1)
					}

					def mapError(failureCause: Throwable): Capture[Int] = {
						spy.run();
						Failed(failureCause)
					}
				}))
				_ <- checkOperation("failingCapture.recover")(spy => failureCapture.recover(failureCause => {
					spy.run();
					Maybe(successfulInteger)
				}))
				_ <- checkOperation("failingCapture.recoverWith")(spy => failureCapture.recoverWith(failureCause => {
					spy.run();
					Maybe(successCapture)
				}))

				// Seized pending Captor
				_ <- {
					val seizedCaptor = doer.Captor[Int]()
					seizedCaptor.seize(Success(successfulInteger))
					checkOperation("seizedCaptor.map")(spy => seizedCaptor.map(integerValue => {
						spy.run();
						integerValue + 1
					}))
				}
			} yield ()
		}
	}

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

}
