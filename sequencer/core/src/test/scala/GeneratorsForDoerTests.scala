package readren.sequencer

import GeneratorsForDoerTests.*

import org.scalacheck.{Arbitrary, Gen}

import java.util.concurrent.Executors
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, ExecutionException, Future}
import readren.common.Maybe
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

object GeneratorsForDoerTests {
	class FaultyValue[A](val value: A, val failureLabel: String) extends RuntimeException(failureLabel) {
		override def toString: String = s"FaultyValue($value, $failureLabel)"
	}

	private val currentForeignDoer: ThreadLocal[Doer] = new ThreadLocal()

	val smallIntGen: Gen[Int] = Gen.choose(-9, 9)

	/** Generates instances of [[Try]] where successes are the provided value and failures are non-fatal. */
	def genTryFrom[A](a: A, failureLabel: String, failureProbability: Int = 20): Gen[Try[A]] = Gen.frequency(
		(100 - failureProbability, Success(a)),
		(failureProbability, Failure(new FaultyValue(a, s"$failureLabel: $a")))
	)

	/** Generates instances of [[Try]] where failures are non-fatal. */
	def genTry[A](using genA: Arbitrary[A]): Gen[Try[A]] = {
		for {
			a <- genA.arbitrary
			tryA <- genTryFrom(a, "genTry")
		} yield tryA
	}

	given tryArbitrary: [A] =>Arbitrary[A] => Arbitrary[Try[A]] = Arbitrary(genTry)


	def genFutureBuilderFromTry[A](tryA: Try[A], failureLabel: String): Gen[() => Future[A]] = {
		val immediateGen: Gen[() => Future[A]] = Gen.const(() => Future.fromTry(tryA))
		val delayedGen: Gen[() => Future[A]] =
			for delay <- Gen.oneOf(0, 1, 2, 4, 8, 16) yield {
				() => {
					Future(Thread.sleep(delay))(using ExecutionContext.global)
						.transform(_ => tryA)(using ExecutionContext.global)
				}
			}
		Gen.oneOf(immediateGen, delayedGen)
	}


	def genFutureBuilder[A](a: A, failureLabel: String): Gen[() => Future[A]] = {
		val immediateGen: Gen[() => Future[A]] = genTryFrom(a, s"$failureLabel / FutureBuilder.immediate").map(tryA => () => Future.fromTry(tryA))
		val delayedGen: Gen[() => Future[A]] =
			for {
				tryA <- genTryFrom(a, s"$failureLabel / FutureBuilder.delayed")
				delay <- Gen.oneOf(0, 1, 2, 4, 8, 16)
			} yield {
				() => {
					Future(Thread.sleep(delay))(using ExecutionContext.global)
						.transform(_ => tryA)(using ExecutionContext.global)
				}
			}
		Gen.oneOf(immediateGen, delayedGen)
	}

	def genFutureFromTry[A](tryA: Try[A], failureLabel: String): Gen[Future[A]] = {
		genFutureBuilderFromTry(tryA, failureLabel).map(builder => builder())
	}

	def genFuture[A](a: A, failureLabel: String): Gen[Future[A]] = {
		genFutureBuilder(a, failureLabel).map(builder => builder())
	}

	given futureArbitrary: [A] =>(arbA: Arbitrary[A]) => Arbitrary[Future[A]] = Arbitrary {
		arbA.arbitrary.flatMap(a => genFuture(a, "futureArbitrary"))
	}

	extension [A, B](function1Gen: Gen[A => B]) {
		/** @return a [[Function1]] generator like this one, but corrupting the generated functions such that, for a subset of inputs based on the specified failureProbabilityPercentage, it throws [[FaultyValue]] with the value that the original function returns. */
		def faulted(failureProbabilityPercentage: Int = 20): Gen[A => B] = {
			require(failureProbabilityPercentage >= 0 && failureProbabilityPercentage <= 100, "failureProbabilityPercentage must be between 0 and 100")

			for {
				f <- function1Gen
				seed <- Gen.choose(Long.MinValue, Long.MaxValue) // Random seed for determinism
			} yield { (x: A) =>
				// Use input's hashCode and seed to create a deterministic random choice
				val random = new scala.util.Random(seed + x.hashCode().toLong)
				val shouldFail = random.nextInt(100) < failureProbabilityPercentage
				if shouldFail then {
					val result = f(x)
					throw FaultyValue(result, "faulted function")
				} else {
					f(x)
				}
			}
		}
	}

	extension (e1: Throwable) {
		@tailrec
		def ====(e2: Throwable): Boolean = {
			if e1.isInstanceOf[ExecutionException] && e1.getCause != null && (e1.getCause ne e1) then e1.getCause ==== e2
			else if e2.isInstanceOf[ExecutionException] && e2.getCause != null && (e2.getCause ne e2) then e1 ==== e2.getCause
			else (e1 eq e2) || (e1 ne null) && (e2 ne null) && (e1.getClass eq e2.getClass) && (e1.getMessage == e2.getMessage)
		}

		def !===(e2: Throwable): Boolean = !(e1 ==== e2)
	}

	extension [A](try1: Try[A]) {
		def ====(try2: Try[A]): Boolean = {
			(try1, try2) match {
				case (Failure(e1), Failure(e2)) => e1 ==== e2
				case (Success(v1), Success(v2)) => v1.equals(v2)
				case _ => false
			}
		}

		def !===(try2: Try[A]): Boolean = !(try1 ==== try2)
	}

	//	extension [A](thisTask: Task[A]) {
	//		def ====(otherTask: Task[A]): Task[Boolean] = {
	//			Task.combine(thisTask, otherTask)((a, b) => Success(a ==== b))
	//		}
	//	}

	given throwableArbitrary: Arbitrary[Throwable] = Arbitrary {
		for {
			msg <- Gen.stringOfN(9, Gen.alphaChar)
			ex <- Gen.oneOf(
				Gen.const(new RuntimeException(s"Simulated runtime: [$msg]")),
				Gen.const(new IllegalArgumentException(s"Simulated illegal argument: [$msg]")),
				Gen.const(new UnsupportedOperationException(s"Simulated unsupported operation: [$msg]")),
				Gen.const(new Exception(s"Simulated general exception: [$msg]", new RuntimeException("Cause exception"))),
				Gen.const(new InternalError(s"Simulated internal error: [$msg]")),
				Gen.const(new LinkageError(s"Simulated linkage error: [$msg]")),
				Gen.const(new Error(s"Simulated error: [$msg]"))
			)
		} yield ex
	}

	/** @return a generator of [[doer.Schedule]] instances with delays and intervales between 1 and `maxDuration` milliseconds. */
	def genSchedule[D <: Doer & SchedulingExtension](doer: D, maxDuration: Int = 5): Gen[doer.Schedule] = {
		for {
			delay <- Gen.choose(1, maxDuration)
			interval <- Gen.choose(1, maxDuration)
			kind <- Gen.oneOf(1, 2, 3)
		} yield if kind == 1 then doer.newDelaySchedule(delay)
		else if kind == 2 then doer.newFixedRateSchedule(delay, interval)
		else doer.newFixedDelaySchedule(delay, interval)
	}
}

/** Offers generators of [[doer.Mono]] instances.
 * Useful for suites that test their behavior. */
class GeneratorsForDoerTests[D <: Doer](val doer: D, doerProvider: DoerProvider[Doer], recursionLevel: Int = 0) {

	import doer.*

	export doer.*

	val includeForeign: Boolean = recursionLevel < 9

	/** A doer with a dedicated single-thread-executor that no other [[Doer]] instance can share. */
	val foreignDoer: Doer = doerProvider.provide(doerProvider.tagFromText(s"foreign-doer-$recursionLevel"))

	/** @return a [[GeneratorsForDoerTest]] instance that offers generators for [[foreignDoer.Mono]] instances. */
	def foreignDoerGenerators(enableRecursiveForeign: Boolean = false): GeneratorsForDoerTests[foreignDoer.type] = new GeneratorsForDoerTests[foreignDoer.type](foreignDoer, doerProvider, recursionLevel + 1)

	/** @return a generator of [[doer.Task]] instances that yield the provided value. */
	def genSuccessfulTaskFrom[A](a: A, syncExecutionOnly: Boolean = false): Gen[Task[A]] = {
		val readyGen: Gen[Task[A]] = Task_ready(a)

		val applyGen: Gen[Task[A]] = Task_apply(() => a)

		val deferredGen: Gen[Task[A]] = Gen.oneOf(readyGen, applyGen).map(da => Task_defers(() => da))

		val fromFutureGen: Gen[Task[A]] = genFutureFromTry(Success(a), "Task_fromFuture").map(future => Task_from(future))

		val fromDeferFutureGen: Gen[Task[A]] = genFutureFromTry(Success(a), "Task_fromDeferFuture").map(future => Task_from(() => future))

		def foreignGen: Gen[Task[A]] = foreignDoerGenerators().genSuccessfulTaskFrom(a).map(_.onBehalfOf(doer))

		if syncExecutionOnly then readyGen
		else if includeForeign then Gen.oneOf(readyGen, applyGen, deferredGen, fromFutureGen, fromDeferFutureGen, foreignGen)
		else Gen.oneOf(readyGen, applyGen, deferredGen, fromFutureGen, fromDeferFutureGen)
	}

	def genSuccessfulTask[A](syncExecutionOnly: Boolean = false)(using genA: Arbitrary[A]): Gen[Task[A]] = {
		for {
			a <- genA.arbitrary
			f <- genSuccessfulTaskFrom(a, syncExecutionOnly)
		} yield f
	}

	def genFailingTaskFrom(e: Throwable, syncExecutionOnly: Boolean = false): Gen[Task[Nothing]] = {
		val readyGen: Gen[Task[Nothing]] = Task_fail(e)

		val deferredGen: Gen[Task[Nothing]] = Task_defers(() => Task_fail(e))

		val fromFutureGen: Gen[Task[Nothing]] = genFutureFromTry(Failure(e), "Task_fromFuture").map(future => Task_from(future))

		val fromDeferFutureGen: Gen[Task[Nothing]] = genFutureFromTry(Failure(e), "Task_fromDeferFuture").map(future => Task_from(() => future))

		def foreignGen: Gen[Task[Nothing]] = foreignDoerGenerators().genFailingTaskFrom(e).map(_.onBehalfOf(doer))

		if syncExecutionOnly then readyGen
		else if includeForeign then Gen.oneOf(readyGen, deferredGen, fromFutureGen, fromDeferFutureGen, foreignGen)
		else Gen.oneOf(readyGen, deferredGen, fromFutureGen, fromDeferFutureGen)
	}

	def genTaskFrom[A](tryA: Try[A], syncExecutionOnly: Boolean = false): Gen[Task[A]] = {
		tryA match {
			case Success(a) => genSuccessfulTaskFrom(a, syncExecutionOnly)
			case Failure(e) => genFailingTaskFrom(e, syncExecutionOnly)
		}
	}

	def genTask[A](syncExecutionOnly: Boolean = false)(using genTryA: Arbitrary[Try[A]]): Gen[Task[A]] = {
		for {
			tryA <- genTryA.arbitrary
			taskA <- genTaskFrom(tryA)
		} yield taskA
	}

	/** Implicitly provide an Arbitrary instance for `doer.Task` */
	given taskArbitrary: [A] =>Arbitrary[Try[A]] => Arbitrary[Task[A]] = Arbitrary(genTask())

	def genSuccessfulCapturerFrom[A](a: A, syncExecutionOnly: Boolean = false): Gen[Capturer[A]] = {
		val readyGen: Gen[Capturer[A]] = Keeper(a)

		val applyGen: Gen[Capturer[A]] = Capturer_apply(() => a)

		val deferredGen: Gen[Capturer[A]] = Gen.oneOf(readyGen, applyGen).map(lt => Capturer_defer(() => lt))

		val fromFutureGen: Gen[Capturer[A]] = genFutureFromTry(Success(a), "Capturer_fromFuture").map(future => Capturer_from(future))

		val fromDeferFutureGen: Gen[Capturer[A]] = genFutureFromTry(Success(a), "Capturer_fromDeferFuture").map(future => Capturer_from(() => future))

		def foreignGen: Gen[Capturer[A]] = foreignDoerGenerators().genSuccessfulCapturerFrom(a).map(_.onBehalfOf(doer))

		if syncExecutionOnly then readyGen
		else if includeForeign then Gen.oneOf(readyGen, applyGen, deferredGen, fromFutureGen, fromDeferFutureGen, foreignGen)
		else Gen.oneOf(readyGen, applyGen, deferredGen, fromFutureGen, fromDeferFutureGen)
	}

	def genSuccessfulCapturer[A](syncExecutionOnly: Boolean = false)(using genA: Arbitrary[A]): Gen[Capturer[A]] = {
		for {
			a <- genA.arbitrary
			f <- genSuccessfulCapturerFrom(a, syncExecutionOnly)
		} yield f
	}

	def genFailingCapturerFrom(e: Throwable, syncExecutionOnly: Boolean = false): Gen[Capturer[Nothing]] = {

		val readyGen: Gen[Capturer[Nothing]] = Failed(e)

		val deferredGen: Gen[Capturer[Nothing]] = readyGen.map(lt => Capturer_defer(() => lt))

		val fromFutureGen: Gen[Capturer[Nothing]] = genFutureFromTry(Failure(e), "Capturer_fromFuture").map(future => Capturer_from(future))

		val fromDeferFutureGen: Gen[Capturer[Nothing]] = genFutureFromTry(Failure(e), "Capturer_fromDeferFuture").map(future => Capturer_from(() => future))

		def foreignGen: Gen[Capturer[Nothing]] = foreignDoerGenerators().genFailingCapturerFrom(e).map(_.onBehalfOf(doer))

		if syncExecutionOnly then readyGen
		else if includeForeign then Gen.oneOf(readyGen, deferredGen, fromFutureGen, fromDeferFutureGen, foreignGen)
		else Gen.oneOf(readyGen, deferredGen, fromFutureGen, fromDeferFutureGen)
	}

	def genCapturerFrom[A](tryA: Try[A], syncExecutionOnly: Boolean = false): Gen[Capturer[A]] = {
		tryA match {
			case Success(a) => genSuccessfulCapturerFrom(a, syncExecutionOnly)
			case Failure(e) => genFailingCapturerFrom(e, syncExecutionOnly)
		}
	}

	def genCapturer[A](syncExecutionOnly: Boolean = false)(using genTryA: Arbitrary[Try[A]]): Gen[Capturer[A]] = {
		for {
			tryA <- genTryA.arbitrary
			capturerA <- genCapturerFrom(tryA, syncExecutionOnly)
		} yield capturerA
	}
	
	/** Implicitly provide an Arbitrary instance for `doer.Task` */
	given capturerArbitrary: [A] =>Arbitrary[Try[A]] => Arbitrary[Capturer[A]] = Arbitrary(genCapturer())

	def genSuccessfulMonoFrom[A](a: A, syncExecutionOnly: Boolean = false): Gen[Mono[A]] = {
		Gen.oneOf(genSuccessfulTaskFrom(a, syncExecutionOnly), genSuccessfulCapturerFrom(a, syncExecutionOnly))
	}

	def genSuccessfulMono[A](syncExecutionOnly: Boolean = false)(using genA: Arbitrary[A]): Gen[Mono[A]] = {
		for {
			a <- genA.arbitrary
			f <- genSuccessfulMonoFrom(a, syncExecutionOnly)
		} yield f
	}

	def genMonoFrom[A](tryA: Try[A], syncExecutionOnly: Boolean = false): Gen[Mono[A]] = {
		Gen.oneOf(genTaskFrom(tryA, syncExecutionOnly), genCapturerFrom(tryA, syncExecutionOnly))
	}

	def genMono[A](syncExecutionOnly: Boolean = false)(using genTryA: Arbitrary[Try[A]]): Gen[Mono[A]] = {
		for {
			tryA <- genTryA.arbitrary
			monoA <- genMonoFrom(tryA, syncExecutionOnly)
		} yield monoA
	}

	given monoArbitrary: [A] =>Arbitrary[Try[A]] => Arbitrary[Mono[A]] = Arbitrary(genMono())

	def genSuccessfulFluxFrom[A: ClassTag](elements: Seq[A], syncExecutionOnly: Boolean = false): Gen[FluxExtension#Flux[A]] = {
		val d = doer.asInstanceOf[D & FluxExtension]
		import d.*

		val applyGen: Gen[Flux[A]] = Gen.const(Flux_apply(elements *))
		val iterableGen: Gen[Flux[A]] = Gen.const(Flux_fromIterable(elements))
		val iterableGuardedGen: Gen[Flux[A]] = Gen.const(Flux_fromIterableGuarded(elements))

		val generateGen: Gen[Flux[A]] = Gen.const(Flux_generate[A] { idx =>
			if idx < elements.length then Maybe(elements(idx)) else Maybe.empty
		})

		val generateStatefullyGen: Gen[Flux[A]] = Gen.const(Flux_generateStatefully[A] { () =>
			var count = 0
			idx => {
				if count < elements.length then {
					val v = elements(count)
					count += 1
					Maybe(v)
				} else Maybe.empty
			}
		})

		val emitterGen: Gen[Flux[A]] = Gen.const {
			new DefaultFlux[A] {
				override def subscribeSync(downChainObserver: FluxObserver[A]): d.Subscription = {
					val emitter = new StreamEmitter[A]
					val sub = emitter.subscribeSync(downChainObserver)
					elements.foreach(emitter.emit)
					emitter.end()
					sub
				}
			}
		}

		val monosSeqGen: Gen[Flux[A]] = {
			val monos = IArray.from(elements.map(a => d.Task_ready(a)))
			Gen.const(Flux_fromMonosSequentially(monos))
		}

		val monosGen: Gen[Flux[A]] = {
			val monos = IArray.from(elements.map(a => d.Task_ready(a)))
			Gen.const(Flux_fromMonos(monos))
		}

		Gen.oneOf(applyGen, iterableGen, iterableGuardedGen, generateGen, generateStatefullyGen, emitterGen, monosSeqGen, monosGen)
	}

	def genFailingFluxFrom[A: ClassTag](elementsBeforeFailure: Seq[A], ex: Throwable, syncExecutionOnly: Boolean = false): Gen[FluxExtension#Flux[A]] = {
		val d = doer.asInstanceOf[D & FluxExtension]
		import d.*

		val emitterGen: Gen[Flux[A]] = Gen.const {
			new DefaultFlux[A] {
				override def subscribeSync(downChainObserver: FluxObserver[A]): d.Subscription = {
					val emitter = new StreamEmitter[A]
					val sub = emitter.subscribeSync(downChainObserver)
					elementsBeforeFailure.foreach(emitter.emit)
					emitter.fail(ex)
					sub
				}
			}
		}

		val iterableGuardedGen: Gen[Flux[A]] = Gen.const {
			val faultyIterable = new Iterable[A] {
				override def iterator: Iterator[A] = new Iterator[A] {
					private var idx = 0

					override def hasNext: Boolean = true

					override def next(): A = {
						if idx < elementsBeforeFailure.length then {
							val res = elementsBeforeFailure(idx)
							idx += 1
							res
						} else throw ex
					}
				}
			}
			Flux_fromIterableGuarded(faultyIterable)
		}

		val generateGen: Gen[Flux[A]] = Gen.const {
			Flux_generate[A] { idx =>
				if idx < elementsBeforeFailure.length then Maybe(elementsBeforeFailure(idx))
				else throw ex
			}
		}

		val monosGen: Gen[Flux[A]] = Gen.const {
			val monosList = elementsBeforeFailure.map(a => d.Task_ready(a)) :+ d.Task_fail(ex)
			Flux_fromMonos(IArray.from(monosList))
		}

		Gen.oneOf(emitterGen, iterableGuardedGen, generateGen, monosGen)
	}

	def genFluxFrom[A: ClassTag](elements: Seq[A], failure: Option[Throwable] = None, syncExecutionOnly: Boolean = false): Gen[FluxExtension#Flux[A]] = {
		failure match {
			case None => genSuccessfulFluxFrom(elements, syncExecutionOnly)
			case Some(ex) => genFailingFluxFrom(elements, ex, syncExecutionOnly)
		}
	}

	def genFlux[A: ClassTag](syncExecutionOnly: Boolean = false)(using genA: Arbitrary[A]): Gen[FluxExtension#Flux[A]] = {
		for {
			elems <- Gen.listOfN(3, genA.arbitrary)
			shouldFail <- Gen.frequency((80, false), (20, true))
			ex <- GeneratorsForDoerTests.throwableArbitrary.arbitrary
			flux <- if shouldFail then genFailingFluxFrom[A](elems, ex, syncExecutionOnly) else genSuccessfulFluxFrom[A](elems, syncExecutionOnly)
		} yield flux
	}

	given fluxArbitrary: [A: ClassTag] =>Arbitrary[A] => Arbitrary[FluxExtension#Flux[A]] = Arbitrary(genFlux())

}
