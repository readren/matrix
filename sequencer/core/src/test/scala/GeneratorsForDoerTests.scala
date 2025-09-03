package readren.sequencer

import GeneratorsForDoerTests.*

import org.scalacheck.{Arbitrary, Gen}

import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object GeneratorsForDoerTests {
	class FaultyValue[A](val value: A, val failureLabel: String) extends RuntimeException(failureLabel) {
		override def toString: String = s"FaultyValue($value, $failureLabel)"
	}

	private val currentForeignDoer: ThreadLocal[Doer] = new ThreadLocal()

	given intGen: Gen[Int] = Gen.choose(-9, 9)

	def genTry[A](a: A, failureLabel: String, failureProbability: Int = 20): Gen[Try[A]] = Gen.frequency(
		(100, Success(a)),
		(failureProbability, Failure(new FaultyValue(a, s"$failureLabel: $a")))
	)

	given tryArbitrary: [A] =>(arbA: Arbitrary[A]) => Arbitrary[Try[A]] = Arbitrary {
		arbA.arbitrary.flatMap(a => genTry(a, ""))
	}


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
		val immediateGen: Gen[() => Future[A]] = genTry(a, s"$failureLabel / FutureBuilder.immediate").map(tryA => () => Future.fromTry(tryA))
		val delayedGen: Gen[() => Future[A]] =
			for {
				tryA <- genTry(a, s"$failureLabel / FutureBuilder.delayed")
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
		genFutureBuilderFromTry(tryA, s"$failureLabel / FutureInstance").map(builder => builder())
	}

	def genFuture[A](a: A, failureLabel: String): Gen[Future[A]] = {
		genFutureBuilder(a, s"$failureLabel / FutureInstance").map(builder => builder())
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
				if (shouldFail) {
					val result = f(x)
					throw FaultyValue(result, "faulted function")
				} else {
					f(x)
				}
			}
		}
	}

	extension (e1: Throwable) {
		def ====(e2: Throwable): Boolean = (e1 eq e2) || (e1 ne null) && (e2 ne null) && (e1.getClass eq e2.getClass) && (e1.getMessage == e2.getMessage)
	}

	extension [A](try1: Try[A]) {
		def ====(try2: Try[A]): Boolean = {
			(try1, try2) match {
				case (Failure(e1), Failure(e2)) => e1 ==== e2
				case (Success(v1), Success(v2)) => v1.equals(v2)
				case _ => false
			}
		}
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
				Gen.const(new RuntimeException(s"Runtime: [$msg]")),
				Gen.const(new IllegalArgumentException(s"Illegal argument: [$msg]")),
				Gen.const(new UnsupportedOperationException(s"Unsupported operation: [$msg]")),
				Gen.const(new Exception(s"General exception: [$msg]", new RuntimeException("Cause exception"))),
				Gen.const(new InternalError(s"InternalError: [$msg]")),
				Gen.const(new LinkageError(s"LinkageError: [$msg]")),
				Gen.const(new Error(s"Error: [$msg]"))
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

/** Offers generators of [[doer.Duty]] and [[doer.Task]] instances.
 * Useful for suites that test their behavior. */
class GeneratorsForDoerTests[D <: Doer](val doer: D, doerProvider: DoerProvider[Doer], synchronousOnly: Boolean = false, includeForeign: Boolean = true, recursionLevel: Int = 0) {

	import doer.*

	export doer.*

	/** A doer with a dedicated single-thread-executor that no other [[Doer]] instance can share. */
	val foreignDoer: Doer = doerProvider.provide(s"foreign-doer-$recursionLevel")

	/** @return a [[GeneratorsForDoerTest]] instance that offers generators for [[foreignDoer.Duty]] and [[foreignDoer.Task]] instances. */
	def foreignDoerGenerators(enableRecursiveForeign: Boolean = false): GeneratorsForDoerTests[foreignDoer.type] = new GeneratorsForDoerTests[foreignDoer.type](foreignDoer, doerProvider, synchronousOnly, enableRecursiveForeign, recursionLevel + 1)

	/** @return a generator of [[doer.Duty]] instances that yield the provided value. */
	def genDuty[A](a: A): Gen[Duty[A]] = {
		val readyGen: Gen[Duty[A]] = Duty.ready(a)

		val mineGen: Gen[Duty[A]] = Duty.mine(() => a)

		val mineFlatGen: Gen[Duty[A]] = Gen.oneOf(readyGen, mineGen).map(da => Duty.mineFlat(() => da))

		def foreignGen: Gen[Duty[A]] = foreignDoerGenerators().genDuty(a).map(Duty.foreign(foreignDoer)(_))

		if synchronousOnly || !includeForeign then Gen.oneOf(readyGen, mineGen, mineFlatGen)
		else Gen.oneOf(readyGen, mineGen, mineFlatGen, foreignGen)
	}

	/** Implicitly provide an Arbitrary instance for `doer.Duty` */
	given dutyArbitrary: [A] =>(arbA: Arbitrary[A]) => Arbitrary[Duty[A]] = Arbitrary {
		arbA.arbitrary.flatMap(a => genDuty(a))
	}


	/** @return a generator of [[doer.Task]] instances that yield the provided value. */
	def genTaskFromTry[A](tryA: Try[A], failureLabel: String, failureProbability: Int = 20): Gen[Task[A]] = {

		val immediateGen: Gen[Task[A]] = Task.ready(tryA)

		val ownGen: Gen[Task[A]] = Task.own(() => tryA)

		val ownFlatGen: Gen[Task[A]] = Gen.oneOf(immediateGen, ownGen).map { taskA => Task.ownFlat(() => taskA) }

		val waitGen: Gen[Task[A]] = genFutureFromTry(tryA, s"$failureLabel / Task.wait").map(Task.wait)

		def foreignGen: Gen[Task[A]] = foreignDoerGenerators().genTaskFromTry(tryA, s"failureLabel / Task.foreign").map(Task.foreign(foreignDoer)(_))

		val alienGen: Gen[Task[A]] = genFutureBuilderFromTry(tryA, s"$failureLabel / Task.alien").map(Task.alien)

		if synchronousOnly then Gen.oneOf(immediateGen, ownGen, ownFlatGen)
		else if includeForeign then Gen.oneOf(immediateGen, ownGen, ownFlatGen, waitGen, alienGen, foreignGen)
		else Gen.oneOf(immediateGen, ownGen, ownFlatGen, waitGen, alienGen)
	}

	/** @return a generator of [[doer.Task]] instances that yield the provided value. */
	def genTask[A](a: A, failureLabel: String, failureProbability: Int = 20): Gen[Task[A]] = {

		val immediateGen: Gen[Task[A]] = genTry(a, s"$failureLabel / Task.immediate", failureProbability).map(Task.ready)

		val ownGen: Gen[Task[A]] = genTry(a, s"$failureLabel / Task.own", failureProbability).map { tryA => Task.own(() => tryA) }

		val ownFlatGen: Gen[Task[A]] = Gen.oneOf(immediateGen, ownGen).map { taskA => Task.ownFlat(() => taskA) }

		val waitGen: Gen[Task[A]] = genFuture(a, s"$failureLabel / Task.wait").map(Task.wait)

		def foreignGen: Gen[Task[A]] = foreignDoerGenerators().genTask(a, s"failureLabel / Task.foreign").map(Task.foreign(foreignDoer)(_))

		val alienGen: Gen[Task[A]] = genFutureBuilder(a, s"$failureLabel / Task.alien").map(Task.alien)

		if synchronousOnly then Gen.oneOf(immediateGen, ownGen, ownFlatGen)
		else if includeForeign then Gen.oneOf(immediateGen, ownGen, ownFlatGen, waitGen, alienGen, foreignGen)
		else Gen.oneOf(immediateGen, ownGen, ownFlatGen, waitGen, alienGen)
	}


	/** Implicitly provide an Arbitrary instance for `doer.Task` */
	given taskArbitrary: [A] =>(arbA: Arbitrary[A]) => Arbitrary[Task[A]] = Arbitrary {
		arbA.arbitrary.flatMap(a => genTask(a, "taskArbitrary"))
	}
}
