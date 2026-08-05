package readren.sequencer

import GeneratorsForDoerTests.{*, given}

import org.scalacheck.Arbitrary
import org.scalacheck.effect.PropF
import readren.common.Maybe

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/** Trait containing tests for [[Doer]] implementations extended with [[DoerLoopingPart]].
 */
trait LoopingDoerTests[D <: Doer : ClassTag] { self: DoerProviderTestBase[D] =>

	test("Task exception handling: looping operations should propagate or uncatch exceptions correctly") {
		forAllTaskOperandExceptions { (successfulTaskParam, failingTaskExceptionParam, failingTaskParam, expectedUnhandledExceptionParam) =>
			val generators = getGenerators
			import generators.*

			def check[R](opName: String, operatedTask: Task[R], shouldPropagateNonFatales: Boolean = false): Future[Unit] = {
				val promise = Promise[Unit]()

				given Promise[Unit] = promise

				checkTaskOperandExceptionHandling(opName, operatedTask, failingTaskExceptionParam, expectedUnhandledExceptionParam, shouldPropagateNonFatales)(using promise)
			}

			def f2[A, B, C](a: A, b: B): C = throw expectedUnhandledExceptionParam

			val sTask = successfulTaskParam.asInstanceOf[Task[Int]]
			val fTask = failingTaskParam.asInstanceOf[Task[Int]]

			for {
				_ <- check("repeatedUntilSome1", sTask.repeatedUntilSome(f2))
				_ <- check("repeatedUntilSome2", fTask.repeatedUntilSome(f2))

				_ <- check("repeatedWhileEmpty1", sTask.repeatedWhileEmpty(Success(0), f2))
				_ <- check("repeatedWhileEmpty2", fTask.repeatedWhileEmpty(Success(0), f2))

				_ <- check("repeatedWhileUndefined1", sTask.repeatedWhileUndefined(0, { case (a, b) => f2[Int, Int, Int](a, b) }))
				_ <- check("repeatedWhileUndefined2", sTask.repeatedWhileUndefined(0, { case (a, b) => f2[Int, Int, Int](a, b) }))
			} yield ()
		}
	}

	test("Task exception handling: `subscribe` with looping operations should not catch exceptions thrown by MonoObserver") {
		forAllSubscribeExceptions { (task1Param, task2Param, thrownExceptionParam, futureParam) =>
			val generators = getGenerators
			import generators.*

			def check[R](opName: String, operatedTask: Task[R]): Future[Unit] = {
				val promise = Promise[Unit]()

				given Promise[Unit] = promise

				checkMonoObserverExceptionNotCaught(opName, operatedTask, thrownExceptionParam)(using promise)
			}

			val t1 = task1Param.asInstanceOf[Task[Int]]

			val randomInt = thrownExceptionParam.getMessage.hashCode()
			val smallNonNegativeInt = randomInt % 9
			val randomBool = (randomInt % 2) == 0
			val randomTryInt = if randomBool then Success(randomInt) else Failure(thrownExceptionParam)

			for {
				_ <- check("repeatedUntilSome", t1.repeatedUntilSome { (n, i) => if n > smallNonNegativeInt then Maybe(randomTryInt) else Maybe.empty })
				_ <- check("repeatedWhileEmpty", t1.repeatedWhileEmpty(Success(0), (n, tryInt) => if n > smallNonNegativeInt then Maybe(randomTryInt) else Maybe.empty))
				_ <- check("repeatedWhileUndefined", t1.repeatedWhileUndefined(Success(0), { case (n, tryInt) if n > smallNonNegativeInt => randomInt }))
			} yield ()
		}
	}
}
