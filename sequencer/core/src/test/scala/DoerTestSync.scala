package readren.sequencer

import DoerTestSync.currentAssistant

import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.compatible.Assertion
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object DoerTestSync {
	val currentAssistant: ThreadLocal[Doer.Assistant] = new ThreadLocal()
}

/** This suite is limited to synchronous tests, and therefore, it only tests part of the behavior of [[Doer]].
 * All the behavior tested in this suite is also tested in the [[DoerTestEffect]] suite. 
 * This suite is kept despite the [[DoerTestEffect]] existence because it is easier to debug in a synchronous environment.
 * */
class DoerTestSync extends AnyFreeSpec with ScalaCheckPropertyChecks with Matchers {

	private var oDoerThreadId: Option[Long] = None
	private val theAssistant = new Doer.Assistant { thisAssistant =>
		override def executeSequentially(runnable: Runnable): Unit = {
			oDoerThreadId match {
				case None => oDoerThreadId = Some(Thread.currentThread().getId)
				case Some(doerThreadId) => assert(doerThreadId == Thread.currentThread().getId)
			}
			currentAssistant.set(thisAssistant)
			try runnable.run()
			finally currentAssistant.remove()
		}

		override def current: Doer.Assistant = currentAssistant.get

		override def reportFailure(cause: Throwable): Unit = throw cause
	}

	val doer: Doer = new Doer {
		override type Assistant = theAssistant.type
		override val assistant: Assistant = theAssistant
	}

	import doer.*
	import Task.*

	val shared = new DoerTestShared[doer.type](doer, true)
	import shared.{*, given}

	private def checkEquality[A](task1: Task[A], task2: Task[A]): Unit = {
		combine(task1, task2) {
			(a1, a2) => Success((a1, a2))
		}.trigger() {
			case Success(z) => assert(z._1 ==== z._2)
			case Failure(cause) => throw cause
		}
	}

	//	implicit override val generatorDrivenConfig: PropertyCheckConfiguration = PropertyCheckConfiguration(minSuccessful = 1)

	"A task should satisfy monadic laws" - {

		// Left Identity: `unit(a).flatMap(f) == f(a)`
		"left identity" in {
			forAll { (a: Int, f: Int => Task[Int]) =>
				val leftTask = successful(a).flatMap(f)
				val rightTask = f(a)
				checkEquality(leftTask, rightTask)
			}
		}

		// Right Identity: `m.flatMap(a => unit(a)) == m`
		"right identity" in {
			forAll { (m: Task[Int]) =>
				val left = m.flatMap(a => successful(a))
				val right = m
				checkEquality(left, right)
			}
		}

		// Associativity: `(m.flatMap(f)).flatMap(g) == m.flatMap(a => f(a).flatMap(g))`
		"associativity" in {
			forAll { (m: Task[Int], f: Int => Task[Int], g: Int => Task[Int]) =>
				val left = m.flatMap(f).flatMap(g)
				val right = m.flatMap(a => f(a).flatMap(g))
				checkEquality(left, right)
			}
		}
	}
	// Functor: `m.map(f) == m.flatMap(a => unit(f(a)))`
	"Task can be transformed with map" in {
		forAll { (m: Task[Int], f: Int => String) =>
			val left = m.map(f)
			val right = m.flatMap(a => successful(f(a)))
			checkEquality(left, right)
		}
	}

	// Recovery: `failedTask.recover(f) == if f.isDefinedAt(e) then successful(f(e)) else failed(e)` where e is the exception thrown by failedTask
	"Task can be recovered from failure" in {
		forAll { (e: Throwable, f: PartialFunction[Throwable, Int]) =>
			whenever(NonFatal(e)) {
				val leftTask = failed[Int](e).recover(f)
				val rightTask = if f.isDefinedAt(e) then successful(f(e)) else failed(e);
				checkEquality(leftTask, rightTask)
			}
		}
	}

	// Combining Tasks: `combine(taskA, taskB)(zip) == own(() => zip(tryA, tryB))`
	"Task can be combined" in {
		forAll(intGen, intGen, Gen.oneOf(true, false), Gen.frequency((3, true), (1, false))) {
			(intA, intB, zipReturnsSuccess, zipFinishesNormally) =>
				def buildTryAndImmediateTask(i: Int): (Try[Int], Task[Int]) = {
					if i >= 0 then (Success(i), successful(i))
					else {
						val cause = new RuntimeException(i.toString)
						(Failure(cause), failed(cause))
					}
				}

				def zip(ta: Try[Int], tb: Try[Int]): Try[(Try[Int], Try[Int])] =
					if zipFinishesNormally then {
						if zipReturnsSuccess then Success((ta, tb))
						else Failure(new RuntimeException(s"finished normally for: intA=$intA, intB=$intB"))
					}
					else throw new RuntimeException(s"finished abruptly for: intA=$intA, intB=$intB")

				println(s"0) intA=$intA, intB=$intB, zipReturnsSuccess=$zipReturnsSuccess, zipFinishesNormally=$zipFinishesNormally")

				val (tryA, taskA) = buildTryAndImmediateTask(intA)
				val (tryB, taskB) = buildTryAndImmediateTask(intB)
				println(s"1) tryA = $tryA, tryB = $tryB")

				val left: Task[(Try[Int], Try[Int])] = combine(taskA, taskB)(zip)
				val right: Task[(Try[Int], Try[Int])] = own(() => zip(tryA, tryB))

				left.trigger() { resultLeft =>
					right.trigger(true) { resultRight =>
						println(s"3) resultLeft = $resultLeft, resultRight = $resultRight")
						(resultLeft, resultRight) match {
							case (Success(a), Success(b)) => assert(a._1 ==== b._1)
							case (Failure(a), Failure(b)) => assert(a ==== b)
							case _ => assert(false)
						}
					}
				}
		}
	}

	"Any task can be combined" in {
		forAll { (taskA: Task[Int], taskB: Task[Int], f: (Try[Int], Try[Int]) => Try[Int]) =>
			val combined = combine(taskA, taskB)(f)

			// do the check in all possible triggering orders
			combined.trigger() { combinedResult =>
				taskA.trigger(true) { taskAResult =>
					taskB.trigger(true) { taskBResult =>
						assert(combinedResult ==== f(taskAResult, taskBResult))
					}
				}
			}
			combined.trigger() { combinedResult =>
				taskB.trigger(true) { taskBResult =>
					taskA.trigger(true) { taskAResult =>
						assert(combinedResult ==== f(taskAResult, taskBResult))
					}
				}
			}
			taskA.trigger() { taskAResult =>
				combined.trigger(true) { combinedResult =>
					taskB.trigger(true) { taskBResult =>
						assert(combinedResult ==== f(taskAResult, taskBResult))
					}
				}
			}
			taskA.trigger() { taskAResult =>
				taskB.trigger(true) { taskBResult =>
					combined.trigger(true) { combinedResult =>
						assert(combinedResult ==== f(taskAResult, taskBResult))
					}
				}
			}
			taskB.trigger() { taskBResult =>
				combined.trigger(true) { combinedResult =>
					taskA.trigger(true) { taskAResult =>
						assert(combinedResult ==== f(taskAResult, taskBResult))
					}
				}
			}
			taskB.trigger() { taskBResult =>
				taskA.trigger(true) { taskAResult =>
					combined.trigger(true) { combinedResult =>
						assert(combinedResult ==== f(taskAResult, taskBResult))
					}
				}
			}
		}
	}
}
