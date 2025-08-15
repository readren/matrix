package readren.sequencer

import DoerTestSync.currentDoer

import munit.ScalaCheckEffectSuite
import org.scalacheck.Prop.propBoolean
import org.scalacheck.{Arbitrary, Gen, Prop}

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object DoerTestSync {
	val currentDoer: ThreadLocal[Doer] = new ThreadLocal()
}

/** This suite is limited to synchronous tests, and therefore, it only tests part of the behavior of [[Doer]].
 * All the behavior tested in this suite is also tested in the [[DoerTestEffect]] suite. 
 * This suite is kept despite the [[DoerTestEffect]] existence because it is easier to debug in a synchronous environment.
 * */
class DoerTestSync extends ScalaCheckEffectSuite {

	private var oDoerThreadId: Option[Long] = None


	private val doer: Doer = new Doer { thisDoer =>
		override type Tag = String
		
		override val tag = "testing doer"
		
		override def executeSequentially(runnable: Runnable): Unit = {
			oDoerThreadId match {
				case None => oDoerThreadId = Some(Thread.currentThread().getId)
				case Some(doerThreadId) => assert(doerThreadId == Thread.currentThread().getId)
			}
			currentDoer.set(thisDoer)
			try runnable.run()
			finally currentDoer.remove()
		}

		override def current: Doer = currentDoer.get

		override def reportFailure(cause: Throwable): Unit = throw cause
	}

	import doer.*
	import Task.*

	private val shared = new DoerTestShared[doer.type](doer, true)
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

	// "A task should satisfy monadic laws"

	// Left Identity: `unit(a).flatMap(f) == f(a)`
	property("A task should satisfy monadic laws: left identity")  {
		Prop.forAll { (a: Int, f: Int => Task[Int]) =>
			val leftTask = successful(a).flatMap(f)
			val rightTask = f(a)
			checkEquality(leftTask, rightTask)
		}
	}

	// Right Identity: `m.flatMap(a => unit(a)) == m`
	property("A task should satisfy monadic laws: right identity") {
		Prop.forAll { (m: Task[Int]) =>
			val left = m.flatMap(a => successful(a))
			val right = m
			checkEquality(left, right)
		}
	}

    // Associativity: `(m.flatMap(f)).flatMap(g) == m.flatMap(a => f(a).flatMap(g))`
	property("A task should satisfy monadic laws: associativity") {
		Prop.forAll { (m: Task[Int], f: Int => Task[Int], g: Int => Task[Int]) =>
			val left = m.flatMap(f).flatMap(g)
			val right = m.flatMap(a => f(a).flatMap(g))
			checkEquality(left, right)
		}
	}
	
	// Functor: `m.map(f) == m.flatMap(a => unit(f(a)))`
	property("A task is a functor: can be transformed with map") {
		Prop.forAll { (m: Task[Int], f: Int => String) =>
			val left = m.map(f)
			val right = m.flatMap(a => successful(f(a)))
			checkEquality(left, right)
		}
	}

	// Recovery: `failedTask.recover(f) == if f.isDefinedAt(e) then successful(f(e)) else failed(e)` where e is the exception thrown by failedTask
	property("A task can be recovered from a non fatal failure") {
		Prop.forAll { (e: Throwable, f: PartialFunction[Throwable, Int]) =>
			NonFatal(e) ==> {
				val leftTask = failed[Int](e).recover(f)
				val rightTask = if f.isDefinedAt(e) then successful(f(e)) else failed(e)
				checkEquality(leftTask, rightTask)
				true
			}
		}
	}

	// Combining Tasks: `combine(taskA, taskB)(zip) == own(() => zip(tryA, tryB))`
	property("A task can be combined") {
		Prop.forAll(intGen, intGen, Gen.oneOf(true, false), Gen.frequency((3, true), (1, false))) {
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

	property("Any task can be combined") {
		Prop.forAll { (taskA: Task[Int], taskB: Task[Int], f: (Try[Int], Try[Int]) => Try[Int]) =>
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
