package readren.sequencer
package sandbox

import readren.common.*

import scala.annotation.targetName
import scala.collection.mutable
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}


/** A discarded variant of [[Doer]] design that defines the hardy operations in mix-ins.
 * Is more elegant and allows the hardy computations (Task, LatchedTask, etc.) to reuse the soft counterparts, but it requires the [[HardyFactory]] indirection which causes operations (map, flatMap, transform, transformWith) to do at least one more allocation than the current design. */
trait Doer3 {

	/** Queues the provided procedure for execution after previous ones. */
	def run(procedure: => Unit): Unit

	//// SOFT ////

	trait Duty[+A] {
		def engage(onComplete: A => Unit): Unit

		def map[B](f: A => B): Duty[B] = new Duty_Map[A, B](this, f)
	}

	private final class Duty_Map[A, B](duty: Duty[A], f: A => B) extends Duty[B] {
		override def engage(onComplete: B => Unit): Unit =
			duty.engage(a => onComplete(f(a)))
	}

	abstract class AbstractDuty[+A] extends Duty[A]

	private final class Duty_Mine[A](supplier: () => A) extends AbstractDuty[A] {
		override def engage(onComplete: A => Unit): Unit =
			run(onComplete(supplier()))
	}

	trait LatchingOps[+A] { thisLatchingDuty: Duty[A] =>
		def maybeValue: Maybe[A]

		def subscribe(consumer: A => Unit): Unit

		override def map[B](f: A => B): LatchingDuty[B] = {
			this match {
				case ready: ReadyDuty[A] => ready.map(f)
				case covenant: Covenant[A] @unchecked => covenant.map(f)
			}
		}

		def flatMap[B](f: A => LatchingDuty[B]): LatchingDuty[B] = {
			this match {
				case ready: ReadyDuty[A] => ready.flatMap(f)
				case covenant: Covenant[A] @unchecked => covenant.flatMap(f)
			}
		}
	}

	sealed trait LatchingDuty[+A] extends Duty[A], LatchingOps[A]

	final class ReadyDuty[+A](a: A) extends LatchingDuty[A] {
		override val maybeValue: Maybe[A] = Maybe(a)

		override def engage(onComplete: A => Unit): Unit = onComplete(a)

		override def subscribe(consumer: A => Unit): Unit = consumer(a)

		override def map[B](f: A => B): LatchingDuty[B] = new ReadyDuty[B](f(a))

		override def flatMap[B](f: A => LatchingDuty[B]): LatchingDuty[B] = f(a)
	}

	final class Covenant[A] extends LatchingDuty[A], SubscriptionHub[A] {

		override def engage(onComplete: A => Unit): Unit =
			attach(onComplete)

		override def maybeValue: Maybe[A] = oValue

		override def subscribe(consumer: A => Unit): Unit =
			attach(consumer)

		override def map[B](f: A => B): LatchingDuty[B] = {
			oValue.fold {
				val covenantB = new Covenant[B]
				this.subscribe(a => covenantB.fulfill(f(a)))
				covenantB
			}(a => new ReadyDuty(f(a)))
		}

		override def flatMap[B](f: A => LatchingDuty[B]): LatchingDuty[B] = ???
	}

	trait SubscriptionHub[A] {
		protected var oValue: Maybe[A] = Maybe.empty
		protected val consumers: mutable.Buffer[A => Unit] = mutable.Buffer.empty

		def attach(consumer: A => Unit): Unit =
			oValue.fold(consumers.addOne(consumer))(consumer)

		def fulfill(value: A): Unit =
			if oValue.isEmpty then {
				oValue = Maybe(value)
				consumers.foreach(consumer => consumer(value))
				consumers.clear()
			}
	}

	//// HARDY ////

	trait HardyFactory[H[_]] {
		def build[A](engager: (onCompleted: Try[A] => Unit) => Unit)(stringifier: => String): H[A]
	}

	// TODO try replacing Duty[Try[A]] with Task[A] in the trait definition
	trait HardyOps[+A, Self[x] <: Duty[Try[x]]](using selfFactory: HardyFactory[Self]) { thisTask: Duty[Try[A]] =>

		def transform[B](f: Try[A] => Try[B]): Self[B] =
			selfFactory.build[B] { onCompleted =>
				thisTask.engage(tryA => onCompleted(tryA.reifyBack(f)))
			}(s"$thisTask.transform(?)")

		@targetName("map_hardyOps")
		def map[B](f: A => B): Self[B] =
			selfFactory.build[B] { onComplete =>
				thisTask.engage { tryA =>
					onComplete(tryA.mapFast(f))
				}
			}(s"$thisTask.map(?)")

		def transformWith[B](f: Try[A] => Self[B]): Self[B] = {
			selfFactory.build[B] { onComplete =>
				thisTask.engage(_.reify[Unit](e => onComplete(Failure(e)))(f(_).engage(onComplete)))
			}(s"$thisTask.transformWith(?)")
		}

		def flatMap[B](f: A => Self[B]): Self[B] = {
			selfFactory.build[B] { onComplete =>
				thisTask.engage {
					case Success(a) =>
						val maybeSelfB = try Maybe(f(a)) catch {
							case NonFatal(e) =>
								onComplete(Failure(e))
								Maybe.empty
						}
						maybeSelfB.foreach(_.engage(onComplete))
					case failure: Failure[A] =>
						onComplete(failure.castTo[B])
				}
			}(s"$thisTask.flatMap(?)")
		}
	}

	trait Task[+A] extends Duty[Try[A]], HardyOps[A, Task]

	abstract class AbstractTask[+A] extends Task[A]

	inline given HardyFactory[Task] {
		override def build[A](engager: (onCompleted: Try[A] => Unit) => Unit)(stringifier: => String): Task[A] = {
			new AbstractTask[A] {
				override def engage(onComplete: Try[A] => Unit): Unit = engager(onComplete)

				override def toString: String = stringifier
			}
		}
	}

	sealed trait LatchingTask[+A] extends Task[A], LatchingOps[Try[A]] {

		@targetName("map_hardyOps")
		override def map[B](f: A => B): LatchingTask[B] = {
			this match {
				case ready: ReadyTask[A] => ready.map(f)
				case commitment: Commitment[A] @unchecked => commitment.map(f)
			}
		}

		override def transform[B](f: Try[A] => Try[B]): LatchingTask[B] = ???

		def flatMap[B](f: A => LatchingTask[B]): LatchingTask[B] = {
			this match {
				case ready: ReadyTask[A] => ready.flatMap(f)
				case commitment: Commitment[A] @unchecked => commitment.flatMap(f)
			}
		}

		def transformWith[B](f: Try[A] => LatchingTask[B]): LatchingTask[B] = ???
	}

	inline given HardyFactory[LatchingTask] {
		override def build[A](engager: (Try[A] => Unit) => Unit)(stringifier: => String): LatchingTask[A] = ???
	}

	final class ReadyTask[+A](value: Try[A]) extends LatchingTask[A] {
		override def engage(onComplete: Try[A] => Unit): Unit = onComplete(value)

		override val maybeValue: Maybe[Try[A]] = Maybe(value)

		override def subscribe(consumer: Try[A] => Unit): Unit = consumer(value)

		@targetName("map_hardyOps")
		override def map[B](f: A => B): LatchingTask[B] = ???

		override def transform[B](f: Try[A] => Try[B]): LatchingTask[B] = ???

		override def flatMap[B](f: A => LatchingTask[B]): LatchingTask[B] = ???

		override def transformWith[B](f: Try[A] => LatchingTask[B]): LatchingTask[B] = ???


	}

	final class Commitment[A] extends LatchingTask[A], SubscriptionHub[Try[A]] {

		override def engage(onComplete: Try[A] => Unit): Unit =
			attach(onComplete)

		override def subscribe(consumer: Try[A] => Unit): Unit =
			attach(consumer)

		override def maybeValue: Maybe[Try[A]] = oValue

		@targetName("map_hardyOps")
		override def map[B](f: A => B): LatchingTask[B] = ???

		override def transform[B](f: Try[A] => Try[B]): LatchingTask[B] = ???

		override def flatMap[B](f: A => LatchingTask[B]): LatchingTask[B] = ???

		override def transformWith[B](f: Try[A] => LatchingTask[B]): LatchingTask[B] = ???

	}
}
