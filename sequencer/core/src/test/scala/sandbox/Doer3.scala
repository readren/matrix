package readren.sequencer
package sandbox

import readren.common.*

import scala.annotation.targetName
import scala.collection.mutable
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}


/** A discarded variant of [[Doer]] design that defines the hardy operations in mix-ins.
 * Is more elegant and allows the hardy computations (Venture, LatchedVenture, etc.) to reuse the soft counterparts, but it requires the [[HardyFactory]] indirection which causes operations (map, flatMap, transform, transformWith) to do at least one more allocation than the current design. */
trait Doer3 {

	/** Queues the provided procedure for execution after previous ones. */
	def run(procedure: => Unit): Unit

	//// SOFT ////

	trait Task[+A] {
		def subscribe(onComplete: A => Unit): Unit

		def map[B](f: A => B): Task[B] = new Task_Map[A, B](this, f)
	}

	private final class Task_Map[A, B](task: Task[A], f: A => B) extends Task[B] {
		override def subscribe(onComplete: B => Unit): Unit =
			task.subscribe(a => onComplete(f(a)))
	}

	abstract class AbstractTask[+A] extends Task[A]

	private final class Task_Mine[A](supplier: () => A) extends AbstractTask[A] {
		override def subscribe(onComplete: A => Unit): Unit =
			run(onComplete(supplier()))
	}

	trait LatchingOps[+A] { thisLatchingTask: Task[A] =>
		def maybeValue: Maybe[A]

		def subscribe(consumer: A => Unit): Unit

		override def map[B](f: A => B): LatchingTask[B] = {
			this match {
				case ready: ReadyTask[A] => ready.map(f)
				case covenant: Covenant[A] @unchecked => covenant.map(f)
			}
		}

		def flatMap[B](f: A => LatchingTask[B]): LatchingTask[B] = {
			this match {
				case ready: ReadyTask[A] => ready.flatMap(f)
				case covenant: Covenant[A] @unchecked => covenant.flatMap(f)
			}
		}
	}

	sealed trait LatchingTask[+A] extends Task[A], LatchingOps[A]

	final class ReadyTask[+A](a: A) extends LatchingTask[A] {
		override val maybeValue: Maybe[A] = Maybe(a)

		override def subscribe(consumer: A => Unit): Unit = consumer(a)

		override def map[B](f: A => B): LatchingTask[B] = new ReadyTask[B](f(a))

		override def flatMap[B](f: A => LatchingTask[B]): LatchingTask[B] = f(a)
	}

	final class Covenant[A] extends LatchingTask[A], SubscriptionHub[A] {

		override def maybeValue: Maybe[A] = oValue

		override def subscribe(consumer: A => Unit): Unit =
			attach(consumer)

		override def map[B](f: A => B): LatchingTask[B] = {
			oValue.fold {
				val covenantB = new Covenant[B]
				this.subscribe(a => covenantB.fulfill(f(a)))
				covenantB
			}(a => new ReadyTask(f(a)))
		}

		override def flatMap[B](f: A => LatchingTask[B]): LatchingTask[B] = ???
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

	// TODO try replacing Task[Try[A]] with Venture[A] in the trait definition
	trait HardyOps[+A, Self[x] <: Task[Try[x]]](using selfFactory: HardyFactory[Self]) { thisVenture: Task[Try[A]] =>

		def transform[B](f: Try[A] => Try[B]): Self[B] =
			selfFactory.build[B] { onCompleted =>
				thisVenture.subscribe(tryA => onCompleted(tryA.reifyBack(f)))
			}(s"$thisVenture.transform(?)")

		@targetName("map_hardyOps")
		def map[B](f: A => B): Self[B] =
			selfFactory.build[B] { onComplete =>
				thisVenture.subscribe { tryA =>
					onComplete(tryA.mapFast(f))
				}
			}(s"$thisVenture.map(?)")

		def transformWith[B](f: Try[A] => Self[B]): Self[B] = {
			selfFactory.build[B] { onComplete =>
				thisVenture.subscribe(_.reify[Unit](e => onComplete(Failure(e)))(f(_).subscribe(onComplete)))
			}(s"$thisVenture.transformWith(?)")
		}

		def flatMap[B](f: A => Self[B]): Self[B] = {
			selfFactory.build[B] { onComplete =>
				thisVenture.subscribe {
					case Success(a) =>
						val maybeSelfB = try Maybe(f(a)) catch {
							case NonFatal(e) =>
								onComplete(Failure(e))
								Maybe.empty
						}
						maybeSelfB.foreach(_.subscribe(onComplete))
					case failure: Failure[A] =>
						onComplete(failure.castTo[B])
				}
			}(s"$thisVenture.flatMap(?)")
		}
	}

	trait Venture[+A] extends Task[Try[A]], HardyOps[A, Venture]

	abstract class AbstractVenture[+A] extends Venture[A]

	inline given HardyFactory[Venture] {
		override def build[A](engager: (onCompleted: Try[A] => Unit) => Unit)(stringifier: => String): Venture[A] = {
			new AbstractVenture[A] {
				override def subscribe(onComplete: Try[A] => Unit): Unit = engager(onComplete)

				override def toString: String = stringifier
			}
		}
	}

	sealed trait LatchingVenture[+A] extends Venture[A], LatchingOps[Try[A]] {

		@targetName("map_hardyOps")
		override def map[B](f: A => B): LatchingVenture[B] = {
			this match {
				case ready: ReadyVenture[A] => ready.map(f)
				case commitment: Commitment[A] @unchecked => commitment.map(f)
			}
		}

		override def transform[B](f: Try[A] => Try[B]): LatchingVenture[B] = ???

		def flatMap[B](f: A => LatchingVenture[B]): LatchingVenture[B] = {
			this match {
				case ready: ReadyVenture[A] => ready.flatMap(f)
				case commitment: Commitment[A] @unchecked => commitment.flatMap(f)
			}
		}

		def transformWith[B](f: Try[A] => LatchingVenture[B]): LatchingVenture[B] = ???
	}

	inline given HardyFactory[LatchingVenture] {
		override def build[A](engager: (Try[A] => Unit) => Unit)(stringifier: => String): LatchingVenture[A] = ???
	}

	final class ReadyVenture[+A](value: Try[A]) extends LatchingVenture[A] {

		override val maybeValue: Maybe[Try[A]] = Maybe(value)

		override def subscribe(consumer: Try[A] => Unit): Unit = consumer(value)

		@targetName("map_hardyOps")
		override def map[B](f: A => B): LatchingVenture[B] = ???

		override def transform[B](f: Try[A] => Try[B]): LatchingVenture[B] = ???

		override def flatMap[B](f: A => LatchingVenture[B]): LatchingVenture[B] = ???

		override def transformWith[B](f: Try[A] => LatchingVenture[B]): LatchingVenture[B] = ???


	}

	final class Commitment[A] extends LatchingVenture[A], SubscriptionHub[Try[A]] {

		override def subscribe(consumer: Try[A] => Unit): Unit =
			attach(consumer)

		override def maybeValue: Maybe[Try[A]] = oValue

		@targetName("map_hardyOps")
		override def map[B](f: A => B): LatchingVenture[B] = ???

		override def transform[B](f: Try[A] => Try[B]): LatchingVenture[B] = ???

		override def flatMap[B](f: A => LatchingVenture[B]): LatchingVenture[B] = ???

		override def transformWith[B](f: Try[A] => LatchingVenture[B]): LatchingVenture[B] = ???

	}
}
