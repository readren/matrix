package readren.sequencer

import readren.common.Maybe
import scala.util.{Try, Success, Failure}
import scala.util.control.NonFatal
import scala.annotation.targetName
import readren.common.*

/** A sandbox prototyping the Task (work) and PotentialValue (result) hierarchies under Observable,
 * verifying path-dependent typing, covariance, and flatMap/map signature specialization
 * with specialized flatMap methods and monadic Venture/TrialCapturer behaviors.
 */
trait SandboxDoer { thisDoer =>

	/** Root super trait of all asynchronous observables. */
	trait Observable[+A] {
		def subscribe(onComplete: A => Unit): Unit

		def foreach(consumer: A => Unit): Unit = subscribe(consumer)

		def map[B](f: A => B): Observable[B]

		def flatMap[B](f: A => Observable[B]): Observable[B]
	}

	// ==================== DOABLE WORK HIERARCHY ====================

	/** An exception-unaware lazy computation that starts a fresh execution on subscribe.
	 * Serves as the root of all doable work.
	 */
	trait Task[+A] extends Observable[A] { thisTask =>
		override def map[B](f: A => B): Task[B] =
			(onComplete: B => Unit) => thisTask.subscribe(a => onComplete(f(a)))

		override def flatMap[B](f: A => Observable[B]): Task[B] =
			(onComplete: B => Unit) => thisTask.subscribe(a => f(a).subscribe(onComplete))

		@targetName("flatMapTask")
		def flatMap[B](f: A => Task[B]): Task[B] =
			(onComplete: B => Unit) => thisTask.subscribe(a => f(a).subscribe(onComplete))
	}

	// ==================== ASYNCHRONOUS RESULT HIERARCHY ====================

	/** Root covariant facade of a memoized/caching result of an already started task. */
	trait PotentialValue[+A] extends Observable[A] { thisLatching =>
		def maybeResult: Maybe[A]

		inline def isCompleted: Boolean = maybeResult.isDefined

		inline def isPending: Boolean = maybeResult.isEmpty

		def unsubscribe(onComplete: A => Unit): Unit

		def isSubscribed(onComplete: A => Unit): Boolean
	}

	/** Exception-unaware single result capturer. Ex LatchingTask
	 * Does not inherit from Task, cleanly separating results from doable work. */
	sealed trait Capturer[+A] extends PotentialValue[A] { thisLatch =>
		override def map[B](f: A => B): Capturer[B] = { // necessary to downcast the result type when a subclass isn't covariant.
			this match {
				case ready: Keeper[A] => ready.map(f)
				case captor: Captor[A] @unchecked => captor.map(f)
			}
		}

		override def flatMap[B](f: A => Observable[B]): Observable[B] = { // necessary to downcast the result type when a subclass isn't covariant.
			this match {
				case ready: Keeper[A] => ready.flatMap(f)
				case captor: Captor[A] @unchecked => captor.flatMap(f)
			}
		}

		// Specialized flatMap
		@targetName("flatMapCapturer")
		def flatMap[B](f: A => Capturer[B]): Capturer[B] = { // necessary to downcast the result type when a subclass isn't covariant.
			this match {
				case ready: Keeper[A] => ready.flatMap(f)
				case captor: Captor[A] @unchecked => captor.flatMap(f)
			}
		}
	}

	/** A [[Capturer]] that has already captured a value. Ex ReadyTask */
	class Keeper[+A](val value: A) extends Capturer[A] {
		override def maybeResult: Maybe[A] = Maybe(value)

		override def subscribe(onComplete: A => Unit): Unit = onComplete(value)

		override def unsubscribe(onComplete: A => Unit): Unit = ()

		override def isSubscribed(onComplete: A => Unit): Boolean = false

		override def map[B](f: A => B): Capturer[B] = new Keeper(f(value))

		override def flatMap[B](f: A => Observable[B]): Observable[B] = f(value)

		@targetName("flatMapCapturer")
		override def flatMap[B](f: A => Capturer[B]): Capturer[B] = f(value)
	}

	/** Ex Covenant. */
	class Captor[A](initialResult: Maybe[A] = Maybe.empty) extends Capturer[A] {
		private var oResult: Maybe[A] = initialResult
		private var subscribers: List[A => Unit] = Nil

		override def maybeResult: Maybe[A] = oResult

		override def subscribe(onComplete: A => Unit): Unit = {
			oResult.fold {
				subscribers = onComplete :: subscribers
			} { a =>
				onComplete(a)
			}
		}

		override def unsubscribe(onComplete: A => Unit): Unit = {
			subscribers = subscribers.filterNot(_ eq onComplete)
		}

		override def isSubscribed(onComplete: A => Unit): Boolean = {
			subscribers.exists(_ eq onComplete)
		}

		def fulfill(result: A): Unit = {
			oResult.fold {
				oResult = Maybe(result)
				val currentSubscribers = subscribers
				subscribers = Nil
				currentSubscribers.reverse.foreach(sub => sub(result))
			}(_ => ())
		}

		override def map[B](f: A => B): Capturer[B] = {
			oResult.fold {
				val cov = new Captor[B]()
				this.subscribe(a => cov.fulfill(f(a)))
				cov
			} { a =>
				new Keeper(f(a))
			}
		}

		override def flatMap[B](f: A => Observable[B]): Observable[B] = {
			oResult.fold {
				val captor = new Captor[B]()
				this.subscribe(a => f(a).subscribe(b => captor.fulfill(b)))
				captor
			}(f)
		}

		@targetName("flatMapCapturer")
		override def flatMap[B](f: A => Capturer[B]): Capturer[B] = {
			oResult.fold {
				val captor = new Captor[B]()
				this.subscribe(a => f(a).subscribe(b => captor.fulfill(b)))
				captor
			}(f)
		}
	}

	/////////////// Trial hierarchies ///////////////

	/** A lazy exception-aware computation.
	 * Monadic combinators map/flatMap over the success value of the Try. */
	trait Venture[+A] extends Task[Try[A]] { thisVenture =>

		def transform[B](f: Try[A] => Try[B]): Venture[B] =
			(onComplete: Try[B] => Unit) => thisVenture.subscribe { tryA =>
				val tryB =
					try f(tryA)
					catch {
						case NonFatal(e) => Failure(e)
					}
				onComplete(tryB)
			}

		def transformWith[B](f: Try[A] => Venture[B]): Venture[B] =
			(onComplete: Try[B] => Unit) => thisVenture.subscribe { tryA =>
				val maybeVentureB =
					try Maybe(f(tryA))
					catch {
						case NonFatal(e) =>
							onComplete(Failure(e))
							Maybe.empty
					}
				maybeVentureB.foreach(_.subscribe(onComplete))
			}

		@targetName("mapSuccess") // Differentiates from Task.map(Try[A] => B) to prevent JVM signature clashes on return-type alignment and enable Java interop
		def map[B](f: A => B): Venture[B] =
			(onComplete: Try[B] => Unit) => thisVenture.subscribe {
				case Success(a) =>
					val tryB =
						try Success(f(a))
						catch {
							case NonFatal(e) => Failure(e)
						}
					onComplete(tryB)
				case failure: Failure[A] => onComplete(failure.asInstanceOf[Failure[B]])
			}

		def flatMap[B](f: A => Venture[B]): Venture[B] =
			(onComplete: Try[B] => Unit) => {
				thisVenture.subscribe {
					case Success(a) =>
						val maybeVentureB =
							try Maybe(f(a))
							catch {
								case NonFatal(e) =>
									onComplete(Failure(e))
									Maybe.empty
							}
						maybeVentureB.foreach(_.subscribe(onComplete))
					case failure: Failure[A] =>
						onComplete(failure.asInstanceOf[Failure[B]])
				}
			}

		@targetName("flatMapSuccess")
		def flatMap[B](f: A => Observable[Try[B]]): Venture[B] =
			(onComplete: Try[B] => Unit) => thisVenture.subscribe {
				case Success(a) =>
					val maybeObservable =
						try Maybe(f(a))
						catch {
							case NonFatal(e) =>
								onComplete(Failure(e))
								Maybe.empty
						}
					maybeObservable.foreach(_.subscribe(onComplete))
				case failure: Failure[A] =>
					onComplete(failure.asInstanceOf[Failure[B]])
			}
	}

	def Venture_ready[A](tryA: Try[A]): Venture[A] =
		(onComplete: Try[A] => Unit) => onComplete(tryA)


	/** Exception-aware latching result (caches a Try[A]).
	 * Monadic combinators map/flatMap over the success value of the Try.
	 */
	sealed trait TrialCapturer[+A] extends PotentialValue[Try[A]] { thisTrialCapturer =>

		override def map[B](f: Try[A] => B): Capturer[B] = thisTrialCapturer match {
			case keeper: TrialKeeper[A] => keeper.map(f)
			case captor: TrialCaptor[A] @unchecked => captor.map(f)
		}

		override def flatMap[B](f: Try[A] => Observable[B]): Observable[B] = thisTrialCapturer match {
			case keeper: TrialKeeper[A] => keeper.flatMap(f)
			case captor: TrialCaptor[A] @unchecked => captor.flatMap(f)
		}

		def flatMap[B](f: Try[A] => Capturer[B]): Capturer[B] = thisTrialCapturer match {
			case keeper: TrialKeeper[A] => keeper.flatMap(f)
			case captor: TrialCaptor[A] @unchecked => captor.flatMap(f)
		}

		@targetName("mapSuccess") // Differentiates from PotentialValue.map(Try[A] => B) to prevent JVM signature clashes on return-type alignment and enable Java interop
		def map[B](f: A => B): TrialCapturer[B] = thisTrialCapturer match {
			case keeper: TrialKeeper[A] => keeper.map(f)
			case captor: TrialCaptor[A] @unchecked => captor.map(f)
		}

		def flatMap[B](f: A => TrialCapturer[B]): TrialCapturer[B] = thisTrialCapturer match {
			case keeper: TrialKeeper[A] => keeper.flatMap(f)
			case captor: TrialCaptor[A] @unchecked => captor.flatMap(f)
		}

		@targetName("flatMapSuccess")
		def flatMap[B](f: A => Observable[Try[B]]): Observable[Try[B]] = thisTrialCapturer match {
			case keeper: TrialKeeper[A] => keeper.flatMap(f)
			case captor: TrialCaptor[A] @unchecked => captor.flatMap(f)
		}
	}

	class TrialKeeper[+A](val value: Try[A]) extends TrialCapturer[A] {
		override def maybeResult: Maybe[Try[A]] = Maybe(value)

		override def unsubscribe(onComplete: Try[A] => Unit): Unit = ()

		override def isSubscribed(onComplete: Try[A] => Unit): Boolean = false

		override def subscribe(onComplete: Try[A] => Unit): Unit = onComplete(value)

		override def map[B](f: Try[A] => B): Capturer[B] = new Keeper(f(value))

		override def flatMap[B](f: Try[A] => Observable[B]): Observable[B] = f(value)

		override def flatMap[B](f: Try[A] => Capturer[B]): Capturer[B] = f(value)

		@targetName("mapSuccess")
		override def map[B](f: A => B): TrialKeeper[B] = value match {
			case Success(a) =>
				val tryB =
					try Success(f(a))
					catch {
						case NonFatal(e) => Failure(e)
					}
				new TrialKeeper(tryB)
			case Failure(e) =>
				this.asInstanceOf[TrialKeeper[B]]
		}

		override def flatMap[B](f: A => TrialCapturer[B]): TrialCapturer[B] = value match {
			case Success(a) =>
				try f(a)
				catch {
					case NonFatal(e) => new TrialKeeper(Failure(e))
				}
			case Failure(e) => this.asInstanceOf[TrialKeeper[B]]
		}

		@targetName("flatMapSuccess")
		override def flatMap[B](f: A => Observable[Try[B]]): Observable[Try[B]] = value match {
			case Success(a) =>
				try f(a)
				catch {
					case NonFatal(e) => new TrialKeeper(Failure(e))
				}
			case Failure(e) => this.asInstanceOf[TrialKeeper[B]]
		}
	}

	class TrialCaptor[A](initialResult: Maybe[Try[A]] = Maybe.empty) extends TrialCapturer[A] {
		private var oResult: Maybe[Try[A]] = initialResult
		private var subscribers: List[Try[A] => Unit] = Nil

		override def maybeResult: Maybe[Try[A]] = oResult

		override def subscribe(onComplete: Try[A] => Unit): Unit = {
			oResult.fold {
				subscribers = onComplete :: subscribers
			} { tryA =>
				onComplete(tryA)
			}
		}

		override def unsubscribe(onComplete: Try[A] => Unit): Unit = {
			subscribers = subscribers.filterNot(_ eq onComplete)
		}

		override def isSubscribed(onComplete: Try[A] => Unit): Boolean = {
			subscribers.exists(_ eq onComplete)
		}

		def fulfill(result: Try[A]): Unit = {
			oResult.fold {
				oResult = Maybe(result)
				val currentSubscribers = subscribers
				subscribers = Nil
				currentSubscribers.reverse.foreach(sub => sub(result))
			}(_ => ())
		}

		override def map[B](f: Try[A] => B): Capturer[B] = {
			oResult.fold {
				val captor = new Captor[B]()
				this.subscribe(tryA => captor.fulfill(f(tryA)))
				captor
			} { tryA =>
				new Keeper(f(tryA))
			}
		}

		override def flatMap[B](f: Try[A] => Observable[B]): Observable[B] = {
			oResult.fold {
				val captor = new Captor[B]()
				this.subscribe(tryA => f(tryA).subscribe(b => captor.fulfill(b)))
				captor
			}(f)
		}

		override def flatMap[B](f: Try[A] => Capturer[B]): Capturer[B] = {
			oResult.fold {
				val captor = new Captor[B]()
				this.subscribe(tryA => f(tryA).subscribe(b => captor.fulfill(b)))
				captor
			}(f)
		}

		@targetName("mapSuccess")
		override def map[B](f: A => B): TrialCapturer[B] = {
			oResult.fold {
				val captor = new TrialCaptor[B]()
				this.subscribe {
					case Success(a) =>
						val tryB =
							try Success(f(a))
							catch {
								case NonFatal(e) => Failure(e)
							}
						captor.fulfill(tryB)
					case failure: Failure[A] =>
						captor.fulfill(failure.asInstanceOf[Failure[B]])
				}
				captor
			} {
				case Success(a) =>
					val tryB =
						try Success(f(a))
						catch {
							case NonFatal(e) => Failure(e)
						}
					new TrialKeeper(tryB)
				case failure: Failure[A] =>
					this.asInstanceOf[TrialCapturer[B]]
			}
		}

		override def flatMap[B](f: A => TrialCapturer[B]): TrialCapturer[B] = {
			oResult.fold {
				val captor = new TrialCaptor[B]()
				this.subscribe {
					case Success(a) =>
						val maybeCapturer =
							try Maybe(f(a))
							catch {
								case NonFatal(e) =>
									captor.fulfill(Failure(e))
									Maybe.empty
							}
						maybeCapturer.foreach(_.subscribe(tryB => captor.fulfill(tryB)))
					case failure: Failure[A] =>
						captor.fulfill(failure.asInstanceOf[Failure[B]])
				}
				captor
			} {
				case Success(a) =>
					try f(a)
					catch {
						case NonFatal(e) => new TrialKeeper(Failure(e))
					}
				case failure: Failure[A] => this.asInstanceOf[TrialCapturer[B]]
			}
		}

		@targetName("flatMapSuccess")
		override def flatMap[B](f: A => Observable[Try[B]]): Observable[Try[B]] = {
			oResult.fold {
				val captor = new TrialCaptor[B]()
				this.subscribe {
					case Success(a) =>
						val maybeObservable =
							try Maybe(f(a))
							catch {
								case NonFatal(e) =>
									captor.fulfill(Failure(e))
									Maybe.empty
							}
						maybeObservable.foreach(_.subscribe(captor.fulfill))
					case failure: Failure[A] =>
						captor.fulfill(failure.asInstanceOf[Failure[B]])
				}
				captor
			} {
				case Success(a) =>
					try f(a)
					catch {
						case NonFatal(e) => new TrialKeeper(Failure(e))
					}
				case failure: Failure[A] =>
					new TrialKeeper(failure.castTo[B])
			}
		}
	}
}