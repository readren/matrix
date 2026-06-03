package readren.sequencer

import readren.common.*

import scala.annotation.targetName
import scala.reflect.ClassTag
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/** A sandbox prototyping the Task (work) and Settling (result) hierarchies under Observable,
 * verifying path-dependent typing, covariance, and flatMap/map signature specialization
 * with specialized flatMap methods and monadic Venture/TrialCapturer behaviors.
 */
trait SandboxDoer { thisDoer =>
	type Key = AnyRef

	inline given CanEqual[Key, Key] = CanEqual.derived

	type Consumer[-A] = A => Unit
	type ArrayConsumer[-A] = (A, Int) => Unit
	type MatrixConsumer[-A] = (A, Int, Int) => Unit

	/** Root super trait of all asynchronous observables. */
	trait Observable[+A] {
		def subscribe(onComplete: Consumer[A]): Unit

		def foreach(consumer: Consumer[A]): Unit = subscribe(consumer)

		def map[B](f: A => B): Observable[B]

		def flatMap[B](f: A => Observable[B]): Observable[B]
	}

	// ==================== DOABLE WORK HIERARCHY ====================

	/** An exception-unaware lazy computation that starts a fresh execution on subscribe.
	 * Serves as the root of all doable work.
	 */
	trait Task[+A] extends Observable[A] { thisTask =>
		override def map[B](f: A => B): Task[B] =
			(onComplete: Consumer[B]) => thisTask.subscribe(a => onComplete(f(a)))

		override def flatMap[B](f: A => Observable[B]): Task[B] =
			(onComplete: Consumer[B]) => thisTask.subscribe(a => f(a).subscribe(onComplete))

		@targetName("flatMapTask")
		def flatMap[B](f: A => Task[B]): Task[B] =
			(onComplete: Consumer[B]) => thisTask.subscribe(a => f(a).subscribe(onComplete))
	}

	// ==================== ASYNCHRONOUS RESULT HIERARCHY ====================

	/** Root covariant facade of a memoized/caching result of an already started task. */
	trait Settling[+A] extends Observable[A] { thisSettling =>
		def maybeValue: Maybe[A]

		inline def isCompleted: Boolean = maybeValue.isDefined

		inline def isPending: Boolean = maybeValue.isEmpty

		override def subscribe(consumer: Consumer[A]): Unit = subscribe((a, _, _) => consumer(a), null, -1, -1)

		def subscribe(consumer: MatrixConsumer[A], key: Key, upChain: Int, downChain: Int): Unit

		def unsubscribe(key: Key): Unit

		def unsubscribe(consumer: Consumer[A]): Unit

		def isSubscribed(key: Key): Boolean

		def isSubscribed(consumer: Consumer[A]): Boolean
	}

	/** Exception-unaware single result capturer. Ex LatchingTask
	 * Does not inherit from Task, cleanly separating results from doable work. */
	sealed trait Capturer[+A] extends Settling[A] { thisLatch =>
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
		override def maybeValue: Maybe[A] = Maybe(value)

		override def subscribe(consumer: Consumer[A]): Unit = consumer(value)

		override def subscribe(consumer: MatrixConsumer[A], key: Key, upChain: Int, downChain: Int): Unit = consumer(value, upChain, downChain)

		override def unsubscribe(consumer: Consumer[A]): Unit = ()

		override def unsubscribe(key: Key): Unit = ()

		override def isSubscribed(consumer: Consumer[A]): Boolean = false

		override def isSubscribed(key: Key): Boolean = false

		override def map[B](f: A => B): Capturer[B] = new Keeper(f(value))

		override def flatMap[B](f: A => Observable[B]): Observable[B] = f(value)

		@targetName("flatMapCapturer")
		override def flatMap[B](f: A => Capturer[B]): Capturer[B] = f(value)
	}

	/** Ex Covenant. */
	class Captor[A](initialState: Maybe[A] = Maybe.empty) extends Capturer[A] {
		private var state: Maybe[A] = initialState
		private var subscribers: List[Consumer[A] | (Key, MatrixConsumer[A], Int, Int)] = Nil

		override def maybeValue: Maybe[A] = state

		override def subscribe(consumer: Consumer[A]): Unit = {
			state.fold {
				subscribers = consumer :: subscribers
			} { a =>
				consumer(a)
			}
		}

		override def subscribe(consumer: MatrixConsumer[A], key: Key, upChain: Int, downChain: Int): Unit = {
			state.fold {
				if key != null then unsubscribe(key)
				subscribers = (key, consumer, upChain, downChain) :: subscribers
			} { a =>
				consumer(a, upChain, downChain)
			}
		}

		override def unsubscribe(key: Key): Unit = {
			if key != null then
				subscribers = subscribers.filterNot {
					case (k: Key, _, _, _) => k == key
					case _ => false
				}
		}

		override def unsubscribe(consumer: Consumer[A]): Unit = {
			subscribers = subscribers.filterNot {
				case c: Consumer[A] @unchecked => c eq consumer
				case _ => false
			}
		}

		override def isSubscribed(key: Key): Boolean = {
			key != null && subscribers.exists {
				case (k: Key, _, _, _) => k == key
				case _ => false
			}
		}

		override def isSubscribed(consumer: Consumer[A]): Boolean = {
			subscribers.exists {
				case c: Consumer[A] @unchecked => c eq consumer
				case _ => false
			}
		}

		override def map[B](f: A => B): Capturer[B] = {
			state.fold {
				val cov = new Captor[B]()
				this.subscribe(a => cov.capture(f(a)))
				cov
			} { a =>
				new Keeper(f(a))
			}
		}

		override def flatMap[B](f: A => Observable[B]): Observable[B] = {
			state.fold {
				val captor = new Captor[B]()
				this.subscribe(a => f(a).subscribe(b => captor.capture(b)))
				captor
			}(f)
		}

		@targetName("flatMapCapturer")
		override def flatMap[B](f: A => Capturer[B]): Capturer[B] = {
			state.fold {
				val captor = new Captor[B]()
				this.subscribe(a => f(a).subscribe(b => captor.capture(b)))
				captor
			}(f)
		}

		def capture(result: A): Unit = {
			state.fold {
				state = Maybe(result)
				val currentSubscribers = subscribers
				subscribers = Nil
				currentSubscribers.reverse.foreach {
					case consumer: Consumer[A] @unchecked =>
						consumer(result)
					case (_, consumer, upChain, downChain) =>
						consumer(result, upChain, downChain)
				}
			}(_ => ())
		}
	}

	/////////////// Trial hierarchies ///////////////

	/** A lazy exception-aware computation.
	 * Monadic combinators map/flatMap over the success value of the Try. */
	trait Venture[+A] extends Task[Try[A]] { thisVenture =>

		def transform[B](f: Try[A] => Try[B]): Venture[B] =
			(onComplete: Consumer[Try[B]]) => thisVenture.subscribe { tryA =>
				val tryB =
					try f(tryA)
					catch {
						case NonFatal(e) => Failure(e)
					}
				onComplete(tryB)
			}

		def transformWith[B](f: Try[A] => Venture[B]): Venture[B] =
			(onComplete: Consumer[Try[B]]) => thisVenture.subscribe { tryA =>
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
			(onComplete: Consumer[Try[B]]) => thisVenture.subscribe {
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
			(onComplete: Consumer[Try[B]]) => {
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
			(onComplete: Consumer[Try[B]]) => thisVenture.subscribe {
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
		(onComplete: Consumer[Try[A]]) => onComplete(tryA)


	/** Exception-aware latching result (caches a Try[A]).
	 * Monadic combinators map/flatMap over the success value of the Try.
	 */
	sealed trait TrialCapturer[+A] extends Settling[Try[A]] { thisTrialCapturer =>

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

		@targetName("mapSuccess") // Differentiates from Settling.map(Try[A] => B) to prevent JVM signature clashes on return-type alignment and enable Java interop
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
		override def maybeValue: Maybe[Try[A]] = Maybe(value)

		override def subscribe(consumer: Consumer[Try[A]]): Unit = consumer(value)

		override def unsubscribe(key: Key): Unit = ()

		override def unsubscribe(consumer: Consumer[Try[A]]): Unit = ()

		override def isSubscribed(key: Key): Boolean = false

		override def isSubscribed(consumer: Consumer[Try[A]]): Boolean = false

		override def subscribe(consumer: MatrixConsumer[Try[A]], key: Key, upChain: Int, downChain: Int): Unit = consumer(value, upChain, downChain)

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

	class TrialCaptor[A](initialState: Maybe[Try[A]] = Maybe.empty) extends TrialCapturer[A] {
		private var state: Maybe[Try[A]] = initialState
		private var subscribers: List[Consumer[Try[A]] | (Key, MatrixConsumer[Try[A]], Int, Int)] = Nil

		override def maybeValue: Maybe[Try[A]] = state

		override def subscribe(consumer: Consumer[Try[A]]): Unit = {
			state.fold {
				subscribers = consumer :: subscribers
			} { tryA =>
				consumer(tryA)
			}
		}

		override def subscribe(consumer: MatrixConsumer[Try[A]], key: Key, upChain: Int, downChain: Int): Unit = {
			state.fold {
				if key != null then unsubscribe(key)
				subscribers = (key, consumer, upChain, downChain) :: subscribers
			} { tryA =>
				consumer(tryA, upChain, downChain)
			}
		}

		override def unsubscribe(key: Key): Unit = {
			if key != null then
				subscribers = subscribers.filterNot {
					case (k: Key, _, _, _) => k == key
					case _ => false
				}
		}

		override def unsubscribe(consumer: Consumer[Try[A]]): Unit = {
			subscribers = subscribers.filterNot {
				case c: Consumer[Try[A]] @unchecked => c eq consumer
				case _ => false
			}
		}

		override def isSubscribed(key: Key): Boolean = {
			key != null && subscribers.exists {
				case (k: Key, _, _, _) => k == key
				case _ => false
			}
		}

		override def isSubscribed(consumer: Consumer[Try[A]]): Boolean = {
			subscribers.exists {
				case c: Consumer[Try[A]] @unchecked => c eq consumer
				case _ => false
			}
		}

		override def map[B](f: Try[A] => B): Capturer[B] = {
			state.fold {
				val captor = new Captor[B]()
				this.subscribe(tryA => captor.capture(f(tryA)))
				captor
			} { tryA =>
				new Keeper(f(tryA))
			}
		}

		override def flatMap[B](f: Try[A] => Observable[B]): Observable[B] = {
			state.fold {
				val captor = new Captor[B]()
				this.subscribe(tryA => f(tryA).subscribe(b => captor.capture(b)))
				captor
			}(f)
		}

		override def flatMap[B](f: Try[A] => Capturer[B]): Capturer[B] = {
			state.fold {
				val captor = new Captor[B]()
				this.subscribe(tryA => f(tryA).subscribe(b => captor.capture(b)))
				captor
			}(f)
		}

		@targetName("mapSuccess")
		override def map[B](f: A => B): TrialCapturer[B] = {
			state.fold {
				val captor = new TrialCaptor[B]()
				this.subscribe {
					case Success(a) =>
						val tryB =
							try Success(f(a))
							catch {
								case NonFatal(e) => Failure(e)
							}
						captor.capture(tryB)
					case failure: Failure[A] =>
						captor.capture(failure.asInstanceOf[Failure[B]])
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
			state.fold {
				val captor = new TrialCaptor[B]()
				this.subscribe {
					case Success(a) =>
						val maybeCapturer =
							try Maybe(f(a))
							catch {
								case NonFatal(e) =>
									captor.capture(Failure(e))
									Maybe.empty
							}
						maybeCapturer.foreach(_.subscribe(tryB => captor.capture(tryB)))
					case failure: Failure[A] =>
						captor.capture(failure.asInstanceOf[Failure[B]])
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
			state.fold {
				val captor = new TrialCaptor[B]()
				this.subscribe {
					case Success(a) =>
						val maybeObservable =
							try Maybe(f(a))
							catch {
								case NonFatal(e) =>
									captor.capture(Failure(e))
									Maybe.empty
							}
						maybeObservable.foreach(_.subscribe(captor.capture))
					case failure: Failure[A] =>
						captor.capture(failure.asInstanceOf[Failure[B]])
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

		def capture(result: Try[A]): Unit = {
			state.fold {
				state = Maybe(result)
				val currentSubscribers = subscribers
				subscribers = Nil
				currentSubscribers.reverse.foreach {
					case consumer: Consumer[Try[A]] @unchecked =>
						consumer(result)
					case (_, consumer: MatrixConsumer[Try[A]] @unchecked, upChain: Int, downChain: Int) =>
						consumer(result, upChain, downChain)
				}
			}(_ => ())
		}
	}

	// ===================================================================
	// ==================== CHAIN SUPPORT PRIMITIVES =====================
	// ===================================================================

	///////////////////////////////////////
	//////////// PUSH BASED ///////////////
	///////////////////////////////////////

	trait ObservableArray[+A] {
		def subscribe(
			onNext: MatrixConsumer[A],
			onError: Throwable => Unit = _ => (),
			onComplete: () => Unit = () => ()
		): Unit

		def subscribe(
			onNext: ArrayConsumer[A],
			onError: Throwable => Unit,
			onComplete: () => Unit
		): Unit = subscribe((a, _, downChain) => onNext(a, downChain), onError, onComplete)

		def subscribe(onNext: ArrayConsumer[A]): Unit = subscribe(onNext, _ => (), () => ())

		def subscribe(
			onNext: Consumer[A],
			onError: Throwable => Unit,
			onComplete: () => Unit
		): Unit = subscribe((a, _, _) => onNext(a), onError, onComplete)

		def subscribe(onNext: Consumer[A]): Unit = subscribe(onNext, _ => (), () => ())

		def foreach(consumer: Consumer[A]): Unit = subscribe(consumer)

		inline def foreachWithIndex(consumer: ArrayConsumer[A]): Unit = subscribe(consumer)

		def map[B: ClassTag](f: A => B): ObservableArray[B]

		def mapWithIndex[B: ClassTag](f: (A, Int) => B): ObservableArray[B]

		def flatMap[B: ClassTag](f: A => ObservableArray[B]): ObservableMatrix[B]

		def flatMapWithIndex[B: ClassTag](f: (A, Int) => ObservableArray[B]): ObservableMatrix[B]

		def scan[B: ClassTag](initial: B)(f: (B, A) => B): ObservableArray[B]

		def buffer[T >: A : ClassTag](size: Int): ObservableArray[IArray[T]]

		def zip[B, C: ClassTag](other: ObservableArray[B])(f: (A, B) => C): ObservableArray[C]

		def take(n: Int): ObservableArray[A]

		def takeWhile(p: A => Boolean): ObservableArray[A]
	}

	/** Partial implementation of [[ObservableArray]] */
	trait DefaultObservableArray[+A] extends ObservableArray[A] {
		override def map[B: ClassTag](f: A => B): ObservableArray[B] =
			new DefaultObservableArray[B] {
				override def subscribe(
					onNext: MatrixConsumer[B],
					onError: Throwable => Unit,
					onComplete: () => Unit
				): Unit = DefaultObservableArray.this.subscribe(
					(a, upChain, downChain) => onNext(f(a), upChain, downChain),
					onError,
					onComplete
				)
			}

		override def mapWithIndex[B: ClassTag](f: (A, Int) => B): ObservableArray[B] =
			new DefaultObservableArray[B] {
				override def subscribe(
					onNext: MatrixConsumer[B],
					onError: Throwable => Unit,
					onComplete: () => Unit
				): Unit = DefaultObservableArray.this.subscribe(
					(a, upChain, downChain) => onNext(f(a, downChain), upChain, downChain),
					onError,
					onComplete
				)
			}

		override def flatMap[B: ClassTag](f: A => ObservableArray[B]): ObservableMatrix[B] =
			new DefaultObservableMatrix[B] {
				override def subscribe(
					onNext: MatrixConsumer[B],
					onError: Throwable => Unit,
					onComplete: () => Unit
				): Unit = arrayFlatMapSubscribe(DefaultObservableArray.this, f, onNext, onError, onComplete)
			}

		override def flatMapWithIndex[B: ClassTag](f: (A, Int) => ObservableArray[B]): ObservableMatrix[B] =
			new DefaultObservableMatrix[B] {
				override def subscribe(
					onNext: MatrixConsumer[B],
					onError: Throwable => Unit,
					onComplete: () => Unit
				): Unit = arrayFlatMapWithIndexSubscribe(DefaultObservableArray.this, f, onNext, onError, onComplete)
			}

		override def scan[B: ClassTag](initial: B)(f: (B, A) => B): ObservableArray[B] =
			new DefaultObservableArray[B] {
				override def subscribe(
					onNext: MatrixConsumer[B],
					onError: Throwable => Unit,
					onComplete: () => Unit
				): Unit = {
					DefaultObservableArray.this.subscribe(
						new MatrixConsumer[A] {
							private var state = initial
							def apply(a: A, upChain: Int, downChain: Int): Unit = {
								state = f(state, a)
								onNext(state, upChain, downChain)
							}
						},
						onError,
						onComplete
					)
				}
			}

		override def buffer[T >: A : ClassTag](size: Int): ObservableArray[IArray[T]] =
			new DefaultObservableArray[IArray[T]] {
				override def subscribe(
					onNext: MatrixConsumer[IArray[T]],
					onError: Throwable => Unit,
					onComplete: () => Unit
				): Unit = {
					val buf = new Array[T](size)
					class BufferedConsumer extends MatrixConsumer[A] {
						private var count = 0
						private var chunkIndex = 0
						def apply(a: A, up: Int, down: Int): Unit = {
							buf(count) = a
							count += 1
							if (count == size) {
								val chunk = IArray.unsafeFromArray(buf.clone())
								count = 0
								val idx = chunkIndex
								chunkIndex += 1
								onNext(chunk, up, idx)
							}
						}
						def flush(): Unit = {
							if (count > 0) {
								val partial = IArray.unsafeFromArray(buf.take(count))
								val idx = chunkIndex
								chunkIndex += 1
								onNext(partial, 0, idx)
							}
						}
					}
					val consumer = new BufferedConsumer()
					DefaultObservableArray.this.subscribe(
						consumer,
						onError,
						onComplete = () => {
							consumer.flush()
							onComplete()
						}
					)
				}
			}

		override def zip[B, C: ClassTag](other: ObservableArray[B])(f: (A, B) => C): ObservableArray[C] =
			new ZippedObservableArray(this, other, f)

		override def take(n: Int): ObservableArray[A] =
			new DefaultObservableArray[A] {
				override def subscribe(
					onNext: MatrixConsumer[A],
					onError: Throwable => Unit,
					onComplete: () => Unit
				): Unit = {
					DefaultObservableArray.this.subscribe(
						new MatrixConsumer[A] {
							private var count = 0
							private var active = true
							def apply(a: A, up: Int, down: Int): Unit = {
								if (active) {
									if (count < n) {
										val currentCount = count
										count += 1
										onNext(a, up, currentCount)
										if (count == n) {
											active = false
											onComplete()
										}
									}
								}
							}
						},
						onError,
						onComplete
					)
				}
			}

		override def takeWhile(p: A => Boolean): ObservableArray[A] =
			new DefaultObservableArray[A] {
				override def subscribe(
					onNext: MatrixConsumer[A],
					onError: Throwable => Unit,
					onComplete: () => Unit
				): Unit = {
					DefaultObservableArray.this.subscribe(
						new MatrixConsumer[A] {
							private var active = true
							private var counter = 0
							def apply(a: A, up: Int, down: Int): Unit = {
								if (active) {
									if (p(a)) {
										val currentCounter = counter
										counter += 1
										onNext(a, up, currentCounter)
									} else {
										active = false
										onComplete()
									}
								}
							}
						},
						onError,
						onComplete
					)
				}
			}
	}

	trait ObservableMatrix[+A] {
		def subscribe(
			onNext: MatrixConsumer[A],
			onError: Throwable => Unit = _ => (),
			onComplete: () => Unit = () => ()
		): Unit

		def flattenToInner: ObservableArray[A]

		def flattenToOuter: ObservableArray[A]

		def flattenWith(f: (A, Int, Int) => Int): ObservableArray[A]

		def flattenToSequential: ObservableArray[A]

		def flattenMap[B: ClassTag](f: (A, Int, Int) => (B, Int)): ObservableArray[B]

		def flattenMap[B: ClassTag](valueMap: (A, Int, Int) => B, indexMap: (A, B, Int, Int) => Int): ObservableArray[B]

		def flattenFold[B: ClassTag, S](initialState: S)(f: (S, A, Int, Int) => (S, B, Int)): ObservableArray[B]
	}

	trait DefaultObservableMatrix[+A] extends ObservableMatrix[A] {
		override def flattenToInner: ObservableArray[A] =
			new DefaultObservableArray[A] {
				override def subscribe(
					onNext: MatrixConsumer[A],
					onError: Throwable => Unit,
					onComplete: () => Unit
				): Unit =
					DefaultObservableMatrix.this.subscribe((a, upChain, downChain) => onNext(a, 0, downChain), onError, onComplete)
			}

		override def flattenToOuter: ObservableArray[A] =
			new DefaultObservableArray[A] {
				override def subscribe(
					onNext: MatrixConsumer[A],
					onError: Throwable => Unit,
					onComplete: () => Unit
				): Unit =
					DefaultObservableMatrix.this.subscribe((a, upChain, downChain) => onNext(a, 0, upChain), onError, onComplete)
			}

		override def flattenWith(f: (A, Int, Int) => Int): ObservableArray[A] =
			new DefaultObservableArray[A] {
				override def subscribe(
					onNext: MatrixConsumer[A],
					onError: Throwable => Unit,
					onComplete: () => Unit
				): Unit =
					DefaultObservableMatrix.this.subscribe((a, upChain, downChain) => onNext(a, 0, f(a, upChain, downChain)), onError, onComplete)
			}

		override def flattenToSequential: ObservableArray[A] =
			new DefaultObservableArray[A] {
				override def subscribe(
					onNext: MatrixConsumer[A],
					onError: Throwable => Unit,
					onComplete: () => Unit
				): Unit =
					flattenToSequentialSubscribe(DefaultObservableMatrix.this, onNext, onError, onComplete)
			}

		override def flattenMap[B: ClassTag](f: (A, Int, Int) => (B, Int)): ObservableArray[B] =
			new DefaultObservableArray[B] {
				override def subscribe(
					onNext: MatrixConsumer[B],
					onError: Throwable => Unit,
					onComplete: () => Unit
				): Unit =
					flattenMapSubscribe(DefaultObservableMatrix.this, f, onNext, onError, onComplete)
			}

		override def flattenMap[B: ClassTag](valueMap: (A, Int, Int) => B, indexMap: (A, B, Int, Int) => Int): ObservableArray[B] =
			new DefaultObservableArray[B] {
				override def subscribe(
					onNext: MatrixConsumer[B],
					onError: Throwable => Unit,
					onComplete: () => Unit
				): Unit =
					flattenMapWithIndexSubscribe(DefaultObservableMatrix.this, valueMap, indexMap, onNext, onError, onComplete)
			}

		override def flattenFold[B: ClassTag, S](initialState: S)(f: (S, A, Int, Int) => (S, B, Int)): ObservableArray[B] =
			new DefaultObservableArray[B] {
				override def subscribe(
					onNext: MatrixConsumer[B],
					onError: Throwable => Unit,
					onComplete: () => Unit
				): Unit =
					flattenFoldSubscribe(DefaultObservableMatrix.this, initialState, f, onNext, onError, onComplete)
			}
	}


	trait TaskArray[+A] extends ObservableArray[A] {
		override def map[B: ClassTag](f: A => B): TaskArray[B]

		override def mapWithIndex[B: ClassTag](f: (A, Int) => B): TaskArray[B]

		override def flatMap[B: ClassTag](f: A => ObservableArray[B]): ObservableMatrix[B]

		override def flatMapWithIndex[B: ClassTag](f: (A, Int) => ObservableArray[B]): ObservableMatrix[B]

		@targetName("flatMapTask")
		def flatMap[B: ClassTag](f: A => TaskArray[B]): TaskMatrix[B]

		@targetName("flatMapTaskWithIndex")
		def flatMapWithIndex[B: ClassTag](f: (A, Int) => TaskArray[B]): TaskMatrix[B]

		override def scan[B: ClassTag](initial: B)(f: (B, A) => B): TaskArray[B]

		override def buffer[T >: A : ClassTag](size: Int): TaskArray[IArray[T]]

		override def zip[B, C: ClassTag](other: ObservableArray[B])(f: (A, B) => C): ObservableArray[C]

		@targetName("zipTask")
		def zip[B, C: ClassTag](other: TaskArray[B])(f: (A, B) => C): TaskArray[C]

		override def take(n: Int): TaskArray[A]

		override def takeWhile(p: A => Boolean): TaskArray[A]
	}

	/** Partial implementation of [[TaskArray]] */
	trait DefaultTaskArray[+A] extends TaskArray[A] {
		override def map[B: ClassTag](f: A => B): TaskArray[B] =
			new DefaultTaskArray[B] {
				override def subscribe(
					onNext: MatrixConsumer[B],
					onError: Throwable => Unit,
					onComplete: () => Unit
				): Unit = DefaultTaskArray.this.subscribe(
					(a, upChain, downChain) => onNext(f(a), upChain, downChain),
					onError,
					onComplete
				)
			}

		override def mapWithIndex[B: ClassTag](f: (A, Int) => B): TaskArray[B] =
			new DefaultTaskArray[B] {
				override def subscribe(
					onNext: MatrixConsumer[B],
					onError: Throwable => Unit,
					onComplete: () => Unit
				): Unit = DefaultTaskArray.this.subscribe(
					(a, upChain, downChain) => onNext(f(a, downChain), upChain, downChain),
					onError,
					onComplete
				)
			}

		override def flatMap[B: ClassTag](f: A => ObservableArray[B]): ObservableMatrix[B] =
			new DefaultObservableMatrix[B] {
				override def subscribe(
					onNext: MatrixConsumer[B],
					onError: Throwable => Unit,
					onComplete: () => Unit
				): Unit = arrayFlatMapSubscribe(DefaultTaskArray.this, f, onNext, onError, onComplete)
			}

		override def flatMapWithIndex[B: ClassTag](f: (A, Int) => ObservableArray[B]): ObservableMatrix[B] =
			new DefaultObservableMatrix[B] {
				override def subscribe(
					onNext: MatrixConsumer[B],
					onError: Throwable => Unit,
					onComplete: () => Unit
				): Unit = arrayFlatMapWithIndexSubscribe(DefaultTaskArray.this, f, onNext, onError, onComplete)
			}

		@targetName("flatMapTask")
		override def flatMap[B: ClassTag](f: A => TaskArray[B]): TaskMatrix[B] =
			new DefaultTaskMatrix[B] {
				override def subscribe(
					onNext: MatrixConsumer[B],
					onError: Throwable => Unit,
					onComplete: () => Unit
				): Unit = arrayFlatMapSubscribe(DefaultTaskArray.this, f, onNext, onError, onComplete)
			}

		@targetName("flatMapTaskWithIndex")
		override def flatMapWithIndex[B: ClassTag](f: (A, Int) => TaskArray[B]): TaskMatrix[B] =
			new DefaultTaskMatrix[B] {
				override def subscribe(
					onNext: MatrixConsumer[B],
					onError: Throwable => Unit,
					onComplete: () => Unit
				): Unit = arrayFlatMapWithIndexSubscribe(DefaultTaskArray.this, f, onNext, onError, onComplete)
			}

		override def scan[B: ClassTag](initial: B)(f: (B, A) => B): TaskArray[B] =
			new DefaultTaskArray[B] {
				override def subscribe(
					onNext: MatrixConsumer[B],
					onError: Throwable => Unit,
					onComplete: () => Unit
				): Unit = {
					DefaultTaskArray.this.subscribe(
						new MatrixConsumer[A] {
							private var state = initial
							def apply(a: A, upChain: Int, downChain: Int): Unit = {
								state = f(state, a)
								onNext(state, upChain, downChain)
							}
						},
						onError,
						onComplete
					)
				}
			}

		override def buffer[T >: A : ClassTag](size: Int): TaskArray[IArray[T]] =
			new DefaultTaskArray[IArray[T]] {
				override def subscribe(
					onNext: MatrixConsumer[IArray[T]],
					onError: Throwable => Unit,
					onComplete: () => Unit
				): Unit = {
					val buf = new Array[T](size)
					class BufferedConsumer extends MatrixConsumer[A] {
						private var count = 0
						private var chunkIndex = 0
						def apply(a: A, up: Int, down: Int): Unit = {
							buf(count) = a
							count += 1
							if (count == size) {
								val chunk = IArray.unsafeFromArray(buf.clone())
								count = 0
								val idx = chunkIndex
								chunkIndex += 1
								onNext(chunk, up, idx)
							}
						}
						def flush(): Unit = {
							if (count > 0) {
								val partial = IArray.unsafeFromArray(buf.take(count))
								val idx = chunkIndex
								chunkIndex += 1
								onNext(partial, 0, idx)
							}
						}
					}
					val consumer = new BufferedConsumer()
					DefaultTaskArray.this.subscribe(
						consumer,
						onError,
						onComplete = () => {
							consumer.flush()
							onComplete()
						}
					)
				}
			}

		override def zip[B, C: ClassTag](other: ObservableArray[B])(f: (A, B) => C): ObservableArray[C] =
			new ZippedObservableArray(this, other, f)

		@targetName("zipTask")
		override def zip[B, C: ClassTag](other: TaskArray[B])(f: (A, B) => C): TaskArray[C] =
			new ZippedTaskArray(this, other, f)

		override def take(n: Int): TaskArray[A] =
			new DefaultTaskArray[A] {
				override def subscribe(
					onNext: MatrixConsumer[A],
					onError: Throwable => Unit,
					onComplete: () => Unit
				): Unit = {
					DefaultTaskArray.this.subscribe(
						new MatrixConsumer[A] {
							private var count = 0
							private var active = true
							def apply(a: A, up: Int, down: Int): Unit = {
								if (active) {
									if (count < n) {
										val currentCount = count
										count += 1
										onNext(a, up, currentCount)
										if (count == n) {
											active = false
											onComplete()
										}
									}
								}
							}
						},
						onError,
						onComplete
					)
				}
			}

		override def takeWhile(p: A => Boolean): TaskArray[A] =
			new DefaultTaskArray[A] {
				override def subscribe(
					onNext: MatrixConsumer[A],
					onError: Throwable => Unit,
					onComplete: () => Unit
				): Unit = {
					DefaultTaskArray.this.subscribe(
						new MatrixConsumer[A] {
							private var active = true
							private var counter = 0
							def apply(a: A, up: Int, down: Int): Unit = {
								if (active) {
									if (p(a)) {
										val currentCounter = counter
										counter += 1
										onNext(a, up, currentCounter)
									} else {
										active = false
										onComplete()
									}
								}
							}
						},
						onError,
						onComplete
					)
				}
			}
	}

	trait TaskMatrix[+A] extends ObservableMatrix[A] {
		override def flattenToInner: TaskArray[A]

		override def flattenToOuter: TaskArray[A]

		override def flattenWith(f: (A, Int, Int) => Int): TaskArray[A]

		override def flattenToSequential: TaskArray[A]

		override def flattenMap[B: ClassTag](f: (A, Int, Int) => (B, Int)): TaskArray[B]

		override def flattenMap[B: ClassTag](valueMap: (A, Int, Int) => B, indexMap: (A, B, Int, Int) => Int): TaskArray[B]

		override def flattenFold[B: ClassTag, S](initialState: S)(f: (S, A, Int, Int) => (S, B, Int)): TaskArray[B]
	}

	trait DefaultTaskMatrix[+A] extends TaskMatrix[A] {
		override def flattenToInner: TaskArray[A] =
			new DefaultTaskArray[A] {
				override def subscribe(
					onNext: MatrixConsumer[A],
					onError: Throwable => Unit,
					onComplete: () => Unit
				): Unit =
					DefaultTaskMatrix.this.subscribe((a, o, i) => onNext(a, 0, i), onError, onComplete)
			}

		override def flattenToOuter: TaskArray[A] =
			new DefaultTaskArray[A] {
				override def subscribe(
					onNext: MatrixConsumer[A],
					onError: Throwable => Unit,
					onComplete: () => Unit
				): Unit =
					DefaultTaskMatrix.this.subscribe((a, o, i) => onNext(a, 0, o), onError, onComplete)
			}

		override def flattenWith(f: (A, Int, Int) => Int): TaskArray[A] =
			new DefaultTaskArray[A] {
				override def subscribe(
					onNext: MatrixConsumer[A],
					onError: Throwable => Unit,
					onComplete: () => Unit
				): Unit =
					DefaultTaskMatrix.this.subscribe((a, o, i) => onNext(a, 0, f(a, o, i)), onError, onComplete)
			}

		override def flattenToSequential: TaskArray[A] =
			new DefaultTaskArray[A] {
				override def subscribe(
					onNext: MatrixConsumer[A],
					onError: Throwable => Unit,
					onComplete: () => Unit
				): Unit =
					flattenToSequentialSubscribe(DefaultTaskMatrix.this, onNext, onError, onComplete)
			}

		override def flattenMap[B: ClassTag](f: (A, Int, Int) => (B, Int)): TaskArray[B] =
			new DefaultTaskArray[B] {
				override def subscribe(
					onNext: MatrixConsumer[B],
					onError: Throwable => Unit,
					onComplete: () => Unit
				): Unit =
					flattenMapSubscribe(DefaultTaskMatrix.this, f, onNext, onError, onComplete)
			}

		override def flattenMap[B: ClassTag](valueMap: (A, Int, Int) => B, indexMap: (A, B, Int, Int) => Int): TaskArray[B] =
			new DefaultTaskArray[B] {
				override def subscribe(
					onNext: MatrixConsumer[B],
					onError: Throwable => Unit,
					onComplete: () => Unit
				): Unit =
					flattenMapWithIndexSubscribe(DefaultTaskMatrix.this, valueMap, indexMap, onNext, onError, onComplete)
			}

		override def flattenFold[B: ClassTag, S](initialState: S)(f: (S, A, Int, Int) => (S, B, Int)): TaskArray[B] =
			new DefaultTaskArray[B] {
				override def subscribe(
					onNext: MatrixConsumer[B],
					onError: Throwable => Unit,
					onComplete: () => Unit
				): Unit =
					flattenFoldSubscribe(DefaultTaskMatrix.this, initialState, f, onNext, onError, onComplete)
			}
	}


	/** Note: This implements the following user requirements:
	 * 2) A factory method that receives a collection of Task[A] instances and returns something that allows to subscribe for all the results as they complete. Each subscription fires a dedicated execution of each Task[A] instance, therefore, different subscribers may receive different results, depending on the Task nature (pure or context dependent).
	 * 3) A method similar to [[Task.subscribe]] in which the passed consumer is fed multiple times: one per each of the provided [[Task]]s along its index. */
	def TaskArray_fromTasks[A](tasks: IArray[Task[A]]): TaskArray[A] =
		new TaskArray_FromTasks(tasks)

	private final class TaskArray_FromTasks[+A](tasks: IArray[Task[A]]) extends DefaultTaskArray[A] {
		override def subscribe(
			onNext: MatrixConsumer[A],
			onError: Throwable => Unit,
			onComplete: () => Unit
		): Unit = {
			val size = tasks.length
			if (size == 0) {
				onComplete()
			} else {
				var completedCount = 0
				tasks.foreachWithIndex { (task, index) =>
					task.subscribe { a =>
						onNext(a, 0, index)
						val done = TaskArray_FromTasks.this.synchronized {
							completedCount += 1
							completedCount == size
						}
						if (done) onComplete()
					}
				}
			}
		}
	}

	trait KeyedObservableArray[+A] extends DefaultObservableArray[A] {
		override def subscribe(
			onNext: MatrixConsumer[A],
			onError: Throwable => Unit = _ => (),
			onComplete: () => Unit = () => ()
		): Unit = subscribe(onNext, onError, onComplete, null)

		def subscribe(
			onNext: MatrixConsumer[A],
			onError: Throwable => Unit,
			onComplete: () => Unit,
			key: Key
		): Unit

		def subscribe(onNext: ArrayConsumer[A], key: Key): Unit = subscribe((a, _, inner) => onNext(a, inner), _ => (), () => (), key)

		def subscribe(onNext: Consumer[A], key: Key): Unit = subscribe((a, _, _) => onNext(a), _ => (), () => (), key)

		def unsubscribe(key: Key): Unit

		def isSubscribed(key: Key): Boolean
	}

	trait SettlingArray[+A] extends KeyedObservableArray[A] {
		def maybeResult(index: Int): Maybe[A]

		inline def isCompleted(index: Int): Boolean = maybeResult(index).isDefined

		inline def isPending(index: Int): Boolean = maybeResult(index).isEmpty
	}

	trait KeyedCapturerArray[+A] extends KeyedObservableArray[A] {
		override def map[B: ClassTag](f: A => B): KeyedCapturerArray[B] =
			new MappedKeyedCapturerArray(this, (a, index) => f(a))

		override def mapWithIndex[B: ClassTag](f: (A, Int) => B): KeyedCapturerArray[B] =
			new MappedKeyedCapturerArray(this, f)
	}

	final class MappedKeyedCapturerArray[A, +B](
		val source: KeyedCapturerArray[A],
		val f: (A, Int) => B
	) extends KeyedCapturerArray[B] {
		override def subscribe(
			onNext: MatrixConsumer[B],
			onError: Throwable => Unit,
			onComplete: () => Unit,
			key: Key
		): Unit =
			source.subscribe((a, outer, inner) => onNext(f(a, inner), outer, inner), onError, onComplete, key)

		override def unsubscribe(key: Key): Unit = source.unsubscribe(key)

		override def isSubscribed(key: Key): Boolean = source.isSubscribed(key)
	}

	sealed trait CapturerArray[+A] extends SettlingArray[A] with KeyedCapturerArray[A] {
		override def map[B: ClassTag](f: A => B): CapturerArray[B] = this match {
			case keeper: KeeperArray[A] => keeper.map(f)
			case captor: CaptorArray[A] @unchecked => captor.map(f)
		}

		override def mapWithIndex[B: ClassTag](f: (A, Int) => B): CapturerArray[B] = this match {
			case keeper: KeeperArray[A] => keeper.mapWithIndex(f)
			case captor: CaptorArray[A] @unchecked => captor.mapWithIndex(f)
		}

		@targetName("flatMapCapturer")
		def flatMap[B: ClassTag](f: A => CapturerArray[B]): CapturerMatrix[B]

		@targetName("flatMapCapturerWithIndex")
		def flatMapWithIndex[B: ClassTag](f: (A, Int) => CapturerArray[B]): CapturerMatrix[B]
	}

	final class KeeperArray[+A](values: IArray[A]) extends CapturerArray[A] {
		override def maybeResult(index: Int): Maybe[A] = Maybe(values(index))

		override def subscribe(
			onNext: MatrixConsumer[A],
			onError: Throwable => Unit,
			onComplete: () => Unit,
			key: Key
		): Unit = {
			values.foreachWithIndex((a, index) => onNext(a, 0, index))
			onComplete()
		}

		override def unsubscribe(key: Key): Unit = ()

		override def isSubscribed(key: Key): Boolean = false

		override def map[B: ClassTag](f: A => B): KeeperArray[B] = new KeeperArray[B](values.mapWithIndex { (a, i) => f(a) })

		override def mapWithIndex[B: ClassTag](f: (A, Int) => B): KeeperArray[B] = new KeeperArray[B](values.mapWithIndex(f))

		override def flatMap[B: ClassTag](f: A => ObservableArray[B]): ObservableMatrix[B] =
			new DefaultObservableMatrix[B] {
				override def subscribe(
					onNext: MatrixConsumer[B],
					onError: Throwable => Unit,
					onComplete: () => Unit
				): Unit = {
					val size = values.length
					if (size == 0) {
						onComplete()
					} else {
						var completedCount = 0
						var errorFired = false
						def tryComplete(): Unit = {
							if (completedCount == size && !errorFired) onComplete()
						}
						def handleFailure(ex: Throwable): Unit = {
							if (!errorFired) {
								errorFired = true
								onError(ex)
							}
						}
						values.foreachWithIndex { (a, outerIndex) =>
							f(a).subscribe(
								(b, innerOuter, innerInner) => onNext(b, outerIndex, innerInner),
								handleFailure,
								() => {
									completedCount += 1
									tryComplete()
								}
							)
						}
					}
				}
			}

		@targetName("flatMapCapturer")
		override def flatMap[B: ClassTag](f: A => CapturerArray[B]): CapturerMatrix[B] =
			new FlatMappedCapturerMatrix(this, f)

		override def flatMapWithIndex[B: ClassTag](f: (A, Int) => ObservableArray[B]): ObservableMatrix[B] =
			new DefaultObservableMatrix[B] {
				override def subscribe(
					onNext: MatrixConsumer[B],
					onError: Throwable => Unit,
					onComplete: () => Unit
				): Unit = {
					val size = values.length
					if (size == 0) {
						onComplete()
					} else {
						var completedCount = 0
						var errorFired = false
						def tryComplete(): Unit = {
							if (completedCount == size && !errorFired) onComplete()
						}
						def handleFailure(ex: Throwable): Unit = {
							if (!errorFired) {
								errorFired = true
								onError(ex)
							}
						}
						values.foreachWithIndex { (a, outerIndex) =>
							f(a, outerIndex).subscribe(
								(b, innerOuter, innerInner) => onNext(b, outerIndex, innerInner),
								handleFailure,
								() => {
									completedCount += 1
									tryComplete()
								}
							)
						}
					}
				}
			}

		@targetName("flatMapCapturerWithIndex")
		override def flatMapWithIndex[B: ClassTag](f: (A, Int) => CapturerArray[B]): CapturerMatrix[B] =
			new FlatMappedWithIndexCapturerMatrix(this, f)
	}

	final class CaptorArray[A](val capturers: IArray[Capturer[A]]) extends CapturerArray[A] { thisCaptorArray =>
		override def maybeResult(index: Int): Maybe[A] = capturers(index).maybeValue

		override def subscribe(
			onNext: MatrixConsumer[A],
			onError: Throwable => Unit,
			onComplete: () => Unit,
			key: Key
		): Unit = {
			if key != null then unsubscribe(key)
			val size = capturers.length
			if (size == 0) {
				onComplete()
			} else {
				var completedCount = 0
				var i = 0
				while i < size do
					val index = i
					capturers(index).subscribe(
						(a, up, down) => {
							onNext(a, 0, index)
							val done = thisCaptorArray.synchronized {
								completedCount += 1
								completedCount == size
							}
							if (done) onComplete()
						},
						key,
						0,
						index
					)
					i += 1
			}
		}

		override def unsubscribe(key: Key): Unit = {
			if key != null then
				var i = 0
				while i < capturers.length do
					capturers(i).unsubscribe(key)
					i += 1
		}

		override def isSubscribed(key: Key): Boolean = {
			key != null && capturers.existsWithIndex((c, _) => c.isSubscribed(key))
		}

		override def map[B: ClassTag](f: A => B): CapturerArray[B] = {
			new CaptorArray(capturers.mapWithIndex((capturer, _) => capturer.map(f)))
		}

		override def mapWithIndex[B: ClassTag](f: (A, Int) => B): CapturerArray[B] = {
			new CaptorArray(capturers.mapWithIndex((capturer, index) => capturer.map(a => f(a, index))))
		}

		override def flatMap[B: ClassTag](f: A => ObservableArray[B]): ObservableMatrix[B] =
			new DefaultObservableMatrix[B] {
				override def subscribe(
					onNext: MatrixConsumer[B],
					onError: Throwable => Unit,
					onComplete: () => Unit
				): Unit =
					arrayFlatMapSubscribe(thisCaptorArray, f, onNext, onError, onComplete)
			}

		override def flatMapWithIndex[B: ClassTag](f: (A, Int) => ObservableArray[B]): ObservableMatrix[B] =
			new DefaultObservableMatrix[B] {
				override def subscribe(
					onNext: MatrixConsumer[B],
					onError: Throwable => Unit,
					onComplete: () => Unit
				): Unit =
					arrayFlatMapWithIndexSubscribe(thisCaptorArray, f, onNext, onError, onComplete)
			}

		@targetName("flatMapCapturer")
		override def flatMap[B: ClassTag](f: A => CapturerArray[B]): CapturerMatrix[B] =
			new FlatMappedCapturerMatrix(this, f)

		@targetName("flatMapCapturerWithIndex")
		override def flatMapWithIndex[B: ClassTag](f: (A, Int) => CapturerArray[B]): CapturerMatrix[B] =
			new FlatMappedWithIndexCapturerMatrix(this, f)
	}

	trait KeyedObservableMatrix[+A] extends ObservableMatrix[A] {
		override def subscribe(
			onNext: MatrixConsumer[A],
			onError: Throwable => Unit = _ => (),
			onComplete: () => Unit = () => ()
		): Unit = subscribe(onNext, onError, onComplete, null)

		def subscribe(
			onNext: MatrixConsumer[A],
			onError: Throwable => Unit,
			onComplete: () => Unit,
			key: Key
		): Unit

		def unsubscribe(key: Key): Unit

		def isSubscribed(key: Key): Boolean
	}

	trait SettlingMatrix[+A] extends KeyedObservableMatrix[A] {
		def maybeResult(outerIndex: Int, innerIndex: Int): Maybe[A]

		inline def isCompleted(outerIndex: Int, innerIndex: Int): Boolean = maybeResult(outerIndex, innerIndex).isDefined

		inline def isPending(outerIndex: Int, innerIndex: Int): Boolean = maybeResult(outerIndex, innerIndex).isEmpty

		override def flattenToInner: KeyedObservableArray[A]

		override def flattenToOuter: KeyedObservableArray[A]

		override def flattenWith(f: (A, Int, Int) => Int): KeyedObservableArray[A]

		override def flattenToSequential: KeyedObservableArray[A]

		override def flattenMap[B: ClassTag](f: (A, Int, Int) => (B, Int)): KeyedObservableArray[B]

		override def flattenMap[B: ClassTag](valueMap: (A, Int, Int) => B, indexMap: (A, B, Int, Int) => Int): KeyedObservableArray[B]

		def flattenFold[B: ClassTag, S](initialState: S)(f: (S, A, Int, Int) => (S, B, Int)): KeyedObservableArray[B]
	}

	trait CapturerMatrix[+A] extends SettlingMatrix[A] {
		override def flattenToInner: KeyedCapturerArray[A]

		override def flattenToOuter: KeyedCapturerArray[A]

		override def flattenWith(f: (A, Int, Int) => Int): KeyedCapturerArray[A]

		override def flattenToSequential: KeyedCapturerArray[A]

		override def flattenMap[B: ClassTag](f: (A, Int, Int) => (B, Int)): KeyedCapturerArray[B]

		override def flattenMap[B: ClassTag](valueMap: (A, Int, Int) => B, indexMap: (A, B, Int, Int) => Int): KeyedCapturerArray[B]

		override def flattenFold[B: ClassTag, S](initialState: S)(f: (S, A, Int, Int) => (S, B, Int)): KeyedCapturerArray[B]
	}

	final class FlatMappedCapturerMatrix[A, B](
		val source: CapturerArray[A],
		val f: A => CapturerArray[B]
	) extends CapturerMatrix[B] {
		private val activeInnerSubscriptions = scala.collection.mutable.Map[Key, List[CapturerArray[B]]]()

		override def maybeResult(outerIndex: Int, innerIndex: Int): Maybe[B] =
			source.maybeResult(outerIndex).flatMap(a => f(a).maybeResult(innerIndex))

		override def subscribe(
			onNext: MatrixConsumer[B],
			onError: Throwable => Unit,
			onComplete: () => Unit,
			key: Key
		): Unit = {
			if key != null then unsubscribe(key)
			flatMappedMatrixSubscribe(source, (a, _) => f(a), activeInnerSubscriptions, onNext, onError, onComplete, key)
		}

		override def unsubscribe(key: Key): Unit = {
			if key != null then
				source.unsubscribe(key)
				activeInnerSubscriptions.remove(key).foreach { list =>
					list.foreach(_.unsubscribe(key))
				}
		}

		override def isSubscribed(key: Key): Boolean =
			key != null && source.isSubscribed(key)

		override def flattenToInner: KeyedCapturerArray[B] = new FlattenedToInnerArray(this)

		override def flattenToOuter: KeyedCapturerArray[B] = new FlattenedToOuterArray(this)

		override def flattenWith(f: (B, Int, Int) => Int): KeyedCapturerArray[B] = new FlattenedWithArray(this, f)

		override def flattenToSequential: KeyedCapturerArray[B] = new FlattenedToSequentialArray(this)

		override def flattenMap[C: ClassTag](f: (B, Int, Int) => (C, Int)): KeyedCapturerArray[C] = new FlattenedMapArray(this, f)

		override def flattenMap[C: ClassTag](valueMap: (B, Int, Int) => C, indexMap: (B, C, Int, Int) => Int): KeyedCapturerArray[C] = new FlattenedMapWithIndexArray(this, valueMap, indexMap)

		override def flattenFold[C: ClassTag, S](initialState: S)(f: (S, B, Int, Int) => (S, C, Int)): KeyedCapturerArray[C] = new FlattenedFoldArray(this, initialState, f)
	}

	final class FlatMappedWithIndexCapturerMatrix[A, B](
		val source: CapturerArray[A],
		val f: (A, Int) => CapturerArray[B]
	) extends CapturerMatrix[B] {
		private val activeInnerSubscriptions = scala.collection.mutable.Map[Key, List[CapturerArray[B]]]()

		override def maybeResult(outerIndex: Int, innerIndex: Int): Maybe[B] =
			source.maybeResult(outerIndex).flatMap(a => f(a, outerIndex).maybeResult(innerIndex))

		override def subscribe(
			onNext: MatrixConsumer[B],
			onError: Throwable => Unit,
			onComplete: () => Unit,
			key: Key
		): Unit = {
			if key != null then unsubscribe(key)
			flatMappedMatrixSubscribe(source, f, activeInnerSubscriptions, onNext, onError, onComplete, key)
		}

		override def unsubscribe(key: Key): Unit = {
			if key != null then
				source.unsubscribe(key)
				activeInnerSubscriptions.remove(key).foreach { list =>
					list.foreach(_.unsubscribe(key))
				}
		}

		override def isSubscribed(key: Key): Boolean =
			key != null && source.isSubscribed(key)

		override def flattenToInner: KeyedCapturerArray[B] = new FlattenedToInnerArray(this)

		override def flattenToOuter: KeyedCapturerArray[B] = new FlattenedToOuterArray(this)

		override def flattenWith(f: (B, Int, Int) => Int): KeyedCapturerArray[B] = new FlattenedWithArray(this, f)

		override def flattenToSequential: KeyedCapturerArray[B] = new FlattenedToSequentialArray(this)

		override def flattenMap[C: ClassTag](f: (B, Int, Int) => (C, Int)): KeyedCapturerArray[C] = new FlattenedMapArray(this, f)

		override def flattenMap[C: ClassTag](valueMap: (B, Int, Int) => C, indexMap: (B, C, Int, Int) => Int): KeyedCapturerArray[C] = new FlattenedMapWithIndexArray(this, valueMap, indexMap)

		override def flattenFold[C: ClassTag, S](initialState: S)(f: (S, B, Int, Int) => (S, C, Int)): KeyedCapturerArray[C] = new FlattenedFoldArray(this, initialState, f)
	}

	final class FlattenedToInnerArray[+A](val matrix: KeyedObservableMatrix[A]) extends KeyedCapturerArray[A] {
		override def subscribe(
			onNext: MatrixConsumer[A],
			onError: Throwable => Unit,
			onComplete: () => Unit,
			key: Key
		): Unit =
			matrix.subscribe((a, outer, inner) => onNext(a, 0, inner), onError, onComplete, key)

		override def unsubscribe(key: Key): Unit = matrix.unsubscribe(key)

		override def isSubscribed(key: Key): Boolean = matrix.isSubscribed(key)
	}

	final class FlattenedToOuterArray[+A](val matrix: KeyedObservableMatrix[A]) extends KeyedCapturerArray[A] {
		override def subscribe(
			onNext: MatrixConsumer[A],
			onError: Throwable => Unit,
			onComplete: () => Unit,
			key: Key
		): Unit =
			matrix.subscribe((a, outer, inner) => onNext(a, 0, outer), onError, onComplete, key)

		override def unsubscribe(key: Key): Unit = matrix.unsubscribe(key)

		override def isSubscribed(key: Key): Boolean = matrix.isSubscribed(key)
	}

	final class FlattenedWithArray[A](val matrix: KeyedObservableMatrix[A], val f: (A, Int, Int) => Int) extends KeyedCapturerArray[A] {
		override def subscribe(
			onNext: MatrixConsumer[A],
			onError: Throwable => Unit,
			onComplete: () => Unit,
			key: Key
		): Unit =
			matrix.subscribe((a, outer, inner) => onNext(a, 0, f(a, outer, inner)), onError, onComplete, key)

		override def unsubscribe(key: Key): Unit = matrix.unsubscribe(key)

		override def isSubscribed(key: Key): Boolean = matrix.isSubscribed(key)
	}

	final class FlattenedToSequentialArray[+A](val matrix: KeyedObservableMatrix[A]) extends KeyedCapturerArray[A] {
		override def subscribe(
			onNext: MatrixConsumer[A],
			onError: Throwable => Unit,
			onComplete: () => Unit,
			key: Key
		): Unit = {
			var counter = 0
			matrix.subscribe(
				(a, outer, inner) => {
					val index = counter
					counter += 1
					onNext(a, 0, index)
				},
				onError,
				onComplete,
				key
			)
		}

		override def unsubscribe(key: Key): Unit = matrix.unsubscribe(key)

		override def isSubscribed(key: Key): Boolean = matrix.isSubscribed(key)
	}

	final class FlattenedMapArray[A, +B](val matrix: KeyedObservableMatrix[A], val f: (A, Int, Int) => (B, Int)) extends KeyedCapturerArray[B] {
		override def subscribe(
			onNext: MatrixConsumer[B],
			onError: Throwable => Unit,
			onComplete: () => Unit,
			key: Key
		): Unit =
			matrix.subscribe(
				(a, outer, inner) => {
					val (b, index) = f(a, outer, inner)
					onNext(b, 0, index)
				},
				onError,
				onComplete,
				key
			)

		override def unsubscribe(key: Key): Unit = matrix.unsubscribe(key)

		override def isSubscribed(key: Key): Boolean = matrix.isSubscribed(key)
	}

	final class FlattenedMapWithIndexArray[A, B](
		val matrix: KeyedObservableMatrix[A],
		val valueMap: (A, Int, Int) => B,
		val indexMap: (A, B, Int, Int) => Int
	) extends KeyedCapturerArray[B] {
		override def subscribe(
			onNext: MatrixConsumer[B],
			onError: Throwable => Unit,
			onComplete: () => Unit,
			key: Key
		): Unit =
			matrix.subscribe(
				(a, outer, inner) => {
					val b = valueMap(a, outer, inner)
					onNext(b, 0, indexMap(a, b, outer, inner))
				},
				onError,
				onComplete,
				key
			)

		override def unsubscribe(key: Key): Unit = matrix.unsubscribe(key)

		override def isSubscribed(key: Key): Boolean = matrix.isSubscribed(key)
	}

	final class FlattenedFoldArray[A, B, S](
		val matrix: KeyedObservableMatrix[A],
		val initialState: S,
		val f: (S, A, Int, Int) => (S, B, Int)
	) extends KeyedCapturerArray[B] {
		override def subscribe(
			onNext: MatrixConsumer[B],
			onError: Throwable => Unit,
			onComplete: () => Unit,
			key: Key
		): Unit = {
			var state = initialState
			matrix.subscribe(
				(a, outer, inner) => {
					val (newState, b, index) = f(state, a, outer, inner)
					state = newState
					onNext(b, 0, index)
				},
				onError,
				onComplete,
				key
			)
		}

		override def unsubscribe(key: Key): Unit = matrix.unsubscribe(key)

		override def isSubscribed(key: Key): Boolean = matrix.isSubscribed(key)
	}

	private final class ZippedObservableArray[A, B, C](
		val left: ObservableArray[A],
		val right: ObservableArray[B],
		val f: (A, B) => C
	) extends DefaultObservableArray[C] {
		override def subscribe(
			onNext: MatrixConsumer[C],
			onError: Throwable => Unit,
			onComplete: () => Unit
		): Unit = {
			val leftValues = scala.collection.mutable.Map[Int, A]()
			val rightValues = scala.collection.mutable.Map[Int, B]()
			var leftCompleted = false
			var rightCompleted = false
			var errorFired = false

			def checkComplete(): Unit = {
				if ((leftCompleted && leftValues.isEmpty) || (rightCompleted && rightValues.isEmpty) || (leftCompleted && rightCompleted)) {
					onComplete()
				}
			}

			def handleFailure(ex: Throwable): Unit = {
				if (!errorFired) {
					errorFired = true
					onError(ex)
				}
			}

			left.subscribe(
				(a, up, down) => {
					rightValues.remove(down) match {
						case Some(b) =>
							onNext(f(a, b), up, down)
							checkComplete()
						case None =>
							leftValues(down) = a
					}
				},
				handleFailure,
				() => {
					leftCompleted = true
					checkComplete()
				}
			)

			right.subscribe(
				(b, up, down) => {
					leftValues.remove(down) match {
						case Some(a) =>
							onNext(f(a, b), up, down)
							checkComplete()
						case None =>
							rightValues(down) = b
					}
				},
				handleFailure,
				() => {
					rightCompleted = true
					checkComplete()
				}
			)
		}
	}

	private final class ZippedTaskArray[A, B, C](
		val left: TaskArray[A],
		val right: TaskArray[B],
		val f: (A, B) => C
	) extends DefaultTaskArray[C] {
		override def subscribe(
			onNext: MatrixConsumer[C],
			onError: Throwable => Unit,
			onComplete: () => Unit
		): Unit = {
			val leftValues = scala.collection.mutable.Map[Int, A]()
			val rightValues = scala.collection.mutable.Map[Int, B]()
			var leftCompleted = false
			var rightCompleted = false
			var errorFired = false

			def checkComplete(): Unit = {
				if ((leftCompleted && leftValues.isEmpty) || (rightCompleted && rightValues.isEmpty) || (leftCompleted && rightCompleted)) {
					onComplete()
				}
			}

			def handleFailure(ex: Throwable): Unit = {
				if (!errorFired) {
					errorFired = true
					onError(ex)
				}
			}

			left.subscribe(
				(a, up, down) => {
					rightValues.remove(down) match {
						case Some(b) =>
							onNext(f(a, b), up, down)
							checkComplete()
						case None =>
							leftValues(down) = a
					}
				},
				handleFailure,
				() => {
					leftCompleted = true
					checkComplete()
				}
			)

			right.subscribe(
				(b, up, down) => {
					leftValues.remove(down) match {
						case Some(a) =>
							onNext(f(a, b), up, down)
							checkComplete()
						case None =>
							rightValues(down) = b
					}
				},
				handleFailure,
				() => {
					rightCompleted = true
					checkComplete()
				}
			)
		}
	}

	final class StreamEmitter[A] extends DefaultObservableArray[A] {
		private var subscribers: List[(MatrixConsumer[A], Throwable => Unit, () => Unit)] = Nil
		private var counter = 0
		private var completed = false
		private var error: Throwable | Null = null

		override def subscribe(
			onNext: MatrixConsumer[A],
			onError: Throwable => Unit,
			onComplete: () => Unit
		): Unit = {
			StreamEmitter.this.synchronized {
				if (error != null) {
					onError(error.asInstanceOf[Throwable])
				} else if (completed) {
					onComplete()
				} else {
					subscribers = (onNext, onError, onComplete) :: subscribers
				}
			}
		}

		def emit(value: A): Unit = StreamEmitter.this.synchronized {
			if (!completed && error == null) {
				val idx = counter
				counter += 1
				subscribers.foreach(_._1(value, 0, idx))
			}
		}

		def fail(ex: Throwable): Unit = StreamEmitter.this.synchronized {
			if (!completed && error == null) {
				error = ex
				subscribers.foreach(_._2(ex))
				subscribers = Nil
			}
		}

		def end(): Unit = StreamEmitter.this.synchronized {
			if (!completed && error == null) {
				completed = true
				subscribers.foreach(_._3())
				subscribers = Nil
			}
		}
	}

	/** A persistent, multicast chain of elements distributed over time.
	 *
	 * Note: This implements the following user requirements:
	 * 1) A factory method that receives a collection of Capturer[A] instances, and returns something that allows to subscribe for all the results as they complete. All the subscribers eventually receive the same results. Results already present at subscription time are fed immediately.
	 * 4) A method similar to [[Capturer.subscribe]] that the passed consumers are fed multiple times. */
	inline def CapturerArray_fromCapturers[A](capturers: IArray[Capturer[A]]): CapturerArray[A] =
		new CaptorArray(capturers)

	private inline def arrayFlatMapSubscribe[A, B](
		array: ObservableArray[A],
		f: A => ObservableArray[B],
		onNext: MatrixConsumer[B],
		onError: Throwable => Unit,
		onComplete: () => Unit
	): Unit = {
		var outerCompleted = false
		var activeInnerCount = 0
		var errorFired = false

		def tryComplete(): Unit = {
			if (outerCompleted && activeInnerCount == 0 && !errorFired) {
				onComplete()
			}
		}

		def handleFailure(ex: Throwable): Unit = {
			if (!errorFired) {
				errorFired = true
				onError(ex)
			}
		}

		array.subscribe(
			(a, outerUp, outerDown) => {
				activeInnerCount += 1
				f(a).subscribe(
					(b, innerUp, innerDown) => onNext(b, outerDown, innerDown),
					handleFailure,
					() => {
						activeInnerCount -= 1
						tryComplete()
					}
				)
			},
			handleFailure,
			() => {
				outerCompleted = true
				tryComplete()
			}
		)
	}

	private inline def arrayFlatMapWithIndexSubscribe[A, B](
		array: ObservableArray[A],
		f: (A, Int) => ObservableArray[B],
		onNext: MatrixConsumer[B],
		onError: Throwable => Unit,
		onComplete: () => Unit
	): Unit = {
		var outerCompleted = false
		var activeInnerCount = 0
		var errorFired = false

		def tryComplete(): Unit = {
			if (outerCompleted && activeInnerCount == 0 && !errorFired) {
				onComplete()
			}
		}

		def handleFailure(ex: Throwable): Unit = {
			if (!errorFired) {
				errorFired = true
				onError(ex)
			}
		}

		array.subscribe(
			(a, outerUp, outerDown) => {
				activeInnerCount += 1
				f(a, outerDown).subscribe(
					(b, innerUp, innerDown) => onNext(b, outerDown, innerDown),
					handleFailure,
					() => {
						activeInnerCount -= 1
						tryComplete()
					}
				)
			},
			handleFailure,
			() => {
				outerCompleted = true
				tryComplete()
			}
		)
	}

	private inline def flattenToSequentialSubscribe[A](
		matrix: ObservableMatrix[A],
		onNext: MatrixConsumer[A],
		onError: Throwable => Unit,
		onComplete: () => Unit
	): Unit = {
		var counter = 0
		matrix.subscribe(
			(a, up, down) => {
				val index = counter
				counter += 1
				onNext(a, 0, index)
			},
			onError,
			onComplete
		)
	}

	private inline def flattenMapSubscribe[A, B](
		matrix: ObservableMatrix[A],
		f: (A, Int, Int) => (B, Int),
		onNext: MatrixConsumer[B],
		onError: Throwable => Unit,
		onComplete: () => Unit
	): Unit = matrix.subscribe(
		(a, up, down) => {
			val (b, index) = f(a, up, down)
			onNext(b, 0, index)
		},
		onError,
		onComplete
	)

	private inline def flattenMapWithIndexSubscribe[A, B](
		matrix: ObservableMatrix[A],
		valueMap: (A, Int, Int) => B,
		indexMap: (A, B, Int, Int) => Int,
		onNext: MatrixConsumer[B],
		onError: Throwable => Unit,
		onComplete: () => Unit
	): Unit = matrix.subscribe(
		(a, up, down) => {
			val b = valueMap(a, up, down)
			onNext(b, 0, indexMap(a, b, up, down))
		},
		onError,
		onComplete
	)

	private inline def flattenFoldSubscribe[A, B, S](
		matrix: ObservableMatrix[A],
		initialState: S,
		f: (S, A, Int, Int) => (S, B, Int),
		onNext: MatrixConsumer[B],
		onError: Throwable => Unit,
		onComplete: () => Unit
	): Unit = {
		var state = initialState
		matrix.subscribe(
			(a, up, down) => {
				val (newState, b, index) = f(state, a, up, down)
				state = newState
				onNext(b, 0, index)
			},
			onError,
			onComplete
		)
	}

	private inline def flatMappedMatrixSubscribe[A, B](
		source: CapturerArray[A],
		inline getInner: (A, Int) => CapturerArray[B],
		activeInnerSubscriptions: scala.collection.mutable.Map[Key, List[CapturerArray[B]]],
		onNext: MatrixConsumer[B],
		onError: Throwable => Unit,
		onComplete: () => Unit,
		key: Key
	): Unit = {
		if key != null then
			activeInnerSubscriptions(key) = Nil
		var outerCompleted = false
		var activeInnerCount = 0
		var errorFired = false

		def tryComplete(): Unit = {
			if (outerCompleted && activeInnerCount == 0 && !errorFired) {
				onComplete()
			}
		}

		def handleFailure(ex: Throwable): Unit = {
			if (!errorFired) {
				errorFired = true
				onError(ex)
			}
		}

		source.subscribe(
			(a, outerOuter, outerInner) => {
				val inner = getInner(a, outerInner)
				if key != null then activeInnerSubscriptions.updateWith(key) {
					case Some(list) => Some(inner :: list)
					case None => Some(inner :: Nil)
				}
				activeInnerCount += 1
				inner.subscribe(
					(b, innerOuter, innerInner) => onNext(b, outerInner, innerInner),
					handleFailure,
					() => {
						activeInnerCount -= 1
						tryComplete()
					},
					key
				)
			},
			handleFailure,
			() => {
				outerCompleted = true
				tryComplete()
			},
			key
		)
	}

	///////////////////////////////////////
	/////////// PROMISE CHAINED ///////////
	///////////////////////////////////////

	/** A node in an asynchronous, promise-chained functional stream.
	 *
	 * @param value the element emitted in this node.
	 * @param next  the lazy placeholder (`Chain`) for the remaining elements of the stream.
	 */
	case class ChainNode[+A](value: A, next: ChainOps.Chain[A])

	object ChainOps {
		opaque type Chain[+A] = Capturer[Maybe[ChainNode[A]]]

		def apply[A](underlying: Capturer[Maybe[ChainNode[A]]]): Chain[A] = underlying

		extension [A](chain: Chain[A]) {
			inline def asCapturer: Capturer[Maybe[ChainNode[A]]] = chain
		}
	}
	import ChainOps.Chain

	/** A producer class for dynamically generating an asynchronous promise-chained functional stream.
	 *
	 * Example usage:
	 * {{{
	 * val emitter = new ChainEmitter[Int]()
	 * val stream = emitter.chain
	 *
	 * // Consumer subscribes and maps over the stream recursively
	 * stream.map(_ * 2).foreach { maybeNode =>
	 *   maybeNode.fold {
	 *     println("Stream finished")
	 *   } { node =>
	 *     println(s"Value: ${node.value}")
	 *     // subscribe/recurse via node.next
	 *   }
	 * }
	 *
	 * // Producer emits elements dynamically
	 * emitter.emit(1)
	 * emitter.emit(2)
	 * emitter.end()
	 * }}}
	 */
	final class ChainEmitter[A] {
		private var currentCaptor = new Captor[Maybe[ChainNode[A]]]()

		def chain: Chain[A] = ChainOps(currentCaptor)

		def emit(value: A): Unit = {
			val nextCaptor = new Captor[Maybe[ChainNode[A]]]()
			val oldCaptor = currentCaptor
			currentCaptor = nextCaptor
			oldCaptor.capture(Maybe(ChainNode(value, ChainOps(nextCaptor))))
		}

		def end(): Unit = {
			currentCaptor.capture(Maybe.empty)
		}
	}

	/** Monadic extension methods enabling lazy transformations over promise-chained functional streams. */
	extension [A](chain: Chain[A]) {
		def map[B](f: A => B): Chain[B] = {
			ChainOps(chain.asCapturer.map { maybeNode =>
				maybeNode.map(node => ChainNode(f(node.value), node.next.map(f)))
			})
		}

		def filter(p: A => Boolean): Chain[A] = {
			ChainOps(chain.asCapturer.flatMap { maybeNode =>
				maybeNode.fold {
					new Keeper(Maybe.empty)
				} { node =>
					if p(node.value) then
						new Keeper(Maybe(ChainNode(node.value, node.next.filter(p))))
					else
						node.next.filter(p).asCapturer
				}
			})
		}

		def concat(other: => Chain[A]): Chain[A] = {
			ChainOps(chain.asCapturer.flatMap { maybeNode =>
				maybeNode.fold {
					other.asCapturer
				} { node =>
					new Keeper(Maybe(ChainNode(node.value, node.next.concat(other))))
				}
			})
		}

		def flatMap[B](f: A => Chain[B]): Chain[B] = {
			ChainOps(chain.asCapturer.flatMap { maybeNode =>
				maybeNode.fold {
					new Keeper(Maybe.empty)
				} { node =>
					f(node.value).concat(node.next.flatMap(f)).asCapturer
				}
			})
		}
	}
}