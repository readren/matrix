package readren.sequencer

import readren.common.*

import scala.annotation.targetName
import scala.annotation.unchecked.uncheckedVariance
import scala.compiletime.uninitialized
import scala.reflect.ClassTag
import scala.util.control.NonFatal

/** A sandbox prototyping the Task (work) and Capturer (result) hierarchies under Observable,
 * verifying path-dependent typing, covariance, and flatMap/map signature specialization
 * with guarded methods and decorators.
 */
trait SandboxDoer { thisDoer =>
	type Key = AnyRef

	inline given CanEqual[Key, Key] = CanEqual.derived

	inline val NOT_APPLICABLE_INDEX = -1

	trait Observer[-A] {
		def onNext(value: A, upChain: Int, downChain: Int): Unit

		def onError(ex: Throwable): Unit

		def onComplete(): Unit
	}

	/** Root super trait of all asynchronous observables. */
	trait Observable[+A] {
		def subscribe(observer: Observer[A]): Unit

		inline def subscribeCallbacks(inline next: (A, Int, Int) => Unit, inline error: Throwable => Unit = _ => (), inline complete: () => Unit = () => ()): Unit = {
			subscribe(new Observer {
				override def onNext(value: A, upChain: Int, downChain: Int): Unit = next(value, upChain, downChain)

				override def onError(ex: Throwable): Unit = error(ex)

				override def onComplete(): Unit = complete()
			})
		}

		inline def foreach(inline consumer: A => Unit): Unit = subscribeCallbacks((a, _, _) => consumer(a))

		def map[B](f: A => B): Observable[B]

		def flatMap[B](f: A => Observable[B]): Observable[B]
	}

	// ==================== DOABLE WORK HIERARCHY ====================

	/** An exception-unaware lazy computation that starts a fresh execution on subscribe.
	 * Serves as the root of all doable work.
	 */
	trait Task[+A] extends Observable[A] { thisTask =>
		override def map[B](f: A => B): Task[B] = {
			(observer: Observer[B]) => {
				thisTask.subscribe(new Observer[A] {
					override def onNext(value: A, upChain: Int, downChain: Int): Unit = observer.onNext(f(value), upChain, downChain)

					override def onError(ex: Throwable): Unit = observer.onError(ex)

					override def onComplete(): Unit = observer.onComplete()
				})
			}
		}

		override def flatMap[B](f: A => Observable[B]): Task[B] = {
			(observer: Observer[B]) => {
				thisTask.subscribe(new Observer[A] {
					override def onNext(value: A, upChain: Int, downChain: Int): Unit = f(value).subscribe(observer)

					override def onError(ex: Throwable): Unit = observer.onError(ex)

					override def onComplete(): Unit = observer.onComplete()
				})
			}
		}

		@targetName("flatMapTask")
		def flatMap[B](f: A => Task[B]): Task[B] = {
			(observer: Observer[B]) => {
				thisTask.subscribe(new Observer[A] {
					override def onNext(value: A, upChain: Int, downChain: Int): Unit = f(value).subscribe(observer)

					override def onError(ex: Throwable): Unit = observer.onError(ex)

					override def onComplete(): Unit = observer.onComplete()
				})
			}
		}

		def mapGuarded[B](f: A => B): Task[B] = {
			(observer: Observer[B]) => {
				thisTask.subscribe(new Observer[A] {
					override def onNext(value: A, upChain: Int, downChain: Int): Unit = {
						val maybeB = try Maybe(f(value)) catch {
							case NonFatal(e) =>
								observer.onError(e)
								Maybe.empty
						}
						maybeB.foreach(observer.onNext(_, upChain, downChain))
					}

					override def onError(ex: Throwable): Unit = observer.onError(ex)

					override def onComplete(): Unit = observer.onComplete()
				})
			}
		}

		def flatMapGuarded[B](f: A => Observable[B]): Task[B] = {
			(observer: Observer[B]) => {
				thisTask.subscribe(new Observer[A] {
					override def onNext(value: A, upChain: Int, downChain: Int): Unit = {
						val maybeObs = try Maybe(f(value)) catch {
							case NonFatal(e) =>
								observer.onError(e)
								Maybe.empty
						}
						maybeObs.foreach(_.subscribe(observer))
					}

					override def onError(ex: Throwable): Unit = observer.onError(ex)

					override def onComplete(): Unit = observer.onComplete()
				})
			}
		}

		@targetName("flatMapTaskGuarded")
		def flatMapGuarded[B](f: A => Task[B]): Task[B] = {
			(observer: Observer[B]) => {
				thisTask.subscribe(new Observer[A] {
					override def onNext(value: A, upChain: Int, downChain: Int): Unit = {
						val maybeTask = try Maybe(f(value)) catch {
							case NonFatal(e) =>
								observer.onError(e)
								Maybe.empty
						}
						maybeTask.foreach(_.subscribe(observer))
					}

					override def onError(ex: Throwable): Unit = observer.onError(ex)

					override def onComplete(): Unit = observer.onComplete()
				})
			}
		}

		def guarded: Task[A] = new GuardedTask(thisTask)
	}

	// ==================== ASYNCHRONOUS RESULT HIERARCHY ====================

	/** Exception-unaware single result capturer. Ex LatchingTask
	 * Does not inherit from Task, cleanly separating results from doable work. */
	sealed trait Capturer[+A] extends Observable[A] { thisLatch =>
		override def subscribe(observer: Observer[A]): Unit = subscribeWithCoords(observer, null, NOT_APPLICABLE_INDEX, NOT_APPLICABLE_INDEX)

		/** Subscribes with coordinate tracking.
		 * @param observer the observer invoked when the value is captured.
		 * Unified as a [[Observer]] carrying both `upChain` and `downChain` indices to avoid adapter allocations during pipeline propagation. Because a [[Capturer]] is a single-value cache:
		 * - Standalone/direct subscriptions default both `upChain` and `downChain` to `-1`.
		 * - When managed by a parent [[CapturerArray]], `upChain` is `0` and `downChain` represents the element's index.
		 * - When managed by a parent [[CapturerMatrix]], `upChain` represents the outer/row index and `downChain` represents the inner/column index. */
		def subscribeWithCoords(observer: Observer[A], key: Key, upChain: Int, downChain: Int): Unit

		def trial: Trial[A]

		def maybeValue: Maybe[A] = trial.toMaybe

		inline def isCompleted: Boolean = trial.isDefined

		inline def isPending: Boolean = trial.isEmpty

		def unsubscribe(key: Key): Unit

		def isSubscribed(key: Key): Boolean

		def unsubscribe(observer: Observer[A]): Unit

		def isSubscribed(observer: Observer[A]): Boolean

		override def map[B](f: A => B): Capturer[B] = { // necessary to downcast the result type when a subclass isn't covariant.
			this match {
				case ready: Keeper[A] =>
					ready.map(f)
				case failed: Failed =>
					failed.map(f)
				case captor: Captor[A] @unchecked =>
					captor.map(f)
				case guarded: GuardedCapturer[A] =>
					guarded.map(f)
			}
		}

		override def flatMap[B](f: A => Observable[B]): Observable[B] = { // necessary to downcast the result type when a subclass isn't covariant.
			this match {
				case ready: Keeper[A] =>
					ready.flatMap(f)
				case failed: Failed =>
					failed.flatMap(f)
				case captor: Captor[A] @unchecked =>
					captor.flatMap(f)
				case guarded: GuardedCapturer[A] =>
					guarded.flatMap(f)
			}
		}

		@targetName("flatMapCapturer")
		def flatMap[B](f: A => Capturer[B]): Capturer[B] = { // necessary to downcast the result type when a subclass isn't covariant.
			this match {
				case ready: Keeper[A] =>
					ready.flatMap(f)
				case failed: Failed =>
					failed.flatMap(f)
				case captor: Captor[A] @unchecked =>
					captor.flatMap(f)
				case guarded: GuardedCapturer[A] =>
					guarded.flatMap(f)
			}
		}

		def mapGuarded[B](f: A => B): Capturer[B] = {
			this match {
				case ready: Keeper[A] =>
					ready.mapGuarded(f)
				case failed: Failed =>
					failed.mapGuarded(f)
				case captor: Captor[A] @unchecked =>
					captor.mapGuarded(f)
				case guarded: GuardedCapturer[A] =>
					guarded.mapGuarded(f)
			}
		}

		def flatMapGuarded[B](f: A => Observable[B]): Observable[B] = {
			this match {
				case ready: Keeper[A] =>
					ready.flatMapGuarded(f)
				case failed: Failed =>
					failed.flatMapGuarded(f)
				case captor: Captor[A] @unchecked =>
					captor.flatMapGuarded(f)
				case guarded: GuardedCapturer[A] =>
					guarded.flatMapGuarded(f)
			}
		}

		@targetName("flatMapCapturerGuarded")
		def flatMapGuarded[B](f: A => Capturer[B]): Capturer[B] = {
			this match {
				case ready: Keeper[A] =>
					ready.flatMapGuarded(f)
				case failed: Failed =>
					failed.flatMapGuarded(f)
				case captor: Captor[A] @unchecked =>
					captor.flatMapGuarded(f)
				case guarded: GuardedCapturer[A] =>
					guarded.flatMapGuarded(f)
			}
		}

		def guarded: Capturer[A] = new GuardedCapturer(thisLatch)
	}

	/** A [[Capturer]] that has already captured a value. Ex ReadyTask */
	class Keeper[+A](val value: A) extends Capturer[A] {
		override def trial: Trial[A] = Trial.success(value)

		override def subscribeWithCoords(observer: Observer[A], key: Key, upChain: Int, downChain: Int): Unit = {
			observer.onNext(value, upChain, downChain)
			observer.onComplete()
		}

		override def unsubscribe(observer: Observer[A]): Unit = ()

		override def unsubscribe(key: Key): Unit = ()

		override def isSubscribed(observer: Observer[A]): Boolean = false

		override def isSubscribed(key: Key): Boolean = false

		override def map[B](f: A => B): Capturer[B] = new Keeper(f(value))

		override def flatMap[B](f: A => Observable[B]): Observable[B] = f(value)

		@targetName("flatMapCapturer")
		override def flatMap[B](f: A => Capturer[B]): Capturer[B] = f(value)

		override def mapGuarded[B](f: A => B): Capturer[B] = {
			try new Keeper(f(value)) catch {
				case NonFatal(e) => new Failed(e)
			}
		}

		override def flatMapGuarded[B](f: A => Observable[B]): Observable[B] = {
			try f(value) catch {
				case NonFatal(e) => new Failed(e)
			}
		}

		@targetName("flatMapCapturerGuarded")
		override def flatMapGuarded[B](f: A => Capturer[B]): Capturer[B] = {
			try f(value) catch {
				case NonFatal(e) => new Failed(e)
			}
		}
	}

	class Failed(val exception: Throwable) extends Capturer[Nothing] {
		override def trial: Trial[Nothing] = Trial.failure(exception)

		override def subscribeWithCoords(observer: Observer[Nothing], key: Key, upChain: Int, downChain: Int): Unit = observer.onError(exception)

		override def unsubscribe(observer: Observer[Nothing]): Unit = ()

		override def unsubscribe(key: Key): Unit = ()

		override def isSubscribed(observer: Observer[Nothing]): Boolean = false

		override def isSubscribed(key: Key): Boolean = false

		override def map[B](f: Nothing => B): Capturer[B] = this.asInstanceOf[Failed]

		override def flatMap[B](f: Nothing => Observable[B]): Observable[B] = this.asInstanceOf[Failed]

		@targetName("flatMapCapturer")
		override def flatMap[B](f: Nothing => Capturer[B]): Capturer[B] = this.asInstanceOf[Failed]

		override def mapGuarded[B](f: Nothing => B): Capturer[B] = this.asInstanceOf[Failed]

		override def flatMapGuarded[B](f: Nothing => Observable[B]): Observable[B] = this.asInstanceOf[Failed]

		@targetName("flatMapCapturerGuarded")
		override def flatMapGuarded[B](f: Nothing => Capturer[B]): Capturer[B] = this.asInstanceOf[Failed]
	}

	class Captor[A](initialState: Trial[A] = Trial.empty) extends Capturer[A] {
		private var state: Trial[A] = initialState
		private var observers: List[(Key, Observer[A], Int, Int)] = Nil

		override def trial: Trial[A] = state

		override def subscribeWithCoords(observer: Observer[A], key: Key, upChain: Int, downChain: Int): Unit = {
			state.fold {
				if key != null then unsubscribe(key)
				observers = (key, observer, upChain, downChain) :: observers
			} { ex =>
				observer.onError(ex)
			} { a =>
				observer.onNext(a, upChain, downChain)
				observer.onComplete()
			}
		}

		override def unsubscribe(key: Key): Unit = if key != null then observers = observers.filterNot(_._1 == key)

		override def unsubscribe(observer: Observer[A]): Unit = observers = observers.filterNot(_._2 eq observer)

		override def isSubscribed(key: Key): Boolean = key != null && observers.exists(_._1 == key)

		override def isSubscribed(observer: Observer[A]): Boolean = observers.exists(_._2 eq observer)

		override def map[B](f: A => B): Capturer[B] = {
			state.fold {
				val captor = new Captor[B]()
				this.subscribe(new Observer[A] {
					override def onNext(a: A, up: Int, down: Int): Unit = captor.capture(f(a))

					override def onError(ex: Throwable): Unit = captor.fail(ex)

					override def onComplete(): Unit = ()
				})
				captor
			} { ex =>
				new Failed(ex)
			} { a =>
				new Keeper(f(a))
			}
		}

		override def flatMap[B](f: A => Observable[B]): Observable[B] = {
			state.fold {
				val captor = new Captor[B]()
				this.subscribe(new Observer[A] {
					override def onNext(a: A, up: Int, down: Int): Unit = {
						f(a).subscribe(new Observer[B] {
							override def onNext(b: B, u: Int, d: Int): Unit = captor.capture(b)

							override def onError(ex: Throwable): Unit = captor.fail(ex)

							override def onComplete(): Unit = ()
						})
					}

					override def onError(ex: Throwable): Unit = captor.fail(ex)

					override def onComplete(): Unit = ()
				})
				captor: Observable[B]
			} { ex =>
				new Failed(ex)
			} { a =>
				f(a)
			}
		}

		@targetName("flatMapCapturer")
		override def flatMap[B](f: A => Capturer[B]): Capturer[B] = {
			state.fold {
				val captor = new Captor[B]()
				this.subscribe(new Observer[A] {
					override def onNext(a: A, up: Int, down: Int): Unit = {
						f(a).subscribe(new Observer[B] {
							override def onNext(b: B, u: Int, d: Int): Unit = captor.capture(b)

							override def onError(ex: Throwable): Unit = captor.fail(ex)

							override def onComplete(): Unit = ()
						})
					}

					override def onError(ex: Throwable): Unit = captor.fail(ex)

					override def onComplete(): Unit = ()
				})
				captor
			} { ex =>
				new Failed(ex)
			} { a =>
				f(a)
			}
		}

		override def mapGuarded[B](f: A => B): Capturer[B] = {
			state.fold {
				val captor = new Captor[B]()
				this.subscribe(new Observer[A] {
					override def onNext(a: A, up: Int, down: Int): Unit = {
						val maybeB = try Maybe(f(a)) catch {
							case NonFatal(e) =>
								captor.fail(e)
								Maybe.empty
						}
						maybeB.foreach(captor.capture)
					}

					override def onError(ex: Throwable): Unit = captor.fail(ex)

					override def onComplete(): Unit = ()
				})
				captor
			} { ex =>
				new Failed(ex)
			} { a =>
				try new Keeper(f(a)) catch {
					case NonFatal(e) => new Failed(e)
				}
			}
		}

		override def flatMapGuarded[B](f: A => Observable[B]): Observable[B] = {
			state.fold {
				val captor = new Captor[B]()
				this.subscribe(new Observer[A] {
					override def onNext(a: A, up: Int, down: Int): Unit = {
						val maybeObs = try Maybe(f(a)) catch {
							case NonFatal(e) =>
								captor.fail(e)
								Maybe.empty
						}
						maybeObs.foreach { obs =>
							obs.subscribe(new Observer[B] {
								override def onNext(b: B, u: Int, d: Int): Unit = captor.capture(b)

								override def onError(ex: Throwable): Unit = captor.fail(ex)

								override def onComplete(): Unit = ()
							})
						}
					}

					override def onError(ex: Throwable): Unit = captor.fail(ex)

					override def onComplete(): Unit = ()
				})
				captor: Observable[B]
			} { ex =>
				new Failed(ex)
			} { a =>
				try f(a) catch {
					case NonFatal(e) => new Failed(e)
				}
			}
		}

		@targetName("flatMapCapturerGuarded")
		override def flatMapGuarded[B](f: A => Capturer[B]): Capturer[B] = {
			state.fold {
				val captor = new Captor[B]()
				this.subscribe(new Observer[A] {
					override def onNext(a: A, up: Int, down: Int): Unit = {
						val maybeCaptor = try Maybe(f(a)) catch {
							case NonFatal(e) =>
								captor.fail(e)
								Maybe.empty
						}
						maybeCaptor.foreach { c =>
							c.subscribe(new Observer[B] {
								override def onNext(b: B, u: Int, d: Int): Unit = captor.capture(b)

								override def onError(ex: Throwable): Unit = captor.fail(ex)

								override def onComplete(): Unit = ()
							})
						}
					}

					override def onError(ex: Throwable): Unit = captor.fail(ex)

					override def onComplete(): Unit = ()
				})
				captor
			} { ex =>
				new Failed(ex)
			} { a =>
				try f(a) catch {
					case NonFatal(e) => new Failed(e)
				}
			}
		}

		def capture(result: A): Unit = {
			if state.isEmpty then {
				state = Trial.success(result)
				val currentObservers = observers
				observers = Nil
				currentObservers.reverse.foreach { (_, observer, upChain, downChain) =>
					observer.onNext(result, upChain, downChain)
					observer.onComplete()
				}
			}
		}

		def fail(ex: Throwable): Unit = {
			if state.isEmpty then {
				state = Trial.failure(ex)
				val currentObservers = observers
				observers = Nil
				currentObservers.reverse.foreach { (_, observer, _, _) =>
					observer.onError(ex)
				}
			}
		}
	}

	class GuardedTask[+A](underlying: Task[A]) extends Task[A] {
		override def subscribe(observer: Observer[A]): Unit = underlying.subscribe(observer)

		override def map[B](f: A => B): Task[B] = underlying.mapGuarded(f)

		override def flatMap[B](f: A => Observable[B]): Task[B] = underlying.flatMapGuarded(f)

		@targetName("flatMapTask")
		override def flatMap[B](f: A => Task[B]): Task[B] = underlying.flatMapGuarded(f)
	}

	class GuardedCapturer[+A](underlying: Capturer[A]) extends Capturer[A] {
		override def trial: Trial[A] = underlying.trial

		override def subscribeWithCoords(observer: Observer[A], key: Key, upChain: Int, downChain: Int): Unit =
			underlying.subscribeWithCoords(observer, key, upChain, downChain)

		override def unsubscribe(key: Key): Unit = underlying.unsubscribe(key)

		override def unsubscribe(observer: Observer[A]): Unit = underlying.unsubscribe(observer)

		override def isSubscribed(key: Key): Boolean = underlying.isSubscribed(key)

		override def isSubscribed(observer: Observer[A]): Boolean = underlying.isSubscribed(observer)

		override def map[B](f: A => B): Capturer[B] = underlying.mapGuarded(f)

		override def flatMap[B](f: A => Observable[B]): Observable[B] = underlying.flatMapGuarded(f)

		@targetName("flatMapCapturer")
		override def flatMap[B](f: A => Capturer[B]): Capturer[B] = underlying.flatMapGuarded(f)

		override def mapGuarded[B](f: A => B): Capturer[B] = underlying.mapGuarded(f)

		override def flatMapGuarded[B](f: A => Observable[B]): Observable[B] = underlying.flatMapGuarded(f)

		@targetName("flatMapCapturerGuarded")
		override def flatMapGuarded[B](f: A => Capturer[B]): Capturer[B] = underlying.flatMapGuarded(f)
	}

	// ===================================================================
	// ==================== CHAIN SUPPORT PRIMITIVES =====================
	// ===================================================================

	///////////////////////////////////////
	//////////// PUSH BASED ///////////////
	///////////////////////////////////////

	trait ObservableStream[+A] { thisObservableStream =>
		def subscribe(observer: Observer[A]): Unit

		inline def subscribeCallbacks(inline onNextCallback: (A, Int, Int) => Unit, inline onErrorCallback: Throwable => Unit = _ => (), inline onCompleteCallback: () => Unit = () => ()): Unit = {
			subscribe(new Observer[A] {
				override def onNext(value: A, upChain: Int, downChain: Int): Unit = onNextCallback(value, upChain, downChain)

				override def onError(ex: Throwable): Unit = onErrorCallback(ex)

				override def onComplete(): Unit = onCompleteCallback()
			})
		}

		inline def foreach(inline consumer: A => Unit): Unit = {
			subscribe(new Observer[A] {
				override def onNext(value: A, upChain: Int, downChain: Int): Unit = consumer(value)

				override def onError(ex: Throwable): Unit = ()

				override def onComplete(): Unit = ()
			})
		}

		inline def foreachWithCoords(inline consumer: (A, Int, Int) => Unit): Unit = {
			subscribe(new Observer[A] {
				override def onNext(value: A, upChain: Int, downChain: Int): Unit = consumer(value, upChain, downChain)

				override def onError(ex: Throwable): Unit = ()

				override def onComplete(): Unit = ()
			})
		}

		def map[B: ClassTag](f: A => B): ObservableStream[B]

		def mapWithCoords[B: ClassTag](f: (A, Int, Int) => B): ObservableStream[B]

		def flatMap[B: ClassTag](f: A => ObservableStream[B]): ObservableMatrix[B]

		def flatMapWithCoords[B: ClassTag](f: (A, Int, Int) => ObservableStream[B]): ObservableMatrix[B]

		def scan[B: ClassTag](initial: B)(f: (B, A) => B): ObservableStream[B]

		def buffer[T >: A : ClassTag](size: Int): ObservableStream[IArray[T]]

		def zip[B, C: ClassTag](other: ObservableStream[B])(f: (A, B) => C): ObservableStream[C]

		def take(n: Int): ObservableStream[A]

		def takeWhile(p: A => Boolean): ObservableStream[A]

		/** Collapses the stream into a single value, allowing early termination via Maybe.empty. */
		def foldWhile[B](initial: B)(f: (B, A) => Maybe[B]): Task[B] = {
			(observer: Observer[B]) => {
				var state = initial
				var active = true

				this.subscribe(new Observer[A] {
					override def onNext(a: A, up: Int, down: Int): Unit = {
						if active then {
							val next = try f(state, a) catch {
								case NonFatal(ex) => 
									active = false
									observer.onError(ex)
									Maybe.empty
							}
							if active then {
								next.fold {
									active = false
									observer.onNext(state, NOT_APPLICABLE_INDEX, NOT_APPLICABLE_INDEX)
									observer.onComplete()
								} { nextState =>
									state = nextState
								}
							}
						}
					}

					override def onError(ex: Throwable): Unit = {
						if active then {
							active = false
							observer.onError(ex)
						}
					}

					override def onComplete(): Unit = {
						if active then {
							active = false
							observer.onNext(state, NOT_APPLICABLE_INDEX, NOT_APPLICABLE_INDEX)
							observer.onComplete()
						}
					}
				})
			}
		}
	}

	object ObservableStream {
		def empty[A]: ObservableStream[A] = new DefaultObservableStream[A] {
			override def subscribe(observer: Observer[A]): Unit = observer.onComplete()
		}

		def apply[A](elements: A*): ObservableStream[A] = fromIterable(elements)

		def fromIterable[A](iterable: Iterable[A]): ObservableStream[A] = new DefaultObservableStream[A] {
			override def subscribe(observer: Observer[A]): Unit = {
				val it = iterable.iterator
				var index = 0
				while it.hasNext do {
					val v = it.next()
					observer.onNext(v, NOT_APPLICABLE_INDEX, index)
					index += 1
				}
				observer.onComplete()
			}
		}

		def fromIterableGuarded[A](iterable: Iterable[A]): ObservableStream[A] = new DefaultObservableStream[A] {
			override def subscribe(observer: Observer[A]): Unit = {
				val it = iterable.iterator
				var index = 0
				var active = true
				while active do {
					val hasNext = try it.hasNext catch {
						case NonFatal(e) =>
							observer.onError(e)
							active = false
							false
					}
					if hasNext then {
						try {
							val v = it.next()
							observer.onNext(v, NOT_APPLICABLE_INDEX, index)
							index += 1
						} catch {
							case NonFatal(e) =>
								observer.onError(e)
								active = false
						}
					} else if active then {
						observer.onComplete()
						active = false
					}
				}
			}
		}

		def generate[A](supplier: () => A): ObservableStream[A] = new DefaultObservableStream[A] {
			override def subscribe(observer: Observer[A]): Unit = {
				var index = 0
				var active = true
				while active do {
					val maybeVal = try Maybe(supplier()) catch {
						case NonFatal(e) =>
							active = false
							observer.onError(e)
							Maybe.empty
					}
					maybeVal.foreach { v =>
						observer.onNext(v, NOT_APPLICABLE_INDEX, index)
						index += 1
					}
				}
			}
		}

		def generateKeyed[A](supplier: () => A): KeyedObservableStream[A] = new KeyedObservableStream[A] {
			private val activeKeys = scala.collection.mutable.Set[Key]()

			override def keyedSubscribe(observer: Observer[A], key: Key): Unit = {
				if key != null then activeKeys.add(key)
				var index = 0
				while (key == null || activeKeys.contains(key)) do {
					val maybeVal = try Maybe(supplier()) catch {
						case NonFatal(e) =>
							if key != null then activeKeys.remove(key)
							observer.onError(e)
							Maybe.empty
					}
					maybeVal.foreach { v =>
						observer.onNext(v, NOT_APPLICABLE_INDEX, index)
						index += 1
					}
				}
			}

			override def unsubscribe(key: Key): Unit = if key != null then activeKeys.remove(key)

			override def isSubscribed(key: Key): Boolean = key != null && activeKeys.contains(key)
		}

		def unfold[S, A](initial: S)(f: S => Maybe[(A, S)]): ObservableStream[A] = new DefaultObservableStream[A] {
			override def subscribe(observer: Observer[A]): Unit = {
				var state = initial
				var index = 0
				var active = true
				while active do {
					val maybeStep = try Maybe(f(state)) catch {
						case NonFatal(e) =>
							active = false
							observer.onError(e)
							Maybe.empty
					}
					maybeStep.fold {
						active = false
					} {
						_.fold {
							active = false
							observer.onComplete()
						} { step =>
							state = step._2
							observer.onNext(step._1, NOT_APPLICABLE_INDEX, index)
							index += 1
						}
					}
				}
			}
		}
	}

	/** Partial implementation of [[ObservableStream]] */
	trait DefaultObservableStream[+A] extends ObservableStream[A] {
		override def map[B: ClassTag](f: A => B): ObservableStream[B] = new MappedObservableStream(this, f)

		override def mapWithCoords[B: ClassTag](f: (A, Int, Int) => B): ObservableStream[B] = new MappedWithCoordsObservableStream(this, f)

		override def flatMap[B: ClassTag](f: A => ObservableStream[B]): ObservableMatrix[B] = new FlatMappedObservableMatrix(this, f)

		override def flatMapWithCoords[B: ClassTag](f: (A, Int, Int) => ObservableStream[B]): ObservableMatrix[B] = new FlatMappedWithCoordsObservableMatrix(this, f)

		override def scan[B: ClassTag](initial: B)(f: (B, A) => B): ObservableStream[B] = new ScannedObservableStream(this, initial, f)

		override def buffer[T >: A : ClassTag](size: Int): ObservableStream[IArray[T]] = new BufferedObservableStream[A, T](this, size)

		override def zip[B, C: ClassTag](other: ObservableStream[B])(f: (A, B) => C): ObservableStream[C] = new ZippedObservableStream(this, other, f)

		override def take(n: Int): ObservableStream[A] = new TakeObservableStream(this, n)

		override def takeWhile(p: A => Boolean): ObservableStream[A] = new TakeWhileObservableStream(this, p)
	}

	trait ObservableMatrix[+A] {
		def subscribe(observer: Observer[A]): Unit

		def subscribe(onNext: (A, Int, Int) => Unit, onError: Throwable => Unit = _ => (), onComplete: () => Unit = () => ()): Unit = {
			val onNextLocal = onNext
			val onErrorLocal = onError
			val onCompleteLocal = onComplete
			subscribe(new Observer[A] {
				override def onNext(value: A, upChain: Int, downChain: Int): Unit = onNextLocal(value, upChain, downChain)

				override def onError(ex: Throwable): Unit = onErrorLocal(ex)

				override def onComplete(): Unit = onCompleteLocal()
			})
		}

		def flattenToInner: ObservableStream[A]

		def flattenToOuter: ObservableStream[A]

		def flattenWith(f: (A, Int, Int) => Int): ObservableStream[A]

		def flattenToSequential: ObservableStream[A]

		def flattenMap[B: ClassTag](f: (A, Int, Int) => (B, Int)): ObservableStream[B]

		def flattenMap[B: ClassTag](valueMap: (A, Int, Int) => B, indexMap: (A, B, Int, Int) => Int): ObservableStream[B]

		def flattenFold[B: ClassTag, S](initialState: S)(f: (S, A, Int, Int) => (S, B, Int)): ObservableStream[B]
	}

	trait DefaultObservableMatrix[+A] extends ObservableMatrix[A] {
		override def flattenToInner: ObservableStream[A] = new FlattenedToInnerStream(this)

		override def flattenToOuter: ObservableStream[A] = new FlattenedToOuterStream(this)

		override def flattenWith(f: (A, Int, Int) => Int): ObservableStream[A] = new FlattenedWithStream(this, f)

		override def flattenToSequential: ObservableStream[A] = new FlattenedToSequentialStream(this)

		override def flattenMap[B: ClassTag](f: (A, Int, Int) => (B, Int)): ObservableStream[B] = new FlattenedMapStream(this, f)

		override def flattenMap[B: ClassTag](valueMap: (A, Int, Int) => B, indexMap: (A, B, Int, Int) => Int): ObservableStream[B] = new FlattenedMapWithCoordsStream(this, valueMap, indexMap)

		override def flattenFold[B: ClassTag, S](initialState: S)(f: (S, A, Int, Int) => (S, B, Int)): ObservableStream[B] = new FlattenedFoldStream(this, initialState, f)
	}

	trait TaskArray[+A] extends ObservableStream[A] {
		override def map[B: ClassTag](f: A => B): TaskArray[B]

		override def mapWithCoords[B: ClassTag](f: (A, Int, Int) => B): TaskArray[B]

		override def flatMap[B: ClassTag](f: A => ObservableStream[B]): ObservableMatrix[B]

		override def flatMapWithCoords[B: ClassTag](f: (A, Int, Int) => ObservableStream[B]): ObservableMatrix[B]

		@targetName("flatMapTask")
		def flatMap[B: ClassTag](f: A => TaskArray[B]): TaskMatrix[B]

		@targetName("flatMapTaskWithCoords")
		def flatMapWithCoords[B: ClassTag](f: (A, Int, Int) => TaskArray[B]): TaskMatrix[B]

		override def scan[B: ClassTag](initial: B)(f: (B, A) => B): TaskArray[B]

		override def buffer[T >: A : ClassTag](size: Int): TaskArray[IArray[T]]

		override def zip[B, C: ClassTag](other: ObservableStream[B])(f: (A, B) => C): ObservableStream[C]

		@targetName("zipTask")
		def zip[B, C: ClassTag](other: TaskArray[B])(f: (A, B) => C): TaskArray[C]

		override def take(n: Int): TaskArray[A]

		override def takeWhile(p: A => Boolean): TaskArray[A]
	}

	/** Partial implementation of [[TaskArray]] */
	trait DefaultTaskArray[+A] extends TaskArray[A] with DefaultObservableStream[A] {
		override def map[B: ClassTag](f: A => B): TaskArray[B] = new MappedTaskArray(this, f)

		override def mapWithCoords[B: ClassTag](f: (A, Int, Int) => B): TaskArray[B] = new MappedWithCoordsTaskArray(this, f)

		override def flatMap[B: ClassTag](f: A => ObservableStream[B]): ObservableMatrix[B] = new FlatMappedObservableMatrix(this, f)

		override def flatMapWithCoords[B: ClassTag](f: (A, Int, Int) => ObservableStream[B]): ObservableMatrix[B] = new FlatMappedWithCoordsObservableMatrix(this, f)

		@targetName("flatMapTask")
		override def flatMap[B: ClassTag](f: A => TaskArray[B]): TaskMatrix[B] = new FlatMappedTaskMatrix(this, f)

		@targetName("flatMapTaskWithCoords")
		override def flatMapWithCoords[B: ClassTag](f: (A, Int, Int) => TaskArray[B]): TaskMatrix[B] = new FlatMappedWithCoordsTaskMatrix(this, f)

		override def scan[B: ClassTag](initial: B)(f: (B, A) => B): TaskArray[B] = new ScannedTaskArray(this, initial, f)

		override def buffer[T >: A : ClassTag](size: Int): TaskArray[IArray[T]] = new BufferedTaskArray[A, T](this, size)

		override def zip[B, C: ClassTag](other: ObservableStream[B])(f: (A, B) => C): ObservableStream[C] = new ZippedObservableStream(this, other, f)

		@targetName("zipTask")
		override def zip[B, C: ClassTag](other: TaskArray[B])(f: (A, B) => C): TaskArray[C] = new ZippedTaskArray(this, other, f)

		override def take(n: Int): TaskArray[A] = new TakeTaskArray(this, n)

		override def takeWhile(p: A => Boolean): TaskArray[A] = new TakeWhileTaskArray(this, p)
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

	trait DefaultTaskMatrix[+A] extends TaskMatrix[A] with DefaultObservableMatrix[A] {
		override def flattenToInner: TaskArray[A] = new FlattenedToInnerTaskArray(this)

		override def flattenToOuter: TaskArray[A] = new FlattenedToOuterTaskArray(this)

		override def flattenWith(f: (A, Int, Int) => Int): TaskArray[A] = new FlattenedWithTaskArray(this, f)

		override def flattenToSequential: TaskArray[A] = new FlattenedToSequentialTaskArray(this)

		override def flattenMap[B: ClassTag](f: (A, Int, Int) => (B, Int)): TaskArray[B] = new FlattenedMapTaskArray(this, f)

		override def flattenMap[B: ClassTag](valueMap: (A, Int, Int) => B, indexMap: (A, B, Int, Int) => Int): TaskArray[B] = new FlattenedMapWithCoordsTaskArray(this, valueMap, indexMap)

		override def flattenFold[B: ClassTag, S](initialState: S)(f: (S, A, Int, Int) => (S, B, Int)): TaskArray[B] = new FlattenedFoldTaskArray(this, initialState, f)
	}


	/** Note: This implements the following user requirements:
	 * 2) A factory method that receives a collection of Task[A] instances and returns something that allows to subscribe for all the results as they complete. Each subscription fires a dedicated execution of each Task[A] instance, therefore, different observers may receive different results, depending on the Task nature (pure or context dependent).
	 * 3) A method similar to [[Task.subscribe]] in which the passed consumer is fed multiple times: one per each of the provided [[Task]]s along its index. */
	def TaskArray_fromTasks[A](tasks: IArray[Task[A]]): TaskArray[A] = new TaskArray_FromTasks(tasks)

	private final class TaskArray_FromTasks[+A](tasks: IArray[Task[A]]) extends DefaultTaskArray[A] {
		override def subscribe(observer: Observer[A]): Unit = {
			val size = tasks.length
			if size == 0 then observer.onComplete()
			else {
				var completedCount = 0
				tasks.foreachWithIndex { (task, index) =>
					task.subscribe(new Observer[A] {
						override def onNext(a: A, upChain: Int, downChain: Int): Unit = {
							observer.onNext(a, 0, index)
							completedCount += 1
							if completedCount == size then {
								observer.onComplete()
							}
						}

						override def onError(ex: Throwable): Unit = observer.onError(ex)

						override def onComplete(): Unit = ()
					})
				}
			}
		}
	}

	trait KeyedObservableStream[+A] extends DefaultObservableStream[A] {
		override def subscribe(observer: Observer[A]): Unit = keyedSubscribe(observer, null)

		def keyedSubscribe(observer: Observer[A], key: Key): Unit

		inline def keyedSubscribeCallbacks(inline onNextCallback: (A, Int, Int) => Unit, inline onErrorCallback: Throwable => Unit = _ => (), inline onCompleteCallback: () => Unit = () => (), key: Key): Unit = {
			keyedSubscribe(
				new Observer[A] {
					override def onNext(value: A, upChain: Int, downChain: Int): Unit = onNextCallback(value, upChain, downChain)

					override def onError(ex: Throwable): Unit = onErrorCallback(ex)

					override def onComplete(): Unit = onCompleteCallback()
				},
				key
			)
		}

		def unsubscribe(key: Key): Unit

		def isSubscribed(key: Key): Boolean
	}

	trait SettlingArray[+A] extends KeyedObservableStream[A] {
		def maybeResult(index: Int): Maybe[A]

		inline def isCompleted(index: Int): Boolean = maybeResult(index).isDefined

		inline def isPending(index: Int): Boolean = maybeResult(index).isEmpty
	}

	trait KeyedCapturerArray[+A] extends KeyedObservableStream[A] {
		override def map[B: ClassTag](f: A => B): KeyedCapturerArray[B] = new MappedKeyedCapturerArray(this, (a, up, down) => f(a))

		override def mapWithCoords[B: ClassTag](f: (A, Int, Int) => B): KeyedCapturerArray[B] = new MappedKeyedCapturerArray(this, f)
	}

	// MappedKeyedCapturerArray refactored at the bottom of the file

	sealed trait CapturerArray[+A] extends SettlingArray[A], KeyedCapturerArray[A] {
		override def map[B: ClassTag](f: A => B): CapturerArray[B] = {
			this match {
				case keeper: KeeperArray[A] => keeper.map(f)
				case captor: CaptorArray[A] @unchecked => captor.map(f)
			}
		}

		override def mapWithCoords[B: ClassTag](f: (A, Int, Int) => B): CapturerArray[B] = {
			this match {
				case keeper: KeeperArray[A] => keeper.mapWithCoords(f)
				case captor: CaptorArray[A] @unchecked => captor.mapWithCoords(f)
			}
		}

		@targetName("flatMapCapturer")
		def flatMap[B: ClassTag](f: A => CapturerArray[B]): CapturerMatrix[B]

		@targetName("flatMapCapturerWithCoords")
		def flatMapWithCoords[B: ClassTag](f: (A, Int, Int) => CapturerArray[B]): CapturerMatrix[B]
	}

	final class KeeperArray[A](values: IArray[A]) extends CapturerArray[A] {
		override def maybeResult(index: Int): Maybe[A] = Maybe(values(index))

		override def keyedSubscribe(observer: Observer[A], key: Key): Unit = {
			values.foreachWithIndex((a, index) => observer.onNext(a, 0, index))
			observer.onComplete()
		}

		override def unsubscribe(key: Key): Unit = ()
		override def isSubscribed(key: Key): Boolean = false

		override def map[B: ClassTag](f: A => B): KeeperArray[B] = new KeeperArray[B](values.mapWithIndex { (a, i) => f(a) })

		override def mapWithCoords[B: ClassTag](f: (A, Int, Int) => B): KeeperArray[B] = new KeeperArray[B](values.mapWithIndex { (a, i) => f(a, 0, i) })

		override def flatMap[B: ClassTag](f: A => ObservableStream[B]): ObservableMatrix[B] =
			new DefaultObservableMatrix[B] {
				override def subscribe(observer: Observer[B]): Unit = {
					if values.length == 0 then observer.onComplete()
					else new FlatMapMatrixObserver(values, f, observer).start()
				}
			}

		override def flatMapWithCoords[B: ClassTag](f: (A, Int, Int) => ObservableStream[B]): ObservableMatrix[B] = {
			new DefaultObservableMatrix[B] {
				override def subscribe(observer: Observer[B]): Unit = {
					if values.length == 0 then observer.onComplete()
					else new FlatMapWithCoordsMatrixObserver(values, f, observer).start()
				}
			}
		}

		@targetName("flatMapCapturerWithCoords")
		override def flatMapWithCoords[B: ClassTag](f: (A, Int, Int) => CapturerArray[B]): CapturerMatrix[B] = new FlatMappedWithCoordsCapturerMatrix(this, f)

		@targetName("flatMapCapturer")
		override def flatMap[B: ClassTag](f: A => CapturerArray[B]): CapturerMatrix[B] = new FlatMappedCapturerMatrix(this, f)		
	}

	final class CaptorArray[A](val capturers: IArray[Capturer[A]]) extends CapturerArray[A] { thisCaptorArray =>
		override def maybeResult(index: Int): Maybe[A] = capturers(index).maybeValue

		override def keyedSubscribe(observer: Observer[A], key: Key): Unit = {
			if key != null then unsubscribe(key)
			val size = capturers.length
			if size == 0 then observer.onComplete()
			else {
				var completedCount = 0
				var i = 0
				while i < size do {
					val index = i
					capturers(index).subscribeWithCoords(
						new Observer[A] {
							override def onNext(a: A, up: Int, down: Int): Unit = {
								observer.onNext(a, 0, index)
								completedCount += 1
								if completedCount == size then {
									observer.onComplete()
								}
							}

							override def onError(ex: Throwable): Unit = observer.onError(ex)

							override def onComplete(): Unit = ()
						},
						key,
						0,
						index
					)
					i += 1
				}
			}
		}

		override def unsubscribe(key: Key): Unit = {
			if key != null then {
				var i = 0
				while i < capturers.length do {
					capturers(i).unsubscribe(key)
					i += 1
				}
			}
		}

		override def isSubscribed(key: Key): Boolean =
			key != null && capturers.existsWithIndex((c, _) => c.isSubscribed(key))

		override def map[B: ClassTag](f: A => B): CapturerArray[B] =
			new CaptorArray(capturers.mapWithIndex((capturer, _) => capturer.map(f)))

		override def mapWithCoords[B: ClassTag](f: (A, Int, Int) => B): CapturerArray[B] =
			new CaptorArray(capturers.mapWithIndex((capturer, index) => capturer.map(a => f(a, 0, index))))

		override def flatMap[B: ClassTag](f: A => ObservableStream[B]): ObservableMatrix[B] =
			new DefaultObservableMatrix[B] {
				override def subscribe(observer: Observer[B]): Unit = streamFlatMapSubscribe(thisCaptorArray, f, observer)
			}

		override def flatMapWithCoords[B: ClassTag](f: (A, Int, Int) => ObservableStream[B]): ObservableMatrix[B] =
			new DefaultObservableMatrix[B] {
				override def subscribe(observer: Observer[B]): Unit = streamFlatMapWithCoordsSubscribe(thisCaptorArray, f, observer)
			}

		@targetName("flatMapCapturer")
		override def flatMap[B: ClassTag](f: A => CapturerArray[B]): CapturerMatrix[B] = new FlatMappedCapturerMatrix(this, f)

		@targetName("flatMapCapturerWithCoords")
		override def flatMapWithCoords[B: ClassTag](f: (A, Int, Int) => CapturerArray[B]): CapturerMatrix[B] = new FlatMappedWithCoordsCapturerMatrix(this, f)
	}

	trait KeyedObservableMatrix[+A] extends ObservableMatrix[A] {
		override def subscribe(observer: Observer[A]): Unit = subscribe(observer, null)

		def subscribe(observer: Observer[A], key: Key): Unit

		def subscribe(onNext: (A, Int, Int) => Unit, onError: Throwable => Unit, onComplete: () => Unit, key: Key): Unit = {
			val onNextLocal = onNext
			val onErrorLocal = onError
			val onCompleteLocal = onComplete
			subscribe(
				new Observer[A] {
					override def onNext(value: A, upChain: Int, downChain: Int): Unit = onNextLocal(value, upChain, downChain)

					override def onError(ex: Throwable): Unit = onErrorLocal(ex)

					override def onComplete(): Unit = onCompleteLocal()
				},
				key
			)
		}

		def unsubscribe(key: Key): Unit

		def isSubscribed(key: Key): Boolean
	}

	trait SettlingMatrix[+A] extends KeyedObservableMatrix[A] {
		def maybeResult(outerIndex: Int, innerIndex: Int): Maybe[A]

		inline def isCompleted(outerIndex: Int, innerIndex: Int): Boolean = maybeResult(outerIndex, innerIndex).isDefined

		inline def isPending(outerIndex: Int, innerIndex: Int): Boolean = maybeResult(outerIndex, innerIndex).isEmpty

		override def flattenToInner: KeyedObservableStream[A]

		override def flattenToOuter: KeyedObservableStream[A]

		override def flattenWith(f: (A, Int, Int) => Int): KeyedObservableStream[A]

		override def flattenToSequential: KeyedObservableStream[A]

		override def flattenMap[B: ClassTag](f: (A, Int, Int) => (B, Int)): KeyedObservableStream[B]

		override def flattenMap[B: ClassTag](valueMap: (A, Int, Int) => B, indexMap: (A, B, Int, Int) => Int): KeyedObservableStream[B]

		def flattenFold[B: ClassTag, S](initialState: S)(f: (S, A, Int, Int) => (S, B, Int)): KeyedObservableStream[B]
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

	final class FlatMappedCapturerMatrix[A, B](val source: CapturerArray[A], val f: A => CapturerArray[B]) extends CapturerMatrix[B] {
		private val activeInnerSubscriptions = scala.collection.mutable.Map[Key, List[CapturerArray[B]]]()

		override def maybeResult(outerIndex: Int, innerIndex: Int): Maybe[B] = source.maybeResult(outerIndex).flatMap(a => f(a).maybeResult(innerIndex))

		override def subscribe(observer: Observer[B], key: Key): Unit = {
			if key != null then unsubscribe(key)
			flatMappedMatrixSubscribe(source, (a, _, _) => f(a), activeInnerSubscriptions, observer, key)
		}

		override def unsubscribe(key: Key): Unit = {
			if key != null then {
				source.unsubscribe(key)
				activeInnerSubscriptions.remove(key).foreach { list =>
					list.foreach(_.unsubscribe(key))
				}
			}
		}

		override def isSubscribed(key: Key): Boolean = key != null && source.isSubscribed(key)

		override def flattenToInner: KeyedCapturerArray[B] = new FlattenedToInnerArray(this)

		override def flattenToOuter: KeyedCapturerArray[B] = new FlattenedToOuterArray(this)

		override def flattenWith(f: (B, Int, Int) => Int): KeyedCapturerArray[B] = new FlattenedWithArray(this, f)

		override def flattenToSequential: KeyedCapturerArray[B] = new FlattenedToSequentialArray(this)

		override def flattenMap[C: ClassTag](f: (B, Int, Int) => (C, Int)): KeyedCapturerArray[C] = new FlattenedMapArray(this, f)

		override def flattenMap[C: ClassTag](valueMap: (B, Int, Int) => C, indexMap: (B, C, Int, Int) => Int): KeyedCapturerArray[C] = new FlattenedMapWithCoordsArray[B, C](this, valueMap, indexMap)

		override def flattenFold[C: ClassTag, S](initialState: S)(f: (S, B, Int, Int) => (S, C, Int)): KeyedCapturerArray[C] = new FlattenedFoldArray(this, initialState, f)
	}

	final class FlatMappedWithCoordsCapturerMatrix[A, B](val source: CapturerArray[A], val f: (A, Int, Int) => CapturerArray[B]) extends CapturerMatrix[B] {
		private val activeInnerSubscriptions = scala.collection.mutable.Map[Key, List[CapturerArray[B]]]()

		override def maybeResult(outerIndex: Int, innerIndex: Int): Maybe[B] = source.maybeResult(outerIndex).flatMap(a => f(a, 0, outerIndex).maybeResult(innerIndex))

		override def subscribe(observer: Observer[B], key: Key): Unit = {
			if key != null then unsubscribe(key)
			flatMappedMatrixSubscribe(source, f, activeInnerSubscriptions, observer, key)
		}

		override def unsubscribe(key: Key): Unit = {
			if key != null then {
				source.unsubscribe(key)
				activeInnerSubscriptions.remove(key).foreach { list =>
					list.foreach(_.unsubscribe(key))
				}
			}
		}

		override def isSubscribed(key: Key): Boolean = {
			key != null && source.isSubscribed(key)
		}

		override def flattenToInner: KeyedCapturerArray[B] = new FlattenedToInnerArray(this)

		override def flattenToOuter: KeyedCapturerArray[B] = new FlattenedToOuterArray(this)

		override def flattenWith(f: (B, Int, Int) => Int): KeyedCapturerArray[B] = new FlattenedWithArray(this, f)

		override def flattenToSequential: KeyedCapturerArray[B] = new FlattenedToSequentialArray(this)

		override def flattenMap[C: ClassTag](f: (B, Int, Int) => (C, Int)): KeyedCapturerArray[C] = new FlattenedMapArray(this, f)

		override def flattenMap[C: ClassTag](valueMap: (B, Int, Int) => C, indexMap: (B, C, Int, Int) => Int): KeyedCapturerArray[C] = new FlattenedMapWithCoordsArray[B, C](this, valueMap, indexMap)

		override def flattenFold[C: ClassTag, S](initialState: S)(f: (S, B, Int, Int) => (S, C, Int)): KeyedCapturerArray[C] = new FlattenedFoldArray(this, initialState, f)
	}

	final class FlattenedToInnerArray[+A](val matrix: KeyedObservableMatrix[A]) extends KeyedCapturerArray[A] {
		override def keyedSubscribe(observer: Observer[A], key: Key): Unit = {
			matrix.subscribe(
				new Observer[A] {
					override def onNext(a: A, up: Int, down: Int): Unit = observer.onNext(a, NOT_APPLICABLE_INDEX, down)

					override def onError(ex: Throwable): Unit = observer.onError(ex)

					override def onComplete(): Unit = observer.onComplete()
				},
				key
			)
		}

		override def unsubscribe(key: Key): Unit = matrix.unsubscribe(key)

		override def isSubscribed(key: Key): Boolean = matrix.isSubscribed(key)
	}

	final class FlattenedToOuterArray[+A](val matrix: KeyedObservableMatrix[A]) extends KeyedCapturerArray[A] {
		override def keyedSubscribe(observer: Observer[A], key: Key): Unit =
			matrix.subscribe(
				new Observer[A] {
					override def onNext(a: A, up: Int, down: Int): Unit = observer.onNext(a, NOT_APPLICABLE_INDEX, up)

					override def onError(ex: Throwable): Unit = observer.onError(ex)

					override def onComplete(): Unit = observer.onComplete()
				},
				key
			)

		override def unsubscribe(key: Key): Unit = matrix.unsubscribe(key)

		override def isSubscribed(key: Key): Boolean = matrix.isSubscribed(key)
	}

	final class FlattenedWithArray[A](val matrix: KeyedObservableMatrix[A], val f: (A, Int, Int) => Int) extends KeyedCapturerArray[A] {
		override def keyedSubscribe(observer: Observer[A], key: Key): Unit = {
			matrix.subscribe(
				new Observer[A] {
					override def onNext(a: A, up: Int, down: Int): Unit = observer.onNext(a, NOT_APPLICABLE_INDEX, f(a, up, down))

					override def onError(ex: Throwable): Unit = observer.onError(ex)

					override def onComplete(): Unit = observer.onComplete()
				},
				key
			)
		}

		override def unsubscribe(key: Key): Unit = matrix.unsubscribe(key)

		override def isSubscribed(key: Key): Boolean = matrix.isSubscribed(key)
	}

	final class FlattenedToSequentialArray[+A](val matrix: KeyedObservableMatrix[A]) extends KeyedCapturerArray[A] {
		override def keyedSubscribe(observer: Observer[A], key: Key): Unit = {
			var counter = 0
			matrix.subscribe(
				new Observer[A] {
					override def onNext(a: A, up: Int, down: Int): Unit = {
						val index = counter
						counter += 1
						observer.onNext(a, NOT_APPLICABLE_INDEX, index)
					}

					override def onError(ex: Throwable): Unit = observer.onError(ex)

					override def onComplete(): Unit = observer.onComplete()
				},
				key
			)
		}

		override def unsubscribe(key: Key): Unit = matrix.unsubscribe(key)

		override def isSubscribed(key: Key): Boolean = matrix.isSubscribed(key)
	}

	final class FlattenedMapArray[A, +B](val matrix: KeyedObservableMatrix[A], val f: (A, Int, Int) => (B, Int)) extends KeyedCapturerArray[B] {
		override def keyedSubscribe(observer: Observer[B], key: Key): Unit = {
			matrix.subscribe(new Observer[A] {
				override def onNext(a: A, up: Int, down: Int): Unit = {
					val (b, index) = f(a, up, down)
					observer.onNext(b, NOT_APPLICABLE_INDEX, index)
				}

				override def onError(ex: Throwable): Unit = observer.onError(ex)

				override def onComplete(): Unit = observer.onComplete()
			}, key)
		}

		override def unsubscribe(key: Key): Unit = matrix.unsubscribe(key)

		override def isSubscribed(key: Key): Boolean = matrix.isSubscribed(key)
	}

	final class FlattenedMapWithCoordsArray[A, B](val matrix: KeyedObservableMatrix[A], val valueMap: (A, Int, Int) => B, val indexMap: (A, B, Int, Int) => Int) extends KeyedCapturerArray[B] {
		override def keyedSubscribe(observer: Observer[B], key: Key): Unit = {
			matrix.subscribe(
				new Observer[A] {
					override def onNext(a: A, up: Int, down: Int): Unit = {
						val b = valueMap(a, up, down)
						observer.onNext(b, NOT_APPLICABLE_INDEX, indexMap(a, b, up, down))
					}

					override def onError(ex: Throwable): Unit = observer.onError(ex)

					override def onComplete(): Unit = observer.onComplete()
				},
				key
			)
		}

		override def unsubscribe(key: Key): Unit = matrix.unsubscribe(key)

		override def isSubscribed(key: Key): Boolean = matrix.isSubscribed(key)
	}

	final class FlattenedFoldArray[A, B, S](val matrix: KeyedObservableMatrix[A], val initialState: S, val f: (S, A, Int, Int) => (S, B, Int)) extends KeyedCapturerArray[B] {
		override def keyedSubscribe(observer: Observer[B], key: Key): Unit = {
			var state = initialState
			matrix.subscribe(
				new Observer[A] {
					override def onNext(a: A, up: Int, down: Int): Unit = {
						val (newState, b, index) = f(state, a, up, down)
						state = newState
						observer.onNext(b, NOT_APPLICABLE_INDEX, index)
					}

					override def onError(ex: Throwable): Unit = observer.onError(ex)

					override def onComplete(): Unit = observer.onComplete()
				},
				key
			)
		}

		override def unsubscribe(key: Key): Unit = matrix.unsubscribe(key)

		override def isSubscribed(key: Key): Boolean = matrix.isSubscribed(key)
	}

	private final class FlatMapMatrixObserver[A, B](values: IArray[A], f: A => ObservableStream[B], observer: Observer[B]) {
		private val size = values.length
		private var completedCount = 0
		private var errorFired = false

		def fireError(ex: Throwable): Unit = {
			if !errorFired then {
				errorFired = true
				observer.onError(ex)
			}
		}

		def start(): Unit = {
			values.foreachWithIndex { (a, outerIndex) =>
				new ElementSubscription(outerIndex).start(a)
			}
		}

		private final class ElementSubscription(outerIndex: Int) extends Observer[B] {
			def start(a: A): Unit = f(a).subscribe(this)

			override def onNext(b: B, innerOuter: Int, innerInner: Int): Unit = observer.onNext(b, outerIndex, innerInner)

			override def onError(ex: Throwable): Unit = fireError(ex)

			override def onComplete(): Unit = {
				completedCount += 1
				if completedCount == size && !errorFired then {
					observer.onComplete()
				}
			}
		}
	}

	private final class FlatMapWithCoordsMatrixObserver[A, B](values: IArray[A], f: (A, Int, Int) => ObservableStream[B], observer: Observer[B]) {
		private val size = values.length
		private var completedCount = 0
		private var errorFired = false

		def fireError(ex: Throwable): Unit = {
			if !errorFired then {
				errorFired = true
				observer.onError(ex)
			}
		}

		def start(): Unit = {
			values.foreachWithIndex { (a, outerIndex) =>
				new ElementSubscription(outerIndex).start(a)
			}
		}

		private final class ElementSubscription(outerIndex: Int) extends Observer[B] {
			def start(a: A): Unit = f(a, 0, outerIndex).subscribe(this)

			override def onNext(b: B, innerOuter: Int, innerInner: Int): Unit = observer.onNext(b, outerIndex, innerInner)

			override def onError(ex: Throwable): Unit = fireError(ex)

			override def onComplete(): Unit = {
				completedCount += 1
				if completedCount == size && !errorFired then {
					observer.onComplete()
				}
			}
		}
	}

	private final class FlatMapObserver[A, B](f: A => ObservableStream[B], observer: Observer[B]) extends Observer[A] {
		private var outerCompleted = false
		private var activeInnerCount = 0
		private var errorFired = false

		override def onError(ex: Throwable): Unit = {
			if !errorFired then {
				errorFired = true
				observer.onError(ex)
			}
		}

		override def onComplete(): Unit = {
			outerCompleted = true
			tryComplete()
		}

		override def onNext(a: A, outerOuter: Int, outerDown: Int): Unit = {
			activeInnerCount += 1
			f(a).subscribe(new Observer[B] {
				override def onNext(b: B, innerUp: Int, innerDown: Int): Unit = observer.onNext(b, outerDown, innerDown)

				override def onError(ex: Throwable): Unit = FlatMapObserver.this.onError(ex)

				override def onComplete(): Unit = {
					activeInnerCount -= 1
					tryComplete()
				}
			})
		}

		inline def tryComplete(): Unit = {
			if outerCompleted && activeInnerCount == 0 && !errorFired then observer.onComplete()
		}
	}

	private inline def streamFlatMapSubscribe[A, B](array: ObservableStream[A], inline f: A => ObservableStream[B], observer: Observer[B]): Unit = {
		array.subscribe(new FlatMapObserver(f, observer))
	}

	private final class FlatMapWithCoordsObserver[A, B](f: (A, Int, Int) => ObservableStream[B], observer: Observer[B]) extends Observer[A] {
		private var outerCompleted = false
		private var activeInnerCount = 0
		private var errorFired = false

		override def onError(ex: Throwable): Unit = {
			if !errorFired then {
				errorFired = true
				observer.onError(ex)
			}
		}

		override def onComplete(): Unit = {
			outerCompleted = true
			tryComplete()
		}

		override def onNext(a: A, outerOuter: Int, outerDown: Int): Unit = {
			activeInnerCount += 1
			f(a, outerOuter, outerDown).subscribe(new Observer[B] {
				override def onNext(b: B, innerUp: Int, innerDown: Int): Unit = {
					observer.onNext(b, outerDown, innerDown)
				}

				override def onError(ex: Throwable): Unit = {
					FlatMapWithCoordsObserver.this.onError(ex)
				}

				override def onComplete(): Unit = {
					activeInnerCount -= 1
					tryComplete()
				}
			})
		}

		inline def tryComplete(): Unit = {
			if outerCompleted && activeInnerCount == 0 && !errorFired then {
				observer.onComplete()
			}
		}
	}

	private inline def streamFlatMapWithCoordsSubscribe[A, B](array: ObservableStream[A], inline f: (A, Int, Int) => ObservableStream[B], observer: Observer[B]): Unit = {
		array.subscribe(new FlatMapWithCoordsObserver(f, observer))
	}

	private final class SequentialConsumer[A](observer: Observer[A]) extends Observer[A] {
		private var counter = 0

		override def onNext(a: A, up: Int, down: Int): Unit = {
			val index = counter
			counter += 1
			observer.onNext(a, NOT_APPLICABLE_INDEX, index)
		}

		override def onError(ex: Throwable): Unit = observer.onError(ex)

		override def onComplete(): Unit = observer.onComplete()
	}

	private inline def flattenToSequentialSubscribe[A](matrix: ObservableMatrix[A], observer: Observer[A]): Unit = {
		matrix.subscribe(new SequentialConsumer(observer))
	}

	private inline def flattenMapSubscribe[A, B](matrix: ObservableMatrix[A], f: (A, Int, Int) => (B, Int), observer: Observer[B]): Unit = {
		matrix.subscribe(new Observer[A] {
			override def onNext(a: A, up: Int, down: Int): Unit = {
				val (b, index) = f(a, up, down)
				observer.onNext(b, NOT_APPLICABLE_INDEX, index)
			}

			override def onError(ex: Throwable): Unit = observer.onError(ex)

			override def onComplete(): Unit = observer.onComplete()
		})
	}

	private inline def flattenMapWithCoordsSubscribe[A, B](matrix: ObservableMatrix[A], valueMap: (A, Int, Int) => B, indexMap: (A, B, Int, Int) => Int, observer: Observer[B]): Unit = {
		matrix.subscribe(new Observer[A] {
			override def onNext(a: A, up: Int, down: Int): Unit = {
				val b = valueMap(a, up, down)
				observer.onNext(b, NOT_APPLICABLE_INDEX, indexMap(a, b, up, down))
			}

			override def onError(ex: Throwable): Unit = observer.onError(ex)

			override def onComplete(): Unit = observer.onComplete()
		})
	}

	private final class FoldConsumer[A, B, S](initialState: S, f: (S, A, Int, Int) => (S, B, Int), observer: Observer[B]) extends Observer[A] {
		private var state = initialState

		override def onNext(a: A, up: Int, down: Int): Unit = {
			val (newState, b, index) = f(state, a, up, down)
			state = newState
			observer.onNext(b, NOT_APPLICABLE_INDEX, index)
		}

		override def onError(ex: Throwable): Unit = observer.onError(ex)

		override def onComplete(): Unit = observer.onComplete()
	}

	private inline def flattenFoldSubscribe[A, B, S](matrix: ObservableMatrix[A], initialState: S, f: (S, A, Int, Int) => (S, B, Int), observer: Observer[B]): Unit =
		matrix.subscribe(new FoldConsumer(initialState, f, observer))

	private final class FlatMappedMatrixObserver[A, B](getInner: (A, Int, Int) => CapturerArray[B], activeInnerSubscriptions: scala.collection.mutable.Map[Key, List[CapturerArray[B]]], observer: Observer[B], key: Key) extends Observer[A] {
		private var outerCompleted = false
		private var activeInnerCount = 0
		private var errorFired = false

		def tryComplete(): Unit = {
			if outerCompleted && activeInnerCount == 0 && !errorFired then observer.onComplete()
		}

		override def onError(ex: Throwable): Unit = {
			if !errorFired then {
				errorFired = true
				observer.onError(ex)
			}
		}

		override def onComplete(): Unit = {
			outerCompleted = true
			tryComplete()
		}

		override def onNext(a: A, outerOuter: Int, outerInner: Int): Unit = {
			val inner = getInner(a, outerOuter, outerInner)
			if key != null then {
				activeInnerSubscriptions.updateWith(key) {
					case Some(list) => Some(inner :: list)
					case None => Some(inner :: Nil)
				}
			}
			activeInnerCount += 1
			inner.keyedSubscribe(new Observer[B] {
				override def onNext(b: B, innerOuter: Int, innerInner: Int): Unit = observer.onNext(b, outerInner, innerInner)

				override def onError(ex: Throwable): Unit = FlatMappedMatrixObserver.this.onError(ex)

				override def onComplete(): Unit = {
					activeInnerCount -= 1
					tryComplete()
				}
			}, key)
		}
	}

	private inline def flatMappedMatrixSubscribe[A, B](source: CapturerArray[A], inline getInner: (A, Int, Int) => CapturerArray[B], activeInnerSubscriptions: scala.collection.mutable.Map[Key, List[CapturerArray[B]]], observer: Observer[B], key: Key): Unit = {
		if key != null then activeInnerSubscriptions(key) = Nil
		val sub = new FlatMappedMatrixObserver(getInner, activeInnerSubscriptions, observer, key)
		source.keyedSubscribe(sub, key)
	}

	private final class ZipObservation[A, B, C](left: ObservableStream[A], right: ObservableStream[B], f: (A, B) => C, observer: Observer[C]) {
		val leftValues = scala.collection.mutable.Map[Int, A]()
		val rightValues = scala.collection.mutable.Map[Int, B]()
		var leftCompleted = false
		var rightCompleted = false
		var errorFired = false

		def checkComplete(): Unit = {
			if (leftCompleted && leftValues.isEmpty) || (rightCompleted && rightValues.isEmpty) || (leftCompleted && rightCompleted) then observer.onComplete()
		}

		def fireError(ex: Throwable): Unit = {
			if !errorFired then {
				errorFired = true
				observer.onError(ex)
			}
		}

		def start(): Unit = {
			left.subscribe(new LeftObserver)
			right.subscribe(new RightObserver)
		}

		private final class LeftObserver extends Observer[A] {
			override def onNext(a: A, up: Int, down: Int): Unit = {
				rightValues.remove(down) match {
					case Some(b) =>
						observer.onNext(f(a, b), up, down)
						checkComplete()
					case None =>
						leftValues(down) = a
				}
			}

			override def onError(ex: Throwable): Unit = fireError(ex)

			override def onComplete(): Unit = {
				leftCompleted = true
				checkComplete()
			}
		}

		private final class RightObserver extends Observer[B] {
			override def onNext(b: B, up: Int, down: Int): Unit = {
				leftValues.remove(down) match {
					case Some(a) =>
						observer.onNext(f(a, b), up, down)
						checkComplete()
					case None =>
						rightValues(down) = b
				}
			}

			override def onError(ex: Throwable): Unit = fireError(ex)

			override def onComplete(): Unit = {
				rightCompleted = true
				checkComplete()
			}
		}
	}

	private final class ZippedObservableStream[A, B, C](val left: ObservableStream[A], val right: ObservableStream[B], val f: (A, B) => C) extends DefaultObservableStream[C] {
		override def subscribe(observer: Observer[C]): Unit = {
			new ZipObservation(left, right, f, observer).start()
		}
	}

	private final class ZippedTaskArray[A, B, C](val left: TaskArray[A], val right: TaskArray[B], val f: (A, B) => C) extends DefaultTaskArray[C] {
		override def subscribe(observer: Observer[C]): Unit = {
			new ZipObservation(left, right, f, observer).start()
		}
	}

	private final class TakeConsumer[A](n: Int, observer: Observer[A]) extends Observer[A] {
		private var count = 0
		private var active = true

		override def onNext(a: A, up: Int, down: Int): Unit = {
			if active then {
				if count < n then {
					val currentCount = count
					count += 1
					observer.onNext(a, up, currentCount)
					if count == n then {
						active = false
						observer.onComplete()
					}
				}
			}
		}

		override def onError(ex: Throwable): Unit = {
			if active then {
				active = false
				observer.onError(ex)
			}
		}

		override def onComplete(): Unit = {
			if active then {
				active = false
				observer.onComplete()
			}
		}
	}

	private final class TakeWhileConsumer[A](p: A => Boolean, observer: Observer[A]) extends Observer[A] {
		private var active = true
		private var counter = 0

		override def onNext(a: A, up: Int, down: Int): Unit = {
			if active then {
				if p(a) then {
					val currentCounter = counter
					counter += 1
					observer.onNext(a, up, currentCounter)
				} else {
					active = false
					observer.onComplete()
				}
			}
		}

		override def onError(ex: Throwable): Unit = {
			if active then {
				active = false
				observer.onError(ex)
			}
		}

		override def onComplete(): Unit = {
			if active then {
				active = false
				observer.onComplete()
			}
		}
	}

	private final class BufferedConsumer[A, T >: A : ClassTag](size: Int, observer: Observer[IArray[T]]) extends Observer[A] {
		private var buffer = new Array[T](size)
		private var count = 0
		private var chunkIndex = 0
		private var active = true

		override def onNext(a: A, up: Int, down: Int): Unit = {
			if active then {
				buffer(count) = a
				count += 1
				if count == size then {
					val chunk = IArray.unsafeFromArray(buffer)
					buffer = new Array[T](size)
					count = 0
					val idx = chunkIndex
					chunkIndex += 1
					observer.onNext(chunk, NOT_APPLICABLE_INDEX, idx)
				}
			}
		}

		override def onError(ex: Throwable): Unit = {
			if active then {
				active = false
				observer.onError(ex)
			}
		}

		override def onComplete(): Unit = {
			if active then {
				active = false
				if count > 0 then {
					val partial = IArray.unsafeFromArray(buffer.take(count))
					observer.onNext(partial, NOT_APPLICABLE_INDEX, chunkIndex)
				}
				observer.onComplete()
			}
		}
	}

	private final class ScanConsumer[A, B](initial: B, f: (B, A) => B, observer: Observer[B]) extends Observer[A] {
		private var state = initial

		override def onNext(a: A, upChain: Int, downChain: Int): Unit = {
			state = f(state, a)
			observer.onNext(state, upChain, downChain)
		}

		override def onError(ex: Throwable): Unit = observer.onError(ex)

		override def onComplete(): Unit = observer.onComplete()
	}

	final class StreamEmitter[A] extends DefaultObservableStream[A] {
		private var observers: List[Observer[A]] = Nil
		private var counter = 0
		private var completed = false
		private var error: Throwable | Null = null

		override def subscribe(observer: Observer[A]): Unit = {
			if error != null then observer.onError(error.asInstanceOf[Throwable])
			else if completed then observer.onComplete()
			else observers = observer :: observers
		}

		def emit(value: A): Unit = {
			if !completed && error == null then {
				val idx = counter
				counter += 1
				observers.foreach(_.onNext(value, NOT_APPLICABLE_INDEX, idx))
			}
		}

		def fail(ex: Throwable): Unit = {
			if !completed && error == null then {
				error = ex
				observers.foreach(_.onError(ex))
				observers = Nil
			}
		}

		def end(): Unit = {
			if !completed && error == null then {
				completed = true
				observers.foreach(_.onComplete())
				observers = Nil
			}
		}
	}

	// ===================================================================
	// ==================== CHAIN SUPPORT PRIMITIVES =====================
	// ===================================================================

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
					if p(node.value) then {
						new Keeper(Maybe(ChainNode(node.value, node.next.filter(p))))
					} else {
						node.next.filter(p).asCapturer
					}
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

	// ===================================================================
	// ========== OPTIMIZED CONCRETE CLASSES (SINGLE-SLOT CACHING) =======
	// ===================================================================

	trait SingleSlotObservableStream[A, +B] extends DefaultObservableStream[B] with Observer[A] {
		protected val source: ObservableStream[A]

		private var downstreamObserver: Observer[B] @uncheckedVariance = uninitialized

		override def subscribe(observer: Observer[B]): Unit = {
			if downstreamObserver == null then {
				this.downstreamObserver = observer
				source.subscribe(this)
			} else {
				source.subscribe(createDelegate(observer))
			}
		}

		protected def createDelegate(observer: Observer[B]): Observer[A]

		protected def resetState(): Unit = ()

		protected def forwardNext(value: B @uncheckedVariance, up: Int, down: Int): Unit = {
			val obs = downstreamObserver
			if obs != null then obs.onNext(value, up, down)
		}

		protected def forwardError(ex: Throwable): Unit = {
			val obs = downstreamObserver
			downstreamObserver = null
			resetState()
			if obs != null then obs.onError(ex)
		}

		protected def forwardComplete(): Unit = {
			val obs = downstreamObserver
			downstreamObserver = null
			resetState()
			if obs != null then obs.onComplete()
		}
	}

	trait SingleSlotObservableMatrixStream[A, +B] extends DefaultObservableStream[B] with Observer[A] {
		protected val source: ObservableMatrix[A]

		private var downstreamObserver: Observer[B] @uncheckedVariance = uninitialized

		override def subscribe(observer: Observer[B]): Unit = {
			if downstreamObserver == null then {
				this.downstreamObserver = observer
				source.subscribe(this)
			} else {
				source.subscribe(createDelegate(observer))
			}
		}

		protected def createDelegate(observer: Observer[B]): Observer[A]

		protected def resetState(): Unit = ()

		protected def forwardNext(value: B @uncheckedVariance, up: Int, down: Int): Unit = {
			val obs = downstreamObserver
			if obs != null then obs.onNext(value, up, down)
		}

		protected def forwardError(ex: Throwable): Unit = {
			val obs = downstreamObserver
			downstreamObserver = null
			resetState()
			if obs != null then obs.onError(ex)
		}

		protected def forwardComplete(): Unit = {
			val obs = downstreamObserver
			downstreamObserver = null
			resetState()
			if obs != null then obs.onComplete()
		}
	}

	trait SingleSlotKeyedStream[A, +B] extends KeyedObservableStream[B] with Observer[A] {
		protected val source: KeyedObservableStream[A]

		private var downstreamObserver: Observer[B] @uncheckedVariance = uninitialized
		private var activeKey: Key = uninitialized

		override def keyedSubscribe(observer: Observer[B], key: Key): Unit = {
			if downstreamObserver == null then {
				this.downstreamObserver = observer
				this.activeKey = key
				source.keyedSubscribe(this, key)
			} else {
				source.keyedSubscribe(createDelegate(observer), key)
			}
		}

		protected def createDelegate(observer: Observer[B]): Observer[A]

		protected def resetState(): Unit = ()

		protected def forwardNext(value: B @uncheckedVariance, up: Int, down: Int): Unit = {
			val obs = downstreamObserver
			if obs != null then obs.onNext(value, up, down)
		}

		protected def forwardError(ex: Throwable): Unit = {
			val obs = downstreamObserver
			clearSlot()
			if obs != null then obs.onError(ex)
		}

		protected def forwardComplete(): Unit = {
			val obs = downstreamObserver
			clearSlot()
			if obs != null then obs.onComplete()
		}

		override def unsubscribe(key: Key): Unit = {
			if key != null && key == activeKey then clearSlot()
			source.unsubscribe(key)
		}

		override def isSubscribed(key: Key): Boolean = {
			(key != null && key == activeKey) || source.isSubscribed(key)
		}

		private def clearSlot(): Unit = {
			downstreamObserver = null
			activeKey = null
			resetState()
		}
	}

	final class MappedKeyedCapturerArray[A, +B](val source: KeyedCapturerArray[A], val f: (A, Int, Int) => B)
		extends SingleSlotKeyedStream[A, B] with KeyedCapturerArray[B] {

		override def onNext(a: A, upChain: Int, downChain: Int): Unit = {
			forwardNext(f(a, upChain, downChain), upChain, downChain)
		}

		override def onError(ex: Throwable): Unit = forwardError(ex)

		override def onComplete(): Unit = forwardComplete()

		override protected def createDelegate(observer: Observer[B]): Observer[A] = new Observer[A] {
			override def onNext(a: A, upChain: Int, downChain: Int): Unit = observer.onNext(f(a, upChain, downChain), upChain, downChain)

			override def onError(ex: Throwable): Unit = observer.onError(ex)

			override def onComplete(): Unit = observer.onComplete()
		}
	}

	final class MappedObservableStream[A, B](val source: ObservableStream[A], val f: A => B) extends SingleSlotObservableStream[A, B] {
		override def onNext(a: A, up: Int, down: Int): Unit = forwardNext(f(a), up, down)

		override def onError(ex: Throwable): Unit = forwardError(ex)

		override def onComplete(): Unit = forwardComplete()

		override protected def createDelegate(observer: Observer[B]): Observer[A] = new Observer[A] {
			override def onNext(a: A, up: Int, down: Int): Unit = observer.onNext(f(a), up, down)

			override def onError(ex: Throwable): Unit = observer.onError(ex)

			override def onComplete(): Unit = observer.onComplete()
		}
	}

	final class MappedWithCoordsObservableStream[A, B](val source: ObservableStream[A], val f: (A, Int, Int) => B) extends SingleSlotObservableStream[A, B] {
		override def onNext(a: A, up: Int, down: Int): Unit = forwardNext(f(a, up, down), up, down)

		override def onError(ex: Throwable): Unit = forwardError(ex)

		override def onComplete(): Unit = forwardComplete()

		override protected def createDelegate(observer: Observer[B]): Observer[A] = new Observer[A] {
			override def onNext(a: A, up: Int, down: Int): Unit = observer.onNext(f(a, up, down), up, down)

			override def onError(ex: Throwable): Unit = observer.onError(ex)

			override def onComplete(): Unit = observer.onComplete()
		}
	}

	final class FlatMappedObservableMatrix[A, B](val source: ObservableStream[A], val f: A => ObservableStream[B])
		extends DefaultObservableMatrix[B] with Observer[A] {

		private var downstreamObserver: Observer[B] = uninitialized
		private var outerCompleted = false
		private var activeInnerCount = 0
		private var errorFired = false

		override def subscribe(observer: Observer[B]): Unit = {
			if downstreamObserver == null then {
				this.downstreamObserver = observer
				source.subscribe(this)
			} else {
				source.subscribe(new FlatMapObserver(f, observer))
			}
		}

		private def resetState(): Unit = {
			outerCompleted = false
			activeInnerCount = 0
			errorFired = false
		}

		private def tryComplete(): Unit = {
			if outerCompleted && activeInnerCount == 0 && !errorFired then {
				val obs = downstreamObserver
				downstreamObserver = null
				resetState()
				if obs != null then obs.onComplete()
			}
		}

		override def onNext(a: A, outerOuter: Int, outerDown: Int): Unit = {
			activeInnerCount += 1
			f(a).subscribe(new InnerObserver(outerDown))
		}

		override def onError(ex: Throwable): Unit = {
			if !errorFired then {
				errorFired = true
				val obs = downstreamObserver
				downstreamObserver = null
				resetState()
				if obs != null then obs.onError(ex)
			}
		}

		override def onComplete(): Unit = {
			outerCompleted = true
			tryComplete()
		}

		private final class InnerObserver(outerDown: Int) extends Observer[B] {
			override def onNext(b: B, innerUp: Int, innerDown: Int): Unit = {
				val obs = downstreamObserver
				if obs != null then obs.onNext(b, outerDown, innerDown)
			}

			override def onError(ex: Throwable): Unit = FlatMappedObservableMatrix.this.onError(ex)

			override def onComplete(): Unit = {
				activeInnerCount -= 1
				tryComplete()
			}
		}
	}

	final class FlatMappedWithCoordsObservableMatrix[A, B](val source: ObservableStream[A], val f: (A, Int, Int) => ObservableStream[B])
		extends DefaultObservableMatrix[B] with Observer[A] {

		private var downstreamObserver: Observer[B] = uninitialized
		private var outerCompleted = false
		private var activeInnerCount = 0
		private var errorFired = false

		override def subscribe(observer: Observer[B]): Unit = {
			if downstreamObserver == null then {
				this.downstreamObserver = observer
				source.subscribe(this)
			} else {
				source.subscribe(new FlatMapWithCoordsObserver(f, observer))
			}
		}

		private def resetState(): Unit = {
			outerCompleted = false
			activeInnerCount = 0
			errorFired = false
		}

		private def tryComplete(): Unit = {
			if outerCompleted && activeInnerCount == 0 && !errorFired then {
				val obs = downstreamObserver
				downstreamObserver = null
				resetState()
				if obs != null then obs.onComplete()
			}
		}

		override def onNext(a: A, outerOuter: Int, outerDown: Int): Unit = {
			activeInnerCount += 1
			f(a, outerOuter, outerDown).subscribe(new InnerObserver(outerDown))
		}

		override def onError(ex: Throwable): Unit = {
			if !errorFired then {
				errorFired = true
				val obs = downstreamObserver
				downstreamObserver = null
				resetState()
				if obs != null then obs.onError(ex)
			}
		}

		override def onComplete(): Unit = {
			outerCompleted = true
			tryComplete()
		}

		private final class InnerObserver(outerDown: Int) extends Observer[B] {
			override def onNext(b: B, innerUp: Int, innerDown: Int): Unit = {
				val obs = downstreamObserver
				if obs != null then obs.onNext(b, outerDown, innerDown)
			}

			override def onError(ex: Throwable): Unit = FlatMappedWithCoordsObservableMatrix.this.onError(ex)

			override def onComplete(): Unit = {
				activeInnerCount -= 1
				tryComplete()
			}
		}
	}

	final class ScannedObservableStream[A, B](val source: ObservableStream[A], val initial: B, val f: (B, A) => B)
		extends SingleSlotObservableStream[A, B] {

		private var state = initial

		override protected def resetState(): Unit = state = initial

		override def onNext(a: A, upChain: Int, downChain: Int): Unit = {
			state = f(state, a)
			forwardNext(state, upChain, downChain)
		}

		override def onError(ex: Throwable): Unit = forwardError(ex)

		override def onComplete(): Unit = forwardComplete()

		override protected def createDelegate(observer: Observer[B]): Observer[A] = new ScanConsumer(initial, f, observer)
	}

	final class BufferedObservableStream[A, T >: A : ClassTag](val source: ObservableStream[A], val size: Int)
		extends SingleSlotObservableStream[A, IArray[T]] {

		private var buffer = new Array[T](size)
		private var count = 0
		private var chunkIndex = 0
		private var active = true

		override protected def resetState(): Unit = {
			count = 0
			chunkIndex = 0
			active = true
		}

		override def onNext(a: A, up: Int, down: Int): Unit = {
			if active then {
				buffer(count) = a
				count += 1
				if count == size then {
					val chunk = IArray.unsafeFromArray(buffer)
					buffer = new Array[T](size)
					count = 0
					val idx = chunkIndex
					chunkIndex += 1
					forwardNext(chunk, NOT_APPLICABLE_INDEX, idx)
				}
			}
		}

		override def onError(ex: Throwable): Unit = {
			if active then {
				active = false
				forwardError(ex)
			}
		}

		override def onComplete(): Unit = {
			if active then {
				active = false
				if count > 0 then {
					val partial = IArray.unsafeFromArray(buffer.take(count))
					forwardNext(partial, NOT_APPLICABLE_INDEX, chunkIndex)
				}
				forwardComplete()
			}
		}

		override protected def createDelegate(observer: Observer[IArray[T]]): Observer[A] = new BufferedConsumer[A, T](size, observer)
	}

	final class TakeObservableStream[A](val source: ObservableStream[A], val n: Int) extends SingleSlotObservableStream[A, A] {
		private var count = 0
		private var active = true

		override protected def resetState(): Unit = {
			count = 0
			active = true
		}

		override def onNext(a: A, up: Int, down: Int): Unit = {
			if active then {
				if count < n then {
					val currentCount = count
					count += 1
					forwardNext(a, up, currentCount)
					if count == n then {
						active = false
						forwardComplete()
					}
				}
			}
		}

		override def onError(ex: Throwable): Unit = {
			if active then {
				active = false
				forwardError(ex)
			}
		}

		override def onComplete(): Unit = {
			if active then {
				active = false
				forwardComplete()
			}
		}

		override protected def createDelegate(observer: Observer[A]): Observer[A] = new TakeConsumer(n, observer)
	}

	final class TakeWhileObservableStream[A](val source: ObservableStream[A], val p: A => Boolean) extends SingleSlotObservableStream[A, A] {
		private var active = true
		private var counter = 0

		override protected def resetState(): Unit = {
			active = true
			counter = 0
		}

		override def onNext(a: A, up: Int, down: Int): Unit = {
			if active then {
				if p(a) then {
					val currentCounter = counter
					counter += 1
					forwardNext(a, up, currentCounter)
				} else {
					active = false
					forwardComplete()
				}
			}
		}

		override def onError(ex: Throwable): Unit = {
			if active then {
				active = false
				forwardError(ex)
			}
		}

		override def onComplete(): Unit = {
			if active then {
				active = false
				forwardComplete()
			}
		}

		override protected def createDelegate(observer: Observer[A]): Observer[A] = new TakeWhileConsumer(p, observer)
	}

	final class FlattenedToInnerStream[A](val source: ObservableMatrix[A]) extends SingleSlotObservableMatrixStream[A, A] {
		override def onNext(a: A, upChain: Int, downChain: Int): Unit = forwardNext(a, NOT_APPLICABLE_INDEX, downChain)

		override def onError(ex: Throwable): Unit = forwardError(ex)

		override def onComplete(): Unit = forwardComplete()

		override protected def createDelegate(observer: Observer[A]): Observer[A] = new Observer[A] {
			override def onNext(a: A, upChain: Int, downChain: Int): Unit = observer.onNext(a, NOT_APPLICABLE_INDEX, downChain)

			override def onError(ex: Throwable): Unit = observer.onError(ex)

			override def onComplete(): Unit = observer.onComplete()
		}
	}

	final class FlattenedToOuterStream[A](val source: ObservableMatrix[A]) extends SingleSlotObservableMatrixStream[A, A] {
		override def onNext(a: A, upChain: Int, downChain: Int): Unit = forwardNext(a, NOT_APPLICABLE_INDEX, upChain)

		override def onError(ex: Throwable): Unit = forwardError(ex)

		override def onComplete(): Unit = forwardComplete()

		override protected def createDelegate(observer: Observer[A]): Observer[A] = new Observer[A] {
			override def onNext(a: A, upChain: Int, downChain: Int): Unit = observer.onNext(a, NOT_APPLICABLE_INDEX, upChain)

			override def onError(ex: Throwable): Unit = observer.onError(ex)

			override def onComplete(): Unit = observer.onComplete()
		}
	}

	final class FlattenedWithStream[A](val source: ObservableMatrix[A], val f: (A, Int, Int) => Int) extends SingleSlotObservableMatrixStream[A, A] {
		override def onNext(a: A, upChain: Int, downChain: Int): Unit = forwardNext(a, NOT_APPLICABLE_INDEX, f(a, upChain, downChain))

		override def onError(ex: Throwable): Unit = forwardError(ex)

		override def onComplete(): Unit = forwardComplete()

		override protected def createDelegate(observer: Observer[A]): Observer[A] = new Observer[A] {
			override def onNext(a: A, upChain: Int, downChain: Int): Unit = observer.onNext(a, NOT_APPLICABLE_INDEX, f(a, upChain, downChain))

			override def onError(ex: Throwable): Unit = observer.onError(ex)

			override def onComplete(): Unit = observer.onComplete()
		}
	}

	final class FlattenedToSequentialStream[A](val source: ObservableMatrix[A]) extends SingleSlotObservableMatrixStream[A, A] {
		private var counter = 0

		override protected def resetState(): Unit = counter = 0

		override def onNext(a: A, up: Int, down: Int): Unit = {
			val idx = counter
			counter += 1
			forwardNext(a, NOT_APPLICABLE_INDEX, idx)
		}

		override def onError(ex: Throwable): Unit = forwardError(ex)

		override def onComplete(): Unit = forwardComplete()

		override protected def createDelegate(observer: Observer[A]): Observer[A] = new SequentialConsumer(observer)
	}

	final class FlattenedMapStream[A, B](val source: ObservableMatrix[A], val f: (A, Int, Int) => (B, Int)) extends SingleSlotObservableMatrixStream[A, B] {
		override def onNext(a: A, up: Int, down: Int): Unit = {
			val (b, index) = f(a, up, down)
			forwardNext(b, NOT_APPLICABLE_INDEX, index)
		}

		override def onError(ex: Throwable): Unit = forwardError(ex)

		override def onComplete(): Unit = forwardComplete()

		override protected def createDelegate(observer: Observer[B]): Observer[A] = new Observer[A] {
			override def onNext(a: A, up: Int, down: Int): Unit = {
				val (b, index) = f(a, up, down)
				observer.onNext(b, NOT_APPLICABLE_INDEX, index)
			}

			override def onError(ex: Throwable): Unit = observer.onError(ex)

			override def onComplete(): Unit = observer.onComplete()
		}
	}

	final class FlattenedMapWithCoordsStream[A, B](val source: ObservableMatrix[A], val valueMap: (A, Int, Int) => B, val indexMap: (A, B, Int, Int) => Int) extends SingleSlotObservableMatrixStream[A, B] {
		override def onNext(a: A, up: Int, down: Int): Unit = {
			val b = valueMap(a, up, down)
			forwardNext(b, NOT_APPLICABLE_INDEX, indexMap(a, b, up, down))
		}

		override def onError(ex: Throwable): Unit = forwardError(ex)

		override def onComplete(): Unit = forwardComplete()

		override protected def createDelegate(observer: Observer[B]): Observer[A] = new Observer[A] {
			override def onNext(a: A, up: Int, down: Int): Unit = {
				val b = valueMap(a, up, down)
				observer.onNext(b, NOT_APPLICABLE_INDEX, indexMap(a, b, up, down))
			}

			override def onError(ex: Throwable): Unit = observer.onError(ex)

			override def onComplete(): Unit = observer.onComplete()
		}
	}

	final class FlattenedFoldStream[A, B, S](val source: ObservableMatrix[A], val initialState: S, val f: (S, A, Int, Int) => (S, B, Int)) extends SingleSlotObservableMatrixStream[A, B] {
		private var state = initialState

		override protected def resetState(): Unit = state = initialState

		override def onNext(a: A, up: Int, down: Int): Unit = {
			val (newState, b, index) = f(state, a, up, down)
			state = newState
			forwardNext(b, NOT_APPLICABLE_INDEX, index)
		}

		override def onError(ex: Throwable): Unit = forwardError(ex)

		override def onComplete(): Unit = forwardComplete()

		override protected def createDelegate(observer: Observer[B]): Observer[A] = new FoldConsumer(initialState, f, observer)
	}

	final class MappedTaskArray[A, B](val source: TaskArray[A], val f: A => B)
		extends SingleSlotObservableStream[A, B] with DefaultTaskArray[B] {
		override def onNext(a: A, up: Int, down: Int): Unit = forwardNext(f(a), up, down)

		override def onError(ex: Throwable): Unit = forwardError(ex)

		override def onComplete(): Unit = forwardComplete()

		override protected def createDelegate(observer: Observer[B]): Observer[A] = new Observer[A] {
			override def onNext(a: A, up: Int, down: Int): Unit = observer.onNext(f(a), up, down)

			override def onError(ex: Throwable): Unit = observer.onError(ex)

			override def onComplete(): Unit = observer.onComplete()
		}
	}

	final class MappedWithCoordsTaskArray[A, B](val source: TaskArray[A], val f: (A, Int, Int) => B)
		extends SingleSlotObservableStream[A, B] with DefaultTaskArray[B] {
		override def onNext(a: A, up: Int, down: Int): Unit = forwardNext(f(a, up, down), up, down)

		override def onError(ex: Throwable): Unit = forwardError(ex)

		override def onComplete(): Unit = forwardComplete()

		override protected def createDelegate(observer: Observer[B]): Observer[A] = new Observer[A] {
			override def onNext(a: A, up: Int, down: Int): Unit = observer.onNext(f(a, up, down), up, down)

			override def onError(ex: Throwable): Unit = observer.onError(ex)

			override def onComplete(): Unit = observer.onComplete()
		}
	}

	final class FlatMappedTaskMatrix[A, B](val source: TaskArray[A], val f: A => TaskArray[B])
		extends DefaultTaskMatrix[B] with Observer[A] {

		private var downstreamObserver: Observer[B] = uninitialized
		private var outerCompleted = false
		private var activeInnerCount = 0
		private var errorFired = false

		override def subscribe(observer: Observer[B]): Unit = {
			if downstreamObserver == null then {
				this.downstreamObserver = observer
				source.subscribe(this)
			} else {
				source.subscribe(new FlatMapObserver(f, observer))
			}
		}

		private def resetState(): Unit = {
			outerCompleted = false
			activeInnerCount = 0
			errorFired = false
		}

		private def tryComplete(): Unit = {
			if outerCompleted && activeInnerCount == 0 && !errorFired then {
				val obs = downstreamObserver
				downstreamObserver = null
				resetState()
				if obs != null then obs.onComplete()
			}
		}

		override def onNext(a: A, outerOuter: Int, outerDown: Int): Unit = {
			activeInnerCount += 1
			f(a).subscribe(new InnerObserver(outerDown))
		}

		override def onError(ex: Throwable): Unit = {
			if !errorFired then {
				errorFired = true
				val obs = downstreamObserver
				downstreamObserver = null
				resetState()
				if obs != null then obs.onError(ex)
			}
		}

		override def onComplete(): Unit = {
			outerCompleted = true
			tryComplete()
		}

		private final class InnerObserver(outerDown: Int) extends Observer[B] {
			override def onNext(b: B, innerUp: Int, innerDown: Int): Unit = {
				val obs = downstreamObserver
				if obs != null then obs.onNext(b, outerDown, innerDown)
			}

			override def onError(ex: Throwable): Unit = FlatMappedTaskMatrix.this.onError(ex)

			override def onComplete(): Unit = {
				activeInnerCount -= 1
				tryComplete()
			}
		}
	}

	final class FlatMappedWithCoordsTaskMatrix[A, B](val source: TaskArray[A], val f: (A, Int, Int) => TaskArray[B])
		extends DefaultTaskMatrix[B] with Observer[A] {

		private var downstreamObserver: Observer[B] = uninitialized
		private var outerCompleted = false
		private var activeInnerCount = 0
		private var errorFired = false

		override def subscribe(observer: Observer[B]): Unit = {
			if downstreamObserver == null then {
				this.downstreamObserver = observer
				source.subscribe(this)
			} else {
				source.subscribe(new FlatMapWithCoordsObserver(f, observer))
			}
		}

		private def resetState(): Unit = {
			outerCompleted = false
			activeInnerCount = 0
			errorFired = false
		}

		private def tryComplete(): Unit = {
			if outerCompleted && activeInnerCount == 0 && !errorFired then {
				val obs = downstreamObserver
				downstreamObserver = null
				resetState()
				if obs != null then obs.onComplete()
			}
		}

		override def onNext(a: A, outerOuter: Int, outerDown: Int): Unit = {
			activeInnerCount += 1
			f(a, outerOuter, outerDown).subscribe(new InnerObserver(outerDown))
		}

		override def onError(ex: Throwable): Unit = {
			if !errorFired then {
				errorFired = true
				val obs = downstreamObserver
				downstreamObserver = null
				resetState()
				if obs != null then obs.onError(ex)
			}
		}

		override def onComplete(): Unit = {
			outerCompleted = true
			tryComplete()
		}

		private final class InnerObserver(outerDown: Int) extends Observer[B] {
			override def onNext(b: B, innerUp: Int, innerDown: Int): Unit = {
				val obs = downstreamObserver
				if obs != null then obs.onNext(b, outerDown, innerDown)
			}

			override def onError(ex: Throwable): Unit = FlatMappedWithCoordsTaskMatrix.this.onError(ex)

			override def onComplete(): Unit = {
				activeInnerCount -= 1
				tryComplete()
			}
		}
	}

	final class ScannedTaskArray[A, B](val source: TaskArray[A], val initial: B, val f: (B, A) => B)
		extends SingleSlotObservableStream[A, B] with DefaultTaskArray[B] {

		private var state = initial

		override protected def resetState(): Unit = state = initial

		override def onNext(a: A, upChain: Int, downChain: Int): Unit = {
			state = f(state, a)
			forwardNext(state, upChain, downChain)
		}

		override def onError(ex: Throwable): Unit = forwardError(ex)

		override def onComplete(): Unit = forwardComplete()

		override protected def createDelegate(observer: Observer[B]): Observer[A] = new ScanConsumer(initial, f, observer)
	}

	final class BufferedTaskArray[A, T >: A : ClassTag](val source: TaskArray[A], val size: Int)
		extends SingleSlotObservableStream[A, IArray[T]] with DefaultTaskArray[IArray[T]] {

		private val buf = new Array[T](size)
		private var count = 0
		private var chunkIndex = 0
		private var active = true

		override protected def resetState(): Unit = {
			count = 0
			chunkIndex = 0
			active = true
		}

		override def onNext(a: A, up: Int, down: Int): Unit = {
			if active then {
				buf(count) = a
				count += 1
				if count == size then {
					val chunk = IArray.unsafeFromArray(buf.clone())
					count = 0
					val idx = chunkIndex
					chunkIndex += 1
					forwardNext(chunk, up, idx)
				}
			}
		}

		override def onError(ex: Throwable): Unit = {
			if active then {
				active = false
				forwardError(ex)
			}
		}

		override def onComplete(): Unit = {
			if active then {
				active = false
				if count > 0 then {
					val partial = IArray.unsafeFromArray(buf.take(count))
					val idx = chunkIndex
					chunkIndex += 1
					forwardNext(partial, 0, idx)
				}
				forwardComplete()
			}
		}

		override protected def createDelegate(observer: Observer[IArray[T]]): Observer[A] = new BufferedConsumer[A, T](size, observer)
	}

	final class TakeTaskArray[A](val source: TaskArray[A], val n: Int)
		extends SingleSlotObservableStream[A, A] with DefaultTaskArray[A] {

		private var count = 0
		private var active = true

		override protected def resetState(): Unit = {
			count = 0
			active = true
		}

		override def onNext(a: A, up: Int, down: Int): Unit = {
			if active then {
				if count < n then {
					val currentCount = count
					count += 1
					forwardNext(a, up, currentCount)
					if count == n then {
						active = false
						forwardComplete()
					}
				}
			}
		}

		override def onError(ex: Throwable): Unit = {
			if active then {
				active = false
				forwardError(ex)
			}
		}

		override def onComplete(): Unit = {
			if active then {
				active = false
				forwardComplete()
			}
		}

		override protected def createDelegate(observer: Observer[A]): Observer[A] = new TakeConsumer(n, observer)
	}

	final class TakeWhileTaskArray[A](val source: TaskArray[A], val p: A => Boolean)
		extends SingleSlotObservableStream[A, A] with DefaultTaskArray[A] {

		private var active = true
		private var counter = 0

		override protected def resetState(): Unit = {
			active = true
			counter = 0
		}

		override def onNext(a: A, up: Int, down: Int): Unit = {
			if active then {
				if p(a) then {
					val currentCounter = counter
					counter += 1
					forwardNext(a, up, currentCounter)
				} else {
					active = false
					forwardComplete()
				}
			}
		}

		override def onError(ex: Throwable): Unit = {
			if active then {
				active = false
				forwardError(ex)
			}
		}

		override def onComplete(): Unit = {
			if active then {
				active = false
				forwardComplete()
			}
		}

		override protected def createDelegate(observer: Observer[A]): Observer[A] = new TakeWhileConsumer(p, observer)
	}

	final class FlattenedToInnerTaskArray[A](val source: TaskMatrix[A])
		extends SingleSlotObservableMatrixStream[A, A] with DefaultTaskArray[A] {

		override def onNext(a: A, upChain: Int, downChain: Int): Unit = forwardNext(a, NOT_APPLICABLE_INDEX, downChain)

		override def onError(ex: Throwable): Unit = forwardError(ex)

		override def onComplete(): Unit = forwardComplete()

		override protected def createDelegate(observer: Observer[A]): Observer[A] = new Observer[A] {
			override def onNext(a: A, upChain: Int, downChain: Int): Unit = observer.onNext(a, NOT_APPLICABLE_INDEX, downChain)

			override def onError(ex: Throwable): Unit = observer.onError(ex)

			override def onComplete(): Unit = observer.onComplete()
		}
	}

	final class FlattenedToOuterTaskArray[A](val source: TaskMatrix[A])
		extends SingleSlotObservableMatrixStream[A, A] with DefaultTaskArray[A] {

		override def onNext(a: A, upChain: Int, downChain: Int): Unit = forwardNext(a, NOT_APPLICABLE_INDEX, upChain)

		override def onError(ex: Throwable): Unit = forwardError(ex)

		override def onComplete(): Unit = forwardComplete()

		override protected def createDelegate(observer: Observer[A]): Observer[A] = new Observer[A] {
			override def onNext(a: A, upChain: Int, downChain: Int): Unit = observer.onNext(a, NOT_APPLICABLE_INDEX, upChain)

			override def onError(ex: Throwable): Unit = observer.onError(ex)

			override def onComplete(): Unit = observer.onComplete()
		}
	}

	final class FlattenedWithTaskArray[A](val source: TaskMatrix[A], val f: (A, Int, Int) => Int)
		extends SingleSlotObservableMatrixStream[A, A] with DefaultTaskArray[A] {

		override def onNext(a: A, upChain: Int, downChain: Int): Unit = forwardNext(a, NOT_APPLICABLE_INDEX, f(a, upChain, downChain))

		override def onError(ex: Throwable): Unit = forwardError(ex)

		override def onComplete(): Unit = forwardComplete()

		override protected def createDelegate(observer: Observer[A]): Observer[A] = new Observer[A] {
			override def onNext(a: A, upChain: Int, downChain: Int): Unit = observer.onNext(a, NOT_APPLICABLE_INDEX, f(a, upChain, downChain))

			override def onError(ex: Throwable): Unit = observer.onError(ex)

			override def onComplete(): Unit = observer.onComplete()
		}
	}

	final class FlattenedToSequentialTaskArray[A](val source: TaskMatrix[A])
		extends SingleSlotObservableMatrixStream[A, A] with DefaultTaskArray[A] {

		private var counter = 0

		override protected def resetState(): Unit = counter = 0

		override def onNext(a: A, up: Int, down: Int): Unit = {
			val idx = counter
			counter += 1
			forwardNext(a, NOT_APPLICABLE_INDEX, idx)
		}

		override def onError(ex: Throwable): Unit = forwardError(ex)

		override def onComplete(): Unit = forwardComplete()

		override protected def createDelegate(observer: Observer[A]): Observer[A] = new SequentialConsumer(observer)
	}

	final class FlattenedMapTaskArray[A, B](val source: TaskMatrix[A], val f: (A, Int, Int) => (B, Int))
		extends SingleSlotObservableMatrixStream[A, B] with DefaultTaskArray[B] {

		override def onNext(a: A, up: Int, down: Int): Unit = {
			val (b, index) = f(a, up, down)
			forwardNext(b, NOT_APPLICABLE_INDEX, index)
		}

		override def onError(ex: Throwable): Unit = forwardError(ex)

		override def onComplete(): Unit = forwardComplete()

		override protected def createDelegate(observer: Observer[B]): Observer[A] = new Observer[A] {
			override def onNext(a: A, up: Int, down: Int): Unit = {
				val (b, index) = f(a, up, down)
				observer.onNext(b, NOT_APPLICABLE_INDEX, index)
			}

			override def onError(ex: Throwable): Unit = observer.onError(ex)

			override def onComplete(): Unit = observer.onComplete()
		}
	}

	final class FlattenedMapWithCoordsTaskArray[A, B](val source: TaskMatrix[A], val valueMap: (A, Int, Int) => B, val indexMap: (A, B, Int, Int) => Int)
		extends SingleSlotObservableMatrixStream[A, B] with DefaultTaskArray[B] {

		override def onNext(a: A, up: Int, down: Int): Unit = {
			val b = valueMap(a, up, down)
			forwardNext(b, NOT_APPLICABLE_INDEX, indexMap(a, b, up, down))
		}

		override def onError(ex: Throwable): Unit = forwardError(ex)

		override def onComplete(): Unit = forwardComplete()

		override protected def createDelegate(observer: Observer[B]): Observer[A] = new Observer[A] {
			override def onNext(a: A, up: Int, down: Int): Unit = {
				val b = valueMap(a, up, down)
				observer.onNext(b, NOT_APPLICABLE_INDEX, indexMap(a, b, up, down))
			}

			override def onError(ex: Throwable): Unit = observer.onError(ex)

			override def onComplete(): Unit = observer.onComplete()
		}
	}

	final class FlattenedFoldTaskArray[A, B, S](val source: TaskMatrix[A], val initialState: S, val f: (S, A, Int, Int) => (S, B, Int))
		extends SingleSlotObservableMatrixStream[A, B] with DefaultTaskArray[B] {

		private var state = initialState

		override protected def resetState(): Unit = state = initialState

		override def onNext(a: A, up: Int, down: Int): Unit = {
			val (newState, b, index) = f(state, a, up, down)
			state = newState
			forwardNext(b, NOT_APPLICABLE_INDEX, index)
		}

		override def onError(ex: Throwable): Unit = forwardError(ex)

		override def onComplete(): Unit = forwardComplete()

		override protected def createDelegate(observer: Observer[B]): Observer[A] = new FoldConsumer(initialState, f, observer)
	}
}