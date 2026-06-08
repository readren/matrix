package readren.sequencer

import readren.common.*

import scala.annotation.targetName
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

		def subscribe(callback: A => Unit): Unit = {
			subscribe(new Observer[A] {
				override def onNext(value: A, upChain: Int, downChain: Int): Unit = callback(value)

				override def onError(ex: Throwable): Unit = ()

				override def onComplete(): Unit = ()
			})
		}

		def foreach(consumer: A => Unit): Unit = {
			subscribe(new Observer[A] {
				override def onNext(value: A, upChain: Int, downChain: Int): Unit = consumer(value)

				override def onError(ex: Throwable): Unit = ()

				override def onComplete(): Unit = ()
			})
		}

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
		override def subscribe(observer: Observer[A]): Unit = subscribe(observer, null, NOT_APPLICABLE_INDEX, NOT_APPLICABLE_INDEX)

		/** Subscribes with coordinate tracking.
		 * @param observer the observer invoked when the value is captured.
		 * Unified as a [[Observer]] carrying both `upChain` and `downChain` indices to avoid adapter allocations during pipeline propagation. Because a [[Capturer]] is a single-value cache:
		 * - Standalone/direct subscriptions default both `upChain` and `downChain` to `-1`.
		 * - When managed by a parent [[CapturerArray]], `upChain` is `0` and `downChain` represents the element's index.
		 * - When managed by a parent [[CapturerMatrix]], `upChain` represents the outer/row index and `downChain` represents the inner/column index. */
		def subscribe(observer: Observer[A], key: Key, upChain: Int, downChain: Int): Unit

		override def subscribe(consumer: A => Unit): Unit = {
			subscribe(new Observer[A] {
				override def onNext(value: A, upChain: Int, downChain: Int): Unit = consumer(value)

				override def onError(ex: Throwable): Unit = ()

				override def onComplete(): Unit = ()
			})
		}

		def subscribe(consumer: (A, Int, Int) => Unit, key: Key, upChain: Int, downChain: Int): Unit = {
			subscribe(
				new Observer[A] {
					override def onNext(value: A, upChain: Int, downChain: Int): Unit = consumer(value, upChain, downChain)

					override def onError(ex: Throwable): Unit = ()

					override def onComplete(): Unit = ()
				},
				key,
				upChain,
				downChain
			)
		}

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

		override def subscribe(observer: Observer[A], key: Key, upChain: Int, downChain: Int): Unit = {
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

		override def subscribe(observer: Observer[Nothing], key: Key, upChain: Int, downChain: Int): Unit = {
			observer.onError(exception)
		}

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

		override def subscribe(observer: Observer[A], key: Key, upChain: Int, downChain: Int): Unit = {
			state.fold {
				if key != null then {
					unsubscribe(key)
				}
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

		override def subscribe(observer: Observer[A], key: Key, upChain: Int, downChain: Int): Unit =
			underlying.subscribe(observer, key, upChain, downChain)

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

	trait ObservableArray[+A] { thisObservableArray =>
		def subscribe(observer: Observer[A]): Unit

		inline def subscribeCallbacks(onNextCallback: (A, Int, Int) => Unit, onErrorCallback: Throwable => Unit = _ => (), onCompleteCallback: () => Unit = () => ()): Unit = {
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

		inline def foreachWithIndex(inline consumer: (A, Int) => Unit): Unit = {
			subscribe(new Observer[A] {
				override def onNext(value: A, upChain: Int, downChain: Int): Unit = consumer(value, downChain)

				override def onError(ex: Throwable): Unit = ()

				override def onComplete(): Unit = ()
			})
		}

		def map[B: ClassTag](f: A => B): ObservableArray[B]

		def mapWithIndex[B: ClassTag](f: (A, Int) => B): ObservableArray[B]

		def flatMap[B: ClassTag](f: A => ObservableArray[B]): ObservableMatrix[B]

		def flatMapWithIndex[B: ClassTag](f: (A, Int) => ObservableArray[B]): ObservableMatrix[B]

		def scan[B: ClassTag](initial: B)(f: (B, A) => B): ObservableArray[B]

		def buffer[T >: A : ClassTag](size: Int): ObservableArray[IArray[T]]

		def zip[B, C: ClassTag](other: ObservableArray[B])(f: (A, B) => C): ObservableArray[C]

		def take(n: Int): ObservableArray[A]

		def takeWhile(p: A => Boolean): ObservableArray[A]

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

	/** Partial implementation of [[ObservableArray]] */
	trait DefaultObservableArray[+A] extends ObservableArray[A] {
		override def map[B: ClassTag](f: A => B): ObservableArray[B] = {
			new DefaultObservableArray[B] {
				override def subscribe(observer: Observer[B]): Unit = {
					DefaultObservableArray.this.subscribe(new Observer[A] {
						override def onNext(a: A, upChain: Int, downChain: Int): Unit = observer.onNext(f(a), upChain, downChain)

						override def onError(ex: Throwable): Unit = observer.onError(ex)

						override def onComplete(): Unit = observer.onComplete()
					})
				}
			}
		}

		override def mapWithIndex[B: ClassTag](f: (A, Int) => B): ObservableArray[B] =
			new DefaultObservableArray[B] {
				override def subscribe(observer: Observer[B]): Unit = {
					DefaultObservableArray.this.subscribe(new Observer[A] {
						override def onNext(a: A, upChain: Int, downChain: Int): Unit = observer.onNext(f(a, downChain), upChain, downChain)

						override def onError(ex: Throwable): Unit = observer.onError(ex)

						override def onComplete(): Unit = observer.onComplete()
					})
				}
			}

		override def flatMap[B: ClassTag](f: A => ObservableArray[B]): ObservableMatrix[B] =
			new DefaultObservableMatrix[B] {
				override def subscribe(observer: Observer[B]): Unit = arrayFlatMapSubscribe(DefaultObservableArray.this, f, observer)
			}

		override def flatMapWithIndex[B: ClassTag](f: (A, Int) => ObservableArray[B]): ObservableMatrix[B] =
			new DefaultObservableMatrix[B] {
				override def subscribe(observer: Observer[B]): Unit = arrayFlatMapWithIndexSubscribe(DefaultObservableArray.this, f, observer)
			}

		override def scan[B: ClassTag](initial: B)(f: (B, A) => B): ObservableArray[B] =
			new DefaultObservableArray[B] {
				override def subscribe(observer: Observer[B]): Unit = DefaultObservableArray.this.subscribe(new ScanConsumer(initial, f, observer))
			}

		override def buffer[T >: A : ClassTag](size: Int): ObservableArray[IArray[T]] =
			new DefaultObservableArray[IArray[T]] {
				override def subscribe(observer: Observer[IArray[T]]): Unit = {
					val consumer = new BufferedConsumer[A, T](size, observer)
					DefaultObservableArray.this.subscribe(consumer)
				}
			}

		override def zip[B, C: ClassTag](other: ObservableArray[B])(f: (A, B) => C): ObservableArray[C] =
			new ZippedObservableArray(this, other, f)

		override def take(n: Int): ObservableArray[A] =
			new DefaultObservableArray[A] {
				override def subscribe(observer: Observer[A]): Unit = DefaultObservableArray.this.subscribe(new TakeConsumer(n, observer))
			}

		override def takeWhile(p: A => Boolean): ObservableArray[A] =
			new DefaultObservableArray[A] {
				override def subscribe(observer: Observer[A]): Unit = DefaultObservableArray.this.subscribe(new TakeWhileConsumer(p, observer))
			}
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
				override def subscribe(observer: Observer[A]): Unit = {
					DefaultObservableMatrix.this.subscribe(new Observer[A] {
						override def onNext(a: A, upChain: Int, downChain: Int): Unit = observer.onNext(a, 0, downChain)

						override def onError(ex: Throwable): Unit = observer.onError(ex)

						override def onComplete(): Unit = observer.onComplete()
					})
				}
			}

		override def flattenToOuter: ObservableArray[A] =
			new DefaultObservableArray[A] {
				override def subscribe(observer: Observer[A]): Unit = {
					DefaultObservableMatrix.this.subscribe(new Observer[A] {
						override def onNext(a: A, upChain: Int, downChain: Int): Unit = observer.onNext(a, 0, upChain)

						override def onError(ex: Throwable): Unit = observer.onError(ex)

						override def onComplete(): Unit = observer.onComplete()
					})
				}
			}

		override def flattenWith(f: (A, Int, Int) => Int): ObservableArray[A] =
			new DefaultObservableArray[A] {
				override def subscribe(observer: Observer[A]): Unit = {
					DefaultObservableMatrix.this.subscribe(new Observer[A] {
						override def onNext(a: A, upChain: Int, downChain: Int): Unit = observer.onNext(a, 0, f(a, upChain, downChain))

						override def onError(ex: Throwable): Unit = observer.onError(ex)

						override def onComplete(): Unit = observer.onComplete()
					})
				}
			}

		override def flattenToSequential: ObservableArray[A] =
			new DefaultObservableArray[A] {
				override def subscribe(observer: Observer[A]): Unit = flattenToSequentialSubscribe(DefaultObservableMatrix.this, observer)
			}

		override def flattenMap[B: ClassTag](f: (A, Int, Int) => (B, Int)): ObservableArray[B] =
			new DefaultObservableArray[B] {
				override def subscribe(observer: Observer[B]): Unit = flattenMapSubscribe(DefaultObservableMatrix.this, f, observer)
			}

		override def flattenMap[B: ClassTag](valueMap: (A, Int, Int) => B, indexMap: (A, B, Int, Int) => Int): ObservableArray[B] =
			new DefaultObservableArray[B] {
				override def subscribe(observer: Observer[B]): Unit = flattenMapWithIndexSubscribe(DefaultObservableMatrix.this, valueMap, indexMap, observer)
			}

		override def flattenFold[B: ClassTag, S](initialState: S)(f: (S, A, Int, Int) => (S, B, Int)): ObservableArray[B] =
			new DefaultObservableArray[B] {
				override def subscribe(observer: Observer[B]): Unit = flattenFoldSubscribe(DefaultObservableMatrix.this, initialState, f, observer)
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
	trait DefaultTaskArray[+A] extends TaskArray[A] with DefaultObservableArray[A] {
		override def map[B: ClassTag](f: A => B): TaskArray[B] =
			new DefaultTaskArray[B] {
				override def subscribe(observer: Observer[B]): Unit =
					DefaultTaskArray.this.subscribe(new Observer[A] {
						override def onNext(a: A, upChain: Int, downChain: Int): Unit = observer.onNext(f(a), upChain, downChain)

						override def onError(ex: Throwable): Unit = observer.onError(ex)

						override def onComplete(): Unit = observer.onComplete()
					})
			}

		override def mapWithIndex[B: ClassTag](f: (A, Int) => B): TaskArray[B] =
			new DefaultTaskArray[B] {
				override def subscribe(observer: Observer[B]): Unit = {
					DefaultTaskArray.this.subscribe(new Observer[A] {
						override def onNext(a: A, upChain: Int, downChain: Int): Unit = observer.onNext(f(a, downChain), upChain, downChain)

						override def onError(ex: Throwable): Unit = observer.onError(ex)

						override def onComplete(): Unit = observer.onComplete()
					})
				}
			}

		override def flatMap[B: ClassTag](f: A => ObservableArray[B]): ObservableMatrix[B] =
			new DefaultObservableMatrix[B] {
				override def subscribe(observer: Observer[B]): Unit = arrayFlatMapSubscribe(DefaultTaskArray.this, f, observer)
			}

		override def flatMapWithIndex[B: ClassTag](f: (A, Int) => ObservableArray[B]): ObservableMatrix[B] = {
			new DefaultObservableMatrix[B] {
				override def subscribe(observer: Observer[B]): Unit = arrayFlatMapWithIndexSubscribe(DefaultTaskArray.this, f, observer)
			}
		}

		@targetName("flatMapTask")
		override def flatMap[B: ClassTag](f: A => TaskArray[B]): TaskMatrix[B] =
			new DefaultTaskMatrix[B] {
				override def subscribe(observer: Observer[B]): Unit = arrayFlatMapSubscribe(DefaultTaskArray.this, f, observer)
			}

		@targetName("flatMapTaskWithIndex")
		override def flatMapWithIndex[B: ClassTag](f: (A, Int) => TaskArray[B]): TaskMatrix[B] =
			new DefaultTaskMatrix[B] {
				override def subscribe(observer: Observer[B]): Unit = arrayFlatMapWithIndexSubscribe(DefaultTaskArray.this, f, observer)
			}

		override def scan[B: ClassTag](initial: B)(f: (B, A) => B): TaskArray[B] =
			new DefaultTaskArray[B] {
				override def subscribe(observer: Observer[B]): Unit = DefaultTaskArray.this.subscribe(new ScanConsumer(initial, f, observer))
			}

		override def buffer[T >: A : ClassTag](size: Int): TaskArray[IArray[T]] =
			new DefaultTaskArray[IArray[T]] {
				override def subscribe(observer: Observer[IArray[T]]): Unit = {
					val consumer = new BufferedConsumer[A, T](size, observer)
					DefaultTaskArray.this.subscribe(consumer)
				}
			}

		override def zip[B, C: ClassTag](other: ObservableArray[B])(f: (A, B) => C): ObservableArray[C] = new ZippedObservableArray(this, other, f)

		@targetName("zipTask")
		override def zip[B, C: ClassTag](other: TaskArray[B])(f: (A, B) => C): TaskArray[C] = new ZippedTaskArray(this, other, f)

		override def take(n: Int): TaskArray[A] =
			new DefaultTaskArray[A] {
				override def subscribe(observer: Observer[A]): Unit = DefaultTaskArray.this.subscribe(new TakeConsumer(n, observer))
			}

		override def takeWhile(p: A => Boolean): TaskArray[A] =
			new DefaultTaskArray[A] {
				override def subscribe(observer: Observer[A]): Unit = DefaultTaskArray.this.subscribe(new TakeWhileConsumer(p, observer))
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

	trait DefaultTaskMatrix[+A] extends TaskMatrix[A] with DefaultObservableMatrix[A] {
		override def flattenToInner: TaskArray[A] =
			new DefaultTaskArray[A] {
				override def subscribe(observer: Observer[A]): Unit =
					DefaultTaskMatrix.this.subscribe(new Observer[A] {
						override def onNext(a: A, upChain: Int, downChain: Int): Unit = observer.onNext(a, 0, downChain)

						override def onError(ex: Throwable): Unit = observer.onError(ex)

						override def onComplete(): Unit = observer.onComplete()
					})
			}

		override def flattenToOuter: TaskArray[A] =
			new DefaultTaskArray[A] {
				override def subscribe(observer: Observer[A]): Unit =
					DefaultTaskMatrix.this.subscribe(new Observer[A] {
						override def onNext(a: A, upChain: Int, downChain: Int): Unit = observer.onNext(a, 0, upChain)

						override def onError(ex: Throwable): Unit = observer.onError(ex)

						override def onComplete(): Unit = observer.onComplete()
					})
			}

		override def flattenWith(f: (A, Int, Int) => Int): TaskArray[A] =
			new DefaultTaskArray[A] {
				override def subscribe(observer: Observer[A]): Unit =
					DefaultTaskMatrix.this.subscribe(new Observer[A] {
						override def onNext(a: A, upChain: Int, downChain: Int): Unit = observer.onNext(a, 0, f(a, upChain, downChain))

						override def onError(ex: Throwable): Unit = observer.onError(ex)

						override def onComplete(): Unit = observer.onComplete()
					})
			}

		override def flattenToSequential: TaskArray[A] =
			new DefaultTaskArray[A] {
				override def subscribe(observer: Observer[A]): Unit = flattenToSequentialSubscribe(DefaultTaskMatrix.this, observer)
			}

		override def flattenMap[B: ClassTag](f: (A, Int, Int) => (B, Int)): TaskArray[B] =
			new DefaultTaskArray[B] {
				override def subscribe(observer: Observer[B]): Unit = flattenMapSubscribe(DefaultTaskMatrix.this, f, observer)
			}

		override def flattenMap[B: ClassTag](valueMap: (A, Int, Int) => B, indexMap: (A, B, Int, Int) => Int): TaskArray[B] =
			new DefaultTaskArray[B] {
				override def subscribe(observer: Observer[B]): Unit = flattenMapWithIndexSubscribe(DefaultTaskMatrix.this, valueMap, indexMap, observer)
			}

		override def flattenFold[B: ClassTag, S](initialState: S)(f: (S, A, Int, Int) => (S, B, Int)): TaskArray[B] =
			new DefaultTaskArray[B] {
				override def subscribe(observer: Observer[B]): Unit = flattenFoldSubscribe(DefaultTaskMatrix.this, initialState, f, observer)
			}
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

	trait KeyedObservableArray[+A] extends DefaultObservableArray[A] {
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

	trait SettlingArray[+A] extends KeyedObservableArray[A] {
		def maybeResult(index: Int): Maybe[A]

		inline def isCompleted(index: Int): Boolean = maybeResult(index).isDefined

		inline def isPending(index: Int): Boolean = maybeResult(index).isEmpty
	}

	trait KeyedCapturerArray[+A] extends KeyedObservableArray[A] {
		override def map[B: ClassTag](f: A => B): KeyedCapturerArray[B] = new MappedKeyedCapturerArray(this, (a, index) => f(a))

		override def mapWithIndex[B: ClassTag](f: (A, Int) => B): KeyedCapturerArray[B] = new MappedKeyedCapturerArray(this, f)
	}

	final class MappedKeyedCapturerArray[A, +B](val source: KeyedCapturerArray[A], val f: (A, Int) => B) extends KeyedCapturerArray[B] {
		override def keyedSubscribe(observer: Observer[B], key: Key): Unit = {
			source.keyedSubscribe(
				new Observer[A] {
					override def onNext(a: A, upChain: Int, downChain: Int): Unit = observer.onNext(f(a, downChain), upChain, downChain)

					override def onError(ex: Throwable): Unit = observer.onError(ex)

					override def onComplete(): Unit = observer.onComplete()
				},
				key
			)
		}

		override def unsubscribe(key: Key): Unit = source.unsubscribe(key)

		override def isSubscribed(key: Key): Boolean = source.isSubscribed(key)
	}

	sealed trait CapturerArray[+A] extends SettlingArray[A], KeyedCapturerArray[A] {
		override def map[B: ClassTag](f: A => B): CapturerArray[B] = {
			this match {
				case keeper: KeeperArray[A] => keeper.map(f)
				case captor: CaptorArray[A] @unchecked => captor.map(f)
			}
		}

		override def mapWithIndex[B: ClassTag](f: (A, Int) => B): CapturerArray[B] = {
			this match {
				case keeper: KeeperArray[A] => keeper.mapWithIndex(f)
				case captor: CaptorArray[A] @unchecked => captor.mapWithIndex(f)
			}
		}

		@targetName("flatMapCapturer")
		def flatMap[B: ClassTag](f: A => CapturerArray[B]): CapturerMatrix[B]

		@targetName("flatMapCapturerWithIndex")
		def flatMapWithIndex[B: ClassTag](f: (A, Int) => CapturerArray[B]): CapturerMatrix[B]
	}

	final class KeeperArray[+A](values: IArray[A]) extends CapturerArray[A] {
		override def maybeResult(index: Int): Maybe[A] = Maybe(values(index))

		override def keyedSubscribe(observer: Observer[A], key: Key): Unit = {
			values.foreachWithIndex((a, index) => observer.onNext(a, 0, index))
			observer.onComplete()
		}

		override def unsubscribe(key: Key): Unit = ()

		override def isSubscribed(key: Key): Boolean = false

		override def map[B: ClassTag](f: A => B): KeeperArray[B] = new KeeperArray[B](values.mapWithIndex { (a, i) => f(a) })

		override def mapWithIndex[B: ClassTag](f: (A, Int) => B): KeeperArray[B] = new KeeperArray[B](values.mapWithIndex(f))

		override def flatMap[B: ClassTag](f: A => ObservableArray[B]): ObservableMatrix[B] =
			new DefaultObservableMatrix[B] {
				override def subscribe(observer: Observer[B]): Unit = {
					if values.length == 0 then observer.onComplete()
					else new FlatMapMatrixObserver(values, f, observer).start()
				}
			}

		@targetName("flatMapCapturer")
		override def flatMap[B: ClassTag](f: A => CapturerArray[B]): CapturerMatrix[B] = new FlatMappedCapturerMatrix(this, f)

		override def flatMapWithIndex[B: ClassTag](f: (A, Int) => ObservableArray[B]): ObservableMatrix[B] = {
			new DefaultObservableMatrix[B] {
				override def subscribe(observer: Observer[B]): Unit = {
					if values.length == 0 then observer.onComplete()
					else new FlatMapWithIndexMatrixObserver(values, f, observer).start()
				}
			}
		}

		@targetName("flatMapCapturerWithIndex")
		override def flatMapWithIndex[B: ClassTag](f: (A, Int) => CapturerArray[B]): CapturerMatrix[B] = new FlatMappedWithIndexCapturerMatrix(this, f)
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
					capturers(index).subscribe(
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

		override def mapWithIndex[B: ClassTag](f: (A, Int) => B): CapturerArray[B] =
			new CaptorArray(capturers.mapWithIndex((capturer, index) => capturer.map(a => f(a, index))))

		override def flatMap[B: ClassTag](f: A => ObservableArray[B]): ObservableMatrix[B] =
			new DefaultObservableMatrix[B] {
				override def subscribe(observer: Observer[B]): Unit = arrayFlatMapSubscribe(thisCaptorArray, f, observer)
			}

		override def flatMapWithIndex[B: ClassTag](f: (A, Int) => ObservableArray[B]): ObservableMatrix[B] =
			new DefaultObservableMatrix[B] {
				override def subscribe(observer: Observer[B]): Unit = arrayFlatMapWithIndexSubscribe(thisCaptorArray, f, observer)
			}

		@targetName("flatMapCapturer")
		override def flatMap[B: ClassTag](f: A => CapturerArray[B]): CapturerMatrix[B] = new FlatMappedCapturerMatrix(this, f)

		@targetName("flatMapCapturerWithIndex")
		override def flatMapWithIndex[B: ClassTag](f: (A, Int) => CapturerArray[B]): CapturerMatrix[B] = new FlatMappedWithIndexCapturerMatrix(this, f)
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

	final class FlatMappedCapturerMatrix[A, B](val source: CapturerArray[A], val f: A => CapturerArray[B]) extends CapturerMatrix[B] {
		private val activeInnerSubscriptions = scala.collection.mutable.Map[Key, List[CapturerArray[B]]]()

		override def maybeResult(outerIndex: Int, innerIndex: Int): Maybe[B] = source.maybeResult(outerIndex).flatMap(a => f(a).maybeResult(innerIndex))

		override def subscribe(observer: Observer[B], key: Key): Unit = {
			if key != null then unsubscribe(key)
			flatMappedMatrixSubscribe(source, (a, _) => f(a), activeInnerSubscriptions, observer, key)
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

		override def flattenMap[C: ClassTag](valueMap: (B, Int, Int) => C, indexMap: (B, C, Int, Int) => Int): KeyedCapturerArray[C] = new FlattenedMapWithIndexArray[B, C](this, valueMap, indexMap)

		override def flattenFold[C: ClassTag, S](initialState: S)(f: (S, B, Int, Int) => (S, C, Int)): KeyedCapturerArray[C] = new FlattenedFoldArray(this, initialState, f)
	}

	final class FlatMappedWithIndexCapturerMatrix[A, B](val source: CapturerArray[A], val f: (A, Int) => CapturerArray[B]) extends CapturerMatrix[B] {
		private val activeInnerSubscriptions = scala.collection.mutable.Map[Key, List[CapturerArray[B]]]()

		override def maybeResult(outerIndex: Int, innerIndex: Int): Maybe[B] = source.maybeResult(outerIndex).flatMap(a => f(a, outerIndex).maybeResult(innerIndex))

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

		override def flattenMap[C: ClassTag](valueMap: (B, Int, Int) => C, indexMap: (B, C, Int, Int) => Int): KeyedCapturerArray[C] = new FlattenedMapWithIndexArray[B, C](this, valueMap, indexMap)

		override def flattenFold[C: ClassTag, S](initialState: S)(f: (S, B, Int, Int) => (S, C, Int)): KeyedCapturerArray[C] = new FlattenedFoldArray(this, initialState, f)
	}

	final class FlattenedToInnerArray[+A](val matrix: KeyedObservableMatrix[A]) extends KeyedCapturerArray[A] {
		override def keyedSubscribe(observer: Observer[A], key: Key): Unit = {
			matrix.subscribe(
				new Observer[A] {
					override def onNext(a: A, outer: Int, inner: Int): Unit = observer.onNext(a, 0, inner)

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
					override def onNext(a: A, outer: Int, inner: Int): Unit = observer.onNext(a, 0, outer)

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
					override def onNext(a: A, outer: Int, inner: Int): Unit = observer.onNext(a, 0, f(a, outer, inner))

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
					override def onNext(a: A, outer: Int, inner: Int): Unit = {
						val index = counter
						counter += 1
						observer.onNext(a, 0, index)
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
				override def onNext(a: A, outer: Int, inner: Int): Unit = {
					val (b, index) = f(a, outer, inner)
					observer.onNext(b, 0, index)
				}

				override def onError(ex: Throwable): Unit = observer.onError(ex)

				override def onComplete(): Unit = observer.onComplete()
			}, key)
		}

		override def unsubscribe(key: Key): Unit = matrix.unsubscribe(key)

		override def isSubscribed(key: Key): Boolean = matrix.isSubscribed(key)
	}

	final class FlattenedMapWithIndexArray[A, B](val matrix: KeyedObservableMatrix[A], val valueMap: (A, Int, Int) => B, val indexMap: (A, B, Int, Int) => Int) extends KeyedCapturerArray[B] {
		override def keyedSubscribe(observer: Observer[B], key: Key): Unit = {
			matrix.subscribe(
				new Observer[A] {
					override def onNext(a: A, outer: Int, inner: Int): Unit = {
						val b = valueMap(a, outer, inner)
						observer.onNext(b, 0, indexMap(a, b, outer, inner))
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
					override def onNext(a: A, outer: Int, inner: Int): Unit = {
						val (newState, b, index) = f(state, a, outer, inner)
						state = newState
						observer.onNext(b, 0, index)
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

	private final class FlatMapMatrixObserver[A, B](values: IArray[A], f: A => ObservableArray[B], observer: Observer[B]) {
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

	private final class FlatMapWithIndexMatrixObserver[A, B](values: IArray[A], f: (A, Int) => ObservableArray[B], observer: Observer[B]) {
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
			def start(a: A): Unit = f(a, outerIndex).subscribe(this)

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

	private final class FlatMapObserver[A, B](f: A => ObservableArray[B], observer: Observer[B]) extends Observer[A] {
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

	private inline def arrayFlatMapSubscribe[A, B](array: ObservableArray[A], inline f: A => ObservableArray[B], observer: Observer[B]): Unit = {
		array.subscribe(new FlatMapObserver(f, observer))
	}

	private final class FlatMapWithIndexObserver[A, B](f: (A, Int) => ObservableArray[B], observer: Observer[B]) extends Observer[A] {
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
			f(a, outerDown).subscribe(new Observer[B] {
				override def onNext(b: B, innerUp: Int, innerDown: Int): Unit = {
					observer.onNext(b, outerDown, innerDown)
				}

				override def onError(ex: Throwable): Unit = {
					FlatMapWithIndexObserver.this.onError(ex)
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

	private inline def arrayFlatMapWithIndexSubscribe[A, B](array: ObservableArray[A], inline f: (A, Int) => ObservableArray[B], observer: Observer[B]): Unit = {
		array.subscribe(new FlatMapWithIndexObserver(f, observer))
	}

	private final class SequentialConsumer[A](observer: Observer[A]) extends Observer[A] {
		private var counter = 0

		override def onNext(a: A, up: Int, down: Int): Unit = {
			val index = counter
			counter += 1
			observer.onNext(a, 0, index)
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
				observer.onNext(b, 0, index)
			}

			override def onError(ex: Throwable): Unit = observer.onError(ex)

			override def onComplete(): Unit = observer.onComplete()
		})
	}

	private inline def flattenMapWithIndexSubscribe[A, B](matrix: ObservableMatrix[A], valueMap: (A, Int, Int) => B, indexMap: (A, B, Int, Int) => Int, observer: Observer[B]): Unit = {
		matrix.subscribe(new Observer[A] {
			override def onNext(a: A, up: Int, down: Int): Unit = {
				val b = valueMap(a, up, down)
				observer.onNext(b, 0, indexMap(a, b, up, down))
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
			observer.onNext(b, 0, index)
		}

		override def onError(ex: Throwable): Unit = observer.onError(ex)

		override def onComplete(): Unit = observer.onComplete()
	}

	private inline def flattenFoldSubscribe[A, B, S](matrix: ObservableMatrix[A], initialState: S, f: (S, A, Int, Int) => (S, B, Int), observer: Observer[B]): Unit =
		matrix.subscribe(new FoldConsumer(initialState, f, observer))

	private final class FlatMappedMatrixObserver[A, B](getInner: (A, Int) => CapturerArray[B], activeInnerSubscriptions: scala.collection.mutable.Map[Key, List[CapturerArray[B]]], observer: Observer[B], key: Key) extends Observer[A] {
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
			val inner = getInner(a, outerInner)
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

	private inline def flatMappedMatrixSubscribe[A, B](source: CapturerArray[A], inline getInner: (A, Int) => CapturerArray[B], activeInnerSubscriptions: scala.collection.mutable.Map[Key, List[CapturerArray[B]]], observer: Observer[B], key: Key): Unit = {
		if key != null then activeInnerSubscriptions(key) = Nil
		val sub = new FlatMappedMatrixObserver(getInner, activeInnerSubscriptions, observer, key)
		source.keyedSubscribe(sub, key)
	}

	private final class ZipObservation[A, B, C](left: ObservableArray[A], right: ObservableArray[B], f: (A, B) => C, observer: Observer[C]) {
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

	private final class ZippedObservableArray[A, B, C](val left: ObservableArray[A], val right: ObservableArray[B], val f: (A, B) => C) extends DefaultObservableArray[C] {
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
		private val buf = new Array[T](size)
		private var count = 0
		private var chunkIndex = 0
		private var active = true

		override def onNext(a: A, up: Int, down: Int): Unit = {
			if active then {
				buf(count) = a
				count += 1
				if count == size then {
					val chunk = IArray.unsafeFromArray(buf.clone())
					count = 0
					val idx = chunkIndex
					chunkIndex += 1
					observer.onNext(chunk, up, idx)
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
					val partial = IArray.unsafeFromArray(buf.take(count))
					val idx = chunkIndex
					chunkIndex += 1
					observer.onNext(partial, 0, idx)
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

	final class StreamEmitter[A] extends DefaultObservableArray[A] {
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
				observers.foreach(_.onNext(value, 0, idx))
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
}