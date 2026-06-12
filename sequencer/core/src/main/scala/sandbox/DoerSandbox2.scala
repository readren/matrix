package readren.sequencer
package sandbox

import readren.common.{Maybe, Trial, foreachWithIndex}

import scala.annotation.targetName
import scala.annotation.unchecked.uncheckedVariance
import scala.compiletime.uninitialized
import scala.reflect.ClassTag
import scala.util.control.NonFatal

class DoerSandbox2 {
	type Key = AnyRef

	inline given CanEqual[Key, Key] = CanEqual.derived

	trait MonoObserver[-A] {
		def onSuccess(value: A): Unit

		def onError(ex: Throwable): Unit
	}

	/** Root super trait of all asynchronous observables. */
	trait Observable[+A] {
		def subscribe(monoObserver: MonoObserver[A]): Unit

		inline def subscribeCallbacks(inline success: A => Unit, inline error: Throwable => Unit = _ => ()): Unit = {
			subscribe(new MonoObserver {
				override def onSuccess(value: A): Unit = success(value)

				override def onError(ex: Throwable): Unit = error(ex)

			})
		}

		inline def foreach(inline consumer: A => Unit): Unit = subscribeCallbacks(consumer)

		def map[B](f: A => B): Observable[B]

		def flatMap[B](f: A => Observable[B]): Observable[B]
	}

	// ==================== DOABLE WORK HIERARCHY ====================

	/** An exception-unaware lazy computation that starts a fresh execution on subscribe.
	 * Serves as the root of all doable work.
	 */
	trait Task[+A] extends Observable[A] { thisTask =>
		override def map[B](f: A => B): Task[B] = {
			(monoObserverB: MonoObserver[B]) => {
				thisTask.subscribe(new MonoObserver[A] {
					override def onSuccess(value: A): Unit = monoObserverB.onSuccess(f(value))

					override def onError(ex: Throwable): Unit = monoObserverB.onError(ex)
				})
			}
		}

		override def flatMap[B](f: A => Observable[B]): Task[B] = {
			(monoObserverB: MonoObserver[B]) => {
				thisTask.subscribe(new MonoObserver[A] {
					override def onSuccess(value: A): Unit = f(value).subscribe(monoObserverB)

					override def onError(ex: Throwable): Unit = monoObserverB.onError(ex)
				})
			}
		}

		@targetName("flatMapTask")
		def flatMap[B](f: A => Task[B]): Task[B] = {
			(monoObserverB: MonoObserver[B]) => {
				thisTask.subscribe(new MonoObserver[A] {
					override def onSuccess(value: A): Unit = f(value).subscribe(monoObserverB)

					override def onError(ex: Throwable): Unit = monoObserverB.onError(ex)
				})
			}
		}

		def mapGuarded[B](f: A => B): Task[B] = {
			(monoObserverB: MonoObserver[B]) => {
				thisTask.subscribe(new MonoObserver[A] {
					override def onSuccess(value: A): Unit = {
						val maybeB = try Maybe(f(value)) catch {
							case NonFatal(e) =>
								monoObserverB.onError(e)
								Maybe.empty
						}
						maybeB.foreach(monoObserverB.onSuccess)
					}

					override def onError(ex: Throwable): Unit = monoObserverB.onError(ex)
				})
			}
		}

		def flatMapGuarded[B](f: A => Observable[B]): Task[B] = {
			(monoObserverB: MonoObserver[B]) => {
				thisTask.subscribe(new MonoObserver[A] {
					override def onSuccess(value: A): Unit = {
						val maybeObs = try Maybe(f(value)) catch {
							case NonFatal(e) =>
								monoObserverB.onError(e)
								Maybe.empty
						}
						maybeObs.foreach(_.subscribe(monoObserverB))
					}

					override def onError(ex: Throwable): Unit = monoObserverB.onError(ex)
				})
			}
		}

		@targetName("flatMapTaskGuarded")
		def flatMapGuarded[B](f: A => Task[B]): Task[B] = {
			(monoObserver: MonoObserver[B]) => {
				thisTask.subscribe(new MonoObserver[A] {
					override def onSuccess(value: A): Unit = {
						val maybeTask = try Maybe(f(value)) catch {
							case NonFatal(e) =>
								monoObserver.onError(e)
								Maybe.empty
						}
						maybeTask.foreach(_.subscribe(monoObserver))
					}

					override def onError(ex: Throwable): Unit = monoObserver.onError(ex)
				})
			}
		}

		def guarded: Task[A] = new GuardedTask(thisTask)
	}

	class GuardedTask[+A](underlying: Task[A]) extends Task[A] {
		override def subscribe(monoObserver: MonoObserver[A]): Unit = underlying.subscribe(monoObserver)

		override def map[B](f: A => B): Task[B] = underlying.mapGuarded(f)

		override def flatMap[B](f: A => Observable[B]): Task[B] = underlying.flatMapGuarded(f)

		@targetName("flatMapTask")
		override def flatMap[B](f: A => Task[B]): Task[B] = underlying.flatMapGuarded(f)
	}


	// ==================== ASYNCHRONOUS RESULT HIERARCHY ====================

	/** Exception-unaware single result capturer. Ex LatchingTask
	 * Does not inherit from Task, cleanly separating results from doable work. */
	sealed trait Capturer[+A] extends Observable[A] { thisCapturer =>
		def trial: Trial[A]

		def maybeValue: Maybe[A]

		def isCompleted: Boolean

		def isPending: Boolean = !isCompleted

		def subscribe(key: Key, monoObserver: MonoObserver[A]): Unit

		def unsubscribe(monoObserver: MonoObserver[A]): Unit

		def unsubscribe(key: Key): Unit

		def countOccurrencesOf(monoObserver: MonoObserver[A]): Int

		def countObserversAssociatedTo(key: Key): Int

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

		def guarded: Capturer[A] = new GuardedCapturer(thisCapturer)
	}

	/** A [[Capturer]] that has already captured a successful value. Ex ReadyTask */
	class Keeper[+A](val value: A) extends Capturer[A] {
		override def maybeValue: Maybe[A] = Maybe(value)

		override def isCompleted: Boolean = true

		override def subscribe(key: Key, monoObserver: MonoObserver[A]): Unit = monoObserver.onSuccess(value)

		override def subscribe(monoObserver: MonoObserver[A]): Unit = monoObserver.onSuccess(value)

		override def trial: Trial[A] = Trial.success(value)

		override def unsubscribe(monoObserver: MonoObserver[A]): Unit = ()

		override def unsubscribe(key: Key): Unit = ()

		override def countOccurrencesOf(monoObserver: MonoObserver[A]): Int = 0

		override def countObserversAssociatedTo(key: Key): Int = 0

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
		override def maybeValue: Maybe[Nothing] = Maybe.empty

		override def isCompleted: Boolean = true

		override def subscribe(key: Key, monoObserver: MonoObserver[Nothing]): Unit = monoObserver.onError(exception)

		override def subscribe(monoObserver: MonoObserver[Nothing]): Unit = monoObserver.onError(exception)

		override def trial: Trial[Nothing] = Trial.failure(exception)

		override def unsubscribe(monoObserver: MonoObserver[Nothing]): Unit = ()

		override def unsubscribe(key: Key): Unit = ()

		override def countOccurrencesOf(monoObserver: MonoObserver[Nothing]): Int = 0

		override def countObserversAssociatedTo(key: Key): Int = 0

		override def map[B](f: Nothing => B): Capturer[B] = this.asInstanceOf[Failed]

		override def flatMap[B](f: Nothing => Observable[B]): Observable[B] = this.asInstanceOf[Failed]

		@targetName("flatMapCapturer")
		override def flatMap[B](f: Nothing => Capturer[B]): Capturer[B] = this.asInstanceOf[Failed]

		override def mapGuarded[B](f: Nothing => B): Capturer[B] = this.asInstanceOf[Failed]

		override def flatMapGuarded[B](f: Nothing => Observable[B]): Observable[B] = this.asInstanceOf[Failed]

		@targetName("flatMapCapturerGuarded")
		override def flatMapGuarded[B](f: Nothing => Capturer[B]): Capturer[B] = this.asInstanceOf[Failed]
	}

	trait Muxer[A, Observer[-_] <: AnyRef] {
		type Entry = Observer[A] | (Key, Observer[A])
		type EntryId = Observer[A] | Key

		extension (entry: Entry) def matches(id: EntryId): Boolean = entry match {
			case (key: Key, _: Observer[A] @unchecked) => key == id
			case observer: Observer[A] @unchecked => observer eq id
		}

		private var maybeFirstOb: Maybe[Entry] = Maybe.empty
		private var maybeFollowingObs: Maybe[Array[Entry]] = Maybe.empty
		private var followingObsSize: Int = 0

		def addEntry(entry: Entry): Unit = {
			maybeFirstOb.fold {
				maybeFirstOb = Maybe(entry)
			} { _ =>
				maybeFollowingObs.fold {
					val followingObs = new Array[Entry](8)
					followingObs(0) = entry
					followingObsSize = 1
					maybeFollowingObs = Maybe(followingObs)
				} { followingObs =>
					val fs = followingObsSize
					val newFollowingObs =
						if fs < followingObs.length then followingObs
						else {
							val expanded = new Array[Entry](fs * 2)
							System.arraycopy(followingObs, 0, expanded, 0, fs)
							maybeFollowingObs = Maybe(expanded)
							expanded
						}
					newFollowingObs(fs) = entry
					followingObsSize = fs + 1
				}
			}
		}

		def removeAllMatching(id: EntryId): Int = {
			var removedCount = 0
			maybeFirstOb.foreach { firstOb =>
				maybeFollowingObs.foreach { followingObs =>
					var index = followingObsSize
					while index > 0 do {
						index -= 1
						if followingObs(index).matches(id) then {
							val shiftedChunkLength = followingObsSize - index - 1
							if shiftedChunkLength > 0 then System.arraycopy(followingObs, index + 1, followingObs, index, shiftedChunkLength)
							removedCount += 1
							followingObsSize -= 1
							followingObs(followingObsSize) = null.asInstanceOf[Entry] // Clear leaked reference
						}
					}
				}
				if firstOb.matches(id) then {
					maybeFirstOb = maybeFollowingObs.flatMap { followingObs =>
						if followingObsSize == 0 then Maybe.empty
						else {
							val firstFollowingOb = followingObs(0)
							// Shift following elements left by 1
							followingObsSize -= 1
							System.arraycopy(followingObs, 1, followingObs, 0, followingObsSize)
							followingObs(followingObsSize) = null.asInstanceOf[Entry] // Clear leaked reference
							Maybe(firstFollowingOb)
						}
					}
					removedCount += 1
				}
			}
			removedCount
		}

		def countAllMatching(id: EntryId): Int = {
			var counter = 0
			maybeFirstOb.foreach { firstOb =>
				if firstOb.matches(id) then counter += 1
				maybeFollowingObs.fold(false) { followingObs =>
					var index = followingObsSize
					while index > 0 do {
						index -= 1
						if followingObs(index).matches(id) then counter += 1
					}
				}
			}
			counter
		}

		inline def foreachEntry(inline consumer: Observer[A] => Unit): Unit = {
			maybeFirstOb.foreach { firstOb =>
				firstOb match {
					case (_: Key, observer: Observer[A] @unchecked) => consumer(observer)
					case observer: Observer[A] @unchecked => consumer(observer)
				}
				// CRITICAL: The cast is necessary to bypass an invalid Scala 3 compiler optimization during the inline expansion. Because Entry is a Union Type, its runtime allocation is a raw JVM Object array (Object[]). However, if an Observer implementation happens to extend a trait like java.io.Serializable, the Scala 3 compiler will try to optimize this inline closure by implicitly downcasting the entire array container to a Serializable[] array. Since an Object[] cannot be downcast to a Serializable[], the JVM explodes with a ClassCastException. Forcing an AnyRef array view strips away this aggressive optimization and keeps it as a safe, generic pointer array.
				maybeFollowingObs.asInstanceOf[Maybe[IArray[AnyRef]]].foreach { followingObs =>
					var i = 0
					val size = followingObsSize
					while i < size do {
						followingObs(i) match {
							case (_: Key, observer: Observer[A] @unchecked) => consumer(observer)
							case observer: Observer[A] @unchecked => consumer(observer)
						}
						i += 1
					}
				}
			}
		}

		def clear(): Unit = {
			maybeFirstOb = Maybe.empty
			maybeFollowingObs = Maybe.empty
			followingObsSize = 0
		}
	}

	class Captor[A](initialState: Trial[A] = Trial.empty) extends Muxer[A, MonoObserver], Capturer[A] {
		private var state: Trial[A] = initialState

		override def maybeValue: Maybe[A] = state.toMaybe

		override def isCompleted: Boolean = state.isDefined

		override def subscribe(key: Key, monoObserver: MonoObserver[A]): Unit = addEntry((key, monoObserver))

		override def subscribe(monoObserver: MonoObserver[A]): Unit = addEntry(monoObserver)

		override def trial: Trial[A] = state

		override def unsubscribe(key: Key): Unit = if key != null then removeAllMatching(key)

		override def unsubscribe(monoObserver: MonoObserver[A]): Unit = removeAllMatching(monoObserver)

		override def countObserversAssociatedTo(key: Key): Int = if key == null then 0 else countAllMatching(key)

		override def countOccurrencesOf(monoObserver: MonoObserver[A]): Int = countAllMatching(monoObserver)

		override def map[B](f: A => B): Capturer[B] = {
			state.fold {
				val captor = new Captor[B]()
				this.subscribe(new MonoObserver[A] {
					override def onSuccess(a: A): Unit = captor.capture(f(a))

					override def onError(ex: Throwable): Unit = captor.fail(ex)
				})
				captor
			}(new Failed(_)) { a => new Keeper(f(a)) }
		}

		override def flatMap[B](f: A => Observable[B]): Observable[B] = {
			state.fold {
				val captor = new Captor[B]()
				this.subscribe(new MonoObserver[A] { // TODO: extend PolyObserver and subscribe myself the first time.
					override def onSuccess(a: A): Unit = {
						f(a).subscribe(new MonoObserver[B] {
							override def onSuccess(b: B): Unit = captor.capture(b)

							override def onError(ex: Throwable): Unit = captor.fail(ex)
						})
					}

					override def onError(ex: Throwable): Unit = captor.fail(ex)
				})
				captor: Observable[B]
			}(new Failed(_))(f)
		}

		@targetName("flatMapCapturer")
		override def flatMap[B](f: A => Capturer[B]): Capturer[B] = {
			state.fold {
				val captor = new Captor[B]()
				this.subscribe(new MonoObserver[A] {
					override def onSuccess(a: A): Unit = {
						f(a).subscribe(new MonoObserver[B] {
							override def onSuccess(b: B): Unit = captor.capture(b)

							override def onError(ex: Throwable): Unit = captor.fail(ex)
						})
					}

					override def onError(ex: Throwable): Unit = captor.fail(ex)
				})
				captor
			}(new Failed(_))(f)
		}

		override def mapGuarded[B](f: A => B): Capturer[B] = {
			state.fold {
				val captor = new Captor[B]()
				this.subscribe(new MonoObserver[A] {
					override def onSuccess(a: A): Unit = {
						val maybeB = try Maybe(f(a)) catch {
							case NonFatal(e) =>
								captor.fail(e)
								Maybe.empty
						}
						maybeB.foreach(captor.capture)
					}

					override def onError(ex: Throwable): Unit = captor.fail(ex)
				})
				captor
			}(new Failed(_)) { a =>
				try new Keeper(f(a)) catch {
					case NonFatal(e) => new Failed(e)
				}
			}
		}

		override def flatMapGuarded[B](f: A => Observable[B]): Observable[B] = {
			state.fold {
				val captor = new Captor[B]()
				this.subscribe(new MonoObserver[A] {
					override def onSuccess(a: A): Unit = {
						val maybeObB = try Maybe(f(a)) catch {
							case NonFatal(e) =>
								captor.fail(e)
								Maybe.empty
						}
						maybeObB.foreach { obB =>
							obB.subscribe(new MonoObserver[B] {
								override def onSuccess(b: B): Unit = captor.capture(b)

								override def onError(ex: Throwable): Unit = captor.fail(ex)
							})
						}
					}

					override def onError(ex: Throwable): Unit = captor.fail(ex)
				})
				captor: Observable[B]
			}(new Failed(_)) { a =>
				try f(a) catch {
					case NonFatal(e) => new Failed(e)
				}
			}
		}

		@targetName("flatMapCapturerGuarded")
		override def flatMapGuarded[B](f: A => Capturer[B]): Capturer[B] = {
			state.fold {
				val captor = new Captor[B]()
				this.subscribe(new MonoObserver[A] {
					override def onSuccess(a: A): Unit = {
						val maybeCapturerB = try Maybe(f(a)) catch {
							case NonFatal(e) =>
								captor.fail(e)
								Maybe.empty
						}
						maybeCapturerB.foreach { capturerB =>
							capturerB.subscribe(new MonoObserver[B] {
								override def onSuccess(b: B): Unit = captor.capture(b)

								override def onError(ex: Throwable): Unit = captor.fail(ex)
							})
						}
					}

					override def onError(ex: Throwable): Unit = captor.fail(ex)
				})
				captor
			}(new Failed(_)) { a =>
				try f(a) catch {
					case NonFatal(e) => new Failed(e)
				}
			}
		}

		def capture(result: A): Unit = {
			if state.isEmpty then {
				state = Trial.success(result)
				foreachEntry(_.onSuccess(result))
				clear()
			}
		}

		def fail(ex: Throwable): Unit = {
			if state.isEmpty then {
				state = Trial.failure(ex)
				foreachEntry(_.onError(ex))
				clear()
			}
		}
	}

	class GuardedCapturer[+A](underlying: Capturer[A]) extends Capturer[A] {
		override def trial: Trial[A] = underlying.trial

		override def maybeValue: Maybe[A] = underlying.maybeValue

		override def isCompleted: Boolean = underlying.isCompleted

		override def subscribe(key: Key, monoObserver: MonoObserver[A]): Unit = underlying.subscribe(key, monoObserver)

		override def subscribe(monoObserver: MonoObserver[A]): Unit = underlying.subscribe(monoObserver)

		override def unsubscribe(key: Key): Unit = underlying.unsubscribe(key)

		override def unsubscribe(monoObserver: MonoObserver[A]): Unit = underlying.unsubscribe(monoObserver)

		override def countObserversAssociatedTo(key: Key): Int = underlying.countObserversAssociatedTo(key)

		override def countOccurrencesOf(monoObserver: MonoObserver[A]): Int = underlying.countOccurrencesOf(monoObserver)

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

	trait FluxObserver[-A] {
		def onNext(a: A, index: Int): Unit

		def onError(ex: Throwable): Unit

		def onComplete(): Unit
	}

	trait Flux[+A] { thisFlux =>
		def subscribe(observer: FluxObserver[A], key: Key | Null = null): Unit

		inline def subscribeCallbacks(inline next: (A, Int) => Unit, inline error: Throwable => Unit = _ => (), inline complete: () => Unit = () => (), key: Key | Null = null): Unit = {
			subscribe(
				new FluxObserver[A] {
					override def onNext(value: A, index: Int): Unit = next(value, index)

					override def onError(ex: Throwable): Unit = error(ex)

					override def onComplete(): Unit = complete()
				},
				key
			)
		}

		def unsubscribe(observer: FluxObserver[A]): Unit

		def unsubscribe(key: Key): Unit

		def countOccurrencesOf(observer: FluxObserver[A]): Int

		def countObserversAssociatedTo(key: Key): Int

		inline def isSubscribed(observer: FluxObserver[A]): Boolean = countOccurrencesOf(observer) != 0

		inline def isSubscribed(key: Key): Boolean = countObserversAssociatedTo(key) != 0

		inline def foreach(inline consumer: A => Unit): Unit = subscribeCallbacks(next = (a, _) => consumer(a))

		inline def foreachWithCoords(inline consumer: (A, Int) => Unit): Unit = subscribeCallbacks(consumer)

		def map[B: ClassTag](f: A => B): Flux[B]

		def mapWithIndex[B: ClassTag](f: (A, Int) => B): Flux[B]

		def flatMap[B: ClassTag](f: A => Flux[B]): Tensor[B]

		def flatMapWithIndex[B: ClassTag](f: (A, Int) => Flux[B]): Tensor[B]

		def scan[B: ClassTag](initial: B)(f: (B, A, Int) => B): Flux[B]

		def buffer[T >: A : ClassTag](size: Int): Flux[IArray[T]]

		def zip[B, C: ClassTag](other: Flux[B])(f: (A, B, Int) => C): Flux[C]

		def take(n: Int): Flux[A]

		def takeWhile(p: (a: A, index: Int, count: Int) => Boolean): Flux[A]

		/** Collapses the flux into a single value, allowing early termination via Maybe.empty. */
		def foldWhileGuarded[B](initial: B)(f: (B, A, Int) => Maybe[B]): Task[B] = (monoObserverB: MonoObserver[B]) => {
			thisFlux.subscribe(new FluxObserver[A] {
				private var state: B = initial
				private var active = true // TODO call `thisFlux.unsubscribe(this)` whenever it is set to false.

				override def onNext(a: A, index: Int): Unit = {
					if active then {
						val maybeB = try f(state, a, index) catch {
							case NonFatal(ex) =>
								active = false
								monoObserverB.onError(ex)
								Maybe.empty
						}
						if active then {
							maybeB.fold {
								active = false
								monoObserverB.onSuccess(state)
							} { b => state = b }
						}
					}
				}

				override def onError(ex: Throwable): Unit = {
					active = false
					monoObserverB.onError(ex)
				}

				override def onComplete(): Unit = {
					active = false
					monoObserverB.onSuccess(state)
				}
			})
		}
	}


	object Flux {
		def empty[A]: Flux[A] = new DefaultFlux[A] with ImmediateCompletion[A] {
			override def subscribe(observer: FluxObserver[A], key: Key | Null = null): Unit = observer.onComplete()
		}

		def apply[A](elements: A*): Flux[A] = fromIterable(elements)

		def fromIterable[A](iterable: Iterable[A]): Flux[A] = new DefaultFlux[A] with ImmediateCompletion[A] {
			override def subscribe(observer: FluxObserver[A], key: Key | Null = null): Unit = {
				val it = iterable.iterator
				var index = 0
				while it.hasNext do {
					val v = it.next()
					observer.onNext(v, index)
					index += 1
				}
				observer.onComplete()
				// TODO call `this.unsubscribe(observer)`.
			}
		}

		def fromIterableGuarded[A](iterable: Iterable[A]): Flux[A] = new DefaultFlux[A] with ImmediateCompletion[A] {
			override def subscribe(observer: FluxObserver[A], key: Key | Null = null): Unit = {
				val it = iterable.iterator
				var index = 0
				var active = true // TODO call `this.unsubscribe(observer)` whenever it is set to false.
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
							observer.onNext(v, index)
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

		def generate[A](supplier: Int => A): Flux[A] = new DefaultFlux[A] with ImmediateCompletion[A] {
			override def subscribe(observer: FluxObserver[A], key: Key | Null = null): Unit = {
				var index = 0
				var active = true // TODO call `this.unsubscribe(observer)` whenever it is set to false.
				while active do {
					val maybeVal = try Maybe(supplier(index)) catch {
						case NonFatal(e) =>
							active = false
							observer.onError(e)
							Maybe.empty
					}
					maybeVal.foreach { v =>
						observer.onNext(v, index)
						index += 1
					}
				}
			}
		}

		def generateStatefully[A](supplierBuilder: () => Int => A): Flux[A] = new DefaultFlux[A] with ImmediateCompletion[A] {
			override def subscribe(observer: FluxObserver[A], key: Key | Null = null): Unit = {
				val supplier = supplierBuilder()
				var index = 0
				var active = true // TODO call `this.unsubscribe(observer)` whenever it is set to false.
				while active do {
					val maybeVal = try Maybe(supplier(index)) catch {
						case NonFatal(e) =>
							active = false
							observer.onError(e)
							Maybe.empty
					}
					maybeVal.foreach { v =>
						observer.onNext(v, index)
						index += 1
					}
				}
			}
		}

		def fromObservablesArray[A](array: IArray[Observable[A]]): Flux[A] = new DefaultFlux[A] {
			override def subscribe(fluxObserver: FluxObserver[A], key: Key | Null = null): Unit = {

				class AllElemsObserver extends MonoObserver[A] {
					private var sequenceIndex = 0
					private var active = true

					override def onSuccess(value: A): Unit = {
						if active then {
							val si = sequenceIndex
							sequenceIndex += 1
							fluxObserver.onNext(value, si)
							if sequenceIndex == array.length then fluxObserver.onComplete()
						}
					}

					override def onError(ex: Throwable): Unit = {
						if active then {
							active = false
							fluxObserver.onError(ex)
						}
					}
				}
				val allElemsObserver = new AllElemsObserver
				array.foreachWithIndex { (observable, index) => observable.subscribe(allElemsObserver) }
			}
		}
	}

	/////////////////////////////////////
	//// Specialized abstract Fluxes ////
	/////////////////////////////////////

	/** Partial implementation of [[Flux]] */
	trait DefaultFlux[+A] extends Flux[A] {
		override def map[B: ClassTag](f: A => B): Flux[B] = new Flux_Map(this, f)

		override def mapWithIndex[B: ClassTag](f: (A, Int) => B): Flux[B] = new Flux_MapWithIndex(this, f)

		override def flatMap[B: ClassTag](f: A => Flux[B]): Tensor[B] = new Flux_FlatMap(this, f)

		override def flatMapWithIndex[B: ClassTag](f: (A, Int) => Flux[B]): Tensor[B] = new Flux_FlatMapWithIndex(this, f)

		override def scan[B: ClassTag](initial: B)(f: (B, A, Int) => B): Flux[B] = new Flux_Scan(this, initial, f)

		override def buffer[T >: A : ClassTag](size: Int): Flux[IArray[T]] = new Flux_Buffer[A, T](this, size)

		override def zip[B, C: ClassTag](other: Flux[B])(f: (A, B, Int) => C): Flux[C] = new Flux_Zip(this, other, f)

		override def take(n: Int): Flux[A] = new Flux_Take(this, n)

		override def takeWhile(p: (a: A, index: Int, count: Int) => Boolean): Flux[A] = new Flux_TakeWhile(this, p)
	}


	trait SingleSlotFluxOp[A, +B] extends DefaultFlux[B], FluxObserver[A] {
		protected val source: Flux[A]

		private var downChainObserverSlot: FluxObserver[B] @uncheckedVariance = uninitialized

		override def subscribe(downChainObserver: FluxObserver[B], key: Key | Null = null): Unit = {
			if downChainObserverSlot == null then {
				this.downChainObserverSlot = downChainObserver
				source.subscribe(this)
			} else {
				source.subscribe(createDelegate(downChainObserver))
			}
		}

		protected def createDelegate(downChainObserver: FluxObserver[B]): FluxObserver[A]

		protected def resetState(): Unit = ()

		protected def forwardNext(value: B @uncheckedVariance, index: Int): Unit = {
			val obs = downChainObserverSlot
			if obs != null then obs.onNext(value, index)
		}

		protected def forwardError(ex: Throwable): Unit = {
			val obs = downChainObserverSlot
			downChainObserverSlot = null
			resetState()
			if obs != null then obs.onError(ex)
		}

		protected def forwardComplete(): Unit = {
			val obs = downChainObserverSlot
			downChainObserverSlot = null
			resetState()
			if obs != null then obs.onComplete()
		}
	}

	trait ImmediateCompletion[+A] { thisFlux: Flux[A] =>
		override def unsubscribe(observer: FluxObserver[A]): Unit = ()

		override def unsubscribe(key: Key): Unit = ()

		override def countOccurrencesOf(observer: FluxObserver[A]): Int = 0

		override def countObserversAssociatedTo(key: Key): Int = 0
	}

	/////////////////////////
	//// Concrete Fluxes ////
	/////////////////////////

	final class StreamEmitter[A] extends Muxer[A, FluxObserver], DefaultFlux[A] {
		private var counter = 0
		private var completed = false
		private var error: Throwable | Null = null

		override def subscribe(observer: FluxObserver[A], key: Key | Null = null): Unit = {
			if error != null then observer.onError(error.asInstanceOf[Throwable])
			else if completed then observer.onComplete()
			else addEntry(observer)
		}

		def emit(value: A): Unit = {
			if !completed && error == null then {
				val idx = counter
				counter += 1
				foreachEntry(_.onNext(value, idx))
			}
		}

		def fail(ex: Throwable): Unit = {
			if !completed && error == null then {
				error = ex
				foreachEntry(_.onError(ex))
				clear()
			}
		}

		def end(): Unit = {
			if !completed && error == null then {
				completed = true
				foreachEntry(_.onComplete())
				clear()
			}
		}
	}

	//////////////////////////////////////////////////
	//// Classes for operations that return a Flux ////
	//////////////////////////////////////////////////

	final class Flux_Map[A, B](val source: Flux[A], val f: A => B) extends SingleSlotFluxOp[A, B] {
		override def onNext(a: A, index: Int): Unit = forwardNext(f(a), index)

		override def onError(ex: Throwable): Unit = forwardError(ex)

		override def onComplete(): Unit = forwardComplete()

		override protected def createDelegate(observer: FluxObserver[B]): FluxObserver[A] = new FluxObserver[A] {
			override def onNext(a: A, index: Int): Unit = observer.onNext(f(a), index)

			override def onError(ex: Throwable): Unit = observer.onError(ex)

			override def onComplete(): Unit = observer.onComplete()
		}
	}

	final class Flux_MapWithIndex[A, B](val source: Flux[A], val f: (A, Int) => B) extends SingleSlotFluxOp[A, B] {
		override def onNext(a: A, index: Int): Unit = forwardNext(f(a, index), index)

		override def onError(ex: Throwable): Unit = forwardError(ex)

		override def onComplete(): Unit = forwardComplete()

		override protected def createDelegate(observer: FluxObserver[B]): FluxObserver[A] = new FluxObserver[A] {
			override def onNext(a: A, index: Int): Unit = observer.onNext(f(a, index), index)

			override def onError(ex: Throwable): Unit = observer.onError(ex)

			override def onComplete(): Unit = observer.onComplete()
		}
	}

	final class Flux_Scan[A, B](val source: Flux[A], val initial: B, val f: (B, A, Int) => B) extends SingleSlotFluxOp[A, B] {

		private var state = initial

		override protected def resetState(): Unit = state = initial

		override def onNext(a: A, index: Int): Unit = {
			state = f(state, a, index)
			forwardNext(state, index)
		}

		override def onError(ex: Throwable): Unit = forwardError(ex)

		override def onComplete(): Unit = forwardComplete()

		override protected def createDelegate(observer: FluxObserver[B]): FluxObserver[A] = new ScanObserver(initial, f, observer)
	}

	final class Flux_Buffer[A, T >: A : ClassTag](val source: Flux[A], val size: Int) extends SingleSlotFluxOp[A, IArray[T]] {

		private var buffer = new Array[T](size)
		private var count = 0
		private var chunkIndex = 0
		private var active = true

		override protected def resetState(): Unit = {
			count = 0
			chunkIndex = 0
			active = true
		}

		override def onNext(a: A, index: Int): Unit = {
			if active then {
				buffer(count) = a
				count += 1
				if count == size then {
					val chunk = IArray.unsafeFromArray(buffer)
					buffer = new Array[T](size)
					count = 0
					val idx = chunkIndex
					chunkIndex += 1
					forwardNext(chunk, index)
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
					forwardNext(partial, chunkIndex)
				}
				forwardComplete()
			}
		}

		override protected def createDelegate(observer: FluxObserver[IArray[T]]): FluxObserver[A] = new BufferedObserver[A, T](size, observer)
	}

	private final class Flux_Zip[A, B, C](val left: Flux[A], val right: Flux[B], val f: (A, B, Int) => C) extends DefaultFlux[C] {
		override def subscribe(observer: FluxObserver[C], key: Key | Null = null): Unit = {
			new ZipObservation(left, right, f, observer).start()
		}
	}

	final class Flux_Take[A](val source: Flux[A], val n: Int) extends SingleSlotFluxOp[A, A] {
		private var count = 0
		private var active = true

		override protected def resetState(): Unit = {
			count = 0
			active = true
		}

		override def onNext(a: A, index: Int): Unit = {
			if active then {
				if count < n then {
					val currentCount = count
					count += 1
					forwardNext(a, index)
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

		override protected def createDelegate(observer: FluxObserver[A]): FluxObserver[A] = new TakeObserver(n, observer)
	}

	final class Flux_TakeWhile[A](val source: Flux[A], val p: (a: A, index: Int, count: Int) => Boolean, flattenToCount: Boolean = true) extends SingleSlotFluxOp[A, A] {
		private var active = true
		private var counter = 0

		override protected def resetState(): Unit = {
			active = true
			counter = 0
		}

		override def onNext(a: A, index: Int): Unit = {
			if active then {
				if p(a, index, counter) then {
					val currentCounter = counter
					counter += 1
					forwardNext(a, if flattenToCount then currentCounter else index)
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

		override protected def createDelegate(observer: FluxObserver[A]): FluxObserver[A] = new TakeWhileObserver(p, flattenToCount, observer)
	}

	/////////////////////////////////////////////////////
	//// Classes for operations that return a Tensor ////
	/////////////////////////////////////////////////////


	final class Flux_FlatMap[A, B](val outerFlux: Flux[A], val f: A => Flux[B]) extends DefaultTensor[B], FluxObserver[A] {

		private var downChainObserverSlot: TensorObserver[B] | Null = null
		private var outerFluxCompleted = false
		private var activeInnerFluxesCount = 0

		private def resetState(): Unit = {
			downChainObserverSlot = null
			outerFluxCompleted = false
			activeInnerFluxesCount = 0
		}

		override def subscribe(tensorObserver: TensorObserver[B]): Unit = {
			if downChainObserverSlot != null then outerFlux.subscribe(new FlatMapObserver(f, tensorObserver))
			else {
				downChainObserverSlot = tensorObserver
				outerFlux.subscribe(this)
			}
		}

		override def onNext(a: A, outer: Int): Unit = {
			activeInnerFluxesCount += 1
			f(a).subscribe(new InnerObserver(outer))
		}

		override def onError(ex: Throwable): Unit = {
			val obs = downChainObserverSlot
			resetState()
			if obs != null then obs.onError(ex)
		}

		override def onComplete(): Unit = {
			outerFluxCompleted = true
			val obs = downChainObserverSlot
			if obs != null then obs.onOuterComplete()
			tryComplete()
		}

		private final class InnerObserver(outer: Int) extends FluxObserver[B] {
			override def onNext(b: B, inner: Int): Unit = {
				val obs = downChainObserverSlot
				if obs != null then obs.onNext(b, inner, outer)
			}

			override def onError(ex: Throwable): Unit = Flux_FlatMap.this.onError(ex)

			override def onComplete(): Unit = {
				activeInnerFluxesCount -= 1
				val obs = downChainObserverSlot
				if obs != null then obs.onInnerComplete(outer)
				tryComplete()
			}
		}

		private def tryComplete(): Unit = {
			if outerFluxCompleted && activeInnerFluxesCount == 0 then {
				val obs = downChainObserverSlot
				resetState()
				if obs != null then obs.onComplete()
			}
		}
	}

	final class Flux_FlatMapWithIndex[A, B](val outerFlux: Flux[A], val f: (A, Int) => Flux[B]) extends DefaultTensor[B], FluxObserver[A] {

		private var downChainObserverSlot: TensorObserver[B] | Null = null
		private var outerFluxCompleted = false
		private var activeInnerFluxesCount = 0

		private def resetState(): Unit = {
			downChainObserverSlot = null
			outerFluxCompleted = false
			activeInnerFluxesCount = 0
		}

		override def subscribe(observer: TensorObserver[B]): Unit = {
			if downChainObserverSlot != null then outerFlux.subscribe(new FlatMapWithIndexObserver(f, observer))
			else {
				downChainObserverSlot = observer
				outerFlux.subscribe(this)
			}
		}

		override def onNext(a: A, outer: Int): Unit = {
			activeInnerFluxesCount += 1
			f(a, outer).subscribe(new InnerObserver(outer))
		}

		override def onError(ex: Throwable): Unit = {
			val obs = downChainObserverSlot
			resetState()
			if obs != null then obs.onError(ex)
		}

		override def onComplete(): Unit = {
			outerFluxCompleted = true
			val obs = downChainObserverSlot
			if obs != null then obs.onOuterComplete()
			tryComplete()
		}

		private final class InnerObserver(outer: Int) extends FluxObserver[B] {
			override def onNext(b: B, inner: Int): Unit = {
				val obs = downChainObserverSlot
				if obs != null then obs.onNext(b, inner, outer)
			}

			override def onError(ex: Throwable): Unit = Flux_FlatMapWithIndex.this.onError(ex)

			override def onComplete(): Unit = {
				activeInnerFluxesCount -= 1
				val obs = downChainObserverSlot
				if obs != null then obs.onInnerComplete(outer)
				tryComplete()
			}
		}

		private def tryComplete(): Unit = {
			if outerFluxCompleted && activeInnerFluxesCount == 0 then {
				val obs = downChainObserverSlot
				resetState()
				if obs != null then obs.onComplete()
			}
		}
	}

	/////////////////////////////////////////////////////////////////////
	//// UpChain observers produced by operations that return a Flux ////
	/////////////////////////////////////////////////////////////////////	

	private final class ScanObserver[A, B](initial: B, f: (B, A, Int) => B, observer: FluxObserver[B]) extends FluxObserver[A] {
		private var state = initial

		override def onNext(a: A, index: Int): Unit = {
			state = f(state, a, index)
			observer.onNext(state, index)
		}

		override def onError(ex: Throwable): Unit = observer.onError(ex)

		override def onComplete(): Unit = observer.onComplete()
	}

	private final class BufferedObserver[A, T >: A : ClassTag](size: Int, observer: FluxObserver[IArray[T]]) extends FluxObserver[A] {
		private var buffer = new Array[T](size)
		private var count = 0
		private var chunkIndex = 0
		private var active = true

		override def onNext(a: A, originalIndex: Int): Unit = {
			if active then {
				buffer(count) = a
				count += 1
				if count == size then {
					val chunk = IArray.unsafeFromArray(buffer)
					buffer = new Array[T](size)
					count = 0
					val idx = chunkIndex
					chunkIndex += 1
					observer.onNext(chunk, idx)
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
					observer.onNext(partial, chunkIndex)
				}
				observer.onComplete()
			}
		}
	}

	private final class ZipObservation[A, B, C](left: Flux[A], right: Flux[B], f: (A, B, Int) => C, observer: FluxObserver[C]) {
		private val leftValues = scala.collection.mutable.Map[Int, A]()
		private val rightValues = scala.collection.mutable.Map[Int, B]()
		private var leftCompleted = false
		private var rightCompleted = false
		private var errorFired = false

		def start(): Unit = {
			left.subscribe(new LeftObserver)
			right.subscribe(new RightObserver)
		}

		private final class LeftObserver extends FluxObserver[A] {
			override def onNext(a: A, leftIndex: Int): Unit = {
				rightValues.remove(leftIndex) match {
					case Some(b) =>
						observer.onNext(f(a, b, leftIndex), leftIndex)
						checkComplete()
					case None =>
						leftValues(leftIndex) = a
				}
			}

			override def onError(ex: Throwable): Unit = fireError(ex)

			override def onComplete(): Unit = {
				leftCompleted = true
				checkComplete()
			}
		}

		private final class RightObserver extends FluxObserver[B] {
			override def onNext(b: B, rightIndex: Int): Unit = {
				leftValues.remove(rightIndex) match {
					case Some(a) =>
						observer.onNext(f(a, b, rightIndex), rightIndex)
						checkComplete()
					case None =>
						rightValues(rightIndex) = b
				}
			}

			override def onError(ex: Throwable): Unit = fireError(ex)

			override def onComplete(): Unit = {
				rightCompleted = true
				checkComplete()
			}
		}

		private def checkComplete(): Unit = if (leftCompleted && leftValues.isEmpty) || (rightCompleted && rightValues.isEmpty) || (leftCompleted && rightCompleted) then observer.onComplete()

		private def fireError(ex: Throwable): Unit = {
			if !errorFired then {
				errorFired = true
				observer.onError(ex)
			}
		}
	}

	private final class TakeObserver[A](n: Int, observer: FluxObserver[A]) extends FluxObserver[A] {
		private var count = 0
		private var active = true

		override def onNext(a: A, index: Int): Unit = {
			if active then {
				if count < n then {
					val currentCount = count
					count += 1
					observer.onNext(a, index)
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

	private final class TakeWhileObserver[A](p: (a: A, index: Int, count: Int) => Boolean, flattenToCount: Boolean, observer: FluxObserver[A]) extends FluxObserver[A] {
		private var active = true
		private var counter = 0

		override def onNext(a: A, index: Int): Unit = {
			if active then {
				if p(a, index, counter) then {
					val currentCounter = counter
					counter += 1
					observer.onNext(a, if flattenToCount then currentCounter else index)
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

	////////////////////////////////////////////////////////////////////////////////////////////////
	/// UpChain Observers produced by operations that return a Tensor when the slot is occupied ///
	////////////////////////////////////////////////////////////////////////////////////////////////

	private final class FlatMapObserver[A, B](f: A => Flux[B], tensorObserver: TensorObserver[B]) extends FluxObserver[A] {
		private var outerFluxCompleted = false
		private var activeInnerFluxesCount = 0
		private var allCompleted = false

		override def onNext(a: A, outerIndex: Int): Unit = {
			activeInnerFluxesCount += 1
			f(a).subscribe(new FluxObserver[B] {
				override def onNext(b: B, innerIndex: Int): Unit = if !allCompleted then tensorObserver.onNext(b, innerIndex, outerIndex)

				override def onError(ex: Throwable): Unit = FlatMapObserver.this.onError(ex)

				override def onComplete(): Unit = {
					activeInnerFluxesCount -= 1
					tensorObserver.onInnerComplete(outerIndex)
					tryComplete()
				}
			})
		}

		override def onError(ex: Throwable): Unit = {
			if !allCompleted then {
				allCompleted = true
				tensorObserver.onError(ex)
			}
		}

		override def onComplete(): Unit = {
			outerFluxCompleted = true
			tensorObserver.onOuterComplete()
			tryComplete()
		}

		inline def tryComplete(): Unit = {
			if outerFluxCompleted && activeInnerFluxesCount == 0 && !allCompleted then {
				allCompleted = true
				tensorObserver.onComplete()
			}
		}
	}

	private final class FlatMapWithIndexObserver[A, B](f: (A, Int) => Flux[B], tensorObserver: TensorObserver[B]) extends FluxObserver[A] {
		private var outerFluxCompleted = false
		private var activeInnerFluxesCount = 0
		private var allCompleted = false

		override def onNext(a: A, outerIndex: Int): Unit = {
			activeInnerFluxesCount += 1
			f(a, outerIndex).subscribe(new FluxObserver[B] {
				override def onNext(b: B, innerIndex: Int): Unit = if !allCompleted then tensorObserver.onNext(b, innerIndex, outerIndex)

				override def onError(ex: Throwable): Unit = FlatMapWithIndexObserver.this.onError(ex)

				override def onComplete(): Unit = {
					activeInnerFluxesCount -= 1
					tensorObserver.onInnerComplete(outerIndex)
					tryComplete()
				}
			})
		}

		override def onError(ex: Throwable): Unit = {
			if !allCompleted then {
				allCompleted = true
				tensorObserver.onError(ex)
			}
		}

		override def onComplete(): Unit = {
			outerFluxCompleted = true
			tensorObserver.onOuterComplete()
			tryComplete()
		}

		inline def tryComplete(): Unit = {
			if outerFluxCompleted && activeInnerFluxesCount == 0 && !allCompleted then {
				allCompleted = true
				tensorObserver.onComplete()
			}
		}
	}

	///////////////////////////////////////
	/////////////// Tensor ////////////////
	///////////////////////////////////////

	trait TensorObserver[-A] {
		def onNext(a: A, innerIndex: Int, outerIndex: Int): Unit

		def onOuterComplete(): Unit

		def onInnerComplete(outerIndex: Int): Unit

		def onError(ex: Throwable): Unit

		def onComplete(): Unit
	}

	trait Tensor[+A] {
		def subscribe(observer: TensorObserver[A]): Unit

		inline def subscribeCallbacks(inline next: (a: A, inner: Int, outer: Int) => Unit, inline error: Throwable => Unit = _ => (), inline outerComplete: () => Unit = () => (), inline innerComplete: Int => Unit = _ => (), inline complete: () => Unit = () => ()): Unit = {
			subscribe(new TensorObserver[A] {
				override def onNext(value: A, inner: Int, outer: Int): Unit = next(value, inner, outer)

				override def onOuterComplete(): Unit = outerComplete()

				override def onInnerComplete(outerIndex: Int): Unit = innerComplete(outerIndex)

				override def onError(ex: Throwable): Unit = error(ex)

				override def onComplete(): Unit = complete()
			})
		}

		def flattenInner: Flux[A]

		def flattenOuter: Flux[A]

		def flattenSequential: Flux[A]

		def flattenWith(f: (a: A, inner: Int, outer: Int, count: Int) => Int): Flux[A]

		def flattenStatefully[B: ClassTag](flattenerBuilder: () => TensorFlattener[A, B]): Flux[B]
	}

	trait TensorFlattener[-A, +B] {
		def onNext(downChainObserver: FluxObserver[B])(a: A, innerIndex: Int, outerIndex: Int): Unit

		def onError(downChainObserver: FluxObserver[B])(ex: Throwable): Unit

		def onOuterComplete(downChainObserver: FluxObserver[B]): Unit

		def onInnerComplete(downChainObserver: FluxObserver[B])(outerIndex: Int): Unit

		def onComplete(downChainObserver: FluxObserver[B]): Unit
	}

	////////////////////////////////
	//// Tensor specializations ////
	////////////////////////////////

	trait DefaultTensor[+A] extends Tensor[A] {
		override def flattenInner: Flux[A] = new Tensor_FlattenInner(this)

		override def flattenOuter: Flux[A] = new Tensor_FlattenOuter(this)

		override def flattenSequential: Flux[A] = new Tensor_FlattenSequential(this)

		override def flattenWith(f: (A, Int, Int, Int) => Int): Flux[A] = new Tensor_FlattenWith(this, f)

		override def flattenStatefully[B: ClassTag](flattenerBuilder: () => TensorFlattener[A, B]): Flux[B] = new Tensor_FlattenStatefully(this, flattenerBuilder)
	}

	trait SingleSlotTensorOp[A, +B] extends DefaultFlux[B], TensorObserver[A] {
		protected val source: Tensor[A]

		private var downChainObserverSlot: FluxObserver[B] @uncheckedVariance | Null = null

		override def subscribe(observer: FluxObserver[B], key: Key | Null = null): Unit = {
			if downChainObserverSlot != null then source.subscribe(createDelegate(observer))
			else {
				downChainObserverSlot = observer
				source.subscribe(this)
			}
		}

		protected def createDelegate(observer: FluxObserver[B]): TensorObserver[A]

		protected def resetState(): Unit = downChainObserverSlot = null

		protected def forwardNext(value: B @uncheckedVariance, index: Int): Unit = {
			val obs = downChainObserverSlot
			if obs != null then obs.onNext(value, index)
		}

		protected def forwardError(ex: Throwable): Unit = {
			val obs = downChainObserverSlot
			resetState()
			if obs != null then obs.onError(ex)
		}

		protected def forwardComplete(): Unit = {
			val obs = downChainObserverSlot
			resetState()
			if obs != null then obs.onComplete()
		}
	}

	///////////////////////////////////////
	//// Classes for Tensor operations ////
	///////////////////////////////////////	

	final class Tensor_FlattenInner[A](override protected val source: Tensor[A]) extends SingleSlotTensorOp[A, A] {
		override def onNext(a: A, inner: Int, outer: Int): Unit = forwardNext(a, inner)

		override def onOuterComplete(): Unit = ()

		override def onInnerComplete(outerIndex: Int): Unit = ()

		override def onError(ex: Throwable): Unit = forwardError(ex)

		override def onComplete(): Unit = forwardComplete()

		override protected def createDelegate(observer: FluxObserver[A]): TensorObserver[A] = new TensorObserver[A] {
			override def onNext(a: A, inner: Int, outer: Int): Unit = observer.onNext(a, inner)

			override def onOuterComplete(): Unit = ()

			override def onInnerComplete(outerIndex: Int): Unit = ()

			override def onError(ex: Throwable): Unit = observer.onError(ex)

			override def onComplete(): Unit = observer.onComplete()
		}
	}

	final class Tensor_FlattenOuter[A](override protected val source: Tensor[A]) extends SingleSlotTensorOp[A, A] {
		override def onNext(a: A, inner: Int, outer: Int): Unit = forwardNext(a, outer)

		override def onOuterComplete(): Unit = ()

		override def onInnerComplete(outerIndex: Int): Unit = ()

		override def onError(ex: Throwable): Unit = forwardError(ex)

		override def onComplete(): Unit = forwardComplete()

		override protected def createDelegate(observer: FluxObserver[A]): TensorObserver[A] = new TensorObserver[A] {
			override def onNext(a: A, inner: Int, outer: Int): Unit = observer.onNext(a, outer)

			override def onOuterComplete(): Unit = ()

			override def onInnerComplete(outerIndex: Int): Unit = ()

			override def onError(ex: Throwable): Unit = observer.onError(ex)

			override def onComplete(): Unit = observer.onComplete()
		}
	}

	final class Tensor_FlattenSequential[A](override protected val source: Tensor[A]) extends SingleSlotTensorOp[A, A] {
		private var counter = 0

		override protected def resetState(): Unit = {
			super.resetState()
			counter = 0
		}

		override def onNext(a: A, inner: Int, outer: Int): Unit = {
			val idx = counter
			counter += 1
			forwardNext(a, idx)
		}

		override def onOuterComplete(): Unit = ()

		override def onInnerComplete(outerIndex: Int): Unit = ()

		override def onError(ex: Throwable): Unit = forwardError(ex)

		override def onComplete(): Unit = forwardComplete()

		override protected def createDelegate(observer: FluxObserver[A]): TensorObserver[A] = new TensorObserver {
			private var counter = 0

			override def onNext(a: A, up: Int, down: Int): Unit = {
				val index = counter
				counter += 1
				observer.onNext(a, index)
			}

			override def onOuterComplete(): Unit = ()

			override def onInnerComplete(outerIndex: Int): Unit = ()

			override def onError(ex: Throwable): Unit = observer.onError(ex)

			override def onComplete(): Unit = observer.onComplete()
		}
	}

	final class Tensor_FlattenWith[A](override protected val source: Tensor[A], val f: (A, Int, Int, Int) => Int) extends SingleSlotTensorOp[A, A] { // TODO add sequential index to the signature
		private var counter = 0

		override protected def resetState(): Unit = {
			super.resetState()
			counter = 0
		}

		override def onNext(a: A, inner: Int, outer: Int): Unit = {
			val count = counter
			counter += 1
			forwardNext(a, f(a, inner, outer, count))
		}

		override def onOuterComplete(): Unit = ()

		override def onInnerComplete(outerIndex: Int): Unit = ()

		override def onError(ex: Throwable): Unit = forwardError(ex)

		override def onComplete(): Unit = forwardComplete()

		override protected def createDelegate(observer: FluxObserver[A]): TensorObserver[A] = new TensorObserver[A] {
			private var counter = 0

			override def onNext(a: A, inner: Int, outer: Int): Unit = {
				val count = counter
				counter += 1
				observer.onNext(a, f(a, inner, outer, count))
			}

			override def onOuterComplete(): Unit = ()

			override def onInnerComplete(outerIndex: Int): Unit = ()

			override def onError(ex: Throwable): Unit = observer.onError(ex)

			override def onComplete(): Unit = observer.onComplete()
		}
	}

	final class Tensor_FlattenStatefully[A, B](source: Tensor[A], flattenerBuilder: () => TensorFlattener[A, B]) extends DefaultFlux[B], TensorObserver[A] {
		private var downChainObserverSlot: FluxObserver[B] | Null = null
		private var flattenerSlot: TensorFlattener[A, B] | Null = null

		override def subscribe(observer: FluxObserver[B]): Unit = {
			val flattener = flattenerBuilder()
			if downChainObserverSlot != null then source.subscribe(createDelegate(observer, flattener))
			else {
				downChainObserverSlot = observer
				flattenerSlot = flattener
				source.subscribe(this)
			}
		}

		private def createDelegate(observer: FluxObserver[B], flattener: TensorFlattener[A, B]): TensorObserver[A] = new TensorObserver[A] {
			override def onNext(a: A, innerIndex: Int, outerIndex: Int): Unit = flattener.onNext(observer)(a, innerIndex, outerIndex)

			override def onOuterComplete(): Unit = flattener.onOuterComplete(observer)

			override def onInnerComplete(outerIndex: Int): Unit = flattener.onInnerComplete(observer)(outerIndex)

			override def onError(ex: Throwable): Unit = flattener.onError(observer)(ex)

			override def onComplete(): Unit = flattener.onComplete(observer)
		}

		override def onNext(a: A, innerIndex: Int, outerIndex: Int): Unit = {
			val dco = downChainObserverSlot
			if dco != null then flattenerSlot.onNext(dco)(a, innerIndex, outerIndex)
		}

		override def onOuterComplete(): Unit = {
			val dco = downChainObserverSlot
			if dco != null then flattenerSlot.onOuterComplete(dco)
		}

		override def onInnerComplete(outerIndex: Int): Unit = {
			val dco = downChainObserverSlot
			if dco != null then flattenerSlot.onInnerComplete(dco)(outerIndex)
		}

		override def onError(ex: Throwable): Unit = {
			val dco = downChainObserverSlot
			downChainObserverSlot = null
			flattenerSlot = null
			if dco != null then flattenerSlot.onError(dco)(ex)
		}

		override def onComplete(): Unit = {
			val dco = downChainObserverSlot
			downChainObserverSlot = null
			flattenerSlot = null
			if dco != null then flattenerSlot.onComplete(dco)
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
