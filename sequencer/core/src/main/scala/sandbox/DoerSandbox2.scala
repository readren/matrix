package readren.sequencer
package sandbox

import readren.common.{Maybe, Trial, foreachWithIndex}

import scala.annotation.targetName
import scala.annotation.unchecked.uncheckedVariance
import scala.compiletime.uninitialized
import scala.reflect.ClassTag
import scala.util.control.NonFatal

class DoerSandbox2 {
	trait Subscription {
		def unsubscribe(): Unit
	}

	val Subscription_empty: Subscription = () => ()

	trait MonoObserver[-A] {
		def onSuccess(value: A): Unit

		def onError(ex: Throwable): Unit
	}

	/** Root super trait of all asynchronous observables. */
	trait Observable[+A] {
		def subscribe(monoObserver: MonoObserver[A]): Subscription

		inline def subscribeCallbacks(inline success: A => Unit, inline error: Throwable => Unit = _ => ()): Subscription = {
			subscribe(new MonoObserver {
				override def onSuccess(value: A): Unit = success(value)

				override def onError(ex: Throwable): Unit = error(ex)

			})
		}

		inline def foreach(inline consumer: A => Unit): Unit = {
			subscribeCallbacks(consumer); ()
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
		override def subscribe(monoObserver: MonoObserver[A]): Subscription = underlying.subscribe(monoObserver)

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

		override def map[B](f: A => B): Capturer[B]

		override def flatMap[B](f: A => Observable[B]): Observable[B]
		@targetName("flatMapCapturer")
		def flatMap[B](f: A => Capturer[B]): Capturer[B]

		def mapGuarded[B](f: A => B): Capturer[B]

		def flatMapGuarded[B](f: A => Observable[B]): Observable[B]
		@targetName("flatMapCapturerGuarded")
		def flatMapGuarded[B](f: A => Capturer[B]): Capturer[B]

		def guarded: Capturer[A] = new GuardedCapturer(thisCapturer)
	}

	final class GuardedCapturer[+A](underlying: Capturer[A]) extends Capturer[A] {
		override def trial: Trial[A] = underlying.trial

		override def maybeValue: Maybe[A] = underlying.maybeValue

		override def isCompleted: Boolean = underlying.isCompleted

		override def subscribe(monoObserver: MonoObserver[A]): Subscription = underlying.subscribe(monoObserver)

		override def map[B](f: A => B): Capturer[B] = underlying.mapGuarded(f)

		override def flatMap[B](f: A => Observable[B]): Observable[B] = underlying.flatMapGuarded(f)

		@targetName("flatMapCapturer")
		override def flatMap[B](f: A => Capturer[B]): Capturer[B] = underlying.flatMapGuarded(f)

		override def mapGuarded[B](f: A => B): Capturer[B] = underlying.mapGuarded(f)

		override def flatMapGuarded[B](f: A => Observable[B]): Observable[B] = underlying.flatMapGuarded(f)

		@targetName("flatMapCapturerGuarded")
		override def flatMapGuarded[B](f: A => Capturer[B]): Capturer[B] = underlying.flatMapGuarded(f)
	}

	/** A [[Capturer]] that has already captured a successful value. Ex ReadyTask */
	final class Keeper[+A](val value: A) extends Capturer[A] {
		override def maybeValue: Maybe[A] = Maybe(value)

		override def isCompleted: Boolean = true

		override def subscribe(monoObserver: MonoObserver[A]): Subscription = {
			monoObserver.onSuccess(value)
			Subscription_empty
		}

		override def trial: Trial[A] = Trial.success(value)

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

	final class Failed(val exception: Throwable) extends Capturer[Nothing] {
		override def maybeValue: Maybe[Nothing] = Maybe.empty

		override def isCompleted: Boolean = true

		override def subscribe(monoObserver: MonoObserver[Nothing]): Subscription = {
			monoObserver.onError(exception)
			Subscription_empty
		}

		override def trial: Trial[Nothing] = Trial.failure(exception)

		override def map[B](f: Nothing => B): Capturer[B] = this.asInstanceOf[Failed]

		override def flatMap[B](f: Nothing => Observable[B]): Observable[B] = this.asInstanceOf[Failed]

		@targetName("flatMapCapturer")
		override def flatMap[B](f: Nothing => Capturer[B]): Capturer[B] = this.asInstanceOf[Failed]

		override def mapGuarded[B](f: Nothing => B): Capturer[B] = this.asInstanceOf[Failed]

		override def flatMapGuarded[B](f: Nothing => Observable[B]): Observable[B] = this.asInstanceOf[Failed]

		@targetName("flatMapCapturerGuarded")
		override def flatMapGuarded[B](f: Nothing => Capturer[B]): Capturer[B] = this.asInstanceOf[Failed]
	}

	abstract class AbstractCaptor[A](initialState: Trial[A] = Trial.empty) extends Muxer[A, MonoObserver], Capturer[A], ObservingSubscription[A, MonoObserver] {

		private var state: Trial[A] = initialState
		private var downChainObserverSlot: MonoObserver[A] | Null = null

		override def trial: Trial[A] = state
		override def maybeValue: Maybe[A] = state.toMaybe
		override def isCompleted: Boolean = state.isDefined

		override def subscribe(monoObserver: MonoObserver[A]): Subscription = {
			state.fold {
				if downChainObserverSlot eq null then {
					downChainObserverSlot = monoObserver
					addTarget(this)
					this
				} else {
					val nest = new ObservingSubscription[A, MonoObserver] {
						override def target: MonoObserver[A] = monoObserver

						override def unsubscribe(): Unit = removeAllMatching(this)
					}
					addTarget(nest)
					nest
				}
			} { ex =>
				monoObserver.onError(ex)
				Subscription_empty
			} { a =>
				monoObserver.onSuccess(a)
				Subscription_empty
			}
		}

		override def unsubscribe(): Unit = {
			val obs = downChainObserverSlot
			if obs != null then {
				downChainObserverSlot = null
				removeAllMatching(this)
			}
		}

		override def target: MonoObserver[A] = {
			val obs = downChainObserverSlot
			if obs eq null then throw new IllegalStateException("No observer registered")
			obs
		}

		override protected def clear(): Unit = {
			super.clear()
			downChainObserverSlot = null
		}

		override def map[B](f: A => B): Capturer[B] = {
			state.fold {
				new Captor_Map(this, f, guarded = false)
			}(new Failed(_)) { a => new Keeper(f(a)) }
		}

		override def flatMap[B](f: A => Observable[B]): Observable[B] = {
			state.fold {
				new Captor_FlatMap(this, f, guarded = false)
			}(new Failed(_)) { a => f(a) }
		}

		@targetName("flatMapCapturer")
		override def flatMap[B](f: A => Capturer[B]): Capturer[B] = {
			state.fold {
				new Captor_FlatMapCapturer(this, f, guarded = false)
			}(new Failed(_)) { a => f(a) }
		}

		override def mapGuarded[B](f: A => B): Capturer[B] = {
			state.fold {
				new Captor_Map(this, f, guarded = true)
			}(new Failed(_)) { a =>
				try new Keeper(f(a)) catch {
					case NonFatal(e) => new Failed(e)
				}
			}
		}

		override def flatMapGuarded[B](f: A => Observable[B]): Observable[B] = {
			state.fold {
				new Captor_FlatMap(this, f, guarded = true)
			}(new Failed(_)) { a =>
				try f(a) catch {
					case NonFatal(e) => new Failed(e)
				}
			}
		}

		@targetName("flatMapCapturerGuarded")
		override def flatMapGuarded[B](f: A => Capturer[B]): Capturer[B] = {
			state.fold {
				new Captor_FlatMapCapturer(this, f, guarded = true)
			}(new Failed(_)) { a =>
				try f(a) catch {
					case NonFatal(e) => new Failed(e)
				}
			}
		}

		protected def forwardSuccess(value: A): Unit = {
			state = Trial.success(value)
			foreachTarget(_.onSuccess(value))
			clear()
		}

		protected def forwardError(ex: Throwable): Unit = {
			state = Trial.failure(ex)
			foreachTarget(_.onError(ex))
			clear()
		}
	}

	final class Captor[A](initialState: Trial[A] = Trial.empty) extends AbstractCaptor[A](initialState) {
		def capture(result: A): Unit = if trial.isEmpty then forwardSuccess(result)

		def fail(ex: Throwable): Unit = if trial.isEmpty then forwardError(ex)
	}

	trait SpareSlotCaptorOp[A, B] extends AbstractCaptor[B] with MonoObserver[A] {
		protected val source: Capturer[A]
		private var upChainSubscriptionSlot: Subscription | Null = null

		protected def startEagerly(): Unit = {
			upChainSubscriptionSlot = source.subscribe(this)
		}

		override def unsubscribe(): Unit = {
			val upSub = upChainSubscriptionSlot
			if upSub != null then {
				upChainSubscriptionSlot = null
				upSub.unsubscribe()
			}
			super.unsubscribe()
		}

		override protected def clear(): Unit = {
			super.clear()
			upChainSubscriptionSlot = null
		}
	}

	////////////////////////////////////////////
	//// Capturer operations helper classes ////
	////////////////////////////////////////////

	final class Captor_Map[A, B](val source: Capturer[A], val f: A => B, guarded: Boolean) extends SpareSlotCaptorOp[A, B] {
		startEagerly()

		override def onSuccess(a: A): Unit = {
			if guarded then {
				val maybeB = try Maybe(f(a)) catch {
					case NonFatal(e) =>
						forwardError(e)
						Maybe.empty
				}
				maybeB.foreach(forwardSuccess)
			} else {
				forwardSuccess(f(a))
			}
		}

		override def onError(ex: Throwable): Unit = forwardError(ex)
	}

	final class Captor_FlatMap[A, B](val source: Capturer[A], val f: A => Observable[B], guarded: Boolean) extends Observable[B] with MonoObserver[A] with Subscription { thisCaptor =>
		private var state: Trial[B] = Trial.empty
		private var innerSubscription: Subscription | Null = null
		private var downChainObserverSlot: MonoObserver[B] | Null = null
		private var upstreamSubscription: Subscription | Null = source.subscribe(this)

		override def subscribe(monoObserver: MonoObserver[B]): Subscription = {
			state.fold {
				if downChainObserverSlot eq null then {
					downChainObserverSlot = monoObserver
					this
				} else {
					new Subscription {
						override def unsubscribe(): Unit = {
							if downChainObserverSlot eq monoObserver then downChainObserverSlot = null
						}
					}
				}
			} { ex =>
				monoObserver.onError(ex)
				Subscription_empty
			} { b =>
				monoObserver.onSuccess(b)
				Subscription_empty
			}
		}

		override def map[C](g: B => C): Observable[C] = {
			val task: Task[C] = (monoObserverC: MonoObserver[C]) => {
				this.subscribe(new MonoObserver[B] {
					override def onSuccess(b: B): Unit = monoObserverC.onSuccess(g(b))

					override def onError(ex: Throwable): Unit = monoObserverC.onError(ex)
				})
			}
			task
		}

		override def flatMap[C](g: B => Observable[C]): Observable[C] = {
			val task: Task[C] = (monoObserverC: MonoObserver[C]) => {
				this.subscribe(new MonoObserver[B] {
					override def onSuccess(b: B): Unit = g(b).subscribe(monoObserverC)

					override def onError(ex: Throwable): Unit = monoObserverC.onError(ex)
				})
			}
			task
		}

		override def onSuccess(a: A): Unit = {
			if guarded then {
				val maybeOb = try Maybe(f(a)) catch {
					case NonFatal(e) =>
						onError(e)
						Maybe.empty
				}
				maybeOb.foreach(subscribeInner)
			} else {
				subscribeInner(f(a))
			}
		}

		private def subscribeInner(ob: Observable[B]): Unit = {
			innerSubscription = ob.subscribe(new MonoObserver[B] {
				override def onSuccess(b: B): Unit = {
					state = Trial.success(b)
					val obs = downChainObserverSlot
					downChainObserverSlot = null
					upstreamSubscription = null
					innerSubscription = null
					if obs != null then obs.onSuccess(b)
				}

				override def onError(ex: Throwable): Unit = thisCaptor.onError(ex)
			})
		}

		override def onError(ex: Throwable): Unit = {
			state = Trial.failure(ex)
			val obs = downChainObserverSlot
			downChainObserverSlot = null
			upstreamSubscription = null
			innerSubscription = null
			if obs != null then obs.onError(ex)
		}

		override def unsubscribe(): Unit = {
			val up = upstreamSubscription
			val inner = innerSubscription
			downChainObserverSlot = null
			upstreamSubscription = null
			innerSubscription = null
			if up != null then up.unsubscribe()
			if inner != null then inner.unsubscribe()
		}
	}

	final class Captor_FlatMapCapturer[A, B](val source: Capturer[A], val f: A => Capturer[B], guarded: Boolean) extends SpareSlotCaptorOp[A, B] {
		private var innerSubscription: Subscription | Null = null

		startEagerly()

		override def onSuccess(a: A): Unit = {
			if guarded then {
				val maybeCap = try Maybe(f(a)) catch {
					case NonFatal(e) =>
						forwardError(e)
						Maybe.empty
				}
				maybeCap.foreach(subscribeInner)
			} else {
				subscribeInner(f(a))
			}
		}

		private def subscribeInner(cap: Capturer[B]): Unit = {
			innerSubscription = cap.subscribe(new MonoObserver[B] {
				override def onSuccess(b: B): Unit = forwardSuccess(b)

				override def onError(ex: Throwable): Unit = forwardError(ex)
			})
		}

		override def onError(ex: Throwable): Unit = forwardError(ex)

		override def unsubscribe(): Unit = {
			super.unsubscribe()
			val inner = innerSubscription
			innerSubscription = null
			if inner != null then inner.unsubscribe()
		}

		override protected def clear(): Unit = {
			super.clear()
			innerSubscription = null
		}
	}

	///////////////////////////////////////////
	//// Targets registry and broadcasting ////
	///////////////////////////////////////////


	trait TargetProxy[-A, +T[-_] <: AnyRef] {
		def target: T[A]
	}

	/** Convenient conjunction of [[TargetProxy]] and [[Subscription]] due to its ubiquity. */
	trait ObservingSubscription[-A, +T[-_] <: AnyRef] extends Subscription, TargetProxy[A, T]

	/** A registry of targets capable of applying any operation to all of them. */
	trait Muxer[A, T[-_] <: AnyRef] {
		type Target = T[A] | TargetProxy[A, T]

		private var maybeFirstTarget: Maybe[Target] = Maybe.empty
		private var maybeFollowingTargets: Maybe[Array[Target]] = Maybe.empty
		private var followingTargetsSize: Int = 0

		protected def addTarget(target: Target): Unit = {
			maybeFirstTarget.fold {
				maybeFirstTarget = Maybe(target)
			} { _ =>
				maybeFollowingTargets.fold {
					val followingTargets = new Array[Target](8)
					followingTargets(0) = target
					followingTargetsSize = 1
					maybeFollowingTargets = Maybe(followingTargets)
				} { followingTargets =>
					val fs = followingTargetsSize
					val newFollowingTargets =
						if fs < followingTargets.length then followingTargets
						else {
							val expanded = new Array[Target](fs * 2)
							System.arraycopy(followingTargets, 0, expanded, 0, fs)
							maybeFollowingTargets = Maybe(expanded)
							expanded
						}
					newFollowingTargets(fs) = target
					followingTargetsSize = fs + 1
				}
			}
		}

		protected def removeAllMatching(target: Target): Int = {
			var removedCount = 0
			maybeFirstTarget.foreach { firstOb =>
				maybeFollowingTargets.foreach { followingTargets =>
					var index = followingTargetsSize
					while index > 0 do {
						index -= 1
						if followingTargets(index) eq target then {
							val shiftedChunkLength = followingTargetsSize - index - 1
							if shiftedChunkLength > 0 then System.arraycopy(followingTargets, index + 1, followingTargets, index, shiftedChunkLength)
							removedCount += 1
							followingTargetsSize -= 1
							followingTargets(followingTargetsSize) = null.asInstanceOf[Target] // Clear leaked reference
						}
					}
				}
				if firstOb eq target then {
					maybeFirstTarget = maybeFollowingTargets.flatMap { followingTargets =>
						if followingTargetsSize == 0 then Maybe.empty
						else {
							val firstFollowingTarget = followingTargets(0)
							// Shift following targets left by 1
							followingTargetsSize -= 1
							System.arraycopy(followingTargets, 1, followingTargets, 0, followingTargetsSize)
							followingTargets(followingTargetsSize) = null.asInstanceOf[Target] // Clear leaked reference
							Maybe(firstFollowingTarget)
						}
					}
					removedCount += 1
				}
			}
			removedCount
		}

		protected def countAllMatching(target: Target): Int = {
			var counter = 0
			maybeFirstTarget.foreach { firstOb =>
				if firstOb eq target then counter += 1
				maybeFollowingTargets.fold(false) { followingTargets =>
					var index = followingTargetsSize
					while index > 0 do {
						index -= 1
						if followingTargets(index) eq target then counter += 1
					}
				}
			}
			counter
		}

		protected inline def foreachTarget(inline consumer: T[A] => Unit): Unit = {
			maybeFirstTarget.foreach { firstTarget =>
				firstTarget match {
					case proxy: TargetProxy[A, ?] @unchecked => consumer(proxy.target.asInstanceOf[T[A]])
					case direct: T[A] @unchecked => consumer(direct)
				}
				// CRITICAL: The cast is necessary to bypass an invalid Scala 3 compiler optimization during the inline expansion. Because Entry is a Union Type, its runtime allocation is a raw JVM Object array (Object[]). However, if an Observer implementation happens to extend a trait like java.io.Serializable, the Scala 3 compiler will try to optimize this inline closure by implicitly downcasting the entire array container to a Serializable[] array. Since an Object[] cannot be downcast to a Serializable[], the JVM explodes with a ClassCastException. Forcing an AnyRef array view strips away this aggressive optimization and keeps it as a safe, generic pointer array.
				maybeFollowingTargets.asInstanceOf[Maybe[IArray[AnyRef]]].foreach { followingTargets =>
					var i = 0
					val size = followingTargetsSize
					while i < size do {
						followingTargets(i) match {
							case proxy: TargetProxy[A, ?] @unchecked => consumer(proxy.target.asInstanceOf[T[A]])
							case direct: T[A] @unchecked => consumer(direct)
						}
						i += 1
					}
				}
			}
		}

		protected def clear(): Unit = {
			maybeFirstTarget = Maybe.empty
			maybeFollowingTargets = Maybe.empty
			followingTargetsSize = 0
		}
	}

	// ===================================
	// ==== STREAM SUPPORT PRIMITIVES ====
	// ===================================

	////////////////////////////
	//// Flux (push based) /////
	////////////////////////////

	trait FluxObserver[-A] {
		def onNext(a: A, index: Int): Unit

		def onError(ex: Throwable): Unit

		def onComplete(): Unit
	}

	trait Flux[+A] { thisFlux =>
		def subscribe(observer: FluxObserver[A]): Subscription

		inline def subscribeCallbacks(inline next: (A, Int) => Unit, inline error: Throwable => Unit = _ => (), inline complete: () => Unit = () => ()): Subscription = {
			subscribe(
				new FluxObserver[A] {
					override def onNext(value: A, index: Int): Unit = next(value, index)

					override def onError(ex: Throwable): Unit = error(ex)

					override def onComplete(): Unit = complete()
				}
			)
		}

		inline def foreach(inline consumer: A => Unit): Unit = {
			subscribeCallbacks(next = (a, _) => consumer(a)); ()
		}

		inline def foreachWithCoords(inline consumer: (A, Int) => Unit): Unit = {
			subscribeCallbacks(consumer); ()
		}

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
			var active = true
			var upstreamSubscription: Subscription | Null = null
			upstreamSubscription = thisFlux.subscribe(new FluxObserver[A] {
				private var state: B = initial

				override def onNext(a: A, index: Int): Unit = {
					if active then {
						val maybeB = try f(state, a, index) catch {
							case NonFatal(ex) =>
								active = false
								monoObserverB.onError(ex)
								if upstreamSubscription ne null then upstreamSubscription.unsubscribe()
								Maybe.empty
						}
						if active then {
							maybeB.fold {
								active = false
								monoObserverB.onSuccess(state)
								if upstreamSubscription ne null then upstreamSubscription.unsubscribe()
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
			new Subscription {
				override def unsubscribe(): Unit = {
					active = false
					if upstreamSubscription ne null then upstreamSubscription.unsubscribe()
				}
			}
		}
	}


	object Flux {
		def empty[A]: Flux[A] = new DefaultFlux[A] {
			override def subscribe(observer: FluxObserver[A]): Subscription = {
				observer.onComplete()
				Subscription_empty
			}
		}

		def apply[A](elements: A*): Flux[A] = fromIterable(elements)

		def fromIterable[A](iterable: Iterable[A]): Flux[A] = new DefaultFlux[A] {
			override def subscribe(observer: FluxObserver[A]): Subscription = {
				val it = iterable.iterator
				var index = 0
				while it.hasNext do {
					val v = it.next()
					observer.onNext(v, index)
					index += 1
				}
				observer.onComplete()
				Subscription_empty
			}
		}

		def fromIterableGuarded[A](iterable: Iterable[A]): Flux[A] = new DefaultFlux[A] {
			override def subscribe(observer: FluxObserver[A]): Subscription = {
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
				Subscription_empty
			}
		}

		def generate[A](supplier: Int => A): Flux[A] = new DefaultFlux[A] {
			override def subscribe(observer: FluxObserver[A]): Subscription = {
				var index = 0
				var active = true
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
				Subscription_empty
			}
		}

		def generateStatefully[A](supplierBuilder: () => Int => A): Flux[A] = new DefaultFlux[A] {
			override def subscribe(observer: FluxObserver[A]): Subscription = {
				val supplier = supplierBuilder()
				var index = 0
				var active = true
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
				Subscription_empty
			}
		}

		def fromObservablesArray[A](array: IArray[Observable[A]]): Flux[A] = new DefaultFlux[A] {
			override def subscribe(fluxObserver: FluxObserver[A]): Subscription = {
				val subs = new Array[Subscription](array.length)
				class AllElemsObserver extends MonoObserver[A] {
					private var sequenceIndex = 0
					var active = true

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
				array.foreachWithIndex { (observable, index) =>
					subs(index) = observable.subscribe(allElemsObserver)
				}
				new Subscription {
					override def unsubscribe(): Unit = {
						allElemsObserver.active = false
						var i = 0
						while i < subs.length do {
							val s = subs(i)
							if s != null then s.unsubscribe()
							i += 1
						}
					}
				}
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

		override def take(n: Int): Flux[A] = new Flux_Take(this, n)

		override def takeWhile(p: (a: A, index: Int, count: Int) => Boolean): Flux[A] = new Flux_TakeWhile(this, p)

		override def zip[B, C: ClassTag](other: Flux[B])(f: (A, B, Int) => C): Flux[C] = new Flux_Zip(this, other, f)
	}


	trait SpareSlotFluxOp[A, +B] extends DefaultFlux[B] with FluxObserver[A] with Subscription {
		protected val source: Flux[A]

		private var downChainObserverSlot: FluxObserver[B] @uncheckedVariance = uninitialized
		private var upChainSubscriptionSlot: Subscription | Null = null

		override def subscribe(downChainObserver: FluxObserver[B]): Subscription = {
			if downChainObserverSlot eq null then {
				downChainObserverSlot = downChainObserver
				upChainSubscriptionSlot = source.subscribe(this)
				this
			} else {
				val delegate = createDelegate(downChainObserver)
				source.subscribe(delegate)
			}
		}

		override def unsubscribe(): Unit = {
			val upSub = upChainSubscriptionSlot
			downChainObserverSlot = null
			upChainSubscriptionSlot = null
			resetState()
			if upSub != null then upSub.unsubscribe()
		}

		protected def createDelegate(downChainObserver: FluxObserver[B]): FluxObserver[A]

		protected def resetState(): Unit

		protected def forwardNext(value: B @uncheckedVariance, index: Int): Unit = {
			val obs = downChainObserverSlot
			if obs != null then obs.onNext(value, index)
		}

		protected def forwardError(ex: Throwable): Unit = {
			val obs = downChainObserverSlot
			downChainObserverSlot = null
			upChainSubscriptionSlot = null
			resetState()
			if obs != null then obs.onError(ex)
		}

		protected def forwardComplete(): Unit = {
			val obs = downChainObserverSlot
			downChainObserverSlot = null
			upChainSubscriptionSlot = null
			resetState()
			if obs != null then obs.onComplete()
		}
	}

	/////////////////////////
	//// Concrete Fluxes ////
	/////////////////////////

	final class StreamEmitter[A] extends Muxer[A, FluxObserver], DefaultFlux[A] {
		private var elemsCounter = 0
		private var completed = false
		private var error: Throwable | Null = null

		override def subscribe(obs: FluxObserver[A]): Subscription = {
			if error ne null then {
				obs.onError(error.asInstanceOf[Throwable])
				Subscription_empty
			} else if completed then {
				obs.onComplete()
				Subscription_empty
			} else {
				val nest = new ObservingSubscription[A, FluxObserver] {
					override def target: FluxObserver[A] = obs

					override def unsubscribe(): Unit = removeAllMatching(this)
				}
				addTarget(nest)
				nest
			}
		}

		def emit(value: A): Unit = {
			if !completed && (error eq null) then {
				val idx = elemsCounter
				elemsCounter += 1
				foreachTarget(_.onNext(value, idx))
			}
		}

		def fail(ex: Throwable): Unit = {
			if !completed && (error eq null) then {
				error = ex
				foreachTarget(_.onError(ex))
				clear()
			}
		}

		def end(): Unit = {
			if !completed && (error eq null) then {
				completed = true
				foreachTarget(_.onComplete())
				clear()
			}
		}
	}

	/////////////////////////////////////////////////////
	//// Concrete fluxes returned by Flux operations ////
	/////////////////////////////////////////////////////

	final class Flux_Map[A, B](val source: Flux[A], val f: A => B) extends SpareSlotFluxOp[A, B] {
		override def onNext(a: A, index: Int): Unit = forwardNext(f(a), index)

		override def onError(ex: Throwable): Unit = forwardError(ex)

		override def onComplete(): Unit = forwardComplete()

		override protected def resetState(): Unit = ()

		override protected def createDelegate(observer: FluxObserver[B]): FluxObserver[A] = new FluxObserver[A] {
			override def onNext(a: A, index: Int): Unit = observer.onNext(f(a), index)

			override def onError(ex: Throwable): Unit = observer.onError(ex)

			override def onComplete(): Unit = observer.onComplete()
		}
	}

	final class Flux_MapWithIndex[A, B](val source: Flux[A], val f: (A, Int) => B) extends SpareSlotFluxOp[A, B] {
		override def onNext(a: A, index: Int): Unit = forwardNext(f(a, index), index)

		override def onError(ex: Throwable): Unit = forwardError(ex)

		override def onComplete(): Unit = forwardComplete()

		override protected def resetState(): Unit = ()

		override protected def createDelegate(observer: FluxObserver[B]): FluxObserver[A] = new FluxObserver[A] {
			override def onNext(a: A, index: Int): Unit = observer.onNext(f(a, index), index)

			override def onError(ex: Throwable): Unit = observer.onError(ex)

			override def onComplete(): Unit = observer.onComplete()
		}
	}

	final class Flux_Scan[A, B](val source: Flux[A], val initial: B, val f: (B, A, Int) => B) extends SpareSlotFluxOp[A, B] {

		private var state = initial

		override protected def resetState(): Unit = state = initial

		override def onNext(a: A, index: Int): Unit = {
			state = f(state, a, index)
			forwardNext(state, index)
		}

		override def onError(ex: Throwable): Unit = forwardError(ex)

		override def onComplete(): Unit = forwardComplete()

		override protected def createDelegate(observer: FluxObserver[B]): FluxObserver[A] = new FluxObserver[A] {
			private var state = initial

			override def onNext(a: A, index: Int): Unit = {
				state = f(state, a, index)
				observer.onNext(state, index)
			}

			override def onError(ex: Throwable): Unit = observer.onError(ex)

			override def onComplete(): Unit = observer.onComplete()
		}
	}

	final class Flux_Buffer[A, T >: A : ClassTag](val source: Flux[A], val size: Int) extends SpareSlotFluxOp[A, IArray[T]] {

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

		override protected def createDelegate(observer: FluxObserver[IArray[T]]): FluxObserver[A] = new FluxObserver[A] {
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
	}

	final class Flux_Take[A](val source: Flux[A], val n: Int) extends SpareSlotFluxOp[A, A] {
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

		override protected def createDelegate(observer: FluxObserver[A]): FluxObserver[A] = new FluxObserver[A] {
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
	}

	final class Flux_TakeWhile[A](val source: Flux[A], val p: (a: A, index: Int, count: Int) => Boolean, flattenToCount: Boolean = true) extends SpareSlotFluxOp[A, A] {
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

		override protected def createDelegate(observer: FluxObserver[A]): FluxObserver[A] = new FluxObserver[A] {
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
	}

	private final class Flux_Zip[A, B, C](val left: Flux[A], val right: Flux[B], val f: (A, B, Int) => C) extends DefaultFlux[C] {
		override def subscribe(observer: FluxObserver[C]): Subscription = {
			new ZipObservation(observer).start()
		}

		private final class ZipObservation(observer: FluxObserver[C]) {
			private val leftValues = scala.collection.mutable.Map[Int, A]()
			private val rightValues = scala.collection.mutable.Map[Int, B]()
			private var leftCompleted = false
			private var rightCompleted = false
			private var errorFired = false
			private var leftSub: Subscription | Null = null
			private var rightSub: Subscription | Null = null

			def start(): Subscription = {
				leftSub = left.subscribe(new LeftObserver)
				rightSub = right.subscribe(new RightObserver)
				new Subscription {
					override def unsubscribe(): Unit = {
						val l = leftSub
						val r = rightSub
						leftSub = null
						rightSub = null
						if l != null then l.unsubscribe()
						if r != null then r.unsubscribe()
					}
				}
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
	}

	/////////////////////////////////////////////////////
	//// Classes for operations that return a Tensor ////
	/////////////////////////////////////////////////////


	trait InnerSubscriptionsTracker {
		private var innerSubscriptions = new Array[Subscription | Null](8)
		private var outerFluxElemCount = 0

		protected def nextSeq(): Int = {
			val seq = outerFluxElemCount
			outerFluxElemCount += 1
			seq
		}

		protected def storeInnerSubscription(index: Int, sub: Subscription): Unit = {
			if index >= innerSubscriptions.length then {
				val newArr = new Array[Subscription | Null](innerSubscriptions.length * 2)
				System.arraycopy(innerSubscriptions, 0, newArr, 0, innerSubscriptions.length)
				innerSubscriptions = newArr
			}
			innerSubscriptions(index) = sub
		}

		protected def clearSubscription(index: Int): Unit = {
			val subs = innerSubscriptions
			if index < subs.length then {
				subs(index) = null
			}
		}

		protected def unsubscribeAndClear(): Unit = {
			val subs = innerSubscriptions
			innerSubscriptions = new Array[Subscription | Null](8)
			outerFluxElemCount = 0
			var i = 0
			val len = subs.length
			while i < len do {
				val sub = subs(i)
				if sub != null then sub.unsubscribe()
				i += 1
			}
		}
	}

	final class Flux_FlatMap[A, B](val outerFlux: Flux[A], val f: A => Flux[B]) extends DefaultTensor[B] with FluxObserver[A] with Subscription with InnerSubscriptionsTracker {

		private var downChainObserverSlot: TensorObserver[B] | Null = null
		private var upChainSubscriptionSlot: Subscription | Null = null
		private var outerFluxCompleted = false
		private var activeInnerFluxesCount = 0

		private def resetState(): Unit = {
			downChainObserverSlot = null
			upChainSubscriptionSlot = null
			unsubscribeAndClear()
			outerFluxCompleted = false
			activeInnerFluxesCount = 0
		}

		override def subscribe(tensorObserver: TensorObserver[B]): Subscription = {
			if downChainObserverSlot ne null then {
				val observer = new FlatMapObserver(tensorObserver)
				observer.start(outerFlux)
			} else {
				downChainObserverSlot = tensorObserver
				upChainSubscriptionSlot = outerFlux.subscribe(this)
				this
			}
		}

		override def unsubscribe(): Unit = {
			val upSub = upChainSubscriptionSlot
			resetState()
			if upSub != null then upSub.unsubscribe()
		}

		override def onNext(a: A, outer: Int): Unit = {
			val localSeq = nextSeq()
			activeInnerFluxesCount += 1
			val innerSubscription = f(a).subscribe(new InnerObserver(outer, localSeq))
			if downChainObserverSlot ne null then {
				storeInnerSubscription(localSeq, innerSubscription)
			} else {
				innerSubscription.unsubscribe()
			}
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

		private final class InnerObserver(outer: Int, localSeq: Int) extends FluxObserver[B] {
			override def onNext(b: B, inner: Int): Unit = {
				val obs = downChainObserverSlot
				if obs != null then obs.onNext(b, inner, outer)
			}

			override def onError(ex: Throwable): Unit = Flux_FlatMap.this.onError(ex)

			override def onComplete(): Unit = {
				activeInnerFluxesCount -= 1
				clearSubscription(localSeq)
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

		private final class FlatMapObserver(tensorObserver: TensorObserver[B]) extends FluxObserver[A] with Subscription with InnerSubscriptionsTracker {
			private var outerFluxCompleted = false
			private var activeInnerFluxesCount = 0
			private var allCompleted = false
			private var upChainSubscription: Subscription | Null = null

			def start(outerFlux: Flux[A]): Subscription = {
				upChainSubscription = outerFlux.subscribe(this)
				this
			}

			override def unsubscribe(): Unit = {
				allCompleted = true
				val upSub = upChainSubscription
				upChainSubscription = null
				if upSub != null then upSub.unsubscribe()
				unsubscribeAndClear()
			}

			override def onNext(a: A, outerIndex: Int): Unit = {
				val localSeq = nextSeq()
				activeInnerFluxesCount += 1
				val sub = f(a).subscribe(new FluxObserver[B] {
					override def onNext(b: B, innerIndex: Int): Unit = if !allCompleted then tensorObserver.onNext(b, innerIndex, outerIndex)

					override def onError(ex: Throwable): Unit = FlatMapObserver.this.onError(ex)

					override def onComplete(): Unit = {
						activeInnerFluxesCount -= 1
						clearSubscription(localSeq)
						tensorObserver.onInnerComplete(outerIndex)
						tryComplete()
					}
				})
				if !allCompleted then {
					storeInnerSubscription(localSeq, sub)
				} else {
					sub.unsubscribe()
				}
			}

			override def onError(ex: Throwable): Unit = {
				if !allCompleted then {
					allCompleted = true
					upChainSubscription = null
					unsubscribeAndClear()
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
					upChainSubscription = null
					tensorObserver.onComplete()
				}
			}
		}
	}

	final class Flux_FlatMapWithIndex[A, B](val outerFlux: Flux[A], val f: (A, Int) => Flux[B]) extends DefaultTensor[B] with FluxObserver[A] with Subscription with InnerSubscriptionsTracker {

		private var downChainObserverSlot: TensorObserver[B] | Null = null
		private var upChainSubscriptionSlot: Subscription | Null = null
		private var outerFluxCompleted = false
		private var activeInnerFluxesCount = 0

		private def resetState(): Unit = {
			downChainObserverSlot = null
			upChainSubscriptionSlot = null
			unsubscribeAndClear()
			outerFluxCompleted = false
			activeInnerFluxesCount = 0
		}

		override def subscribe(observer: TensorObserver[B]): Subscription = {
			if downChainObserverSlot ne null then {
				val flatMapObserver = new FlatMapWithIndexObserver(observer)
				flatMapObserver.start(outerFlux)
			} else {
				downChainObserverSlot = observer
				upChainSubscriptionSlot = outerFlux.subscribe(this)
				this
			}
		}

		override def unsubscribe(): Unit = {
			val upSub = upChainSubscriptionSlot
			resetState()
			if upSub != null then upSub.unsubscribe()
		}

		override def onNext(a: A, outer: Int): Unit = {
			val localSeq = nextSeq()
			activeInnerFluxesCount += 1
			val sub = f(a, outer).subscribe(new InnerObserver(outer, localSeq))
			if downChainObserverSlot ne null then {
				storeInnerSubscription(localSeq, sub)
			} else {
				sub.unsubscribe()
			}
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

		private final class InnerObserver(outer: Int, localSeq: Int) extends FluxObserver[B] {
			override def onNext(b: B, inner: Int): Unit = {
				val obs = downChainObserverSlot
				if obs != null then obs.onNext(b, inner, outer)
			}

			override def onError(ex: Throwable): Unit = Flux_FlatMapWithIndex.this.onError(ex)

			override def onComplete(): Unit = {
				activeInnerFluxesCount -= 1
				clearSubscription(localSeq)
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

		private final class FlatMapWithIndexObserver(tensorObserver: TensorObserver[B]) extends FluxObserver[A] with Subscription with InnerSubscriptionsTracker {
			private var outerFluxCompleted = false
			private var activeInnerFluxesCount = 0
			private var allCompleted = false
			private var upstreamSubscription: Subscription | Null = null

			def start(outerFlux: Flux[A]): Subscription = {
				upstreamSubscription = outerFlux.subscribe(this)
				this
			}

			override def unsubscribe(): Unit = {
				allCompleted = true
				val upSub = upstreamSubscription
				upstreamSubscription = null
				if upSub != null then upSub.unsubscribe()
				unsubscribeAndClear()
			}

			override def onNext(a: A, outerIndex: Int): Unit = {
				val localSeq = nextSeq()
				activeInnerFluxesCount += 1
				val sub = f(a, outerIndex).subscribe(new FluxObserver[B] {
					override def onNext(b: B, innerIndex: Int): Unit = if !allCompleted then tensorObserver.onNext(b, innerIndex, outerIndex)

					override def onError(ex: Throwable): Unit = FlatMapWithIndexObserver.this.onError(ex)

					override def onComplete(): Unit = {
						activeInnerFluxesCount -= 1
						clearSubscription(localSeq)
						tensorObserver.onInnerComplete(outerIndex)
						tryComplete()
					}
				})
				if !allCompleted then {
					storeInnerSubscription(localSeq, sub)
				} else {
					sub.unsubscribe()
				}
			}

			override def onError(ex: Throwable): Unit = {
				if !allCompleted then {
					allCompleted = true
					upstreamSubscription = null
					unsubscribeAndClear()
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
					upstreamSubscription = null
					tensorObserver.onComplete()
				}
			}
		}
	}

	////////////////////////////////////////////////////////////////////////////////////////////////
	/// UpChain Observers produced by operations that return a Tensor when the slot is occupied ///
	////////////////////////////////////////////////////////////////////////////////////////////////

	trait TensorObserver[-A] {
		def onNext(a: A, innerIndex: Int, outerIndex: Int): Unit

		def onOuterComplete(): Unit

		def onInnerComplete(outerIndex: Int): Unit

		def onError(ex: Throwable): Unit

		def onComplete(): Unit
	}

	trait Tensor[+A] {
		def subscribe(observer: TensorObserver[A]): Subscription

		inline def subscribeCallbacks(inline next: (a: A, inner: Int, outer: Int) => Unit, inline error: Throwable => Unit = _ => (), inline outerComplete: () => Unit = () => (), inline innerComplete: Int => Unit = _ => (), inline complete: () => Unit = () => ()): Subscription = {
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

	trait SpareSlotTensorOp[A, +B] extends DefaultFlux[B] with TensorObserver[A] with Subscription {
		protected val source: Tensor[A]

		private var downChainObserverSlot: FluxObserver[B] @uncheckedVariance | Null = null
		private var upstreamSubscription: Subscription | Null = null

		protected def resetState(): Unit

		override def subscribe(observer: FluxObserver[B]): Subscription = {
			if downChainObserverSlot ne null then source.subscribe(createDelegate(observer))
			else {
				downChainObserverSlot = observer
				upstreamSubscription = source.subscribe(this)
				this
			}
		}

		override def unsubscribe(): Unit = {
			val upSub = upstreamSubscription
			downChainObserverSlot = null
			upstreamSubscription = null
			resetState()
			if upSub != null then upSub.unsubscribe()
		}

		protected def createDelegate(observer: FluxObserver[B]): TensorObserver[A]

		protected def forwardNext(value: B @uncheckedVariance, index: Int): Unit = {
			val obs = downChainObserverSlot
			if obs != null then obs.onNext(value, index)
		}

		protected def forwardError(ex: Throwable): Unit = {
			val obs = downChainObserverSlot
			downChainObserverSlot = null
			upstreamSubscription = null
			resetState()
			if obs != null then obs.onError(ex)
		}

		protected def forwardComplete(): Unit = {
			val obs = downChainObserverSlot
			downChainObserverSlot = null
			upstreamSubscription = null
			resetState()
			if obs != null then obs.onComplete()
		}
	}

	///////////////////////////////////////
	//// Classes for Tensor operations ////
	///////////////////////////////////////	

	final class Tensor_FlattenInner[A](override protected val source: Tensor[A]) extends SpareSlotTensorOp[A, A] {
		override def resetState(): Unit = ()

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

	final class Tensor_FlattenOuter[A](override protected val source: Tensor[A]) extends SpareSlotTensorOp[A, A] {
		override def resetState(): Unit = ()

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

	final class Tensor_FlattenSequential[A](override protected val source: Tensor[A]) extends SpareSlotTensorOp[A, A] {
		private var counter = 0

		override protected def resetState(): Unit = counter = 0

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

	final class Tensor_FlattenWith[A](override protected val source: Tensor[A], val f: (A, Int, Int, Int) => Int) extends SpareSlotTensorOp[A, A] { // TODO add sequential index to the signature
		private var counter = 0

		override protected def resetState(): Unit = counter = 0

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

	final class Tensor_FlattenStatefully[A, B](source: Tensor[A], flattenerBuilder: () => TensorFlattener[A, B]) extends DefaultFlux[B] with TensorObserver[A] with Subscription {
		private var downChainObserverSlot: FluxObserver[B] | Null = null
		private var flattenerSlot: TensorFlattener[A, B] | Null = null
		private var upChainSubscriptionSlot: Subscription | Null = null

		override def subscribe(observer: FluxObserver[B]): Subscription = {
			val flattener = flattenerBuilder()
			if downChainObserverSlot ne null then source.subscribe(createDelegate(observer, flattener))
			else {
				downChainObserverSlot = observer
				flattenerSlot = flattener
				upChainSubscriptionSlot = source.subscribe(this)
				this
			}
		}

		override def unsubscribe(): Unit = {
			val upSub = upChainSubscriptionSlot
			downChainObserverSlot = null
			flattenerSlot = null
			upChainSubscriptionSlot = null
			if upSub != null then upSub.unsubscribe()
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
			val flat = flattenerSlot
			if dco != null && flat != null then flat.onNext(dco)(a, innerIndex, outerIndex)
		}

		override def onOuterComplete(): Unit = {
			val dco = downChainObserverSlot
			val flat = flattenerSlot
			if dco != null && flat != null then flat.onOuterComplete(dco)
		}

		override def onInnerComplete(outerIndex: Int): Unit = {
			val dco = downChainObserverSlot
			val flat = flattenerSlot
			if dco != null && flat != null then flat.onInnerComplete(dco)(outerIndex)
		}

		override def onError(ex: Throwable): Unit = {
			val dco = downChainObserverSlot
			val flat = flattenerSlot
			downChainObserverSlot = null
			flattenerSlot = null
			upChainSubscriptionSlot = null
			if dco != null && flat != null then flat.onError(dco)(ex)
		}

		override def onComplete(): Unit = {
			val dco = downChainObserverSlot
			val flat = flattenerSlot
			downChainObserverSlot = null
			flattenerSlot = null
			upChainSubscriptionSlot = null
			if dco != null && flat != null then flat.onComplete(dco)
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
