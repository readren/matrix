package readren.sequencer

import readren.common.Maybe
import readren.common.{foreachWithIndex, mapWithIndex}

import scala.annotation.threadUnsafe
import scala.reflect.ClassTag
import scala.util.control.NonFatal


trait FluxExtension { thisDoer: Doer =>

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

	@threadUnsafe lazy val FluxObserver_ignore: FluxObserver[Any] = new FluxObserver[Any] {
		override def onNext(a: Any, index: Int): Unit = ()

		override def onError(ex: Throwable): Unit = ()

		override def onComplete(): Unit = ()
	}

	trait Flux[+A] { thisFlux =>
		def subscribeSync(observer: FluxObserver[A]): Subscription

		inline def subscribeSyncCallbacks(inline next: (A, Int) => Unit, inline error: Throwable => Unit = _ => (), inline complete: () => Unit = () => ()): Subscription = {
			class LocalObserver extends FluxObserver[A] {
				override def onNext(value: A, index: Int): Unit = next(value, index)

				override def onError(ex: Throwable): Unit = error(ex)

				override def onComplete(): Unit = complete()
			}
			subscribeSync(new LocalObserver)
		}

		final inline def subscribe(inline isWithinDoSerEx: Boolean = isInSequence)(observer: FluxObserver[A]): Subscription = {
			if isWithinDoSerEx then subscribeSync(observer)
			else {
				class LocalSubscription extends Subscription {
					private var isActive = true
					private var maybeTargetSubscription: Maybe[Subscription] = Maybe.empty

					{
						thisDoer.run {
							if isActive then {
								val targetSubscription = subscribeSync(observer)
								if isActive then maybeTargetSubscription = Maybe(targetSubscription)
								else targetSubscription.unsubscribeSync()
							}
						}
					}

					override def unsubscribeSync(): Unit = {
						isActive = false
						val mts = maybeTargetSubscription
						maybeTargetSubscription = Maybe.empty
						mts.foreach(_.unsubscribeSync())
					}
				}
				new LocalSubscription
			}
		}

		inline def subscribeAndForget(inline isWithinDoSerEx: Boolean = isInSequence): Subscription = subscribe(isWithinDoSerEx)(FluxObserver_ignore)

		/** Like [[subscribeSync]] but does not return a [[Subscription]].
		 * The default implementation calls [[subscribeSync]], but some subclasses have a more efficient implementation. */
		def triggerSync(observer: FluxObserver[A]): Unit = subscribeSync(observer)

		final inline def trigger(inline isWithinDoSerEx: Boolean = isInSequence)(observer: FluxObserver[A]): Unit = {
			if isWithinDoSerEx then triggerSync(observer)
			else thisDoer.run(triggerSync(observer))
		}

		inline final def triggerAndForget(inline isWithinDoSerEx: Boolean = isInSequence): Unit = {
			if isWithinDoSerEx then triggerSync(FluxObserver_ignore)
			else thisDoer.run(triggerSync(FluxObserver_ignore))
		}

		inline def foreach(inline consumer: A => Unit): Unit = {
			checkWithin()
			class ForeachObserver extends FluxObserver[A] {
				override def onNext(value: A, index: Int): Unit = {
					consumer(value)
				}

				override def onError(ex: Throwable): Unit = ()

				override def onComplete(): Unit = ()
			}
			triggerSync(new ForeachObserver)
		}

		inline def foreachWithCoords(inline consumer: (A, Int) => Unit): Unit = {
			checkWithin()
			class ForeachWIObserver extends FluxObserver[A] {
				override def onNext(value: A, index: Int): Unit = {
					consumer(value, index)
				}

				override def onError(ex: Throwable): Unit = ()

				override def onComplete(): Unit = ()
			}

			triggerSync(new ForeachWIObserver)
		}

		def andThen(observer: FluxObserver[A]): Flux[A]

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
		def foldWhile[B](initial: B, isGuarded: Boolean)(f: (B, A, Int) => Maybe[B]): Task[B] = new Task[B] {
			override def subscribeSync(downChainObserver: MonoObserver[B]): Subscription = {
				new Subscription with FluxObserver[A] {
					private var state: B = initial
					private var isActive = true
					private var maybeUpChainSubscription: Maybe[Subscription] = Maybe.empty
					{ // Constructor
						val upChainSubscription = thisFlux.subscribeSync(this)
						if isActive then maybeUpChainSubscription = Maybe(upChainSubscription)
					}

					override def onNext(a: A, index: Int): Unit = {
						if isActive then {
							val maybeB =
								if isGuarded then try f(state, a, index) catch {
									case NonFatal(ex) =>
										unsubscribeSync()
										downChainObserver.onError(ex)
										Maybe.empty
								} else f(state, a, index)
							if isActive then {
								maybeB.fold {
									unsubscribeSync()
									downChainObserver.onSuccess(state)
								} { b => state = b }
							}
						}
					}

					override def onError(ex: Throwable): Unit = {
						isActive = false
						maybeUpChainSubscription = Maybe.empty
						downChainObserver.onError(ex)
					}

					override def onComplete(): Unit = {
						isActive = false
						maybeUpChainSubscription = Maybe.empty
						downChainObserver.onSuccess(state)
					}

					override def unsubscribeSync(): Unit = {
						isActive = false
						val mus = maybeUpChainSubscription
						maybeUpChainSubscription = Maybe.empty
						mus.foreach(_.unsubscribeSync())
					}
				}
			}
		}
	}

	//////////////////////////////
	//// Flux factory methods ////
	//////////////////////////////

	@threadUnsafe lazy val Flux_empty: Flux[Nothing] = new DefaultFlux[Nothing] {
		override def subscribeSync(downChainObserver: FluxObserver[Nothing]): Subscription = {
			downChainObserver.onComplete()
			Subscription_empty
		}
	}

	def Flux_apply[A](elements: A*): Flux[A] = Flux_fromIterable(elements)

	def Flux_fromIterable[A](iterable: Iterable[A]): Flux[A] = new DefaultFlux[A] {
		override def subscribeSync(downChainObserver: FluxObserver[A]): Subscription = {
			val it = iterable.iterator
			var index = 0
			while it.hasNext do {
				val v = it.next()
				downChainObserver.onNext(v, index)
				index += 1
			}
			downChainObserver.onComplete()
			Subscription_empty
		}
	}

	def Flux_fromIterableGuarded[A](iterable: Iterable[A]): Flux[A] = new DefaultFlux[A] {
		override def subscribeSync(downChainObserver: FluxObserver[A]): Subscription = {
			val it = iterable.iterator
			var index = 0
			var isActive = true
			while isActive do {
				val hasNext = try it.hasNext catch {
					case NonFatal(e) =>
						downChainObserver.onError(e)
						isActive = false
						false
				}
				if hasNext then {
					try {
						val v = it.next()
						downChainObserver.onNext(v, index)
						index += 1
					} catch {
						case NonFatal(e) =>
							downChainObserver.onError(e)
							isActive = false
					}
				} else if isActive then {
					downChainObserver.onComplete()
					isActive = false
				}
			}
			Subscription_empty
		}
	}

	def Flux_generate[A](supplier: Int => Maybe[A]): Flux[A] = new DefaultFlux[A] {
		override def subscribeSync(downChainObserver: FluxObserver[A]): Subscription = {
			var index = 0
			var isActive = true
			while isActive do {
				val maybeVal = try supplier(index) catch {
					case NonFatal(e) =>
						isActive = false
						downChainObserver.onError(e)
						Maybe.empty
				}
				if isActive then maybeVal.fold {
					isActive = false
					downChainObserver.onComplete()
				} { a =>
					downChainObserver.onNext(a, index)
					index += 1
				}
			}
			Subscription_empty
		}
	}

	def Flux_generateStatefully[A](supplierBuilder: () => Int => Maybe[A]): Flux[A] = new DefaultFlux[A] {
		override def subscribeSync(downChainObserver: FluxObserver[A]): Subscription = {
			val supplier = supplierBuilder()
			var index = 0
			var isActive = true
			while isActive do {
				val maybeA = try supplier(index) catch {
					case NonFatal(e) =>
						isActive = false
						downChainObserver.onError(e)
						Maybe.empty
				}
				if isActive then maybeA.fold {
					isActive = false
					downChainObserver.onComplete()
				} { v =>
					downChainObserver.onNext(v, index)
					index += 1
				}
			}
			Subscription_empty
		}
	}

	def Flux_fromMonosSequentially[A](monos: IArray[Mono[A]]): Flux[A] = {
		if monos.length == 0 then Flux_empty
		else new DefaultFlux[A] {
			override def subscribeSync(downChainObserver: FluxObserver[A]): Subscription = {
				new Subscription with MonoObserver[A] {
					private var sequenceIndex = 0
					private var isActive = true
					private var maybeMonoSubscriptions: Maybe[IArray[Subscription]] = Maybe.empty

					{ // Constructor
						val monoSubscriptions = monos.mapWithIndex { (mono, _) =>
							if isActive then mono.subscribeSync(this)
							else Subscription_empty
						}
						if isActive then maybeMonoSubscriptions = Maybe(monoSubscriptions)
						else unsubscribeSync()
					}

					override def onSuccess(value: A): Unit = {
						if isActive then {
							val si = sequenceIndex
							sequenceIndex = si + 1
							downChainObserver.onNext(value, si)
							if sequenceIndex == monos.length then {
								isActive = false
								maybeMonoSubscriptions = Maybe.empty
								downChainObserver.onComplete()
							}
						}
					}

					override def onError(ex: Throwable): Unit = {
						if isActive then {
							unsubscribeSync()
							downChainObserver.onError(ex)
						}
					}

					override def unsubscribeSync(): Unit = {
						isActive = false
						val mms = maybeMonoSubscriptions
						maybeMonoSubscriptions = Maybe.empty
						mms.foreach(_.foreachWithIndex { (subscription, _) => subscription.unsubscribeSync() })
					}
				}
			}
		}
	}

	def Flux_fromMonos[A](monos: IArray[Mono[A]]): Flux[A] = {
		if monos.length == 0 then Flux_empty
		else new DefaultFlux[A] {
			override def subscribeSync(observer: FluxObserver[A]): Subscription = new Subscription {
				private var successesCounter = 0
				private var isActive = true
				private var maybeMonoSubscriptions: Maybe[IArray[Subscription]] = Maybe.empty

				{
					val monoSubscriptions: IArray[Subscription] = monos.mapWithIndex { (mono, index) =>
						if isActive then mono.subscribeSyncCallbacks( // TODO this allocation could be avoided if the MonoObserver propagated the subscription id/index.
							a => if isActive then {
								val sc = successesCounter + 1
								successesCounter = sc
								observer.onNext(a, index)
								if isActive && sc == monos.length then {
									isActive = false
									maybeMonoSubscriptions = Maybe.empty
									observer.onComplete()
								}
							},
							e => if isActive then {
								unsubscribeSync()
								observer.onError(e)
							}
						) else Subscription_empty
					}

					if isActive then maybeMonoSubscriptions = Maybe(monoSubscriptions)
					else unsubscribeMonos(monoSubscriptions)
				}

				override def unsubscribeSync(): Unit = {
					isActive = false
					val mms = maybeMonoSubscriptions
					maybeMonoSubscriptions = Maybe.empty
					mms.foreach(unsubscribeMonos)
				}

				private def unsubscribeMonos(subscriptions: IArray[Subscription]): Unit = subscriptions.foreachWithIndex { (subscription, _) => subscription.unsubscribeSync() }
			}
		}
	}

	/////////////////////////////////////
	//// Specialized abstract Fluxes ////
	/////////////////////////////////////

	/** Partial implementation of [[Flux]] */
	trait DefaultFlux[+A] extends Flux[A] {
		override def andThen(observer: FluxObserver[A]): Flux[A] = {
			triggerSync(observer)
			this
		}

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


	/////////////////////////
	//// Concrete Fluxes ////
	/////////////////////////

	final class StreamEmitter[A] extends Muxer[A, FluxObserver], DefaultFlux[A] {
		private var elemsCounter = 0
		private var completed = false
		private var error: Throwable | Null = null

		override def subscribeSync(obs: FluxObserver[A]): Subscription = {
			if error ne null then {
				obs.onError(error.asInstanceOf[Throwable])
				Subscription_empty
			} else if completed then {
				obs.onComplete()
				Subscription_empty
			} else {
				val nest = new ObservingSubscription[A, FluxObserver] {
					override def target: FluxObserver[A] = obs

					override def unsubscribeSync(): Unit = removeAllMatching(this)
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
				clearRegistry()
			}
		}

		def end(): Unit = {
			if !completed && (error eq null) then {
				completed = true
				foreachTarget(_.onComplete())
				clearRegistry()
			}
		}
	}

	/////////////////////////////////////////////////////
	//// Concrete fluxes returned by Flux operations ////
	/////////////////////////////////////////////////////

	final class Flux_Map[A, B](upChainFlux: Flux[A], f: A => B) extends DefaultFlux[B] {
		override def subscribeSync(downChainObserver: FluxObserver[B]): Subscription = {
			new FluxObserver[A] with Subscription {
				private var upChainSubscription: Subscription | Null = upChainFlux.subscribeSync(this)

				override def onNext(a: A, index: Int): Unit = downChainObserver.onNext(f(a), index)

				override def onError(ex: Throwable): Unit = {
					upChainSubscription = null
					downChainObserver.onError(ex)
				}

				override def onComplete(): Unit = {
					upChainSubscription = null
					downChainObserver.onComplete()
				}

				override def unsubscribeSync(): Unit = {
					val sub = upChainSubscription
					upChainSubscription = null
					if sub != null then sub.unsubscribeSync()
				}
			}
		}
	}

	final class Flux_MapWithIndex[A, B](upChainFlux: Flux[A], f: (A, Int) => B) extends DefaultFlux[B] {
		override def subscribeSync(downChainObserver: FluxObserver[B]): Subscription = {
			new FluxObserver[A] with Subscription {
				private var upChainSubscription: Subscription | Null = upChainFlux.subscribeSync(this)

				override def onNext(a: A, index: Int): Unit = downChainObserver.onNext(f(a, index), index)

				override def onError(ex: Throwable): Unit = {
					upChainSubscription = null
					downChainObserver.onError(ex)
				}

				override def onComplete(): Unit = {
					upChainSubscription = null
					downChainObserver.onComplete()
				}

				override def unsubscribeSync(): Unit = {
					val sub = upChainSubscription
					upChainSubscription = null
					if sub != null then sub.unsubscribeSync()
				}
			}
		}
	}

	final class Flux_Scan[A, B](upChainFlux: Flux[A], initial: B, f: (B, A, Int) => B) extends DefaultFlux[B] {
		override def subscribeSync(downChainObserver: FluxObserver[B]): Subscription = {
			new FluxObserver[A] with Subscription {
				private var state = initial
				private var upChainSubscription: Subscription | Null = upChainFlux.subscribeSync(this)

				override def onNext(a: A, index: Int): Unit = {
					state = f(state, a, index)
					downChainObserver.onNext(state, index)
				}

				override def onError(ex: Throwable): Unit = {
					upChainSubscription = null
					downChainObserver.onError(ex)
				}

				override def onComplete(): Unit = {
					upChainSubscription = null
					downChainObserver.onComplete()
				}

				override def unsubscribeSync(): Unit = {
					val sub = upChainSubscription
					upChainSubscription = null
					if sub != null then sub.unsubscribeSync()
				}
			}
		}
	}

	final class Flux_Buffer[A, T >: A : ClassTag](upChainFlux: Flux[A], size: Int) extends DefaultFlux[IArray[T]] {
		override def subscribeSync(downChainObserver: FluxObserver[IArray[T]]): Subscription = {
			new FluxObserver[A] with Subscription {
				private var buffer = new Array[T](size)
				private var count = 0
				private var chunkIndex = 0
				private var active = true
				private var upChainSubscription: Subscription | Null = null
				{
					upChainSubscription = upChainFlux.subscribeSync(this)
				}

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
							downChainObserver.onNext(chunk, idx)
						}
					}
				}

				override def onError(ex: Throwable): Unit = {
					if active then {
						active = false
						downChainObserver.onError(ex)
					}
				}

				override def onComplete(): Unit = {
					if active then {
						active = false
						if count > 0 then {
							val partial = IArray.unsafeFromArray(buffer.take(count))
							downChainObserver.onNext(partial, chunkIndex)
						}
						downChainObserver.onComplete()
					}
				}

				override def unsubscribeSync(): Unit = {
					active = false
					val sub = upChainSubscription
					upChainSubscription = null
					if sub != null then sub.unsubscribeSync()
				}
			}
		}
	}

	final class Flux_Take[A](upChainFlux: Flux[A], n: Int) extends DefaultFlux[A] {
		override def subscribeSync(downChainObserver: FluxObserver[A]): Subscription = {
			new FluxObserver[A] with Subscription {
				private var count = 0
				private var active = true
				private var upChainSubscription: Subscription | Null = null
				{
					upChainSubscription = upChainFlux.subscribeSync(this)
				}

				override def onNext(a: A, index: Int): Unit = {
					if active then {
						if count < n then {
							count += 1
							downChainObserver.onNext(a, index)
							if count == n then {
								active = false
								downChainObserver.onComplete()
							}
						}
					}
				}

				override def onError(ex: Throwable): Unit = {
					if active then {
						active = false
						downChainObserver.onError(ex)
					}
				}

				override def onComplete(): Unit = {
					if active then {
						active = false
						downChainObserver.onComplete()
					}
				}

				override def unsubscribeSync(): Unit = {
					active = false
					val sub = upChainSubscription
					upChainSubscription = null
					if sub != null then sub.unsubscribeSync()
				}
			}
		}
	}

	final class Flux_TakeWhile[A](upChainFlux: Flux[A], p: (a: A, index: Int, count: Int) => Boolean, flattenToCount: Boolean = true) extends DefaultFlux[A] {
		override def subscribeSync(downChainObserver: FluxObserver[A]): Subscription = {
			new FluxObserver[A] with Subscription {
				private var active = true
				private var counter = 0
				private var upChainSubscription: Subscription | Null = null
				{
					upChainSubscription = upChainFlux.subscribeSync(this)
				}

				override def onNext(a: A, index: Int): Unit = {
					if active then {
						if p(a, index, counter) then {
							val currentCounter = counter
							counter += 1
							downChainObserver.onNext(a, if flattenToCount then currentCounter else index)
						} else {
							active = false
							downChainObserver.onComplete()
						}
					}
				}

				override def onError(ex: Throwable): Unit = {
					if active then {
						active = false
						downChainObserver.onError(ex)
					}
				}

				override def onComplete(): Unit = {
					if active then {
						active = false
						downChainObserver.onComplete()
					}
				}

				override def unsubscribeSync(): Unit = {
					active = false
					val sub = upChainSubscription
					upChainSubscription = null
					if sub != null then sub.unsubscribeSync()
				}
			}
		}
	}

	private final class Flux_Zip[A, B, C](upChainLeftFlux: Flux[A], upChainRightFlux: Flux[B], val f: (A, B, Int) => C) extends DefaultFlux[C] {
		override def subscribeSync(downChainObserver: FluxObserver[C]): Subscription = {
			new Subscription with FluxObserver[A] {
				private val leftValues = scala.collection.mutable.Map[Int, A]()
				private val rightValues = scala.collection.mutable.Map[Int, B]()
				private var leftCompleted = false
				private var rightCompleted = false
				private var errorFired = false
				private var upChainSubscriptionLeft: Subscription | Null = null
				private var upChainSubscriptionRight: Subscription | Null = null

				{ // Constructor
					upChainSubscriptionLeft = upChainLeftFlux.subscribeSync(this)
					upChainSubscriptionRight = upChainRightFlux.subscribeSync(new FluxObserver[B] {
						override def onNext(b: B, rightIndex: Int): Unit = {
							leftValues.remove(rightIndex) match {
								case Some(a) =>
									downChainObserver.onNext(f(a, b, rightIndex), rightIndex)
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
					})
				}

				override def onNext(a: A, leftIndex: Int): Unit = {
					rightValues.remove(leftIndex) match {
						case Some(b) =>
							downChainObserver.onNext(f(a, b, leftIndex), leftIndex)
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

				private def checkComplete(): Unit = {
					if (leftCompleted && leftValues.isEmpty) || (rightCompleted && rightValues.isEmpty) || (leftCompleted && rightCompleted) then downChainObserver.onComplete()
				}

				private def fireError(ex: Throwable): Unit = {
					if !errorFired then {
						errorFired = true
						downChainObserver.onError(ex)
					}
				}

				override def unsubscribeSync(): Unit = {
					val l = upChainSubscriptionLeft
					val r = upChainSubscriptionRight
					upChainSubscriptionLeft = null
					upChainSubscriptionRight = null
					if l != null then l.unsubscribeSync()
					if r != null then r.unsubscribeSync()
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
			val is = innerSubscriptions
			innerSubscriptions = new Array[Subscription | Null](8)
			outerFluxElemCount = 0
			var i = 0
			val len = is.length
			while i < len do {
				val sub = is(i)
				if sub != null then sub.unsubscribeSync()
				i += 1
			}
		}
	}

	final class Flux_FlatMap[A, B](upChainFlux: Flux[A], f: A => Flux[B]) extends DefaultTensor[B] {
		override def subscribe(downChainObserver: TensorObserver[B]): Subscription = {
			new FluxObserver[A] with Subscription with InnerSubscriptionsTracker { selfObserver =>
				private var outerFluxCompleted = false
				private var activeInnerFluxesCount = 0
				private var allCompleted = false
				private var upChainSubscription: Subscription | Null = null

				{ // Constructor
					upChainSubscription = upChainFlux.subscribeSync(this)
				}

				override def unsubscribeSync(): Unit = {
					allCompleted = true
					val ucs = upChainSubscription
					upChainSubscription = null
					if ucs != null then ucs.unsubscribeSync()
					unsubscribeAndClear()
				}

				override def onNext(a: A, outerIndex: Int): Unit = {
					val localSeq = nextSeq()
					activeInnerFluxesCount += 1
					val sub = f(a).subscribeSync(new FluxObserver[B] {
						override def onNext(b: B, innerIndex: Int): Unit = if !allCompleted then downChainObserver.onNext(b, innerIndex, outerIndex)

						override def onError(ex: Throwable): Unit = selfObserver.onError(ex)

						override def onComplete(): Unit = {
							activeInnerFluxesCount -= 1
							clearSubscription(localSeq)
							downChainObserver.onInnerComplete(outerIndex)
							tryComplete()
						}
					})
					if allCompleted then {
						sub.unsubscribeSync()
					} else {
						storeInnerSubscription(localSeq, sub)
					}
				}

				override def onError(ex: Throwable): Unit = {
					if !allCompleted then {
						allCompleted = true
						upChainSubscription = null
						unsubscribeAndClear()
						downChainObserver.onError(ex)
					}
				}

				override def onComplete(): Unit = {
					outerFluxCompleted = true
					downChainObserver.onOuterComplete()
					tryComplete()
				}

				inline def tryComplete(): Unit = {
					if outerFluxCompleted && activeInnerFluxesCount == 0 && !allCompleted then {
						allCompleted = true
						upChainSubscription = null
						downChainObserver.onComplete()
					}
				}
			}
		}
	}

	final class Flux_FlatMapWithIndex[A, B](upChainFlux: Flux[A], f: (A, Int) => Flux[B]) extends DefaultTensor[B] {
		override def subscribe(downChainObserver: TensorObserver[B]): Subscription = {
			new FluxObserver[A] with Subscription with InnerSubscriptionsTracker { selfObserver =>
				private var outerFluxCompleted = false
				private var activeInnerFluxesCount = 0
				private var allCompleted = false
				private var upChainSubscription: Subscription | Null = null

				{ // Constructor
					upChainSubscription = upChainFlux.subscribeSync(this)
				}

				override def unsubscribeSync(): Unit = {
					allCompleted = true
					val upSub = upChainSubscription
					upChainSubscription = null
					if upSub != null then upSub.unsubscribeSync()
					unsubscribeAndClear()
				}

				override def onNext(a: A, outerIndex: Int): Unit = {
					val localSeq = nextSeq()
					activeInnerFluxesCount += 1
					val sub = f(a, outerIndex).subscribeSync(new FluxObserver[B] {
						override def onNext(b: B, innerIndex: Int): Unit = if !allCompleted then downChainObserver.onNext(b, innerIndex, outerIndex)

						override def onError(ex: Throwable): Unit = selfObserver.onError(ex)

						override def onComplete(): Unit = {
							activeInnerFluxesCount -= 1
							clearSubscription(localSeq)
							downChainObserver.onInnerComplete(outerIndex)
							tryComplete()
						}
					})
					if allCompleted then {
						sub.unsubscribeSync()
					} else {
						storeInnerSubscription(localSeq, sub)
					}
				}

				override def onError(ex: Throwable): Unit = {
					if !allCompleted then {
						allCompleted = true
						upChainSubscription = null
						unsubscribeAndClear()
						downChainObserver.onError(ex)
					}
				}

				override def onComplete(): Unit = {
					outerFluxCompleted = true
					downChainObserver.onOuterComplete()
					tryComplete()
				}

				inline def tryComplete(): Unit = {
					if outerFluxCompleted && activeInnerFluxesCount == 0 && !allCompleted then {
						allCompleted = true
						upChainSubscription = null
						downChainObserver.onComplete()
					}
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
			class LocalObserver extends TensorObserver[A] {
				override def onNext(value: A, inner: Int, outer: Int): Unit = next(value, inner, outer)

				override def onOuterComplete(): Unit = outerComplete()

				override def onInnerComplete(outerIndex: Int): Unit = innerComplete(outerIndex)

				override def onError(ex: Throwable): Unit = error(ex)

				override def onComplete(): Unit = complete()
			}
			subscribe(new LocalObserver)
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

	///////////////////////////////////////
	//// Classes for Tensor operations ////
	///////////////////////////////////////

	final class Tensor_FlattenInner[A](val upChainTensor: Tensor[A]) extends DefaultFlux[A] {
		override def subscribeSync(downChainObserver: FluxObserver[A]): Subscription = {
			new TensorObserver[A] with Subscription {
				private var upChainSubscription: Subscription | Null = upChainTensor.subscribe(this)

				override def onNext(a: A, inner: Int, outer: Int): Unit = downChainObserver.onNext(a, inner)

				override def onOuterComplete(): Unit = ()

				override def onInnerComplete(outerIndex: Int): Unit = ()

				override def onError(ex: Throwable): Unit = {
					upChainSubscription = null
					downChainObserver.onError(ex)
				}

				override def onComplete(): Unit = {
					upChainSubscription = null
					downChainObserver.onComplete()
				}

				override def unsubscribeSync(): Unit = {
					val sub = upChainSubscription
					upChainSubscription = null
					if sub != null then sub.unsubscribeSync()
				}
			}
		}
	}

	final class Tensor_FlattenOuter[A](upChainTensor: Tensor[A]) extends DefaultFlux[A] {
		override def subscribeSync(downChainObserver: FluxObserver[A]): Subscription = {
			new TensorObserver[A] with Subscription {
				private var upChainSubscription: Subscription | Null = upChainTensor.subscribe(this)

				override def onNext(a: A, inner: Int, outer: Int): Unit = downChainObserver.onNext(a, outer)

				override def onOuterComplete(): Unit = ()

				override def onInnerComplete(outerIndex: Int): Unit = ()

				override def onError(ex: Throwable): Unit = {
					upChainSubscription = null
					downChainObserver.onError(ex)
				}

				override def onComplete(): Unit = {
					upChainSubscription = null
					downChainObserver.onComplete()
				}

				override def unsubscribeSync(): Unit = {
					val sub = upChainSubscription
					upChainSubscription = null
					if sub != null then sub.unsubscribeSync()
				}
			}
		}
	}

	final class Tensor_FlattenSequential[A](upChainTensor: Tensor[A]) extends DefaultFlux[A] {
		override def subscribeSync(downChainObserver: FluxObserver[A]): Subscription = {
			new TensorObserver[A] with Subscription {
				private var counter = 0
				private var upChainSubscription: Subscription | Null = upChainTensor.subscribe(this)

				override def onNext(a: A, inner: Int, outer: Int): Unit = {
					val index = counter
					counter += 1
					downChainObserver.onNext(a, index)
				}

				override def onOuterComplete(): Unit = ()

				override def onInnerComplete(outerIndex: Int): Unit = ()

				override def onError(ex: Throwable): Unit = {
					upChainSubscription = null
					downChainObserver.onError(ex)
				}

				override def onComplete(): Unit = {
					upChainSubscription = null
					downChainObserver.onComplete()
				}

				override def unsubscribeSync(): Unit = {
					val sub = upChainSubscription
					upChainSubscription = null
					if sub != null then sub.unsubscribeSync()
				}
			}
		}
	}

	final class Tensor_FlattenWith[A](upChainTensor: Tensor[A], f: (A, Int, Int, Int) => Int) extends DefaultFlux[A] {
		override def subscribeSync(downChainObserver: FluxObserver[A]): Subscription = {
			new TensorObserver[A] with Subscription {
				private var counter = 0
				private var upChainSubscription: Subscription | Null = upChainTensor.subscribe(this)

				override def onNext(a: A, inner: Int, outer: Int): Unit = {
					val count = counter
					counter += 1
					downChainObserver.onNext(a, f(a, inner, outer, count))
				}

				override def onOuterComplete(): Unit = ()

				override def onInnerComplete(outerIndex: Int): Unit = ()

				override def onError(ex: Throwable): Unit = {
					upChainSubscription = null
					downChainObserver.onError(ex)
				}

				override def onComplete(): Unit = {
					upChainSubscription = null
					downChainObserver.onComplete()
				}

				override def unsubscribeSync(): Unit = {
					val sub = upChainSubscription
					upChainSubscription = null
					if sub != null then sub.unsubscribeSync()
				}
			}
		}
	}

	final class Tensor_FlattenStatefully[A, B](upChainTensor: Tensor[A], flattenerBuilder: () => TensorFlattener[A, B]) extends DefaultFlux[B] {
		override def subscribeSync(downChainObserver: FluxObserver[B]): Subscription = {
			new TensorObserver[A] with Subscription {
				private val flattener = flattenerBuilder()
				private var upChainSubscription: Subscription | Null = upChainTensor.subscribe(this)

				override def onNext(a: A, innerIndex: Int, outerIndex: Int): Unit = flattener.onNext(downChainObserver)(a, innerIndex, outerIndex)

				override def onOuterComplete(): Unit = flattener.onOuterComplete(downChainObserver)

				override def onInnerComplete(outerIndex: Int): Unit = flattener.onInnerComplete(downChainObserver)(outerIndex)

				override def onError(ex: Throwable): Unit = {
					upChainSubscription = null
					flattener.onError(downChainObserver)(ex)
				}

				override def onComplete(): Unit = {
					upChainSubscription = null
					flattener.onComplete(downChainObserver)
				}

				override def unsubscribeSync(): Unit = {
					val sub = upChainSubscription
					upChainSubscription = null
					if sub != null then sub.unsubscribeSync()
				}
			}
		}
	}
}
