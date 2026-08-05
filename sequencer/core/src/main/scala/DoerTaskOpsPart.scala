package readren.sequencer

import readren.common.*

import scala.annotation.unchecked.uncheckedVariance
import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

trait DoerTaskOpsPart { thisDoer: Doer & DoerCorePart =>

	//// Concrete implementations of [[Task]] returned by instance methods ////

	/** $suppressSyntheticCompanionObject */
	private inline def Task_AndThen(trap: Nothing): Any = trap

	final class Task_AndThen[+A](upChainMono: Mono[A], monoObserver: MonoObserver[A]) extends AbstractTask[A] {
		override def subscribeSync(downChainObserver: MonoObserver[A]): Subscription = {
			upChainMono.subscribeSync(new MonoObserver[A] {
				override def onSuccess(a: A): Unit = {
					monoObserver.onSuccess(a)
					downChainObserver.onSuccess(a)
				}

				override def onError(ex: Throwable): Unit = {
					monoObserver.onError(ex)
					downChainObserver.onError(ex)
				}
			})
		}

		override def toString: String = deriveToString[Task_AndThen[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_Reconcile(trap: Nothing): Any = trap

	final class Task_Reconcile[A](upChainMono: Mono[A]) extends AbstractTask[Try[A]] {
		override def subscribeSync(downChainObserver: MonoObserver[Try[A]]): Subscription = {
			upChainMono.subscribeSync(new MonoObserver[A] {
				override def onSuccess(a: A): Unit = downChainObserver.onSuccess(Success(a))

				override def onError(e: Throwable): Unit = downChainObserver.onSuccess(Failure(e))
			})
		}

		override def toString: String = deriveToString[Task_Reconcile[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_WithFilter(trap: Nothing): Any = trap

	final class Task_WithFilter[+A](upChainMono: Mono[A], p: A => Boolean, isGuarded: Boolean) extends AbstractTask[A] {
		override def subscribeSync(downChainObserver: MonoObserver[A]): Subscription = {
			upChainMono.subscribeSync(new MonoObserver[A] {
				override def onSuccess(a: A): Unit = {
					val pass =
						if isGuarded then {
							try (if p(a) then 1 else 0) catch {
								case NonFatal(e) =>
									downChainObserver.onError(e)
									-1
							}
						} else if p(a) then 1 else 0
					if pass == 1 then downChainObserver.onSuccess(a)
					else if pass == 0 then downChainObserver.onError(new NoSuchElementException)
				}

				override def onError(e: Throwable): Unit = downChainObserver.onError(e)
			})
		}

		override def toString: String = deriveToString[Task_WithFilter[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_Map(trap: Nothing): Any = trap

	final class Task_Map[+A, +B](upChainMono: Mono[A], f: A => B, isGuarded: Boolean) extends AbstractTask[B] {
		override def subscribeSync(downChainObserver: MonoObserver[B]): Subscription = {
			// Propagates the subscription upstream while mapping success values
			upChainMono.subscribeSync(new MonoObserver[A] {
				override def onSuccess(a: A): Unit = {
					if isGuarded then {
						val maybeB = try Maybe(f(a)) catch {
							case NonFatal(e) =>
								downChainObserver.onError(e)
								Maybe.empty
						}
						maybeB.foreach(downChainObserver.onSuccess)
					}
					else downChainObserver.onSuccess(f(a))
				}

				override def onError(ex: Throwable): Unit = downChainObserver.onError(ex)
			})
		}

		override def toString: String = deriveToString[Task_Map[A, B]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_Recover(trap: Nothing): Any = trap

	final class Task_Recover[-A, +B >: A](upChainMono: Mono[A], pf: Throwable => Maybe[B], isGuarded: Boolean) extends AbstractTask[B] {
		override def subscribeSync(downChainObserver: MonoObserver[B]): Subscription = {
			upChainMono.subscribeSync(new MonoObserver[A] {
				override def onSuccess(a: A): Unit = downChainObserver.onSuccess(a)

				override def onError(e1: Throwable): Unit = {
					if isGuarded then {
						var isActive = true
						val maybeB = try pf(e1) catch {
							case NonFatal(e2) =>
								isActive = false
								downChainObserver.onError(e2)
								Maybe.empty
						}
						if isActive then maybeB.fold(downChainObserver.onError(e1))(downChainObserver.onSuccess)
					} else {
						pf(e1).fold(downChainObserver.onError(e1))(downChainObserver.onSuccess)
					}
				}
			})
		}

		override def toString: String = deriveToString[Task_Recover[A, B]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_Transform(trap: Nothing): Any = trap

	final class Task_Transform[-A, B](upChainMono: Task[A], f: Try[A] => Try[B], isGuarded: Boolean) extends AbstractTask[B] {

		override def subscribeSync(downChainObserver: MonoObserver[B]): Subscription = {
			upChainMono.subscribeSync(new MonoObserver[A] {
				override def onSuccess(a: A): Unit = handle(Success(a))

				override def onError(e: Throwable): Unit = handle(Failure(e))

				private def handle(tryA: Try[A]): Unit = {
					val tryB = if isGuarded then try f(tryA) catch {
						case NonFatal(e) => Failure(e)
					} else f(tryA)
					tryB match {
						case Success(b) => downChainObserver.onSuccess(b)
						case Failure(ex) => downChainObserver.onError(ex)
					}
				}
			})
		}

		override def toString: String = deriveToString[Task_Transform[A, B]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_FlatMap(trap: Nothing): Any = trap

	/** TODO This class is very similar to [[Captor_FlatMap]]. Consider removing duplication by extending a common super class. */
	final class Task_FlatMap[+A, +B](upChainMono: Mono[A], f: A => Mono[B], isGuarded: Boolean) extends AbstractTask[B] {
		override def subscribeSync(downChainObserver: MonoObserver[B]): Subscription = {
			new Subscription with MonoObserver[A] {
				private var isActive = true
				private var maybeInnerSubscription: Maybe[Subscription] = Maybe.empty
				private var maybeUpChainSubscription: Maybe[Subscription] = Maybe.empty
				{
					val upChainSubscription = upChainMono.subscribeSync(this)
					if isActive then maybeUpChainSubscription = Maybe(upChainSubscription)
				}

				override def onSuccess(a: A): Unit = {
					if isActive then {
						isActive = false
						maybeUpChainSubscription = Maybe.empty
						val maybeInnerMonoB =
							if isGuarded then {
								try Maybe(f(a)) catch {
									case NonFatal(e) =>
										downChainObserver.onError(e)
										Maybe.empty
								}
							} else Maybe(f(a))
						maybeInnerMonoB.foreach { innerMonoB =>
							val innerSubscription = innerMonoB.subscribeSync(downChainObserver)
							if isActive then maybeInnerSubscription = Maybe(innerSubscription)
						}
					}
				}

				override def onError(e: Throwable): Unit = {
					if isActive then {
						isActive = false
						maybeUpChainSubscription = Maybe.empty
						downChainObserver.onError(e)
					}
				}

				override def unsubscribeSync(): Unit = {
					checkWithin()
					isActive = false
					val mus = maybeUpChainSubscription
					val mis = maybeInnerSubscription
					maybeUpChainSubscription = Maybe.empty
					maybeInnerSubscription = Maybe.empty
					mus.foreach(_.unsubscribeSync())
					mis.foreach(_.unsubscribeSync())
				}
			}
		}

		override def toString: String = deriveToString[Task_FlatMap[A, B]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_RecoverWith(trap: Nothing): Any = trap

	final class Task_RecoverWith[-A, +B >: A](upChainMono: Mono[A], pf: Throwable => Maybe[Mono[B]], isGuarded: Boolean) extends AbstractTask[B] {
		override def subscribeSync(downChainObserver: MonoObserver[B]): Subscription = {
			new Subscription with MonoObserver[A] {
				private var isActive = true
				private var maybeInnerSubscription: Maybe[Subscription] = Maybe.empty
				private var maybeUpChainSubscription: Maybe[Subscription] = Maybe.empty
				{ // Constructor
					val upChainSubscription = upChainMono.subscribeSync(this)
					if isActive then maybeUpChainSubscription = Maybe(upChainSubscription)
				}

				override def onSuccess(a: A): Unit = {
					if isActive then {
						isActive = false
						maybeUpChainSubscription = Maybe.empty
						downChainObserver.onSuccess(a)
					}
				}

				override def onError(e1: Throwable): Unit = {
					if isActive then {
						maybeUpChainSubscription = Maybe.empty
						val maybeMonoB =
							if isGuarded then {
								try pf(e1) catch {
									case NonFatal(e2) =>
										isActive = false
										downChainObserver.onError(e2)
										Maybe.empty
								}
							} else pf(e1)

						if isActive then maybeMonoB.fold(downChainObserver.onError(e1)) { monoB =>
							val innerSubscription = monoB.subscribeSync(downChainObserver)
							if isActive then maybeInnerSubscription = Maybe(innerSubscription)
						}
					}
				}

				override def unsubscribeSync(): Unit = {
					isActive = false
					val up = maybeUpChainSubscription
					val inner = maybeInnerSubscription
					maybeUpChainSubscription = Maybe.empty
					maybeInnerSubscription = Maybe.empty
					up.foreach(_.unsubscribeSync())
					inner.foreach(_.unsubscribeSync())
				}
			}
		}

		override def toString: String = deriveToString[Task_RecoverWith[A, B]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_TransformWith(trap: Nothing): Any = trap

	final class Task_TransformWith[+A, +B](upChainMono: Mono[A], f: Try[A] => Mono[B], isGuarded: Boolean) extends AbstractTask[B] {
		override def subscribeSync(downChainObserver: MonoObserver[B]): Subscription = {
			new Subscription with MonoObserver[A] {
				private var isActive = true
				private var maybeUpChainSubscription: Maybe[Subscription] = Maybe.empty
				private var maybeInnerSubscription: Maybe[Subscription] = Maybe.empty

				{ // Constructor
					val upChainSubscription = upChainMono.subscribeSync(this)
					if isActive then maybeUpChainSubscription = Maybe(upChainSubscription)
				}

				override def onSuccess(a: A): Unit = handle(Success(a))

				override def onError(e: Throwable): Unit = handle(Failure(e))

				private def handle(tryA: Try[A]): Unit = {
					if isActive then {
						maybeUpChainSubscription = Maybe.empty
						val maybeMonoB = if isGuarded then try Maybe(f(tryA)) catch {
							case NonFatal(e) =>
								isActive = false
								maybeUpChainSubscription = Maybe.empty
								downChainObserver.onError(e)
								Maybe.empty
						} else Maybe(f(tryA))
						maybeMonoB.foreach { monoB =>
							val innerSubscription = monoB.subscribeSync(downChainObserver)
							if isActive then maybeInnerSubscription = Maybe(innerSubscription)
						}
					}
				}

				override def unsubscribeSync(): Unit = {
					checkWithin()
					isActive = false
					val mus = maybeUpChainSubscription
					maybeUpChainSubscription = Maybe.empty
					mus.foreach(_.unsubscribeSync())
					val mis = maybeInnerSubscription
					maybeInnerSubscription = Maybe.empty
					mis.foreach(_.unsubscribeSync())
				}
			}
		}

		override def toString: String = deriveToString[Task_TransformWith[A, B]](this)
	}

	//// Concrete implementations of [[Task]] returned by factory methods ////

	/** $suppressSyntheticCompanionObject */
	private inline def Task_Never(trap: Nothing): Any = trap

	class Task_Never extends AbstractTask[Nothing] {
		override def subscribeSync(downChainObserver: MonoObserver[Nothing]): Subscription = {
			// Nothing is ever emitted, so returns empty subscription
			Subscription_empty
		}

		override def toString: String = deriveToString[Task_Never](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_Ready(trap: Nothing): Any = trap

	final class Task_Ready[+A](a: A) extends AbstractTask[A] {
		override def subscribeSync(downChainObserver: MonoObserver[A]): Subscription = {
			// Completes immediately, so returns empty subscription
			downChainObserver.onSuccess(a)
			Subscription_empty
		}

		override def toFuture(isWithinDoSiThEx: Boolean = isInSequence): Future[A] = Future.successful(a)

		override def toString: String = deriveToString[Task_Ready[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_Fail(trap: Nothing): Any = trap

	final class Task_Fail(e: Throwable) extends AbstractTask[Nothing] {
		override def subscribeSync(downChainObserver: MonoObserver[Nothing]): Subscription = {
			downChainObserver.onError(e)
			Subscription_empty
		}

		override def toFuture(isWithinDoSiThEx: Boolean = isInSequence): Future[Nothing] = Future.failed(e)

		override def toString: String = deriveToString[Task_Fail](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_Apply(trap: Nothing): Any = trap

	final class Task_Apply[+A](supplier: () => A) extends AbstractTask[A] {
		override def subscribeSync(downChainObserver: MonoObserver[A]): Subscription = {
			downChainObserver.onSuccess(supplier())
			Subscription_empty
		}

		override def toString: String = deriveToString[Task_Apply[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_ApplyGuarded(trap: Nothing): Any = trap

	final class Task_ApplyGuarded[+A](supplier: () => A) extends AbstractTask[A] {
		override def subscribeSync(downChainObserver: MonoObserver[A]): Subscription = {
			val maybeA = try Maybe(supplier()) catch {
				case NonFatal(e) =>
					downChainObserver.onError(e)
					Maybe.empty
			}
			maybeA.foreach(downChainObserver.onSuccess)
			Subscription_empty
		}

		override def toString: String = deriveToString[Task_ApplyGuarded[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_Defers(trap: Nothing): Any = trap

	final class Task_Defers[+A](supplier: () => Task[A]) extends AbstractTask[A] {
		override def subscribeSync(downChainObserver: MonoObserver[A]): Subscription = {
			// Propagate the inner subscription directly
			supplier().subscribeSync(downChainObserver)
		}

		override def toString: String = deriveToString[Task_Defers[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_DefersGuarded(trap: Nothing): Any = trap

	final class Task_DefersGuarded[+A](supplier: () => Task[A]) extends AbstractTask[A] {
		override def subscribeSync(downChainObserver: MonoObserver[A]): Subscription = {
			val maybeTaskA = try Maybe(supplier()) catch {
				case NonFatal(e) =>
					downChainObserver.onError(e)
					Maybe.empty
			}
			maybeTaskA.fold(Subscription_empty)(_.subscribeSync(downChainObserver))
		}

		override def toString: String = deriveToString[Task_DefersGuarded[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_FromMono(trap: Nothing): Any = trap

	final class Task_FromMono[+A](monoA: Mono[A]) extends AbstractTask[A] {
		override def subscribeSync(downChainObserver: MonoObserver[A]): Subscription = monoA.subscribeSync(downChainObserver)

		override def toString: String = deriveToString[Task_FromMono[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_FromForeign(trap: Nothing): Any = trap

	final class Task_FromForeign[+A](foreignDoer: Doer, foreignMono: foreignDoer.Mono[A]) extends AbstractTask[A] {
		override def subscribeSync(downChainSubscripton: MonoObserver[A]): Subscription = {
			new Subscription with foreignDoer.MonoObserver[A] with Runnable {
				@volatile private var isActive = true
				@volatile private var maybeForeignSubscription: Maybe[foreignDoer.Subscription] = Maybe.empty

				{ // Constructor
					foreignDoer.executeSequentially(this)
				}

				override def run(): Unit = {
					if isActive then {
						val foreignSubscription = foreignMono.subscribeSync(this)
						// Note: Unlike single-threaded tasks (such as [[Task_FlatMap]]), we do not perform defensive checks to guarantee the clearing of maybeForeignSubscription because a failure to clear the reference is very rare and only results in a transient, minor memory leak (which is reclaimed once the delegating subscription is garbage collected), the performance and complexity cost of such optimization is not justified here.
						maybeForeignSubscription = Maybe(foreignSubscription)
					}
				}

				override def onSuccess(a: A): Unit = { // runs in foreignDoer
					if isActive then {
						maybeForeignSubscription = Maybe.empty
						thisDoer.run {
							if isActive then {
								isActive = false
								downChainSubscripton.onSuccess(a)
							}
						}
					}
				}

				override def onError(e: Throwable): Unit = { // runs in foreignDoer
					if isActive then {
						maybeForeignSubscription = Maybe.empty
						thisDoer.run {
							if isActive then {
								isActive = false
								downChainSubscripton.onError(e)
							}
						}
					}
				}

				override def unsubscribeSync(): Unit = {
					if isActive then {
						isActive = false
						foreignDoer.run {
							val mfs = maybeForeignSubscription
							maybeForeignSubscription = Maybe.empty
							mfs.foreach(_.unsubscribeSync())
						}
					}
				}
			}
		}

		override def toString: String = deriveToString[Task_FromForeign[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_FromFuture(trap: Nothing): Any = trap

	final class Task_FromFuture[+A](future: Future[A]) extends AbstractTask[A] {
		override def subscribeSync(downChainObserver: MonoObserver[A]): Subscription = {
			new Subscription with (Try[A] => Unit) {
				private var active = true
				{ // Constructor
					future.onComplete(this)(using ownSerialExecutionContext)
				}

				override def apply(tryA: Try[A]): Unit = {
					if active then tryA match {
						case Success(a) => downChainObserver.onSuccess(a)
						case Failure(ex) => downChainObserver.onError(ex)
					}
				}

				override def unsubscribeSync(): Unit = {
					checkWithin()
					active = false
				}
			}
		}

		override def toString: String = deriveToString[Task_FromFuture[A]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_FromFutureSupplier(trap: Nothing): Any = trap

	final class Task_FromFutureSupplier[+A](supplier: () => Future[A], isGuarded: Boolean) extends AbstractTask[A] {
		override def subscribeSync(downChainObserver: MonoObserver[A]): Subscription = {
			new Subscription with (Try[A] => Unit) {
				private var active = true
				{ // Constructor
					val maybeFuture =
						if isGuarded then try Maybe(supplier()) catch {
							case NonFatal(e) =>
								downChainObserver.onError(e)
								Maybe.empty
						} else Maybe(supplier())
					maybeFuture.foreach(_.onComplete(this)(using ownSerialExecutionContext))
				}

				override def apply(tryA: Try[A]): Unit = {
					if active then tryA match {
						case Success(a) => downChainObserver.onSuccess(a)
						case Failure(ex) => downChainObserver.onError(ex)
					}
				}

				override def unsubscribeSync(): Unit = {
					checkWithin()
					active = false
				}
			}
		}

		override def toString: String = deriveToString[Task_FromFutureSupplier[A]](this)
	}


	/** $suppressSyntheticCompanionObject */
	private inline def Task_Combined(trap: Nothing): Any = trap

	final class Task_Combined[+A, +B, +C](taskA: Task[A], taskB: Task[B], f: (A, B) => C, isGuarded: Boolean) extends AbstractTask[C] {
		override def subscribeSync(downChainObserver: MonoObserver[C]): Subscription = new Subscription with MonoObserver[A] {
			private var isActive = true
			private var maybeA: Maybe[A] = Maybe.empty
			private var maybeB: Maybe[B] = Maybe.empty
			private var maybeSubscriptionA: Maybe[Subscription] = Maybe.empty
			private var maybeSubscriptionB: Maybe[Subscription] = Maybe.empty

			{ // Constructor
				val subscriptionA = taskA.subscribeSync(this)

				if isActive then {
					if maybeA.isEmpty then maybeSubscriptionA = Maybe(subscriptionA)
					val subscriptionB = taskB.subscribeSync(new MonoObserver[B] {
						override def onSuccess(b: B): Unit = {
							if isActive then {
								maybeSubscriptionB = Maybe.empty
								maybeA.fold {
									maybeB = Maybe(b)
								} { a => zip(a, b) }
							}
						}

						override def onError(e: Throwable): Unit = {
							if isActive then {
								isActive = false
								maybeSubscriptionB = Maybe.empty
								downChainObserver.onError(e)
								maybeSubscriptionA.foreach(_.unsubscribeSync())
							}
						}
					})
					if isActive && maybeB.isEmpty then maybeSubscriptionB = Maybe(subscriptionB)
				}
			}

			override def onSuccess(a: A): Unit = {
				if isActive then {
					maybeSubscriptionA = Maybe.empty
					maybeB.fold {
						maybeA = Maybe(a)
					} { b => zip(a, b) }
				}
			}

			override def onError(e: Throwable): Unit = {
				if isActive then {
					isActive = false
					maybeSubscriptionA = Maybe.empty
					downChainObserver.onError(e)
					maybeSubscriptionB.foreach(_.unsubscribeSync())
				}
			}

			private def zip(a: A, b: B): Unit = {
				isActive = false
				val maybeC =
					if isGuarded then try Maybe(f(a, b)) catch {
						case NonFatal(e) =>
							downChainObserver.onError(e)
							Maybe.empty
					} else Maybe(f(a, b))
				maybeC.foreach(downChainObserver.onSuccess)
			}

			override def unsubscribeSync(): Unit = {
				checkWithin()
				if isActive then {
					isActive = false
					val msa = maybeSubscriptionA
					val msb = maybeSubscriptionB
					maybeSubscriptionA = Maybe.empty
					maybeSubscriptionB = Maybe.empty
					msa.foreach(_.unsubscribeSync())
					msb.foreach(_.unsubscribeSync())
				}
			}
		}

		override def toString: String = deriveToString[Task_Combined[A, B, C]](this)
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_Sequence(trap: Nothing): Any = trap

	/** @see [[Task_sequenceToArray]] */
	final class Task_Sequence[A: ClassTag, +C[x] <: Iterable[x]](monos: C[Mono[A]]) extends AbstractTask[Array[A]] {
		override def subscribeSync(downChainObserver: MonoObserver[Array[A]]): Subscription = {
			val size = monos.size
			val array = Array.ofDim[A](size)
			if size == 0 then {
				downChainObserver.onSuccess(array)
				Subscription_empty
			} else new Subscription {
				private var completedCounter: Int = 0
				private var isActive = true
				private val subscriptions = new Array[Subscription](size)

				{ // Constructor
					var index = 0
					val monosIterator = monos.iterator
					while index < size && isActive do {
						val mono = monosIterator.next()
						val monoIndex = index
						val subscription = mono.subscribeSync(new MonoObserver[A] { // TODO this allocation could be avoided if the MonoObserver propagated the subscription id/index.
							override def onSuccess(a: A): Unit = {
								if isActive then {
									array(monoIndex) = a
									completedCounter += 1
									if completedCounter == size then {
										isActive = false
										downChainObserver.onSuccess(array)
									}
								}
							}

							override def onError(ex: Throwable): Unit = {
								if isActive then {
									isActive = false
									downChainObserver.onError(ex)
									unsubscribeAll()
								}
							}
						})
						if isActive then subscriptions(monoIndex) = subscription
						index += 1
					}
				}

				private final def unsubscribeAll(): Unit = {
					var index = 0
					while index < size do {
						val sub = subscriptions(index)
						if sub != null then {
							subscriptions(index) = null
							sub.unsubscribeSync()
						}
						index += 1
					}
				}

				override def unsubscribeSync(): Unit = {
					checkWithin()
					if isActive then {
						isActive = false
						unsubscribeAll()
					}
				}
			}
		}
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Task_SequenceHardy(trap: Nothing): Any = trap

	final class Task_SequenceHardyToArray[A: ClassTag, C[x] <: Iterable[x]](monos: C[Mono[A]]) extends AbstractTask[Array[Try[A]]] {
		override def subscribeSync(monoObserver: MonoObserver[Array[Try[A]]]): Subscription = {
			val size = monos.size
			val array = Array.ofDim[Try[A]](size)
			if size == 0 then {
				monoObserver.onSuccess(array)
				Subscription_empty
			} else new Subscription {
				private val monosSubscriptions = new Array[Subscription](size)
				private var completedCounter: Int = 0
				private var index: Int = 0

				{ // constructor
					val monosIterator = monos.iterator
					while index < size do {
						val mono = monosIterator.next()
						val ventureIndex = index

						val monoSubscription = new Subscription {
							private var active = true
							private var innerSub: Subscription = Subscription_empty
							{
								innerSub = mono.subscribeSync(new MonoObserver[A] {
									override def onSuccess(a: A): Unit = {
										if active then {
											array(ventureIndex) = Success(a)
											completedCounter += 1
											if completedCounter == size then monoObserver.onSuccess(array)
										}
									}

									override def onError(ex: Throwable): Unit = {
										if active then {
											array(ventureIndex) = Failure(ex)
											completedCounter += 1
											if completedCounter == size then monoObserver.onSuccess(array)
										}
									}
								})
							}

							override def unsubscribeSync(): Unit = {
								active = false
								innerSub.unsubscribeSync()
							}
						}
						monosSubscriptions(index) = monoSubscription
						index += 1
					}
				}

				override def unsubscribeSync(): Unit = {
					var i = 0
					while i < size do {
						val s = monosSubscriptions(i)
						if s != null then s.unsubscribeSync()
						i += 1
					}
				}
			}
		}
	}


}
