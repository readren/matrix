package readren.sequencer

import SchedulingExtension.{DELAY, ScheduleKind}

import readren.common.Maybe

import scala.collection.mutable
import scala.util.control.NonFatal

trait SchedulingDoerFluxPart { thisDoer: Doer & SchedulingExtension =>

	trait TimedFlux[+A] extends Flux[A] {
		override def subscribeSync(downChainObserver: FluxObserver[A]): TimedSubscription

		inline final def andOnSubscription(action: Schedule => Unit): TimedFlux[A] = new Flux_OnSubscription[A](this, action)
	}

	//// Flux factory methods ////

	/** Builds a [[Flux]] that schedules the execution of a supplier function according to a specified [[Schedule]] and yields the supplier’s result for each scheduled execution.\
	 * The schedule is activated only whenever the returned [[Flux]] is started, not when it is constructed.\
	 * For periodic schedules (e.g., fixed-rate or fixed-delay), the supplier is executed repeatedly, yielding each result, until the schedule is canceled.
	 *
	 * @param supplier the function that produces a value of type [[A]] for each scheduled execution.
	 * @return a [[Flux]] that yields the supplier’s result(s) according to the specified [[Schedule]]. */
	inline def Flux_schedules[A](kind: ScheduleKind, initialDelay: MilliDuration, loopDelay: MilliDuration)(supplier: TimedSubscription => A): TimedFlux[A] =
		new Flux_SchedulesSupplier(kind, initialDelay, loopDelay, supplier)

	/** Builds a [[Flux]] that schedules the execution of a [[Task]] builder according to a specified [[Schedule]] and yields the results of the [[Task]] produced by the builder for each scheduled execution.\
	 * The schedule is activated only whenever the returned [[Flux]] is started, not when it is constructed.\
	 * For periodic schedules (e.g., fixed-rate or fixed-delay), the builder is executed repeatedly, producing a new [[Task]] for each execution, and the results of each produced [[Task]] are yielded until the schedule is canceled.
	 *
	 * @param builder the function that produces a new [[Task[A]]] for each scheduled execution.
	 * @return a [[Flux]] that yields the results of the [[Task]] produced by the builder according to the specified [[Schedule]]. */
	inline def Flux_schedulesFlat[A](kind: ScheduleKind, initialDelay: MilliDuration, loopDelay: MilliDuration)(builder: TimedSubscription => Task[A]): TimedFlux[A] =
		new Flux_SchedulesSupplierFlat(kind, initialDelay, loopDelay, builder)


	//// Task extension methods for scheduled Fluxes ////

	extension [A](thisTask: Task[A]) {

		/** Returns a [[Flux]] that triggers the up-chain [[Task]] according to a [[Schedule]].
		 * The [[Schedule]] is activated whenever the returned [[Flux]] is executed.
		 * For periodic schedules (e.g., fixed-rate or fixed-delay), the up-chain [[Task]] is executed repeatedly, yielding each result, until the schedule is canceled. */
		inline def scheduled(kind: ScheduleKind, initialDelay: MilliDuration, loopDelay: MilliDuration): TimedFlux[A] = {
			new Flux_SchedulesSupplierFlat(kind, initialDelay, loopDelay, _ => thisTask, false)
		}
	}

	//// Flux operations implementation classes ////

	/** $suppressSyntheticCompanionObject */
	private inline def Flux_SchedulesSupplier(trap: Nothing): Any = trap

	final class Flux_SchedulesSupplier[A](kind: ScheduleKind, initialDelay: MilliDuration, loopDelay: MilliDuration, supplier: TimedSubscription => A) extends DefaultFlux[A], TimedFlux[A] {
		override def subscribeSync(downChainObserver: FluxObserver[A]): TimedSubscription = {
			new TimedSubscription with (Schedule => Unit) {
				private val aSchedule: Schedule = buildSchedule(kind, initialDelay, loopDelay)
				private var isActive = true
				private var elementIndex = 0

				override def schedule: Schedule = aSchedule

				{ // Constructor
					thisDoer.schedule(aSchedule)(this)
				}

				override def apply(schedule: Schedule): Unit = {
					if isActive then {
						val maybeA = try Maybe(supplier(this)) catch {
							case NonFatal(e) =>
								isActive = false
								downChainObserver.onError(e)
								Maybe.empty
						}

						maybeA.foreach { a =>
							if kind == DELAY then isActive = false
							val idx = elementIndex
							elementIndex = idx + 1
							downChainObserver.onNext(a, idx)
							if !isActive then downChainObserver.onComplete()
						}
					}
				}

				override def unsubscribeSync(): Unit = {
					if isActive then {
						isActive = false
						cancel(aSchedule)
					}
				}
			}
		}
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Flux_SchedulesSupplierFlat(trap: Nothing): Any = trap

	final class Flux_SchedulesSupplierFlat[A](kind: ScheduleKind, initialDelay: MilliDuration, loopDelay: MilliDuration, supplier: TimedSubscription => Task[A], isGuarded: Boolean = false) extends DefaultFlux[A], TimedFlux[A] {
		override def subscribeSync(downChainObserver: FluxObserver[A]): TimedSubscription = {
			new TimedSubscription with (Schedule => Unit) {
				private val aSchedule: Schedule = buildSchedule(kind, initialDelay, loopDelay)
				private var isActive = true
				/** Contains the instances of [[TickObserver]] corresponding to executions of the `upChainTask` that are not complete. */
				private val pendingObservers: mutable.LongMap[TickObserver] = mutable.LongMap.empty
				private var nextTickIndex = 0

				override def schedule: Schedule = aSchedule

				{ // Constructor
					thisDoer.schedule(aSchedule)(this)
				}

				override def apply(schedule: Schedule): Unit = {
					if isActive then {
						val maybeTaskA =
							if isGuarded then try Maybe(supplier(this)) catch {
								case NonFatal(e) =>
									if isActive then {
										unsubscribeSync()
										downChainObserver.onError(e)
									}
									Maybe.empty
							} else Maybe(supplier(this))
						maybeTaskA.foreach { taskA =>
							val tickIndex = nextTickIndex
							nextTickIndex = tickIndex + 1
							val tickObserver = new TickObserver(tickIndex)
							val innerSubscription = taskA.subscribeSync(tickObserver)
							if isActive then {
								tickObserver.maybeSubscription = Maybe(innerSubscription)
								pendingObservers.put(tickIndex, tickObserver)
							}
						}
					}
				}

				class TickObserver(val tickIndex: Int) extends MonoObserver[A] {
					var maybeSubscription: Maybe[Subscription] = Maybe.empty

					override def onSuccess(a: A): Unit = {
						if isActive then {
							maybeSubscription = Maybe.empty
							pendingObservers.remove(tickIndex)
							downChainObserver.onNext(a, tickIndex)
							if kind == DELAY then downChainObserver.onComplete()
						}
					}

					override def onError(e: Throwable): Unit = {
						if isActive then {
							unsubscribeSync()
							downChainObserver.onError(e)
						}
					}
				}

				override def unsubscribeSync(): Unit = {
					if isActive then {
						isActive = false
						cancel(aSchedule)
						pendingObservers.foreachValue { po =>
							val mus = po.maybeSubscription
							po.maybeSubscription = Maybe.empty
							mus.foreach(_.unsubscribeSync())
						}
						pendingObservers.clear()
					}
				}
			}
		}
	}

	/** $suppressSyntheticCompanionObject */
	private inline def Flux_OnSubscription(trap: Nothing): Any = trap

	final class Flux_OnSubscription[+A](fluxA: TimedFlux[A], action: Schedule => Unit) extends DefaultFlux[A], TimedFlux[A] {
		override def subscribeSync(downChainObserver: FluxObserver[A]): TimedSubscription = {
			val upChainSubscription = fluxA.subscribeSync(downChainObserver)
			try {
				action(upChainSubscription.schedule)
				upChainSubscription
			} catch {
				case scala.util.control.NonFatal(e) =>
					upChainSubscription.unsubscribeSync()
					throw e
			}
		}
	}
}
