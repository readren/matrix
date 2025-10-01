package readren.sequencer
package providers

import providers.StandardSchedulingDp.ProvidedDoerFacade

import readren.common.CompileTime.getTypeName
import readren.common.{Maybe, deriveToString}

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentLinkedQueue, Executors, ScheduledFuture, TimeUnit}

object StandardSchedulingDp {
	trait ProvidedDoerFacade extends Doer, SchedulingExtension, LoopingExtension, ShutdownAble {
		/** @return true if the provided [[Schedule]] was activated and still not fully cancelled. */
		def isActive(schedule: Schedule): Boolean
	}

	class Impl(
		failureReporter: (Doer, Throwable) => Unit = DefaultDoerFaultReporter(true),
		unhandledExceptionReporter: (Doer, Throwable) => Unit = DefaultDoerFaultReporter(false),
	) extends StandardSchedulingDp {
		override type Tag = String

		override def tagFromText(text: String): String = text

		/** Called when a [[Runnable]] passed to the [[Doer.executeSequentially]] method of a provided [[Doer]] throws an exception. */
		override protected def onUnhandledException(doer: Doer, exception: Throwable): Unit = unhandledExceptionReporter(doer, exception)

		/** Called when the [[Doer.reportFailure]] method of a provided [[Doer]] is called. */
		override protected def onFailureReported(doer: Doer, failure: Throwable): Unit = failureReporter(doer, failure)
	}
}

/** A [[DoerProvider]] that provides [[Doer]] with [[SchedulingExtension]] instances which own a dedicated single-thread-scheduled-executor instance provided by [[Executors.newSingleThreadScheduledExecutor]].
 * Designed for testing, but can be used in production in scenarios where few instances of [[Doer & SchedulingExtension]] are created.
 *
 */
trait StandardSchedulingDp extends DoerProvider[StandardSchedulingDp.ProvidedDoerFacade], ShutdownAble { thisProvider =>
	/** Thread-local storage for the current Doer instance. */
	private val currentDoerThreadLocal: ThreadLocal[DoerImpl] = new ThreadLocal()

	private val providedDoers: java.util.concurrent.ConcurrentLinkedQueue[DoerImpl] = new ConcurrentLinkedQueue()

	override def provide(tag: Tag): StandardSchedulingDp.ProvidedDoerFacade = {
		val doer = new DoerImpl(tag)
		providedDoers.add(doer)
		doer
	}

	override def currentDoer: Maybe[ProvidedDoerFacade] = Maybe(currentDoerThreadLocal.get)

	/** A Doer implementation with embedded testing infrastructure.
	 *
	 * This Doer provides the necessary infrastructure for testing while maintaining
	 * the abstract interface required by Doer and SchedulingExtension.
	 */
	class DoerImpl(override val tag: Tag) extends AbstractDoer, StandardSchedulingDp.ProvidedDoerFacade { thisDoer =>

		override type Tag = thisProvider.Tag

		/** The single-threaded executor for this Doer. */
		private[StandardSchedulingDp] val doSiThEx = Executors.newSingleThreadScheduledExecutor()

		/** Sequencer for tracking execution order. */
		private val sequencer: AtomicInteger = new AtomicInteger(0)

		override def executeSequentially(runnable: Runnable): Unit = {
			val id = sequencer.incrementAndGet()
			// println(s"queuedForSequentialExecution: pre execute; id=$id, thread=${Thread.currentThread().getName}; runnable=$runnable")
			doSiThEx.execute(() => {
				currentDoerThreadLocal.set(thisDoer)
				// println(s"queuedForSequentialExecution: pre run; id=$id; thread=${Thread.currentThread().getName}")
				try {
					runnable.run()
					// println(s"queuedForSequentialExecution: run completed normally; id=$id; thread=${Thread.currentThread().getName}")
				}
				catch {
					case cause: Throwable =>
						// println(s"queuedForSequentialExecution: run completed abruptly with: $cause; id=$id; thread=${Thread.currentThread().getName}")
						onUnhandledException(thisDoer, cause)
						throw cause
				} finally {
					currentDoerThreadLocal.remove()
					// println(s"queuedForSequentialExecution: finally; id=$id; thread=${Thread.currentThread().getName}")
				}
			})
		}

		override def current: Maybe[ProvidedDoerFacade] = Maybe(currentDoerThreadLocal.get)

		override def reportFailure(failure: Throwable): Unit = onFailureReported(thisDoer, failure)

		//// SCHEDULING EXTENSION

		sealed abstract class TSchedule {
			/** This variable's value is changed one time only, from `null` to the [[ScheduledFuture]] reference returned by [[doSiThEx.schedule]] when [[scheduleSequentially]] is called.
			 * Therefore, there is no need to synchronize access to it from the [[Runnable]] that is created when the value is set.*/
			private[DoerImpl] var scheduledFuture: ScheduledFuture[?] | Null = null
			/** This variable's value is changed one time only, from `false` to `true`, when [[cancel]] or [[cancelAll]] is called. */
			@volatile private[DoerImpl] var canceled: Boolean = false
		}

		class TDelaySchedule(val delay: MilliDuration) extends TSchedule {
			override def toString: String = deriveToString[TDelaySchedule](this)
		}

		class TFixedRateSchedule(val initialDelay: MilliDuration, val interval: MilliDuration) extends TSchedule {
			override def toString: String = deriveToString[TFixedRateSchedule](this)
		}

		class TFixedDelaySchedule(val initialDelay: MilliDuration, val delay: MilliDuration) extends TSchedule {
			override def toString: String = deriveToString[TFixedDelaySchedule](this)
		}

		/** needed to support [[cancelAll]]. */
		private val activatedSchedules: java.util.concurrent.ConcurrentLinkedQueue[TSchedule] = new java.util.concurrent.ConcurrentLinkedQueue()

		override type Schedule = TSchedule

		override def newDelaySchedule(delay: MilliDuration): TDelaySchedule = TDelaySchedule(delay)

		override def newFixedRateSchedule(initialDelay: MilliDuration, interval: MilliDuration): TFixedRateSchedule = TFixedRateSchedule(initialDelay, interval)

		override def newFixedDelaySchedule(initialDelay: MilliDuration, delay: MilliDuration): Schedule = TFixedDelaySchedule(initialDelay, delay)

		override def scheduleSequentially(schedule: Schedule, routine: Schedule => Unit): Unit = {
			schedule.synchronized {
				if schedule.canceled then return
				else if schedule.scheduledFuture ne null then throw IllegalStateException(s"The ${getTypeName[Schedule]} instance `$schedule` was already used before and can't be used twice.")
				else {
					schedule.scheduledFuture = schedule match {
						case ds: TDelaySchedule =>
							val wrapper: Runnable = () =>
								if !schedule.scheduledFuture.isDone then {
									currentDoerThreadLocal.set(this)
									try routine(schedule) // TODO: use the ThreadFactory to setup the unhandled exceptions handler instead of this try-catch
									catch {
										case cause: Throwable =>
											onUnhandledException(thisDoer, cause)
											throw cause
									} finally {
										currentDoerThreadLocal.remove()
										activatedSchedules.remove(schedule)
									}
								}
							doSiThEx.schedule(wrapper, ds.delay, TimeUnit.MILLISECONDS)

						case frs: TFixedRateSchedule =>
							val wrapper: Runnable = () => if !schedule.scheduledFuture.isDone then {
								try routine(schedule) // TODO: use the ThreadFactory to setup the unhandled exceptions handler instead of this try-catch
								catch {
									case cause: Throwable =>
										onUnhandledException(thisDoer, cause)
										throw cause
								}

							}
							doSiThEx.scheduleAtFixedRate(wrapper, frs.initialDelay, frs.interval, TimeUnit.MILLISECONDS)

						case fds: TFixedDelaySchedule =>
							val wrapper: Runnable = () => if !schedule.scheduledFuture.isDone then {
								try routine(schedule) // TODO: use the ThreadFactory to setup the unhandled exceptions handler instead of this try-catch
								catch {
									case cause: Throwable =>
										onUnhandledException(thisDoer, cause)
										throw cause
								}

							}
							doSiThEx.scheduleWithFixedDelay(wrapper, fds.initialDelay, fds.delay, TimeUnit.MILLISECONDS)
					}
					activatedSchedules.add(schedule)
				}
			}
		}

		/** @inheritdoc
		 * Internal design assumption: This implementation assumes that [[schedule.scheduledFuture]] is modified a single time only, from `null` to `non-null`. */
		override def cancel(schedule: Schedule): Unit = {
			schedule.canceled = true
			if (schedule.scheduledFuture ne null) || schedule.synchronized(schedule.scheduledFuture ne null) then {
				schedule.scheduledFuture.cancel(false)
				activatedSchedules.remove(schedule)
			}
		}

		override def cancelAll(): Unit = {
			activatedSchedules.forEach { schedule =>
				cancel(schedule)
			}
		}

		/** @inheritdoc
		 * Design assumption: This implementation assumes that [[schedule.scheduledFuture]] is modified a single time only, from `null` to `non-null`. */
		override def wasActivated(schedule: Schedule): Boolean =
			(schedule.scheduledFuture ne null) || schedule.synchronized(schedule.scheduledFuture ne null)

		def isActive(schedule: Schedule): Boolean = {
			if schedule.scheduledFuture ne null then !schedule.scheduledFuture.isDone
			else if schedule.synchronized(schedule.scheduledFuture ne null) then !schedule.scheduledFuture.isDone
			else false
		}

		override def isCanceled(schedule: TSchedule): Boolean = schedule.canceled

		/** @inheritdoc
		 * This implementation shutdowns the executor and cleans up resources. */
		override def shutdown(): Unit = {
			doSiThEx.shutdown()
			providedDoers.remove(thisDoer)
		}

		override def awaitTermination(timeout: MilliDuration, unit: TimeUnit): Boolean =
			doSiThEx.awaitTermination(timeout, unit)

		override def diagnose(stringBuilder: StringBuilder): StringBuilder = ??? // TODO
	}

	override def shutdown(): Unit = {
		providedDoers.forEach(_.doSiThEx.shutdown())
		providedDoers.clear()
	}

	override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = {
		val timeoutMillis: MilliDuration = unit.toMillis(timeout)
		val startingTime = System.currentTimeMillis()
		var lastTerminated = true
		val iterator = providedDoers.iterator()
		while lastTerminated && iterator.hasNext do {
			val executor = iterator.next().doSiThEx
			if !executor.isTerminated then {
				val remainingTime = timeoutMillis - (System.currentTimeMillis() - startingTime)
				lastTerminated =
					if remainingTime > 0 then executor.awaitTermination(remainingTime, TimeUnit.MILLISECONDS)
					else false
			}
		}
		lastTerminated
	}

	override def diagnose(stringBuilder: StringBuilder): StringBuilder = ???
}
