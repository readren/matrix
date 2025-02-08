package readren.matrix
package providers

import core.Matrix.*
import core.MatrixDoer
import utils.CompileTime.getTypeName

import readren.taskflow.Doer

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

/** An implementation of the [[DoerProvidersManager]] trait that is [[ShutdownAble]].
 * Call to methods inherited from [[ShutdownAble]] are propagated to all the [[DoerProvider]] instances managed by this instance, provided the [[DoerProvider]] are [[ShutdownAble]].  */
class ShutdownAbleDpd extends DoerProvidersManager, ShutdownAble {

	private val registeredProviders: ConcurrentHashMap[DoerProviderDescriptor[?], DoerProvider[?]] = new ConcurrentHashMap()
	private val wasShutdown: AtomicBoolean = new AtomicBoolean(false)

	/** Gets the [[DoerProvider]] associated with the provided [[DoerProviderDescriptor]]. If none exists one is created.
	 * @throws IllegalStateException if both, this method is called after [[shutdown]] has been called, and it is the first time that this method receives this reference or an equivalent one. */
	override def get[D <: MatrixDoer](descriptor: DoerProviderDescriptor[D]): DoerProvider[D] = {
		val provider = registeredProviders.computeIfAbsent(descriptor, descriptor => {
			if wasShutdown.get() then null
			else descriptor.build(this)
		}).asInstanceOf[DoerProvider[D]]
		if provider == null then throw new IllegalStateException(s"A ${getTypeName[ShutdownAbleDpd]} instance was asked to build a new instances of ${getTypeName[DoerProvider[D]]} after it was shutdown.")
		else provider
	}

	def shutdown(): Unit = {
		if wasShutdown.compareAndSet(false, true) then {
			registeredProviders.forEach((dapKind, dap) =>
				dap match {
					case s: ShutdownAble => s.shutdown()
					case _ => // do nothing
				}
			)
		}
	}

	/**
	 * Blocks until all thread have completed execution after a shutdown request, or the timeout occurs, or the current thread is interrupted, whichever happens first.
	 *
	 * @param timeout the maximum time to wait.
	 * @param unit the time unit of the timeout argument.
	 * @return `true` if all the registered doer providers terminated and `false` if the timeout elapsed before all have terminated.
	 * @throws InterruptedException if interrupted while waiting
	 */
	def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = {
		var allProvidersAreTerminated = true
		val enumeration = registeredProviders.elements()
		var remainingNanos = unit.toNanos(timeout)
		var startingNanoTime = System.nanoTime()
		while enumeration.hasMoreElements && allProvidersAreTerminated && remainingNanos > 0 do {
			enumeration.nextElement() match {
				case provider: ShutdownAble =>
					allProvidersAreTerminated = provider.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS)
					val currentNanoTime = System.nanoTime()
					remainingNanos -= currentNanoTime - startingNanoTime
					startingNanoTime = currentNanoTime
				case _ => // do nothing
			}
		}
		allProvidersAreTerminated
	}

	override def diagnose(sb: StringBuilder): StringBuilder = {
		val enumeration = registeredProviders.elements()
		while enumeration.hasMoreElements do {
			enumeration.nextElement() match {
				case provider: ShutdownAble => provider.diagnose(sb)
				case _ => // do nothing
			}
		}
		sb
	}
}
