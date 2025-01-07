package readren.matrix
package dap

import Matrix.*

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

class CloseableDoerAssistantProvidersManager extends DoerAssistantProviderManager, ShutdownAble {

	private val registeredDaps: ConcurrentHashMap[DoerAssistantProviderRef[DoerAssistantProvider], DoerAssistantProvider] = new ConcurrentHashMap()
	private val wasShutdown: AtomicBoolean = new AtomicBoolean(false)

	/** Gets the [[DoerAssistantProvider]] associated with the provided [[DoerAssistantProviderRef]]. If none exists one is created.
	 * @throws IllegalStateException if both, this method is called after [[shutdown]] has been called, and it is the first time that this method receives this reference or an equivalent one. */
	override def get[A <: DoerAssistantProvider](ref: DoerAssistantProviderRef[A]): A = {
		val dap = registeredDaps.computeIfAbsent(ref, ref => {
			if wasShutdown.get() then null
			else ref.build(this)
		}).asInstanceOf[A]
		if dap == null then throw new IllegalStateException("Instances of CloseableDoerAssistantProviderManager that were shutdown should not build new instances of DoerAssistantProvider")
		else dap
	}

	def shutdown(): Unit = {
		if wasShutdown.compareAndSet(false, true) then {
			registeredDaps.forEach((ref, dap) =>
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
		var allDapsAreTerminated = true
		val enumeration = registeredDaps.elements()
		var remainingNanos = unit.toNanos(timeout)
		var startingNanoTime = System.nanoTime()
		while enumeration.hasMoreElements && allDapsAreTerminated && remainingNanos > 0 do {
			enumeration.nextElement() match {
				case dap: ShutdownAble =>
					allDapsAreTerminated = dap.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS)
					val currentNanoTime = System.nanoTime()
					remainingNanos -= currentNanoTime - startingNanoTime
					startingNanoTime = currentNanoTime
				case _ => // do nothing
			}
		}
		allDapsAreTerminated
	}

	override def diagnose(sb: StringBuilder): StringBuilder = {
		val enumeration = registeredDaps.elements()
		while enumeration.hasMoreElements do {
			enumeration.nextElement() match {
				case dap: ShutdownAble => dap.diagnose(sb)
				case _ => // do nothing
			}
		}
		sb
	}
}
