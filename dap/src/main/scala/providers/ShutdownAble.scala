package readren.matrix
package providers

import java.util.concurrent.TimeUnit

trait ShutdownAble {

	/** Initiates the shutdown of this [[ShutdownAble]].
	 * The behavior of this instance and the conditions that trigger the termination depend on the implementation.
	 * 
	 * This method does not wait the shutdown process to complete. Use [[awaitTermination]] to do that.
	 * @throws SecurityException if a security manager exists and shutting down this [[ShutdownAble]] may manipulate threads that the caller is not permitted to modify because it does not hold RuntimePermission("modifyThread"), or the security manager's checkAccess method denies access. 
	 */
	def shutdown(): Unit

	/**
	 * Blocks until all thread have completed execution after a shutdown request, or the timeout occurs, or the current thread is interrupted, whichever happens first.
	 *
	 * @param timeout the maximum time to wait.
	 * @param unit the time unit of the timeout argument.
	 * @return `true` if this doer provider terminated and `false` if the timeout elapsed before termination.
	 * @throws InterruptedException if interrupted while waiting
	 */
	def awaitTermination(timeout: Long, unit: TimeUnit): Boolean

	def diagnose(stringBuilder: StringBuilder): StringBuilder
}
