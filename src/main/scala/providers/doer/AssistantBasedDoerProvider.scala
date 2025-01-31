package readren.matrix
package providers.doer 

import core.Matrix.DoerProvider
import core.{AbstractMatrix, MatrixDoer}
import providers.ShutdownAble
import providers.doer.AssistantBasedDoerProvider.DoerAssistantProvider

import readren.taskflow.Doer

import java.util.concurrent.TimeUnit

object AssistantBasedDoerProvider {
	/**
	 * Defines how assistant providers must expose their methods to be adapted by [[AssistantBasedDoerProvider]].
	 *
	 * Implementations of this trait provide assistants that serve as parameters to construct [[MatrixDoer]] instances.
	 */
	trait DoerAssistantProvider {

		/** The type of assistant provided. Must extend [[Doer.Assistant]]. */
		type ProvidedAssistant <: Doer.Assistant

		/**
		 * Supplies a [[Doer.Assistant]] to be used in constructing a [[MatrixDoer]].
		 *
		 * @param serial
		 *   A unique identifier for the [[MatrixDoer]] that the provided [[Doer.Assistant]] will be associated with.
		 *   This identifier is unique for each call to this method. Implementations may use the `serial` for purposes
		 *   such as debugging or tracking, but this is optional and not required for the assistant's functionality.
		 *   The method may return the same [[ProvidedAssistant]] instance for different calls.
		 */
		def provide(serial: MatrixDoer.Id): ProvidedAssistant
	}
}

/**
 * Adapts an assistant provider to a [[DoerProvider]].
 *
 * This class serves as an adapter between a [[DoerAssistantProvider]] and the [[DoerProvider]] interface,
 * allowing the use of assistants provided by the former to create [[MatrixDoer]] instances. It also delegates
 * lifecycle management operations (e.g., shutdown) to the underlying assistant provider.
 */
abstract class AssistantBasedDoerProvider[MD <: MatrixDoer] extends DoerProvider[MD], ShutdownAble {

	/** The underlying assistant provider that this adapter wraps. */
	protected val assistantProvider: DoerAssistantProvider & ShutdownAble

	/**
	 * Creates a [[MatrixDoer]] using an assistant provided by the underlying [[DoerAssistantProvider]].
	 *
	 * @param matrix
	 *   The [[AbstractMatrix]] instance for which a [[MatrixDoer]] is required.
	 * @return
	 *   A new [[MatrixDoer]] instance initialized with the provided assistant and the given matrix.
	 */
	override def provide(matrix: AbstractMatrix): MD

	/**
	 * Shuts down the underlying assistant provider.
	 */
	override def shutdown(): Unit = assistantProvider.shutdown()

	/**
	 * Waits for the underlying assistant provider to terminate.
	 *
	 * @param timeout
	 *   Maximum time to wait.
	 * @param unit
	 *   Time unit of the timeout parameter.
	 * @return
	 *   `true` if the termination completed within the timeout, `false` otherwise.
	 */
	override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = assistantProvider.awaitTermination(timeout, unit)

	/**
	 * Appends diagnostic information from the underlying assistant provider.
	 *
	 * @param stringBuilder
	 *   A [[StringBuilder]] to append diagnostic information to.
	 * @return
	 *   The same [[StringBuilder]] instance with appended diagnostic information.
	 */
	override def diagnose(stringBuilder: StringBuilder): StringBuilder = assistantProvider.diagnose(stringBuilder)
}

