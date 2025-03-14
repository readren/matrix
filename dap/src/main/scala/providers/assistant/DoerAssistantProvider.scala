package readren.matrix
package providers.assistant

import providers.assistant.DoerAssistantProvider.Tag

import readren.taskflow.Doer

object DoerAssistantProvider {
	type Tag = Long
}
/**
 * A provided interface that regularizes the usage of assistant providers.
 *
 * Implementations of this trait provide assistants that serve as parameters to construct [[MatrixDoer]] instances.
 * @tparam A The type of the provided assistant. Must extend [[Doer.Assistant]].
 */
trait DoerAssistantProvider[+A <: Doer.Assistant] {

	/**
	 * Supplies a [[Doer.Assistant]] to be used in constructing a [[Doer]] instance.
	 * The method may return the same [[Doer.Assistant]] instance for different calls.
	 *
	 * @param serial
	 *   A tag that the provider may attach to the returned assistant for diagnostic purposes only.
	 *   Only implementations that return a new instance every call are able to use it.  
	 *   Its solely goals is to help tracking to which objects the assistant is associated with, provided that said objects are also tagged with the same serial. 
	 *   Implementations may use the `serial` for purposes such as debugging or tracking, but this is optional and not required for the assistant's nor provider functionality.
	 */
	def provide(serial: Tag): A
}