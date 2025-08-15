package readren.matrix
package providers

import providers.DoerProvider.Tag

import readren.sequencer.Doer

object DoerProvider {
	type Tag = String // TODO move inside the trait and make abstract. 
}
/**
 * A provided interface that regularizes the usage of [[Doer]] providers.
 *
 * Implementations of this trait provide [[Doer]] instances.
 * @tparam D The type of the provided [[Doer]]. Must extend [[Doer]].
 */
trait DoerProvider[+D <: Doer] {

	/**
	 * Supplies a [[Doer]] instance.
	 * The implementation may return the same [[Doer]] instance for different calls, in which case its documentation should mention it.
	 *
	 * @param tag
	 *   A tag that the provider should attach to the returned [[Doer]] if possible.
	 *   Only implementations that return a new instance every call are able to use it.  
	 *   Its solely goals is to help tracking to which objects the [[Doer]] is associated with, provided that said objects are also tagged.
	 *   Implementations may use the `tag` for purposes such as debugging or tracking, but this is optional and not required for the [[DoerProvider]] functionality.
	 */
	def provide(tag: Tag): D
}