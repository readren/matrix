package readren.sequencer

import readren.common.Maybe

/**
 * A provided interface that regularizes the usage of [[Doer]] providers.
 *
 * Implementations of this trait provide [[Doer]] instances.
 * @tparam D The type of the provided [[Doer]]. Must extend [[Doer]].
 */
trait DoerProvider[+D <: Doer] {

	/** The type of parameter of the [[provide]] method.
	 * For implementations that create a new [[Doer]] instance for every call to [[provide]], this type is usually the same as the [[Doer.Tag]] of the provided [[Doer]] instances. */
	type Tag

	/**
	 * Supplies a [[Doer]] instance.
	 * The implementation may return the same [[Doer]] instance for different calls, in which case its documentation should mention it.
	 *
	 * @param tag
	 *   A tag that the provider should attach to the returned [[Doer]] if possible.
	 *   Only implementations that return a new instance every call are able to use it.  
	 *   Its solely goals is to help tracking to which objects the [[Doer]] is associated with, provided that said objects are also tagged.
	 *   Implementations may use the `tag` for purposes such as debugging or tracking, but this is optional and not required for the [[DoerProvider]] functionality.
	 *
	 * @note Subclasses must ensure that every `D` instance returned here is exclusively associated with this [[DoerProvider]] instance: the `D`'s [[Doer.executeSequentially]] implementation must set `this` provider's thread-local (and no other provider's), and no returned `D` instance may be shared with or returned by a provider of a different concrete type. Violating this will cause [[currentDoer]] to produce a value of the wrong type at runtime.
	 */
	def provide(tag: Tag): D

	def tagFromText(text: String): Tag

	/** @return the [[Doer]] instance — among those supplied by this [[DoerProvider]] — to which the current [[Thread]] is currently assigned, if any; [[Maybe.empty]] if the current [[Thread]] is not currently assigned to a [[Doer]] supplied by this [[DoerProvider]]. */
	def currentDoer: Maybe[D]

	/** Called when a [[Runnable]] passed to the [[Doer.executeSequentially]] method of a provided [[Doer]] throws an exception.
	 * The implementation may assume that the call is within the thread currently assigned to the provided doer. */
	protected def onUnhandledException(doer: Doer, exception: Throwable): Unit

	/** Called when the [[Doer.reportFailure]] method of a provided [[Doer]] is called. */
	protected def onFailureReported(doer: Doer, failure: Throwable): Unit

}