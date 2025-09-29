package readren.sequencer
package manager

/** Specifies how a [[Matrix]] instance interacts with the object responsible for managing the [[DoerProviderDescriptor]]s and corresponding [[DoerProvider]]s that the [[Matrix]] instance uses. */
trait DoerProvidersManager {

	/** Gets the [[DoerProvider]] associated with the provided [[DoerProviderDescriptor]]. If none exists one is created. */
	def get[D <: Doer](descriptor: DoerProviderDescriptor[D]): DoerProvider[D]

	inline def provideDoer[D <: Doer](tag: DoerProvider.Tag, descriptor: DoerProviderDescriptor[D]): D =
		get(descriptor).provide(tag)
}