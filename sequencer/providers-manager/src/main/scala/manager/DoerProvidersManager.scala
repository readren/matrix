package readren.sequencer
package manager

/** Specifies how a [[Matrix]] instance interacts with the object responsible for managing the [[DoerProviderDescriptor]]s and corresponding [[DoerProvider]]s that the [[Matrix]] instance uses. */
trait DoerProvidersManager {

	/** Gets the [[DoerProvider]] associated with the provided [[DoerProviderDescriptor]]. If none exists one is created. */
	def get[D <: Doer](descriptor: DoerProviderDescriptor[D]): descriptor.DP

	inline def provideDoer[D <: Doer](descriptor: DoerProviderDescriptor[D], tag: descriptor.Tag): D =
		get(descriptor).provide(tag)

	inline def provideDoer[D <: Doer](text: String, descriptor: DoerProviderDescriptor[D]): D = {
		val provider = get(descriptor)
		provider.provide(provider.tagFromText(text))
	}
}