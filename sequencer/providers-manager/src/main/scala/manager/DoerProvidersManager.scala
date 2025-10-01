package readren.sequencer
package manager

/** Manages and provides [[DoerProvider]] instances. */
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