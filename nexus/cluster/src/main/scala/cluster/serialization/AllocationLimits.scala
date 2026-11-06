package readren.nexus
package cluster.serialization

/** Configurable memory and element allocation thresholds to defend against adversarial payloads during deserialization.
 * @param maxAllocationBytes the maximum allowed memory allocation in bytes for any single array or collection
 * @param maxCollectionElements the maximum allowed number of elements in any single collection or array
 * @param maxStringCharacters the maximum allowed number of characters in a deserialized string */
final class AllocationLimits(
	val maxAllocationBytes: Int = AllocationLimits.DefaultMaxAllocationBytes,
	val maxCollectionElements: Int = AllocationLimits.DefaultMaxCollectionElements,
	val maxStringCharacters: Int = AllocationLimits.DefaultMaxStringCharacters
)

object AllocationLimits {
	inline val DefaultMaxAllocationBytes = 16 * 1024 * 1024 // 16 MiB
	inline val DefaultMaxCollectionElements = 1_000_000     // 1,000,000 items
	inline val DefaultMaxStringCharacters = 1_000_000       // 1,000,000 chars

	val Default: AllocationLimits = new AllocationLimits()
}
