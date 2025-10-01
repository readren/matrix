package readren.nexus
package cluster.serialization

/** The mode in which the match-case construct is implemented for derived serializers/deserializer.
 * The mode only has relevance when the sum-type has a nested sum-type.
 * The mode affects the binary representation and whether the [[DiscriminationCriteria]] should provide discriminator values for nested sum-types:
 * - In the [[NestedSumMatchMode.FLAT]] mode, nested sum-types don't require a discriminator value, and only one discriminator leads the binary representation.
 * - In the [[NestedSumMatchMode.TREE]] mode, nested sum-types require a discriminator value, and the binary representation is lead with as many discriminators as the depth of the variant in the ADT hierarchy.
 * Note that, independently of the mode, nested sum-types for which a given [[Serializer]]/[[Deserializer]] exists require that the [[DiscriminationCriteria]] provide a value for the sum-type which will lead the binary representation of its child variants.
 * */
type NestedSumMatchMode = Boolean

object NestedSumMatchMode {
	/** The generated match-case construct has a flat structure. All the cases at zero depth.
	 * Besides the product-types, only the sum-types for which a Serializer/Deserializer is given require a discriminator.
	 * Only one discriminator leads the binary representation of products, unless it is a child of a nested sum-type for which a [[Serializer]]/[[Deserializer]] is given.
	 * This mode is more efficient in both binary representation size and execution speed.
	 **/
	inline val FLAT: true = true

	/**
	 * The generated match-case construct has a tree structure resembling the type hierarchy.
	 * Both product-types and nested sum-types require a discriminator.
	 * The number of leading discriminators in the binary representation equals the depth of the product-type's depth in the ADT hierarchy.
	 * The only advantage of this mode is that the binary representation is independent on whether given [[Serializer]]/[[Deserializer]] exists for nested sum-types. */
	inline val TREE: false = false
}

