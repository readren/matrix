package readren.matrix
package cluster.serialization

/** The mode in which the match-case construct is implemented for derived serializers/deserializer.
 * The mode only has relevance when a sum-type is nested.
 * The mode affects the binary representation:
 * [[NestedSumMatchMode.TREE]] uses more discriminators (one per depth-level of ADT hierarchy) than the other two.
 * [[NestedSumMatchMode.FLAT]] and [[NestedSumMatchMode.NEST]] produce/interpret the same representation (have the same behavior). Only the implementation differs and only for the serializers. For deserializers, these two modes are indistinct. */
type NestedSumMatchMode = Int

object NestedSumMatchMode {
	/** All the match-cases of the generated code are at depth zero. 
	 * Besides the product-types, only the sum-types for which a Serializer/Deserializer is given require a discriminator.
	 * Only one discriminator precedes products that aren't part of a nested sum-type for which a Serializer/Deserializer is given.  
	 **/
	inline val FLAT: 0 = 0

	/** For serializers, the depth of the generated match-cases equals the depth of the variant in the ADT hierarchy.
	 * For deserializer, all the match-cases of the generated code are at depth zero
	 * Besides the product-types, only the sum-types for which a Serializer/Deserializer is given require a discriminator.
	 * Only one discriminator precedes products that aren't part of a nested sum-type for which a Serializer/Deserializer is given. */  
	inline val NEST: 1 = 1

	/** The depth of the generated match-cases equals the depth of the variant in the ADT hierarchy.
	 * Both product-types and nested sum-types require a discriminator.
	 * The number of discriminators that precedes a product-type equals the depth of the product-type in the ADT hierarchy. */
	inline val TREE: 2 = 2
}

