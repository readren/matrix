package readren.matrix
package cluster.serialization


import scala.collection.{MapFactory, immutable, mutable}

/** An invariant wrapper of an [[scala.collection.MapFactory]] instance. Used to suppress the covariance of the [[scala.collection.MapFactory]] trait.
 *
 * @tparam MC the type constructor of the map collection for which the wrapped factory generates builders. */
class InvariantMapFactory[MC[_, _]](val factory: MapFactory[MC]);

object InvariantMapFactory {

	//		type NvhMf[UMC[_, _]] = InvariantMapFactory[UMC]
	//		@inline private def nvhMf[UMC[_, _]](factory: MapFactory[UMC]) = new InvariantMapFactory(factory)

	given genericMapFactory: InvariantMapFactory[scala.collection.Map] = new InvariantMapFactory(scala.collection.Map)

	given immutableMapFactory: InvariantMapFactory[immutable.Map] = new InvariantMapFactory(immutable.Map)
	given immutableHashMapFactory: InvariantMapFactory[immutable.HashMap] = new InvariantMapFactory(immutable.HashMap)
	given immutableSeqMapFactory: InvariantMapFactory[immutable.SeqMap] = new InvariantMapFactory(immutable.SeqMap)
	given immutableListMapFactory: InvariantMapFactory[immutable.ListMap] = new InvariantMapFactory(immutable.ListMap)

	given mutableMapFactory: InvariantMapFactory[mutable.Map] = new InvariantMapFactory(mutable.Map)
	given mutableHashMapFactory: InvariantMapFactory[mutable.HashMap] = new InvariantMapFactory(mutable.HashMap)
}