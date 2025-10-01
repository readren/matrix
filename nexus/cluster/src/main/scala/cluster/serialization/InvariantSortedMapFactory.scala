package readren.nexus
package cluster.serialization

import scala.collection.{SortedMapFactory, immutable, mutable}

/** An invariant wrapper of an [[scala.collection.SortedMapFactory]][SMC] instance. Used to suppress the covariance of the [[scala.collection.SortedMapFactory]] trait.
 *
 * @tparam SMC the type constructor of the sorted map collection for which the wrapped factory generates builders. */
class InvariantSortedMapFactory[SMC[_, _]](val factory: SortedMapFactory[SMC])

object InvariantSortedMapFactory {

	given genSortedMapFactory: InvariantSortedMapFactory[scala.collection.SortedMap] = new InvariantSortedMapFactory(scala.collection.SortedMap)

	given immutableSortedMapFactory: InvariantSortedMapFactory[immutable.SortedMap] = new InvariantSortedMapFactory(immutable.SortedMap)
	given immutableTreeMapFactory: InvariantSortedMapFactory[immutable.TreeMap] = new InvariantSortedMapFactory(immutable.TreeMap)

	given mutableSortedMapFactory: InvariantSortedMapFactory[mutable.SortedMap] = new InvariantSortedMapFactory(mutable.SortedMap)
	given mutableTreeMapFactory: InvariantSortedMapFactory[mutable.TreeMap] = new InvariantSortedMapFactory(mutable.TreeMap)
}