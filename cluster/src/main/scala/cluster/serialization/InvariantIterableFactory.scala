package readren.matrix
package cluster.serialization

import scala.collection.{IterableFactory, immutable, mutable}

/** An invariant wrapper of an [[scala.collection.IterableFactory]][IC] instance. Used to suppress the covariance of the [[scala.collection.IterableFactory]] trait.
 *
 *  @tparam IC the type constructor of the iterable collection for which the wrapped factory generates builders. */
class InvariantIterableFactory[IC[_]](val factory: IterableFactory[IC]) {
	val id: String = factory.getClass.getSimpleName
}

object InvariantIterableFactory {

	given genIterableFactory: InvariantIterableFactory[scala.collection.Iterable] = new InvariantIterableFactory(scala.collection.Iterable)
	given genSeqFactory: InvariantIterableFactory[scala.collection.Seq] = new InvariantIterableFactory(scala.collection.Seq)
	given genIndexedSeqFactory: InvariantIterableFactory[scala.collection.IndexedSeq] = new InvariantIterableFactory(scala.collection.IndexedSeq)
	given genLinearSeqFactory: InvariantIterableFactory[scala.collection.LinearSeq] = new InvariantIterableFactory(scala.collection.LinearSeq)
	given genSetFactory: InvariantIterableFactory[scala.collection.Set] = new InvariantIterableFactory(scala.collection.Set)

	given immutableIterableFactory: InvariantIterableFactory[immutable.Iterable] = new InvariantIterableFactory(immutable.Iterable)
	given immutableSeqFactory: InvariantIterableFactory[immutable.Seq] = new InvariantIterableFactory(immutable.Seq)
	given immutableIndexedSeqFactory: InvariantIterableFactory[immutable.IndexedSeq] = new InvariantIterableFactory(immutable.IndexedSeq)
	given immutableLinearSeqFactory: InvariantIterableFactory[immutable.LinearSeq] = new InvariantIterableFactory(immutable.LinearSeq)
	given immutableListFactory: InvariantIterableFactory[immutable.List] = new InvariantIterableFactory(immutable.List)
	given immutableVectorFactory: InvariantIterableFactory[immutable.Vector] = new InvariantIterableFactory(immutable.Vector)
	given immutableSetFactory: InvariantIterableFactory[immutable.Set] = new InvariantIterableFactory(immutable.Set)

	given mutableIterableFactory: InvariantIterableFactory[mutable.Iterable] = new InvariantIterableFactory(mutable.Iterable)
	given mutableArrayBufferFactory: InvariantIterableFactory[mutable.ArrayBuffer] = new InvariantIterableFactory(mutable.ArrayBuffer)
	given mutableListBufferFactory: InvariantIterableFactory[mutable.ListBuffer] = new InvariantIterableFactory(mutable.ListBuffer)
	given mutableQueueFactory: InvariantIterableFactory[mutable.Queue] = new InvariantIterableFactory(mutable.Queue)
	given mutableArrayDequeFactory: InvariantIterableFactory[mutable.ArrayDeque] = new InvariantIterableFactory(mutable.ArrayDeque)
	given mutableStackFactory: InvariantIterableFactory[mutable.Stack] = new InvariantIterableFactory(mutable.Stack)

}