import scala.collection.mutable

val a = mutable.ArrayBuffer.empty[Int]
val b = mutable.ArrayBuffer.empty[Int]
val av = a.view
val bv = b.view
av.iterator.sameElements(av.iterator)
