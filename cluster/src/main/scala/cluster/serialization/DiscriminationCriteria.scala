package readren.matrix
package cluster.serialization

trait DiscriminationCriteria[T] {
	def getFor[S <: T](s: S): Int
}