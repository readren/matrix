package readren.matrix
package cluster.misc

object DoNothing extends (Any => Unit) {
	override def apply(v1: Any): Unit = ()
}
