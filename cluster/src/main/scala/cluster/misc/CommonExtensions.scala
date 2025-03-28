package readren.matrix
package cluster.misc

object CommonExtensions {
	extension [K, V <: AnyRef](map: Map[K, V]) {
		/** Equivalent to `get(key).fold(f, default)` without memory allocation.
		 * Requires that the map does not contain legitimate null values. */
		inline def mapOrElse[X](key: K, default: X)(inline f: V => X): X = map.getOrElse(key, null) match {
			case null => default
			case v: V => f(v)
		}
	}
}
