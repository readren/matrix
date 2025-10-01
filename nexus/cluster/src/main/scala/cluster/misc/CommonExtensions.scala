package readren.nexus
package cluster.misc

object CommonExtensions {
	extension [K, V <: AnyRef](map: Map[K, V]) {
		/** Equivalent to `get(key).fold(f, default)` without memory allocation for map implementations with efficient `getOrElse`.
		 * Requires that the map does not contain legitimate null values. */
		inline def getAndTransformOrElse[X](key: K, default: X)(inline f: V => X): X = map.getOrElse(key, null) match {
			case null => default
			case v: V => f(v)
		}

		/** Equivalent to `get(key).map(f)` without memory allocation for map implementations with efficient `getOrElse`.
		 * Requires that the map does not contain legitimate null values. */
		inline def getAndApply(key: K)(inline f: V => Unit): Unit = {
			val v = map.getOrElse(key, null)
			if v ne null then f(v.asInstanceOf[V])
		}
	}
}
