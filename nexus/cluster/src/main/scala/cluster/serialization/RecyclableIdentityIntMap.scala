package readren.nexus
package cluster.serialization

import java.util
import java.util.Arrays

/**
 * An open-addressing, linear-probing hash map specialized for `AnyRef -> Int`.
 * Designed for high-performance reference deduplication with zero steady-state heap allocations.
 *
 * Keys are compared by object identity (`eq`), and values are primitive `Int`s to avoid boxing.
 * Provides an in-place [[reset]] method to clear entries across composite message frames without
 * reallocating backing arrays.
 */
final class RecyclableIdentityIntMap(initialCapacity: Int = 64) {
	require(initialCapacity > 0, "Initial capacity must be positive")

	private def ceilingPowerOfTwo(n: Int): Int = {
		val highest = Integer.highestOneBit(n)
		if highest == n then n else highest << 1
	}

	private var capacity: Int = math.max(16, ceilingPowerOfTwo(initialCapacity))
	private var mask: Int = capacity - 1
	private var threshold: Int = (capacity * 0.65).toInt

	private var keys: Array[AnyRef] = new Array[AnyRef](capacity)
	private var values: Array[Int] = new Array[Int](capacity)
	private var _size: Int = 0

	def size: Int = _size

	def isEmpty: Boolean = _size == 0

	def nonEmpty: Boolean = _size > 0

	private inline def scramble(h: Int): Int = {
		val x = h ^ (h >>> 16)
		(x * 0x45d9f3b) & 0x7fffffff
	}

	/**
	 * Returns the existing index associated with `ref` if present,
	 * or associates `ref` with `valueIfAbsent` and returns `valueIfAbsent`.
	 */
	def putIfAbsent(ref: AnyRef, valueIfAbsent: Int): Int = {
		require(ref ne null, "Null references are not supported in deduplication table")
		val hash = scramble(System.identityHashCode(ref))
		var idx = hash & mask
		while true do {
			val existing = keys(idx)
			if existing eq null then {
				keys(idx) = ref
				values(idx) = valueIfAbsent
				_size += 1
				if _size >= threshold then rehash()
				return valueIfAbsent
			} else if existing eq ref then {
				return values(idx)
			}
			idx = (idx + 1) & mask
		}
		valueIfAbsent
	}

	/**
	 * Resets the map in-place, zeroing object references to permit garbage collection
	 * while retaining backing array allocations for subsequent messages.
	 */
	def reset(): Unit = {
		if _size > 0 then {
			util.Arrays.fill(keys, null)
			_size = 0
		}
	}

	private def rehash(): Unit = {
		val oldCap = capacity
		val oldKeys = keys
		val oldValues = values

		capacity = oldCap << 1
		mask = capacity - 1
		threshold = (capacity * 0.65).toInt

		keys = new Array[AnyRef](capacity)
		values = new Array[Int](capacity)

		var i = 0
		while i < oldCap do {
			val k = oldKeys(i)
			if k ne null then {
				val v = oldValues(i)
				val hash = scramble(System.identityHashCode(k))
				var idx = hash & mask
				while keys(idx) ne null do {
					idx = (idx + 1) & mask
				}
				keys(idx) = k
				values(idx) = v
			}
			i += 1
		}
	}
}
