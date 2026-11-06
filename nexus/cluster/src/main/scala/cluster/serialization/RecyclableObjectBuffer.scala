package readren.nexus
package cluster.serialization

import java.util
import java.util.Arrays

/**
 * A flat, recyclable array buffer specialized for `AnyRef` deduplication on the read path.
 *
 * Provides indexed O(1) access and in-place [[reset]] that nulls only used elements `0 until size`
 * to allow garbage collection of deserialized objects while preserving allocated capacity.
 */
final class RecyclableObjectBuffer(initialCapacity: Int = 64) {
	require(initialCapacity > 0, "Initial capacity must be positive")

	private var elements: Array[AnyRef] = new Array[AnyRef](math.max(16, initialCapacity))
	private var _size: Int = 0

	def size: Int = _size

	def isEmpty: Boolean = _size == 0

	def nonEmpty: Boolean = _size > 0

	def add(ref: AnyRef): Int = {
		require(ref ne null, "Null references are not supported in deduplication buffer")
		if _size == elements.length then {
			elements = util.Arrays.copyOf(elements, elements.length << 1)
		}
		val idx = _size
		elements(idx) = ref
		_size += 1
		idx
	}

	def apply(index: Int): AnyRef = {
		if index < 0 || index >= _size then {
			throw new IndexOutOfBoundsException(s"$index is out of bounds for deduplication buffer of size ${_size}")
		}
		elements(index)
	}

	/**
	 * Resets the buffer in-place, nulling only occupied positions to allow GC
	 * while retaining backing array allocation.
	 */
	def reset(): Unit = {
		if _size > 0 then {
			util.Arrays.fill(elements, 0, _size, null)
			_size = 0
		}
	}
}
