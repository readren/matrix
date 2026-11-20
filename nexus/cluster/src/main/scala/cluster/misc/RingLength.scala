package readren.nexus
package cluster.misc

/** Positive number of circular buffers in a [[DualEndedCircularStorage]] ring (`>= 1`). */
opaque type RingLength <: Int = Int

object RingLength {
	/** Constructs a [[RingLength]] after validating that `length` is positive (`>= 1`).
	 * @param length the number of circular buffers; must be at least `1`.
	 * @throws IllegalArgumentException if `length < 1`. */
	def apply(length: Int): RingLength = {
		require(length >= 1, s"RingLength must be positive (>= 1), got $length")
		length
	}

	/** Returns the smaller of two [[RingLength]] values. */
	inline def min(a: RingLength, b: RingLength): RingLength = if a <= b then a else b
}
