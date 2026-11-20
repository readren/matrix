package readren.nexus
package cluster.serialization

/** A [[Deserializer.Reader]] decorator that reads a fixed `byteLength` slice from an `enclosing` reader using an isolated [[RecyclableObjectBuffer]] reference-deduplication table.
 * All back-references read through this reader resolve exclusively against objects deserialized within this nested scope rather than the enclosing message's deduplication table.
 * @param enclosing the underlying [[Deserializer.Reader]] supplying raw bytes.
 * @param byteLength the exact number of content bytes belonging to this nested region.*/
final class NestedScopeReader(
	val enclosing: Deserializer.Reader,
	val byteLength: Int
) extends Deserializer.Reader(new RecyclableObjectBuffer(), enclosing.allocationLimits) {
	require(byteLength >= 0, s"byteLength must be non-negative, got $byteLength")

	private var consumedBytes: Int = 0

	/** Returns the number of unread bytes remaining in this nested scope. */
	inline def remainingRegionBytes: Int = byteLength - consumedBytes

	override def governingVersion: ProtocolVersion = enclosing.governingVersion

	override def position: Int = enclosing.position

	override def remainingContentBytes: Int = math.min(enclosing.remainingContentBytes, remainingRegionBytes)

	override def peekByte: Byte = {
		if consumedBytes >= byteLength then {
			throw new Deserializer.LengthExceedsRemainingBytesException(position, 1L, 0, "NestedScopeReader")
		}
		enclosing.peekByte
	}

	override def readByte(): Byte = {
		if consumedBytes >= byteLength then {
			throw new Deserializer.LengthExceedsRemainingBytesException(position, 1L, 0, "NestedScopeReader")
		}
		val byte = enclosing.readByte()
		consumedBytes += 1
		byte
	}

	override def readBytes(destination: Array[Byte], offset: Int, length: Int): Unit = {
		if length < 0 then throw new Deserializer.NegativeLengthException(position, length.toLong, "Array[Byte]")
		if length > remainingRegionBytes then {
			throw new Deserializer.LengthExceedsRemainingBytesException(position, length.toLong, remainingRegionBytes, "NestedScopeReader")
		}
		enclosing.readBytes(destination, offset, length)
		consumedBytes += length
	}

	/** Verifies that all `byteLength` bytes of this nested region have been consumed.
	 * @throws Deserializer.DeserializationException if unconsumed bytes remain in the nested scope.*/
	def requireFullyConsumed(): Unit = {
		if consumedBytes != byteLength then {
			throw new Deserializer.DeserializationException(position, s"Nested scope was not fully consumed: expected $byteLength bytes, consumed $consumedBytes bytes (${byteLength - consumedBytes} bytes remaining)")
		}
	}
}
