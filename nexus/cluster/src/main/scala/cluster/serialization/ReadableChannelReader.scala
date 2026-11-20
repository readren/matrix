package readren.nexus
package cluster.serialization

import java.nio.ByteBuffer
import java.nio.channels.ReadableByteChannel

/** A pull-based [[Deserializer.Reader]] backed by a synchronous [[ReadableByteChannel]] (such as a channel opened from a sealed [[readren.common.ByteSource]]) with an isolated [[RecyclableObjectBuffer]] reference-deduplication table.
 * Tracks remaining stream capacity using 64-bit `Long` arithmetic to support streams exceeding [[Int.MaxValue]] bytes while clamping [[remainingContentBytes]] to [[Int.MaxValue]] for per-collection validation.
 * @param channel the source [[ReadableByteChannel]] from which serialized bytes are pulled.
 * @param byteLength the exact total number of bytes expected to be read from `channel`.
 * @param governingVersion the [[ProtocolVersion]] governing deserialization of the stream.
 * @param allocationLimits the security bounds governing maximum permissible allocations during deserialization.
 * @param transferBufferCapacity the byte capacity of the internal heap transfer buffer used to batch channel reads (default 64 KiB).*/
final class ReadableChannelReader(
	val channel: ReadableByteChannel,
	val byteLength: Long,
	override val governingVersion: ProtocolVersion,
	allocationLimits: AllocationLimits = AllocationLimits.Default,
	val transferBufferCapacity: Int = ReadableChannelReader.DefaultTransferBufferCapacity
) extends Deserializer.Reader(new RecyclableObjectBuffer(), allocationLimits) {
	require(byteLength >= 0L, s"byteLength must be non-negative, got $byteLength")
	require(transferBufferCapacity > 0, s"transferBufferCapacity must be positive, got $transferBufferCapacity")

	private val transferBuffer: ByteBuffer = {
		val initialCap = math.min(transferBufferCapacity.toLong, math.max(1L, byteLength)).toInt
		val buf = ByteBuffer.allocate(initialCap)
		buf.limit(0)
		buf
	}

	private var totalBytesConsumed: Long = 0L

	/** Total number of unread bytes remaining in the declared `byteLength` stream. */
	inline def remainingStreamBytes: Long = byteLength - totalBytesConsumed

	override def position: Int = math.min(totalBytesConsumed, Int.MaxValue.toLong).toInt

	override def remainingContentBytes: Int = math.min(remainingStreamBytes, Int.MaxValue.toLong).toInt

	private def fillTransferBuffer(): Unit = {
		while !transferBuffer.hasRemaining do {
			val remainingInStream = byteLength - totalBytesConsumed
			if remainingInStream <= 0L then {
				throw new Deserializer.LengthExceedsRemainingBytesException(position, 1L, 0, "ReadableChannelReader")
			}
			transferBuffer.clear()
			val maxToRead = math.min(transferBuffer.capacity().toLong, remainingInStream).toInt
			transferBuffer.limit(maxToRead)
			val bytesRead = channel.read(transferBuffer)
			if bytesRead < 0 then {
				throw new Deserializer.DeserializationException(position, s"Premature end of channel in ReadableChannelReader: expected $byteLength bytes, reached EOF after $totalBytesConsumed bytes")
			}
			transferBuffer.flip()
		}
	}

	override def peekByte: Byte = {
		fillTransferBuffer()
		transferBuffer.get(transferBuffer.position())
	}

	override def readByte(): Byte = {
		fillTransferBuffer()
		val byte = transferBuffer.get()
		totalBytesConsumed += 1L
		byte
	}

	override def readBytes(destination: Array[Byte], offset: Int, length: Int): Unit = {
		if length < 0 then throw new Deserializer.NegativeLengthException(position, length.toLong, "Array[Byte]")
		if length.toLong > remainingStreamBytes then {
			throw new Deserializer.LengthExceedsRemainingBytesException(position, length.toLong, remainingContentBytes, "ReadableChannelReader")
		}
		var currentOffset = offset
		var remainingToCopy = length
		while remainingToCopy > 0 do {
			fillTransferBuffer()
			val chunk = math.min(remainingToCopy, transferBuffer.remaining())
			transferBuffer.get(destination, currentOffset, chunk)
			currentOffset += chunk
			remainingToCopy -= chunk
			totalBytesConsumed += chunk.toLong
		}
	}

	/** Verifies that all `byteLength` bytes declared for this stream have been consumed.
	 * @throws Deserializer.DeserializationException if unconsumed bytes remain in the stream.*/
	def requireFullyConsumed(): Unit = {
		if totalBytesConsumed != byteLength then {
			throw new Deserializer.DeserializationException(position, s"Stream was not fully consumed: expected $byteLength bytes, consumed $totalBytesConsumed bytes (${byteLength - totalBytesConsumed} bytes remaining)")
		}
	}
}

object ReadableChannelReader {
	inline val DefaultTransferBufferCapacity = 64 * 1024
}
