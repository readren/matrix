package readren.nexus
package cluster.serialization

import java.nio.ByteBuffer
import scala.collection.mutable.ArrayBuffer

/** A standalone [[Serializer.Writer]] that encodes values into a growable sequence of heap [[ByteBuffer]] chunks with an isolated [[RecyclableIdentityIntMap]] reference-deduplication table.
 * Enforces an upper bound (`maxEncodedBytes`) on the total number of bytes produced so oversized payloads fail fast before exhausting heap memory.
 * @param governingVersion the [[ProtocolVersion]] governing serialization of the encoded payload.
 * @param chunkCapacity the default byte capacity allocated for each heap [[ByteBuffer]] chunk (default 64 KiB).
 * @param maxEncodedBytes the maximum total number of bytes allowed to be written into this writer before throwing [[Serializer.SerializationException]].*/
final class ChunkedHeapWriter(
	override val governingVersion: ProtocolVersion,
	val chunkCapacity: Int = ChunkedHeapWriter.DefaultChunkCapacity,
	val maxEncodedBytes: Long = Long.MaxValue
) extends Serializer.Writer(new RecyclableIdentityIntMap()) {
	require(chunkCapacity >= Serializer.BUFFER_SIZE_REQUIRED_BY_MOST_DEMANDING_OPERATION, s"chunkCapacity must be at least ${Serializer.BUFFER_SIZE_REQUIRED_BY_MOST_DEMANDING_OPERATION}, got $chunkCapacity")
	require(maxEncodedBytes >= 0L, s"maxEncodedBytes must be non-negative, got $maxEncodedBytes")

	private val completedChunks: ArrayBuffer[ByteBuffer] = new ArrayBuffer[ByteBuffer](4)
	private var activeBuffer: ByteBuffer = ByteBuffer.allocate(math.min(chunkCapacity.toLong, math.max(Serializer.BUFFER_SIZE_REQUIRED_BY_MOST_DEMANDING_OPERATION.toLong, maxEncodedBytes)).toInt)
	private var completedBytesCount: Long = 0L

	/** Total number of bytes written to this writer so far across completed chunks and the active buffer. */
	def totalBytesWritten: Long = completedBytesCount + activeBuffer.position().toLong

	override def position: Int = math.min(totalBytesWritten, Int.MaxValue.toLong).toInt

	protected override def getBuffer(minimumRemaining: Int): ByteBuffer = {
		val currentTotal = totalBytesWritten
		if currentTotal + minimumRemaining.toLong > maxEncodedBytes then {
			throw new Serializer.SerializationException(position, s"Encoded payload size (${currentTotal + minimumRemaining.toLong} bytes) exceeds maximum allowed limit ($maxEncodedBytes bytes)")
		}
		if activeBuffer.remaining() < minimumRemaining then {
			if activeBuffer.position() > 0 then {
				completedBytesCount += activeBuffer.position().toLong
				activeBuffer.flip()
				completedChunks.addOne(activeBuffer)
			}
			val remainingBudget = maxEncodedBytes - completedBytesCount
			val nextCapacity = math.max(minimumRemaining.toLong, math.min(chunkCapacity.toLong, remainingBudget)).toInt
			activeBuffer = ByteBuffer.allocate(nextCapacity)
		}
		activeBuffer
	}

	override def putBytes(bytes: Array[Byte], offset: Int, length: Int): Unit = {
		if totalBytesWritten + length.toLong > maxEncodedBytes then {
			throw new Serializer.SerializationException(position, s"Encoded payload size (${totalBytesWritten + length.toLong} bytes) exceeds maximum allowed limit ($maxEncodedBytes bytes)")
		}
		super.putBytes(bytes, offset, length)
	}

	override def putBuffer(source: ByteBuffer): Unit = {
		if totalBytesWritten + source.remaining().toLong > maxEncodedBytes then {
			throw new Serializer.SerializationException(position, s"Encoded payload size (${totalBytesWritten + source.remaining().toLong} bytes) exceeds maximum allowed limit ($maxEncodedBytes bytes)")
		}
		super.putBuffer(source)
	}

	/** Finalizes the active buffer and returns the immutable sequence of flipped [[ByteBuffer]] chunks together with the exact total byte count written.
	 * @return a tuple `(chunks, totalBytes)` where each buffer in `chunks` is positioned at `0` with limit equal to its written byte count.*/
	def finish(): (IArray[ByteBuffer], Long) = {
		if activeBuffer.position() > 0 then {
			completedBytesCount += activeBuffer.position().toLong
			activeBuffer.flip()
			completedChunks.addOne(activeBuffer)
			activeBuffer = ByteBuffer.allocate(0)
		}
		(IArray.unsafeFromArray(completedChunks.toArray), completedBytesCount)
	}
}

object ChunkedHeapWriter {
	inline val DefaultChunkCapacity = 64 * 1024
}
