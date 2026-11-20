package readren.common

import java.nio.ByteBuffer
import java.nio.channels.{ClosedChannelException, CompletionHandler}

/** An in-memory [[ByteSink]] implementation backed by a writable [[ByteBuffer]].\
 * Wraps a caller-supplied buffer across its initial `[position, limit)` range, or allocates a heap [[ByteBuffer]] via the secondary `Int` constructor to avoid direct-memory zeroing and GC cleaner overhead for small streams.
 * @param backing the writable [[ByteBuffer]] whose initial remaining range (`[position, limit)`) will be populated. */
final class BufferByteSink(backing: ByteBuffer) extends ByteSink {
	require(!backing.isReadOnly, "BufferByteSink requires a writable ByteBuffer")

	private var closed: Boolean = false

	/** Allocates a heap-backed [[BufferByteSink]] of the specified byte length.
	 * @param byteLength the exact capacity of the heap [[ByteBuffer]] to allocate. */
	def this(byteLength: Int) = this(ByteBuffer.allocate(byteLength))

	override val expectedSize: Long = backing.remaining().toLong

	override val byteSource: BufferByteSource = new BufferByteSource(backing.asReadOnlyBuffer())

	override def write[A](src: ByteBuffer, attachment: A, handler: CompletionHandler[Integer, ? >: A]): Int = {
		if closed then throw new ClosedChannelException()
		val bytesToWrite = src.remaining()
		backing.put(src)
		bytesToWrite
	}

	override def seal(): Unit = {
		closed = true
	}

	override def discard(): Unit = {
		closed = true
	}
}
