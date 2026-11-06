package readren.consensus
package env

import protocol.SnapshotPayload

import java.nio.ByteBuffer
import java.nio.channels.{ReadableByteChannel, WritableByteChannel}
import scala.annotation.targetName

/** [[SnapshotPayload]] implementation backed by an in-memory [[ByteBuffer]], avoiding disk I/O for tests. */
final class BufferSnapshotPayload(val buffer: ByteBuffer) extends SnapshotPayload {
	override def openChannel(): ReadableByteChannel = {
		val duplicate = buffer.duplicate()
		new ReadableByteChannel {
			private var open = true
			override def read(dst: ByteBuffer): Int = {
				if !open then throw new java.nio.channels.ClosedChannelException()
				if !duplicate.hasRemaining then -1
				else {
					val toTransfer = math.min(dst.remaining(), duplicate.remaining())
					val limit = duplicate.limit()
					duplicate.limit(duplicate.position() + toTransfer)
					dst.put(duplicate)
					duplicate.limit(limit)
					toTransfer
				}
			}
			override def isOpen: Boolean = open
			override def close(): Unit = {
				open = false
			}
		}
	}

	override def size: Long = buffer.remaining().toLong

	override def transferTo(target: WritableByteChannel): Long = {
		val duplicate = buffer.duplicate()
		val total = duplicate.remaining().toLong
		while duplicate.hasRemaining do {
			target.write(duplicate)
		}
		total
	}
}

object BufferSnapshotPayload {
	def apply(bytes: Array[Byte]): BufferSnapshotPayload = new BufferSnapshotPayload(ByteBuffer.wrap(bytes))
	@targetName("fromIArray")
	def apply(bytes: IArray[Byte]): BufferSnapshotPayload = new BufferSnapshotPayload(ByteBuffer.wrap(bytes.unsafeArray))
}
