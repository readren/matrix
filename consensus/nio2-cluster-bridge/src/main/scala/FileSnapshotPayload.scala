package readren.consensus
package nio2

import protocol.SnapshotPayload

import java.nio.ByteBuffer
import java.nio.channels.{FileChannel, ReadableByteChannel, WritableByteChannel}
import java.nio.file.{Files, Path, StandardOpenOption}

/** [[SnapshotPayload]] implementation backed by an on-disk file, supporting zero-copy [[FileChannel.transferTo]]. */
final class FileSnapshotPayload(val path: Path, val deleteOnClose: Boolean = false) extends SnapshotPayload {
	override def openChannel(): FileChannel = FileChannel.open(path, StandardOpenOption.READ)

	override def size: Long = {
		val ch = openChannel()
		try ch.size() finally ch.close()
	}

	override def transferTo(target: WritableByteChannel): Long = {
		val ch = openChannel()
		try {
			val total = ch.size()
			var position = 0L
			while position < total do {
				val transferred = ch.transferTo(position, total - position, target)
				if transferred <= 0 then throw new java.io.IOException(s"Zero-copy transfer stalled at position $position of $total")
				position += transferred
			}
			position
		} finally {
			ch.close()
		}
	}

	override def close(): Unit = {
		if deleteOnClose then {
			try Files.deleteIfExists(path) catch { case _: Throwable => () }
		}
	}
}

/** In-memory [[SnapshotPayload]] implementation backed by a [[ByteBuffer]]. */
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
