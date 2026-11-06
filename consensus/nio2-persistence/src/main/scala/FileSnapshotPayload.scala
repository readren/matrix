package readren.consensus
package persistence.nio2

import readren.consensus.protocol.SnapshotPayload

import java.nio.channels.{FileChannel, WritableByteChannel}
import java.nio.file.{Path, StandardOpenOption}

/** [[SnapshotPayload]] implementation backed by an on-disk file, supporting zero-copy [[FileChannel.transferTo]]. */
final class FileSnapshotPayload(val path: Path) extends SnapshotPayload {
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
}
