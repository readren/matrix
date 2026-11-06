package readren.consensus
package protocol

import java.nio.channels.{ReadableByteChannel, WritableByteChannel}

/** A repeatable supplier of streaming byte channels representing a serialized snapshot.
 *
 * To support zero-copy network replication, repeated transmission to multiple lagging followers,
 * and lightweight in-memory testing without cursor exhaustion, this abstraction supplies fresh
 * [[ReadableByteChannel]] instances on demand and encapsulates the byte length.
 */
trait SnapshotPayload extends AutoCloseable {
	/** Opens a fresh [[ReadableByteChannel]] positioned at the beginning of the snapshot payload. */
	def openChannel(): ReadableByteChannel

	/** The total size of the snapshot payload in bytes. */
	def size: Long

	/** Transfers the entire snapshot payload to the target [[WritableByteChannel]].
	 *
	 * @param target the destination channel to write bytes to.
	 * @return the total number of bytes transferred.
	 */
	def transferTo(target: WritableByteChannel): Long

	override def close(): Unit = ()
}
