package readren.common

import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousFileChannel, CompletionHandler}
import java.nio.file.{Files, Path, StandardOpenOption}
import scala.util.control.NonFatal

/** A single-use, write-only staged accumulator that ingests bytes asynchronously and transitions into an immutable [[ByteSource]].
 *
 * ==Capabilities & Operational Guarantees==
 *  - '''Declared Capacity:''' Declares an [[expectedSize]] upfront, enabling implementations to pre-allocate memory buffers or file allocations.
 *  - '''Early Handle Projection:''' Exposes the resulting [[byteSource]] handle immediately upon sink construction, allowing callers to bind references and assemble composite domain structures before byte transfer completes.
 *  - '''Asynchronous Ingestion:''' Ingests byte chunks non-blocking via [[write]] using standard NIO.2 completion callbacks.
 *  - '''Strict Terminal Lifecycle:''' An active sink begins in the ingesting state and must transition into exactly one of two mutually exclusive terminal states:
 *    - [[seal]]: Flushes all written data to storage, releases write locks, and finalizes the projected [[ByteSource]] for reading.
 *    - [[discard]]: Aborts ingestion immediately, closes channels, and unlinks/deletes any ephemeral physical storage (such as temporary files).
 *
 * ==Concurrency Contract (Single-Flight Invariant)==
 * At most one [[write]] operation may be pending per [[ByteSink]] instance at any given time.
 * Initiating a new write before the previous completion callback has executed is strictly prohibited.\
 * Callers without an iterative trampoline rely on implementations reporting synchronous completions via the `Int` return value rather than invoking [[java.nio.channels.CompletionHandler]] inline before returning [[ByteSink.PENDING]], preventing unbounded stack recursion when ingesting multi-chunk streams.
 *
 * @note Common use cases include staging incoming out-of-line network message streams and spooling uploaded files to temporary storage. */
trait ByteSink {

	/** The [[ByteSource]] handle that will be populated by this sink.\
	 * This handle is available immediately upon sink creation so that enclosing messages or domain objects can be constructed synchronously before byte ingestion completes.
	 * @return the projected [[ByteSource]] instance. */
	def byteSource: ByteSource

	/** The total number of bytes expected to be written to this sink. */
	def expectedSize: Long

	/** Writes bytes from the source buffer into this sink.\
	 * Consumes all bytes between `src.position()` and `src.limit()`.
	 * @param src the buffer containing the bytes to write.
	 * @param attachment an arbitrary user attachment passed through to the completion handler; may be `null`.
	 * @param handler the callback invoked upon asynchronous write completion (reporting the total bytes written) or failure.
	 * @return the number of bytes written synchronously (`>= 0`), or [[ByteSink.PENDING]] if completing asynchronously via [[handler]]. */
	def write[A](src: ByteBuffer, attachment: A, handler: CompletionHandler[Integer, ? >: A]): Int

	/** Finalizes this sink upon successful ingestion of all [[expectedSize]] bytes.\
	 * Flushes any pending buffers, closes write channel handles, and commits the underlying data so that the projected [[byteSource]] becomes readable.
	 * @throws java.io.IOException if an I/O error occurs while closing the channel or committing data to storage. */
	def seal(): Unit

	/** Aborts ingestion and immediately cleans up all allocated physical storage.\
	 * Closes underlying channel handles and unlinks/deletes any temporary files or memory buffers created for this sink. */
	def discard(): Unit
}

object ByteSink {

	/** Sentinel returned by [[ByteSink.write]] indicating that write completion will be signaled asynchronously via the handler callback. */
	inline val PENDING: -1 = -1

	/** Factory strategy for provisioning a [[ByteSink]] based on the expected byte length of an incoming stream. */
	trait Allocator {

		/** Allocates and configures a [[ByteSink]] suitable for staging a stream of the specified size.
		 * @param expectedSize the expected total length of the byte stream.
		 * @return a fresh [[ByteSink]] instance. */
		def allocate(expectedSize: Long): ByteSink
	}

	/** A size-tiered allocator that routes small streams into direct memory buffers and larger streams into temporary disk files.
	 * @param inMemoryThreshold the maximum byte length eligible for in-memory buffer staging (default 64 KiB).
	 * @param tempFilePrefix the filename prefix used when creating temporary staging files.
	 * @param tempFileSuffix the filename suffix used when creating temporary staging files. */
	class DefaultAllocator(
		val inMemoryThreshold: Int = 64 * 1024,
		val tempFilePrefix: String = "matrix-bytesource-",
		val tempFileSuffix: String = ".bin"
	) extends Allocator {

		override def allocate(expectedSize: Long): ByteSink = {
			if expectedSize <= inMemoryThreshold then new BufferByteSink(expectedSize.toInt)
			else new FileByteSink(Files.createTempFile(tempFilePrefix, tempFileSuffix), expectedSize)
		}
	}

	/** The default singleton allocator instance with a 64 KiB in-memory threshold. */
	object DefaultAllocator extends DefaultAllocator(64 * 1024, "matrix-bytesource-", ".bin")
}

/** A [[ByteSink]] implementation backed by an on-disk temporary file via [[AsynchronousFileChannel]].\
 * Upon [[seal]], the write channel is closed and the projected [[FileByteSource]] (configured with `deleteOnClose = true`) becomes readable.\
 * Upon [[discard]], the write channel is closed and the temporary file is deleted immediately.
 * @param path the file system path of the staging file.
 * @param expectedSize the total number of bytes expected to be written. */
final class FileByteSink(val path: Path, override val expectedSize: Long) extends ByteSink {
	private val channel = AsynchronousFileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
	/** The position of the next byte to write.
	 *  @note Assumes the concurrency invariant: Is thread-safe because callers must adhere to the single-flight contract (at most one outstanding read at any time).
	 * @note Memory visibility guaranteed because JLS §17.4.5 establishes a happens-before edge between write initiation and completion dispatch across NIO.2 worker threads. */
	private var position: Long = 0L

	override val byteSource: FileByteSource = new FileByteSource(path, knownSize = expectedSize, deleteOnClose = true)

	override def write[A](src: ByteBuffer, attachment: A, handler: CompletionHandler[Integer, ? >: A]): Int = {
		val bytesToWrite = src.remaining()
		if bytesToWrite == 0 then 0
		else {
			writeRemaining(src, 0, attachment, handler)
			ByteSink.PENDING
		}
	}

	private def writeRemaining[A](src: ByteBuffer, accumulated: Int, attachment: A, handler: CompletionHandler[Integer, ? >: A]): Unit = {
		channel.write(src, position, attachment, new CompletionHandler[Integer, A] {
			override def completed(result: Integer, att: A): Unit = {
				val written = if result != null && result.intValue() > 0 then result.intValue() else 0
				if written > 0 then position += written
				val totalWritten = accumulated + written
				if src.hasRemaining then writeRemaining(src, totalWritten, att, handler)
				else handler.completed(totalWritten, att)
			}
			override def failed(exc: Throwable, att: A): Unit = handler.failed(exc, att)
		})
	}

	/** @inheritdoc
	 * This implementation in particular closes the write channel, finalizing the staging file so the projected [[FileByteSource]] can be read.
	 * @throws java.io.IOException if closing the underlying [[java.nio.channels.AsynchronousFileChannel]] fails during finalization. */
	override def seal(): Unit = {
		channel.close()
	}

	/** @inheritdoc
	 * This implementation in particular closes the channel and immediately unlinks the staging file from disk to prevent orphaned files on aborted or corrupted transfers. */
	override def discard(): Unit = {
		// Rollback teardown: Suppress secondary close errors so scratch file unlinking can proceed and primary abort faults are not masked
		try channel.close() catch { case NonFatal(_) => () }
		// Best-effort unlinking: Suppress non-fatal deletion errors (e.g. file locks) to prevent aborting caller cleanup loops
		try Files.deleteIfExists(path) catch { case NonFatal(_) => () }
	}
}

/** An in-memory [[ByteSink]] implementation backed by a direct [[ByteBuffer]].\
 * Suitable for small streams within direct memory capacity ceilings.
 * @param byteLength the exact capacity of the direct memory buffer to allocate. */
final class BufferByteSink(val byteLength: Int) extends ByteSink {
	private val backing = ByteBuffer.allocateDirect(byteLength)

	override val byteSource: BufferByteSource = new BufferByteSource(backing.asReadOnlyBuffer().position(0).limit(byteLength))

	override def expectedSize: Long = byteLength.toLong

	override def write[A](src: ByteBuffer, attachment: A, handler: CompletionHandler[Integer, ? >: A]): Int = {
		val bytesToWrite = src.remaining()
		backing.put(src)
		bytesToWrite
	}

	override def seal(): Unit = ()

	override def discard(): Unit = ()
}
