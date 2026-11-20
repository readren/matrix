package readren.common

import java.nio.ByteBuffer
import java.nio.channels.CompletionHandler
import java.nio.file.Files

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
 * Implementations must not invoke [[java.nio.channels.CompletionHandler]] inline before returning [[ByteSink.PENDING]]: synchronous completions are reported via the returned `Int`, synchronous entry-time failures (such as writing after [[seal]] or [[discard]]) throw directly without invoking [[java.nio.channels.CompletionHandler]], and only post-[[ByteSink.PENDING]] continuations notify [[java.nio.channels.CompletionHandler]]. Callers without an iterative trampoline rely on this separation to prevent unbounded stack recursion and inverted callback ordering.
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
	 * @return the number of bytes written synchronously (`>= 0`), or [[ByteSink.PENDING]] if completing asynchronously via [[handler]].
	 * @throws java.nio.channels.ClosedChannelException if this sink has already been sealed or discarded. */
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

	/** A size-tiered allocator that routes small streams into heap memory buffers and larger streams into temporary disk files.
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