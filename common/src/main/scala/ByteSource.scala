package readren.common

import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousFileChannel, ClosedChannelException, CompletionHandler, FileChannel, ReadableByteChannel, WritableByteChannel}
import java.nio.file.{Files, Path, StandardOpenOption}
import scala.annotation.targetName
import scala.util.control.NonFatal

/** An immutable, repeatable supplier of sequential byte streams with a known bounded extent.\
 * Encapsulates a finite sequence of bytes and provides multi-session extraction independence: each call to [[openAsyncReader]] or [[openChannel]] produces a fresh, isolated reading session starting at offset 0 without shared cursor mutations.
 *
 * ==Capabilities==
 *  - '''Exact Size Certainty:''' Exposes an exact byte count via [[size]].
 *  - '''Multi-Session Repeatability:''' Reading does not consume or mutate the source; multiple reader sessions can proceed concurrently or sequentially.
 *  - '''Dual Read Modalities:''' Supports asynchronous non-blocking push reads via [[openAsyncReader]] and synchronous pull reads via [[openChannel]].
 *  - '''Bulk Channel Transfer:''' Supports kernel-level zero-copy bulk transfer to target writable channels via [[transferTo]].
 *  - '''Deterministic Lifecycle:''' Implements [[AutoCloseable]] to release underlying persistent resources (e.g., deleting temporary staging files when configured).
 *
 * @note Common use cases include streaming large network message payloads, persisting consensus state snapshots, and re-reading staged direct memory buffers. */
trait ByteSource extends AutoCloseable {

	/** The total number of bytes contained in this byte source. */
	def size: Long

	/** Opens a fresh sequential asynchronous reader positioned at the beginning of this byte source.\
	 * Each call instantiates an independent session that reads from offset 0 without affecting other active readers.
	 * @return a new [[ByteSource.AsyncReader]] instance positioned at offset 0. */
	def openAsyncReader(): ByteSource.AsyncReader

	/** Opens a fresh synchronous [[ReadableByteChannel]] positioned at the beginning of this byte source.\
	 * Each call instantiates an independent channel that reads from offset 0 without affecting other active readers.
	 * @return a new [[ReadableByteChannel]] instance positioned at offset 0.
	 * @throws java.io.IOException if an I/O error occurs opening the channel. */
	def openChannel(): ReadableByteChannel

	/** Transfers the entire content of this byte source directly to the specified target [[WritableByteChannel]].\
	 * Implementations backed by physical files should leverage OS-level zero-copy channel transfer mechanisms where supported.
	 * @param target the writable channel into which bytes are transferred.
	 * @return the total number of bytes transferred.
	 * @throws java.io.IOException if an I/O error occurs during channel transfer. */
	def transferTo(target: WritableByteChannel): Long

	/** Releases any underlying system resources associated with this byte source.\
	 * The default implementation is a no-op; implementations managing temporary files or native allocations override this to perform cleanup. */
	override def close(): Unit = ()
}

object ByteSource {

	/** A single-flight sequential asynchronous reading session over a [[ByteSource]].\
	 * Encapsulates the current read cursor position across successive chunk read operations.
	 *
	 * ==Concurrency Contract (Single-Flight Invariant)==
	 * At most one asynchronous [[read]] operation may be in flight per [[AsyncReader]] instance at any given time.
	 * Initiating a new read before the previous completion callback has executed is strictly prohibited. Successive non-overlapping
	 * calls do not require single-thread affinity: because NIO.2 completion dispatch establishes a happens-before relationship,
	 * successive reads may safely be triggered from arbitrary worker threads (such as from within completion handler callbacks). */
	trait AsyncReader extends AutoCloseable {

		/** Reads a sequence of bytes from this reader session into the destination buffer.\
		 * Consumes bytes up to `dst.remaining()`, advancing the reader's internal position by the number of bytes read.
		 * @param dst the buffer into which bytes are transferred.
		 * @param attachment an arbitrary user attachment passed through to the completion handler; may be `null`.
		 * @param handler the callback invoked upon asynchronous read completion (`> 0` bytes read, `0` if destination was empty, or `-1` on EOF) or failure.
		 * @return the number of bytes read synchronously (`>= 0`), `-1` on immediate EOF, or [[ByteSource.AsyncReader.PENDING]] if completing asynchronously via [[handler]]. */
		def read[A](dst: ByteBuffer, attachment: A, handler: CompletionHandler[Integer, ? >: A]): Int

		/** Releases any file descriptors or channel resources allocated exclusively to this reader session.\
		 * Closing an individual reader session does not close or invalidate the parent [[ByteSource]] or other concurrent reader sessions. */
		override def close(): Unit = ()
	}

	object AsyncReader {
		/** Sentinel returned by [[AsyncReader.read]] indicating that read completion will be signaled asynchronously via the handler callback. */
		inline val PENDING: -2 = -2
	}
}

/** A [[ByteSource]] implementation backed by an on-disk file, supporting non-blocking [[AsynchronousFileChannel]] reads and zero-copy [[FileChannel.transferTo]].
 * @param path the file system path of the backing file.
 * @param knownSize the pre-computed size of the file, or `-1L` to query [[Files.size]] lazily.
 * @param deleteOnClose whether the backing file should be deleted upon calling [[close]]. */
final class FileByteSource(val path: Path, val knownSize: Long = -1L, val deleteOnClose: Boolean = false) extends ByteSource {

	private val cleanable: Maybe[java.lang.ref.Cleaner.Cleanable] = if deleteOnClose then {
		Maybe(FileByteSource.cleaner.register(this, new FileByteSource.DeletionTask(path)))
	} else Maybe.empty

	override lazy val size: Long = {
		if knownSize >= 0L then knownSize
		else Files.size(path)
	}

	override def openAsyncReader(): ByteSource.AsyncReader = new ByteSource.AsyncReader {
		private val channel = AsynchronousFileChannel.open(path, StandardOpenOption.READ)
		/** The position of the next byte to read.
		 * @note Assumes the concurrency invariant: Is thread-safe because callers must adhere to the single-flight contract (at most one outstanding read at any time).
		 * @note Memory visibility guaranteed because JLS §17.4.5 establishes a happens-before edge between write initiation and completion dispatch across NIO.2 worker threads. */
		private var position: Long = 0L

		override def read[A](dst: ByteBuffer, attachment: A, handler: CompletionHandler[Integer, ? >: A]): Int = {
			if !dst.hasRemaining then 0
			else {
				channel.read(dst, position, attachment, new CompletionHandler[Integer, A] {
					override def completed(result: Integer, att: A): Unit = {
						if result > 0 then position += result
						handler.completed(result, att)
					}
					override def failed(exc: Throwable, att: A): Unit = handler.failed(exc, att)
				})
				ByteSource.AsyncReader.PENDING
			}
		}

		/** @inheritdoc
		 * This implementation in particular closes the underlying file channel to release OS file handles. */
		override def close(): Unit = {
			// Read-only handle disposal: Suppress secondary close errors because all required bytes were already transferred and errors must not mask cancellation faults
			try channel.close() catch { case NonFatal(_) => () }
		}
	}

	/** @inheritdoc
	 * @throws java.io.IOException if an I/O error occurs opening the file for reading. */
	override def openChannel(): FileChannel = FileChannel.open(path, StandardOpenOption.READ)

	/** @inheritdoc
	 * @throws java.io.IOException if an I/O error occurs during channel transfer or if zero-copy transfer stalls. */
	override def transferTo(target: WritableByteChannel): Long = {
		val ch = openChannel()
		try {
			val total = ch.size()
			var pos = 0L
			while pos < total do {
				val transferred = ch.transferTo(pos, total - pos, target)
				if transferred <= 0 then throw new java.io.IOException(s"Zero-copy transfer stalled at position $pos of $total")
				pos += transferred
			}
			pos
		} finally {
			ch.close()
		}
	}

	/** @inheritdoc
	 * This implementation in particular unlinks the underlying file from the file system if configured with deleteOnClose. */
	override def close(): Unit = {
		cleanable.foreach(_.clean())
	}
}

object FileByteSource {
	private val cleaner: java.lang.ref.Cleaner = java.lang.ref.Cleaner.create()

	private final class DeletionTask(path: Path) extends Runnable {
		override def run(): Unit = {
			// Fallback unlinking: Suppress non-fatal deletion errors to avoid crashing Cleaner daemon thread or masking primary caller exceptions
			try Files.deleteIfExists(path) catch { case NonFatal(_) => () }
		}
	}
}

/** An in-memory [[ByteSource]] implementation backed by a [[ByteBuffer]].\
 * Multi-session extraction independence is achieved by duplicating the underlying buffer on each call, preserving independent buffer cursors.
 * @param buffer the backing buffer containing the source bytes between its current position and limit. */
final class BufferByteSource(val buffer: ByteBuffer) extends ByteSource {

	override def size: Long = buffer.remaining().toLong

	override def openAsyncReader(): ByteSource.AsyncReader = new ByteSource.AsyncReader {
		private val duplicate = buffer.duplicate()
		private var open = true

		override def read[A](dst: ByteBuffer, attachment: A, handler: CompletionHandler[Integer, ? >: A]): Int = {
			if !open then {
				val exc = new ClosedChannelException()
				handler.failed(exc, attachment)
				throw exc
			} else if !duplicate.hasRemaining then -1
			else {
				val toTransfer = math.min(dst.remaining(), duplicate.remaining())
				val savedLimit = duplicate.limit()
				duplicate.limit(duplicate.position() + toTransfer)
				dst.put(duplicate)
				duplicate.limit(savedLimit)
				toTransfer
			}
		}

		override def close(): Unit = {
			open = false
		}
	}

	override def openChannel(): ReadableByteChannel = new ReadableByteChannel {
		private val duplicate = buffer.duplicate()
		private var open = true

		override def read(dst: ByteBuffer): Int = {
			if !open then throw new ClosedChannelException()
			if !duplicate.hasRemaining then -1
			else {
				val toTransfer = math.min(dst.remaining(), duplicate.remaining())
				val savedLimit = duplicate.limit()
				duplicate.limit(duplicate.position() + toTransfer)
				dst.put(duplicate)
				duplicate.limit(savedLimit)
				toTransfer
			}
		}

		override def isOpen: Boolean = open
		override def close(): Unit = { open = false }
	}

	override def transferTo(target: WritableByteChannel): Long = {
		val duplicate = buffer.duplicate()
		val total = duplicate.remaining().toLong
		while duplicate.hasRemaining do {
			target.write(duplicate)
		}
		total
	}
}

object BufferByteSource {

	/** Creates a [[BufferByteSource]] wrapping the supplied byte array.
	 * @param bytes the byte array to wrap.
	 * @return a new [[BufferByteSource]] backed by a heap buffer. */
	def apply(bytes: Array[Byte]): BufferByteSource = new BufferByteSource(ByteBuffer.wrap(bytes))

	/** Creates a [[BufferByteSource]] wrapping the supplied immutable byte array.
	 * @param bytes the immutable byte array to wrap.
	 * @return a new [[BufferByteSource]] backed by a heap buffer. */
	@targetName("applyIArray")
	def apply(bytes: IArray[Byte]): BufferByteSource = new BufferByteSource(ByteBuffer.wrap(bytes.unsafeArray))
}
