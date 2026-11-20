package readren.common

import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousFileChannel, ClosedChannelException, CompletionHandler}
import java.nio.file.{Files, Path, StandardOpenOption}
import java.util.concurrent.ExecutionException
import scala.util.control.NonFatal

/** A [[ByteSink]] implementation backed by an on-disk temporary file via [[AsynchronousFileChannel]].\
 * Small fragments below [[blockBufferCapacity]] are coalesced synchronously in a lazily allocated heap staging buffer until a full block is accumulated, [[expectedSize]] is reached, or [[seal]] is called; full-block and stream-completing writes bypass staging directly with zero copy.\
 * Upon [[seal]], any remaining staged tail is flushed and the write channel is closed so the projected [[FileByteSource]] (configured with `deleteOnClose = true`) becomes readable.\
 * Upon [[discard]], the write channel is closed and the temporary file is deleted immediately.
 * @param path the file system path of the staging file.
 * @param expectedSize the total number of bytes expected to be written.
 * @param blockBufferCapacity the staging block size in bytes used to coalesce small writes before flushing to disk (default 64 KiB; `0` disables buffering). */
final class FileByteSink(
	val path: Path,
	override val expectedSize: Long,
	val blockBufferCapacity: Int = FileByteSink.DefaultBlockBufferCapacity
) extends ByteSink {
	require(blockBufferCapacity >= 0, s"blockBufferCapacity must be non-negative, got $blockBufferCapacity")

	private val channel = AsynchronousFileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
	/** The file offset of the next byte to flush to [[channel]].
	 * @note Assumes the concurrency invariant: Is thread-safe because callers must adhere to the single-flight contract (at most one outstanding write at any time).
	 * @note Memory visibility guaranteed because JLS §17.4.5 establishes a happens-before edge between write initiation and completion dispatch across NIO.2 worker threads. */
	private var position: Long = 0L

	/** Effective staging block capacity capped to [[expectedSize]] when positive so small-file test configurations avoid over-allocating. */
	private val effectiveBlockCapacity: Int =
		if blockBufferCapacity == 0 then 0
		else if expectedSize > 0L then math.min(blockBufferCapacity.toLong, expectedSize).toInt
		else blockBufferCapacity

	/** Lazily allocated heap buffer used to coalesce sub-block writes before issuing [[AsynchronousFileChannel.write]]. */
	private var stagingBuffer: ByteBuffer | Null = null
	@volatile private var sealedOrDiscarded: Boolean = false

	// Hoisted single-flight asynchronous write state eliminating per-write CompletionHandler allocations.
	private var activePhase: Int = FileByteSink.PhaseIdle
	private var activeSrc: ByteBuffer | Null = null
	private var activeConsumed: Int = 0
	private var activeAttachment: Any = null
	private var activeHandler: CompletionHandler[Integer, Any] | Null = null

	override val byteSource: FileByteSource = new FileByteSource(path, knownSize = expectedSize, deleteOnClose = true)

	override def write[A](src: ByteBuffer, attachment: A, handler: CompletionHandler[Integer, ? >: A]): Int = {
		if sealedOrDiscarded || !channel.isOpen then throw new ClosedChannelException()
		val bytesToWrite = src.remaining()
		if bytesToWrite == 0 then 0
		else {
			val currentBuf = stagingBuffer
			val bufferedBytes = if currentBuf ne null then currentBuf.position() else 0
			if bufferedBytes == 0 && (effectiveBlockCapacity == 0 || bytesToWrite >= effectiveBlockCapacity || position + bytesToWrite.toLong >= expectedSize) then {
				// Zero-copy bypass: no buffered bytes exist and the incoming buffer fills at least one block or finishes the expected stream.
				startAsyncWrite(FileByteSink.PhaseDirectWrite, src, src, bytesToWrite, attachment, handler)
				ByteSink.PENDING
			} else {
				val buf = ensureStagingBuffer()
				val stagingRemaining = buf.remaining()
				if bytesToWrite < stagingRemaining && (position + bufferedBytes.toLong + bytesToWrite.toLong) < expectedSize then {
					// Coalesce sub-block fragment synchronously in memory without issuing a disk write or allocating a completion handler.
					buf.put(src)
					bytesToWrite
				} else if bytesToWrite <= stagingRemaining then {
					buf.put(src)
					buf.flip()
					startAsyncWrite(FileByteSink.PhaseFlushStagingOnly, buf, null, bytesToWrite, attachment, handler)
					ByteSink.PENDING
				} else {
					val savedLimit = src.limit()
					src.limit(src.position() + stagingRemaining)
					buf.put(src)
					src.limit(savedLimit)
					buf.flip()
					startAsyncWrite(FileByteSink.PhaseFlushStagingThenSrc, buf, src, bytesToWrite, attachment, handler)
					ByteSink.PENDING
				}
			}
		}
	}

	private def ensureStagingBuffer(): ByteBuffer = {
		val existing = stagingBuffer
		if existing ne null then existing
		else {
			val allocated = ByteBuffer.allocate(effectiveBlockCapacity)
			stagingBuffer = allocated
			allocated
		}
	}

	private def startAsyncWrite[A](
		phase: Int,
		bufferToWrite: ByteBuffer,
		remainingSrc: ByteBuffer | Null,
		consumed: Int,
		attachment: A,
		handler: CompletionHandler[Integer, ? >: A]
	): Unit = {
		activePhase = phase
		activeSrc = remainingSrc
		activeConsumed = consumed
		activeAttachment = attachment
		activeHandler = handler.asInstanceOf[CompletionHandler[Integer, Any]]
		try {
			channel.write(bufferToWrite, position, null, internalWriteHandler)
		} catch {
			case NonFatal(e) =>
				// Propagate entry-time synchronous failures directly without invoking CompletionHandler inline before returning ByteSink.PENDING.
				clearActiveWriteState()
				throw e
		}
	}

	private def submitContinuationWrite(bufferToWrite: ByteBuffer): Unit = {
		try {
			channel.write(bufferToWrite, position, null, internalWriteHandler)
		} catch {
			case NonFatal(e) =>
				val savedHandler = activeHandler
				val savedAttachment = activeAttachment
				clearActiveWriteState()
				if savedHandler ne null then savedHandler.failed(e, savedAttachment)
		}
	}

	private def finishActiveWrite(): Unit = {
		val savedHandler = activeHandler
		val savedAttachment = activeAttachment
		val savedConsumed = activeConsumed
		clearActiveWriteState()
		if savedHandler ne null then savedHandler.completed(Integer.valueOf(savedConsumed), savedAttachment)
	}

	private def clearActiveWriteState(): Unit = {
		activePhase = FileByteSink.PhaseIdle
		activeSrc = null
		activeConsumed = 0
		activeAttachment = null
		activeHandler = null
	}

	private object internalWriteHandler extends CompletionHandler[Integer, Null] {
		override def completed(result: Integer, att: Null): Unit = {
			val written = if result != null && result.intValue() > 0 then result.intValue() else 0
			if written > 0 then position += written
			(activePhase: @scala.annotation.switch) match {
				case FileByteSink.PhaseDirectWrite =>
					val src = activeSrc.nn
					if src.hasRemaining then submitContinuationWrite(src)
					else finishActiveWrite()

				case FileByteSink.PhaseFlushStagingOnly =>
					val buf = stagingBuffer.nn
					if buf.hasRemaining then submitContinuationWrite(buf)
					else {
						buf.clear()
						if position >= expectedSize then stagingBuffer = null
						finishActiveWrite()
					}

				case FileByteSink.PhaseFlushStagingThenSrc =>
					val buf = stagingBuffer.nn
					if buf.hasRemaining then submitContinuationWrite(buf)
					else {
						buf.clear()
						val src = activeSrc.nn
						val remaining = src.remaining()
						if remaining == 0 then {
							if position >= expectedSize then stagingBuffer = null
							finishActiveWrite()
						} else if effectiveBlockCapacity == 0 || remaining >= effectiveBlockCapacity || position + remaining.toLong >= expectedSize then {
							activePhase = FileByteSink.PhaseDirectWrite
							if position + remaining.toLong >= expectedSize then stagingBuffer = null
							submitContinuationWrite(src)
						} else {
							buf.put(src)
							finishActiveWrite()
						}
					}

				case _ => ()
			}
		}

		override def failed(exc: Throwable, att: Null): Unit = {
			val savedHandler = activeHandler
			val savedAttachment = activeAttachment
			clearActiveWriteState()
			if savedHandler ne null then savedHandler.failed(exc, savedAttachment)
		}
	}

	/** @inheritdoc
	 * This implementation flushes any buffered tail bytes and closes the write channel, finalizing the staging file so the projected [[FileByteSource]] can be read.
	 * @throws java.io.IOException if flushing staged bytes or closing the underlying [[java.nio.channels.AsynchronousFileChannel]] fails during finalization. */
	override def seal(): Unit = {
		sealedOrDiscarded = true
		try {
			val buf = stagingBuffer
			if buf ne null then {
				stagingBuffer = null
				if buf.position() > 0 then {
					buf.flip()
					while buf.hasRemaining do {
						val written = try {
							channel.write(buf, position).get()
						} catch {
							case ee: ExecutionException if ee.getCause ne null => throw ee.getCause
						}
						if written <= 0 then throw new java.io.IOException(s"FileByteSink failed to flush staged tail at position $position")
						position += written
					}
				}
			}
		} finally {
			clearActiveWriteState()
			channel.close()
		}
	}

	/** @inheritdoc
	 * This implementation in particular closes the channel and immediately unlinks the staging file from disk to prevent orphaned files on aborted or corrupted transfers. */
	override def discard(): Unit = {
		sealedOrDiscarded = true
		stagingBuffer = null
		clearActiveWriteState()
		// Rollback teardown: Suppress secondary close errors so scratch file unlinking can proceed and primary abort faults are not masked
		try channel.close() catch {
			case NonFatal(_) => ()
		}
		// Best-effort unlinking: Suppress non-fatal deletion errors (e.g. file locks) to prevent aborting caller cleanup loops
		try Files.deleteIfExists(path) catch {
			case NonFatal(_) => ()
		}
	}
}

object FileByteSink {
	/** Default staging block capacity (64 KiB) used to coalesce small writes before flushing to [[AsynchronousFileChannel]]. */
	inline val DefaultBlockBufferCapacity = 65536

	private inline val PhaseIdle = 0
	private inline val PhaseDirectWrite = 1
	private inline val PhaseFlushStagingOnly = 2
	private inline val PhaseFlushStagingThenSrc = 3
}