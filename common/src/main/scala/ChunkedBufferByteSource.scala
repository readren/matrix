package readren.common

import java.nio.ByteBuffer
import java.nio.channels.{ClosedChannelException, CompletionHandler, ReadableByteChannel, WritableByteChannel}

/** An immutable, in-memory [[ByteSource]] implementation backed by a sequence of heap or direct [[ByteBuffer]] chunks.
 * Multi-session extraction independence is achieved by duplicating the underlying chunk buffers on each session creation, preserving independent cursors across concurrent readers and allowing total stream sizes exceeding [[Int.MaxValue]] bytes.
 * @param chunks the immutable sequence of buffers containing the source bytes between each buffer's current position and limit.
 * @param totalSize the exact total byte count across all [[chunks]].*/
final class ChunkedBufferByteSource(val chunks: IArray[ByteBuffer], val totalSize: Long) extends ByteSource {

	override def size: Long = totalSize

	override def openAsyncReader(): ByteSource.AsyncReader = new ByteSource.AsyncReader {
		private val duplicatedChunks: Array[ByteBuffer] = Array.tabulate(chunks.length)(index => chunks(index).duplicate())
		private var currentChunkIndex: Int = 0
		private var isOpen: Boolean = true

		override def read[A](dst: ByteBuffer, attachment: A, handler: CompletionHandler[Integer, ? >: A]): Int = {
			if !isOpen then throw new ClosedChannelException()
			else if !dst.hasRemaining then 0
			else {
				var totalBytesCopied = 0
				while dst.hasRemaining && currentChunkIndex < duplicatedChunks.length do {
					val chunk = duplicatedChunks(currentChunkIndex)
					if chunk.hasRemaining then {
						val toTransfer = math.min(dst.remaining(), chunk.remaining())
						val savedLimit = chunk.limit()
						chunk.limit(chunk.position() + toTransfer)
						dst.put(chunk)
						chunk.limit(savedLimit)
						totalBytesCopied += toTransfer
					}
					if !chunk.hasRemaining then currentChunkIndex += 1
				}
				if totalBytesCopied > 0 then totalBytesCopied else -1
			}
		}

		override def close(): Unit = {
			isOpen = false
		}
	}

	override def openChannel(): ReadableByteChannel = new ReadableByteChannel {
		private val duplicatedChunks: Array[ByteBuffer] = Array.tabulate(chunks.length)(index => chunks(index).duplicate())
		private var currentChunkIndex: Int = 0
		private var openState: Boolean = true

		override def read(dst: ByteBuffer): Int = {
			if !openState then throw new ClosedChannelException()
			else if !dst.hasRemaining then 0
			else {
				var totalBytesCopied = 0
				while dst.hasRemaining && currentChunkIndex < duplicatedChunks.length do {
					val chunk = duplicatedChunks(currentChunkIndex)
					if chunk.hasRemaining then {
						val toTransfer = math.min(dst.remaining(), chunk.remaining())
						val savedLimit = chunk.limit()
						chunk.limit(chunk.position() + toTransfer)
						dst.put(chunk)
						chunk.limit(savedLimit)
						totalBytesCopied += toTransfer
					}
					if !chunk.hasRemaining then currentChunkIndex += 1
				}
				if totalBytesCopied > 0 then totalBytesCopied else -1
			}
		}

		override def isOpen: Boolean = openState

		override def close(): Unit = {
			openState = false
		}
	}

	override def transferTo(target: WritableByteChannel): Long = {
		var transferredTotal = 0L
		var index = 0
		while index < chunks.length do {
			val duplicate = chunks(index).duplicate()
			while duplicate.hasRemaining do {
				transferredTotal += target.write(duplicate)
			}
			index += 1
		}
		transferredTotal
	}
}

object ChunkedBufferByteSource {

	/** Constructs a [[ChunkedBufferByteSource]] by summing the remaining bytes across all provided `chunks`.
	 * @param chunks the immutable array of buffers holding the source bytes.
	 * @return a new [[ChunkedBufferByteSource]] instance.*/
	def apply(chunks: IArray[ByteBuffer]): ChunkedBufferByteSource = {
		var sum = 0L
		var index = 0
		while index < chunks.length do {
			sum += chunks(index).remaining().toLong
			index += 1
		}
		new ChunkedBufferByteSource(chunks, sum)
	}
}
