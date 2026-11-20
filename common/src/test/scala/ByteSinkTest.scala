package readren.common

import munit.FunSuite

import java.nio.ByteBuffer
import java.nio.channels.{ClosedChannelException, CompletionHandler}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.concurrent.{Await, Promise}
import scala.concurrent.duration.*

class ByteSinkTest extends FunSuite {

	test("BufferByteSink accumulates written buffers synchronously into a heap-backed BufferByteSource") {
		val totalSize = 30
		val sink = new BufferByteSink(totalSize)
		assertEquals(sink.expectedSize, 30L)
		assert(!sink.byteSource.buffer.isDirect)

		val chunk1 = ByteBuffer.wrap("1234567890".getBytes(StandardCharsets.UTF_8))
		val chunk2 = ByteBuffer.wrap("abcdefghij".getBytes(StandardCharsets.UTF_8))
		val chunk3 = ByteBuffer.wrap("ABCDEFGHIJ".getBytes(StandardCharsets.UTF_8))

		val w1 = sink.write(chunk1, null, null)
		assertEquals(w1, 10)

		val w2 = sink.write(chunk2, null, null)
		assertEquals(w2, 10)

		val w3 = sink.write(chunk3, null, null)
		assertEquals(w3, 10)

		sink.seal()

		val source = sink.byteSource
		assertEquals(source.size, 30L)
		val ch = source.openChannel()
		val readBuf = ByteBuffer.allocate(30)
		ch.read(readBuf)
		ch.close()
		readBuf.flip()
		assertEquals(new String(readBuf.array(), StandardCharsets.UTF_8), "1234567890abcdefghijABCDEFGHIJ")
	}

	test("BufferByteSink can wrap an existing ByteBuffer slice and derive expectedSize from remaining") {
		val backing = ByteBuffer.allocate(20)
		backing.position(4)
		backing.limit(14)
		val sink = new BufferByteSink(backing)
		assertEquals(sink.expectedSize, 10L)

		val chunk = ByteBuffer.wrap("0123456789".getBytes(StandardCharsets.UTF_8))
		assertEquals(sink.write(chunk, null, null), 10)
		sink.seal()

		val source = sink.byteSource
		assertEquals(source.size, 10L)
		val out = ByteBuffer.allocate(10)
		val ch = source.openChannel()
		ch.read(out)
		ch.close()
		out.flip()
		assertEquals(new String(out.array(), StandardCharsets.UTF_8), "0123456789")
	}

	test("FileByteSink writes chunks asynchronously, seals on success, and discard removes file on failure") {
		val tempFile = Files.createTempFile("test-file-sink-", ".bin")
		val totalBytes = 24L
		val sink = new FileByteSink(tempFile, totalBytes)
		assertEquals(sink.expectedSize, 24L)

		val part1 = ByteBuffer.wrap("FirstChunk-8".getBytes(StandardCharsets.UTF_8))
		val part2 = ByteBuffer.wrap("SecondChunk-".getBytes(StandardCharsets.UTF_8))

		val p1 = Promise[Int]()
		val res1 = sink.write(part1, null, new CompletionHandler[Integer, Null] {
			override def completed(res: Integer, att: Null): Unit = p1.success(res)
			override def failed(exc: Throwable, att: Null): Unit = p1.failure(exc)
		})
		val written1 = if res1 == ByteSink.PENDING then Await.result(p1.future, 2.seconds) else res1
		assertEquals(written1, 12)

		val p2 = Promise[Int]()
		val res2 = sink.write(part2, null, new CompletionHandler[Integer, Null] {
			override def completed(res: Integer, att: Null): Unit = p2.success(res)
			override def failed(exc: Throwable, att: Null): Unit = p2.failure(exc)
		})
		val written2 = if res2 == ByteSink.PENDING then Await.result(p2.future, 2.seconds) else res2
		assertEquals(written2, 12)

		sink.seal()
		val source = sink.byteSource
		assertEquals(source.size, 24L)
		val verifyBuf = ByteBuffer.allocate(24)
		val verifyCh = source.openChannel()
		while verifyBuf.hasRemaining && verifyCh.read(verifyBuf) >= 0 do ()
		verifyCh.close()
		verifyBuf.flip()
		assertEquals(StandardCharsets.UTF_8.decode(verifyBuf).toString, "FirstChunk-8SecondChunk-")
		source.close()

		val discardTemp = Files.createTempFile("test-discard-sink-", ".bin")
		val discardSink = new FileByteSink(discardTemp, 100L)
		assert(Files.exists(discardTemp))
		discardSink.discard()
		assert(!Files.exists(discardTemp), "discard should delete temporary sink file")
	}

	test("DefaultAllocator selects BufferByteSink for <= 64KB and FileByteSink for > 64KB") {
		val smallSink = ByteSink.DefaultAllocator.allocate(64 * 1024)
		assert(smallSink.isInstanceOf[BufferByteSink])

		val largeSink = ByteSink.DefaultAllocator.allocate(64 * 1024 + 1)
		assert(largeSink.isInstanceOf[FileByteSink])
		largeSink.discard()
	}

	test("TC-10: FileByteSink.write drains multi-fragment chunk buffer completely") {
		val tempFile = Files.createTempFile("test-partial-write-sink-", ".bin")
		val totalBytes = 100L
		val sink = new FileByteSink(tempFile, totalBytes)
		val rawData = ("A" * 50 + "B" * 50).getBytes(StandardCharsets.UTF_8)
		val chunk = ByteBuffer.wrap(rawData)

		val p = Promise[Int]()
		val writeRes = sink.write(chunk, null, new CompletionHandler[Integer, Null] {
			override def completed(res: Integer, att: Null): Unit = p.success(res)
			override def failed(exc: Throwable, att: Null): Unit = p.failure(exc)
		})
		assertEquals(writeRes, ByteSink.PENDING)
		val written = Await.result(p.future, 2.seconds)
		assertEquals(written, 100)
		assertEquals(chunk.remaining(), 0)

		sink.seal()
		val source = sink.byteSource
		assertEquals(source.size, 100L)
		val readBuf = ByteBuffer.allocate(100)
		val ch = source.openChannel()
		ch.read(readBuf)
		ch.close()
		source.close()
		assertEquals(readBuf.array().toSeq, rawData.toSeq)
	}

	test("TC-11: FileByteSink.seal finalizes staging file and rejects subsequent writes synchronously without invoking handler") {
		val tempFile = Files.createTempFile("test-seal-exc-sink-", ".bin")
		val sink = new FileByteSink(tempFile, 10L)
		sink.seal()
		val chunk = ByteBuffer.wrap("12345".getBytes(StandardCharsets.UTF_8))
		var completedInvoked = false
		var failedInvoked = false
		val handler = new CompletionHandler[Integer, Null] {
			override def completed(res: Integer, att: Null): Unit = completedInvoked = true
			override def failed(exc: Throwable, att: Null): Unit = failedInvoked = true
		}
		intercept[ClosedChannelException] {
			sink.write(chunk, null, handler)
		}
		assert(!completedInvoked, "CompletionHandler.completed must not be invoked when throwing ClosedChannelException synchronously")
		assert(!failedInvoked, "CompletionHandler.failed must not be invoked when throwing ClosedChannelException synchronously")
		sink.byteSource.close()
	}

	test("FileByteSink.discard and BufferByteSink.seal/discard reject subsequent writes synchronously without invoking handler") {
		var completedInvoked = false
		var failedInvoked = false
		val handler = new CompletionHandler[Integer, Null] {
			override def completed(res: Integer, att: Null): Unit = completedInvoked = true
			override def failed(exc: Throwable, att: Null): Unit = failedInvoked = true
		}

		val fileSink = new FileByteSink(Files.createTempFile("test-discard-exc-sink-", ".bin"), 10L)
		fileSink.discard()
		intercept[ClosedChannelException] {
			fileSink.write(ByteBuffer.wrap("123".getBytes(StandardCharsets.UTF_8)), null, handler)
		}

		val sealedBufSink = new BufferByteSink(10)
		sealedBufSink.seal()
		intercept[ClosedChannelException] {
			sealedBufSink.write(ByteBuffer.wrap("123".getBytes(StandardCharsets.UTF_8)), null, handler)
		}

		val discardedBufSink = new BufferByteSink(10)
		discardedBufSink.discard()
		intercept[ClosedChannelException] {
			discardedBufSink.write(ByteBuffer.wrap("123".getBytes(StandardCharsets.UTF_8)), null, handler)
		}

		assert(!completedInvoked)
		assert(!failedInvoked)
	}

	private def writeSyncOrAsync(sink: ByteSink, chunk: ByteBuffer): Int = {
		val p = Promise[Int]()
		val status = sink.write(chunk, null, new CompletionHandler[Integer, Null] {
			override def completed(res: Integer, att: Null): Unit = p.success(res)
			override def failed(exc: Throwable, att: Null): Unit = p.failure(exc)
		})
		if status == ByteSink.PENDING then Await.result(p.future, 2.seconds) else {
			assert(!p.isCompleted)
			status
		}
	}

	test("O4: FileByteSink coalesces small writes synchronously, splits cross-block writes, and bypasses full blocks") {
		val tempFile = Files.createTempFile("test-coalesce-sink-", ".bin")
		val payload = "0123456789abcdefghijklmnopqrstuvwxyzABCD".getBytes(StandardCharsets.UTF_8) // 40 bytes
		val sink = new FileByteSink(tempFile, payload.length.toLong, blockBufferCapacity = 16)

		// Fragment 1: 5 bytes (< 16) -> buffered synchronously.
		val p1 = Promise[Int]()
		assertEquals(sink.write(ByteBuffer.wrap(payload, 0, 5), null, new CompletionHandler[Integer, Null] {
			override def completed(res: Integer, att: Null): Unit = p1.success(res)
			override def failed(exc: Throwable, att: Null): Unit = p1.failure(exc)
		}), 5)
		assert(!p1.isCompleted)

		// Fragment 2: 7 bytes (5 + 7 = 12 < 16) -> buffered synchronously.
		assertEquals(writeSyncOrAsync(sink, ByteBuffer.wrap(payload, 5, 7)), 7)

		// Fragment 3: 10 bytes (12 + 10 = 22 > 16) -> flushes 16-byte block async and stages remaining 6 bytes.
		val p3 = Promise[Int]()
		val s3 = sink.write(ByteBuffer.wrap(payload, 12, 10), null, new CompletionHandler[Integer, Null] {
			override def completed(res: Integer, att: Null): Unit = p3.success(res)
			override def failed(exc: Throwable, att: Null): Unit = p3.failure(exc)
		})
		assertEquals(s3, ByteSink.PENDING)
		assertEquals(Await.result(p3.future, 2.seconds), 10)

		// Fragment 4: 6 bytes (6 + 6 = 12 < 16, 16 + 12 = 28 < 40) -> buffered synchronously.
		assertEquals(writeSyncOrAsync(sink, ByteBuffer.wrap(payload, 22, 6)), 6)

		// Fragment 5: 12 bytes (reaches expectedSize = 40) -> flushes remaining 24 bytes asynchronously.
		assertEquals(writeSyncOrAsync(sink, ByteBuffer.wrap(payload, 28, 12)), 12)

		sink.seal()
		val source = sink.byteSource
		val readBack = Files.readAllBytes(tempFile)
		assertEquals(readBack.toSeq, payload.toSeq)
		source.close()
		assert(!Files.exists(tempFile))
	}

	test("O4: FileByteSink seal flushes buffered tail even if written bytes are below expectedSize") {
		val tempFile = Files.createTempFile("test-seal-tail-sink-", ".bin")
		val sink = new FileByteSink(tempFile, expectedSize = 100L, blockBufferCapacity = 64)
		val tail = "partial-tail-bytes".getBytes(StandardCharsets.UTF_8)
		assertEquals(writeSyncOrAsync(sink, ByteBuffer.wrap(tail)), tail.length)
		sink.seal()
		assertEquals(Files.readAllBytes(tempFile).toSeq, tail.toSeq)
		sink.byteSource.close()
	}

	test("O4: FileByteSink bypasses staging buffer when incoming write covers blockBufferCapacity") {
		val tempFile = Files.createTempFile("test-bypass-sink-", ".bin")
		val payload = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".getBytes(StandardCharsets.UTF_8) // 36 bytes
		val sink = new FileByteSink(tempFile, expectedSize = payload.length.toLong, blockBufferCapacity = 16)

		val p1 = Promise[Int]()
		val s1 = sink.write(ByteBuffer.wrap(payload, 0, 20), null, new CompletionHandler[Integer, Null] {
			override def completed(res: Integer, att: Null): Unit = p1.success(res)
			override def failed(exc: Throwable, att: Null): Unit = p1.failure(exc)
		})
		assertEquals(s1, ByteSink.PENDING)
		assertEquals(Await.result(p1.future, 2.seconds), 20)

		assertEquals(writeSyncOrAsync(sink, ByteBuffer.wrap(payload, 20, 16)), 16)
		sink.seal()
		assertEquals(Files.readAllBytes(tempFile).toSeq, payload.toSeq)
		sink.byteSource.close()
	}
}
