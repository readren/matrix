package readren.common

import munit.FunSuite

import java.nio.ByteBuffer
import java.nio.channels.CompletionHandler
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.concurrent.{Await, Promise}
import scala.concurrent.duration.*

class ByteSinkTest extends FunSuite {

	test("BufferByteSink accumulates written buffers synchronously into a valid BufferByteSource") {
		val totalSize = 30
		val sink = new BufferByteSink(totalSize)
		assertEquals(sink.expectedSize, 30L)

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

	test("FileByteSink writes chunks asynchronously, seals on success, and discard removes file on failure") {
		val tempFile = Files.createTempFile("test-file-sink-", ".bin")
		val totalBytes = 24L
		val sink = new FileByteSink(tempFile, totalBytes)
		assertEquals(sink.expectedSize, 24L)

		val part1 = ByteBuffer.wrap("FirstChunk-8".getBytes(StandardCharsets.UTF_8))
		val part2 = ByteBuffer.wrap("SecondChunk-".getBytes(StandardCharsets.UTF_8))

		val p1 = Promise[Int]()
		sink.write(part1, null, new CompletionHandler[Integer, Null] {
			override def completed(res: Integer, att: Null): Unit = p1.success(res)
			override def failed(exc: Throwable, att: Null): Unit = p1.failure(exc)
		})
		assertEquals(Await.result(p1.future, 2.seconds), 12)

		val p2 = Promise[Int]()
		sink.write(part2, null, new CompletionHandler[Integer, Null] {
			override def completed(res: Integer, att: Null): Unit = p2.success(res)
			override def failed(exc: Throwable, att: Null): Unit = p2.failure(exc)
		})
		assertEquals(Await.result(p2.future, 2.seconds), 12)

		sink.seal()
		val source = sink.byteSource
		assertEquals(source.size, 24L)
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

	test("TC-11: FileByteSink.seal finalizes staging file and prevents further writes") {
		val tempFile = Files.createTempFile("test-seal-exc-sink-", ".bin")
		val sink = new FileByteSink(tempFile, 10L)
		sink.seal()
		val chunk = ByteBuffer.wrap("12345".getBytes(StandardCharsets.UTF_8))
		val p = Promise[Int]()
		sink.write(chunk, null, new CompletionHandler[Integer, Null] {
			override def completed(res: Integer, att: Null): Unit = p.success(res)
			override def failed(exc: Throwable, att: Null): Unit = p.failure(exc)
		})
		intercept[Throwable] {
			Await.result(p.future, 2.seconds)
		}
		sink.byteSource.close()
	}
}
