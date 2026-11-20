package readren.common

import munit.FunSuite

import java.nio.ByteBuffer
import java.nio.channels.{ClosedChannelException, CompletionHandler}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.concurrent.{Await, Promise}
import scala.concurrent.duration.*

class ByteSourceTest extends FunSuite {

	test("BufferByteSource openAsyncReader reads content sequentially and signals EOF synchronously") {
		val data = "Hello BufferByteSource AsyncReader".getBytes(StandardCharsets.UTF_8)
		val source = BufferByteSource(data)
		assertEquals(source.size, data.length.toLong)

		val reader = source.openAsyncReader()
		val dst = ByteBuffer.allocate(10)

		val firstRead = reader.read(dst, null, null)
		assertEquals(firstRead, 10)
		assertEquals(dst.position(), 10)

		val remainingDst = ByteBuffer.allocate(100)
		val secondRead = reader.read(remainingDst, null, null)
		assertEquals(secondRead, data.length - 10)

		val emptyDst = ByteBuffer.allocate(10)
		val eofResult = reader.read(emptyDst, null, null)
		assertEquals(eofResult, -1)

		reader.close()
	}

	test("BufferByteSource AsyncReader throws ClosedChannelException when read after close without invoking handler") {
		val source = BufferByteSource("closed-reader-test".getBytes(StandardCharsets.UTF_8))
		val reader = source.openAsyncReader()
		reader.close()

		var completedInvoked = false
		var failedInvoked = false
		val handler = new CompletionHandler[Integer, Null] {
			override def completed(result: Integer, attachment: Null): Unit = completedInvoked = true
			override def failed(exc: Throwable, attachment: Null): Unit = failedInvoked = true
		}

		intercept[ClosedChannelException] {
			reader.read(ByteBuffer.allocate(8), null, handler)
		}
		assert(!completedInvoked, "CompletionHandler.completed must not be invoked when throwing synchronously")
		assert(!failedInvoked, "CompletionHandler.failed must not be invoked when throwing ClosedChannelException synchronously")
	}

	test("FileByteSource openAsyncReader reads content accurately and deleteOnClose works") {
		val tempFile = Files.createTempFile("test-file-bytesource-", ".bin")
		val rawContent = "Asynchronous file reading with known size and lifecycle cleanup".getBytes(StandardCharsets.UTF_8)
		Files.write(tempFile, rawContent)

		val source = new FileByteSource(tempFile, knownSize = rawContent.length.toLong, deleteOnClose = true)
		assertEquals(source.size, rawContent.length.toLong)

		val reader = source.openAsyncReader()
		val dst = ByteBuffer.allocate(rawContent.length)
		val readPromise = Promise[Int]()
		reader.read(dst, null, new CompletionHandler[Integer, Null] {
			override def completed(result: Integer, att: Null): Unit = readPromise.success(result)
			override def failed(exc: Throwable, att: Null): Unit = readPromise.failure(exc)
		})
		val bytesRead = Await.result(readPromise.future, 2.seconds)
		assertEquals(bytesRead, rawContent.length)
		dst.flip()
		val readArray = new Array[Byte](dst.remaining())
		dst.get(readArray)
		assertEquals(new String(readArray, StandardCharsets.UTF_8), new String(rawContent, StandardCharsets.UTF_8))

		reader.close()
		source.close()
		assert(!Files.exists(tempFile), "deleteOnClose should delete the file upon source.close()")
	}

	test("FileByteSource AsyncReader throws ClosedChannelException when read after close without invoking handler") {
		val tempFile = Files.createTempFile("test-closed-file-source-", ".bin")
		Files.write(tempFile, "closed-file-reader".getBytes(StandardCharsets.UTF_8))
		val source = new FileByteSource(tempFile, deleteOnClose = true)
		val reader = source.openAsyncReader()
		reader.close()

		var completedInvoked = false
		var failedInvoked = false
		val handler = new CompletionHandler[Integer, Null] {
			override def completed(result: Integer, attachment: Null): Unit = completedInvoked = true
			override def failed(exc: Throwable, attachment: Null): Unit = failedInvoked = true
		}

		intercept[ClosedChannelException] {
			reader.read(ByteBuffer.allocate(8), null, handler)
		}
		assert(!completedInvoked, "CompletionHandler.completed must not be invoked when throwing synchronously")
		assert(!failedInvoked, "CompletionHandler.failed must not be invoked when throwing ClosedChannelException synchronously")
		source.close()
	}

	test("BufferByteSource supports repeatable independent readers without cursor interference") {
		val data = "Repeatable extraction independence".getBytes(StandardCharsets.UTF_8)
		val source = BufferByteSource(data)

		val r1 = source.openChannel()
		val r2 = source.openChannel()

		val b1 = ByteBuffer.allocate(10)
		val b2 = ByteBuffer.allocate(20)

		assertEquals(r1.read(b1), 10)
		assertEquals(r2.read(b2), 20)

		r1.close()
		r2.close()
	}

	test("BufferByteSource async reading is stack-safe over tens of thousands of chunks") {
		val totalBytes = 100000
		val raw = new Array[Byte](totalBytes)
		val source = BufferByteSource(raw)
		val reader = source.openAsyncReader()
		val chunkBuf = ByteBuffer.allocate(1)

		var readCount = 0
		var done = false

		while !done do {
			chunkBuf.clear()
			val res = reader.read(chunkBuf, null, null)
			if res == -1 then done = true
			else readCount += res
		}

		assertEquals(readCount, totalBytes)
		reader.close()
	}

	test("TC-13: FileByteSource deleteOnClose unlinks file on JVM garbage collection via Cleaner") {
		val tempFile = Files.createTempFile("test-cleaner-source-", ".bin")
		Files.write(tempFile, "cleaner-content".getBytes(StandardCharsets.UTF_8))
		assert(Files.exists(tempFile))

		def allocateAndDrop(): Unit = {
			val source = new FileByteSource(tempFile, knownSize = 15L, deleteOnClose = true)
			assert(source.size == 15L)
		}

		allocateAndDrop()

		var attempts = 0
		while Files.exists(tempFile) && attempts < 50 do {
			System.gc()
			System.runFinalization()
			Thread.sleep(50)
			attempts += 1
		}
		assert(!Files.exists(tempFile), "Cleaner should delete the temporary file after FileByteSource is garbage collected")
	}
}
