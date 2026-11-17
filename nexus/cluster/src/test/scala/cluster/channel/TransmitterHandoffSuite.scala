package readren.nexus
package cluster.channel

import cluster.misc.VLQ
import cluster.serialization.Serializer.SerializationException
import cluster.serialization.{ProtocolVersion, Serializer}
import cluster.service.{ChannelId, ChannelOrigin}

import munit.FunSuite
import readren.common.Maybe

import java.io.{ByteArrayOutputStream, IOException}
import java.net.{InetSocketAddress, SocketAddress, SocketOption}
import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousSocketChannel, CompletionHandler, ReadableByteChannel}
import java.util
import java.util.Collections
import java.util.concurrent.*
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class TransmitterHandoffSuite extends FunSuite {

	private enum WriteMode {
		/** Captures written bytes and notifies completion synchronously on the calling thread. */
		case InlineComplete(maxBytesPerWrite: Int = Int.MaxValue)
		/** Captures written bytes and queues completion until manually pumped by the test. */
		case DeferredComplete(maxBytesPerWrite: Int = Int.MaxValue)
		/** Throws the given exception synchronously when `channel.write` is called. */
		case SyncThrow(cause: Throwable)
		/** Invokes `CompletionHandler.failed` inline inside `channel.write`. */
		case InlineFailed(cause: Throwable)
	}

	private final class PendingWrite(
		val bytesWritten: Long,
		val attachment: Any,
		val handler: CompletionHandler[java.lang.Long, Any]
	) {
		def completeSuccess(): Unit = handler.completed(java.lang.Long.valueOf(bytesWritten), attachment)
		def completeFailure(cause: Throwable): Unit = handler.failed(cause, attachment)
	}

	private abstract class BaseMockAsyncSocketChannel extends AsynchronousSocketChannel(null) {
		protected val captured = new ByteArrayOutputStream()

		def writtenBytes: Array[Byte] = synchronized(captured.toByteArray)

		protected final def consumeAndCapture(srcs: Array[ByteBuffer], offset: Int, length: Int, limit: Int): Long = {
			var total = 0L
			var remaining = limit
			var i = offset
			val end = offset + length
			while i < end && remaining > 0 do {
				val buf = srcs(i)
				val toRead = math.min(buf.remaining(), remaining)
				if toRead > 0 then {
					val tmp = new Array[Byte](toRead)
					buf.get(tmp)
					synchronized(captured.write(tmp))
					total += toRead
					remaining -= toRead
				}
				i += 1
			}
			total
		}

		override def bind(local: SocketAddress): AsynchronousSocketChannel = this
		override def setOption[T](name: SocketOption[T], value: T): AsynchronousSocketChannel = this
		override def shutdownInput(): AsynchronousSocketChannel = this
		override def shutdownOutput(): AsynchronousSocketChannel = this
		override def getRemoteAddress: SocketAddress = new InetSocketAddress("127.0.0.1", 9002)
		override def getLocalAddress: SocketAddress = new InetSocketAddress("127.0.0.1", 9001)
		override def close(): Unit = ()
		override def isOpen: Boolean = true
		override def getOption[T](name: SocketOption[T]): T = throw new UnsupportedOperationException()
		override def supportedOptions(): util.Set[SocketOption[?]] = Collections.emptySet()
		override def connect[A](remote: SocketAddress, attachment: A, handler: CompletionHandler[Void, ? >: A]): Unit = throw new UnsupportedOperationException()
		override def connect(remote: SocketAddress): Future[Void] = throw new UnsupportedOperationException()
		override def read[A](dst: ByteBuffer, timeout: Long, unit: TimeUnit, attachment: A, handler: CompletionHandler[Integer, ? >: A]): Unit = throw new UnsupportedOperationException()
		override def read(dst: ByteBuffer): Future[Integer] = throw new UnsupportedOperationException()
		override def read[A](dsts: Array[ByteBuffer], offset: Int, length: Int, timeout: Long, unit: TimeUnit, attachment: A, handler: CompletionHandler[java.lang.Long, ? >: A]): Unit = throw new UnsupportedOperationException()
		override def write[A](src: ByteBuffer, timeout: Long, unit: TimeUnit, attachment: A, handler: CompletionHandler[Integer, ? >: A]): Unit = throw new UnsupportedOperationException()
		override def write(src: ByteBuffer): Future[Integer] = throw new UnsupportedOperationException()
	}

	private final class ControlledAsyncChannel(scriptedModes: Int => WriteMode = _ => WriteMode.InlineComplete())
		extends BaseMockAsyncSocketChannel {

		private val writeCounter = new AtomicInteger(0)
		private val pendingWrites = new ConcurrentLinkedQueue[PendingWrite]()

		def writeInvocationCount: Int = writeCounter.get()
		def pollPending(): PendingWrite | Null = pendingWrites.poll()

		def drainPending(): Int = {
			var count = 0
			var next = pendingWrites.poll()
			while next ne null do {
				next.completeSuccess()
				count += 1
				next = pendingWrites.poll()
			}
			count
		}

		override def write[A](
			srcs: Array[ByteBuffer],
			offset: Int,
			length: Int,
			timeout: Long,
			unit: TimeUnit,
			attachment: A,
			handler: CompletionHandler[java.lang.Long, ? >: A]
		): Unit = {
			val idx = writeCounter.getAndIncrement()
			val mode = scriptedModes(idx)
			val anyHandler = handler.asInstanceOf[CompletionHandler[java.lang.Long, Any]]

			mode match {
				case WriteMode.SyncThrow(cause) =>
					throw cause
				case WriteMode.InlineFailed(cause) =>
					anyHandler.failed(cause, attachment)
				case WriteMode.InlineComplete(maxBytes) =>
					val written = consumeAndCapture(srcs, offset, length, maxBytes)
					anyHandler.completed(java.lang.Long.valueOf(written), attachment)
				case WriteMode.DeferredComplete(maxBytes) =>
					val written = consumeAndCapture(srcs, offset, length, maxBytes)
					pendingWrites.add(new PendingWrite(written, attachment, anyHandler))
			}
		}
	}

	private def newContext: Transmitter.Context = new Transmitter.Context {
		private val addr = new InetSocketAddress("127.0.0.1", 9001)
		override def myAddress: SocketAddress = addr
		override def oPeerAddress: Maybe[SocketAddress] = Maybe.some(new InetSocketAddress("127.0.0.1", 9002))
		override def channelId: ChannelId = new ChannelId(ChannelOrigin.INITIATED, Maybe.some(addr))
	}

	private case class DecodedPackage(payload: Array[Byte], frameCount: Int, corrupted: Boolean, trailingStream: Boolean)

	private def decodeWirePackage(wire: Array[Byte]): DecodedPackage = {
		var pos = 0
		val reader: VLQ.ByteReader = () => {
			val b = wire(pos)
			pos += 1
			b
		}
		val payload = new ByteArrayOutputStream()
		var frameCount = 0
		var corrupted = false
		var trailingStream = false
		var done = false
		while !done do {
			val frameLen = Integer.toUnsignedLong(VLQ.decodeUnsignedInt(reader))
			if frameLen == 0L then done = true
			else if frameLen == Transmitter.CORRUPTED_PACKAGE_SENTINEL then {
				corrupted = true
				done = true
			} else if frameLen == Transmitter.STREAM_FOLLOWS_SENTINEL then {
				trailingStream = true
				done = true
			} else {
				val n = frameLen.toInt
				payload.write(wire, pos, n)
				pos += n
				frameCount += 1
			}
		}
		assertEquals(pos, wire.length, "Unexpected trailing wire bytes after package terminator")
		DecodedPackage(payload.toByteArray, frameCount, corrupted, trailingStream)
	}

	private def sequentialPatternBytes(length: Int): Array[Byte] = {
		val arr = new Array[Byte](length)
		var i = 0
		while i < length do {
			arr(i) = ((i * 31 + 7) & 0xff).toByte
			i += 1
		}
		arr
	}

	test("constructor validates maxBuffers, aggregate capacity, streamTransferBufferCapacity, and maxGatheringBuffers") {
		val channel = new ControlledAsyncChannel()
		intercept[IllegalArgumentException] {
			new Transmitter(channel, newContext, buffersCapacity = 16, maxBuffers = 0)
		}
		intercept[IllegalArgumentException] {
			new Transmitter(channel, newContext, buffersCapacity = 1 << 20, maxBuffers = 4096)
		}
		intercept[IllegalArgumentException] {
			new Transmitter(channel, newContext, buffersCapacity = 16, maxBuffers = 4, streamTransferBufferCapacity = 0)
		}
		intercept[IllegalArgumentException] {
			new Transmitter(channel, newContext, buffersCapacity = 16, maxBuffers = 4, maxGatheringBuffers = 1)
		}
	}

	test("multi-buffer control package is merged into a single frame and drained across partial deferred gathering writes") {
		val channel = new ControlledAsyncChannel(_ => WriteMode.DeferredComplete(maxBytesPerWrite = 7))
		val transmitter = new Transmitter(channel, newContext, buffersCapacity = 16, maxBuffers = 8, maxGatheringBuffers = 3)
		val payload = sequentialPatternBytes(48)
		val reports = new ArrayBuffer[Transmitter.Report]()

		given Serializer[Array[Byte]] = (msg, writer) => {
			writer.putBytes(msg)
		}

		transmitter.transmit(payload, ProtocolVersion.OF_THIS_PROJECT)(report => reports += report)
		assertEquals(reports.size, 0, "onComplete must not run while gathering writes are still deferred")

		val steps = channel.drainPending()
		assert(steps >= 3, s"Expected multiple async write steps, got $steps")
		assertEquals(reports.toList, List(Transmitter.Delivered))

		val decoded = decodeWirePackage(channel.writtenBytes)
		assertEquals(decoded.frameCount, 1)
		assert(!decoded.corrupted)
		assert(!decoded.trailingStream)
		assertEquals(decoded.payload.toSeq, payload.toSeq)
	}

	test("standalone sentinel spills to next gathering batch when maxGatheringBuffers is saturated") {
		val channel = new ControlledAsyncChannel(_ => WriteMode.InlineComplete())
		val transmitter = new Transmitter(channel, newContext, buffersCapacity = 16, maxBuffers = 8, maxGatheringBuffers = 2)
		val payload = sequentialPatternBytes(48)
		val reports = new ArrayBuffer[Transmitter.Report]()

		given Serializer[Array[Byte]] = (msg, writer) => {
			writer.putBytes(msg)
		}

		transmitter.transmit(payload, ProtocolVersion.OF_THIS_PROJECT)(report => reports += report)
		assertEquals(reports.toList, List(Transmitter.Delivered))
		assertEquals(channel.writeInvocationCount, 3, "Expected [header, buf0], [buf1, buf2], [sentinel] across 3 gathering calls")

		val decoded = decodeWirePackage(channel.writtenBytes)
		assertEquals(decoded.frameCount, 1)
		assert(!decoded.corrupted)
		assert(!decoded.trailingStream)
		assertEquals(decoded.payload.toSeq, payload.toSeq)
	}

	test("zero-byte control package omits frame header and writes only the 0x00 sentinel") {
		val channel = new ControlledAsyncChannel(_ => WriteMode.InlineComplete())
		val transmitter = new Transmitter(channel, newContext, buffersCapacity = 16, maxBuffers = 4)
		val reports = new ArrayBuffer[Transmitter.Report]()

		given Serializer[Unit] = (_, _) => ()

		transmitter.transmit((), ProtocolVersion.OF_THIS_PROJECT)(report => reports += report)
		assertEquals(reports.toList, List(Transmitter.Delivered))
		assertEquals(channel.writtenBytes.toSeq, Seq(0.toByte))

		val decoded = decodeWirePackage(channel.writtenBytes)
		assertEquals(decoded.frameCount, 0)
		assert(!decoded.corrupted)
		assert(!decoded.trailingStream)
		assertEquals(decoded.payload.length, 0)
	}

	test("synchronous channel.write exception reports TransmissionFailure once and preserves reusability") {
		val syncBoom = new IOException("sync write boom")
		val channel = new ControlledAsyncChannel(idx =>
			if idx == 0 then WriteMode.SyncThrow(syncBoom) else WriteMode.InlineComplete()
		)
		val transmitter = new Transmitter(channel, newContext, buffersCapacity = 16, maxBuffers = 4)
		val reports = new ArrayBuffer[Transmitter.Report]()

		given Serializer[Array[Byte]] = (msg, writer) => writer.putBytes(msg)

		val firstPayload = sequentialPatternBytes(24)
		transmitter.transmit(firstPayload, ProtocolVersion.OF_THIS_PROJECT)(report => reports += report)
		assertEquals(reports.toList, List(Transmitter.TransmissionFailure(firstPayload, syncBoom)))

		val secondPayload = sequentialPatternBytes(10)
		val nextReports = new ArrayBuffer[Transmitter.Report]()
		transmitter.transmit(secondPayload, ProtocolVersion.OF_THIS_PROJECT)(report => nextReports += report)
		assertEquals(nextReports.toList, List(Transmitter.Delivered))

		val decoded = decodeWirePackage(channel.writtenBytes)
		assertEquals(decoded.frameCount, 1)
		assertEquals(decoded.payload.toSeq, secondPayload.toSeq)
	}

	test("asynchronous channel write failure reports TransmissionFailure once and resets storage for reuse") {
		val ioBoom = new IOException("async socket broken")
		val channel = new ControlledAsyncChannel(idx =>
			if idx == 0 then WriteMode.InlineFailed(ioBoom) else WriteMode.InlineComplete()
		)
		val transmitter = new Transmitter(channel, newContext, buffersCapacity = 16, maxBuffers = 8)
		val reports = new ArrayBuffer[Transmitter.Report]()

		given Serializer[Array[Byte]] = (msg, writer) => writer.putBytes(msg)

		val firstPayload = sequentialPatternBytes(30)
		transmitter.transmit(firstPayload, ProtocolVersion.OF_THIS_PROJECT)(report => reports += report)
		assertEquals(reports.toList, List(Transmitter.TransmissionFailure(firstPayload, ioBoom)))
		assertEquals(channel.writtenBytes.length, 0)

		val secondPayload = sequentialPatternBytes(12)
		val nextReports = new ArrayBuffer[Transmitter.Report]()
		transmitter.transmit(secondPayload, ProtocolVersion.OF_THIS_PROJECT)(report => nextReports += report)
		assertEquals(nextReports.toList, List(Transmitter.Delivered))

		val decoded = decodeWirePackage(channel.writtenBytes)
		assertEquals(decoded.frameCount, 1)
		assertEquals(decoded.payload.toSeq, secondPayload.toSeq)
	}

	test("SerializationException writes zero wire bytes, reports aFragmentWasTransmitted=false, and resets storage") {
		val channel = new ControlledAsyncChannel(_ => WriteMode.InlineComplete())
		val transmitter = new Transmitter(channel, newContext, buffersCapacity = 16, maxBuffers = 8)
		val reports = new ArrayBuffer[Transmitter.Report]()
		val failure = new SerializationException(5, "bad object")

		given Serializer[String] = (s, writer) => {
			if s == "bad" then {
				writer.putBytes(Array.fill[Byte](20)(1))
				throw failure
			} else {
				writer.putBytes(s.getBytes("UTF-8"))
			}
		}

		transmitter.transmit("bad", ProtocolVersion.OF_THIS_PROJECT)(report => reports += report)
		assertEquals(reports.toList, List(Transmitter.SerializationProblem("bad", failure, aFragmentWasTransmitted = false)))
		assertEquals(channel.writeInvocationCount, 0, "No socket writes must occur when serialization fails")
		assertEquals(channel.writtenBytes.length, 0)

		val nextReports = new ArrayBuffer[Transmitter.Report]()
		transmitter.transmit("ok", ProtocolVersion.OF_THIS_PROJECT)(report => nextReports += report)
		assertEquals(nextReports.toList, List(Transmitter.Delivered))

		val decoded = decodeWirePackage(channel.writtenBytes)
		assertEquals(decoded.frameCount, 1)
		assertEquals(decoded.payload.toSeq, "ok".getBytes("UTF-8").toSeq)
	}

	test("BuffersCountExceededException writes zero wire bytes, reports TransmissionFailure, and resets storage") {
		val channel = new ControlledAsyncChannel(_ => WriteMode.InlineComplete())
		val transmitter = new Transmitter(channel, newContext, buffersCapacity = 16, maxBuffers = 2)
		val reports = new ArrayBuffer[Transmitter.Report]()

		given Serializer[Array[Byte]] = (msg, writer) => writer.putBytes(msg)

		val oversized = sequentialPatternBytes(33)
		transmitter.transmit(oversized, ProtocolVersion.OF_THIS_PROJECT)(report => reports += report)
		assertEquals(reports.size, 1)
		reports.head match {
			case Transmitter.TransmissionFailure(msg, _: BuffersCountExceededException) => assertEquals(msg, oversized)
			case other => fail(s"Expected TransmissionFailure(_, BuffersCountExceededException), got $other")
		}
		assertEquals(channel.writeInvocationCount, 0, "No socket writes must occur when buffer capacity is exceeded")
		assertEquals(channel.writtenBytes.length, 0)

		val valid = sequentialPatternBytes(20)
		val nextReports = new ArrayBuffer[Transmitter.Report]()
		transmitter.transmit(valid, ProtocolVersion.OF_THIS_PROJECT)(report => nextReports += report)
		assertEquals(nextReports.toList, List(Transmitter.Delivered))

		val decoded = decodeWirePackage(channel.writtenBytes)
		assertEquals(decoded.frameCount, 1)
		assertEquals(decoded.payload.toSeq, valid.toSeq)
	}

	test("concurrent transmit while in flight throws TransmissionInProgressException and re-entrant transmit in callback succeeds") {
		val channel = new ControlledAsyncChannel(_ => WriteMode.DeferredComplete())
		val transmitter = new Transmitter(channel, newContext, buffersCapacity = 16, maxBuffers = 8)
		val reports = new ArrayBuffer[Transmitter.Report]()

		given Serializer[Array[Byte]] = (msg, writer) => writer.putBytes(msg)

		val first = sequentialPatternBytes(10)
		val second = sequentialPatternBytes(6)
		transmitter.transmit(first, ProtocolVersion.OF_THIS_PROJECT) { report =>
			reports += report
			transmitter.transmit(second, ProtocolVersion.OF_THIS_PROJECT)(r => reports += r)
		}
		intercept[Transmitter.TransmissionInProgressException] {
			transmitter.transmit(second, ProtocolVersion.OF_THIS_PROJECT)(_ => ())
		}

		channel.drainPending()
		assertEquals(reports.toList, List(Transmitter.Delivered, Transmitter.Delivered))
	}

	test("single-frame control package with trailing ByteSource writes STREAM_FOLLOWS_SENTINEL and stream chunks") {
		val channel = new ControlledAsyncChannel(_ => WriteMode.InlineComplete(maxBytesPerWrite = 5))
		val transmitter = new Transmitter(channel, newContext, buffersCapacity = 16, maxBuffers = 8, maxGatheringBuffers = 2)
		val controlPrefix = sequentialPatternBytes(14)
		val streamBytes = sequentialPatternBytes(23)
		val source: readren.common.ByteSource = readren.common.BufferByteSource(streamBytes)
		val reports = new ArrayBuffer[Transmitter.Report]()

		given Serializer[(Array[Byte], readren.common.ByteSource)] = (msg, writer) => {
			writer.putBytes(msg._1)
			writer.putByteSource(msg._2)
		}

		transmitter.transmit((controlPrefix, source), ProtocolVersion.OF_THIS_PROJECT)(report => reports += report)
		assertEquals(reports.toList, List(Transmitter.Delivered))

		val wire = channel.writtenBytes
		var pos = 0
		val reader: VLQ.ByteReader = () => {
			val b = wire(pos)
			pos += 1
			b
		}
		val controlLen = VLQ.decodeUnsignedInt(reader)
		val controlOut = java.util.Arrays.copyOfRange(wire, pos, pos + controlLen)
		pos += controlLen
		val sentinel = Integer.toUnsignedLong(VLQ.decodeUnsignedInt(reader))
		assertEquals(sentinel, Transmitter.STREAM_FOLLOWS_SENTINEL)

		val streamed = new ByteArrayOutputStream()
		var streamDone = false
		while !streamDone do {
			val chunkLen = VLQ.decodeUnsignedInt(reader)
			if chunkLen == 0 then streamDone = true
			else {
				streamed.write(wire, pos, chunkLen)
				pos += chunkLen
			}
		}
		assertEquals(pos, wire.length)
		assertEquals(streamed.toByteArray.toSeq, streamBytes.toSeq)
		assert(controlOut.startsWith(controlPrefix))
	}

	test("synchronous channel.write exception during trailing stream closes reader, reports TransmissionFailure, and preserves reusability") {
		val streamSyncBoom = new IOException("trailing stream sync write boom")
		val channel = new ControlledAsyncChannel(idx =>
			if idx == 1 then WriteMode.SyncThrow(streamSyncBoom) else WriteMode.InlineComplete()
		)
		val transmitter = new Transmitter(channel, newContext, buffersCapacity = 32, maxBuffers = 8)
		val reports = new ArrayBuffer[Transmitter.Report]()
		var readerClosed = false

		val streamBytes = sequentialPatternBytes(18)
		val spySource = new readren.common.ByteSource {
			private val delegate = readren.common.BufferByteSource(streamBytes)
			override def size: Long = delegate.size
			override def openChannel(): ReadableByteChannel = delegate.openChannel()
			override def transferTo(target: java.nio.channels.WritableByteChannel): Long = delegate.transferTo(target)
			override def openAsyncReader(): readren.common.ByteSource.AsyncReader = new readren.common.ByteSource.AsyncReader {
				private val underlying = delegate.openAsyncReader()
				override def read[A](dst: ByteBuffer, attachment: A, handler: CompletionHandler[Integer, ? >: A]): Int = underlying.read(dst, attachment, handler)
				override def close(): Unit = {
					readerClosed = true
					underlying.close()
				}
			}
		}

		given Serializer[(Array[Byte], readren.common.ByteSource)] = (msg, writer) => {
			writer.putBytes(msg._1)
			writer.putByteSource(msg._2)
		}

		val firstMsg = (sequentialPatternBytes(6), spySource)
		transmitter.transmit(firstMsg, ProtocolVersion.OF_THIS_PROJECT)(report => reports += report)
		assertEquals(reports.toList, List(Transmitter.TransmissionFailure(firstMsg, streamSyncBoom)))
		assert(readerClosed, "Active AsyncReader should be closed on synchronous trailing-stream channel.write failure")

		given Serializer[Array[Byte]] = (msg, writer) => writer.putBytes(msg)
		val bytesBeforeSecond = channel.writtenBytes.length
		val secondPayload = sequentialPatternBytes(9)
		val secondReports = new ArrayBuffer[Transmitter.Report]()
		transmitter.transmit(secondPayload, ProtocolVersion.OF_THIS_PROJECT)(report => secondReports += report)
		assertEquals(secondReports.toList, List(Transmitter.Delivered))

		val wireAfterFirst = java.util.Arrays.copyOfRange(channel.writtenBytes, bytesBeforeSecond, channel.writtenBytes.length)
		val decoded = decodeWirePackage(wireAfterFirst)
		assertEquals(decoded.frameCount, 1)
		assertEquals(decoded.payload.toSeq, secondPayload.toSeq)
	}

	test("async NIO2 executor stress completes losslessly as a single frame with single callback per message") {
		val executor = Executors.newFixedThreadPool(2)
		try {
			val rng = new Random(42L)
			var msgIndex = 0
			while msgIndex < 200 do {
				val asyncChannel = new BaseMockAsyncSocketChannel {
					override def write[A](
						srcs: Array[ByteBuffer],
						offset: Int,
						length: Int,
						timeout: Long,
						unit: TimeUnit,
						attachment: A,
						handler: CompletionHandler[java.lang.Long, ? >: A]
					): Unit = {
						val chunkSize = 1 + rng.nextInt(32)
						val written = consumeAndCapture(srcs, offset, length, chunkSize)
						val n = java.lang.Long.valueOf(written)
						if rng.nextInt(4) == 0 then handler.completed(n, attachment)
						else {
							executor.execute(() => {
								if rng.nextBoolean() then Thread.`yield`()
								handler.completed(n, attachment)
							})
						}
					}
				}

				val transmitter = new Transmitter(asyncChannel, newContext, buffersCapacity = 16, maxBuffers = 32, maxGatheringBuffers = 3)
				val payload = sequentialPatternBytes(if msgIndex % 20 == 0 then 0 else 1 + rng.nextInt(150))
				val callbackCount = new AtomicInteger(0)
				val reportRef = new AtomicReference[Transmitter.Report | Null](null)
				val doneLatch = new CountDownLatch(1)

				given Serializer[Array[Byte]] = (msg, writer) => {
					var off = 0
					while off < msg.length do {
						val step = math.min(9, msg.length - off)
						writer.putBytes(msg, off, step)
						off += step
					}
				}

				transmitter.transmit(payload, ProtocolVersion.OF_THIS_PROJECT) { report =>
					callbackCount.incrementAndGet()
					reportRef.set(report)
					doneLatch.countDown()
				}

				assert(doneLatch.await(10, TimeUnit.SECONDS), s"Timed out waiting for message #$msgIndex")
				assertEquals(callbackCount.get(), 1, s"Expected single completion callback on message #$msgIndex")
				assertEquals(reportRef.get(), Transmitter.Delivered)

				val decoded = decodeWirePackage(asyncChannel.writtenBytes)
				assertEquals(decoded.frameCount, if payload.isEmpty then 0 else 1)
				assert(!decoded.corrupted)
				assertEquals(decoded.payload.toSeq, payload.toSeq)
				msgIndex += 1
			}
		} finally {
			executor.shutdownNow()
		}
	}

	test("SerializationException thrown inside Writer.write resets haveToEscape for the subsequent message") {
		val channel = new ControlledAsyncChannel(_ => WriteMode.InlineComplete())
		val transmitter = new Transmitter(channel, newContext, buffersCapacity = 16, maxBuffers = 8)
		val failure = new SerializationException(0, "abort inside write")

		final case class Box(bytes: Array[Byte], shouldFail: Boolean)
		given Serializer[Box] = (box, writer) => {
			if box.shouldFail then throw failure
			else writer.putBytes(box.bytes)
		}
		given Serializer[(Box, Boolean)] = (msg, writer) => {
			if msg._2 then writer.write(msg._1)
			else writer.putBytes(msg._1.bytes)
		}

		val failingMsg = (Box(Array.emptyByteArray, shouldFail = true), true)
		val firstReports = new ArrayBuffer[Transmitter.Report]()
		transmitter.transmit(failingMsg, ProtocolVersion.OF_THIS_PROJECT)(r => firstReports += r)
		assertEquals(firstReports.toList, List(Transmitter.SerializationProblem(failingMsg, failure, aFragmentWasTransmitted = false)))

		val secondPayload = Array[Byte](Serializer.BACK_REFERENCE_HEADER, 1, 2, 3)
		val secondReports = new ArrayBuffer[Transmitter.Report]()
		transmitter.transmit((Box(secondPayload, shouldFail = false), false), ProtocolVersion.OF_THIS_PROJECT)(r => secondReports += r)
		assertEquals(secondReports.toList, List(Transmitter.Delivered))

		val decoded = decodeWirePackage(channel.writtenBytes)
		assertEquals(decoded.frameCount, 1)
		assertEquals(decoded.payload.toSeq, secondPayload.toSeq)
	}

	test("sequential transmissions across distinct caller and NIO2 threads preserve state isolation and lossless delivery") {
		val ioExecutor = Executors.newSingleThreadExecutor()
		val callerExecutors = Array(Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor())
		try {
			final class ResettableAsyncChannel extends BaseMockAsyncSocketChannel {
				private val capturedFrames = new ByteArrayOutputStream()
				override def writtenBytes: Array[Byte] = synchronized(capturedFrames.toByteArray)
				def resetCaptured(): Unit = synchronized(capturedFrames.reset())

				override def write[A](
					srcs: Array[ByteBuffer],
					offset: Int,
					length: Int,
					timeout: Long,
					unit: TimeUnit,
					attachment: A,
					handler: CompletionHandler[java.lang.Long, ? >: A]
				): Unit = {
					val written = consumeAndCapture(srcs, offset, length, 5)
					synchronized {
						capturedFrames.write(captured.toByteArray)
						captured.reset()
					}
					ioExecutor.execute(() => handler.completed(java.lang.Long.valueOf(written), attachment))
				}
			}
			val asyncChannel = new ResettableAsyncChannel

			val transmitter = new Transmitter(asyncChannel, newContext, buffersCapacity = 16, maxBuffers = 16, maxGatheringBuffers = 2)
			given Serializer[Array[Byte]] = (msg, writer) => writer.putBytes(msg)

			var i = 0
			while i < 40 do {
				asyncChannel.resetCaptured()
				val payload = sequentialPatternBytes(1 + (i * 7) % 60)
				val expectedAttachment = s"att-$i"
				val result = new AtomicReference[(Transmitter.Report, String) | Null](null)
				val done = new CountDownLatch(1)
				val caller = callerExecutors(i % 2)
				caller.execute(() => {
					transmitter.transmitWithAttachment(payload, ProtocolVersion.OF_THIS_PROJECT, expectedAttachment) { (report, att) =>
						result.set((report, att))
						done.countDown()
					}
				})
				assert(done.await(5, TimeUnit.SECONDS), s"Timed out on cross-thread message #$i")
				assertEquals(result.get(), (Transmitter.Delivered, expectedAttachment))
				val decoded = decodeWirePackage(asyncChannel.writtenBytes)
				assertEquals(decoded.frameCount, 1)
				assertEquals(decoded.payload.toSeq, payload.toSeq)
				i += 1
			}
		} finally {
			ioExecutor.shutdownNow()
			callerExecutors.foreach(_.shutdownNow())
		}
	}
}
