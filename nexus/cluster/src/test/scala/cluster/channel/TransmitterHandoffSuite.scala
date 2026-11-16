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
import java.nio.channels.{AsynchronousSocketChannel, CompletionHandler}
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

	private case class DecodedPackage(payload: Array[Byte], corrupted: Boolean, trailingStream: Boolean)

	private def decodeWirePackage(wire: Array[Byte]): DecodedPackage = {
		var pos = 0
		val reader: VLQ.ByteReader = () => {
			val b = wire(pos)
			pos += 1
			b
		}
		val payload = new ByteArrayOutputStream()
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
			}
		}
		assertEquals(pos, wire.length, "Unexpected trailing wire bytes after package terminator")
		DecodedPackage(payload.toByteArray, corrupted, trailingStream)
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

	test("SERIALIZING_AND_TRANSMITTING -> SERIALIZATION_COMPLETED_AND_TRANSMITTING: slow NIO2 drains all multi-buffer frames cleanly") {
		val channel = new ControlledAsyncChannel(_ => WriteMode.DeferredComplete(maxBytesPerWrite = 7))
		val transmitter = new Transmitter(channel, newContext, buffersCapacity = 16, maxBuffers = 8)
		val payload = sequentialPatternBytes(48)
		val reports = new ArrayBuffer[Transmitter.Report]()

		given Serializer[Array[Byte]] = (msg, writer) => {
			writer.putBytes(msg)
		}

		transmitter.transmit(payload, ProtocolVersion.OF_THIS_PROJECT)(report => reports += report)
		assertEquals(reports.size, 0, "onComplete must not run while initial behind-transmission write is still deferred")

		val steps = channel.drainPending()
		assert(steps >= 3, s"Expected multiple async write steps, got $steps")
		assertEquals(reports.toList, List(Transmitter.Delivered))

		val decoded = decodeWirePackage(channel.writtenBytes)
		assert(!decoded.corrupted)
		assert(!decoded.trailingStream)
		assertEquals(decoded.payload.toSeq, payload.toSeq)
	}

	test("SERIALIZING_AND_TRANSMITTING -> TRANSMISSION_PAUSED_WHILE_SERIALIZING -> SERIALIZATION_COMPLETED_AND_TRANSMITTING: fast NIO2 pauses and caller resumes cleanly") {
		val channel = new ControlledAsyncChannel(_ => WriteMode.InlineComplete())
		val transmitter = new Transmitter(channel, newContext, buffersCapacity = 16, maxBuffers = 8)
		val payload = sequentialPatternBytes(36)
		val reports = new ArrayBuffer[Transmitter.Report]()

		given Serializer[Array[Byte]] = (msg, writer) => {
			writer.putBytes(msg, 0, 16)
			// Completing write #0 inline during the buffer advance inside `putBytes` transitions `coordinationCell` to TRANSMISSION_PAUSED_WHILE_SERIALIZING before chunk 2 is written.
			writer.putBytes(msg, 16, 16)
			// Caller advances to buffer #3 and observes TRANSMISSION_PAUSED_WHILE_SERIALIZING.
			writer.putBytes(msg, 32, 4)
		}

		transmitter.transmit(payload, ProtocolVersion.OF_THIS_PROJECT)(report => reports += report)
		assertEquals(reports.toList, List(Transmitter.Delivered))

		val decoded = decodeWirePackage(channel.writtenBytes)
		assert(!decoded.corrupted)
		assertEquals(decoded.payload.toSeq, payload.toSeq)
	}

	test("synchronous channel.write exception in getBuffer reports TransmissionFailure once and preserves reusability") {
		val syncBoom = new IOException("sync write boom")
		val channel = new ControlledAsyncChannel(idx =>
			if idx == 0 then WriteMode.SyncThrow(syncBoom) else WriteMode.InlineComplete()
		)
		val transmitter = new Transmitter(channel, newContext, buffersCapacity = 16, maxBuffers = 4)
		val reports = new ArrayBuffer[Transmitter.Report]()

		given Serializer[Int] = (n, writer) => {
			var i = 0
			while i < n do {
				writer.putBytes(Array.fill[Byte](10)(i.toByte))
				i += 1
			}
		}

		transmitter.transmit(3, ProtocolVersion.OF_THIS_PROJECT)(report => reports += report)
		assertEquals(reports.toList, List(Transmitter.TransmissionFailure(3, syncBoom)))

		val nextReports = new ArrayBuffer[Transmitter.Report]()
		transmitter.transmit(1, ProtocolVersion.OF_THIS_PROJECT)(report => nextReports += report)
		assertEquals(nextReports.toList, List(Transmitter.Delivered))
	}

	test("SerializationException after behind-transmission started while NIO2 in flight emits corrupted sentinel") {
		val channel = new ControlledAsyncChannel(_ => WriteMode.DeferredComplete())
		val transmitter = new Transmitter(channel, newContext, buffersCapacity = 16, maxBuffers = 8)
		val reports = new ArrayBuffer[Transmitter.Report]()
		val failure = new SerializationException(5, "bad object")

		given Serializer[String] = (_, writer) => {
			writer.putBytes(Array.fill[Byte](12)(1))
			writer.putBytes(Array.fill[Byte](5)(2))
			throw failure
		}

		transmitter.transmit("msg", ProtocolVersion.OF_THIS_PROJECT)(report => reports += report)
		assertEquals(reports.size, 0, "Callback must wait until in-flight frame and corrupted sentinel complete")

		channel.drainPending()
		assertEquals(reports.toList, List(Transmitter.SerializationProblem("msg", failure, true)))

		val decoded = decodeWirePackage(channel.writtenBytes)
		assert(decoded.corrupted, "Wire package must terminate with CORRUPTED_PACKAGE_SENTINEL")
	}

	test("SerializationException when behind-transmission already paused emits corrupted sentinel from caller") {
		val channel = new ControlledAsyncChannel(_ => WriteMode.InlineComplete())
		val transmitter = new Transmitter(channel, newContext, buffersCapacity = 16, maxBuffers = 8)
		val reports = new ArrayBuffer[Transmitter.Report]()
		val failure = new SerializationException(3, "paused failure")

		given Serializer[String] = (_, writer) => {
			writer.putBytes(Array.fill[Byte](12)(1))
			// Crossing the 16-byte buffer boundary starts write #0, which completes synchronously inline -> TRANSMISSION_PAUSED_WHILE_SERIALIZING.
			writer.putBytes(Array.fill[Byte](5)(2))
			throw failure
		}

		transmitter.transmit("msg-paused", ProtocolVersion.OF_THIS_PROJECT)(report => reports += report)
		assertEquals(reports.toList, List(Transmitter.SerializationProblem("msg-paused", failure, true)))

		val decoded = decodeWirePackage(channel.writtenBytes)
		assert(decoded.corrupted, "Wire package must terminate with CORRUPTED_PACKAGE_SENTINEL")
	}

	test("TRANSMISSION_FAILED aborts serialization on next getBuffer without exhausting remaining buffers") {
		val channel = new ControlledAsyncChannel(_ => WriteMode.DeferredComplete())
		val transmitter = new Transmitter(channel, newContext, buffersCapacity = 16, maxBuffers = 16)
		val reports = new ArrayBuffer[Transmitter.Report]()
		val ioBoom = new IOException("async socket broken")
		var iterationsCompleted = 0

		given Serializer[Int] = (count, writer) => {
			var i = 0
			while i < count do {
				writer.putBytes(Array.fill[Byte](12)(i.toByte))
				if i == 1 then {
					val pending = channel.pollPending()
					assert(pending ne null)
					pending.nn.completeFailure(ioBoom)
				}
				iterationsCompleted += 1
				i += 1
			}
		}

		transmitter.transmit(10, ProtocolVersion.OF_THIS_PROJECT)(report => reports += report)
		assertEquals(iterationsCompleted, 2, "Serialization should abort on the third getBuffer immediately after TRANSMISSION_FAILED")
		assertEquals(reports.toList, List(Transmitter.TransmissionFailure(10, ioBoom)))
	}

	test("TRANSMISSION_FAILED right before serialize returns reports TransmissionFailure once without corrupted sentinel") {
		val channel = new ControlledAsyncChannel(_ => WriteMode.DeferredComplete())
		val transmitter = new Transmitter(channel, newContext, buffersCapacity = 16, maxBuffers = 8)
		val reports = new ArrayBuffer[Transmitter.Report]()
		val ioBoom = new IOException("late socket failure")

		given Serializer[String] = (_, writer) => {
			writer.putBytes(Array.fill[Byte](12)(1))
			writer.putBytes(Array.fill[Byte](12)(2))
			val pending = channel.pollPending()
			assert(pending ne null)
			pending.nn.completeFailure(ioBoom)
		}

		transmitter.transmit("late-fail", ProtocolVersion.OF_THIS_PROJECT)(report => reports += report)
		assertEquals(reports.toList, List(Transmitter.TransmissionFailure("late-fail", ioBoom)))
		assertEquals(channel.writeInvocationCount, 1, "Must not dispatch sentinel writes after TRANSMISSION_FAILED")
	}

	test("BuffersCountExceededException after behind-transmission started emits corrupted sentinel and reports TransmissionFailure") {
		val channel = new ControlledAsyncChannel(_ => WriteMode.DeferredComplete())
		val transmitter = new Transmitter(channel, newContext, buffersCapacity = 16, maxBuffers = 2)
		val reports = new ArrayBuffer[Transmitter.Report]()

		given Serializer[Int] = (_, writer) => {
			writer.putBytes(Array.fill[Byte](16)(1))
			writer.putBytes(Array.fill[Byte](16)(2))
			// With maxBuffers = 2 (32 bytes total) and write #0 deferred, advancing to a 3rd buffer exceeds capacity.
			writer.putBytes(Array.fill[Byte](1)(3))
		}

		transmitter.transmit(42, ProtocolVersion.OF_THIS_PROJECT)(report => reports += report)
		assertEquals(reports.size, 0, "Callback must wait until deferred frame and corrupted sentinel finish")

		channel.drainPending()
		assertEquals(reports.size, 1)
		reports.head match {
			case Transmitter.TransmissionFailure(42, _: BuffersCountExceededException) => ()
			case other => fail(s"Expected TransmissionFailure(_, BuffersCountExceededException), got $other")
		}

		val decoded = decodeWirePackage(channel.writtenBytes)
		assert(decoded.corrupted, "Wire package must terminate with CORRUPTED_PACKAGE_SENTINEL after capacity exceeded mid-flight")
	}

	test("concurrent serializer vs async NIO2 executor stress completes losslessly with single callback per message") {
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

				val transmitter = new Transmitter(asyncChannel, newContext, buffersCapacity = 16, maxBuffers = 32)
				val payload = sequentialPatternBytes(8 + rng.nextInt(120))
				val callbackCount = new AtomicInteger(0)
				val reportRef = new AtomicReference[Transmitter.Report | Null](null)
				val doneLatch = new CountDownLatch(1)

				given Serializer[Array[Byte]] = (msg, writer) => {
					var off = 0
					while off < msg.length do {
						val step = math.min(9, msg.length - off)
						writer.putBytes(msg, off, step)
						off += step
						if rng.nextBoolean() then Thread.`yield`()
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
				assert(!decoded.corrupted)
				assertEquals(decoded.payload.toSeq, payload.toSeq)
				msgIndex += 1
			}
		} finally {
			executor.shutdownNow()
		}
	}
}
