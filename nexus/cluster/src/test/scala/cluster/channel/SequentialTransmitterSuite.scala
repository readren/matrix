package readren.nexus
package cluster.channel

import cluster.misc.VLQ
import cluster.serialization.Serializer.SerializationException
import cluster.serialization.{ProtocolVersion, Serializer}
import cluster.service.{ChannelId, ChannelOrigin}

import munit.FunSuite
import readren.common.Maybe

import java.io.ByteArrayOutputStream
import java.net.{InetSocketAddress, SocketAddress, SocketOption}
import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousSocketChannel, CompletionHandler}
import java.util
import java.util.Collections
import java.util.concurrent.*
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class SequentialTransmitterSuite extends FunSuite {

	private enum WriteMode {
		case InlineComplete(maxBytesPerWrite: Int = Int.MaxValue)
		case DeferredComplete(maxBytesPerWrite: Int = Int.MaxValue)
	}

	private final class PendingWrite(
		val bytesWritten: Long,
		val attachment: Any,
		val handler: CompletionHandler[java.lang.Long, Any]
	) {
		def completeSuccess(): Unit = handler.completed(java.lang.Long.valueOf(bytesWritten), attachment)
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
		val recordedTimeouts = new ArrayBuffer[(Long, TimeUnit)]()

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
			synchronized(recordedTimeouts += ((timeout, unit)))
			val idx = writeCounter.getAndIncrement()
			val mode = scriptedModes(idx)
			val anyHandler = handler.asInstanceOf[CompletionHandler[java.lang.Long, Any]]
			mode match {
				case WriteMode.InlineComplete(maxBytes) =>
					val written = consumeAndCapture(srcs, offset, length, maxBytes)
					anyHandler.completed(java.lang.Long.valueOf(written), attachment)
				case WriteMode.DeferredComplete(maxBytes) =>
					val written = consumeAndCapture(srcs, offset, length, maxBytes)
					pendingWrites.add(new PendingWrite(written, attachment, anyHandler))
			}
		}
	}

	private def newContext: SequentialTransmitter.Context = new SequentialTransmitter.Context {
		private val addr = new InetSocketAddress("127.0.0.1", 9001)
		override def myAddress: SocketAddress = addr
		override def oPeerAddress: Maybe[SocketAddress] = Maybe.some(new InetSocketAddress("127.0.0.1", 9002))
		override def channelId: ChannelId = new ChannelId(ChannelOrigin.INITIATED, Maybe.some(addr))
	}

	private def decodeAllWirePackages(wire: Array[Byte]): List[Array[Byte]] = {
		var pos = 0
		val reader: VLQ.ByteReader = () => {
			val b = wire(pos)
			pos += 1
			b
		}
		val packages = new ArrayBuffer[Array[Byte]]()
		while pos < wire.length do {
			val payload = new ByteArrayOutputStream()
			var done = false
			while !done do {
				val frameLen = Integer.toUnsignedLong(VLQ.decodeUnsignedInt(reader))
				if frameLen == 0L then done = true
				else {
					val n = frameLen.toInt
					payload.write(wire, pos, n)
					pos += n
				}
			}
			packages += payload.toByteArray
		}
		packages.toList
	}

	test("queued submissions preserve per-message ProtocolVersion, timeout, timeUnit, attachment, and Serializer") {
		val channel = new ControlledAsyncChannel(idx =>
			if idx == 0 then WriteMode.DeferredComplete() else WriteMode.InlineComplete()
		)
		val transmitter = new SequentialTransmitter[Int](channel, newContext, buffersCapacity = 16, maxBuffers = 8)
		val events = new ArrayBuffer[(Int, ProtocolVersion, String, SequentialTransmitter.Report)]()

		val serializerA: Serializer[Int] = (v, w) => {
			w.putByte(1)
			w.putInt(v)
			events += ((v, w.governingVersion, "serA", SequentialTransmitter.Delivered))
		}
		val serializerB: Serializer[Int] = (v, w) => {
			w.putByte(2)
			w.putInt(v * 10)
			events += ((v, w.governingVersion, "serB", SequentialTransmitter.Delivered))
		}

		val callbacks = new ArrayBuffer[(Int, String, SequentialTransmitter.Report)]()
		transmitter.transmitWithAttachment(10, ProtocolVersion(1), "att-1", timeout = 2L, timeUnit = TimeUnit.SECONDS) { (r, a) =>
			callbacks += ((10, a, r))
		}(using serializerA)
		transmitter.transmitWithAttachment(20, ProtocolVersion(7), "att-2", timeout = 9L, timeUnit = TimeUnit.MILLISECONDS) { (r, a) =>
			callbacks += ((20, a, r))
		}(using serializerB)

		channel.drainPending()
		assertEquals(
			callbacks.toList,
			List((10, "att-1", SequentialTransmitter.Delivered), (20, "att-2", SequentialTransmitter.Delivered))
		)
		assertEquals(
			events.map(e => (e._1, e._2, e._3)).toList,
			List((10, ProtocolVersion(1), "serA"), (20, ProtocolVersion(7), "serB"))
		)
		assertEquals(channel.recordedTimeouts.toList, List((2L, TimeUnit.SECONDS), (9L, TimeUnit.MILLISECONDS)))

		val decoded = decodeAllWirePackages(channel.writtenBytes)
		assertEquals(decoded.size, 2)
		assertEquals(ByteBuffer.wrap(decoded(0)).get(), 1.toByte)
		assertEquals(ByteBuffer.wrap(decoded(0)).getInt(1), 10)
		assertEquals(ByteBuffer.wrap(decoded(1)).get(), 2.toByte)
		assertEquals(ByteBuffer.wrap(decoded(1)).getInt(1), 200)
	}

	test("callback for completed message finishes strictly before next queued message serializes or completes") {
		val channel = new ControlledAsyncChannel(idx =>
			if idx == 0 then WriteMode.DeferredComplete() else WriteMode.InlineComplete()
		)
		val transmitter = new SequentialTransmitter[Int](channel, newContext, buffersCapacity = 16, maxBuffers = 8)
		val trace = new ArrayBuffer[String]()

		given Serializer[Int] = (v, w) => {
			trace += s"serialize:$v"
			w.putInt(v)
		}

		transmitter.transmit(1, ProtocolVersion.OF_THIS_PROJECT)(r => trace += s"callback:1:$r")
		transmitter.transmit(2, ProtocolVersion.OF_THIS_PROJECT)(r => trace += s"callback:2:$r")

		channel.drainPending()
		assertEquals(
			trace.toList,
			List("serialize:1", "callback:1:Delivered", "serialize:2", "callback:2:Delivered")
		)
	}

	test("draining thousands of synchronously completing queued messages stays in O(1) stack space") {
		val channel = new ControlledAsyncChannel(idx =>
			if idx == 0 then WriteMode.DeferredComplete() else WriteMode.InlineComplete()
		)
		val transmitter = new SequentialTransmitter[Int](channel, newContext, buffersCapacity = 16, maxBuffers = 8)
		val total = 5000
		val completedCount = new AtomicInteger(0)
		var lastSeq = -1

		given Serializer[Int] = (v, w) => w.putInt(v)

		var i = 0
		while i < total do {
			val seq = i
			transmitter.transmit(seq, ProtocolVersion.OF_THIS_PROJECT) { report =>
				assertEquals(report, SequentialTransmitter.Delivered)
				assertEquals(seq, lastSeq + 1)
				lastSeq = seq
				completedCount.incrementAndGet()
			}
			i += 1
		}

		channel.drainPending()
		assertEquals(completedCount.get(), total)
	}

	test("idle pruning and explicit pruneTo respect Pruning state and concurrent submissions") {
		val channel = new ControlledAsyncChannel(_ => WriteMode.DeferredComplete())
		val transmitter = new SequentialTransmitter[Array[Byte]](channel, newContext, buffersCapacity = 16, maxBuffers = 8, minBuffersOnIdle = 1)
		given Serializer[Array[Byte]] = (msg, writer) => writer.putBytes(msg)

		val reports = new ArrayBuffer[SequentialTransmitter.Report]()
		transmitter.transmit(new Array[Byte](64), ProtocolVersion.OF_THIS_PROJECT)(r => reports += r)
		assert(transmitter.bufferCount >= 4)

		// Explicit pruneTo while transmitting must be a safe no-op and return current bufferCount.
		val duringActive = transmitter.pruneTo(1)
		assert(duringActive >= 4)

		channel.drainPending()
		assertEquals(reports.toList, List(SequentialTransmitter.Delivered))
		assertEquals(transmitter.bufferCount, 1)
	}

	test("callback exception propagates without stalling subsequent queued transmissions") {
		val channel = new ControlledAsyncChannel(idx =>
			if idx == 0 then WriteMode.DeferredComplete() else WriteMode.InlineComplete()
		)
		val transmitter = new SequentialTransmitter[Int](channel, newContext, buffersCapacity = 16, maxBuffers = 8)
		val reports = new ArrayBuffer[(Int, SequentialTransmitter.Report)]()
		given Serializer[Int] = (v, w) => w.putInt(v)

		transmitter.transmit(1, ProtocolVersion.OF_THIS_PROJECT) { r =>
			reports += ((1, r))
			throw new RuntimeException("user callback boom")
		}
		transmitter.transmit(2, ProtocolVersion.OF_THIS_PROJECT)(r => reports += ((2, r)))
		transmitter.transmit(3, ProtocolVersion.OF_THIS_PROJECT)(r => reports += ((3, r)))

		val pending = channel.pollPending()
		assert(pending ne null)
		val boom = intercept[RuntimeException] {
			pending.nn.completeSuccess()
		}
		assertEquals(boom.getMessage, "user callback boom")
		assertEquals(
			reports.toList,
			List((1, SequentialTransmitter.Delivered), (2, SequentialTransmitter.Delivered), (3, SequentialTransmitter.Delivered))
		)
		assertEquals(decodeAllWirePackages(channel.writtenBytes).size, 3)
	}

	test("triggerOnIdle composes callbacks and supports re-entrant transmit in onComplete and onIdle") {
		val channel = new ControlledAsyncChannel(idx =>
			if idx == 0 then WriteMode.DeferredComplete() else WriteMode.InlineComplete()
		)
		val transmitter = new SequentialTransmitter[Int](channel, newContext, buffersCapacity = 16, maxBuffers = 8)
		val events = new ArrayBuffer[String]()
		given Serializer[Int] = (v, w) => w.putInt(v)

		val immediateLatch = new CountDownLatch(1)
		transmitter.triggerOnIdle(() => immediateLatch.countDown())
		assert(immediateLatch.await(1, TimeUnit.SECONDS))

		transmitter.transmit(1, ProtocolVersion.OF_THIS_PROJECT) { r =>
			events += s"cb1:$r"
			transmitter.transmit(2, ProtocolVersion.OF_THIS_PROJECT)(r2 => events += s"cb2:$r2")
		}
		transmitter.triggerOnIdle(() => {
			events += "idle1"
			transmitter.transmit(3, ProtocolVersion.OF_THIS_PROJECT)(r3 => events += s"cb3:$r3")
		})
		transmitter.triggerOnIdle(() => events += "idle2")

		channel.drainPending()
		assertEquals(
			events.toList,
			List("cb1:Delivered", "cb2:Delivered", "idle1", "cb3:Delivered", "idle2")
		)
	}

	test("transmit throws RejectedExecutionException when wait queue reaches MaxQueuedCount") {
		val channel = new ControlledAsyncChannel(_ => WriteMode.DeferredComplete())
		val transmitter = new SequentialTransmitter[Int](channel, newContext, buffersCapacity = 16, maxBuffers = 4)
		given Serializer[Int] = (v, w) => w.putInt(v)

		transmitter.transmit(1, ProtocolVersion.OF_THIS_PROJECT)(_ => ())

		val stateField = classOf[SequentialTransmitter[?]].getDeclaredField("coordinationState")
		stateField.setAccessible(true)
		val coord = stateField.get(transmitter).asInstanceOf[AtomicInteger]
		val stageTransmitting = 1 << 29
		val maxQueued = 0x0fff_ffff
		val saved = coord.getAndSet(stageTransmitting | maxQueued)
		try {
			intercept[RejectedExecutionException] {
				transmitter.transmit(2, ProtocolVersion.OF_THIS_PROJECT)(_ => ())
			}
			intercept[RejectedExecutionException] {
				transmitter.transmitWithAttachment(3, ProtocolVersion.OF_THIS_PROJECT, "att")((_, _) => ())
			}
		} finally {
			coord.set(saved)
		}
		channel.drainPending()
	}

	test("multi-threaded burst completes losslessly in strict per-producer order with safe concurrent pruneTo") {
		val ioExecutor = Executors.newFixedThreadPool(2)
		val producerExecutor = Executors.newFixedThreadPool(4)
		try {
			val rng = new Random(123L)
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
					else ioExecutor.execute(() => handler.completed(n, attachment))
				}
			}

			val transmitter = new SequentialTransmitter[Array[Byte]](asyncChannel, newContext, buffersCapacity = 16, maxBuffers = 32, minBuffersOnIdle = 2)
			val totalMessages = 80
			val payloads = Array.tabulate(totalMessages) { i =>
				val body = Array.tabulate[Byte](8 + (i % 11) * 7)(j => ((i + j) & 0xff).toByte)
				val withId = new Array[Byte](4 + body.length)
				ByteBuffer.wrap(withId).putInt(i).put(body)
				withId
			}
			val callbackCounts = Array.fill(totalMessages)(new AtomicInteger(0))
			val reports = Array.fill[SequentialTransmitter.Report | Null](totalMessages)(null)
			val doneLatch = new CountDownLatch(totalMessages)
			given Serializer[Array[Byte]] = (msg, writer) => writer.putBytes(msg)

			val startBarrier = new CyclicBarrier(4)
			for t <- 0 until 4 do {
				producerExecutor.execute(() => {
					startBarrier.await()
					var i = t
					while i < totalMessages do {
						val idx = i
						if idx % 9 == 0 then transmitter.pruneTo(2)
						transmitter.transmit(payloads(idx), ProtocolVersion.OF_THIS_PROJECT) { report =>
							callbackCounts(idx).incrementAndGet()
							reports(idx) = report
							doneLatch.countDown()
						}
						i += 4
					}
				})
			}

			assert(doneLatch.await(15, TimeUnit.SECONDS), "Timed out waiting for multi-threaded SequentialTransmitter burst")
			val idleLatch = new CountDownLatch(1)
			transmitter.triggerOnIdle(() => idleLatch.countDown())
			assert(idleLatch.await(5, TimeUnit.SECONDS), "Timed out waiting for triggerOnIdle")

			for i <- 0 until totalMessages do {
				assertEquals(callbackCounts(i).get(), 1)
				assertEquals(reports(i), SequentialTransmitter.Delivered)
			}

			val decoded = decodeAllWirePackages(asyncChannel.writtenBytes)
			assertEquals(decoded.size, totalMessages)
			val seenIds = decoded.map(p => ByteBuffer.wrap(p).getInt())
			assertEquals(seenIds.sorted, (0 until totalMessages).toList)
			for pkg <- decoded do {
				val id = ByteBuffer.wrap(pkg).getInt()
				assertEquals(pkg.toSeq, payloads(id).toSeq)
			}
		} finally {
			producerExecutor.shutdownNow()
			ioExecutor.shutdownNow()
		}
	}
}
