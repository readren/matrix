package readren.nexus
package cluster.channel

import cluster.misc.{RingLength, VLQ}
import cluster.serialization.Serializer.SerializationException
import cluster.serialization.{ProtocolVersion, Serializer}
import cluster.service.{ChannelId, ChannelOrigin}

import munit.FunSuite
import readren.common.{BufferByteSource, ByteSource, Maybe}

import java.io.{ByteArrayOutputStream, EOFException, IOException}
import java.net.{InetSocketAddress, SocketAddress, SocketOption}
import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousSocketChannel, CompletionHandler, InterruptedByTimeoutException}
import java.util
import java.util.Collections
import java.util.concurrent.*
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.locks.LockSupport
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.util.control.NonFatal

class PipelinedTransmitterSuite extends FunSuite {

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

		protected final def consumeAndCapture(sources: Array[ByteBuffer], offset: Int, length: Int, limit: Int): Long = {
			var total = 0L
			var remaining = limit
			var i = offset
			val end = offset + length
			while i < end && remaining > 0 do {
				val buf = sources(i)
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
		private val maxObservedBatchLength = new AtomicInteger(0)
		private val pendingWrites = new ConcurrentLinkedQueue[PendingWrite]()
		private val recordedTimeouts = new ArrayBuffer[(Long, TimeUnit)]()

		def writeInvocationCount: Int = writeCounter.get()
		def maxBatchLength: Int = maxObservedBatchLength.get()
		def pollPending(): PendingWrite | Null = pendingWrites.poll()
		def observedTimeouts: List[(Long, TimeUnit)] = synchronized(recordedTimeouts.toList)

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
			synchronized(recordedTimeouts.addOne(timeout -> unit))
			maxObservedBatchLength.accumulateAndGet(length, math.max)
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

	private def newContext: PipelinedTransmitter.Context = new PipelinedTransmitter.Context {
		private val addr = new InetSocketAddress("127.0.0.1", 9001)
		override def myAddress: SocketAddress = addr
		override def oPeerAddress: Maybe[SocketAddress] = Maybe.some(new InetSocketAddress("127.0.0.1", 9002))
		override def channelId: ChannelId = new ChannelId(ChannelOrigin.INITIATED, Maybe.some(addr))
	}

	private case class DecodedPackage(
		payload: Array[Byte],
		corrupted: Boolean,
		trailingStream: Boolean,
		frameCount: Int,
		streamChunks: List[Array[Byte]] = Nil,
		streamAborted: Boolean = false
	) {
		def streamBytes: Array[Byte] = {
			val out = new ByteArrayOutputStream()
			streamChunks.foreach(out.write)
			out.toByteArray
		}
	}

	private def decodeAllWirePackages(wire: Array[Byte], readTrailingStreams: Boolean = false): List[DecodedPackage] = {
		var pos = 0
		val reader: VLQ.ByteReader = () => {
			val b = wire(pos)
			pos += 1
			b
		}
		val packages = new ArrayBuffer[DecodedPackage]()
		while pos < wire.length do {
			val payload = new ByteArrayOutputStream()
			var corrupted = false
			var trailingStream = false
			var frameCount = 0
			var done = false
			while !done do {
				val frameLen = Integer.toUnsignedLong(VLQ.decodeUnsignedInt(reader))
				if frameLen == 0L then done = true
				else if frameLen == PipelinedTransmitter.CORRUPTED_PACKAGE_SENTINEL then {
					corrupted = true
					done = true
				} else if frameLen == PipelinedTransmitter.STREAM_FOLLOWS_SENTINEL then {
					trailingStream = true
					done = true
				} else {
					frameCount += 1
					val n = frameLen.toInt
					payload.write(wire, pos, n)
					pos += n
				}
			}
			val chunks = new ArrayBuffer[Array[Byte]]()
			var streamAborted = false
			if trailingStream && readTrailingStreams then {
				var streamDone = false
				while !streamDone do {
					val chunkLen = Integer.toUnsignedLong(VLQ.decodeUnsignedInt(reader))
					if chunkLen == 0L then streamDone = true
					else if chunkLen == PipelinedTransmitter.CORRUPTED_PACKAGE_SENTINEL then {
						streamAborted = true
						streamDone = true
					} else {
						val n = chunkLen.toInt
						val chunk = new Array[Byte](n)
						System.arraycopy(wire, pos, chunk, 0, n)
						pos += n
						chunks += chunk
					}
				}
			}
			packages += DecodedPackage(payload.toByteArray, corrupted, trailingStream, frameCount, chunks.toList, streamAborted)
		}
		assertEquals(pos, wire.length, "Unexpected trailing wire bytes after package terminators")
		packages.toList
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

	test("single-frame multi-buffer control package exceeding maxGatheringBuffers transmits across bounded batches with partial writes") {
		val channel = new ControlledAsyncChannel(_ => WriteMode.DeferredComplete(maxBytesPerWrite = 7))
		val transmitter = new PipelinedTransmitter[Array[Byte]](channel, newContext, capacityOfBufferInRing = 16, ringMaxLength = RingLength(32), ringMinLengthOnIdle = RingLength(2), maxGatheringBuffers = 4)
		// 264 bytes -> spans 17 circular storage buffers (> maxGatheringBuffers = 4).
		val payload = sequentialPatternBytes(22 * 12)
		val reports = new ArrayBuffer[PipelinedTransmitter.Report]()

		given Serializer[Array[Byte]] = (msg, writer) => {
			writer.putBytes(msg)
		}

		transmitter.transmit(payload, ProtocolVersion.OF_THIS_PROJECT)(report => reports += report)
		assertEquals(reports.size, 0, "Callback must wait until all bounded gathering batches finish")

		val steps = channel.drainPending()
		assert(steps > 6, s"Expected multiple bounded gathering batches and partial writes, got $steps")
		assert(channel.maxBatchLength <= 4, s"Gathering batch exceeded maxGatheringBuffers=4: ${channel.maxBatchLength}")
		assertEquals(reports.toList, List(PipelinedTransmitter.Delivered))
		assertEquals(transmitter.bufferInRingTotalCount, 3, "Transmitted stage prunes to ringMinLengthOnIdle=2 while untouched stage retains 1 initial buffer")

		val decoded = decodeAllWirePackages(channel.writtenBytes)
		assertEquals(decoded.size, 1)
		assertEquals(decoded.head.frameCount, 1, "Entire control package must be emitted as a single protocol frame")
		assert(!decoded.head.corrupted)
		assert(!decoded.head.trailingStream)
		assertEquals(decoded.head.payload.toSeq, payload.toSeq)
	}

	test("exact-capacity buffer triggers separate sentinel buffer even when batch reaches maxGatheringBuffers") {
		val channel = new ControlledAsyncChannel(_ => WriteMode.InlineComplete())
		// 4 buffers * 16 bytes = 64 bytes -> header(1) + 4 buffers = 5 buffers, with maxGatheringBuffers = 2 forces sentinel into a separate batch.
		val transmitter = new PipelinedTransmitter[Array[Byte]](channel, newContext, capacityOfBufferInRing = 16, ringMaxLength = RingLength(8), maxGatheringBuffers = 2)
		val payload = sequentialPatternBytes(64)
		val reports = new ArrayBuffer[PipelinedTransmitter.Report]()

		given Serializer[Array[Byte]] = (msg, writer) => {
			writer.putBytes(msg)
		}

		transmitter.transmit(payload, ProtocolVersion.OF_THIS_PROJECT)(report => reports += report)
		assertEquals(reports.toList, List(PipelinedTransmitter.Delivered))
		assert(channel.maxBatchLength <= 2)

		val decoded = decodeAllWirePackages(channel.writtenBytes)
		assertEquals(decoded.size, 1)
		assertEquals(decoded.head.frameCount, 1)
		assertEquals(decoded.head.payload.toSeq, payload.toSeq)
	}

	test("inter-message pipelining burst swaps serialization and transmission rings, delivers FIFO, fires triggerOnIdle, and prunes buffers") {
		val channel = new ControlledAsyncChannel(_ => WriteMode.DeferredComplete(maxBytesPerWrite = 11))
		val transmitter = new PipelinedTransmitter[Array[Byte]](channel, newContext, capacityOfBufferInRing = 16, ringMaxLength = RingLength(16), ringMinLengthOnIdle = RingLength(2), maxGatheringBuffers = 4)
		val reports = new ArrayBuffer[(Int, PipelinedTransmitter.Report)]()
		val idleCalls = new AtomicInteger(0)

		given Serializer[Array[Byte]] = (msg, writer) => {
			writer.putBytes(msg)
		}

		val count = 50
		val messages = Array.tabulate(count)(i => sequentialPatternBytes(40 + (i % 7) * 12))
		for i <- 0 until count do {
			transmitter.transmit(messages(i), ProtocolVersion.OF_THIS_PROJECT)(report => reports.addOne(i -> report))
		}
		transmitter.triggerOnIdle(() => idleCalls.incrementAndGet())
		assertEquals(idleCalls.get(), 0, "onIdle must not fire while pipeline is active")

		channel.drainPending()
		assertEquals(idleCalls.get(), 1, "onIdle must fire exactly once upon draining the burst")
		assertEquals(reports.map(_._1).toList, (0 until count).toList)
		assert(reports.forall(_._2 == PipelinedTransmitter.Delivered))
		assertEquals(transmitter.bufferInRingTotalCount, 4)

		val decoded = decodeAllWirePackages(channel.writtenBytes)
		assertEquals(decoded.size, count)
		for i <- 0 until count do {
			assertEquals(decoded(i).frameCount, 1)
			assertEquals(decoded(i).payload.toSeq, messages(i).toSeq)
		}

		// Calling triggerOnIdle when already idle fires immediately once.
		transmitter.triggerOnIdle(() => idleCalls.incrementAndGet())
		assertEquals(idleCalls.get(), 2)
	}

	test("control serialization failures (SerializationException and RingLengthExceededException) do not taint the wire") {
		val channel = new ControlledAsyncChannel(_ => WriteMode.DeferredComplete())
		val transmitter = new PipelinedTransmitter[(Int, String)](channel, newContext, capacityOfBufferInRing = 16, ringMaxLength = RingLength(2), ringMinLengthOnIdle = RingLength(2))
		val reports = new ArrayBuffer[(Int, PipelinedTransmitter.Report)]()
		val serFailure = new SerializationException(10, "bad object")

		given Serializer[(Int, String)] = (pair, writer) => {
			pair._1 match {
				case 1 =>
					writer.putBytes(Array.fill[Byte](12)(11))
				case 2 =>
					writer.putBytes(Array.fill[Byte](12)(22))
					throw serFailure
				case 3 =>
					writer.putBytes(Array.fill[Byte](16)(33))
					writer.putBytes(Array.fill[Byte](16)(34))
					// Exceeds ringMaxLength = 2 (total capacity = 32 bytes)
					writer.putBytes(Array.fill[Byte](1)(35))
				case 4 =>
					writer.putBytes(Array.fill[Byte](12)(44))
			}
		}

		// Message 1 enters transmission (DeferredComplete); 2, 3, 4 go through serialization / queue.
		transmitter.transmit((1, "ok1"), ProtocolVersion.OF_THIS_PROJECT)(r => reports.addOne(1 -> r))
		transmitter.transmit((2, "bad-ser"), ProtocolVersion.OF_THIS_PROJECT)(r => reports.addOne(2 -> r))
		transmitter.transmit((3, "bad-buf"), ProtocolVersion.OF_THIS_PROJECT)(r => reports.addOne(3 -> r))
		transmitter.transmit((4, "ok4"), ProtocolVersion.OF_THIS_PROJECT)(r => reports.addOne(4 -> r))

		channel.drainPending()
		assertEquals(reports.map(_._1).toList, List(1, 2, 3, 4), "Callbacks must fire in strict FIFO order even when queued messages fail serialization")

		val byId = reports.toMap
		assertEquals(byId(1), PipelinedTransmitter.Delivered)
		assertEquals(byId(2), PipelinedTransmitter.SerializationProblem((2, "bad-ser"), serFailure))
		byId(3) match {
			case PipelinedTransmitter.TransmissionFailure(msg, _: RingLengthExceededException) => assertEquals(msg, (3, "bad-buf"))
			case other => fail(s"Expected TransmissionFailure(_, RingLengthExceededException), got $other")
		}
		assertEquals(byId(4), PipelinedTransmitter.Delivered)

		val decoded = decodeAllWirePackages(channel.writtenBytes)
		assertEquals(decoded.size, 2, "Failed serializations must write zero bytes and zero sentinels to the channel")
		assertEquals(decoded(0).payload.toSeq, Array.fill[Byte](12)(11).toSeq)
		assertEquals(decoded(1).payload.toSeq, Array.fill[Byte](12)(44).toSeq)
	}

	test("serialization failure when idle resets storage, triggers onIdle, and preserves reusability") {
		val channel = new ControlledAsyncChannel(_ => WriteMode.InlineComplete())
		val transmitter = new PipelinedTransmitter[Boolean](channel, newContext, capacityOfBufferInRing = 16, ringMaxLength = RingLength(4), ringMinLengthOnIdle = RingLength(2))
		val reports = new ArrayBuffer[PipelinedTransmitter.Report]()
		val serFailure = new SerializationException(4, "isolated failure")
		val idleCalls = new AtomicInteger(0)

		given Serializer[Boolean] = (shouldFail, writer) => {
			writer.putBytes(Array.fill[Byte](12)(7))
			if shouldFail then throw serFailure
		}

		transmitter.transmit(true, ProtocolVersion.OF_THIS_PROJECT)(r => reports += r)
		assertEquals(reports.toList, List(PipelinedTransmitter.SerializationProblem(true, serFailure)))
		assertEquals(channel.writtenBytes.length, 0)
		transmitter.triggerOnIdle(() => idleCalls.incrementAndGet())
		assertEquals(idleCalls.get(), 1)

		transmitter.transmit(false, ProtocolVersion.OF_THIS_PROJECT)(r => reports += r)
		assertEquals(reports.size, 2)
		assertEquals(reports(1), PipelinedTransmitter.Delivered)
		val decoded = decodeAllWirePackages(channel.writtenBytes)
		assertEquals(decoded.size, 1)
	}

	test("synchronous channel.write exception reports TransmissionFailure once and advances queued messages") {
		val syncBoom = new IOException("sync write boom")
		val channel = new ControlledAsyncChannel(idx =>
			if idx == 0 then WriteMode.SyncThrow(syncBoom) else WriteMode.InlineComplete()
		)
		val transmitter = new PipelinedTransmitter[Int](channel, newContext, capacityOfBufferInRing = 16, ringMaxLength = RingLength(4))
		val reports = new ArrayBuffer[PipelinedTransmitter.Report]()

		given Serializer[Int] = (n, writer) => {
			var i = 0
			while i < n do {
				writer.putBytes(Array.fill[Byte](10)(i.toByte))
				i += 1
			}
		}

		transmitter.transmit(3, ProtocolVersion.OF_THIS_PROJECT)(report => reports += report)
		assertEquals(reports.toList, List(PipelinedTransmitter.TransmissionFailure(3, syncBoom)))

		val nextReports = new ArrayBuffer[PipelinedTransmitter.Report]()
		transmitter.transmit(1, ProtocolVersion.OF_THIS_PROJECT)(report => nextReports += report)
		assertEquals(nextReports.toList, List(PipelinedTransmitter.Delivered))
	}

	test("asynchronous channel write failure reports TransmissionFailure and continues queued messages cleanly") {
		val ioBoom = new IOException("async socket broken")
		val channel = new ControlledAsyncChannel(idx =>
			if idx == 0 then WriteMode.DeferredComplete() else WriteMode.InlineComplete()
		)
		val transmitter = new PipelinedTransmitter[Int](channel, newContext, capacityOfBufferInRing = 16, ringMaxLength = RingLength(8))
		val reports = new ArrayBuffer[(Int, PipelinedTransmitter.Report)]()

		given Serializer[Int] = (v, writer) => {
			writer.putInt(v)
		}

		transmitter.transmit(1, ProtocolVersion.OF_THIS_PROJECT)(r => reports.addOne(1 -> r))
		transmitter.transmit(2, ProtocolVersion.OF_THIS_PROJECT)(r => reports.addOne(2 -> r))

		val pending = channel.pollPending()
		assert(pending ne null)
		pending.nn.completeFailure(ioBoom)

		assertEquals(reports.toList, List((1, PipelinedTransmitter.TransmissionFailure(1, ioBoom)), (2, PipelinedTransmitter.Delivered)))
	}

	test("multi-threaded pipelined burst completes losslessly in FIFO order with single callback per message") {
		val ioExecutor = Executors.newFixedThreadPool(2)
		val producerExecutor = Executors.newFixedThreadPool(4)
		try {
			val rng = new Random(42L)
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
					val chunkSize = 1 + rng.nextInt(48)
					val written = consumeAndCapture(srcs, offset, length, chunkSize)
					val n = java.lang.Long.valueOf(written)
					if rng.nextInt(4) == 0 then handler.completed(n, attachment)
					else {
						ioExecutor.execute(() => {
							if rng.nextBoolean() then Thread.`yield`()
							handler.completed(n, attachment)
						})
					}
				}
			}

			val transmitter = new PipelinedTransmitter[Array[Byte]](asyncChannel, newContext, capacityOfBufferInRing = 16, ringMaxLength = RingLength(32), ringMinLengthOnIdle = RingLength(2), maxGatheringBuffers = 4)
			val totalMessages = 60
			val payloads = Array.tabulate(totalMessages)(i => {
				val body = sequentialPatternBytes(8 + (i % 13) * 11)
				val withId = new Array[Byte](4 + body.length)
				ByteBuffer.wrap(withId).putInt(i).put(body)
				withId
			})
			val callbackCounts = Array.fill(totalMessages)(new AtomicInteger(0))
			val reports = Array.fill[PipelinedTransmitter.Report | Null](totalMessages)(null)
			val doneLatch = new CountDownLatch(totalMessages)

			given Serializer[Array[Byte]] = (msg, writer) => {
				writer.putBytes(msg)
			}

			val startBarrier = new CyclicBarrier(4)
			for t <- 0 until 4 do {
				producerExecutor.execute(() => {
					startBarrier.await()
					var i = t
					while i < totalMessages do {
						val idx = i
						transmitter.transmit(payloads(idx), ProtocolVersion.OF_THIS_PROJECT) { report =>
							callbackCounts(idx).incrementAndGet()
							reports(idx) = report
							doneLatch.countDown()
						}
						i += 4
					}
				})
			}

			assert(doneLatch.await(15, TimeUnit.SECONDS), "Timed out waiting for multi-threaded pipelined burst")

			val idleLatch = new CountDownLatch(1)
			transmitter.triggerOnIdle(() => idleLatch.countDown())
			assert(idleLatch.await(5, TimeUnit.SECONDS), "Timed out waiting for triggerOnIdle")

			for i <- 0 until totalMessages do {
				assertEquals(callbackCounts(i).get(), 1, s"Expected single callback for message #$i")
				assertEquals(reports(i), PipelinedTransmitter.Delivered)
			}

			val decoded = decodeAllWirePackages(asyncChannel.writtenBytes)
			assertEquals(decoded.size, totalMessages)
			val seenIds = decoded.map(p => ByteBuffer.wrap(p.payload).getInt())
			assertEquals(seenIds.sorted, (0 until totalMessages).toList)
			for pkg <- decoded do {
				val id = ByteBuffer.wrap(pkg.payload).getInt()
				assertEquals(pkg.frameCount, 1)
				assertEquals(pkg.payload.toSeq, payloads(id).toSeq)
			}
			assertEquals(transmitter.bufferInRingTotalCount, 4)
		} finally {
			producerExecutor.shutdownNow()
			ioExecutor.shutdownNow()
		}
	}

	test("transmit and transmitWithAttachment throw RejectedExecutionException synchronously when queuedCount reaches MaxQueuedCount") {
		val channel = new ControlledAsyncChannel(_ => WriteMode.InlineComplete())
		val transmitter = new PipelinedTransmitter[Int](channel, newContext)
		val enteredSerialization = new CountDownLatch(1)
		val releaseSerialization = new CountDownLatch(1)
		val firstDone = new CountDownLatch(1)
		@volatile var firstReport: PipelinedTransmitter.Report | Null = null
		val rejectedReports = ArrayBuffer.empty[PipelinedTransmitter.Report]

		given Serializer[Int] = (msg, writer) => {
			if msg == 1 then {
				enteredSerialization.countDown()
				val released = releaseSerialization.await(5, TimeUnit.SECONDS)
				assert(released, "Timed out waiting to release serialization")
			}
			writer.putInt(msg)
		}

		val holderThread = new Thread(() => {
			transmitter.transmit(1, ProtocolVersion.OF_THIS_PROJECT) { report =>
				firstReport = report
				firstDone.countDown()
			}
		}, "ptx-overflow-holder")
		holderThread.start()

		try {
			val entered = enteredSerialization.await(5, TimeUnit.SECONDS)
			assert(entered, "Holder thread did not enter serialization")
			val stateField = classOf[PipelinedTransmitter[?]].getDeclaredField("coordinationState")
			stateField.setAccessible(true)
			val rawState = stateField.get(transmitter).asInstanceOf[AtomicInteger]

			val expectedSerializingBits = 1 << 28 // JointStageState.SerializingOnly
			val saturatedBits = expectedSerializingBits | 0x0fff_ffff
			assertEquals(rawState.get(), expectedSerializingBits)
			assert(rawState.compareAndSet(expectedSerializingBits, saturatedBits), "Failed to saturate queuedCount")

			try {
				intercept[RejectedExecutionException] {
					transmitter.transmit(2, ProtocolVersion.OF_THIS_PROJECT)(r => rejectedReports += r)
				}
				intercept[RejectedExecutionException] {
					transmitter.transmitWithAttachment(3, ProtocolVersion.OF_THIS_PROJECT, "att")((r, _) => rejectedReports += r)
				}
				assertEquals(rejectedReports.toList, Nil)
				assertEquals(rawState.get(), saturatedBits)
			} finally {
				assert(rawState.compareAndSet(saturatedBits, expectedSerializingBits), "Failed to restore queuedCount before draining")
			}

			releaseSerialization.countDown()
			val completed = firstDone.await(5, TimeUnit.SECONDS)
			assert(completed, "Timed out waiting for holder transmission")
			assertEquals(firstReport, PipelinedTransmitter.Delivered)
			assertEquals(rawState.get(), 0)
		} finally {
			releaseSerialization.countDown()
			holderThread.join(2000L)
		}
	}

	test("constructor rejects invalid capacity and batch parameters") {
		val channel = new ControlledAsyncChannel()
		intercept[IllegalArgumentException] {
			new PipelinedTransmitter[Int](channel, newContext, ringMaxLength = RingLength(0))
		}
		intercept[IllegalArgumentException] {
			new PipelinedTransmitter[Int](channel, newContext, ringMinLengthOnIdle = RingLength(0))
		}
		intercept[IllegalArgumentException] {
			new PipelinedTransmitter[Int](channel, newContext, streamTransferBufferCapacity = 0)
		}
		intercept[IllegalArgumentException] {
			new PipelinedTransmitter[Int](channel, newContext, maxGatheringBuffers = 1)
		}
	}

	test("zero-byte control payload omits frame header and transmits standalone sentinel cleanly") {
		val channel = new ControlledAsyncChannel(_ => WriteMode.InlineComplete())
		val transmitter = new PipelinedTransmitter[Array[Byte]](channel, newContext, capacityOfBufferInRing = 16, ringMaxLength = RingLength(4))
		val reports = new ArrayBuffer[PipelinedTransmitter.Report]()

		given Serializer[Array[Byte]] = (msg, writer) => {
			if msg.nonEmpty then writer.putBytes(msg)
		}

		transmitter.transmit(Array.emptyByteArray, ProtocolVersion.OF_THIS_PROJECT)(r => reports += r)
		assertEquals(reports.toList, List(PipelinedTransmitter.Delivered))
		assertEquals(channel.writtenBytes.toSeq, Seq(0.toByte))

		val followUp = Array[Byte](10, 20, 30)
		transmitter.transmit(followUp, ProtocolVersion.OF_THIS_PROJECT)(r => reports += r)
		assertEquals(reports.toList, List(PipelinedTransmitter.Delivered, PipelinedTransmitter.Delivered))

		val decoded = decodeAllWirePackages(channel.writtenBytes)
		assertEquals(decoded.size, 2)
		assertEquals(decoded(0).frameCount, 0)
		assertEquals(decoded(0).payload.length, 0)
		assertEquals(decoded(1).frameCount, 1)
		assertEquals(decoded(1).payload.toSeq, followUp.toSeq)
	}

	test("STREAM_FOLLOWS_SENTINEL spills into separate sentinelBuffer batch across maxGatheringBuffers boundary") {
		val channel = new ControlledAsyncChannel(_ => WriteMode.DeferredComplete())
		// 15 control bytes + 1 VLQ byte (size=7) = 16 bytes -> 0 tail room in 16-byte buffer; header(1) + content(1) = maxGatheringBuffers(2) forces sentinel into batch 2.
		val transmitter = new PipelinedTransmitter[(Array[Byte], ByteSource)](
			channel,
			newContext,
			capacityOfBufferInRing = 16,
			ringMaxLength = RingLength(8),
			ringMinLengthOnIdle = RingLength(1),
			streamTransferBufferCapacity = 4,
			maxGatheringBuffers = 2
		)
		val ctrl = sequentialPatternBytes(15)
		val streamBytes = sequentialPatternBytes(7)
		val reports = new ArrayBuffer[PipelinedTransmitter.Report]()

		given Serializer[(Array[Byte], ByteSource)] = (pair, writer) => {
			writer.putBytes(pair._1)
			writer.putByteSource(pair._2)
		}

		transmitter.transmit((ctrl, BufferByteSource(streamBytes)), ProtocolVersion.OF_THIS_PROJECT)(r => reports += r)
		assertEquals(reports.size, 0)
		val steps = channel.drainPending()
		assert(steps >= 4, s"Expected control batch + separate sentinel batch + multiple stream chunks, got $steps")
		assertEquals(channel.maxBatchLength, 2)
		assertEquals(reports.toList, List(PipelinedTransmitter.Delivered))

		val decoded = decodeAllWirePackages(channel.writtenBytes, readTrailingStreams = true)
		assertEquals(decoded.size, 1)
		assert(decoded.head.trailingStream)
		assert(!decoded.head.streamAborted)
		assertEquals(decoded.head.payload.take(15).toSeq, ctrl.toSeq)
		assertEquals(decoded.head.streamBytes.toSeq, streamBytes.toSeq)
	}

	test("multi-ByteSource trailing stream aborts mid-second-source under partial writes, drops remaining sources, suppresses close errors, and recovers") {
		val channel = new ControlledAsyncChannel(_ => WriteMode.DeferredComplete(maxBytesPerWrite = 3))
		val transmitter = new PipelinedTransmitter[(Int, List[ByteSource])](
			channel,
			newContext,
			capacityOfBufferInRing = 16,
			ringMaxLength = RingLength(8),
			ringMinLengthOnIdle = RingLength(1),
			streamTransferBufferCapacity = 4,
			maxGatheringBuffers = 3
		)

		val goodBytes = sequentialPatternBytes(10)
		val secondPrefix = sequentialPatternBytes(5)
		val failingCloseCount = new AtomicInteger(0)
		val unreachedOpenCount = new AtomicInteger(0)
		val readBoom = new IOException("second source boom")

		val failingSource = new ByteSource {
			override def size: Long = 20L
			override def openChannel(): java.nio.channels.ReadableByteChannel = throw new UnsupportedOperationException()
			override def transferTo(target: java.nio.channels.WritableByteChannel): Long = throw new UnsupportedOperationException()
			override def openAsyncReader(): ByteSource.AsyncReader = new ByteSource.AsyncReader {
				private var sentFirst = false
				override def read[A](dst: ByteBuffer, attachment: A, handler: CompletionHandler[Integer, ? >: A]): Int = {
					if !sentFirst then {
						sentFirst = true
						val n = math.min(dst.remaining(), secondPrefix.length)
						dst.put(secondPrefix, 0, n)
						n
					} else throw readBoom
				}
				override def close(): Unit = {
					failingCloseCount.incrementAndGet()
					throw new IOException("secondary reader close failure")
				}
			}
		}

		val unreachedSource = new ByteSource {
			override def size: Long = 10L
			override def openChannel(): java.nio.channels.ReadableByteChannel = throw new UnsupportedOperationException()
			override def transferTo(target: java.nio.channels.WritableByteChannel): Long = throw new UnsupportedOperationException()
			override def openAsyncReader(): ByteSource.AsyncReader = {
				unreachedOpenCount.incrementAndGet()
				BufferByteSource(sequentialPatternBytes(10)).openAsyncReader()
			}
		}

		val followUpStream = sequentialPatternBytes(6)
		val reports = new ArrayBuffer[(Int, PipelinedTransmitter.Report)]()

		given Serializer[(Int, List[ByteSource])] = (pair, writer) => {
			writer.putInt(pair._1)
			pair._2.foreach(writer.putByteSource)
		}

		transmitter.transmit((1, List(BufferByteSource(goodBytes), failingSource, unreachedSource)), ProtocolVersion.OF_THIS_PROJECT) { r =>
			reports.addOne(1 -> r)
		}
		transmitter.transmit((2, List(BufferByteSource(followUpStream))), ProtocolVersion.OF_THIS_PROJECT) { r =>
			reports.addOne(2 -> r)
		}

		channel.drainPending()
		assertEquals(reports.size, 2)
		assertEquals(reports(0)._1, 1)
		reports(0)._2 match {
			case PipelinedTransmitter.ByteSourceProblem(msg, cause) =>
				assertEquals(msg.asInstanceOf[(Int, List[ByteSource])]._1, 1)
				assertEquals(cause, readBoom)
			case other => fail(s"Expected ByteSourceProblem, got $other")
		}
		assertEquals(reports(1), (2, PipelinedTransmitter.Delivered))
		assertEquals(failingCloseCount.get(), 1)
		assertEquals(unreachedOpenCount.get(), 0)

		val decoded = decodeAllWirePackages(channel.writtenBytes, readTrailingStreams = true)
		assertEquals(decoded.size, 2)
		assert(decoded(0).trailingStream && decoded(0).streamAborted)
		assertEquals(decoded(0).streamBytes.take(goodBytes.length).toSeq, goodBytes.toSeq)
		assert(decoded(1).trailingStream && !decoded(1).streamAborted)
		assertEquals(decoded(1).streamBytes.toSeq, followUpStream.toSeq)
	}

	test("trailing stream handles AsyncReader.read returning 0 as premature EOF") {
		val channel = new ControlledAsyncChannel(_ => WriteMode.InlineComplete())
		val transmitter = new PipelinedTransmitter[ByteSource](channel, newContext, capacityOfBufferInRing = 16, ringMaxLength = RingLength(4), streamTransferBufferCapacity = 8)
		val reports = new ArrayBuffer[PipelinedTransmitter.Report]()

		val zeroReturnSource = new ByteSource {
			override def size: Long = 10L
			override def openChannel(): java.nio.channels.ReadableByteChannel = throw new UnsupportedOperationException()
			override def transferTo(target: java.nio.channels.WritableByteChannel): Long = throw new UnsupportedOperationException()
			override def openAsyncReader(): ByteSource.AsyncReader = new ByteSource.AsyncReader {
				override def read[A](dst: ByteBuffer, attachment: A, handler: CompletionHandler[Integer, ? >: A]): Int = 0
			}
		}

		given Serializer[ByteSource] = (src, writer) => writer.putByteSource(src)

		transmitter.transmit(zeroReturnSource, ProtocolVersion.OF_THIS_PROJECT)(r => reports += r)
		assertEquals(reports.size, 1)
		reports.head match {
			case PipelinedTransmitter.ByteSourceProblem(_, _: EOFException) => ()
			case other => fail(s"Expected ByteSourceProblem(EOFException), got $other")
		}
		val decoded = decodeAllWirePackages(channel.writtenBytes, readTrailingStreams = true)
		assertEquals(decoded.size, 1)
		assert(decoded.head.streamAborted)
	}

	test("trailing stream write failures (sync throw and async abort-sentinel write failure) report TransmissionFailure and close reader") {
		val streamSyncBoom = new IOException("sync stream write failure")
		val syncReaderClosed = new AtomicInteger(0)
		val syncChannel = new ControlledAsyncChannel(idx =>
			if idx == 1 then WriteMode.SyncThrow(streamSyncBoom) else WriteMode.InlineComplete()
		)
		val tx1 = new PipelinedTransmitter[List[ByteSource]](syncChannel, newContext, capacityOfBufferInRing = 16, ringMaxLength = RingLength(4), streamTransferBufferCapacity = 8)
		val reports1 = new ArrayBuffer[PipelinedTransmitter.Report]()

		def trackingSource(bytes: Array[Byte], closeCounter: AtomicInteger): ByteSource = new ByteSource {
			private val delegate = BufferByteSource(bytes)
			override def size: Long = delegate.size
			override def openChannel(): java.nio.channels.ReadableByteChannel = delegate.openChannel()
			override def transferTo(target: java.nio.channels.WritableByteChannel): Long = delegate.transferTo(target)
			override def openAsyncReader(): ByteSource.AsyncReader = {
				val inner = delegate.openAsyncReader()
				new ByteSource.AsyncReader {
					override def read[A](dst: ByteBuffer, attachment: A, handler: CompletionHandler[Integer, ? >: A]): Int = inner.read(dst, attachment, handler)
					override def close(): Unit = {
						closeCounter.incrementAndGet()
						inner.close()
					}
				}
			}
		}

		given Serializer[List[ByteSource]] = (srcs, writer) => {
			writer.putUnsignedIntVlq(srcs.size)
			srcs.foreach(writer.putByteSource)
		}

		tx1.transmit(List(trackingSource(sequentialPatternBytes(6), syncReaderClosed)), ProtocolVersion.OF_THIS_PROJECT)(r => reports1 += r)
		tx1.transmit(Nil, ProtocolVersion.OF_THIS_PROJECT)(r => reports1 += r)
		assertEquals(syncReaderClosed.get(), 1)
		assertEquals(reports1.size, 2)
		reports1(0) match {
			case PipelinedTransmitter.TransmissionFailure(_, cause) => assertEquals(cause, streamSyncBoom)
			case other => fail(s"Expected TransmissionFailure, got $other")
		}
		assertEquals(reports1(1), PipelinedTransmitter.Delivered)

		val sentinelBoom = new IOException("abort sentinel write failed")
		val abortReaderClosed = new AtomicInteger(0)
		val asyncFailChannel = new ControlledAsyncChannel(idx =>
			if idx == 1 then WriteMode.DeferredComplete() else WriteMode.InlineComplete()
		)
		val tx2 = new PipelinedTransmitter[List[ByteSource]](asyncFailChannel, newContext, capacityOfBufferInRing = 16, ringMaxLength = RingLength(4), streamTransferBufferCapacity = 8)
		val reports2 = new ArrayBuffer[PipelinedTransmitter.Report]()
		val abortingSource = new ByteSource {
			override def size: Long = 10L
			override def openChannel(): java.nio.channels.ReadableByteChannel = throw new UnsupportedOperationException()
			override def transferTo(target: java.nio.channels.WritableByteChannel): Long = throw new UnsupportedOperationException()
			override def openAsyncReader(): ByteSource.AsyncReader = new ByteSource.AsyncReader {
				override def read[A](dst: ByteBuffer, attachment: A, handler: CompletionHandler[Integer, ? >: A]): Int = throw new IOException("reader broken")
				override def close(): Unit = {
					abortReaderClosed.incrementAndGet()
					()
				}
			}
		}
		tx2.transmit(List(abortingSource), ProtocolVersion.OF_THIS_PROJECT)(r => reports2 += r)
		val pendingSentinel = asyncFailChannel.pollPending()
		assert(pendingSentinel ne null)
		pendingSentinel.nn.completeFailure(sentinelBoom)
		assertEquals(abortReaderClosed.get(), 1)
		assertEquals(reports2.size, 1)
		reports2.head match {
			case PipelinedTransmitter.TransmissionFailure(_, cause) => assertEquals(cause, sentinelBoom)
			case other => fail(s"Expected TransmissionFailure superseding ByteSourceProblem, got $other")
		}
	}

	test("partial control batch resubmission sync throw and write timeout propagation report TransmissionFailure") {
		val partialBoom = new IOException("partial control resubmit boom")
		val channel = new ControlledAsyncChannel(idx =>
			if idx == 0 then WriteMode.DeferredComplete(maxBytesPerWrite = 3)
			else if idx == 1 then WriteMode.SyncThrow(partialBoom)
			else WriteMode.InlineComplete()
		)
		val transmitter = new PipelinedTransmitter[(Array[Byte], Option[ByteSource])](channel, newContext, capacityOfBufferInRing = 16, ringMaxLength = RingLength(4), streamTransferBufferCapacity = 8)
		val reports = new ArrayBuffer[(Int, PipelinedTransmitter.Report)]()

		given Serializer[(Array[Byte], Option[ByteSource])] = (pair, writer) => {
			writer.putBytes(pair._1)
			pair._2.foreach(writer.putByteSource)
		}

		transmitter.transmitWithAttachment((sequentialPatternBytes(10), None), ProtocolVersion.OF_THIS_PROJECT, 1, timeout = 42L, timeUnit = TimeUnit.MILLISECONDS) { (r, id) =>
			reports.addOne(id -> r)
		}
		val p0 = channel.pollPending()
		assert(p0 ne null)
		p0.nn.completeSuccess()

		transmitter.transmitWithAttachment((sequentialPatternBytes(4), Some(BufferByteSource(sequentialPatternBytes(5)))), ProtocolVersion.OF_THIS_PROJECT, 2, timeout = 99L, timeUnit = TimeUnit.MICROSECONDS) { (r, id) =>
			reports.addOne(id -> r)
		}

		assertEquals(reports.size, 2)
		assertEquals(reports(0)._2.asInstanceOf[PipelinedTransmitter.TransmissionFailure].cause, partialBoom)
		assertEquals(reports(1)._2, PipelinedTransmitter.Delivered)
		assert(channel.observedTimeouts.take(2).forall(_ == (42L, TimeUnit.MILLISECONDS)))
		assert(channel.observedTimeouts.drop(2).forall(_ == (99L, TimeUnit.MICROSECONDS)))

		val timeoutChan = new ControlledAsyncChannel(_ => WriteMode.InlineFailed(new InterruptedByTimeoutException()))
		val timeoutTx = new PipelinedTransmitter[Int](timeoutChan, newContext)
		val timeoutReports = new ArrayBuffer[PipelinedTransmitter.Report]()
		given Serializer[Int] = (v, w) => w.putInt(v)
		timeoutTx.transmit(1, ProtocolVersion.OF_THIS_PROJECT, timeout = 5L, timeUnit = TimeUnit.MILLISECONDS)(r => timeoutReports += r)
		assert(timeoutReports.head.asInstanceOf[PipelinedTransmitter.TransmissionFailure].cause.isInstanceOf[InterruptedByTimeoutException])
	}

	test("pruneTo honors idle vs active state, releases streamTransferBuffer, preserves triggerOnIdle composition order, and handles re-entrant transmit in onIdle") {
		val channel = new ControlledAsyncChannel(_ => WriteMode.DeferredComplete())
		val transmitter = new PipelinedTransmitter[(Array[Byte], Option[ByteSource])](
			channel,
			newContext,
			capacityOfBufferInRing = 16,
			ringMaxLength = RingLength(16),
			ringMinLengthOnIdle = RingLength(2),
			streamTransferBufferCapacity = 8
		)
		val events = new ArrayBuffer[String]()

		given Serializer[(Array[Byte], Option[ByteSource])] = (pair, writer) => {
			writer.putBytes(pair._1)
			pair._2.foreach(writer.putByteSource)
		}

		transmitter.transmit((sequentialPatternBytes(48), Some(BufferByteSource(sequentialPatternBytes(12)))), ProtocolVersion.OF_THIS_PROJECT) { r =>
			events += s"msg1:$r"
		}
		val activeCount = transmitter.bufferInRingTotalCount
		assert(activeCount >= 4)
		assertEquals(transmitter.pruneTo(RingLength(1)), activeCount, "pruneTo must not prune while active")

		transmitter.triggerOnIdle(() => events += "idle1")
		transmitter.triggerOnIdle(() => events += "idle2")
		assert(!events.exists(_.startsWith("idle")))

		channel.drainPending()
		assertEquals(events.toList, List("idle1", "idle2", "msg1:Delivered"))
		assertEquals(transmitter.bufferInRingTotalCount, 3, "Transmitted stage prunes to ringMinLengthOnIdle=2 while untouched stage retains 1 initial buffer")

		assertEquals(transmitter.pruneTo(RingLength(1)), 2)
		assertEquals(transmitter.bufferInRingTotalCount, 2)
		val transferField = classOf[PipelinedTransmitter[?]].getDeclaredField("streamTransferBuffer")
		transferField.setAccessible(true)
		assert(transferField.get(transmitter) eq null, "pruneTo must release streamTransferBuffer")

		val reentrantDone = new ArrayBuffer[String]()
		transmitter.triggerOnIdle(() => {
			reentrantDone += "outerIdle"
			transmitter.transmit((sequentialPatternBytes(6), Some(BufferByteSource(sequentialPatternBytes(10)))), ProtocolVersion.OF_THIS_PROJECT) { r =>
				reentrantDone += s"reentrant:$r"
			}
		})
		transmitter.triggerOnIdle(() => reentrantDone += "postReentrantIdle")
		channel.drainPending()
		assertEquals(reentrantDone.toList, List("outerIdle", "postReentrantIdle", "reentrant:Delivered"))
		assert(transferField.get(transmitter) eq null, "Case D transition to idle must release lazily reallocated streamTransferBuffer")
	}

	test("exception thrown by onComplete callback does not stall subsequent queued transmissions") {
		val channel = new ControlledAsyncChannel(idx =>
			if idx == 0 then WriteMode.DeferredComplete() else WriteMode.InlineComplete()
		)
		val transmitter = new PipelinedTransmitter[Int](channel, newContext, capacityOfBufferInRing = 16, ringMaxLength = RingLength(8))
		val reports = new ArrayBuffer[(Int, PipelinedTransmitter.Report)]()
		given Serializer[Int] = (v, w) => w.putInt(v)

		transmitter.transmit(1, ProtocolVersion.OF_THIS_PROJECT) { r =>
			reports.addOne(1 -> r)
			throw new RuntimeException("user callback boom")
		}
		transmitter.transmit(2, ProtocolVersion.OF_THIS_PROJECT)(r => reports.addOne(2 -> r))
		transmitter.transmit(3, ProtocolVersion.OF_THIS_PROJECT)(r => reports.addOne(3 -> r))

		val pending = channel.pollPending()
		assert(pending ne null)
		val boom = intercept[RuntimeException] {
			pending.nn.completeSuccess()
		}
		assertEquals(boom.getMessage, "user callback boom")
		assertEquals(reports.toList, List((1, PipelinedTransmitter.Delivered), (2, PipelinedTransmitter.Delivered), (3, PipelinedTransmitter.Delivered)))
		assertEquals(decodeAllWirePackages(channel.writtenBytes).size, 3)
	}

	test("forced CAS contention races across serialization/transmission handoff, Case D, pruneTo, triggerOnIdle, and pre-return AsyncReader completion") {
		val executor = Executors.newFixedThreadPool(4)
		try {
			// 1. Race runSerializationLoop handoff against onTransmissionFinished Case C and Case D vs Empty->Serializing.
			val channel1 = new ControlledAsyncChannel(_ => WriteMode.DeferredComplete())
			val tx1 = new PipelinedTransmitter[Int](channel1, newContext, capacityOfBufferInRing = 16, ringMaxLength = RingLength(8), ringMinLengthOnIdle = RingLength(1))
			val barrier = new CyclicBarrier(2)

			@volatile var delayMsg2 = false
			val msg2InSer = new AtomicBoolean(false)
			given Serializer[Int] = (v, w) => {
				w.putInt(v)
				if delayMsg2 && v == 2 then {
					msg2InSer.set(true)
					barrier.await(5, TimeUnit.SECONDS)
				}
			}

			for round <- 0 until 200 do {
				val latch = new CountDownLatch(3)
				val rMap = new ConcurrentHashMap[Int, PipelinedTransmitter.Report]()
				delayMsg2 = true
				msg2InSer.set(false)

				tx1.transmit(1, ProtocolVersion.OF_THIS_PROJECT) { r => rMap.put(1, r); latch.countDown() }
				val f2 = executor.submit(new Runnable {
					override def run(): Unit = tx1.transmit(2, ProtocolVersion.OF_THIS_PROJECT) { r => rMap.put(2, r); latch.countDown() }
				})
				while !msg2InSer.get() do Thread.`yield`()

				val p1 = channel1.pollPending()
				assert(p1 ne null)
				barrier.await(5, TimeUnit.SECONDS)
				p1.nn.completeSuccess()
				f2.get(5, TimeUnit.SECONDS)

				// Now race draining p2 (Case D -> Idle) with submitting msg 3 (Empty -> Serializing).
				var p2 = channel1.pollPending()
				while p2 eq null do {
					Thread.`yield`()
					p2 = channel1.pollPending()
				}
				val raceSubmit = new CyclicBarrier(2)
				val f3 = executor.submit(new Runnable {
					override def run(): Unit = {
						raceSubmit.await(5, TimeUnit.SECONDS)
						tx1.transmit(3, ProtocolVersion.OF_THIS_PROJECT) { r => rMap.put(3, r); latch.countDown() }
					}
				})
				raceSubmit.await(5, TimeUnit.SECONDS)
				p2.nn.completeSuccess()
				f3.get(5, TimeUnit.SECONDS)
				while latch.getCount > 0 do {
					val w = channel1.pollPending()
					if w ne null then w.nn.completeSuccess()
					else Thread.`yield`()
				}
				assert(latch.await(5, TimeUnit.SECONDS), s"Lost callback in round $round")
				assertEquals(rMap.get(1), PipelinedTransmitter.Delivered)
				assertEquals(rMap.get(2), PipelinedTransmitter.Delivered)
				assertEquals(rMap.get(3), PipelinedTransmitter.Delivered)
			}

			// 2. Race pruneTo(1), transmit, and triggerOnIdle simultaneously.
			val channel2 = new ControlledAsyncChannel(_ => WriteMode.InlineComplete())
			val tx2 = new PipelinedTransmitter[Array[Byte]](channel2, newContext, capacityOfBufferInRing = 16, ringMaxLength = RingLength(16), ringMinLengthOnIdle = RingLength(2))
			given Serializer[Array[Byte]] = (bytes, w) => w.putBytes(bytes)
			val threeBarrier = new CyclicBarrier(3)
			for round <- 0 until 200 do {
				// Grow buffers before racing pruneTo so pruneTo holds Pruning state long enough to collide with transmit & triggerOnIdle.
				tx2.transmit(sequentialPatternBytes(60), ProtocolVersion.OF_THIS_PROJECT)(_ => ())
				val idleLatch = new CountDownLatch(1)
				val txLatch = new CountDownLatch(1)
				@volatile var rep: PipelinedTransmitter.Report | Null = null
				val fPrune = executor.submit(new Runnable {
					override def run(): Unit = {
						threeBarrier.await(5, TimeUnit.SECONDS)
						tx2.pruneTo(RingLength(1))
						()
					}
				})
				val fTx = executor.submit(new Runnable {
					override def run(): Unit = {
						threeBarrier.await(5, TimeUnit.SECONDS)
						tx2.transmit(sequentialPatternBytes(8), ProtocolVersion.OF_THIS_PROJECT) { r =>
							rep = r
							txLatch.countDown()
						}
					}
				})
				threeBarrier.await(5, TimeUnit.SECONDS)
				tx2.triggerOnIdle(() => idleLatch.countDown())
				fPrune.get(5, TimeUnit.SECONDS)
				fTx.get(5, TimeUnit.SECONDS)
				assert(txLatch.await(5, TimeUnit.SECONDS), s"pruneTo race stalled transmit in round $round")
				assert(idleLatch.await(5, TimeUnit.SECONDS), s"triggerOnIdle missed idle transition in round $round")
				assertEquals(rep, PipelinedTransmitter.Delivered)
			}

			// 3. Cross-thread AsyncReader.read completion executing runTrailingStreamLoop BEFORE read() returns PENDING.
			val channel3 = new ControlledAsyncChannel(_ => WriteMode.InlineComplete())
			val tx3 = new PipelinedTransmitter[ByteSource](channel3, newContext, capacityOfBufferInRing = 16, ringMaxLength = RingLength(4), streamTransferBufferCapacity = 5)
			given Serializer[ByteSource] = (s, w) => w.putByteSource(s)
			val fullStream = sequentialPatternBytes(37)
			val earlyCompletionSource = new ByteSource {
				override def size: Long = fullStream.length.toLong
				override def openChannel(): java.nio.channels.ReadableByteChannel = throw new UnsupportedOperationException()
				override def transferTo(target: java.nio.channels.WritableByteChannel): Long = throw new UnsupportedOperationException()
				override def openAsyncReader(): ByteSource.AsyncReader = new ByteSource.AsyncReader {
					private var offset = 0
					override def read[A](dst: ByteBuffer, attachment: A, handler: CompletionHandler[Integer, ? >: A]): Int = {
						val workerRan = new CountDownLatch(1)
						val rem = fullStream.length - offset
						if rem <= 0 then {
							executor.execute(() => {
								workerRan.countDown()
								LockSupport.parkNanos(20_000L)
								handler.completed(Integer.valueOf(-1), attachment)
							})
						} else {
							val n = math.min(dst.remaining(), rem)
							dst.put(fullStream, offset, n)
							offset += n
							executor.execute(() => {
								workerRan.countDown()
								handler.completed(Integer.valueOf(n), attachment)
							})
						}
						assert(workerRan.await(5, TimeUnit.SECONDS))
						LockSupport.parkNanos(50_000L)
						ByteSource.AsyncReader.PENDING
					}
				}
			}
			val streamDone = new CountDownLatch(1)
			@volatile var streamRep: PipelinedTransmitter.Report | Null = null
			tx3.transmit(earlyCompletionSource, ProtocolVersion.OF_THIS_PROJECT) { r =>
				streamRep = r
				streamDone.countDown()
			}
			assert(streamDone.await(5, TimeUnit.SECONDS))
			assertEquals(streamRep, PipelinedTransmitter.Delivered)
			val dec3 = decodeAllWirePackages(channel3.writtenBytes, readTrailingStreams = true)
			assertEquals(dec3.size, 1)
			assertEquals(dec3.head.streamBytes.toSeq, fullStream.toSeq)
		} finally {
			executor.shutdownNow()
		}
	}
}
