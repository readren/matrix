package readren.nexus
package cluster.channel

import cluster.channel.PipelinedTransmitter.*
import cluster.misc.VLQ
import cluster.serialization.CommonSerializers.given
import cluster.serialization.Serializer.SerializationException
import cluster.serialization.{AllocationLimits, Deserializer, ProtocolVersion, Serializer}
import cluster.service.{ChannelId, ChannelOrigin}

import munit.ScalaCheckSuite
import org.scalacheck.{Gen, Prop, Test}
import readren.common.{BufferByteSource, ByteSink as CommonByteSink, ByteSource, FileByteSource, Maybe}

import java.io.{ByteArrayOutputStream, EOFException, IOException}
import java.net.{InetSocketAddress, SocketAddress, SocketOption, StandardSocketOptions}
import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousChannelGroup, AsynchronousServerSocketChannel, AsynchronousSocketChannel, CompletionHandler, InterruptedByTimeoutException}
import java.nio.file.{Files, Path}
import java.util.{Arrays, Collections}
import java.util.concurrent.*
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.locks.LockSupport
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.*
import scala.util.Random
import scala.util.control.NonFatal

/** Property-based and adversarial concurrency suite verifying [[PipelinedTransmitter]] end-to-end against [[Receiver]] over non-mocked loopback [[java.nio.channels.AsynchronousSocketChannel]] pairs with minimal and near-minimal buffer sizes. */
class PipelinedTransmitterLoopbackPbtSuite extends ScalaCheckSuite {

	override def scalaCheckTestParameters: Test.Parameters =
		super.scalaCheckTestParameters.withMinSuccessfulTests(20).withMaxDiscardRatio(10)

	private enum SerializationBehavior {
		case Normal
		case ThrowSerExMidway(prefix: Array[Byte])
		case ThrowNegativeByteSourceSize(prefix: Array[Byte])
		case ExceedMaxBuffers(overflowBytes: Int)
		case ThrowNonFatalMidway(prefix: Array[Byte])
	}

	private enum AsyncReadScheduling {
		case AlwaysSync
		case AlwaysInlineCallback
		case AlwaysExecutorCallback
		case Alternating(seed: Long)
	}

	private enum StreamAbortKind {
		case OpenThrows
		case SyncThrowAfter(bytesBeforeFail: Int)
		case AsyncFailAfter(bytesBeforeFail: Int, inline: Boolean)
		case PrematureEofAfter(bytesBeforeFail: Int)
		case SizeDriftOver(extraBytes: Int)
	}

	private enum StreamSpec {
		case Empty
		case InlineBuffer(payload: Array[Byte])
		case DiskFile(payload: Array[Byte])
		case AdversarialAsync(payload: Array[Byte], maxChunk: Int, scheduling: AsyncReadScheduling, cpuYieldNanos: Int)
		case AbortingStream(declaredSize: Int, maxChunk: Int, abortKind: StreamAbortKind, seed: Long)
	}

	private case class EscapeTag(marker: Byte, payload: Int)

	private given Serializer[EscapeTag] = (value: EscapeTag, writer: Serializer.Writer) => {
		writer.putByte(value.marker)
		writer.putInt(value.payload)
	}

	private given Deserializer[EscapeTag] = (reader: Deserializer.Reader) => {
		val marker = reader.readByte()
		val payload = reader.readInt()
		EscapeTag(marker, payload)
	}

	private case class WireEnvelope(
		producerId: Int,
		seq: Int,
		versionTag: Int,
		controlPayload: Array[Byte],
		dedupFirst: EscapeTag,
		dedupSecond: EscapeTag,
		streams: List[ByteSource],
		expectedStreams: List[Array[Byte]] = Nil,
		serializationDelayNanos: Long = 0L,
		serializationBehavior: SerializationBehavior = SerializationBehavior.Normal
	)

	private given Serializer[WireEnvelope] = (msg: WireEnvelope, writer: Serializer.Writer) => {
		msg.serializationBehavior match {
			case SerializationBehavior.Normal =>
				if msg.serializationDelayNanos > 0L then LockSupport.parkNanos(msg.serializationDelayNanos)
				writer.putInt(msg.producerId)
				writer.putInt(msg.seq)
				writer.putInt(msg.versionTag)
				writer.putInt(writer.governingVersion.identifier)
				writer.putUnsignedIntVlq(msg.controlPayload.length)
				writer.putBytes(msg.controlPayload)
				writer.write(msg.dedupFirst)
				writer.write(msg.dedupSecond)
				writer.putUnsignedIntVlq(msg.streams.size)
				msg.streams.foreach(bs => writer.putByteSource(bs))
			case SerializationBehavior.ThrowSerExMidway(prefix) =>
				writer.putBytes(prefix)
				throw new SerializationException(writer.position, "Injected mid-serialization failure")
			case SerializationBehavior.ThrowNegativeByteSourceSize(prefix) =>
				writer.putBytes(prefix)
				val invalidSource = new ByteSource {
					override def size: Long = -1L
					override def openAsyncReader(): ByteSource.AsyncReader = throw new AssertionError("Should never open invalid size source")
					override def openChannel(): java.nio.channels.ReadableByteChannel = throw new AssertionError("Should never open invalid size source")
					override def transferTo(target: java.nio.channels.WritableByteChannel): Long = throw new AssertionError("Should never transfer invalid size source")
				}
				writer.putByteSource(invalidSource)
			case SerializationBehavior.ExceedMaxBuffers(overflowBytes) =>
				val chunk = new Array[Byte](overflowBytes)
				writer.putBytes(chunk)
			case SerializationBehavior.ThrowNonFatalMidway(prefix) =>
				writer.putBytes(prefix)
				throw new IllegalStateException("Injected non-fatal serialization-stage exception")
		}
	}

	private given Deserializer[WireEnvelope] = (reader: Deserializer.Reader) => {
		val producerId = reader.readInt()
		val seq = reader.readInt()
		val versionTag = reader.readInt()
		val encodedVersion = reader.readInt()
		if encodedVersion != reader.governingVersion.identifier then {
			throw new IllegalStateException(s"ProtocolVersion mismatch: encoded $encodedVersion != reader.governingVersion ${reader.governingVersion.identifier}")
		}
		val payloadLen = reader.validateArrayLength(reader.readUnsignedIntVlq(), 1, "controlPayload")
		val payload = reader.readBytes(payloadLen)
		val first = reader.read[EscapeTag]
		val second = reader.read[EscapeTag]
		val streamCount = reader.validateCollectionLength(reader.readUnsignedIntVlq(), 1, "streams")
		val builder = List.newBuilder[ByteSource]
		var i = 0
		while i < streamCount do {
			builder.addOne(reader.readByteSource())
			i += 1
		}
		WireEnvelope(producerId, seq, versionTag, payload, first, second, builder.result())
	}

	private enum ChannelGroupMode {
		case SystemDefault
		case FixedPool(threads: Int)
	}

	private enum SinkCompletionMode {
		case DefaultAllocator(inMemoryThreshold: Int)
		case AdversarialAsync(inMemoryThreshold: Int, seed: Long)
	}

	private case class LoopbackConfig(
		txBuffersCapacity: Int,
		txMaxBuffers: Int,
		txMinBuffersOnIdle: Int,
		txStreamTransferBufferCapacity: Int,
		txMaxGatheringBuffers: Int,
		rxBuffersCapacity: Int,
		rxMaxBuffers: Int,
		rxMinBuffersOnIdle: Int,
		rxStreamTransferBufferCapacity: Int,
		soSndBuf: Int,
		soRcvBuf: Int,
		tcpNoDelay: Boolean,
		groupMode: ChannelGroupMode,
		sinkMode: SinkCompletionMode
	)

	private def daemonThreadFactory(prefix: String): ThreadFactory = {
		val counter = new AtomicInteger(0)
		(r: Runnable) => {
			val t = new Thread(r, s"$prefix-${counter.incrementAndGet()}")
			t.setDaemon(true)
			t
		}
	}

	private def configureSocketOptions(channel: AsynchronousSocketChannel, cfg: LoopbackConfig): Unit = {
		try channel.setOption(StandardSocketOptions.SO_SNDBUF, Integer.valueOf(cfg.soSndBuf)) catch { case NonFatal(_) => () }
		try channel.setOption(StandardSocketOptions.SO_RCVBUF, Integer.valueOf(cfg.soRcvBuf)) catch { case NonFatal(_) => () }
		try channel.setOption(StandardSocketOptions.TCP_NODELAY, java.lang.Boolean.valueOf(cfg.tcpNoDelay)) catch { case NonFatal(_) => () }
	}

	private def makeContext(localAddr: SocketAddress, peerAddr: SocketAddress, origin: ChannelOrigin): PipelinedTransmitter.Context =
		new PipelinedTransmitter.Context {
			override def myAddress: SocketAddress = localAddr
			override def oPeerAddress: Maybe[SocketAddress] = Maybe.some(peerAddr)
			override def channelId: ChannelId = new ChannelId(origin, Maybe.some(localAddr))
		}

	private def makeReceiverContext(localAddr: SocketAddress, peerAddr: SocketAddress, origin: ChannelOrigin): Receiver.Context =
		new Receiver.Context {
			override def myAddress: SocketAddress = localAddr
			override def oPeerAddress: Maybe[SocketAddress] = Maybe.some(peerAddr)
			override def channelId: ChannelId = new ChannelId(origin, Maybe.some(localAddr))
		}

	/** Creates an adversarial [[ByteSink.Allocator]] that exercises synchronous, inline asynchronous, and executor-dispatched asynchronous [[ByteSink.write]] completions alongside real disk-backed [[FileByteSink]] staging. */
	private def makeSinkAllocator(mode: SinkCompletionMode, bgExecutor: ExecutorService): CommonByteSink.Allocator =
		mode match {
			case SinkCompletionMode.DefaultAllocator(threshold) =>
				new CommonByteSink.DefaultAllocator(inMemoryThreshold = threshold, tempFilePrefix = "ptx-pbt-rx-", tempFileSuffix = ".bin")
			case SinkCompletionMode.AdversarialAsync(threshold, seed) =>
				val base = new CommonByteSink.DefaultAllocator(inMemoryThreshold = threshold, tempFilePrefix = "ptx-pbt-rx-", tempFileSuffix = ".bin")
				val rng = new Random(seed)
				(expectedSize: Long) => {
					val delegate = base.allocate(expectedSize)
					if expectedSize <= threshold then {
						new CommonByteSink {
							override def byteSource: ByteSource = delegate.byteSource
							override def expectedSize: Long = delegate.expectedSize
							override def seal(): Unit = delegate.seal()
							override def discard(): Unit = delegate.discard()
							override def write[A](src: ByteBuffer, attachment: A, handler: CompletionHandler[Integer, ? >: A]): Int = {
								val written = delegate.write(src, null, noopIntHandler)
								val pick = rng.synchronized(rng.nextInt(3))
								if pick == 0 then written
								else if pick == 1 then {
									handler.completed(Integer.valueOf(written), attachment)
									CommonByteSink.PENDING
								} else {
									bgExecutor.execute(() => handler.completed(Integer.valueOf(written), attachment))
									CommonByteSink.PENDING
								}
							}
						}
					} else {
						delegate
					}
				}
		}

	private object noopIntHandler extends CompletionHandler[Integer, Null] {
		override def completed(result: Integer, attachment: Null): Unit = ()
		override def failed(exc: Throwable, attachment: Null): Unit = ()
	}

	private def materializeStream(spec: StreamSpec, tempDir: Path, bgExecutor: ExecutorService): ByteSource =
		spec match {
			case StreamSpec.Empty =>
				BufferByteSource(Array.emptyByteArray)
			case StreamSpec.InlineBuffer(payload) =>
				BufferByteSource(payload)
			case StreamSpec.DiskFile(payload) =>
				val path = Files.createTempFile(tempDir, "ptx-pbt-tx-", ".bin")
				Files.write(path, payload)
				new FileByteSource(path, knownSize = payload.length.toLong, deleteOnClose = true)
			case StreamSpec.AdversarialAsync(payload, maxChunk, scheduling, cpuYieldNanos) =>
				new ByteSource {
					override def size: Long = payload.length.toLong
					override def openChannel(): java.nio.channels.ReadableByteChannel =
						BufferByteSource(payload).openChannel()
					override def transferTo(target: java.nio.channels.WritableByteChannel): Long =
						BufferByteSource(payload).transferTo(target)
					override def openAsyncReader(): ByteSource.AsyncReader = new ByteSource.AsyncReader {
						private var offset = 0
						private val rng = scheduling match {
							case AsyncReadScheduling.Alternating(s) => new Random(s)
							case _ => null
						}
						private var step = 0

						override def read[A](dst: ByteBuffer, attachment: A, handler: CompletionHandler[Integer, ? >: A]): Int = {
							if cpuYieldNanos > 0 then LockSupport.parkNanos(cpuYieldNanos.toLong)
							val remaining = payload.length - offset
							if remaining <= 0 then {
								dispatchReadCompletion(-1, attachment, handler)
							} else {
								val toRead = math.min(math.min(dst.remaining(), remaining), math.max(1, maxChunk))
								dst.put(payload, offset, toRead)
								offset += toRead
								dispatchReadCompletion(toRead, attachment, handler)
							}
						}

						private def dispatchReadCompletion[A](result: Int, attachment: A, handler: CompletionHandler[Integer, ? >: A]): Int = {
							val dispatchKind = scheduling match {
								case AsyncReadScheduling.AlwaysSync => 0
								case AsyncReadScheduling.AlwaysInlineCallback => 1
								case AsyncReadScheduling.AlwaysExecutorCallback => 2
								case AsyncReadScheduling.Alternating(_) =>
									val k = (step + rng.nextInt(3)) % 3
									step += 1
									k
							}
							if dispatchKind == 0 then result
							else if dispatchKind == 1 then {
								handler.completed(Integer.valueOf(result), attachment)
								ByteSource.AsyncReader.PENDING
							} else {
								bgExecutor.execute(() => handler.completed(Integer.valueOf(result), attachment))
								ByteSource.AsyncReader.PENDING
							}
						}
					}
				}
			case StreamSpec.AbortingStream(declaredSize, maxChunk, abortKind, seed) =>
				new ByteSource {
					override def size: Long = declaredSize.toLong
					override def openChannel(): java.nio.channels.ReadableByteChannel = throw new UnsupportedOperationException()
					override def transferTo(target: java.nio.channels.WritableByteChannel): Long = throw new UnsupportedOperationException()
					override def openAsyncReader(): ByteSource.AsyncReader = abortKind match {
						case StreamAbortKind.OpenThrows =>
							throw new IOException("Injected openAsyncReader failure")
						case _ =>
							new ByteSource.AsyncReader {
								private var sent = 0
								private val rng = new Random(seed)

								override def read[A](dst: ByteBuffer, attachment: A, handler: CompletionHandler[Integer, ? >: A]): Int = abortKind match {
									case StreamAbortKind.OpenThrows =>
										throw new AssertionError("Unreachable")
									case StreamAbortKind.SyncThrowAfter(limit) =>
										if sent >= limit then throw new IOException("Injected synchronous read failure")
										val toWrite = math.min(math.min(dst.remaining(), limit - sent), math.max(1, maxChunk))
										var i = 0
										while i < toWrite do {
											dst.put(((sent + i) & 0xff).toByte)
											i += 1
										}
										sent += toWrite
										toWrite
									case StreamAbortKind.AsyncFailAfter(limit, inline) =>
										if sent >= limit then {
											val exc = new IOException("Injected asynchronous read failure")
											if inline then handler.failed(exc, attachment)
											else bgExecutor.execute(() => handler.failed(exc, attachment))
											ByteSource.AsyncReader.PENDING
										} else {
											val toWrite = math.min(math.min(dst.remaining(), limit - sent), math.max(1, maxChunk))
											var i = 0
											while i < toWrite do {
												dst.put(((sent + i) & 0xff).toByte)
												i += 1
											}
											sent += toWrite
											if rng.nextBoolean() then toWrite
											else {
												bgExecutor.execute(() => handler.completed(Integer.valueOf(toWrite), attachment))
												ByteSource.AsyncReader.PENDING
											}
										}
									case StreamAbortKind.PrematureEofAfter(limit) =>
										if sent >= limit then -1
										else {
											val toWrite = math.min(math.min(dst.remaining(), limit - sent), math.max(1, maxChunk))
											var i = 0
											while i < toWrite do {
												dst.put(((sent + i) & 0xff).toByte)
												i += 1
											}
											sent += toWrite
											toWrite
										}
									case StreamAbortKind.SizeDriftOver(extra) =>
										val totalToEmit = declaredSize + math.max(1, extra)
										if sent >= totalToEmit then -1
										else {
											val toWrite = math.min(math.min(dst.remaining(), totalToEmit - sent), math.max(1, maxChunk))
											var i = 0
											while i < toWrite do {
												dst.put(((sent + i) & 0xff).toByte)
												i += 1
											}
											sent += toWrite
											toWrite
										}
								}
							}
					}
				}
		}

	private def expectedBytesOf(spec: StreamSpec): Array[Byte] =
		spec match {
			case StreamSpec.Empty => Array.emptyByteArray
			case StreamSpec.InlineBuffer(payload) => payload
			case StreamSpec.DiskFile(payload) => payload
			case StreamSpec.AdversarialAsync(payload, _, _, _) => payload
			case StreamSpec.AbortingStream(_, _, _, _) => Array.emptyByteArray
		}

	private def readAndCloseByteSource(bs: ByteSource): Array[Byte] = {
		val expectedLen = bs.size.toInt
		val out = new Array[Byte](expectedLen)
		val buf = ByteBuffer.wrap(out)
		val ch = bs.openChannel()
		try {
			while buf.hasRemaining do {
				val n = ch.read(buf)
				if n < 0 then throw new EOFException(s"Premature EOF reading received ByteSource at ${buf.position()} / $expectedLen")
			}
			val extra = ByteBuffer.allocate(1)
			val eofCheck = ch.read(extra)
			assert(eofCheck == -1, s"Expected EOF after $expectedLen bytes, got read=$eofCheck")
			out
		} finally {
			try ch.close() catch { case NonFatal(_) => () }
			try bs.close() catch { case NonFatal(_) => () }
		}
	}

	private def deleteTree(path: Path): Unit = {
		try {
			if Files.exists(path) then {
				val stream = Files.walk(path)
				try {
					val paths = stream.sorted(java.util.Comparator.reverseOrder()).iterator()
					while paths.hasNext do {
						try Files.deleteIfExists(paths.next()) catch { case NonFatal(_) => () }
					}
				} finally {
					stream.close()
				}
			}
		} catch {
			case NonFatal(_) => ()
		}
	}

	/** Opens a real non-mocked TCP loopback pair using [[AsynchronousServerSocketChannel]] and [[AsynchronousSocketChannel]], runs `fn`, and cleans up all resources. */
	private def withLoopbackPair[A](cfg: LoopbackConfig)(
		fn: (PipelinedTransmitter[WireEnvelope], Receiver, AsynchronousSocketChannel, AsynchronousSocketChannel, ExecutorService, Path) => A
	): A = {
		val tempDir = Files.createTempDirectory("ptx-pbt-")
		val bgExecutor = Executors.newFixedThreadPool(4, daemonThreadFactory("ptx-pbt-async"))
		val groupExecutor: ExecutorService | Null = cfg.groupMode match {
			case ChannelGroupMode.SystemDefault => null
			case ChannelGroupMode.FixedPool(threads) => Executors.newFixedThreadPool(threads, daemonThreadFactory("ptx-pbt-nio"))
		}
		val group: AsynchronousChannelGroup | Null =
			if groupExecutor ne null then AsynchronousChannelGroup.withThreadPool(groupExecutor)
			else null

		val serverChannel =
			if group ne null then AsynchronousServerSocketChannel.open(group)
			else AsynchronousServerSocketChannel.open()
		serverChannel.bind(new InetSocketAddress("127.0.0.1", 0))
		val port = serverChannel.getLocalAddress.asInstanceOf[InetSocketAddress].getPort

		val clientChannel =
			if group ne null then AsynchronousSocketChannel.open(group)
			else AsynchronousSocketChannel.open()
		var serverConnection: AsynchronousSocketChannel | Null = null

		try {
			val acceptFuture = serverChannel.accept()
			val connectFuture = clientChannel.connect(new InetSocketAddress("127.0.0.1", port))
			connectFuture.get(5, TimeUnit.SECONDS)
			serverConnection = acceptFuture.get(5, TimeUnit.SECONDS)
			val accepted = serverConnection.nn

			configureSocketOptions(clientChannel, cfg)
			configureSocketOptions(accepted, cfg)

			val tx = new PipelinedTransmitter[WireEnvelope](
				clientChannel,
				makeContext(clientChannel.getLocalAddress, accepted.getLocalAddress, ChannelOrigin.INITIATED),
				buffersCapacity = cfg.txBuffersCapacity,
				maxBuffers = cfg.txMaxBuffers,
				minBuffersOnIdle = cfg.txMinBuffersOnIdle,
				streamTransferBufferCapacity = cfg.txStreamTransferBufferCapacity,
				maxGatheringBuffers = cfg.txMaxGatheringBuffers
			)
			val rx = new Receiver(
				accepted,
				makeReceiverContext(accepted.getLocalAddress, clientChannel.getLocalAddress, ChannelOrigin.ACCEPTED),
				buffersCapacity = cfg.rxBuffersCapacity,
				maxBuffers = cfg.rxMaxBuffers,
				minBuffersOnIdle = cfg.rxMinBuffersOnIdle,
				allocationLimits = AllocationLimits.Default,
				byteSinkAllocator = makeSinkAllocator(cfg.sinkMode, bgExecutor),
				streamTransferBufferCapacity = cfg.rxStreamTransferBufferCapacity
			)
			fn(tx, rx, clientChannel, accepted, bgExecutor, tempDir)
		} finally {
			try clientChannel.close() catch { case NonFatal(_) => () }
			if serverConnection ne null then {
				try serverConnection.nn.close() catch { case NonFatal(_) => () }
			}
			try serverChannel.close() catch { case NonFatal(_) => () }
			bgExecutor.shutdownNow()
			if group ne null then {
				try group.shutdownNow() catch { case NonFatal(_) => () }
			}
			if groupExecutor ne null then {
				groupExecutor.nn.shutdownNow()
			}
			deleteTree(tempDir)
		}
	}

	/** Stack-safe, race-free trampoline wrapper around [[Receiver.receive]] for consuming streams of messages over loopback. */
	private final class ReceiverPump(rx: Receiver) {
		private val lock = new Object
		private var inReceiveCall = false
		private var pendingVersionFn: (() => ProtocolVersion) | Null = null
		private var pendingTimeout: Long = 10L
		private var pendingTimeUnit: TimeUnit = TimeUnit.SECONDS
		private var pendingOnOutcome: (WireEnvelope | Receiver.Fault => Unit) | Null = null
		private var stopped = false

		def stop(): Unit = lock.synchronized {
			stopped = true
			pendingVersionFn = null
			pendingOnOutcome = null
		}

		def requestReceive(msgVersionFn: () => ProtocolVersion, timeout: Long = 10L, timeUnit: TimeUnit = TimeUnit.SECONDS)(
			onOutcome: WireEnvelope | Receiver.Fault => Unit
		): Unit = {
			var curVersionFn: () => ProtocolVersion = msgVersionFn
			var curTimeout: Long = timeout
			var curTimeUnit: TimeUnit = timeUnit
			var curOnOutcome: WireEnvelope | Receiver.Fault => Unit = onOutcome
			val shouldRunLoop = lock.synchronized {
				if stopped then false
				else if inReceiveCall then {
					pendingVersionFn = msgVersionFn
					pendingTimeout = timeout
					pendingTimeUnit = timeUnit
					pendingOnOutcome = onOutcome
					false
				} else {
					inReceiveCall = true
					true
				}
			}
			if shouldRunLoop then {
				var continue = true
				while continue do {
					val ver = curVersionFn()
					val callback = curOnOutcome
					try {
						rx.receive[WireEnvelope](ver, curTimeout, curTimeUnit)(callback)
					} catch {
						case NonFatal(e) =>
							lock.synchronized {
								inReceiveCall = false
								pendingVersionFn = null
								pendingOnOutcome = null
							}
							throw e
					}
					continue = lock.synchronized {
						if !stopped && (pendingVersionFn ne null) && (pendingOnOutcome ne null) then {
							curVersionFn = pendingVersionFn.nn
							curTimeout = pendingTimeout
							curTimeUnit = pendingTimeUnit
							curOnOutcome = pendingOnOutcome.nn
							pendingVersionFn = null
							pendingOnOutcome = null
							true
						} else {
							inReceiveCall = false
							pendingVersionFn = null
							pendingOnOutcome = null
							false
						}
					}
				}
			}
		}
	}

	// ====================================================================================================
	// Minimal & Almost-Minimal Buffer Generators
	// ====================================================================================================

	private val genTxBuffersCapacity: Gen[Int] =
		Gen.frequency(3 -> Gen.const(9), 2 -> Gen.const(10), 2 -> Gen.const(11), 2 -> Gen.const(12), 3 -> Gen.chooseNum(13, 28))

	private val genRxBuffersCapacity: Gen[Int] =
		Gen.frequency(3 -> Gen.const(6), 2 -> Gen.const(7), 2 -> Gen.const(8), 2 -> Gen.const(9), 3 -> Gen.chooseNum(10, 28))

	private val genStreamTransferBufferCapacity: Gen[Int] =
		Gen.frequency(3 -> Gen.const(1), 2 -> Gen.const(2), 2 -> Gen.const(3), 2 -> Gen.const(5), 3 -> Gen.chooseNum(6, 28))

	private val genMaxGatheringBuffers: Gen[Int] =
		Gen.frequency(3 -> Gen.const(2), 2 -> Gen.const(3), 2 -> Gen.const(4), 1 -> Gen.const(5))

	private val genMinBuffersOnIdle: Gen[Int] = Gen.chooseNum(1, 3)

	private val genChannelGroupMode: Gen[ChannelGroupMode] =
		Gen.oneOf(ChannelGroupMode.SystemDefault, ChannelGroupMode.FixedPool(1), ChannelGroupMode.FixedPool(2), ChannelGroupMode.FixedPool(4))

	private def genSinkMode(seed: Long): Gen[SinkCompletionMode] =
		for {
			threshold <- Gen.chooseNum(1, 24)
			async <- Gen.oneOf(false, true)
		} yield if async then SinkCompletionMode.AdversarialAsync(threshold, seed) else SinkCompletionMode.DefaultAllocator(threshold)

	private def genLoopbackConfig(maxControlBytesPerMsg: Int, txMaxBuffersFloor: Int = 0): Gen[LoopbackConfig] =
		for {
			txCap <- genTxBuffersCapacity
			txGather <- genMaxGatheringBuffers
			txMinIdle <- genMinBuffersOnIdle
			txStreamCap <- genStreamTransferBufferCapacity
			rxCap <- genRxBuffersCapacity
			rxMinIdle <- genMinBuffersOnIdle
			rxStreamCap <- genStreamTransferBufferCapacity
			soSndBuf <- Gen.chooseNum(128, 1024)
			soRcvBuf <- Gen.chooseNum(128, 1024)
			tcpNoDelay <- Gen.oneOf(true, false)
			groupMode <- genChannelGroupMode
			sinkSeed <- Gen.chooseNum(Long.MinValue, Long.MaxValue)
			sinkMode <- genSinkMode(sinkSeed)
			extraTxBuf <- Gen.chooseNum(2, 6)
			extraRxBuf <- Gen.chooseNum(4, 10)
		} yield {
			val txNeeded = math.max(txMinIdle, math.ceil((maxControlBytesPerMsg + 64).toDouble / txCap).toInt + extraTxBuf)
			val txMaxBuf = if txMaxBuffersFloor > 0 then math.max(txMinIdle, txMaxBuffersFloor) else math.max(txNeeded, 4)
			val rxUsable = rxCap - 5
			val rxNeeded = math.max(rxMinIdle, math.ceil((maxControlBytesPerMsg + 80).toDouble / rxUsable).toInt + extraRxBuf)
			LoopbackConfig(
				txBuffersCapacity = txCap,
				txMaxBuffers = txMaxBuf,
				txMinBuffersOnIdle = txMinIdle,
				txStreamTransferBufferCapacity = txStreamCap,
				txMaxGatheringBuffers = txGather,
				rxBuffersCapacity = rxCap,
				rxMaxBuffers = math.max(rxNeeded, 4),
				rxMinBuffersOnIdle = rxMinIdle,
				rxStreamTransferBufferCapacity = rxStreamCap,
				soSndBuf = soSndBuf,
				soRcvBuf = soRcvBuf,
				tcpNoDelay = tcpNoDelay,
				groupMode = groupMode,
				sinkMode = sinkMode
			)
		}

	private def genRandomBytes(minLen: Int, maxLen: Int): Gen[Array[Byte]] =
		for {
			len <- Gen.chooseNum(minLen, maxLen)
			bytes <- Gen.containerOfN[Array, Byte](len, Gen.chooseNum(Byte.MinValue, Byte.MaxValue))
		} yield bytes

	private val genAsyncScheduling: Gen[AsyncReadScheduling] =
		for {
			seed <- Gen.chooseNum(Long.MinValue, Long.MaxValue)
			mode <- Gen.oneOf(
				AsyncReadScheduling.AlwaysSync,
				AsyncReadScheduling.AlwaysInlineCallback,
				AsyncReadScheduling.AlwaysExecutorCallback,
				AsyncReadScheduling.Alternating(seed)
			)
		} yield mode

	private def genValidStreamSpec(minBytes: Int = 1, maxBytes: Int = 140): Gen[StreamSpec] =
		Gen.frequency(
			1 -> Gen.const(StreamSpec.Empty),
			3 -> genRandomBytes(1, maxBytes).map(StreamSpec.InlineBuffer.apply),
			3 -> genRandomBytes(1, maxBytes).map(StreamSpec.DiskFile.apply),
			4 -> (for {
				bytes <- genRandomBytes(minBytes, maxBytes)
				chunk <- Gen.chooseNum(1, 14)
				sched <- genAsyncScheduling
				yieldNs <- Gen.oneOf(0, 0, 5000)
			} yield StreamSpec.AdversarialAsync(bytes, chunk, sched, yieldNs))
		)

	private case class MsgPlan(
		controlPayload: Array[Byte],
		markerStartsWithEscape: Boolean,
		tagPayload: Int,
		versionTag: Int,
		msgVersionId: Int,
		streamSpecs: List[StreamSpec],
		serializationDelayNanos: Long
	)

	private def genMsgPlan(maxControlLen: Int): Gen[MsgPlan] =
		for {
			ctrl <- genRandomBytes(0, maxControlLen)
			esc <- Gen.oneOf(true, false)
			tagVal <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
			verId <- Gen.chooseNum(1, 3)
			streamCount <- Gen.frequency(3 -> Gen.const(0), 4 -> Gen.const(1), 2 -> Gen.const(2), 1 -> Gen.const(3))
			streams <- Gen.listOfN(streamCount, genValidStreamSpec())
			delayNs <- Gen.frequency(3 -> Gen.const(0L), 1 -> Gen.chooseNum(1000L, 150000L))
		} yield MsgPlan(ctrl, esc, tagVal, verId, verId, streams, delayNs)

	private def buildEnvelope(
		producerId: Int,
		seq: Int,
		plan: MsgPlan,
		tempDir: Path,
		bgExecutor: ExecutorService,
		behavior: SerializationBehavior = SerializationBehavior.Normal
	): WireEnvelope = {
		val marker = if plan.markerStartsWithEscape then Serializer.BACK_REFERENCE_HEADER else (plan.tagPayload & 0x3f).toByte
		val shared = EscapeTag(marker, plan.tagPayload)
		val sources = plan.streamSpecs.map(spec => materializeStream(spec, tempDir, bgExecutor))
		val expected = plan.streamSpecs.map(expectedBytesOf)
		WireEnvelope(
			producerId = producerId,
			seq = seq,
			versionTag = plan.versionTag,
			controlPayload = plan.controlPayload,
			dedupFirst = shared,
			dedupSecond = shared,
			streams = sources,
			expectedStreams = expected,
			serializationDelayNanos = plan.serializationDelayNanos,
			serializationBehavior = behavior
		)
	}

	private def assertEnvelopeMatches(expected: WireEnvelope, actual: WireEnvelope): Unit = {
		assertEquals(actual.producerId, expected.producerId)
		assertEquals(actual.seq, expected.seq)
		assertEquals(actual.versionTag, expected.versionTag)
		assert(Arrays.equals(actual.controlPayload, expected.controlPayload), s"Control payload mismatch for (${expected.producerId}, ${expected.seq})")
		assertEquals(actual.dedupFirst, expected.dedupFirst)
		assertEquals(actual.dedupSecond, expected.dedupSecond)
		assert(actual.dedupFirst eq actual.dedupSecond, s"Reference deduplication failed for (${expected.producerId}, ${expected.seq})")
		assertEquals(actual.streams.size, expected.expectedStreams.size)
		actual.streams.zip(expected.expectedStreams).foreach { (actualBs, expectedBytes) =>
			val readBytes = readAndCloseByteSource(actualBs)
			assert(Arrays.equals(readBytes, expectedBytes), s"Trailing ByteSource payload mismatch for (${expected.producerId}, ${expected.seq}): expectedLen=${expectedBytes.length}, actualLen=${readBytes.length}")
		}
	}

	// ====================================================================================================
	// Property 1: Sequential/Backpressured Loopback Burst with Minimal Buffers & Dynamic Versions
	// ====================================================================================================

	private case class SequentialBurstScenario(
		cfg: LoopbackConfig,
		plans: Vector[MsgPlan],
		preQueueBeforeReceive: Int,
		interReceiveDelayNs: Long
	)

	private val genSequentialBurstScenario: Gen[SequentialBurstScenario] = {
		val maxCtrl = 160
		for {
			cfg <- genLoopbackConfig(maxCtrl)
			n <- Gen.chooseNum(6, 22)
			plans <- Gen.containerOfN[Vector, MsgPlan](n, genMsgPlan(maxCtrl))
			preQueue <- Gen.chooseNum(0, n)
			rxDelay <- Gen.frequency(3 -> Gen.const(0L), 1 -> Gen.chooseNum(1000L, 200000L))
		} yield SequentialBurstScenario(cfg, plans, preQueue, rxDelay)
	}

	property("PBT: minimal buffer configurations round-trip arbitrary control, reference-dedup, multi-ByteSource, and version-switching bursts over loopback") {
		Prop.forAllNoShrink(genSequentialBurstScenario) { sc =>
			withLoopbackPair(sc.cfg) { (tx, rx, _, _, bgExecutor, tempDir) =>
				val n = sc.plans.size
				val envelopes = sc.plans.zipWithIndex.map { (plan, idx) =>
					buildEnvelope(0, idx, plan, tempDir, bgExecutor)
				}

				val txCallbacks = new ArrayBuffer[(Int, Report)]()
				val txDoneLatch = new CountDownLatch(n)
				val rxOutcomes = new ArrayBuffer[WireEnvelope]()
				val rxErrors = new ArrayBuffer[String]()
				val rxDoneLatch = new CountDownLatch(n)
				val rxCursor = new AtomicInteger(0)
				val pump = new ReceiverPump(rx)

				def armNextReceive(): Unit = {
					val nextIdx = rxCursor.get()
					if nextIdx < n then {
						pump.requestReceive(() => ProtocolVersion(sc.plans(nextIdx).msgVersionId)) {
							case fault: Receiver.Fault =>
								rxErrors.synchronized(rxErrors += s"Receiver fault at $nextIdx: $fault")
								pump.stop()
								while rxDoneLatch.getCount > 0 do rxDoneLatch.countDown()
							case msg: WireEnvelope =>
								rxOutcomes.synchronized(rxOutcomes += msg)
								val completedIdx = rxCursor.incrementAndGet()
								rxDoneLatch.countDown()
								if sc.interReceiveDelayNs > 0L then LockSupport.parkNanos(sc.interReceiveDelayNs)
								if completedIdx < n then armNextReceive()
						}
					}
				}

				val preCount = math.min(sc.preQueueBeforeReceive, n)
				var i = 0
				while i < preCount do {
					val idx = i
					tx.transmitWithAttachment(envelopes(idx), ProtocolVersion(sc.plans(idx).msgVersionId), idx, timeout = 10L, timeUnit = TimeUnit.SECONDS) { (rep, att) =>
						txCallbacks.synchronized(txCallbacks += ((att, rep)))
						txDoneLatch.countDown()
					}
					i += 1
				}

				armNextReceive()

				while i < n do {
					val idx = i
					tx.transmitWithAttachment(envelopes(idx), ProtocolVersion(sc.plans(idx).msgVersionId), idx, timeout = 10L, timeUnit = TimeUnit.SECONDS) { (rep, att) =>
						txCallbacks.synchronized(txCallbacks += ((att, rep)))
						txDoneLatch.countDown()
					}
					i += 1
				}

				val txDoneOk = txDoneLatch.await(15, TimeUnit.SECONDS)
				assert(txDoneOk, s"Timed out waiting for TX callbacks (remaining=${txDoneLatch.getCount})")
				val rxDoneOk = rxDoneLatch.await(15, TimeUnit.SECONDS)
				assert(rxDoneOk, s"Timed out waiting for RX messages (remaining=${rxDoneLatch.getCount}, rxErrors=${rxErrors.mkString("; ")})")
				assertEquals(rxErrors.toList, Nil)

				val idleLatch = new CountDownLatch(1)
				tx.triggerOnIdle(() => idleLatch.countDown())
				val idleOk = idleLatch.await(5, TimeUnit.SECONDS)
				assert(idleOk, "Timed out waiting for PipelinedTransmitter idle")

				assertEquals(txCallbacks.map(_._1).toVector, (0 until n).toVector)
				txCallbacks.foreach { (idx, rep) =>
					assertEquals(rep, Delivered: Report, s"TX failure at index $idx")
				}
				assertEquals(rxOutcomes.size, n)
				var k = 0
				while k < n do {
					assertEnvelopeMatches(envelopes(k), rxOutcomes(k))
					k += 1
				}
				assert(tx.bufferCount <= 2 * sc.cfg.txMinBuffersOnIdle, s"Expected pruned bufferCount <= ${2 * sc.cfg.txMinBuffersOnIdle}, got ${tx.bufferCount}")
				true
			}
		}
	}

	// ====================================================================================================
	// Property 2: Adversarial Multi-Threaded Producers + Concurrent pruneTo + triggerOnIdle + Reentrant Submissions
	// ====================================================================================================

	private case class ConcurrentStormScenario(
		cfg: LoopbackConfig,
		perThreadPlans: Vector[Vector[MsgPlan]],
		reentrantEveryK: Int
	)

	private val genConcurrentStormScenario: Gen[ConcurrentStormScenario] = {
		val maxCtrl = 140
		for {
			cfg <- genLoopbackConfig(maxCtrl)
			threads <- Gen.chooseNum(3, 6)
			perThread <- Gen.chooseNum(6, 12)
			plans <- Gen.containerOfN[Vector, Vector[MsgPlan]](threads, Gen.containerOfN[Vector, MsgPlan](perThread, genMsgPlan(maxCtrl)))
			reentrantK <- Gen.frequency(2 -> Gen.const(0), 2 -> Gen.const(3), 2 -> Gen.const(4))
		} yield ConcurrentStormScenario(cfg, plans, reentrantK)
	}

	property("PBT: adversarial multi-threaded producers with concurrent pruneTo, triggerOnIdle, and reentrant callbacks maintain linearizable wire/callback order and per-thread FIFO") {
		Prop.forAllNoShrink(genConcurrentStormScenario) { sc =>
			withLoopbackPair(sc.cfg) { (tx, rx, _, _, bgExecutor, tempDir) =>
				val numThreads = sc.perThreadPlans.size
				val expectedByKey = new ConcurrentHashMap[(Int, Int), WireEnvelope]()
				val reentrantSpecs = new ConcurrentHashMap[(Int, Int), WireEnvelope]()
				val fixedVersion = ProtocolVersion(1)

				val baseTotal = sc.perThreadPlans.map(_.size).sum
				var reentrantCount = 0

				val threadPlansNormalized = sc.perThreadPlans.zipWithIndex.map { (plans, pid) =>
					plans.zipWithIndex.map { (p, seq) =>
						val normPlan = p.copy(msgVersionId = 1, versionTag = 1)
						val env = buildEnvelope(pid, seq, normPlan, tempDir, bgExecutor)
						expectedByKey.put((pid, seq), env)
						if sc.reentrantEveryK > 0 && ((seq + pid) % sc.reentrantEveryK == 0) then {
							reentrantCount += 1
							val reSeq = 10000 + seq
							val rePlan = MsgPlan(Array((pid ^ seq).toByte, 0x40.toByte, 0x11.toByte), markerStartsWithEscape = true, tagPayload = reSeq, versionTag = 1, msgVersionId = 1, streamSpecs = List(StreamSpec.InlineBuffer(Array[Byte](1, 2, 3, 4))), serializationDelayNanos = 0L)
							val reEnv = buildEnvelope(pid, reSeq, rePlan, tempDir, bgExecutor)
							expectedByKey.put((pid, reSeq), reEnv)
							reentrantSpecs.put((pid, seq), reEnv)
						}
						env
					}
				}

				val totalMessages = baseTotal + reentrantCount
				val callbackOrder = new ArrayBuffer[(Int, Int)]()
				val rxOrder = new ArrayBuffer[WireEnvelope]()
				val rxErrors = new ArrayBuffer[String]()
				val txDoneLatch = new CountDownLatch(totalMessages)
				val rxDoneLatch = new CountDownLatch(totalMessages)
				val pump = new ReceiverPump(rx)

				def armReceive(): Unit = {
					pump.requestReceive(() => fixedVersion) {
						case fault: Receiver.Fault =>
							rxErrors.synchronized(rxErrors += s"Receiver fault: $fault")
							pump.stop()
							while rxDoneLatch.getCount > 0 do rxDoneLatch.countDown()
						case msg: WireEnvelope =>
							rxOrder.synchronized(rxOrder += msg)
							rxDoneLatch.countDown()
							if rxDoneLatch.getCount > 0 then armReceive()
					}
				}
				armReceive()

				def submitEnvelope(env: WireEnvelope): Unit = {
					val key = (env.producerId, env.seq)
					tx.transmitWithAttachment(env, fixedVersion, key, timeout = 10L, timeUnit = TimeUnit.SECONDS) { (report, attKey) =>
						if report != Delivered then {
							rxErrors.synchronized(rxErrors += s"Failed delivery for $attKey: $report")
						}
						callbackOrder.synchronized(callbackOrder += attKey)
						txDoneLatch.countDown()
						val reEnv = reentrantSpecs.remove(attKey)
						if reEnv ne null then submitEnvelope(reEnv)
					}
				}

				val stopDisruptor = new AtomicBoolean(false)
				val idleListenersPending = new AtomicInteger(0)
				val disruptor = new Thread(() => {
					while !stopDisruptor.get() do {
						tx.pruneTo(sc.cfg.txMinBuffersOnIdle)
						if idleListenersPending.get() < 4 then {
							idleListenersPending.incrementAndGet()
							tx.triggerOnIdle(() => {
								idleListenersPending.decrementAndGet()
								()
							})
						}
						LockSupport.parkNanos(250_000L)
					}
				}, "ptx-pbt-disruptor")
				disruptor.setDaemon(true)
				disruptor.start()

				try {
					val startGate = new CountDownLatch(1)
					val producersDone = new CountDownLatch(numThreads)
					val producerErrors = new ConcurrentLinkedQueue[Throwable]()

					val workers = threadPlansNormalized.zipWithIndex.map { (envs, pid) =>
						val t = new Thread(() => {
							try {
								startGate.await()
								envs.foreach(submitEnvelope)
							} catch {
								case NonFatal(e) =>
									producerErrors.add(e)
									()
							} finally {
								producersDone.countDown()
							}
						}, s"ptx-pbt-producer-$pid")
						t.setDaemon(true)
						t.start()
						t
					}

					startGate.countDown()
					val prodOk = producersDone.await(15, TimeUnit.SECONDS)
					assert(prodOk, "Producers timed out")
					val txOk = txDoneLatch.await(20, TimeUnit.SECONDS)
					assert(txOk, s"TX callbacks timed out (remaining=${txDoneLatch.getCount})")
					val rxOk = rxDoneLatch.await(20, TimeUnit.SECONDS)
					assert(rxOk, s"RX outcomes timed out (remaining=${rxDoneLatch.getCount}, errors=${rxErrors.mkString("; ")})")

					assert(producerErrors.isEmpty, s"Producer thread errors: $producerErrors")
				} finally {
					stopDisruptor.set(true)
					disruptor.join(2000L)
				}
				assertEquals(rxErrors.toList, Nil)

				val idleLatch = new CountDownLatch(1)
				tx.triggerOnIdle(() => idleLatch.countDown())
				val idleOk = idleLatch.await(5, TimeUnit.SECONDS)
				assert(idleOk, "Timed out waiting for idle")

				// Verify global linearizability: exact RX wire order matches TX onComplete FIFO order
				val rxKeys = rxOrder.map(m => (m.producerId, m.seq)).toVector
				val txKeys = callbackOrder.toVector
				assertEquals(rxKeys.size, totalMessages)
				assertEquals(txKeys, rxKeys)

				// Verify strict per-producer monotonic sequence order on wire for base tasks
				for pid <- 0 until numThreads do {
					val baseSeqsForPid = rxOrder.filter(m => m.producerId == pid && m.seq < 10000).map(_.seq).toVector
					assertEquals(baseSeqsForPid, (0 until sc.perThreadPlans(pid).size).toVector)
				}

				// Verify exact content of every received envelope
				rxOrder.foreach { actual =>
					val expected = expectedByKey.get((actual.producerId, actual.seq))
					assert(expected ne null, s"Unexpected envelope (${actual.producerId}, ${actual.seq})")
					assertEnvelopeMatches(expected, actual)
				}
				assert(tx.bufferCount <= 2 * sc.cfg.txMinBuffersOnIdle)
				true
			}
		}
	}

	// ====================================================================================================
	// Property 3: Serialization Failures Interleaved Concurrently Never Corrupt Wire or Break FIFO Callbacks
	// ====================================================================================================

	private enum OpOutcomeExpectation {
		case ExpectDelivered
		case ExpectSerializationProblem
		case ExpectTransmissionFailure
	}

	private case class SerializationOpPlan(
		msgPlan: MsgPlan,
		behavior: SerializationBehavior,
		expectation: OpOutcomeExpectation
	)

	private case class SerializationFailureScenario(
		cfg: LoopbackConfig,
		concurrentProducers: Boolean,
		ops: Vector[SerializationOpPlan]
	)

	private val genSerializationFailureScenario: Gen[SerializationFailureScenario] = {
		for {
			smallTxMaxBuf <- Gen.chooseNum(6, 9)
			maxBurstCtrl = smallTxMaxBuf * 28
			rawCfg <- genLoopbackConfig(maxBurstCtrl, txMaxBuffersFloor = smallTxMaxBuf)
			cfg = rawCfg.copy(txMinBuffersOnIdle = math.min(rawCfg.txMinBuffersOnIdle, smallTxMaxBuf))
			maxValidCtrl = math.max(0, smallTxMaxBuf * cfg.txBuffersCapacity - 42)
			maxSafePrefix = math.max(1, smallTxMaxBuf * cfg.txBuffersCapacity - 12)
			n <- Gen.chooseNum(10, 20)
			concurrent <- Gen.oneOf(false, true)
			ops <- Gen.containerOfN[Vector, SerializationOpPlan](n, for {
				ctrl <- genRandomBytes(0, maxValidCtrl)
				esc <- Gen.oneOf(true, false)
				tagVal <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
				streamCount <- Gen.oneOf(0, 1)
				streams <- Gen.listOfN(streamCount, genValidStreamSpec(maxBytes = 80))
				plan = MsgPlan(ctrl, esc, tagVal, 1, 1, streams, 0L)
				prefixLen <- Gen.chooseNum(1, math.min(maxSafePrefix, cfg.txBuffersCapacity * 2 + 3))
				prefix <- genRandomBytes(prefixLen, prefixLen)
				kind <- Gen.frequency(
					5 -> Gen.const(0),
					2 -> Gen.const(1),
					2 -> Gen.const(2),
					2 -> Gen.const(3),
					1 -> Gen.const(4)
				)
			} yield kind match {
				case 0 => SerializationOpPlan(plan, SerializationBehavior.Normal, OpOutcomeExpectation.ExpectDelivered)
				case 1 => SerializationOpPlan(plan, SerializationBehavior.ThrowSerExMidway(prefix), OpOutcomeExpectation.ExpectSerializationProblem)
				case 2 => SerializationOpPlan(plan, SerializationBehavior.ThrowNegativeByteSourceSize(prefix), OpOutcomeExpectation.ExpectSerializationProblem)
				case 3 =>
					val overflow = smallTxMaxBuf * cfg.txBuffersCapacity + 1
					SerializationOpPlan(plan, SerializationBehavior.ExceedMaxBuffers(overflow), OpOutcomeExpectation.ExpectTransmissionFailure)
				case _ => SerializationOpPlan(plan, SerializationBehavior.ThrowNonFatalMidway(prefix), OpOutcomeExpectation.ExpectTransmissionFailure)
			})
		} yield SerializationFailureScenario(cfg, concurrent, ops)
	}

	property("PBT: Serialization and capacity failures interleaved never taint socket wire and preserve FIFO callback order") {
		Prop.forAllNoShrink(genSerializationFailureScenario) { sc =>
			withLoopbackPair(sc.cfg) { (tx, rx, _, _, bgExecutor, tempDir) =>
				val fixedVersion = ProtocolVersion(1)
				val totalOps = sc.ops.size
				val envelopes = sc.ops.zipWithIndex.map { (op, idx) =>
					buildEnvelope(0, idx, op.msgPlan, tempDir, bgExecutor, op.behavior)
				}
				val validCount = sc.ops.count(_.expectation == OpOutcomeExpectation.ExpectDelivered)
				val postProbePlan = MsgPlan(Array[Byte](0x40, 0x01, 0x7f), markerStartsWithEscape = true, tagPayload = 99999, versionTag = 1, msgVersionId = 1, streamSpecs = List(StreamSpec.InlineBuffer(Array[Byte](9, 8, 7))), serializationDelayNanos = 0L)
				val postProbeEnv = buildEnvelope(0, 99999, postProbePlan, tempDir, bgExecutor)

				val txResults = new ArrayBuffer[(Int, Report)]()
				val txDoneLatch = new CountDownLatch(totalOps)
				val rxOutcomes = new ArrayBuffer[WireEnvelope]()
				val rxErrors = new ArrayBuffer[String]()
				val expectedTotalRx = validCount + 1
				val rxDoneLatch = new CountDownLatch(expectedTotalRx)
				val pump = new ReceiverPump(rx)

				def armReceive(): Unit = {
					pump.requestReceive(() => fixedVersion) {
						case fault: Receiver.Fault =>
							rxErrors.synchronized(rxErrors += s"Receiver fault: $fault")
							pump.stop()
							while rxDoneLatch.getCount > 0 do rxDoneLatch.countDown()
						case msg: WireEnvelope =>
							rxOutcomes.synchronized(rxOutcomes += msg)
							rxDoneLatch.countDown()
							if rxDoneLatch.getCount > 0 then armReceive()
					}
				}
				armReceive()

				if sc.concurrentProducers then {
					val mid = totalOps / 2
					val gate = new CountDownLatch(1)
					val submitDone = new CountDownLatch(2)
					def spawnRange(from: Int, untilIdx: Int): Unit = {
						val t = new Thread(() => {
							gate.await()
							var idx = from
							while idx < untilIdx do {
								tx.transmitWithAttachment(envelopes(idx), fixedVersion, idx, timeout = 10L, timeUnit = TimeUnit.SECONDS) { (rep, att) =>
									txResults.synchronized(txResults += ((att, rep)))
									txDoneLatch.countDown()
								}
								idx += 1
							}
							submitDone.countDown()
						})
						t.setDaemon(true)
						t.start()
					}
					spawnRange(0, mid)
					spawnRange(mid, totalOps)
					gate.countDown()
					val subOk = submitDone.await(10, TimeUnit.SECONDS)
					assert(subOk)
				} else {
					var idx = 0
					while idx < totalOps do {
						tx.transmitWithAttachment(envelopes(idx), fixedVersion, idx, timeout = 10L, timeUnit = TimeUnit.SECONDS) { (rep, att) =>
							txResults.synchronized(txResults += ((att, rep)))
							txDoneLatch.countDown()
						}
						idx += 1
					}
				}

				val txOk = txDoneLatch.await(15, TimeUnit.SECONDS)
				assert(txOk, s"TX callbacks timed out (remaining=${txDoneLatch.getCount})")
				val idleLatch1 = new CountDownLatch(1)
				tx.triggerOnIdle(() => idleLatch1.countDown())
				val idle1Ok = idleLatch1.await(5, TimeUnit.SECONDS)
				assert(idle1Ok, "Idle after failure burst timed out")

				// Verify transmitter remains cleanly reusable after serialization failures
				val postLatch = new CountDownLatch(1)
				@volatile var postReport: Report | Null = null
				tx.transmit(postProbeEnv, fixedVersion, timeout = 10L, timeUnit = TimeUnit.SECONDS) { rep =>
					postReport = rep
					postLatch.countDown()
				}
				val postOk = postLatch.await(10, TimeUnit.SECONDS)
				assert(postOk)
				assertEquals(postReport, Delivered: Report)

				val rxOk = rxDoneLatch.await(15, TimeUnit.SECONDS)
				assert(rxOk, s"RX timed out (remaining=${rxDoneLatch.getCount}, errors=${rxErrors.mkString("; ")})")
				assertEquals(rxErrors.toList, Nil)

				// Check every callback matched its expected serialization failure or Delivered outcome
				txResults.foreach { (idx, rep) =>
					sc.ops(idx).expectation match {
						case OpOutcomeExpectation.ExpectDelivered =>
							assertEquals(rep, Delivered: Report)
						case OpOutcomeExpectation.ExpectSerializationProblem =>
							rep match {
								case SerializationProblem(_, _, aFragmentWasTransmitted) =>
									assert(!aFragmentWasTransmitted, s"Serialization failure leaked fragment flag at $idx")
								case other => fail(s"Expected SerializationProblem at $idx, got $other")
							}
						case OpOutcomeExpectation.ExpectTransmissionFailure =>
							assert(rep.isInstanceOf[TransmissionFailure], s"Expected TransmissionFailure at $idx, got $rep")
					}
				}

				if !sc.concurrentProducers then {
					assertEquals(txResults.map(_._1).toVector, (0 until totalOps).toVector)
				}

				val expectedValidIds = txResults.collect { case (idx, Delivered) => idx }.toVector :+ 99999
				val actualRxIds = rxOutcomes.map(_.seq).toVector
				assertEquals(actualRxIds, expectedValidIds)

				rxOutcomes.foreach { actual =>
					val expected = if actual.seq == 99999 then postProbeEnv else envelopes(actual.seq)
					assertEnvelopeMatches(expected, actual)
				}
				true
			}
		}
	}

	// ====================================================================================================
	// Property 4: Mid-Stream Trailing ByteSource Aborts Over Loopback & Open-Channel Receiver Recovery
	// ====================================================================================================

	private case class TrailingAbortScenario(
		cfg: LoopbackConfig,
		items: Vector[Either[StreamAbortKind, MsgPlan]]
	)

	private val genTrailingAbortScenario: Gen[TrailingAbortScenario] = {
		val maxCtrl = 110
		for {
			cfg <- genLoopbackConfig(maxCtrl)
			n <- Gen.chooseNum(7, 16)
			middle <- Gen.containerOfN[Vector, Either[StreamAbortKind, MsgPlan]](n, for {
				isAbort <- Gen.frequency(2 -> Gen.const(true), 3 -> Gen.const(false))
				res <- if isAbort then {
					for {
						decl <- Gen.chooseNum(12, 96)
						failAt <- Gen.chooseNum(0, decl - 1)
						kind <- Gen.oneOf(
							StreamAbortKind.OpenThrows,
							StreamAbortKind.SyncThrowAfter(failAt),
							StreamAbortKind.AsyncFailAfter(failAt, inline = true),
							StreamAbortKind.AsyncFailAfter(failAt, inline = false),
							StreamAbortKind.PrematureEofAfter(failAt),
							StreamAbortKind.SizeDriftOver(5)
						)
					} yield Left(kind)
				} else {
					genMsgPlan(maxCtrl).map(p => Right(p.copy(msgVersionId = 1, versionTag = 1)))
				}
			} yield res)
			// Always append a trailing valid multi-ByteSource message to verify open-channel recovery after aborts
			sentinelPlan <- genMsgPlan(maxCtrl).map(_.copy(msgVersionId = 1, versionTag = 1))
		} yield TrailingAbortScenario(cfg, middle :+ Right(sentinelPlan))
	}

	property("PBT: trailing ByteSource aborts emit CORRUPTED_PACKAGE_SENTINEL cleanly while Receiver keeps channel open and delivers all valid messages") {
		Prop.forAllNoShrink(genTrailingAbortScenario) { sc =>
			withLoopbackPair(sc.cfg) { (tx, rx, _, _, bgExecutor, tempDir) =>
				val fixedVersion = ProtocolVersion(1)
				val total = sc.items.size
				val envelopes = sc.items.zipWithIndex.map {
					case (Right(plan), idx) =>
						buildEnvelope(0, idx, plan, tempDir, bgExecutor)
					case (Left(abortKind), idx) =>
						val decl = abortKind match {
							case StreamAbortKind.PrematureEofAfter(k) => k + 17
							case StreamAbortKind.SyncThrowAfter(k) => k + 17
							case StreamAbortKind.AsyncFailAfter(k, _) => k + 17
							case StreamAbortKind.OpenThrows => 32
							case StreamAbortKind.SizeDriftOver(_) => 24
						}
						val abortPlan = MsgPlan(Array[Byte](idx.toByte, 0x22, 0x40), markerStartsWithEscape = true, tagPayload = idx, versionTag = 1, msgVersionId = 1, streamSpecs = List(StreamSpec.AbortingStream(decl, maxChunk = 5, abortKind, seed = idx.toLong + 101L)), serializationDelayNanos = 0L)
						buildEnvelope(0, idx, abortPlan, tempDir, bgExecutor)
				}

				val expectedValidSeqs = sc.items.zipWithIndex.collect { case (Right(_), idx) => idx }
				val txReports = new ArrayBuffer[(Int, Report)]()
				val txLatch = new CountDownLatch(total)
				val rxDelivered = new ArrayBuffer[WireEnvelope]()
				val rxErrors = new ArrayBuffer[String]()
				val rxLatch = new CountDownLatch(expectedValidSeqs.size)
				val pump = new ReceiverPump(rx)

				def armReceive(): Unit = {
					pump.requestReceive(() => fixedVersion) {
						case fault: Receiver.Fault =>
							rxErrors.synchronized(rxErrors += s"Receiver fault: $fault")
							pump.stop()
							while rxLatch.getCount > 0 do rxLatch.countDown()
						case msg: WireEnvelope =>
							rxDelivered.synchronized(rxDelivered += msg)
							rxLatch.countDown()
							if rxLatch.getCount > 0 then armReceive()
					}
				}
				armReceive()

				var idx = 0
				while idx < total do {
					val id = idx
					tx.transmitWithAttachment(envelopes(id), fixedVersion, id, timeout = 10L, timeUnit = TimeUnit.SECONDS) { (rep, att) =>
						txReports.synchronized(txReports += ((att, rep)))
						txLatch.countDown()
					}
					idx += 1
				}

				val txOk = txLatch.await(20, TimeUnit.SECONDS)
				assert(txOk, s"TX callbacks timed out (remaining=${txLatch.getCount})")
				val rxOk = rxLatch.await(20, TimeUnit.SECONDS)
				assert(rxOk, s"RX timed out (remaining=${rxLatch.getCount}, errors=${rxErrors.mkString("; ")})")
				assertEquals(rxErrors.toList, Nil)

				val idleLatch = new CountDownLatch(1)
				tx.triggerOnIdle(() => idleLatch.countDown())
				val idleOk = idleLatch.await(5, TimeUnit.SECONDS)
				assert(idleOk, "Timed out waiting for idle")

				assertEquals(txReports.map(_._1).toVector, (0 until total).toVector)
				txReports.foreach { (id, rep) =>
					sc.items(id) match {
						case Right(_) => assertEquals(rep, Delivered: Report)
						case Left(_) => assert(rep.isInstanceOf[ByteSourceProblem], s"Expected ByteSourceProblem at $id, got $rep")
					}
				}

				assertEquals(rxDelivered.map(_.seq).toVector, expectedValidSeqs)
				rxDelivered.foreach { actual =>
					assertEnvelopeMatches(envelopes(actual.seq), actual)
				}
				assert(tx.bufferCount <= 2 * sc.cfg.txMinBuffersOnIdle)
				true
			}
		}
	}

	// ====================================================================================================
	// Property 5: Socket Teardown Race During Active Concurrent Pipelining Never Drops Callbacks or Deadlocks
	// ====================================================================================================

	private case class TeardownRaceScenario(
		cfg: LoopbackConfig,
		threads: Int,
		msgsPerThread: Int,
		closeClientSide: Boolean,
		killAfterRxCount: Int,
		plans: Vector[MsgPlan]
	)

	private val genTeardownRaceScenario: Gen[TeardownRaceScenario] = {
		val maxCtrl = 140
		for {
			cfg <- genLoopbackConfig(maxCtrl)
			threads <- Gen.chooseNum(2, 4)
			msgsPerThread <- Gen.chooseNum(6, 10)
			closeClient <- Gen.oneOf(true, false)
			killAfter <- Gen.chooseNum(0, 4)
			plans <- Gen.containerOfN[Vector, MsgPlan](threads * msgsPerThread, genMsgPlan(maxCtrl))
		} yield TeardownRaceScenario(cfg, threads, msgsPerThread, closeClient, killAfter, plans)
	}

	property("PBT: closing either loopback endpoint mid-burst drains all queued callbacks without deadlock and returns transmitter to idle") {
		Prop.forAllNoShrink(genTeardownRaceScenario) { sc =>
			withLoopbackPair(sc.cfg) { (tx, rx, clientChannel, serverConnection, bgExecutor, tempDir) =>
				val fixedVersion = ProtocolVersion(1)
				val total = sc.threads * sc.msgsPerThread
				val envelopes = sc.plans.zipWithIndex.map { (plan, idx) =>
					val p = plan.copy(msgVersionId = 1, versionTag = 1, serializationDelayNanos = 0L)
					buildEnvelope(idx / sc.msgsPerThread, idx % sc.msgsPerThread, p, tempDir, bgExecutor)
				}

				val txCompleted = new AtomicInteger(0)
				val txDeliveredCount = new AtomicInteger(0)
				val txFailedCount = new AtomicInteger(0)
				val txCallbacksLatch = new CountDownLatch(total)
				val overlapGuard = new AtomicBoolean(false)
				val overlapDetected = new AtomicBoolean(false)

				val rxSeenCount = new AtomicInteger(0)
				val killTriggered = new AtomicBoolean(false)
				val rxTerminatedLatch = new CountDownLatch(1)
				val rxErrors = new ConcurrentLinkedQueue[String]()
				val pump = new ReceiverPump(rx)

				def triggerTeardown(): Unit = {
					if killTriggered.compareAndSet(false, true) then {
						try {
							bgExecutor.execute(() => {
								val victim = if sc.closeClientSide then clientChannel else serverConnection
								try victim.close() catch { case NonFatal(_) => () }
							})
						} catch {
							case _: RejectedExecutionException => ()
						}
					}
				}

				val rxReceivedBeforeTeardown = new ConcurrentLinkedQueue[WireEnvelope]()

				def armReceive(): Unit = {
					pump.requestReceive(() => fixedVersion) {
						case _: Receiver.ChannelClosedByPeer =>
							pump.stop()
							rxTerminatedLatch.countDown()
						case _: Receiver.ReceptionFailure =>
							pump.stop()
							rxTerminatedLatch.countDown()
						case otherFault: Receiver.Fault =>
							rxErrors.add(s"Unexpected framing/deserialization fault during teardown: $otherFault")
							pump.stop()
							rxTerminatedLatch.countDown()
						case msg: WireEnvelope =>
							rxReceivedBeforeTeardown.add(msg)
							val count = rxSeenCount.incrementAndGet()
							if count >= sc.killAfterRxCount then triggerTeardown()
							if count < total then armReceive() else rxTerminatedLatch.countDown()
					}
				}
				armReceive()
				if sc.killAfterRxCount == 0 then triggerTeardown()

				val startGate = new CountDownLatch(1)
				val submitDone = new CountDownLatch(sc.threads)
				for pid <- 0 until sc.threads do {
					val t = new Thread(() => {
						startGate.await()
						var seq = 0
						while seq < sc.msgsPerThread do {
							val env = envelopes(pid * sc.msgsPerThread + seq)
							tx.transmitWithAttachment(env, fixedVersion, (pid, seq), timeout = 5L, timeUnit = TimeUnit.SECONDS) { (rep, _) =>
								if !overlapGuard.compareAndSet(false, true) then overlapDetected.set(true)
								try {
									rep match {
										case Delivered => txDeliveredCount.incrementAndGet()
										case _: NotDelivered => txFailedCount.incrementAndGet()
									}
									txCompleted.incrementAndGet()
								} finally {
									overlapGuard.set(false)
									txCallbacksLatch.countDown()
								}
							}
							seq += 1
						}
						submitDone.countDown()
					}, s"ptx-pbt-teardown-$pid")
					t.setDaemon(true)
					t.start()
				}

				startGate.countDown()
				val subOk = submitDone.await(15, TimeUnit.SECONDS)
				assert(subOk, "Submit threads timed out")
				val txOk = txCallbacksLatch.await(20, TimeUnit.SECONDS)
				assert(txOk, s"TX callbacks hung after close (completed=${txCompleted.get()} / $total)")
				val rxTermOk = rxTerminatedLatch.await(10, TimeUnit.SECONDS)
				assert(rxTermOk, "Receiver did not observe closure after teardown")

				val rxIt = rxReceivedBeforeTeardown.iterator()
				while rxIt.hasNext do {
					val msg = rxIt.next()
					val expected = envelopes(msg.producerId * sc.msgsPerThread + msg.seq)
					assertEnvelopeMatches(expected, msg)
				}

				assert(!overlapDetected.get(), "Detected concurrent overlapping execution of onComplete callbacks")
				assertEquals(txCompleted.get(), total)
				assertEquals(txDeliveredCount.get() + txFailedCount.get(), total)
				assert(rxErrors.isEmpty, s"Receiver reported corruption faults during teardown: $rxErrors")

				val idleLatch = new CountDownLatch(1)
				tx.triggerOnIdle(() => idleLatch.countDown())
				val idleOk = idleLatch.await(5, TimeUnit.SECONDS)
				assert(idleOk, "Transmitter failed to transition to idle after teardown")
				assert(tx.bufferCount <= 2 * sc.cfg.txMinBuffersOnIdle)
				true
			}
		}
	}

	// ====================================================================================================
	// Property 6: Combined Chaotic Faults, Multi-Source Trailing Aborts, Prune/Idle Disruptor & Re-entrancy
	// ====================================================================================================

	private enum ChaoticOpKind {
		case Deliver(plan: MsgPlan)
		case FailSerialization(plan: MsgPlan, behavior: SerializationBehavior, expectation: OpOutcomeExpectation)
		case MultiStreamAbort(
			ctrlPayload: Array[Byte],
			prefixSpecs: List[StreamSpec],
			abortKind: StreamAbortKind,
			abortDeclaredSize: Int,
			abortCloseThrows: Boolean,
			suffixSpecs: List[StreamSpec]
		)
	}

	private case class ChaoticOpPlan(kind: ChaoticOpKind, reentrantFollowUp: Option[MsgPlan])

	private case class ChaoticStormScenario(
		cfg: LoopbackConfig,
		perThreadOps: Vector[Vector[ChaoticOpPlan]]
	)

	private def wrapTrackedByteSource(
		delegate: ByteSource,
		openCounter: AtomicInteger | Null = null,
		closeCounter: AtomicInteger | Null = null,
		closeThrows: Boolean = false,
		preReturnAsyncWorkerDelay: ExecutorService | Null = null
	): ByteSource = new ByteSource {
		override def size: Long = delegate.size
		override def openChannel(): java.nio.channels.ReadableByteChannel = delegate.openChannel()
		override def transferTo(target: java.nio.channels.WritableByteChannel): Long = delegate.transferTo(target)
		override def close(): Unit = delegate.close()
		override def openAsyncReader(): ByteSource.AsyncReader = {
			if openCounter ne null then openCounter.nn.incrementAndGet()
			val inner = delegate.openAsyncReader()
			new ByteSource.AsyncReader {
				override def read[A](dst: ByteBuffer, attachment: A, handler: CompletionHandler[Integer, ? >: A]): Int = {
					if preReturnAsyncWorkerDelay ne null then LockSupport.parkNanos(5_000L)
					inner.read(dst, attachment, handler)
				}
				override def close(): Unit = {
					if closeCounter ne null then closeCounter.nn.incrementAndGet()
					try inner.close() catch { case NonFatal(_) => () }
					if closeThrows then throw new IOException("Injected secondary reader.close failure")
				}
			}
		}
	}

	private val genChaoticStormScenario: Gen[ChaoticStormScenario] = {
		for {
			smallTxMaxBuf <- Gen.chooseNum(6, 9)
			maxBurstCtrl = smallTxMaxBuf * 28
			rawCfg <- genLoopbackConfig(maxBurstCtrl, txMaxBuffersFloor = smallTxMaxBuf)
			cfg = rawCfg.copy(txMinBuffersOnIdle = math.min(rawCfg.txMinBuffersOnIdle, smallTxMaxBuf))
			maxValidCtrl = math.max(0, smallTxMaxBuf * cfg.txBuffersCapacity - 64)
			maxSafePrefix = math.max(1, smallTxMaxBuf * cfg.txBuffersCapacity - 16)
			threads <- Gen.chooseNum(2, 4)
			perThread <- Gen.chooseNum(4, 7)
			threadOps <- Gen.containerOfN[Vector, Vector[ChaoticOpPlan]](threads, Gen.containerOfN[Vector, ChaoticOpPlan](perThread, for {
				kindTag <- Gen.frequency(5 -> Gen.const(0), 2 -> Gen.const(1), 2 -> Gen.const(2))
				op <- kindTag match {
					case 0 =>
						for {
							ctrl <- genRandomBytes(0, maxValidCtrl)
							esc <- Gen.oneOf(true, false)
							tagVal <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
							scCount <- Gen.frequency(2 -> Gen.const(0), 3 -> Gen.const(1), 2 -> Gen.const(2))
							streams <- Gen.listOfN(scCount, genValidStreamSpec(maxBytes = 72))
							delayNs <- Gen.frequency(3 -> Gen.const(0L), 1 -> Gen.chooseNum(1000L, 60000L))
						} yield ChaoticOpKind.Deliver(MsgPlan(ctrl, esc, tagVal, 1, 1, streams, delayNs))
					case 1 =>
						for {
							ctrl <- genRandomBytes(0, math.min(maxValidCtrl, 32))
							prefixLen <- Gen.chooseNum(1, math.min(maxSafePrefix, cfg.txBuffersCapacity + 3))
							prefix <- genRandomBytes(prefixLen, prefixLen)
							errPick <- Gen.chooseNum(0, 3)
							plan = MsgPlan(ctrl, markerStartsWithEscape = true, tagPayload = prefixLen, versionTag = 1, msgVersionId = 1, streamSpecs = Nil, serializationDelayNanos = 0L)
						} yield errPick match {
							case 0 => ChaoticOpKind.FailSerialization(plan, SerializationBehavior.ThrowSerExMidway(prefix), OpOutcomeExpectation.ExpectSerializationProblem)
							case 1 => ChaoticOpKind.FailSerialization(plan, SerializationBehavior.ThrowNegativeByteSourceSize(prefix), OpOutcomeExpectation.ExpectSerializationProblem)
							case 2 => ChaoticOpKind.FailSerialization(plan, SerializationBehavior.ExceedMaxBuffers(smallTxMaxBuf * cfg.txBuffersCapacity + 1), OpOutcomeExpectation.ExpectTransmissionFailure)
							case _ => ChaoticOpKind.FailSerialization(plan, SerializationBehavior.ThrowNonFatalMidway(prefix), OpOutcomeExpectation.ExpectTransmissionFailure)
						}
					case _ =>
						for {
							ctrl <- genRandomBytes(0, math.min(maxValidCtrl, 48))
							prefixCount <- Gen.chooseNum(0, 1)
							suffixCount <- Gen.chooseNum(0, 1)
							prefixStreams <- Gen.listOfN(prefixCount, genValidStreamSpec(minBytes = 2, maxBytes = 40))
							suffixStreams <- Gen.listOfN(suffixCount, genValidStreamSpec(minBytes = 2, maxBytes = 40))
							decl <- Gen.chooseNum(12, 60)
							failAt <- Gen.chooseNum(0, decl - 1)
							abortKind <- Gen.oneOf(
								StreamAbortKind.OpenThrows,
								StreamAbortKind.SyncThrowAfter(failAt),
								StreamAbortKind.AsyncFailAfter(failAt, inline = true),
								StreamAbortKind.AsyncFailAfter(failAt, inline = false),
								StreamAbortKind.PrematureEofAfter(failAt),
								StreamAbortKind.SizeDriftOver(4)
							)
							closeThrows <- Gen.oneOf(false, true)
						} yield ChaoticOpKind.MultiStreamAbort(ctrl, prefixStreams, abortKind, decl, closeThrows, suffixStreams)
				}
				addReentrant <- Gen.frequency(1 -> Gen.const(true), 3 -> Gen.const(false))
				reentrant <- if addReentrant then {
					for {
						rCtrl <- genRandomBytes(0, math.min(maxValidCtrl, 24))
						rStreams <- Gen.listOfN(1, genValidStreamSpec(maxBytes = 24))
					} yield Some(MsgPlan(rCtrl, markerStartsWithEscape = true, tagPayload = 77, versionTag = 1, msgVersionId = 1, streamSpecs = rStreams, serializationDelayNanos = 0L))
				} else Gen.const(None)
			} yield ChaoticOpPlan(op, reentrant)))
		} yield ChaoticStormScenario(cfg, threadOps)
	}

	property("PBT: combined chaotic serialization faults, multi-ByteSource aborts, concurrent pruneTo/triggerOnIdle, and reentrant idle/complete submissions preserve invariants") {
		Prop.forAllNoShrink(genChaoticStormScenario) { sc =>
			withLoopbackPair(sc.cfg) { (tx, rx, _, _, bgExecutor, tempDir) =>
				val fixedVersion = ProtocolVersion(1)
				val unreachedOpenCount = new AtomicInteger(0)
				val abortCloseCount = new AtomicInteger(0)
				val expectedByKey = new ConcurrentHashMap[(Int, Int), WireEnvelope]()
				val expectedOutcomeByKey = new ConcurrentHashMap[(Int, Int), String]()
				val reentrantByTriggerKey = new ConcurrentHashMap[(Int, Int), WireEnvelope]()

				var totalSubmissions = 0
				var expectedDeliveries = 0
				var expectedAborts = 0

				val threadMessages = sc.perThreadOps.zipWithIndex.map { (ops, pid) =>
					ops.zipWithIndex.map { (op, seq) =>
						totalSubmissions += 1
						val key = (pid, seq)
						val env = op.kind match {
							case ChaoticOpKind.Deliver(plan) =>
								expectedDeliveries += 1
								expectedOutcomeByKey.put(key, "Delivered")
								val built = buildEnvelope(pid, seq, plan, tempDir, bgExecutor)
								expectedByKey.put(key, built)
								built
							case ChaoticOpKind.FailSerialization(plan, behavior, expectation) =>
								val tag = expectation match {
									case OpOutcomeExpectation.ExpectSerializationProblem => "SerProblem"
									case _ => "TxFailure"
								}
								expectedOutcomeByKey.put(key, tag)
								buildEnvelope(pid, seq, plan, tempDir, bgExecutor, behavior)
							case ChaoticOpKind.MultiStreamAbort(ctrl, prefixSpecs, abortKind, decl, closeThrows, suffixSpecs) =>
								expectedAborts += 1
								expectedOutcomeByKey.put(key, "ByteSourceProblem")
								val declEffective = abortKind match {
									case StreamAbortKind.PrematureEofAfter(k) => k + 17
									case StreamAbortKind.SyncThrowAfter(k) => k + 17
									case StreamAbortKind.AsyncFailAfter(k, _) => k + 17
									case StreamAbortKind.OpenThrows => math.max(decl, 16)
									case StreamAbortKind.SizeDriftOver(_) => math.max(decl, 16)
								}
								val prefixBuilt = prefixSpecs.map(s => materializeStream(s, tempDir, bgExecutor))
								val abortBuilt = wrapTrackedByteSource(
									materializeStream(StreamSpec.AbortingStream(declEffective, maxChunk = 5, abortKind, seed = (pid * 97 + seq).toLong), tempDir, bgExecutor),
									closeCounter = abortCloseCount,
									closeThrows = closeThrows && abortKind != StreamAbortKind.OpenThrows,
									preReturnAsyncWorkerDelay = bgExecutor
								)
								val suffixBuilt = suffixSpecs.map { s =>
									val nonEmptySpec = if s == StreamSpec.Empty then StreamSpec.InlineBuffer(Array[Byte](1, 2, 3)) else s
									wrapTrackedByteSource(materializeStream(nonEmptySpec, tempDir, bgExecutor), openCounter = unreachedOpenCount)
								}
								WireEnvelope(
									producerId = pid,
									seq = seq,
									versionTag = 1,
									controlPayload = ctrl,
									dedupFirst = EscapeTag(Serializer.BACK_REFERENCE_HEADER, seq),
									dedupSecond = EscapeTag(Serializer.BACK_REFERENCE_HEADER, seq),
									streams = prefixBuilt ++ List(abortBuilt) ++ suffixBuilt
								)
						}
						op.reentrantFollowUp.foreach { rPlan =>
							val rSeq = 10000 + seq
							val rKey = (pid, rSeq)
							totalSubmissions += 1
							expectedDeliveries += 1
							expectedOutcomeByKey.put(rKey, "Delivered")
							val rEnv = buildEnvelope(pid, rSeq, rPlan, tempDir, bgExecutor)
							expectedByKey.put(rKey, rEnv)
							reentrantByTriggerKey.put(key, rEnv)
						}
						env
					}
				}

				val txDeliveredOrder = new ArrayBuffer[(Int, Int)]()
				val txErrors = new ConcurrentLinkedQueue[String]()
				val rxDelivered = new ArrayBuffer[WireEnvelope]()
				val rxErrors = new ConcurrentLinkedQueue[String]()
				val txLatch = new CountDownLatch(totalSubmissions)
				val rxLatch = new CountDownLatch(expectedDeliveries)
				val pump = new ReceiverPump(rx)

				def armReceive(): Unit = {
					pump.requestReceive(() => fixedVersion) {
						case fault: Receiver.Fault =>
							rxErrors.add(s"Unexpected Receiver fault: $fault")
							pump.stop()
							while rxLatch.getCount > 0 do rxLatch.countDown()
						case msg: WireEnvelope =>
							rxDelivered.synchronized(rxDelivered += msg)
							rxLatch.countDown()
							if rxLatch.getCount > 0 then armReceive()
					}
				}
				if expectedDeliveries > 0 then armReceive()

				def submitMsg(env: WireEnvelope, isExtraProbe: Boolean = false, probeLatch: CountDownLatch | Null = null): Unit = {
					val key = (env.producerId, env.seq)
					tx.transmitWithAttachment(env, fixedVersion, key, timeout = 10L, timeUnit = TimeUnit.SECONDS) { (rep, att) =>
						if isExtraProbe then {
							if rep != Delivered then txErrors.add(s"Idle probe failed: $rep")
							if probeLatch ne null then probeLatch.nn.countDown()
						} else {
							expectedOutcomeByKey.get(att) match {
								case "Delivered" =>
									if rep != Delivered then txErrors.add(s"Expected Delivered for $att, got $rep")
									else txDeliveredOrder.synchronized(txDeliveredOrder += att)
								case "SerProblem" =>
									rep match {
										case SerializationProblem(_, _, false) => ()
										case other => txErrors.add(s"Expected SerializationProblem(aFragmentWasTransmitted=false) for $att, got $other")
									}
								case "TxFailure" =>
									if !rep.isInstanceOf[TransmissionFailure] then txErrors.add(s"Expected TransmissionFailure for $att, got $rep")
								case "ByteSourceProblem" =>
									if !rep.isInstanceOf[ByteSourceProblem] then txErrors.add(s"Expected ByteSourceProblem for $att, got $rep")
								case other =>
									txErrors.add(s"Missing outcome expectation for $att: $other")
							}
							txLatch.countDown()
							val follow = reentrantByTriggerKey.remove(att)
							if follow ne null then submitMsg(follow)
						}
					}
				}

				val stopDisruptor = new AtomicBoolean(false)
				val idleCounter = new AtomicInteger(0)
				val disruptor = new Thread(() => {
					val r = new Random(77L)
					while !stopDisruptor.get() do {
						tx.pruneTo(r.nextInt(sc.cfg.txMinBuffersOnIdle + 2))
						tx.triggerOnIdle(() => {
							idleCounter.incrementAndGet()
							()
						})
						LockSupport.parkNanos(200_000L)
					}
				}, "ptx-pbt-chaos-disruptor")
				disruptor.setDaemon(true)
				disruptor.start()

				try {
					val gate = new CountDownLatch(1)
					val submitDone = new CountDownLatch(threadMessages.size)
					for (envs, pid) <- threadMessages.zipWithIndex do {
						val t = new Thread(() => {
							gate.await()
							envs.foreach(e => submitMsg(e))
							submitDone.countDown()
						}, s"ptx-pbt-chaos-prod-$pid")
						t.setDaemon(true)
						t.start()
					}
					gate.countDown()
					assert(submitDone.await(15, TimeUnit.SECONDS), "Producers timed out")
					assert(txLatch.await(20, TimeUnit.SECONDS), s"TX callbacks timed out (remaining=${txLatch.getCount}, errors=$txErrors)")
					assert(rxLatch.await(20, TimeUnit.SECONDS), s"RX deliveries timed out (remaining=${rxLatch.getCount}, rxErrors=$rxErrors)")
				} finally {
					stopDisruptor.set(true)
					disruptor.join(2000L)
				}

				assert(txErrors.isEmpty, s"TX errors: $txErrors")
				assert(rxErrors.isEmpty, s"RX errors: $rxErrors")
				assertEquals(unreachedOpenCount.get(), 0, "Unreached trailing ByteSource was opened after an earlier source aborted")

				val txDeliveredVec = txDeliveredOrder.toVector
				val rxDeliveredVec = rxDelivered.map(m => (m.producerId, m.seq)).toVector
				assertEquals(rxDeliveredVec, txDeliveredVec)
				rxDelivered.foreach { actual =>
					val expected = expectedByKey.get((actual.producerId, actual.seq))
					assertEnvelopeMatches(expected, actual)
				}

				// Verify re-entrant transmit launched from inside a chained triggerOnIdle callback once quiescent.
				val postIdleEvents = new ArrayBuffer[String]()
				val postIdleDone = new CountDownLatch(1)
				val postProbeTxDone = new CountDownLatch(1)
				val postProbeDone = new CountDownLatch(1)
				val probeEnv = buildEnvelope(99, 1, MsgPlan(Array[Byte](1, 2, 3), markerStartsWithEscape = true, tagPayload = 1, versionTag = 1, msgVersionId = 1, streamSpecs = Nil, serializationDelayNanos = 0L), tempDir, bgExecutor)
				pump.requestReceive(() => fixedVersion) {
					case f: Receiver.Fault =>
						rxErrors.add(s"Post-idle probe RX fault: $f")
						postProbeDone.countDown()
					case env: WireEnvelope =>
						try assertEnvelopeMatches(probeEnv, env)
						catch { case NonFatal(e) => rxErrors.add(s"Post-idle probe mismatch: $e") }
						finally postProbeDone.countDown()
				}
				tx.triggerOnIdle(() => {
					postIdleEvents.synchronized(postIdleEvents += "idle1")
					submitMsg(probeEnv, isExtraProbe = true, probeLatch = postProbeTxDone)
				})
				tx.triggerOnIdle(() => {
					postIdleEvents.synchronized(postIdleEvents += "idle2")
					postIdleDone.countDown()
				})
				def dumpAllThreads(): String =
					Thread.getAllStackTraces.asScala.toVector
						.sortBy(_._1.getName)
						.map { case (t, st) =>
							val frames = st.take(20).map(e => s"    at $e").mkString("\n")
							s"Thread[${t.getName}, state=${t.getState}, daemon=${t.isDaemon}]:\n$frames"
						}
						.mkString("\n")

				if !postIdleDone.await(10, TimeUnit.SECONDS) then {
					fail(s"Chained onIdle callbacks did not run (txErrors=$txErrors, rxErrors=$rxErrors, idle=$postIdleEvents, txProbeRem=${postProbeTxDone.getCount}, rxProbeRem=${postProbeDone.getCount}, rxBuf=${rx.bufferCount}, txBuf=${tx.bufferCount})\n--- THREAD DUMP ---\n${dumpAllThreads()}")
				}
				if !postProbeTxDone.await(10, TimeUnit.SECONDS) then {
					fail(s"Re-entrant onIdle probe TX did not complete (txErrors=$txErrors, rxErrors=$rxErrors, idle=$postIdleEvents, rxProbeRem=${postProbeDone.getCount}, rxBuf=${rx.bufferCount}, txBuf=${tx.bufferCount})\n--- THREAD DUMP ---\n${dumpAllThreads()}")
				}
				if !postProbeDone.await(10, TimeUnit.SECONDS) then {
					fail(s"Re-entrant onIdle probe was not delivered to Receiver (txErrors=$txErrors, rxErrors=$rxErrors, idle=$postIdleEvents, rxBuf=${rx.bufferCount}, txBuf=${tx.bufferCount})\n--- THREAD DUMP ---\n${dumpAllThreads()}")
				}
				assert(txErrors.isEmpty, s"Post-idle TX errors: $txErrors")
				assert(rxErrors.isEmpty, s"Post-idle RX errors: $rxErrors")
				assertEquals(postIdleEvents.toList, List("idle1", "idle2"))

				val finalIdleLatch = new CountDownLatch(1)
				tx.triggerOnIdle(() => finalIdleLatch.countDown())
				assert(finalIdleLatch.await(5, TimeUnit.SECONDS))
				assertEquals(tx.pruneTo(1), 2)
				assertEquals(tx.bufferCount, 2)
				true
			}
		}
	}

	// ====================================================================================================
	// Property 7: Mock Socket Chaos (Arbitrary Partial Writes, 1-Byte Inline Stack Depth, Timeouts & Faults)
	// ====================================================================================================

	private case class ChaosMockScenario(
		buffersCapacity: Int,
		maxBuffers: Int,
		minBuffersOnIdle: Int,
		streamTransferCapacity: Int,
		maxGatheringBuffers: Int,
		timeoutValue: Long,
		timeoutUnit: TimeUnit,
		maxBytesPerWrite: Int,
		inlineProbabilityPct: Int,
		seed: Long,
		injectWriteFaultAt: Int,
		syncThrowFault: Boolean,
		useTimeoutException: Boolean,
		messages: Vector[(Array[Byte], List[StreamSpec])]
	)

	private val genChaosMockScenario: Gen[ChaosMockScenario] =
		for {
			bufCap <- Gen.frequency(2 -> Gen.const(16), 2 -> Gen.const(20), 1 -> Gen.const(32))
			maxBuf <- Gen.chooseNum(4, 12)
			minIdle <- Gen.chooseNum(1, math.min(3, maxBuf))
			streamCap <- Gen.oneOf(1, 2, 5, 13)
			gather <- Gen.chooseNum(2, 4)
			timeoutVal <- Gen.oneOf(1L, 42L, 500L)
			timeoutUnit <- Gen.oneOf(TimeUnit.MILLISECONDS, TimeUnit.MICROSECONDS, TimeUnit.SECONDS)
			maxChunkWrite <- Gen.frequency(2 -> Gen.const(1), 3 -> Gen.chooseNum(2, 7), 2 -> Gen.chooseNum(8, 32))
			inlinePct <- Gen.oneOf(0, 25, 75, 100)
			seed <- Gen.chooseNum(Long.MinValue, Long.MaxValue)
			injectFault <- Gen.frequency(3 -> Gen.const(-1), 2 -> Gen.chooseNum(0, 12))
			syncFault <- Gen.oneOf(false, true)
			timeoutEx <- Gen.oneOf(false, true)
			msgCount <- Gen.chooseNum(4, 12)
			maxMsgBytes = maxBuf * bufCap - 16
			msgs <- Gen.containerOfN[Vector, (Array[Byte], List[StreamSpec])](msgCount, for {
				ctrl <- genRandomBytes(0, math.min(96, maxMsgBytes))
				sCount <- Gen.chooseNum(0, 2)
				streams <- Gen.listOfN(sCount, Gen.frequency(
					1 -> Gen.const(StreamSpec.Empty),
					2 -> genRandomBytes(1, 36).map(StreamSpec.InlineBuffer.apply),
					2 -> (for {
						b <- genRandomBytes(1, 36)
						c <- Gen.chooseNum(1, 10)
						sched <- genAsyncScheduling
					} yield StreamSpec.AdversarialAsync(b, c, sched, 0))
				))
			} yield (ctrl, streams))
		} yield ChaosMockScenario(bufCap, maxBuf, minIdle, streamCap, gather, timeoutVal, timeoutUnit, maxChunkWrite, inlinePct, seed, injectFault, syncFault, timeoutEx, msgs)

	private def decodeControlAndStreamWire(wire: Array[Byte]): Vector[(Array[Byte], Boolean, Vector[Array[Byte]])] = {
		var pos = 0
		val reader: VLQ.ByteReader = () => {
			val b = wire(pos)
			pos += 1
			b
		}
		val out = Vector.newBuilder[(Array[Byte], Boolean, Vector[Array[Byte]])]
		while pos < wire.length do {
			val ctrlBuf = new java.io.ByteArrayOutputStream()
			var hasStream = false
			var done = false
			while !done do {
				val len = Integer.toUnsignedLong(VLQ.decodeUnsignedInt(reader))
				if len == 0L then done = true
				else if len == STREAM_FOLLOWS_SENTINEL then {
					hasStream = true
					done = true
				} else {
					val n = len.toInt
					ctrlBuf.write(wire, pos, n)
					pos += n
				}
			}
			val streamBuf = new java.io.ByteArrayOutputStream()
			if hasStream then {
				var sDone = false
				while !sDone do {
					val cLen = Integer.toUnsignedLong(VLQ.decodeUnsignedInt(reader))
					if cLen == 0L then sDone = true
					else {
						val n = cLen.toInt
						streamBuf.write(wire, pos, n)
						pos += n
					}
				}
			}
			out.addOne((ctrlBuf.toByteArray, hasStream, Vector(streamBuf.toByteArray)))
		}
		assertEquals(pos, wire.length)
		out.result()
	}

	property("PBT: arbitrary partial socket writes, inline recursion, timeout forwarding, and injected write faults never corrupt framing or drop callbacks") {
		Prop.forAllNoShrink(genChaosMockScenario) { sc =>
			val tempDir = Files.createTempDirectory("ptx-pbt-mock-")
			val bgExecutor = Executors.newFixedThreadPool(3, daemonThreadFactory("ptx-pbt-mock-bg"))
			try {
				val rng = new Random(sc.seed)
				val captured = new java.io.ByteArrayOutputStream()
				val writeCalls = new AtomicInteger(0)
				val injectedFaultTriggered = new AtomicBoolean(false)
				val timeoutMismatch = new AtomicBoolean(false)
				val maxObservedBatch = new AtomicInteger(0)

				val mockChannel = new AsynchronousSocketChannel(null) {
					override def write[A](
						srcs: Array[ByteBuffer],
						offset: Int,
						length: Int,
						timeout: Long,
						unit: TimeUnit,
						attachment: A,
						handler: CompletionHandler[java.lang.Long, ? >: A]
					): Unit = {
						if timeout != sc.timeoutValue || unit != sc.timeoutUnit then timeoutMismatch.set(true)
						maxObservedBatch.accumulateAndGet(length, math.max)
						val idx = writeCalls.getAndIncrement()
						if sc.injectWriteFaultAt >= 0 && idx == sc.injectWriteFaultAt && injectedFaultTriggered.compareAndSet(false, true) then {
							val exc: Throwable = if sc.useTimeoutException then new InterruptedByTimeoutException() else new IOException(s"Injected mock write fault at #$idx")
							if sc.syncThrowFault then throw exc
							else if rng.nextBoolean() then handler.failed(exc, attachment)
							else bgExecutor.execute(() => handler.failed(exc, attachment))
						} else {
							var remaining = sc.maxBytesPerWrite
							var total = 0L
							var i = offset
							val end = offset + length
							while i < end && remaining > 0 do {
								val b = srcs(i)
								val toRead = math.min(b.remaining(), remaining)
								if toRead > 0 then {
									val tmp = new Array[Byte](toRead)
									b.get(tmp)
									captured.synchronized(captured.write(tmp))
									total += toRead
									remaining -= toRead
								}
								i += 1
							}
							val n = java.lang.Long.valueOf(total)
							val doInline = rng.nextInt(100) < sc.inlineProbabilityPct
							if doInline then handler.completed(n, attachment)
							else bgExecutor.execute(() => handler.completed(n, attachment))
						}
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
					override def supportedOptions(): java.util.Set[SocketOption[?]] = Collections.emptySet()
					override def connect[A](remote: SocketAddress, attachment: A, handler: CompletionHandler[Void, ? >: A]): Unit = throw new UnsupportedOperationException()
					override def connect(remote: SocketAddress): Future[Void] = throw new UnsupportedOperationException()
					override def read[A](dst: ByteBuffer, timeout: Long, unit: TimeUnit, attachment: A, handler: CompletionHandler[Integer, ? >: A]): Unit = throw new UnsupportedOperationException()
					override def read(dst: ByteBuffer): Future[Integer] = throw new UnsupportedOperationException()
					override def read[A](dsts: Array[ByteBuffer], offset: Int, length: Int, timeout: Long, unit: TimeUnit, attachment: A, handler: CompletionHandler[java.lang.Long, ? >: A]): Unit = throw new UnsupportedOperationException()
					override def write[A](src: ByteBuffer, timeout: Long, unit: TimeUnit, attachment: A, handler: CompletionHandler[Integer, ? >: A]): Unit = throw new UnsupportedOperationException()
					override def write(src: ByteBuffer): Future[Integer] = throw new UnsupportedOperationException()
				}

				val tx = new PipelinedTransmitter[(Int, Array[Byte], List[ByteSource])](
					mockChannel,
					makeContext(new InetSocketAddress("127.0.0.1", 9001), new InetSocketAddress("127.0.0.1", 9002), ChannelOrigin.INITIATED),
					buffersCapacity = sc.buffersCapacity,
					maxBuffers = sc.maxBuffers,
					minBuffersOnIdle = sc.minBuffersOnIdle,
					streamTransferBufferCapacity = sc.streamTransferCapacity,
					maxGatheringBuffers = sc.maxGatheringBuffers
				)

				given Serializer[(Int, Array[Byte], List[ByteSource])] = (pair, writer) => {
					val (id, ctrl, sources) = pair
					writer.putInt(id)
					writer.putUnsignedIntVlq(ctrl.length)
					if ctrl.nonEmpty then writer.putBytes(ctrl)
					writer.putUnsignedIntVlq(sources.size)
					sources.foreach(writer.putByteSource)
				}

				val openReaderTrackers = new ArrayBuffer[AtomicInteger]()
				val closeReaderTrackers = new ArrayBuffer[AtomicInteger]()

				val expectedControls = new ArrayBuffer[Array[Byte]]()
				val expectedStreams = new ArrayBuffer[Array[Byte]]()
				val reports = new ArrayBuffer[(Int, Report)]()
				val doneLatch = new CountDownLatch(sc.messages.size)
				val overlapGuard = new AtomicBoolean(false)
				val overlapSeen = new AtomicBoolean(false)

				val payloads = sc.messages.zipWithIndex.map { case ((ctrl, specs), idx) =>
					val sources = specs.map { sp =>
						val oc = new AtomicInteger(0)
						val cc = new AtomicInteger(0)
						openReaderTrackers += oc
						closeReaderTrackers += cc
						wrapTrackedByteSource(materializeStream(sp, tempDir, bgExecutor), openCounter = oc, closeCounter = cc)
					}
					val expectedStreamAll = {
						val sOut = new java.io.ByteArrayOutputStream()
						specs.foreach(sp => sOut.write(expectedBytesOf(sp)))
						sOut.toByteArray
					}
					val expectedBuf = ByteBuffer.allocate(2048)
					val bw = new Serializer.Writer(new cluster.serialization.RecyclableIdentityIntMap()) {
						override protected def getBuffer(minRem: Int): ByteBuffer = expectedBuf
						override def governingVersion: ProtocolVersion = ProtocolVersion(1)
						override def position: Int = expectedBuf.position()
						override def putByteSource(source: ByteSource): Unit = {
							if source.size < 0 then throw new SerializationException(position, "neg")
							putUnsignedLongVlq(source.size)
						}
					}
					bw.putInt(idx)
					bw.putUnsignedIntVlq(ctrl.length)
					if ctrl.nonEmpty then bw.putBytes(ctrl)
					bw.putUnsignedIntVlq(sources.size)
					sources.foreach(bw.putByteSource)
					expectedBuf.flip()
					val expectedArr = new Array[Byte](expectedBuf.remaining())
					expectedBuf.get(expectedArr)
					expectedControls += expectedArr
					expectedStreams += expectedStreamAll
					(idx, ctrl, sources)
				}

				payloads.foreach { item =>
					tx.transmitWithAttachment(item, ProtocolVersion(1), item._1, timeout = sc.timeoutValue, timeUnit = sc.timeoutUnit) { (rep, id) =>
						if !overlapGuard.compareAndSet(false, true) then overlapSeen.set(true)
						try {
							reports.synchronized(reports += ((id, rep)))
							doneLatch.countDown()
						} finally {
							overlapGuard.set(false)
						}
					}
				}

				assert(doneLatch.await(15, TimeUnit.SECONDS), s"Mock PBT timed out (remaining=${doneLatch.getCount})")
				val idleLatch = new CountDownLatch(1)
				tx.triggerOnIdle(() => idleLatch.countDown())
				assert(idleLatch.await(5, TimeUnit.SECONDS), "Mock PBT failed to reach idle")

				assert(!overlapSeen.get(), "Detected overlapping callback execution")
				assert(!timeoutMismatch.get(), "Detected mismatched timeout/timeUnit passed to channel.write")
				assert(maxObservedBatch.get() <= sc.maxGatheringBuffers, s"Batch length ${maxObservedBatch.get()} exceeded ${sc.maxGatheringBuffers}")
				assertEquals(reports.map(_._1).toVector, (0 until sc.messages.size).toVector)
				assertEquals(openReaderTrackers.map(_.get()).sum, closeReaderTrackers.map(_.get()).sum, "Leaked open AsyncReader session")

				if injectedFaultTriggered.get() then {
					assert(reports.exists(_._2.isInstanceOf[TransmissionFailure]), s"Expected at least one TransmissionFailure after injected socket fault, got $reports")
				} else {
					assert(reports.forall(_._2 == Delivered), s"Expected all Delivered when no fault triggered, got $reports")
					val wire = captured.synchronized(captured.toByteArray)
					val decoded = decodeControlAndStreamWire(wire)
					assertEquals(decoded.size, sc.messages.size)
					for idx <- 0 until sc.messages.size do {
						assert(Arrays.equals(decoded(idx)._1, expectedControls(idx)), s"Control wire mismatch at $idx")
						val actualStream = decoded(idx)._3.head
						assert(Arrays.equals(actualStream, expectedStreams(idx)), s"Stream wire mismatch at $idx")
					}
				}
				assertEquals(tx.pruneTo(1), 2)
				true
			} finally {
				bgExecutor.shutdownNow()
				deleteTree(tempDir)
			}
		}
	}
}
