package readren.consensus
package nio2

import munit.ScalaCheckSuite
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll
import protocol.*
import readren.common.{BufferByteSource, ByteSink, ByteSource, FileByteSink, FileByteSource, Maybe}
import readren.consensus.ConsensusWireTypesCdm
import readren.nexus.cluster.channel.{PipelinedTransmitter, Receiver}
import readren.nexus.cluster.misc.VLQ
import readren.nexus.cluster.serialization.*
import readren.nexus.cluster.serialization.CommonSerializers.given
import readren.nexus.cluster.serialization.InvariantIterableFactory.given
import readren.nexus.cluster.service.{ChannelId, ChannelOrigin}

import java.io.IOException
import java.net.{InetSocketAddress, SocketAddress}
import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousServerSocketChannel, AsynchronousSocketChannel, CompletionHandler}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.{Executors, TimeUnit}
import scala.concurrent.{Await, ExecutionContext, Promise}
import scala.concurrent.duration.*
import scala.util.control.NonFatal

class ClusterWireStreamingSuite extends ScalaCheckSuite {

	override val munitTimeout: Duration = 60.seconds
	override val scalaCheckTestParameters: org.scalacheck.Test.Parameters =
		super.scalaCheckTestParameters.withMinSuccessfulTests(10)

	object TestSdm extends ConsensusWireTypesCdm {
		override type ParticipantId = String
		override type ClientId = String
		override type ClientCommand = String
		override type StateMachineResponse = String
	}

	private val wireProtocol = new ClusterWireProtocol(TestSdm)
	import wireProtocol.*
	import wireProtocol.given

	private inline def term(i: Int): Term = i.asInstanceOf[Term]

	val genMinimalBuffersCapacity: Gen[Int] = Gen.chooseNum(10, 128)
	val genMinimalStreamTransferCapacity: Gen[Int] = Gen.chooseNum(16, 256)
	val genChunkSliceSize: Gen[Int] = Gen.chooseNum(1, 64)

	private def withLoopbackConnection(
		buffersCapacity: Int = 8192,
		streamTransferBufferCapacity: Int = 64 * 1024,
		allocationLimits: AllocationLimits = AllocationLimits.Default
	)(testCode: (PipelinedTransmitter[ClusterWireMessage], Receiver, AsynchronousSocketChannel, AsynchronousSocketChannel) => Unit): Unit = {
		withTypedLoopbackConnection[ClusterWireMessage](buffersCapacity, streamTransferBufferCapacity, allocationLimits)(testCode)
	}

	private def withTypedLoopbackConnection[M](
		buffersCapacity: Int = 8192,
		streamTransferBufferCapacity: Int = 64 * 1024,
		allocationLimits: AllocationLimits = AllocationLimits.Default
	)(testCode: (PipelinedTransmitter[M], Receiver, AsynchronousSocketChannel, AsynchronousSocketChannel) => Unit): Unit = {
		val serverChannel = AsynchronousServerSocketChannel.open().bind(new InetSocketAddress("127.0.0.1", 0))
		val port = serverChannel.getLocalAddress.asInstanceOf[InetSocketAddress].getPort

		val clientChannel = AsynchronousSocketChannel.open()
		val connectFuture = clientChannel.connect(new InetSocketAddress("127.0.0.1", port))
		val acceptFuture = serverChannel.accept()

		connectFuture.get(5, TimeUnit.SECONDS)
		val serverConnection = acceptFuture.get(5, TimeUnit.SECONDS)

		val transmitterContext = new PipelinedTransmitter.Context {
			override def myAddress: SocketAddress = clientChannel.getLocalAddress
			override def oPeerAddress: Maybe[SocketAddress] = Maybe.some(serverConnection.getLocalAddress)
			override def channelId: ChannelId = new ChannelId(ChannelOrigin.INITIATED, Maybe.some(clientChannel.getLocalAddress))
		}
		val receiverContext = new Receiver.Context {
			override def myAddress: SocketAddress = serverConnection.getLocalAddress
			override def oPeerAddress: Maybe[SocketAddress] = Maybe.some(clientChannel.getLocalAddress)
			override def channelId: ChannelId = new ChannelId(ChannelOrigin.ACCEPTED, Maybe.some(serverConnection.getLocalAddress))
		}

		val transmitter = new PipelinedTransmitter[M](
			clientChannel,
			transmitterContext,
			buffersCapacity = buffersCapacity,
			streamTransferBufferCapacity = streamTransferBufferCapacity
		)
		val receiver = new Receiver(
			serverConnection,
			receiverContext,
			buffersCapacity = buffersCapacity,
			allocationLimits = allocationLimits,
			streamTransferBufferCapacity = streamTransferBufferCapacity
		)

		try {
			testCode(transmitter, receiver, clientChannel, serverConnection)
		} finally {
			try clientChannel.close() catch { case NonFatal(_) => () }
			try serverConnection.close() catch { case NonFatal(_) => () }
			try serverChannel.close() catch { case NonFatal(_) => () }
		}
	}

	private def createWriterAndReader(buffer: ByteBuffer): (Serializer.Writer, Deserializer.Reader) = {
		val writer = new Serializer.Writer {
			override val governingVersion: ProtocolVersion = ProtocolVersion(1)
			protected override def getBuffer(minimumRemaining: Int): ByteBuffer = buffer
			override def position: Int = buffer.position()
			override def putByteSource(source: ByteSource): Unit = {
				val size = source.size
				if size < 0 then throw new Serializer.SerializationException(position, s"Negative size ($size)")
				putUnsignedLongVlq(size)
			}
		}
		val reader = new Deserializer.Reader {
			override val governingVersion: ProtocolVersion = ProtocolVersion(1)
			override def position: Int = buffer.position()
			override def readByte(): Byte = {
				if buffer.remaining() < 1 then throw new Deserializer.DeserializationException(buffer.position(), "Unexpected end of buffer")
				buffer.get()
			}
			override def peekByte: Byte = {
				if buffer.remaining() < 1 then throw new Deserializer.DeserializationException(buffer.position(), "Unexpected end of buffer")
				buffer.get(buffer.position())
			}
		}
		(writer, reader)
	}

	// -------------------------------------------------------------------------
	// TC-01: Asynchronous Completion Trampolines & Mixed Synchronous Hand-offs
	// -------------------------------------------------------------------------
	test("TC-01: Asynchronous completion trampolines and race conditions round-trip losslessly under randomized minimal capacities") {
		val pool = Executors.newFixedThreadPool(2)
		val bgEc = ExecutionContext.fromExecutor(pool)
		try {
			for _ <- 0 until 5 do {
				val bufCap = genMinimalBuffersCapacity.sample.getOrElse(32)
				val streamCap = genMinimalStreamTransferCapacity.sample.getOrElse(64)
				val maxChunk = genChunkSliceSize.sample.getOrElse(16)
				val rawBytes = ("StreamingPayloadChunkData" * 20).getBytes(StandardCharsets.UTF_8)

				withLoopbackConnection(buffersCapacity = bufCap, streamTransferBufferCapacity = streamCap) { (transmitter, receiver, _, _) =>
					var offset = 0
					var isSync = true
					val alternatingSource = new ByteSource {
						override def size: Long = rawBytes.length.toLong
						override def openChannel(): java.nio.channels.ReadableByteChannel = ???
						override def transferTo(target: java.nio.channels.WritableByteChannel): Long = ???
						override def openAsyncReader(): ByteSource.AsyncReader = new ByteSource.AsyncReader {
							override def read[A](dst: ByteBuffer, attachment: A, handler: CompletionHandler[Integer, ? >: A]): Int = {
								val remaining = rawBytes.length - offset
								if remaining <= 0 then -1
								else {
									val toRead = math.min(math.min(dst.remaining(), remaining), maxChunk)
									dst.put(rawBytes, offset, toRead)
									offset += toRead
									val currentlySync = isSync
									isSync = !isSync
									if currentlySync then toRead
									else {
										bgEc.execute(() => handler.completed(toRead, attachment))
										ByteSource.AsyncReader.PENDING
									}
								}
							}
							override def close(): Unit = ()
						}
						override def close(): Unit = ()
					}

					val snapMsg: ClusterWireMessage = InstallSnapshot(
						correlationId = CorrelationId(1.toShort),
						inquirerTerm = term(1),
						snapshot = SnapshotData[String](10L, term(1), SoleElectorateChange[String](term(1), "req", term(1), Set("a"), Set("a")), 10L, alternatingSource),
						batch = IArray.empty[Record],
						leaderCommit = 10L,
						termAtLeaderCommit = term(1)
					)

					val recvPromise = Promise[ClusterWireMessage]()
					receiver.receive[ClusterWireMessage](ProtocolVersion(1), 5L, TimeUnit.SECONDS) {
						case m: ClusterWireMessage => recvPromise.success(m)
						case f: Receiver.Fault => recvPromise.failure(new RuntimeException(s"Receiver fault: $f"))
					}
					transmitter.transmit(snapMsg, ProtocolVersion(1), timeout = 5L, timeUnit = TimeUnit.SECONDS)(_ => ())

					val received = Await.result(recvPromise.future, 5.seconds)
					val snap = received.asInstanceOf[InstallSnapshot]
					assertEquals(snap.snapshot.stateMachineSnapshot.size, rawBytes.length.toLong)
					val readBack = new Array[Byte](rawBytes.length)
					val ch = snap.snapshot.stateMachineSnapshot.openChannel()
					ch.read(ByteBuffer.wrap(readBack))
					ch.close()
					assertEquals(readBack.toSeq, rawBytes.toSeq)
				}
			}
		} finally {
			pool.shutdown()
		}
	}

	// -------------------------------------------------------------------------
	// TC-02: Trailing Stream Leftover Buffer Management & Circular Storage Slicing
	// -------------------------------------------------------------------------
	test("TC-02: Leftover bytes exceeding single buffer in finishTrailingStream slice cleanly across circular storage") {
		withLoopbackConnection(buffersCapacity = 8192, streamTransferBufferCapacity = 64 * 1024) { (_, receiver, clientChannel, _) =>
			val snapPayload = new BufferSnapshotByteSource(ByteBuffer.wrap(new Array[Byte](100)))
			val snapMsg = InstallSnapshot(
				correlationId = CorrelationId(1.toShort),
				inquirerTerm = term(1),
				snapshot = SnapshotData[String](10L, term(1), SoleElectorateChange[String](term(1), "req", term(1), Set("a"), Set("a")), 10L, snapPayload),
				batch = IArray.empty[Record],
				leaderCommit = 10L,
				termAtLeaderCommit = term(1)
			)

			val largeCommandStr = "X" * 10240
			val largeAppendMsg = AppendRecords(
				correlationId = CorrelationId(2.toShort),
				inquirerTerm = term(1),
				prevLogIndex = 10L,
				prevLogTerm = term(1),
				batch = IArray(CommandRecord(term(1), largeCommandStr)),
				leaderCommit = 10L,
				termAtLeaderCommit = term(1)
			)

			val buf1 = ByteBuffer.allocate(8192)
			val (w1, _) = createWriterAndReader(buf1)
			wireProtocol.clusterWireMessageSerializer.serialize(snapMsg, w1)
			buf1.flip()

			val buf2 = ByteBuffer.allocate(16384)
			val (w2, _) = createWriterAndReader(buf2)
			wireProtocol.clusterWireMessageSerializer.serialize(largeAppendMsg, w2)
			buf2.flip()

			val combined = ByteBuffer.allocate(64 * 1024)
			val writerSAM: VLQ.ByteWriter = (b: Byte) => { combined.put(b); () }
			VLQ.encodeUnsignedInt(buf1.remaining(), writerSAM)
			combined.put(buf1)
			combined.put(PipelinedTransmitter.STREAM_FOLLOWS_SENTINEL_ENCODED)
			VLQ.encodeUnsignedInt(100, writerSAM)
			combined.put(new Array[Byte](100))
			combined.put(0: Byte)
			VLQ.encodeUnsignedInt(buf2.remaining(), writerSAM)
			combined.put(buf2)
			combined.put(0: Byte)
			combined.flip()

			while combined.hasRemaining do clientChannel.write(combined).get(5, TimeUnit.SECONDS)

			val p1 = Promise[ClusterWireMessage]()
			receiver.receive[ClusterWireMessage](ProtocolVersion(1), 5L, TimeUnit.SECONDS) {
				case m: ClusterWireMessage => p1.success(m)
				case f: Receiver.Fault => p1.failure(new RuntimeException(s"Fault on msg 1: $f"))
			}
			val msg1 = Await.result(p1.future, 5.seconds)
			assert(msg1.isInstanceOf[InstallSnapshot])

			val p2 = Promise[ClusterWireMessage]()
			receiver.receive[ClusterWireMessage](ProtocolVersion(1), 5L, TimeUnit.SECONDS) {
				case m: ClusterWireMessage => p2.success(m)
				case f: Receiver.Fault => p2.failure(new RuntimeException(s"Fault on msg 2: $f"))
			}
			val msg2 = Await.result(p2.future, 5.seconds)
			assert(msg2.isInstanceOf[AppendRecords])
		}
	}

	// -------------------------------------------------------------------------
	// TC-03: Pipelined Stream-After-Stream Transmission
	// -------------------------------------------------------------------------
	test("TC-03: Sequential transmission of consecutive streaming messages on same socket completes losslessly") {
		for _ <- 0 until 3 do {
			val bufCap = genMinimalBuffersCapacity.sample.getOrElse(48)
			val streamCap = genMinimalStreamTransferCapacity.sample.getOrElse(64)
			val payload1 = ("Stream1Data" * 15).getBytes(StandardCharsets.UTF_8)
			val payload2 = ("Stream2Data" * 25).getBytes(StandardCharsets.UTF_8)

			withLoopbackConnection(buffersCapacity = bufCap, streamTransferBufferCapacity = streamCap) { (transmitter, receiver, _, _) =>
				val snap1: ClusterWireMessage = InstallSnapshot(
					correlationId = CorrelationId(1.toShort),
					inquirerTerm = term(1),
					snapshot = SnapshotData[String](1L, term(1), SoleElectorateChange[String](term(1), "req1", term(1), Set("a"), Set("a")), 1L, new BufferSnapshotByteSource(ByteBuffer.wrap(payload1))),
					batch = IArray.empty[Record],
					leaderCommit = 1L,
					termAtLeaderCommit = term(1)
				)
				val snap2: ClusterWireMessage = InstallSnapshot(
					correlationId = CorrelationId(2.toShort),
					inquirerTerm = term(1),
					snapshot = SnapshotData[String](2L, term(1), SoleElectorateChange[String](term(1), "req2", term(1), Set("a"), Set("a")), 2L, new BufferSnapshotByteSource(ByteBuffer.wrap(payload2))),
					batch = IArray.empty[Record],
					leaderCommit = 2L,
					termAtLeaderCommit = term(1)
				)

				val p1 = Promise[ClusterWireMessage]()
				val p2 = Promise[ClusterWireMessage]()

				receiver.receive[ClusterWireMessage](ProtocolVersion(1), 5L, TimeUnit.SECONDS) {
					case m: ClusterWireMessage => p1.success(m)
					case f: Receiver.Fault => p1.failure(new RuntimeException(s"Fault 1: $f"))
				}
				transmitter.transmit(snap1, ProtocolVersion(1), timeout = 5L, timeUnit = TimeUnit.SECONDS) { _ =>
					transmitter.transmit(snap2, ProtocolVersion(1), timeout = 5L, timeUnit = TimeUnit.SECONDS)(_ => ())
				}

				val r1 = Await.result(p1.future, 5.seconds).asInstanceOf[InstallSnapshot]
				receiver.receive[ClusterWireMessage](ProtocolVersion(1), 5L, TimeUnit.SECONDS) {
					case m: ClusterWireMessage => p2.success(m)
					case f: Receiver.Fault => p2.failure(new RuntimeException(s"Fault 2: $f"))
				}
				val r2 = Await.result(p2.future, 5.seconds).asInstanceOf[InstallSnapshot]

				assertEquals(r1.snapshot.stateMachineSnapshot.size, payload1.length.toLong)
				assertEquals(r2.snapshot.stateMachineSnapshot.size, payload2.length.toLong)
			}
		}
	}

	// -------------------------------------------------------------------------
	// TC-04: Zero-Length ByteSource Inline Handling
	// -------------------------------------------------------------------------
	test("TC-04: Zero-length ByteSource round-trips without stream sentinels or framing mismatch under randomized capacities") {
		for _ <- 0 until 5 do {
			val bufCap = genMinimalBuffersCapacity.sample.getOrElse(32)
			withLoopbackConnection(buffersCapacity = bufCap) { (transmitter, receiver, _, _) =>
				val emptySource = new BufferSnapshotByteSource(ByteBuffer.allocate(0))
				val msg: ClusterWireMessage = InstallSnapshot(
					correlationId = CorrelationId(10.toShort),
					inquirerTerm = term(2),
					snapshot = SnapshotData[String](5L, term(2), SoleElectorateChange[String](term(2), "req0", term(1), Set("a"), Set("a")), 4L, emptySource),
					batch = IArray.empty[Record],
					leaderCommit = 5L,
					termAtLeaderCommit = term(2)
				)

				val promise = Promise[ClusterWireMessage]()
				receiver.receive[ClusterWireMessage](ProtocolVersion(1), 5L, TimeUnit.SECONDS) {
					case m: ClusterWireMessage => promise.success(m)
					case f: Receiver.Fault => promise.failure(new RuntimeException(s"Receiver fault: $f"))
				}
				transmitter.transmit(msg, ProtocolVersion(1), timeout = 5L, timeUnit = TimeUnit.SECONDS)(_ => ())

				val received = Await.result(promise.future, 5.seconds)
				val snap = received.asInstanceOf[InstallSnapshot]
				assertEquals(snap.snapshot.stateMachineSnapshot.size, 0L)
			}
		}
	}

	// -------------------------------------------------------------------------
	// TC-05: Zero-Length ByteSource Pipelined with Follow-Up Message
	// -------------------------------------------------------------------------
	test("TC-05: Zero-length ByteSource followed immediately by pipelined message in same frame deserializes cleanly") {
		withLoopbackConnection(buffersCapacity = 8192) { (_, receiver, clientChannel, _) =>
			val emptySource = new BufferSnapshotByteSource(ByteBuffer.allocate(0))
			val snapMsg = InstallSnapshot(
				correlationId = CorrelationId(1.toShort),
				inquirerTerm = term(1),
				snapshot = SnapshotData[String](5L, term(1), SoleElectorateChange[String](term(1), "req", term(1), Set("a"), Set("a")), 5L, emptySource),
				batch = IArray.empty[Record],
				leaderCommit = 5L,
				termAtLeaderCommit = term(1)
			)
			val quiesceMsg = PermitQuiescence(CorrelationId(2.toShort), 5L)

			val buf1 = ByteBuffer.allocate(4096)
			val (w1, _) = createWriterAndReader(buf1)
			wireProtocol.clusterWireMessageSerializer.serialize(snapMsg, w1)
			buf1.flip()

			val buf2 = ByteBuffer.allocate(4096)
			val (w2, _) = createWriterAndReader(buf2)
			wireProtocol.clusterWireMessageSerializer.serialize(quiesceMsg, w2)
			buf2.flip()

			val combined = ByteBuffer.allocate(16384)
			val writerSAM: VLQ.ByteWriter = (b: Byte) => { combined.put(b); () }
			VLQ.encodeUnsignedInt(buf1.remaining(), writerSAM)
			combined.put(buf1)
			combined.put(0: Byte) // terminal sentinel for message 1
			VLQ.encodeUnsignedInt(buf2.remaining(), writerSAM)
			combined.put(buf2)
			combined.put(0: Byte) // terminal sentinel for message 2
			combined.flip()

			while combined.hasRemaining do clientChannel.write(combined).get(5, TimeUnit.SECONDS)

			val p1 = Promise[ClusterWireMessage]()
			receiver.receive[ClusterWireMessage](ProtocolVersion(1), 5L, TimeUnit.SECONDS) {
				case m: ClusterWireMessage => p1.success(m)
				case f: Receiver.Fault => p1.failure(new RuntimeException(s"Fault 1: $f"))
			}
			val r1 = Await.result(p1.future, 5.seconds)
			assert(r1.isInstanceOf[InstallSnapshot])

			val p2 = Promise[ClusterWireMessage]()
			receiver.receive[ClusterWireMessage](ProtocolVersion(1), 5L, TimeUnit.SECONDS) {
				case m: ClusterWireMessage => p2.success(m)
				case f: Receiver.Fault => p2.failure(new RuntimeException(s"Fault 2: $f"))
			}
			val r2 = Await.result(p2.future, 5.seconds)
			assert(r2.isInstanceOf[PermitQuiescence])
		}
	}

	// -------------------------------------------------------------------------
	// TC-06: Malformed Chunk Header & Length Bounds Validation
	// -------------------------------------------------------------------------
	test("TC-06: Malformed chunk header with > 5 continuation bytes triggers protocol fault") {
		withLoopbackConnection(buffersCapacity = 8192) { (_, receiver, clientChannel, _) =>
			val snapPayload = new BufferSnapshotByteSource(ByteBuffer.wrap(new Array[Byte](100)))
			val snapMsg = InstallSnapshot(
				correlationId = CorrelationId(1.toShort),
				inquirerTerm = term(1),
				snapshot = SnapshotData[String](10L, term(1), SoleElectorateChange[String](term(1), "req", term(1), Set("a"), Set("a")), 10L, snapPayload),
				batch = IArray.empty[Record],
				leaderCommit = 10L,
				termAtLeaderCommit = term(1)
			)

			val buf1 = ByteBuffer.allocate(8192)
			val (w1, _) = createWriterAndReader(buf1)
			wireProtocol.clusterWireMessageSerializer.serialize(snapMsg, w1)
			buf1.flip()

			val combined = ByteBuffer.allocate(64 * 1024)
			val writerSAM: VLQ.ByteWriter = (b: Byte) => { combined.put(b); () }
			VLQ.encodeUnsignedInt(buf1.remaining(), writerSAM)
			combined.put(buf1)
			combined.put(PipelinedTransmitter.STREAM_FOLLOWS_SENTINEL_ENCODED)
			for _ <- 0 until 7 do combined.put(0x80.toByte)
			combined.put(0x01.toByte)
			combined.flip()

			while combined.hasRemaining do clientChannel.write(combined).get(5, TimeUnit.SECONDS)

			val p = Promise[ClusterWireMessage]()
			receiver.receive[ClusterWireMessage](ProtocolVersion(1), 5L, TimeUnit.SECONDS) {
				case m: ClusterWireMessage => p.success(m)
				case f: Receiver.Fault => p.failure(new RuntimeException(s"Fault: $f"))
			}

			val result = intercept[RuntimeException] {
				Await.result(p.future, 2.seconds)
			}
			assert(result.getMessage.contains("Fault") && result.getMessage.toLowerCase.contains("chunk header"))
		}
	}

	test("TC-06B: Chunk size exceeding remaining expected stream bytes triggers protocol fault") {
		withLoopbackConnection(buffersCapacity = 8192) { (_, receiver, clientChannel, _) =>
			val snapPayload = new BufferSnapshotByteSource(ByteBuffer.wrap(new Array[Byte](50)))
			val snapMsg = InstallSnapshot(
				correlationId = CorrelationId(1.toShort),
				inquirerTerm = term(1),
				snapshot = SnapshotData[String](10L, term(1), SoleElectorateChange[String](term(1), "req", term(1), Set("a"), Set("a")), 10L, snapPayload),
				batch = IArray.empty[Record],
				leaderCommit = 10L,
				termAtLeaderCommit = term(1)
			)

			val buf1 = ByteBuffer.allocate(4096)
			val (w1, _) = createWriterAndReader(buf1)
			wireProtocol.clusterWireMessageSerializer.serialize(snapMsg, w1)
			buf1.flip()

			val combined = ByteBuffer.allocate(16384)
			val writerSAM: VLQ.ByteWriter = (b: Byte) => { combined.put(b); () }
			VLQ.encodeUnsignedInt(buf1.remaining(), writerSAM)
			combined.put(buf1)
			combined.put(PipelinedTransmitter.STREAM_FOLLOWS_SENTINEL_ENCODED)
			// Declared size in snapPayload was 50, but chunk header claims 100 bytes:
			VLQ.encodeUnsignedInt(100, writerSAM)
			combined.put(new Array[Byte](100))
			combined.flip()

			while combined.hasRemaining do clientChannel.write(combined).get(5, TimeUnit.SECONDS)

			val p = Promise[ClusterWireMessage]()
			receiver.receive[ClusterWireMessage](ProtocolVersion(1), 5L, TimeUnit.SECONDS) {
				case m: ClusterWireMessage => p.success(m)
				case f: Receiver.Fault => p.failure(new RuntimeException(s"Fault: $f"))
			}

			val result = intercept[RuntimeException] {
				Await.result(p.future, 2.seconds)
			}
			assert(result.getMessage.contains("Fault"))
		}
	}

	// -------------------------------------------------------------------------
	// TC-07: Payload Size Validation against AllocationLimits
	// -------------------------------------------------------------------------
	test("TC-07: Declared ByteSource size exceeding AllocationLimits triggers rejection before sink allocation") {
		val tightLimits = new AllocationLimits(maxByteSourceBytes = 256L)
		withLoopbackConnection(buffersCapacity = 8192, allocationLimits = tightLimits) { (transmitter, receiver, _, _) =>
			val oversizedPayload = new BufferSnapshotByteSource(ByteBuffer.wrap(new Array[Byte](1024)))
			val snapMsg: ClusterWireMessage = InstallSnapshot(
				correlationId = CorrelationId(1.toShort),
				inquirerTerm = term(1),
				snapshot = SnapshotData[String](10L, term(1), SoleElectorateChange[String](term(1), "req", term(1), Set("a"), Set("a")), 10L, oversizedPayload),
				batch = IArray.empty[Record],
				leaderCommit = 10L,
				termAtLeaderCommit = term(1)
			)

			val p = Promise[ClusterWireMessage]()
			receiver.receive[ClusterWireMessage](ProtocolVersion(1), 5L, TimeUnit.SECONDS) {
				case m: ClusterWireMessage => p.success(m)
				case f: Receiver.Fault => p.failure(new RuntimeException(s"Receiver fault: $f"))
			}
			transmitter.transmit(snapMsg, ProtocolVersion(1), timeout = 5L, timeUnit = TimeUnit.SECONDS)(_ => ())

			val result = intercept[RuntimeException] {
				Await.result(p.future, 2.seconds)
			}
			assert(result.getMessage.toLowerCase.contains("fault") && result.getMessage.contains("ByteSource") && result.getMessage.contains("maximum safe allocation threshold"))
		}
	}

	// -------------------------------------------------------------------------
	// TC-08: Outbound Mid-Stream Abort & Receiver Recovery
	// -------------------------------------------------------------------------
	test("TC-08: Outbound AsyncReader failure mid-stream emits corruption sentinel and Receiver skips aborted package on open channel") {
		withLoopbackConnection(buffersCapacity = 4096, streamTransferBufferCapacity = 64) { (transmitter, receiver, _, _) =>
			var chunkCount = 0
			val failingSource = new ByteSource {
				override def size: Long = 500L
				override def openChannel(): java.nio.channels.ReadableByteChannel = ???
				override def transferTo(target: java.nio.channels.WritableByteChannel): Long = ???
				override def openAsyncReader(): ByteSource.AsyncReader = new ByteSource.AsyncReader {
					override def read[A](dst: ByteBuffer, attachment: A, handler: CompletionHandler[Integer, ? >: A]): Int = {
						chunkCount += 1
						if chunkCount == 1 then {
							dst.put("InitialChunk".getBytes(StandardCharsets.UTF_8))
							12
						} else {
							throw new IOException("Simulated disk read error midway through stream")
						}
					}
					override def close(): Unit = ()
				}
				override def close(): Unit = ()
			}

			val snapMsg: ClusterWireMessage = InstallSnapshot(
				correlationId = CorrelationId(1.toShort),
				inquirerTerm = term(1),
				snapshot = SnapshotData[String](10L, term(1), SoleElectorateChange[String](term(1), "req", term(1), Set("a"), Set("a")), 10L, failingSource),
				batch = IArray.empty[Record],
				leaderCommit = 10L,
				termAtLeaderCommit = term(1)
			)
			val followUpMsg: ClusterWireMessage = PermitQuiescence(CorrelationId(2.toShort), 77L)

			val recvPromise = Promise[ClusterWireMessage]()
			receiver.receive[ClusterWireMessage](ProtocolVersion(1), 5L, TimeUnit.SECONDS) {
				case m: ClusterWireMessage => recvPromise.success(m)
				case f: Receiver.Fault => recvPromise.failure(new RuntimeException(s"Fault: $f"))
			}

			val transmitPromise1 = Promise[PipelinedTransmitter.Report]()
			transmitter.transmit(snapMsg, ProtocolVersion(1), timeout = 5L, timeUnit = TimeUnit.SECONDS) { report =>
				transmitPromise1.success(report)
			}
			val transmitPromise2 = Promise[PipelinedTransmitter.Report]()
			transmitter.transmit(followUpMsg, ProtocolVersion(1), timeout = 5L, timeUnit = TimeUnit.SECONDS) { report =>
				transmitPromise2.success(report)
			}

			val transmitReport1 = Await.result(transmitPromise1.future, 3.seconds)
			assert(transmitReport1.isInstanceOf[PipelinedTransmitter.ByteSourceProblem])

			val transmitReport2 = Await.result(transmitPromise2.future, 3.seconds)
			assertEquals(transmitReport2, PipelinedTransmitter.Delivered)

			val received = Await.result(recvPromise.future, 3.seconds)
			assertEquals(received, followUpMsg)
		}
	}

	// -------------------------------------------------------------------------
	// TC-09: Inbound Mid-Stream Peer Abort Sentinel Recovery
	// -------------------------------------------------------------------------
	test("TC-09: Peer emitting in-band corruption sentinel skips aborted stream and receives subsequent message") {
		withLoopbackConnection(buffersCapacity = 8192) { (_, receiver, clientChannel, _) =>
			val snapPayload = new BufferSnapshotByteSource(ByteBuffer.wrap(new Array[Byte](200)))
			val snapMsg = InstallSnapshot(
				correlationId = CorrelationId(1.toShort),
				inquirerTerm = term(1),
				snapshot = SnapshotData[String](10L, term(1), SoleElectorateChange[String](term(1), "req", term(1), Set("a"), Set("a")), 10L, snapPayload),
				batch = IArray.empty[Record],
				leaderCommit = 10L,
				termAtLeaderCommit = term(1)
			)
			val followUpMsg: ClusterWireMessage = PermitQuiescence(CorrelationId(2.toShort), 88L)

			val buf1 = ByteBuffer.allocate(4096)
			val (w1, _) = createWriterAndReader(buf1)
			wireProtocol.clusterWireMessageSerializer.serialize(snapMsg, w1)
			buf1.flip()

			val buf2 = ByteBuffer.allocate(4096)
			val (w2, _) = createWriterAndReader(buf2)
			wireProtocol.clusterWireMessageSerializer.serialize(followUpMsg, w2)
			buf2.flip()

			val combined = ByteBuffer.allocate(16384)
			val writerSAM: VLQ.ByteWriter = (b: Byte) => { combined.put(b); () }
			VLQ.encodeUnsignedInt(buf1.remaining(), writerSAM)
			combined.put(buf1)
			combined.put(PipelinedTransmitter.STREAM_FOLLOWS_SENTINEL_ENCODED)
			// Chunk 1: 50 bytes
			VLQ.encodeUnsignedInt(50, writerSAM)
			combined.put(new Array[Byte](50))
			// Instead of chunk 2, send CORRUPTED_PACKAGE_SENTINEL:
			combined.put(PipelinedTransmitter.CORRUPTED_PACKAGE_SENTINEL_ENCODED)
			// Immediately pipeline followUpMsg on the same open connection:
			VLQ.encodeUnsignedInt(buf2.remaining(), writerSAM)
			combined.put(buf2)
			combined.put(0: Byte)
			combined.flip()

			while combined.hasRemaining do clientChannel.write(combined).get(5, TimeUnit.SECONDS)

			val p = Promise[ClusterWireMessage]()
			receiver.receive[ClusterWireMessage](ProtocolVersion(1), 5L, TimeUnit.SECONDS) {
				case m: ClusterWireMessage => p.success(m)
				case f: Receiver.Fault => p.failure(new RuntimeException(s"Fault: $f"))
			}

			val received = Await.result(p.future, 3.seconds)
			assertEquals(received, followUpMsg)
		}
	}

	// -------------------------------------------------------------------------
	// TC-12: Pipelined Queueing During Active Trailing Streaming
	// -------------------------------------------------------------------------
	test("TC-12: PipelinedTransmitter queues and pipelines concurrent transmission during active trailing streaming") {
		withLoopbackConnection(buffersCapacity = 8192) { (transmitter, receiver, _, _) =>
			val readInitiatedPromise = Promise[(ByteBuffer, Any, CompletionHandler[Integer, Any])]()
			val payloadBytes = Array.tabulate[Byte](1024)(i => (i & 0xff).toByte)
			val slowByteSource = new ByteSource {
				private var offset = 0
				override def size: Long = payloadBytes.length.toLong
				override def openChannel(): java.nio.channels.ReadableByteChannel = ???
				override def transferTo(target: java.nio.channels.WritableByteChannel): Long = ???
				override def openAsyncReader(): ByteSource.AsyncReader = new ByteSource.AsyncReader {
					override def read[A](dst: ByteBuffer, attachment: A, handler: CompletionHandler[Integer, ? >: A]): Int = {
						if offset == 0 then {
							offset = payloadBytes.length
							readInitiatedPromise.trySuccess((dst, attachment, handler.asInstanceOf[CompletionHandler[Integer, Any]]))
							ByteSource.AsyncReader.PENDING
						} else -1
					}
					override def close(): Unit = ()
				}
				override def close(): Unit = ()
			}

			val streamingMsg: ClusterWireMessage = InstallSnapshot(
				correlationId = CorrelationId(1.toShort),
				inquirerTerm = term(1),
				snapshot = SnapshotData[String](1L, term(1), SoleElectorateChange[String](term(1), "req", term(1), Set("a"), Set("a")), 1L, slowByteSource),
				batch = IArray.empty[Record],
				leaderCommit = 1L,
				termAtLeaderCommit = term(1)
			)
			val concurrentMsg: ClusterWireMessage = PermitQuiescence(CorrelationId(2.toShort), 10L)

			val report1 = Promise[PipelinedTransmitter.Report]()
			val report2 = Promise[PipelinedTransmitter.Report]()
			val recv1 = Promise[ClusterWireMessage]()
			val recv2 = Promise[ClusterWireMessage]()

			receiver.receive[ClusterWireMessage](ProtocolVersion(1), 5L, TimeUnit.SECONDS) {
				case m: ClusterWireMessage =>
					recv1.success(m)
					receiver.receive[ClusterWireMessage](ProtocolVersion(1), 5L, TimeUnit.SECONDS) {
						case m2: ClusterWireMessage => recv2.success(m2)
						case f2: Receiver.Fault => recv2.failure(new RuntimeException(s"Fault2: $f2"))
					}
				case f: Receiver.Fault => recv1.failure(new RuntimeException(s"Fault1: $f"))
			}

			transmitter.transmit(streamingMsg, ProtocolVersion(1), timeout = 5L, timeUnit = TimeUnit.SECONDS)(r => report1.success(r))
			val (dst, att, handler) = Await.result(readInitiatedPromise.future, 2.seconds)

			// Concurrent transmission during active trailing streaming is serialized in the serialization stage and queued cleanly without throwing.
			transmitter.transmit(concurrentMsg, ProtocolVersion(1), timeout = 5L, timeUnit = TimeUnit.SECONDS)(r => report2.success(r))

			// Resume the paused trailing stream by supplying the remaining bytes asynchronously.
			dst.put(payloadBytes)
			handler.completed(Integer.valueOf(payloadBytes.length), att)

			assertEquals(Await.result(report1.future, 3.seconds), PipelinedTransmitter.Delivered)
			assertEquals(Await.result(report2.future, 3.seconds), PipelinedTransmitter.Delivered)

			val got1 = Await.result(recv1.future, 3.seconds).asInstanceOf[InstallSnapshot]
			assertEquals(got1.correlationId, CorrelationId(1.toShort))
			got1.snapshot.stateMachineSnapshot.close()

			val got2 = Await.result(recv2.future, 3.seconds)
			assertEquals(got2, concurrentMsg)
		}
	}

	// -------------------------------------------------------------------------
	// TC-14: Multi-Span Control Frame Serialization with getBuffer
	// -------------------------------------------------------------------------
	test("TC-14: Multi-buffer frame serialization flushes do not emit premature sentinels") {
		// buffersCapacity = 32 bytes forces a 500-byte AppendRecords message to span ~16 circular storage buffers
		withLoopbackConnection(buffersCapacity = 32) { (transmitter, receiver, _, _) =>
			val largePayload = "MultiBufferFramingTestContentPayload" * 15
			val appendMsg: ClusterWireMessage = AppendRecords(
				correlationId = CorrelationId(99.toShort),
				inquirerTerm = term(3),
				prevLogIndex = 100L,
				prevLogTerm = term(3),
				batch = IArray(CommandRecord(term(3), largePayload)),
				leaderCommit = 100L,
				termAtLeaderCommit = term(3)
			)

			val p = Promise[ClusterWireMessage]()
			receiver.receive[ClusterWireMessage](ProtocolVersion(1), 5L, TimeUnit.SECONDS) {
				case m: ClusterWireMessage => p.success(m)
				case f: Receiver.Fault => p.failure(new RuntimeException(s"Fault: $f"))
			}

			transmitter.transmit(appendMsg, ProtocolVersion(1), timeout = 5L, timeUnit = TimeUnit.SECONDS)(_ => ())
			val received = Await.result(p.future, 5.seconds).asInstanceOf[AppendRecords]
			assertEquals(received.batch.length, 1)
			val recCmd = received.batch(0).asInstanceOf[CommandRecord[String]]
			assertEquals(recCmd.command, largePayload)
		}
	}

	// -------------------------------------------------------------------------
	// TC-15: Multiple ByteSources per Package
	// -------------------------------------------------------------------------
	case class CompositeStreamMessage(id: Long, sourceA: ByteSource, sourceB: ByteSource)

	test("TC-15: Multiple ByteSources per message are streamed sequentially into distinct sinks") {
		val ser: Serializer[CompositeStreamMessage] = (value: CompositeStreamMessage, writer: Serializer.Writer) => {
			writer.putLong(value.id)
			writer.putByteSource(value.sourceA)
			writer.putByteSource(value.sourceB)
		}
		val deser: Deserializer[CompositeStreamMessage] = (reader: Deserializer.Reader) => {
			val id = reader.readLong()
			val sA = reader.readByteSource()
			val sB = reader.readByteSource()
			CompositeStreamMessage(id, sA, sB)
		}

		val payloadA = "FirstStreamPayloadData".getBytes(StandardCharsets.UTF_8)
		val payloadB = "SecondStreamPayloadData-WithDifferentLength".getBytes(StandardCharsets.UTF_8)

		withTypedLoopbackConnection[CompositeStreamMessage](buffersCapacity = 48, streamTransferBufferCapacity = 32) { (transmitter, receiver, _, _) =>
			val msg = CompositeStreamMessage(
				42L,
				new BufferSnapshotByteSource(ByteBuffer.wrap(payloadA)),
				new BufferSnapshotByteSource(ByteBuffer.wrap(payloadB))
			)

			val p = Promise[CompositeStreamMessage]()
			receiver.receive[CompositeStreamMessage](ProtocolVersion(1), 5L, TimeUnit.SECONDS) {
				case m: CompositeStreamMessage => p.success(m)
				case f: Receiver.Fault => p.failure(new RuntimeException(s"Fault: $f"))
			}(using deser)

			transmitter.transmit(msg, ProtocolVersion(1), timeout = 5L, timeUnit = TimeUnit.SECONDS)(_ => ())(using ser)

			val received = Await.result(p.future, 5.seconds)
			assertEquals(received.id, 42L)
			assertEquals(received.sourceA.size, payloadA.length.toLong)
			assertEquals(received.sourceB.size, payloadB.length.toLong)

			val readBackA = new Array[Byte](payloadA.length)
			val chA = received.sourceA.openChannel()
			chA.read(ByteBuffer.wrap(readBackA))
			chA.close()
			assertEquals(readBackA.toSeq, payloadA.toSeq)

			val readBackB = new Array[Byte](payloadB.length)
			val chB = received.sourceB.openChannel()
			chB.read(ByteBuffer.wrap(readBackB))
			chB.close()
			assertEquals(readBackB.toSeq, payloadB.toSeq)
		}
	}
}
