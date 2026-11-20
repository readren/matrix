package readren.consensus
package nio2

import munit.ScalaCheckSuite
import readren.common.*
import readren.consensus.ConsensusWireTypesCdm
import readren.consensus.protocol.*
import readren.nexus.cluster.channel.RingLengthExceededException
import readren.nexus.cluster.misc.RingLength
import readren.nexus.cluster.serialization.*
import readren.nexus.cluster.serialization.CommonSerializers.given
import readren.sequencer.SchedulingDoer
import readren.sequencer.providers.CooperativeThreadDrivenSchedulerDp

import java.io.IOException
import java.net.{InetSocketAddress, SocketAddress}
import java.nio.ByteBuffer
import java.nio.channels.{ClosedChannelException, CompletionHandler, ReadableByteChannel}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.Comparator
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{TimeUnit, *}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.*
import scala.concurrent.{Await, Promise}

class RecordBatchStreamingTest extends ScalaCheckSuite {

	override val munitTimeout: Duration = 60.seconds

	object WireTestSdm extends ConsensusWireTypesCdm {
		override type ParticipantId = String
		override type ClientId = String
		override type ClientCommand = String
		override type StateMachineResponse = String
	}

	private val strSerializer: Serializer[String] = summon
	private val strDeserializer: Deserializer[String] = summon

	private val wireProtocol = new ClusterWireProtocol(WireTestSdm)
	import wireProtocol.{*, given}

	private inline def term(i: Int): Term = i.asInstanceOf[Term]

	//// 1. Transport-Generic Primitives Unit Tests ////

	test("ChunkedBufferByteSource supports multi-chunk reads, independent sessions, transferTo, and synchronous AsyncReader contract") {
		val chunk1 = ByteBuffer.wrap("Hello, ".getBytes(StandardCharsets.UTF_8))
		val chunk2 = ByteBuffer.wrap("chunked ".getBytes(StandardCharsets.UTF_8))
		val chunk3 = ByteBuffer.wrap("world!".getBytes(StandardCharsets.UTF_8))
		val source = ChunkedBufferByteSource(IArray(chunk1, chunk2, chunk3))
		val expectedBytes = "Hello, chunked world!".getBytes(StandardCharsets.UTF_8)

		assertEquals(source.size, expectedBytes.length.toLong)

		// 1. AsyncReader synchronous contract: never invokes CompletionHandler inline, returns bytes read >= 0 or -1 on EOF
		val reader1 = source.openAsyncReader()
		val reader2 = source.openAsyncReader()
		val handlerCalledInline = new AtomicBoolean(false)
		val forbiddenHandler = new CompletionHandler[Integer, Null] {
			override def completed(result: Integer, attachment: Null): Unit = handlerCalledInline.set(true)
			override def failed(exc: Throwable, attachment: Null): Unit = handlerCalledInline.set(true)
		}

		val acc1 = new ArrayBuffer[Byte]()
		val smallDst = ByteBuffer.allocate(5)
		var status = 0
		while {
			smallDst.clear()
			status = reader1.read(smallDst, null, forbiddenHandler)
			status > 0
		} do {
			smallDst.flip()
			while smallDst.hasRemaining do acc1.addOne(smallDst.get())
		}
		assertEquals(status, -1)
		assert(!handlerCalledInline.get(), "ChunkedBufferByteSource.AsyncReader must never invoke CompletionHandler inline")
		assertEquals(acc1.toSeq, expectedBytes.toSeq)

		// 2. Concurrent second reader session is completely independent of reader1's exhausted cursor
		val fullDst = ByteBuffer.allocate(64)
		val read2Count = reader2.read(fullDst, null, forbiddenHandler)
		assertEquals(read2Count, expectedBytes.length)
		fullDst.flip()
		val arr2 = new Array[Byte](fullDst.remaining())
		fullDst.get(arr2)
		assertEquals(arr2.toSeq, expectedBytes.toSeq)

		reader1.close()
		reader2.close()
		intercept[ClosedChannelException] {
			reader1.read(ByteBuffer.allocate(4), null, forbiddenHandler)
		}

		// 3. openChannel and transferTo
		val channel = source.openChannel()
		val chDst = ByteBuffer.allocate(expectedBytes.length)
		var totalChRead = 0
		while chDst.hasRemaining do {
			val n = channel.read(chDst)
			assert(n > 0)
			totalChRead += n
		}
		assertEquals(totalChRead, expectedBytes.length)
		assertEquals(channel.read(ByteBuffer.allocate(1)), -1)
		channel.close()

		val sinkBuffer = ByteBuffer.allocate(expectedBytes.length)
		val writable = new java.nio.channels.WritableByteChannel {
			override def write(src: ByteBuffer): Int = {
				val n = src.remaining()
				sinkBuffer.put(src)
				n
			}
			override def isOpen: Boolean = true
			override def close(): Unit = ()
		}
		assertEquals(source.transferTo(writable), expectedBytes.length.toLong)
		sinkBuffer.flip()
		val transferredBytes = new Array[Byte](sinkBuffer.remaining())
		sinkBuffer.get(transferredBytes)
		assertEquals(transferredBytes.toSeq, expectedBytes.toSeq)
	}

	test("ChunkedHeapWriter grows across chunk boundaries and enforces exact maxEncodedBytes limit") {
		val writer = new ChunkedHeapWriter(ProtocolVersion(1), chunkCapacity = 16, maxEncodedBytes = 30L)
		writer.putLong(0x0102030405060708L) // 8 bytes
		writer.putLong(0x1112131415161718L) // 16 bytes (fills chunk 1)
		writer.putInt(0x21222324)           // 20 bytes (allocates chunk 2)
		val tenBytes = Array.tabulate[Byte](10)(i => (i + 1).toByte)
		writer.putBytes(tenBytes)           // 30 bytes (exact limit)
		assertEquals(writer.totalBytesWritten, 30L)

		// Writing 1 more byte beyond 30L must fail with SerializationException
		intercept[Serializer.SerializationException] {
			writer.putByte(1: Byte)
		}

		val (chunks, totalBytes) = writer.finish()
		assertEquals(totalBytes, 30L)
		val source = new ChunkedBufferByteSource(chunks, totalBytes)
		val ch = source.openChannel()
		val reader = new ReadableChannelReader(ch, totalBytes, ProtocolVersion(1), transferBufferCapacity = 7)
		assertEquals(reader.readLong(), 0x0102030405060708L)
		assertEquals(reader.readLong(), 0x1112131415161718L)
		assertEquals(reader.readInt(), 0x21222324)
		assertEquals(reader.readBytes(10).toSeq, tenBytes.toSeq)
		reader.requireFullyConsumed()
		ch.close()
	}

	test("NestedScopeReader and ReadableChannelReader enforce region bounds, requireFullyConsumed, and support > Int.MaxValue stream bounds") {
		val writer = new ChunkedHeapWriter(ProtocolVersion(1), chunkCapacity = 64)
		writer.putInt(42)
		writer.putInt(99)
		val (chunks, totalBytes) = writer.finish()
		assertEquals(totalBytes, 8L)

		val source = new ChunkedBufferByteSource(chunks, totalBytes)
		val ch = source.openChannel()
		val channelReader = new ReadableChannelReader(ch, totalBytes, ProtocolVersion(1))
		val nested = new NestedScopeReader(channelReader, byteLength = 4)
		assertEquals(nested.readInt(), 42)
		nested.requireFullyConsumed()

		// Over-reading nested scope must throw LengthExceedsRemainingBytesException
		intercept[Deserializer.LengthExceedsRemainingBytesException] {
			nested.readByte()
		}

		// Under-consuming channelReader must fail requireFullyConsumed
		intercept[Deserializer.DeserializationException] {
			channelReader.requireFullyConsumed()
		}
		assertEquals(channelReader.readInt(), 99)
		channelReader.requireFullyConsumed()
		ch.close()

		// Synthetic > Int.MaxValue stream length clamping check without allocating 2 GiB
		val syntheticHugeSize = Int.MaxValue.toLong + 5000L
		val syntheticChannel = new ReadableByteChannel {
			override def read(dst: ByteBuffer): Int = {
				val n = dst.remaining()
				while dst.hasRemaining do dst.put(0: Byte)
				n
			}
			override def isOpen: Boolean = true
			override def close(): Unit = ()
		}
		val hugeReader = new ReadableChannelReader(syntheticHugeSizeChannel(syntheticChannel), syntheticHugeSize, ProtocolVersion(1))
		assertEquals(hugeReader.remainingStreamBytes, syntheticHugeSize)
		assertEquals(hugeReader.remainingContentBytes, Int.MaxValue)
		assertEquals(hugeReader.readLong(), 0L)
		assertEquals(hugeReader.remainingStreamBytes, syntheticHugeSize - 8L)
	}

	private def syntheticHugeSizeChannel(ch: ReadableByteChannel): ReadableByteChannel = ch

	//// 2. RecordBatch Wire Tags & Deduplication Scope Isolation Tests ////

	test("InstallSnapshot with preceding SnapshotData[P] references round-trips losslessly across all three RecordBatch wire tags (0, 1, 2)") {
		val p1 = new String("node-alpha")
		val p2 = new String("node-beta")
		val p3 = new String("node-gamma")

		// SnapshotData references p1 and p2 BEFORE the batch is serialized
		val snapshotElectorate = SoleElectorateChange[String](term(2), "snap-ec", term(1), Set(p1), Set(p1, p2))
		val snapPayloadBytes = "snapshot-binary-state".getBytes(StandardCharsets.UTF_8)

		val batchRecords: IArray[Record] = IArray(
			CommandRecord(term(3), "cmd-after-snap-1"),
			JointElectorateChange[String](term(3), "batch-ec-1", Set(p1, p2), Set(p1, p2, p3)),
			SoleElectorateChange[String](term(3), "batch-ec-1", term(3), Set(p1, p2), Set(p1, p2, p3)),
			LeaderTransition(term(3)),
			CommandRecord(term(3), "cmd-after-snap-2")
		)

		val encodedBatch = RecordBatchCodec.encode(batchRecords, ProtocolVersion(1), AllocationLimits.Default)(using wireRecordBatchArraySerializer)

		// Placement boundary check
		val exactInline = RecordBatchCodec.place(encodedBatch, encodedBatch.byteLength.toInt)
		assert(exactInline.isInstanceOf[RecordBatch.EncodedInline])
		val oneByteOver = RecordBatchCodec.place(encodedBatch, encodedBatch.byteLength.toInt - 1)
		assert(oneByteOver.isInstanceOf[RecordBatch.EncodedOutOfLine])

		val variants: List[(String, RecordBatch)] = List(
			"Tag 0 (Records)" -> RecordBatch.Records(batchRecords),
			"Tag 1 (EncodedInline)" -> RecordBatch.EncodedInline(encodedBatch),
			"Tag 2 (EncodedOutOfLine)" -> RecordBatch.EncodedOutOfLine(encodedBatch.toByteSource)
		)

		for (label, recordBatchVariant) <- variants do {
			val msg: ClusterWireMessage = new InstallSnapshot(
				correlationId = CorrelationId(77.toShort),
				inquirerTerm = term(3),
				snapshot = SnapshotData[String](
					lastIncludedRecordIndex = 100L,
					lastIncludedRecordTerm = term(2),
					latestElectorateChange = snapshotElectorate,
					latestElectorateChangeIndex = 95L,
					stateMachineSnapshot = new BufferByteSource(ByteBuffer.wrap(snapPayloadBytes))
				),
				batch = recordBatchVariant,
				leaderCommit = 102L,
				termAtLeaderCommit = term(3)
			)

			val writer = new ChunkedHeapWriter(ProtocolVersion(1))
			clusterWireMessageSerializer.serialize(msg, writer)
			val (chunks, totalBytes) = writer.finish()
			val ch = new ChunkedBufferByteSource(chunks, totalBytes).openChannel()
			val reader = new ReadableChannelReader(ch, totalBytes, ProtocolVersion(1))
			val decodedMsg = clusterWireMessageDeserializer.deserialize(reader).asInstanceOf[InstallSnapshot]
			reader.requireFullyConsumed()
			ch.close()

			assertEquals(decodedMsg.snapshot.latestElectorateChange, snapshotElectorate, s"Snapshot electorate mismatch in $label")
			val decodedRecords: IArray[Record] = decodedMsg.batch match {
				case RecordBatch.Records(r) => r
				case RecordBatch.EncodedOutOfLine(source) =>
					try {
						RecordBatchCodec.decodeOutOfLine(source, ProtocolVersion(1), AllocationLimits.Default)(using wireRecordBatchArrayDeserializer)
					} finally {
						source.close()
					}
				case other => fail(s"Unexpected decoded RecordBatch variant in $label: $other")
			}
			assertEquals(decodedRecords.toSeq, batchRecords.toSeq, s"Decoded records mismatch in $label")
		}
	}

	//// 3. Nio2ClusterBridge End-to-End Streaming, Ordering, Fail-Fast & Teardown Tests ////

	private class BridgeHarnessNode(
		val nodeId: String,
		val schedulerDp: CooperativeThreadDrivenSchedulerDp.Impl,
		val addressMap: ConcurrentHashMap[String, SocketAddress],
		override val bridgeConfig: ClusterBridgeConfig,
		val customExecutorFactory: Option[String => ExecutorService] = None
	) extends Nio2ClusterBridgeCdm {
		override type ParticipantId = String
		override val participantIdComparator: Comparator[ParticipantId] = Ordering.String
		override type ClientId = String
		override type ClientCommand = String
		override type StateMachineResponse = String

		override val sequencer: SchedulingDoer = schedulerDp.provide(s"seq-$nodeId")

		override given participantIdSerializer: Serializer[ParticipantId] = strSerializer
		override given participantIdDeserializer: Deserializer[ParticipantId] = strDeserializer
		override given clientCommandSerializer: Serializer[ClientCommand] = strSerializer
		override given clientCommandDeserializer: Deserializer[ClientCommand] = strDeserializer
		override given stateMachineResponseSerializer: Serializer[StateMachineResponse] = strSerializer
		override given stateMachineResponseDeserializer: Deserializer[StateMachineResponse] = strDeserializer

		override def contactAddressOf(id: ParticipantId): SocketAddress =
			addressMap.getOrDefault(id, new InetSocketAddress("127.0.0.1", 0))

		override def localBindAddress: SocketAddress = new InetSocketAddress("127.0.0.1", 0)

		override protected def newBatchCodecExecutor(participantId: ParticipantId): ExecutorService =
			customExecutorFactory.map(_(participantId)).getOrElse(super.newBatchCodecExecutor(participantId))

		val bridge: Nio2ClusterBridge = new Nio2ClusterBridge(nodeId)

		val receivedAppendBatches: CopyOnWriteArrayList[(Long, Seq[Record])] = new CopyOnWriteArrayList()
		val receivedSnapshots: CopyOnWriteArrayList[(Long, Array[Byte], Seq[Record])] = new CopyOnWriteArrayList()

		object StubDelegate extends bridge.Delegate {
			override def onHowAreYou(inquirerId: String, inquirerInfo: StateInfo): sequencer.Capture[StateInfo] = {
				sequencer.checkWithin()
				sequencer.Keeper(StateInfo(term(1), ER_CANDIDATE, term(1), 0L, term(1), 0L))
			}

			override def onRequestBallot(inquirerId: String, inquirerInfo: StateInfo): sequencer.Capture[Ballot] = {
				sequencer.checkWithin()
				sequencer.Keeper(Ballot(term(1), true, ER_CANDIDATE))
			}

			override def onAppendRecords(
				inquirerId: String,
				inquirerTerm: Term,
				prevLogIndex: RecordIndex,
				prevLogTerm: Term,
				batch: IArray[Record],
				leaderCommit: RecordIndex,
				termAtLeaderCommit: Term
			): sequencer.Capture[AppendResult] = {
				sequencer.checkWithin()
				receivedAppendBatches.add((prevLogIndex, batch.toSeq))
				sequencer.Keeper(new AppendResult_Accepted(inquirerTerm, FOLLOWER))
			}

			override def onInstallSnapshot(
				inquirerId: String,
				inquirerTerm: Term,
				snapshot: SnapshotData[String],
				batch: IArray[Record],
				leaderCommit: RecordIndex,
				termAtLeaderCommit: Term
			): sequencer.Capture[AppendResult] = {
				sequencer.checkWithin()
				val ch = snapshot.stateMachineSnapshot.openChannel()
				val snapBytes = new Array[Byte](snapshot.stateMachineSnapshot.size.toInt)
				try {
					val buf = ByteBuffer.wrap(snapBytes)
					while buf.hasRemaining do ch.read(buf)
				} finally {
					ch.close()
					snapshot.stateMachineSnapshot.close()
				}
				receivedSnapshots.add((snapshot.lastIncludedRecordIndex, snapBytes, batch.toSeq))
				sequencer.Keeper(new AppendResult_Accepted(inquirerTerm, FOLLOWER))
			}

			override def onQuiescencePermitted(leaderId: String, indexOfGrantedSec: RecordIndex): Unit = {
				sequencer.checkWithin()
			}

			override def onCommandFromClient(command: String, attemptFlag: CommandAttemptFlag): sequencer.Capture[ResponseToClient] = {
				sequencer.checkWithin()
				sequencer.Keeper(Processed(1L, s"ok:$command"))
			}

			override def requestElectorateChange(requestId: ElectorateChangeRequestId, desiredParticipants: Set[String]): sequencer.Capture[ElectorateChangeResponse[String]] = {
				sequencer.checkWithin()
				sequencer.Keeper(ElectorateChangeResponse.SuccessfullyChanged)
			}
		}

		def start(): Unit = {
			bridge.setBound(StubDelegate)
			bridge.start()
			addressMap.put(nodeId, bridge.localAddress)
		}

		def stop(): Unit = {
			bridge.close()
		}
	}

	test("Per-peer FIFO ordering is preserved across interleaved StagedOnCodecExecutor, StagedOnSequencer, and WithinControlPackage batches under worker jitter") {
		val schedulerDp = new CooperativeThreadDrivenSchedulerDp.Impl(unhandledExceptionReporter = (_, e) => e.printStackTrace())
		val addressMap = new ConcurrentHashMap[String, SocketAddress]()

		// Select strategy based on batch length:
		// length == 0 -> WithinControlPackage
		// length == 1 -> WithinControlPackage
		// length == 2 -> StagedOnSequencer
		// length >= 3 -> StagedOnCodecExecutor
		val mixedConfig = ClusterBridgeConfig(
			sizeOfBuffersInRing = 256,
			maxRingLengthForTransmission = RingLength(16),
			maxRingLengthForReception = RingLength(16),
			stagedBatchInlineThreshold = RingLength(1), // 256-byte inline cap so some staged batches go inline (tag 1) and others go out-of-line (tag 2)
			batchCodecParallelism = 4,
			recordBatchEncodingSelector = batch => {
				if batch.length <= 1 then RecordBatchEncodingStrategy.WithinControlPackage
				else if batch.length == 2 then RecordBatchEncodingStrategy.StagedOnSequencer
				else RecordBatchEncodingStrategy.StagedOnCodecExecutor
			}
		)

		val jitterPool = Executors.newFixedThreadPool(4)
		val jitterFactory: String => ExecutorService = _ => new java.util.concurrent.AbstractExecutorService {
			override def execute(command: Runnable): Unit = {
				jitterPool.execute(() => {
					// Add reverse jitter so earlier submissions finish encoding after later ones
					Thread.sleep(java.util.concurrent.ThreadLocalRandom.current().nextInt(2, 20).toLong)
					command.run()
				})
			}
			override def shutdown(): Unit = ()
			override def shutdownNow(): java.util.List[Runnable] = java.util.Collections.emptyList()
			override def isShutdown: Boolean = false
			override def isTerminated: Boolean = false
			override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = true
		}

		val leader = new BridgeHarnessNode("node-1", schedulerDp, addressMap, mixedConfig, Some(jitterFactory))
		val follower = new BridgeHarnessNode("node-2", schedulerDp, addressMap, mixedConfig)

		try {
			leader.start()
			follower.start()

			val totalRequests = 24
			val allDone = new CountDownLatch(totalRequests)
			val failures = new CopyOnWriteArrayList[Throwable]()

			leader.sequencer.run {
				import leader.bridge.*
				for seqNum <- 1 to totalRequests do {
					val batch: IArray[Record] = seqNum % 4 match {
						case 0 => IArray.empty[Record]
						case 1 => IArray(CommandRecord(term(1), s"inline-cmd-$seqNum"))
						case 2 => IArray(CommandRecord(term(1), s"seq-staged-$seqNum-a"), CommandRecord(term(1), s"seq-staged-$seqNum-b-" + ("X" * 200)))
						case 3 => IArray(
							CommandRecord(term(1), s"async-staged-$seqNum-1"),
							CommandRecord(term(1), s"async-staged-$seqNum-2"),
							CommandRecord(term(1), s"async-staged-$seqNum-3-" + ("Y" * (if seqNum % 2 == 0 then 20 else 500)))
						)
					}
					"node-2".appendRecords(term(1), seqNum.toLong, term(1), batch, seqNum.toLong, term(1)).andThen(
						_ => allDone.countDown(),
						err => {
							failures.add(err)
							allDone.countDown()
						}
					)
				}
			}

			assert(allDone.await(15, TimeUnit.SECONDS), "Timed out waiting for mixed-strategy AppendRecords RPCs")
			assertEquals(failures.size(), 0, s"Unexpected RPC failures: $failures")
			assertEquals(follower.receivedAppendBatches.size(), totalRequests)

			val observedIndices = (0 until totalRequests).map(i => follower.receivedAppendBatches.get(i)._1).toList
			assertEquals(observedIndices, (1 to totalRequests).map(_.toLong).toList, "Per-peer FIFO ordering was violated")
		} finally {
			leader.stop()
			follower.stop()
			jitterPool.shutdownNow()
			schedulerDp.shutdown()
		}
	}

	test("Oversized batch exceeding control package capacity succeeds under StagedOnCodecExecutor/StagedOnSequencer and fails fast under WithinControlPackage") {
		val schedulerDp = new CooperativeThreadDrivenSchedulerDp.Impl(unhandledExceptionReporter = (_, e) => e.printStackTrace())
		val addressMap = new ConcurrentHashMap[String, SocketAddress]()

		// Control package capacity = 4 buffers * 4096 bytes = 16 KiB
		val stagedConfig = ClusterBridgeConfig(
			sizeOfBuffersInRing = 4096,
			maxRingLengthForTransmission = RingLength(4),
			maxRingLengthForReception = RingLength(4),
			fastRpcTimeout = 10000L,
			recordBatchEncodingSelector = _ => RecordBatchEncodingStrategy.StagedOnCodecExecutor
		)
		val inlineForcedConfig = stagedConfig.copy(
			recordBatchEncodingSelector = _ => RecordBatchEncodingStrategy.WithinControlPackage
		)

		val leaderStaged = new BridgeHarnessNode("leader-staged", schedulerDp, addressMap, stagedConfig)
		val leaderInline = new BridgeHarnessNode("leader-inline", schedulerDp, addressMap, inlineForcedConfig)
		val follower = new BridgeHarnessNode("follower", schedulerDp, addressMap, stagedConfig)

		try {
			leaderStaged.start()
			leaderInline.start()
			follower.start()

			// 120 KiB batch (7.5x larger than the 16 KiB control package limit)
			val bigCommand = "Z" * (40 * 1024)
			val oversizedBatch: IArray[Record] = IArray(
				CommandRecord(term(1), bigCommand),
				CommandRecord(term(1), bigCommand),
				CommandRecord(term(1), bigCommand)
			)

			// 1. StagedOnCodecExecutor automatically streams out-of-line (tag 2) and succeeds
			val stagedPromise = Promise[AppendResult]()
			leaderStaged.sequencer.run {
				import leaderStaged.bridge.*
				"follower".appendRecords(term(1), 1L, term(1), oversizedBatch, 1L, term(1)).andThen(
					res => stagedPromise.success(res),
					err => stagedPromise.failure(err)
				)
			}
			val stagedRes = Await.result(stagedPromise.future, 5.seconds)
			assert(stagedRes.isInstanceOf[AppendResult_Accepted])
			assertEquals(follower.receivedAppendBatches.size(), 1)
			assertEquals(follower.receivedAppendBatches.get(0)._2, oversizedBatch.toSeq)

			// 2. Forced WithinControlPackage fails fast (well under the 10,000ms fastRpcTimeout) via NotDelivered report
			// First establish connection with an empty batch so we measure only transmission failure time
			val warmupPromise = Promise[AppendResult]()
			leaderInline.sequencer.run {
				import leaderInline.bridge.*
				"follower".appendRecords(term(1), 2L, term(1), IArray.empty[Record], 2L, term(1)).andThen(
					res => warmupPromise.success(res),
					err => warmupPromise.failure(err)
				)
			}
			Await.result(warmupPromise.future, 5.seconds)

			val startNanos = System.nanoTime()
			val inlineFailPromise = Promise[AppendResult]()
			leaderInline.sequencer.run {
				import leaderInline.bridge.*
				"follower".appendRecords(term(1), 3L, term(1), oversizedBatch, 3L, term(1)).andThen(
					res => inlineFailPromise.success(res),
					err => inlineFailPromise.failure(err)
				)
			}
			val failErr = intercept[IOException] {
				Await.result(inlineFailPromise.future, 3.seconds)
			}
			val elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos)
			assert(elapsedMs < 2000L, s"Expected fail-fast completion (< 2000ms), but took ${elapsedMs}ms")
			assert(failErr.getCause.isInstanceOf[RingLengthExceededException], s"Expected cause RingLengthExceededException, got ${failErr.getCause}")

			// Connection remains healthy after pre-wire serialization failure: subsequent valid RPC succeeds
			val recoveryPromise = Promise[AppendResult]()
			leaderInline.sequencer.run {
				import leaderInline.bridge.*
				"follower".appendRecords(term(1), 4L, term(1), IArray(CommandRecord(term(1), "small-ok")), 4L, term(1)).andThen(
					res => recoveryPromise.success(res),
					err => recoveryPromise.failure(err)
				)
			}
			val recRes = Await.result(recoveryPromise.future, 5.seconds)
			assert(recRes.isInstanceOf[AppendResult_Accepted])
		} finally {
			leaderStaged.stop()
			leaderInline.stop()
			follower.stop()
			schedulerDp.shutdown()
		}
	}

	test("Handshake maxInboundControlPackageBytes negotiation bounds inline placement by remote receiver capacity and cleans up InstallSnapshot dual streams") {
		val schedulerDp = new CooperativeThreadDrivenSchedulerDp.Impl(unhandledExceptionReporter = (_, e) => e.printStackTrace())
		val addressMap = new ConcurrentHashMap[String, SocketAddress]()

		val allocatedStagingFiles = new CopyOnWriteArrayList[Path]()
		val baseAllocator = new ByteSink.DefaultAllocator(inMemoryThreshold = 1024)
		val trackingAllocator: ByteSink.Allocator = (expectedSize: Long) => {
			val sink = baseAllocator.allocate(expectedSize)
			sink.byteSource match {
				case fbs: FileByteSource => allocatedStagingFiles.add(fbs.path)
				case _ => ()
			}
			sink
		}

		// Leader has large 1 MiB transmitter ring and allows up to 64 inline buffers (512 KiB) locally
		val leaderConfig = ClusterBridgeConfig(
			sizeOfBuffersInRing = 8192,
			maxRingLengthForTransmission = RingLength(128),
			stagedBatchInlineThreshold = RingLength(64),
			recordBatchEncodingSelector = _ => RecordBatchEncodingStrategy.StagedOnSequencer
		)
		// Follower has tiny 16 KiB receiver control ring (2 buffers * 8192 bytes)
		val followerConfig = ClusterBridgeConfig(
			sizeOfBuffersInRing = 8192,
			maxRingLengthForReception = RingLength(2),
			inboundStagingAllocator = trackingAllocator
		)

		val leader = new BridgeHarnessNode("leader-node", schedulerDp, addressMap, leaderConfig)
		val follower = new BridgeHarnessNode("follower-node", schedulerDp, addressMap, followerConfig)

		try {
			leader.start()
			follower.start()

			// Send an InstallSnapshot with a 32 KiB stateMachineSnapshot AND a 40 KiB record batch.
			// 40 KiB is well below leader's local 512 KiB inline limit, but exceeds follower's 16 KiB receiver ring!
			// Thanks to handshake maxInboundControlPackageBytes advertisement, leader places the 40 KiB batch out-of-line (tag 2).
			val snapBytes = Array.tabulate[Byte](32 * 1024)(i => (i & 0xff).toByte)
			val batchRecords: IArray[Record] = IArray(
				CommandRecord(term(2), "A" * (20 * 1024)),
				CommandRecord(term(2), "B" * (20 * 1024))
			)
			val snapData = SnapshotData[String](
				lastIncludedRecordIndex = 50L,
				lastIncludedRecordTerm = term(2),
				latestElectorateChange = SoleElectorateChange[String](term(2), "ec-snap", term(1), Set("leader-node"), Set("leader-node", "follower-node")),
				latestElectorateChangeIndex = 45L,
				stateMachineSnapshot = new BufferByteSource(ByteBuffer.wrap(snapBytes))
			)

			val snapPromise = Promise[AppendResult]()
			leader.sequencer.run {
				import leader.bridge.*
				"follower-node".installSnapshot(term(2), snapData, batchRecords, 52L, term(2)).andThen(
					res => snapPromise.success(res),
					err => snapPromise.failure(err)
				)
			}

			val res = Await.result(snapPromise.future, 10.seconds)
			assert(res.isInstanceOf[AppendResult_Accepted])
			assertEquals(follower.receivedSnapshots.size(), 1)
			val (idx, recvSnapBytes, recvBatch) = follower.receivedSnapshots.get(0)
			assertEquals(idx, 50L)
			assertEquals(recvSnapBytes.toSeq, snapBytes.toSeq)
			assertEquals(recvBatch, batchRecords.toSeq)

			// Both the snapshot staging file and the temporary batch staging file must have been allocated (> 1 KiB) and deleted on close!
			assertEquals(allocatedStagingFiles.size(), 2, "Expected 2 file-backed staging sinks (snapshot + out-of-line batch)")
			for path <- allocatedStagingFiles.toArray(new Array[Path](0)) do {
				assert(!Files.exists(path), s"Temporary staging file $path was not deleted after close()")
			}
		} finally {
			leader.stop()
			follower.stop()
			schedulerDp.shutdown()
		}
	}

	test("Inbound streamed batch violating follower AllocationLimits closes snapshot and batch staging files and tears down connection") {
		val schedulerDp = new CooperativeThreadDrivenSchedulerDp.Impl(unhandledExceptionReporter = (_, e) => e.printStackTrace())
		val addressMap = new ConcurrentHashMap[String, SocketAddress]()

		val allocatedStagingFiles = new CopyOnWriteArrayList[Path]()
		val baseAllocator = new ByteSink.DefaultAllocator(inMemoryThreshold = 64)
		val trackingAllocator: ByteSink.Allocator = (expectedSize: Long) => {
			val sink = baseAllocator.allocate(expectedSize)
			sink.byteSource match {
				case fbs: FileByteSource => allocatedStagingFiles.add(fbs.path)
				case _ => ()
			}
			sink
		}

		// Leader allows up to 1000 collection elements and caps inline packages at 1 small buffer (256 bytes) so the 25-record batch goes out-of-line
		val leaderConfig = ClusterBridgeConfig(
			sizeOfBuffersInRing = 256,
			maxRingLengthForTransmission = RingLength(16),
			maxRingLengthForReception = RingLength(16),
			stagedBatchInlineThreshold = RingLength(1),
			fastRpcTimeout = 5000L,
			snapshotRpcTimeout = 5000L,
			allocationLimits = new AllocationLimits(maxCollectionElements = 1000)
		)
		// Follower restricts maxCollectionElements = 20 (allowing 11-byte "leader-node" strings in handshake/snapshot metadata, while rejecting a 25-record streamed batch on the codec executor)
		val followerConfig = ClusterBridgeConfig(
			sizeOfBuffersInRing = 256,
			maxRingLengthForTransmission = RingLength(16),
			maxRingLengthForReception = RingLength(16),
			stagedBatchInlineThreshold = RingLength(1),
			inboundStagingAllocator = trackingAllocator,
			allocationLimits = new AllocationLimits(maxCollectionElements = 20)
		)

		val leader = new BridgeHarnessNode("leader-node", schedulerDp, addressMap, leaderConfig)
		val follower = new BridgeHarnessNode("follower-node", schedulerDp, addressMap, followerConfig)

		try {
			leader.start()
			follower.start()

			val snapBytes = new Array[Byte](512)
			val twentyFiveRecords: IArray[Record] = IArray.tabulate(25)(i => CommandRecord(term(1), s"cmd-$i"))
			val snapData = SnapshotData[String](
				lastIncludedRecordIndex = 10L,
				lastIncludedRecordTerm = term(1),
				latestElectorateChange = SoleElectorateChange[String](term(1), "ec", term(1), Set("leader-node"), Set("leader-node")),
				latestElectorateChangeIndex = 1L,
				stateMachineSnapshot = new BufferByteSource(ByteBuffer.wrap(snapBytes))
			)

			val snapPromise = Promise[AppendResult]()
			leader.sequencer.run {
				import leader.bridge.*
				"follower-node".installSnapshot(term(1), snapData, twentyFiveRecords, 35L, term(1)).andThen(
					res => snapPromise.success(res),
					err => snapPromise.failure(err)
				)
			}

			// Because the follower tears down the socket channel upon batch decode failure, the leader's Receiver detects EOF (ChannelClosedByPeer) and fails the pending RPC fast!
			intercept[IOException] {
				Await.result(snapPromise.future, 3.seconds)
			}
			assertEquals(follower.receivedSnapshots.size(), 0, "Corrupted/over-limit batch must never reach Delegate.onInstallSnapshot")
			assertEquals(allocatedStagingFiles.size(), 2)
			for path <- allocatedStagingFiles.toArray(new Array[Path](0)) do {
				assert(!Files.exists(path), s"Temporary staging file $path leaked on decode failure")
			}
		} finally {
			leader.stop()
			follower.stop()
			schedulerDp.shutdown()
		}
	}

	test("Simultaneous bidirectional connection establishment preserves canonical PeerConnection state and completes all queued RPCs") {
		val schedulerDp = new CooperativeThreadDrivenSchedulerDp.Impl(unhandledExceptionReporter = (_, e) => e.printStackTrace())
		val addressMap = new ConcurrentHashMap[String, SocketAddress]()
		val config = ClusterBridgeConfig(
			sizeOfBuffersInRing = 128,
			maxRingLengthForTransmission = RingLength(16),
			maxRingLengthForReception = RingLength(16),
			stagedBatchInlineThreshold = RingLength(1)
		)

		val nodeA = new BridgeHarnessNode("node-A", schedulerDp, addressMap, config)
		val nodeB = new BridgeHarnessNode("node-B", schedulerDp, addressMap, config)

		try {
			nodeA.start()
			nodeB.start()

			val totalPerSide = 8
			val doneLatch = new CountDownLatch(totalPerSide * 2)
			val failures = new CopyOnWriteArrayList[Throwable]()

			// Trigger simultaneous outbound connections and queued batch encodings from both sides at the same time
			nodeA.sequencer.run {
				import nodeA.bridge.*
				for i <- 1 to totalPerSide do {
					val batch: IArray[Record] = IArray(CommandRecord(term(1), s"from-A-$i-" + ("A" * 100)))
					"node-B".appendRecords(term(1), i.toLong, term(1), batch, i.toLong, term(1)).andThen(
						_ => doneLatch.countDown(),
						err => {
							failures.add(err)
							doneLatch.countDown()
						}
					)
				}
			}
			nodeB.sequencer.run {
				import nodeB.bridge.*
				for i <- 1 to totalPerSide do {
					val batch: IArray[Record] = IArray(CommandRecord(term(1), s"from-B-$i-" + ("B" * 100)))
					"node-A".appendRecords(term(1), i.toLong, term(1), batch, i.toLong, term(1)).andThen(
						_ => doneLatch.countDown(),
						err => {
							failures.add(err)
							doneLatch.countDown()
						}
					)
				}
			}

			assert(doneLatch.await(10, TimeUnit.SECONDS), "Timed out waiting for simultaneous bidirectional RPCs")
			assertEquals(failures.size(), 0, s"Unexpected failures during simultaneous connect: $failures")
			assertEquals(nodeB.receivedAppendBatches.size(), totalPerSide)
			assertEquals(nodeA.receivedAppendBatches.size(), totalPerSide)
		} finally {
			nodeA.stop()
			nodeB.stop()
			schedulerDp.shutdown()
		}
	}

	test("Measured envelope headroom forces large-metadata InstallSnapshot batches out-of-line instead of overflowing small control rings") {
		val schedulerDp = new CooperativeThreadDrivenSchedulerDp.Impl(unhandledExceptionReporter = (_, e) => e.printStackTrace())
		val addressMap = new ConcurrentHashMap[String, SocketAddress]()

		val allocatedStagingFiles = new CopyOnWriteArrayList[Path]()
		val baseAllocator = new ByteSink.DefaultAllocator(inMemoryThreshold = 512)
		val trackingAllocator: ByteSink.Allocator = (expectedSize: Long) => {
			val sink = baseAllocator.allocate(expectedSize)
			sink.byteSource match {
				case fbs: FileByteSource => allocatedStagingFiles.add(fbs.path)
				case _ => ()
			}
			sink
		}

		// 4 buffers * 4096 bytes = 16 KiB ring capacity; allow up to 16 KiB locally so placement is governed by measured envelope bytes
		val config = ClusterBridgeConfig(
			sizeOfBuffersInRing = 4096,
			maxRingLengthForTransmission = RingLength(4),
			maxRingLengthForReception = RingLength(4),
			stagedBatchInlineThreshold = RingLength(4),
			inboundStagingAllocator = trackingAllocator,
			recordBatchEncodingSelector = _ => RecordBatchEncodingStrategy.StagedOnSequencer
		)
		assertEquals(config.maxInlineBatchBytesForPeer(config.localReceiverControlCapacity, 53), config.localReceiverControlCapacity - 53)

		val leader = new BridgeHarnessNode("leader-node", schedulerDp, addressMap, config)
		val follower = new BridgeHarnessNode("follower-node", schedulerDp, addressMap, config)

		try {
			leader.start()
			follower.start()

			// Construct SnapshotData with ~10 KiB of electorate metadata, leaving ~6 KiB in the 16 KiB ring.
			// A 7 KiB record batch would have exceeded 16 KiB total under a static capacity/2 (8 KiB) headroom heuristic,
			// but measured envelope sizing forces the batch out-of-line (tag 2) cleanly.
			val largeMembers = (1 to 100).map(i => f"peer-node-identifier-$i%03d-" + ("M" * 80)).toSet
			val snapBytes = Array.tabulate[Byte](1024)(i => (i & 0xff).toByte)
			val batchRecords: IArray[Record] = IArray(CommandRecord(term(2), "C" * (7 * 1024)))
			val snapData = SnapshotData[String](
				lastIncludedRecordIndex = 20L,
				lastIncludedRecordTerm = term(2),
				latestElectorateChange = SoleElectorateChange[String](term(2), "ec-large", term(1), largeMembers, largeMembers),
				latestElectorateChangeIndex = 15L,
				stateMachineSnapshot = new BufferByteSource(ByteBuffer.wrap(snapBytes))
			)

			val snapPromise = Promise[AppendResult]()
			leader.sequencer.run {
				import leader.bridge.*
				"follower-node".installSnapshot(term(2), snapData, batchRecords, 21L, term(2)).andThen(
					res => snapPromise.success(res),
					err => snapPromise.failure(err)
				)
			}

			val res = Await.result(snapPromise.future, 10.seconds)
			assert(res.isInstanceOf[AppendResult_Accepted])
			assertEquals(follower.receivedSnapshots.size(), 1)
			val (idx, recvSnapBytes, recvBatch) = follower.receivedSnapshots.get(0)
			assertEquals(idx, 20L)
			assertEquals(recvSnapBytes.toSeq, snapBytes.toSeq)
			assertEquals(recvBatch, batchRecords.toSeq)

			assertEquals(allocatedStagingFiles.size(), 2, "Expected both snapshot and out-of-line batch to be staged")
			for path <- allocatedStagingFiles.toArray(new Array[Path](0)) do {
				assert(!Files.exists(path), s"Temporary staging file $path leaked")
			}
		} finally {
			leader.stop()
			follower.stop()
			schedulerDp.shutdown()
		}
	}
}
