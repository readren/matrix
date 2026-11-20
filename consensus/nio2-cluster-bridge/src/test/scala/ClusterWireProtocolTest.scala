package readren.consensus
package nio2

import ClusterWireProtocolTest.*
import protocol.*
import readren.common.{BufferByteSource, Maybe}
import readren.consensus.ConsensusWireTypesCdm
import readren.nexus.cluster.channel.{PipelinedTransmitter, Receiver}
import readren.nexus.cluster.serialization.*
import readren.nexus.cluster.serialization.CommonSerializers.given
import readren.nexus.cluster.serialization.InvariantIterableFactory.given
import readren.nexus.cluster.service.{ChannelId, ChannelOrigin}
import munit.ScalaCheckSuite

import java.net.{InetSocketAddress, SocketAddress}
import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousServerSocketChannel, AsynchronousSocketChannel}
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit
import scala.collection.immutable.ListSet
import scala.concurrent.{Await, Promise}
import scala.concurrent.duration.*

object ClusterWireProtocolTest {

	object TestSdm extends ConsensusWireTypesCdm {
		override type ParticipantId = String
		override type ClientId = String
		override type ClientCommand = String
		override type StateMachineResponse = String
	}

}

class ClusterWireProtocolTest extends ScalaCheckSuite {

	private val wireProtocol = new ClusterWireProtocol(TestSdm)
	import wireProtocol.*
	import wireProtocol.given

	private inline def term(i: Int): Term = i.asInstanceOf[Term]

	private def createWriterAndReader(buffer: ByteBuffer): (Serializer.Writer, Deserializer.Reader) = {
		val writer = new Serializer.Writer {
			override val governingVersion: ProtocolVersion = ProtocolVersion(1)
			protected override def getBuffer(minimumRemaining: Int): ByteBuffer = buffer
			override def position: Int = buffer.position()
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

	private def roundTripMessage(frame: ClusterWireMessage, bufferCapacity: Int = 16384): ClusterWireMessage = {
		val buffer = ByteBuffer.allocate(bufferCapacity)
		val (writer, reader) = createWriterAndReader(buffer)
		wireProtocol.clusterWireMessageSerializer.serialize(frame, writer)
		buffer.flip()
		val decoded = wireProtocol.clusterWireMessageDeserializer.deserialize(reader)
		assertEquals(buffer.remaining(), 0, "Buffer should be fully consumed after deserialization")
		decoded
	}

	//// 1. CorrelationId Circular Arithmetic & Staleness Tests ////

	test("CorrelationId Initial value is 0") {
		assertEquals(CorrelationId.Initial.toShort, 0.toShort)
	}

	test("CorrelationId next increments and wraps around signed 16-bit boundaries") {
		assertEquals(CorrelationId(0.toShort).next.toShort, 1.toShort)
		assertEquals(CorrelationId(32767.toShort).next.toShort, -32768.toShort)
		assertEquals(CorrelationId(-32768.toShort).next.toShort, -32767.toShort)
		assertEquals(CorrelationId(-1.toShort).next.toShort, 0.toShort)
	}

	test("CorrelationId isLapsedRelativeTo correctly evaluates awaiter capacity exhaustion across wrap-around boundaries") {
		val x = CorrelationId(5.toShort)

		// 1. Within safe capacity window (<= 32,767 requests issued after x): not lapsed
		assertEquals(x.isLapsedRelativeTo(x), false)
		assertEquals(x.isLapsedRelativeTo(CorrelationId(6.toShort)), false)
		assertEquals(x.isLapsedRelativeTo(CorrelationId(10.toShort)), false)
		assertEquals(x.isLapsedRelativeTo(CorrelationId((5 + 32767).toShort)), false)

		// 2. Exceeding capacity window (> 32,767 requests issued after x): lapsed
		val lapsedHorizon = CorrelationId((5 + 32769).toShort)
		assertEquals(x.isLapsedRelativeTo(lapsedHorizon), true)

		// 3. Boundary wrap-around from Short.MaxValue (32767) into negative Short range
		val maxId = CorrelationId(Short.MaxValue)
		val justAfterMax = maxId.next // -32768 (1 request after maxId)
		assertEquals(maxId.isLapsedRelativeTo(justAfterMax), false)

		val maxPlus32767 = CorrelationId((Short.MaxValue + 32767).toShort) // -2
		assertEquals(maxId.isLapsedRelativeTo(maxPlus32767), false)

		val maxLapsedHorizon = CorrelationId(0.toShort) // 32,769 requests after Short.MaxValue
		assertEquals(maxId.isLapsedRelativeTo(maxLapsedHorizon), true)

		// Almost completed full 65,536 cycle (imminent collision from behind)
		val imminentCollision = CorrelationId((Short.MaxValue - 1).toShort) // 32766
		assertEquals(maxId.isLapsedRelativeTo(imminentCollision), true)
	}

	//// 2. Handshake Frames Round-Trip Tests ////

	test("PeerHandshake round-trip serialization") {
		val frame = PeerHandshake("node-alpha", Set(ProtocolVersion(1), ProtocolVersion(2)), Receiver.DEFAULT_MAX_SINGLE_FRAME_PACKAGE_PAYLOAD_BYTES)
		val decoded = roundTripMessage(frame)
		assertEquals(decoded, frame)
	}

	test("ClientHandshake round-trip serialization") {
		val frame = ClientHandshake(Set(ProtocolVersion(1)))
		val decoded = roundTripMessage(frame)
		assertEquals(decoded, frame)
	}

	test("HandshakeAck round-trip serialization") {
		val frame = HandshakeAck("node-beta", ProtocolVersion(1), Receiver.DEFAULT_MAX_SINGLE_FRAME_PACKAGE_PAYLOAD_BYTES)
		val decoded = roundTripMessage(frame)
		assertEquals(decoded, frame)
	}

	test("HandshakeReject round-trip serialization") {
		val frame = HandshakeReject("Incompatible protocol version")
		val decoded = roundTripMessage(frame)
		assertEquals(decoded, frame)
	}

	//// 3. Consensus RPC Request Frames Round-Trip Tests ////

	test("HowAreYou request round-trip serialization") {
		val stateInfo = StateInfo(term(3), ER_CANDIDATE, term(2), 15L, term(3), 18L)
		val frame = HowAreYou(CorrelationId(101.toShort), stateInfo)
		val decoded = roundTripMessage(frame)
		assertEquals(decoded, frame)
	}

	test("RequestBallot request round-trip serialization") {
		val stateInfo = StateInfo(term(4), ER_JOINER, term(3), 20L, term(4), 25L)
		val frame = RequestBallot(CorrelationId(102.toShort), stateInfo)
		val decoded = roundTripMessage(frame)
		assertEquals(decoded, frame)
	}

	test("AppendRecords request round-trip serialization with command records") {
		val records: IArray[Record] = IArray(
			CommandRecord(term(3), "put key1 val1"),
			LeaderTransition(term(3)),
			SoleElectorateChange(term(3), "req-ec", term(2), Set("node-alpha"), Set("node-alpha", "node-beta"))
		)
		val frame = AppendRecords(
			correlationId = CorrelationId(103.toShort),
			inquirerTerm = term(3),
			prevLogIndex = 12L,
			prevLogTerm = term(2),
			batch = records,
			leaderCommit = 10L,
			termAtLeaderCommit = term(2)
		)
		val decoded = roundTripMessage(frame)
		decoded match {
			case dec: AppendRecords =>
				assertEquals(dec.correlationId, frame.correlationId)
				assertEquals(dec.inquirerTerm, frame.inquirerTerm)
				assertEquals(dec.prevLogIndex, frame.prevLogIndex)
				assertEquals(dec.prevLogTerm, frame.prevLogTerm)
				assertEquals(dec.batch.toSeq, frame.batch.toSeq)
				assertEquals(dec.leaderCommit, frame.leaderCommit)
				assertEquals(dec.termAtLeaderCommit, frame.termAtLeaderCommit)
			case other =>
				fail(s"Expected AppendRecords but got $other")
		}
	}

	test("InstallSnapshot request round-trip serialization with streaming snapshot payload") {
		val payload = new BufferByteSource(ByteBuffer.wrap("state-machine-bytes".getBytes(StandardCharsets.UTF_8)))
		val sec = SoleElectorateChange[String](term(3), "req-snap", term(2), Set("node-1"), Set("node-1", "node-2"))
		val snapData = SnapshotData[String](
			lastIncludedRecordIndex = 50L,
			lastIncludedRecordTerm = term(3),
			latestElectorateChange = sec,
			latestElectorateChangeIndex = 48L,
			stateMachineSnapshot = payload
		)
		val frame = InstallSnapshot(
			correlationId = CorrelationId(104.toShort),
			inquirerTerm = term(3),
			snapshot = snapData,
			batch = IArray.empty[Record],
			leaderCommit = 50L,
			termAtLeaderCommit = term(3)
		)
		val decoded = roundTripMessage(frame)
		decoded match {
			case dec: InstallSnapshot =>
				assertEquals(dec.correlationId, frame.correlationId)
				assertEquals(dec.inquirerTerm, frame.inquirerTerm)
				assertEquals(dec.snapshot.lastIncludedRecordIndex, frame.snapshot.lastIncludedRecordIndex)
				assertEquals(dec.snapshot.lastIncludedRecordTerm, frame.snapshot.lastIncludedRecordTerm)
				assertEquals(dec.snapshot.latestElectorateChange, frame.snapshot.latestElectorateChange)
				assertEquals(dec.snapshot.stateMachineSnapshot.size, payload.size)
				assertEquals(dec.batch.toSeq, frame.batch.toSeq)
				assertEquals(dec.leaderCommit, frame.leaderCommit)
				assertEquals(dec.termAtLeaderCommit, frame.termAtLeaderCommit)
			case other =>
				fail(s"Expected InstallSnapshot but got $other")
		}
	}

	test("PermitQuiescence request round-trip serialization") {
		val frame = PermitQuiescence(CorrelationId(105.toShort), 75L)
		val decoded = roundTripMessage(frame)
		assertEquals(decoded, frame)
	}

	//// 4. Consensus RPC Response Frames Round-Trip Tests ////

	test("HowAreYouReply response round-trip serialization") {
		val stateInfo = StateInfo(term(5), ER_NONE, term(4), 100L, term(5), 105L)
		val frame = HowAreYouReply(CorrelationId(201.toShort), stateInfo)
		val decoded = roundTripMessage(frame)
		assertEquals(decoded, frame)
	}

	test("RequestBallotReply response round-trip serialization for Granted and Rejected") {
		val granted = RequestBallotReply(CorrelationId(202.toShort), Ballot(term(5), true, ER_LEADING))
		assertEquals(roundTripMessage(granted), granted)

		val rejected = RequestBallotReply(CorrelationId(203.toShort), Ballot(term(6), false, ER_CANDIDATE))
		assertEquals(roundTripMessage(rejected), rejected)
	}

	test("AppendRecordsReply response round-trip serialization for Accepted and Rejected") {
		val accepted = AppendRecordsReply(CorrelationId(204.toShort), new AppendResult_Accepted(term(5), FOLLOWER))
		val decodedAccepted = roundTripMessage(accepted).asInstanceOf[AppendRecordsReply]
		assertEquals(decodedAccepted.correlationId, accepted.correlationId)
		val acceptedResult = decodedAccepted.appendResult.asInstanceOf[AppendResult_Accepted]
		assertEquals(acceptedResult.term, term(5))
		assertEquals(acceptedResult.roleOrdinal, FOLLOWER)

		val rejected = AppendRecordsReply(CorrelationId(205.toShort), new AppendResult_Rejected(term(6), 25L, LEADER))
		val decodedRejected = roundTripMessage(rejected).asInstanceOf[AppendRecordsReply]
		assertEquals(decodedRejected.correlationId, rejected.correlationId)
		val rejectedResult = decodedRejected.appendResult.asInstanceOf[AppendResult_Rejected]
		assertEquals(rejectedResult.term, term(6))
		assertEquals(rejectedResult.firstEmptyRecordIndex, 25L)
		assertEquals(rejectedResult.roleOrdinal, LEADER)
	}

	test("InstallSnapshotReply response round-trip serialization") {
		val reply = InstallSnapshotReply(CorrelationId(206.toShort), new AppendResult_Accepted(term(5), FOLLOWER))
		val decoded = roundTripMessage(reply).asInstanceOf[InstallSnapshotReply]
		assertEquals(decoded.correlationId, reply.correlationId)
		val acceptedResult = decoded.appendResult.asInstanceOf[AppendResult_Accepted]
		assertEquals(acceptedResult.term, term(5))
		assertEquals(acceptedResult.roleOrdinal, FOLLOWER)
	}

	test("PermitQuiescenceReply response round-trip serialization") {
		val frame = PermitQuiescenceReply(CorrelationId(207.toShort))
		val decoded = roundTripMessage(frame)
		assertEquals(decoded, frame)
	}

	//// 5. Client & Reconfiguration RPC Frames Round-Trip Tests ////

	test("SubmitClientCommand request round-trip serialization") {
		val frame = SubmitClientCommand(CorrelationId(301.toShort), "put user:1001 alice", FIRST_ATTEMPT)
		val decoded = roundTripMessage(frame)
		assertEquals(decoded, frame)
	}

	test("SubmitClientCommandReply response round-trip for Processed, RedirectTo, and Unable") {
		val processed = SubmitClientCommandReply(CorrelationId(302.toShort), TestSdm.Processed(42L, "success-response"))
		assertEquals(roundTripMessage(processed), processed)

		val redirect = SubmitClientCommandReply(CorrelationId(303.toShort), TestSdm.RedirectTo(ListSet("leader-node", "fallback-peer")))
		assertEquals(roundTripMessage(redirect), redirect)

		val unable = SubmitClientCommandReply(CorrelationId(304.toShort), TestSdm.Unable(FALLBACK, ListSet("peer-a", "peer-b")))
		assertEquals(roundTripMessage(unable), unable)
	}

	test("SubmitElectorateChange request round-trip serialization") {
		val frame = SubmitElectorateChange(CorrelationId(305.toShort), "electorate-req-1", Set("node-1", "node-2", "node-3"))
		val decoded = roundTripMessage(frame)
		assertEquals(decoded, frame)
	}

	test("SubmitElectorateChangeReply response round-trip serialization") {
		val applied = SubmitElectorateChangeReply(CorrelationId(306.toShort), ElectorateChangeResponse.SuccessfullyChanged)
		assertEquals(roundTripMessage(applied), applied)

		val excluded = SubmitElectorateChangeReply(CorrelationId(307.toShort), ElectorateChangeResponse.Excluded(ListSet("node-4")))
		assertEquals(roundTripMessage(excluded), excluded)

		val conflict = SubmitElectorateChangeReply(CorrelationId(308.toShort), ElectorateChangeResponse.AnotherChangeInProgress("req-in-prog", Set("node-2", "node-3"), ListSet("node-1")))
		assertEquals(roundTripMessage(conflict), conflict)
	}

	//// 6. Discrimination Header Encoding & Header Validation Tests ////

	test("DiscriminationCriteria produces distinct expected discriminator bytes for all 18 variants") {
		val cases = DiscriminationCriteria.casesOf[ClusterWireMessage](NestedSumMatchMode.FLAT)
		assertEquals(cases.size, 18)

		val expectedMapping = Map(
			"PeerHandshake" -> 1,
			"ClientHandshake" -> 2,
			"HandshakeAck" -> 3,
			"HandshakeReject" -> 4,
			"HowAreYou" -> 10,
			"RequestBallot" -> 11,
			"AppendRecords" -> 12,
			"InstallSnapshot" -> 13,
			"PermitQuiescence" -> 14,
			"HowAreYouReply" -> 30,
			"RequestBallotReply" -> 31,
			"AppendRecordsReply" -> 32,
			"InstallSnapshotReply" -> 33,
			"PermitQuiescenceReply" -> 34,
			"SubmitClientCommand" -> 50,
			"SubmitElectorateChange" -> 51,
			"SubmitClientCommandReply" -> 70,
			"SubmitElectorateChangeReply" -> 71
		)

		for entry <- cases do {
			entry match {
				case DiscriminationCriteria.FlatEntry(disc, name) =>
					assertEquals(disc, expectedMapping(name), s"Discriminator mismatch for $name")
				case other =>
					fail(s"Unexpected tree entry in FLAT mode: $other")
			}
		}
	}

	test("Invalid discriminator throws Deserializer.InvalidDiscriminatorException") {
		val buffer = ByteBuffer.allocate(64)
		val (writer, reader) = createWriterAndReader(buffer)
		writer.putUnsignedIntVlq(99) // Invalid discriminator tag
		writer.putShort(0.toShort)
		buffer.flip()

		intercept[Deserializer.InvalidDiscriminatorException] {
			wireProtocol.clusterWireMessageDeserializer.deserialize(reader)
		}
	}

	//// 7. Physical Network Pipeline Tests via PipelinedTransmitter & Receiver ////

	test("Round-trip framing through PipelinedTransmitter and Receiver over NIO2 TCP socket loopback") {
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

		val transmitter = new PipelinedTransmitter[ClusterWireMessage](clientChannel, transmitterContext, capacityOfBufferInRing = 8192)
		val receiver = new Receiver(serverConnection, receiverContext, capacityOfBufferInRing = 8192)

		def transmitAndReceive(frame: ClusterWireMessage): ClusterWireMessage = {
			val promise = Promise[ClusterWireMessage]()
			receiver.receive[ClusterWireMessage](ProtocolVersion(1), 5, TimeUnit.SECONDS) {
				case received: ClusterWireMessage => promise.success(received)
				case fault: Receiver.Fault => promise.failure(new RuntimeException(s"Receiver fault: $fault"))
			}
			transmitter.transmit(frame, ProtocolVersion(1), timeout = 5, timeUnit = TimeUnit.SECONDS) {
				case PipelinedTransmitter.Delivered => ()
				case notDelivered: PipelinedTransmitter.NotDelivered => promise.failure(new RuntimeException(s"Transmission failure: $notDelivered"))
			}
			Await.result(promise.future, 5.seconds)
		}

		try {
			// 1. Handshake
			val handshake = PeerHandshake("node-source", Set(ProtocolVersion(1)), Receiver.DEFAULT_MAX_SINGLE_FRAME_PACKAGE_PAYLOAD_BYTES)
			val receivedHandshake = transmitAndReceive(handshake)
			assertEquals(receivedHandshake, handshake)

			// 2. Consensus Request
			val appendReq = AppendRecords(
				correlationId = CorrelationId(1.toShort),
				inquirerTerm = term(2),
				prevLogIndex = 5L,
				prevLogTerm = term(2),
				batch = IArray(CommandRecord(term(2), "cmd-test")),
				leaderCommit = 4L,
				termAtLeaderCommit = term(2)
			)
			val receivedAppend = transmitAndReceive(appendReq)
			receivedAppend match {
				case dec: AppendRecords =>
					assertEquals(dec.correlationId, appendReq.correlationId)
					assertEquals(dec.inquirerTerm, appendReq.inquirerTerm)
					assertEquals(dec.batch.toSeq, appendReq.batch.toSeq)
				case other => fail(s"Expected AppendRecords but got $other")
			}

			// 3. Consensus Response
			val appendReply = AppendRecordsReply(CorrelationId(1.toShort), new AppendResult_Accepted(term(2), FOLLOWER))
			val receivedReply = transmitAndReceive(appendReply)
			assertEquals(receivedReply.asInstanceOf[AppendRecordsReply].correlationId, appendReply.correlationId)
			assertEquals(receivedReply.asInstanceOf[AppendRecordsReply].appendResult.asInstanceOf[AppendResult_Accepted].term, term(2))

			// 4. Client Request & Response
			val clientCmd = SubmitClientCommand(CorrelationId(2.toShort), "key=val", FIRST_ATTEMPT)
			val receivedClientCmd = transmitAndReceive(clientCmd)
			assertEquals(receivedClientCmd, clientCmd)

			val clientReply = SubmitClientCommandReply(CorrelationId(2.toShort), TestSdm.Processed(6L, "val"))
			val receivedClientReply = transmitAndReceive(clientReply)
			assertEquals(receivedClientReply, clientReply)

			// 5. Streaming InstallSnapshot with FilePayload
			val tempSnapFile = java.nio.file.Files.createTempFile("test-stream-snap-", ".bin")
			tempSnapFile.toFile.deleteOnExit()
			val snapBytes = "cluster-wire-protocol-streaming-snapshot-content-123456789".getBytes(StandardCharsets.UTF_8)
			java.nio.file.Files.write(tempSnapFile, snapBytes)
			val snapPayload = new readren.common.FileByteSource(tempSnapFile, deleteOnClose = false)
			val installSnap = InstallSnapshot(
				correlationId = CorrelationId(3.toShort),
				inquirerTerm = term(4),
				snapshot = SnapshotData[String](
					lastIncludedRecordIndex = 25L,
					lastIncludedRecordTerm = term(4),
					latestElectorateChange = SoleElectorateChange[String](term(4), "req-3", term(3), Set("node-source"), Set("node-source", "node-dest")),
					latestElectorateChangeIndex = 24L,
					stateMachineSnapshot = snapPayload
				),
				batch = IArray.empty[Record],
				leaderCommit = 25L,
				termAtLeaderCommit = term(4)
			)
			val receivedInstallSnap = transmitAndReceive(installSnap)
			receivedInstallSnap match {
				case dec: InstallSnapshot =>
					assertEquals(dec.correlationId, installSnap.correlationId)
					assertEquals(dec.inquirerTerm, installSnap.inquirerTerm)
					assertEquals(dec.snapshot.lastIncludedRecordIndex, 25L)
					assertEquals(dec.snapshot.stateMachineSnapshot.size, snapBytes.length.toLong)
					val readCh = dec.snapshot.stateMachineSnapshot.openChannel()
					try {
						val dst = ByteBuffer.allocate(snapBytes.length)
						readCh.read(dst)
						dst.flip()
						val readArr = new Array[Byte](dst.remaining())
						dst.get(readArr)
						assertEquals(new String(readArr, StandardCharsets.UTF_8), new String(snapBytes, StandardCharsets.UTF_8))
					} finally {
						readCh.close()
						dec.snapshot.stateMachineSnapshot.close()
					}
				case other => fail(s"Expected InstallSnapshot but got $other")
			}
		} finally {
			clientChannel.close()
			serverConnection.close()
			serverChannel.close()
		}
	}

}
