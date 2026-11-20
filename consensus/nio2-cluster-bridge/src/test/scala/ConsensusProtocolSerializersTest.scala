package readren.consensus
package nio2

import ConsensusProtocolSerializers.given
import protocol.*
import readren.common.{BufferByteSource, FileByteSource}
import readren.consensus.ConsensusWireTypesCdm
import readren.nexus.cluster.serialization.*
import readren.nexus.cluster.serialization.CommonSerializers.given
import readren.nexus.cluster.serialization.ValueOrReferenceTest.given

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import munit.ScalaCheckSuite
import org.scalacheck.Prop.*
import scala.collection.immutable.ListSet

object ConsensusProtocolSerializersTest {

	object TestSdm extends ConsensusWireTypesCdm {
		override type ParticipantId = String
		override type ClientId = String
		override type ClientCommand = String
		override type StateMachineResponse = String
	}

}

class ConsensusProtocolSerializersTest extends ScalaCheckSuite {
	import ConsensusProtocolSerializersTest.*

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

	private def roundTrip[T](value: T, bufferCapacity: Int = 16384)(using serializer: Serializer[T], deserializer: Deserializer[T]): T = {
		val buffer = ByteBuffer.allocate(bufferCapacity)
		val (writer, reader) = createWriterAndReader(buffer)
		serializer.serialize(value, writer)
		buffer.flip()
		val decoded = deserializer.deserialize(reader)
		assertEquals(buffer.remaining(), 0, "Buffer should be fully consumed after deserialization")
		decoded
	}

	private def roundTripDedup[T <: AnyRef](first: T, second: T, bufferCapacity: Int = 16384)(using serializer: Serializer[T], deserializer: Deserializer[T], vor: ValueOrReferenceTest[T]): (T, T) = {
		val buffer = ByteBuffer.allocate(bufferCapacity)
		val (writer, reader) = createWriterAndReader(buffer)
		writer.write(first)
		writer.write(second)
		buffer.flip()
		val dec1 = reader.read[T]
		val dec2 = reader.read[T]
		assertEquals(buffer.remaining(), 0)
		(dec1, dec2)
	}

	//// 1. Opaque Types: Term, RoleOrdinal, ElectionRank, CommandAttemptFlag ////

	test("Term round-trip serialization across boundary values") {
		val sampleTerms: List[Term] = List(PRE_INIT, term(1), term(2), term(42), term(1000), term(Int.MaxValue))
		for t <- sampleTerms do {
			val decoded = roundTrip[Term](t)
			assertEquals(decoded, t)
		}
	}

	test("RoleOrdinal round-trip serialization for all defined roles") {
		val roles: List[RoleOrdinal] = List(
			QUIESCED, STARTING, RETIRING, JOINING, ISOLATED, ABDICATING, FOLLOWER, LEADER
		)
		for role <- roles do {
			val decoded = roundTrip[RoleOrdinal](role)
			assertEquals(decoded, role)
			assertEquals(RoleOrdinal_nameOf(decoded), RoleOrdinal_nameOf(role))
		}
	}

	test("ElectionRank round-trip serialization for all ranks") {
		val ranks: List[ElectionRank] = List(
			ER_NONE, ER_RETIREE, ER_JOINER, ER_CANDIDATE, ER_LEADING
		)
		for rank <- ranks do {
			val decoded = roundTrip[ElectionRank](rank)
			assertEquals(decoded, rank)
			assertEquals(ElectionRank_nameOf(decoded), ElectionRank_nameOf(rank))
		}
	}

	test("CommandAttemptFlag round-trip serialization for flags and combinations") {
		val flags: List[CommandAttemptFlag] = List(
			FIRST_ATTEMPT,
			REDIRECTED,
			FALLBACK,
			LEADERSHIP_VACATED,
			REDIRECTED | FALLBACK
		)
		for flag <- flags do {
			val decoded = roundTrip[CommandAttemptFlag](flag)
			assertEquals(decoded, flag)
			assertEquals(decoded.isFallback, flag.isFallback)
			assertEquals(decoded.isLeaderVacated, flag.isLeaderVacated)
		}
	}

	//// 2. StateInfo & Ballot ////

	test("StateInfo round-trip serialization") {
		val samples = List(
			StateInfo(currentTerm = term(1), rank = ER_LEADING, termAtCommitIndex = term(1), commitIndex = 10L, lastRecordTerm = term(1), lastRecordIndex = 15L),
			StateInfo(currentTerm = term(5), rank = ER_CANDIDATE, termAtCommitIndex = term(3), commitIndex = 100L, lastRecordTerm = term(4), lastRecordIndex = 120L),
			StateInfo(currentTerm = term(0), rank = ER_NONE, termAtCommitIndex = term(0), commitIndex = 0L, lastRecordTerm = term(0), lastRecordIndex = 0L)
		)
		for stateInfo <- samples do {
			val decoded = roundTrip[StateInfo](stateInfo)
			assertEquals(decoded, stateInfo)
		}
	}

	property("Ballot round-trip serialization for arbitrary terms and flags") {
		forAll { (termInt: Int, granted: Boolean, rankByte: Byte) =>
			val rank = rankByte.asInstanceOf[ElectionRank]
			val ballot = Ballot(term(termInt), granted, rank)
			val decoded = roundTrip[Ballot](ballot)
			assertEquals(decoded.term, ballot.term)
			assertEquals(decoded.granted, ballot.granted)
			assertEquals(decoded.voterRank, ballot.voterRank)
		}
	}

	//// 3. AppendResult Hierarchy and Throwable Codec ////

	test("AppendResult_Accepted round-trip serialization") {
		val accepted = new AppendResult_Accepted(term = term(5), roleOrdinal = LEADER)
		val decoded = roundTrip[AppendResult](accepted).asInstanceOf[AppendResult_Accepted]
		assertEquals(decoded.term, accepted.term)
		assertEquals(decoded.roleOrdinal, accepted.roleOrdinal)
	}

	test("AppendResult_Rejected round-trip serialization") {
		val rejected = new AppendResult_Rejected(term = term(4), firstEmptyRecordIndex = 99L, roleOrdinal = FOLLOWER)
		val decoded = roundTrip[AppendResult](rejected).asInstanceOf[AppendResult_Rejected]
		assertEquals(decoded.term, rejected.term)
		assertEquals(decoded.firstEmptyRecordIndex, rejected.firstEmptyRecordIndex)
		assertEquals(decoded.roleOrdinal, rejected.roleOrdinal)
	}


	//// 4. ElectorateChange Hierarchy ////

	test("JointElectorateChange round-trip serialization") {
		val jec = JointElectorateChange[String](
			term = term(3),
			requestId = "change-req-1",
			oldParticipants = Set("node-1", "node-2"),
			newParticipants = Set("node-2", "node-3")
		)
		val decoded = roundTrip[ElectorateChange[String]](jec).asInstanceOf[JointElectorateChange[String]]
		assertEquals(decoded, jec)
	}

	test("SoleElectorateChange round-trip serialization") {
		val sec = SoleElectorateChange[String](
			term = term(4),
			requestId = "change-req-1",
			coupleTerm = term(3),
			oldParticipants = Set("node-1", "node-2"),
			newParticipants = Set("node-2", "node-3")
		)
		val decoded = roundTrip[ElectorateChange[String]](sec).asInstanceOf[SoleElectorateChange[String]]
		assertEquals(decoded, sec)
	}

	//// 5. Record Dispatch ////

	test("Record round-trip across all record variants") {
		given Serializer[Record] = recordSerializer[String, String]
		given Deserializer[Record] = recordDeserializer[String, String]

		val records: List[Record] = List(
			CommandRecord[String](term = term(2), command = "PUT key=val"),
			LeaderTransition(term = term(3)),
			JointElectorateChange[String](term = term(4), requestId = "req-1", oldParticipants = Set("n1"), newParticipants = Set("n1", "n2")),
			SoleElectorateChange[String](term = term(5), requestId = "req-1", coupleTerm = term(4), oldParticipants = Set("n1"), newParticipants = Set("n1", "n2"))
		)

		for record <- records do {
			val decoded = roundTrip[Record](record)
			assertEquals(decoded, record)
		}
	}

	//// 6. SnapshotByteSource & SnapshotData ////

	test("SnapshotByteSource round-trip preserves bytes and size") {
		val rawBytes = "Snapshot serialized state machine content bytes 123456789".getBytes(StandardCharsets.UTF_8)
		val payload = new BufferByteSource(ByteBuffer.wrap(rawBytes))
		val decoded = roundTrip[SnapshotByteSource](payload)
		assertEquals(decoded.size, payload.size)

		val ch = decoded.openChannel()
		try {
			val dest = ByteBuffer.allocate(rawBytes.length)
			ch.read(dest)
			dest.flip()
			val readArray = new Array[Byte](dest.remaining())
			dest.get(readArray)
			assertEquals(new String(readArray, StandardCharsets.UTF_8), new String(rawBytes, StandardCharsets.UTF_8))
		} finally {
			ch.close()
			decoded.close()
		}
	}

	test("FileByteSource round-trip preserves file content and zero-copy transfer") {
		val tempFile = java.nio.file.Files.createTempFile("test-snap-", ".bin")
		tempFile.toFile.deleteOnExit()
		val content = "FileByteSource on-disk streaming verification".getBytes(StandardCharsets.UTF_8)
		java.nio.file.Files.write(tempFile, content)

		val payload = new FileByteSource(tempFile, deleteOnClose = false)
		val decoded = roundTrip[SnapshotByteSource](payload)
		assertEquals(decoded.size, content.length.toLong)

		val ch = decoded.openChannel()
		try {
			val dest = ByteBuffer.allocate(content.length)
			ch.read(dest)
			dest.flip()
			val readArray = new Array[Byte](dest.remaining())
			dest.get(readArray)
			assertEquals(new String(readArray, StandardCharsets.UTF_8), new String(content, StandardCharsets.UTF_8))
		} finally {
			ch.close()
			decoded.close()
			payload.close()
			java.nio.file.Files.deleteIfExists(tempFile)
		}
	}

	test("SnapshotData[String] round-trip serialization") {
		val sec = SoleElectorateChange[String](
			term = term(5),
			requestId = "snap-req",
			coupleTerm = term(4),
			oldParticipants = Set("n1"),
			newParticipants = Set("n1", "n2")
		)
		val snapPayload = new BufferByteSource(ByteBuffer.wrap("snap-content".getBytes(StandardCharsets.UTF_8)))
		val snapshotData = SnapshotData[String](
			lastIncludedRecordIndex = 200L,
			lastIncludedRecordTerm = term(5),
			latestElectorateChange = sec,
			latestElectorateChangeIndex = 195L,
			stateMachineSnapshot = snapPayload
		)

		val decoded = roundTrip[SnapshotData[String]](snapshotData)
		assertEquals(decoded.lastIncludedRecordIndex, snapshotData.lastIncludedRecordIndex)
		assertEquals(decoded.lastIncludedRecordTerm, snapshotData.lastIncludedRecordTerm)
		assertEquals(decoded.latestElectorateChange, snapshotData.latestElectorateChange)
		assertEquals(decoded.latestElectorateChangeIndex, snapshotData.latestElectorateChangeIndex)
		assertEquals(decoded.stateMachineSnapshot.size, snapPayload.size)
	}

	//// 7. ElectorateChangeResponse Variants ////

	test("ElectorateChangeResponse round-trip serialization for all enum cases") {
		val cases: List[ElectorateChangeResponse[String]] = List(
			ElectorateChangeResponse.SuccessfullyChanged,
			ElectorateChangeResponse.AlreadyChanged,
			ElectorateChangeResponse.WaitGhostLeaderIsDemoted(ListSet("n1", "n2")),
			ElectorateChangeResponse.AnotherChangeInProgress("in-prog-1", Set("n2", "n3"), ListSet("n1")),
			ElectorateChangeResponse.AskTheLeader(ListSet("leader1", "peer2")),
			ElectorateChangeResponse.CatchingUp(ListSet("n1")),
			ElectorateChangeResponse.RequestTrackingLostAfterFirstPhaseStarted(ListSet("n1")),
			ElectorateChangeResponse.RequestTrackingLostAfterFirstPhaseCommitted(ListSet("n1")),
			ElectorateChangeResponse.RequestTrackingLostAfterSecondPhaseStarted(ListSet("n1")),
			ElectorateChangeResponse.Excluded(ListSet("n1")),
			ElectorateChangeResponse.Secluded(ListSet("n1")),
			ElectorateChangeResponse.Stopped(ListSet.empty)
		)

		for response <- cases do {
			val decoded = roundTrip[ElectorateChangeResponse[String]](response)
			assertEquals(decoded, response)
			assertEquals(decoded.isTerminal, response.isTerminal)
			assertEquals(decoded.suggestedParticipants.toList, response.suggestedParticipants.toList)
		}
	}

	//// 8. ListSet Insertion Order Preservation ////

	test("ListSet preserves strict insertion order through serialization and deserialization") {
		val original = ListSet("delta", "alpha", "charlie", "bravo")
		val decoded = roundTrip[ListSet[String]](original)
		assertEquals(decoded.toList, original.toList)
	}

	//// 9. Reference Deduplication ////

	test("Reference deduplication preserves referential identity on duplicate writes") {
		val sharedCmd = CommandRecord[String](term = term(10), command = "dedup-payload")
		val (dec1, dec2) = roundTripDedup[CommandRecord[String]](sharedCmd, sharedCmd)
		assertEquals(dec1, sharedCmd)
		assertEquals(dec2, sharedCmd)
		assert(dec1 eq dec2, "Decoded instances must share referential identity through deduplication")
	}

	//// 10. sdm.ResponseToClient Codecs ////

	test("sdm.ResponseToClient round-trip across Processed, RedirectTo, and Unable") {
		given sdm: TestSdm.type = TestSdm

		val responses: List[TestSdm.ResponseToClient] = List(
			TestSdm.Processed(recordIndex = 50L, content = "CommandAppliedSuccessfully"),
			TestSdm.RedirectTo(otherParticipants = ListSet("leader-node", "fallback-node")),
			TestSdm.Unable(nextAttemptFlag = FALLBACK, otherParticipants = ListSet("alternate-1", "alternate-2"))
		)

		for resp <- responses do {
			val decoded = roundTrip[TestSdm.ResponseToClient](resp)
			assertEquals(decoded, resp)
		}
	}

}
