package readren.consensus
package nio2

import protocol.*
import readren.nexus.cluster.serialization.*
import readren.nexus.cluster.serialization.CommonSerializers.given
import readren.nexus.cluster.serialization.InvariantIterableFactory.given
import readren.nexus.cluster.serialization.NestedSumMatchMode.FLAT
import readren.nexus.cluster.serialization.ValueOrReferenceTest.given

opaque type CorrelationId = Short

object CorrelationId {
	val Initial: CorrelationId = 0.toShort

	inline def apply(value: Short): CorrelationId = value

	extension (self: CorrelationId) {
		inline def next: CorrelationId = (self + 1).toShort
		inline def toShort: Short = self

		/** Evaluates whether this correlation identifier has lapsed relative to the latest issued identifier, requiring its awaiter to be forcibly failed.
		 *
		 * In the multiplexed RPC transport, each in-flight request registers an awaiter tagged with this [[CorrelationId]] to match asynchronous replies over a shared channel.
		 * Because correlation identifiers are allocated from a finite 16-bit circular sequence space, at most 32,767 concurrent in-flight awaiters can be disambiguated without collision.
		 * When `x.isLapsedRelativeTo(lastIssued)` evaluates to `true`, more than 32,767 subsequent requests have been issued since `x`, exhausting the safe capacity window.
		 * The awaiter associated with `x` must be evicted and forced to fail immediately to prevent ambiguous response pairing when `lastIssued` wraps around to `x`.
		 *
		 * Implementation detail note: Evaluated via signed 16-bit modular difference `(self - lastIssued).toShort > 0`. When `lastIssued` is within 1 to 32,767 steps ahead of `self`,
		 * `(self - lastIssued).toShort` is negative. Once `lastIssued` advances beyond 32,767 steps, two's complement wrap-around causes the difference to become positive, detecting
		 * capacity overflow across the [[Short.MaxValue]] to [[Short.MinValue]] boundary without JVM 32-bit integer widening hazards.
		 *
		 * @param lastIssued the most recently issued [[CorrelationId]] allocated by the local transmitter.
		 * @return `true` if more than 32,767 identifiers have been issued since this identifier, requiring its awaiter to be evicted and failed; `false` otherwise.*/
		inline def isLapsedRelativeTo(lastIssued: CorrelationId): Boolean = (self - lastIssued).toShort > 0

		/** @see [[isLapsedRelativeTo]] */
		inline def isStaleRelativeTo(lastIssued: CorrelationId): Boolean = isLapsedRelativeTo(lastIssued)
	}

	given correlationIdSerializer: Serializer[CorrelationId] =
		(id: CorrelationId, writer: Serializer.Writer) => writer.putShort(id)

	given correlationIdDeserializer: Deserializer[CorrelationId] =
		(reader: Deserializer.Reader) => reader.readShort().asInstanceOf[CorrelationId]
}

class ClusterWireProtocol[S <: ConsensusWireTypesCdm](val sdm: S)(using
	val participantIdSerializer: Serializer[sdm.ParticipantId],
	val participantIdDeserializer: Deserializer[sdm.ParticipantId],
	val clientCommandSerializer: Serializer[sdm.ClientCommand],
	val clientCommandDeserializer: Deserializer[sdm.ClientCommand],
	val stateMachineResponseSerializer: Serializer[sdm.StateMachineResponse],
	val stateMachineResponseDeserializer: Deserializer[sdm.StateMachineResponse]
) extends ConsensusProtocolSerializers {

	type P = sdm.ParticipantId
	type C = sdm.ClientCommand

	given sdm.type = sdm

	given wireRecordSerializer: Serializer[Record] =
		ConsensusProtocolSerializers.recordSerializer[C, P](using clientCommandSerializer, participantIdSerializer, participantIdDeserializer)

	given wireRecordDeserializer: Deserializer[Record] =
		ConsensusProtocolSerializers.recordDeserializer[C, P](using clientCommandDeserializer, participantIdDeserializer, ValueOrReferenceTest.whenReference[P])

	//// Message Hierarchy ////

	sealed trait ClusterWireMessage

	sealed trait HandshakeMessage extends ClusterWireMessage
	final case class PeerHandshake(senderId: P, supportedVersions: Set[ProtocolVersion]) extends HandshakeMessage
	final case class ClientHandshake(supportedVersions: Set[ProtocolVersion]) extends HandshakeMessage
	final case class HandshakeAck(responderId: P, agreedVersion: ProtocolVersion) extends HandshakeMessage
	final case class HandshakeReject(reason: String) extends HandshakeMessage

	sealed trait ConsensusRpcRequest extends ClusterWireMessage {
		val correlationId: CorrelationId
	}
	final case class HowAreYou(correlationId: CorrelationId, inquirerInfo: StateInfo) extends ConsensusRpcRequest
	final case class RequestBallot(correlationId: CorrelationId, inquirerInfo: StateInfo) extends ConsensusRpcRequest

	// TODO: Future optimization: Implement Anchor + Relative VLQ Delta wire format for AppendRecords.
	// In that optimized format:
	// - inquirerTerm is serialized once using writer.putInt and prevLogIndex once using writer.putLong as anchors.
	// - The remaining terms (prevLogTerm, termAtLeaderCommit, and embedded record terms) are encoded as relative VLQ deltas (writer.putIntVlq) from inquirerTerm.
	// - The remaining record indices (leaderCommit and embedded record indices) are encoded as relative VLQ deltas (writer.putLongVlq) from prevLogIndex.
	final case class AppendRecords(correlationId: CorrelationId, inquirerTerm: Term, prevLogIndex: RecordIndex, prevLogTerm: Term, batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term) extends ConsensusRpcRequest
	final case class InstallSnapshot(correlationId: CorrelationId, inquirerTerm: Term, snapshot: SnapshotData[P], batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term) extends ConsensusRpcRequest
	final case class PermitQuiescence(correlationId: CorrelationId, indexOfGrantedSec: RecordIndex) extends ConsensusRpcRequest

	sealed trait ConsensusRpcResponse extends ClusterWireMessage {
		val correlationId: CorrelationId
	}
	final case class HowAreYouReply(correlationId: CorrelationId, stateInfo: StateInfo) extends ConsensusRpcResponse
	final case class RequestBallotReply(correlationId: CorrelationId, ballot: Ballot) extends ConsensusRpcResponse
	final case class AppendRecordsReply(correlationId: CorrelationId, appendResult: AppendResult) extends ConsensusRpcResponse
	final case class InstallSnapshotReply(correlationId: CorrelationId, appendResult: AppendResult) extends ConsensusRpcResponse
	final case class PermitQuiescenceReply(correlationId: CorrelationId) extends ConsensusRpcResponse

	sealed trait ClientRpcRequest extends ClusterWireMessage {
		val correlationId: CorrelationId
	}
	final case class SubmitClientCommand(correlationId: CorrelationId, command: C, attemptFlag: CommandAttemptFlag) extends ClientRpcRequest
	final case class SubmitElectorateChange(correlationId: CorrelationId, requestId: ElectorateChangeRequestId, desiredParticipantsSet: Set[P]) extends ClientRpcRequest

	sealed trait ClientRpcResponse extends ClusterWireMessage {
		val correlationId: CorrelationId
	}
	final case class SubmitClientCommandReply(correlationId: CorrelationId, response: sdm.ResponseToClient) extends ClientRpcResponse
	final case class SubmitElectorateChangeReply(correlationId: CorrelationId, response: ElectorateChangeResponse[P]) extends ClientRpcResponse

	//// Discrimination Criteria & Codecs ////

	object wireMessageDc extends DiscriminationCriteria[ClusterWireMessage] {
		override type Discriminator[F <: ClusterWireMessage] = F match {
			case PeerHandshake => 1
			case ClientHandshake => 2
			case HandshakeAck => 3
			case HandshakeReject => 4
			case HowAreYou => 10
			case RequestBallot => 11
			case AppendRecords => 12
			case InstallSnapshot => 13
			case PermitQuiescence => 14
			case HowAreYouReply => 30
			case RequestBallotReply => 31
			case AppendRecordsReply => 32
			case InstallSnapshotReply => 33
			case PermitQuiescenceReply => 34
			case SubmitClientCommand => 50
			case SubmitElectorateChange => 51
			case SubmitClientCommandReply => 70
			case SubmitElectorateChangeReply => 71
		}
	}

	transparent inline given wireMessageDiscriminationCriteria: DiscriminationCriteria[ClusterWireMessage] = wireMessageDc

	given clusterWireMessageSerializer: Serializer[ClusterWireMessage] = Serializer.derive[ClusterWireMessage](FLAT)

	given clusterWireMessageDeserializer: Deserializer[ClusterWireMessage] = Deserializer.derive[ClusterWireMessage](FLAT)
}
