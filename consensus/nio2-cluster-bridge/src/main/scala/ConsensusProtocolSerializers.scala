package readren.consensus
package nio2

import readren.consensus.protocol.*
import readren.nexus.cluster.serialization.*
import readren.nexus.cluster.serialization.CommonSerializers.given
import readren.nexus.cluster.serialization.InvariantIterableFactory.given
import readren.nexus.cluster.serialization.ValueOrReferenceTest.given

import scala.collection.immutable.ListSet

trait ConsensusProtocolSerializers {

	/** Insertion-order-preserving collection factory for [[ListSet]], enabling [[CommonSerializers.iterableSerializer]] and [[CommonSerializers.iterableDeserializer]]. */
	given listSetFactory: InvariantIterableFactory[ListSet] = new InvariantIterableFactory(ListSet)

	//// Opaque Type Codecs ////

	given termSerializer: Serializer[Term] = (term: Term, writer: Serializer.Writer) => writer.putInt(term)
	given termDeserializer: Deserializer[Term] = (reader: Deserializer.Reader) => reader.readInt().asInstanceOf[Term]

	given roleOrdinalSerializer: Serializer[RoleOrdinal] = (roleOrdinal: RoleOrdinal, writer: Serializer.Writer) => writer.putByte(roleOrdinal.asInstanceOf[Byte])
	given roleOrdinalDeserializer: Deserializer[RoleOrdinal] = (reader: Deserializer.Reader) => reader.readByte().asInstanceOf[RoleOrdinal]

	given electionRankSerializer: Serializer[ElectionRank] = (electionRank: ElectionRank, writer: Serializer.Writer) => writer.putByte(electionRank.asInstanceOf[Byte])
	given electionRankDeserializer: Deserializer[ElectionRank] = (reader: Deserializer.Reader) => reader.readByte().asInstanceOf[ElectionRank]

	given commandAttemptFlagSerializer: Serializer[CommandAttemptFlag] = (flag: CommandAttemptFlag, writer: Serializer.Writer) => writer.putByte(flag.asInstanceOf[Byte])
	given commandAttemptFlagDeserializer: Deserializer[CommandAttemptFlag] = (reader: Deserializer.Reader) => reader.readByte().asInstanceOf[CommandAttemptFlag]

	//// Protocol Codecs Derived via Tasty Macro Derivation ////

	// TODO: Future optimization (Iteration 3): Implement Anchor + Relative VLQ Delta wire format for StateInfo (and note it for AppendRecords).
	// In that optimized format:
	// - currentTerm is serialized once using writer.putInt and lastRecordIndex once using writer.putLong as anchors.
	// - The remaining terms (lastRecordTerm, termAtCommitIndex, and future lastAppliedRecordTerm, lastConfigTerm, snapshotTerm) are encoded as relative VLQ deltas (writer.putIntVlq) from currentTerm.
	// - The remaining record indices (commitIndex, and future lastApplied, lastConfigIndex, snapshotIndex) are encoded as relative VLQ deltas (writer.putLongVlq) from lastRecordIndex.
	given stateInfoSerializer: Serializer[StateInfo] = Serializer.derive[StateInfo]
	given stateInfoDeserializer: Deserializer[StateInfo] = Deserializer.derive[StateInfo]

	given ballotSerializer: Serializer[Ballot] = Serializer.derive[Ballot]
	given ballotDeserializer: Deserializer[Ballot] = Deserializer.derive[Ballot]

	given appendResultAcceptedSerializer: Serializer[AppendResult_Accepted] = Serializer.derive[AppendResult_Accepted]
	given appendResultAcceptedDeserializer: Deserializer[AppendResult_Accepted] = Deserializer.derive[AppendResult_Accepted]

	given appendResultRejectedSerializer: Serializer[AppendResult_Rejected] = Serializer.derive[AppendResult_Rejected]
	given appendResultRejectedDeserializer: Deserializer[AppendResult_Rejected] = Deserializer.derive[AppendResult_Rejected]

	given appendResultSerializer: Serializer[AppendResult] = Serializer.derive[AppendResult]
	given appendResultDeserializer: Deserializer[AppendResult] = Deserializer.derive[AppendResult]

	given jointElectorateChangeSerializer: [P <: AnyRef : Serializer] => Serializer[JointElectorateChange[P]] =
		Serializer.derive[JointElectorateChange[P]]
	given jointElectorateChangeDeserializer: [P <: AnyRef : {Deserializer, ValueOrReferenceTest}] => Deserializer[JointElectorateChange[P]] =
		Deserializer.derive[JointElectorateChange[P]]

	given soleElectorateChangeSerializer: [P <: AnyRef : Serializer] => Serializer[SoleElectorateChange[P]] =
		Serializer.derive[SoleElectorateChange[P]]
	given soleElectorateChangeDeserializer: [P <: AnyRef : {Deserializer, ValueOrReferenceTest}] => Deserializer[SoleElectorateChange[P]] =
		Deserializer.derive[SoleElectorateChange[P]]

	given electorateChangeSerializer: [P <: AnyRef : {Serializer, Deserializer}] => Serializer[ElectorateChange[P]] =
		Serializer.derive[ElectorateChange[P]]
	given electorateChangeDeserializer: [P <: AnyRef : {Deserializer, ValueOrReferenceTest}] => Deserializer[ElectorateChange[P]] =
		Deserializer.derive[ElectorateChange[P]]

	given commandRecordSerializer: [C <: AnyRef : Serializer] => Serializer[CommandRecord[C]] =
		Serializer.derive[CommandRecord[C]]
	given commandRecordDeserializer: [C <: AnyRef : Deserializer] => Deserializer[CommandRecord[C]] =
		Deserializer.derive[CommandRecord[C]]

	given leaderTransitionSerializer: Serializer[LeaderTransition] =
		Serializer.derive[LeaderTransition]
	given leaderTransitionDeserializer: Deserializer[LeaderTransition] =
		Deserializer.derive[LeaderTransition]

	given recordSerializer: [C <: AnyRef : Serializer, P <: AnyRef : {Serializer, Deserializer}] => Serializer[Record] = {
		val sCmd = commandRecordSerializer[C]
		val sLt = leaderTransitionSerializer
		val sEc = electorateChangeSerializer[P]
		(record: Record, writer: Serializer.Writer) => {
			record match {
				case cr: CommandRecord[C @unchecked] =>
					writer.putUnsignedIntVlq(0)
					sCmd.serialize(cr, writer)
				case lt: LeaderTransition =>
					writer.putUnsignedIntVlq(1)
					sLt.serialize(lt, writer)
				case ec: ElectorateChange[P @unchecked] =>
					writer.putUnsignedIntVlq(2)
					sEc.serialize(ec, writer)
			}
		}
	}

	given recordDeserializer: [C <: AnyRef : Deserializer, P <: AnyRef : {Deserializer, ValueOrReferenceTest}] => Deserializer[Record] = {
		val dCmd = commandRecordDeserializer[C]
		val dLt = leaderTransitionDeserializer
		val dEc = electorateChangeDeserializer[P]
		(reader: Deserializer.Reader) => {
			reader.readUnsignedIntVlq() match {
				case 0 => dCmd.deserialize(reader)
				case 1 => dLt.deserialize(reader)
				case 2 => dEc.deserialize(reader)
				case other => throw new Deserializer.InvalidDiscriminatorException(reader.position, other, "Record")
			}
		}
	}

	given snapshotDataSerializer: [P <: AnyRef : {Serializer, Deserializer}] => Serializer[SnapshotData[P]] =
		Serializer.derive[SnapshotData[P]]
	given snapshotDataDeserializer: [P <: AnyRef : {Deserializer, ValueOrReferenceTest}] => Deserializer[SnapshotData[P]] =
		Deserializer.derive[SnapshotData[P]]

	given electorateChangeResponseSerializer: [P <: AnyRef : Serializer] => Serializer[ElectorateChangeResponse[P]] =
		Serializer.derive[ElectorateChangeResponse[P]]
	given electorateChangeResponseDeserializer: [P <: AnyRef : {Deserializer, ValueOrReferenceTest}] => Deserializer[ElectorateChangeResponse[P]] =
		Deserializer.derive[ElectorateChangeResponse[P]]

	given processedSerializer: (sdm: ConsensusWireTypesCdm) => (Serializer[sdm.StateMachineResponse]) => Serializer[sdm.Processed] =
		Serializer.derive[sdm.Processed]

	given processedDeserializer: (sdm: ConsensusWireTypesCdm) => (Deserializer[sdm.StateMachineResponse]) => Deserializer[sdm.Processed] =
		Deserializer.derive[sdm.Processed]

	given redirectToSerializer: (sdm: ConsensusWireTypesCdm) => (Serializer[sdm.ParticipantId]) => Serializer[sdm.RedirectTo] =
		Serializer.derive[sdm.RedirectTo]

	given redirectToDeserializer: (sdm: ConsensusWireTypesCdm) => (Deserializer[sdm.ParticipantId]) => Deserializer[sdm.RedirectTo] =
		Deserializer.derive[sdm.RedirectTo]

	given unableSerializer: (sdm: ConsensusWireTypesCdm) => (Serializer[sdm.ParticipantId]) => Serializer[sdm.Unable] =
		Serializer.derive[sdm.Unable]

	given unableDeserializer: (sdm: ConsensusWireTypesCdm) => (Deserializer[sdm.ParticipantId]) => Deserializer[sdm.Unable] =
		Deserializer.derive[sdm.Unable]

	given responseToClientSerializer: (sdm: ConsensusWireTypesCdm) => (Serializer[sdm.ParticipantId], Serializer[sdm.StateMachineResponse]) => Serializer[sdm.ResponseToClient] =
		Serializer.derive[sdm.ResponseToClient]

	given responseToClientDeserializer: (sdm: ConsensusWireTypesCdm) => (Deserializer[sdm.ParticipantId], Deserializer[sdm.StateMachineResponse]) => Deserializer[sdm.ResponseToClient] =
		Deserializer.derive[sdm.ResponseToClient]

}

object ConsensusProtocolSerializers extends ConsensusProtocolSerializers
