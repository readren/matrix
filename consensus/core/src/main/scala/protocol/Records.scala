package readren.consensus.protocol

import scala.collection.immutable.Set

/** Base trait for records stored in the consensus log. */
sealed trait Record {
	def term: Term
}

/** A log record containing a client command payload. */
final case class CommandRecord[+C <: AnyRef](override val term: Term, command: C) extends Record

/** A log record marking a leader transition for a new term. */
final case class LeaderTransition(override val term: Term) extends Record

/** Base trait for cluster electorate change records stored in the log. */
sealed trait ElectorateChange[P <: AnyRef] extends Record {
	val requestId: ElectorateChangeRequestId
	val oldParticipants: Set[P]
	val newParticipants: Set[P]

	def isActive(participantId: P): Boolean
	def activeParticipants: Set[P]
}

/** A transitional (joint consensus) electorate change record. */
final case class JointElectorateChange[P <: AnyRef](
	override val term: Term,
	override val requestId: ElectorateChangeRequestId,
	override val oldParticipants: Set[P],
	override val newParticipants: Set[P]
) extends ElectorateChange[P] {
	override def isActive(participantId: P): Boolean = newParticipants.contains(participantId) || oldParticipants.contains(participantId)

	override def activeParticipants: Set[P] = newParticipants.union(oldParticipants)
}

/** A stable (single consensus) configuration change record. */
final case class SoleElectorateChange[P <: AnyRef](
	override val term: Term,
	override val requestId: ElectorateChangeRequestId,
	coupleTerm: Term,
	override val oldParticipants: Set[P],
	override val newParticipants: Set[P]
) extends ElectorateChange[P] {
	override def isActive(participantId: P): Boolean = newParticipants.contains(participantId)

	override def activeParticipants: Set[P] = newParticipants

	/** @return true if the provided [[ElectorateChange]] is the [[JointElectorateChange]] corresponding to this [[SoleElectorateChange]]. */
	def isCoupleOf(cc: ElectorateChange[P]): Boolean = {
		cc match {
			case tcc: JointElectorateChange[P] => tcc.term == coupleTerm && tcc.requestId == requestId && tcc.newParticipants == newParticipants && tcc.oldParticipants == oldParticipants
			case _: SoleElectorateChange[P] => false
		}
	}

	def recreateCouple: JointElectorateChange[P] = JointElectorateChange(coupleTerm, requestId, oldParticipants, newParticipants)
}

/** A state machine snapshot and related log metadata.
 * @param lastIncludedRecordIndex the index of the last log entry included in the snapshot.
 * @param lastIncludedRecordTerm the term of the last log entry included in the snapshot.
 * @param latestElectorateChange the most recent electorate change as of the snapshot.
 * @param latestElectorateChangeIndex the log index of `latestElectorateChange`.
 * @param stateMachineSnapshot opaque serialized state of the state machine.
 */
final case class SnapshotData[P <: AnyRef](
	lastIncludedRecordIndex: RecordIndex,
	lastIncludedRecordTerm: Term,
	latestElectorateChange: ElectorateChange[P],
	latestElectorateChangeIndex: RecordIndex,
	stateMachineSnapshot: IArray[Byte]
)
