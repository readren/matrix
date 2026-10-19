package readren.consensus.protocol

import readren.common.*
import scala.util.{Failure, Success, Try}

/** The type of the index for the logs where [[Record]]s are stored.
 * Index base is 1.
 * Zero means before the first log [[Record]] entry. */
final type RecordIndex = Long

/** The integer type used for term numbers.
 * Terms are numbered with consecutive integers. Each term begins when a leader is elected.
 * Starts from 1.
 * Zero means "before first election". */
opaque final type Term <: Int = Int

inline def PRE_INIT: Term = 0

extension (term: Term) def incremented: Term = term + 1


final type ElectorateChangeRequestId = String

/** Type of the identifiers of the concrete role subtypes. */
opaque final type RoleOrdinal = Byte
final val QUIESCED: RoleOrdinal = 0
final val STARTING: RoleOrdinal = 1
final val RETIRING: RoleOrdinal = 4
final val JOINING: RoleOrdinal = 8
final val ISOLATED: RoleOrdinal = 16
final val ABDICATING: RoleOrdinal = 17
final val FOLLOWER: RoleOrdinal = 18
final val LEADER: RoleOrdinal = 32

extension (thisRoleOrdinal: RoleOrdinal) {
	inline def <(other: RoleOrdinal): Boolean = thisRoleOrdinal < other
	inline def <=(other: RoleOrdinal): Boolean = thisRoleOrdinal <= other
	inline def >(other: RoleOrdinal): Boolean = thisRoleOrdinal > other
	inline def >=(other: RoleOrdinal): Boolean = thisRoleOrdinal >= other
}

def RoleOrdinal_nameOf(ordinal: RoleOrdinal): String = {
	ordinal match {
		case QUIESCED => "QUIESCED"
		case RETIRING => "RETIRING"
		case STARTING => "STARTING"
		case JOINING => "JOINING"
		case ISOLATED => "ISOLATED"
		case ABDICATING => "ABDICATING"
		case FOLLOWER => "FOLLOWER"
		case LEADER => "LEADER"
	}
}

/** Type of the identifiers of the election ranks. Each role has a fixed rank. */
opaque final type ElectionRank = Byte
/** The [[ElectionRank]] of roles that are ineligible, do not participate in elections (vote for themselves with term=0), and don't reduce the quorum threshold. */
final val ER_NONE: ElectionRank = QUIESCED
/** The [[ElectionRank]] of the [[RETIRING]] role, which cast blank votes and reduces quorum threshold of old participants by one. */
final val ER_RETIREE: ElectionRank = RETIRING
/** The [[ElectionRank]] of the [[JOINING]] role, which cast blank votes and reduces quorum threshold of new participants by one. */
final val ER_JOINER: ElectionRank = JOINING
/** The [[ElectionRank]] of the non-leading roles, which are eligible and fully participate in elections. */
final val ER_CANDIDATE: ElectionRank = ISOLATED
/** The [[ElectionRank]] of the leading roles, which are eligible and fully participate in elections, but should be chosen as leader by all voters provided the [[Term]] it exposes is the highest observed by the voter. */
final val ER_LEADING: ElectionRank = LEADER

inline def ElectionRank_from(ordinal: RoleOrdinal): ElectionRank = (ordinal & 0xFC).toByte

def ElectionRank_nameOf(rank: ElectionRank): String = {
	rank match {
		case ER_NONE => "NONE"
		case ER_RETIREE => "RETIREE"
		case ER_JOINER => "JOINER"
		case ER_CANDIDATE => "CANDIDATE"
		case ER_LEADING => "LEADING"
	}
}

/** Outcome of an electorate change request.
 * Parameterless cases are compiled as static singleton values, eliminating allocation overhead across consensus transitions.
 * @param isTerminal true if the response represents a terminal outcome (completed or already matching) that satisfies the requester; false otherwise.
 * @param suggestedPeers candidate participants to attempt contacting next upon non-terminal rejection or redirection.\
 * This set is provided on a best-effort, advisory basis and reflects the participant's local, potentially historical consensus state.\
 * It does not guarantee that the suggested peers are currently reachable or still members of the active electorate if cluster reconfiguration occurred while the responder was partitioned or isolated.\
 * Callers should fall back to broader discovery or alternative target pools if contacting the suggested peers fails. */
enum ElectorateChangeResponse[+P](val isTerminal: Boolean, val suggestedPeers: Set[? <: P]) {
	/** The requested electorate change was successfully replicated to a majority (not necessarily committed). Only participants with the [[LEADER]] role answer this. */
	case SuccessfullyChanged extends ElectorateChangeResponse[Nothing](true, Set.empty)

	/** The participant is leading and already has the requested electorate committed. */
	case AlreadyChanged extends ElectorateChangeResponse[Nothing](true, Set.empty)

	/** The participant is leading but excluded (leading as a ghost). A ghost leader cannot initiate configuration changes. */
	case WaitGhostLeaderIsDemoted[P](peers: Set[P] = Set.empty) extends ElectorateChangeResponse[P](false, peers)

	/** The participant is the leader but is currently processing another electorate change.
	 * @param inProgressRequestId the identifier of the electorate change currently being processed.
	 * @param inProgressTargetParticipants the target electorate of the change currently being processed.
	 * @param peers advisory routing hints indicating active peers known to the leader. */
	case AnotherChangeInProgress[P](inProgressRequestId: ElectorateChangeRequestId, inProgressTargetParticipants: Set[P], peers: Set[P] = Set.empty) extends ElectorateChangeResponse[P](false, peers ++ inProgressTargetParticipants)

	/** The participant is a follower, suggesting redirection to the leader it follows.\
	 * The suggested leader reflects the participant's last known leader, which may have since abdicated or disconnected. */
	case AskTheLeader[P](leaderId: P) extends ElectorateChangeResponse[P](false, Set(leaderId))

	/** The participant is catching up because it is joining as an unanchored learner.\
	 * The suggested peers set is empty until the participant absorbs its first configuration change. */
	case CatchingUp[P](peers: Set[P] = Set.empty) extends ElectorateChangeResponse[P](false, peers)

	case RequestTrackingLostAfterFirstPhaseStarted[P](peers: Set[P] = Set.empty) extends ElectorateChangeResponse[P](false, peers)

	case RequestTrackingLostAfterFirstPhaseCommitted[P](peers: Set[P] = Set.empty) extends ElectorateChangeResponse[P](false, peers)

	case RequestTrackingLostAfterSecondPhaseStarted[P](peers: Set[P] = Set.empty) extends ElectorateChangeResponse[P](false, peers)

	/** The participant is excluded from the active electorate.\
	 * The suggested peers reflect the members of the excluding configuration committed in its log. */
	case Excluded[P](peers: Set[P] = Set.empty) extends ElectorateChangeResponse[P](false, peers)

	/** The participant is isolated from the cluster majority.\
	 * The suggested peers reflect the participants in its locally recorded configuration. Because the cluster may have reconfigured during the isolation, these hints are advisory and the caller should fall back to other known participants if contacting them fails. */
	case Secluded[P](peers: Set[P] = Set.empty) extends ElectorateChangeResponse[P](false, peers)

	/** The participant has stopped or quiesced and has no active consensus state.\
	 * The suggested peers set is empty, signaling the absence of consensus-derived routing information. */
	case Stopped[P](peers: Set[P] = Set.empty) extends ElectorateChangeResponse[P](false, peers)
}

/** Informs a participant receiving a command about the outcome of the client's previous attempt to send that command to the consensus group. */
opaque final type CommandAttemptFlag = Byte

inline def FIRST_ATTEMPT: CommandAttemptFlag = 0
inline def REDIRECTED: CommandAttemptFlag = 0x01
inline def FALLBACK: CommandAttemptFlag = 0x02
inline def LEADERSHIP_VACATED: CommandAttemptFlag = 0x06

extension (flag: CommandAttemptFlag) {
	inline def |(other: CommandAttemptFlag): CommandAttemptFlag = (flag | other).toByte
	inline def isFallback: Boolean = (flag & FALLBACK) != 0
	inline def isLeaderVacated: Boolean = (flag & LEADERSHIP_VACATED) == LEADERSHIP_VACATED
}

final val assertionsEnabled: Boolean = classOf[StateInfo].desiredAssertionStatus()

/** A response to a leader election vote request (RequestBallot RPC).
 * @param term The responder's current [[Term]].
 * @param granted True if the vote was granted to the inquiring candidate; false if declined.
 * @param voterRank The [[ElectionRank]] of the responding voter. */
final case class Ballot(term: Term, granted: Boolean, voterRank: ElectionRank) {
	override def toString: String = s"Ballot(term=$term, granted=$granted, voterRank=${ElectionRank_nameOf(voterRank)})"
}

/** The local candidate nomination determined by an [[Electorate]] during Phase 1 Pre-Vote discovery.
 * @param term The [[Term]] at which the nomination was made.Ð
 * @param candidateId The nominated candidate participant identifier.
 * @param candidateRank The [[ElectionRank]] of the nominated candidate. */
final case class CandidateNomination[Id <: AnyRef](term: Term, candidateId: Id, candidateRank: ElectionRank) {
	override def toString: String = s"CandidateNomination(term=$term, candidateId=$candidateId, candidateRank=${ElectionRank_nameOf(candidateRank)})"
}

/** The result of an append operation. */
sealed trait AppendResult {
	val term: Term
}

final class AppendResult_Accepted(val term: Term, val roleOrdinal: RoleOrdinal) extends AppendResult {
	override def toString: String = s"Accepted(@$term, accepted, ${RoleOrdinal_nameOf(roleOrdinal)})"
}

final class AppendResult_Rejected(val term: Term, val firstEmptyRecordIndex: RecordIndex, val roleOrdinal: RoleOrdinal) extends AppendResult {
	override def toString: String = s"Rejected(@$term, rejected, firstEmptyRecordIndex=$firstEmptyRecordIndex, ${RoleOrdinal_nameOf(roleOrdinal)})"
}

final class AppendResult_Failed(val error: Throwable) extends AppendResult {
	override val term: Term = PRE_INIT
	override def toString: String = s"AppendResult(failed, error=$error)"
}

val failedAppendResultBuilder: Throwable => Maybe[AppendResult_Failed] = e => Maybe(new AppendResult_Failed(e))

/**
 * Information that a participant exposes about itself for the purpose of leader election.
 */
final case class StateInfo(currentTerm: Term, rank: ElectionRank, termAtCommitIndex: Term, commitIndex: RecordIndex, lastRecordTerm: Term, lastRecordIndex: RecordIndex) {
	if assertionsEnabled then assert(currentTerm >= termAtCommitIndex)

	inline def matchesValues(currentTerm: Term, rank: ElectionRank, termAtCommitIndex: Term, commitIndex: RecordIndex, lastRecordTerm: Term, lastRecordIndex: RecordIndex): Boolean = {
		this.lastRecordIndex == lastRecordIndex && this.commitIndex == commitIndex && this.rank == rank && this.currentTerm == currentTerm && this.lastRecordTerm == lastRecordTerm && this.termAtCommitIndex == termAtCommitIndex
	}

	def compareCompleteness(other: StateInfo): Int = {
		if this.lastRecordTerm > other.lastRecordTerm then 1
		else if this.lastRecordTerm < other.lastRecordTerm then -1
		else if this.lastRecordIndex > other.lastRecordIndex then 1
		else if this.lastRecordIndex < other.lastRecordIndex then -1
		else 0
	}

	override def toString: String = s"StateInfo(@$currentTerm, ${ElectionRank_nameOf(rank)}, termAtCommitIndex=$termAtCommitIndex, commitIndex=$commitIndex)"
}

sealed trait RoleDiagnostic {
	def role: RoleOrdinal
}

final case class GenericRoleDiagnostic(role: RoleOrdinal) extends RoleDiagnostic

final case class LeaderRoleDiagnostic[P <: AnyRef](
	role: RoleOrdinal,
	activeElectorateChange: ElectorateChange[P],
	peerProgress: IArray[PeerProgressDiagnostic[P]]
) extends RoleDiagnostic

final case class PeerProgressDiagnostic[P](
	peerId: P,
	highestRecordIndexKnownToBeAppended: RecordIndex,
	highestRecordIndexKnowToBeCommitted: RecordIndex
)

enum WakeUpReason {
	case QuiescenceAuthorizationRetry
	case UnreachableFollowersRetry
	case RetirementDriveRetry
	case ReplicationLoopRetry
}

trait WakeUpToken {
	def cancel(): Unit
}

class GracefullyReleased extends RuntimeException("Workspace gracefully released")

//// Discovery Quorum Outcomes ////

sealed trait DiscoveryQuorumOutcome

class DiscoveryQuorumOutcome_MajorityReached private[consensus]() extends DiscoveryQuorumOutcome {
	override def toString: String = "MajorityReached"
}
object DiscoveryQuorumOutcome_MajorityReached extends DiscoveryQuorumOutcome_MajorityReached

class DiscoveryQuorumOutcome_MajorityImpossible private[consensus]() extends DiscoveryQuorumOutcome {
	override def toString: String = "MajorityImpossible"
}
object DiscoveryQuorumOutcome_MajorityImpossible extends DiscoveryQuorumOutcome_MajorityImpossible

final class DiscoveryQuorumOutcome_Stale(val higherTerm: Term) extends DiscoveryQuorumOutcome {
	override def toString: String = s"Stale($higherTerm)"
}

final class DiscoveryQuorumOutcome_ActiveLeaderDetected[Id <: AnyRef](val leaderId: Id, val leaderTerm: Term) extends DiscoveryQuorumOutcome {
	override def toString: String = s"ActiveLeaderDetected($leaderId, $leaderTerm)"
}

final class DiscoveryQuorumResult[Id <: AnyRef](
	val outcome: DiscoveryQuorumOutcome,
	val highestTermSeen: Term,
	val replies: IArray[Try[StateInfo]]
)

val DiscoveryEarlyExitCancellation: Try[Nothing] = Failure(new java.util.concurrent.CancellationException("Early termination: discovery quorum already resolved"))

//// Voting Quorum Outcomes ////

sealed trait VotingQuorumOutcome

class VotingQuorumOutcome_Won private[consensus]() extends VotingQuorumOutcome {
	override def toString: String = "Won"
}
object VotingQuorumOutcome_Won extends VotingQuorumOutcome_Won

class VotingQuorumOutcome_Lost private[consensus]() extends VotingQuorumOutcome {
	override def toString: String = "Lost"
}
object VotingQuorumOutcome_Lost extends VotingQuorumOutcome_Lost

final class VotingQuorumOutcome_Stale(val higherTerm: Term) extends VotingQuorumOutcome {
	override def toString: String = s"Stale($higherTerm)"
}

final class VotingQuorumResult(
	val outcome: VotingQuorumOutcome,
	val highestTermSeen: Term,
	val replies: IArray[Try[Ballot]]
)

val VotingEarlyExitCancellation: Try[Nothing] = Failure(new java.util.concurrent.CancellationException("Early termination: voting quorum already resolved"))
