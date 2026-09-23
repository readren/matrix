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

opaque final type Ballot = Byte

inline def INITIAL_BALLOT: Ballot = 0

extension (ballot: Ballot) {
	inline def bumped: Ballot = (ballot + 1).toByte
	inline infix def laterThan(other: Ballot): Boolean = ballot - other > 0
}

final type ElectorateChangeRequestId = String

/** Type of the identifiers of the concrete role subtypes. */
opaque final type RoleOrdinal = Byte
final val QUIESCED: RoleOrdinal = 0
final val STARTING: RoleOrdinal = 1
final val RETIRING: RoleOrdinal = 4
final val JOINING: RoleOrdinal = 8
final val ISOLATED: RoleOrdinal = 16
final val HANDING_OFF: RoleOrdinal = 17
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
		case HANDING_OFF => "HANDING_OFF"
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

opaque final type ElectionRanksSet = Int

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

trait ElectorateChangeResponse

/** Marker trait for terminal electorate change outcomes (completed or already matching). */
trait TerminalElectorateChangeResponse extends ElectorateChangeResponse

/** Marker trait for non-terminal electorate change outcomes (such as redirections, rejections, or lost tracking) that participate in election ballot propagation.
 * Propagating [[latestBallotSeen]] via prior answers allows subsequent nodes in a discovery loop to fast-forward their local ballot and clear obsolete peer caches without unprovoked ballot increments or issuing RPCs with stale ballot rounds. */
trait NonTerminalElectorateChangeResponse extends ElectorateChangeResponse {
	val latestBallotSeen: Ballot
}

/** The requested electorate change was successfully replicated to a majority (not necessarily committed). Only participants with the [[LEADER]] role answer this. */
class SUCCESSFULLY_CHANGED extends TerminalElectorateChangeResponse {
	override def toString: String = deriveToString[SUCCESSFULLY_CHANGED](this)
}

/** The participant is leading and already has the requested electorate committed. */
class ALREADY_CHANGED extends TerminalElectorateChangeResponse {
	override def toString: String = deriveToString[ALREADY_CHANGED](this)
}

/** The participant is leading but excluded (leading as a ghost). A ghost leader cannot initiate configuration changes. */
class WAIT_GHOST_LEADER_IS_DEMOTED(val latestBallotSeen: Ballot) extends NonTerminalElectorateChangeResponse {
	override def toString: String = deriveToString[WAIT_GHOST_LEADER_IS_DEMOTED](this)
}

/** The participant is a follower, suggesting redirection to the leader it follows. */
class ASK_THE_LEADER(val leaderId: AnyRef, val latestBallotSeen: Ballot) extends NonTerminalElectorateChangeResponse {
	override def toString: String = deriveToString[ASK_THE_LEADER](this)
}

/** The participant is catching up because it is joining. */
class CATCHING_UP(val latestBallotSeen: Ballot) extends NonTerminalElectorateChangeResponse {
	override def toString: String = deriveToString[CATCHING_UP](this)
}

class REQUEST_TRACKING_LOST_AFTER_FIRST_PHASE_STARTED(val latestBallotSeen: Ballot) extends NonTerminalElectorateChangeResponse {
	override def toString: String = deriveToString[REQUEST_TRACKING_LOST_AFTER_FIRST_PHASE_STARTED](this)
}

class REQUEST_TRACKING_LOST_AFTER_FIRST_PHASE_COMMITTED(val latestBallotSeen: Ballot) extends NonTerminalElectorateChangeResponse {
	override def toString: String = deriveToString[REQUEST_TRACKING_LOST_AFTER_FIRST_PHASE_COMMITTED](this)
}

class REQUEST_TRACKING_LOST_AFTER_SECOND_PHASE_STARTED(val latestBallotSeen: Ballot) extends NonTerminalElectorateChangeResponse {
	override def toString: String = deriveToString[REQUEST_TRACKING_LOST_AFTER_SECOND_PHASE_STARTED](this)
}

class EXCLUDED(val latestBallotSeen: Ballot) extends NonTerminalElectorateChangeResponse {
	override def toString: String = deriveToString[EXCLUDED](this)
}

class SECLUDED(val latestBallotSeen: Ballot) extends NonTerminalElectorateChangeResponse {
	override def toString: String = deriveToString[SECLUDED](this)
}

class STOPPED(val latestBallotSeen: Ballot) extends NonTerminalElectorateChangeResponse {
	override def toString: String = deriveToString[STOPPED](this)
}

/** Informs a participant receiving a command about the outcome of the client's previous attempt to send that command to the consensus group. */
opaque final type CommandAttemptFlag = Byte

inline def FIRST_ATTEMPT: CommandAttemptFlag = 0
inline def REDIRECTED: CommandAttemptFlag = 0x01
inline def FALLBACK: CommandAttemptFlag = 0x02
inline def LEADERSHIP_VACATED: CommandAttemptFlag = 0x06
inline def INTERNAL_VACATE_HANDOFF: CommandAttemptFlag = 0x16

extension (flag: CommandAttemptFlag) {
	inline def |(other: CommandAttemptFlag): CommandAttemptFlag = (flag | other).toByte
	inline def isFallback: Boolean = (flag & FALLBACK) != 0
	inline def isLeaderVacated: Boolean = (flag & LEADERSHIP_VACATED) == LEADERSHIP_VACATED
	inline def isInternalVacateHandoff: Boolean = (flag & INTERNAL_VACATE_HANDOFF) == INTERNAL_VACATE_HANDOFF
	inline def withInternalBitsCleared: CommandAttemptFlag = (flag & 0x0f).toByte
}

final val assertionsEnabled: Boolean = classOf[StateInfo].desiredAssertionStatus()

/**
 * A vote for a leader.
 * @param term The term for which the vote is cast.
 * @param votedId The id of the voted candidate.
 * @param reachableCommonCount The number of reachable and viable participants in the common set.
 * @param reachableTargetCount The number of reachable and viable participants in the target set.
 * @param votedRank The [[ElectionRank]] of the voted candidate.
 * @param ballot The election round to which this vote belongs.
 */
final case class Vote[Id <: AnyRef](term: Term, votedId: Id, reachableCommonCount: Int, reachableTargetCount: Int, votedRank: ElectionRank, ballot: Ballot) {
	inline def isBlank: Boolean = reachableCommonCount == 0 && reachableTargetCount == 0
	inline def isNonBlank: Boolean = !isBlank
	override def toString: String = s"Vote(term=$term, votedId=$votedId, reachableCommon=$reachableCommonCount, reachableTarget=$reachableTargetCount, rank=${ElectionRank_nameOf(votedRank)}, ballot=$ballot)"
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
final case class StateInfo(currentTerm: Term, rank: ElectionRank, termAtCommitIndex: Term, commitIndex: RecordIndex, lastRecordTerm: Term, lastRecordIndex: RecordIndex, ballot: Ballot) {
	if assertionsEnabled then assert(currentTerm >= termAtCommitIndex)

	inline def isTyingWith(other: StateInfo): Boolean = {
		tiesWith(other.currentTerm, other.rank, other.termAtCommitIndex, other.commitIndex, other.lastRecordTerm, other.lastRecordIndex)
	}

	inline def tiesWith(currentTerm: Term, rank: ElectionRank, termAtCommitIndex: Term, commitIndex: RecordIndex, lastRecordTerm: Term, lastRecordIndex: RecordIndex): Boolean = {
		this.lastRecordTerm == lastRecordTerm && this.lastRecordIndex == lastRecordIndex && this.commitIndex == commitIndex && this.rank == rank && this.currentTerm == currentTerm && this.termAtCommitIndex == termAtCommitIndex
	}

	def compareCompleteness(other: StateInfo): Int = {
		if this.lastRecordTerm > other.lastRecordTerm then 1
		else if this.lastRecordTerm < other.lastRecordTerm then -1
		else if this.lastRecordIndex > other.lastRecordIndex then 1
		else if this.lastRecordIndex < other.lastRecordIndex then -1
		else 0
	}

	override def toString: String = s"StateInfo(@$currentTerm, ${ElectionRank_nameOf(rank)}, termAtCommitIndex=$termAtCommitIndex, commitIndex=$commitIndex, ballot=$ballot)"
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

final class DiscoveryQuorumOutcome_Stale(val higherTerm: Term, val higherBallot: Ballot) extends DiscoveryQuorumOutcome {
	override def toString: String = s"Stale($higherTerm, $higherBallot)"
}

final class DiscoveryQuorumOutcome_ActiveLeaderDetected[Id <: AnyRef](val leaderId: Id, val leaderTerm: Term) extends DiscoveryQuorumOutcome {
	override def toString: String = s"ActiveLeaderDetected($leaderId, $leaderTerm)"
}

final class DiscoveryQuorumResult[Id <: AnyRef](
	val outcome: DiscoveryQuorumOutcome,
	val highestTermSeen: Term,
	val highestBallotSeen: Ballot,
	val replies: IArray[Try[StateInfo]],
	val onUnsubscribeRemaining: () => Unit
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

final class VotingQuorumOutcome_Stale(val higherTerm: Term, val higherBallot: Ballot) extends VotingQuorumOutcome {
	override def toString: String = s"Stale($higherTerm, $higherBallot)"
}

final class VotingQuorumResult[Id <: AnyRef](
	val outcome: VotingQuorumOutcome,
	val highestTermSeen: Term,
	val highestBallotSeen: Ballot,
	val replies: IArray[Try[Vote[Id]]]
)

val VotingEarlyExitCancellation: Try[Nothing] = Failure(new java.util.concurrent.CancellationException("Early termination: voting quorum already resolved"))
