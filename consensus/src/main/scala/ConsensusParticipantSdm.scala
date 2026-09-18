package readren.consensus

import readren.common.*
import readren.common.Trace.Context
import readren.sequencer.{CausalFence, CoalescedQuery, Doer, ResultIncrementalCoalescing}

import java.util
import java.util.Comparator
import scala.annotation.{publicInBinary, threadUnsafe}
import scala.collection.immutable.{ArraySeq, ListSet, StringOps}
import scala.collection.mutable.ArrayBuffer
import scala.collection.{mutable, IndexedSeq as GenIndexedSeq}
import scala.compiletime.asMatchable
import scala.math.Ordering.Implicits.infixOrderingOps
import scala.reflect.ClassTag
import scala.runtime.IntRef
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}
import readren.sequencer.CompletionObserver
import readren.sequencer.OriginId

object ConsensusParticipantSdm {
	/** The type of the index for the logs where [[Record]]s are stored.
	 * Index base is 1.
	 * Zero means before the first log [[Record]] entry. */
	final type RecordIndex = Long

	/** The integer type used for term numbers.
	 * Terms are numbered with consecutive integers. Each term begins when a [[ConsensusParticipantSdm.ConsensusParticipant]] becomes leader.
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

	final type ConfigChangeRequestId = String

	/** Type of the identifiers of the concrete [[Role]] subtypes. */
	opaque final type RoleOrdinal = Byte
	final val QUIESCED: RoleOrdinal = 0
	final val STARTING: RoleOrdinal = 1
	final val RETIRING: RoleOrdinal = 4
	final val JOINING: RoleOrdinal = 8
	final val ISOLATED: RoleOrdinal = 16
	final val HANDING_OFF: RoleOrdinal = 17
	final val FOLLOWER: RoleOrdinal = 18
	final val PROMOTING: RoleOrdinal = 32
	final val LEADER: RoleOrdinal = 33
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
			case PROMOTING => "PROMOTING"
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
	final val ER_LEADING: ElectionRank = PROMOTING

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

	trait ConfigChangeResponse

	/** Marker trait for terminal configuration change outcomes (completed or already matching). */
	trait TerminalConfigChangeResponse extends ConfigChangeResponse

	/** Marker trait for non-terminal configuration change outcomes (such as redirections, rejections, or lost tracking) that participate in election ballot propagation.
	 * Propagating [[latestBallotSeen]] via prior answers allows subsequent nodes in a discovery loop to fast-forward their local ballot and clear obsolete peer caches without unprovoked ballot increments or issuing RPCs with stale ballot rounds. */
	trait NonTerminalConfigChangeResponse extends ConfigChangeResponse {
		val latestBallotSeen: Ballot
	}

	private inline def SUCCESSFULLY_CHANGED(trap: Nothing): Any = trap

	/** The requested configuration change was successfully replicated to a majority (not necessarily committed). Only participants with the [[LEADER]] role answer this. */
	class SUCCESSFULLY_CHANGED extends TerminalConfigChangeResponse {
		override def toString: String = deriveToString[SUCCESSFULLY_CHANGED](this)
	}

	private inline def ALREADY_CHANGED(trap: Nothing): Any = trap

	/** The participant is leading and already has the requested configuration committed. */
	class ALREADY_CHANGED extends TerminalConfigChangeResponse {
		override def toString: String = deriveToString[ALREADY_CHANGED](this)
	}

	private inline def WAIT_GHOST_LEADER_IS_DEMOTED(trap: Nothing): Any = trap

	/** The participant is leading but excluded (leading as a ghost). A ghost leader can not initiate configuration changes. This condition will last until either: a participant in the new configuration becomes leader and calls the append records RPC on this participant by means of a retirement driver; or this participant sees that all the participants in the new configuration have committed the [[StableConfigChange]] that excluded this participant; whichever happens first. */
	class WAIT_GHOST_LEADER_IS_DEMOTED(val latestBallotSeen: Ballot) extends NonTerminalConfigChangeResponse {
		override def toString: String = deriveToString[WAIT_GHOST_LEADER_IS_DEMOTED](this)
	}

	private inline def ASK_THE_LEADER(trap: Nothing): Any = trap
	/** The participant is a follower. So, it suggests to redirect the participant it is following. */
	class ASK_THE_LEADER(val leaderId: AnyRef, val latestBallotSeen: Ballot) extends NonTerminalConfigChangeResponse {
		override def toString: String = deriveToString[ASK_THE_LEADER](this)
	}

	private inline def CATCHING_UP(trap: Nothing): Any = trap
	/** The participant is catching-up because it is joining. */
	class CATCHING_UP(val latestBallotSeen: Ballot) extends NonTerminalConfigChangeResponse {
		override def toString: String = deriveToString[CATCHING_UP](this)
	}

	private inline def REQUEST_TRACKING_LOST_AFTER_FIRST_PHASE_STARTED(trap: Nothing): Any = trap
	/** The tracking of the [[Configuration]] change request was lost due to a leader change after the first phase was started. The process may complete or not depending on which participant is promoted. If completed, the [[ConsensusParticipantSdm.ClusterParticipant.onActiveConfigChanged]] is called. If not, just silence. // TODO avoid the mentioned silence. */
	class REQUEST_TRACKING_LOST_AFTER_FIRST_PHASE_STARTED(val latestBallotSeen: Ballot) extends NonTerminalConfigChangeResponse {
		override def toString: String = deriveToString[REQUEST_TRACKING_LOST_AFTER_FIRST_PHASE_STARTED](this)
	}

	private inline def REQUEST_TRACKING_LOST_AFTER_FIRST_PHASE_COMMITTED(trap: Nothing): Any = trap
	/** The tracking of the [[Configuration]] change request was lost due to a leader change after the first phase was committed (replicated to majority). The process will continue provided the system is sufficiently incited by client commands or further configuration change requests. Listen to [[ConsensusParticipantSdm.ClusterParticipant.onActiveConfigChanged]] calls to observe when the process completes. */
	class REQUEST_TRACKING_LOST_AFTER_FIRST_PHASE_COMMITTED(val latestBallotSeen: Ballot) extends NonTerminalConfigChangeResponse {
		override def toString: String = deriveToString[REQUEST_TRACKING_LOST_AFTER_FIRST_PHASE_COMMITTED](this)
	}

	private inline def REQUEST_TRACKING_LOST_AFTER_SECOND_PHASE_STARTED(trap: Nothing): Any = trap
	/** The tracking of the [[Configuration]] change request was lost due to a leader change after the second phase was started. The process will continue anyway provided the system is sufficiently incited by client commands or further configuration change requests. Listen to [[ConsensusParticipantSdm.ClusterParticipant.onActiveConfigChanged]] calls to observe when the process completes. */
	class REQUEST_TRACKING_LOST_AFTER_SECOND_PHASE_STARTED(val latestBallotSeen: Ballot) extends NonTerminalConfigChangeResponse {
		override def toString: String = deriveToString[REQUEST_TRACKING_LOST_AFTER_SECOND_PHASE_STARTED](this)
	}

	private inline def EXCLUDED(trap: Nothing): Any = trap
	/** The participant was excluded by a previous call to [[ConsensusParticipantSdm.ClusterParticipant.Delegate.requestConfigChange]]. */
	class EXCLUDED(val latestBallotSeen: Ballot) extends NonTerminalConfigChangeResponse {
		override def toString: String = deriveToString[EXCLUDED](this)
	}

	private inline def SECLUDED(trap: Nothing): Any = trap
	/** The participant is up but secluded from the majority at this moment. */
	class SECLUDED(val latestBallotSeen: Ballot) extends NonTerminalConfigChangeResponse {
		override def toString: String = deriveToString[SECLUDED](this)
	}

	private inline def STOPPED(trap: Nothing): Any = trap
	/** The participant is [[QUIESCED]] or not able to access to its primary state. */
	class STOPPED(val latestBallotSeen: Ballot) extends NonTerminalConfigChangeResponse {
		override def toString: String = deriveToString[STOPPED](this)
	}

	/** Informs a participant receiving a command about the outcome of the client's previous attempt to send that command to the consensus group.
	 * This flag typically refers to an attempt made toward a different [[ConsensusParticipantSdm.ConsensusParticipant]] and informs something about the resulting [[ConsensusParticipantSdm.ResponseToClient]].
	 * The client is responsible for setting the appropriate value of this flag. See the values documentation, below. */
	opaque final type CommandAttemptFlag = Byte

	/** The client has not previously attempted to send the command to any participant. */
	inline def FIRST_ATTEMPT: CommandAttemptFlag = 0

	/** The client previously sent the command to a different participant, which responded with a redirect instruction. */
	inline def REDIRECTED: CommandAttemptFlag = 0x01

	/** The client previously did a failed attempt to send the command to a participant. Either the command did not reach the destination, the response was lost, or haven't arrived withing time. */
	inline def FALLBACK: CommandAttemptFlag = 0x02

	/** Indicates the client previously sent the command to a participant that was leading at the time, but that participant relinquished leadership and responded with [[ConsensusParticipantSdm.Unable]].
	 * A curious client may notice about the deposition comparing the [[ConsensusParticipantSdm.Unable.nextAttemptFlag]] field with this value.
	 * The client does not set this value manually; it is retrieved from the [[ConsensusParticipantSdm.Unable.nextAttemptFlag]] field of the response from the deposed participant.
	 * @note This flag must be propagated through the client and participants until a new leader is established. Note that bitwise or-ing this value with [[FALLBACK]] returns this value. */
	inline def LEADERSHIP_VACATED: CommandAttemptFlag = 0x06

	/** Internal flag passed between roles within the same participant when a leader delegates command-handling during leadership vacating.
	 *
	 * When a leading participant receives a client command but must vacate leadership, it hands the command off to another role within the same participant (e.g., [[FOLLOWER]] or [[ISOLATED]]). This flag tells the receiving role:
	 *
	 * - The command was received while leading.
	 * - Respond to the client with [[LEADERSHIP_VACATED]] flag so it propagates the leader-vacated information to the next participant it tries.
	 * - Do NOT bump the ballot (already done by original leader), and also will be bumped anyway due to the role change.
	 *
	 * This value is NEVER sent over RPC - it's strictly for intra-participant role-to-role communication.
	 */
	inline def INTERNAL_VACATE_HANDOFF: CommandAttemptFlag = 0x16 // = LEADERSHIP_VACATED | 8

	extension (flag: CommandAttemptFlag) {
		inline def |(other: CommandAttemptFlag): CommandAttemptFlag = (flag | other).toByte
		inline def isFallback: Boolean = (flag & FALLBACK) != 0
		inline def isLeaderVacated: Boolean = (flag & LEADERSHIP_VACATED) == LEADERSHIP_VACATED
		inline def isInternalVacateHandoff: Boolean = (flag & INTERNAL_VACATE_HANDOFF) == INTERNAL_VACATE_HANDOFF
		inline def withInternalBitsCleared: CommandAttemptFlag = (flag & 0x0f).toByte
	}

	final val assertionsEnabled: Boolean = classOf[ConsensusParticipantSdm].desiredAssertionStatus()

	//// STANDALONE DATA TYPES ////

	/**
	 * A vote for a leader.
	 * @see [[ConsensusParticipantSdm.ClusterParticipant.chooseALeader]] and [[ConsensusParticipantSdm.ClusterParticipant.Delegate.onChooseALeader]].
	 * @tparam Id The identifier type for participants in the consensus cluster.
	 *            This must match the concrete type used to implement [[ConsensusParticipantSdm.ParticipantId]].
	 *            It allows the user to customize how participants are identified (e.g., UUID, String, custom class), while preserving type safety across [[Vote]] instances exchanges.
	 *            Although [[Vote]] is designed to travel between participants, it remains path-dependent and must be instantiated within a module that resolves [[ParticipantId]] to a concrete type.
	 * @param term The term for which the vote is cast.
	 * @param votedId The id of the voted candidate.
	 * @param reachableCommonCount The number of reachable and viable participants (including the voter itself) in the common set. The common set is [[ConfigChange.newParticipants]] if the voter active configuration is stable, and [[ConfigChange.oldParticipants]] if the voter active configuration is transitional.
	 * @param reachableTargetCount The number of reachable and viable participants (including the voter itself) in the target set. The target set is the empty set if the voter active configuration is stable, and [[ConfigChange.newParticipants]] if the voter active configuration is transitional.
	 * @param votedRank The [[ElectionRank]] of the [[Role]] of the voted candidate.
	 * @param ballot the election round to which this [[Vote]] belongs to. */
	final case class Vote[Id <: AnyRef](term: Term, votedId: Id, reachableCommonCount: Int, reachableTargetCount: Int, votedRank: ElectionRank, ballot: Ballot) {
		inline def isBlank: Boolean = reachableCommonCount == 0 && reachableTargetCount == 0

		inline def isNonBlank: Boolean = !isBlank

		override def toString: String = s"Vote(term=$term, votedId=$votedId, reachableCommon=$reachableCommonCount, reachableTarget=$reachableTargetCount, rank=${ElectionRank_nameOf(votedRank)}, ballot=$ballot)"
	}

	/** The result of an append operation.
	 * @see [[ConsensusParticipantSdm.ClusterParticipant.appendRecords]] and [[ConsensusParticipantSdm.ClusterParticipant.Delegate.onAppendRecords]].
	 * @param term The term of the follower that is responding. A value greater than the append-request's [[Term]] indicates a rejection, makes the `successOrIndexForNextAttempt` field irrelevant, and, more importantly, indicate that the inquirer has an obsolete state.
	 * @param successOrIndexForNextAttempt when relevant, a zero value indicates the appending was successful, and a [[RecordIndex]] indicates a rejection due to earlier records needed from the provided index. */
	sealed trait AppendResult { // TODO make this type only sum transmitted variants and define another type that also sums the synthetic ones with term set to PRE_INIT.
		val term: Term
	}

	/** Prevents the generation of a synthetic companion object. */
	private inline def AppendResult_Accepted(term: Term, roleOrdinal: RoleOrdinal): AppendResult_Accepted = new AppendResult_Accepted(term, roleOrdinal)
	final class AppendResult_Accepted(val term: Term, val roleOrdinal: RoleOrdinal) extends AppendResult {
		override def toString: String = s"Accepted(@$term, accepted, ${RoleOrdinal_nameOf(roleOrdinal)})"
	}

	/** Prevents the generation of a synthetic companion object. */
	private inline def AppendResult_Rejected(term: Term, firstEmptyRecordIndex: RecordIndex, roleOrdinal: RoleOrdinal): AppendResult_Rejected = new AppendResult_Rejected(term, firstEmptyRecordIndex, roleOrdinal)
	final class AppendResult_Rejected(val term: Term, val firstEmptyRecordIndex: RecordIndex, val roleOrdinal: RoleOrdinal) extends AppendResult {
		override def toString: String = s"Rejected(@$term, rejected, firstEmptyRecordIndex=$firstEmptyRecordIndex, ${RoleOrdinal_nameOf(roleOrdinal)})"
	}

	/** Prevents the generation of a synthetic companion object. */
	private inline def AppendResult_Failed(error: Throwable): AppendResult_Failed = new AppendResult_Failed(error)
	final class AppendResult_Failed(val error: Throwable) extends AppendResult {
		override val term: Term = PRE_INIT

		override def toString: String = s"AppendResult(failed, error=$error)"
	}


	private val appendFailedBuilder: Throwable => Maybe[AppendResult_Failed] = e => Maybe(new AppendResult_Failed(e))

	/**
	 * Information that a participant exposes about itself for the purpose of leader election.
	 * Other participants require this data to decide both their vote and their own role.
	 * This information is exposed not only on demand in the response to the question [[ConsensusParticipantSdm.ClusterParticipant.howAreYou]], but also proactively in some questions.
	 * @param currentTerm The term of the participant that.
	 * @param rank The [[ElectionRank]] of the [[ConsensusParticipantSdm.ConsensusParticipant.Role]] of the participant.
	 * @param termAtCommitIndex The term of the last committed record in the log of the participant that is answering.
	 * @param commitIndex The index of the last committed record in the log of the participant that is answering.
	 * @param lastRecordTerm The [[Term]] of the last [[Record]] in the log.
	 * @param lastRecordIndex The [[RecordIndex]] of the latest [[Record]] in the log.
	 * @param configIndex The index of the active [[ConfigChange]].
	 * @param ballot the election round to which this [[StateInfo]] belongs to.
	 * TODO add something that changes when the active configuration changes, like its index.
	 */
	final case class StateInfo(currentTerm: Term, rank: ElectionRank, termAtCommitIndex: Term, commitIndex: RecordIndex, lastRecordTerm: Term, lastRecordIndex: RecordIndex, configIndex: RecordIndex, ballot: Ballot) {
		if assertionsEnabled then assert(currentTerm >= termAtCommitIndex)

		/** @return true if this and the other instance are equal ignoring the [[ballot]]. */
		inline def isTyingWith(other: StateInfo): Boolean = {
			tiesWith(other.currentTerm, other.rank, other.termAtCommitIndex, other.commitIndex, other.lastRecordTerm, other.lastRecordIndex, other.configIndex)
		}

		/** @return true if this instance fields match the provided values. Note that the [[ballot]] is not considered. */
		inline def tiesWith(currentTerm: Term, rank: ElectionRank, termAtCommitIndex: Term, commitIndex: RecordIndex, lastRecordTerm: Term, lastRecordIndex: RecordIndex, configIndex: RecordIndex): Boolean = {
			this.lastRecordTerm == lastRecordTerm && this.lastRecordIndex == lastRecordIndex && this.commitIndex == commitIndex && this.rank == rank && this.configIndex == configIndex && this.currentTerm == currentTerm && this.termAtCommitIndex == termAtCommitIndex
		}

		def compareCompleteness(other: StateInfo): Int = {
			if this.lastRecordTerm > other.lastRecordTerm then 1
			else if this.lastRecordTerm < other.lastRecordTerm then -1
			else if this.lastRecordIndex > other.lastRecordIndex then 1
			else if this.lastRecordIndex < other.lastRecordIndex then -1
			else 0
		}

		override def toString: String = s"StateInfo(@$currentTerm, ${ElectionRank_nameOf(rank)}, termAtCommitIndex=$termAtCommitIndex, commitIndex=$commitIndex, configIndex=$configIndex, ballot=$ballot)"
	}

	/** A [[ConsensusParticipantSdm.StateMachine]] state's snapshot and related log metadata.
	 * @param lastIncludedRecordIndex the index of the last log entry included in the snapshot.
	 * @param lastIncludedRecordTerm the term of the last log entry included in the snapshot.
	 * @param latestConfigChange the most recent configuration change as of the snapshot.
	 * @param latestConfigChangeIndex the log index of `latestConfigChange`.
	 * @param stateMachineSnapshot opaque serialized state of the state machine. */
	final case class SnapshotData[P <: AnyRef](
		lastIncludedRecordIndex: RecordIndex,
		lastIncludedRecordTerm: Term,
		latestConfigChange: ConfigChange[P],
		latestConfigChangeIndex: RecordIndex,
		stateMachineSnapshot: IArray[Byte]
	)

	//// LOG RECORD ////

	sealed trait Record {
		def term: Term
	}

	private[consensus] final case class CommandRecord[+C <: AnyRef](override val term: Term, command: C) extends Record

	private[consensus] final case class LeaderTransition(override val term: Term) extends Record

	sealed trait ConfigChange[P <: AnyRef] extends Record {
		val requestId: ConfigChangeRequestId
		val oldParticipants: Set[P]
		val newParticipants: Set[P]

		def isActive(participantId: P): Boolean
		def activeParticipants: Set[P]
	}

	private[consensus] final case class TransitionalConfigChange[P <: AnyRef](override val term: Term, override val requestId: ConfigChangeRequestId, override val oldParticipants: Set[P], override val newParticipants: Set[P]) extends ConfigChange[P] {
		override def isActive(participantId: P): Boolean = newParticipants.contains(participantId) || oldParticipants.contains(participantId)

		override def activeParticipants: Set[P] = newParticipants.union(oldParticipants)
	}

	private[consensus] final case class StableConfigChange[P <: AnyRef](override val term: Term, override val requestId: ConfigChangeRequestId, coupleTerm: Term, override val oldParticipants: Set[P], override val newParticipants: Set[P]) extends ConfigChange[P] {
		override def isActive(participantId: P): Boolean = newParticipants.contains(participantId)

		override def activeParticipants: Set[P] = newParticipants

		/** @return true if the provided [[ConfigChange]] is the [[TransitionalConfigChange]] corresponding to this [[StableConfigChange]]. */
		def isCoupleOf(cc: ConfigChange[P]): Boolean = {
			cc match {
				case tcc: TransitionalConfigChange[P] => tcc.term == coupleTerm && tcc.requestId == requestId && tcc.newParticipants == newParticipants && tcc.oldParticipants == oldParticipants
				case _: StableConfigChange[P] => false
			}
		}

		def recreateCouple: TransitionalConfigChange[P] = TransitionalConfigChange(coupleTerm, requestId, oldParticipants, newParticipants)
	}

	//// DIAGNOSTIC ////

	sealed trait RoleDiagnostic {
		def role: RoleOrdinal
	}

	final case class GenericRoleDiagnostic(role: RoleOrdinal) extends RoleDiagnostic

	final case class LeaderRoleDiagnostic[P <: AnyRef](
		role: RoleOrdinal,
		activeConfigChange: ConfigChange[P],
		peerProgress: IArray[PeerProgressDiagnostic[P]]
	) extends RoleDiagnostic

	final case class PeerProgressDiagnostic[P](
		peerId: P,
		highestRecordIndexKnownToBeAppended: RecordIndex,
		highestRecordIndexKnowToBeCommitted: RecordIndex
	)

	//// WAKE UP ////

	/** Describes what the consensus algorithm needs retried when it requests a deferred wake-up via [[ClusterParticipant.requestWakeUp]].
	 * The host uses this to choose an appropriate delay before invoking the callback. */
	enum WakeUpReason {
		/** Retry sending PermitQuiesce to participants that failed to receive it. */
		case QuiescenceAuthorizationRetry
		/** Retry [[ClusterParticipant.appendRecords]] calls to followers that were unreachable. */
		case UnreachableFollowersRetry
		/** Retry sending records to a retiring participant that was unreachable. */
		case RetirementDriveRetry
		/** Retry the replication loop after insufficient quorum. */
		case ReplicationLoopRetry
	}

	/** An opaque token returned by [[ClusterParticipant.requestWakeUp]], used to cancel a pending wake-up via [[ClusterParticipant.cancelWakeUp]]. */
	trait WakeUpToken {
		def cancel(): Unit
	}

	//// Final state ////

	class GracefullyReleased extends RuntimeException("Workspace gracefully released")
}


/**
 * A service definition module for the [[ConsensusParticipant]] service
 *
 * A "service definition module" is trait that encapsulates a concrete service class or trait, along with its required interfaces, configuration, and type abstractions.
 * The Sdm serves as a type-level namespace and structural container, enabling modular composition, dependency injection, and architectural clarity.
 * It typically includes:
 * - Abstract type members or parameters
 * - Required interfaces as nested traits or abstract methods
 * - Configuration as abstract vals
 * - A concrete service definition that depends on the above.
 *
 * This trait implements the [[Service Definition Module Pattern]], which serves multiple purposes:
 *
 * 1. **Type Parameter Container**: Defines abstract types (`ParticipantId`, `ClientCommand`) to eliminate the need for generic type parameters on the main service class.
 *
 * 2. **Cohesive Namespace**: Groups all consensus-related types, traits, and classes together in a single namespace,
 *    including response types, data structures, cluster interfaces, persistence abstractions, and the main service class.
 *
 * 3. **Configuration Interface**: Defines the required dependencies and configuration that implementations must provide,
 *    such as the sequencer, command application logic, and various timing parameters.
 *
 * 4. **Service Factory**: Provides the main [[ConsensusParticipant]] class that implements the consensus algorithm.
 *
 * This pattern enables a clean, type-safe API while avoiding the complexity of multiple generic type parameters
 * that would otherwise be needed for a service with many interrelated types.
 *
 * @see [[ConsensusParticipant]] for the main service implementation and detailed algorithm documentation
 * @define suppressSyntheticCompanionObject Suppresses the generation of the synthetic companion object. This dummy definition creates a name collision to prevent the compiler from generating a module for universal apply, thereby avoiding the bytecode overhead of a lazy-initialized nested module. By requiring a [[Nothing]] parameter, this method is made uncallable, ensuring any inadvertent use is caught at compile-time.
 */
trait ConsensusParticipantSdm { thisModule =>

	import ConsensusParticipantSdm.*

	/** The type of participant ids. */
	type ParticipantId <: AnyRef: {Ordering, ClassTag}

	/** The type of the client identifier.
	 *
	 * Each client interacting with the consensus system must be uniquely identifiable.
	 * This identifier is used to associate commands with their origin and to enforce per-client deduplication and retry semantics.
	 */
	type ClientId

	/** The type of commands received from clients.
	 *
	 * Commands must carry enough information to support deduplication, ordering, and conflict detection. Typically, this includes a `ClientId` and a monotonically increasing request identifier or timestamp.
	 */
	type ClientCommand <: AnyRef

	//	/** Defines a total ordering over [[ClientCommand]] instances.
	//	 *
	//	 * This ordering is used to determine the relative freshness of commands from the same client.
	//	 * Implementations must ensure that:
	//	 *
	//	 *   - For any two commands `a` and `b` from the same client, if `a` is issued *after* `b`, then `clientCommandOrdering.compare(a, b) > 0`.
	//	 *   - If `a` and `b` are semantically identical (e.g., same request ID), then `clientCommandOrdering.compare(a, b) == 0`.
	//	 *   - If `a` is issued *before* `b`, then `clientCommandOrdering.compare(a, b) < 0`.
	//	 *
	//	 * The ordering must be consistent and total for commands from the same client.
	//	 * Ordering between commands from different clients may be arbitrary or undefined.
	//	 */
	//	val clientCommandOrdering: Ordering[ClientCommand]

	//	/** Extracts the client identifier from a given [[ClientCommand]].
	//	 *
	//	 * This enables the consensus module to group commands by origin and apply per-client deduplication and retry logic.
	//	 */
	//	def clientIdOf(command: ClientCommand): ClientId

	/** The type of the state-machine's [[StateMachine.applyClientCommand]] method's responses. */
	type StateMachineResponse

	/** The type of [[Workspace]] implementation. */
	type WS <: Workspace

	//// CONFIGURATION

	val MAX_RECURSION_DEPTH: Int = 20
	val MAX_PERMIT_QUIESCENCE_RETRIES: Int = 9

	/** Maximum number of inflight [[ClusterParticipant.appendRecords]] calls.
	 * Must be greater than zero. */
	val maxInFlightAppendsPerPeer = 1

	def retiringParticipantMaxRetries: Int = 9

	/** Maximum number of log entries to retain before triggering compaction.
	 * Once the log exceeds this size and the commitIndex is sufficiently advanced, entries up to the highest applied command index are discarded and a snapshot is taken. */
	val logCompactionThreshold: Int = 1000

	/** Determines how many records to retain in the log when a compaction is fired. */
	def logRetentionAfterSnapshot: Int = 10

	//// THREADING

	/** The execution sequencer that [[ConsensusParticipant]] instances uses to mutate its state.
	 *
	 * All methods that access mutable consensus state must be invoked through this sequencer to ensure deterministic,
	 * single-threaded execution. This coordination model avoids the need for blocking synchronization.
	 */
	val sequencer: Doer

	inline def isInSequence: Boolean = sequencer.isInSequence

	//// STATE MACHINE

	/** Describes the interface that a [[ConsensusParticipant]] relies on to interact with the state machine. */
	trait StateMachine {
		/** Applies the given [[ClientCommand]] to the state machine.\
		 * @return a [[sequencer.Capture]] that yields the [[StateMachineResponse]]
		 */
		def applyClientCommand(index: RecordIndex, command: ClientCommand): sequencer.Capture[StateMachineResponse]

		/** Returns a [[sequencer.Task]] that yields the [[RecordIndex]] most recently passed to [[applyClientCommand]] whose corresponding [[sequencer.Capture]] is completed.\
		 * If the implementation cannot determine this index or prefers to relay on the [[Workspace]]'s log, it should return zero. That instructs the [[ConsensusParticipant]] to install the [[Workspace]]'s latest snapshot and replay all the commands in its log.\
		 * This method is invoked only during recovery after restarts or persistence failures.
		 */
		def recoverIndexOfLastAppliedCommand: sequencer.Capture[RecordIndex]

		/** Creates a snapshot of the state machine state at the moment of the call.\
		 * The implementation should support calls to [[StateMachine.applyClientCommand]] while this method is running, keeping the result invariant.\
		 * This method is called when the log exceeds the compaction threshold and all entries up to the highest applied command index have been applied.\
		 * @return a [[sequencer.Capture]] that yields the serialized state machine state. */
		def takeSnapshot(): sequencer.Capture[IArray[Byte]]

		/** Installs a snapshot received from the leader, replacing the current state machine state.\
		 * @param data the serialized state machine state.
		 * @return a [[sequencer.Capture]] that completes when the snapshot has been installed. */
		def installSnapshot(data: IArray[Byte]): sequencer.Capture[Unit]
	}

	//// RESPONSE TO CLIENT

	/** The response to a client command.
	 * @see [[ClusterParticipant.Delegate.onCommandFromClient]]. */
	sealed trait ResponseToClient

	/** The command was appended to a majority of the participants persistent logs, and applied to the leader's [[StateMachine]] which responded with the specified [[content]]. */
	final case class Processed(recordIndex: RecordIndex, content: StateMachineResponse) extends ResponseToClient

	/** The client has to repeat the command to the specified participant. This happens when the receiver is or becomes a [[FOLLOWER]]. */
	final case class RedirectTo(participantId: ParticipantId) extends ResponseToClient

	/** Indicates the participant cannot process the command because its state is [[ISOLATED]], [[RETIRING]], or [[QUIESCED]].
	 * Upon receiving this response, the client should retry the request with one of the [[otherParticipants]] using the provided `nextAttemptFlag`.
	 * @param nextAttemptFlag The value the client must pass in the `attemptFlag` parameter of the [[ClusterParticipant]]'s command-delivery RPC method. This RPC method is responsible for passing both the flag and the command to [[ClusterParticipant.Delegate.onCommandFromClient]].
	 * @param otherParticipants A set of alternative participant identifiers for the client to attempt. This list is provided on a best-effort basis and may be incomplete or contain stale/unavailable participants. The first elements of the list are more probable to be correct and up-to-date than the last ones. */
	final case class Unable(nextAttemptFlag: CommandAttemptFlag, otherParticipants: ListSet[ParticipantId]) extends ResponseToClient

	//	/** The command was rejected because a newer command from the same origin has already been processed, while this one was never received earlier.
	//	 *
	//	 * The client should discard this command, assuming the [[StateMachine]] does not require contiguous, gap‑free, monotonic ordering of command identifiers.
	//	 *
	//	 * If the [[StateMachine]] does require strict contiguous ordering, it must enforce that policy itself by rejecting non‑contiguous commands with a special [[StateMachineResponse]], which the client will receive wrapped inside a [[Processed]].
	//	 * @param command the [[ClientCommand]] received in the request
	//	 * @param lastCommandIndex the [[RecordIndex]] of the last [[ClientCommand]] received from the same client (same [[ClientId]]).
	//	 */
	//	final case class Superseded(command: ClientCommand, lastCommandIndex: RecordIndex) extends ResponseToClient

	//	/** The command predates the last snapshot boundary, so the leader cannot determine whether it was ever processed, nor can it replay the corresponding [[StateMachineResponse]].
	//	 *
	//	 * The client must resolve this situation (e.g. by resynchronizing or discarding).
	//	 * @param command the [[ClientCommand]] received in the request
	//	 */
	//	final case class TooOld(command: ClientCommand) extends ResponseToClient

	//	/** The command was previously processed, but its response is not replayed because replayability of responses to commands older than the last received has been disabled by the [[ConsensusParticipantSdm.Workspace.indexOf]] implementation returning 0.
	//	 *
	//	 * The client should treat this as a stale retry and discard it.
	//	 * @param command the [[ClientCommand]] received in the request
	//	 * @param lastCommandIndex the [[RecordIndex]] of the last [[ClientCommand]] received from the same client (same [[ClientId]]).
	//	 */
	//	final case class Stale(command: ClientCommand, lastCommandIndex: RecordIndex) extends ResponseToClient

	//	/** Tells that the [[Workspace]] of the [[ConsensusParticipant]] service is inconsistent. Should never happen. TODO consider restarting the service in this situation, instead. */
	//	final case class InconsistentState(detail: String) extends ResponseToClient

	//// CLUSTER

	/** Specifies what a [[ConsensusParticipant]] service requires from the cluster-participant-service it is bound to.
	 *
	 * A [[ClusterParticipant]] represents a single participant within a specific cluster and provides the identity, membership, and communication mechanisms required by the bound [[ConsensusParticipant]] service.
	 *
	 * Responsibilities of a [[ClusterParticipant]] include:
	 * - Exposing the identity of the participant it services via [[boundParticipantId]].
	 * - Providing the initial cluster membership via [[getInitialParticipants]], which must return the same set across all participants listed.
	 * - Acting as the source of truth for cluster membership and determining when a configuration change should be triggered. This includes reacting to node join/leave events, quorum loss, scaling decisions, or health-based adjustments.
	 * - Initiating configuration transitions by calling [[Delegate.requestConfigChange]] when a change is required.
	 * - Routing inter-participant RPCs (e.g., [[howAreYou]], [[chooseALeader]], [[appendRecords]]) to the appropriate [[Delegate]] methods.
	 * - Delivering client commands and consensus messages to the bound [[ConsensusParticipant]] via the last [[Delegate]] set with [[setBound]] by the [[ConsensusParticipant]].
	 * - Scheduling deferred wake-ups when requested by the [[ConsensusParticipant]] via [[requestWakeUp]], and invoking the provided callback within the [[sequencer]] after an appropriate host-determined delay.
	 * - Ensuring all invocations occur within the [[sequencer]] thread.
	 *
	 * Each [[ClusterParticipant]] instance is tightly bound to a single [[ConsensusParticipant]] instance.
	 * If a cluster-service had to service more than one [[ConsensusParticipant]] instance simultaneously, it would have to create a different instance of [[ClusterParticipant]] for each.
	 */
	trait ClusterParticipant {

		/** The implementation should return the identifier of the participant that this [[ClusterParticipant]] service — and its bound [[ConsensusParticipant]] service — are responsible for. */
		val boundParticipantId: ParticipantId

		/** The implementation should return  the identifiers of the consensus participants in the cluster formation, when all participants are brand-new (empty logs).
		 * This method is called by the bound [[ConsensusParticipant]] when started for the first time ([[Workspace.isBrandNew]] returns true); and must return exactly the same set across all participants listed.
		 * The returned set must include the identifier of the participant serviced by this [[ClusterParticipant]] instance.
		 */
		def getInitialParticipants: Set[ParticipantId]

		/** The implementation should return the identifiers of the participants that this [[ClusterParticipant]] optimistically suspect are members of the consensus set, excluding the bound one.
		 * This method is called when the [[ConsensusParticipant]] that is [[STARTING]] or [[QUIESCED]] has to respond [[Unable]] to a client. */
		def getOtherProbableParticipants: ListSet[ParticipantId]

		/** **Outbound bridge (advisory hook)**: Called by the bound [[ConsensusParticipant]] to advise that its active [[ConsensusParticipant.Configuration]] has changed, and now it expects connectivity with the active participants of the provided [[ConfigChange]].\
		 *
		 * This method is invoked upon activation of a new [[ConsensusParticipant.Configuration]] to tell the cluster-layer which are the participants that the consensus-layer expects to be reachable.\
		 * Given configuration changes is a two-phase process, a call to [[Delegate.requestConfigChange]] causes two [[ConfigChange]] records to be appended and, therefore, two calls to this method per involved [[ConsensusParticipant]] service.\
		 * Successive calls with the same argument may occur. Implementations may ignore such calls only if no intervening call with a different argument has occurred — i.e., if the configuration has not changed.\
		 * @param change The [[ConfigChange]] that backs the activated [[ConsensusParticipant.Configuration]].
		 * @param changeIndex the [[RecordIndex]] of the applied [[ConfigurationChange]]
		 */
		def onActiveConfigChanged(change: ConfigChange[ParticipantId], changeIndex: RecordIndex, roleOrdinal: RoleOrdinal): Unit

		/** **Outbound bridge (advisory hook)**: Called by the bound [[ConsensusParticipant]] after it becomes quiesced. This allows this [[ClusterParticipant]] service to release the resources dedicated to it. */
		def onQuiesced(motive: Try[String]): Unit

		/** Called by the bound [[ConsensusParticipant]] when it needs to be woken up after some host-determined delay.
		 * The host should eventually invoke the provided `callback` within the [[sequencer]], after an appropriate delay.
		 * The [[WakeUpReason]] conveys what the consensus algorithm needs retried so the host can choose an appropriate delay.
		 *
		 * Must be called within the [[sequencer]].
		 *
		 * @param reason describes what the consensus algorithm needs retried.
		 * @param wakeupsDone the number of related wake-up requests done before. Allows the implementation to determine a delay that depends on the number of failed attempts.
		 * @param callback the function to invoke when the delay elapses. Must be invoked within the [[sequencer]].
		 * @return a token that can be passed to [[cancelWakeUp]] to cancel the pending wake-up.
		 */
		def requestWakeUp(reason: WakeUpReason, wakeupsDone: Int, callback: () => Unit): WakeUpToken

		/** Defines the operations that the [[ConsensusParticipant]] exposes to this [[ClusterParticipant]] instance, specially the call-backs methods for the events that the [[ConsensusParticipant]] needs to be noticed of.
		 *
		 * The [[ConsensusParticipant]] is responsible for setting the bound to this [[ClusterParticipant]] instance by calling [[setBound]].
		 * This [[ClusterParticipant]] instance talks to the bound [[ConsensusParticipant]] by calling these methods.
		 * For example, to deliver client commands, consensus messages, and request cluster-configuration changes.
		 * All invocations must occur within the [[sequencer]] to preserve consistency and serialization guarantees.
		 *
		 * This delegate represents one direction of the interaction between the tightly bound [[ClusterParticipant]] and [[ConsensusParticipant]] services.
		 */
		trait Delegate {

			/** **Inbound bridge**: Handles a client-submitted command intended for the state machine.
			 *
			 * This method is invoked by the bound [[ClusterParticipant]] when a client sends a command to this participant.
			 * It must be called within the [[sequencer]].
			 * TODO Analyze the alternative of returning a Task that yields a variant of [[Unable]] that details the failure when something fails.
			 *
			 * @param command The command issued by the client for state-machine execution.
			 * @param attemptFlag Indicates the outcome of the client's previous attempt to send this command.
			 * @return A [[sequencer.Task]] that yields one of:
			 *         - The state-machine's response to the client.
			 *         - A [[RedirectTo]] message instructing the client to contact the current leader.
			 *         - An [[Unable]] message indicating that this participant cannot currently reach consensus.
			 */
			def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.Capture[ResponseToClient]

			/** **Inbound bridge**: This method is invoked by this [[ClusterParticipant]] when another participant calls [[howAreYou]] on the [[ParticipantId]] of the owner of this [[Delegate]]
			 *
			 * Must be called within the [[sequencer]].
			 * @param inquirerId The id of the participant that called [[howAreYou]].
			 * @param inquirerInfo The [[StateInfo]] of the participant that called [[howAreYou]].
			 * @return The state information of the destination participant.
			 */
			def onHowAreYou(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.Capture[StateInfo]

			/** **Inbound bridge**: This method is invoked by this [[ClusterParticipant]] when another participant calls [[chooseALeader]] on the [[ParticipantId]] of the owner of this [[Delegate]]
			 *
			 * Must be called within the [[sequencer]].
			 * @param inquirerId The id of the participant that called [[chooseALeader]].
			 * @param inquirerInfo Information about the state of the participant that called.
			 * @return A [[sequencer.Capture]] that yields a [[Vote]] indicating the candidate chosen by the listening participant for the specified term.
			 */
			def onChooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.Capture[Vote[ParticipantId]]

			/** **Inbound bridge**: This method is invoked by this [[ClusterParticipant]] when another participant calls [[appendRecords]] on the [[ParticipantId]] of the owner of this [[Delegate]]
			 *
			 * Must be called within the [[sequencer]].
			 * @param inquirerId The id of the participant that called [[appendRecords]].
			 * @param inquirerTerm The term of the participant that called [[appendRecords]].
			 * @param prevRecordIndex The index of record after which the specified `records` should be appended.
			 * @param prevRecordTerm The term of the record after which the specified `records` should be appended.
			 * @param batch The records to append. // TODO pass the whole log buffer and the range of records to send instead. That would avoid the allocation of the array.
			 * @param leaderCommit The index of the highest log entry known to be committed (replicated to a majority) according to the inquirer.
			 * @return A [[sequencer.Capture]] that yields the result of the append operation.
			 */
			def onAppendRecords(inquirerId: ParticipantId, inquirerTerm: Term, prevRecordIndex: RecordIndex, prevRecordTerm: Term, batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.Capture[AppendResult]

			/** **Inbound bridge**: Handles an InstallSnapshot RPC from the leader.
			 * Invoked when the leader has discarded log entries that this follower needs.
			 *
			 * Must be called within the [[sequencer]].
			 * @param inquirerId the leader's participant ID.
			 * @param inquirerTerm the leader's current term.
			 * @param snapshot the snapshot data including state machine state and metadata.
			 * @return A [[sequencer.Capture]] that yields a rejecting [[AppendResult]] equivalent to the one that [[onAppendRecords]] would return when asks for earlier records starting from [[SnapshotData.lastIncludedRecordIndex]] + 1. */
			def onInstallSnapshot(inquirerId: ParticipantId, inquirerTerm: Term, snapshot: SnapshotData[ParticipantId], batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.Capture[AppendResult]

			/** **Inbound bridge**: This method is invoked by this [[ClusterParticipant]] when another participant calls [[permitQuiescence]] on the [[ParticipantId]] of the owner of this [[Delegate]].
			 * @param grantorId the identifier of the participant that granted permission to quiesce.
			 * @param indexOfGrantedStableConfigChange The index of the [[StableConfigChange]] record for which the permission to quiesce was granted. Said record is the one that excludes the destination participant.
			 */
			def onQuiescencePermitted(grantorId: ParticipantId, indexOfGrantedStableConfigChange: RecordIndex): Unit

			/** Allows the [[ClusterParticipant]] service to request changes to the set of consensus-participants.
			 * Usually called whenever the set of consensus-participants has forcefully changed (i.e: a cluster-member included in the current consensus-participants-set went down) or is about to change (i.e: a node intended to be part of consensus-participants-set joined the cluster, or is going to leave the cluster for maintenance).
			 * To improve availability during planned cluster-membership transitions, the manager of the planed change should do the following:
			 *		1 call this method on every consensus-participant service to ensure the leader gets noticed, // TODO this is awkward. Make the configuration-change request be propagated to the leader when received by non-leaders.
			 *		2 wait until either:
			 *			- the returned [[sequencer.Capture]] yields either [[SUCCESSFULLY_CHANGED]] or [[ALREADY_CHANGED]] for any of the consensus-participants,
			 *			- or the [[onActiveConfigChanged]] is called in any of the consensus-participants with the provided request identifier or desired participants set.
			 *
			 * @param requestId an identifier chosen by the caller that will be propagated up to the invocations of the [[onActiveConfigChanged]] method of each of the [[ClusterParticipant]] instances bound to the involved [[ConsensusParticipant]] services.
			 * @param desiredParticipantsSet the identifiers of the participants that are going to seek consensus from now on.
			 * @param priorAnswer should contain the response to the last request done by the inquirer to this or any other participant, if any.
			 * @return a [[sequencer.Capture]] that yields:
			 *         [[SUCCESSFULLY_CHANGED]] if the requested change was successfully completed.
			 *         [[ALREADY_CHANGED]] if the requested change is already done or in progress.
			 *         [[ASK_THE_LEADER]] if none of the previous bullet is true and the [[ConsensusParticipant]] is a [[FOLLOWER]].
			 *         [[STOPPED]] if the participant is not able to become neither the [[LEADER]] nor a [[FOLLOWER]]
			 *         - currently the leader or a follower that already has the desired participants set as the current or scheduled one;
			 *         - currently the leader and was able to replicate the corresponding [[TransitionalConfigChange]] to a majority according to that same [[TransitionalConfigChange]] rules. */
			def requestConfigChange(requestId: ConfigChangeRequestId, desiredParticipantsSet: Set[ParticipantId], priorAnswer: Maybe[ConfigChangeResponse]): sequencer.Capture[ConfigChangeResponse]
		}

		/**
		 * The [[ConsensusParticipant]] calls this method to expose itself through a [[Delegate]] instance which specifies the operations through which this [[ClusterParticipant]] can talk to it.
		 * The [[ConsensusParticipant]] calls this method not only at startup to set the bound, but also several times later, every time it changes its behavior (role).
		 * Is called within the [[sequencer]] thread. */
		def setBound(delegate: Delegate): Unit

		/** Called by the [[ConsensusParticipant]] when it is leaving existence. */
		def removeBound(): Unit

		extension (destinationId: ParticipantId) {
			/**
			 * Asks the destination participant how it is doing.
			 * The implementation should make, somehow, the destination participant's [[Delegate.onHowAreYou]] to be called, and return what it returns.
			 * Called within the [[sequencer]] thread.
			 * @param inquirerInfo The term of the participant that is asking.
			 * @return A [[sequencer.Capture]] that yields the state information of the destination participant.
			 */
			def howAreYou(inquirerInfo: StateInfo): sequencer.Capture[StateInfo]

			/**
			 * Request the destination participant to choose a leader.
			 * The implementation should make, somehow, the destination participant's [[Delegate.onChooseALeader]] to be called, and return what it returns.
			 * Called within the [[sequencer]] thread.
			 * @param inquirerId The id of the participant that is asking.
			 * @param inquirerInfo Information about the state of the participant that is asking.
			 * @return A [[sequencer.Capture]] that yields a [[Vote]] indicating the candidate chosen by the destination participant.
			 */
			def chooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.Capture[Vote[ParticipantId]]

			/**
			 * Request the destination participant to append records.
			 * The implementation should make, somehow, the destination participant's [[Delegate.onAppendRecords]] to be called with the same parameter values, and return what it returns.
			 * // TODO consider the addition of a wrapper that suppresses records already sent in in-flight calls, and maybe avoids the call at all if no records are left.
			 * This method is called within the [[sequencer]].
			 * @param inquirerTerm The term of the participant that is asking.
			 * @param prevLogIndex the [[RecordIndex]] of the [[Record]] after which the records should be appended.
			 * @param prevLogTerm the expected [[Term]] of the [[Record]] at `prevLogIndex`.
			 * @param batch The records to append.
			 * @param leaderCommit The index of the highest log entry known to be committed (replicated to a majority) according to the inquirer.
			 * @return A [[sequencer.Capture]] that yields the result of the append operation.
			 */
			def appendRecords(inquirerTerm: Term, prevLogIndex: RecordIndex, prevLogTerm: Term, batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.Capture[AppendResult]

			/**
			 * Sends a snapshot to the destination participant, replacing its log and state machine state. Only leaders call this method.
			 * The implementation should make, somehow, the destination participant's [[Delegate.onInstallSnapshot]] to be called with the same parameter values, and return what it returns.
			 * This method is called within the [[sequencer]].
			 * @param inquirerTerm The term of the participant that is sending the snapshot.
			 * @param snapshot the snapshot data including state machine state and metadata.
			 * @return A [[sequencer.Capture]] that yields the result of the installation operation.
			 */
			def installSnapshot(inquirerTerm: Term, snapshot: SnapshotData[ParticipantId], batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.Capture[AppendResult]


			/**
			 * Authorizes the destination participant to transition from [[RETIRING]] to the terminal [[QUIESCED]] [[ConsensusParticipant.Role]], provided it becomes [[RETIRING]] due to being excluded by the [[StableConfigChange]] at the specified index.
			 * This bridge is invoked by the leader established AFTER the second phase of a configuration change (that excluded the destination participant) has finalized.
			 * By requiring the leader of the new configuration to issue this permission, the system ensures the caller is a stable authority within the finalized membership set — thereby excluding the 'ghost leader' from performing this final decommissioning.
			 *
			 * This call is the trigger for the **Retiring Quorum-Buffering** mechanism to release the buffer.
			 * The purpose of this mechanism is to maintain the quorum safety of the old participants set during joint consensus.
			 * By holding excluded participants in the [[RETIRING]] role, the system ensures they contribute to the quorum of the old set (by not voting but effectively lowering the required threshold of active votes) until a new, stable majority is functionally proven by a new leader.
			 *
			 * @param indexOfGrantedStableConfigChange The index of the [[StableConfigChange]] record for which the authorization is granted, which is the one that excludes the destination participant.
			 * @return A [[sequencer.Capture]] that completes successfully if either: the permission was successfully delivered, or the participant is already in a post-retirement state ([[QUIESCED]], released, or no longer exists).
			 */
			def permitQuiescence(indexOfGrantedStableConfigChange: RecordIndex): sequencer.Capture[Unit]
		}
	}


	//// PERSISTENCE 

	/** Specifies the unit of work that a [[ConsensusParticipant]] requires to manage its persistent state.
	 * Instances of this trait must be accessed only within a `primaryStateUpdater` passed to the [[sequencer.CausalFence.advance]] method of the [[sequencer.CausalFence]] instance of the [[ConsensusParticipant]]. */
	trait Workspace {

		/** The current term according to this participant.
		 * The Initial value is zero.
		 * Zero means "before the first election". */
		def getCurrentTerm: Term

		def setCurrentTerm(term: Term): Unit

		/** The candidate participant id this participant voted for in [[getCurrentTerm]], or [[Maybe.empty]] if none. */
		def getVotedFor: Maybe[ParticipantId]

		/** Sets the candidate participant id this participant voted for in [[getCurrentTerm]]. */
		def setVotedFor(votedFor: Maybe[ParticipantId]): Unit

		/** Sets both the current term and the voted candidate atomically. */
		def setTermAndVote(term: Term, votedFor: Maybe[ParticipantId]): Unit = {
			setCurrentTerm(term)
			setVotedFor(votedFor)
		}

		/** The index of the oldest [[Record]] stored in the log buffer. The initial value is 1. */
		def logBufferOffset: RecordIndex

		/** The index of the first empty entry in the log. The initial value is 1. */
		def firstEmptyRecordIndex: RecordIndex

		def getRecordAt(index: RecordIndex): Record

		/** Returns the records in the log starting at `from` and up to `until` exclusive. */
		def getRecordsBetween(from: RecordIndex, until: RecordIndex): IArray[Record]

		/** Appends a record. */
		def appendRecord(record: Record): Unit

		/** Discards all records in the log buffer starting from the specified index (inclusive). */
		def truncateSuffix(fromIndex: RecordIndex): Unit

		/** Replaces the whole log with the provided snapshot and tail records. */
		def resetLog(snapshot: SnapshotData[ParticipantId], tailRecords: IArray[Record]): Unit

		/** Truncates the log by replacing its earlier records with the provided snapshot.
		 * After this call, [[logBufferOffset]] must return `snapshot.lastIncludedRecordIndex + 1`.
		 * @param snapshot the consolidated [[SnapshotData]] to replace the truncated records */
		def truncatePrefix(snapshot: SnapshotData[ParticipantId]): Unit

		/** @return the [[SnapshotData]] produced by the last call to [[truncatePrefix]]. */
		def latestSnapshot: Maybe[SnapshotData[ParticipantId]]

		/** Called by the [[ConsensusParticipant]] to inform that it will not reference this [[Workspace]] instance anymore and may be purged. */
		def release(): sequencer.Capture[Unit]
	}

	/** Defines what a [[ConsensusParticipant]] requires from a persistence service to load and save its [[Workspace]].
	 * Implementations may assume that all methods of this trait are invoked within the [[sequencer]] thread, enabling optimizations such as avoiding unnecessary creation of new task objects. */
	trait Storage {
		def load: sequencer.Capture[WS]

		/** Saves the workspace to the persistence storage.
		 * Design Note: A failure to save the workspace should restart the [[ConsensusParticipant]] as if it had crashed and lost all non-persistent variables.
		 */
		def save(workspace: WS): sequencer.Capture[Unit]
	}

	//// NOTIFICATIONS


	trait NotificationListener {
		def onStarting(previous: RoleOrdinal, indexOfTheIncludingConfigChange: RecordIndex): Unit

		def onStarted(previous: RoleOrdinal, term: Term, initialConfigChange: ConfigChange[ParticipantId], isSeed: Boolean): Unit

		def onBecameQuiesced(previous: RoleOrdinal, term: Term, motive: Try[String]): Unit

		def onJoining(previous: RoleOrdinal, indexOfTheIncludingConfigChange: RecordIndex): Unit

		def onBecameIsolated(previous: RoleOrdinal, term: Term): Unit

		def onBecameCandidate(previous: RoleOrdinal, term: Term): Unit

		def onBecameFollower(previous: RoleOrdinal, term: Term, leaderId: ParticipantId): Unit

		def onPromoting(previous: RoleOrdinal, term: Term): Unit

		def onBecameLeader(previous: RoleOrdinal, term: Term): Unit

		def onAbdicating(term: Term): Unit

		def onRetiring(previous: RoleOrdinal, term: Term): Unit

		def onRoleLeft(left: RoleOrdinal, term: Term): Unit

		def onCommitIndexChanged(previous: RecordIndex, current: RecordIndex, as: RoleOrdinal, at: Term): Unit

		/** Called whenever a [[CommandRecord]] is applied to the [[StateMachine]]. */
		def onCommandApplied(appliedCommandIndex: RecordIndex, appliedCommandTerm: Term): Unit

		def onActiveConfigChanged(currentRole: RoleOrdinal, currentTerm: Term, configChangeIndex: RecordIndex, configChange: ConfigChange[ParticipantId]): Unit
	}

	/**
	 * A convenience [[NotificationListener]] implementation with no-op methods.
	 * Extend this class and override only the methods you need.
	 */
	open class DefaultNotificationListener extends NotificationListener {
		override def onStarting(previous: RoleOrdinal, indexOfTheIncludingConfigChange: RecordIndex): Unit = ()

		override def onStarted(previous: RoleOrdinal, term: Term, initialConfigChange: ConfigChange[ParticipantId], isSeed: Boolean): Unit = ()

		override def onBecameQuiesced(previous: RoleOrdinal, term: Term, motive: Try[String]): Unit = ()

		override def onJoining(previous: RoleOrdinal, indexOfTheIncludingConfigChange: RecordIndex): Unit = ()

		override def onBecameIsolated(previous: RoleOrdinal, term: Term): Unit = ()

		override def onBecameCandidate(previous: RoleOrdinal, term: Term): Unit = ()

		override def onBecameFollower(previous: RoleOrdinal, term: Term, leaderId: ParticipantId): Unit = ()

		override def onPromoting(previous: RoleOrdinal, term: Term): Unit = ()

		override def onBecameLeader(previous: RoleOrdinal, term: Term): Unit = ()

		override def onAbdicating(term: Term): Unit = ()

		override def onRetiring(previous: RoleOrdinal, term: Term): Unit = ()

		override def onRoleLeft(left: RoleOrdinal, term: Term): Unit = ()

		override def onCommitIndexChanged(previous: RecordIndex, current: RecordIndex, as: RoleOrdinal, at: Term): Unit = ()

		override def onCommandApplied(appliedCommandIndex: RecordIndex, appliedCommandTerm: Term): Unit = ()

		override def onActiveConfigChanged(currentRole: RoleOrdinal, currentTerm: Term, configChangeIndex: RecordIndex, configChange: ConfigChange[ParticipantId]): Unit = ()
	}

	inline def checkWithin(): Unit = {
		if ConsensusParticipantSdm.assertionsEnabled && !isInSequence then throw new AssertionError(Doer.checkWithinMsg(sequencer))
	}



	//// PARTICIPANT'S CONSENSUS SERVICE ////

	/**
	 * A service of consensus for managing a replicated log with other participants in a distributed system.
	 *
	 * This service algorithm enables multiple participants (typically hosted on different nodes) to reach agreement on values.
	 * Once consensus is reached on a value, that decision becomes final and irreversible.
	 *
	 * Like Raft, this consensus algorithm relies on strong leadership and makes progress when a majority of participants are available. The algorithm is based on the following core principles:
	 *
	 * - Each participant has a unique identifier.
	 * - Each participant operates in one of several behavioral states depending on his role: starting, isolated, candidate, follower, leader, or quiesced.
	 * - The system makes progress when a leader is elected and can replicate client commands to a majority of participants.
	 * - Only the leader accepts and processes client commands.
	 * - Time is divided into terms, with each term beginning with a leader election.
	 * - Each participant maintains a persistent log that stores client commands along with the term in which they were appended.
	 * - Each participant tracks a commit index representing the highest log entry known to be committed.
	 * - The leader maintains replication state for each follower, tracking the next record to send and the highest replicated record index.
	 *
	 * The key innovation of this algorithm is its deterministic election process, which differs from Raft in several ways:
	 *
	 * - No heartbeat mechanism: Elections are triggered by client fallback requests rather than periodic timeouts. Fallback requests are those that are a retry of a previous request sent to a different participant.
	 * - Deterministic candidate selection: All participants choose the same leader candidate when they have identical knowledge
	 *   of cluster state, even with partial information about other participants
	 * - Leader selection criteria (in order of precedence): highest current term, has leading role or not, participates in elections or not, highest commit-index term, highest commit-index, and lexicographically the smallest participant ID.
	 *
	 * The election process works as follows:
	 *
	 * - Elections are triggered when a client request is received by a participant in isolated or candidate state, or a follower if the request is marked with the "isFallback" flag.
	 * - The initiating participant queries other participants with "how are you" questions to update its role and view of the cluster state.
	 * - These queries include a flag indicating that "a leader may be missing" to prompt followers to update their own views and roles.
	 * - When a participant receives a "how are you" query, it responds with its current state information.
	 * - Participants in "isolated" or "candidate" also update their view of the cluster state and their role by querying other participants. Followers do the same but only if the query is tagged with "a leader may be missing".
	 * - A participant becomes leader if either:
	 *   - It receives responses from all participants (which means it has all the information necessary to unambiguously determine the leader without asking vor votes) and based on those responses the leader-selection-criteria points it as the chosen leader.
	 *   - It receives responses from a majority of participants and, when asked for their vote, all of them vote for it. Note that votes are requested only when a participant is not reachable.
	 * - The term number is incremented by the newly elected leader. The other participants notice about the new term when they receive an "append log records" request.
	 *
	 * Invariants:
	 * - Election Safety: at most one leader can be elected in a given term. §5.2
	 * - Leader Append-Only: a leader never overwrites or deletes entries in its log; it only appends new entries. §5.3
	 * - Log Matching: if two logs contain an entry with the same index and term, then the logs are identical in all entries up through the given index. §5.3
	 * - Leader Completeness: if a log entry is committed in a given term, then that entry will be present in the logs of the leaders for all higher-numbered terms. §5.4
	 * - State Machine Safety: if a server has applied a log entry at a given index to its state machine, no other server will ever apply a different log entry for the same index. §5.4.3
	 *
	 * Design notes:
	 * - Only one [[ConsensusParticipant]] instance should exist per participant in the cluster.
	 * - All state mutations must occur within the sequencer thread to ensure consistency.
	 * @param indexOfTheIncludingConfigChange the [[RecordIndex]] of the [[TransitionalConfigChange]] that caused this [[ConsensusParticipant]] service to join.
	 */
	class ConsensusParticipant(cluster: ClusterParticipant, storage: Storage, machine: StateMachine, indexOfTheIncludingConfigChange: RecordIndex, participantsInTheIncludingConfigChange: ListSet[ParticipantId], initialListeners: Iterable[NotificationListener]) { thisConsensusParticipant =>

		import cluster.*

		/** A [[sequencer.Capture]] of an [[AppendResult]] paired with its appendSerial. */
		private type AppendRequest = sequencer.Capture[(Int, AppendResult)]

		/** A code that identifies the kind of [[AppendResult]] */
		private type AppendOutcome = Int
		private inline val AO_SUCCESS = 1
		private inline val AO_NEEDS_EARLIER_RECORDS = 2
		private inline val AO_IS_RETIRING = 3
		private inline val AO_IS_QUIESCED = 4
		private inline val AO_IS_UNREACHABLE = 5
		private inline val AO_STALE = 7
		/** Should never happen because [[ConsensusParticipant.Leader.handleAppendResponse]] is not called in this case. */
		private inline val AO_HAS_HIGHER_TERM = 8

		/** [[PrimaryState.tryFusingRecords]]'s fusion report: The [[PrimaryState]] is not accessible or the term is stale. */
		trait FusionReport {
			/** [[PrimaryState.tryFusingRecords]]'s fusion report: The records were appended. */
			var isFused: Boolean = false
			/** [[PrimaryState.tryFusingRecords]]'s fusion report: The [[Term]] was updated. */
			var isTermUpdated: Boolean = false
			/** The snapshot was updated and the records were appended. */
			var isSnapshotUpdated: Boolean = false
			/** The snapshot is useful but the [[StateMachine]]'s commands applier is currently running. */
			var haveToWaitCommandApplier: Boolean = false
		}

		/** The index of the highest entry known to be committed according to this participant.
		 * A log record is committed once the leader that created the record has replicated it on a majority of the participants.
		 * This also commits all preceding records in the leader’s log, including records created by previous leaders.
		 * CAUTION: This variable depends on the [[PrimaryState]] (as well as external states); mutations to the [[PrimaryState]] modify its value. To ensure deterministic causal ordering relative to these mutations, a [[StatefulRole]] must only access this variable within consumers synchronously subscribed to the [[sequencer.Capture]] returned by [[sequencer.CausalFence.causalAnchor]] or [[sequencer.CausalFence.advance]]-like methods on the [[StatefulRole.primaryStateFence]]. This ensures the variable is read in synchronization with the specific PrimaryState mutation it depends on. */
		private var commitIndex: RecordIndex = 0

		/** The index of the [[CommandRecord]] with the highest index whose command was successfully applied to the [[StateMachine]] of this [[ConsensusParticipant]].
		 * CAUTION: This variable depends on the [[PrimaryState]] (as well as external states); mutations to the [[PrimaryState]] modify its value. To ensure deterministic causal ordering relative to these mutations, a [[StatefulRole]] must only access this variable within consumers synchronously subscribed to the [[sequencer.Capture]] returned by [[sequencer.CausalFence.causalAnchor]] or [[sequencer.CausalFence.advance]]-like methods on the [[StatefulRole.primaryStateFence]]. This ensures the variable is read in synchronization with the specific PrimaryState mutation it depends on. */
		private var highestAppliedCommandIndex: RecordIndex = 0

		/** The current role of this [[ConsensusParticipant]].
		 * @note The [[currentRole]] state is neither entirely derived from the [[PrimaryState]] nor orthogonal to it. They are interrelated.
		 * CAUTION: [[PrimaryState]] mutations depend on the value of this variable. Therefore, this variable value must be in sync with the [[PrimaryState]] by means of the [[StatefulRole.primaryStateFence]] game changing invariant. */
		private var currentRole: Role = new Starting(indexOfTheIncludingConfigChange, participantsInTheIncludingConfigChange)

		/** Used to chain onto prior release covenants to ensure all previous workspace releases complete before advancing the [[PrimaryState]], guaranteeing a single observable completion handle for quiescence and disposal. */
		private var workspaceReleaseCompletion: sequencer.Capture[Unit] = sequencer.Keeper(())

		/** Memorizes the latest [[Configuration]] derived by the [[StatefulRole.deriveConfigurationFrom]] method.\
		 * It is initialized by [[Starting.handleEnter]] with a synthetic [[TransitionalConfig]] before transitioning to a [[StatefulRole]] and stays defined as long as the [[currentRole]] is stateful.\
		 * CAUTION: This variable depends on the [[PrimaryState]]; mutations to the [[PrimaryState]] modify its value. To ensure deterministic causal ordering relative to these mutations, a [[StatefulRole]] must only access this variable within consumers synchronously subscribed to the [[sequencer.Capture]] returned by [[sequencer.CausalFence.causalAnchor]] or [[sequencer.CausalFence.advance]]-like methods on the [[StatefulRole.primaryStateFence]]. This ensures the variable is read in synchronization with the specific PrimaryState mutation it depends on. */
		private var latestDerivedConfig: Maybe[Configuration] = Maybe.empty

		/** Memory where the [[Role.onQuiescencePermitted]] method stores the [[ParticipantId]] of the last quiescence grantor. */
		private var quiescenceGrantor: Maybe[ParticipantId] = Maybe.empty
		/** Memory where the [[Role.onQuiescencePermitted]] method stores the [[RecordIndex]] of the last [[StableConfigChange]] for which quiescence was authorized. */
		private var indexOfStableConfigChangeForWhichQuiescenceWasPermitted: RecordIndex = 0
		/** Memorizes the token for the pending wake-up used to retry failed calls to [[permitQuiescence]]. Needed to be able to cancel the retry. */
		private var retryPermitQuiescenceWakeUpToken: Maybe[WakeUpToken] = Maybe.empty
		/** Knows the participants that are waiting for an acknowledge to the quiescence authorizations, and the corresponding [[RecordIndex]] of the [[StableConfigChange]] for which the permission granted. */
		private val nonAcknowledgedQuiescencePermissions: mutable.Map[ParticipantId, RecordIndex] = mutable.Map.empty

		/** Knows the [[LearnerProgress]]s corresponding to the participants that were excluded from the configuration and potentially have not received the appends to notice that they can leave. */
		private val retiringLearnersById: mutable.Map[ParticipantId, LearnerProgress] = mutable.Map.empty

		/** The current election round.
		 * Should be bumped whenever the part of the state of this participant that is exposed in questions to other participants (term and commitIndex as of this writing) changes.
		 * CAUTION: This variable depends on the [[PrimaryState]] (as well as external states); mutations to the [[PrimaryState]] modify its value. To ensure deterministic causal ordering relative to these mutations, a [[StatefulRole]] must only access this variable within consumers synchronously subscribed to the [[sequencer.Capture]] returned by [[sequencer.CausalFence.causalAnchor]] or [[sequencer.CausalFence.advance]]-like methods on the [[StatefulRole.primaryStateFence]]. This ensures the variable is read in synchronization with the specific PrimaryState mutation it depends on. */
		private var currentBallot: Ballot = INITIAL_BALLOT

		/** Stores the last [[StateInfo]] instance returned by [[Role.syncLocalStateInfo]]
		 * CAUTION: This variable depends on the [[PrimaryState]] (as well as external states); mutations to the [[PrimaryState]] modify its value. To ensure deterministic causal ordering relative to these mutations, a [[StatefulRole]] must only access this variable within consumers synchronously subscribed to the [[sequencer.Capture]] returned by [[sequencer.CausalFence.causalAnchor]] or [[sequencer.CausalFence.advance]]-like methods on the [[StatefulRole.primaryStateFence]]. This ensures the variable is read in synchronization with the specific PrimaryState mutation it depends on. */
		private var stateInfoExposedInLastInteraction: StateInfo = StateInfo(PRE_INIT, ER_NONE, PRE_INIT, 0, PRE_INIT, 0, 0, INITIAL_BALLOT)

		/** Memorizes the [[StateInfo]] of the other participants seen during the [[currentBallot]].
		 * The [[StateInfo.ballot]] field of contained instances should match the [[currentBallot]].
		 * When a [[StateInfo]] with a newer ballot is seen, this map is cleared before adding it.
		 * DO NOT FORGET TO call the appropriate method (like [[Role.syncLocalStateInfo]] or [[updateSeenStateInfo]]) to update this variable before reading it.
		 * CAUTION: This variable depends on the [[PrimaryState]] (as well as external states); mutations to the [[PrimaryState]] modify its value. To ensure deterministic causal ordering relative to these mutations, a [[StatefulRole]] must only access this variable within consumers synchronously subscribed to the [[sequencer.Capture]] returned by [[sequencer.CausalFence.causalAnchor]] or [[sequencer.CausalFence.advance]]-like methods on the [[StatefulRole.primaryStateFence]]. This ensures the variable is read in synchronization with the specific PrimaryState mutation it depends on.
		 * @note uses a java map to improve efficiency. // TODO consider using an array instead. But only if complexity isn't increased to much. The inefficiencies due to being a map only apply during role updates.
		 * TODO make values be [[Captor]]s of [[StateInfo]] so that received questions that include a [[StateInfo]] fulfill the howAreYou questions done by this participant. */
		private val memorizedPeersInfos: java.util.Map[ParticipantId, StateInfo] = new java.util.HashMap()

		/** CAUTION: [[PrimaryState]] mutations depend on the value of this variable. Therefore, this variable value must be in sync with the [[PrimaryState]] by means of the [[StatefulRole.primaryStateFence]] game changing invariant. */
		private var decoupledCommandsApplierCompletion: sequencer.Capture[Unit] = sequencer.Capture_unit

		private val notificationListeners: java.util.WeakHashMap[NotificationListener, None.type] = new util.WeakHashMap()

		private val participantIdComparator = new Comparator[ParticipantId] {
			private val ordering = summon[Ordering[ParticipantId]]

			override def compare(a: ParticipantId, b: ParticipantId): Int = ordering.compare(a, b)
		}

		/** The serial number of the last execution of [[StatefulRole.updateRole]]. */
		private var serialOfLastUpdateRoleExecution = 0
		private var incumbentUpdateRoleSerial = serialOfLastUpdateRoleExecution
		private var myStateInfoAtLastUpdateRoleStart: StateInfo = stateInfoExposedInLastInteraction
		private val updateRoleCoalescing = new ResultIncrementalCoalescing[Unit, sequencer.type](sequencer)

		private val coalescedHowAreYou = CoalescedQuery[(otherParticipantId: ParticipantId, stateInfo: StateInfo), StateInfo, sequencer.type](sequencer)(params =>
			params.otherParticipantId.howAreYou(params.stateInfo)
		)

		private val entryPointDelegate: Delegate = new Delegate {
			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.Capture[ResponseToClient] = {
				Trace.init(() => s"$boundParticipantId: onCommandFromClient") {
					checkWithin()
					currentRole.onCommandFromClient(command, attemptFlag).recover { e =>
						if !e.isInstanceOf[GracefullyReleased] then scribe.error(s"$boundParticipantId: Unexpected error processing client command: $command", e)
						val nextAttemptFlag = if attemptFlag == REDIRECTED then LEADERSHIP_VACATED else attemptFlag.withInternalBitsCleared
						Maybe(Unable(nextAttemptFlag, cluster.getOtherProbableParticipants))
					}
				}
			}

			override def onHowAreYou(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.Capture[StateInfo] =
				Trace.init(() => s"$boundParticipantId: onHowAreYou") {
					checkWithin()
					currentRole.onHowAreYou(inquirerId, inquirerInfo).recover {
						case _: GracefullyReleased => Maybe(currentRole.syncStatelessStateInfo())
						case _ => Maybe.empty
					}
				}

			override def onChooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.Capture[Vote[ParticipantId]] =
				Trace.init(() => s"$boundParticipantId: onChooseALeader") {
					checkWithin()
					currentRole.onChooseALeader(inquirerId, inquirerInfo).recover {
						case _: GracefullyReleased =>
							val info = currentRole.syncStatelessStateInfo()
							Maybe(currentRole.blankVote(info.currentTerm, info.ballot))
						case _ => Maybe.empty
					}
				}

			override def onAppendRecords(inquirerId: ParticipantId, inquirerTerm: Term, prevRecordIndex: RecordIndex, prevRecordTerm: Term, batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.Capture[AppendResult] =
				Trace.init(() => s"$boundParticipantId: onAppendRecords") {
					checkWithin()
					currentRole.onAppendRecords(inquirerId, inquirerTerm, prevRecordIndex, prevRecordTerm, batch, leaderCommit, termAtLeaderCommit).recover {
						case e: GracefullyReleased => appendFailedBuilder(e)
						case _ => Maybe.empty
					}
				}

			override def onInstallSnapshot(inquirerId: ParticipantId, inquirerTerm: Term, snapshot: SnapshotData[ParticipantId], batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.Capture[AppendResult] =
				Trace.init(() => s"$boundParticipantId: onInstallSnapshot") {
					checkWithin()
					currentRole.onInstallSnapshot(inquirerId, inquirerTerm, snapshot, batch, leaderCommit, termAtLeaderCommit).recover {
						case e: GracefullyReleased => appendFailedBuilder(e)
						case _ => Maybe.empty
					}
				}

			override def requestConfigChange(requestId: ConfigChangeRequestId, desiredParticipants: Set[ParticipantId], priorAnswer: Maybe[ConfigChangeResponse]): sequencer.Capture[ConfigChangeResponse] = {
				Trace.init(() => s"$boundParticipantId: requestConfigChange-$requestId") {
					checkWithin()
					currentRole.requestConfigChange(requestId, desiredParticipants, priorAnswer).recover {
						case _: GracefullyReleased => Maybe(new STOPPED(currentRole.syncStatelessStateInfo().ballot))
						case _ => Maybe.empty
					}
				}
			}

			override def onQuiescencePermitted(grantorId: ParticipantId, indexOfGrantedStableConfigChange: RecordIndex): Unit =
				Trace.init(() => s"$boundParticipantId: onQuiescencePermitted") {
					checkWithin()
					currentRole.onQuiescencePermitted(grantorId, indexOfGrantedStableConfigChange)
				}
		}

		{ // Constructor
			initialListeners.foreach(notificationListeners.put(_, None))
			cluster.setBound(entryPointDelegate)
			Trace.init(() => s"$boundParticipantId: Ctor") {
				currentRole.handleEnter(currentRole)
			}
		}

		/** @return the ordinal of the current behavior. */
		def getRoleOrdinal: RoleOrdinal = currentRole.ordinal

		/** Returns diagnostic information for this participant's current role.
		 *
		 * @note CAUTION: The returned snapshot reflects the state of the active role.
		 * To guarantee causal consistency, this method should only be invoked when
		 * the participant's sequencer runnable queue is empty. */
		def inspectRole: RoleDiagnostic = currentRole.diagnosticInfo

		/** @return a [[sequencer.Task]] that quiesces this [[ConsensusParticipant]] instance. */
		def quiesce(): sequencer.Capture[Unit] = {
			Trace.init(() => s"$boundParticipantId: quiesces") {
				sequencer.Capture_defer { () =>
					val quiesced = Quiesced(Success("This ConsensusParticipant instance was forcefully quiesced."))
					become(quiesced).asInstanceOf[Quiesced].completed
				}
			}
		}

		/** Quiesces and disposes this [[ConsensusParticipant]] instance.
		 * @return a capturer of completion. */
		def dispose(): sequencer.Capture[Unit] = {
			quiesce().andThen(
				_ => {
					notificationListeners.clear()
					cluster.removeBound()
				},
				error => scribe.error(s"The dispose of $boundParticipantId failed.", error)
			)
		}

		/** Synchronously transitions this [[ConsensusParticipant]]'s [[Role]] to the provided one if defined. */
		private def become(maybeNewRole: Maybe[Role])(using Trace.Context): Role = Trace.step("become") {
			checkWithin()
			maybeNewRole.foreach { newRole =>
				val previousRole = currentRole
				currentRole = newRole
				previousRole.handleExit()
				notifyListeners(_.onRoleLeft(previousRole.ordinal, previousRole.getCommittedTerm))
				newRole.handleEnter(previousRole)
			}
			currentRole
		}

		/** Starts a new ballot by incrementing the [[currentBallot]] and clearing the [[memorizedPeersInfos]]. */
		private inline def startNewBallot(): Unit = {
			currentBallot = currentBallot.bumped
			memorizedPeersInfos.clear()
		}

		/** Updates the [[currentBallot]] and clears the [[memorizedPeersInfos]] if it is lower than the `seenBallot`.
		 * @return true if the [[currentBallot]] was updated. */
		private def updateBallotIfLowerThan(myStateInfo: StateInfo, seenBallot: Ballot): Boolean = {
			if seenBallot laterThan myStateInfo.ballot then {
				currentBallot = seenBallot
				memorizedPeersInfos.clear()
				true
			} else false
		}

		/** Updates the [[currentBallot]] and the [[memorizedPeersInfos]] based on the bound participant's current [[StateInfo]] (which must be provided) and a seen [[StateInfo]] of another participant.
		 * Assumes that [[StateInfo.ballot]] behaves as a primary key among all the instances of [[StateInfo]] created by the same participant.
		 * @return true if either the [[currentBallot]] or the [[memorizedPeersInfos]] are updated. */
		private def updateSeenStateInfo(myCurrentStateInfo: StateInfo, seenParticipantId: ParticipantId, seenStateInfo: StateInfo): Boolean = {
			if updateBallotIfLowerThan(myCurrentStateInfo, seenStateInfo.ballot)
				|| (seenStateInfo.ballot == myCurrentStateInfo.ballot && !memorizedPeersInfos.containsKey(seenParticipantId))
			then {
				memorizedPeersInfos.put(seenParticipantId, seenStateInfo)
				true
			} else false
		}

		//// ROLE ////

		/**
		 * Abstract base class for the consensus participant behaviors at each role.
		 *
		 * Each subtype represents a different role in the consensus algorithm and implements the message handling logic specific to that role.
		 * Roles can transition to other roles based on received messages and internal logic.
		 *
		 * It also implements behavior that is common to the [[STARTING]] and [[QUIESCED]] roles.
		 *
		 * All [[Role]] methods are executed within the sequencer thread to ensure thread safety.
		 */
		private sealed abstract class Role { thisRole =>
			def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag)(using Trace.Context): sequencer.Capture[ResponseToClient]

			def onHowAreYou(inquirerId: ParticipantId, inquirerInfo: StateInfo)(using Trace.Context): sequencer.Capture[StateInfo]

			def onChooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo)(using Trace.Context): sequencer.Capture[Vote[ParticipantId]]

			def onAppendRecords(inquirerId: ParticipantId, inquirerTerm: Term, prevRecordIndex: RecordIndex, prevRecordTerm: Term, batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term)(using Trace.Context): sequencer.Capture[AppendResult]

			def onInstallSnapshot(inquirerId: ParticipantId, inquirerTerm: Term, snapshot: SnapshotData[ParticipantId], batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term)(using Trace.Context): sequencer.Capture[AppendResult]

			def requestConfigChange(requestId: ConfigChangeRequestId, desiredParticipants: Set[ParticipantId], priorAnswer: Maybe[ConfigChangeResponse])(using Trace.Context): sequencer.Capture[ConfigChangeResponse]

			/** The ordinal corresponding to this [[Role]] */
			val ordinal: RoleOrdinal
			val rank: ElectionRank

			def diagnosticInfo: RoleDiagnostic = GenericRoleDiagnostic(ordinal)

			final def blankVote(term: Term, ballot: Ballot): Vote[ParticipantId] = Vote(term, boundParticipantId, 0, 0, thisRole.rank, ballot)

			final def yieldsBlankVote(term: Term, ballot: Ballot): sequencer.Capture[Vote[ParticipantId]] = sequencer.Keeper(blankVote(term, ballot))

			/** Called by [[become]] after the previous [[Role]]'s [[Role.handleExit]] method has returned, and the [[currentRole]] variable set to this [[Role]] instance.
			 * This method is suitable to enqueue primary state updates that must happen before any updates enqueued after [[become]] returns. */
			def handleEnter(previous: Role)(using Trace.Context): Unit

			/** Called by [[become]] before transitioning to another role. */
			def handleExit()(using Trace.Context): Unit = ()

			/** Updates the derived state that is stored in the [[Role]] instance and depends on the current [[Configuration]]. Only the [[Leader]] role has such state as this writing. */
			def handleActiveConfigChange(currentPrimaryState: PrimaryState, currentConfig: Configuration, newConfig: Configuration, indexOfNewConfigChange: RecordIndex)(using Context): Unit = ()

			def getCommittedTerm: Term = PRE_INIT

			/** Synchronizes and returns the current [[StateInfo]], bumping [[currentBallot]] and clearing memorized peer info if any state component changed since the last interaction.
			 * @param maybePrimaryState the current causally anchored [[PrimaryState]], which must be defined for [[StatefulRole]]s and empty for stateless roles. */
			def syncLocalStateInfo(maybePrimaryState: Maybe[PrimaryState])(using Trace.Context): StateInfo

			/** Convenience method for stateless callers or fallback paths where no [[PrimaryState]] is held. */
			inline final def syncStatefulStateInfo(primaryState: PrimaryState)(using Trace.Context): StateInfo = syncLocalStateInfo(Maybe(primaryState))

			/** Convenience method for stateless callers or fallback paths where no [[PrimaryState]] is held. */
			inline final def syncStatelessStateInfo()(using Trace.Context): StateInfo = syncLocalStateInfo(Maybe.empty)

			final def updateLocalStateInfo(maybePrimaryState: Maybe[PrimaryState], seenParticipantId: ParticipantId, seenStateInfo: StateInfo)(using Trace.Context): StateInfo = {
				var updatedStateInfo = syncLocalStateInfo(maybePrimaryState)
				if updateSeenStateInfo(updatedStateInfo, seenParticipantId, seenStateInfo) then updatedStateInfo = syncLocalStateInfo(maybePrimaryState)
				updatedStateInfo
			}

			/** Returns a [[StateInfo]] that indicates disability to participate; and, if the returned value differs from [[stateInfoExposedInLastInteraction]], bumps the [[currentBallot]] and clears the [[memorizedPeersInfos]]. */
			protected final def buildIneligibleInfo(term: Term): StateInfo = {
				val rememberedInfo = stateInfoExposedInLastInteraction
				val newInfo =
					if rememberedInfo.tiesWith(term, thisRole.rank, PRE_INIT, 0, PRE_INIT, 0, 0) then {
						if rememberedInfo.ballot == currentBallot then rememberedInfo else StateInfo(term, thisRole.rank, PRE_INIT, 0, PRE_INIT, 0, 0, currentBallot)
					} else {
						currentBallot = currentBallot.bumped
						memorizedPeersInfos.clear()
						StateInfo(term, thisRole.rank, PRE_INIT, 0, PRE_INIT, 0, 0, currentBallot)
					}
				stateInfoExposedInLastInteraction = newInfo
				newInfo
			}

			/** Decides the vote of this participant which may require asking the other participants how they are.\
			 * @note This process updates the [[PrimaryState.currentTerm]] if a later one is seen, which may cause a [[currentRole]] update.
			 * @param primaryState0 the current local [[PrimaryState]]
			 * @param currentStateInfo the current local [[StateInfo]]
			 * @param blankVoteIfRoleChanges instructs to yield a [[blankVote]] if the [[currentRole]] is changed by other process before this process completes.
			 * @return A [[sequencer.Capture]] that yields a [[Vote]] with the chosen leader. */
			def determineMyVote(primaryState0: PrimaryState, currentStateInfo: StateInfo, blankVoteIfRoleChanges: Boolean)(using Context): sequencer.Capture[Vote[ParticipantId]]

			/** Must be called before transitioning to [[Retiring]] to handle the special case when the active [[Configuration]] in an empty [[StableConfig]].\
			 * The [[Leader]] role should start the process that authorizes others to transition to the terminal [[QUIESCED]] state.\
			 * "Vanished" means the new config has zero participants. In that case there will be no successor leader to authorize quiescence, so the current (ghost) leader must do it itself before retiring.\
			 * @param config The currently active [[Configuration]]. */
			def authorizeQuiescenceIfVanished(config: StableConfig)(using Trace.Context): Unit = ()

			def onQuiescencePermitted(grantorId: ParticipantId, indexOfGrantedStableConfigChange: RecordIndex)(using Trace.Context): Unit = {
				if indexOfGrantedStableConfigChange > indexOfStableConfigChangeForWhichQuiescenceWasPermitted then {
					quiescenceGrantor = Maybe(grantorId)
					indexOfStableConfigChangeForWhichQuiescenceWasPermitted = indexOfGrantedStableConfigChange
				}
			}
		}

		//// STATEFUL ROLE ////

		/** Partial implementation of the [[Role]]s that accesses the [[PrimaryState]] of the bound participant.
		 * @param primaryStateFence the [[CausalFence]] that must be used to ensure causal ordering of the state updates. It must be propagated to subsequent [[StatefulRole]] instances. */
		private abstract class StatefulRole(val primaryStateFence: CausalFence[PrimaryState, sequencer.type]) extends Role { thisStatefulRole =>

			private type TermRef = IntRef
			/** The default argument for the [[updateTermIfLessThan]] method's second parameter.\
			 * It is private and defined in the same class as the [[primaryStateFence]] to ensure that the contained [[Term]] variable reflects the expected value provided it is read within the synchronous part of a synchronously subscribed consumer to the [[sequencer.Capture]] returned by [[updateTermIfLessThan]]. See the game-changing-invariant in [[Doer.CausalFence]]. */
			protected final val defaultPreviousTermRef: TermRef = new TermRef(0)

			override def handleExit()(using Trace.Context): Unit = {
				if !currentRole.isInstanceOf[StatefulRole] then {
					workspaceReleaseCompletion = (for {
						_ <- workspaceReleaseCompletion // Monotonically chains onto prior release completion capturer to ensure all previous workspace releases complete before advancing this fence, guaranteeing a single observable completion handle for quiescence and disposal.
						_ <- primaryStateFence.advance(_.withWorkspaceReleased())
					} yield ()).recover {
						case _: GracefullyReleased => Maybe(())
						case _ => Maybe.empty
					}
				}
			}

			override final def getCommittedTerm: Term = primaryStateFence.committedState.map(_.currentTerm).getOrElse(PRE_INIT)

			/** Returns a [[StateInfo]] that reflects the provided [[PrimaryState]], the [[commitIndex]], the [[rank]], and the [[currentBallot]] of the bound participant; and, if the returned value differs from the one returned in the previous call (stored in [[stateInfoExposedInLastInteraction]]), bumps the [[currentBallot]] and clears the [[memorizedPeersInfos]]. */
			// TODO consider adding a parameter with the activeConfigChangeIndex to make clear that depends on it.
			override final def syncLocalStateInfo(maybePrimaryState: Maybe[PrimaryState])(using Trace.Context): StateInfo = Trace.step("syncLocalStateInfo") {
				assert(maybePrimaryState.isDefined, s"StatefulRole (${RoleOrdinal_nameOf(ordinal)}) requires a defined PrimaryState")
				val primaryState = maybePrimaryState.get
				val rememberedInfo = stateInfoExposedInLastInteraction
				val termAtCommitIndex = primaryState.getRecordTermAt(commitIndex)
				val lastRecordIndex = primaryState.firstEmptyRecordIndex - 1
				val lastRecordTerm = primaryState.getRecordTermAt(lastRecordIndex)
				val activeConfigChangeIndex = deriveConfigurationFrom(primaryState).changeIndex
				val newInfo =
					if rememberedInfo.tiesWith(primaryState.currentTerm, thisStatefulRole.rank, termAtCommitIndex, commitIndex, lastRecordTerm, lastRecordIndex, activeConfigChangeIndex) then {
						if rememberedInfo.ballot == currentBallot then rememberedInfo else StateInfo(primaryState.currentTerm, thisStatefulRole.rank, termAtCommitIndex, commitIndex, lastRecordTerm, lastRecordIndex, activeConfigChangeIndex, currentBallot)
					} else {
						currentBallot = currentBallot.bumped
						memorizedPeersInfos.clear()
						StateInfo(primaryState.currentTerm, thisStatefulRole.rank, termAtCommitIndex, commitIndex, lastRecordTerm, lastRecordIndex, activeConfigChangeIndex, currentBallot)
					}
				stateInfoExposedInLastInteraction = newInfo
				if assertionsEnabled then assert(newInfo.rank != ER_NONE)
				newInfo
			}

			override def determineMyVote(primaryState0: PrimaryState, stateInfo0a: StateInfo, blankVoteIfRoleChanges: Boolean)(using Context): sequencer.Capture[Vote[ParticipantId]] = {
				Trace.step("determineMyVote") {
					val config0 = deriveConfigurationFrom(primaryState0)
					// Yield a blank vote if this participant is not included in the current configuration.
					if !config0.isBoundIncluded then yieldsBlankVote(primaryState0.currentTerm, stateInfo0a.ballot)
					else {
						// Update the local commitIndex to the highest peer's commitIndex, provided its term at that index is consistent with the local log. If the commitIndex is updated, refresh the local StateInfo.
						val stateInfo0b = if absorbHigherCommitIndexFromPeers(primaryState0, stateInfo0a) then syncStatefulStateInfo(primaryState0) else stateInfo0a
						// Create the howAreYou questions
						val howAreYouQuestions0 = askHowOtherParticipantsAre(config0.peers, stateInfo0b, memorizedPeersInfos)
						// determine my vote based on the answers to the howAreYou questions
						for {
							howAreYouAnswers0 <- sequencer.Capture_sequenceHardyToArray(howAreYouQuestions0, true)
							primaryState1 <- {
								val highestTermSeen = IArray.unsafeFromArray(howAreYouAnswers0).foldLeftWithIndex(primaryState0.currentTerm) { (latestTermSeen, answer, _) =>
									answer match {
										case Success(info) => if info.currentTerm > latestTermSeen then info.currentTerm else latestTermSeen
										case _: Failure[StateInfo] => latestTermSeen
									}
								}
								updateTermIfLessThan(highestTermSeen) // Note that this may change the role
							}
							myVote <- {
								var stateInfo1 = currentRole.syncStatefulStateInfo(primaryState1)
								// Update the memorizedPeersInfos (by filling the missing entries with the StateInfo instances in the answers), and count the failed answers.
								val numberOfFailedAnswers = {
									IArray.unsafeFromArray(howAreYouAnswers0).foldLeftWithIndex(0) { (failedAnswersCounter, answer, participantIndex) =>
										val participantId = config0.peers(participantIndex)
										answer match {
											case Success(info) =>
												if updateSeenStateInfo(stateInfo1, participantId, info) then stateInfo1 = currentRole.syncStatefulStateInfo(primaryState1)
												failedAnswersCounter

											case _: Failure[StateInfo] =>
												if memorizedPeersInfos.containsKey(participantId) then failedAnswersCounter
												else failedAnswersCounter + 1
										}
									}
								}

								if blankVoteIfRoleChanges && (currentRole ne thisStatefulRole) then currentRole.yieldsBlankVote(primaryState1.currentTerm, stateInfo1.ballot)
								// Update the local commitIndex to the highest peer's commitIndex, provided its term at that index is consistent with the local log. If the commitIndex is updated, restart.
								else if absorbHigherCommitIndexFromPeers(primaryState1, stateInfo1) then currentRole.determineMyVote(primaryState1, currentRole.syncStatefulStateInfo(primaryState1), blankVoteIfRoleChanges)
								else {
									val config1 = deriveConfigurationFrom(primaryState1)
									// if the bound participant is included, then:
									if config1.isBoundIncluded || (currentRole.isInstanceOf[Leader] && currentRole.asInstanceOf[Leader].isGhost) then {
										// If either, the active configuration changed while waiting the responses to the howAreYou questions, or a successful answer has an obsolete ballot; then ignore this `determineMyVote` execution replacing it with a new fresh one.
										if (config1 ne config0) || memorizedPeersInfos.size + numberOfFailedAnswers < config0.peers.length then {
											Trace.trace(s"Restarting my vote determination due to ${if config1 ne config0 then s"a concurrent configuration change (${config0.changeIndex}->${config1.changeIndex})" else s"an obsolete answer, currentBallot=$currentBallot, memorizedInfosSize=${memorizedPeersInfos.size}, numberOfFailedAnswers=$numberOfFailedAnswers, numberOfRequests=${config1.peers.size}"}")
											// TODO analyze if memorizedPeersInfos should be cleared here.
											currentRole.determineMyVote(primaryState1, stateInfo1, blankVoteIfRoleChanges)
										}
										// else, decide the vote based on the `StateInfo` stored in the `memorizedPeersInfos`.
										else sequencer.Keeper(
											config1.decideMyVote(stateInfo1, memorizedPeersInfosToArray(config1))
												.fold(currentRole.blankVote(stateInfo1.currentTerm, stateInfo1.ballot))(identity)
										)
									}
									// If the bound participant is excluded, then yield a blank vote.
									else currentRole.yieldsBlankVote(primaryState1.currentTerm, stateInfo1.ballot)
								}

							}
						} yield myVote
					}
				}
			}

			/** Advances the local [[commitIndex]] as far as the peers, provided the [[Term]] of the local [[Record]] at the peer's [[StateInfo.commitIndex]] matches the peer's [[StateInfo.termAtCommitIndex]].\
			 * This advancement safety is guaranteed by Raft's Log Matching Property.
			 * @param primaryState the current [[PrimaryState]]
			 * @param stateInfo the current [[StateInfo]]. Not referenced in the body but present as parameter to require [[memorizedPeersInfos]] be up-to-date. */
			protected final def absorbHigherCommitIndexFromPeers(primaryState: PrimaryState, stateInfo: StateInfo)(using Trace.Context): Boolean = {
				if memorizedPeersInfos.isEmpty then false
				else {
					val firstEmptyRecordIndex = primaryState.firstEmptyRecordIndex
					var currentCommitIndex = commitIndex
					var absorbed = false
					val iterator = memorizedPeersInfos.values.iterator()
					while iterator.hasNext do {
						val peerStateInfo = iterator.next()
						val peerCommitIndex = peerStateInfo.commitIndex
						// Using Raft's Log Matching Property, we can safely advance our commitIndex if a peer has a higher commitIndex and our logs match up to that index.
						if firstEmptyRecordIndex > peerCommitIndex
							&& peerCommitIndex > currentCommitIndex
							&& primaryState.getRecordTermAt(peerCommitIndex) == peerStateInfo.termAtCommitIndex
						then {
							commitIndex = peerCommitIndex
							notifyListeners(_.onCommitIndexChanged(currentCommitIndex, peerCommitIndex, currentRole.ordinal, primaryState.currentTerm))
							currentCommitIndex = peerCommitIndex
							absorbed = true
						}
					}
					absorbed
				}
			}

			override def onHowAreYou(inquirerId: ParticipantId, inquirerInfo: StateInfo)(using Trace.Context): sequencer.Capture[StateInfo] = {
				for {
					primaryState1 <- updateTermIfLessThan(inquirerInfo.currentTerm) // Note that this may change the role.
					response <- {
						if currentRole ne this then currentRole.onHowAreYou(inquirerId, inquirerInfo)
						else sequencer.Keeper(updateLocalStateInfo(Maybe(primaryState1), inquirerId, inquirerInfo))
					}
				} yield response
			}

			override def onChooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo)(using Trace.Context): sequencer.Capture[Vote[ParticipantId]] = {
				val term0Ref = new TermRef(0)
				for {
					// if the term is stale, update it persistently before interacting with other participants so that they see this participant with its updated and persisted state.
					primaryState1 <- updateTermIfLessThan(inquirerInfo.currentTerm, term0Ref) // Note that this may change the role.
					myVote <- {
						if currentRole ne this then currentRole.onChooseALeader(inquirerId, inquirerInfo)
						else {
							var currentStateInfo = updateLocalStateInfo(Maybe(primaryState1), inquirerId, inquirerInfo)
							if absorbHigherCommitIndexFromPeers(primaryState1, currentStateInfo) then currentStateInfo = syncStatefulStateInfo(primaryState1)
							val config1 = deriveConfigurationFrom(primaryState1)
							if !config1.isBoundIncluded || inquirerInfo.currentTerm < primaryState1.currentTerm then {
								yieldsBlankVote(primaryState1.currentTerm, currentStateInfo.ballot)
							} else {
								val isLogUpToDate = inquirerInfo.compareCompleteness(currentStateInfo) >= 0
								if !isLogUpToDate then {
									yieldsBlankVote(primaryState1.currentTerm, currentStateInfo.ballot)
								} else {
									for {
										primaryState2 <- primaryStateFence.causalAnchor()
										vFinal <- {
											if currentRole ne this then currentRole.onChooseALeader(inquirerId, inquirerInfo)
											else if primaryState2.votedFor.isDefined && !primaryState2.votedFor.contains(inquirerId) then {
												currentRole.yieldsBlankVote(primaryState2.currentTerm, currentStateInfo.ballot)
											} else if primaryState2.votedFor.contains(inquirerId) then {
												val grantedVote = Vote(primaryState2.currentTerm, inquirerId, 1, 0, inquirerInfo.rank, currentStateInfo.ballot)
												sequencer.Keeper(grantedVote)
											} else {
												primaryStateFence.advanceIf { ps =>
													if currentRole.isInstanceOf[StatefulRole] && ps.currentTerm == inquirerInfo.currentTerm && (ps.votedFor.isEmpty || ps.votedFor.contains(inquirerId)) then {
														Maybe(ps.withTermAndVoteUpdated(inquirerInfo.currentTerm, Maybe(inquirerId)))
													} else Maybe.empty
												}.map { psSaved =>
													if psSaved.votedFor.contains(inquirerId) then {
														Vote(psSaved.currentTerm, inquirerId, 1, 0, inquirerInfo.rank, currentStateInfo.ballot)
													} else currentRole.blankVote(psSaved.currentTerm, currentStateInfo.ballot)
												}
											}
										}
									} yield vFinal
								}
							}
						}
					}
				} yield myVote
			}

			/**
			 * Handles an AppendEntries RPC from the leader, attempting to reconcile log state and apply committed [[Record]]s.
			 *
			 * This method performs the following steps:
			 *
			 *   - Rejects the request if:
			 *     - This participant is still starting or was quiesced (`ordinal < ISOLATED`).
			 *     - The leader's term is stale (`inquirerTerm < currentTerm`).
			 *     - The leader's `prevRecordIndex` does not match the term at that index locally.
			 *     - This participant state transitions to a non-receptive one while updating this participant consensus state due to a configuration change [[Record]] among the received [[Record]]s that should be committed.
			 *
			 *   - Appends new records from the leader, resolving any log conflicts, and, if the leader's term is newer than this participant's current one, also updates `currentTerm` to `inquirerTerm`.
			 *
			 *   - If the term is updated (in the previous bullet) or this participant is not yet a follower, starts the role-update process in a decoupled manner.
			 *
			 *   - Updates the [[commitIndex]] as the minimum of `leaderCommit` and the index of the last appended record.
			 *
			 *   - If this participant state haven't changed to a no receptive one ([[Quiesced]], [[Starting]] or [[Retiring]]) while waiting the application of committed [[Record]]s of the kind that update this participant consensus state (like [[TransitionalConfigChange]] and [[TransitionalConfigChange]]), then :
			 *     - Persists the updated workspace via `storage.saves`.
			 *     - On failure to persist, transitions to `Quiesced` and returns a failed result.
			 *
			 *   - Starts, in a decoupled manner, the process that silently applies committed commands to the state machine in log order.
			 *
			 * @param inquirerId ID of the leader sending the AppendEntries request
			 * @param inquirerTerm Term of the leader
			 * @param prevRecordIndex Index of the record preceding the new entries
			 * @param prevRecordTerm Term of the preceding record
			 * @param batch The batch of records to append
			 * @param leaderCommit       Commit index reported by the leader
			 * @return a [[sequencer.Capture]] yielding the [[AppendResult]] with:
			 *  - the `success` field with true if, and only if, all the following are true when the appending was processed (specifically, when this participant's `primaryStateFence` was crossed):
			 *    * the [[PrimaryState]] is valid;
			 *    * `inquirerTerm >= currentTerm`;
			 *    * the role is either ISOLATED or FOLLOWER;
			 *    * the term of the log record at `prevRecordIndex` is equal to `prevRecordTerm`;
			 *  - the `term` field with `max(inquirerTerm, currentTerm)`.
			 *  - the `roleOrdinal` field tells the current role of this participant. */
			override def onAppendRecords(inquirerId: ParticipantId, inquirerTerm: Term, prevRecordIndex: RecordIndex, prevRecordTerm: Term, batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term)(using Trace.Context): sequencer.Capture[AppendResult] = {
				Trace.trace(s"onAppendRecords($inquirerId, @$inquirerTerm, $prevRecordIndex, $prevRecordTerm, ${batch.mkString("[", ", ", "]")}, $leaderCommit, $termAtLeaderCommit) called")

				val reporterAndUpdater = new primaryStateFence.Updater[PrimaryState] with FusionReport {
					def update(primaryState0: PrimaryState): Maybe[sequencer.Mono[PrimaryState]] = {
						val currentTerm = primaryState0.currentTerm
						// If the appending is not allowed (either the inquirer term is stale, the current role isn't stateful, or the this participant is and will continue leading); then do not mutate the primary state.
						if inquirerTerm < currentTerm || (currentRole ne thisStatefulRole) || (currentRole.rank == ER_LEADING && inquirerTerm == currentTerm) then Maybe.empty
						// Else (if inquirerTerm >= currentTerm && currentRole.isInstanceOf[StatefulRole] && (currentRole.rank != ER_LEADING || inquirerTerm > currentTerm)), do the appending.
						else primaryState0.tryFusingRecords(inquirerTerm, prevRecordIndex, prevRecordTerm, batch, this)
					}
				}
				for {
					primaryState1 <- primaryStateFence.advanceIfWith(reporterAndUpdater)
					response <- {
						if currentRole ne thisStatefulRole then currentRole.onAppendRecords(inquirerId, inquirerTerm, prevRecordIndex, prevRecordTerm, batch, leaderCommit, termAtLeaderCommit)
						else handleAppendOutcome(primaryState1, inquirerId, inquirerTerm, Maybe.empty, prevRecordIndex, prevRecordTerm, batch, leaderCommit, termAtLeaderCommit, reporterAndUpdater)
					}
				} yield response
			}

			/** Handles an [[ClusterParticipant.Delegate.onInstallSnapshot]] RPC from a peer by replacing the local log and the [[StateMachine]]' state with the snapshot data; provided some conditions are met.
			 *
			 * This method performs the following steps:
			 *   - Rejects the request if the primary state is inaccessible or the leader's term is stale.
			 *   - Updates the [[PrimaryState]] by truncating the [[Workspace]]' log, updating the snapshot and current term, and appending the provided records after the snapshot point.
			 *   - Persists the updated [[PrimaryState]].
			 *   - Installs the snapshot into the state machine.
			 *   - Updates the `commitIndex`, and `highestAppliedCommandIndex`.
			 *   - Derives the configuration from the updated [[PrimaryState]] and [[commitIndex]]
			 *   - Updates the role accordingly.
			 * If the [[StateMachine]]'s commands applier is running, then waits the applier to finish before doing anything other than updating the [[PrimaryState.currentTerm]] with is updated immediately without wait.
			 */
			override def onInstallSnapshot(inquirerId: ParticipantId, inquirerTerm: Term, snapshot: SnapshotData[ParticipantId], batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term)(using Trace.Context): sequencer.Capture[AppendResult] = {
				Trace.trace(s"onInstallSnapshot(inquirerId=$inquirerId, inquirerTerm=$inquirerTerm, $snapshot, ${batch.mkString("[", ", ", "]")}, leaderCommit=$leaderCommit, termAtCommitIndex=$termAtLeaderCommit) called")
				val updater = new primaryStateFence.Updater[PrimaryState] with FusionReport {
					def update(primaryState0: PrimaryState): Maybe[sequencer.Mono[PrimaryState]] = {
						val currentTerm = primaryState0.currentTerm
						val currentRole = thisConsensusParticipant.currentRole
						// If the appending is not allowed (either the inquirer term is stale, the role changed, or the this participant is and will continue leading); then do not mutate the primary state.
						if inquirerTerm < currentTerm || (currentRole ne thisStatefulRole) || (currentRole.rank == ER_LEADING && inquirerTerm == currentTerm) then Maybe.empty
						// If the received snapshot is older than what we already have in the local log, then fusion the batch records only.
						else if snapshot.lastIncludedRecordIndex < commitIndex || (
							snapshot.lastIncludedRecordIndex < primaryState0.firstEmptyRecordIndex
								&& primaryState0.getRecordTermAt(snapshot.lastIncludedRecordIndex) == snapshot.lastIncludedRecordTerm
							) then primaryState0.tryFusingRecords(inquirerTerm, snapshot.lastIncludedRecordIndex, snapshot.lastIncludedRecordTerm, batch, this)
						// The following `if` breaks determinism unless the commands applier completion is externally synchronized with the primary state mutations.
						// If the snapshot is useful but the commands applier is running:
						else if decoupledCommandsApplierCompletion.isPending then {
							haveToWaitCommandApplier = true
							if inquirerTerm > currentTerm then {
								isTermUpdated = true
								Maybe(primaryState0.withTermUpdated(inquirerTerm))
							} else Maybe.empty
						}
						// Else, update the snapshot, truncate the log, update the term, and append the tail records.
						else {
							isSnapshotUpdated = true
							isFused = true
							if inquirerTerm > currentTerm then isTermUpdated = true
							Maybe(primaryState0.withLogReplaced(inquirerTerm, snapshot, batch))
						}
					}
				}
				for {
					primaryState1 <- primaryStateFence.advanceIfWith(updater)
					response <- {
						if currentRole ne thisStatefulRole then currentRole.onInstallSnapshot(inquirerId, inquirerTerm, snapshot, batch, leaderCommit, termAtLeaderCommit)
						else handleAppendOutcome(primaryState1, inquirerId, inquirerTerm, Maybe(snapshot), snapshot.lastIncludedRecordIndex, snapshot.lastIncludedRecordTerm, batch, leaderCommit, termAtLeaderCommit, updater)
					}
				} yield response
			}

			/** Finalizes the participant's state and formulates the response after either [[onAppendRecords]] or [[onInstallSnapshot]] has been invoked.
			 *
			 * This method synchronizes the derived state (role, commit index, active configuration) with the outcome of a primary state mutation (log fusion or snapshot installation) and handles any necessary side effects like triggering the command applier or role transitions.
			 *
			 * Specifically, it performs the following:
			 *  - Updates the local `commitIndex` based on the leader's commit and the local log's first empty index.
			 *  - Updates the current role (e.g., becoming [[Follower]], [[Isolated]], or [[Retiring]]) based on the new configuration and term.
			 *  - Notifies listeners of commit index changes and starts the [[decoupledCommandsApplier]] if needed.
			 *  - If the mutation was rejected or deferred (e.g., waiting for the command applier), it schedules a retry or calculates a "smart rejection" hint (`successOrIndexForNextAttempt`) to help the leader reach a verifiable point in the log.
			 *
			 * @param primaryState1    The [[PrimaryState]] resulting from the mutation attempt.
			 * @param inquirerId       The identifier of the participant that initiated the replication.
			 * @param inquirerTerm     The term of the inquirer.
			 * @param maybeSnapshot    The snapshot data received, if any (only provided during snapshot installation).
			 * @param prevRecordIndex  The index of the record immediately before the received batch.
			 * @param prevRecordTerm   The term of the record at `prevRecordIndex`.
			 * @param batch            The batch of records received.
			 * @param leaderCommit     The highest commit index known by the leader.
			 * @param termAtLeaderCommit The term of the record at the leader's commit index.
			 * @param fusionReport     A report describing what happened during the primary state update.
			 * @return A [[sequencer.Capture]] yielding the [[AppendResult]] to be sent back to the leader. */
			private def handleAppendOutcome(primaryState1: PrimaryState, inquirerId: ParticipantId, inquirerTerm: Term, maybeSnapshot: Maybe[SnapshotData[ParticipantId]], prevRecordIndex: RecordIndex, prevRecordTerm: Term, batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term, fusionReport: FusionReport)(using Trace.Context): sequencer.Capture[AppendResult] = {
				assert(primaryStateFence.committedState.is(primaryState1))
				// If the term was updated and the current role is sensible to term updates, then update the role.
				if fusionReport.isTermUpdated then this.onTermUpdated(primaryState1, Maybe(inquirerId))
				// If the local snapshot was updated or a record was fused.
				if fusionReport.isFused then {
					val previousCommitIndex = commitIndex
					val newCommitIndex = if leaderCommit < primaryState1.firstEmptyRecordIndex then leaderCommit else primaryState1.firstEmptyRecordIndex - 1
					// If the commitIndex is bumped, update it.
					if newCommitIndex > previousCommitIndex then commitIndex = newCommitIndex
					// Derive the active configuration from the updated primary state and commitIndex.
					val config1 = deriveConfigurationFrom(primaryState1)
					// Update the currentRole:
					val cro = currentRole.ordinal
					// If not joining or the catching-up is complete then:
					if cro != JOINING || (primaryState1.firstEmptyRecordIndex > currentRole.asInstanceOf[Joining].indexOfTheIncludingConfigChange) then {
						// If this participant belongs to the active configuration, then become a Follower or Isolated, depending on whether the inquirer belongs to the active configuration or not.
						if config1.isBoundIncluded then {
							// Become follower of the inquirer if it belongs to the active configuration.
							if config1.peers.contains(inquirerId) then become(Follower(primaryState1.currentTerm, inquirerId, primaryStateFence))
							// Become isolated if this participant is joining, the catching-up is complete, and the inquirer is not in the active configuration.
							else if cro == JOINING then become(Isolated(primaryStateFence))
							// Keep the current role otherwise.
							// Note that, if the inquirer is excluded and the current role is follower of an excluded participant, the role is not changed to isolated here because it might be following a ghost leader.
						}
						// If this participant is excluded and ...
						else config1 match {
							case stable: StableConfig => // ... the active configuration is stable, then become Retiring.
								currentRole.authorizeQuiescenceIfVanished(stable)
								become(Retiring(primaryState1.currentTerm, stable.term, stable.changeIndex, stable.electorate))

							case transitional: TransitionalConfig => // ... the active configuration is transitional, then something is wrong.
								illegalStateQuiesce(s"$inquirerId=$inquirerId, inquirerTerm=$inquirerTerm, prevRecordIndex=$prevRecordIndex, batch=$batch, leaderCommit=$leaderCommit, termAtLeaderCommit=$termAtLeaderCommit, fusionReport=$fusionReport")
							// if currentRole.ordinal != JOINING || accessible1.firstEmptyRecordIndex > currentRole.asInstanceOf[Joining].indexOfTheIncludingConfigChange then become(Isolated(primaryStateFence))
						}
					}
					if newCommitIndex > previousCommitIndex then {
						// Notify the commitIndex bump.
						notifyListeners(_.onCommitIndexChanged(previousCommitIndex, newCommitIndex, currentRole.ordinal, primaryState1.currentTerm))
						// Start the "apply committed commands" process if it isn't already started.
						if decoupledCommandsApplierCompletion.isCompleted && currentRole.isInstanceOf[StatefulRole] then startApplyingCommittedCommands(primaryState1, fusionReport.isSnapshotUpdated)
					}
					sequencer.Keeper(AppendResult_Accepted(primaryState1.currentTerm, currentRole.ordinal))
				}
				// If the snapshot was not updated nor a record was fused, then:
				else {
					// If the received snapshot is useful but not installed because the commands-applier is running, then wait it to finish and then restart. // TODO Is it necessary to signal the commands-applier to stop because whatever it is doing will be discarded?
					if fusionReport.haveToWaitCommandApplier then {
						decoupledCommandsApplierCompletion.flatMap { _ =>
							maybeSnapshot.fold(currentRole.onAppendRecords(inquirerId, inquirerTerm, prevRecordIndex, prevRecordTerm, batch, leaderCommit, termAtLeaderCommit)) { snapshot =>
								currentRole.onInstallSnapshot(inquirerId, inquirerTerm, snapshot, batch, leaderCommit, termAtLeaderCommit)
							}
						}
					}
					// Else, surely either the inquirer's term is stale, the terms at prevRecordIndex do not match, the batch fully predates the local latest snapshot, or the cluster is in an illegal state with two leaders. So, respond with a rejection asking for earlier records, pointing to the first record that is missing or we known the term does not match.
					else {
						// This point is reached if either the inquirer's term is stale, earlier records are needed, the terms at prevRecordIndex do not match, the batch fully predates the local latest snapshot, or the cluster is in an illegal state with two leaders. So, respond with a rejection asking for earlier records, pointing to the first record that is missing or we known the term does not match.
						val successOrIndexForNextAttempt: RecordIndex =
							if primaryState1.firstEmptyRecordIndex < prevRecordIndex then primaryState1.firstEmptyRecordIndex // This happens when no snapshot was received and earlier records are needed. Tell the leader to start from the local log's first empty index.
							else if prevRecordIndex + batch.length < primaryState1.logBufferOffset - 1L then primaryState1.firstEmptyRecordIndex // This happens when the batch fully predates the local latest snapshot. Suggesting firstEmptyRecordIndex helps the inquirer to jump to a verifiable point.
							else if prevRecordIndex > 0L then prevRecordIndex // This happens when the terms do not match.
							else 1L // This happens when the terms do not match at the first record of the log.
						sequencer.Keeper(AppendResult_Rejected(primaryState1.currentTerm, successOrIndexForNextAttempt, currentRole.ordinal))
					}
				}
			}


			/** Applies committed [[CommandRecord]]s silently and in a decoupled manner until reaching [[commitIndex]].\
			 * This method assumes that the [[PrimaryState]]'s log is not truncated while this method is running: log truncation must wait until the [[decoupledCommandsApplierCompletion]] is fulfilled.\
			 * CAUTION: This process may synchronously advance the [[primaryStateFence]] and therefore will cause causal safety assertions like `primaryState0 eq primaryStateFence.committedState` to fail. This problem can be avoided moving the call to this method after the check line, preferably to the end of the block that uses a [[PrimaryState]] instance.
			 * @param primaryState0 the current [[PrimaryState]].
			 * @param mustInstallSnapshot instructs to reset the [[StateMachine]]' state from the latest snapshot in the current [[Workspace]]. */
			protected def startApplyingCommittedCommands(primaryState0: PrimaryState, mustInstallSnapshot: Boolean)(using Trace.Context): Unit = {
				assert(decoupledCommandsApplierCompletion.isCompleted)

				val completionCovenant = sequencer.Captor[Unit]()
				decoupledCommandsApplierCompletion = completionCovenant

				def applyBehind(primaryState1: PrimaryState): Unit = {
					applyCommittedCommands(primaryState1, Long.MaxValue, 0).triggerSync(new sequencer.MonoObserver[Unit] {
						override def onSuccess(dummy: Unit): Unit = {
							// If log compaction is needed, compact it in a decoupled way.
							if highestAppliedCommandIndex - primaryState1.logBufferOffset > logCompactionThreshold && currentRole.isInstanceOf[StatefulRole] then startLogCompaction()
							completionCovenant.captureSync(())
						}

						override def onError(e: Throwable): Unit = {
							Trace.error("Failure while applying commands to the state machine:", e) // TODO create a test that covers this fatal case
							become(Quiesced(Failure(e)))
							completionCovenant.trapSync(e)
						}
					})
				}

				/** Resets the [[StateMachine]]' state from the latest snapshot in the log, and updates the [[highestAppliedCommandIndex]] accordingly.\
				 * Also notifies about the command application to the [[PrimaryState]] observers. */
				def startFromSnapshot(snapshotData: SnapshotData[ParticipantId]): Unit = {
					val installCompletedCapture = for {
						_ <- machine.installSnapshot(snapshotData.stateMachineSnapshot)
						primaryState1 <- {
							notifyListeners(_.onCommandApplied(snapshotData.lastIncludedRecordIndex, snapshotData.lastIncludedRecordTerm))
							highestAppliedCommandIndex = snapshotData.lastIncludedRecordIndex
							primaryStateFence.causalAnchor()
						}
					} yield applyBehind(primaryState1)
					installCompletedCapture.triggerSync(new sequencer.MonoObserver[Unit] {
						override def onSuccess(a: Unit): Unit = ()

						override def onError(e: Throwable): Unit = {
							Trace.error("Failure while or after installing a snapshot to the state machine:", e) // TODO create a test that covers this fatal case
							become(Quiesced(Failure(e)))
							completionCovenant.trapSync(e)
						}
					})
				}

				// If instructed to install the latest snapshot or the `highestAppliedCommandIndex` predates the latest snapshot, then reset the state machine's state with it and start applying the commands after the snapshot point.
				if mustInstallSnapshot || primaryState0.latestSnapshot.exists(_.lastIncludedRecordIndex > highestAppliedCommandIndex) then startFromSnapshot(primaryState0.latestSnapshot.get)
				// Otherwise, start applying the commands after the highest applied one.
				else applyBehind(primaryState0)
			}

			/** Applies to the [[StateMachine]] the already committed but still not applied commands whose index is lower or equal to the provided bound.\
			 * They are applied one after the other as long as the [[currentRole]] is stateful, assuming the log isn't truncated while this method is running.
			 * @param primaryState any reference to an [[PrimaryState]] instance produced by [[primaryStateFence]]. It is not necessary it be the current, causally anchored one. It is used to read committed records, which don't mutate.
			 * @param upTo the upper inclusive bound of [[RecordIndex]] to apply, together with [[commitIndex]]. */
			def applyCommittedCommands(primaryState: PrimaryState, upTo: RecordIndex, recursionDepth: Int)(using Trace.Context): sequencer.Capture[Unit] = {
				val indexOfCommandToApply = highestAppliedCommandIndex + 1
				if indexOfCommandToApply > upTo || indexOfCommandToApply > commitIndex then sequencer.Capture_unit
				else {
					primaryState.getRecordAt(indexOfCommandToApply) match {
						case command: CommandRecord[ClientCommand] @unchecked =>
							val previousExecutionSerial = sequencer.currentExecutionSerial
							for {
								_ <- machine.applyClientCommand(indexOfCommandToApply, command.command)
								_ <- {
									notifyListeners(_.onCommandApplied(indexOfCommandToApply, command.term))
									highestAppliedCommandIndex = indexOfCommandToApply
									// It is not necessary to have an updated primary state here because committed records are never mutated and we are not mutating the primary state here. We only need to know if the current role is stateful.
									if currentRole.isInstanceOf[StatefulRole] then {
										if sequencer.currentExecutionSerial != previousExecutionSerial then applyCommittedCommands(primaryState, upTo, 0)
										else if recursionDepth < MAX_RECURSION_DEPTH then applyCommittedCommands(primaryState, upTo, recursionDepth + 1)
										else sequencer.Captor_defer(() => applyCommittedCommands(primaryState, upTo, 0))
									} else sequencer.Capture_unit
								}
							} yield ()
						case _ =>
							highestAppliedCommandIndex = indexOfCommandToApply
							applyCommittedCommands(primaryState, upTo, recursionDepth + 1)
					}
				}
			}

			/** Starts a process that compacts the log.
			 * Assumes the [[machine]] supports calls to [[StateMachine.applyClientCommand]] while [[StateMachine.takeSnapshot]] is running.
			 * @return a [[sequencer.Capture]] that yields the current [[PrimaryState]] with the log truncated. */
			protected final def startLogCompaction()(using Trace.Context): Unit = {
				// Decouple the execution to avoid synchronous mutations of the primary state which would violating the "decoupled mutation contract".
				sequencer.run {
					val completionCapture = for {
						primaryState0 <- primaryStateFence.causalAnchor() // This anchor is necessary because highestAppliedCommandIndex depends is derived from the primary state.
						lastIncludedIndex = highestAppliedCommandIndex - logRetentionAfterSnapshot.min(logCompactionThreshold)
						snapshot <- {
							Trace.trace(s"$boundParticipantId: Starting log compaction up to index $lastIncludedIndex at term ${primaryState0.currentTerm}.")
							machine.takeSnapshot()
						}
						primaryState2 <- primaryStateFence.advance(primaryState1 => primaryState1.withLogTruncated(primaryState1.currentTerm, lastIncludedIndex, snapshot))
					} yield Trace.trace(s"$boundParticipantId: Compacted log up to index $lastIncludedIndex at term ${primaryState2.currentTerm}.")
					completionCapture.triggerSyncCallbacks(
						_ => (),
						e => {
							Trace.error(s"$boundParticipantId: Fatal error during log compaction:", e)
							become(Quiesced(Failure(e)))
						}
					)
				}
			}

			/** @inheritdoc
			 * Wait in line for the [[PrimaryState]] and then delegate the request to the concrete stateful role. */
			override final def requestConfigChange(requestId: ConfigChangeRequestId, desiredParticipants: Set[ParticipantId], priorAnswer: Maybe[ConfigChangeResponse])(using Trace.Context): sequencer.Capture[ConfigChangeResponse] = {
				Trace.trace(s"Handling change to $desiredParticipants, priorAnswer=$priorAnswer.")
				for {
					primaryState <- primaryStateFence.causalAnchor()
					response <- {
						// If a prior answer is provided, update the ballot and memorizedPeersInfos
						val ballotWasUpdated = priorAnswer.fold(false) {
							case nonTerminal: NonTerminalConfigChangeResponse =>
								updateBallotIfLowerThan(currentRole.syncStatefulStateInfo(primaryState), nonTerminal.latestBallotSeen)
							case _: TerminalConfigChangeResponse => false
						}
						// Delegate the request to the concrete stateful role.
						currentRole match {
							case stateful: StatefulRole =>
								stateful.requestConfigChange(primaryState, requestId, desiredParticipants, ballotWasUpdated)
							case stateless =>
								stateless.requestConfigChange(requestId, desiredParticipants, priorAnswer)
						}
					}
				} yield {
					Trace.trace(s"response: $response")
					response
				}
			}

			def requestConfigChange(primaryState: PrimaryState, requestId: ConfigChangeRequestId, desiredParticipants: Set[ParticipantId], ballotWasUpdated: Boolean)(using Context): sequencer.Capture[ConfigChangeResponse]

			//// Role updaters

			/** Starts a process that updates the [[currentRole]] and [[PrimaryState.currentTerm]] based on the [[StateInfo]]s returned by calling [[ClusterParticipant.howAreYou]] on the other participants and, if necessary, also based on the [[Vote]]s returned by calling [[ClusterParticipant.chooseALeader]] on them.
			 * This process always ends immediately after a call to [[become]] returns. So, its [[Role]] outcome can be seen in the [[currentRole]] derived state variable.
			 * The [[currentRole]] is updated only if the desired one if not equivalent to the [[currentRole]]. If updated, any other in-flight [[updateRole]] process is canceled and immediately completed.
			 * Concurrent executions of this method return the same result. */
			def updateRole()(using Trace.Context): sequencer.Capture[Unit] = {
				if assertionsEnabled then assert(currentRole eq this)
				for {
					primaryState <- primaryStateFence.causalAnchor()
					_ <- {
						if currentRole ne this then sequencer.Capture_unit
						else updateRole(primaryState)
					}
				} yield ()
			}


			/** Like [[updateRole]] but already knowing the current [[PrimaryState]].
			 * TODO rely on a heartbeat that bypasses the [[primaryStateFence]] to demote a leader. It the response the the heartbeat should contain a StateInfo to allow discarding false positives (when the follower persistence is silently stuck). */
			def updateRole(primaryState0: PrimaryState)(using Context): sequencer.Capture[Unit] = {
				checkWithin()
				if assertionsEnabled then assert(currentRole eq this)

				// Identify this execution.
				serialOfLastUpdateRoleExecution += 1
				val serial = serialOfLastUpdateRoleExecution

				Trace.step(() => s"updateRole#$serial") {

					/** Checks if this specific execution of `updateRole` has become obsolete and must be aborted. This condition is met if the participant has transitioned to a different role or if a newer concurrent execution of `updateRole` has started. */
					inline def haveToAbort: Boolean = (currentRole ne this) || incumbentUpdateRoleSerial != serial

					/** Role decision logic when my vote is for a peer and got the [[StateInfo]] of a majority. */
					def whenVotingAnother(currentState: PrimaryState, vote: Vote[ParticipantId]): sequencer.Capture[Unit] = {
						if vote.votedRank == ER_LEADING then {
							become(Follower(currentState.currentTerm, vote.votedId, primaryStateFence))
							sequencer.Capture_unit
						}
						// If the voted participant isn't retiring, become Isolated.
						else if vote.votedRank != ER_RETIREE then {
							become(Isolated(primaryStateFence))
							sequencer.Capture_unit
						}
						// If the voted participant is Retiring, then:
						else {
							val votedStateInfo = memorizedPeersInfos.get(vote.votedId)
							// If a retiree wins the election, active candidates would get stuck indefinitely, as the retiree will never become Leader to advance their commitIndex via AppendEntries.
							// To unstick the cluster, we attempt to safely absorb the retiree's commitIndex out-of-band using Raft's Log Matching Property: "If two entries in different logs have the same index and term, the logs are identical in all preceding entries."
							// By verifying our local log has the exact same term at the retiree's commitIndex, we mathematically prove our log holds all the entries the retiree knew to be committed, making it unequivocally safe to advance our own commitIndex.
							if currentState.firstEmptyRecordIndex > votedStateInfo.commitIndex
								&& currentState.getRecordTermAt(votedStateInfo.commitIndex) == votedStateInfo.termAtCommitIndex
								&& votedStateInfo.commitIndex > commitIndex
							then {
								commitIndex = votedStateInfo.commitIndex
								updateRole(currentState)
							} else {
								become(Isolated(primaryStateFence))
								sequencer.Capture_unit
							}
						}
					}

					/** Continue the role update process assuming my vote is non-blank. */
					def updateRoleKnowingMyNonBlankVote(currentState2: PrimaryState, config2: Configuration, myVote2: Vote[ParticipantId]): sequencer.Capture[Unit] = {
						if assertionsEnabled then assert(myVote2.term == currentState2.currentTerm)

						// If excluded and not leading as ghost, then retire immediately.
						if !config2.isBoundIncluded && !this.isInstanceOf[Leader] then {
							this.authorizeQuiescenceIfVanished(config2.asInstanceOf[StableConfig]) // The downcast is safe because exclusion is checked every record and transitional configurations are never more restrictive than the contiguous stable ones.
							become(Retiring(currentState2.currentTerm, config2.term, config2.changeIndex, config2.electorate))
							sequencer.Capture_unit
						}
						// else, if got the StateInfo of a majority of the active participants, then:
						else if config2.reachedAMajority(myVote2) then {
							// If my vote is for other participant, become follower or isolated depending on the other is leading or not.
							if myVote2.votedId != boundParticipantId then whenVotingAnother(currentState2, myVote2)
							// If my vote is for myself and I am leading, abort the role update.
							else if this.ordinal >= PROMOTING then sequencer.Capture_unit
							// If the vote is for myself and I am not leading, decide based on everyone’s votes.
							else {
								val highestTermSeen = {
									val iterator = memorizedPeersInfos.values.iterator()
									var maxT = currentState2.currentTerm
									while iterator.hasNext do {
										val t = iterator.next().currentTerm
										if t > maxT then maxT = t
									}
									maxT
								}
								val targetTerm = highestTermSeen.incremented
								for {
									primaryStateBumped <- primaryStateFence.advanceIf { (ps0: PrimaryState) =>
										if currentRole ne thisStatefulRole then Maybe.empty
										else Maybe(ps0.withTermAndVoteUpdated(targetTerm, Maybe(boundParticipantId)))
									}
									_ <- {
										if (currentRole ne thisStatefulRole) || primaryStateBumped.currentTerm < targetTerm then sequencer.Capture_unit
										else {
											val myStateInfoAtChooseALeaderRequest = syncStatefulStateInfo(primaryStateBumped)
											val inquires = for replierId <- config2.peers yield replierId.chooseALeader(boundParticipantId, myStateInfoAtChooseALeaderRequest)
											for {
												replies <- sequencer.Capture_sequenceHardyToArray(inquires, true)
												primaryState3 <- {
													val latestTermSeen = IArray.unsafeFromArray(replies).foldLeftWithIndex(primaryStateBumped.currentTerm)((latestTermSeen, reply, _) => reply match {
														case Success(replierVote) => if replierVote.term > latestTermSeen then replierVote.term else latestTermSeen
														case _: Failure[Vote[ParticipantId]] => latestTermSeen
													})
													Trace.trace(s"Replied votes=${replies.zip(config2.peers).mkString("[", ", ", "]")}, latestTermSeen=$latestTermSeen, myVote=$myVote2") // TODO delete line
													updateTermIfLessThan(latestTermSeen) // Note that this may change the role.
												}
												_ <- {
													if haveToAbort then sequencer.Capture_unit
													else {
														// If the term was bumped (while waiting the votes from the other participants or due to a higher term seen in them), then the role update is responsibility of `StatefulRole.onTermBumped`; so abort this update. Restarting the role update here might collide with role changes caused by the bump.
														if primaryState3.currentTerm > primaryStateBumped.currentTerm then {
															if assertionsEnabled then assert(this.ordinal < PROMOTING) // because while leading the term should never change.
															sequencer.Capture_unit
														} else {
															val myStateInfo3 = syncStatefulStateInfo(primaryState3)
															val highestBallotSeenInVotes = IArray.unsafeFromArray(replies).foldLeftWithIndex(myStateInfo3.ballot)((highestBallot, reply, _) => reply match {
																case Success(replierVote) => if replierVote.ballot laterThan highestBallot then replierVote.ballot else highestBallot
																case _: Failure[Vote[ParticipantId]] => highestBallot
															})
															val aHigherBallotHaveBeenSeenInVotes = updateBallotIfLowerThan(myStateInfo3, highestBallotSeenInVotes)
															if aHigherBallotHaveBeenSeenInVotes || myStateInfo3.ballot != myStateInfoAtChooseALeaderRequest.ballot then {
																// TODO consider the inclusion of the StateInfo in Vote in order to keep the StateInfo instances with the highest ballot seen. This would save howAreYou calls to participants for which the StateInfo in the Vote already corresponds to the new ballot. Note that this safe would occur only when restarting the role update due to a higher ballot seen in votes.
																Trace.trace(s"Restarting due ${if aHigherBallotHaveBeenSeenInVotes then "a higher ballot seen in votes" else "to a ballot bump"}.")
																updateRole(primaryState3)
															} else {
																val config3 = deriveConfigurationFrom(primaryState3)
																// TODO make decideMyVote support the commitIndex-auto-bump like the `updateRoleOmnisciently` if possible
																val myVote3 = config3.decideMyVote(syncStatefulStateInfo(primaryState3), memorizedPeersInfosToArray(config3))
																	.fold(blankVote(myStateInfo3.currentTerm, myStateInfo3.ballot))(identity)
																become(config3.determineRole(primaryState3, primaryStateFence, myVote3, replies))
																sequencer.Capture_unit
															}
														}
													}
												}
											} yield ()
										}
									}
								} yield ()
							}
						}
						// else (if the successful answers to the howAreYou RPC are not a majority)
						else {
							become(Isolated(primaryStateFence))
							sequencer.Capture_unit
						}
					}

					/** Continue the role update process by treating blank vote cases. */
					def updateRoleKnowingMyVote(primaryState2: PrimaryState, myVote: Vote[ParticipantId]): sequencer.Capture[Unit] = {
						val config2 = deriveConfigurationFrom(primaryState2)

						// If my vote is blank, then:
						if myVote.isBlank then {
							// if we are included, then:
							if config2.isBoundIncluded then {
								// Advance our commitIndex by absorbing it from a more complete peer and, if successful, restart the role update. This is necessary again here because to handle the situation when a concurrent RPC (such as onHowAreYou or onChooseALeader from another peer) updates memorizedPeersInfos with a higher commit index after determineMyVote has returned but before primaryState2 is causally anchored.
								if absorbHigherCommitIndexFromPeers(primaryState2, syncStatefulStateInfo(primaryState2)) then updateRole(primaryState2)
								// else become Isolated.
								else {
									become(Isolated(primaryStateFence))
									sequencer.Capture_unit
								}
							}
							// if we are not included, the become Retiring.
							else {
								if assertionsEnabled then assert(config2.isInstanceOf[StableConfig]) // because exclusion is checked every record and transitional configurations are never more restrictive than the contiguous stable ones.
								become(Retiring(primaryState2.currentTerm, config2.term, config2.changeIndex, config2.electorate))
								sequencer.Capture_unit
							}
						}
						// if my vote is non-blank...
						else {
							val stateInfo2 = syncStatefulStateInfo(primaryState2)
							// ... and no StateInfo has changed, continue the role update knowing the vote is non blank.
							if stateInfo2.ballot == myVote.ballot then updateRoleKnowingMyNonBlankVote(primaryState2, config2, myVote)
							// else start the role process again (superseding this execution).
							else {
								Trace.trace(s"Restarting due to a ballot bump: currentBallot=${stateInfo2.ballot}, myVote.ballot=${myVote.ballot}")
								updateRole(primaryState2)
							}
						}
					}

					/** Starts a role update process by deciding the local vote. */
					def start(primaryState1: PrimaryState): sequencer.Capture[Unit] = {
						incumbentUpdateRoleSerial = serial
						memorizedPeersInfos.clear()
						if currentRole ne this then sequencer.Capture_unit
						else {
							val myStateInfo1 = syncStatefulStateInfo(primaryState1)
							for {
								myVote <- determineMyVote(primaryState1, myStateInfo1, true)
								_ <- {
									if haveToAbort then sequencer.Capture_unit
									else for {
										primaryState2 <- primaryStateFence.causalAnchor()
										_ <- {
											if haveToAbort then sequencer.Capture_unit
											else updateRoleKnowingMyVote(primaryState2, myVote)
										}
									} yield ()
								}
							} yield ()
						}
					}

					// Coalesce the result of concurrent calls either, superseding ongoing executions started with obsolete StateInfo, or merging to the execution started with the same StateInfo.
					val updateCovenant = updateRoleCoalescing.contend(true) {
						maybePreviousUpdateRoleExecution =>
							val myCurrentStateInfo = syncStatefulStateInfo(primaryState0)
							maybePreviousUpdateRoleExecution.fold {
								myStateInfoAtLastUpdateRoleStart = myCurrentStateInfo
								Trace.trace(s"No concurrent execution. StateInfo=$myCurrentStateInfo")
								start(primaryState0)
							} { previousUpdateRoleExecution =>
								if myCurrentStateInfo == myStateInfoAtLastUpdateRoleStart then {
									Trace.trace(s"Merging with incumbent execution. StateInfo=$myCurrentStateInfo")
									previousUpdateRoleExecution
								}
								else {
									Trace.trace(s"Superseding incumbent execution due to StateInfo change: old:$myStateInfoAtLastUpdateRoleStart, new=$myCurrentStateInfo")
									myStateInfoAtLastUpdateRoleStart = myCurrentStateInfo
									start(primaryState0)
								}
							}
					}
					updateCovenant.andThen(
						_ => Trace.trace(s"Execution #$serial ended"),
						{
							case _: GracefullyReleased => Trace.trace(s"Execution #$serial canceled because the workspace was gracefully released.")
							case error => Trace.error(s"Execution #$serial failed unexpectedly.", error)
						}
					)
				}
			}

			/** Updates the [[Role]] of this [[ConsensusParticipant]] and then returns the [[sequencer.Capture]] returned by the [[Role.onCommandFromClient]] method applied to the updated [[Role]].
			 * @return a [[sequencer.Capture]] returned by [[Role.onCommandFromClient]] applied to the updated [[Role]] */
			final def updateRoleAndThenCallsOnCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag)(using Context): sequencer.Capture[ResponseToClient] = {
				Trace.step("updateRoleAndThenCallsOnCommandFromClient") {
					Trace.trace(s"Current role=${RoleOrdinal_nameOf(ordinal)}, attemptFlag=$attemptFlag, memorizedInfos=$memorizedPeersInfos.")
					if !attemptFlag.isInternalVacateHandoff && attemptFlag != FIRST_ATTEMPT then startNewBallot() // TODO this ballot bump may cause unnecessary "determineVote" restarts that may never converge when many clients call concurrently. The ballot should be bumped only if it is equal to the ballot used by the previous participant. So, the ballot should be included in the data propagated through the client to the next participant.
					for {
						_ <- updateRole()
						result <- {
							if currentRole.ordinal >= FOLLOWER then currentRole.onCommandFromClient(command, FIRST_ATTEMPT)
							else {
								val nextAttemptFlag = if attemptFlag == REDIRECTED then LEADERSHIP_VACATED else attemptFlag.withInternalBitsCleared
								currentRole match {
									case stateful: StatefulRole =>
										for primaryState <- stateful.primaryStateFence.causalAnchor() yield Unable(nextAttemptFlag, deriveConfigurationFrom(primaryState).otherProbableParticipants)

									case retiring: Retiring =>
										sequencer.Keeper(Unable(
											nextAttemptFlag,
											ListSet.newBuilder[ParticipantId].addAll(retiring.excludingConfigElectorate).addAll(cluster.getOtherProbableParticipants).result()
										))
									case stateless =>
										sequencer.Keeper(Unable(nextAttemptFlag, cluster.getOtherProbableParticipants))
								}
							}

						}
					} yield result
				}
			}


			/** Derives the active [[Configuration]] state from the current [[PrimaryState]] and [[commitIndex]].
			 * Depends on, and updates, the [[latestDerivedConfig]]. Also updates other derived state.
			 *
			 * CAUTION: the provided [[PrimaryState]] instance must be the current one. So, this method must be called only within the synchronous part of consumers subscribed synchronously to the [[sequencer.Capture]] returned by either [[sequencer.CausalFence.advance]]-like or [[sequencer.CausalFence.causalAnchor]] methods, passing the [[PrimaryState]] provided to the consumer. This requirement is needed because this method's side effects update derived state.
			 *  @note Accessing the current [[Configuration]] through this method ensures that the current [[Configuration]] is updated before any other derived-state update that depend on it.
			 * @param currentPrimaryState the current [[PrimaryState]].
			 * @return a [[Configuration]] derived from the provided [[PrimaryState]]. */
			def deriveConfigurationFrom(currentPrimaryState: PrimaryState)(using Context): Configuration = {
				assert(primaryStateFence.committedState.is(currentPrimaryState)) // Fails if the primaryStateFence was touched after yielding the PrimaryState instance received as parameter.

				val indexOfLatestConfigChange = currentPrimaryState.indexOfLatestConfigChange
				if indexOfLatestConfigChange == 0 then latestDerivedConfig.get
				else {
					val oldConfig = latestDerivedConfig.get
					val activeConfigChange: ConfigChange[ParticipantId] | Null = currentPrimaryState.latestConfigChange.get match {
						case stable: StableConfigChange[ParticipantId] @unchecked =>
							if commitIndex >= indexOfLatestConfigChange then stable
							else if stable.isCoupleOf(oldConfig.backingConfigChange) then null // null indicates to keep the current configuration as the active one.
							else stable.recreateCouple

						case transitional: TransitionalConfigChange[ParticipantId] @unchecked =>
							transitional
					}
					if activeConfigChange == null || activeConfigChange == oldConfig.backingConfigChange then oldConfig
					else {
						val newConfig = Configuration_from(activeConfigChange, indexOfLatestConfigChange)
						// Update the derived state stored in the `currentRole` instance. Only the Leader role has such state as of this writing.
						currentRole.handleActiveConfigChange(currentPrimaryState, oldConfig, newConfig, indexOfLatestConfigChange)
						latestDerivedConfig = Maybe(newConfig)
						// Inform the cluster service and notify the listeners about the configuration change.
						cluster.onActiveConfigChanged(activeConfigChange, indexOfLatestConfigChange, currentRole.ordinal)
						notifyListeners(_.onActiveConfigChanged(currentRole.ordinal, currentPrimaryState.currentTerm, indexOfLatestConfigChange, activeConfigChange))
						newConfig
					}
				}
			}

			/** Queues an updater of the [[PrimaryState.currentTerm]] that does the following:
			 * - updates the [[PrimaryState.currentTerm]] if the provided [[Term]] is higher than it at the moment the updater is executed.
			 * - if the role is sensible to term updates, the [[currentRole]] is changed.
			 * @param seenTerm the [[Term]] to update the [[PrimaryState]] with, provided it is higher than the [[PrimaryState.currentTerm]] when the queued updater is executed.
			 * @param previousTermRef the [[Term]] value in this reference object is overwritten with the [[PrimaryState.currentTerm]] corresponding to the [[PrimaryState]] before the causally anchored advance is performed.
			 * @note About the safety of reusing the same [[TermRef]] instance for different calls: The value is guaranteed to reflect the expected value provided it is read within the synchronous part of a synchronously subscribed consumer to the [[sequencer.Capture]] returned by [[updateTermIfLessThan]]. See the game-changing-invariant in [[Doer.CausalFence]]. */
			protected def updateTermIfLessThan(seenTerm: Term, previousTermRef: TermRef = defaultPreviousTermRef)(using Trace.Context): sequencer.Capture[PrimaryState] =
				Trace.step("updateTermIfLessThan") {
					for primaryState1 <- primaryStateFence.advanceIf { (primaryState0: PrimaryState) =>
						previousTermRef.elem = primaryState0.currentTerm
						if currentRole.isInstanceOf[StatefulRole] && seenTerm > primaryState0.currentTerm then Maybe(primaryState0.withTermUpdated(seenTerm))
						else Maybe.empty
					} yield {
						if primaryState1.currentTerm <= previousTermRef.elem then primaryState1
						else currentRole match {
							case stateful: StatefulRole => stateful.onTermUpdated(primaryState1, Maybe.empty)
							case _ => primaryState1
						}
					}
				}

			private def memorizedPeersInfosToArray(currentConfig: Configuration): IArray[StateInfo] = {
				currentConfig.peers.mapWithIndex { (peerId, _) => memorizedPeersInfos.get(peerId) }
			}

			/** Called by [[updateTermIfLessThan]] and [[onInstallSnapshot]] when the [[PrimaryState.currentTerm]] is updated because a higher term was observed.\
			 * @param primaryState the current [[PrimaryState]]
			 * @param maybeLeaderId the identifier of the leader, if known. */
			def onTermUpdated(primaryState: PrimaryState, maybeLeaderId: Maybe[ParticipantId])(using Context): PrimaryState = primaryState
		}

		//// QUIESCED ////

		/** A terminal [[Role]] that indicates this [[ConsensusParticipant]] service will be ready to be disposed after all the in-flight RPC calls it did have been heard.
		 *
		 * Taken when either:
		 *		- the bound participant has been excluded from the participant and completed its retirement.
		 *		- this [[ConsensusParticipant]] service was forcibly quiesced by executing the [[sequencer.Task]] returned by the [[quiesces]] method.
		 *		- an ineludible failure occurred. */
		private final class Quiesced(val motive: Try[String]) extends Role {
			override val ordinal: RoleOrdinal = QUIESCED
			override val rank: ElectionRank = ElectionRank_from(QUIESCED)

			/** A [[sequencer.Capture]] that is fulfilled when the all the allocated [[Workspace]]s are released. */
			def completed: sequencer.Capture[Unit] = workspaceReleaseCompletion

			override def handleEnter(previousRole: Role)(using Context): Unit = {
				Trace.step("Quiesced.onEnter") {
					notifyListeners(_.onBecameQuiesced(previousRole.ordinal, previousRole.getCommittedTerm, motive))
					retiringLearnersById.clear()
					retryPermitQuiescenceWakeUpToken.foreach(_.cancel())
					retryPermitQuiescenceWakeUpToken = Maybe.empty
					nonAcknowledgedQuiescencePermissions.clear()
					cluster.onQuiesced(motive)
				}
			}


			override def syncLocalStateInfo(maybePrimaryState: Maybe[PrimaryState])(using Context): StateInfo = {
				buildIneligibleInfo(PRE_INIT)
			}

			override def determineMyVote(primaryState0: PrimaryState, currentStateInfo: StateInfo, blankVoteIfRoleChanges: Boolean)(using Context): sequencer.Capture[Vote[ParticipantId]] =
				yieldsBlankVote(PRE_INIT, currentStateInfo.ballot)

			override def onHowAreYou(inquirerId: ParticipantId, inquirerInfo: StateInfo)(using Trace.Context): sequencer.Capture[StateInfo] = {
				sequencer.Keeper(updateLocalStateInfo(Maybe.empty, inquirerId, inquirerInfo))
			}

			override def onChooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo)(using Trace.Context): sequencer.Capture[Vote[ParticipantId]] = {
				val myStateInfo = updateLocalStateInfo(Maybe.empty, inquirerId, inquirerInfo)
				yieldsBlankVote(PRE_INIT, myStateInfo.ballot)
			}

			override def onAppendRecords(inquirerId: ParticipantId, inquirerTerm: Term, prevRecordIndex: RecordIndex, prevRecordTerm: Term, batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term)(using Trace.Context): sequencer.Capture[AppendResult] = {
				sequencer.Keeper(AppendResult_Rejected(PRE_INIT, Long.MaxValue, ordinal))
			}

			override def onInstallSnapshot(inquirerId: ParticipantId, inquirerTerm: Term, snapshot: SnapshotData[ParticipantId], batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term)(using Trace.Context): sequencer.Capture[AppendResult] = {
				sequencer.Keeper(AppendResult_Rejected(PRE_INIT, Long.MaxValue, ordinal))
			}

			/** @inheritdoc
			 * This implementation responds with a rejection that propagates the received `attemptFlag` or-ing the [[FALLBACK]] bit to alert the participant with which the client would try next.
			 * Why the [[FALLBACK]] bit? Because the behavior of a [[Quiesced]] and a non-existent participant should be similar, given [[Quiesced]] is just a transient state before becoming inexistent. */
			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag)(using Trace.Context): sequencer.Capture[ResponseToClient] = {
				sequencer.Keeper(Unable(attemptFlag.withInternalBitsCleared | FALLBACK, cluster.getOtherProbableParticipants))
			}

			override def requestConfigChange(requestId: ConfigChangeRequestId, desiredParticipantsSet: Set[ParticipantId], priorAnswer: Maybe[ConfigChangeResponse])(using Trace.Context): sequencer.Capture[ConfigChangeResponse] = {
				var myCurrentStateInfo = syncStatelessStateInfo()
				// If a prior answer is provided, update the current ballot and memorizedPeersInfos
				priorAnswer.foreach {
					case nonTerminal: NonTerminalConfigChangeResponse =>
						if updateBallotIfLowerThan(myCurrentStateInfo, nonTerminal.latestBallotSeen) then myCurrentStateInfo = syncStatelessStateInfo()
					case _: TerminalConfigChangeResponse =>
				}
				sequencer.Keeper(new STOPPED(myCurrentStateInfo.ballot))
			}
		}

		private final def Quiesced(motive: Try[String]): Maybe[Quiesced] = {
			if currentRole.ordinal == QUIESCED then Maybe.empty
			else Maybe(new Quiesced(motive))
		}

		//// RETIRING ////

		/** A transitional [[Role]] before [[Quiesced]] to which a participant transitions to when a [[StableConfigChange]] that excludes it becomes active.\
		 * The life of this [[Role]] last until a stable [[Leader]] of a subsequent [[Term]] authorizes this participant to quiesce.\
		 * This [[Role]] is part of the **Retiring Quorum-Buffering** mechanism.\
		 * The purpose of this mechanism is to maintain the quorum safety of the old participant set during joint consensus.\
		 * By holding excluded participants in the [[RETIRING]] role, the system ensures they contribute toward the old set's quorum. Although they do not cast a specific vote, they effectively lower the required threshold of active votes by one, acting as a neutral "don't care" participant until a succeeding leader establishes a stable majority in the new configuration.\
		 * Since this role must exist for that reason, we also take advantage of its presence to wait for the retirement pipelines to conclude their job. In this scenario, the job of the retirement pipelines of this retiring ex-leader will overlap with the job of the retirement pipelines of the succeeding [[Leader]], but, if I am not mistaken, this overlap is more beneficial than harmful because it removes some burden to the new [[Leader]].\
		 * @param finalTerm the [[Term]] during which this participant became [[Retiring]]. Used only as argument for the [[NotificationListener.onRetiring]] method, and [[AppendResult]] responses.
		 * @param termAtExcludingConfigIndex the [[Term]] of the [[StableConfigChange]] that excluded this participant causing its retirement. This is the term that a [[Retiring]] participant exposes in [[StateInfo]] during elections.
		 * @param excludingConfigIndex the index of the [[StableConfigChange]] that excluded this participant causing its retirement.
		 * @param excludingConfigElectorate The electorate of the [[StableConfigChange]] that excluded this participant. */
		private final class Retiring(val finalTerm: Term, val termAtExcludingConfigIndex: Term, val excludingConfigIndex: RecordIndex, val excludingConfigElectorate: IArray[ParticipantId]) extends Role { thisRetiring =>
			override val ordinal: RoleOrdinal = RETIRING
			override val rank: ElectionRank = ElectionRank_from(RETIRING)

			if assertionsEnabled then assert(!excludingConfigElectorate.contains(boundParticipantId))

			override def handleEnter(previous: Role)(using Trace.Context): Unit = {
				notifyListeners(_.onRetiring(previous.ordinal, finalTerm))
				becomeQuiescedIfEligible(excludingConfigIndex)
			}


			override def syncLocalStateInfo(maybePrimaryState: Maybe[PrimaryState])(using Context): StateInfo = {
				if stateInfoExposedInLastInteraction.tiesWith(termAtExcludingConfigIndex, rank, termAtExcludingConfigIndex, excludingConfigIndex, termAtExcludingConfigIndex, excludingConfigIndex, excludingConfigIndex) then {
					if stateInfoExposedInLastInteraction.ballot != currentBallot then stateInfoExposedInLastInteraction = StateInfo(termAtExcludingConfigIndex, rank, termAtExcludingConfigIndex, excludingConfigIndex, termAtExcludingConfigIndex, excludingConfigIndex, excludingConfigIndex, currentBallot)
				} else {
					currentBallot = currentBallot.bumped
					stateInfoExposedInLastInteraction = StateInfo(termAtExcludingConfigIndex, rank, termAtExcludingConfigIndex, excludingConfigIndex, termAtExcludingConfigIndex, excludingConfigIndex, excludingConfigIndex, currentBallot)
				}
				stateInfoExposedInLastInteraction
			}

			override def determineMyVote(primaryState0: PrimaryState, currentStateInfo: StateInfo, blankVoteIfRoleChanges: Boolean)(using Context): sequencer.Capture[Vote[ParticipantId]] = {
				yieldsBlankVote(termAtExcludingConfigIndex, currentStateInfo.ballot)
			}

			override def onHowAreYou(inquirerId: ParticipantId, inquirerInfo: StateInfo)(using Trace.Context): sequencer.Capture[StateInfo] = {
				sequencer.Keeper(updateLocalStateInfo(Maybe.empty, inquirerId, inquirerInfo))
			}

			override def onChooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo)(using Trace.Context): sequencer.Capture[Vote[ParticipantId]] = {
				val myStateInfo = updateLocalStateInfo(Maybe.empty, inquirerId, inquirerInfo)
				yieldsBlankVote(termAtExcludingConfigIndex, myStateInfo.ballot)
			}

			override def requestConfigChange(requestId: ConfigChangeRequestId, desiredParticipantsSet: Set[ParticipantId], priorAnswer: Maybe[ConfigChangeResponse])(using Trace.Context): sequencer.Capture[ConfigChangeResponse] = {
				var myCurrentStateInfo = syncStatelessStateInfo()
				// If a prior answer is provided, update the current ballot and memorizedPeersInfos
				priorAnswer.foreach {
					case nonTerminal: NonTerminalConfigChangeResponse =>
						if updateBallotIfLowerThan(myCurrentStateInfo, nonTerminal.latestBallotSeen) then myCurrentStateInfo = syncStatelessStateInfo()
					case _: TerminalConfigChangeResponse =>
				}
				sequencer.Keeper(new EXCLUDED(myCurrentStateInfo.ballot))
			}

			/** @inheritdoc
			 * This implementation responds with a rejection that propagates the received `attemptFlag`. */
			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag)(using Trace.Context): sequencer.Capture[ResponseToClient] = {
				sequencer.Keeper(Unable(
					attemptFlag.withInternalBitsCleared,
					ListSet.newBuilder[ParticipantId].addAll(excludingConfigElectorate).addAll(cluster.getOtherProbableParticipants).result()
				))
			}

			override def onAppendRecords(inquirerId: ParticipantId, inquirerTerm: Term, prevRecordIndex: RecordIndex, prevRecordTerm: Term, batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term)(using Trace.Context): sequencer.Capture[AppendResult] = {
				// Check if the received records contain a later [[TransitionalConfigChange]] that includes this participant.
				findLastIncludingConfigChangeIn(prevRecordIndex + 1, batch).fold(
					// If not, return a rejection.
					sequencer.Keeper(AppendResult_Rejected(finalTerm, excludingConfigIndex + 1, ordinal))
				) { findResult =>
					// If yes, become starting and redirect the append records request to the new role.
					val activeParticipants = ListSet.newBuilder.addAll(findResult.tcc.oldParticipants).addAll(findResult.tcc.newParticipants).result()
					become(Starting(findResult.index, activeParticipants))
						.onAppendRecords(inquirerId, inquirerTerm, prevRecordIndex, prevRecordTerm, batch, leaderCommit, termAtLeaderCommit)
				}
			}

			/** Finds the last [[TransitionalConfigChange]] that includes this participant among the provided records.
			 * @param offset the [[RecordIndex]] of the first record.
			 * @param records the records to search in. */
			private def findLastIncludingConfigChangeIn(offset: RecordIndex, records: IArray[Record]): Maybe[(index: RecordIndex, tcc: TransitionalConfigChange[ParticipantId])] = {
				val excludingConfigRelativeIndex = (thisRetiring.excludingConfigIndex - offset).toInt
				var relativeIndex = records.length - 1
				while relativeIndex >= 0 && relativeIndex > excludingConfigRelativeIndex do {
					records(relativeIndex) match {
						case tcc: TransitionalConfigChange[ParticipantId] @unchecked if tcc.newParticipants.contains(boundParticipantId) => return Maybe((relativeIndex + offset, tcc))
						case _ => relativeIndex -= 1
					}
				}
				Maybe.empty
			}

			override def onInstallSnapshot(inquirerId: ParticipantId, inquirerTerm: Term, snapshot: SnapshotData[ParticipantId], batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term)(using Trace.Context): sequencer.Capture[AppendResult] = {
				// Check if the received records or the snapshot contain a later [[TransitionalConfigChange]] that includes this participant.
				findLastIncludingConfigChangeIn(snapshot.lastIncludedRecordIndex + 1, batch).orElse(
					snapshot.latestConfigChange match {
						case tcc: TransitionalConfigChange[ParticipantId] if tcc.newParticipants.contains(boundParticipantId) && snapshot.latestConfigChangeIndex > excludingConfigIndex => Maybe((snapshot.lastIncludedRecordIndex, tcc))
						case _ => Maybe.empty
					}
				).fold(
					// If not, return a rejection.
					sequencer.Keeper(AppendResult_Rejected(finalTerm, excludingConfigIndex + 1, ordinal))
				) { findResult =>
					// If yes, become starting and redirect the append records request to the new role.
					val activeParticipants = ListSet.newBuilder.addAll(findResult.tcc.oldParticipants).addAll(findResult.tcc.newParticipants).result()
					become(Starting(findResult.index, activeParticipants))
						.onInstallSnapshot(inquirerId, inquirerTerm, snapshot, batch, leaderCommit, termAtLeaderCommit)
				}
			}

			override def onQuiescencePermitted(grantorId: ParticipantId, indexOfGrantedStableConfigChange: RecordIndex)(using Trace.Context): Unit = {
				if indexOfGrantedStableConfigChange > indexOfStableConfigChangeForWhichQuiescenceWasPermitted then {
					quiescenceGrantor = Maybe(grantorId)
					indexOfStableConfigChangeForWhichQuiescenceWasPermitted = indexOfGrantedStableConfigChange
					becomeQuiescedIfEligible(excludingConfigIndex)
				}
			}
		}

		/**
		 * @param finalTerm the [[Term]] during which this participant became [[Retiring]]. Used only as argument for the [[NotificationListener.onRetiring]] method, and [[AppendResult]] responses.
		 * @param termAtExcludingConfigIndex the [[Term]] of the [[StableConfigChange]] that excluded this participant causing its retirement. This is the term that a [[Retiring]] participant exposes in [[StateInfo]] during elections.
		 * @param excludingConfigIndex the index of the [[StableConfigChange]] that excluded this participant causing its retirement.
		 * @param excludingConfigElectorate the electorate of the [[StableConfigChange]] that excluded this participant. */
		private final def Retiring(finalTerm: Term, termAtExcludingConfigIndex: Term, excludingConfigIndex: RecordIndex, excludingConfigElectorate: IArray[ParticipantId]): Maybe[Retiring] = {
			currentRole match {
				case retiring: Retiring if retiring.excludingConfigIndex == excludingConfigIndex && retiring.termAtExcludingConfigIndex == termAtExcludingConfigIndex && retiring.finalTerm == finalTerm => Maybe.empty
				case _ => Maybe(new Retiring(finalTerm, termAtExcludingConfigIndex, excludingConfigIndex, excludingConfigElectorate))
			}
		}

		//// STARTING ////

		/** The behavior when the participant has the [[STARTING]] role. This is a transitory role during which the participant state is initialized.
		 * When initialization is completed it transitions to the [[Isolated]] state.
		 * @param indexOfTheIncludingConfigChange the [[RecordIndex]] of the [[TransitionalConfigChange]] that caused this [[ConsensusParticipant]] service to join.
		 * @param participantsInTheIncludingConfigChange the active participants in the [[TransitionalConfigChange]] pointed by `indexOfTheIncludingConfigChange`.
		 * TODO consider adding a parameter with the set of active participants in the including [[ConfigChange]], to pass it to the Joining role, in order to return a more updated set of active participants when responding with [[Unable]] to a command from a client. */
		private final class Starting(val indexOfTheIncludingConfigChange: RecordIndex, participantsInTheIncludingConfigChange: ListSet[ParticipantId]) extends Role {
			override val ordinal: RoleOrdinal = STARTING
			override val rank: ElectionRank = ElectionRank_from(STARTING)
			/** Is fulfilled after initializing this [[ConsensusParticipant]] and becoming another [[Role]]: [[Joining]], [[Isolated]], or [[Quiesced]]. */
			private val startingCompletedCovenant: sequencer.Captor[Maybe[PrimaryState]] = sequencer.Captor()

			override def handleEnter(previous: Role)(using Trace.Context): Unit = {
				Trace.step("Starting.onEnter") {
					notifyListeners(_.onStarting(previous.ordinal, indexOfTheIncludingConfigChange))

					val startupCapture = for {
						loadedWorkspace <- storage.load
						recoveredHighestAppliedCommandIndex <- machine.recoverIndexOfLastAppliedCommand
					} yield (loadedWorkspace, recoveredHighestAppliedCommandIndex)

					startupCapture.triggerSync(new sequencer.MonoObserver[(WS, RecordIndex)] {
						override def onSuccess(startupData: (WS, RecordIndex)): Unit = {
							val (loadedWorkspace, recoveredHighestAppliedCommandIndex) = startupData
							val primaryState = new PrimaryState(loadedWorkspace)
							val indexOfLatestConfigChange = primaryState.indexOfLatestConfigChange
							val snapshotCommitIndex = loadedWorkspace.latestSnapshot.fold(0L)(_.lastIncludedRecordIndex)
							highestAppliedCommandIndex = recoveredHighestAppliedCommandIndex
							commitIndex = recoveredHighestAppliedCommandIndex.max(snapshotCommitIndex)
							val rulingConfigChange = {
								if indexOfLatestConfigChange == 0 then {
									loadedWorkspace.setTermAndVote(PRE_INIT, Maybe.empty)
									new TransitionalConfigChange[ParticipantId](PRE_INIT, "Initial-Config", Set.empty, cluster.getInitialParticipants) // TODO consider using the set provided in the Starting constructor instead, and remove the `getInitialParticipants` method.

								} else primaryState.latestConfigChange.get match {
									// If the top configuration change in the log is a stable one, then the previous transitional configuration change rules until the commitIndex crosses the index of top stable one.
									case stableConfigChange: StableConfigChange[ParticipantId] @unchecked =>
										if commitIndex >= indexOfLatestConfigChange then stableConfigChange
										else stableConfigChange.recreateCouple

									// If the top configuration change in the log is a transitional one, then it rules immediately.
									case transitionalConfigChange: TransitionalConfigChange[ParticipantId] @unchecked =>
										transitionalConfigChange
								}
							}
							val config = Configuration_from(rulingConfigChange, indexOfLatestConfigChange)
							val isSeed = indexOfTheIncludingConfigChange == 0
							if isSeed && !config.isBoundIncluded then {
								become(Quiesced(Success(s"Start-up aborted because this ConsensusParticipant instance does not belong to the active cluster-configuration.")))
								startingCompletedCovenant.captureSync(Maybe.empty)
							}
							else {
								latestDerivedConfig = Maybe(config)
								val primaryStateFence = CausalFence[PrimaryState, sequencer.type](sequencer)(primaryState)
								notifyListeners(_.onStarted(previous.ordinal, primaryState.currentTerm, rulingConfigChange, isSeed))
								if isSeed then become(Isolated(primaryStateFence))
								else become(Joining(primaryStateFence, indexOfTheIncludingConfigChange, participantsInTheIncludingConfigChange))
								startingCompletedCovenant.captureSync(Maybe(primaryState))
							}
						}

						override def onError(e: Throwable): Unit = {
							Trace.error(s"$boundParticipantId: Unexpected error while loading the consensus-service's workspace or recovering state machine:", e)
							become(Quiesced(Failure(e)))
							startingCompletedCovenant.captureSync(Maybe.empty)
						}
					})
				}
			}

			override def syncLocalStateInfo(maybePrimaryState: Maybe[PrimaryState])(using Context): StateInfo = {
				buildIneligibleInfo(maybePrimaryState.fold(PRE_INIT)(_.currentTerm))
			}

			override def determineMyVote(primaryState0: PrimaryState, stateInfo0: StateInfo, blankVoteIfRoleChanges: Boolean)(using Context): sequencer.Capture[Vote[ParticipantId]] = {
				checkWithin()
				for {
					maybePrimaryState1 <- startingCompletedCovenant
					response <- maybePrimaryState1.fold {
						currentRole.yieldsBlankVote(PRE_INIT, stateInfo0.ballot)
					} { primaryState1 =>
						currentRole.determineMyVote(primaryState1, currentRole.syncStatefulStateInfo(primaryState1), blankVoteIfRoleChanges)
					}
				} yield response
			}

			override def onHowAreYou(inquirerId: ParticipantId, inquirerInfo: StateInfo)(using Trace.Context): sequencer.Capture[StateInfo] = {
				for {
					_ <- startingCompletedCovenant
					response <- currentRole.onHowAreYou(inquirerId, inquirerInfo)
				} yield response
			}

			override def onChooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo)(using Trace.Context): sequencer.Capture[Vote[ParticipantId]] = {
				for {
					_ <- startingCompletedCovenant
					response <- currentRole.onChooseALeader(inquirerId, inquirerInfo)
				} yield response
			}

			override def onAppendRecords(inquirerId: ParticipantId, inquirerTerm: Term, prevRecordIndex: RecordIndex, prevRecordTerm: Term, batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term)(using Trace.Context): sequencer.Capture[AppendResult] = {
				for {
					_ <- startingCompletedCovenant
					response <- currentRole.onAppendRecords(inquirerId, inquirerTerm, prevRecordIndex, prevRecordTerm, batch, leaderCommit, termAtLeaderCommit)
				} yield response
			}

			override def onInstallSnapshot(inquirerId: ParticipantId, inquirerTerm: Term, snapshot: SnapshotData[ParticipantId], batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term)(using Trace.Context): sequencer.Capture[AppendResult] = {
				for {
					_ <- startingCompletedCovenant
					response <- currentRole.onInstallSnapshot(inquirerId, inquirerTerm, snapshot, batch, leaderCommit, termAtLeaderCommit)
				} yield response
			}

			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag)(using Trace.Context): sequencer.Capture[ResponseToClient] = {
				for {
					_ <- startingCompletedCovenant
					rtc <- currentRole.onCommandFromClient(command, attemptFlag)
				} yield rtc
			}

			override def requestConfigChange(requestId: ConfigChangeRequestId, desiredParticipantsSet: Set[ParticipantId], priorAnswer: Maybe[ConfigChangeResponse])(using Trace.Context): sequencer.Capture[ConfigChangeResponse] = {
				for {
					_ <- startingCompletedCovenant
					response <- currentRole.requestConfigChange(requestId, desiredParticipantsSet, priorAnswer)
				} yield response
			}
		}

		private final def Starting(indexOfTheIncludingConfigChange: RecordIndex, participantsInTheIncludingConfigChange: ListSet[ParticipantId]): Maybe[Starting] = {
			currentRole match {
				case starting: Starting if starting.indexOfTheIncludingConfigChange == indexOfTheIncludingConfigChange => Maybe.empty
				case _ => Maybe(new Starting(indexOfTheIncludingConfigChange, participantsInTheIncludingConfigChange))
			}
		}

		//// JOINING ////

		private final class Joining(psf: CausalFence[PrimaryState, sequencer.type], val indexOfTheIncludingConfigChange: RecordIndex, participantsInTheIncludingConfigChange: ListSet[ParticipantId]) extends StatefulRole(psf) { thisJoining =>
			/** The ordinal corresponding to this [[Role]] */
			override val ordinal: RoleOrdinal = JOINING
			override val rank: ElectionRank = ElectionRank_from(JOINING)

			override def handleEnter(previous: Role)(using Trace.Context): Unit = {
				notifyListeners(_.onJoining(previous.ordinal, indexOfTheIncludingConfigChange))
			}

			override def determineMyVote(primaryState0: PrimaryState, currentStateInfo: StateInfo, blankVoteIfRoleChanges: Boolean)(using Context): sequencer.Capture[Vote[ParticipantId]] = {
				yieldsBlankVote(primaryState0.currentTerm, currentStateInfo.ballot)
			}

			override def onChooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo)(using Trace.Context): sequencer.Capture[Vote[ParticipantId]] = {
				for {
					primaryState <- primaryStateFence.causalAnchor()
					response <- {
						if currentRole ne thisJoining then currentRole.onChooseALeader(inquirerId, inquirerInfo)
						else yieldsBlankVote(primaryState.currentTerm, updateLocalStateInfo(Maybe(primaryState), inquirerId, inquirerInfo).ballot)
					}
				} yield response
			}

			/** @inheritdoc
			 * This implementation responds with a rejection that propagates the received `attemptFlag`. */
			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag)(using Trace.Context): sequencer.Capture[ResponseToClient] = {
				sequencer.Keeper(Unable(attemptFlag.withInternalBitsCleared, participantsInTheIncludingConfigChange))
			}

			override def requestConfigChange(primaryState: PrimaryState, requestId: ConfigChangeRequestId, desiredParticipants: Set[ParticipantId], ballotWasUpdated: Boolean)(using Trace.Context): sequencer.Capture[ConfigChangeResponse] = {
				sequencer.Keeper(new CATCHING_UP(syncStatefulStateInfo(primaryState).ballot))
			}
		}

		private final def Joining(psf: CausalFence[PrimaryState, sequencer.type], indexOfTheIncludingConfigChange: RecordIndex, participantsInTheIncludingConfigChange: ListSet[ParticipantId]): Maybe[Joining] = {
			currentRole match {
				case joining: Joining if joining.indexOfTheIncludingConfigChange == indexOfTheIncludingConfigChange && (joining.primaryStateFence eq psf) => Maybe.empty
				case _ => Maybe(new Joining(psf, indexOfTheIncludingConfigChange, participantsInTheIncludingConfigChange))
			}
		}

		//// ISOLATED ////

		/**
		 * Behavior when the participant has the [[ISOLATED]] role. Taken when reachability to a majority of the participants was not achieved or after the [[STARTING]] role has completed.
		 * The participant transitions to this state after [[Starting]] or when reachability to other participants drops below [[smallestMajority]].
		 * This state is abandoned when a majority of the participants are reachable.
		 * [[Vote]]s cast by participants in this state are ignored.
		 */
		private class Isolated(psf: CausalFence[PrimaryState, sequencer.type]) extends StatefulRole(psf) { thisIsolated =>
			override val ordinal: RoleOrdinal = ISOLATED
			override val rank: ElectionRank = ElectionRank_from(ISOLATED)

			/**
			 * The main loop of the isolated state.
			 * It checks if the current term leader is reachable or the reachable participants including itself are the majority.
			 * If so, it becomes a follower or a candidate respectively.
			 * If not, it stays in the isolated state and checks again after a while.
			 */
			override def handleEnter(previous: Role)(using Trace.Context): Unit = {
				notifyListeners(_.onBecameIsolated(previous.ordinal, psf.committedState.fold(PRE_INIT)(_ => PRE_INIT)(_.currentTerm)))
			}

			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag)(using Trace.Context): sequencer.Capture[ResponseToClient] = {
				checkWithin()
				for {
					primaryState <- primaryStateFence.causalAnchor()
					response <- {
						if currentRole ne thisIsolated then currentRole.onCommandFromClient(command, attemptFlag)
						else updateRoleAndThenCallsOnCommandFromClient(command, attemptFlag)
					}
				} yield response
			}

			override def requestConfigChange(primaryState0: PrimaryState, requestId: ConfigChangeRequestId, desiredParticipants: Set[ParticipantId], ballotWasUpdated: Boolean)(using Context): sequencer.Capture[ConfigChangeResponse] = {
				Trace.trace(s"Updating role from ${RoleOrdinal_nameOf(currentRole.ordinal)} due to a configuration change request. ")
				for {
					_ <- updateRole(primaryState0) // TODO consider making updateRole return the current primary state, so that the causalAnchor method call is not needed here (and other places also).
					primaryState <- primaryStateFence.causalAnchor()
					response <- {
						if currentRole ne thisIsolated then currentRole.requestConfigChange(requestId, desiredParticipants, Maybe.empty)
						else sequencer.Keeper(new SECLUDED(syncStatefulStateInfo(primaryState).ballot))
					}
				} yield response
			}
		}

		private final def Isolated(psf: CausalFence[PrimaryState, sequencer.type]): Maybe[Isolated] = {
			currentRole match {
				case isolated: Isolated if isolated.ordinal == ISOLATED && (isolated.primaryStateFence eq psf) => Maybe.empty
				case _ => Maybe(new Isolated(psf))
			}
		}

		//// ABDICATING ////

		/** A transitional [[Role]] that:
		 *		- always comes immediately after [[Leader]] when a later [[Term]] is seen in a [[StateInfo]] of a peer;
		 *		- updates the [[Term]] to the provided one, and then transitions to [[Isolated]] or [[Retiring]].
		 *
		 * Note that this [[Role]] is not hosted when the [[Term]] is updated by the [[StatefulRole.onAppendRecords]] handler, which does the update itself.
		 * It behaves as [[Isolated]] except that, in the [[handleEnter]] life-cycle stage it enqueues an updater of the [[PrimaryState.currentTerm]] that sets it to the latest [[Term]] seen if not already; and then transitions to [[Retiring]] if this participant is excluded from the active [[Configuration]], or to [[Isolated]] otherwise.
		 *
		 * @param endedTerm the [[Term]] that concluded, during which this participant acted as [[Leader]].
		 * TODO Replace this class with a method that transitions to [[Isolated]] or [[Retiring]] in a synchronous manner, and then enqueues a term update. The problem with the current class approach is the incorrect isolated-like behavior during the transition to retiring.
		 */
		private final class Abdicating(endedTerm: Term, latestTermSeen: Term, psf: CausalFence[PrimaryState, sequencer.type]) extends Isolated(psf) { thisAbdicating =>
			override val ordinal: RoleOrdinal = HANDING_OFF

			override val rank: ElectionRank = ElectionRank_from(HANDING_OFF)

			override def handleEnter(previous: Role)(using Trace.Context): Unit = {
				notifyListeners(_.onAbdicating(endedTerm))

				primaryStateFence.advanceIf { primaryState0 =>
					if primaryState0.currentTerm >= latestTermSeen then Maybe.empty
					else Maybe(primaryState0.withTermUpdated(latestTermSeen))
				}.triggerSync(new sequencer.MonoObserver[PrimaryState] {
					override def onSuccess(primaryState1: PrimaryState): Unit = {
						if currentRole eq thisAbdicating then {
							// check if excluded from the new configuration.
							val updatedConfig = deriveConfigurationFrom(primaryState1)
							// if excluded, become Retiring
							if !updatedConfig.isBoundIncluded then become(Retiring(primaryState1.currentTerm, updatedConfig.term, updatedConfig.changeIndex, updatedConfig.electorate))
							// else, become Isolated
							else become(Isolated(primaryStateFence))
						}
					}

					override def onError(e: Throwable): Unit = become(Quiesced(Failure(e)))
				})
			}
		}

		private final def Abdicating(endedTerm: Term, latestTermSeen: Term, psf: CausalFence[PrimaryState, sequencer.type]): Maybe[Abdicating] = {
			Maybe(new Abdicating(endedTerm, latestTermSeen, psf))
		}

		//// FOLLOWER ////

		/**
		 * Behavior when the participant has the follower role. Taken when reachability to a majority of the participants is achieved and one of them has the [[Leader]] role and is in a higher or equal term.
		 *
		 * In this state, the participant acknowledges the specified leader.
		 * @param term the [[Term]] during which the followed participant is the leader. This field exists to differentiate [[Follower]] instances. // TODO explain why is necessary to differentiate them.
		 * @param followeeId The ID of the participant this follower is following.
		 */
		private final class Follower(val term: Term, val followeeId: ParticipantId, psf: CausalFence[PrimaryState, sequencer.type]) extends StatefulRole(psf) { thisFollower =>
			override val ordinal: RoleOrdinal = FOLLOWER
			override val rank: ElectionRank = ElectionRank_from(FOLLOWER)

			override def handleEnter(previous: Role)(using Trace.Context): Unit = {
				notifyListeners(_.onBecameFollower(previous.ordinal, term, followeeId))
			}

			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag)(using Trace.Context): sequencer.Capture[ResponseToClient] = {
				for {
					primaryState <- primaryStateFence.causalAnchor()
					response <- {
						if currentRole ne thisFollower then currentRole.onCommandFromClient(command, attemptFlag)
						else if attemptFlag == FIRST_ATTEMPT then sequencer.Keeper(RedirectTo(followeeId))
						else updateRoleAndThenCallsOnCommandFromClient(command, attemptFlag)
					}
				} yield response
			}

			override def requestConfigChange(primaryState0: PrimaryState, requestId: ConfigChangeRequestId, desiredParticipants: Set[ParticipantId], ballotWasUpdated: Boolean)(using Context): sequencer.Capture[ConfigChangeResponse] = {
				Trace.trace(s"Updating role from ${RoleOrdinal_nameOf(currentRole.ordinal)} due to a configuration change request.")
				for {
					_ <- updateRole(primaryState0)
					primaryState1 <- primaryStateFence.causalAnchor()
					response <- {
						if currentRole ne thisFollower then currentRole.requestConfigChange(requestId, desiredParticipants, Maybe.empty)
						else sequencer.Keeper(new ASK_THE_LEADER(followeeId, syncStatefulStateInfo(primaryState1).ballot))
					}
				} yield response
			}

			override def onTermUpdated(primaryState: PrimaryState, maybeLeaderId: Maybe[ParticipantId])(using Context): PrimaryState = {
				become(maybeLeaderId.fold(Isolated(primaryStateFence))(Follower(primaryState.currentTerm, _, primaryStateFence)))
				primaryState
			}
		}

		private final def Follower(term: Term, leaderId: ParticipantId, psf: CausalFence[PrimaryState, sequencer.type]): Maybe[Follower] = {
			currentRole match {
				case follower: Follower if follower.term == term && follower.followeeId == leaderId && (follower.primaryStateFence eq psf) => Maybe.empty
				case _ => Maybe(new Follower(term, leaderId, psf))
			}
		}

		//// PROMOTING ////

		/** A hidden (not seen by other participants) and transitional substage of a leading participant that last until the term bump is stored.
		 * During this interval, all the RPC calls this [[ConsensusParticipant]] receives are put in standby until the bumped term is stored and the role transitioned. This means that responses to queries form the outside never complete in this role and, therefore, the role ordinal in responses is never [[PROMOTING]].
		 * Also, given the [[currentRole]] is changed to [[Leader]] synchronously in a consumer synchronously subscribed to the [[Capture]] returned by [[primaryStateFence.advanceIf]], sections of code guarded by the same fence will never see [[currentRole]] referencing a [[Promoting]] instance. See the [[CausalFence]]'s game changing invariant. */
		private final class Promoting(fromTerm: Term, psf: CausalFence[PrimaryState, sequencer.type]) extends StatefulRole(psf) { thisPromoting =>
			/** The ordinal corresponding to this [[Role]] */
			override val ordinal: RoleOrdinal = PROMOTING
			override val rank: ElectionRank = ElectionRank_from(PROMOTING)

			/** Is fulfilled after bumping the term and becoming [[Leader]] if success, or [[Quiesced]] if fails to persist the primary state. */
			private val promotionCovenant: sequencer.Captor[PrimaryState] = sequencer.Captor()

			override def handleEnter(previous: Role)(using Trace.Context): Unit =
				Trace.step("Promoting.onEnter") {
					// Notify the listeners
					notifyListeners(_.onPromoting(previous.ordinal, getCommittedTerm))

					primaryStateFence.causalAnchor().triggerSyncCallbacks(
						primaryState1 => {
							if currentRole eq thisPromoting then {
								// Become the leader.
								val config1 = deriveConfigurationFrom(primaryState1)
								become(Maybe(new Leader(primaryState1.currentTerm, primaryState1, config1, primaryStateFence)))
							}
							promotionCovenant.captureSync(primaryState1)
						},
						error => {
							become(Quiesced(Failure(error)))
							promotionCovenant.trapSync(error)
						}
					)
				}

			override def determineMyVote(primaryState0: PrimaryState, dummy: StateInfo, blankVoteIfRoleChanges: Boolean)(using Context): sequencer.Capture[Vote[ParticipantId]] = {
				for {
					primaryState1 <- promotionCovenant
					vote <- currentRole.determineMyVote(primaryState1, currentRole.syncStatefulStateInfo(primaryState1), blankVoteIfRoleChanges)
				} yield vote
			}

			override def onHowAreYou(inquirerId: ParticipantId, inquirerInfo: StateInfo)(using Trace.Context): sequencer.Capture[StateInfo] = {
				for {
					_ <- promotionCovenant
					stateInfo <- currentRole.onHowAreYou(inquirerId, inquirerInfo)
				} yield stateInfo
			}

			override def onChooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo)(using Trace.Context): sequencer.Capture[Vote[ParticipantId]] = {
				for {
					_ <- promotionCovenant
					vote <- currentRole.onChooseALeader(inquirerId, inquirerInfo)
				} yield vote
			}

			override def onAppendRecords(inquirerId: ParticipantId, inquirerTerm: Term, prevRecordIndex: RecordIndex, prevRecordTerm: Term, batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term)(using Trace.Context): sequencer.Capture[AppendResult] = {
				for {
					_ <- promotionCovenant
					response <- currentRole.onAppendRecords(inquirerId, inquirerTerm, prevRecordIndex, prevRecordTerm, batch, leaderCommit, termAtLeaderCommit)
				} yield response
			}

			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag)(using Trace.Context): sequencer.Capture[ResponseToClient] = {
				for {
					_ <- promotionCovenant
					response <- currentRole.onCommandFromClient(command, attemptFlag)
				} yield response
			}

			override def requestConfigChange(primaryState: PrimaryState, requestId: ConfigChangeRequestId, desiredParticipants: Set[ParticipantId], ballotWasUpdated: Boolean)(using Trace.Context): sequencer.Capture[ConfigChangeResponse] = {
				for {
					_ <- promotionCovenant
					response <- currentRole.requestConfigChange(requestId, desiredParticipants, Maybe.empty)
				} yield response
			}
		}

		private final def Promoting(fromTerm: Term, psf: CausalFence[PrimaryState, sequencer.type]): Maybe[Promoting] = {
			currentRole match {
				case leader: Leader if leader.leadedTerm == fromTerm && (leader.primaryStateFence eq psf) => Maybe.empty
				case _ => Maybe(new Promoting(fromTerm, psf))
			}
		}

		//// LEADER ////

		/**
		 * Behavior when the participant has the [[LEADER]] role. Taken when reachability to a majority of the participants is achieved, none of them is a [[Leader]] with higher or equal term, and wins the new leader election.
		 *
		 * In this state, the participant coordinates consensus decisions.
		 * @param leadedTerm the [[Term]] owned by this [[Leader]] instance.
		 * @param initialPrimaryState the current [[PrimaryState]] when this [[Leader]] instance was created. Intended to be used in the [[handleEnter]] method only. Do not use elsewhere.
		 * @param initialConfig the active [[Configuration]] when this [[Leader]] instance was created. Intended to be used in the [[handleEnter]] method only. Do not use elsewhere.
		 * @param wsf the [[CausalFence]] that must be used to ensure causal ordering of the state updates. It must be propagated to subsequent [[StatefulRole]] instances.
		 * TODO replace the `initialPrimaryState` parameter with what is obtained from it. Storing an instance of [[PrimaryState]] is error prone.
		 */
		private final class Leader(val leadedTerm: Term, initialPrimaryState: PrimaryState, initialConfig: Configuration, wsf: CausalFence[PrimaryState, sequencer.type]) extends StatefulRole(wsf) { thisLeader =>
			/** The outcome of the [[sequencer.Capture]] returned by a call to [[ClusterParticipant.appendRecords]]. */
			private type AppendResponse = (Int, AppendResult)

			override val ordinal: RoleOrdinal = LEADER
			override val rank: ElectionRank = ElectionRank_from(LEADER)

			override def diagnosticInfo: RoleDiagnostic = {
				val config = latestDerivedConfig.get
				val progress = config.peers.mapWithIndex { (peerId, idx) =>
					val lp = learnerProgressByIndex(idx)
					PeerProgressDiagnostic(peerId, lp.highestRecordIndexKnownToBeAppended, lp.highestRecordIndexKnowToBeCommitted)
				}
				LeaderRoleDiagnostic(ordinal, config.backingConfigChange, progress)
			}

			private var learnerProgressByIndex: IArray[LearnerProgress] = IArray.tabulate(initialConfig.peers.size)(_ => new LearnerProgress(initialPrimaryState.firstEmptyRecordIndex))

			/** Either, the index of the [[StableConfigChange]] that excluded this leading participant causing it become a ghost leader, or zero if in joint consensus or not excluded.
			 * Set by the [[Leader.driveTheRetirements]] method, which is called by [[deriveConfigurationFrom]] when the active [[Configuration]] changes from a [[TransitionalConfig]] to a [[StableConfig]]. */
			private var indexOfConfigChangeThatExcludedThisParticipant: RecordIndex = 0

			private def CommitIndexAwaiter(targetIndex: RecordIndex, captor: sequencer.Captor[PrimaryState]): CommitIndexAwaiter = new CommitIndexAwaiter(targetIndex)

			/** TODO to minimize allocations, make this class extend [[Captor]] instead of containing one. */
			private class CommitIndexAwaiter(val targetIndex: RecordIndex) extends sequencer.Captor[PrimaryState]

			private val pendingRecordBecomesCommittedAwaiters: scala.collection.mutable.ArrayBuffer[CommitIndexAwaiter] = scala.collection.mutable.ArrayBuffer.empty

			private var pendingConfigChangesCompletion: sequencer.Capture[ConfigChangeResponse] = sequencer.Keeper(new SUCCESSFULLY_CHANGED)

			override def handleEnter(previous: Role)(using Trace.Context): Unit = {
				Trace.step("Leader.onEnter") {
					notifyListeners(_.onBecameLeader(previous.ordinal, leadedTerm))

					val indexOfLatestConfigChange = initialPrimaryState.indexOfLatestConfigChange
					// if the log lacks a ConfigChange record (is empty), create a synthetic one with the seed participants of the initial synthetic configuration (appointed in `latestDerivedConfig` during Starting).
					if indexOfLatestConfigChange == 0 then {
						for primaryState1 <- primaryStateFence.advance { primaryState0 =>
							primaryState0.withSingleRecordAppended(primaryState0.currentTerm, latestDerivedConfig.get.backingConfigChange)
						} yield startConfigChangeSecondPhase(latestDerivedConfig.get.backingConfigChange.asInstanceOf[TransitionalConfigChange[ParticipantId]], 1)
					}
					// if the log contains a ConfigChange then:
					else initialPrimaryState.latestConfigChange.get match {
						// If the top configuration change in the local log is a transitional one, continue the configuration transition process. This happens when the leader that started the first phase of the configuration change crashed or left the leadership before achieving the replication of the TransitionalConfigChange to a majority, or while storing the StableConfigChange in his persistent log.
						case tcc: TransitionalConfigChange[ParticipantId @unchecked] =>
							pendingConfigChangesCompletion = startSecondPhase(tcc, indexOfLatestConfigChange)

						// If, on the contrary, is a stable one
						case scc: StableConfigChange[ParticipantId @unchecked] =>
							// ... and it was committed (commitIndex >= its index in the log), program the driving of excluded participants to retirement.
							if commitIndex >= indexOfLatestConfigChange then thisLeader.driveTheRetirements(initialPrimaryState, Maybe.empty, scc, indexOfLatestConfigChange)
							// ... and it wasn't committed (commitIndex < its index in the log), drive its commitment eagerly.
							else {
								pendingConfigChangesCompletion = for {
									isSccReplicated <- replicateSccUntilSuccessOrLeaderRoleIsAbandoned(initialPrimaryState, indexOfLatestConfigChange, 0)
									response <- {
										if isSccReplicated then sequencer.Keeper[ConfigChangeResponse](new SUCCESSFULLY_CHANGED)
										else for primaryState1 <- primaryStateFence.causalAnchor() yield new REQUEST_TRACKING_LOST_AFTER_SECOND_PHASE_STARTED(currentRole.syncLocalStateInfo(Maybe(primaryState1)).ballot)
									}
								} yield response
							}
							
					}
				}
			}

			override def handleExit()(using Trace.Context): Unit = {
				pendingRecordBecomesCommittedAwaiters.foreach { awaiter =>
					awaiter.seizeWith(primaryStateFence.causalAnchor())
				}
				pendingRecordBecomesCommittedAwaiters.clear()
				learnerProgressByIndex.foreach(_.unreachableRetryWakeUpToken.foreach(_.cancel()))
				retiringLearnersById.values.foreach(_.unreachableRetryWakeUpToken.foreach(_.cancel()))
				retryPermitQuiescenceWakeUpToken.foreach(_.cancel())
				retryPermitQuiescenceWakeUpToken = Maybe.empty
				super.handleExit()
			}

			def isGhost: Boolean = indexOfConfigChangeThatExcludedThisParticipant > 0

			/** @inheritdoc
			 *  This implementation does two different things:
			 *  1) Updates the [[retirementDriverByParticipantId]] map to include any new old-configuration-only retiring participant (those that are not part of the new [[Configuration]], but still need more appends until their [[commitIndex]] reaches the index of the [[StableConfigChange]] that excluded them).
			 *  2) Recreates and initializes the [[learnerProgressByIndex]] array keeping the elements corresponding to the participants that remain and moving them to the appropriate index.
			 * @param oldConfig the [[Configuration]] that determines which participants corresponds to each element of the [[learnerProgressByIndex]] array before the transition.
			 * @note This rearrangement wouldn't be necessary if maps instead of arrays were used. But considering these two collections are heavily used, efficiency was primed. */
			override def handleActiveConfigChange(currentPrimaryState: PrimaryState, oldConfig: Configuration, newConfig: Configuration, indexOfNewConfigChange: RecordIndex)(using Context): Unit = Trace.step("handleActiveConfigChange") {
				// Stop and remove retirement pipelines for any participant that is an active peer in the new configuration.
				retiringLearnersById.filterInPlace { (retireeId, learnerProgress) =>
					if newConfig.activeParticipants.contains(retireeId) then {
						learnerProgress.unreachableRetryWakeUpToken.foreach(_.cancel())
						false
					} else true
				}

				// Step one. Must be before step two.
				newConfig.backingConfigChange.match {
					case scc: StableConfigChange[ParticipantId] =>
						thisLeader.driveTheRetirements(currentPrimaryState, Maybe(oldConfig), scc, indexOfNewConfigChange)
					case tcc: TransitionalConfigChange[ParticipantId] =>
						// If a previous configuration change excluded this leading participant but a later configuration change includes it, clear the mark that instructs itself to retire (when it sees that the previous config change is committed).
						if indexOfConfigChangeThatExcludedThisParticipant > 0 && newConfig.isBoundIncluded then indexOfConfigChangeThatExcludedThisParticipant = 0
				}

				// Step two
				val newAllOtherParticipantsArrayLength = newConfig.peers.length
				val newLearnerProgressByIndex: Array[LearnerProgress] = new Array(newAllOtherParticipantsArrayLength)

				var participantNewIndex = newAllOtherParticipantsArrayLength
				while participantNewIndex > 0 do {
					participantNewIndex -= 1
					val participantId = newConfig.peers(participantNewIndex)
					val participantOldIndex = oldConfig.peerIndexOf(participantId)
					if participantOldIndex >= 0 then {
						newLearnerProgressByIndex(participantNewIndex) = learnerProgressByIndex(participantOldIndex)
					} else {
						newLearnerProgressByIndex(participantNewIndex) = new LearnerProgress(indexOfNewConfigChange)
					}
				}
				learnerProgressByIndex = IArray.unsafeFromArray(newLearnerProgressByIndex)
			}

			/** Drives the excluded participants (the ones that are not active in the provided [[StableConfigChange]]) to retirement.
			 *		- If this [[Leader]] is excluded, sets the threshold [[indexOfConfigChangeThatExcludedThisParticipant]]. The replication logic checks it after successful appends to decide if a transition to the [[Retiring]] [[Role]] is needed.
			 *		- Registers and starts a retirement pipeline via [[driveReplicationPipeline]] for each excluded follower that needs more appends to become [[Retiring]].
			 * Must be called a single time whenever the participant becomes [[Leader]] with a [[StableConfig]] or the participant is leading and the active [[Configuration]] transitions to a [[StableConfig]].
			 *
			 * @param primaryState the [[PrimaryState]] from which the transition is derived.
			 * @param maybeStandingConfig the [[Configuration]] on which the [[Leader]] derived state is based, or [[Maybe.empty]] to indicate [[thisLeader]] is brand new (called from [[Leader.handleEnter]]). It's [[Configuration.backingConfigChange]] may be the same as the received in the `stableConfigChange` parameter. It is needed to know what is in each element of the [[indexOfNextRecordToSend_ByParticipantIndex]] and [[highestRecordIndexKnownToBeAppended_ByParticipantIndex]].
			 * @param stableConfigChange the [[StableConfigChange]] that might exclude participants.
			 * @param stableConfigChangeIndex the log index where the provided [[StableConfigChange]] is stored. */
			private def driveTheRetirements(primaryState: PrimaryState, maybeStandingConfig: Maybe[Configuration], stableConfigChange: StableConfigChange[ParticipantId], stableConfigChangeIndex: RecordIndex)(using Context): Unit = Trace.step("driveTheRetirements") {
				assert(commitIndex >= stableConfigChangeIndex && maybeStandingConfig.fold(true)(_.isInstanceOf[TransitionalConfig]))

				// Stop pipelines for participants that become included.
				retiringLearnersById.filterInPlace { (retireeId, learnerProgress) =>
					if stableConfigChange.newParticipants.contains(retireeId) then {
						learnerProgress.unreachableRetryWakeUpToken.foreach(_.cancel())
						false
					} else true
				}

				// Find out which are the participants that become excluded.
				val newRetiringParticipants = stableConfigChange.oldParticipants.diff(stableConfigChange.newParticipants)
				val notNewParticipants = newRetiringParticipants.union(cluster.getOtherProbableParticipants).diff(stableConfigChange.newParticipants)

				// If this leader is excluded, set the threshold until which this leader will continue leading as a ghost.
				if newRetiringParticipants.contains(boundParticipantId) then thisLeader.indexOfConfigChangeThatExcludedThisParticipant = stableConfigChangeIndex
				// If this leader continues as a stable leader (not a ghost), authorize the quiescence of the retiring followers.
				else authorizeQuiescenceTo(notNewParticipants, stableConfigChangeIndex, false)

				/** Creates and registers a [[LearnerProgress]] tracker and starts the continuous replication pipeline for the specified participant. */
				def start(participantId: ParticipantId, optimisticIndexOfNextRecordToSend: RecordIndex): Unit = {
					val learnerProgress = new LearnerProgress(optimisticIndexOfNextRecordToSend)
					learnerProgress.excludingSccIndex = stableConfigChangeIndex
					retiringLearnersById.put(participantId, learnerProgress)
					driveReplicationPipeline(primaryState, participantId, learnerProgress, 0, isRetiree = true)
				}

				// Start a replication pipeline for each participant that both, is not included, and we are not certain that it has committed the `stableConfigChange`.
				maybeStandingConfig.fold(
					// Logic for a brand new Leader: Start a pipeline for all the excluded peers, each of which starts sending the `stableConfigChange` record only.
					notNewParticipants.foreach { participantId =>
						if participantId != boundParticipantId then start(participantId, stableConfigChangeIndex)
					}
				) { standingConfig => // Logic for a incumbent Leader: Start a pipeline for all the excluded peers that haven't already committed the `stableConfigChange`, each of which starts sending the records from the `indexOfNextRecordToSend` up to `stableConfigChangeIndex`.
					val initialLearnerProgressByIndex = learnerProgressByIndex
					standingConfig.peers.foreachWithIndex { (participantId, participantIndex) =>
						if initialLearnerProgressByIndex(participantIndex).highestRecordIndexKnowToBeCommitted < stableConfigChangeIndex && newRetiringParticipants.contains(participantId)
						then start(participantId, initialLearnerProgressByIndex(participantIndex).optimisticIndexOfNextRecordToSend)
					}
				}
			}

			/** Starts a process that insistently authorizes the quiescence of the participants specified in this and previous calls; allowing them to transition from [[Retiring]] to [[Quiesced]] provided they retire due to being excluded by a [[StableConfigChange]] at the `permittedConfigChangeIndex`.
			 * @param peers the participants to authorize the quiescence of.
			 * @param permittedConfigChangeIndex the index of the [[StableConfigChange]] for which the quiescence is authorized. The destination participant will quiesce only if it reaches the [[Retiring]] state with a [[Retiring.excludingConfigIndex]] equal to this value.
			 * @param includeMyself whether to include this participant in the set of participants to authorize the quiescence of. If true, this participant will be authorized after all the others have acknowledged the authorization.
			 * TODO Consider having independent attempts counter for each peer. */
			private def authorizeQuiescenceTo(peers: Set[ParticipantId], permittedConfigChangeIndex: RecordIndex, includeMyself: Boolean)(using Trace.Context): Unit = {
				Trace.step(() => s"authorizeQuiescenceTo($peers, $permittedConfigChangeIndex, $includeMyself)") {
					def loop(attemptsDone: Int = 0): Unit = {
						val nonAcknowledgedQuiescencePermissionsArray = nonAcknowledgedQuiescencePermissions.toArray
						val calls = for (participantId, indexOfAuthorizedScc) <- nonAcknowledgedQuiescencePermissionsArray yield participantId.permitQuiescence(indexOfAuthorizedScc)
						for responses <- sequencer.Capture_sequenceHardyToArray(calls) do {
							Trace.trace(s"Quiescence permission acknowledgments: ${nonAcknowledgedQuiescencePermissionsArray.zip(responses).mkString("[", ", ", "]")}")
							IArray.unsafeFromArray(responses).foreachWithIndex { (response, arrayIndex) =>
								val nonAcknowledgedPermissionEntry = nonAcknowledgedQuiescencePermissionsArray(arrayIndex)
								val peerId = nonAcknowledgedPermissionEntry._1
								val configChangeIndexAssociatedToResponse = nonAcknowledgedPermissionEntry._2
								response match {
									case Failure(e) =>
										Trace.debug(s"$boundParticipantId: An attempt to permit $peerId to quiesce at $configChangeIndexAssociatedToResponse failed after $attemptsDone attempts ${if configChangeIndexAssociatedToResponse == permittedConfigChangeIndex then "" else s"(since the configuration change at $permittedConfigChangeIndex)"} with:", e)
									case _ =>
										if nonAcknowledgedQuiescencePermissions.getOrElse(peerId, 0L) == configChangeIndexAssociatedToResponse then nonAcknowledgedQuiescencePermissions.remove(peerId)
								}
							}
							if nonAcknowledgedQuiescencePermissions.nonEmpty && attemptsDone < MAX_PERMIT_QUIESCENCE_RETRIES then {
								val token = requestWakeUp(WakeUpReason.QuiescenceAuthorizationRetry, attemptsDone, () => loop(attemptsDone + 1))
								retryPermitQuiescenceWakeUpToken = Maybe(token)
							} else {
								if nonAcknowledgedQuiescencePermissions.nonEmpty then {
									Trace.warn(s"$boundParticipantId: The limit of attempts ($attemptsDone) to permit the participants $nonAcknowledgedQuiescencePermissions to quiesce at $permittedConfigChangeIndex has been reached.")
									nonAcknowledgedQuiescencePermissions.clear()
								}
								if includeMyself then currentRole.onQuiescencePermitted(boundParticipantId, permittedConfigChangeIndex)
								becomeQuiescedIfEligible(permittedConfigChangeIndex)
							}
						}
					}

					retryPermitQuiescenceWakeUpToken.foreach(_.cancel())
					peers.foreach { participantId => nonAcknowledgedQuiescencePermissions.put(participantId, permittedConfigChangeIndex) }
					loop(0)
				}
			}

			override def authorizeQuiescenceIfVanished(config: StableConfig)(using Trace.Context): Unit = {
				if config.electorate.length == 0 then {
					val excludedParticipantsExceptSelf = config.backingConfigChange.oldParticipants.union(cluster.getOtherProbableParticipants) - boundParticipantId
					authorizeQuiescenceTo(excludedParticipantsExceptSelf, config.changeIndex, true)
				}
			}


			private def startSecondPhase(tcc: TransitionalConfigChange[ParticipantId], tccIndex: RecordIndex)(using Context): sequencer.Capture[ConfigChangeResponse] = {
				for {
					isSccReplicatedToMajority <- startConfigChangeSecondPhase(tcc, tccIndex)
					response <- {
						if isSccReplicatedToMajority then sequencer.Keeper(new SUCCESSFULLY_CHANGED)
						else {
							(for primaryState1 <- primaryStateFence.causalAnchor() yield {
								new REQUEST_TRACKING_LOST_AFTER_SECOND_PHASE_STARTED(currentRole.syncLocalStateInfo(Maybe(primaryState1)).ballot)
							}): sequencer.Capture[ConfigChangeResponse]
						}
					}
				} yield response
			}

			private def replicateTccAndThenStartSecondPhase(primaryState1: PrimaryState, tcc: TransitionalConfigChange[ParticipantId], tccIndex: RecordIndex, attemptsDone: Int)(using Context): sequencer.Capture[ConfigChangeResponse] = Trace.step(() => s"replicateTccAndThenStartSecondPhase(tccIndex=$tccIndex, attempts=$attemptsDone)") {
				if currentRole ne thisLeader then {
					val ballot = currentRole.syncLocalStateInfo(Maybe(primaryState1)).ballot
					sequencer.Keeper(new REQUEST_TRACKING_LOST_AFTER_FIRST_PHASE_STARTED(ballot))
				} else {
					if assertionsEnabled then assert(primaryState1.currentTerm == leadedTerm)
					for {
						// Replicate to other participants.
						_ <- {
							driveReplicationPipelines(primaryState1)
							awaitCommitWatermark(primaryState1, tccIndex, attemptsDone)
						}
						response <- sequencer.Capture_defer(() => { // Design Tradeoff (Decoupled Mutation Contract): This manual deferral is the cost of the design decision that advanceCommitIndex to run synchronously to avoid allocations and deferral overhead on the happy path without re-entrancy bugs. Given that startSecondPhase mutates primaryStateFence, execution is explicitly deferred.
							for {
								primaryState3 <- primaryStateFence.causalAnchor()
								result <- {
									if currentRole ne thisLeader then {
										val ballot1 = currentRole.syncLocalStateInfo(Maybe(primaryState3)).ballot
										sequencer.Keeper(if commitIndex >= tccIndex then new REQUEST_TRACKING_LOST_AFTER_FIRST_PHASE_COMMITTED(ballot1) else new REQUEST_TRACKING_LOST_AFTER_FIRST_PHASE_STARTED(ballot1))
									} else {
										assert(primaryState3.currentTerm == leadedTerm && commitIndex >= tccIndex)
										startSecondPhase(tcc, tccIndex)
									}
								}
							} yield result
						})
					} yield response
				}
			}

			/** Handles configuration-change request for [[Leader]]
			 * Attempts a [[Configuration]] change, starting with the first phase and, if successful, continuing with the second. */
			override final def requestConfigChange(primaryState0: PrimaryState, requestId: ConfigChangeRequestId, desiredParticipants: Set[ParticipantId], ballotWasUpdated: Boolean)(using Context): sequencer.Capture[ConfigChangeResponse] = {
				Trace.trace(s"${if pendingConfigChangesCompletion.isPending then "Enqueuing" else "Handling"} the config change request $requestId as leader")
				pendingConfigChangesCompletion = for {
					_ <- pendingConfigChangesCompletion
					primaryState1 <- primaryStateFence.causalAnchor()
					response <- {
						if currentRole ne thisLeader then currentRole.requestConfigChange(requestId, desiredParticipants, Maybe.empty)
						else {
							val config1 = deriveConfigurationFrom(primaryState1)
							val myStateInfo1 = syncStatefulStateInfo(primaryState1)
							Trace.trace(s"StateInfo=$myStateInfo1")
							config1 match {
								case stable1: StableConfig =>
									if desiredParticipants == stable1.stableParticipants then sequencer.Keeper(new ALREADY_CHANGED)
									// Do not start a configuration transition if excluded from both, the current, and the new configuration.
									else if !stable1.isBoundIncluded && !desiredParticipants.contains(boundParticipantId) then {
										// Also, become retiring immediately if all followers have committed the excluding config change. The intention of this is to minimize the time that a participant is kept leading after it was excluded.
										if isGhostAndAllLearnersCommittedTheExcludingConfigChange then {
											assert(indexOfConfigChangeThatExcludedThisParticipant == stable1.changeIndex)
											authorizeQuiescenceIfVanished(stable1)
											become(Retiring(primaryState1.currentTerm, stable1.term, stable1.changeIndex, stable1.electorate))
												.requestConfigChange(requestId, desiredParticipants, Maybe.empty)
										}
										// If leading as a ghost and some learner hasn't committed the excluding config change, answer informing the situation.
										else sequencer.Keeper(new WAIT_GHOST_LEADER_IS_DEMOTED(myStateInfo1.ballot))
									} else {
										// start the first phase of the configuration change
										val tcc = new TransitionalConfigChange[ParticipantId](primaryState1.currentTerm, requestId, stable1.stableParticipants, desiredParticipants)
										Trace.trace(s"About to append TCC $tcc")
										for {
											// Update primary state
											primaryState3 <- primaryStateFence.advanceIf { primaryState2 =>
												if currentRole ne thisLeader then Maybe.empty
												else {
													assert(primaryState2.currentTerm == leadedTerm)
													Maybe(primaryState2.withSingleRecordAppended(tcc.term, tcc))
												}
											}
											// replicate the TransitionalConfigChange and then start the second phase.
											response <- replicateTccAndThenStartSecondPhase(primaryState3, tcc, primaryState3.firstEmptyRecordIndex - 1, 0)
										} yield response
									}

								case transitional1: TransitionalConfig =>
									// We re-evaluate state after waiting, so it must be stable unless there's a logic bug.
									// But if somehow we are here, we must not infinite loop. We'll return an error.
									sequencer.Keeper(new REQUEST_TRACKING_LOST_AFTER_FIRST_PHASE_STARTED(myStateInfo1.ballot))
							}
						}
					}
				} yield response
				pendingConfigChangesCompletion
			}

			/** Starts the second phase of a configuration change.
			 * Appends a [[StableConfigChange]] instance in the local log, stores it, and then attempts to replicate it to the participants in both, old and new configurations as if its configuration was the corresponding [[TransitionalConfigChange]].
			 * @param correspondingTransitionalConfigChange the [[TransitionalConfigChange]] that initiated the first phase of the configuration change.
			 * @return  a [[sequencer.Capture]] that yields true/false if the [[StableConfigChange]] [[Record]] was/wasn't replicated to a majority. */
			private def startConfigChangeSecondPhase(correspondingTransitionalConfigChange: TransitionalConfigChange[ParticipantId], tccIndex: RecordIndex)(using Context): sequencer.Capture[Boolean] = {
				Trace.step(() => s"startConfigChangeSecondPhase(tccIndex=$tccIndex)") {
					for {
						primaryState1 <- primaryStateFence.advanceIf { primaryState0 =>
							if currentRole ne thisLeader then Maybe.empty
							else {
								assert(primaryState0.currentTerm == leadedTerm)
								if primaryState0.indexOfLatestConfigChange > tccIndex then Maybe.empty
								else {
									val scc = new StableConfigChange[ParticipantId](primaryState0.currentTerm, correspondingTransitionalConfigChange.requestId, correspondingTransitionalConfigChange.term, correspondingTransitionalConfigChange.oldParticipants, correspondingTransitionalConfigChange.newParticipants)
									Maybe(primaryState0.withSingleRecordAppended(primaryState0.currentTerm, scc))
								}
							}
						}

						isSecondPhaseChangeReplicatedToMajority <- {
							// TODO add a coupleIndex field in StableConfigChange and use it in the next if condition instead of the requestId (whose uniqueness depends on the user).
							if primaryState1.latestConfigChange.get.requestId == correspondingTransitionalConfigChange.requestId then {
								val sccIndex = primaryState1.indexOfLatestConfigChange
								Trace.trace(s"Starting replication of SCC at $sccIndex. The corresponding TCC is $correspondingTransitionalConfigChange at $tccIndex")
								replicateSccUntilSuccessOrLeaderRoleIsAbandoned(primaryState1, sccIndex, 0)
							} else sequencer.Capture_true
						}
					} yield isSecondPhaseChangeReplicatedToMajority
				}
			}

			/** Replicates all the uncommitted records in the local log to the peers, retrying until either:
			 *  - the [[Record]]s up to the provided index are replicated to a majority ([[commitIndex]] equal or greater than the provided index).
			 *  - the [[currentRole]] stops being this [[Leader]] instance.
			 *
			 * A no-op [[LeaderTransition]] record is appended if [[Record]]s of a previous [[Term]] are blocking the [[commitIndex]] advancement due to the Raft safety rule (§5.4.2): "A leader cannot determine commitment using entries from previous terms". This constraint is implemented in [[TransitionalConfig.indexOfTheCommittableRecordWithHighestIndex]].
			 *
			 * @note Decoupled Mutation Contract: Because this method mutates the [[primaryStateFence]] upfront via [[appendNoOpRecordLocally]] when handling prior-term records, callers must NEVER invoke this method synchronously from within [[advanceCommitIndex]] or from a synchronous continuation of [[awaitCommitWatermark]] without explicitly deferring execution via [[sequencer.Capture_defer]]. */
			private def replicateSccUntilSuccessOrLeaderRoleIsAbandoned(primaryState0: PrimaryState, sccIndex: RecordIndex, attemptsDone: Int)(using Context): sequencer.Capture[Boolean] = {
				Trace.step("replicateSccUntilSuccessOrLeaderRoleIsAbandoned") {
					if currentRole ne thisLeader then sequencer.Capture_false
					else if commitIndex >= sccIndex then sequencer.Capture_true
					else {
						assert(primaryStateFence.committedState.is(primaryState0)) // Fails if the primaryStateFence was touched after yielding the PrimaryState instance received as parameter.
						assert(primaryState0.currentTerm == leadedTerm) // Assumes that the demotion due to higher term seen is always applied synchronously within a section causally ordered by the primaryStateFence. See the CausalFence's game changing invariant.
						val targetCapture: sequencer.Capture[(PrimaryState, RecordIndex)] = {
							if primaryState0.getRecordTermAt(sccIndex) == leadedTerm then {
								sequencer.Capture_ready((primaryState0, sccIndex))
							} else if primaryState0.firstEmptyRecordIndex - 1 > sccIndex && primaryState0.getRecordTermAt(primaryState0.firstEmptyRecordIndex - 1) == leadedTerm then {
								// Defensive fallback: in case this method were ever invoked after current-term entries had already been appended to the log.
								sequencer.Capture_ready((primaryState0, primaryState0.firstEmptyRecordIndex - 1))
							} else {
								// If the SCC was appended in a previous term (which happens exclusively during leader takeover in `Leader.handleEnter`), Raft §5.4.2 prohibits committing it by counting replicas alone. At the moment of takeover, the newly crowned leader has appended zero records in `leadedTerm`. Therefore, this branch executes to append a no-op `LeaderTransition` record, which is simultaneously the first, last, and only record in `leadedTerm`.
								Trace.trace(s"Appending a no-op record to be able to commit records of previous [[Term]] transitively.")
								for primaryState1 <- appendNoOpRecordLocally() yield {
									(primaryState1, primaryState1.firstEmptyRecordIndex - 1)
								}
							}
						}
						for {
							(primaryState1, targetIndex) <- targetCapture
							_ <- {
								if currentRole ne thisLeader then sequencer.Capture_ready(primaryState1)
								else {
									driveReplicationPipelines(primaryState1)
									awaitCommitWatermark(primaryState1, targetIndex, attemptsDone)
								}
							}
						} yield (commitIndex >= sccIndex) && (currentRole eq thisLeader)
					}
				}
			}

			private def appendNoOpRecordLocally()(using Context): sequencer.Capture[PrimaryState] = {
				primaryStateFence.advanceIf { primaryState =>
					if primaryState.currentTerm != leadedTerm then Maybe.empty
					else Maybe(primaryState.withSingleRecordAppended(leadedTerm, LeaderTransition(leadedTerm)))
				}
			}

			def onCommandFromClient(clientCommand: ClientCommand, attemptFlag: CommandAttemptFlag)(using Trace.Context): sequencer.Capture[ResponseToClient] = {
				class PsUpdater extends primaryStateFence.Updater[PrimaryState] {
					var commandRecordIndex: RecordIndex = 0

					override def update(primaryState0: PrimaryState): Maybe[primaryStateFence.doer.Mono[PrimaryState]] = {
						if currentRole ne thisLeader then Maybe.empty
						else {
							val currentTerm = primaryState0.currentTerm
							assert(currentTerm == leadedTerm) // Assumes that the demotion due to higher term seen is always applied synchronously within a section causally ordered by the primaryStateFence. See the CausalFence's game changing invariant.
							commandRecordIndex = primaryState0.firstEmptyRecordIndex
							Maybe(primaryState0.withSingleRecordAppended(currentTerm, CommandRecord(currentTerm, clientCommand)))
						}
					}
				}
				val psUpdater = new PsUpdater
				for {
					// First, append the command to the log if it wasn't already
					primaryState1 <- primaryStateFence.advanceIfWith(psUpdater)
					// Second, replicate it if not already, and then, if replication was successful, apply the command to the state machine assuming it is idempotent.
					response <- handleCommandReplication(primaryState1, clientCommand, psUpdater.commandRecordIndex)
				} yield response
			}

			private def handleCommandReplication(primaryState1: PrimaryState, clientCommand: ClientCommand, commandRecordIndex: RecordIndex)(using Trace.Context): sequencer.Capture[ResponseToClient] = {
				// The role may have changed due to a failure while storing the primary state. In that case, delegate the handling to the current role. The appended command record will be overwritten when the new leader calls the append records RPC.
				if currentRole ne thisLeader then currentRole.onCommandFromClient(clientCommand, INTERNAL_VACATE_HANDOFF)
				else {
					assert(primaryStateFence.committedState.is(primaryState1)) // Fails if the primaryStateFence was touched after yielding the PrimaryState instance received as parameter.
					assert(primaryState1.currentTerm == leadedTerm) // Assumes that the demotion due to higher term seen is always applied synchronously within a section causally ordered by the primaryStateFence. See the CausalFence's game changing invariant.
					val logBufferOffset1 = primaryState1.logBufferOffset
					for {
						primaryState2 <- {
							driveReplicationPipelines(primaryState1)
							awaitCommitWatermark(primaryState1, commandRecordIndex, 0)
						}
						response <- {
							// The role may have changed while attempting the replication. In that case, delegate the handling to the current role. The appended command record will be overwritten when the new leader calls the append records RPC.
							if currentRole ne thisLeader then currentRole.onCommandFromClient(clientCommand, INTERNAL_VACATE_HANDOFF)
							else {
								assert(commitIndex >= commandRecordIndex)
								for {
									_ <- decoupledCommandsApplierCompletion // Waits the committed-commands-applier to complete any work left by a previous role.
									response <- {
										// It is not necessary to have an updated primary state here because committed records are never mutated and we are not mutating the primary state here. We only need to know if we are still leading.
										if currentRole ne thisLeader then currentRole.onCommandFromClient(clientCommand, INTERNAL_VACATE_HANDOFF)
										else for {
											_ <- applyCommittedCommands(primaryState2, commandRecordIndex - 1, 0)
											response <- {
												if currentRole ne thisLeader then currentRole.onCommandFromClient(clientCommand, INTERNAL_VACATE_HANDOFF)
												else for smr <- machine.applyClientCommand(commandRecordIndex, clientCommand) yield {
													highestAppliedCommandIndex = commandRecordIndex
													if commandRecordIndex - logBufferOffset1 > logCompactionThreshold then startLogCompaction()
													Processed(commandRecordIndex, smr)
												}
											}
										} yield response
									}
								} yield response
							}
						}
					} yield response
				}
			}

			/** Triggers the continuous replication pipelines for all active and retiring learners.\
			 * Used to jump-start the replication process when new records are appended or when a configuration change occurs.
			 * @param primaryState the current primary state. */
			private def driveReplicationPipelines(primaryState: PrimaryState)(using Trace.Context): Unit = Trace.step("driveReplicationPipelines") {
				assert(primaryStateFence.committedState.is(primaryState)) // Fails if the primaryStateFence was touched after yielding the PrimaryState instance received as parameter.
				if currentRole ne thisLeader then return
				val config = deriveConfigurationFrom(primaryState)
				if currentRole ne thisLeader then return

				if config.peers.length == 0 then advanceCommitIndex(primaryState)
				else {
					config.peers.foreachWithIndex { (learnerId, learnerIndex) =>
						driveReplicationPipeline(primaryState, learnerId, learnerProgressByIndex(learnerIndex), 0, isRetiree = false)
					}
					retiringLearnersById.foreach { (learnerId, learnerProgress) =>
						driveReplicationPipeline(primaryState, learnerId, learnerProgress, 0, isRetiree = true)
					}
				}
			}

			/**
			 * Drives continuous log replication to a participant (either an active cluster peer or a retiring follower).
			 * Ensures that up to `maxInFlightAppendsPerPeer` append requests are in flight.
			 *
			 * @param primaryState0 the current primary state before anchoring.
			 * @param learnerId the ID of the participant.
			 * @param learnerProgress the state object tracking the replication progress of this participant.
			 * @param attemptsDone the number of retry attempts made due to reachability failures.
			 * @param isRetiree whether the target participant is an excluded follower undergoing retirement synchronization.
			 */
			private def driveReplicationPipeline(primaryState0: PrimaryState, learnerId: ParticipantId, learnerProgress: LearnerProgress, attemptsDone: Int, isRetiree: Boolean)(using Trace.Context): Unit = {
				assert(primaryStateFence.committedState.is(primaryState0), s"$primaryState0 != ${primaryStateFence.committedState}") // Fails if the primaryStateFence was touched after yielding the PrimaryState instance received as parameter.
				if learnerProgress.inFlightAppendCount >= maxInFlightAppendsPerPeer then return // TODO consider moving this condition to the callers site.

				val requestLeaderCommit = if isRetiree then learnerProgress.excludingSccIndex else commitIndex
				val fromIndex = learnerProgress.optimisticIndexOfNextRecordToSend
				val toIndex = if isRetiree then requestLeaderCommit + 1 else primaryState0.firstEmptyRecordIndex

				if toIndex > fromIndex then learnerProgress.optimisticIndexOfNextRecordToSend = toIndex
				// If the append would be empty and the leaderCommit parameter would be equal or less than the known been committed, then skip the append.
				else if requestLeaderCommit <= learnerProgress.highestRecordIndexKnowToBeCommitted then {
					// If also is retiring and the SCC is committed in the learner, then its retirement driving is complete.
					if isRetiree && learnerProgress.highestRecordIndexKnowToBeCommitted >= learnerProgress.excludingSccIndex then {
						retiringLearnersById.remove(learnerId)
					}
					return
				}
				// If not skipped then:
				val appendSerial = learnerProgress.lastEmittedAppendSerial + 1
				learnerProgress.lastEmittedAppendSerial = appendSerial

				val appendResultObserver = new sequencer.MonoObserver[AppendResult] with CompletionObserver[PrimaryState] { thisAppendResultObserver =>
					private var maybeAppendResult: Maybe[AppendResult] = Maybe.empty

					override def onSuccess(appendResult: AppendResult): Unit = {
						if (currentRole eq thisLeader) && (abdicateAndBumpTermIfLessThan(appendResult.term) eq thisLeader) then {
							maybeAppendResult = Maybe(appendResult)
							primaryStateFence.causalAnchor(thisAppendResultObserver)
						}
					}

					override def onError(e: Throwable): Unit = {
						Trace.debug(s"Replication with serial $appendSerial to $learnerId failed with:", e)
						if currentRole eq thisLeader then {
							maybeAppendResult = Maybe(new AppendResult_Failed(e))
							primaryStateFence.causalAnchor(thisAppendResultObserver)
						}
					}

					override def onSuccess(primaryState1: PrimaryState, originId: OriginId): Unit = {
						if currentRole eq thisLeader then {
							val appendOutcome = handleAppendResponse(learnerId, learnerProgress, maybeAppendResult.get, appendSerial, primaryState0.currentTerm, toIndex, requestLeaderCommit, isRetiree)

							if appendOutcome == AO_NEEDS_EARLIER_RECORDS then {
								driveReplicationPipeline(primaryState1, learnerId, learnerProgress, 0, isRetiree)
							} else if appendOutcome == AO_IS_UNREACHABLE then {
								val token = requestWakeUp(WakeUpReason.UnreachableFollowersRetry, attemptsDone, () => {
									for primaryState2 <- primaryStateFence.causalAnchor() do {
										if currentRole eq thisLeader then {
											driveReplicationPipeline(primaryState2, learnerId, learnerProgress, attemptsDone + 1, isRetiree)
										}
									}
								})
								learnerProgress.unreachableRetryWakeUpToken = Maybe(token)
							} else if appendOutcome == AO_SUCCESS then {
								if !isRetiree then advanceCommitIndex(primaryState1)
								if currentRole eq thisLeader then driveReplicationPipeline(primaryState1, learnerId, learnerProgress, attemptsDone, isRetiree)
							} else if appendOutcome == AO_IS_RETIRING || appendOutcome == AO_IS_QUIESCED then {
								retiringLearnersById.remove(learnerId)
								// Evaluate advanceCommitIndex to potentially trigger a ghost leader abdication.
								advanceCommitIndex(primaryState1)
							}
						}
					}

					override def onError(e: Throwable, originId: OriginId): Unit = {
						if e.isInstanceOf[GracefullyReleased] then Trace.debug(s"Replication pipeline closed due to graceful quiescence.")
						else Trace.warn(s"Replication pipeline closed due to inability to obtain the primary state after append #$appendSerial to learner $learnerId:", e)
					}
				}
				requestAppend(learnerId, primaryState0, fromIndex, toIndex, requestLeaderCommit).triggerSync(appendResultObserver)
			}

			/**
			 * Issues an append RPC (`appendRecords` or `installSnapshot`) to a specific learner peer.
			 * Automatically selects `installSnapshot` if the learner's missing records precede the `logBufferOffset`.
			 *
			 * @param learnerId the ID of the destination peer.
			 * @param primaryState0 the current state of the participant.
			 * @param fromIndex the starting index from which records should be appended.
			 * @param toIndex the index before which records should be appended.
			 * @param requestCommitIndex the index up to which the learner can commit records.
			 * @return a [[sequencer.Capture]] holding the [[AppendResult]] response from the learner.
			 */
			private def requestAppend(learnerId: ParticipantId, primaryState0: PrimaryState, fromIndex: RecordIndex, toIndex: RecordIndex, requestCommitIndex: RecordIndex): sequencer.Capture[AppendResult] = {
				assert(primaryStateFence.committedState.is(primaryState0)) // Fails if the primaryStateFence was touched after yielding the PrimaryState instance received as parameter.
				val maybeLatestSnapshot = primaryState0.latestSnapshot
				val latestSnapshotLastIncludedRecordIndex = maybeLatestSnapshot.fold(0L)(_.lastIncludedRecordIndex)
				if fromIndex >= primaryState0.logBufferOffset && requestCommitIndex >= latestSnapshotLastIncludedRecordIndex then {
					// If the required records are present in the plain records buffer, dispatch the missing slice.
					// Identify the index of the record preceding the batch to enable log consistency validation.
					val previousRecordIndex = fromIndex - 1
					// Retrieve the term of the preceding record for log matching verification.
					// Retrieve term from the plain records buffer if the preceding index falls within its bounds, or from the snapshot boundary if it is immediately before.
					val previousRecordTerm = primaryState0.getRecordTermAt(previousRecordIndex)

					val recordsToSend = if fromIndex >= toIndex then IArray.empty[Record] else primaryState0.getRecordsBetween(fromIndex, toIndex)
					// Slice the suffix of plain records starting from `fromIndex` up to `targetIndexBound` and transmit it.
					learnerId.appendRecords(
						primaryState0.currentTerm,
						previousRecordIndex,
						previousRecordTerm,
						recordsToSend,
						requestCommitIndex,
						primaryState0.getRecordTermAt(requestCommitIndex)
					)
				} else {
					// If the participant requires historical records predating the plain log buffer, install the latest snapshot and subsequent plain records.
					val recordsToSend = if primaryState0.logBufferOffset >= toIndex then IArray.empty[Record] else primaryState0.getRecordsBetween(primaryState0.logBufferOffset, toIndex)
					val clampedRequestCommitIndex = requestCommitIndex.max(latestSnapshotLastIncludedRecordIndex)
					learnerId.installSnapshot(
						primaryState0.currentTerm,
						maybeLatestSnapshot.get,
						recordsToSend,
						clampedRequestCommitIndex,
						primaryState0.getRecordTermAt(clampedRequestCommitIndex)
					)
				}
			}

			/** Analyzes the response of an append RPC and updates the `LearnerProgress` accordingly. \
			 * It computes the optimistic and pessimistic indices for the next request and adjusts the highest known replicated/committed indices. \
			 * When `isRetiree` is false, the response is evaluated for an active peer: it never yields `AO_IS_RETIRING`, and a `RETIRING` role response is rewound to receive earlier records. \
			 * When `isRetiree` is true, acknowledging or rejecting with [[roleOrdinal == RETIRING]] indicates retirement synchronization is complete (`AO_IS_RETIRING`).
			 * @note Out-of-order responses (where `appendSerial < lastReceivedAppendSerial`) are intentionally processed rather than dropped. They carry valid historical index data, and their potential to cause spurious backtracking is naturally neutralized by the monotonic watermarks (`highestRecordIndexKnownToBeAppended`). However, non-monotonic state like `respondedAsRetiring` must be explicitly guarded by `appendSerial` to prevent stale responses from overwriting the latest known state. \
			 * @param learnerId the identifier of the peer or retiring participant.
			 * @param learnerProgress the [[LearnerProgress]] instance for the participant.
			 * @param appendResult the response from the peer about the appending.
			 * @param appendSerial the serial number of the RPC request.
			 * @param appendRequestTerm the term passed to [[ClusterParticipant.appendRecords]] as `inquirerTerm` parameter.
			 * @param indexAfterTopRecordSent index of the record after the one at the top of the records passed to `appendRecords`.
			 * @param appendRequestLeaderCommit the commit index passed to the `leaderCommit` parameter.
			 * @param isRetiree whether the participant is a retiring follower rather than an active peer.
			 * @return the [[AppendOutcome]] categorizing the next step for the pipeline. */
			private def handleAppendResponse(learnerId: ParticipantId, learnerProgress: LearnerProgress, appendResult: AppendResult, appendSerial: Int, appendRequestTerm: Term, indexAfterTopRecordSent: RecordIndex, appendRequestLeaderCommit: RecordIndex, isRetiree: Boolean)(using Context): AppendOutcome = {
				appendResult match {
					case result: AppendResult_Accepted =>
						// Guard non-monotonic state updates against stale, out-of-order responses.
						if appendSerial > learnerProgress.lastReceivedAppendSerial then {
							learnerProgress.lastReceivedAppendSerial = appendSerial
							learnerProgress.respondedAsRetiring = result.roleOrdinal == RETIRING
						}

						if result.term > appendRequestTerm then AO_HAS_HIGHER_TERM
						else if result.roleOrdinal == QUIESCED then AO_IS_QUIESCED
						else if appendSerial < learnerProgress.lastReceivedAppendSerial then AO_STALE
						else {
							val highestRecordIndexKnownToBeAppended = learnerProgress.highestRecordIndexKnownToBeAppended
							if indexAfterTopRecordSent <= highestRecordIndexKnownToBeAppended then {
								if isRetiree && result.roleOrdinal == RETIRING then AO_IS_RETIRING else AO_SUCCESS
							} else {
								if appendRequestLeaderCommit > learnerProgress.highestRecordIndexKnowToBeCommitted then learnerProgress.highestRecordIndexKnowToBeCommitted = appendRequestLeaderCommit
								if indexAfterTopRecordSent > learnerProgress.pessimisticIndexOfNextRecordToSend then {
									learnerProgress.pessimisticIndexOfNextRecordToSend = indexAfterTopRecordSent
									if indexAfterTopRecordSent > learnerProgress.optimisticIndexOfNextRecordToSend then learnerProgress.optimisticIndexOfNextRecordToSend = indexAfterTopRecordSent
								}
								val indexOfTopRecordSent = indexAfterTopRecordSent - 1
								if indexOfTopRecordSent > highestRecordIndexKnownToBeAppended then learnerProgress.highestRecordIndexKnownToBeAppended = indexOfTopRecordSent
								if isRetiree && result.roleOrdinal == RETIRING then AO_IS_RETIRING else AO_SUCCESS
							}
						}

					case result: AppendResult_Rejected =>
						// Guard non-monotonic state updates against stale, out-of-order responses.
						if appendSerial > learnerProgress.lastReceivedAppendSerial then {
							learnerProgress.lastReceivedAppendSerial = appendSerial
							learnerProgress.respondedAsRetiring = result.roleOrdinal == RETIRING
						}
						if result.term > appendRequestTerm then AO_HAS_HIGHER_TERM
						else if result.roleOrdinal == QUIESCED then AO_IS_QUIESCED
						else if appendSerial < learnerProgress.lastReceivedAppendSerial then AO_STALE
						else {
							val highestRecordIndexKnownToBeAppended = learnerProgress.highestRecordIndexKnownToBeAppended
							if indexAfterTopRecordSent <= highestRecordIndexKnownToBeAppended then {
								if isRetiree && result.roleOrdinal == RETIRING then AO_IS_RETIRING else AO_SUCCESS
							} else {
								val learnerFirstEmptyRecordIndex = result.firstEmptyRecordIndex
								if isRetiree && result.roleOrdinal == RETIRING then {
									val retireeExcludingConfigIndex = learnerFirstEmptyRecordIndex - 1
									learnerProgress.highestRecordIndexKnowToBeCommitted = retireeExcludingConfigIndex
									learnerProgress.pessimisticIndexOfNextRecordToSend = learnerFirstEmptyRecordIndex
									learnerProgress.optimisticIndexOfNextRecordToSend = learnerFirstEmptyRecordIndex
									learnerProgress.highestRecordIndexKnownToBeAppended = retireeExcludingConfigIndex
									AO_IS_RETIRING
								} else {
									val indexForNextAttempt = learnerFirstEmptyRecordIndex
									learnerProgress.pessimisticIndexOfNextRecordToSend = indexForNextAttempt
									learnerProgress.optimisticIndexOfNextRecordToSend = indexForNextAttempt
									val highestActual = indexForNextAttempt - 1
									if learnerProgress.highestRecordIndexKnownToBeAppended > highestActual then {
										learnerProgress.highestRecordIndexKnownToBeAppended = highestActual
									}
									AO_NEEDS_EARLIER_RECORDS
								}
							}
						}

					case result: AppendResult_Failed =>
						if appendSerial > learnerProgress.lastReceivedAppendSerial then {
							learnerProgress.lastReceivedAppendSerial = appendSerial
							Trace.debug(s"$boundParticipantId: The replication to $learnerId failed with:", result.error)
							learnerProgress.optimisticIndexOfNextRecordToSend = learnerProgress.pessimisticIndexOfNextRecordToSend
							AO_IS_UNREACHABLE
						} else AO_STALE
				}
			}

			/** Updates the actual [[commitIndex]] based on the highest record index that has been replicated during the leaded term to a quorum of learners, and returns the updated [[Configuration]].\
			 * If the commit index advances, this method notifies listeners, resolves any pending awaiters in `pendingRecordBecomesCommittedAwaiters`, and checks if the leader itself has been excluded from the cluster (becoming a ghost leader) to trigger quiescence.
			 * @param primaryState0 the current state of the participant.			 * @return the updated [[Configuration]] derived from the provided [[PrimaryState]]. */
			private def advanceCommitIndex(primaryState0: PrimaryState)(using Context): Unit = {
				assert(primaryStateFence.committedState.is(primaryState0), s"$primaryState0 != ${primaryStateFence.committedState}") // Fails if the primaryStateFence was touched after yielding the PrimaryState instance received as parameter.
				var config0A = deriveConfigurationFrom(primaryState0)

				var keepLooping = true
				while keepLooping do {
					if isGhostAndAllLearnersCommittedTheExcludingConfigChange then {
						authorizeQuiescenceIfVanished(config0A.asInstanceOf[StableConfig])
						val termOfConfigChangeThatExcludedThisParticipant = primaryState0.getRecordTermAt(indexOfConfigChangeThatExcludedThisParticipant) // This is safe (no IndexOutOfBoundsException) because `Leader.requestConfigChange` prevents transitions while leading as ghost, and the `PrimaryState.getRecordTermAt` checks the `snapshot.latestConfigChangeIndex`.
						become(Retiring(leadedTerm, termOfConfigChangeThatExcludedThisParticipant, indexOfConfigChangeThatExcludedThisParticipant, config0A.electorate))
						return
					}

					val previousCommitIndex = commitIndex
					val newCommitIndex = config0A.indexOfTheCommittableRecordWithHighestIndex(primaryState0, previousCommitIndex, learnerProgressByIndex)
					if newCommitIndex == previousCommitIndex then {
						keepLooping = false
					} else {
						commitIndex = newCommitIndex
						notifyListeners(_.onCommitIndexChanged(previousCommitIndex, newCommitIndex, LEADER, primaryState0.currentTerm))

						val newConfig = deriveConfigurationFrom(primaryState0)
						if newConfig ne config0A then config0A = newConfig
						else keepLooping = false
					}
				}

				// We partition the awaiters in-place to avoid allocating an intermediate array.
				// This groups all fulfilled awaiters at the end of the collection (from `partitionIdx` to the end).
				var i = 0
				var partitionIdx = pendingRecordBecomesCommittedAwaiters.length
				while i < partitionIdx do {
					val awaiter = pendingRecordBecomesCommittedAwaiters(i)
					if commitIndex >= awaiter.targetIndex then {
						partitionIdx -= 1
						pendingRecordBecomesCommittedAwaiters(i) = pendingRecordBecomesCommittedAwaiters(partitionIdx)
						pendingRecordBecomesCommittedAwaiters(partitionIdx) = awaiter
					} else {
						i += 1
					}
				}

				// We iterate backwards to remove and execute the fulfilled awaiters.
				// This reverse iteration is completely safe against re-entrance (where `captureSync` synchronously adds or clears awaiters):
				// - If an observer synchronously adds a new awaiter, it is appended at the end of the buffer. Our `remove(j)` safely 
				//   shifts that new awaiter leftward without us processing it prematurely or corrupting the iteration.
				// - If an observer synchronously clears the buffer (e.g., an abdication triggers `handleExit`), the `j < length` 
				//   check gracefully prevents an IndexOutOfBoundsException.
				//
				// Furthermore, an outer rescan loop is no longer necessary after notifying observers because:
				// 1. State-mutating observers are decoupled (e.g., via `Capture_defer` or `sequencer.run`), so `captureSync` 
				//    does not synchronously trigger pipeline evaluations or advance the commit index.
				// 2. Even if a new awaiter were added synchronously, `awaitCommitWatermark` fast-paths and returns instantly
				//    if its `targetIndex` is <= `commitIndex`, meaning immediately-fulfillable awaiters are never added to this collection anyway.
				var j = pendingRecordBecomesCommittedAwaiters.length - 1
				while j >= partitionIdx do {
					if j < pendingRecordBecomesCommittedAwaiters.length then {
						val awaiter = pendingRecordBecomesCommittedAwaiters(j)
						pendingRecordBecomesCommittedAwaiters.remove(j)
						awaiter.captureSync(primaryState0)
					}
					j -= 1
				}

				assert(primaryStateFence.committedState.is(primaryState0), "An observer of the awaiter mutated the primary state synchronously, violating the decoupled mutation contract.")
			}

			/** Captures an asynchronous event that completes when the `commitIndex` reaches or exceeds the specified target index.\
			 * If the current `commitIndex` is already greater than or equal to `targetIndex`, it returns a synchronous pre-completed capture.\
			 * Otherwise, it registers a `Captor` in the `pendingRecordBecomesCommittedAwaiters` collection which will be fulfilled by `advanceCommitIndex`.\
			 *
			 * @note Commitment Barrier Invariant: This method functions strictly as a commit-watermark barrier. It never completes while
			 * the leader remains in office unless `commitIndex >= targetIndex` (e.g. `commitIndex >= tccIndex` or `commitIndex >= commandRecordIndex`).
			 * Consequently, when this capture resolves while `currentRole eq thisLeader`, `commitIndex >= targetIndex` is already guaranteed
			 * to be true. It never completes with a failure or uncommitted status while leading; if quorum is lost, it remains suspended
			 * until the leader exits office (where awaiters are seized in [[handleExit]]).
			 *
			 * @param targetIndex the record index to wait for.
			 * @return a [[sequencer.Capture]] that resolves to the [[PrimaryState]] when the commit index reaches the target. */
			private def awaitCommitWatermark(primaryState: PrimaryState, targetIndex: RecordIndex, attemptsDone: Int)(using Context): sequencer.Capture[PrimaryState] = Trace.step(() => s"awaitCommitWatermark($targetIndex)") {
				if commitIndex >= targetIndex then sequencer.Capture_ready(primaryState)
				else {
					val awaiter = new CommitIndexAwaiter(targetIndex)
					pendingRecordBecomesCommittedAwaiters.addOne(awaiter)
					awaiter
				}
			}

			/** @return true if this leading participant is a ghost leader (not included in the active [[Configuration]]) and all the learners have committed the SCC that excluded this leader (turning it into a ghost).
			 * @note that for the result of this operation be reliable, the [[PrimaryState]] should have stayed constant since the last call to [[deriveConfigurationFrom]]. */
			private def isGhostAndAllLearnersCommittedTheExcludingConfigChange: Boolean = {
				indexOfConfigChangeThatExcludedThisParticipant > 0
					&& learnerProgressByIndex.forall(_.highestRecordIndexKnowToBeCommitted >= indexOfConfigChangeThatExcludedThisParticipant)
					&& retiringLearnersById.forall(_._2.highestRecordIndexKnowToBeCommitted >= indexOfConfigChangeThatExcludedThisParticipant)
			}

			def abdicateAndBumpTermIfLessThan(seenTerm: Term)(using Context): Role = {
				Trace.step("handoffAndBumpTermIfLessThan") {
					if thisLeader.leadedTerm < seenTerm then {
						Trace.trace(s"About to hand-off due to a higher term seen.")
						become(Abdicating(thisLeader.leadedTerm, seenTerm, primaryStateFence))
					} else thisLeader
				}
			}

			override def onTermUpdated(primaryState: PrimaryState, maybeLeaderId: Maybe[ParticipantId])(using Context): PrimaryState = {
				Trace.step("onTermBumped") {
					Trace.trace(s"About to hand-off due to term bump.")
					become(maybeLeaderId.fold(Isolated(primaryStateFence))(Follower(primaryState.currentTerm, _, primaryStateFence)))
					primaryState
				}
			}
		}

		//// LearnerProgress ////

		/** Defined to prevent the compiler from generating a synthetic companion. */
		private inline def LearnerProgress(firstEmptyRecordIndex: RecordIndex): LearnerProgress = new LearnerProgress(firstEmptyRecordIndex)

		/**
		 * Tracks the progress of replication to a specific learner peer within the continuous stream pipeline.
		 *
		 * @param firstEmptyRecordIndex the initial log index from which the leader will begin replicating.
		 */
		private final class LearnerProgress(firstEmptyRecordIndex: RecordIndex) {
			/** The index of the next record to send to the peer assuming the in-flight appends will fail.
			 * This is the index of the highest record for which an append result hasn't been received, successful or not.\
			 * Expresses the lower bound of unacknowledged log entries (where replication resumes if an in-flight attempt fails or a rejection occurs).
			 * Optimistically initialized to the first empty record index of the leader's workspace for all participants, assuming each follower's log is already up-to-date with the leader's log.
			 * If a follower's log is actually behind or inconsistent, this index is decremented upon rejection until logs align.
			 * @note TODO: Consider initializing it with the first empty record index unless the last filled ones are configuration changes, in which case initialize with the index of the first of them. Sending extra [[ConfigChange]] instances is cheap and may avoid rejections due to need of an earlier [[Record]]. */
			var pessimisticIndexOfNextRecordToSend: RecordIndex = firstEmptyRecordIndex
			/** The index of the next record to send to the peer assuming the in-flight appends will succeed.
			 * If a follower's log is actually behind or inconsistent, this index is decremented upon rejection until logs align.
			 * @note TODO: Consider initializing it with the first empty record index unless the last filled ones are configuration changes, in which case initialize with the index of the first of them. Sending extra [[ConfigChange]] instances is cheap and may avoid rejections due to need of an earlier [[Record]]. */
			var optimisticIndexOfNextRecordToSend: RecordIndex = pessimisticIndexOfNextRecordToSend
			/** The highest record index known to be replicated to the peer.
			 * This is the index of the highest record for which a successful append result hasn't been received.\
			 * Conservatively initialized to 0 at the start of the leader's term to ensure the leader does not overestimate follower replication state and retries appends if necessary. */
			var highestRecordIndexKnownToBeAppended: RecordIndex = 0
			/** The highest record index known to be committed by the peer.
			 * Conservatively initialized to 0 at the start of the leader's term to ensure the leader does not overestimate follower replication state and only advances commitIndex when a true majority is confirmed. */
			var highestRecordIndexKnowToBeCommitted: RecordIndex = 0
			/** Monotonic serial number of the last append RPC emitted to this peer. */
			var lastEmittedAppendSerial: Int = 0
			/** Monotonic serial number of the last append RPC response processed for this peer. */
			var lastReceivedAppendSerial: Int = 0
			/** The active timer token used for delaying retries to unreachable learners. */
			var unreachableRetryWakeUpToken: Maybe[WakeUpToken] = Maybe.empty
			/** True if the last append response from this peer indicated that it is in the RETIRING role. */
			var respondedAsRetiring: Boolean = false
			/** The index of the `StableConfigChange` that excludes this learner, or 0 if it is not retiring. */
			var excludingSccIndex: RecordIndex = 0

			/** @return true if the learner is retiring (i.e., its excluding index is greater than 0). */
			inline def isRetiring: Boolean = excludingSccIndex > 0

			/** @return the current number of in-flight append requests sent to this peer. */
			inline def inFlightAppendCount: Int = lastEmittedAppendSerial - lastReceivedAppendSerial
		}

		//// Retirement driver ////
	
		/** Attempts to transition this participant to the [[QUIESCED]] role.\
		 * This check is performed whenever a potential prerequisite for quiescence is met (e.g., is retiring, a retirement pipeline finishes, or permission to quiesce is granted).\
		 * The transition only proceeds if the participant is in the [[RETIRING]] role, no retirement pipeline is active, and protocol permission was granted.\
		 * Three independent async processes must converge: (a) all retirement pipelines must complete and be removed from `retiringLearnersById`, (b) role must be RETIRING, (c) quiescence permission must be granted. And that these are fulfilled by different mechanisms (driveReplicationPipeline, become(Retiring), authorizeQuiescenceTo). */
		private def becomeQuiescedIfEligible(indexOfExcludingConfigChange: RecordIndex)(using Trace.Context): Unit = {
			Trace.step("becomeQuiescedIfEligible") {
				if retiringLearnersById.isEmpty && nonAcknowledgedQuiescencePermissions.isEmpty && currentRole.ordinal == RETIRING && indexOfExcludingConfigChange <= indexOfStableConfigChangeForWhichQuiescenceWasPermitted
				then become(Quiesced(Success(s"The incoming leader ${quiescenceGrantor.value} authorized quiescence and no retirement driver exists.")))
			}
		}

		//// PRIMARY STATE ////

		/** $suppressSyntheticCompanionObject */
		private inline final def PrimaryState(workspace: WS): PrimaryState = new PrimaryState(workspace)

		/** A view of the participant’s current primary state (log, term, etc.), and also trivially derived state.
		 *
		 * IMPORTANT: the [[PrimaryState]] subtype of this trait exposes mutable state. To preserve causal ordering:
		 * - All writes must occur inside an updater passed to [[StatefulRole.primaryStateFence.advance]].
		 * - All reads must occur either inside said updater or in a consumer subscribed to [[StatefulRole.primaryStateFence.causalAnchor]].
		 *
		 * Direct mutation or observation of [[PrimaryState]] outside these mechanisms breaks causal guarantees.
		 */

		/** Defines the [[PrimaryState]] when this [[ConsensusParticipant]] has access to the [[Storage]] where the primary state is persisted. */
		private final class PrimaryState(@publicInBinary protected val workspace: WS) { thisPrimaryState =>

			/** The [[Term]] of this [[PrimaryState]]. Must be immutable because it is accessed after updates of the [[Workspace]]. */
			val currentTerm: Term = workspace.getCurrentTerm

			/** The participant id this participant voted for in [[currentTerm]]. */
			val votedFor: Maybe[ParticipantId] = workspace.getVotedFor

			/** Index of the first empty record in the log. Must be immutable because it is accessed after updates of the [[Workspace]].
			 * This is trivially derived state. */
			val firstEmptyRecordIndex: RecordIndex = workspace.firstEmptyRecordIndex

			private var _indexOfLatestConfigChange: RecordIndex = 0
			private var _maybeLatestConfigChange: Maybe[ConfigChange[ParticipantId]] = Maybe.empty

			{
				val offset = workspace.logBufferOffset
				var index = workspace.firstEmptyRecordIndex
				var record: Record | Null = null
				while index > offset && {
					index -= 1
					record = workspace.getRecordAt(index)
					!record.isInstanceOf[ConfigChange[ParticipantId] @unchecked]
				} do ()
				record match {
					case cc: ConfigChange[ParticipantId] @unchecked =>
						_indexOfLatestConfigChange = index
						_maybeLatestConfigChange = Maybe(cc)
					case _ =>
						if workspace.latestSnapshot.isDefined then {
							val snapshot = workspace.latestSnapshot.get
							_indexOfLatestConfigChange = snapshot.latestConfigChangeIndex
							_maybeLatestConfigChange = Maybe(snapshot.latestConfigChange)
						}
				}
			}

			/** CAUTION: This method accesses mutable state of the [[PrimaryState]], so it should be called either:
			 * - within an [[StatefulRole.primaryStateFence.advance]] section. In this case, the call must occur before or during the completion of the [[sequencer.Capture]] returned by the function passed	to `advance`; once that task has completed, the causal fence is closed and later calls are unsafe.
			 * - within a causally anchored consumer (i.e. consumers subscribed to the [[sequencer.Capture]] returned by either [[StatefulRole.primaryStateFence.advance]] or [[StatefulRole.primaryStateFence.causalAnchor]]). In this case, the call must occur synchronously during the consumer’s execution; it must not be deferred to code scheduled after the consumer has returned, since such deferred code would no longer be causally anchored. */
			def getRecordAt(index: RecordIndex): Record = {
				if index >= logBufferOffset then workspace.getRecordAt(index)
				else latestSnapshot.fold(throw IndexOutOfBoundsException(s"Record at index $index is below lower bound 1.")) { snapshot =>
					if index == snapshot.latestConfigChangeIndex then snapshot.latestConfigChange
					else throw IndexOutOfBoundsException(s"Record at index $index is below logBufferOffset=$logBufferOffset and is not the latest config change.")
				}
			}


			/** CAUTION: This method accesses mutable state of the [[PrimaryState]], so it should be called either:
			 * - within an [[StatefulRole.primaryStateFence.advance]] section. In this case, the call must occur before or during the completion of the [[sequencer.Capture]] returned by the function passed	to `advance`; once that task has completed, the causal fence is closed and later calls are unsafe.
			 * - within a causally anchored consumer (i.e. consumers subscribed to the [[sequencer.Capture]] returned by either [[StatefulRole.primaryStateFence.advance]] or [[StatefulRole.primaryStateFence.causalAnchor]]). In this case, the call must occur synchronously during the consumer’s execution; it must not be deferred to code scheduled after the consumer has returned, since such deferred code would no longer be causally anchored. */
			def getRecordTermAt(index: RecordIndex): Term = {
				if index >= logBufferOffset then getRecordAt(index).term
				else if index == 0 then PRE_INIT
				else latestSnapshot.fold(throw IndexOutOfBoundsException(s"Record's term at index $index is below lower bound zero.")) { snapshot =>
					if index == snapshot.latestConfigChangeIndex then snapshot.latestConfigChange.term
					else if index == snapshot.lastIncludedRecordIndex then snapshot.lastIncludedRecordTerm
					else throw IndexOutOfBoundsException(s"Record at index $index is below logBufferOffset=$logBufferOffset and is not the latest config change")
				}
			}

			/** CAUTION: This method accesses mutable state of the [[PrimaryState]], so it should be called either:
			 * - within an [[StatefulRole.primaryStateFence.advance]] section. In this case, the call must occur before or during the completion of the [[sequencer.Capture]] returned by the function passed	to `advance`; once that task has completed, the causal fence is closed and later calls are unsafe.
			 * - within a causally anchored consumer (i.e. consumers subscribed to the [[sequencer.Capture]] returned by either [[StatefulRole.primaryStateFence.advance]] or [[StatefulRole.primaryStateFence.causalAnchor]]). In this case, the call must occur synchronously during the consumer’s execution; it must not be deferred to code scheduled after the consumer has returned, since such deferred code would no longer be causally anchored. */
			inline def getRecordsBetween(from: RecordIndex, until: RecordIndex): IArray[Record] =
				workspace.getRecordsBetween(from, until)

			/** CAUTION: This method accesses mutable state of the [[PrimaryState]], so it should be called either:
			 * - within an [[StatefulRole.primaryStateFence.advance]] section. In this case, the call must occur before or during the completion of the [[sequencer.Capture]] returned by the function passed	to `advance`; once that task has completed, the causal fence is closed and later calls are unsafe.
			 * - within a causally anchored consumer (i.e. consumers subscribed to the [[sequencer.Capture]] returned by either [[StatefulRole.primaryStateFence.advance]] or [[StatefulRole.primaryStateFence.causalAnchor]]). In this case, the call must occur synchronously during the consumer’s execution; it must not be deferred to code scheduled after the consumer has returned, since such deferred code would no longer be causally anchored. */
			inline def logBufferOffset: RecordIndex =
				workspace.logBufferOffset

			/** CAUTION: This method accesses mutable state of the [[PrimaryState]], so it should be called either:
			 * - within an [[StatefulRole.primaryStateFence.advance]] section. In this case, the call must occur before or during the completion of the [[sequencer.Capture]] returned by the function passed	to `advance`; once that task has completed, the causal fence is closed and later calls are unsafe.
			 * - within a causally anchored consumer (i.e. consumers subscribed to the [[sequencer.Capture]] returned by either [[StatefulRole.primaryStateFence.advance]] or [[StatefulRole.primaryStateFence.causalAnchor]]). In this case, the call must occur synchronously during the consumer’s execution; it must not be deferred to code scheduled after the consumer has returned, since such deferred code would no longer be causally anchored. */
			inline def indexOfLatestConfigChange: RecordIndex =
				_indexOfLatestConfigChange

			/** CAUTION: This method accesses mutable state of the [[PrimaryState]], so it should be called either:
			 * - within an [[StatefulRole.primaryStateFence.advance]] section. In this case, the call must occur before or during the completion of the [[sequencer.Capture]] returned by the function passed	to `advance`; once that task has completed, the causal fence is closed and later calls are unsafe.
			 * - within a causally anchored consumer (i.e. consumers subscribed to the [[sequencer.Capture]] returned by either [[StatefulRole.primaryStateFence.advance]] or [[StatefulRole.primaryStateFence.causalAnchor]]). In this case, the call must occur synchronously during the consumer’s execution; it must not be deferred to code scheduled after the consumer has returned, since such deferred code would no longer be causally anchored. */
			inline def latestConfigChange: Maybe[ConfigChange[ParticipantId]] =
				_maybeLatestConfigChange

			/** CAUTION: This method mutates the [[PrimaryState]], so it should be called within the safe temporal windows provided by [[StatefulRole.primaryStateFence.advance]]. */
			def withTermUpdated(newTerm: Term)(using Trace.Context): sequencer.Capture[PrimaryState] = {
				if newTerm <= workspace.getCurrentTerm then sequencer.Keeper(thisPrimaryState)
				else {
					workspace.setTermAndVote(newTerm, Maybe.empty)
					saveWorkspace()
				}
			}

			/** CAUTION: This method mutates the [[PrimaryState]], so it should be called within the safe temporal windows provided by [[StatefulRole.primaryStateFence.advance]]. */
			def withTermAndVoteUpdated(newTerm: Term, newVotedFor: Maybe[ParticipantId])(using Trace.Context): sequencer.Capture[PrimaryState] = {
				if newTerm < workspace.getCurrentTerm then sequencer.Keeper(thisPrimaryState)
				else if newTerm == workspace.getCurrentTerm && newVotedFor == workspace.getVotedFor then sequencer.Keeper(thisPrimaryState)
				else {
					workspace.setTermAndVote(newTerm, newVotedFor)
					saveWorkspace()
				}
			}

			/** CAUTION: This method mutates the [[PrimaryState]], so it should be called within the safe temporal windows provided by [[StatefulRole.primaryStateFence.advance]]. */
			def withSingleRecordAppended(term: Term, record: Record)(using Trace.Context): sequencer.Capture[PrimaryState] = {
				if term > workspace.getCurrentTerm then workspace.setTermAndVote(term, Maybe.empty)
				else workspace.setCurrentTerm(term)
				workspace.appendRecord(record)
				saveWorkspace()
			}

			/** Tries to fuse the provided batch of [[Records]] with the local log.
			 * @param term the [[Term]] of the provider of the batch of records.
			 * @param prevRecordIndex the index of the [[Record]] immediately before the first entry in the batch.
			 * @param prevRecordTerm the term of the [[Record]] immediately before the first entry in the batch.
			 * @param batch the [[Record]]s to fuse.
			 * @param reportReceptacle a [[FusionReport]] to be mutated by this method to communicate what happened during the update: The [[FusionReport.isFused]] is set if a record was fused; the [[FusionReport.isTermUpdated]] is set if the local term was updated.
			 * @return a [[Maybe]] containing either:
			 *  - a [[sequencer.Capture]] that yields the updated [[PrimaryState]] after successfully saving it in the [[Storage]];
			 *  - a failed [[sequencer.Capture]] if the saving failed;
			 *  - nothing ([[Maybe.empty]]) if either earlier [[Record]]s are needed, the term mismatches, or the batch fully predates the latest snapshot. */
			def tryFusingRecords(term: Term, prevRecordIndex: RecordIndex, prevRecordTerm: Term, batch: IArray[Record], reportReceptacle: FusionReport)(using Trace.Context): Maybe[sequencer.Capture[PrimaryState]] = {
				var isMutated = false // memorizes if the workspace is mutated.

				if term > workspace.getCurrentTerm then {
					isMutated = true
					workspace.setTermAndVote(term, Maybe.empty)
					reportReceptacle.isTermUpdated = true
				}

				var indexInBatchOfFirstRecordToFuse: Int = 0
				// 1. Verify consistency assuming the Log Matching invariant holds.
				val isConsistent = {
					if prevRecordIndex >= thisPrimaryState.workspace.logBufferOffset then prevRecordIndex < firstEmptyRecordIndex && prevRecordTerm == workspace.getRecordAt(prevRecordIndex).term
					else workspace.latestSnapshot.fold {
						prevRecordIndex == 0 && prevRecordTerm == PRE_INIT
					} { snapshot =>
						indexInBatchOfFirstRecordToFuse = (snapshot.lastIncludedRecordIndex - prevRecordIndex).toInt
						if indexInBatchOfFirstRecordToFuse == 0 then prevRecordTerm == snapshot.lastIncludedRecordTerm
						else indexInBatchOfFirstRecordToFuse <= batch.length && batch(indexInBatchOfFirstRecordToFuse - 1).term == snapshot.lastIncludedRecordTerm
					}
				}

				if isConsistent then {
					reportReceptacle.isFused = true
					var readIndex = indexInBatchOfFirstRecordToFuse
					var compareIndex = prevRecordIndex + indexInBatchOfFirstRecordToFuse + 1
					while readIndex < batch.length && compareIndex < workspace.firstEmptyRecordIndex && {
						if workspace.getRecordAt(compareIndex).term == batch(readIndex).term then true
						else {
							isMutated = true
							workspace.truncateSuffix(compareIndex)
							false
						}
					} do {
						readIndex += 1
						compareIndex += 1
					}
					if readIndex < batch.length then {
						isMutated = true
						while {
							workspace.appendRecord(batch(readIndex))
							readIndex += 1
							readIndex < batch.length
						} do ()
					} else {
						if compareIndex <= commitIndex then compareIndex = commitIndex + 1
						if compareIndex < workspace.firstEmptyRecordIndex && workspace.getRecordAt(compareIndex).term < term then {
							isMutated = true
							workspace.truncateSuffix(compareIndex)
						}
					}
				}

				if isMutated then Maybe(saveWorkspace()) else Maybe.empty
			}

			/** Truncates the log's by replacing the earlier records (up to and including the provided [[RecordIndex]]) with the provided snapshot.\
			 * The snapshot must be taken immediately after the last [[ClientCommand]] of the removed records was applied.\
			 * CAUTION: This method mutates the [[PrimaryState]] so it should be called within the safe temporal windows provided by [[StatefulRole.primaryStateFence.advance]]. */
			def withLogTruncated(term: Term, lastIncludedRecordIndex: RecordIndex, stateMachineSnapshot: IArray[Byte])(using Trace.Context): sequencer.Capture[PrimaryState] = {
				if term > workspace.getCurrentTerm then workspace.setTermAndVote(term, Maybe.empty)
				else workspace.setCurrentTerm(term)
				val lastIncludedRecordTerm = getRecordTermAt(lastIncludedRecordIndex)

				val snapshot = if _indexOfLatestConfigChange <= lastIncludedRecordIndex then {
					new SnapshotData[ParticipantId](lastIncludedRecordIndex, lastIncludedRecordTerm, _maybeLatestConfigChange.get, _indexOfLatestConfigChange, stateMachineSnapshot)
				} else {
					var relativeIndex = lastIncludedRecordIndex
					while relativeIndex >= workspace.logBufferOffset && !workspace.getRecordAt(relativeIndex).isInstanceOf[ConfigChange[ParticipantId] @unchecked] do relativeIndex -= 1

					if relativeIndex >= workspace.logBufferOffset then {
						val cc = workspace.getRecordAt(relativeIndex).asInstanceOf[ConfigChange[ParticipantId]]
						new SnapshotData[ParticipantId](lastIncludedRecordIndex, lastIncludedRecordTerm, cc, relativeIndex, stateMachineSnapshot)
					} else {
						val cc = workspace.latestSnapshot.get.latestConfigChange
						val ccIndex = workspace.latestSnapshot.get.latestConfigChangeIndex
						new SnapshotData[ParticipantId](lastIncludedRecordIndex, lastIncludedRecordTerm, cc, ccIndex, stateMachineSnapshot)
					}
				}

				workspace.truncatePrefix(snapshot)
				saveWorkspace()
			}

			/** Replaces the whole log's with the provided snapshot followed with the provided records.\
			 * The snapshot must have been taken immediately after the last [[ClientCommand]] of the removed records was applied.\
			 * CAUTION: This method mutates the [[PrimaryState]] so it should be called within the safe temporal windows provided by [[StatefulRole.primaryStateFence.advance]]. */
			def withLogReplaced(term: Term, snapshot: SnapshotData[ParticipantId], tailRecords: IArray[Record])(using Trace.Context): sequencer.Capture[PrimaryState] = {
				workspace.resetLog(snapshot, tailRecords)
				if term > workspace.getCurrentTerm then workspace.setTermAndVote(term, Maybe.empty)
				else workspace.setCurrentTerm(term)
				saveWorkspace()
			}

			/** CAUTION: This method accesses mutable state of the [[PrimaryState]], so it should be called either:
			 * - within an [[StatefulRole.primaryStateFence.advance]] section. In this case, the call must occur before or during the completion of the [[sequencer.Capture]] returned by the function passed	to `advance`; once that task has completed, the causal fence is closed and later calls are unsafe.
			 * - within a causally anchored consumer (i.e. consumers subscribed to the [[sequencer.Capture]] returned by either [[StatefulRole.primaryStateFence.advance]] or [[StatefulRole.primaryStateFence.causalAnchor]]). In this case, the call must occur synchronously during the consumer’s execution; it must not be deferred to code scheduled after the consumer has returned, since such deferred code would no longer be causally anchored. */
			inline def latestSnapshot: Maybe[SnapshotData[ParticipantId]] =
				workspace.latestSnapshot


			/** CAUTION: This method mutates of the [[PrimaryState]], so it should be called within the safe temporal windows provided by [[StatefulRole.primaryStateFence.advance]]. */
			def withWorkspaceReleased(): sequencer.Capture[PrimaryState] = {
				workspace.release()
				sequencer.Failed(new GracefullyReleased)
			}

			/** Saves the [[Workspace]] of this [[PrimaryState]] in the [[Storage]].
			 * CAUTION: This method accesses mutable state of the [[PrimaryState]], so it should be called within an [[StatefulRole.primaryStateFence.advance]] section only.
			 * @return the [[sequencer.Capture]] that yields the saved [[PrimaryState]] */
			private def saveWorkspace()(using Trace.Context): sequencer.Capture[PrimaryState] = {
				new sequencer.Captor[PrimaryState] with sequencer.MonoObserver[Unit] { thisCaptor =>
					{ // Constructor
						storage.save(workspace).triggerSync(thisCaptor)
					}

					override def onSuccess(a: Unit): Unit = {
						if currentRole.isInstanceOf[StatefulRole] then thisCaptor.captureSync(new PrimaryState(workspace))
						else {
							workspace.release()
							thisCaptor.trapSync(new IllegalStateException(s"Illegal role ${RoleOrdinal_nameOf(currentRole.ordinal)}. Workspace released"))
						}
					}

					override def onError(e: Throwable): Unit = {
						Trace.error(s"$boundParticipantId: Unexpected error while saving the workspace. This participant's consensus service is unable to continue following the leader and will quiesce.", e)
						become(Quiesced(Failure(e)))
						workspace.release() // just in case storage.save does not do it.
						thisCaptor.trapSync(e)
					}
				}
			}

			override def toString: String = s"PrimaryState(currentTerm=$currentTerm, votedFor=$votedFor, firstEmptyRecordIndex=$firstEmptyRecordIndex, indexOfTopConfigChange=$indexOfLatestConfigChange)"
		}

		//// CONFIGURATION ////

		/** Knows which are the participants involved in the consensus and defines rules pertaining to elections and replication that govern the participant's behavior.\
		 * It has exactly two concrete subclasses:
		 *  - [[TransitionalConfig]]: Active during joint consensus (`Cold` ∪ `Cnew`).\
		 *     This behavior begins immediately once the transitional entry is appended to the participant's log.\
		 *     Replication and election quorum require majorities across both `Cold` and `Cnew`.\
		 *     Transitional behavior remains active until the corresponding [[StableConfigChange]] entry is replicated to a majority of both `Cold` and `Cnew` ([[commitIndex]] >= indexOfCorrespondingStableConfigChange).
		 *  - [[StableConfig]]: Active during non-joint consensus (Cnew).\
		 *     This behavior begins only once the backing [[StableConfigChange]] entry is committed (i.e. when [[commitIndex]] ≥ indexOfBackingStableConfigChange).\
		 *     Replication and election quorum require the majority of `Cnew` only.\
		 *     Non-leader `Cold` only participants having committed the [[StableConfigChange]] entry that excluded them, do transition to [[Retiring]] and wait authorization to quiesce from a stable leader of a succeeding term.\
		 *     A leader `Cold` only participant having committed the [[StableConfigChange]] entry that excluded it, do stay leading as ghost leader until either: it sees all the other participants had commited said [[StableConfigChange]]; or it receives either an "append records" or "authorization to quiesce" RPC from a leader of a higher term.\
		 *
		 * **Commit Index Dependency**
		 *  - [[TransitionalConfig]] behavior starts on append of its [[backingConfigChange]], but ends when the stable entry is committed.
		 *  - [[StableConfig]] behavior starts only when its [[backingConfigChange]] is committed.
		 *
		 * Thus, despite each of these two behaviors depend only on primary state (the log), the whole configuration behavior is commit-sensitive because the transition instant between them depends on [[commitIndex]]. */
		private sealed trait Configuration {
			/** The [[Term]] of the backing [[ConfigChange]]. */
			val term: Term
			/** The index of the backing [[ConfigChange]]. */
			val changeIndex: RecordIndex
			/** The participants that conform the electorate for consensus and elections. Contains the same elements as the [[electorate]] array. */
			val activeParticipants: Set[ParticipantId]
			/** The current set of participants involved in the consensus, sorted. Contains the same elements as the [[activeParticipants]] set. */
			val electorate: IArray[ParticipantId]
			/** The current set of participants involved in the consensus, excluding the bound participant, sorted. */
			val peers: IArray[ParticipantId]
			/** True when the bound participant is part of the [[electorate]]. */
			val isBoundIncluded: Boolean

			/** The [[ConfigChange]] that caused this [[Configuration]] and on which it is based.
			 * A [[ConfigChange]]s is a snapshots, so it contain all the information needed. */
			val backingConfigChange: ConfigChange[ParticipantId]

			/** The identifiers of the stable participants. Equivalent to [[backingConfigChange.newParticipants]]. */
			val stableParticipants: Set[ParticipantId]

			inline def isInNew(participantId: ParticipantId): Boolean = stableParticipants.contains(participantId)

			/** The set of participants to include in [[Unable]] responses. */
			def otherProbableParticipants: ListSet[ParticipantId]

			def reachedAll(vote: Vote[ParticipantId]): Boolean

			def reachedAMajority(vote: Vote[ParticipantId]): Boolean

			def hasMajorityAppended(index: RecordIndex, learnerProgressByIndex: IArray[LearnerProgress]): Boolean

			def indexOfTheCommittableRecordWithHighestIndex(primaryState: PrimaryState, from: RecordIndex, learnerProgressByIndex: IArray[LearnerProgress]): RecordIndex


			/** Determines the [[Role]] to become based on the votes of all the participants. */
			def determineRole(primaryState: PrimaryState, primaryStateFence: CausalFence[PrimaryState, sequencer.type], myVote: Vote[ParticipantId], peerVotes: Array[Try[Vote[ParticipantId]]])(using Trace.Context): Maybe[Role]

			/**
			 * Determines the best leader candidate based on the [[StateInfo]]s of all the participants, including itself.
			 * This method only queries. Does not mutate anything.
			 *
			 * @param peersStateInfos the answers to the [[ClusterParticipant.howAreYou]] questions done to the other participants, stored as a parallel array with index correspondence [[peers]].
			 * @return A task that yields a [[Vote]] with the chosen leader for the current term.
			 */
			def decideMyVote(myStateInfo: StateInfo, peersStateInfos: IArray[StateInfo | Null])(using Trace.Context): Maybe[Vote[ParticipantId]]

			/** @return the index of the provided [[ParticipantId]] in the [[peers]]' [[IndexedSeq]] or a negative number if not present.
			 * @param peerId the id of the peer to find. */
			inline def peerIndexOf(peerId: ParticipantId): Int = {
				java.util.Arrays.binarySearch(peers.asInstanceOf[Array[ParticipantId]], peerId, participantIdComparator)
			}

			override def toString: String = backingConfigChange.toString
		}

		//// StableConfig ////

		/** Pure new configuration (`Cnew`).
		 *
		 * Activated only once the [[StableConfigChange]] entry is committed (commitIndex ≥ indexOfStableConfigChange).
		 * Replication and quorum reduce to `Cnew` only.
		 * Cold-only servers, having seen this entry, shut down.
		 */
		private final class StableConfig(override val backingConfigChange: StableConfigChange[ParticipantId], override val changeIndex: RecordIndex) extends Configuration {
			override val term: Term = backingConfigChange.term
			override val activeParticipants: Set[ParticipantId] = backingConfigChange.activeParticipants
			override val electorate: IArray[ParticipantId] = IArray.unsafeFromArray(activeParticipants.toArray.sorted)
			private val halfTheNumberOfParticipants = electorate.length / 2
			override val peers: IArray[ParticipantId] = electorate.filter(_ != boundParticipantId)
			override val isBoundIncluded: Boolean = activeParticipants.contains(boundParticipantId)

			override val stableParticipants: Set[ParticipantId] = backingConfigChange.newParticipants

			def otherProbableParticipants: ListSet[ParticipantId] = {
				ListSet.newBuilder
					.addAll(peers)
					.addAll(cluster.getOtherProbableParticipants)
					.result()
			}

			override def reachedAll(vote: Vote[ParticipantId]): Boolean = {
				vote.reachableCommonCount == electorate.length
			}

			override def reachedAMajority(vote: Vote[ParticipantId]): Boolean = {
				vote.reachableCommonCount > halfTheNumberOfParticipants || electorate.length == 0
			}

			override def hasMajorityAppended(index: RecordIndex, learnerProgressByIndex: IArray[LearnerProgress]): Boolean = {
				val othersQuorumThreshold = if isBoundIncluded then halfTheNumberOfParticipants else halfTheNumberOfParticipants + 1
				learnerProgressByIndex.countWithIndex((learnerProgress, _) => learnerProgress.highestRecordIndexKnownToBeAppended >= index) >= othersQuorumThreshold
			}

			override def indexOfTheCommittableRecordWithHighestIndex(primaryState: PrimaryState, from: RecordIndex, learnerProgressByIndex: IArray[LearnerProgress]): RecordIndex = {
				assert(from >= primaryState.latestSnapshot.fold(0: RecordIndex)(_.lastIncludedRecordIndex), s"from=$from, snapshot=${primaryState.latestSnapshot}")
				var n = primaryState.firstEmptyRecordIndex
				while {
					n -= 1
					// The second condition of this expression enforces Raft §5.4.2 ("A leader cannot determine commitment using entries from previous terms") and that a leader inheriting previous-term records cannot commit them without first committing a current-term record
					n > from && (primaryState.getRecordTermAt(n) != primaryState.currentTerm || !hasMajorityAppended(n, learnerProgressByIndex))
				} do ()
				n
			}


			override def determineRole(primaryState: PrimaryState, primaryStateFence: CausalFence[PrimaryState, sequencer.type], myVote: Vote[ParticipantId], peerVotes: Array[Try[Vote[ParticipantId]]])(using Trace.Context): Maybe[Role] = {
				var votesMatchingMyVoteCount = 1 // includes my vote
				var newParticipantsJoining = 0
				for case Success(replierVote) <- peerVotes do {
					if replierVote.votedId == myVote.votedId && replierVote.term == myVote.term && replierVote.isNonBlank then votesMatchingMyVoteCount += 1
					else if replierVote.votedRank == ER_JOINER then newParticipantsJoining += 1
				}
				if votesMatchingMyVoteCount + newParticipantsJoining <= halfTheNumberOfParticipants then Isolated(primaryStateFence)
				else if myVote.votedId == boundParticipantId then Promoting(primaryState.currentTerm, primaryStateFence)
				else Follower(primaryState.currentTerm, myVote.votedId, primaryStateFence)
			}

			override def decideMyVote(myStateInfo: StateInfo, peersStateInfos: IArray[StateInfo | Null])(using Trace.Context): Maybe[Vote[ParticipantId]] = {
				Trace.step(() => s"${this.toString}.decideMyVote") {
					var participantsCount = 0

					var contender_id = boundParticipantId
					var contender_stateInfo = myStateInfo
					val decider = new CandidateDecider(contender_id, contender_stateInfo, true)
					var contender_index = peers.length
					while contender_index >= 0 do {
						if contender_stateInfo.rank != ER_NONE then participantsCount += 1

						var goNext = true
						contender_index -= 1
						// navigate to the next successfully replied StateInfo and contend it.
						while contender_index >= 0 && goNext do {
							peersStateInfos(contender_index) match {
								case null =>
									contender_index -= 1
								case peerStateInfo: StateInfo =>
									contender_id = peers(contender_index)
									contender_stateInfo = peerStateInfo
									decider.contend(contender_id, contender_stateInfo, true)
									goNext = false
							}
						}
					}
					decider.castVote(participantsCount, 0, myStateInfo.ballot)
				}
			}
		}

		//// TransitionalConfig ////

		/** Joint consensus configuration (Cold ∪ Cnew).
		 *
		 * Activated immediately upon append of the transitional entry.
		 * Replication and quorum require majorities across both Cold and Cnew.
		 * Elections must also consider both sets.
		 * Ends when a [[StableConfig]] entry is committed.
		 */
		private final class TransitionalConfig(override val backingConfigChange: TransitionalConfigChange[ParticipantId], override val changeIndex: RecordIndex) extends Configuration {
			private val oldParticipants: Set[ParticipantId] = backingConfigChange.oldParticipants
			private val newParticipants: Set[ParticipantId] = backingConfigChange.newParticipants
			private val halfOfOldParticipants: Int = oldParticipants.size / 2
			private val halfOfNewParticipants: Int = newParticipants.size / 2
			override val term: Term = backingConfigChange.term
			override val activeParticipants: Set[ParticipantId] = backingConfigChange.activeParticipants
			override val electorate: IArray[ParticipantId] = IArray.unsafeFromArray(activeParticipants.toArray.sorted)
			override val peers: IArray[ParticipantId] = electorate.filter(_ != boundParticipantId)
			override val isBoundIncluded: Boolean = activeParticipants.contains(boundParticipantId)

			override val stableParticipants: Set[ParticipantId] = newParticipants

			def otherProbableParticipants: ListSet[ParticipantId] = {
				ListSet.newBuilder
					.addAll(peers)
					.addAll(cluster.getOtherProbableParticipants)
					.result()
			}

			override def reachedAll(vote: Vote[ParticipantId]): Boolean = {
				vote.reachableCommonCount == oldParticipants.size && vote.reachableTargetCount == newParticipants.size
			}

			override def reachedAMajority(vote: Vote[ParticipantId]): Boolean = {
				(vote.reachableCommonCount > halfOfOldParticipants || oldParticipants.isEmpty)
					&& (vote.reachableTargetCount > halfOfNewParticipants || newParticipants.isEmpty)
			}

			override def hasMajorityAppended(index: RecordIndex, learnerProgressByIndex: IArray[LearnerProgress]): Boolean = {
				var oldParticipantsWithRecordAtN = 0
				var newParticipantsWithRecordAtN = 0

				var participantId = boundParticipantId
				var otherParticipantIndex = peers.length
				while otherParticipantIndex >= 0 do {
					if oldParticipants.contains(participantId) then oldParticipantsWithRecordAtN += 1
					if newParticipants.contains(participantId) then newParticipantsWithRecordAtN += 1

					var goNext = true
					otherParticipantIndex -= 1
					while otherParticipantIndex >= 0 && goNext do {
						participantId = peers(otherParticipantIndex)
						if learnerProgressByIndex(otherParticipantIndex).highestRecordIndexKnownToBeAppended >= index
							|| (learnerProgressByIndex(otherParticipantIndex).respondedAsRetiring && !newParticipants.contains(participantId))
						then goNext = false
						else otherParticipantIndex -= 1
					}
				}
				(oldParticipantsWithRecordAtN > halfOfOldParticipants || oldParticipants.isEmpty) && (newParticipantsWithRecordAtN > halfOfNewParticipants || newParticipants.isEmpty)
			}

			override def indexOfTheCommittableRecordWithHighestIndex(primaryState: PrimaryState, from: RecordIndex, learnerProgressByIndex: IArray[LearnerProgress]): RecordIndex = {
				assert(from >= primaryState.latestSnapshot.fold(0: RecordIndex)(_.lastIncludedRecordIndex), s"from=$from, snapshot=${primaryState.latestSnapshot}")
				var n = primaryState.firstEmptyRecordIndex - 1
				while n > from do {
					// The first condition of this `if` enforces Raft §5.4.2 ("A leader cannot determine commitment using entries from previous terms") and that a leader inheriting previous-term records cannot commit them without first committing a current-term record
					if primaryState.getRecordTermAt(n) == primaryState.currentTerm && hasMajorityAppended(n, learnerProgressByIndex) then return n
					n -= 1
				}
				from
			}


			override def determineRole(primaryState: PrimaryState, primaryStateFence: CausalFence[PrimaryState, sequencer.type], myVote: Vote[ParticipantId], peerVotes: Array[Try[Vote[ParticipantId]]])(using Trace.Context): Maybe[Role] = {
				var oldParticipantsVotesMatchingMyVote = 0
				var newParticipantsVotesMatchingMyVote = 0
				var oldParticipantsRetiring = 0
				var newParticipantsJoining = 0

				var participantVote = myVote
				var participantId = boundParticipantId
				var participantIndex = peerVotes.length
				while participantIndex >= 0 do {
					if participantVote.votedId == myVote.votedId && participantVote.term == myVote.term && participantVote.isNonBlank then {
						if oldParticipants.contains(participantId) then oldParticipantsVotesMatchingMyVote += 1
						if newParticipants.contains(participantId) then newParticipantsVotesMatchingMyVote += 1
					} else {
						if participantVote.votedRank == ER_RETIREE && oldParticipants.contains(participantId) then oldParticipantsRetiring += 1
						if participantVote.votedRank == ER_JOINER && newParticipants.contains(participantId) then newParticipantsJoining += 1
					}
					var goNext = true
					participantIndex -= 1
					// navigate to the next successful vote and get it.
					while participantIndex >= 0 && goNext do {
						peerVotes(participantIndex) match {
							case s: Success[Vote[ParticipantId]] =>
								participantVote = s.value
								participantId = peers(participantIndex)
								goNext = false

							case _: Failure[Vote[ParticipantId]] =>
								participantIndex -= 1
						}
					}
				}

				if (oldParticipantsVotesMatchingMyVote + oldParticipantsRetiring <= halfOfOldParticipants && oldParticipants.nonEmpty) || (newParticipantsVotesMatchingMyVote + newParticipantsJoining <= halfOfNewParticipants && newParticipants.nonEmpty) then Isolated(primaryStateFence)
				else if myVote.votedId == boundParticipantId then Promoting(primaryState.currentTerm, primaryStateFence)
				else Follower(primaryState.currentTerm, myVote.votedId, primaryStateFence)
			}

			override def decideMyVote(myStateInfo: StateInfo, peersStateInfos: IArray[StateInfo | Null])(using Trace.Context): Maybe[Vote[ParticipantId]] = {
				Trace.step(() => s"${this.toString}.decideMyVote") {
					var oldParticipantsCount = 0
					var newParticipantsCount = 0

					var contender_id = boundParticipantId
					var contender_stateInfo = myStateInfo
					var contender_isInOldSet = oldParticipants.contains(boundParticipantId)
					val decider = new CandidateDecider(contender_id, contender_stateInfo, contender_isInOldSet)
					var contender_index = peers.length
					while contender_index >= 0 do {
						if contender_isInOldSet && contender_stateInfo.rank != ER_NONE then oldParticipantsCount += 1
						if contender_stateInfo.rank != ER_NONE && newParticipants.contains(contender_id) then newParticipantsCount += 1

						var goNext = true
						contender_index -= 1
						// navigate to the next successfully replied StateInfo and contend it.
						while contender_index >= 0 && goNext do {
							peersStateInfos(contender_index) match {
								case null =>
									contender_index -= 1

								case peerStateInfo: StateInfo =>
									contender_id = peers(contender_index)
									contender_stateInfo = peerStateInfo
									contender_isInOldSet = oldParticipants.contains(contender_id)
									decider.contend(contender_id, contender_stateInfo, contender_isInOldSet)
									goNext = false
							}
						}
					}
					decider.castVote(oldParticipantsCount, newParticipantsCount, myStateInfo.ballot)
				}
			}
		}

		private def Configuration_from(configChange: ConfigChange[ParticipantId], changeIndex: RecordIndex): Configuration = {
			configChange match {
				case cc: TransitionalConfigChange[ParticipantId] =>
					new TransitionalConfig(cc, changeIndex)
				case cc: StableConfigChange[ParticipantId] =>
					new StableConfig(cc, changeIndex)
			}
		}

		//// UTILITIES USED BY MANY BEHAVIORS

		private class CandidateDecider(voterId: ParticipantId, voterStateInfo: StateInfo, voterIsInCommonSet: Boolean) {
			private var chosenId = voterId
			private var chosenInfo = voterStateInfo
			private var chosenIsInCommonSet = voterIsInCommonSet
			private var mostCompleteInfo = voterStateInfo
			private val borrame: ArrayBuffer[(ParticipantId, StateInfo, Boolean)] = ArrayBuffer((voterId, voterStateInfo, voterIsInCommonSet)) //TODO delete line

			def contend(otherId: ParticipantId, otherInfo: StateInfo, otherIsInCommonConfig: Boolean): Unit = {
				val incumbentId = chosenId
				val incumbentInfo = chosenInfo
				val incumbentIsInCommonConfig = chosenIsInCommonSet
				borrame.addOne((otherId, otherInfo, otherIsInCommonConfig)) //TODO delete line

				val theOtherWins =
					if incumbentInfo.currentTerm > otherInfo.currentTerm then false
					else if incumbentInfo.currentTerm < otherInfo.currentTerm then true
					else if incumbentInfo.rank == ER_LEADING && otherInfo.rank != ER_LEADING then false
					else if incumbentInfo.rank != ER_LEADING && otherInfo.rank == ER_LEADING then true
					else {
						val completenessComparison = incumbentInfo.compareCompleteness(otherInfo)
						if completenessComparison > 0 then false
						else if completenessComparison < 0 then true
						else if incumbentInfo.rank == ER_CANDIDATE && otherInfo.rank != ER_CANDIDATE then false
						else if incumbentInfo.rank != ER_CANDIDATE && otherInfo.rank == ER_CANDIDATE then true
						else if incumbentIsInCommonConfig && !otherIsInCommonConfig then false
						else if !incumbentIsInCommonConfig && otherIsInCommonConfig then true
						else if incumbentId < otherId then false
						else true
					}
				if theOtherWins then {
					chosenId = otherId
					chosenInfo = otherInfo
					chosenIsInCommonSet = otherIsInCommonConfig
				}
				if otherInfo.compareCompleteness(mostCompleteInfo) > 0 then mostCompleteInfo = otherInfo
			}

			def castVote(reachableCommonParticipants: Int, reachableTargetParticipants: Int, ballot: Ballot)(using Trace.Context): Maybe[Vote[ParticipantId]] = {
				val ci = chosenInfo
				val castedVote =
					if ci.compareCompleteness(mostCompleteInfo) >= 0 then Maybe(Vote(voterStateInfo.currentTerm, chosenId, reachableCommonParticipants, reachableTargetParticipants, ci.rank, ballot))
					else Maybe.empty
				Trace.debug(s"castedVote=$castedVote, contestants=$borrame") //TODO delete line
				castedVote
			}
		}

		/**
		 * Asks the [[Configuration.peers]] how they are ([[ClusterParticipant.howAreYou]]) in a coalesced manner: If an equivalent question is in flight, reuses the same pending [[sequencer.Capture]] of the in-flight question; otherwise, a new request is done.
		 * Supports the forcing of answers.
		 *
		 * @param participantsIds the [[ParticipantId]]s of the target participants.
		 * @param stateInfo the [[StateInfo]] to put in the inquires.
		 * @param forcedAnswerByParticipantId the forced answers indexed by [[ParticipantId]].
		 * @return An [[IndexedSeq]] containing a [[sequencer.Capture]] for each [[ParticipantId]] in the provided array. Each [[sequencer.Capture]] element is the one returned by [[ClusterParticipant.howAreYou]] applied to the corresponding [[ParticipantId]] in the provided array, except the corresponding to the provided `idOfExcludedParticipant`, which yield the provided [[StateInfo]].
		 */
		private def askHowOtherParticipantsAre(participantsIds: IArray[ParticipantId], stateInfo: StateInfo, forcedAnswerByParticipantId: java.util.Map[ParticipantId, StateInfo]): IArray[sequencer.Capture[StateInfo]] = {
			participantsIds.mapWithIndex { (participantId, _) =>
				forcedAnswerByParticipantId.get(participantId) match {
					case null => coalescedHowAreYou.getOrStart((participantId, stateInfo), true)
					case forcedAnswer: StateInfo => sequencer.Capture_ready(forcedAnswer)
				}
			}
		}

		private final def illegalStateQuiesce(detail: String = "")(using Trace.Context): Role = {
			Trace.step("illegalStateQuiesce") {
				val failure = new IllegalStateException(s"Should never happen. $detail")
				Trace.error(s"Should never happen", failure)
				become(Quiesced(Failure(failure)))
			}
		}

		//// NOTIFICATIONS

		final def subscribe(listener: NotificationListener): Unit = {
			checkWithin()
			notificationListeners.put(listener, None)
		}

		final def unsubscribe(listener: NotificationListener): Boolean = {
			checkWithin()
			notificationListeners.remove(listener) eq None
		}

		/** @param notification a function that receives a [[NotificationListener]] and calls one of its methods. */
		private def notifyListeners(notifier: NotificationListener => Unit)(using Trace.Context): Unit = {
			checkWithin()
			notificationListeners.forEach { (listener, _) =>
				try notifier(listener)
				catch {
					case NonFatal(e) => Trace.error(s"$boundParticipantId: A notification listener threw:", e)
				}
			}
		}

		//// Just for efficiency ////

		@threadUnsafe private lazy val _emptyCapture: sequencer.Capture[Maybe[AnyRef]] = sequencer.Keeper(Maybe.empty)

		/** An already completed [[sequencer.Capture]] that yields [[Maybe.empty]].
		 * CAUTION: This @threadUnsafe lazy val does not guarantee a unique instance under concurrent access. Its use is only safe for logic that depends on the value's data, not its object identity. */
		private inline final def emptyCapture[A]: sequencer.Capture[Maybe[A]] = _emptyCapture.asInstanceOf[sequencer.Capture[Maybe[A]]]

		/** $suppressSyntheticCompanionObject */
		private inline final def Leader(trap: Nothing): Any = trap

		/** $suppressSyntheticCompanionObject */
		private inline final def CandidateInfo(trap: Nothing): Any = trap

		/** $suppressSyntheticCompanionObject */
		private inline final def StableConfig(trap: Nothing): Any = trap

		/** $suppressSyntheticCompanionObject */
		private inline final def TransitionalConfig(trap: Nothing): Any = trap

		/** $suppressSyntheticCompanionObject */
	}
}
