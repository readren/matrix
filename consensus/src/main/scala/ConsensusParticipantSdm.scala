package readren.consensus

import readren.common.*
import readren.common.Trace.Context
import readren.sequencer.{CoalescedQuery, Doer, ResultIncrementalCoalescing}

import java.util
import java.util.Comparator
import scala.annotation.threadUnsafe
import scala.collection.immutable.{ArraySeq, ListSet}
import scala.collection.mutable.ArrayBuffer
import scala.collection.{mutable, IndexedSeq as GenIndexedSeq}
import scala.compiletime.asMatchable
import scala.math.Ordering.Implicits.infixOrderingOps
import scala.reflect.ClassTag
import scala.runtime.IntRef
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

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

	final type Ballot = Int

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
		def <(other: RoleOrdinal): Boolean = thisRoleOrdinal < other
		def <=(other: RoleOrdinal): Boolean = thisRoleOrdinal <= other
		def >(other: RoleOrdinal): Boolean = thisRoleOrdinal > other
		def >=(other: RoleOrdinal): Boolean = thisRoleOrdinal >= other
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
	/** The [[ElectionRank]] of roles that are ineligible, do not participante in elections (vote for themselves with term=0), and don't reduce the quorum threshold. */
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

	final val ERS_COUNT_IN_OLD: ElectionRanksSet = ER_RETIREE | ER_CANDIDATE | ER_LEADING
	final val ERS_COUNT_IN_NEW: ElectionRanksSet = ER_JOINER | ER_CANDIDATE | ER_LEADING

	inline def ElectionRank_from(ordinal: RoleOrdinal): ElectionRank = (ordinal & 0xFC).toByte

	extension (rank: ElectionRank) inline def in(mask: ElectionRanksSet): Boolean = (rank & mask) != 0

	def ElectionRank_nameOf(rank: ElectionRank): String = {
		rank match {
			case ER_NONE => "NONE"
			case ER_RETIREE => "RETIREE"
			case ER_JOINER => "JOINER"
			case ER_CANDIDATE => "CANDIDATE"
			case ER_LEADING => "LEADING"
		}
	}

	trait ConfigChangeResponse {
		val latestBallotSeen: Ballot
	}

	/** The requested configuration change was successfully completed. Only participants with the [[LEADER]] role answer this. */
	class SUCCESSFULLY_CHANGED(override val latestBallotSeen: Ballot) extends ConfigChangeResponse {
		override def toString: String = deriveToString[SUCCESSFULLY_CHANGED](this)
	}

	/** The participant is leading and already has the requested configuration. */
	class ALREADY_CHANGED(override val latestBallotSeen: Ballot) extends ConfigChangeResponse {
		override def toString: String = deriveToString[ALREADY_CHANGED](this)
	}

	/** The participant is leading and already transitioning to the requested configuration.
	 * TODO: consider avoiding this result and coalesce with the in-flight change. */
	class ALREADY_IN_PROGRESS(override val latestBallotSeen: Ballot) extends ConfigChangeResponse {
		override def toString: String = deriveToString[ALREADY_IN_PROGRESS](this)
	}

	/** The participant is leading but is currently processing another change to a configuration different from the requested.
	 * TODO: consider avoiding this result and coalesce with the in-flight change. */
	class WAIT_PREVIOUS_CHANGE_TO_COMPLETE(override val latestBallotSeen: Ballot) extends ConfigChangeResponse {
		override def toString: String = deriveToString[WAIT_PREVIOUS_CHANGE_TO_COMPLETE](this)
	}

	/** The participant is leading but excluded (leading as a ghost). A ghost leader can not initiate configuration changes. This condition will last until either: a participant in the new configuration becomes leader and calls the append records RPC on this participant by means of a retirement driver; or this participant sees that all the participants in the new configuration have commited the [[StableConfigChange]] that excluded this participant; whichever happens first. */
	class WAIT_GHOST_LEADER_IS_DEPOTED(override val latestBallotSeen: Ballot) extends ConfigChangeResponse {
		override def toString: String = deriveToString[WAIT_GHOST_LEADER_IS_DEPOTED](this)
	}

	/** The participant is a follower. So, it suggests to redirect the participant it is following. */
	class ASK_THE_LEADER(leaderId: AnyRef, override val latestBallotSeen: Ballot) extends ConfigChangeResponse {
		override def toString: String = deriveToString[ASK_THE_LEADER](this)
	}

	/** The participant is catching-up because it is joining. */
	class CATCHING_UP(override val latestBallotSeen: Ballot) extends ConfigChangeResponse {
		override def toString: String = deriveToString[CATCHING_UP](this)
	}

	/** The tracking of the [[Configuration]] change request was lost due to a leader change after the first phase was started. The process may complete or not depending on which participant is promoted. If completed, the [[ConsensusParticipantSdm.ClusterParticipant.notifyActiveConfigChanged]] is called. If not, just silence. // TODO avoid the mentioned silence. */
	class REQUEST_TRACKING_LOST_AFTER_FIRST_PHASE_STARTED(override val latestBallotSeen: Ballot) extends ConfigChangeResponse {
		override def toString: String = deriveToString[REQUEST_TRACKING_LOST_AFTER_FIRST_PHASE_STARTED](this)
	}

	/** The tracking of the [[Configuration]] change request was lost due to a leader change after the first phase was committed (replicated to majority). The process will continue provided the system is sufficiently incited by client commands or further configuration change requests. Listen to [[ConsensusParticipantSdm.ClusterParticipant.notifyActiveConfigChanged]] calls to observe when the process completes. */
	class REQUEST_TRACKING_LOST_AFTER_FIRST_PHASE_COMMITED(override val latestBallotSeen: Ballot) extends ConfigChangeResponse {
		override def toString: String = deriveToString[REQUEST_TRACKING_LOST_AFTER_FIRST_PHASE_COMMITED](this)
	}

	/** The tracking of the [[Configuration]] change request was lost due to a leader change after the second phase was started. The process will continue anyway provided the system is sufficiently incited by client commands or further configuration change requests. Listen to [[ConsensusParticipantSdm.ClusterParticipant.notifyActiveConfigChanged]] calls to observe when the process completes. */
	class REQUEST_TRACKING_LOST_AFTER_SECOND_PHASE_STARTED(override val latestBallotSeen: Ballot) extends ConfigChangeResponse {
		override def toString: String = deriveToString[REQUEST_TRACKING_LOST_AFTER_SECOND_PHASE_STARTED](this)
	}

	/** The participant was excluded by a previous call to [[ConsensusParticipantSdm.ClusterParticipant.Delegate.requestConfigChange]]. */
	class EXCLUDED(override val latestBallotSeen: Ballot) extends ConfigChangeResponse {
		override def toString: String = deriveToString[EXCLUDED](this)
	}

	/** The participant is up but secluded from the majority at this moment. */
	class SECLUDED(override val latestBallotSeen: Ballot) extends ConfigChangeResponse {
		override def toString: String = deriveToString[SECLUDED](this)
	}

	/** The participant is [[QUIESCED]] or not able to access to its primary state. */
	class STOPPED(override val latestBallotSeen: Ballot) extends ConfigChangeResponse {
		override def toString: String = deriveToString[STOPPED](this)
	}

	/** Informs a participant receiving a command about the outcome of the client's previous attempt to send that command to the consensus group.
	 * This flag typically refers to an attempt made toward a different [[ConsensusParticipantSdm.ConsensusParticipant]] and informs something about the resulting [[ConsensusParticipantSdm.ResponseToClient]].
	 * The client is responsible of setting the appropriate value of this flag. See the values documentation, below. */
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
	 * @note This flag must be propagated through the client and participants until a new leader is established. Note that bitwise or-ing the this value with [[FALLBACK]] returns this value. */
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

	//// STANDALONE DATA TYPES

	/**
	 * A vote for a leader.
	 * @see [[ConsensusParticipantSdm.ClusterParticipant.chooseALeader]] and [[ConsensusParticipantSdm.ClusterParticipant.Delegate.onChooseALeader]].
	 * @tparam Id The identifier type for participants in the consensus cluster.
	 *            This must match the concrete type used to implement [[ConsensusParticipantSdm.ParticipantId]].
	 *            It allows the user to customize how participants are identified (e.g., UUID, String, custom class), while preserving type safety across [[Vote]] instances exchanges.
	 *            Although [[Vote]] is designed to travel between participants, it remains path-dependent and must be instantiated within a module that resolves [[ParticipantId]] to a concrete type.
	 * @param term The term for which the vote is cast.
	 * @param votedId The id of the voted candidate.
	 * @param reachableCandidatesOfOldConf The number of candidates of the current or old set of participants that were reachable by the voter (including the voter itself) when the vote was cast. Zero means the voter is still initializing or was quiesced.
	 * @param reachableCandidatesOfNewConf The number of candidates new the new set or participants that were reachable by the voter (including the voter itself) when the vote was cast. Zero means the voter is still initializing or was quiesced.
	 * @param rank The [[ElectionRank]] of the [[Role]] of the voted candidate.
	 * @param ballot the election round to which this [[Vote]] belongs to, or zero to indicate this [[Vote]] is blank.
	 */
	final case class Vote[Id <: AnyRef](term: Term, votedId: Id, reachableCandidatesOfOldConf: Int, reachableCandidatesOfNewConf: Int, rank: ElectionRank, ballot: Ballot) {
		inline def isBlank: Boolean = ballot == 0
		inline def isNonBlank: Boolean = ballot > 0
		override def toString: String = s"Vote(term=$term, votedId=$votedId, reachOld=$reachableCandidatesOfOldConf, reachNew=$reachableCandidatesOfNewConf, rank=${ElectionRank_nameOf(rank)}, ballot=$ballot)"
	}

	/** The result of an append operation.
	 * @see [[ConsensusParticipantSdm.ClusterParticipant.appendRecords]] and [[ConsensusParticipantSdm.ClusterParticipant.Delegate.onAppendRecords]].
	 * @param term The term of the follower that is responding. A value greater than the append-request's [[Term]] indicates a rejection, makes the `successOrIndexForNextAttempt` field irrelevant, and, more importantly, indicate that the inquirer has an obsolete state.
	 * @param successOrIndexForNextAttempt when relevant, a zero value indicates the appending was successful, and a [[RecordIndex]] indicates a rejection due to earlier records needed from the provided index.
	 * @param roleOrdinal The role of the follower that is responding. A [[QUIESCED]] or [[RETIRING]] value indicates a rejection and makes the other fields irrelevant.
	 */
	final case class AppendResult(term: Term, successOrIndexForNextAttempt: RecordIndex, roleOrdinal: RoleOrdinal) {
		if assertionsEnabled then assert(roleOrdinal != PROMOTING)

		override def toString: String = s"AppendResult(@$term, ${if successOrIndexForNextAttempt == 0 then "accepted" else s"rejected, firstEmptyRecordIndex=$successOrIndexForNextAttempt"}, ${RoleOrdinal_nameOf(roleOrdinal)}"
	}

	/**
	 * Information that a participant exposes about itself for the purpose of leader election.
	 * Other participants require this data to decide both their vote and their own role.
	 * This information is exposed not only on demand in the response to the question [[ConsensusParticipantSdm.ClusterParticipant.howAreYou]], but also proactively in some questions.
	 * @param currentTerm The term of the participant that.
	 * @param rank The [[ElectionRank]] of the [[ConsensusParticipantSdm.ConsensusParticipant.Role]] of the participant.
	 * @param termAtCommitIndex The term of the last committed record in the log of the participant that is answering.
	 * @param commitIndex The index of the last committed record in the log of the participant that is answering.
	 * @param configIndex The index of the active [[ConfigChange]].
	 * @param ballot the election round to which this [[StateInfo]] belongs to.
	 * TODO add something that changes when the active configuration changes, like its index.
	 */
	final case class StateInfo(currentTerm: Term, rank: ElectionRank, termAtCommitIndex: Term, commitIndex: RecordIndex, configIndex: RecordIndex, ballot: Ballot) {
		if assertionsEnabled then assert(currentTerm >= termAtCommitIndex)

		/** @return true if this and the other istance are equal ignoring the [[ballot]]. */
		inline def isTyingWith(other: StateInfo): Boolean = {
			tiesWith(other.currentTerm, other.rank, other.termAtCommitIndex, other.commitIndex, other.configIndex)
		}

		/** @return true if this instance fields match the provided values. Note that the [[ballot]] is not considered. */
		inline def tiesWith(currentTerm: Term, rank: ElectionRank, termAtCommitIndex: Term, commitIndex: RecordIndex, configIndex: RecordIndex): Boolean = {
			this.commitIndex == commitIndex && this.rank == rank && this.configIndex == configIndex && this.currentTerm == currentTerm && this.termAtCommitIndex == termAtCommitIndex
		}

		override def toString: String = s"StateInfo(@$currentTerm, ${ElectionRank_nameOf(rank)}, termAtCommitIndex=$termAtCommitIndex, commitIndex=$commitIndex, configIndex=$configIndex, ballot=$ballot)"
	}

	//// LOG RECORD

	sealed trait Record {
		def term: Term
	}

	private[consensus] final case class CommandRecord[+C <: AnyRef](override val term: Term, command: C) extends Record

	private[consensus] final case class LeaderTransition(override val term: Term) extends Record

	private[consensus] final case class SnapshotPoint(override val term: Term) extends Record

	sealed trait ConfigChange[P <: AnyRef] extends Record {
		val requestId: ConfigChangeRequestId
		val oldParticipants: Set[P]
		val newParticipants: Set[P]

		def isActive(participantId: P): Boolean
	}

	private[consensus] final case class TransitionalConfigChange[P <: AnyRef](override val term: Term, override val requestId: ConfigChangeRequestId, override val oldParticipants: Set[P], override val newParticipants: Set[P]) extends ConfigChange[P] {
		override def isActive(participantId: P): Boolean = newParticipants.contains(participantId) || oldParticipants.contains(participantId)
	}

	private[consensus] final case class StableConfigChange[P <: AnyRef](override val term: Term, override val requestId: ConfigChangeRequestId, coupleTerm: Term, override val oldParticipants: Set[P], override val newParticipants: Set[P]) extends ConfigChange[P] {
		override def isActive(participantId: P): Boolean = newParticipants.contains(participantId)

		/** @return true if the provided [[ConfigChange]] is the [[TransitionalConfigChange]] corresponding to this [[StableConfigChange]]. */
		def isCoupleOf(cc: ConfigChange[P]): Boolean = {
			cc match {
				case tcc: TransitionalConfigChange[P] => tcc.term == coupleTerm && tcc.requestId == requestId && tcc.newParticipants == newParticipants && tcc.oldParticipants == oldParticipants
				case _: StableConfigChange[P] => false
			}
		}

		def recreateCouple: TransitionalConfigChange[P] = TransitionalConfigChange(coupleTerm, requestId, oldParticipants, newParticipants)
	}

	/** Describes what the consensus algorithm needs retried when it requests a deferred wake-up via [[ClusterParticipant.requestWakeUp]].
	 * The host uses this to choose an appropriate delay before invoking the callback. */
	enum WakeUpReason {
		/** Retry sending PermitQuiesce to participants that failed to receive it. */
		case QuiescenceAuthorizationRetry
		/** Retry [[ClusterParticipant.appendRecords]] calls to followers that were unreachable. */
		case UnreachableFollowersRetry
		/** Retry sending records to a retiring participant that was unreachable. */
		case RetiringParticipantRetry
	}

	/** An opaque token returned by [[ClusterParticipant.requestWakeUp]], used to cancel a pending wake-up via [[ClusterParticipant.cancelWakeUp]]. */
	opaque type WakeUpToken = Any
	object WakeUpToken {
		inline def apply(value: Any): WakeUpToken = value
	}
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

	/** Defines a total ordering over [[ClientCommand]] instances.
	 *
	 * This ordering is used to determine the relative freshness of commands from the same client.
	 * Implementations must ensure that:
	 *
	 *   - For any two commands `a` and `b` from the same client, if `a` is issued *after* `b`, then `clientCommandOrdering.compare(a, b) > 0`.
	 *   - If `a` and `b` are semantically identical (e.g., same request ID), then `clientCommandOrdering.compare(a, b) == 0`.
	 *   - If `a` is issued *before* `b`, then `clientCommandOrdering.compare(a, b) < 0`.
	 *
	 * The ordering must be consistent and total for commands from the same client.
	 * Ordering between commands from different clients may be arbitrary or undefined.
	 */
	val clientCommandOrdering: Ordering[ClientCommand]

	/** Extracts the client identifier from a given [[ClientCommand]].
	 *
	 * This enables the consensus module to group commands by origin and apply per-client deduplication and retry logic.
	 */
	def clientIdOf(command: ClientCommand): ClientId

	/** The type of the state-machine's [[StateMachine.applyClientCommand]] method's responses. */
	type StateMachineResponse

	/** The type of [[Workspace]] implementation. */
	type WS <: Workspace

	//// CONFIGURATION

	val MAX_RECURSION_DEPTH: Int = 99
	val MAX_PERMIT_QUIESCENCE_RETRIES: Int = 9

	def retiringParticipantMaxRetries: Int = 9

	def isEager: Boolean = false

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
		/** Applies the given [[ClientCommand]] to the state machine.
		 * @return a [[sequencer.LatchingDuty]] that yields the [[StateMachineResponse]]
		 */
		def applyClientCommand(index: RecordIndex, command: ClientCommand): sequencer.LatchingDuty[StateMachineResponse]

		/** Returns a [[sequencer.Task]] that yields the [[RecordIndex]] most recently passed to [[applyClientCommand]] whose corresponding [[sequencer.LatchingDuty]] completed has been completed.
		 * If the implementation cannot determine this index, it should return zero, indicating that all commands must be replayed.
		 *
		 * This method is invoked only during recovery after restarts or persistence failures.
		 */
		def recoverIndexOfLastAppliedCommand: sequencer.LatchingDuty[RecordIndex]
	}

	//// RESPONSE TO CLIENT

	/** The response to a client command.
	 * @see [[ClusterParticipant.Delegate.onCommandFromClient]]. */
	sealed trait ResponseToClient

	/** The command was appended at the specified [[recordIndex]] to a majority of the participants persistent logs, and applied to the [[StateMachine]] which responded with the specified [[content]]. */
	final case class Processed(recordIndex: RecordIndex, content: StateMachineResponse) extends ResponseToClient

	/** The client has to repeat the command to the specified participant. This happens when the receiver is or becomes a [[FOLLOWER]]. */
	final case class RedirectTo(participantId: ParticipantId) extends ResponseToClient

	/** Indicates the participant cannot process the command because its state is [[ISOLATED]], [[RETIRING]], or [[QUIESCED]].
	 * Upon receiving this response, the client should retry the request with one of the [[otherParticipants]] using the provided `nextAttemptFlag`.
	 * @param nextAttemptFlag The value the client must pass in the `attemptFlag` parameter of the [[ClusterParticipant]]'s command-delivery RPC method. This RPC method is responsible for passing both the flag and the command to [[ClusterParticipant.Delegate.onCommandFromClient]].
	 * @param otherParticipants A set of alternative participant identifiers for the client to attempt. This list is provided on a best-effort basis and may be incomplete or contain stale/unavailable participants. The first elements of the list are more probable to be correct and up-to-date than the last ones. */
	final case class Unable(nextAttemptFlag: CommandAttemptFlag, otherParticipants: ListSet[ParticipantId]) extends ResponseToClient

	/** The command was rejected because a newer command from the same origin has already been processed, while this one was never received earlier.
	 *
	 * The client should discard this command, assuming the [[StateMachine]] does not require contiguous, gap‑free, monotonic ordering of command identifiers.
	 *
	 * If the [[StateMachine]] does require strict contiguous ordering, it must enforce that policy itself by rejecting non‑contiguous commands with a special [[StateMachineResponse]], which the client will receive wrapped inside a [[Processed]].
	 * @param command the [[ClientCommand]] received in the request
	 * @param lastCommandIndex the [[RecordIndex]] of the last [[ClientCommand]] received from the same client (same [[ClientId]]).
	 */
	final case class Superseded(command: ClientCommand, lastCommandIndex: RecordIndex) extends ResponseToClient

	/** The command predates the last snapshot boundary, so the leader cannot determine whether it was ever processed, nor can it replay the corresponding [[StateMachineResponse]].
	 *
	 * The client must resolve this situation (e.g. by resynchronizing or discarding).
	 * @param command the [[ClientCommand]] received in the request
	 */
	final case class TooOld(command: ClientCommand) extends ResponseToClient

	/** The command was previously processed, but its response is not replayed because replayability of responses to commands older than the last received has been disabled by the [[ConsensusParticipantSdm.Workspace.indexOf]] implementation returning 0.
	 *
	 * The client should treat this as a stale retry and discard it.
	 * @param command the [[ClientCommand]] received in the request
	 * @param lastCommandIndex the [[RecordIndex]] of the last [[ClientCommand]] received from the same client (same [[ClientId]]).
	 */
	final case class Stale(command: ClientCommand, lastCommandIndex: RecordIndex) extends ResponseToClient

	/** Tells that the [[Workspace]] of the [[ConsensusParticipant]] service is inconsistent. Should never happen. TODO consider restarting the service in this situation, instead. */
	final case class InconsistentState(detail: String) extends ResponseToClient

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
		def getOtherProbableParticipant: ListSet[ParticipantId]

		/** Called by the bound [[ConsensusParticipant]] to notify that its active [[ConsensusParticipant.Configuration]] has changed, and now it expects connectivity with a different set of participants.
		 *
		 * This method is invoked upon activation of a new [[ConsensusParticipant.Configuration]] to tell the cluster-layer which are the participants that the consensus-layer expects to be reachable.
		 * Given configuration changes is a two-phase process, a call to [[Delegate.requestConfigChange]] causes two [[ConfigChange]] records to be appended and, therefore, two calls to this method per involved [[ConsensusParticipant]] service.
		 * Successive calls with the same argument may occur. Implementations may ignore such calls only if no intervening call with a different argument has occurred — i.e., if the configuration has not changed.
		 * @param change The [[ConfigChange]] that backs the activated [[ConsensusParticipant.Configuration]].
		 * @param changeIndex the [[RecordIndex]] of the applied [[ConfigurationChange]]
		 */
		def notifyActiveConfigChanged(change: ConfigChange[ParticipantId], changeIndex: RecordIndex, roleOrdinal: RoleOrdinal): Unit

		/** Called by the bound [[ConsensusParticipant]] after it becomes quiesced. This allows this [[ClusterParticipant]] service to release the resources dedicated to it. */
		def notifyQuiesced(motive: Try[String]): Unit

		/** Called by the bound [[ConsensusParticipant]] when it needs to be woken up after some host-determined delay.
		 * The host should eventually invoke the provided `callback` within the [[sequencer]], after an appropriate delay.
		 * The [[WakeUpReason]] conveys what the consensus algorithm needs retried so the host can choose an appropriate delay.
		 *
		 * Must be called within the [[sequencer]].
		 *
		 * @param reason describes what the consensus algorithm needs retried.
		 * @param callback the function to invoke when the delay elapses. Must be invoked within the [[sequencer]].
		 * @return a token that can be passed to [[cancelWakeUp]] to cancel the pending wake-up.
		 */
		def requestWakeUp(reason: WakeUpReason, callback: () => Unit): WakeUpToken

		/** Cancels a previously requested wake-up.
		 * After this method returns, the callback associated with the given token must never be invoked.
		 * Must be called within the [[sequencer]].
		 */
		def cancelWakeUp(token: WakeUpToken): Unit

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

			/** Handles a client-submitted command intended for the state machine.
			 *
			 * This method is invoked by the bound [[ClusterParticipant]] when a client sends a command to this participant.
			 * It must be called within the [[sequencer]].
			 * TODO Analyze the alternative of returning a Duty that yields a variant of [[Unable]] that details the failure when something fails. 
			 *
			 * @param command The command issued by the client for state-machine execution.
			 * @param attemptFlag Indicates the outcome of the client's previous attempt to send this command.
			 * @return A [[sequencer.Duty]] that yields one of:
			 *         - The state-machine's response to the client.
			 *         - A [[RedirectTo]] message instructing the client to contact the current leader.
			 *         - An [[Unable]] message indicating that this participant cannot currently reach consensus.
			 */
			def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.LatchingDuty[ResponseToClient]

			/**
			 * This method is invoked by this [[ClusterParticipant]] when another participant calls [[howAreYou]] on the [[ParticipantId]] of the owner of this [[Delegate]]
			 *
			 * Must be called within the [[sequencer]].
			 * @param inquirerId The id of the participant that called [[howAreYou]].
			 * @param inquirerInfo The [[StateInfo]] of the participant that called [[howAreYou]].
			 * @return The state information of the destination participant.
			 */
			def onHowAreYou(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.LatchingDuty[StateInfo]

			/**
			 * This method is invoked by this [[ClusterParticipant]] when another participant calls [[chooseALeader]] on the [[ParticipantId]] of the owner of this [[Delegate]]
			 *
			 * Must be called within the [[sequencer]].
			 * @param inquirerId The id of the participant that called [[chooseALeader]].
			 * @param inquirerInfo Information about the state of the participant that called.
			 * @return A [[sequencer.Task]] that yields a [[Vote]] indicating the candidate chosen by the listening participant for the specified term.
			 */
			def onChooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.LatchingDuty[Vote[ParticipantId]]

			/**
			 * This method is invoked by this [[ClusterParticipant]] when another participant calls [[appendRecords]] on the [[ParticipantId]] of the owner of this [[Delegate]]
			 *
			 * Must be called within the [[sequencer]].
			 * @param inquirerId The id of the participant that called [[appendRecords]].
			 * @param inquirerTerm The term of the participant that called [[appendRecords]].
			 * @param prevRecordIndex The index of record after which the specified `records` should be appended.
			 * @param prevRecordTerm The term of the record after which the specified `records` should be appended.
			 * @param records The records to append.
			 * @param leaderCommit The index of the highest log entry known to be committed (replicated to a majority) according to the inquirer.
			 * @return A [[sequencer.Task]] that yields the result of the append operation.
			 */
			def onAppendRecords(inquirerId: ParticipantId, inquirerTerm: Term, prevRecordIndex: RecordIndex, prevRecordTerm: Term, records: GenIndexedSeq[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.LatchingDuty[AppendResult]

			/**
			 * This method is invoked by this [[ClusterParticipant]] when another participant calls [[permitQuiescence]] on the [[ParticipantId]] of the owner of this [[Delegate]].
			 * @param grantorId the identifier of the participant that granted permission to quiesce.
			 * @param indexOfGrantedStableConfigChange The index of the [[StableConfigChange]] record for which the permission to quiesce was granted. Said record is the one that excludes the destination participant.
			 */
			def onQuiescencePermitted(grantorId: ParticipantId, indexOfGrantedStableConfigChange: RecordIndex): Unit

			/** Allows the [[ClusterParticipant]] service to request changes to the set of consensus-participants.
			 * Usually called whenever the set of consensus-participants has forcefully changed (i.e: a cluster-member included in the current consensus-participants-set went down) or is about to change (i.e: a node intended to be part of consensus-participants-set joined the cluster, or is going to leave the cluster for maintenance).
			 * To improve availability during planned cluster-membership transitions, the manager of the planed change should do the following:
			 *		1 call this method on every consensus-participant service to ensure the leader gets noticed, // TODO this is awkward. Make the configuration-change request be propagated to the leader when received by non-leaders.
			 *		2 wait until either:
			 *			- the returned [[sequencer.LatchingDuty]] yields either [[SUCCESSFULLY_CHANGED]] or [[ALREADY_CHANGED]] for any of the consensus-participants,
			 *			- or the [[notifyActiveConfigChanged]] is called in any of the consensus-participants with the provided request identifier or desired participants set.
			 *
			 * @param requestId an identifier chosen by the caller that will be propagated up to the invocations of the [[notifyActiveConfigChanged]] method of each of the [[ClusterParticipant]] instances bound to the involved [[ConsensusParticipant]] services.
			 * @param desiredParticipantsSet the identifiers of the participants that are going to seek consensus from now on.
			 * @param priorAnswer should contain the response to the last request done by the inquirer to this or any other participant, if any. 
			 * @return a [[sequencer.Duty]] that yields:
			 *         [[SUCCESSFULLY_CHANGED]] if the requested change was successfully completed.
			 *         [[ALREADY_CHANGED]] if the requested change is already done or in progress.
			 *         [[ALREADY_IN_PROGRESS]] if the participant is currently transitioning to the requested configuration 
			 *         [[ASK_THE_LEADER]] if none of the previous bullet is true and the [[ConsensusParticipant]] is a [[FOLLOWER]].
			 *         [[WAIT_PREVIOUS_CHANGE_TO_COMPLETE]] if the participant is the leader but is currently processing a change to a configuration different from the requested.  
			 *         [[STOPPED]] if the participant is not able to become neither the [[LEADER]] nor a [[FOLLOWER]]
			 *         - currently the leader or a follower that already has the desired participants set as the current or scheduled one;
			 *         - currently the leader and was able to replicate the corresponding [[TransitionalConfigChange]] to a majority according to that same [[TransitionalConfigChange]] rules. */
			def requestConfigChange(requestId: ConfigChangeRequestId, desiredParticipantsSet: Set[ParticipantId], priorAnswer: Maybe[ConfigChangeResponse]): sequencer.LatchingDuty[ConfigChangeResponse]
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
			 * @return A [[sequencer.Task]] that yields the state information of the destination participant.
			 */
			def howAreYou(inquirerInfo: StateInfo): sequencer.Task[StateInfo]

			/**
			 * Request the destination participant to choose a leader.
			 * The implementation should make, somehow, the destination participant's [[Delegate.onChooseALeader]] to be called, and return what it returns.
			 * Called within the [[sequencer]] thread.
			 * @param inquirerId The id of the participant that is asking.
			 * @param inquirerInfo Information about the state of the participant that is asking.
			 * @return A [[sequencer.Task]] that yields a [[Vote]] indicating the candidate chosen by the destination participant.
			 */
			def chooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.Task[Vote[ParticipantId]]

			/**
			 * Request the destination participant to append records.
			 * The implementation should make, somehow, the destination participant's [[Delegate.onAppendRecords]] to be called with the same parameter values, and return what it returns.
			 * // TODO consider the addition of a wrapper that suppresses records already sent in in-flight calls, and maybe avoids the call at all if no records are left.
			 * This method is called within the [[sequencer]].
			 * @param inquirerTerm The term of the participant that is asking.
			 * @param prevLogIndex the [[RecordIndex]] of the [[Record]] after which the records should be appended.
			 * @param prevLogTerm the expected [[Term]] of the [[Record]] at `prevLogIndex`.
			 * @param records The records to append.
			 * @param leaderCommit The index of the highest log entry known to be committed (replicated to a majority) according to the inquirer.
			 * @return A [[sequencer.Task]] that yields the result of the append operation.
			 */
			def appendRecords(inquirerTerm: Term, prevLogIndex: RecordIndex, prevLogTerm: Term, records: GenIndexedSeq[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.Task[AppendResult]


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
			 * @return A [[sequencer.Task]] that completes successfully if either: the permission was successfully delivered, or the participant is already in a post-retirement state ([[QUIESCED]], released, or no longer exists).
			 */
			def permitQuiescence(indexOfGrantedStableConfigChange: RecordIndex): sequencer.Task[Unit]
		}
	}


	//// PERSISTENCE 

	/** Specifies the unit of work that a [[ConsensusParticipant]] requires to manage its persistent state.
	 * Instances of this trait must be accessed only within a `primaryStateUpdater` passed to the [[sequencer.CausalFence.advance]] method of the [[sequencer.CausalFence]] instance of the [[ConsensusParticipant]]. */
	trait Workspace {

		//		/** Record index of the first record in the log buffer. */
		//		private var logBufferOffset: RecordIndex = 1
		//
		//		/** The log buffer.
		//		 * Contains the records since the last snapshot. */
		//		private val logBuffer: mutable.ArrayBuffer[Record] = mutable.ArrayBuffer.empty

		/** The implementation should return the index of the [[ConfigChange]] instance with greater index in the log.
		 * This method is called very frequently so the implementation should strive to be efficient. */
		def indexOfTopConfigChange: RecordIndex

		/** The current term according to this participant.
		 * The Initial value is zero.
		 * Zero means "before the first election". */
		def getCurrentTerm: Term

		def setCurrentTerm(term: Term): Unit

		/** The index of the oldest [[Record]] stored in the log buffer. The initial value is 1. */
		def logBufferOffset: RecordIndex

		/** The index of the first empty entry in the log. The initial value is 1. */
		def firstEmptyRecordIndex: RecordIndex // = logBufferOffset + logBuffer.size

		def getRecordAt(index: RecordIndex): Record // = logBuffer((index - logBufferOffset).toInt)

		/** Returns the records in the log starting at `from` and up to `until` exclusive.
		 */
		def getRecordsBetween(from: RecordIndex, until: RecordIndex): IArray[Record] // = logBuffer.slice((from - logBufferOffset).toInt, (until - logBufferOffset).toInt)

		/** Appends a record and returns it index. */
		def appendRecord(record: Record): Unit

		/** Appends any new record not already in the log starting at the specified index.
		 * If a conflict is detected (i.e., a stored record and a new record at the same index have different terms), all stored records from that index onward are removed before appending the new records.
		 * */
		def appendResolvingConflicts(records: GenIndexedSeq[Record], from: RecordIndex): Unit

		/** Should be called whenever the [[ConsensusParticipant.highestAppliedCommandIndex]] changes to allow this [[Workspace]] to release the storage used to memorize the records that are pending to be applied to the [[StateMachine]]. */
		def informAppliedCommandIndex(appliedCommandIndex: RecordIndex): Unit

		/** Should return the index of the last [[CommandRecord]] in the log whose [[ClientId]] is the provided one, or zero if none. */
		def indexOfLastAppendedCommandFrom(clientId: ClientId): RecordIndex

		/** The implementation should return one of:
		 * - the index of the provided [[ClientCommand]] in the Raft log if it is found
		 * - `-1` if it is not present
		 * - `0` if clients do not require response replayability for commands older than
		 *   the last received from the same client
		 *
		 * This method is invoked when the leader receives a command that is older (according to [[ConsensusParticipantSdm.clientCommandOrdering]]) than the most recently received command from the same client.
		 *
		 * If replayability is required, return the index of the original occurrence so that the same response can be deterministically replayed.
		 * If the command is not found — either because it was never appended or was appended before the last snapshot point — return `-1`. In this case the command is rejected as [[TooOld]] (if it predates the last received at the snapshot) or	[[Superseded]] (if it was never in the log).
		 * If replayability is not required for commands older than the last received, return `0`, in which case the command is rejected as [[Stale]].
		 *
		 * @param clientCommand the command received from the client
		 * @return the Raft log index of the original command for replay, `-1` if not found, or `0` to reject as stale when replayability of responses to commands older than the last received is not required
		 */
		def indexOf(clientCommand: ClientCommand): RecordIndex

		/** The [[ConsensusParticipant]] triggers the returned [[Duty]] to inform that it will not reference this instance anymore and this [[Workspace]] may be purged. */
		def releases: sequencer.Duty[Unit]
	}

	/** Defines what a [[ConsensusParticipant]] requires from a persistence service to load and save its [[Workspace]].
	 * Implementations may assume that all methods of this trait are invoked within the [[sequencer]] thread, enabling optimizations such as avoiding unnecessary creation of new task objects. */
	trait Storage {
		def load: sequencer.LatchingDuty[Try[WS]]

		/** Saves the workspace to the persistence storage.
		 * Design Note: A failure to save the workspace should restart the [[ConsensusParticipant]] as if it had crashed and lost all non-persistent variables.
		 */
		def save(workspace: WS): sequencer.LatchingDuty[Try[Unit]]
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

		def onHandingOff(term: Term): Unit

		def onRetiring(previous: RoleOrdinal, term: Term): Unit

		def onRoleLeft(left: RoleOrdinal, term: Term): Unit

		def onCommitIndexChanged(previous: RecordIndex, current: RecordIndex): Unit

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

		override def onHandingOff(term: Term): Unit = ()

		override def onRetiring(previous: RoleOrdinal, term: Term): Unit = ()

		override def onRoleLeft(left: RoleOrdinal, term: Term): Unit = ()

		override def onCommitIndexChanged(previous: RecordIndex, current: RecordIndex): Unit = ()

		override def onActiveConfigChanged(currentRole: RoleOrdinal, currentTerm: Term, configChangeIndex: RecordIndex, configChange: ConfigChange[ParticipantId]): Unit = ()
	}

	inline def checkWithin(): Unit = {
		if ConsensusParticipantSdm.assertionsEnabled && !isInSequence then throw new AssertionError(sequencer.checkWithinMsg())
	}



	//// PARTICIPANT'S CONSENSUS SERVICE

	/**
	 * A service of consensus for managing a replicated log with other participants in a distributed system.
	 *
	 * This service algorithm enables multiple participants (typically hosted on different nodes) to reach agreement on values.
	 * Once consensus is reached on a value, that decision becomes final and irreversible.
	 *
	 * Like Raft, this consensus algorithm relies on strong leadership and makes progress when a majority of participants
	 * are available. The algorithm is based on the following core principles:
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

		private type AppendOutcome = Int
		private inline val AO_IS_LAGGING_MASK = 16
		private inline val AO_SUCCESS = 0
		private inline val AO_NEEDS_EARLIER_RECORDS = 1 | AO_IS_LAGGING_MASK
		private inline val AO_MISSING_BECAUSE_PARTICIPANT_WAS_NOT_PART_OF_THE_CONFIGURATION = 2 | AO_IS_LAGGING_MASK
		private inline val AO_HAS_HIGHER_TERM = 3
		private inline val AO_SKIPPED_BECAUSE_OUT_OF_CONFIGURATION = 4
		private inline val AO_IS_RETIRING = 5
		private inline val AO_IS_QUIESCED = 6
		private inline val AO_NEEDS_RECORDS_THAT_PREDATE_SNAPSHOT = 7
		private inline val AO_IS_UNREACHABLE = 8
		private inline val AO_UNEXPECTED_RETIRING = 9

		/** The index of the highest entry known to be committed according to this participant.
		 * A log record is committed once the leader that created the record has replicated it on a majority of the participants.
		 * This also commits all preceding records in the leader’s log, including records created by previous leaders.
		 * CAUTION: This variable depends on the [[PrimaryState]] (as well as external states); mutations to the [[PrimaryState]] modify its value. To ensure deterministic causal ordering relative to these mutations, a [[StatefulRole]] must only access this variable within consumers synchronously subscribed to the [[sequencer.LatchingDuty]] returned by [[sequencer.CausalFence.causalAnchor]] or [[sequencer.CausalFence.advance]]-like methods on the [[StatefulRole.primaryStateFence]]. This ensures the variable is read in synchronization with the specific PrimaryState mutation it depends on.
		 * */
		private var commitIndex: RecordIndex = 0

		/** The index of the [[CommandRecord]] with the highest index whose command was successfully applied to the [[StateMachine]] of this [[ConsensusParticipant]].
		 * CAUTION: This variable depends on the [[PrimaryState]] (as well as external states); mutations to the [[PrimaryState]] modify its value. To ensure deterministic causal ordering relative to these mutations, a [[StatefulRole]] must only access this variable within consumers synchronously subscribed to the [[sequencer.LatchingDuty]] returned by [[sequencer.CausalFence.causalAnchor]] or [[sequencer.CausalFence.advance]]-like methods on the [[StatefulRole.primaryStateFence]]. This ensures the variable is read in synchronization with the specific PrimaryState mutation it depends on.
		 * */
		private var highestAppliedCommandIndex: RecordIndex = 0

		/** The current role of this [[ConsensusParticipant]].
		 * @note The [[currentRole]] state is neither entirely derived from the [[PrimaryState]] nor orthogonal to it. They are interrelated.
		 * CAUTION: This variable depends on the [[PrimaryState]] (as well as external states); mutations to the [[PrimaryState]] modify its value. To ensure deterministic causal ordering relative to these mutations, a [[StatefulRole]] must only access this variable within consumers synchronously subscribed to the [[sequencer.LatchingDuty]] returned by [[sequencer.CausalFence.causalAnchor]] or [[sequencer.CausalFence.advance]]-like methods on the [[StatefulRole.primaryStateFence]]. This ensures the variable is read in synchronization with the specific PrimaryState mutation it depends on.
		 * */
		private var currentRole: Role = new Starting(indexOfTheIncludingConfigChange, participantsInTheIncludingConfigChange)

		private var workspaceReleasedCovenant: sequencer.LatchingDuty[Unit] = sequencer.ReadyDuty(())

		/** The current, not causally anchored, [[Configuration]] of this [[ConsensusParticipant]].
		 * To obtain a causally anchored [[Configuration]] use [[StatefulRole.deriveConfigurationFrom]] instead.
		 * This variable is updated by the [[StatefulRole.deriveConfigurationFrom]] method.
		 * CAUTION: This variable depends on the [[PrimaryState]]; mutations to the [[PrimaryState]] modify its value. To ensure deterministic causal ordering relative to these mutations, a [[StatefulRole]] must only access this variable within consumers synchronously subscribed to the [[sequencer.LatchingDuty]] returned by [[sequencer.CausalFence.causalAnchor]] or [[sequencer.CausalFence.advance]]-like methods on the [[StatefulRole.primaryStateFence]]. This ensures the variable is read in synchronization with the specific PrimaryState mutation it depends on.
		 * */
		private var currentConfig: Configuration = NoConfig

		/** Memory where the [[Role.onQuiescencePermitted]] method stores the [[ParticipantId]] of the last quiescence grantor. */
		private var quiescenceGrantor: Maybe[ParticipantId] = Maybe.empty
		/** Memory where the [[Role.onQuiescencePermitted]] method stores the [[RecordIndex]] of the last [[StableConfigChange]] for which quiescence was authorized. */
		private var indexOfStableConfigChangeForWhichQuiescenceWasPermitted: RecordIndex = 0
		/** Memorizes the token for the pending wake-up used to retry failed calls to [[permitQuiescence]]. Needed to be able to cancel the retry. */
		private var retryPermitQuiescenceWakeUp: Maybe[WakeUpToken] = Maybe.empty

		/** Knows the [[RetirementDriver]]s corresponding to the participants that were excluded from the configuration and potentially have not received the appends to notice that they can leave. */
		private val retirementDriverByParticipantId: mutable.Map[ParticipantId, RetirementDriver] = mutable.Map.empty

		/** The current election round.
		 * Should be bumped whenever the part of the state of this participant that is exposed in questions to other participants (term and commitIndex as of this writing) changes.
		 * CAUTION: This variable depends on the [[PrimaryState]] (as well as external states); mutations to the [[PrimaryState]] modify its value. To ensure deterministic causal ordering relative to these mutations, a [[StatefulRole]] must only access this variable within consumers synchronously subscribed to the [[sequencer.LatchingDuty]] returned by [[sequencer.CausalFence.causalAnchor]] or [[sequencer.CausalFence.advance]]-like methods on the [[StatefulRole.primaryStateFence]]. This ensures the variable is read in synchronization with the specific PrimaryState mutation it depends on.
		 * */
		private var currentBallot: Ballot = 1

		/** Stores the last [[StateInfo]] instance returned by [[Role.syncLocalStateInfo]]
		 * CAUTION: This variable depends on the [[PrimaryState]] (as well as external states); mutations to the [[PrimaryState]] modify its value. To ensure deterministic causal ordering relative to these mutations, a [[StatefulRole]] must only access this variable within consumers synchronously subscribed to the [[sequencer.LatchingDuty]] returned by [[sequencer.CausalFence.causalAnchor]] or [[sequencer.CausalFence.advance]]-like methods on the [[StatefulRole.primaryStateFence]]. This ensures the variable is read in synchronization with the specific PrimaryState mutation it depends on.
		 * */
		private var stateInfoExposedInLastInteraction: StateInfo = StateInfo(PRE_INIT, ER_NONE, PRE_INIT, 0, 0, 0)

		/** Memorizes the [[StateInfo]] of the other participants seen during the [[currentBallot]].
		 * The [[StateInfo.ballot]] field of contained instances should match the [[currentBallot]].
		 * When a [[StateInfo]] with a newer ballot is seen, this map is cleared before adding it.
		 * DO NOT FORGET TO call the appropriate method (like [[Role.syncLocalStateInfo]] or [[updateSeenStateInfo]]) to update this variable before reading it.
		 * CAUTION: This variable depends on the [[PrimaryState]] (as well as external states); mutations to the [[PrimaryState]] modify its value. To ensure deterministic causal ordering relative to these mutations, a [[StatefulRole]] must only access this variable within consumers synchronously subscribed to the [[sequencer.LatchingDuty]] returned by [[sequencer.CausalFence.causalAnchor]] or [[sequencer.CausalFence.advance]]-like methods on the [[StatefulRole.primaryStateFence]]. This ensures the variable is read in synchronization with the specific PrimaryState mutation it depends on.
		 * @note uses a java map to improve efficiency.
		 * TODO make values be [[Covenant]]s of [[StateInfo]] so that received questions that include a [[StateInfo]] fulfill the howAreYou questions done by this participant. */
		private val memorizedParticipantInfos: java.util.Map[ParticipantId, StateInfo] = new java.util.HashMap()

		private var decoupledCommandsApplierIsRunning: Boolean = false

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

		private val coalescedHowAreYou = CoalescedQuery[(otherParticipantId: ParticipantId, stateInfo: StateInfo), StateInfo, sequencer.type](sequencer, params =>
			sequencer.Commitment_triggerAndWire(params.otherParticipantId.howAreYou(params.stateInfo))
		)

		{
			initialListeners.foreach(notificationListeners.put(_, None))
			cluster.setBound(currentRole)
			Trace.init(() => s"$boundParticipantId: Ctor") {
				currentRole.onEnter(currentRole)
			}
		}

		/** @return the ordinal of the current behavior. */
		inline def getRoleOrdinal: RoleOrdinal = currentRole.ordinal

		/** @return a [[sequencer.Duty]] that quiesces this [[ConsensusParticipant]] instance. */
		def quiesces: sequencer.Duty[Unit] = {
			Trace.init(() => s"$boundParticipantId: quiesces") {
				sequencer.Duty_mineFlat { () =>
					val quiesced = Quiesced(Success("This ConsensusParticipant instance was forcefully quiesced."))
					become(quiesced).asInstanceOf[Quiesced].completed
				}
			}
		}

		/** @return a [[sequencer.Duty]] that quiesces and disposes this [[ConsensusParticipant]] instance. */
		def disposes: sequencer.Duty[Unit] = {
			quiesces.andThen { _ =>
				notificationListeners.clear()
				cluster.removeBound()
			}
		}

		/** Synchronously transitions this [[ConsensusParticipant]]'s [[Role]] to the provided one if defined. */
		private def become(maybeNewRole: Maybe[Role])(using Trace.Context): Role = Trace.step("become") {
			checkWithin()
			maybeNewRole.foreach { newRole =>
				currentRole.onLeave(newRole)
				val previousRole = currentRole
				val committedTerm = currentRole.getCommittedPrimaryState.currentTerm
				notifyListeners(_.onRoleLeft(previousRole.ordinal, committedTerm))
				currentRole = newRole
				cluster.setBound(newRole)
				newRole.onEnter(previousRole)
			}
			currentRole
		}

		/** Starts a new ballot by incrementing the [[currentBallot]] and clearing the [[memorizedParticipantInfos]]. */
		private inline def startNewBallot(): Unit = {
			currentBallot += 1
			memorizedParticipantInfos.clear()
		}

		/** Updates the [[currentBallot]] and clears the [[memorizedParticipantInfos]] if it is lower than the `seenBallot`.
		 * @return true if the [[currentBallot]] was updated. */
		private def updateBallotIfLowerThan(myStateInfo: StateInfo, seenBallot: Ballot): Boolean = {
			if seenBallot > myStateInfo.ballot then {
				currentBallot = seenBallot
				memorizedParticipantInfos.clear()
				true
			} else false
		}

		/** Updates the [[currentBallot]] and the [[memorizedParticipantInfos]] based on the bound participant's current [[StateInfo]] (which must be provided) and a seen [[StateInfo]] of another participant.
		 * Assumes that [[StateInfo.ballot]] behaves as a primary key among all the instances of [[StateInfo]] created by the same participant.
		 * @return true if either the [[currentBallot]] or the [[memorizedParticipantInfos]] are updated. */
		private def updateSeenStateInfo(myCurrentStateInfo: StateInfo, seenParticipantId: ParticipantId, seenStateInfo: StateInfo): Boolean = {
			if updateBallotIfLowerThan(myCurrentStateInfo, seenStateInfo.ballot) || (seenStateInfo.ballot == myCurrentStateInfo.ballot && !memorizedParticipantInfos.containsKey(seenParticipantId)) then {
				memorizedParticipantInfos.put(seenParticipantId, seenStateInfo)
				true
			} else false
		}

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
		private sealed abstract class Role extends Delegate { thisRole =>
			/** The ordinal corresponding to this [[Role]] */
			val ordinal: RoleOrdinal
			val rank: ElectionRank

			final def blankVote(term: Term): Vote[ParticipantId] = Vote(term, boundParticipantId, 0, 0, this.rank, 0)

			final def yieldsBlankVote(term: Term): sequencer.LatchingDuty[Vote[ParticipantId]] = sequencer.LatchingDuty_ready(blankVote(term))

			/** Called by [[become]] after the previous [[Role]]'s [[Role.onLeave]] method has returned, and the [[currentRole]] variable set to this [[Role]] instance.
			 * This method is suitable to enqueue primary state updates that must happen before any updates enqueued after [[become]] returns. */
			def onEnter(previous: Role)(using Trace.Context): Unit

			/** Called by [[become]] before transitioning to another role. */
			def onLeave(newRole: Role): Unit = ()

			/** Updates the derived state that is stored in the [[Role]] instance and depends on the current [[Configuration]]. Only the [[Leader]] role has such state as this writing. */
			def onActiveConfigChanged(currentPrimaryState: Accessible, currentConfig: Configuration, newConfig: Configuration, indexOfNewConfigChange: RecordIndex)(using Context): Unit = ()

			def getCommittedPrimaryState: PrimaryState =
				Inaccessible

			/** Returns a [[StateInfo]] that reflects the provided [[PrimaryState]], the [[commitIndex]], the [[rank]], and the [[currentBallot]] of the bound participant; and, if the returned value differs from the one returned in the previous call (stored in [[stateInfoExposedInLastInteraction]]), bumps the [[currentBallot]] and clears the [[memorizedParticipantInfos]]. */
			def syncLocalStateInfo(primaryState: PrimaryState)(using Trace.Context): StateInfo

			/** Returns a [[StateInfo]] that indicates disability to participate; and, if the returned value differs from [[stateInfoExposedInLastInteraction]], bumps the [[currentBallot]] and clears the [[memorizedParticipantInfos]]. */
			protected final def buildIneligibleInfo(term: Term): StateInfo = {
				val rememberedInfo = stateInfoExposedInLastInteraction
				val newInfo =
					if rememberedInfo.tiesWith(term, this.rank, PRE_INIT, 0, 0) then {
						if rememberedInfo.ballot == currentBallot then rememberedInfo else StateInfo(term, this.rank, PRE_INIT, 0, 0, currentBallot)
					} else {
						currentBallot += 1
						memorizedParticipantInfos.clear()
						StateInfo(term, this.rank, PRE_INIT, 0, 0, currentBallot)
					}
				stateInfoExposedInLastInteraction = newInfo
				newInfo
			}

			/**
			 * Asks the other participants how they are and decides which should be the leader based on their answers.
			 * @note This process updates the [[PrimaryState.currentTerm]] if a later one is seen, which may cause a [[currentRole]] update.
			 *
			 * @param primaryState0 a causally anchored [[PrimaryState]]
			 * @param blankVoteIfRoleChanges instructs to yield a [[blankVote]] if the [[currentRole]] is changed by other process before this process completes.
			 * @return A [[sequencer.LatchingDuty]] that yields a [[Vote]] with the chosen leader.
			 */
			def determineMyVote(primaryState0: PrimaryState, blankVoteIfRoleChanges: Boolean)(using Context): sequencer.LatchingDuty[Vote[ParticipantId]]

			/** Must be called before transitioning to [[Retiring]] to handle the special case when the active [[Configuration]] in an empty [[StableConfig]].
			 * The [[Leader]] role should start the process that authorizes others to transition to the terminal [[QUIESCED]] state.
			 *
			 * "Vanished" means the new config has zero participants. In that case there will be no successor leader to authorize quiescence, so the current (ghost) leader must do it itself before retiring.
			 * @param config The currently active [[Configuration]]. */
			def authorizeQuiescenceIfVanished(config: StableConfig)(using Trace.Context): Unit = ()
		}

		/** Partial implementation of the [[Role]]s that accesses the [[PrimaryState]] of the bound participant.
		 * @param primaryStateFence the [[sequencer.CausalFence]] that must be used to ensure causal ordering of the state updates. It must be propagated to subsequent [[StatefulRole]] instances. */
		private abstract class StatefulRole(val primaryStateFence: sequencer.CausalFence[PrimaryState]) extends Role {

			private type TermRef = IntRef
			/** The default argument for the [[updateTermIfLessThan]] method's second parameter.
			 * It is private and defined in the same class as the [[primaryStateFence]] to ensure that the contained [[Term]] variable reflects the expected value provided it is read within the synchronous part of a synchronously subscribed consumer to the [[sequencer.LatchingDuty]] returned by [[updateTermIfLessThan]]. See the game-changing-invariant in [[Doer.CausalFence]]. */
			protected final val defaultPreviousTermRef: TermRef = new TermRef(0)

			override def onLeave(newRole: Role): Unit = {
				if !newRole.isInstanceOf[StatefulRole] then {
					workspaceReleasedCovenant = for {
						_ <- workspaceReleasedCovenant
						_ <- primaryStateFence.advanceIf {
							case Inaccessible => Maybe.empty
							case a: Accessible => Maybe.some(a.withWorkspaceReleased())
						}
					} yield ()
				}
			}

			override final def getCommittedPrimaryState: PrimaryState =
				primaryStateFence.committedState

			override final def syncLocalStateInfo(primaryState: PrimaryState)(using Trace.Context): StateInfo = Trace.step("syncLocalStateInfo") {
				primaryState match {
					case Inaccessible =>
						buildIneligibleInfo(primaryState.currentTerm)

					case accessible: Accessible =>
						val rememberedInfo = stateInfoExposedInLastInteraction
						val termAtCommitIndex = accessible.getRecordTermAt(commitIndex)
						val activeConfigChangeIndex = deriveConfigurationFrom(accessible).changeIndex
						val newInfo =
							if rememberedInfo.tiesWith(primaryState.currentTerm, this.rank, termAtCommitIndex, commitIndex, activeConfigChangeIndex) then {
								if rememberedInfo.ballot == currentBallot then rememberedInfo else StateInfo(accessible.currentTerm, this.rank, termAtCommitIndex, commitIndex, activeConfigChangeIndex, currentBallot)
							} else {
								currentBallot += 1
								memorizedParticipantInfos.clear()
								StateInfo(accessible.currentTerm, this.rank, termAtCommitIndex, commitIndex, activeConfigChangeIndex, currentBallot)
							}
						stateInfoExposedInLastInteraction = newInfo
						if assertionsEnabled then assert(newInfo.rank != ER_NONE)
						newInfo
				}
			}

			override def determineMyVote(primaryState0: PrimaryState, blankVoteIfRoleChanges: Boolean)(using Context): sequencer.LatchingDuty[Vote[ParticipantId]] = {
				Trace.step("determineMyVote") {
					checkWithin()

					val config0 = deriveConfigurationFrom(primaryState0)
					// Yield a blank vote if this participant is not included in the current configuration.
					if !config0.allParticipants.contains(boundParticipantId) then yieldsBlankVote(primaryState0.currentTerm)
					else {
						// get the updated StateInfo of the bound participant, which also updates the `memorizedParticipantsInfos`
						val myStateInfo0 = syncLocalStateInfo(primaryState0)
						// Create the howAreYou questions
						val howAreYouQuestions0 = askHowOtherParticipantsAre(config0.allOtherParticipants, myStateInfo0, memorizedParticipantInfos)
						// determine my vote based on the answers to the howAreYou questions
						for {
							howAreYouAnswers0 <- sequencer.LatchingDuty_sequenceTasksToArray(howAreYouQuestions0, true)
							primaryState1 <- {
								val highestTermSeen = IArray.unsafeFromArray(howAreYouAnswers0).foldLeftWithIndex(primaryState0.currentTerm) { (latestTermSeen, answer, _) =>
									answer match {
										case Success(info) => if info.currentTerm > latestTermSeen then info.currentTerm else latestTermSeen
										case _: Failure[StateInfo] => latestTermSeen
									}
								}
								updateTermIfLessThan(highestTermSeen)
							}
							myVote <- {
								// Update the memorizedParticipantInfos (by filling the missing entries with the StateInfo instances in the answers), and count the failed answers.
								val numberOfFailedAnswers = {
									var myStateInfo = syncLocalStateInfo(primaryState1)
									IArray.unsafeFromArray(howAreYouAnswers0).foldLeftWithIndex(0) { (failedAnswersCounter, answer, participantIndex) =>
										val participantId = config0.allOtherParticipants(participantIndex)
										answer match {
											case Success(info) =>
												if updateSeenStateInfo(myStateInfo, participantId, info) then myStateInfo = syncLocalStateInfo(primaryState1)
												failedAnswersCounter

											case _: Failure[StateInfo] =>
												if memorizedParticipantInfos.containsKey(participantId) then failedAnswersCounter
												else failedAnswersCounter + 1
										}
									}
								}

								if blankVoteIfRoleChanges && currentRole != this then currentRole.yieldsBlankVote(primaryState1.currentTerm)
								else currentRole match {
									case sf: StatefulRole =>
										val config1 = deriveConfigurationFrom(primaryState1)
										// if the bound participant is excluded, yield a blank vote.
										if !config1.allParticipants.contains(boundParticipantId) then currentRole.yieldsBlankVote(primaryState1.currentTerm)
										// ask again if either, the active configuration changed while waiting the responses to the howAreYou questions, or a successful answer has an obsolete ballot.
										else {
											// if either, the active configuration changed while waiting the responses to the howAreYou questions, or a successful answer has an obsolete ballot; then ignore this `determineMyVote` execution replacing it with a new fresh one.
											if (config1 ne config0) || memorizedParticipantInfos.size + numberOfFailedAnswers < config0.allOtherParticipants.length then {
												Trace.trace(s"Restarting my vote determination due to ${if config1 ne config0 then s"a concurrent configuration change (${config0.changeIndex}->${config1.changeIndex})" else s"an obsolete answer, currentBallot=$currentBallot, memorizedInfosSize=${memorizedParticipantInfos.size}, numberOfFailedAnswers=$numberOfFailedAnswers, numberOfRequests=${config1.allOtherParticipants.size}"}")
												// TODO analyze if memorizedParticipantInfos should be cleared here.
												sf.determineMyVote(primaryState1, blankVoteIfRoleChanges)
											}
											// else, decide the vote based on the `StateInfo` stored in the `memorizedParticipantInfos`.
											else sequencer.LatchingDuty_ready(config1.decideMyVote(syncLocalStateInfo(primaryState1), memorizedParticipantInfosToArray(config1)))
										}

									case _ =>
										currentRole.yieldsBlankVote(primaryState1.currentTerm)
								}
							}
						} yield myVote
					}
				}
			}

			override def onHowAreYou(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.LatchingDuty[StateInfo] = {
				Trace.init(() => s"$boundParticipantId: onHowAreYou") {
					checkWithin()
					for primaryState1 <- updateTermIfLessThan(inquirerInfo.currentTerm) yield {
						val myStateInfo = currentRole.syncLocalStateInfo(primaryState1)
						if updateSeenStateInfo(myStateInfo, inquirerId, inquirerInfo) then currentRole.syncLocalStateInfo(primaryState1)
						else myStateInfo
					}
				}
			}

			override def onChooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.LatchingDuty[Vote[ParticipantId]] = {
				Trace.init(() => s"$boundParticipantId: onChooseALeader") {
					checkWithin()
					val term0Ref = new TermRef(0)
					for {
						// if the term is stale, update it persistently before interacting with other participants so that they see this participant with its updated and persisted state.
						primaryState1 <- updateTermIfLessThan(inquirerInfo.currentTerm, term0Ref)
						myVote <- {
							updateSeenStateInfo(syncLocalStateInfo(primaryState1), inquirerId, inquirerInfo)
							currentRole.determineMyVote(primaryState1, false)
						}
					} yield myVote
				}
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
			 *   - If this participant state haven't changed to a no receptive one ([[Quiesed]], [[Starting]] or [[Retiring]]) while waiting the application of committed [[Record]]s of the kind that update this participant consensus state (like [[TransitionalConfigChange]] and [[TransitionalConfigChange]]), then :
			 *     - Persists the updated workspace via `storage.saves`.
			 *     - On failure to persist, transitions to `Quiesced` and returns a failed result.
			 *
			 *   - Starts, in a decoupled manner, the process that silently applies committed commands to the state machine in log order.
			 *
			 * @param inquirerId         ID of the leader sending the AppendEntries request
			 * @param inquirerTerm       Term of the leader
			 * @param prevRecordIndex    Index of the record preceding the new entries
			 * @param prevRecordTerm     Term of the preceding record
			 * @param records            New records to append
			 * @param leaderCommit       Commit index reported by the leader
			 * @return a [[sequencer.LatchingDuty]] yielding the [[AppendResult]] where:
			 *         - `success` is true if, and only if, all the following are true when the appending was processed (specifically, when this participant's `primaryStateFence` was crossed):
			 *         		- the [[PrimaryState]] is valid;
			 *         		- `inquirerTerm >= currentTerm`;
			 *         		- the role is either ISOLATED or FOLLOWER;
			 *         		- the term of the log record at `prevRecordIndex` is equal to `prevRecordTerm`;
			 *         - `term = max(inquirerTerm, currentTerm)` when the appending was processed (causal fence crossed).
			 *         - `roleOrdinal` tells which was the role of this participant when the appending was processed (causal fence crossed).
			 */
			override def onAppendRecords(inquirerId: ParticipantId, inquirerTerm: Term, prevRecordIndex: RecordIndex, prevRecordTerm: Term, records: GenIndexedSeq[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.LatchingDuty[AppendResult] = {
				Trace.init(() => s"$boundParticipantId: onAppendRecords") {
					checkWithin()
					Trace.trace(s"onAppendRecords($inquirerId, s$inquirerTerm, $prevRecordIndex, $prevRecordTerm, $records, $leaderCommit, $termAtLeaderCommit) called")
					var primaryUpdateResult: (isTermUpdated: Boolean, appendSuccess: Boolean) = null
					for primaryState1 <- primaryStateFence.advanceIf {
						case Inaccessible =>
							primaryUpdateResult = (isTermUpdated = false, appendSuccess = false)
							Maybe.empty

						case accessible0: Accessible =>
							val currentTerm = accessible0.currentTerm
							if inquirerTerm < currentTerm then {
								primaryUpdateResult = (isTermUpdated = false, appendSuccess = false)
								Maybe.empty
							} else {
								val isTermUpdated = inquirerTerm > currentTerm
								val appendSuccess =
									JOINING <= currentRole.ordinal && (isTermUpdated || currentRole.ordinal <= FOLLOWER) // Note that even a leader appends records when received from another leader with a higher term. The role of the deposed participant is changed below, in the yield block, within the same Doer execution; so these two mutations are viewed as atomic from other executions of the same Doer.
										&& prevRecordIndex < accessible0.firstEmptyRecordIndex
										&& prevRecordTerm == accessible0.getRecordTermAt(prevRecordIndex)
								primaryUpdateResult = (isTermUpdated, appendSuccess)
								if appendSuccess && records.nonEmpty then Maybe(accessible0.withRecordsAppended(inquirerTerm, records, prevRecordIndex + 1))
								else if isTermUpdated then Maybe(accessible0.withTermUpdated(inquirerTerm))
								else Maybe.empty
							}
					} yield {
						primaryState1 match {
							case Inaccessible =>
								illegalStateQuiesce()
								AppendResult(PRE_INIT, Long.MaxValue, currentRole.ordinal)

							case accessible1: Accessible =>
								val successOrIndexForNextAttempt: RecordIndex =
									if primaryUpdateResult.appendSuccess then 0
									else if accessible1.firstEmptyRecordIndex < prevRecordIndex then accessible1.firstEmptyRecordIndex
									else if prevRecordIndex > 0 then prevRecordIndex
									else 1

								// At this point the primary state is already updated and the response is almost entirely determined, except the roleOrdinal field. All the following actions are causally-anchored derived-state updates.

								val previousCommitIndex = commitIndex
								if primaryUpdateResult.appendSuccess then {
									// Update the commitIndex
									if commitIndex < leaderCommit then commitIndex = if leaderCommit < accessible1.firstEmptyRecordIndex then leaderCommit else accessible1.firstEmptyRecordIndex - 1
									// if the commitIndex is bumped then:
									if commitIndex != previousCommitIndex then {
										// notify the change
										notifyListeners(_.onCommitIndexChanged(previousCommitIndex, commitIndex))
										// start the "apply committed commands" process if it isn't already started.
										if !decoupledCommandsApplierIsRunning then startApplyingCommittedCommands(accessible1)
									}
								}

								if primaryUpdateResult.appendSuccess || primaryUpdateResult.isTermUpdated then {
									val cro = currentRole.ordinal

									// get the updated configuration. Note that this must be done after updating the commitIndex.
									val config1 = deriveConfigurationFrom(accessible1)


									// If not joining or the catching-up is complete then:
									if cro != JOINING || (accessible1.firstEmptyRecordIndex > currentRole.asInstanceOf[Joining].indexOfTheIncludingConfigChange) then {
										// If this participant belongs to the active configuration, then:
										if config1.allParticipants.contains(boundParticipantId) then {
											// Become follower of the inquirer if it belongs to the active configuration.
											if config1.allOtherParticipants.contains(inquirerId) then become(Follower(accessible1.currentTerm, inquirerId, primaryStateFence))
											// Become isolated if this participant is joining, the catching-up is complete, and the inquirer is not in the active configuration.
											else if cro == JOINING then become(Isolated(primaryStateFence))
											// Keep the current role otherwise.
											// Note that, if the inquirer is excluded and the current role is follower of an excluded participant, the role is not changed to isolated here because it might be following a ghost leader.
										}
										// If this participant does not belong to the active configuration, then retire it.
										else {
											if assertionsEnabled then assert(config1.isInstanceOf[StableConfig]) // because exclusion is checked every record and transitional configurations are never more restrictive than the contiguos stable ones.
											currentRole.authorizeQuiescenceIfVanished(config1.asInstanceOf[StableConfig])
											become(Retiring(accessible1.currentTerm, config1.term, config1.changeIndex, config1.allParticipants))
										}
									}
								}

								AppendResult(primaryState1.currentTerm, successOrIndexForNextAttempt, currentRole.ordinal)
						}
					}
				}
			}

			/** Applies committed [[CommandRecord]]s silently and in a decoupled manner until reaching [[commitIndex]].
			 * @param primaryState the causally anchored [[PrimaryState]]. */
			private def startApplyingCommittedCommands(primaryState: Accessible): Unit = {
				decoupledCommandsApplierIsRunning = true

				def applyCommittedCommandsLoop(recursionDepth: Int): Unit = {
					if highestAppliedCommandIndex >= commitIndex then decoupledCommandsApplierIsRunning = false
					else {
						val indexOfCommandToApply = highestAppliedCommandIndex + 1
						primaryState.getRecordAt(indexOfCommandToApply) match {
							case command: CommandRecord[ClientCommand] @unchecked =>
								val previousExecutionSerial = sequencer.currentExecutionSerial
								for _ <- machine.applyClientCommand(indexOfCommandToApply, command.command) do {
									highestAppliedCommandIndex = indexOfCommandToApply
									primaryState.informAppliedCommandIndex(indexOfCommandToApply)
									if sequencer.currentExecutionSerial != previousExecutionSerial then applyCommittedCommandsLoop(0)
									else if recursionDepth < MAX_RECURSION_DEPTH then applyCommittedCommandsLoop(recursionDepth + 1)
									else sequencer.run(applyCommittedCommandsLoop(0))
								}
							case _ =>
								highestAppliedCommandIndex = indexOfCommandToApply
								primaryState.informAppliedCommandIndex(indexOfCommandToApply)
								if recursionDepth < MAX_RECURSION_DEPTH then applyCommittedCommandsLoop(recursionDepth + 1)
								else sequencer.run(applyCommittedCommandsLoop(0))
						}
					}
				}

				if highestAppliedCommandIndex > 0 then applyCommittedCommandsLoop(0)
				else {
					for index <- machine.recoverIndexOfLastAppliedCommand yield {
						highestAppliedCommandIndex = index
						primaryState.informAppliedCommandIndex(index)
						applyCommittedCommandsLoop(0)
					}
				}
			}

			/** @inheritdoc
			 * Wait in line for the [[PrimaryState]] and then delegate the request to the concrete stateful role. */
			override final def requestConfigChange(requestId: ConfigChangeRequestId, desiredParticipants: Set[ParticipantId], priorAnswer: Maybe[ConfigChangeResponse]): sequencer.LatchingDuty[ConfigChangeResponse] = {
				Trace.init(() => s"$boundParticipantId: requestConfigChange") {
					Trace.trace(s"Handling $requestId to $desiredParticipants, priorAnswer=$priorAnswer.")
					for {
						primaryState <- primaryStateFence.causalAnchor()
						response <- {
							// If a prior answer is provided, update the ballot and memorizedParticipantInfos 
							val ballotWasUpdated = priorAnswer.fold(false) { priorResponse =>
								updateBallotIfLowerThan(currentRole.syncLocalStateInfo(primaryState), priorResponse.latestBallotSeen)
							}
							// Delegate the request to the concrete stateful role.
							currentRole match {
								case stateful: StatefulRole =>
									primaryState match {
										case Inaccessible =>
											illegalStateQuiesce()
											sequencer.LatchingDuty_ready(STOPPED(currentRole.syncLocalStateInfo(primaryState).ballot))
										case accessible: Accessible =>
											stateful.requestConfigChange(accessible, requestId, desiredParticipants, ballotWasUpdated)
									}
								case stateless =>
									stateless.requestConfigChange(requestId, desiredParticipants, priorAnswer)
							}
						}
					} yield {
						Trace.trace(s"response to config change request $requestId is: $response")
						response
					}
				}
			}

			def requestConfigChange(primaryState: Accessible, requestId: ConfigChangeRequestId, desiredParticipants: Set[ParticipantId], ballotWasUpdated: Boolean)(using Context): sequencer.LatchingDuty[ConfigChangeResponse]

			override def onQuiescencePermitted(grantorId: ParticipantId, indexOfGrantedStableConfigChange: RecordIndex): Unit = {
				quiescenceGrantor = Maybe(grantorId)
				indexOfStableConfigChangeForWhichQuiescenceWasPermitted = indexOfGrantedStableConfigChange
			}

			//// Role updaters


			/** Starts a process that updates the [[currentRole]] and [[PrimaryState.currentTerm]] based on the [[StateInfo]]s returned by calling [[ClusterParticipant.howAreYou]] on the other participants and, if necessary, also based on the [[Vote]]s returned by calling [[ClusterParticipant.chooseALeader]] on them.
			 * This process always ends immediately after a call to [[become]] returns. So, its [[Role]] outcome can be seen in the [[currentRole]] derived state variable.
			 * The [[currentRole]] is updated only if the desired one if not equivalent to the [[currentRole]]. If updated, any other in-flight [[updateRole]] process is canceled and immediately completed.
			 * Concurrent executions of this method return the same result. */
			def updateRole()(using Trace.Context): sequencer.LatchingDuty[Unit] = {
				if assertionsEnabled then assert(currentRole eq this)
				for {
					primaryState <- primaryStateFence.causalAnchor()
					_ <- {
						if currentRole ne this then sequencer.LatchingDuty_unit
						else updateRole(primaryState)
					}
				} yield ()
			}


			/** Like [[updateRole]] but already knowing the current [[PrimaryState]]. */
			def updateRole(primaryState0: PrimaryState)(using Context): sequencer.LatchingDuty[Unit] = {
				checkWithin()
				if assertionsEnabled then assert(currentRole eq this)

				serialOfLastUpdateRoleExecution += 1
				val serial = serialOfLastUpdateRoleExecution

				Trace.step(() => s"updateRole#$serial") {

					inline def haveToAbort: Boolean = (currentRole ne this) || incumbentUpdateRoleSerial != serial

					/** Logic when the vote is for another participant. */
					def whenVotingAnother(currentState: Accessible, vote: Vote[ParticipantId]): sequencer.LatchingDuty[Unit] = {
						if vote.rank == ER_LEADING then {
							become(Follower(currentState.currentTerm, vote.votedId, primaryStateFence))
							sequencer.LatchingDuty_unit
						}
						// If the voted participant isn't retiring, become Isolated.
						else if vote.rank != ER_RETIREE then {
							become(Isolated(primaryStateFence))
							sequencer.LatchingDuty_unit
						}
						// If the voted participant is Retiring, then:
						else {
							val votedStateInfo = memorizedParticipantInfos.get(vote.votedId)
							// If a retiree wins the election, active candidates would get stuck indefinitely, as the retiree will never become Leader to advance their commitIndex via AppendEntries.
							// To unstuck the cluster, we attempt to safely absorb the retiree's commitIndex out-of-band using Raft's Log Matching Property: "If two entries in different logs have the same index and term, the logs are identical in all preceding entries."
							// By verifying our local log has the exact same term at the retiree's commitIndex, we mathematically prove our log holds all the entries the retiree knew to be committed, making it unequivocally safe to advance our own commitIndex.
							if currentState.firstEmptyRecordIndex > votedStateInfo.commitIndex
								&& currentState.getRecordTermAt(votedStateInfo.commitIndex) == votedStateInfo.termAtCommitIndex
								&& votedStateInfo.commitIndex > commitIndex
							then {
								commitIndex = votedStateInfo.commitIndex
								updateRole(currentState)
							} else {
								become(Isolated(primaryStateFence))
								sequencer.LatchingDuty_unit
							}
						}
					}

					def updateRoleKnowingMyVote(currentState1: Accessible, myVote1: Vote[ParticipantId]): sequencer.LatchingDuty[Unit] = {
						if assertionsEnabled then {
							assert(myVote1.term == currentState1.currentTerm || myVote1.ballot == 0, s"myVote=$myVote1, currentTerm=${currentState1.currentTerm}")
						}

						// Trace.trace(s"$boundParticipantId: updateRoleKnowingMyVote($myVote) was called") // TODO delete line
						val config1 = deriveConfigurationFrom(currentState1)
						// Trace.trace(s"$boundParticipantId: updateRoleKnowingMyVote($myVote) - configChange=${config1.correspondingConfigChange}") // TODO delete line

						if !config1.allParticipants.contains(boundParticipantId) then {
							if assertionsEnabled then assert(config1.isInstanceOf[StableConfig]) // because is required by Retiring. // TODO analyze if the config1 is always a stable one here.
							this.authorizeQuiescenceIfVanished(config1.asInstanceOf[StableConfig])
							become(Retiring(currentState1.currentTerm, config1.term, config1.changeIndex, config1.allParticipants))
							sequencer.LatchingDuty_unit
						}
						// If all the participants successfully answered the howAreYou RPC, then decide the vote omnisciently.
						else if config1.reachedAll(myVote1) then {
							if myVote1.votedId == boundParticipantId then {
								become(Promoting(currentState1.currentTerm, primaryStateFence))
								sequencer.LatchingDuty_unit
							} else whenVotingAnother(currentState1, myVote1)
						}
						// else, if a majority of the participants successfully answered the howAreYou RPC, then:
						else if config1.reachedAMajority(myVote1) then {
							// If my vote is for other participant, become follower or isolated depending on the other is leading or not.
							if myVote1.votedId != boundParticipantId then whenVotingAnother(currentState1, myVote1)
							// If my vote is for myself and I am leading, abort the role update.
							else if this.ordinal >= PROMOTING then sequencer.LatchingDuty_unit
							// If the vote is for myself and I am not leading, decide based on everyone’s votes.
							else {
								val myStateInfoAtChooseALeaderRequest = syncLocalStateInfo(currentState1)
								// Trace.trace(s"$boundParticipantId: updateRoleKnowingMyVote($myVote) - myStateInfo1=$myStateInfo1") // TODO delete line
								val inquires = for replierId <- config1.allOtherParticipants yield replierId.chooseALeader(boundParticipantId, myStateInfoAtChooseALeaderRequest)
								for {
									replies <- sequencer.LatchingDuty_sequenceTasksToArray(inquires, true)
									primaryState2 <- {
										val latestTermSeen = IArray.unsafeFromArray(replies).foldLeftWithIndex(currentState1.currentTerm)((latestTermSeen, reply, _) => reply match {
											case Success(replierVote) => if replierVote.term > latestTermSeen then replierVote.term else latestTermSeen
											case _: Failure[Vote[ParticipantId]] => latestTermSeen
										})
										Trace.trace(s"Replied votes=${replies.zip(config1.allOtherParticipants).mkString("[", ", ", "]")}, latestTermSeen=$latestTermSeen, myVote=$myVote1") // TODO delete line
										updateTermIfLessThan(latestTermSeen)
									}
									_ <- {
										if haveToAbort then sequencer.LatchingDuty_unit
										else primaryState2 match {
											case Inaccessible =>
												illegalStateQuiesce()
												sequencer.LatchingDuty_unit

											case accessible2: Accessible =>
												// If the term was bumped (while waiting the votes from the other participants or due to a higher term seen in them), then the role update is responsibility of `StatefulRole.onTermBumped`; so abort this update. Restarting the role update here might collide with role changes caused by the bump.
												if accessible2.currentTerm > currentState1.currentTerm then {
													if assertionsEnabled then assert(this.ordinal < PROMOTING) // because while leading the term should never change.
													sequencer.LatchingDuty_unit
												} else {
													val myStateInfo2 = syncLocalStateInfo(accessible2)
													val highestBallotSeenInVotes = IArray.unsafeFromArray(replies).foldLeftWithIndex(myStateInfo2.ballot)((highestBallot, reply, _) => reply match {
														case Success(replierVote) => if replierVote.ballot > highestBallot then replierVote.ballot else highestBallot
														case _: Failure[Vote[ParticipantId]] => highestBallot
													})
													val aHigherBallotHaveBeenSeenInVotes = updateBallotIfLowerThan(myStateInfo2, highestBallotSeenInVotes)
													if aHigherBallotHaveBeenSeenInVotes || myStateInfo2.ballot != myStateInfoAtChooseALeaderRequest.ballot then {
														// TODO consider the inclusion of the StateInfo in Vote in order to keep the StateInfo instances with the highest ballot seen. This would save howAreYou calls to participants for which the StateInfo in the Vote already corresponds to the new ballot. Note that this safe would occur only when restarting the role update due to a higher ballot seen in votes.
														Trace.trace(s"Restarting due ${if aHigherBallotHaveBeenSeenInVotes then "a higher ballot seen in votes" else "to a ballot bump"}.")
														updateRole(accessible2)
													} else {
														val config2 = deriveConfigurationFrom(accessible2)
														// TODO make decideMyVote support the commitIndex-auto-bump like the `updateRoleOmnisciently` if possible
														val myVote2 = config2.decideMyVote(syncLocalStateInfo(accessible2), memorizedParticipantInfosToArray(config2))
														become(config2.determineRole(accessible2, primaryStateFence, myVote2, replies))
														sequencer.LatchingDuty_unit
													}
												}
										}
									}
								} yield ()
							}
						}
						// else (if the successful answers to the howAreYou RPC are not a majority)
						else {
							become(Isolated(primaryStateFence))
							sequencer.LatchingDuty_unit
						}
					}

					def start(primaryState1: PrimaryState): sequencer.LatchingDuty[Unit] = {
						incumbentUpdateRoleSerial = serial
						// Trace.trace(s"$boundParticipantId: updateRole($inquirerId, $inquirerInfo) was called") // TODO delete line
						if currentRole ne this then sequencer.LatchingDuty_unit
						else {
							val myStateInfo1 = syncLocalStateInfo(primaryState1)
							// Trace.trace(s"$boundParticipantId: updateRole($inquirerId, $inquirerInfo) - primaryState=$primaryState") // TODO delete line
							for {
								myVote <- determineMyVote(primaryState1, true)
								_ <- {
									if haveToAbort then sequencer.LatchingDuty_unit
									// if the leader is a ghost
									else for {
										primaryState2 <- primaryStateFence.causalAnchor()
										_ <- {
											// Trace.trace(s"$boundParticipantId: updateRole($inquirerId, $inquirerInfo) - myVote=$myVote") // TODO delete line
											if haveToAbort then sequencer.LatchingDuty_unit
											// else if excluded
											else primaryState2 match {
												case Inaccessible =>
													illegalStateQuiesce()
													sequencer.LatchingDuty_unit

												case accessible2: Accessible =>
													if myVote.isBlank then {
														val config2 = deriveConfigurationFrom(accessible2)
														if assertionsEnabled then assert(config2.isInstanceOf[StableConfig] && !config2.desiredParticipants.contains(boundParticipantId))
														become(Retiring(accessible2.currentTerm, config2.term, config2.changeIndex, config2.allParticipants))
														sequencer.LatchingDuty_unit
													} else {
														val stateInfo2 = syncLocalStateInfo(accessible2)
														if stateInfo2.ballot == myVote.ballot then updateRoleKnowingMyVote(accessible2, myVote)
														else {
															Trace.trace(s"Restarting due to a ballot bump: currentBallot=${stateInfo2.ballot}, myVote.ballot=${myVote.ballot}")
															updateRole(accessible2)
														}
													}
											}
										}
									} yield ()
								}
							} yield ()
						}
					}

					val updateCovenant = updateRoleCoalescing.contend(true) {
						maybePreviousUpdateRoleExecution =>
							val myCurrentStateInfo = syncLocalStateInfo(primaryState0)
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
					updateCovenant.andThen(_ => Trace.trace(s"Execution #$serial ended"))
				}
			}

			/** Updates the [[Role]] of this [[ConsensusParticipant]] and then returns the [[sequencer.LatchingDuty]] returned by the [[Role.onCommandFromClient]] method applied to the updated [[Role]].
			 * @return a [[sequencer.Task]] returned by [[Role.onCommandFromClient]] applied to the updated [[Role]] */
			final def updateRoleAndThenCallsOnCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag)(using Context): sequencer.LatchingDuty[ResponseToClient] = {
				Trace.step("updateRoleAndThenCallsOnCommandFromClient") {
					Trace.trace(s"Current role=${RoleOrdinal_nameOf(ordinal)}, attemptFlag=$attemptFlag, memorizedInfos=$memorizedParticipantInfos.")
					if !attemptFlag.isInternalVacateHandoff && attemptFlag != FIRST_ATTEMPT then startNewBallot() // TODO this ballot bump may cause unnecessary "determineVote" restarts that may never converge when many clients call concurrently. The ballot should be bumped only if it is equal to the ballot used by the previous participant. So, the ballot should be included in the data propagated through the client to the next participant.
					for {
						_ <- updateRole()
						result <- {
							if currentRole.ordinal >= FOLLOWER then currentRole.onCommandFromClient(command, FIRST_ATTEMPT)
							else {
								val nextAttemptFlag = if attemptFlag == REDIRECTED then LEADERSHIP_VACATED else attemptFlag.withInternalBitsCleared
								currentRole match {
									case stateful: StatefulRole =>
										for primaryState <- stateful.primaryStateFence.causalAnchor() yield Unable(
											nextAttemptFlag,
											deriveConfigurationFrom(primaryState).otherProbableParticipants
										)
									case retiring: Retiring =>
										sequencer.LatchingDuty_ready(Unable(
											nextAttemptFlag,
											ListSet.newBuilder[ParticipantId].addAll(retiring.newParticipants).addAll(cluster.getOtherProbableParticipant).result()
										))
									case stateless =>
										sequencer.LatchingDuty_ready(Unable(nextAttemptFlag, cluster.getOtherProbableParticipant))
								}
							}

						}
					} yield result
				}
			}


			/** Derives the active [[Configuration]] state from the current [[PrimaryState]] and the [[commitIndex]].
			 * Depends on, and updates, the [[currentConfig]]. Also updates other derived state.
			 *
			 * CAUTION: the provided [[PrimaryState]] instance must be the current one. So, this method must be called only within the synchronous part of consumers subscribed synchronously to the [[sequencer.LatchingDuty]] returned by either [[sequencer.CausalFence.advance]]-like or [[sequencer.CausalFence.causalAnchor]] methods, passing the [[PrimaryState]] provided to the consumer. This requirement is needed becase this method's side effects update derived state.
			 *  @note Accessing the current [[Configuration]] through this method ensures that the current [[Configuration]] is updated before any other derived-state update that depend on it.
			 * @param currentPrimaryState the instance of the causally anchored [[PrimaryState]].
			 * @return a [[Configuration]] derived from the provided [[PrimaryState]]. */
			def deriveConfigurationFrom(currentPrimaryState: PrimaryState)(using Context): Configuration = {
				if assertionsEnabled then assert(currentPrimaryState eq primaryStateFence.committedState)

				currentPrimaryState match {
					case accessible: Accessible =>
						val indexOfTopConfigChange = accessible.indexOfTopConfigChange
						if indexOfTopConfigChange > 0 then {
							val oldConfig = currentConfig
							val desiredConfigChange: ConfigChange[ParticipantId] | Null = accessible.getRecordAt(indexOfTopConfigChange).asInstanceOf[ConfigChange[ParticipantId]] match {
								case stable: StableConfigChange[ParticipantId] @unchecked =>
									if commitIndex >= indexOfTopConfigChange then stable
									else if stable.isCoupleOf(oldConfig.backingConfigChange) then null
									else stable.recreateCouple

								case transitional: TransitionalConfigChange[ParticipantId] @unchecked =>
									transitional
							}
							if desiredConfigChange != null && desiredConfigChange != oldConfig.backingConfigChange then {
								val newConfig = Configuration_from(desiredConfigChange, indexOfTopConfigChange)
								// Update the derived state stored in the `currentRole` instance. Only the Leader role has such state as of this writing.
								currentRole.onActiveConfigChanged(accessible, oldConfig, newConfig, indexOfTopConfigChange)
								currentConfig = newConfig
								// Inform the cluster service and notify the listeners about the configuration change.
								cluster.notifyActiveConfigChanged(desiredConfigChange, indexOfTopConfigChange, currentRole.ordinal)
								notifyListeners(_.onActiveConfigChanged(currentRole.ordinal, accessible.currentTerm, indexOfTopConfigChange, desiredConfigChange))
							}
						}

					case Inaccessible =>
						currentConfig = NoConfig
				}
				currentConfig
			}

			/** Queues an updater of the [[PrimaryState.currentTerm]] that does the following: updates the [[PrimaryState.currentTerm]] if the provided [[Term]] is higher than it at the moment the updater is executed.
			 * @param seenTerm the [[Term]] to update the [[PrimaryState]] with, provided it is higher than the [[PrimaryState.currentTerm]] when the queued updater is executed.
			 * @param previousTermRef the [[Term]] value in this reference object is overwritten with the [[PrimaryState.currentTerm]] corresponding to the [[PrimaryState]] before the causally anchored advance is performed.
			 * @note About the safety of reusing the same [[TermRef]] instance for different calls: The value is guaranteed to reflect the expected value provided it is read within the synchronous part of a synchronously subscribed consumer to the [[sequencer.LatchingDuty]] returned by [[updateTermIfLessThan]]. See the game-changing-invariant in [[Doer.CausalFence]]. */
			protected def updateTermIfLessThan(seenTerm: Term, previousTermRef: TermRef = defaultPreviousTermRef)(using Trace.Context): sequencer.LatchingDuty[PrimaryState] =
				Trace.step("updateTermIfLessThan") {
					for primaryState1 <- primaryStateFence.advanceIf { (primaryState0: PrimaryState) => 
						previousTermRef.elem = primaryState0.currentTerm
						if seenTerm > primaryState0.currentTerm then Maybe(primaryState0.withTermUpdated(seenTerm))
						else Maybe.empty	
					} yield if primaryState1.currentTerm > previousTermRef.elem then onTermBumped(primaryState1) else primaryState1
				}

			private def memorizedParticipantInfosToArray(currentConfig: Configuration): IArray[StateInfo] = {
				currentConfig.allOtherParticipants.mapWithIndex { (participantId, _) => memorizedParticipantInfos.get(participantId) }
			}

			/** Called by [[updateTermIfLessThan]] if the [[PrimaryState.currentTerm]] is updated. */
			def onTermBumped(primaryState: PrimaryState)(using Context): PrimaryState = primaryState
		}

		/** A terminal [[Role]] that indicates this [[ConsensusParticipant]] service will be ready to be disposed after all the in-flight RPC calls it did have been heard.
		 *
		 * Taken when either:
		 *		- the bound participant has been excluded from the participant and completed its retirement.
		 *		- this [[ConsensusParticipant]] service was forcibly quiesced by executing the [[sequencer.Duty]] returned by the [[quiesces]] method.
		 *		- an ineludible failure occurred. */
		private final class Quiesced(val motive: Try[String]) extends Role {
			override val ordinal: RoleOrdinal = QUIESCED
			override val rank: ElectionRank = ElectionRank_from(ordinal)

			/** A [[sequencer.LatchingDuty]] that is fulfilled when the all the allocated [[Workspace]]s are released. */
			def completed: sequencer.LatchingDuty[Unit] = workspaceReleasedCovenant

			override def onEnter(previousRole: Role)(using Context): Unit = {
				Trace.step("Quiesced.onEnter") {
					notifyListeners(_.onBecameQuiesced(previousRole.ordinal, previousRole.getCommittedPrimaryState.currentTerm, motive))
					retirementDriverByParticipantId.clear()
					cluster.notifyQuiesced(motive)
				}
			}

			override def syncLocalStateInfo(primaryState: PrimaryState)(using Context): StateInfo =
				buildIneligibleInfo(PRE_INIT)

			override def determineMyVote(primaryState0: PrimaryState, blankVoteIfRoleChanges: Boolean)(using Context): sequencer.LatchingDuty[Vote[ParticipantId]] =
				yieldsBlankVote(PRE_INIT)

			override def onHowAreYou(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.LatchingDuty[StateInfo] = {
				checkWithin()
				updateSeenStateInfo(buildIneligibleInfo(PRE_INIT), inquirerId, inquirerInfo)
				sequencer.LatchingDuty_ready(buildIneligibleInfo(PRE_INIT))
			}

			override def onChooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.LatchingDuty[Vote[ParticipantId]] = {
				checkWithin()
				updateSeenStateInfo(buildIneligibleInfo(PRE_INIT), inquirerId, inquirerInfo)
				yieldsBlankVote(PRE_INIT)
			}

			override def onAppendRecords(inquirerId: ParticipantId, inquirerTerm: Term, prevRecordIndex: RecordIndex, prevRecordTerm: Term, records: GenIndexedSeq[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.LatchingDuty[AppendResult] = {
				checkWithin()
				sequencer.LatchingDuty_ready(AppendResult(PRE_INIT, Long.MaxValue, ordinal))
			}

			/** @inheritdoc
			 * This implementation responds with a rejection that propagates the received `attemptFlag` or-ing the [[FALLBACK]] bit to alert the participant with which the client would try next.
			 * Why the [[FALLBACK]] bit? Because the behavior of a [[Quiesced]] and a non-existent participant should be similar, given [[Quiesced]] is just a transient state before becoming inexistent. */
			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.LatchingDuty[ResponseToClient] = {
				checkWithin()
				sequencer.LatchingDuty_ready(Unable(attemptFlag.withInternalBitsCleared | FALLBACK, cluster.getOtherProbableParticipant))
			}

			override def requestConfigChange(requestId: ConfigChangeRequestId, desiredParticipantsSet: Set[ParticipantId], priorAnswer: Maybe[ConfigChangeResponse]): sequencer.LatchingDuty[ConfigChangeResponse] = {
				val myCurrentStateInfo = buildIneligibleInfo(PRE_INIT)
				// If a prior answer is provided, update the current ballot and memorizedParticipantInfos
				priorAnswer.foreach { priorResponse =>
					updateBallotIfLowerThan(myCurrentStateInfo, priorResponse.latestBallotSeen)
				}
				sequencer.LatchingDuty_ready(STOPPED(currentBallot))
			}

			override def onQuiescencePermitted(grantorId: ParticipantId, indexOfGrantedStableConfigChange: RecordIndex): Unit = ()
		}

		private final def Quiesced(motive: Try[String]): Maybe[Quiesced] = {
			if currentRole.ordinal == QUIESCED then Maybe.empty
			else Maybe(new Quiesced(motive))
		}

		/** A transitional [[Role]] before [[Quiesced]] to which a participant transitions to when a [[StableConfigChange]] that excludes it becomes active.
		 * The life of this [[Role]] last until a stable [[Leader]] of a subsequent [[Term]] authorizes this participant to quiesce.
		 *
		 * This [[Role]] is part of the **Retiring Quorum-Buffering** mechanism.
		 * The purpose of this mechanism is to maintain the quorum safety of the old participant set during joint consensus.
		 * By holding excluded participants in the [[RETIRING]] role, the system ensures they contribute toward the old set's quorum. Although they do not cast a specific vote, they effectively lower the required threshold of active votes by one, acting as a neutral "don't care" participant until a succeeding leader (líder sucesor) establishes a stable majority in the new configuration.
		 *
		 * Since this role must exist for that reason, we also take advantage of its presence to wait for the [[RetirementDriver]]s to conclude their job. In this scenario, the job of the [[RetirementDriver]]s of this retiring ex-leader will overlap with the job of the [[RetirementDriver]]s of the succeeding [[Leader]], but, if I am not mistaken, this overlap is more beneficial than harmful because it removes some burden to the new [[Leader]].
		 * @param finalTerm the [[Term]] during which this participant became [[Retiring]].
		 * @param excludingConfigIndex the index of the [[StableConfigChange]] that excluded this participant causing its retirement.
		 * @param newParticipants the [[StableConfigChange.newParticipants]] of the [[StableConfigChange]] that excluded this participant while it was [[Leader]]. */
		private final class Retiring(val finalTerm: Term, val termAtExcludingConfigIndex: Term, val excludingConfigIndex: RecordIndex, val newParticipants: IArray[ParticipantId]) extends Role {
			override val ordinal: RoleOrdinal = RETIRING
			override val rank: ElectionRank = ElectionRank_from(ordinal)

			if assertionsEnabled then assert(!newParticipants.contains(boundParticipantId))

			override def onEnter(previous: Role)(using Trace.Context): Unit = {
				notifyListeners(_.onRetiring(previous.ordinal, finalTerm))
				becomeQuiescedIfEligible(excludingConfigIndex)
			}

			override def syncLocalStateInfo(primaryState: PrimaryState)(using Context): StateInfo =
				syncLocalStateInfo()

			private def syncLocalStateInfo(): StateInfo = {
				if stateInfoExposedInLastInteraction.tiesWith(finalTerm, rank, termAtExcludingConfigIndex, excludingConfigIndex, excludingConfigIndex) then {
					if stateInfoExposedInLastInteraction.ballot != currentBallot then stateInfoExposedInLastInteraction = StateInfo(finalTerm, rank, termAtExcludingConfigIndex, excludingConfigIndex, excludingConfigIndex, currentBallot)
				} else {
					currentBallot += 1
					stateInfoExposedInLastInteraction = StateInfo(finalTerm, rank, termAtExcludingConfigIndex, excludingConfigIndex, excludingConfigIndex, currentBallot)
				}
				stateInfoExposedInLastInteraction
			}

			override def determineMyVote(primaryState0: PrimaryState, blankVoteIfRoleChanges: Boolean)(using Context): sequencer.LatchingDuty[Vote[ParticipantId]] =
				yieldsBlankVote(finalTerm)

			override def onHowAreYou(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.LatchingDuty[StateInfo] = {
				checkWithin()
				var stateInfo = syncLocalStateInfo()
				if updateSeenStateInfo(stateInfo, inquirerId, inquirerInfo) then stateInfo = syncLocalStateInfo()
				sequencer.LatchingDuty_ready(stateInfo)
			}

			override def onChooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.LatchingDuty[Vote[ParticipantId]] = {
				checkWithin()
				if updateSeenStateInfo(syncLocalStateInfo(), inquirerId, inquirerInfo) then syncLocalStateInfo()
				yieldsBlankVote(finalTerm)
			}

			override def requestConfigChange(requestId: ConfigChangeRequestId, desiredParticipantsSet: Set[ParticipantId], priorAnswer: Maybe[ConfigChangeResponse]): sequencer.LatchingDuty[ConfigChangeResponse] = {
				Trace.init(() => s"$boundParticipantId: Retiring.requestConfigChange") {
					var myCurrentStateInfo = syncLocalStateInfo()
					// If a prior answer is provided, update the current ballot and memorizedParticipantInfos
					priorAnswer.foreach { priorResponse =>
						if updateBallotIfLowerThan(myCurrentStateInfo, priorResponse.latestBallotSeen) then myCurrentStateInfo = syncLocalStateInfo()
					}
					sequencer.LatchingDuty_ready(EXCLUDED(myCurrentStateInfo.ballot))
				}
			}

			/** @inheritdoc
			 * This implementation responds with a rejection that propagates the received `attemptFlag`. */
			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.LatchingDuty[ResponseToClient] = {
				sequencer.LatchingDuty_ready(Unable(
					attemptFlag.withInternalBitsCleared,
					ListSet.newBuilder[ParticipantId].addAll(newParticipants).addAll(cluster.getOtherProbableParticipant).result()
				))
			}

			override def onAppendRecords(inquirerId: ParticipantId, inquirerTerm: Term, prevRecordIndex: RecordIndex, prevRecordTerm: Term, records: GenIndexedSeq[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.LatchingDuty[AppendResult] = {
				Trace.init(() => s"$boundParticipantId: Retiring.onAppendRecords") {
					// Check if the received records contain a later [[TransitionalConfigChange]] that includes this participant.
					val relativeIndexOfTheIncludingConfigChange = records.lastIndexWhere {
						case tcc: TransitionalConfigChange[ParticipantId] @unchecked => tcc.newParticipants.contains(boundParticipantId)
						case _ => false
					}
					val indexOfTheIncludingConfigChange = prevRecordIndex + 1 + relativeIndexOfTheIncludingConfigChange
					// If not, return a rejection.
					if relativeIndexOfTheIncludingConfigChange < 0 || indexOfTheIncludingConfigChange <= excludingConfigIndex
					then sequencer.LatchingDuty_ready(AppendResult(finalTerm, excludingConfigIndex + 1, ordinal))
					// If yes, become starting and redirect the append records request to the new role.
					else {
						val includingConfigChange = records(relativeIndexOfTheIncludingConfigChange).asInstanceOf[TransitionalConfigChange[ParticipantId]] // Safe: the element at this index was matched as TransitionalConfigChange[ParticipantId] by lastIndexWhere above.
						val activeParticipants = ListSet.newBuilder.addAll(includingConfigChange.oldParticipants).addAll(includingConfigChange.newParticipants).result()
						become(Starting(indexOfTheIncludingConfigChange, activeParticipants))
							.onAppendRecords(inquirerId, inquirerTerm, prevRecordIndex, prevRecordTerm, records, leaderCommit, termAtLeaderCommit)
					}
				}
			}

			override def onQuiescencePermitted(grantorId: ParticipantId, indexOfGrantedStableConfigChange: RecordIndex): Unit = {
				Trace.init(() => s"$boundParticipantId: Retiring.onQuiescencePermitted") {
					quiescenceGrantor = Maybe(grantorId)
					indexOfStableConfigChangeForWhichQuiescenceWasPermitted = indexOfGrantedStableConfigChange
					becomeQuiescedIfEligible(excludingConfigIndex)
				}
			}
		}

		/**
		 * @param finalTerm the [[Term]] during which this participant became [[Retiring]].
		 * @param excludingConfigIndex the index of the [[StableConfigChange]] that excluded this participant causing its retirement.
		 * @param newParticipants the [[StableConfigChange.newParticipants]] of the [[StableConfigChange]] that excluded this participant while it was [[Leader]]. */
		private final def Retiring(finalTerm: Term, termAtExcludingConfigIndex: Term, excludingConfigIndex: RecordIndex, newParticipants: IArray[ParticipantId]): Maybe[Retiring] = {
			currentRole match {
				case retiring: Retiring if retiring.finalTerm == finalTerm && retiring.excludingConfigIndex == excludingConfigIndex => Maybe.empty
				case _ => Maybe(new Retiring(finalTerm, termAtExcludingConfigIndex, excludingConfigIndex, newParticipants))
			}
		}

		/** The behavior when the participant has the [[STARTING]] role. This is a transitory role during which the participant state is initialized.
		 * When initialization is completed it transitions to the [[Isolated]] state.
		 * @param indexOfTheIncludingConfigChange the [[RecordIndex]] of the [[TransitionalConfigChange]] that caused this [[ConsensusParticipant]] service to join.
		 * @param participantsInTheIncludingConfigChange the active participants in the [[TransitionalConfigChange]] pointed by `indexOfTheIncludingConfigChange`.
		 * TODO consider adding a parameter with the set of active participants in the including [[ConfigChange]], to pass it to the Joining role, in order to return a more updated set of active participants when responding with [[Unable]] to a command from a client. */
		private final class Starting(val indexOfTheIncludingConfigChange: RecordIndex, participantsInTheIncludingConfigChange: ListSet[ParticipantId]) extends Role {
			override val ordinal: RoleOrdinal = STARTING
			override val rank: ElectionRank = ElectionRank_from(ordinal)
			/** Is fulfilled after initializing this [[ConsensusParticipant]] and becoming another [[Role]]: [[Joining]], [[Isolated]], or [[Quiesced]]. */
			private val startingCompletedCovenant: sequencer.Covenant[PrimaryState] = sequencer.Covenant()

			override def onEnter(previous: Role)(using Trace.Context): Unit = {
				Trace.step("Starting.onEnter") {
					notifyListeners(_.onStarting(previous.ordinal, indexOfTheIncludingConfigChange))

					storage.load.subscribe {
						case Success(loadedWorkspace) =>
							val indexOfTopConfigChange = loadedWorkspace.indexOfTopConfigChange
							val rulingConfigChange = {
								if indexOfTopConfigChange == 0 then {
									loadedWorkspace.setCurrentTerm(PRE_INIT)
									new TransitionalConfigChange[ParticipantId](PRE_INIT, "Initial-Config", Set.empty, cluster.getInitialParticipants) // TODO consider using the set provided in the Starting constructor instead, and remove the `getInitialParticipants` method.

								} else loadedWorkspace.getRecordAt(indexOfTopConfigChange).asInstanceOf[ConfigChange[ParticipantId]] match {
									// If the top configuration change in the log is a stable one, then the previous transitional configuration change rules until the commitIndex crosses the index of top stable one, which is not happening now because the commitIndex is initialized with zero.
									case stableConfigChange: StableConfigChange[ParticipantId] @unchecked =>
										if commitIndex >= indexOfTopConfigChange then stableConfigChange
										else stableConfigChange.recreateCouple

									// If the top configuration change in the log is a transitional one, then it rules immediately.
									case transitionalConfigChange: TransitionalConfigChange[ParticipantId] @unchecked =>
										transitionalConfigChange
								}
							}
							val config = Configuration_from(rulingConfigChange, indexOfTopConfigChange)
							val isSeed = indexOfTheIncludingConfigChange == 0
							if isSeed && !config.allParticipants.contains(boundParticipantId) then {
								become(Quiesced(Success(s"Start-up aborted because this ConsensusParticipant instance does not belong to the active cluster-configuration.")))
								startingCompletedCovenant.fulfillUnsafe(Inaccessible)
							}
							else {
								currentConfig = config
								val primaryState = new Accessible(loadedWorkspace)
								val primaryStateFence = sequencer.CausalFence[PrimaryState](primaryState)
								notifyListeners(_.onStarted(previous.ordinal, primaryState.currentTerm, rulingConfigChange, isSeed))
								if isSeed then become(Isolated(primaryStateFence))
								else become(Joining(primaryStateFence, indexOfTheIncludingConfigChange, participantsInTheIncludingConfigChange))
								startingCompletedCovenant.fulfillUnsafe(primaryState)
							}

						case failure@Failure(e) =>
							Trace.error(s"$boundParticipantId: Unexpected error while loading the consensus-service's workspace:", e)
							become(Quiesced(failure.castTo[String]))
							startingCompletedCovenant.fulfillUnsafe(Inaccessible)
					}
				}
			}

			override def syncLocalStateInfo(primaryState: PrimaryState)(using Context): StateInfo =
				buildIneligibleInfo(primaryState.currentTerm)

			override def determineMyVote(primaryState0: PrimaryState, blankVoteIfRoleChanges: Boolean)(using Context): sequencer.LatchingDuty[Vote[ParticipantId]] = {
				checkWithin()
				for {
					primaryState1 <- startingCompletedCovenant // TODO ignoring the received primary state is suspicious. Analyze it.
					response <- currentRole.determineMyVote(primaryState1, blankVoteIfRoleChanges)
				} yield response
			}

			override def onHowAreYou(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.LatchingDuty[StateInfo] = {
				checkWithin()
				for {
					_ <- startingCompletedCovenant
					response <- currentRole.onHowAreYou(inquirerId, inquirerInfo)
				} yield response
			}

			override def onChooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.LatchingDuty[Vote[ParticipantId]] = {
				checkWithin()
				for {
					_ <- startingCompletedCovenant
					response <- currentRole.onChooseALeader(inquirerId, inquirerInfo)
				} yield response
			}

			override def onAppendRecords(inquirerId: ParticipantId, inquirerTerm: Term, prevRecordIndex: RecordIndex, prevRecordTerm: Term, records: GenIndexedSeq[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.LatchingDuty[AppendResult] = {
				checkWithin()
				for {
					_ <- startingCompletedCovenant
					response <- currentRole.onAppendRecords(inquirerId, inquirerTerm, prevRecordIndex, prevRecordTerm, records, leaderCommit, termAtLeaderCommit)
				} yield response
			}

			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.LatchingDuty[ResponseToClient] = {
				checkWithin()
				for {
					_ <- startingCompletedCovenant
					rtc <- currentRole.onCommandFromClient(command, attemptFlag)
				} yield rtc
			}

			override def requestConfigChange(requestId: ConfigChangeRequestId, desiredParticipantsSet: Set[ParticipantId], priorAnswer: Maybe[ConfigChangeResponse]): sequencer.LatchingDuty[ConfigChangeResponse] = {
				checkWithin()
				for {
					_ <- startingCompletedCovenant
					response <- currentRole.requestConfigChange(requestId, desiredParticipantsSet, priorAnswer)
				} yield response
			}

			override def onQuiescencePermitted(grantorId: ParticipantId, indexOfGrantedStableConfigChange: RecordIndex): Unit = {
				quiescenceGrantor = Maybe(grantorId)
				indexOfStableConfigChangeForWhichQuiescenceWasPermitted = indexOfGrantedStableConfigChange
			}
		}

		private final def Starting(indexOfTheIncludingConfigChange: RecordIndex, participantsInTheIncludingConfigChange: ListSet[ParticipantId]): Maybe[Starting] = {
			currentRole match {
				case starting: Starting if starting.indexOfTheIncludingConfigChange == indexOfTheIncludingConfigChange => Maybe.empty
				case _ => Maybe(new Starting(indexOfTheIncludingConfigChange, participantsInTheIncludingConfigChange))
			}
		}

		private final class Joining(psf: sequencer.CausalFence[PrimaryState], val indexOfTheIncludingConfigChange: RecordIndex, participantsInTheIncludingConfigChange: ListSet[ParticipantId]) extends StatefulRole(psf) {
			/** The ordinal corresponding to this [[Role]] */
			override val ordinal: RoleOrdinal = JOINING
			override val rank: ElectionRank = ElectionRank_from(ordinal)

			override def onEnter(previous: Role)(using Trace.Context): Unit = {
				notifyListeners(_.onJoining(previous.ordinal, indexOfTheIncludingConfigChange))
			}

			override def determineMyVote(primaryState0: PrimaryState, blankVoteIfRoleChanges: Boolean)(using Context): sequencer.LatchingDuty[Vote[ParticipantId]] = {
				yieldsBlankVote(primaryState0.currentTerm)
			}

			override def onChooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.LatchingDuty[Vote[ParticipantId]] = {
				Trace.init(() => s"$boundParticipantId: Joinin.onChooseALeader") {
					for primaryState <- primaryStateFence.causalAnchor() yield {
						updateSeenStateInfo(syncLocalStateInfo(primaryState), inquirerId, inquirerInfo)
						blankVote(primaryState.currentTerm)
					}
				}
			}

			/** @inheritdoc
			 * This implementation responds with a rejection that propagates the received `attemptFlag`. */
			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.LatchingDuty[ResponseToClient] = {
				checkWithin()
				sequencer.LatchingDuty_ready(Unable(attemptFlag.withInternalBitsCleared, participantsInTheIncludingConfigChange))
			}

			override def requestConfigChange(primaryState: Accessible, requestId: ConfigChangeRequestId, desiredParticipants: Set[ParticipantId], ballotWasUpdated: Boolean)(using Trace.Context): sequencer.LatchingDuty[ConfigChangeResponse] = {
				Trace.step("Joining.requestConfigChange") {
					sequencer.LatchingDuty_ready(CATCHING_UP(syncLocalStateInfo(primaryState).ballot))
				}
			}
		}

		private final def Joining(psf: sequencer.CausalFence[PrimaryState], indexOfTheIncludingConfigChange: RecordIndex, participantsInTheIncludingConfigChange: ListSet[ParticipantId]): Maybe[Joining] = {
			currentRole match {
				case joining: Joining if joining.indexOfTheIncludingConfigChange == indexOfTheIncludingConfigChange && (joining.primaryStateFence eq psf) => Maybe.empty
				case _ => Maybe(new Joining(psf, indexOfTheIncludingConfigChange, participantsInTheIncludingConfigChange))
			}
		}

		/**
		 * Behavior when the participant has the [[ISOLATED]] role. Taken when reachability to a majority of the participants was not achieved or after the [[STARTING]] role has completed.
		 * The participant transitions to this state after [[Starting]] or when reachability to other participants drops below [[smallestMajority]].
		 * This state is abandoned when a majority of the participants are reachable.
		 * [[Vote]]s cast by participants in this state are ignored.
		 */
		private class Isolated(psf: sequencer.CausalFence[PrimaryState]) extends StatefulRole(psf) {
			override val ordinal: RoleOrdinal = ISOLATED
			override val rank: ElectionRank = ElectionRank_from(ordinal)

			/**
			 * The main loop of the isolated state.
			 * It checks if the current term leader is reachable or the reachable participants including itself are the majority.
			 * If so, it becomes a follower or a candidate respectively.
			 * If not, it stays in the isolated state and checks again after a while.
			 */
			override def onEnter(previous: Role)(using Trace.Context): Unit = {
				notifyListeners(_.onBecameIsolated(previous.ordinal, psf.committedState.currentTerm))
			}
			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.LatchingDuty[ResponseToClient] = {
				Trace.init(() => s"$boundParticipantId: Isolated.onCommandFromClient") {
					checkWithin()
					for {
						primaryState <- primaryStateFence.causalAnchor()
						response <- {
							if currentRole ne this then currentRole.onCommandFromClient(command, attemptFlag)
							else updateRoleAndThenCallsOnCommandFromClient(command, attemptFlag)
						}
					} yield response
				}
			}

			override def requestConfigChange(primaryState0: Accessible, requestId: ConfigChangeRequestId, desiredParticipants: Set[ParticipantId], ballotWasUpdated: Boolean)(using Context): sequencer.LatchingDuty[ConfigChangeResponse] = {
				Trace.step("Isolated.requestConfigChange") {
					Trace.trace(s"Updating role from ${RoleOrdinal_nameOf(currentRole.ordinal)} due to a configuration change request. ")
					for {
						_ <- updateRole(primaryState0) // TODO consider making updateRole return the current primary state, so that the causalAnchor method call is not needed here (and other places also).
						primaryState <- primaryStateFence.causalAnchor()
						response <- {
							if currentRole ne this then currentRole.requestConfigChange(requestId, desiredParticipants, Maybe.empty)
							else sequencer.LatchingDuty_ready(SECLUDED(syncLocalStateInfo(primaryState).ballot))
						}
					} yield response
				}
			}
		}

		private final def Isolated(psf: sequencer.CausalFence[PrimaryState]): Maybe[Isolated] = {
			currentRole match {
				case isolated: Isolated if isolated.primaryStateFence eq psf => Maybe.empty
				case _ => Maybe(new Isolated(psf))
			}
		}

		/** A transitional [[Role]] that:
		 *		- always comes after [[Leader]] when a later [[Term]] is seen in a [[StateInfo]] of another participant;
		 *		- updates the [[Term]] to the provided one, and then transitions to [[Isolated]] or [[Retiring]].
		 *
		 * Note that this [[Role]] is not hosted when the [[Term]] is updated by the [[StatefulRole.onAppendRecords]] handler, which does the update itself.
		 * It behaves as [[Isolated]] except that, in the [[onEnter]] life-cycle stage it enqueues an updater of the [[PrimaryState.currentTerm]] that sets it to the latest [[Term]] seen if not already; and then transitions to [[Retiring]] if this participant is excluded from the active [[Configuration]], or to [[Isolated]] otherwise.
		 *
		 * @param endedTerm the [[Term]] that concluded, during which this participant acted as [[Leader]].
		 * TODO Replace this class with a method that transitions to [[Isolated]] or [[Retiring]] in a synchronous manner, and then enqueues a term update. The problem with the current class approach is the incorrect isolated-like behavior during the transition to retiring.
		 */
		private final class HandingOff(endedTerm: Term, latestTermSeen: Term, psf: sequencer.CausalFence[PrimaryState]) extends Isolated(psf) {
			override val ordinal: RoleOrdinal = HANDING_OFF

			override def onEnter(previous: Role)(using Trace.Context): Unit = {
				notifyListeners(_.onHandingOff(endedTerm))

				for {
					primaryState1 <- primaryStateFence.advanceIf { primaryState0 =>
						if primaryState0.currentTerm < latestTermSeen then Maybe(primaryState0.withTermUpdated(latestTermSeen)) else Maybe.empty
					}
				} do if currentRole eq this then primaryState1 match {
					case Inaccessible =>
						illegalStateQuiesce()

					case accessible: Accessible =>
						// check if excluded from the new configuration.
						val updatedConfig = deriveConfigurationFrom(accessible)
						// if excluded, become Retiring
						if !updatedConfig.allParticipants.contains(boundParticipantId) then become(Retiring(accessible.currentTerm, updatedConfig.term, updatedConfig.changeIndex, updatedConfig.allParticipants))
						// else, become Isolated
						else become(Isolated(primaryStateFence))
				}
			}
		}

		private final def HandingOff(endedTerm: Term, latestTermSeen: Term, psf: sequencer.CausalFence[PrimaryState]): Maybe[HandingOff] = {
			Maybe(new HandingOff(endedTerm, latestTermSeen, psf))
		}

		/**
		 * Behavior when the participant has the follower role. Taken when reachability to a majority of the participants is achieved and one of them has the [[Leader]] role and is in a higher or equal term.
		 *
		 * In this state, the participant acknowledges the specified leader.
		 *
		 * @param followeeId The ID of the participant this participant is following.
		 */
		private final class Follower(val term: Term, val followeeId: ParticipantId, psf: sequencer.CausalFence[PrimaryState]) extends StatefulRole(psf) {
			override val ordinal: RoleOrdinal = FOLLOWER
			override val rank: ElectionRank = ElectionRank_from(ordinal)

			override def onEnter(previous: Role)(using Trace.Context): Unit = {
				notifyListeners(_.onBecameFollower(previous.ordinal, term, followeeId))
			}

			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.LatchingDuty[ResponseToClient] = {
				Trace.init(() => s"$boundParticipantId: Follower.onCommandFromClient") {
					checkWithin()
					for {
						primaryState <- primaryStateFence.causalAnchor()
						response <- {
							if currentRole ne this then currentRole.onCommandFromClient(command, attemptFlag)
							else if attemptFlag == FIRST_ATTEMPT then sequencer.LatchingDuty_ready(RedirectTo(followeeId))
							else updateRoleAndThenCallsOnCommandFromClient(command, attemptFlag)
						}
					} yield response
				}
			}

			override def requestConfigChange(primaryState0: Accessible, requestId: ConfigChangeRequestId, desiredParticipants: Set[ParticipantId], ballotWasUpdated: Boolean)(using Context): sequencer.LatchingDuty[ConfigChangeResponse] = {
				Trace.step("Follower.requestConfigChange") {
					Trace.trace(s"Updating role from ${RoleOrdinal_nameOf(currentRole.ordinal)} due to a configuration change request.")
					for {
						_ <- updateRole(primaryState0)
						primaryState1 <- primaryStateFence.causalAnchor()
						response <- {
							if currentRole ne this then currentRole.requestConfigChange(requestId, desiredParticipants, Maybe.empty)
							else sequencer.LatchingDuty_ready(ASK_THE_LEADER(followeeId, syncLocalStateInfo(primaryState1).ballot))
						}
					} yield response
				}
			}
		}

		private final def Follower(term: Term, leaderId: ParticipantId, psf: sequencer.CausalFence[PrimaryState]): Maybe[Follower] = {
			currentRole match {
				case follower: Follower if follower.term == term && follower.followeeId == leaderId && (follower.primaryStateFence eq psf) => Maybe.empty
				case _ => Maybe(new Follower(term, leaderId, psf))
			}
		}

		/** A hidden (not seen by other participants) and transitional substate of a leading participant that last until the term bump is stored.
		 * During this interval, all the RPC calls this [[ConsensusParticipant]] receives are put in standby until the bumped term is stored and role transitioned, such that responses to queries form the outside never completed in this role and, therefore, the role ordinal in responses is never [[PROMOTING]]. */
		private final class Promoting(fromTerm: Term, psf: sequencer.CausalFence[PrimaryState]) extends StatefulRole(psf) {
			/** The ordinal corresponding to this [[Role]] */
			override val ordinal: RoleOrdinal = PROMOTING
			override val rank: ElectionRank = ElectionRank_from(ordinal)

			/** Is fulfilled after bumping the term and becoming [[Leader]] if success, or [[Quiesced]] if fails to persist the primary state. */
			private val promotionCovenant: sequencer.Covenant[PrimaryState] = sequencer.Covenant()

			override def onEnter(previous: Role)(using Trace.Context): Unit =
				Trace.step("Promoting.onEnter") {
					notifyListeners(_.onPromoting(previous.ordinal, getCommittedPrimaryState.currentTerm))

					for {
						// Bump the term
						primaryState1 <- primaryStateFence.advanceIf { primaryState0 =>
							if currentRole ne this then Maybe.empty
							else primaryState0 match {
								case Inaccessible => Maybe.empty
								case accessible0: Accessible => Maybe(primaryState0.withTermUpdated(primaryState0.currentTerm.incremented))
							}
						}
					} do {
						if currentRole eq this then {
							primaryState1 match {
								case Inaccessible =>
									illegalStateQuiesce()

								case accessible1: Accessible =>
									become(Maybe(new Leader(accessible1.currentTerm, accessible1, deriveConfigurationFrom(accessible1), primaryStateFence)))
							}
						}
						promotionCovenant.fulfillUnsafe(primaryState1)
					}
				}

			override def determineMyVote(primaryState0: PrimaryState, blankVoteIfRoleChanges: Boolean)(using Context): sequencer.LatchingDuty[Vote[ParticipantId]] = {
				Trace.step("Promoting.determineMyVote") {
					checkWithin()
					for {
						primaryState1 <- promotionCovenant
						vote <- currentRole.determineMyVote(primaryState1, blankVoteIfRoleChanges)
					} yield vote
				}
			}

			override def onHowAreYou(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.LatchingDuty[StateInfo] = {
				checkWithin()
				for {
					_ <- promotionCovenant
					stateInfo <- currentRole.onHowAreYou(inquirerId, inquirerInfo)
				} yield stateInfo
			}

			override def onChooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.LatchingDuty[Vote[ParticipantId]] = {
				checkWithin()
				for {
					_ <- promotionCovenant
					vote <- currentRole.onChooseALeader(inquirerId, inquirerInfo)
				} yield vote
			}

			override def onAppendRecords(inquirerId: ParticipantId, inquirerTerm: Term, prevRecordIndex: RecordIndex, prevRecordTerm: Term, records: GenIndexedSeq[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.LatchingDuty[AppendResult] = {
				checkWithin()
				for {
					_ <- promotionCovenant
					response <- currentRole.onAppendRecords(inquirerId, inquirerTerm, prevRecordIndex, prevRecordTerm, records, leaderCommit, termAtLeaderCommit)
				} yield response
			}

			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.LatchingDuty[ResponseToClient] = {
				checkWithin()
				for {
					_ <- promotionCovenant
					response <- currentRole.onCommandFromClient(command, attemptFlag)
				} yield response
			}

			override def requestConfigChange(primaryState: Accessible, requestId: ConfigChangeRequestId, desiredParticipants: Set[ParticipantId], ballotWasUpdated: Boolean)(using Trace.Context): sequencer.LatchingDuty[ConfigChangeResponse] = {
				Trace.step("Promoting.requestConfigChange") {
					for {
						_ <- promotionCovenant
						response <- currentRole.requestConfigChange(requestId, desiredParticipants, Maybe.empty)
					} yield response
				}
			}
		}

		private inline def Promoting(fromTerm: Term, psf: sequencer.CausalFence[PrimaryState]): Maybe[Promoting] = {
			currentRole match {
				case leader: Leader if leader.leadedTerm == fromTerm && (leader.primaryStateFence eq psf) => Maybe.empty
				case _ => Maybe(new Promoting(fromTerm, psf))
			}
		}

		/**
		 * Behavior when the participant has the [[LEADER]] role. Taken when reachability to a majority of the participants is achieved, none of them is a [[Leader]] with higher or equal term, and wins the new leader election.
		 *
		 * In this state, the participant coordinates consensus decisions.
		 * @param leadedTerm the [[Term]] owned by this [[Leader]] instance.
		 * @param initialPrimaryState the current [[PrimaryState]] when this [[Leader]] instance was created. Intended to be used in the [[onEnter]] method only. Do not use elsewhere.
		 * @param initialConfig the active [[Configuration]] when this [[Leader]] instance was created. Intended to be used in the [[onEnter]] method only. Do not use elsewhere.
		 * @param wsf the [[sequencer.CausalFence]] that must be used to ensure causal ordering of the state updates. It must be propagated to subsequent [[StatefulRole]] instances.
		 * TODO replace the `initialPrimaryState` parameter with what is obtained from it. Storing an instance of [[Accessible]] is error prone.
		 */
		private final class Leader(val leadedTerm: Term, initialPrimaryState: Accessible, initialConfig: Configuration, wsf: sequencer.CausalFence[PrimaryState]) extends StatefulRole(wsf) { thisLeader =>
			/** The [[sequencer.Task]] returned by a call to [[ClusterParticipant.appendRecords]]. */
			private type AppendRequest = sequencer.Task[AppendResult]

			/** The outcome of the [[sequencer.Task]] returned by a call to [[ClusterParticipant.appendRecords]]. */
			private type AppendResponse = Try[AppendResult]

			override val ordinal: RoleOrdinal = LEADER
			override val rank: ElectionRank = ElectionRank_from(ordinal)

			/** The index of the next record to send to a participant, indexed by the participant index.
			 * This array is optimistically initialized to the first empty record index of the leader's workspace for all participants,
			 * assuming that each follower's log is already up-to-date with the leader's log. This optimistic initialization
			 * allows the leader to attempt to append new entries immediately, but if a follower's log is actually behind or inconsistent,
			 * the index will be decremented as needed until the logs are aligned.
			 * When a record is successfully replicated to a participant, the index of the next record to send to that participant is incremented.
			 * When a record is not successfully replicated to a participant, the index of the next record to send to that participant is decremented.
			 * TODO: Consider initializing the array with the first empty record index unless the last filled ones are configuration changes, in which case initialize with the index of the first of them. Why? Because sending extra [[ConfigChange]] instances is cheap and may avoid rejections due to need of an earlier [[Record]].
			 */
			private var indexOfNextRecordToSend_ByParticipantIndex: Array[RecordIndex] = Array.fill(initialConfig.allOtherParticipants.size)(initialPrimaryState.firstEmptyRecordIndex)
			/** The highest record index known to be replicated to a participant, indexed by the participant index.
			 * This array is conservatively initialized to 0 for all participants, assuming that no records are known to be replicated to any follower at the start of the leader's term.
			 * As records are successfully replicated to a participant, the corresponding value is incremented.
			 * This conservative initialization ensures that the leader does not overestimate the replication state of any follower and only advances commitIndex when a true majority is confirmed.
			 */
			private var highestRecordIndexKnownToBeAppended_ByParticipantIndex: Array[RecordIndex] = Array.fill(initialConfig.allOtherParticipants.size)(0)

			private var highestRecordIndexKnowToBeCommitted_ByParticipantIndex: Array[RecordIndex] = Array.fill(initialConfig.allOtherParticipants.size)(0)

			/** Either, the index of the [[StableConfigChange]] that excluded this leading participant causing it become a ghost leader, or zero if in joint consensus or not excluded.
			 * Set by the [[Leader.driveTheRetirements]] method, which is called by [[deriveConfigurationFrom]] when the active [[Configuration]] changes from a [[TransitionalConfig]] to a [[StableConfig]]. */
			private var indexOfConfigChangeThatExcludedThisParticipant: RecordIndex = 0

			/** The serial number of the last replication attempt. Incremented whenever the [[attemptToUpdateOtherParticipantsLogs]] method is called. */
			private var serialOfLastReplicationAttempt: Int = 0

			private var unreachableFollowersRetryWakeUp: Maybe[WakeUpToken] = Maybe.empty

			override def onEnter(previous: Role)(using Trace.Context): Unit = {
				Trace.step("Leader.onEnter") {
					notifyListeners(_.onBecameLeader(previous.ordinal, leadedTerm))

					val indexOfTopConfigChange = initialPrimaryState.indexOfTopConfigChange
					// if the log lacks a ConfigChange record (is empty), create a synthetic one with the seed participants of the initial synthetic configuration (appointed in `currentConfig` during Starting).
					if indexOfTopConfigChange == 0 then {
						for primaryState1 <- primaryStateFence.advanceIf {
							case Inaccessible => Maybe.empty
							case accessible0: Accessible => Maybe(accessible0.withSingleRecordAppended(accessible0.currentTerm, currentConfig.backingConfigChange))
						} yield startConfigChangeSecondPhase(currentConfig.backingConfigChange.asInstanceOf[TransitionalConfigChange[ParticipantId]], 1)
					}
					// if the log contains a ConfigChange record then:
					else initialPrimaryState.getRecordAt(indexOfTopConfigChange).asInstanceOf[ConfigChange[ParticipantId]] match {
						// If the top configuration change in the local log is a transitional one, continue the configuration transition process. This happens when the leader that started the first phase of the configuration change crashed or left the leadership before achieving the replication of the TransitionalConfigChange to a majority, or while storing the StableConfigChange in his persistent log.
						case tcc: TransitionalConfigChange[ParticipantId @unchecked] =>
							startConfigChangeSecondPhase(tcc, indexOfTopConfigChange)

						// If, on the contrary, is a stable one
						case scc: StableConfigChange[ParticipantId @unchecked] =>
							// ... and it was committed (commitIndex >= its index in the log), program the driving of excluded participants to retirement and authorize retiring participants to quiesce.
							if commitIndex >= indexOfTopConfigChange then thisLeader.driveTheRetirements(initialPrimaryState, initialConfig, scc, indexOfTopConfigChange)
							// ... and it wasn't committed (commitIndex < its index in the log), drive its commitment eagerly.
							else replicateUntilSuccessOrLeaderRoleIsAbandoned(initialPrimaryState, indexOfTopConfigChange)
							
					}
				}
			}


			override def onLeave(newRole: Role): Unit = {
				super.onLeave(newRole)
				unreachableFollowersRetryWakeUp.foreach(cancelWakeUp(_))
				unreachableFollowersRetryWakeUp = Maybe.empty
			}

			def isGhost: Boolean = indexOfConfigChangeThatExcludedThisParticipant > 0

			/** @inheritdoc
			 *  This implementation does two different things:
			 *  1) Updates the [[RetiringParticipantsManager]] to include any new old-configuration-only retiring participant (those that are not part of the new [[Configuration]], but still need more appends until their [[commitIndex]] reaches the index of the [[StableConfigChange]] that excluded them).
			 *  2) Recreates and initializes the [[indexOfNextRecordToSend_ByParticipantIndex]] and [[highestRecordIndexKnownToBeAppended_ByParticipantIndex]] arrays keeping the elements corresponding to the participants that remain and moving them to the appropriate index.
			 * @note This rearrangement wouldn't be necessary if maps instead of arrays were used. But considering these two collections are heavily used, efficiency was primed. */
			override def onActiveConfigChanged(currentPrimaryState: Accessible, oldConfig: Configuration, newConfig: Configuration, indexOfNewConfigChange: RecordIndex)(using Context): Unit = Trace.step("onActiveConfigChanged") {
				// Step one. Must be before step two.
				newConfig.backingConfigChange.match {
					case scc: StableConfigChange[ParticipantId] =>
						thisLeader.driveTheRetirements(currentPrimaryState, oldConfig, scc, indexOfNewConfigChange)
					case tcc: TransitionalConfigChange[ParticipantId] =>
						// If a previous configuration change excluded this leading participant but a later configuration change includes it, clear the mark that instructs itself to retire (when it sees that the previous config change is committed).
						if indexOfConfigChangeThatExcludedThisParticipant > 0 && newConfig.allParticipants.contains(boundParticipantId) then indexOfConfigChangeThatExcludedThisParticipant = 0
				}

				// Step two
				val newAllOtherParticipantsArrayLength = newConfig.allOtherParticipants.length
				val newIndexOfNextRecordToSend_ByParticipantIndex: Array[RecordIndex] = new Array(newAllOtherParticipantsArrayLength)
				val newHighestRecordIndexKnowToBeAppended_ByParticipantIndex: Array[RecordIndex] = new Array(newAllOtherParticipantsArrayLength)
				val newHighestRecordIndexKnowToBeCommitted_ByParticipantIndex: Array[RecordIndex] = new Array(newAllOtherParticipantsArrayLength)

				var participantNewIndex = newAllOtherParticipantsArrayLength
				while participantNewIndex > 0 do {
					participantNewIndex -= 1
					val participantId = newConfig.allOtherParticipants(participantNewIndex)
					val participantOldIndex = oldConfig.participantIndexOf(participantId)
					if participantOldIndex >= 0 then {
						newIndexOfNextRecordToSend_ByParticipantIndex(participantNewIndex) = indexOfNextRecordToSend_ByParticipantIndex(participantOldIndex)
						newHighestRecordIndexKnowToBeAppended_ByParticipantIndex(participantNewIndex) = highestRecordIndexKnownToBeAppended_ByParticipantIndex(participantOldIndex)
						newHighestRecordIndexKnowToBeCommitted_ByParticipantIndex(participantNewIndex) = highestRecordIndexKnowToBeCommitted_ByParticipantIndex(participantOldIndex)

					} else {
						newIndexOfNextRecordToSend_ByParticipantIndex(participantNewIndex) = indexOfNewConfigChange
						newHighestRecordIndexKnowToBeAppended_ByParticipantIndex(participantNewIndex) = 0
						newHighestRecordIndexKnowToBeCommitted_ByParticipantIndex(participantNewIndex) = 0
					}
				}
				indexOfNextRecordToSend_ByParticipantIndex = newIndexOfNextRecordToSend_ByParticipantIndex
				highestRecordIndexKnownToBeAppended_ByParticipantIndex = newHighestRecordIndexKnowToBeAppended_ByParticipantIndex
				highestRecordIndexKnowToBeCommitted_ByParticipantIndex = newHighestRecordIndexKnowToBeCommitted_ByParticipantIndex
			}

			/** Drives the excluded participant (the ones that are not active in the provided [[StableConfigChange]]) to retirement.
			 *		- If this [[Leader]] is excluded, sets the threshold [[indexOfConfigChangeThatExcludedThisParticipant]]. The replication logic checks it after successful appends to decide if a transition to the [[Retiring]] [[Role]] is needed.
			 *		- Creates and registers an instance of [[RetirementDriver]] for each excluded follower that needs more appends to become [[Retiring]].
			 * Must be called a single time whenever the participant becomes [[Leader]] with a [[StableConfig]] or the participant is leading and the active [[Configuration]] transitions to a [[StableConfig]].
			 *
			 * @param primaryState the [[PrimaryState]] from which the transition is derived.
			 * @param oldConfig the [[Configuration]] that is being abandoned and on which the [[Leader]] derived state is based. Needed to know what is in each element of the [[indexOfNextRecordToSend_ByParticipantIndex]] and [[highestRecordIndexKnownToBeAppended_ByParticipantIndex]].
			 * @param configChange the [[ConfigChange]] that caused the transition.
			 * @param configChangeIndex the log index where the provided [[ConfigChange]] is stored. */
			private def driveTheRetirements(primaryState: Accessible, oldConfig: Configuration, configChange: StableConfigChange[ParticipantId], configChangeIndex: RecordIndex)(using Context): Unit = {
				assert(commitIndex >= configChangeIndex)

				// Stop the RetirementDriver instances corresponding to the participants that become included.
				retirementDriverByParticipantId.filterInPlace { (retireeId, driver) => configChange.newParticipants.contains(retireeId) }

				// Find out which are the participants that become excluded.
				val newRetiringParticipants = configChange.oldParticipants.diff(configChange.newParticipants)

				// If this leader is excluded, set the threshold until which this leader will continue leading as a ghost.
				if newRetiringParticipants.contains(boundParticipantId) then thisLeader.indexOfConfigChangeThatExcludedThisParticipant = configChangeIndex
				// If this leader continues as a stable leader (not a ghost), authorize the quiescence of the retiring followers.
				else authorizeQuiescenceTo(configChange.oldParticipants - boundParticipantId, configChangeIndex, false)

				// Create and register a retirement driver for each follower that is excluded.
				oldConfig.allOtherParticipants.foreachWithIndex { (participantId, participantIndex) =>
					if highestRecordIndexKnowToBeCommitted_ByParticipantIndex(participantIndex) < configChangeIndex && newRetiringParticipants.contains(participantId) then {
						val highestRecordIndexKnownToBeAppended = thisLeader.highestRecordIndexKnownToBeAppended_ByParticipantIndex(participantIndex)
						val indexOfFirstPotentiallyUnappendedRecord = highestRecordIndexKnownToBeAppended + 1
						val potentiallyUnappendedRecords: GenIndexedSeq[Record] = primaryState.getRecordsBetween(indexOfFirstPotentiallyUnappendedRecord, configChangeIndex + 1)
						val retirementDriver = new RetirementDriver(
							participantId,
							primaryState.getRecordTermAt(highestRecordIndexKnownToBeAppended),
							potentiallyUnappendedRecords,
							indexOfFirstPotentiallyUnappendedRecord,
							configChangeIndex,
							primaryState.getRecordTermAt(configChangeIndex),
							thisLeader.indexOfNextRecordToSend_ByParticipantIndex(participantIndex)
						)
						retirementDriverByParticipantId.put(participantId, retirementDriver)
						retirementDriver.driveLoop(primaryState.currentTerm, 0)
					}
				}
			}

			/** Starts a process that insistently authorizes the quiescence of the specified participants; allowing them to transition from [[Retiring]] to [[Quiesced]] provided they retire due to being excluded by a [[StableConfigChange]] at the `permittedConfigChangeIndex`.
			 * @param participants the participants to authorize the quiescence of.
			 * @param permittedConfigChangeIndex the index of the [[StableConfigChange]] for which the quiescence is authorized. The destination participant will quiesce only if it reaches the [[Retiring]] state with a [[Retiring.excludingConfigIndex]] equal to this value.
			 * @param includeMyself whether to include this participant in the set of participants to authorize the quiescence of. If true, this participant will be authorized after all the others have acknowledged the authorization.
			 */
			private def authorizeQuiescenceTo(participants: Set[ParticipantId], permittedConfigChangeIndex: RecordIndex, includeMyself: Boolean)(using Trace.Context): Unit = {
				Trace.step("authorizeQuiescenceTo") {
					retryPermitQuiescenceWakeUp.foreach(cancelWakeUp(_))

					def loop(remainingParticipants: IArray[ParticipantId], retriesDone: Int = 0): Unit = {

						val calls = for participantId <- remainingParticipants yield participantId.permitQuiescence(permittedConfigChangeIndex)
						for responses <- sequencer.LatchingDuty_sequenceTasksToArray(calls) do {

							val failedParticipants = IArray.unsafeFromArray(responses).collectWithIndex { (response, index) =>
								response match {
									case Failure(e) =>
										val participantId = remainingParticipants(index)
										Trace.debug(s"$boundParticipantId: An attempt to permit $participantId to quiesce at $permittedConfigChangeIndex failed after $retriesDone retries with:", e)
										Maybe(participantId)

									case _ =>
										Maybe.empty
								}
							}

							if failedParticipants.length > 0 && retriesDone < MAX_PERMIT_QUIESCENCE_RETRIES then {
								val token = requestWakeUp(WakeUpReason.QuiescenceAuthorizationRetry, () => loop(failedParticipants, retriesDone + 1))
								retryPermitQuiescenceWakeUp = Maybe(token)
							} else {
								if failedParticipants.length > 0 then Trace.warn(s"$boundParticipantId: The limit of retries ($retriesDone) to permit the participants ${remainingParticipants.mkString("[", ", ", "]")} to quiesce at $permittedConfigChangeIndex has been reached.")
								if includeMyself then currentRole.onQuiescencePermitted(boundParticipantId, permittedConfigChangeIndex)
							}
						}
					}

					loop(IArray.from(participants))
				}
			}

			override def authorizeQuiescenceIfVanished(config: StableConfig)(using Trace.Context): Unit = {
				if config.allParticipants.length == 0 then authorizeQuiescenceTo(config.backingConfigChange.oldParticipants - boundParticipantId, config.changeIndex, true)
			}

			/** Handles configuration-change request for [[Leader]]
			 * Attempts a [[Configuration]] change, starting with the first phase and, if successful, continuing with the second. */
			override def requestConfigChange(primaryState0: Accessible, requestId: ConfigChangeRequestId, desiredParticipants: Set[ParticipantId], ballotWasUpdated: Boolean)(using Context): sequencer.LatchingDuty[ConfigChangeResponse] = {
				Trace.step("Leader.requestConfigChange") {

					def startSecondPhase(tcc: TransitionalConfigChange[ParticipantId], tccIndex: RecordIndex): sequencer.LatchingDuty[ConfigChangeResponse] = {
						for {
							isSccReplicatedToMajority <- startConfigChangeSecondPhase(tcc, tccIndex)
							primaryState1 <- primaryStateFence.causalAnchor()
						} yield {
							val ballot1 = syncLocalStateInfo(primaryState1).ballot
							if isSccReplicatedToMajority then SUCCESSFULLY_CHANGED(ballot1) else REQUEST_TRACKING_LOST_AFTER_SECOND_PHASE_STARTED(ballot1)
						}
					}

					/** This method recurses whenever it fails and the consequent [[updateRole]] does not change the [[Role]] (stays as leader) */
					def replicateTccAndThenStartSecondPhase(primaryState1: PrimaryState, tcc: TransitionalConfigChange[ParticipantId], tccIndex: RecordIndex): sequencer.LatchingDuty[ConfigChangeResponse] = {
						if currentRole ne this then sequencer.LatchingDuty_ready(REQUEST_TRACKING_LOST_AFTER_FIRST_PHASE_STARTED(syncLocalStateInfo(primaryState1).ballot))
						else primaryState1 match {
							case Inaccessible =>
								illegalStateQuiesce()
								sequencer.LatchingDuty_ready(STOPPED(buildIneligibleInfo(leadedTerm).ballot))

							case accessible0: Accessible =>
								if assertionsEnabled then assert(accessible0.currentTerm == leadedTerm)
								Trace.trace(s"Replicating TCC at index $tccIndex")
								for {
									// Replicate to other participants.
									isTccReplicatedToMajority <- attemptToUpdateOtherParticipantsLogs(accessible0)
									primaryState2 <- primaryStateFence.causalAnchor()
									response <- {
										if assertionsEnabled then assert(primaryState2.currentTerm == leadedTerm)
										if currentRole ne this then {
											val ballot1 = syncLocalStateInfo(primaryState2).ballot
											sequencer.LatchingDuty_ready(if isTccReplicatedToMajority then REQUEST_TRACKING_LOST_AFTER_FIRST_PHASE_COMMITED(ballot1) else REQUEST_TRACKING_LOST_AFTER_FIRST_PHASE_STARTED(ballot1))
										}
										else if isTccReplicatedToMajority then startSecondPhase(tcc, tccIndex)
										else {
											startNewBallot()
											Trace.trace(s"Updating role (in a new ballot=$currentBallot) due to insufficient quorum when replicating the TCC at $tccIndex.")
											for {
												_ <- updateRole(primaryState2)
												primaryState3 <- primaryStateFence.causalAnchor()
												response <- replicateTccAndThenStartSecondPhase(primaryState3, tcc, tccIndex)
											} yield response
										}
									}
								} yield response
						}
					}

					if assertionsEnabled then assert(primaryState0.currentTerm == leadedTerm)

					// First, synchronize the [[StateInfo]] of the bound participant and memorize the current ballot round.
					val myStateInfo0 = syncLocalStateInfo(primaryState0)
					Trace.trace(s"StateInfo=$myStateInfo0")
					val ballot0 = myStateInfo0.ballot
					deriveConfigurationFrom(primaryState0) match {
						case stable0: StableConfig =>
							if desiredParticipants == stable0.desiredParticipants then sequencer.LatchingDuty_ready(ALREADY_CHANGED(ballot0))
							// Do not start a configuration transition if excluded from both, the current, and the new configuration.
							else if !stable0.desiredParticipants.contains(boundParticipantId) && !desiredParticipants.contains(boundParticipantId) then {
								// Also, become retiring immediately if all followers have committed the excluding config change. The intention of this is to minimize the time that a participant is kept leading after it was excluded.
								if isGhostAndAllFollowersCommittedTheExcludingConfigChange then {
									if assertionsEnabled then assert(indexOfConfigChangeThatExcludedThisParticipant == stable0.changeIndex)
									authorizeQuiescenceIfVanished(stable0)
									become(Retiring(primaryState0.currentTerm, stable0.term, stable0.changeIndex, stable0.allParticipants)).requestConfigChange(requestId, desiredParticipants, Maybe.empty)
								}
								// If leading as a ghost and some follower hasn't commited the excluding config change, make them commit it.
								else {
									for {
										isCurrentConfigReplicationCommited <- attemptToUpdateOtherParticipantsLogs(primaryState0)
										primaryState1 <- primaryStateFence.causalAnchor()
										response <- {
											if isCurrentConfigReplicationCommited then currentRole.requestConfigChange(requestId, desiredParticipants, Maybe.empty)
											else sequencer.LatchingDuty_ready(WAIT_GHOST_LEADER_IS_DEPOTED(syncLocalStateInfo(primaryState1).ballot))
										}
									} yield response
								}
							} else {
								// start the first phase of the configuration change
								val tcc = new TransitionalConfigChange[ParticipantId](primaryState0.currentTerm, requestId, stable0.desiredParticipants, desiredParticipants)
								Trace.trace(s"About to append TCC $tcc")
								for {
									// Update primary state
									primaryState2 <- primaryStateFence.advanceIf { primaryState1 =>
										if currentRole ne this then Maybe.empty
										else primaryState1 match {
											case Inaccessible =>
												Maybe.empty
											case accessible1: Accessible =>
												if assertionsEnabled then assert(primaryState1.currentTerm == leadedTerm)
												Maybe(accessible1.withSingleRecordAppended(tcc.term, tcc))
										}
									}

									// replicate the TransitionalConfigChange and then start the second phase.
									response <- replicateTccAndThenStartSecondPhase(primaryState2, tcc, primaryState2.firstEmptyRecordIndex - 1)
								} yield response
							}

						case transitional0: TransitionalConfig =>
							val response = if desiredParticipants == transitional0.desiredParticipants then ALREADY_IN_PROGRESS(ballot0) else WAIT_PREVIOUS_CHANGE_TO_COMPLETE(ballot0)
							sequencer.LatchingDuty_ready(response)

						case NoConfig =>
							sequencer.LatchingDuty_ready(STOPPED(ballot0))
					}
				}
			}

			/** Starts the second phase of a configuration change.
			 * Appends a [[StableConfigChange]] instance in the local log, stores it, and then attempts to replicate it to the participants in both, old and new configurations as if its configuration was the corresponding [[TransitionalConfigChange]].
			 * @param correspondingTransitionalConfigChange the [[TransitionalConfigChange]] that initiated the first phase of the configuration change.
			 * @return  a [[sequencer.LatchingDuty]] that yields true/false if the [[StableConfigChange]] [[Record]] was/wasn't replicated to a majority. */
			private def startConfigChangeSecondPhase(correspondingTransitionalConfigChange: TransitionalConfigChange[ParticipantId], tccIndex: RecordIndex)(using Context): sequencer.LatchingDuty[Boolean] = {
				Trace.step("startConfigChangeSecondPhase") {
					for {
						primaryState1 <- primaryStateFence.advanceIf { primaryState0 =>
							if currentRole ne this then Maybe.empty
							else primaryState0 match {
								case accessible0: Accessible =>
									if assertionsEnabled then assert(accessible0.currentTerm == leadedTerm)
									val scc = new StableConfigChange[ParticipantId](accessible0.currentTerm, correspondingTransitionalConfigChange.requestId, correspondingTransitionalConfigChange.term, correspondingTransitionalConfigChange.oldParticipants, correspondingTransitionalConfigChange.newParticipants)
									Maybe(accessible0.withSingleRecordAppended(accessible0.currentTerm, scc))
								case Inaccessible =>
									Maybe.empty
							}
						}

						isSecondPhaseChangeReplicatedToMajority <- {
							val sccIndex = primaryState1.firstEmptyRecordIndex - 1
							Trace.trace(s"Starting replication of SCC at $sccIndex. The corresponding TCC is $correspondingTransitionalConfigChange at $tccIndex")
							replicateUntilSuccessOrLeaderRoleIsAbandoned(primaryState1, sccIndex)
						}
					} yield isSecondPhaseChangeReplicatedToMajority
				}
			}

			/** Replicates all the records in this [[ConsensusParticipant]], retrying until either:
			 *		- the [[Record]]s up to the provided index are replicated to a majority ([[commitIndex]] equals or greater than the provided index).
			 *		- the [[currentRole]] stops being this [[Leader]] instance.
			 * A no-op [[LeaderTransition]] record is appended if [[Record]]s of a previous [[Term]] are blocking the [[commitIndex]] advancement due to the Raft safety rule (§5.4.2): "A leader cannot determine commitment using entries from previous terms". This contraint is implemented in [[TransitionalConfig.indexOfTheCommittedRecordWithHighestIndex]].
			 * This method recurses whenever it fails and the consequent [[updateRole]] does not change the [[Role]] (stays as leader) */
			private def replicateUntilSuccessOrLeaderRoleIsAbandoned(primaryState0: PrimaryState, targetIndex: RecordIndex)(using Context): sequencer.LatchingDuty[Boolean] = {
				Trace.step("replicateUntilSuccessOrLeaderRoleIsAbandoned") {
					if currentRole ne this then sequencer.LatchingDuty_false
					else primaryState0 match {
						case Inaccessible =>
							sequencer.LatchingDuty_false
						case accessible0: Accessible =>
							if assertionsEnabled then assert(accessible0.currentTerm == leadedTerm)
							for {
								isReplicationSuccessful <- {
									if commitIndex < targetIndex then attemptToUpdateOtherParticipantsLogs(accessible0)
									else sequencer.LatchingDuty_true
								}
								result <- {
									if isReplicationSuccessful && commitIndex >= targetIndex then sequencer.LatchingDuty_true
									else if currentRole ne this then sequencer.LatchingDuty_false
									else if isReplicationSuccessful then {
										Trace.trace(s"Appending a no-op record to be able to commit records of previous [[Term]] transitively.")
										for {
											primaryState2 <- primaryStateFence.advanceIf {
												case Inaccessible =>
													Maybe.empty
												case accessible1: Accessible =>
													Maybe(accessible1.withSingleRecordAppended(accessible0.currentTerm, LeaderTransition(accessible1.currentTerm)))
											}
											result <- replicateUntilSuccessOrLeaderRoleIsAbandoned(primaryState2, targetIndex)
										} yield result
									} else {
										Trace.trace(s"Updating role (in a new ballot) due to insufficient quorum when replicating records up-to-index $targetIndex.")
										startNewBallot()
										for {
											_ <- updateRole()
											result <- {
												if currentRole ne this then sequencer.LatchingDuty_false
												else for {
													primaryState1 <- primaryStateFence.causalAnchor()
													recursionResult <- replicateUntilSuccessOrLeaderRoleIsAbandoned(primaryState1, targetIndex)
												} yield recursionResult
											}
										} yield result
									}
								}
							} yield result
					}
				}
			}

			override def onCommandFromClient(clientCommand: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.LatchingDuty[ResponseToClient] = {
				Trace.init(() => s"$boundParticipantId: Leader.onCommandFromClient") {
					checkWithin()

					val clientId = clientIdOf(clientCommand)
					type PrimaryStateUpdaterResult = (recordIndex: RecordIndex, indexOfLastAppendedCommandFromClient: RecordIndex, shouldRetire: Boolean)
					var primaryStateUpdaterResultCompanion: PrimaryStateUpdaterResult | Null = null // secondary return value of the causal fence exclusive section
					for {
						// First, append the command to the log if it wasn't already
						primaryState1 <- primaryStateFence.advanceIf { primaryState0 =>
							if currentRole ne this then Maybe.empty
							else primaryState0 match {
								case Inaccessible =>
									Maybe.empty

								case accessible0: Accessible =>
									val currentTerm = accessible0.currentTerm
									if assertionsEnabled then assert(currentTerm == leadedTerm)

									// Do not append the command if the bound participant is excluded and ready to retire. The intention of this is to minimize the time that a participant is kept leading after it was excluded.
									// Note that for the result of `isGhostAndAllFollowersCommittedTheExcludingConfigChange` be fiable here, it is required that the [[deriveConfigurationFrom]] be called whenever a [[Record]] is appended to the local log. // TODO check the mentioned requirement is satisfied. Never delete this TODO. This is a candidate target for an AI based agentic source checker
									if isGhostAndAllFollowersCommittedTheExcludingConfigChange then { // TODO using derived state here is risky and hard to maintain. Do this another way.
										if assertionsEnabled then assert(accessible0.indexOfTopConfigChange == indexOfConfigChangeThatExcludedThisParticipant || accessible0.getRecordAt(accessible0.indexOfTopConfigChange).asInstanceOf[ConfigChange[ParticipantId]].newParticipants.contains(boundParticipantId)) // because Leader.requestConfigChange never starts a configuration transition if the bound participant is not present in neither the current nor the desired participants set.
										primaryStateUpdaterResultCompanion = (0L, 0L, true)
										Maybe.empty
									} else {
										val indexOfLastAppendedCommandFromClient = accessible0.indexOfLastAppendedCommandFrom(clientId)

										// If this is the first command received from the client, proceed normally (append, replicate, apply)
										if indexOfLastAppendedCommandFromClient == 0 then {
											primaryStateUpdaterResultCompanion = (primaryState0.firstEmptyRecordIndex, indexOfLastAppendedCommandFromClient, false)
											Maybe(accessible0.withSingleRecordAppended(currentTerm, CommandRecord(currentTerm, clientCommand)))
										}
										// else, check if the command was received before:
										else {
											// @formatter:off
											accessible0.getRecordAt(indexOfLastAppendedCommandFromClient) match {
												case CommandRecord[ClientCommand @unchecked](lastClientCommandTerm, lastClientCommand) =>
													val comparison = clientCommandOrdering.compare(clientCommand, lastClientCommand)
													// if the command is newer than the last received from the same client, append it to the log memorizing the index.
													if comparison > 0 then {
														primaryStateUpdaterResultCompanion = (accessible0.firstEmptyRecordIndex, indexOfLastAppendedCommandFromClient, false)
														Maybe.some(accessible0.withSingleRecordAppended(currentTerm, CommandRecord(currentTerm, clientCommand)))
													}
													// if the command is the same as the last received, memorize the index of the last received.
													else if comparison == 0 then {
														primaryStateUpdaterResultCompanion = (indexOfLastAppendedCommandFromClient, indexOfLastAppendedCommandFromClient, false)
														Maybe.empty
													}
													// if the command is older than the last received, obtain its index and memorize it.
													else {
														primaryStateUpdaterResultCompanion = (accessible0.indexOf(clientCommand), indexOfLastAppendedCommandFromClient, false)
														Maybe.empty
													}
												case _ => // State inconsistency between the client and the log exposed by the bound ClusterParticipant: the record at indexOfLastAppendedCommandFrom(clientId) isn't a CommandRecord.
													Maybe.empty
											}
											// @formatter:on
										}
									}
							}
						}
						// Second, replicate it if not already, and then, if replication was successful, apply the command to the state machine assuming it is idempotent.
						response <- {
							if currentRole ne this then currentRole.onCommandFromClient(clientCommand, INTERNAL_VACATE_HANDOFF)
							else primaryState1 match {
								case Inaccessible =>
									sequencer.LatchingDuty_ready(Unable(LEADERSHIP_VACATED, cluster.getOtherProbableParticipant))

								case accessible1: Accessible =>
									if assertionsEnabled then assert(accessible1.currentTerm == leadedTerm)

									primaryStateUpdaterResultCompanion.asMatchable match {
										case null => // Reaches here when the last known log-entry index for the client does not point to a CommandRecord.
											sequencer.LatchingDuty_ready(InconsistentState(s"Inconsistency between the client $clientId and the log exposed by the bound `ClusterParticipant`: the log record at `ClusterParticipant.indexOfLastAppendedCommandFrom(clientId)` isn't a `CommandRecord`."))

										case (recordIndex, indexOfLastAppendedCommandFromClient, shouldRetire) =>
											if shouldRetire then {
												val config1 = deriveConfigurationFrom(accessible1)
												if assertionsEnabled then {
													assert(config1.isInstanceOf[StableConfig]) // because exclusion is checked every record and transitional configurations are never more restrictive than the contiguos stable ones.
													assert(indexOfConfigChangeThatExcludedThisParticipant == config1.changeIndex) // because `shouldRetire == true` implies that `indexOfConfigChangeThatExcludedThisParticipant > 0` and the primary state was not mutated (=> `primaryState1 eq primaryState0` and `config1 eq config0`). And given it is assumed that [[deriveConfigurationFrom]] is called whenever a [[Record]] is appended to the local log, then `indexOfConfigChangeThatExcludedThisParticipant == config0.changeIndex`.
												}
												authorizeQuiescenceIfVanished(config1.asInstanceOf[StableConfig])
												become(Retiring(accessible1.currentTerm, config1.term, config1.changeIndex, config1.allParticipants)).onCommandFromClient(clientCommand, INTERNAL_VACATE_HANDOFF)
											}
											else if recordIndex == 0L then sequencer.LatchingDuty_ready(Stale(clientCommand, indexOfLastAppendedCommandFromClient))
											else if recordIndex == -1L then sequencer.LatchingDuty_ready(TooOld(clientCommand))
											else {
												assert(recordIndex > 0)
												for {
													isCommitSuccessful <- {
														// if the command is not already committed, attempt the replication to a majority.
														if recordIndex > commitIndex then attemptToUpdateOtherParticipantsLogs(accessible1)
														else sequencer.LatchingDuty_true
													}
													response <- {
														if currentRole ne this then currentRole.onCommandFromClient(clientCommand, INTERNAL_VACATE_HANDOFF)
														else if isCommitSuccessful then {
															// if the command is committed, apply it to the state machine to get the result assuming it is idempotent.
															for smr <- machine.applyClientCommand(recordIndex, clientCommand) yield {
																if recordIndex > highestAppliedCommandIndex then highestAppliedCommandIndex = recordIndex
																Processed(recordIndex, smr)
															}
														} else {
															// if not able to replicate then start a new ballot, update the role, and start again.
															startNewBallot() // TODO analyze if this ballot bump is necessary.
															Trace.trace(s"Updating role (in a new ballot=$currentBallot) due to insufficient quorum when replicating a command record.")
															for {
																_ <- updateRole()
																response <- currentRole.onCommandFromClient(clientCommand, INTERNAL_VACATE_HANDOFF)
															} yield response
														}
													}
												} yield response
											}
									}
							}
						}
					} yield response
				}
			}

			/**
			 * Attempts to append the [[Record]]s that this participant has, to the logs of the participants that lack them.
			 * Detailed behavior:
			 *		- If [[Record]]s weren't appended to another participant, attempt to append them.
			 *			- If successful: update the corresponding entry of [[indexOfNextRecordToSend_ByParticipantIndex]] and [[highestRecordIndexKnownToBeAppended_ByParticipantIndex]].
			 *			- If AppendEntries fails because of log inconsistency: decrement the corresponding entry of [[indexOfNextRecordToSend_ByParticipantIndex]] and [[highestRecordIndexKnownToBeAppended_ByParticipantIndex]], and retry.
			 *		- If there exists an N such that N > [[commitIndex]], a majority of the [[highestRecordIndexKnownToBeAppended_ByParticipantIndex]] entries is ≥ N, and log[N].term == [[currentTerm]]: set commitIndex = N
			 *		- If there are unreachable participants (a minority whose corresponding entry in [[highestRecordIndexKnownToBeAppended_ByParticipantIndex]] trails the leader's [[commitIndex]]), initiate targeted retries to catch them up.
			 * @return a [[sequencer.LatchingDuty]] that yields true if, and only if, all the following are true:
			 *		- the [[currentRole]] is not changed during this process;
			 *		- none of the responses has a higher [[Term]];
			 *		- for all the participants sets of the current [[Configuration]], all the records in this participant's log are successfully appended to at least:
			 *			- half of the other participants of the set, if this participant belongs to the set;
			 *			- a majority of the other participants of the set, if this participant does not belong to the set.
			 *		TODO coalesce calls with same argument. */
			private def attemptToUpdateOtherParticipantsLogs(primaryState0: Accessible)(using Context): sequencer.LatchingDuty[Boolean] = {
				assert(primaryState0 eq primaryStateFence.committedState, s"$primaryState0 eq ${primaryStateFence.committedState}")
				// Set the serial number of this method execution.
				serialOfLastReplicationAttempt += 1
				val serialOfReplicationAttempt = serialOfLastReplicationAttempt
				Trace.step(() => s"attemptToUpdateOtherParticipantsLogs#$serialOfReplicationAttempt") {
					// Memorize the index after the top record to include in the appends produced by this replication process.
					val indexAfterTopRecordToSend = primaryState0.firstEmptyRecordIndex
					Trace.trace(s"starting replication until record index $indexAfterTopRecordToSend.")

					// Cancel the previous wake-up for retrying appends to unreachable followers, if any.
					unreachableFollowersRetryWakeUp.foreach(cancelWakeUp(_))
					unreachableFollowersRetryWakeUp = Maybe.empty

					// Then, start regular replication to all followers.
					val config0 = deriveConfigurationFrom(primaryState0)
					// For every other participants, generate a task to replicate records this participant has and believes the others lack.
					val appendRequests_byParticipantIndex0 = config0.allOtherParticipants.mapWithIndex { (otherParticipantId, otherParticipantIndex0) =>
						val nextRecordToSend = indexOfNextRecordToSend_ByParticipantIndex(otherParticipantIndex0)
						appendsRecordsToParticipant(primaryState0, otherParticipantId, otherParticipantIndex0, nextRecordToSend, indexAfterTopRecordToSend)
					}
					val commitIndexAtAppendRequest = commitIndex
					for {
						// Execute the tasks in parallel.
						appendResponses0 <- sequenceAppendRequests(appendRequests_byParticipantIndex0)
						isReplicatedToMajority <- {

							if currentRole ne this then sequencer.LatchingDuty_false
							else if handoffAndBumpTermIfLessThan(highestTermIn(appendResponses0)) ne this then sequencer.LatchingDuty_false
							else for {
								primaryState1 <- primaryStateFence.causalAnchor()
								isReplicatedToMajority <- {
									if currentRole ne this then sequencer.LatchingDuty_false
									else primaryState1 match {
										case Inaccessible =>
											sequencer.LatchingDuty_false
										case accessible1: Accessible =>
											if assertionsEnabled then assert(accessible1.currentTerm == leadedTerm) // because the Leader Role should never bump the term before leaving transitioning to another Role.
											val config1 = deriveConfigurationFrom(accessible1)
											// Rearrange the responses according to the current configuration. Discard any responses from excluded participants.
											// TODO: Find a way for the [[RetirementDriver]]s created for those excluded participants (by `deriveConfigurationFrom`) to make use of the discarded responses. The only approach that comes to mind is passing those responses into `deriveConfigurationFrom`.
											val appendResponses1 = rearrangeAppendResponses(appendResponses0, config0, config1)
											val appendOutcomes1 = appendResponses1.mapWithIndex { (appendResponse, otherParticipantIndex) =>
												val otherParticipantId = config1.allOtherParticipants(otherParticipantIndex)
												// Handle the responses. Note that when successful, it updates the corresponding entry of the `indexOfNextRecordToSend_ByParticipantIndex` and `highestRecordIndexKnownToBeAppended_ByParticipantIndex` arrays.
												handleAppendResponse(accessible1, config1, otherParticipantId, otherParticipantIndex, appendResponse, primaryState0.currentTerm, indexAfterTopRecordToSend, commitIndexAtAppendRequest)
											}
											Trace.trace(s"The append responses until $indexAfterTopRecordToSend have been handled, excludingConfigIndex=$indexOfConfigChangeThatExcludedThisParticipant, aboutOthers=${(for i <- config1.allOtherParticipants.indices yield s"${config1.allOtherParticipants(i)}: outcome=${appendOutcomes1(i)}, nextToSend=${indexOfNextRecordToSend_ByParticipantIndex(i)}, knownAppended=${highestRecordIndexKnownToBeAppended_ByParticipantIndex(i)}, knowCommitted=${highestRecordIndexKnowToBeCommitted_ByParticipantIndex(i)}").mkString("[", "; ", "]")}") // TODO delete
											for maybeLastAppendAttemptInfo2 <- retryLaggingLearners(accessible1, config1, appendOutcomes1, indexAfterTopRecordToSend, false, false, serialOfReplicationAttempt)
												yield {
													// If the behavior changed while waiting the result or the number of successful appends isn't enough to achieve quorum, return false.
													if (currentRole ne this) || maybeLastAppendAttemptInfo2.isEmpty then false
													// If the number of successful appends is enough to achieve quorum, then: update the `commitIndex`, retry laggards, and schedule retries for the unreachable participants, and yield true.
													else {
														// At this point, the result of the method is already determined (true). The following lines update derived state and start uncoupled retry processes.

														val (appendOutcomes2a, accessible2, config2a) = maybeLastAppendAttemptInfo2.get
														// Update the commitIndex if a majority of the followers have replicated the uncommitted records.
														// If there exists an N such that N > commitIndex, the highest log-entry index known to be replicated is > N in a majority of the servers, and getRecordAt[N].term == currentTerm: set commitIndex = N
														val previousCommitIndex = commitIndex
														commitIndex = config2a.indexOfTheCommittedRecordWithHighestIndex(accessible2, previousCommitIndex, IArray.unsafeFromArray(highestRecordIndexKnownToBeAppended_ByParticipantIndex), appendOutcomes2a)
														val config2b =
															if commitIndex == previousCommitIndex then config2a
															else {
																notifyListeners(_.onCommitIndexChanged(previousCommitIndex, commitIndex))
																// The active configuration depends on the commitIndex, so, update it
																deriveConfigurationFrom(accessible2)
															}

														// Rearrange the append outcomes if the active configuration changed due to a bump of the commitIndex.
														val appendOutcomes2b =
															if config2b eq config2a then appendOutcomes2a
															else {
																config2a.allOtherParticipants.mapWithIndex { (participantId, participantIndex) =>
																	val indexFrom = config2a.participantIndexOf(participantId)
																	if indexFrom < 0 then AO_MISSING_BECAUSE_PARTICIPANT_WAS_NOT_PART_OF_THE_CONFIGURATION // TODO analyse: Is it correct to include the participants that weren't in the active configuration in the previous attempt into the next attempt of this replication?
																	else appendOutcomes2a(indexFrom)
																}
															}

														// In a decoupled manner, retry the appends to lagging participants, and then: retire if excluded, or schedule retries for the unreachable participants.
														for {
															// For those minority of participants that responded asking for earlier records, attempt the appends again including earlier records. And for those that weren't in the active configuration during the previous attempt, attempt the first append.
															maybeLastAppendAttemptInfo3 <- retryLaggingLearners(accessible2, config2b, appendOutcomes2b, indexAfterTopRecordToSend, true, true, serialOfReplicationAttempt)
														} yield {
															// If no newer call to attemptToUpdateOtherParticipantsLogs was done, then:
															maybeLastAppendAttemptInfo3.foreach { lastAppendAttemptInfo3 =>
																val config3 = lastAppendAttemptInfo3.updatedConfig
																Trace.trace(s"Final step: excludingConfigIndex=$indexOfConfigChangeThatExcludedThisParticipant, aboutOthers=${(for i <- config3.allOtherParticipants.indices yield s"${config3.allOtherParticipants(i)}: outcome=${lastAppendAttemptInfo3.lastAttemptOutcomes(i)}, nextToSend=${indexOfNextRecordToSend_ByParticipantIndex(i)}, knownAppended=${highestRecordIndexKnownToBeAppended_ByParticipantIndex(i)}, knowCommitted=${highestRecordIndexKnowToBeCommitted_ByParticipantIndex(i)}").mkString("[", "; ", "]")}") // TODO delete
																// If this leading participant is not included in the active configuration and all the followers in the new configuration have committed the StableConfigChange that excludes this participant, retire this participant.
																// Note that these lines are skipped if another replication started (call to this method). Therefore, this check and the transition to retiring should be done before calling this method (attemptToUpdateOtherParticipantsLogs) in order to minimize the time a participant is leading while excluded.
																if isGhostAndAllFollowersCommittedTheExcludingConfigChange then {
																	assert(config3.isInstanceOf[StableConfig]) // because exclusion is checked every record and transitional configurations are never more restrictive than the contiguos stable ones.
																	authorizeQuiescenceIfVanished(config3.asInstanceOf[StableConfig])
																	become(Retiring(leadedTerm, lastAppendAttemptInfo3.currentPrimaryState.getRecordTermAt(indexOfConfigChangeThatExcludedThisParticipant), indexOfConfigChangeThatExcludedThisParticipant, config3.allParticipants))
																} // This point is reached if this participant was able to make the followers commit the StableConfigChange that excludes this leader before receiving an "appendRecords" call from the new leader.
																/// Else (if not retiring), for those minority of participants whose highestRecordIndexKnowToBeReplicated is less than the commitIndex (because append failed with IS_UNREACHABLE), retry the append records RPC. This retry is indefinite until the outer method (attemptToUpdateOtherParticipantsLogs) is called again (as the effect of an external stimulus).
																else scheduleUnreachableParticipantsRetry(config3.allOtherParticipants, lastAppendAttemptInfo3.lastAttemptOutcomes, indexAfterTopRecordToSend, serialOfReplicationAttempt)
															}
														}

														true
													}
												}
									}
								}
							} yield isReplicatedToMajority
						}
					} yield isReplicatedToMajority
				}
			}

			/** Creates a [[sequencer.Task]] that appends the specified range of [[Record]]s from this participant log to the specified destination participant.
			 * CAUTION: requires that the derived state variables had been updated by applying the [[deriveConfigurationFrom]] method to the provided [[PrimaryState]].
			 * @param primaryState the current [[PrimaryState]]
			 * @param destinationParticipantId the [[ParticipantId]] of the [[ConsensusParticipant]] to append the records to.
			 * @param destinationParticipantIndex the index of the [[ParticipantId]] in the [[Configuration.allOtherParticipants]] array of the [[Configuration]] derived from the provided [[PrimaryState]].
			 * @param fromIndex the [[RecordIndex]] of the first [[Record]] to include in the [[ClusterParticipant.appendRecords]] call.
			 * @param untilIndex the [[RecordIndex]] after the last [[Record]] to include in the [[ClusterParticipant.appendRecords]] call. */
			private def appendsRecordsToParticipant(primaryState: Accessible, destinationParticipantId: ParticipantId, destinationParticipantIndex: Int, fromIndex: RecordIndex, untilIndex: RecordIndex)(using Trace.Context): AppendRequest = {
				Trace.step("appendsRecordsToParticipant") {
					if assertionsEnabled then {
						assert(isInSequence)
						assert(currentConfig eq deriveConfigurationFrom(primaryState))
					}

					// if the appending would be empty and with the same `leaderCommit` as a previous successful append, skip it and fake a successful response.
					if fromIndex >= untilIndex && commitIndex == highestRecordIndexKnowToBeCommitted_ByParticipantIndex(destinationParticipantIndex) && untilIndex <= 1 + highestRecordIndexKnownToBeAppended_ByParticipantIndex(destinationParticipantIndex)
					then sequencer.Task_successful(AppendResult(primaryState.currentTerm, 0, ISOLATED))
					else {
						val previousRecordIndex = fromIndex - 1
						val recordsToSend = primaryState.getRecordsBetween(fromIndex, untilIndex)
						val previousRecordTerm = primaryState.getRecordTermAt(previousRecordIndex)
						destinationParticipantId.appendRecords(primaryState.currentTerm, previousRecordIndex, previousRecordTerm, recordsToSend, commitIndex, primaryState.getRecordTermAt(commitIndex))
					}
				}
			}

			private def highestTermIn(appendResponses: IArray[AppendResponse]): Term = {
				var latestTermSeen = PRE_INIT
				var index = appendResponses.length
				while index > 0 do {
					index -= 1
					appendResponses(index) match {
						case Success(appendResult) => if appendResult.term > latestTermSeen then latestTermSeen = appendResult.term
						case _ => // do nothing
					}
				}
				latestTermSeen
			}

			/** Rearrange the elements of the provided array of [[AppendResponse]] in the same way as the elements of the [[Configuration.allOtherParticipants]] array changes when transitioning from the first provided [[Configuration]] to the second. */
			private def rearrangeAppendResponses(appendResponses: IArray[AppendResponse], from: Configuration, to: Configuration): IArray[AppendResponse | Null] = {
				if to eq from then appendResponses else {
					to.allOtherParticipants.mapWithIndex { (participantId, participantIndex) =>
						val indexFrom = from.participantIndexOf(participantId)
						if indexFrom < 0 then null
						else appendResponses(indexFrom)
					}
				}
			}

			/** Retry the [[ClusterParticipant.appendRecords]] call for each participant that needs earlier records or was not present in the [[Configuration]] when the previous attempt was done, until either:
			 *		- if `untilNoneLags` is `true`, strictly until there is no lagging follower.
			 *		- else,	relaxedly until there is no lagging follower or a majority of the appends is successful.
			 *
			 * This method is called solely from [[attemptToUpdateOtherParticipantsLogs]] and twice: before and after the top sent record is committed.
			 * @param accessible1 the current, accessible, and causally anchored [[PrimaryState]] of this [[ConsensusParticipant]].
			 * @param config1 the current updated [[Configuration]] of this [[ConsensusParticipant]].
			 * @param previousAttemptAppendOutcomes the [[AppendOutcome]]s of the results of the previous [[ClusterParticipant.appendRecords]] attempt, stored as a parallel array with index correspondence to `config1.allOtherParticipants`.
			 * @param indexAfterTopRecordSent the [[RecordIndex]] after the top [[Record]] sent in the calls to [[ClusterParticipant.appendRecords]].
			 * @param untilNoneLags instructs if the retries must continue until no learner is lagging (true), or until a majority of the appends is successful (false).
			 * @param serialOfReplicationAttempt the serial number of the call to [[attemptToUpdateOtherParticipantsLogs]] that initiated this method execution.
			 * @return a [[sequencer.LatchingDuty]] that yields either:
			 *		- [[Maybe.empty]] if either:
			 *			- `abortIfAnotherReplicationStarts` is true and another execution of [[attemptToUpdateOtherParticipantsLogs]] has been started after the one that called this method.
			 *			- no learner is lagging but the successful append outcomes is not enough to achieve quorum.
			 *		- otherwise [[Maybe.some]] containing:
			 *			- the causally anchored [[PrimaryState]],
			 *			- the active [[Configuration]] derived from it,
			 *			- and an array of [[AppendOutcome]] values representing the responses to the [[ClusterParticipant.appendRecords]] calls, stored as a parallel array with index correspondence to the [[Configuration.allOtherParticipants]] array of the accompanying [[Configuration]].
			 *	TODO when `abortIfAnotherReplicationStarts==false`, couple with the appends requests done by the newer replication to avoid repeating requests.
			 * */
			private def retryLaggingLearners(
				accessible1: Accessible,
				config1: Configuration,
				previousAttemptAppendOutcomes: IArray[AppendOutcome],
				indexAfterTopRecordSent: RecordIndex, // TODO  is this parameter necessary? Why not just get it from `PrimaryState.firstEmptyRecordIndex`? A consequence is that retries would include the records of this participant log that are appended after this method was called and before the retry is done.
				untilNoneLags: Boolean,
				abortIfAnotherReplicationStarts: Boolean,
				serialOfReplicationAttempt: Int
			)(using Context): sequencer.LatchingDuty[Maybe[(lastAttemptOutcomes: IArray[AppendOutcome], currentPrimaryState: Accessible, updatedConfig: Configuration)]] = {
				Trace.step("retryLaggingLearners") {
					assert((accessible1 eq primaryStateFence.committedState) && (config1 eq currentConfig), s"$accessible1 eq ${primaryStateFence.committedState} && $config1 eq $currentConfig")
					// Trace.trace(s"retryLaggingLearners($accessible1, $config1, ${previousAttemptAppendOutcomes.mkString("[", ", ", "]")}, $indexAfterTopRecordSent, $untilNoneLags, $serialOfReplicationAttempt)") // TODO delete line

					// If [[attemptToUpdateOtherParticipantsLogs]] was called again after the call that initiated this `retryLaggingLearners` execution, then there is no need to continue this execution because the later call to `attemptToUpdateOtherParticipantsLogs` will start a new one if necessary.
					if abortIfAnotherReplicationStarts && serialOfReplicationAttempt != serialOfLastReplicationAttempt then emptyLatchedDuty
					else {

						val noParticipantIsLagging = previousAttemptAppendOutcomes.forallWithIndex((previousOutcome, _) => (previousOutcome & AO_IS_LAGGING_MASK) == 0)
						if noParticipantIsLagging then {
							if untilNoneLags || config1.achievesQuorumWhen(previousAttemptAppendOutcomes) then sequencer.LatchingDuty_ready(Maybe((previousAttemptAppendOutcomes, accessible1, config1)))
							else emptyLatchedDuty
						}
						else if !untilNoneLags && config1.achievesQuorumWhen(previousAttemptAppendOutcomes) then sequencer.LatchingDuty_ready(Maybe((previousAttemptAppendOutcomes, accessible1, config1)))
						else {

							val laggingParticipants: mutable.ArrayBuffer[ParticipantId] = new mutable.ArrayBuffer(config1.allOtherParticipants.length)
							val newAppendRequests: mutable.ArrayBuffer[AppendRequest] = new mutable.ArrayBuffer(config1.allOtherParticipants.length)
							config1.allOtherParticipants.foreachWithIndex { (participantId, participantIndex) =>
								if (previousAttemptAppendOutcomes(participantIndex) & AO_IS_LAGGING_MASK) != 0 then {
									laggingParticipants.addOne(participantId)
									val indexOfNextRecordToSend = indexOfNextRecordToSend_ByParticipantIndex(participantIndex)
									newAppendRequests.addOne(appendsRecordsToParticipant(accessible1, participantId, participantIndex, indexOfNextRecordToSend, indexAfterTopRecordSent))
								}
							}
							// Trace.trace(s"retrying the lagging learners $laggingParticipants, newAppendRequests=$newAppendRequests") // TODO delete line
							val commitIndexAtAppendRequest = commitIndex
							for {
								newAppendResponses <- sequenceAppendRequests(newAppendRequests)
								newAppendAttemptInfo <- {
									if currentRole ne this then emptyLatchedDuty
									else if handoffAndBumpTermIfLessThan(highestTermIn(newAppendResponses)) ne this then emptyLatchedDuty
									else for {
										primaryState2 <- primaryStateFence.causalAnchor()
										newAppendAttemptInfo <- {
											// Trace.trace(s"newAppendResponses=${newAppendResponses.mkString("[", ", ", "]")}, primaryState2=$primaryState2") // TODO delete line
											if currentRole ne this then emptyLatchedDuty
											else primaryState2 match {
												case Inaccessible => emptyLatchedDuty
												case accessible2: Accessible =>
													if assertionsEnabled then assert(accessible2.currentTerm == leadedTerm) // because the Leader Role should never bump the term before transitioning to another Role.

													val config2 = deriveConfigurationFrom(accessible2)
													// Handle the new appends responses and merge the old and new summaries. Note that the appends are handled even if a later replication attempt was started.
													val newOutcomes: IArray[AppendOutcome] = config2.allOtherParticipants.mapWithIndex { (participantId, participantIndex2) =>
														val participantIndex1 = if config2 eq config1 then participantIndex2 else config1.participantIndexOf(participantId)
														if participantIndex1 < 0 then AO_MISSING_BECAUSE_PARTICIPANT_WAS_NOT_PART_OF_THE_CONFIGURATION
														else {
															val previousOutcome = previousAttemptAppendOutcomes(participantIndex1)
															if (previousOutcome & AO_IS_LAGGING_MASK) == 0 then previousOutcome
															else {
																val newRequestIndex = laggingParticipants.indexOf(participantId)
																assert(newRequestIndex >= 0)
																handleAppendResponse(accessible2, config2, participantId, participantIndex2, newAppendResponses(newRequestIndex), accessible1.currentTerm, indexAfterTopRecordSent, commitIndexAtAppendRequest)
															}
														}
													}
													Trace.trace(s"The append responses to the laggard-retry of replication #$serialOfReplicationAttempt until $indexAfterTopRecordSent have been handled, excludingConfigIndex=$indexOfConfigChangeThatExcludedThisParticipant, aboutOthers=${(for i <- config2.allOtherParticipants.indices yield s"${config2.allOtherParticipants(i)}: outcome=${newOutcomes(i)}, nextToSend=${indexOfNextRecordToSend_ByParticipantIndex(i)}, knownAppended=${highestRecordIndexKnownToBeAppended_ByParticipantIndex(i)}, knowCommitted=${highestRecordIndexKnowToBeCommitted_ByParticipantIndex(i)}").mkString("[", "; ", "]")}") // TODO delete
													retryLaggingLearners(accessible2, config2, newOutcomes, indexAfterTopRecordSent, untilNoneLags, abortIfAnotherReplicationStarts, serialOfReplicationAttempt)
											}
										}
									} yield newAppendAttemptInfo
								}
							} yield newAppendAttemptInfo
						}
					}
				}
			}

			/** Handles the result of an [[ClusterParticipant.appendRecords]] call.
			 *
			 * Updates the [[indexOfNextRecordToSend_ByParticipantIndex]], [[highestRecordIndexKnownToBeAppended_ByParticipantIndex]], and [[highestRecordIndexKnowToBeCommitted_ByParticipantIndex]] arrays and maps the [[AppendResult]] to an [[AppendOutcome]].
			 * @param primaryState the current [[PrimaryState]].
			 * @param config the active [[Configuration]]. Assumes it is derived from `config`.
			 * @param participantId the identifier of the participant whose response is being handled.
			 * @param participantIndex the index of the participant in the [[Configuration.allOtherParticipants]] derived from the provided [[PrimaryState]].
			 * @param appendResponse the response from the peer about the appending.
			 * @param appendRequestTerm the term passed to [[ClusterParticipant.appendRecords]] as `inquirerTerm` parameter.
			 * @param indexAfterTopRecordSent index of the record after the one at the top of the [[IndexedSeq]] of [[Record]]s passed to [[ClusterParticipant.appendRecords]] as argument to the parameter named `records`.
			 * @param appendRequestLeaderCommit the [[commitIndex]] of this [[Leader]] when [[ClusterParticipant.appendRecords]] was called. Must match the value passed to the `leaderCommit` parameter. */
			private def handleAppendResponse(primaryState: Accessible, config: Configuration, participantId: ParticipantId, participantIndex: Int, appendResponse: AppendResponse | Null, appendRequestTerm: Term, indexAfterTopRecordSent: RecordIndex, appendRequestLeaderCommit: RecordIndex)(using Context): AppendOutcome = {
				appendResponse match {
					case null =>
						AO_MISSING_BECAUSE_PARTICIPANT_WAS_NOT_PART_OF_THE_CONFIGURATION

					case Success(appendResult) =>

						// Trace.trace(s"handleAppendResponse@${primaryState.currentTerm} from $participantId requested@$appendRequestTerm, dialog=$appendResponse, indexAfterTopRecordSent=$indexAfterTopRecordSent") // TODO delete line
						if appendResult.term > appendRequestTerm then AO_HAS_HIGHER_TERM
						else if appendResult.roleOrdinal == QUIESCED then AO_IS_QUIESCED
						else {
							if assertionsEnabled then {
								assert(appendResult.roleOrdinal != STARTING) // because the Starting role always defers to other role.
								assert(appendResult.term == appendRequestTerm || appendResult.roleOrdinal == RETIRING) // because the term in replies is always greater than or equal to the term in inquires.
							}

							val indexOfNextRecordToSend = indexOfNextRecordToSend_ByParticipantIndex(participantIndex)
							val highestRecordIndexKnownToBeAppended = highestRecordIndexKnownToBeAppended_ByParticipantIndex(participantIndex)
							val highestRecordIndexKnownToBeCommited = highestRecordIndexKnowToBeCommitted_ByParticipantIndex(participantIndex)

							if appendResult.roleOrdinal == RETIRING && appendResult.successOrIndexForNextAttempt != 0 then {
								val retireeExcludingConfigIndex = appendResult.successOrIndexForNextAttempt - 1 // see `Retiring.onAppendRecords`
								// If the commitIndex of the retiree is higher than the commitIndex of this leading participant when append was called, then this leader was crowned with a still not active StableConfigChange in its log, which became active in the retiree.
								// Else, the retiree is still retiring from a previous joint consensus and was included back, but it hasn't noticed yet.
								// In both cases it must be considered as a wild-card voter.
								highestRecordIndexKnowToBeCommitted_ByParticipantIndex(participantIndex) = retireeExcludingConfigIndex
								if retireeExcludingConfigIndex >= indexOfNextRecordToSend then indexOfNextRecordToSend_ByParticipantIndex(participantIndex) = appendResult.successOrIndexForNextAttempt
								highestRecordIndexKnownToBeAppended_ByParticipantIndex(participantIndex) = retireeExcludingConfigIndex
								AO_IS_RETIRING
							}
							// If the appending is obsolete (the records it intended to append are already appended), return AO_SUCCESS updating nothing.
							else if indexAfterTopRecordSent <= highestRecordIndexKnownToBeAppended then if appendResult.roleOrdinal == RETIRING then AO_IS_RETIRING else AO_SUCCESS
							// If the appending was successful, update the local knowledge about the participant and return AO_SUCCESS.
							else if appendResult.successOrIndexForNextAttempt == 0 then {
								if appendRequestLeaderCommit > highestRecordIndexKnownToBeCommited then highestRecordIndexKnowToBeCommitted_ByParticipantIndex(participantIndex) = appendRequestLeaderCommit
								if indexAfterTopRecordSent > indexOfNextRecordToSend then indexOfNextRecordToSend_ByParticipantIndex(participantIndex) = indexAfterTopRecordSent
								val indexOfTopRecordSent = indexAfterTopRecordSent - 1
								if indexOfTopRecordSent > highestRecordIndexKnownToBeAppended then highestRecordIndexKnownToBeAppended_ByParticipantIndex(participantIndex) = indexOfTopRecordSent
								if appendResult.roleOrdinal == RETIRING then AO_IS_RETIRING else AO_SUCCESS
							} else { // else the rejection was because the participant needs earlier records (the previous record's does not exist in its log or its term is not the same as in this participant log then):
								val suggestedIndexForNextAttempt = appendResult.successOrIndexForNextAttempt
								// Clamp the index of the first record to append in the next attempt after the highest record index known to be appended.
								val indexForNextAttempt = if suggestedIndexForNextAttempt <= highestRecordIndexKnownToBeAppended then highestRecordIndexKnownToBeAppended + 1 else suggestedIndexForNextAttempt
								// If the earlier record predates the snapshot, report an error.
								if indexForNextAttempt < primaryState.logBufferOffset then {
									// inform about the illegal state. // TODO analyze a better way to inform this problem after implementing the log snapshot mechanism if this situation is possible to happen at all (because highestRecordIndexKnownToBeAppended_ByParticipantIndex would always be greater than or equal to the lobBufferOffset).
									Trace.error(s"$boundParticipantId: Unable to append records to $participantId because its log has inconsistencies at records that predate the last snapshot. They are at indexes less than the logBufferOffset=${primaryState.logBufferOffset}. THIS SHOULD NOT HAPPEN. PLEASE REPORT THIS AS A BUG.")
									// return false without any change to the corresponding entry in indexOfNextRecordToSend_ByParticipantIndex and highestRecordIndexKnownToBeAppended_ByParticipantIndex.
									AO_NEEDS_RECORDS_THAT_PREDATE_SNAPSHOT
								} else {
									if assertionsEnabled then assert(indexForNextAttempt > highestRecordIndexKnownToBeCommited)
									indexOfNextRecordToSend_ByParticipantIndex(participantIndex) = indexForNextAttempt
									AO_NEEDS_EARLIER_RECORDS
								}
							}
						}

					case Failure(e) =>
						Trace.debug(s"$boundParticipantId: The replication to $participantId failed with:", e)
						AO_IS_UNREACHABLE
				}
			}

			/**
			 * Schedule [[ClusterParticipant.appendRecords]] calls for the participants that were [[AO_IS_UNREACHABLE]] in the previous attempt.
			 * @param correspondingParticipantIds the [[ParticipantId]] corresponding to the provided [[AppendOutcome]] array.
			 * @param previousAttemptOutcomes the [[AppendOutcome]]s of the previous attempt.
			 * @param indexAfterTopRecordSent the index after the top [[Record]] of the set of [[Records]] that should be included in the attempts. */
			private def scheduleUnreachableParticipantsRetry(correspondingParticipantIds: IArray[ParticipantId], previousAttemptOutcomes: IArray[AppendOutcome], indexAfterTopRecordSent: RecordIndex, serialOfReplicationAttempt: Int)(using Context): Unit = {
				Trace.step("scheduleUnreachableParticipantsRetry") {
					// if there are unreachable participants, request a wake-up to retry replicating the log to them.
					if serialOfReplicationAttempt == serialOfLastReplicationAttempt && previousAttemptOutcomes.contains(AO_IS_UNREACHABLE) then {
						val token = requestWakeUp(WakeUpReason.UnreachableFollowersRetry, () => {
							if currentRole eq this then {
								for {
									primaryState1 <- primaryStateFence.causalAnchor()
								} yield {
									if (currentRole eq this) && serialOfReplicationAttempt == serialOfLastReplicationAttempt then primaryState1 match {
										case Inaccessible => // do nothing
										case accessible1: Accessible =>
											if assertionsEnabled then assert(accessible1.currentTerm == leadedTerm)
											val config1 = deriveConfigurationFrom(accessible1)

											// Build an "append" task for each unreachable participant
											val previousAttemptOutcomesLength = previousAttemptOutcomes.length
											val unreachableParticipantIds = new ArrayBuffer[ParticipantId](previousAttemptOutcomesLength)
											val appendRequests = new ArrayBuffer[AppendRequest](previousAttemptOutcomesLength)
											var previousAttemptOutcomeIndex = previousAttemptOutcomesLength
											while previousAttemptOutcomeIndex > 0 do {
												previousAttemptOutcomeIndex -= 1

												if previousAttemptOutcomes(previousAttemptOutcomeIndex) == AO_IS_UNREACHABLE then {
													val participantId = correspondingParticipantIds(previousAttemptOutcomeIndex)
													val participantIndex = config1.participantIndexOf(participantId)
													if participantIndex >= 0 then {
														unreachableParticipantIds.addOne(participantId)
														val indexOfNextRectorToSend = indexOfNextRecordToSend_ByParticipantIndex(participantIndex)
														appendRequests.addOne(appendsRecordsToParticipant(accessible1, participantId, participantIndex, indexOfNextRectorToSend, indexAfterTopRecordSent))
													}
												}
											}
											Trace.trace(s"Retrying the appendRecords RPCs to the unreachable participants $unreachableParticipantIds in replication #$serialOfReplicationAttempt")
											// Execute the tasks, handle the results, and schedule a retry for the still unreachable participants.
											val commitIndexAtAppendRequest = commitIndex
											for {
												appendResults <- sequenceAppendRequests(appendRequests)
												primaryState2 <- primaryStateFence.causalAnchor()
											} do if (currentRole eq this) && highestTermIn(appendResults) == leadedTerm then {
												primaryState2 match {
													case Inaccessible => // do nothing
													case accessible2: Accessible =>
														if assertionsEnabled then assert(accessible2.currentTerm == leadedTerm) // because the Leader Role should never bump the term before leaving transitioning to another Role.
														val config2 = deriveConfigurationFrom(accessible2)
														val appendsOutcomes = appendResults.mapWithIndex { (appendResult, resultIndex) =>
															val participantId = unreachableParticipantIds(resultIndex)
															val participantIndex = config2.participantIndexOf(participantId)
															if participantIndex < 0 then AO_SKIPPED_BECAUSE_OUT_OF_CONFIGURATION
															else handleAppendResponse(accessible2, config2, participantId, participantIndex, appendResult, accessible1.currentTerm, indexAfterTopRecordSent, commitIndexAtAppendRequest)
														}
														Trace.trace(s"The append responses to the unreachable-retry of replication #$serialOfReplicationAttempt until $indexAfterTopRecordSent have been handled, excludingConfigIndex=$indexOfConfigChangeThatExcludedThisParticipant, outcomes=${unreachableParticipantIds.zip(appendsOutcomes)}") // TODO delete
														scheduleUnreachableParticipantsRetry(IArray.from(unreachableParticipantIds), appendsOutcomes, indexAfterTopRecordSent, serialOfReplicationAttempt)
												}
											}
									}
								}
							}
						})
						unreachableFollowersRetryWakeUp = Maybe(token)
					}
				}
			}

			/**
			 * CAUTION: this method is called from a [[PrimaryState]] updater. So, limit the implementation to operation allowed there.
			 * @note that for the result of this operation be fiable, the [[PrimaryState]] should have stayed constant since the last call to [[deriveConfigurationFrom]].
			 * @return true if this leading participant is not included in the active [[Configuration]] and all the followers in the new [[Configuration]] have committed the [[StableConfigChange]] that excludes this participant. */
			private def isGhostAndAllFollowersCommittedTheExcludingConfigChange: Boolean = {
				isGhost && IArray.unsafeFromArray(highestRecordIndexKnowToBeCommitted_ByParticipantIndex).forallWithIndex((highestRecordIndexKnowToBeCommitted, _) => highestRecordIndexKnowToBeCommitted >= indexOfConfigChangeThatExcludedThisParticipant)
			}

			/** Consolidates the many [[sequencer.Task]]s into a single [[sequencer.LatchingDuty]] that yields an array with the results of the [[sequencer.Task]]s.
			 * TODO this is inefficient because the pace is determined by the slowest. Implement it using a [[sequencer.StreamDuty]] instead. */
			private inline def sequenceAppendRequests(appendRequests: scala.collection.IndexedSeq[AppendRequest]): sequencer.LatchingDuty[IArray[AppendResponse]] = {
				for appendDialog <- sequencer.LatchingDuty_sequenceTasksToArray(appendRequests, true) yield IArray.unsafeFromArray(appendDialog)
			}

			def handoffAndBumpTermIfLessThan(seenTerm: Term)(using Context): Role = {
				Trace.step("handoffAndBumpTermIfLessThan") {
					if thisLeader.leadedTerm < seenTerm then {
						Trace.trace(s"About to hand-off due to a higher term seen.")
						become(HandingOff(thisLeader.leadedTerm, seenTerm, primaryStateFence))
					} else thisLeader
				}
			}

			override def onTermBumped(primaryState: PrimaryState)(using Context): PrimaryState = {
				Trace.step("onTermBumped") {
					Trace.trace(s"About to hand-off due to term bump.")
					val config = deriveConfigurationFrom(primaryState)
					if config.allParticipants.contains(boundParticipantId) then {
						become(Isolated(primaryStateFence))
						primaryState
					} else {
						become(Retiring(primaryState.currentTerm, config.term, config.changeIndex, config.allParticipants))
						Inaccessible
					}
				}
			}
		}

		/** Orchestrates the synchronization of an excluded participant to prepare it for retirement.
		 *
		 * This driver performs a log-matching loop, invoking [[appendRecords]] on the target until its log contains the [[StableConfigChange]] record that triggered its exclusion.
		 * By passing a `leaderCommit` equal to the index of that configuration change, the driver ensures the remote participant's active [[Configuration]] transitions to the stable set that excludes it.
		 *
		 * In other words: pushes uncommitted records + leaderCommit to followers so they can enter [[Retiring]]; authorizeQuiescenceTo/PermitQuiesce grants them permission to transition from [[Retiring]] → [[Quiesced]]. Neither alone is sufficient.
		 *
		 * @param id the [[ParticipantId]] of the retiring participant.
		 * @param termOfHighestRecordKnownToBeAppended the term of the [[Record]] immediately preceding the first potentially unappended [[Record]].
		 * @param potentiallyUnappendedRecords a sequence of [[Record]]s starting from the lowest potentially unappended [[RecordIndex]], up to the [[StableConfigChange]] record.
		 * @param indexOfFirstPotentiallyUnappendedRecord the index of the first [[Record]] in the potentially unappended records sequence.
		 * @param configChangeIndex the index of the [[StableConfigChange]] record that caused the exclusión.
		 * @param configChangeTerm the term of the [[StableConfigChange]] record.
		 * @param indexOfNextRecordToSend the index of the next record to be replicated, upper-bounded by `configChangeIndex`.
		 * */
		private class RetirementDriver(
			id: ParticipantId,
			termOfHighestRecordKnownToBeAppended: Term,
			potentiallyUnappendedRecords: GenIndexedSeq[Record],
			indexOfFirstPotentiallyUnappendedRecord: RecordIndex,
			configChangeIndex: RecordIndex,
			configChangeTerm: Term,
			private var indexOfNextRecordToSend: RecordIndex
		) {
			if assertionsEnabled then {
				assert(commitIndex >= configChangeIndex)
				assert(potentiallyUnappendedRecords.isEmpty || indexOfFirstPotentiallyUnappendedRecord + potentiallyUnappendedRecords.length - 1 == configChangeIndex)
				assert(indexOfNextRecordToSend >= indexOfFirstPotentiallyUnappendedRecord)
			}

			/** Starts the process that makes a retiring participant to append the records until the [[StableConfigChange]] that caused its exclusión, passing a `leaderCommit` equal to the index of that same [[StableConfigChange]] record.
			 * This method is called immediately after this [[RetirementDriver]] instance is created and added to the [[retirementDriverByParticipantId]] map, which happens during a [[Configuration]] transition.
			 *
			 *  @param currentTerm0 the [[Term]] of the current [[PrimaryState]]. In the first call is the [[PrimaryState]] from which the [[Configuration]] transition that produced this [[RetirementDriver]] is derived. In the recursión calls is the [[PrimaryState.currentTerm]] when the [[AppendResponse]] arrived if the [[currentRole]] is [[StatefulRole]], or the highest term seen before leaving it. */
			def driveLoop(currentTerm0: Term, retriesDone: Int)(using Context): Unit = {
				Trace.step("driveLoop") {

					Trace.trace(s"RetirementDriver($id, term=$termOfHighestRecordKnownToBeAppended, records=$potentiallyUnappendedRecords, firstIndex=$indexOfFirstPotentiallyUnappendedRecord, changeIndex=$configChangeIndex, changeTerm=$configChangeTerm, indexOfNextRecordToSend=$indexOfNextRecordToSend).startAppendLoop(retriesDone=$retriesDone) called") // TODO delete line
					val inquire =
						if indexOfNextRecordToSend > configChangeIndex then {
							id.appendRecords(
								currentTerm0,
								configChangeIndex,
								configChangeTerm,
								IndexedSeq.empty,
								configChangeIndex,
								configChangeTerm
							)
						} else {
							val previousRecordIndex = indexOfNextRecordToSend - 1
							val previousRecordTerm =
								if previousRecordIndex < indexOfFirstPotentiallyUnappendedRecord then termOfHighestRecordKnownToBeAppended
								else potentiallyUnappendedRecords((previousRecordIndex - indexOfFirstPotentiallyUnappendedRecord).toInt).term
							id.appendRecords(
								currentTerm0,
								previousRecordIndex,
								previousRecordTerm,
								potentiallyUnappendedRecords.drop((indexOfNextRecordToSend - indexOfFirstPotentiallyUnappendedRecord).toInt),
								configChangeIndex,
								configChangeTerm
							)
						}
					inquire.trigger(true) { response =>
						// if the driver wasn't removed...
						for driver <- retirementDriverByParticipantId.get(id) do {
							// if the driver instance was replaced with a newer one, ignore the response. Else:
							if driver eq this then {

								// Enqueue the processing of the response in the primary state fence if the currentRole is stateful. Also get the current term.
								for currentTerm1 <- currentRole match {
										case stateful: StatefulRole =>
											for primaryState1 <- stateful.primaryStateFence.causalAnchor() yield primaryState1.currentTerm
										case _ =>
											sequencer.LatchingDuty_ready(currentTerm0)
								} do response match {
									case Success(appendResult) =>
										// If the appending was successful or the target is already retired, then: terminate this driver and, if this participant is retiring, authorized to become Quiesced by the incoming leader, and all the drivers have completed; become Quiesced.
										if appendResult.successOrIndexForNextAttempt == 0 || appendResult.roleOrdinal < JOINING then {
											retirementDriverByParticipantId.remove(id)
											becomeQuiescedIfEligible(configChangeIndex)
										}
										// If the appending was rejected for an unreversible reason, terminate this driver, scribe a message and, if this participant is retiring, authorized to become Quiesced by the incoming leader, and all the drivers have completed; become Quiesced.
										else if appendResult.term != currentTerm0 then {
											Trace.debug(s"$boundParticipantId: Aborting the replication to retire participant $id because its response ($appendResult) tells this retirement driver is obsolete, configChangeIndex=$configChangeIndex, configChangeTerm=$configChangeTerm")
											retirementDriverByParticipantId.remove(id)
											becomeQuiescedIfEligible(configChangeIndex)
										}
										// Else, the rejection was because the retiree needs earlier records. So, retry including them.
										else {
											val newIndexOfNextRecordToSend = appendResult.successOrIndexForNextAttempt
											indexOfNextRecordToSend = newIndexOfNextRecordToSend
											// if the earlier records are in the potentiallyUnappendedRecords array, then retry the appending including them.
											if newIndexOfNextRecordToSend >= indexOfFirstPotentiallyUnappendedRecord then driveLoop(currentTerm1, 0)
											else {
												Trace.error(s"$boundParticipantId: THIS SHOULD NOT HAPPEN! The retiring participant $id asks for earlier records than the expected: result=$appendResult, indexOfFirstPotentiallyUnappendedRecord=$indexOfFirstPotentiallyUnappendedRecord, configChangeIndex=$configChangeIndex, configChangeTerm=$configChangeTerm")
												if assertionsEnabled then assert(false)
												retirementDriverByParticipantId.remove(id)
												becomeQuiescedIfEligible(configChangeIndex)
											}
										}

									case Failure(e) =>
										if retriesDone > retiringParticipantMaxRetries then Trace.error(s"$boundParticipantId: The replication to the retiring participant $id is aborted because it failed too many times. The last attempt failure was:", e)
										else {
											val nextRetryNumber = retriesDone + 1
											Trace.debug(s"$boundParticipantId: The replication to the retiring participant $id failed. Scheduling retry #$nextRetryNumber. The failure was:", e)
											requestWakeUp(
												WakeUpReason.RetiringParticipantRetry,
												() => if retirementDriverByParticipantId.contains(id) then driveLoop(currentTerm1, nextRetryNumber)
											)
										}
									
								}
							}
						}
					}
				}
			}
		}

		/** Attempts to transition this participant to the [[QUIESCED]] role.
		 *
		 * This check is performed whenever a potential prerequisite for quiescence is met (e.g., is retiring, a [[RetirementDriver]] finishes, or permission is granted).
		 * The transition only proceeds if the participant is in the [[RETIRING]] role, no [[RetirementDriver]] is active, and protocol permission was granted.
		 * Three independent async processes must converge: (a) all RetirementDrivers must complete/be removed, (b) role must be RETIRING, (c) quiescence permission must be granted. And that these are fulfilled by different mechanisms (RetirementDriver.driveLoop, become(Retiring), authorizeQuiescenceTo).
		 */
		private def becomeQuiescedIfEligible(indexOfExcludingConfigChange: RecordIndex)(using Trace.Context): Unit = {
			Trace.step("becomeQuiescedIfEligible") {
				if retirementDriverByParticipantId.isEmpty && currentRole.ordinal == RETIRING && indexOfExcludingConfigChange == indexOfStableConfigChangeForWhichQuiescenceWasPermitted
				then become(Quiesced(Success(s"The incoming leader ${quiescenceGrantor.value} authorized quiescence and no retirement driver exists.")))

			}
		}

		//// Primary State

		/** A view of the participant’s current primary state (log, term, etc.), and also trivially derived state.
		 *
		 * IMPORTANT: the [[Accessible]] subtype of this trait exposes mutable state. To preserve causal ordering:
		 * - All writes must occur inside an updater passed to [[StatefulRole.primaryStateFence.advance]].
		 * - All reads must occur either inside said updater or in a consumer subscribed to [[StatefulRole.primaryStateFence.causalAnchor]].
		 *
		 * Direct mutation or observation of [[Accessible]] outside these mechanisms breaks causal guarantees.
		 */
		private sealed trait PrimaryState {
			/** The [[Term]] of this [[PrimaryState]]. Should be immutable because it is accessed after updates of the [[Workspace]]. */
			val currentTerm: Term

			/** Index of the first empty record in the log. Should be immutable because it is accessed after updates of the [[Workspace]].
			 * This is trivially derived state. */
			val firstEmptyRecordIndex: RecordIndex

			def withTermUpdated(term: Term)(using Trace.Context): sequencer.LatchingDuty[PrimaryState]
		}

		/** The [[PrimaryState]] value when this [[ConsensusParticipant]] does not have access to the [[Storage]] where the primary state is persisted. Either because it does not need it (gracefully [[Quiesced]]), is [[Starting]], or became [[Quiesced]] due to a failure. */
		private object Inaccessible extends PrimaryState {
			override val currentTerm: Term = PRE_INIT
			override val firstEmptyRecordIndex: RecordIndex = 0

			override def withTermUpdated(term: Term)(using Trace.Context): sequencer.LatchingDuty[PrimaryState] = sequencer.LatchingDuty_ready(this)
		}

		/** Defines the [[PrimaryState]] when this [[ConsensusParticipant]] has access to the [[Storage]] where the primary state is persisted. */
		private final class Accessible(workspace: WS) extends PrimaryState {

			override val currentTerm: Term = workspace.getCurrentTerm
			override val firstEmptyRecordIndex: RecordIndex = workspace.firstEmptyRecordIndex

			/** CAUTION: This method accesses mutable state of the [[PrimaryState]], so it should be called either:
			 *   - within an [[StatefulRole.primaryStateFence.advance]] section. In this case, the call must occur before or during the completion of the [[sequencer.LatchingDuty]] returned by the function passed	to `advance`; once that duty has completed, the causal fence is closed and later calls are unsafe.
			 *   - within a causally anchored consumer (i.e. consumers subscribed to the [[sequencer.LatchingDuty]] returned by either [[StatefulRole.primaryStateFence.advance]] or [[StatefulRole.primaryStateFence.causalAnchor]]). In this case, the call must occur synchronously during the consumer’s execution; it must not be deferred to code scheduled after the consumer has returned, since such deferred code would no longer be causally anchored. */
			inline def getRecordAt(index: RecordIndex): Record =
				workspace.getRecordAt(index)


			/** CAUTION: This method accesses mutable state of the [[PrimaryState]], so it should be called either:
			 *   - within an [[StatefulRole.primaryStateFence.advance]] section. In this case, the call must occur before or during the completion of the [[sequencer.LatchingDuty]] returned by the function passed	to `advance`; once that duty has completed, the causal fence is closed and later calls are unsafe.
			 *   - within a causally anchored consumer (i.e. consumers subscribed to the [[sequencer.LatchingDuty]] returned by either [[StatefulRole.primaryStateFence.advance]] or [[StatefulRole.primaryStateFence.causalAnchor]]). In this case, the call must occur synchronously during the consumer’s execution; it must not be deferred to code scheduled after the consumer has returned, since such deferred code would no longer be causally anchored. */
			inline def getRecordTermAt(index: RecordIndex): Term =
				if index == 0 then PRE_INIT
				else workspace.getRecordAt(index).term

			/** CAUTION: This method accesses mutable state of the [[PrimaryState]], so it should be called either:
			 *   - within an [[StatefulRole.primaryStateFence.advance]] section. In this case, the call must occur before or during the completion of the [[sequencer.LatchingDuty]] returned by the function passed	to `advance`; once that duty has completed, the causal fence is closed and later calls are unsafe.
			 *   - within a causally anchored consumer (i.e. consumers subscribed to the [[sequencer.LatchingDuty]] returned by either [[StatefulRole.primaryStateFence.advance]] or [[StatefulRole.primaryStateFence.causalAnchor]]). In this case, the call must occur synchronously during the consumer’s execution; it must not be deferred to code scheduled after the consumer has returned, since such deferred code would no longer be causally anchored. */
			inline def getRecordsBetween(from: RecordIndex, until: RecordIndex): GenIndexedSeq[Record] = {
				workspace.getRecordsBetween(from, until)
			}

			/** CAUTION: This method accesses mutable state of the [[PrimaryState]], so it should be called either:
			 *   - within an [[StatefulRole.primaryStateFence.advance]] section. In this case, the call must occur before or during the completion of the [[sequencer.LatchingDuty]] returned by the function passed	to `advance`; once that duty has completed, the causal fence is closed and later calls are unsafe.
			 *   - within a causally anchored consumer (i.e. consumers subscribed to the [[sequencer.LatchingDuty]] returned by either [[StatefulRole.primaryStateFence.advance]] or [[StatefulRole.primaryStateFence.causalAnchor]]). In this case, the call must occur synchronously during the consumer’s execution; it must not be deferred to code scheduled after the consumer has returned, since such deferred code would no longer be causally anchored. */
			inline def logBufferOffset: RecordIndex =
				workspace.logBufferOffset

			/** CAUTION: This method accesses mutable state of the [[PrimaryState]], so it should be called either:
			 *   - within an [[StatefulRole.primaryStateFence.advance]] section. In this case, the call must occur before or during the completion of the [[sequencer.LatchingDuty]] returned by the function passed	to `advance`; once that duty has completed, the causal fence is closed and later calls are unsafe.
			 *   - within a causally anchored consumer (i.e. consumers subscribed to the [[sequencer.LatchingDuty]] returned by either [[StatefulRole.primaryStateFence.advance]] or [[StatefulRole.primaryStateFence.causalAnchor]]). In this case, the call must occur synchronously during the consumer’s execution; it must not be deferred to code scheduled after the consumer has returned, since such deferred code would no longer be causally anchored. */
			inline def indexOfTopConfigChange: RecordIndex =
				workspace.indexOfTopConfigChange

			/** CAUTION: This method accesses mutable state of the [[PrimaryState]], so it should be called either:
			 *   - within an [[StatefulRole.primaryStateFence.advance]] section. In this case, the call must occur before or during the completion of the [[sequencer.LatchingDuty]] returned by the function passed	to `advance`; once that duty has completed, the causal fence is closed and later calls are unsafe.
			 *   - within a causally anchored consumer (i.e. consumers subscribed to the [[sequencer.LatchingDuty]] returned by either [[StatefulRole.primaryStateFence.advance]] or [[StatefulRole.primaryStateFence.causalAnchor]]). In this case, the call must occur synchronously during the consumer’s execution; it must not be deferred to code scheduled after the consumer has returned, since such deferred code would no longer be causally anchored. */
			inline def indexOfLastAppendedCommandFrom(clientId: ClientId): RecordIndex = {
				workspace.indexOfLastAppendedCommandFrom(clientId)
			}

			/** CAUTION: This method accesses mutable state of the [[PrimaryState]], so it should be called either:
			 *   - within an [[StatefulRole.primaryStateFence.advance]] section. In this case, the call must occur before or during the completion of the [[sequencer.LatchingDuty]] returned by the function passed	to `advance`; once that duty has completed, the causal fence is closed and later calls are unsafe.
			 *   - within a causally anchored consumer (i.e. consumers subscribed to the [[sequencer.LatchingDuty]] returned by either [[StatefulRole.primaryStateFence.advance]] or [[StatefulRole.primaryStateFence.causalAnchor]]). In this case, the call must occur synchronously during the consumer’s execution; it must not be deferred to code scheduled after the consumer has returned, since such deferred code would no longer be causally anchored. */
			inline def indexOf(clientCommand: ClientCommand): RecordIndex =
				workspace.indexOf(clientCommand)

			/** CAUTION: This method accesses mutable state of the [[PrimaryState]], so it should be called either:
			 *   - within an [[StatefulRole.primaryStateFence.advance]] section. In this case, the call must occur before or during the completion of the [[sequencer.LatchingDuty]] returned by the function passed	to `advance`; once that duty has completed, the causal fence is closed and later calls are unsafe.
			 *   - within a causally anchored consumer (i.e. consumers subscribed to the [[sequencer.LatchingDuty]] returned by either [[StatefulRole.primaryStateFence.advance]] or [[StatefulRole.primaryStateFence.causalAnchor]]). In this case, the call must occur synchronously during the consumer’s execution; it must not be deferred to code scheduled after the consumer has returned, since such deferred code would no longer be causally anchored. */
			inline def informAppliedCommandIndex(appliedCommandIndex: RecordIndex): Unit = {
				workspace.informAppliedCommandIndex(appliedCommandIndex)
			}

			/** CAUTION: This method mutates of the [[PrimaryState]], so it should be called within the safe temporal windows provided by [[StatefulRole.primaryStateFence.advance]]. */
			override def withTermUpdated(newTerm: Term)(using Trace.Context): sequencer.LatchingDuty[PrimaryState] = {
				val currentTerm = workspace.getCurrentTerm
				if newTerm < currentTerm then sequencer.LatchingDuty_ready(new Accessible(workspace))
				else if newTerm == currentTerm then sequencer.LatchingDuty_ready(this)
				else {
					workspace.setCurrentTerm(newTerm)
					saveWorkspace()
				}
			}

			/** CAUTION: This method mutates of the [[PrimaryState]], so it should be called within the safe temporal windows provided by [[StatefulRole.primaryStateFence.advance]]. */
			def withSingleRecordAppended(term: Term, record: Record)(using Trace.Context): sequencer.LatchingDuty[PrimaryState] = {
				workspace.setCurrentTerm(term)
				workspace.appendRecord(record)
				saveWorkspace()
			}

			/** CAUTION: This method mutates of the [[PrimaryState]], so it should be called within the safe temporal windows provided by [[StatefulRole.primaryStateFence.advance]]. */
			def withRecordsAppended(term: Term, records: GenIndexedSeq[Record], from: RecordIndex)(using Trace.Context): sequencer.LatchingDuty[PrimaryState] = {
				workspace.setCurrentTerm(term)
				workspace.appendResolvingConflicts(records, from)
				saveWorkspace()
			}

			/** CAUTION: This method mutates of the [[PrimaryState]], so it should be called within the safe temporal windows provided by [[StatefulRole.primaryStateFence.advance]]. */
			def withWorkspaceReleased(): sequencer.Duty[Inaccessible.type] =
				workspace.releases.map(_ => Inaccessible)

			/** Saves the [[Workspace]] of this [[PrimaryState]] in the [[Storage]].
			 * CAUTION: This method accesses mutable state of the [[PrimaryState]], so it should be called within an [[StatefulRole.primaryStateFence.advance]] section only.
			 * @return the [[sequencer.LatchingDuty]] that yields the saved [[PrimaryState]] */
			private def saveWorkspace()(using Trace.Context): sequencer.LatchingDuty[PrimaryState] = {
				storage.save(workspace).map {
					case _: Success[Unit] =>
						if currentRole.isInstanceOf[StatefulRole] then new Accessible(workspace)
						// Release the workspace if the current role changed to a stateless one during the save.
						else {
							workspace.releases.triggerAndForget()
							Inaccessible
						}

					case failure: Failure[Unit] =>
						Trace.error(s"$boundParticipantId: Unexpected error while saving the workspace. This participant's consensus service is unable to continue following the leader and will quiesce.", failure.exception)
						become(Quiesced(failure.castTo[String]))
						workspace.releases.triggerAndForget() // just in case storage.save does not do it.
						Inaccessible
				}
			}

			override def toString: String = s"Accessible(currentTerm=$currentTerm, firstEmptyRecordIndex=$firstEmptyRecordIndex, indexOfTopConfigChange=$indexOfTopConfigChange)"
		}

		/** Knows which are the participants involved in the consensus and defines rules pertaining to elections and replication that govern the participant's behavior.
		 *
		 * It has exactly three concrete subclasses:
		 *
		 *	- [[NoConfig]]: Active when quiesced or lack of access to the persistent state (log, term, etc), which drives the participant to [[Quiesced]].
		 *
		 *	- [[TransitionalConfig]]: Active during joint consensus (`Cold` ∪ `Cnew`).
		 *     This behavior begins immediately once the transitional entry is appended to the participant's log.
		 *     Replication and election quorum require majorities across both `Cold` and `Cnew`.
		 *     Transitional behavior remains active until the corresponding [[StableConfigChange]] entry is replicated to a majority of both `Cold` and `Cnew` ([[commitIndex]] >= indexOfCorrespondingStableConfigChange).
		 *
		 *	- [[StableConfig]]: Active during non-joint consensus (Cnew).
		 *     This behavior begins only once the backing [[StableConfigChange]] entry is committed (i.e. when [[commitIndex]] ≥ indexOfBackingStableConfigChange).
		 *     Replication and election quorum require the majority of `Cnew` only.
		 *     Non-leader `Cold` only participants having commited the [[StableConfigChange]] entry that excluded them, do transition to [[Retiring]] and wait authorization to quiesce from a stable leader of a succeeding term.
		 *     A leader `Cold` only participant having commited the [[StableConfigChange]] entry that excluded it, do stay leading as ghost leader until either: it sees all the other participants had commited said [[StableConfigChange]]; or it receives either an "append records" or "authorization to quiesce" RPC from a leader of a higher term.
		 *
		 * === Commit Index Dependency ===
		 * - [[TransitionalConfig]] behavior starts on append of its entry, but ends when the stable entry is committed.
		 * - [[StableConfig]] behavior starts only when its entry is committed.
		 * Thus, despite each of these two behaviors depend only on primary state (the log), the whole configuration behavior is commit-sensitive because the transition instant between them depends on [[commitIndex]].
		 * */
		private sealed trait Configuration {
			/** The [[Term]] of the backing [[ConfigChange]]. */
			val term: Term
			/** The index of the backing [[ConfigChange]]. */
			val changeIndex: RecordIndex
			/** The current set of participants involved in the consensus, sorted.
			 * Should be reflected in the [[Workspace]].
			 * */
			val allParticipants: IArray[ParticipantId]
			/** The current set of participants involved in the consensus, excluding this participant, sorted.
			 * Should be updated whenever [[currentParticipants]] mutates */
			val allOtherParticipants: IArray[ParticipantId]

			/** The [[ConfigChange]] that caused this [[Configuration]] and on which it is based.
			 * A [[ConfigChange]]s is a snapshots, so it contain all the information needed. */
			val backingConfigChange: ConfigChange[ParticipantId]

			/** The identifiers of the desired set of participants (backing [[ConfigChange.newParticipants]]). */
			val desiredParticipants: Set[ParticipantId]

			inline def isInNew(participantId: ParticipantId): Boolean = desiredParticipants.contains(participantId)

			/** The set of participants to include in [[Unable]] responses. */
			def otherProbableParticipants: ListSet[ParticipantId]

			def reachedAll(vote: Vote[ParticipantId]): Boolean

			def reachedAMajority(vote: Vote[ParticipantId]): Boolean

			def indexOfTheCommittedRecordWithHighestIndex(primaryState: Accessible, from: RecordIndex, highestRecordIndexKnowToBeAppended_ByParticipantIndex: IArray[RecordIndex], appendOutcomes: IArray[AppendOutcome]): RecordIndex

			/** @param appendOutcomes the [[AppendOutcome]] of each other participant, indexed according to [[allOtherParticipants]].
			 * @return true if at least half of the [[AppendOutcome]]s in the provided array are [[AO_SUCCESS]]. */
			def achievesQuorumWhen(appendOutcomes: IArray[AppendOutcome]): Boolean

			/** Determines the [[Role]] to become based on the votes of all the participants. */
			def determineRole(primaryState: Accessible, primaryStateFence: sequencer.CausalFence[PrimaryState], myVote: Vote[ParticipantId], votesFromOthers: Array[Try[Vote[ParticipantId]]])(using Trace.Context): Maybe[Role]

			/**
			 * Determines the best leader candidate based on the [[StateInfo]]s of all the participants, including itself.
			 * This method only queries. Does not mutate anything.
			 *
			 * @param howAreYouAnswers the answers to the [[ClusterParticipant.howAreYou]] questions done to the other participants, stored as a parallel array with index correspondence [[allOtherParticipants]].
			 * @return A task that yields a [[Vote]] with the chosen leader for the current term.
			 */
			def decideMyVote(myStateInfo: StateInfo, howAreYouAnswers: IArray[StateInfo | Null])(using Trace.Context): Vote[ParticipantId]

			/** @return the index of the provided [[ParticipantId]] in the [[allOtherParticipants]]' [[IndexedSeq]] or a negative number if not present.
			 * @param otherParticipantId the id of the participant to find. */
			inline def participantIndexOf(otherParticipantId: ParticipantId): Int = {
				java.util.Arrays.binarySearch(allOtherParticipants.asInstanceOf[Array[ParticipantId]], otherParticipantId, participantIdComparator)
			}

			override def toString: String = backingConfigChange.toString
		}

		private object NoConfig extends Configuration {
			override val term: Term = PRE_INIT

			override val changeIndex: RecordIndex = 0

			override val allParticipants: IArray[ParticipantId] = IArray(boundParticipantId)

			override val allOtherParticipants: IArray[ParticipantId] = IArray.empty

			override val backingConfigChange: ConfigChange[ParticipantId] = null

			override val desiredParticipants: Set[ParticipantId] = Set.empty

			override def otherProbableParticipants: ListSet[ParticipantId] =
				cluster.getOtherProbableParticipant

			override def reachedAll(vote: Vote[ParticipantId]): Boolean =
				false

			override def reachedAMajority(vote: Vote[ParticipantId]): Boolean =
				false

			override def indexOfTheCommittedRecordWithHighestIndex(primaryState: Accessible, from: RecordIndex, highestRecordIndexKnowToBeAppended_ByParticipantIndex: IArray[RecordIndex], appendOutcomes: IArray[AppendOutcome]): RecordIndex =
				from

			override def achievesQuorumWhen(appendOutcomes: IArray[AppendOutcome]): Boolean =
				false

			override def determineRole(primaryState: Accessible, primaryStateFence: sequencer.CausalFence[PrimaryState], myVote: Vote[ParticipantId], votesFromOthers: Array[Try[Vote[ParticipantId]]])(using Trace.Context): Maybe[Role] =
				Maybe(illegalStateQuiesce())

			override def decideMyVote(myStateInfo: StateInfo, howAreYouAnswers: IArray[StateInfo | Null])(using Trace.Context): Vote[ParticipantId] =
				currentRole.blankVote(myStateInfo.currentTerm)
		}

		/** Pure new configuration (`Cnew`).
		 *
		 * Activated only once the [[StableConfigChange]] entry is committed (commitIndex ≥ indexOfStableConfigChange).
		 * Replication and quorum reduce to `Cnew` only.
		 * Cold-only servers, having seen this entry, shut down.
		 */
		private final class StableConfig(override val backingConfigChange: StableConfigChange[ParticipantId], override val changeIndex: RecordIndex) extends Configuration {
			override val term: Term = backingConfigChange.term
			override val allParticipants: IArray[ParticipantId] = IArray.unsafeFromArray(backingConfigChange.newParticipants.toArray.sorted)
			private val halfTheNumberOfParticipants = allParticipants.length / 2
			override val allOtherParticipants: IArray[ParticipantId] = allParticipants.filter(_ != boundParticipantId)

			override val desiredParticipants: Set[ParticipantId] = backingConfigChange.newParticipants

			def otherProbableParticipants: ListSet[ParticipantId] = {
				ListSet.newBuilder
					.addAll(allOtherParticipants)
					.addAll(cluster.getOtherProbableParticipant)
					.result()
			}

			override def reachedAll(vote: Vote[ParticipantId]): Boolean = {
				vote.reachableCandidatesOfOldConf == allParticipants.length
			}

			override def reachedAMajority(vote: Vote[ParticipantId]): Boolean = {
				vote.reachableCandidatesOfOldConf > halfTheNumberOfParticipants || allParticipants.length == 0
			}

			override def indexOfTheCommittedRecordWithHighestIndex(primaryState: Accessible, from: RecordIndex, highestRecordIndexKnowToBeAppended_ByParticipantIndex: IArray[RecordIndex], appendOutcomes: IArray[AppendOutcome]): RecordIndex = {
				var n = primaryState.firstEmptyRecordIndex - 1
				while n > from && (
					highestRecordIndexKnowToBeAppended_ByParticipantIndex.countWithIndex((hri, _) => hri >= n) < halfTheNumberOfParticipants
						|| primaryState.getRecordTermAt(n) != primaryState.currentTerm // This second term or the `or` enforces Raft §5.4.2 ("A leader cannot determine commitment using entries from previous terms") and that a leader inheriting previous-term records cannot commit them without first committing a current-term record
					)
				do n -= 1
				n
			}

			override def achievesQuorumWhen(appendSummaries: IArray[AppendOutcome]): Boolean = {
				appendSummaries.countWithIndex { (summary, index) => summary == AO_SUCCESS } >= halfTheNumberOfParticipants
			}

			override def determineRole(primaryState: Accessible, primaryStateFence: sequencer.CausalFence[PrimaryState], myVote: Vote[ParticipantId], votesFromOthers: Array[Try[Vote[ParticipantId]]])(using Trace.Context): Maybe[Role] = {
				var votesMatchingMyVoteCount = 1 // includes my vote
				var newParticipantsJoining = 0
				for case Success(replierVote) <- votesFromOthers do {
					if replierVote.votedId == myVote.votedId && replierVote.isNonBlank then votesMatchingMyVoteCount += 1
					else if replierVote.rank == ER_JOINER then newParticipantsJoining += 1
				}
				if votesMatchingMyVoteCount + newParticipantsJoining <= halfTheNumberOfParticipants then Isolated(primaryStateFence)
				else if myVote.votedId == boundParticipantId then Promoting(primaryState.currentTerm, primaryStateFence)
				else Follower(primaryState.currentTerm, myVote.votedId, primaryStateFence)
			}

			override def decideMyVote(myStateInfo: StateInfo, howAreYouAnswers: IArray[StateInfo | Null])(using Trace.Context): Vote[ParticipantId] = {
				Trace.step("decideMyVote") {
					var latestTermSeen = myStateInfo.currentTerm
					var chosenCandidate = new CandidateInfo(boundParticipantId, true, myStateInfo)
					var borrame = List(chosenCandidate) //TODO delete line
					var reachableCandidatesCounter = if myStateInfo.rank.in(ERS_COUNT_IN_NEW) then 1 else 0 // includes itself

					howAreYouAnswers.foreachWithIndex { (reply, replierIndex) =>
						val replierId = allOtherParticipants(replierIndex)
						reply match {
							case null =>
								Trace.debug(s"$boundParticipantId: `$replierId.howAreYou` failed while deciding vote")

							case replierInfo: StateInfo =>
								if replierInfo.rank.in(ERS_COUNT_IN_NEW) then reachableCandidatesCounter += 1
								if replierInfo.currentTerm > latestTermSeen then {
									latestTermSeen = replierInfo.currentTerm
								}
								val candidateInfo = new CandidateInfo(replierId, true, replierInfo)
								borrame = candidateInfo :: borrame //TODO delete line
								chosenCandidate = chosenCandidate.getWinnerAgainst(candidateInfo)

						}
					}
					Trace.debug(s"$boundParticipantId: decideMyVote($myStateInfo, ${howAreYouAnswers.mkString("[", ", ", "]")}): chosen=$chosenCandidate, among: $borrame, config=$backingConfigChange") //TODO delete line
					Vote(latestTermSeen, chosenCandidate.id, reachableCandidatesCounter, 0, chosenCandidate.info.rank, myStateInfo.ballot)
				}
			}
		}

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
			override val allParticipants: IArray[ParticipantId] = IArray.unsafeFromArray(newParticipants.union(oldParticipants).toArray.sorted)
			override val allOtherParticipants: IArray[ParticipantId] = allParticipants.filter(_ != boundParticipantId)

			override val desiredParticipants: Set[ParticipantId] = newParticipants

			def otherProbableParticipants: ListSet[ParticipantId] = {
				ListSet.newBuilder
					.addAll(allOtherParticipants)
					.addAll(cluster.getOtherProbableParticipant)
					.result()
			}

			override def reachedAll(vote: Vote[ParticipantId]): Boolean = {
				vote.reachableCandidatesOfOldConf == oldParticipants.size && vote.reachableCandidatesOfNewConf == newParticipants.size
			}

			override def reachedAMajority(vote: Vote[ParticipantId]): Boolean = {
				(vote.reachableCandidatesOfOldConf > halfOfOldParticipants || oldParticipants.isEmpty)
					&& (vote.reachableCandidatesOfNewConf > halfOfNewParticipants || newParticipants.isEmpty)
			}

			override def indexOfTheCommittedRecordWithHighestIndex(primaryState: Accessible, from: RecordIndex, highestRecordIndexKnowToBeAppended_ByParticipantIndex: IArray[RecordIndex], appendOutcomes: IArray[AppendOutcome]): RecordIndex = {
				var n = primaryState.firstEmptyRecordIndex - 1
				while n > from do {
					// This if enforces Raft §5.4.2 ("A leader cannot determine commitment using entries from previous terms") and that a leader inheriting previous-term records cannot commit them without first committing a current-term record
					if primaryState.getRecordTermAt(n) == primaryState.currentTerm then {
						var oldParticipantsWithRecordAtNSuccessfullyAppended = 0
						var newParticipantsWithRecordAtNSuccessfullyAppended = 0

						var participantId = boundParticipantId
						var otherParticipantIndex = allOtherParticipants.length
						while otherParticipantIndex >= 0 do {
							if oldParticipants.contains(participantId) then oldParticipantsWithRecordAtNSuccessfullyAppended += 1
							if newParticipants.contains(participantId) then newParticipantsWithRecordAtNSuccessfullyAppended += 1

							var goNext = true
							otherParticipantIndex -= 1
							while otherParticipantIndex >= 0 && goNext do {
								participantId = allOtherParticipants(otherParticipantIndex)
								// The other participant's record at index `n` is considered up-to-date if either, an append that contains records of equal or greater index was successful, or the other participant is retiring and is part of the old configuration only.
								// Why are retiring participants considered up-to-date? Because during joint consensus, a newly crowned leader cannot directly commit previous-term records (such as a pending `StableConfigChange`). It must indirectly commit them by committing a record from its current term. Treating retiring participants as up-to-date for all records acts as a wildcard "YES" vote in the old configuration, allowing the new leader to commit current-term records and successfully transition out of joint consensus.
								if highestRecordIndexKnowToBeAppended_ByParticipantIndex(otherParticipantIndex) >= n
									|| (appendOutcomes(otherParticipantIndex) == AO_IS_RETIRING && !newParticipants.contains(participantId))
								then goNext = false
								else otherParticipantIndex -= 1
							}
						}
						if (oldParticipantsWithRecordAtNSuccessfullyAppended > halfOfOldParticipants || oldParticipants.isEmpty)
							&& (newParticipantsWithRecordAtNSuccessfullyAppended > halfOfNewParticipants || newParticipants.isEmpty)
						then return n
					}
					n -= 1
				}
				from
			}

			override def achievesQuorumWhen(appendOutcomes: IArray[AppendOutcome]): Boolean = {
				var newParticipantsWithSuccessfulAppendResult = 0
				var oldParticipantsWithSuccessfulAppendResult = 0
				var oldParticipantsRetiring = 0
				// start the loop with this participant, assuming it already appended the records and will persist its state after calling this method.
				var participantId = boundParticipantId
				var otherParticipantIndex = allOtherParticipants.length
				while otherParticipantIndex >= 0 do {
					if oldParticipants.contains(participantId) then oldParticipantsWithSuccessfulAppendResult += 1
					if newParticipants.contains(participantId) then newParticipantsWithSuccessfulAppendResult += 1

					var goNext = true
					otherParticipantIndex -= 1
					while otherParticipantIndex >= 0 && goNext do {
						appendOutcomes(otherParticipantIndex) match {
							case AO_SUCCESS =>
								participantId = allOtherParticipants(otherParticipantIndex)
								goNext = false
							case AO_IS_RETIRING =>
								if oldParticipants.contains(allOtherParticipants(otherParticipantIndex)) then oldParticipantsRetiring += 1
								otherParticipantIndex -= 1
							case _ =>
								otherParticipantIndex -= 1
						}
					}
				}
				(oldParticipantsWithSuccessfulAppendResult + oldParticipantsRetiring > halfOfOldParticipants || oldParticipants.isEmpty)
					&& (newParticipantsWithSuccessfulAppendResult > halfOfNewParticipants || newParticipants.isEmpty)
			}

			override def determineRole(primaryState: Accessible, primaryStateFence: sequencer.CausalFence[PrimaryState], myVote: Vote[ParticipantId], votesFromOthers: Array[Try[Vote[ParticipantId]]])(using Trace.Context): Maybe[Role] = {
				var oldParticipantsVotesMatchingMyVote = 0
				var newParticipantsVotesMatchingMyVote = 0
				var oldParticipantsRetiring = 0
				var newParticipantsJoining = 0
				var otherVote = myVote
				var participantId = boundParticipantId
				var otherParticipantIndex = votesFromOthers.length
				while otherParticipantIndex >= 0 do {
					if otherVote.votedId == myVote.votedId && otherVote.isNonBlank then {
						if oldParticipants.contains(participantId) then oldParticipantsVotesMatchingMyVote += 1
						if newParticipants.contains(participantId) then newParticipantsVotesMatchingMyVote += 1
					} else {
						if otherVote.rank == ER_RETIREE && oldParticipants.contains(otherVote.votedId) then oldParticipantsRetiring += 1
						if otherVote.rank == ER_JOINER && newParticipants.contains(otherVote.votedId) then newParticipantsJoining += 1
					}
					var goNext = true
					otherParticipantIndex -= 1
					// navigate to the next successful vote and get it.
					while otherParticipantIndex >= 0 && goNext do {
						votesFromOthers(otherParticipantIndex) match {
							case s: Success[Vote[ParticipantId]] =>
								otherVote = s.value
								participantId = allOtherParticipants(otherParticipantIndex)
								goNext = false

							case _: Failure[Vote[ParticipantId]] =>
								otherParticipantIndex -= 1
						}
					}
				}

				if (oldParticipantsVotesMatchingMyVote + oldParticipantsRetiring <= halfOfOldParticipants && oldParticipants.nonEmpty) || (newParticipantsVotesMatchingMyVote + newParticipantsJoining <= halfOfNewParticipants && newParticipants.nonEmpty) then Isolated(primaryStateFence)
				else if myVote.votedId == boundParticipantId then Promoting(primaryState.currentTerm, primaryStateFence)
				else Follower(primaryState.currentTerm, myVote.votedId, primaryStateFence)
			}

			override def decideMyVote(myStateInfo: StateInfo, howAreYouAnswers: IArray[StateInfo | Null])(using Trace.Context): Vote[ParticipantId] = {
				Trace.step("decideMyVote") {
					var participantId = boundParticipantId
					var highestTermSeen = myStateInfo.currentTerm

					var oldParticipantsThatAreReachable = 0
					var newParticipantsThatAreReachable = 0
					var isInOldConfig = oldParticipants.contains(participantId)
					var candidateStateInfo = myStateInfo
					var chosenCandidate = new CandidateInfo(participantId, isInOldConfig, myStateInfo)
					var borrame = List(chosenCandidate) //TODO delete line

					var participantIndex = allOtherParticipants.length
					while participantIndex >= 0 do {
						if isInOldConfig && candidateStateInfo.rank.in(ERS_COUNT_IN_OLD) then oldParticipantsThatAreReachable += 1
						if candidateStateInfo.rank.in(ERS_COUNT_IN_NEW) && newParticipants.contains(participantId) then newParticipantsThatAreReachable += 1

						var goNext = true
						participantIndex -= 1
						// navigate to the next successfully replied StateInfo and get it
						while participantIndex >= 0 && goNext do {
							participantId = allOtherParticipants(participantIndex)
							howAreYouAnswers(participantIndex) match {
								case null =>
									Trace.debug(s"$boundParticipantId: `$participantId.howAreYou` failed while deciding vote")
									participantIndex -= 1

								case stateInfo: StateInfo =>
									if stateInfo.currentTerm > highestTermSeen then highestTermSeen = stateInfo.currentTerm
									isInOldConfig = oldParticipants.contains(participantId)
									val candidateInfo = new CandidateInfo(participantId, isInOldConfig, stateInfo)
									candidateStateInfo = stateInfo
									chosenCandidate = chosenCandidate.getWinnerAgainst(candidateInfo)
									borrame = candidateInfo :: borrame //TODO delete line
									goNext = false
							}
						}
					}
					val result = Vote(highestTermSeen, chosenCandidate.id, oldParticipantsThatAreReachable, newParticipantsThatAreReachable, chosenCandidate.info.rank, myStateInfo.ballot)
					Trace.debug(s"Decision: result=$result, myStateInfo=$myStateInfo, answers=${howAreYouAnswers.mkString("[", ", ", "]")}, among=$borrame, config=$backingConfigChange") //TODO the "borrame" part
					result
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

		/**
		 * Asks the [[Configuration.allOtherParticipants]] how they are ([[ClusterParticipant.howAreYou]]) in a coalesced manner: If an equivalent question is in flight, reuses the same pending [[sequencer.LatchingDuty]] of the in-flight question; otherwise, a new request is done.
		 * Supports the forcing of answers.
		 *
		 * @param participantsIds the [[ParticipantId]]s of the target participants.
		 * @param stateInfo the [[StateInfo]] to put in the inquires.
		 * @param forcedAnswerByParticipantId the forced answers indexed by [[ParticipantId]].
		 * @return An [[IndexedSeq]] containing a [[sequencer.LatchingTask]] for each [[ParticipantId]] in the provided array. Each [[sequencer.LatchingTask]] element is the one returned by [[ClusterParticipant.howAreYou]] applied to the corresponding [[ParticipantId]] in the provided array, except the corresponding to the provided `idOfExcludedParticipant`, which yield the provided [[StateInfo]].
		 */
		private def askHowOtherParticipantsAre(participantsIds: IArray[ParticipantId], stateInfo: StateInfo, forcedAnswerByParticipantId: java.util.Map[ParticipantId, StateInfo]): IArray[sequencer.LatchingTask[StateInfo]] = {
			participantsIds.mapWithIndex { (participantId, _) =>
				forcedAnswerByParticipantId.get(participantId) match {
					case null => coalescedHowAreYou.getOrStart((participantId, stateInfo), true)
					case forcedAnswer: StateInfo => sequencer.LatchingTask_ready(Success(forcedAnswer))
				}
			}
		}

		private final def illegalStateQuiesce()(using Trace.Context): Role = {
			Trace.step("illegalStateQuiesce") {
				val failure = new IllegalStateException("Should never happen")
				Trace.error(s"Should never happen", failure)
				become(Quiesced(Failure(failure)))
			}
		}

		/**
		 * The information about a participant that counts for leader elections.
		 */
		private final class CandidateInfo(val id: ParticipantId, val isInOldConfig: Boolean, val info: StateInfo) {
			/** @return the winner of the competition between this candidate and the other candidate when competing for leadership. The winner is the more up-to-date one.
			 * The more up-to-date criteria are: greater current term, is leading over not, is eligible over not, greater commit-index  term, greater commit-index, and lesser [[ParticipantId]], with the left to right precedence.
			 */
			def getWinnerAgainst(other: CandidateInfo): CandidateInfo = {
				if this.info.currentTerm > other.info.currentTerm then this
				else if this.info.currentTerm < other.info.currentTerm then other
				else if this.info.termAtCommitIndex > other.info.termAtCommitIndex then this
				else if this.info.termAtCommitIndex < other.info.termAtCommitIndex then other
				else if this.info.commitIndex > other.info.commitIndex then this
				else if this.info.commitIndex < other.info.commitIndex then other
				else if this.info.rank == ER_LEADING && other.info.rank != ER_LEADING then this
				else if this.info.rank != ER_LEADING && other.info.rank == ER_LEADING then other
				else if this.info.rank == ER_CANDIDATE && other.info.rank != ER_CANDIDATE then this
				else if this.info.rank != ER_CANDIDATE && other.info.rank == ER_CANDIDATE then other
				else if this.isInOldConfig && !other.isInOldConfig then this
				else if !this.isInOldConfig && other.isInOldConfig then other
				else if this.id < other.id then this
				else other
			}

			override def toString: String =
				s"CandidateInfo(participant=$id, currentTerm:${info.currentTerm}, rank=${ElectionRank_nameOf(info.rank)}, termAtCommitIndex=${info.termAtCommitIndex}, commitIndex=${info.commitIndex}, ballot=${info.ballot}, isInOldConfig=$isInOldConfig)"
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

		/** @param notificator a function that receives a [[NotificationListener]] and calls one of its methods. */
		private inline def notifyListeners(inline notificator: NotificationListener => Unit)(using Trace.Context): Unit = {
			checkWithin()
			notificationListeners.forEach { (listener, _) =>
				try notificator(listener)
				catch {
					case NonFatal(e) => Trace.error(s"$boundParticipantId: the listener $listener threw an exception while handling $notificator", e)
				}
			}
		}

		//// Just for efficiency ////

		@threadUnsafe private lazy val _emptyLatchingDuty: sequencer.LatchingDuty[Maybe[AnyRef]] = sequencer.LatchingDuty_ready(Maybe.empty)

		/** An already completed [[sequencer.LatchingDuty]] that yields [[Maybe.empty]].
		 * CAUTION: This @threadUnsafe lazy val does not guarantee a unique instance under concurrent access. Its use is only safe for logic that depends on the value's data, not its object identity (eq/ne). */
		private inline final def emptyLatchedDuty[A]: sequencer.LatchingDuty[Maybe[A]] = _emptyLatchingDuty.asInstanceOf[sequencer.LatchingDuty[Maybe[A]]]

		/** $suppressSyntheticCompanionObject */
		private inline final def Leader(trap: Nothing): Any = trap

		/** $suppressSyntheticCompanionObject */
		private inline final def CandidateInfo(trap: Nothing): Any = trap

		/** $suppressSyntheticCompanionObject */
		private inline final def StableConfig(trap: Nothing): Any = trap

		/** $suppressSyntheticCompanionObject */
		private inline final def TransitionalConfig(trap: Nothing): Any = trap

		/** $suppressSyntheticCompanionObject */
		private inline final def Accessible(trap: Nothing): Any = trap

		/** $suppressSyntheticCompanionObject */
		private inline def RetirementDriver(trap: Nothing): Any = trap
	}
}
