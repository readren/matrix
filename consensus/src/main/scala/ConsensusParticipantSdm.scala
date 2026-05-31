package readren.consensus

import readren.common.*
import readren.common.Trace.Context
import readren.sequencer.{CausalFence, CoalescedQuery, Doer, ResultIncrementalCoalescing}

import java.util
import java.util.Comparator
import scala.annotation.{threadUnsafe, publicInBinary}
import scala.collection.immutable.{ArraySeq, ListSet, StringOps}
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
	class ASK_THE_LEADER(val leaderId: AnyRef, override val latestBallotSeen: Ballot) extends ConfigChangeResponse {
		override def toString: String = deriveToString[ASK_THE_LEADER](this)
	}

	/** The participant is catching-up because it is joining. */
	class CATCHING_UP(override val latestBallotSeen: Ballot) extends ConfigChangeResponse {
		override def toString: String = deriveToString[CATCHING_UP](this)
	}

	/** The tracking of the [[Configuration]] change request was lost due to a leader change after the first phase was started. The process may complete or not depending on which participant is promoted. If completed, the [[ConsensusParticipantSdm.ClusterParticipant.onActiveConfigChanged]] is called. If not, just silence. // TODO avoid the mentioned silence. */
	class REQUEST_TRACKING_LOST_AFTER_FIRST_PHASE_STARTED(override val latestBallotSeen: Ballot) extends ConfigChangeResponse {
		override def toString: String = deriveToString[REQUEST_TRACKING_LOST_AFTER_FIRST_PHASE_STARTED](this)
	}

	/** The tracking of the [[Configuration]] change request was lost due to a leader change after the first phase was committed (replicated to majority). The process will continue provided the system is sufficiently incited by client commands or further configuration change requests. Listen to [[ConsensusParticipantSdm.ClusterParticipant.onActiveConfigChanged]] calls to observe when the process completes. */
	class REQUEST_TRACKING_LOST_AFTER_FIRST_PHASE_COMMITED(override val latestBallotSeen: Ballot) extends ConfigChangeResponse {
		override def toString: String = deriveToString[REQUEST_TRACKING_LOST_AFTER_FIRST_PHASE_COMMITED](this)
	}

	/** The tracking of the [[Configuration]] change request was lost due to a leader change after the second phase was started. The process will continue anyway provided the system is sufficiently incited by client commands or further configuration change requests. Listen to [[ConsensusParticipantSdm.ClusterParticipant.onActiveConfigChanged]] calls to observe when the process completes. */
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
	 * @param reachableCommonCount The number of reachable and viable participatns (including the voter itself) in the common set. The common set is [[ConfigChange.newParticipants]] if the voter active configuration is stable, and [[ConfigChange.oldParticipants]] if the voter active configuration is transitional.
	 * @param reachableTargetCount The number of reachable and viable participatns (including the voter itself) in the target set. The target set is the empty set if the voter active configuration is stable, and [[ConfigChange.newParticipants]] if the voter active configuration is transitional.
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
	 * @param lastRecordTerm The [[Term]] of the last [[Record]] in the log.
	 * @param lastRecordIndex The [[RecordIndex]] of the latt [[Record]] in the log.
	 * @param configIndex The index of the active [[ConfigChange]].
	 * @param ballot the election round to which this [[StateInfo]] belongs to.
	 * TODO add something that changes when the active configuration changes, like its index.
	 */
	final case class StateInfo(currentTerm: Term, rank: ElectionRank, termAtCommitIndex: Term, commitIndex: RecordIndex, lastRecordTerm: Term, lastRecordIndex: RecordIndex, configIndex: RecordIndex, ballot: Ballot) {
		if assertionsEnabled then assert(currentTerm >= termAtCommitIndex)

		/** @return true if this and the other istance are equal ignoring the [[ballot]]. */
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

	val MAX_RECURSION_DEPTH: Int = 99
	val MAX_PERMIT_QUIESCENCE_RETRIES: Int = 9

	def retiringParticipantMaxRetries: Int = 9

	/** Maximum number of log entries to retain before triggering compaction.
	 * Once the log exceeds this size and the commitIndex is sufficiently advanced, entries up to the highest applied command index are discarded and a snapshot is taken. */
	def logCompactionThreshold: Int = 1000

	/** Determines how many [[Redords]] to retain in the log when a compaction is fired. */
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
		 * @return a [[sequencer.LatchingTask]] that yields the [[StateMachineResponse]]
		 */
		def applyClientCommand(index: RecordIndex, command: ClientCommand): sequencer.LatchingTask[StateMachineResponse]

		/** Returns a [[sequencer.Task]] that yields the [[RecordIndex]] most recently passed to [[applyClientCommand]] whose corresponding [[sequencer.LatchingTask]] is completed.\
		 * If the implementation cannot determine this index or prefers to relay on the [[Workspace]]'s log, it should return zero. That instructs the [[ConsensusParticipant]] to install the [[Workspace]]'s latest snapshot and replay all the commands in its log.\
		 * This method is invoked only during recovery after restarts or persistence failures.
		 */
		def recoverIndexOfLastAppliedCommand: sequencer.LatchingTask[RecordIndex]

		/** Creates a snapshot of the state machine state at the moment of the call.\
		 * The implementation should support calls to [[StateMachine.applyClientCommand]] while this method is running, keeping the result invariant.\
		 * This method is called when the log exceeds the compaction threshold and all entries up to the highest applied command index have been applied.\
		 * @return a [[sequencer.LatchingTask]] that yields the serialized state machine state. */
		def takeSnapshot(): sequencer.LatchingTask[IArray[Byte]]

		/** Installs a snapshot received from the leader, replacing the current state machine state.\
		 * @param data the serialized state machine state.
		 * @return a [[sequencer.LatchingTask]] that completes when the snapshot has been installed. */
		def installSnapshot(data: IArray[Byte]): sequencer.LatchingVenture[Unit]
	}

	//// RESPONSE TO CLIENT

	/** The response to a client command.
	 * @see [[ClusterParticipant.Delegate.onCommandFromClient]]. */
	sealed trait ResponseToClient

	/** The command was appended to a majority of the participants persistent logs, and applied to the leader's [[StateMachine]] which responded with the specified [[content]]. */
	final case class Processed(content: StateMachineResponse) extends ResponseToClient

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
		 * @param wakeupsDone the number of calls to this method done before the current one to delay the next attempt.
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
			def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.LatchingTask[ResponseToClient]

			/** **Inbound bridge**: This method is invoked by this [[ClusterParticipant]] when another participant calls [[howAreYou]] on the [[ParticipantId]] of the owner of this [[Delegate]]
			 *
			 * Must be called within the [[sequencer]].
			 * @param inquirerId The id of the participant that called [[howAreYou]].
			 * @param inquirerInfo The [[StateInfo]] of the participant that called [[howAreYou]].
			 * @return The state information of the destination participant.
			 */
			def onHowAreYou(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.LatchingTask[StateInfo]

			/** **Inbound bridge**: This method is invoked by this [[ClusterParticipant]] when another participant calls [[chooseALeader]] on the [[ParticipantId]] of the owner of this [[Delegate]]
			 *
			 * Must be called within the [[sequencer]].
			 * @param inquirerId The id of the participant that called [[chooseALeader]].
			 * @param inquirerInfo Information about the state of the participant that called.
			 * @return A [[sequencer.Venture]] that yields a [[Vote]] indicating the candidate chosen by the listening participant for the specified term.
			 */
			def onChooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.LatchingTask[Vote[ParticipantId]]

			/** **Inbound bridge**: This method is invoked by this [[ClusterParticipant]] when another participant calls [[appendRecords]] on the [[ParticipantId]] of the owner of this [[Delegate]]
			 *
			 * Must be called within the [[sequencer]].
			 * @param inquirerId The id of the participant that called [[appendRecords]].
			 * @param inquirerTerm The term of the participant that called [[appendRecords]].
			 * @param prevRecordIndex The index of record after which the specified `records` should be appended.
			 * @param prevRecordTerm The term of the record after which the specified `records` should be appended.
			 * @param batch The records to append.
			 * @param leaderCommit The index of the highest log entry known to be committed (replicated to a majority) according to the inquirer.
			 * @return A [[sequencer.Venture]] that yields the result of the append operation.
			 */
			def onAppendRecords(inquirerId: ParticipantId, inquirerTerm: Term, prevRecordIndex: RecordIndex, prevRecordTerm: Term, batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.LatchingTask[AppendResult]

			/** **Inbound bridge**: Handles an InstallSnapshot RPC from the leader.
			 * Invoked when the leader has discarded log entries that this follower needs.
			 *
			 * Must be called within the [[sequencer]].
			 * @param inquirerId the leader's participant ID.
			 * @param inquirerTerm the leader's current term.
			 * @param snapshot the snapshot data including state machine state and metadata.
			 * @return A [[sequencer.LatchingTask]] that yields a rejecting [[AppendResult]] equivalent to the one that [[onAppendRecords]] would return when asks for earlier records starting from [[SnapshotData.lastIncludedRecordIndex]] + 1. */
			def onInstallSnapshot(inquirerId: ParticipantId, inquirerTerm: Term, snapshot: SnapshotData[ParticipantId], batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.LatchingTask[AppendResult]

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
			 *			- the returned [[sequencer.LatchingTask]] yields either [[SUCCESSFULLY_CHANGED]] or [[ALREADY_CHANGED]] for any of the consensus-participants,
			 *			- or the [[onActiveConfigChanged]] is called in any of the consensus-participants with the provided request identifier or desired participants set.
			 *
			 * @param requestId an identifier chosen by the caller that will be propagated up to the invocations of the [[onActiveConfigChanged]] method of each of the [[ClusterParticipant]] instances bound to the involved [[ConsensusParticipant]] services.
			 * @param desiredParticipantsSet the identifiers of the participants that are going to seek consensus from now on.
			 * @param priorAnswer should contain the response to the last request done by the inquirer to this or any other participant, if any.
			 * @return a [[sequencer.Task]] that yields:
			 *         [[SUCCESSFULLY_CHANGED]] if the requested change was successfully completed.
			 *         [[ALREADY_CHANGED]] if the requested change is already done or in progress.
			 *         [[ALREADY_IN_PROGRESS]] if the participant is currently transitioning to the requested configuration 
			 *         [[ASK_THE_LEADER]] if none of the previous bullet is true and the [[ConsensusParticipant]] is a [[FOLLOWER]].
			 *         [[WAIT_PREVIOUS_CHANGE_TO_COMPLETE]] if the participant is the leader but is currently processing a change to a configuration different from the requested.  
			 *         [[STOPPED]] if the participant is not able to become neither the [[LEADER]] nor a [[FOLLOWER]]
			 *         - currently the leader or a follower that already has the desired participants set as the current or scheduled one;
			 *         - currently the leader and was able to replicate the corresponding [[TransitionalConfigChange]] to a majority according to that same [[TransitionalConfigChange]] rules. */
			def requestConfigChange(requestId: ConfigChangeRequestId, desiredParticipantsSet: Set[ParticipantId], priorAnswer: Maybe[ConfigChangeResponse]): sequencer.LatchingTask[ConfigChangeResponse]
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
			 * @return A [[sequencer.Venture]] that yields the state information of the destination participant.
			 */
			def howAreYou(inquirerInfo: StateInfo): sequencer.Venture[StateInfo] // TODO consider returning a LatchingTask instead

			/**
			 * Request the destination participant to choose a leader.
			 * The implementation should make, somehow, the destination participant's [[Delegate.onChooseALeader]] to be called, and return what it returns.
			 * Called within the [[sequencer]] thread.
			 * @param inquirerId The id of the participant that is asking.
			 * @param inquirerInfo Information about the state of the participant that is asking.
			 * @return A [[sequencer.Venture]] that yields a [[Vote]] indicating the candidate chosen by the destination participant.
			 */
			def chooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.Venture[Vote[ParticipantId]] // TODO consider returning a LatchingTask instead

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
			 * @return A [[sequencer.Venture]] that yields the result of the append operation.
			 */
			def appendRecords(inquirerTerm: Term, prevLogIndex: RecordIndex, prevLogTerm: Term, batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.Venture[AppendResult] // TODO consider returning a LatchingTask instead

			/**
			 * Sends a snapshot to the destination participant, replacing its log and state machine state. Only leaders call this method.
			 * The implementation should make, somehow, the destination participant's [[Delegate.onInstallSnapshot]] to be called with the same parameter values, and return what it returns.
			 * This method is called within the [[sequencer]].
			 * @param inquirerTerm The term of the participant that is sending the snapshot.
			 * @param snapshot the snapshot data including state machine state and metadata.
			 * @return A [[sequencer.Venture]] that yields the result of the installation operation.
			 */
			def installSnapshot(inquirerTerm: Term, snapshot: SnapshotData[ParticipantId], batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.Venture[AppendResult] // TODO consider returning a LatchingTask instead


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
			 * @return A [[sequencer.Venture]] that completes successfully if either: the permission was successfully delivered, or the participant is already in a post-retirement state ([[QUIESCED]], released, or no longer exists).
			 */
			def permitQuiescence(indexOfGrantedStableConfigChange: RecordIndex): sequencer.Venture[Unit] // TODO consider returning a LatchingTask instead
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

		/** The implementation should return the index of the [[ConfigChange]] instance with greater index in the log, or zero if none.
		 * This method is called very frequently so the implementation should strive to be efficient. */
		def indexOfLatestConfigChange: RecordIndex

		/** The implementation should returnn the latest [[ConfigChange]] in the log. */
		def latestConfigChange: Maybe[ConfigChange[ParticipantId]]

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

		/** Returns the records in the log starting at `from` and up to `until` exclusive. */
		def getRecordsBetween(from: RecordIndex, until: RecordIndex): IArray[Record] // = logBuffer.slice((from - logBufferOffset).toInt, (until - logBufferOffset).toInt)

		/** Appends a record and returns it index. */
		def appendRecord(record: Record): Unit

		/** Appends any new record not already in the log starting at the specified index.
		 * If a conflict is detected (i.e., a stored record and a new record at the same index have different terms), all stored records from that index onward are removed before appending the new records. */
		def appendResolvingConflicts(records: IArray[Record], from: RecordIndex): Unit

		/** Should be called whenever the [[ConsensusParticipant.highestAppliedCommandIndex]] changes to allow this [[Workspace]] to release the storage used to memorize the records that are pending to be applied to the [[StateMachine]]. */
		def informAppliedCommandIndex(appliedCommandIndex: RecordIndex): Unit

		/** @return the index of the last [[Record]] with [[RecordIndex]] greater or equal to a [[RecordIndex]] and with [[Term]] equal to a [[Term]]. Returns `from - 1` if none is found.
		 * The implementation may assume that the provided `from` [[RecordIndex]] is within the log buffer. */
		def indexOfLastRecordWithTerm(term: Term, from: RecordIndex): RecordIndex

		/** Replaces the whole log with the provided snapshot. */
		def resetLog(snapshot: SnapshotData[ParticipantId], tailRecords: IArray[Record]): Unit

		/** Truncates the log by replacing its earlier records (up to and including the provided index) with a snapshot of the [[StateMachine]].
		 * After this call, [[logBufferOffset]] must return `lastIncludedIndex + 1`.
		 * @param lastIncludedRecordIndex the index of the last [[Record]] applied to the [[StateMachine]] before the provided snapshot was taken.
		 * @param stateMachineSnapshot a snapshot of the [[StateMachine]]'s state.
		 * @return a [[SnapshotData]] that consolidates the [[StateMachine]]'s snapshot with the log metadata */
		def truncateLogUpTo(lastIncludedRecordIndex: RecordIndex, stateMachineSnapshot: IArray[Byte]): Unit

		/** @return the [[SnapshotData]] produced by the last call to [[truncateLogUpTo]]. */
		def latestSnapshot: Maybe[SnapshotData[ParticipantId]]

		/** The [[ConsensusParticipant]] triggers the returned [[Task]] to inform that it will not reference this instance anymore and this [[Workspace]] may be purged. */
		def releases: sequencer.Task[Unit]
	}

	/** Defines what a [[ConsensusParticipant]] requires from a persistence service to load and save its [[Workspace]].
	 * Implementations may assume that all methods of this trait are invoked within the [[sequencer]] thread, enabling optimizations such as avoiding unnecessary creation of new task objects. */
	trait Storage {
		def load: sequencer.LatchingTask[Try[WS]]

		/** Saves the workspace to the persistence storage.
		 * Design Note: A failure to save the workspace should restart the [[ConsensusParticipant]] as if it had crashed and lost all non-persistent variables.
		 */
		def save(workspace: WS): sequencer.LatchingTask[Try[Unit]]
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

		def onCommitIndexChanged(previous: RecordIndex, current: RecordIndex, as: RoleOrdinal, at: Term): Unit

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

		override def onCommitIndexChanged(previous: RecordIndex, current: RecordIndex, as: RoleOrdinal, at: Term): Unit = ()

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

		/** The [[sequencer.Venture]] returned by a call to [[ClusterParticipant.appendRecords]]. */
		private type AppendRequest = sequencer.Venture[AppendResult]

		private type AppendOutcome = Int
		private inline val AO_IS_LAGGING_MASK = 16
		private inline val AO_SUCCESS = 0
		private inline val AO_NEEDS_EARLIER_RECORDS = 1 | AO_IS_LAGGING_MASK
		private inline val AO_MISSING_BECAUSE_PARTICIPANT_WAS_NOT_PART_OF_THE_CONFIGURATION = 2 | AO_IS_LAGGING_MASK
		private inline val AO_HAS_HIGHER_TERM = 3
		private inline val AO_SKIPPED_BECAUSE_OUT_OF_CONFIGURATION = 4
		private inline val AO_IS_RETIRING = 5
		private inline val AO_IS_QUIESCED = 6
		private inline val AO_IS_UNREACHABLE = 8
		private inline val AO_UNEXPECTED_RETIRING = 9

		/** [[Accessible.tryFusingRecords]]'s fusion report: The [[PrimaryState]] is not accessible or the term is stale. */
		private inline val FR_IGNORED = 0
		/** [[Accessible.tryFusingRecords]]'s fusion report: The records were appended. */
		private inline val FR_RECORD_FUSED = 1
		/** [[Accessible.tryFusingRecords]]'s fusion report: The [[Term]] was updated. */
		private inline val FR_TERM_UPDATED = 2
		/** The snapshot was updated and the records were appended. */
		inline val FR_SNAPSHOT_UPDATED = 4
		/** The snapshot is useful but the [[StateMachine]]'s commands applier is currently running. */
		inline val FR_HAVE_TO_WAIT_COMMAND_APPLIER = 8

		/** The index of the highest entry known to be committed according to this participant.
		 * A log record is committed once the leader that created the record has replicated it on a majority of the participants.
		 * This also commits all preceding records in the leader’s log, including records created by previous leaders.
		 * CAUTION: This variable depends on the [[PrimaryState]] (as well as external states); mutations to the [[PrimaryState]] modify its value. To ensure deterministic causal ordering relative to these mutations, a [[StatefulRole]] must only access this variable within consumers synchronously subscribed to the [[sequencer.LatchingTask]] returned by [[sequencer.CausalFence.causalAnchor]] or [[sequencer.CausalFence.advance]]-like methods on the [[StatefulRole.primaryStateFence]]. This ensures the variable is read in synchronization with the specific PrimaryState mutation it depends on. */
		private var commitIndex: RecordIndex = 0

		/** The index of the [[CommandRecord]] with the highest index whose command was successfully applied to the [[StateMachine]] of this [[ConsensusParticipant]].
		 * CAUTION: This variable depends on the [[PrimaryState]] (as well as external states); mutations to the [[PrimaryState]] modify its value. To ensure deterministic causal ordering relative to these mutations, a [[StatefulRole]] must only access this variable within consumers synchronously subscribed to the [[sequencer.LatchingTask]] returned by [[sequencer.CausalFence.causalAnchor]] or [[sequencer.CausalFence.advance]]-like methods on the [[StatefulRole.primaryStateFence]]. This ensures the variable is read in synchronization with the specific PrimaryState mutation it depends on. */
		private var highestAppliedCommandIndex: RecordIndex = 0

		/** The current role of this [[ConsensusParticipant]].
		 * @note The [[currentRole]] state is neither entirely derived from the [[PrimaryState]] nor orthogonal to it. They are interrelated.
		 * CAUTION: [[PrimaryState]] mutations depend on the value of this variable. Therefore, this variable value must be in sync with the [[PrimaryState]] by means of the [[StatefulRole.primaryStateFence]] game changing invariant. */
		private var currentRole: Role = new Starting(indexOfTheIncludingConfigChange, participantsInTheIncludingConfigChange)

		private var workspaceReleasedCovenant: sequencer.LatchingTask[Unit] = sequencer.ReadyTask(())

		/** Memorizes the latest [[Configuration]] derived by the [[StatefulRole.deriveConfigurationFrom]] method.\
		 * It is initialized by [[Starting.handleEnter]] with a synthetic [[TransitionalConfig]] before transitioning to a [[StatefullRole]] and stays defined as long as the [[currentRole]] is stateful.\
		 * CAUTION: This variable depends on the [[PrimaryState]]; mutations to the [[PrimaryState]] modify its value. To ensure deterministic causal ordering relative to these mutations, a [[StatefulRole]] must only access this variable within consumers synchronously subscribed to the [[sequencer.LatchingTask]] returned by [[sequencer.CausalFence.causalAnchor]] or [[sequencer.CausalFence.advance]]-like methods on the [[StatefulRole.primaryStateFence]]. This ensures the variable is read in synchronization with the specific PrimaryState mutation it depends on. */
		private var latestDerivedConfig: Maybe[Configuration] = Maybe.empty

		/** Memory where the [[Role.onQuiescencePermitted]] method stores the [[ParticipantId]] of the last quiescence grantor. */
		private var quiescenceGrantor: Maybe[ParticipantId] = Maybe.empty
		/** Memory where the [[Role.onQuiescencePermitted]] method stores the [[RecordIndex]] of the last [[StableConfigChange]] for which quiescence was authorized. */
		private var indexOfStableConfigChangeForWhichQuiescenceWasPermitted: RecordIndex = 0
		/** Memorizes the token for the pending wake-up used to retry failed calls to [[permitQuiescence]]. Needed to be able to cancel the retry. */
		private var retryPermitQuiescenceWakeUp: Maybe[WakeUpToken] = Maybe.empty
		/** Knows the participants that are waiting for an acknowledge to the quiescence authorizations, and the corresponding [[RecordIndex]] of the [[StableConfigChange]] for which the permission granted. */
		private val nonAcknowledgedQuiescencePermissions: mutable.Map[ParticipantId, RecordIndex] = mutable.Map.empty

		/** Knows the [[RetirementDriver]]s corresponding to the participants that were excluded from the configuration and potentially have not received the appends to notice that they can leave. */
		private val retirementDriverByParticipantId: mutable.Map[ParticipantId, RetirementDriver] = mutable.Map.empty

		/** The current election round.
		 * Should be bumped whenever the part of the state of this participant that is exposed in questions to other participants (term and commitIndex as of this writing) changes.
		 * CAUTION: This variable depends on the [[PrimaryState]] (as well as external states); mutations to the [[PrimaryState]] modify its value. To ensure deterministic causal ordering relative to these mutations, a [[StatefulRole]] must only access this variable within consumers synchronously subscribed to the [[sequencer.LatchingTask]] returned by [[sequencer.CausalFence.causalAnchor]] or [[sequencer.CausalFence.advance]]-like methods on the [[StatefulRole.primaryStateFence]]. This ensures the variable is read in synchronization with the specific PrimaryState mutation it depends on. */
		private var currentBallot: Ballot = INITIAL_BALLOT

		/** Stores the last [[StateInfo]] instance returned by [[Role.syncLocalStateInfo]]
		 * CAUTION: This variable depends on the [[PrimaryState]] (as well as external states); mutations to the [[PrimaryState]] modify its value. To ensure deterministic causal ordering relative to these mutations, a [[StatefulRole]] must only access this variable within consumers synchronously subscribed to the [[sequencer.LatchingTask]] returned by [[sequencer.CausalFence.causalAnchor]] or [[sequencer.CausalFence.advance]]-like methods on the [[StatefulRole.primaryStateFence]]. This ensures the variable is read in synchronization with the specific PrimaryState mutation it depends on. */
		private var stateInfoExposedInLastInteraction: StateInfo = StateInfo(PRE_INIT, ER_NONE, PRE_INIT, 0, PRE_INIT, 0, 0, INITIAL_BALLOT)

		/** Memorizes the [[StateInfo]] of the other participants seen during the [[currentBallot]].
		 * The [[StateInfo.ballot]] field of contained instances should match the [[currentBallot]].
		 * When a [[StateInfo]] with a newer ballot is seen, this map is cleared before adding it.
		 * DO NOT FORGET TO call the appropriate method (like [[Role.syncLocalStateInfo]] or [[updateSeenStateInfo]]) to update this variable before reading it.
		 * CAUTION: This variable depends on the [[PrimaryState]] (as well as external states); mutations to the [[PrimaryState]] modify its value. To ensure deterministic causal ordering relative to these mutations, a [[StatefulRole]] must only access this variable within consumers synchronously subscribed to the [[sequencer.LatchingTask]] returned by [[sequencer.CausalFence.causalAnchor]] or [[sequencer.CausalFence.advance]]-like methods on the [[StatefulRole.primaryStateFence]]. This ensures the variable is read in synchronization with the specific PrimaryState mutation it depends on.
		 * @note uses a java map to improve efficiency.
		 * TODO make values be [[Covenant]]s of [[StateInfo]] so that received questions that include a [[StateInfo]] fulfill the howAreYou questions done by this participant. */
		private val memorizedPeersInfos: java.util.Map[ParticipantId, StateInfo] = new java.util.HashMap()

		/** CAUTION: [[PrimaryState]] mutations depend on the value of this variable. Therefore, this variable value must be in sync with the [[PrimaryState]] by means of the [[StatefulRole.primaryStateFence]] game changing invariant. */
		private var decoupledCommandsApplierCompletion: sequencer.LatchingTask[Unit] = sequencer.LatchingTask_unit

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
			sequencer.Commitment_triggerAndWire(params.otherParticipantId.howAreYou(params.stateInfo))
		)

		{
			initialListeners.foreach(notificationListeners.put(_, None))
			cluster.setBound(currentRole)
			Trace.init(() => s"$boundParticipantId: Ctor") {
				currentRole.handleEnter(currentRole)
			}
		}

		/** @return the ordinal of the current behavior. */
		def getRoleOrdinal: RoleOrdinal = currentRole.ordinal

		/** @return a [[sequencer.Task]] that quiesces this [[ConsensusParticipant]] instance. */
		def quiesces: sequencer.Task[Unit] = {
			Trace.init(() => s"$boundParticipantId: quiesces") {
				sequencer.Task_mineFlat { () =>
					val quiesced = Quiesced(Success("This ConsensusParticipant instance was forcefully quiesced."))
					become(quiesced).asInstanceOf[Quiesced].completed
				}
			}
		}

		/** @return a [[sequencer.Task]] that quiesces and disposes this [[ConsensusParticipant]] instance. */
		def disposes: sequencer.Task[Unit] = {
			quiesces.andThen { _ =>
				notificationListeners.clear()
				cluster.removeBound()
			}
		}

		/** Synchronously transitions this [[ConsensusParticipant]]'s [[Role]] to the provided one if defined. */
		private def become(maybeNewRole: Maybe[Role])(using Trace.Context): Role = Trace.step("become") {
			checkWithin()
			maybeNewRole.foreach { newRole =>
				currentRole.handleExit(newRole)
				val previousRole = currentRole
				val committedTerm = currentRole.getCommittedPrimaryState.currentTerm
				notifyListeners(_.onRoleLeft(previousRole.ordinal, committedTerm))
				currentRole = newRole
				cluster.setBound(newRole)
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

			final def blankVote(term: Term, ballot: Ballot): Vote[ParticipantId] = Vote(term, boundParticipantId, 0, 0, this.rank, ballot)

			final def yieldsBlankVote(term: Term, ballot: Ballot): sequencer.LatchingTask[Vote[ParticipantId]] = sequencer.LatchingTask_ready(blankVote(term, ballot))

			/** Called by [[become]] after the previous [[Role]]'s [[Role.handleExit]] method has returned, and the [[currentRole]] variable set to this [[Role]] instance.
			 * This method is suitable to enqueue primary state updates that must happen before any updates enqueued after [[become]] returns. */
			def handleEnter(previous: Role)(using Trace.Context): Unit

			/** Called by [[become]] before transitioning to another role. */
			def handleExit(newRole: Role): Unit = ()

			/** Updates the derived state that is stored in the [[Role]] instance and depends on the current [[Configuration]]. Only the [[Leader]] role has such state as this writing. */
			def handleActiveConfigChange(currentPrimaryState: Accessible, currentConfig: Configuration, newConfig: Configuration, indexOfNewConfigChange: RecordIndex)(using Context): Unit = ()

			def getCommittedPrimaryState: PrimaryState =
				Inaccessible

			/** Returns a [[StateInfo]] that reflects the provided [[PrimaryState]], the [[commitIndex]], the [[rank]], and the [[currentBallot]] of the bound participant; and, if the returned value differs from the one returned in the previous call (stored in [[stateInfoExposedInLastInteraction]]), bumps the [[currentBallot]] and clears the [[memorizedPeersInfos]]. */
			def syncLocalStateInfo(primaryState: PrimaryState)(using Trace.Context): StateInfo

			final def updateLocalStateInfo(primaryState: PrimaryState, seenParticipantId: ParticipantId, seenStateInfo: StateInfo)(using Trace.Context): StateInfo = {
				var updatedStateInfo = syncLocalStateInfo(primaryState)
				if updateSeenStateInfo(updatedStateInfo, seenParticipantId, seenStateInfo) then updatedStateInfo = syncLocalStateInfo(primaryState)
				updatedStateInfo
			}

			/** Returns a [[StateInfo]] that indicates disability to participate; and, if the returned value differs from [[stateInfoExposedInLastInteraction]], bumps the [[currentBallot]] and clears the [[memorizedPeersInfos]]. */
			protected final def buildIneligibleInfo(term: Term): StateInfo = {
				val rememberedInfo = stateInfoExposedInLastInteraction
				val newInfo =
					if rememberedInfo.tiesWith(term, this.rank, PRE_INIT, 0, PRE_INIT, 0, 0) then {
						if rememberedInfo.ballot == currentBallot then rememberedInfo else StateInfo(term, this.rank, PRE_INIT, 0, PRE_INIT, 0, 0, currentBallot)
					} else {
						currentBallot = currentBallot.bumped
						memorizedPeersInfos.clear()
						StateInfo(term, this.rank, PRE_INIT, 0, PRE_INIT, 0, 0, currentBallot)
					}
				stateInfoExposedInLastInteraction = newInfo
				newInfo
			}

			/** Asks the other participants how they are and decides which should be the leader based on their answers.\
			 * @note This process updates the [[PrimaryState.currentTerm]] if a later one is seen, which may cause a [[currentRole]] update.
			 * @param primaryState0 the current local [[PrimaryState]]
			 * @param currentStateInfo the current local [[StateInfo]]
			 * @param blankVoteIfRoleChanges instructs to yield a [[blankVote]] if the [[currentRole]] is changed by other process before this process completes.
			 * @return A [[sequencer.LatchingTask]] that yields a [[Vote]] with the chosen leader. */
			def determineMyVote(primaryState0: PrimaryState, currentStateInfo: StateInfo, blankVoteIfRoleChanges: Boolean)(using Context): sequencer.LatchingTask[Vote[ParticipantId]]

			/** Must be called before transitioning to [[Retiring]] to handle the special case when the active [[Configuration]] in an empty [[StableConfig]].\
			 * The [[Leader]] role should start the process that authorizes others to transition to the terminal [[QUIESCED]] state.\
			 * "Vanished" means the new config has zero participants. In that case there will be no successor leader to authorize quiescence, so the current (ghost) leader must do it itself before retiring.\
			 * @param config The currently active [[Configuration]]. */
			def authorizeQuiescenceIfVanished(config: StableConfig)(using Trace.Context): Unit = ()
		}

		/** Partial implementation of the [[Role]]s that accesses the [[PrimaryState]] of the bound participant.
		 * @param primaryStateFence the [[CausalFence]] that must be used to ensure causal ordering of the state updates. It must be propagated to subsequent [[StatefulRole]] instances. */
		private abstract class StatefulRole(val primaryStateFence: CausalFence[PrimaryState, sequencer.type]) extends Role {

			private type TermRef = IntRef
			/** The default argument for the [[updateTermIfLessThan]] method's second parameter.\
			 * It is private and defined in the same class as the [[primaryStateFence]] to ensure that the contained [[Term]] variable reflects the expected value provided it is read within the synchronous part of a synchronously subscribed consumer to the [[sequencer.LatchingTask]] returned by [[updateTermIfLessThan]]. See the game-changing-invariant in [[Doer.CausalFence]]. */
			protected final val defaultPreviousTermRef: TermRef = new TermRef(0)

			override def handleExit(newRole: Role): Unit = {
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

			// TODO consider adding a parameter with the activeConfigChangeIndex.
			override final def syncLocalStateInfo(primaryState: PrimaryState)(using Trace.Context): StateInfo = Trace.step("syncLocalStateInfo") {
				primaryState match {
					case Inaccessible =>
						buildIneligibleInfo(primaryState.currentTerm)

					case accessible: Accessible =>
						val rememberedInfo = stateInfoExposedInLastInteraction
						val termAtCommitIndex = accessible.getRecordTermAt(commitIndex)
						val lastRecordIndex = accessible.firstEmptyRecordIndex - 1
						val lastRecordTerm = accessible.getRecordTermAt(lastRecordIndex)
						val activeConfigChangeIndex = deriveConfigurationFrom(accessible).changeIndex
						val newInfo =
							if rememberedInfo.tiesWith(primaryState.currentTerm, this.rank, termAtCommitIndex, commitIndex, lastRecordTerm, lastRecordIndex, activeConfigChangeIndex) then {
								if rememberedInfo.ballot == currentBallot then rememberedInfo else StateInfo(accessible.currentTerm, this.rank, termAtCommitIndex, commitIndex, lastRecordTerm, lastRecordIndex, activeConfigChangeIndex, currentBallot)
							} else {
								currentBallot = currentBallot.bumped
								memorizedPeersInfos.clear()
								StateInfo(accessible.currentTerm, this.rank, termAtCommitIndex, commitIndex, lastRecordTerm, lastRecordIndex, activeConfigChangeIndex, currentBallot)
							}
						stateInfoExposedInLastInteraction = newInfo
						if assertionsEnabled then assert(newInfo.rank != ER_NONE)
						newInfo
				}
			}

			override def determineMyVote(primaryState0: PrimaryState, stateInfo0a: StateInfo, blankVoteIfRoleChanges: Boolean)(using Context): sequencer.LatchingTask[Vote[ParticipantId]] = {
				Trace.step("determineMyVote") {
					primaryState0 match {
						case Inaccessible =>
							yieldsBlankVote(primaryState0.currentTerm, stateInfo0a.ballot)

						case accessible0: Accessible =>
							val config0 = deriveConfigurationFrom(accessible0)
							// Yield a blank vote if this participant is not included in the current configuration.
							if !config0.isBoundIncluded then yieldsBlankVote(accessible0.currentTerm, stateInfo0a.ballot)
							else {
								// Update the local commitIndex to the highest peer's commitIndex, provided its term at that index is consistent with the local log. If the commitIndex is updated, refresh the local StateInfo.
								val stateInfo0b = if absorbHigherCommitIndexFromPeers(accessible0, stateInfo0a) then syncLocalStateInfo(accessible0) else stateInfo0a
								// Create the howAreYou questions
								val howAreYouQuestions0 = askHowOtherParticipantsAre(config0.peers, stateInfo0b, memorizedPeersInfos)
								// determine my vote based on the answers to the howAreYou questions
								for {
									howAreYouAnswers0 <- sequencer.LatchingTask_sequenceVenturesToArray(howAreYouQuestions0, true)
									primaryState1 <- {
										val highestTermSeen = IArray.unsafeFromArray(howAreYouAnswers0).foldLeftWithIndex(accessible0.currentTerm) { (latestTermSeen, answer, _) =>
											answer match {
												case Success(info) => if info.currentTerm > latestTermSeen then info.currentTerm else latestTermSeen
												case _: Failure[StateInfo] => latestTermSeen
											}
										}
										updateTermIfLessThan(highestTermSeen) // Note that this may change the role
									}
									myVote <- {
										var stateInfo1 = currentRole.syncLocalStateInfo(primaryState1)
										// Update the memorizedPeersInfos (by filling the missing entries with the StateInfo instances in the answers), and count the failed answers.
										val numberOfFailedAnswers = {
											IArray.unsafeFromArray(howAreYouAnswers0).foldLeftWithIndex(0) { (failedAnswersCounter, answer, participantIndex) =>
												val participantId = config0.peers(participantIndex)
												answer match {
													case Success(info) =>
														if updateSeenStateInfo(stateInfo1, participantId, info) then stateInfo1 = currentRole.syncLocalStateInfo(primaryState1)
														failedAnswersCounter

													case _: Failure[StateInfo] =>
														if memorizedPeersInfos.containsKey(participantId) then failedAnswersCounter
														else failedAnswersCounter + 1
												}
											}
										}

										if blankVoteIfRoleChanges && currentRole != this then currentRole.yieldsBlankVote(primaryState1.currentTerm, stateInfo1.ballot)
										else primaryState1 match {
											case accessible1: Accessible =>
												// Update the local commitIndex to the highest peer's commitIndex, provided its term at that index is consistent with the local log. If the commitIndex is updated, restart.
												if absorbHigherCommitIndexFromPeers(accessible1, stateInfo1) then determineMyVote(accessible1, syncLocalStateInfo(accessible1), blankVoteIfRoleChanges)
												else {
													val config1 = deriveConfigurationFrom(accessible1)
													// if the bound participant is included, then:
													if config1.isBoundIncluded || (currentRole.isInstanceOf[Leader] && currentRole.asInstanceOf[Leader].isGhost) then {
														// If either, the active configuration changed while waiting the responses to the howAreYou questions, or a successful answer has an obsolete ballot; then ignore this `determineMyVote` execution replacing it with a new fresh one.
														if (config1 ne config0) || memorizedPeersInfos.size + numberOfFailedAnswers < config0.peers.length then {
															Trace.trace(s"Restarting my vote determination due to ${if config1 ne config0 then s"a concurrent configuration change (${config0.changeIndex}->${config1.changeIndex})" else s"an obsolete answer, currentBallot=$currentBallot, memorizedInfosSize=${memorizedPeersInfos.size}, numberOfFailedAnswers=$numberOfFailedAnswers, numberOfRequests=${config1.peers.size}"}")
															// TODO analyze if memorizedPeersInfos should be cleared here.
															currentRole.determineMyVote(accessible1, stateInfo1, blankVoteIfRoleChanges)
														}
														// else, decide the vote based on the `StateInfo` stored in the `memorizedPeersInfos`.
														else sequencer.LatchingTask_ready(
															config1.decideMyVote(stateInfo1, memorizedPeersInfosToArray(config1))
																.fold(currentRole.blankVote(stateInfo1.currentTerm, stateInfo1.ballot))(identity)
														)
													}
													// If the bound participant is excluded, then yield a blank vote.
													else yieldsBlankVote(accessible1.currentTerm, stateInfo1.ballot)
												}

											case Inaccessible =>
												currentRole.yieldsBlankVote(primaryState1.currentTerm, stateInfo1.ballot)
										}
									}
								} yield myVote

							}
					}
				}
			}

			/** Advances the local [[commitIndex]] as far as the peers provided the [[Term]] of the local [[Record]] at the peer's [[StateInfo.commitIndex]] matches the peer's [[StateInfo.termAtCommitIndex]].\
			 * This advancement safety is guaranteed by Raft's Log Matching Property.
			 * @param primaryState the current [[PrimaryState]]
			 * @param stateInfo the current [[StateInfo]]. Not referenced in the body but present as parameter to require [[memorizedPeersInfos]] be up-to-date. */
			protected final def absorbHigherCommitIndexFromPeers(primaryState: Accessible, stateInfo: StateInfo)(using Trace.Context): Boolean = {
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

			override def onHowAreYou(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.LatchingTask[StateInfo] = {
				Trace.init(() => s"$boundParticipantId: onHowAreYou") {
					checkWithin()
					for {
						primaryState1 <- updateTermIfLessThan(inquirerInfo.currentTerm) // Note that this may change the role.
						response <- {
							if currentRole ne this then currentRole.onHowAreYou(inquirerId, inquirerInfo)
							else sequencer.LatchingTask_ready(updateLocalStateInfo(primaryState1, inquirerId, inquirerInfo))
						}
					} yield response
				}
			}

			override def onChooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.LatchingTask[Vote[ParticipantId]] = {
				Trace.init(() => s"$boundParticipantId: onChooseALeader") {
					checkWithin()
					val term0Ref = new TermRef(0)
					for {
						// if the term is stale, update it persistently before interacting with other participants so that they see this participant with its updated and persisted state.
						primaryState1 <- updateTermIfLessThan(inquirerInfo.currentTerm, term0Ref) // Note that this may change the role.
						myVote <- {
							if currentRole ne this then currentRole.onChooseALeader(inquirerId, inquirerInfo)
							else {
								val currentStateInfo = updateLocalStateInfo(primaryState1, inquirerId, inquirerInfo)
								currentRole.determineMyVote(primaryState1, currentStateInfo, false)
							}
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
			 * @param inquirerId ID of the leader sending the AppendEntries request
			 * @param inquirerTerm Term of the leader
			 * @param prevRecordIndex Index of the record preceding the new entries
			 * @param prevRecordTerm Term of the preceding record
			 * @param batch The batch of records to append
			 * @param leaderCommit       Commit index reported by the leader
			 * @return a [[sequencer.LatchingTask]] yielding the [[AppendResult]] with:
			 *  - the `success` field with true if, and only if, all the following are true when the appending was processed (specifically, when this participant's `primaryStateFence` was crossed):
			 *    * the [[PrimaryState]] is valid;
			 *    * `inquirerTerm >= currentTerm`;
			 *    * the role is either ISOLATED or FOLLOWER;
			 *    * the term of the log record at `prevRecordIndex` is equal to `prevRecordTerm`;
			 *  - the `term` field with `max(inquirerTerm, currentTerm)`.
			 *  - the `roleOrdinal` field tells the current role of this participant. */
			override def onAppendRecords(inquirerId: ParticipantId, inquirerTerm: Term, prevRecordIndex: RecordIndex, prevRecordTerm: Term, batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.LatchingTask[AppendResult] = {
				Trace.init(() => s"$boundParticipantId: onAppendRecords") {
					checkWithin()
					Trace.trace(s"onAppendRecords($inquirerId, @$inquirerTerm, $prevRecordIndex, $prevRecordTerm, ${batch.mkString("[", ", ", "]")}, $leaderCommit, $termAtLeaderCommit) called")

					val fusionReport = new IntRef(FR_IGNORED)
					for {
						primaryState1 <- primaryStateFence.advanceIf {
							case Inaccessible =>
								Maybe.empty

							case accessible0: Accessible =>
								val currentTerm = accessible0.currentTerm
								// If the appending is not allowed (either the inquirer term is stale, the current role isn't stateful, or the this participant is and will continue leading); then do not mutate the primary state.
								if inquirerTerm < currentTerm || (currentRole ne this) || (currentRole.rank == ER_LEADING && inquirerTerm == currentTerm) then Maybe.empty
								// Else (if inquirerTerm >= currentTerm && currentRole.isInstanceOf[StatefulRole] && (currentRole.rank != ER_LEADING || inquirerTerm > currentTerm)), do the appending.
								else accessible0.tryFusingRecords(inquirerTerm, prevRecordIndex, prevRecordTerm, batch, fusionReport)

						}
						response <- {
							if currentRole ne this then currentRole.onAppendRecords(inquirerId, inquirerTerm, prevRecordIndex, prevRecordTerm, batch, leaderCommit, termAtLeaderCommit)
							else handleAppendOutcome(primaryState1, inquirerId, inquirerTerm, Maybe.empty, prevRecordIndex, prevRecordTerm, batch, leaderCommit, termAtLeaderCommit, fusionReport.elem)
						}
					} yield response
				}
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
			override def onInstallSnapshot(inquirerId: ParticipantId, inquirerTerm: Term, snapshot: SnapshotData[ParticipantId], batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.LatchingTask[AppendResult] = {
				Trace.init(() => s"$boundParticipantId: onInstallSnapshot") { // TODO consider new parameters
					checkWithin()
					Trace.trace(s"onInstallSnapshot(inquirerId=$inquirerId, inquirerTerm=$inquirerTerm, $snapshot, ${batch.mkString("[", ", ", "]")}, leaderCommit=$leaderCommit, termAtCommitIndex=$termAtLeaderCommit) called")
					val fusionReport = new IntRef(FR_IGNORED)
					for {
						primaryState1 <- primaryStateFence.advanceIf {
							case Inaccessible =>
								Maybe.empty

							case accessible0: Accessible =>
								val currentTerm = accessible0.currentTerm
								val currentRole = thisConsensusParticipant.currentRole
								// If the appending is not allowed (either the inquirer term is stale, the current role isn't stateful, or the this participant is and will continue leading); then do not mutate the primary state.
								if inquirerTerm < currentTerm || (currentRole ne this) || (currentRole.rank == ER_LEADING && inquirerTerm == currentTerm) then Maybe.empty
								// If the received snapshot is older than what we already have in the local log, then fusion the batch records only.
								else if snapshot.lastIncludedRecordIndex <= accessible0.indexOfLastRecordWithTerm(inquirerTerm, commitIndex) then {
									accessible0.tryFusingRecords(inquirerTerm, snapshot.lastIncludedRecordIndex, snapshot.lastIncludedRecordTerm, batch, fusionReport)
								}
								// The following `if` breaks determinism unless the commands applier completion is externally synchronized with the primary state mutations.
								// If the snapshot is useful but the commands applier is running:
								else if decoupledCommandsApplierCompletion.isPending then {
									if inquirerTerm > currentTerm then {
										fusionReport.elem = FR_HAVE_TO_WAIT_COMMAND_APPLIER | FR_TERM_UPDATED
										Maybe(accessible0.withTermUpdated(inquirerTerm))
									} else {
										fusionReport.elem = FR_HAVE_TO_WAIT_COMMAND_APPLIER
										Maybe.empty
									}
								}
								// Else, update the snapshot, truncate the log, update the term, and append the tail records.
								else {
									fusionReport.elem = if inquirerTerm > currentTerm then FR_SNAPSHOT_UPDATED | FR_RECORD_FUSED | FR_TERM_UPDATED else FR_SNAPSHOT_UPDATED | FR_RECORD_FUSED
									Maybe(accessible0.withLogReplaced(inquirerTerm, snapshot, batch))
								}
						}
						response <- {
							if currentRole ne this then currentRole.onInstallSnapshot(inquirerId, inquirerTerm, snapshot, batch, leaderCommit, termAtLeaderCommit)
							else handleAppendOutcome(primaryState1, inquirerId, inquirerTerm, Maybe(snapshot), snapshot.lastIncludedRecordIndex, snapshot.lastIncludedRecordTerm, batch, leaderCommit, termAtLeaderCommit, fusionReport.elem)
						}
					} yield response
				}
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
			 * @param fusionReport     A bitmask of outcome flags (e.g., [[FR_RECORD_FUSED]], [[FR_TERM_UPDATED]], [[FR_SNAPSHOT_UPDATED]]) describing what happened during the primary state update.
			 * @return A [[sequencer.LatchingTask]] yielding the [[AppendResult]] to be sent back to the leader. */
			private def handleAppendOutcome(primaryState1: PrimaryState, inquirerId: ParticipantId, inquirerTerm: Term, maybeSnapshot: Maybe[SnapshotData[ParticipantId]], prevRecordIndex: RecordIndex, prevRecordTerm: Term, batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term, fusionReport: Int)(using Trace.Context): sequencer.LatchingTask[AppendResult] = {
				assert(primaryState1 eq primaryStateFence.committedState)
				primaryState1 match {
					case Inaccessible =>
						illegalStateQuiesce()
						sequencer.LatchingTask_ready(AppendResult(PRE_INIT, Long.MaxValue, this.ordinal))

					case accessible1: Accessible =>
						// If the local snapshot was updated or a record was fused.
						if (fusionReport & FR_RECORD_FUSED) != 0 then {
							val previousCommitIndex = commitIndex
							val newCommitIndex = if leaderCommit < accessible1.firstEmptyRecordIndex then leaderCommit else accessible1.firstEmptyRecordIndex - 1
							// If the commitIndex is bumped, update it.
							if newCommitIndex > previousCommitIndex then commitIndex = newCommitIndex
							// Derive the active configuration from the updated primary state and commitIndex.
							val config1 = deriveConfigurationFrom(accessible1)
							// Update the currentRole:
							val cro = currentRole.ordinal
							// If not joining or the catching-up is complete then:
							if cro != JOINING || (accessible1.firstEmptyRecordIndex > currentRole.asInstanceOf[Joining].indexOfTheIncludingConfigChange) then {
								// If this participant belongs to the active configuration, then become a Follower or Isolated, depending on whether the inquirer belongs to the active configuration or not.
								if config1.isBoundIncluded then {
									// Become follower of the inquirer if it belongs to the active configuration.
									if config1.peers.contains(inquirerId) then become(Follower(accessible1.currentTerm, inquirerId, primaryStateFence))
									// Become isolated if this participant is joining, the catching-up is complete, and the inquirer is not in the active configuration.
									else if cro == JOINING then become(Isolated(primaryStateFence))
									// Keep the current role otherwise.
									// Note that, if the inquirer is excluded and the current role is follower of an excluded participant, the role is not changed to isolated here because it might be following a ghost leader.
								}
								// If this participant is excluded and ...
								else config1 match {
									case stable: StableConfig => // ... the active configuration is stable, then become Retiring.
										this.authorizeQuiescenceIfVanished(stable)
										become(Retiring(accessible1.currentTerm, stable.term, stable.changeIndex, stable.electorate))

									case transitional: TransitionalConfig => // ... the active configuration is transitional, then something is wrong.
										illegalStateQuiesce(s"$inquirerId=$inquirerId, inquirerTerm=$inquirerTerm, prevRecordIndex=$prevRecordIndex, batch=$batch, leaderCommit=$leaderCommit, termAtLeaderCommit=$termAtLeaderCommit, fusionReport=$fusionReport")
									// if this.ordinal != JOINING || accessible1.firstEmptyRecordIndex > currentRole.asInstanceOf[Joining].indexOfTheIncludingConfigChange then become(Isolated(primaryStateFence))
								}
							}
							if newCommitIndex > previousCommitIndex then {
								// Notify the commitIndex bump.
								notifyListeners(_.onCommitIndexChanged(previousCommitIndex, newCommitIndex, currentRole.ordinal, accessible1.currentTerm))
								// Start the "apply committed commands" process if it isn't already started.
								if decoupledCommandsApplierCompletion.isCompleted && currentRole.isInstanceOf[StatefulRole] then startApplyingCommittedCommands(accessible1, (fusionReport & FR_SNAPSHOT_UPDATED) != 0)
							}
							sequencer.LatchingTask_ready(AppendResult(accessible1.currentTerm, 0L, currentRole.ordinal))
						}
						// If the snapshot was not updated nor a record was fused, then:
						else {
							// If the term was updated and the current role is sensible to term updates, then update the role.
							if (fusionReport & FR_TERM_UPDATED) != 0 then this.onTermUpdated(accessible1, Maybe(inquirerId))

							// If the received snapshot is useful but not installed because the commands-applier is running, then wait it to finish and then restart. // TODO Is it necessary to signal the commands-applier to stop because whatever it is doing will be discarded?
							if (fusionReport & FR_HAVE_TO_WAIT_COMMAND_APPLIER) != 0 then {
								decoupledCommandsApplierCompletion.flatMap { _ =>
									maybeSnapshot.fold(currentRole.onAppendRecords(inquirerId, inquirerTerm, prevRecordIndex, prevRecordTerm, batch, leaderCommit, termAtLeaderCommit)) { snapshot =>
										currentRole.onInstallSnapshot(inquirerId, inquirerTerm, snapshot, batch, leaderCommit, termAtLeaderCommit)
									}
								}
							}
							// Else, surely either the inquirer's term is stale, the terms at prevRecordIndex do not match, the batch fully predates the local latest snapshot, or the cluster is in an illegal state with two leaders. So, respond with a rejection asking for earlier records, pointing to the first record that is missig or we known the term does not match.
							else {
								// This point is reached if either the inquirer's term is stale, earlier records are needed, the terms at prevRecordIndex do not match, the batch fully predates the local latest snapshot, or the cluster is in an illegal state with two leaders. So, respond with a rejection asking for earlier records, pointing to the first record that is missig or we known the term does not match.
								val successOrIndexForNextAttempt: RecordIndex =
									if accessible1.firstEmptyRecordIndex < prevRecordIndex then accessible1.firstEmptyRecordIndex // This happens when no snapshot was received and earlier records are needed. Tell the leader to start from the local log's first empty index.
									else if prevRecordIndex + batch.length < accessible1.logBufferOffset - 1L then accessible1.firstEmptyRecordIndex // This happens when the batch fully predates the local latest snapshot. Suggesting firstEmptyRecordIndex helps the inquirer to jump to a verifiable point.
									else if prevRecordIndex > 0L then prevRecordIndex // This happens when the terms do not match.
									else 1L // This happens when the terms do not match at the first record of the log.
								sequencer.LatchingTask_ready(AppendResult(accessible1.currentTerm, successOrIndexForNextAttempt, currentRole.ordinal))
							}
						}
				}
			}


			/** Applies committed [[CommandRecord]]s silently and in a decoupled manner until reaching [[commitIndex]].\
			 * This method assumes that the [[PrimaryState]]'s log is not turncated while this method is running: log truncation must wait until the [[decoupledCommandsApplierCompletion]] is fulfilled.\
			 * CAUTION: This process may synchronously advance the [[primaryStateFence]] and therofore will cause causal safety assertions like `primaryState0 eq primaryStateFence.committedState` to fail. This problem can be avoided moving the call to this method after the check line, preferably to the end of the block that uses a [[PrimaryState]] instance.
			 * @param primaryState0 the current [[PrimaryState]].
			 * @param mustInstallSnapshot instructs to reset the [[StateMachine]]' state from the latest snapshot in the current [[Workspace]]. */
			protected def startApplyingCommittedCommands(primaryState0: Accessible, mustInstallSnapshot: Boolean)(using Trace.Context): Unit = {
				assert(decoupledCommandsApplierCompletion.isCompleted)

				val completionCovenant = sequencer.Covenant[Unit]()
				decoupledCommandsApplierCompletion = completionCovenant

				def applyBehind(primaryState1: Accessible): Unit = {
					applyCommittedCommands(primaryState1, Long.MaxValue, 0).subscribe { _ =>
						// If log compaction is needed, compact it in a decoupled way.
						if highestAppliedCommandIndex - primaryState1.logBufferOffset > logCompactionThreshold && currentRole.isInstanceOf[StatefulRole] then startLogCompaction()
						completionCovenant.fulfillUnsafe(())
					}
				}

				/** Resets the [[StateMachine]]' state from the latest snapshot in the log, and updates the [[highestAppliedCommandIndex]] accordingly.\
				 * Also notifies about the command application to the [[PrimaryState]] observers. */
				def startFromSnapshot(snapshotData: SnapshotData[ParticipantId]): Unit = {
					for {
						_ <- machine.installSnapshot(snapshotData.stateMachineSnapshot)
						primaryState1 <- {
							highestAppliedCommandIndex = snapshotData.lastIncludedRecordIndex
							primaryStateFence.causalAnchor()
						}
					} do primaryState1 match {
						case Inaccessible =>
							Trace.debug("The committed commands applier stopped after installing a snapshot because the primary state is inaccessible.")
							completionCovenant.fulfillUnsafe(())
						case accessible1: Accessible =>
							accessible1.informAppliedCommandIndex(snapshotData.lastIncludedRecordIndex)
							applyBehind(accessible1)
					}
				}

				// If instructed to install the latest snapshot or the `highestAppliedCommandIndex` predates the latest snapshot, then reset the state machine's state with it and start applying the commands after the snapshot point.
				if mustInstallSnapshot || primaryState0.latestSnapshot.exists(_.lastIncludedRecordIndex > highestAppliedCommandIndex) then startFromSnapshot(primaryState0.latestSnapshot.get)
				// Else, if we are not starting from scratch (a command was applied before), then start applying the commands after the last applied one.
				else if highestAppliedCommandIndex > 0 then applyBehind(primaryState0)
				// Else, if we are starting from scratch or recovering from a crash, then ask the state machine if it knows the index of the last applied command and then refresh the primary state.
				else for {
					index <- machine.recoverIndexOfLastAppliedCommand
					primaryState1 <- {
						highestAppliedCommandIndex = index
						primaryStateFence.causalAnchor()
					}
				} do primaryState1 match {
					case Inaccessible =>
						Trace.debug(s"The commited commands applier stopped during recovery because the primary state is inaccessible.")
						completionCovenant.fulfillUnsafe(())
					case accessible1: Accessible =>
						// If the state machine knows the index of the last applied command, then start applying the commands after it.
						if index > 0 then {
							accessible1.informAppliedCommandIndex(index)
							applyBehind(accessible1)
						}
						// Else, replay the commands from the log. If a snapshot exists, install it on the state machine and start applying the commands that are after the snapshot point. Otherwise, start applying the commands from the very beginning of the log.
						else {
							accessible1.latestSnapshot.fold {
								highestAppliedCommandIndex = 0
								applyBehind(accessible1)
							}(startFromSnapshot)
						}
				}
			}

			/** Applies to the [[StateMachine]] the already commited but still not applied commands whose index is lower or equal to the provided bound.\
			 * They are applied one after the other as long as the [[currentRole]] is statefull, assuming the log isn't truncated while this method is running.
			 * @param primaryState any reference to an [[Accessible]] instance produced by [[primaryStateFence]]. It is not necessary it be the current, causally anchored one. It is used to read committed records, which don't mutate.
			 * @param upTo the upper inclusive bound of [[RecordIndex]] to apply, together with [[commitIndex]]. */
			def applyCommittedCommands(primaryState: Accessible, upTo: RecordIndex, recursionDepth: Int): sequencer.LatchingTask[Unit] = {
				val indexOfCommandToApply = highestAppliedCommandIndex + 1
				if indexOfCommandToApply > upTo || indexOfCommandToApply > commitIndex then sequencer.LatchingTask_unit
				else {
					primaryState.getRecordAt(indexOfCommandToApply) match {
						case command: CommandRecord[ClientCommand] @unchecked =>
							val previousExecutionSerial = sequencer.currentExecutionSerial
							for {
								_ <- machine.applyClientCommand(indexOfCommandToApply, command.command)
								_ <- {
									highestAppliedCommandIndex = indexOfCommandToApply
									// It is not necessary to have an updated primary state here because committed records are never mutated and we are not mutating the primary state here. We only need to know if the current role is statefull.
									if currentRole.isInstanceOf[StatefulRole] then {
										primaryState.informAppliedCommandIndex(indexOfCommandToApply)

										if sequencer.currentExecutionSerial != previousExecutionSerial then applyCommittedCommands(primaryState, upTo, 0)
										else if recursionDepth < MAX_RECURSION_DEPTH then applyCommittedCommands(primaryState, upTo, recursionDepth + 1)
										else sequencer.Covenant_mineFlat(() => applyCommittedCommands(primaryState, upTo, 0))
									} else sequencer.LatchingTask_unit
								}
							} yield ()
						case _ =>
							highestAppliedCommandIndex = indexOfCommandToApply
							primaryState.informAppliedCommandIndex(indexOfCommandToApply)
							applyCommittedCommands(primaryState, upTo, recursionDepth + 1)
					}
				}
			}

			/** Starts a process that compacts the log assuming the [[RecordIndex]] of last [[Record]] applied to the [[machine]] is the provided one.
			 * Assumes the [[machine]] supports calls to [[StateMachine.applyClientCommand]] while [[StateMachine.takeSnapshot]] is running.
			 * @return a [[sequencer.LatchingTask]] that yields the current [[PrimaryState]] with the log truncated. */
			protected final def startLogCompaction()(using Trace.Context): sequencer.LatchingTask[PrimaryState] = {
				val lastIncludedIndex = highestAppliedCommandIndex - logRetentionAfterSnapshot
				for {
					snapshot <- machine.takeSnapshot()
					primaryState1 <- primaryStateFence.advanceIf {
						case Inaccessible =>
							Maybe.empty
						case accessible1: Accessible =>
							Maybe(accessible1.withLogTruncated(accessible1.currentTerm, lastIncludedIndex, snapshot))
					}
				} yield {
					Trace.trace(s"$boundParticipantId: Compacted log up to index $lastIncludedIndex (term ${primaryState1.currentTerm}).")
					primaryState1
				}
			}

			/** @inheritdoc
			 * Wait in line for the [[PrimaryState]] and then delegate the request to the concrete stateful role. */
			override final def requestConfigChange(requestId: ConfigChangeRequestId, desiredParticipants: Set[ParticipantId], priorAnswer: Maybe[ConfigChangeResponse]): sequencer.LatchingTask[ConfigChangeResponse] = {
				Trace.init(() => s"$boundParticipantId: requestConfigChange-$requestId") {
					Trace.trace(s"Handling change to $desiredParticipants, priorAnswer=$priorAnswer.")
					for {
						primaryState <- primaryStateFence.causalAnchor()
						response <- {
							// If a prior answer is provided, update the ballot and memorizedPeersInfos
							val ballotWasUpdated = priorAnswer.fold(false) { priorResponse =>
								updateBallotIfLowerThan(currentRole.syncLocalStateInfo(primaryState), priorResponse.latestBallotSeen)
							}
							// Delegate the request to the concrete stateful role.
							currentRole match {
								case stateful: StatefulRole =>
									primaryState match {
										case Inaccessible =>
											illegalStateQuiesce()
											sequencer.LatchingTask_ready(STOPPED(currentRole.syncLocalStateInfo(primaryState).ballot))
										case accessible: Accessible =>
											stateful.requestConfigChange(accessible, requestId, desiredParticipants, ballotWasUpdated)
									}
								case stateless =>
									stateless.requestConfigChange(requestId, desiredParticipants, priorAnswer)
							}
						}
					} yield {
						Trace.trace(s"response: $response")
						response
					}
				}
			}

			def requestConfigChange(primaryState: Accessible, requestId: ConfigChangeRequestId, desiredParticipants: Set[ParticipantId], ballotWasUpdated: Boolean)(using Context): sequencer.LatchingTask[ConfigChangeResponse]

			override def onQuiescencePermitted(grantorId: ParticipantId, indexOfGrantedStableConfigChange: RecordIndex): Unit = {
				quiescenceGrantor = Maybe(grantorId)
				indexOfStableConfigChangeForWhichQuiescenceWasPermitted = indexOfGrantedStableConfigChange
			}

			//// Role updaters


			/** Starts a process that updates the [[currentRole]] and [[PrimaryState.currentTerm]] based on the [[StateInfo]]s returned by calling [[ClusterParticipant.howAreYou]] on the other participants and, if necessary, also based on the [[Vote]]s returned by calling [[ClusterParticipant.chooseALeader]] on them.
			 * This process always ends immediately after a call to [[become]] returns. So, its [[Role]] outcome can be seen in the [[currentRole]] derived state variable.
			 * The [[currentRole]] is updated only if the desired one if not equivalent to the [[currentRole]]. If updated, any other in-flight [[updateRole]] process is canceled and immediately completed.
			 * Concurrent executions of this method return the same result. */
			def updateRole()(using Trace.Context): sequencer.LatchingTask[Unit] = {
				if assertionsEnabled then assert(currentRole eq this)
				for {
					primaryState <- primaryStateFence.causalAnchor()
					_ <- {
						if currentRole ne this then sequencer.LatchingTask_unit
						else updateRole(primaryState)
					}
				} yield ()
			}


			/** Like [[updateRole]] but already knowing the current [[PrimaryState]]. */
			def updateRole(primaryState0: PrimaryState)(using Context): sequencer.LatchingTask[Unit] = {
				checkWithin()
				if assertionsEnabled then assert(currentRole eq this)

				// Identify this execution.
				serialOfLastUpdateRoleExecution += 1
				val serial = serialOfLastUpdateRoleExecution

				Trace.step(() => s"updateRole#$serial") {

					/** Checks if this specific execution of `updateRole` has become obsolete and must be aborted. This condition is met if the participant has transitioned to a different role or if a newer concurrent execution of `updateRole` has started. */
					inline def haveToAbort: Boolean = (currentRole ne this) || incumbentUpdateRoleSerial != serial

					/** Role decision logic when my vote is for a peer and got the [[StateInfo]] of a majority. */
					def whenVotingAnother(currentState: Accessible, vote: Vote[ParticipantId]): sequencer.LatchingTask[Unit] = {
						if vote.votedRank == ER_LEADING then {
							become(Follower(currentState.currentTerm, vote.votedId, primaryStateFence))
							sequencer.LatchingTask_unit
						}
						// If the voted participant isn't retiring, become Isolated.
						else if vote.votedRank != ER_RETIREE then {
							become(Isolated(primaryStateFence))
							sequencer.LatchingTask_unit
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
								sequencer.LatchingTask_unit
							}
						}
					}

					/** Continue the role update process assuming my vote is non-blank. */
					def updateRoleKnowingMyNonBlankVote(currentState2: Accessible, config2: Configuration, myVote2: Vote[ParticipantId]): sequencer.LatchingTask[Unit] = {
						if assertionsEnabled then assert(myVote2.term == currentState2.currentTerm)

						// If excluded and not leading as ghost, then retire immediately.
						if !config2.isBoundIncluded && !this.isInstanceOf[Leader] then {
							this.authorizeQuiescenceIfVanished(config2.asInstanceOf[StableConfig]) // The downcast is safe because exclusion is checked every record and transitional configurations are never more restrictive than the contiguos stable ones.
							become(Retiring(currentState2.currentTerm, config2.term, config2.changeIndex, config2.electorate))
							sequencer.LatchingTask_unit
						}
						// If got the StateInfo of all the active participants, then decide the vote omnisciently.
						else if config2.reachedAll(myVote2) then {
							if myVote2.votedId == boundParticipantId then {
								become(Promoting(currentState2.currentTerm, primaryStateFence))
								sequencer.LatchingTask_unit
							} else whenVotingAnother(currentState2, myVote2)
						}
						// else, if got the StateInfo of a majority of the active participants, then:
						else if config2.reachedAMajority(myVote2) then {
							// If my vote is for other participant, become follower or isolated depending on the other is leading or not.
							if myVote2.votedId != boundParticipantId then whenVotingAnother(currentState2, myVote2)
							// If my vote is for myself and I am leading, abort the role update.
							else if this.ordinal >= PROMOTING then sequencer.LatchingTask_unit
							// If the vote is for myself and I am not leading, decide based on everyone’s votes.
							else {
								val myStateInfoAtChooseALeaderRequest = syncLocalStateInfo(currentState2)
								val inquires = for replierId <- config2.peers yield replierId.chooseALeader(boundParticipantId, myStateInfoAtChooseALeaderRequest)
								for {
									replies <- sequencer.LatchingTask_sequenceVenturesToArray(inquires, true)
									primaryState3 <- {
										val latestTermSeen = IArray.unsafeFromArray(replies).foldLeftWithIndex(currentState2.currentTerm)((latestTermSeen, reply, _) => reply match {
											case Success(replierVote) => if replierVote.term > latestTermSeen then replierVote.term else latestTermSeen
											case _: Failure[Vote[ParticipantId]] => latestTermSeen
										})
										Trace.trace(s"Replied votes=${replies.zip(config2.peers).mkString("[", ", ", "]")}, latestTermSeen=$latestTermSeen, myVote=$myVote2") // TODO delete line
										updateTermIfLessThan(latestTermSeen) // Note that this may change the role.
									}
									_ <- {
										if haveToAbort then sequencer.LatchingTask_unit
										else primaryState3 match {
											case Inaccessible =>
												illegalStateQuiesce()
												sequencer.LatchingTask_unit

											case accessible3: Accessible =>
												// If the term was bumped (while waiting the votes from the other participants or due to a higher term seen in them), then the role update is responsibility of `StatefulRole.onTermBumped`; so abort this update. Restarting the role update here might collide with role changes caused by the bump.
												if accessible3.currentTerm > currentState2.currentTerm then {
													if assertionsEnabled then assert(this.ordinal < PROMOTING) // because while leading the term should never change.
													sequencer.LatchingTask_unit
												} else {
													val myStateInfo3 = syncLocalStateInfo(accessible3)
													val highestBallotSeenInVotes = IArray.unsafeFromArray(replies).foldLeftWithIndex(myStateInfo3.ballot)((highestBallot, reply, _) => reply match {
														case Success(replierVote) => if replierVote.ballot laterThan highestBallot then replierVote.ballot else highestBallot
														case _: Failure[Vote[ParticipantId]] => highestBallot
													})
													val aHigherBallotHaveBeenSeenInVotes = updateBallotIfLowerThan(myStateInfo3, highestBallotSeenInVotes)
													if aHigherBallotHaveBeenSeenInVotes || myStateInfo3.ballot != myStateInfoAtChooseALeaderRequest.ballot then {
														// TODO consider the inclusion of the StateInfo in Vote in order to keep the StateInfo instances with the highest ballot seen. This would save howAreYou calls to participants for which the StateInfo in the Vote already corresponds to the new ballot. Note that this safe would occur only when restarting the role update due to a higher ballot seen in votes.
														Trace.trace(s"Restarting due ${if aHigherBallotHaveBeenSeenInVotes then "a higher ballot seen in votes" else "to a ballot bump"}.")
														updateRole(accessible3)
													} else {
														val config3 = deriveConfigurationFrom(accessible3)
														// TODO make decideMyVote support the commitIndex-auto-bump like the `updateRoleOmnisciently` if possible
														val myVote3 = config3.decideMyVote(syncLocalStateInfo(accessible3), memorizedPeersInfosToArray(config3))
															.fold(blankVote(myStateInfo3.currentTerm, myStateInfo3.ballot))(identity)
														become(config3.determineRole(accessible3, primaryStateFence, myVote3, replies))
														sequencer.LatchingTask_unit
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
							sequencer.LatchingTask_unit
						}
					}

					/** Continue the role update process by treating blank vote cases. */
					def udateRoleKnowingMyVote(accessible2: Accessible, myVote: Vote[ParticipantId]): sequencer.LatchingTask[Unit] = {
						val config2 = deriveConfigurationFrom(accessible2)

						// If my vote is blank, then:
						if myVote.isBlank then {
							// if we are included, then:
							if config2.isBoundIncluded then {
								// Advance our commitIndex by absorbing it from a more complete peer and, if successful, restart the role update. This is necessary again here because to handle the situation when a concurrent RPC (such as onHowAreYou or onChooseALeader from another peer) updates memorizedPeersInfos with a higher commit index after determineMyVote has returned but before primaryState2 is causally anchored.
								if absorbHigherCommitIndexFromPeers(accessible2, syncLocalStateInfo(accessible2)) then updateRole(accessible2)
								// else become Isolated.
								else {
									become(Isolated(primaryStateFence))
									sequencer.LatchingTask_unit
								}
							}
							// if we are not included, the become Retiring.
							else {
								if assertionsEnabled then assert(config2.isInstanceOf[StableConfig]) // because exclusion is checked every record and transitional configurations are never more restrictive than the contiguos stable ones.
								become(Retiring(accessible2.currentTerm, config2.term, config2.changeIndex, config2.electorate))
								sequencer.LatchingTask_unit
							}
						}
						// if my vote is non-blank...
						else {
							val stateInfo2 = syncLocalStateInfo(accessible2)
							// ... and no StateInfo has changed, continue the role update knowing the vote is non blank.
							if stateInfo2.ballot == myVote.ballot then updateRoleKnowingMyNonBlankVote(accessible2, config2, myVote)
							// else start the role process again (superseding this execution).
							else {
								Trace.trace(s"Restarting due to a ballot bump: currentBallot=${stateInfo2.ballot}, myVote.ballot=${myVote.ballot}")
								updateRole(accessible2)
							}
						}
					}

					/** Starts a role update process by decicing the local vote. */
					def start(primaryState1: PrimaryState): sequencer.LatchingTask[Unit] = {
						incumbentUpdateRoleSerial = serial
						memorizedPeersInfos.clear()
						if currentRole ne this then sequencer.LatchingTask_unit
						else {
							val myStateInfo1 = syncLocalStateInfo(primaryState1)
							for {
								myVote <- determineMyVote(primaryState1, myStateInfo1, true)
								_ <- {
									if haveToAbort then sequencer.LatchingTask_unit
									else for {
										primaryState2 <- primaryStateFence.causalAnchor()
										_ <- {
											if haveToAbort then sequencer.LatchingTask_unit
											else primaryState2 match {
												case Inaccessible =>
													illegalStateQuiesce()
													sequencer.LatchingTask_unit

												case accessible2: Accessible =>
													udateRoleKnowingMyVote(accessible2, myVote)
											}
										}
									} yield ()
								}
							} yield ()
						}
					}

					// Coalesce the result of concurrent calls either, superseding ongoing executions started with obsolete StateInfo, or merging to the execution started with the same StateInfo.
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

			/** Updates the [[Role]] of this [[ConsensusParticipant]] and then returns the [[sequencer.LatchingTask]] returned by the [[Role.onCommandFromClient]] method applied to the updated [[Role]].
			 * @return a [[sequencer.Venture]] returned by [[Role.onCommandFromClient]] applied to the updated [[Role]] */
			final def updateRoleAndThenCallsOnCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag)(using Context): sequencer.LatchingTask[ResponseToClient] = {
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
										for primaryState <- stateful.primaryStateFence.causalAnchor() yield primaryState match {
											case accessible: Accessible =>
												Unable(nextAttemptFlag, deriveConfigurationFrom(accessible).otherProbableParticipants)
											case Inaccessible =>
												illegalStateQuiesce()
												Unable(nextAttemptFlag, cluster.getOtherProbableParticipants)
										}
									case retiring: Retiring =>
										sequencer.LatchingTask_ready(Unable(
											nextAttemptFlag,
											ListSet.newBuilder[ParticipantId].addAll(retiring.excludingConfigElectorate).addAll(cluster.getOtherProbableParticipants).result()
										))
									case stateless =>
										sequencer.LatchingTask_ready(Unable(nextAttemptFlag, cluster.getOtherProbableParticipants))
								}
							}

						}
					} yield result
				}
			}


			/** Derives the active [[Configuration]] state from the current [[PrimaryState]] and [[commitIndex]].
			 * Depends on, and updates, the [[latestDerivedConfig]]. Also updates other derived state.
			 *
			 * CAUTION: the provided [[PrimaryState]] instance must be the current one. So, this method must be called only within the synchronous part of consumers subscribed synchronously to the [[sequencer.LatchingTask]] returned by either [[sequencer.CausalFence.advance]]-like or [[sequencer.CausalFence.causalAnchor]] methods, passing the [[PrimaryState]] provided to the consumer. This requirement is needed becase this method's side effects update derived state.
			 *  @note Accessing the current [[Configuration]] through this method ensures that the current [[Configuration]] is updated before any other derived-state update that depend on it.
			 * @param currentPrimaryState the current [[PrimaryState]].
			 * @return a [[Configuration]] derived from the provided [[Accessible]]. */
			def deriveConfigurationFrom(currentPrimaryState: Accessible)(using Context): Configuration = {
				assert(currentPrimaryState eq primaryStateFence.committedState) // Fails if the primaryStateFence was touched after yielding the PrimaryState instance received as parameter.

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
			 * @note About the safety of reusing the same [[TermRef]] instance for different calls: The value is guaranteed to reflect the expected value provided it is read within the synchronous part of a synchronously subscribed consumer to the [[sequencer.LatchingTask]] returned by [[updateTermIfLessThan]]. See the game-changing-invariant in [[Doer.CausalFence]]. */
			protected def updateTermIfLessThan(seenTerm: Term, previousTermRef: TermRef = defaultPreviousTermRef)(using Trace.Context): sequencer.LatchingTask[PrimaryState] =
				Trace.step("updateTermIfLessThan") {
					for primaryState1 <- primaryStateFence.advanceIf { (primaryState0: PrimaryState) => 
						previousTermRef.elem = primaryState0.currentTerm
						if seenTerm > primaryState0.currentTerm then primaryState0 match {
							case accessible0: Accessible => Maybe(accessible0.withTermUpdated(seenTerm))
							case Inaccessible => Maybe.empty
						}
						else Maybe.empty
					} yield if primaryState1.currentTerm > previousTermRef.elem then onTermUpdated(primaryState1, Maybe.empty) else primaryState1
				}

			private def memorizedPeersInfosToArray(currentConfig: Configuration): IArray[StateInfo] = {
				currentConfig.peers.mapWithIndex { (peerId, _) => memorizedPeersInfos.get(peerId) }
			}

			/** Called by [[updateTermIfLessThan]] and [[onInstallSnapshot]] when the [[PrimaryState.currentTerm]] is updated because a higher term was observed.\
			 * @param primaryState the current [[PrimaryState]]
			 * @param maybeLeaderId the identifier of the leader, if known. */
			def onTermUpdated(primaryState: PrimaryState, maybeLeaderId: Maybe[ParticipantId])(using Context): PrimaryState = primaryState
		}

		/** A terminal [[Role]] that indicates this [[ConsensusParticipant]] service will be ready to be disposed after all the in-flight RPC calls it did have been heard.
		 *
		 * Taken when either:
		 *		- the bound participant has been excluded from the participant and completed its retirement.
		 *		- this [[ConsensusParticipant]] service was forcibly quiesced by executing the [[sequencer.Task]] returned by the [[quiesces]] method.
		 *		- an ineludible failure occurred. */
		private final class Quiesced(val motive: Try[String]) extends Role {
			override val ordinal: RoleOrdinal = QUIESCED
			override val rank: ElectionRank = ElectionRank_from(QUIESCED)

			/** A [[sequencer.LatchingTask]] that is fulfilled when the all the allocated [[Workspace]]s are released. */
			def completed: sequencer.LatchingTask[Unit] = workspaceReleasedCovenant

			override def handleEnter(previousRole: Role)(using Context): Unit = {
				Trace.step("Quiesced.onEnter") {
					notifyListeners(_.onBecameQuiesced(previousRole.ordinal, previousRole.getCommittedPrimaryState.currentTerm, motive))
					retirementDriverByParticipantId.clear()
					retryPermitQuiescenceWakeUp.foreach(_.cancel())
					retryPermitQuiescenceWakeUp = Maybe.empty
					nonAcknowledgedQuiescencePermissions.clear()
					cluster.onQuiesced(motive)
				}
			}

			override def syncLocalStateInfo(primaryState: PrimaryState)(using Context): StateInfo =
				buildIneligibleInfo(PRE_INIT)

			override def determineMyVote(primaryState0: PrimaryState, currentStateInfo: StateInfo, blankVoteIfRoleChanges: Boolean)(using Context): sequencer.LatchingTask[Vote[ParticipantId]] =
				yieldsBlankVote(PRE_INIT, currentStateInfo.ballot)

			override def onHowAreYou(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.LatchingTask[StateInfo] = {
				checkWithin()
				updateSeenStateInfo(buildIneligibleInfo(PRE_INIT), inquirerId, inquirerInfo)
				sequencer.LatchingTask_ready(buildIneligibleInfo(PRE_INIT))
			}

			override def onChooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.LatchingTask[Vote[ParticipantId]] = {
				checkWithin()
				var myStateInfo = buildIneligibleInfo(PRE_INIT)
				if updateSeenStateInfo(myStateInfo, inquirerId, inquirerInfo) then myStateInfo = buildIneligibleInfo(PRE_INIT)
				yieldsBlankVote(PRE_INIT, myStateInfo.ballot)
			}

			override def onAppendRecords(inquirerId: ParticipantId, inquirerTerm: Term, prevRecordIndex: RecordIndex, prevRecordTerm: Term, batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.LatchingTask[AppendResult] = {
				checkWithin()
				sequencer.LatchingTask_ready(AppendResult(PRE_INIT, Long.MaxValue, ordinal))
			}

			override def onInstallSnapshot(inquirerId: ParticipantId, inquirerTerm: Term, snapshot: SnapshotData[ParticipantId], batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.LatchingTask[AppendResult] = {
				checkWithin()
				sequencer.LatchingTask_ready(AppendResult(PRE_INIT, Long.MaxValue, ordinal))
			}

			/** @inheritdoc
			 * This implementation responds with a rejection that propagates the received `attemptFlag` or-ing the [[FALLBACK]] bit to alert the participant with which the client would try next.
			 * Why the [[FALLBACK]] bit? Because the behavior of a [[Quiesced]] and a non-existent participant should be similar, given [[Quiesced]] is just a transient state before becoming inexistent. */
			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.LatchingTask[ResponseToClient] = {
				checkWithin()
				sequencer.LatchingTask_ready(Unable(attemptFlag.withInternalBitsCleared | FALLBACK, cluster.getOtherProbableParticipants))
			}

			override def requestConfigChange(requestId: ConfigChangeRequestId, desiredParticipantsSet: Set[ParticipantId], priorAnswer: Maybe[ConfigChangeResponse]): sequencer.LatchingTask[ConfigChangeResponse] = {
				var myCurrentStateInfo = buildIneligibleInfo(PRE_INIT)
				// If a prior answer is provided, update the current ballot and memorizedPeersInfos
				priorAnswer.foreach { priorResponse =>
					if updateBallotIfLowerThan(myCurrentStateInfo, priorResponse.latestBallotSeen) then myCurrentStateInfo = buildIneligibleInfo(PRE_INIT)
				}
				sequencer.LatchingTask_ready(STOPPED(myCurrentStateInfo.ballot))
			}

			override def onQuiescencePermitted(grantorId: ParticipantId, indexOfGrantedStableConfigChange: RecordIndex): Unit = ()
		}

		private final def Quiesced(motive: Try[String]): Maybe[Quiesced] = {
			if currentRole.ordinal == QUIESCED then Maybe.empty
			else Maybe(new Quiesced(motive))
		}

		/** A transitional [[Role]] before [[Quiesced]] to which a participant transitions to when a [[StableConfigChange]] that excludes it becomes active.\
		 * The life of this [[Role]] last until a stable [[Leader]] of a subsequent [[Term]] authorizes this participant to quiesce.\
		 * This [[Role]] is part of the **Retiring Quorum-Buffering** mechanism.\
		 * The purpose of this mechanism is to maintain the quorum safety of the old participant set during joint consensus.\
		 * By holding excluded participants in the [[RETIRING]] role, the system ensures they contribute toward the old set's quorum. Although they do not cast a specific vote, they effectively lower the required threshold of active votes by one, acting as a neutral "don't care" participant until a succeeding leader (líder sucesor) establishes a stable majority in the new configuration.\
		 * Since this role must exist for that reason, we also take advantage of its presence to wait for the [[RetirementDriver]]s to conclude their job. In this scenario, the job of the [[RetirementDriver]]s of this retiring ex-leader will overlap with the job of the [[RetirementDriver]]s of the succeeding [[Leader]], but, if I am not mistaken, this overlap is more beneficial than harmful because it removes some burden to the new [[Leader]].\
		 * @param finalTerm the [[Term]] during which this participant became [[Retiring]]. Used only as argument for the [[NotificationListener.onRetiring]] method, and [[AppendResult]] responses.
		 * @param termAtExcludingConfigIndex the [[Term]] of the [[StableConfigChange]] that excluded this participant causing its retirement. This is the term that a [[Retiring]] participant exposes in [[StateInfo]] during elections.
		 * @param excludingConfigIndex the index of the [[StableConfigChange]] that excluded this participant causing its retirement.
		 * @param excludingConfigElectorate The electorate of the [[StableConfigChange]] that excluded this participant. */
		private final class Retiring(val finalTerm: Term, val termAtExcludingConfigIndex: Term, val excludingConfigIndex: RecordIndex, val excludingConfigElectorate: IArray[ParticipantId]) extends Role {
			override val ordinal: RoleOrdinal = RETIRING
			override val rank: ElectionRank = ElectionRank_from(RETIRING)

			if assertionsEnabled then assert(!excludingConfigElectorate.contains(boundParticipantId))

			override def handleEnter(previous: Role)(using Trace.Context): Unit = {
				notifyListeners(_.onRetiring(previous.ordinal, finalTerm))
				becomeQuiescedIfEligible(excludingConfigIndex)
			}

			override def syncLocalStateInfo(primaryState: PrimaryState)(using Context): StateInfo =
				syncLocalStateInfo()

			private def syncLocalStateInfo(): StateInfo = {
				if stateInfoExposedInLastInteraction.tiesWith(termAtExcludingConfigIndex, rank, termAtExcludingConfigIndex, excludingConfigIndex, termAtExcludingConfigIndex, excludingConfigIndex, excludingConfigIndex) then {
					if stateInfoExposedInLastInteraction.ballot != currentBallot then stateInfoExposedInLastInteraction = StateInfo(termAtExcludingConfigIndex, rank, termAtExcludingConfigIndex, excludingConfigIndex, termAtExcludingConfigIndex, excludingConfigIndex, excludingConfigIndex, currentBallot)
				} else {
					currentBallot = currentBallot.bumped
					stateInfoExposedInLastInteraction = StateInfo(termAtExcludingConfigIndex, rank, termAtExcludingConfigIndex, excludingConfigIndex, termAtExcludingConfigIndex, excludingConfigIndex, excludingConfigIndex, currentBallot)
				}
				stateInfoExposedInLastInteraction
			}

			override def determineMyVote(primaryState0: PrimaryState, currentStateInfo: StateInfo, blankVoteIfRoleChanges: Boolean)(using Context): sequencer.LatchingTask[Vote[ParticipantId]] = {
				yieldsBlankVote(termAtExcludingConfigIndex, currentStateInfo.ballot)
			}

			override def onHowAreYou(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.LatchingTask[StateInfo] = {
				checkWithin()
				var stateInfo = syncLocalStateInfo()
				if updateSeenStateInfo(stateInfo, inquirerId, inquirerInfo) then stateInfo = syncLocalStateInfo()
				sequencer.LatchingTask_ready(stateInfo)
			}

			override def onChooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.LatchingTask[Vote[ParticipantId]] = {
				checkWithin()
				var myStateInfo = syncLocalStateInfo()
				if updateSeenStateInfo(myStateInfo, inquirerId, inquirerInfo) then myStateInfo = syncLocalStateInfo()
				yieldsBlankVote(termAtExcludingConfigIndex, myStateInfo.ballot)
			}

			override def requestConfigChange(requestId: ConfigChangeRequestId, desiredParticipantsSet: Set[ParticipantId], priorAnswer: Maybe[ConfigChangeResponse]): sequencer.LatchingTask[ConfigChangeResponse] = {
				Trace.init(() => s"$boundParticipantId: Retiring.requestConfigChange") {
					var myCurrentStateInfo = syncLocalStateInfo()
					// If a prior answer is provided, update the current ballot and memorizedPeersInfos
					priorAnswer.foreach { priorResponse =>
						if updateBallotIfLowerThan(myCurrentStateInfo, priorResponse.latestBallotSeen) then myCurrentStateInfo = syncLocalStateInfo()
					}
					sequencer.LatchingTask_ready(EXCLUDED(myCurrentStateInfo.ballot))
				}
			}

			/** @inheritdoc
			 * This implementation responds with a rejection that propagates the received `attemptFlag`. */
			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.LatchingTask[ResponseToClient] = {
				sequencer.LatchingTask_ready(Unable(
					attemptFlag.withInternalBitsCleared,
					ListSet.newBuilder[ParticipantId].addAll(excludingConfigElectorate).addAll(cluster.getOtherProbableParticipants).result()
				))
			}

			override def onAppendRecords(inquirerId: ParticipantId, inquirerTerm: Term, prevRecordIndex: RecordIndex, prevRecordTerm: Term, batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.LatchingTask[AppendResult] = {
				Trace.init(() => s"$boundParticipantId: Retiring.onAppendRecords") {
					// Check if the received records contain a later [[TransitionalConfigChange]] that includes this participant.
					findLastIncludingConfigChangeIn(prevRecordIndex + 1, batch).fold(
						// If not, return a rejection.
						sequencer.LatchingTask_ready(AppendResult(finalTerm, excludingConfigIndex + 1, ordinal))
					) { findResult =>
						// If yes, become starting and redirect the append records request to the new role.
						val activeParticipants = ListSet.newBuilder.addAll(findResult.tcc.oldParticipants).addAll(findResult.tcc.newParticipants).result()
						become(Starting(findResult.index, activeParticipants))
							.onAppendRecords(inquirerId, inquirerTerm, prevRecordIndex, prevRecordTerm, batch, leaderCommit, termAtLeaderCommit)
					}
				}
			}

			/** Finds the last [[TransitionalConfigChange]] that includes this participant among the provided records.
			 * @param offset the [[RecordIndex]] of the first record.
			 * @param records the records to sarch in. */
			private def findLastIncludingConfigChangeIn(offset: RecordIndex, records: IArray[Record]): Maybe[(index: RecordIndex, tcc: TransitionalConfigChange[ParticipantId])] = {
				val excludingConfigRelativeIndex = (this.excludingConfigIndex - offset).toInt
				var relativeIndex = records.length - 1
				while relativeIndex >= 0 && relativeIndex > excludingConfigRelativeIndex do {
					records(relativeIndex) match {
						case tcc: TransitionalConfigChange[ParticipantId] @unchecked if tcc.newParticipants.contains(boundParticipantId) => return Maybe((relativeIndex + offset, tcc))
						case _ => relativeIndex -= 1
					}
				}
				Maybe.empty
			}

			override def onInstallSnapshot(inquirerId: ParticipantId, inquirerTerm: Term, snapshot: SnapshotData[ParticipantId], batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.LatchingTask[AppendResult] = {
				checkWithin()
				Trace.init(() => s"$boundParticipantId: Retiring.onInstallSnapshot") {
					// Check if the received records or the snapshot contain a later [[TransitionalConfigChange]] that includes this participant.
					findLastIncludingConfigChangeIn(snapshot.lastIncludedRecordIndex + 1, batch).orElse(
						snapshot.latestConfigChange match {
							case tcc: TransitionalConfigChange[ParticipantId] if tcc.newParticipants.contains(boundParticipantId) && snapshot.latestConfigChangeIndex > excludingConfigIndex => Maybe((snapshot.lastIncludedRecordIndex, tcc))
							case _ => Maybe.empty
						}
					).fold(
						// If not, return a rejection.
						sequencer.LatchingTask_ready(AppendResult(finalTerm, excludingConfigIndex + 1, ordinal))
					) { findResult =>
						// If yes, become starting and redirect the append records request to the new role.
						val activeParticipants = ListSet.newBuilder.addAll(findResult.tcc.oldParticipants).addAll(findResult.tcc.newParticipants).result()
						become(Starting(findResult.index, activeParticipants))
							.onInstallSnapshot(inquirerId, inquirerTerm, snapshot, batch, leaderCommit, termAtLeaderCommit)
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

		/** The behavior when the participant has the [[STARTING]] role. This is a transitory role during which the participant state is initialized.
		 * When initialization is completed it transitions to the [[Isolated]] state.
		 * @param indexOfTheIncludingConfigChange the [[RecordIndex]] of the [[TransitionalConfigChange]] that caused this [[ConsensusParticipant]] service to join.
		 * @param participantsInTheIncludingConfigChange the active participants in the [[TransitionalConfigChange]] pointed by `indexOfTheIncludingConfigChange`.
		 * TODO consider adding a parameter with the set of active participants in the including [[ConfigChange]], to pass it to the Joining role, in order to return a more updated set of active participants when responding with [[Unable]] to a command from a client. */
		private final class Starting(val indexOfTheIncludingConfigChange: RecordIndex, participantsInTheIncludingConfigChange: ListSet[ParticipantId]) extends Role {
			override val ordinal: RoleOrdinal = STARTING
			override val rank: ElectionRank = ElectionRank_from(STARTING)
			/** Is fulfilled after initializing this [[ConsensusParticipant]] and becoming another [[Role]]: [[Joining]], [[Isolated]], or [[Quiesced]]. */
			private val startingCompletedCovenant: sequencer.Covenant[PrimaryState] = sequencer.Covenant()

			override def handleEnter(previous: Role)(using Trace.Context): Unit = {
				Trace.step("Starting.onEnter") {
					notifyListeners(_.onStarting(previous.ordinal, indexOfTheIncludingConfigChange))

					storage.load.subscribe {
						case Success(loadedWorkspace) =>
							val indexOfLatestConfigChange = loadedWorkspace.indexOfLatestConfigChange
							val primaryState = new Accessible(loadedWorkspace)
							val rulingConfigChange = {
								if indexOfLatestConfigChange == 0 then {
									loadedWorkspace.setCurrentTerm(PRE_INIT)
									new TransitionalConfigChange[ParticipantId](PRE_INIT, "Initial-Config", Set.empty, cluster.getInitialParticipants) // TODO consider using the set provided in the Starting constructor instead, and remove the `getInitialParticipants` method.

								} else loadedWorkspace.latestConfigChange.get match {
									// If the top configuration change in the log is a stable one, then the previous transitional configuration change rules until the commitIndex crosses the index of top stable one, which is not happening now because the commitIndex is initialized with zero.
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
								startingCompletedCovenant.fulfillUnsafe(Inaccessible)
							}
							else {
								latestDerivedConfig = Maybe(config)
								val primaryStateFence = CausalFence[PrimaryState, sequencer.type](sequencer)(primaryState)
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

			override def determineMyVote(primaryState0: PrimaryState, dummy: StateInfo, blankVoteIfRoleChanges: Boolean)(using Context): sequencer.LatchingTask[Vote[ParticipantId]] = {
				checkWithin()
				for {
					primaryState1 <- startingCompletedCovenant // TODO ignoring the received primary state is suspicious. Analyze it.
					response <- currentRole.determineMyVote(primaryState1, currentRole.syncLocalStateInfo(primaryState1), blankVoteIfRoleChanges)
				} yield response
			}

			override def onHowAreYou(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.LatchingTask[StateInfo] = {
				checkWithin()
				for {
					_ <- startingCompletedCovenant
					response <- currentRole.onHowAreYou(inquirerId, inquirerInfo)
				} yield response
			}

			override def onChooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.LatchingTask[Vote[ParticipantId]] = {
				checkWithin()
				for {
					_ <- startingCompletedCovenant
					response <- currentRole.onChooseALeader(inquirerId, inquirerInfo)
				} yield response
			}

			override def onAppendRecords(inquirerId: ParticipantId, inquirerTerm: Term, prevRecordIndex: RecordIndex, prevRecordTerm: Term, batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.LatchingTask[AppendResult] = {
				checkWithin()
				for {
					_ <- startingCompletedCovenant
					response <- currentRole.onAppendRecords(inquirerId, inquirerTerm, prevRecordIndex, prevRecordTerm, batch, leaderCommit, termAtLeaderCommit)
				} yield response
			}

			override def onInstallSnapshot(inquirerId: ParticipantId, inquirerTerm: Term, snapshot: SnapshotData[ParticipantId], batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.LatchingTask[AppendResult] = {
				checkWithin()
				for {
					_ <- startingCompletedCovenant
					response <- currentRole.onInstallSnapshot(inquirerId, inquirerTerm, snapshot, batch, leaderCommit, termAtLeaderCommit)
				} yield response
			}

			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.LatchingTask[ResponseToClient] = {
				checkWithin()
				for {
					_ <- startingCompletedCovenant
					rtc <- currentRole.onCommandFromClient(command, attemptFlag)
				} yield rtc
			}

			override def requestConfigChange(requestId: ConfigChangeRequestId, desiredParticipantsSet: Set[ParticipantId], priorAnswer: Maybe[ConfigChangeResponse]): sequencer.LatchingTask[ConfigChangeResponse] = {
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

		private final class Joining(psf: CausalFence[PrimaryState, sequencer.type], val indexOfTheIncludingConfigChange: RecordIndex, participantsInTheIncludingConfigChange: ListSet[ParticipantId]) extends StatefulRole(psf) {
			/** The ordinal corresponding to this [[Role]] */
			override val ordinal: RoleOrdinal = JOINING
			override val rank: ElectionRank = ElectionRank_from(JOINING)

			override def handleEnter(previous: Role)(using Trace.Context): Unit = {
				notifyListeners(_.onJoining(previous.ordinal, indexOfTheIncludingConfigChange))
			}

			override def determineMyVote(primaryState0: PrimaryState, currentStateInfo: StateInfo, blankVoteIfRoleChanges: Boolean)(using Context): sequencer.LatchingTask[Vote[ParticipantId]] = {
				yieldsBlankVote(primaryState0.currentTerm, currentStateInfo.ballot)
			}

			override def onChooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.LatchingTask[Vote[ParticipantId]] = {
				Trace.init(() => s"$boundParticipantId: Joining.onChooseALeader") {
					for {
						primaryState <- primaryStateFence.causalAnchor()
						response <- {
							if currentRole ne this then currentRole.onChooseALeader(inquirerId, inquirerInfo)
							else yieldsBlankVote(primaryState.currentTerm, updateLocalStateInfo(primaryState, inquirerId, inquirerInfo).ballot)
						}
					} yield response
				}
			}

			/** @inheritdoc
			 * This implementation responds with a rejection that propagates the received `attemptFlag`. */
			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.LatchingTask[ResponseToClient] = {
				checkWithin()
				sequencer.LatchingTask_ready(Unable(attemptFlag.withInternalBitsCleared, participantsInTheIncludingConfigChange))
			}

			override def requestConfigChange(primaryState: Accessible, requestId: ConfigChangeRequestId, desiredParticipants: Set[ParticipantId], ballotWasUpdated: Boolean)(using Trace.Context): sequencer.LatchingTask[ConfigChangeResponse] = {
				sequencer.LatchingTask_ready(CATCHING_UP(syncLocalStateInfo(primaryState).ballot))
			}
		}

		private final def Joining(psf: CausalFence[PrimaryState, sequencer.type], indexOfTheIncludingConfigChange: RecordIndex, participantsInTheIncludingConfigChange: ListSet[ParticipantId]): Maybe[Joining] = {
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
		private class Isolated(psf: CausalFence[PrimaryState, sequencer.type]) extends StatefulRole(psf) {
			override val ordinal: RoleOrdinal = ISOLATED
			override val rank: ElectionRank = ElectionRank_from(ISOLATED)

			/**
			 * The main loop of the isolated state.
			 * It checks if the current term leader is reachable or the reachable participants including itself are the majority.
			 * If so, it becomes a follower or a candidate respectively.
			 * If not, it stays in the isolated state and checks again after a while.
			 */
			override def handleEnter(previous: Role)(using Trace.Context): Unit = {
				notifyListeners(_.onBecameIsolated(previous.ordinal, psf.committedState.currentTerm))
			}

			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.LatchingTask[ResponseToClient] = {
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

			override def requestConfigChange(primaryState0: Accessible, requestId: ConfigChangeRequestId, desiredParticipants: Set[ParticipantId], ballotWasUpdated: Boolean)(using Context): sequencer.LatchingTask[ConfigChangeResponse] = {
				Trace.trace(s"Updating role from ${RoleOrdinal_nameOf(currentRole.ordinal)} due to a configuration change request. ")
				for {
					_ <- updateRole(primaryState0) // TODO consider making updateRole return the current primary state, so that the causalAnchor method call is not needed here (and other places also).
					primaryState <- primaryStateFence.causalAnchor()
					response <- {
						if currentRole ne this then currentRole.requestConfigChange(requestId, desiredParticipants, Maybe.empty)
						else sequencer.LatchingTask_ready(SECLUDED(syncLocalStateInfo(primaryState).ballot))
					}
				} yield response
			}
		}

		private final def Isolated(psf: CausalFence[PrimaryState, sequencer.type]): Maybe[Isolated] = {
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
		 * It behaves as [[Isolated]] except that, in the [[handleEnter]] life-cycle stage it enqueues an updater of the [[PrimaryState.currentTerm]] that sets it to the latest [[Term]] seen if not already; and then transitions to [[Retiring]] if this participant is excluded from the active [[Configuration]], or to [[Isolated]] otherwise.
		 *
		 * @param endedTerm the [[Term]] that concluded, during which this participant acted as [[Leader]].
		 * TODO Replace this class with a method that transitions to [[Isolated]] or [[Retiring]] in a synchronous manner, and then enqueues a term update. The problem with the current class approach is the incorrect isolated-like behavior during the transition to retiring.
		 */
		private final class HandingOff(endedTerm: Term, latestTermSeen: Term, psf: CausalFence[PrimaryState, sequencer.type]) extends Isolated(psf) {
			override val ordinal: RoleOrdinal = HANDING_OFF

			override val rank: ElectionRank = ElectionRank_from(HANDING_OFF)

			override def handleEnter(previous: Role)(using Trace.Context): Unit = {
				notifyListeners(_.onHandingOff(endedTerm))

				for {
					primaryState1 <- primaryStateFence.advanceIf { primaryState0 =>
						if primaryState0.currentTerm >= latestTermSeen then Maybe.empty
						else primaryState0 match {
							case accessible0: Accessible => Maybe(accessible0.withTermUpdated(latestTermSeen))
							case Inaccessible => Maybe.empty
						}
					}
				} do if currentRole eq this then primaryState1 match {
					case Inaccessible =>
						illegalStateQuiesce()

					case accessible: Accessible =>
						// check if excluded from the new configuration.
						val updatedConfig = deriveConfigurationFrom(accessible)
						// if excluded, become Retiring
						if !updatedConfig.isBoundIncluded then become(Retiring(accessible.currentTerm, updatedConfig.term, updatedConfig.changeIndex, updatedConfig.electorate))
						// else, become Isolated
						else become(Isolated(primaryStateFence))
				}
			}
		}

		private final def HandingOff(endedTerm: Term, latestTermSeen: Term, psf: CausalFence[PrimaryState, sequencer.type]): Maybe[HandingOff] = {
			Maybe(new HandingOff(endedTerm, latestTermSeen, psf))
		}

		/**
		 * Behavior when the participant has the follower role. Taken when reachability to a majority of the participants is achieved and one of them has the [[Leader]] role and is in a higher or equal term.
		 *
		 * In this state, the participant acknowledges the specified leader.
		 * @param term the [[Term]] during which the followed participant is the leader. This field exists to differentiate [[Follower]] instances. // TODO explain why is necessary to differentiate them.
		 * @param followeeId The ID of the participant this follower is following.
		 */
		private final class Follower(val term: Term, val followeeId: ParticipantId, psf: CausalFence[PrimaryState, sequencer.type]) extends StatefulRole(psf) {
			override val ordinal: RoleOrdinal = FOLLOWER
			override val rank: ElectionRank = ElectionRank_from(FOLLOWER)

			override def handleEnter(previous: Role)(using Trace.Context): Unit = {
				notifyListeners(_.onBecameFollower(previous.ordinal, term, followeeId))
			}

			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.LatchingTask[ResponseToClient] = {
				Trace.init(() => s"$boundParticipantId: Follower.onCommandFromClient") {
					checkWithin()
					for {
						primaryState <- primaryStateFence.causalAnchor()
						response <- {
							if currentRole ne this then currentRole.onCommandFromClient(command, attemptFlag)
							else if attemptFlag == FIRST_ATTEMPT then sequencer.LatchingTask_ready(RedirectTo(followeeId))
							else updateRoleAndThenCallsOnCommandFromClient(command, attemptFlag)
						}
					} yield response
				}
			}

			override def requestConfigChange(primaryState0: Accessible, requestId: ConfigChangeRequestId, desiredParticipants: Set[ParticipantId], ballotWasUpdated: Boolean)(using Context): sequencer.LatchingTask[ConfigChangeResponse] = {
				Trace.trace(s"Updating role from ${RoleOrdinal_nameOf(currentRole.ordinal)} due to a configuration change request.")
				for {
					_ <- updateRole(primaryState0)
					primaryState1 <- primaryStateFence.causalAnchor()
					response <- {
						if currentRole ne this then currentRole.requestConfigChange(requestId, desiredParticipants, Maybe.empty)
						else sequencer.LatchingTask_ready(ASK_THE_LEADER(followeeId, syncLocalStateInfo(primaryState1).ballot))
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

		/** A hidden (not seen by other participants) and transitional substate of a leading participant that last until the term bump is stored.
		 * During this interval, all the RPC calls this [[ConsensusParticipant]] receives are put in standby until the bumped term is stored and the role transitioned. This means that responses to queries form the outside never complete in this role and, therefore, the role ordinal in responses is never [[PROMOTING]].
		 * Also, given the [[currentRole]] is changed to [[Leader]] synchronously in a consumer synchronously subscribed to the [[LatchingTask]] returned by [[primaryStateFence.advanceIf]], sections of code guarded by the same fence will never see [[currentRole]] referencing a [[Promoting]] instance. See the [[CausalFence]]'s game changing invariant. */
		private final class Promoting(fromTerm: Term, psf: CausalFence[PrimaryState, sequencer.type]) extends StatefulRole(psf) {
			/** The ordinal corresponding to this [[Role]] */
			override val ordinal: RoleOrdinal = PROMOTING
			override val rank: ElectionRank = ElectionRank_from(PROMOTING)

			/** Is fulfilled after bumping the term and becoming [[Leader]] if success, or [[Quiesced]] if fails to persist the primary state. */
			private val promotionCovenant: sequencer.Covenant[PrimaryState] = sequencer.Covenant()

			override def handleEnter(previous: Role)(using Trace.Context): Unit =
				Trace.step("Promoting.onEnter") {
					// Notifiy the listeners
					notifyListeners(_.onPromoting(previous.ordinal, getCommittedPrimaryState.currentTerm))

					for {
						// Bump the term
						primaryState1 <- primaryStateFence.advanceIf { primaryState0 =>
							if currentRole ne this then Maybe.empty
							else primaryState0 match {
								case Inaccessible => Maybe.empty
								case accessible0: Accessible => Maybe(accessible0.withTermUpdated(primaryState0.currentTerm.incremented))
							}
						}
					} do {
						if currentRole eq this then {
							primaryState1 match {
								case Inaccessible =>
									illegalStateQuiesce()

								case accessible1: Accessible =>
									// Become the leader.
									val config1 = deriveConfigurationFrom(accessible1)
									become(Maybe(new Leader(accessible1.currentTerm, accessible1, config1, primaryStateFence)))
								// // Start the commited-commands-applier to apply any potentially unapplied record commited by the previous role. This is necessary because the Leader applies the commands directly to the state machine and therefore, never starts the commited-commands-applier.
								// if decoupledCommandsApplierCompletion.isCompleted then startApplyingCommittedCommands(accessible1, false)
							}
						}
						promotionCovenant.fulfillUnsafe(primaryState1)
					}
				}

			override def determineMyVote(primaryState0: PrimaryState, dummy: StateInfo, blankVoteIfRoleChanges: Boolean)(using Context): sequencer.LatchingTask[Vote[ParticipantId]] = {
				checkWithin()
				for {
					primaryState1 <- promotionCovenant
					vote <- currentRole.determineMyVote(primaryState1, currentRole.syncLocalStateInfo(primaryState1), blankVoteIfRoleChanges)
				} yield vote
			}

			override def onHowAreYou(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.LatchingTask[StateInfo] = {
				checkWithin()
				for {
					_ <- promotionCovenant
					stateInfo <- currentRole.onHowAreYou(inquirerId, inquirerInfo)
				} yield stateInfo
			}

			override def onChooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.LatchingTask[Vote[ParticipantId]] = {
				checkWithin()
				for {
					_ <- promotionCovenant
					vote <- currentRole.onChooseALeader(inquirerId, inquirerInfo)
				} yield vote
			}

			override def onAppendRecords(inquirerId: ParticipantId, inquirerTerm: Term, prevRecordIndex: RecordIndex, prevRecordTerm: Term, batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.LatchingTask[AppendResult] = {
				checkWithin()
				for {
					_ <- promotionCovenant
					response <- currentRole.onAppendRecords(inquirerId, inquirerTerm, prevRecordIndex, prevRecordTerm, batch, leaderCommit, termAtLeaderCommit)
				} yield response
			}

			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.LatchingTask[ResponseToClient] = {
				checkWithin()
				for {
					_ <- promotionCovenant
					response <- currentRole.onCommandFromClient(command, attemptFlag)
				} yield response
			}

			override def requestConfigChange(primaryState: Accessible, requestId: ConfigChangeRequestId, desiredParticipants: Set[ParticipantId], ballotWasUpdated: Boolean)(using Trace.Context): sequencer.LatchingTask[ConfigChangeResponse] = {
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

		/**
		 * Behavior when the participant has the [[LEADER]] role. Taken when reachability to a majority of the participants is achieved, none of them is a [[Leader]] with higher or equal term, and wins the new leader election.
		 *
		 * In this state, the participant coordinates consensus decisions.
		 * @param leadedTerm the [[Term]] owned by this [[Leader]] instance.
		 * @param initialPrimaryState the current [[PrimaryState]] when this [[Leader]] instance was created. Intended to be used in the [[handleEnter]] method only. Do not use elsewhere.
		 * @param initialConfig the active [[Configuration]] when this [[Leader]] instance was created. Intended to be used in the [[handleEnter]] method only. Do not use elsewhere.
		 * @param wsf the [[CausalFence]] that must be used to ensure causal ordering of the state updates. It must be propagated to subsequent [[StatefulRole]] instances.
		 * TODO replace the `initialPrimaryState` parameter with what is obtained from it. Storing an instance of [[Accessible]] is error prone.
		 */
		private final class Leader(val leadedTerm: Term, initialPrimaryState: Accessible, initialConfig: Configuration, wsf: CausalFence[PrimaryState, sequencer.type]) extends StatefulRole(wsf) { thisLeader =>
			/** The outcome of the [[sequencer.Venture]] returned by a call to [[ClusterParticipant.appendRecords]]. */
			private type AppendResponse = Try[AppendResult]

			override val ordinal: RoleOrdinal = LEADER
			override val rank: ElectionRank = ElectionRank_from(LEADER)

			/** The index of the next record to send to a participant, indexed by the participant index.
			 * This array is optimistically initialized to the first empty record index of the leader's workspace for all participants,
			 * assuming that each follower's log is already up-to-date with the leader's log. This optimistic initialization
			 * allows the leader to attempt to append new entries immediately, but if a follower's log is actually behind or inconsistent,
			 * the index will be decremented as needed until the logs are aligned.
			 * When a record is successfully replicated to a participant, the index of the next record to send to that participant is incremented.
			 * When a record is not successfully replicated to a participant, the index of the next record to send to that participant is decremented.
			 * TODO: Consider initializing the array with the first empty record index unless the last filled ones are configuration changes, in which case initialize with the index of the first of them. Why? Because sending extra [[ConfigChange]] instances is cheap and may avoid rejections due to need of an earlier [[Record]].
			 */
			private var indexOfNextRecordToSend_ByParticipantIndex: Array[RecordIndex] = Array.fill(initialConfig.peers.size)(initialPrimaryState.firstEmptyRecordIndex)
			/** The highest record index known to be replicated to a participant, indexed by the participant index.
			 * This array is conservatively initialized to 0 for all participants, assuming that no records are known to be replicated to any follower at the start of the leader's term.
			 * As records are successfully replicated to a participant, the corresponding value is incremented.
			 * This conservative initialization ensures that the leader does not overestimate the replication state of any follower and only advances commitIndex when a true majority is confirmed.
			 */
			private var highestRecordIndexKnownToBeAppended_ByParticipantIndex: Array[RecordIndex] = Array.fill(initialConfig.peers.size)(0)

			private var highestRecordIndexKnowToBeCommitted_ByParticipantIndex: Array[RecordIndex] = Array.fill(initialConfig.peers.size)(0)

			/** Either, the index of the [[StableConfigChange]] that excluded this leading participant causing it become a ghost leader, or zero if in joint consensus or not excluded.
			 * Set by the [[Leader.driveTheRetirements]] method, which is called by [[deriveConfigurationFrom]] when the active [[Configuration]] changes from a [[TransitionalConfig]] to a [[StableConfig]]. */
			private var indexOfConfigChangeThatExcludedThisParticipant: RecordIndex = 0

			/** The serial number of the last replication attempt. Incremented whenever the [[attemptToUpdateOtherParticipantsLogs]] method is called. */
			private var serialOfLastReplicationAttempt: Int = 0

			private var unreachableFollowersRetryWakeUp: Maybe[WakeUpToken] = Maybe.empty
			private var sccReplicationRetryWakeUp: Maybe[WakeUpToken] = Maybe.empty
			private var tccReplicationRetryWakeup: Maybe[WakeUpToken] = Maybe.empty

			override def handleEnter(previous: Role)(using Trace.Context): Unit = {
				Trace.step("Leader.onEnter") {
					notifyListeners(_.onBecameLeader(previous.ordinal, leadedTerm))

					val indexOfLatestConfigChange = initialPrimaryState.indexOfLatestConfigChange
					// if the log lacks a ConfigChange record (is empty), create a synthetic one with the seed participants of the initial synthetic configuration (appointed in `latestDerivedConfig` during Starting).
					if indexOfLatestConfigChange == 0 then {
						for primaryState1 <- primaryStateFence.advanceIf {
							case Inaccessible => Maybe.empty
							case accessible0: Accessible => Maybe(accessible0.withSingleRecordAppended(accessible0.currentTerm, latestDerivedConfig.get.backingConfigChange))
						} yield startConfigChangeSecondPhase(latestDerivedConfig.get.backingConfigChange.asInstanceOf[TransitionalConfigChange[ParticipantId]], 1)
					}
					// if the log contains a ConfigChange then:
					else initialPrimaryState.latestConfigChange.get match {
						// If the top configuration change in the local log is a transitional one, continue the configuration transition process. This happens when the leader that started the first phase of the configuration change crashed or left the leadership before achieving the replication of the TransitionalConfigChange to a majority, or while storing the StableConfigChange in his persistent log.
						case tcc: TransitionalConfigChange[ParticipantId @unchecked] =>
							startConfigChangeSecondPhase(tcc, indexOfLatestConfigChange)

						// If, on the contrary, is a stable one
						case scc: StableConfigChange[ParticipantId @unchecked] =>
							// ... and it was committed (commitIndex >= its index in the log), program the driving of excluded participants to retirement.
							if commitIndex >= indexOfLatestConfigChange then thisLeader.driveTheRetirements(initialPrimaryState, Maybe.empty, scc, indexOfLatestConfigChange)
							// ... and it wasn't committed (commitIndex < its index in the log), drive its commitment eagerly.
							else replicateSccUntilSuccessOrLeaderRoleIsAbandoned(initialPrimaryState, indexOfLatestConfigChange, 0)
							
					}
				}
			}


			override def handleExit(newRole: Role): Unit = {
				super.handleExit(newRole)
				unreachableFollowersRetryWakeUp.foreach(_.cancel())
				unreachableFollowersRetryWakeUp = Maybe.empty
				sccReplicationRetryWakeUp.foreach(_.cancel())
				sccReplicationRetryWakeUp = Maybe.empty
				tccReplicationRetryWakeup.foreach(_.cancel())
				tccReplicationRetryWakeup = Maybe.empty
			}

			def isGhost: Boolean = indexOfConfigChangeThatExcludedThisParticipant > 0

			/** @inheritdoc
			 *  This implementation does two different things:
			 *  1) Updates the [[RetiringParticipantsManager]] to include any new old-configuration-only retiring participant (those that are not part of the new [[Configuration]], but still need more appends until their [[commitIndex]] reaches the index of the [[StableConfigChange]] that excluded them).
			 *  2) Recreates and initializes the [[indexOfNextRecordToSend_ByParticipantIndex]], [[highestRecordIndexKnownToBeAppended_ByParticipantIndex]], and [[highestRecordIndexKnowToBeCommitted_ByParticipantIndex]] arrays keeping the elements corresponding to the participants that remain and moving them to the appropriate index.
			 * @param oldConfig the [[Configuration]] that determines which participants corresponds to each element of the [[indexOfNextRecordToSend_ByParticipantIndex]], [[highestRecordIndexKnownToBeAppended_ByParticipantIndex]], and [[highestRecordIndexKnowToBeCommitted_ByParticipantIndex]] arrays.
			 * @note This rearrangement wouldn't be necessary if maps instead of arrays were used. But considering these two collections are heavily used, efficiency was primed. */
			override def handleActiveConfigChange(currentPrimaryState: Accessible, oldConfig: Configuration, newConfig: Configuration, indexOfNewConfigChange: RecordIndex)(using Context): Unit = Trace.step("onActiveConfigChanged") {
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
				val newIndexOfNextRecordToSend_ByParticipantIndex: Array[RecordIndex] = new Array(newAllOtherParticipantsArrayLength)
				val newHighestRecordIndexKnowToBeAppended_ByParticipantIndex: Array[RecordIndex] = new Array(newAllOtherParticipantsArrayLength)
				val newHighestRecordIndexKnowToBeCommitted_ByParticipantIndex: Array[RecordIndex] = new Array(newAllOtherParticipantsArrayLength)

				var participantNewIndex = newAllOtherParticipantsArrayLength
				while participantNewIndex > 0 do {
					participantNewIndex -= 1
					val participantId = newConfig.peers(participantNewIndex)
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

			/** Drives the excluded participants (the ones that are not active in the provided [[StableConfigChange]]) to retirement.
			 *		- If this [[Leader]] is excluded, sets the threshold [[indexOfConfigChangeThatExcludedThisParticipant]]. The replication logic checks it after successful appends to decide if a transition to the [[Retiring]] [[Role]] is needed.
			 *		- Creates and registers an instance of [[RetirementDriver]] for each excluded follower that needs more appends to become [[Retiring]].
			 * Must be called a single time whenever the participant becomes [[Leader]] with a [[StableConfig]] or the participant is leading and the active [[Configuration]] transitions to a [[StableConfig]].
			 *
			 * @param primaryState the [[PrimaryState]] from which the transition is derived.
			 * @param maybeStandingConfig the [[Configuration]] on which the [[Leader]] derived state is based. May be the same as the received in the `stableConfigChange` parameter. It is needed to know what is in each element of the [[indexOfNextRecordToSend_ByParticipantIndex]] and [[highestRecordIndexKnownToBeAppended_ByParticipantIndex]].
			 * @param stableConfigChange the [[StableConfigChange]] that might exclude participants.
			 * @param stableConfigChangeIndex the log index where the provided [[StableConfigChange]] is stored. */
			private def driveTheRetirements(primaryState: Accessible, maybeStandingConfig: Maybe[Configuration], stableConfigChange: StableConfigChange[ParticipantId], stableConfigChangeIndex: RecordIndex)(using Context): Unit = {
				assert(commitIndex >= stableConfigChangeIndex && maybeStandingConfig.fold(true)(_.isInstanceOf[TransitionalConfig]))

				// Stop the RetirementDriver instances corresponding to the participants that become included.
				retirementDriverByParticipantId.filterInPlace { (retireeId, driver) => !stableConfigChange.newParticipants.contains(retireeId) }

				// Find out which are the participants that become excluded.
				val newRetiringParticipants = stableConfigChange.oldParticipants.diff(stableConfigChange.newParticipants)

				// If this leader is excluded, set the threshold until which this leader will continue leading as a ghost.
				if newRetiringParticipants.contains(boundParticipantId) then thisLeader.indexOfConfigChangeThatExcludedThisParticipant = stableConfigChangeIndex
				// If this leader continues as a stable leader (not a ghost), authorize the quiescence of the retiring followers.
				else {
					val notNewParticipants = newRetiringParticipants.union(cluster.getOtherProbableParticipants).diff(stableConfigChange.newParticipants)
					authorizeQuiescenceTo(notNewParticipants, stableConfigChangeIndex, false)
				}

				val minCommitIndexOfExcludedParticipants: RecordIndex = maybeStandingConfig.fold(0L) { standingConfig =>
					// Find the minimum of the `highestRecordIndexKnowToBeCommitted_ByParticipantIndex` array among the excluded participants.
					standingConfig.peers.foldLeftWithIndex(commitIndex) { (minCommitIndex, participantId, participantIndex) =>
						val highestRecordIndexKnowToBeCommitted = highestRecordIndexKnowToBeCommitted_ByParticipantIndex(participantIndex)
						if highestRecordIndexKnowToBeCommitted < minCommitIndex && newRetiringParticipants.contains(participantId) then highestRecordIndexKnowToBeCommitted
						else minCommitIndex
					}
				}
				val termBeforeFirstPotentiallyUnappendedPlainRecord =
					if minCommitIndexOfExcludedParticipants >= primaryState.logBufferOffset then primaryState.getRecordTermAt(minCommitIndexOfExcludedParticipants)
					else primaryState.latestSnapshot.fold(PRE_INIT)(_.lastIncludedRecordTerm)
				val indexOfFirstPotentiallyUnappendedRecord = minCommitIndexOfExcludedParticipants + 1
				val indexOfFirstPotentiallyUnappendedPlainRecord = if indexOfFirstPotentiallyUnappendedRecord >= primaryState.logBufferOffset then indexOfFirstPotentiallyUnappendedRecord else primaryState.logBufferOffset
				val potentiallyUnappendedPlainRecords: IArray[Record] = primaryState.getRecordsBetween(indexOfFirstPotentiallyUnappendedPlainRecord, stableConfigChangeIndex + 1)

				/** Creates and registers a [[RetirementDriver]] for the specified participant. */
				def start(participantId: ParticipantId, indexOfNextRecordToSend: RecordIndex): Unit = {
					val retirementDriver = new RetirementDriver(
						participantId,
						termBeforeFirstPotentiallyUnappendedPlainRecord,
						potentiallyUnappendedPlainRecords,
						indexOfFirstPotentiallyUnappendedPlainRecord,
						stableConfigChangeIndex,
						primaryState.getRecordTermAt(stableConfigChangeIndex),
						primaryState.latestSnapshot,
						indexOfNextRecordToSend
					)
					retirementDriverByParticipantId.put(participantId, retirementDriver)
					retirementDriver.driveLoop(primaryState.currentTerm, 0)
				}

				// Create and register a retirement driver for each follower that both, is excluded, and we are not certain that it has commited the `stableConfigChange`.
				maybeStandingConfig.fold(
					newRetiringParticipants.foreach { participantId =>
						if participantId != boundParticipantId then start(participantId, stableConfigChangeIndex) // The exclusion of the bound participants is currently not necessary but is harmless and may be necessary and hard to catch if the logic is modified.
					}
				) { standingConfig =>
					standingConfig.peers.foreachWithIndex { (participantId, participantIndex) =>
						if highestRecordIndexKnowToBeCommitted_ByParticipantIndex(participantIndex) < stableConfigChangeIndex && newRetiringParticipants.contains(participantId)
						then start(participantId, thisLeader.indexOfNextRecordToSend_ByParticipantIndex(participantIndex))
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
					def loop(attemptsDone: Int = 0): Unit = {
						val persmissionsArray = nonAcknowledgedQuiescencePermissions.toArray
						val calls = for (participantId, indexOfAuthorizedScc) <- persmissionsArray yield participantId.permitQuiescence(indexOfAuthorizedScc)
						for responses <- sequencer.LatchingTask_sequenceVenturesToArray(calls) do {
							IArray.unsafeFromArray(responses).foreachWithIndex { (response, arrayIndex) =>
								val permissionEntry = persmissionsArray(arrayIndex)
								val participantId = permissionEntry._1
								response match {
									case Failure(e) =>
										val permittedCci = permissionEntry._2
										Trace.debug(s"$boundParticipantId: An attempt to permit $participantId to quiesce at $permittedCci failed after $attemptsDone attempts ${if permittedCci == permittedConfigChangeIndex then "" else s"(since the configuration change at $permittedConfigChangeIndex)"} with:", e)
									case _ =>
										nonAcknowledgedQuiescencePermissions.remove(participantId)
								}
							}
							if nonAcknowledgedQuiescencePermissions.nonEmpty && attemptsDone < MAX_PERMIT_QUIESCENCE_RETRIES then {
								val token = requestWakeUp(WakeUpReason.QuiescenceAuthorizationRetry, attemptsDone, () => loop(attemptsDone + 1))
								retryPermitQuiescenceWakeUp = Maybe(token)
							} else {
								if nonAcknowledgedQuiescencePermissions.nonEmpty then {
									Trace.warn(s"$boundParticipantId: The limit of attempts ($attemptsDone) to permit the participants $nonAcknowledgedQuiescencePermissions to quiesce at $permittedConfigChangeIndex has been reached.")
									nonAcknowledgedQuiescencePermissions.clear()
								}
								if includeMyself then currentRole.onQuiescencePermitted(boundParticipantId, permittedConfigChangeIndex)
								else becomeQuiescedIfEligible(permittedConfigChangeIndex)
							}
						}
					}

					retryPermitQuiescenceWakeUp.foreach(_.cancel())
					participants.foreach { participantId => nonAcknowledgedQuiescencePermissions.put(participantId, permittedConfigChangeIndex) }
					loop(0)
				}
			}

			override def authorizeQuiescenceIfVanished(config: StableConfig)(using Trace.Context): Unit = {
				if config.electorate.length == 0 then {
					val notBoundParticipantId = config.backingConfigChange.oldParticipants.union(cluster.getOtherProbableParticipants) - boundParticipantId
					authorizeQuiescenceTo(notBoundParticipantId, config.changeIndex, true)
				}
			}

			/** Handles configuration-change request for [[Leader]]
			 * Attempts a [[Configuration]] change, starting with the first phase and, if successful, continuing with the second. */
			override def requestConfigChange(primaryState0: Accessible, requestId: ConfigChangeRequestId, desiredParticipants: Set[ParticipantId], ballotWasUpdated: Boolean)(using Context): sequencer.LatchingTask[ConfigChangeResponse] = {

				def startSecondPhase(tcc: TransitionalConfigChange[ParticipantId], tccIndex: RecordIndex): sequencer.LatchingTask[ConfigChangeResponse] = {
					for {
						isSccReplicatedToMajority <- startConfigChangeSecondPhase(tcc, tccIndex)
						primaryState1 <- primaryStateFence.causalAnchor()
					} yield {
						val ballot1 = syncLocalStateInfo(primaryState1).ballot
						if isSccReplicatedToMajority then SUCCESSFULLY_CHANGED(ballot1) else REQUEST_TRACKING_LOST_AFTER_SECOND_PHASE_STARTED(ballot1)
					}
				}

				/** This method recurses whenever it fails and the consequent [[updateRole]] does not change the [[Role]] (stays as leader) */
				def replicateTccAndThenStartSecondPhase(primaryState1: PrimaryState, tcc: TransitionalConfigChange[ParticipantId], tccIndex: RecordIndex, attemptsDone: Int): sequencer.LatchingTask[ConfigChangeResponse] = {
					if currentRole ne this then sequencer.LatchingTask_ready(REQUEST_TRACKING_LOST_AFTER_FIRST_PHASE_STARTED(syncLocalStateInfo(primaryState1).ballot))
					else primaryState1 match {
						case Inaccessible =>
							illegalStateQuiesce()
							sequencer.LatchingTask_ready(STOPPED(buildIneligibleInfo(leadedTerm).ballot))

						case accessible0: Accessible =>
							if assertionsEnabled then assert(accessible0.currentTerm == leadedTerm)
							Trace.trace(s"Replicating TCC at index $tccIndex")
							tccReplicationRetryWakeup.foreach(_.cancel())
							for {
								// Replicate to other participants.
								isTccReplicatedToMajority <- attemptToUpdateOtherParticipantsLogs(accessible0)
								primaryState2 <- primaryStateFence.causalAnchor()
								response <- {
									if currentRole ne this then {
										val ballot1 = syncLocalStateInfo(primaryState2).ballot
										sequencer.LatchingTask_ready(if isTccReplicatedToMajority then REQUEST_TRACKING_LOST_AFTER_FIRST_PHASE_COMMITED(ballot1) else REQUEST_TRACKING_LOST_AFTER_FIRST_PHASE_STARTED(ballot1))
									}
									else {
										if assertionsEnabled then assert(primaryState2.currentTerm == leadedTerm)
										if isTccReplicatedToMajority then startSecondPhase(tcc, tccIndex)
										else {
											// Wait some time before trying again.
											val covenant = sequencer.Covenant[ConfigChangeResponse]()
											val token = requestWakeUp(WakeUpReason.ReplicationLoopRetry, attemptsDone, () => {
												if commitIndex >= tccIndex then covenant.fulfillWith(startSecondPhase(tcc, tccIndex))
												else {
													Trace.trace(s"Updating role due to insufficient quorum when replicating the TCC at $tccIndex. Attempt #$attemptsDone")
													for {
														_ <- updateRole()
														primaryState3 <- primaryStateFence.causalAnchor()
														response <- replicateTccAndThenStartSecondPhase(primaryState3, tcc, tccIndex, attemptsDone + 1)
													} do covenant.fulfillUnsafe(response)
												}
											})
											tccReplicationRetryWakeup = Maybe(token)
											covenant
										}
									}
								}
							} yield response
					}
				}

				if assertionsEnabled then assert(primaryState0.currentTerm == leadedTerm)

				// First, synchronize the [[StateInfo]] of the bound participant and memorize the current ballot round.
				val myStateInfo0 = syncLocalStateInfo(primaryState0)
				Trace.trace(s"StateInfo=$myStateInfo0")
				deriveConfigurationFrom(primaryState0) match {
					case stable0: StableConfig =>
						if desiredParticipants == stable0.stableParticipants then sequencer.LatchingTask_ready(ALREADY_CHANGED(myStateInfo0.ballot))
						// Do not start a configuration transition if excluded from both, the current, and the new configuration.
						else if !stable0.isBoundIncluded && !desiredParticipants.contains(boundParticipantId) then {
							// Also, become retiring immediately if all followers have committed the excluding config change. The intention of this is to minimize the time that a participant is kept leading after it was excluded.
							if isGhostAndAllFollowersCommittedTheExcludingConfigChange then {
								if assertionsEnabled then assert(indexOfConfigChangeThatExcludedThisParticipant == stable0.changeIndex)
								authorizeQuiescenceIfVanished(stable0)
								become(Retiring(primaryState0.currentTerm, stable0.term, stable0.changeIndex, stable0.electorate)).requestConfigChange(requestId, desiredParticipants, Maybe.empty)
							}
							// If leading as a ghost and some follower hasn't commited the excluding config change, make them commit it.
							else {
								for {
									isCurrentConfigReplicationCommited <- attemptToUpdateOtherParticipantsLogs(primaryState0)
									primaryState1 <- primaryStateFence.causalAnchor()
									response <- {
										if isCurrentConfigReplicationCommited then currentRole.requestConfigChange(requestId, desiredParticipants, Maybe.empty)
										else sequencer.LatchingTask_ready(WAIT_GHOST_LEADER_IS_DEPOTED(syncLocalStateInfo(primaryState1).ballot))
									}
								} yield response
							}
						} else {
							// start the first phase of the configuration change
							val tcc = new TransitionalConfigChange[ParticipantId](primaryState0.currentTerm, requestId, stable0.stableParticipants, desiredParticipants)
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
								response <- replicateTccAndThenStartSecondPhase(primaryState2, tcc, primaryState2.firstEmptyRecordIndex - 1, 0)
							} yield response
						}

					case transitional0: TransitionalConfig =>
						// TODO use a coalescer to avoid this lazy answers.
						val response = if desiredParticipants == transitional0.stableParticipants then ALREADY_IN_PROGRESS(myStateInfo0.ballot) else WAIT_PREVIOUS_CHANGE_TO_COMPLETE(myStateInfo0.ballot)
						sequencer.LatchingTask_ready(response)
				}
			}

			/** Starts the second phase of a configuration change.
			 * Appends a [[StableConfigChange]] instance in the local log, stores it, and then attempts to replicate it to the participants in both, old and new configurations as if its configuration was the corresponding [[TransitionalConfigChange]].
			 * @param correspondingTransitionalConfigChange the [[TransitionalConfigChange]] that initiated the first phase of the configuration change.
			 * @return  a [[sequencer.LatchingTask]] that yields true/false if the [[StableConfigChange]] [[Record]] was/wasn't replicated to a majority. */
			private def startConfigChangeSecondPhase(correspondingTransitionalConfigChange: TransitionalConfigChange[ParticipantId], tccIndex: RecordIndex)(using Context): sequencer.LatchingTask[Boolean] = {
				Trace.step("startConfigChangeSecondPhase") {
					for {
						primaryState1 <- primaryStateFence.advanceIf { primaryState0 =>
							if currentRole ne this then Maybe.empty
							else primaryState0 match {
								case accessible0: Accessible =>
									if assertionsEnabled then assert(accessible0.currentTerm == leadedTerm)
									if accessible0.indexOfLatestConfigChange > tccIndex then Maybe.empty
									else {
										val scc = new StableConfigChange[ParticipantId](accessible0.currentTerm, correspondingTransitionalConfigChange.requestId, correspondingTransitionalConfigChange.term, correspondingTransitionalConfigChange.oldParticipants, correspondingTransitionalConfigChange.newParticipants)
										Maybe(accessible0.withSingleRecordAppended(accessible0.currentTerm, scc))
									}
								case Inaccessible =>
									Maybe.empty
							}
						}

						isSecondPhaseChangeReplicatedToMajority <- {
							primaryState1 match {
								case Inaccessible =>
									sequencer.LatchingTask_false
								case accessible1: Accessible =>
									// TODO add a coupleIndex field in StableConfiChange and use it in the next if condition instead of the requestId (whose uniqueness depends on the user).
									if accessible1.latestConfigChange.get.requestId == correspondingTransitionalConfigChange.requestId then {
										val sccIndex = accessible1.indexOfLatestConfigChange
										Trace.trace(s"Starting replication of SCC at $sccIndex. The corresponding TCC is $correspondingTransitionalConfigChange at $tccIndex")
										replicateSccUntilSuccessOrLeaderRoleIsAbandoned(primaryState1, sccIndex, 0)
									} else sequencer.LatchingTask_true
							}
						}
					} yield isSecondPhaseChangeReplicatedToMajority
				}
			}

			/** Replicates all the uncommitted records in the local log to the peers, retrying until either:
			 *  - the [[Record]]s up to the provided index are replicated to a majority ([[commitIndex]] equals or greater than the provided index).
			 *  - the [[currentRole]] stops being this [[Leader]] instance.
			 *
			 * A no-op [[LeaderTransition]] record is appended if [[Record]]s of a previous [[Term]] are blocking the [[commitIndex]] advancement due to the Raft safety rule (§5.4.2): "A leader cannot determine commitment using entries from previous terms". This contraint is implemented in [[TransitionalConfig.indexOfTheCommittedRecordWithHighestIndex]].
			 * This method recurses whenever it fails and the consequent [[updateRole]] does not change the [[Role]] (stays as leader) */
			private def replicateSccUntilSuccessOrLeaderRoleIsAbandoned(primaryState0: PrimaryState, sccIndex: RecordIndex, attemptsDone: Int)(using Context): sequencer.LatchingTask[Boolean] = {
				Trace.step("replicateSccUntilSuccessOrLeaderRoleIsAbandoned") {
					if currentRole ne this then sequencer.LatchingTask_false
					else primaryState0 match {
						case Inaccessible =>
							sequencer.LatchingTask_false
						case accessible0: Accessible =>
							assert(accessible0.currentTerm == leadedTerm)
							sccReplicationRetryWakeUp.foreach(_.cancel())
							for {
								isReplicationSuccessful <- {
									if commitIndex < sccIndex then attemptToUpdateOtherParticipantsLogs(accessible0)
									else sequencer.LatchingTask_true
								}
								result <- {
									if isReplicationSuccessful && commitIndex >= sccIndex then sequencer.LatchingTask_true
									else if currentRole ne this then sequencer.LatchingTask_false
									else if isReplicationSuccessful then {
										Trace.trace(s"Appending a no-op record to be able to commit records of previous [[Term]] transitively.")
										for {
											primaryState2 <- primaryStateFence.advanceIf {
												case Inaccessible =>
													Maybe.empty
												case accessible1: Accessible =>
													Maybe(accessible1.withSingleRecordAppended(accessible0.currentTerm, LeaderTransition(accessible1.currentTerm)))
											}
											result <- replicateSccUntilSuccessOrLeaderRoleIsAbandoned(primaryState2, sccIndex, 0)
										} yield result
									} else {
										// Wait some time before trying again.
										val covenant = sequencer.Covenant[Boolean]()
										val token = requestWakeUp(WakeUpReason.ReplicationLoopRetry, attemptsDone, () => {
											if commitIndex >= sccIndex then covenant.fulfillUnsafe(true)
											else {
												Trace.trace(s"Updating role due to insufficient quorum when replicating records up-to-index $sccIndex. Attempt #$attemptsDone")
												for {
													_ <- updateRole()
													primaryState1 <- primaryStateFence.causalAnchor()
													result <- replicateSccUntilSuccessOrLeaderRoleIsAbandoned(primaryState1, sccIndex, attemptsDone + 1)
												} do covenant.fulfillUnsafe(result)
											}
										})
										sccReplicationRetryWakeUp = Maybe(token)
										covenant
									}
								}
							} yield result
					}
				}
			}

			def onCommandFromClient(clientCommand: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.LatchingTask[ResponseToClient] = {
				checkWithin()
				Trace.init(() => s"$boundParticipantId: Leader.onCommandFromClient") {
					var commandRecordIndex: RecordIndex = 0
					for {
						// First, append the command to the log if it wasn't already
						primaryState1 <- primaryStateFence.advanceIf { primaryState0 =>
							if currentRole ne this then Maybe.empty
							else primaryState0 match {
								case Inaccessible =>
									Maybe.empty
								case accessible0: Accessible =>
									val currentTerm = accessible0.currentTerm
									assert(currentTerm == leadedTerm) // Assumes that the demotion due to higher term seen is always applied synchronously within a section causally ordered by the primaryStateFence. See the CausalFence's game changing invariant.
									commandRecordIndex = accessible0.firstEmptyRecordIndex
									Maybe(accessible0.withSingleRecordAppended(currentTerm, CommandRecord(currentTerm, clientCommand)))
							}
						}
						// Second, replicate it if not already, and then, if replication was successful, apply the command to the state machine assuming it is idempotent.
						response <- handleCommandReplication(primaryState1, clientCommand, commandRecordIndex)
					} yield response
				}
			}

			private def handleCommandReplication(primaryState1: PrimaryState, clientCommand: ClientCommand, commandRecordIndex: RecordIndex)(using Trace.Context): sequencer.LatchingTask[ResponseToClient] = {
				// The role may have changed due to a failure while storing the primary state. In that case, delegate the handling to the current role. The appended command record will be overwritten when the new leader calls the append records RPC.
				if currentRole ne this then currentRole.onCommandFromClient(clientCommand, INTERNAL_VACATE_HANDOFF)
				else primaryState1 match {
					case Inaccessible =>
						// This should never happen, but just in case, respond appropriately.
						sequencer.LatchingTask_ready(Unable(LEADERSHIP_VACATED, cluster.getOtherProbableParticipants))

					case accessible1: Accessible =>
						assert(accessible1.currentTerm == leadedTerm) // Assumes that the demotion due to higher term seen is always applied synchronously within a section causally ordered by the primaryStateFence. See the CausalFence's game changing invariant.
						val logBufferOffset1 = accessible1.logBufferOffset
						for {
							isCommitSuccessful <- attemptToUpdateOtherParticipantsLogs(accessible1)
							response <- {
								// The role may have changed while atempting the replication. In that case, delegate the handling to the current role. The appended command record will be overwritten when the new leader calls the append records RPC.
								if currentRole ne this then currentRole.onCommandFromClient(clientCommand, INTERNAL_VACATE_HANDOFF)
								else if isCommitSuccessful then {
									// If the command is committed, apply it to the state machine to get the result, assuming the state machine handles idempotency and deduplication.
									for {
										_ <- decoupledCommandsApplierCompletion // Waits the committed-commands-applier to complete any work left by a previous role.
										response <- {
											// It is not necessary to have an updated primary state here because committed records are never mutated and we are not mutating the primary state here. We only need to know if we are still leading.
											if currentRole ne this then onCommandFromClient(clientCommand, INTERNAL_VACATE_HANDOFF)
											else for {
												_ <- applyCommittedCommands(accessible1, commandRecordIndex - 1, 0)
												response <- {
													if currentRole ne this then onCommandFromClient(clientCommand, INTERNAL_VACATE_HANDOFF)
													else for smr <- machine.applyClientCommand(commandRecordIndex, clientCommand) yield {
														highestAppliedCommandIndex = commandRecordIndex
														if commandRecordIndex - logBufferOffset1 > logCompactionThreshold then startLogCompaction()
														Processed(smr)
													}
												}
											} yield response
										}
									} yield response
								} else {
									// If not able to replicate then update the role and start again.
									Trace.trace(s"Updating role due to insufficient quorum when replicating a command record.")
									for {
										// TODO: Implement customizable direct Append retries (hybrid retry strategy). Instead of calling updateRole() immediately on replication failure, retry the Append RPC directly up to a configurable number of times (e.g., 1 or 2 attempts) to absorb transient glitches on the fast path before escalating to role check/demotion. Idealy, the retry delay should discount the failure timeout.
										_ <- updateRole()
										primaryState2 <- primaryStateFence.causalAnchor()
										response <- handleCommandReplication(primaryState2, clientCommand, commandRecordIndex)
									} yield response
								}
							}
						} yield response
				}
			}

			/** Attempts to append the [[Record]]s that this participant has, to the logs of the participants that lack them.\
			 * Detailed behavior:
			 *  - If [[Record]]s weren't appended to another participant, attempt to append them.
			 *    - If successful: update the corresponding entry of [[indexOfNextRecordToSend_ByParticipantIndex]] and [[highestRecordIndexKnownToBeAppended_ByParticipantIndex]].
			 *    - If AppendEntries fails because of log inconsistency: decrement the corresponding entry of [[indexOfNextRecordToSend_ByParticipantIndex]] and [[highestRecordIndexKnownToBeAppended_ByParticipantIndex]], and retry.
			 *  - If there exists an N such that N > [[commitIndex]], a majority of the [[highestRecordIndexKnownToBeAppended_ByParticipantIndex]] entries is ≥ N, and log[N].term == [[currentTerm]]: set commitIndex = N
			 *  - If there are unreachable participants (a minority whose corresponding entry in [[highestRecordIndexKnownToBeAppended_ByParticipantIndex]] trails the leader's [[commitIndex]]), initiate targeted retries to catch them up.
			 * @return a [[sequencer.LatchingTask]] that yields true if, and only if, all the following are true:
			 *  - the [[currentRole]] is not changed during this process;
			 *  - none of the responses has a higher [[Term]];
			 *  - for all the participants sets of the current [[Configuration]], all the records in this participant's log are successfully appended to at least:
			 *    - half of the other participants of the set, if this participant belongs to the set;
			 *    - a majority of the other participants of the set, if this participant does not belong to the set. */
			private def attemptToUpdateOtherParticipantsLogs(primaryState0: Accessible)(using Context): sequencer.LatchingTask[Boolean] = { // TODO coalesce calls with same argument.
				assert(primaryState0 eq primaryStateFence.committedState, s"$primaryState0 eq ${primaryStateFence.committedState}")
				// Set the serial number of this method execution.
				serialOfLastReplicationAttempt += 1
				val serialOfReplicationAttempt = serialOfLastReplicationAttempt
				Trace.step(() => s"attemptToUpdateOtherParticipantsLogs#$serialOfReplicationAttempt") {
					// Memorize the index after the top record to include in the appends produced by this replication process.
					val indexAfterTopRecordToSend = primaryState0.firstEmptyRecordIndex
					Trace.trace(s"starting replication until record index $indexAfterTopRecordToSend.")

					// Cancel the previous wake-up for retrying appends to unreachable followers, if any.
					unreachableFollowersRetryWakeUp.foreach(_.cancel())
					unreachableFollowersRetryWakeUp = Maybe.empty

					// Then, start regular replication to all followers.
					val config0 = deriveConfigurationFrom(primaryState0)
					// For every other participants, generate a task to replicate records this participant has and believes the others lack.
					val appendRequests_byParticipantIndex0 = config0.peers.mapWithIndex { (otherParticipantId, otherParticipantIndex0) =>
						val nextRecordToSend = indexOfNextRecordToSend_ByParticipantIndex(otherParticipantIndex0)
						appendsRecordsToParticipant(primaryState0, otherParticipantId, otherParticipantIndex0, nextRecordToSend, indexAfterTopRecordToSend)
					}
					val commitIndexAtAppendRequest = commitIndex
					for {
						// Execute the tasks in parallel.
						appendResponses0 <- sequenceAppendRequests(appendRequests_byParticipantIndex0)
						isReplicatedToMajority <- {

							if currentRole ne this then sequencer.LatchingTask_false
							else if handoffAndBumpTermIfLessThan(highestTermIn(appendResponses0)) ne this then sequencer.LatchingTask_false
							else for {
								primaryState1 <- primaryStateFence.causalAnchor()
								isReplicatedToMajority <- {
									if currentRole ne this then sequencer.LatchingTask_false
									else primaryState1 match {
										case Inaccessible =>
											sequencer.LatchingTask_false
										case accessible1: Accessible =>
											assert(accessible1.currentTerm == leadedTerm) // because the Leader Role should never bump the term before leaving transitioning to another Role.
											val config1 = deriveConfigurationFrom(accessible1)
											// Rearrange the responses according to the current configuration. Discard any responses from excluded participants.
											// TODO: Find a way for the [[RetirementDriver]]s created for those excluded participants (by `deriveConfigurationFrom`) to make use of the discarded responses. The only approach that comes to mind is passing those responses into `deriveConfigurationFrom`.
											val appendResponses1 = rearrangeAppendResponses(appendResponses0, config0, config1)
											val appendOutcomes1 = appendResponses1.mapWithIndex { (appendResponse, otherParticipantIndex) =>
												val otherParticipantId = config1.peers(otherParticipantIndex)
												// Handle the responses. Note that when successful, it updates the corresponding entry of the `indexOfNextRecordToSend_ByParticipantIndex` and `highestRecordIndexKnownToBeAppended_ByParticipantIndex` arrays.
												handleAppendResponse(accessible1, config1, otherParticipantId, otherParticipantIndex, appendResponse, primaryState0.currentTerm, indexAfterTopRecordToSend, commitIndexAtAppendRequest)
											}
											Trace.trace(s"The append responses until $indexAfterTopRecordToSend have been handled, excludingConfigIndex=$indexOfConfigChangeThatExcludedThisParticipant, aboutOthers=${(for i <- config1.peers.indices yield s"${config1.peers(i)}: outcome=${appendOutcomes1(i)}, nextToSend=${indexOfNextRecordToSend_ByParticipantIndex(i)}, knownAppended=${highestRecordIndexKnownToBeAppended_ByParticipantIndex(i)}, knowCommitted=${highestRecordIndexKnowToBeCommitted_ByParticipantIndex(i)}").mkString("[", "; ", "]")}") // TODO delete
											for maybeLastAppendAttemptInfo2 <- retryLaggingLearners(accessible1, config1, appendOutcomes1, indexAfterTopRecordToSend, false, false, serialOfReplicationAttempt)
												yield {
													// If the role changed while waiting the result or the number of successful appends isn't enough to achieve quorum, return false.
													if (currentRole ne this) || !maybeLastAppendAttemptInfo2.exists(_.quorumAchieved) then false
													// If the number of successful appends is enough to achieve quorum, then: update the `commitIndex`, retry laggards, and schedule retries for the unreachable participants, and yield true.
													else {
														// At this point, the result of the method is already determined (true). The following lines update derived state and start uncoupled retry processes.

														val (noParticipantIsLagging, isQuorumAchieved, appendOutcomes2a, accessible2, config2a) = maybeLastAppendAttemptInfo2.get
														// Update the commitIndex if a majority has replicated the uncommitted records.
														// If there exists an N such that N > commitIndex, the highest log-entry index known to be replicated is >= N for a majority of the servers, and getRecordAt[N].term == currentTerm: set commitIndex = N
														val previousCommitIndex = commitIndex
														commitIndex = config2a.indexOfTheCommittedRecordWithHighestIndex(accessible2, previousCommitIndex, IArray.unsafeFromArray(highestRecordIndexKnownToBeAppended_ByParticipantIndex), appendOutcomes2a)
														val config2b =
															if commitIndex == previousCommitIndex then config2a
															else {
																notifyListeners(_.onCommitIndexChanged(previousCommitIndex, commitIndex, LEADER, accessible2.currentTerm))
																// The active configuration depends on the commitIndex, so, update it
																deriveConfigurationFrom(accessible2)
															}

														// Rearrange the append outcomes if the active configuration changed due to a bump of the commitIndex.
														val appendOutcomes2b =
															if config2b eq config2a then appendOutcomes2a
															else {
																config2b.peers.mapWithIndex { (participantId, participantIndex) =>
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
															// If still leading
															maybeLastAppendAttemptInfo3.foreach { lastAppendAttemptInfo3 =>
																val config3 = lastAppendAttemptInfo3.updatedConfig
																Trace.trace(s"Final step: excludingConfigIndex=$indexOfConfigChangeThatExcludedThisParticipant, aboutOthers=${(for i <- config3.peers.indices yield s"${config3.peers(i)}: outcome=${lastAppendAttemptInfo3.lastAttemptOutcomes(i)}, nextToSend=${indexOfNextRecordToSend_ByParticipantIndex(i)}, knownAppended=${highestRecordIndexKnownToBeAppended_ByParticipantIndex(i)}, knowCommitted=${highestRecordIndexKnowToBeCommitted_ByParticipantIndex(i)}").mkString("[", "; ", "]")}") // TODO delete
																// If this leading participant is not included in the active configuration and all the followers in the new configuration have committed the StableConfigChange that excludes this participant, retire this participant.
																if isGhostAndAllFollowersCommittedTheExcludingConfigChange then {
																	// This point is reached if this leading participant was able to make the followers commit the StableConfigChange that excludes it before receiving an "appendRecords" call from the new leader (which is the other way to leave the gohst leader state)..
																	assert(config3.isInstanceOf[StableConfig]) // because exclusion is checked every record and transitional configurations are never more restrictive than the contiguos stable ones.
																	authorizeQuiescenceIfVanished(config3.asInstanceOf[StableConfig])
																	become(Retiring(leadedTerm, lastAppendAttemptInfo3.currentPrimaryState.getRecordTermAt(indexOfConfigChangeThatExcludedThisParticipant), indexOfConfigChangeThatExcludedThisParticipant, config3.electorate))
																}
																/// If not retiring and this method wasn't called again, then, for those minority of participants whose highestRecordIndexKnowToBeReplicated is less than the commitIndex (because append failed with IS_UNREACHABLE), retry the append records RPC. This retry is indefinite until the outer method (attemptToUpdateOtherParticipantsLogs) is called again (as the effect of an external stimulus).
																else if serialOfLastReplicationAttempt == serialOfReplicationAttempt then scheduleUnreachableParticipantsRetry(config3.peers, lastAppendAttemptInfo3.lastAttemptOutcomes, indexAfterTopRecordToSend, serialOfReplicationAttempt, 0)
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

			/** Creates an [[AppendRequest]] that appends the specified range of [[Record]]s from this participant log to the specified destination participant.
			 * CAUTION: requires that the derived state variables had been updated by applying the [[deriveConfigurationFrom]] method to the provided [[PrimaryState]].
			 * @param primaryState the current [[PrimaryState]]
			 * @param destinationParticipantId the [[ParticipantId]] of the [[ConsensusParticipant]] to append the records to.
			 * @param destinationParticipantIndex the index of the [[ParticipantId]] in the [[Configuration.peers]] array of the [[Configuration]] derived from the provided [[PrimaryState]].
			 * @param fromIndex the [[RecordIndex]] of the first [[Record]] to include in the [[ClusterParticipant.appendRecords]] call.
			 * @param untilIndex the [[RecordIndex]] after the last [[Record]] to include in the [[ClusterParticipant.appendRecords]] call. */
			private def appendsRecordsToParticipant(primaryState: Accessible, destinationParticipantId: ParticipantId, destinationParticipantIndex: Int, fromIndex: RecordIndex, untilIndex: RecordIndex)(using Trace.Context): AppendRequest = {
				Trace.step("appendsRecordsToParticipant") {
					// if the appending would be empty and with the same `leaderCommit` as a previous successful append, skip it and fake a successful response.
					if commitIndex == highestRecordIndexKnowToBeCommitted_ByParticipantIndex(destinationParticipantIndex) && untilIndex <= 1 + highestRecordIndexKnownToBeAppended_ByParticipantIndex(destinationParticipantIndex)
					then sequencer.Venture_successful(AppendResult(primaryState.currentTerm, 0, ISOLATED))
					else thisConsensusParticipant.appendRecordsToParticipant(primaryState, destinationParticipantId, fromIndex, untilIndex, commitIndex)
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

			/** Rearrange the elements of the provided array of [[AppendResponse]] in the same way as the elements of the [[Configuration.peers]] array changes when transitioning from the first provided [[Configuration]] to the second. */
			private def rearrangeAppendResponses(appendResponses: IArray[AppendResponse], from: Configuration, to: Configuration): IArray[AppendResponse | Null] = {
				if to eq from then appendResponses else {
					to.peers.mapWithIndex { (participantId, participantIndex) =>
						val indexFrom = from.participantIndexOf(participantId)
						if indexFrom < 0 then null
						else appendResponses(indexFrom)
					}
				}
			}

			/** Retry the [[ClusterParticipant.appendRecords]] call for each participant that needs earlier records or was not present in the [[Configuration]] when the previous attempt was done, until either:
			 * - `abortIfAnotherReplicationStarts` is true and another execution of [[attemptToUpdateOtherParticipantsLogs]] has been started after the one that called this method.
			 * - if `untilNoneLags` is `true`, strictly until there is no lagging follower.
			 * - else,	relaxedly until there is no lagging follower or a majority of the appends is successful.
			 *
			 * This method is called solely from [[attemptToUpdateOtherParticipantsLogs]] and twice: before and after the top sent record is committed.
			 * @param accessible1 the current, accessible, and causally anchored [[PrimaryState]] of this [[ConsensusParticipant]].
			 * @param config1 the current updated [[Configuration]] of this [[ConsensusParticipant]].
			 * @param previousAttemptAppendOutcomes the [[AppendOutcome]]s of the results of the previous [[ClusterParticipant.appendRecords]] attempt, stored as a parallel array with index correspondence to `config1.peers`.
			 * @param indexAfterTopRecordSent the [[RecordIndex]] after the top [[Record]] sent in the calls to [[ClusterParticipant.appendRecords]].
			 * @param untilNoneLags instructs if the retries must continue until no learner is lagging (true), or until a majority of the appends is successful (false).
			 * @param serialOfReplicationAttempt the serial number of the call to [[attemptToUpdateOtherParticipantsLogs]] that initiated this method execution.
			 * @return a [[sequencer.LatchingTask]] that yields either:
			 *  - [[Maybe.empty]] if the role changed while waiting the result:
			 *  - otherwise [[Maybe.some]] containing:
			 *    - a boolean telling if no participant is lagging,
			 *    - a boolean telling if quarum was achieved,
			 *    - the current [[PrimaryState]],
			 *    - the active [[Configuration]] derived from it,
			 *    - and an array of [[AppendOutcome]] values representing the responses to the [[ClusterParticipant.appendRecords]] calls, stored as a parallel array with index correspondence to the [[Configuration.peers]] array of the accompanying [[Configuration]].
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
			)(using Context): sequencer.LatchingTask[Maybe[(noParticipantIsLagging: Boolean, quorumAchieved: Boolean, lastAttemptOutcomes: IArray[AppendOutcome], currentPrimaryState: Accessible, updatedConfig: Configuration)]] = {
				Trace.step("retryLaggingLearners") {
					assert((accessible1 eq primaryStateFence.committedState) && (config1 eq latestDerivedConfig.get), s"$accessible1 eq ${primaryStateFence.committedState} && $config1 eq $latestDerivedConfig")
					// Trace.trace(s"retryLaggingLearners($accessible1, $config1, ${previousAttemptAppendOutcomes.mkString("[", ", ", "]")}, $indexAfterTopRecordSent, $untilNoneLags, $serialOfReplicationAttempt)") // TODO delete line

					val noParticipantIsLagging = previousAttemptAppendOutcomes.forallWithIndex((previousOutcome, _) => (previousOutcome & AO_IS_LAGGING_MASK) == 0)
					val quorumAchieved = config1.achievesQuorumWhen(previousAttemptAppendOutcomes)
					if noParticipantIsLagging
						|| !untilNoneLags && quorumAchieved
						|| abortIfAnotherReplicationStarts && serialOfReplicationAttempt != serialOfLastReplicationAttempt // If [[attemptToUpdateOtherParticipantsLogs]] was called again after the call that initiated this `retryLaggingLearners` execution, then there is no need to continue this execution because the later call to `attemptToUpdateOtherParticipantsLogs` will start a new one if necessary.
					then sequencer.LatchingTask_ready(Maybe((noParticipantIsLagging, quorumAchieved, previousAttemptAppendOutcomes, accessible1, config1)))
					else {
						val laggingParticipants: mutable.ArrayBuffer[ParticipantId] = new mutable.ArrayBuffer(config1.peers.length)

						val newAppendRequests: mutable.ArrayBuffer[AppendRequest] = new mutable.ArrayBuffer(config1.peers.length)
						config1.peers.foreachWithIndex { (participantId, participantIndex) =>
							if (previousAttemptAppendOutcomes(participantIndex) & AO_IS_LAGGING_MASK) != 0 then {
								laggingParticipants.addOne(participantId)
								val indexOfNextRecordToSend = indexOfNextRecordToSend_ByParticipantIndex(participantIndex)
								val appendRequest = appendsRecordsToParticipant(accessible1, participantId, participantIndex, indexOfNextRecordToSend, indexAfterTopRecordSent)
								newAppendRequests.addOne(appendRequest)
							}
						}
						// Trace.trace(s"retrying the lagging learners $laggingParticipants, newAppendRequests=$newAppendRequests") // TODO delete line
						val commitIndexAtAppendRequest = commitIndex
						for {
							newAppendResponses <- sequenceAppendRequests(newAppendRequests)
							newAppendAttemptInfo <- {
								if currentRole ne this then emptyLatchedTask
								else if handoffAndBumpTermIfLessThan(highestTermIn(newAppendResponses)) ne this then emptyLatchedTask
								else for {
									primaryState2 <- primaryStateFence.causalAnchor()
									newAppendAttemptInfo <- {
										// Trace.trace(s"newAppendResponses=${newAppendResponses.mkString("[", ", ", "]")}, primaryState2=$primaryState2") // TODO delete line
										if currentRole ne this then emptyLatchedTask
										else primaryState2 match {
											case Inaccessible => emptyLatchedTask
											case accessible2: Accessible =>
												if assertionsEnabled then assert(accessible2.currentTerm == leadedTerm) // because the Leader Role should never bump the term before transitioning to another Role.

												val config2 = deriveConfigurationFrom(accessible2)
												// Handle the new appends responses and merge the old and new summaries. Note that the appends are handled even if a later replication attempt was started.
												val newOutcomes: IArray[AppendOutcome] = config2.peers.mapWithIndex { (participantId, participantIndex2) =>
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
												Trace.trace(s"The append responses to the laggard-retry of replication #$serialOfReplicationAttempt until $indexAfterTopRecordSent have been handled, excludingConfigIndex=$indexOfConfigChangeThatExcludedThisParticipant, aboutOthers=${(for i <- config2.peers.indices yield s"${config2.peers(i)}: outcome=${newOutcomes(i)}, nextToSend=${indexOfNextRecordToSend_ByParticipantIndex(i)}, knownAppended=${highestRecordIndexKnownToBeAppended_ByParticipantIndex(i)}, knowCommitted=${highestRecordIndexKnowToBeCommitted_ByParticipantIndex(i)}").mkString("[", "; ", "]")}") // TODO delete
												retryLaggingLearners(accessible2, config2, newOutcomes, indexAfterTopRecordSent, untilNoneLags, abortIfAnotherReplicationStarts, serialOfReplicationAttempt)
										}
									}
								} yield newAppendAttemptInfo
							}
						} yield newAppendAttemptInfo
					}
				}
			}

			/** Handles the result of an [[ClusterParticipant.appendRecords]] call.
			 *
			 * Updates the [[indexOfNextRecordToSend_ByParticipantIndex]], [[highestRecordIndexKnownToBeAppended_ByParticipantIndex]], and [[highestRecordIndexKnowToBeCommitted_ByParticipantIndex]] arrays and maps the [[AppendResult]] to an [[AppendOutcome]].
			 * @param primaryState the current [[PrimaryState]].
			 * @param config the active [[Configuration]]. Assumes it is derived from `config`.
			 * @param participantId the identifier of the participant whose response is being handled.
			 * @param participantIndex the index of the participant in the [[Configuration.peers]] derived from the provided [[PrimaryState]].
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
								highestRecordIndexKnowToBeCommitted_ByParticipantIndex(participantIndex) = retireeExcludingConfigIndex
								indexOfNextRecordToSend_ByParticipantIndex(participantIndex) = appendResult.successOrIndexForNextAttempt
								highestRecordIndexKnownToBeAppended_ByParticipantIndex(participantIndex) = retireeExcludingConfigIndex
								// If the commitIndex of the retiree is higher than the commitIndex of this leading participant when append was called, then this leader was crowned during joint consensus with the commitIndex behind the retiree's and, therefore, needs to consider it as a wildcard-voter.
								// Else, the retiree is still retiring from a previous joint consensus and was included back but hasn't noticed yet. Therefore it needs missing records to catch up.
								if retireeExcludingConfigIndex > commitIndex then AO_IS_RETIRING else AO_NEEDS_EARLIER_RECORDS
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
								if assertionsEnabled then assert(indexForNextAttempt > highestRecordIndexKnownToBeCommited)
								indexOfNextRecordToSend_ByParticipantIndex(participantIndex) = indexForNextAttempt
								AO_NEEDS_EARLIER_RECORDS
							}
						}

					case Failure(e) =>
						Trace.debug(s"$boundParticipantId: The replication to $participantId failed with:", e)
						AO_IS_UNREACHABLE
				}
			}

			/**
			 * Schedule [[ClusterParticipant.appendRecords]] calls for the participants that were [[AO_IS_UNREACHABLE]] in the previous attempt.
			 * @param correspondingParticipantIds the [[ParticipantId]] corresponding to the provided [[AppendOutcome]]s array.
			 * @param previousAttemptOutcomes the [[AppendOutcome]]s of the previous attempt.
			 * @param indexAfterTopRecordToSend the index after the top [[Record]] of the set of [[Records]] that should be included in the attempts. */
			private def scheduleUnreachableParticipantsRetry(correspondingParticipantIds: IArray[ParticipantId], previousAttemptOutcomes: IArray[AppendOutcome], indexAfterTopRecordToSend: RecordIndex, serialOfReplicationAttempt: Int, attemptsDone: Int)(using Context): Unit = {
				Trace.step("scheduleUnreachableParticipantsRetry") {
					// if there are unreachable participants, request a wake-up to retry replicating the log to them.
					if serialOfReplicationAttempt == serialOfLastReplicationAttempt && previousAttemptOutcomes.contains(AO_IS_UNREACHABLE) then {
						val token = requestWakeUp(WakeUpReason.UnreachableFollowersRetry, attemptsDone, () => {
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
														val indexOfNextRecordToSend = indexOfNextRecordToSend_ByParticipantIndex(participantIndex)
														appendRequests.addOne(appendsRecordsToParticipant(accessible1, participantId, participantIndex, indexOfNextRecordToSend, indexAfterTopRecordToSend))
													}
												}
											}
											Trace.trace(s"Retrying the appendRecords RPCs to the unreachable participants $unreachableParticipantIds in replication #$serialOfReplicationAttempt until $indexAfterTopRecordToSend after #$attemptsDone failed attempts.")
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
															else handleAppendResponse(accessible2, config2, participantId, participantIndex, appendResult, accessible1.currentTerm, indexAfterTopRecordToSend, commitIndexAtAppendRequest)
														}
														Trace.trace(s"The append responses to the unreachable-retry of replication #$serialOfReplicationAttempt until $indexAfterTopRecordToSend have been handled, excludingConfigIndex=$indexOfConfigChangeThatExcludedThisParticipant, outcomes=${unreachableParticipantIds.zip(appendsOutcomes)}") // TODO delete
														scheduleUnreachableParticipantsRetry(IArray.from(unreachableParticipantIds), appendsOutcomes, indexAfterTopRecordToSend, serialOfReplicationAttempt, attemptsDone + 1)
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

			/** Consolidates the many [[sequencer.Venture]]s into a single [[sequencer.LatchingTask]] that yields an array with the results of the [[sequencer.Venture]]s.
			 * TODO this is inefficient because the pace is determined by the slowest. Implement it using a stram instead. */
			private inline def sequenceAppendRequests(appendRequests: scala.collection.IndexedSeq[AppendRequest]): sequencer.LatchingTask[IArray[AppendResponse]] = {
				for appendDialog <- sequencer.LatchingTask_sequenceVenturesToArray(appendRequests, true) yield IArray.unsafeFromArray(appendDialog)
			}

			def handoffAndBumpTermIfLessThan(seenTerm: Term)(using Context): Role = {
				Trace.step("handoffAndBumpTermIfLessThan") {
					if thisLeader.leadedTerm < seenTerm then {
						Trace.trace(s"About to hand-off due to a higher term seen.")
						become(HandingOff(thisLeader.leadedTerm, seenTerm, primaryStateFence))
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

		/** Orchestrates the synchronization of an excluded participant to prepare it for retirement.
		 *
		 * This driver performs a log-matching loop, invoking [[appendRecords]] on the target until its log contains the [[StableConfigChange]] record that triggered its exclusion.
		 * By passing a `leaderCommit` equal to the index of that configuration change, the driver ensures the remote participant's active [[Configuration]] transitions to the stable set that excludes it.
		 *
		 * In other words: pushes uncommitted records + leaderCommit to followers so they can enter [[Retiring]]; authorizeQuiescenceTo/PermitQuiesce grants them permission to transition from [[Retiring]] → [[Quiesced]]. Neither alone is sufficient.
		 *
		 * @param id the [[ParticipantId]] of the retiring participant.
		 * @param precedingTerm the term of the [[Record]] immediately preceding the first potentially unappended [[Record]].
		 * @param potentiallyUnappendedRecords a sequence of [[Record]]s starting from the lowest potentially unappended [[RecordIndex]], up to the [[StableConfigChange]] record.
		 * @param indexOfFirstPotentiallyUnappendedRecord the index of the first [[Record]] in the potentially unappended records sequence.
		 * @param configChangeIndex the index of the [[StableConfigChange]] record that caused the exclusión.
		 * @param configChangeTerm the term of the [[StableConfigChange]] record.
		 * @param indexOfNextRecordToSend the index of the next record to be replicated, upper-bounded by `configChangeIndex`.
		 * */
		private class RetirementDriver(
			id: ParticipantId,
			precedingTerm: Term,
			potentiallyUnappendedRecords: IArray[Record],
			indexOfFirstPotentiallyUnappendedRecord: RecordIndex,
			configChangeIndex: RecordIndex,
			configChangeTerm: Term,
			maybeSnapshot: Maybe[SnapshotData[ParticipantId]],
			private var indexOfNextRecordToSend: RecordIndex
		) {
			if assertionsEnabled then {
				assert(commitIndex >= configChangeIndex)
				assert(potentiallyUnappendedRecords.isEmpty || indexOfFirstPotentiallyUnappendedRecord + potentiallyUnappendedRecords.length - 1 == configChangeIndex)
				assert(indexOfNextRecordToSend >= indexOfFirstPotentiallyUnappendedRecord || maybeSnapshot.isDefined)
			}

			/** Starts the process that makes a retiring participant to append the records up to the [[StableConfigChange]] that caused its exclusión, passing a `leaderCommit` equal to the index of that same [[StableConfigChange]] record.
			 * This method is called immediately after this [[RetirementDriver]] instance is created and added to the [[retirementDriverByParticipantId]] map, which happens during a [[Configuration]] transition.
			 *
			 *  @param currentTerm0 the [[Term]] of the current [[PrimaryState]]. In the first call is the [[PrimaryState]] from which the [[Configuration]] transition that produced this [[RetirementDriver]] is derived. In the recursión calls is the [[PrimaryState.currentTerm]] when the [[AppendResponse]] arrived if the [[currentRole]] is [[StatefulRole]], or the highest term seen before leaving it. */
			def driveLoop(currentTerm0: Term, attemptsDone: Int)(using Context): Unit = {
				Trace.step("driveLoop") {

					Trace.trace(s"RetirementDriver($id, term=$precedingTerm, records=${potentiallyUnappendedRecords.mkString("[", ", ", "]")}, firstIndex=$indexOfFirstPotentiallyUnappendedRecord, changeIndex=$configChangeIndex, changeTerm=$configChangeTerm, indexOfNextRecordToSend=$indexOfNextRecordToSend).startAppendLoop(attemptsDone=$attemptsDone) called") // TODO delete line
					val inquire =
						if indexOfNextRecordToSend > configChangeIndex then {
							id.appendRecords(currentTerm0, configChangeIndex, configChangeTerm, IArray.empty, configChangeIndex, configChangeTerm)
						} else if indexOfNextRecordToSend < indexOfFirstPotentiallyUnappendedRecord then {
							id.installSnapshot(currentTerm0, maybeSnapshot.get, potentiallyUnappendedRecords, configChangeIndex, configChangeTerm)
						} else {
							val previousRecordIndex = indexOfNextRecordToSend - 1
							val previousRecordTerm = {
								if previousRecordIndex >= indexOfFirstPotentiallyUnappendedRecord then potentiallyUnappendedRecords((previousRecordIndex - indexOfFirstPotentiallyUnappendedRecord).toInt).term
								else if previousRecordIndex == indexOfFirstPotentiallyUnappendedRecord - 1 then precedingTerm
								else throw new IndexOutOfBoundsException(previousRecordIndex.toInt)
							}
							val records = potentiallyUnappendedRecords.drop((indexOfNextRecordToSend - indexOfFirstPotentiallyUnappendedRecord).toInt)
							id.appendRecords(currentTerm0, previousRecordIndex, previousRecordTerm, records, configChangeIndex, configChangeTerm)
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
											sequencer.LatchingTask_ready(currentTerm0)
								} do response match {
									case Success(appendResult) =>
										// If the appending was successful or the target is already retired, then: terminate this driver and, if this participant is retiring, authorized to become Quiesced by the incoming leader, and all the drivers have completed; become Quiesced.
										if appendResult.successOrIndexForNextAttempt == 0 || appendResult.roleOrdinal < JOINING then {
											retirementDriverByParticipantId.remove(id)
											becomeQuiescedIfEligible(configChangeIndex)
										}
										// If the appending was rejected due to an unreversible reason, terminate this driver, scribe a message and, if this participant is retiring, authorized to become Quiesced by the incoming leader, and all the drivers have completed; become Quiesced.
										else if appendResult.term > currentTerm1 then {
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
										if attemptsDone >= retiringParticipantMaxRetries then Trace.error(s"$boundParticipantId: The replication to the retiring participant $id is aborted because it failed too many times. The last attempt failure was:", e)
										else {
											val updatedAttemptsDone = attemptsDone + 1
											Trace.debug(s"$boundParticipantId: The replication attempt #$updatedAttemptsDone to the retiring participant $id failed with:", e)
											requestWakeUp(
												WakeUpReason.RetirementDriveRetry,
												attemptsDone,
												() => if retirementDriverByParticipantId.contains(id) then {
													currentRole match {
														case stateful: StatefulRole =>
															stateful.primaryStateFence.causalAnchor { (primaryState2, _) => driveLoop(primaryState2.currentTerm, updatedAttemptsDone) }
														case retiring: Retiring =>
															driveLoop(retiring.finalTerm, updatedAttemptsDone)
														case _ =>
															driveLoop(currentTerm1, updatedAttemptsDone)
													}
												}
											)
										}
									
								}
							}
						}
					}
				}
			}
		}

		/** Creates an [[AppendRequest]] that appends the specified range of [[Record]]s from this participant log to the specified destination participant.
		 * @param primaryState the current [[PrimaryState]]
		 * @param destinationParticipantId the [[ParticipantId]] of the [[ConsensusParticipant]] to append the records to.
		 * @param fromIndex the [[RecordIndex]] of the first [[Record]] to include in the [[ClusterParticipant.appendRecords]] call.
		 * @param untilIndex the [[RecordIndex]] after the last [[Record]] to include in the [[ClusterParticipant.appendRecords]] call.
		 * @param leaderCommit the `leaderCommit` argument for the [[ClusterParticipant.appendRecords]] call. */
		private def appendRecordsToParticipant(primaryState: Accessible, destinationParticipantId: ParticipantId, fromIndex: RecordIndex, untilIndex: RecordIndex, leaderCommit: RecordIndex)(using Trace.Context): AppendRequest = {
			Trace.step("appendsRecordsToParticipant") {
				// if the appending would be empty and with the same `leaderCommit` as a previous successful append, skip it and fake a successful response.
				if fromIndex >= primaryState.logBufferOffset then {
					val previousRecordIndex = fromIndex - 1
					val previousRecordTerm = primaryState.getRecordTermAt(previousRecordIndex)
					destinationParticipantId.appendRecords(
						primaryState.currentTerm,
						previousRecordIndex,
						previousRecordTerm,
						primaryState.getRecordsBetween(fromIndex, untilIndex),
						leaderCommit,
						primaryState.getRecordTermAt(leaderCommit)
					)
				} else {
					destinationParticipantId.installSnapshot(
						primaryState.currentTerm,
						primaryState.latestSnapshot.get,
						primaryState.getRecordsBetween(primaryState.logBufferOffset, untilIndex),
						leaderCommit,
						primaryState.getRecordTermAt(leaderCommit)
					)
				}
			}
		}

		/** Attempts to transition this participant to the [[QUIESCED]] role.\
		 * This check is performed whenever a potential prerequisite for quiescence is met (e.g., is retiring, a [[RetirementDriver]] finishes, or permission to quiesce is granted).\
		 * The transition only proceeds if the participant is in the [[RETIRING]] role, no [[RetirementDriver]] is active, and protocol permission was granted.\
		 * Three independent async processes must converge: (a) all RetirementDrivers must complete/be removed, (b) role must be RETIRING, (c) quiescence permission must be granted. And that these are fulfilled by different mechanisms (RetirementDriver.driveLoop, become(Retiring), authorizeQuiescenceTo). */
		private def becomeQuiescedIfEligible(indexOfExcludingConfigChange: RecordIndex)(using Trace.Context): Unit = {
			Trace.step("becomeQuiescedIfEligible") {
				if retirementDriverByParticipantId.isEmpty && nonAcknowledgedQuiescencePermissions.isEmpty && currentRole.ordinal == RETIRING && indexOfExcludingConfigChange <= indexOfStableConfigChangeForWhichQuiescenceWasPermitted
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
		}

		/** The [[PrimaryState]] value when this [[ConsensusParticipant]] does not have access to the [[Storage]] where the primary state is persisted. Either because it does not need it (gracefully [[Quiesced]]), is [[Starting]], or became [[Quiesced]] due to a failure. */
		private object Inaccessible extends PrimaryState {
			override val currentTerm: Term = PRE_INIT
			override val firstEmptyRecordIndex: RecordIndex = 0
		}

		/** Defines the [[PrimaryState]] when this [[ConsensusParticipant]] has access to the [[Storage]] where the primary state is persisted. */
		private final class Accessible(@publicInBinary protected val workspace: WS) extends PrimaryState { thisAccessible =>

			override val currentTerm: Term = workspace.getCurrentTerm
			override val firstEmptyRecordIndex: RecordIndex = workspace.firstEmptyRecordIndex

			/** CAUTION: This method accesses mutable state of the [[PrimaryState]], so it should be called either:
			 * - within an [[StatefulRole.primaryStateFence.advance]] section. In this case, the call must occur before or during the completion of the [[sequencer.LatchingTask]] returned by the function passed	to `advance`; once that task has completed, the causal fence is closed and later calls are unsafe.
			 * - within a causally anchored consumer (i.e. consumers subscribed to the [[sequencer.LatchingTask]] returned by either [[StatefulRole.primaryStateFence.advance]] or [[StatefulRole.primaryStateFence.causalAnchor]]). In this case, the call must occur synchronously during the consumer’s execution; it must not be deferred to code scheduled after the consumer has returned, since such deferred code would no longer be causally anchored. */
			def getRecordAt(index: RecordIndex): Record = {
				if index >= logBufferOffset then workspace.getRecordAt(index)
				else latestSnapshot.fold(throw IndexOutOfBoundsException(s"Record at index $index is below lower bound 1.")) { snapshot =>
					if index == snapshot.latestConfigChangeIndex then snapshot.latestConfigChange
					else throw IndexOutOfBoundsException(s"Record at index $index is below logBufferOffset=$logBufferOffset and is not the latest config change.")
				}
			}


			/** CAUTION: This method accesses mutable state of the [[PrimaryState]], so it should be called either:
			 * - within an [[StatefulRole.primaryStateFence.advance]] section. In this case, the call must occur before or during the completion of the [[sequencer.LatchingTask]] returned by the function passed	to `advance`; once that task has completed, the causal fence is closed and later calls are unsafe.
			 * - within a causally anchored consumer (i.e. consumers subscribed to the [[sequencer.LatchingTask]] returned by either [[StatefulRole.primaryStateFence.advance]] or [[StatefulRole.primaryStateFence.causalAnchor]]). In this case, the call must occur synchronously during the consumer’s execution; it must not be deferred to code scheduled after the consumer has returned, since such deferred code would no longer be causally anchored. */
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
			 * - within an [[StatefulRole.primaryStateFence.advance]] section. In this case, the call must occur before or during the completion of the [[sequencer.LatchingTask]] returned by the function passed	to `advance`; once that task has completed, the causal fence is closed and later calls are unsafe.
			 * - within a causally anchored consumer (i.e. consumers subscribed to the [[sequencer.LatchingTask]] returned by either [[StatefulRole.primaryStateFence.advance]] or [[StatefulRole.primaryStateFence.causalAnchor]]). In this case, the call must occur synchronously during the consumer’s execution; it must not be deferred to code scheduled after the consumer has returned, since such deferred code would no longer be causally anchored. */
			inline def getRecordsBetween(from: RecordIndex, until: RecordIndex): IArray[Record] =
				workspace.getRecordsBetween(from, until)

			/** @return the index of the last [[Record]] with term equal to, and index greater than, the provided [[Term]] and [[RecordIndex]]. Returns `after` if none is found.
			 * CAUTION: This method accesses mutable state of the [[PrimaryState] so it should be called either:
			 * - within an [[StatefulRole.primaryStateFence.advance]] section. In this case, the call must occur before or during the completion of the [[sequencer.LatchingTask]] returned by the function passed	to `advance`; once that task has completed, the causal fence is closed and later calls are unsafe.
			 * - within a causally anchored consumer (i.e. consumers subscribed to the [[sequencer.LatchingTask]] returned by either [[StatefulRole.primaryStateFence.advance]] or [[StatefulRole.primaryStateFence.causalAnchor]]). In this case, the call must occur synchronously during the consumer’s execution; it must not be deferred to code scheduled after the consumer has returned, since such deferred code would no longer be causally anchored. */
			def indexOfLastRecordWithTerm(term: Term, after: RecordIndex): RecordIndex = {
				val offset = workspace.logBufferOffset
				val boundedAfter = if after >= offset then after else offset
				val index = workspace.indexOfLastRecordWithTerm(term, boundedAfter)
				if index >= boundedAfter then index
				else workspace.latestSnapshot.fold(after) { snapshot =>
					if snapshot.lastIncludedRecordIndex > after && snapshot.lastIncludedRecordTerm == term then snapshot.lastIncludedRecordIndex
					else after
				}
			}

			/** CAUTION: This method accesses mutable state of the [[PrimaryState]], so it should be called either:
			 * - within an [[StatefulRole.primaryStateFence.advance]] section. In this case, the call must occur before or during the completion of the [[sequencer.LatchingTask]] returned by the function passed	to `advance`; once that task has completed, the causal fence is closed and later calls are unsafe.
			 * - within a causally anchored consumer (i.e. consumers subscribed to the [[sequencer.LatchingTask]] returned by either [[StatefulRole.primaryStateFence.advance]] or [[StatefulRole.primaryStateFence.causalAnchor]]). In this case, the call must occur synchronously during the consumer’s execution; it must not be deferred to code scheduled after the consumer has returned, since such deferred code would no longer be causally anchored. */
			inline def logBufferOffset: RecordIndex =
				workspace.logBufferOffset

			/** CAUTION: This method accesses mutable state of the [[PrimaryState]], so it should be called either:
			 * - within an [[StatefulRole.primaryStateFence.advance]] section. In this case, the call must occur before or during the completion of the [[sequencer.LatchingTask]] returned by the function passed	to `advance`; once that task has completed, the causal fence is closed and later calls are unsafe.
			 * - within a causally anchored consumer (i.e. consumers subscribed to the [[sequencer.LatchingTask]] returned by either [[StatefulRole.primaryStateFence.advance]] or [[StatefulRole.primaryStateFence.causalAnchor]]). In this case, the call must occur synchronously during the consumer’s execution; it must not be deferred to code scheduled after the consumer has returned, since such deferred code would no longer be causally anchored. */
			inline def indexOfLatestConfigChange: RecordIndex =
				workspace.indexOfLatestConfigChange

			/** CAUTION: This method accesses mutable state of the [[PrimaryState]], so it should be called either:
			 * - within an [[StatefulRole.primaryStateFence.advance]] section. In this case, the call must occur before or during the completion of the [[sequencer.LatchingTask]] returned by the function passed	to `advance`; once that task has completed, the causal fence is closed and later calls are unsafe.
			 * - within a causally anchored consumer (i.e. consumers subscribed to the [[sequencer.LatchingTask]] returned by either [[StatefulRole.primaryStateFence.advance]] or [[StatefulRole.primaryStateFence.causalAnchor]]). In this case, the call must occur synchronously during the consumer’s execution; it must not be deferred to code scheduled after the consumer has returned, since such deferred code would no longer be causally anchored. */
			inline def latestConfigChange: Maybe[ConfigChange[ParticipantId]] =
				workspace.latestConfigChange

			/** CAUTION: This method accesses mutable state of the [[PrimaryState]], so it should be called either:
			 * - within an [[StatefulRole.primaryStateFence.advance]] section. In this case, the call must occur before or during the completion of the [[sequencer.LatchingTask]] returned by the function passed	to `advance`; once that task has completed, the causal fence is closed and later calls are unsafe.
			 * - within a causally anchored consumer (i.e. consumers subscribed to the [[sequencer.LatchingTask]] returned by either [[StatefulRole.primaryStateFence.advance]] or [[StatefulRole.primaryStateFence.causalAnchor]]). In this case, the call must occur synchronously during the consumer’s execution; it must not be deferred to code scheduled after the consumer has returned, since such deferred code would no longer be causally anchored. */
			inline def informAppliedCommandIndex(appliedCommandIndex: RecordIndex): Unit = {
				workspace.informAppliedCommandIndex(appliedCommandIndex)
			}

			/** CAUTION: This method mutates of the [[PrimaryState]], so it should be called within the safe temporal windows provided by [[StatefulRole.primaryStateFence.advance]]. */
			def withTermUpdated(newTerm: Term)(using Trace.Context): sequencer.LatchingTask[PrimaryState] = {
				val currentTerm = workspace.getCurrentTerm
				if newTerm < currentTerm then sequencer.LatchingTask_ready(new Accessible(workspace))
				else if newTerm == currentTerm then sequencer.LatchingTask_ready(this)
				else {
					workspace.setCurrentTerm(newTerm)
					saveWorkspace()
				}
			}

			/** CAUTION: This method mutates of the [[PrimaryState]], so it should be called within the safe temporal windows provided by [[StatefulRole.primaryStateFence.advance]]. */
			def withSingleRecordAppended(term: Term, record: Record)(using Trace.Context): sequencer.LatchingTask[PrimaryState] = {
				workspace.setCurrentTerm(term)
				workspace.appendRecord(record)
				saveWorkspace()
			}

			/** Tries to fuse the provided batch of [[Records]] with the local log.
			 * @param term the [[Term]] of the provider of the batch of records.
			 * @param prevRecordIndex the index of the [[Record]] immediately before the first entry in the batch.
			 * @param prevRecordTerm the term of the [[Record]] immediately before the first entry in the batch.
			 * @param batch the [[Record]]s to fuse.
			 * @param report an [[IntRef]] whose value is mutated by this method to communicate the outcome flags: The [[FR_RECORD_FUSED]] bit is set if a record was fused; the [[FR_TERM_UPDATED]] is set if the local term was updated.
			 * @return a [[Maybe]] containing:
			 *  - a [[sequencer.LatchingTask]] that yields the updated [[Accessible]] after successfully saving it in the [[Storage]];
			 *  - a [[sequencer.LatchingTask]] that yields [[Inaccessible]] if the saving failed;
			 *  - nothing ([[Maybe.empty]]) if either earlier [[Record]]s are needed, the term mismatches, or the batch fully predates the latest snapshot. */
			def tryFusingRecords(term: Term, prevRecordIndex: RecordIndex, prevRecordTerm: Term, batch: IArray[Record], report: IntRef)(using Trace.Context): Maybe[sequencer.LatchingTask[PrimaryState]] = {
				val lbo = this.workspace.logBufferOffset
				val batchFirstRecordIndex = prevRecordIndex + 1L

				var indexOfFirstRecordToFuseInBatch: Int = 0
				// 1. Verify consistency assuming the Log Matching invariant holds.
				val isConsistent = {
					if prevRecordIndex >= lbo then prevRecordIndex < firstEmptyRecordIndex && prevRecordTerm == workspace.getRecordAt(prevRecordIndex).term
					else {
						val latestSnapshotLastIncludedRecordIndex = lbo - 1L
						val latestSnapshotLastIncludedRecordTerm = this.latestSnapshot.fold(PRE_INIT)(_.lastIncludedRecordTerm)
						if prevRecordIndex == latestSnapshotLastIncludedRecordIndex then prevRecordTerm == latestSnapshotLastIncludedRecordTerm
						else {
							val latestSnapshotLastIncludedRecordIndexInBatch = (latestSnapshotLastIncludedRecordIndex - batchFirstRecordIndex).toInt
							indexOfFirstRecordToFuseInBatch = latestSnapshotLastIncludedRecordIndexInBatch + 1
							latestSnapshotLastIncludedRecordIndexInBatch < batch.length && batch(latestSnapshotLastIncludedRecordIndexInBatch).term == latestSnapshotLastIncludedRecordTerm
						}
					}
				}

				val isTermUpdated = term > this.currentTerm
				if isTermUpdated then {
					report.elem |= FR_TERM_UPDATED
					workspace.setCurrentTerm(term)
				}
				if isConsistent then {
					report.elem |= FR_RECORD_FUSED
					if indexOfFirstRecordToFuseInBatch == 0 then {
						workspace.appendResolvingConflicts(batch, batchFirstRecordIndex)
					} else {
						val truncatedBatchLength = batch.length - indexOfFirstRecordToFuseInBatch
						val truncatedBatch = new Array[Record](truncatedBatchLength)
						System.arraycopy(batch, indexOfFirstRecordToFuseInBatch, truncatedBatch, 0, truncatedBatchLength)
						workspace.appendResolvingConflicts(IArray.unsafeFromArray(truncatedBatch), batchFirstRecordIndex + indexOfFirstRecordToFuseInBatch)
					}
				}
				if isConsistent || isTermUpdated then Maybe(saveWorkspace()) else Maybe.empty
			}

			/** Truncates the log's by replacing the earlier records (up to and including the provided [[RecordIndex]]) with the provided snapshot.\
			 * The snapshot must be taken immediately after the last [[ClientCommand]] of the removed records was applied.\
			 * CAUTION: This method mutates the [[PrimaryState]] so it should be called within the safe temporal windows provided by [[StatefulRole.primaryStateFence.advance]]. */
			def withLogTruncated(term: Term, lastIncludedRecordIndex: RecordIndex, stateMachineSnapshot: IArray[Byte])(using Trace.Context): sequencer.LatchingTask[PrimaryState] = {
				workspace.setCurrentTerm(term)
				workspace.truncateLogUpTo(lastIncludedRecordIndex, stateMachineSnapshot)
				saveWorkspace()
			}

			/** Replaces the whole log's with the provided snapshot followed with the provided records.\
			 * The snapshot must have been taken immediately after the last [[ClientCommand]] of the removed records was applied.\
			 * CAUTION: This method mutates the [[PrimaryState]] so it should be called within the safe temporal windows provided by [[StatefulRole.primaryStateFence.advance]]. */
			def withLogReplaced(term: Term, snapshot: SnapshotData[ParticipantId], tailRecords: IArray[Record])(using Trace.Context): sequencer.LatchingTask[PrimaryState] = {
				workspace.resetLog(snapshot, tailRecords)
				workspace.setCurrentTerm(term)
				saveWorkspace()
			}

			/** CAUTION: This method accesses mutable state of the [[PrimaryState]], so it should be called either:
			 * - within an [[StatefulRole.primaryStateFence.advance]] section. In this case, the call must occur before or during the completion of the [[sequencer.LatchingTask]] returned by the function passed	to `advance`; once that task has completed, the causal fence is closed and later calls are unsafe.
			 * - within a causally anchored consumer (i.e. consumers subscribed to the [[sequencer.LatchingTask]] returned by either [[StatefulRole.primaryStateFence.advance]] or [[StatefulRole.primaryStateFence.causalAnchor]]). In this case, the call must occur synchronously during the consumer’s execution; it must not be deferred to code scheduled after the consumer has returned, since such deferred code would no longer be causally anchored. */
			inline def latestSnapshot: Maybe[SnapshotData[ParticipantId]] =
				workspace.latestSnapshot


			/** CAUTION: This method mutates of the [[PrimaryState]], so it should be called within the safe temporal windows provided by [[StatefulRole.primaryStateFence.advance]]. */
			def withWorkspaceReleased(): sequencer.Task[Inaccessible.type] =
				workspace.releases.map(_ => Inaccessible)

			/** Saves the [[Workspace]] of this [[PrimaryState]] in the [[Storage]].
			 * CAUTION: This method accesses mutable state of the [[PrimaryState]], so it should be called within an [[StatefulRole.primaryStateFence.advance]] section only.
			 * @return the [[sequencer.LatchingTask]] that yields the saved [[PrimaryState]] */
			private def saveWorkspace()(using Trace.Context): sequencer.LatchingTask[PrimaryState] = {
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

			override def toString: String = s"Accessible(currentTerm=$currentTerm, firstEmptyRecordIndex=$firstEmptyRecordIndex, indexOfTopConfigChange=$indexOfLatestConfigChange)"
		}

		/** Knows which are the participants involved in the consensus and defines rules pertaining to elections and replication that govern the participant's behavior.\
		 * It has exactly two concrete subclasses:
		 *  - [[TransitionalConfig]]: Active during joint consensus (`Cold` ∪ `Cnew`).\
		 *     This behavior begins immediately once the transitional entry is appended to the participant's log.\
		 *     Replication and election quorum require majorities across both `Cold` and `Cnew`.\
		 *     Transitional behavior remains active until the corresponding [[StableConfigChange]] entry is replicated to a majority of both `Cold` and `Cnew` ([[commitIndex]] >= indexOfCorrespondingStableConfigChange).
		 *  - [[StableConfig]]: Active during non-joint consensus (Cnew).\
		 *     This behavior begins only once the backing [[StableConfigChange]] entry is committed (i.e. when [[commitIndex]] ≥ indexOfBackingStableConfigChange).\
		 *     Replication and election quorum require the majority of `Cnew` only.\
		 *     Non-leader `Cold` only participants having commited the [[StableConfigChange]] entry that excluded them, do transition to [[Retiring]] and wait authorization to quiesce from a stable leader of a succeeding term.\
		 *     A leader `Cold` only participant having commited the [[StableConfigChange]] entry that excluded it, do stay leading as ghost leader until either: it sees all the other participants had commited said [[StableConfigChange]]; or it receives either an "append records" or "authorization to quiesce" RPC from a leader of a higher term.\
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

			def indexOfTheCommittedRecordWithHighestIndex(primaryState: Accessible, from: RecordIndex, highestRecordIndexKnowToBeAppended_ByParticipantIndex: IArray[RecordIndex], appendOutcomes: IArray[AppendOutcome]): RecordIndex

			/** @param appendOutcomes the [[AppendOutcome]] of each other participant, indexed according to [[peers]].
			 * @return true if at least half of the [[AppendOutcome]]s in the provided array are [[AO_SUCCESS]]. */
			def achievesQuorumWhen(appendOutcomes: IArray[AppendOutcome]): Boolean

			/** Determines the [[Role]] to become based on the votes of all the participants. */
			def determineRole(primaryState: Accessible, primaryStateFence: CausalFence[PrimaryState, sequencer.type], myVote: Vote[ParticipantId], peerVotes: Array[Try[Vote[ParticipantId]]])(using Trace.Context): Maybe[Role]

			/**
			 * Determines the best leader candidate based on the [[StateInfo]]s of all the participants, including itself.
			 * This method only queries. Does not mutate anything.
			 *
			 * @param peersStateInfos the answers to the [[ClusterParticipant.howAreYou]] questions done to the other participants, stored as a parallel array with index correspondence [[peers]].
			 * @return A task that yields a [[Vote]] with the chosen leader for the current term.
			 */
			def decideMyVote(myStateInfo: StateInfo, peersStateInfos: IArray[StateInfo | Null])(using Trace.Context): Maybe[Vote[ParticipantId]]

			/** @return the index of the provided [[ParticipantId]] in the [[peers]]' [[IndexedSeq]] or a negative number if not present.
			 * @param otherParticipantId the id of the participant to find. */
			inline def participantIndexOf(otherParticipantId: ParticipantId): Int = {
				java.util.Arrays.binarySearch(peers.asInstanceOf[Array[ParticipantId]], otherParticipantId, participantIdComparator)
			}

			override def toString: String = backingConfigChange.toString
		}

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

			override def indexOfTheCommittedRecordWithHighestIndex(primaryState: Accessible, from: RecordIndex, highestRecordIndexKnowToBeAppended_ByParticipantIndex: IArray[RecordIndex], appendOutcomes: IArray[AppendOutcome]): RecordIndex = {
				var n = primaryState.firstEmptyRecordIndex - 1
				val othersQuorumThreshold = if isBoundIncluded then halfTheNumberOfParticipants else halfTheNumberOfParticipants + 1
				while n > from && (
					highestRecordIndexKnowToBeAppended_ByParticipantIndex.countWithIndex((hri, _) => hri >= n) < othersQuorumThreshold
						|| primaryState.getRecordTermAt(n) != primaryState.currentTerm // This second term or the `or` enforces Raft §5.4.2 ("A leader cannot determine commitment using entries from previous terms") and that a leader inheriting previous-term records cannot commit them without first committing a current-term record
					)
				do n -= 1
				n
			}

			override def achievesQuorumWhen(appendSummaries: IArray[AppendOutcome]): Boolean = {
				val othersAttendance = appendSummaries.countWithIndex { (summary, index) => summary == AO_SUCCESS }
				if isBoundIncluded then othersAttendance >= halfTheNumberOfParticipants else othersAttendance > halfTheNumberOfParticipants
			}

			override def determineRole(primaryState: Accessible, primaryStateFence: CausalFence[PrimaryState, sequencer.type], myVote: Vote[ParticipantId], peerVotes: Array[Try[Vote[ParticipantId]]])(using Trace.Context): Maybe[Role] = {
				var votesMatchingMyVoteCount = 1 // includes my vote
				var newParticipantsJoining = 0
				for case Success(replierVote) <- peerVotes do {
					if replierVote.votedId == myVote.votedId && replierVote.isNonBlank then votesMatchingMyVoteCount += 1
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

			override def indexOfTheCommittedRecordWithHighestIndex(primaryState: Accessible, from: RecordIndex, highestRecordIndexKnowToBeAppended_ByParticipantIndex: IArray[RecordIndex], appendOutcomes: IArray[AppendOutcome]): RecordIndex = {
				var n = primaryState.firstEmptyRecordIndex - 1
				while n > from do {
					// This if enforces Raft §5.4.2 ("A leader cannot determine commitment using entries from previous terms") and that a leader inheriting previous-term records cannot commit them without first committing a current-term record
					if primaryState.getRecordTermAt(n) == primaryState.currentTerm then {
						var oldParticipantsWithRecordAtNSuccessfullyAppended = 0
						var newParticipantsWithRecordAtNSuccessfullyAppended = 0

						var participantId = boundParticipantId
						var otherParticipantIndex = peers.length
						while otherParticipantIndex >= 0 do {
							if oldParticipants.contains(participantId) then oldParticipantsWithRecordAtNSuccessfullyAppended += 1
							if newParticipants.contains(participantId) then newParticipantsWithRecordAtNSuccessfullyAppended += 1

							var goNext = true
							otherParticipantIndex -= 1
							while otherParticipantIndex >= 0 && goNext do {
								participantId = peers(otherParticipantIndex)
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
				var otherParticipantIndex = peers.length
				while otherParticipantIndex >= 0 do {
					if oldParticipants.contains(participantId) then oldParticipantsWithSuccessfulAppendResult += 1
					if newParticipants.contains(participantId) then newParticipantsWithSuccessfulAppendResult += 1

					var goNext = true
					otherParticipantIndex -= 1
					while otherParticipantIndex >= 0 && goNext do {
						appendOutcomes(otherParticipantIndex) match {
							case AO_SUCCESS =>
								participantId = peers(otherParticipantIndex)
								goNext = false
							case AO_IS_RETIRING =>
								if oldParticipants.contains(peers(otherParticipantIndex)) then oldParticipantsRetiring += 1
								otherParticipantIndex -= 1
							case _ =>
								otherParticipantIndex -= 1
						}
					}
				}
				(oldParticipantsWithSuccessfulAppendResult + oldParticipantsRetiring > halfOfOldParticipants || oldParticipants.isEmpty)
					&& (newParticipantsWithSuccessfulAppendResult > halfOfNewParticipants || newParticipants.isEmpty)
			}

			override def determineRole(primaryState: Accessible, primaryStateFence: CausalFence[PrimaryState, sequencer.type], myVote: Vote[ParticipantId], peerVotes: Array[Try[Vote[ParticipantId]]])(using Trace.Context): Maybe[Role] = {
				var oldParticipantsVotesMatchingMyVote = 0
				var newParticipantsVotesMatchingMyVote = 0
				var oldParticipantsRetiring = 0
				var newParticipantsJoining = 0

				var participantVote = myVote
				var participantId = boundParticipantId
				var participantIndex = peerVotes.length
				while participantIndex >= 0 do {
					if participantVote.votedId == myVote.votedId && participantVote.isNonBlank then {
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
				Trace.debug(s"castedVote=$castedVote, contendants=$borrame") //TODO delete line
				castedVote
			}
		}

		/**
		 * Asks the [[Configuration.peers]] how they are ([[ClusterParticipant.howAreYou]]) in a coalesced manner: If an equivalent question is in flight, reuses the same pending [[sequencer.LatchingTask]] of the in-flight question; otherwise, a new request is done.
		 * Supports the forcing of answers.
		 *
		 * @param participantsIds the [[ParticipantId]]s of the target participants.
		 * @param stateInfo the [[StateInfo]] to put in the inquires.
		 * @param forcedAnswerByParticipantId the forced answers indexed by [[ParticipantId]].
		 * @return An [[IndexedSeq]] containing a [[sequencer.LatchingVenture]] for each [[ParticipantId]] in the provided array. Each [[sequencer.LatchingVenture]] element is the one returned by [[ClusterParticipant.howAreYou]] applied to the corresponding [[ParticipantId]] in the provided array, except the corresponding to the provided `idOfExcludedParticipant`, which yield the provided [[StateInfo]].
		 */
		private def askHowOtherParticipantsAre(participantsIds: IArray[ParticipantId], stateInfo: StateInfo, forcedAnswerByParticipantId: java.util.Map[ParticipantId, StateInfo]): IArray[sequencer.LatchingVenture[StateInfo]] = {
			participantsIds.mapWithIndex { (participantId, _) =>
				forcedAnswerByParticipantId.get(participantId) match {
					case null => coalescedHowAreYou.getOrStart((participantId, stateInfo), true)
					case forcedAnswer: StateInfo => sequencer.LatchingVenture_ready(Success(forcedAnswer))
				}
			}
		}

		private final def illegalStateQuiesce(detail: String = "")(using Trace.Context): Role = {
			Trace.step("illegalStateQuiesce") {
				val failure = new IllegalStateException("Should never happen. $detail")
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

		/** @param notificator a function that receives a [[NotificationListener]] and calls one of its methods. */
		private def notifyListeners(notificator: NotificationListener => Unit)(using Trace.Context): Unit = {
			checkWithin()
			notificationListeners.forEach { (listener, _) =>
				try notificator(listener)
				catch {
					case NonFatal(e) => Trace.error(s"$boundParticipantId: A notification listener threw:", e)
				}
			}
		}

		//// Just for efficiency ////

		@threadUnsafe private lazy val _emptyLatchingTask: sequencer.LatchingTask[Maybe[AnyRef]] = sequencer.LatchingTask_ready(Maybe.empty)

		/** An already completed [[sequencer.LatchingTask]] that yields [[Maybe.empty]].
		 * CAUTION: This @threadUnsafe lazy val does not guarantee a unique instance under concurrent access. Its use is only safe for logic that depends on the value's data, not its object identity. */
		private inline final def emptyLatchedTask[A]: sequencer.LatchingTask[Maybe[A]] = _emptyLatchingTask.asInstanceOf[sequencer.LatchingTask[Maybe[A]]]

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
