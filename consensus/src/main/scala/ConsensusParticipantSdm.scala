package readren.consensus

import ConsensusParticipantSdm.ConfigChangeResponse.*

import readren.common.*
import readren.sequencer.{Doer, MilliDuration, SchedulingExtension}

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
	final type Term = Int
	final type Ballot = Int

	final type ConfigChangeRequestId = String

	/** Type of the identifiers of the concrete [[Role]] subtypes. */
	opaque final type RoleOrdinal = Byte
	final val STOPPED: RoleOrdinal = 0
	final val RETIRING: RoleOrdinal = 1
	final val STARTING: RoleOrdinal = 2
	final val JOINING: RoleOrdinal = 4
	final val ISOLATED: RoleOrdinal = 8
	final val HANDING_OFF: RoleOrdinal = 9
	final val FOLLOWER: RoleOrdinal = 10
	final val PROMOTING: RoleOrdinal = 12
	final val LEADER: RoleOrdinal = 13
	extension (thisRoleOrdinal: RoleOrdinal) {
		def <(other: RoleOrdinal): Boolean = thisRoleOrdinal < other
		def <=(other: RoleOrdinal): Boolean = thisRoleOrdinal <= other
		def >(other: RoleOrdinal): Boolean = thisRoleOrdinal > other
		def >=(other: RoleOrdinal): Boolean = thisRoleOrdinal >= other
	}

	def RoleOrdinal_nameOf(ordinal: RoleOrdinal): String = {
		ordinal match {
			case STOPPED => "STOPPED"
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
	final val ER_NONE: ElectionRank = STOPPED
	/** The [[ElectionRank]] of the [[JOINING]] role, which is ineligible and does not participate in elections (votes for itself with term=0), but reduces quorum threshold by one. */
	final val ER_PRE_CANDIDATE: ElectionRank = JOINING
	/** The [[ElectionRank]] of the non-leading roles, which are eligible and fully participate in elections. */
	final val ER_CANDIDATE: ElectionRank = ISOLATED
	/** The [[ElectionRank]] of the leading roles, which are eligible and fully participate in elections, but should be chosen as leader by all voters provided the [[Term]] it exposes is the highest observed by the voter. */
	final val ER_LEADING: ElectionRank = PROMOTING

	inline def ElectionRank_from(ordinal: RoleOrdinal): ElectionRank = (ordinal & 0xFC).toByte

	def ElectionRank_nameOf(rank: ElectionRank): String = {
		rank match {
			case ER_NONE => "LOW"
			case ER_PRE_CANDIDATE => "LOW_JOINING"
			case ER_CANDIDATE => "MEDIUM"
			case ER_LEADING => "HIGH"
		}
	}

	enum ConfigChangeResponse {
		/** The requested configuration change was successfully completed. Only participants with the [[LEADER]]] role answer this. */
		case SUCCESSFULLY_CHANGED
		/** The participant has already changed to the requested configuration. Only participants with the [[LEADER]]] or [[FOLLOWER]] roles answer this. */
		case ALREADY_CHANGED
		/** The participant is currently transitioning to the requested configuration. Only participants with the [[LEADER]]] or [[FOLLOWER]] roles answer this. */
		case ALREADY_IN_PROGRESS
		/** The participant is the leader but is currently processing another change to a configuration different from the requested. Only participants with the [[LEADER]]] role answer this. */
		case WAIT_PREVIOUS_CHANGE_TO_COMPLETE
		/** The participant is a follower whose target configuration differs from the requested. Only participants in the [[FOLLOWER]] role answer this. */
		case ASK_THE_LEADER(leaderId: AnyRef)
		/** The participant is catching-up because it is joining. */
		case CATCHING_UP
		/** The tracking of the [[Configuration]] change request was lost due to a leader change after the first phase was started. The process may complete or not depending on which participant is promoted. If completed, the [[ConsensusParticipantSdm.ClusterParticipant.onActiveConfigChanged]] is called. If not, just silence. // TODO avoid the mentioned silence. */
		case REQUEST_TRACKING_LOST_AFTER_FIRST_PHASE_STARTED
		/** The tracking of the [[Configuration]] change request was lost due to a leader change after the first phase was commited (replicated to majority). The process will continue anyway. Listen to [[ConsensusParticipantSdm.ClusterParticipant.onActiveConfigChanged]] calls to observe when the process completes. */
		case REQUEST_TRACKING_LOST_AFTER_FIRST_PHASE_COMMITED
		/** The tracking of the [[Configuration]] change request was lost due to a leader change after the second phase was started. The process will continue anyway. Listen to [[ConsensusParticipantSdm.ClusterParticipant.onActiveConfigChanged]] calls to observe when the process completes. */
		case REQUEST_TRACKING_LOST_AFTER_SECOND_PHASE_STARTED
		/** The participant was excluded by a previous call to [[ConsensusParticipantSdm.ClusterParticipant.Delegate.requestConfigChange]]. */
		case EXCLUDED
		/** The participant role allows to become [[FOLLOWER]] or [[LEADER]] but is not able at the moment due to connectivity problems. */
		case UNABLE
	}

	/** Indicates the outcome of a client's previous attempt to send a command to a [[ConsensusParticipantSdm.ConsensusParticipant]].
	 * Used to annotate retry semantics and guide routing behavior. */
	type CommandAttemptFlag = Byte

	/** The client has not previously attempted to send the command to any participant. */
	final val FIRST_ATTEMPT: CommandAttemptFlag = 0

	/** The client previously sent the command to a different participant, which responded with a redirect instruction. */
	final val REDIRECTED: CommandAttemptFlag = 1

	/** The client previously sent the command to a different participant, which either failed or rejected the request. */
	final val FALLBACK: CommandAttemptFlag = 2

	val assertionsEnabled: Boolean = classOf[ConsensusParticipantSdm].desiredAssertionStatus()

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
	 * @param reachableCandidatesOfOldConf The number of candidates of the current or old set of participants that were reachable by the voter (including the voter itself) when the vote was cast. Zero means the voter is still initializing or was stopped.
	 * @param reachableCandidatesOfNewConf The number of candidates new the new set or participants that were reachable by the voter (including the voter itself) when the vote was cast. Zero means the voter is still initializing or was stopped.
	 * @param rank The [[ElectionRank]] of the [[Role]] of the voted candidate.
	 * @param ballot the election round to which this [[Vote]] belongs to, or zero to indicate this [[Vote]] is blank.
	 */
	final case class Vote[Id <: AnyRef](term: Term, votedId: Id, reachableCandidatesOfOldConf: Int, reachableCandidatesOfNewConf: Int, rank: ElectionRank, ballot: Ballot) {
		if assertionsEnabled then assert(rank != PROMOTING)

		override def toString: String = s"Vote(term=$term, votedId=$votedId, reachOld=$reachableCandidatesOfOldConf, reachNew=$reachableCandidatesOfNewConf, rank=${ElectionRank_nameOf(rank)}, ballot=$ballot)"
	}

	/** The result of an append operation.
	 * @see [[ConsensusParticipantSdm.ClusterParticipant.appendRecords]] and [[ConsensusParticipantSdm.ClusterParticipant.Delegate.onAppendRecords]].
	 * @param term The term of the follower that is responding.
	 * @param successOrIndexForNextAttempt contains the [[RecordIndex]] suggested for the next attempt if either:
	 *		- the participant that is responding is not ready (Starting or Stopped);
	 *		- the leader's term is less than the current term of the follower;
	 *		- or the follower's log does not contain a record at the `prevRecordIndex` whose term is `prevRecordTerm`.
	 *
	 * [[Maybe.empty]] otherwise.
	 * @param roleOrdinal The role of the follower that is responding.
	 */
	final case class AppendResult(term: Term, successOrIndexForNextAttempt: Maybe[RecordIndex], roleOrdinal: RoleOrdinal) {
		if assertionsEnabled then assert(roleOrdinal != PROMOTING)

		override def toString: String = s"AppendResult(@$term, ${successOrIndexForNextAttempt.fold("accepted")(feri => s"rejected, firstEmptyRecordIndex=$feri")}, ${RoleOrdinal_nameOf(roleOrdinal)}"
	}

	/**
	 * Information that a participant exposes about itself for the purpose of leader election.
	 * Other participants require this data to decide both their vote and their own role.
	 * This information is exposed not only on demand in the response to the question [[ConsensusParticipantSdm.ClusterParticipant.howAreYou]], but also proactively in some questions.
	 * @param currentTerm The term of the participant that.
	 * @param rank The [[ElectionRank]] of the [[ConsensusParticipantSdm.ConsensusParticipant.Role]] of the participant.
	 * @param termAtCommitIndex The term of the last commited record in the log of the participant that is answering.
	 * @param commitIndex The index of the last commited record in the log of the participant that is answering.
	 * @param ballot the election round to which this [[StateInfo]] belongs to.
	 */
	final case class StateInfo(currentTerm: Term, rank: ElectionRank, termAtCommitIndex: Term, commitIndex: RecordIndex, ballot: Ballot) {
		if assertionsEnabled then assert(currentTerm >= termAtCommitIndex)

		override def toString: String = s"StateInfo(@$currentTerm, ${ElectionRank_nameOf(rank)}, termAtCommitIndex=$termAtCommitIndex, commitIndex=$commitIndex, ballot=$ballot)"
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

	def retiringParticipantRetryPeriod: MilliDuration = 5000

	def retiringParticipantMaxRetries: Int = 9

	def isEager: Boolean = false

	/** Used for both, [[ISOLATED]] and [[HANDING_OFF]] roles. */
	def isolatedMainLoopInterval: MilliDuration = 500

	def candidateMainLoopInterval: MilliDuration = 500

	//	def leaderMainLoopInterval: MilliDuration = 500

	def failedReplicationsLoopInterval: MilliDuration = 500

	//// THREADING & TIMING

	/** The execution sequencer that [[ConsensusParticipant]] instances uses to mutate its state and schedule tasks.
	 *
	 * All methods that access mutable consensus state must be invoked through this sequencer to ensure deterministic,
	 * single-threaded execution. This coordination model avoids the need for blocking synchronization.
	 *
	 * The sequencer supports both ASAP and scheduled execution via [[Doer.executeSequentially]] and [[SchedulingExtension.scheduleSequentially]].
	 */
	val sequencer: Doer & SchedulingExtension

	inline def isInSequence: Boolean = sequencer.isInSequence

	//// STATE MACHINE

	/** Describes the interface that a [[ConsensusParticipant]] relies on to interact with the state machine. */
	trait StateMachine {
		/** Applies the given [[ClientCommand]] to the state machine.
		 * @return a [[sequencer.LatchedDuty]] that yields the [[StateMachineResponse]]
		 */
		def applyClientCommand(index: RecordIndex, command: ClientCommand): sequencer.LatchedDuty[StateMachineResponse]

		/** Returns a [[sequencer.Task]] that yields the [[RecordIndex]] most recently passed to [[applyClientCommand]] whose corresponding [[sequencer.LatchedDuty]] completed has been completed.
		 * If the implementation cannot determine this index, it should return zero, indicating that all commands must be replayed.
		 *
		 * This method is invoked only during recovery after restarts or persistence failures.
		 */
		def recoverIndexOfLastAppliedCommand: sequencer.LatchedDuty[RecordIndex]
	}

	//// RESPONSE TO CLIENT

	/** The response to a client command.
	 * @see [[ClusterParticipant.Delegate.onCommandFromClient]]. */
	sealed trait ResponseToClient

	/** The command was appended at the specified [[recordIndex]] to a majority of the participants persistent logs, and applied to the [[StateMachine]] which responded with the specified [[content]]. */
	final case class Processed(recordIndex: RecordIndex, content: StateMachineResponse) extends ResponseToClient

	/** The client has to repeat the command to the specified participant. This happens when the receiver is or becomes a [[FOLLOWER]]. */
	final case class RedirectTo(participantId: ParticipantId) extends ResponseToClient

	/** The participant is unable to process the command because it is [[ISOLATED]], [[STARTING]], or [[STOPPED]].
	 * After receiving this response, the client should try again with the [[otherParticipants]] with a flag indicating it is a fallback.
	 * @param roleOrdinal The role of the participant that is not able to reach consensus.
	 * @param otherParticipants The identifiers of alternative participants to try. This list is provided on a best-effort basis and may be incomplete or include servers that are stale or unavailable.
	 */
	final case class Unable(roleOrdinal: RoleOrdinal, otherParticipants: ListSet[ParticipantId]) extends ResponseToClient

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
	 *   - Exposing the identity of the participant it services via [[boundParticipantId]].
	 *   - Providing the initial cluster membership via [[getInitialParticipants]], which must return the same set across all participants listed.
	 *   - Acting as the source of truth for cluster membership and determining when a configuration change should be triggered.
	 *     This includes reacting to node join/leave events, quorum loss, scaling decisions, or health-based adjustments.
	 *   - Initiating configuration transitions by calling [[Delegate.requestConfigChange]] when a change is required.
	 *   - Routing inter-participant RPCs (e.g., [[howAreYou]], [[chooseALeader]], [[appendRecords]]) to the appropriate [[MessageListener]] methods.
	 *   - Delivering client commands and consensus messages to the bound [[ConsensusParticipant]] via a registered [[MessageListener]],
	 *     ensuring all invocations occur within the [[sequencer]] thread.
	 *
	 * Each [[ClusterParticipant]] instance is tightly bound to a single [[ConsensusParticipant]] instance.
	 * If a cluster-service had to service more than one [[ConsensusParticipant]] instance simultaneously, it would have to create a different instance of [[ClusterParticipant]] for each.
	 */
	trait ClusterParticipant {

		/** The implementation should return the identifier of the participant that this [[ClusterParticipant]] service — and its bound [[ConsensusParticipant]] service — are responsible for. */
		val boundParticipantId: ParticipantId

		/** The implementation should return  the identifiers of the consensus participants in the cluster formation, when all participants are brand-new (empty logs).
		 * This method is called by the bounded [[ConsensusParticipant]] when started for the first time ([[Workspace.isBrandNew]] returns true); and must return exactly the same set across all participants listed.
		 * The returned set must include the identifier of the participant serviced by this [[ClusterParticipant]] instance.
		 */
		def getInitialParticipants: Set[ParticipantId]

		/** The implementation should return the identifiers of the participants that this [[ClusterParticipant]] optimistically suspect are members of the consensus set, excluding the bound one.
		 * This method is called when the [[ConsensusParticipant]] that is [[STARTING]] or [[STOPPED]] has to respond [[Unable]] to a client. */
		def getOtherProbableParticipant: ListSet[ParticipantId]

		/** Called by the bounded [[ConsensusParticipant]] to notify that its active cluster-configuration has changed and now expects connectivity with the given set/sets of participants.
		 *
		 * This method is invoked upon application of a [[ConfigChange]]-typed [[Record]].
		 * Given configuration changes is a two-phase process, a call to [[Delegate.requestConfigChange]] causes two [[ConfigChange]] records to be applied and, therefore, two calls tho this method. This happens in all the involved [[ConsensusParticipant]] services.
		 * Successive calls with the same argument may occur. Implementations may ignore such calls only if no intervening call with a different argument has occurred — i.e., if the configuration has not changed.
		 * @param change The [[ConfigChange]] of participants that the consensus layer expects to be reachable.
		 */
		def onActiveConfigChanged(change: ConfigChange[ParticipantId], roleOrdinal: RoleOrdinal): Unit


		/** Called by the bounded [[ConsensusParticipant]] after it stops to allow the cluster-service exposing this interface to release the resources dedicated to it. */
		def onStopped(motive: Try[String]): Unit

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
			def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.LatchedDuty[ResponseToClient]

			/**
			 * This method is invoked by this cluster when another participant calls [[howAreYou]].
			 *
			 * Must be called within the [[sequencer]].
			 * @param inquirerId The id of the participant that called [[howAreYou]].
			 * @param inquirerInfo The [[StateInfo]] of the participant that called [[howAreYou]].
			 * @return The state information of the destination participant.
			 */
			def onHowAreYou(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.LatchedDuty[StateInfo]

			/**
			 * This method is invoked by this cluster when another participant calls [[chooseALeader]].
			 *
			 * Must be called within the [[sequencer]].
			 * @param inquirerId The id of the participant that called [[chooseALeader]].
			 * @param inquirerInfo Information about the state of the participant that called.
			 * @return A [[sequencer.Task]] that yields a [[Vote]] indicating the candidate chosen by the listening participant for the specified term.
			 */
			def onChooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.LatchedDuty[Vote[ParticipantId]]

			/**
			 * This method is invoked by this cluster when another participant calls [[appendRecords]].
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
			def onAppendRecords(inquirerId: ParticipantId, inquirerTerm: Term, prevRecordIndex: RecordIndex, prevRecordTerm: Term, records: GenIndexedSeq[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.LatchedDuty[AppendResult]

			/** Allows the [[ClusterParticipant]] service to request changes to the set of consensus-participants.
			 * Usually called whenever the set of consensus-participants has forcefully changed (i.e: a cluster-member included in the current consensus-participants-set went down) or is about to change (i.e: a node intended to be part of consensus-participants-set joined the cluster, or is going to leave the cluster for maintenance).
			 * To improve availability during planned cluster-membership transitions, the manager of the planed change should do the following:
			 *		1 call this method on every consensus-participant service to ensure the leader gets noticed, // TODO this is awkward. Make the configuration-change request be propagated to the leader when received by non-leaders.
			 *		2 wait until either:
			 *			- the returned [[LatchedDuty]] yields either [[SUCCESSFULLY_CHANGED]] or [[ALREADY_CHANGED]] for any of the consensus-participants,
			 *			- or the [[onActiveConfigChanged]] is called in any of the consensus-participants with the provided request identifier or desired participants set.
			 *
			 * @param requestId an identifier chosen by the caller that will be propagated up to the invocations of the [[onActiveConfigChanged]] method of each of the [[ClusterParticipant]] instances bounded to the involved [[ConsensusParticipant]] services.
			 * @param desiredParticipantsSet the identifiers of the participants that are going to seek consensus from now on.
			 * @return a [[sequencer.Duty]] that yields:
			 *         [[SUCCESSFULLY_CHANGED]] if the requested change was successfully completed.
			 *         [[ALREADY_CHANGED]] if the requested change is already done or in progress.
			 *         [[ALREADY_IN_PROGRESS]] if the participant is currently transitioning to the requested configuration 
			 *         [[ASK_THE_LEADER]] if none of the previous bullet is true and the [[ConsensusParticipant]] is a [[FOLLOWER]].
			 *         [[WAIT_PREVIOUS_CHANGE_TO_COMPLETE]] if the participant is the leader but is currently processing a change to a configuration different from the requested.  
			 *         [[UNABLE]] if the participant is not able to become neither the [[LEADER]] nor a [[FOLLOWER]]
			 *         - currently the leader or a follower that already has the desired participants set as the current or scheduled one;
			 *         - currently the leader and was able to replicate the corresponding [[TransitionalConfigChange]] to a majority according to that same [[TransitionalConfigChange]] rules. */
			def requestConfigChange(requestId: ConfigChangeRequestId, desiredParticipantsSet: Set[ParticipantId]): sequencer.LatchedDuty[ConfigChangeResponse]
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
		def load: sequencer.LatchedDuty[Try[WS]]

		/** Saves the workspace to the persistence storage.
		 * Design Note: A failure to save the workspace should restart the [[ConsensusParticipant]] as if it had crashed and lost all non-persistent variables.
		 */
		def save(workspace: WS): sequencer.LatchedDuty[Try[Unit]]
	}

	//// NOTIFICATIONS


	trait NotificationListener {
		def onStarting(previous: RoleOrdinal, isSeed: Boolean): Unit

		def onStarted(previous: RoleOrdinal, term: Term, initialConfigChange: ConfigChange[ParticipantId], isSeed: Boolean): Unit

		def onBecameStopped(previous: RoleOrdinal, term: Term, motive: Try[String]): Unit

		def onJoining(previous: RoleOrdinal): Unit

		def onBecameIsolated(previous: RoleOrdinal, term: Term): Unit

		def onBecameCandidate(previous: RoleOrdinal, term: Term): Unit

		def onBecameFollower(previous: RoleOrdinal, term: Term, leaderId: ParticipantId): Unit

		def onPromoting(previous: RoleOrdinal, term: Term): Unit

		def onBecameLeader(previous: RoleOrdinal, term: Term): Unit

		def onHandingOff(term: Term): Unit

		def onRetiring(term: Term): Unit

		def onRoleLeft(left: RoleOrdinal, term: Term): Unit

		def onCommitIndexChanged(previous: RecordIndex, current: RecordIndex): Unit

		def onActiveConfigChanged(currentRole: RoleOrdinal, currentTerm: Term, configChangeIndex: RecordIndex, configChange: ConfigChange[ParticipantId]): Unit
	}

	/**
	 * A convenience [[NotificationListener]] implementation with no-op methods.
	 * Extend this class and override only the methods you need.
	 */
	open class DefaultNotificationListener extends NotificationListener {
		override def onStarting(previous: RoleOrdinal, isSeed: Boolean): Unit = ()

		override def onStarted(previous: RoleOrdinal, term: Term, initialConfigChange: ConfigChange[ParticipantId], isSeed: Boolean): Unit = ()

		override def onBecameStopped(previous: RoleOrdinal, term: Term, motive: Try[String]): Unit = ()

		override def onJoining(previous: RoleOrdinal): Unit = ()

		override def onBecameIsolated(previous: RoleOrdinal, term: Term): Unit = ()

		override def onBecameCandidate(previous: RoleOrdinal, term: Term): Unit = ()

		override def onBecameFollower(previous: RoleOrdinal, term: Term, leaderId: ParticipantId): Unit = ()

		override def onPromoting(previous: RoleOrdinal, term: Term): Unit = ()

		override def onBecameLeader(previous: RoleOrdinal, term: Term): Unit = ()

		override def onHandingOff(term: Term): Unit = ()

		override def onRetiring(term: Term): Unit = ()

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
	 * - Each participant operates in one of several behavioral states depending on his role: starting, isolated, candidate, follower, leader, or stopped.
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
	 */
	class ConsensusParticipant(cluster: ClusterParticipant, storage: Storage, machine: StateMachine, isSeed: Boolean, initialListeners: Iterable[NotificationListener]) { thisConsensusParticipant =>

		import cluster.*

		private type AppendOutcome = Int
		private inline val AO_IS_LAGGING_MASK = 16
		private inline val AO_SUCCESS = 0
		private inline val AO_NEEDS_EARLIER_RECORDS = 1 | AO_IS_LAGGING_MASK
		private inline val AO_MISSING_BECAUSE_PARTICIPANT_WAS_NOT_PART_OF_THE_CONFIGURATION = 2 | AO_IS_LAGGING_MASK
		private inline val AO_IS_STARTING = 3 | AO_IS_LAGGING_MASK
		private inline val AO_SKIPPED_BECAUSE_OUT_OF_CONFIGURATION = 4
		private inline val AO_IS_RETIRING_OR_STOPPING = 5
		private inline val AO_NEEDS_RECORDS_THAT_PREDATE_SNAPSHOT = 6
		private inline val AO_IS_UNREACHABLE = 7
		private inline val AO_UNEXPECTED = 8

		/** The index of the highest entry known to be committed according to this participant.
		 * A log record is committed once the leader that created the record has replicated it on a majority of the participants.
		 * This also commits all preceding records in the leader’s log, including records created by previous leaders.
		 * CAUTION: this is state derived from the [[PrimaryState]]. Therefore, when the [[currentRole]] is a [[StatefulRole]], this variable should be accessed only within consumers synchronously subscribed to the [[sequencer.LatchedDuty]] returned by the [[sequencer.CausalFence.advance]]-like and [[sequencer.CausalFence.causalAnchor]] methods applied to the [[StatefulRole.primaryStateFence]].
		 * */
		private var commitIndex: RecordIndex = 0

		/** The index of the [[CommandRecord]] with the highest index whose command was successfully applied to the [[StateMachine]] of this [[ConsensusParticipant]].
		 * CAUTION: this is state derived from the [[PrimaryState]]. Therefore, when the [[currentRole]] is a [[StatefulRole]], this variable should be accessed only within consumers synchronously subscribed to the [[sequencer.LatchedDuty]] returned by the [[sequencer.CausalFence.advance]]-like and [[sequencer.CausalFence.causalAnchor]] methods applied to the [[StatefulRole.primaryStateFence]].
		 * */
		private var highestAppliedCommandIndex: RecordIndex = 0

		/** The current role of this [[ConsensusParticipant]].
		 * @note The [[currentRole]] state is neither entirely derived from the [[PrimaryState]] nor orthogonal to it. They are interrelated. */
		private var currentRole: Role = Starting(isSeed)

		/** The current, not causally anchored, [[Configuration]] of this [[ConsensusParticipant]].
		 * To obtain a causally anchored [[Configuration]] use [[StatefulRole.deriveConfigurationFrom]] instead.
		 * This variable is updated by the [[StatefulRole.deriveConfigurationFrom]] method.
		 * CAUTION: this is state derived from the [[PrimaryState]]. Therefore, when the [[currentRole]] is a [[StatefulRole]], this variable should be accessed only within consumers synchronously subscribed to the [[sequencer.LatchedDuty]] returned by the [[sequencer.CausalFence.advance]]-like and [[sequencer.CausalFence.causalAnchor]] methods applied to the [[StatefulRole.primaryStateFence]].
		 * */
		private var currentConfig: Configuration = NoConfig

		/** Knows the [[RetireeAgent]]s corresponding to the participant that were excluded from the configuration and potentially have not received the appends to notice it can leave. */
		private val retireeAgentByParticipantId: mutable.Map[ParticipantId, RetireeAgent] = mutable.Map.empty

		/** The current election round.
		 * Should be bumped whenever the part of the state of this participant that is exposed in questions to other participants (term and commitIndex as of this writing) changes.
		 * CAUTION: this is state derived from the [[PrimaryState]]. Therefore, when the [[currentRole]] is a [[StatefulRole]], this variable should be accessed only within consumers synchronously subscribed to the [[sequencer.LatchedDuty]] returned by the [[sequencer.CausalFence.advance]]-like and [[sequencer.CausalFence.causalAnchor]] methods applied to the [[StatefulRole.primaryStateFence]].
		 * */
		private var currentBallot: Ballot = 1

		/** Stores the last [[StateInfo]] instance returned by [[Role.buildMyStateInfo]]
		 * CAUTION: this is state derived from the [[PrimaryState]]. Therefore, when the [[currentRole]] is a [[StatefulRole]], this variable should be accessed only within consumers synchronously subscribed to the [[sequencer.LatchedDuty]] returned by the [[sequencer.CausalFence.advance]]-like and [[sequencer.CausalFence.causalAnchor]] methods applied to the [[StatefulRole.primaryStateFence]].
		 * */
		private var stateInfoExposedInLastInteraction: StateInfo = StateInfo(0, ER_NONE, 0, 0, 0)

		/** Memorizes the [[StateInfo]] of the other participants during an election round.
		 * Contained instances' [[StateInfo.ballot]] field should match the [[currentBallot]].
		 * Entry values are never overwritten with a [[StateInfo]] instance with equal or lower [[StateInfo.ballot]].
		 * CAUTION: this is state derived from the [[PrimaryState]]. Therefore, when the [[currentRole]] is a [[StatefulRole]], this variable should be accessed only within consumers synchronously subscribed to the [[sequencer.LatchedDuty]] returned by the [[sequencer.CausalFence.advance]]-like and [[sequencer.CausalFence.causalAnchor]] methods applied to the [[StatefulRole.primaryStateFence]].
		 * TODO make values be [[Covenant]]s of [[StateInfo]] so that received questions that include a [[StateInfo]] fulfill the howAreYou questions done by this participant. */
		private val convergingParticipantStates: java.util.Map[ParticipantId, StateInfo] = new java.util.HashMap()

		/** Memorices the schedule that is currently scheduled. Needed to cancel the schedule when transitioning to a non-equivalent [[Role]] instance. */
		private var currentSchedule: Maybe[sequencer.Schedule] = Maybe.empty

		private var decoupledCommandsApplierIsRunning: Boolean = false

		private val notificationListeners: java.util.WeakHashMap[NotificationListener, None.type] = new util.WeakHashMap()

		private val participantIdComparator = new Comparator[ParticipantId] {
			private val ordering = summon[Ordering[ParticipantId]]

			override def compare(a: ParticipantId, b: ParticipantId): Int = ordering.compare(a, b)
		}

		private val coalescedHowAreYou = sequencer.CoalescedQuery[(otherParticipantId: ParticipantId, stateInfo: StateInfo), StateInfo](params =>
			sequencer.Commitment_triggerAndWire(params.otherParticipantId.howAreYou(params.stateInfo))
		)

		{
			initialListeners.foreach(notificationListeners.put(_, None))
			cluster.setBound(currentRole)
			currentRole.onEnter(currentRole)
		}

		/** @return the ordinal of the current behavior. */
		inline def getRoleOrdinal: RoleOrdinal = currentRole.ordinal

		/** @return a [[sequencer.Duty]] that stops this [[ConsensusParticipant]] instance. */
		def stops: sequencer.Duty[Unit] = {
			sequencer.Duty_mineFlat { () =>
				val stopped = Stopped(Success("This ConsensusParticipant instance was forcefully stopped."))
				become(stopped)
				stopped.completed
			}
		}

		/** @return a [[sequencer.Duty]] that stops and disposes this [[ConsensusParticipant]] instance. */
		def disposes: sequencer.Duty[Unit] = {
			stops.andThen { _ =>
				notificationListeners.clear()
				currentSchedule.foreach(sequencer.cancel(_))
				cluster.removeBound()
			}
		}

		/**
		 *  Synchronously transitions this [[ConsensusParticipant]]'s [[Role]] to the provided one.
		 */
		private def become(newRole: Role): Role = {
			checkWithin()
			if !newRole.isEquivalentTo(currentRole) then {
				currentRole.onLeave()
				currentSchedule.foreach(sequencer.cancel(_))
				currentSchedule = Maybe.empty
				val previousRole = currentRole
				val committedTerm = currentRole.getCommittedPrimaryState.currentTerm
				notifyListeners(_.onRoleLeft(previousRole.ordinal, committedTerm))
				currentRole = newRole
				cluster.setBound(newRole)
				newRole.onEnter(previousRole)
			}
			currentRole
		}

		private inline def startNewBallot(): Unit = {
			currentBallot += 1
			convergingParticipantStates.clear()
		}

		private def updateConvergingStates(seenParticipant: ParticipantId, seenStateInfo: StateInfo): Unit = {
			if seenStateInfo.ballot > currentBallot then {
				currentBallot = seenStateInfo.ballot
				convergingParticipantStates.clear()
				convergingParticipantStates.put(seenParticipant, seenStateInfo)
			} else if seenStateInfo.ballot == currentBallot && !convergingParticipantStates.containsKey(seenParticipant) then {
				convergingParticipantStates.put(seenParticipant, seenStateInfo)
			}
		}

		/**
		 * Abstract base class for the consensus participant behaviors at each role.
		 *
		 * Each subtype represents a different role in the consensus algorithm and implements the message handling logic specific to that role.
		 * Roles can transition to other roles based on received messages and internal logic.
		 *
		 * It also implements behavior that is common to the [[STARTING]] and [[STOPPED]] roles.
		 *
		 * All [[Role]] methods are executed within the sequencer thread to ensure thread safety.
		 */
		private sealed abstract class Role extends Delegate { thisRole =>
			/** The ordinal corresponding to this [[Role]] */
			val ordinal: RoleOrdinal
			val rank: ElectionRank = ElectionRank_from(ordinal)

			@threadUnsafe lazy val blankVote: Vote[ParticipantId] = Vote(0, boundParticipantId, 0, 0, this.rank, 0)
			@threadUnsafe lazy val yieldsBlankVote: sequencer.LatchedDuty[Vote[ParticipantId]] = sequencer.LatchedDuty_ready(blankVote)

			def isEquivalentTo(other: Role): Boolean

			/** Called by [[become]] after the previous [[Role]]'s [[Role.onLeave]] method has returned, and the [[currentRole]] variable set to this [[Role]] instance.
			 * This method is suitable to enqueue primary state updates that must happen before any updates enqueued after [[become]] returns. */
			def onEnter(previous: Role): Unit

			/** Called by [[become]] before transitioning to another role. */
			def onLeave(): Unit = ()

			/** Updates the derived state that is stored in the [[Role]] instance and depends on the current [[Configuration]]. Only the [[Leader]] role has such state as this writing. */
			def onActiveConfigChanged(currentPrimaryState: Accessible, currentConfig: Configuration, newConfig: Configuration, indexOfNewConfigChange: RecordIndex): Unit = ()

			def getCommittedPrimaryState: PrimaryState = Inaccessible

			/** Returns a [[StateInfo]] that reflects the current state of the bounded participant; and, if the returned value differs from the one returned in the previous call (stored in [[stateInfoExposedInLastInteraction]]), bumps the [[currentBallot]] and clears the [[convergingParticipantStates]]. */
			def buildMyStateInfo(primaryState: PrimaryState): StateInfo = buildStatelessInfo()

			/** Returns a [[StateInfo]] that indicates disability to participate; and, if the returned value differs from [[stateInfoExposedInLastInteraction]], bumps the [[currentBallot]] and clears the [[convergingParticipantStates]]. */
			protected def buildStatelessInfo(): StateInfo = {
				val rememberedInfo = stateInfoExposedInLastInteraction
				val newInfo =
					if rememberedInfo.currentTerm == 0 && rememberedInfo.rank == this.rank && rememberedInfo.termAtCommitIndex == 0 && rememberedInfo.commitIndex == 0 then {
						if rememberedInfo.ballot == currentBallot then rememberedInfo else StateInfo(0, this.rank, 0, 0, currentBallot)
					} else {
						currentBallot += 1
						convergingParticipantStates.clear()
						StateInfo(0, rank, 0, 0, currentBallot)
					}
				stateInfoExposedInLastInteraction = newInfo
				newInfo
			}

			/**
			 * Asks the other participants how they are and decides which should be the leader based on their answers.
			 * This method may update the [[PrimaryState.currentTerm]] and [[currentRole]].
			 * <s>This method leaves the causally anchored [[PrimaryState]] committed (because no asynchronous operation is done after anchoring to it). So, consumers subscribed synchronously to the returned [[sequencer.LatchedDuty]] can obtain the causally anchored [[PrimaryState]] from [[primaryStateFence.committedState]]. See the [[sequencer.CausalFence]] game changing invariant.</s>
			 *
			 * @param primaryState0 a causally anchored [[PrimaryState]]
			 * @param isExclusionConsidered instructs if the [[Vote]] decision criteria must consider if the bounded participant is or isn't included in the active [[Configuration]]. If `true` and the bounded participant is excluded, a blank vote is yielded.
			 * @return A [[sequencer.LatchedDuty]] that yields a [[Vote]] with the chosen leader.
			 */
			def decideMyVote(primaryState0: PrimaryState, isExclusionConsidered: Boolean, blankVoteIfRoleChanges: Boolean): sequencer.LatchedDuty[Vote[ParticipantId]] = {
				yieldsBlankVote
			}

			override def onHowAreYou(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.LatchedDuty[StateInfo] = {
				checkWithin()
				updateConvergingStates(inquirerId, inquirerInfo)
				sequencer.LatchedDuty_ready(buildStatelessInfo())
			}

			override def onChooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.LatchedDuty[Vote[ParticipantId]] = {
				checkWithin()
				updateConvergingStates(inquirerId, inquirerInfo)
				yieldsBlankVote
			}

			override def onAppendRecords(inquirerId: ParticipantId, inquirerTerm: Term, prevRecordIndex: RecordIndex, prevRecordTerm: Term, records: GenIndexedSeq[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.LatchedDuty[AppendResult] = {
				checkWithin()
				sequencer.LatchedDuty_ready(AppendResult(0, Maybe(0), ordinal))
			}
		}

		/** Partial implementation of [[Role]]s that have state.
		 * All concrete subclasses of [[Role]] except [[STARTING]] and [[STOPPED]] extend this abstract class.
		 * @param primaryStateFence the [[sequencer.CausalFence]] that must be used to ensure causal ordering of the state updates. It must be propagated to subsequent [[StatefulRole]] instances. */
		private abstract class StatefulRole(val primaryStateFence: sequencer.CausalFence[PrimaryState]) extends Role {

			private type TermRef = IntRef
			/** The default argument for the [[updateTermIfLessThan]] method's second parameter.
			 * It is private and defined in the same class as the [[primaryStateFence]] to ensure that the contained [[Term]] variable reflects the expected value provided it is read within the synchronous part of a synchronously subscribed consumer to the [[LatchedDuty]] returned by [[updateTermIfLessThan]]. See the game-changing-invariant in [[Doer.CausalFence]]. */
			protected final val defaultPreviousTermRef: TermRef = new TermRef(0)

			override def isEquivalentTo(other: Role): Boolean =
				other.ordinal == this.ordinal && (other.asInstanceOf[StatefulRole].primaryStateFence eq this.primaryStateFence)

			override final def getCommittedPrimaryState: PrimaryState =
				primaryStateFence.committedState

			override final def buildMyStateInfo(primaryState: PrimaryState): StateInfo = {
				primaryState match {
					case Inaccessible =>
						buildStatelessInfo()

					case accessible: Accessible =>
						val rememberedInfo = stateInfoExposedInLastInteraction
						val termAtCommitIndex = accessible.getRecordTermAt(commitIndex)
						val newInfo =
							if rememberedInfo.currentTerm == primaryState.currentTerm && rememberedInfo.rank == this.rank && rememberedInfo.termAtCommitIndex == termAtCommitIndex && rememberedInfo.commitIndex == commitIndex then {
								if rememberedInfo.ballot == currentBallot then rememberedInfo else StateInfo(accessible.currentTerm, this.rank, termAtCommitIndex, commitIndex, currentBallot)
							} else {
								currentBallot += 1
								convergingParticipantStates.clear()
								StateInfo(accessible.currentTerm, rank, termAtCommitIndex, commitIndex, currentBallot)
							}
						stateInfoExposedInLastInteraction = newInfo
						newInfo
				}
			}

			override def decideMyVote(primaryState0: PrimaryState, isExclusionConsidered: Boolean, blankVoteIfRoleChanges: Boolean): sequencer.LatchedDuty[Vote[ParticipantId]] = {
				checkWithin()

				val config0 = deriveConfigurationFrom(primaryState0)
				// If this participant is not included in the current configuration
				if isExclusionConsidered && !config0.allParticipants.contains(boundParticipantId) then yieldsBlankVote
				else {
					val myStateInfo0 = buildMyStateInfo(primaryState0)
					val howAreYouQuestions0 = askHowOtherParticipantsAre(config0.allOtherParticipants, myStateInfo0, convergingParticipantStates)
					for {
						howAreYouAnswers0 <- sequencer.LatchedDuty_sequenceTasksToArray(howAreYouQuestions0, true)
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
							// Update the convergingParticipantStates (by filling the missing entries with the StateInfo instances in the answers), and count the failed answers.
							val numberOfFailedAnswers = IArray.unsafeFromArray(howAreYouAnswers0).foldLeftWithIndex(0) { (failedAnswersCounter, answer, participantIndex) =>
								val participantId = config0.allOtherParticipants(participantIndex)
								answer match {
									case Success(info) =>
										updateConvergingStates(participantId, info)
										failedAnswersCounter

									case _: Failure[StateInfo] =>
										if convergingParticipantStates.containsKey(participantId) then failedAnswersCounter
										else failedAnswersCounter + 1
								}
							}

							if blankVoteIfRoleChanges && currentRole != this then currentRole.yieldsBlankVote
							else currentRole match {
								case sf: StatefulRole =>
									val config1 = deriveConfigurationFrom(primaryState1)
									// if the bounded participant is excluded and its exclusion is considered, yield a blank vote.
									if isExclusionConsidered && !config1.allParticipants.contains(boundParticipantId) then currentRole.yieldsBlankVote
									// ask again if either, the active configuration changed while waiting the responses to the howAreYou questions, or a successful answer has an obsolete ballot.
									else if (config1 ne config0) || convergingParticipantStates.size + numberOfFailedAnswers < config0.allOtherParticipants.length then {
										scribe.trace(s"$boundParticipantId: Deciding my vote again due to ${if config1 ne config0 then "a concurrent configuration change" else "an obsolete answer"}, isExclusionConsidered=$isExclusionConsidered")
										// TODO analyze if convergingParticipantStates should be cleared here.
										sf.decideMyVote(primaryState1, isExclusionConsidered, blankVoteIfRoleChanges)
									}
									// else, decide the vote based on the responses.	
									else {
										sequencer.LatchedDuty_ready(config1.decideMyVote(currentRole.buildMyStateInfo(primaryState1), convergedParticipantStates(config1)))
									}

								case _ =>
									currentRole.yieldsBlankVote
							}
						}
					} yield myVote
				}
			}

			override def onHowAreYou(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.LatchedDuty[StateInfo] = {
				checkWithin()
				for primaryState1 <- updateTermIfLessThan(inquirerInfo.currentTerm) yield {
					updateConvergingStates(inquirerId, inquirerInfo)
					currentRole.buildMyStateInfo(primaryState1)
				}
			}

			override def onChooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.LatchedDuty[Vote[ParticipantId]] = {
				checkWithin()
				val term0Ref = new TermRef(0)
				for {
					// if the term is stale, update it persistently before interacting with other participants so that they see this participant with its updated and persisted state.
					primaryState1 <- updateTermIfLessThan(inquirerInfo.currentTerm, term0Ref)
					myVote <- {
						updateConvergingStates(inquirerId, inquirerInfo)
						currentRole.decideMyVote(primaryState1, true, false)
					}
				} yield myVote
			}

			/**
			 * Handles an AppendEntries RPC from the leader, attempting to reconcile log state and apply committed [[Record]]s.
			 *
			 * This method performs the following steps:
			 *
			 *   - Rejects the request if:
			 *     - This participant is still starting or was stopped (`ordinal < ISOLATED`).
			 *     - The leader's term is stale (`inquirerTerm < currentTerm`).
			 *     - The leader's `prevRecordIndex` does not match the term at that index locally.
			 *     - This participant state transitions to a non-receptive one while updating this participant consensus state due to a configuration change [[Record]] among the received [[Record]]s that should be commited.
			 *
			 *   - Appends new records from the leader, resolving any log conflicts, and, if the leader's term is newer than this participant's current one, also updates `currentTerm` to `inquirerTerm`.
			 *
			 *   - If the term is updated (in the previous bullet) or this participant is not yet a follower, starts the role-update process in a decoupled manner.
			 *
			 *   - Updates the [[commitIndex]] as the minimum of `leaderCommit` and the index of the last appended record.
			 *
			 *   - If this participant state haven't changed to a no receptive one (`ordinal <= STARTING`) while waiting the application of commited [[Record]]s of the kind that update this participant consensus state (like [[TransitionalConfigChange]] and [[TransitionalConfigChange]]), then :
			 *     - Persists the updated workspace via `storage.saves`.
			 *     - On failure to persist, transitions to `Stopped` and returns a failed result.
			 *
			 *   - Starts, in a decoupled manner, the process that silently applies committed commands to the state machine in log order.
			 *
			 * @param inquirerId         ID of the leader sending the AppendEntries request
			 * @param inquirerTerm       Term of the leader
			 * @param prevRecordIndex    Index of the record preceding the new entries
			 * @param prevRecordTerm     Term of the preceding record
			 * @param records            New records to append
			 * @param leaderCommit       Commit index reported by the leader
			 * @return a [[sequencer.LatchedDuty]] yielding the [[AppendResult]] where:
			 *         - `success` is true if, and only if, all the following are true when the appending was processed (specifically, when this participant's `primaryStateFence` was crossed):
			 *         		- the [[PrimaryState]] is valid;
			 *         		- `inquirerTerm >= currentTerm`;
			 *         		- the role is either ISOLATED or FOLLOWER;
			 *         		- the term of the log record at `prevRecordIndex` is equal to `prevRecordTerm`;
			 *         - `term = max(inquirerTerm, currentTerm)` when the appending was processed (causal fence crossed).
			 *         - `roleOrdinal` tells which was the role of this participant when the appending was processed (causal fence crossed).
			 */
			override def onAppendRecords(inquirerId: ParticipantId, inquirerTerm: Term, prevRecordIndex: RecordIndex, prevRecordTerm: Term, records: GenIndexedSeq[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.LatchedDuty[AppendResult] = {
				checkWithin()
				var primaryUpdateResult: (isTermBumped: Boolean, appendSuccess: Boolean) = null
				for primaryState1 <- primaryStateFence.advanceIf(
					(primaryState0, _) => primaryState0 match {
						case Inaccessible =>
							primaryUpdateResult = (isTermBumped = false, appendSuccess = false)
							Maybe.empty

						case accessible0: Accessible =>
							val currentTerm = accessible0.currentTerm
							if inquirerTerm < currentTerm then {
								primaryUpdateResult = (isTermBumped = false, appendSuccess = false)
								Maybe.empty
							} else {
								val isTermBumped = inquirerTerm > currentTerm
								val appendSuccess =
									JOINING <= currentRole.ordinal && (isTermBumped || currentRole.ordinal <= FOLLOWER)
										&& prevRecordIndex < accessible0.firstEmptyRecordIndex
										&& prevRecordTerm == accessible0.getRecordTermAt(prevRecordIndex)
								primaryUpdateResult = (isTermBumped, appendSuccess)
								if appendSuccess && records.nonEmpty then Maybe(accessible0.withRecordsAppended(inquirerTerm, records, prevRecordIndex + 1))
								else if isTermBumped then Maybe(accessible0.withTermUpdated(inquirerTerm))
								else Maybe.empty
							}
					},
					true
				)
				yield {

					primaryState1 match {
						case Inaccessible =>
							illegalStateStop()
							AppendResult(0, Maybe(0), currentRole.ordinal)

						case accessible1: Accessible =>
							val successOrIndexForNextAttempt =
								if primaryUpdateResult.appendSuccess then Maybe.empty
								else if accessible1.firstEmptyRecordIndex < prevRecordIndex then Maybe(accessible1.firstEmptyRecordIndex)
								else Maybe(prevRecordIndex)
							val response = AppendResult(primaryState1.currentTerm, successOrIndexForNextAttempt, currentRole.ordinal)
							// At this point the primary state is already updated and the response already determined. All the following actions are causally-anchored derived-state updates.

							val previousCommitIndex = commitIndex
							if primaryUpdateResult.appendSuccess then {
								// Update the commitIndex
								commitIndex = if leaderCommit < accessible1.firstEmptyRecordIndex then leaderCommit else accessible1.firstEmptyRecordIndex - 1
								// if the commitIndex is bumped then:
								if commitIndex != previousCommitIndex then {
									// notify the change
									notifyListeners(_.onCommitIndexChanged(previousCommitIndex, commitIndex))
									// start the "apply commited commands" process if it isn't already started.
									if !decoupledCommandsApplierIsRunning then startApplyingCommitedCommands(accessible1)
								}
							}

							if primaryUpdateResult.appendSuccess || primaryUpdateResult.isTermBumped then {
								val cro = currentRole.ordinal

								// get the updated configuration. Note that this must be done after updating the commitIndex.
								val config1 = deriveConfigurationFrom(accessible1)


								// If not joining or the catching-up is complete then:
								if cro != JOINING || commitIndex == leaderCommit then {
									// If this participant belongs to the active configuration, then:
									if config1.allParticipants.contains(boundParticipantId) then {
										// Become follower of the inquirer if it belongs to the active configuration.
										if config1.allOtherParticipants.contains(inquirerId) then become(Follower(accessible1.currentTerm, inquirerId, primaryStateFence))
										// Become candidate if this participant is joining, the catching-up is complete, and the inquirer is not in the active configuration.
										else if cro == JOINING then become(Isolated(primaryStateFence))
										// Keep the current role otherwise.
									}
									// If this participant does not belong to the active configuration, then retire it.
									else become(Retiring(accessible1.currentTerm, config1.allParticipants))
								}
							}
							response
					}
				}
			}

			/** Applies commited [[CommandRecord]]s silently and in a decoupled manner until reaching [[commitIndex]].
			 * @param primaryState the causally anchored [[PrimaryState]]. */
			private def startApplyingCommitedCommands(primaryState: Accessible): Unit = {
				decoupledCommandsApplierIsRunning = true

				def applyCommitedCommandsLoop(): sequencer.LatchedDuty[Unit] = {
					if highestAppliedCommandIndex >= commitIndex then sequencer.LatchedDuty_unit
					else {
						val indexOfCommandToApply = highestAppliedCommandIndex + 1
						primaryState.getRecordAt(indexOfCommandToApply) match {
							case command: CommandRecord[ClientCommand] @unchecked =>
								for {
									_ <- machine.applyClientCommand(indexOfCommandToApply, command.command)
									_ <- {
										highestAppliedCommandIndex = indexOfCommandToApply
										primaryState.informAppliedCommandIndex(indexOfCommandToApply)
										applyCommitedCommandsLoop()
									}
								} yield ()
							case _ =>
								highestAppliedCommandIndex = indexOfCommandToApply
								primaryState.informAppliedCommandIndex(indexOfCommandToApply)
								applyCommitedCommandsLoop()
						}
					}
				}

				val applyCommitedCommands =
					if highestAppliedCommandIndex > 0 then applyCommitedCommandsLoop()
					else {
						for {
							index <- machine.recoverIndexOfLastAppliedCommand
							_ <- {
								highestAppliedCommandIndex = index
								primaryState.informAppliedCommandIndex(index)
								applyCommitedCommandsLoop()
							}
						} yield ()
					}
				applyCommitedCommands.andThen(_ => decoupledCommandsApplierIsRunning = false)
			}

			/** @inheritdoc
			 * Calls to this method are dispatched here when the [[Role]] is either [[Isolated]] or [[Follower]]. */
			override def requestConfigChange(requestId: ConfigChangeRequestId, desiredParticipants: Set[ParticipantId]): sequencer.LatchedDuty[ConfigChangeResponse] = {
				if assertionsEnabled then assert(currentRole.ordinal != PROMOTING)
				for {
					_ <- {
						if currentRole.ordinal >= FOLLOWER then sequencer.LatchedDuty_unit
						else {
							scribe.trace(s"$boundParticipantId: Updating role from ${RoleOrdinal_nameOf(currentRole.ordinal)} due to a configuration change request. ")
							updateRole(true)
						}
					}
					primaryState <- primaryStateFence.causalAnchor()
					response <- {
						if currentRole.ordinal >= PROMOTING then currentRole.requestConfigChange(requestId, desiredParticipants)
						else if currentRole.ordinal == FOLLOWER then {
							val updatedConfig = deriveConfigurationFrom(primaryState)
							val response =
								if desiredParticipants != updatedConfig.desiredParticipants then ASK_THE_LEADER(currentRole.asInstanceOf[Follower].leaderId)
								else if updatedConfig.isInstanceOf[StableConfig] then ALREADY_CHANGED
								else ALREADY_IN_PROGRESS
							sequencer.LatchedDuty_ready(response)
						} else sequencer.LatchedDuty_ready(UNABLE)
					}
				} yield response
			}


			//// Role updaters

			/** Starts a process that updates the [[currentRole]] and [[PrimaryState.currentTerm]] based on the [[StateInfo]]s returned by calling [[ClusterParticipant.howAreYou]] on the other participants and, if necessary, also based on the [[Vote]]s returned by calling [[ClusterParticipant.chooseALeader]] on them.
			 * This process always ends immediately after a call to [[become]] returns. So, its [[Role]] outcome can be seen in the [[currentRole]] derived state variable.
			 * The [[currentRole]] is updated only if the desired one if not [[isEquivalentTo]] the [[currentRole]]. If updated, any other in-flight [[updateRole]] process is canceled and immediately completed.
			 * Many of this processes can be running simultaneously.
			 * @param isExclusionConsidered instructs if the [[Vote]] decision criteria must consider if the bounded participant is or isn't included in the active [[Configuration]]. If `true` and the bounded participant is excluded then the [[currentRole]] becomes [[Isolated]].
			 * */
			def updateRole(isExclusionConsidered: Boolean): sequencer.LatchedDuty[Unit] = {
				checkWithin()

				/**
				 * @param stateAtRequest the [[PrimaryState]] when the [[howAreYou]] calls were done. Needed to notice if the [[PrimaryState]] mutates during the decision of this participant [[Vote]]. */
				def updateRoleKnowingMyVote(stateAtRequest: PrimaryState, currentState1: PrimaryState, myVote: Vote[ParticipantId]): sequencer.LatchedDuty[Unit] = {
					if assertionsEnabled then {
						assert(isInSequence)
						assert(myVote.term == currentState1.currentTerm || myVote.ballot == 0, s"myVote=$myVote, currentTerm=${currentState1.currentTerm}")
					}

					// scribe.trace(s"$boundParticipantId: updateRoleKnowingMyVote($myVote) was called") // TODO delete line
					if currentRole ne this then sequencer.LatchedDuty_unit
					else currentState1 match {
						case accessible1: Accessible =>
							val config1 = deriveConfigurationFrom(accessible1)
							// scribe.trace(s"$boundParticipantId: updateRoleKnowingMyVote($myVote) - configChange=${config1.correspondingConfigChange}") // TODO delete line

							// If all the participants successfully answered the howAreYou RPC, then:
							if config1.reachedAll(myVote) then {
								if myVote.votedId == boundParticipantId then {
									if currentState1 eq stateAtRequest then {
										become(Promoting(accessible1.currentTerm, primaryStateFence))
										sequencer.LatchedDuty_unit
									}
									else {
										scribe.trace(s"$boundParticipantId: updating role again due to concurrent primary-state mutation.")
										updateRole(isExclusionConsidered)
									}
								} else {
									become(Follower(accessible1.currentTerm, myVote.votedId, primaryStateFence))
									sequencer.LatchedDuty_unit
								}
							}
							// else, if a majority of the participants successfully answered the howAreYou RPC, then:
							else if config1.reachedAMajority(myVote) then {
								// If my vote is for other participant, become follower or isolated depending on the other is leading or not.
								if myVote.votedId != boundParticipantId then {
									if myVote.rank == ER_LEADING then become(Follower(accessible1.currentTerm, myVote.votedId, primaryStateFence))
									else become(Isolated(primaryStateFence))
									sequencer.LatchedDuty_unit
								}
								// If my vote is for myself and I am leading, abort the role update.
								else if currentRole.ordinal >= PROMOTING then sequencer.LatchedDuty_unit
								// If the vote is for myself and I am not leading, decide based on everyone’s votes.
								else {
									val myStateInfo1 = currentRole.buildMyStateInfo(accessible1)
									// scribe.trace(s"$boundParticipantId: updateRoleKnowingMyVote($myVote) - myStateInfo1=$myStateInfo1") // TODO delete line
									val inquires = for replierId <- config1.allOtherParticipants yield replierId.chooseALeader(boundParticipantId, myStateInfo1)
									for {
										replies <- sequencer.LatchedDuty_sequenceTasksToArray(inquires, true)
										primaryState2 <- {
											val latestTermSeen = IArray.unsafeFromArray(replies).foldLeftWithIndex(accessible1.currentTerm)((latestTermSeen, reply, _) => reply match {
												case Success(replierVote) => if replierVote.term > latestTermSeen then replierVote.term else latestTermSeen
												case _: Failure[Vote[ParticipantId]] => latestTermSeen
											})
											scribe.trace(s"$boundParticipantId: updateRoleKnowingMyVote($myVote) - replies=${replies.zip(config1.allOtherParticipants).mkString("[", ", ", "]")}, latestTermSeen=$latestTermSeen") // TODO delete line
											updateTermIfLessThan(latestTermSeen)
										}
										_ <- {
											if currentRole ne this then sequencer.LatchedDuty_unit
											else primaryState2 match {
												case Inaccessible =>
													illegalStateStop()
													sequencer.LatchedDuty_unit

												case accessible2: Accessible =>
													if assertionsEnabled then assert(myVote.term == accessible2.currentTerm || (myVote.term < accessible2.currentTerm && currentRole.ordinal < PROMOTING))

													val highestBallotSeenInVotes = IArray.unsafeFromArray(replies).foldLeftWithIndex(currentBallot)((highestBallot, reply, _) => reply match {
														case Success(replierVote) => if replierVote.ballot > highestBallot then replierVote.ballot else highestBallot
														case _: Failure[Vote[ParticipantId]] => highestBallot
													})
													if highestBallotSeenInVotes > currentBallot then {
														currentBallot = highestBallotSeenInVotes
														convergingParticipantStates.clear()
														scribe.trace(s"$boundParticipantId: updating role again due a higher ballot seen in votes.")
														// TODO consider the inclusion of the StateInfo in Vote in order to save howAreYou calls to participants for which the StateInfo in the Vote already corresponds to the new ballot. Note that this safe would occur only when restarting the role update due to a higher ballot seen in votes.
														updateRole(isExclusionConsidered)
													} else if myVote.term < accessible2.currentTerm then {
														become(Isolated(primaryStateFence))
														sequencer.LatchedDuty_unit
													} else {
														val config2 = deriveConfigurationFrom(accessible2)
														val myStateInfo2 = buildMyStateInfo(accessible2)
														// If, while waiting the responses to the chooseALeader questions, all the missing StateInfo instances (due to failed responses to the howAreYou questions) are received along with questions of those participants, then ignore the Votes and decide the role based solely on all the StateInfo instances.
														if convergingParticipantStates.size >= config2.allOtherParticipants.length && config2.allOtherParticipants.forallWithIndex { (id, _) => convergingParticipantStates.containsKey(id) } then {
															val myVote = config2.decideMyVote(myStateInfo2, convergedParticipantStates(config2))
															if myVote.votedId == boundParticipantId then {
																if accessible2 eq accessible1 then {
																	Promoting(accessible1.currentTerm, primaryStateFence)
																	sequencer.LatchedDuty_unit
																} else {
																	scribe.trace(s"$boundParticipantId: updating role again due to concurrent primary-state mutation.")
																	updateRole(isExclusionConsidered)
																}
															} else {
																become(Follower(accessible1.currentTerm, myVote.votedId, primaryStateFence))
																sequencer.LatchedDuty_unit
															}
														}
														// If some StateInfo is missing, decide the role based on the Votes.
														else {
															val newRole = config2.determineRole(accessible2, primaryStateFence, myVote, replies)
															if newRole.ordinal == PROMOTING && (accessible2 ne accessible1) then {
																scribe.trace(s"$boundParticipantId: updating role again due to concurrent primary-state mutation.")
																updateRole(isExclusionConsidered)
															}
															else {
																become(newRole)
																sequencer.LatchedDuty_unit
															}
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
								sequencer.LatchedDuty_unit
							}
						case Inaccessible =>
							illegalStateStop()
							sequencer.LatchedDuty_unit
					}
				}

				// scribe.trace(s"$boundParticipantId: updateRole($inquirerId, $inquirerInfo) was called") // TODO delete line
				for {
					primaryState1 <- primaryStateFence.causalAnchor()
					_ <- {
						if currentRole ne this then sequencer.LatchedDuty_unit
						else {
							// scribe.trace(s"$boundParticipantId: updateRole($inquirerId, $inquirerInfo) - primaryState=$primaryState") // TODO delete line
							for {
								myVote <- currentRole.decideMyVote(primaryState1, isExclusionConsidered, true)
								primaryState2 <- primaryStateFence.causalAnchor()
								// scribe.trace(s"$boundParticipantId: updateRole($inquirerId, $inquirerInfo) - myVote=$myVote") // TODO delete line
								_ <- updateRoleKnowingMyVote(primaryState1, primaryState2, myVote)
							} yield ()
						}
					}
				} yield ()
			}

			/** Updates the [[Role]] of this [[ConsensusParticipant]] and then returns the [[sequencer.LatchedDuty]] returned by the [[Role.onCommandFromClient]] method applied to the updated [[Role]].
			 * @return a [[sequencer.Task]] returned by [[Role.onCommandFromClient]] applied to the updated [[Role]] */
			final def updatesRoleAndThenCallsOnCommandFromClient(command: ClientCommand): sequencer.LatchedDuty[ResponseToClient] = {
				val initialBallot = currentBallot
				val convergingStateHash = convergingParticipantStates.hashCode()
				scribe.trace(s"$boundParticipantId: Updating role from ${RoleOrdinal_nameOf(ordinal)} due to command arrival.")
				for {
					_ <- updateRole(true)
					result <- {
						val currentRoleOrdinal = currentRole.ordinal
						if currentRoleOrdinal >= FOLLOWER then currentRole.onCommandFromClient(command, FIRST_ATTEMPT)
						else {
							if currentRoleOrdinal == ISOLATED && currentBallot == initialBallot && !convergingParticipantStates.isEmpty && convergingStateHash == convergingParticipantStates.hashCode() then {
								scribe.trace(s"$boundParticipantId: Ballot bumped due to stuckness.")
								startNewBallot()
							}
							for primaryState <- primaryStateFence.causalAnchor() yield Unable(currentRoleOrdinal, deriveConfigurationFrom(primaryState).otherProbableParticipants)
						}
					}
				} yield result
			}


			/** Derives the active [[Configuration]] state from the current [[PrimaryState]] and the [[commitIndex]].
			 * Depends on, and updates, the [[currentConfig]]. Also updates other derived state.
			 *
			 * CAUTION: the provided [[PrimaryState]] instance must be the current one. So, this method must be called only within the synchronous part of consumers subscribed synchronously to the [[sequencer.LatchedDuty]] returned by either [[sequencer.CausalFence.advance]]-like or [[sequencer.CausalFence.causalAnchor]] methods, passing the [[PrimaryState]] provided to the consumer. This requirement is needed becase this method's side effects update derived state.
			 *  @note Accessing the current [[Configuration]] through this method ensures that the current [[Configuration]] is updated before any other derived-state update that depend on it.
			 * @param currentPrimaryState the instance of the causally anchored [[PrimaryState]].
			 * @return a [[Configuration]] derived from the provided [[PrimaryState]]. */
			def deriveConfigurationFrom(currentPrimaryState: PrimaryState): Configuration = {
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
								val newConfig = Configuration_from(desiredConfigChange)
								// Update the derived state stored in the `currentRole` instance. Only the Leader role has such state as of this writing.
								currentRole.onActiveConfigChanged(accessible, oldConfig, newConfig, indexOfTopConfigChange)
								currentConfig = newConfig
								// Inform the cluster service and notify the listeners about the configuration change.
								cluster.onActiveConfigChanged(desiredConfigChange, currentRole.ordinal)
								notifyListeners(_.onActiveConfigChanged(currentRole.ordinal, accessible.currentTerm, indexOfTopConfigChange, desiredConfigChange))
							}
						}

					case Inaccessible =>
						currentConfig = NoConfig
				}
				currentConfig
			}

			/** Queues an updater to the [[PrimaryState.currentTerm]] that does the following: updates the [[PrimaryState.currentTerm]] if the provided [[Term]] is higher than it at the moment the updater is executed.
			 * @param seenTerm the [[Term]] to update the [[PrimaryState]] with, provided it is higher than the [[PrimaryState.currentTerm]] when the queued updater is executed.
			 * @param previousTermRef the [[Term]] value in this reference object is overwritten with the [[PrimaryState.currentTerm]] corresponding to the [[PrimaryState]] before the causally anchored advance is performed.
			 * @note About the safety of reusing the same [[TermRef]] instance for different calls: The value is guaranteed to reflect the expected value provided it is read within the synchronous part of a synchronously subscribed consumer to the [[LatchedDuty]] returned by [[updateTermIfLessThan]]. See the game-changing-invariant in [[Doer.CausalFence]]. */
			protected def updateTermIfLessThan(seenTerm: Term, previousTermRef: TermRef = defaultPreviousTermRef): sequencer.LatchedDuty[PrimaryState] = {
				primaryStateFence.advanceIf(
					(primaryState0, _) => {
						previousTermRef.elem = primaryState0.currentTerm
						if seenTerm > primaryState0.currentTerm then {
							Maybe(primaryState0.withTermUpdated(seenTerm))
						}
						else Maybe.empty
					},
					true
				).andThen(ps => if ps.currentTerm > previousTermRef.elem then onTermBumped(ps))
			}

			private def convergedParticipantStates(currentConfig: Configuration): IArray[StateInfo] = {
				IArray.tabulate(currentConfig.allOtherParticipants.size)(participantIndex => convergingParticipantStates.get(currentConfig.allOtherParticipants(participantIndex)))
			}

			def onTermBumped(primaryState: PrimaryState): Unit = ()
		}

		/** The behavior when the participant has the [[STOPPED]] role. Taken when the service is stopped or after a failure. */
		private final class Stopped(val motive: Try[String]) extends Role {
			override val ordinal: RoleOrdinal = STOPPED

			val completed: sequencer.Covenant[Unit] = sequencer.Covenant()

			override def isEquivalentTo(other: Role): Boolean =
				other.ordinal == STOPPED

			override def onEnter(previousRole: Role): Unit = {
				notifyListeners(_.onBecameStopped(previousRole.ordinal, previousRole.getCommittedPrimaryState.currentTerm, motive))
				retireeAgentByParticipantId.clear()
				cluster.onStopped(motive)
				previousRole match {
					case statefulRole: StatefulRole =>
						statefulRole.primaryStateFence.advanceIf(
							(ps, _) => {
								if currentRole ne this then Maybe.empty
								else ps match {
									case Inaccessible => Maybe.empty
									case a: Accessible => Maybe.some(a.withWorkspaceReleased())
								}
							},
							true
						).andThen(_ => completed.fulfillUnsafe(()))

					case _ =>
						completed.fulfillUnsafe(())
				}
			}

			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.LatchedDuty[ResponseToClient] = {
				checkWithin()
				sequencer.LatchedDuty_ready(Unable(ordinal, cluster.getOtherProbableParticipant))
			}

			override def requestConfigChange(requestId: ConfigChangeRequestId, desiredParticipantsSet: Set[ParticipantId]): sequencer.LatchedDuty[ConfigChangeResponse] =
				sequencer.LatchedDuty_ready(UNABLE)
		}

		/** A transitional [[Role]] before [[Stopped]] to which a participant transitions to when a [[StableConfigChange]] that excludes it becomes active.
		 * This [[Role]] last until all the [[RetireeAgent]]s created while leading, in case it was, have concluded their job. The [[RetireeAgent]] that concludes lately triggers the transition to [[Stopped]].
		 *
		 * This [[Role]] exists only to support configuration changes that result in an empty participant set.
		 * Why is it needed? Because excluded participants depend on the leader to send them the final appends confirming they are safe to stop. If this [[Role]] didn't exist, there would be no [[RetireeAgent]]s responsible for that task when the configuration changes to an empty set.
		 * Since this role must exist for that reason, we also take advantage of its presence to wait the [[RetireeAgent]]s to conclude their job even when the deposing [[ConfigChange]]'s [[ConfigChange.newParticipants]] set is not empty. In this scenario, the job of the [[RetireeAgent]]s of this retiring ex-leader will overlap with the job of the [[RetireeAgent]]s of the new [[Leader]], but, if I am not mistaken, this overlap is more beneficial than harmful because it removes some burden to the new [[Leader]].
		 * @param finalTerm the last [[Term]] during which this participant was active.
		 * @param newParticipants the [[StableConfigChange.newParticipants]] of the [[StableConfigChange]] that excluded this participant while it was [[Leader]]. */
		private final class Retiring(val finalTerm: Term, newParticipants: IArray[ParticipantId]) extends Role {
			override val ordinal: RoleOrdinal = RETIRING

			if assertionsEnabled then assert(!newParticipants.contains(boundParticipantId))

			override def isEquivalentTo(other: Role): Boolean = {
				other.ordinal == RETIRING && other.asInstanceOf[Retiring].finalTerm == this.finalTerm
			}

			override def onEnter(previous: Role): Unit = {
				notifyListeners(_.onRetiring(finalTerm))
				if retireeAgentByParticipantId.isEmpty then become(Stopped(Success(s"$boundParticipantId: The retiring stage ended immediately because there are no pending retirees.")))
			}

			override def requestConfigChange(requestId: ConfigChangeRequestId, desiredParticipantsSet: Set[ParticipantId]): sequencer.LatchedDuty[ConfigChangeResponse] = {
				sequencer.LatchedDuty_ready(EXCLUDED)
			}

			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.LatchedDuty[ResponseToClient] = {
				sequencer.LatchedDuty_ready(Unable(
					ordinal,
					ListSet.newBuilder[ParticipantId].addAll(newParticipants).addAll(cluster.getOtherProbableParticipant).result()
				))
			}

			override def onAppendRecords(inquirerId: ParticipantId, inquirerTerm: Term, prevRecordIndex: RecordIndex, prevRecordTerm: Term, records: GenIndexedSeq[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.LatchedDuty[AppendResult] = {
				become(Starting(false)).onAppendRecords(inquirerId, inquirerTerm, prevRecordIndex, prevRecordTerm, records, leaderCommit, termAtLeaderCommit)
			}
		}

		/** The behavior when the participant has the [[STARTING]] role. This is a transitory role during which the participant state is initialized.
		 * When initialization is completed it transitions to the [[Isolated]] state. */
		private final class Starting(val isSeed: Boolean) extends Role {
			override val ordinal: RoleOrdinal = STARTING
			/** Is fulfilled after initializing this [[ConsensusParticipant]] and becoming another [[Role]]: [[Joining]], [[Isolated]], or [[Stopped]]. */
			private val startingCompletedCovenant: sequencer.Covenant[Unit] = sequencer.Covenant()

			override def isEquivalentTo(other: Role): Boolean =
				other.ordinal == STARTING && other.asInstanceOf[Starting].isSeed == this.isSeed

			override def onEnter(previous: Role): Unit = {
				notifyListeners(_.onStarting(previous.ordinal, isSeed))

				storage.load.subscribe {
					case Success(loadedWorkspace) =>
						val indexOfTopConfigChange = loadedWorkspace.indexOfTopConfigChange
						val rulingConfigChange = {
							if indexOfTopConfigChange == 0 then {
								loadedWorkspace.setCurrentTerm(0)
								new TransitionalConfigChange[ParticipantId](0, "Initial-Config", Set.empty, cluster.getInitialParticipants)

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
						val config = Configuration_from(rulingConfigChange)
						if !isSeed || config.allParticipants.contains(boundParticipantId) then {
							currentConfig = config
							val primaryState = Accessible(loadedWorkspace)
							val primaryStateFence = sequencer.CausalFence[PrimaryState](primaryState)
							notifyListeners(_.onStarted(previous.ordinal, primaryState.currentTerm, rulingConfigChange, isSeed))
							if isSeed then become(Isolated(primaryStateFence))
							else become(Joining(primaryStateFence))

						} else {
							become(Stopped(Success(s"Star-up aborted because this ConsensusParticipant instance does not belong to the initial cluster-configuration.")))
						}
						startingCompletedCovenant.fulfillUnsafe(())

					case failure@Failure(e) =>
						scribe.error(s"$boundParticipantId: Unexpected error while loading the consensus-service's workspace:", e)
						become(Stopped(failure.castTo[String]))
						startingCompletedCovenant.fulfillUnsafe(())
				}
			}

			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.LatchedDuty[ResponseToClient] = {
				checkWithin()
				for {
					_ <- startingCompletedCovenant
					rtc <- currentRole.onCommandFromClient(command, attemptFlag)
				} yield rtc
			}

			override def requestConfigChange(requestId: ConfigChangeRequestId, desiredParticipantsSet: Set[ParticipantId]): sequencer.LatchedDuty[ConfigChangeResponse] = {
				checkWithin()
				for {
					_ <- startingCompletedCovenant
					response <- currentRole.requestConfigChange(requestId, desiredParticipantsSet)
				} yield response
			}
		}

		private final class Joining(psf: sequencer.CausalFence[PrimaryState]) extends StatefulRole(psf) {
			/** The ordinal corresponding to this [[Role]] */
			override val ordinal: RoleOrdinal = JOINING

			override def onEnter(previous: Role): Unit = {
				notifyListeners(_.onJoining(previous.ordinal))
			}

			override def decideMyVote(primaryState0: PrimaryState, isExclusionConsidered: Boolean, blankVoteIfRoleChanges: Boolean): sequencer.LatchedDuty[Vote[ParticipantId]] = {
				yieldsBlankVote
			}

			override def onChooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.LatchedDuty[Vote[ParticipantId]] = {
				yieldsBlankVote
			}

			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.LatchedDuty[ResponseToClient] = {
				sequencer.LatchedDuty_ready(Unable(ordinal, cluster.getOtherProbableParticipant))
			}

			override def requestConfigChange(requestId: ConfigChangeRequestId, desiredParticipants: Set[ParticipantId]): sequencer.LatchedDuty[ConfigChangeResponse] = {
				sequencer.LatchedDuty_ready(CATCHING_UP)
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

			/**
			 * The main loop of the isolated state.
			 * It checks if the current term leader is reachable or the reachable participants including itself are the majority.
			 * If so, it becomes a follower or a candidate respectively.
			 * If not, it stays in the isolated state and checks again after a while.
			 */
			override def onEnter(previous: Role): Unit = {
				notifyListeners(_.onBecameIsolated(previous.ordinal, psf.committedState.currentTerm))
				if isEager then {
					val schedule = sequencer.newDelaySchedule(isolatedMainLoopInterval)
					currentSchedule = Maybe(schedule)
					// schedule the state-updater process
					sequencer.schedule(schedule)(_ => updateRole(true))
				}
			}

			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.LatchedDuty[ResponseToClient] = {
				checkWithin()
				updatesRoleAndThenCallsOnCommandFromClient(command)
			}
		}

		/** A transitional [[Role]] hosted only while the [[Term]] is bumped after exiting the [[Leader]] [[Role]] due to a later [[Term]] seen.
		 * It behaves as [[Isolated]] except that, in the [[onEnter]] life-cycle stage it enqueues an updater of the [[PrimaryState.currentTerm]] that sets it to the latest seen term if not already; and then transitions to [[Retiring]] if this participant is excluded from the active [[Configuration]], or to [[Isolated]] otherwise.
		 *
		 * @param endedTerm the [[Term]] that concluded, during which this participant acted as [[Leader]].
		 * TODO Consider replacing this class with a method that transitions to [[Isolated]] or [[Retiring]] in a synchronous manner, and then enqueues a term update. An alternative would be to add an optional parameter to the new [[Follower]] class that accepts the term to update to in the onEnter.
		 */
		private final class HandingOff(val endedTerm: Term, latestTermSeen: Term, psf: sequencer.CausalFence[PrimaryState]) extends Isolated(psf) {
			override val ordinal: RoleOrdinal = HANDING_OFF

			override def isEquivalentTo(other: Role): Boolean = {
				super.isEquivalentTo(other) && other.asInstanceOf[HandingOff].endedTerm == this.endedTerm
			}

			override def onEnter(previous: Role): Unit = {
				notifyListeners(_.onHandingOff(endedTerm))

				for {
					primaryState1 <- primaryStateFence.advanceIf(
						(primaryState0, _) => if primaryState0.currentTerm < latestTermSeen then Maybe(primaryState0.withTermUpdated(latestTermSeen)) else Maybe.empty,
						true
					)
				} yield if currentRole eq this then primaryState1 match {
					case Inaccessible =>
						illegalStateStop()

					case accessible: Accessible =>
						// check if excluded from the new configuration.
						val updatedConfig = deriveConfigurationFrom(accessible)
						// if excluded, become Retiring
						if !updatedConfig.allParticipants.contains(boundParticipantId) then become(Retiring(accessible.currentTerm, updatedConfig.allParticipants))
						// else, become Isolated
						else become(Isolated(primaryStateFence))
				}
			}
		}

		/**
		 * Behavior when the participant has the follower role. Taken when reachability to a majority of the participants is achieved and one of them has the [[Leader]] role and is in a higher or equal term.
		 *
		 * In this state, the participant acknowledges the specified leader.
		 *
		 * @param leaderId The ID of the leader this participant is following
		 */
		private final class Follower(val term: Term, val leaderId: ParticipantId, psf: sequencer.CausalFence[PrimaryState]) extends StatefulRole(psf) {
			override val ordinal: RoleOrdinal = FOLLOWER

			override def isEquivalentTo(other: Role): Boolean =
				super.isEquivalentTo(other) && (other match {
					case follower: Follower => term == follower.term && leaderId == follower.leaderId
					case _ => false
				})

			override def onEnter(previous: Role): Unit = {
				notifyListeners(_.onBecameFollower(previous.ordinal, term, leaderId))
			}

			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.LatchedDuty[ResponseToClient] = {
				checkWithin()
				if attemptFlag == FIRST_ATTEMPT then sequencer.LatchedDuty_ready(RedirectTo(leaderId))
				else updatesRoleAndThenCallsOnCommandFromClient(command)
			}
		}

		/** A substate of a leading participant that last until the term bump is stored.
		 * During this interval, all the RPC calls this [[ConsensusParticipant]] receives are put in standby until the bumped term is stored and role transitioned, such that responses to queries form the outside never completed in this role and, therefore, the role ordinal never in responses is never [[PROMOTING]]. */
		private final class Promoting(currentTerm: Term, psf: sequencer.CausalFence[PrimaryState]) extends StatefulRole(psf) {
			/** The ordinal corresponding to this [[Role]] */
			override val ordinal: RoleOrdinal = PROMOTING

			/** Is fulfilled after bumping the term and becoming [[Leader]] if success, or [[Stopped]] if fails to persist the primary state. */
			private val promotionCovenant: sequencer.Covenant[Unit] = sequencer.Covenant()

			override def isEquivalentTo(other: Role): Boolean = {
				other match {
					case leader: Leader => leader.leadedTerm == currentTerm
					case _ => super.isEquivalentTo(other)
				}
			}

			override def onEnter(previous: Role): Unit = {
				notifyListeners(_.onPromoting(previous.ordinal, getCommittedPrimaryState.currentTerm))

				for {
					// Bump the term
					primaryState1 <- primaryStateFence.advanceIf(
						(primaryState0, _) => {
							if currentRole ne this then Maybe.empty
							else primaryState0 match {
								case Inaccessible => Maybe.empty
								case accessible0: Accessible => Maybe(primaryState0.withTermUpdated(primaryState0.currentTerm + 1))
							}

						},
						true
					)
				} yield {
					if currentRole eq this then {
						primaryState1 match {
							case Inaccessible =>
								illegalStateStop()

							case accessible1: Accessible =>
								become(Leader(accessible1.currentTerm, accessible1, deriveConfigurationFrom(accessible1), primaryStateFence))
						}
					}
					promotionCovenant.fulfillUnsafe(())
				}
			}

			override def decideMyVote(primaryState0: PrimaryState, isExclusionConsidered: Boolean, blankVoteIfRoleChanges: Boolean): sequencer.LatchedDuty[Vote[ParticipantId]] = {
				checkWithin()
				for {
					_ <- promotionCovenant
					primaryState1 <- primaryStateFence.causalAnchor()
					vote <- currentRole.decideMyVote(primaryState1, isExclusionConsidered, blankVoteIfRoleChanges)
				} yield vote
			}

			override def onHowAreYou(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.LatchedDuty[StateInfo] = {
				checkWithin()
				for {
					_ <- promotionCovenant
					stateInfo <- currentRole.onHowAreYou(inquirerId, inquirerInfo)
				} yield stateInfo
			}

			override def onChooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.LatchedDuty[Vote[ParticipantId]] = {
				checkWithin()
				for {
					_ <- promotionCovenant
					vote <- currentRole.onChooseALeader(inquirerId, inquirerInfo)
				} yield vote
			}

			override def onAppendRecords(inquirerId: ParticipantId, inquirerTerm: Term, prevRecordIndex: RecordIndex, prevRecordTerm: Term, records: GenIndexedSeq[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.LatchedDuty[AppendResult] = {
				checkWithin()
				for {
					_ <- promotionCovenant
					response <- currentRole.onAppendRecords(inquirerId, inquirerTerm, prevRecordIndex, prevRecordTerm, records, leaderCommit, termAtLeaderCommit)
				} yield response
			}

			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.LatchedDuty[ResponseToClient] = {
				checkWithin()
				for {
					_ <- promotionCovenant
					response <- currentRole.onCommandFromClient(command, attemptFlag)
				} yield response
			}

			override def requestConfigChange(requestId: ConfigChangeRequestId, desiredParticipants: Set[ParticipantId]): sequencer.LatchedDuty[ConfigChangeResponse] = {
				checkWithin()
				for {
					_ <- promotionCovenant
					response <- currentRole.requestConfigChange(requestId, desiredParticipants)
				} yield response
			}

		}

		/**
		 * Behavior when the participant has the [[LEADER]] role. Taken when reachability to a majority of the participants is achieved, none of them is a [[Leader]] with higher or equal term, and wins the new leader election.
		 *
		 * In this state, the participant coordinates consensus decisions.
		 * TODO replace the `initialPrimaryState` parameter with what is obtained from it. Storing an instance of [[Accessible]] is error prone.
		 */
		private final class Leader(val leadedTerm: Term, initialPrimaryState: Accessible, initialConfig: Configuration, wsf: sequencer.CausalFence[PrimaryState]) extends StatefulRole(wsf) { thisLeader =>
			/** The [[sequencer.Task]] returned by a call to [[ClusterParticipant.appendRecords]]. */
			private type AppendRequest = sequencer.Task[AppendResult]

			/** The outcome of the [[sequencer.Task]] returned by a call to [[ClusterParticipant.appendRecords]]. */
			private type AppendResponse = Try[AppendResult]

			override val ordinal: RoleOrdinal = LEADER

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

			private var highestRecordIndexKnowToBeCommited_ByParticipantIndex: Array[RecordIndex] = Array.fill(initialConfig.allOtherParticipants.size)(0)

			/** Set to the index of the [[StableConfigChange]] that excluded this [[ConsensusParticipant]] by the [[Leader.programTheRetirements]] method, which is called by [[deriveConfigurationFrom]] when the active [[Configuration]] changes from a [[TransitionalConfig]] to a [[StableConfig]]. */
			private var indexOfConfigChangeThatExcludedThisParticipant: RecordIndex = 0

			/** The sequencer used to generate the serial number that identifies each execution of the [[attemptToUpdateOtherParticipantsLogs]] method. */
			private var sequencerOfReplicationAttempts: Int = 0

			private var unreachableFollowersRetrySchedule: Maybe[sequencer.Schedule] = Maybe.empty

			override def isEquivalentTo(other: Role): Boolean =
				super.isEquivalentTo(other) && this.leadedTerm == other.asInstanceOf[Leader].leadedTerm

			override def onEnter(previous: Role): Unit = {
				notifyListeners(_.onBecameLeader(previous.ordinal, leadedTerm))

				val indexOfTopConfigChange = initialPrimaryState.indexOfTopConfigChange
				// if the log lacks a ConfigChange record (is empty), create a synthetic one with the seed participants of the initial synthetic configuration (appointed in `currentConfig` during Starting).
				if indexOfTopConfigChange == 0 then {
					for primaryState1 <- primaryStateFence.advanceIf(
						(primaryState0, _) => {
							primaryState0 match {
								case Inaccessible => Maybe.empty
								case accessible0: Accessible => Maybe(accessible0.withSingleRecordAppended(accessible0.currentTerm, currentConfig.backingConfigChange))
							}
						},
						true
					)
					yield {
						startConfigChangeSecondPhase(currentConfig.backingConfigChange.asInstanceOf[TransitionalConfigChange[ParticipantId]], 1)
					}
				}
				// if the log contains a ConfigChange record then:
				else initialPrimaryState.getRecordAt(indexOfTopConfigChange).asInstanceOf[ConfigChange[ParticipantId]] match {
					// If the top configuration change in the local log is a transitional one, continue the configuration transition process. This happens when the leader that started the first phase of the configuration change crashed or left the leadership before achieving the replication of the TransitionalConfigChange to a majority, or while storing the StableConfigChange in his persistent log.
					case tcc: TransitionalConfigChange[ParticipantId @unchecked] =>
						startConfigChangeSecondPhase(tcc, indexOfTopConfigChange)

					// If, on the contrary, is a stable one
					case scc: StableConfigChange[ParticipantId @unchecked] =>
						// ... and it was commited (commitIndex >= its index in the log), update the retiring-participants.
						if commitIndex >= indexOfTopConfigChange then thisLeader.programTheRetirements(initialPrimaryState, initialConfig, scc, indexOfTopConfigChange)
				}
			}


			override def onLeave(): Unit = {
				unreachableFollowersRetrySchedule.foreach(sequencer.cancel(_))
				unreachableFollowersRetrySchedule = Maybe.empty
			}

			/** @inheritdoc
			 *  This implementation does two different things:
			 *  1) Updates the [[RetiringParticipantsManager]] to include any new old-configuration-only retiring participant (those that are not part of the new [[Configuration]], but still need more appends until their [[commitIndex]] reaches the index of the [[StableConfigChange]] that excluded them).
			 *  2) Recreates and initializes the [[indexOfNextRecordToSend_ByParticipantIndex]] and [[highestRecordIndexKnownToBeAppended_ByParticipantIndex]] arrays keeping the elements corresponding to the participants that remain and moving them to the appropriate index.
			 * @note This rearrangement wouldn't be necessary if maps instead of arrays were used. But considering these two collections are heavily used, efficiency was primed. */
			override def onActiveConfigChanged(currentPrimaryState: Accessible, oldConfig: Configuration, newConfig: Configuration, indexOfNewConfigChange: RecordIndex): Unit = {
				// Step one. Must be before step two.
				newConfig.backingConfigChange.match {
					case scc: StableConfigChange[ParticipantId] =>
						thisLeader.programTheRetirements(currentPrimaryState, oldConfig, scc, indexOfNewConfigChange)
					case tcc: TransitionalConfigChange[ParticipantId] =>
						// If a previous configuration change excluded this leading participant but a later configuration change includes it, clear the mark that instructs itself to retire (when it sees that the previous config change is commited).
						if indexOfConfigChangeThatExcludedThisParticipant > 0 && newConfig.allParticipants.contains(boundParticipantId) then indexOfConfigChangeThatExcludedThisParticipant = 0
				}

				// Step two
				val newAllOtherParticipantsArrayLength = newConfig.allOtherParticipants.length
				val newIndexOfNextRecordToSend_ByParticipantIndex: Array[RecordIndex] = new Array(newAllOtherParticipantsArrayLength)
				val newHighestRecordIndexKnowToBeAppended_ByParticipantIndex: Array[RecordIndex] = new Array(newAllOtherParticipantsArrayLength)
				val newHighestRecordIndexKnowToBeCommited_ByParticipantIndex: Array[RecordIndex] = new Array(newAllOtherParticipantsArrayLength)

				var participantNewIndex = newAllOtherParticipantsArrayLength
				while participantNewIndex > 0 do {
					participantNewIndex -= 1
					val participantId = newConfig.allOtherParticipants(participantNewIndex)
					val participantOldIndex = oldConfig.participantIndexOf(participantId)
					if participantOldIndex >= 0 then {
						newIndexOfNextRecordToSend_ByParticipantIndex(participantNewIndex) = indexOfNextRecordToSend_ByParticipantIndex(participantOldIndex)
						newHighestRecordIndexKnowToBeAppended_ByParticipantIndex(participantNewIndex) = highestRecordIndexKnownToBeAppended_ByParticipantIndex(participantOldIndex)
						newHighestRecordIndexKnowToBeCommited_ByParticipantIndex(participantNewIndex) = highestRecordIndexKnowToBeCommited_ByParticipantIndex(participantOldIndex)

					} else {
						newIndexOfNextRecordToSend_ByParticipantIndex(participantNewIndex) = indexOfNewConfigChange
						newHighestRecordIndexKnowToBeAppended_ByParticipantIndex(participantNewIndex) = 0
						newHighestRecordIndexKnowToBeCommited_ByParticipantIndex(participantNewIndex) = 0
					}
				}
				indexOfNextRecordToSend_ByParticipantIndex = newIndexOfNextRecordToSend_ByParticipantIndex
				highestRecordIndexKnownToBeAppended_ByParticipantIndex = newHighestRecordIndexKnowToBeAppended_ByParticipantIndex
				highestRecordIndexKnowToBeCommited_ByParticipantIndex = newHighestRecordIndexKnowToBeCommited_ByParticipantIndex
			}

			/** Programs the retirement of the participants that are not included in the [[Configuration]] that is going to become the active one.
			 *		- If this [[Leader]] is excluded, sets the threshold [[indexOfConfigChangeThatExcludedThisParticipant]]. The replication logic checks it after successful appends to decide if a transition to the [[Retiring]] [[Role]] is needed.
			 *		- Creates and registers an instance of [[RetireeAgent]] for each excluded follower that needs more appends to notice it can abandon the cluster.
			 * Must be called a single time whenever the active [[Configuration]] changes from a [[TransitionalConfig]] to a [[StableConfig]].
			 *
			 * @param primaryState the [[PrimaryState]] from which the transition is derived.
			 * @param oldConfig the [[Configuration]] that is being abandoned and on which the [[Leader]] derived state is based. Needed to know what is in each element of the [[indexOfNextRecordToSend_ByParticipantIndex]] and [[highestRecordIndexKnownToBeAppended_ByParticipantIndex]].
			 * @param configChange the [[ConfigChange]] that caused the transition.
			 * @param configChangeIndex the log index where the provided [[ConfigChange]] is stored. */
			private def programTheRetirements(primaryState: Accessible, oldConfig: Configuration, configChange: StableConfigChange[ParticipantId], configChangeIndex: RecordIndex): Unit = {
				assert(commitIndex >= configChangeIndex)

				// Remove, from the retiree agents map, the ones corresponding to the participants that become included.
				retireeAgentByParticipantId.filterInPlace { (retireeId, agent) => configChange.newParticipants.contains(retireeId) }

				// Find out which are the participants that become excluded.
				val newRetiringParticipants = configChange.oldParticipants.diff(configChange.newParticipants)

				// If this leader is excluded, set the threshold until which this leader will continue leading.
				if newRetiringParticipants.contains(boundParticipantId) then thisLeader.indexOfConfigChangeThatExcludedThisParticipant = configChangeIndex

				// Create and register the retiree agents for the other participants that were excluded.
				oldConfig.allOtherParticipants.foreachWithIndex { (participantId, participantIndex) =>
					if highestRecordIndexKnowToBeCommited_ByParticipantIndex(participantIndex) < configChangeIndex
						&& newRetiringParticipants.contains(participantId)
						&& !retireeAgentByParticipantId.contains(participantId)
					then {
						val highestRecordIndexKnownToBeAppended = thisLeader.highestRecordIndexKnownToBeAppended_ByParticipantIndex(participantIndex)
						val indexOfFirstPotentiallyUnappendedRecord = highestRecordIndexKnownToBeAppended + 1
						val potentiallyUnappendedRecords: GenIndexedSeq[Record] = primaryState.getRecordsBetween(indexOfFirstPotentiallyUnappendedRecord, configChangeIndex + 1)
						val retireeAgent = RetireeAgent(
							participantId,
							primaryStateFence,
							primaryState.getRecordTermAt(highestRecordIndexKnownToBeAppended),
							potentiallyUnappendedRecords,
							indexOfFirstPotentiallyUnappendedRecord,
							configChangeIndex,
							primaryState.getRecordTermAt(configChangeIndex),
							thisLeader.indexOfNextRecordToSend_ByParticipantIndex(participantIndex)
						)
						retireeAgentByParticipantId.put(participantId, retireeAgent)
						retireeAgent.startAppendLoop(primaryState)
					}
				}
			}


			/** Handles configuration-change request for [[Leader]]
			 * Attempts a [[Configuration]] change, starting with the first phase and, if successful, continuing with the second. */
			override def requestConfigChange(requestId: ConfigChangeRequestId, desiredParticipants: Set[ParticipantId]): sequencer.LatchedDuty[ConfigChangeResponse] = {

				def startSecondPhase(tcc: TransitionalConfigChange[ParticipantId], tccIndex: RecordIndex): sequencer.LatchedDuty[ConfigChangeResponse] = {
					for isSccReplicatedToMajority <- startConfigChangeSecondPhase(tcc, tccIndex)
						yield if isSccReplicatedToMajority then SUCCESSFULLY_CHANGED else REQUEST_TRACKING_LOST_AFTER_SECOND_PHASE_STARTED
				}

				/** This method recurses whenever it fails and the consequent [[updateRole]] does not change the [[Role]] (stays as leader) */
				def replicateTccAndThenStartSecondPhase(primaryState0: PrimaryState, tcc: TransitionalConfigChange[ParticipantId], tccIndex: RecordIndex): sequencer.LatchedDuty[ConfigChangeResponse] = {
					if currentRole ne this then sequencer.LatchedDuty_ready(REQUEST_TRACKING_LOST_AFTER_FIRST_PHASE_STARTED)
					else primaryState0 match {
						case Inaccessible =>
							sequencer.LatchedDuty_ready(UNABLE)
						case accessible2: Accessible =>
							if assertionsEnabled then assert(primaryState0.currentTerm == leadedTerm)
							for {
								// Replicate to other participants.
								isTccReplicatedToMajority <- attemptToUpdateOtherParticipantsLogs(accessible2)
								response <- {
									if currentRole ne this then sequencer.LatchedDuty_ready(if isTccReplicatedToMajority then REQUEST_TRACKING_LOST_AFTER_FIRST_PHASE_COMMITED else REQUEST_TRACKING_LOST_AFTER_FIRST_PHASE_STARTED)
									else if isTccReplicatedToMajority then startSecondPhase(tcc, tccIndex)
									else {
										for {
											_ <- {
												scribe.trace(s"$boundParticipantId: Updating role due to insufficient quorum when replicating a TransitionalConfigChange record.")
												startNewBallot()
												updateRole(false)
											}
											primaryState1 <- primaryStateFence.causalAnchor()
											response <- replicateTccAndThenStartSecondPhase(primaryState1, tcc, tccIndex)
										} yield response
									}
								}
							} yield response
					}
				}

				scribe.trace(s"$boundParticipantId: handling the config change request $requestId to $desiredParticipants.")
				for {
					primaryState0 <- primaryStateFence.causalAnchor()
					response <- {
						if currentRole ne this then currentRole.requestConfigChange(requestId, desiredParticipants)
						else {
							if assertionsEnabled then assert(primaryState0.currentTerm == leadedTerm)

							deriveConfigurationFrom(primaryState0) match {
								case stable0: StableConfig =>
									if desiredParticipants == stable0.desiredParticipants then sequencer.LatchedDuty_ready(ALREADY_CHANGED)
									// Do not start a configuration transition if excluded from both, the current, and the new configuration.
									else if !stable0.allParticipants.contains(boundParticipantId) && !desiredParticipants.contains(boundParticipantId) then {
										if isExcludedAndAllFollowersCommittedTheExcludingConfigChange then become(Retiring(leadedTerm, stable0.allParticipants))
										sequencer.LatchedDuty_ready(EXCLUDED)
									} else {
										// start the first phase of the configuration change
										val tcc = new TransitionalConfigChange[ParticipantId](primaryState0.currentTerm, requestId, stable0.desiredParticipants, desiredParticipants)
										scribe.trace(s"$boundParticipantId: About to append the first phase of config change $tcc")
										for {
											// Update primary state
											primaryState2 <- primaryStateFence.advanceIf(
												(primaryState1, _) => {
													if currentRole ne this then Maybe.empty
													else primaryState1 match {
														case Inaccessible =>
															Maybe.empty
														case accessible1: Accessible =>
															if assertionsEnabled then assert(primaryState1.currentTerm == leadedTerm)
															Maybe(accessible1.withSingleRecordAppended(tcc.term, tcc))
													}
												},
												true
											)
											// replicate the TransitionalConfigChange and then start the second phase.
											response <- replicateTccAndThenStartSecondPhase(primaryState2, tcc, primaryState2.firstEmptyRecordIndex - 1)
										} yield response
									}

								case transitional0: TransitionalConfig =>
									val response = if desiredParticipants == transitional0.desiredParticipants then ALREADY_IN_PROGRESS else WAIT_PREVIOUS_CHANGE_TO_COMPLETE
									sequencer.LatchedDuty_ready(response)

								case NoConfig =>
									sequencer.LatchedDuty_ready(UNABLE)
							}
						}
					}
				} yield response
			}

			/** Starts the second phase of a configuration change.
			 * Appends a [[StableConfigChange]] instance in the local log, stores it, and then attempts to replicate it to the participants in both, old and new configurations as if its configuration was the corresponding [[TransitionalConfigChange]].
			 * @param correspondingTransitionalConfigChange the [[TransitionalConfigChange]] that initiated the first phase of the configuration change.
			 * @return  a [[sequencer.LatchedDuty]] that yields true/false if the [[StableConfigChange]] [[Record]] was/wasn't replicated to a majority. */
			private def startConfigChangeSecondPhase(correspondingTransitionalConfigChange: TransitionalConfigChange[ParticipantId], tccIndex: RecordIndex): sequencer.LatchedDuty[Boolean] = {

				scribe.trace(s"$boundParticipantId: Starting second phase of the configuration change started with $correspondingTransitionalConfigChange")
				for {
					primaryState1 <- primaryStateFence.advanceIf(
						(primaryState0, _) => {
							if currentRole ne this then Maybe.empty
							else primaryState0 match {
								case accessible0: Accessible =>
									if assertionsEnabled then assert(accessible0.currentTerm == leadedTerm)
									val scc = new StableConfigChange[ParticipantId](accessible0.currentTerm, correspondingTransitionalConfigChange.requestId, correspondingTransitionalConfigChange.term, correspondingTransitionalConfigChange.oldParticipants, correspondingTransitionalConfigChange.newParticipants)
									Maybe(accessible0.withSingleRecordAppended(accessible0.currentTerm, scc))
								case Inaccessible =>
									Maybe.empty
							}
						},
						true
					)
					sccIndex = primaryState1.firstEmptyRecordIndex - 1
					isSecondPhaseChangeReplicatedToMajority <- replicateUntilSuccessOrLeaderRoleIsAbandoned(primaryState1, tccIndex)
				} yield isSecondPhaseChangeReplicatedToMajority
			}

			/** Replicates all the records in this [[ConsensusParticipant]], retrying until either:
			 *		- the [[Record]]s up to the provided index are replicated to a majority.
			 *		- the [[currentRole]] stops being this [[Leader]] instance.
			 * This method recurses whenever it fails and the consequent [[updateRole]] does not change the [[Role]] (stays as leader) */
			private def replicateUntilSuccessOrLeaderRoleIsAbandoned(primaryState0: PrimaryState, targetIndex: RecordIndex): sequencer.LatchedDuty[Boolean] = {
				if currentRole ne this then sequencer.LatchedDuty_false
				else primaryState0 match {
					case Inaccessible =>
						sequencer.LatchedDuty_false
					case accessible0: Accessible =>
						if assertionsEnabled then assert(accessible0.currentTerm == leadedTerm)
						for {
							isSccReplicatedToMajority <- {
								if commitIndex < targetIndex then attemptToUpdateOtherParticipantsLogs(accessible0)
								else sequencer.LatchedDuty_true
							}
							result <- {
								if isSccReplicatedToMajority then sequencer.LatchedDuty_true
								else {
									scribe.trace(s"$boundParticipantId: Updating role due to insufficient quorum when replicating a StableConfigChange record.")
									startNewBallot()
									for {
										_ <- updateRole(false)
										primaryState1 <- primaryStateFence.causalAnchor()
										recursionResult <- replicateUntilSuccessOrLeaderRoleIsAbandoned(primaryState1, targetIndex)
									} yield recursionResult
								}
							}
						} yield result
				}
			}

			override def onCommandFromClient(clientCommand: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.LatchedDuty[ResponseToClient] = {
				checkWithin()

				val clientId = clientIdOf(clientCommand)
				var primaryStateUpdaterResult: (recordIndex: RecordIndex, indexOfLastAppendedCommandFromClient: RecordIndex, shouldRetire: Boolean) | Null = null // secondary return value of the causal fence exclusive section
				for {
					// First, append the command to the log if it wasn't already
					primaryState1 <- primaryStateFence.advanceIf(
						(primaryState0, _) => {
							if currentRole ne this then Maybe.empty
							else primaryState0 match {
								case Inaccessible =>
									Maybe.empty
								case accessible0: Accessible =>
									val currentTerm = accessible0.currentTerm
									if assertionsEnabled then assert(currentTerm == leadedTerm)

									// Do not append the command if the bounded participant is excluded and ready to retire. The intention of this is to minimize the time that a participant is kept leading after it was excluded.
									if isExcludedAndAllFollowersCommittedTheExcludingConfigChange then {
										if assertionsEnabled then assert(accessible0.indexOfTopConfigChange == indexOfConfigChangeThatExcludedThisParticipant || accessible0.getRecordAt(accessible0.indexOfTopConfigChange).asInstanceOf[ConfigChange[ParticipantId]].newParticipants.contains(boundParticipantId)) // because Leader.requestConfigChange never starts a configuration transition if the bounded participant is not present in neither the current nor the desired participants set.
										primaryStateUpdaterResult = (0, 0, true)
										Maybe.empty
									} else {
										val indexOfLastAppendedCommandFromClient = accessible0.indexOfLastAppendedCommandFrom(clientId)

										// If this is the first command received from the client, proceed normally (append, replicate, apply)
										if indexOfLastAppendedCommandFromClient == 0 then {
											primaryStateUpdaterResult = (primaryState0.firstEmptyRecordIndex, indexOfLastAppendedCommandFromClient, false)
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
														primaryStateUpdaterResult = (accessible0.firstEmptyRecordIndex, indexOfLastAppendedCommandFromClient, false)
														Maybe.some(accessible0.withSingleRecordAppended(currentTerm, CommandRecord(currentTerm, clientCommand)))
													}
													// if the command is the same as the last received, memorize the index of the last received.
													else if comparison == 0 then {
														primaryStateUpdaterResult = (indexOfLastAppendedCommandFromClient, indexOfLastAppendedCommandFromClient, false)
														Maybe.empty
													}
													// if the command is older than the last received, obtain its index and memorize it.
													else {
														primaryStateUpdaterResult = (accessible0.indexOf(clientCommand), indexOfLastAppendedCommandFromClient, false)
														Maybe.empty
													}
												case _ =>
													// Inconsistency. Should never happen
												Maybe.empty
											}
										// @formatter:on
										}
									}
							}
						},
						true
					)
					// Second, replicate it if not already, and then, if replication was successful, apply the command to the state machine assuming it is idempotent.
					response <- {
						if currentRole ne this then currentRole.onCommandFromClient(clientCommand, attemptFlag) // TODO analyze if the attemptFlag should be propagated as is or updated
						else primaryState1 match {
							case Inaccessible =>
								sequencer.LatchedDuty_ready(Unable(currentRole.ordinal, cluster.getOtherProbableParticipant))

							case accessible1: Accessible =>
								if assertionsEnabled then assert(accessible1.currentTerm == leadedTerm)
								primaryStateUpdaterResult.asMatchable match {
									case null =>
										sequencer.LatchedDuty_ready(InconsistentState(s"For client $clientId, the last known log-entry index does not point to a record of the expected type."))

									case (recordIndex, indexOfLastAppendedCommandFromClient, shouldRetire) =>
										if shouldRetire then become(Retiring(leadedTerm, deriveConfigurationFrom(accessible1).allParticipants)).onCommandFromClient(clientCommand, attemptFlag)
										else if recordIndex == 0 then sequencer.LatchedDuty_ready(Stale(clientCommand, indexOfLastAppendedCommandFromClient))
										else if recordIndex == -1 then sequencer.LatchedDuty_ready(TooOld(clientCommand))
										else {
											assert(recordIndex > 0)
											for {
												isCommitSuccessful <- {
													// if the command is not already commited, attempt the replication to a majority.
													if recordIndex > commitIndex then attemptToUpdateOtherParticipantsLogs(accessible1)
													else sequencer.LatchedDuty_true
												}
												response <- {
													if currentRole ne this then {
														currentRole.onCommandFromClient(clientCommand, attemptFlag) // TODO analyze if the attemptFlag should be propagated as is or updated
													} else if isCommitSuccessful then {
														// if the command is commited, apply it to the state machine to get the result assuming it is idempotent.
														for smr <- machine.applyClientCommand(recordIndex, clientCommand) yield {
															if recordIndex > highestAppliedCommandIndex then highestAppliedCommandIndex = recordIndex
															Processed(recordIndex, smr)
														}
													} else {
														// if not able to replicate then update the role and start again
														for {
															_ <- {
																scribe.trace(s"$boundParticipantId: Updating role due to insufficient quorum when replicating a command record.")
																startNewBallot()
																updateRole(false)
															}
															response <- currentRole.onCommandFromClient(clientCommand, attemptFlag) // TODO analyze if the attemptFlag should be propagated as is or updated
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

			/**
			 * Attempts to append the [[Record]]s that this participant has, to the logs of the participants that lack them.
			 * Detailed behavior:
			 *		- If [[Record]]s weren't appended to another participant, attempt to append them.
			 *			- If successful: update the corresponding entry of [[indexOfNextRecordToSend_ByParticipantIndex]] and [[highestRecordIndexKnownToBeAppended_ByParticipantIndex]].
			 *			- If AppendEntries fails because of log inconsistency: decrement the corresponding entry of [[indexOfNextRecordToSend_ByParticipantIndex]] and [[highestRecordIndexKnownToBeAppended_ByParticipantIndex]], and retry.
			 *		- If there exists an N such that N > [[commitIndex]], a majority of the [[highestRecordIndexKnownToBeAppended_ByParticipantIndex]] entries is ≥ N, and log[N].term == [[currentTerm]]: set commitIndex = N
			 *		- If there are unreachable participants (a minority whose corresponding entry in [[highestRecordIndexKnownToBeAppended_ByParticipantIndex]] trails the leader's [[commitIndex]]), initiate targeted retries to catch them up.
			 * @return a [[sequencer.LatchedDuty]] that yields true if, and only if, all the following are true:
			 *		- the [[currentRole]] is not changed during this process;
			 *		- none of the responses has a higher [[Term]];
			 *		- for all the participants sets of the current [[Configuration]], all the records in this participant's log are successfully appended to at least:
			 *			- half of the other participants of the set, if this participant belongs to the set;
			 *			- a majority of the other participants of the set, if this participant does not belong to the set.
			 * */
			private def attemptToUpdateOtherParticipantsLogs(primaryState0: Accessible): sequencer.LatchedDuty[Boolean] = {
				assert(primaryState0 eq primaryStateFence.committedState, s"$primaryState0 eq ${primaryStateFence.committedState}")
				// Set the serial number of this method execution.
				sequencerOfReplicationAttempts += 1
				val serialOfReplicationAttempt = sequencerOfReplicationAttempts
				// Memorize the index after the top record to include in the appends produced by this replication process.
				val indexAfterTopRecordToSend = primaryState0.firstEmptyRecordIndex
				scribe.trace(s"$boundParticipantId: starting replication #$serialOfReplicationAttempt until record index $indexAfterTopRecordToSend.")

				// Cancel the previous schedule to retry appends to unreachable followers, if any.
				unreachableFollowersRetrySchedule.foreach(sequencer.cancel(_))
				unreachableFollowersRetrySchedule = Maybe.empty

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

						if currentRole ne this then sequencer.LatchedDuty_false
						else if handoffAndBumpTermIfLessThan(highestTermIn(appendResponses0)) ne this then sequencer.LatchedDuty_false
						else for {
							primaryState1 <- primaryStateFence.causalAnchor()
							isReplicatedToMajority <- {
								if currentRole ne this then sequencer.LatchedDuty_false
								else primaryState1 match {
									case Inaccessible =>
										sequencer.LatchedDuty_false
									case accessible1: Accessible =>
										if assertionsEnabled then assert(accessible1.currentTerm == leadedTerm) // because the Leader Role should never bump the term before leaving transitioning to another Role.
										val config1 = deriveConfigurationFrom(accessible1)
										val appendResponses1 = rearrangeAppendResponses(appendResponses0, config0, config1)
										val appendOutcomes1 = appendResponses1.mapWithIndex { (appendResponse, otherParticipantIndex) =>
											// Handle the responses. Note that when successful, it updates the corresponding entry of the `indexOfNextRecordToSend_ByParticipantIndex` and `highestRecordIndexKnownToBeAppended_ByParticipantIndex` arrays.
											handleAppendResponse(accessible1, config1.allOtherParticipants(otherParticipantIndex), otherParticipantIndex, appendResponse, primaryState0.currentTerm, indexAfterTopRecordToSend, commitIndexAtAppendRequest)
										}
										scribe.trace(s"$boundParticipantId: The append responses of replication #$serialOfReplicationAttempt have been handled, excludingConfigIndex=$indexOfConfigChangeThatExcludedThisParticipant, aboutOthers=${(for i <- config1.allOtherParticipants.indices yield s"${config1.allOtherParticipants(i)}: outcome=${appendOutcomes1(i)}, nextToSend=${indexOfNextRecordToSend_ByParticipantIndex(i)}, knownAppended=${highestRecordIndexKnownToBeAppended_ByParticipantIndex(i)}, knowCommitted=${highestRecordIndexKnowToBeCommited_ByParticipantIndex(i)}").mkString("[", "; ", "]")}") // TODO delete
										for maybeLastAppendAttemptInfo2 <- retryLaggingLearners(accessible1, config1, appendOutcomes1, indexAfterTopRecordToSend, false, false, serialOfReplicationAttempt)
											yield {
												// If the behavior changed while waiting the result or the number of successful appends isn't enough to achieve quorum, return false.
												if (currentRole ne this) || maybeLastAppendAttemptInfo2.isEmpty then false
												// If the number of successful appends is enough to achieve quorum, then: update the `commitIndex`, retry laggards, and schedule retries for the unreachable participants, and yield true.
												else {
													// At this point, the result of the method is already determined. The following lines update derived state and start uncoupled retry processes.

													val (appendOutcomes2a, accessible2, config2a) = maybeLastAppendAttemptInfo2.get
													// Update the commitIndex if a majority of the followers have replicated the uncommited records.
													// If there exists an N such that N > commitIndex, the highest log-entry index known to be replicated is > N in a majority of the servers, and getRecordAt[N].term == currentTerm: set commitIndex = N
													val previousCommitIndex = commitIndex
													commitIndex = config2a.indexOfTheCommittedRecordWithHighestIndex(accessible2, previousCommitIndex, IArray.unsafeFromArray(highestRecordIndexKnownToBeAppended_ByParticipantIndex))
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

															scribe.trace(s"$boundParticipantId: replication #$serialOfReplicationAttempt final step: excludingConfigIndex=$indexOfConfigChangeThatExcludedThisParticipant, aboutOthers=${(for i <- config2b.allOtherParticipants.indices yield s"${config2b.allOtherParticipants(i)}: outcome=${appendOutcomes1(i)}, nextToSend=${indexOfNextRecordToSend_ByParticipantIndex(i)}, knownAppended=${highestRecordIndexKnownToBeAppended_ByParticipantIndex(i)}, knowCommitted=${highestRecordIndexKnowToBeCommited_ByParticipantIndex(i)}").mkString("[", "; ", "]")}") // TODO delete
															// If this leading participant is not included in the active configuration and all the followers in the new configuration have committed the StableConfigChange that excludes this participant, retire this participant.
															if isExcludedAndAllFollowersCommittedTheExcludingConfigChange then become(Retiring(leadedTerm, config2b.allParticipants)) // This point is reached if this participant was able to make the followers commit the StableConfigChange that excludes this leader before receiving an "appendRecords" call from the new leader.
															/// Else (if not retiring), for those minority of participants whose highestRecordIndexKnowToBeReplicated is less than the commitIndex (because append failed with IS_UNREACHABLE), retry the append records RPC. This retry is indefinite until the outer method (attemptToUpdateOtherParticipantsLogs) is called again (as the effect of an external stimulus).
															else scheduleUnreachableParticipantsRetry(lastAppendAttemptInfo3.updatedConfig.allOtherParticipants, lastAppendAttemptInfo3.lastAttemptOutcomes, indexAfterTopRecordToSend, serialOfReplicationAttempt)
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

			/** Creates a [[sequencer.Task]] that appends the specified range of [[Record]]s from this participant log to the specified destination participant.
			 * CAUTION: requires that the derived state variables had been updated by applying the [[deriveConfigurationFrom]] method to the provided [[PrimaryState]].
			 * @param primaryState the current [[PrimaryState]]
			 * @param destinationParticipantId the [[ParticipantId]] of the [[ConsensusParticipant]] to append the records to.
			 * @param destinationParticipantIndex the index of the [[ParticipantId]] in the [[Configuration.allOtherParticipants]] array of the [[Configuration]] derived from the provided [[PrimaryState]].
			 * @param fromIndex the [[RecordIndex]] of the first [[Record]] to include in the [[ClusterParticipant.appendRecords]] call.
			 * @param untilIndex the [[RecordIndex]] after the last [[Record]] to include in the [[ClusterParticipant.appendRecords]] call. */
			private def appendsRecordsToParticipant(primaryState: Accessible, destinationParticipantId: ParticipantId, destinationParticipantIndex: Int, fromIndex: RecordIndex, untilIndex: RecordIndex): AppendRequest = {
				if assertionsEnabled then {
					assert(isInSequence)
					assert(currentConfig eq deriveConfigurationFrom(primaryState))
				}

				// if the appending would be empty, with same `leaderCommit` as a previous successful append, and at least one append to the target was successful since ths participant is the leader, fake a successful response.
				if fromIndex >= untilIndex
					&& commitIndex == highestRecordIndexKnowToBeCommited_ByParticipantIndex(destinationParticipantIndex)
					&& highestRecordIndexKnownToBeAppended_ByParticipantIndex(destinationParticipantIndex) > 0
				then sequencer.Task_successful(AppendResult(primaryState.currentTerm, Maybe.empty, ISOLATED)) // TODO consider using a fake role ordinal instead of ISOLATED.
				else {
					val previousRecordIndex = fromIndex - 1
					val recordsToSend = primaryState.getRecordsBetween(fromIndex, untilIndex)
					val previousRecordTerm = primaryState.getRecordTermAt(previousRecordIndex)
					destinationParticipantId.appendRecords(primaryState.currentTerm, previousRecordIndex, previousRecordTerm, recordsToSend, commitIndex, primaryState.getRecordTermAt(commitIndex))
				}
			}

			private def highestTermIn(appendResponses: IArray[AppendResponse]): Term = {
				var latestTermSeen = 0
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
			 * This method is called solely from [[attemptToUpdateOtherParticipantsLogs]] and twice: before and after the top sent record is commited.
			 * @param accessible1 the current, accessible, and causally anchored [[PrimaryState]] of this [[ConsensusParticipant]].
			 * @param config1 the current updated [[Configuration]] of this [[ConsensusParticipant]].
			 * @param previousAttemptAppendOutcomes the [[AppendOutcome]]s of the results of the previous [[ClusterParticipant.appendRecords]] attempt, stored as a parallel array with index correspondence to `config1.allOtherParticipants`.
			 * @param indexAfterTopRecordSent the [[RecordIndex]] after the top [[Record]] sent in the calls to [[ClusterParticipant.appendRecords]].
			 * @param untilNoneLags instructs if the retries must continue until no learner is lagging (true), or until a majority of the appends is successful (false).
			 * @param serialOfReplicationAttempt the serial number of the call to [[attemptToUpdateOtherParticipantsLogs]] that initiated this method execution.
			 * @return a [[LatchedDuty]] that yields either:
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
			): sequencer.LatchedDuty[Maybe[(lastAttemptOutcomes: IArray[AppendOutcome], currentPrimaryState: Accessible, updatedConfig: Configuration)]] = {
				assert((accessible1 eq primaryStateFence.committedState) && (config1 eq currentConfig), s"$accessible1 eq ${primaryStateFence.committedState} && $config1 eq $currentConfig")
				// scribe.trace(s"$boundParticipantId: retryLaggingLearners($accessible1, $config1, ${previousAttemptAppendOutcomes.mkString("[", ", ", "]")}, $indexAfterTopRecordSent, $untilNoneLags, $serialOfReplicationAttempt)") // TODO delete line

				// If [[attemptToUpdateOtherParticipantsLogs]] was called again after the call that initiated this `retryLaggingLearners` execution, then there is no need to continue this execution because the later call to `attemptToUpdateOtherParticipantsLogs` will start a new one if necessary.
				if abortIfAnotherReplicationStarts && serialOfReplicationAttempt != sequencerOfReplicationAttempts then emptyLatchedDuty
				else {

					val noParticipantIsLagging = previousAttemptAppendOutcomes.forallWithIndex((previousOutcome, _) => (previousOutcome & AO_IS_LAGGING_MASK) == 0)
					if noParticipantIsLagging then {
						if untilNoneLags || config1.achievesQuorumWhen(previousAttemptAppendOutcomes) then sequencer.LatchedDuty_ready(Maybe((previousAttemptAppendOutcomes, accessible1, config1)))
						else emptyLatchedDuty
					}
					else if !untilNoneLags && config1.achievesQuorumWhen(previousAttemptAppendOutcomes) then sequencer.LatchedDuty_ready(Maybe((previousAttemptAppendOutcomes, accessible1, config1)))
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
						// scribe.trace(s"$boundParticipantId: retrying the lagging learners $laggingParticipants, newAppendRequests=$newAppendRequests") // TODO delete line
						val commitIndexAtAppendRequest = commitIndex
						for {
							newAppendResponses <- sequenceAppendRequests(newAppendRequests)
							newAppendAttemptInfo <- {
								if currentRole ne this then emptyLatchedDuty
								else if handoffAndBumpTermIfLessThan(highestTermIn(newAppendResponses)) ne this then emptyLatchedDuty
								else for {
									primaryState2 <- primaryStateFence.causalAnchor()
									newAppendAttemptInfo <- {
										// scribe.trace(s"$boundParticipantId: newAppendResponses=${newAppendResponses.mkString("[", ", ", "]")}, primaryState2=$primaryState2") // TODO delete line
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
															handleAppendResponse(accessible2, participantId, participantIndex2, newAppendResponses(newRequestIndex), accessible1.currentTerm, indexAfterTopRecordSent, commitIndexAtAppendRequest)
														}
													}
												}
												scribe.trace(s"$boundParticipantId: The append responses to the laggard-retry of replication #$serialOfReplicationAttempt have been handled, excludingConfigIndex=$indexOfConfigChangeThatExcludedThisParticipant, aboutOthers=${(for i <- config2.allOtherParticipants.indices yield s"${config2.allOtherParticipants(i)}: outcome=${newOutcomes(i)}, nextToSend=${indexOfNextRecordToSend_ByParticipantIndex(i)}, knownAppended=${highestRecordIndexKnownToBeAppended_ByParticipantIndex(i)}, knowCommitted=${highestRecordIndexKnowToBeCommited_ByParticipantIndex(i)}").mkString("[", "; ", "]")}") // TODO delete
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
			 * Updates the [[indexOfNextRecordToSend_ByParticipantIndex]], [[highestRecordIndexKnownToBeAppended_ByParticipantIndex]], and [[highestRecordIndexKnowToBeCommited_ByParticipantIndex]] arrays and maps the [[AppendResult]] to an [[AppendOutcome]].
			 * @param primaryState the current, causally anchored, accessible [[PrimaryState]].
			 * @param participantId the identifier of the participant whose response is being handled.
			 * @param participantIndex the index of the participant in the [[Configuration.allOtherParticipants]] derived from the provided [[PrimaryState]].
			 * @param appendRequestTerm the term passed to [[ClusterParticipant.appendRecords]] as `inquirerTerm` parameter.
			 * @param indexAfterTopRecordSent index of the record after the one at the top of the [[IndexedSeq]] of [[Record]]s passed to [[ClusterParticipant.appendRecords]] as argument to the parameter named `records`.
			 * @param appendRequestLeaderCommit the [[commitIndex]] of this [[Leader]] when [[ClusterParticipant.appendRecords]] was called. Must match the value passed to the `leaderCommit` parameter. */
			private def handleAppendResponse(primaryState: Accessible, participantId: ParticipantId, participantIndex: Int, appendResponse: AppendResponse | Null, appendRequestTerm: Term, indexAfterTopRecordSent: RecordIndex, appendRequestLeaderCommit: RecordIndex): AppendOutcome = {
				if appendResponse == null then AO_MISSING_BECAUSE_PARTICIPANT_WAS_NOT_PART_OF_THE_CONFIGURATION
				else appendResponse match {
					case Success(appendResult) =>
						val indexOfNextRecordToSend = indexOfNextRecordToSend_ByParticipantIndex(participantIndex)
						// scribe.trace(s"$boundParticipantId: handleAppendResponse@${primaryState.currentTerm} from $participantId requested@$appendRequestTerm, dialog=$appendResponse, indexAfterTopRecordSent=$indexAfterTopRecordSent") // TODO delete line
						appendResult.successOrIndexForNextAttempt.fold {
							highestRecordIndexKnowToBeCommited_ByParticipantIndex(participantIndex) = appendRequestLeaderCommit
							if indexAfterTopRecordSent > indexOfNextRecordToSend then indexOfNextRecordToSend_ByParticipantIndex(participantIndex) = indexAfterTopRecordSent
							val indexOfTopRecordSent = indexAfterTopRecordSent - 1
							if indexOfTopRecordSent > highestRecordIndexKnownToBeAppended_ByParticipantIndex(participantIndex) then highestRecordIndexKnownToBeAppended_ByParticipantIndex(participantIndex) = indexOfTopRecordSent
							AO_SUCCESS
						} { indexForNextAttempt => // else (if the participant rejected the appending)
							// If the rejection was because the participant needs earlier records (the previous record's does not exist in its log or its term is not the same as in this participant log then):
							if appendResult.roleOrdinal >= JOINING && appendResult.term == appendRequestTerm then {
								// If the record does not predate the snapshot, then decrement the index of the next record to send and return AO_NEEDS_EARLIER_RECORDS so a retry is fired with one more earlier record.
								if indexForNextAttempt >= primaryState.logBufferOffset then {
									if indexOfNextRecordToSend > indexForNextAttempt && indexForNextAttempt >= highestRecordIndexKnownToBeAppended_ByParticipantIndex(participantIndex) + 1 then indexOfNextRecordToSend_ByParticipantIndex(participantIndex) = indexForNextAttempt
									AO_NEEDS_EARLIER_RECORDS
								}
								// else (previous record predates snapshot):
								else {
									// inform about the illegal state. // TODO analyze a better way to inform this problem after implementing the log snapshot mechanism if this situation is possible to happen at all (because highestRecordIndexKnownToBeAppended_ByParticipantIndex would always be greater than or equal to the lobBufferOffset).
									scribe.error(s"$boundParticipantId: Unable to replicate uncommited records to $participantId because its log has inconsistencies at records that predate the last snapshot. They are at indexes less than the logBufferOffset=${primaryState.logBufferOffset}. THIS SHOULD NOT HAPPEN. PLEASE REPORT THIS AS A BUG.")
									// return false without any change to the corresponding entry in indexOfNextRecordToSend_ByParticipantIndex and highestRecordIndexKnownToBeAppended_ByParticipantIndex.
									AO_NEEDS_RECORDS_THAT_PREDATE_SNAPSHOT
								}
							}
							// if the rejection was for another reason, yield a hint of the motive.
							else if appendResult.roleOrdinal <= RETIRING then AO_IS_RETIRING_OR_STOPPING
							else if appendResult.roleOrdinal == STARTING then AO_IS_STARTING
							else AO_UNEXPECTED
						}

					case Failure(e) =>
						scribe.debug(s"$boundParticipantId: The replication to $participantId failed with:", e)
						AO_IS_UNREACHABLE
				}
			}

			/**
			 * Schedule [[ClusterParticipant.appendRecords]] calls for the participants that were [[AO_IS_UNREACHABLE]] in the previous attempt.
			 * @param correspondingParticipantIds the [[ParticipantId]] corresponding to the provided [[AppendOutcome]] array.
			 * @param previousAttemptOutcomes the [[AppendOutcome]]s of the previous attempt.
			 * @param indexAfterTopRecordSent the index after the top [[Record]] of the set of [[Records]] that should be included in the attempts. */
			private def scheduleUnreachableParticipantsRetry(correspondingParticipantIds: IArray[ParticipantId], previousAttemptOutcomes: IArray[AppendOutcome], indexAfterTopRecordSent: RecordIndex, serialOfReplicationAttempt: Int): Unit = {

				// if there are unreachable participants, schedule a retry that replicates the log to them.
				if serialOfReplicationAttempt == sequencerOfReplicationAttempts && previousAttemptOutcomes.contains(AO_IS_UNREACHABLE) then {
					val schedule = sequencer.newDelaySchedule(failedReplicationsLoopInterval)
					unreachableFollowersRetrySchedule = Maybe(schedule)
					sequencer.schedule(schedule) { _ =>
						if currentRole eq this then {
							for {
								primaryState1 <- primaryStateFence.causalAnchor()
							} yield {
								if (currentRole eq this) && serialOfReplicationAttempt == sequencerOfReplicationAttempts then primaryState1 match {
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
										scribe.trace(s"$boundParticipantId: Retrying the appendRecords RPCs to the unreachable participants $unreachableParticipantIds in replication #$serialOfReplicationAttempt")
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
														else handleAppendResponse(accessible2, participantId, participantIndex, appendResult, accessible1.currentTerm, indexAfterTopRecordSent, commitIndexAtAppendRequest)
													}
													scribe.trace(s"$boundParticipantId: The append responses to the unreachable-retry of replication #$serialOfReplicationAttempt have been handled, excludingConfigIndex=$indexOfConfigChangeThatExcludedThisParticipant, outcomes=${unreachableParticipantIds.zip(appendsOutcomes)}") // TODO delete
													scheduleUnreachableParticipantsRetry(IArray.from(unreachableParticipantIds), appendsOutcomes, indexAfterTopRecordSent, serialOfReplicationAttempt)
											}
										}
								}
							}
						}
					}
				}
			}

			/**
			 * Caution: this method is called from a [[PrimaryState]] updater.
			 * @return true if this leading participant is not included in the active [[Configuration]] and all the followers in the new [[Configuration]] have committed the [[StableConfigChange]] that excludes this participant. */
			private def isExcludedAndAllFollowersCommittedTheExcludingConfigChange: Boolean = {
				indexOfConfigChangeThatExcludedThisParticipant > 0
					&& commitIndex >= indexOfConfigChangeThatExcludedThisParticipant
					&& IArray.unsafeFromArray(highestRecordIndexKnowToBeCommited_ByParticipantIndex).forallWithIndex((highestRecordIndexKnowToBeCommited, _) => highestRecordIndexKnowToBeCommited >= indexOfConfigChangeThatExcludedThisParticipant)
			}

			/** Consolidates the many [[sequencer.Task]]s into a single [[sequencer.LatchedDuty]] that yields an array with the results of the [[sequencer.Task]]s.
			 * TODO this is inefficient because the pace is determined by the slowest. Implement it using a [[sequencer.StreamDuty]] instead. */
			private inline def sequenceAppendRequests(appendRequests: scala.collection.IndexedSeq[AppendRequest]): sequencer.LatchedDuty[IArray[AppendResponse]] = {
				for appendDialog <- sequencer.LatchedDuty_sequenceTasksToArray(appendRequests, true) yield IArray.unsafeFromArray(appendDialog)
			}

			def handoffAndBumpTermIfLessThan(seenTerm: Term): Role = {
				if thisLeader.leadedTerm < seenTerm then {
					scribe.trace(s"$boundParticipantId: About to hand-off due to a higher term seen.")
					become(HandingOff(thisLeader.leadedTerm, seenTerm, primaryStateFence))
				} else thisLeader
			}

			override def onTermBumped(primaryState: PrimaryState): Unit = {
				scribe.trace(s"$boundParticipantId: About to hand-off due to term bump.")
				super.onTermBumped(primaryState)
				val config = deriveConfigurationFrom(primaryState)
				if config.allParticipants.contains(boundParticipantId) then become(Isolated(primaryStateFence))
				else become(Retiring(primaryState.currentTerm, config.allParticipants))
			}
		}

		/** Responsible for making a retiring participant to append the unappended records until the [[StableConfigChange]] that caused its exclusión, passing a `leaderCommit` equal to the index of that same [[StableConfigChange]] record.
		 * @param id the [[ParticipantId]] of the retiring participant.
		 * @param termOfHighestRecordKnownToBeAppended the term of the [[Record]] immediately before the first potentially unappended [[Record]]. Should be the term of the highest [[Record]] known to be appended.
		 * @param potentiallyUnappendedRecords a sequence starting with the lowest [[Record]] potentially unappended by the retiree, up to the [[StableConfigChange]].
		 * @param indexOfFirstPotentiallyUnappendedRecord index of the first [[Record]] potentially unappended.
		 * @param configChangeIndex the index of the [[StableConfigChange]] record that caused the exclusión.
		 * @param configChangeTerm the term of the [[StableConfigChange]] record.
		 * @param indexOfNextRecordToSend the nextRecord to send top-bounded by the index of the [[StableConfigChange]].
		 * */
		private class RetireeAgent(
			id: ParticipantId,
			primaryStateFence: sequencer.CausalFence[PrimaryState],
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
			 * This method is called immediately after this [[RetireeAgent]] instance is created and added to the [[retireeAgentByParticipantId]] map, which happens during a [[Configuration]] transition.
			 *
			 * CAUTION: this method is indirectly called by the synchronous part of the [[Leader.onEnter]] method which requires no synchronous calls to [[become]]. So, transitively, this method must also not call [[become]] synchronously.
			 *  @param primaryState the current [[PrimaryState]]. In the first call is the [[PrimaryState]] from which the [[Configuration]] transition that produced this [[RetireeAgent]] is derived. In the recursión calls is the [[PrimaryState]] at when the [[AppendResponse]] arrived. */
			def startAppendLoop(primaryState: Accessible, retriesDone: Int = 0): Unit = {

				scribe.trace(s"$boundParticipantId: RetireeAgent($id, term=$termOfHighestRecordKnownToBeAppended, records=$potentiallyUnappendedRecords, firstIndex=$indexOfFirstPotentiallyUnappendedRecord, changeIndex=$configChangeIndex, changeTerm=$configChangeTerm, indexOfNextRecordToSend=$indexOfNextRecordToSend).startAppendLoop(retriesDone=$retriesDone) called") // TODO delete line
				val inquire =
					if indexOfNextRecordToSend > configChangeIndex then {
						id.appendRecords(
							primaryState.currentTerm,
							configChangeIndex,
							configChangeTerm,
							IndexedSeq.empty,
							configChangeIndex,
							configChangeTerm
						)
					} else {
						val previousRecordIndex = indexOfNextRecordToSend - 1
						val previousRecordTerm = {
							if previousRecordIndex < indexOfFirstPotentiallyUnappendedRecord then termOfHighestRecordKnownToBeAppended
							else potentiallyUnappendedRecords((previousRecordIndex - indexOfFirstPotentiallyUnappendedRecord).toInt).term
						}
						id.appendRecords(
							primaryState.currentTerm,
							previousRecordIndex,
							previousRecordTerm,
							potentiallyUnappendedRecords.drop((indexOfNextRecordToSend - indexOfFirstPotentiallyUnappendedRecord).toInt),
							configChangeIndex,
							configChangeTerm
						)
					}
				inquire.trigger(true) { response =>
					if retireeAgentByParticipantId.contains(id) then response match {
						case Success(appendResult) =>
							// scribe.trace(s"$boundParticipantId: handleAppendResponse@${primaryState.currentTerm} from $participantId requested@$appendRequestTerm, dialog=$appendResponse, indexAfterTopRecordSent=$indexAfterTopRecordSent") // TODO delete line
							// if the append was successful
							appendResult.successOrIndexForNextAttempt.fold(removeRetireeAgent(id)) { indexForNextAttempt =>
								// else, if the retiree needs earlier records, retry including one earlier record.
								if appendResult.roleOrdinal >= JOINING && appendResult.term == primaryState.currentTerm then {
									indexOfNextRecordToSend = indexForNextAttempt
									if indexForNextAttempt >= indexOfFirstPotentiallyUnappendedRecord then {
										primaryStateFence.causalAnchor { (primaryStateAtResponse, _) =>
											if retireeAgentByParticipantId.contains(id) then primaryStateAtResponse match {
												case accessible: Accessible => startAppendLoop(accessible)
												case _ => illegalStateStop()
											}
										}
									} else scribe.error(s"$boundParticipantId: This should not happen! The retiring participant $id asks for earlier records than the expected: result=$appendResult, indexOfFirstPotentiallyUnappendedRecord=$indexOfFirstPotentiallyUnappendedRecord, configChangeIndex=$configChangeIndex, configChangeTerm=$configChangeTerm")
								}
								// if the rejection was for another reason
								else {
									removeRetireeAgent(id)
									scribe.warn(s"$boundParticipantId: The replication to retiring participant $id was aborted because its response tells it does not need a retry: result=$appendResult, indexOfFirstPotentiallyUnappendedRecord=$indexOfFirstPotentiallyUnappendedRecord, configChangeIndex=$configChangeIndex, configChangeTerm=$configChangeTerm")
								}
							}

						case Failure(e) =>
							if retriesDone > retiringParticipantMaxRetries then scribe.error(s"$boundParticipantId: The replication to the retiring participant $id is aborted because it failed too many times. The last attempt failure was:", e)
							else {
								val nextRetryNumber = retriesDone + 1
								scribe.debug(s"$boundParticipantId: The replication to the retiring participant $id failed. Scheduling retry #$nextRetryNumber. The failure was:", e)
								sequencer.schedule(sequencer.newDelaySchedule(retiringParticipantRetryPeriod)) { _ =>
									if retireeAgentByParticipantId.contains(id) then {
										primaryStateFence.causalAnchor { (primaryState, _) =>
											primaryState match {
												case Inaccessible =>
													illegalStateStop()
												case accessible: Accessible =>
													if retireeAgentByParticipantId.contains(id) then startAppendLoop(accessible, nextRetryNumber)
											}
										}
									}
								}
							}
					}
				}
			}
		}

		private def removeRetireeAgent(retireeId: ParticipantId): Unit = {
			retireeAgentByParticipantId.remove(retireeId)
			if retireeAgentByParticipantId.isEmpty && currentRole.ordinal == RETIRING then become(Stopped(Success(s"The last retiree agent has completed its job.")))
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

			def withTermUpdated(term: Term): sequencer.LatchedDuty[PrimaryState]
		}

		/** The [[PrimaryState]] value when this [[ConsensusParticipant]] does not have access to the [[Storage]] where the primary state is persisted. Either because it does not need it (gracefully [[Stopped]]), is [[Starting]], or became [[Stopped]] due to a failure. */
		private object Inaccessible extends PrimaryState {
			override val currentTerm: Term = 0
			override val firstEmptyRecordIndex: RecordIndex = 0

			override def withTermUpdated(term: Term): sequencer.LatchedDuty[PrimaryState] = sequencer.LatchedDuty_ready(this)
		}

		/** Defines the [[PrimaryState]] when this [[ConsensusParticipant]] has access to the [[Storage]] where the primary state is persisted. */
		private final class Accessible(workspace: WS) extends PrimaryState {

			override val currentTerm: Term = workspace.getCurrentTerm
			override val firstEmptyRecordIndex: RecordIndex = workspace.firstEmptyRecordIndex

			/** CAUTION: This method accesses mutable state of the [[PrimaryState]], so it should be called either:
			 *   - within an [[StatefulRole.primaryStateFence.advance]] section. In this case, the call must occur before or during the completion of the [[sequencer.LatchedDuty]] returned by the function passed	to `advance`; once that duty has completed, the causal fence is closed and later calls are unsafe.
			 *   - within a causally anchored consumer (i.e. consumers subscribed to the [[sequencer.LatchedDuty]] returned by either [[StatefulRole.primaryStateFence.advance]] or [[StatefulRole.primaryStateFence.causalAnchor]]). In this case, the call must occur synchronously during the consumer’s execution; it must not be deferred to code scheduled after the consumer has returned, since such deferred code would no longer be causally anchored. */
			inline def getRecordAt(index: RecordIndex): Record =
				workspace.getRecordAt(index)


			/** CAUTION: This method accesses mutable state of the [[PrimaryState]], so it should be called either:
			 *   - within an [[StatefulRole.primaryStateFence.advance]] section. In this case, the call must occur before or during the completion of the [[sequencer.LatchedDuty]] returned by the function passed	to `advance`; once that duty has completed, the causal fence is closed and later calls are unsafe.
			 *   - within a causally anchored consumer (i.e. consumers subscribed to the [[sequencer.LatchedDuty]] returned by either [[StatefulRole.primaryStateFence.advance]] or [[StatefulRole.primaryStateFence.causalAnchor]]). In this case, the call must occur synchronously during the consumer’s execution; it must not be deferred to code scheduled after the consumer has returned, since such deferred code would no longer be causally anchored. */
			inline def getRecordTermAt(index: RecordIndex): Term =
				if index == 0 then 0
				else workspace.getRecordAt(index).term

			/** CAUTION: This method accesses mutable state of the [[PrimaryState]], so it should be called either:
			 *   - within an [[StatefulRole.primaryStateFence.advance]] section. In this case, the call must occur before or during the completion of the [[sequencer.LatchedDuty]] returned by the function passed	to `advance`; once that duty has completed, the causal fence is closed and later calls are unsafe.
			 *   - within a causally anchored consumer (i.e. consumers subscribed to the [[sequencer.LatchedDuty]] returned by either [[StatefulRole.primaryStateFence.advance]] or [[StatefulRole.primaryStateFence.causalAnchor]]). In this case, the call must occur synchronously during the consumer’s execution; it must not be deferred to code scheduled after the consumer has returned, since such deferred code would no longer be causally anchored. */
			inline def getRecordsBetween(from: RecordIndex, until: RecordIndex): GenIndexedSeq[Record] = {
				workspace.getRecordsBetween(from, until)
			}

			/** CAUTION: This method accesses mutable state of the [[PrimaryState]], so it should be called either:
			 *   - within an [[StatefulRole.primaryStateFence.advance]] section. In this case, the call must occur before or during the completion of the [[sequencer.LatchedDuty]] returned by the function passed	to `advance`; once that duty has completed, the causal fence is closed and later calls are unsafe.
			 *   - within a causally anchored consumer (i.e. consumers subscribed to the [[sequencer.LatchedDuty]] returned by either [[StatefulRole.primaryStateFence.advance]] or [[StatefulRole.primaryStateFence.causalAnchor]]). In this case, the call must occur synchronously during the consumer’s execution; it must not be deferred to code scheduled after the consumer has returned, since such deferred code would no longer be causally anchored. */
			inline def logBufferOffset: RecordIndex =
				workspace.logBufferOffset

			/** CAUTION: This method accesses mutable state of the [[PrimaryState]], so it should be called either:
			 *   - within an [[StatefulRole.primaryStateFence.advance]] section. In this case, the call must occur before or during the completion of the [[sequencer.LatchedDuty]] returned by the function passed	to `advance`; once that duty has completed, the causal fence is closed and later calls are unsafe.
			 *   - within a causally anchored consumer (i.e. consumers subscribed to the [[sequencer.LatchedDuty]] returned by either [[StatefulRole.primaryStateFence.advance]] or [[StatefulRole.primaryStateFence.causalAnchor]]). In this case, the call must occur synchronously during the consumer’s execution; it must not be deferred to code scheduled after the consumer has returned, since such deferred code would no longer be causally anchored. */
			inline def indexOfTopConfigChange: RecordIndex =
				workspace.indexOfTopConfigChange

			/** CAUTION: This method accesses mutable state of the [[PrimaryState]], so it should be called either:
			 *   - within an [[StatefulRole.primaryStateFence.advance]] section. In this case, the call must occur before or during the completion of the [[sequencer.LatchedDuty]] returned by the function passed	to `advance`; once that duty has completed, the causal fence is closed and later calls are unsafe.
			 *   - within a causally anchored consumer (i.e. consumers subscribed to the [[sequencer.LatchedDuty]] returned by either [[StatefulRole.primaryStateFence.advance]] or [[StatefulRole.primaryStateFence.causalAnchor]]). In this case, the call must occur synchronously during the consumer’s execution; it must not be deferred to code scheduled after the consumer has returned, since such deferred code would no longer be causally anchored. */
			inline def indexOfLastAppendedCommandFrom(clientId: ClientId): RecordIndex = {
				workspace.indexOfLastAppendedCommandFrom(clientId)
			}

			/** CAUTION: This method accesses mutable state of the [[PrimaryState]], so it should be called either:
			 *   - within an [[StatefulRole.primaryStateFence.advance]] section. In this case, the call must occur before or during the completion of the [[sequencer.LatchedDuty]] returned by the function passed	to `advance`; once that duty has completed, the causal fence is closed and later calls are unsafe.
			 *   - within a causally anchored consumer (i.e. consumers subscribed to the [[sequencer.LatchedDuty]] returned by either [[StatefulRole.primaryStateFence.advance]] or [[StatefulRole.primaryStateFence.causalAnchor]]). In this case, the call must occur synchronously during the consumer’s execution; it must not be deferred to code scheduled after the consumer has returned, since such deferred code would no longer be causally anchored. */
			inline def indexOf(clientCommand: ClientCommand): RecordIndex =
				workspace.indexOf(clientCommand)

			/** CAUTION: This method accesses mutable state of the [[PrimaryState]], so it should be called either:
			 *   - within an [[StatefulRole.primaryStateFence.advance]] section. In this case, the call must occur before or during the completion of the [[sequencer.LatchedDuty]] returned by the function passed	to `advance`; once that duty has completed, the causal fence is closed and later calls are unsafe.
			 *   - within a causally anchored consumer (i.e. consumers subscribed to the [[sequencer.LatchedDuty]] returned by either [[StatefulRole.primaryStateFence.advance]] or [[StatefulRole.primaryStateFence.causalAnchor]]). In this case, the call must occur synchronously during the consumer’s execution; it must not be deferred to code scheduled after the consumer has returned, since such deferred code would no longer be causally anchored. */
			inline def informAppliedCommandIndex(appliedCommandIndex: RecordIndex): Unit = {
				workspace.informAppliedCommandIndex(appliedCommandIndex)
			}	

			/** CAUTION: This method mutates of the [[PrimaryState]], so it should be called within the safe temporal windows provided by [[StatefulRole.primaryStateFence.advance]]. */
			override def withTermUpdated(newTerm: Term): sequencer.LatchedDuty[PrimaryState] = {
				val currentTerm = workspace.getCurrentTerm
				if newTerm < currentTerm then sequencer.LatchedDuty_ready(Accessible(workspace))
				else if newTerm == currentTerm then sequencer.LatchedDuty_ready(this)
				else {
					workspace.setCurrentTerm(newTerm)
					saveWorkspace()
				}
			}

			/** CAUTION: This method mutates of the [[PrimaryState]], so it should be called within the safe temporal windows provided by [[StatefulRole.primaryStateFence.advance]]. */
			def withSingleRecordAppended(term: Term, record: Record): sequencer.LatchedDuty[PrimaryState] = {
				workspace.setCurrentTerm(term)
				workspace.appendRecord(record)
				saveWorkspace()
			}

			/** CAUTION: This method mutates of the [[PrimaryState]], so it should be called within the safe temporal windows provided by [[StatefulRole.primaryStateFence.advance]]. */
			def withRecordsAppended(term: Term, records: GenIndexedSeq[Record], from: RecordIndex): sequencer.LatchedDuty[PrimaryState] = {
				workspace.setCurrentTerm(term)
				workspace.appendResolvingConflicts(records, from)
				saveWorkspace()
			}

			/** CAUTION: This method mutates of the [[PrimaryState]], so it should be called within the safe temporal windows provided by [[StatefulRole.primaryStateFence.advance]]. */
			def withWorkspaceReleased(): sequencer.Duty[PrimaryState] =
				workspace.releases.map(_ => Inaccessible)

			/** Saves the [[Workspace]] of this [[PrimaryState]] in the [[Storage]].
			 * CAUTION: This method accesses mutable state of the [[PrimaryState]], so it should be called within an [[StatefulRole.primaryStateFence.advance]] section only.
			 * @return the saved [[PrimaryState]] */
			private def saveWorkspace(): sequencer.LatchedDuty[PrimaryState] = {
				storage.save(workspace).map {
					case _: Success[Unit] =>
						if currentRole.ordinal <= STARTING then {
							workspace.releases.triggerAndForget()
							Inaccessible
						}
						else Accessible(workspace)

					case failure: Failure[Unit] =>
						scribe.error(s"$boundParticipantId: Unexpected error while saving the workspace. This participant's consensus service is unable to continue following the leader and will stop.", failure.exception)
						become(Stopped(failure.castTo[String]))
						workspace.releases.triggerAndForget() // just in case storage.save does not do it.
						Inaccessible
				}
			}

			override def toString: String = s"Accessible(currentTerm=$currentTerm, firstEmptyRecordIndex=$firstEmptyRecordIndex, indexOfTopConfigChange=$indexOfTopConfigChange)"
		}

		/** Knows which are the participants involved in the consensus and defines rules that govern replication, quorum formation, and elections.
		 *
		 * It has exactly three concrete subclasses:
		 *
		 *	- [[NoConfig]]: Active when lack of access to the needed information.
		 *
		 *	- [[TransitionalConfig]]: Active during joint consensus (`Cold` ∪ `Cnew`).
		 *     This behavior begins immediately once the transitional entry is appended to the log.
		 *     Replication and quorum require majorities across both `Cold` and `Cnew`.
		 *     Elections must also consider both sets.
		 *     Transitional behavior remains active until a [[StableConfigChange]] entry is replicated to a majority of both `Cold` and `Cnew` ([[commitIndex]] >= indexOfStableConfigChange).
		 *
		 *	- [[StableConfig]]: Active during non-joint consensus (Cnew).
		 *     This behavior begins only once the stable entry is committed (i.e. when [[commitIndex]] ≥ indexOfStableConfigChange).
		 *     Replication and quorum reduce to `Cnew` only.
		 *     `Cold` only servers, having seen the stable entry, shut down.
		 *
		 * === Commit Index Dependency ===
		 * - [[TransitionalConfig]] behavior starts on append of its entry, but ends when the stable entry is committed.
		 * - [[StableConfig]] behavior starts only when its entry is committed.
		 * Thus, despite each of these two behaviors depend only on primary state (the log), the whole configuration behavior is commit-sensitive because the transition instant between them depends on [[commitIndex]].
		 * */
		private sealed trait Configuration {
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

			/** The identifiers of the desired set of participants. */
			val desiredParticipants: Set[ParticipantId]

			/** The set of participants to include in [[Unable]] responses. */
			def otherProbableParticipants: ListSet[ParticipantId]

			def reachedAll(vote: Vote[ParticipantId]): Boolean

			def reachedAMajority(vote: Vote[ParticipantId]): Boolean

			def indexOfTheCommittedRecordWithHighestIndex(primaryState: Accessible, from: RecordIndex, highestRecordIndexKnowToBeAppended_ByParticipantIndex: IArray[RecordIndex]): RecordIndex

			/** @param appendOutcomes the [[AppendOutcome]] of each other participant, indexed according to [[allOtherParticipants]].
			 * @return true if at least half of the [[AppendOutcome]]s in the provided array are [[AO_SUCCESS]]. */
			def achievesQuorumWhen(appendOutcomes: IArray[AppendOutcome]): Boolean

			/** Determines the [[Role]] to become based on the votes of all the participants. */
			def determineRole(primaryState: Accessible, primaryStateFence: sequencer.CausalFence[PrimaryState], myVote: Vote[ParticipantId], votesFromOthers: Array[Try[Vote[ParticipantId]]]): Role

			/**
			 * Determines the best leader candidate based on the [[StateInfo]]s of all the participants, including itself.
			 * This method only queries. Does not mutate anything.
			 *
			 * @param howAreYouAnswers the answers to the [[ClusterParticipant.howAreYou]] questions done to the other participants, stored as a parallel array with index correspondence [[allOtherParticipants]].
			 * @return A task that yields a [[Vote]] with the chosen leader for the current term.
			 */
			def decideMyVote(myStateInfo: StateInfo, howAreYouAnswers: IArray[StateInfo | Null]): Vote[ParticipantId]

			/** @return the index of the provided [[ParticipantId]] in the [[allOtherParticipants]]' [[IndexedSeq]] or a negative number if not present.
			 * @param otherParticipantId the id of the participant to find. */
			inline def participantIndexOf(otherParticipantId: ParticipantId): Int = {
				java.util.Arrays.binarySearch(allOtherParticipants.asInstanceOf[Array[ParticipantId]], otherParticipantId, participantIdComparator)
			}

			override def toString: String = backingConfigChange.toString
		}

		private object NoConfig extends Configuration {
			override val allParticipants: IArray[ParticipantId] =
				IArray(boundParticipantId)

			override val allOtherParticipants: IArray[ParticipantId] =
				IArray.empty

			override val backingConfigChange: ConfigChange[ParticipantId] =
				null

			override val desiredParticipants: Set[ParticipantId] =
				Set.empty

			def otherProbableParticipants: ListSet[ParticipantId] =
				cluster.getOtherProbableParticipant

			override def reachedAll(vote: Vote[ParticipantId]): Boolean =
				false

			override def reachedAMajority(vote: Vote[ParticipantId]): Boolean =
				false

			override def indexOfTheCommittedRecordWithHighestIndex(primaryState: Accessible, from: RecordIndex, highestRecordIndexKnowToBeAppended_ByParticipantIndex: IArray[RecordIndex]): RecordIndex =
				from

			override def achievesQuorumWhen(appendOutcomes: IArray[AppendOutcome]): Boolean =
				false

			override def determineRole(primaryState: Accessible, primaryStateFence: sequencer.CausalFence[PrimaryState], myVote: Vote[ParticipantId], votesFromOthers: Array[Try[Vote[ParticipantId]]]): Role =
				illegalStateStop()

			override def decideMyVote(myStateInfo: StateInfo, howAreYouAnswers: IArray[StateInfo | Null]): Vote[ParticipantId] =
				currentRole.blankVote
		}

		/** Pure new configuration (`Cnew`).
		 *
		 * Activated only once the [[StableConfigChange]] entry is committed (commitIndex ≥ indexOfStableConfigChange).
		 * Replication and quorum reduce to `Cnew` only.
		 * Cold-only servers, having seen this entry, shut down.
		 */
		private final class StableConfig(override val backingConfigChange: StableConfigChange[ParticipantId]) extends Configuration {
			override val allParticipants: IArray[ParticipantId] = IArray.unsafeFromArray(backingConfigChange.newParticipants.toArray.sorted)
			private val halfTheNumberOfParticipants = allParticipants.length / 2
			override val allOtherParticipants: IArray[ParticipantId] = allParticipants.filter(_ != boundParticipantId)

			override val desiredParticipants: Set[ParticipantId] =
				backingConfigChange.newParticipants

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

			override def indexOfTheCommittedRecordWithHighestIndex(primaryState: Accessible, from: RecordIndex, highestRecordIndexKnowToBeAppended_ByParticipantIndex: IArray[RecordIndex]): RecordIndex = {
				var n = primaryState.firstEmptyRecordIndex - 1
				while n > from && (
					highestRecordIndexKnowToBeAppended_ByParticipantIndex.countWithIndex((hri, _) => hri >= n) < halfTheNumberOfParticipants
						|| primaryState.getRecordTermAt(n) != primaryState.currentTerm
					)
				do n -= 1
				n
			}

			override def achievesQuorumWhen(appendSummaries: IArray[AppendOutcome]): Boolean = {
				appendSummaries.countWithIndex { (summary, index) => summary == AO_SUCCESS } >= halfTheNumberOfParticipants
			}

			override def determineRole(primaryState: Accessible, primaryStateFence: sequencer.CausalFence[PrimaryState], myVote: Vote[ParticipantId], votesFromOthers: Array[Try[Vote[ParticipantId]]]): Role = {
				var votesMatchingMyVoteCount = 1 // includes my vote
				for case Success(replierVote) <- votesFromOthers do {
					if replierVote.votedId == myVote.votedId then votesMatchingMyVoteCount += 1
				}
				if votesMatchingMyVoteCount <= halfTheNumberOfParticipants then Isolated(primaryStateFence)
				else if myVote.votedId == boundParticipantId then Promoting(primaryState.currentTerm, primaryStateFence)
				else Follower(primaryState.currentTerm, myVote.votedId, primaryStateFence)
			}

			override def decideMyVote(myStateInfo: StateInfo, howAreYouAnswers: IArray[StateInfo | Null]): Vote[ParticipantId] = {
				var latestTermSeen = myStateInfo.currentTerm
				var chosenCandidate = CandidateInfo(boundParticipantId, true, myStateInfo)
				var borrame = List(chosenCandidate) //TODO delete line
				var reachableCandidatesCounter = 1 // includes itself

				howAreYouAnswers.foreachWithIndex { (reply, replierIndex) =>
					val replierId = allOtherParticipants(replierIndex)
					reply match {
						case null =>
							scribe.debug(s"$boundParticipantId: `$replierId.howAreYou` failed while deciding vote")

						case replierInfo: StateInfo =>
							reachableCandidatesCounter += 1
							if replierInfo.currentTerm > latestTermSeen then {
								latestTermSeen = replierInfo.currentTerm
							}
							val candidateInfo = CandidateInfo(replierId, true, replierInfo)
							borrame = candidateInfo :: borrame //TODO delete line
							chosenCandidate = chosenCandidate.getWinnerAgainst(candidateInfo)

					}
				}
				scribe.debug(s"$boundParticipantId: decideMyVote($myStateInfo, ${howAreYouAnswers.mkString("[", ", ", "]")}): chosen=$chosenCandidate, among: $borrame, config=$backingConfigChange") //TODO delete line
				Vote(latestTermSeen, chosenCandidate.id, reachableCandidatesCounter, 0, chosenCandidate.info.rank, currentBallot)
			}
		}

		/** Joint consensus configuration (Cold ∪ Cnew).
		 *
		 * Activated immediately upon append of the transitional entry.
		 * Replication and quorum require majorities across both Cold and Cnew.
		 * Elections must also consider both sets.
		 * Ends when a [[StableConfig]] entry is committed.
		 */
		private final class TransitionalConfig(override val backingConfigChange: TransitionalConfigChange[ParticipantId]) extends Configuration {
			private val oldParticipants: Set[ParticipantId] = backingConfigChange.oldParticipants
			private val newParticipants: Set[ParticipantId] = backingConfigChange.newParticipants
			private val halfOfOldParticipants: Int = oldParticipants.size / 2
			private val halfOfNewParticipants: Int = newParticipants.size / 2
			override val allParticipants: IArray[ParticipantId] = IArray.unsafeFromArray(newParticipants.union(oldParticipants).toArray.sorted)
			override val allOtherParticipants: IArray[ParticipantId] = allParticipants.filter(_ != boundParticipantId)

			override val desiredParticipants: Set[ParticipantId] =
				backingConfigChange.newParticipants

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

			override def indexOfTheCommittedRecordWithHighestIndex(primaryState: Accessible, from: RecordIndex, highestRecordIndexKnowToBeAppended_ByParticipantIndex: IArray[RecordIndex]): RecordIndex = {
				var n = primaryState.firstEmptyRecordIndex - 1
				while n > from do {
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
								if highestRecordIndexKnowToBeAppended_ByParticipantIndex(otherParticipantIndex) >= n then {
									participantId = allOtherParticipants(otherParticipantIndex)
									goNext = false
								}
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

			override def achievesQuorumWhen(appendSummaries: IArray[AppendOutcome]): Boolean = {
				var newParticipantsWithSuccessfulAppendResult = 0
				var oldParticipantsWithSuccessfulAppendResult = 0
				// start the loop with this participant, assuming it already appended the records and will persist its state after calling this method.
				var participantId = boundParticipantId
				var otherParticipantIndex = allOtherParticipants.length
				while otherParticipantIndex >= 0 do {
					if oldParticipants.contains(participantId) then oldParticipantsWithSuccessfulAppendResult += 1
					if newParticipants.contains(participantId) then newParticipantsWithSuccessfulAppendResult += 1

					var goNext = true
					otherParticipantIndex -= 1
					while otherParticipantIndex >= 0 && goNext do {
						appendSummaries(otherParticipantIndex) match {
							case AO_SUCCESS =>
								participantId = allOtherParticipants(otherParticipantIndex)
								goNext = false
							case _ =>
								otherParticipantIndex -= 1
						}
					}
				}
				(oldParticipantsWithSuccessfulAppendResult > halfOfOldParticipants || oldParticipants.isEmpty)
					&& (newParticipantsWithSuccessfulAppendResult > halfOfNewParticipants || newParticipants.isEmpty)
			}

			override def determineRole(primaryState: Accessible, primaryStateFence: sequencer.CausalFence[PrimaryState], myVote: Vote[ParticipantId], votesFromOthers: Array[Try[Vote[ParticipantId]]]): Role = {
				var oldParticipantsVotesMatchingMyVote = 0
				var newParticipantsVotesMatchingMyVote = 0
				var newParticipantsJoining = 0
				var vote = myVote
				var participantId = boundParticipantId
				var otherParticipantIndex = votesFromOthers.length
				while otherParticipantIndex >= 0 do {
					if vote.votedId == myVote.votedId then {
						if oldParticipants.contains(participantId) then oldParticipantsVotesMatchingMyVote += 1
						if newParticipants.contains(participantId) then newParticipantsVotesMatchingMyVote += 1
					} else if vote.rank == ER_PRE_CANDIDATE && newParticipants.contains(vote.votedId) then newParticipantsJoining += 1
					var goNext = true
					otherParticipantIndex -= 1
					// navigate to the next successful vote and get it.
					while otherParticipantIndex >= 0 && goNext do {
						votesFromOthers(otherParticipantIndex) match {
							case s: Success[Vote[ParticipantId]] =>
								vote = s.value
								participantId = allOtherParticipants(otherParticipantIndex)
								goNext = false

							case _: Failure[Vote[ParticipantId]] =>
								otherParticipantIndex -= 1
						}
					}
				}

				if (oldParticipantsVotesMatchingMyVote <= halfOfOldParticipants && oldParticipants.nonEmpty) || (newParticipantsVotesMatchingMyVote + newParticipantsJoining <= halfOfNewParticipants && newParticipants.nonEmpty) then Isolated(primaryStateFence)
				else if myVote.votedId == boundParticipantId then Promoting(primaryState.currentTerm, primaryStateFence)
				else Follower(primaryState.currentTerm, myVote.votedId, primaryStateFence)
			}

			override def decideMyVote(myStateInfo: StateInfo, howAreYouAnswers: IArray[StateInfo | Null]): Vote[ParticipantId] = {
				var participantId = boundParticipantId
				var highestTermSeen = myStateInfo.currentTerm

				var oldParticipantsThatAreReachable = 0
				var newParticipantsThatAreReachable = 0
				var isInOldConfig = oldParticipants.contains(participantId)
				var isInNewConfig = newParticipants.contains(participantId)
				var chosenCandidate = CandidateInfo(participantId, isInOldConfig, myStateInfo)
				var borrame = List(chosenCandidate) //TODO delete line

				var participantIndex = allOtherParticipants.length
				while participantIndex >= 0 do {
					if isInOldConfig then oldParticipantsThatAreReachable += 1
					if isInNewConfig then newParticipantsThatAreReachable += 1

					var goNext = true
					participantIndex -= 1
					// navigate to the next successfully replied StateInfo and get it
					while participantIndex >= 0 && goNext do {
						participantId = allOtherParticipants(participantIndex)
						howAreYouAnswers(participantIndex) match {
							case null =>
								scribe.debug(s"$boundParticipantId: `$participantId.howAreYou` failed while deciding vote")
								participantIndex -= 1

							case stateInfo: StateInfo =>
								if stateInfo.currentTerm > highestTermSeen then highestTermSeen = stateInfo.currentTerm
								isInOldConfig = oldParticipants.contains(participantId)
								isInNewConfig = newParticipants.contains(participantId)
								val candidateInfo = CandidateInfo(participantId, isInOldConfig, stateInfo)
								chosenCandidate = chosenCandidate.getWinnerAgainst(candidateInfo)
								borrame = candidateInfo :: borrame //TODO delete line
								goNext = false
						}
					}
				}
				val result = Vote(highestTermSeen, chosenCandidate.id, oldParticipantsThatAreReachable, newParticipantsThatAreReachable, chosenCandidate.info.rank, currentBallot)
				scribe.debug(s"$boundParticipantId: decideMyVote($myStateInfo, ${howAreYouAnswers.mkString("[", ", ", "]")}): result=$result, among=$borrame, config=$backingConfigChange") //TODO delete line
				result
			}
		}

		private def Configuration_from(configChange: ConfigChange[ParticipantId]): Configuration = {
			configChange match {
				case cc: TransitionalConfigChange[ParticipantId] =>
					TransitionalConfig(cc)
				case cc: StableConfigChange[ParticipantId] =>
					StableConfig(cc)
			}
		}

		//// UTILITIES USED BY MANY BEHAVIORS

		/**
		 * Asks the [[Configuration.allOtherParticipants]] how they are ([[ClusterParticipant.howAreYou]]) in a coalesced manner: If an equivalent question is in flight, reuses the same pending [[sequencer.LatchedDuty]] of the in-flight question; otherwise, a new request is done.
		 * Supports the forcing of answers.
		 *
		 * @param participantsIds the [[ParticipantId]]s of the target participants.
		 * @param stateInfo the [[StateInfo]] to put in the inquires.
		 * @param forcedAnswerByParticipantId the forced answers indexed by [[ParticipantId]].
		 * @return An [[IndexedSeq]] containing a [[LatchedTask]] for each [[ParticipantId]] in the provided array. Each [[LatchedTask]] element is the one returned by [[ClusterParticipant.howAreYou]] applied to the corresponding [[ParticipantId]] in the provided array, except the corresponding to the provided `idOfExcludedParticipant`, which yield the provided [[StateInfo]].
		 */
		private def askHowOtherParticipantsAre(participantsIds: IArray[ParticipantId], stateInfo: StateInfo, forcedAnswerByParticipantId: java.util.Map[ParticipantId, StateInfo]): IArray[sequencer.LatchedTask[StateInfo]] = {
			participantsIds.mapWithIndex { (participantId, _) =>
				forcedAnswerByParticipantId.get(participantId) match {
					case null => coalescedHowAreYou.getOrStart((participantId, stateInfo), true)
					case forcedAnswer: StateInfo => sequencer.LatchedTask_ready(Success(forcedAnswer))
				}
			}
		}

		private final def illegalStateStop(): Role = {
			become(Stopped(Failure(new AssertionError("can not happen"))))
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
				else if this.info.rank == ER_LEADING && other.info.rank != ER_LEADING then this
				else if this.info.rank != ER_LEADING && other.info.rank == ER_LEADING then other
				else if this.info.rank == ER_CANDIDATE && other.info.rank != ER_CANDIDATE then this
				else if this.info.rank != ER_CANDIDATE && other.info.rank == ER_CANDIDATE then other
				else if this.info.termAtCommitIndex > other.info.termAtCommitIndex then this
				else if this.info.termAtCommitIndex < other.info.termAtCommitIndex then other
				else if this.info.commitIndex > other.info.commitIndex then this
				else if this.info.commitIndex < other.info.commitIndex then other
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
		private inline def notifyListeners(inline notificator: NotificationListener => Unit): Unit = {
			checkWithin()
			notificationListeners.forEach { (listener, _) =>
				try notificator(listener)
				catch {
					case NonFatal(e) => scribe.error(s"$boundParticipantId: the listener $listener threw an exception while handling $notificator", e)
				}
			}
		}

		//// Just for efficiency ////

		@threadUnsafe private lazy val _emptyLatchedDuty: sequencer.LatchedDuty[Maybe[AnyRef]] = sequencer.LatchedDuty_ready(Maybe.empty)

		/** An already completed [[sequencer.LatchedDuty]] that yields [[Maybe.empty]].
		 * CAUTION: This @threadUnsafe lazy val does not guarantee a unique instance under concurrent access. Its use is only safe for logic that depends on the value's data, not its object identity (eq/ne). */
		private final def emptyLatchedDuty[A]: sequencer.LatchedDuty[Maybe[A]] = _emptyLatchedDuty.asInstanceOf[sequencer.LatchedDuty[Maybe[A]]]
	}
}
