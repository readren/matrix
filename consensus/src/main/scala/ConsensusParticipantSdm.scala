package readren.consensus

import readren.common.{Maybe, castTo}
import readren.sequencer.{Doer, MilliDuration, SchedulingExtension}

import java.util
import java.util.Comparator
import scala.annotation.tailrec
import scala.collection.{mutable, IndexedSeq as GenIndexedSeq}
import scala.compiletime.asMatchable
import scala.math.Ordering.Implicits.infixOrderingOps
import scala.reflect.ClassTag
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

	final type ConfigChangeRequestId = String

	/** The ides of the concrete [[Role]] subtypes. */
	final type RoleOrdinal = Byte
	final val STOPPED: RoleOrdinal = 0
	final val STARTING: RoleOrdinal = 1
	final val ISOLATED: RoleOrdinal = 2
	final val CANDIDATE: RoleOrdinal = 3
	final val FOLLOWER: RoleOrdinal = 4
	final val LEADER: RoleOrdinal = 5

	final type ConfigChangeResponse = Byte
	/** The requested configuration change was successfully completed. Only participants with the [[LEADER]]] role answer this. */
	final val SUCCESSFULLY_CHANGED: ConfigChangeResponse = 0
	/** The participant has already changed to the requested configuration. Only participants with the [[LEADER]]] or [[FOLLOWER]] roles answer this. */
	final val ALREADY_CHANGED: ConfigChangeResponse = 1
	/** The participant is currently transitioning to the requested configuration. Only participants with the [[LEADER]]] or [[FOLLOWER]] roles answer this. */
	final val ALREADY_IN_PROGRESS: ConfigChangeResponse = 2
	/** The participant is the leader but is currently processing another change to a configuration different from the requested. Only participants with the [[LEADER]]] role answer this. */
	final val WAIT_PREVIOUS_CHANGE_TO_COMPLETE: ConfigChangeResponse = 3
	/** The participant is a follower whose target configuration differs from the requested. Only participants in the [[FOLLOWER]] role answer this. */
	final val ASK_THE_LEADER: ConfigChangeResponse = 4
	/** The participant was not able to become neither the [[LEADER]] nor a [[FOLLOWER]]. */
	final val UNABLE: ConfigChangeResponse = 5 

	/** Indicates the outcome of a client's previous attempt to send a command to a [[ConsensusParticipantSdm.ConsensusParticipant]].
	 * Used to annotate retry semantics and guide routing behavior. */
	type CommandAttemptFlag = Byte

	/** The client has not previously attempted to send the command to any participant. */
	final val FIRST_ATTEMPT: CommandAttemptFlag = 0

	/** The client previously sent the command to a different participant, which responded with a redirect instruction. */
	final val REDIRECTED: CommandAttemptFlag = 1

	/** The client previously sent the command to a different participant, which either failed or rejected the request. */
	final val FALLBACK: CommandAttemptFlag = 2

	//// STANDALONE DATA TYPES

	/**
	 * A vote for a leader.
	 * @see [[ConsensusParticipantSdm.ClusterParticipant.chooseALeader]] and [[ConsensusParticipantSdm.ClusterParticipant.Delegate.onChooseALeader]].
	 * @tparam Id The identifier type for participants in the consensus cluster.
	 *            This must match the concrete type used to implement [[ConsensusParticipantSdm.ParticipantId]].
	 *            It allows the user to customize how participants are identified (e.g., UUID, String, custom class), while preserving type safety across [[Vote]] instances exchanges.
	 *            Although [[Vote]] is designed to travel between participants, it remains path-dependent and must be instantiated within a module that resolves [[ParticipantId]] to a concrete type.
	 * @param term The term for which the vote is cast.
	 * @param candidateId The id of the voted candidate.
	 * @param reachableCandidatesOfOldConf The number of candidates of the current or old set of participants that were reachable by the voter (including the voter itself) when the vote was cast. Zero means the voter is still initializing or was stopped.
	 * @param reachableCandidatesOfNewConf The number of candidates new the new set or participants that were reachable by the voter (including the voter itself) when the vote was cast. Zero means the voter is still initializing or was stopped.
	 * @param roleOrdinal The role of the voted candidate.
	 */
	final case class Vote[Id <: AnyRef](term: Term, candidateId: Id, reachableCandidatesOfOldConf: Int, reachableCandidatesOfNewConf: Int, roleOrdinal: RoleOrdinal)

	/** The result of an append operation.
	 * @see [[ConsensusParticipantSdm.ClusterParticipant.appendRecords]] and [[ConsensusParticipantSdm.ClusterParticipant.Delegate.onAppendRecords]].
	 * @param term The term of the follower that is responding.
	 * @param success False if either:
	 * - the participant that is responding is not ready (Starting or Stopped);
	 * - the leader's term is less than the current term of the follower;
	 * - or the follower's log does not contain a record at the `prevRecordIndex` whose term is `prevRecordTerm`.
	 * @param roleOrdinal The role of the follower that is responding.
	 */
	final case class AppendResult(term: Term, success: Boolean, roleOrdinal: RoleOrdinal)

	/**
	 * Knows all the information about a candidate necessary by participants to decide their own role and which to vote in a leader election.
	 * The response to a "how are you" question from a participant to another.
	 * @param currentTerm The term of the participant that is answering.
	 * @param ordinal The role of the participant that is answering.
	 * @param termAtCommitIndex The term of the last commited record in the log of the participant that is answering.
	 * @param commitIndex The index of the last commited record in the log of the participant that is answering.
	 */
	final case class StateInfo(currentTerm: Term, ordinal: RoleOrdinal, termAtCommitIndex: Term, commitIndex: RecordIndex) {
		assert(currentTerm >= termAtCommitIndex)
	}

	//// LOG RECORD

	sealed trait Record {
		def term: Term
	}

	private[consensus] final case class CommandRecord[+C <: AnyRef](override val term: Term, command: C) extends Record

	private[consensus] final case class LeaderTransition(override val term: Term) extends Record

	private[consensus] final case class SnapshotPoint(override val term: Term) extends Record

	sealed trait ConfigChange extends Record

	private[consensus] final case class TransitionalConfigChange[P <: AnyRef](override val term: Term, requestId: ConfigChangeRequestId, oldParticipants: Set[P], newParticipants: Set[P]) extends ConfigChange

	private[consensus] final case class StableConfigChange[P <: AnyRef](override val term: Term, requestId: ConfigChangeRequestId, previousChangeTerm: Term, oldParticipants: Set[P], newParticipants: Set[P]) extends ConfigChange

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
	 *   - For any two commands `a` and `b` from the same client, if `a` was issued *after* `b`, then `clientCommandOrdering.compare(a, b) > 0`.
	 *   - If `a` and `b` are semantically identical (e.g., same request ID), then `clientCommandOrdering.compare(a, b) == 0`.
	 *   - If `a` was issued *before* `b`, then `clientCommandOrdering.compare(a, b) < 0`.
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

	def isEager: Boolean = false

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
	 * @param otherParticipants The identifiers of alternative participants to try. This list is provided as a best effort and may be incomplete or include servers that are stale or unavailable.
	 */
	final case class Unable(roleOrdinal: RoleOrdinal, otherParticipants: Set[ParticipantId]) extends ResponseToClient

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

		/** The implementation should return  the identifiers of the participants in the initial cluster configuration.
		 * This method is called by the bounded [[ConsensusParticipant]] when started for the first time ([[Workspace.isBrandNew]] returns true); and must return exactly the same set across all participants listed.
		 * The returned set must include the identifier of the participant serviced by this [[ClusterParticipant]] instance.
		 */
		def getInitialParticipants: Set[ParticipantId]

		/** The implementation should return the identifiers of the participants that this [[ClusterParticipant]] optimistically suspect are members of the consensus set, excluding the bound one.
		 * This method is called when the [[ConsensusParticipant]] that is [[STARTING]] or [[STOPPED]] has to respond [[Unable]] to a client. */
		def getOtherProbableParticipant: Set[ParticipantId]

		/** Called by the bounded [[ConsensusParticipant]] to notify that its cluster-configuration has changed and now expects connectivity with the given set/sets of participants.
		 *
		 * This method is invoked upon application of a [[ConfigChange]]-typed [[Record]].
		 * Given configuration changes is a two-phase process, a call to [[Delegate.requestConfigChange]] causes two [[ConfigChange]] records to be applied and, therefore, two calls tho this method. This happens in all the involved [[ConsensusParticipant]] services.
		 * Successive calls with the same argument may occur. Implementations may ignore such calls only if no intervening call with a different argument has occurred — i.e., if the configuration has not changed.
		 * @param change The [[ConfigChange]] of participants that the consensus layer expects to be reachable.
		 */
		def onConfigurationChanged(change: ConfigChange): Unit


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
			 * @param inquirerTerm The term of the participant that called [[howAreYou]].
			 * @return The state information of the destination participant.
			 */
			def onHowAreYou(inquirerId: ParticipantId, inquirerTerm: Term): sequencer.LatchedDuty[StateInfo]

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
			 *			- or the [[onConfigurationChanged]] is called in any of the consensus-participants with the provided request identifier or desired participants set.
			 *
			 * @param requestId an identifier chosen by the caller that will be propagated up to the invocations of the [[onConfigurationChanged]] method of each of the [[ClusterParticipant]] instances bounded to the involved [[ConsensusParticipant]] services.  	
			 * @param desiredParticipantsSet the identifiers of the participants that are going to seek consensus from now on.
			 * @return a [[sequencer.Duty]] that yields:
			 *         [[SUCCESSFULLY_CHANGED]] if the requested change was successfully completed.
			 *         [[ALREADY_CHANGED]] if the requested change is already done or in progress.
			 *         [[ALREADY_IN_PROGRESS]] if the participant is currently transitioning to the requested configuration 
			 *         [[ASK_THE_LEADER]] if none of the the previous bullet is true and the [[ConsensusParticipant]] is a [[FOLLOWER]].
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

		extension (destinationId: ParticipantId) {
			/**
			 * Asks the destination participant how it is doing.
			 * The implementation should make, somehow, the destination participant's [[Delegate.onHowAreYou]] to be called, and return what it returns.
			 * Called within the [[sequencer]] thread.
			 * @param inquirerTerm The term of the participant that is asking.
			 * @return A [[sequencer.Task]] that yields the state information of the destination participant.
			 */
			def howAreYou(inquirerTerm: Term): sequencer.Task[StateInfo]

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
			 *
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

		def isBrandNew: Boolean

		//		/** Record index of the first record in the log buffer. */
		//		private var logBufferOffset: RecordIndex = 1
		//
		//		/** The log buffer.
		//		 * Contains the records since the last snapshot. */
		//		private val logBuffer: mutable.ArrayBuffer[Record] = mutable.ArrayBuffer.empty

		def indexOfTopConfigChange: RecordIndex

		def topConfigChange: ConfigChange
		
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
		 * // TODO: consider returning an iterator instead of an iterable.
		 */
		def getRecordsBetween(from: RecordIndex, until: RecordIndex): GenIndexedSeq[Record] // = logBuffer.slice((from - logBufferOffset).toInt, (until - logBufferOffset).toInt)

		/** Appends a record and returns it index. */
		def appendRecord(record: Record): Unit

		/** Appends any new record not already in the log starting at the specified index.
		 * If a conflict is detected (i.e., a stored record and a new record at the same index have different terms), all stored records from that index onward are removed before appending the new records.
		 * */
		def appendResolvingConflicts(records: GenIndexedSeq[Record], from: RecordIndex): Unit

		/** Should be called whenever the [[ConsensusParticipant.lastAppliedCommandIndex]] changes to allow this [[Workspace]] to release the storage used to memorize the records that are pending to be applied to the [[StateMachine]]. */
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

		/** Called by the [[ConsensusParticipant]] to inform that it will not reference this instance anymore and this [[Workspace]] may be purged. */
		def release(): Unit
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
		def onStarting(previous: RoleOrdinal, isRestart: Boolean): Unit

		def onStarted(previous: RoleOrdinal, term: Term, initialConfigChange: ConfigChange, isRestart: Boolean): Unit

		def onBecameStopped(previous: RoleOrdinal, term: Term, motive: Try[String]): Unit

		def onBecameIsolated(previous: RoleOrdinal, term: Term): Unit

		def onBecameCandidate(previous: RoleOrdinal, term: Term): Unit

		def onBecameFollower(previous: RoleOrdinal, term: Term, leaderId: ParticipantId): Unit

		def onBecameLeader(previous: RoleOrdinal, term: Term): Unit

		def onLeft(left: RoleOrdinal, term: Term): Unit

		def onCommitIndexChange(previous: RecordIndex, current: RecordIndex): Unit

		def onConfigurationChangeApplied(currentRole: RoleOrdinal, currentTerm: Term, cc: ConfigChange): Unit
	}

	/**
	 * A convenience [[NotificationListener]] implementation with no-op methods.
	 * Extend this class and override only the methods you need.
	 */
	open class DefaultNotificationListener extends NotificationListener {
		override def onStarting(previous: RoleOrdinal, isRestart: Boolean): Unit = ()

		override def onStarted(previous: RoleOrdinal, term: Term, initialConfigChange: ConfigChange, isRestart: Boolean): Unit = ()

		override def onBecameStopped(previous: RoleOrdinal, term: Term, motive: Try[String]): Unit = ()

		override def onBecameIsolated(previous: RoleOrdinal, term: Term): Unit = ()

		override def onBecameCandidate(previous: RoleOrdinal, term: Term): Unit = ()

		override def onBecameFollower(previous: RoleOrdinal, term: Term, leaderId: ParticipantId): Unit = ()

		override def onBecameLeader(previous: RoleOrdinal, term: Term): Unit = ()

		override def onLeft(left: RoleOrdinal, term: Term): Unit = ()

		override def onCommitIndexChange(previous: RecordIndex, current: RecordIndex): Unit = ()

		override def onConfigurationChangeApplied(currentRole: RoleOrdinal, currentTerm: Term, cc: ConfigChange): Unit = ()
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
	 * - Leader selection criteria (in order of priority): highest current term, is leader or not, highest last record term, longest log, and lexicographically the smallest participant ID.
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
	class ConsensusParticipant(cluster: ClusterParticipant, storage: Storage, machine: StateMachine, initialListeners: Iterable[NotificationListener]) { thisConsensusParticipant =>

		import cluster.*

		/** The index of the highest entry known to be committed according to this participant.
		 * A log record is committed once the leader that created the record has replicated it on a majority of the participants.
		 * This also commits all preceding records in the leader’s log, including records created by previous leaders. */
		private var commitIndex: RecordIndex = 0

		/** The index of the highest command entry whose command was successfully applied to the [[StateMachine]] of this [[ConsensusParticipant]]. */
		private var lastAppliedCommandIndex: RecordIndex = 0

		/** The current role of this [[ConsensusParticipant]]. */
		private var currentRole: Role = Starting(false)

		/** The current participation related [[Configuration]] of this [[ConsensusParticipant]].
		 * Do not access it directly. Use [[getCurrentConfigUpdated]] instead. */
		private var _currentConfig: Configuration = NoConfig

		/** A reference to a [[sequencer.Covenant]] that is created during the [[Starting]] role, which is completed when the leaving the startup process completes.
		 * In a restart, the old instance is wired to complete if the new one does.
		 * // TODO I don't like this reference being a var. It adds race conditions in situations that occur rarely. Analyze making it a val which would stop supporting restarts, or remove this variable at all. */
		//		private var startingCompletedCovenant: sequencer.Covenant[Boolean] = sequencer.Covenant_ready(false)

		/** Memorices the schedule that is currently scheduled. Needed to cancel the schedule when transitioning to a non-equivalent [[Role]] instance. */
		private var currentSchedule: Maybe[sequencer.Schedule] = Maybe.empty

		private var decoupledCommandsApplierIsRunning: Boolean = false

		private val notificationListeners: java.util.WeakHashMap[NotificationListener, None.type] = new util.WeakHashMap()

		/** //TODO clear this map when the [[currentTerm]] is updated */
		private val howAreYouRequestOnTheWayByParticipant: mutable.Map[ParticipantId, sequencer.Commitment[StateInfo]] = mutable.Map.empty

		{
			initialListeners.foreach(notificationListeners.put(_, None))
			cluster.setBound(currentRole)
			currentRole.onEnter(currentRole)
		}

		/** @note Accessing the current [[Configuration]] through this method ensures that the current [[Configuration]] is updated before any other derived-state update that depend on it. 
		 * @return the current [[Configuration]] after updating it based on the provided [[PrimaryState]] if it was stale. */
		private def getCurrentConfigUpdated(primaryState: PrimaryState): Configuration = {
			primaryState match {
				case accessible: Accessible =>
					val desiredConfigChange = accessible.topConfigChange match {
						case stable: StableConfigChange[ParticipantId] @unchecked =>
							if commitIndex >= accessible.indexOfTopConfigChange then stable
							else _currentConfig.correspondingConfigChange

						case transitional: TransitionalConfigChange[ParticipantId] @unchecked =>
							transitional
					}
					if desiredConfigChange != _currentConfig.correspondingConfigChange then {
						_currentConfig = Configuration_from(desiredConfigChange)
						// Inform the cluster service and notify the listeners about the configuration change.
						cluster.onConfigurationChanged(desiredConfigChange)
						notifyListeners(_.onConfigurationChangeApplied(currentRole.ordinal, accessible.currentTerm, desiredConfigChange))
					}

				case Inaccessible =>
					_currentConfig = NoConfig
			}
			_currentConfig
		}


		private object participantIdComparator extends Comparator[ParticipantId] {
			private val ordering = summon[Ordering[ParticipantId]]

			override def compare(a: ParticipantId, b: ParticipantId): Int = ordering.compare(a, b)
		}

		/** @return the ordinal of the current behavior. */
		inline def getRoleOrdinal: RoleOrdinal = currentRole.ordinal

		/** Stops this [[ConsensusParticipant]] instance.
		 * Should be called within the [[sequencer]]. */
		def stop(): Unit = {
			assert(isInSequence)
			if currentRole.ordinal != STOPPED then {
				become(Stopped(Success("This ConsensusParticipant instance was externally stopped.")))
			} 
		}

		/**
		 * Transitions the participant to a new behavior.
		 */
		private def become(newRole: Role): Role = {
			assert(isInSequence)
			if !newRole.isEquivalentTo(currentRole) then {
				currentRole.onLeave()
				currentSchedule.foreach(sequencer.cancel(_))
				currentSchedule = Maybe.empty
				val previousRole = currentRole
				val committedTerm = currentRole.getCommittedPrimaryState.currentTerm
				notifyListeners(_.onLeft(previousRole.ordinal, committedTerm))
				currentRole = newRole
				cluster.setBound(newRole)
				newRole.onEnter(previousRole)
			}
			currentRole
		}

		extension [T](task: sequencer.Task[T]) {
			private def triggerExposingFailures(): Unit = {
				task.trigger(true) {
					case Success(_) => // do nothing
					case Failure(e) => scribe.error(s"$boundParticipantId: Unexpected error:", e)
				}
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

			def isEquivalentTo(other: Role): Boolean

			def onEnter(previous: Role): Unit

			/** Called by [[become]] before transitioning to another role. */
			def onLeave(): Unit = ()

			def getCommittedPrimaryState: PrimaryState = Inaccessible

			def buildMyStateInfo(primaryState: PrimaryState): StateInfo =
				StateInfo(0, currentRole.ordinal, 0, 0)

			def decideMyVote(primaryState: PrimaryState, inquirerId: ParticipantId | Null, inquirerInfo: StateInfo): sequencer.LatchedDuty[Vote[ParticipantId]] =
				NoConfig.decideMyVote(primaryState, inquirerId, inquirerInfo)

			/** Updates the state and [[Role]] of this [[ConsensusParticipant]] and then returns the [[sequencer.Duty]] returned by the [[Role.onCommandFromClient]] method applied to the updated [[Role]].
			 * @return a [[sequencer.Duty]] returned by [[Role.onCommandFromClient]] applied to the updated [[Role]] */
			def updatesRoleAndThenCallsOnCommandFromClient(command: ClientCommand): sequencer.LatchedDuty[ResponseToClient] = {
				sequencer.LatchedDuty_ready(Unable(ordinal, cluster.getOtherProbableParticipant))
			}

			override def onHowAreYou(inquirerId: ParticipantId, inquirerTerm: Term): sequencer.LatchedDuty[StateInfo] = {
				sequencer.LatchedDuty_ready(StateInfo(0, ordinal, 0, 0))
			}

			override def onChooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.LatchedDuty[Vote[ParticipantId]] = {
				assert(isInSequence)
				sequencer.LatchedDuty_ready(Vote(0, boundParticipantId, 0, 0, ordinal))
			}

			override def onAppendRecords(inquirerId: ParticipantId, inquirerTerm: Term, prevRecordIndex: RecordIndex, prevRecordTerm: Term, records: GenIndexedSeq[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.LatchedDuty[AppendResult] = {
				assert(isInSequence)
				sequencer.LatchedDuty_ready(AppendResult(0, false, ordinal))
			}
		}

		/** Partial implementation of [[Role]]s that have state.
		 * All concrete subclasses of [[Role]] except [[STARTING]] and [[STOPPED]] extend this abstract class.
		 * @param primaryStateFence the [[sequencer.CausalFence]] that must be used to ensure causal ordering of the state updates. It must be propagated to subsequent [[StatefulRole]] instances. */
		private abstract class StatefulRole(val primaryStateFence: sequencer.CausalFence[PrimaryState]) extends Role {

			override def isEquivalentTo(other: Role): Boolean =
				other.ordinal == this.ordinal && (other.asInstanceOf[StatefulRole].primaryStateFence eq this.primaryStateFence)

			override final def getCommittedPrimaryState: PrimaryState =
				primaryStateFence.committedUnsafe

			override final def buildMyStateInfo(primaryState: PrimaryState): StateInfo = {
				primaryState match {
					case Inaccessible => StateInfo(0, ordinal, 0, 0)
					case accessible: Accessible => StateInfo(accessible.currentTerm, ordinal, accessible.getRecordTermAt(commitIndex), commitIndex)
				}
			}

			override final def decideMyVote(primaryState: PrimaryState, inquirerId: ParticipantId | Null, inquirerInfo: StateInfo): sequencer.LatchedDuty[Vote[ParticipantId]] =
				getCurrentConfigUpdated(primaryState).decideMyVote(primaryState, inquirerId, inquirerInfo)

			override final def onHowAreYou(inquirerId: ParticipantId, inquirerTerm: Term): sequencer.LatchedDuty[StateInfo] = {
				assert(isInSequence)
				for {
					primaryState0 <- primaryStateFence.causalAnchor(true)
					primaryState1 <- primaryStateFence.advanceIf(updateTermIfLessThan(inquirerTerm), true)
				} yield {
					if primaryState1.currentTerm > primaryState0.currentTerm then {
						// Enqueue the execution of the role-update-process. Note that it will be executed later and therefore will not affect the returned [[StateInfo]] (which may be outdated).
						updateRole().triggerAndForget(true)
					}
					currentRole.buildMyStateInfo(primaryState1)
				}
			}

			override final def onChooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.LatchedDuty[Vote[ParticipantId]] = {
				assert(isInSequence)

				for {
					primaryState0 <- primaryStateFence.causalAnchor(true)
					// if the term is stale, update it persistently before interacting with other participants so that they see this participant with its updated and persisted state. 
					primaryState1 <- primaryStateFence.advanceIf(updateTermIfLessThan(inquirerInfo.currentTerm), true)
					myVote <- currentRole.decideMyVote(primaryState1, inquirerId, inquirerInfo)
					primaryState2 <- primaryStateFence.advanceIf(updateTermIfLessThan(myVote.term), true) // this line is necessary so that participants asked for their Vote don't see this participant with a term that hasn't been already persisted.
				} yield {
					if primaryState2.currentTerm > primaryState0.currentTerm || (currentRole.ordinal >= CANDIDATE && !getCurrentConfigUpdated(primaryState2).reachedAMajority(myVote)) then {
						// Enqueue the execution of the role-update-process. Note that it will be executed later and therefore will not affect the returned [[StateInfo]] (which may be outdated).
						updateRoleKnowingMyVote(myVote).triggerAndForget(true)
					}

					myVote
				}
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
			 *         		- the role is either ISOLATED, CANDIDATE, or FOLLOWER;
			 *         		- the term of the log record at `prevRecordIndex` is equal to `prevRecordTerm`;
			 *         - `term = max(inquirerTerm, currentTerm)` when the appending was processed (causal fence crossed).
			 *         - `roleOrdinal` tells which was the role of this participant when the appending was processed (causal fence crossed).
			 */
			override final def onAppendRecords(inquirerId: ParticipantId, inquirerTerm: Term, prevRecordIndex: RecordIndex, prevRecordTerm: Term, records: GenIndexedSeq[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.LatchedDuty[AppendResult] = {
				assert(isInSequence)
				var appendSuccess = false
				var termUpdate = false
				for primaryState1 <- primaryStateFence.advanceIf(
					(primaryState0, _) => primaryState0 match {
						case Inaccessible =>
							Maybe.empty

						case accessible: Accessible =>
							val currentTerm = accessible.currentTerm
							if inquirerTerm < currentTerm then Maybe.empty
							else {
								termUpdate = inquirerTerm > currentTerm
								appendSuccess =
									ISOLATED <= currentRole.ordinal && currentRole.ordinal <= FOLLOWER
										&& prevRecordIndex < accessible.firstEmptyRecordIndex
										&& prevRecordTerm == accessible.getRecordTermAt(prevRecordIndex)
								if appendSuccess && records.nonEmpty then Maybe.some(accessible.withRecordsAppended(inquirerTerm, records, prevRecordIndex + 1))
								else if termUpdate then Maybe.some(accessible.withTermUpdated(inquirerTerm))
								else Maybe.empty
							}
					},
					true
				)
				yield {
					val response = AppendResult(primaryState1.currentTerm, appendSuccess, currentRole.ordinal)
					// At this point the log is already updated and the response already determined. All the following actions are causally anchored side effects.

					primaryState1 match {
						case accessible: Accessible =>
							assert(inquirerTerm <= accessible.currentTerm, s"$inquirerTerm == ${accessible.currentTerm}")

							// Update the commitIndex notifying if it changed.
							val previousCommitIndex = commitIndex
							commitIndex = if leaderCommit < accessible.firstEmptyRecordIndex then leaderCommit else accessible.firstEmptyRecordIndex - 1
							if commitIndex != previousCommitIndex then {
								if !decoupledCommandsApplierIsRunning then startApplyingCommitedCommands(accessible)
								notifyListeners(_.onCommitIndexChange(previousCommitIndex, commitIndex))
							}

						case Inaccessible => // do nothing
					}

					// If necessary, update the role in a decoupled manner.
					if currentRole.ordinal >= ISOLATED && (termUpdate || currentRole.ordinal < FOLLOWER) then {
						val inquirerInfo = StateInfo(inquirerTerm, LEADER, termAtLeaderCommit, leaderCommit)
						// Enqueue the execution of the role-update-process. Note that it will be executed later and therefore will not affect the returned [[AppendResult]].
						updateRole(inquirerId, inquirerInfo).triggerAndForget(true) // TODO: analyze what happens if this updateState causes to become the leader, preferably with a test.
					}

					response
				}
			}

			/** Applies commited [[CommandRecord]]s silently and in a decoupled manner. */
			private def startApplyingCommitedCommands(initialPrimaryState: Accessible): Unit = {
				decoupledCommandsApplierIsRunning = true

				def applyCommitedCommandsLoop(primaryState0: Accessible): sequencer.LatchedDuty[Unit] = {
					if lastAppliedCommandIndex >= commitIndex then sequencer.LatchedDuty_unit
					else {
						val indexOfCommandToApply = lastAppliedCommandIndex + 1
						primaryState0.getRecordAt(indexOfCommandToApply) match {
							case command: CommandRecord[ClientCommand] @unchecked =>
								for {
									_ <- machine.applyClientCommand(indexOfCommandToApply, command.command)
									primaryState1 <- {
										lastAppliedCommandIndex = indexOfCommandToApply

										// Even in this decoupled applier, we must use [[CausalFence.causalAnchor]].
										// If instead, [[commited]] was used, it would only fence commited-state visibility, leaving races between this derived state update and other state updates (RPC handlers, role transitions, snapshotting).
										// [[causalAnchor]] ensures that the mutation is sequenced in the same causal chain as other consensus-state updates — provided those updates mutate primary-state within an [[advance]] section or derived-state within a causally anchored consumers.
										// This preserves determinism and prevents observers from seeing partially applied state.
										primaryStateFence.causalAnchor(true)
									}
									_ <- {
										primaryState1 match {
											case accessible: Accessible => applyCommitedCommandsLoop(accessible)
											case Inaccessible => sequencer.LatchedDuty_unit
										}
									}
								} yield ()
							case _ =>
								lastAppliedCommandIndex = indexOfCommandToApply
								applyCommitedCommandsLoop(primaryState0)
						}
					}
				}

				val applyCommitedCommands =
					if lastAppliedCommandIndex > 0 then applyCommitedCommandsLoop(initialPrimaryState)
					else {
						for {
							index <- machine.recoverIndexOfLastAppliedCommand
							_ <- {
								lastAppliedCommandIndex = index
								applyCommitedCommandsLoop(initialPrimaryState)
							}
						} yield ()
					}
				applyCommitedCommands.andThen(_ => decoupledCommandsApplierIsRunning = false)
			}

			/** Handles configuration change request for the [[Isolated]], [[Candidate]], and [[Follower]] roles. */
			override def requestConfigChange(requestId: ConfigChangeRequestId, desiredParticipants: Set[ParticipantId]): sequencer.LatchedDuty[ConfigChangeResponse] = {
				for {
					_ <- {
						if currentRole.ordinal < FOLLOWER then updateRole()
						else sequencer.LatchedDuty_unit
					}
					primaryState <- primaryStateFence.causalAnchor(true)
					response <- {
						if currentRole.ordinal == LEADER then currentRole.requestConfigChange(requestId, desiredParticipants)
						else if currentRole.ordinal == FOLLOWER then {
							val currentConfig = getCurrentConfigUpdated(primaryState)
							val response =
								if desiredParticipants != currentConfig.desiredParticipants then ASK_THE_LEADER
								else if currentConfig.isInstanceOf[StableConfig] then ALREADY_CHANGED
								else UNABLE
							sequencer.LatchedDuty_ready(response)
						} else sequencer.LatchedDuty_ready(UNABLE)
					}
				} yield response
			}


			//// Role updaters

			/** Updates the [[currentRole]] and [[PrimaryState.currentTerm]] based on the [[StateInfo]]s returned by calling [[ClusterParticipant.howAreYou]] on the other participants and, if necessary, also the [[Vote]]s returned by calling [[ClusterParticipant.chooseALeader]] on them. */
			inline def updateRole(): sequencer.LatchedDuty[Unit] =
				updateRole(null, null)


			/** Like [[updateRole]] but already knowing the [[StateInfo]] of a single other participant, which saves a call to [[ClusterParticipant.howAreYou]]. */
			def updateRole(inquirerId: ParticipantId | Null, inquirerInfo: StateInfo): sequencer.LatchedDuty[Unit] = {
				assert(sequencer.isInSequence)
				// scribe.trace(s"$boundParticipantId: updateRole($inquirerId, $inquirerInfo) was called") // TODO delete line
				for {
					primaryState <- {
						if inquirerId eq null then primaryStateFence.causalAnchor(true)
						else primaryStateFence.advanceIf(updateTermIfLessThan(inquirerInfo.currentTerm), true)
					}
					myVote <- {
						// scribe.trace(s"$boundParticipantId: updateRole($inquirerId, $inquirerInfo) - primaryState=$primaryState") // TODO delete line
						currentRole.decideMyVote(primaryState, inquirerId, inquirerInfo)
					}
					_ <- {
						// scribe.trace(s"$boundParticipantId: updateRole($inquirerId, $inquirerInfo) - myVote=$myVote") // TODO delete line
						updateRoleKnowingMyVote(myVote)
					}
				} yield ()
			}

			/** Like [[updateRole]] but already having the result of [[currentRole.decideMyVote]], which saves the all the calls to [[ClusterParticipant.howAreYou]]. */
			def updateRoleKnowingMyVote(myVote: Vote[ParticipantId]): sequencer.LatchedDuty[Unit] = {
				assert(isInSequence)
				// scribe.trace(s"$boundParticipantId: updateRoleKnowingMyVote($myVote) was called") // TODO delete line
				for {
					primaryState1 <- primaryStateFence.advanceIf(updateTermIfLessThan(myVote.term), true)
					_ <- {
						// scribe.trace(s"$boundParticipantId: updateRoleKnowingMyVote($myVote) - primaryState1=$primaryState1") // TODO delete line
						if currentRole ne this then sequencer.LatchedDuty_unit
						else primaryState1 match {
							case accessible1: Accessible =>
								val config1 = getCurrentConfigUpdated(accessible1)
								if config1.reachedAll(myVote) then {
									if myVote.candidateId == boundParticipantId then become(Leader(accessible1, primaryStateFence))
									else become(Follower(myVote.candidateId, primaryStateFence))
									sequencer.LatchedDuty_unit
								} else if config1.reachedAMajority(myVote) then {
									val myStateInfo = buildMyStateInfo(accessible1)
									// scribe.trace(s"$boundParticipantId: updateRoleKnowingMyVote($myVote) - myStateInfo=$myStateInfo") // TODO delete line
									val ongoingInquires = for replierId <- config1.allOtherParticipants yield replierId.chooseALeader(boundParticipantId, myStateInfo)
									for {
										replies <- sequencer.LatchedDuty_sequenceTasksToArray(ongoingInquires)
										primaryState2 <- {
											// scribe.trace(s"$boundParticipantId: updateRoleKnowingMyVote($myVote) - replies=${replies.mkString("[", ", ", "]")}") // TODO delete line
											var highestTermSeen = accessible1.currentTerm
											for case Success(replierVote) <- replies do if replierVote.term > highestTermSeen then highestTermSeen = replierVote.term
											primaryStateFence.advanceIf(updateTermIfLessThan(highestTermSeen))
										}
									} yield {
										if currentRole eq this then primaryState2 match {
											case accessible2: Accessible =>
												val newRole =
													if myVote.term < accessible2.currentTerm then Candidate(primaryStateFence)
													else getCurrentConfigUpdated(primaryState2).determineRole(accessible2, primaryStateFence, myVote, replies)
												become(newRole)
											case Inaccessible =>
												become(Stopped(Failure(new AssertionError("Can not happen"))))
										}
									}
								} else {
									become(Isolated(primaryStateFence))
									sequencer.LatchedDuty_unit
								}
							case Inaccessible =>
								assert(currentRole.ordinal == STOPPED, s"Unexpected behavior: $currentRole")
								sequencer.LatchedDuty_unit
						}
					}
				} yield ()
			}

			/** Updates the state and [[Role]] of this [[ConsensusParticipant]] and then returns the [[sequencer.LatchedDuty]] returned by the [[Role.onCommandFromClient]] method applied to the updated [[Role]].
			 * @return a [[sequencer.Task]] returned by [[Role.onCommandFromClient]] applied to the updated [[Role]] */
			override final def updatesRoleAndThenCallsOnCommandFromClient(command: ClientCommand): sequencer.LatchedDuty[ResponseToClient] = {
				assert(isInSequence)
				for {
					_ <- updateRole()
					primaryState <- primaryStateFence.causalAnchor(true)
					result <- {
						val currentRoleOrdinal = currentRole.ordinal
						if currentRoleOrdinal >= FOLLOWER then currentRole.onCommandFromClient(command, FIRST_ATTEMPT)
						else sequencer.LatchedDuty_ready(Unable(currentRoleOrdinal, getCurrentConfigUpdated(primaryState).otherProbableParticipants))
					}
				} yield result
			}


		}

		/** The behavior when the participant has the [[STOPPED]] role. Taken when the service is stopped or after a failure. */
		private final class Stopped(val motive: Try[String]) extends Role {
			override val ordinal: RoleOrdinal = STOPPED
			/** The committed term when this instance was created. */
			private val committedTermWhenStopped = currentRole.getCommittedPrimaryState.currentTerm

			override def isEquivalentTo(other: Role): Boolean =
				other.ordinal == STOPPED

			override def onEnter(previous: Role): Unit = {
				notifyListeners(_.onBecameStopped(previous.ordinal, committedTermWhenStopped, motive))
				cluster.onStopped(motive)
				previous match {
					case ir: StatefulRole => ir.primaryStateFence.committedUnsafe match {
						case accessible: Accessible => accessible.release()
						case _ => ()
					}
					case _ => ()
				}
			}

			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.LatchedDuty[ResponseToClient] = {
				assert(isInSequence)
				sequencer.LatchedDuty_ready(Unable(ordinal, cluster.getOtherProbableParticipant))
			}

			override def requestConfigChange(requestId: ConfigChangeRequestId, desiredParticipantsSet: Set[ParticipantId]): sequencer.LatchedDuty[ConfigChangeResponse] =
				sequencer.LatchedDuty_ready(UNABLE)

		}

		/** The behavior when the participant has the [[STARTING]] role. This is a transitory role during which the participant state is initialized.
		 * When initialization is completed it transitions to the [[Isolated]] state. */
		private final class Starting(val isRestart: Boolean) extends Role {
			override val ordinal: RoleOrdinal = STARTING
			private val startingCompletedCovenant: sequencer.Covenant[Unit] = sequencer.Covenant()

			override def isEquivalentTo(other: Role): Boolean =
				other.ordinal == STARTING && other.asInstanceOf[Starting].isRestart == this.isRestart

			override def onEnter(previous: Role): Unit = {

				notifyListeners(_.onStarting(previous.ordinal, isRestart))
				storage.load.subscribe {
					case Success(loadedWorkspace) =>


						val governingConfigChange = {
							if loadedWorkspace.isBrandNew then {
								loadedWorkspace.setCurrentTerm(0)
								val initialConfigChange = new StableConfigChange[ParticipantId](0, "Initial-Config", 0, cluster.getInitialParticipants, cluster.getInitialParticipants)
								loadedWorkspace.appendRecord(initialConfigChange)
								initialConfigChange

							} else loadedWorkspace.topConfigChange match {
								// If the top configuration change in the log is a stable one, then the previous transitional configuration change governs until the commitIndex crosses the index of top stable one.
								case stableConfigChange: StableConfigChange[ParticipantId] @unchecked =>
									// Recreate the previous configuration change with the information about it stored in the top stable one.
									TransitionalConfigChange(stableConfigChange.previousChangeTerm, stableConfigChange.requestId, stableConfigChange.oldParticipants, stableConfigChange.newParticipants)

								// If the top configuration change in the log is a transitional one, then it governs immediately.
								case transitionalConfigChange: TransitionalConfigChange[ParticipantId] @unchecked =>
									transitionalConfigChange
							}
						}
						val config = Configuration_from(governingConfigChange)
						if config.allParticipants.contains(boundParticipantId) then {
							_currentConfig = config
							val primaryState = Accessible(loadedWorkspace)
							val primaryStateFence = sequencer.CausalFence[PrimaryState](primaryState)

							val previousCommitIndex = commitIndex
							commitIndex = 0
							if previousCommitIndex != 0 then notifyListeners(_.onCommitIndexChange(previousCommitIndex, 0))

							lastAppliedCommandIndex = 0
							loadedWorkspace.informAppliedCommandIndex(0)

							notifyListeners(_.onStarted(previous.ordinal, primaryState.currentTerm, governingConfigChange, isRestart))
							become(Isolated(primaryStateFence))
							startingCompletedCovenant.fulfillHere(())()
						} else {
							startingCompletedCovenant.fulfillHere(())()
							become(Stopped(Success(s"Star-up aborted because this ConsensusParticipant instance does not belong to the initial cluster-configuration.")))
						}

					case failure@Failure(e) =>
						scribe.error(s"$boundParticipantId: Unexpected error while loading the consensus-service's workspace:", e)
						become(Stopped(failure.castTo[String]))
						startingCompletedCovenant.fulfillHere(())()
				}
			}

			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.LatchedDuty[ResponseToClient] = {
				assert(isInSequence)
				for {
					_ <- startingCompletedCovenant
					rtc <- currentRole.onCommandFromClient(command, attemptFlag)
				} yield rtc
			}

			override def requestConfigChange(requestId: ConfigChangeRequestId, desiredParticipantsSet: Set[ParticipantId]): sequencer.LatchedDuty[ConfigChangeResponse] = {
				assert(isInSequence)
				for {
					_ <- startingCompletedCovenant
					response <- currentRole.requestConfigChange(requestId, desiredParticipantsSet)
				} yield response
			}
		}

		/**
		 * Behavior when the participant has the [[ISOLATED]] role. Taken when reachability to a majority of the participants was not achieved or after the [[STARTING]] role has completed.
		 * The participant transitions to this state after [[Starting]] or when reachability to other participants drops below [[smallestMajority]].
		 * This state is abandoned when a majority of the participants are reachable.
		 * [[Vote]]s cast by participants in this state are ignored.
		 */
		private final class Isolated(wsf: sequencer.CausalFence[PrimaryState]) extends StatefulRole(wsf) {
			override val ordinal: RoleOrdinal = ISOLATED

			/**
			 * The main loop of the isolated state.
			 * It checks if the current term leader is reachable or the reachable participants including itself are the majority.
			 * If so, it becomes a follower or a candidate respectively.
			 * If not, it stays in the isolated state and checks again after a while.
			 */
			override def onEnter(previous: Role): Unit = {
				notifyListeners(_.onBecameIsolated(previous.ordinal, wsf.committedUnsafe.currentTerm))
				if isEager then {
					val schedule = sequencer.newDelaySchedule(isolatedMainLoopInterval)
					currentSchedule = Maybe.some(schedule)
					// schedule the state-updater process
					updateRole().scheduled(schedule).triggerAndForget(true)
				}
			}

			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.LatchedDuty[ResponseToClient] = {
				currentRole.updatesRoleAndThenCallsOnCommandFromClient(command)
			}
		}

		/**
		 * Behavior when the participant has the [[Candidate]] role. Taken when reachability to a majority of the participants is achieved but none of them is in [[Leader]] role.
		 *
		 * In this state, the participant participates in leader election and may transition to [[Leader]], [[Follower]], or [[Isolated]] based on his viewpoint of other participants state.
		 * A new term is started when becoming leader.
		 * The [[ConsensusParticipant]] behaves exactly the same in [[Isolated]] and [[Candidate]] states. The only goal of the separation is to allow the user to distinguish if a majority of participants is reachable or not.
		 */
		private final class Candidate(wsf: sequencer.CausalFence[PrimaryState]) extends StatefulRole(wsf) {
			override val ordinal: RoleOrdinal = CANDIDATE

			override def onEnter(previous: Role): Unit = {
				notifyListeners(_.onBecameCandidate(previous.ordinal, getCommittedPrimaryState.currentTerm))
				if isEager then {
					val schedule = sequencer.newDelaySchedule(candidateMainLoopInterval)
					currentSchedule = Maybe.some(schedule)
					// schedule the state-update process
					updateRole().scheduled(schedule).triggerAndForget(true)
				}
			}

			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.LatchedDuty[ResponseToClient] = {
				updatesRoleAndThenCallsOnCommandFromClient(command)
			}
		}

		/**
		 * Behavior when the participant has the follower role. Taken when reachability to a majority of the participants is achieved and one of them has the [[Leader]] role and is in a higher or equal term.
		 *
		 * In this state, the participant acknowledges the specified leader and may transition to [[Candidate]] if the consensus protocol requires it.
		 *
		 * @param leaderId The ID of the leader this participant is following
		 */
		private final class Follower(val leaderId: ParticipantId, wsf: sequencer.CausalFence[PrimaryState]) extends StatefulRole(wsf) {
			override val ordinal: RoleOrdinal = FOLLOWER

			override def isEquivalentTo(other: Role): Boolean =
				super.isEquivalentTo(other) && leaderId == other.asInstanceOf[Follower].leaderId

			override def onEnter(previous: Role): Unit = {
				notifyListeners(_.onBecameFollower(previous.ordinal, getCommittedPrimaryState.currentTerm, leaderId))
			}

			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.LatchedDuty[ResponseToClient] = {
				assert(isInSequence)
				if attemptFlag == FIRST_ATTEMPT then sequencer.LatchedDuty_ready(RedirectTo(leaderId))
				else updatesRoleAndThenCallsOnCommandFromClient(command)
			}
		}

		/**
		 * Behavior when the participant has the [[LEADER]] role. Taken when reachability to a majority of the participants is achieved, none of them is a [[Leader]] with higher or equal term, and wins the new leader election.
		 *
		 * In this state, the participant coordinates consensus decisions.
		 */
		private final class Leader(initialPrimaryState: Accessible, wsf: sequencer.CausalFence[PrimaryState]) extends StatefulRole(wsf) {

			/** The index of the next record to send to a participant, indexed by the participant index.
			 * This array is optimistically initialized to the first empty record index of the leader's workspace for all participants,
			 * assuming that each follower's log is already up-to-date with the leader's log. This optimistic initialization
			 * allows the leader to attempt to append new entries immediately, but if a follower's log is actually behind or inconsistent,
			 * the index will be decremented as needed until the logs are aligned.
			 * When a record is successfully replicated to a participant, the index of the next record to send to that participant is incremented.
			 * When a record is not successfully replicated to a participant, the index of the next record to send to that participant is decremented.
			 */
			private val indexOfNextRecordToSend_ByParticipantIndex: Array[RecordIndex] = Array.fill(getCurrentConfigUpdated(initialPrimaryState).allOtherParticipants.size)(initialPrimaryState.firstEmptyRecordIndex)
			/** The highest record index known to be replicated to a participant, indexed by the participant index.
			 * This array is conservatively initialized to 0 for all participants, assuming that no records are known to be replicated to any follower at the start of the leader's term.
			 * As records are successfully replicated to a participant, the corresponding value is incremented.
			 * This conservative initialization ensures that the leader does not overestimate the replication state of any follower and only advances commitIndex when a true majority is confirmed.
			 */
			private val highestRecordIndexKnowToBeAppended_ByParticipantIndex: Array[RecordIndex] = Array.fill(getCurrentConfigUpdated(initialPrimaryState).allOtherParticipants.size)(0)

			override val ordinal: RoleOrdinal = LEADER

			private var failedReplicationsLoopSchedule: Maybe[sequencer.Schedule] = Maybe.empty

			override def onEnter(previous: Role): Unit = {
				for {
					primaryState1 <- primaryStateFence.advanceIf(
						(primaryState0, _) => {
							if currentRole ne this then Maybe.empty
							else Maybe.some(primaryState0.withTermUpdated(primaryState0.currentTerm + 1))
						},
						true
					)
					secondPhaseConfigChangeNotNeededOrItsReplicationWasSuccessful <- {
						if currentRole ne this then sequencer.LatchedDuty_false
						else primaryState1 match {
							case Inaccessible =>
								sequencer.LatchedDuty_false
							case accessible1: Accessible =>
								notifyListeners(_.onBecameLeader(previous.ordinal, primaryState1.currentTerm))

								accessible1.topConfigChange match {
									// If the top configuration change is a transitional one, start the second phase of the configuration change. This happens in the rare case that a leader that started a configuration change crashed or left the leadership before achieving the replicating of the StableConfigurationChange to a majority.
									case transitionalChange: TransitionalConfigChange[ParticipantId @unchecked] =>
										attemptConfigChangeSecondPhase(transitionalChange)
									case _: StableConfigChange[ParticipantId @unchecked] =>
										sequencer.LatchedDuty_true
								}
						}
					}
				} yield if currentRole eq this then {
					// if a second phase configuration change is needed and its replication failed, then become isolated.
					if !secondPhaseConfigChangeNotNeededOrItsReplicationWasSuccessful then become(Isolated(primaryStateFence))
					// else, if eager replication is set, start the replication in a decoupled manner.
					else if isEager then {
						for primaryState2 <- primaryStateFence.causalAnchor(true) yield {
							if currentRole eq this then primaryState2 match {
								case accessible2: Accessible => attemptToUpdateOtherParticipantsLogs(accessible2)
								case Inaccessible => ()
							}
						}
					}
				}
			}

			override def onLeave(): Unit = {
				failedReplicationsLoopSchedule.foreach(sequencer.cancel(_))
				failedReplicationsLoopSchedule = Maybe.empty
			}


			override def requestConfigChange(requestId: ConfigChangeRequestId, desiredParticipants: Set[ParticipantId]): sequencer.LatchedDuty[ConfigChangeResponse] = {
				for {
					primaryState0 <- primaryStateFence.causalAnchor(true)
					response <- {
						val currentConfig0 = getCurrentConfigUpdated(primaryState0)
						if currentRole ne this then currentRole.requestConfigChange(requestId, desiredParticipants)
						else {
							currentConfig0 match {
								case stable0: StableConfig =>
									if desiredParticipants == stable0.desiredParticipants then sequencer.LatchedDuty_ready(ALREADY_CHANGED)
									else {
										// start the first phase of the configuration change
										val tcc = new TransitionalConfigChange[ParticipantId](primaryState0.currentTerm, requestId, stable0.desiredParticipants, desiredParticipants)
										for {
											primaryState2 <- primaryStateFence.advanceIf(
												(primaryState1, _) => {
													if currentRole ne this then Maybe.empty
													else primaryState1 match {
														case accessible1: Accessible => Maybe.some(accessible1.withSingleRecordAppended(tcc))
														case Inaccessible => Maybe.empty
													}
												},
												true
											)
											response <- {
												if currentRole ne this then currentRole.requestConfigChange(requestId, desiredParticipants)
												else primaryState2 match {
													case Inaccessible =>
														sequencer.LatchedDuty_ready(UNABLE)
													case accessible2: Accessible =>
														for {
															isFirstPhaseChangeReplicatedToMajority <- attemptToUpdateOtherParticipantsLogs(accessible2)
															response <- {
																if currentRole ne this then currentRole.requestConfigChange(requestId, desiredParticipants)
																else if isFirstPhaseChangeReplicatedToMajority then {
																	for {
																		isSecondPhaseChangeReplicatedToMajority <- attemptConfigChangeSecondPhase(tcc)
																		response <- {
																			if isSecondPhaseChangeReplicatedToMajority then sequencer.LatchedDuty_ready(SUCCESSFULLY_CHANGED)
																			else if currentRole ne this then currentRole.requestConfigChange(requestId, desiredParticipants)
																			else for {
																				_ <- updateRole()
																				response <- currentRole.requestConfigChange(requestId, desiredParticipants)
																			} yield response
																		}
																	} yield response
																} else for {
																	_ <- updateRole()
																	response <- currentRole.requestConfigChange(requestId, desiredParticipants)
																} yield response
															}
														} yield response
												}
											}
										} yield response
									}

								case transitional0: TransitionalConfig =>
									if desiredParticipants == transitional0.desiredParticipants then sequencer.LatchedDuty_ready(ALREADY_IN_PROGRESS)
									else sequencer.LatchedDuty_ready(WAIT_PREVIOUS_CHANGE_TO_COMPLETE)

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
			 * @return  a [[sequencer.LatchedDuty]] that yields true/false if the [[StableConfigChange]] [[Record]] was/wasn't replicated to a majority of the given participants. */
			private def attemptConfigChangeSecondPhase(correspondingTransitionalConfigChange: TransitionalConfigChange[ParticipantId]): sequencer.LatchedDuty[Boolean] = {
				for {
					primaryState1 <- primaryStateFence.advanceIf(
						(primaryState0, _) => {
							if currentRole ne this then Maybe.empty
							else primaryState0 match {
								case accessible: Accessible =>
									val ccNew = new StableConfigChange[ParticipantId](accessible.currentTerm, correspondingTransitionalConfigChange.requestId, correspondingTransitionalConfigChange.term, correspondingTransitionalConfigChange.oldParticipants, correspondingTransitionalConfigChange.newParticipants)
									Maybe.some(accessible.withSingleRecordAppended(ccNew))
								case Inaccessible =>
									Maybe.empty
							}
						},
						true
					)
					isSecondPhaseChangeReplicatedToMajority <- {
						if currentRole ne this then sequencer.LatchedDuty_false
						else primaryState1 match {
							case accessible1: Accessible => attemptToUpdateOtherParticipantsLogs(accessible1)
							case Inaccessible => sequencer.LatchedDuty_false
						}
					}
				} yield {
					// if the second phase was replicated to a majority and this participant does not belong to the new configuration, become [[Stopped]].
					if isSecondPhaseChangeReplicatedToMajority && !correspondingTransitionalConfigChange.newParticipants.contains(boundParticipantId) then become(Stopped(Success("The second phase of a cluster-configuration transition has completed and this ConsensusParticipant does not belong to the new participants set.")))
					isSecondPhaseChangeReplicatedToMajority
				}
			}

			override def onCommandFromClient(clientCommand: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.LatchedDuty[ResponseToClient] = {
				assert(isInSequence)

				val clientId = clientIdOf(clientCommand)
				var commandRecordInfo: (recordIndex: RecordIndex, indexOfLastAppendedCommandFromClient: RecordIndex) | Null = null // secondary return value of the causal fence exclusive section
				for {
					// First, append the command to the log if it wasn't already
					primaryState1 <- primaryStateFence.advanceIf(
						(primaryState0, _) => {
							if currentRole ne this then Maybe.empty
							else primaryState0 match {
								case Inaccessible =>
									Maybe.empty
								case accessible: Accessible =>
									val indexOfLastAppendedCommandFromClient = accessible.indexOfLastAppendedCommandFrom(clientId)

									// If this is the first command received from the client, proceed normally (append, replicate, apply)
									if indexOfLastAppendedCommandFromClient == 0 then {
										commandRecordInfo = (primaryState0.firstEmptyRecordIndex, indexOfLastAppendedCommandFromClient)
										Maybe.some(accessible.withSingleRecordAppended(CommandRecord(accessible.currentTerm, clientCommand)))
									}
									// else, check if the command was received before:
									else {
										// @formatter:off
										accessible.getRecordAt(indexOfLastAppendedCommandFromClient) match {
											case CommandRecord[ClientCommand @unchecked](lastClientCommandTerm, lastClientCommand) =>
												val comparison = clientCommandOrdering.compare(clientCommand, lastClientCommand)
												// if the command is newer than the last received from the same client, append it to the log memorizing the index.
												if comparison > 0 then {
													commandRecordInfo = (accessible.firstEmptyRecordIndex, indexOfLastAppendedCommandFromClient)
													Maybe.some(accessible.withSingleRecordAppended(CommandRecord(accessible.currentTerm, clientCommand)))
												}
												// if the command is the same as the last received, memorize the index of the last received.
												else if comparison == 0 then {
													commandRecordInfo = (indexOfLastAppendedCommandFromClient, indexOfLastAppendedCommandFromClient)
													Maybe.empty
												}
												// if the command is older than the last received, obtain its index and memorize it.
												else {
													commandRecordInfo = (accessible.indexOf(clientCommand), indexOfLastAppendedCommandFromClient)
													Maybe.empty
												}
											case _ =>
												// Inconsistency. Should never happen
												Maybe.empty
										}
										// @formatter:on
									}
							}
						},
						true
					)
					// Second, replicate it if not already, and then, if replication was successful, apply it to the state machine assuming it is idempotent.
					response <- {
						if currentRole ne this then currentRole.onCommandFromClient(clientCommand, attemptFlag) // TODO analyze if the attemptFlag should be propagated as is or updated
						else primaryState1 match {
							case Inaccessible =>
								sequencer.LatchedDuty_ready(Unable(currentRole.ordinal, cluster.getOtherProbableParticipant))
							case accessible1: Accessible =>
								commandRecordInfo.asMatchable match {
									case null =>
										sequencer.LatchedDuty_ready(InconsistentState(s"For client $clientId, the last known log-entry index does not point to a record of the expected type."))

									case (recordIndex, indexOfLastAppendedCommandFromClient) =>
										if recordIndex == 0 then sequencer.LatchedDuty_ready(Stale(clientCommand, indexOfLastAppendedCommandFromClient))
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
														// if the command is commited, apply it to the state machine assuming it is idempotent.
														for smr <- machine.applyClientCommand(recordIndex, clientCommand) yield {
															lastAppliedCommandIndex = recordIndex
															Processed(recordIndex, smr)
														}
													} else {
														// if not able to replicate then update the role and start again // TODO analyze if updating the role instead of becoming isolated is a better alternative in some situations and in that case, add a configuration option that allows the user to decide.
														for {
															_ <- updateRole()
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
			 *		- If [[Record]]s weren't appended to a another participant, attempt to append them.
			 *			- If successful: update the corresponding entry of [[indexOfNextRecordToSend_ByParticipantIndex]]
			 *			- If AppendEntries fails because of log inconsistency: decrement the corresponding entry of [[indexOfNextRecordToSend_ByParticipantIndex]], and retry.
			 *		- If there exists an N such that N > [[commitIndex]], a majority of the [[highestRecordIndexKnowToBeAppended_ByParticipantIndex]] entries is ≥ N, and log[N].term == [[currentTerm]]: set commitIndex = N
			 *		- If there are laggard participants (a minority whose corresponding entry in [[highestRecordIndexKnowToBeAppended_ByParticipantIndex]] trails the leader's [[commitIndex]]), initiate targeted retries to catch them up.
			 * @return a [[sequencer.LatchedDuty]] that yields true if, and only if, for all the participants sets of the current [[Configuration]], all the records in this participant's log are successfully appended to at least:
			 *         - half of the other participants of the set, if this participant belongs to the set;
			 *         - a majority of the other participants of the set, if this participant does not belong to the set.
			 * */
			private def attemptToUpdateOtherParticipantsLogs(primaryState0: Accessible): sequencer.LatchedDuty[Boolean] = {
				// First, abort the failed replications loop if it is running.
				failedReplicationsLoopSchedule.foreach(sequencer.cancel(_))
				failedReplicationsLoopSchedule = Maybe.empty
				// Then, start regular replication to all followers.
				val until = primaryState0.firstEmptyRecordIndex
				// For every other participants, generate a task to replicate records this participant has and believes the others lack.
				val requestToAppendPendingRecords_byParticipantIndex =
					for otherParticipantIndex <- getCurrentConfigUpdated(primaryState0).allOtherParticipants.indices yield {
						appendsRecordsToParticipant(primaryState0, otherParticipantIndex, until)
					}
				// Execute the tasks in parallel. Note that the tasks, when successful, update the corresponding entry of the `indexOfNextRecordToSend_ByParticipantIndex` and `highestRecordIndexKnowToBeAppended_ByParticipantIndex` arrays.
				for {
					appendResults <- sequencer.LatchedDuty_sequenceTasksToArray(requestToAppendPendingRecords_byParticipantIndex)
					primaryState1 <- primaryStateFence.causalAnchor(true)
				} yield {
					// If the behavior hasn't changed while waiting the result, then the leader is still the same, and we can continue.
					if currentRole ne this then false
					else primaryState1 match {
						case accessible1: Accessible =>
							// Update the commitIndex if a majority of the followers have replicated the uncommited records.
							// If there exists an N such that N > commitIndex, the highest log-entry index known to be replicated is > N in a majority of the servers, and getRecordAt[N].term == currentTerm: set commitIndex = N
							val previousCommitIndex = commitIndex
							val config1 = getCurrentConfigUpdated(primaryState1)
							commitIndex = config1.indexOfTheCommittedRecordWithHighestIndex(accessible1, previousCommitIndex, IArray.unsafeFromArray(highestRecordIndexKnowToBeAppended_ByParticipantIndex))
							if commitIndex > previousCommitIndex then notifyListeners(_.onCommitIndexChange(previousCommitIndex, commitIndex))

							// If all the records in this participant were appended in enough other participants to achieve quorum, then schedule retries for the laggard participants and return true.
							if config1.achievesQuorumWhen(appendResults) then {
								// For those minority of participants whose highestRecordIndexKnowToBeReplicated is less than the commitIndex (because they failed to replicate commited records), retry the append records RPC.
								// This retry is indefinite until the outer method (replicateUncommitedRecords) is called again (as the effect of an external stimulus).

								// Retry the append on laggard participants but only if the replication succeeded in a majority. TODO: analyze if the retry should be done independently of majority-replication success.
								scheduleLaggardParticipantsRetry(config1, appendResults.zipWithIndex)
								true
							} else false

						case Inaccessible =>
							false
					}
				}
			}

			private def scheduleLaggardParticipantsRetry(config0: Configuration, previousTryResults: Iterable[(previousTryResult: Try[Boolean], participantIndex: Int)]): Unit = {
				if (currentRole ne this) || (config0 eq NoConfig) then return
				// Build the list of laggard participants. Include only the participants for which the append records RPC failed. Those for which the result was either successful or definitively rejected are filtered out.
				val indicesOfLaggardParticipants: List[Int] = previousTryResults.foldLeft(Nil) { (accumulator, elem) =>
					elem.previousTryResult match {
						case Success(wasReplicationSuccessful) =>
							assert(!wasReplicationSuccessful || highestRecordIndexKnowToBeAppended_ByParticipantIndex(elem.participantIndex) >= commitIndex, s"!$wasReplicationSuccessful || ${highestRecordIndexKnowToBeAppended_ByParticipantIndex(elem.participantIndex)} >= $commitIndex")
							accumulator
						case Failure(e) =>
							scribe.debug(s"$boundParticipantId: The replication of the commited records to ${config0.allOtherParticipants(elem.participantIndex)} failed with:", e)
							elem.participantIndex :: accumulator
					}
				}
				// if there are laggard participants, schedule a retry that replicates the log to them.
				if indicesOfLaggardParticipants.nonEmpty then {
					val schedule = sequencer.newDelaySchedule(failedReplicationsLoopInterval)
					failedReplicationsLoopSchedule = Maybe.some(schedule)
					sequencer.schedule(schedule) { _ =>
						if currentRole eq this then {
							for {
								primaryState1 <- primaryStateFence.causalAnchor(true)
							} yield {
								if currentRole eq this then primaryState1 match {
									case accessible1: Accessible =>
										val appendTasks = for followerIndex <- indicesOfLaggardParticipants yield appendsRecordsToParticipant(accessible1, followerIndex, accessible1.firstEmptyRecordIndex) // TODO Is `firstEmptyRecordIndex` the right index to retry until? Or should it be the `commitIndex`?
										for {
											appendResults <- sequencer.LatchedDuty_sequenceTasksToArray(appendTasks)
											primaryState2 <- primaryStateFence.causalAnchor(true)
										} yield scheduleLaggardParticipantsRetry(getCurrentConfigUpdated(primaryState2), appendResults.zipWithIndex)

									case Inaccessible => // do nothing
								}
							}
						}
					}
				}
			}

			/**
			 * Creates a task that sends log records to the specified participant (assuming optimistically it is in receptive state), starting from the corresponding [[indexOfNextRecordToSend_ByParticipantIndex]] up to (but not including) the given index.
			 * If the participant's response indicates its log does not match this participant's log before `indexOfNextRecordToSend`, the task will retry after a delay, starting from the previous record.
			 * @param destinationParticipantIndex The index of the participant in the [[otherParticipants]] array.
			 * @param until The index immediately after the last record to send. TODO: is this parameter needed? or should it be fixed to the [[Workspace.firstEmptyRecordIndex]]?
			 * @return A task that attempts to append records to the specified participant, yielding
			 *         - `true` if the participant was successful appending them, but not necessarily applying them. This is the only case in which, as side effect, the corresponding entry in the [[indexOfNextRecordToSend_ByParticipantIndex]] and [[highestRecordIndexKnowToBeAppended_ByParticipantIndex]] arrays is updated.
			 *         - `false` if the participant rejected the appending definitively.
			 *         - a failure if [[ClusterParticipant.appendRecords]] fails.
			 */
			private def appendsRecordsToParticipant(primaryState0: Accessible, destinationParticipantIndex: Int, until: RecordIndex): sequencer.Task[Boolean] = {
				assert(sequencer.isInSequence)

				val config0 = getCurrentConfigUpdated(primaryState0)
				val destinationParticipantId = config0.allOtherParticipants(destinationParticipantIndex)
				val indexOfNextRecordToSend = indexOfNextRecordToSend_ByParticipantIndex(destinationParticipantIndex)
				assert(until >= indexOfNextRecordToSend)
				val recordsToSend = primaryState0.getRecordsBetween(indexOfNextRecordToSend, until)
				val previousRecordIndex = indexOfNextRecordToSend - 1
				val previousRecordTerm = primaryState0.getRecordTermAt(previousRecordIndex)
				for {
					appendResult <- destinationParticipantId.appendRecords(primaryState0.currentTerm, previousRecordIndex, previousRecordTerm, recordsToSend, commitIndex, primaryState0.getRecordTermAt(commitIndex))
					appendWasSuccessful <- {
						// This point is reached only if the destination participant responds normally to the AppendRecords request.
						// If this participant's behavior changed while waiting the destination participant response, ignore the response and yield successfully (to skip/exit the retry laggards loop).
						if currentRole ne this then sequencer.Task_false
						// If the term (according to the target) is greater than the current term (according to this participant), then the leader has changed. So:
						else {
							for {
								primaryState1 <- primaryStateFence.advanceIf(updateTermIfLessThan(appendResult.term)).toTask
								appendWasSuccessful <- {
									if currentRole ne this then sequencer.Task_false
									else primaryState1 match {
										case Inaccessible =>
											sequencer.Task_false
										case accessible1: Accessible =>
											if accessible1.currentTerm > primaryState0.currentTerm then {
												// start a role update in a decoupled manner.
												updateRole()
												// yield false
												sequencer.Task_false
											}
											// If the destination participant has successfully appended the records to its log and persisted its workspace, then update the index of the next record to send and the highest record index known to be replicated and yield a success.
											else if appendResult.success then {
												assert(indexOfNextRecordToSend + recordsToSend.size == until)
												indexOfNextRecordToSend_ByParticipantIndex(destinationParticipantIndex) = until
												highestRecordIndexKnowToBeAppended_ByParticipantIndex(destinationParticipantIndex) = until - 1
												sequencer.Task_true
											}
											// else (if the participant rejected the appending)
											else {
												// If the rejection was because the previous record's term is not the same in his log and my log then:
												if appendResult.roleOrdinal >= ISOLATED && appendResult.term == accessible1.currentTerm then {
													// if I remember the previous record then try again starting one record before.
													if indexOfNextRecordToSend > accessible1.lobBufferOffset then {
														indexOfNextRecordToSend_ByParticipantIndex(destinationParticipantIndex) = indexOfNextRecordToSend - 1
														appendsRecordsToParticipant(accessible1, destinationParticipantIndex, accessible1.firstEmptyRecordIndex)
													}
													// else (I don't remember the previous record)
													else {
														// inform about the illegal state. // TODO analyze a better way to inform this problem after implementing the log snapshot mechanism.
														scribe.error(s"$boundParticipantId: Unable to replicate uncommited records to $destinationParticipantId because its log has inconsistencies at records that predate the last snapshot (they are at indexes less than my logBufferOffset ($accessible1.logBufferOffset)). THIS SHOULD NOT HAPPEN. PLEASE REPORT THIS AS A BUG.")
														// yield a success but without any change to the corresponding entry in indexOfNextRecordToSend_ByParticipantIndex and highestRecordIndexKnowToBeAppended_ByParticipantIndex.
														sequencer.Task_false
													}
												}
												// if the rejection was for another reason, yield false without any change to the corresponding entry in indexOfNextRecordToSend_ByParticipantIndex and highestRecordIndexKnowToBeAppended_ByParticipantIndex.
												else sequencer.Task_false
											}
									}
								}
							} yield appendWasSuccessful
						}
					}
				} yield appendWasSuccessful
			}
		}


		/** A view of the participant’s current primary state (log, term, etc.), and also trivially derived state.
		 *
		 * IMPORTANT: the [[Accessible]] subtype of this trait exposes mutable state. To preserve causal ordering:
		 * - All writes must occur inside an updater passed to [[StatefulRole.primaryStateFence.advance]].
		 * - All reads must occur either inside said updater or in a consumer subscribed to [[StatefulRole.primaryStateFence.causalAnchor]].
		 *
		 * Direct mutation or observation of [[Accessible]] outside these mechanisms breaks causal guarantees.
		 */
		private sealed trait PrimaryState {
			val currentTerm: Term

			/** Index of the first empty record in the log.
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
			inline def lobBufferOffset: RecordIndex =
				workspace.logBufferOffset

			/** CAUTION: This method accesses mutable state of the [[PrimaryState]], so it should be called either:
			 *   - within an [[StatefulRole.primaryStateFence.advance]] section. In this case, the call must occur before or during the completion of the [[sequencer.LatchedDuty]] returned by the function passed	to `advance`; once that duty has completed, the causal fence is closed and later calls are unsafe.
			 *   - within a causally anchored consumer (i.e. consumers subscribed to the [[sequencer.LatchedDuty]] returned by either [[StatefulRole.primaryStateFence.advance]] or [[StatefulRole.primaryStateFence.causalAnchor]]). In this case, the call must occur synchronously during the consumer’s execution; it must not be deferred to code scheduled after the consumer has returned, since such deferred code would no longer be causally anchored. */
			inline def indexOfTopConfigChange: RecordIndex =
				workspace.indexOfTopConfigChange

			/** CAUTION: This method accesses mutable state of the [[PrimaryState]], so it should be called either:
			 *   - within an [[StatefulRole.primaryStateFence.advance]] section. In this case, the call must occur before or during the completion of the [[sequencer.LatchedDuty]] returned by the function passed	to `advance`; once that duty has completed, the causal fence is closed and later calls are unsafe.
			 *   - within a causally anchored consumer (i.e. consumers subscribed to the [[sequencer.LatchedDuty]] returned by either [[StatefulRole.primaryStateFence.advance]] or [[StatefulRole.primaryStateFence.causalAnchor]]). In this case, the call must occur synchronously during the consumer’s execution; it must not be deferred to code scheduled after the consumer has returned, since such deferred code would no longer be causally anchored. */
			inline def topConfigChange: ConfigChange =
				workspace.topConfigChange

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

			/** CAUTION: This method mutates of the [[PrimaryState]], so it should be called within the safe temporal windows provided by [[StatefulRole.primaryStateFence.advance]]. */
			override def withTermUpdated(newTerm: Term): sequencer.LatchedDuty[PrimaryState] = {
				assert(newTerm > currentTerm)
				workspace.setCurrentTerm(newTerm)
				saveWorkspace()
			}

			/** CAUTION: This method mutates of the [[PrimaryState]], so it should be called within an [[StatefulRole.primaryStateFence.advance]] section only. */
			def withSingleRecordAppended(record: Record): sequencer.LatchedDuty[PrimaryState] = {
				workspace.appendRecord(record)
				saveWorkspace()
			}

			/** CAUTION: This method mutates of the [[PrimaryState]], so it should be called within an [[StatefulRole.primaryStateFence.advance]] section only. */
			def withRecordsAppended(term: Term, records: GenIndexedSeq[Record], from: RecordIndex): sequencer.LatchedDuty[PrimaryState] = {
				workspace.setCurrentTerm(term)
				workspace.appendResolvingConflicts(records, from)
				saveWorkspace()
			}

			/** CAUTION: This method mutates of the [[PrimaryState]], so it should be called within the safe temporal windows provided by [[StatefulRole.primaryStateFence.advance]]. */
			inline def release(): Unit =
				workspace.release()

			/** Saves the [[Workspace]] of this [[PrimaryState]] in the [[Storage]].
			 * CAUTION: This method accesses mutable state of the [[PrimaryState]], so it should be called within an [[StatefulRole.primaryStateFence.advance]] section only.
			 * @return the saved [[PrimaryState]] */
			private def saveWorkspace(): sequencer.LatchedDuty[PrimaryState] = {
				storage.save(workspace).map {
					case _: Success[Unit] =>
						if currentRole.ordinal <= STARTING then {
							workspace.release()
							Inaccessible
						}
						else Accessible(workspace)

					case failure: Failure[Unit] =>
						scribe.error(s"$boundParticipantId: Unexpected error while saving the workspace. This participant's consensus service is unable to continue following the leader and will stop.", failure.exception)
						become(Stopped(failure.castTo[String]))
						workspace.release() // just in case storage.save does not do it.
						Inaccessible
				}
			}

			override def toString: String = s"Accessible(currentTerm=$currentTerm, firstEmptyRecordIndex=$firstEmptyRecordIndex, indexOfTopConfigChange=$indexOfTopConfigChange)"
		}

		/** Defines the participation-configuration-related behavior of the participant.
		 *
		 * A [[Configuration]] represents the membership rules that govern replication, quorum formation, and elections. There are exactly two behaviors:
		 *
		 *   - [[TransitionalConfig]]: Joint consensus (`Cold` ∪ `Cnew`).
		 *     This behavior begins immediately once the transitional entry is appended to the log.
		 *     Replication and quorum require majorities across both `Cold` and `Cnew`.
		 *     Elections must also consider both sets.
		 *     Transitional behavior remains active until a [[StableConfigChange]] entry is replicated to majority of both `Cold` and `Cnew` ([[commitIndex]] >= indexOfStableConfigChange).
		 *
		 *   - [[StableConfig]]: Pure new consensus (Cnew).
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
			/** The current set of participants in the cluster, according to this participant, sorted.
			 * Should be reflected in the [[Workspace]].
			 * */
			val allParticipants: IArray[ParticipantId]
			/** The current set of participants in the cluster excluding this participant, sorted.
			 * Should be updated whenever [[currentParticipants]] mutates */
			val allOtherParticipants: IArray[ParticipantId]

			/** The [[ConfigChange]] that caused this [[Configuration]] and on which it is based.
			 * A [[ConfigChange]]s is a snapshots, so it contain all the information needed. */
			val correspondingConfigChange: ConfigChange

			/** The identifiers of the desired set of participants. */
			def desiredParticipants: Set[ParticipantId]

			/** The set of participants to include in [[Unable]] responses. */
			def otherProbableParticipants: Set[ParticipantId]

			def isMajority(iterator: Iterator[ParticipantId]): Boolean

			def reachedAll(vote: Vote[ParticipantId]): Boolean

			def reachedAMajority(vote: Vote[ParticipantId]): Boolean

			def indexOfTheCommittedRecordWithHighestIndex(primaryState: Accessible, from: RecordIndex, highestRecordIndexKnowToBeAppended_ByParticipantIndex: IArray[RecordIndex]): RecordIndex

			def achievesQuorumWhen(appendResults: Array[Try[Boolean]]): Boolean

			def determineRole(primaryState: Accessible, primaryStateFence: sequencer.CausalFence[PrimaryState], myVote: Vote[ParticipantId], votesFromOthers: Array[Try[Vote[ParticipantId]]]): Role

			/**
			 * Determines the best leader candidate based on the [[StateInfo]]s of all the participants, including itself.
			 * This method only queries. Does not mutate anything.
			 *
			 * @param inquirerId the id of the participant of which this participant already knows its [[StateInfo]] (because it sent it to this participant along the request this participant is currently responding) or null if this participant does not know the [[StateInfo]] of anyone else. Note that this parameter is not necessarily needed. Its only purpose is to improve efficiency by saving a query of already known information.
			 * @param inquirerInfo the [[StateInfo]] of the inquirer if `inquirerId` is not null. Note that this parameter is not necessarily needed. Its only purpose is to improve efficiency by saving a query of already known information.
			 * @return A task that yields a [[Vote]] with the chosen leader for the current term.
			 */
			def decideMyVote(primaryState: PrimaryState, inquirerId: ParticipantId | Null, inquirerInfo: StateInfo): sequencer.LatchedDuty[Vote[ParticipantId]]

			/** @return the index of the specified [[ParticipantId]] in the [[config.allParticipants]]' [[IndexedSeq]].
			 * @param participantId the id of the participant to find. */
			inline def participantIndexOf(participantId: ParticipantId): Int = {
				java.util.Arrays.binarySearch(allParticipants.asInstanceOf[Array[ParticipantId]], participantId, participantIdComparator)
			}
		}

		private object NoConfig extends Configuration {
			override val allParticipants: IArray[ParticipantId] =
				IArray(boundParticipantId)

			override val allOtherParticipants: IArray[ParticipantId] =
				IArray.empty

			override val correspondingConfigChange: ConfigChange =
				null

			override def desiredParticipants: Set[ParticipantId] =
				Set.empty

			def otherProbableParticipants: Set[ParticipantId] =
				cluster.getOtherProbableParticipant

			override def isMajority(iterator: Iterator[ParticipantId]): Boolean =
				false

			override def reachedAll(vote: Vote[ParticipantId]): Boolean =
				false

			override def reachedAMajority(vote: Vote[ParticipantId]): Boolean =
				false

			override def indexOfTheCommittedRecordWithHighestIndex(primaryState: Accessible, from: RecordIndex, highestRecordIndexKnowToBeAppended_ByParticipantIndex: IArray[RecordIndex]): RecordIndex =
				from

			override def achievesQuorumWhen(appendResults: Array[Try[Boolean]]): Boolean =
				false

			override def determineRole(primaryState: Accessible, primaryStateFence: sequencer.CausalFence[PrimaryState], myVote: Vote[ParticipantId], votesFromOthers: Array[Try[Vote[ParticipantId]]]): Role =
				Stopped(Failure(AssertionError("Can not happen")))

			override def decideMyVote(primaryState: PrimaryState, inquirerId: ParticipantId | Null, inquirerInfo: StateInfo): sequencer.LatchedDuty[Vote[ParticipantId]] =
				sequencer.LatchedDuty_ready(Vote(0, boundParticipantId, 0, 0, currentRole.ordinal))
		}

		/** Pure new configuration (`Cnew`).
		 *
		 * Activated only once the [[StableConfigChange]] entry is committed (commitIndex ≥ indexOfStableConfigChange).
		 * Replication and quorum reduce to `Cnew` only.
		 * Cold-only servers, having seen this entry, shut down.
		 */
		private final class StableConfig(override val correspondingConfigChange: StableConfigChange[ParticipantId]) extends Configuration {

			override val allParticipants: IArray[ParticipantId] = IArray.unsafeFromArray(correspondingConfigChange.newParticipants.toArray.sorted)
			private val smallestMajority: Int = 1 + allParticipants.length / 2
			override val allOtherParticipants: IArray[ParticipantId] = allParticipants.filter(_ != boundParticipantId)

			override def desiredParticipants: Set[ParticipantId] =
				correspondingConfigChange.newParticipants

			def otherProbableParticipants: Set[ParticipantId] =
				allOtherParticipants.toSet

			override def isMajority(iterator: Iterator[ParticipantId]): Boolean = {
				iterator.count(p => allParticipants.contains(p)) >= smallestMajority
			}

			override def reachedAll(vote: Vote[ParticipantId]): Boolean = {
				vote.reachableCandidatesOfOldConf == allParticipants.length
			}

			override def reachedAMajority(vote: Vote[ParticipantId]): Boolean = {
				vote.reachableCandidatesOfOldConf >= smallestMajority
			}

			@tailrec
			override def indexOfTheCommittedRecordWithHighestIndex(primaryState: Accessible, from: RecordIndex, highestRecordIndexKnowToBeAppended_ByParticipantIndex: IArray[RecordIndex]): RecordIndex = {
				val n = from + 1
				if n < primaryState.firstEmptyRecordIndex && highestRecordIndexKnowToBeAppended_ByParticipantIndex.count(_ >= n) + 1 >= smallestMajority && primaryState.getRecordTermAt(n) == primaryState.currentTerm then {
					indexOfTheCommittedRecordWithHighestIndex(primaryState, n, highestRecordIndexKnowToBeAppended_ByParticipantIndex)
				} else from
			}

			override def achievesQuorumWhen(appendResults: Array[Try[Boolean]]): Boolean = {
				1 + appendResults.count(r => r.isSuccess && r.get) >= smallestMajority
			}

			override def determineRole(primaryState: Accessible, primaryStateFence: sequencer.CausalFence[PrimaryState], myVote: Vote[ParticipantId], votesFromOthers: Array[Try[Vote[ParticipantId]]]): Role = {
				if myVote.term < primaryState.currentTerm then Candidate(primaryStateFence)
				else {
					var votesMatchingMyVoteCount = 1 // includes my vote
					for case Success(replierVote) <- votesFromOthers do {
						if replierVote.candidateId == myVote.candidateId && reachedAMajority(replierVote) then votesMatchingMyVoteCount += 1
					}
					if votesMatchingMyVoteCount < smallestMajority then Candidate(primaryStateFence)
					else if myVote.candidateId == boundParticipantId then Leader(primaryState, primaryStateFence)
					else Follower(myVote.candidateId, primaryStateFence)
				}
			}

			override def decideMyVote(primaryState: PrimaryState, inquirerId: ParticipantId | Null, inquirerInfo: StateInfo): sequencer.LatchedDuty[Vote[ParticipantId]] = {
				scribe.trace(s"$boundParticipantId: decideMyVote($primaryState, $inquirerId, $inquirerInfo) was called")
				val howAreYouOngoingQuestions = askHowOtherParticipantsAre(primaryState, inquirerId, inquirerInfo)
				for replies <- sequencer.LatchedDuty_sequenceTasksToArray(howAreYouOngoingQuestions) yield {
					scribe.trace(s"$boundParticipantId: decideMyVote($primaryState, $inquirerId, $inquirerInfo) - replies = ${replies.mkString("[", ", ", "]")}")
					var latestTermSeen = primaryState.currentTerm
					var chosenCandidate = CandidateInfo(boundParticipantId, currentRole.buildMyStateInfo(primaryState))
					var borrame = List(chosenCandidate) //TODO delete line
					var reachableCandidatesCounter = 1 // includes itself
					for replierIndex <- replies.indices do {
						val replierId = allOtherParticipants(replierIndex)
						replies(replierIndex) match {
							case Success(replierInfo) =>
								reachableCandidatesCounter += 1
								if replierInfo.currentTerm > latestTermSeen then {
									latestTermSeen = replierInfo.currentTerm
								}
								val candidateInfo = CandidateInfo(replierId, replierInfo)
								borrame = candidateInfo :: borrame //TODO delete line
								chosenCandidate = chosenCandidate.getWinnerAgainst(candidateInfo)

							case Failure(e) =>
								scribe.debug(s"$boundParticipantId: `$replierId.howAreYou(${primaryState.currentTerm})` failed while deciding vote: $e")
						}
					}
					scribe.debug(s"$boundParticipantId: decideMyVote($inquirerId, $inquirerInfo): chosen=$chosenCandidate, among: $borrame") //TODO delete line
					Vote(latestTermSeen, chosenCandidate.id, reachableCandidatesCounter, 0, chosenCandidate.info.ordinal)
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
		private final class TransitionalConfig(override val correspondingConfigChange: TransitionalConfigChange[ParticipantId]) extends Configuration {
			private val oldParticipants: Set[ParticipantId] = correspondingConfigChange.oldParticipants
			private val newParticipants: Set[ParticipantId] = correspondingConfigChange.newParticipants
			private val oldSmallestMajority: Int = 1 + oldParticipants.size / 2
			private val newSmallestMajority: Int = 1 + newParticipants.size / 2
			override val allParticipants: IArray[ParticipantId] = IArray.unsafeFromArray((newParticipants union oldParticipants).toArray.sorted)
			override val allOtherParticipants: IArray[ParticipantId] = allParticipants.filter(_ != boundParticipantId)

			def otherProbableParticipants: Set[ParticipantId] = allOtherParticipants.toSet

			override def desiredParticipants: Set[ParticipantId] =
				correspondingConfigChange.newParticipants

			override def isMajority(iterator: Iterator[ParticipantId]): Boolean = {
				var oldCount = 0
				var newCount = 0
				for p <- iterator do {
					if oldParticipants.contains(p) then oldCount += 1
					if newParticipants.contains(p) then newCount += 1
				}
				oldCount >= oldSmallestMajority && newCount >= newSmallestMajority
			}

			override def reachedAll(vote: Vote[ParticipantId]): Boolean = {
				vote.reachableCandidatesOfOldConf == oldParticipants.size && vote.reachableCandidatesOfNewConf == newParticipants.size
			}

			override def reachedAMajority(vote: Vote[ParticipantId]): Boolean = {
				vote.reachableCandidatesOfOldConf >= oldSmallestMajority && vote.reachableCandidatesOfNewConf >= newSmallestMajority
			}

			@tailrec
			override def indexOfTheCommittedRecordWithHighestIndex(primaryState: Accessible, from: RecordIndex, highestRecordIndexKnowToBeAppended_ByParticipantIndex: IArray[RecordIndex]): RecordIndex = {
				val n = from + 1
				if n >= primaryState.firstEmptyRecordIndex then from
				else if primaryState.getRecordTermAt(n) != primaryState.currentTerm then from
				else {
					var oldParticipantsWithRecordAtNSuccessfullyAppended = 0
					var newParticipantsWithRecordAtNSuccessfullyAppended = 0
					for otherParticipantIndex <- allOtherParticipants.indices do {
						if highestRecordIndexKnowToBeAppended_ByParticipantIndex(otherParticipantIndex) >= n then {
							val otherParticipantId = allOtherParticipants(otherParticipantIndex)
							if oldParticipants.contains(otherParticipantId) then oldParticipantsWithRecordAtNSuccessfullyAppended += 1
							if newParticipants.contains(otherParticipantId) then newParticipantsWithRecordAtNSuccessfullyAppended += 1
						}
					}
					if oldParticipantsWithRecordAtNSuccessfullyAppended < oldSmallestMajority || newParticipantsWithRecordAtNSuccessfullyAppended < newSmallestMajority then from
					else indexOfTheCommittedRecordWithHighestIndex(primaryState, n, highestRecordIndexKnowToBeAppended_ByParticipantIndex)
				}
			}

			override def achievesQuorumWhen(appendResults: Array[Try[Boolean]]): Boolean = {
				var newParticipantsWithSuccessfulAppendResult = 0
				var oldParticipantsWithSuccessfulAppendResult = 0
				// start the loop with this participant, assuming it already appended the records and will persist its state after calling this method.
				var participantId = boundParticipantId
				var otherParticipantIndex = allOtherParticipants.length
				while otherParticipantIndex >= 0 do {
					if oldParticipants.contains(participantId) then oldParticipantsWithSuccessfulAppendResult += 1
					if newParticipants.contains(participantId) then newParticipantsWithSuccessfulAppendResult += 1
					otherParticipantIndex -= 1
					while otherParticipantIndex >= 0 && participantId == boundParticipantId do {
						appendResults(otherParticipantIndex) match {
							case Success(true) =>
								participantId = allOtherParticipants(otherParticipantIndex)
							case _ =>
								otherParticipantIndex -= 1
								participantId = boundParticipantId
						}
					}
				}
				oldParticipantsWithSuccessfulAppendResult >= oldSmallestMajority && newParticipantsWithSuccessfulAppendResult >= newSmallestMajority
			}

			override def determineRole(primaryState: Accessible, primaryStateFence: sequencer.CausalFence[PrimaryState], myVote: Vote[ParticipantId], votesFromOthers: Array[Try[Vote[ParticipantId]]]): Role = {

				if myVote.term < primaryState.currentTerm then Candidate(primaryStateFence)
				else {
					var oldParticipantsVotesMatchingMyVote = 0
					var newParticipantsVotesMatchingMyVote = 0
					var vote = myVote
					var voteIndex = votesFromOthers.length
					while voteIndex >= 0 do {
						if vote.candidateId == myVote.candidateId && reachedAMajority(vote) then {
							if oldParticipants.contains(vote.candidateId) then oldParticipantsVotesMatchingMyVote += 1
							if newParticipants.contains(vote.candidateId) then newParticipantsVotesMatchingMyVote += 1
						}
						voteIndex -= 1
						// navigate to the next successful vote and get it.
						while voteIndex >= 0 && (vote eq myVote) do {
							votesFromOthers(voteIndex) match {
								case s: Success[Vote[ParticipantId]] =>
									vote = s.value

								case _: Failure[Vote[ParticipantId]] =>
									vote = myVote
									voteIndex -= 1
							}
						}
					}
					if oldParticipantsVotesMatchingMyVote < oldSmallestMajority || newParticipantsVotesMatchingMyVote < newSmallestMajority then Candidate(primaryStateFence)
					else if myVote.candidateId == boundParticipantId then Leader(primaryState, primaryStateFence)
					else Follower(myVote.candidateId, primaryStateFence)
				}
			}

			override def decideMyVote(primaryState: PrimaryState, inquirerId: ParticipantId | Null, inquirerInfo: StateInfo): sequencer.LatchedDuty[Vote[ParticipantId]] = {
				val howAreYouQuestions = askHowOtherParticipantsAre(primaryState, inquirerId, inquirerInfo)

				for replies <- sequencer.LatchedDuty_sequenceTasksToArray(howAreYouQuestions) yield {

					val myStateInfo = currentRole.buildMyStateInfo(primaryState)

					var stateInfo = myStateInfo
					var participantId = boundParticipantId
					var highestTermSeen = primaryState.currentTerm

					var oldParticipantsThatAreReachable = 0
					var newParticipantsThatAreReachable = 0
					var chosenCandidate = CandidateInfo(participantId, stateInfo)
					var borrame = List(chosenCandidate) //TODO delete line

					var participantIndex = allOtherParticipants.length
					while participantIndex >= 0 do {
						if oldParticipants.contains(participantId) then oldParticipantsThatAreReachable += 1
						if newParticipants.contains(participantId) then newParticipantsThatAreReachable += 1

						participantIndex -= 1
						// navigate to the next successfully replied StateInfo and get it
						while participantIndex >= 0 && (stateInfo eq myStateInfo) do {
							participantId = allOtherParticipants(participantIndex)
							replies(participantIndex) match {
								case s: Success[StateInfo] =>
									stateInfo = s.value
									if stateInfo.currentTerm > highestTermSeen then highestTermSeen = stateInfo.currentTerm
									val candidateInfo = CandidateInfo(participantId, stateInfo)
									chosenCandidate = chosenCandidate.getWinnerAgainst(candidateInfo)
									borrame = candidateInfo :: borrame //TODO delete line

								case f: Failure[StateInfo] =>
									scribe.debug(s"$boundParticipantId: `$participantId.howAreYou(${primaryState.currentTerm})` failed while deciding vote: ${f.exception}")
									stateInfo = myStateInfo
									participantIndex -= 1
							}
						}
					}
					scribe.debug(s"$boundParticipantId: decideMyVote($inquirerId, $inquirerInfo): chosen=$chosenCandidate, among: $borrame") //TODO delete line

					Vote(highestTermSeen, chosenCandidate.id, oldParticipantsThatAreReachable, newParticipantsThatAreReachable, currentRole.ordinal)
				}
			}
		}

		private def Configuration_from(configChange: ConfigChange): Configuration = {
			configChange match {
				case cc: TransitionalConfigChange[ParticipantId] @unchecked =>
					TransitionalConfig(cc)
				case cc: StableConfigChange[ParticipantId] @unchecked =>
					StableConfig(cc)
			}
		}

		//// UTILITIES USED BY MANY BEHAVIORS

		/** @return a [[PrimaryState]] updater procedure that, if the provided term is greater than the persisted one, updates the second. */
		private def updateTermIfLessThan(seenTerm: Term): (PrimaryState, Null) => Maybe[sequencer.LatchedDuty[PrimaryState]] = {
			// scribe.trace(s"$boundParticipantId: updateTermIfLessThan(seenTerm=$seenTerm) was called")
			(primaryState, _) => {
				// scribe.trace(s"$boundParticipantId: updateTermIfLessThan(seenTerm=$seenTerm) - primaryState=$primaryState")
				if seenTerm <= primaryState.currentTerm then Maybe.empty
				else Maybe.some(primaryState.withTermUpdated(seenTerm))
			}
		}

		/**
		 * Asks the [[otherParticipants]] how are they.
		 *
		 * @param inquirerId the id of the participant of which this participant already knows its [[StateInfo]] (because it sent it to this participant along the request this participant is currently responding) or null if this participant does not know the [[StateInfo]] of anyone else. Note that this parameter is not necessarily needed. Its only purpose is to improve efficiency by saving a query of already known information.
		 * @param inquirerInfo the [[StateInfo]] of the inquirer if `inquirerId` is not null. Note that this parameter is not necessarily needed. Its only purpose is to improve efficiency by saving a query of already known information.
		 * @return A task that yields a [[Vote]] with the chosen leader for the current term.
		 */
		private def askHowOtherParticipantsAre(primaryState: PrimaryState, inquirerId: ParticipantId | Null, inquirerInfo: StateInfo): IndexedSeq[sequencer.LatchedTask[StateInfo]] = {
			// scribe.trace(s"$boundParticipantId: askHowOtherParticipantsAre(primaryState=$primaryState, inquirerId=$inquirerId, inquirerInfo=$inquirerInfo) was called.")
			for otherParticipantId <- getCurrentConfigUpdated(primaryState).allOtherParticipants yield {
				// scribe.trace(s"$boundParticipantId: askHowOtherParticipantsAre(primaryState=$primaryState, inquirerId=$inquirerId, inquirerInfo=$inquirerInfo) - otherParticipantId=$otherParticipantId.")
				if otherParticipantId == inquirerId then sequencer.LatchedTask_ready(Success(inquirerInfo))
				else askHowIsAnotherIfNotAlready(primaryState, otherParticipantId)
			}
		}

		/** @return a [[sequencer.Task]] that yields the response to the "how are you" question to a specified participant.
		 *         If such a question is on the way, yields the response of already done question. */
		private def askHowIsAnotherIfNotAlready(primaryState: PrimaryState, otherParticipantId: ParticipantId): sequencer.LatchedTask[StateInfo] = {
			howAreYouRequestOnTheWayByParticipant.getOrElseUpdate(
				otherParticipantId,
				sequencer.Commitment_triggerAndWire(
					otherParticipantId.howAreYou(primaryState.currentTerm).andThen(_ => howAreYouRequestOnTheWayByParticipant.remove(otherParticipantId)),
					true
				)
			)
		}

		/**
		 * Knows all the information about a candidate necessary by participants to decide which to vote in a leader election.
		 */
		private final class CandidateInfo(val id: ParticipantId, val info: StateInfo) {
			/** @return the winner of the competition between this candidate and the other candidate when competing for leadership. The winner is the more up-to-date one.
			 * The more up-to-date criteria are: greater current term, is leader over not, greater last record term, longer log, and lesser [[ParticipantId]], with the left to right priority.
			 */
			def getWinnerAgainst(other: CandidateInfo): CandidateInfo = {
				if this.info.currentTerm > other.info.currentTerm then this
				else if this.info.currentTerm < other.info.currentTerm then other
				else if this.info.ordinal == LEADER && other.info.ordinal != LEADER then this
				else if this.info.ordinal != LEADER && other.info.ordinal == LEADER then other
				else if this.info.termAtCommitIndex > other.info.termAtCommitIndex then this
				else if this.info.termAtCommitIndex < other.info.termAtCommitIndex then other
				else if this.info.commitIndex > other.info.commitIndex then this
				else if this.info.commitIndex < other.info.commitIndex then other
				else if this.id < other.id then this
				else other
			}

			override def toString: String =
				s"CandidateInfo(participant: $id, currentTerm:${info.currentTerm}, ordinal:${info.ordinal}, termAtCommitIndex: ${info.termAtCommitIndex}, commitIndex: ${info.commitIndex})"
		}

		//// NOTIFICATIONS

		final def subscribe(listener: NotificationListener): Unit = {
			assert(isInSequence)
			notificationListeners.put(listener, None)
		}

		final def unsubscribe(listener: NotificationListener): Boolean = {
			assert(isInSequence)
			notificationListeners.remove(listener) eq None
		}

		/** @param notificator a function that receives a [[NotificationListener]] and calls one of its methods. */
		private inline def notifyListeners(inline notificator: NotificationListener => Unit): Unit = {
			assert(sequencer.isInSequence)
			notificationListeners.forEach { (listener, _) =>
				try notificator(listener)
				catch {
					case NonFatal(e) => scribe.error(s"$boundParticipantId: the listener $listener threw an exception while handling $notificator", e)
				}
			}
		}
	}
}
