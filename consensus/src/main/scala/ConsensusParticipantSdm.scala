package readren.consensus

import readren.common.Maybe
import readren.sequencer.{Doer, MilliDuration, SchedulingExtension}

import java.util
import java.util.Comparator
import scala.annotation.tailrec
import scala.collection.{mutable, IndexedSeq as GenIndexedSeq}
import scala.math.Ordering.Implicits.infixOrderingOps
import scala.reflect.ClassTag
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

	/** The type of behavior ids. */
	final type BehaviorOrdinal = Byte
	final val STOPPED: BehaviorOrdinal = 0
	final val STARTING: BehaviorOrdinal = 1
	final val ISOLATED: BehaviorOrdinal = 2
	final val CANDIDATE: BehaviorOrdinal = 3
	final val FOLLOWER: BehaviorOrdinal = 4
	final val LEADER: BehaviorOrdinal = 5

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
	 * @see [[ConsensusParticipantSdm.ClusterParticipant.chooseALeader]] and [[ConsensusParticipantSdm.ClusterParticipant.MessagesListener.onChooseALeader]].
	 * @tparam Id The identifier type for participants in the consensus cluster.
	 *            This must match the concrete type used to implement [[ConsensusParticipantSdm.ParticipantId]].
	 *            It allows the user to customize how participants are identified (e.g., UUID, String, custom class), while preserving type safety across [[Vote]] instances exchanges.
	 *            Although [[Vote]] is designed to travel between participants, it remains path-dependent and must be instantiated within a module that resolves [[ParticipantId]] to a concrete type.
	 * @param term The term for which the vote is cast.
	 * @param candidateId The id of the voted candidate.
	 * @param reachableCandidatesOfOldConf The number of candidates of the current or old set of participants that were reachable by the voter (including the voter itself) when the vote was cast. Zero means the voter is still initializing or was stopped.
	 * @param reachableCandidatesOfNewConf The number of candidates new the new set or participants that were reachable by the voter (including the voter itself) when the vote was cast. Zero means the voter is still initializing or was stopped.
	 * @param behaviorOrdinal The behavior of the voted candidate.
	 */
	case class Vote[Id <: AnyRef](term: Term, candidateId: Id, reachableCandidatesOfOldConf: Int, reachableCandidatesOfNewConf: Int, behaviorOrdinal: BehaviorOrdinal)

	/** The result of an append operation.
	 * @see [[ConsensusParticipantSdm.ClusterParticipant.appendRecords]] and [[ConsensusParticipantSdm.ClusterParticipant.MessagesListener.onAppendRecords]].
	 * @param term The term of the follower that is responding.
	 * @param success False if either:
	 * - the participant that is responding is not ready (Starting or Stopped);
	 * - the leader's term is less than the current term of the follower;
	 * - or the follower's log does not contain a record at the `prevRecordIndex` whose term is `prevRecordTerm`.
	 * @param behaviorOrdinal The behavior of the follower that is responding.
	 */
	case class AppendResult(term: Term, success: Boolean, behaviorOrdinal: BehaviorOrdinal)

	/**
	 * Knows all the information about a candidate necessary by participants to decide their own role and which to vote in a leader election.
	 * The response to a "how are you" question from a participant to another.
	 * @param currentTerm The term of the participant that is answering.
	 * @param ordinal The behavior of the participant that is answering.
	 * @param termAtCommitIndex The term of the last commited record in the log of the participant that is answering.
	 * @param commitIndex The index of the last commited record in the log of the participant that is answering.
	 */
	case class StateInfo(currentTerm: Term, ordinal: BehaviorOrdinal, termAtCommitIndex: Term, commitIndex: RecordIndex) {
		assert(currentTerm >= termAtCommitIndex)
	}

	//// LOG RECORD

	sealed trait Record {
		def term: Term
	}

	private[consensus] case class CommandRecord[C <: AnyRef](override val term: Term, command: C) extends Record

	private[consensus] case class LeaderTransition(override val term: Term) extends Record

	private[consensus] case class SnapshotPoint(override val term: Term) extends Record

	sealed trait ConfigurationChange extends Record

	private[consensus] case class ConfigurationChangeOldNew[P <: AnyRef](override val term: Term, requestId: ConfigChangeRequestId, oldParticipants: Set[P], newParticipants: Set[P]) extends ConfigurationChange

	private[consensus] case class ConfigurationChangeNew[P <: AnyRef](override val term: Term, requestId: ConfigChangeRequestId, newParticipants: Set[P]) extends ConfigurationChange

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

	/** The type of the state-machine's [[StateMachine.appliesClientCommand]] method's responses. */
	type StateMachineResponse

	/** The type of [[Workspace]] implementation. */
	type WS <: Workspace

	//// CONFIGURATION

	/** Tells how many times the task returned by [[appliesClientCommand]] method should be tried when its execution fails.
	 * After exceeding the specified number of retries, the state machine is considered corrupted, and therefore the corresponding [[ConsensusParticipant]] service is restarted with a flag indicating the [[StateMachine]] should be restarted also. */
	def applyCommandRetries: Int = 1

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
		/** Returns a [[sequencer.Task]] that applies the given [[ClientCommand]] to the state machine.
		 * The implementation may assume that tasks returned by successive calls to this method are executed in the same order as the calls themselves.
		 */
		def appliesClientCommand(index: RecordIndex, command: ClientCommand): sequencer.Task[StateMachineResponse]

		/** Returns a [[sequencer.Task]] that yields the [[RecordIndex]] most recently passed to [[appliesClientCommand]] whose corresponding [[sequencer.Task]] completed successfully.
		 * If the implementation cannot determine this index, it should return zero, indicating that all commands must be replayed.
		 *
		 * This method is invoked only during recovery after restarts or persistence failures.
		 */
		def recoversIndexOfLastAppliedCommand: sequencer.Task[RecordIndex]
	}

	//// RESPONSE TO CLIENT

	/** The response to a client command.
	 * @see [[ClusterParticipant.MessagesListener.onCommandFromClient]]. */
	sealed trait ResponseToClient

	case class Processed(recordIndex: RecordIndex, content: StateMachineResponse) extends ResponseToClient

	case class RedirectTo(participantId: ParticipantId) extends ResponseToClient

	/** Tells that the [[Workspace]] of the [[ConsensusParticipant]] service is inconsistent. Should never happen. TODO consider restarting the service in this situation, instead. */
	case class InconsistentState(detail: String) extends ResponseToClient

	/** The participant is unable to process the command because it is isolated, starting, or stopped.
	 * After receiving this response, the client should try again with any of the [[otherParticipants]] with a flag indicating it is a fallback. 
	 * @param behaviorOrdinal The behavior of the participant that is not able to reach consensus.
	 * @param otherParticipants The ids of the other participants to try with. This field is not necessary because the cluster already knows them, but it is included for convenience.
	 */
	case class Unable(behaviorOrdinal: BehaviorOrdinal, otherParticipants: IndexedSeq[ParticipantId]) extends ResponseToClient

	//// CLUSTER

	/** Specifies what a [[ConsensusParticipant]] service requires from the cluster-participant-service it is bound to.
	 *
	 * A [[ClusterParticipant]] represents a single participant within a specific cluster and provides the identity,
	 * membership, and communication mechanisms required by the bound [[ConsensusParticipant]] service.
	 *
	 * Responsibilities of a [[ClusterParticipant]] include:
	 *   - Exposing the identity of the participant it services via [[boundParticipantId]].
	 *   - Providing the initial cluster membership via [[getInitialParticipants]], which must return the same set across all participants listed.
	 *   - Acting as the source of truth for cluster membership and determining when a configuration change should be triggered.
	 *     This includes reacting to node join/leave events, quorum loss, scaling decisions, or health-based adjustments.
	 *   - Initiating configuration transitions by calling a method on the bound [[ConsensusParticipant]] when a change is required.
	 *   - Routing inter-participant RPCs (e.g., [[howAreYou]], [[chooseALeader]], [[appendRecords]]) to the appropriate [[MessageListener]] methods.
	 *   - Delivering client commands and consensus messages to the bound [[ConsensusParticipant]] via a registered [[MessageListener]],
	 *     ensuring all invocations occur within the [[sequencer]] thread.
	 *
	 * Each [[ClusterParticipant]] instance is tightly bound to a single [[ConsensusParticipant]] instance and must not be shared across multiple participants.
	 */
	trait ClusterParticipant {

		/** The identifier of the participant that this [[ClusterParticipant]] service — and its bound [[ConsensusParticipant]] service — are responsible for. */
		def boundParticipantId: ParticipantId

		/** Returns the identifiers of the participants in the initial cluster configuration.
		 * This method is called by the bounded [[ConsensusParticipant]] when started for the first time ([[Workspace.isBrandNew]] returns true); and must return exactly the same set across all participants listed.
		 * The returned set must include the identifier of the participant serviced by this [[ClusterParticipant]] instance.
		 */
		def getInitialParticipants: Set[ParticipantId]

		/** Called by the bounded [[ConsensusParticipant]] to notify that its configuration has changed and now expects connectivity with the given set of participants.
		 *
		 * This method is invoked upon application of a [[ConfigurationChange]]-typed [[Record]].
		 * A call to [[MessagesListener.onConfigurationChangeRequest]] causes two [[ConfigurationChange]] records to be applied and, therefore, two calls tho this method per involved [[ConsensusParticipant]] service.   
		 * Successive calls with the same argument may occur. Implementations may ignore such calls only if no intervening call with a different argument has occurred — i.e., if the configuration has not changed.
		 * @param includedParticipants The set of participants that the consensus layer expects to be reachable.
		 */
		def onConfigurationChanged(requestId: ConfigChangeRequestId, includedParticipants: Set[ParticipantId]): Unit

		/** Defines the listener that a [[ConsensusParticipant]] creates and registers with its bound [[ClusterParticipant]].
		 *
		 * The [[ClusterParticipant]] invokes the methods of this listener to deliver client commands and consensus messages.
		 * All invocations must occur within the [[sequencer]] thread to preserve consistency and serialization guarantees.
		 *
		 * This listener is unidirectional and tightly bound to a single [[ConsensusParticipant]] instance.
		 */
		trait MessagesListener {

			/** Handles a client-submitted command intended for the state machine.
			 *
			 * This method is invoked by the bound [[ClusterParticipant]] when a client sends a command to this participant.
			 * It must be called within the [[sequencer]].
			 * TODO Analyze the alternative of returning a Duty that yields a variant of [[Unable]] that details the failure when something fails. 
			 *
			 * @param command The command issued by the client for state-machine execution.
			 * @param attemptFlag Indicates the outcome of the client's previous attempt to send this command.
			 * @return A [[sequencer.Task]] that yields one of:
			 *         - The state-machine's response to the client.
			 *         - A [[RedirectTo]] message instructing the client to contact the current leader.
			 *         - An [[Unable]] message indicating that this participant cannot currently reach consensus.
			 */
			def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.Task[ResponseToClient]

			/**
			 * This method is invoked by this cluster when another participant calls [[howAreYou]].
			 *
			 * Must be called within the [[sequencer]].
			 * @param inquirerId The id of the participant that called [[howAreYou]].
			 * @param inquirerTerm The term of the participant that called [[howAreYou]].
			 * @return The state information of the destination participant.
			 */
			def onHowAreYou(inquirerId: ParticipantId, inquirerTerm: Term): StateInfo

			/**
			 * This method is invoked by this cluster when another participant calls [[chooseALeader]].
			 *
			 * Must be called within the [[sequencer]].
			 * @param inquirerId The id of the participant that called [[chooseALeader]].
			 * @param inquirerInfo Information about the state of the participant that called.
			 * @return A [[sequencer.Task]] that yields a [[Vote]] indicating the candidate chosen by the listening participant for the specified term.
			 */
			def onChooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.Duty[Vote[ParticipantId]]

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
			def onAppendRecords(inquirerId: ParticipantId, inquirerTerm: Term, prevRecordIndex: RecordIndex, prevRecordTerm: Term, records: GenIndexedSeq[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.Task[AppendResult]

			/** This [[ClusterParticipant]] should call this method whenever the set of consensus-participants has forcefully changed (i.e: a cluster-member included in the current consensus-participants-set went down) or is about to change (i.e: a node intended to be part of consensus-participants-set joined the cluster, or is going to leave the cluster for maintenance).
			 * To improve availability during planned cluster-membership transitions, the manager of the planed change should do the following before proceeding with the change:
			 *		1 call this method on every consensus-participant service,
			 *		2 wait until either:
			 *			- the returned task yields true for any of the consensus-participants,
			 *			- or the [[onConfigurationChanged]] is called in any of the consensus-participants.
			 *
			 * @param requestId an identifier chosen by the caller that will be propagated up to the invocations of the [[onConfigurationChanged]] method of each of the [[ClusterParticipant]] instances bounded to the involved [[ConsensusParticipant]] services.  	
			 * @param newParticipants the identifiers of the participants that are going to seek consensus from now on.
			 * @return a [[sequencer.Task]] that yields true/false if the [[ConsensusParticipant]] is/isn't either:
			 *         - currently the leader or a follower that already has the given configuration;
			 *         - or currently the leader and was able to commit the joint configuration change. */
			def onConfigurationChangeRequest(requestId: ConfigChangeRequestId, newParticipants: Set[ParticipantId]): sequencer.Task[Boolean]
		}

		/**
		 * Sets the message listener.
		 * Is called within the [[sequencer]] thread. */
		def setMessagesListener(listener: MessagesListener | Null): Unit

		extension (destinationId: ParticipantId) {
			/**
			 * Asks the destination participant how it is doing.
			 * The implementation should make, somehow, the destination participant's [[MessagesListener.onHowAreYou]] to be called, and return what it returns.
			 * Called within the [[sequencer]] thread.
			 * @param inquirerTerm The term of the participant that is asking.
			 * @return A [[sequencer.Task]] that yields the state information of the destination participant.
			 */
			def howAreYou(inquirerTerm: Term): sequencer.Task[StateInfo]

			/**
			 * Request the destination participant to choose a leader.
			 * The implementation should make, somehow, the destination participant's [[MessagesListener.onChooseALeader]] to be called, and return what it returns.
			 * Called within the [[sequencer]] thread.
			 * @param inquirerId The id of the participant that is asking.
			 * @param inquirerInfo Information about the state of the participant that is asking.
			 * @return A [[sequencer.Task]] that yields a [[Vote]] indicating the candidate chosen by the destination participant.
			 */
			def chooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.Task[Vote[ParticipantId]]

			/**
			 * Request the destination participant to append records.
			 * The implementation should make, somehow, the destination participant's [[MessagesListener.onAppendRecords]] to be called with the same parameter values, and return what it returns.
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

	/** Specifies the unit of work that a [[ConsensusParticipant]] requires to manage its persistent state. */
	trait Workspace {

		def isBrandNew: Boolean

		//		/** Record index of the first record in the log buffer. */
		//		private var logBufferOffset: RecordIndex = 1
		//
		//		/** The log buffer.
		//		 * Contains the records since the last snapshot. */
		//		private val logBuffer: mutable.ArrayBuffer[Record] = mutable.ArrayBuffer.empty

		def getLastConfigurationChange: ConfigurationChange

		/** The current term according to this participant.
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
		 * @return The index of the last record appended. */
		def appendResolvingConflicts(records: GenIndexedSeq[Record], from: RecordIndex): RecordIndex

		/** Should be called whenever the [[ConsensusParticipant.lastAppliedCommandIndex]] changes to allow this [[Workspace]] to release the storage used to memorize the records that are pending to be applied to the [[StateMachine]]. */
		def informAppliedCommandIndex(appliedCommandIndex: RecordIndex): Unit

		/** Should return the index of the last [[CommandRecord]] in the log whose [[ClientId]] is the provided one, or zero if none. */
		def indexOfLastAppendedCommandFrom(clientId: ClientId): RecordIndex

		def release(): Unit

		inline def getRecordTermAt(index: RecordIndex): Term =
			if index == 0 then 0
			else getRecordAt(index).term
	}

	/** Defines what a [[ConsensusParticipant]] requires from a persistence service to load and save its [[Workspace]].
	 * Implementations may assume that all methods of this trait are invoked within the [[sequencer]] thread, enabling optimizations such as avoiding unnecessary creation of new task objects. */
	trait Storage {
		val loads: sequencer.Task[WS]

		/** Saves the workspace to the persistence storage.
		 * Design Note: A failure to save the workspace should restart the [[ConsensusParticipant]] as if it had crashed and lost all non-persistent variables.
		 */
		def saves(workspace: WS): sequencer.Task[Unit]
	}

	//// NOTIFICATIONS


	trait NotificationListener {
		def onStarting(previous: BehaviorOrdinal, term: Term, isRestart: Boolean): Unit

		def onStarted(previous: BehaviorOrdinal, term: Term, isRestart: Boolean): Unit

		def onBecameStopped(previous: BehaviorOrdinal, term: Term, motive: Throwable | Null): Unit

		def onBecameIsolated(previous: BehaviorOrdinal, term: Term): Unit

		def onBecameCandidate(previous: BehaviorOrdinal, term: Term): Unit

		def onBecameFollower(previous: BehaviorOrdinal, term: Term, leaderId: ParticipantId): Unit

		def onBecameLeader(previous: BehaviorOrdinal, term: Term): Unit

		def onLeft(left: BehaviorOrdinal, term: Term): Unit

		def onCommitIndexChange(previous: RecordIndex, current: RecordIndex): Unit

		def onConfigurationChangeApplied(currentBehavior: BehaviorOrdinal, currentTerm: Term, cc: ConfigurationChange): Unit
	}

	/**
	 * A convenience [[NotificationListener]] implementation with no-op methods.
	 * Extend this class and override only the methods you need.
	 */
	open class DefaultNotificationListener extends NotificationListener {
		override def onStarting(previous: BehaviorOrdinal, term: Term, isRestart: Boolean): Unit = ()

		override def onStarted(previous: BehaviorOrdinal, term: Term, isRestart: Boolean): Unit = ()

		override def onBecameStopped(previous: BehaviorOrdinal, term: Term, motive: Throwable | Null): Unit = ()

		override def onBecameIsolated(previous: BehaviorOrdinal, term: Term): Unit = ()

		override def onBecameCandidate(previous: BehaviorOrdinal, term: Term): Unit = ()

		override def onBecameFollower(previous: BehaviorOrdinal, term: Term, leaderId: ParticipantId): Unit = ()

		override def onBecameLeader(previous: BehaviorOrdinal, term: Term): Unit = ()

		override def onLeft(left: BehaviorOrdinal, term: Term): Unit = ()

		override def onCommitIndexChange(previous: RecordIndex, current: RecordIndex): Unit = ()

		override def onConfigurationChangeApplied(currentBehavior: BehaviorOrdinal, currentTerm: Term, cc: ConfigurationChange): Unit = ()
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
	 * - Each participant operates in one of several behavioral states: starting, isolated, candidate, follower, leader, or stopped.
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
	 * - Leader selection criteria (in order of priority): highest last record term, longest log, highest current term,
	 *   highest behavior ordinal, and lexicographically the smallest participant ID.
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
	class ConsensusParticipant(cluster: ClusterParticipant, storage: Storage, machine: StateMachine, initialListeners: Iterable[NotificationListener], stateMachineNeedsRestart: Boolean) { thisConsensusParticipant =>

		import cluster.*

		val module: thisModule.type = thisModule

		private var currentConfig: Configuration = NoConfig

		/** The current term according to this participant.
		 * The Initial value is zero.
		 * Zero means "before the first election".
		 * Should be reflected in the [[Workspace]].
		 * */
		private var currentTerm: Term = 0

		/** The index of the highest entry known to be committed according to this participant.
		 * A log record is committed once the leader that created the record has replicated it on a majority of the participants.
		 * This also commits all preceding records in the leader’s log, including records created by previous leaders. */
		private var commitIndex: RecordIndex = 0

		/** The index of the highest command entry whose command was successfully applied to the [[StateMachine]] of this [[ConsensusParticipant]].
		 * Whenever it changes should be informed to the [[Workspace]] by calling [[Workspace.informAppliedCommandIndex]]. */
		private var lastAppliedCommandIndex: RecordIndex = 0

		private var workspace: WS | Null = null

		/** The current behavior of this participant. */
		private var currentBehavior: Behavior = Starting(false, stateMachineNeedsRestart)

		private var startingCompletedCovenant: sequencer.Covenant[Boolean] = sequencer.Covenant().fulfillHere(false)()

		/** Memorices the schedule that is currently scheduled. Needed to cancel the schedule when becoming a new behavior. */
		private var currentSchedule: Maybe[sequencer.Schedule] = Maybe.empty

		private var decoupledCommandsApplierIsRunning: Boolean = false

		private val notificationListeners: java.util.WeakHashMap[NotificationListener, None.type] = new util.WeakHashMap()

		private val howAreYouRequestOnTheWayByParticipant: mutable.Map[ParticipantId, sequencer.Commitment[StateInfo]] = mutable.Map.empty

		{
			initialListeners.foreach(notificationListeners.put(_, None))
			currentBehavior.onEnter(STOPPED)
		}

		private object participantIdComparator extends Comparator[ParticipantId] {
			private val ordering = summon[Ordering[ParticipantId]]

			override def compare(a: ParticipantId, b: ParticipantId): Int = ordering.compare(a, b)
		}

		/** The current set of participants in the cluster, according to this participant, sorted.
		 * Should be reflected in the [[Workspace]].
		 * */
		inline def currentParticipants: IArray[ParticipantId] = currentConfig.participants

		/** Should be updated whenever [[currentParticipants]] mutates */
		inline def otherParticipants: IndexedSeq[ParticipantId] = currentConfig.otherParticipants

		/** @return the ordinal of the current behavior. */
		inline def getBehaviorOrdinal: BehaviorOrdinal = currentBehavior.ordinal

		/** @return the current term. */
		inline def getTerm: Term = currentTerm

		/** Stops this [[ConsensusParticipant]] instance.
		 * Should be called within the [[sequencer]]. */
		def stop(): Unit = {
			assert(isInSequence)
			if currentBehavior.ordinal != STOPPED then become(Stopped(null))
		}

		/** @return the index of the specified [[ParticipantId]] in the [[currentParticipants]]' [[IndexedSeq]].
		 * @param participantId the id of the participant to find. */
		private inline def participantIndexOf(participantId: ParticipantId): Int = {
			java.util.Arrays.binarySearch(currentParticipants.asInstanceOf[Array[ParticipantId]], participantId, participantIdComparator)
		}

		/**
		 * Transitions the participant to a new behavior.
		 */
		private def become(newBehavior: Behavior): Behavior = {
			assert(isInSequence)
			if newBehavior != currentBehavior then {
				currentBehavior.onLeave()
				currentSchedule.foreach(sequencer.cancel(_))
				currentSchedule = Maybe.empty
				val previousBehaviorOrdinal = currentBehavior.ordinal
				notifyListeners(_.onLeft(previousBehaviorOrdinal, currentTerm))
				currentBehavior = newBehavior
				cluster.setMessagesListener(newBehavior)
				newBehavior.onEnter(previousBehaviorOrdinal)
			}
			currentBehavior
		}


		private def buildMyStateInfo: StateInfo = {
			val currentBehaviorOrdinal = currentBehavior.ordinal
			if currentBehaviorOrdinal <= STARTING then StateInfo(0, currentBehaviorOrdinal, 0, 0)
			else {
				val termAtCommitIndex = workspace.getRecordTermAt(commitIndex)
				StateInfo(currentTerm, currentBehaviorOrdinal, termAtCommitIndex, commitIndex)
			}
		}

		private inline def updateState(): sequencer.Duty[Unit] =
			updateState(null, null)


		private def updateState(inquirerId: ParticipantId | Null, inquirerInfo: StateInfo): sequencer.Duty[Unit] = {
			assert(sequencer.isInSequence)
			for {
				_ <- startingCompletedCovenant
				_ <- {
					if currentBehavior.ordinal == STOPPED then sequencer.Duty_unit
					else for {
						myVote <- currentConfig.decideMyVote(inquirerId, inquirerInfo)
						_ <- updateStateKnowingMyVote(myVote)
					} yield ()
				}
			} yield ()
		}

		private def updateStateKnowingMyVote(myVote: Vote[ParticipantId]): sequencer.Duty[Unit] = {
			assert(isInSequence)
			for {
				_ <- startingCompletedCovenant
				_ <- {
					if currentBehavior.ordinal == STOPPED then sequencer.Duty_unit
					else {
						if myVote.term > currentTerm then {
							assert(isInSequence) // TODO delete line
							currentTerm = myVote.term
						}

						if currentConfig.reachedAll(myVote) then {
							if myVote.candidateId == boundParticipantId then become(Leader())
							else become(Follower(myVote.candidateId))
							sequencer.Duty_unit
						} else if currentConfig.reachedAMajority(myVote) then {
							val myStateInfo = buildMyStateInfo
							val inquires = for replierId <- otherParticipants yield replierId.chooseALeader(boundParticipantId, myStateInfo)
							for {
								replies <- sequencer.Duty_sequenceTasksToArray(inquires)
							} yield {
								currentConfig.updateStateKnowingAllVotes(myVote, replies)
								()
							}
						} else {
							become(Isolated)
							sequencer.Duty_unit
						}
					}
				}
			} yield ()
		}

		/** Updates the state and [[Behavior]] of this [[ConsensusParticipant]] and then returns the [[sequencer.Task]] returned by the [[Behavior.onCommandFromClient]] method applied to the updated [[Behavior]].
		 * @return a [[sequencer.Task]] returned by [[Behavior.onCommandFromClient]] applied to the updated [[Behavior]] */
		private def updatesStateAndThenCallsOnCommandFromClient(command: ClientCommand): sequencer.Task[ResponseToClient] = {
			assert(isInSequence)
			for {
				_ <- updateState().toTask
				result <- {
					val currentBehaviorOrdinal = currentBehavior.ordinal
					if currentBehaviorOrdinal >= FOLLOWER then currentBehavior.onCommandFromClient(command, FIRST_ATTEMPT)
					else sequencer.Task_successful(Unable(currentBehaviorOrdinal, otherParticipants))
				}
			} yield result
		}


		private def saveWorkspace(): sequencer.Task[Unit] = {
			workspace.setCurrentTerm(currentTerm)
			storage.saves(workspace.asInstanceOf[WS]).transform {
				case su: Success[Unit] =>
					su

				case failure: Failure[Unit] =>
					scribe.error(s"$boundParticipantId: Unexpected error while saving the workspace. This participant's consensus service is unable to continue following the leader and will stop.", failure.exception)
					become(Stopped(failure.exception))
					failure
			}
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
		 * Abstract base class for consensus behavior states.
		 *
		 * Each behavior represents a different role in the consensus algorithm and implements
		 * the message handling logic specific to that role. Behaviors can transition to other
		 * behaviors based on received messages and internal logic.
		 *
		 * All behavior methods are executed within the sequencer thread to ensure thread safety.
		 */
		private sealed abstract class Behavior extends MessagesListener, Product { thisBehavior =>
			/** The ordinal corresponding to this [[Behavior]] */
			val ordinal: BehaviorOrdinal

			def onEnter(previous: BehaviorOrdinal): Unit

			/** Called by [[become]] before transitioning to another behavior. */
			def onLeave(): Unit = ()


			override def onHowAreYou(inquirerId: ParticipantId, inquirerTerm: Term): StateInfo = {
				assert(isInSequence)
				if inquirerTerm > currentTerm && ordinal >= ISOLATED then {
					assert(isInSequence) // TODO delete line
					currentTerm = inquirerTerm
					// Enqueue the execution of the update-process. Note that it will be executed later and therefore will not affect the returned [[StateInfo]] (which may be outdated).
					updateState().triggerAndForget(true)
				}
				buildMyStateInfo
			}

			override def onChooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.Duty[Vote[ParticipantId]] = {
				assert(isInSequence)
				if ordinal <= STARTING then sequencer.Duty_ready(Vote(0, boundParticipantId, 0, 0, ordinal))
				else currentConfig.decideMyVote(inquirerId, inquirerInfo)
					.andThen { vote =>
						if (vote.term > currentTerm && currentBehavior.ordinal >= ISOLATED) || (currentBehavior.ordinal >= CANDIDATE && !currentConfig.reachedAMajority(vote)) then {
							// start the state-updater process
							updateStateKnowingMyVote(vote).triggerAndForget(true)
						}
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
			 *   - If the leader's term is newer or this participant is not yet a follower:
			 *     - Updates `currentTerm` to `inquirerTerm`.
			 *     - Triggers the state-update process via `updatesState.triggerExposingFailures()`.
			 *
			 *   - Appends new records from the leader, resolving any log conflicts.
			 *   - Updates the [[commitIndex]] as the minimum of `leaderCommit` and the index of the last appended record.
			 *
			 *   - If this participant state haven't changed to a no receptive one (`ordinal <= STARTING`) while waiting the application of commited [[Record]]s of the kind that update this participant consensus state (like [[ConfigurationChangeOldNew]] and [[ConfigurationChangeOldNew]]), then :
			 *     - Persists the updated workspace via `storage.saves`.
			 *     - On failure to persist, transitions to `Stopped` and returns a failed result.
			 *
			 *   - Starts the decoupled process that applies committed commands to the state machine in log order:
			 *     - Iteratively applies records from `commitIndex + 1` to `newCommitIndex`.
			 *     - Handles [[CommandRecord]] entries by invoking [[StateMachine.appliesClientCommand]].
			 *     - On failure, retries up to [[applyCommandRetries]] times before restarting the [[ConsensusParticipant]] and its [[StateMachine]] (by calling {{{ become(Starting(true, true)) }}}).
			 *     - Other record types (`LeaderTransition`, `SnapshotPoint`, `ConfigurationChange`) are currently unimplemented.
			 *
			 * @param inquirerId         ID of the leader sending the AppendEntries request
			 * @param inquirerTerm       Term of the leader
			 * @param prevRecordIndex    Index of the record preceding the new entries
			 * @param prevRecordTerm     Term of the preceding record
			 * @param records            New records to append
			 * @param leaderCommit       Commit index reported by the leader
			 * @return a [[sequencer.Task]] yielding [[AppendResult]]:
			 *         - If all the records were appended and commands corresponding to commited [[Record]]s successfully applied: contains the current term, success flag, and behavior ordinal;
			 *         - If rejected: contains the current term (or 0), failure flag, and behavior ordinal
			 *         - If a command application failed or the participant failed to persist its [[Workspace]]: contains the exception.
			 */
			override def onAppendRecords(inquirerId: ParticipantId, inquirerTerm: Term, prevRecordIndex: RecordIndex, prevRecordTerm: Term, records: GenIndexedSeq[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.Task[AppendResult] = {
				assert(isInSequence)
				if ordinal < ISOLATED then sequencer.Task_successful(AppendResult(0, false, ordinal))
				else if inquirerTerm < currentTerm then sequencer.Task_successful(AppendResult(currentTerm, false, ordinal))
				else if prevRecordIndex >= workspace.firstEmptyRecordIndex || workspace.getRecordTermAt(prevRecordIndex) != prevRecordTerm then sequencer.Task_successful(AppendResult(currentTerm, false, ordinal))
				else {
					if inquirerTerm > currentTerm || ordinal <= CANDIDATE then {
						currentTerm = inquirerTerm
						val inquirerInfo = StateInfo(inquirerTerm, LEADER, termAtLeaderCommit, leaderCommit)
						updateState(inquirerId, inquirerInfo).triggerAndForget(true) // TODO: analyze what happens if this updateState causes to become the leader, preferably with a test.
					}

					assert(inquirerTerm == currentTerm, s"$inquirerTerm == $currentTerm")
					// Append any new entry not already in the log resolving conflicts.
					val lastAppendedRecordIndex = workspace.appendResolvingConflicts(records, prevRecordIndex + 1)

					// Apply configuration changes if any.					
					applyLastConfigChangeLocallyIfAny(records)

					// Update the commitIndex
					val previousCommitIndex = commitIndex
					commitIndex = if leaderCommit <= lastAppendedRecordIndex then leaderCommit else lastAppendedRecordIndex
					if commitIndex > previousCommitIndex then {
						if !decoupledCommandsApplierIsRunning then startApplyingCommitedCommands()
						notifyListeners(_.onCommitIndexChange(previousCommitIndex, commitIndex))
					}

					// Save the persisten state and respond accordingly.
					saveWorkspace().transform {
						case _: Success[Unit] => Success(AppendResult(currentTerm, true, currentBehavior.ordinal))
						case _: Failure[Unit] => Success(AppendResult(0, false, STOPPED))
					}
				}
			}

			/** Finds the last [[ConfigurationChange]]-typed [[Record]] in the given sequence and, if some is found, applies it locally.
			 *
			 * Scans the records in reverse order and locally applies the first encountered configuration change.
			 */
			private def applyLastConfigChangeLocallyIfAny(records: GenIndexedSeq[Record]): Unit = {
				var index = records.size
				while index > 0 do {
					index -= 1
					records(index) match {
						case cc: ConfigurationChange =>
							applyConfigChangeLocally(cc)
							return
						case _ => // do nothing	
					}
				}
			}

			protected def applyConfigChangeLocally(configChange: ConfigurationChange): Unit = {
				configChange match {
					case cc: ConfigurationChangeOldNew[ParticipantId] @unchecked =>
						currentConfig = JointConf(IArray.unsafeFromArray(cc.oldParticipants.toArray.sorted), cc.newParticipants)
						cluster.onConfigurationChanged(cc.requestId, cc.oldParticipants.union(cc.newParticipants))
					case cc: ConfigurationChangeNew[ParticipantId] @unchecked =>
						currentConfig = RegularConf(IArray.unsafeFromArray(cc.newParticipants.toArray.sorted))
						cluster.onConfigurationChanged(cc.requestId, cc.newParticipants)
				}
				notifyListeners(_.onConfigurationChangeApplied(currentBehavior.ordinal, currentTerm, configChange))
			}

			/** Applies commited [[CommandRecord]]s decoupled. */
			private def startApplyingCommitedCommands(): Unit = {
				decoupledCommandsApplierIsRunning = true

				def applyCommitedCommandsLoop(index: RecordIndex): sequencer.Task[Any] = {
					if index > commitIndex then sequencer.Task_unit
					else {
						workspace.getRecordAt(index) match {
							case command: CommandRecord[ClientCommand] @unchecked =>
								val appliesCommand = machine.appliesClientCommand(index, command.command)
								var retriesCounter = 0
								appliesCommand.transformWith {
									case Success(_) =>
										lastAppliedCommandIndex = index
										workspace.informAppliedCommandIndex(index)
										applyCommitedCommandsLoop(index + 1)
									case Failure(e) =>
										if retriesCounter < applyCommandRetries then {
											retriesCounter += 1
											appliesCommand
										} else {
											scribe.error(s"$boundParticipantId: Unexpected error while applying commited commands to the state machine. This participant's state-machine must be restarted and commands replayed.", e)
											become(Starting(true, true))
											sequencer.Task_failed(e)
										}
								}
							case _ =>
								sequencer.Task_unit
						}
					}
				}

				val appliesCommitedCommands =
					if lastAppliedCommandIndex > 0 then applyCommitedCommandsLoop(lastAppliedCommandIndex + 1)
					else {
						for {
							index <- machine.recoversIndexOfLastAppliedCommand
							_ <- {
								lastAppliedCommandIndex = index
								workspace.informAppliedCommandIndex(index)
								if index == 0 then sequencer.Task_unit
								else applyCommitedCommandsLoop(index + 1)
							}
						} yield ()
					}
				appliesCommitedCommands.trigger(true) { _ => decoupledCommandsApplierIsRunning = false }
			}
		}

		private case class Stopped(motive: Throwable | Null) extends Behavior {
			override val ordinal: BehaviorOrdinal = STOPPED

			override def onEnter(previous: BehaviorOrdinal): Unit = {
				notifyListeners(_.onBecameStopped(previous, currentTerm, motive))
				// cluster.setMessagesListener(null) // commented because it complicates a bit the ClusterParticipant implementation.
				workspace.release()
				// workspace = null // commented after commenting setMessagesListener(null), because, otherwise, all usages of `workspace` in the MessageListener methods would require null check.  Uncomment this only if the `setMessagesLister(null)` line is also uncommented.
			}

			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.Task[ResponseToClient] = {
				assert(isInSequence)
				sequencer.Task_successful(Unable(ordinal, otherParticipants))
			}

			override def onConfigurationChangeRequest(requestId: ConfigChangeRequestId, newParticipants: Set[ParticipantId]): sequencer.Task[Boolean] =
				sequencer.Task_false

		}

		/** The initial state where all the common state variables are initialized.
		 * This state is transitory. When initialization is completed it transitions to the [[Isolated]] state. */
		private case class Starting(isRestart: Boolean, stateMachineNeedsRestart: Boolean) extends Behavior {
			override val ordinal: BehaviorOrdinal = STARTING

			override def onEnter(previous: BehaviorOrdinal): Unit = {
				if workspace ne null then workspace.release()
				workspace = null

				val previosStartingCompletedCovenant = startingCompletedCovenant
				startingCompletedCovenant = sequencer.Covenant()
				previosStartingCompletedCovenant.fulfillWith(startingCompletedCovenant, true)
				
				notifyListeners(_.onStarting(previous, currentTerm, isRestart))
				storage.loads.trigger(true) {
					case Success(loadedWorkspace) =>
						// TODO consider the stateMachineNeedsRestart flag
						if loadedWorkspace.isBrandNew then {
							loadedWorkspace.setCurrentTerm(0)
							loadedWorkspace.appendRecord(new ConfigurationChangeNew[ParticipantId](0, "Initial-Config", cluster.getInitialParticipants))
						}
						workspace = loadedWorkspace
						assert(isInSequence) // TODO delete line
						currentTerm = loadedWorkspace.getCurrentTerm
						currentConfig = loadedWorkspace.getLastConfigurationChange match {
							case cc: ConfigurationChangeNew[ParticipantId] @unchecked =>
								RegularConf(IArray.unsafeFromArray(cc.newParticipants.toArray.sorted))

							case cc: ConfigurationChangeOldNew[ParticipantId] @unchecked =>
								JointConf(IArray.unsafeFromArray(cc.oldParticipants.toArray.sorted), cc.newParticipants)
						}

						val previousCommitIndex = commitIndex
						commitIndex = 0
						if previousCommitIndex != 0 then notifyListeners(_.onCommitIndexChange(previousCommitIndex, 0))

						if stateMachineNeedsRestart then lastAppliedCommandIndex = 0
						loadedWorkspace.informAppliedCommandIndex(0)

						notifyListeners(_.onStarted(previous, currentTerm, isRestart))
						become(Isolated)
						startingCompletedCovenant.fulfillHere(true)()

					case Failure(e) =>
						scribe.error(s"$boundParticipantId: Unexpected error while loading the consensus-service's workspace:", e)
						become(Stopped(e))
						startingCompletedCovenant.fulfillHere(false)()
				}
			}

			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.Task[ResponseToClient] = {
				assert(isInSequence)
				sequencer.Task_successful(Unable(ordinal, otherParticipants))
			}

			override def onConfigurationChangeRequest(requestId: ConfigChangeRequestId, newParticipants: Set[ParticipantId]): sequencer.Task[Boolean] = {
				assert(isInSequence)
				for {
					_ <- startingCompletedCovenant.toTask
					isAble <- currentBehavior.onConfigurationChangeRequest(requestId, newParticipants)
				} yield isAble
			}
		}

		/**
		 * Behavior state when reachability to a majority of the participants was not achieved or is still not checked.
		 * The participant transitions to this state after [[Starting]] or when reachability to other participants drops below [[smallestMajority]].
		 * This state is abandoned when a majority of the participants are reachable.
		 * [[Vote]]s cast by participants in this state are ignored.
		 */
		private case object Isolated extends Behavior {
			override val ordinal: BehaviorOrdinal = ISOLATED

			/**
			 * The main loop of the isolated state.
			 * It checks if the current term leader is reachable or the reachable participants including itself are the majority.
			 * If so, it becomes a follower or a candidate respectively.
			 * If not, it stays in the isolated state and checks again after a while.
			 */
			override def onEnter(previous: BehaviorOrdinal): Unit = {
				notifyListeners(_.onBecameIsolated(previous, currentTerm))
				if isEager then {
					val schedule = sequencer.newDelaySchedule(isolatedMainLoopInterval)
					currentSchedule = Maybe.some(schedule)
					// schedule the state-updater process
					updateState().scheduled(schedule).triggerAndForget(true)
				}
			}

			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.Task[ResponseToClient] = {
				updatesStateAndThenCallsOnCommandFromClient(command)
			}

			override def onConfigurationChangeRequest(requestId: ConfigChangeRequestId, newParticipants: Set[ParticipantId]): sequencer.Task[Boolean] = {
				for {
					_ <- updateState().toTask
					isAble <-
						if currentBehavior.ordinal == LEADER then currentBehavior.onConfigurationChangeRequest(requestId, newParticipants)
						else if currentBehavior.ordinal == FOLLOWER && currentConfig.participants.toSet == newParticipants then sequencer.Task_true
						else sequencer.Task_false
				} yield isAble
			}
		}

		/**
		 * Behavior state when reachability to a majority of the participants is achieved but none of them is in [[Leader]] state.
		 *
		 * In this state, the participant participates in leader election and may transition to [[Leader]], [[Follower]], or [[Isolated]] based on his viewpoint of other participants state.
		 * A new term is started when becoming leader.
		 * The [[ConsensusParticipant]] behaves exactly the same in [[Isolated]] and [[Candidate]] states. The only goal of the separation is to allow the user to distinguish if a majority of participants is reachable or not.
		 */
		private case object Candidate extends Behavior {
			override val ordinal: BehaviorOrdinal = CANDIDATE

			override def onEnter(previous: BehaviorOrdinal): Unit = {
				notifyListeners(_.onBecameCandidate(previous, currentTerm))
				if isEager then {
					val schedule = sequencer.newDelaySchedule(candidateMainLoopInterval)
					currentSchedule = Maybe.some(schedule)
					// schedule the state-update process
					updateState().scheduled(schedule).triggerAndForget(true)
				}
			}

			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.Task[ResponseToClient] = {
				updatesStateAndThenCallsOnCommandFromClient(command)
			}


			override def onConfigurationChangeRequest(requestId: ConfigChangeRequestId, newParticipants: Set[ParticipantId]): sequencer.Task[Boolean] =
				Isolated.onConfigurationChangeRequest(requestId, newParticipants)
		}

		/**
		 * Behavior state when the participant follows a known leader.
		 *
		 * In this state, the participant acknowledges the specified leader and may transition to [[Candidate]] if the consensus protocol requires it.
		 *
		 * @param leaderId The ID of the leader this participant is following
		 */
		private case class Follower(leaderId: ParticipantId) extends Behavior {
			override val ordinal: BehaviorOrdinal = FOLLOWER

			override def onEnter(previous: BehaviorOrdinal): Unit = {
				notifyListeners(_.onBecameFollower(previous, currentTerm, leaderId))
			}

			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.Task[ResponseToClient] = {
				assert(isInSequence)
				if attemptFlag == FIRST_ATTEMPT then sequencer.Task_successful(RedirectTo(leaderId))
				else updatesStateAndThenCallsOnCommandFromClient(command)
			}

			override def onConfigurationChangeRequest(requestId: ConfigChangeRequestId, newParticipants: Set[ParticipantId]): sequencer.Task[Boolean] = {
				Isolated.onConfigurationChangeRequest(requestId, newParticipants)
			}
		}

		/**
		 * Behavior state when the participant is the current leader.
		 *
		 * In this state, the participant coordinates consensus decisions and may
		 * transition to [[Candidate]] if the consensus protocol requires it.
		 */
		private case class Leader(previousTerm: Term = currentTerm) extends Behavior {

			/** The index of the next record to send to a participant, indexed by the participant index.
			 * This array is optimistically initialized to the first empty record index of the leader's workspace for all participants,
			 * assuming that each follower's log is already up-to-date with the leader's log. This optimistic initialization
			 * allows the leader to attempt to append new entries immediately, but if a follower's log is actually behind or inconsistent,
			 * the index will be decremented as needed until the logs are aligned.
			 * When a record is successfully replicated to a participant, the index of the next record to send to that participant is incremented.
			 * When a record is not successfully replicated to a participant, the index of the next record to send to that participant is decremented.
			 */
			private val indexOfNextRecordToSend_ByParticipantIndex: Array[RecordIndex] = Array.fill(otherParticipants.size)(workspace.firstEmptyRecordIndex)
			/** The highest record index known to be replicated to a participant, indexed by the participant index.
			 * This array is conservatively initialized to 0 for all participants, assuming that no records are known to be replicated to any follower at the start of the leader's term.
			 * As records are successfully replicated to a participant, the corresponding value is incremented.
			 * This conservative initialization ensures that the leader does not overestimate the replication state of any follower and only advances commitIndex when a true majority is confirmed.
			 */
			private val highestRecordIndexKnowToBeAppended_ByParticipantIndex: Array[RecordIndex] = Array.fill(otherParticipants.size)(0)

			override val ordinal: BehaviorOrdinal = LEADER

			private var failedReplicationsLoopSchedule: Maybe[sequencer.Schedule] = Maybe.empty

			override def onEnter(previous: BehaviorOrdinal): Unit = {
				currentTerm += 1
				notifyListeners(_.onBecameLeader(previous, currentTerm))

				// If the last configuration change record is to a joint one, start the second phase of the configuration change.
				workspace.getLastConfigurationChange match {
					case jointConf: ConfigurationChangeOldNew[ParticipantId] @unchecked =>
						startConfigChangeSecondPhase(jointConf.requestId, jointConf.newParticipants).trigger(true) {
							case Success(isReplicated) =>
								if !isReplicated && currentBehavior.ordinal > ISOLATED then become(Isolated)
							case Failure(e) =>
								if currentBehavior.ordinal > ISOLATED then become(Isolated)
								scribe.info(s"$boundParticipantId: An attempt to complete the second phase of a configuration change started in a previous term (previousTerm: ${jointConf.term}, currentTerm: $currentTerm) when becoming the leader, has failed with:", e)
						}
					case _ => // do nothing	
				}
				if isEager then attemptToUpdateOtherParticipantsLogs().triggerAndForget(true)
			}

			override def onLeave(): Unit = {
				failedReplicationsLoopSchedule.foreach(sequencer.cancel(_))
				failedReplicationsLoopSchedule = Maybe.empty
			}


			override def onConfigurationChangeRequest(requestId: ConfigChangeRequestId, newParticipants: Set[ParticipantId]): sequencer.Task[Boolean] = {
				if newParticipants == currentConfig.participants.toSet then return sequencer.Task_true

				// start the first phase of the configuration change
				val ccOldNew = new ConfigurationChangeOldNew[ParticipantId](currentTerm, requestId, currentParticipants.toSet, newParticipants)
				workspace.appendRecord(ccOldNew)

				applyConfigChangeLocally(ccOldNew)

				for {
					sufficientOthersUpdated <- attemptToUpdateOtherParticipantsLogs().toTask
					_ <- {
						if currentBehavior ne this then sequencer.Task_unit
						else {
							for {
								_ <- saveWorkspace()
								_ <- {
									if sufficientOthersUpdated && (currentBehavior eq this) then startConfigChangeSecondPhase(requestId, newParticipants)
									else sequencer.Task_false
								}
							} yield ()
						}
					}
				} yield sufficientOthersUpdated
			}

			/** Starts the second phase of a configuration change.
			 * @param newParticipants the identifiers of the participants of the new configuration.
			 * @return  a [[sequencer.Task]] that yields true/false if the [[ConfigurationChangeNew]] [[Record]] was/wasn't commited (replicated to a majority of the given participants). */
			private def startConfigChangeSecondPhase(requestId: ConfigChangeRequestId, newParticipants: Set[ParticipantId]): sequencer.Task[Boolean] = {
				val ccNew = new ConfigurationChangeNew[ParticipantId](currentTerm, requestId, newParticipants)
				workspace.appendRecord(ccNew)
				applyConfigChangeLocally(ccNew)

				for {
					sufficientOthersUpdated <- attemptToUpdateOtherParticipantsLogs().toTask
					_ <- saveWorkspace()

				} yield {
					if sufficientOthersUpdated && !newParticipants.contains(boundParticipantId) then become(Stopped(null))
					sufficientOthersUpdated
				}
			}

			private val lastResponseCommitmentByClientId: mutable.Map[ClientId, sequencer.Commitment[ResponseToClient]] = mutable.Map.empty

			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.Task[ResponseToClient] = {
				assert(isInSequence)

				val clientId = clientIdOf(command)
				val indexOfLastAppendedCommandFromClient = workspace.indexOfLastAppendedCommandFrom(clientId)

				// I this is the first command received from the client, proceed normally (append, replicate, apply)
				if indexOfLastAppendedCommandFromClient == 0 then acceptNewCommand(clientId, command)
				// else, check if the command was received before:
				else {
					// @formatter:off
					workspace.getRecordAt(indexOfLastAppendedCommandFromClient) match {
						case CommandRecord[ClientCommand@unchecked](term, lastClientCommand) =>
							val comparison = clientCommandOrdering.compare(command, lastClientCommand)
							// if the command is newer than the last received from the same client, proceed normally (append, replicate, apply).
							if comparison > 0 then acceptNewCommand(clientId, command)
							// If the command was committed — and thus already applied — simply query the state machine for the result it produced during application and return that same result again, assuming the state-machine is idempotent
							else if indexOfLastAppendedCommandFromClient <= commitIndex then for smr <- machine.appliesClientCommand(indexOfLastAppendedCommandFromClient, command) yield Processed(indexOfLastAppendedCommandFromClient, smr)
							// if the command wasn't commited and is the same as the last received from the same client, then:
							else if comparison == 0 then {
								// If the previous call with this command was received during the current term but was not commited, then another call of this method with the same arguments is in progress.
								if term == currentTerm then lastResponseCommitmentByClientId(clientId)
								else ???
							} else ???

						case _ =>
							sequencer.Task_successful(InconsistentState(s"For client $clientId, the last known log-entry index ($indexOfLastAppendedCommandFromClient) does not point to a record of the expected type."))
					}
					// @formatter:on
				}
			}


			/** TODO Analyze the alternative of returning a Duty that yields a variant of [[Unable]] that details the failure when something fails. */
			private def acceptNewCommand(clientId: ClientId, command: ClientCommand): sequencer.Task[ResponseToClient] = {
				// append the command to the log buffer
				val commandRecord = CommandRecord(currentTerm, command)
				val recordIndex = workspace.firstEmptyRecordIndex
				workspace.appendRecord(commandRecord)

				val responseCommitment = sequencer.Commitment[ResponseToClient]()
				lastResponseCommitmentByClientId.update(clientId, responseCommitment)

				val replicatesThenSavesThenApplies = for {
					sufficientOthersUpdated <- attemptToUpdateOtherParticipantsLogs().toTask
					_ <- saveWorkspace()
					rtc <- {
						if sufficientOthersUpdated then {
							for {
								smr <- machine.appliesClientCommand(recordIndex, command)
							} yield {
								lastAppliedCommandIndex = recordIndex
								workspace.informAppliedCommandIndex(recordIndex)
								Processed(recordIndex, smr)
							}
						} else {
							become(Isolated)
							sequencer.Task_successful(Unable(Isolated.ordinal, otherParticipants))
						}
					}: sequencer.Task[ResponseToClient]
				} yield rtc

				sequencer.Commitment_triggerAndWire(replicatesThenSavesThenApplies, true)
			}

			/**
			 * Attempts to append the [[Record]]s that this participant has to the logs of the participants that lack them.
			 * Detailed behavior:
			 *		- If [[Record]]s weren't appended to a another participant, attempt to append them.
			 *			- If successful: update the corresponding entry of [[indexOfNextRecordToSend_ByParticipantIndex]]
			 *			- If AppendEntries fails because of log inconsistency: decrement the corresponding entry of [[indexOfNextRecordToSend_ByParticipantIndex]], and retry.
			 *		- If there exists an N such that N > [[commitIndex]], a majority of the [[highestRecordIndexKnowToBeAppended_ByParticipantIndex]] entries is ≥ N, and log[N].term == [[currentTerm]]: set commitIndex = N
			 *		- If there are laggard participants (a minority whose corresponding entry in [[highestRecordIndexKnowToBeAppended_ByParticipantIndex]] trails the leader's [[commitIndex]]), initiate targeted retries to catch them up.
			 * @return a [[sequencer.Task]] that yields true if, and only if, for all the participants sets of the [[currentConfig]], all the records in this participant's log are successfully appended to at least:
			 *         - half of the other participants of the set, if this participant belongs to the set;
			 *         - a majority of the other participants of the set, if this participant does not belong to the set.
			 * */
			private def attemptToUpdateOtherParticipantsLogs(): sequencer.Duty[Boolean] = {
				// First, abort the failed replications loop if it is running.
				failedReplicationsLoopSchedule.foreach(sequencer.cancel(_))
				failedReplicationsLoopSchedule = Maybe.empty
				// Then, start regular replication to all followers.
				val until = workspace.firstEmptyRecordIndex
				// For every other participants, generate a task to replicate records this participant has and believes the others lack.
				val requestToAppendPendingRecords_byParticipantIndex =
					for otherParticipantIndex <- otherParticipants.indices yield {
						appendsRecordsToParticipant(otherParticipantIndex, until)
					}
				// Execute the tasks in parallel. Note that the tasks, when successful, update the corresponding entry of the `indexOfNextRecordToSend_ByParticipantIndex` and `highestRecordIndexKnowToBeAppended_ByParticipantIndex` arrays.
				for appendResults <- sequencer.Duty_sequenceTasksToArray(requestToAppendPendingRecords_byParticipantIndex) yield {
					// If the behavior hasn't changed while waiting the result, then the leader is still the same, and we can continue.
					if currentBehavior ne this then false
					else {
						// Update the commitIndex if a majority of the followers have replicated the uncommited records.
						// If there exists an N such that N > commitIndex, the highest log-entry index known to be replicated is > N in a majority of the servers, and getRecordAt[N].term == currentTerm: set commitIndex = N
						val previousCommitIndex = commitIndex
						commitIndex = currentConfig.indexOfTheCommitedRecordWithHighestIndex(previousCommitIndex, IArray.unsafeFromArray(highestRecordIndexKnowToBeAppended_ByParticipantIndex))
						if commitIndex > previousCommitIndex then notifyListeners(_.onCommitIndexChange(previousCommitIndex, commitIndex))

						// For those minority of participants whose highestRecordIndexKnowToBeReplicated is less than the commitIndex (because they failed to replicate commited records), retry the append records RPC.
						// This retry is indefinite until the outer method (replicateUncommitedRecords) is called again (as the effect of an external stimulus).
						def retryLaggardParticipants(previousTryResults: Iterable[(previousTryResult: Try[Boolean], participantIndex: Int)]): Unit = {
							if currentBehavior ne this then return
							// Include only the participants for which the append records RPC failed. Those for which the result was either successful or definitively rejected are filtered out.
							val indicesOfLaggardParticipants: List[Int] = previousTryResults.foldLeft(Nil) { (accumulator, elem) =>
								elem.previousTryResult match {
									case Success(wasReplicationSuccessful) =>
										assert(!wasReplicationSuccessful || highestRecordIndexKnowToBeAppended_ByParticipantIndex(elem.participantIndex) >= commitIndex, s"!$wasReplicationSuccessful || ${highestRecordIndexKnowToBeAppended_ByParticipantIndex(elem.participantIndex)} >= $commitIndex")
										accumulator
									case Failure(e) =>
										scribe.debug(s"$boundParticipantId: The replication of the commited records to ${otherParticipants(elem.participantIndex)} failed with:", e)
										elem.participantIndex :: accumulator
								}
							}
							if indicesOfLaggardParticipants.nonEmpty then {
								val schedule = sequencer.newDelaySchedule(failedReplicationsLoopInterval)
								failedReplicationsLoopSchedule = Maybe.some(schedule)
								sequencer.schedule(schedule) { _ =>
									if currentBehavior eq this then {
										val appendTasks = for followerIndex <- indicesOfLaggardParticipants yield appendsRecordsToParticipant(followerIndex, workspace.firstEmptyRecordIndex) // TODO Is `firstEmptyRecordIndex` the right index to retry until? Or should it be the `commitIndex`?
										sequencer.Duty_sequenceTasksToArray(appendTasks)
											.map(appendResults => retryLaggardParticipants(appendResults.zipWithIndex))
											.triggerAndForget(true)
									}
								}
							}
						}

						// Return true if all the records in this participant were appended in enough other participants to achieve quorum.
						if currentConfig.achievesQuorumWhen(appendResults) then {
							// Retry the append on laggard participants but only if the replication succeeded in a majority. TODO: analyze if the retry should be done independently of majority-replication success.
							retryLaggardParticipants(appendResults.zipWithIndex)
							true
						} else false
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
			 *         - a failure if the [[sequencer.Task]] returned by the participant's [[ClusterParticipant.MessagesListener.onAppendRecords]] method completed with failure, or there was a [[ClusterParticipant]] related failure.
			 */
			private def appendsRecordsToParticipant(destinationParticipantIndex: Int, until: RecordIndex): sequencer.Task[Boolean] = {
				assert(sequencer.isInSequence)

				val destinationParticipantId = otherParticipants(destinationParticipantIndex)
				val indexOfNextRecordToSend = indexOfNextRecordToSend_ByParticipantIndex(destinationParticipantIndex)
				assert(until >= indexOfNextRecordToSend)
				val recordsToSend = workspace.getRecordsBetween(indexOfNextRecordToSend, until)
				val previousRecordIndex = indexOfNextRecordToSend - 1
				val previousRecordTerm = if previousRecordIndex == 0 then 0 else workspace.getRecordAt(previousRecordIndex).term
				for {
					appendResult <- destinationParticipantId.appendRecords(currentTerm, previousRecordIndex, previousRecordTerm, recordsToSend, commitIndex, workspace.getRecordTermAt(commitIndex))
					appendWasSuccessful <- {
						// This point is reached only if the destination participant responds normally to the AppendRecords request.
						// If this participant's behavior changed while waiting the destination participant response, ignore the response and yield successfully (to skip/exit the retry laggards loop).
						if currentBehavior ne this then sequencer.Task_false
						// If the term (according to the target) is greater than the current term (according to this participant), then the leader has changed. So:
						else if appendResult.term > currentTerm then {
							// update the current term and start the state-update process
							assert(isInSequence) // TODO delete line
							currentTerm = appendResult.term
							updateState().triggerAndForget(true)
							// Yield a successful result to avoid the retry loop below.
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
							if appendResult.behaviorOrdinal >= ISOLATED && appendResult.term == currentTerm then {
								// if I remember the previous record then try again starting one record before.
								if indexOfNextRecordToSend > workspace.logBufferOffset then {
									indexOfNextRecordToSend_ByParticipantIndex(destinationParticipantIndex) = indexOfNextRecordToSend - 1
									appendsRecordsToParticipant(destinationParticipantIndex, workspace.firstEmptyRecordIndex)
								}
								// else (I don't remember the previous record)
								else {
									// inform about the illegal state. // TODO analyze a better way to inform this problem after implementing the log snapshot mechanism.
									scribe.error(s"$boundParticipantId: Unable to replicate uncommited records to $destinationParticipantId because its log has inconsistencies at records that I already snapshotted (they are at indexes less than my logBufferOffset ($workspace.logBufferOffset)). THIS SHOULD NOT HAPPEN. PLEASE REPORT THIS AS A BUG.")
									// yield a success but without any change to the corresponding entry in indexOfNextRecordToSend_ByParticipantIndex and highestRecordIndexKnowToBeAppended_ByParticipantIndex.
									sequencer.Task_false
								}
							}
							// if the rejection was for another reason, yield false without any change to the corresponding entry in indexOfNextRecordToSend_ByParticipantIndex and highestRecordIndexKnowToBeAppended_ByParticipantIndex.
							else sequencer.Task_false
						}
					}
				} yield appendWasSuccessful
			}
		}

		//// Configuration dependant knowledge and behavior.

		private sealed trait Configuration {
			/** The current set of participants in the cluster, according to this participant, sorted.
			 * Should be reflected in the [[Workspace]].
			 * */
			val participants: IArray[ParticipantId]
			val otherParticipants: IArray[ParticipantId]

			def isMajority(iterator: Iterator[ParticipantId]): Boolean

			def reachedAll(vote: Vote[ParticipantId]): Boolean

			def reachedAMajority(vote: Vote[ParticipantId]): Boolean

			def indexOfTheCommitedRecordWithHighestIndex(from: RecordIndex, highestRecordIndexKnowToBeAppended_ByParticipantIndex: IArray[RecordIndex]): RecordIndex

			def achievesQuorumWhen(appendResults: Array[Try[Boolean]]): Boolean

			def updateStateKnowingAllVotes(myVote: Vote[ParticipantId], votesFromOthers: Array[Try[Vote[ParticipantId]]]): Behavior

			/**
			 * Determines the best leader candidate based on the [[StateInfo]]s of all the participants, including itself.
			 * of the inquirer, the bound, and the other participants.
			 * This method only queries. Does not mutate anything.
			 *
			 * @param inquirerId the id of the participant of which this participant already knows its [[StateInfo]] (because it sent it to this participant along the request this participant is currently responding) or null if this participant does not know the [[StateInfo]] of anyone else. Note that this parameter is not necessarily needed. Its only purpose is to improve efficiency by saving a query of already known information.
			 * @param inquirerInfo the [[StateInfo]] of the inquirer if `inquirerId` is not null. Note that this parameter is not necessarily needed. Its only purpose is to improve efficiency by saving a query of already known information.
			 * @return A task that yields a [[Vote]] with the chosen leader for the current term.
			 */
			def decideMyVote(inquirerId: ParticipantId | Null, inquirerInfo: StateInfo): sequencer.Duty[Vote[ParticipantId]]
		}

		private object NoConfig extends Configuration {
			override val participants: IArray[ParticipantId] =
				IArray(boundParticipantId)

			override val otherParticipants: IArray[ParticipantId] =
				IArray.empty

			override def isMajority(iterator: Iterator[ParticipantId]): Boolean =
				false

			override def reachedAll(vote: Vote[ParticipantId]): Boolean =
				false

			override def reachedAMajority(vote: Vote[ParticipantId]): Boolean =
				false

			override def indexOfTheCommitedRecordWithHighestIndex(from: RecordIndex, highestRecordIndexKnowToBeAppended_ByParticipantIndex: IArray[RecordIndex]): RecordIndex =
				from

			override def achievesQuorumWhen(appendResults: Array[Try[Boolean]]): Boolean =
				false

			override def updateStateKnowingAllVotes(myVote: Vote[ParticipantId], votesFromOthers: Array[Try[Vote[ParticipantId]]]): Behavior =
				Stopped(null)

			override def decideMyVote(inquirerId: ParticipantId | Null, inquirerInfo: StateInfo): sequencer.Duty[Vote[ParticipantId]] =
				sequencer.Duty_ready(Vote(0, boundParticipantId, 0, 0, STOPPED))
		}

		private class RegularConf(override val participants: IArray[ParticipantId]) extends Configuration {

			private val smallestMajority: Int = 1 + participants.length / 2
			override val otherParticipants: IArray[ParticipantId] = participants.filter(_ != boundParticipantId)

			override def isMajority(iterator: Iterator[ParticipantId]): Boolean = {
				iterator.count(p => participants.contains(p)) >= smallestMajority
			}

			override def reachedAll(vote: Vote[ParticipantId]): Boolean = {
				vote.reachableCandidatesOfOldConf == participants.length
			}

			override def reachedAMajority(vote: Vote[ParticipantId]): Boolean = {
				vote.reachableCandidatesOfOldConf >= smallestMajority
			}

			@tailrec
			override final def indexOfTheCommitedRecordWithHighestIndex(from: RecordIndex, highestRecordIndexKnowToBeAppended_ByParticipantIndex: IArray[RecordIndex]): RecordIndex = {
				val n = from + 1
				if n < workspace.firstEmptyRecordIndex && highestRecordIndexKnowToBeAppended_ByParticipantIndex.count(_ >= n) + 1 >= smallestMajority && workspace.getRecordAt(n).term == currentTerm then {
					indexOfTheCommitedRecordWithHighestIndex(n, highestRecordIndexKnowToBeAppended_ByParticipantIndex)
				} else from
			}

			override def achievesQuorumWhen(appendResults: Array[Try[Boolean]]): Boolean = {
				1 + appendResults.count(r => r.isSuccess && r.get) >= smallestMajority
			}

			override def updateStateKnowingAllVotes(myVote: Vote[ParticipantId], votesFromOthers: Array[Try[Vote[ParticipantId]]]): Behavior = {
				var myVoteIsStale = false
				var votesMatchingMyVoteCount = 1 // includes my vote
				for case Success(replierVote) <- votesFromOthers do {
					if replierVote.term > currentTerm then {
						assert(isInSequence) // TODO delete line
						currentTerm = replierVote.term
						workspace.setCurrentTerm(currentTerm)
						myVoteIsStale = true
					}
					if replierVote.candidateId == myVote.candidateId && currentConfig.reachedAMajority(replierVote) then votesMatchingMyVoteCount += 1
				}
				if myVoteIsStale || votesMatchingMyVoteCount < smallestMajority then become(Candidate)
				else if myVote.candidateId == boundParticipantId then become(Leader())
				else become(Follower(myVote.candidateId))
			}

			override def decideMyVote(inquirerId: ParticipantId | Null, inquirerInfo: StateInfo): sequencer.Duty[Vote[ParticipantId]] = {
				val howAreYouQuestions = asksHowOtherParticipantsAre(inquirerId, inquirerInfo)
				for replies <- sequencer.Duty_sequenceTasksToArray(howAreYouQuestions) yield {
					var latestTermSeen = currentTerm
					var chosenCandidate = CandidateInfo(boundParticipantId, buildMyStateInfo)
					var borrame = List(chosenCandidate) //TODO delete line
					var reachableCandidatesCounter = 1 // includes itself
					for replierIndex <- replies.indices do {
						val replierId = otherParticipants(replierIndex)
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
								scribe.debug(s"$boundParticipantId: `$replierId.howAreYou($currentTerm)` failed while deciding vote: $e")
						}
					}
					scribe.debug(s"$boundParticipantId: decideMyVote($inquirerId, $inquirerInfo): chosen=$chosenCandidate, among: $borrame") //TODO delete line						
					Vote(latestTermSeen, chosenCandidate.id, reachableCandidatesCounter, 0, chosenCandidate.info.ordinal)
				}
			}
		}

		private class JointConf(oldParticipants: IArray[ParticipantId], newParticipants: Set[ParticipantId]) extends Configuration {
			private val oldSmallestMajority: Int = 1 + oldParticipants.length / 2
			private val newSmallestMajority: Int = 1 + newParticipants.size / 2
			override val participants: IArray[ParticipantId] = IArray.unsafeFromArray((newParticipants ++ oldParticipants).toArray.sorted)
			override val otherParticipants: IArray[ParticipantId] = participants.filter(_ != boundParticipantId)

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
				vote.reachableCandidatesOfOldConf == oldParticipants.length && vote.reachableCandidatesOfNewConf == newParticipants.size
			}

			override def reachedAMajority(vote: Vote[ParticipantId]): Boolean = {
				vote.reachableCandidatesOfOldConf >= oldSmallestMajority && vote.reachableCandidatesOfNewConf >= newSmallestMajority
			}

			@tailrec
			override final def indexOfTheCommitedRecordWithHighestIndex(from: RecordIndex, highestRecordIndexKnowToBeAppended_ByParticipantIndex: IArray[RecordIndex]): RecordIndex = {
				val n = from + 1
				if n >= workspace.firstEmptyRecordIndex then from
				else if workspace.getRecordAt(n).term != currentTerm then from
				else {
					var oldParticipantsWithRecordAtNSuccessfullyAppended = 0
					var newParticipantsWithRecordAtNSuccessfullyAppended = 0
					for otherParticipantIndex <- otherParticipants.indices do {
						if highestRecordIndexKnowToBeAppended_ByParticipantIndex(otherParticipantIndex) >= n then {
							val otherParticipantId = otherParticipants(otherParticipantIndex)
							if oldParticipants.contains(otherParticipantId) then oldParticipantsWithRecordAtNSuccessfullyAppended += 1
							if newParticipants.contains(otherParticipantId) then newParticipantsWithRecordAtNSuccessfullyAppended += 1
						}
					}
					if oldParticipantsWithRecordAtNSuccessfullyAppended < oldSmallestMajority || newParticipantsWithRecordAtNSuccessfullyAppended < newSmallestMajority then from
					else indexOfTheCommitedRecordWithHighestIndex(n, highestRecordIndexKnowToBeAppended_ByParticipantIndex)
				}
			}

			override def achievesQuorumWhen(appendResults: Array[Try[Boolean]]): Boolean = {
				var newParticipantsWithSuccessfulAppendResult = 0
				var oldParticipantsWithSuccessfulAppendResult = 0
				// start the loop with this participant, assuming it already appended the records and will persist its state after calling this method.
				var participantId = boundParticipantId
				var otherParticipantIndex = otherParticipants.length
				while otherParticipantIndex >= 0 do {
					if oldParticipants.contains(participantId) then oldParticipantsWithSuccessfulAppendResult += 1
					if newParticipants.contains(participantId) then newParticipantsWithSuccessfulAppendResult += 1
					otherParticipantIndex -= 1
					while otherParticipantIndex >= 0 && participantId == boundParticipantId do {
						appendResults(otherParticipantIndex) match {
							case Success(true) =>
								participantId = otherParticipants(otherParticipantIndex)
							case _ =>
								otherParticipantIndex -= 1
								participantId = boundParticipantId
						}
					}
				}
				oldParticipantsWithSuccessfulAppendResult >= oldSmallestMajority && newParticipantsWithSuccessfulAppendResult >= newSmallestMajority
			}

			override def updateStateKnowingAllVotes(myVote: Vote[ParticipantId], votesFromOthers: Array[Try[Vote[ParticipantId]]]): Behavior = {
				var myVoteIsStale = false
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
								// if the term is stale then: update it, clear the counters, and break both loops.
								if vote.term > currentTerm then {
									assert(isInSequence) // TODO delete line
									currentTerm = vote.term
									oldParticipantsVotesMatchingMyVote = 0
									newParticipantsVotesMatchingMyVote = 0
									voteIndex = -1
								}

							case _: Failure[Vote[ParticipantId]] =>
								vote = myVote
								voteIndex -= 1
						}
					}
				}
				if oldParticipantsVotesMatchingMyVote < oldSmallestMajority || newParticipantsVotesMatchingMyVote < newSmallestMajority then become(Candidate)
				else if myVote.candidateId == boundParticipantId then become(Leader())
				else become(Follower(myVote.candidateId))

			}

			override def decideMyVote(inquirerId: ParticipantId | Null, inquirerInfo: StateInfo): sequencer.Duty[Vote[ParticipantId]] = {
				val howAreYouQuestions = asksHowOtherParticipantsAre(inquirerId, inquirerInfo)

				for replies <- sequencer.Duty_sequenceTasksToArray(howAreYouQuestions) yield {

					val myStateInfo = buildMyStateInfo

					var stateInfo = myStateInfo
					var participantId = boundParticipantId
					var latestTermSeen = currentTerm

					var oldParticipantsThatAreReachable = 0
					var newParticipantsThatAreReachable = 0
					var chosenCandidate = CandidateInfo(participantId, stateInfo)
					var borrame = List(chosenCandidate) //TODO delete line

					var participantIndex = otherParticipants.length
					while participantIndex >= 0 do {
						if oldParticipants.contains(participantId) then oldParticipantsThatAreReachable += 1
						if newParticipants.contains(participantId) then newParticipantsThatAreReachable += 1

						participantIndex -= 1
						// navigate to the next successfully replied StateInfo and get it
						while participantIndex >= 0 && (stateInfo eq myStateInfo) do {
							participantId = otherParticipants(participantIndex)
							replies(participantIndex) match {
								case s: Success[StateInfo] =>
									stateInfo = s.value
									if stateInfo.currentTerm > latestTermSeen then latestTermSeen = stateInfo.currentTerm
									val candidateInfo = CandidateInfo(participantId, stateInfo)
									chosenCandidate = chosenCandidate.getWinnerAgainst(candidateInfo)
									borrame = candidateInfo :: borrame //TODO delete line

								case f: Failure[StateInfo] =>
									scribe.debug(s"$boundParticipantId: `$participantId.howAreYou($currentTerm)` failed while deciding vote: ${f.exception}")
									stateInfo = myStateInfo
									participantIndex -= 1
							}
						}
					}
					scribe.debug(s"$boundParticipantId: decideMyVote($inquirerId, $inquirerInfo): chosen=$chosenCandidate, among: $borrame") //TODO delete line

					Vote(latestTermSeen, chosenCandidate.id, oldParticipantsThatAreReachable, newParticipantsThatAreReachable, currentBehavior.ordinal)
				}
			}
		}

		//// UTILITIES USED BY MANY BEHAVIORS

		/**
		 * Asks the [[otherParticipants]] how are they.
		 *
		 * @param inquirerId the id of the participant of which this participant already knows its [[StateInfo]] (because it sent it to this participant along the request this participant is currently responding) or null if this participant does not know the [[StateInfo]] of anyone else. Note that this parameter is not necessarily needed. Its only purpose is to improve efficiency by saving a query of already known information.
		 * @param inquirerInfo the [[StateInfo]] of the inquirer if `inquirerId` is not null. Note that this parameter is not necessarily needed. Its only purpose is to improve efficiency by saving a query of already known information.
		 * @return A task that yields a [[Vote]] with the chosen leader for the current term.
		 */
		private def asksHowOtherParticipantsAre(inquirerId: ParticipantId | Null, inquirerInfo: StateInfo): IndexedSeq[sequencer.Task[StateInfo]] = {
			for otherParticipantId <- otherParticipants yield {
				if otherParticipantId == inquirerId then sequencer.Task_successful(inquirerInfo)
				else askHowIsAnotherIfNotAlready(otherParticipantId)
			}
		}

		/** @return a [[sequencer.Task]] that yields the response to the "how are you" question to a specified participant.
		 *         If such a question is on the way, yields the response of already done question. */
		private def askHowIsAnotherIfNotAlready(otherParticipantId: ParticipantId): sequencer.Task[StateInfo] = {
			howAreYouRequestOnTheWayByParticipant.getOrElseUpdate(
				otherParticipantId,
				sequencer.Commitment_triggerAndWire(
					otherParticipantId.howAreYou(currentTerm).andThen(_ => howAreYouRequestOnTheWayByParticipant.remove(otherParticipantId)),
					true
				)
			)
		}

		/**
		 * Knows all the information about a candidate necessary by participants to decide which to vote in a leader election.
		 */
		private class CandidateInfo(val id: ParticipantId, val info: StateInfo) {
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

		def subscribe(listener: NotificationListener): Unit = {
			assert(isInSequence)
			notificationListeners.put(listener, None)
		}

		def unsubscribe(listener: NotificationListener): Boolean = {
			assert(isInSequence)
			notificationListeners.remove(listener) eq None
		}

		/** @param notificator a function that receives a [[NotificationListener]] and calls one of its methods. */
		private inline def notifyListeners(inline notificator: NotificationListener => Unit): Unit = {
			assert(sequencer.isInSequence)
			notificationListeners.forEach { (listener, _) => notificator(listener) }
		}
	}
}
