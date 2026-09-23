package readren.consensus

import protocol.*

import readren.common.*
import readren.common.Trace.Context
import readren.sequencer.*

import java.util
import java.util.Comparator
import scala.collection.immutable.ListSet
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math.Ordering.Implicits.infixOrderingOps
import scala.reflect.ClassTag
import scala.runtime.IntRef
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object ConsensusParticipantSdm {
	final val assertionsEnabled: Boolean = classOf[ConsensusParticipantSdm].desiredAssertionStatus()
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
trait ConsensusParticipantSdm extends ConsensusElectorateCdm with ConsensusPrimaryStateCdm { thisModule =>

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

	/** The type of the state-machine's [[StateMachine.applyClientCommand]] method's responses. */
	type StateMachineResponse

	/** The type of [[Workspace]] implementation. */
	type WS <: Workspace
	
	//// ConsensusElectorateCdm implementation ////

	override val participantIdComparator: Comparator[ParticipantId] = summon[Ordering[ParticipantId]]

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

	//// CLUSTER

	/** Specifies what a [[ConsensusParticipant]] service requires from the cluster-participant-service it is bound to.
	 *
	 * A [[ClusterParticipant]] represents a single participant within a specific cluster and provides the identity, membership, and communication mechanisms required by the bound [[ConsensusParticipant]] service.
	 *
	 * Responsibilities of a [[ClusterParticipant]] include:
	 * - Exposing the identity of the participant it services via [[boundParticipantId]].
	 * - Providing the initial cluster membership via [[getInitialParticipants]], which must return the same set across all participants listed.
	 * - Acting as the source of truth for cluster membership and determining when an electorate change should be triggered. This includes reacting to node join/leave events, quorum loss, scaling decisions, or health-based adjustments.
	 * - Initiating electorate transitions by calling [[Delegate.requestElectorateChange]] when a change is required.
	 * - Routing inter-participant RPCs (e.g., [[howAreYou]], [[chooseALeader]], [[appendRecords]]) to the appropriate [[Delegate]] methods.
	 * - Delivering client commands and consensus messages to the bound [[ConsensusParticipant]] via the last [[Delegate]] set with [[setBound]] by the [[ConsensusParticipant]].
	 * - Scheduling deferred wake-ups when requested by the [[ConsensusParticipant]] via [[requestWakeUp]], and invoking the provided callback within the [[sequencer]] after an appropriate host-determined delay.
	 * - Ensuring all invocations occur within the [[sequencer]] thread.
	 *
	 * Each [[ClusterParticipant]] instance is tightly bound to a single [[ConsensusParticipant]] instance.
	 * If a cluster-service had to service more than one [[ConsensusParticipant]] instance simultaneously, it would have to create a different instance of [[ClusterParticipant]] for each.
	 */
	trait ClusterParticipant extends ClusterView {

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

		/** **Outbound bridge (advisory hook)**: Called by the bound [[ConsensusParticipant]] to advise that its active [[Electorate]] has changed, and now it expects connectivity with the active participants of the provided [[ElectorateChange]].\
		 *
		 * This method is invoked upon activation of a new [[Electorate]] to tell the cluster-layer which are the participants that the consensus-layer expects to be reachable.\
		 * Given electorate changes is a two-phase process, a call to [[Delegate.requestElectorateChange]] causes two [[ElectorateChange]] records to be appended and, therefore, two calls to this method per involved [[ConsensusParticipant]] service.\
		 * Successive calls with the same argument may occur. Implementations may ignore such calls only if no intervening call with a different argument has occurred — i.e., if the electorate has not changed.\
		 * @param change The [[ElectorateChange]] that backs the activated [[Electorate]].
		 * @param changeIndex the [[RecordIndex]] of the applied [[ElectorateChange]]
		 */
		def onActiveElectorateChanged(change: ElectorateChange[ParticipantId], changeIndex: RecordIndex, roleOrdinal: RoleOrdinal): Unit

		/** **Outbound bridge (advisory hook)**: Called by the bound [[ConsensusParticipant]] after it becomes quiesced. This allows this [[ClusterParticipant]] service to release the resources dedicated to it. */
		def onQuiesced(motive: Try[String]): Unit

		/** Called by the bound [[ConsensusParticipant]] when it needs to be woken up after some host-determined delay.
		 * The host should eventually invoke the provided `callback` within the [[sequencer]], after an appropriate delay.
		 * The [[WakeUpReason]] conveys what the consensus algorithm needs retried so the host can choose an appropriate delay.
		 *
		 * Must be called within the [[sequencer]].
		 *
		 * @param reason describes what the consensus algorithm needs retried.
		 * @param wakeUpsDone the number of related wake-up requests done before. Allows the implementation to determine a delay that depends on the number of failed attempts.
		 * @param callback the function to invoke when the delay elapses. Must be invoked within the [[sequencer]].
		 * @return a token that can be passed to [[cancelWakeUp]] to cancel the pending wake-up.
		 */
		def requestWakeUp(reason: WakeUpReason, wakeUpsDone: Int, callback: () => Unit): WakeUpToken

		/** Defines the operations that the [[ConsensusParticipant]] exposes to this [[ClusterParticipant]] instance, specially the call-backs methods for the events that the [[ConsensusParticipant]] needs to be noticed of.
		 *
		 * The [[ConsensusParticipant]] is responsible for setting the bound to this [[ClusterParticipant]] instance by calling [[setBound]].
		 * This [[ClusterParticipant]] instance talks to the bound [[ConsensusParticipant]] by calling these methods.
		 * For example, to deliver client commands, consensus messages, and request cluster-electorate changes.
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
			 * @param indexOfGrantedSec The index of the [[SoleElectorateChange]] record for which the permission to quiesce was granted. Said record is the one that excludes the destination participant.
			 */
			def onQuiescencePermitted(grantorId: ParticipantId, indexOfGrantedSec: RecordIndex): Unit

			/** Allows the [[ClusterParticipant]] service to request changes to the set of consensus-participants.
			 * Usually called whenever the set of consensus-participants has forcefully changed (i.e: a cluster-member included in the current consensus-participants-set went down) or is about to change (i.e: a node intended to be part of consensus-participants-set joined the cluster, or is going to leave the cluster for maintenance).
			 * To improve availability during planned cluster-membership transitions, the manager of the planed change should do the following:
			 *		1 call this method on every consensus-participant service to ensure the leader gets noticed, // TODO this is awkward. Make the electorate-change request be propagated to the leader when received by non-leaders.
			 *		2 wait until either:
			 *			- the returned [[sequencer.Capture]] yields either [[SUCCESSFULLY_CHANGED]] or [[ALREADY_CHANGED]] for any of the consensus-participants,
			 *			- or the [[onActiveElectorateChanged]] is called in any of the consensus-participants with the provided request identifier or desired participants set.
			 *
			 * @param requestId an identifier chosen by the caller that will be propagated up to the invocations of the [[onActiveElectorateChanged]] method of each of the [[ClusterParticipant]] instances bound to the involved [[ConsensusParticipant]] services.
			 * @param desiredParticipantsSet the identifiers of the participants that are going to seek consensus from now on.
			 * @param priorAnswer should contain the response to the last request done by the inquirer to this or any other participant, if any.
			 * @return a [[sequencer.Capture]] that yields:
			 *         [[SUCCESSFULLY_CHANGED]] if the requested change was successfully completed.
			 *         [[ALREADY_CHANGED]] if the requested change is already done or in progress.
			 *         [[ASK_THE_LEADER]] if none of the previous bullet is true and the [[ConsensusParticipant]] is a [[FOLLOWER]].
			 *         [[STOPPED]] if the participant is not able to become neither the [[LEADER]] nor a [[FOLLOWER]]
			 *         - currently the leader or a follower that already has the desired participants set as the current or scheduled one;
			 *         - currently the leader and was able to replicate the corresponding [[JointElectorateChange]] to a majority according to that same [[JointElectorateChange]] rules. */
			def requestElectorateChange(requestId: ElectorateChangeRequestId, desiredParticipantsSet: Set[ParticipantId], priorAnswer: Maybe[ElectorateChangeResponse]): sequencer.Capture[ElectorateChangeResponse]
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
			 * Authorizes the destination participant to transition from [[RETIRING]] to the terminal [[QUIESCED]] [[ConsensusParticipant.Role]], provided it becomes [[RETIRING]] due to being excluded by the [[SoleElectorateChange]] at the specified index.
			 * This bridge is invoked by the leader established AFTER the second phase of an electorate change (that excluded the destination participant) has finalized.
			 * By requiring the leader of the new electorate to issue this permission, the system ensures the caller is a stable authority within the finalized membership set — thereby excluding the 'ghost leader' from performing this final decommissioning.
			 *
			 * This call is the trigger for the **Retiring Quorum-Buffering** mechanism to release the buffer.
			 * The purpose of this mechanism is to maintain the quorum safety of the old participants set during joint consensus.
			 * By holding excluded participants in the [[RETIRING]] role, the system ensures they contribute to the quorum of the old set (by not voting but effectively lowering the required threshold of active votes) until a new, stable majority is functionally proven by a new leader.
			 *
			 * @param indexOfGrantedSec The index of the [[SoleElectorateChange]] record for which the authorization is granted, which is the one that excludes the destination participant.
			 * @return A [[sequencer.Capture]] that completes successfully if either: the permission was successfully delivered, or the participant is already in a post-retirement state ([[QUIESCED]], released, or no longer exists).
			 */
			def permitQuiescence(indexOfGrantedSec: RecordIndex): sequencer.Capture[Unit]
		}
	}

	//// NOTIFICATIONS

	trait NotificationListener {
		def onStarting(previous: RoleOrdinal, indexOfTheIncludingElectorateChange: RecordIndex): Unit

		def onStarted(previous: RoleOrdinal, term: Term, initialElectorateChange: ElectorateChange[ParticipantId], isSeed: Boolean): Unit

		def onBecameQuiesced(previous: RoleOrdinal, term: Term, motive: Try[String]): Unit

		def onJoining(previous: RoleOrdinal, indexOfTheIncludingElectorateChange: RecordIndex): Unit

		def onBecameIsolated(previous: RoleOrdinal, term: Term): Unit

		def onBecameCandidate(previous: RoleOrdinal, term: Term): Unit

		def onBecameFollower(previous: RoleOrdinal, term: Term, leaderId: ParticipantId): Unit

		def onBecameLeader(previous: RoleOrdinal, term: Term): Unit

		def onAbdicating(term: Term): Unit

		def onRetiring(previous: RoleOrdinal, term: Term): Unit

		def onRoleLeft(left: RoleOrdinal, term: Term): Unit

		def onCommitIndexChanged(previous: RecordIndex, current: RecordIndex, as: RoleOrdinal, at: Term): Unit

		/** Called whenever a [[CommandRecord]] is applied to the [[StateMachine]]. */
		def onCommandApplied(appliedCommandIndex: RecordIndex, appliedCommandTerm: Term): Unit

		def onActiveElectorateChanged(currentRole: RoleOrdinal, currentTerm: Term, electorateChangeIndex: RecordIndex, electorateChange: ElectorateChange[ParticipantId]): Unit
	}

	/** A convenience [[NotificationListener]] implementation with no-op methods.\
	 * Extend this class and override only the methods you need. */
	open class DefaultNotificationListener extends NotificationListener {
		override def onStarting(previous: RoleOrdinal, indexOfTheIncludingElectorateChange: RecordIndex): Unit = ()

		override def onStarted(previous: RoleOrdinal, term: Term, initialElectorateChange: ElectorateChange[ParticipantId], isSeed: Boolean): Unit = ()

		override def onBecameQuiesced(previous: RoleOrdinal, term: Term, motive: Try[String]): Unit = ()

		override def onJoining(previous: RoleOrdinal, indexOfTheIncludingElectorateChange: RecordIndex): Unit = ()

		override def onBecameIsolated(previous: RoleOrdinal, term: Term): Unit = ()

		override def onBecameCandidate(previous: RoleOrdinal, term: Term): Unit = ()

		override def onBecameFollower(previous: RoleOrdinal, term: Term, leaderId: ParticipantId): Unit = ()

		override def onBecameLeader(previous: RoleOrdinal, term: Term): Unit = ()

		override def onAbdicating(term: Term): Unit = ()

		override def onRetiring(previous: RoleOrdinal, term: Term): Unit = ()

		override def onRoleLeft(left: RoleOrdinal, term: Term): Unit = ()

		override def onCommitIndexChanged(previous: RecordIndex, current: RecordIndex, as: RoleOrdinal, at: Term): Unit = ()

		override def onCommandApplied(appliedCommandIndex: RecordIndex, appliedCommandTerm: Term): Unit = ()

		override def onActiveElectorateChanged(currentRole: RoleOrdinal, currentTerm: Term, electorateChangeIndex: RecordIndex, electorateChange: ElectorateChange[ParticipantId]): Unit = ()
	}

	private inline def checkWithin(): Unit = {
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
	 * @param indexOfTheIncludingElectorateChange the [[RecordIndex]] of the [[JointElectorateChange]] that caused this [[ConsensusParticipant]] service to join.
	 */
	final class ConsensusParticipant(cluster: ClusterParticipant, storage: Storage, machine: StateMachine, indexOfTheIncludingElectorateChange: RecordIndex, participantsInTheIncludingElectorateChange: ListSet[ParticipantId], initialListeners: Iterable[NotificationListener]) { thisConsensusParticipant =>

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

		/** Decorates persistence storage to handle save failures by transitioning this participant to [[Quiesced]]. */
		private val coordinatingStorage: Storage = new Storage {
			override def load: sequencer.Capture[WS] = storage.load

			override def save(workspace: WS): sequencer.Capture[Unit] = {
				storage.save(workspace).andThen(
					_ => (),
					e => {
						Trace.init(() => s"$boundParticipantId: onStorageSaveFailure") {
							Trace.error(s"$boundParticipantId: Unexpected error while saving the workspace. This participant's consensus service is unable to continue following the leader and will quiesce.", e)
							become(Quiesced(Failure(e)))
						}
					}
				)
			}
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
		private var currentRole: Role = new Starting(indexOfTheIncludingElectorateChange, participantsInTheIncludingElectorateChange)

		/** Used to chain onto prior release covenants to ensure all previous workspace releases complete before advancing the [[PrimaryState]], guaranteeing a single observable completion handle for quiescence and disposal. */
		private var workspaceReleaseCompletion: sequencer.Capture[Unit] = sequencer.Keeper(())

		/** Memorizes the latest [[Electorate]] derived by the [[StatefulRole.deriveElectorateFrom]] method.\
		 * It is initialized by [[Starting.handleEnter]] with a synthetic [[JointElectorate]] before transitioning to a [[StatefulRole]] and stays defined as long as the [[currentRole]] is stateful.\
		 * CAUTION: This variable depends on the [[PrimaryState]]; mutations to the [[PrimaryState]] modify its value. To ensure deterministic causal ordering relative to these mutations, a [[StatefulRole]] must only access this variable within consumers synchronously subscribed to the [[sequencer.Capture]] returned by [[sequencer.CausalFence.causalAnchor]] or [[sequencer.CausalFence.advance]]-like methods on the [[StatefulRole.primaryStateFence]]. This ensures the variable is read in synchronization with the specific PrimaryState mutation it depends on. */
		private var latestDerivedElectorate: Maybe[Electorate] = Maybe.empty

		/** Memory where the [[Role.onQuiescencePermitted]] method stores the [[ParticipantId]] of the last quiescence grantor. */
		private var quiescenceGrantor: Maybe[ParticipantId] = Maybe.empty
		/** Memory where the [[Role.onQuiescencePermitted]] method stores the [[RecordIndex]] of the last [[SoleElectorateChange]] for which quiescence was authorized. */
		private var indexOfSecForWhichQuiescenceWasPermitted: RecordIndex = 0
		/** Memorizes the token for the pending wake-up used to retry failed calls to [[permitQuiescence]]. Needed to be able to cancel the retry. */
		private var retryPermitQuiescenceWakeUpToken: Maybe[WakeUpToken] = Maybe.empty
		/** Knows the participants that are waiting for an acknowledgment to the quiescence authorizations, and the corresponding [[RecordIndex]] of the [[SoleElectorateChange]] for which the permission granted. */
		private val nonAcknowledgedQuiescencePermissions: mutable.Map[ParticipantId, RecordIndex] = mutable.Map.empty

		/** Knows the [[LearnerProgress]]s corresponding to the participants that were excluded from the electorate and potentially have not received the appends to notice that they can leave. */
		private val retiringLearnersById: mutable.Map[ParticipantId, LearnerProgress] = mutable.Map.empty

		/** The current election round.
		 * Should be bumped whenever the part of the state of this participant that is exposed in questions to other participants (term and commitIndex as of this writing) changes.
		 * CAUTION: This variable depends on the [[PrimaryState]] (as well as external states); mutations to the [[PrimaryState]] modify its value. To ensure deterministic causal ordering relative to these mutations, a [[StatefulRole]] must only access this variable within consumers synchronously subscribed to the [[sequencer.Capture]] returned by [[sequencer.CausalFence.causalAnchor]] or [[sequencer.CausalFence.advance]]-like methods on the [[StatefulRole.primaryStateFence]]. This ensures the variable is read in synchronization with the specific PrimaryState mutation it depends on. */
		private var currentBallot: Ballot = INITIAL_BALLOT

		/** Stores the last [[StateInfo]] instance returned by [[Role.syncLocalStateInfo]]
		 * CAUTION: This variable depends on the [[PrimaryState]] (as well as external states); mutations to the [[PrimaryState]] modify its value. To ensure deterministic causal ordering relative to these mutations, a [[StatefulRole]] must only access this variable within consumers synchronously subscribed to the [[sequencer.Capture]] returned by [[sequencer.CausalFence.causalAnchor]] or [[sequencer.CausalFence.advance]]-like methods on the [[StatefulRole.primaryStateFence]]. This ensures the variable is read in synchronization with the specific PrimaryState mutation it depends on. */
		private var stateInfoExposedInLastInteraction: StateInfo = StateInfo(PRE_INIT, ER_NONE, PRE_INIT, 0, PRE_INIT, 0, INITIAL_BALLOT)

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
						case e: GracefullyReleased => failedAppendResultBuilder(e)
						case _ => Maybe.empty
					}
				}

			override def onInstallSnapshot(inquirerId: ParticipantId, inquirerTerm: Term, snapshot: SnapshotData[ParticipantId], batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.Capture[AppendResult] =
				Trace.init(() => s"$boundParticipantId: onInstallSnapshot") {
					checkWithin()
					currentRole.onInstallSnapshot(inquirerId, inquirerTerm, snapshot, batch, leaderCommit, termAtLeaderCommit).recover {
						case e: GracefullyReleased => failedAppendResultBuilder(e)
						case _ => Maybe.empty
					}
				}

			override def requestElectorateChange(requestId: ElectorateChangeRequestId, desiredParticipants: Set[ParticipantId], priorAnswer: Maybe[ElectorateChangeResponse]): sequencer.Capture[ElectorateChangeResponse] = {
				Trace.init(() => s"$boundParticipantId: requestElectorateChange-$requestId") {
					checkWithin()
					currentRole.requestElectorateChange(requestId, desiredParticipants, priorAnswer).recover {
						case _: GracefullyReleased => Maybe(new STOPPED(currentRole.syncStatelessStateInfo().ballot))
						case _ => Maybe.empty
					}
				}
			}

			override def onQuiescencePermitted(grantorId: ParticipantId, indexOfGrantedSec: RecordIndex): Unit =
				Trace.init(() => s"$boundParticipantId: onQuiescencePermitted") {
					checkWithin()
					currentRole.onQuiescencePermitted(grantorId, indexOfGrantedSec)
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

			def requestElectorateChange(requestId: ElectorateChangeRequestId, desiredParticipants: Set[ParticipantId], priorAnswer: Maybe[ElectorateChangeResponse])(using Trace.Context): sequencer.Capture[ElectorateChangeResponse]

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

			/** Updates the derived state that is stored in the [[Role]] instance and depends on the current [[Electorate]]. Only the [[Leader]] role has such state as this writing. */
			def handleActiveElectorateChange(currentPrimaryState: PrimaryState, currentElectorate: Electorate, newElectorate: Electorate, indexOfNewElectorateChange: RecordIndex)(using Context): Unit = ()

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
					if rememberedInfo.tiesWith(term, thisRole.rank, PRE_INIT, 0, PRE_INIT, 0) then {
						if rememberedInfo.ballot == currentBallot then rememberedInfo else StateInfo(term, thisRole.rank, PRE_INIT, 0, PRE_INIT, 0, currentBallot)
					} else {
						currentBallot = currentBallot.bumped
						memorizedPeersInfos.clear()
						StateInfo(term, thisRole.rank, PRE_INIT, 0, PRE_INIT, 0, currentBallot)
					}
				stateInfoExposedInLastInteraction = newInfo
				newInfo
			}

			/** Must be called before transitioning to [[Retiring]] to handle the special case when the active [[Electorate]] in an empty [[SoleElectorate]].\
			 * The [[Leader]] role should start the process that authorizes others to transition to the terminal [[QUIESCED]] state.\
			 * "Vanished" means the new electorate has zero participants. In that case there will be no successor leader to authorize quiescence, so the current (ghost) leader must do it itself before retiring.\
			 * @param soleElectorate The currently active [[SoleElectorate]]. */
			def authorizeQuiescenceIfVanished(soleElectorate: SoleElectorate)(using Trace.Context): Unit = ()

			def onQuiescencePermitted(grantorId: ParticipantId, indexOfGrantedSec: RecordIndex)(using Trace.Context): Unit = {
				if indexOfGrantedSec > indexOfSecForWhichQuiescenceWasPermitted then {
					quiescenceGrantor = Maybe(grantorId)
					indexOfSecForWhichQuiescenceWasPermitted = indexOfGrantedSec
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
			override final def syncLocalStateInfo(maybePrimaryState: Maybe[PrimaryState])(using Trace.Context): StateInfo = Trace.step("syncLocalStateInfo") {
				assert(maybePrimaryState.isDefined, s"StatefulRole (${RoleOrdinal_nameOf(ordinal)}) requires a defined PrimaryState")
				val primaryState = maybePrimaryState.get
				val rememberedInfo = stateInfoExposedInLastInteraction
				val termAtCommitIndex = primaryState.getRecordTermAt(commitIndex)
				val lastRecordIndex = primaryState.firstEmptyRecordIndex - 1
				val lastRecordTerm = primaryState.getRecordTermAt(lastRecordIndex)
				val newInfo =
					if rememberedInfo.tiesWith(primaryState.currentTerm, thisStatefulRole.rank, termAtCommitIndex, commitIndex, lastRecordTerm, lastRecordIndex) then {
						if rememberedInfo.ballot == currentBallot then rememberedInfo else StateInfo(primaryState.currentTerm, thisStatefulRole.rank, termAtCommitIndex, commitIndex, lastRecordTerm, lastRecordIndex, currentBallot)
					} else {
						currentBallot = currentBallot.bumped
						memorizedPeersInfos.clear()
						StateInfo(primaryState.currentTerm, thisStatefulRole.rank, termAtCommitIndex, commitIndex, lastRecordTerm, lastRecordIndex, currentBallot)
					}
				stateInfoExposedInLastInteraction = newInfo
				if assertionsEnabled then assert(newInfo.rank != ER_NONE)
				newInfo
			}

			protected sealed trait DiscoveryReconciliation

			protected case class DiscoveryReconciledReady(primaryState: PrimaryState, electorate: Electorate, stateInfo: StateInfo) extends DiscoveryReconciliation

			protected case class DiscoveryReconciledRestart(primaryState: PrimaryState, reason: String) extends DiscoveryReconciliation

			protected case object DiscoveryReconciledRoleChanged extends DiscoveryReconciliation

			/** Phase 1 Discovery Query: Inquires all peers in the provided [[Electorate]] how they are ([[ClusterParticipant.howAreYou]]) and accumulates their replies into a [[DiscoveryQuorumResult]].\
			 * Pure query: performs zero mutations on [[PrimaryState]], [[commitIndex]], [[memorizedPeersInfos]], or [[currentBallot]]. */
			protected final def discoverPeersState(electorate: Electorate, stateInfo: StateInfo)(using Trace.Context): sequencer.Capture[DiscoveryQuorumResult[ParticipantId]] = {
				val inquiries = askHowOtherParticipantsAre(electorate.peers, stateInfo, memorizedPeersInfos)
				electorate.accumulateDiscoveryQuorum(stateInfo, inquiries)
			}

			/** Phase 1 State Reconciliation Command: Ingests the discovery result by updating the term (if a higher term was seen), ingesting peer state info, advancing the commit index (if absorptive), and checking for electorate shifts. */
			protected final def reconcileDiscoveredState(electorateAtRequest: Electorate, discoveryResult: DiscoveryQuorumResult[ParticipantId])(using Trace.Context): sequencer.Capture[DiscoveryReconciliation] = {
				Trace.trace(s"Discovery replies=${discoveryResult.replies.zip(electorateAtRequest.peers).mkString("[", ", ", "]")}, latestTermSeen=${discoveryResult.highestTermSeen}, outcome=${discoveryResult.outcome}")
				for primaryState1 <- updateTermIfLessThan(discoveryResult.highestTermSeen) yield {
					if currentRole ne thisStatefulRole then DiscoveryReconciledRoleChanged
					else {
						var stateInfo1 = currentRole.syncStatefulStateInfo(primaryState1)
						discoveryResult.outcome match {
							case _: DiscoveryQuorumOutcome_MajorityReached =>
								electorateAtRequest.peers.foreachWithIndex { (peerId, peerIndex) =>
									discoveryResult.replies(peerIndex) match {
										case Success(peerInfo) =>
											if updateSeenStateInfo(stateInfo1, peerId, peerInfo) then stateInfo1 = currentRole.syncStatefulStateInfo(primaryState1)
										case _ => ()
									}
								}
								if absorbHigherCommitIndexFromPeers(primaryState1, stateInfo1) then DiscoveryReconciledRestart(primaryState1, "commit index absorbed from peer")
								else {
									val electorate1 = deriveElectorateFrom(primaryState1)
									if electorate1 ne electorateAtRequest then DiscoveryReconciledRestart(primaryState1, s"electorate change (${electorateAtRequest.changeIndex}->${electorate1.changeIndex})")
									else DiscoveryReconciledReady(primaryState1, electorate1, stateInfo1)
								}

							case stale: DiscoveryQuorumOutcome_Stale =>
								updateBallotIfLowerThan(stateInfo1, stale.higherBallot)
								val electorate1 = deriveElectorateFrom(primaryState1)
								DiscoveryReconciledReady(primaryState1, electorate1, currentRole.syncStatefulStateInfo(primaryState1))

							case _ =>
								val electorate1 = deriveElectorateFrom(primaryState1)
								DiscoveryReconciledReady(primaryState1, electorate1, stateInfo1)
						}
					}
				}
			}

			/** Phase 1 Vote Decision Query: Pure synchronous calculation that evaluates our vote from the reconciled state and discovery outcome. */
			def decideMyVote(
				primaryState: PrimaryState,
				electorate: Electorate,
				stateInfo: StateInfo,
				outcome: DiscoveryQuorumOutcome
			)(using Trace.Context): Vote[ParticipantId] = {
				outcome match {
					case _: DiscoveryQuorumOutcome_MajorityReached =>
						if electorate.isBoundIncluded || (currentRole.isInstanceOf[Leader] && currentRole.asInstanceOf[Leader].isGhost) then {
							electorate.decideMyVote(stateInfo, memorizedPeersInfosToArray(electorate))
								.fold(blankVote(primaryState.currentTerm, stateInfo.ballot))(identity)
						} else blankVote(primaryState.currentTerm, stateInfo.ballot)

					case _: DiscoveryQuorumOutcome_MajorityImpossible | _: DiscoveryQuorumOutcome_Stale =>
						blankVote(primaryState.currentTerm, stateInfo.ballot)

					case activeLeader: DiscoveryQuorumOutcome_ActiveLeaderDetected[ParticipantId] @unchecked =>
						Vote(
							primaryState.currentTerm,
							activeLeader.leaderId,
							reachableCommonCount = 1,
							reachableTargetCount = 1,
							votedRank = ER_LEADING,
							ballot = stateInfo.ballot
						)
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

			override final def onHowAreYou(inquirerId: ParticipantId, inquirerInfo: StateInfo)(using Trace.Context): sequencer.Capture[StateInfo] = {
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
							val electorate1 = deriveElectorateFrom(primaryState1)
							if !electorate1.isBoundIncluded || inquirerInfo.currentTerm < primaryState1.currentTerm then {
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
			 *     - This participant state transitions to a non-receptive one while updating this participant consensus state due toan electorate change [[Record]] among the received [[Record]]s that should be committed.
			 *
			 *   - Appends new records from the leader, resolving any log conflicts, and, if the leader's term is newer than this participant's current one, also updates `currentTerm` to `inquirerTerm`.
			 *
			 *   - If the term is updated (in the previous bullet) or this participant is not yet a follower, starts the role-update process in a decoupled manner.
			 *
			 *   - Updates the [[commitIndex]] as the minimum of `leaderCommit` and the index of the last appended record.
			 *
			 *   - If this participant state haven't changed to a no receptive one ([[Quiesced]], [[Starting]] or [[Retiring]]) while waiting the application of committed [[Record]]s of the kind that update this participant consensus state (like [[JointElectorateChange]] and [[JointElectorateChange]]), then :
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
			override final def onAppendRecords(inquirerId: ParticipantId, inquirerTerm: Term, prevRecordIndex: RecordIndex, prevRecordTerm: Term, batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term)(using Trace.Context): sequencer.Capture[AppendResult] = {
				Trace.trace(s"onAppendRecords($inquirerId, @$inquirerTerm, $prevRecordIndex, $prevRecordTerm, ${batch.mkString("[", ", ", "]")}, $leaderCommit, $termAtLeaderCommit) called")

				val reporterAndUpdater = new primaryStateFence.Updater[PrimaryState] with FusionReport {
					def update(primaryState0: PrimaryState): Maybe[sequencer.Mono[PrimaryState]] = {
						val currentTerm = primaryState0.currentTerm
						// If the appending is not allowed (either the inquirer term is stale, the current role isn't stateful, or this participant is and will continue leading); then do not mutate the primary state.
						if inquirerTerm < currentTerm || (currentRole ne thisStatefulRole) || (currentRole.rank == ER_LEADING && inquirerTerm == currentTerm) then Maybe.empty
						// Else (if inquirerTerm >= currentTerm && currentRole.isInstanceOf[StatefulRole] && (currentRole.rank != ER_LEADING || inquirerTerm > currentTerm)), do the appending.
						else primaryState0.tryFusingRecords(inquirerTerm, prevRecordIndex, prevRecordTerm, batch, commitIndex, this)
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
			 *   - Derives the electorate from the updated [[PrimaryState]] and [[commitIndex]]
			 *   - Updates the role accordingly.
			 * If the [[StateMachine]]'s commands applier is running, then waits the applier to finish before doing anything other than updating the [[PrimaryState.currentTerm]] with is updated immediately without wait.
			 */
			override final def onInstallSnapshot(inquirerId: ParticipantId, inquirerTerm: Term, snapshot: SnapshotData[ParticipantId], batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term)(using Trace.Context): sequencer.Capture[AppendResult] = {
				Trace.trace(s"onInstallSnapshot(inquirerId=$inquirerId, inquirerTerm=$inquirerTerm, $snapshot, ${batch.mkString("[", ", ", "]")}, leaderCommit=$leaderCommit, termAtCommitIndex=$termAtLeaderCommit) called")
				val updater = new primaryStateFence.Updater[PrimaryState] with FusionReport {
					def update(primaryState0: PrimaryState): Maybe[sequencer.Mono[PrimaryState]] = {
						val currentTerm = primaryState0.currentTerm
						val currentRole = thisConsensusParticipant.currentRole
						// If the appending is not allowed (either the inquirer term is stale, the role changed, or this participant is and will continue leading); then do not mutate the primary state.
						if inquirerTerm < currentTerm || (currentRole ne thisStatefulRole) || (currentRole.rank == ER_LEADING && inquirerTerm == currentTerm) then Maybe.empty
						// If the received snapshot is older than what we already have in the local log, then fusion the batch records only.
						else if snapshot.lastIncludedRecordIndex < commitIndex || (
							snapshot.lastIncludedRecordIndex < primaryState0.firstEmptyRecordIndex
								&& primaryState0.getRecordTermAt(snapshot.lastIncludedRecordIndex) == snapshot.lastIncludedRecordTerm
							) then primaryState0.tryFusingRecords(inquirerTerm, snapshot.lastIncludedRecordIndex, snapshot.lastIncludedRecordTerm, batch, commitIndex, this)
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
			 * This method synchronizes the derived state (role, commit index, active electorate) with the outcome of a primary state mutation (log fusion or snapshot installation) and handles any necessary side effects like triggering the command applier or role transitions.
			 *
			 * Specifically, it performs the following:
			 *  - Updates the local `commitIndex` based on the leader's commit and the local log's first empty index.
			 *  - Updates the current role (e.g., becoming [[Follower]], [[Isolated]], or [[Retiring]]) based on the new electorate and term.
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
				if fusionReport.isTermUpdated then thisStatefulRole.onTermUpdated(primaryState1, Maybe(inquirerId))
				// If the local snapshot was updated or a record was fused.
				if fusionReport.isFused then {
					val previousCommitIndex = commitIndex
					val newCommitIndex = if leaderCommit < primaryState1.firstEmptyRecordIndex then leaderCommit else primaryState1.firstEmptyRecordIndex - 1
					// If the commitIndex is bumped, update it.
					if newCommitIndex > previousCommitIndex then commitIndex = newCommitIndex
					// Derive the active electorate from the updated primary state and commitIndex.
					val electorate1 = deriveElectorateFrom(primaryState1)
					// Update the currentRole:
					val cro = currentRole.ordinal
					// If not joining or the catching-up is complete then:
					if cro != JOINING || (primaryState1.firstEmptyRecordIndex > currentRole.asInstanceOf[Joining].indexOfTheIncludingElectorateChange) then {
						// If this participant belongs to the active electorate, then become a Follower or Isolated, depending on whether the inquirer belongs to the active electorate or not.
						if electorate1.isBoundIncluded then {
							// Become follower of the inquirer if it belongs to the active electorate.
							if electorate1.peers.contains(inquirerId) then become(Follower(primaryState1.currentTerm, inquirerId, primaryStateFence))
							// Become isolated if this participant is joining, the catching-up is complete, and the inquirer is not in the active electorate.
							else if cro == JOINING then become(Isolated(primaryStateFence))
							// Keep the current role otherwise.
							// Note that, if the inquirer is excluded and the current role is follower of an excluded participant, the role is not changed to isolated here because it might be following a ghost leader.
						}
						// If this participant is excluded and ...
						else electorate1 match {
							case sole: SoleElectorate => // ... the active electorate is a sole kind, then become Retiring.
								currentRole.authorizeQuiescenceIfVanished(sole)
								become(Retiring(primaryState1.currentTerm, sole.term, sole.changeIndex, sole.members))

							case joint: JointElectorate => // ... the active electorate is joint, then something is wrong.
								illegalStateQuiesce(s"$inquirerId=$inquirerId, inquirerTerm=$inquirerTerm, prevRecordIndex=$prevRecordIndex, batch=${batch.mkString("[", ", ", "]")}, leaderCommit=$leaderCommit, termAtLeaderCommit=$termAtLeaderCommit, fusionReport=$fusionReport")
							// if currentRole.ordinal != JOINING || accessible1.firstEmptyRecordIndex > currentRole.asInstanceOf[Joining].indexOfTheIncludingElectorateChange then become(Isolated(primaryStateFence))
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
					// Else, surely either the inquirer's term is stale, the terms at prevRecordIndex do not match, the batch fully predates the local latest snapshot, or the cluster is in an illegal state with two leaders. So, respond with a rejection asking for earlier records, pointing to the first record that is missing.
					else {
						// This point is reached if either the inquirer's term is stale, earlier records are needed, the terms at prevRecordIndex do not match, the batch fully predates the local latest snapshot, or the cluster is in an illegal state with two leaders. So, respond with a rejection asking for earlier records, pointing to the first record that is missing.
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
			protected final def startApplyingCommittedCommands(primaryState0: PrimaryState, mustInstallSnapshot: Boolean)(using Trace.Context): Unit = {
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
			final def applyCommittedCommands(primaryState: PrimaryState, upTo: RecordIndex, recursionDepth: Int)(using Trace.Context): sequencer.Capture[Unit] = {
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
									// It is not necessary to have an updated primary state here because committed records are never mutated, and we are not mutating the primary state here. We only need to know if the current role is stateful.
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
						primaryState0 <- primaryStateFence.causalAnchor() // This anchor is necessary because highestAppliedCommandIndex is derived from the primary state.
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
			override final def requestElectorateChange(requestId: ElectorateChangeRequestId, desiredParticipants: Set[ParticipantId], priorAnswer: Maybe[ElectorateChangeResponse])(using Trace.Context): sequencer.Capture[ElectorateChangeResponse] = {
				Trace.trace(s"Handling change to $desiredParticipants, priorAnswer=$priorAnswer.")
				for {
					primaryState <- primaryStateFence.causalAnchor()
					response <- {
						// If a prior answer is provided, update the ballot and memorizedPeersInfos
						val ballotWasUpdated = priorAnswer.fold(false) {
							case nonTerminal: NonTerminalElectorateChangeResponse =>
								updateBallotIfLowerThan(currentRole.syncStatefulStateInfo(primaryState), nonTerminal.latestBallotSeen)
							case _: TerminalElectorateChangeResponse => false
						}
						// Delegate the request to the concrete stateful role.
						currentRole match {
							case stateful: StatefulRole =>
								stateful.requestElectorateChange(primaryState, requestId, desiredParticipants, ballotWasUpdated)
							case stateless =>
								stateless.requestElectorateChange(requestId, desiredParticipants, priorAnswer)
						}
					}
				} yield {
					Trace.trace(s"response: $response")
					response
				}
			}

			def requestElectorateChange(primaryState: PrimaryState, requestId: ElectorateChangeRequestId, desiredParticipants: Set[ParticipantId], ballotWasUpdated: Boolean)(using Context): sequencer.Capture[ElectorateChangeResponse]

			//// Role updaters

			/** Starts a process that updates the [[currentRole]] and [[PrimaryState.currentTerm]] based on the [[StateInfo]]s returned by calling [[ClusterParticipant.howAreYou]] on the other participants and, if necessary, also based on the [[Vote]]s returned by calling [[ClusterParticipant.chooseALeader]] on them.
			 * This process always ends immediately after a call to [[become]] returns. So, its [[Role]] outcome can be seen in the [[currentRole]] derived state variable.
			 * The [[currentRole]] is updated only if the desired one if not equivalent to the [[currentRole]]. If updated, any other in-flight [[updateRole]] process is canceled and immediately completed.
			 * Concurrent executions of this method return the same result. */
			final def updateRole()(using Trace.Context): sequencer.Capture[Unit] = {
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
			final def updateRole(primaryState0: PrimaryState)(using Context): sequencer.Capture[Unit] = {
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
					def updateRoleKnowingMyNonBlankVote(currentState2: PrimaryState, electorate2: Electorate, myVote2: Vote[ParticipantId]): sequencer.Capture[Unit] = {
						if assertionsEnabled then assert(myVote2.term == currentState2.currentTerm)

						// If excluded and not leading as ghost, then retire immediately.
						if !electorate2.isBoundIncluded && !this.isInstanceOf[Leader] then {
							this.authorizeQuiescenceIfVanished(electorate2.asInstanceOf[SoleElectorate]) // The downcast is safe because exclusion is checked every record and joint electorates are never more restrictive than the contiguous sole electorates.
							become(Retiring(currentState2.currentTerm, electorate2.term, electorate2.changeIndex, electorate2.members))
							sequencer.Capture_unit
						}
						// If my vote is for an active leader, become/remain follower immediately without requiring a full discovery quorum.
						else if myVote2.votedRank == ER_LEADING then whenVotingAnother(currentState2, myVote2)
						// else, if got the StateInfo of a majority of the active participants, then:
						else if electorate2.reachedAMajority(myVote2) then {
							// If my vote is for other participant, become follower or isolated depending on the other is leading or not.
							if myVote2.votedId != boundParticipantId then whenVotingAnother(currentState2, myVote2)
							// If my vote is for myself and I am leading, abort the role update.
							else if this.ordinal == LEADER then sequencer.Capture_unit
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
											val inquires = for replierId <- electorate2.peers yield replierId.chooseALeader(boundParticipantId, myStateInfoAtChooseALeaderRequest)
											for {
												quorumResult <- electorate2.accumulateVotingQuorum(myVote2, myStateInfoAtChooseALeaderRequest, inquires)
												primaryState3 <- {
													Trace.trace(s"Replied votes=${quorumResult.replies.zip(electorate2.peers).mkString("[", ", ", "]")}, latestTermSeen=${quorumResult.highestTermSeen}, myVote=$myVote2, outcome=${quorumResult.outcome}")
													updateTermIfLessThan(quorumResult.highestTermSeen) // Note that this may change the role.
												}
												_ <- {
													if haveToAbort then sequencer.Capture_unit
													else {
														// If the term was bumped (while waiting the votes from the other participants or due to a higher term seen in them), then the role update is responsibility of `StatefulRole.onTermBumped`; so abort this update. Restarting the role update here might collide with role changes caused by the bump.
														if primaryState3.currentTerm > primaryStateBumped.currentTerm then {
															assert(this.ordinal != LEADER) // because while leading the term should never change.
															sequencer.Capture_unit
														} else {
															val myStateInfo3 = syncStatefulStateInfo(primaryState3)
															val aHigherBallotHaveBeenSeenInVotes = updateBallotIfLowerThan(myStateInfo3, quorumResult.highestBallotSeen)
															if aHigherBallotHaveBeenSeenInVotes || myStateInfo3.ballot != myStateInfoAtChooseALeaderRequest.ballot then {
																// TODO consider the inclusion of the StateInfo in Vote in order to keep the StateInfo instances with the highest ballot seen. This would save howAreYou calls to participants for which the StateInfo in the Vote already corresponds to the new ballot. Note that this safe would occur only when restarting the role update due to a higher ballot seen in votes.
																Trace.trace(s"Restarting due to ${if aHigherBallotHaveBeenSeenInVotes then "a higher ballot seen in votes" else "to a ballot bump"}.")
																updateRole(primaryState3)
															} else {
																quorumResult.outcome match {
																	case _: VotingQuorumOutcome_Won =>
																		if assertionsEnabled then assert(myVote2.votedId == boundParticipantId)
																		val electorate3 = deriveElectorateFrom(primaryState3)
																		become(Leader(primaryState3.currentTerm, primaryState3, electorate3, primaryStateFence))
																		sequencer.Capture_unit
																	case _: VotingQuorumOutcome_Lost =>
																		become(Isolated(primaryStateFence))
																		sequencer.Capture_unit
																	case stale: VotingQuorumOutcome_Stale =>
																		Trace.trace(s"Restarting due to stale voting quorum outcome (${stale.higherTerm}, ${stale.higherBallot}).")
																		updateRole(primaryState3)
																}
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
						val electorate2 = deriveElectorateFrom(primaryState2)

						// If my vote is blank, then:
						if myVote.isBlank then {
							// if we are included, then:
							if electorate2.isBoundIncluded then {
								// Advance our commitIndex by absorbing it from a more complete peer and, if successful, restart the role update. This is necessary again here to handle the situation when a concurrent RPC (such as onHowAreYou or onChooseALeader from another peer) updates memorizedPeersInfos with a higher commit index after reconcileDiscoveredState has returned but before primaryState2 is causally anchored.
								if absorbHigherCommitIndexFromPeers(primaryState2, syncStatefulStateInfo(primaryState2)) then updateRole(primaryState2)
								// else become Isolated.
								else {
									become(Isolated(primaryStateFence))
									sequencer.Capture_unit
								}
							}
							// if we are not included, the become Retiring.
							else {
								if assertionsEnabled then assert(electorate2.isInstanceOf[SoleElectorate]) // because exclusion is checked every record and joint electorates are never more restrictive than the contiguous sole ones.
								become(Retiring(primaryState2.currentTerm, electorate2.term, electorate2.changeIndex, electorate2.members))
								sequencer.Capture_unit
							}
						}
						// if my vote is non-blank...
						else {
							val stateInfo2 = syncStatefulStateInfo(primaryState2)
							// ... and no StateInfo has changed, continue the role update knowing the vote is non-blank.
							if stateInfo2.ballot == myVote.ballot then updateRoleKnowingMyNonBlankVote(primaryState2, electorate2, myVote)
							// else start the role process again (superseding this execution).
							else {
								Trace.trace(s"Restarting due to a ballot bump: currentBallot=${stateInfo2.ballot}, myVote.ballot=${myVote.ballot}")
								updateRole(primaryState2)
							}
						}
					}

					/** Starts a role update process by running state discovery, reconciliation, and local vote decision. */
					def start(primaryState1: PrimaryState): sequencer.Capture[Unit] = {
						incumbentUpdateRoleSerial = serial
						memorizedPeersInfos.clear()
						if currentRole ne this then sequencer.Capture_unit
						else {
							val electorate1 = deriveElectorateFrom(primaryState1)
							// If excluded and not leading as ghost, then retire immediately.
							if !electorate1.isBoundIncluded && !this.isInstanceOf[Leader] then {
								this.authorizeQuiescenceIfVanished(electorate1.asInstanceOf[SoleElectorate])
								become(Retiring(primaryState1.currentTerm, electorate1.term, electorate1.changeIndex, electorate1.members))
								sequencer.Capture_unit
							} else {
								val stateInfo1 = syncStatefulStateInfo(primaryState1)
								for {
									discoveryResult <- discoverPeersState(electorate1, stateInfo1)
									_ <- {
										if haveToAbort then sequencer.Capture_unit
										else for {
											reconciliation <- reconcileDiscoveredState(electorate1, discoveryResult)
											_ <- {
												if haveToAbort then sequencer.Capture_unit
												else reconciliation match {
													case DiscoveryReconciledRoleChanged => sequencer.Capture_unit
													case DiscoveryReconciledRestart(ps, reason) =>
														Trace.trace(s"Restarting updateRole due to reconciliation: $reason")
														updateRole(ps)

													case DiscoveryReconciledReady(ps, cfg, si) =>
														val myVote = decideMyVote(ps, cfg, si, discoveryResult.outcome)
														for {
															primaryState2 <- primaryStateFence.causalAnchor()
															_ <- {
																if haveToAbort then sequencer.Capture_unit
																else updateRoleKnowingMyVote(primaryState2, myVote)
															}
														} yield ()
												}
											}
										} yield ()
									}
								} yield ()
							}
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
										for primaryState <- stateful.primaryStateFence.causalAnchor() yield Unable(nextAttemptFlag, deriveElectorateFrom(primaryState).otherProbableParticipants)

									case retiring: Retiring =>
										sequencer.Keeper(Unable(
											nextAttemptFlag,
											ListSet.newBuilder[ParticipantId].addAll(retiring.excludingElectorate).addAll(cluster.getOtherProbableParticipants).result()
										))
									case stateless =>
										sequencer.Keeper(Unable(nextAttemptFlag, cluster.getOtherProbableParticipants))
								}
							}

						}
					} yield result
				}
			}


			/** Derives the active [[Electorate]] state from the current [[PrimaryState]] and [[commitIndex]].
			 * Depends on, and updates, the [[latestDerivedElectorate]]. Also updates other derived state.
			 *
			 * CAUTION: the provided [[PrimaryState]] instance must be the current one. So, this method must be called only within the synchronous part of consumers subscribed synchronously to the [[sequencer.Capture]] returned by either [[sequencer.CausalFence.advance]]-like or [[sequencer.CausalFence.causalAnchor]] methods, passing the [[PrimaryState]] provided to the consumer. This requirement is needed because this method's side effects update derived state.
			 *  @note Accessing the current [[Electorate]] through this method ensures that the current [[Electorate]] is updated before any other derived-state update that depend on it.
			 * @param currentPrimaryState the current [[PrimaryState]].
			 * @return a [[Electorate]] derived from the provided [[PrimaryState]]. */
			final def deriveElectorateFrom(currentPrimaryState: PrimaryState)(using Context): Electorate = {
				assert(primaryStateFence.committedState.is(currentPrimaryState)) // Fails if the primaryStateFence was touched after yielding the PrimaryState instance received as parameter.

				val indexOfLatestElectorateChange = currentPrimaryState.indexOfLatestElectorateChange
				if indexOfLatestElectorateChange == 0 then latestDerivedElectorate.get
				else {
					val oldElectorate = latestDerivedElectorate.get
					val activeElectorateChange: ElectorateChange[ParticipantId] | Null = currentPrimaryState.latestElectorateChange.get match {
						case sec: SoleElectorateChange[ParticipantId] @unchecked =>
							if commitIndex >= indexOfLatestElectorateChange then sec
							else if sec.isCoupleOf(oldElectorate.backingElectorateChange) then null // null indicates to keep the current electorate as the active one.
							else sec.recreateCouple

						case jec: JointElectorateChange[ParticipantId] @unchecked =>
							jec
					}
					if activeElectorateChange == null || activeElectorateChange == oldElectorate.backingElectorateChange then oldElectorate
					else {
						val newElectorate = Electorate_from(activeElectorateChange, indexOfLatestElectorateChange, cluster)
						// Update the derived state stored in the `currentRole` instance. Only the Leader role has such state as of this writing.
						currentRole.handleActiveElectorateChange(currentPrimaryState, oldElectorate, newElectorate, indexOfLatestElectorateChange)
						latestDerivedElectorate = Maybe(newElectorate)
						// Inform the cluster service and notify the listeners about the electorate change.
						cluster.onActiveElectorateChanged(activeElectorateChange, indexOfLatestElectorateChange, currentRole.ordinal)
						notifyListeners(_.onActiveElectorateChanged(currentRole.ordinal, currentPrimaryState.currentTerm, indexOfLatestElectorateChange, activeElectorateChange))
						newElectorate
					}
				}
			}

			/** Queues an updater of the [[PrimaryState.currentTerm]] that does the following:
			 * - updates the [[PrimaryState.currentTerm]] if the provided [[Term]] is higher than it at the moment the updater is executed.
			 * - if the role is sensible to term updates, the [[currentRole]] is changed.
			 * @param seenTerm the [[Term]] to update the [[PrimaryState]] with, provided it is higher than the [[PrimaryState.currentTerm]] when the queued updater is executed.
			 * @param previousTermRef the [[Term]] value in this reference object is overwritten with the [[PrimaryState.currentTerm]] corresponding to the [[PrimaryState]] before the causally anchored advance is performed.
			 * @note About the safety of reusing the same [[TermRef]] instance for different calls: The value is guaranteed to reflect the expected value provided it is read within the synchronous part of a synchronously subscribed consumer to the [[sequencer.Capture]] returned by [[updateTermIfLessThan]]. See the game-changing-invariant in [[Doer.CausalFence]]. */
			final protected def updateTermIfLessThan(seenTerm: Term, previousTermRef: TermRef = defaultPreviousTermRef)(using Trace.Context): sequencer.Capture[PrimaryState] =
				Trace.step("updateTermIfLessThan") {
					for primaryState1 <- primaryStateFence.advanceIf { (primaryState0: PrimaryState) =>
						previousTermRef.elem = primaryState0.currentTerm
						if currentRole.isInstanceOf[StatefulRole] && seenTerm > primaryState0.currentTerm then Maybe(primaryState0.withTermUpdated(seenTerm))
						else Maybe.empty
					} yield {
						if primaryState1.currentTerm > previousTermRef.elem then currentRole match {
							case stateful: StatefulRole => stateful.onTermUpdated(primaryState1, Maybe.empty)
							case _ =>
						}
						primaryState1
					}
				}

			final private def memorizedPeersInfosToArray(currentElectorate: Electorate): IArray[StateInfo] = {
				currentElectorate.peers.mapWithIndex { (peerId, _) => memorizedPeersInfos.get(peerId) }
			}

			/** Called by [[updateTermIfLessThan]] and [[onInstallSnapshot]] when the [[PrimaryState.currentTerm]] is updated because a higher term was observed.\
			 * The implementation should not mutate the [[PrimaryState]]
			 * @param primaryState the current [[PrimaryState]]
			 * @param maybeLeaderId the identifier of the leader, if known. */
			def onTermUpdated(primaryState: PrimaryState, maybeLeaderId: Maybe[ParticipantId])(using Context): Unit = ()
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

			override def requestElectorateChange(requestId: ElectorateChangeRequestId, desiredParticipantsSet: Set[ParticipantId], priorAnswer: Maybe[ElectorateChangeResponse])(using Trace.Context): sequencer.Capture[ElectorateChangeResponse] = {
				var myCurrentStateInfo = syncStatelessStateInfo()
				// If a prior answer is provided, update the current ballot and memorizedPeersInfos
				priorAnswer.foreach {
					case nonTerminal: NonTerminalElectorateChangeResponse =>
						if updateBallotIfLowerThan(myCurrentStateInfo, nonTerminal.latestBallotSeen) then myCurrentStateInfo = syncStatelessStateInfo()
					case _: TerminalElectorateChangeResponse =>
				}
				sequencer.Keeper(new STOPPED(myCurrentStateInfo.ballot))
			}
		}

		private def Quiesced(motive: Try[String]): Maybe[Quiesced] = {
			if currentRole.ordinal == QUIESCED then Maybe.empty
			else Maybe(new Quiesced(motive))
		}

		//// RETIRING ////

		/** A transitional [[Role]] before [[Quiesced]] to which a participant transitions to when a [[SoleElectorateChange]] that excludes it becomes active.\
		 * The life of this [[Role]] last until a stable [[Leader]] of a subsequent [[Term]] authorizes this participant to quiesce.\
		 * This [[Role]] is part of the **Retiring Quorum-Buffering** mechanism.\
		 * The purpose of this mechanism is to maintain the quorum safety of the old participant set during joint consensus.\
		 * By holding excluded participants in the [[RETIRING]] role, the system ensures they contribute toward the old set's quorum. Although they do not cast a specific vote, they effectively lower the required threshold of active votes by one, acting as a neutral "don't care" participant until a succeeding leader establishes a stable majority in the new electorate.\
		 * Since this role must exist for that reason, we also take advantage of its presence to wait for the retirement pipelines to conclude their job. In this scenario, the job of the retirement pipelines of this retiring ex-leader will overlap with the job of the retirement pipelines of the succeeding [[Leader]], but, if I am not mistaken, this overlap is more beneficial than harmful because it removes some burden to the new [[Leader]].\
		 * @param finalTerm the [[Term]] during which this participant became [[Retiring]]. Used only as argument for the [[NotificationListener.onRetiring]] method, and [[AppendResult]] responses.
		 * @param termAtExcludingElectorateIndex the [[Term]] of the [[SoleElectorateChange]] that excluded this participant causing its retirement. This is the term that a [[Retiring]] participant exposes in [[StateInfo]] during elections.
		 * @param excludingElectorateIndex the index of the [[SoleElectorateChange]] that excluded this participant causing its retirement.
		 * @param excludingElectorate The electorate of the [[SoleElectorateChange]] that excluded this participant. */
		private final class Retiring(val finalTerm: Term, val termAtExcludingElectorateIndex: Term, val excludingElectorateIndex: RecordIndex, val excludingElectorate: IArray[ParticipantId]) extends Role { thisRetiring =>
			override val ordinal: RoleOrdinal = RETIRING
			override val rank: ElectionRank = ElectionRank_from(RETIRING)

			if assertionsEnabled then assert(!excludingElectorate.contains(boundParticipantId))

			override def handleEnter(previous: Role)(using Trace.Context): Unit = {
				notifyListeners(_.onRetiring(previous.ordinal, finalTerm))
				becomeQuiescedIfEligible(excludingElectorateIndex)
			}


			override def syncLocalStateInfo(maybePrimaryState: Maybe[PrimaryState])(using Context): StateInfo = {
				if stateInfoExposedInLastInteraction.tiesWith(termAtExcludingElectorateIndex, rank, termAtExcludingElectorateIndex, excludingElectorateIndex, termAtExcludingElectorateIndex, excludingElectorateIndex) then {
					if stateInfoExposedInLastInteraction.ballot != currentBallot then stateInfoExposedInLastInteraction = StateInfo(termAtExcludingElectorateIndex, rank, termAtExcludingElectorateIndex, excludingElectorateIndex, termAtExcludingElectorateIndex, excludingElectorateIndex, currentBallot)
				} else {
					currentBallot = currentBallot.bumped
					stateInfoExposedInLastInteraction = StateInfo(termAtExcludingElectorateIndex, rank, termAtExcludingElectorateIndex, excludingElectorateIndex, termAtExcludingElectorateIndex, excludingElectorateIndex, currentBallot)
				}
				stateInfoExposedInLastInteraction
			}

			override def onHowAreYou(inquirerId: ParticipantId, inquirerInfo: StateInfo)(using Trace.Context): sequencer.Capture[StateInfo] = {
				sequencer.Keeper(updateLocalStateInfo(Maybe.empty, inquirerId, inquirerInfo))
			}

			override def onChooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo)(using Trace.Context): sequencer.Capture[Vote[ParticipantId]] = {
				val myStateInfo = updateLocalStateInfo(Maybe.empty, inquirerId, inquirerInfo)
				yieldsBlankVote(termAtExcludingElectorateIndex, myStateInfo.ballot)
			}

			override def requestElectorateChange(requestId: ElectorateChangeRequestId, desiredParticipantsSet: Set[ParticipantId], priorAnswer: Maybe[ElectorateChangeResponse])(using Trace.Context): sequencer.Capture[ElectorateChangeResponse] = {
				var myCurrentStateInfo = syncStatelessStateInfo()
				// If a prior answer is provided, update the current ballot and memorizedPeersInfos
				priorAnswer.foreach {
					case nonTerminal: NonTerminalElectorateChangeResponse =>
						if updateBallotIfLowerThan(myCurrentStateInfo, nonTerminal.latestBallotSeen) then myCurrentStateInfo = syncStatelessStateInfo()
					case _: TerminalElectorateChangeResponse =>
				}
				sequencer.Keeper(new EXCLUDED(myCurrentStateInfo.ballot))
			}

			/** @inheritdoc
			 * This implementation responds with a rejection that propagates the received `attemptFlag`. */
			override def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag)(using Trace.Context): sequencer.Capture[ResponseToClient] = {
				sequencer.Keeper(Unable(
					attemptFlag.withInternalBitsCleared,
					ListSet.newBuilder[ParticipantId].addAll(excludingElectorate).addAll(cluster.getOtherProbableParticipants).result()
				))
			}

			override def onAppendRecords(inquirerId: ParticipantId, inquirerTerm: Term, prevRecordIndex: RecordIndex, prevRecordTerm: Term, batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term)(using Trace.Context): sequencer.Capture[AppendResult] = {
				// Check if the received records contain a later [[JointElectorateChange]] that includes this participant.
				findLastIncludingElectorateChangeIn(prevRecordIndex + 1, batch).fold(
					// If not, return a rejection.
					sequencer.Keeper(AppendResult_Rejected(finalTerm, excludingElectorateIndex + 1, ordinal))
				) { findResult =>
					// If yes, become starting and redirect the append records request to the new role.
					val activeParticipants = ListSet.newBuilder.addAll(findResult.tcc.oldParticipants).addAll(findResult.tcc.newParticipants).result()
					become(Starting(findResult.index, activeParticipants))
						.onAppendRecords(inquirerId, inquirerTerm, prevRecordIndex, prevRecordTerm, batch, leaderCommit, termAtLeaderCommit)
				}
			}

			/** Finds the last [[JointElectorateChange]] that includes this participant among the provided records.
			 * @param offset the [[RecordIndex]] of the first record.
			 * @param records the records to search in. */
			private def findLastIncludingElectorateChangeIn(offset: RecordIndex, records: IArray[Record]): Maybe[(index: RecordIndex, tcc: JointElectorateChange[ParticipantId])] = {
				val excludingElectorateRelativeIndex = (thisRetiring.excludingElectorateIndex - offset).toInt
				var relativeIndex = records.length - 1
				while relativeIndex >= 0 && relativeIndex > excludingElectorateRelativeIndex do {
					records(relativeIndex) match {
						case tcc: JointElectorateChange[ParticipantId] @unchecked if tcc.newParticipants.contains(boundParticipantId) => return Maybe((relativeIndex + offset, tcc))
						case _ => relativeIndex -= 1
					}
				}
				Maybe.empty
			}

			override def onInstallSnapshot(inquirerId: ParticipantId, inquirerTerm: Term, snapshot: SnapshotData[ParticipantId], batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term)(using Trace.Context): sequencer.Capture[AppendResult] = {
				// Check if the received records or the snapshot contain a later [[JointElectorateChange]] that includes this participant.
				findLastIncludingElectorateChangeIn(snapshot.lastIncludedRecordIndex + 1, batch).orElse(
					snapshot.latestElectorateChange match {
						case tcc: JointElectorateChange[ParticipantId] if tcc.newParticipants.contains(boundParticipantId) && snapshot.latestElectorateChangeIndex > excludingElectorateIndex => Maybe((snapshot.lastIncludedRecordIndex, tcc))
						case _ => Maybe.empty
					}
				).fold(
					// If not, return a rejection.
					sequencer.Keeper(AppendResult_Rejected(finalTerm, excludingElectorateIndex + 1, ordinal))
				) { findResult =>
					// If yes, become starting and redirect the append records request to the new role.
					val activeParticipants = ListSet.newBuilder.addAll(findResult.tcc.oldParticipants).addAll(findResult.tcc.newParticipants).result()
					become(Starting(findResult.index, activeParticipants))
						.onInstallSnapshot(inquirerId, inquirerTerm, snapshot, batch, leaderCommit, termAtLeaderCommit)
				}
			}

			override def onQuiescencePermitted(grantorId: ParticipantId, indexOfGrantedSoleElectorateChange: RecordIndex)(using Trace.Context): Unit = {
				if indexOfGrantedSoleElectorateChange > indexOfSecForWhichQuiescenceWasPermitted then {
					quiescenceGrantor = Maybe(grantorId)
					indexOfSecForWhichQuiescenceWasPermitted = indexOfGrantedSoleElectorateChange
					becomeQuiescedIfEligible(excludingElectorateIndex)
				}
			}
		}

		/**
		 * @param finalTerm the [[Term]] during which this participant became [[Retiring]]. Used only as argument for the [[NotificationListener.onRetiring]] method, and [[AppendResult]] responses.
		 * @param termAtExcludingElectorateIndex the [[Term]] of the [[SoleElectorateChange]] that excluded this participant causing its retirement. This is the term that a [[Retiring]] participant exposes in [[StateInfo]] during elections.
		 * @param excludingElectorateIndex the index of the [[SoleElectorateChange]] that excluded this participant causing its retirement.
		 * @param excludingElectorateElectorate the electorate of the [[SoleElectorateChange]] that excluded this participant. */
		private def Retiring(finalTerm: Term, termAtExcludingElectorateIndex: Term, excludingElectorateIndex: RecordIndex, excludingElectorateElectorate: IArray[ParticipantId]): Maybe[Retiring] = {
			currentRole match {
				case retiring: Retiring if retiring.excludingElectorateIndex == excludingElectorateIndex && retiring.termAtExcludingElectorateIndex == termAtExcludingElectorateIndex && retiring.finalTerm == finalTerm => Maybe.empty
				case _ => Maybe(new Retiring(finalTerm, termAtExcludingElectorateIndex, excludingElectorateIndex, excludingElectorateElectorate))
			}
		}

		//// STARTING ////

		/** The behavior when the participant has the [[STARTING]] role. This is a transitory role during which the participant state is initialized.
		 * When initialization is completed it transitions to the [[Isolated]] state.
		 * @param indexOfTheIncludingElectorateChange the [[RecordIndex]] of the [[JointElectorateChange]] that caused this [[ConsensusParticipant]] service to join.
		 * @param participantsInTheIncludingElectorateChange the active participants in the [[JointElectorateChange]] pointed by `indexOfTheIncludingElectorateChange`.
		 * TODO consider adding a parameter with the set of active participants in the including [[ElectorateChange]], to pass it to the Joining role, in order to return a more updated set of active participants when responding with [[Unable]] to a command from a client. */
		private final class Starting(val indexOfTheIncludingElectorateChange: RecordIndex, participantsInTheIncludingElectorateChange: ListSet[ParticipantId]) extends Role {
			override val ordinal: RoleOrdinal = STARTING
			override val rank: ElectionRank = ElectionRank_from(STARTING)
			/** Is fulfilled after initializing this [[ConsensusParticipant]] and becoming another [[Role]]: [[Joining]], [[Isolated]], or [[Quiesced]]. */
			private val startingCompletedCovenant: sequencer.Captor[Maybe[PrimaryState]] = sequencer.Captor()

			override def handleEnter(previous: Role)(using Trace.Context): Unit = {
				Trace.step("Starting.onEnter") {
					notifyListeners(_.onStarting(previous.ordinal, indexOfTheIncludingElectorateChange))

					val startupCapture = for {
						loadedWorkspace <- storage.load
						recoveredHighestAppliedCommandIndex <- machine.recoverIndexOfLastAppliedCommand
					} yield (loadedWorkspace, recoveredHighestAppliedCommandIndex)

					startupCapture.triggerSync(new sequencer.MonoObserver[(WS, RecordIndex)] {
						override def onSuccess(startupData: (WS, RecordIndex)): Unit = {
							val (loadedWorkspace, recoveredHighestAppliedCommandIndex) = startupData
							val primaryState = new PrimaryState(loadedWorkspace, coordinatingStorage)
							val indexOfLatestElectorateChange = primaryState.indexOfLatestElectorateChange
							val snapshotCommitIndex = loadedWorkspace.latestSnapshot.fold(0L)(_.lastIncludedRecordIndex)
							highestAppliedCommandIndex = recoveredHighestAppliedCommandIndex
							commitIndex = recoveredHighestAppliedCommandIndex.max(snapshotCommitIndex)
							val rulingElectorateChange = {
								if indexOfLatestElectorateChange == 0 then {
									loadedWorkspace.setTermAndVote(PRE_INIT, Maybe.empty)
									new JointElectorateChange[ParticipantId](PRE_INIT, "Initial-Electorate", Set.empty, cluster.getInitialParticipants) // TODO consider using the set provided in the Starting constructor instead, and remove the `getInitialParticipants` method.

								} else primaryState.latestElectorateChange.get match {
									// If the top electorate change in the log is a sole-kind one, then the previous joint electorate change rules until the commitIndex crosses the index of the top sole-kind one.
									case sec: SoleElectorateChange[ParticipantId] @unchecked =>
										if commitIndex >= indexOfLatestElectorateChange then sec
										else sec.recreateCouple

									// If the top electorate change in the log is a joint one, then it rules immediately.
									case jec: JointElectorateChange[ParticipantId] @unchecked =>
										jec
								}
							}
							val electorate = Electorate_from(rulingElectorateChange, indexOfLatestElectorateChange, cluster)
							val isSeed = indexOfTheIncludingElectorateChange == 0
							if isSeed && !electorate.isBoundIncluded then {
								become(Quiesced(Success(s"Start-up aborted because this ConsensusParticipant instance does not belong to the active cluster-electorate.")))
								startingCompletedCovenant.captureSync(Maybe.empty)
							}
							else {
								latestDerivedElectorate = Maybe(electorate)
								val primaryStateFence = CausalFence[PrimaryState, sequencer.type](sequencer)(primaryState)
								notifyListeners(_.onStarted(previous.ordinal, primaryState.currentTerm, rulingElectorateChange, isSeed))
								if isSeed then become(Isolated(primaryStateFence))
								else become(Joining(primaryStateFence, indexOfTheIncludingElectorateChange, participantsInTheIncludingElectorateChange))
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

			override def requestElectorateChange(requestId: ElectorateChangeRequestId, desiredParticipantsSet: Set[ParticipantId], priorAnswer: Maybe[ElectorateChangeResponse])(using Trace.Context): sequencer.Capture[ElectorateChangeResponse] = {
				for {
					_ <- startingCompletedCovenant
					response <- currentRole.requestElectorateChange(requestId, desiredParticipantsSet, priorAnswer)
				} yield response
			}
		}

		private def Starting(indexOfTheIncludingElectorateChange: RecordIndex, participantsInTheIncludingElectorateChange: ListSet[ParticipantId]): Maybe[Starting] = {
			currentRole match {
				case starting: Starting if starting.indexOfTheIncludingElectorateChange == indexOfTheIncludingElectorateChange => Maybe.empty
				case _ => Maybe(new Starting(indexOfTheIncludingElectorateChange, participantsInTheIncludingElectorateChange))
			}
		}

		//// JOINING ////

		private final class Joining(psf: CausalFence[PrimaryState, sequencer.type], val indexOfTheIncludingElectorateChange: RecordIndex, participantsInTheIncludingElectorateChange: ListSet[ParticipantId]) extends StatefulRole(psf) { thisJoining =>
			/** The ordinal corresponding to this [[Role]] */
			override val ordinal: RoleOrdinal = JOINING
			override val rank: ElectionRank = ElectionRank_from(JOINING)

			override def handleEnter(previous: Role)(using Trace.Context): Unit = {
				notifyListeners(_.onJoining(previous.ordinal, indexOfTheIncludingElectorateChange))
			}

			override def decideMyVote(primaryState: PrimaryState, electorate: Electorate, stateInfo: StateInfo, outcome: DiscoveryQuorumOutcome)(using Trace.Context): Vote[ParticipantId] = {
				blankVote(primaryState.currentTerm, stateInfo.ballot)
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
				sequencer.Keeper(Unable(attemptFlag.withInternalBitsCleared, participantsInTheIncludingElectorateChange))
			}

			override def requestElectorateChange(primaryState: PrimaryState, requestId: ElectorateChangeRequestId, desiredParticipants: Set[ParticipantId], ballotWasUpdated: Boolean)(using Trace.Context): sequencer.Capture[ElectorateChangeResponse] = {
				sequencer.Keeper(new CATCHING_UP(syncStatefulStateInfo(primaryState).ballot))
			}
		}

		private def Joining(psf: CausalFence[PrimaryState, sequencer.type], indexOfTheIncludingElectorateChange: RecordIndex, participantsInTheIncludingElectorateChange: ListSet[ParticipantId]): Maybe[Joining] = {
			currentRole match {
				case joining: Joining if joining.indexOfTheIncludingElectorateChange == indexOfTheIncludingElectorateChange && (joining.primaryStateFence eq psf) => Maybe.empty
				case _ => Maybe(new Joining(psf, indexOfTheIncludingElectorateChange, participantsInTheIncludingElectorateChange))
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

			override def requestElectorateChange(primaryState0: PrimaryState, requestId: ElectorateChangeRequestId, desiredParticipants: Set[ParticipantId], ballotWasUpdated: Boolean)(using Context): sequencer.Capture[ElectorateChangeResponse] = {
				Trace.trace(s"Updating role from ${RoleOrdinal_nameOf(currentRole.ordinal)} due toan electorate change request. ")
				for {
					_ <- updateRole(primaryState0) // TODO consider making updateRole return the current primary state, so that the causalAnchor method call is not needed here (and other places also).
					primaryState <- primaryStateFence.causalAnchor()
					response <- {
						if currentRole ne thisIsolated then currentRole.requestElectorateChange(requestId, desiredParticipants, Maybe.empty)
						else sequencer.Keeper(new SECLUDED(syncStatefulStateInfo(primaryState).ballot))
					}
				} yield response
			}
		}

		private def Isolated(psf: CausalFence[PrimaryState, sequencer.type]): Maybe[Isolated] = {
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
		 * It behaves as [[Isolated]] except that, in the [[handleEnter]] life-cycle stage it enqueues an updater of the [[PrimaryState.currentTerm]] that sets it to the latest [[Term]] seen if not already; and then transitions to [[Retiring]] if this participant is excluded from the active [[Electorate]], or to [[Isolated]] otherwise.
		 *
		 * @param endedTerm the [[Term]] that concluded, during which this participant acted as [[Leader]].
		 * TODO Consider replacing this class with a method that transitions to [[Isolated]] or [[Retiring]] in a synchronous manner, and then enqueues a term update. The problem with the current class approach is the incorrect isolated-like behavior during the transition to retiring.
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
							// check if excluded from the new electorate.
							val updatedElectorate = deriveElectorateFrom(primaryState1)
							// if excluded, become Retiring
							if !updatedElectorate.isBoundIncluded then become(Retiring(primaryState1.currentTerm, updatedElectorate.term, updatedElectorate.changeIndex, updatedElectorate.members))
							// else, become Isolated
							else become(Isolated(primaryStateFence))
						}
					}

					override def onError(e: Throwable): Unit = become(Quiesced(Failure(e)))
				})
			}
		}

		private def Abdicating(endedTerm: Term, latestTermSeen: Term, psf: CausalFence[PrimaryState, sequencer.type]): Maybe[Abdicating] = {
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

			override def requestElectorateChange(primaryState0: PrimaryState, requestId: ElectorateChangeRequestId, desiredParticipants: Set[ParticipantId], ballotWasUpdated: Boolean)(using Context): sequencer.Capture[ElectorateChangeResponse] = {
				Trace.trace(s"Updating role from ${RoleOrdinal_nameOf(currentRole.ordinal)} due toan electorate change request.")
				for {
					_ <- updateRole(primaryState0)
					primaryState1 <- primaryStateFence.causalAnchor()
					response <- {
						if currentRole ne thisFollower then currentRole.requestElectorateChange(requestId, desiredParticipants, Maybe.empty)
						else sequencer.Keeper(new ASK_THE_LEADER(followeeId, syncStatefulStateInfo(primaryState1).ballot))
					}
				} yield response
			}

			override def onTermUpdated(primaryState: PrimaryState, maybeLeaderId: Maybe[ParticipantId])(using Context): Unit = {
				become(maybeLeaderId.fold(Isolated(primaryStateFence))(Follower(primaryState.currentTerm, _, primaryStateFence)))
			}
		}

		private def Follower(term: Term, leaderId: ParticipantId, psf: CausalFence[PrimaryState, sequencer.type]): Maybe[Follower] = {
			currentRole match {
				case follower: Follower if follower.term == term && follower.followeeId == leaderId && (follower.primaryStateFence eq psf) => Maybe.empty
				case _ => Maybe(new Follower(term, leaderId, psf))
			}
		}

		//// LEADER ////

		/**
		 * Behavior when the participant has the [[LEADER]] role. Taken when reachability to a majority of the participants is achieved, none of them is a [[Leader]] with higher or equal term, and wins the new leader election.
		 *
		 * In this state, the participant coordinates consensus decisions.
		 * @param leadedTerm the [[Term]] owned by this [[Leader]] instance.
		 * @param initialPrimaryState the current [[PrimaryState]] when this [[Leader]] instance was created. Intended to be used in the [[handleEnter]] method only. Do not use elsewhere.
		 * @param initialElectorate the active [[Electorate]] when this [[Leader]] instance was created. Intended to be used in the [[handleEnter]] method only. Do not use elsewhere.
		 * @param wsf the [[CausalFence]] that must be used to ensure causal ordering of the state updates. It must be propagated to subsequent [[StatefulRole]] instances.
		 * TODO replace the `initialPrimaryState` parameter with what is obtained from it. Storing an instance of [[PrimaryState]] is error prone.
		 */
		private final class Leader(val leadedTerm: Term, initialPrimaryState: PrimaryState, initialElectorate: Electorate, wsf: CausalFence[PrimaryState, sequencer.type]) extends StatefulRole(wsf) { thisLeader =>
			/** The outcome of the [[sequencer.Capture]] returned by a call to [[ClusterParticipant.appendRecords]]. */
			private type AppendResponse = (Int, AppendResult)

			override val ordinal: RoleOrdinal = LEADER
			override val rank: ElectionRank = ElectionRank_from(LEADER)

			override def diagnosticInfo: RoleDiagnostic = {
				val electorate = latestDerivedElectorate.get
				val progress = electorate.peers.mapWithIndex { (peerId, idx) =>
					val lp = learnerProgressByIndex(idx)
					PeerProgressDiagnostic(peerId, lp.highestRecordIndexKnownToBeAppended, lp.highestRecordIndexKnowToBeCommitted)
				}
				LeaderRoleDiagnostic(ordinal, electorate.backingElectorateChange, progress)
			}

			private var learnerProgressByIndex: IArray[LearnerProgress] = IArray.tabulate(initialElectorate.peers.size)(_ => new LearnerProgress(initialPrimaryState.firstEmptyRecordIndex))

			/** Either, the index of the [[SoleElectorateChange]] that excluded this leading participant causing it become a ghost leader, or zero if in joint consensus or not excluded.
			 * Set by the [[Leader.driveTheRetirements]] method, which is called by [[deriveElectorateFrom]] when the active [[Electorate]] changes from a [[JointElectorate]] to a [[SoleElectorate]]. */
			private var indexOfElectorateChangeThatExcludedThisParticipant: RecordIndex = 0

			private def RecordBecomesCommittedCaptor(targetIndex: RecordIndex): RecordBecomesCommittedCaptor = new RecordBecomesCommittedCaptor(targetIndex)

			private class RecordBecomesCommittedCaptor(val targetIndex: RecordIndex) extends sequencer.Captor[PrimaryState]

			private val recordBecomesCommittedCaptors: mutable.ArrayBuffer[RecordBecomesCommittedCaptor] = mutable.ArrayBuffer.empty

			private var pendingElectorateChangesCompletion: sequencer.Capture[ElectorateChangeResponse] = sequencer.Keeper(new SUCCESSFULLY_CHANGED)

			override def handleEnter(previous: Role)(using Trace.Context): Unit = {
				Trace.step("Leader.onEnter") {
					notifyListeners(_.onBecameLeader(previous.ordinal, leadedTerm))

					val indexOfLatestElectorateChange = initialPrimaryState.indexOfLatestElectorateChange
					// if the log lacks a ElectorateChange record (is empty), create a synthetic one with the seed participants of the initial synthetic electorate (appointed in `latestDerivedElectorate` during Starting).
					if indexOfLatestElectorateChange == 0 then {
						for primaryState1 <- primaryStateFence.advance { primaryState0 =>
							primaryState0.withSingleRecordAppended(primaryState0.currentTerm, latestDerivedElectorate.get.backingElectorateChange)
						} yield appendALocalSecAndReplicateIt(latestDerivedElectorate.get.backingElectorateChange.asInstanceOf[JointElectorateChange[ParticipantId]], 1)
					}
					// if the log contains a ElectorateChange then:
					else initialPrimaryState.latestElectorateChange.get match {
						// If the top electorate change in the local log is a joint-kind one, continue the electorate transition process. This happens when the leader that started the first phase of the electorate change crashed or left the leadership before achieving the replication of the JointElectorateChange to a majority, or while storing the SoleElectorateChange in his persistent log.
						case tcc: JointElectorateChange[ParticipantId @unchecked] =>
							pendingElectorateChangesCompletion = continueWithSecondPhase(tcc, indexOfLatestElectorateChange)

						// If, on the contrary, is a sole-kind one
						case scc: SoleElectorateChange[ParticipantId @unchecked] =>
							// ... and it was committed (commitIndex >= its index in the log), program the driving of excluded participants to retirement.
							if commitIndex >= indexOfLatestElectorateChange then thisLeader.driveTheRetirements(initialPrimaryState, Maybe.empty, scc, indexOfLatestElectorateChange)
							// ... and it wasn't committed (commitIndex < its index in the log), drive its commitment eagerly.
							else {
								pendingElectorateChangesCompletion = for {
									isSecReplicated <- replicateSec(initialPrimaryState, indexOfLatestElectorateChange)
									response <- {
										if isSecReplicated then sequencer.Keeper[ElectorateChangeResponse](new SUCCESSFULLY_CHANGED)
										else for primaryState1 <- primaryStateFence.causalAnchor() yield new REQUEST_TRACKING_LOST_AFTER_SECOND_PHASE_STARTED(currentRole.syncLocalStateInfo(Maybe(primaryState1)).ballot)
									}
								} yield response
							}
							
					}
				}
			}

			override def handleExit()(using Trace.Context): Unit = {
				learnerProgressByIndex.foreach(_.unreachableRetryWakeUpToken.foreach(_.cancel()))
				retiringLearnersById.values.foreach(_.unreachableRetryWakeUpToken.foreach(_.cancel()))
				retryPermitQuiescenceWakeUpToken.foreach(_.cancel())
				retryPermitQuiescenceWakeUpToken = Maybe.empty
				val awaitersToSeize = recordBecomesCommittedCaptors.toArray
				recordBecomesCommittedCaptors.clear()
				super.handleExit()

				// Decoupled Mutation Contract / Re-Entrance Prevention: Observers of `recordBecomesCommittedCaptors` (primarily `handleCommandReplication`) react to abdication (`currentRole ne thisLeader`) by immediately delegating vacated in-flight commands to `currentRole.onCommandFromClient(..., INTERNAL_VACATE_HANDOFF)`. If awaiters were seized synchronously here, that delegation would execute re-entrance inside `handleExit`, triggering `updateRole` and mutating the primary state fence while the outer caller (e.g., `onAppendRecords`) is still suspended on the call stack. Deferring seizure to `sequencer.run` ensures the outer role transition and RPC turn complete atomically before vacated commands are handled.
				val dummyPrimaryState = sequencer.Capture_ready(initialPrimaryState) // Any PrimaryState instance is valid because continuations discard the payload when `currentRole ne thisLeader`.
				if awaitersToSeize.nonEmpty then {
					sequencer.run {
						awaitersToSeize.foreach(_.seizeWith(dummyPrimaryState, true))
					}
				}
			}

			def isGhost: Boolean = indexOfElectorateChangeThatExcludedThisParticipant > 0

			/** @inheritdoc
			 *  This implementation does two different things:
			 *  1) Updates the [[retirementDriverByParticipantId]] map to include any new old-electorate-only retiring participant (those that are not part of the new [[Electorate]], but still need more appends until their [[commitIndex]] reaches the index of the [[SoleElectorateChange]] that excluded them).
			 *  2) Recreates and initializes the [[learnerProgressByIndex]] array keeping the elements corresponding to the participants that remain and moving them to the appropriate index.
			 * @param oldElectorate the [[Electorate]] that determines which participants corresponds to each element of the [[learnerProgressByIndex]] array before the transition.
			 * @note This rearrangement wouldn't be necessary if maps instead of arrays were used. But considering these two collections are heavily used, efficiency was primed. */
			override def handleActiveElectorateChange(currentPrimaryState: PrimaryState, oldElectorate: Electorate, newElectorate: Electorate, indexOfNewElectorateChange: RecordIndex)(using Context): Unit = Trace.step("handleActiveElectorateChange") {
				// Stop and remove retirement pipelines for any participant that is an active peer in the new electorate.
				retiringLearnersById.filterInPlace { (retireeId, learnerProgress) =>
					if newElectorate.activeParticipants.contains(retireeId) then {
						learnerProgress.unreachableRetryWakeUpToken.foreach(_.cancel())
						false
					} else true
				}

				// Step one. Must be before step two.
				newElectorate.backingElectorateChange.match {
					case scc: SoleElectorateChange[ParticipantId] =>
						thisLeader.driveTheRetirements(currentPrimaryState, Maybe(oldElectorate), scc, indexOfNewElectorateChange)
					case tcc: JointElectorateChange[ParticipantId] =>
						// If a previous electorate change excluded this leading participant but a later electorate change includes it, clear the mark that instructs itself to retire (when it sees that the previous electorate change is committed).
						if indexOfElectorateChangeThatExcludedThisParticipant > 0 && newElectorate.isBoundIncluded then indexOfElectorateChangeThatExcludedThisParticipant = 0
				}

				// Step two
				val newAllOtherParticipantsArrayLength = newElectorate.peers.length
				val newLearnerProgressByIndex: Array[LearnerProgress] = new Array(newAllOtherParticipantsArrayLength)

				var participantNewIndex = newAllOtherParticipantsArrayLength
				while participantNewIndex > 0 do {
					participantNewIndex -= 1
					val participantId = newElectorate.peers(participantNewIndex)
					val participantOldIndex = oldElectorate.peerIndexOf(participantId)
					if participantOldIndex >= 0 then {
						newLearnerProgressByIndex(participantNewIndex) = learnerProgressByIndex(participantOldIndex)
					} else {
						newLearnerProgressByIndex(participantNewIndex) = new LearnerProgress(indexOfNewElectorateChange)
					}
				}
				learnerProgressByIndex = IArray.unsafeFromArray(newLearnerProgressByIndex)
			}

			/** Drives the excluded participants (the ones that are not active in the provided [[SoleElectorateChange]]) to retirement.
			 *		- If this [[Leader]] is excluded, sets the threshold [[indexOfElectorateChangeThatExcludedThisParticipant]]. The replication logic checks it after successful appends to decide if a transition to the [[Retiring]] [[Role]] is needed.
			 *		- Registers and starts a retirement pipeline via [[driveReplicationPipeline]] for each excluded follower that needs more appends to become [[Retiring]].
			 * Must be called a single time whenever the participant becomes [[Leader]] with a [[SoleElectorate]] or the participant is leading and the active [[Electorate]] transitions to a [[SoleElectorate]].
			 *
			 * @param primaryState the [[PrimaryState]] from which the transition is derived.
			 * @param maybeStandingElectorate the [[Electorate]] on which the [[Leader]] derived state is based, or [[Maybe.empty]] to indicate [[thisLeader]] is brand new (called from [[Leader.handleEnter]]). It's [[Electorate.backingElectorateChange]] may be the same as the received in the `soleElectorateChange` parameter. It is needed to know what is in each element of the [[indexOfNextRecordToSend_ByParticipantIndex]] and [[highestRecordIndexKnownToBeAppended_ByParticipantIndex]].
			 * @param soleElectorateChange the [[SoleElectorateChange]] that might exclude participants.
			 * @param soleElectorateChangeIndex the log index where the provided [[SoleElectorateChange]] is stored. */
			private def driveTheRetirements(primaryState: PrimaryState, maybeStandingElectorate: Maybe[Electorate], soleElectorateChange: SoleElectorateChange[ParticipantId], soleElectorateChangeIndex: RecordIndex)(using Context): Unit = Trace.step("driveTheRetirements") {
				assert(commitIndex >= soleElectorateChangeIndex && maybeStandingElectorate.fold(true)(_.isInstanceOf[JointElectorate]))

				// Stop pipelines for participants that become included.
				retiringLearnersById.filterInPlace { (retireeId, learnerProgress) =>
					if soleElectorateChange.newParticipants.contains(retireeId) then {
						learnerProgress.unreachableRetryWakeUpToken.foreach(_.cancel())
						false
					} else true
				}

				// Find out which are the participants that become excluded.
				val newRetiringParticipants = soleElectorateChange.oldParticipants.diff(soleElectorateChange.newParticipants)
				val notNewParticipants = newRetiringParticipants.union(cluster.getOtherProbableParticipants).diff(soleElectorateChange.newParticipants)

				// If this leader is excluded, set the threshold until which this leader will continue leading as a ghost.
				if newRetiringParticipants.contains(boundParticipantId) then thisLeader.indexOfElectorateChangeThatExcludedThisParticipant = soleElectorateChangeIndex
				// If this leader continues as a stable leader (not a ghost), authorize the quiescence of the retiring followers.
				else authorizeQuiescenceTo(notNewParticipants, soleElectorateChangeIndex, false)

				/** Creates and registers a [[LearnerProgress]] tracker and starts the continuous replication pipeline for the specified participant. */
				def start(participantId: ParticipantId, optimisticIndexOfNextRecordToSend: RecordIndex): Unit = {
					val learnerProgress = new LearnerProgress(optimisticIndexOfNextRecordToSend)
					learnerProgress.excludingSecIndex = soleElectorateChangeIndex
					retiringLearnersById.put(participantId, learnerProgress)
					driveReplicationPipeline(primaryState, participantId, learnerProgress, 0, isRetiree = true)
				}

				// Start a replication pipeline for each participant that both, is not included, and we are not certain that it has committed the `soleElectorateChange`.
				maybeStandingElectorate.fold(
					// Logic for a brand-new Leader: Start a pipeline for all the excluded peers, each of which starts sending the `soleElectorateChange` record only.
					notNewParticipants.foreach { participantId =>
						if participantId != boundParticipantId then start(participantId, soleElectorateChangeIndex)
					}
				) { standingElectorate => // Logic for an incumbent Leader: Start a pipeline for all the excluded peers that haven't already committed the `soleElectorateChange`, each of which starts sending the records from the `indexOfNextRecordToSend` up to `soleElectorateChangeIndex`.
					val initialLearnerProgressByIndex = learnerProgressByIndex
					standingElectorate.peers.foreachWithIndex { (participantId, participantIndex) =>
						if initialLearnerProgressByIndex(participantIndex).highestRecordIndexKnowToBeCommitted < soleElectorateChangeIndex && newRetiringParticipants.contains(participantId)
						then start(participantId, initialLearnerProgressByIndex(participantIndex).optimisticIndexOfNextRecordToSend)
					}
				}
			}

			/** Starts a process that insistently authorizes the quiescence of the participants specified in this and previous calls; allowing them to transition from [[Retiring]] to [[Quiesced]] provided they retire due to being excluded by a [[SoleElectorateChange]] at the `permittedElectorateChangeIndex`.
			 * @param peers the participants to authorize the quiescence of.
			 * @param permittedElectorateChangeIndex the index of the [[SoleElectorateChange]] for which the quiescence is authorized. The destination participant will quiesce only if it reaches the [[Retiring]] state with a [[Retiring.excludingElectorateIndex]] equal to this value.
			 * @param includeMyself whether to include this participant in the set of participants to authorize the quiescence of. If true, this participant will be authorized after all the others have acknowledged the authorization.
			 * TODO Consider having independent attempts counter for each peer. */
			private def authorizeQuiescenceTo(peers: Set[ParticipantId], permittedElectorateChangeIndex: RecordIndex, includeMyself: Boolean)(using Trace.Context): Unit = {
				Trace.step(() => s"authorizeQuiescenceTo($peers, $permittedElectorateChangeIndex, $includeMyself)") {
					def loop(attemptsDone: Int): Unit = {
						val nonAcknowledgedQuiescencePermissionsArray = nonAcknowledgedQuiescencePermissions.toArray
						val calls = for (participantId, indexOfAuthorizedSec) <- nonAcknowledgedQuiescencePermissionsArray yield participantId.permitQuiescence(indexOfAuthorizedSec)
						for responses <- sequencer.Capture_sequenceHardyToArray(calls) do {
							Trace.trace(s"Quiescence permission acknowledgments: ${nonAcknowledgedQuiescencePermissionsArray.zip(responses).mkString("[", ", ", "]")}")
							IArray.unsafeFromArray(responses).foreachWithIndex { (response, arrayIndex) =>
								val nonAcknowledgedPermissionEntry = nonAcknowledgedQuiescencePermissionsArray(arrayIndex)
								val peerId = nonAcknowledgedPermissionEntry._1
								val electorateChangeIndexAssociatedToResponse = nonAcknowledgedPermissionEntry._2
								response match {
									case Failure(e) =>
										Trace.debug(s"$boundParticipantId: An attempt to permit $peerId to quiesce at $electorateChangeIndexAssociatedToResponse failed after $attemptsDone attempts ${if electorateChangeIndexAssociatedToResponse == permittedElectorateChangeIndex then "" else s"(since the electorate change at $permittedElectorateChangeIndex)"} with:", e)
									case _ =>
										if nonAcknowledgedQuiescencePermissions.getOrElse(peerId, 0L) == electorateChangeIndexAssociatedToResponse then nonAcknowledgedQuiescencePermissions.remove(peerId)
								}
							}
							if nonAcknowledgedQuiescencePermissions.nonEmpty && attemptsDone < MAX_PERMIT_QUIESCENCE_RETRIES then {
								val token = requestWakeUp(WakeUpReason.QuiescenceAuthorizationRetry, attemptsDone, () => loop(attemptsDone + 1))
								retryPermitQuiescenceWakeUpToken = Maybe(token)
							} else {
								if nonAcknowledgedQuiescencePermissions.nonEmpty then {
									Trace.warn(s"$boundParticipantId: The limit of attempts ($attemptsDone) to permit the participants $nonAcknowledgedQuiescencePermissions to quiesce at $permittedElectorateChangeIndex has been reached.")
									nonAcknowledgedQuiescencePermissions.clear()
								}
								if includeMyself then currentRole.onQuiescencePermitted(boundParticipantId, permittedElectorateChangeIndex)
								becomeQuiescedIfEligible(permittedElectorateChangeIndex)
							}
						}
					}

					retryPermitQuiescenceWakeUpToken.foreach(_.cancel())
					peers.foreach { participantId => nonAcknowledgedQuiescencePermissions.put(participantId, permittedElectorateChangeIndex) }
					loop(0)
				}
			}

			override def authorizeQuiescenceIfVanished(soleElectorate: SoleElectorate)(using Trace.Context): Unit = {
				if soleElectorate.members.length == 0 then {
					val excludedParticipantsExceptSelf = soleElectorate.backingElectorateChange.oldParticipants.union(cluster.getOtherProbableParticipants) - boundParticipantId
					authorizeQuiescenceTo(excludedParticipantsExceptSelf, soleElectorate.changeIndex, true)
				}
			}


			private def continueWithSecondPhase(tcc: JointElectorateChange[ParticipantId], tccIndex: RecordIndex)(using Context): sequencer.Capture[ElectorateChangeResponse] = {
				for {
					isSecReplicatedToMajority <- appendALocalSecAndReplicateIt(tcc, tccIndex)
					response <- {
						if isSecReplicatedToMajority then sequencer.Keeper(new SUCCESSFULLY_CHANGED)
						else {
							(for primaryState1 <- primaryStateFence.causalAnchor() yield {
								new REQUEST_TRACKING_LOST_AFTER_SECOND_PHASE_STARTED(currentRole.syncLocalStateInfo(Maybe(primaryState1)).ballot)
							}): sequencer.Capture[ElectorateChangeResponse]
						}
					}
				} yield response
			}

			private def replicateJecAndThenContinueWithSecondPhase(primaryState1: PrimaryState, tcc: JointElectorateChange[ParticipantId], tccIndex: RecordIndex)(using Context): sequencer.Capture[ElectorateChangeResponse] = Trace.step(() => s"replicateJecAndThenContinueWithSecondPhase(tccIndex=$tccIndex)") {
				if currentRole ne thisLeader then {
					val ballot = currentRole.syncLocalStateInfo(Maybe(primaryState1)).ballot
					sequencer.Keeper(new REQUEST_TRACKING_LOST_AFTER_FIRST_PHASE_STARTED(ballot))
				} else {
					if assertionsEnabled then assert(primaryState1.currentTerm == leadedTerm)
					for {
						// Replicate to other participants.
						_ <- {
							driveReplicationPipelines(primaryState1)
							awaitCommitWatermark(primaryState1, tccIndex)
						}
						response <- sequencer.Capture_defer { () => // Design Tradeoff (Decoupled Mutation Contract): This manual deferral is the cost of the design decision that updateCommitIndex to run synchronously to avoid allocations and deferral overhead on the happy path without re-entrance bugs. Given that continueWithSecondPhase mutates primaryStateFence, execution is explicitly deferred.
							for {
								primaryState3 <- primaryStateFence.causalAnchor()
								result <- {
									if currentRole ne thisLeader then {
										val ballot1 = currentRole.syncLocalStateInfo(Maybe(primaryState3)).ballot
										sequencer.Keeper(if commitIndex >= tccIndex then new REQUEST_TRACKING_LOST_AFTER_FIRST_PHASE_COMMITTED(ballot1) else new REQUEST_TRACKING_LOST_AFTER_FIRST_PHASE_STARTED(ballot1))
									} else {
										assert(primaryState3.currentTerm == leadedTerm && commitIndex >= tccIndex)
										continueWithSecondPhase(tcc, tccIndex)
									}
								}
							} yield result
						}
					} yield response
				}
			}

			/** Handles electorate-change request for [[Leader]]
			 * Attempts a [[Electorate]] change, starting with the first phase and, if successful, continuing with the second. */
			override def requestElectorateChange(primaryState0: PrimaryState, requestId: ElectorateChangeRequestId, desiredParticipants: Set[ParticipantId], ballotWasUpdated: Boolean)(using Context): sequencer.Capture[ElectorateChangeResponse] = {
				Trace.trace(s"${if pendingElectorateChangesCompletion.isPending then "Enqueuing" else "Handling"} the electorate change request $requestId as leader")
				pendingElectorateChangesCompletion =
					for {
						_ <- pendingElectorateChangesCompletion
						response <- sequencer.Capture_defer { () => // Design Tradeoff (Decoupled Mutation Contract): This manual deferral is the cost of the design decision that updateCommitIndex to run synchronously to avoid allocations and deferral overhead on the happy path without re-entrance bugs. Given the enclosed code mutates primaryStateFence, execution is explicitly deferred.
							if currentRole ne thisLeader then currentRole.requestElectorateChange(requestId, desiredParticipants, Maybe.empty)
							else for {
								primaryState1 <- primaryStateFence.causalAnchor()
								response <- {
									val electorate1 = deriveElectorateFrom(primaryState1)
									val myStateInfo1 = syncStatefulStateInfo(primaryState1)
									Trace.trace(s"StateInfo=$myStateInfo1")
									electorate1 match {
										case sec1: SoleElectorate =>
											if desiredParticipants == sec1.stableParticipants then sequencer.Keeper(new ALREADY_CHANGED)
											// Do not start an electorate transition if excluded from both, the current, and the new electorate.
											else if !sec1.isBoundIncluded && !desiredParticipants.contains(boundParticipantId) then {
												// Also, become retiring immediately if all followers have committed the excluding electorate change. The intention of this is to minimize the time that a participant is kept leading after it was excluded.
												if isGhostAndAllLearnersCommittedTheExcludingElectorateChange then {
													assert(indexOfElectorateChangeThatExcludedThisParticipant == sec1.changeIndex)
													authorizeQuiescenceIfVanished(sec1)
													become(Retiring(primaryState1.currentTerm, sec1.term, sec1.changeIndex, sec1.members))
														.requestElectorateChange(requestId, desiredParticipants, Maybe.empty)
												}
												// If leading as a ghost and some learner hasn't committed the excluding electorate change, answer informing the situation.
												else sequencer.Keeper(new WAIT_GHOST_LEADER_IS_DEMOTED(myStateInfo1.ballot))
											} else {
												// start the first phase of the electorate change
												val tcc = new JointElectorateChange[ParticipantId](primaryState1.currentTerm, requestId, sec1.stableParticipants, desiredParticipants)
												Trace.trace(s"About to append JEC $tcc")
												for {
													// Update primary state
													primaryState3 <- primaryStateFence.advanceIf { primaryState2 =>
														if currentRole ne thisLeader then Maybe.empty
														else {
															assert(primaryState2.currentTerm == leadedTerm)
															Maybe(primaryState2.withSingleRecordAppended(tcc.term, tcc))
														}
													}
													// replicate the JointElectorateChange and then start the second phase.
													response <- replicateJecAndThenContinueWithSecondPhase(primaryState3, tcc, primaryState3.firstEmptyRecordIndex - 1)
												} yield response
											}

										case jec1: JointElectorate =>
											// We re-evaluate state after waiting, so it must be stable unless there's a logic bug.
											// But if somehow we are here, we must not infinite loop. We'll return an error.
											sequencer.Keeper(new REQUEST_TRACKING_LOST_AFTER_FIRST_PHASE_STARTED(myStateInfo1.ballot))
									}
								}
							} yield response
						}
					} yield response
				pendingElectorateChangesCompletion
			}

			/** Starts the second phase of an electorate change.
			 * Appends a [[SoleElectorateChange]] instance in the local log, stores it, and then attempts to replicate it to the participants in both, old and new electorate as if its electorate was the corresponding [[JointElectorateChange]].
			 * @param correspondingJointElectorateChange the [[JointElectorateChange]] that initiated the first phase of the electorate change.
			 * @return  a [[sequencer.Capture]] that yields true when the SEC is commited or false when demoted. */
			private def appendALocalSecAndReplicateIt(correspondingJointElectorateChange: JointElectorateChange[ParticipantId], tccIndex: RecordIndex)(using Context): sequencer.Capture[Boolean] = {
				Trace.step(() => s"appendALocalSecAndReplicateIt(tccIndex=$tccIndex)") {
					for {
						primaryState1 <- primaryStateFence.advanceIf { primaryState0 =>
							if currentRole ne thisLeader then Maybe.empty
							else {
								assert(primaryState0.currentTerm == leadedTerm)
								if primaryState0.indexOfLatestElectorateChange > tccIndex then Maybe.empty
								else {
									val scc = new SoleElectorateChange[ParticipantId](primaryState0.currentTerm, correspondingJointElectorateChange.requestId, correspondingJointElectorateChange.term, correspondingJointElectorateChange.oldParticipants, correspondingJointElectorateChange.newParticipants)
									Maybe(primaryState0.withSingleRecordAppended(primaryState0.currentTerm, scc))
								}
							}
						}

						isSecondPhaseChangeReplicatedToMajority <- {
							// TODO add a coupleIndex field in SoleElectorateChange and use it in the next if condition instead of the requestId (whose uniqueness depends on the user).
							if primaryState1.latestElectorateChange.get.requestId == correspondingJointElectorateChange.requestId then {
								val sccIndex = primaryState1.indexOfLatestElectorateChange
								Trace.trace(s"Starting replication of SEC at $sccIndex. The corresponding JEC is $correspondingJointElectorateChange at $tccIndex")
								replicateSec(primaryState1, sccIndex)
							} else sequencer.Capture_true
						}
					} yield isSecondPhaseChangeReplicatedToMajority
				}
			}

			/** Replicates the specified [[SoleElectorateChange]] record (and all the preceding uncommitted records) in the local log to the peers.
			 * A no-op [[LeaderTransition]] record is appended if [[Record]]s of a previous [[Term]] are blocking the [[commitIndex]] advancement due to the Raft safety rule (§5.4.2): "A leader cannot determine commitment using entries from previous terms". This constraint is implemented in [[JointElectorate.indexOfTheCommittableRecordWithHighestIndex]].
			 * @return a [[sequencer.Capture]] that yields true when the SEC record becomes commited, or false when demoted if happens before.
			 * @note Decoupled Mutation Contract: Because this method mutates the [[primaryStateFence]] upfront via [[appendNoOpRecordLocally]] when handling prior-term records, callers must NEVER invoke this method synchronously from within [[updateCommitIndex]] or from a synchronous continuation of [[awaitCommitWatermark]] without explicitly deferring execution via [[sequencer.Capture_defer]]. */
			private def replicateSec(primaryState0: PrimaryState, sccIndex: RecordIndex)(using Context): sequencer.Capture[Boolean] = {
				Trace.step("replicateSec") {
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
								// If the SEC was appended in a previous term (which happens exclusively during leader takeover in `Leader.handleEnter`), Raft §5.4.2 prohibits committing it by counting replicas alone. At the moment of takeover, the newly crowned leader has appended zero records in `leadedTerm`. Therefore, this branch executes to append a no-op `LeaderTransition` record, which is simultaneously the first, last, and only record in `leadedTerm`.
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
									awaitCommitWatermark(primaryState1, targetIndex)
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
							awaitCommitWatermark(primaryState1, commandRecordIndex)
						}
						response <- {
							// The role may have changed while attempting the replication. In that case, delegate the handling to the current role. The appended command record will be overwritten when the new leader calls the append records RPC.
							if currentRole ne thisLeader then currentRole.onCommandFromClient(clientCommand, INTERNAL_VACATE_HANDOFF)
							else {
								assert(commitIndex >= commandRecordIndex)
								for {
									_ <- decoupledCommandsApplierCompletion // Waits the committed-commands-applier to complete any work left by a previous role.
									response <- {
										// It is not necessary to have an updated primary state here because committed records are never mutated, and we are not mutating the primary state here. We only need to know if we are still leading.
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
			 * Used to jump-start the replication process when new records are appended or when an electorate change occurs.
			 * @param primaryState the current primary state. */
			private def driveReplicationPipelines(primaryState: PrimaryState)(using Trace.Context): Unit = Trace.step("driveReplicationPipelines") {
				assert(primaryStateFence.committedState.is(primaryState)) // Fails if the primaryStateFence was touched after yielding the PrimaryState instance received as parameter.
				if currentRole ne thisLeader then return
				val electorate = deriveElectorateFrom(primaryState)
				if currentRole ne thisLeader then return

				if electorate.peers.length == 0 then updateCommitIndex(primaryState)
				else {
					electorate.peers.foreachWithIndex { (learnerId, learnerIndex) =>
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

				val requestLeaderCommit = if isRetiree then learnerProgress.excludingSecIndex else commitIndex
				val fromIndex = learnerProgress.optimisticIndexOfNextRecordToSend
				val toIndex = if isRetiree then requestLeaderCommit + 1 else primaryState0.firstEmptyRecordIndex

				if toIndex > fromIndex then learnerProgress.optimisticIndexOfNextRecordToSend = toIndex
				// If the appending would be empty and the leaderCommit parameter would be equal or less than the known been committed, then skip the appending.
				else if requestLeaderCommit <= learnerProgress.highestRecordIndexKnowToBeCommitted then {
					// If also is retiring and the SEC is committed in the learner, then its retirement driving is complete.
					if isRetiree && learnerProgress.highestRecordIndexKnowToBeCommitted >= learnerProgress.excludingSecIndex then {
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
						if (currentRole eq thisLeader) && (abdicateAndUpdateTermIfLessThan(appendResult.term) eq thisLeader) then {
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
								val previousCommitIndex = commitIndex
								if !isRetiree then updateCommitIndex(primaryState1)
								if currentRole eq thisLeader then {
									if commitIndex > previousCommitIndex then driveReplicationPipelines(primaryState1)
									else driveReplicationPipeline(primaryState1, learnerId, learnerProgress, attemptsDone, isRetiree)
								}
							} else if appendOutcome == AO_IS_RETIRING || appendOutcome == AO_IS_QUIESCED then {
								retiringLearnersById.remove(learnerId)
								// Evaluate updateCommitIndex to potentially trigger a ghost leader abdication.
								updateCommitIndex(primaryState1)
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
									val retireeExcludingElectorateIndex = learnerFirstEmptyRecordIndex - 1
									learnerProgress.highestRecordIndexKnowToBeCommitted = retireeExcludingElectorateIndex
									learnerProgress.pessimisticIndexOfNextRecordToSend = learnerFirstEmptyRecordIndex
									learnerProgress.optimisticIndexOfNextRecordToSend = learnerFirstEmptyRecordIndex
									learnerProgress.highestRecordIndexKnownToBeAppended = retireeExcludingElectorateIndex
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

			/** Updates the actual [[commitIndex]] based on the highest record index that has been replicated during the leaded term to a quorum of learners.\
			 * If the commit index advances, this method notifies listeners, triggers the `recordBecomesCommittedCaptors`, and checks if the leader itself has been excluded from the cluster (becoming a ghost leader) to trigger quiescence.
			 * @param primaryState0 the current state of the participant. */
			private def updateCommitIndex(primaryState0: PrimaryState)(using Context): Unit = {
				assert(primaryStateFence.committedState.is(primaryState0), s"$primaryState0 != ${primaryStateFence.committedState}") // Fails if the primaryStateFence was touched after yielding the PrimaryState instance received as parameter.
				var electorate0A = deriveElectorateFrom(primaryState0)

				var keepLooping = true
				while keepLooping do {
					if isGhostAndAllLearnersCommittedTheExcludingElectorateChange then {
						authorizeQuiescenceIfVanished(electorate0A.asInstanceOf[SoleElectorate])
						val termOfElectorateChangeThatExcludedThisParticipant = primaryState0.getRecordTermAt(indexOfElectorateChangeThatExcludedThisParticipant) // This is safe (no IndexOutOfBoundsException) because `Leader.requestElectorateChange` prevents transitions while leading as ghost, and the `PrimaryState.getRecordTermAt` checks the `snapshot.latestElectorateChangeIndex`.
						become(Retiring(leadedTerm, termOfElectorateChangeThatExcludedThisParticipant, indexOfElectorateChangeThatExcludedThisParticipant, electorate0A.members))
						return
					}

					val previousCommitIndex = commitIndex
					val newCommitIndex = electorate0A.indexOfTheCommittableRecordWithHighestIndex(primaryState0, previousCommitIndex, learnerProgressByIndex)
					if newCommitIndex == previousCommitIndex then {
						keepLooping = false
					} else {
						commitIndex = newCommitIndex
						notifyListeners(_.onCommitIndexChanged(previousCommitIndex, newCommitIndex, LEADER, primaryState0.currentTerm))

						val electorate0B = deriveElectorateFrom(primaryState0)
						if electorate0B ne electorate0A then electorate0A = electorate0B
						else keepLooping = false
					}
				}

				// We partition the awaiters in-place to avoid allocating an intermediate array.
				// This groups all fulfilled awaiters at the end of the collection (from `partitionIdx` to the end).
				var i = 0
				var partitionIdx = recordBecomesCommittedCaptors.length
				while i < partitionIdx do {
					val awaiter = recordBecomesCommittedCaptors(i)
					if commitIndex >= awaiter.targetIndex then {
						partitionIdx -= 1
						recordBecomesCommittedCaptors(i) = recordBecomesCommittedCaptors(partitionIdx)
						recordBecomesCommittedCaptors(partitionIdx) = awaiter
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
				var j = recordBecomesCommittedCaptors.length
				while j > partitionIdx do {
					j -= 1
					if j < recordBecomesCommittedCaptors.length then {
						val awaiter = recordBecomesCommittedCaptors(j)
						recordBecomesCommittedCaptors.remove(j)
						awaiter.captureSync(primaryState0)
					}
				}

				assert(primaryStateFence.committedState.is(primaryState0), "An observer of the awaiter mutated the primary state synchronously, violating the decoupled mutation contract.")
			}

			/** Captures an asynchronous event that completes when the `commitIndex` reaches or exceeds the specified target index.\
			 * If the current `commitIndex` is already greater than or equal to `targetIndex`, it returns a synchronous pre-completed capture.\
			 * Otherwise, it registers a `Captor` in the `recordBecomesCommittedCaptors` collection which will be fulfilled by `updateCommitIndex`.\
			 *
			 * @note Commitment Barrier Invariant: This method functions strictly as a commit-watermark barrier. It never completes while
			 * the leader remains in office unless `commitIndex >= targetIndex` (e.g. `commitIndex >= tccIndex` or `commitIndex >= commandRecordIndex`).
			 * Consequently, when this capture resolves while `currentRole eq thisLeader`, `commitIndex >= targetIndex` is already guaranteed
			 * to be true. It never completes with a failure or uncommitted status while leading; if quorum is lost, it remains suspended
			 * until the leader exits office (where awaiters are seized in [[handleExit]]).
			 *
			 * @param targetIndex the record index to wait for.
			 * @return a [[sequencer.Capture]] that resolves to the [[PrimaryState]] when the commit index reaches the target. */
			private def awaitCommitWatermark(primaryState: PrimaryState, targetIndex: RecordIndex)(using Context): sequencer.Capture[PrimaryState] = Trace.step(() => s"awaitCommitWatermark($targetIndex)") {
				if commitIndex >= targetIndex then sequencer.Capture_ready(primaryState)
				else {
					val awaiter = new RecordBecomesCommittedCaptor(targetIndex)
					recordBecomesCommittedCaptors.addOne(awaiter)
					awaiter
				}
			}

			/** @return true if this leading participant is a ghost leader (not included in the active [[Electorate]]) and all the learners have committed the SEC that excluded this leader (turning it into a ghost).
			 * @note that for the result of this operation be reliable, the [[PrimaryState]] should have stayed constant since the last call to [[deriveElectorateFrom]]. */
			private def isGhostAndAllLearnersCommittedTheExcludingElectorateChange: Boolean = {
				indexOfElectorateChangeThatExcludedThisParticipant > 0
					&& learnerProgressByIndex.forall(_.highestRecordIndexKnowToBeCommitted >= indexOfElectorateChangeThatExcludedThisParticipant)
					&& retiringLearnersById.forall(_._2.highestRecordIndexKnowToBeCommitted >= indexOfElectorateChangeThatExcludedThisParticipant)
			}

			def abdicateAndUpdateTermIfLessThan(seenTerm: Term)(using Context): Role = {
				Trace.step("abdicateAndUpdateTermIfLessThan") {
					if thisLeader.leadedTerm < seenTerm then {
						Trace.trace(s"About to abdicate due to a higher term seen.")
						become(Abdicating(thisLeader.leadedTerm, seenTerm, primaryStateFence))
					} else thisLeader
				}
			}

			override def onTermUpdated(primaryState: PrimaryState, maybeLeaderId: Maybe[ParticipantId])(using Context): Unit = {
				Trace.step("Leader.onTermUpdated") {
					Trace.trace(s"About to abdicate due to term update.")
					become(maybeLeaderId.fold(Isolated(primaryStateFence))(Follower(primaryState.currentTerm, _, primaryStateFence)))
				}
			}
		}

		private def Leader(term: Term, primaryState: PrimaryState, electorate: Electorate, psf: CausalFence[PrimaryState, sequencer.type]): Maybe[Leader] = {
			currentRole match {
				case leader: Leader if leader.leadedTerm == term && (leader.primaryStateFence eq psf) => Maybe.empty
				case _ => Maybe(new Leader(term, primaryState, electorate, psf))
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
		private final class LearnerProgress(firstEmptyRecordIndex: RecordIndex) extends PeerProgressView {
			/** The index of the next record to send to the peer assuming the in-flight appends will fail.
			 * This is the index of the highest record for which an append result hasn't been received, successful or not.\
			 * Expresses the lower bound of unacknowledged log entries (where replication resumes if an in-flight attempt fails or a rejection occurs).
			 * Optimistically initialized to the first empty record index of the leader's workspace for all participants, assuming each follower's log is already up-to-date with the leader's log.
			 * If a follower's log is actually behind or inconsistent, this index is decremented upon rejection until logs align.
			 * @note TODO: Consider initializing it with the first empty record index unless the last filled ones are electorate changes, in which case initialize with the index of the first of them. Sending extra [[ElectorateChange]] instances is cheap and may avoid rejections due to need of an earlier [[Record]]. */
			var pessimisticIndexOfNextRecordToSend: RecordIndex = firstEmptyRecordIndex
			/** The index of the next record to send to the peer assuming the in-flight appends will succeed.
			 * If a follower's log is actually behind or inconsistent, this index is decremented upon rejection until logs align.
			 * @note TODO: Consider initializing it with the first empty record index unless the last filled ones are electorate changes, in which case initialize with the index of the first of them. Sending extra [[ElectorateChange]] instances is cheap and may avoid rejections due to need of an earlier [[Record]]. */
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
			/** The index of the [[SoleElectorateChange]] that excludes this learner, or 0 if it is not retiring. */
			var excludingSecIndex: RecordIndex = 0

			/** @return true if the learner is retiring (i.e., it is excluding index is greater than 0). */
			inline def isRetiring: Boolean = excludingSecIndex > 0

			/** @return the current number of in-flight append requests sent to this peer. */
			inline def inFlightAppendCount: Int = lastEmittedAppendSerial - lastReceivedAppendSerial
		}

		//// MISCELLANEOUS

		/**
		 * Asks the [[Electorate.peers]] how they are ([[ClusterParticipant.howAreYou]]) in a coalesced manner: If an equivalent question is in flight, reuses the same pending [[sequencer.Capture]] of the in-flight question; otherwise, a new request is done.
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

		/** Attempts to transition this participant to the [[QUIESCED]] role.\
		 * This check is performed whenever a potential prerequisite for quiescence is met (e.g., is retiring, a retirement pipeline finishes, or permission to quiesce is granted).\
		 * The transition only proceeds if the participant is in the [[RETIRING]] role, no retirement pipeline is active, and protocol permission was granted.\
		 * Three independent async processes must converge: (a) all retirement pipelines must complete and be removed from `retiringLearnersById`, (b) role must be RETIRING, (c) quiescence permission must be granted. And that these are fulfilled by different mechanisms (driveReplicationPipeline, become(Retiring), authorizeQuiescenceTo). */
		private def becomeQuiescedIfEligible(indexOfExcludingElectorateChange: RecordIndex)(using Trace.Context): Unit = {
			Trace.step("becomeQuiescedIfEligible") {
				if retiringLearnersById.isEmpty && nonAcknowledgedQuiescencePermissions.isEmpty && currentRole.ordinal == RETIRING && indexOfExcludingElectorateChange <= indexOfSecForWhichQuiescenceWasPermitted
				then become(Quiesced(Success(s"The incoming leader ${quiescenceGrantor.value} authorized quiescence and no retirement driver exists.")))
			}
		}

		private def illegalStateQuiesce(detail: String = "")(using Trace.Context): Role = {
			Trace.step("illegalStateQuiesce") {
				val failure = new IllegalStateException(s"Should never happen. $detail")
				Trace.error(s"Should never happen", failure)
				become(Quiesced(Failure(failure)))
			}
		}

		//// NOTIFICATIONS

		def subscribe(listener: NotificationListener): Unit = {
			checkWithin()
			notificationListeners.put(listener, None)
		}

		def unsubscribe(listener: NotificationListener): Boolean = {
			checkWithin()
			notificationListeners.remove(listener) eq None
		}

		/** @param notifier a function that receives a [[NotificationListener]] and calls one of its methods. */
		private def notifyListeners(notifier: NotificationListener => Unit)(using Trace.Context): Unit = {
			notificationListeners.forEach { (listener, _) =>
				try notifier(listener)
				catch {
					case NonFatal(e) => Trace.error(s"$boundParticipantId: A notification listener threw:", e)
				}
			}
		}
	}
}
