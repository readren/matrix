package readren.consensus

import protocol.*
import readren.common.*
import readren.sequencer.*

import scala.collection.immutable.{ListSet, Set}
import scala.util.Try

/**
 * Component Definition Module (CDM) for cluster participant bridge communication,
 * membership routing, and deferred wake-up scheduling.
 *
 * Encapsulates the [[ClusterBridge]] SPI and its callback [[ClusterBridge.Delegate]],
 * coordinating deterministic asynchronous RPC execution via [[sequencer: Doer]].
 */
trait ConsensusClusterBridgeCdm extends ConsensusWireTypesCdm { thisBridgeModule =>

	/** The execution sequencer that drives deterministic state transitions.
	 *
	 * All methods that access mutable consensus state must be invoked through this sequencer to ensure deterministic,
	 * single-threaded execution. This coordination model avoids the need for blocking synchronization.
	 */
	val sequencer: Doer

	inline def isInSequence: Boolean = sequencer.isInSequence

	/** Specifies what a consensus participant service requires from the cluster-participant-service it is bound to.
	 *
	 * A [[ClusterBridge]] binds a single consensus participant within a specific cluster and provides the identity, membership, and communication mechanisms required by the bound consensus participant service.
	 *
	 * Responsibilities of a [[ClusterBridge]] include:
	 * - Exposing the identity of the participant it services via [[boundParticipantId]].
	 * - Acting as the source of truth for cluster membership and determining when an electorate change should be triggered. This includes reacting to node join/leave events, quorum loss, scaling decisions, or health-based adjustments.
	 * - Initiating electorate transitions by calling [[Delegate.requestElectorateChange]] when a change is required.
	 * - Routing inter-participant RPCs (e.g., [[howAreYou]], [[requestBallot]], [[appendRecords]]) to the appropriate [[Delegate]] methods.
	 * - Delivering client commands and consensus messages to the bound consensus participant via the last [[Delegate]] set with [[setBound]] by the consensus participant.
	 * - Scheduling deferred wake-ups when requested by the consensus participant via [[requestWakeUp]], and invoking the provided callback within the [[sequencer]] after an appropriate host-determined delay.
	 * - Ensuring all invocations occur within the [[sequencer]] thread.
	 *
	 * Each [[ClusterBridge]] instance is tightly bound to a single consensus participant instance.
	 */
	trait ClusterBridge extends ClusterView {

		/** The implementation should return the identifier of the participant that this [[ClusterBridge]] service — and its bound consensus participant service — are responsible for. */
		val boundParticipantId: ParticipantId

		/** Returns the memorized set of other cluster participants (excluding [[boundParticipantId]]) that this [[ClusterBridge]] currently considers stable, up, and intended for consensus. */
		def getOtherProbableParticipants: ListSet[ParticipantId]

		/** **Outbound bridge (advisory hook)**: Called by the bound consensus participant to advise that its active electorate has changed, and now it expects connectivity with the active participants of the provided [[ElectorateChange]].
		 *
		 * @param change The [[ElectorateChange]] that backs the activated electorate.
		 * @param changeIndex the [[RecordIndex]] of the applied [[ElectorateChange]]
		 * @param roleOrdinal the [[RoleOrdinal]] of the bound participant in the new electorate.
		 */
		def onActiveElectorateChanged(change: ElectorateChange[ParticipantId], changeIndex: RecordIndex, roleOrdinal: RoleOrdinal): Unit

		/** **Outbound bridge (advisory hook)**: Called by the bound consensus participant after it becomes quiesced. This allows this [[ClusterBridge]] service to release the resources dedicated to it. */
		def onQuiesced(motive: Try[String]): Unit

		/** Called by the bound consensus participant when it needs to be woken up after some host-determined delay.
		 * The host should eventually invoke the provided `callback` within the [[sequencer]], after an appropriate delay.
		 * The [[WakeUpReason]] conveys what the consensus algorithm needs retried so the host can choose an appropriate delay.
		 *
		 * Must be called within the [[sequencer]].
		 *
		 * @param reason describes what the consensus algorithm needs retried.
		 * @param wakeUpsDone the number of related wake-up requests done before. Allows the implementation to determine a delay that depends on the number of failed attempts.
		 * @param callback the function to invoke when the delay elapses. Must be invoked within the [[sequencer]].
		 * @return a token that can be passed to cancel the pending wake-up.
		 */
		def requestWakeUp(reason: WakeUpReason, wakeUpsDone: Int, callback: () => Unit): WakeUpToken

		/** Defines the operations that the consensus participant exposes to this [[ClusterBridge]] instance, specially the callback methods for the events that the consensus participant needs to be noticed of.
		 *
		 * The consensus participant is responsible for setting the bound to this [[ClusterBridge]] instance by calling [[setBound]].
		 * This [[ClusterBridge]] instance talks to the bound consensus participant by calling these methods.
		 * For example, to deliver client commands, consensus messages, and request cluster-electorate changes.
		 * All invocations must occur within the [[sequencer]] to preserve consistency and serialization guarantees.
		 */
		trait Delegate {

			/** **Inbound bridge**: Handles a client-submitted command intended for the state machine.
			 *
			 * This method is invoked by the bound [[ClusterBridge]] when a client sends a command to this participant.
			 * It must be called within the [[sequencer]].
			 *
			 * @param command The command issued by the client for state-machine execution.
			 * @param attemptFlag Indicates the outcome of the client's previous attempt to send this command.
			 * @return A [[sequencer.Capture]] that yields one of:
			 *         - The state-machine's response to the client.
			 *         - A [[RedirectTo]] message instructing the client to contact the current leader.
			 *         - An [[Unable]] message indicating that this participant cannot currently reach consensus.
			 */
			def onCommandFromClient(command: ClientCommand, attemptFlag: CommandAttemptFlag): sequencer.Capture[ResponseToClient]

			/** **Inbound bridge**: This method is invoked by this [[ClusterBridge]] when another participant calls [[howAreYou]] on the [[ParticipantId]] of the owner of this [[Delegate]].
			 *
			 * Must be called within the [[sequencer]].
			 * @param inquirerId The id of the participant that called [[howAreYou]].
			 * @param inquirerInfo The [[StateInfo]] of the participant that called [[howAreYou]].
			 * @return The state information of the destination participant.
			 */
			def onHowAreYou(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.Capture[StateInfo]

			/** **Inbound bridge**: This method is invoked by this [[ClusterBridge]] when another participant calls [[requestBallot]] on the [[ParticipantId]] of the owner of this [[Delegate]].
			 *
			 * Must be called within the [[sequencer]].
			 * @param inquirerId The id of the participant that called [[requestBallot]].
			 * @param inquirerInfo Information about the state of the participant that called.
			 * @return A [[sequencer.Capture]] that yields a [[Ballot]] indicating whether the vote was granted to the inquiring candidate.
			 */
			def onRequestBallot(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.Capture[Ballot]

			/** **Inbound bridge**: This method is invoked by this [[ClusterBridge]] when another participant calls [[appendRecords]] on the [[ParticipantId]] of the owner of this [[Delegate]].
			 *
			 * Must be called within the [[sequencer]].
			 * @param inquirerId The id of the participant that called [[appendRecords]].
			 * @param inquirerTerm The term of the participant that called [[appendRecords]].
			 * @param prevRecordIndex The index of record after which the specified `records` should be appended.
			 * @param prevRecordTerm The term of the record after which the specified `records` should be appended.
			 * @param batch The records to append.
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

			/** **Inbound bridge**: This method is invoked by this [[ClusterBridge]] when another participant calls [[permitQuiescence]] on the [[ParticipantId]] of the owner of this [[Delegate]].
			 * @param grantorId the identifier of the participant that granted permission to quiesce.
			 * @param indexOfGrantedSec The index of the [[SoleElectorateChange]] record for which the permission to quiesce was granted. Said record is the one that excludes the destination participant.
			 */
			def onQuiescencePermitted(grantorId: ParticipantId, indexOfGrantedSec: RecordIndex): Unit

			/** Allows the [[ClusterBridge]] service to request changes to the set of consensus-participants.
			 *
			 * @param requestId an identifier chosen by the caller that will be propagated up to the invocations of the [[onActiveElectorateChanged]] method of each of the [[ClusterBridge]] instances bound to the involved consensus participant services.
			 * @param desiredParticipantsSet the identifiers of the participants that are going to seek consensus from now on.
			 * @return a [[sequencer.Capture]] that yields an [[ElectorateChangeResponse]].
			 */
			def requestElectorateChange(requestId: ElectorateChangeRequestId, desiredParticipantsSet: Set[ParticipantId]): sequencer.Capture[ElectorateChangeResponse[ParticipantId]]
		}

		/** The consensus participant calls this method to expose itself through a [[Delegate]] instance which specifies the operations through which this [[ClusterBridge]] can talk to it.
		 * Is called within the [[sequencer]] thread. */
		def setBound(delegate: Delegate): Unit

		/** Called by the consensus participant when it is leaving existence. */
		def removeBound(): Unit

		extension (destinationId: ParticipantId) {
			/** Asks the destination participant how it is doing.
			 * Called within the [[sequencer]] thread.
			 * @param inquirerInfo The term of the participant that is asking.
			 * @return A [[sequencer.Capture]] that yields the state information of the destination participant.
			 */
			def howAreYou(inquirerInfo: StateInfo): sequencer.Capture[StateInfo]

			/** Solicits a ballot from the destination participant.
			 * Called within the [[sequencer]] thread.
			 * @param inquirerId The id of the participant that is asking.
			 * @param inquirerInfo Information about the state of the participant that is asking.
			 * @return A [[sequencer.Capture]] that yields a [[Ballot]] indicating whether the vote was granted by the destination participant.
			 */
			def requestBallot(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.Capture[Ballot]

			/** Request the destination participant to append records.
			 * Called within the [[sequencer]] thread.
			 * @param inquirerTerm The term of the participant that is asking.
			 * @param prevLogIndex the [[RecordIndex]] of the [[Record]] after which the records should be appended.
			 * @param prevLogTerm the expected [[Term]] of the [[Record]] at `prevLogIndex`.
			 * @param batch The records to append.
			 * @param leaderCommit The index of the highest log entry known to be committed according to the inquirer.
			 * @return A [[sequencer.Capture]] that yields the result of the append operation.
			 */
			def appendRecords(inquirerTerm: Term, prevLogIndex: RecordIndex, prevLogTerm: Term, batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.Capture[AppendResult]

			/** Sends a snapshot to the destination participant, replacing its log and state machine state. Only leaders call this method.
			 * Called within the [[sequencer]] thread.
			 * @param inquirerTerm The term of the participant that is sending the snapshot.
			 * @param snapshot the snapshot data including state machine state and metadata.
			 * @return A [[sequencer.Capture]] that yields the result of the installation operation.
			 */
			def installSnapshot(inquirerTerm: Term, snapshot: SnapshotData[ParticipantId], batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.Capture[AppendResult]

			/** Authorizes the destination participant to transition to the terminal quiesced state.
			 * @param indexOfGrantedSec The index of the [[SoleElectorateChange]] record for which the authorization is granted.
			 * @return A [[sequencer.Capture]] that completes successfully if the permission was delivered.
			 */
			def permitQuiescence(indexOfGrantedSec: RecordIndex): sequencer.Capture[Unit]
		}
	}
}
