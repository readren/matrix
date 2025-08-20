package readren.matrix
package consensus

import readren.common.Maybe
import readren.sequencer.SchedulingExtension.MilliDuration
import readren.sequencer.{Doer, SchedulingExtension}

import java.util
import java.util.Comparator
import scala.collection.IndexedSeq as GenIndexedSeq
import scala.math.Ordering.Implicits.infixOrderingOps
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}


object Conciliator {
	/** The type of record indices.
	 * Index base is 1.
	 * Zero means before the first record. */
	final type RecordIndex = Long
	/** The type of terms.
	 * Starts from 1.
	 * Zero means "before first election". */
	final type Term = Int
	/** The type of behavior ids. */
	final type BehaviorOrdinal = Byte


	final val UNBORN: BehaviorOrdinal = -1
	final val STOPPED: BehaviorOrdinal = 0
	final val STARTING: BehaviorOrdinal = 1
	final val ISOLATED: BehaviorOrdinal = 2
	final val CANDIDATE: BehaviorOrdinal = 3
	final val FOLLOWER: BehaviorOrdinal = 4
	final val LEADER: BehaviorOrdinal = 5
}


/**
 * Service Module Trait that defines the consensus algorithm interface and types.
 *
 * This trait implements the [[Service Module Trait Pattern]], which serves multiple purposes:
 *
 * 1. **Type Parameter Container**: Defines abstract types (`ParticipantId`, `ClientCommand`) and concrete type aliases
 *    (`RecordIndex`, `Term`, `BehaviorOrdinal`) to eliminate the need for generic type parameters on the main service class.
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
trait Conciliator { thisConciliator =>

	import Conciliator.*

	/** The type of participant ids. */
	type ParticipantId <: AnyRef: {Ordering, ClassTag}
	/** The type of client commands. */
	type ClientCommand
	/** The type of the state-machine's [[StateMachine.appliesClientCommand]] method's responses. */
	type StateMachineResponse

	/** The type of [[Workspace]] implementation. */
	type WS <: Workspace

	//// CONFIGURATION

	/** Tells how many times the task returned by [[appliesClientCommand]] method should be tried when its execution fails.
	 * After exceeding the specified number of retries, the state machine is considered corrupted, and therefore the consensus participant service stops because it has no way to continue replicating. */
	def applyCommandRetries: Int = 1

	def isEager: Boolean = false

	def isolatedMainLoopInterval: MilliDuration = 500

	def candidateMainLoopInterval: MilliDuration = 500

	//	def leaderMainLoopInterval: MilliDuration = 500

	def failedReplicationsLoopInterval: MilliDuration = 500

	//// THREADING & TIMING

	/** The tool that the hosting [[ConsensusParticipant]] uses to mutate its state and schedule executions sequentially. */
	val sequencer: Doer & SchedulingExtension

	inline def isInSequence: Boolean = sequencer.isInSequence

	//// STATE MACHINE

	trait StateMachine {
		/** Gives the [[sequencer.Task]] that applies the [[ClientCommand]] to the state-machine. */
		def appliesClientCommand(index: RecordIndex, command: ClientCommand): sequencer.Task[StateMachineResponse]
	}

	//// RESPONSE TO CLIENT

	/** The response to a client command.
	 * @see [[Cluster.MessagesListener.onCommandFromClient]]. */
	trait ResponseToClient

	case class Processed(recordIndex: RecordIndex, content: StateMachineResponse) extends ResponseToClient

	case class RedirectTo(participantId: ParticipantId) extends ResponseToClient

	/** The participant is unable to process the command because it is isolated, starting, or stopped.
	 * After receiving this response, the client should try again with any of the [[otherParticipants]] with a flag indicating it is a fallback. 
	 * @param behaviorOrdinal The behavior of the participant that is not able to reach consensus.
	 * @param otherParticipants The ids of the other participants to try with. This field is not necessary because the cluster already knows them, but it is included for convenience.
	 */
	case class Unable(behaviorOrdinal: BehaviorOrdinal, otherParticipants: IndexedSeq[ParticipantId]) extends ResponseToClient

	//// DATA TYPES

	/** A vote for a leader.
	 * @see [[Cluster.askToChooseForLeader]] and [[Cluster.MessagesListener.onChooseALeader]].
	 * @param term The term for which the vote is cast.
	 * @param candidateId The id of the voted candidate.
	 * @param reachableCandidateCount The number of candidates that were reachable by the voter (including the voter itself) when the vote was cast.
	 * @param behaviorOrdinal The behavior of the voted candidate.
	 */
	case class Vote(term: Term, candidateId: ParticipantId, reachableCandidateCount: Int, behaviorOrdinal: BehaviorOrdinal)

	/** The result of an append operation.
	 * @see [[Cluster.asksToAppendRecords]] and [[Cluster.MessagesListener.onAppendRecords]].
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
	 * @param lastRecordTerm The term of the last record in the log of the participant that is answering.
	 * @param lastRecordIndex The index of the last record in the log of the participant that is answering.
	 */
	case class StateInfo(currentTerm: Term, ordinal: BehaviorOrdinal, lastRecordTerm: Term, lastRecordIndex: RecordIndex)

	//// CLUSTER

	/** Specifies what a [[ConsensusParticipant]] service requires from the cluster-participant-service hosted in the participant. */
	trait Cluster {

		/** The id of the participant that hosts this cluster service. */
		def hostId: ParticipantId

		/** The ids of all participants in the cluster. */
		def getParticipants: Set[ParticipantId]

		/**
		 * Specifies what a [[ConsensusParticipant]] listens to
		 * The implementation should call the methods of the listener within the [[sequencer]] thread. */
		trait MessagesListener {

			/**
			 * This method is invoked by this cluster when a client sends a command to the state-machine.
			 *
			 * @param command The command sent by the client.
			 * @param isFallback true if the client request is a retry of a previous request that failed and sent to a different participant.
			 * @return A [[sequencer.Task]] that yields the response of the state-machine to the client, or a [[RedirectTo]] message instructing to send the command to the leader, or a [[Unable]] message if the participant is not able to reach consensus.
			 */
			def onCommandFromClient(command: ClientCommand, isFallback: Boolean): sequencer.Task[ResponseToClient]

			/**
			 * This method is invoked by this cluster when another participant calls [[asksHowAreYou]].
			 *
			 * @param inquirerId The id of the participant that called [[asksHowAreYou]].
			 * @param inquirerTerm The term of the participant that called [[asksHowAreYou]].
			 * @return The state information of the destination participant.
			 */
			def onHowAreYou(inquirerId: ParticipantId, inquirerTerm: Term): StateInfo

			/**
			 * This method is invoked by this cluster when another participant calls [[askToChooseForLeader]].
			 *
			 * @param inquirerId The id of the participant that called [[askToChooseForLeader]].
			 * @param inquirerInfo Information about the state of the participant that called.
			 * @return A [[sequencer.Task]] that yields a [[Vote]] indicating the candidate chosen by the listening participant for the specified term.
			 */
			def onChooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.Task[Vote]

			/**
			 * This method is invoked by this cluster when another participant calls [[asksToAppendRecords]].
			 *
			 * @param inquirerId The id of the participant that called [[asksToAppendRecords]].
			 * @param inquirerTerm The term of the participant that called [[asksToAppendRecords]].
			 * @param prevRecordIndex The index of record after which the specified `records` should be appended.
			 * @param prevRecordTerm The term of the record after which the specified `records` should be appended.
			 * @param records The records to append.
			 * @param leaderCommit The index of the highest log entry known to be committed by the leader.
			 * @return A [[sequencer.Task]] that yields the result of the append operation.
			 */
			def onAppendRecords(inquirerId: ParticipantId, inquirerTerm: Term, prevRecordIndex: RecordIndex, prevRecordTerm: Term, records: GenIndexedSeq[Record], leaderCommit: RecordIndex): sequencer.Task[AppendResult]
		}

		/**
		 * Sets the message listener.
		 * Is called within the [[sequencer]] thread. */
		def setMessagesListener(listener: MessagesListener): Unit

		extension (destinationId: ParticipantId) {
			/**
			 * Asks the destination participant how it is doing.
			 * The implementation should make, somehow, the destination participant's [[MessagesListener.onHowAreYou]] to be called and return the result.
			 * Called within the [[sequencer]] thread.
			 * @param inquirerTerm The term of the participant that is asking.
			 * @param aLeaderMaybeMissing If true, the destination participant should update its view of the cluster state and its role.
			 * @return A [[sequencer.Task]] that yields the state information of the destination participant.
			 */
			def asksHowAreYou(inquirerTerm: Term, aLeaderMaybeMissing: Boolean): sequencer.Task[StateInfo]

			/**
			 * Asks the destination participant to choose a leader.
			 * The implementation should make, somehow, the destination participant's [[MessagesListener.onChooseALeader]] to be called and return the result.
			 * Called within the [[sequencer]] thread.
			 * @param inquirerId The id of the participant that is asking.
			 * @param inquirerInfo Information about the state of the participant that is asking.
			 * @return A [[sequencer.Task]] that yields a [[Vote]] indicating the candidate chosen by the destination participant.
			 */
			def askToChooseForLeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.Task[Vote]

			/**
			 * Asks the destination participant to append records.
			 * The implementation should make, somehow, the destination participant's [[MessagesListener.onAppendRecords]] to be called and return the result.
			 * Called within the [[sequencer]] thread.
			 * @param inquirerTerm The term of the participant that is asking.
			 * @param leaderId The id of the leader that is appending the records.
			 */
			def asksToAppendRecords(inquirerTerm: Term, prevLogIndex: RecordIndex, prevLogTerm: Term, records: GenIndexedSeq[Record], leaderCommit: RecordIndex): sequencer.Task[AppendResult]
		}
	}

	//// LOG RECORD

	sealed trait Record {
		def term: Term
	}

	private[consensus] case class Command(override val term: Term, command: ClientCommand) extends Record

	private[consensus] case class LeaderTransition(override val term: Term) extends Record

	private[consensus] case class SnapshotPoint(override val term: Term) extends Record

	private[consensus] case class ConfigurationChangeOldNew(override val term: Term, oldParticipants: Set[ParticipantId], newParticipants: Set[ParticipantId]) extends Record

	private[consensus] case class ConfigurationChangeNew(term: Term, newParticipants: Set[ParticipantId]) extends Record

	//// PERSISTENCE 

	/** A unit of work for managing the persistent state of a [[ConsensusParticipant]]. */
	trait Workspace {

		def isBrandNew: Boolean

		//		/** Record index of the first record in the log buffer. */
		//		private var logBufferOffset: RecordIndex = 1
		//
		//		/** The log buffer.
		//		 * Contains the records since the last snapshot. */
		//		private val logBuffer: mutable.ArrayBuffer[Record] = mutable.ArrayBuffer.empty

		def getCurrentParticipants: IndexedSeq[ParticipantId]

		def setCurrentParticipants(participants: IndexedSeq[ParticipantId]): Unit

		/** The current term according to this participant.
		 * Zero means "before the first election". */
		def getCurrentTerm: Term

		def setCurrentTerm(term: Term): Unit

		/** The index of the oldest [[Record]] stored in the log buffer. The initial value is 1. */
		def logBufferOffset: RecordIndex

		/** The index of the first empty entry in the log. The initial value is 1. */
		def firstEmptyRecordIndex: RecordIndex // = logBufferOffset + logBuffer.size

		/** The index of the last appended record. The initial value is 0, which means that the log is empty. */
		def lastAppendedRecord: Record // = getRecordAt(firstEmptyRecordIndex - 1)

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

		/** Should be called whenever the commit index changed to allow this [[Workspace]] to release the storage used to memorize already commited records. */
		def informCommitIndex(commitIndex: RecordIndex): Unit

		def release(): Unit

		inline def getRecordTermAt(index: RecordIndex): Term =
			if index == 0 then 0
			else getRecordAt(index).term
	}
	
	class InvalidWorkspaceException extends RuntimeException
	
	/** Partial implementation of an invalid [[Workspace]].
	 * Design note: An invalid [[Workspace]] is required to avoid the use of [[Option]], [[Maybe]] or nullable in the definition of [[ConsensusParticipant.workspace]]. // TODO analyze if this crap is worth. */
	abstract class InvalidWorkspacePi extends Workspace {
		override def isBrandNew: Boolean = throw new InvalidWorkspaceException

		override def getCurrentParticipants: IndexedSeq[ParticipantId] = throw new InvalidWorkspaceException

		override def setCurrentParticipants(participants: IndexedSeq[ParticipantId]): Unit = throw new InvalidWorkspaceException

		override def getCurrentTerm: Term = throw new InvalidWorkspaceException

		override def setCurrentTerm(term: Term): Unit = throw new InvalidWorkspaceException

		override def logBufferOffset: RecordIndex = throw new InvalidWorkspaceException

		override def firstEmptyRecordIndex: RecordIndex = throw new InvalidWorkspaceException

		override def lastAppendedRecord: Record = throw new InvalidWorkspaceException

		override def getRecordAt(index: RecordIndex): Record = throw new InvalidWorkspaceException

		override def getRecordsBetween(from: RecordIndex, until: RecordIndex): GenIndexedSeq[Record] = throw new InvalidWorkspaceException

		override def appendRecord(record: Record): Unit = throw new InvalidWorkspaceException

		override def appendResolvingConflicts(records: GenIndexedSeq[Record], from: RecordIndex): RecordIndex = throw new InvalidWorkspaceException

		override def informCommitIndex(commitIndex: RecordIndex): Unit = throw new InvalidWorkspaceException

		override def release(): Unit = ()
	}

	/** Defines what [[ConsensusParticipant]] requires from the persistence service within the same participant.
	 * Implementations may assume that all methods of this trait are invoked within the [[sequencer]] thread, enabling optimizations such as avoiding unnecessary creation of new task objects. */
	trait Storage {
		/** Creates a new invalid workspace whose methods (except [[Workspace.release]]) should terminate abruptly. */
		def invalidWorkspace(): WS

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

		def onBecameStopped(previous: BehaviorOrdinal, term: Term, motive: Throwable): Unit

		def onBecameIsolated(previous: BehaviorOrdinal, term: Term): Unit

		def onBecameCandidate(previous: BehaviorOrdinal, term: Term): Unit

		def onBecameFollower(previous: BehaviorOrdinal, term: Term, leaderId: ParticipantId): Unit

		def onBecameLeader(previous: BehaviorOrdinal, term: Term): Unit

		def onLeft(left: BehaviorOrdinal, term: Term): Unit
	}

	/**
	 * A convenience [[NotificationListener]] implementation with no-op methods.
	 * Extend this class and override only the methods you need.
	 */
	open class DefaultNotificationListener extends NotificationListener {
		override def onStarting(previous: BehaviorOrdinal, term: Term, isRestart: Boolean): Unit = ()

		override def onStarted(previous: BehaviorOrdinal, term: Term, isRestart: Boolean): Unit = ()

		override def onBecameStopped(previous: BehaviorOrdinal, term: Term, motive: Throwable): Unit = ()

		override def onBecameIsolated(previous: BehaviorOrdinal, term: Term): Unit = ()

		override def onBecameCandidate(previous: BehaviorOrdinal, term: Term): Unit = ()

		override def onBecameFollower(previous: BehaviorOrdinal, term: Term, leaderId: ParticipantId): Unit = ()

		override def onBecameLeader(previous: BehaviorOrdinal, term: Term): Unit = ()

		def onLeft(left: BehaviorOrdinal, term: Term): Unit = ()
	}


	//// PARTICIPANT'S CONSENSUS SERVICE

	/**
	 * A consensus algorithm for managing a replicated log in distributed systems.
	 *
	 * This algorithm enables multiple participants (typically hosted on different nodes) to reach agreement on values.
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
	 *   highest behavior ordinal, and lexicographically smallest participant ID.
	 *
	 * The election process works as follows:
	 *
	 * - Elections are triggered when a client request is received by a participant in isolated or candidate state, or a follower if the request is marked with "isFallback" flag.
	 * - The initiating participant queries other participants with "how are you" questions to update its view of cluster state and its role.
	 * - These queries include a flag indicating that "a leader may be missing" to prompt followers to update their own views and roles.
	 * - When a participant receives a "how are you" query, it responds with its current state information.
	 * - Participants in "isolated" or "candidate" also update their view of the cluster state and their role by querying other participants. Followers do the same but only if the query is tagged with "a leader may be missing".
	 * - A participant becomes leader if either:
	 *   - It receives responses from all participants and is determined to be the chosen candidate according to the selection criteria.
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
	class ConsensusParticipant(cluster: Cluster, storage: Storage, machine: StateMachine, initialListeners: Iterable[NotificationListener], stateMachineNeedsRestart: Boolean) { thisConsensusParticipant =>

		import cluster.*

		val module: thisConciliator.type = thisConciliator

		/** The current set of participants in the cluster, according to this participant, sorted.
		 * Should be reflected in the [[Workspace]].
		 * */
		private var currentParticipants: IndexedSeq[ParticipantId] = IndexedSeq.empty

		/** Should be updated whenever [[currentParticipants]] mutates */
		private var smallestMajority: Int = 0

		/** Should be updated whenever [[currentParticipants]] mutates */
		private var otherParticipants: IndexedSeq[ParticipantId] = IndexedSeq.empty

		/** The current term according to this participant.
		 * The Initial value is zero.
		 * Zero means "before the first election".
		 * Should be reflected in the [[Workspace]].
		 * */
		private var currentTerm: Term = 0

		/** The index of the highest entry known to be committed according to this participant.
		 * A log record is committed once the leader that created the record has replicated it on a majority of the servers.
		 * This also commits all preceding records in the leader’s log, including records created by previous leaders.
		 * Should be informed to the [[Workspace]] whenever it changes. */
		private var commitIndex: RecordIndex = 0

		private var workspace: WS = storage.invalidWorkspace()

		/** The current behavior of this participant. */
		private var currentBehavior: Behavior = Starting(false, stateMachineNeedsRestart)

		/** Memorices the schedule that is currently scheduled. Needed to cancel the schedule when becoming a new behavior. */
		private var currentSchedule: Maybe[sequencer.Schedule] = Maybe.empty

		private val notificationListeners: java.util.WeakHashMap[NotificationListener, None.type] = new util.WeakHashMap()

		{
			initialListeners.foreach(notificationListeners.put(_, None))
			currentBehavior.onEnter(STOPPED)
		}

		private object participantIdComparator extends Comparator[ParticipantId] {
			private val ordering = summon[Ordering[ParticipantId]]

			override def compare(a: ParticipantId, b: ParticipantId): Int = ordering.compare(a, b)
		}

		/** @return the ordinal of the current behavior. */
		inline def getBehaviorOrdinal: BehaviorOrdinal = currentBehavior.ordinal

		/** @return the current term. */
		inline def getTerm: Term = currentTerm

		/** @return the index of the specified [[ParticipantId]] in the [[currentParticipants]]' IndexedSeq.
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

		/**
		 * Abstract base class for consensus behavior states.
		 *
		 * Each behavior represents a different role in the consensus algorithm and implements
		 * the message handling logic specific to that role. Behaviors can transition to other
		 * behaviors based on received messages and internal logic.
		 *
		 * All behavior methods are executed within the sequencer thread to ensure thread safety.
		 */
		private sealed abstract class Behavior extends MessagesListener {
			/** The ordinal corresponding to this [[Behavior]] */
			val ordinal: BehaviorOrdinal

			def onEnter(previous: BehaviorOrdinal): Unit

			/** Called by [[become]] before transitioning to another behavior. */
			def onLeave(): Unit = ()

			def buildMyStateInfo: StateInfo = {
				val currentBehaviorOrdinal = currentBehavior.ordinal
				if currentBehaviorOrdinal <= STARTING then StateInfo(0, ordinal, 0, 0)
				else {
					val lastRecordIndex = workspace.firstEmptyRecordIndex - 1
					val lastRecordTerm = workspace.getRecordTermAt(lastRecordIndex)
					StateInfo(currentTerm, currentBehaviorOrdinal, lastRecordTerm, lastRecordIndex)
				}
			}

			override def onHowAreYou(inquirerId: ParticipantId, inquirerTerm: Term): StateInfo = {
				assert(isInSequence)
				if inquirerTerm > currentTerm && ordinal >= ISOLATED then {
					currentTerm = inquirerTerm
					// start the state-updater process
					updatesState.triggerExposingFailures()
				}
				buildMyStateInfo
			}

			override def onChooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.Task[Vote] = {
				assert(isInSequence)
				if ordinal <= STARTING then sequencer.Task.successful(Vote(0, hostId, 0, ordinal))
				else decidesMyVote(inquirerId, inquirerInfo).andThen {
					case Success(vote) =>
						if (vote.term > currentTerm && currentBehavior.ordinal >= ISOLATED) || (vote.reachableCandidateCount < smallestMajority && ordinal >= CANDIDATE) then {
							// start the state-updater process
							updatesState(vote).triggerExposingFailures()
						}
					case Failure(e) =>
						scribe.error(s"$hostId: Unexpected error while deciding my vote for $inquirerId.")
						// start the state-updater process
						updatesState.triggerExposingFailures()
				}
			}

			override def onAppendRecords(inquirerId: ParticipantId, inquirerTerm: Term, prevRecordIndex: RecordIndex, prevRecordTerm: Term, records: GenIndexedSeq[Record], leaderCommit: RecordIndex): sequencer.Task[AppendResult] = {
				assert(isInSequence)
				if ordinal < ISOLATED then sequencer.Task.successful(AppendResult(0, false, ordinal))
				else if inquirerTerm < currentTerm then sequencer.Task.successful(AppendResult(currentTerm, false, ordinal))
				else if workspace.getRecordTermAt(prevRecordIndex) != prevRecordTerm then sequencer.Task.successful(AppendResult(currentTerm, false, ordinal))
				else {
					if inquirerTerm > currentTerm || ordinal <= CANDIDATE then {
						currentTerm = inquirerTerm
						updatesState.triggerExposingFailures() // TODO: analyze what happens if this updateState causes to become the leader, preferably with a test.
					}

					assert(inquirerTerm == currentTerm)
					// Append any new entry not already in the log resolving conflicts.
					val lastAppendedRecordIndex = workspace.appendResolvingConflicts(records, prevRecordIndex + 1)

					// Unconditionally apply commited records to the state machine (in log order)
					val newCommitIndex = if leaderCommit < lastAppendedRecordIndex then leaderCommit else lastAppendedRecordIndex

					def applyRecordLoop(index: RecordIndex): sequencer.Task[Any] = {
						if index <= newCommitIndex then {
							val task: sequencer.Task[Any] = workspace.getRecordAt(index) match {
								case command: Command =>
									machine.appliesClientCommand(index, command.command).recover { e =>
										scribe.error(s"$hostId: Unexpected error while applying commited records to the state machine. This participant's state-machine must be restarted and commands replayed.", e)
										become(Starting(true, true))
									}
								case lt: LeaderTransition =>
									???
								case snapshot: SnapshotPoint =>
									???
								case cc1: ConfigurationChangeOldNew =>
									???
								case cc2: ConfigurationChangeNew =>
									???
							}
							var retriesCounter = 0
							task.transformWith {
								case Success(_) =>
									applyRecordLoop(index + 1)
								case Failure(e) =>
									if retriesCounter < applyCommandRetries then {
										retriesCounter += 1
										task
									} else sequencer.Task.failed(e)
							}
						} else sequencer.Task.successful(())
					}

					for {
						_ <- applyRecordLoop(commitIndex + 1)
						result <- {
							if currentBehavior.ordinal <= STARTING then sequencer.Task.successful(AppendResult(0, false, ordinal))
							else {
								commitIndex = newCommitIndex
								workspace.informCommitIndex(newCommitIndex)
								storage.saves(workspace).transform {
									case Success(_) =>
										Success(AppendResult(currentTerm, true, currentBehavior.ordinal))
									case Failure(e) =>
										scribe.error(s"$hostId: Unexpected error while saving the workspace. This participant's consensus service is unable to continue following the leader and will stop.", e)
										become(Stopped(e))
										Success(AppendResult(0, false, currentBehavior.ordinal))
								}
							}
						}
					} yield result
				}
			}

			protected def updatesState: sequencer.Task[Unit] = {
				assert(currentBehavior.ordinal >= ISOLATED)
				for {
					myVote <- decidesMyVote(null, null)
					saveResult <- updatesState(myVote)
				} yield saveResult
			}

			protected def updatesState(myVote: Vote): sequencer.Task[Unit] = {
				assert(currentBehavior.ordinal >= ISOLATED)
				if myVote.term > currentTerm then currentTerm = myVote.term

				if myVote.reachableCandidateCount == currentParticipants.size then {
					if myVote.candidateId == hostId then become(Leader())
					else become(Follower(myVote.candidateId))
					workspace.setCurrentTerm(currentTerm)
					storage.saves(workspace)
				} else if myVote.reachableCandidateCount < smallestMajority then {
					become(Isolated)
					workspace.setCurrentTerm(currentTerm)
					storage.saves(workspace)
				} else {
					val myStateInfo = buildMyStateInfo
					val inquires = for replierId <- otherParticipants yield replierId.askToChooseForLeader(hostId, myStateInfo)
					for {
						replies <- sequencer.Task.sequenceHardyToArray(inquires)
						saveResult <- {
							var myVoteIsValid = true
							var votesCount = 1 // my vote
							for case Success(replierVote) <- replies do {
								if replierVote.term > currentTerm then {
									currentTerm = replierVote.term
									myVoteIsValid = false
								}
								if replierVote.candidateId == myVote.candidateId && replierVote.reachableCandidateCount >= smallestMajority then votesCount += 1
							}
							if myVoteIsValid && votesCount >= smallestMajority then {
								if myVote.candidateId == hostId then become(Leader())
								else become(Follower(myVote.candidateId))
							} else become(Candidate)

							workspace.setCurrentTerm(currentTerm)
							storage.saves(workspace)
						}
					} yield saveResult
				}
			}

			extension [T](task: sequencer.Task[T]) {
				def triggerExposingFailures(): Unit = {
					updatesState.trigger(true) {
						case Success(()) => // do nothing
						case Failure(e) => scribe.error(s"$hostId: Unexpected error:", e)
					}
				}
			}
		}

		private case class Stopped(motive: Throwable) extends Behavior {
			override val ordinal: BehaviorOrdinal = STOPPED

			override def onEnter(previous: BehaviorOrdinal): Unit = {
				notifyListeners(_.onBecameStopped(previous, currentTerm, motive))
			}

			override def onCommandFromClient(command: ClientCommand, isFallback: Boolean): sequencer.Task[ResponseToClient] = {
				assert(isInSequence)
				sequencer.Task.successful(Unable(ordinal, otherParticipants))
			}
		}

		/** The initial state where all the common state variables are initialized.
		 * This state is transitory. When initialization is completed it transitions to the [[Isolated]] state. */
		private case class Starting(isRestart: Boolean, stateMachineNeedsRestart: Boolean) extends Behavior {
			override val ordinal: BehaviorOrdinal = STARTING

			override def onEnter(previous: BehaviorOrdinal): Unit = {
				workspace.release()
				workspace = storage.invalidWorkspace()
				notifyListeners(_.onStarting(previous, currentTerm, isRestart))
				storage.loads.trigger(true) {
					case Success(newWorkspace) =>
						// TODO consider the stateMachineNeedsRestart flag
						workspace = newWorkspace
						if newWorkspace.isBrandNew then {
							newWorkspace.setCurrentTerm(0)
							newWorkspace.setCurrentParticipants(cluster.getParticipants.toIndexedSeq.sorted)
						}
						commitIndex = 0
						currentTerm = newWorkspace.getCurrentTerm
						currentParticipants = newWorkspace.getCurrentParticipants
						otherParticipants = currentParticipants.filter(_ != hostId)
						smallestMajority = currentParticipants.length / 2 + 1
						notifyListeners(_.onStarted(previous, currentTerm, isRestart))
						become(Isolated)

					case Failure(e) =>
						scribe.error(s"$hostId: Unexpected error while restarting the consensus service:", e)
						become(Stopped(e))
				}
			}

			override def onCommandFromClient(command: ClientCommand, isFallback: Boolean): sequencer.Task[ResponseToClient] = {
				assert(isInSequence)
				sequencer.Task.successful(Unable(ordinal, otherParticipants))
			}
		}

		/**
		 * Behavior state to which the participant transitions to when the leader becomes unreachable or receives a message with a term greater than the current term.
		 * This state is abandoned when the leader is found or the majority of the participants are reachable.
		 * The [[ConsensusParticipant]] behaves exactly the same in [[Isolated]] and [[Candidate]] states. The only goal of the separation is to allow the user to distinguish if a majority of participants is reachable or not.
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
					updatesState.appointed(schedule).triggerExposingFailures()
				}
			}

			override def onCommandFromClient(command: ClientCommand, isFallback: Boolean): sequencer.Task[ResponseToClient] = {
				assert(isInSequence)
				for {
					_ <- updatesState
					result <- {
						val currentBehaviorOrdinal = currentBehavior.ordinal
						if currentBehaviorOrdinal >= FOLLOWER then currentBehavior.onCommandFromClient(command, false)
						else sequencer.Task.successful(Unable(currentBehaviorOrdinal, otherParticipants))
					}
				} yield result
			}
		}

		/**
		 * Behavior state when the participant is running for leadership.
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
					updatesState.appointed(schedule).triggerExposingFailures()
				}
			}

			override def onCommandFromClient(command: ClientCommand, isFallback: Boolean): sequencer.Task[ResponseToClient] = {
				Isolated.onCommandFromClient(command, isFallback)
			}
		}

		/**
		 * Behavior state when the participant follows a known leader.
		 *
		 * In this state, the participant acknowledges the specified leader and may
		 * transition to [[Candidate]] if the consensus protocol requires it.
		 *
		 * @param leaderId The ID of the leader this participant is following
		 */
		private case class Follower(leaderId: ParticipantId) extends Behavior {
			override val ordinal: BehaviorOrdinal = FOLLOWER

			override def onEnter(previous: BehaviorOrdinal): Unit = {
				notifyListeners(_.onBecameFollower(previous, currentTerm, leaderId))
			}

			override def onCommandFromClient(command: ClientCommand, isFallback: Boolean): sequencer.Task[ResponseToClient] = {
				assert(isInSequence)
				if isFallback then Isolated.onCommandFromClient(command, isFallback)
				else sequencer.Task.successful(RedirectTo(leaderId))
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
			private val highestRecordIndexKnowToBeReplicated_ByParticipantIndex: Array[RecordIndex] = Array.fill(otherParticipants.size)(0)

			override val ordinal: BehaviorOrdinal = LEADER

			private var failedReplicationsLoopSchedule: Maybe[sequencer.Schedule] = Maybe.empty

			override def onEnter(previous: BehaviorOrdinal): Unit = {
				currentTerm += 1
				notifyListeners(_.onBecameLeader(previous, currentTerm))
				replicateUncommitedRecords().trigger(true) {
					case Success(_) => // do nothing
					case Failure(e) =>
						scribe.trace(s"$hostId: Failed to replicate pending records:", e)
				}
			}

			override def onLeave(): Unit = {
				failedReplicationsLoopSchedule.foreach(sequencer.cancel(_))
				failedReplicationsLoopSchedule = Maybe.empty
			}

			override def onCommandFromClient(command: ClientCommand, isFallback: Boolean): sequencer.Task[ResponseToClient] = {
				assert(isInSequence)
				// append the command to the log buffer
				val commandRecord = Command(currentTerm, command)
				val index = workspace.firstEmptyRecordIndex
				workspace.appendRecord(commandRecord)

				for {
					_ <- replicateUncommitedRecords()
					response <- machine.appliesClientCommand(index, command)
				} yield Processed(index, response)
			}

			/**
			 * Replicates uncommitted records to all followers.
			 *	- If firstEmptyRecordIndex > indexOfNextRecordToSend for a follower: send AppendEntries RPC with log entries starting at indexOfNextRecordToSend
			 *		- If successful: update indexOfNextRecordToSend and matchIndex for follower
			 *		- If AppendEntries fails because of log inconsistency: decrement indexOfNextRecordToSend and retry
			 *	- If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
			 * */
			private def replicateUncommitedRecords(): sequencer.Task[Unit] = {
				// First, abort the failed replications loop if it is running.
				failedReplicationsLoopSchedule.foreach(sequencer.cancel(_))
				failedReplicationsLoopSchedule = Maybe.empty
				// Then, start regular replication to all followers.
				val until = workspace.firstEmptyRecordIndex
				// Create a task that appends uncommited records for each follower.
				val asksToAppendUncommitedRecords_byFollowerIndex =
					for followerIndex <- otherParticipants.indices yield {
						val followerId = otherParticipants(followerIndex)
						appendsRecordsToFollower(followerIndex, until)
					}
				// Execute the tasks in parallel.
				for appendResults <- sequencer.Task.sequenceHardyToArray(asksToAppendUncommitedRecords_byFollowerIndex) yield {
					// If the behavior hasn't changed, then the leader is still the same, and we can continue.
					if currentBehavior eq this then {
						// Update the commitIndex if a majority of the followers have replicated the uncommited records.
						// If there exists an N such that N > commitIndex, a majority of highestEntryIndexKnowToBeReplicated_ByParticipantIndex[i] ≥ N, and getRecordAt[N].term == currentTerm: set commitIndex = N
						var n = commitIndex + 1
						while n < workspace.firstEmptyRecordIndex && highestRecordIndexKnowToBeReplicated_ByParticipantIndex.count(_ >= n) >= smallestMajority && workspace.getRecordAt(n).term == currentTerm do {
							commitIndex = n
							n += 1
						}

						// For those minority of followers whose highestRecordIndexKnowToBeReplicated is less than the commitIndex (because they failed to replicate the uncommited records), retry to append records to them.
						// This retry is indefinite until this method (replicateUncommitedRecords) is called again (as the effect of an external stimulus).
						def loop(previousTryResults: Array[Try[Unit]]): Unit = {
							if currentBehavior ne this then return
							val schedule = sequencer.newDelaySchedule(failedReplicationsLoopInterval)
							failedReplicationsLoopSchedule = Maybe.some(schedule)
							sequencer.schedule(schedule) { () =>
								if currentBehavior eq this then {
									val retryTasks =
										for followerIndex <- previousTryResults.indices yield {
											val followerId = otherParticipants(followerIndex)
											previousTryResults(followerIndex) match {
												case Success(_) =>
													assert(highestRecordIndexKnowToBeReplicated_ByParticipantIndex(followerIndex) >= commitIndex)
													sequencer.Task.successful(())

												case Failure(e) =>
													if highestRecordIndexKnowToBeReplicated_ByParticipantIndex(followerIndex) >= commitIndex then {
														sequencer.Task.successful(())
													} else {
														scribe.info(s"$hostId: Retrying to replicate commited records to $followerId because of:", e)
														appendsRecordsToFollower(followerIndex, workspace.firstEmptyRecordIndex) // TODO Is `firstEmptyRecordIndex` the right index to retry until? Or should it be the `commitIndex`?
													}
											}
										}
									sequencer.Task.sequenceHardyToArray(retryTasks).map(loop).triggerAndForget(true)
								}
							}
						}

						loop(appendResults)
					}
				}
			}

			/**
			 * Creates a task that sends log records to the specified follower, starting from the follower's next expected record up to (but not including) the given index.
			 * If the follower's response indicates its log does not match this participant's log before `indexOfNextRecordToSend`, the task will retry after a delay, starting from the previous record.
			 * @param followerIndex The index of the follower in the [[otherParticipants]] array.
			 * @param until The index immediately after the last record to send. TODO: is this parameter needed? or should it be fixed to the [[workspace.firstEmptyRecordIndex]]?
			 * @return A task that attempts to append records to the follower.
			 */
			private def appendsRecordsToFollower(followerIndex: Int, until: RecordIndex): sequencer.Task[Unit] = {
				val followerId = otherParticipants(followerIndex)
				val indexOfNextRecordToSend = indexOfNextRecordToSend_ByParticipantIndex(followerIndex)
				if until <= indexOfNextRecordToSend then sequencer.Task.successful(Maybe.empty)
				else {
					val recordsToSend = workspace.getRecordsBetween(indexOfNextRecordToSend, until)
					val previousRecordIndex = indexOfNextRecordToSend - 1
					val previousRecordTerm = if previousRecordIndex == 0 then 0 else workspace.getRecordAt(previousRecordIndex).term
					for {
						AppendResult(followerTerm, success, ordinal) <- followerId.asksToAppendRecords(currentTerm, previousRecordIndex, previousRecordTerm, recordsToSend, commitIndex)
						result <- {
							// If the behavior haven't changed, then the leader is still the same, and we can continue.
							if currentBehavior ne this then sequencer.Task.successful(())
							else {
								val indexOfNextRecordToSend = indexOfNextRecordToSend_ByParticipantIndex(followerIndex)
								if followerTerm > currentTerm then {
									// If the term is greater than the current term, then the leader has changed. So, start the state-update process.
									currentTerm = followerTerm
									updatesState.triggerExposingFailures()
									// Yield a successful result to avoid the retry loop.
									sequencer.Task.successful(())
								} else if success then {
									// If the follower has successfully appended the records, then update the index of the next record to send and the highest record index known to be replicated.
									indexOfNextRecordToSend_ByParticipantIndex(followerIndex) += recordsToSend.size
									highestRecordIndexKnowToBeReplicated_ByParticipantIndex(followerIndex) = indexOfNextRecordToSend + recordsToSend.size
									sequencer.Task.successful(())
								} else if indexOfNextRecordToSend > workspace.logBufferOffset then {
									// If the follower has not successfully appended the records because his the previous record is not the same as the previous record in my log, then try again at the previous record.
									indexOfNextRecordToSend_ByParticipantIndex(followerIndex) = indexOfNextRecordToSend - 1
									appendsRecordsToFollower(followerIndex, until)
								} else {
									scribe.error(s"$hostId: Unable to replicate uncommited records to $followerId because its log has inconsitencies at records that I already snapshotted (they are at indexes less than my logBufferOffset ($workspace.logBufferOffset)). THIS SHOULD NOT HAPPEN. PLEASE REPORT THIS AS A BUG.")
									sequencer.Task.successful(())
								}
							}
						}
					} yield result
				}
			}
		}

		//// UTILITIES USED BY MANY BEHAVIORS

		/**
		 * Determines the best leader candidate based on the [StateInfo] of the inquirer, the host, and the other participants.
		 * Design note: The caller should not update any state variable before calling this method.
		 *
		 * @param inquirerId the id of the participant that asked me to cast a vote; or null if asking myself.
		 * @param inquirerInfo The candidate info of the inquirer; or null if asking myself.
		 * @return A task that yields a [[Vote]] with the chosen leader for the current term.
		 */
		private def decidesMyVote(inquirerId: ParticipantId | Null, inquirerInfo: StateInfo): sequencer.Task[Vote] = {
			val areYouThereQuestions: IndexedSeq[sequencer.Task[StateInfo]] =
				for replierId <- otherParticipants yield
					if replierId == inquirerId then sequencer.Task.successful(inquirerInfo)
					else replierId.asksHowAreYou(this.currentTerm, false)

			for replies <- sequencer.Task.sequenceHardyToArray(areYouThereQuestions) yield {
				var laterCurrentTerm = this.currentTerm
				var chosenCandidate = CandidateInfo(hostId, this.currentBehavior.buildMyStateInfo)
				var reachableCandidatesCounter = 1 // myself
				for replierIndex <- replies.indices do {
					val replierId = otherParticipants(replierIndex)
					replies(replierIndex) match {
						case Success(replierInfo) =>
							reachableCandidatesCounter += 1
							if replierInfo.currentTerm > laterCurrentTerm then {
								laterCurrentTerm = replierInfo.currentTerm
							}
							chosenCandidate = chosenCandidate.getWinnerAgainst(CandidateInfo(replierId, replierInfo))

						case Failure(e) =>
							scribe.trace(s"$hostId: `$replierId.asksHowAreYou($currentTerm)` failed with:", e)
					}
				}
				Vote(laterCurrentTerm, chosenCandidate.id, reachableCandidatesCounter, chosenCandidate.info.ordinal)
			}
		}

		/**
		 * Knows all the information about a candidate necessary by participants to decide which to vote in a leader election.
		 */
		private class CandidateInfo(val id: ParticipantId, val info: StateInfo) {
			/** @return the winner of the competition between this candidate and the other candidate when competing for leadership. The winner is the more up-to-date one.
			 * The more up-to-date criteria are: greater last record term, longer log, greater current term, greater behavior ordinal, and lesser [[ParticipantId]], with the left to right priority.
			 */
			def getWinnerAgainst(other: CandidateInfo): CandidateInfo = {
				if this.info.lastRecordTerm > other.info.lastRecordTerm then this
				else if this.info.lastRecordTerm < other.info.lastRecordTerm then other
				else if this.info.lastRecordIndex > other.info.lastRecordIndex then this
				else if this.info.lastRecordIndex < other.info.lastRecordIndex then other
				else if this.info.currentTerm > other.info.currentTerm then this
				else if this.info.currentTerm < other.info.currentTerm then other
				else if this.info.ordinal == LEADER && other.info.ordinal != LEADER then this
				else if this.info.ordinal != LEADER && other.info.ordinal == LEADER then other
				else if this.id < other.id then this
				else other
			}
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

		private inline def notifyListeners(inline what: NotificationListener => Unit): Unit = {
			notificationListeners.forEach { (listener, _) => what(listener) }
		}
	}
}
