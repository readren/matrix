package readren.matrix
package consensus

import readren.taskflow.SchedulingExtension.MilliDuration
import readren.taskflow.{Doer, Maybe, SchedulingExtension}

import java.util.Comparator
import scala.math.Ordering.Implicits.infixOrderingOps
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

/*
 * A consensus algorithm (like Raft or Paxos) for managing a replicated log.
 * Consensus involves multiple [[Participants]] instances (usually hosted by different nodes in a distributed system) agreeing on values. Once they reach a decision on a value, that decision is final.
 * Like Raft, this consensus algorithm relies on strong leadership and makes progress when any majority of their participants is available.
 * But it differs from Raft in that it minimizes the number of messages sent between participants by reducing the heartbeat messages.
 * 
 * The algorithm is based on the following principles:
 * - Each participant has a unique id.
 * - Each participant has a current term.
 * - Each participant has a log of records.
 * - Each participant has a leader.
 * - Each participant has a state.
 */
trait Conciliator {

	/** The type of record indices.
	 * Index base is 1.
	 * Zero means before the first record. */
	final type RecordIndex = Long
	/** The type of terms.
	 * Starts from 1.
	 * Zero means "before first election". */
	final type Term = Int
	/** The type of behavior ids. */
	final type BehaviorOrdinal = Int

	/** The type of participant ids. */
	type ParticipantId <: AnyRef: {Ordering, ClassTag}
	/** The type of client commands. */
	type ClientCommand

	//// CONFIGURATION

	/** Tells how many times the task returned by [[appliesClientCommand]] method should be tried when its execution fails.
	 * After exceeding the specified number of retries, the state machine is considered corrupted, and therefore the consensus participant service stops because it has no way to continue replicating. */
	def applyCommandRetries: Int = 1

	def isEager: Boolean = false

	def isolatedMainLoopInterval: MilliDuration = 500

	def candidateMainLoopInterval: MilliDuration = 500

	def leaderMainLoopInterval: MilliDuration = 500

	def failedReplicationsLoopInterval: MilliDuration = 500

	//// THREADING & TIMING

	/** The tool that the hosting [[ConsensusParticipant]] uses to mutate its state and schedule executions sequentially. */
	val sequencer: Doer & SchedulingExtension

	inline def isInSequence: Boolean = sequencer.assistant.isWithinDoSiThEx

	//// STATE MACHINE

	/** Gives the [[sequencer.Task]] that applies the [[ClientCommand]] to the state-machine. */
	def appliesClientCommand(command: ClientCommand): sequencer.Task[ResponseToClient]

	//// RESPONSE TO CLIENT

	trait ResponseToClient

	case class RecordConsumed(recordIndex: RecordIndex) extends ResponseToClient

	case class RedirectTo(participantId: ParticipantId) extends ResponseToClient

	case class NoConsensus(behaviorOrdinal: BehaviorOrdinal) extends ResponseToClient

	//// DATA TYPES

	/** A vote for a leader.
	 * @param term The term for which the vote is cast.
	 * @param candidateId The id of the voted candidate.
	 * @param reachableCandidateCount The number of candidates that were reachable by the voter (including the voter itself) when the vote was cast.
	 * @param behaviorOrdinal The behavior of the voted candidate.
	 */
	case class Vote(term: Term, candidateId: ParticipantId, reachableCandidateCount: Int, behaviorOrdinal: BehaviorOrdinal)

	case class AppendResult(term: Term, success: Boolean, behaviorOrdinal: BehaviorOrdinal)

	/**
	 * Knows all the information about a candidate necessary by participants to decide which to vote in a leader election.
	 * The response to a "how are you" question from a participant to another.
	 * @param currentTerm The term of the participant that is answering.
	 * @param ordinal The behavior of the participant that is answering.
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
			def onCommandFromClient(command: ClientCommand): sequencer.Task[ResponseToClient]

			def onHowAreYou(senderId: ParticipantId, senderTerm: Term): StateInfo

			/**
			 * This method is invoked by this cluster when another participant calls [[askToChooseForLeader]].
			 *
			 * @param senderId The id of the participant that called [[askToChooseForLeader]].
			 * @param senderInfo Information about the state of the participant that called.
			 * @return A [[sequencer.Task]] that yields a [[Vote]] indicating the candidate chosen by the listening participant for the specified term.
			 */
			def onChooseALeader(senderId: ParticipantId, senderInfo: StateInfo): sequencer.Task[Vote]

			def onAppendRecords(senderId: ParticipantId, senderTerm: Term, leaderId: ParticipantId, prevLogIndex: RecordIndex, prevLogTerm: Term, records: IndexedSeq[Record], leaderCommit: RecordIndex): sequencer.Task[AppendResult]
		}

		/**
		 * Sets the message listener.
		 * Must be called within the [[sequencer]] thread. */
		def setsMessagesListener(listener: MessagesListener): Unit

		extension (destinationId: ParticipantId) {
			def asksHowAreYou(senderTerm: Term): sequencer.Task[StateInfo]
			def askToChooseForLeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.Task[Vote]
			@deprecated("Not used.", "0.1.0")
			def imposeLeadership(senderTerm: Term): Term
			def asksToAppendRecords(senderTerm: Term, leaderId: ParticipantId, prevLogIndex: RecordIndex, prevLogTerm: Term, records: IndexedSeq[Record], leaderCommit: RecordIndex): sequencer.Task[AppendResult]
		}
	}

	//// LOG RECORD

	sealed trait Record {
		def term: Term
	}

	private case class Command(override val term: Term, command: ClientCommand) extends Record

	private case class LeaderTransition(override val term: Term) extends Record

	private case class SnapshotPoint(override val term: Term) extends Record

	private case class ConfigurationChangeOldNew(override val term: Term, oldParticipants: Set[ParticipantId], newParticipants: Set[ParticipantId]) extends Record

	private case class ConfigurationChangeNew(term: Term, newParticipants: Set[ParticipantId]) extends Record

	//// PERSISTENCE 

	trait Repository {

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
		def getRecordsBetween(from: RecordIndex, until: RecordIndex): IndexedSeq[Record] // = logBuffer.slice((from - logBufferOffset).toInt, (until - logBufferOffset).toInt)

		def appendRecord(record: Record): RecordIndex

		/** Appends any new record not already in the log starting at the specified index.
		 * If a conflict is detected (i.e., a stored record and a new record at the same index have different terms), all stored records from that index onward are removed before appending the new records.
		 * @return The index of the last record appended. */
		def appendResolvingConflicts(records: IndexedSeq[Record], from: RecordIndex): RecordIndex

		/** Should be called whenever the commit index changed to allow this [[Repository]] to release the memory used to memorize already commited records. */
		def informCommitIndex(commitIndex: RecordIndex): Unit

		def release(): Unit

		inline def getRecordTermAt(index: RecordIndex): Term =
			if index == 0 then 0
			else getRecordAt(index).term
	}

	/** Defines the requirements a [[ConsensusParticipant]] service has for the persistence service hosted within the same participant.
	 * Implementations may assume that all methods of this trait are invoked within the [[sequencer]] thread, enabling optimizations such as avoiding unnecessary creation of new task objects. */
	trait Memory {
		/** Creates a new invalid repository whose methods should terminate abruptly. */
		def invalidRepository(): Repository

		val loads: sequencer.Task[Repository]

		/** Saves the repository to the persistence memory.
		 * Design Note: A failure to save the repository should restart the [[ConsensusParticipant]] as if it had crashed and lost all non-persistent variables.
		 */
		def saves(repository: Repository): sequencer.Task[Unit]
	}

	trait NotificationListener {
		def onStarting(isRestart: Boolean): Unit

		def onStarted(isRestart: Boolean): Unit

		def onStoped(): Unit
	}


	//// PARTICIPANT'S CONSENSUS SERVICE

	/**
	 * Consensus service for a participant.
	 * Services and represents a participant in the consensus algorithm that coordinates with other participants to achieve distributed agreement.
	 *
	 * This class implements a Raft-like consensus algorithm where each participant can be in one of four states:
	 * - [[Isolated]]: Initial state when the participant is not connected to a leader
	 * - [[Candidate]]: State when the participant is running for leadership
	 * - [[Follower]]: State when the participant follows a known leader
	 * - [[Leader]]: State when the participant is the current leader
	 *
	 * Key responsibilities:
	 * - Managing consensus state and term progression
	 * - Coordinating leader election and state transitions
	 * - Handling consensus protocol messages
	 * - Ensuring thread-safe operations through the sequencer
	 *
	 * Only one [[ConsensusParticipant]] instance should exist per participant in the cluster.
	 * All state mutations must occur within the sequencer thread to ensure consistency.
	 */
	class ConsensusParticipant(val cluster: Cluster, memory: Memory, notificationListeners: java.util.WeakHashMap[NotificationListener, None.type]) { thisConsensusParticipant =>

		import cluster.*

		private inline val STOPPED = 0
		private inline val STARTING = 1
		private inline val ISOLATED = 2
		private inline val CANDIDATE = 3
		private inline val FOLLOWER = 4
		private inline val LEADER = 5

		/** The current set of participants in the cluster, according to this participant, sorted.
		 * Should be reflected in the [[Repository]].
		 * */
		private var currentParticipants: IndexedSeq[ParticipantId] = IndexedSeq.empty

		/** Should be updated whenever [[currentParticipants]] mutates */
		private var smallestMajority: Int = 0

		/** Should be updated whenever [[currentParticipants]] mutates */
		private var otherParticipants: IndexedSeq[ParticipantId] = IndexedSeq.empty

		/** The current term according to this participant.
		 * The Initial value is zero.
		 * Zero means "before the first election".
		 * Should be reflected in the [[Repository]].
		 * */
		private var currentTerm: Term = 0

		/** The index of the highest entry known to be committed according to this participant.
		 * A log record is committed once the leader that created the record has replicated it on a majority of the servers.
		 * This also commits all preceding records in the leader’s log, including records created by previous leaders.
		 * Should be informed to the [[Repository]] whenever it changes. */
		private var commitIndex: RecordIndex = 0

		private var repository: Repository = memory.invalidRepository()

		/** The current behavior of this participant. */
		private var currentBehavior: Behavior = Starting(false)

		/** Memorices the schedule that is currently scheduled. Needed to cancel the schedule when becoming a new behavior. */
		private var currentSchedule: Maybe[sequencer.Schedule] = Maybe.empty

		private object participantIdComparator extends Comparator[ParticipantId] {
			private val ordering = summon[Ordering[ParticipantId]]

			override def compare(a: ParticipantId, b: ParticipantId): BehaviorOrdinal = ordering.compare(a, b)
		}

		/** @return the index of the specified [[ParticipantId]] in the [[currentParticipants]] array. */
		private inline def participantIndexOf(participantId: ParticipantId): Int = {
			java.util.Arrays.binarySearch(currentParticipants.asInstanceOf[Array[ParticipantId]], participantId, participantIdComparator)
		}

		/**
		 * Transitions the participant to a new behavior.
		 */
		private def become(newBehavior: Behavior): Behavior = {
			assert(isInSequence)
			if newBehavior ne currentBehavior then {
				currentSchedule.foreach(sequencer.cancel(_))
				currentSchedule = Maybe.empty
				currentBehavior = newBehavior
				cluster.setsMessagesListener(newBehavior)
				newBehavior.run()
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
		private sealed abstract class Behavior extends MessagesListener, Runnable {
			val ordinal: BehaviorOrdinal

			def buildMyStateInfo: StateInfo = {
				if ordinal <= STARTING then StateInfo(0, ordinal, 0, 0)
				else {
					val lastRecordIndex = repository.firstEmptyRecordIndex - 1
					val lastRecordTerm = repository.getRecordTermAt(lastRecordIndex)
					StateInfo(currentTerm, ordinal, lastRecordTerm, lastRecordIndex)
				}
			}

			override def onHowAreYou(senderId: ParticipantId, senderTerm: Term): StateInfo = {
				if senderTerm > currentTerm && ordinal >= ISOLATED then {
					currentTerm = senderTerm
					// start the state-updater process
					updatesState.triggerAndForget(true)
				}
				buildMyStateInfo
			}

			override def onChooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.Task[Vote] = {
				if ordinal <= STARTING then sequencer.Task.successful(Vote(0, hostId, 0, ordinal))
				else {
					decidesMyVote(inquirerId, inquirerInfo).andThen {
						case Success(vote) =>
							if (vote.term > currentTerm && ordinal >= ISOLATED) || (vote.reachableCandidateCount < smallestMajority && ordinal >= CANDIDATE) then {
								// start the state-updater process
								updatesState(vote).triggerAndForget(true)
							}
						case Failure(e) =>
							// start the state-updater process
							updatesState.triggerAndForget(true)
					}
				}
			}

			override def onAppendRecords(senderId: ParticipantId, senderTerm: Term, leaderId: ParticipantId, prevLogIndex: RecordIndex, prevLogTerm: Term, records: IndexedSeq[Record], leaderCommit: RecordIndex): sequencer.Task[AppendResult] = {
				if ordinal < ISOLATED then sequencer.Task.successful(AppendResult(0, false, ordinal))
				else if senderTerm < currentTerm then sequencer.Task.successful(AppendResult(currentTerm, false, ordinal))
				else if senderTerm > currentTerm then {
					currentTerm = senderTerm
					for {
						_ <- updatesState // TODO avoid the double save: one by the `updatesState` here, and the other in `appliesCommitedRecordsAndSavesTheRepository`.
						appendResult <- onAppendRecords(senderId, senderTerm, leaderId, prevLogIndex, prevLogTerm, records, leaderCommit)
					} yield appendResult
				} else if repository.getRecordTermAt(prevLogIndex) != prevLogTerm then sequencer.Task.successful(AppendResult(currentTerm, false, ordinal))
				else {
					assert(senderTerm == currentTerm)
					// Append any new entry not already in the log resolving conflicts.
					val lastAppendedRecordIndex = repository.appendResolvingConflicts(records, prevLogIndex + 1)

					val newCommitIndex = if leaderCommit < lastAppendedRecordIndex then leaderCommit else lastAppendedRecordIndex

					// Unconditionally apply commited records to the state machine (in log order)
					val commitedRecordsIterator = repository.getRecordsBetween(commitIndex + 1, newCommitIndex).iterator

					def applyNextCommitedRecord(): sequencer.Task[Any] = {
						if commitedRecordsIterator.hasNext then {
							val task: sequencer.Task[Any] = commitedRecordsIterator.next() match {
								case command: Command =>
									appliesClientCommand(command.command)
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
									applyNextCommitedRecord()
								case Failure(e) =>
									if retriesCounter < applyCommandRetries then {
										retriesCounter += 1
										task
									} else sequencer.Task.failed(e)
							}
						} else sequencer.Task.successful(())
					}

					val appliesCommitedRecordsAndSavesTheRepository = for {
						_ <- applyNextCommitedRecord()
						_ = commitIndex = newCommitIndex
						_ = repository.informCommitIndex(newCommitIndex)
						_ <- memory.saves(repository)
					} yield AppendResult(currentTerm, true, ordinal)


					appliesCommitedRecordsAndSavesTheRepository.recover {
						case e =>
							scribe.error(s"$hostId: Unexpected error while applying commited records to the state machine or saving the repository. This participant's consensus service is unable to continue following the leader and will stop.", e)
							become(Stopped)
							AppendResult(0, false, ordinal)
					}

				}
			}

			protected def updatesState: sequencer.Task[Unit] = {
				assert(ordinal >= ISOLATED)
				for {
					myVote <- decidesMyVote(null, null)
					saveResult <- updatesState(myVote)
				} yield saveResult
			}

			protected def updatesState(myVote: Vote): sequencer.Task[Unit] = {
				inline def onConsensus(): Unit = {
					if myVote.candidateId == hostId then {
						currentTerm += 1
						become(Leader())
					} else become(Follower(myVote.candidateId))
				}

				assert(ordinal >= ISOLATED)
				if myVote.term > currentTerm then currentTerm = myVote.term

				if myVote.reachableCandidateCount == currentParticipants.size then {
					onConsensus()
					repository.setCurrentTerm(currentTerm)
					memory.saves(repository)
				} else if myVote.reachableCandidateCount < smallestMajority then {
					become(Isolated)
					repository.setCurrentTerm(currentTerm)
					memory.saves(repository)
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
							if myVoteIsValid && votesCount >= smallestMajority then onConsensus()
							else become(Candidate)

							repository.setCurrentTerm(currentTerm)
							memory.saves(repository)
						}
					} yield saveResult
				}
			}
		}

		private case object Stopped extends Behavior {
			override val ordinal: BehaviorOrdinal = STOPPED

			override def run(): Unit = {
				// No operation for Stopped state
			}

			override def onCommandFromClient(command: ClientCommand): sequencer.Task[ResponseToClient] = {
				sequencer.Task.successful(NoConsensus(ordinal))
			}
		}

		/** The initial state where all the common state variables are initialized.
		 * This state is transitory. When initialization is completed it transitions to the [[Isolated]] state. */
		private case class Starting(isRestart: Boolean) extends Behavior {
			override val ordinal: BehaviorOrdinal = STARTING

			override def run(): Unit = {
				repository.release()
				repository = memory.invalidRepository()
				notifyListeners(_.onStarting(isRestart))
				memory.loads.trigger(true) {
					case Success(newRepository) =>
						repository = newRepository
						if newRepository.isBrandNew then {
							newRepository.setCurrentTerm(0)
							newRepository.setCurrentParticipants(cluster.getParticipants.toIndexedSeq.sorted)
						}
						commitIndex = 0
						currentTerm = newRepository.getCurrentTerm
						currentParticipants = newRepository.getCurrentParticipants
						otherParticipants = currentParticipants.filter(_ != hostId)
						smallestMajority = currentParticipants.length / 2 + 1
						become(Isolated)
						notifyListeners(_.onStarted(isRestart))

					case Failure(e) =>
						scribe.error(s"$hostId: Unexpected error while restarting the consensus service:", e)
						become(Stopped)
						notifyListeners(_.onStoped())
				}
			}

			override def onCommandFromClient(command: ClientCommand): sequencer.Task[ResponseToClient] = {
				sequencer.Task.successful(NoConsensus(ordinal))
			}
		}

		/**
		 * Behavior state to which the participant transitions to when the leader becomes unreachable or receives a message with a term greater than the current term.
		 * This state is abandoned when the leader is found or the majority of the participants are reachable.
		 */
		private case object Isolated extends Behavior {
			override val ordinal: BehaviorOrdinal = ISOLATED

			/**
			 * The main loop of the isolated state.
			 * It checks if the current term leader is reachable or the reachable participants including itself are the majority.
			 * If so, it becomes a follower or a candidate respectively.
			 * If not, it stays in the isolated state and checks again after a while.
			 */
			override def run(): Unit = {
				if isEager then {
					val schedule = sequencer.newDelaySchedule(isolatedMainLoopInterval)
					currentSchedule = Maybe.some(schedule)
					// schedule the state-updater process
					updatesState.appointed(schedule).triggerAndForget(true)
				}
			}

			override def onCommandFromClient(command: ClientCommand): sequencer.Task[ResponseToClient] = {
				for {
					_ <- updatesState
					result <-
						if ordinal >= FOLLOWER then currentBehavior.onCommandFromClient(command)
						else sequencer.Task.successful(NoConsensus(ordinal))
				} yield result
			}
		}

		/**
		 * Behavior state when the participant is running for leadership.
		 *
		 * In this state, the participant participates in leader election and may transition to [[Leader]], [[Follower]], or [[Isolated]] based on the election outcome.
		 * A new term is started when becoming leader.
		 */
		private case object Candidate extends Behavior {
			override val ordinal: BehaviorOrdinal = CANDIDATE

			override def run(): Unit = {
				if isEager then {
					val schedule = sequencer.newDelaySchedule(candidateMainLoopInterval)
					currentSchedule = Maybe.some(schedule)
					// schedule the state-update process
					updatesState.appointed(schedule).triggerAndForget(true)
				}
			}

			override def onCommandFromClient(command: ClientCommand): sequencer.Task[ResponseToClient] = {
				for {
					_ <- updatesState
					result <-
						if ordinal >= FOLLOWER then currentBehavior.onCommandFromClient(command)
						else sequencer.Task.successful(NoConsensus(ordinal))
				} yield result
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

			override def run(): Unit = {
				// TODO should I do something here?
			}

			override def onCommandFromClient(command: ClientCommand): sequencer.Task[ResponseToClient] = {
				sequencer.Task.successful(RedirectTo(leaderId))
			}
		}

		/**
		 * Behavior state when the participant is the current leader.
		 *
		 * In this state, the participant coordinates consensus decisions and may
		 * transition to [[Candidate]] if the consensus protocol requires it.
		 */
		private case class Leader() extends Behavior {

			/** The index of the next record to send to a participant, indexed by the participant index.
			 * This array is optimistically initialized to the first empty record index of the leader's repository for all participants,
			 * assuming that each follower's log is already up-to-date with the leader's log. This optimistic initialization
			 * allows the leader to attempt to append new entries immediately, but if a follower's log is actually behind or inconsistent,
			 * the index will be decremented as needed until the logs are aligned.
			 * When a record is successfully replicated to a participant, the index of the next record to send to that participant is incremented.
			 * When a record is not successfully replicated to a participant, the index of the next record to send to that participant is decremented.
			 */
			private val indexOfNextRecordToSend_ByParticipantIndex: Array[RecordIndex] = Array.fill(otherParticipants.size)(repository.firstEmptyRecordIndex)
			/** The highest record index known to be replicated to a participant, indexed by the participant index.
			 * This array is conservatively initialized to 0 for all participants, assuming that no records are known to be replicated to any follower at the start of the leader's term.
			 * As records are successfully replicated to a participant, the corresponding value is incremented.
			 * This conservative initialization ensures that the leader does not overestimate the replication state of any follower and only advances commitIndex when a true majority is confirmed.
			 */
			private val highestRecordIndexKnowToBeReplicated_ByParticipantIndex: Array[RecordIndex] = Array.fill(otherParticipants.size)(0)

			override val ordinal: BehaviorOrdinal = LEADER

			override def run(): Unit = {
				replicateUncommitedRecords().trigger(true) {
					case Success(_) => // do nothing
					case Failure(e) =>
						scribe.trace(s"$hostId: Failed to replicate pending records:", e)
				}
				val delay = sequencer.newDelaySchedule(leaderMainLoopInterval)
				currentSchedule = Maybe.some(delay)
				sequencer.scheduleSequentially(delay)(currentBehavior)
			}

			var failedReplicationsLoopSchedule: Maybe[sequencer.Schedule] = Maybe.empty

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
				val until = repository.firstEmptyRecordIndex
				// Create a task that appends uncommited records for each follower.
				val asksToAppendUncommitedRecords_byFollowerIndex =
					for followerIndex <- otherParticipants.indices yield {
						val followerId = otherParticipants(followerIndex)
						appendsRecordsToFollower(followerIndex, until)
					}
				// Execute the tasks in parallel.
				for appendResults <- sequencer.Task.sequenceHardyToArray(asksToAppendUncommitedRecords_byFollowerIndex) yield {
					// If the behavior haven't changed, then the leader is still the same, and we can continue.
					if currentBehavior eq this then {
						// Update the commitIndex if a majority of the followers have replicated the uncommited records.
						// If there exists an N such that N > commitIndex, a majority of highestEntryIndexKnowToBeReplicated_ByParticipantIndex[i] ≥ N, and getRecordAt[N].term == currentTerm: set commitIndex = N
						var n = commitIndex + 1
						while highestRecordIndexKnowToBeReplicated_ByParticipantIndex.count(_ >= n) >= smallestMajority && repository.getRecordAt(n).term == currentTerm do {
							commitIndex = n
							n += 1
						}

						// For those minority of followers whose highestRecordIndexKnowToBeReplicated is less than the commitIndex (because they failed to replicate the uncommited records), retry to append records to them.
						// This retry is indefinite until this method (replicateUncommitedRecords) is called again (as the effect of an external stimulus).
						def loop(previousTryResults: Array[Try[Unit]]): Unit = {
							if currentBehavior ne this then return
							val schedule = sequencer.newDelaySchedule(failedReplicationsLoopInterval)
							failedReplicationsLoopSchedule = Maybe.some(schedule)
							sequencer.scheduleSequentially(schedule) { () =>
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
														appendsRecordsToFollower(followerIndex, commitIndex) // TODO Is `commitIndex` the right index to retry until? Or should it be the index of the last record in the log?
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
			 * Builds a task that appends to the specified follower the records between the follower's next record to send and the given index.
			 * If the follower responce is not successful because his log does not match mine before the `indexOfNextRecordToSend`, then the task will retry to append the records again starting from the previous record after a delay.
			 * @param followerIndex The index of the follower in the [[otherParticipants]] array.
			 * @param until The index after the last record to send.
			 * @return A task that appends records to the follower.
			 */
			private def appendsRecordsToFollower(followerIndex: Int, until: RecordIndex): sequencer.Task[Unit] = {
				val followerId = otherParticipants(followerIndex)
				val indexOfNextRecordToSend = indexOfNextRecordToSend_ByParticipantIndex(followerIndex)
				if until <= indexOfNextRecordToSend then sequencer.Task.successful(Maybe.empty)
				else {
					val recordsToSend = repository.getRecordsBetween(indexOfNextRecordToSend, until)
					val previousRecordIndex = indexOfNextRecordToSend - 1
					val previousRecordTerm = if previousRecordIndex == 0 then 0 else repository.getRecordAt(previousRecordIndex).term
					for {
						AppendResult(followerTerm, success, ordinal) <- followerId.asksToAppendRecords(currentTerm, hostId, previousRecordIndex, previousRecordTerm, recordsToSend, commitIndex)
						result <- {
							// If the behavior haven't changed, then the leader is still the same, and we can continue.
							if currentBehavior ne this then sequencer.Task.successful(())
							else {
								val indexOfNextRecordToSend = indexOfNextRecordToSend_ByParticipantIndex(followerIndex)
								if followerTerm > currentTerm then {
									// If the term is greater than the current term, then the leader has changed. So, start the state-update process. 
									currentTerm = followerTerm
									updatesState.triggerAndForget(true)
									// Yield a successful result to avoid the retry loop.
									sequencer.Task.successful(())
								} else if success then {
									// If the follower has successfully appended the records, then update the index of the next record to send and the highest record index known to be replicated.
									indexOfNextRecordToSend_ByParticipantIndex(followerIndex) += recordsToSend.size
									highestRecordIndexKnowToBeReplicated_ByParticipantIndex(followerIndex) = indexOfNextRecordToSend + recordsToSend.size
									sequencer.Task.successful(())
								} else if indexOfNextRecordToSend > repository.logBufferOffset then {
									// If the follower has not successfully appended the records because his the previous record is not the same as the previous record in my log, then try again at the previous record.
									indexOfNextRecordToSend_ByParticipantIndex(followerIndex) = indexOfNextRecordToSend - 1
									appendsRecordsToFollower(followerIndex, until)
								} else {
									scribe.error(s"$hostId: Unable to replicate uncommited records to $followerId because its log has inconsitencies at records that I already snapshotted (they are at indexes less than my logBufferOffset ($repository.logBufferOffset)). THIS SHOULD NOT HAPPEN. PLEASE REPORT THIS AS A BUG.")
									sequencer.Task.successful(())
								}
							}							
						}
					} yield result
				}
			}

			override def onCommandFromClient(command: ClientCommand): sequencer.Task[ResponseToClient] = {
				// append the command to the log buffer
				val commandRecord = Command(currentTerm, command)
				repository.appendRecord(commandRecord)

				for {
					_ <- replicateUncommitedRecords()
					response <- appliesClientCommand(command)
				} yield response
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
					else replierId.asksHowAreYou(this.currentTerm)

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
			 * The more up-to-date criteria are: greater last record term, longer log, greater current term, and lesser [[ParticipantId]], with the left to right priority.
			 */
			def getWinnerAgainst(other: CandidateInfo): CandidateInfo = {
				if this.info.lastRecordTerm > other.info.lastRecordTerm then this
				else if this.info.lastRecordTerm < other.info.lastRecordTerm then other
				else if this.info.lastRecordIndex > other.info.lastRecordIndex then this
				else if this.info.lastRecordIndex < other.info.lastRecordIndex then other
				else if this.info.currentTerm > other.info.currentTerm then this
				else if this.info.currentTerm < other.info.currentTerm then other
				else if this.info.ordinal > other.info.ordinal then this
				else if this.info.ordinal < other.info.ordinal then other
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
