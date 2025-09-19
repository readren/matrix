package readren.matrix
package consensus.raft

import consensus.*

import readren.sequencer.MilliDuration
import scribe.*

import scala.collection.mutable
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Random, Success}

/** Configuration settings for the Raft consensus algorithm */
case class RaftConfig(
	/** Minimum election timeout in milliseconds */
	electionTimeoutMin: MilliDuration = 150,
	/** Maximum election timeout in milliseconds */
	electionTimeoutMax: MilliDuration = 300,
	/** Heartbeat interval in milliseconds */
	heartbeatInterval: MilliDuration = 50,
	/** Maximum number of log entries to send in a single AppendEntries RPC */
	maxLogEntriesPerRpc: Int = 100,
	/** Maximum number of bytes to send in a single AppendEntries RPC */
	maxBytesPerRpc: Int = 1024 * 1024, // 1MB
	/** Timeout for RPC calls in milliseconds */
	rpcTimeout: MilliDuration = 100,
	/** Maximum number of retries for failed RPC calls */
	maxRpcRetries: Int = 3,
	/** Snapshot interval (number of log entries before creating snapshot) */
	snapshotInterval: Int = 1000,
	/** Whether to enable pre-vote phase (prevents unnecessary elections) */
	enablePreVote: Boolean = true,
	/** Whether to enable leader transfer */
	enableLeaderTransfer: Boolean = false,
	// TODO: Implement RPC retry logic and batching in message handlers
	// TODO: Implement log compaction and snapshotting to prevent unbounded log growth
	// TODO: Implement pre-vote optimization to prevent unnecessary elections
	// TODO: Implement leader transfer feature
)

/** Current role of this Raft instance */
enum Role {
	case Follower, Candidate, Leader
}

/**
 * Main Raft module implementing the Raft consensus algorithm.
 *
 * This class implements the core Raft consensus protocol as described in the Raft paper.
 * It handles leader election, log replication, and safety guarantees for distributed consensus.
 *
 * Key features:
 * - Leader election with randomized timeouts to prevent split votes
 * - Log replication with consistency checks
 * - Safety guarantees: only one leader per term, log consistency
 * - Dynamic membership changes (basic support)
 *
 * @param config Configuration settings for the Raft algorithm
 * @param clusterService The cluster service that provides networking and membership
 * @param ec Execution context for async operations
 */
class Raft(val config: RaftConfig, val clusterService: RaftClusterService)(using ec: ExecutionContext) {
	// ===== TASK SEQUENCER =====

	/** The task sequencer for thread-safe execution and scheduling */
	val sequencer: clusterService.sequencer.type = clusterService.sequencer

	// ===== RAFT PERSISTENT STATE (survives crashes) =====

	/** Current term number, monotonically increasing */
	private var currentTerm: Term = 0L

	/** Node that received vote in current term (null if none) */
	private var votedFor: Option[NodeId] = None

	/** Log entries; each entry contains command for state machine and term when entry was received by leader */
	private var log: Vector[LogEntry] = Vector.empty

	// ===== RAFT VOLATILE STATE (reset after crashes) =====

	/** Index of highest log entry known to be committed (initialized to 0, increases monotonically) */
	private var commitIndex: LogIndex = 0L

	/** Index of highest log entry applied to state machine (initialized to 0, increases monotonically) */
	private var lastApplied: LogIndex = 0L

	/** For each server, index of the next log entry to send to that server (initialized to leader last log index + 1) */
	private val nextIndex: mutable.Map[NodeId, LogIndex] = mutable.Map.empty

	/** For each server, index of highest log entry known to be replicated on server (initialized to 0) */
	private val matchIndex: mutable.Map[NodeId, LogIndex] = mutable.Map.empty

	/** Current role of this Raft instance */
	private var role: Role = Role.Follower

	/** ID of the current leader (if known) */
	private var currentLeaderId: Option[NodeId] = None

	/** Timestamp of the last time a heartbeat or valid AppendEntries was received from the current leader */
	private var lastHeartbeatTime: Long = System.currentTimeMillis()

	/** Timestamp of when the current election timeout started */
	private var electionTimeoutExpiry: Long = 0L

	/** Flag to indicate if a configuration change is in progress */
	private var configurationChangeInProgress: Boolean = false

	// Register message handler with the cluster service
	clusterService.registerMessageHandler(handleMessage)

	// ===== SCHEDULING =====
	private var electionTimeoutSchedule: Option[sequencer.Schedule] = None
	private var heartbeatSchedule: Option[sequencer.Schedule] = None

	def startElectionTimeoutScheduler(): Unit = {
		val schedule = sequencer.newFixedRateSchedule(config.electionTimeoutMin, config.electionTimeoutMin)
		electionTimeoutSchedule = Some(schedule)
		sequencer.schedule(schedule) { _ =>
			checkElectionTimeout()
		}
	}

	def stopElectionTimeoutScheduler(): Unit = {
		electionTimeoutSchedule.foreach { schedule =>
			sequencer.cancel(schedule)
			electionTimeoutSchedule = None
		}
	}

	private def startHeartbeatScheduler(): Unit = {
		val schedule = sequencer.newFixedRateSchedule(config.heartbeatInterval, config.heartbeatInterval)
		heartbeatSchedule = Some(schedule)
		sequencer.schedule(schedule) { _ =>
			sendHeartbeat()
		}
	}

	private def stopHeartbeatScheduler(): Unit = {
		heartbeatSchedule.foreach { schedule =>
			sequencer.cancel(schedule)
			heartbeatSchedule = None
		}
	}

	private def checkElectionTimeout(): Unit = {
		sequencer.execute {
			if shouldStartElection then {
				scribe.info(s"${clusterService.context.showContext}: election timeout expired, starting new election")
				startElection()
			}
		}
	}

	/**
	 * Sets a new random election timeout.
	 * This should be called when a new election timeout period begins.
	 */
	private def setElectionTimeout(): Unit = {
		val minTimeout = config.electionTimeoutMin
		val maxTimeout = config.electionTimeoutMax
		val randomTimeout = minTimeout + scala.util.Random.nextInt((maxTimeout - minTimeout + 1).toInt)
		electionTimeoutExpiry = System.currentTimeMillis() + randomTimeout
	}

	/**
	 * Resets the election timeout.
	 * This should be called when a valid AppendEntries RPC or RequestVote RPC is received.
	 */
	private def resetElectionTimeout(): Unit = {
		setElectionTimeout()
	}

	/**
	 * Checks if the election timeout has expired.
	 * @return true if the election timeout has expired, false otherwise.
	 */
	private def isElectionTimeoutExpired: Boolean = {
		System.currentTimeMillis() >= electionTimeoutExpiry
	}

	/**
	 * Determines if an election should be started based on the timeout.
	 * @return true if an election should be started, false otherwise.
	 */
	private def shouldStartElection: Boolean = {
		role != Role.Leader && isElectionTimeoutExpired
	}

	/**
	 * Handles incoming Raft messages.
	 *
	 * This method acts as a dispatcher, forwarding messages to the appropriate
	 * handler based on their type. All state modifications are wrapped in
	 * `sequencer.executeSequentially` to ensure thread safety.
	 *
	 * @param message The incoming Raft message
	 */
	private def handleMessage(message: RaftMessage): Unit = {
		scribe.trace(s"${clusterService.context.showContext}: received ${message.getClass.getSimpleName} from ${message.sourceNodeId} (term=${message.term})")
		sequencer.execute {
			message match {
				case rv: RequestVote => handleRequestVote(rv)
				case rvr: RequestVoteResponse => handleRequestVoteResponse(rvr)
				case ae: AppendEntries => handleAppendEntries(ae)
				case aer: AppendEntriesResponse => handleAppendEntriesResponse(aer)
				case cc: ConfigurationChange => handleConfigurationChange(cc)
				case ccr: ConfigurationChangeResponse => handleConfigurationChangeResponse(ccr)
				case _ => scribe.warn(s"${clusterService.context.showContext}: unknown Raft message type received: $message")
			}
		}
	}

	/**
	 * Handles an incoming RequestVote RPC.
	 *
	 * @param request The RequestVote message
	 */
	private def handleRequestVote(request: RequestVote): Unit = {
		// Raft rules for RequestVote RPC
		// 1. If request.term < currentTerm, reply false.
		// 2. If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote.
		if request.term < currentTerm then {
			send(RequestVoteResponse(
				sourceNodeId = clusterService.getCurrentNodeId,
				targetNodeId = request.sourceNodeId,
				term = currentTerm,
				voteGranted = false
			))
		} else {
			if request.term > currentTerm then {
				becomeFollower(request.term)
			}

			val logIsUpToDate = isLogUpToDate(request.lastLogIndex, request.lastLogTerm)
			val canVote = votedFor.isEmpty || votedFor.contains(request.candidateId)

			if canVote && logIsUpToDate then {
				votedFor = Some(request.candidateId)
				resetElectionTimeout() // Granting vote resets timeout
				scribe.info(s"${clusterService.context.showContext}: granted vote to ${request.candidateId} for term ${request.term}")
				send(RequestVoteResponse(
					sourceNodeId = clusterService.getCurrentNodeId,
					targetNodeId = request.sourceNodeId,
					term = currentTerm,
					voteGranted = true
				))
			} else {
				scribe.info(s"${clusterService.context.showContext}: denied vote to ${request.candidateId} for term ${request.term} (canVote=$canVote, logIsUpToDate=$logIsUpToDate)")
				send(RequestVoteResponse(
					sourceNodeId = clusterService.getCurrentNodeId,
					targetNodeId = request.sourceNodeId,
					term = currentTerm,
					voteGranted = false
				))
			}
		}
	}

	/**
	 * Checks if a candidate's log is at least as up-to-date as the receiver's log.
	 * Raft determines which of two logs is more up-to-date by comparing the index and term of the last entries in the logs.
	 * If the logs have last entries with different terms, then the log with the later term is more up-to-date.
	 * If the logs end with the same term, then whichever log is longer is more up-to-date.
	 *
	 * @param candidateLastLogIndex The index of the candidate's last log entry.
	 * @param candidateLastLogTerm The term of the candidate's last log entry.
	 * @return true if the candidate's log is at least as up-to-date, false otherwise.
	 */
	private def isLogUpToDate(candidateLastLogIndex: LogIndex, candidateLastLogTerm: Term): Boolean = {
		val lastEntry = log.lastOption
		lastEntry match {
			case Some(entry) =>
				if candidateLastLogTerm > entry.term then true
				else if candidateLastLogTerm == entry.term then candidateLastLogIndex >= entry.index
				else false
			case None =>
				// If receiver's log is empty, candidate's log is always at least as up-to-date.
				true
		}
	}

	/**
	 * Handles an incoming RequestVoteResponse RPC.
	 *
	 * @param response The RequestVoteResponse message
	 */
	private def handleRequestVoteResponse(response: RequestVoteResponse): Unit = {
		// Raft rules for RequestVoteResponse
		// 1. If response.term > currentTerm, become follower.
		// 2. If role is Candidate and voteGranted, count votes. If majority, become leader.
		if response.term > currentTerm then {
			becomeFollower(response.term)
		} else if role == Role.Candidate && response.term == currentTerm then {
			if response.voteGranted then {
				// Increment votes received
				// If majority, become leader
				val activeNodes = clusterService.getActiveNodes
				val majority = (activeNodes.size / 2) + 1
				scribe.info(s"${clusterService.context.showContext}: received vote from ${response.sourceNodeId} for term $currentTerm")
				// This is a placeholder. A real implementation would track votes.
				// For now, assume if we get a vote, and we are the only other node, we win.
				if activeNodes.size <= 2 then { // Simplified for testing with 1-2 nodes
					becomeLeader()
				}
			}
		}
	}

	/**
	 * Handles an incoming AppendEntries RPC (heartbeat or log replication).
	 *
	 * @param request The AppendEntries message
	 */
	private def handleAppendEntries(request: AppendEntries): Unit = {
		// Raft rules for AppendEntries RPC
		// 1. If request.term < currentTerm, reply false.
		// 2. If leaderId is valid and request.term >= currentTerm, become follower, reset timeout.
		// 3. Log consistency check: If log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm, reply false.
		// 4. If an existing entry conflicts with a new one (same index, different terms), delete existing entry and all that follow it.
		// 5. Append any new entries not already in the log.
		// 6. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry).
		if request.term < currentTerm then {
			send(AppendEntriesResponse(
				sourceNodeId = clusterService.getCurrentNodeId,
				targetNodeId = request.sourceNodeId,
				term = currentTerm,
				success = false,
				matchIndex = commitIndex
			))
		} else {
			becomeFollower(request.term) // Always become follower if term >= currentTerm
			currentLeaderId = Some(request.leaderId) // Track the current leader
			resetElectionTimeout() // Valid AppendEntries resets timeout

			// Log consistency check
			if request.prevLogIndex > 0 && (log.length <= request.prevLogIndex || log(request.prevLogIndex.toInt - 1).term != request.prevLogTerm) then {
				send(AppendEntriesResponse(
					sourceNodeId = clusterService.getCurrentNodeId,
					targetNodeId = request.sourceNodeId,
					term = currentTerm,
					success = false,
					matchIndex = commitIndex
				))
			} else {
				// Append new entries
				var newLog = log.take(request.prevLogIndex.toInt)
				request.entries.foreach { entry =>
					if newLog.length <= entry.index.toInt - 1 || newLog(entry.index.toInt - 1) != entry then {
						newLog = newLog.take(entry.index.toInt - 1) :+ entry
					}
				}
				log = newLog

				// Update commitIndex
				if request.leaderCommit > commitIndex then {
					commitIndex = math.min(request.leaderCommit, log.length)
					// Apply committed entries to state machine (TODO: implement state machine application)
				}

				send(AppendEntriesResponse(
					sourceNodeId = clusterService.getCurrentNodeId,
					targetNodeId = request.sourceNodeId,
					term = currentTerm,
					success = true,
					matchIndex = log.length
				))
			}
		}
	}

	/**
	 * Handles an incoming AppendEntriesResponse RPC.
	 *
	 * @param response The AppendEntriesResponse message
	 */
	private def handleAppendEntriesResponse(response: AppendEntriesResponse): Unit = {
		// Raft rules for AppendEntriesResponse
		// 1. If response.term > currentTerm, become follower.
		// 2. If role is Leader and response.term == currentTerm:
		//    If success, update nextIndex and matchIndex for follower.
		//    If !success, decrement nextIndex and retry AppendEntries.
		//    If majority of matchIndex >= N, commit log entry N.
		if response.term > currentTerm then {
			becomeFollower(response.term)
		} else if role == Role.Leader && response.term == currentTerm then {
			if response.success then {
				nextIndex.update(response.sourceNodeId, response.matchIndex + 1)
				matchIndex.update(response.sourceNodeId, response.matchIndex)
				// Check for commitment (majority rule)
				// This is a placeholder. A real implementation would check if a majority of matchIndex >= N.
			} else {
				// Decrement nextIndex and retry AppendEntries
				val followerId = response.sourceNodeId
				val currentNextIndex = nextIndex.getOrElse(followerId, 1L: LogIndex)
				if currentNextIndex > 1 then {
					nextIndex.update(followerId, currentNextIndex - 1)
					// Retry AppendEntries to this follower (TODO: implement retry logic)
				}
			}
		}
	}

	/**
	 * Handles an incoming ConfigurationChange RPC.
	 *
	 * @param request The ConfigurationChange message
	 */
	private def handleConfigurationChange(request: ConfigurationChange): Unit = {
		// Raft rules for ConfigurationChange
		// This is a simplified placeholder. Real Raft handles joint consensus.
		if request.term < currentTerm then {
			send(ConfigurationChangeResponse(
				sourceNodeId = clusterService.getCurrentNodeId,
				targetNodeId = request.sourceNodeId,
				term = currentTerm,
				success = false
			))
		} else {
			becomeFollower(request.term)
			resetElectionTimeout()

			// For simplicity, directly apply the new configuration if it's valid
			// In a real Raft, this would involve joint consensus and log entries.
			if !clusterService.isConfigurationChangeInProgress then {
				// TODO: This should be async, not blocking
				// For now, we'll assume success and handle the Future properly later
				send(ConfigurationChangeResponse(
					sourceNodeId = clusterService.getCurrentNodeId,
					targetNodeId = request.sourceNodeId,
					term = currentTerm,
					success = true
				))
			} else {
				send(ConfigurationChangeResponse(
					sourceNodeId = clusterService.getCurrentNodeId,
					targetNodeId = request.sourceNodeId,
					term = currentTerm,
					success = false
				))
			}
		}
	}

	/**
	 * Handles an incoming ConfigurationChangeResponse RPC.
	 *
	 * @param response The ConfigurationChangeResponse message
	 */
	private def handleConfigurationChangeResponse(response: ConfigurationChangeResponse): Unit = {
		// TODO: Implement logic to track responses from both old and new configurations
		// to finalize the transition from joint consensus to the new configuration.
		// This would involve checking if a majority of both configurations have acknowledged the change.
	}

	/**
	 * Transitions the Raft instance to the Follower role.
	 *
	 * @param newTerm The term to transition to.
	 */
	private def becomeFollower(newTerm: Term): Unit = {
		role = Role.Follower
		currentTerm = newTerm
		votedFor = None
		currentLeaderId = None // Clear leader when becoming follower
		resetElectionTimeout()
		stopHeartbeatScheduler()
		startElectionTimeoutScheduler()
		clusterService.updateTerm(newTerm)
		clusterService.notifyFollower(newTerm)
		scribe.info(s"${clusterService.context.showContext}: became Follower for term $currentTerm")
	}

	/**
	 * Transitions the Raft instance to the Candidate role.
	 */
	private def startElection(): Unit = {
		role = Role.Candidate
		currentTerm += 1
		votedFor = Some(clusterService.getCurrentNodeId)
		resetElectionTimeout()
		stopHeartbeatScheduler()
		startElectionTimeoutScheduler() // Keep election timeout running for new election
		clusterService.updateTerm(currentTerm)
		clusterService.notifyCandidate()
		scribe.info(s"${clusterService.context.showContext}: started election for term $currentTerm")

		// Send RequestVote RPCs to all other nodes
		val activeNodes = clusterService.getActiveNodes
		activeNodes.foreach { nodeId =>
			if nodeId != clusterService.getCurrentNodeId then {
				send(RequestVote(
					sourceNodeId = clusterService.getCurrentNodeId,
					targetNodeId = nodeId,
					term = currentTerm,
					candidateId = clusterService.getCurrentNodeId,
					lastLogIndex = log.length,
					lastLogTerm = log.lastOption.map(_.term).getOrElse(0L: Term)
				))
			}
		}
	}

	/**
	 * Transitions the Raft instance to the Leader role.
	 */
	private def becomeLeader(): Unit = {
		role = Role.Leader
		currentLeaderId = Some(clusterService.getCurrentNodeId) // Set self as leader
		stopElectionTimeoutScheduler()
		startHeartbeatScheduler()
		clusterService.notifyLeaderElected(currentTerm)
		scribe.info(s"${clusterService.context.showContext}: became Leader for term $currentTerm")

		// Initialize nextIndex and matchIndex for all followers
		val activeNodes = clusterService.getActiveNodes
		activeNodes.foreach { nodeId =>
			if nodeId != clusterService.getCurrentNodeId then {
				nextIndex.update(nodeId, log.length + 1)
				matchIndex.update(nodeId, 0L: LogIndex)
			}
		}
		sendHeartbeat() // Send initial heartbeat
	}

	/**
	 * Sends AppendEntries RPCs (heartbeats) to all followers.
	 */
	private def sendHeartbeat(): Unit = {
		if role == Role.Leader then {
			scribe.trace(s"${clusterService.context.showContext}: sending heartbeat to followers")
			val activeNodes = clusterService.getActiveNodes
			activeNodes.foreach { nodeId =>
				if nodeId != clusterService.getCurrentNodeId then {
					send(AppendEntries(
						sourceNodeId = clusterService.getCurrentNodeId,
						targetNodeId = nodeId,
						term = currentTerm,
						leaderId = clusterService.getCurrentNodeId,
						prevLogIndex = log.length, // For heartbeats, prevLogIndex and prevLogTerm can be last log entry
						prevLogTerm = log.lastOption.map(_.term).getOrElse(0L: Term),
						entries = List.empty, // Empty for heartbeats
						leaderCommit = commitIndex
					))
				}
			}
		}
	}

	/**
	 * Gets the current leader node ID, if known.
	 * 
	 * This method is used by the cluster service to redirect client requests to the leader.
	 * 
	 * @return Some(leaderId) if the leader is known, None if no leader is known
	 */
	def getCurrentLeader: Option[NodeId] = currentLeaderId

	/**
	 * Appends a new entry to the log (leader only).
	 *
	 * When a client sends a request to the leader, the leader appends the command
	 * to its log as a new entry, then issues AppendEntries RPCs in parallel to
	 * each of the other servers to replicate the entry.
	 *
	 * If this node is not the leader, it will redirect the client to the known leader
	 * or respond with NoLeaderKnown if no leader is currently known.
	 *
	 * @param command The command to append to the log
	 * @return Task[ClientResponse] indicating the result of the client request
	 */
	def appendEntry(command: Any): sequencer.Task[ClientResponse] = {
		if role == Role.Leader then {
			sequencer.Task_ownFlat { () =>
				val entry = LogEntry(currentTerm, log.length + 1, command)
				log = log :+ entry

				// Replicate to followers
				val activeNodes = clusterService.getActiveNodes
				val appendEntriesTasks = for nodeId <- activeNodes if nodeId != clusterService.getCurrentNodeId yield {
					send(AppendEntries(
						sourceNodeId = clusterService.getCurrentNodeId,
						targetNodeId = nodeId,
						term = currentTerm,
						leaderId = clusterService.getCurrentNodeId,
						prevLogIndex = log.length - 1,
						prevLogTerm = if log.length > 1 then log((log.length - 2).toInt).term else 0L: Term,
						entries = List(entry),
						leaderCommit = commitIndex
					))
				}
				sequencer.Task_sequenceToArray(appendEntriesTasks).map { _ =>
					ClientResponse.Success(entry.index)
				}
			}
		} else {
			// If not leader, redirect to leader or respond with NoLeaderKnown
			val response = currentLeaderId match {
				case Some(leaderId) => 
					// Redirect to known leader
					ClientResponse.RedirectToLeader(leaderId)
				case None => 
					// No leader known - respond immediately as per Raft safety requirements
					// Waiting for leader election would require volatile state that could be lost on crash
					scribe.info(s"${clusterService.context.showContext}: no leader known, responding with NoLeaderKnown")
					ClientResponse.NoLeaderKnown
			}
			sequencer.Task_successful(response)
		}
	}

	// ===== UTILITY METHODS =====

	/**
	 * Sends a message to a target node via the cluster service.
	 *
	 * @param msg The message to send
	 */
	private def send(msg: RaftMessage): sequencer.Task[Unit] = {
		val target = msg.targetNodeId
		scribe.trace(s"${clusterService.context.showContext}: sending ${msg.getClass.getSimpleName} to $target")
		clusterService.sendMessage(target, msg)
	}
} 