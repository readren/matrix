package readren.matrix
package consensus

import readren.taskflow.{Doer, SchedulingExtension}

import scala.concurrent.Future

/** Global type alias for node identifiers */
type NodeId = String

/** Global type alias for Raft term numbers (monotonically increasing) */
type Term = Long

/** Global type alias for log entry indices (1-based indexing) */
type LogIndex = Long

/**
 * Context information required by the Raft consensus module for enriched logging and error messages.
 * 
 * Users of the Raft module must implement this trait to provide contextual information
 * that will be used to enhance log messages and error reporting.
 */
trait RaftContext {
	/** The current node's identifier */
	def currentNodeId: NodeId
	
	/** A human-readable name or description for the current node */
	def nodeName: String
	
	/** The cluster name or identifier this node belongs to */
	def clusterName: String
	
	/** Returns a string representation of the context for logging */
	def showContext: String = s"node=$currentNodeId($nodeName) cluster=$clusterName"
}

/**
 * Service interface that the Raft consensus module requires from the cluster service.
 * 
 * This interface abstracts away networking, node discovery, and task sequencing details from the Raft consensus algorithm, allowing it to focus purely on consensus logic.
 * 
 * The cluster service implementing this interface is responsible for:
 * - Node discovery and membership management
 * - Network communication between nodes
 * - Task sequencing for thread-safe execution
 * - Scheduling of periodic tasks (elections, heartbeats)
 */
trait RaftClusterService {
	/** Context information for enriched logging and error messages */
	val context: RaftContext
	/**
	 * Gets the set of currently active nodes in the cluster.
	 * 
	 * This method should return the current view of cluster membership as known by the cluster service. The Raft module uses this for leader election and log replication decisions.
	 * 
	 * @return Set of active node IDs
	 */
	def getActiveNodes: Set[NodeId]
	
	/**
	 * Gets the ID of the current node.
	 * 
	 * @return The current node's ID
	 */
	def getCurrentNodeId: NodeId
	
	/**
	 * Checks if a specific node is reachable.
	 * 
	 * This method should perform a quick health check to determine if the target node is currently reachable for communication.
	 * 
	 * @param nodeId The node ID to check
	 * @return true if the node is reachable, false otherwise
	 */
	def isNodeReachable(nodeId: NodeId): Boolean
	
	/**
	 * Sends a Raft protocol message to a target node.
	 * 
	 * This method should handle the actual network transmission of Raft messages. The returned [[TaskSequencer.Task]], if triggered, completes when the message is sent (not necessarily delivered or acknowledged).
	 * 
	 * Network failures are handled through sequencer.Task failures.
	 * 
	 * @param targetNodeId The target node's ID
	 * @param message The Raft message to send
	 * @return sequencer.Task[Unit] that completes when the message is sent
	 */
	def sendMessage(targetNodeId: NodeId, message: RaftMessage): sequencer.Task[Unit]
	
	/**
	 * Registers a message handler for incoming Raft messages.
	 * 
	 * The cluster service should call this handler whenever a Raft
	 * message is received from any other node in the cluster.
	 * 
	 * @param messageHandler The handler function for incoming messages
	 */
	def registerMessageHandler(messageHandler: RaftMessage => Unit): Unit
	
	/**
	 * Gets the current term number.
	 * 
	 * This method should return the current term as maintained by
	 * the cluster service. The Raft module uses this for term
	 * synchronization across the cluster.
	 * 
	 * @return The current term number
	 */
	def getCurrentTerm: Term
	
	/**
	 * Updates the current term number.
	 * 
	 * This method is called by the Raft module when it discovers
	 * a higher term from another node or starts a new election.
	 * 
	 * @param newTerm The new term number
	 */
	def updateTerm(newTerm: Term): Unit
	
	/**
	 * Notifies the cluster service that this node has become the leader.
	 * 
	 * This method allows the cluster service to update its internal state
	 * and potentially trigger leader-specific behaviors.
	 * 
	 * @param term The term in which this node became leader
	 */
	def notifyLeaderElected(term: Term): Unit
	
	/**
	 * Notify the cluster service that this node has stepped down from leadership. This allows the cluster service to update its internal state.
	 * 
	 * @param term The term in which this node stepped down
	 */
	def notifyFollower(term: Term): Unit

	/**
	 * Notifies the cluster service that this node has become a candidate.
	 *
	 * This method is called by the Raft module when it transitions
	 * to the candidate role.
	 */
	def notifyCandidate(): Unit

	/**
	 * Gets the current leader node ID, if known.
	 * 
	 * This method should return the ID of the current leader as known by this node.
	 * The Raft module uses this to redirect client requests to the leader.
	 * 
	 * @return Some(leaderId) if the leader is known, None if no leader is known
	 */
	def getCurrentLeader: Option[NodeId] = None // Default implementation for backward compatibility

	/**
	 * Request the cluster service to initiate a configuration change.
	 * 
	 * This method is called by the Raft module when it needs to change the cluster
	 * configuration (e.g., when nodes join or leave). The cluster service should
	 * handle the actual configuration change logic and notify the Raft module
	 * through the existing interface methods.
	 * 
	 * The Future completes when the configuration change request is processed
	 * (not necessarily when the change is fully applied).
	 * 
	 * @param newConfig The new cluster configuration to apply
	 * @return a sequencer.Task indicating whether the configuration change was initiated successfully
	 */
	def requestConfigurationChange(newConfig: Set[NodeId]): sequencer.Task[Boolean]

	/**
	 * Check if a configuration change is currently in progress.
	 * 
	 * @return true if a configuration change is being processed
	 */
	def isConfigurationChangeInProgress: Boolean

	/**
	 * Gets the task sequencer for thread-safe execution and scheduling.
	 * 
	 * The task sequencer provides:
	 * - `executeSequentially(task: Runnable): Unit` for thread-safe execution
	 * - Scheduling capabilities for periodic tasks (elections, heartbeats)
	 * - Guaranteed single-threaded execution of Raft state modifications
	 * 
	 * @return The task sequencer instance
	 */
	val sequencer: Doer & SchedulingExtension
}

/**
 * Base trait for all Raft messages that can be sent between nodes.
 * Specific message types should extend this trait.
 */
trait RaftMessage:
	def sourceNodeId: NodeId
	def targetNodeId: NodeId
	def term: Term

/**
 * Request for votes during leader election.
 */
case class RequestVote(
	sourceNodeId: NodeId,
	targetNodeId: NodeId,
	term: Term,
	candidateId: NodeId,
	lastLogIndex: LogIndex,
	lastLogTerm: Term
) extends RaftMessage

/**
 * Response to a vote request.
 */
case class RequestVoteResponse(
	sourceNodeId: NodeId,
	targetNodeId: NodeId,
	term: Term,
	voteGranted: Boolean
) extends RaftMessage

/**
 * Request to append entries to the log (heartbeat or actual data).
 */
case class AppendEntries(
	sourceNodeId: NodeId,
	targetNodeId: NodeId,
	term: Term,
	leaderId: NodeId,
	prevLogIndex: LogIndex,
	prevLogTerm: Term,
	entries: List[LogEntry],
	leaderCommit: LogIndex
) extends RaftMessage

/**
 * Response to an append entries request.
 */
case class AppendEntriesResponse(
	sourceNodeId: NodeId,
	targetNodeId: NodeId,
	term: Term,
	success: Boolean,
	matchIndex: LogIndex
) extends RaftMessage

/**
 * A log entry in the Raft log.
 */
case class LogEntry(
	term: Term,
	index: LogIndex,
	command: Any // The actual command/data to be applied
)

/**
 * Configuration change request for dynamic membership.
 */
case class ConfigurationChange(
	sourceNodeId: NodeId,
	targetNodeId: NodeId,
	term: Term,
	oldConfig: Set[NodeId],
	newConfig: Set[NodeId]
) extends RaftMessage

/**
 * Response to a configuration change request.
 */
case class ConfigurationChangeResponse(
	sourceNodeId: NodeId,
	targetNodeId: NodeId,
	term: Term,
	success: Boolean
) extends RaftMessage

/**
 * Response to a client request.
 * 
 * This enum represents the possible outcomes when a client sends a request to a Raft node.
 */
enum ClientResponse:
	/** The request was successfully processed by the leader */
	case Success(logIndex: LogIndex)
	/** The request should be redirected to the specified leader */
	case RedirectToLeader(leaderId: NodeId)
	/** No leader is currently known, client should retry later */
	case NoLeaderKnown 