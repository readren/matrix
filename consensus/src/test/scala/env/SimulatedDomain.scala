package readren.consensus.env

import readren.consensus.ConsensusParticipantSdm.*
import readren.consensus.protocol.*

import scala.collection.immutable.ListSet
import scala.reflect.ClassTag
import scala.util.Try

/** Unique identifier of a simulated consensus node/participant, implementing [[ConsensusParticipantSdm.ParticipantId]]. */
opaque type NodeId <: String = String

object NodeId {
	/** Wraps a string as a [[NodeId]]. */
	inline def apply(s: String): NodeId = s

	/** Provides the runtime [[ClassTag]] for [[NodeId]] arrays. */
	given ClassTag[NodeId] = summon[ClassTag[String]]
}

/** Ergonomic reference to a node/participant, supporting [[NodeId]], [[String]] ("p-0", "0"), or [[Int]] (0 -> "p-0"). */
type NodeRef = NodeId | String | Int

extension (ref: NodeRef) {
	/** Normalizes this [[NodeRef]] into its canonical [[NodeId]] representation. */
	def asNodeId: NodeId = ref match {
		case s: String => NodeId(if s.forall(_.isDigit) then s"p-$s" else s)
		case i: Int => NodeId(s"p-$i")
	}
}

/** Ergonomic reference to a client, supporting either [[String]] ("c-0") or [[Int]] (0 -> "c-0"). */
type ClientRef = String | Int

extension (ref: ClientRef) {
	/** Normalizes this [[ClientRef]] into its canonical client identifier string. */
	def asClientId: String = ref match {
		case s: String => if s.forall(_.isDigit) then s"c-$s" else s
		case i: Int => s"c-$i"
	}
}

/** Unique identifier for packets in [[SimulatedNetwork]]. */
type PacketId = Int

/** Unique identifier for pending persistence operations in [[SimulatedStorageSubsystem]]. */
type StorageOpId = Int

/** Unique identifier for pending wake-up tokens in [[VirtualClock]]. */
type WakeUpTokenId = Int

/** Virtual time measured in discrete integer ticks within [[VirtualClock]]. */
type VirtualTime = Int

/** Sealed domain representation of consensus RPC requests and their expected response types. */
sealed trait ConsensusRpc {
	/** The expected response payload type for this RPC request. */
	type Response
}

object ConsensusRpc {
	/** Phase 1 discovery inquiry soliciting peer state information.
	 * @param inquirerInfo the state snapshot of the inquiring participant. */
	final case class HowAreYou(inquirerInfo: StateInfo) extends ConsensusRpc {
		type Response = StateInfo
	}

	/** Phase 2 election inquiry soliciting an electoral vote for `inquirerId`.
	 * @param inquirerId the candidate participant requesting the vote.
	 * @param inquirerInfo the state snapshot of the inquiring candidate. */
	final case class RequestBallot(inquirerId: NodeId, inquirerInfo: StateInfo) extends ConsensusRpc {
		type Response = Ballot
	}

	/** Log replication RPC carrying a batch of log records and leader commit watermarks.
	 * @param inquirerTerm the term of the sending leader.
	 * @param prevLogIndex the index of the log entry immediately preceding `batch`.
	 * @param prevLogTerm the term of the log entry at `prevLogIndex`.
	 * @param batch the contiguous sequence of records to append.
	 * @param leaderCommit the leader's current commit index.
	 * @param termAtLeaderCommit the term of the record at `leaderCommit`. */
	final case class AppendRecords(
		inquirerTerm: Term,
		prevLogIndex: RecordIndex,
		prevLogTerm: Term,
		batch: IArray[Record],
		leaderCommit: RecordIndex,
		termAtLeaderCommit: Term
	) extends ConsensusRpc {
		type Response = AppendResult
	}

	/** Compaction recovery RPC carrying a state machine snapshot, tail records, and leader commit watermarks.
	 * @param inquirerTerm the term of the sending leader.
	 * @param snapshot the consolidated snapshot data to install.
	 * @param batch any trailing log records immediately following the snapshot boundary.
	 * @param leaderCommit the leader's current commit index.
	 * @param termAtLeaderCommit the term of the record at `leaderCommit`. */
	final case class InstallSnapshot(
		inquirerTerm: Term,
		snapshot: SnapshotData[NodeId],
		batch: IArray[Record],
		leaderCommit: RecordIndex,
		termAtLeaderCommit: Term
	) extends ConsensusRpc {
		type Response = AppendResult
	}

	/** Leader authorization RPC permitting a retiring participant to transition to [[QUIESCED]].
	 * @param indexOfGrantedSec the log index of the [[SoleElectorateChange]] under which quiescence is granted. */
	final case class PermitQuiescence(indexOfGrantedSec: RecordIndex) extends ConsensusRpc {
		type Response = Unit
	}
}

/** A network packet traveling between two nodes across a point-to-point channel in [[SimulatedNetwork]]. */
sealed trait Packet {
	/** Unique monotonic identifier assigned to this packet upon enqueueing. */
	def id: PacketId

	/** The identifier of the sending node. */
	def source: NodeId

	/** The identifier of the receiving node. */
	def destination: NodeId

	/** The [[VirtualTime]] at which this packet was enqueued into the network. */
	def departureTime: VirtualTime

	/** The underlying [[ConsensusRpc]] associated with this packet. */
	def rpc: ConsensusRpc
}

/** A packet representing an outbound RPC request traveling from `source` to `destination`.
 * @param id unique identifier of this request packet.
 * @param source the originating node identifier.
 * @param destination the target node identifier.
 * @param departureTime the virtual time when the request was sent.
 * @param rpc the consensus RPC payload.
 * @param completeCaller callback invoked on the sender's [[StepDoer]] when the corresponding response is delivered or when the packet fails in transit. */
final case class RequestPacket(
	id: PacketId,
	source: NodeId,
	destination: NodeId,
	departureTime: VirtualTime,
	rpc: ConsensusRpc,
	completeCaller: Try[Any] => Unit
) extends Packet

/** A packet representing the outbound response to an earlier [[RequestPacket]].
 * @param id unique identifier of this response packet.
 * @param source the responding node identifier (the destination of the original request).
 * @param destination the original requester node identifier.
 * @param departureTime the virtual time when the response was enqueued.
 * @param correlationRequestId the [[PacketId]] of the originating [[RequestPacket]].
 * @param rpc the original [[ConsensusRpc]] request to which this response corresponds.
 * @param response the success value or failure exception produced by the responder.
 * @param completeCaller callback completing the original caller's [[readren.sequencer.Doer.Capture]]. */
final case class ResponsePacket(
	id: PacketId,
	source: NodeId,
	destination: NodeId,
	departureTime: VirtualTime,
	correlationRequestId: PacketId,
	rpc: ConsensusRpc,
	response: Try[Any],
	completeCaller: Try[Any] => Unit
) extends Packet

/** Diagnostic outcome returned when a [[Packet]] is delivered by [[SimulatedNetwork]].
 * @param packetId the identifier of the delivered packet.
 * @param source the sender node identifier.
 * @param destination the recipient node identifier.
 * @param destinationHadPendingRunnables true if the destination node's [[StepDoer]] already had unexecuted tasks in its queue at delivery time.
 * @param warning optional diagnostic message emitted when `destinationHadPendingRunnables` is true. */
final case class DeliverOutcome(
	packetId: PacketId,
	source: NodeId,
	destination: NodeId,
	destinationHadPendingRunnables: Boolean,
	warning: Option[String]
)

/** Listener interface for observing RPC packet lifecycle events within [[SimulatedNetwork]]. */
trait RpcLifecycleListener {
	/** Invoked when a [[RequestPacket]] is enqueued into a network channel.
	 * @param req the enqueued request packet.
	 * @param travelingCount the number of packets already in transit prior to enqueueing `req`. */
	def onRpcEnqueued(req: RequestPacket, travelingCount: Int): Unit

	/** Invoked immediately before a [[RequestPacket]] is dispatched to the destination node's delegate.
	 * @param req the request packet being delivered.
	 * @param travelingCount the current number of packets in transit across all channels. */
	def onRpcDelivering(req: RequestPacket, travelingCount: Int): Unit

	/** Invoked when the destination node finishes processing a [[RequestPacket]] and produces an outcome.
	 * @param req the processed request packet.
	 * @param outcome the result produced by the destination node before response transmission.
	 * @param travelingCount the current number of packets in transit across all channels. */
	def onRpcCompleted(req: RequestPacket, outcome: Try[Any], travelingCount: Int): Unit

	/** Invoked when a [[ResponsePacket]] (or synthetic drop/failure response) is delivered to the original requester.
	 * @param resp the delivered response packet.
	 * @param travelingCount the remaining number of packets in transit across all channels. */
	def onResponseDelivered(resp: ResponsePacket, travelingCount: Int): Unit
}

/** Client-visible lifecycle status of a command submitted via [[ConsensusEnvironment.submitClientCommand]]. */
sealed trait ClientCommandStatus

object ClientCommandStatus {
	/** The command has been submitted to the target node and is awaiting completion. */
	case object InFlight extends ClientCommandStatus

	/** The command was committed and applied to the state machine at `recordIndex`, yielding `response`. */
	final case class Processed(recordIndex: RecordIndex, response: Int) extends ClientCommandStatus

	/** The target node is a follower and redirected the client to `otherParticipants`, ordered from most to least probable next target (with its known leader at the head). */
	final case class Redirected(otherParticipants: ListSet[NodeId]) extends ClientCommandStatus

	/** The target node was unable to process the command and returned `nextAttemptFlag` along with `otherParticipants` ordered from most to least probable next target. */
	final case class Unable(nextAttemptFlag: CommandAttemptFlag, otherParticipants: ListSet[NodeId]) extends ClientCommandStatus

	/** The command submission failed due to node unavailability or an unhandled exception. */
	final case class Failed(cause: Throwable) extends ClientCommandStatus
}

/** Aggregated client-visible telemetry and commit watermarks for a simulated client.
 * @param lastSent the serial number of the most recently submitted command for this client.
 * @param lastSuccess the serial number of the most recently processed command for this client.
 * @param lastRecordIndex the log index at which `lastSuccess` was committed and applied.
 * @param lastStatus the most recent [[ClientCommandStatus]] observed by this client. */
final case class ClientStats(
	lastSent: Option[Int],
	lastSuccess: Option[Int],
	lastRecordIndex: Option[Long],
	lastStatus: Option[ClientCommandStatus] = None
)

/** Control-plane lifecycle status of an electorate change submitted via [[ConsensusEnvironment.submitElectorateChange]]. */
sealed trait ElectorateChangeStatus

object ElectorateChangeStatus {
	/** The reconfiguration request is currently in flight on the target node. */
	case object InFlight extends ElectorateChangeStatus

	/** The target node completed the request with the protocol `response` (which may be terminal or non-terminal). */
	final case class Completed(response: ElectorateChangeResponse[NodeId]) extends ElectorateChangeStatus

	/** The reconfiguration request failed due to target node unavailability or an unhandled exception. */
	final case class Failed(cause: Throwable) extends ElectorateChangeStatus
}

/** External handle tracking an in-flight or completed client command submission.
 * @param commandId unique environment-assigned identifier for this submission attempt.
 * @param clientId the identifier of the submitting client.
 * @param serial the client-assigned command serial number.
 * @param targetNodeId the node to which this submission was directed.
 * @param submissionTime the [[VirtualTime]] at which the command was submitted. */
final case class ClientCommandHandle(
	commandId: Int,
	clientId: String,
	serial: Int,
	targetNodeId: NodeId,
	submissionTime: VirtualTime
)

/** External handle tracking an in-flight or completed electorate change request.
 * @param requestId unique protocol request identifier (`ccReq-N`).
 * @param targetNodeId the node to which this reconfiguration request was directed.
 * @param desiredParticipants the target voting electorate requested by the control plane.
 * @param submissionTime the [[VirtualTime]] at which the request was submitted. */
final case class ElectorateChangeHandle(
	requestId: String,
	targetNodeId: NodeId,
	desiredParticipants: Set[NodeId],
	submissionTime: VirtualTime
)
