package readren.consensus.env

import readren.consensus.ConsensusParticipantSdm.*
import readren.consensus.protocol.*

import scala.reflect.ClassTag
import scala.util.Try

opaque type NodeId <: String = String

object NodeId {
	inline def apply(s: String): NodeId = s

	given ClassTag[NodeId] = summon[ClassTag[String]]
}

/** Ergonomic reference to a node/participant, supporting [[NodeId]], [[String]] ("p-0", "0"), or [[Int]] (0 -> "p-0"). */
type NodeRef = NodeId | String | Int

extension (ref: NodeRef) {
	def asNodeId: NodeId = ref match {
		case s: String => NodeId(if s.forall(_.isDigit) then s"p-$s" else s)
		case i: Int => NodeId(s"p-$i")
	}
}

/** Ergonomic reference to a client, supporting either [[String]] ("c-0") or [[Int]] (0 -> "c-0"). */
type ClientRef = String | Int

extension (ref: ClientRef) {
	def asClientId: String = ref match {
		case s: String => if s.forall(_.isDigit) then s"c-$s" else s
		case i: Int => s"c-$i"
	}
}

/** Unique identifier for packets in the environment network. */
type PacketId = Int

/** Unique identifier for pending persistence operations. */
type StorageOpId = Int

/** Unique identifier for pending wake-up tokens. */
type WakeUpTokenId = Int

/** Virtual time in integer ticks. */
type VirtualTime = Int

/** Sealed domain representation of consensus RPC requests and their expected response types. */
sealed trait ConsensusRpc {
	type Response
}

object ConsensusRpc {
	final case class HowAreYou(inquirerInfo: StateInfo) extends ConsensusRpc {
		type Response = StateInfo
	}

	final case class RequestBallot(inquirerId: NodeId, inquirerInfo: StateInfo) extends ConsensusRpc {
		type Response = Ballot
	}

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

	final case class InstallSnapshot(
		inquirerTerm: Term,
		snapshot: SnapshotData[NodeId],
		batch: IArray[Record],
		leaderCommit: RecordIndex,
		termAtLeaderCommit: Term
	) extends ConsensusRpc {
		type Response = AppendResult
	}

	final case class PermitQuiescence(indexOfGrantedSec: RecordIndex) extends ConsensusRpc {
		type Response = Unit
	}
}

/** A packet traveling between two nodes across the network. */
sealed trait Packet {
	def id: PacketId

	def source: NodeId

	def destination: NodeId

	def departureTime: VirtualTime

	def rpc: ConsensusRpc
}

/** A packet representing an outbound RPC request from source to destination. */
final case class RequestPacket(
	id: PacketId,
	source: NodeId,
	destination: NodeId,
	departureTime: VirtualTime,
	rpc: ConsensusRpc,
	completeCaller: Try[Any] => Unit
) extends Packet

/** A packet representing the outbound response to an earlier RPC request. */
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

/** The outcome of a packet deliver operation. */
final case class DeliverOutcome(
	packetId: PacketId,
	source: NodeId,
	destination: NodeId,
	destinationHadPendingRunnables: Boolean,
	warning: Option[String]
)

/** Listener interface for observing RPC packet lifecycle events. */
trait RpcLifecycleListener {
	def onRpcEnqueued(req: RequestPacket, travelingCount: Int): Unit

	def onRpcDelivering(req: RequestPacket, travelingCount: Int): Unit

	def onRpcCompleted(req: RequestPacket, outcome: Try[Any], travelingCount: Int): Unit

	def onResponseDelivered(resp: ResponsePacket, travelingCount: Int): Unit
}

/** Status of an injected client command. */
sealed trait ClientCommandStatus

object ClientCommandStatus {
	case object InFlight extends ClientCommandStatus

	final case class Processed(recordIndex: RecordIndex, response: Int) extends ClientCommandStatus

	final case class Redirected(leaderId: NodeId) extends ClientCommandStatus

	final case class Unable(nextAttemptFlag: CommandAttemptFlag, otherParticipants: Set[NodeId]) extends ClientCommandStatus

	final case class Failed(cause: Throwable) extends ClientCommandStatus
}

/** Aggregated metrics and commit watermarks for a client. */
final case class ClientStats(
	lastSent: Option[Int],
	lastSuccess: Option[Int],
	lastRecordIndex: Option[Long],
	lastStatus: Option[ClientCommandStatus] = None
)

/** Status of an injected electorate change request. */
sealed trait ElectorateChangeStatus

object ElectorateChangeStatus {
	case object InFlight extends ElectorateChangeStatus

	final case class Completed(response: ElectorateChangeResponse[NodeId]) extends ElectorateChangeStatus

	final case class Failed(cause: Throwable) extends ElectorateChangeStatus
}

/** A handle to an in-flight or completed client command. */
final case class ClientCommandHandle(
	commandId: Int,
	clientId: String,
	serial: Int,
	targetNodeId: NodeId,
	submissionTime: VirtualTime
)

/** A handle to an in-flight or completed electorate change request. */
final case class ElectorateChangeHandle(
	requestId: String,
	targetNodeId: NodeId,
	desiredParticipants: Set[NodeId],
	submissionTime: VirtualTime
)
