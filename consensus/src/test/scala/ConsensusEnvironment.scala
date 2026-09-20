package readren.consensus

import ConsensusParticipantSdm.*
import readren.common.{Maybe, Trial}
import readren.sequencer.Doer

import scala.collection.immutable.{ListMap, ListSet}
import scala.collection.mutable
import scala.compiletime.uninitialized
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

opaque type NodeId <: String = String

object NodeId {
	inline def apply(s: String): NodeId = s

	given ClassTag[NodeId] = summon[ClassTag[String]]
}

/** Ergonomic reference to a node/participant, supporting NodeId, String ("p-0", "0"), or Int (0 -> "p-0"). */
type NodeRef = NodeId | String | Int

extension (ref: NodeRef) {
	def asNodeId: NodeId = ref match {
		case s: String => NodeId(if s.forall(_.isDigit) then s"p-$s" else s)
		case i: Int => NodeId(s"p-$i")
	}
}

/** Ergonomic reference to a client, supporting either String ("c-0") or Int (0 -> "c-0"). */
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

	final case class ChooseALeader(inquirerId: NodeId, inquirerInfo: StateInfo) extends ConsensusRpc {
		type Response = Vote[NodeId]
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

	final case class PermitQuiescence(indexOfGrantedStableConfigChange: RecordIndex) extends ConsensusRpc {
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

/** A pending wake-up registered via [[ClusterParticipant.requestWakeUp]]. */
final case class PendingWakeUp(
	tokenId: Int,
	token: WakeUpToken,
	nodeId: NodeId,
	reason: WakeUpReason,
	wakeupsDone: Int,
	scheduledTime: VirtualTime,
	callback: () => Unit
)

/** A pending persistence operation waiting for explicit completion or failure. */
final case class PendingPersistence(
	opId: StorageOpId,
	nodeId: NodeId,
	term: Term,
	logBufferOffset: RecordIndex,
	firstEmptyRecordIndex: RecordIndex,
	records: IArray[Record],
	snapshot: Maybe[SnapshotData[NodeId]],
	completeOp: () => Unit,
	failOp: Throwable => Unit
)

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
	lastRecordIndex: Option[Long]
)

/** Status of an injected configuration change request. */
sealed trait ConfigChangeStatus

object ConfigChangeStatus {
	case object InFlight extends ConfigChangeStatus

	final case class Completed(response: ConfigChangeResponse) extends ConfigChangeStatus

	final case class Failed(cause: Throwable) extends ConfigChangeStatus
}

/** A handle to an in-flight or completed client command. */
final case class ClientCommandHandle(
	commandId: Int,
	clientId: String,
	serial: Int,
	targetNodeId: NodeId,
	submissionTime: VirtualTime
)

/** A handle to an in-flight or completed configuration change request. */
final case class ConfigChangeHandle(
	requestId: String,
	targetNodeId: NodeId,
	desiredParticipants: Set[NodeId],
	submissionTime: VirtualTime
)

/** A node instance managed by [[ConsensusEnvironment]]. */
class EnvironmentNode(
	val myId: NodeId,
	val initialParticipants: ListSet[NodeId],
	val env: ConsensusEnvironment
) extends ConsensusParticipantSdm { thisNode =>

	override type ParticipantId = NodeId
	override type ClientCommand = TestClientCommand
	override type StateMachineResponse = Int
	override type ClientId = String
	override type WS = TestWorkspace

	override val maxInFlightAppendsPerPeer: Int = env.maxInFlightAppendsPerPeer
	override val logCompactionThreshold: Int = env.logCompactionThreshold

	override def retiringParticipantMaxRetries: Int = env.retiringParticipantMaxRetries

	override def logRetentionAfterSnapshot: Int = env.logRetentionAfterSnapshot

	val stepDoer: StepDoer = new StepDoer(s"doer-$myId")
	override val sequencer: Doer = stepDoer

	var isDown: Boolean = true
	private var _participant: ConsensusParticipant = null

	inline def participant: ConsensusParticipant = _participant

	def inspectRole: Option[RoleDiagnostic] = {
		if isDown || _participant == null then None
		else Some(_participant.inspectRole)
	}

	var autoSucceedUntilTerm: Term = Int.MaxValue.asInstanceOf[Term]
	var autoSucceedUntilRecordIndex: RecordIndex = Long.MaxValue

	val machine: TestStateMachine = new TestStateMachine()
	val storage: TestStorage = new TestStorage()
	val clusterParticipant: TestClusterParticipant = new TestClusterParticipant()

	def startIfNotRunning(indexOfIncludingConfig: RecordIndex, participants: ListSet[ParticipantId]): Unit = {
		stepDoer.executeSequentially(() => {
			if _participant == null || _participant.getRoleOrdinal == QUIESCED then {
				_participant = new ConsensusParticipant(
					clusterParticipant,
					storage,
					machine,
					indexOfIncludingConfig,
					participants,
					List(notificationListener)
				)
				isDown = false
			}
		})
		stepDoer.drain()
	}

	def release(): Unit = {
		stepDoer.executeSequentially(() => {
			_participant = null
			isDown = true
		})
		stepDoer.drain()
	}

	class TestStateMachine extends StateMachine {
		var highestAppliedCommandSerial: Int = 0
		var highestAppliedCommandIndex: RecordIndex = 0

		override def applyClientCommand(index: RecordIndex, command: ClientCommand): sequencer.Capture[StateMachineResponse] = {
			sequencer.checkWithin()
			if index > highestAppliedCommandIndex then highestAppliedCommandIndex = index
			if command.serial > highestAppliedCommandSerial then highestAppliedCommandSerial = command.serial
			env.onCommandApplied(thisNode, command, index)
			sequencer.Keeper(command.serial)
		}

		override def recoverIndexOfLastAppliedCommand: sequencer.Capture[RecordIndex] = {
			sequencer.checkWithin()
			if env.remembersLastAppliedCommandIndex then sequencer.Keeper(highestAppliedCommandIndex)
			else {
				highestAppliedCommandSerial = 0
				highestAppliedCommandIndex = 0
				sequencer.Keeper(0)
			}
		}

		override def takeSnapshot(): sequencer.Capture[IArray[Byte]] = {
			sequencer.checkWithin()
			val bytes = java.io.ByteArrayOutputStream()
			val out = java.io.ObjectOutputStream(bytes)
			out.writeInt(highestAppliedCommandSerial)
			out.writeLong(highestAppliedCommandIndex)
			out.flush()
			sequencer.Keeper(IArray.unsafeFromArray(bytes.toByteArray))
		}

		override def installSnapshot(data: IArray[Byte]): sequencer.Capture[Unit] = {
			sequencer.checkWithin()
			val in = java.io.ObjectInputStream(java.io.ByteArrayInputStream(data.unsafeArray))
			highestAppliedCommandSerial = in.readInt()
			highestAppliedCommandIndex = in.readLong()
			sequencer.Capture_ready(Doer.successUnit)
		}
	}

	class TestWorkspace extends Workspace {
		var currentTerm: Term = PRE_INIT
		var _votedFor: Maybe[ParticipantId] = Maybe.empty
		val logBuffer: mutable.ArrayBuffer[Record] = mutable.ArrayBuffer.empty
		var _logBufferOffset: RecordIndex = 1
		var maybeLatestSnapshot: Maybe[SnapshotData[ParticipantId]] = Maybe.empty

		override def getCurrentTerm: Term = currentTerm

		override def setCurrentTerm(term: Term): Unit = {
			if term != currentTerm then _votedFor = Maybe.empty
			currentTerm = term
		}

		override def getVotedFor: Maybe[ParticipantId] = _votedFor

		override def setVotedFor(votedFor: Maybe[ParticipantId]): Unit = {
			_votedFor = votedFor
		}

		override def setTermAndVote(term: Term, votedFor: Maybe[ParticipantId]): Unit = {
			currentTerm = term
			_votedFor = votedFor
		}

		override def logBufferOffset: RecordIndex = _logBufferOffset

		override def firstEmptyRecordIndex: RecordIndex = _logBufferOffset + logBuffer.size

		override def latestSnapshot: Maybe[SnapshotData[ParticipantId]] = maybeLatestSnapshot

		override def getRecordAt(index: RecordIndex): Record = {
			logBuffer((index - _logBufferOffset).toInt)
		}

		override def getRecordsBetween(from: RecordIndex, until: RecordIndex): IArray[Record] = {
			val fromIdx = (from - _logBufferOffset).toInt
			val untilIdx = (until - _logBufferOffset).toInt
			val len = untilIdx - fromIdx
			if len <= 0 then IArray.empty
			else {
				val arr = new Array[Record](len)
				Array.copy(logBuffer.toArray, fromIdx, arr, 0, len)
				IArray.unsafeFromArray(arr)
			}
		}

		override def appendRecord(record: Record): Unit = {
			logBuffer.addOne(record)
		}

		override def truncateSuffix(fromIndex: RecordIndex): Unit = {
			val writeIndex = (fromIndex - _logBufferOffset).toInt
			if writeIndex < logBuffer.size then {
				val firstRemoved = logBuffer(writeIndex)
				env.onLogTruncated(thisNode, fromIndex, firstRemoved)
				logBuffer.takeInPlace(writeIndex)
			}
		}

		override def resetLog(snapshot: SnapshotData[ParticipantId], tailRecords: IArray[Record]): Unit = {
			maybeLatestSnapshot = Maybe(snapshot)
			_logBufferOffset = snapshot.lastIncludedRecordIndex + 1
			logBuffer.clear()
			logBuffer.addAll(tailRecords)
		}

		override def truncatePrefix(snapshot: SnapshotData[ParticipantId]): Unit = {
			val newOffset = snapshot.lastIncludedRecordIndex + 1
			val dropCount = (newOffset - _logBufferOffset).toInt
			if dropCount > 0 then {
				if dropCount >= logBuffer.size then logBuffer.clear()
				else logBuffer.dropInPlace(dropCount)
				_logBufferOffset = newOffset
			}
			maybeLatestSnapshot = Maybe(snapshot)
		}

		override def release(): sequencer.Capture[Unit] = sequencer.Capture_unit

		def deepCopy(): TestWorkspace = {
			val cp = new TestWorkspace()
			cp.currentTerm = this.currentTerm
			cp._votedFor = this._votedFor
			cp._logBufferOffset = this._logBufferOffset
			cp.logBuffer.addAll(this.logBuffer)
			cp.maybeLatestSnapshot = this.maybeLatestSnapshot
			cp
		}
	}

	class TestStorage extends Storage {
		var savedMemory: TestWorkspace = new TestWorkspace()

		override def load: sequencer.Capture[TestWorkspace] = {
			sequencer.checkWithin()
			sequencer.Keeper(savedMemory.deepCopy())
		}

		override def save(workspace: TestWorkspace): sequencer.Capture[Unit] = {
			sequencer.checkWithin()
			val exceedsThreshold = workspace.getCurrentTerm >= autoSucceedUntilTerm || workspace.firstEmptyRecordIndex > autoSucceedUntilRecordIndex
			if !exceedsThreshold then {
				savedMemory = workspace.deepCopy()
				env.onStorageSaved(thisNode)
				sequencer.Capture_ready(Doer.successUnit)
			} else {
				val opId = env.nextStorageOpId()
				val captor = sequencer.Captor[Unit]()
				val pending = PendingPersistence(
					opId = opId,
					nodeId = myId,
					term = workspace.getCurrentTerm,
					logBufferOffset = workspace.logBufferOffset,
					firstEmptyRecordIndex = workspace.firstEmptyRecordIndex,
					records = workspace.getRecordsBetween(workspace.logBufferOffset, workspace.firstEmptyRecordIndex),
					snapshot = workspace.latestSnapshot,
					completeOp = () => {
						savedMemory = workspace.deepCopy()
						env.onStorageSaved(thisNode)
						stepDoer.executeSequentially(() => captor.captureSync(()))
					},
					failOp = err => {
						stepDoer.executeSequentially(() => captor.trapSync(err))
					}
				)
				env.registerPendingPersistence(pending)
				captor
			}
		}
	}

	class TestClusterParticipant extends ClusterParticipant {
		override val boundParticipantId: ParticipantId = myId
		var delegate: Delegate = uninitialized

		override def getInitialParticipants: Set[ParticipantId] = initialParticipants

		override def getOtherProbableParticipants: ListSet[ParticipantId] = ListSet.from(initialParticipants - myId)

		override def setBound(delegate: Delegate): Unit = {
			this.delegate = delegate
		}

		override def removeBound(): Unit = {
			this.delegate = null
		}

		override def onActiveConfigChanged(change: ConfigChange[ParticipantId], changeIndex: RecordIndex, roleOrdinal: RoleOrdinal): Unit = {
			env.onActiveConfigChanged(thisNode, change, changeIndex, roleOrdinal)
		}

		override def onQuiesced(motive: Try[String]): Unit = {
			env.onNodeQuiesced(thisNode, motive)
		}

		override def requestWakeUp(reason: WakeUpReason, wakeupsDone: Int, callback: () => Unit): WakeUpToken = {
			env.registerWakeUp(thisNode, reason, wakeupsDone, callback)
		}

		extension (destinationId: ParticipantId) {
			override def howAreYou(inquirerInfo: StateInfo): sequencer.Capture[StateInfo] = {
				sequencer.checkWithin()
				val captor = sequencer.Captor[StateInfo]()
				val req = RequestPacket(
					id = env.nextPacketId(),
					source = myId,
					destination = destinationId,
					departureTime = env.currentVirtualTime,
					rpc = ConsensusRpc.HowAreYou(inquirerInfo),
					completeCaller = res => stepDoer.executeSequentially(() => res.fold(e => captor.trapSync(e), v => captor.captureSync(v.asInstanceOf[StateInfo])))
				)
				env.enqueuePacket(req)
				captor
			}

			override def chooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.Capture[Vote[ParticipantId]] = {
				sequencer.checkWithin()
				val captor = sequencer.Captor[Vote[ParticipantId]]()
				val req = RequestPacket(
					id = env.nextPacketId(),
					source = myId,
					destination = destinationId,
					departureTime = env.currentVirtualTime,
					rpc = ConsensusRpc.ChooseALeader(inquirerId, inquirerInfo),
					completeCaller = res => stepDoer.executeSequentially(() => res.fold(e => captor.trapSync(e), v => captor.captureSync(v.asInstanceOf[Vote[ParticipantId]])))
				)
				env.enqueuePacket(req)
				captor
			}

			override def appendRecords(inquirerTerm: Term, prevLogIndex: RecordIndex, prevLogTerm: Term, batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.Capture[AppendResult] = {
				sequencer.checkWithin()
				val captor = sequencer.Captor[AppendResult]()
				val req = RequestPacket(
					id = env.nextPacketId(),
					source = myId,
					destination = destinationId,
					departureTime = env.currentVirtualTime,
					rpc = ConsensusRpc.AppendRecords(inquirerTerm, prevLogIndex, prevLogTerm, batch, leaderCommit, termAtLeaderCommit),
					completeCaller = res => stepDoer.executeSequentially(() => res.fold(e => captor.trapSync(e), v => captor.captureSync(v.asInstanceOf[AppendResult])))
				)
				env.enqueuePacket(req)
				captor
			}

			override def installSnapshot(inquirerTerm: Term, snapshot: SnapshotData[ParticipantId], batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.Capture[AppendResult] = {
				sequencer.checkWithin()
				val captor = sequencer.Captor[AppendResult]()
				val req = RequestPacket(
					id = env.nextPacketId(),
					source = myId,
					destination = destinationId,
					departureTime = env.currentVirtualTime,
					rpc = ConsensusRpc.InstallSnapshot(inquirerTerm, snapshot, batch, leaderCommit, termAtLeaderCommit),
					completeCaller = res => stepDoer.executeSequentially(() => res.fold(e => captor.trapSync(e), v => captor.captureSync(v.asInstanceOf[AppendResult])))
				)
				env.enqueuePacket(req)
				captor
			}

			override def permitQuiescence(indexOfGrantedStableConfigChange: RecordIndex): sequencer.Capture[Unit] = {
				sequencer.checkWithin()
				val captor = sequencer.Captor[Unit]()
				val req = RequestPacket(
					id = env.nextPacketId(),
					source = myId,
					destination = destinationId,
					departureTime = env.currentVirtualTime,
					rpc = ConsensusRpc.PermitQuiescence(indexOfGrantedStableConfigChange),
					completeCaller = res => stepDoer.executeSequentially(() => res.fold(e => captor.trapSync(e), _ => captor.captureSync(())))
				)
				env.enqueuePacket(req)
				captor
			}
		}
	}

	object notificationListener extends NotificationListener {
		override def onStarting(previous: RoleOrdinal, indexOfTheIncludingConfigChange: RecordIndex): Unit = ()

		override def onStarted(previous: RoleOrdinal, term: Term, initialConfigChange: ConfigChange[ParticipantId], isSeed: Boolean): Unit = ()

		override def onBecameQuiesced(previous: RoleOrdinal, term: Term, motive: Try[String]): Unit = ()

		override def onJoining(previous: RoleOrdinal, indexOfTheIncludingConfigChange: RecordIndex): Unit = ()

		override def onBecameIsolated(previous: RoleOrdinal, term: Term): Unit = ()

		override def onBecameCandidate(previous: RoleOrdinal, term: Term): Unit = ()

		override def onBecameFollower(previous: RoleOrdinal, term: Term, leaderId: ParticipantId): Unit = ()

		override def onPromoting(previous: RoleOrdinal, term: Term): Unit = ()

		override def onBecameLeader(previous: RoleOrdinal, term: Term): Unit = {
			env.onBecameLeader(thisNode, term)
		}

		override def onAbdicating(term: Term): Unit = ()

		override def onRetiring(previous: RoleOrdinal, term: Term): Unit = ()

		override def onRoleLeft(left: RoleOrdinal, term: Term): Unit = ()

		override def onCommitIndexChanged(previous: RecordIndex, current: RecordIndex, as: RoleOrdinal, at: Term): Unit = {
			env.onCommitIndexChanged(thisNode, previous, current, as, at)
		}

		override def onCommandApplied(appliedCommandIndex: RecordIndex, appliedCommandTerm: Term): Unit = ()

		override def onActiveConfigChanged(currentRole: RoleOrdinal, currentTerm: Term, configChangeIndex: RecordIndex, configChange: ConfigChange[ParticipantId]): Unit = ()
	}
}

/** An operation applied to [[ConsensusEnvironment]] recorded for deterministic replay and undo. */
sealed trait EnvOperation

object EnvOperation {
	final case class StepNode(node: NodeId) extends EnvOperation
	case object StepAllNodes extends EnvOperation

	final case class RunNodeUntilIdle(node: NodeId, maxSteps: Int) extends EnvOperation
	final case class RunAllNodesUntilIdle(maxRounds: Int) extends EnvOperation

	final case class DeliverPacket(packetId: PacketId) extends EnvOperation
	final case class DropPacket(packetId: PacketId) extends EnvOperation

	final case class DeliverNext(from: NodeId, to: NodeId) extends EnvOperation

	final case class DropNext(from: NodeId, to: NodeId) extends EnvOperation

	final case class DeliverFirstN(from: NodeId, to: NodeId, n: Int) extends EnvOperation

	final case class DropFirstN(from: NodeId, to: NodeId, n: Int) extends EnvOperation

	final case class DeliverAllBetween(from: NodeId, to: NodeId) extends EnvOperation

	final case class DropAllBetween(from: NodeId, to: NodeId) extends EnvOperation

	final case class DeliverAllTo(to: NodeId) extends EnvOperation

	case object DeliverAll extends EnvOperation
	final case class FailPacket(packetId: PacketId, errorMsg: String) extends EnvOperation

	final case class AdvanceTime(ticks: Int) extends EnvOperation
	final case class TriggerWakeUp(tokenId: Int) extends EnvOperation

	final case class CompleteStorageSave(opId: StorageOpId) extends EnvOperation

	final case class CompleteNextStorageSave(node: NodeId) extends EnvOperation

	final case class CompleteAllStorageSaves(node: NodeId) extends EnvOperation
	final case class FailStorageSave(opId: StorageOpId, errorMsg: String) extends EnvOperation

	final case class StartNode(node: NodeId) extends EnvOperation

	final case class CrashNode(node: NodeId) extends EnvOperation

	final case class RestartNode(node: NodeId) extends EnvOperation
	case object StartAllNodes extends EnvOperation

	final case class SubmitClientCommand(targetNode: NodeId, client: String, serial: Option[Int], attemptFlag: CommandAttemptFlag) extends EnvOperation

	final case class SubmitConfigChange(targetNode: NodeId, desiredParticipants: Set[NodeId]) extends EnvOperation

	final case class UpdateDynamicSettings(
		retiringMaxRetries: Option[Int],
		logRetention: Option[Int],
		autoSucceedUntilTerm: Option[Term],
		autoSucceedUntilRecordIndex: Option[RecordIndex],
		targetNode: Option[NodeId]
	) extends EnvOperation

	final case class ToggleStorageAutoSucceed(targetNode: Option[NodeId]) extends EnvOperation
}

/** The discrete-event, fine-grained testing harness for [[ConsensusParticipantSdm]]. */
class ConsensusEnvironment(
	val clusterSize: Int = 3,
	val initialSeedParticipants: Option[Set[? <: NodeRef]] = None,
	val ticksPerMilli: Int = 10,
	val maxInFlightAppendsPerPeer: Int = 2,
	val logCompactionThreshold: Int = 5,
	val initialRetiringParticipantMaxRetries: Int = 2,
	val initialLogRetentionAfterSnapshot: Int = 1,
	var initializer: ConsensusEnvironment => Unit = _ => ()
) {
	var retiringParticipantMaxRetries: Int = initialRetiringParticipantMaxRetries
	var logRetentionAfterSnapshot: Int = initialLogRetentionAfterSnapshot
	var remembersLastAppliedCommandIndex: Boolean = false
	var onNodeQuiescedHook: (EnvironmentNode, Try[String]) => Unit = (node, _) => node.release()
	var rpcLifecycleListener: Option[RpcLifecycleListener] = None

	private var _virtualTime: VirtualTime = 0
	private var packetIdSequencer: PacketId = 0
	private var storageOpIdSequencer: StorageOpId = 0
	private var commandIdSequencer: Int = 0
	private var configReqIdSequencer: Int = 0
	private var wakeUpTokenSequencer: Int = 0

	private val channels: mutable.Map[(NodeId, NodeId), mutable.ArrayDeque[Packet]] = mutable.Map.empty
	private val nodesMap: mutable.Map[NodeId, EnvironmentNode] = mutable.Map.empty
	private val pendingWakeUpsMap: mutable.Map[Int, PendingWakeUp] = mutable.Map.empty
	private val pendingPersistenceMap: mutable.Map[StorageOpId, PendingPersistence] = mutable.Map.empty
	private val clientStatuses: mutable.Map[Int, ClientCommandStatus] = mutable.Map.empty
	private val clientSerialCounters: mutable.Map[String, Int] = mutable.Map.empty.withDefaultValue(0)
	private val clientLastSent: mutable.Map[String, Int] = mutable.Map.empty
	private val clientLastSuccess: mutable.Map[String, Int] = mutable.Map.empty
	private val clientLastRecordIndex: mutable.Map[String, Long] = mutable.Map.empty
	private val configStatuses: mutable.Map[String, ConfigChangeStatus] = mutable.Map.empty

	// Invariant Tracking
	private val leaderByTerm: mutable.Map[Term, NodeId] = mutable.Map.empty
	private val appliedCommandsByIndex: mutable.Map[RecordIndex, (NodeId, TestClientCommand)] = mutable.Map.empty
	private val committedRecordsByNode: mutable.Map[NodeId, mutable.ArrayBuffer[Record | None.type]] = mutable.Map.empty

	// Operation Memory & Tags
	private val _appliedOperations: mutable.ArrayBuffer[EnvOperation] = mutable.ArrayBuffer.empty
	private val _tags: mutable.Map[String, List[EnvOperation]] = mutable.Map.empty
	private var isReplaying: Boolean = false
	private var operationDepth: Int = 0

	val defaultInitialParticipants: ListSet[NodeId] = initialSeedParticipants match {
		case Some(seeds) => ListSet.from(seeds.map(_.asNodeId))
		case None => ListSet.from((0 until clusterSize).map(i => NodeId(s"p-$i")))
	}

	for i <- 0 until clusterSize do {
		val id = NodeId(s"p-$i")
		nodesMap(id) = new EnvironmentNode(id, defaultInitialParticipants, this)
		committedRecordsByNode(id) = mutable.ArrayBuffer.empty
	}

	inline def currentVirtualTime: VirtualTime = _virtualTime

	private inline def recordOrExecute[T](op: => EnvOperation)(action: => T): T = {
		if !isReplaying && operationDepth == 0 then {
			_appliedOperations.append(op)
		}
		operationDepth += 1
		try {
			action
		} finally {
			operationDepth -= 1
		}
	}

	def appliedOperations: Seq[EnvOperation] = _appliedOperations.toSeq

	def appliedOperationsCount: Int = _appliedOperations.size

	def tags: Map[String, List[EnvOperation]] = _tags.toMap

	def createTag(name: String): Unit = {
		_tags(name) = _appliedOperations.toList
	}

	def restoreTag(name: String): Boolean = {
		_tags.get(name) match {
			case None => false
			case Some(savedOps) =>
				reset()
				_appliedOperations.clear()
				_appliedOperations.addAll(savedOps)
				isReplaying = true
				try {
					for op <- savedOps do applyOperation(op)
				} finally {
					isReplaying = false
				}
				true
		}
	}

	def deleteTag(name: String): Boolean = {
		_tags.remove(name).isDefined
	}

	def undo(): Boolean = {
		if _appliedOperations.isEmpty then false
		else {
			val opsToReplay = _appliedOperations.dropRight(1).toList
			reset()
			_appliedOperations.clear()
			_appliedOperations.addAll(opsToReplay)
			isReplaying = true
			try {
				for op <- opsToReplay do applyOperation(op)
			} finally {
				isReplaying = false
			}
			true
		}
	}

	def reset(): Unit = {
		_virtualTime = 0
		packetIdSequencer = 0
		storageOpIdSequencer = 0
		commandIdSequencer = 0
		configReqIdSequencer = 0
		wakeUpTokenSequencer = 0

		channels.clear()
		nodesMap.clear()
		pendingWakeUpsMap.clear()
		pendingPersistenceMap.clear()
		clientStatuses.clear()
		clientSerialCounters.clear()
		clientLastSent.clear()
		clientLastSuccess.clear()
		clientLastRecordIndex.clear()
		configStatuses.clear()
		leaderByTerm.clear()
		appliedCommandsByIndex.clear()
		committedRecordsByNode.clear()

		retiringParticipantMaxRetries = initialRetiringParticipantMaxRetries
		logRetentionAfterSnapshot = initialLogRetentionAfterSnapshot
		remembersLastAppliedCommandIndex = false
		onNodeQuiescedHook = (node, _) => node.release()

		for i <- 0 until clusterSize do {
			val id = NodeId(s"p-$i")
			nodesMap(id) = new EnvironmentNode(id, defaultInitialParticipants, this)
			committedRecordsByNode(id) = mutable.ArrayBuffer.empty
		}

		val prevReplaying = isReplaying
		isReplaying = true
		try {
			initializer(this)
		} finally {
			isReplaying = prevReplaying
		}
	}

	def applyOperation(op: EnvOperation): Unit = op match {
		case EnvOperation.StepNode(node) => stepNode(node)
		case EnvOperation.StepAllNodes => stepAllNodes()
		case EnvOperation.RunNodeUntilIdle(node, maxSteps) => runNodeUntilIdle(node, maxSteps)
		case EnvOperation.RunAllNodesUntilIdle(maxRounds) => runAllNodesUntilIdle(maxRounds)
		case EnvOperation.DeliverPacket(id) => deliverPacket(id)
		case EnvOperation.DropPacket(id) => dropPacket(id)
		case EnvOperation.DeliverNext(from, to) => deliverNext(from, to)
		case EnvOperation.DropNext(from, to) => dropNext(from, to)
		case EnvOperation.DeliverFirstN(from, to, n) => deliverFirstN(from, to, n)
		case EnvOperation.DropFirstN(from, to, n) => dropFirstN(from, to, n)
		case EnvOperation.DeliverAllBetween(from, to) => deliverAllBetween(from, to)
		case EnvOperation.DropAllBetween(from, to) => dropAllBetween(from, to)
		case EnvOperation.DeliverAllTo(to) => deliverAllTo(to)
		case EnvOperation.DeliverAll => deliverAll()
		case EnvOperation.FailPacket(id, msg) => failPacket(id, new java.io.IOException(msg))
		case EnvOperation.AdvanceTime(ticks) => advanceTime(ticks)
		case EnvOperation.TriggerWakeUp(tokenId) => triggerWakeUp(tokenId)
		case EnvOperation.CompleteStorageSave(opId) => completeStorageSave(opId)
		case EnvOperation.CompleteNextStorageSave(node) => completeNextStorageSave(node)
		case EnvOperation.CompleteAllStorageSaves(node) => completeAllStorageSaves(node)
		case EnvOperation.FailStorageSave(opId, msg) => failStorageSave(opId, new RuntimeException(msg))
		case EnvOperation.StartNode(node) => startNode(node)
		case EnvOperation.CrashNode(node) => crashNode(node)
		case EnvOperation.RestartNode(node) => restartNode(node)
		case EnvOperation.StartAllNodes => startAllNodes()
		case EnvOperation.SubmitClientCommand(target, client, serial, flag) => submitClientCommand(target, client, serial, flag)
		case EnvOperation.SubmitConfigChange(target, desired) => submitConfigChange(target, desired)
		case EnvOperation.UpdateDynamicSettings(retries, retention, term, idx, target) => updateDynamicSettings(retries, retention, term, idx, target)
		case EnvOperation.ToggleStorageAutoSucceed(target) => toggleStorageAutoSucceed(target)
	}

	def updateDynamicSettings(
		retiringMaxRetries: Option[Int] = None,
		logRetention: Option[Int] = None,
		autoSucceedUntilTerm: Option[Term] = None,
		autoSucceedUntilRecordIndex: Option[RecordIndex] = None,
		targetNode: Option[NodeRef] = None
	): Unit = recordOrExecute(EnvOperation.UpdateDynamicSettings(retiringMaxRetries, logRetention, autoSucceedUntilTerm, autoSucceedUntilRecordIndex, targetNode.map(_.asNodeId))) {
		retiringMaxRetries.foreach(v => retiringParticipantMaxRetries = v)
		logRetention.foreach(v => logRetentionAfterSnapshot = v)
		targetNode match {
			case Some(ref) if ref.asNodeId.nonEmpty && ref.asNodeId != "all" =>
				val n = node(ref)
				autoSucceedUntilTerm.foreach(t => n.autoSucceedUntilTerm = t)
				autoSucceedUntilRecordIndex.foreach(i => n.autoSucceedUntilRecordIndex = i)
			case _ =>
				for i <- 0 until clusterSize do {
					val n = node(i)
					autoSucceedUntilTerm.foreach(t => n.autoSucceedUntilTerm = t)
					autoSucceedUntilRecordIndex.foreach(i => n.autoSucceedUntilRecordIndex = i)
				}
		}
	}

	def toggleStorageAutoSucceed(targetNode: Option[NodeRef] = None): Unit = recordOrExecute(EnvOperation.ToggleStorageAutoSucceed(targetNode.map(_.asNodeId))) {
		targetNode match {
			case Some(ref) if ref.asNodeId.nonEmpty && ref.asNodeId != "all" =>
				val n = node(ref)
				val currentlyAuto = n.autoSucceedUntilTerm == Int.MaxValue && n.autoSucceedUntilRecordIndex == Long.MaxValue
				if currentlyAuto then {
					n.autoSucceedUntilRecordIndex = 0L
				} else {
					n.autoSucceedUntilTerm = Int.MaxValue.asInstanceOf[Term]
					n.autoSucceedUntilRecordIndex = Long.MaxValue
				}
			case _ =>
				val anyAuto = (0 until clusterSize).exists(i => node(i).autoSucceedUntilTerm == Int.MaxValue && node(i).autoSucceedUntilRecordIndex == Long.MaxValue)
				for i <- 0 until clusterSize do {
					val n = node(i)
					if anyAuto then {
						n.autoSucceedUntilRecordIndex = 0L
					} else {
						n.autoSucceedUntilTerm = Int.MaxValue.asInstanceOf[Term]
						n.autoSucceedUntilRecordIndex = Long.MaxValue
					}
				}
		}
	}

	private[readren] def nextPacketId(): PacketId = {
		packetIdSequencer += 1
		packetIdSequencer
	}

	private[readren] def nextStorageOpId(): StorageOpId = {
		storageOpIdSequencer += 1
		storageOpIdSequencer
	}

	private[readren] def nextCommandId(): Int = {
		commandIdSequencer += 1
		commandIdSequencer
	}

	private[readren] def nextConfigReqId(): Int = {
		configReqIdSequencer += 1
		configReqIdSequencer
	}

	def travelingPacketsCount: Int = channels.values.map(_.size).sum

	private def channelQueue(from: NodeId, to: NodeId): mutable.ArrayDeque[Packet] = {
		channels.getOrElseUpdate((from, to), mutable.ArrayDeque.empty)
	}

	private[readren] def enqueuePacket(packet: Packet): Unit = {
		val onTheWay = travelingPacketsCount
		channelQueue(packet.source, packet.destination).append(packet)
		packet match {
			case req: RequestPacket =>
				rpcLifecycleListener.foreach(_.onRpcEnqueued(req, onTheWay))
			case _ => ()
		}
	}

	// Node Management
	def node(ref: NodeRef): EnvironmentNode = nodesMap(ref.asNodeId)

	def startNode(ref: NodeRef): Unit = recordOrExecute(EnvOperation.StartNode(ref.asNodeId)) {
		val n = node(ref)
		n.startIfNotRunning(0, defaultInitialParticipants)
	}

	def startAllNodes(): Unit = recordOrExecute(EnvOperation.StartAllNodes) {
		for id <- defaultInitialParticipants do startNode(id)
	}

	def crashNode(ref: NodeRef): Unit = recordOrExecute(EnvOperation.CrashNode(ref.asNodeId)) {
		val n = node(ref)
		n.isDown = true
		n.stepDoer.clear()
	}

	def restartNode(ref: NodeRef): Unit = recordOrExecute(EnvOperation.RestartNode(ref.asNodeId)) {
		val n = node(ref)
		n.isDown = false
		n.startIfNotRunning(0, defaultInitialParticipants)
	}

	def nodeRole(ref: NodeRef): String = {
		val n = node(ref)
		if n.isDown || n.participant == null then "DOWN"
		else RoleOrdinal_nameOf(n.participant.getRoleOrdinal)
	}

	// Stepping Operations
	def stepNode(ref: NodeRef): Boolean = recordOrExecute(EnvOperation.StepNode(ref.asNodeId)) {
		node(ref).stepDoer.step()
	}

	def stepAllNodes(): Int = recordOrExecute(EnvOperation.StepAllNodes) {
		var stepped = 0
		for n <- nodesMap.values do {
			if n.stepDoer.step() then stepped += 1
		}
		stepped
	}

	def runNodeUntilIdle(ref: NodeRef, maxSteps: Int = 1000): Int = recordOrExecute(EnvOperation.RunNodeUntilIdle(ref.asNodeId, maxSteps)) {
		node(ref).stepDoer.drain(maxSteps)
	}

	def runAllNodesUntilIdle(maxRounds: Int = 1000): Int = recordOrExecute(EnvOperation.RunAllNodesUntilIdle(maxRounds)) {
		var totalExecuted = 0
		var rounds = 0
		var progressed = true
		while progressed && rounds < maxRounds do {
			rounds += 1
			val count = stepAllNodes()
			totalExecuted += count
			progressed = count > 0
		}
		totalExecuted
	}

	// Packet Inspection & Operations
	def pendingPackets: Seq[Packet] = {
		channels.values.flatten.toSeq.sortBy(_.id)
	}

	def pendingPacketsBetween(from: NodeRef, to: NodeRef): Seq[Packet] = {
		channelQueue(from.asNodeId, to.asNodeId).toSeq
	}

	def deliverPacket(packetId: PacketId): DeliverOutcome = recordOrExecute(EnvOperation.DeliverPacket(packetId)) {
		val targetQueueOpt = channels.find(_._2.exists(_.id == packetId))
		targetQueueOpt match {
			case None => throw new NoSuchElementException(s"Packet $packetId not found in any channel")
			case Some((fromTo, queue)) =>
				val idx = queue.indexWhere(_.id == packetId)
				val packet = queue.remove(idx)
				executeDeliver(packet)
		}
	}

	def deliverNext(from: NodeRef, to: NodeRef): Option[DeliverOutcome] = recordOrExecute(EnvOperation.DeliverNext(from.asNodeId, to.asNodeId)) {
		val queue = channelQueue(from.asNodeId, to.asNodeId)
		if queue.isEmpty then None
		else Some(executeDeliver(queue.removeHead()))
	}

	def deliverFirstN(from: NodeRef, to: NodeRef, n: Int): Seq[DeliverOutcome] = recordOrExecute(EnvOperation.DeliverFirstN(from.asNodeId, to.asNodeId, n)) {
		val queue = channelQueue(from.asNodeId, to.asNodeId)
		val count = math.min(n, queue.size)
		(0 until count).map(_ => executeDeliver(queue.removeHead()))
	}

	def deliverAllBetween(from: NodeRef, to: NodeRef): Seq[DeliverOutcome] = recordOrExecute(EnvOperation.DeliverAllBetween(from.asNodeId, to.asNodeId)) {
		val queue = channelQueue(from.asNodeId, to.asNodeId)
		val res = mutable.ArrayBuffer.empty[DeliverOutcome]
		while queue.nonEmpty do {
			res.append(executeDeliver(queue.removeHead()))
		}
		res.toSeq
	}

	def deliverAllTo(to: NodeRef): Seq[DeliverOutcome] = recordOrExecute(EnvOperation.DeliverAllTo(to.asNodeId)) {
		val toId = to.asNodeId
		val res = mutable.ArrayBuffer.empty[DeliverOutcome]
		for ((f, t), q) <- channels if t == toId do {
			while q.nonEmpty do res.append(executeDeliver(q.removeHead()))
		}
		res.toSeq
	}

	def deliverAll(): Seq[DeliverOutcome] = recordOrExecute(EnvOperation.DeliverAll) {
		val all = pendingPackets
		all.map(p => deliverPacket(p.id))
	}

	def dropPacket(packetId: PacketId): Boolean = recordOrExecute(EnvOperation.DropPacket(packetId)) {
		failPacket(packetId, new java.io.IOException(s"Network packet $packetId dropped in transit"))
	}

	def dropNext(from: NodeRef, to: NodeRef): Boolean = recordOrExecute(EnvOperation.DropNext(from.asNodeId, to.asNodeId)) {
		dropFirstN(from, to, 1) > 0
	}

	def dropFirstN(from: NodeRef, to: NodeRef, n: Int): Int = recordOrExecute(EnvOperation.DropFirstN(from.asNodeId, to.asNodeId, n)) {
		val q = channelQueue(from.asNodeId, to.asNodeId)
		val count = math.min(n, q.size)
		for _ <- 0 until count do {
			val packet = q.removeHead()
			val error = new java.io.IOException(s"Network packet ${packet.id} dropped in transit")
			packet match {
				case req: RequestPacket =>
					val syntheticResp = ResponsePacket(
						id = packet.id,
						source = req.destination,
						destination = req.source,
						departureTime = _virtualTime,
						correlationRequestId = req.id,
						rpc = req.rpc,
						response = Failure(error),
						completeCaller = req.completeCaller
					)
					rpcLifecycleListener.foreach(_.onResponseDelivered(syntheticResp, travelingPacketsCount))
					req.completeCaller(Failure(error))
				case resp: ResponsePacket =>
					val failedResp = resp.copy(response = Failure(error))
					rpcLifecycleListener.foreach(_.onResponseDelivered(failedResp, travelingPacketsCount))
					resp.completeCaller(Failure(error))
			}
		}
		count
	}

	def dropAllBetween(from: NodeRef, to: NodeRef): Int = recordOrExecute(EnvOperation.DropAllBetween(from.asNodeId, to.asNodeId)) {
		val q = channelQueue(from.asNodeId, to.asNodeId)
		dropFirstN(from, to, q.size)
	}

	def failPacket(packetId: PacketId, error: Throwable): Boolean = recordOrExecute(EnvOperation.FailPacket(packetId, error.getMessage)) {
		channels.values.find(_.exists(_.id == packetId)).fold(false) { q =>
			val idx = q.indexWhere(_.id == packetId)
			val packet = q.remove(idx)
			packet match {
				case req: RequestPacket =>
					val syntheticResp = ResponsePacket(
						id = packet.id,
						source = req.destination,
						destination = req.source,
						departureTime = _virtualTime,
						correlationRequestId = req.id,
						rpc = req.rpc,
						response = Failure(error),
						completeCaller = req.completeCaller
					)
					rpcLifecycleListener.foreach(_.onResponseDelivered(syntheticResp, travelingPacketsCount))
					req.completeCaller(Failure(error))
				case resp: ResponsePacket =>
					val failedResp = resp.copy(response = Failure(error))
					rpcLifecycleListener.foreach(_.onResponseDelivered(failedResp, travelingPacketsCount))
					resp.completeCaller(Failure(error))
			}
			true
		}
	}

	private def executeDeliver(packet: Packet): DeliverOutcome = {
		val destNode = nodesMap.getOrElse(packet.destination, throw new IllegalStateException(s"Destination node ${packet.destination} does not exist"))
		val hadPending = destNode.stepDoer.hasPendingTasks
		val warning = if hadPending then Some(s"Destination node ${packet.destination} had ${destNode.stepDoer.pendingTasksCount} pending task(s) when packet ${packet.id} was delivered.") else None

		packet match {
			case req: RequestPacket =>
				destNode.stepDoer.executeSequentially(() => {
					if destNode.isDown || (destNode.participant eq null) then {
						val failure = Failure(new RuntimeException(s"Node ${destNode.myId} is down"))
						rpcLifecycleListener.foreach(_.onRpcCompleted(req, failure, travelingPacketsCount))
						val resp = ResponsePacket(
							id = nextPacketId(),
							source = req.destination,
							destination = req.source,
							departureTime = _virtualTime,
							correlationRequestId = req.id,
							rpc = req.rpc,
							response = failure,
							completeCaller = req.completeCaller
						)
						enqueuePacket(resp)
					} else {
						val onSuccess: Any => Unit = result => {
							val outcome = Success(result)
							rpcLifecycleListener.foreach(_.onRpcCompleted(req, outcome, travelingPacketsCount))
							val resp = ResponsePacket(
								id = nextPacketId(),
								source = req.destination,
								destination = req.source,
								departureTime = _virtualTime,
								correlationRequestId = req.id,
								rpc = req.rpc,
								response = outcome,
								completeCaller = req.completeCaller
							)
							enqueuePacket(resp)
						}
						val onError: Throwable => Unit = ex => {
							val outcome = Failure(ex)
							rpcLifecycleListener.foreach(_.onRpcCompleted(req, outcome, travelingPacketsCount))
							val resp = ResponsePacket(
								id = nextPacketId(),
								source = req.destination,
								destination = req.source,
								departureTime = _virtualTime,
								correlationRequestId = req.id,
								rpc = req.rpc,
								response = outcome,
								completeCaller = req.completeCaller
							)
							enqueuePacket(resp)
						}

						rpcLifecycleListener.foreach(_.onRpcDelivering(req, travelingPacketsCount))

						req.rpc match {
							case ConsensusRpc.HowAreYou(inquirerInfo) =>
								destNode.clusterParticipant.delegate.onHowAreYou(req.source, inquirerInfo).triggerSyncCallbacks(onSuccess, onError)
							case ConsensusRpc.ChooseALeader(inquirerId, inquirerInfo) =>
								destNode.clusterParticipant.delegate.onChooseALeader(inquirerId, inquirerInfo).triggerSyncCallbacks(onSuccess, onError)
							case ConsensusRpc.AppendRecords(inquirerTerm, prevLogIndex, prevLogTerm, batch, leaderCommit, termAtLeaderCommit) =>
								destNode.clusterParticipant.delegate.onAppendRecords(req.source, inquirerTerm, prevLogIndex, prevLogTerm, batch, leaderCommit, termAtLeaderCommit).triggerSyncCallbacks(onSuccess, onError)
							case ConsensusRpc.InstallSnapshot(inquirerTerm, snapshot, batch, leaderCommit, termAtLeaderCommit) =>
								destNode.clusterParticipant.delegate.onInstallSnapshot(req.source, inquirerTerm, snapshot, batch, leaderCommit, termAtLeaderCommit).triggerSyncCallbacks(onSuccess, onError)
							case ConsensusRpc.PermitQuiescence(indexOfGrantedStableConfigChange) =>
								destNode.clusterParticipant.delegate.onQuiescencePermitted(req.source, indexOfGrantedStableConfigChange)
								onSuccess(())
						}
					}
				})

			case resp: ResponsePacket =>
				rpcLifecycleListener.foreach(_.onResponseDelivered(resp, travelingPacketsCount))
				resp.completeCaller(resp.response)
		}

		DeliverOutcome(packet.id, packet.source, packet.destination, hadPending, warning)
	}

	// Virtual Clock & Wake-Ups
	private[readren] def registerWakeUp(node: EnvironmentNode, reason: WakeUpReason, wakeupsDone: Int, callback: () => Unit): WakeUpToken = {
		wakeUpTokenSequencer += 1
		val tokenId = wakeUpTokenSequencer
		val delayTicks = reason match {
			case WakeUpReason.RetirementDriveRetry => 10 * (wakeupsDone + 1) * ticksPerMilli
			case WakeUpReason.UnreachableFollowersRetry => 10 * (wakeupsDone + 1) * ticksPerMilli
			case WakeUpReason.QuiescenceAuthorizationRetry => 10 * (wakeupsDone + 1) * ticksPerMilli
			case WakeUpReason.ReplicationLoopRetry => 10 * (wakeupsDone + 1) * ticksPerMilli
		}
		val scheduled = _virtualTime + delayTicks
		val token = new WakeUpToken {
			override def cancel(): Unit = pendingWakeUpsMap.remove(tokenId)
		}
		pendingWakeUpsMap(tokenId) = PendingWakeUp(tokenId, token, node.myId, reason, wakeupsDone, scheduled, callback)
		token
	}

	def pendingWakeUps: Seq[PendingWakeUp] = pendingWakeUpsMap.values.toSeq.sortBy(_.scheduledTime)

	def advanceTime(ticks: Int): Seq[PendingWakeUp] = recordOrExecute(EnvOperation.AdvanceTime(ticks)) {
		_virtualTime += ticks
		val expired = pendingWakeUpsMap.values.filter(_.scheduledTime <= _virtualTime).toSeq.sortBy(_.scheduledTime)
		for w <- expired do {
			pendingWakeUpsMap.remove(w.tokenId)
			val n = nodesMap(w.nodeId)
			n.stepDoer.executeSequentially(() => w.callback())
		}
		expired
	}

	def advanceTimeMillis(ms: Int): Seq[PendingWakeUp] = advanceTime(ms * ticksPerMilli)

	def triggerWakeUp(tokenId: Int): Boolean = recordOrExecute(EnvOperation.TriggerWakeUp(tokenId)) {
		pendingWakeUpsMap.remove(tokenId).fold(false) { w =>
			val n = nodesMap(w.nodeId)
			n.stepDoer.executeSequentially(() => w.callback())
			true
		}
	}

	// Storage Operations & Thresholds
	private[readren] def registerPendingPersistence(p: PendingPersistence): Unit = {
		pendingPersistenceMap(p.opId) = p
	}

	def pendingPersistenceOperations: Seq[PendingPersistence] = pendingPersistenceMap.values.toSeq.sortBy(_.opId)

	def completeStorageSave(opId: StorageOpId): Boolean = recordOrExecute(EnvOperation.CompleteStorageSave(opId)) {
		pendingPersistenceMap.remove(opId).fold(false) { p =>
			p.completeOp()
			true
		}
	}

	def completeNextStorageSave(nodeRef: NodeRef): Boolean = recordOrExecute(EnvOperation.CompleteNextStorageSave(nodeRef.asNodeId)) {
		val nodeId = nodeRef.asNodeId
		pendingPersistenceOperations.find(_.nodeId == nodeId).fold(false) { p =>
			completeStorageSave(p.opId)
		}
	}

	def completeAllStorageSaves(nodeRef: NodeRef): Int = recordOrExecute(EnvOperation.CompleteAllStorageSaves(nodeRef.asNodeId)) {
		val nodeId = nodeRef.asNodeId
		val matching = pendingPersistenceOperations.filter(_.nodeId == nodeId)
		for p <- matching do completeStorageSave(p.opId)
		matching.size
	}

	def failStorageSave(opId: StorageOpId, error: Throwable): Boolean = recordOrExecute(EnvOperation.FailStorageSave(opId, error.getMessage)) {
		pendingPersistenceMap.remove(opId).fold(false) { p =>
			p.failOp(error)
			true
		}
	}

	// Client Command Injections
	def submitClientCommand(
		targetNode: NodeRef,
		client: ClientRef,
		serial: Option[Int] = None,
		attemptFlag: CommandAttemptFlag = FIRST_ATTEMPT
	): ClientCommandHandle = {
		val clientId = client.asClientId
		val targetId = targetNode.asNodeId
		val cmdSerial = serial match {
			case Some(s) =>
				clientSerialCounters(clientId) = math.max(clientSerialCounters(clientId), s)
				s
			case None =>
				val s = clientSerialCounters(clientId) + 1
				clientSerialCounters(clientId) = s
				s
		}
		recordOrExecute(EnvOperation.SubmitClientCommand(targetId, clientId, Some(cmdSerial), attemptFlag)) {
			val cmdId = nextCommandId()
			val handle = ClientCommandHandle(cmdId, clientId, cmdSerial, targetId, _virtualTime)
			clientStatuses(cmdId) = ClientCommandStatus.InFlight
			clientLastSent(clientId) = cmdSerial

			val n = node(targetId)
			n.stepDoer.executeSequentially(() => {
				if n.isDown || n.participant == null then {
					clientStatuses(cmdId) = ClientCommandStatus.Failed(new RuntimeException(s"Node $targetId is down"))
				} else {
					val command = TestClientCommand(cmdSerial, clientId)
					val capture = n.clusterParticipant.delegate.onCommandFromClient(command, attemptFlag)
					capture.triggerSyncCallbacks(
						{
							case n.Processed(recIdx, content) =>
								clientStatuses(cmdId) = ClientCommandStatus.Processed(recIdx, content)
								clientLastSuccess(clientId) = cmdSerial
								clientLastRecordIndex(clientId) = recIdx
							case n.RedirectTo(leaderId) => clientStatuses(cmdId) = ClientCommandStatus.Redirected(leaderId)
							case n.Unable(flag, others) => clientStatuses(cmdId) = ClientCommandStatus.Unable(flag, others)
						},
						ex => {
							clientStatuses(cmdId) = ClientCommandStatus.Failed(ex)
						}
					)
				}
			})
			handle
		}
	}

	def clientCommandStatus(commandId: Int): ClientCommandStatus = clientStatuses.getOrElse(commandId, ClientCommandStatus.InFlight)

	def inFlightClientCommandsCount(client: ClientRef): Int = {
		val cid = client.asClientId
		clientStatuses.values.count {
			case ClientCommandStatus.InFlight => true
			case _ => false
		}
	}

	// Configuration Change Injections
	def submitConfigChange(
		targetNode: NodeRef,
		desiredParticipants: Set[? <: NodeRef],
		priorAnswer: Maybe[ConfigChangeResponse] = Maybe.empty
	): ConfigChangeHandle = {
		val targetId = targetNode.asNodeId
		val desiredIds: Set[NodeId] = desiredParticipants.map(_.asNodeId)
		recordOrExecute(EnvOperation.SubmitConfigChange(targetId, desiredIds)) {
			val reqId = s"ccReq-${nextConfigReqId()}"
			val handle = ConfigChangeHandle(reqId, targetId, desiredIds, _virtualTime)
			configStatuses(reqId) = ConfigChangeStatus.InFlight

			val n = node(targetId)
			n.stepDoer.executeSequentially(() => {
				if n.isDown || n.participant == null then {
					configStatuses(reqId) = ConfigChangeStatus.Failed(new RuntimeException(s"Node $targetId is down"))
				} else {
					val capture = n.clusterParticipant.delegate.requestConfigChange(reqId, desiredIds, priorAnswer)
					capture.triggerSyncCallbacks(
						res => {
							configStatuses(reqId) = ConfigChangeStatus.Completed(res)
						},
						ex => {
							configStatuses(reqId) = ConfigChangeStatus.Failed(ex)
						}
					)
				}
			})
			handle
		}
	}

	def configChangeStatus(requestId: String): ConfigChangeStatus = configStatuses.getOrElse(requestId, ConfigChangeStatus.InFlight)

	// Invariant Checks & Internal Callbacks
	private[readren] def onLogTruncated(node: EnvironmentNode, index: RecordIndex, firstRemovedRecord: Record): Unit = {
		if node.participant != null && node.participant.getRoleOrdinal == LEADER then {
			throw new AssertionError(s"Leader Append-Only invariant violated: Leader ${node.myId} truncated log at index $index. Removed: $firstRemovedRecord")
		}
	}

	private[readren] def onStorageSaved(node: EnvironmentNode): Unit = {
		checkLogMatching()
	}

	private[readren] def onBecameLeader(node: EnvironmentNode, term: Term): Unit = {
		leaderByTerm.get(term) match {
			case Some(existing) if existing != node.myId =>
				throw new AssertionError(s"Election Safety invariant violated: Multiple leaders elected in term $term: $existing and ${node.myId}")
			case _ =>
				leaderByTerm(term) = node.myId
		}
	}

	private[readren] def onCommitIndexChanged(node: EnvironmentNode, previous: RecordIndex, current: RecordIndex, as: RoleOrdinal, at: Term): Unit = {
		val thisNodeCommitted = committedRecordsByNode.getOrElseUpdate(node.myId, mutable.ArrayBuffer.empty)
		val mem = node.storage.savedMemory
		val thisNodeLogBufferOffset = mem.logBufferOffset

		// Validate consistency for any overlapping records that were already recorded before restart/re-join
		val indexOfFirstRecordToCheck = (previous + 1).max(thisNodeLogBufferOffset)
		val indexOfLastRecordToCheck = current.min(thisNodeCommitted.size.toLong)
		var checkIdx = indexOfFirstRecordToCheck
		while checkIdx <= indexOfLastRecordToCheck do {
			thisNodeCommitted(checkIdx.toInt - 1) match {
				case contentInParallelMemory: Record =>
					val contentInStorage = mem.getRecordAt(checkIdx)
					if contentInStorage != contentInParallelMemory then {
						throw new AssertionError(s"Node ${node.myId} committed a different record at index $checkIdx after restart: previous=$contentInParallelMemory, current=$contentInStorage")
					}
				case None => ()
			}
			checkIdx += 1
		}

		// Memorize the committed records in parallel memory, filling potential holes with None
		val indexOfFirstRecordToAppend = indexOfFirstRecordToCheck.max(thisNodeCommitted.size + 1L)
		if indexOfFirstRecordToAppend <= current then {
			val indexOfFirstRecordToAppendBase0 = indexOfFirstRecordToAppend.toInt - 1
			val holeLength = indexOfFirstRecordToAppendBase0 - thisNodeCommitted.size
			if holeLength > 0 then thisNodeCommitted.addAll(Iterable.fill(holeLength)(None))
			val newCommittedRecords = mem.getRecordsBetween(indexOfFirstRecordToAppend, current + 1)
			thisNodeCommitted.addAll(newCommittedRecords)
		}

		if as == LEADER then {
			// Check that records committed in the past by other nodes are present in the leader's log
			for (otherId, otherCommitted) <- committedRecordsByNode if otherId != node.myId do {
				for (otherRec, recordIndexBase0) <- otherCommitted.zipWithIndex do {
					otherRec match {
						case None => ()
						case otherNodeCommittedRecord: Record =>
							if recordIndexBase0 < thisNodeCommitted.size then {
								thisNodeCommitted(recordIndexBase0) match {
									case None => ()
									case leaderCommittedRecord: Record =>
										if leaderCommittedRecord != otherNodeCommittedRecord then {
											throw new AssertionError(s"Node $otherId has a committed record at index ${recordIndexBase0 + 1} that differs from the record of current leader ${node.myId}, breaking Leader Completeness: $otherId -> $otherNodeCommittedRecord; ${node.myId} -> $leaderCommittedRecord")
										}
								}
							} else if otherNodeCommittedRecord.term <= at then {
								throw new AssertionError(s"Node $otherId has more committed records with term <= $at than current leader ${node.myId}, breaking Leader Completeness.")
							}
					}
				}
			}
		}
	}

	private[readren] def onCommandApplied(node: EnvironmentNode, command: TestClientCommand, index: RecordIndex): Unit = {
		appliedCommandsByIndex.get(index) match {
			case Some((prevNode, prevCmd)) =>
				if prevCmd != command then {
					throw new AssertionError(s"State Machine Safety invariant violated at index $index: Node ${node.myId} applied $command, but $prevNode applied $prevCmd")
				}
			case None =>
				appliedCommandsByIndex(index) = (node.myId, command)
		}
	}

	private[readren] def onActiveConfigChanged(node: EnvironmentNode, change: ConfigChange[NodeId], changeIndex: RecordIndex, roleOrdinal: RoleOrdinal): Unit = ()

	private[readren] def onNodeQuiesced(node: EnvironmentNode, motive: Try[String]): Unit = {
		onNodeQuiescedHook(node, motive)
	}

	def checkLogMatching(): Unit = {
		val activeNodes = nodesMap.values.filterNot(_.isDown).toSeq
		for i <- activeNodes.indices do {
			for j <- (i + 1) until activeNodes.size do {
				val nodeA = activeNodes(i)
				val nodeB = activeNodes(j)
				val memA = nodeA.storage.savedMemory
				val memB = nodeB.storage.savedMemory
				val start = math.max(memA.logBufferOffset, memB.logBufferOffset)
				val end = math.min(memA.firstEmptyRecordIndex, memB.firstEmptyRecordIndex)
				var lastSameTermIdx = end - 1
				while lastSameTermIdx >= start && memA.getRecordAt(lastSameTermIdx).term != memB.getRecordAt(lastSameTermIdx).term do {
					lastSameTermIdx -= 1
				}
				for idx <- start to lastSameTermIdx do {
					val recA = memA.getRecordAt(idx)
					val recB = memB.getRecordAt(idx)
					if recA != recB then {
						throw new AssertionError(s"Log Matching invariant violated at index $idx: Node ${nodeA.myId} has $recA, but Node ${nodeB.myId} has $recB")
					}
				}
			}
		}
	}

	def allNodes: Seq[EnvironmentNode] = (0 until clusterSize).map(i => node(i))

	def allChannels: Seq[((NodeId, NodeId), Seq[Packet])] = channels.map((k, v) => (k, v.toSeq)).toSeq

	def allClientStatuses: Map[Int, ClientCommandStatus] = clientStatuses.toMap

	def allClientStats: Map[String, ClientStats] = {
		val allIds = clientSerialCounters.keySet ++ clientLastSent.keySet ++ clientLastSuccess.keySet ++ clientLastRecordIndex.keySet
		allIds.map(id => id -> ClientStats(clientLastSent.get(id), clientLastSuccess.get(id), clientLastRecordIndex.get(id))).toMap
	}

	def allConfigStatuses: Map[String, ConfigChangeStatus] = configStatuses.toMap

	def leaderByTermMap: Map[Term, NodeId] = leaderByTerm.toMap
}
