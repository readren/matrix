package readren.consensus

import ConsensusParticipantSdm.*
import readren.common.{Maybe, Trial}
import readren.sequencer.Doer

import scala.collection.immutable.{ListMap, ListSet}
import scala.collection.mutable
import scala.compiletime.uninitialized
import scala.util.{Failure, Success, Try}

/** Ergonomic reference to a node/participant, supporting either String ("p-0") or Int (0 -> "p-0"). */
type NodeRef = String | Int

extension (ref: NodeRef) {
	def asNodeId: String = ref match {
		case s: String => if s.forall(_.isDigit) then s"p-$s" else s
		case i: Int => s"p-$i"
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

/** Metadata about a log record contained in an in-transit packet. */
final case class PacketRecordInfo(
	index: RecordIndex,
	term: Term,
	kind: String,
	summary: String
)

/** A packet traveling between two nodes across the network. */
sealed trait Packet {
	def id: PacketId

	def source: String

	def destination: String

	def departureTime: VirtualTime

	def rpcKind: String

	def summary: String
}

/** A packet representing an outbound RPC request from source to destination. */
final case class RequestPacket(
	id: PacketId,
	source: String,
	destination: String,
	departureTime: VirtualTime,
	rpcKind: String,
	executeOn: (EnvironmentNode, Any => Unit, Throwable => Unit) => Unit,
	completeCaller: Try[Any] => Unit,
	summary: String,
	records: IArray[PacketRecordInfo] = IArray.empty
) extends Packet

/** A packet representing the outbound response to an earlier RPC request. */
final case class ResponsePacket(
	id: PacketId,
	source: String,
	destination: String,
	departureTime: VirtualTime,
	correlationRequestId: PacketId,
	rpcKind: String,
	response: Try[Any],
	completeCaller: Try[Any] => Unit,
	summary: String
) extends Packet

/** The outcome of a packet dispatch operation. */
final case class DispatchOutcome(
	packetId: PacketId,
	source: String,
	destination: String,
	destinationHadPendingRunnables: Boolean,
	warning: Option[String]
)

/** A pending wake-up registered via [[ClusterParticipant.requestWakeUp]]. */
final case class PendingWakeUp(
	tokenId: Int,
	token: WakeUpToken,
	nodeId: String,
	reason: WakeUpReason,
	wakeupsDone: Int,
	scheduledTime: VirtualTime,
	callback: () => Unit
)

/** A pending persistence operation waiting for explicit completion or failure. */
final case class PendingPersistence(
	opId: StorageOpId,
	nodeId: String,
	term: Term,
	logBufferOffset: RecordIndex,
	firstEmptyRecordIndex: RecordIndex,
	records: IArray[Record],
	snapshot: Maybe[SnapshotData[String]],
	completeOp: () => Unit,
	failOp: Throwable => Unit
)

/** Status of an injected client command. */
sealed trait ClientCommandStatus

object ClientCommandStatus {
	case object InFlight extends ClientCommandStatus

	final case class Processed(recordIndex: RecordIndex, response: Int) extends ClientCommandStatus

	final case class Redirected(leaderId: String) extends ClientCommandStatus

	final case class Unable(nextAttemptFlag: CommandAttemptFlag, otherParticipants: Set[String]) extends ClientCommandStatus

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
	targetNodeId: String,
	submissionTime: VirtualTime
)

/** A handle to an in-flight or completed configuration change request. */
final case class ConfigChangeHandle(
	requestId: String,
	targetNodeId: String,
	desiredParticipants: Set[String],
	submissionTime: VirtualTime
)

/** A node instance managed by [[ConsensusEnvironment]]. */
class EnvironmentNode(
	val myId: String,
	val initialParticipants: ListSet[String],
	val env: ConsensusEnvironment
) extends ConsensusParticipantSdm { thisNode =>

	override type ParticipantId = String
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
			sequencer.Keeper(highestAppliedCommandIndex)
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
		val logBuffer: mutable.ArrayBuffer[Record] = mutable.ArrayBuffer.empty
		var _logBufferOffset: RecordIndex = 1
		var maybeLatestSnapshot: Maybe[SnapshotData[ParticipantId]] = Maybe.empty

		override def getCurrentTerm: Term = currentTerm

		override def setCurrentTerm(term: Term): Unit = {
			currentTerm = term
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
					rpcKind = "HAY",
					executeOn = (target, onSuccess, onError) => {
						target.clusterParticipant.delegate.onHowAreYou(myId, inquirerInfo).triggerSyncCallbacks(onSuccess, onError)
					},
					completeCaller = res => stepDoer.executeSequentially(() => res.fold(e => captor.trapSync(e), v => captor.captureSync(v.asInstanceOf[StateInfo]))),
					summary = s"howAreYou($inquirerInfo)"
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
					rpcKind = "CAL",
					executeOn = (target, onSuccess, onError) => {
						target.clusterParticipant.delegate.onChooseALeader(inquirerId, inquirerInfo).triggerSyncCallbacks(onSuccess, onError)
					},
					completeCaller = res => stepDoer.executeSequentially(() => res.fold(e => captor.trapSync(e), v => captor.captureSync(v.asInstanceOf[Vote[ParticipantId]]))),
					summary = s"chooseALeader(inquirerId=$inquirerId, term=${inquirerInfo.currentTerm})"
				)
				env.enqueuePacket(req)
				captor
			}

			override def appendRecords(inquirerTerm: Term, prevLogIndex: RecordIndex, prevLogTerm: Term, batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.Capture[AppendResult] = {
				sequencer.checkWithin()
				val captor = sequencer.Captor[AppendResult]()
				val recordInfos: IArray[PacketRecordInfo] = IArray.tabulate(batch.length) { i =>
					val rec = batch(i)
					val idx = prevLogIndex + 1L + i
					val kind = rec match {
						case _: CommandRecord[?] => "Cmd"
						case _: LeaderTransition => "LT"
						case _: TransitionalConfigChange[?] => "TCC"
						case _: StableConfigChange[?] => "SCC"
					}
					val summary = rec match {
						case cmd: CommandRecord[?] => cmd.command match {
							case tc: TestClientCommand => s"#${tc.serial} from ${tc.clientId}"
							case other => s"$other"
						}
						case tcc: TransitionalConfigChange[?] =>
							val oldStr = tcc.oldParticipants.toSeq.map(_.toString).sorted.mkString(", ")
							val newStr = tcc.newParticipants.toSeq.map(_.toString).sorted.mkString(", ")
							s"{$oldStr} -> {$newStr}"
						case scc: StableConfigChange[?] =>
							val newStr = scc.newParticipants.toSeq.map(_.toString).sorted.mkString(", ")
							s"{$newStr}"
						case lt: LeaderTransition => s"term=${lt.term}"
					}
					PacketRecordInfo(idx, rec.term, kind, summary)
				}
				val req = RequestPacket(
					id = env.nextPacketId(),
					source = myId,
					destination = destinationId,
					departureTime = env.currentVirtualTime,
					rpcKind = "APR",
					executeOn = (target, onSuccess, onError) => {
						target.clusterParticipant.delegate.onAppendRecords(myId, inquirerTerm, prevLogIndex, prevLogTerm, batch, leaderCommit, termAtLeaderCommit).triggerSyncCallbacks(onSuccess, onError)
					},
					completeCaller = res => stepDoer.executeSequentially(() => res.fold(e => captor.trapSync(e), v => captor.captureSync(v.asInstanceOf[AppendResult]))),
					summary = s"appendRecords(term=$inquirerTerm, prevIdx=$prevLogIndex, commit=$leaderCommit)",
					records = recordInfos
				)
				env.enqueuePacket(req)
				captor
			}

			override def installSnapshot(inquirerTerm: Term, snapshot: SnapshotData[ParticipantId], batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.Capture[AppendResult] = {
				sequencer.checkWithin()
				val captor = sequencer.Captor[AppendResult]()
				val recordInfos: IArray[PacketRecordInfo] = IArray.tabulate(batch.length) { i =>
					val rec = batch(i)
					val idx = snapshot.lastIncludedRecordIndex + 1L + i
					val kind = rec match {
						case _: CommandRecord[?] => "Cmd"
						case _: LeaderTransition => "LT"
						case _: TransitionalConfigChange[?] => "TCC"
						case _: StableConfigChange[?] => "SCC"
					}
					val summary = rec match {
						case cmd: CommandRecord[?] => cmd.command match {
							case tc: TestClientCommand => s"#${tc.serial} from ${tc.clientId}"
							case other => s"$other"
						}
						case tcc: TransitionalConfigChange[?] =>
							val oldStr = tcc.oldParticipants.toSeq.map(_.toString).sorted.mkString(", ")
							val newStr = tcc.newParticipants.toSeq.map(_.toString).sorted.mkString(", ")
							s"{$oldStr} -> {$newStr}"
						case scc: StableConfigChange[?] =>
							val newStr = scc.newParticipants.toSeq.map(_.toString).sorted.mkString(", ")
							s"{$newStr}"
						case lt: LeaderTransition => s"term=${lt.term}"
					}
					PacketRecordInfo(idx, rec.term, kind, summary)
				}
				val req = RequestPacket(
					id = env.nextPacketId(),
					source = myId,
					destination = destinationId,
					departureTime = env.currentVirtualTime,
					rpcKind = "SNP",
					executeOn = (target, onSuccess, onError) => {
						target.clusterParticipant.delegate.onInstallSnapshot(myId, inquirerTerm, snapshot, batch, leaderCommit, termAtLeaderCommit).triggerSyncCallbacks(onSuccess, onError)
					},
					completeCaller = res => stepDoer.executeSequentially(() => res.fold(e => captor.trapSync(e), v => captor.captureSync(v.asInstanceOf[AppendResult]))),
					summary = s"installSnapshot(term=$inquirerTerm, lastIncludedIdx=${snapshot.lastIncludedRecordIndex})",
					records = recordInfos
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
					rpcKind = "QUI",
					executeOn = (target, onSuccess, onError) => {
						target.clusterParticipant.delegate.onQuiescencePermitted(myId, indexOfGrantedStableConfigChange)
						onSuccess(())
					},
					completeCaller = res => stepDoer.executeSequentially(() => res.fold(e => captor.trapSync(e), _ => captor.captureSync(()))),
					summary = s"permitQuiescence(configIdx=$indexOfGrantedStableConfigChange)"
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
			env.onCommitIndexChanged(thisNode, current, as, at)
		}

		override def onCommandApplied(appliedCommandIndex: RecordIndex, appliedCommandTerm: Term): Unit = ()

		override def onActiveConfigChanged(currentRole: RoleOrdinal, currentTerm: Term, configChangeIndex: RecordIndex, configChange: ConfigChange[ParticipantId]): Unit = ()
	}
}

/** The discrete-event, fine-grained testing harness for [[ConsensusParticipantSdm]]. */
class ConsensusEnvironment(
	val clusterSize: Int = 3,
	val initialSeedParticipants: Option[Set[String]] = None,
	val ticksPerMilli: Int = 10,
	val maxInFlightAppendsPerPeer: Int = 2,
	val logCompactionThreshold: Int = 5,
	var retiringParticipantMaxRetries: Int = 2,
	var logRetentionAfterSnapshot: Int = 1
) {
	private var _virtualTime: VirtualTime = 0
	private var packetIdSequencer: PacketId = 0
	private var storageOpIdSequencer: StorageOpId = 0
	private var commandIdSequencer: Int = 0
	private var configReqIdSequencer: Int = 0
	private var wakeUpTokenSequencer: Int = 0

	private val channels: mutable.Map[(String, String), mutable.ArrayDeque[Packet]] = mutable.Map.empty
	private val nodesMap: mutable.Map[String, EnvironmentNode] = mutable.Map.empty
	private val pendingWakeUpsMap: mutable.Map[Int, PendingWakeUp] = mutable.Map.empty
	private val pendingPersistenceMap: mutable.Map[StorageOpId, PendingPersistence] = mutable.Map.empty
	private val clientStatuses: mutable.Map[Int, ClientCommandStatus] = mutable.Map.empty
	private val clientSerialCounters: mutable.Map[String, Int] = mutable.Map.empty.withDefaultValue(0)
	private val clientLastSent: mutable.Map[String, Int] = mutable.Map.empty
	private val clientLastSuccess: mutable.Map[String, Int] = mutable.Map.empty
	private val clientLastRecordIndex: mutable.Map[String, Long] = mutable.Map.empty
	private val configStatuses: mutable.Map[String, ConfigChangeStatus] = mutable.Map.empty

	// Invariant Tracking
	private val leaderByTerm: mutable.Map[Term, String] = mutable.Map.empty
	private val appliedCommandsByIndex: mutable.Map[RecordIndex, (String, TestClientCommand)] = mutable.Map.empty
	private val committedRecordsByNode: mutable.Map[String, mutable.ArrayBuffer[Record]] = mutable.Map.empty

	val defaultInitialParticipants: ListSet[String] = initialSeedParticipants match {
		case Some(seeds) => ListSet.from(seeds.map(_.asNodeId))
		case None => ListSet.from((0 until clusterSize).map(i => s"p-$i"))
	}

	for i <- 0 until clusterSize do {
		val id = s"p-$i"
		nodesMap(id) = new EnvironmentNode(id, defaultInitialParticipants, this)
		committedRecordsByNode(id) = mutable.ArrayBuffer.empty
	}

	inline def currentVirtualTime: VirtualTime = _virtualTime

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

	private def channelQueue(from: String, to: String): mutable.ArrayDeque[Packet] = {
		channels.getOrElseUpdate((from, to), mutable.ArrayDeque.empty)
	}

	private[readren] def enqueuePacket(packet: Packet): Unit = {
		channelQueue(packet.source, packet.destination).append(packet)
	}

	// Node Management
	def node(ref: NodeRef): EnvironmentNode = nodesMap(ref.asNodeId)

	def startNode(ref: NodeRef): Unit = {
		val n = node(ref)
		n.startIfNotRunning(0, defaultInitialParticipants)
	}

	def startAllNodes(): Unit = {
		for id <- defaultInitialParticipants do startNode(id)
	}

	def crashNode(ref: NodeRef): Unit = {
		val n = node(ref)
		n.isDown = true
		n.stepDoer.clear()
	}

	def restartNode(ref: NodeRef): Unit = {
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
	def stepNode(ref: NodeRef): Boolean = {
		node(ref).stepDoer.step()
	}

	def stepAllNodes(): Int = {
		var stepped = 0
		for n <- nodesMap.values do {
			if n.stepDoer.step() then stepped += 1
		}
		stepped
	}

	def runNodeUntilIdle(ref: NodeRef, maxSteps: Int = 1000): Int = {
		node(ref).stepDoer.drain(maxSteps)
	}

	def runAllNodesUntilIdle(maxRounds: Int = 1000): Int = {
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

	def dispatchPacket(packetId: PacketId): DispatchOutcome = {
		val targetQueueOpt = channels.find(_._2.exists(_.id == packetId))
		targetQueueOpt match {
			case None => throw new NoSuchElementException(s"Packet $packetId not found in any channel")
			case Some((fromTo, queue)) =>
				val idx = queue.indexWhere(_.id == packetId)
				val packet = queue.remove(idx)
				executeDispatch(packet)
		}
	}

	def dispatchNext(from: NodeRef, to: NodeRef): Option[DispatchOutcome] = {
		val queue = channelQueue(from.asNodeId, to.asNodeId)
		if queue.isEmpty then None
		else Some(executeDispatch(queue.removeHead()))
	}

	def dispatchFirstN(from: NodeRef, to: NodeRef, n: Int): Seq[DispatchOutcome] = {
		val queue = channelQueue(from.asNodeId, to.asNodeId)
		val count = math.min(n, queue.size)
		(0 until count).map(_ => executeDispatch(queue.removeHead()))
	}

	def dispatchAllBetween(from: NodeRef, to: NodeRef): Seq[DispatchOutcome] = {
		val queue = channelQueue(from.asNodeId, to.asNodeId)
		val res = mutable.ArrayBuffer.empty[DispatchOutcome]
		while queue.nonEmpty do {
			res.append(executeDispatch(queue.removeHead()))
		}
		res.toSeq
	}

	def dispatchAllTo(to: NodeRef): Seq[DispatchOutcome] = {
		val toId = to.asNodeId
		val res = mutable.ArrayBuffer.empty[DispatchOutcome]
		for ((f, t), q) <- channels if t == toId do {
			while q.nonEmpty do res.append(executeDispatch(q.removeHead()))
		}
		res.toSeq
	}

	def dispatchAll(): Seq[DispatchOutcome] = {
		val all = pendingPackets
		all.map(p => dispatchPacket(p.id))
	}

	def dropPacket(packetId: PacketId): Boolean = {
		failPacket(packetId, new java.io.IOException(s"Network packet $packetId dropped in transit"))
	}

	def dropNext(from: NodeRef, to: NodeRef): Boolean = {
		dropFirstN(from, to, 1) > 0
	}

	def dropFirstN(from: NodeRef, to: NodeRef, n: Int): Int = {
		val q = channelQueue(from.asNodeId, to.asNodeId)
		val count = math.min(n, q.size)
		for _ <- 0 until count do {
			val packet = q.removeHead()
			val error = new java.io.IOException(s"Network packet ${packet.id} dropped in transit")
			packet match {
				case req: RequestPacket => req.completeCaller(Failure(error))
				case resp: ResponsePacket => resp.completeCaller(Failure(error))
			}
		}
		count
	}

	def dropAllBetween(from: NodeRef, to: NodeRef): Int = {
		val q = channelQueue(from.asNodeId, to.asNodeId)
		dropFirstN(from, to, q.size)
	}


	def failPacket(packetId: PacketId, error: Throwable): Boolean = {
		channels.values.find(_.exists(_.id == packetId)).fold(false) { q =>
			val idx = q.indexWhere(_.id == packetId)
			val packet = q.remove(idx)
			packet match {
				case req: RequestPacket => req.completeCaller(Failure(error))
				case resp: ResponsePacket => resp.completeCaller(Failure(error))
			}
			true
		}
	}

	private def executeDispatch(packet: Packet): DispatchOutcome = {
		val destNode = nodesMap.getOrElse(packet.destination, throw new IllegalStateException(s"Destination node ${packet.destination} does not exist"))
		val hadPending = destNode.stepDoer.hasPendingTasks
		val warning = if hadPending then Some(s"Destination node ${packet.destination} had ${destNode.stepDoer.pendingTasksCount} pending task(s) when packet ${packet.id} was dispatched.") else None

		packet match {
			case req: RequestPacket =>
				destNode.stepDoer.executeSequentially(() => {
					if destNode.isDown || (destNode.participant eq null) then {
						val resp = ResponsePacket(
							id = nextPacketId(),
							source = req.destination,
							destination = req.source,
							departureTime = _virtualTime,
							correlationRequestId = req.id,
							rpcKind = req.rpcKind,
							response = Failure(new RuntimeException(s"Node ${destNode.myId} is down")),
							completeCaller = req.completeCaller,
							summary = s"Node ${destNode.myId} is down"
						)
						enqueuePacket(resp)
					} else {
						req.executeOn(
							destNode,
							result => {
								val resp = ResponsePacket(
									id = nextPacketId(),
									source = req.destination,
									destination = req.source,
									departureTime = _virtualTime,
									correlationRequestId = req.id,
									rpcKind = req.rpcKind,
									response = Success(result),
									completeCaller = req.completeCaller,
									summary = s"Response($result)"
								)
								enqueuePacket(resp)
							},
							ex => {
								val resp = ResponsePacket(
									id = nextPacketId(),
									source = req.destination,
									destination = req.source,
									departureTime = _virtualTime,
									correlationRequestId = req.id,
									rpcKind = req.rpcKind,
									response = Failure(ex),
									completeCaller = req.completeCaller,
									summary = s"Failure($ex)"
								)
								enqueuePacket(resp)
							}
						)
					}
				})

			case resp: ResponsePacket =>
				resp.completeCaller(resp.response)
		}

		DispatchOutcome(packet.id, packet.source, packet.destination, hadPending, warning)
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

	def advanceTime(ticks: Int): Seq[PendingWakeUp] = {
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

	def triggerWakeUp(tokenId: Int): Boolean = {
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

	def completeStorageSave(opId: StorageOpId): Boolean = {
		pendingPersistenceMap.remove(opId).fold(false) { p =>
			p.completeOp()
			true
		}
	}

	def completeNextStorageSave(nodeRef: NodeRef): Boolean = {
		val nodeId = nodeRef.asNodeId
		pendingPersistenceOperations.find(_.nodeId == nodeId).fold(false) { p =>
			completeStorageSave(p.opId)
		}
	}

	def completeAllStorageSaves(nodeRef: NodeRef): Int = {
		val nodeId = nodeRef.asNodeId
		val matching = pendingPersistenceOperations.filter(_.nodeId == nodeId)
		for p <- matching do completeStorageSave(p.opId)
		matching.size
	}

	def failStorageSave(opId: StorageOpId, error: Throwable): Boolean = {
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
		val cmdSerial = serial.getOrElse {
			val s = clientSerialCounters(clientId) + 1
			clientSerialCounters(clientId) = s
			s
		}
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
		desiredParticipants: Set[NodeRef],
		priorAnswer: Maybe[ConfigChangeResponse] = Maybe.empty
	): ConfigChangeHandle = {
		val targetId = targetNode.asNodeId
		val desiredIds = desiredParticipants.map(_.asNodeId)
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

	private[readren] def onCommitIndexChanged(node: EnvironmentNode, commitIndex: RecordIndex, as: RoleOrdinal, at: Term): Unit = {
		val nodeCommitted = committedRecordsByNode(node.myId)
		val mem = node.storage.savedMemory
		val offset = mem.logBufferOffset
		val startIdx = (nodeCommitted.size + 1L).max(offset)
		if startIdx <= commitIndex then {
			val newRecords = mem.getRecordsBetween(startIdx, commitIndex + 1)
			nodeCommitted.addAll(newRecords)
		}

		if as == LEADER then {
			// Leader Completeness: all records committed by any peer in term <= at must match leader log
			for (otherId, otherCommitted) <- committedRecordsByNode if otherId != node.myId do {
				for (otherRec, idxBase0) <- otherCommitted.zipWithIndex do {
					val recordIndex = idxBase0 + 1L
					if otherRec.term <= at then {
						if recordIndex <= nodeCommitted.size then {
							val leaderRec = nodeCommitted(idxBase0)
							if leaderRec != otherRec then {
								throw new AssertionError(s"Leader Completeness invariant violated at index $recordIndex: Leader ${node.myId} has $leaderRec, but $otherId committed $otherRec in term <= $at")
							}
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

	private[readren] def onActiveConfigChanged(node: EnvironmentNode, change: ConfigChange[String], changeIndex: RecordIndex, roleOrdinal: RoleOrdinal): Unit = ()

	private[readren] def onNodeQuiesced(node: EnvironmentNode, motive: Try[String]): Unit = ()

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

	def allChannels: Seq[((String, String), Seq[Packet])] = channels.map((k, v) => (k, v.toSeq)).toSeq

	def allClientStatuses: Map[Int, ClientCommandStatus] = clientStatuses.toMap

	def allClientStats: Map[String, ClientStats] = {
		val allIds = clientSerialCounters.keySet ++ clientLastSent.keySet ++ clientLastSuccess.keySet ++ clientLastRecordIndex.keySet
		allIds.map(id => id -> ClientStats(clientLastSent.get(id), clientLastSuccess.get(id), clientLastRecordIndex.get(id))).toMap
	}

	def allConfigStatuses: Map[String, ConfigChangeStatus] = configStatuses.toMap

	def leaderByTermMap: Map[Term, String] = leaderByTerm.toMap
}
