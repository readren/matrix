package readren.consensus.env

import readren.common.Maybe
import readren.consensus.ConsensusParticipantSdm
import readren.consensus.ConsensusParticipantSdm.*
import readren.consensus.TestClientCommand
import readren.consensus.protocol.*
import readren.sequencer.Doer

import scala.collection.immutable.ListSet
import scala.collection.mutable
import scala.compiletime.uninitialized
import scala.util.Try

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

	override def retiringParticipantMaxRetriesUnderEmptyElectorate: Int = env.retiringParticipantMaxRetries

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

	def startIfNotRunning(): Unit = {
		stepDoer.executeSequentially(() => {
			if _participant == null || _participant.getRoleOrdinal == QUIESCED then {
				val maybeSeeds = if initialParticipants.contains(myId) then Maybe(initialParticipants) else Maybe.empty
				_participant = new ConsensusParticipant(
					clusterParticipant,
					storage,
					machine,
					maybeSeeds,
					List(notificationListener)
				)
				isDown = false
			}
		})
		if !stepDoer.isInSequence then stepDoer.walk()
	}

	def release(): Unit = {
		stepDoer.executeSequentially(() => {
			_participant = null
			isDown = true
		})
		if !stepDoer.isInSequence then stepDoer.walk()
	}

	class TestStateMachine extends StateMachine {
		var highestAppliedCommandSerial: Int = 0
		var highestAppliedCommandIndex: RecordIndex = 0

		override def applyClientCommand(index: RecordIndex, command: ClientCommand): sequencer.Capture[StateMachineResponse] = {
			sequencer.checkWithin()
			if index > highestAppliedCommandIndex then highestAppliedCommandIndex = index
			if command.serial > highestAppliedCommandSerial then highestAppliedCommandSerial = command.serial
			env.oracle.onCommandApplied(thisNode, command, index)
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
				env.oracle.onLogTruncated(thisNode, fromIndex, firstRemoved)
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
				env.oracle.onStorageSaved(thisNode)
				sequencer.Capture_ready(Doer.successUnit)
			} else {
				val opId = env.storage.nextStorageOpId()
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
						env.oracle.onStorageSaved(thisNode)
						stepDoer.executeSequentially(() => captor.captureSync(()))
					},
					failOp = err => {
						stepDoer.executeSequentially(() => captor.trapSync(err))
					}
				)
				env.storage.registerPendingPersistence(pending)
				captor
			}
		}
	}

	class TestClusterParticipant extends ClusterParticipant {
		override val boundParticipantId: ParticipantId = myId
		var delegate: Delegate = uninitialized

		override def getOtherProbableParticipants: ListSet[ParticipantId] = ListSet.from(initialParticipants - myId)

		override def setBound(delegate: Delegate): Unit = {
			this.delegate = delegate
		}

		override def removeBound(): Unit = {
			this.delegate = null
		}

		override def onActiveElectorateChanged(change: ElectorateChange[ParticipantId], changeIndex: RecordIndex, roleOrdinal: RoleOrdinal): Unit = {
			env.onActiveElectorateChanged(thisNode, change, changeIndex, roleOrdinal)
		}

		override def onQuiesced(motive: Try[String]): Unit = {
			env.onNodeQuiesced(thisNode, motive)
		}

		override def requestWakeUp(reason: WakeUpReason, wakeupsDone: Int, callback: () => Unit): WakeUpToken = {
			env.clock.registerWakeUp(thisNode, reason, wakeupsDone, callback)
		}

		extension (destinationId: ParticipantId) {
			override def howAreYou(inquirerInfo: StateInfo): sequencer.Capture[StateInfo] = {
				sequencer.checkWithin()
				val captor = sequencer.Captor[StateInfo]()
				val req = RequestPacket(
					id = env.network.nextPacketId(),
					source = myId,
					destination = destinationId,
					departureTime = env.clock.currentVirtualTime,
					rpc = ConsensusRpc.HowAreYou(inquirerInfo),
					completeCaller = res => stepDoer.executeSequentially(() => res.fold(e => captor.trapSync(e), v => captor.captureSync(v.asInstanceOf[StateInfo])))
				)
				env.network.enqueuePacket(req)
				captor
			}

			override def requestBallot(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.Capture[Ballot] = {
				sequencer.checkWithin()
				val captor = sequencer.Captor[Ballot]()
				val req = RequestPacket(
					id = env.network.nextPacketId(),
					source = myId,
					destination = destinationId,
					departureTime = env.clock.currentVirtualTime,
					rpc = ConsensusRpc.RequestBallot(inquirerId, inquirerInfo),
					completeCaller = res => stepDoer.executeSequentially(() => res.fold(e => captor.trapSync(e), v => captor.captureSync(v.asInstanceOf[Ballot])))
				)
				env.network.enqueuePacket(req)
				captor
			}

			override def appendRecords(inquirerTerm: Term, prevLogIndex: RecordIndex, prevLogTerm: Term, batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.Capture[AppendResult] = {
				sequencer.checkWithin()
				val captor = sequencer.Captor[AppendResult]()
				val req = RequestPacket(
					id = env.network.nextPacketId(),
					source = myId,
					destination = destinationId,
					departureTime = env.clock.currentVirtualTime,
					rpc = ConsensusRpc.AppendRecords(inquirerTerm, prevLogIndex, prevLogTerm, batch, leaderCommit, termAtLeaderCommit),
					completeCaller = res => stepDoer.executeSequentially(() => res.fold(e => captor.trapSync(e), v => captor.captureSync(v.asInstanceOf[AppendResult])))
				)
				env.network.enqueuePacket(req)
				captor
			}

			override def installSnapshot(inquirerTerm: Term, snapshot: SnapshotData[ParticipantId], batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.Capture[AppendResult] = {
				sequencer.checkWithin()
				val captor = sequencer.Captor[AppendResult]()
				val req = RequestPacket(
					id = env.network.nextPacketId(),
					source = myId,
					destination = destinationId,
					departureTime = env.clock.currentVirtualTime,
					rpc = ConsensusRpc.InstallSnapshot(inquirerTerm, snapshot, batch, leaderCommit, termAtLeaderCommit),
					completeCaller = res => stepDoer.executeSequentially(() => res.fold(e => captor.trapSync(e), v => captor.captureSync(v.asInstanceOf[AppendResult])))
				)
				env.network.enqueuePacket(req)
				captor
			}

			override def permitQuiescence(indexOfGrantedSec: RecordIndex): sequencer.Capture[Unit] = {
				sequencer.checkWithin()
				val captor = sequencer.Captor[Unit]()
				val req = RequestPacket(
					id = env.network.nextPacketId(),
					source = myId,
					destination = destinationId,
					departureTime = env.clock.currentVirtualTime,
					rpc = ConsensusRpc.PermitQuiescence(indexOfGrantedSec),
					completeCaller = res => stepDoer.executeSequentially(() => res.fold(e => captor.trapSync(e), _ => captor.captureSync(())))
				)
				env.network.enqueuePacket(req)
				captor
			}
		}
	}

	object notificationListener extends NotificationListener {
		override def onStarting(previous: RoleOrdinal): Unit = ()

		override def onStarted(previous: RoleOrdinal, term: Term, initialElectorateChange: ElectorateChange[ParticipantId], isSeed: Boolean): Unit = ()

		override def onBecameQuiesced(previous: RoleOrdinal, term: Term, motive: Try[String]): Unit = ()

		override def onJoining(previous: RoleOrdinal): Unit = ()

		override def onBecameIsolated(previous: RoleOrdinal, term: Term): Unit = ()

		override def onBecameCandidate(previous: RoleOrdinal, term: Term): Unit = ()

		override def onBecameFollower(previous: RoleOrdinal, term: Term, leaderId: ParticipantId): Unit = ()

		override def onBecameLeader(previous: RoleOrdinal, term: Term): Unit = {
			env.oracle.onBecameLeader(thisNode, term)
		}

		override def onAbdicating(term: Term): Unit = ()

		override def onRetiring(previous: RoleOrdinal, term: Term): Unit = ()

		override def onRoleLeft(left: RoleOrdinal, term: Term): Unit = ()

		override def onCommitIndexChanged(previous: RecordIndex, current: RecordIndex, as: RoleOrdinal, at: Term): Unit = {
			env.oracle.onCommitIndexChanged(thisNode, previous, current, as, at)
		}

		override def onCommandApplied(appliedCommandIndex: RecordIndex, appliedCommandTerm: Term): Unit = ()

		override def onActiveElectorateChanged(currentRole: RoleOrdinal, currentTerm: Term, electorateChangeIndex: RecordIndex, electorateChange: ElectorateChange[ParticipantId]): Unit = ()
	}
}
