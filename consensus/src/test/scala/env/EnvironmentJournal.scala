package readren.consensus.env

import readren.consensus.ConsensusParticipantSdm.*
import readren.consensus.protocol.*

import scala.collection.mutable
import scala.util.control.NonFatal

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

	final case class TriggerWakeUp(tokenId: WakeUpTokenId) extends EnvOperation

	final case class CompleteStorageSave(opId: StorageOpId) extends EnvOperation

	final case class CompleteNextStorageSave(node: NodeId) extends EnvOperation

	final case class CompleteAllStorageSaves(node: NodeId) extends EnvOperation

	final case class FailStorageSave(opId: StorageOpId, errorMsg: String) extends EnvOperation

	final case class StartNode(node: NodeId) extends EnvOperation

	final case class CrashNode(node: NodeId) extends EnvOperation

	final case class RestartNode(node: NodeId) extends EnvOperation

	case object StartAllNodes extends EnvOperation

	final case class SubmitClientCommand(targetNode: NodeId, client: String, serial: Option[Int], attemptFlag: CommandAttemptFlag) extends EnvOperation

	final case class SubmitElectorateChange(targetNode: NodeId, desiredParticipants: Set[NodeId]) extends EnvOperation

	final case class UpdateDynamicSettings(
		retiringMaxRetries: Option[Int],
		logRetention: Option[Int],
		autoSucceedUntilTerm: Option[Term],
		autoSucceedUntilRecordIndex: Option[RecordIndex],
		targetNode: Option[NodeId]
	) extends EnvOperation

	final case class ToggleStorageAutoSucceed(targetNode: Option[NodeId]) extends EnvOperation
}

/** A checkpoint snapshot capturing applied operations and undone (redo) operations. */
final case class CheckpointTag(
	appliedOperations: List[EnvOperation],
	undoneOperations: List[EnvOperation]
) {
	def size: Int = appliedOperations.size
}

/** Deterministic operation recording, checkpoint tagging, and undo/redo replay engine for [[ConsensusEnvironment]]. */
class EnvironmentJournal(env: ConsensusEnvironment) {
	private val _appliedOperations: mutable.ArrayBuffer[EnvOperation] = mutable.ArrayBuffer.empty
	private val _undoneOperations: mutable.ArrayBuffer[EnvOperation] = mutable.ArrayBuffer.empty
	private val _tags: mutable.Map[String, CheckpointTag] = mutable.Map.empty
	private var isReplaying: Boolean = false
	private var operationDepth: Int = 0

	private[env] def recordOrExecute[T](op: => EnvOperation)(action: => T): T = {
		if !isReplaying && operationDepth == 0 then {
			_appliedOperations.append(op)
			_undoneOperations.clear()
		}
		operationDepth += 1
		try action
		finally operationDepth -= 1
	}

	private[env] def withReplaySuppression[T](action: => T): T = {
		val prevReplaying = isReplaying
		isReplaying = true
		try action
		finally isReplaying = prevReplaying
	}

	def appliedOperations: Seq[EnvOperation] = _appliedOperations.toSeq

	def appliedOperationsCount: Int = _appliedOperations.size

	def undoneOperations: Seq[EnvOperation] = _undoneOperations.toSeq

	def undoneOperationsCount: Int = _undoneOperations.size

	def tags: Map[String, CheckpointTag] = _tags.toMap

	def createTag(name: String): Unit = {
		_tags(name) = CheckpointTag(_appliedOperations.toList, _undoneOperations.toList)
	}

	def restoreTag(name: String): Boolean = {
		_tags.get(name) match {
			case None => false
			case Some(tag) =>
				env.reset()
				_appliedOperations.clear()
				_appliedOperations.addAll(tag.appliedOperations)
				_undoneOperations.clear()
				_undoneOperations.addAll(tag.undoneOperations)
				isReplaying = true
				try {
					for op <- tag.appliedOperations do applyOperation(op)
				} finally {
					isReplaying = false
				}
				true
		}
	}

	def deleteTag(name: String): Boolean = _tags.remove(name).isDefined

	def undo(): Boolean = {
		if _appliedOperations.isEmpty then false
		else {
			val op = _appliedOperations.remove(_appliedOperations.size - 1)
			_undoneOperations.append(op)
			val opsToReplay = _appliedOperations.toList
			env.reset()
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

	def redo(): Boolean = {
		if _undoneOperations.isEmpty then false
		else {
			val op = _undoneOperations.remove(_undoneOperations.size - 1)
			_appliedOperations.append(op)
			isReplaying = true
			try {
				applyOperation(op)
				true
			} catch {
				case NonFatal(e) =>
					_appliedOperations.remove(_appliedOperations.size - 1)
					_undoneOperations.append(op)
					throw e
			} finally {
				isReplaying = false
			}
		}
	}

	def applyOperation(op: EnvOperation): Unit = op match {
		case EnvOperation.StepNode(node) => env.stepNode(node)
		case EnvOperation.StepAllNodes => env.stepAllNodes()
		case EnvOperation.RunNodeUntilIdle(node, maxSteps) => env.runNodeUntilIdle(node, maxSteps)
		case EnvOperation.RunAllNodesUntilIdle(maxRounds) => env.runAllNodesUntilIdle(maxRounds)
		case EnvOperation.DeliverPacket(id) => env.network.deliverPacket(id)
		case EnvOperation.DropPacket(id) => env.network.dropPacket(id)
		case EnvOperation.DeliverNext(from, to) => env.network.deliverNext(from, to)
		case EnvOperation.DropNext(from, to) => env.network.dropNext(from, to)
		case EnvOperation.DeliverFirstN(from, to, n) => env.network.deliverFirstN(from, to, n)
		case EnvOperation.DropFirstN(from, to, n) => env.network.dropFirstN(from, to, n)
		case EnvOperation.DeliverAllBetween(from, to) => env.network.deliverAllBetween(from, to)
		case EnvOperation.DropAllBetween(from, to) => env.network.dropAllBetween(from, to)
		case EnvOperation.DeliverAllTo(to) => env.network.deliverAllTo(to)
		case EnvOperation.DeliverAll => env.network.deliverAll()
		case EnvOperation.FailPacket(id, msg) => env.network.failPacket(id, new java.io.IOException(msg))
		case EnvOperation.AdvanceTime(ticks) => env.clock.advanceTime(ticks)
		case EnvOperation.TriggerWakeUp(tokenId) => env.clock.triggerWakeUp(tokenId)
		case EnvOperation.CompleteStorageSave(opId) => env.storage.completeStorageSave(opId)
		case EnvOperation.CompleteNextStorageSave(node) => env.storage.completeNextStorageSave(node)
		case EnvOperation.CompleteAllStorageSaves(node) => env.storage.completeAllStorageSaves(node)
		case EnvOperation.FailStorageSave(opId, msg) => env.storage.failStorageSave(opId, new RuntimeException(msg))
		case EnvOperation.StartNode(node) => env.startNode(node)
		case EnvOperation.CrashNode(node) => env.crashNode(node)
		case EnvOperation.RestartNode(node) => env.restartNode(node)
		case EnvOperation.StartAllNodes => env.startAllNodes()
		case EnvOperation.SubmitClientCommand(target, client, serial, flag) => env.submitClientCommand(target, client, serial, flag)
		case EnvOperation.SubmitElectorateChange(target, desired) => env.submitElectorateChange(target, desired)
		case EnvOperation.UpdateDynamicSettings(retries, retention, term, idx, target) => env.updateDynamicSettings(retries, retention, term, idx, target)
		case EnvOperation.ToggleStorageAutoSucceed(target) => env.storage.toggleStorageAutoSucceed(target)
	}
}
