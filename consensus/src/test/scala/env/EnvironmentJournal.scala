package readren.consensus.env

import readren.consensus.ConsensusParticipantSdm.*
import readren.consensus.protocol.*

import scala.collection.mutable
import scala.util.control.NonFatal

/** Algebraic representation of a discrete-event operation applied to [[ConsensusEnvironment]], recorded by [[EnvironmentJournal]] for deterministic replay, undo/redo, and checkpoint restoration. */
sealed trait EnvOperation

object EnvOperation {
	/** Executes a single pending task on `node`'s [[StepDoer]]. */
	final case class StepNode(node: NodeId) extends EnvOperation

	/** Executes at most one pending task on each node's [[StepDoer]]. */
	case object StepAllNodes extends EnvOperation

	/** Executes pending tasks on `node`'s [[StepDoer]] until idle or `maxSteps` is reached. */
	final case class RunNodeUntilIdle(node: NodeId, maxSteps: Int) extends EnvOperation

	/** Repeatedly steps all nodes until all [[StepDoer]] queues are empty or `maxRounds` is reached. */
	final case class RunAllNodesUntilIdle(maxRounds: Int) extends EnvOperation

	/** Delivers a specific network packet identified by `packetId`. */
	final case class DeliverPacket(packetId: PacketId) extends EnvOperation

	/** Drops a specific network packet identified by `packetId` in transit. */
	final case class DropPacket(packetId: PacketId) extends EnvOperation

	/** Delivers the head packet in the directional channel from `from` to `to`. */
	final case class DeliverNext(from: NodeId, to: NodeId) extends EnvOperation

	/** Drops the head packet in the directional channel from `from` to `to`. */
	final case class DropNext(from: NodeId, to: NodeId) extends EnvOperation

	/** Delivers up to `n` packets from the front of the directional channel from `from` to `to`. */
	final case class DeliverFirstN(from: NodeId, to: NodeId, n: Int) extends EnvOperation

	/** Drops up to `n` packets from the front of the directional channel from `from` to `to`. */
	final case class DropFirstN(from: NodeId, to: NodeId, n: Int) extends EnvOperation

	/** Delivers all packets in the directional channel from `from` to `to`. */
	final case class DeliverAllBetween(from: NodeId, to: NodeId) extends EnvOperation

	/** Drops all packets in the directional channel from `from` to `to`. */
	final case class DropAllBetween(from: NodeId, to: NodeId) extends EnvOperation

	/** Delivers all packets queued across all inbound channels targeting `to`. */
	final case class DeliverAllTo(to: NodeId) extends EnvOperation

	/** Delivers all packets currently in transit across all channels. */
	case object DeliverAll extends EnvOperation

	/** Fails a specific network packet identified by `packetId` with an `IOException(errorMsg)`. */
	final case class FailPacket(packetId: PacketId, errorMsg: String) extends EnvOperation

	/** Advances the virtual clock by `ticks` and triggers any expired wake-up timers. */
	final case class AdvanceTime(ticks: Int) extends EnvOperation

	/** Immediately fires a pending wake-up timer identified by `tokenId`. */
	final case class TriggerWakeUp(tokenId: WakeUpTokenId) extends EnvOperation

	/** Completes a suspended storage save operation identified by `opId`. */
	final case class CompleteStorageSave(opId: StorageOpId) extends EnvOperation

	/** Completes the oldest suspended storage save operation for `node`. */
	final case class CompleteNextStorageSave(node: NodeId) extends EnvOperation

	/** Completes all suspended storage save operations for `node`. */
	final case class CompleteAllStorageSaves(node: NodeId) extends EnvOperation

	/** Fails a suspended storage save operation identified by `opId` with a `RuntimeException(errorMsg)`. */
	final case class FailStorageSave(opId: StorageOpId, errorMsg: String) extends EnvOperation

	/** Starts `node` if it is not currently running. */
	final case class StartNode(node: NodeId) extends EnvOperation

	/** Crashes `node` and discards all unexecuted tasks from its [[StepDoer]] queue. */
	final case class CrashNode(node: NodeId) extends EnvOperation

	/** Restarts a previously crashed `node` from its durable storage state. */
	final case class RestartNode(node: NodeId) extends EnvOperation

	/** Starts all initial seed participants in the cluster. */
	case object StartAllNodes extends EnvOperation

	/** Submits a client command to `targetNode` on behalf of `client`. */
	final case class SubmitClientCommand(targetNode: NodeId, client: String, serial: Option[Int], attemptFlag: CommandAttemptFlag) extends EnvOperation

	/** Submits an electorate change proposal to `targetNode` requesting `desiredParticipants`. */
	final case class SubmitElectorateChange(targetNode: NodeId, desiredParticipants: Set[NodeId]) extends EnvOperation

	/** Updates dynamic environment settings and storage auto-succeed thresholds. */
	final case class UpdateDynamicSettings(
		retiringMaxRetries: Option[Int],
		logRetention: Option[Int],
		autoSucceedUntilTerm: Option[Term],
		autoSucceedUntilRecordIndex: Option[RecordIndex],
		targetNode: Option[NodeId]
	) extends EnvOperation

	/** Toggles synchronous storage auto-succeed mode on `targetNode` (or across all nodes if [[None]]). */
	final case class ToggleStorageAutoSucceed(targetNode: Option[NodeId]) extends EnvOperation


	/** Administratively updates the cluster tier's memorized probable participants on `targetNode`. */
	final case class UpdateProbableParticipants(targetNode: NodeId, probableParticipants: Set[NodeId]) extends EnvOperation
}

/** Immutable snapshot of an [[EnvironmentJournal]] timeline capturing both applied operations and undone (redo-eligible) operations.
 * @param appliedOperations ordered list of operations applied from initial reset up to the checkpoint.
 * @param undoneOperations stack of undone operations available for [[EnvironmentJournal.redo]] at the time of the checkpoint. */
final case class CheckpointTag(
	appliedOperations: List[EnvOperation],
	undoneOperations: List[EnvOperation]
) {
	/** Number of applied operations recorded in this checkpoint. */
	def size: Int = appliedOperations.size
}

/** Deterministic operation recording, checkpoint tagging, and undo/redo replay engine for [[ConsensusEnvironment]].
 *
 * Intercepts top-level environment mutations via [[recordOrExecute]], appending each [[EnvOperation]] to an ordered journal. Supports rolling back steps via [[undo]] (which resets the environment and replays the prefix of remaining operations), re-applying rolled-back steps via [[redo]], and branching/restoring named timeline checkpoints via [[createTag]] and [[restoreTag]].
 * @param env the owning [[ConsensusEnvironment]]. */
class EnvironmentJournal(env: ConsensusEnvironment) {
	private val _appliedOperations: mutable.ArrayBuffer[EnvOperation] = mutable.ArrayBuffer.empty
	private val _undoneOperations: mutable.ArrayBuffer[EnvOperation] = mutable.ArrayBuffer.empty
	private val _tags: mutable.Map[String, CheckpointTag] = mutable.Map.empty
	private var isReplaying: Boolean = false
	private var operationDepth: Int = 0

	/** Records `op` into the journal (and clears the redo stack) if invoked at top-level outside a replay, then executes `action`.
	 *
	 * Nested environment calls invoked within `action` (when `operationDepth > 0`) are executed directly without creating duplicate journal entries. */
	private[env] def recordOrExecute[T](op: => EnvOperation)(action: => T): T = {
		if !isReplaying && operationDepth == 0 then {
			_appliedOperations.append(op)
			_undoneOperations.clear()
		}
		operationDepth += 1
		try action
		finally operationDepth -= 1
	}

	/** Executes `action` with journal recording suppressed, used when running [[ConsensusEnvironment.initializer]] during resets. */
	private[env] def withReplaySuppression[T](action: => T): T = {
		val prevReplaying = isReplaying
		isReplaying = true
		try action
		finally isReplaying = prevReplaying
	}

	/** Ordered sequence of operations currently applied to the environment. */
	def appliedOperations: Seq[EnvOperation] = _appliedOperations.toSeq

	/** Number of operations currently applied to the environment. */
	def appliedOperationsCount: Int = _appliedOperations.size

	/** Sequence of undone operations currently available on the redo stack. */
	def undoneOperations: Seq[EnvOperation] = _undoneOperations.toSeq

	/** Number of undone operations currently available on the redo stack. */
	def undoneOperationsCount: Int = _undoneOperations.size

	/** Map of all named [[CheckpointTag]] snapshots registered in this journal. */
	def tags: Map[String, CheckpointTag] = _tags.toMap

	/** Saves the current applied operation history and redo stack under the checkpoint name `name`.
	 * @param name identifier for the checkpoint tag. */
	def createTag(name: String): Unit = {
		_tags(name) = CheckpointTag(_appliedOperations.toList, _undoneOperations.toList)
	}

	/** Resets the environment and deterministically replays the operation history saved under checkpoint `name`, restoring its redo stack as well.
	 * @param name identifier of the checkpoint tag to restore.
	 * @return true if the tag existed and was restored; false otherwise. */
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

	/** Removes the checkpoint tag named `name` from the journal.
	 * @param name identifier of the checkpoint tag to delete.
	 * @return true if the tag existed and was removed; false otherwise. */
	def deleteTag(name: String): Boolean = _tags.remove(name).isDefined

	/** Rolls back the most recently applied operation by moving it to the redo stack, resetting the environment, and replaying all preceding operations.
	 * @return true if an operation was undone; false if [[appliedOperations]] was empty. */
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

	/** Re-applies the most recently undone operation from the top of the redo stack.
	 * @return true if an operation was re-applied; false if [[undoneOperations]] was empty. */
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

	/** Dispatches a single recorded [[EnvOperation]] to its corresponding [[ConsensusEnvironment]] or subsystem method. */
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
		case EnvOperation.UpdateProbableParticipants(target, peers) => env.updateProbableParticipants(target, peers)
	}
}
