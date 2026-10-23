package readren.consensus.env

import readren.common.Maybe
import readren.consensus.ConsensusParticipantSdm.*
import readren.consensus.protocol.*

import scala.collection.mutable

/** A suspended persistence write operation awaiting explicit completion or failure injection in [[SimulatedStorageSubsystem]].
 * @param opId unique identifier of this persistence operation.
 * @param nodeId identifier of the node performing the workspace save.
 * @param term the term stored in the workspace being saved.
 * @param logBufferOffset the starting index of the log buffer in the workspace being saved.
 * @param firstEmptyRecordIndex the next unallocated log index in the workspace being saved.
 * @param records the snapshot of log records contained in the workspace being saved.
 * @param snapshot the latest state machine and configuration snapshot present in the workspace being saved.
 * @param completeOp callback that commits the workspace copy to durable memory and schedules completion on the node's [[StepDoer]].
 * @param failOp callback that schedules a persistence failure on the node's [[StepDoer]]. */
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

/** Asynchronous persistence queue and storage fault-injection subsystem (Tier 1) for [[ConsensusEnvironment]].
 *
 * Manages workspace saves that exceed a node's automatic persistence thresholds (`autoSucceedUntilTerm` or `autoSucceedUntilRecordIndex`), allowing tests to interleave, delay, or fail disk writes deterministically.
 * @param env the owning [[ConsensusEnvironment]]. */
class SimulatedStorageSubsystem(env: ConsensusEnvironment) {
	private var storageOpIdSequencer: StorageOpId = 0
	private val pendingPersistenceMap: mutable.Map[StorageOpId, PendingPersistence] = mutable.Map.empty

	/** Resets the storage operation ID counter and clears all pending persistence operations. */
	private[env] def reset(): Unit = {
		storageOpIdSequencer = 0
		pendingPersistenceMap.clear()
	}

	/** Allocates the next monotonically increasing [[StorageOpId]]. */
	private[env] def nextStorageOpId(): StorageOpId = {
		storageOpIdSequencer += 1
		storageOpIdSequencer
	}

	/** Enqueues a suspended [[PendingPersistence]] operation into the pending registry. */
	private[env] def registerPendingPersistence(p: PendingPersistence): Unit = {
		pendingPersistenceMap(p.opId) = p
	}

	/** Returns all currently suspended persistence operations across the cluster, ordered by `opId`. */
	def pendingPersistenceOperations: Seq[PendingPersistence] = pendingPersistenceMap.values.toSeq.sortBy(_.opId)

	/** Completes a specific pending workspace save operation, committing its contents to the node's durable storage.
	 * @param opId the identifier of the pending storage save operation.
	 * @return true if the operation was found and completed; false otherwise. */
	def completeStorageSave(opId: StorageOpId): Boolean = env.journal.recordOrExecute(EnvOperation.CompleteStorageSave(opId)) {
		pendingPersistenceMap.remove(opId).fold(false) { p =>
			p.completeOp()
			true
		}
	}

	/** Completes the oldest pending workspace save operation for `nodeRef` in FIFO order.
	 * @param nodeRef the node whose next pending save should be completed.
	 * @return true if a pending save existed for `nodeRef` and was completed; false otherwise. */
	def completeNextStorageSave(nodeRef: NodeRef): Boolean = env.journal.recordOrExecute(EnvOperation.CompleteNextStorageSave(nodeRef.asNodeId)) {
		val nodeId = nodeRef.asNodeId
		pendingPersistenceOperations.find(_.nodeId == nodeId).fold(false) { p =>
			completeStorageSave(p.opId)
		}
	}

	/** Completes all currently pending workspace save operations for `nodeRef` in FIFO order.
	 * @param nodeRef the node whose pending saves should all be completed.
	 * @return the number of storage save operations completed. */
	def completeAllStorageSaves(nodeRef: NodeRef): Int = env.journal.recordOrExecute(EnvOperation.CompleteAllStorageSaves(nodeRef.asNodeId)) {
		val nodeId = nodeRef.asNodeId
		val matching = pendingPersistenceOperations.filter(_.nodeId == nodeId)
		for p <- matching do completeStorageSave(p.opId)
		matching.size
	}

	/** Fails a specific pending workspace save operation with `error`, leaving the node's previously saved memory intact.
	 * @param opId the identifier of the pending storage save operation to fail.
	 * @param error the exception delivered to the storage save caller.
	 * @return true if the operation was found and failed; false otherwise. */
	def failStorageSave(opId: StorageOpId, error: Throwable): Boolean = env.journal.recordOrExecute(EnvOperation.FailStorageSave(opId, error.getMessage)) {
		pendingPersistenceMap.remove(opId).fold(false) { p =>
			p.failOp(error)
			true
		}
	}

	/** Toggles synchronous auto-succeed persistence mode vs. manual asynchronous persistence queueing for a specific node or across all nodes.
	 * @param targetNode the target node to toggle, or [[None]] (or `"all"`) to toggle all nodes in the cluster. */
	def toggleStorageAutoSucceed(targetNode: Option[NodeRef] = None): Unit = env.journal.recordOrExecute(EnvOperation.ToggleStorageAutoSucceed(targetNode.map(_.asNodeId))) {
		targetNode match {
			case Some(ref) if ref.asNodeId.nonEmpty && ref.asNodeId != "all" =>
				val n = env.node(ref)
				val currentlyAuto = n.autoSucceedUntilTerm == Int.MaxValue && n.autoSucceedUntilRecordIndex == Long.MaxValue
				if currentlyAuto then n.autoSucceedUntilRecordIndex = 0L
				else {
					n.autoSucceedUntilTerm = Int.MaxValue.asInstanceOf[Term]
					n.autoSucceedUntilRecordIndex = Long.MaxValue
				}
			case _ =>
				val anyAuto = (0 until env.clusterSize).exists(i => env.node(i).autoSucceedUntilTerm == Int.MaxValue && env.node(i).autoSucceedUntilRecordIndex == Long.MaxValue)
				for i <- 0 until env.clusterSize do {
					val n = env.node(i)
					if anyAuto then n.autoSucceedUntilRecordIndex = 0L
					else {
						n.autoSucceedUntilTerm = Int.MaxValue.asInstanceOf[Term]
						n.autoSucceedUntilRecordIndex = Long.MaxValue
					}
				}
		}
	}
}
