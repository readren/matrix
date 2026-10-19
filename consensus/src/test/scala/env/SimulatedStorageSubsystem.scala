package readren.consensus.env

import readren.common.Maybe
import readren.consensus.ConsensusParticipantSdm.*
import readren.consensus.protocol.*

import scala.collection.mutable

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

/** Asynchronous persistence queue and threshold controls for [[ConsensusEnvironment]]. */
class SimulatedStorageSubsystem(env: ConsensusEnvironment) {
	private var storageOpIdSequencer: StorageOpId = 0
	private val pendingPersistenceMap: mutable.Map[StorageOpId, PendingPersistence] = mutable.Map.empty

	private[env] def reset(): Unit = {
		storageOpIdSequencer = 0
		pendingPersistenceMap.clear()
	}

	private[env] def nextStorageOpId(): StorageOpId = {
		storageOpIdSequencer += 1
		storageOpIdSequencer
	}

	private[env] def registerPendingPersistence(p: PendingPersistence): Unit = {
		pendingPersistenceMap(p.opId) = p
	}

	def pendingPersistenceOperations: Seq[PendingPersistence] = pendingPersistenceMap.values.toSeq.sortBy(_.opId)

	def completeStorageSave(opId: StorageOpId): Boolean = env.journal.recordOrExecute(EnvOperation.CompleteStorageSave(opId)) {
		pendingPersistenceMap.remove(opId).fold(false) { p =>
			p.completeOp()
			true
		}
	}

	def completeNextStorageSave(nodeRef: NodeRef): Boolean = env.journal.recordOrExecute(EnvOperation.CompleteNextStorageSave(nodeRef.asNodeId)) {
		val nodeId = nodeRef.asNodeId
		pendingPersistenceOperations.find(_.nodeId == nodeId).fold(false) { p =>
			completeStorageSave(p.opId)
		}
	}

	def completeAllStorageSaves(nodeRef: NodeRef): Int = env.journal.recordOrExecute(EnvOperation.CompleteAllStorageSaves(nodeRef.asNodeId)) {
		val nodeId = nodeRef.asNodeId
		val matching = pendingPersistenceOperations.filter(_.nodeId == nodeId)
		for p <- matching do completeStorageSave(p.opId)
		matching.size
	}

	def failStorageSave(opId: StorageOpId, error: Throwable): Boolean = env.journal.recordOrExecute(EnvOperation.FailStorageSave(opId, error.getMessage)) {
		pendingPersistenceMap.remove(opId).fold(false) { p =>
			p.failOp(error)
			true
		}
	}

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
