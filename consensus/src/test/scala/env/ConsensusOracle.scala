package readren.consensus.env

import readren.consensus.ConsensusParticipantSdm.*
import readren.consensus.TestClientCommand
import readren.consensus.protocol.*

import scala.collection.mutable

/** Quarantined omniscient invariant inspector for [[ConsensusEnvironment]], verifying cross-node consensus safety properties. */
class ConsensusOracle(env: ConsensusEnvironment) {
	private val _leaderByTerm: mutable.Map[Term, NodeId] = mutable.Map.empty
	private val _appliedCommandsByIndex: mutable.Map[RecordIndex, (NodeId, TestClientCommand)] = mutable.Map.empty
	private val _committedRecordsByNode: mutable.Map[NodeId, mutable.ArrayBuffer[Record | None.type]] = mutable.Map.empty

	def leaderByTerm: collection.Map[Term, NodeId] = _leaderByTerm

	def appliedCommandsByIndex: collection.Map[RecordIndex, (NodeId, TestClientCommand)] = _appliedCommandsByIndex

	def committedRecordsByNode: collection.Map[NodeId, collection.Seq[Record | None.type]] = _committedRecordsByNode

	def highestCommittedRecordIndex: RecordIndex = _committedRecordsByNode.values.map(_.size.toLong).maxOption.getOrElse(0L)

	private[env] def reset(): Unit = {
		_leaderByTerm.clear()
		_appliedCommandsByIndex.clear()
		_committedRecordsByNode.clear()
		for i <- 0 until env.clusterSize do {
			val id = NodeId(s"p-$i")
			_committedRecordsByNode(id) = mutable.ArrayBuffer.empty
		}
	}

	private[env] def onLogTruncated(node: EnvironmentNode, index: RecordIndex, firstRemovedRecord: Record): Unit = {
		// Defer the check to let the updating execution complete its atomic state and role changes.
		node.stepDoer.run {
			if node.participant != null && node.participant.getRoleOrdinal == LEADER then {
				throw new AssertionError(s"Leader Append-Only invariant violated: Leader ${node.myId} truncated log at index $index. Removed: $firstRemovedRecord")
			}
		}
	}

	private[env] def onStorageSaved(node: EnvironmentNode): Unit = checkLogMatching()

	private[env] def onBecameLeader(node: EnvironmentNode, term: Term): Unit = {
		_leaderByTerm.get(term) match {
			case Some(existing) if existing != node.myId =>
				throw new AssertionError(s"Election Safety invariant violated: Multiple leaders elected in term $term: $existing and ${node.myId}")
			case _ =>
				_leaderByTerm(term) = node.myId
		}
	}

	private[env] def onCommitIndexChanged(node: EnvironmentNode, previous: RecordIndex, current: RecordIndex, as: RoleOrdinal, at: Term): Unit = {
		val thisNodeCommitted = _committedRecordsByNode.getOrElseUpdate(node.myId, mutable.ArrayBuffer.empty)
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
			for (otherId, otherCommitted) <- _committedRecordsByNode if otherId != node.myId do {
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

	private[env] def onCommandApplied(node: EnvironmentNode, command: TestClientCommand, index: RecordIndex): Unit = {
		_appliedCommandsByIndex.get(index) match {
			case Some((prevNode, prevCmd)) =>
				if prevCmd != command then {
					throw new AssertionError(s"State Machine Safety invariant violated at index $index: Node ${node.myId} applied $command, but $prevNode applied $prevCmd")
				}
			case None =>
				_appliedCommandsByIndex(index) = (node.myId, command)
		}
	}

	def checkLogMatching(): Unit = {
		val activeNodes = env.allNodes.filterNot(_.isDown)
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
}
