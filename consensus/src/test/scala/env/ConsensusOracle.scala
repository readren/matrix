package readren.consensus.env

import readren.consensus.ConsensusParticipantSdm.*
import readren.consensus.TestClientCommand
import readren.consensus.protocol.*

import scala.collection.mutable

/** Quarantined omniscient invariant inspector (Tier 3) for [[ConsensusEnvironment]], verifying cross-node consensus safety properties.
 *
 * Isolates privileged global state tracking—such as cross-node committed log histories, elected leaders per term, and state machine command applications—from external client and control-plane gateways (Tier 2). Continuously enforces the five fundamental Raft safety invariants:
 *  - **Election Safety**: At most one leader can be elected in a given term ([[onBecameLeader]]).
 *  - **Leader Append-Only**: A leader never overwrites or truncates entries in its log ([[onLogTruncated]]).
 *  - **Log Matching**: If two logs contain an entry with the same index and term, then the logs are identical in all entries up through the given index ([[checkLogMatching]]).
 *  - **Leader Completeness**: If a log entry is committed in a given term, that entry will be present in the logs of the leaders for all higher-numbered terms ([[onCommitIndexChanged]]).
 *  - **State Machine Safety**: If a participant has applied a log entry at a given index to its state machine, no other participant will ever apply a different log entry for the same index ([[onCommandApplied]]).
 * @param env the owning [[ConsensusEnvironment]]. */
class ConsensusOracle(env: ConsensusEnvironment) {
	private val _leaderByTerm: mutable.Map[Term, NodeId] = mutable.Map.empty
	private val _appliedCommandsByIndex: mutable.Map[RecordIndex, (NodeId, TestClientCommand)] = mutable.Map.empty
	private val _committedRecordsByNode: mutable.Map[NodeId, mutable.ArrayBuffer[Record | None.type]] = mutable.Map.empty

	/** Read-only view of the unique leader elected in each [[Term]]. */
	def leaderByTerm: collection.Map[Term, NodeId] = _leaderByTerm

	/** Read-only view mapping each applied [[RecordIndex]] to the first `(NodeId, TestClientCommand)` pair that applied it. */
	def appliedCommandsByIndex: collection.Map[RecordIndex, (NodeId, TestClientCommand)] = _appliedCommandsByIndex

	/** Read-only view of each node's historical committed log records (0-based buffer where index `i` corresponds to `RecordIndex = i + 1`, with `None` occupying unrecorded snapshot gaps). */
	def committedRecordsByNode: collection.Map[NodeId, collection.Seq[Record | None.type]] = _committedRecordsByNode

	/** Returns the highest [[RecordIndex]] known to have been committed by any node in the cluster. */
	def highestCommittedRecordIndex: RecordIndex = _committedRecordsByNode.values.map(_.size.toLong).maxOption.getOrElse(0L)

	/** Clears all recorded oracle history and re-initializes per-node committed record buffers. */
	private[env] def reset(): Unit = {
		_leaderByTerm.clear()
		_appliedCommandsByIndex.clear()
		_committedRecordsByNode.clear()
		for i <- 0 until env.clusterSize do {
			val id = NodeId(s"p-$i")
			_committedRecordsByNode(id) = mutable.ArrayBuffer.empty
		}
	}

	/** Verifies the **Leader Append-Only** invariant when `node` truncates its log suffix starting at `index`.
	 *
	 * Schedules the role check at the end of the current sequencer turn so that atomic role demotions (such as a leader stepping down to follower upon discovering a higher term before truncating) complete before the assertion is evaluated. */
	private[env] def onLogTruncated(node: EnvironmentNode, index: RecordIndex, firstRemovedRecord: Record): Unit = {
		// Defer the check to let the updating execution complete its atomic state and role changes.
		node.stepDoer.run {
			if node.participant != null && node.participant.getRoleOrdinal == LEADER then {
				throw new AssertionError(s"Leader Append-Only invariant violated: Leader ${node.myId} truncated log at index $index. Removed: $firstRemovedRecord")
			}
		}
	}

	/** Invoked whenever `node` persists its workspace to durable storage, triggering [[checkLogMatching]]. */
	private[env] def onStorageSaved(node: EnvironmentNode): Unit = checkLogMatching()

	/** Verifies the **Election Safety** invariant when `node` transitions to [[LEADER]] in `term`. */
	private[env] def onBecameLeader(node: EnvironmentNode, term: Term): Unit = {
		_leaderByTerm.get(term) match {
			case Some(existing) if existing != node.myId =>
				throw new AssertionError(s"Election Safety invariant violated: Multiple leaders elected in term $term: $existing and ${node.myId}")
			case _ =>
				_leaderByTerm(term) = node.myId
		}
	}

	/** Records newly committed log entries for `node` and verifies both per-node commit consistency across restarts and **Leader Completeness** when `as == LEADER`. */
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

	/** Verifies the **State Machine Safety** invariant when `node` applies `command` at log `index`. */
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

	/** Verifies the **Log Matching** invariant across all active (non-down) nodes' durable storage logs.
	 *
	 * For every pair of active nodes, finds the highest overlapping log index where both logs share the same term and asserts that all preceding overlapping records are identical. */
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

	/** Returns true if all cluster nodes have shut down, released their service, or reached [[QUIESCED]]. */
	def areAllNodesQuiesced: Boolean =
		env.allNodes.forall(n => n.isDown || n.participant == null || n.participant.getRoleOrdinal == QUIESCED)

	/** Formats a diagnostic string of all active non-quiesced node roles. */
	def formatNonQuiescedNodesDiagnostic: String =
		env.allNodes.filter(n => !n.isDown && n.participant != null && n.participant.getRoleOrdinal != QUIESCED).map(n => s"${n.myId}:${n.participant.getRoleOrdinal}").mkString(", ")
}
