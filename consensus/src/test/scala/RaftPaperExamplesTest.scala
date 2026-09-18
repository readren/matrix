package readren.consensus

import munit.FunSuite
import readren.common.Maybe
import readren.consensus.ConsensusParticipantSdm.*

class RaftPaperExamplesTest extends FunSuite {

	private def cmd(term: Int, serial: Int): CommandRecord[TestClientCommand] = {
		CommandRecord(term.asInstanceOf[Term], TestClientCommand(serial, "c-1"))
	}

	test("Figure 7: Follower log reconciliation across scenarios a, b, c, d, e, and f") {
		// Cluster of 7 nodes:
		// p-0 is the leader (term 8, 10 records across terms 1, 4, 5, 6)
		// p-1 (scenario a): missing entry 10 (has entries 1-9)
		// p-2 (scenario b): missing entries 5-10 (has entries 1-4)
		// p-3 (scenario c): has entries 1-10, plus extra entry 11 (term 6), was partitioned during election
		// p-4 (scenario d): has entries 1-10, plus extra entries 11-12 (term 7), was crashed/down during election
		// p-5 (scenario e): has entries 1-3 (term 1), entries 4-7 (term 4)
		// p-6 (scenario f): has entries 1-3 (term 1), entries 4-6 (term 2), entries 7-8 (term 3)
		val env = new ConsensusEnvironment(clusterSize = 7, logCompactionThreshold = 100)

		val initConfig = TransitionalConfigChange[String](
			1.asInstanceOf[Term],
			"cfg-0",
			Set.empty,
			env.defaultInitialParticipants
		)

		// 1. Leader (p-0) log: 10 entries across terms 1, 4, 5, 6
		val leaderMem = env.node(0).storage.savedMemory
		leaderMem.currentTerm = 8.asInstanceOf[Term]
		leaderMem.logBuffer.clear()
		leaderMem.appendRecord(initConfig) // index 1: term 1
		leaderMem.appendRecord(cmd(1, 2)) // index 2: term 1
		leaderMem.appendRecord(cmd(1, 3)) // index 3: term 1
		leaderMem.appendRecord(cmd(4, 4)) // index 4: term 4
		leaderMem.appendRecord(cmd(4, 5)) // index 5: term 4
		leaderMem.appendRecord(cmd(5, 6)) // index 6: term 5
		leaderMem.appendRecord(cmd(5, 7)) // index 7: term 5
		leaderMem.appendRecord(cmd(6, 8)) // index 8: term 6
		leaderMem.appendRecord(cmd(6, 9)) // index 9: term 6
		leaderMem.appendRecord(cmd(6, 10)) // index 10: term 6

		// 2. Follower p-1 (scenario a): missing entry 10
		val memA = env.node(1).storage.savedMemory
		memA.currentTerm = 6.asInstanceOf[Term]
		memA.logBuffer.clear()
		for i <- 1 to 9 do memA.appendRecord(leaderMem.getRecordAt(i))

		// 3. Follower p-2 (scenario b): missing entries 5-10
		val memB = env.node(2).storage.savedMemory
		memB.currentTerm = 4.asInstanceOf[Term]
		memB.logBuffer.clear()
		for i <- 1 to 4 do memB.appendRecord(leaderMem.getRecordAt(i))

		// 4. Follower p-3 (scenario c): has 1-10, plus uncommitted entry 11 (term 6)
		val memC = env.node(3).storage.savedMemory
		memC.currentTerm = 6.asInstanceOf[Term]
		memC.logBuffer.clear()
		for i <- 1 to 10 do memC.appendRecord(leaderMem.getRecordAt(i))
		memC.appendRecord(cmd(6, 11))

		// 5. Follower p-4 (scenario d): has 1-10, plus uncommitted entries 11-12 (term 7).
		// Per Raft paper §5.3/Figure 7, server d was leader for term 7 and crashed/remained down.
		val memD = env.node(4).storage.savedMemory
		memD.currentTerm = 7.asInstanceOf[Term]
		memD.logBuffer.clear()
		for i <- 1 to 10 do memD.appendRecord(leaderMem.getRecordAt(i))
		memD.appendRecord(cmd(7, 11))
		memD.appendRecord(cmd(7, 12))

		// 6. Follower p-5 (scenario e): entries 1-3 (term 1), entries 4-7 (term 4)
		val memE = env.node(5).storage.savedMemory
		memE.currentTerm = 4.asInstanceOf[Term]
		memE.logBuffer.clear()
		for i <- 1 to 3 do memE.appendRecord(leaderMem.getRecordAt(i))
		memE.appendRecord(cmd(4, 4))
		memE.appendRecord(cmd(4, 5))
		memE.appendRecord(cmd(4, 6))
		memE.appendRecord(cmd(4, 7))

		// 7. Follower p-6 (scenario f): entries 1-3 (term 1), 4-6 (term 2), 7-8 (term 3)
		val memF = env.node(6).storage.savedMemory
		memF.currentTerm = 3.asInstanceOf[Term]
		memF.logBuffer.clear()
		for i <- 1 to 3 do memF.appendRecord(leaderMem.getRecordAt(i))
		memF.appendRecord(cmd(2, 4))
		memF.appendRecord(cmd(2, 5))
		memF.appendRecord(cmd(2, 6))
		memF.appendRecord(cmd(3, 7))
		memF.appendRecord(cmd(3, 8))

		// Start online nodes forming the election quorum (p-3 and p-4 were offline/partitioned during election)
		env.startNode(0)
		env.startNode(1)
		env.startNode(2)
		env.startNode(5)
		env.startNode(6)

		// =========================================================================
		// ADVANCE EXECUTION FROM THIS POINT ONWARD
		// =========================================================================

		// Client submits command to p-0; triggers leader election bumping term from 8 to 9
		val handle = env.submitClientCommand(targetNode = 0, client = 1)
		env.runAllNodesUntilIdle()

		// Dispatch discovery and election packets
		while env.nodeRole(0) != "LEADER" && env.pendingPackets.nonEmpty do {
			env.deliverAll()
			env.runAllNodesUntilIdle()
		}

		assertEquals(env.nodeRole(0), "LEADER")

		// Reconnect followers p-3 (scenario c) and p-4 (scenario d) to the cluster
		env.startNode(3)
		env.startNode(4)

		// Advance virtual time to trigger retry for unreachable followers
		env.advanceTime(500)
		env.runAllNodesUntilIdle()

		// Dispatch all append requests and replies across the cluster until all logs are repaired
		while env.pendingPackets.nonEmpty do {
			env.deliverAll()
			env.runAllNodesUntilIdle()
		}

		// Verify every follower's log is repaired and identically matches the leader up to index 10
		for followerIdx <- 1 to 6 do {
			val followerMem = env.node(followerIdx).storage.savedMemory
			for recordIdx <- 1 to 10 do {
				assertEquals(
					followerMem.getRecordAt(recordIdx).term,
					leaderMem.getRecordAt(recordIdx).term,
					s"Follower p-$followerIdx mismatched leader at index $recordIdx"
				)
			}
			// Conflicting entries at index 11 (scenarios c and d) were overwritten with the leader's term 9 command
			assertEquals(followerMem.getRecordAt(11).term, 9.asInstanceOf[Term])
		}

		val ClientCommandStatus.Processed(_, res) = env.clientCommandStatus(handle.commandId): @unchecked
		assertEquals(res, 1)
	}

	test("Figure 8: Leader never commits prior-term log entries by counting replicas alone") {
		// 5 nodes: p-0 to p-4 (corresponding to S1 to S5 in Raft paper Figure 8)
		val env = new ConsensusEnvironment(clusterSize = 5)

		val initConfig = TransitionalConfigChange[String](
			1.asInstanceOf[Term],
			"cfg-0",
			Set.empty,
			env.defaultInitialParticipants
		)

		// Set up state at Phase (c):
		// - Entry 1 (term 1) is committed initial config
		// - Entry 2 was created by p-0 (S1) in term 2, and replicated to p-1 (S2)
		// - p-4 (S5) has an uncommitted entry at index 2 from term 3
		// - p-0 (S1) starts election from term 3, bumping its term to 4 upon becoming leader
		val mem0 = env.node(0).storage.savedMemory
		mem0.currentTerm = 3.asInstanceOf[Term]
		mem0.logBuffer.clear()
		mem0.appendRecord(initConfig) // index 1: term 1
		mem0.appendRecord(cmd(2, 2)) // index 2: term 2

		val mem1 = env.node(1).storage.savedMemory
		mem1.currentTerm = 2.asInstanceOf[Term]
		mem1.logBuffer.clear()
		mem1.appendRecord(initConfig)
		mem1.appendRecord(cmd(2, 2))

		val mem2 = env.node(2).storage.savedMemory
		mem2.currentTerm = 1.asInstanceOf[Term]
		mem2.logBuffer.clear()
		mem2.appendRecord(initConfig)

		val mem3 = env.node(3).storage.savedMemory
		mem3.currentTerm = 1.asInstanceOf[Term]
		mem3.logBuffer.clear()
		mem3.appendRecord(initConfig)

		val mem4 = env.node(4).storage.savedMemory
		mem4.currentTerm = 3.asInstanceOf[Term]
		mem4.logBuffer.clear()
		mem4.appendRecord(initConfig)
		mem4.appendRecord(cmd(3, 2))

		// Start p-0, p-1, p-2 (quorum of 3 out of 5)
		env.startNode(0)
		env.startNode(1)
		env.startNode(2)

		// =========================================================================
		// ADVANCE EXECUTION FROM THIS POINT ONWARD
		// =========================================================================

		// Trigger p-0 leader discovery and election; increments term from 3 to 4
		env.submitClientCommand(targetNode = 0, client = 1)
		env.runAllNodesUntilIdle()

		while env.nodeRole(0) != "LEADER" && env.pendingPackets.nonEmpty do {
			env.deliverAll()
			env.runAllNodesUntilIdle()
		}

		assertEquals(env.nodeRole(0), "LEADER")
		assertEquals(env.node(0).storage.savedMemory.getCurrentTerm, 4.asInstanceOf[Term])

		// Leader p-0 sent AppendRecords for [index 2 (term 2), index 3 (term 4)] to p-1 and p-2.
		// Deliver AppendRecords only to p-2 and step p-2, but hold p-1's response.
		// This replicates index 2 to p-2, reaching a majority {p-0, p-1, p-2} of 3 out of 5.
		while env.pendingPackets.exists(p => p.destination == "p-2" || p.source == "p-2") do {
			for p <- env.pendingPackets.filter(p => p.destination == "p-2" || p.source == "p-2") do {
				env.deliverPacket(p.id)
			}
			env.runAllNodesUntilIdle()
		}

		// At this point, index 2 (term 2) is present on p-0, p-1, and p-2 (majority of 3 out of 5)
		assertEquals(env.node(0).storage.savedMemory.getRecordAt(2).term, 2.asInstanceOf[Term])
		assertEquals(env.node(1).storage.savedMemory.getRecordAt(2).term, 2.asInstanceOf[Term])
		assertEquals(env.node(2).storage.savedMemory.getRecordAt(2).term, 2.asInstanceOf[Term])

		// RAFT §5.4.2 / Figure 8 CHECK:
		// Entry 2 is from term 2 (older term). It CANNOT be committed by replica count alone.
		// Since entry 3 (term 4) has only reached 2 nodes (p-0 and p-2, not a majority of 3),
		// commitIndex on p-0 cannot advance to 2 or 3!
		assert(env.node(0).machine.highestAppliedCommandIndex < 2)

		// Phase (e): Now deliver replication to p-1 as well, so current-term entry 3 reaches majority
		while env.pendingPackets.nonEmpty do {
			env.deliverAll()
			env.runAllNodesUntilIdle()
		}

		// With a current-term entry (term 4) replicated to majority, both index 2 and index 3 commit
		assert(env.node(0).machine.highestAppliedCommandIndex >= 2)
	}

	test("Figure 10: Joint consensus configuration transition with joint quorum") {
		// Cluster starts with Cold = {p-0, p-1, p-2}
		val env = new ConsensusEnvironment(clusterSize = 4)
		env.startNode(0)
		env.startNode(1)
		env.startNode(2)

		// Elect p-0 as initial leader
		val hInit = env.submitClientCommand(targetNode = 0, client = 1)
		env.runAllNodesUntilIdle()

		while env.nodeRole(0) != "LEADER" && env.pendingPackets.nonEmpty do {
			env.deliverAll()
			env.runAllNodesUntilIdle()
		}
		while env.pendingPackets.nonEmpty do {
			env.deliverAll()
			env.runAllNodesUntilIdle()
		}
		val ClientCommandStatus.Processed(_, resInit) = env.clientCommandStatus(hInit.commandId): @unchecked
		assertEquals(resInit, 1)

		// Start new joiner p-3
		env.startNode(3)

		// =========================================================================
		// ADVANCE EXECUTION FROM THIS POINT ONWARD
		// =========================================================================

		// Request membership transition from Cold {p-0, p-1, p-2} to Cnew {p-1, p-2, p-3}
		val ccHandle = env.submitConfigChange(targetNode = 0, desiredParticipants = Set(1, 2, 3))
		env.runAllNodesUntilIdle()

		// Drive the 2-phase joint consensus transition:
		// Phase 1: TransitionalConfigChange (Cold,new) replicated and committed across joint quorum
		// Phase 2: StableConfigChange (Cnew) replicated and committed
		while env.configChangeStatus(ccHandle.requestId) == ConfigChangeStatus.InFlight && env.pendingPackets.nonEmpty do {
			env.deliverAll()
			env.runAllNodesUntilIdle()
		}

		// The configuration change completed successfully
		env.configChangeStatus(ccHandle.requestId) match {
			case ConfigChangeStatus.Completed(resp) =>
				assert(resp.isInstanceOf[SUCCESSFULLY_CHANGED])
			case other =>
				fail(s"Expected SUCCESSFULLY_CHANGED, but got $other")
		}

		// The new configuration excludes p-0, which gracefully authorizes quiescence
		val p0Log = env.node(0).storage.savedMemory
		assert(p0Log.logBuffer.exists(_.isInstanceOf[TransitionalConfigChange[?]]))
		assert(p0Log.logBuffer.exists(_.isInstanceOf[StableConfigChange[?]]))
	}

	test("Figure 13: InstallSnapshot catch-up for lagging follower past compaction boundary") {
		// 3 nodes: p-0, p-1, p-2
		val env = new ConsensusEnvironment(clusterSize = 3)

		val initConfig = TransitionalConfigChange[String](
			1.asInstanceOf[Term],
			"cfg-0",
			Set.empty,
			env.defaultInitialParticipants
		)

		// Prepare valid serialized state machine snapshot data
		val smBytes = java.io.ByteArrayOutputStream()
		val smOut = java.io.ObjectOutputStream(smBytes)
		smOut.writeInt(0) // highestAppliedCommandSerial
		smOut.writeLong(5L) // highestAppliedCommandIndex
		smOut.flush()
		val snapshotData = IArray.unsafeFromArray(smBytes.toByteArray)

		// Seed leader p-0 with a snapshot covering entries 1 to 5, and logBufferOffset = 6
		val leaderMem = env.node(0).storage.savedMemory
		leaderMem.currentTerm = 1.asInstanceOf[Term]
		val snapshot = new SnapshotData[String](
			lastIncludedRecordIndex = 5,
			lastIncludedRecordTerm = 1.asInstanceOf[Term],
			latestConfigChange = initConfig,
			latestConfigChangeIndex = 1,
			stateMachineSnapshot = snapshotData
		)
		leaderMem.maybeLatestSnapshot = Maybe(snapshot)
		leaderMem._logBufferOffset = 6
		leaderMem.logBuffer.clear()
		// Entry at index 6 in plain buffer
		leaderMem.appendRecord(cmd(1, 6))

		// Leader p-0 and synchronized follower p-2 have already applied up through snapshot index 5
		env.node(0).machine.highestAppliedCommandIndex = 5
		env.node(2).machine.highestAppliedCommandIndex = 5

		// Follower p-2 is in sync with leader
		val mem2 = env.node(2).storage.savedMemory
		mem2.currentTerm = 1.asInstanceOf[Term]
		mem2.maybeLatestSnapshot = Maybe(snapshot)
		mem2._logBufferOffset = 6
		mem2.logBuffer.clear()
		mem2.appendRecord(cmd(1, 6))

		// Follower p-1 is severely lagging: only has initial empty log (logBufferOffset = 1, last record index = 0)
		val mem1 = env.node(1).storage.savedMemory
		mem1.currentTerm = 1.asInstanceOf[Term]
		mem1.logBuffer.clear()

		env.startAllNodes()

		// =========================================================================
		// ADVANCE EXECUTION FROM THIS POINT ONWARD
		// =========================================================================

		// Client submits command to p-0
		val handle = env.submitClientCommand(targetNode = 0, client = 1)
		env.runAllNodesUntilIdle()

		// Run leader election and initial synchronization between p-0 and p-2
		while env.nodeRole(0) != "LEADER" && env.pendingPackets.nonEmpty do {
			env.deliverAll()
			env.runAllNodesUntilIdle()
		}

		assertEquals(env.nodeRole(0), "LEADER")

		// Replicate to lagging follower p-1:
		// Because p-1 requires records predating p-0's logBufferOffset (6),
		// p-0 dispatches InstallSnapshot instead of AppendRecords.
		var snapshotTransmitted = false
		while env.pendingPackets.nonEmpty do {
			for packet <- env.pendingPackets do {
				if packet.summary.toLowerCase.contains("installsnapshot") then snapshotTransmitted = true
			}
			env.deliverAll()
			env.runAllNodesUntilIdle()
		}

		assert(snapshotTransmitted, "Expected InstallSnapshot RPC to be transmitted to lagging follower p-1")

		// Follower p-1 installed snapshot and updated its logBufferOffset to 6
		val finalMem1 = env.node(1).storage.savedMemory
		assertEquals(finalMem1.logBufferOffset, 6L)
		assertEquals(finalMem1.latestSnapshot.get.lastIncludedRecordIndex, 5L)

		// Follower p-1 also received the subsequent entry at index 6
		assertEquals(finalMem1.getRecordAt(6).term, 1.asInstanceOf[Term])
	}
}
