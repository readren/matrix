package readren.consensus

import munit.FunSuite
import readren.consensus.ConsensusParticipantSdm.*

class ConsensusEnvironmentTest extends FunSuite {

	test("cluster startup and initial leader discovery, election, and command replication") {
		val env = new ConsensusEnvironment(clusterSize = 3)
		env.startAllNodes()

		// Nodes are initially in isolated or starting state
		assertEquals(env.pendingPackets.size, 0)

		// Client c-1 sends command to node p-0
		val handle = env.submitClientCommand(targetNode = 0, client = 1)
		assertEquals(env.clientCommandStatus(handle.commandId), ClientCommandStatus.InFlight)

		// Step p-0 to process the incoming client command: p-0 initiates leader discovery (HowAreYou to p-1, p-2)
		env.stepNode(0)
		val discoveryPackets = env.pendingPackets
		assertEquals(discoveryPackets.size, 2)
		assert(discoveryPackets.forall(_.source == "p-0"))

		// Deliver discovery requests to peers
		env.deliverNext(0, 1)
		env.deliverNext(0, 2)

		// Peers process HowAreYou RPCs and queue responses
		env.stepNode(1)
		env.stepNode(2)

		val responsesToDiscovery = env.pendingPackets
		assertEquals(responsesToDiscovery.size, 2)

		// Deliver responses back to p-0
		env.deliverAllTo(0)

		// p-0 processes responses and initiates election (ChooseALeader)
		env.stepNode(0)
		env.stepNode(0)

		val voteRequests = env.pendingPackets
		assertEquals(voteRequests.size, 2)

		// Deliver vote requests to peers
		env.deliverNext(0, 1)
		env.deliverNext(0, 2)

		// Peers cast votes for p-0
		env.stepNode(1)
		env.stepNode(2)

		// Deliver votes back to p-0
		env.deliverAllTo(0)

		// p-0 processes votes, becomes leader, and appends the pending command
		env.runNodeUntilIdle(0)
		assertEquals(env.nodeRole(0), "LEADER")

		// Leader p-0 sends AppendRecords to p-1 and p-2 (initial config wave + client command wave)
		val appendPackets = env.pendingPackets
		assertEquals(appendPackets.size, 4)

		// Deliver AppendRecords to followers
		env.deliverAllTo(1)
		env.deliverAllTo(2)

		// Followers process AppendRecords and save to storage
		env.runNodeUntilIdle(1)
		env.runNodeUntilIdle(2)

		// Followers respond with append success
		val appendReplies = env.pendingPackets
		assertEquals(appendReplies.size, 4)

		// Deliver replies back to leader p-0
		env.deliverAllTo(0)

		// Leader processes quorum replication, commits, applies to state machine, and finishes client command
		env.runNodeUntilIdle(0)

		val ClientCommandStatus.Processed(_, res) = env.clientCommandStatus(handle.commandId): @unchecked
		assertEquals(res, 1)
	}

	test("dropped response packet allows quorum progress") {
		val env = new ConsensusEnvironment(clusterSize = 3)
		env.startAllNodes()

		// Submit initial command to elect p-0 as leader
		val h1 = env.submitClientCommand(0, 1)
		env.runAllNodesUntilIdle()

		// Deliver discovery and election
		while env.pendingPackets.nonEmpty do {
			env.deliverAll()
			env.runAllNodesUntilIdle()
		}

		assertEquals(env.nodeRole(0), "LEADER")
		val ClientCommandStatus.Processed(_, res1) = env.clientCommandStatus(h1.commandId): @unchecked
		assertEquals(res1, 1)

		// Submit second command to leader p-0
		val h2 = env.submitClientCommand(0, 1)
		env.runNodeUntilIdle(0)

		// p-0 sent AppendRecords to p-1 and p-2
		assertEquals(env.pendingPackets.size, 2)
		env.deliverNext(0, 1)
		env.deliverNext(0, 2)

		env.stepNode(1)
		env.stepNode(2)

		// Responses p-1 -> p-0 and p-2 -> p-0 are in flight
		assertEquals(env.pendingPackets.size, 2)

		// DROP the response from p-1 to p-0
		val dropped = env.dropNext(1, 0)
		assert(dropped)
		assertEquals(env.pendingPackets.size, 1)

		// Deliver the response from p-2 to p-0
		env.deliverNext(2, 0)

		// Leader p-0 processes p-2's ack: p-0 + p-2 form majority of 3
		env.runNodeUntilIdle(0)

		// Command commits despite p-1 response packet drop
		val ClientCommandStatus.Processed(_, res2) = env.clientCommandStatus(h2.commandId): @unchecked
		assertEquals(res2, 2)
	}

	test("threshold gated persistence pauses and resumes on demand") {
		val env = new ConsensusEnvironment(clusterSize = 3)
		env.startAllNodes()

		// Configure p-0 to gate persistence at record index 1
		env.node(0).autoSucceedUntilRecordIndex = 1

		// Submit command to p-0
		val h1 = env.submitClientCommand(0, 1)

		// Run through discovery & election
		while env.pendingPersistenceOperations.isEmpty && (env.pendingPackets.nonEmpty || env.nodeRole(0) != "LEADER") do {
			env.runAllNodesUntilIdle()
			env.deliverAll()
		}

		// When p-0 becomes leader and attempts to save record at index 1 to storage, it exceeds threshold
		env.runNodeUntilIdle(0)

		val pendingOps = env.pendingPersistenceOperations
		assertEquals(pendingOps.size, 1)
		assertEquals(pendingOps.head.nodeId, "p-0")

		// Complete the pending persistence explicitly
		val completed = env.completeNextStorageSave(0)
		assert(completed)
		assertEquals(env.pendingPersistenceOperations.size, 0)

		// Restore auto-persistence threshold so subsequent saves proceed
		env.node(0).autoSucceedUntilRecordIndex = Long.MaxValue

		// Now replication proceeds
		env.runAllNodesUntilIdle()
		while env.pendingPackets.nonEmpty do {
			env.deliverAll()
			env.runAllNodesUntilIdle()
		}

		assertEquals(env.nodeRole(0), "LEADER")
		val ClientCommandStatus.Processed(_, resFinal) = env.clientCommandStatus(h1.commandId): @unchecked
		assertEquals(resFinal, 1)
	}

	test("virtual clock and wake-up progression") {
		val env = new ConsensusEnvironment(clusterSize = 3, ticksPerMilli = 10)
		assertEquals(env.currentVirtualTime, 0)

		var wakeUpFired = false
		val token = env.registerWakeUp(env.node(0), WakeUpReason.UnreachableFollowersRetry, 0, () => {
			wakeUpFired = true
		})

		// Wakeup scheduled at 10ms * (0 + 1) * 10 = 100 ticks
		assertEquals(env.pendingWakeUps.size, 1)
		assertEquals(env.pendingWakeUps.head.scheduledTime, 100)

		// Advance 50 ticks: not yet expired
		val expired1 = env.advanceTime(50)
		assertEquals(expired1.size, 0)
		assertEquals(env.currentVirtualTime, 50)
		assert(!wakeUpFired)

		// Advance another 50 ticks: expires at 100
		val expired2 = env.advanceTime(50)
		assertEquals(expired2.size, 1)
		assertEquals(env.currentVirtualTime, 100)

		// Callback was enqueued to p-0's StepDoer
		env.stepNode(0)
		assert(wakeUpFired)
	}

	test("fenced voter rejects stale leader append preventing split-brain commit across terms") {
		val env = new ConsensusEnvironment(clusterSize = 3)
		env.startAllNodes()

		// 1. Initial startup: elect p-0 as leader of term 1 and commit command h1
		val h1 = env.submitClientCommand(targetNode = 0, client = 1)
		env.runAllNodesUntilIdle()
		while env.pendingPackets.nonEmpty do {
			env.deliverAll()
			env.runAllNodesUntilIdle()
		}

		assertEquals(env.nodeRole(0), "LEADER")
		val ClientCommandStatus.Processed(_, res1) = env.clientCommandStatus(h1.commandId): @unchecked
		assertEquals(res1, 1)

		// 2. Client submits command h2 to leader p-0
		val h2 = env.submitClientCommand(targetNode = 0, client = 1)
		env.runNodeUntilIdle(0)

		// p-0 appends command h2 at index 3 in term 1 and queues AppendRecords to p-1 and p-2
		assert(env.pendingPacketsBetween(0, 1).nonEmpty)
		assert(env.pendingPacketsBetween(0, 2).nonEmpty)

		// Simulate packet loss / partition from p-0 to p-1
		env.dropNext(0, 1)

		// 3. Client submits command h3 with FALLBACK to follower p-1, prompting election / role update
		val h3 = env.submitClientCommand(targetNode = 1, client = 2, attemptFlag = FALLBACK)
		env.stepNode(1)

		// Drop discovery queries to p-0 (simulating partition from p-0)
		while env.pendingPacketsBetween(1, 0).nonEmpty do env.dropNext(1, 0)
		while env.pendingPacketsBetween(1, 2).nonEmpty do env.deliverNext(1, 2)
		env.stepNode(2)

		while env.pendingPacketsBetween(2, 0).nonEmpty do env.dropNext(2, 0)
		while env.pendingPacketsBetween(2, 1).nonEmpty do env.deliverNext(2, 1)
		env.runNodeUntilIdle(1)

		// 4. p-1 launches Phase 2 (ChooseALeader)
		while env.pendingPacketsBetween(1, 0).nonEmpty do env.dropNext(1, 0)
		while env.pendingPacketsBetween(1, 2).nonEmpty do env.deliverNext(1, 2)
		env.stepNode(2)

		// p-2 sends discovery to p-0: drop it and step p-2 so it completes discovery and casts vote
		while env.pendingPacketsBetween(2, 0).nonEmpty do env.dropNext(2, 0)
		env.runNodeUntilIdle(2)
		while env.pendingPacketsBetween(2, 0).nonEmpty do env.dropNext(2, 0)

		// Deliver p-2's vote back to p-1
		while env.pendingPacketsBetween(2, 1).nonEmpty do env.deliverNext(2, 1)
		env.runNodeUntilIdle(1)

		// p-1 receives p-2's vote, enters Promoting, and becomes leader of term 2
		assertEquals(env.nodeRole(1), "LEADER")
		assertEquals(env.node(1).storage.savedMemory.currentTerm, 2.asInstanceOf[Term])
		assertEquals(env.node(2).storage.savedMemory.currentTerm, 2.asInstanceOf[Term])
		assertEquals(env.node(2).storage.savedMemory.getVotedFor, readren.common.Maybe("p-1"))

		// p-1 has queued AppendRecords for command h3 (term 2) to p-2. Hold it in flight!
		assert(env.pendingPacketsBetween(1, 2).exists(_.rpcKind == "APR"))

		// 5. Fatal race averted: deliver p-0's delayed AppendRecords (command h2, term 1) to p-2
		// Because p-2 epoch-fenced term 1 when voting for p-1, p-2 rejects p-0's append at term 1!
		env.deliverNext(0, 2)
		env.runNodeUntilIdle(2)

		// Deliver p-2's rejection back to p-0
		env.deliverNext(2, 0)
		env.runNodeUntilIdle(0)

		// p-0 receives p-2's rejection: p-0 discovers term 2, steps down, and does NOT commit index 3
		assert(env.clientCommandStatus(h2.commandId) != ClientCommandStatus.Processed)
		assertEquals(env.nodeRole(0), "ISOLATED")

		// 6. Now deliver p-1's AppendRecords (command h3, term 2) to p-2
		env.deliverNext(1, 2)
		env.stepNode(2)

		// Deliver p-2's acknowledgment back to p-1
		env.deliverNext(2, 1)
		env.runNodeUntilIdle(1)

		// p-1 receives p-2's ack: p-1 + p-2 forms majority in term 2!
		val ClientCommandStatus.Processed(recIdx3, res3) = env.clientCommandStatus(h3.commandId): @unchecked
		assertEquals(res3, 1)
		val p1Record = env.node(1).storage.savedMemory.getRecordAt(recIdx3)
		assertEquals(p1Record.term, 2.asInstanceOf[Term])
	}
}
