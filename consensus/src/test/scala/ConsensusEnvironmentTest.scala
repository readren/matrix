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

		// Dispatch discovery requests to peers
		env.dispatchNext(0, 1)
		env.dispatchNext(0, 2)

		// Peers process HowAreYou RPCs and queue responses
		env.stepNode(1)
		env.stepNode(2)

		val responsesToDiscovery = env.pendingPackets
		assertEquals(responsesToDiscovery.size, 2)

		// Dispatch responses back to p-0
		env.dispatchAllTo(0)

		// p-0 processes responses and initiates election (ChooseALeader)
		env.stepNode(0)
		env.stepNode(0)

		val voteRequests = env.pendingPackets
		assertEquals(voteRequests.size, 2)

		// Dispatch vote requests to peers
		env.dispatchNext(0, 1)
		env.dispatchNext(0, 2)

		// Peers cast votes for p-0
		env.stepNode(1)
		env.stepNode(2)

		// Dispatch votes back to p-0
		env.dispatchAllTo(0)

		// p-0 processes votes, becomes leader, and appends the pending command
		env.runNodeUntilIdle(0)
		assertEquals(env.nodeRole(0), "LEADER")

		// Leader p-0 sends AppendRecords to p-1 and p-2
		val appendPackets = env.pendingPackets
		assertEquals(appendPackets.size, 2)

		// Dispatch AppendRecords to followers
		env.dispatchNext(0, 1)
		env.dispatchNext(0, 2)

		// Followers process AppendRecords and save to storage
		env.stepNode(1)
		env.stepNode(2)

		// Followers respond with append success
		val appendReplies = env.pendingPackets
		assertEquals(appendReplies.size, 2)

		// Dispatch replies back to leader p-0
		env.dispatchAllTo(0)

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

		// Dispatch discovery and election
		while env.pendingPackets.nonEmpty do {
			env.dispatchAll()
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
		env.dispatchNext(0, 1)
		env.dispatchNext(0, 2)

		env.stepNode(1)
		env.stepNode(2)

		// Responses p-1 -> p-0 and p-2 -> p-0 are in flight
		assertEquals(env.pendingPackets.size, 2)

		// DROP the response from p-1 to p-0
		val dropped = env.dropNext(1, 0)
		assert(dropped)
		assertEquals(env.pendingPackets.size, 1)

		// Dispatch the response from p-2 to p-0
		env.dispatchNext(2, 0)

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
		for _ <- 0 until 5 do {
			env.runAllNodesUntilIdle()
			env.dispatchAll()
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
		for _ <- 0 until 5 do {
			env.runAllNodesUntilIdle()
			env.dispatchAll()
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
}
