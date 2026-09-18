package readren.consensus

import munit.FunSuite
import readren.consensus.ConsensusParticipantSdm.*

class ConsensusEnvironmentUndoTest extends FunSuite {

	test("operation tracking records steps, dispatches, and client commands") {
		val env = new ConsensusEnvironment(clusterSize = 3)
		assertEquals(env.appliedOperationsCount, 0)

		env.startAllNodes()
		assertEquals(env.appliedOperationsCount, 1)
		assertEquals(env.appliedOperations.last, EnvOperation.StartAllNodes)

		val handle = env.submitClientCommand(targetNode = 0, client = 1)
		assertEquals(env.appliedOperationsCount, 2)

		env.stepNode(0)
		assertEquals(env.appliedOperationsCount, 3)
		assertEquals(env.appliedOperations.last, EnvOperation.StepNode("p-0"))
		assertEquals(env.pendingPackets.size, 2)
	}

	test("undo rolls back discrete step and restored state") {
		val env = new ConsensusEnvironment(clusterSize = 3)
		env.startAllNodes()

		val handle = env.submitClientCommand(targetNode = 0, client = 1)
		env.stepNode(0)
		assertEquals(env.pendingPackets.size, 2)

		// Undo the stepNode(0)
		val undone = env.undo()
		assert(undone)
		assertEquals(env.appliedOperationsCount, 2)
		assertEquals(env.pendingPackets.size, 0)
		assertEquals(env.clientCommandStatus(handle.commandId), ClientCommandStatus.InFlight)
	}

	test("undo rolls back packet dispatch and restores packet to channel queue") {
		val env = new ConsensusEnvironment(clusterSize = 3)
		env.startAllNodes()

		env.submitClientCommand(targetNode = 0, client = 1)
		env.stepNode(0)
		assertEquals(env.pendingPackets.size, 2)

		val dispatchedOutcome = env.deliverNext(0, 1)
		assert(dispatchedOutcome.isDefined)
		assertEquals(env.pendingPackets.size, 1)

		// Undo the dispatch
		val undone = env.undo()
		assert(undone)
		assertEquals(env.pendingPackets.size, 2)
		assert(env.pendingPackets.exists(_.destination == "p-1"))
		assert(env.pendingPackets.exists(_.destination == "p-2"))
	}

	test("undo rolls back dynamic settings") {
		val env = new ConsensusEnvironment(clusterSize = 3)
		env.startAllNodes()
		assertEquals(env.retiringParticipantMaxRetries, 2)

		env.updateDynamicSettings(retiringMaxRetries = Some(15))
		assertEquals(env.retiringParticipantMaxRetries, 15)

		val undone = env.undo()
		assert(undone)
		assertEquals(env.retiringParticipantMaxRetries, 2)
	}

	test("tags allow creating checkpoints and restoring state branches") {
		val env = new ConsensusEnvironment(clusterSize = 3)
		env.startAllNodes()

		env.submitClientCommand(targetNode = 0, client = 1)
		env.stepNode(0)
		assertEquals(env.pendingPackets.size, 2)

		val checkpointOps = env.appliedOperationsCount
		env.createTag("checkpoint-1")
		assert(env.tags.contains("checkpoint-1"))
		assertEquals(env.tags("checkpoint-1").size, checkpointOps)

		// Further progress
		env.deliverNext(0, 1)
		env.stepNode(1)
		assert(env.appliedOperationsCount > checkpointOps)

		// Restore checkpoint
		val restored = env.restoreTag("checkpoint-1")
		assert(restored)
		assertEquals(env.appliedOperationsCount, checkpointOps)
		assertEquals(env.pendingPackets.size, 2)

		// Delete tag
		val deleted = env.deleteTag("checkpoint-1")
		assert(deleted)
		assert(!env.tags.contains("checkpoint-1"))
	}

	test("undo on empty operations is safe no-op") {
		val env = new ConsensusEnvironment(clusterSize = 3, initializer = _.startAllNodes())
		env.reset()
		assertEquals(env.appliedOperationsCount, 0)

		val undone = env.undo()
		assert(!undone)
		assertEquals(env.appliedOperationsCount, 0)
	}

	test("preset baseline is preserved across undo and tag restoration") {
		var presetInitCount = 0
		val presetInitializer: ConsensusEnvironment => Unit = e => {
			presetInitCount += 1
			e.startNode(0)
			e.startNode(1)
		}

		val env = new ConsensusEnvironment(clusterSize = 3, initializer = presetInitializer)
		env.reset()
		assertEquals(presetInitCount, 1)
		assertEquals(env.appliedOperationsCount, 0)
		assertEquals(env.nodeRole(0), "ISOLATED")
		assertEquals(env.nodeRole(1), "ISOLATED")
		assertEquals(env.nodeRole(2), "DOWN")

		// Apply an operation
		env.submitClientCommand(0, 1)
		assertEquals(env.appliedOperationsCount, 1)

		env.stepNode(0)
		assertEquals(env.appliedOperationsCount, 2)

		// Undo stepNode(0)
		env.undo()
		assertEquals(env.appliedOperationsCount, 1)
		assertEquals(env.nodeRole(2), "DOWN")

		// Undo submitClientCommand
		env.undo()
		assertEquals(env.appliedOperationsCount, 0)
		assertEquals(env.nodeRole(0), "ISOLATED")
		assertEquals(env.nodeRole(1), "ISOLATED")
		assertEquals(env.nodeRole(2), "DOWN")
	}
}
