package readren.consensus

import munit.FunSuite
import readren.consensus.ConsensusParticipantSdm.*
import readren.consensus.env.*
import readren.consensus.protocol.*

class ConsensusEnvironmentUndoTest extends FunSuite {

	test("operation tracking records steps, dispatches, and client commands") {
		val env = new ConsensusEnvironment(clusterSize = 3)
		assertEquals(env.journal.appliedOperationsCount, 0)

		env.startAllNodes()
		assertEquals(env.journal.appliedOperationsCount, 1)
		assertEquals(env.journal.appliedOperations.last, EnvOperation.StartAllNodes)

		val handle = env.submitClientCommand(targetNode = 0, client = 1)
		assertEquals(env.journal.appliedOperationsCount, 2)

		env.stepNode(0)
		assertEquals(env.journal.appliedOperationsCount, 3)
		assertEquals(env.journal.appliedOperations.last, EnvOperation.StepNode(NodeId("p-0")))
		assertEquals(env.network.pendingPackets.size, 2)
	}

	test("undo rolls back discrete step and restored state") {
		val env = new ConsensusEnvironment(clusterSize = 3)
		env.startAllNodes()

		val handle = env.submitClientCommand(targetNode = 0, client = 1)
		env.stepNode(0)
		assertEquals(env.network.pendingPackets.size, 2)

		// Undo the stepNode(0)
		val undone = env.journal.undo()
		assert(undone)
		assertEquals(env.journal.appliedOperationsCount, 2)
		assertEquals(env.network.pendingPackets.size, 0)
		assertEquals(env.clientCommandStatus(handle.commandId), ClientCommandStatus.InFlight)
	}

	test("undo rolls back packet dispatch and restores packet to channel queue") {
		val env = new ConsensusEnvironment(clusterSize = 3)
		env.startAllNodes()

		env.submitClientCommand(targetNode = 0, client = 1)
		env.stepNode(0)
		assertEquals(env.network.pendingPackets.size, 2)

		val dispatchedOutcome = env.network.deliverNext(0, 1)
		assert(dispatchedOutcome.isDefined)
		assertEquals(env.network.pendingPackets.size, 1)

		// Undo the dispatch
		val undone = env.journal.undo()
		assert(undone)
		assertEquals(env.network.pendingPackets.size, 2)
		assert(env.network.pendingPackets.exists(_.destination == NodeId("p-1")))
		assert(env.network.pendingPackets.exists(_.destination == NodeId("p-2")))
	}

	test("undo rolls back dynamic settings") {
		val env = new ConsensusEnvironment(clusterSize = 3)
		env.startAllNodes()
		assertEquals(env.retiringParticipantMaxRetries, 2)

		env.updateDynamicSettings(retiringMaxRetries = Some(15))
		assertEquals(env.retiringParticipantMaxRetries, 15)

		val undone = env.journal.undo()
		assert(undone)
		assertEquals(env.retiringParticipantMaxRetries, 2)
	}

	test("tags allow creating checkpoints and restoring state branches") {
		val env = new ConsensusEnvironment(clusterSize = 3)
		env.startAllNodes()

		env.submitClientCommand(targetNode = 0, client = 1)
		env.stepNode(0)
		assertEquals(env.network.pendingPackets.size, 2)

		val checkpointOps = env.journal.appliedOperationsCount
		env.journal.createTag("checkpoint-1")
		assert(env.journal.tags.contains("checkpoint-1"))
		assertEquals(env.journal.tags("checkpoint-1").size, checkpointOps)

		// Further progress
		env.network.deliverNext(0, 1)
		env.stepNode(1)
		assert(env.journal.appliedOperationsCount > checkpointOps)

		// Restore checkpoint
		val restored = env.journal.restoreTag("checkpoint-1")
		assert(restored)
		assertEquals(env.journal.appliedOperationsCount, checkpointOps)
		assertEquals(env.network.pendingPackets.size, 2)

		// Delete tag
		val deleted = env.journal.deleteTag("checkpoint-1")
		assert(deleted)
		assert(!env.journal.tags.contains("checkpoint-1"))
	}

	test("undo on empty operations is safe no-op") {
		val env = new ConsensusEnvironment(clusterSize = 3, initializer = _.startAllNodes())
		env.reset()
		assertEquals(env.journal.appliedOperationsCount, 0)

		val undone = env.journal.undo()
		assert(!undone)
		assertEquals(env.journal.appliedOperationsCount, 0)
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
		assertEquals(env.journal.appliedOperationsCount, 0)
		assertEquals(env.nodeRole(0), "ISOLATED")
		assertEquals(env.nodeRole(1), "ISOLATED")
		assertEquals(env.nodeRole(2), "DOWN")

		// Apply an operation
		env.submitClientCommand(0, 1)
		assertEquals(env.journal.appliedOperationsCount, 1)

		env.stepNode(0)
		assertEquals(env.journal.appliedOperationsCount, 2)

		// Undo stepNode(0)
		env.journal.undo()
		assertEquals(env.journal.appliedOperationsCount, 1)
		assertEquals(env.nodeRole(2), "DOWN")

		// Undo submitClientCommand
		env.journal.undo()
		assertEquals(env.journal.appliedOperationsCount, 0)
		assertEquals(env.nodeRole(0), "ISOLATED")
		assertEquals(env.nodeRole(1), "ISOLATED")
		assertEquals(env.nodeRole(2), "DOWN")
	}

	test("redo reapplies undone operation and restores environment state") {
		val env = new ConsensusEnvironment(clusterSize = 3)
		env.startAllNodes()

		val handle = env.submitClientCommand(targetNode = 0, client = 1)
		env.stepNode(0)
		assertEquals(env.journal.appliedOperationsCount, 3)
		assertEquals(env.journal.undoneOperationsCount, 0)
		assertEquals(env.network.pendingPackets.size, 2)

		// Undo stepNode(0)
		assert(env.journal.undo())
		assertEquals(env.journal.appliedOperationsCount, 2)
		assertEquals(env.journal.undoneOperationsCount, 1)
		assertEquals(env.network.pendingPackets.size, 0)

		// Redo stepNode(0)
		assert(env.journal.redo())
		assertEquals(env.journal.appliedOperationsCount, 3)
		assertEquals(env.journal.undoneOperationsCount, 0)
		assertEquals(env.network.pendingPackets.size, 2)
	}

	test("multiple undo and multiple redo preserve LIFO order") {
		val env = new ConsensusEnvironment(clusterSize = 3)
		env.startAllNodes()
		env.submitClientCommand(targetNode = 0, client = 1)
		env.stepNode(0)
		assertEquals(env.journal.appliedOperationsCount, 3)

		assert(env.journal.undo()) // undo stepNode
		assertEquals(env.journal.appliedOperationsCount, 2)
		assertEquals(env.journal.undoneOperationsCount, 1)

		assert(env.journal.undo()) // undo submitClientCommand
		assertEquals(env.journal.appliedOperationsCount, 1)
		assertEquals(env.journal.undoneOperationsCount, 2)

		assert(env.journal.redo()) // redo submitClientCommand
		assertEquals(env.journal.appliedOperationsCount, 2)
		assertEquals(env.journal.undoneOperationsCount, 1)

		assert(env.journal.redo()) // redo stepNode
		assertEquals(env.journal.appliedOperationsCount, 3)
		assertEquals(env.journal.undoneOperationsCount, 0)
		assertEquals(env.network.pendingPackets.size, 2)
	}

	test("new action clears redo stack") {
		val env = new ConsensusEnvironment(clusterSize = 3)
		env.startAllNodes()
		env.submitClientCommand(targetNode = 0, client = 1)
		env.stepNode(0)

		assert(env.journal.undo())
		assertEquals(env.journal.undoneOperationsCount, 1)

		// Applying a new action must invalidate the redo stack
		env.clock.advanceTime(10)
		assertEquals(env.journal.appliedOperationsCount, 3)
		assertEquals(env.journal.undoneOperationsCount, 0)
		assert(!env.journal.redo())
	}

	test("tags include redo memory and restore undone operations") {
		val env = new ConsensusEnvironment(clusterSize = 3)
		env.startAllNodes()
		env.submitClientCommand(targetNode = 0, client = 1)
		env.stepNode(0)

		assert(env.journal.undo())
		assertEquals(env.journal.appliedOperationsCount, 2)
		assertEquals(env.journal.undoneOperationsCount, 1)

		// Checkpoint tag captures both applied and undone operations
		env.journal.createTag("branch-with-redo")
		val tag = env.journal.tags("branch-with-redo")
		assertEquals(tag.appliedOperations.size, 2)
		assertEquals(tag.undoneOperations.size, 1)

		// Progress further by applying a different action
		env.clock.advanceTime(20)
		assertEquals(env.journal.appliedOperationsCount, 3)
		assertEquals(env.journal.undoneOperationsCount, 0)

		// Restore tag: should restore applied operations and redo stack
		assert(env.journal.restoreTag("branch-with-redo"))
		assertEquals(env.journal.appliedOperationsCount, 2)
		assertEquals(env.journal.undoneOperationsCount, 1)

		// Redo should now be possible from the restored checkpoint
		assert(env.journal.redo())
		assertEquals(env.journal.appliedOperationsCount, 3)
		assertEquals(env.journal.undoneOperationsCount, 0)
		assertEquals(env.network.pendingPackets.size, 2)
	}

	test("redo on empty redo stack is safe no-op") {
		val env = new ConsensusEnvironment(clusterSize = 3)
		env.startAllNodes()
		assertEquals(env.journal.undoneOperationsCount, 0)
		assert(!env.journal.redo())
	}
}
