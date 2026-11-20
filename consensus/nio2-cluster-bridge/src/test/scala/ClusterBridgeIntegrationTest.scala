package readren.consensus
package nio2

import munit.FunSuite
import readren.common.{BufferByteSource, FileByteSource, Maybe}
import readren.consensus.protocol.*
import readren.nexus.cluster.misc.RingLength
import readren.nexus.cluster.serialization.*
import readren.nexus.cluster.serialization.CommonSerializers.given
import readren.sequencer.SchedulingDoer
import readren.sequencer.providers.CooperativeThreadDrivenSchedulerDp

import java.net.{InetSocketAddress, SocketAddress}
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.file.Files
import java.util.Comparator
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import scala.collection.immutable.ListSet
import scala.collection.mutable
import scala.compiletime.uninitialized
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration.*
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

class ClusterBridgeIntegrationTest extends FunSuite {

	override val munitTimeout: Duration = 60.seconds

	private val strSer: Serializer[String] = summon
	private val strDeser: Deserializer[String] = summon

	private inline def term(i: Int): Term = i.asInstanceOf[Term]

	// -------------------------------------------------------------------------
	// Multi-Node Loopback TCP Test Harness
	// -------------------------------------------------------------------------

	class TestNode(
		val nodeId: String,
		peerAddressLookup: String => SocketAddress,
		val schedulerDp: CooperativeThreadDrivenSchedulerDp.Impl,
		val initialElectorate: Set[String],
		val customCompactionThreshold: Int = 1000,
		val customRetentionAfterSnapshot: Int = 10,
		val remembersLastAppliedCommandIndex: Boolean = true,
		val customBridgeConfig: Option[ClusterBridgeConfig] = None
	) extends ConsensusParticipantSdm with Nio2ClusterBridgeCdm { thisNode =>

		override type ParticipantId = String
		override val participantIdComparator: Comparator[ParticipantId] = Ordering.String
		override type ClientId = String
		override type ClientCommand = String
		override type StateMachineResponse = String
		override type WS = MemoryWorkspace

		override val sequencer: SchedulingDoer = schedulerDp.provide(s"sequencer-$nodeId")

		override val logCompactionThreshold: Int = customCompactionThreshold
		override def logRetentionAfterSnapshot: Int = customRetentionAfterSnapshot

		override given participantIdSerializer: Serializer[ParticipantId] = strSer
		override given participantIdDeserializer: Deserializer[ParticipantId] = strDeser
		override given clientCommandSerializer: Serializer[ClientCommand] = strSer
		override given clientCommandDeserializer: Deserializer[ClientCommand] = strDeser
		override given stateMachineResponseSerializer: Serializer[StateMachineResponse] = strSer
		override given stateMachineResponseDeserializer: Deserializer[StateMachineResponse] = strDeser

		override def contactAddressOf(id: ParticipantId): SocketAddress = peerAddressLookup(id)
		override def localBindAddress: SocketAddress = new InetSocketAddress("127.0.0.1", 0)

		override val bridgeConfig: ClusterBridgeConfig = customBridgeConfig.getOrElse(
			ClusterBridgeConfig(
				fastRpcTimeout = 4000L,
				snapshotRpcTimeout = 10000L,
				clientRpcTimeout = 5000L,
				connectTimeout = 4000L,
				unreachableFollowersRetryDelay = 100L,
				replicationLoopRetryDelay = 50L
			)
		)

		var bridge: Nio2ClusterBridge = new Nio2ClusterBridge(nodeId)

		val stateMachine: TestStateMachine = new TestStateMachine()
		val workspace: MemoryWorkspace = new MemoryWorkspace()
		val storage: MemoryStorage = new MemoryStorage(workspace)

		val listener: TestNotificationListener = new TestNotificationListener(nodeId)

		private var _participant: ConsensusParticipant = uninitialized
		def participant: ConsensusParticipant = _participant

		def startParticipant(): Unit = {
			val maybeSeeds = if initialElectorate.contains(nodeId) then Maybe(initialElectorate) else Maybe.empty
			val p = Promise[Unit]()
			sequencer.run {
				_participant = new ConsensusParticipant(bridge, storage, stateMachine, maybeSeeds, List(listener))
				p.success(())
			}
			Await.result(p.future, 5.seconds)
		}

		def stop(): Unit = {
			bridge.close()
			if _participant != null then {
				val p = Promise[Unit]()
				sequencer.run {
					_participant.dispose().andThen(
						_ => p.success(()),
						e => p.failure(e)
					)
				}
				Try(Await.result(p.future, 3.seconds))
				_participant = null
			}
		}

		def restartWithFreshBridge(registeredAddressCallback: SocketAddress => Unit): Unit = {
			bridge.close()
			bridge = new Nio2ClusterBridge(nodeId)
			bridge.start()
			registeredAddressCallback(bridge.localAddress)
			val p = Promise[Unit]()
			sequencer.run {
				_participant = new ConsensusParticipant(bridge, storage, stateMachine, Maybe.empty, List(listener))
				p.success(())
			}
			Await.result(p.future, 5.seconds)
		}

		class MemoryWorkspace extends Workspace {
			private var currentTerm: Term = PRE_INIT
			private var _votedFor: Maybe[ParticipantId] = Maybe.empty
			val logBuffer: mutable.ArrayBuffer[Record] = mutable.ArrayBuffer.empty
			private var _logBufferOffset: RecordIndex = 1
			private var maybeLatestSnapshot: Maybe[SnapshotData[ParticipantId]] = Maybe.empty

			override def latestSnapshot: Maybe[SnapshotData[ParticipantId]] = {
				sequencer.checkWithin()
				maybeLatestSnapshot
			}

			override def getCurrentTerm: Term = {
				sequencer.checkWithin()
				currentTerm
			}

			override def setCurrentTerm(term: Term): Unit = {
				sequencer.checkWithin()
				if term != currentTerm then _votedFor = Maybe.empty
				currentTerm = term
			}

			override def getVotedFor: Maybe[ParticipantId] = {
				sequencer.checkWithin()
				_votedFor
			}

			override def setVotedFor(votedFor: Maybe[ParticipantId]): Unit = {
				sequencer.checkWithin()
				_votedFor = votedFor
			}

			override def setTermAndVote(term: Term, votedFor: Maybe[ParticipantId]): Unit = {
				sequencer.checkWithin()
				currentTerm = term
				_votedFor = votedFor
			}

			override def logBufferOffset: RecordIndex = {
				sequencer.checkWithin()
				_logBufferOffset
			}

			override def firstEmptyRecordIndex: RecordIndex = {
				sequencer.checkWithin()
				_logBufferOffset + logBuffer.size
			}

			override def getRecordAt(index: RecordIndex): Record = {
				sequencer.checkWithin()
				logBuffer((index - _logBufferOffset).toInt)
			}

			override def getRecordsBetween(from: RecordIndex, until: RecordIndex): IArray[Record] = {
				sequencer.checkWithin()
				val fromIndex = (from - _logBufferOffset).toInt
				val untilIndex = (until - _logBufferOffset).toInt
				val len = untilIndex - fromIndex
				if len <= 0 then IArray.empty
				else {
					val array = new Array[Record](len)
					Array.copy(logBuffer.toArray, fromIndex, array, 0, len)
					IArray.unsafeFromArray(array)
				}
			}

			override def appendRecord(record: Record): Unit = {
				sequencer.checkWithin()
				logBuffer.addOne(record)
			}

			override def truncateSuffix(fromIndex: RecordIndex): Unit = {
				sequencer.checkWithin()
				val writeIndex = (fromIndex - _logBufferOffset).toInt
				if writeIndex < logBuffer.size then {
					logBuffer.takeInPlace(writeIndex)
				}
			}

			override def resetLog(snapshot: SnapshotData[ParticipantId], tailRecords: IArray[Record]): Unit = {
				sequencer.checkWithin()
				maybeLatestSnapshot.foreach { old =>
					if old.stateMachineSnapshot ne snapshot.stateMachineSnapshot then old.stateMachineSnapshot.close()
				}
				maybeLatestSnapshot = Maybe(snapshot)
				val newOffset = snapshot.lastIncludedRecordIndex + 1
				_logBufferOffset = newOffset
				logBuffer.clear()
				logBuffer.addAll(tailRecords)
			}

			override def truncatePrefix(snapshot: SnapshotData[ParticipantId]): Unit = {
				sequencer.checkWithin()
				val newOffset = snapshot.lastIncludedRecordIndex + 1
				val dropCount = (newOffset - _logBufferOffset).toInt
				if dropCount > 0 then {
					if dropCount >= logBuffer.size then logBuffer.clear()
					else logBuffer.dropInPlace(dropCount)
					_logBufferOffset = newOffset
				}
				maybeLatestSnapshot.foreach { old =>
					if old.stateMachineSnapshot ne snapshot.stateMachineSnapshot then old.stateMachineSnapshot.close()
				}
				maybeLatestSnapshot = Maybe(snapshot)
			}

			override def release(): sequencer.Capture[Unit] = {
				sequencer.checkWithin()
				maybeLatestSnapshot.foreach(_.stateMachineSnapshot.close())
				maybeLatestSnapshot = Maybe.empty
				sequencer.Capture_unit
			}
		}

		class MemoryStorage(var memory: MemoryWorkspace) extends Storage {
			override def load: sequencer.Capture[WS] = {
				sequencer.checkWithin()
				sequencer.Keeper(memory)
			}

			override def save(workspace: WS): sequencer.Capture[Unit] = {
				sequencer.checkWithin()
				memory = workspace
				sequencer.Keeper(())
			}
		}

		class TestStateMachine extends StateMachine {
			val appliedCommands: mutable.Buffer[(RecordIndex, ClientCommand)] = mutable.ArrayBuffer.empty
			@volatile var highestAppliedIndex: RecordIndex = 0L
			@volatile var useFileSnapshot: Boolean = false

			override def applyClientCommand(index: RecordIndex, command: ClientCommand): sequencer.Capture[StateMachineResponse] = {
				sequencer.checkWithin()
				appliedCommands.addOne((index, command))
				if index > highestAppliedIndex then highestAppliedIndex = index
				sequencer.Keeper(s"ack-$command")
			}

			override def recoverIndexOfLastAppliedCommand: sequencer.Capture[RecordIndex] = {
				sequencer.checkWithin()
				if remembersLastAppliedCommandIndex then sequencer.Keeper(highestAppliedIndex)
				else sequencer.Keeper(0L)
			}

			override def takeSnapshot(): sequencer.Capture[SnapshotByteSource] = {
				sequencer.checkWithin()
				val baos = new java.io.ByteArrayOutputStream()
				val oos = new java.io.ObjectOutputStream(baos)
				oos.writeLong(highestAppliedIndex)
				oos.writeInt(appliedCommands.size)
				for (idx, cmd) <- appliedCommands do {
					oos.writeLong(idx)
					oos.writeUTF(cmd)
				}
				oos.flush()
				val bytes = baos.toByteArray
				if useFileSnapshot then {
					val tempFile = Files.createTempFile("state-machine-snapshot-", ".bin")
					tempFile.toFile.deleteOnExit()
					Files.write(tempFile, bytes)
					sequencer.Keeper(new FileByteSource(tempFile, deleteOnClose = true))
				} else {
					sequencer.Keeper(new BufferByteSource(ByteBuffer.wrap(bytes)))
				}
			}

			override def installSnapshot(data: SnapshotByteSource): sequencer.Capture[Unit] = {
				sequencer.checkWithin()
				val channel = data.openChannel()
				val in = new java.io.ObjectInputStream(Channels.newInputStream(channel))
				highestAppliedIndex = in.readLong()
				val count = in.readInt()
				appliedCommands.clear()
				var i = 0
				while i < count do {
					val idx = in.readLong()
					val cmd = in.readUTF()
					appliedCommands.addOne((idx, cmd))
					i += 1
				}
				in.close()
				channel.close()
				data.close()
				sequencer.Capture_unit
			}
		}

		class TestNotificationListener(val targetNodeId: String) extends DefaultNotificationListener {
			@volatile var leaderTerm: Maybe[Term] = Maybe.empty
			@volatile var currentRole: RoleOrdinal = STARTING
			@volatile var followee: Maybe[ParticipantId] = Maybe.empty
			@volatile var commitIndex: RecordIndex = 0L
			@volatile var highestAppliedIndex: RecordIndex = 0L
			@volatile var isQuiesced: Boolean = false

			val leaderPromise: Promise[Term] = Promise()
			val followerPromise: Promise[(Term, ParticipantId)] = Promise()
			val quiescedPromise: Promise[Unit] = Promise()
			private val commandAppliedPromises: mutable.Map[RecordIndex, Promise[Unit]] = mutable.Map.empty

			def awaitApplied(index: RecordIndex): Future[Unit] = synchronized {
				if highestAppliedIndex >= index then Future.successful(())
				else commandAppliedPromises.getOrElseUpdate(index, Promise()).future
			}

			override def onBecameLeader(previous: RoleOrdinal, term: Term): Unit = {
				currentRole = LEADER
				leaderTerm = Maybe(term)
				leaderPromise.trySuccess(term)
			}

			override def onBecameFollower(previous: RoleOrdinal, term: Term, leaderId: ParticipantId): Unit = {
				currentRole = FOLLOWER
				followee = Maybe(leaderId)
				followerPromise.trySuccess((term, leaderId))
			}

			override def onJoining(previous: RoleOrdinal): Unit = {
				currentRole = JOINING
			}

			override def onBecameIsolated(previous: RoleOrdinal, term: Term): Unit = {
				currentRole = ISOLATED
			}

			override def onCommitIndexChanged(previous: RecordIndex, current: RecordIndex, as: RoleOrdinal, at: Term): Unit = {
				commitIndex = current
			}

			override def onCommandApplied(appliedCommandIndex: RecordIndex, appliedCommandTerm: Term): Unit = synchronized {
				highestAppliedIndex = appliedCommandIndex
				commandAppliedPromises.get(appliedCommandIndex).foreach(_.trySuccess(()))
			}

			override def onBecameQuiesced(previous: RoleOrdinal, term: Term, motive: Try[String]): Unit = {
				currentRole = QUIESCED
				isQuiesced = true
				quiescedPromise.trySuccess(())
			}
		}
	}

	def withCluster(
		nodeIds: List[String],
		initialElectorate: Set[String],
		compactionThreshold: Int = 1000,
		retentionAfterSnapshot: Int = 10,
		customBridgeConfig: Option[ClusterBridgeConfig] = None
	)(testBody: (Map[String, TestNode], ConcurrentHashMap[String, SocketAddress], CooperativeThreadDrivenSchedulerDp.Impl) => Unit): Unit = {
		val schedulerDp = new CooperativeThreadDrivenSchedulerDp.Impl(unhandledExceptionReporter = (_, e) => e.printStackTrace())
		val addressRegistry = new ConcurrentHashMap[String, SocketAddress]()
		val peerLookup: String => SocketAddress = id => {
			val addr = addressRegistry.get(id)
			if addr == null then new InetSocketAddress("127.0.0.1", 0) else addr
		}

		val nodes = nodeIds.map { id =>
			id -> new TestNode(id, peerLookup, schedulerDp, initialElectorate, compactionThreshold, retentionAfterSnapshot, customBridgeConfig = customBridgeConfig)
		}.toMap

		try {
			for ((id, node) <- nodes) {
				node.bridge.start()
				addressRegistry.put(id, node.bridge.localAddress)
			}
			for ((_, node) <- nodes) {
				node.startParticipant()
			}

			testBody(nodes, addressRegistry, schedulerDp)
		} finally {
			for ((_, node) <- nodes) {
				node.stop()
			}
			schedulerDp.shutdown()
			schedulerDp.awaitTermination(5, TimeUnit.SECONDS)
		}
	}

	def withClient[T](node: TestNode)(body: ClusterClient[node.type] => T): T = {
		val client = new ClusterClient[node.type](node)(node.bridge.localAddress)
		try {
			Await.result(client.connect(), 5.seconds)
			body(client)
		} finally {
			client.close()
		}
	}

	// -------------------------------------------------------------------------
	// Planned End-to-End Scenarios
	// -------------------------------------------------------------------------

	test("Scenario 1 - Multi-node discovery and leader election over loopback TCP") {
		val clusterIds = List("node-1", "node-2", "node-3")
		withCluster(clusterIds, clusterIds.toSet) { (nodes, _, _) =>
			val node1 = nodes("node-1")
			val node2 = nodes("node-2")
			val node3 = nodes("node-3")

			withClient(node1) { client =>
				val response = Await.result(client.sendCommand("bootstrap-command"), 10.seconds)
				response match {
					case node1.Processed(idx, content) =>
						assertEquals(content, "ack-bootstrap-command")
						assert(idx >= 1L)
					case other => fail(s"Expected Processed response, got: $other")
				}
			}

			val electedTerm = Await.result(node1.listener.leaderPromise.future, 5.seconds)
			val (term2, followee2) = Await.result(node2.listener.followerPromise.future, 5.seconds)
			val (term3, followee3) = Await.result(node3.listener.followerPromise.future, 5.seconds)

			assertEquals(node1.listener.currentRole, LEADER)
			assertEquals(node2.listener.currentRole, FOLLOWER)
			assertEquals(node3.listener.currentRole, FOLLOWER)

			assertEquals(followee2, "node-1")
			assertEquals(followee3, "node-1")
			assertEquals(term2, electedTerm)
			assertEquals(term3, electedTerm)
		}
	}

	test("Scenario 2 - Linearizable client command replication and follower redirection") {
		val clusterIds = List("node-1", "node-2", "node-3")
		withCluster(clusterIds, clusterIds.toSet) { (nodes, _, _) =>
			val node1 = nodes("node-1")
			val node2 = nodes("node-2")
			val node3 = nodes("node-3")

			withClient(node1) { client1 =>
				val r1 = Await.result(client1.sendCommand("cmd-1"), 5.seconds)
				r1 match {
					case node1.Processed(_, content) => assertEquals(content, "ack-cmd-1")
					case other => fail(s"Expected Processed, got $other")
				}
			}

			Await.result(node1.listener.leaderPromise.future, 5.seconds)
			Await.result(node2.listener.followerPromise.future, 5.seconds)
			Await.result(node3.listener.followerPromise.future, 5.seconds)

			// Follower redirection
			withClient(node2) { client2 =>
				val redirectResponse = Await.result(client2.sendCommand("cmd-redirect", FIRST_ATTEMPT), 5.seconds)
				redirectResponse match {
					case node2.RedirectTo(candidates) =>
						assert(candidates.nonEmpty)
						assertEquals(candidates.head, "node-1")
					case other => fail(s"Expected RedirectTo, got: $other")
				}
			}

			// Submit to redirected leader
			withClient(node1) { client1 =>
				val r2 = Await.result(client1.sendCommand("cmd-redirect"), 5.seconds)
				r2 match {
					case node1.Processed(idx, content) =>
						assertEquals(content, "ack-cmd-redirect")
						Await.result(node1.listener.awaitApplied(idx), 5.seconds)
						Await.result(node2.listener.awaitApplied(idx), 5.seconds)
						Await.result(node3.listener.awaitApplied(idx), 5.seconds)
					case other => fail(s"Expected Processed, got $other")
				}
			}

			assertEquals(node1.stateMachine.highestAppliedIndex, node2.stateMachine.highestAppliedIndex)
			assertEquals(node1.stateMachine.highestAppliedIndex, node3.stateMachine.highestAppliedIndex)
		}
	}

	test("Scenario 3 - Log compaction and streaming InstallSnapshot over TCP") {
		val clusterIds = List("node-1", "node-2", "node-3")
		withCluster(clusterIds, clusterIds.toSet, compactionThreshold = 2, retentionAfterSnapshot = 1) { (nodes, addressRegistry, _) =>
			val node1 = nodes("node-1")
			val node2 = nodes("node-2")
			val node3 = nodes("node-3")

			node1.stateMachine.useFileSnapshot = true

			withClient(node1) { client =>
				Await.result(client.sendCommand("cmd-init"), 5.seconds)
				Await.result(node1.listener.leaderPromise.future, 5.seconds)

				// Disconnect node-3 so it misses entries
				node3.bridge.close()

				// Commit multiple commands through quorum (node-1 + node-2) to exceed compaction threshold
				for (i <- 1 to 5) {
					Await.result(client.sendCommand(s"cmd-compact-$i"), 10.seconds)
				}
			}

			// Wait for leader log compaction to advance logBufferOffset
			val deadline = System.currentTimeMillis() + 5000L
			var currentOffset = 1L
			while currentOffset <= 2 && System.currentTimeMillis() < deadline do {
				val p = Promise[RecordIndex]()
				node1.sequencer.run { p.success(node1.workspace.logBufferOffset) }
				currentOffset = Await.result(p.future, 1.second)
				if currentOffset <= 2 then Thread.sleep(50)
			}
			assert(currentOffset > 2, s"Expected logBufferOffset > 2, but was $currentOffset")

			// Reconnect node-3 with fresh bridge
			node3.restartWithFreshBridge(newAddr => addressRegistry.put("node-3", newAddr))

			// Reconnected follower should receive InstallSnapshot and catch up
			val highestIdx = node1.stateMachine.highestAppliedIndex
			Await.result(node3.listener.awaitApplied(highestIdx), 10.seconds)

			assertEquals(node3.stateMachine.highestAppliedIndex, highestIdx)

			val snapPromise = Promise[Maybe[SnapshotData[String]]]()
			node3.sequencer.run { snapPromise.success(node3.workspace.latestSnapshot) }
			val maybeSnap = Await.result(snapPromise.future, 1.second)
			assert(maybeSnap.isDefined)
		}
	}

	test("Scenario 4 - Dynamic electorate change and node quiescence over TCP") {
		val initialCluster = List("node-1", "node-2", "node-3")
		val schedulerDp = new CooperativeThreadDrivenSchedulerDp.Impl(unhandledExceptionReporter = (_, e) => e.printStackTrace())
		val addressRegistry = new ConcurrentHashMap[String, SocketAddress]()
		val peerLookup: String => SocketAddress = id => {
			val addr = addressRegistry.get(id)
			if addr == null then new InetSocketAddress("127.0.0.1", 0) else addr
		}

		val node1 = new TestNode("node-1", peerLookup, schedulerDp, initialCluster.toSet)
		val node2 = new TestNode("node-2", peerLookup, schedulerDp, initialCluster.toSet)
		val node3 = new TestNode("node-3", peerLookup, schedulerDp, initialCluster.toSet)
		val node4 = new TestNode("node-4", peerLookup, schedulerDp, Set.empty) // Dynamic joiner

		val allNodes = List(node1, node2, node3, node4)

		try {
			for (node <- allNodes) {
				node.bridge.start()
				addressRegistry.put(node.nodeId, node.bridge.localAddress)
			}
			for (node <- List(node1, node2, node3)) {
				node.startParticipant()
			}

			withClient(node1) { client =>
				Await.result(client.sendCommand("bootstrap-electorate"), 5.seconds)
				Await.result(node1.listener.leaderPromise.future, 5.seconds)

				// Start node-4 as dynamic joining participant
				node4.startParticipant()

				// Request electorate change: replace node-3 with node-4
				val changeResponse = Await.result(
					client.requestElectorateChange("req-reconfig-1", Set("node-1", "node-2", "node-4")),
					10.seconds
				)
				assertEquals(changeResponse, ElectorateChangeResponse.SuccessfullyChanged)

				// Subsequent commands commit across new electorate {node-1, node-2, node-4}
				val r = Await.result(client.sendCommand("post-reconfig-command"), 5.seconds)
				r match {
					case node1.Processed(idx, _) =>
						Await.result(node1.listener.awaitApplied(idx), 5.seconds)
						Await.result(node2.listener.awaitApplied(idx), 5.seconds)
						Await.result(node4.listener.awaitApplied(idx), 5.seconds)
					case other => fail(s"Expected Processed, got: $other")
				}
			}

			// Excluded node-3 must receive quiescence permission and transition to QUIESCED
			Await.result(node3.listener.quiescedPromise.future, 10.seconds)
			assertEquals(node3.listener.currentRole, QUIESCED)

			// node-4 must now be an active follower
			val (_, followee4) = Await.result(node4.listener.followerPromise.future, 10.seconds)
			assertEquals(followee4, "node-1")
			assertEquals(node4.listener.currentRole, FOLLOWER)
		} finally {
			for (node <- allNodes) {
				node.stop()
			}
			schedulerDp.shutdown()
			schedulerDp.awaitTermination(5, TimeUnit.SECONDS)
		}
	}

	test("Scenario 5 - Node disconnect, quorum commit, and reconnect recovery") {
		val clusterIds = List("node-1", "node-2", "node-3")
		withCluster(clusterIds, clusterIds.toSet) { (nodes, addressRegistry, _) =>
			val node1 = nodes("node-1")
			val node2 = nodes("node-2")
			val node3 = nodes("node-3")

			withClient(node1) { client =>
				Await.result(client.sendCommand("cmd-baseline"), 5.seconds)
				Await.result(node1.listener.leaderPromise.future, 5.seconds)

				// Disconnect node-3
				node3.bridge.close()

				// Quorum commits on surviving nodes {node-1, node-2}
				for (i <- 1 to 3) {
					val resp = Await.result(client.sendCommand(s"cmd-quorum-$i"), 5.seconds)
					resp match {
						case node1.Processed(idx, _) =>
							Await.result(node1.listener.awaitApplied(idx), 5.seconds)
							Await.result(node2.listener.awaitApplied(idx), 5.seconds)
						case other => fail(s"Expected Processed, got: $other")
					}
				}
			}

			// Restart node-3 with fresh bridge on new ephemeral port
			node3.restartWithFreshBridge(newAddr => addressRegistry.put("node-3", newAddr))

			// node-3 catches up with leader via replication loop
			val highestIdx = node1.stateMachine.highestAppliedIndex
			Await.result(node3.listener.awaitApplied(highestIdx), 10.seconds)

			assertEquals(node3.stateMachine.highestAppliedIndex, highestIdx)

			// Subsequent commands commit across all three nodes
			withClient(node1) { client =>
				val resp = Await.result(client.sendCommand("cmd-after-reconnect"), 5.seconds)
				resp match {
					case node1.Processed(idx, _) =>
						Await.result(node1.listener.awaitApplied(idx), 5.seconds)
						Await.result(node2.listener.awaitApplied(idx), 5.seconds)
						Await.result(node3.listener.awaitApplied(idx), 5.seconds)
					case other => fail(s"Expected Processed, got: $other")
				}
			}
		}
	}

	test("Scenario 6 - Replication of oversized record batches exceeding control package capacity via out-of-line ByteSource streaming") {
		val clusterIds = List("node-1", "node-2", "node-3")
		// Constrain peer control package capacity to 4 * 4096 = 16 KiB while allowing client submissions up to 64 KiB
		val constrainedPeerConfig = ClusterBridgeConfig(
			sizeOfBuffersInRing = 4096,
			maxRingLengthForTransmission = RingLength(16),
			maxRingLengthForReception = RingLength(16),
			stagedBatchInlineThreshold = RingLength(1),
			fastRpcTimeout = 5000L,
			snapshotRpcTimeout = 10000L,
			clientRpcTimeout = 5000L,
			connectTimeout = 4000L,
			unreachableFollowersRetryDelay = 100L,
			replicationLoopRetryDelay = 50L
		)
		withCluster(clusterIds, clusterIds.toSet, customBridgeConfig = Some(constrainedPeerConfig)) { (nodes, addressRegistry, _) =>
			val node1 = nodes("node-1")
			val node2 = nodes("node-2")
			val node3 = nodes("node-3")

			withClient(node1) { client =>
				Await.result(client.sendCommand("cmd-init"), 5.seconds)
				Await.result(node1.listener.leaderPromise.future, 5.seconds)

				// Disconnect node-3 while committing five 16 KiB commands (~80 KiB catch-up batch, far above stagedBatchInlineThreshold = 1 buffer / 4 KiB and 64 KiB control ring)
				node3.bridge.close()

				val largePayload = "L" * (16 * 1024)
				for i <- 1 to 5 do {
					val resp = Await.result(client.sendCommand(s"large-$i-$largePayload"), 5.seconds)
					resp match {
						case node1.Processed(idx, _) =>
							Await.result(node1.listener.awaitApplied(idx), 5.seconds)
							Await.result(node2.listener.awaitApplied(idx), 5.seconds)
						case other => fail(s"Expected Processed, got: $other")
					}
				}
			}

			// Reconnect node-3: leader replicates the entire 80 KiB catch-up batch in a single AppendRecords RPC streamed out-of-line
			node3.restartWithFreshBridge(newAddr => addressRegistry.put("node-3", newAddr))
			val highestIdx = node1.stateMachine.highestAppliedIndex
			Await.result(node3.listener.awaitApplied(highestIdx), 10.seconds)
			assertEquals(node3.stateMachine.highestAppliedIndex, highestIdx)
			assertEquals(node3.stateMachine.appliedCommands.toList, node1.stateMachine.appliedCommands.toList)
		}
	}
}
