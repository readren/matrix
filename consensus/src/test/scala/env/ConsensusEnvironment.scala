package readren.consensus.env

import readren.consensus.TestClientCommand
import readren.consensus.protocol.*

import scala.collection.immutable.ListSet
import scala.collection.mutable

/** The discrete-event, fine-grained simulation and verification harness for [[readren.consensus.ConsensusParticipantSdm]].
 *
 * Enforces a strict three-tier architectural boundary separating physical execution from privileged correctness inspection:
 *  - **Tier 1 (Physical Environment & Subsystems)**: Node lifecycle and execution stepping (`startNode`, `crashNode`, `restartNode`, `stepNode`, `runNodeUntilIdle`), point-to-point packet transport ([[network]]), virtual time and timer management ([[clock]]), and asynchronous persistence queueing ([[storage]]).
 *  - **Tier 2 (External Client & Control-Plane Gateway)**: Client command injection (`submitClientCommand`, `clientCommandStatus`, `allClientStats`) and cluster reconfiguration requests (`submitElectorateChange`, `electorateChangeStatus`), operating strictly on client-visible protocol responses without visibility into internal node state.
 *  - **Tier 3 (Quarantined Verification Oracle)**: Cross-node consensus safety verification isolated within [[oracle]] (`ConsensusOracle`).
 *
 * All top-level operational mutations are recorded by [[journal]] (`EnvironmentJournal`) to support deterministic replay, undo/redo, and named timeline checkpoints.
 * @param clusterSize total number of simulated nodes (`p-0` through `p-(clusterSize - 1)`).
 * @param initialSeedParticipants optional subset of nodes forming the initial seed electorate; defaults to all `clusterSize` nodes when [[None]].
 * @param ticksPerMilli conversion factor from simulated milliseconds to virtual clock ticks.
 * @param maxInFlightAppendsPerPeer maximum number of concurrent pipelined `AppendRecords` RPCs per follower.
 * @param logCompactionThreshold number of log entries triggering automatic snapshot compaction.
 * @param initialRetiringParticipantMaxRetries initial retry limit for ghost leaders driving retirement under an empty electorate.
 * @param initialLogRetentionAfterSnapshot initial number of trailing log entries retained after snapshot truncation.
 * @param remembersLastAppliedCommandIndex when true, [[EnvironmentNode.TestStateMachine.recoverIndexOfLastAppliedCommand]] returns the last applied index across restarts; when false, it resets to 0 to force snapshot installation and log replay.
 * @param initializer hook invoked automatically at the end of [[reset]] (with journal recording suppressed) to establish baseline cluster state. */
class ConsensusEnvironment(
	val clusterSize: Int,
	val initialSeedParticipants: Option[Set[? <: NodeRef]] = None,
	val ticksPerMilli: Int = 10,
	val maxInFlightAppendsPerPeer: Int = 2,
	val logCompactionThreshold: Int = 5,
	val initialRetiringParticipantMaxRetries: Int = 2,
	val initialLogRetentionAfterSnapshot: Int = 1,
	val remembersLastAppliedCommandIndex: Boolean = false,
	private val initializer: ConsensusEnvironment => Unit = _ => ()
) {
	private var _retiringParticipantMaxRetries: Int = initialRetiringParticipantMaxRetries

	/** Dynamic retry limit for ghost leaders driving retirement under an empty electorate. */
	def retiringParticipantMaxRetries: Int = _retiringParticipantMaxRetries

	private var _logRetentionAfterSnapshot: Int = initialLogRetentionAfterSnapshot

	/** Dynamic number of trailing log records retained when a snapshot is taken. */
	def logRetentionAfterSnapshot: Int = _logRetentionAfterSnapshot

	/** Canonical initial seed voting electorate derived from [[initialSeedParticipants]] or `0 until clusterSize`. */
	val defaultInitialParticipants: ListSet[NodeId] = initialSeedParticipants match {
		case Some(seeds) => ListSet.from(seeds.map(_.asNodeId))
		case None => ListSet.from((0 until clusterSize).map(i => NodeId(s"p-$i")))
	}

	private var commandIdSequencer: Int = 0
	private var electorateReqIdSequencer: Int = 0

	private val nodesMap: mutable.Map[NodeId, EnvironmentNode] = mutable.Map.empty
	private val clientStatuses: mutable.Map[Int, ClientCommandStatus] = mutable.Map.empty
	private val clientSerialCounters: mutable.Map[String, Int] = mutable.Map.empty.withDefaultValue(0)
	private val clientLastSent: mutable.Map[String, Int] = mutable.Map.empty
	private val clientLastSuccess: mutable.Map[String, Int] = mutable.Map.empty
	private val clientLastRecordIndex: mutable.Map[String, Long] = mutable.Map.empty
	private val clientLastStatus: mutable.Map[String, ClientCommandStatus] = mutable.Map.empty
	private val electorateChangeStatuses: mutable.Map[String, ElectorateChangeStatus] = mutable.Map.empty
	private val inFlightElectorateChanges: mutable.Map[String, ElectorateChangeHandle] = mutable.Map.empty

	/** Deterministic operation journal providing undo, redo, and checkpoint tag replay. */
	val journal: EnvironmentJournal = new EnvironmentJournal(this)

	/** Virtual clock and wake-up timer subsystem (Tier 1). */
	val clock: VirtualClock = new VirtualClock(ticksPerMilli, this)

	/** Asynchronous workspace persistence queue and storage fault-injection subsystem (Tier 1). */
	val storage: SimulatedStorageSubsystem = new SimulatedStorageSubsystem(this)

	/** Point-to-point FIFO network channel and packet delivery subsystem (Tier 1). */
	val network: SimulatedNetwork = new SimulatedNetwork(this)

	/** Quarantined omniscient safety invariant inspector (Tier 3). */
	val oracle: ConsensusOracle = new ConsensusOracle(this)

	/** Simulated administrative control plane coordinating peer discovery, dynamic joins, and cluster membership (Tier 2). */
	val controlPlane: SimulatedControlPlane = new SimulatedControlPlane(this)

	for i <- 0 until clusterSize do {
		val id = NodeId(s"p-$i")
		nodesMap(id) = new EnvironmentNode(id, defaultInitialParticipants, this)
	}
	oracle.reset()

	/** Resets all subsystems ([[clock]], [[storage]], [[network]], [[oracle]]), client/control-plane tracking tables, and [[EnvironmentNode]] instances to their initial state, then executes [[initializer]] with replay recording suppressed. */
	def reset(): Unit = {
		clock.reset()
		storage.reset()
		network.reset()
		oracle.reset()
		controlPlane.reset()

		commandIdSequencer = 0
		electorateReqIdSequencer = 0

		nodesMap.clear()
		clientStatuses.clear()
		clientSerialCounters.clear()
		clientLastSent.clear()
		clientLastSuccess.clear()
		clientLastRecordIndex.clear()
		clientLastStatus.clear()
		electorateChangeStatuses.clear()
		inFlightElectorateChanges.clear()

		_retiringParticipantMaxRetries = initialRetiringParticipantMaxRetries
		_logRetentionAfterSnapshot = initialLogRetentionAfterSnapshot

		for i <- 0 until clusterSize do {
			val id = NodeId(s"p-$i")
			nodesMap(id) = new EnvironmentNode(id, defaultInitialParticipants, this)
		}

		journal.withReplaySuppression {
			initializer(this)
		}
	}

	/** Updates dynamic environment parameters and per-node automatic storage persistence thresholds.
	 * @param retiringMaxRetries optional new value for [[retiringParticipantMaxRetries]].
	 * @param logRetention optional new value for [[logRetentionAfterSnapshot]].
	 * @param autoSucceedUntilTerm optional term threshold for automatic storage save completion.
	 * @param autoSucceedUntilRecordIndex optional record index threshold for automatic storage save completion.
	 * @param targetNode specific node to apply storage thresholds to, or [[None]] (or `"all"`) to apply across all nodes. */
	def updateDynamicSettings(
		retiringMaxRetries: Option[Int] = None,
		logRetention: Option[Int] = None,
		autoSucceedUntilTerm: Option[Term] = None,
		autoSucceedUntilRecordIndex: Option[RecordIndex] = None,
		targetNode: Option[NodeRef] = None
	): Unit = journal.recordOrExecute(EnvOperation.UpdateDynamicSettings(retiringMaxRetries, logRetention, autoSucceedUntilTerm, autoSucceedUntilRecordIndex, targetNode.map(_.asNodeId))) {
		retiringMaxRetries.foreach(v => _retiringParticipantMaxRetries = v)
		logRetention.foreach(v => _logRetentionAfterSnapshot = v)
		targetNode match {
			case Some(ref) if ref.asNodeId.nonEmpty && ref.asNodeId != "all" =>
				val n = node(ref)
				autoSucceedUntilTerm.foreach(t => n.autoSucceedUntilTerm = t)
				autoSucceedUntilRecordIndex.foreach(i => n.autoSucceedUntilRecordIndex = i)
			case _ =>
				for i <- 0 until clusterSize do {
					val n = node(i)
					autoSucceedUntilTerm.foreach(t => n.autoSucceedUntilTerm = t)
					autoSucceedUntilRecordIndex.foreach(i => n.autoSucceedUntilRecordIndex = i)
				}
		}
	}

	private def nextCommandId(): Int = {
		commandIdSequencer += 1
		commandIdSequencer
	}

	private def nextConfigReqId(): Int = {
		electorateReqIdSequencer += 1
		electorateReqIdSequencer
	}

	//// NODE MANAGEMENT (TIER 1) ////

	/** Returns the [[EnvironmentNode]] corresponding to `ref`. */
	def node(ref: NodeRef): EnvironmentNode = nodesMap(ref.asNodeId)

	/** Returns `Some(node)` if `ref` exists in the environment, or [[None]] otherwise. */
	private[env] def nodeOpt(ref: NodeRef): Option[EnvironmentNode] = nodesMap.get(ref.asNodeId)

	/** Returns all [[EnvironmentNode]] instances in the cluster ordered by node index (`p-0` to `p-(clusterSize - 1)`). */
	def allNodes: Seq[EnvironmentNode] = (0 until clusterSize).map(i => node(i))

	/** Starts the consensus participant on node `ref` if it is not already running. */
	def startNode(ref: NodeRef): Unit = journal.recordOrExecute(EnvOperation.StartNode(ref.asNodeId)) {
		node(ref).startIfNotRunning()
	}

	/** Starts all initial seed nodes in [[defaultInitialParticipants]]. */
	def startAllNodes(): Unit = journal.recordOrExecute(EnvOperation.StartAllNodes) {
		for id <- defaultInitialParticipants do startNode(id)
	}

	/** Simulates an abrupt crash of node `ref` by marking it down and discarding all unexecuted tasks in its [[StepDoer]] queue while preserving its durable [[EnvironmentNode.TestStorage.savedMemory]]. */
	def crashNode(ref: NodeRef): Unit = journal.recordOrExecute(EnvOperation.CrashNode(ref.asNodeId)) {
		val n = node(ref)
		n.isDown = true
		n.stepDoer.forgetPendingTasks()
	}

	/** Restarts node `ref` from its durable storage state. */
	def restartNode(ref: NodeRef): Unit = journal.recordOrExecute(EnvOperation.RestartNode(ref.asNodeId)) {
		val n = node(ref)
		n.isDown = false
		n.startIfNotRunning()
	}

	/** Returns the human-readable role name of node `ref` (e.g., `"LEADER"`, `"FOLLOWER"`, `"CANDIDATE"`), or `"DOWN"` if the node is crashed or uninitialized. */
	def nodeRole(ref: NodeRef): String = {
		val n = node(ref)
		if n.isDown || n.participant == null then "DOWN"
		else RoleOrdinal_nameOf(n.participant.getRoleOrdinal)
	}

	//// STEPPING OPERATIONS (TIER 1) ////

	/** Dequeues and executes a single runnable task on node `ref`'s [[StepDoer]].
	 * @return true if a task was executed; false if the node's queue was empty. */
	def stepNode(ref: NodeRef): Boolean = journal.recordOrExecute(EnvOperation.StepNode(ref.asNodeId)) {
		node(ref).stepDoer.step()
	}

	/** Executes at most one runnable task on each node in the cluster.
	 * @return the number of nodes that executed a task during this round. */
	def stepAllNodes(): Int = journal.recordOrExecute(EnvOperation.StepAllNodes) {
		var stepped = 0
		for n <- nodesMap.values do {
			if n.stepDoer.step() then stepped += 1
		}
		stepped
	}

	/** Walks node `ref`'s [[StepDoer]] queue until empty or until `maxSteps` tasks have been executed.
	 * @param ref the node to run.
	 * @param maxSteps safeguard bound on the number of tasks executed.
	 * @return the number of tasks executed. */
	def runNodeUntilIdle(ref: NodeRef, maxSteps: Int = 1000): Int = journal.recordOrExecute(EnvOperation.RunNodeUntilIdle(ref.asNodeId, maxSteps)) {
		node(ref).stepDoer.walk(maxSteps)
	}

	/** Repeatedly invokes [[stepAllNodes]] until all nodes' [[StepDoer]] queues are empty or `maxRounds` rounds have elapsed.
	 * @param maxRounds safeguard bound on the number of cluster-wide stepping rounds.
	 * @return the total number of tasks executed across all nodes. */
	def runAllNodesUntilIdle(maxRounds: Int = 1000): Int = journal.recordOrExecute(EnvOperation.RunAllNodesUntilIdle(maxRounds)) {
		var totalExecuted = 0
		var rounds = 0
		var progressed = true
		while progressed && rounds < maxRounds do {
			rounds += 1
			val count = stepAllNodes()
			totalExecuted += count
			progressed = count > 0
		}
		totalExecuted
	}

	//// CLIENT COMMAND GATEWAY (TIER 2) ////

	/** Submits a client command to `targetNode` on behalf of `client` and tracks its completion status in [[clientCommandStatus]].
	 * @param targetNode the node to which the client sends the command.
	 * @param client the client identifier reference.
	 * @param serial optional explicit command serial number; auto-increments the client's last serial counter when [[None]].
	 * @param attemptFlag protocol attempt flag (`FIRST_ATTEMPT`, `RETRY`, or `RE_SUBMIT_ON_UNCERTAIN`).
	 * @return a [[ClientCommandHandle]] identifying the submitted command. */
	def submitClientCommand(
		targetNode: NodeRef,
		client: ClientRef,
		serial: Option[Int] = None,
		attemptFlag: CommandAttemptFlag = FIRST_ATTEMPT
	): ClientCommandHandle = {
		val clientId = client.asClientId
		val targetId = targetNode.asNodeId
		val cmdSerial = serial match {
			case Some(s) =>
				clientSerialCounters(clientId) = math.max(clientSerialCounters(clientId), s)
				s
			case None =>
				val s = clientSerialCounters(clientId) + 1
				clientSerialCounters(clientId) = s
				s
		}
		journal.recordOrExecute(EnvOperation.SubmitClientCommand(targetId, clientId, Some(cmdSerial), attemptFlag)) {
			val cmdId = nextCommandId()
			val handle = ClientCommandHandle(cmdId, clientId, cmdSerial, targetId, clock.currentVirtualTime)
			clientStatuses(cmdId) = ClientCommandStatus.InFlight
			clientLastStatus(clientId) = ClientCommandStatus.InFlight
			clientLastSent(clientId) = cmdSerial

			val n = node(targetId)
			n.stepDoer.executeSequentially(() => {
				if n.isDown || n.participant == null then {
					val status = ClientCommandStatus.Failed(new RuntimeException(s"Node $targetId is down"))
					clientStatuses(cmdId) = status
					clientLastStatus(clientId) = status
				} else {
					val command = TestClientCommand(cmdSerial, clientId)
					val capture = n.clusterParticipant.delegate.onCommandFromClient(command, attemptFlag)
					capture.triggerSyncCallbacks(
						{
							case n.Processed(recIdx, content) =>
								val status = ClientCommandStatus.Processed(recIdx, content)
								clientStatuses(cmdId) = status
								clientLastStatus(clientId) = status
								clientLastSuccess(clientId) = cmdSerial
								clientLastRecordIndex(clientId) = recIdx
							case n.RedirectTo(others) =>
								val status = ClientCommandStatus.Redirected(others)
								clientStatuses(cmdId) = status
								clientLastStatus(clientId) = status
							case n.Unable(flag, others) =>
								val status = ClientCommandStatus.Unable(flag, others)
								clientStatuses(cmdId) = status
								clientLastStatus(clientId) = status
						},
						ex => {
							val status = ClientCommandStatus.Failed(ex)
							clientStatuses(cmdId) = status
							clientLastStatus(clientId) = status
						}
					)
				}
			})
			handle
		}
	}

	/** Returns the current [[ClientCommandStatus]] of the command submission identified by `commandId`. */
	def clientCommandStatus(commandId: Int): ClientCommandStatus = clientStatuses.getOrElse(commandId, ClientCommandStatus.InFlight)

	/** Returns the number of commands currently in flight across the environment. */
	def inFlightClientCommandsCount(client: ClientRef): Int = {
		val _ = client.asClientId
		clientStatuses.values.count {
			case ClientCommandStatus.InFlight => true
			case _ => false
		}
	}

	/** Returns a snapshot of all submitted command IDs and their current [[ClientCommandStatus]]. */
	def allClientStatuses: Map[Int, ClientCommandStatus] = clientStatuses.toMap

	/** Returns aggregated [[ClientStats]] for all clients that have interacted with the environment. */
	def allClientStats: Map[String, ClientStats] = {
		val allIds = clientSerialCounters.keySet ++ clientLastSent.keySet ++ clientLastSuccess.keySet ++ clientLastRecordIndex.keySet ++ clientLastStatus.keySet
		allIds.map(id => id -> ClientStats(clientLastSent.get(id), clientLastSuccess.get(id), clientLastRecordIndex.get(id), clientLastStatus.get(id))).toMap
	}

	//// ELECTORATE CHANGE GATEWAY (TIER 2) ////

	/** Submits an electorate change request to `targetNode` proposing `desiredParticipants` as the new voting electorate.
	 * @param targetNode the node to which the reconfiguration request is sent.
	 * @param desiredParticipants the target set of voting participant references.
	 * @return an [[ElectorateChangeHandle]] tracking the request ID and target membership. */
	def submitElectorateChange(
		targetNode: NodeRef,
		desiredParticipants: Set[? <: NodeRef]
	): ElectorateChangeHandle = {
		val targetId = targetNode.asNodeId
		val desiredIds: Set[NodeId] = desiredParticipants.map(_.asNodeId)
		journal.recordOrExecute(EnvOperation.SubmitElectorateChange(targetId, desiredIds)) {
			val reqId = s"ccReq-${nextConfigReqId()}"
			val handle = ElectorateChangeHandle(reqId, targetId, desiredIds, clock.currentVirtualTime)
			electorateChangeStatuses(reqId) = ElectorateChangeStatus.InFlight
			inFlightElectorateChanges(reqId) = handle

			val n = node(targetId)
			n.stepDoer.executeSequentially(() => {
				if n.isDown || n.participant == null then {
					inFlightElectorateChanges.remove(reqId)
					electorateChangeStatuses(reqId) = ElectorateChangeStatus.Failed(new RuntimeException(s"Node $targetId is down"))
				} else {
					val capture = n.clusterParticipant.delegate.requestElectorateChange(reqId, desiredIds)
					capture.triggerSyncCallbacks(
						res => {
							inFlightElectorateChanges.remove(reqId)
							electorateChangeStatuses(reqId) = ElectorateChangeStatus.Completed(res)
						},
						ex => {
							inFlightElectorateChanges.remove(reqId)
							electorateChangeStatuses(reqId) = ElectorateChangeStatus.Failed(ex)
						}
					)
				}
			})
			handle
		}
	}

	/** Returns the current [[ElectorateChangeStatus]] of the reconfiguration request identified by `requestId`. */
	def electorateChangeStatus(requestId: String): ElectorateChangeStatus = electorateChangeStatuses.getOrElse(requestId, ElectorateChangeStatus.InFlight)

	/** Administrative control-plane gateway operation updating the cluster tier's memorized probable participants on `targetNode`.
	 * Delegated to [[controlPlane]]. */
	inline def updateProbableParticipants(targetNode: NodeRef, probableParticipants: Set[? <: NodeRef]): Unit =
		controlPlane.updateProbableParticipants(targetNode, probableParticipants)

	/** Administrative control-plane gateway operation updating the cluster tier's memorized probable participants across `targetNodes`.
	 * Delegated to [[controlPlane]]. */
	inline def broadcastProbableParticipantsUpdate(probableParticipants: Set[? <: NodeRef], targetNodes: Option[Iterable[? <: NodeRef]] = None): Unit =
		controlPlane.broadcastProbableParticipantsUpdate(probableParticipants, targetNodes)
}
