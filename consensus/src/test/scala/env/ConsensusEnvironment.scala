package readren.consensus.env

import readren.consensus.ConsensusParticipantSdm.*
import readren.consensus.TestClientCommand
import readren.consensus.protocol.*

import scala.collection.immutable.ListSet
import scala.collection.mutable
import scala.util.Try

/** The discrete-event, fine-grained testing harness for [[readren.consensus.ConsensusParticipantSdm]]. */
class ConsensusEnvironment(
	val clusterSize: Int = 3,
	val initialSeedParticipants: Option[Set[? <: NodeRef]] = None,
	val ticksPerMilli: Int = 10,
	val maxInFlightAppendsPerPeer: Int = 2,
	val logCompactionThreshold: Int = 5,
	val initialRetiringParticipantMaxRetries: Int = 2,
	val initialLogRetentionAfterSnapshot: Int = 1,
	var initializer: ConsensusEnvironment => Unit = _ => ()
) {
	var retiringParticipantMaxRetries: Int = initialRetiringParticipantMaxRetries
	var logRetentionAfterSnapshot: Int = initialLogRetentionAfterSnapshot
	var remembersLastAppliedCommandIndex: Boolean = false
	var onNodeQuiescedHook: (EnvironmentNode, Try[String]) => Unit = (node, _) => node.release()

	val journal: EnvironmentJournal = new EnvironmentJournal(this)
	val clock: VirtualClock = new VirtualClock(ticksPerMilli, this)
	val storage: SimulatedStorageSubsystem = new SimulatedStorageSubsystem(this)
	val network: SimulatedNetwork = new SimulatedNetwork(this)
	val oracle: ConsensusOracle = new ConsensusOracle(this)

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

	val defaultInitialParticipants: ListSet[NodeId] = initialSeedParticipants match {
		case Some(seeds) => ListSet.from(seeds.map(_.asNodeId))
		case None => ListSet.from((0 until clusterSize).map(i => NodeId(s"p-$i")))
	}

	for i <- 0 until clusterSize do {
		val id = NodeId(s"p-$i")
		nodesMap(id) = new EnvironmentNode(id, defaultInitialParticipants, this)
	}
	oracle.reset()

	def reset(): Unit = {
		clock.reset()
		storage.reset()
		network.reset()
		oracle.reset()

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

		retiringParticipantMaxRetries = initialRetiringParticipantMaxRetries
		logRetentionAfterSnapshot = initialLogRetentionAfterSnapshot
		remembersLastAppliedCommandIndex = false
		onNodeQuiescedHook = (node, _) => node.release()

		for i <- 0 until clusterSize do {
			val id = NodeId(s"p-$i")
			nodesMap(id) = new EnvironmentNode(id, defaultInitialParticipants, this)
		}

		journal.withReplaySuppression {
			initializer(this)
		}
	}

	def updateDynamicSettings(
		retiringMaxRetries: Option[Int] = None,
		logRetention: Option[Int] = None,
		autoSucceedUntilTerm: Option[Term] = None,
		autoSucceedUntilRecordIndex: Option[RecordIndex] = None,
		targetNode: Option[NodeRef] = None
	): Unit = journal.recordOrExecute(EnvOperation.UpdateDynamicSettings(retiringMaxRetries, logRetention, autoSucceedUntilTerm, autoSucceedUntilRecordIndex, targetNode.map(_.asNodeId))) {
		retiringMaxRetries.foreach(v => retiringParticipantMaxRetries = v)
		logRetention.foreach(v => logRetentionAfterSnapshot = v)
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

	// Node Management
	def node(ref: NodeRef): EnvironmentNode = nodesMap(ref.asNodeId)

	private[env] def nodeOpt(ref: NodeRef): Option[EnvironmentNode] = nodesMap.get(ref.asNodeId)

	def allNodes: Seq[EnvironmentNode] = (0 until clusterSize).map(i => node(i))

	def startNode(ref: NodeRef): Unit = journal.recordOrExecute(EnvOperation.StartNode(ref.asNodeId)) {
		node(ref).startIfNotRunning()
	}

	def startAllNodes(): Unit = journal.recordOrExecute(EnvOperation.StartAllNodes) {
		for id <- defaultInitialParticipants do startNode(id)
	}

	def crashNode(ref: NodeRef): Unit = journal.recordOrExecute(EnvOperation.CrashNode(ref.asNodeId)) {
		val n = node(ref)
		n.isDown = true
		n.stepDoer.forgetPendingTasks()
	}

	def restartNode(ref: NodeRef): Unit = journal.recordOrExecute(EnvOperation.RestartNode(ref.asNodeId)) {
		val n = node(ref)
		n.isDown = false
		n.startIfNotRunning()
	}

	def nodeRole(ref: NodeRef): String = {
		val n = node(ref)
		if n.isDown || n.participant == null then "DOWN"
		else RoleOrdinal_nameOf(n.participant.getRoleOrdinal)
	}

	// Stepping Operations
	def stepNode(ref: NodeRef): Boolean = journal.recordOrExecute(EnvOperation.StepNode(ref.asNodeId)) {
		node(ref).stepDoer.step()
	}

	def stepAllNodes(): Int = journal.recordOrExecute(EnvOperation.StepAllNodes) {
		var stepped = 0
		for n <- nodesMap.values do {
			if n.stepDoer.step() then stepped += 1
		}
		stepped
	}

	def runNodeUntilIdle(ref: NodeRef, maxSteps: Int = 1000): Int = journal.recordOrExecute(EnvOperation.RunNodeUntilIdle(ref.asNodeId, maxSteps)) {
		node(ref).stepDoer.walk(maxSteps)
	}

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

	// Client Command Injections (Tier 2 Gateway)
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
							case n.RedirectTo(leaderId) =>
								val status = ClientCommandStatus.Redirected(leaderId)
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

	def clientCommandStatus(commandId: Int): ClientCommandStatus = clientStatuses.getOrElse(commandId, ClientCommandStatus.InFlight)

	def inFlightClientCommandsCount(client: ClientRef): Int = {
		val _ = client.asClientId
		clientStatuses.values.count {
			case ClientCommandStatus.InFlight => true
			case _ => false
		}
	}

	def allClientStatuses: Map[Int, ClientCommandStatus] = clientStatuses.toMap

	def allClientStats: Map[String, ClientStats] = {
		val allIds = clientSerialCounters.keySet ++ clientLastSent.keySet ++ clientLastSuccess.keySet ++ clientLastRecordIndex.keySet ++ clientLastStatus.keySet
		allIds.map(id => id -> ClientStats(clientLastSent.get(id), clientLastSuccess.get(id), clientLastRecordIndex.get(id), clientLastStatus.get(id))).toMap
	}

	// Electorate Change Injections (Tier 2 Gateway)
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

	def electorateChangeStatus(requestId: String): ElectorateChangeStatus = electorateChangeStatuses.getOrElse(requestId, ElectorateChangeStatus.InFlight)

	private[env] def onActiveElectorateChanged(node: EnvironmentNode, change: ElectorateChange[NodeId], changeIndex: RecordIndex, roleOrdinal: RoleOrdinal): Unit = ()

	private[env] def onNodeQuiesced(node: EnvironmentNode, motive: Try[String]): Unit = {
		onNodeQuiescedHook(node, motive)
	}
}
