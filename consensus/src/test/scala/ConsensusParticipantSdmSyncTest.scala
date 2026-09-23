package readren.consensus

import ConsensusParticipantSdm.*
import readren.consensus.protocol.*

import munit.ScalaCheckSuite
import org.scalacheck.Gen
import org.scalacheck.Prop
import org.scalacheck.Test.Parameters
import readren.common.ScribeConfig
import scribe.modify.LogModifier
import scribe.message.LoggableMessage
import scribe.output.TextOutput
import scribe.throwable.TraceLoggableMessage
import scribe.{LogRecord, Priority}

import scala.collection.immutable.{ListMap, ListSet}
import scala.collection.mutable
import scala.util.{Failure, Random, Success, Try}

/** A synchronous, discrete-event invariant verification test suite for [[ConsensusParticipantSdm]],
 * backed by [[ConsensusEnvironment]].
 *
 * Replaces the multi-threaded cooperative scheduler and asynchronous network simulation
 * with a deterministic, single-threaded simulation loop using [[StepDoer]].
 */
class ConsensusParticipantSdmSyncTest extends ScalaCheckSuite {

	ScribeConfig.init(deleteLogFilesOnLaunch = true, modifiers = List(new LogModifier {
		override def id: String = "simulated-failures-filter"

		override def priority: Priority = Priority.Normal

		private def isSimulatedFailure(throwable: Throwable): Boolean = {
			val msg = throwable.getMessage
			msg != null && (
				msg.startsWith("Net: simulated failure") ||
					msg.startsWith("Net: target node is down") ||
					msg.startsWith("Network packet") ||
					msg.startsWith("Node ")
				)
		}

		override def apply(record: LogRecord): Option[LogRecord] = {
			if true then {
				val mappedMessages: List[scribe.message.LoggableMessage] = record.messages.map {
					case TraceLoggableMessage(throwable) if isSimulatedFailure(throwable) => LoggableMessage[String](TextOutput.apply)(throwable.getMessage)
					case x => x
				}
				Some(record.copy(messages = mappedMessages))
			} else {
				val filteredMessages = record.messages.filterNot {
					case TraceLoggableMessage(throwable) if isSimulatedFailure(throwable) => true
					case _ => false
				}
				if filteredMessages.size < record.messages.size then Some(record.copy(messages = filteredMessages)) else Some(record)
			}
		}

		override def withId(id: String): LogModifier = this
	}))

	override def scalaCheckTestParameters: Parameters = super.scalaCheckTestParameters.withMinSuccessfulTests(50)

	/** Randomizable electorate parameters for a participant node. */
	private case class NodeConfig(
		remembersLastAppliedCommandIndex: Boolean,
		maxRecursionDepth: Int,
		logCompactionThreshold: Int,
		maxInFlightAppendsPerPeer: Int,
		logRetentionAfterSnapshot: Int
	)

	private val genNodeConfig: Gen[NodeConfig] = for {
		remembersLastAppliedCommandIndex <- Gen.oneOf(true, false)
		maxRecursionDepth <- Gen.oneOf(0, 1, 9)
		logCompactionThreshold <- Gen.oneOf(3, 5)
		maxInFlightAppendsPerPeer <- Gen.oneOf(1, 2, 9)
		logRetentionAfterSnapshot <- Gen.oneOf(0, 1, 3)
	} yield NodeConfig(
		remembersLastAppliedCommandIndex,
		maxRecursionDepth,
		logCompactionThreshold,
		maxInFlightAppendsPerPeer,
		logRetentionAfterSnapshot
	)

	/** Drives a discrete-event simulation of the consensus cluster using [[ConsensusEnvironment]].
	 *
	 * Interleaves node execution, pseudorandom packet delivery and drops, virtual time advancement,
	 * client command submission and retries, and optional electorate noise.
	 */
	private def runSimulation(
		clusterSize: Int,
		initialConfigMask: IArray[Boolean],
		randomnessSeed: Long,
		startWithHighestPriorityParticipant: Boolean,
		numberOfCommandsToSend: Int = 20,
		requestFailurePercentage: Int = 10,
		responseFailurePercentage: Int = 10,
		electorateChangeProbability: Float = 0.0f,
		maxRetries: Int = 25,
		remembersLastAppliedCommandIndex: Boolean = false,
		logCompactionThreshold: Int = 5,
		maxInFlightAppendsPerPeer: Int = 1,
		logRetentionAfterSnapshot: Int = 0
	): Unit = {
		val random = new Random(randomnessSeed)
		val initialSeedSet: Set[NodeId] = (0 until clusterSize).filter(initialConfigMask).map(i => NodeId(s"p-$i")).toSet
		val initialSeeds: Set[NodeId] = if initialSeedSet.isEmpty then Set(NodeId("p-0")) else initialSeedSet

		val env = new ConsensusEnvironment(
			clusterSize = clusterSize,
			initialSeedParticipants = Some(initialSeeds),
			maxInFlightAppendsPerPeer = maxInFlightAppendsPerPeer,
			logCompactionThreshold = logCompactionThreshold,
			initialRetiringParticipantMaxRetries = 5,
			initialLogRetentionAfterSnapshot = logRetentionAfterSnapshot
		)
		env.remembersLastAppliedCommandIndex = remembersLastAppliedCommandIndex

		// Production lifecycle: stop nodes when onNodeQuiesced fires
		env.onNodeQuiescedHook = (node, _) => node.release()

		env.rpcLifecycleListener = Some(new RpcLifecycleListener {
			private def formatRpc(rpc: ConsensusRpc): String = rpc match {
				case ConsensusRpc.HowAreYou(inquirerInfo) =>
					s"HowAreYou(inquirerInfo=$inquirerInfo)"
				case ConsensusRpc.ChooseALeader(inquirerId, inquirerInfo) =>
					s"ChooseALeader(inquirerId=$inquirerId, inquirerInfo=$inquirerInfo)"
				case ConsensusRpc.AppendRecords(inquirerTerm, prevLogIndex, prevLogTerm, batch, leaderCommit, termAtLeaderCommit) =>
					val recordsStr = batch.mkString("[", ", ", "]")
					s"AppendRecords(inquirerTerm:$inquirerTerm, previousLogIndex:$prevLogIndex, previousLogTerm:$prevLogTerm, records:$recordsStr, leaderCommit:$leaderCommit, termAtLeaderCommit:$termAtLeaderCommit)"
				case ConsensusRpc.InstallSnapshot(inquirerTerm, snapshot, batch, leaderCommit, termAtLeaderCommit) =>
					val recordsStr = batch.mkString("[", ", ", "]")
					s"InstallSnapshot(inquirerTerm:$inquirerTerm, snapshot:$snapshot, records:$recordsStr, leaderCommit:$leaderCommit, termAtLeaderCommit:$termAtLeaderCommit)"
				case ConsensusRpc.PermitQuiescence(indexOfGrantedSec) =>
					s"PermitQuiescence(indexOfGrantedSec=$indexOfGrantedSec)"
			}

			override def onRpcEnqueued(req: RequestPacket, travelingCount: Int): Unit = {
				val senderRole = env.nodeRole(req.source)
				scribe.trace(s"${req.source} >- ${req.destination}: (${req.id}):${formatRpc(req.rpc)}, sent as $senderRole, $travelingCount messages on the way")
			}

			override def onRpcDelivering(req: RequestPacket, travelingCount: Int): Unit = {
				scribe.trace(s"${req.source} -> ${req.destination}: (${req.id}):${formatRpc(req.rpc)}, $travelingCount messages are traveling.")
			}

			override def onRpcCompleted(req: RequestPacket, outcome: Try[Any], travelingCount: Int): Unit = {
				val replierRole = env.nodeRole(req.destination)
				val resStr = outcome match {
					case Success(res) => s"`$res`"
					case Failure(e) => s"Failure($e)"
				}
				scribe.trace(s"${req.source} -< ${req.destination}: (${req.id}):${formatRpc(req.rpc)} returned $resStr as $replierRole, $travelingCount messages are traveling.")
			}

			override def onResponseDelivered(resp: ResponsePacket, travelingCount: Int): Unit = {
				scribe.trace(s"${resp.destination} <- ${resp.source}: (${resp.correlationRequestId}):${resp.response}, $travelingCount messages on the way")
			}
		})

		// Start initial seed participants
		env.startAllNodes()
		env.runAllNodesUntilIdle()

		var currentActiveParticipants: Set[NodeId] = initialSeeds
		val knownParticipants: mutable.Set[NodeId] = mutable.Set.from(initialSeeds)
		var currentTargetParticipant: NodeId = if startWithHighestPriorityParticipant then initialSeeds.toSeq.sorted.head else initialSeeds.toSeq.sorted.last
		val triedParticipantsForCommand: mutable.Set[NodeId] = mutable.Set.empty

		var currentCommandSerial = 1
		var activeCommandHandle: Option[ClientCommandHandle] = None
		var currentAttemptFlag: CommandAttemptFlag = FIRST_ATTEMPT
		var commandRetries = 0

		var activeElectorateChangeHandle: Option[ElectorateChangeHandle] = None

		val maxTotalSteps = 60000
		var totalSteps = 0

		while currentCommandSerial <= numberOfCommandsToSend && totalSteps < maxTotalSteps do {
			totalSteps += 1

			// 1. Drain pending node tasks
			env.runAllNodesUntilIdle()

			// 2. Manage client commands
			if activeCommandHandle.isEmpty then {
				val handle = env.submitClientCommand(
					targetNode = currentTargetParticipant,
					client = "c-1",
					serial = Some(currentCommandSerial),
					attemptFlag = currentAttemptFlag
				)
				activeCommandHandle = Some(handle)
				triedParticipantsForCommand.add(currentTargetParticipant)
				env.runAllNodesUntilIdle()
			} else {
				val handle = activeCommandHandle.get
				env.clientCommandStatus(handle.commandId) match {
					case ClientCommandStatus.Processed(_, _) =>
						activeCommandHandle = None
						currentCommandSerial += 1
						commandRetries = 0
						triedParticipantsForCommand.clear()
						currentAttemptFlag = FIRST_ATTEMPT

					case ClientCommandStatus.Redirected(leaderId) =>
						activeCommandHandle = None
						currentTargetParticipant = leaderId
						knownParticipants.add(leaderId)
						if currentAttemptFlag == FALLBACK && triedParticipantsForCommand.contains(leaderId) then {
							triedParticipantsForCommand.add(leaderId)
						}
						currentAttemptFlag = REDIRECTED

					case ClientCommandStatus.Unable(nextAttemptFlag, otherParticipants) =>
						activeCommandHandle = None
						knownParticipants ++= otherParticipants
						val untried = knownParticipants.find(p => !triedParticipantsForCommand.contains(p) && !env.node(p).isDown)
						untried match {
							case Some(next) =>
								currentTargetParticipant = next
								currentAttemptFlag = nextAttemptFlag
							case None =>
								commandRetries += 1
								if commandRetries > maxRetries then {
									throw new AssertionError(s"Cluster unable to progress command $currentCommandSerial after $maxRetries cycles across participants $knownParticipants")
								}
								triedParticipantsForCommand.clear()
								val anyUp = knownParticipants.find(p => !env.node(p).isDown).getOrElse(knownParticipants.head)
								currentTargetParticipant = anyUp
								currentAttemptFlag = nextAttemptFlag
								if env.pendingWakeUps.nonEmpty then {
									val earliestTime = env.pendingWakeUps.map(_.scheduledTime).min
									val dt = math.max(1, earliestTime - env.currentVirtualTime)
									env.advanceTime(dt)
									env.runAllNodesUntilIdle()
								}
						}

					case ClientCommandStatus.Failed(cause) =>
						activeCommandHandle = None
						commandRetries += 1
						if commandRetries > maxRetries then {
							throw new AssertionError(s"Command $currentCommandSerial failed after $maxRetries retries: ${cause.getMessage}", cause)
						}
						val untried = knownParticipants.find(p => !triedParticipantsForCommand.contains(p) && !env.node(p).isDown)
						untried match {
							case Some(next) => currentTargetParticipant = next
							case None =>
								triedParticipantsForCommand.clear()
								currentTargetParticipant = knownParticipants.find(p => !env.node(p).isDown).getOrElse(knownParticipants.head)
						}
						currentAttemptFlag = FALLBACK

					case ClientCommandStatus.InFlight =>
					// Processing
				}
			}

			// 3. Optional electorate change noise
			if electorateChangeProbability > 0.0f then {
				if activeElectorateChangeHandle.isEmpty then {
					if random.nextFloat() < electorateChangeProbability then {
						val newMask = Array.fill(clusterSize)(random.nextBoolean())
						if !newMask.contains(true) then newMask(random.nextInt(clusterSize)) = true
						val desiredParticipants = (0 until clusterSize).filter(newMask).map(i => NodeId(s"p-$i")).toSet
						if desiredParticipants != currentActiveParticipants then {
							// Start nodes BEFORE requesting electorate change
							for id <- desiredParticipants do {
								val n = env.node(id)
								if n.isDown then env.startNode(id)
							}
							env.runAllNodesUntilIdle()

							val targetNode = env.leaderByTermMap.values.lastOption.getOrElse(currentTargetParticipant)
							val ccHandle = env.submitElectorateChange(targetNode, desiredParticipants)
							activeElectorateChangeHandle = Some(ccHandle)
							env.runAllNodesUntilIdle()
						}
					}
				} else {
					val ccHandle = activeElectorateChangeHandle.get
					env.electorateChangeStatus(ccHandle.requestId) match {
						case ElectorateChangeStatus.Completed(res) =>
							activeElectorateChangeHandle = None
							res match {
								case _: (SUCCESSFULLY_CHANGED | ALREADY_CHANGED) =>
									currentActiveParticipants = ccHandle.desiredParticipants
									knownParticipants ++= currentActiveParticipants
								case _ => ()
							}
						case ElectorateChangeStatus.Failed(_) =>
							activeElectorateChangeHandle = None
						case ElectorateChangeStatus.InFlight =>
						// In-flight
					}
				}
			}

			// 4. Packet delivery or drop
			val packets = env.pendingPackets
			if packets.nonEmpty then {
				val chosenPacket = packets(random.nextInt(packets.size))
				val isRequest = chosenPacket.isInstanceOf[RequestPacket]
				val failureRate = if isRequest then requestFailurePercentage else responseFailurePercentage
				if random.nextInt(100) < failureRate then {
					env.dropPacket(chosenPacket.id)
				} else {
					env.deliverPacket(chosenPacket.id)
				}
				env.runAllNodesUntilIdle()
			} else if env.pendingWakeUps.nonEmpty then {
				// 5. Advance virtual time to earliest wakeup
				val earliestTime = env.pendingWakeUps.map(_.scheduledTime).min
				val dt = math.max(1, earliestTime - env.currentVirtualTime)
				env.advanceTime(dt)
				env.runAllNodesUntilIdle()
			}
		}

		if totalSteps >= maxTotalSteps then {
			throw new AssertionError(s"Simulation reached step limit of $maxTotalSteps. Commands sent: ${currentCommandSerial - 1}/$numberOfCommandsToSend")
		}

		// 6. Graceful shutdown: request electorate change to empty set
		var shutdownAttempts = 0
		val maxShutdownAttempts = 15
		var shutdownCompleted = false

		while !shutdownCompleted && shutdownAttempts < maxShutdownAttempts do {
			shutdownAttempts += 1
			val activeLeader = env.leaderByTermMap.values.lastOption.getOrElse(currentTargetParticipant)
			val shutdownHandle = env.submitElectorateChange(activeLeader, Set.empty[NodeId])
			env.runAllNodesUntilIdle()

			var innerSteps = 0
			val maxInnerSteps = 500
			while innerSteps < maxInnerSteps && env.electorateChangeStatus(shutdownHandle.requestId) == ElectorateChangeStatus.InFlight do {
				innerSteps += 1
				val packets = env.pendingPackets
				if packets.nonEmpty then {
					val p = packets(random.nextInt(packets.size))
					env.deliverPacket(p.id)
					env.runAllNodesUntilIdle()
				} else if env.pendingWakeUps.nonEmpty then {
					val earliest = env.pendingWakeUps.map(_.scheduledTime).min
					val dt = math.max(1, earliest - env.currentVirtualTime)
					env.advanceTime(dt)
					env.runAllNodesUntilIdle()
				} else {
					innerSteps = maxInnerSteps
				}
			}

			env.electorateChangeStatus(shutdownHandle.requestId) match {
				case ElectorateChangeStatus.Completed(res) =>
					res match {
						case _: TerminalElectorateChangeResponse => shutdownCompleted = true
						case _ => ()
					}
				case _ => ()
			}

			if env.allNodes.forall(n => n.isDown || n.participant == null || n.participant.getRoleOrdinal == QUIESCED) then {
				shutdownCompleted = true
			}
		}

		// 7. Final invariant verification
		env.checkLogMatching()
	}

	test("assertions are enabled") {
		if !ConsensusParticipantSdm.assertionsEnabled then println("Enable assertions for all tests to detect more bugs by adding the -ea VM option.")
		assert(ConsensusParticipantSdm.assertionsEnabled, "Assertions are not enabled. Enable VM option -ea.")
	}

	test("deterministic simulation baseline") {
		val initialMask = IArray(true, true, true)
		runSimulation(
			clusterSize = 3,
			initialConfigMask = initialMask,
			randomnessSeed = 42L,
			startWithHighestPriorityParticipant = true,
			numberOfCommandsToSend = 10,
			requestFailurePercentage = 0,
			responseFailurePercentage = 0,
			electorateChangeProbability = 0.0f
		)
	}

	test("deterministic simulation with network failures") {
		val initialMask = IArray(true, true, true)
		runSimulation(
			clusterSize = 3,
			initialConfigMask = initialMask,
			randomnessSeed = 12345L,
			startWithHighestPriorityParticipant = false,
			numberOfCommandsToSend = 10,
			requestFailurePercentage = 10,
			responseFailurePercentage = 10,
			electorateChangeProbability = 0.0f
		)
	}

	property("All invariants must comply - without electorate changes noise") {
		Prop.forAll(
			Gen.choose(2, 5),
			Gen.oneOf(true, false),
			Gen.long,
			genNodeConfig
		) { (clusterSize, startWithHighestPriorityParticipant, netRandomnessSeed, nodeConfig) =>
			val initialMask = IArray.fill(clusterSize)(true)
			runSimulation(
				clusterSize = clusterSize,
				initialConfigMask = initialMask,
				randomnessSeed = netRandomnessSeed,
				startWithHighestPriorityParticipant = startWithHighestPriorityParticipant,
				numberOfCommandsToSend = 10,
				requestFailurePercentage = 10,
				responseFailurePercentage = 10,
				electorateChangeProbability = 0.0f,
				maxRetries = 25,
				remembersLastAppliedCommandIndex = nodeConfig.remembersLastAppliedCommandIndex,
				logCompactionThreshold = nodeConfig.logCompactionThreshold,
				maxInFlightAppendsPerPeer = nodeConfig.maxInFlightAppendsPerPeer,
				logRetentionAfterSnapshot = nodeConfig.logRetentionAfterSnapshot
			)
			true
		}
	}

	property("All invariants must comply - with electorate changes noise".ignore) {
		Prop.forAll(
			Gen.choose(3, 5),
			Gen.oneOf(true, false),
			Gen.long,
			genNodeConfig
		) { (clusterSize, startWithHighestPriorityParticipant, netRandomnessSeed, nodeConfig) =>
			val initialMask = IArray.fill(clusterSize)(true)
			runSimulation(
				clusterSize = clusterSize,
				initialConfigMask = initialMask,
				randomnessSeed = netRandomnessSeed,
				startWithHighestPriorityParticipant = startWithHighestPriorityParticipant,
				numberOfCommandsToSend = 10,
				requestFailurePercentage = 10,
				responseFailurePercentage = 10,
				electorateChangeProbability = 0.03f,
				maxRetries = 25,
				remembersLastAppliedCommandIndex = nodeConfig.remembersLastAppliedCommandIndex,
				logCompactionThreshold = nodeConfig.logCompactionThreshold,
				maxInFlightAppendsPerPeer = nodeConfig.maxInFlightAppendsPerPeer,
				logRetentionAfterSnapshot = nodeConfig.logRetentionAfterSnapshot
			)
			true
		}
	}

	/** Regression table for seeds that previously exposed failures during discrete-event simulation.
	 * Initialized as empty per design; entries are added upon discovering reproducible failing cases.
	 */
	test("Previous failing cases") {
		type FailingCase = (
			numberOfCommandsToSend: Int,
			clusterSize: Int,
			startWithHighestPriorityParticipant: Boolean,
			netRandomnessSeed: Long,
			remembersLastAppliedCommandIndex: Boolean,
			maxRecursionDepth: Int,
			logCompactionThreshold: Int,
			maxInFlightAppendsPerPeer: Int,
			logRetentionAfterSnapshot: Int
		)

		val failingCases = Seq.empty[FailingCase]

		for (caseData <- failingCases) {
			val (commands, size, startHigh, seed, remembersIndex, _, compaction, maxInFlight, retention) = caseData
			val mask = IArray.fill(size)(true)
			runSimulation(
				clusterSize = size,
				initialConfigMask = mask,
				randomnessSeed = seed,
				startWithHighestPriorityParticipant = startHigh,
				numberOfCommandsToSend = commands,
				requestFailurePercentage = 10,
				responseFailurePercentage = 10,
				electorateChangeProbability = 0.0f,
				remembersLastAppliedCommandIndex = remembersIndex,
				logCompactionThreshold = compaction,
				maxInFlightAppendsPerPeer = maxInFlight,
				logRetentionAfterSnapshot = retention
			)
		}
	}
}
