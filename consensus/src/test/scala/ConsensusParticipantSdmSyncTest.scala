package readren.consensus

import ConsensusParticipantSdm.*
import readren.consensus.env.*
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
import scribe.{Level, LogRecord, Priority}

import scala.collection.immutable.{ListMap, ListSet}
import scala.collection.mutable
import scala.util.{Failure, Random, Success, Try}

object ConsensusParticipantSdmSyncTest {
	final val MAX_PERMIT_QUIESCENCE_RETRIES: Int = 9
}

/** A synchronous, discrete-event invariant verification test suite for [[ConsensusParticipantSdm]],
 * backed by [[ConsensusEnvironment]].
 *
 * Replaces the multi-threaded cooperative scheduler and asynchronous network simulation
 * with a deterministic, single-threaded simulation loop using [[StepDoer]].
 */
class ConsensusParticipantSdmSyncTest extends ScalaCheckSuite {

	import ConsensusParticipantSdmSyncTest.*

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

	override def scalaCheckTestParameters: Parameters = super.scalaCheckTestParameters.withMinSuccessfulTests(10000)

	/** Randomizable electorate parameters for a participant node. */
	private case class NodeConfig(
		remembersLastAppliedCommandIndex: Boolean,
		logCompactionThreshold: Int,
		maxInFlightAppendsPerPeer: Int,
		logRetentionAfterSnapshot: Int
	)

	private val genNodeConfig: Gen[NodeConfig] = for {
		remembersLastAppliedCommandIndex <- Gen.oneOf(true, false)
		logCompactionThreshold <- Gen.oneOf(3, 5)
		maxInFlightAppendsPerPeer <- Gen.oneOf(1, 2, 9)
		logRetentionAfterSnapshot <- Gen.oneOf(0, 1, 3)
	} yield NodeConfig(
		remembersLastAppliedCommandIndex,
		logCompactionThreshold,
		maxInFlightAppendsPerPeer,
		logRetentionAfterSnapshot
	)

	/** Fault mode applied to inter-node channels during an active network partition. */
	private enum PartitionFaultMode {
		case Drop
		case Buffer
	}

	/** Synthetic client identifier used for simulated client command submissions. */
	private val TestClientId: String = "c-1"

	/** Denominator basis for percentage probability evaluations. */
	private val PercentageBasis: Int = 100

	/** Drives a discrete-event simulation of the consensus cluster using [[ConsensusEnvironment]].\
	 * Interleaves node execution, pseudorandom packet delivery and drops, virtual time advancement,
	 * client command submission and retries, dynamic electorate noise, and network partitions.
	 * @param clusterSize total number of nodes participating in the simulation cluster.
	 * @param initialConfigMask bitmask indicating which node indices belong to the initial seed electorate.
	 * @param randomnessSeed seed value for deterministic pseudorandom number generation.
	 * @param startWithHighestPriorityParticipant true to target p-0 initially; false to target the highest node index.
	 * @param numberOfCommandsToSend total number of client commands to submit before initiating graceful shutdown.
	 * @param requestFailurePercentage integer percentage [0, 100] of outbound RPC request packets to drop.
	 * @param responseFailurePercentage integer percentage [0, 100] of outbound RPC response packets to drop.
	 * @param electorateChangeProbability probability per commit event of scheduling a dynamic noise electorate change.
	 * @param maxRetries retry budget for client commands and graceful shutdown before asserting non-progress.
	 * @param remembersLastAppliedCommandIndex whether participants persist their last applied command index.
	 * @param logCompactionThreshold log size threshold triggering background log compaction and snapshotting.
	 * @param maxInFlightAppendsPerPeer maximum concurrent unacknowledged append RPCs permitted per follower pipeline.
	 * @param logRetentionAfterSnapshot number of historical log entries retained behind snapshot compaction boundaries.
	 * @param computeDomainProbability probability of arbitrating execution steps to compute instead of network.
	 * @param ticksPerNodeCycle virtual time ticks deducted per compute node execution step.
	 * @param asymmetricSplitProbability probability of asymmetric channel cuts during network partitions.
	 * @param bufferPartitionProbability probability of buffering rather than dropping packets during partitions.
	 * @param maxPartitionWakeUps leader wake-up count threshold triggering early partition healing.
	 * @param partitionBaseStepsPerChannel base simulation steps per channel defining the partition epoch duration.
	 * @param healedBaseStepsPerChannel base simulation steps per channel defining the healed network epoch duration.
	 * @param emaSmoothingDecay decay factor for updating the exponential moving average of commit intervals.
	 * @param retiringParticipantMaxRetries retry quota for unreachable retirees before decommissioning under empty electorate.
	 * @param scribeLevel minimum Scribe logging level active during the simulation.
	 * @return total number of dynamic noise electorate changes injected throughout the simulation. */
	private def runSimulation(
		clusterSize: Int,
		initialConfigMask: IArray[Boolean],
		randomnessSeed: Long,
		startWithHighestPriorityParticipant: Boolean,
		numberOfCommandsToSend: Int,
		requestFailurePercentage: Int,
		responseFailurePercentage: Int,
		electorateChangeProbability: Float,
		maxRetries: Int,
		remembersLastAppliedCommandIndex: Boolean,
		logCompactionThreshold: Int,
		maxInFlightAppendsPerPeer: Int,
		logRetentionAfterSnapshot: Int,
		computeDomainProbability: Float,
		ticksPerNodeCycle: Int,
		asymmetricSplitProbability: Float,
		bufferPartitionProbability: Float,
		maxPartitionWakeUps: Int,
		partitionBaseStepsPerChannel: Int,
		healedBaseStepsPerChannel: Int,
		emaSmoothingDecay: Double,
		retiringParticipantMaxRetries: Int,
		scribeLevel: Level,
		maxPermitQuiescenceRetries: Int = MAX_PERMIT_QUIESCENCE_RETRIES
	): Int = {
		scribe.Logger.root.withMinimumLevel(scribeLevel).replace()
		scribe.info(s"\n----------------\nBegin: clusterSize=$clusterSize, initialConfig=${initialConfigMask.mkString("[", ", ", "]")}, randomnessSeed=$randomnessSeed, startWithHighestPriorityParticipant=$startWithHighestPriorityParticipant, numberOfCommandsToSend=$numberOfCommandsToSend, requestFailurePercentage=$requestFailurePercentage, responseFailurePercentage=$responseFailurePercentage, electorateChangeProbability=$electorateChangeProbability, maxRetries=$maxRetries, remembersLastAppliedCommandIndex=$remembersLastAppliedCommandIndex, logCompactionThreshold=$logCompactionThreshold, maxInFlightAppendsPerPeer=$maxInFlightAppendsPerPeer, logRetentionAfterSnapshot=$logRetentionAfterSnapshot, computeDomainProbability=$computeDomainProbability, ticksPerNodeCycle=$ticksPerNodeCycle, asymmetricSplitProbability=$asymmetricSplitProbability, bufferPartitionProbability=$bufferPartitionProbability, maxPartitionWakeUps=$maxPartitionWakeUps, partitionBaseStepsPerChannel=$partitionBaseStepsPerChannel, healedBaseStepsPerChannel=$healedBaseStepsPerChannel, emaSmoothingDecay=$emaSmoothingDecay, retiringParticipantMaxRetries=$retiringParticipantMaxRetries, scribeLevel=$scribeLevel\n($clusterSize, IArray(${initialConfigMask.mkString(", ")}), ${randomnessSeed}L, $startWithHighestPriorityParticipant, $numberOfCommandsToSend, $requestFailurePercentage, $responseFailurePercentage, ${electorateChangeProbability}f, $maxRetries, $remembersLastAppliedCommandIndex, $logCompactionThreshold, $maxInFlightAppendsPerPeer, $logRetentionAfterSnapshot, ${computeDomainProbability}f, $ticksPerNodeCycle, ${asymmetricSplitProbability}f, ${bufferPartitionProbability}f, $maxPartitionWakeUps, $partitionBaseStepsPerChannel, $healedBaseStepsPerChannel, $emaSmoothingDecay, $retiringParticipantMaxRetries),")
		val testCompletionHeader = "**** TEST COMPLETED! **** Stopping the Net. Result:"
		try {
			val random = new Random(randomnessSeed)
			val initialSeedSet: Set[NodeId] = (0 until clusterSize).filter(initialConfigMask).map(i => NodeId(s"p-$i")).toSet
			val initialSeeds: Set[NodeId] = if initialSeedSet.isEmpty then Set(NodeId("p-0")) else initialSeedSet

			val env = new ConsensusEnvironment(
				clusterSize = clusterSize,
				initialSeedParticipants = Some(initialSeeds),
				maxInFlightAppendsPerPeer = maxInFlightAppendsPerPeer,
				logCompactionThreshold = logCompactionThreshold,
				initialRetiringParticipantMaxRetries = retiringParticipantMaxRetries,
				initialLogRetentionAfterSnapshot = logRetentionAfterSnapshot,
				remembersLastAppliedCommandIndex = remembersLastAppliedCommandIndex,
				maxPermitQuiescenceRetries = maxPermitQuiescenceRetries
			)


			env.network.rpcLifecycleListener = Some(new RpcLifecycleListener {
				private def formatRpc(rpc: ConsensusRpc): String = rpc match {
					case ConsensusRpc.HowAreYou(inquirerInfo) =>
						s"HowAreYou(inquirerInfo=$inquirerInfo)"
					case ConsensusRpc.RequestBallot(inquirerId, inquirerInfo) =>
						s"RequestBallot(inquirerId=$inquirerId, inquirerInfo=$inquirerInfo)"
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

			// --- Simulation Derived Metrics & Timing Bounds ---
			/** Maximum duration in simulation steps of an active network partition. */
			val maxPartitionSteps: Int = (clusterSize * (clusterSize - 1) * partitionBaseStepsPerChannel).max(1)

			/** Target duration in simulation steps of a healed network connectivity epoch. */
			val healedEpochStepsTarget: Int = (clusterSize * (clusterSize - 1) * healedBaseStepsPerChannel).max(1)

			/** Timeout threshold for in-flight client commands and electorate changes, spanning a partition and healed cycle. */
			val maxInFlightSteps: Int = maxPartitionSteps + healedEpochStepsTarget

			/** Maximum graceful shutdown attempts before asserting failure. */
			val maxShutdownAttempts: Int = maxRetries

			/** Workload-scaled total simulation step budget bounding the main execution loop. */
			val maxTotalSteps: Int = ((numberOfCommandsToSend * maxRetries) + (maxShutdownAttempts * clusterSize)) * (maxPartitionSteps + healedEpochStepsTarget) / clusterSize

			/** Initial estimate for the exponential moving average of elapsed steps between consecutive commits. */
			val initialEmaStepsBetweenCommits: Double = (clusterSize + 1).toDouble * 4.0

			/** Weight applied to new commit interval delta samples in the exponential moving average calculation. */
			val emaSmoothingWeight: Double = 1.0 - emaSmoothingDecay

			/** Upper bound on commit interval delta samples to prevent partition stalls from distorting the moving average. */
			val maxEmaDeltaCap: Int = (maxPartitionSteps / 4).max(50)

			/** Backoff delay in simulation steps before resubmitting a secluded graceful shutdown request. */
			val shutdownBackoffSteps: Int = clusterSize * ticksPerNodeCycle

			// --- Reconfiguration Coordination and Client State ---
			/** Dedicated simulated client (Tier 3) driving business command submissions and target failover. */
			val client: SimulatedClient = new SimulatedClient(
				clientId = TestClientId,
				env = env,
				initialKnownEndpoints = initialSeeds,
				maxInFlightSteps = maxInFlightSteps,
				maxRetries = maxRetries
			)

			/** Target participant currently addressed by administrative reconfiguration submissions. */
			var reconfigTarget: NodeId = if startWithHighestPriorityParticipant then initialSeeds.toSeq.sorted.head else initialSeeds.toSeq.sorted.last

			/** Accumulated set of all participant endpoints discovered across reconfiguration history. */
			val reconfigEndpoints: mutable.Set[NodeId] = mutable.Set.from(initialSeeds)

			/** Elapsed discrete simulation step counter. */
			var totalSteps: Int = 0

			/** Shared tracker for in-flight electorate change requests submitted by ElectorateNoise and GracefulShutdown. */
			object ElectorateTracker {
				/** In-flight tracking container recording the submission handle, elapsed steps, and retries. */
				final class InFlightElectorateChange(val handle: ElectorateChangeHandle, var inFlightSteps: Int = 0, var retries: Int = 0)

				/** Active map of in-flight electorate changes indexed by unique request ID. */
				val inFlightElectorateChanges: mutable.Map[String, InFlightElectorateChange] = mutable.Map.empty

				/** Rotates across a candidate pool of participants, preserving candidate pool priority order.
				 * @param candidatesPool the candidate set to choose from.
				 * @param currentTarget the participant identifier that was previously targeted. */
				def rotateAmong(candidatesPool: Iterable[NodeId], currentTarget: NodeId): Option[NodeId] = {
					val candidates = candidatesPool.toSeq.distinct
					if candidates.nonEmpty then {
						candidates.find(_ != currentTarget).orElse(candidates.headOption)
					} else None
				}

				/** Rotates the target participant across all presumed active participants or known reconfiguration endpoints.
				 * @param currentTarget the participant identifier that was previously targeted. */
				def rotateTargetParticipant(currentTarget: NodeId): Unit = {
					val activePool = if env.controlPlane.presumedActiveParticipants.nonEmpty then env.controlPlane.presumedActiveParticipants else reconfigEndpoints
					val candidates = activePool.toSeq.sorted
					if candidates.nonEmpty then {
						val nextIdx = (candidates.indexOf(currentTarget) + 1) % candidates.size
						reconfigTarget = candidates(nextIdx)
					}
				}

				/** Polls the status of all active electorate changes, updating cluster membership and handling completions or timeouts. */
				def pollActiveElectorateChanges(): Unit = {
					if inFlightElectorateChanges.nonEmpty then {
						val completedReqIds = mutable.ArrayBuffer.empty[String]
						val newInFlight = mutable.ArrayBuffer.empty[InFlightElectorateChange]
						for (reqId, inFlight) <- inFlightElectorateChanges do {
							val currentHandle = inFlight.handle
							if NetworkPartitions.isPartitionActive && inFlight.retries > maxRetries / 2 then {
								NetworkPartitions.healPartition()
							}
							env.electorateChangeStatus(reqId) match {
								case ElectorateChangeStatus.Completed(res) =>
									if currentHandle.desiredParticipants.isEmpty then {
										completedReqIds += reqId
										scribe.trace(s"GracefulShutdown completed $reqId on ${currentHandle.targetNodeId} with $res")
										if res.isTerminal then {
											GracefulShutdown.shutdownCommitted = true
											env.controlPlane.onReconfigurationTerminal(Set.empty, isSuccessful = true)
										} else {
											res match {
												case ElectorateChangeResponse.AskTheLeader(suggested) =>
													val leaderNodeId = suggested.head
													reconfigTarget = leaderNodeId
													reconfigEndpoints ++= suggested
													GracefulShutdown.shutdownBackoffRemaining = 0
												case _: ElectorateChangeResponse.Excluded[_] | _: ElectorateChangeResponse.Stopped[_] =>
													env.controlPlane.onParticipantExcluded(currentHandle.targetNodeId)
													reconfigEndpoints.remove(currentHandle.targetNodeId)
													val candidate = rotateAmong(res.suggestedParticipants, currentHandle.targetNodeId).getOrElse {
														rotateTargetParticipant(currentHandle.targetNodeId)
														reconfigTarget
													}
													reconfigTarget = candidate
													GracefulShutdown.shutdownBackoffRemaining = 0
												case _: ElectorateChangeResponse.CatchingUp[_] =>
													env.controlPlane.onParticipantExcluded(currentHandle.targetNodeId)
													val candidate = rotateAmong(res.suggestedParticipants, currentHandle.targetNodeId).getOrElse {
														rotateTargetParticipant(currentHandle.targetNodeId)
														reconfigTarget
													}
													reconfigTarget = candidate
													GracefulShutdown.shutdownBackoffRemaining = 0
												case _: ElectorateChangeResponse.AnotherChangeInProgress[_] =>
													val candidate = rotateAmong(res.suggestedParticipants, currentHandle.targetNodeId).getOrElse {
														rotateTargetParticipant(currentHandle.targetNodeId)
														reconfigTarget
													}
													reconfigTarget = candidate
													GracefulShutdown.shutdownBackoffRemaining = shutdownBackoffSteps
												case _ =>
													val candidate = rotateAmong(res.suggestedParticipants, currentHandle.targetNodeId).getOrElse {
														rotateTargetParticipant(currentHandle.targetNodeId)
														reconfigTarget
													}
													reconfigTarget = candidate
													GracefulShutdown.shutdownBackoffRemaining = shutdownBackoffSteps
											}
										}
									} else {
										completedReqIds += reqId
										res match {
											case ElectorateChangeResponse.SuccessfullyChanged | ElectorateChangeResponse.AlreadyChanged =>
												env.controlPlane.onReconfigurationTerminal(currentHandle.desiredParticipants, isSuccessful = true)
												reconfigEndpoints ++= currentHandle.desiredParticipants
											case ElectorateChangeResponse.AskTheLeader(suggested) =>
												val leaderNodeId = suggested.head
												reconfigTarget = leaderNodeId
												reconfigEndpoints ++= suggested
												if inFlight.retries < maxRetries then {
													val newHandle = env.submitElectorateChange(reconfigTarget, currentHandle.desiredParticipants)
													newInFlight += new InFlightElectorateChange(newHandle, 0, inFlight.retries + 1)
												} else {
													env.controlPlane.onReconfigurationTerminal(currentHandle.desiredParticipants, isSuccessful = false)
												}
											case _: ElectorateChangeResponse.Excluded[_] | _: ElectorateChangeResponse.Stopped[_] =>
												env.controlPlane.onParticipantExcluded(currentHandle.targetNodeId)
												reconfigEndpoints.remove(currentHandle.targetNodeId)
												if inFlight.retries < maxRetries then {
													val candidatePool = res.suggestedParticipants ++ env.controlPlane.presumedActiveParticipants
													val nextTarget = rotateAmong(candidatePool, currentHandle.targetNodeId).getOrElse {
														rotateTargetParticipant(currentHandle.targetNodeId)
														reconfigTarget
													}
													reconfigTarget = nextTarget
													val newHandle = env.submitElectorateChange(reconfigTarget, currentHandle.desiredParticipants)
													newInFlight += new InFlightElectorateChange(newHandle, 0, inFlight.retries + 1)
												} else {
													env.controlPlane.onReconfigurationTerminal(currentHandle.desiredParticipants, isSuccessful = false)
												}
											case _: ElectorateChangeResponse.CatchingUp[_] =>
												env.controlPlane.onParticipantExcluded(currentHandle.targetNodeId)
												if inFlight.retries < maxRetries then {
													val candidatePool = res.suggestedParticipants ++ env.controlPlane.presumedActiveParticipants
													val nextTarget = rotateAmong(candidatePool, currentHandle.targetNodeId).getOrElse {
														rotateTargetParticipant(currentHandle.targetNodeId)
														reconfigTarget
													}
													reconfigTarget = nextTarget
													val newHandle = env.submitElectorateChange(reconfigTarget, currentHandle.desiredParticipants)
													newInFlight += new InFlightElectorateChange(newHandle, 0, inFlight.retries + 1)
												} else {
													env.controlPlane.onReconfigurationTerminal(currentHandle.desiredParticipants, isSuccessful = false)
												}
											case res: ElectorateChangeResponse.AnotherChangeInProgress[_] =>
												if currentHandle.desiredParticipants == res.inProgressTargetParticipants.asInstanceOf[Set[NodeId]] then {
													if inFlight.retries < maxRetries then {
														val candidatePool = res.suggestedParticipants.asInstanceOf[Iterable[NodeId]] ++ env.controlPlane.presumedActiveParticipants
														val nextTarget = rotateAmong(candidatePool, currentHandle.targetNodeId).getOrElse {
															rotateTargetParticipant(currentHandle.targetNodeId)
															reconfigTarget
														}
														reconfigTarget = nextTarget
														val newHandle = env.submitElectorateChange(reconfigTarget, currentHandle.desiredParticipants)
														newInFlight += new InFlightElectorateChange(newHandle, 0, inFlight.retries + 1)
													} else {
														env.controlPlane.onReconfigurationTerminal(currentHandle.desiredParticipants, isSuccessful = false)
													}
												} else {
													env.controlPlane.onReconfigurationTerminal(currentHandle.desiredParticipants, isSuccessful = false)
												}
											case _ =>
												if inFlight.retries < maxRetries then {
													val candidatePool = res.suggestedParticipants ++ env.controlPlane.presumedActiveParticipants
													val nextTarget = rotateAmong(candidatePool, currentHandle.targetNodeId).getOrElse {
														rotateTargetParticipant(currentHandle.targetNodeId)
														reconfigTarget
													}
													reconfigTarget = nextTarget
													val newHandle = env.submitElectorateChange(reconfigTarget, currentHandle.desiredParticipants)
													newInFlight += new InFlightElectorateChange(newHandle, 0, inFlight.retries + 1)
												} else {
													env.controlPlane.onReconfigurationTerminal(currentHandle.desiredParticipants, isSuccessful = false)
												}
										}
									}
								case ElectorateChangeStatus.Failed(err) =>
									completedReqIds += reqId
									if currentHandle.desiredParticipants.isEmpty then {
										scribe.trace(s"GracefulShutdown failed $reqId on ${currentHandle.targetNodeId} with $err")
										rotateTargetParticipant(currentHandle.targetNodeId)
										GracefulShutdown.shutdownBackoffRemaining = shutdownBackoffSteps
									} else if inFlight.retries < maxRetries then {
										rotateTargetParticipant(currentHandle.targetNodeId)
										val newHandle = env.submitElectorateChange(reconfigTarget, currentHandle.desiredParticipants)
										newInFlight += new InFlightElectorateChange(newHandle, 0, inFlight.retries + 1)
									} else {
										env.controlPlane.onReconfigurationTerminal(currentHandle.desiredParticipants, isSuccessful = false)
									}
								case ElectorateChangeStatus.InFlight =>
									inFlight.inFlightSteps += 1
									if inFlight.inFlightSteps > maxInFlightSteps then {
										completedReqIds += reqId
										if currentHandle.desiredParticipants.isEmpty then {
											scribe.trace(s"GracefulShutdown timed out $reqId on ${currentHandle.targetNodeId} after ${inFlight.inFlightSteps} steps")
											rotateTargetParticipant(currentHandle.targetNodeId)
											GracefulShutdown.shutdownBackoffRemaining = shutdownBackoffSteps
										} else if inFlight.retries < maxRetries then {
											rotateTargetParticipant(currentHandle.targetNodeId)
											val newHandle = env.submitElectorateChange(reconfigTarget, currentHandle.desiredParticipants)
											newInFlight += new InFlightElectorateChange(newHandle, 0, inFlight.retries + 1)
										} else {
											env.controlPlane.onReconfigurationTerminal(currentHandle.desiredParticipants, isSuccessful = false)
										}
									}
							}
						}
						for reqId <- completedReqIds do inFlightElectorateChanges.remove(reqId)
						for item <- newInFlight do inFlightElectorateChanges(item.handle.requestId) = item
					}
				}
			}

			/** Injects dynamic electorate changes triggered by commit advancements. */
			object ElectorateNoise {
				/** Total steps recorded at the most recent commit watermark advancement. */
				var lastCommitStep: Int = 0
				/** Highest committed record index observed on the cluster environment. */
				var lastCommittedIndex: Long = 0L
				/** Exponential moving average of elapsed steps between consecutive commits. */
				var emaStepsBetweenCommits: Double = initialEmaStepsBetweenCommits
				/** Scheduled simulation step for submitting the next noise electorate change. */
				var scheduledElectorateChangeStep: Option[Int] = None
				/** Total count of noise electorate changes injected throughout the simulation. */
				var noiseElectorateChangesCount: Int = 0

				/** Observes commit index progression, schedules noise events, and injects configuration changes. */
				def inject(): Unit = {
					val currentMaxCommitted = env.oracle.highestCommittedRecordIndex
					if currentMaxCommitted > lastCommittedIndex then {
						val deltaSteps = totalSteps - lastCommitStep
						lastCommitStep = totalSteps
						lastCommittedIndex = currentMaxCommitted
						emaStepsBetweenCommits = emaSmoothingDecay * emaStepsBetweenCommits + emaSmoothingWeight * math.min(deltaSteps, maxEmaDeltaCap)

						if electorateChangeProbability > 0.0f && scheduledElectorateChangeStep.isEmpty && ElectorateTracker.inFlightElectorateChanges.isEmpty then {
							if random.nextFloat() < electorateChangeProbability then {
								val delay = math.max(1, (random.nextFloat() * emaStepsBetweenCommits).toInt)
								scheduledElectorateChangeStep = Some(totalSteps + delay)
							}
						}
					}

					if scheduledElectorateChangeStep.exists(totalSteps >= _) then {
						scheduledElectorateChangeStep = None
						if ElectorateTracker.inFlightElectorateChanges.isEmpty then {
							val activeCandidates = env.controlPlane.presumedActiveParticipants.toSeq.sorted
							if activeCandidates.nonEmpty then {
								val newMask = Array.fill(activeCandidates.size)(random.nextBoolean())
								if !newMask.contains(true) then newMask(random.nextInt(activeCandidates.size)) = true
								val desiredParticipants = activeCandidates.indices.filter(newMask).map(activeCandidates).toSet
								if desiredParticipants != env.controlPlane.presumedActiveParticipants then {
									val targetNode = reconfigTarget
									env.controlPlane.onReconfigurationProposed(desiredParticipants)
									val ccHandle = env.submitElectorateChange(targetNode, desiredParticipants)
									ElectorateTracker.inFlightElectorateChanges(ccHandle.requestId) = new ElectorateTracker.InFlightElectorateChange(ccHandle, 0)
									noiseElectorateChangesCount += 1
								}
							}
						}
					}

					ElectorateTracker.pollActiveElectorateChanges()
				}
			}

			/** Drives cluster graceful shutdown by proposing an empty electorate and tracking quiescence retries under network faults. */
			object GracefulShutdown {
				/** Total shutdown attempts initiated by the coordinator. */
				var shutdownAttempts: Int = 0
				/** True once an electorate change reducing participants to empty is committed. */
				var shutdownCommitted: Boolean = false
				/** Delay counter in simulation steps before resubmitting an empty electorate request after seclusion. */
				var shutdownBackoffRemaining: Int = 0
				/** Highest quiescence authorization retries observed across all nodes during post-commit shutdown. */
				var maxQuiescenceRetriesObserved: Int = 0
				/** Highest unreachable retiree replication retries observed across all nodes during post-commit shutdown. */
				var maxUnreachableRetriesObserved: Int = 0

				/** Drives graceful shutdown requests until all nodes are unseated and quiesced. */
				def drive(): Unit = {
					ElectorateNoise.scheduledElectorateChangeStep = None

					if shutdownCommitted then {
						for w <- env.clock.pendingWakeUps do {
							if w.reason == WakeUpReason.QuiescenceAuthorizationRetry && w.wakeupsDone > maxQuiescenceRetriesObserved then {
								maxQuiescenceRetriesObserved = w.wakeupsDone
							} else if w.reason == WakeUpReason.UnreachableFollowersRetry && w.wakeupsDone > maxUnreachableRetriesObserved then {
								maxUnreachableRetriesObserved = w.wakeupsDone
							}
						}
					}

					val shouldDisarm = if !shutdownCommitted then {
						shutdownAttempts > maxShutdownAttempts / 2
					} else {
						maxQuiescenceRetriesObserved >= math.max(1, maxPermitQuiescenceRetries / 2) ||
							maxUnreachableRetriesObserved >= math.max(1, env.retiringParticipantMaxRetries / 2)
					}

					if shouldDisarm && !NetworkPartitions.partitionsPermanentlyDisarmed then {
						NetworkPartitions.disarmAndHeal()
					}
					if !shutdownCommitted then {
						if shutdownBackoffRemaining > 0 then {
							shutdownBackoffRemaining -= 1
						} else if ElectorateTracker.inFlightElectorateChanges.isEmpty then {
							shutdownAttempts += 1
							if shutdownAttempts > maxShutdownAttempts then {
								throw new AssertionError(s"Cluster failed to gracefully shut down after $maxShutdownAttempts attempts across participants $reconfigEndpoints")
							}
							val candidate = reconfigTarget
							scribe.trace(s"GracefulShutdown attempt $shutdownAttempts: candidate=$candidate, reconfigTarget=$reconfigTarget")
							env.controlPlane.onReconfigurationProposed(Set.empty)
							val shutdownHandle = env.submitElectorateChange(candidate, Set.empty[NodeId])
							ElectorateTracker.inFlightElectorateChanges(shutdownHandle.requestId) = new ElectorateTracker.InFlightElectorateChange(shutdownHandle, 0)
						}
					}

					ElectorateTracker.pollActiveElectorateChanges()
				}
			}

			/** Injects network partitions, bipartite channel severing, and alternating healed epochs. */
			object NetworkPartitions {
				/** True while an active network partition is in effect; false during healed epochs. */
				var isPartitionActive: Boolean = false
				/** Active fault mode determining whether severed channels drop or buffer packets. */
				var currentFaultMode: PartitionFaultMode = PartitionFaultMode.Drop
				/** Directed channel pairs severed by the current partition topology. */
				val severedChannels: mutable.Set[(NodeId, NodeId)] = mutable.Set.empty
				/** Elapsed steps within the current partition or healed epoch. */
				var partitionEpochSteps: Int = 0
				/** Target step threshold for transitioning between partitioned and healed states. */
				var targetEpochSteps: Int = healedEpochStepsTarget
				/** Leader wake-up retry count observed during the active partition epoch. */
				var leaderWakeUpsInPartition: Int = 0
				/** Flag indicating whether partitions are permanently disarmed during cluster shutdown. */
				var partitionsPermanentlyDisarmed: Boolean = false

				/** Restores full network connectivity and resets partition epoch timers. */
				def healPartition(): Unit = {
					isPartitionActive = false
					severedChannels.clear()
					leaderWakeUpsInPartition = 0
					partitionEpochSteps = 0
					targetEpochSteps = healedEpochStepsTarget
					scribe.trace(s"Partition healed. Next partition in $targetEpochSteps steps.")
				}

				/** Permanently disarms future network partitions and immediately heals any existing partition. */
				def disarmAndHeal(): Unit = {
					partitionsPermanentlyDisarmed = true
					if isPartitionActive then healPartition()
				}

				/** Partitions the cluster into two subsets with directed or bidirectional channel severance. */
				def startPartition(): Unit = {
					if !partitionsPermanentlyDisarmed && clusterSize >= 2 then {
						val mode = if random.nextFloat() < bufferPartitionProbability then PartitionFaultMode.Buffer else PartitionFaultMode.Drop
						val allNodeIds = (0 until clusterSize).map(i => NodeId(s"p-$i")).toIndexedSeq
						val mask = Array.fill(clusterSize)(random.nextBoolean())
						if !mask.contains(true) then mask(random.nextInt(clusterSize)) = true
						if !mask.contains(false) then mask(random.nextInt(clusterSize)) = false
						val (subsetA, subsetB) = allNodeIds.zip(mask).partition(_._2) match {
							case (a, b) => (a.map(_._1), b.map(_._1))
						}
						severedChannels.clear()
						val isAsymmetric = random.nextFloat() < asymmetricSplitProbability
						if isAsymmetric then {
							val aToB = random.nextBoolean()
							if aToB then {
								for a <- subsetA; b <- subsetB do severedChannels.add((a, b))
							} else {
								for a <- subsetA; b <- subsetB do severedChannels.add((b, a))
							}
						} else {
							for a <- subsetA; b <- subsetB do {
								severedChannels.add((a, b))
								severedChannels.add((b, a))
							}
						}
						isPartitionActive = true
						currentFaultMode = mode
						leaderWakeUpsInPartition = 0
						partitionEpochSteps = 0
						targetEpochSteps = maxPartitionSteps
						scribe.trace(s"Partition started: mode=$mode, asymmetric=$isAsymmetric, severedChannels=$severedChannels, maxSteps=$targetEpochSteps")
					}
				}

				/** Advances partition epoch tracking and transitions between partitioned and healed network connectivity. */
				def advanceEpoch(): Unit = {
					if !partitionsPermanentlyDisarmed then {
						partitionEpochSteps += 1
						if isPartitionActive then {
							if leaderWakeUpsInPartition >= maxPartitionWakeUps || partitionEpochSteps >= targetEpochSteps then {
								healPartition()
							}
						} else {
							if partitionEpochSteps >= targetEpochSteps then {
								startPartition()
							}
						}
					}
				}
			}

			/** Tracks and synchronizes scheduled node wake-up timers with the environment. */
			object WakeUpSynchronization {
				/** Map associating each node with its active wake-up tokens and remaining ticks. */
				val nodeTimers: mutable.Map[NodeId, mutable.Map[WakeUpTokenId, Int]] = mutable.Map.empty
				for n <- env.allNodes do nodeTimers(n.myId) = mutable.Map.empty

				/** Ingests pending wake-up registrations from the environment and purges cancelled tokens. */
				def synchronize(): Unit = {
					val currentWakeUps = env.clock.pendingWakeUps
					val activeTokenIds = currentWakeUps.map(_.tokenId).toSet
					for w <- currentWakeUps do {
						val timersForNode = nodeTimers.getOrElseUpdate(w.nodeId, mutable.Map.empty)
						if !timersForNode.contains(w.tokenId) then {
							val initialTicks = math.max(1, w.scheduledTime - env.clock.currentVirtualTime)
							timersForNode(w.tokenId) = initialTicks
						}
					}
					for (_, timers) <- nodeTimers do {
						timers.filterInPlace((id, _) => activeTokenIds.contains(id))
					}
				}
			}

			/** Fast-forwards virtual time when compute queues and deliverable network channels are drained. */
			object QuiescenceFastForward {
				/** Evaluates idle system state and advances virtual time directly to the earliest pending timer expiration. */
				def advanceIfIdle(): Unit = {
					val activeChannels = if NetworkPartitions.isPartitionActive && NetworkPartitions.currentFaultMode == PartitionFaultMode.Buffer then {
						env.network.activeChannelKeys.filterNot(NetworkPartitions.severedChannels.contains)
					} else {
						env.network.activeChannelKeys
					}
					if activeChannels.isEmpty && !env.allNodes.exists(_.stepDoer.hasPendingTasks) then {
						val allActiveTimerEntries = for {
							(nodeId, timers) <- WakeUpSynchronization.nodeTimers
							(tokenId, remaining) <- timers
						} yield (nodeId, tokenId, remaining)

						if allActiveTimerEntries.nonEmpty then {
							val minRemaining = allActiveTimerEntries.map(_._3).min
							val expired = env.clock.advanceTime(minRemaining)
							if NetworkPartitions.isPartitionActive then {
								NetworkPartitions.leaderWakeUpsInPartition += expired.count(w =>
									w.reason == WakeUpReason.UnreachableFollowersRetry ||
										w.reason == WakeUpReason.QuiescenceAuthorizationRetry ||
										w.reason == WakeUpReason.RetirementDriveRetry
								)
							}
							for (_, timers) <- WakeUpSynchronization.nodeTimers do {
								val matured = mutable.ArrayBuffer.empty[WakeUpTokenId]
								for (tokenId, remaining) <- timers do {
									val next = remaining - minRemaining
									if next <= 0 then matured.append(tokenId)
									else timers(tokenId) = next
								}
								for tokenId <- matured do {
									timers.remove(tokenId)
									env.clock.triggerWakeUp(tokenId)
								}
							}
						}
					}
				}
			}

			/** Dedicated adversarial channel picker driving temporally correlated drift and semantic packet weighting. */
			val adversarialPicker = new AdversarialPacketPicker(env, random)

			/** Arbitrates compute versus network execution and dispatches discrete event steps with adversarial traffic shaping. */
			object DiscreteStepDispatcher {
				/** Executes one discrete simulation step by arbitrating between node task execution and packet transmission. */
				def dispatchNext(): Unit = {
					adversarialPicker.advanceStep()

					val currentActiveChannels = if NetworkPartitions.isPartitionActive && NetworkPartitions.currentFaultMode == PartitionFaultMode.Buffer then {
						env.network.activeChannelKeys.filterNot(NetworkPartitions.severedChannels.contains)
					} else {
						env.network.activeChannelKeys
					}
					val currentActiveNodes = env.allNodes.filter(n => n.stepDoer.hasPendingTasks || WakeUpSynchronization.nodeTimers.get(n.myId).exists(_.nonEmpty))
					val hasNetwork = currentActiveChannels.nonEmpty
					val hasCompute = currentActiveNodes.nonEmpty

					if hasNetwork || hasCompute then {
						val pickCompute = if hasNetwork && hasCompute then {
							random.nextFloat() < computeDomainProbability
						} else hasCompute

						if pickCompute then {
							val chosenNode = currentActiveNodes(random.nextInt(currentActiveNodes.size))
							if chosenNode.stepDoer.hasPendingTasks then {
								env.stepNode(chosenNode.myId)
							}
							WakeUpSynchronization.nodeTimers.get(chosenNode.myId).foreach { timers =>
								val matured = mutable.ArrayBuffer.empty[WakeUpTokenId]
								for (tokenId, remaining) <- timers do {
									val next = remaining - ticksPerNodeCycle
									if next <= 0 then matured.append(tokenId)
									else timers(tokenId) = next
								}
								for tokenId <- matured do {
									timers.remove(tokenId)
									if NetworkPartitions.isPartitionActive then {
										if env.clock.pendingWakeUps.find(_.tokenId == tokenId).exists(w =>
											w.reason == WakeUpReason.UnreachableFollowersRetry ||
												w.reason == WakeUpReason.QuiescenceAuthorizationRetry ||
												w.reason == WakeUpReason.RetirementDriveRetry
										) then {
											NetworkPartitions.leaderWakeUpsInPartition += 1
										}
									}
									env.clock.triggerWakeUp(tokenId)
								}
							}
						} else {
							val (from, to) = adversarialPicker.selectChannel(currentActiveChannels)
							if NetworkPartitions.isPartitionActive && NetworkPartitions.currentFaultMode == PartitionFaultMode.Drop && NetworkPartitions.severedChannels.contains((from, to)) then {
								env.network.dropNext(from, to)
							} else {
								env.network.channelHeadPacket(from, to).foreach { headPacket =>
									val isRequest = headPacket.isInstanceOf[RequestPacket]
									val failureRate = if isRequest then requestFailurePercentage else responseFailurePercentage
									if random.nextInt(PercentageBasis) < failureRate then {
										env.network.dropNext(from, to)
									} else {
										env.network.deliverNext(from, to)
									}
								}
							}
						}
					}
				}
			}

			while (!env.oracle.areAllNodesQuiesced || client.currentCommandSerial <= numberOfCommandsToSend) && totalSteps < maxTotalSteps do {
				totalSteps += 1

				client.driveCommandLifecycle(numberOfCommandsToSend)
				if NetworkPartitions.isPartitionActive && client.commandRetries > maxRetries / 2 then {
					NetworkPartitions.healPartition()
				}

				if client.currentCommandSerial <= numberOfCommandsToSend then {
					ElectorateNoise.inject()
				} else {
					GracefulShutdown.drive()
				}

				NetworkPartitions.advanceEpoch()

				WakeUpSynchronization.synchronize()
				QuiescenceFastForward.advanceIfIdle()
				DiscreteStepDispatcher.dispatchNext()
			}

			if totalSteps >= maxTotalSteps then {
				val nonQuiesced = env.oracle.formatNonQuiescedNodesDiagnostic
				throw new AssertionError(s"Simulation reached step limit of $maxTotalSteps. Commands sent: ${client.totalCommandsSucceeded}/$numberOfCommandsToSend, allNodesQuiesced=${env.oracle.areAllNodesQuiesced}, nonQuiesced=[$nonQuiesced]")
			}
			assert(env.oracle.areAllNodesQuiesced, s"Not all nodes reached QUIESCED after simulation (${GracefulShutdown.shutdownAttempts} shutdown attempts)")

			// Final invariant verification
			env.oracle.checkLogMatching()
			scribe.trace(s"$testCompletionHeader passed successfully (noise requests: ${ElectorateNoise.noiseElectorateChangesCount})")
			ElectorateNoise.noiseElectorateChangesCount
		} catch {
			case scala.util.control.NonFatal(e) =>
				scribe.trace(s"$testCompletionHeader failed with:", e)
				throw e
		} finally {
			scribe.Logger.root.withMinimumLevel(Level.Trace).replace()
		}
	}

	test("assertions are enabled") {
		if !ConsensusParticipantSdm.assertionsEnabled then println("Enable assertions for all tests to detect more bugs by adding the -ea VM option.")
		assert(ConsensusParticipantSdm.assertionsEnabled, "Assertions are not enabled. Enable VM option -ea.")
	}

	test("deterministic simulation baseline") {
		val scribeLevel = Level.Info
		val initialMask = IArray(true, true, true)
		runSimulation(
			clusterSize = 3,
			initialConfigMask = initialMask,
			randomnessSeed = 42L,
			startWithHighestPriorityParticipant = true,
			numberOfCommandsToSend = 10,
			requestFailurePercentage = 0,
			responseFailurePercentage = 0,
			electorateChangeProbability = 0.0f,
			maxRetries = 25,
			remembersLastAppliedCommandIndex = false,
			logCompactionThreshold = 5,
			maxInFlightAppendsPerPeer = 1,
			logRetentionAfterSnapshot = 0,
			computeDomainProbability = 0.5f,
			ticksPerNodeCycle = 10,
			asymmetricSplitProbability = 0.2f,
			bufferPartitionProbability = 0.5f,
			maxPartitionWakeUps = 3,
			partitionBaseStepsPerChannel = 15,
			healedBaseStepsPerChannel = 20,
			emaSmoothingDecay = 0.8,
			retiringParticipantMaxRetries = 25,
			scribeLevel = scribeLevel
		)
	}

	test("deterministic simulation with network failures") {
		val scribeLevel = Level.Info
		val initialMask = IArray(true, true, true)
		runSimulation(
			clusterSize = 3,
			initialConfigMask = initialMask,
			randomnessSeed = 12345L,
			startWithHighestPriorityParticipant = false,
			numberOfCommandsToSend = 10,
			requestFailurePercentage = 10,
			responseFailurePercentage = 10,
			electorateChangeProbability = 0.0f,
			maxRetries = 25,
			remembersLastAppliedCommandIndex = false,
			logCompactionThreshold = 5,
			maxInFlightAppendsPerPeer = 1,
			logRetentionAfterSnapshot = 0,
			computeDomainProbability = 0.5f,
			ticksPerNodeCycle = 10,
			asymmetricSplitProbability = 0.2f,
			bufferPartitionProbability = 0.5f,
			maxPartitionWakeUps = 3,
			partitionBaseStepsPerChannel = 15,
			healedBaseStepsPerChannel = 20,
			emaSmoothingDecay = 0.8,
			retiringParticipantMaxRetries = 25,
			scribeLevel = scribeLevel
		)
	}

	property("All invariants must comply - without electorate changes noise") {
		val scribeLevel = Level.Info
		Prop.forAllNoShrink(
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
				logRetentionAfterSnapshot = nodeConfig.logRetentionAfterSnapshot,
				computeDomainProbability = 0.5f,
				ticksPerNodeCycle = 10,
				asymmetricSplitProbability = 0.2f,
				bufferPartitionProbability = 0.5f,
				maxPartitionWakeUps = 3,
				partitionBaseStepsPerChannel = 15,
				healedBaseStepsPerChannel = 20,
				emaSmoothingDecay = 0.8,
				retiringParticipantMaxRetries = 25,
				scribeLevel = scribeLevel
			)
			true
		}
	}

	property("All invariants must comply - with electorate changes noise") {
		val scribeLevel = Level.Info
		var pbtSamplesCount = 0
		var pbtTotalNoiseRequests = 0
		Prop.forAllNoShrink(
			Gen.choose(3, 9),
			Gen.oneOf(true, false),
			Gen.long,
			genNodeConfig
		) { (clusterSize, startWithHighestPriorityParticipant, netRandomnessSeed, nodeConfig) =>
			val initialMask = IArray.fill(clusterSize)(true)
			val noiseCount = runSimulation(
				clusterSize = clusterSize,
				initialConfigMask = initialMask,
				randomnessSeed = netRandomnessSeed,
				startWithHighestPriorityParticipant = startWithHighestPriorityParticipant,
				numberOfCommandsToSend = 30,
				requestFailurePercentage = 10,
				responseFailurePercentage = 10,
				electorateChangeProbability = 0.1f,
				maxRetries = 25,
				remembersLastAppliedCommandIndex = nodeConfig.remembersLastAppliedCommandIndex,
				logCompactionThreshold = nodeConfig.logCompactionThreshold,
				maxInFlightAppendsPerPeer = nodeConfig.maxInFlightAppendsPerPeer,
				logRetentionAfterSnapshot = nodeConfig.logRetentionAfterSnapshot,
				computeDomainProbability = 0.5f,
				ticksPerNodeCycle = 10,
				asymmetricSplitProbability = 0.2f,
				bufferPartitionProbability = 0.5f,
				maxPartitionWakeUps = 3,
				partitionBaseStepsPerChannel = 15,
				healedBaseStepsPerChannel = 20,
				emaSmoothingDecay = 0.8,
				retiringParticipantMaxRetries = 25,
				scribeLevel = scribeLevel
			)
			pbtSamplesCount += 1
			pbtTotalNoiseRequests += noiseCount
			if pbtSamplesCount % 20 == 0 || pbtSamplesCount == 100 then {
				scribe.info(s"PBT noise frequency verification: samples=$pbtSamplesCount/100, aggregateNoiseRequests=$pbtTotalNoiseRequests, expectedMean=${pbtSamplesCount * 10 * 0.03}")
			}
			true
		}
	}

	/** Regression table for seeds that previously exposed failures during discrete-event simulation.
	 * Initialized as empty per design; entries are added upon discovering reproducible failing cases.
	 */
	test("Previous failing cases") {
		val scribeLevel = Level.Info
		type FailingCase = (
			clusterSize: Int,
			initialConfigMask: IArray[Boolean],
			randomnessSeed: Long,
			startWithHighestPriorityParticipant: Boolean,
			numberOfCommandsToSend: Int,
			requestFailurePercentage: Int,
			responseFailurePercentage: Int,
			electorateChangeProbability: Float,
			maxRetries: Int,
			remembersLastAppliedCommandIndex: Boolean,
			logCompactionThreshold: Int,
			maxInFlightAppendsPerPeer: Int,
			logRetentionAfterSnapshot: Int,
			computeDomainProbability: Float,
			ticksPerNodeCycle: Int,
			asymmetricSplitProbability: Float,
			bufferPartitionProbability: Float,
			maxPartitionWakeUps: Int,
			partitionBaseStepsPerChannel: Int,
			healedBaseStepsPerChannel: Int,
			emaSmoothingDecay: Double,
			retiringParticipantMaxRetries: Int
		)

		val failingCases = Seq[FailingCase](
			(6, IArray(true, true, true, true, true, true), 6862003055765340310L, false, 30, 10, 10, 0.1f, 25, true, 5, 9, 0, 0.5f, 10, 0.2f, 0.5f, 3, 15, 20, 0.8, 25),
			(7, IArray(true, true, true, true, true, true, true), -2151228842506317038L, false, 10, 10, 10, 0.1f, 25, false, 3, 9, 1, 0.5f, 10, 0.2f, 0.5f, 3, 15, 20, 0.8, 25),
			(9, IArray(true, true, true, true, true, true, true, true, true), 8770737199023491237L, false, 30, 10, 10, 0.1f, 25, false, 3, 2, 0, 0.5f, 10, 0.2f, 0.5f, 3, 15, 20, 0.8, 25),
			(6, IArray(true, true, true, true, true, true), -4081278148514554797L, true, 30, 10, 10, 0.1f, 25, true, 5, 9, 0, 0.5f, 10, 0.2f, 0.5f, 3, 15, 20, 0.8, 25),
			(3, IArray(true, true, true), -7186606340068033813L, true, 30, 10, 10, 0.1f, 25, false, 5, 2, 0, 0.5f, 10, 0.2f, 0.5f, 3, 15, 20, 0.8, 25),
			(7, IArray(true, true, true, true, true, true, true), -9068938783188895233L, true, 30, 10, 10, 0.1f, 25, false, 3, 9, 0, 0.5f, 10, 0.2f, 0.5f, 3, 15, 20, 0.8, 25),
			(3, IArray(true, true, true), 6275675194096261271L, false, 30, 10, 10, 0.1f, 25, false, 3, 9, 1, 0.5f, 10, 0.2f, 0.5f, 3, 15, 20, 0.8, 25),
			(9, IArray(true, true, true, true, true, true, true, true, true), 3392782448973611414L, false, 30, 10, 10, 0.1f, 25, true, 5, 9, 0, 0.5f, 10, 0.2f, 0.5f, 3, 15, 20, 0.8, 25),
			(9, IArray(true, true, true, true, true, true, true, true, true), 348902260173106136L, true, 30, 10, 10, 0.1f, 25, false, 3, 9, 0, 0.5f, 10, 0.2f, 0.5f, 3, 15, 20, 0.8, 25),
			(9, IArray(true, true, true, true, true, true, true, true, true), 7013924277443498472L, true, 30, 10, 10, 0.1f, 25, false, 3, 2, 0, 0.5f, 10, 0.2f, 0.5f, 3, 15, 20, 0.8, 25),
			(4, IArray(true, true, true, true), -3518346649973445104L, false, 30, 10, 10, 0.1f, 25, false, 5, 2, 0, 0.5f, 10, 0.2f, 0.5f, 3, 15, 20, 0.8, 25),
			(4, IArray(true, true, true, true), 7264845992207177295L, false, 30, 10, 10, 0.1f, 25, false, 3, 1, 3, 0.5f, 10, 0.2f, 0.5f, 3, 15, 20, 0.8, 25),
			(9, IArray(true, true, true, true, true, true, true, true, true), 3161691290716389568L, true, 30, 10, 10, 0.1f, 25, true, 5, 9, 1, 0.5f, 10, 0.2f, 0.5f, 3, 15, 20, 0.8, 25),
			(6, IArray(true, true, true, true, true, true), -2260191410438714386L, false, 30, 10, 10, 0.1f, 25, true, 5, 2, 0, 0.5f, 10, 0.2f, 0.5f, 3, 15, 20, 0.8, 25),
			(7, IArray(true, true, true, true, true, true, true), -3807853259925956503L, true, 30, 10, 10, 0.1f, 25, true, 3, 2, 1, 0.5f, 10, 0.2f, 0.5f, 3, 15, 20, 0.8, 25),
			(6, IArray(true, true, true, true, true, true), -6011867305159537457L, true, 30, 10, 10, 0.1f, 25, true, 3, 1, 1, 0.5f, 10, 0.2f, 0.5f, 3, 15, 20, 0.8, 25),
			(8, IArray(true, true, true, true, true, true, true, true), -4400713448448958795L, true, 30, 10, 10, 0.1f, 25, true, 5, 9, 0, 0.5f, 10, 0.2f, 0.5f, 3, 15, 20, 0.8, 25),
			(9, IArray(true, true, true, true, true, true, true, true, true), -4869190977466226919L, false, 30, 10, 10, 0.1f, 25, true, 3, 1, 0, 0.5f, 10, 0.2f, 0.5f, 3, 15, 20, 0.8, 25),
			(4, IArray(true, true, true, true), -6244441253779539979L, true, 30, 10, 10, 0.1f, 25, true, 5, 1, 3, 0.5f, 10, 0.2f, 0.5f, 3, 15, 20, 0.8, 25),
			(6, IArray(true, true, true, true, true, true), 5851711118296182511L, true, 30, 10, 10, 0.1f, 25, true, 5, 1, 3, 0.5f, 10, 0.2f, 0.5f, 3, 15, 20, 0.8, 25),
			(5, IArray(true, true, true, true, true), -697813527002376471L, true, 30, 10, 10, 0.1f, 25, false, 5, 1, 3, 0.5f, 10, 0.2f, 0.5f, 3, 15, 20, 0.8, 25),
			(6, IArray(true, true, true, true, true, true), 7686206174213037724L, false, 30, 10, 10, 0.1f, 25, true, 3, 1, 3, 0.5f, 10, 0.2f, 0.5f, 3, 15, 20, 0.8, 25),
			(8, IArray(true, true, true, true, true, true, true, true), 3804016885324562908L, false, 30, 10, 10, 0.1f, 25, false, 5, 2, 0, 0.5f, 10, 0.2f, 0.5f, 3, 15, 20, 0.8, 25),
			(5, IArray(true, true, true, true, true), 4548242388870746891L, false, 30, 10, 10, 0.1f, 25, true, 5, 9, 0, 0.5f, 10, 0.2f, 0.5f, 3, 15, 20, 0.8, 25),
			(9, IArray.fill(9)(true), 7380181359191953245L, true, 30, 10, 10, 0.1f, 25, false, 3, 1, 1, 0.5f, 10, 0.2f, 0.5f, 3, 15, 20, 0.8, 25),
			(8, IArray(true, true, true, true, true, true, true, true), -8028806361595823517L, false, 30, 10, 10, 0.1f, 25, false, 5, 2, 3, 0.5f, 10, 0.2f, 0.5f, 3, 15, 20, 0.8, 25),
			(4, IArray(true, true, true, true), 2622779781087689361L, false, 30, 10, 10, 0.1f, 25, true, 5, 2, 0, 0.5f, 10, 0.2f, 0.5f, 3, 15, 20, 0.8, 25),
			(5, IArray(true, true, true, true, true), -806900698207097022L, false, 10, 10, 10, 0.1f, 25, false, 5, 1, 1, 0.5f, 10, 0.2f, 0.5f, 3, 15, 20, 0.8, 25),
			(9, IArray.fill(9)(true), 6167047151743735597L, true, 10, 10, 10, 0.1f, 25, false, 3, 9, 0, 0.5f, 10, 0.2f, 0.5f, 3, 15, 20, 0.8, 25),
			(3, IArray.fill(3)(true), -3029948934805879799L, true, 10, 10, 10, 0.1f, 25, true, 5, 2, 0, 0.5f, 10, 0.2f, 0.5f, 3, 15, 20, 0.8, 25),
			(6, IArray.fill(6)(true), 2681691328823084768L, true, 10, 10, 10, 0.1f, 25, false, 3, 9, 1, 0.5f, 10, 0.2f, 0.5f, 3, 15, 20, 0.8, 25),
			(3, IArray.fill(3)(true), 3396056744307990485L, false, 10, 10, 10, 0.1f, 25, true, 5, 2, 3, 0.5f, 10, 0.2f, 0.5f, 3, 15, 20, 0.8, 25),
			(4, IArray.fill(4)(true), 5739198640270613660L, true, 10, 10, 10, 0.1f, 25, true, 3, 9, 3, 0.5f, 10, 0.2f, 0.5f, 3, 15, 20, 0.8, 25),
			(4, IArray.fill(4)(true), 7L, false, 10, 10, 10, 0.03f, 25, false, 3, 2, 0, 0.5f, 10, 0.2f, 0.5f, 3, 15, 20, 0.8, 25),
			(4, IArray.fill(4)(true), 8370446902253282128L, false, 10, 10, 10, 0.03f, 25, false, 3, 2, 0, 0.5f, 10, 0.2f, 0.5f, 3, 15, 20, 0.8, 25),
			(5, IArray.fill(5)(true), -2138682370221098961L, true, 10, 10, 10, 0.03f, 25, true, 5, 2, 1, 0.5f, 10, 0.2f, 0.5f, 3, 15, 20, 0.8, 25),
			(4, IArray.fill(4)(true), -7866577207221667707L, true, 10, 10, 10, 0.03f, 25, true, 3, 1, 1, 0.5f, 10, 0.2f, 0.5f, 3, 15, 20, 0.8, 25),
			(5, IArray.fill(5)(true), -1433210546132280752L, false, 10, 10, 10, 0.0f, 25, false, 3, 1, 0, 0.5f, 10, 0.2f, 0.5f, 3, 15, 20, 0.8, 25)
		)

		for (caseData <- failingCases) {
			val (
				clusterSize,
				initialConfigMask,
				randomnessSeed,
				startWithHighestPriorityParticipant,
				numberOfCommandsToSend,
				requestFailurePercentage,
				responseFailurePercentage,
				electorateChangeProbability,
				maxRetries,
				remembersLastAppliedCommandIndex,
				logCompactionThreshold,
				maxInFlightAppendsPerPeer,
				logRetentionAfterSnapshot,
				computeDomainProbability,
				ticksPerNodeCycle,
				asymmetricSplitProbability,
				bufferPartitionProbability,
				maxPartitionWakeUps,
				partitionBaseStepsPerChannel,
				healedBaseStepsPerChannel,
				emaSmoothingDecay,
				retiringParticipantMaxRetries
			) = caseData
			runSimulation(
				clusterSize = clusterSize,
				initialConfigMask = initialConfigMask,
				randomnessSeed = randomnessSeed,
				startWithHighestPriorityParticipant = startWithHighestPriorityParticipant,
				numberOfCommandsToSend = numberOfCommandsToSend,
				requestFailurePercentage = requestFailurePercentage,
				responseFailurePercentage = responseFailurePercentage,
				electorateChangeProbability = electorateChangeProbability,
				maxRetries = maxRetries,
				remembersLastAppliedCommandIndex = remembersLastAppliedCommandIndex,
				logCompactionThreshold = logCompactionThreshold,
				maxInFlightAppendsPerPeer = maxInFlightAppendsPerPeer,
				logRetentionAfterSnapshot = logRetentionAfterSnapshot,
				computeDomainProbability = computeDomainProbability,
				ticksPerNodeCycle = ticksPerNodeCycle,
				asymmetricSplitProbability = asymmetricSplitProbability,
				bufferPartitionProbability = bufferPartitionProbability,
				maxPartitionWakeUps = maxPartitionWakeUps,
				partitionBaseStepsPerChannel = partitionBaseStepsPerChannel,
				healedBaseStepsPerChannel = healedBaseStepsPerChannel,
				emaSmoothingDecay = emaSmoothingDecay,
				retiringParticipantMaxRetries = retiringParticipantMaxRetries,
				scribeLevel = scribeLevel
			)
		}
	}

	test("All invariants special case") {
		val scribeLevel = Level.Trace
		val (
			clusterSize,
			initialConfigMask,
			randomnessSeed,
			startWithHighestPriorityParticipant,
			numberOfCommandsToSend,
			requestFailurePercentage,
			responseFailurePercentage,
			electorateChangeProbability,
			maxRetries,
			remembersLastAppliedCommandIndex,
			logCompactionThreshold,
			maxInFlightAppendsPerPeer,
			logRetentionAfterSnapshot,
			computeDomainProbability,
			ticksPerNodeCycle,
			asymmetricSplitProbability,
			bufferPartitionProbability,
			maxPartitionWakeUps,
			partitionBaseStepsPerChannel,
			healedBaseStepsPerChannel,
			emaSmoothingDecay,
			retiringParticipantMaxRetries
		) = (5, IArray.fill(5)(true), -1433210546132280752L, false, 10, 10, 10, 0.0f, 25, false, 3, 1, 0, 0.5f, 10, 0.2f, 0.5f, 3, 15, 20, 0.8, 25)
		runSimulation(
			clusterSize = clusterSize,
			initialConfigMask = initialConfigMask,
			randomnessSeed = randomnessSeed,
			startWithHighestPriorityParticipant = startWithHighestPriorityParticipant,
			numberOfCommandsToSend = numberOfCommandsToSend,
			requestFailurePercentage = requestFailurePercentage,
			responseFailurePercentage = responseFailurePercentage,
			electorateChangeProbability = electorateChangeProbability,
			maxRetries = maxRetries,
			remembersLastAppliedCommandIndex = remembersLastAppliedCommandIndex,
			logCompactionThreshold = logCompactionThreshold,
			maxInFlightAppendsPerPeer = maxInFlightAppendsPerPeer,
			logRetentionAfterSnapshot = logRetentionAfterSnapshot,
			computeDomainProbability = computeDomainProbability,
			ticksPerNodeCycle = ticksPerNodeCycle,
			asymmetricSplitProbability = asymmetricSplitProbability,
			bufferPartitionProbability = bufferPartitionProbability,
			maxPartitionWakeUps = maxPartitionWakeUps,
			partitionBaseStepsPerChannel = partitionBaseStepsPerChannel,
			healedBaseStepsPerChannel = healedBaseStepsPerChannel,
			emaSmoothingDecay = emaSmoothingDecay,
			retiringParticipantMaxRetries = retiringParticipantMaxRetries,
			scribeLevel = scribeLevel
		)
	}
}
