package readren.consensus

import ConsensusParticipantSdm.*

import munit.ScalaCheckEffectSuite
import org.scalacheck.Gen
import org.scalacheck.Test.Parameters
import org.scalacheck.effect.PropF
import readren.common.{Maybe, ScribeConfig}
import readren.sequencer.Doer
import readren.sequencer.providers.CooperativeWorkersWithAsyncSchedulerDp
import scribe.{LogRecord, Priority}
import scribe.modify.LogModifier
import scribe.throwable.TraceLoggableMessage

import java.util.concurrent.{Executors, TimeUnit}
import scala.collection.immutable.ListSet
import scala.collection.{mutable, IndexedSeq as GenIndexedSeq}
import scala.compiletime.uninitialized
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.reflect.ClassTag
import scala.util.{Failure, Random, Success, Try}

class ConsensusParticipantSdmTest extends ScalaCheckEffectSuite {

	ScribeConfig.init(useSimpleFormatter = true, modifiers = List(new LogModifier {
		override def id: String = "simulated-failures-filter"

		override def priority: Priority = Priority.Normal

		override def apply(record: LogRecord): Option[LogRecord] = {
			val y = record.messages.filterNot {
				case TraceLoggableMessage(throwable) if throwable.getMessage != null && (throwable.getMessage.startsWith("Net: simulated failure") || throwable.getMessage.startsWith("Net: target node is down")) => true
				case _ => false
			}
			Some(record.copy(messages = y))
		}


		override def withId(id: String): LogModifier = this
	}))

	override def scalaCheckTestParameters: Parameters = super.scalaCheckTestParameters.withMinSuccessfulTests(500)

	//	override def scalaCheckInitialSeed = "mLbrswnMGqQ8czetIVPfw3Wh8rh8m-nkAjknVe4oiUE="

	override def munitTimeout: Duration = new FiniteDuration(6000, TimeUnit.SECONDS)

	private given ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(4))

	/** Id of a [[Node]]. Implements [[ConsensusParticipantSdm.ParticipantId]]. */
	private type Id = String

	// Test command type
	private case class TestClientCommand(value: Int, clientId: String)

	/** The provider of all the [[Doer]] instances used by this testing infrastructure. */
	private val sharedDap = new CooperativeWorkersWithAsyncSchedulerDp.Impl(
		failureReporter = (doer, e) =>
			scribe.error(s"Failure reported by a task executed by the sequencer tagged with ${doer.tag}", e),
		unhandledExceptionReporter = (doer, e) =>
			scribe.error(s"Unhandled exception in a task executed by the sequencer tagged with ${doer.tag}", e)
	)

	private type ScheduSequen = CooperativeWorkersWithAsyncSchedulerDp.SchedulingDoerFacade

	/** Simulates a net of nodes for testing.
	 * @param clusterSize the total number of [[Node]] instances involved.
	 * @param randomnessSeed the seed for the pseud-randomness of the messages fate.
	 * @param requestFailurePercentage Probability (as a percentage) that a request message fails to reach its target [[Node]].
	 * @param responseFailurePercentage Probability (as a percentage) that a response message—sent in reply to a successfully delivered request—fails to reach the originating [[Node]].
	 * @param stimulusSettlingTime duration (in milliseconds) allotted for the system to settle after a stimulus (i.e., message delivery), before releasing the next traveling message.
	 *                             This pause ensures that all [[Node]]s complete their internal processing and inter-node communication, preserving test determinism.
	 *                             Note: Test execution time scales with this value, so avoid setting it excessively high.
	 * */
	private class Net(
		val clusterSize: Int,
		val randomnessSeed: Long = 0,
		requestFailurePercentage: Int = 10,
		responseFailurePercentage: Int = 10,
		configChangeBeforeRequestDelivered_probability: Float = 0.05,
		configChangeBeforeResponseDelivered_probability: Float = 0.05,
		configChangeAfterResponseDelivered_probability: Float = 0.05,
		stimulusSettlingTime: Int = 5
	) {
		val nodesIds: IndexedSeq[Id] = (0 until clusterSize).map(i => s"p-$i")

		/** Threshold for the number of traveling messages enqueued before one is delivered, even if the [[stimulusSettlingTime]] has not yet elapsed.
		 * When this threshold is reached, a single message is selected pseudo-randomly and delivered, allowing the system to progress without waiting for full settling.
		 * This accelerates test execution while reducing delivery reordering.
		 */
		private val enqueueThresholdForEarlyDelivery = clusterSize * 2

		/** Duration (expressed as the number of requests initiated by [[Node]]s) for which communication between two nodes remains in a failure state once it begins.
		 * This value represents the square root of the intended failure duration, due to the underlying probability distribution:
		 * the actual duration is sampled as the square of a uniform random variable.
		 */
		private def failureMaxDurationSqrt = Math.max(1, Math.min(activeConfigChange.oldParticipants.size, activeConfigChange.newParticipants.size))

		/** All the mutable variables used by this [[Net]] instance are accessed within this [[ScheduSequen]]. */
		val netSequencer: ScheduSequen = sharedDap.provide("net-sequencer")

		/** The indices of the [[Node]]s in this [[Net]], indexed by node identifier. */
		private var indexById: Map[Id, Int] = Map.empty

		/** The [[Node]]s in this [[Net]], indexed by node index. */
		private val nodeByIndex: Array[Node | Null] = new Array(clusterSize)

		def addNode(node: Node): Unit = synchronized {
			assert(!indexById.contains(node.myId))
			val index = indexById.size
			indexById += node.myId -> index
			nodeByIndex(index) = node
		}

		def getNode(index: Int): Node = synchronized {
			nodeByIndex(index)
		}

		def getNode(id: String): Node = synchronized {
			nodeByIndex(indexById(id))
		}

		def indexOf(id: String): Int = synchronized {
			indexById(id)
		}

		def startsAllNodes: netSequencer.Duty[Array[Unit]] = {
			val starters = for nodeIndex <- 0 until clusterSize yield {
				val node = getNode(nodeIndex)
				node.startsIfNotRunning(true).onBehalfOf(netSequencer)
			}
			netSequencer.Duty_sequenceToArray(starters)
		}

		/** Stops all the running [[Node]]s and clears all [[Channel]]s used to simulate the TCP communication between them. */
		def stop(): Unit = {
			val nodesStoppers = for i <- 0 until clusterSize yield {
				val node = getNode(i)
				val stopsNode = node.sequencer.Duty_mineFlat(() => if node.isDown then node.sequencer.Duty_unit else node.participant.stops)
				netSequencer.Duty_foreign(node.sequencer)(stopsNode)
			}
			netSequencer.Duty_sequenceToArray(nodesStoppers)
				.andThen { _ =>
					numberOfTravelingMessages = 0
					for i <- 0 until clusterSize do {
						for j <- 0 until clusterSize do channelBySenderByReceiver(i)(j).clear()
					}
				}
				.triggerAndForget()
		}

		//// The following members implement the communication between nodes of this net.

		private type RequestId = (global: Int, channel: Int)

		/** A communication channel between two [[Node]]s.
		 * Mimics a TCP channel by maintaining delivery order. */
		private case class Channel() {
			private val queue: mutable.Queue[netSequencer.Duty[Unit]] = mutable.Queue.empty
			private var lastRequestId = 0
			private var failingUntil: Int = 0

			inline def nextRequestId: RequestId = {
				assert(netSequencer.isInSequence)
				lastRequestId += 1
				lastGlobalRequestId += 1
				(lastGlobalRequestId, lastRequestId)
			}

			inline def enqueue(duty: netSequencer.Duty[Unit]): Unit = {
				assert(netSequencer.isInSequence)
				queue.enqueue(duty)
				numberOfTravelingMessages += 1
				dispatchAMessageIfLongSilence()
			}

			inline def nonEmpty: Boolean = {
				assert(netSequencer.isInSequence)
				queue.nonEmpty
			}

			inline def dispatchNext(): Unit = {
				assert(netSequencer.isInSequence)
				numberOfTravelingMessages -= 1
				queue.dequeue().triggerAndForget(true)
			}

			inline def markAsFailing(durationSqrt: Int): Unit = {
				assert(netSequencer.isInSequence)
				failingUntil = lastGlobalRequestId + durationSqrt * durationSqrt
			}

			inline def isFailing: Boolean = {
				assert(netSequencer.isInSequence)
				lastGlobalRequestId <= failingUntil
			}

			inline def clear(): Unit = queue.clear()
		}

		private val random = new Random(randomnessSeed)
		private val channelBySenderByReceiver: Array[Array[Channel]] = Array.fill(clusterSize, clusterSize)(Channel())
		private var numberOfTravelingMessages: Int = 0
		private var lastGlobalRequestId: Int = 0
		private var lastProcessTimeoutSchedule: Maybe[netSequencer.Schedule] = Maybe.empty

		def dispatchAMessageIfLongSilence(): Unit = {
			assert(netSequencer.isInSequence)

			lastProcessTimeoutSchedule.foreach(netSequencer.cancel)
			val processTimeoutSchedule = netSequencer.newDelaySchedule(stimulusSettlingTime)
			lastProcessTimeoutSchedule = Maybe(processTimeoutSchedule)
			netSequencer.schedule(processTimeoutSchedule) { _ =>
				if numberOfTravelingMessages > 0 then {
					// scribe.trace(s"A long silence occurred with $numberOfTravelingMessages messages on the way. Dispatching one of them.")
					chooseAChannel().dispatchNext()
					dispatchAMessageIfLongSilence()
				}
			}
		}

		extension (inquirerId: Id) {
			/** Performs a Remote Procedure Call from a [[Node]] of this [[Net]] (the inquirer) to another [[Node]] of this [[Net]] (the replier).
			 * Assumes that the set of [[Node]]s remains invariant since the first invocation.
			 * To simulate a real network, the order in which messages of different [[Channel]]s are delivered is modified randomly.
			 * Messages sent from a [[Node]] to another maintain delivery order to mimic TCP characteristics.
			 * The randomness is deterministic to allow reproducing a scenario.
			 * The fate of all the stages of an RPC are determined in advance in the first stage.
			 * @param replierId the identifier of the targeted [[Node]], the one on whose [[Node.sequencer]] is the `call` function is executed.
			 * @param call a function that takes the replier [[Node]] and returns a `replierNode.sequencer.Task` that yields the value to be yielded by the returned [[readren.sequencer.Doer.Task]]. The function is called within the replier's [[Node.sequencer]].
			 * @return a [[netSequencer.Task]] that yields the value yielded by the `replierNode.sequencer.Task` returned by applying the provided function `call` to the replier [[Node]].
			 * @throws RuntimeException if this [[Net]] does not contain the [[Node]]s identified with `inquirerId` and `replierId`. */
			def rpc[R](replierId: Id, requestDescription: String)(call: (replierNode: Node) => replierNode.sequencer.Duty[R]): netSequencer.Task[R] = {

				if true then {
					val inquirerIndex = indexOf(inquirerId)
					val replierIndex = indexOf(replierId)
					val inquirerNode = getNode(inquirerIndex)
					assert(inquirerNode.sequencer.isInSequence)
					val inquirerRole = RoleOrdinal_nameOf(inquirerNode.participant.getRoleOrdinal)
					val covenant = netSequencer.Covenant[(Try[R], RequestId)]()
					netSequencer.execute {
						lastProcessTimeoutSchedule.foreach(netSequencer.cancel)

						val requestChannel = channelBySenderByReceiver(inquirerIndex)(replierIndex)
						val responseChannel = channelBySenderByReceiver(replierIndex)(inquirerIndex)
						val requestId = requestChannel.nextRequestId
						val requestChannelIsFailing = requestChannel.isFailing
						val responseChannelIsFailing = responseChannel.isFailing

						scribe.trace(s"$inquirerId >- $replierId: $requestId:$requestDescription, sent as $inquirerRole, $numberOfTravelingMessages messages on the way")

						// Determine the fate of this RPC before processing it.
						val requestIsCursed = requestChannelIsFailing || random.nextInt(100) < requestFailurePercentage
						val responseIsCursed = responseChannelIsFailing || random.nextInt(100) < responseFailurePercentage
						if !requestChannelIsFailing && requestIsCursed then {
							val failureDurationSqrt = random.nextInt(failureMaxDurationSqrt)
							requestChannel.markAsFailing(failureDurationSqrt)
						}
						if !responseChannelIsFailing && responseIsCursed then {
							val failureDurationSqrt = random.nextInt(failureMaxDurationSqrt)
							responseChannel.markAsFailing(failureDurationSqrt)
						}

						// Determine the fate of configuration changes during the different phases of this RPC.
						injectConfigurationNoise(configChangeBeforeRequestDelivered_probability)

						// Create a lazy duty that perform the RPC
						val replierNode = getNode(replierId)
						val requestingDuty =
							if requestIsCursed then {
								netSequencer.Duty_mine[Unit] { () =>
									injectConfigurationNoise(configChangeBeforeResponseDelivered_probability)
									covenant.fulfill((Failure(new RuntimeException(s"Net: simulated failure of request $requestId")), requestId), true)
									injectConfigurationNoise(configChangeAfterResponseDelivered_probability)
								}
							} else {
								for {
									_ <- netSequencer.Duty_mine(() => scribe.trace(s"$inquirerId -> $replierId: $requestId:$requestDescription, $numberOfTravelingMessages messages are traveling."))
									replyAndRole <- netSequencer.Duty_foreign(replierNode.sequencer) {
										replierNode.sequencer.Duty_mineFlat { () =>
											if replierNode.isDown then replierNode.sequencer.Duty_ready(null)
											else for reply <- call(replierNode) yield reply -> (if replierNode.isDown then "DOWN" else RoleOrdinal_nameOf(replierNode.participant.getRoleOrdinal))
										}
									}
								} yield replyAndRole match {
									case null =>
										scribe.trace(s"$inquirerId -< $replierId: $requestId:$requestDescription failed because the node is down, $numberOfTravelingMessages messages are traveling.")
										val respondingDuty = netSequencer.Duty_mine[Unit] { () =>
											injectConfigurationNoise(configChangeBeforeResponseDelivered_probability)
											covenant.fulfill((Failure(new RuntimeException(s"Net: target node is down: requestId=$requestId")), requestId), true)
											injectConfigurationNoise(configChangeAfterResponseDelivered_probability)
										}
										responseChannel.enqueue(respondingDuty)

									case reply -> replierRole =>
										scribe.trace(s"$inquirerId -< $replierId: $requestId:$requestDescription returned `$reply`, received as $replierRole, $numberOfTravelingMessages messages are traveling.")
										val response =
											if responseIsCursed then Failure(new RuntimeException(s"Net: simulated failure of response $requestId"))
											else Success(reply)
										val respondingDuty = netSequencer.Duty_mine[Unit] { () =>
											injectConfigurationNoise(configChangeBeforeResponseDelivered_probability)
											covenant.fulfill((response, requestId), true)
											injectConfigurationNoise(configChangeAfterResponseDelivered_probability)
										}
										responseChannel.enqueue(respondingDuty)
								}

							}
						// Enqueue the lazy duty that performs the RPC in the channel corresponding to the requests from the inquirer to the replier.
						requestChannel.enqueue(requestingDuty)

						while numberOfTravelingMessages > enqueueThresholdForEarlyDelivery do chooseAChannel().dispatchNext()
					}

					netSequencer.Task_fromDuty(covenant.map {
						case (response, requestId) =>
							// TODO consider moving this to the line after calling `covenant.fulfill` (which would avoid the need to pass the requestId) and also consider using a commitment instead.
							scribe.trace(s"$inquirerId <- $replierId: $requestId:$response, $numberOfTravelingMessages messages on the way")
							response
					})

				} else {
					/// Simple implementation that always succeeds and adds no randomness
					val replierNode = getNode(replierId)
					replierNode.sequencer.Duty_mineFlat[R] { () =>
						call(replierNode)
					}.onBehalfOf(netSequencer).toTask
				}
			}
		}

		private def chooseAChannel(): Channel = {
			val alternatives: mutable.Buffer[Channel] = mutable.Buffer.empty
			for i <- 0 until clusterSize do {
				for j <- 0 until clusterSize do {
					val channel = channelBySenderByReceiver(i)(j)
					if channel.nonEmpty then alternatives.addOne(channel)
				}
			}
			alternatives(random.between(0, alternatives.size))
		}

		//// The following members correspond to the mechanism that produces configuration changes. ////

		type ConfigMask = IArray[Boolean]

		private var configChangeRequestSequencer: Int = 0
		/** Counts how many times the [[TestClientCommand]] was sent. */
		private var commandsSentByClients_count = 0
		/** Counts how many times the [[injectConfigurationNoise]] method was invoked in all nodes. The probability that a configuration change actually occurs in an invocation is very low. */
		private var configNoiseInjection_count = 0
		/** Counts how many times the [[injectConfigurationNoise]] method was invoked since the last [[TestClientCommand]] was sent. */
		private var configNoiseInjectionsSinceLastClientCommand_count = 2 * clusterSize * clusterSize // Initialized with an estimation
		private var numberOfConfigNoiseInjectionsBetweenThePreviousTwoClientCommands = 0
		val initialConfigMask: ConfigMask = {
			val a = Array.fill(clusterSize)(random.nextBoolean())
			if a.contains(true) then IArray.unsafeFromArray(a)
			else IArray.fill(clusterSize)(true)
		}
		private var lastProposedConfigMask: ConfigMask = initialConfigMask
		private var activeConfigChange: ConfigChange[Id] = StableConfigChange(0, "", 0, nodesIncludedIn(initialConfigMask), nodesIncludedIn(initialConfigMask))

		/** Should be called when the client simulator sends a command to a consensus-participant, before it is received.
		 * Needed to allow the [[Net]] to count the number of commands sent, which is required to adjust the configuration noise probability. */
		def onBeforeClientCommandSent(): Unit = {
			numberOfConfigNoiseInjectionsBetweenThePreviousTwoClientCommands = configNoiseInjectionsSinceLastClientCommand_count
			configNoiseInjectionsSinceLastClientCommand_count = 0
			commandsSentByClients_count += 1
		}

		private def injectConfigurationNoise(changeProbability: Float): Unit = {
			assert(netSequencer.isInSequence)
			configNoiseInjection_count += 1
			configNoiseInjectionsSinceLastClientCommand_count += 1
			determineNewConfig(changeProbability).foreach { case (previousConfigMask, newConfigMask) =>
				configChangeRequestSequencer += 1
				val configChangeRequest = configChangeRequestSequencer
				scribe.info(s"Net: About to request configuration change #$configChangeRequest from ${previousConfigMask.mkString("[", ", ", "]")} to ${newConfigMask.mkString("[", ", ", "]")}")
				// For each node, create a duty that request a configuration change to the same newConfig.
				val configChangeRequestingDutyByNodeIndex =
					for node <- nodeByIndex yield netSequencer.Duty_foreign(node.sequencer)(
						node.sequencer.Duty_mineFlat(() =>
							node.clusterParticipant.delegate.requestConfigChange(s"ccReq-$configChangeRequest", nodesIncludedIn(newConfigMask).toSet)
						)
					)
				// trigger all those duties in their respective node's sequencer
				for configChangeReplies <- netSequencer.Duty_sequenceToArray(configChangeRequestingDutyByNodeIndex) do {
					scribe.info(s"Net: the configuration change request #$configChangeRequest sent to each node completed with: ${configChangeReplies.mkString("[", ", ", "]")}.")
				}
			}
		}

		private def determineNewConfig(changeProbabilityBetweenClientCommands: Float): Maybe[(previousConfig: ConfigMask, newConfig: ConfigMask)] = {
			assert(netSequencer.isInSequence)
			val numberOfConfigNoiseInjectionsPerClientCommand = configNoiseInjection_count / commandsSentByClients_count
			val adjustedProbability = changeProbabilityBetweenClientCommands / numberOfConfigNoiseInjectionsPerClientCommand
			if random.nextFloat() >= adjustedProbability then Maybe.empty
			else {
				val newConfigMask = IArray.unsafeFromArray(Array.fill[Boolean](clusterSize)(random.nextBoolean))
				if newConfigMask.contains(true) then {
					val oldConfigMask = lastProposedConfigMask
					lastProposedConfigMask = newConfigMask
					Maybe((oldConfigMask, newConfigMask))
				} else Maybe.empty
			}
		}

		/** Gets the [[Id]]s of the [[Node]]s included in the provided configuration mask. */
		def nodesIncludedIn(configMask: ConfigMask): ListSet[Id] = {
			val includedNodes = ListSet.newBuilder[Id]
			var nodeIndex = 0
			while nodeIndex < clusterSize do {
				if configMask(nodeIndex) then includedNodes.addOne(nodesIds(nodeIndex))
				nodeIndex += 1
			}
			includedNodes.result()
		}

		def onActiveConfigChanged(change: ConfigChange[Id]): Unit = {
			netSequencer.execute {
				activeConfigChange = change
				for nodeIndex <- 0 until clusterSize do {
					val node = this.getNode(nodeIndex)
					if change.newParticipants.contains(node.myId) then {
						node.startsIfNotRunning(false).triggerAndForget(false)
					}
				}
			}
		}

		/** TODO consider removing this. It is responsibility of the [[ConsensusParticipant]] to coordinate the stop of the participants. */
		def onNodeStopped(node: Node): Unit = {
			netSequencer.execute {
				if activeConfigChange.isActive(node.myId) then node.startsIfNotRunning(false).triggerAndForget(false)
			}
		}
	}

	/**
	 * Simulates a client.
	 * @param net The network to use.
	 * @param startWithHighestPriorityParticipant determines from with side of the known participants queue to start the attempts to send commands. */
	private class Client[N <: Net](clientId: String, val net: N, startWithHighestPriorityParticipant: Boolean) {
		private var knownParticipants: ListSet[Id] = net.nodesIncludedIn(net.initialConfigMask)
		private var targetParticipant: Node = net.getNode(if startWithHighestPriorityParticipant then knownParticipants.head else knownParticipants.last)
		private val alreadyTriedParticipants: mutable.Set[Id] = mutable.Set.empty

		/**
		 * Sends a command to a [[Node]] of the net.
		 * Initially, the target [[Node]] is the [[Node]] at the [[initialReceiverIndex]].
		 * If the target [[Node]] responds with a [[RedirectTo]] the target [[Node]] is updated to the redirected [[Node]] and following commands are sent to the new target [[Node]].
		 * If the target [[Node]] responds with an [[Unable]] the target [[Node]] is updated to the next [[Node]] of the [[Net]] and the command is retried to it.
		 * If the target [[Node]] responds with a [[Processed]], [[Superseded]], [[Stale]], or [[TooOld]] the command is considered processed and the target [[Node]] is not updated.
		 * @param commandPayload The payload of the command to send.
		 * @param attemptFlag tells the participant that will receive the command whether this is the first attempt, a redirect, or a fallback.
		 * @return A task that completes with true if the command was processed; false if the command was not processed despite all nodes were tried or the net is empty. */
		def sendsCommand(commandPayload: Int, attemptFlag: CommandAttemptFlag = FIRST_ATTEMPT): net.netSequencer.Duty[Boolean] = {
			if knownParticipants.isEmpty then return net.netSequencer.Duty_false
			val receiverNode = targetParticipant
			scribe.info(s"Client: Sent command:$commandPayload, attemptFlag:$attemptFlag, to:${receiverNode.myId}")
			net.onBeforeClientCommandSent()

			def retry(): net.netSequencer.Duty[Boolean] = {
				knownParticipants.find(p => !alreadyTriedParticipants.contains(p)).fold {
					alreadyTriedParticipants.clear()
					net.netSequencer.Duty_false
				} { chosen =>
					targetParticipant = net.getNode(chosen)
					sendsCommand(commandPayload, FALLBACK)
				}
			}

			net.netSequencer.Duty_foreign(receiverNode.sequencer)(receiverNode.sequencer.Duty_mineFlat { () =>
				receiverNode.clusterParticipant.delegate.onCommandFromClient(TestClientCommand(commandPayload, clientId), attemptFlag)
			}).flatMap[Boolean] {
				case receiverNode.Processed(index, content) =>
					scribe.info(s"Client: command `$commandPayload` was processed @$index by ${receiverNode.myId} which replied with `$content`.")
					alreadyTriedParticipants.clear()
					net.netSequencer.Duty_true
				case receiverNode.RedirectTo(leaderId) =>
					scribe.info(s"Client: the follower ${receiverNode.myId} redirected the command `$commandPayload` to the leader $leaderId.")
					targetParticipant = net.getNode(leaderId)
					sendsCommand(commandPayload, REDIRECTED)
				case receiverNode.Unable(roleOrdinal, otherParticipants) =>
					knownParticipants = otherParticipants + receiverNode.myId
					alreadyTriedParticipants.addOne(targetParticipant.myId)
					scribe.info(s"Client: the participant ${receiverNode.myId} is unable to process the command `$commandPayload`. otherParticipants=$otherParticipants, knowParticipants=$knownParticipants, alreadyTriedParticipants=$alreadyTriedParticipants")
					retry()
				case receiverNode.InconsistentState(m) =>
					scribe.info(s"Client: the participant ${receiverNode.myId} internal state become inconsistent. Detected when the command `$commandPayload` was processed.")
					throw new NotImplementedError()
				case stale: receiverNode.Stale =>
					scribe.info(s"Client: command `${stale.command}` is stale. The last received command at that moment was at index @${stale.lastCommandIndex}. Received by ${receiverNode.myId}.")
					alreadyTriedParticipants.clear()
					net.netSequencer.Duty_true
				case superseded: receiverNode.Superseded =>
					scribe.info(s"Client: command `${superseded.command}` is superseded. The last received command at that moment was at index @${superseded.lastCommandIndex}. Received by ${receiverNode.myId}.")
					alreadyTriedParticipants.clear()
					net.netSequencer.Duty_true
				case tooOld: receiverNode.TooOld =>
					scribe.info(s"Client: command `${tooOld.command}` is too old. Received by ${receiverNode.myId}.")
					alreadyTriedParticipants.clear()
					net.netSequencer.Duty_true
			}
		}

		def sendsCommandsUntil(predicate: (commandIndex: Int) => Boolean, maxRetries: Int = 9): net.netSequencer.Task[Unit] = {

			def sendCommandLoop(commandIndex: Int, attemptsCounter: Int): net.netSequencer.Task[Unit] = {
				if predicate.apply(commandIndex) then net.netSequencer.Task_unit
				else if attemptsCounter > maxRetries then net.netSequencer.Task_failed(new AssertionError("The cluster got stuck unable to progress"))
				else for {
					wasProcessed <- sendsCommand(commandIndex).toTask
					_ <- {
						if wasProcessed then sendCommandLoop(commandIndex + 1, 0)
						else {
							scribe.info(s"Client: The command $commandIndex was tried with all the participants. Retrying all again. Attempts done so far: ${attemptsCounter + 1}.")
							sendCommandLoop(commandIndex, attemptsCounter + 1)
						}
					}
				} yield ()
			}

			net.netSequencer.Task_ownFlat(() =>
				sendCommandLoop(1, 0)
			)
		}

	}

	private trait NodeStateChangesListener {
		/** Called when any entry in the log buffer of a [[Node]] is overwritten.
		 * @param index the [[RecordIndex]] of the first overwritten entry.
		 * @param firstReplacedRecord the first stored [[Record]] that is removed.
		 * @param firstReplacingRecord the [[Record]] with which the first overwritten log entry is replaced with.
		 * @param roleOrdinal the behavior of the [[ConsensusParticipant]] when the conflict occurred. */
		def onLogOverwrite(index: RecordIndex, firstReplacedRecord: Record, firstReplacingRecord: Record, roleOrdinal: RoleOrdinal): Unit = ()

		/** Called when a [[Record]] is appended to the log buffer. */
		def onRecordAppended(record: Record, index: RecordIndex): Unit = ()

		def onCommandApplied(command: TestClientCommand, index: RecordIndex): Unit = ()
	}

	/**
	 * An implementation of the [[ConsensusParticipantSdm]] for testing.
	 */
	private class Node(val myId: Id, initialParticipants: Set[Id], net: Net) extends ConsensusParticipantSdm { thisNode =>

		import net.rpc

		override type ParticipantId = Id
		override type ClientCommand = TestClientCommand
		override type StateMachineResponse = Int
		override type ClientId = String

		override val sequencer: ScheduSequen = sharedDap.provide(s"node-sequencer-$myId")

		var statesChangesListener: NodeStateChangesListener = new NodeStateChangesListener() {
			override def onLogOverwrite(index: RecordIndex, firstReplacedRecord: Record, firstReplacingRecord: Record, roleOrdinal: RoleOrdinal): Unit = ()
		}

		private var initialNotificationListener: NotificationListener = new DefaultNotificationListener()

		private var _participant: ConsensusParticipant = uninitialized

		override val clientCommandOrdering: Ordering[TestClientCommand] = (x: TestClientCommand, y: TestClientCommand) => x.value - y.value

		override def clientIdOf(command: TestClientCommand): ClientId = command.clientId

		inline def isDown: Boolean = _participant eq null

		/** @return the [[ConsensusParticipant]] service instance corresponding to this [[Node]]. */
		inline def participant: ConsensusParticipant = _participant

		/** Initializes this [[Node]]. Does not start the [[ConsensusParticipant]] service. */
		inline def initialize(initialNotificationListener: NotificationListener = DefaultNotificationListener()): Unit = {
			this.initialNotificationListener = initialNotificationListener
		}

		/** Creates the [[ConsensusParticipant]] service instance of this [[Node]]. */
		def startsIfNotRunning(isSeed: Boolean): sequencer.Duty[Unit] = {
			sequencer.Duty_mine { () =>
				if isDown || participant.getRoleOrdinal == STOPPED then {
					scribe.info(s"node-$myId: about to create the consensus participant service")
					_participant = ConsensusParticipant(clusterParticipant, storage, machine, isSeed, List(initialNotificationListener, notificationScribe))
				}
			}
		}

		object machine extends StateMachine {
			private val appliedCommands: mutable.Map[RecordIndex, ClientCommand] = mutable.LongMap.empty
			var highestAppliedIndex: RecordIndex = 0

			override def applyClientCommand(index: RecordIndex, command: ClientCommand): sequencer.LatchedDuty[StateMachineResponse] = {
				sequencer.checkWithin()

				if index <= highestAppliedIndex then assert(appliedCommands.get(index).contains(command))
				else {
					highestAppliedIndex = index
					appliedCommands.put(index, command)
					statesChangesListener.onCommandApplied(command, index)
				}


				// TODO add delay
				sequencer.LatchedDuty_ready(command.value)
			}

			override def recoverIndexOfLastAppliedCommand: sequencer.LatchedDuty[RecordIndex] = {
				sequencer.checkWithin()
				sequencer.LatchedDuty_ready(0)
			}
		}

		override def isEager: Boolean = false

		/**
		 * Test instance and implementation of the [[ClusterParticipant]] service interface required by the [[participant]] (the [[ConsensusParticipant]] service corresponding to a [[Node]]).
		 */
		object clusterParticipant extends ClusterParticipant {

			override val boundParticipantId: ParticipantId = myId
			private var currentParticipants: Set[ParticipantId] = initialParticipants

			var delegate: Delegate = uninitialized

			override def getInitialParticipants: Set[ParticipantId] = {
				sequencer.checkWithin()
				currentParticipants
			}

			override def setBound(delegate: Delegate): Unit = {
				sequencer.checkWithin()
				this.delegate = delegate
			}

			/** Called by the [[ConsensusParticipant]] when it is leaving existence. */
			override def removeBound(): Unit = {
				sequencer.checkWithin()
				this.delegate = null
			}

			override def onActiveConfigChanged(change: ConfigChange[ParticipantId], roleOrdinal: RoleOrdinal): Unit = {
				sequencer.checkWithin()
				scribe.info(s"cluster-$boundParticipantId: onConfigurationChanged called by a ${RoleOrdinal_nameOf(roleOrdinal)} with `$change`.")
				if roleOrdinal == LEADER then net.onActiveConfigChanged(change)
			}

			extension (replierId: ParticipantId) {


				override def howAreYou(inquirerInfo: StateInfo): sequencer.Task[StateInfo] = {
					sequencer.checkWithin()
					boundParticipantId.rpc[StateInfo](
						replierId,
						s"HowAreYou(inquirerInfo=$inquirerInfo)"
					) { replierNode =>
						replierNode.clusterParticipant.delegate.onHowAreYou(boundParticipantId, inquirerInfo)
					}.onBehalfOf(sequencer)
				}

				override def chooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.Task[Vote[ParticipantId]] = {
					sequencer.checkWithin()
					boundParticipantId.rpc[Vote[ParticipantId]](
						replierId,
						s"ChooseALeader(inquirerId:$inquirerId, inquirerInfo:$inquirerInfo)"
					) { replier =>
						replier.clusterParticipant.delegate.onChooseALeader(inquirerId, inquirerInfo)
					}.onBehalfOf(sequencer)
				}

				override def appendRecords(inquirerTerm: Term, prevLogIndex: RecordIndex, prevLogTerm: Term, records: GenIndexedSeq[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.Task[AppendResult] = {
					sequencer.checkWithin()
					boundParticipantId.rpc[AppendResult](
						replierId,
						s"AppendRecords(inquirerTerm:$inquirerTerm, previousLogIndex:$prevLogIndex, previousLogTerm:$prevLogTerm, records:$records, leaderCommit:$leaderCommit, termAtLeaderCommit:$termAtLeaderCommit)"
					) { replier =>
						replier.clusterParticipant.delegate.onAppendRecords(boundParticipantId, inquirerTerm, prevLogIndex, prevLogTerm, records, leaderCommit, termAtLeaderCommit)
					}.onBehalfOf(sequencer)
				}
			}

			override def getOtherProbableParticipant: ListSet[ParticipantId] = ListSet.from(net.nodesIds) - myId

			override def onStopped(motive: Try[String]): Unit = {
				scribe.info(s"cluster-$myId: `onStopped` was called with motive=$motive")
				thisNode._participant = null
				net.onNodeStopped(thisNode)
			}
		}

		/**
		 * Test instance and implementation of the [[Storage]] service interface required by the [[participant]] (the [[ConsensusParticipant]] service corresponding to a [[Node]]).
		 */
		object storage extends Storage {
			private[ConsensusParticipantSdmTest] var memory: WS = TestWorkspace()

			override def load: sequencer.LatchedDuty[Try[WS]] = {
				sequencer.checkWithin()
				sequencer.LatchedDuty_ready(Success(memory))
			} // TODO add a delay

			override def save(workspace: WS): sequencer.LatchedDuty[Try[Unit]] = {
				sequencer.checkWithin()
				memory = workspace
				sequencer.LatchedDuty_ready(Doer.successUnit) // TODO add a delay
			}
		}

		override type WS = TestWorkspace

		/**
		 * Test implementation of [[Workspace]]
		 */
		class TestWorkspace extends Workspace {
			private var currentTerm: Term = 0
			private var _logBufferOffset: RecordIndex = 1
			private val logBuffer: mutable.ArrayBuffer[Record] = mutable.ArrayBuffer.empty

			override def getCurrentTerm: Term = {
				sequencer.checkWithin()
				currentTerm
			}

			override def setCurrentTerm(term: Term): Unit = {
				sequencer.checkWithin()
				currentTerm = term
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
				logBuffer((index - logBufferOffset).toInt)
			}

			override def getRecordsBetween(from: RecordIndex, until: RecordIndex): IArray[Record] = {
				sequencer.checkWithin()
				val fromIndex = (from - logBufferOffset).toInt
				val untilIndex = (until - logBufferOffset).toInt
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
				val index = firstEmptyRecordIndex
				logBuffer.addOne(record)
				statesChangesListener.onRecordAppended(record, index)
			}

			override def appendResolvingConflicts(records: GenIndexedSeq[Record], from: RecordIndex): Unit = {
				sequencer.checkWithin()
				var storedIndex = (from - logBufferOffset).toInt
				var newIndex = 0
				var conflictFound = false
				while newIndex < records.size && storedIndex < logBuffer.size && !conflictFound do {
					if logBuffer(storedIndex).term != records(newIndex).term then conflictFound = true
					else {
						storedIndex += 1
						newIndex += 1
					}
				}
				if conflictFound then {
					statesChangesListener.onLogOverwrite(storedIndex + logBufferOffset, logBuffer(storedIndex), records(newIndex), participant.getRoleOrdinal)
					logBuffer.takeInPlace(storedIndex)
				}
				while newIndex < records.size do {
					appendRecord(records(newIndex))
					newIndex += 1
				}
			}

			override def indexOfTopConfigChange: RecordIndex = {
				sequencer.checkWithin()
				val internalIndex = logBuffer.lastIndexWhere(_.isInstanceOf[ConfigChange[?]])
				if internalIndex < 0 then 0
				else _logBufferOffset + internalIndex
			}

			override def informAppliedCommandIndex(appliedCommandIndex: RecordIndex): Unit = {
				sequencer.checkWithin()
				()
			}

			override def indexOfLastAppendedCommandFrom(clientId: ClientId): RecordIndex = {
				sequencer.checkWithin()
				// @formatter:off
				val index = logBuffer.lastIndexWhere {
					case CommandRecord[TestClientCommand @unchecked](term, command) => command.clientId == clientId
					case _ => false
				}
				// @formatter:on
				if index == -1 then 0 else index + _logBufferOffset
			}

			override def indexOf(clientCommand: TestClientCommand): RecordIndex = {
				sequencer.checkWithin()
				0
			}

			override def releases: sequencer.Duty[Unit] = {
				scribe.info(s"workspace-$myId: was released")
				sequencer.Duty_unit
			}
		}

		object notificationScribe extends NotificationListener {
			override def onStarting(previous: RoleOrdinal, isSeed: Boolean): Unit = {
				sequencer.checkWithin()
				scribe.info(s"scribe-$myId: is starting ${if isSeed then "as seed" else "to join"}.")
			}

			override def onStarted(previous: RoleOrdinal, term: Term, initialConfigChange: ConfigChange[ParticipantId], isSeed: Boolean): Unit = {
				sequencer.checkWithin()
				scribe.info(s"scribe-$myId: completed the start-up with: previousRole=$previous, term=$term, initialConfigChange=$initialConfigChange, isSeed=$isSeed")
			}

			override def onBecameStopped(previous: RoleOrdinal, term: Term, motive: Try[String]): Unit = {
				sequencer.checkWithin()
				scribe.info(s"scribe-$myId: became stopped from ${RoleOrdinal_nameOf(previous)} during term $term because $motive.")
			}

			override def onJoining(previous: RoleOrdinal): Unit = {
				sequencer.checkWithin()
				scribe.info(s"scribe-$myId: is catching-up to join.")
			}

			override def onBecameIsolated(previous: RoleOrdinal, term: Term): Unit = {
				sequencer.checkWithin()
				scribe.info(s"scribe-$myId: became isolated from ${RoleOrdinal_nameOf(previous)} during term $term.")
			}

			override def onBecameCandidate(previous: RoleOrdinal, term: Term): Unit = {
				sequencer.checkWithin()
				scribe.info(s"scribe-$myId: became candidate from ${RoleOrdinal_nameOf(previous)} during term $term.")
			}

			override def onBecameFollower(previous: RoleOrdinal, term: Term, leaderId: ParticipantId): Unit = {
				sequencer.checkWithin()
				scribe.info(s"scribe-$myId: became follower of $leaderId from ${RoleOrdinal_nameOf(previous)} during term $term")
			}

			override def onPromoting(previous: RoleOrdinal, term: Term): Unit = {
				sequencer.checkWithin()
				scribe.info(s"scribe-$myId: is claiming leadership at term $term from ${RoleOrdinal_nameOf(previous)}.")
			}

			override def onBecameLeader(previous: RoleOrdinal, term: Term): Unit = {
				sequencer.checkWithin()
				scribe.info(s"scribe-$myId: became leader of term $term from ${RoleOrdinal_nameOf(previous)}")
			}

			override def onHandingOff(term: Term): Unit = {
				sequencer.checkWithin()
				scribe.info(s"scribe-$myId: is handing off the leadership. The term $term is over.")
			}

			override def onRetiring(term: Term): Unit = {
				sequencer.checkWithin()
				scribe.info(s"scribe-$myId: is retiring because it was excluded. Its final term is $term.")
			}

			override def onRoleLeft(left: RoleOrdinal, term: Term): Unit = ()

			override def onCommitIndexChanged(previous: RecordIndex, current: RecordIndex): Unit = {
				sequencer.checkWithin()
				scribe.info(s"scribe-$myId: commitIndex changed from $previous to $current")
			}

			override def onActiveConfigChanged(currentRole: RoleOrdinal, currentTerm: Term, configChangeIndex: RecordIndex, configChange: ConfigChange[ParticipantId]): Unit = {
				sequencer.checkWithin()
				scribe.info(s"scribe-$myId: the active configuration has changed: currentBehavior=$currentRole, currentTerm=$currentTerm, changeIndex=$configChangeIndex, configChange=$configChange")
			}
		}
	}


	/** Helper to create and initialize [[Node]]s.
	 * @param net the [[Net]] where the created [[Node]] instances will be added.
	 * @param weakReferencesHolder a collection to which the created instances of [[NotificationListener]] are added in order to avoid being garbage-collected.
	 * @param notificationListenerBuilder a function that takes the new [[Node]] and builds the [[NotificationListener]] to be passed its [[ConsensusParticipantSdm.ConsensusParticipant]] service constructor. */
	private def createsAndInitializesNodes[N <: Net](net: N, weakReferencesHolder: mutable.Buffer[AnyRef])(notificationListenerBuilder: (node: Node) => node.NotificationListener): Unit = {

		val initialParticipants = net.nodesIncludedIn(net.initialConfigMask).toSet
		for id <- net.nodesIds do {
			val node = Node(id, initialParticipants, net)
			net.addNode(node)
			val nl = notificationListenerBuilder(node)
			weakReferencesHolder.addOne(nl)
			node.initialize(nl)
		}
	}

	private def testAllInvariants(net: Net, startWithHighestPriorityParticipant: Boolean, numberOfCommandsToSend: Int = 20): Future[Unit] = {
		val promise = Promise[Unit]()
		val clusterSize = net.clusterSize
		val weakReferencesHolder = mutable.Buffer.empty[AnyRef]
		val leaderNodeByTerm: mutable.Buffer[Node] = mutable.Buffer.empty
		val committedRecordsByNodeIndex: Array[mutable.Buffer[Record]] = Array.fill(clusterSize)(mutable.Buffer.empty)
		val appliedCommandsByNodeIndex: Array[mutable.LongMap[TestClientCommand | None.type]] = Array.fill(clusterSize)(mutable.LongMap.withDefault(_ => None))

		createsAndInitializesNodes(net, weakReferencesHolder) { node =>
			node.statesChangesListener = new NodeStateChangesListener() {
				override def onLogOverwrite(index: RecordIndex, firstReplacedRecord: Record, firstReplacingRecord: Record, roleOrdinal: RoleOrdinal): Unit = {
					// "Leader Append-Only: a leader never overwrites or deletes entries in its log; it only appends new entries. §5.3"
					if roleOrdinal == LEADER then promise.tryFailure(new AssertionError(s"The participant ${node.myId} broke the \"append only rule\" at index $index. Removed record: $firstReplacedRecord, replacing record: $firstReplacingRecord."))
				}

				override def onRecordAppended(record: Record, index: RecordIndex): Unit = {
					// Log Matching: if two logs contain an entry with the same index and term, then the logs are identical in all entries up through the given index. §5.3
					val thisNodeRecords = node.storage.memory.getRecordsBetween(1, index)

					for nodeIndex <- 0 until clusterSize do {
						val otherNode = net.getNode(nodeIndex)
						if otherNode ne node then {
							otherNode.sequencer.execute {
								if otherNode.storage.memory.firstEmptyRecordIndex > index && otherNode.storage.memory.getRecordAt(index).term == record.term then {
									val otherNodeRecords = otherNode.storage.memory.getRecordsBetween(1, index)
									if !otherNodeRecords.sameElements(thisNodeRecords) then promise.tryFailure(new AssertionError(s"The logs of nodes ${node.myId} and ${otherNode.myId} are not identical in all entries up through $index despite the records at $index have the same term. ${otherNodeRecords.toSeq}==${thisNodeRecords.toSeq}"))
								}
							}

						}
					}
				}

				override def onCommandApplied(command: node.ClientCommand, index: RecordIndex): Unit = {
					// State Machine Safety: if a server has applied a log entry at a given index to its state machine, no other server will ever apply a different log entry for the same index. §5.4.3
					val commandsAppliedToThisNode = appliedCommandsByNodeIndex(net.indexOf(node.myId))
					val previouslyAppliedCommand = commandsAppliedToThisNode(index.toInt)
					if previouslyAppliedCommand ne None then promise.tryFailure(new AssertionError(s"Two commands with same index were applied to the node ${node.myId}: previous=$previouslyAppliedCommand, new=$command"))
					var i = index.toInt
					while i < numberOfCommandsToSend do {
						i += 1
						if commandsAppliedToThisNode(i) ne None then promise.tryFailure(new AssertionError(s"The command $command was applied with index $index which is less than the index $i of the previously applied command ${commandsAppliedToThisNode(i)}."))
					}
					commandsAppliedToThisNode(index.toInt) = command

					for nodeIndex <- 0 until clusterSize do {
						val otherNode = net.getNode(nodeIndex)
						if otherNode ne node then {
							otherNode.sequencer.execute {
								val commandsAppliedToTheOtherNode = appliedCommandsByNodeIndex(net.indexOf(otherNode.myId))
								val commandAppliedToTheOtherNodeAtIndex = commandsAppliedToTheOtherNode(index.toInt)
								if (commandAppliedToTheOtherNodeAtIndex ne None) && commandAppliedToTheOtherNodeAtIndex != command then
									promise.tryFailure(new AssertionError(s"The node ${node.myId} applied the command $command at index $index, which is different from the command $commandAppliedToTheOtherNodeAtIndex applied at the same index in node ${otherNode.myId}."))

							}

						}
					}
				}
			}
			new node.DefaultNotificationListener() {
				override def onBecameLeader(previous: RoleOrdinal, term: Term): Unit = {
					// Election Safety: at most one leader can be elected in a given term. §5.2
					if leaderNodeByTerm.size < term then leaderNodeByTerm.addOne(node)
					else if leaderNodeByTerm(term - 1) ne node then promise.tryFailure(new AssertionError(s"Node ${node.myId} became leader at term $term despite node ${leaderNodeByTerm(term - 1).myId} was a leader of the same term before"))

					// Leader Completeness: if a log entry is committed in a given term, then that entry will be present in the logs of the leaders for all higher-numbered terms. §5.4
					val thisNodeRecords = node.storage.memory.getRecordsBetween(1, node.storage.memory.firstEmptyRecordIndex)
					// check that the log of the leader contains the records previously committed by all participants
					for nodeIndex <- 0 until clusterSize do {
						val otherNode = net.getNode(nodeIndex)
						otherNode.sequencer.execute {
							val committedRecordsMemory = committedRecordsByNodeIndex(nodeIndex)
							for committedRecordIndex <- committedRecordsMemory.indices if committedRecordIndex < thisNodeRecords.size do {
								val committedRecord = committedRecordsMemory(committedRecordIndex)
								val storedRecord = thisNodeRecords(committedRecordIndex)
								if storedRecord != committedRecord then promise.tryFailure(new AssertionError(s"The record $committedRecord committed by ${otherNode.myId} at index ${committedRecordIndex + 1} does not match $storedRecord, the one in the log of the leader ${node.myId}"))
							}
						}
					}
				}

				override def onCommitIndexChanged(previous: RecordIndex, current: RecordIndex): Unit = {
					val committedRecordsMemory = committedRecordsByNodeIndex(net.indexOf(node.myId))
					if committedRecordsMemory.size < current then {
						val newCommittedRecords = node.storage.memory.getRecordsBetween(committedRecordsMemory.size + 1, current)
						committedRecordsMemory.addAll(newCommittedRecords)
					}
				}
			}
		}

		val checks = for {
			_ <- net.startsAllNodes
			client = Client[net.type]("A", net, startWithHighestPriorityParticipant)
			_ <- client.sendsCommandsUntil(commandIndex => commandIndex > numberOfCommandsToSend || promise.isCompleted, 20)
				.toDuty(promise.tryFailure)
		} yield promise.tryComplete(Success(()))
		checks.triggerAndForget(false)
		promise.future.andThen(_ => net.stop())
	}

	test("assertions are enabled") {
		println(ConsensusParticipantSdm.assertionsEnabled)
		if ConsensusParticipantSdm.assertionsEnabled then Future.successful(()) else Future.failed(new RuntimeException(""))
	}

	test("special sample") {
		val startWithHighestPriorityParticipant = false
		val net = new Net(clusterSize = 5, randomnessSeed = -4515092198012648896L, requestFailurePercentage = 10, responseFailurePercentage = 10, stimulusSettlingTime = 5, configChangeBeforeRequestDelivered_probability = 0, configChangeBeforeResponseDelivered_probability = 0, configChangeAfterResponseDelivered_probability = 0)
		scribe.info(s"Begin: clusterSize=${net.clusterSize}, initialConfig=${net.initialConfigMask.mkString("[", ", ", "]")}, startWithHighestPriorityParticipant=$startWithHighestPriorityParticipant, netRandomnessSeed=${net.randomnessSeed}")

		testAllInvariants(net, startWithHighestPriorityParticipant, numberOfCommandsToSend = 10)
	}

	test("All invariants must comply - without configuration changes noise") {
		inline val numberOfCommandsToSend = 10
		PropF.forAllNoShrinkF(
			Gen.choose(2, 7),
			Gen.oneOf(true, false),
			Gen.long
		) { (clusterSize, startWithHighestPriorityParticipant, netRandomnessSeed) =>
			val net = new Net(clusterSize, randomnessSeed = netRandomnessSeed, requestFailurePercentage = 10, responseFailurePercentage = 10, stimulusSettlingTime = 5, configChangeBeforeRequestDelivered_probability = 0, configChangeBeforeResponseDelivered_probability = 0, configChangeAfterResponseDelivered_probability = 0)
			scribe.info(s"\n----------------\nBegin: clusterSize=$clusterSize, initialConfig=${net.initialConfigMask.mkString("[", ", ", "]")}, startWithHighestPriorityParticipant=$startWithHighestPriorityParticipant, netRandomnessSeed=$netRandomnessSeed")
			testAllInvariants(net, startWithHighestPriorityParticipant, numberOfCommandsToSend)
		}
	}

	test("All invariants special case") {
		inline val numberOfCommandsToSend = 30
		val clusterSize = 3
		val startWithHighestPriorityParticipant = true
		val netRandomnessSeed = 4326203398922213591L
		val net = new Net(clusterSize, randomnessSeed = netRandomnessSeed, requestFailurePercentage = 10, responseFailurePercentage = 10, stimulusSettlingTime = 50)
		scribe.info(s"\n----------------\nBegin: clusterSize=$clusterSize, initialConfig=${net.initialConfigMask.mkString("[", ", ", "]")}, startWithHighestPriorityParticipant=$startWithHighestPriorityParticipant, netRandomnessSeed=$netRandomnessSeed")
		testAllInvariants(net, startWithHighestPriorityParticipant, numberOfCommandsToSend)
	}

	test("All invariants must comply") {
		inline val numberOfCommandsToSend = 30
		PropF.forAllNoShrinkF(
			Gen.choose(1, 9),
			Gen.oneOf(true, false),
			Gen.long
		) { (clusterSize, startWithHighestPriorityParticipant, netRandomnessSeed) =>
			val net = new Net(clusterSize, randomnessSeed = netRandomnessSeed, requestFailurePercentage = 10, responseFailurePercentage = 10, stimulusSettlingTime = 1)
			scribe.info(s"\n----------------\nBegin: clusterSize=$clusterSize, initialConfig=${net.initialConfigMask.mkString("[", ", ", "]")}, startWithHighestPriorityParticipant=$startWithHighestPriorityParticipant, netRandomnessSeed=$netRandomnessSeed")
			testAllInvariants(net, startWithHighestPriorityParticipant, numberOfCommandsToSend)
		}
	}

	test("Election Safety: at most one leader can be elected in a given term. §5.2".ignore) {
		inline val numberOfCommandsToSend = 10
		PropF.forAllNoShrinkF(
			Gen.choose(2, 5),
			Gen.oneOf(true, false),
			Gen.long
		) { (clusterSize, startWithHighestPriorityParticipant, netRandomnessSeed) =>
			val promise = Promise[Unit]()
			val net = new Net(clusterSize, randomnessSeed = netRandomnessSeed)
			scribe.info(s"\nBegin: clusterSize=$clusterSize, initialConfig=${net.initialConfigMask.mkString("[", ", ", "]")} , startWithHighestPriorityParticipant=$startWithHighestPriorityParticipant, netRandomnessSeed=$netRandomnessSeed")
			val weakReferencesHolder = mutable.Buffer.empty[AnyRef]
			val leaderNodeByTerm: mutable.Buffer[Node] = mutable.Buffer.empty

			createsAndInitializesNodes(net, weakReferencesHolder) { node =>
				new node.DefaultNotificationListener() {
					override def onBecameLeader(previous: RoleOrdinal, term: Term): Unit = {
						if leaderNodeByTerm.size < term then leaderNodeByTerm.addOne(node)
						else if leaderNodeByTerm(term - 1) ne node then promise.tryFailure(new AssertionError(s"Node ${node.myId} became leader at term $term despite node ${leaderNodeByTerm(term - 1).myId} was a leader of the same term before"))
					}
				}
			}

			val checks = for {
				_ <- net.startsAllNodes
				client = Client[net.type]("A", net, startWithHighestPriorityParticipant)
				_ <- client.sendsCommandsUntil(commandIndex => commandIndex > numberOfCommandsToSend || promise.isCompleted)
					.toDuty(promise.tryFailure)
			} yield promise.tryComplete(Success(()))
			checks.triggerAndForget(false)
			promise.future.andThen(_ => net.stop())
		}
	}

	test("Leader Append-Only: a leader never overwrites or deletes entries in its log; it only appends new entries. §5.3".ignore) {
		inline val numberOfCommandsToSend = 10
		PropF.forAllNoShrinkF(
			Gen.choose(2, 5),
			Gen.oneOf(true, false),
			Gen.long
		) { (clusterSize, startWithHighestPriorityParticipant, netRandomnessSeed) =>
			scribe.info(s"\nBegin: clusterSize=$clusterSize, startWithHighestPriorityParticipant=$startWithHighestPriorityParticipant, netRandomnessSeed=$netRandomnessSeed")

			val promise = Promise[Unit]()
			val net = new Net(clusterSize, randomnessSeed = netRandomnessSeed)
			val weakReferencesHolder = mutable.Buffer.empty[AnyRef]

			createsAndInitializesNodes(net, weakReferencesHolder) { node =>
				node.statesChangesListener = new NodeStateChangesListener() {
					override def onLogOverwrite(index: RecordIndex, firstReplacedRecord: Record, firstReplacingRecord: Record, roleOrdinal: RoleOrdinal): Unit = {
						if roleOrdinal == LEADER then promise.tryFailure(new AssertionError(s"The participant ${node.myId} broke the \"append only rule\" at index $index. Removed record: $firstReplacedRecord, replacing record: $firstReplacingRecord."))
					}
				}
				node.DefaultNotificationListener()
			}

			val checks: net.netSequencer.Task[Unit] = for {
				_ <- net.startsAllNodes.toTask
				client = Client[net.type]("A", net, startWithHighestPriorityParticipant)
				_ <- client.sendsCommandsUntil(commandIndex => commandIndex > numberOfCommandsToSend || promise.isCompleted)
			} yield promise.tryComplete(Success(()))

			checks.trigger(false)(promise.tryComplete)
			promise.future.andThen(_ => net.stop())
		}
	}

	test("Log Matching: if two logs contain an entry with the same index and term, then the logs are identical in all entries up through the given index. §5.3".ignore) {
		inline val numberOfCommandsToSend = 10

		PropF.forAllNoShrinkF(
			Gen.choose(2, 5),
			Gen.oneOf(true, false),
			Gen.long
		) { (clusterSize, startWithHighestPriorityParticipant, netRandomnessSeed) =>
			scribe.info(s"\nBegin: clusterSize=$clusterSize, startWithHighestPriorityParticipant=$startWithHighestPriorityParticipant, netRandomnessSeed=$netRandomnessSeed")

			val promise = Promise[Unit]()
			val net = new Net(clusterSize, randomnessSeed = netRandomnessSeed)
			val weakReferencesHolder = mutable.Buffer.empty[AnyRef]

			createsAndInitializesNodes(net, weakReferencesHolder) { node =>
				node.statesChangesListener = new NodeStateChangesListener() {
					override def onRecordAppended(record: Record, index: RecordIndex): Unit = {
						assert(record.asInstanceOf[CommandRecord[TestClientCommand]].command.value == index) // TODO remove or adapt to work when snapshots and configurations changes are added.

						val thisNodeRecords = node.storage.memory.getRecordsBetween(1, index)

						for nodeIndex <- 0 until clusterSize do {
							val otherNode = net.getNode(nodeIndex)
							if otherNode ne node then {
								otherNode.sequencer.execute {
									if otherNode.storage.memory.firstEmptyRecordIndex > index && otherNode.storage.memory.getRecordAt(index).term == record.term then {
										val otherNodeRecords = otherNode.storage.memory.getRecordsBetween(1, index)
										if !otherNodeRecords.sameElements(thisNodeRecords) then promise.tryFailure(new AssertionError(s"The logs of nodes ${node.myId} and ${otherNode.myId} are not identical in all entries up through $index despite the records at $index have the same term."))
									}
								}

							}
						}
					}
				}
				node.DefaultNotificationListener()
			}

			val checks: net.netSequencer.Task[Unit] = for {
				_ <- net.startsAllNodes.toTask
				client = Client[net.type]("A", net, startWithHighestPriorityParticipant)
				_ <- client.sendsCommandsUntil(commandIndex => commandIndex > numberOfCommandsToSend || promise.isCompleted)
			} yield promise.tryComplete(Success(()))

			checks.trigger(false)(promise.tryComplete)
			promise.future.andThen(_ => net.stop())
		}
	}

	test("Leader Completeness: if a log entry is committed in a given term, then that entry will be present in the logs of the leaders for all higher-numbered terms. §5.4".ignore) {
		inline val numberOfCommandsToSend = 10

		PropF.forAllNoShrinkF(
			Gen.choose(2, 5),
			Gen.oneOf(true, false),
			Gen.long
		) { (clusterSize, startWithHighestPriorityParticipant, netRandomnessSeed) =>
			scribe.info(s"\nBegin: clusterSize=$clusterSize, startWithHighestPriorityParticipant=$startWithHighestPriorityParticipant, netRandomnessSeed=$netRandomnessSeed")

			val promise = Promise[Unit]()
			val net = new Net(clusterSize, randomnessSeed = netRandomnessSeed)
			val weakReferencesHolder = mutable.Buffer.empty[AnyRef]
			val committedRecordsByNodeIndex: Array[mutable.Buffer[Record]] = Array.fill(clusterSize)(mutable.Buffer.empty)

			createsAndInitializesNodes(net, weakReferencesHolder) { node =>
				new node.DefaultNotificationListener() {
					override def onCommitIndexChanged(previous: RecordIndex, current: RecordIndex): Unit = {
						val committedRecordsMemory = committedRecordsByNodeIndex(net.indexOf(node.myId))
						if committedRecordsMemory.size < current then {
							val newCommittedRecords = node.storage.memory.getRecordsBetween(committedRecordsMemory.size + 1, current)
							committedRecordsMemory.addAll(newCommittedRecords)
						}
					}

					override def onBecameLeader(previous: RoleOrdinal, term: Term): Unit = {
						val thisNodeRecords = node.storage.memory.getRecordsBetween(1, node.storage.memory.firstEmptyRecordIndex)
						// check that the log of the leader contains the records previously committed by all participants
						for nodeIndex <- 0 until clusterSize do {
							val otherNode = net.getNode(nodeIndex)
							otherNode.sequencer.execute {
								val committedRecordsMemory = committedRecordsByNodeIndex(nodeIndex)
								for committedRecordIndex <- committedRecordsMemory.indices if committedRecordIndex < thisNodeRecords.size do {
									val committedRecord = committedRecordsMemory(committedRecordIndex)
									val storedRecord = thisNodeRecords(committedRecordIndex)
									if storedRecord != committedRecord then promise.tryFailure(new AssertionError(s"The record $committedRecord committed by ${otherNode.myId} at index ${committedRecordIndex + 1} does not match $storedRecord, the one in the log of the leader ${node.myId}"))
								}
							}
						}
					}
				}
			}

			val checks: net.netSequencer.Task[Unit] = for {
				_ <- net.startsAllNodes.toTask
				client = Client[net.type]("A", net, startWithHighestPriorityParticipant)
				_ <- client.sendsCommandsUntil(commandIndex => commandIndex > numberOfCommandsToSend || promise.isCompleted)
			} yield promise.tryComplete(Success(()))

			checks.trigger(false)(promise.tryComplete)
			promise.future.andThen(_ => net.stop())
		}
	}

	test("State Machine Safety: if a server has applied a log entry at a given index to its state machine, no other server will ever apply a different log entry for the same index. §5.4.3".ignore) {
		inline val numberOfCommandsToSend = 10

		PropF.forAllNoShrinkF(
			Gen.choose(2, 5),
			Gen.oneOf(true, false),
			Gen.long
		) { (clusterSize, startWithHighestPriorityParticipant, netRandomnessSeed) =>
			scribe.info(s"\nBegin: clusterSize=$clusterSize, startWithHighestPriorityParticipant=$startWithHighestPriorityParticipant, netRandomnessSeed=$netRandomnessSeed")

			val promise = Promise[Unit]()
			val net = new Net(clusterSize, randomnessSeed = netRandomnessSeed)
			val weakReferencesHolder = mutable.Buffer.empty[AnyRef]
			val appliedCommandsByNodeIndex: Array[mutable.LongMap[TestClientCommand | None.type]] = Array.fill(clusterSize)(mutable.LongMap.withDefault(_ => None))

			createsAndInitializesNodes(net, weakReferencesHolder) { node =>
				node.statesChangesListener = new NodeStateChangesListener() {

					override def onCommandApplied(command: node.ClientCommand, index: RecordIndex): Unit = {
						assert(node.sequencer.isInSequence)
						val commandsAppliedToThisNode = appliedCommandsByNodeIndex(net.indexOf(node.myId))
						val previouslyAppliedCommand = commandsAppliedToThisNode(index.toInt)
						if previouslyAppliedCommand ne None then promise.tryFailure(new AssertionError(s"Two commands with same index were applied to the node ${node.myId}: previous=$previouslyAppliedCommand, new=$command"))
						var i = index.toInt
						while i < numberOfCommandsToSend do {
							i += 1
							if commandsAppliedToThisNode(i) ne None then promise.tryFailure(new AssertionError(s"The command $command was applied with index $index which is less than the index $i of the previously applied command ${commandsAppliedToThisNode(i)}."))
						}
						commandsAppliedToThisNode(index.toInt) = command

						for nodeIndex <- 0 until clusterSize do {
							val otherNode = net.getNode(nodeIndex)
							if otherNode ne node then {
								otherNode.sequencer.execute {
									val commandsAppliedToTheOtherNode = appliedCommandsByNodeIndex(net.indexOf(otherNode.myId))
									val commandAppliedToTheOtherNodeAtIndex = commandsAppliedToTheOtherNode(index.toInt)
									if (commandAppliedToTheOtherNodeAtIndex ne None) && commandAppliedToTheOtherNodeAtIndex != command then
										promise.tryFailure(new AssertionError(s"The node ${node.myId} applied the command $command at index $index, which is different from the command $commandAppliedToTheOtherNodeAtIndex applied at the same index in node ${otherNode.myId}."))

								}

							}
						}
					}
				}
				node.DefaultNotificationListener()
			}

			val checks: net.netSequencer.Task[Unit] = for {
				_ <- net.startsAllNodes.toTask
				client = Client[net.type]("A", net, startWithHighestPriorityParticipant)
				_ <- client.sendsCommandsUntil(commandIndex => commandIndex > numberOfCommandsToSend || promise.isCompleted)
			} yield promise.tryComplete(Success(()))

			checks.trigger(false)(promise.tryComplete)
			promise.future.andThen(_ => net.stop())
		}
	}
}