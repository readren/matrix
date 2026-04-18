package readren.consensus

import ConsensusParticipantSdm.*

import munit.ScalaCheckEffectSuite
import org.scalacheck.Gen
import org.scalacheck.Test.Parameters
import org.scalacheck.effect.PropF
import readren.common.{Maybe, ScribeConfig}
import readren.sequencer.{Doer, MilliDuration}
import readren.sequencer.providers.CooperativeWorkersWithSyncSchedulerDp
import scribe.modify.LogModifier
import scribe.throwable.TraceLoggableMessage
import scribe.{LogRecord, Priority}

import java.util.concurrent.{Executors, TimeUnit}
import scala.collection.immutable.ListSet
import scala.collection.{mutable, IndexedSeq as GenIndexedSeq}
import scala.compiletime.uninitialized
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.reflect.ClassTag
import scala.util.{Failure, Random, Success, Try}




/** A comprehensive test suite for the `ConsensusParticipantSdm` module, designed to verify the correctness and robustness of the distributed consensus algorithm. It uses `munit.ScalaCheckEffectSuite` and `ScalaCheck` for property-based testing, allowing for a wide range of scenarios to be tested with varying parameters.
 * In essence, `ConsensusParticipantSdmTest` provides a robust and well-structured approach to testing a complex distributed consensus algorithm by simulating a network environment, actively injecting faults and dynamic changes, and continuously verifying critical invariants that ensure correctness and safety.
 * */
class ConsensusParticipantSdmTest extends ScalaCheckEffectSuite {

	ScribeConfig.init(deleteLogFilesOnLaunch = true, modifiers = List(new LogModifier {
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

	/** Used by the main promise of each test only. Does not introduce randomness to the tests. */
	private given ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(4))

	type TickTime = Int
	type TickDuration = Int

	/** Id of a [[Node]]. Implements [[ConsensusParticipantSdm.ParticipantId]]. */
	private type Id = String

	// Test command type
	private case class TestClientCommand(value: Int, clientId: String)

	/** The provider of all the [[Doer]] instances used by this testing infrastructure. */
	private val sharedDap = new CooperativeWorkersWithSyncSchedulerDp.Impl(
		failureReporter = (doer, e) =>
			scribe.error(s"Failure reported by a task executed by the sequencer tagged with ${doer.tag}", e),
		unhandledExceptionReporter = (doer, e) =>
			scribe.error(s"Unhandled exception in a task executed by the sequencer tagged with ${doer.tag}", e)
	)

	private type ScheduSequen = CooperativeWorkersWithSyncSchedulerDp.SchedulingDoerFacade

	/** Simulates the network environment in which the consensus participants operate.
	 * @param clusterSize the total number of [[Node]] instances involved.
	 * @param randomnessSeed the seed for the pseud-randomness of the messages fate.
	 * @param requestFailurePercentage Probability (as a percentage) that a request message fails to reach its target [[Node]].
	 * @param responseFailurePercentage Probability (as a percentage) that a response message—sent in reply to a successfully delivered request—fails to reach the originating [[Node]].
	 * @param stimulusSettlingTime duration (in milliseconds) allotted for the system to settle after a stimulus (i.e., message delivery), before releasing the next traveling message.
	 *	This pause ensures that all [[Node]]s complete their internal processing and inter-node communication, preserving test determinism.
	 *	Note: Test execution time scales with this value, so avoid setting it excessively high.
	 * @param enqueueThresholdForEarlyDelivery Threshold for the number of traveling messages enqueued before one is delivered, even if the [[stimulusSettlingTime]] has not yet elapsed.
	 * 	When this threshold is reached, a single message is selected pseudo-randomly and delivered, allowing the system to progress without waiting for full settling.
	 *	This accelerates test execution while reducing delivery reordering. */
	private class Net(
		val clusterSize: Int,
		val randomnessSeed: Long = 0,
		requestFailurePercentage: Int = 10,
		responseFailurePercentage: Int = 10,
		configChangeBeforeRequestDelivered_probability: Float = 0.05,
		configChangeBeforeResponseDelivered_probability: Float = 0.05,
		configChangeAfterResponseDelivered_probability: Float = 0.05,
		stimulusSettlingTime: Int = 5,
		enqueueThresholdForEarlyDelivery: Int = Int.MaxValue
	) {
		val nodesIds: IndexedSeq[Id] = (0 until clusterSize).map(i => s"p-$i")

		/** Threshold for the number of traveling messages enqueued before one is delivered, even if the [[stimulusSettlingTime]] has not yet elapsed.
		 * When this threshold is reached, a single message is selected pseudo-randomly and delivered, allowing the system to progress without waiting for full settling.
		 * This accelerates test execution while reducing delivery reordering.
		 */

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
				node.startsIfNotRunning(0, ListSet.empty).onBehalfOf(netSequencer)
			}
			netSequencer.Duty_sequenceToArray(starters)
		}

		/** Stops all the running [[Node]]s and clears all [[Channel]]s used to simulate the TCP communication between them. */
		def stop(): Unit = {
			val nodesStoppers = for i <- 0 until clusterSize yield {
				val node = getNode(i)
				val stopsNode = node.sequencer.Duty_mineFlat(() => if node.isDown then node.sequencer.Duty_unit else node.participant.quiesces)
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

		//// Deterministic clock ////

		/** A clock whose ticks are triggered by the delivery of messages, or by an explicit call to [[tick]].
		 * When inactivity timeout occurs, the clock is advanced to the earliest still not consumed scheduled TickTime.
		 */
		object clock {
			private var tickTime: TickTime = 0
			private var maybeInactivityTimer: Maybe[netSequencer.Schedule] = Maybe.empty

			private val observers: mutable.SortedMap[TickTime, List[() => Unit]] = mutable.SortedMap.empty

			def tick(): Unit = {
				// scribe.trace(s"Net: tick $tickTime")
				this.tickTime += 1
				consume(tickTime)

				// Reset the inactivity timeout and schedule a new one.
				maybeInactivityTimer.foreach(netSequencer.cancel)
				armInactivityTimer()
			}

			/** TODO consider avoiding timers completely. How? Instead of using a [[DoerProvider]] with the [[SchedulingExtesion]], use a vanilla [[CooperativeWorkersDp]] and override [[CooperativeWorkersDp.determineWaitDurationFor]] to wait nothing and increase to [[tickTime]] when all the workers are idle and [[observers.nonEmpty]]. Or, why not, implement a variant of scheduling doer provider that does the same thing in a reusable manner (would define a required interface named Clock with a method named tick) */
			private def armInactivityTimer(): Unit = {
				// scribe.trace(s"Net: armInactivityTimer")
				observers.headOption match {
					case Some(entry) =>
						val schedule = netSequencer.newDelaySchedule(stimulusSettlingTime)
						maybeInactivityTimer = Maybe.some(schedule)
						netSequencer.schedule(schedule) { _ =>
							// When inactivity timeout occurs, advance the clock to the earliest TickTime among the still not consumed subscriptions.
							tickTime = entry._1
							consume(tickTime)
							armInactivityTimer()
						}
					case None =>
						maybeInactivityTimer = Maybe.empty
				}
			}

			private def consume(tickTime: TickTime): Unit = {
				// scribe.trace(s"Net: consume($tickTime)")
				observers.headOption match {
					case Some(firstEntry) if firstEntry._1 == tickTime =>
						observers.remove(tickTime)
						firstEntry._2.reverse.foreach(_.apply())
					case _ =>
				}
			}

			def schedule(tickDuration: TickDuration)(onElapsed: () => Unit): Unit = {
				// scribe.trace(s"Net: schedule($tickDuration), tickTime=$tickTime, observers=${observers.size}")
				assert(netSequencer.isInSequence)
				if tickDuration <= 0 then onElapsed()
				else {
					observers.updateWith(tickTime + tickDuration) {
						case Some(list) => Some(onElapsed :: list)
						case None => Some(onElapsed :: Nil)
					}
				}
				if maybeInactivityTimer.isEmpty then armInactivityTimer()
			}
		}

		//// The following members implement the communication between nodes of this net.

		private type RequestId = (global: Int, channel: Int)

		/** Represents a communication channel between two [[Node]]s, managing message queues and failure states.
		 * Mimics a TCP channel by maintaining delivery order. */
		private case class Channel() {
			private val queue: mutable.Queue[netSequencer.Duty[Unit]] = mutable.Queue.empty
			private var lastRequestId = 0
			private var failingUntil: Int = 0

			inline def nextRequestId: RequestId = {
				assert(netSequencer.isInSequence)
				lastRequestId += 1
				lastGlobalRequestId += 1
				clock.tick()
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
			/** Provides a simulated Remote Procedure Call (RPC) mechanism between nodes, handling message queuing, delivery order (mimicking TCP), and the simulated failures.
			 * Performs a Remote Procedure Call from a [[Node]] of this [[Net]] (the inquirer) to another [[Node]] of this [[Net]] (the replier).
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
					netSequencer.run {
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

					netSequencer.Task_fromDuty(
						netSequencer.Duty_mineFlat { () =>
							covenant.map {
								case (response, requestId) =>
									// TODO consider moving this to the line after calling `covenant.fulfill` (which would avoid the need to pass the requestId) and also consider using a commitment instead.
									scribe.trace(s"$inquirerId <- $replierId: $requestId:$response, $numberOfTravelingMessages messages on the way")
									response
							}
						}
					)

				} else {
					/// Simple implementation that always succeeds and adds no randomness
					val replierNode = getNode(replierId)
					replierNode.sequencer.Duty_mineFlat[R] { () =>
						call(replierNode)
					}.onBehalfOf(netSequencer).succeed
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

		private var isConfigNoiseEnabled: Boolean = true

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
		private var activeConfigChange: ConfigChange[Id] = TransitionalConfigChange(PRE_INIT, "", Set.empty, nodesIncludedIn(initialConfigMask))
		private var indexOfActiveConfigChange: RecordIndex = 0

		/** Should be called when the client simulator sends a command to a consensus-participant, before it is received.
		 * Needed to allow the [[Net]] to count the number of commands sent, which is required to adjust the configuration noise probability. */
		def onBeforeClientCommandSent(): Unit = {
			numberOfConfigNoiseInjectionsBetweenThePreviousTwoClientCommands = configNoiseInjectionsSinceLastClientCommand_count
			configNoiseInjectionsSinceLastClientCommand_count = 0
			commandsSentByClients_count += 1
		}

		/** Introduces a configuration change during test execution based on the provided probability. */
		private def injectConfigurationNoise(changeProbability: Float): Unit = {
			assert(netSequencer.isInSequence)
			if isConfigNoiseEnabled then {
				configNoiseInjection_count += 1
				configNoiseInjectionsSinceLastClientCommand_count += 1
				determineNewConfig(changeProbability).foreach { case (previousConfigMask, newConfigMask) =>
					val configChangeRequest = createNewConfigChangeRequestId()

					// trigger all those duties in their respective node's sequencer
					for configChangeReplies <- sendsConfigChangeRequests(configChangeRequest, nodesIncludedIn(newConfigMask)) do {
						scribe.info(s"Net: the configuration change request #$configChangeRequest sent to each node completed with: ${configChangeReplies.mkString("[", ", ", "]")}.")
					}

				}
			}
		}

		private def determineNewConfig(changeProbabilityBetweenClientCommands: Float): Maybe[(previousConfig: ConfigMask, newConfig: ConfigMask)] = {
			assert(netSequencer.isInSequence)
			val numberOfConfigNoiseInjectionsPerClientCommand = configNoiseInjection_count / commandsSentByClients_count
			val adjustedProbability = changeProbabilityBetweenClientCommands / numberOfConfigNoiseInjectionsPerClientCommand
			if random.nextFloat() >= adjustedProbability then Maybe.empty
			else {
				val newConfigMask = IArray.unsafeFromArray(Array.fill[Boolean](clusterSize)(random.nextBoolean()))
				if newConfigMask.contains(true) then {
					val oldConfigMask = lastProposedConfigMask
					lastProposedConfigMask = newConfigMask
					Maybe((oldConfigMask, newConfigMask))
				} else Maybe.empty
			}
		}

		private def createNewConfigChangeRequestId(): String = {
			configChangeRequestSequencer += 1
			s"ccReq-$configChangeRequestSequencer"
		}

		/** @return a [[netSequencer.Duty]] that sends a configuration change request to each [[Node]] of this [[Net]]. */
		private def sendsConfigChangeRequests(configChangeRequest: String, includedParticipants: ListSet[Id]): netSequencer.Duty[Array[ConfigChangeResponse]] = {
			scribe.info(s"Net: About to request (#$configChangeRequest) a configuration change to $includedParticipants")
			// For each node, create a duty that request a configuration change to the same newConfig.
			val configChangeRequestingDutyByNodeIndex =
				for node <- nodeByIndex yield netSequencer.Duty_foreign(node.sequencer)(
					node.sequencer.Duty_mineFlat(() =>
						node.clusterParticipant.delegate.requestConfigChange(configChangeRequest, includedParticipants, Maybe.empty) // TODO replace the last argument with the incorrect prior response.
					)
				)
			netSequencer.Duty_sequenceToArray(configChangeRequestingDutyByNodeIndex)
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

		//// Consensus services lifecycle management ////

		def onActiveConfigChanged(change: ConfigChange[Id], changeIndex: RecordIndex): Unit = {
			netSequencer.run {
				scribe.trace(s"Net: onActiveConfigChanged($change, index=$changeIndex) was called") // when readyToRetireParticipants=$readyToRetireParticipants, quiescedParticipants=$quiescedParticipants ")
				activeConfigChange = change
				indexOfActiveConfigChange = changeIndex
				change match {
					case tcc: TransitionalConfigChange[Id] =>
						for nodeIndex <- 0 until clusterSize do {
							val node = this.getNode(nodeIndex)
							if tcc.newParticipants.contains(node.myId) && !tcc.oldParticipants.contains(node.myId) then {
								val participantsInTheTcc = ListSet.newBuilder.addAll(tcc.oldParticipants).addAll(tcc.newParticipants).result()
								node.startsIfNotRunning(changeIndex, participantsInTheTcc).triggerAndForget(false)
							}
						}
					case _ => // Do nothing.
				}
			}
		}

		def onNodeQuiesced(node: Node): Unit = {
			netSequencer.run {
				scribe.trace(s"Net: onNodeQuiesced(${node.myId}) was called") // when indexOfActiveConfigChange=$indexOfActiveConfigChange, readyToRetireParticipants=$readyToRetireParticipants, quiescedParticipants=$quiescedParticipants ")
				if activeConfigChange.isActive(node.myId) then {
					val participantsInActiveConfigChange = ListSet.newBuilder.addAll(activeConfigChange.oldParticipants).addAll(activeConfigChange.newParticipants).result()
					node.startsIfNotRunning(indexOfActiveConfigChange, participantsInActiveConfigChange).triggerAndForget(false)
				}
			}
		}


		/**
		 * Attempts to gracefully shut down the network by repeatedly requesting a configuration change to an empty set of participants.
		 * In order to give the network time to process the request and handle any ongoing communication deterministically,
		 * it waits for the network to "settle" between failed attempts.
		 * The test considers the network settled when either a maximum number of messages have been dispatched (`maxTotalIncrements`)
		 * or `stimulusSettlingTime` has passed without any new messages being dispatched.
		 *
		 * @param maxAttempts The maximum number of configuration change requests before failing the shutdown process.
		 * @param durationBetweenAttempts The duration to wait between attempts to shut down.
		 * @return a [[netSequencer.Duty]] yielding [[Maybe.empty]] on success, or a message detailing the failure if `maxAttempts` is reached.
		 */
		def shutsDownGracefully(maxAttempts: Int, durationBetweenAttempts: TickDuration = 9): netSequencer.Duty[Maybe[String]] = {
			netSequencer.checkWithin()

			def loop(failedAttempts: Int): netSequencer.Duty[Maybe[String]] = {
				if failedAttempts == maxAttempts then netSequencer.Duty_ready(Maybe(s"Net: graceful shutdown failed after $maxAttempts attempts"))
				else {
					val configChangeRequestId = createNewConfigChangeRequestId()
					for {
						responses <- sendsConfigChangeRequests(configChangeRequestId, ListSet.empty)
						maybeErrorMessage <- {
							if responses.exists { response => response.isInstanceOf[SUCCESSFULLY_CHANGED] || response.isInstanceOf[ALREADY_CHANGED] }
								|| responses.forall { response => response.isInstanceOf[STOPPED] }
							then {
								scribe.trace(s"Net: Graceful shutdown of the net completed with: ${responses.mkString("[", ", ", "]")}")
								netSequencer.Duty_ready(Maybe.empty)
							}
							else {
								val attemptNumber = failedAttempts + 1
								scribe.trace(s"Net: Attempt #$attemptNumber to shutdown the net failed with ${responses.mkString("[", ", ", "]")}")

								val covenant = netSequencer.Covenant[Maybe[String]]()
								clock.schedule(durationBetweenAttempts) { () =>
									covenant.fulfillWith(loop(failedAttempts + 1), true)
								}
								covenant
							}
						}
					} yield maybeErrorMessage
				}
			}

			isConfigNoiseEnabled = false
			loop(0)
		}
	}

	/**
	 * Simulates a client interacting with the consensus cluster.
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
		 * @return A task that completes with [[Maybe.empty]] if the command was processed; a [[Maybe]] containing the next attempt flag if the command was not processed despite all nodes were tried or the net is empty. */
		private def sendsCommand(commandPayload: Int, attemptFlag: CommandAttemptFlag): net.netSequencer.Duty[Maybe[CommandAttemptFlag]] = {
			if knownParticipants.isEmpty then return net.netSequencer.Duty_ready(Maybe(attemptFlag))
			val receiverNode = targetParticipant
			scribe.info(s"Client: Sent command:$commandPayload, attemptFlag:$attemptFlag, to:${receiverNode.myId}")
			net.onBeforeClientCommandSent()

			def retry(nextAttemptFlag: CommandAttemptFlag): net.netSequencer.Duty[Maybe[CommandAttemptFlag]] = {
				knownParticipants.find(p => !alreadyTriedParticipants.contains(p)).fold {
					alreadyTriedParticipants.clear()
					net.netSequencer.Duty_ready(Maybe(nextAttemptFlag))
				} { chosen =>
					targetParticipant = net.getNode(chosen)
					sendsCommand(commandPayload, nextAttemptFlag)
				}
			}

			net.netSequencer.Duty_foreign(receiverNode.sequencer)(receiverNode.sequencer.Duty_mineFlat { () =>
				receiverNode.clusterParticipant.delegate.onCommandFromClient(TestClientCommand(commandPayload, clientId), attemptFlag)
			}).flatMap {
				case receiverNode.Processed(index, content) =>
					scribe.info(s"Client: command `$commandPayload` was processed @$index by ${receiverNode.myId} which replied with `$content`.")
					alreadyTriedParticipants.clear()
					net.netSequencer.Duty_ready(Maybe.empty)
				case receiverNode.RedirectTo(leaderId) =>
					scribe.info(s"Client: the follower ${receiverNode.myId} redirected the command `$commandPayload` to the leader $leaderId.")
					targetParticipant = net.getNode(leaderId)
					// if despite the attempt flag sent to the participant was FALLBACK (which instructs to update the role before responding) it responds with a redirection to an already tried participant, add it to the already tried ones.
					if attemptFlag == FALLBACK && alreadyTriedParticipants.contains(leaderId) then alreadyTriedParticipants.addOne(targetParticipant.myId)
					sendsCommand(commandPayload, REDIRECTED)
				case receiverNode.Unable(nextAttemptFlag, otherParticipants) =>
					knownParticipants = otherParticipants + receiverNode.myId
					alreadyTriedParticipants.addOne(targetParticipant.myId)
					scribe.info(s"Client: the participant ${receiverNode.myId} is unable to process the command `$commandPayload`, nextAttemptFlag=$nextAttemptFlag, otherParticipants=$otherParticipants, knowParticipants=$knownParticipants, alreadyTriedParticipants=$alreadyTriedParticipants")
					retry(nextAttemptFlag)
				case receiverNode.InconsistentState(m) =>
					scribe.info(s"Client: the participant ${receiverNode.myId} internal state become inconsistent. Detected when the command `$commandPayload` was processed.")
					throw new NotImplementedError()
				case stale: receiverNode.Stale =>
					scribe.info(s"Client: command `${stale.command}` is stale. The last received command at that moment was at index @${stale.lastCommandIndex}. Received by ${receiverNode.myId}.")
					alreadyTriedParticipants.clear()
					net.netSequencer.Duty_ready(Maybe.empty)
				case superseded: receiverNode.Superseded =>
					scribe.info(s"Client: command `${superseded.command}` is superseded. The last received command at that moment was at index @${superseded.lastCommandIndex}. Received by ${receiverNode.myId}.")
					alreadyTriedParticipants.clear()
					net.netSequencer.Duty_ready(Maybe.empty)
				case tooOld: receiverNode.TooOld =>
					scribe.info(s"Client: command `${tooOld.command}` is too old. Received by ${receiverNode.myId}.")
					alreadyTriedParticipants.clear()
					net.netSequencer.Duty_ready(Maybe.empty)
			}
		}

		def sendsCommandsUntil(predicate: (commandIndex: Int) => Boolean, maxRetries: Int = 9): net.netSequencer.Duty[Maybe[String]] = {

			def sendCommandLoop(commandIndex: Int, attemptsCounter: Int, attemptFlag: CommandAttemptFlag): net.netSequencer.Duty[Maybe[String]] = {
				if predicate.apply(commandIndex) then net.netSequencer.Duty_ready(Maybe.empty)
				else if attemptsCounter > maxRetries then net.netSequencer.Duty_ready(Maybe("The cluster got stuck unable to progress"))
				else for {
					wasProcessed <- sendsCommand(commandIndex, attemptFlag)
					maybeError <- {
						wasProcessed.fold(sendCommandLoop(commandIndex + 1, 0, attemptFlag)) { nextAttemptFlag =>
							scribe.info(s"Client: The command $commandIndex was tried with all the participants. Retrying all again. Attempts done so far: ${attemptsCounter + 1}.")
							sendCommandLoop(commandIndex, attemptsCounter + 1, nextAttemptFlag)
						}
					}
				} yield maybeError
			}

			net.netSequencer.Duty_mineFlat(() =>
				sendCommandLoop(1, 0, FIRST_ATTEMPT)
			)
		}

	}

	/** A callback interface to observe and react to internal state changes within a `Node` (log appends, overwrites, command applications). */
	private trait NodeStateChangesListener {
		/** Called when any entry in the log buffer of a [[Node]] is overwritten.
		 * @param index the [[RecordIndex]] of the first overwritten entry.
		 * @param firstReplacedRecord the first stored [[Record]] that is removed.
		 * @param firstReplacingRecord the [[Record]] with which the first overwritten log entry is replaced with. */
		def onLogOverwrite(index: RecordIndex, firstReplacedRecord: Record, firstReplacingRecord: Record): Unit = ()

		/** Called when a [[Record]] is appended to the log buffer. */
		def onRecordAppended(record: Record, index: RecordIndex): Unit = ()

		def onCommandApplied(command: TestClientCommand, index: RecordIndex): Unit = ()
	}

	/**
	 * An implementation of the [[ConsensusParticipantSdm]] for testing.
	 */
	private class Node(val myId: Id, initialParticipants: Set[Id], net: Net, retirementDriveRetryPeriod: TickDuration, unreachableFollowersRetryPeriod: TickDuration, quiescenceAuthorizationRetryPeriod: TickDuration) extends ConsensusParticipantSdm { thisNode =>

		import net.rpc

		override type ParticipantId = Id
		override type ClientCommand = TestClientCommand
		override type StateMachineResponse = Int
		override type ClientId = String

		override val sequencer: ScheduSequen = sharedDap.provide(s"node-sequencer-$myId")



		var statesChangesListener: NodeStateChangesListener = new NodeStateChangesListener() {
			override def onLogOverwrite(index: RecordIndex, firstReplacedRecord: Record, firstReplacingRecord: Record): Unit = ()
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
		def startsIfNotRunning(indexOfTheIncludingConfigChange: RecordIndex, participantsInTheIncludingConfigChange: ListSet[ParticipantId]): sequencer.Duty[Unit] = {
			sequencer.Duty_mine { () =>
				if isDown || participant.getRoleOrdinal == QUIESCED then {
					scribe.info(s"node-$myId: about to create the consensus participant service due to the configuration change at $indexOfTheIncludingConfigChange")
					_participant = ConsensusParticipant(clusterParticipant, storage, machine, indexOfTheIncludingConfigChange, participantsInTheIncludingConfigChange, List(initialNotificationListener, notificationScribe))
				} else scribe.info(s"node-$myId: service creation skipped because it is already running with role ${participant.getRoleOrdinal}.")
			}
		}

		def release(): Unit = {
			sequencer.run {
				_participant = null
				scribe.trace(s"node-$myId: consensus service was released")
			}
		}

		/** A simple [[StateMachine]] implementation that tracks applied commands. */
		object machine extends StateMachine {
			private val appliedCommands: mutable.Map[RecordIndex, ClientCommand] = mutable.LongMap.empty
			var highestAppliedIndex: RecordIndex = 0

			override def applyClientCommand(index: RecordIndex, command: ClientCommand): sequencer.LatchingDuty[StateMachineResponse] = {
				sequencer.checkWithin()

				if index <= highestAppliedIndex then assert(appliedCommands.get(index).contains(command))
				else {
					highestAppliedIndex = index
					appliedCommands.put(index, command)
					statesChangesListener.onCommandApplied(command, index)
				}


				// TODO add delay
				sequencer.LatchingDuty_ready(command.value)
			}

			override def recoverIndexOfLastAppliedCommand: sequencer.LatchingDuty[RecordIndex] = {
				sequencer.checkWithin()
				sequencer.LatchingDuty_ready(0)
			}
		}

		override def isEager: Boolean = false

		/**
		 * Test instance and implementation of the [[ClusterParticipant]] service interface required by the [[participant]] (the [[ConsensusParticipant]] service corresponding to a [[Node]]).
		 */
		object clusterParticipant extends ClusterParticipant {
			override val boundParticipantId: ParticipantId = myId
			private val currentParticipants: Set[ParticipantId] = initialParticipants

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

			override def notifyActiveConfigChanged(change: ConfigChange[ParticipantId], changeIndex: RecordIndex, roleOrdinal: RoleOrdinal): Unit = {
				sequencer.checkWithin()
				scribe.info(s"cluster-$boundParticipantId: onConfigurationChanged($change, index=$changeIndex, ${RoleOrdinal_nameOf(roleOrdinal)}) called.")
				if roleOrdinal == LEADER then net.onActiveConfigChanged(change, changeIndex)
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

				override def permitQuiescence(indexOfGrantedStableConfigChange: RecordIndex): sequencer.Task[Unit] = {
					sequencer.checkWithin()
					boundParticipantId.rpc[Unit](
						replierId,
						s"PermitQuiesce($indexOfGrantedStableConfigChange)"
					) { replier =>
						replier.sequencer.Duty_ready(replier.clusterParticipant.delegate.onQuiescencePermitted(boundParticipantId, indexOfGrantedStableConfigChange))
					}.onBehalfOf(sequencer)
				}
			}

			override def getOtherProbableParticipant: ListSet[ParticipantId] = ListSet.from(net.nodesIds) - myId

			override def notifyQuiesced(motive: Try[String]): Unit = {
				scribe.info(s"cluster-$myId: `notifyQuiesced` was called with motive=$motive")
				net.onNodeQuiesced(thisNode)
			}

			override def requestWakeUp(reason: WakeUpReason, callback: () => Unit): WakeUpToken = {
				sequencer.checkWithin()
				object token extends WakeUpToken, (() => Unit) {
					var isCanceled: Boolean = false

					override def cancel(): Unit = {
						sequencer.checkWithin()
						isCanceled = true
					}

					override def apply(): Unit = if !isCanceled then sequencer.run {
						if !isCanceled then callback()
					}
				}
				val duration = reason match {
					case WakeUpReason.RetirementDriveRetry => retirementDriveRetryPeriod
					case WakeUpReason.UnreachableFollowersRetry => unreachableFollowersRetryPeriod
					case WakeUpReason.QuiescenceAuthorizationRetry => quiescenceAuthorizationRetryPeriod
				}
				net.netSequencer.run {
					net.clock.schedule(duration)(token)
				}
				token
			}
		}

		/**
		 * Test instance and implementation of the [[Storage]] service interface required by the [[participant]] (the [[ConsensusParticipant]] service corresponding to a [[Node]]).
		 * Uses an in-memory [[TestWorkspace]] for storing the participant's log and state.
		 */
		object storage extends Storage {
			private[ConsensusParticipantSdmTest] var memory: WS = TestWorkspace()

			override def load: sequencer.LatchingDuty[Try[WS]] = {
				sequencer.checkWithin()
				sequencer.LatchingDuty_ready(Success(memory))
			} // TODO add a delay

			override def save(workspace: WS): sequencer.LatchingDuty[Try[Unit]] = {
				sequencer.checkWithin()
				memory = workspace
				sequencer.LatchingDuty_ready(Doer.successUnit) // TODO add a delay
			}
		}

		override type WS = TestWorkspace

		/**
		 * Test implementation of [[Workspace]]
		 */
		class TestWorkspace extends Workspace {
			private var currentTerm: Term = PRE_INIT
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
					statesChangesListener.onLogOverwrite(storedIndex + logBufferOffset, logBuffer(storedIndex), records(newIndex))
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
			override def onStarting(previous: RoleOrdinal, indexOfTheIncludingConfigChange: RecordIndex): Unit = {
				sequencer.checkWithin()
				scribe.info(s"scribe-$myId: is starting ${if indexOfTheIncludingConfigChange == 0 then "as seed" else s"to join due to a transitional-configuration-change at $indexOfTheIncludingConfigChange"}.")
			}

			override def onStarted(previous: RoleOrdinal, term: Term, initialConfigChange: ConfigChange[ParticipantId], isSeed: Boolean): Unit = {
				sequencer.checkWithin()
				scribe.info(s"scribe-$myId: completed the start-up with: previousRole=$previous, term=$term, initialConfigChange=$initialConfigChange, isSeed=$isSeed")
			}

			override def onBecameQuiesced(previous: RoleOrdinal, term: Term, motive: Try[String]): Unit = {
				sequencer.checkWithin()
				scribe.info(s"scribe-$myId: became quiesced from ${RoleOrdinal_nameOf(previous)} during term $term because $motive.")
			}

			override def onJoining(previous: RoleOrdinal, indexOfTheIncludingConfigChange: RecordIndex): Unit = {
				sequencer.checkWithin()
				scribe.info(s"scribe-$myId: is catching-up to join due to a transitional-configuration-change at $indexOfTheIncludingConfigChange.")
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

			override def onRetiring(previous: RoleOrdinal, term: Term): Unit = {
				sequencer.checkWithin()
				scribe.info(s"scribe-$myId: became retiree from ${RoleOrdinal_nameOf(previous)} at @$term.")
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
	private def createAndInitializeNodes[N <: Net](
		net: N,
		weakReferencesHolder: mutable.Buffer[AnyRef],
		retirementDriveRetryPeriod: TickDuration = 10,
		unreachableFollowersRetryPeriod: TickDuration = 10,
		quiescenceAuthorizationRetryPeriod: TickDuration = 10
	)(notificationListenerBuilder: (node: Node) => node.NotificationListener): Unit = {

		val initialParticipants = net.nodesIncludedIn(net.initialConfigMask)
		for id <- net.nodesIds do {
			val node = Node(id, initialParticipants, net, retirementDriveRetryPeriod, unreachableFollowersRetryPeriod, quiescenceAuthorizationRetryPeriod)
			net.addNode(node)
			val nl = notificationListenerBuilder(node)
			weakReferencesHolder.addOne(nl)
			node.initialize(nl)
		}
	}

	/** This is the core logic for verifying the correctness of the consensus algorithm. It sets up a simulated environment and then actively checks several critical invariants:
	 *		- Leader Append-Only**: Ensures that a leader never overwrites or deletes entries in its log, only appends.
	 *		- Log Matching**: Verifies that if two logs contain an entry with the same index and term, then the logs are identical up through that index.
	 *		- State Machine Safety**: Guarantees that if a server has applied a log entry at a given index to its state machine, no other server will ever apply a different log entry for the same index.
	 *		- Election Safety**: Asserts that at most one leader can be elected in a given term.
	 *		- Leader Completeness**: Confirms that if a log entry is committed in a given term, then that entry will be present in the logs of the leaders for all higher-numbered terms.
	 *
	 *   These invariants are checked using the `NodeStateChangesListener` and by comparing the state across multiple simulated nodes. If any invariant is violated, the test fails immediately via a `Promise.tryFailure`. */
	private def testAllInvariants(net: Net,
		startWithHighestPriorityParticipant: Boolean,
		numberOfCommandsToSend: Int = 20,
		commandRetries: Int = 20,
		retirementDriveRetryPeriod: TickDuration = 10,
		unreachableFollowersRetryPeriod: TickDuration = 10,
		quiescenceAuthorizationRetryPeriod: TickDuration = 10
	): Future[Unit] = {
		val promise = Promise[Unit]()
		val clusterSize = net.clusterSize
		val weakReferencesHolder = mutable.Buffer.empty[AnyRef]
		val leaderNodeByTerm: mutable.Buffer[Node] = mutable.Buffer.empty
		val committedRecordsByNodeIndex: Array[mutable.Buffer[Record]] = Array.fill(clusterSize)(mutable.Buffer.empty)
		val appliedCommandsByNodeIndex: Array[mutable.LongMap[TestClientCommand | None.type]] = Array.fill(clusterSize)(mutable.LongMap.withDefault(_ => None))

		createAndInitializeNodes(net, weakReferencesHolder) { node =>
			node.statesChangesListener = new NodeStateChangesListener() {
				override def onLogOverwrite(index: RecordIndex, firstReplacedRecord: Record, firstReplacingRecord: Record): Unit = {
					// Defer the check to let the updating execution to complete the atomic changes.
					node.sequencer.run {
						// "Leader Append-Only: a leader never overwrites or deletes entries in its log; it only appends new entries. §5.3"
						if node.participant.getRoleOrdinal == LEADER then promise.tryFailure(new AssertionError(s"The participant ${node.myId} broke the \"append only rule\" at index $index. Removed record: $firstReplacedRecord, replacing record: $firstReplacingRecord."))
					}
				}

				override def onRecordAppended(record: Record, index: RecordIndex): Unit = {
					// Log Matching: if two logs contain an entry with the same index and term, then the logs are identical in all entries up through the given index. §5.3
					val thisNodeRecords = node.storage.memory.getRecordsBetween(1, index)

					for nodeIndex <- 0 until clusterSize do {
						val otherNode = net.getNode(nodeIndex)
						if otherNode ne node then {
							otherNode.sequencer.run {
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
							otherNode.sequencer.run {
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
						otherNode.sequencer.run {
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

		for {
			_ <- net.startsAllNodes
			client = Client[net.type]("A", net, startWithHighestPriorityParticipant)
			maybeCommandErrorMsg <- client.sendsCommandsUntil(commandIndex => commandIndex > numberOfCommandsToSend || promise.isCompleted, commandRetries)
			maybeErrorMsg <- maybeCommandErrorMsg.fold {
				net.shutsDownGracefully(commandRetries)
			} { errorMsg =>
				net.netSequencer.Duty_ready(Maybe(errorMsg))
			}
		} do maybeErrorMsg.fold(promise.tryComplete(Success(())))(errorMsg => promise.tryFailure(new AssertionError(errorMsg)))

		promise.future.andThen { tr =>
			scribe.trace(s"**** TEST COMPLETED! **** Stopping the Net. Result: ${tr.fold(e => s"${e.getMessage}, cause:${e.getCause.getMessage}", _ => "Success")}")
			net.stop()
		}
	}

	// A simple test to remind the user to run tests with assertions enabled (`-ea` VM option)
	test("assertions are enabled") {
		if !ConsensusParticipantSdm.assertionsEnabled then println("Enable assertions for all test to detect more bugs by adding the the -ea VM option.")
		if ConsensusParticipantSdm.assertionsEnabled then Future.successful(()) else Future.failed(new RuntimeException(""))
	}

	// A property-based test that runs many simulations with varying cluster sizes, starting participants, and random seeds, but *without* injecting configuration changes.
	test("All invariants must comply - without configuration changes noise".ignore) {
		inline val numberOfCommandsToSend = 10
		PropF.forAllNoShrinkF(
			Gen.choose(2, 7),
			Gen.oneOf(true, false),
			Gen.long
		) { (clusterSize, startWithHighestPriorityParticipant, netRandomnessSeed) =>
			val net = new Net(clusterSize, randomnessSeed = netRandomnessSeed, requestFailurePercentage = 10, responseFailurePercentage = 10, stimulusSettlingTime = 5, configChangeBeforeRequestDelivered_probability = 0, configChangeBeforeResponseDelivered_probability = 0, configChangeAfterResponseDelivered_probability = 0)
			scribe.info(s"\n----------------\nBegin: clusterSize=$clusterSize, initialConfig=${net.initialConfigMask.mkString("[", ", ", "]")}, startWithHighestPriorityParticipant=$startWithHighestPriorityParticipant, netRandomnessSeed=$netRandomnessSeed")
			testAllInvariants(net, startWithHighestPriorityParticipant, numberOfCommandsToSend, clusterSize * 10, clusterSize * 10, clusterSize * 10)
		}
	}

	test("Previous failing cases") {
		type FailingCase = (numberOfCommandsToSend: Int, clusterSize: Int, startWithHighestPriorityParticipant: Boolean, netRandomnessSeed: Long)
		val failingCases = Seq[FailingCase](
			(30, 4, false, -1201266674536539693L), // MatchError thrown at StableConfigChange.isCoupleOf
			(30, 7, false, -6232654863579614157L), // Net: graceful shutdown failed after 20 attempts
			(30, 3, true, -2370286264465510604L), // PanicException thrown in replicateTccAndThenStartSecondPhase
			(30, 2, true, 1380848690399351272L), // The node p-1 applied the command TestClientCommand(19,A) at index 22, which is different from the command TestClientCommand(18,A) applied at the same index in node p-0.
			(30, 3, false, -2547866549608645507L), // PanicException
			(30, 3, true, -7417113718760886059L), // "Should never happen" assertion triggered
			(30, 3, false, 7259924510493798812L),
			(30, 3, true, -7726398781820803091L), // assertion failed: currentPrimaryState eq primaryStateFence.committedState
			(30, 3, false, 3592691889253758326L),
			(30, 5, false, 2968913177794423906L),
			(30, 4, false, 1456932037162721701L),
		)
		for failingCase <- failingCases do {
			val (numberOfCommandsToSend, clusterSize, startWithHighestPriorityParticipant, netRandomnessSeed) = failingCase
			val net = new Net(clusterSize, randomnessSeed = netRandomnessSeed, requestFailurePercentage = 10, responseFailurePercentage = 10, stimulusSettlingTime = 10)
			scribe.info(s"\n----------------\nBegin: clusterSize=$clusterSize, initialConfig=${net.initialConfigMask.mkString("[", ", ", "]")}, startWithHighestPriorityParticipant=$startWithHighestPriorityParticipant, netRandomnessSeed=$netRandomnessSeed")
			testAllInvariants(net, startWithHighestPriorityParticipant, numberOfCommandsToSend, clusterSize * 10, clusterSize * 10, clusterSize * 10)
		}

	}

	// A specific test run with a fixed random seed and configuration to debug or analyze particular scenarios.
	test("All invariants special case") {
		inline val numberOfCommandsToSend = 30
		val (clusterSize, startWithHighestPriorityParticipant, netRandomnessSeed) = (4, false, -1201266674536539693L)
		val net = new Net(clusterSize, randomnessSeed = netRandomnessSeed, requestFailurePercentage = 10, responseFailurePercentage = 10, stimulusSettlingTime = 10)
		scribe.info(s"\n----------------\nBegin: clusterSize=$clusterSize, initialConfig=${net.initialConfigMask.mkString("[", ", ", "]")}, startWithHighestPriorityParticipant=$startWithHighestPriorityParticipant, netRandomnessSeed=$netRandomnessSeed")
		testAllInvariants(net, startWithHighestPriorityParticipant, numberOfCommandsToSend, clusterSize * 10, clusterSize * 10, clusterSize * 10)
	}

	// A property-based test that runs many simulations with varying cluster sizes, starting participants, and random seeds.
	test("All invariants must comply") {
		inline val numberOfCommandsToSend = 30
		PropF.forAllNoShrinkF(
			Gen.choose(3, 7),
			Gen.oneOf(true, false),
			Gen.long
		) { (clusterSize, startWithHighestPriorityParticipant, netRandomnessSeed) =>
			val net = new Net(clusterSize, randomnessSeed = netRandomnessSeed, requestFailurePercentage = 10, responseFailurePercentage = 10, stimulusSettlingTime = 1)
			scribe.info(s"\n----------------\nBegin: clusterSize=$clusterSize, initialConfig=${net.initialConfigMask.mkString("[", ", ", "]")}, startWithHighestPriorityParticipant=$startWithHighestPriorityParticipant, netRandomnessSeed=$netRandomnessSeed")
			testAllInvariants(net, startWithHighestPriorityParticipant, numberOfCommandsToSend, clusterSize * 10, clusterSize * 10, clusterSize * 10)
		}
	}
}