package readren.consensus

import ConsensusParticipantSdm.{ALREADY_CHANGED, SnapshotData, WAIT_GHOST_LEADER_IS_DEMOTED, *}

import munit.ScalaCheckEffectSuite
import org.scalacheck.Gen
import org.scalacheck.Test.Parameters
import org.scalacheck.effect.PropF
import readren.common.{Maybe, ScribeConfig}
import readren.sequencer.providers.CooperativeFlatPollingSchedulerDp
import readren.sequencer.{Doer, MilliDuration, MilliTime, MonotonicClock}
import scribe.modify.LogModifier
import scribe.message.LoggableMessage
import scribe.output.TextOutput
import scribe.throwable.TraceLoggableMessage
import scribe.{LogRecord, Priority}

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.{Executors, TimeUnit}
import scala.collection.immutable.{ListMap, ListSet}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
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
			if true then {
				val mappedMessages: List[scribe.message.LoggableMessage] = record.messages.map {
					case TraceLoggableMessage(throwable) if throwable.getMessage != null && (throwable.getMessage.startsWith("Net: simulated failure") || throwable.getMessage.startsWith("Net: target node is down")) => LoggableMessage[String](TextOutput.apply)(throwable.getMessage)

					case x => x
				}
				Some(record.copy(messages = mappedMessages))
			} else {
				val filteredMessages = record.messages.filterNot {
					case TraceLoggableMessage(throwable) if throwable.getMessage != null && (throwable.getMessage.startsWith("Net: simulated failure") || throwable.getMessage.startsWith("Net: target node is down")) => true
					case _ => false
				}
				if filteredMessages.size < record.messages.size then Some(record.copy(messages = filteredMessages)) else Some(record)
			}
		}


		override def withId(id: String): LogModifier = this
	}))

	override def scalaCheckTestParameters: Parameters = super.scalaCheckTestParameters.withMinSuccessfulTests(500)

	//	override def scalaCheckInitialSeed = "mLbrswnMGqQ8czetIVPfw3Wh8rh8m-nkAjknVe4oiUE="

	override def munitTimeout: Duration = new FiniteDuration(6000, TimeUnit.SECONDS)

	/** Used by the main promise of each test only. Does not introduce randomness to the tests. */
	private given ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(4))

	// type TickTime = Int
	// type TickDuration = Int

	/** Id of a [[Node]]. Implements [[ConsensusParticipantSdm.ParticipantId]]. */
	private type Id = String


	private type ScheduSequen = CooperativeFlatPollingSchedulerDp.SchedulingDoerFacade

	/** Simulates the network environment in which the consensus participants operate.
	 * @param clusterSize the total number of [[Node]] instances involved.
	 * @param randomnessSeed the seed for the pseudo-randomness of the messages fate.
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
		threadPoolSize: Int = 1,
		stimulusSettlingTime: Int = 0,
		enqueueThresholdForEarlyDelivery: Int = Int.MaxValue
	) { thisNet =>
		val nodesIds: IndexedSeq[Id] = (0 until clusterSize).map(i => s"p-$i")

		//// The following fields implement the communication between nodes of this net.
		private type RequestId = (global: Int, channel: Int)
		private val random = new Random(randomnessSeed)
		val initialConfigMask: ConfigMask = {
			val a = Array.fill(clusterSize)(random.nextBoolean())
			if a.contains(true) then IArray.unsafeFromArray(a)
			else IArray.fill(clusterSize)(true)
		}
		private val channelBySenderByReceiver: Array[Array[Channel]] = Array.fill(clusterSize, clusterSize)(Channel())
		@volatile private var numberOfTravelingMessages: Int = 0
		private var lastGlobalRequestId: Int = 0

		/** Duration (expressed as the number of requests initiated by [[Node]]s) for which communication between two nodes remains in a failure state once it begins.
		 * This value represents the square root of the intended failure duration, due to the underlying probability distribution: the actual duration is sampled as the square of a uniform random variable. */
		private var failureMaxDurationSqrt: Int = initialConfigMask.count(identity)

		/** The indices of the [[Node]]s in this [[Net]], indexed by node identifier. */
		private var indexById: Map[Id, Int] = Map.empty

		/** The [[Node]]s in this [[Net]], indexed by node index. */
		private val nodeByIndex: Array[Node | Null] = new Array(clusterSize)


		//// The following fields correspond to the mechanism that produces configuration changes. ////

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
		private var lastProposedConfigMask: ConfigMask = initialConfigMask

		/** The [[ConfigChange]] heard by the [[Node.clusterParticipant.onActiveConfigChanged]] of the leading node.
		 * CAUTION: This variable mutates nondeterministically. Where and when is it safe to reference it without introducing random noise? It is only safe to reference it if you take a static snapshot of it before initiating asynchronous operations, or during periods where all node workers are guaranteed to be quiescent. */
		private var activeConfigChange: ConfigChange[Id] = TransitionalConfigChange(PRE_INIT, "", Set.empty, nodesIncludedIn(initialConfigMask))

		/** The index of the [[ConfigChange]] heard by the [[Node.clusterParticipant.onActiveConfigChanged]] of the leading node.
		 * CAUTION: This variable mutates nondeterministically. Where and when is it safe to reference it without introducing random noise? It is only safe to reference it if you take a static snapshot of it before initiating asynchronous operations, or during periods where all node workers are guaranteed to be quiescent. */
		private var indexOfActiveConfigChange: RecordIndex = 0

		//// The Provider of Doer instances. Will produce one Doer for the Net and one for each of the nodes. ////

		/** The provider of all the [[Doer]] instances used by this testing infrastructure. */
		val doerProvider = new CooperativeFlatPollingSchedulerDp.Impl(
			threadPoolSize = threadPoolSize,
			unhandledExceptionReporter = (doer, e) => scribe.error(s"Unhandled exception an operation executed by the sequencer tagged with ${doer.tag}", e),
			clock = clock
		)

		/** All the mutable variables used by this [[Net]] instance are accessed within this [[ScheduSequen]]. */
		val netSequencer: ScheduSequen = doerProvider.provide("net-sequencer")


		//// Node Management ////

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

		def startAllNodes: netSequencer.Capture[Array[Unit]] = {
			val starters = for nodeIndex <- 0 until clusterSize yield {
				val node = getNode(nodeIndex)
				node.startIfNotRunning(0, ListSet.empty).onBehalfOf(netSequencer)
			}
			netSequencer.Capture_sequenceToArray(starters)
		}

		/** Stops all the running [[Node]]s and clears all [[Channel]]s used to simulate the TCP communication between them. */
		def stop(): netSequencer.Capture[Array[Unit]] = {
			netSequencer.Capture_defer { () =>
				val nodesStoppers = for i <- 0 until clusterSize yield {
					val node = getNode(i)
					val stopsNode = node.sequencer.Capture_defer(() => if node.isDown then node.sequencer.Capture_unit else node.participant.quiesce())
					netSequencer.Capture_from(node.sequencer)(stopsNode)
				}
				netSequencer.Capture_sequenceToArray(nodesStoppers)
					.andThen(
						{ _ =>
							numberOfTravelingMessages = 0
							for i <- 0 until clusterSize do {
								for j <- 0 until clusterSize do channelBySenderByReceiver(i)(j).clear()
							}
							doerProvider.shutdown()
						},
						error => throw new Exception(error) // TODO analyze what to do here
					)
			}
		}

		//// Deterministic clock ////

		object clock extends MonotonicClock { thisClock =>
			private type TickTime = Long
			private inline final val TICKS_PER_MILLI: 10 = 10

			private val tickTime: AtomicLong = new AtomicLong(0)
			private val suspendCallSerial: AtomicInteger = new AtomicInteger(0)

			override val InitialValue: MilliTime = 0
			override val MaxValue: MilliTime = Long.MaxValue

			override def currentTimeRoundedDown: MilliTime = tickTime.get / TICKS_PER_MILLI

			override def currentTimeRoundedUp: MilliTime = (tickTime.get + TICKS_PER_MILLI - 1) / TICKS_PER_MILLI

			def tick(): Unit = {
				tickTime.incrementAndGet()
			}

			override def suspend(lockObject: Object): Unit = {
				suspend(lockObject, 0)
			}

			/** Called from [[CooperativeFlatPollingSchedulerDp.lull]], within the [[Thread]] of the last worker (lockObject) that entered the sleep zone, when it sees all other workers are sleeping and there is a pending schedule. */
			override def suspend(lockObject: Object, duration: MilliDuration): Unit = {
				// scribe.trace(s"Net: clock.suspend($lockObject, $duration)")
				val callSerial = suspendCallSerial.incrementAndGet()
				// Brief timed wait to ensure the call was not spurious due to race conditions between workers in the sleep zone. If a new call occurs soon, ignore the previous one.
				if threadPoolSize > 1 then lockObject.wait(stimulusSettlingTime)
				if suspendCallSerial.get == callSerial then {
					// print(":") // This print is to see where the system settles.
					if netSequencer == null then lockObject.wait()
					// Block the worker thread synchronously if the system is completely settled
					else if duration == 0 && numberOfTravelingMessages == 0 then lockObject.wait()
					// Arguably, this point is reached when the system is settled (all the other workers are already sleeping, and this one intention is to follow them).
					// And arguably, the points where the system settles are deterministic with respect to the netSequencer at least. So, enqueueing a Runnable to the netSequencer here is deterministic. <-- Not true
					// Instead of suspending this, the only non-sleeping worker's thread, excite the system by queueing the following task to the netSequencer.
					else netSequencer.run {
						// Update the values of deterministic variables that track nondeterministic ones.
						onSystemSettled()
						// If messages are still traveling, dispatch one of them.
						if numberOfTravelingMessages > 0 then chooseAChannel().dispatchNext()
						// Else, if there is no message still traveling, advance the time up to the end of the suspension period. This causes the `CooperativeFlatPollingSchedulerDp.pollNextDoer` that will be called later by a worker, to poll the next scheduled task instead of entering the sleep zone again.
						else if duration > 0 then tickTime.addAndGet(duration * TICKS_PER_MILLI)
					}
				} // else print(".")
			}
		}

		private def onSystemSettled(): Unit = {
			failureMaxDurationSqrt = Math.max(1, Math.min(activeConfigChange.oldParticipants.size, activeConfigChange.newParticipants.size))
		}


		/** Represents a communication channel between two [[Node]]s, managing message queues and failure states.
		 * Mimics a TCP channel by maintaining delivery order. */
		private case class Channel() {
			private val queue: mutable.Queue[netSequencer.Task[Unit]] = mutable.Queue.empty
			private var lastRequestId = 0
			private var failingUntil: Int = 0

			def nextRequestId: RequestId = {
				assert(netSequencer.isInSequence)
				lastRequestId += 1
				lastGlobalRequestId += 1
				(lastGlobalRequestId, lastRequestId)
			}

			def enqueue(task: netSequencer.Task[Unit]): Unit = {
				assert(netSequencer.isInSequence)
				queue.enqueue(task)
				numberOfTravelingMessages += 1
				clock.tick()
			}

			def nonEmpty: Boolean = {
				assert(netSequencer.isInSequence)
				queue.nonEmpty
			}

			def dispatchNext(): Unit = {
				assert(netSequencer.isInSequence)
				numberOfTravelingMessages -= 1
				queue.dequeue().triggerAndForget(true)
			}

			def markAsFailing(durationSqrt: Int): Unit = {
				assert(netSequencer.isInSequence)
				failingUntil = lastGlobalRequestId + durationSqrt * durationSqrt
			}

			def isFailing: Boolean = {
				assert(netSequencer.isInSequence)
				lastGlobalRequestId <= failingUntil
			}

			def clear(): Unit = queue.clear()
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
			 * @param call a function that takes the replier [[Node]] and returns a `replierNode.sequencer.Task` that yields the value to be yielded by the returned [[readren.sequencer.Doer.Capture]]. The function is called within the replier's [[Node.sequencer]].
			 * @return a [[netSequencer.Capture]] that yields the value yielded by the `replierNode.sequencer.Task` returned by applying the provided function `call` to the replier [[Node]].
			 * @throws RuntimeException if this [[Net]] does not contain the [[Node]]s identified with `inquirerId` and `replierId`. */
			def rpc[R](replierId: Id, requestDescription: String)(call: (replierNode: Node) => replierNode.sequencer.Capture[R]): netSequencer.Capture[R] = {

				if true then {
					val inquirerIndex = indexOf(inquirerId)
					val replierIndex = indexOf(replierId)
					val inquirerNode = getNode(inquirerIndex)
					assert(inquirerNode.sequencer.isInSequence)
					val inquirerRole = RoleOrdinal_nameOf(inquirerNode.participant.getRoleOrdinal)
					val captor = netSequencer.Captor[(Try[R], RequestId)]()
					netSequencer.run {
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

						// Create a lazy task that perform the RPC
						val replierNode = getNode(replierId)
						val requestingTask =
							if requestIsCursed then {
								netSequencer.Task_apply[Unit] { () =>
									injectConfigurationNoise(configChangeBeforeResponseDelivered_probability)
									captor.capture((Failure(new RuntimeException(s"Net: simulated failure of request $requestId")), requestId), true)
									injectConfigurationNoise(configChangeAfterResponseDelivered_probability)
								}
							} else {
								for {
									_ <- netSequencer.Task_apply(() => scribe.trace(s"$inquirerId -> $replierId: $requestId:$requestDescription, $numberOfTravelingMessages messages are traveling."))
									replyAndRole <- netSequencer.Task_from(replierNode.sequencer) {
										replierNode.sequencer.Task_defers { () =>
											if replierNode.isDown then replierNode.sequencer.Task_ready(null)
											else {
												for reply <- call(replierNode).reconcile yield reply -> (if replierNode.isDown then "DOWN" else RoleOrdinal_nameOf(replierNode.participant.getRoleOrdinal))
											}
										}
									}
								} yield replyAndRole match {
									case null =>
										scribe.trace(s"$inquirerId -< $replierId: $requestId:$requestDescription failed because the node is down, $numberOfTravelingMessages messages are traveling.")
										val respondingTask = netSequencer.Task_apply[Unit] { () =>
											injectConfigurationNoise(configChangeBeforeResponseDelivered_probability)
											captor.capture((Failure(new RuntimeException(s"Net: target node is down: requestId=$requestId")), requestId), true)
											injectConfigurationNoise(configChangeAfterResponseDelivered_probability)
										}
										responseChannel.enqueue(respondingTask)

									case reply -> replierRole =>
										reply match {
											case Success(r) => scribe.trace(s"$inquirerId -< $replierId: $requestId:$requestDescription returned `$r` as $replierRole, $numberOfTravelingMessages messages are traveling.")
											case Failure(e) => scribe.trace(s"$inquirerId -< $replierId: $requestId:$requestDescription failed with:", e)
										}
										val response =
											if responseIsCursed then Failure(new RuntimeException(s"Net: simulated failure of response $requestId"))
											else reply
										val respondingTask = netSequencer.Task_apply[Unit] { () =>
											injectConfigurationNoise(configChangeBeforeResponseDelivered_probability)
											captor.capture((response, requestId), true)
											injectConfigurationNoise(configChangeAfterResponseDelivered_probability)
										}
										responseChannel.enqueue(respondingTask)
								}
							}
						// Enqueue the lazy task that performs the RPC in the channel corresponding to the requests from the inquirer to the replier.
						requestChannel.enqueue(requestingTask)

						while numberOfTravelingMessages > enqueueThresholdForEarlyDelivery do chooseAChannel().dispatchNext()
					}

					netSequencer.Capture_defer { () =>
						captor.transform(new netSequencer.MonoTransformer[(Try[R], RequestId), Try[R]] {
							override def mapSuccess(a: (Try[R], RequestId)): Try[R] = {
								val (response, requestId) = a
								scribe.trace(s"$inquirerId <- $replierId: $requestId:$response, $numberOfTravelingMessages messages on the way")
								response
							}

							override def mapError(e: Throwable): Try[R] = Failure(e)

						})
					}
				} else {
					/// Simple implementation where RPCs always succeeds and adds no randomness
					val replierNode = getNode(replierId)
					replierNode.sequencer.Capture_defer[R](() => call(replierNode)).onBehalfOf(netSequencer)
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

		//// Configuration changes injection ////

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
				determineNewConfig(changeProbability).foreach { (previousConfigMask, newConfigMask) =>
					val configChangeRequest = createNewConfigChangeRequestId()

					// trigger all those duties in their respective node's sequencer
					for configChangeReplies <- sendsConfigChangeRequests(nodesIds, configChangeRequest, nodesIncludedIn(newConfigMask)) do {
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

		/** Sends a configuration change request to each [[Node]] of this [[Net]].
		 * @return a [[Capture]] of the responses of the [[Node]]s */
		private def sendsConfigChangeRequests(targetNodes: Seq[Id], configChangeRequest: String, includedParticipants: ListSet[Id]): netSequencer.Capture[ListMap[Id, ConfigChangeResponse]] = {
			scribe.info(s"Net: About to request (#$configChangeRequest) a configuration change to $includedParticipants")

			def loop(previousResponses: ListMap[Id, ConfigChangeResponse], alreadyTriedNodes: List[Id]): netSequencer.Capture[ListMap[Id, ConfigChangeResponse]] = {
				val remainingTargetNodes = ArrayBuffer.from[Id](targetNodes.filter(n => !alreadyTriedNodes.contains(n)))


				def takeRandomNode(): Node = {
					val nodeIndex = random.between(0, remainingTargetNodes.size)
					val nodeId = remainingTargetNodes.remove(nodeIndex)
					thisNet.getNode(nodeId)
				}

				if remainingTargetNodes.isEmpty then netSequencer.Keeper(previousResponses)
				else {
					val previousResponseAndNextNodeId: (previousResponse: Maybe[ConfigChangeResponse], maybeNextNodeId: Maybe[Id]) =
						if previousResponses.isEmpty then {
							// Start inquiring a random Node among the active ones in the activeConfigChange
							(Maybe.empty, Maybe(takeRandomNode().myId))
						} else {
							val previousResponse = previousResponses.last._2
							val maybeNextNodeId: Maybe[Id] = previousResponse match {
								case pr: (SUCCESSFULLY_CHANGED | ALREADY_CHANGED) =>
									Maybe.empty
								case pr: WAIT_GHOST_LEADER_IS_DEMOTED =>
									Maybe.empty
								case pr: ASK_THE_LEADER =>
									val leaderIndex = remainingTargetNodes.indexOf(pr.leaderId)
									if leaderIndex >= 0 then {
										val leaderId = remainingTargetNodes.remove(leaderIndex)
										Maybe(leaderId)
									} else Maybe(takeRandomNode().myId)

								case pr: (CATCHING_UP | EXCLUDED | SECLUDED | STOPPED) =>
									Maybe(takeRandomNode().myId)
								case pr: (REQUEST_TRACKING_LOST_AFTER_FIRST_PHASE_STARTED | REQUEST_TRACKING_LOST_AFTER_FIRST_PHASE_COMMITTED | REQUEST_TRACKING_LOST_AFTER_SECOND_PHASE_STARTED) =>
									Maybe.empty
							}
							(Maybe(previousResponse), maybeNextNodeId)
					}
					previousResponseAndNextNodeId.maybeNextNodeId.fold {
						netSequencer.Keeper(previousResponses)
					} { nextNodeId =>
						val node = thisNet.getNode(nextNodeId)
						val inquire = node.sequencer.Capture_defer(() =>
							node.clusterParticipant.delegate.requestConfigChange(configChangeRequest, includedParticipants, previousResponseAndNextNodeId.previousResponse)
						).onBehalfOf(netSequencer)
						for {
							response <- inquire
							recursion <- loop(previousResponses ++ ListMap(node.myId -> response), nextNodeId :: alreadyTriedNodes)
						} yield recursion
					}
				}
			}

			loop(ListMap.empty, Nil)
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

		/** Starts the participants in the Config-new-only set when the leading node active [[ConfigChange]] changes to a [[TransitionalConfigChange]].\
		 * In a production environment the nodes wouldn't be started here but before calling [[ConsensusParticipantSdm.ClusterParticipant.Delegate.requestConfigChange]].
		 * Called by the leading node when its [[Node.clusterParticipant.onActiveConfigChanged]] method is called.\ */
		def onActiveConfigChanged(change: ConfigChange[Id], changeIndex: RecordIndex): Unit = {
			netSequencer.run {
				if change.term > activeConfigChange.term || change.term == activeConfigChange.term && changeIndex > indexOfActiveConfigChange then {
					scribe.trace(s"Net: onActiveConfigChanged($change, index=$changeIndex) was called") // when readyToRetireParticipants=$readyToRetireParticipants, quiescedParticipants=$quiescedParticipants ")
					activeConfigChange = change
					indexOfActiveConfigChange = changeIndex
					change match {
						case tcc: TransitionalConfigChange[Id] =>
							for nodeIndex <- 0 until clusterSize do {
								val node = this.getNode(nodeIndex)
								if tcc.newParticipants.contains(node.myId) && !tcc.oldParticipants.contains(node.myId) then {
									val participantsInTheTcc = ListSet.newBuilder.addAll(tcc.oldParticipants).addAll(tcc.newParticipants).result()
									node.startIfNotRunning(changeIndex, participantsInTheTcc).triggerAndForget(false)
								}
							}
						case _ => // Do nothing.
					}
				}
			}
		}

		/** Starts again a [[Node]] that was just [[QUIESCED]] due to a previous [[ConfigChange]], if the active [[ConfigChange]] includes it again.\
		 * Called by the [[QUIESCED]] [[Node]] when its [[Node.clusterParticipant.onQuiesced]] method is called. */
		def onNodeQuiesced(node: Node): Unit = {
			netSequencer.run {
				scribe.trace(s"Net: onNodeQuiesced(${node.myId}) was called") // when indexOfActiveConfigChange=$indexOfActiveConfigChange, readyToRetireParticipants=$readyToRetireParticipants, quiescedParticipants=$quiescedParticipants ")
				if activeConfigChange.isActive(node.myId) then {
					val participantsInActiveConfigChange = ListSet.newBuilder.addAll(activeConfigChange.oldParticipants).addAll(activeConfigChange.newParticipants).result()
					node.startIfNotRunning(indexOfActiveConfigChange, participantsInActiveConfigChange).triggerAndForget(false)
				}
			}
		}

		/**
		 * Attempts to gracefully shut down the network by repeatedly requesting a configuration change to an empty set of participants.\
		 * In order to give the network time to process the request and handle any ongoing communication deterministically, it waits for the network to "settle" between failed attempts.\
		 * The test considers the network settled when either a maximum number of messages have been dispatched (`maxTotalIncrements`) or `stimulusSettlingTime` has passed without any new messages being dispatched.\
		 *
		 * @param maxAttempts The maximum number of configuration change requests before failing the shutdown process.
		 * @param durationBetweenAttempts The duration to wait between attempts to shut down.
		 * @return a [[netSequencer.Task]] yielding [[Maybe.empty]] on success, or a message detailing the failure if `maxAttempts` is reached.
		 */
		def shutDownGracefully(maxAttempts: Int, durationBetweenAttempts: MilliDuration = 9): netSequencer.Capture[Maybe[String]] = {
			netSequencer.checkWithin()

			def loop(failedAttempts: Int): netSequencer.Capture[Maybe[String]] = {
				if failedAttempts == maxAttempts then netSequencer.Keeper(Maybe(s"Net: graceful shutdown failed after $maxAttempts attempts"))
				else {
					val configChangeRequestId = createNewConfigChangeRequestId()
					for {
						responses <- sendsConfigChangeRequests(nodesIds, configChangeRequestId, ListSet.empty)
						maybeErrorMessage <- {
							if responses.exists { response => response._2.isInstanceOf[TerminalConfigChangeResponse] }
								|| responses.forall { response => response._2.isInstanceOf[STOPPED] }
							then {
								scribe.trace(s"Net: Graceful shutdown of the net completed with: ${responses.mkString("[", ", ", "]")}")
								netSequencer.Keeper(Maybe.empty)
							}
							else {
								val attemptNumber = failedAttempts + 1
								scribe.trace(s"Net: Attempt #$attemptNumber to shutdown the net failed with ${responses.mkString("[", ", ", "]")}")
								netSequencer.Capture_delayFlat(netSequencer.newDelaySchedule(durationBetweenAttempts)) { _ => loop(failedAttempts + 1) }
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
		private def sendCommand(commandPayload: Int, attemptFlag: CommandAttemptFlag): net.netSequencer.Capture[Maybe[CommandAttemptFlag]] = {
			if knownParticipants.isEmpty then return net.netSequencer.Keeper(Maybe(attemptFlag))
			val receiverNode = targetParticipant
			scribe.info(s"Client: Sent command:$commandPayload, attemptFlag:$attemptFlag, to:${receiverNode.myId}")
			net.onBeforeClientCommandSent()

			def retry(nextAttemptFlag: CommandAttemptFlag): net.netSequencer.Capture[Maybe[CommandAttemptFlag]] = {
				knownParticipants.find(p => !alreadyTriedParticipants.contains(p)).fold {
					alreadyTriedParticipants.clear()
					net.netSequencer.Keeper(Maybe(nextAttemptFlag))
				} { chosen =>
					targetParticipant = net.getNode(chosen)
					sendCommand(commandPayload, nextAttemptFlag)
				}
			}

			net.netSequencer.Capture_from(receiverNode.sequencer)(receiverNode.sequencer.Capture_defer { () =>
				receiverNode.clusterParticipant.delegate.onCommandFromClient(TestClientCommand(commandPayload, clientId), attemptFlag)
			}).flatMap {
				case receiverNode.Processed(_, content) =>
					scribe.info(s"Client: command `$commandPayload` was processed by ${receiverNode.myId} which replied with `$content`.")
					alreadyTriedParticipants.clear()
					net.netSequencer.Keeper(Maybe.empty)
				case receiverNode.RedirectTo(leaderId) =>
					scribe.info(s"Client: the follower ${receiverNode.myId} redirected the command `$commandPayload` to the leader $leaderId.")
					targetParticipant = net.getNode(leaderId)
					// if despite the attempt flag sent to the participant was FALLBACK (which instructs to update the role before responding) it responds with a redirection to an already tried participant, add it to the already tried ones.
					if attemptFlag == FALLBACK && alreadyTriedParticipants.contains(leaderId) then alreadyTriedParticipants.addOne(targetParticipant.myId)
					sendCommand(commandPayload, REDIRECTED)
				case receiverNode.Unable(nextAttemptFlag, otherParticipants) =>
					knownParticipants = otherParticipants + receiverNode.myId
					alreadyTriedParticipants.addOne(targetParticipant.myId)
					scribe.info(s"Client: the participant ${receiverNode.myId} is unable to process the command `$commandPayload`, nextAttemptFlag=$nextAttemptFlag, otherParticipants=$otherParticipants, knowParticipants=$knownParticipants, alreadyTriedParticipants=$alreadyTriedParticipants")
					retry(nextAttemptFlag)
			}
		}

		def sendCommandsUntil(predicate: (commandIndex: Int) => Boolean, maxRetries: Int = 9): net.netSequencer.Capture[Maybe[String]] = {

			def sendCommandLoop(commandIndex: Int, attemptsCounter: Int, attemptFlag: CommandAttemptFlag): net.netSequencer.Capture[Maybe[String]] = {
				if predicate.apply(commandIndex) then net.netSequencer.Keeper(Maybe.empty)
				else if attemptsCounter > maxRetries then net.netSequencer.Keeper(Maybe("The cluster got stuck unable to progress"))
				else for {
					wasProcessed <- sendCommand(commandIndex, attemptFlag)
					maybeError <- {
						wasProcessed.fold(sendCommandLoop(commandIndex + 1, 0, attemptFlag)) { nextAttemptFlag =>
							scribe.info(s"Client: The command $commandIndex was tried with all the participants. Retrying all again. Attempts done so far: ${attemptsCounter + 1}.")
							sendCommandLoop(commandIndex, attemptsCounter + 1, nextAttemptFlag)
						}
					}
				} yield maybeError
			}

			net.netSequencer.Capture_defer[Maybe[String]](() =>
				sendCommandLoop(1, 0, FIRST_ATTEMPT)
			)
		}
	}

	/** A callback interface to observe and react to internal state changes within a `Node` (log appends, overwrites, command applications). */
	private trait NodeStateChangesListener {
		/** Called when any entry in the log buffer of a [[Node]] is removed (either overwritten or just truncated).
		 * @param index the [[RecordIndex]] of the first removed entry.
		 * @param firstRemovedRecord the first stored [[Record]] that is removed. */
		def onLogTruncated(index: RecordIndex, firstRemovedRecord: Record): Unit = ()

		/** Called when a [[Record]] is appended to the log buffer. */
		def onRecordAppended(record: Record, index: RecordIndex): Unit = ()

		def onCommandApplied(command: TestClientCommand, index: RecordIndex): Unit = ()
	}

	/** An implementation of the [[ConsensusParticipantSdm]] for testing.
	 * @param myId the identifier of this participant.
	 * @param initialParticipants the initial participant identifiers known to this node.
	 * @param net the network simulation environment.
	 * @param remembersLastAppliedCommandIndex whether the state machine remembers the index of the last applied command.
	 * @param retirementDriveRetryPeriod the retry period for retirement driving.
	 * @param unreachableFollowersRetryPeriod the retry period when followers are unreachable.
	 * @param quiescenceAuthorizationRetryPeriod the retry period for quiescence authorization requests.
	 * @param MAX_RECURSION_DEPTH maximum call depth when recursively applying committed commands or processing learners.
	 * @param logCompactionThreshold maximum number of log entries to retain before triggering compaction.
	 * @param maxInFlightAppendsPerPeer maximum number of in-flight append-records calls per peer.
	 * @param logRetentionAfterSnapshot number of records to retain in the log after compaction. */
	private class Node(
		val myId: Id,
		initialParticipants: Set[Id],
		net: Net,
		val remembersLastAppliedCommandIndex: Boolean,
		retirementDriveRetryPeriod: MilliDuration,
		unreachableFollowersRetryPeriod: MilliDuration,
		quiescenceAuthorizationRetryPeriod: MilliDuration,
		override val MAX_RECURSION_DEPTH: Int,
		override val logCompactionThreshold: Int,
		override val maxInFlightAppendsPerPeer: Int,
		override val logRetentionAfterSnapshot: Int
	) extends ConsensusParticipantSdm { thisNode =>

		import net.rpc

		override type ParticipantId = Id
		override type ClientCommand = TestClientCommand
		override type StateMachineResponse = Int
		override type ClientId = String

		override val sequencer: ScheduSequen = net.doerProvider.provide(s"node-sequencer-$myId")

		var statesChangesListener: NodeStateChangesListener = new NodeStateChangesListener() {
			override def onLogTruncated(index: RecordIndex, firstRemovedRecord: Record): Unit = ()
		}

		private var initialNotificationListener: NotificationListener = new DefaultNotificationListener()

		private var _participant: ConsensusParticipant = uninitialized

		inline def isDown: Boolean = _participant eq null

		/** @return the [[ConsensusParticipant]] service instance corresponding to this [[Node]]. */
		inline def participant: ConsensusParticipant = _participant

		/** Initializes this [[Node]]. Does not start the [[ConsensusParticipant]] service. */
		inline def initialize(initialNotificationListener: NotificationListener = DefaultNotificationListener()): Unit = {
			this.initialNotificationListener = initialNotificationListener
		}

		/** Creates the [[ConsensusParticipant]] service instance of this [[Node]]. */
		def startIfNotRunning(indexOfTheIncludingConfigChange: RecordIndex, participantsInTheIncludingConfigChange: ListSet[ParticipantId]): sequencer.Capture[Unit] = {
			sequencer.Capture_apply { () =>
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
			var highestAppliedCommandSerial: Int = 0
			var highestAppliedCommandIndex: RecordIndex = 0

			override def applyClientCommand(index: RecordIndex, command: ClientCommand): sequencer.Capture[StateMachineResponse] = {
				sequencer.checkWithin()
				if index > highestAppliedCommandIndex then highestAppliedCommandIndex = index
				if command.serial > highestAppliedCommandSerial then {
					assert(command.serial - highestAppliedCommandSerial == 1, s"command.serial=${command.serial}, highestAppliedCommandSerial=$highestAppliedCommandSerial")
					highestAppliedCommandSerial = command.serial
				}
				statesChangesListener.onCommandApplied(command, index)
				// TODO add delay
				sequencer.Keeper(command.serial)
			}

			override def recoverIndexOfLastAppliedCommand: sequencer.Capture[RecordIndex] = {
				sequencer.checkWithin()
				if remembersLastAppliedCommandIndex then sequencer.Keeper(highestAppliedCommandIndex)
				else {
					highestAppliedCommandSerial = 0
					highestAppliedCommandIndex = 0
					sequencer.Keeper(0)
				}
			}

			override def takeSnapshot(): sequencer.Capture[IArray[Byte]] = {
				sequencer.checkWithin()
				// Serialize the applied commands map as a simple byte array
				val bytes = java.io.ByteArrayOutputStream()
				val out = java.io.ObjectOutputStream(bytes)
				out.writeInt(highestAppliedCommandSerial)
				out.writeLong(highestAppliedCommandIndex)
				out.flush()
				sequencer.Keeper(IArray.unsafeFromArray(bytes.toByteArray))
			}

			override def installSnapshot(data: IArray[Byte]): sequencer.Capture[Unit] = {
				sequencer.checkWithin()
				val in = java.io.ObjectInputStream(java.io.ByteArrayInputStream(data.unsafeArray))
				highestAppliedCommandSerial = in.readInt()
				highestAppliedCommandIndex = in.readLong()
				sequencer.Capture_ready(Doer.successUnit)
			}
		}

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

			override def onActiveConfigChanged(change: ConfigChange[ParticipantId], changeIndex: RecordIndex, roleOrdinal: RoleOrdinal): Unit = {
				sequencer.checkWithin()
				scribe.info(s"cluster-$boundParticipantId: onConfigurationChanged($change, index=$changeIndex, ${RoleOrdinal_nameOf(roleOrdinal)}) called.")
				if roleOrdinal == LEADER then net.onActiveConfigChanged(change, changeIndex)
			}

			extension (replierId: ParticipantId) {


				override def howAreYou(inquirerInfo: StateInfo): sequencer.Capture[StateInfo] = {
					sequencer.checkWithin()
					boundParticipantId.rpc[StateInfo](
						replierId,
						s"HowAreYou(inquirerInfo=$inquirerInfo)"
					) { replierNode =>
						replierNode.clusterParticipant.delegate.onHowAreYou(boundParticipantId, inquirerInfo)
					}.onBehalfOf(sequencer)
				}

				override def chooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.Capture[Vote[ParticipantId]] = {
					sequencer.checkWithin()
					boundParticipantId.rpc[Vote[ParticipantId]](
						replierId,
						s"ChooseALeader(inquirerId:$inquirerId, inquirerInfo:$inquirerInfo)"
					) { replier =>
						replier.clusterParticipant.delegate.onChooseALeader(inquirerId, inquirerInfo)
					}.onBehalfOf(sequencer)
				}

				override def appendRecords(inquirerTerm: Term, prevLogIndex: RecordIndex, prevLogTerm: Term, batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.Capture[AppendResult] = {
					sequencer.checkWithin()
					boundParticipantId.rpc[AppendResult](
						replierId,
						s"AppendRecords(inquirerTerm:$inquirerTerm, previousLogIndex:$prevLogIndex, previousLogTerm:$prevLogTerm, records:${batch.mkString("[", ", ", "]")}, leaderCommit:$leaderCommit, termAtLeaderCommit:$termAtLeaderCommit)"
					) { replier =>
						replier.clusterParticipant.delegate.onAppendRecords(boundParticipantId, inquirerTerm, prevLogIndex, prevLogTerm, IArray.from(batch), leaderCommit, termAtLeaderCommit)
					}.onBehalfOf(sequencer)
				}

				override def permitQuiescence(indexOfGrantedStableConfigChange: RecordIndex): sequencer.Capture[Unit] = {
					sequencer.checkWithin()
					boundParticipantId.rpc[Unit](
						replierId,
						s"PermitQuiesce($indexOfGrantedStableConfigChange)"
					) { replier =>
						replier.sequencer.Keeper(replier.clusterParticipant.delegate.onQuiescencePermitted(boundParticipantId, indexOfGrantedStableConfigChange))
					}.onBehalfOf(sequencer)
				}

				override def installSnapshot(inquirerTerm: Term, snapshot: SnapshotData[ParticipantId], batch: IArray[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.Capture[AppendResult] = {
					sequencer.checkWithin()
					boundParticipantId.rpc[AppendResult](
						replierId,
						s"InstallSnapshot(inquirerTerm:$inquirerTerm, snapshot:$snapshot, records:${batch.mkString("[", ", ", "]")}, leaderCommit:$leaderCommit, termAtLeaderCommit:$termAtLeaderCommit)"
					) { replier =>
						replier.clusterParticipant.delegate.onInstallSnapshot(boundParticipantId, inquirerTerm, snapshot, batch, leaderCommit, termAtLeaderCommit)
					}.onBehalfOf(sequencer)
				}
			}

			override def getOtherProbableParticipants: ListSet[ParticipantId] = ListSet.from(net.nodesIds) - myId

			override def onQuiesced(motive: Try[String]): Unit = {
				scribe.info(s"cluster-$myId: `notifyQuiesced` was called with motive=$motive")
				net.onNodeQuiesced(thisNode)
			}

			override def requestWakeUp(reason: WakeUpReason, wakeupsDone: Int, callback: () => Unit): WakeUpToken = {
				val baseDuration = reason match {
					case WakeUpReason.RetirementDriveRetry => retirementDriveRetryPeriod
					case WakeUpReason.UnreachableFollowersRetry => unreachableFollowersRetryPeriod
					case WakeUpReason.QuiescenceAuthorizationRetry => quiescenceAuthorizationRetryPeriod
					case WakeUpReason.ReplicationLoopRetry => unreachableFollowersRetryPeriod
				}
				val delaySchedule: sequencer.Schedule = sequencer.newDelaySchedule(baseDuration * (wakeupsDone + 1))
				object token extends WakeUpToken, (sequencer.Schedule => Unit) {

					override def cancel(): Unit = {
						sequencer.cancel(delaySchedule)
					}

					override def apply(schedule: sequencer.Schedule): Unit =
						callback()
				}
				sequencer.schedule(delaySchedule)(token)
				token
			}
		}

		/**
		 * Test instance and implementation of the [[Storage]] service interface required by the [[participant]] (the [[ConsensusParticipant]] service corresponding to a [[Node]]).
		 * Uses an in-memory [[TestWorkspace]] for storing the participant's log and state.
		 */
		object storage extends Storage {
			private[ConsensusParticipantSdmTest] var memory: WS = TestWorkspace()

			override def load: sequencer.Capture[WS] = {
				sequencer.checkWithin()
				sequencer.Keeper(memory)
			} // TODO add a delay

			override def save(workspace: WS): sequencer.Capture[Unit] = {
				sequencer.checkWithin()
				memory = workspace
				sequencer.Keeper(Doer.successUnit) // TODO add a delay
			}
		}

		override type WS = TestWorkspace

		/**
		 * Test implementation of [[Workspace]]
		 */
		class TestWorkspace extends Workspace {
			private var currentTerm: Term = PRE_INIT
			private var _votedFor: Maybe[ParticipantId] = Maybe.empty
			private val logBuffer: mutable.ArrayBuffer[Record] = mutable.ArrayBuffer.empty
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

			override def truncateSuffix(fromIndex: RecordIndex): Unit = {
				sequencer.checkWithin()
				val writeIndex = (fromIndex - _logBufferOffset).toInt
				if writeIndex < logBuffer.size then {
					val firstRemovedRecord = logBuffer(writeIndex)
					statesChangesListener.onLogTruncated(fromIndex, firstRemovedRecord)
					logBuffer.takeInPlace(writeIndex)
				}
			}

			override def resetLog(snapshot: SnapshotData[ParticipantId], tailRecords: IArray[Record]): Unit = {
				sequencer.checkWithin()
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
				maybeLatestSnapshot = Maybe(snapshot)
			}

			override def release(): sequencer.Capture[Unit] = {
				scribe.info(s"workspace-$myId: was released")
				sequencer.Capture_unit
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

			override def onAbdicating(term: Term): Unit = {
				sequencer.checkWithin()
				scribe.info(s"scribe-$myId: is handing off the leadership. The term $term is over.")
			}

			override def onRetiring(previous: RoleOrdinal, term: Term): Unit = {
				sequencer.checkWithin()
				scribe.info(s"scribe-$myId: became retiree from ${RoleOrdinal_nameOf(previous)} at @$term.")
			}

			override def onRoleLeft(left: RoleOrdinal, term: Term): Unit = ()

			override def onCommitIndexChanged(previous: RecordIndex, current: RecordIndex, as: RoleOrdinal, at: Term): Unit = {
				sequencer.checkWithin()
				scribe.info(s"scribe-$myId: commitIndex changed from $previous to $current as ${RoleOrdinal_nameOf(as)} @$at}")
			}

			override def onCommandApplied(appliedCommandIndex: RecordIndex, appliedCommandTerm: Term): Unit = {
				sequencer.checkWithin()
				scribe.info(s"scribe-$myId: the command at index $appliedCommandIndex and term $appliedCommandTerm was applied to the state machine.")
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
	 * @param remembersLastAppliedCommandIndex whether the state machine remembers the index of the last applied command.
	 * @param retirementDriveRetryPeriod the retry period for retirement driving.
	 * @param unreachableFollowersRetryPeriod the retry period when followers are unreachable.
	 * @param quiescenceAuthorizationRetryPeriod the retry period for quiescence authorization requests.
	 * @param MAX_RECURSION_DEPTH maximum call depth when recursively applying committed commands or processing learners.
	 * @param logCompactionThreshold maximum number of log entries to retain before triggering compaction.
	 * @param maxInFlightAppendsPerPeer maximum number of in-flight append-records calls per peer.
	 * @param logRetentionAfterSnapshot number of records to retain in the log after compaction.
	 * @param notificationListenerBuilder a function that takes the new [[Node]] and builds the [[NotificationListener]] to be passed to its [[ConsensusParticipantSdm.ConsensusParticipant]] service constructor. */
	private def createAndInitializeNodes[N <: Net](
		net: N,
		weakReferencesHolder: mutable.Buffer[AnyRef],
		remembersLastAppliedCommandIndex: Boolean,
		retirementDriveRetryPeriod: MilliDuration,
		unreachableFollowersRetryPeriod: MilliDuration,
		quiescenceAuthorizationRetryPeriod: MilliDuration,
		MAX_RECURSION_DEPTH: Int,
		logCompactionThreshold: Int,
		maxInFlightAppendsPerPeer: Int,
		logRetentionAfterSnapshot: Int
	)(notificationListenerBuilder: (node: Node) => node.NotificationListener): Unit = {

		val initialParticipants = net.nodesIncludedIn(net.initialConfigMask)
		for id <- net.nodesIds do {
			val node = Node(
				id,
				initialParticipants,
				net,
				remembersLastAppliedCommandIndex,
				retirementDriveRetryPeriod,
				unreachableFollowersRetryPeriod,
				quiescenceAuthorizationRetryPeriod,
				MAX_RECURSION_DEPTH,
				logCompactionThreshold,
				maxInFlightAppendsPerPeer,
				logRetentionAfterSnapshot
			)
			net.addNode(node)
			val nl = notificationListenerBuilder(node)
			weakReferencesHolder.addOne(nl)
			node.initialize(nl)
		}
	}

	/** This is the core logic for verifying the correctness of the consensus algorithm. It sets up a simulated environment and then actively checks several critical invariants:
	 *  - Leader Append-Only: Ensures that a leader never overwrites or deletes entries in its log, only appends.
	 *  - Log Matching: Verifies that if two logs contain an entry with the same index and term, then the logs are identical up through that index.
	 *  - State Machine Safety: Guarantees that if a server has applied a log entry at a given index to its state machine, no other server will ever apply a different log entry for the same index.
	 *  - Election Safety: Asserts that at most one leader can be elected in a given term.
	 *  - Leader Completeness: Confirms that if a log entry is committed in a given term, then that entry will be present in the logs of the leaders for all higher-numbered terms.
	 *
	 * These invariants are checked using the `NodeStateChangesListener` and by comparing the state across multiple simulated nodes. If any invariant is violated, the test fails immediately via a `Promise.tryFailure`.
	 * @param net the [[Net]] simulation environment.
	 * @param startWithHighestPriorityParticipant whether the client starts by communicating with the highest priority participant.
	 * @param remembersLastAppliedCommandIndex whether the state machine remembers the index of the last applied command.
	 * @param numberOfCommandsToSend total number of client commands to submit.
	 * @param maxRetries maximum retry attempts for client commands and graceful shutdown.
	 * @param retirementDriveRetryPeriod the retry period for retirement driving.
	 * @param unreachableFollowersRetryPeriod the retry period when followers are unreachable.
	 * @param quiescenceAuthorizationRetryPeriod the retry period for quiescence authorization requests.
	 * @param configChangeRetryPeriod the retry period for configuration changes during shutdown.
	 * @param MAX_RECURSION_DEPTH maximum call depth when recursively applying committed commands or processing learners.
	 * @param logCompactionThreshold maximum number of log entries to retain before triggering compaction.
	 * @param maxInFlightAppendsPerPeer maximum number of in-flight append-records calls per peer.
	 * @param logRetentionAfterSnapshot number of records to retain in the log after compaction. */
	private def testAllInvariants(
		net: Net,
		startWithHighestPriorityParticipant: Boolean,
		remembersLastAppliedCommandIndex: Boolean = false,
		numberOfCommandsToSend: Int = 20,
		maxRetries: Int = 20,
		retirementDriveRetryPeriod: MilliDuration = 10,
		unreachableFollowersRetryPeriod: MilliDuration = 10,
		quiescenceAuthorizationRetryPeriod: MilliDuration = 10,
		configChangeRetryPeriod: MilliDuration = 100,
		MAX_RECURSION_DEPTH: Int = 1,
		logCompactionThreshold: Int = 5,
		maxInFlightAppendsPerPeer: Int = 1,
		logRetentionAfterSnapshot: Int = 0
	): Future[Unit] = {
		val promise = Promise[Unit]()
		val clusterSize = net.clusterSize
		val netRandomnessSeed = net.randomnessSeed
		scribe.info(s"\n----------------\nBegin: clusterSize=$clusterSize, initialConfig=${net.initialConfigMask.mkString("[", ", ", "]")}, startWithHighestPriorityParticipant=$startWithHighestPriorityParticipant, netRandomnessSeed=$netRandomnessSeed, remembersLastAppliedCommandIndex=$remembersLastAppliedCommandIndex, maxRecursionDepth=$MAX_RECURSION_DEPTH, logCompactionThreshold=$logCompactionThreshold, maxInFlightAppendsPerPeer=$maxInFlightAppendsPerPeer, logRetentionAfterSnapshot=$logRetentionAfterSnapshot\n($numberOfCommandsToSend, $clusterSize, $startWithHighestPriorityParticipant, ${netRandomnessSeed}L, $remembersLastAppliedCommandIndex, $MAX_RECURSION_DEPTH, $logCompactionThreshold, $maxInFlightAppendsPerPeer, $logRetentionAfterSnapshot),")
		val weakReferencesHolder = mutable.Buffer.empty[AnyRef]
		val leaderNodeByTerm: mutable.SortedMap[Term, Node] = mutable.SortedMap.empty
		val committedRecordsByNodeIndex: Array[mutable.Buffer[Record | None.type]] = Array.fill(clusterSize)(mutable.Buffer.empty)
		val appliedCommandsByNodeIndex: Array[mutable.SortedMap[Int, TestClientCommand]] = Array.fill(clusterSize)(mutable.SortedMap.empty)

		createAndInitializeNodes(
			net,
			weakReferencesHolder,
			remembersLastAppliedCommandIndex = remembersLastAppliedCommandIndex,
			retirementDriveRetryPeriod = retirementDriveRetryPeriod,
			unreachableFollowersRetryPeriod = unreachableFollowersRetryPeriod,
			quiescenceAuthorizationRetryPeriod = quiescenceAuthorizationRetryPeriod,
			MAX_RECURSION_DEPTH = MAX_RECURSION_DEPTH,
			logCompactionThreshold = logCompactionThreshold,
			maxInFlightAppendsPerPeer = maxInFlightAppendsPerPeer,
			logRetentionAfterSnapshot = logRetentionAfterSnapshot
		) { node =>
			node.statesChangesListener = new NodeStateChangesListener() {
				override def onLogTruncated(index: RecordIndex, firstRemovedRecord: Record): Unit = {
					// Defer the check to let the updating execution to complete the atomic changes.
					node.sequencer.run {
						// Checks Leader Append-Only: a leader never overwrites or deletes entries in its log
						if node.participant.getRoleOrdinal == LEADER then promise.tryFailure(new AssertionError(s"The participant ${node.myId} broke the 'append only rule' by truncating its log at index $index. Removed record: $firstRemovedRecord."))
					}
				}

				override def onRecordAppended(record: Record, index: RecordIndex): Unit = {
					// Checks Log Matching: if two logs contain an entry with the same index and term, then the logs are identical in all entries up through the given index. §5.3
					val thisNodeMemory = node.storage.memory
					val thisNodeFrom = thisNodeMemory.logBufferOffset.toInt
					val thisNodeUntil = thisNodeMemory.firstEmptyRecordIndex.toInt
					val thisNodeRecords = thisNodeMemory.getRecordsBetween(thisNodeFrom, thisNodeUntil)

					for nodeIndex <- 0 until clusterSize do {
						val otherNode = net.getNode(nodeIndex)
						if otherNode ne node then {
							otherNode.sequencer.run {
								val otherNodeMemory = otherNode.storage.memory
								val otherNodeFrom = otherNodeMemory.logBufferOffset.toInt
								val otherNodeUntil = otherNodeMemory.firstEmptyRecordIndex.toInt
								val otherNodeRecords = otherNodeMemory.getRecordsBetween(otherNodeFrom, otherNodeUntil)

								val maxFrom = math.max(thisNodeFrom, otherNodeFrom)
								val minUntil = math.min(thisNodeUntil, otherNodeUntil)
								// Find the last record with same term.								
								var indexOfLastRecordWithSameTerm = minUntil - 1
								while indexOfLastRecordWithSameTerm >= maxFrom && otherNodeRecords(indexOfLastRecordWithSameTerm - otherNodeFrom).term != thisNodeRecords(indexOfLastRecordWithSameTerm - thisNodeFrom).term do indexOfLastRecordWithSameTerm -= 1
								// Check all records up to that record index are identical.
								for index <- maxFrom to indexOfLastRecordWithSameTerm do {
									if otherNodeRecords(index - otherNodeFrom) != thisNodeRecords(index - thisNodeFrom) then promise.tryFailure(new AssertionError(s"The logs of nodes ${node.myId} and ${otherNode.myId} are not identical in all entries up through $indexOfLastRecordWithSameTerm despite the records at that index have the same term. ${node.myId}: from=$thisNodeFrom, records=${thisNodeRecords.mkString("[", ", ", "]")}; ${otherNode.myId}: from=$otherNodeFrom, records=${otherNodeRecords.mkString("[", ", ", "]")}"))
								}
							}
						}
					}
				}

				override def onCommandApplied(command: TestClientCommand, index: RecordIndex): Unit = {
					// Checks State Machine Safety: if a server has applied a log entry at a given index to its state machine, no other server will ever apply a different log entry for the same index. §5.4.3
					val commandsAppliedToThisNode = appliedCommandsByNodeIndex(net.indexOf(node.myId))
					commandsAppliedToThisNode.get(index.toInt) match {
						case Some(previouslyAppliedCommand) =>
							if command != previouslyAppliedCommand then promise.tryFailure(new AssertionError(s"Two different commands with same log index were applied to the node ${node.myId}: previous=$previouslyAppliedCommand, new=$command"))
						case None =>
							commandsAppliedToThisNode(index.toInt) = command
					}

					for nodeIndex <- 0 until clusterSize do {
						val otherNode = net.getNode(nodeIndex)
						if otherNode ne node then {
							otherNode.sequencer.run {
								val commandsAppliedToTheOtherNode = appliedCommandsByNodeIndex(net.indexOf(otherNode.myId))
								commandsAppliedToTheOtherNode.get(index.toInt).foreach { commandAppliedToTheOtherNode =>
									if commandAppliedToTheOtherNode != command then promise.tryFailure(new AssertionError(s"The node ${node.myId} applied the command $command at index $index, which is different from the command $commandAppliedToTheOtherNode applied at the same index in node ${otherNode.myId}."))
								}
							}
						}
					}
				}
			}
			new node.DefaultNotificationListener() {
				override def onStarting(previous: RoleOrdinal, indexOfTheIncludingConfigChange: RecordIndex): Unit = {
					if !node.remembersLastAppliedCommandIndex then {
						committedRecordsByNodeIndex(net.indexOf(node.myId)).clear()
						appliedCommandsByNodeIndex(net.indexOf(node.myId)).clear()
					}
				}

				override def onBecameLeader(previous: RoleOrdinal, term: Term): Unit = {
					// Checks Election Safety: at most one leader can be elected in a given term. §5.2
					leaderNodeByTerm.get(term) match {
						case Some(firstOwner) => promise.tryFailure(new AssertionError(s"Node ${node.myId} became leader at term $term despite node ${firstOwner.myId} was a leader of the same term before."))
						case None => leaderNodeByTerm.put(term, node)
					}
				}

				override def onCommitIndexChanged(previous: RecordIndex, current: RecordIndex, as: RoleOrdinal, at: Term): Unit = {
					// Checks Leader Completeness: if a log entry is committed in a given term, then that entry will be present in the logs of the leaders for all higher-numbered terms. §5.4
					val thisNodeCommittedRecordsMemory = committedRecordsByNodeIndex(net.indexOf(node.myId))
					val thisNodeLogBufferOffset = node.storage.memory.logBufferOffset

					// Validate consistency for any overlapping records that were already recorded before restart/re-join
					val indexOfFirstRecordToCheck = (previous + 1).max(thisNodeLogBufferOffset) // clamped by the logBufferOffset to skip records that predate the last snapshot
					val indexOfLastRecordToCheck = current.min(thisNodeCommittedRecordsMemory.size.toLong) // clamped by the base1-index of the last record memorized in the parallel memory (which has base0).
					var checkIdx = indexOfFirstRecordToCheck
					while checkIdx <= indexOfLastRecordToCheck do {
						thisNodeCommittedRecordsMemory(checkIdx.toInt - 1) match {
							case contentInParallelMemory: Record =>
								val contentInStorage = node.storage.memory.getRecordAt(checkIdx)
								if contentInStorage != contentInParallelMemory then {
									promise.tryFailure(new AssertionError(s"Node ${node.myId} committed a different record at index $checkIdx after restart: previous=$contentInParallelMemory, current=$contentInStorage"))
								}
							case None => ()
						}
						checkIdx += 1
					}

					// Memorize the committed records in the parallel memory, filling potential holes with `None`.
					// Append only newly committed records beyond the previously recorded memory size
					val indexOfFirstRecordToAppendToParallelMemory = indexOfFirstRecordToCheck.max(thisNodeCommittedRecordsMemory.size + 1L)
					if indexOfFirstRecordToAppendToParallelMemory <= current then {
						val indexOfFirstRecordToAppendToParallelMemoryBase0 = indexOfFirstRecordToAppendToParallelMemory.toInt - 1
						val holeLength = indexOfFirstRecordToAppendToParallelMemoryBase0 - thisNodeCommittedRecordsMemory.size
						if holeLength > 0 then thisNodeCommittedRecordsMemory.addAll(Iterable.fill(holeLength)(None))
						val newCommittedRecords = node.storage.memory.getRecordsBetween(indexOfFirstRecordToAppendToParallelMemory, current + 1)
						thisNodeCommittedRecordsMemory.addAll(newCommittedRecords)
					}

					if as == LEADER then {
						// Check that records committed in the past by other nodes are present in the leader's log.
						for nodeIndex <- 0 until clusterSize do {
							val otherNode = net.getNode(nodeIndex)
							if otherNode ne node then {
								val otherNodeCommittedRecordsMemory = committedRecordsByNodeIndex(nodeIndex)
								for (otherNodeCommittedRecord, recordIndexBase0) <- otherNodeCommittedRecordsMemory.zipWithIndex do {
									otherNodeCommittedRecord match {
										case None => ()
										case otherNodeCommittedRecord: Record =>
											if recordIndexBase0 < thisNodeCommittedRecordsMemory.size then {
												thisNodeCommittedRecordsMemory(recordIndexBase0) match {
													case None => ()
													case leaderCommittedRecord: Record =>
														if leaderCommittedRecord != otherNodeCommittedRecord then {
															promise.tryFailure(new AssertionError(s"Node ${otherNode.myId} has a committed record at index ${recordIndexBase0 + 1} that differs from the record of the current leader ${node.myId}, which breaks the \"Leader completeness\" invariant. The committed records of each are: ${otherNode.myId} -> $otherNodeCommittedRecordsMemory; ${node.myId} -> $thisNodeCommittedRecordsMemory"))
														}
												}
											}
											// If a peer has committed records with higher index than this node commitIndex, then this node was deposed and the missing records must have a higher term.
											else if otherNodeCommittedRecord.term <= at then {
												promise.tryFailure(new AssertionError(s"Node ${otherNode.myId} has more committed records with term <= the term (term $at) leaded by the current leader ${node.myId},  which breaks the \"Leader completeness\" invariant. The committed records of each are: ${otherNode.myId} -> $otherNodeCommittedRecordsMemory; ${node.myId} -> $thisNodeCommittedRecordsMemory"))
											}
									}
								}
							}
						}
					}
				}
			}
		}

		net.netSequencer.run {
			for {
				_ <- net.startAllNodes
				client = Client[net.type]("A", net, startWithHighestPriorityParticipant)
				maybeCommandErrorMsg <- client.sendCommandsUntil(commandIndex => commandIndex > numberOfCommandsToSend || promise.isCompleted, maxRetries)
				maybeErrorMsg <- maybeCommandErrorMsg.fold {
					val maxAuthAttempts = net.getNode(0).MAX_PERMIT_QUIESCENCE_RETRIES
					val authTotalTime = quiescenceAuthorizationRetryPeriod * (maxAuthAttempts + 1) * (maxAuthAttempts + 2) / 2
					val maxDriveAttempts = net.getNode(0).retiringParticipantMaxRetries
					val driveTotalTime = retirementDriveRetryPeriod * (maxDriveAttempts + 1) * (maxDriveAttempts + 2) / 2
					val maxNodeSettleTime = (authTotalTime + driveTotalTime) * net.clusterSize
					net.shutDownGracefully(maxRetries, maxNodeSettleTime)
				} { errorMsg =>
					net.netSequencer.Keeper(Maybe(errorMsg))
				}
			} do maybeErrorMsg.fold(promise.tryComplete(Success(())))(errorMsg => promise.tryFailure(new AssertionError(errorMsg)))
		}

		promise.future.andThen { tr =>
			val header = "**** TEST COMPLETED! **** Stopping the Net. Result:"
			tr.fold(
				e => scribe.trace(s"$header failed with:", e),
				_ => scribe.trace(s"$header passed successfully")
			)
			net.stop()
				.toFuture()
				.map(_ => ())
		}
	}

	// A simple test to remind the user to run tests with assertions enabled (`-ea` VM option)
	test("assertions are enabled") {
		if !ConsensusParticipantSdm.assertionsEnabled then println("Enable assertions for all test to detect more bugs by adding the the -ea VM option.")
		if ConsensusParticipantSdm.assertionsEnabled then Future.successful(()) else Future.failed(new RuntimeException(""))
	}

	/** Randomizable configuration parameters for a [[Node]].
	 * @param remembersLastAppliedCommandIndex whether the state machine remembers the index of the last applied command.
	 * @param maxRecursionDepth maximum call depth when recursively applying committed commands or processing learners.
	 * @param logCompactionThreshold maximum number of log entries to retain before triggering compaction.
	 * @param maxInFlightAppendsPerPeer maximum number of in-flight append-records calls per peer.
	 * @param logRetentionAfterSnapshot number of records to retain in the log after compaction. */
	private case class NodeConfig(
		remembersLastAppliedCommandIndex: Boolean,
		maxRecursionDepth: Int,
		logCompactionThreshold: Int,
		maxInFlightAppendsPerPeer: Int,
		logRetentionAfterSnapshot: Int
	)

	/** ScalaCheck generator for [[NodeConfig]]. */
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

	// A property-based test that runs many simulations with varying cluster sizes, starting participants, and random seeds, but *without* injecting configuration changes.
	test("All invariants must comply - without configuration changes noise".ignore) {
		inline val numberOfCommandsToSend = 10
		PropF.forAllNoShrinkF(
			Gen.choose(2, 7),
			Gen.oneOf(true, false),
			Gen.long,
			genNodeConfig
		) { (clusterSize, startWithHighestPriorityParticipant, netRandomnessSeed, nodeConfig) =>
			val net = new Net(clusterSize, randomnessSeed = netRandomnessSeed, requestFailurePercentage = 10, responseFailurePercentage = 10, configChangeBeforeRequestDelivered_probability = 0, configChangeBeforeResponseDelivered_probability = 0, configChangeAfterResponseDelivered_probability = 0)
			testAllInvariants(
				net,
				startWithHighestPriorityParticipant,
				remembersLastAppliedCommandIndex = nodeConfig.remembersLastAppliedCommandIndex,
				numberOfCommandsToSend = numberOfCommandsToSend,
				maxRetries = 15,
				retirementDriveRetryPeriod = clusterSize * 10,
				unreachableFollowersRetryPeriod = clusterSize * 10,
				quiescenceAuthorizationRetryPeriod = clusterSize * 10,
				configChangeRetryPeriod = clusterSize * 100,
				MAX_RECURSION_DEPTH = nodeConfig.maxRecursionDepth,
				logCompactionThreshold = nodeConfig.logCompactionThreshold,
				maxInFlightAppendsPerPeer = nodeConfig.maxInFlightAppendsPerPeer,
				logRetentionAfterSnapshot = nodeConfig.logRetentionAfterSnapshot
			)
		}
	}

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
		val failingCases = Seq[FailingCase](
			(30, 3, true, 1494279300139860962L, false, 0, 5, 9, 0),
			(30, 6, false, 5418681597785684599L, false, 1, 5, 9, 3),
			(30, 2, true, -3834115379994352266L, false, 1, 5, 9, 0),
			(30, 4, true, -2499323556213279510L, false, 1, 5, 1, 0),
			(30, 4, true, -7641283460342501667L, true, 0, 3, 1, 1),
			(30, 6, false, -7096462650479832304L, false, 9, 3, 1, 0),
			(30, 3, true, -4148517921068024394L, false, 0, 5, 1, 0),
			(30, 2, false, -5783341547509500611L, true, 9, 3, 9, 3),
			(30, 10, true, 4021753203816662023L, false, 0, 5, 2, 0),
			(30, 8, true, 4118164278127760845L, true, 1, 3, 9, 3),
			(30, 10, true, -2595686814493846026L, true, 0, 3, 9, 3),
			(30, 9, false, -5356717205083865951L, false, 0, 5, 2, 1), // The super sample that exposed 4 bugs.
			(30, 5, true, -6412920902264911829L, false, 9, 3, 9, 0),
			(30, 5, false, -6344082515431853467L, false, 9, 5, 9, 3),
			(30, 12, false, -979546981164946039L, true, 0, 3, 1, 0),
			(30, 9, false, 7958057327002876682L, false, 1, 1, 9, 5),
			(30, 12, false, -2040876099453344345L, false, 1, 5, 1, 0),
			(30, 12, false, -4525504475399466095L, false, 1, 5, 1, 0),
			(30, 15, false, 5715498412747712398L, false, 1, 5, 1, 0),
			(30, 8, true, -8505862789124375259L, false, 1, 5, 1, 0),
			(30, 6, true, -8695189366888117562L, false, 1, 5, 1, 0),
			(30, 8, false, -7045886391286260825L, false, 1, 5, 1, 0),
			(30, 2, true, -5719502751839801933L, false, 1, 5, 1, 0),
			(30, 9, true, -5561042816536613276L, false, 1, 5, 1, 0),
			(30, 5, false, 3454827329483479159L, false, 1, 5, 1, 0), // strange situation during graceful shutdown
			(30, 6, false, 4377378712223639909L, false, 1, 5, 1, 0), // LeaderTransition record kind is used.
			(30, 8, true, 3219848794431902011L, false, 1, 5, 1, 0),
			(30, 3, false, 2486592392277813285L, false, 1, 5, 1, 0),
			(30, 4, true, 4513069980120952979L, false, 1, 5, 1, 0),
			(30, 6, false, -25160870373328826L, false, 1, 5, 1, 0),
			(30, 9, true, -7099459776600378137L, false, 1, 5, 1, 0),
			(30, 6, true, -4000233556805337121L, false, 1, 5, 1, 0),
			(30, 15, false, -8377836387231152620L, false, 1, 5, 1, 0),
			(30, 2, true, 5465215041039872636L, false, 1, 5, 1, 0),
			(30, 4, false, -2871391883553136003L, false, 1, 5, 1, 0),
			(30, 2, true, 5082886513912816935L, false, 1, 5, 1, 0), // hanged up without any error.
			(30, 15, false, 107166222495627916L, false, 1, 5, 1, 0),
			(30, 3, false, -4120685655909330148L, false, 1, 5, 1, 0),
			(30, 6, false, 4457054910789412562L, false, 1, 5, 1, 0), // Retiring finalTerm greater than termAtExcludingConfigChange.
			(30, 6, true, 1187713772695268880L, false, 1, 5, 1, 0),
			(30, 15, true, -7036178255522478916L, false, 1, 5, 1, 0), // Does not converge
			(30, 4, false, -1201266674536539693L, false, 1, 5, 1, 0), // MatchError thrown at StableConfigChange.isCoupleOf
			(30, 7, false, -6232654863579614157L, false, 1, 5, 1, 0), // Net: graceful shutdown failed after 20 attempts
			(30, 3, true, -2370286264465510604L, false, 1, 5, 1, 0), // PanicException thrown in replicateTccAndThenStartSecondPhase
			(30, 2, true, 1380848690399351272L, false, 1, 5, 1, 0), // The node p-1 applied the command TestClientCommand(19,A) at index 22, which is different from the command TestClientCommand(18,A) applied at the same index in node p-0.
			(30, 3, false, -2547866549608645507L, false, 1, 5, 1, 0), // PanicException
			(30, 3, true, -7417113718760886059L, false, 1, 5, 1, 0), // "Should never happen" assertion triggered
			(30, 3, false, 7259924510493798812L, false, 1, 5, 1, 0),
			(30, 3, true, -7726398781820803091L, false, 1, 5, 1, 0), // assertion failed: currentPrimaryState eq primaryStateFence.committedState
			(30, 3, false, 3592691889253758326L, false, 1, 5, 1, 0),
			(30, 5, false, 2968913177794423906L, false, 1, 5, 1, 0),
			(30, 4, false, 1456932037162721701L, false, 1, 5, 1, 0),
		)

		failingCases.foldLeft(Future.successful(())) { (previousResult, failingCase) =>
			previousResult.flatMap { _ =>
				val (numberOfCommandsToSend, clusterSize, startWithHighestPriorityParticipant, netRandomnessSeed, remembersLastAppliedCommandIndex, maxRecursionDepth, logCompactionThreshold, maxInFlightAppendsPerPeer, logRetentionAfterSnapshot) = failingCase
				val net = new Net(clusterSize, randomnessSeed = netRandomnessSeed, requestFailurePercentage = 10, responseFailurePercentage = 10)
				testAllInvariants(
					net,
					startWithHighestPriorityParticipant,
					remembersLastAppliedCommandIndex = remembersLastAppliedCommandIndex,
					numberOfCommandsToSend = numberOfCommandsToSend,
					maxRetries = 15,
					retirementDriveRetryPeriod = clusterSize * 10,
					unreachableFollowersRetryPeriod = clusterSize * 10,
					quiescenceAuthorizationRetryPeriod = clusterSize * 10,
					configChangeRetryPeriod = clusterSize * 100,
					MAX_RECURSION_DEPTH = maxRecursionDepth,
					logCompactionThreshold = logCompactionThreshold,
					maxInFlightAppendsPerPeer = maxInFlightAppendsPerPeer,
					logRetentionAfterSnapshot = logRetentionAfterSnapshot
				)
			}
		}
	}
	// clusterSize=6, initialConfig=[false, true, false, true, false, true], startWithHighestPriorityParticipant=false, netRandomnessSeed=-7096462650479832304, remembersLastAppliedCommandIndex=false, maxRecursionDepth=9, logCompactionThreshold=3, maxInFlightAppendsPerPeer=1, logRetentionAfterSnapshot=0

	// A specific test run with a fixed random seed and configuration to debug or analyze particular scenarios.
	test("All invariants special case") {
		val (numberOfCommandsToSend, clusterSize, startWithHighestPriorityParticipant, netRandomnessSeed, remembersLastAppliedCommandIndex, maxRecursionDepth, logCompactionThreshold, maxInFlightAppendsPerPeer, logRetentionAfterSnapshot) =
			(30, 3, true, 1494279300139860962L, false, 0, 5, 9, 0)
		val net = new Net(clusterSize, randomnessSeed = netRandomnessSeed, requestFailurePercentage = 10, responseFailurePercentage = 10)
		testAllInvariants(
			net,
			startWithHighestPriorityParticipant,
			remembersLastAppliedCommandIndex = remembersLastAppliedCommandIndex,
			numberOfCommandsToSend = numberOfCommandsToSend,
			maxRetries = 15,
			retirementDriveRetryPeriod = clusterSize * 10,
			unreachableFollowersRetryPeriod = clusterSize * 10,
			quiescenceAuthorizationRetryPeriod = clusterSize * 10,
			configChangeRetryPeriod = clusterSize * 100,
			MAX_RECURSION_DEPTH = maxRecursionDepth,
			logCompactionThreshold = logCompactionThreshold,
			maxInFlightAppendsPerPeer = maxInFlightAppendsPerPeer,
			logRetentionAfterSnapshot = logRetentionAfterSnapshot
		)
	}

	// A property-based test that runs many simulations with varying cluster sizes, starting participants, and random seeds.
	test("All invariants must comply") {
		// Thread.sleep(20000)
		inline val numberOfCommandsToSend = 30
		PropF.forAllNoShrinkF(
			Gen.choose(1, 3), // 1, 2, 3; 2, 4, 6; 3, 6, 9; 4, 8, 12; 5, 10, 15
			Gen.choose(1, 2),
			Gen.oneOf(true, false),
			Gen.long,
			genNodeConfig
		) { (clusterSize1, clusterSize2, startWithHighestPriorityParticipant, netRandomnessSeed, nodeConfig) =>
			val clusterSize = clusterSize1 * clusterSize2
			val net = new Net(clusterSize, randomnessSeed = netRandomnessSeed, requestFailurePercentage = 10, responseFailurePercentage = 10)
			testAllInvariants(
				net,
				startWithHighestPriorityParticipant,
				remembersLastAppliedCommandIndex = nodeConfig.remembersLastAppliedCommandIndex,
				numberOfCommandsToSend = numberOfCommandsToSend,
				maxRetries = 15,
				retirementDriveRetryPeriod = clusterSize * 10,
				unreachableFollowersRetryPeriod = clusterSize * 10,
				quiescenceAuthorizationRetryPeriod = clusterSize * 10,
				configChangeRetryPeriod = clusterSize * 100,
				MAX_RECURSION_DEPTH = nodeConfig.maxRecursionDepth,
				logCompactionThreshold = nodeConfig.logCompactionThreshold,
				maxInFlightAppendsPerPeer = nodeConfig.maxInFlightAppendsPerPeer,
				logRetentionAfterSnapshot = nodeConfig.logRetentionAfterSnapshot
			)
		}
	}
}
