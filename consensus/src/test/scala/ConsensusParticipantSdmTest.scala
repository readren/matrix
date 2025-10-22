package readren.consensus

import ConsensusParticipantSdm.*

import munit.ScalaCheckEffectSuite
import org.scalacheck.Gen
import org.scalacheck.effect.PropF
import readren.common.{Maybe, ScribeConfig}
import readren.sequencer.Doer
import readren.sequencer.providers.CooperativeWorkersWithAsyncSchedulerDp

import java.util.concurrent.{Executors, TimeUnit}
import scala.collection.{mutable, IndexedSeq as GenIndexedSeq}
import scala.compiletime.uninitialized
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Promise}
import scala.reflect.ClassTag
import scala.util.{Failure, Random, Success, Try}

class ConsensusParticipantSdmTest extends ScalaCheckEffectSuite {

	ScribeConfig.init(true)

	override def scalaCheckInitialSeed = "mLbrswnMGqQ8czetIVPfw3Wh8rh8m-nkAjknVe4oiUE="

	override def munitTimeout: Duration = new FiniteDuration(200, TimeUnit.SECONDS)

	private given ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(4))

	/** Id of a [[Node]]. Implements [[ConsensusParticipantSdm.ParticipantId]]. */
	private type Id = String

	private def nameOf(ordinal: BehaviorOrdinal): String = {
		ordinal match {
			case STOPPED => "STOPPED"
			case STARTING => "STARTING"
			case ISOLATED => "ISOLATED"
			case CANDIDATE => "CANDIDATE"
			case FOLLOWER => "FOLLOWER"
			case LEADER => "LEADER"
		}
	}

	// Test command type
	private case class TestCommand(value: String)

	/** The provider of all the [[Doer]] instances used by this testing infrastructure. */
	private val sharedDap = new CooperativeWorkersWithAsyncSchedulerDp.Impl(
		failureReporter = (doer, e) => scribe.error(s"Failure reported by a task executed by the sequencer tagged with ${doer.tag}", e),
		unhandledExceptionReporter = (doer, e) => scribe.error(s"Unhandled exception in a task executed by the sequencer tagged with ${doer.tag}")
	)

	private type ScheduSequen = CooperativeWorkersWithAsyncSchedulerDp.SchedulingDoerFacade

	/** Simulates a net of nodes for testing. */
	private class Net(val size: Int, randomnessSeed: Long) {
		private val MAX_TRAVELING_MESSAGES = size * 2
		private inline val SILENCE_MAX_DURATION = 5
		private inline val REQUEST_FAILURE_PERCENTAGE = 10
		private inline val RESPONSE_FAILURE_PERCENTAGE = 10
		private val failureMaxDurationSqrt = size - 1

		val netSequencer: ScheduSequen = sharedDap.provide("net-sequencer")
		private var indexById: Map[Id, Int] = Map.empty
		private val nodeByIndex: Array[Node | Null] = new Array(size)

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

		private type RequestId = (global: Int, channel: Int)

		/** A communication channel between two [[Node]]s.
		 * Mimics a TCP channel by maintaining delivery order. */
		private case class Channel() {
			private val queue: mutable.Queue[netSequencer.Duty[Unit]] = mutable.Queue.empty
			private var lastRequestId = 0
			private var failingUntil: Int = 0

			inline def nextRequestId: RequestId = {
				lastRequestId += 1
				lastGlobalRequestId += 1
				(lastGlobalRequestId, lastRequestId)
			}

			inline def enqueue(duty: netSequencer.Duty[Unit]): Unit = {
				queue.enqueue(duty)
				travelingMessages += 1
			}

			inline def nonEmpty: Boolean = queue.nonEmpty

			inline def dispatchNext(): Unit = {
				travelingMessages -= 1
				queue.dequeue().triggerAndForget(true)
			}

			inline def markAsFailing(durationSqrt: Int): Unit = {
				failingUntil = lastGlobalRequestId + durationSqrt * durationSqrt
			}

			inline def isFailing: Boolean = lastGlobalRequestId <= failingUntil
		}

		private val random = new Random(randomnessSeed)
		private val channelBySenderByReceiver: Array[Array[Channel]] = Array.fill(size, size)(Channel())
		private var travelingMessages: Int = 0
		private var lastProcessTimeoutSchedule: Maybe[netSequencer.Schedule] = Maybe.empty
		private var lastGlobalRequestId: Int = 0

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
			def rpc[R](replierId: Id, requestDescription: String)(call: (replierNode: Node) => replierNode.sequencer.Task[R]): netSequencer.Task[R] = {

				if true then {
					val inquirerIndex = indexOf(inquirerId)
					val replierIndex = indexOf(replierId)
					val inquirerNode = getNode(inquirerIndex)
					assert(inquirerNode.sequencer.isInSequence)
					val inquirerRole = nameOf(inquirerNode.participant.getBehaviorOrdinal)
					val covenant = netSequencer.Covenant[(Try[R], RequestId)]()
					netSequencer.execute {
						lastProcessTimeoutSchedule.foreach(netSequencer.cancel)

						val requestChannel = channelBySenderByReceiver(inquirerIndex)(replierIndex)
						val responseChannel = channelBySenderByReceiver(replierIndex)(inquirerIndex)
						val requestId = requestChannel.nextRequestId
						val requestChannelIsFailing = requestChannel.isFailing
						val responseChannelIsFailing = responseChannel.isFailing
						assert(requestChannelIsFailing == responseChannelIsFailing)

						scribe.trace(s"$inquirerId >- $replierId: $requestId:$requestDescription as $inquirerRole while there were $travelingMessages messages on the way")

						// Determine the fate of all the stages of this RPC here, in the first stage.
						val requestIsCursed = requestChannelIsFailing || responseChannelIsFailing || random.nextInt(100) < REQUEST_FAILURE_PERCENTAGE
						val responseIsCursed = requestIsCursed || random.nextInt(100) < RESPONSE_FAILURE_PERCENTAGE
						if !requestChannelIsFailing && (requestIsCursed || responseIsCursed) then {
							val failureDurationSqrt = random.nextInt(failureMaxDurationSqrt)
							requestChannel.markAsFailing(failureDurationSqrt)
							responseChannel.markAsFailing(failureDurationSqrt)
						}
						val replierNode = getNode(replierId)
						val requestingDuty =
							if requestIsCursed then {
								netSequencer.Duty_mine[Unit] { () =>
									covenant.fulfill((Failure(new RuntimeException(s"Net: simulated failure of request $requestId")), requestId))()
								}
							} else {
								netSequencer.Task_foreign(replierNode.sequencer)(replierNode.sequencer.Task_ownFlat { () =>
									val replierRole = nameOf(replierNode.participant.getBehaviorOrdinal)
									scribe.trace(s"$inquirerId -> $replierId: $requestId:$requestDescription as $replierRole")
									call(replierNode)
								}).transform { reply =>
									val response =
										if responseIsCursed then Failure(new RuntimeException(s"Net: simulated failure of response $requestId"))
										else reply
									val respondingDuty = netSequencer.Duty_mine[Unit](() => covenant.fulfill((response, requestId))())
									responseChannel.enqueue(respondingDuty)
									Doer.successUnit
								}.toDuty(_ => ())
							}
						requestChannel.enqueue(requestingDuty)

						while travelingMessages > MAX_TRAVELING_MESSAGES do chooseAChannel().dispatchNext()

						def dispatchAMessageIfLongSilence(): Unit = {
							val processTimeoutSchedule = netSequencer.newDelaySchedule(SILENCE_MAX_DURATION)
							lastProcessTimeoutSchedule = Maybe.some(processTimeoutSchedule)
							netSequencer.schedule(processTimeoutSchedule) { _ =>
								if travelingMessages > 0 then {
									// scribe.trace(s"A long silence occurred with $travelingMessages messages on the way. Dispatching one of them.")
									chooseAChannel().dispatchNext()
									dispatchAMessageIfLongSilence()
								}
							}
						}

						dispatchAMessageIfLongSilence()
					}

					netSequencer.Task_fromDuty(covenant.map {
						case (response, requestId) =>
							scribe.trace(s"$inquirerId <- $replierId: $requestId:$response, leaving $travelingMessages messages on the way")
							response
					})

				} else {
					/// Simple implementation that always succeeds and adds no randomness
					val replierNode = getNode(replierId)
					replierNode.sequencer.Task_ownFlat[R] { () =>
						call(replierNode)
					}.onBehalfOf(netSequencer)
				}
			}
		}

		private def chooseAChannel(): Channel = {
			val alternatives: mutable.Buffer[Channel] = mutable.Buffer.empty
			for i <- 0 until size do {
				for j <- 0 until size do {
					val channel = channelBySenderByReceiver(i)(j)
					if channel.nonEmpty then alternatives.addOne(channel)
				}
			}
			alternatives(random.between(0, alternatives.size))
		}
	}

	/**
	 * Simulates a client.
	 * @param net The network to use.
	 * @param initialReceiverIndex The initial receiver index. */
	private class Client[N <: Net](val net: N, initialReceiverIndex: Int) {
		private var receiverIndex: Int = initialReceiverIndex

		/**
		 * Sends a command to a [[Node]] of the net.
		 * Initially, the target [[Node]] is the [[Node]] at the [[initialReceiverIndex]].
		 * If the target [[Node]] responds with a [[RedirectTo]] the target [[Node]] is updated to the redirected [[Node]] and following commands are sent to the new target [[Node]].
		 * If the target [[Node]] responds with an [[Unable]] the target [[Node]] is updated to the next [[Node]] of the [[Net]] and the command is retried to it.
		 * If the target [[Node]] responds with a [[Processed]] the command is considered processed and the target [[Node]] is not updated.
		 * @param command The command to send.
		 * @param isFallback Whether the command is a fallback command.
		 * @return A task that completes with true if the command was processed; false if the command was not processed despite all nodes were tried or the net is empty. */
		def sendsCommand(command: String, isFallback: Boolean = false, retriesCounter: Int = 0): net.netSequencer.Task[Boolean] = {
			if net.size == 0 then return net.netSequencer.Task_successful(false)
			if receiverIndex < 0 then receiverIndex = net.size - 1
			val receiverNode = net.getNode(receiverIndex)
			scribe.info(s"Client: About to send command:$command, isFallback:$isFallback, retriesCounter:$retriesCounter, to:${receiverNode.myId}")

			def retry(): receiverNode.sequencer.Task[Boolean] = {
				receiverIndex -= 1
				if retriesCounter > net.size then receiverNode.sequencer.Task_successful(false)
				else sendsCommand(command, true, retriesCounter + 1).onBehalfOf(receiverNode.sequencer)
			}

			receiverNode.sequencer.Task_ownFlat { () =>
				val receiverBehavior = receiverNode.participant.getBehaviorOrdinal
				receiverNode.clusterParticipant.messagesListener.onCommandFromClient(TestCommand(command), isFallback)
					.transformWith[Boolean] {
						case Success(receiverNode.Processed(index, content)) =>
							scribe.info(s"Client: command `$command` was processed @$index by ${receiverNode.myId} which replied with `$content`.")
							receiverNode.sequencer.Task_successful(true)
						case Success(receiverNode.RedirectTo(leaderId)) =>
							scribe.info(s"Client: the follower ${receiverNode.myId} redirected the command `$command` to the leader $leaderId.")
							receiverIndex = net.indexOf(leaderId)
							sendsCommand(command).onBehalfOf(receiverNode.sequencer)
						case Success(receiverNode.Unable(behaviorOrdinal, otherParticipants)) =>
							scribe.info(s"Client: the participant ${receiverNode.myId}(role:$receiverBehavior) is unable (retries=$retriesCounter) to process the command `$command`.")
							retry()
						case Failure(e) =>
							scribe.info(s"Client: the participant ${receiverNode.myId} failed (retries=$retriesCounter) to respond to the command `$command`:", e)
							retry()
					}
			}.onBehalfOf(net.netSequencer)
		}

		def sendsCommandsUntil(predicate: (commandIndex: Int) => Boolean, maxRetries: Int = 9): net.netSequencer.Task[Unit] = {

			def sendCommandLoop(commandIndex: Int, retriesCounter: Int): net.netSequencer.Task[Unit] = {
				if predicate.apply(commandIndex) then net.netSequencer.Task_unit
				else if retriesCounter > maxRetries then net.netSequencer.Task_failed(new AssertionError("The cluster got stuck unable to progress"))
				else for {
					wasProcessed <- sendsCommand(s"command#$commandIndex")
					_ <- {
						if wasProcessed then sendCommandLoop(commandIndex + 1, 0)
						else sendCommandLoop(commandIndex, retriesCounter + 1)
					}
				} yield ()
			}

			sendCommandLoop(1, 0)
		}

	}

	private trait NodeStateChangesListener {
		/** Called when any entry in the log buffer of a [[Node]] is overwritten.
		 * @param index the [[RecordIndex]] of the first overwritten entry.
		 * @param firstReplacedRecord the first stored [[Record]] that is removed.
		 * @param firstReplacingRecord the [[Record]] with which the first overwritten log entry is replaced with.
		 * @param behaviorOrdinal the behavior of the [[ConsensusParticipant]] when the conflict occurred. */
		def onLogOverwrite(index: RecordIndex, firstReplacedRecord: Record, firstReplacingRecord: Record, behaviorOrdinal: BehaviorOrdinal): Unit = ()

		/** Called when a [[Record]] is appended to the log buffer. */
		def onRecordAppended(record: Record, index: RecordIndex): Unit = ()

		def onCommandApplied(command: TestCommand, index: RecordIndex): Unit = ()
	}

	/**
	 * An implementation of the [[ConsensusParticipantSdm]] for testing.
	 */
	private class Node(val myId: Id, initialParticipants: Set[Id], net: Net) extends ConsensusParticipantSdm { thisNode =>

		import net.rpc

		override type ParticipantId = Id
		override type ClientCommand = TestCommand
		override type StateMachineResponse = String

		var statesChangesListener: NodeStateChangesListener = new NodeStateChangesListener() {
			override def onLogOverwrite(index: RecordIndex, firstReplacedRecord: Record, firstReplacingRecord: Record, behaviorOrdinal: BehaviorOrdinal): Unit = ()
		}

		private var initialNotificationListener: NotificationListener = new DefaultNotificationListener()

		private var _participant: ConsensusParticipant = uninitialized

		/** @return the [[ConsensusParticipant]] service instance corresponding to this [[Node]]. */
		inline def participant: ConsensusParticipant = _participant

		/** Initializes this [[Node]]. Does not start the [[ConsensusParticipant]] service. */
		inline def initialize(initialNotificationListener: NotificationListener = DefaultNotificationListener()): Unit = {
			this.initialNotificationListener = initialNotificationListener;
		}

		/** Creates this [[Node]]'s [[ConsensusParticipant]] service instance. */
		def starts(stateMachineNeedsRestart: Boolean = false): sequencer.Duty[Unit] = {
			sequencer.Duty_mine { () =>
				_participant = ConsensusParticipant(clusterParticipant, storage, machine, List(initialNotificationListener, notificationScribe), stateMachineNeedsRestart)
			}
		}

		def stops(): sequencer.Duty[Unit] = {
			sequencer.Duty_mine { () =>
				participant.stop()
				_participant = null
			}
		}

		override val sequencer: ScheduSequen = sharedDap.provide("node-sequencer")

		object machine extends StateMachine {
			override def appliesClientCommand(index: RecordIndex, command: ClientCommand): sequencer.Task[StateMachineResponse] = {
				// TODO add delay
				sequencer.Task_mine { () =>
					statesChangesListener.onCommandApplied(command, index)
					command.value
				}
			}

			override def recoversIndexOfLastAppliedCommand: sequencer.Task[RecordIndex] = sequencer.Task_successful(0)
		}

		override def isEager: Boolean = false

		/**
		 * Test instance and implementation of the [[ClusterParticipant]] service interface required by the [[participant]] (the [[ConsensusParticipant]] service corresponding to a [[Node]]).
		 */
		object clusterParticipant extends ClusterParticipant {

			override val boundParticipantId: ParticipantId = myId

			var messagesListener: MessagesListener = uninitialized

			override def getInitialParticipants: Set[ParticipantId] = initialParticipants

			override def setMessagesListener(listener: MessagesListener): Unit = {
				messagesListener = listener
			}

			extension (replierId: ParticipantId) {


				def howAreYou(inquirerTerm: Term): sequencer.Task[StateInfo] = {
					boundParticipantId.rpc[StateInfo](
						replierId,
						s"HowAreYou(inquirerTerm:$inquirerTerm)"
					) { replierNode =>
						replierNode.sequencer.Task_successful(replierNode.clusterParticipant.messagesListener.onHowAreYou(boundParticipantId, inquirerTerm))
					}.onBehalfOf(sequencer)
				}

				def chooseALeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.Task[Vote[ParticipantId]] = {
					boundParticipantId.rpc[Vote[ParticipantId]](
						replierId,
						s"ChooseALeader(inquirerId:$inquirerId, inquirerInfo:$inquirerInfo)"
					) { replier =>
						replier.clusterParticipant.messagesListener.onChooseALeader(inquirerId, inquirerInfo).toTask
					}.onBehalfOf(sequencer)
				}

				def appendRecords(inquirerTerm: Term, prevLogIndex: RecordIndex, prevLogTerm: Term, records: GenIndexedSeq[Record], leaderCommit: RecordIndex, termAtLeaderCommit: Term): sequencer.Task[AppendResult] = {
					boundParticipantId.rpc[AppendResult](
						replierId,
						s"AppendRecords(inquirerTerm:$inquirerTerm, previousLogIndex:$prevLogIndex, previousLogTerm;$prevLogTerm, records:$records, leaderCommit:$leaderCommit)"
					) { replier =>
						replier.clusterParticipant.messagesListener.onAppendRecords(boundParticipantId, inquirerTerm, prevLogIndex, prevLogTerm, records, leaderCommit, termAtLeaderCommit)
					}.onBehalfOf(sequencer)
				}
			}
		}

		/**
		 * Test instance and implementation of the [[Storage]] service interface required by the [[participant]] (the [[ConsensusParticipant]] service corresponding to a [[Node]]).
		 */
		object storage extends Storage {
			private[ConsensusParticipantSdmTest] var memory: WS = TestWorkspace()

			override val loads: sequencer.Task[WS] = sequencer.Task_successful(memory) // TODO add a delay

			override def saves(workspace: WS): sequencer.Task[Unit] = {
				memory = workspace
				sequencer.Task_unit // TODO add a delay
			}
		}

		override type WS = TestWorkspace

		/**
		 * Test implementation of [[Workspace]]
		 */
		class TestWorkspace extends Workspace {
			private var currentParticipants: IArray[ParticipantId] = IArray.empty
			private var currentTerm: Term = 0
			private var _logBufferOffset: RecordIndex = 1
			private val logBuffer: mutable.ArrayBuffer[Record] = mutable.ArrayBuffer.empty

			override def isBrandNew: Boolean = logBuffer.isEmpty

			override def getCurrentParticipants: IArray[ParticipantId] = currentParticipants

			override def setCurrentParticipants(participants: IArray[ParticipantId]): Unit = {
				currentParticipants = participants
			}

			override def getCurrentTerm: Term = currentTerm

			override def setCurrentTerm(term: Term): Unit = {
				currentTerm = term
			}

			override def logBufferOffset: RecordIndex = _logBufferOffset

			override def firstEmptyRecordIndex: RecordIndex = _logBufferOffset + logBuffer.size

			override def getRecordAt(index: RecordIndex): Record = logBuffer((index - logBufferOffset).toInt)

			override def getRecordsBetween(from: RecordIndex, until: RecordIndex): GenIndexedSeq[Record] = {
				val fromIndex = (from - logBufferOffset).toInt
				val untilIndex = (until - logBufferOffset).toInt
				logBuffer.slice(fromIndex, untilIndex)
			}

			override def appendRecord(record: Record): Unit = {
				val index = firstEmptyRecordIndex
				logBuffer.addOne(record)
				statesChangesListener.onRecordAppended(record, index)
			}

			override def appendResolvingConflicts(records: GenIndexedSeq[Record], from: RecordIndex): RecordIndex = {
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
					logBuffer.takeInPlace(storedIndex)
					statesChangesListener.onLogOverwrite(storedIndex + logBufferOffset, logBuffer(storedIndex), records(newIndex), participant.getBehaviorOrdinal)
				}
				while newIndex < records.size do {
					appendRecord(records(newIndex))
					newIndex += 1
				}
				logBufferOffset + logBuffer.size - 1
			}

			override def informAppliedCommandIndex(appliedCommandIndex: RecordIndex): Unit = ()

			override def release(): Unit = ()
		}

		object notificationScribe extends NotificationListener {
			override def onStarting(previous: BehaviorOrdinal, term: Term, isRestart: Boolean): Unit = scribe.info(s"$myId: ${if isRestart then "re" else ""}starting...")

			override def onStarted(previous: BehaviorOrdinal, term: Term, isRestart: Boolean): Unit = scribe.info(s"$myId: ${if isRestart then "re" else ""}started.")

			override def onBecameStopped(previous: BehaviorOrdinal, term: Term, motive: Throwable | Null): Unit = scribe.info(s"$myId: became stopped from ${nameOf(previous)} during term $term.", motive)

			override def onBecameIsolated(previous: BehaviorOrdinal, term: Term): Unit = scribe.info(s"$myId: became isolated from ${nameOf(previous)} during term $term.")

			override def onBecameCandidate(previous: BehaviorOrdinal, term: Term): Unit = scribe.info(s"$myId: became candidate from ${nameOf(previous)} during term $term.")

			override def onBecameFollower(previous: BehaviorOrdinal, term: Term, leaderId: Id): Unit = scribe.info(s"$myId: became follower of $leaderId from ${nameOf(previous)} during term $term")

			override def onBecameLeader(previous: BehaviorOrdinal, term: Term): Unit = scribe.info(s"$myId: became leader of term $term from ${nameOf(previous)}")

			override def onLeft(left: BehaviorOrdinal, term: Term): Unit = ()
		}
	}

	/**
	 * Helper to create participant ids.
	 */
	private def createIds(size: Int): IndexedSeq[Id] = {
		(0 until size).map(i => s"p-$i")
	}


	/** Helper to create and initialize [[Node]]s.
	 * @param net the [[Net]] where the created [[Node]] instances will be added.
	 * @param clusterSize the number of [[Node]]s to create.
	 * @param weakReferencesHolder a collection to which the created instances of [[NotificationListener]] are added in order to avoid being garbage-collected.
	 * @param notificationListenerBuilder a function that takes the new [[Node]] and builds the [[NotificationListener]] to be passed its [[ConsensusParticipantSdm.ConsensusParticipant]] service constructor. */
	private def createsAndInitializesNodes[N <: Net](net: N, clusterSize: Int, weakReferencesHolder: mutable.Buffer[AnyRef])(notificationListenerBuilder: (node: Node) => node.NotificationListener): Unit = {
		val ids = createIds(clusterSize)
		for id <- ids do {
			val node = Node(id, ids.toSet, net)
			net.addNode(node)
			val nl = notificationListenerBuilder(node)
			weakReferencesHolder.addOne(nl)
			node.initialize(nl)
		}
	}

	private def startsAllNodes(net: Net): net.netSequencer.Duty[Array[Unit]] = {
		val starters = for nodeIndex <- 0 until net.size yield {
			val node = net.getNode(nodeIndex)
			node.starts(false).onBehalfOf(net.netSequencer)
		}
		net.netSequencer.Duty_sequenceToArray(starters)
	}


	test("Election Safety: at most one leader can be elected in a given term. §5.2") {
		inline val timeout = 20
		inline val numberOfCommandsToSend = 10
		PropF.forAllNoShrinkF(
			Gen.choose(1, 5).flatMap(n => Gen.choose(0, n - 1).map(m => (n, m))),
			Gen.long
		) { (t, netRandomnessSeed) =>
			val (clusterSize, receiverIndex) = t
			scribe.info(s"\nBegin: clusterSize=$clusterSize, receiverIndex=$receiverIndex, netRandomnessSeed=$netRandomnessSeed")

			val promise = Promise[Unit]()
			val net = new Net(clusterSize, netRandomnessSeed)
			val weakReferencesHolder = mutable.Buffer.empty[AnyRef]
			val leaderNodeByTerm: Array[Node] = new Array(numberOfCommandsToSend + 1)

			createsAndInitializesNodes(net, clusterSize, weakReferencesHolder) { node =>
				new node.DefaultNotificationListener() {
					override def onBecameLeader(previous: BehaviorOrdinal, term: Term): Unit = {
						if leaderNodeByTerm(term) eq null then leaderNodeByTerm(term) = node
						else promise.tryFailure(new AssertionError(s"Node ${node.myId} became leader at term $t despite node ${leaderNodeByTerm(term).myId} was a leader of the same term before"))
					}
				}
			}

			val checks = for {
				_ <- startsAllNodes(net)
				client = Client[net.type](net, receiverIndex)
				_ <- client.sendsCommandsUntil(commandIndex => commandIndex > numberOfCommandsToSend || promise.isCompleted)
					.toDuty(promise.tryFailure)
			} yield promise.tryComplete(Success(()))
			checks.triggerAndForget(false)
			promise.future
		}
	}

	test("Leader Append-Only: a leader never overwrites or deletes entries in its log; it only appends new entries. §5.3") {
		inline val numberOfCommandsToSend = 10
		PropF.forAllNoShrinkF(
			Gen.choose(3, 5).flatMap(clusterSize => Gen.choose(0, clusterSize - 1).map(receiverIndex => (clusterSize, receiverIndex))),
			Gen.long,
		) { (clusterConf, netRandomnessSeed) =>
			val (clusterSize, receiverIndex) = clusterConf
			scribe.info(s"\nBegin: clusterSize=$clusterSize, receiverIndex=$receiverIndex, netRandomnessSeed=$netRandomnessSeed")

			val promise = Promise[Unit]()
			val net = new Net(clusterSize, netRandomnessSeed)
			val weakReferencesHolder = mutable.Buffer.empty[AnyRef]

			createsAndInitializesNodes(net, clusterSize, weakReferencesHolder) { node =>
				node.statesChangesListener = new NodeStateChangesListener() {
					override def onLogOverwrite(index: RecordIndex, firstReplacedRecord: Record, firstReplacingRecord: Record, behaviorOrdinal: BehaviorOrdinal): Unit = {
						if behaviorOrdinal == LEADER then promise.tryFailure(new AssertionError(s"The participant ${node.myId} broke the \"append only rule\" at index $index. Removed record: $firstReplacedRecord, replacing record: $firstReplacingRecord."))
					}
				}
				node.DefaultNotificationListener()
			}

			val checks: net.netSequencer.Task[Unit] = for {
				_ <- startsAllNodes(net).toTask
				client = Client[net.type](net, receiverIndex)
				_ <- client.sendsCommandsUntil(commandIndex => commandIndex > numberOfCommandsToSend || promise.isCompleted)
			} yield promise.tryComplete(Success(()))

			checks.trigger(false)(promise.tryComplete)
			promise.future
		}
	}

	test("Log Matching: if two logs contain an entry with the same index and term, then the logs are identical in all entries up through the given index. §5.3") {
		inline val numberOfCommandsToSend = 10

		PropF.forAllNoShrinkF(
			Gen.choose(3, 5).flatMap(clusterSize => Gen.choose(0, clusterSize - 1).map(receiverIndex => (clusterSize, receiverIndex))),
			Gen.long,
		) { (clusterConf, netRandomnessSeed) =>
			val (clusterSize, receiverIndex) = clusterConf
			scribe.info(s"\nBegin: clusterSize=$clusterSize, receiverIndex=$receiverIndex, netRandomnessSeed=$netRandomnessSeed")

			val promise = Promise[Unit]()
			val net = new Net(clusterSize, netRandomnessSeed)
			val weakReferencesHolder = mutable.Buffer.empty[AnyRef]

			createsAndInitializesNodes(net, clusterSize, weakReferencesHolder) { node =>
				node.statesChangesListener = new NodeStateChangesListener() {
					override def onRecordAppended(record: Record, index: RecordIndex): Unit = {

						val thisNodeRecords = node.storage.memory.getRecordsBetween(1, index)

						for nodeIndex <- 0 until clusterSize do {
							val otherNode = net.getNode(nodeIndex)
							if otherNode ne node then {
								otherNode.sequencer.execute {
									if otherNode.storage.memory.firstEmptyRecordIndex > index && otherNode.storage.memory.getRecordAt(index).term == record.term then {
										val otherNodeRecords = otherNode.storage.memory.getRecordsBetween(1, index)
										if otherNodeRecords != thisNodeRecords then promise.tryFailure(new AssertionError(s"The logs of nodes ${node.myId} and ${otherNode.myId} are not identical in all entries up through $index despite the records at $index have the same term."))
									}
								}

							}
						}
					}
				}
				node.DefaultNotificationListener()
			}

			val checks: net.netSequencer.Task[Unit] = for {
				_ <- startsAllNodes(net).toTask
				client = Client[net.type](net, receiverIndex)
				_ <- client.sendsCommandsUntil(commandIndex => commandIndex > numberOfCommandsToSend || promise.isCompleted)
			} yield promise.tryComplete(Success(()))

			checks.trigger(false)(promise.tryComplete)
			promise.future
		}
	}

	test("Leader Completeness: if a log entry is committed in a given term, then that entry will be present in the logs of the leaders for all higher-numbered terms. §5.4") {
		inline val numberOfCommandsToSend = 10

		PropF.forAllNoShrinkF(
			Gen.choose(3, 5).flatMap(clusterSize => Gen.choose(0, clusterSize - 1).map(receiverIndex => (clusterSize, receiverIndex))),
			Gen.long,
		) { (clusterConf, netRandomnessSeed) =>
			val (clusterSize, receiverIndex) = clusterConf
			scribe.info(s"\nBegin: clusterSize=$clusterSize, receiverIndex=$receiverIndex, netRandomnessSeed=$netRandomnessSeed")

			val promise = Promise[Unit]()
			val net = new Net(clusterSize, netRandomnessSeed)
			val weakReferencesHolder = mutable.Buffer.empty[AnyRef]

			createsAndInitializesNodes(net, clusterSize, weakReferencesHolder) { node =>
				node.statesChangesListener = new NodeStateChangesListener() {
					override def onRecordAppended(record: Record, index: RecordIndex): Unit = {

						val thisNodeRecords = node.storage.memory.getRecordsBetween(1, index)

						for nodeIndex <- 0 until clusterSize do {
							val otherNode = net.getNode(nodeIndex)
							if otherNode ne node then {
								otherNode.sequencer.execute {
									if otherNode.storage.memory.firstEmptyRecordIndex > index && otherNode.storage.memory.getRecordAt(index).term == record.term then {
										val otherNodeRecords = otherNode.storage.memory.getRecordsBetween(1, index)
										if otherNodeRecords != thisNodeRecords then promise.tryFailure(new AssertionError(s"The logs of nodes ${node.myId} and ${otherNode.myId} are not identical in all entries up through $index despite the records at $index have the same term."))
									}
								}

							}
						}
					}
				}
				node.DefaultNotificationListener()
			}

			val checks: net.netSequencer.Task[Unit] = for {
				_ <- startsAllNodes(net).toTask
				client = Client[net.type](net, receiverIndex)
				_ <- client.sendsCommandsUntil(commandIndex => commandIndex > numberOfCommandsToSend || promise.isCompleted)
			} yield promise.tryComplete(Success(()))

			checks.trigger(false)(promise.tryComplete)
			promise.future
		}
	}

	test("State Machine Safety: if a server has applied a log entry at a given index to its state machine, no other server will ever apply a different log entry for the same index. §5.4.3") {
		inline val numberOfCommandsToSend = 10

		PropF.forAllNoShrinkF(
			Gen.choose(3, 5).flatMap(clusterSize => Gen.choose(0, clusterSize - 1).map(receiverIndex => (clusterSize, receiverIndex))),
			Gen.long
		) { (clusterConf, netRandomnessSeed) =>
			val (clusterSize, receiverIndex) = clusterConf
			scribe.info(s"\nBegin: clusterSize=$clusterSize, receiverIndex=$receiverIndex, netRandomnessSeed=$netRandomnessSeed")

			val promise = Promise[Unit]()
			val net = new Net(clusterSize, netRandomnessSeed)
			val weakReferencesHolder = mutable.Buffer.empty[AnyRef]
			val appliedCommandsByNodeIndex: Array[Array[TestCommand | Null]] = Array.fill(clusterSize, numberOfCommandsToSend + 1)(null)

			createsAndInitializesNodes(net, clusterSize, weakReferencesHolder) { node =>
				node.statesChangesListener = new NodeStateChangesListener() {
					override def onCommandApplied(command: node.ClientCommand, index: RecordIndex): Unit = {

						val commandsAppliedToThisNode = appliedCommandsByNodeIndex(net.indexOf(node.myId))
						val previouslyAppliedCommand = commandsAppliedToThisNode(index.toInt)
						if previouslyAppliedCommand ne null then promise.tryFailure(new AssertionError(s"Two commands with same index were applied to the node ${node.myId}: previous=$previouslyAppliedCommand, new=$command"))
						var i = index.toInt
						while i < numberOfCommandsToSend do {
							i += 1
							if commandsAppliedToThisNode(i) ne null then promise.tryFailure(new AssertionError(s"The command $command was applied with index $index which is less than the index $i of the previously applied command ${commandsAppliedToThisNode(i)}."))
						}
						commandsAppliedToThisNode(index.toInt) = command

						for nodeIndex <- 0 until clusterSize do {
							val otherNode = net.getNode(nodeIndex)
							if otherNode ne node then {
								otherNode.sequencer.execute {
									val commandsAppliedToTheOtherNode = appliedCommandsByNodeIndex(net.indexOf(otherNode.myId))
									val commandAppliedToTheOtherNodeAtIndex = commandsAppliedToTheOtherNode(index.toInt)
									if (commandAppliedToTheOtherNodeAtIndex ne null) && commandAppliedToTheOtherNodeAtIndex != command then
										promise.tryFailure(new AssertionError(s"The node ${node.myId} applied the command $command at index $index, which is different from the command $commandAppliedToTheOtherNodeAtIndex applied at the same index in node ${otherNode.myId}."))

								}

							}
						}
					}
				}
				node.DefaultNotificationListener()
			}

			val checks: net.netSequencer.Task[Unit] = for {
				_ <- startsAllNodes(net).toTask
				client = Client[net.type](net, receiverIndex)
				_ <- client.sendsCommandsUntil(commandIndex => commandIndex > numberOfCommandsToSend || promise.isCompleted)
			} yield promise.tryComplete(Success(()))

			checks.trigger(false)(promise.tryComplete)
			promise.future
		}
	}
}