package readren.consensus

import ConsensusParticipantSdm.{AppendResult, BehaviorOrdinal, LEADER, Record, RecordIndex, StateInfo, Term, Vote}

import munit.ScalaCheckEffectSuite
import org.scalacheck.Gen
import org.scalacheck.effect.PropF
import readren.sequencer.MilliDuration
import readren.sequencer.providers.CooperativeWorkersWithAsyncSchedulerDp

import java.util.concurrent.Executors
import scala.collection.mutable.ArrayBuffer
import scala.collection.{mutable, IndexedSeq as GenIndexedSeq}
import scala.compiletime.uninitialized
import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

class ConsensusParticipantSdmTest extends ScalaCheckEffectSuite {

	private given ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(4))

	/** Id of a [[Node]]. Implements [[ConsensusParticipantSdm.ParticipantId]]. */
	private type Id = String

	// Test command type
	private case class TestCommand(value: String)

	/** The provider of all the [[Doer]] instances used by this testing infrastructure. */
	private val sharedDap = new CooperativeWorkersWithAsyncSchedulerDp.Impl(
		failureReporter = (doer, e) => scribe.error(s"Failure reported by a task executed by the sequencer tagged with ${doer.tag}", e),
		unhandledExceptionReporter = (doer, e) => scribe.error(s"Unhandled exception in a task executed by the sequencer tagged with ${doer.tag}")
	)

	private type ScheduSequen = CooperativeWorkersWithAsyncSchedulerDp.SchedulingDoerFacade

	/** Knows the set of [[Node]]s involved in the test. */
	private class Net(latency: MilliDuration, timeout: MilliDuration) {
		val netSequencer: ScheduSequen = sharedDap.provide("net-sequencer")
		private var nodeById: Map[Id, Node] = Map.empty
		private var indexById: Map[Id, Int] = Map.empty
		private val nodeByIndex: ArrayBuffer[Node] = ArrayBuffer.empty

		def addNode(id: Id, node: Node): Unit = synchronized {
			nodeById += id -> node
			indexById += id -> nodeByIndex.size
			nodeByIndex.addOne(node)
		}

		def size: Int = synchronized(nodeById.size)

		def getNode(index: Int): Node = synchronized {
			nodeByIndex(index)
		}

		def getNode(id: String): Option[Node] = synchronized {
			nodeById.get(id)
		}

		def indexOf(id: String): Int = synchronized {
			indexById(id)
		}

		extension (inquirerId: Id) {
			/** @return a [[netSequencer.Task]] that yields the [[Node]] with the specified [[Id]] simulating latency, or a [[RuntimeException]] if this [[Net]] know no [[Node]] with that [[Id]] simulating timeout dealy. */
			def accessNode(id: Id): netSequencer.Task[Node] = {
				netSequencer.Task_ownFlat { () =>
					getNode(id).fold {
						netSequencer.Task_failed(new RuntimeException(s"$inquirerId: no response from $id")).scheduled(netSequencer.newDelaySchedule(timeout))
					} { node =>
						netSequencer.Task_successful(node).scheduled(netSequencer.newDelaySchedule(latency))
					}
				}
			}
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
			if net.size == 0 then net.netSequencer.Task_successful(false)
			if receiverIndex < 0 then receiverIndex = net.size - 1
			val receiverNode = net.getNode(receiverIndex)

			def retry(): receiverNode.sequencer.Task[Boolean] = {
				receiverIndex -= 1
				if retriesCounter > net.size then receiverNode.sequencer.Task_successful(false)
				else sendsCommand(command, true, retriesCounter + 1).onBehalfOf(receiverNode.sequencer)
			}

			receiverNode.sequencer.Task_ownFlat { () =>
				val receiverBehavior = receiverNode.participant.getBehaviorOrdinal
				receiverNode.cluster.messagesListener.onCommandFromClient(TestCommand(command), isFallback)
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
						case Success(_) =>
							throw new AssertionError("unreachable")
					}
			}.onBehalfOf(net.netSequencer)
		}

	}

	/**
	 * An implementation of the [[ConsensusParticipantSdm]] for testing.
	 */
	private class Node(val myId: Id, participantsIds: Set[Id], net: Net, stateMachineNeedsRestart: Boolean = false) extends ConsensusParticipantSdm { thisNode =>
		assert(participantsIds.contains(myId))

		import net.accessNode

		override type ParticipantId = Id
		override type ClientCommand = TestCommand
		override type StateMachineResponse = String

		var appendRecordListener: AppendRecordListener = new AppendRecordListener() {
			override def onLogOverwrite(index: RecordIndex, firstReplacedRecord: Record, firstReplacingRecord: Record, behaviorOrdinal: BehaviorOrdinal): Unit = ()
		}

		private var _participant: ConsensusParticipant = uninitialized

		/** @return the [[ConsensusParticipant]] service instance corresponding to this [[Node]]. */
		inline def participant: ConsensusParticipant = _participant

		/** Initializes this [[Node]], adding it to the `net` and creating its [[ConsensusParticipant]] service instance. */
		def initializes(initialNotificationListener: NotificationListener = DefaultNotificationListener()): sequencer.Duty[Unit] = {
			net.addNode(myId, thisNode)
			sequencer.Duty_mine { () =>
				_participant = ConsensusParticipant(cluster, storage, machine, List(initialNotificationListener, notificationScribe), stateMachineNeedsRestart)
			}
		}


		/** Initializes this [[Node]] and waits its [[ConsensusParticipant]] service instance to be started. */
		def starts(initialNotificationListener: NotificationListener = DefaultNotificationListener()): sequencer.Duty[Unit] = {
			net.addNode(myId, thisNode)
			sequencer.Duty_mineFlat { () =>
				val startedCovenant = sequencer.Covenant[Unit]
				val startedListener = new DefaultNotificationListener() {
					override def onStarted(previous: BehaviorOrdinal, term: Term, isRestart: Boolean): Unit = if !isRestart then startedCovenant.fulfill(())(_ => ())
				}
				_participant = ConsensusParticipant(cluster, storage, machine, List(startedListener, initialNotificationListener, notificationScribe), stateMachineNeedsRestart)
				startedCovenant
			}
		}

		override val sequencer: ScheduSequen = sharedDap.provide("node-sequencer")

		object machine extends StateMachine {
			override def appliesClientCommand(index: RecordIndex, command: ClientCommand): sequencer.Task[StateMachineResponse] = {
				sequencer.Task_successful(command.value) // TODO add delay
			}
		}

		override def isEager: Boolean = false

		/**
		 * Test instance and implementation of the [[Cluster]] service interface required by the [[participant]] (the [[ConsensusParticipant]] service corresponding to a [[Node]]).
		 */
		object cluster extends Cluster {

			override val hostId: ParticipantId = myId

			var messagesListener: MessagesListener = uninitialized

			override def getParticipants: Set[ParticipantId] = participantsIds

			override def setMessagesListener(listener: MessagesListener): Unit = {
				messagesListener = listener
			}

			extension (destinationId: ParticipantId) {
				def asksHowAreYou(inquirerTerm: Term, aLeaderMaybeMissing: Boolean): sequencer.Task[StateInfo] = {
					val result =
						for {
							replier <- hostId.accessNode(destinationId).onBehalfOf(sequencer)
							si <- replier.sequencer.Task_mine { () =>
								replier.cluster.messagesListener.onHowAreYou(hostId, inquirerTerm)
							}.onBehalfOf(sequencer)
						} yield si

					result.andThen { response =>
						scribe.info(s"$hostId: $destinationId.asksHowAreYou(inquirerTerm:$inquirerTerm, leaderMissing:$aLeaderMaybeMissing) returned $response")
					}
				}

				def askToChooseForLeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.Task[Vote[ParticipantId]] = {
					val result =
						for {
							replier <- hostId.accessNode(destinationId).onBehalfOf(sequencer)
							vote <- replier.sequencer.Task_ownFlat { () =>
								replier.cluster.messagesListener.onChooseALeader(destinationId, inquirerInfo)
							}.onBehalfOf(sequencer)
						} yield vote
					result.andThen { response =>
						scribe.info(s"$hostId: $destinationId.askToChooseForLeader(inquirerId:$inquirerId, inquirerInfo:$inquirerInfo) returned $response")
					}
				}

				def asksToAppendRecords(inquirerTerm: Term, prevLogIndex: RecordIndex, prevLogTerm: Term, records: GenIndexedSeq[Record], leaderCommit: RecordIndex): sequencer.Task[AppendResult] = {
					scribe.info(s"$myId: Asking $destinationId to append the records: inquirerTerm=$inquirerTerm, prevLogIndex=$prevLogIndex, prevLogTerm=$prevLogTerm, records=$records, leaderCommit=$leaderCommit")
					val result =
						for {
							replier <- hostId.accessNode(destinationId).onBehalfOf(sequencer)
							ar <- replier.sequencer.Task_ownFlat { () =>
								replier.cluster.messagesListener.onAppendRecords(hostId, inquirerTerm, prevLogIndex, prevLogTerm, records, leaderCommit)
							}.onBehalfOf(sequencer)
						} yield ar
					result.andThen { response =>
						scribe.info(s"$myId: $destinationId.asksToAppendRecords(inquirerTerm:$inquirerTerm, previousLogIndex:$prevLogIndex, previousLogTerm;$prevLogTerm, records:$records, leaderCommit:$leaderCommit) returned $response")
					}
				}
			}
		}

		/**
		 * Test instance and implementation of the [[Storage]] service interface required by the [[participant]] (the [[ConsensusParticipant]] service corresponding to a [[Node]]).
		 */
		object storage extends Storage {
			override def invalidWorkspace(): WS = InvalidWorkspace

			private var memory: WS = ValidWorkspace()

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
		sealed trait TestWorkspace extends Workspace

		object InvalidWorkspace extends InvalidWorkspacePi, TestWorkspace

		class ValidWorkspace extends TestWorkspace {
			private var currentParticipants: IndexedSeq[ParticipantId] = IndexedSeq.empty
			private var currentTerm: Term = 0
			private var _logBufferOffset: RecordIndex = 1
			private val logBuffer: mutable.ArrayBuffer[Record] = mutable.ArrayBuffer.empty

			override def isBrandNew: Boolean = logBuffer.isEmpty

			override def getCurrentParticipants: IndexedSeq[ParticipantId] = currentParticipants

			override def setCurrentParticipants(participants: IndexedSeq[ParticipantId]): Unit = {
				currentParticipants = participants
			}

			override def getCurrentTerm: Term = currentTerm

			override def setCurrentTerm(term: Term): Unit = {
				currentTerm = term
			}

			override def logBufferOffset: RecordIndex = _logBufferOffset

			override def firstEmptyRecordIndex: RecordIndex = _logBufferOffset + logBuffer.size

			override def lastAppendedRecord: Record = logBuffer.last

			override def getRecordAt(index: RecordIndex): Record = logBuffer((index - logBufferOffset).toInt)

			override def getRecordsBetween(from: RecordIndex, until: RecordIndex): GenIndexedSeq[Record] = {
				val fromIndex = (from - logBufferOffset).toInt
				val untilIndex = (until - logBufferOffset).toInt
				logBuffer.slice(fromIndex, untilIndex)
			}

			override def appendRecord(record: Record): Unit = {
				logBuffer += record
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
					appendRecordListener.onLogOverwrite(storedIndex + logBufferOffset, logBuffer(storedIndex), records(newIndex), participant.getBehaviorOrdinal)
				}
				while newIndex < records.size do {
					logBuffer += records(newIndex)
					newIndex += 1
				}
				logBufferOffset + logBuffer.size - 1
			}

			override def informCommitIndex(commitIndex: RecordIndex): Unit = ()

			override def release(): Unit = ()
		}

		trait AppendRecordListener {
			/** Called when any entry in the log buffer of a [[Node]] is overwritten.
			 * @param index the [[RecordIndex]] of the first overwritten entry.
			 * @param firstReplacedRecord the first stored [[Record]] that is removed.
			 * @param firstReplacingRecord the [[Record]] with which the first overwritten log entry is replaced with.
			 * @param behaviorOrdinal the behavior of the [[ConsensusParticipant]] when the conflict occurred. */
			def onLogOverwrite(index: RecordIndex, firstReplacedRecord: Record, firstReplacingRecord: Record, behaviorOrdinal: BehaviorOrdinal): Unit
		}

		object notificationScribe extends NotificationListener {
			override def onStarting(previous: BehaviorOrdinal, term: Term, isRestart: Boolean): Unit = scribe.info(s"$myId: ${if isRestart then "re" else ""}starting...")

			override def onStarted(previous: BehaviorOrdinal, term: Term, isRestart: Boolean): Unit = scribe.info(s"$myId: ${if isRestart then "re" else ""}started.")

			override def onBecameStopped(previous: BehaviorOrdinal, term: Term, motive: Throwable): Unit = scribe.info(s"$myId: became stoped during term $term.", motive)

			override def onBecameIsolated(previous: BehaviorOrdinal, term: Term): Unit = scribe.info(s"$myId: became isolated from $previous during term $term.")

			override def onBecameCandidate(previous: BehaviorOrdinal, term: Term): Unit = scribe.info(s"$myId: became candidate during term $term.")

			override def onBecameFollower(previous: BehaviorOrdinal, term: Term, leaderId: Id): Unit = scribe.info(s"$myId: became follower of $leaderId during term $term")

			override def onBecameLeader(previous: BehaviorOrdinal, term: Term): Unit = scribe.info(s"$myId: became leader of term $term")

			override def onLeft(left: BehaviorOrdinal, term: Term): Unit = ()
		}
	}


	/**
	 * Helper to create participant ids.
	 */
	private def createIds(size: Int): IndexedSeq[Id] = {
		(1 to size).map(i => s"participant-$i")
	}


	/** Helper to create and initialize [[Node]]s.
	 * @param net the [[Net]] where the created [[Node]] instances will be added.
	 * @param clusterSize the number of [[Node]]s to create.
	 * @param weakReferencesHolder a collection to which the created instances of [[NotificationListener]] are added in order to avoid being garbage-collected.
	 * @param notificationListenerBuilder a function that takes the new [[Node]] and builds the [[NotificationListener]] to be passed its [[ConsensusParticipantSdm.ConsensusParticipant]] service constructor.
	 * @return a [[Duty]] that completes when the creation and initialization is complete. */
	private def createsAndInitializesNodes[N <: Net](net: N, clusterSize: Int, weakReferencesHolder: mutable.Buffer[AnyRef])(notificationListenerBuilder: (node: Node) => node.NotificationListener): net.netSequencer.Duty[Array[Unit]] = {
		val ids = createIds(clusterSize)
		val nodes = for id <- ids yield Node(id, ids.toSet, net)
		val nodeInitializerByIndex =
			for node <- nodes yield {
				val nl = notificationListenerBuilder(node)
				weakReferencesHolder.addOne(nl)
				node.initializes(initialNotificationListener = nl).onBehalfOf(net.netSequencer)
			}
		net.netSequencer.Duty_sequenceToArray(nodeInitializerByIndex)
	}


	test("Election Safety - at most one leader can be elected in a given term") {
		inline val timeout = 20
		inline val numberOfCommandsToSend = 10
		PropF.forAllNoShrinkF(
			Gen.choose(1, 5).flatMap(n => Gen.choose(0, n - 1).map(m => (n, m))),
			Gen.choose(1, timeout + 1 + timeout / 10)
		) { (t, latency) =>
			val (clusterSize, receiverIndex) = t
			scribe.info(s"\nBegin: clusterSize=$clusterSize, receiverIndex=$receiverIndex, latency=$latency")
			val net = new Net(latency, timeout)
			val leaderNodeByTerm: Array[Node] = new Array(numberOfCommandsToSend + 1)
			val atMostOneLeaderCommitment = net.netSequencer.Commitment[Unit]
			val weakReferencesHolder = mutable.Buffer.empty[AnyRef]

			val initializes = createsAndInitializesNodes(net, clusterSize, weakReferencesHolder) { node =>
				new node.DefaultNotificationListener() {
					override def onBecameLeader(previous: BehaviorOrdinal, term: Term): Unit = {
						if leaderNodeByTerm(term) eq null then leaderNodeByTerm(term) = node
						else atMostOneLeaderCommitment.break(new AssertionError(s"Node $node became leader at term $t despite node ${leaderNodeByTerm(term)} was a leader of the same term before"))()
					}
				}
			}
			val checks = for _ <- initializes yield {

				val client = Client[net.type](net, receiverIndex)

				inline val MAX_RETRIES = 9

				def sendCommandLoop(commandIndex: Int, retriesCounter: Int): net.netSequencer.Task[Unit] = {
					if commandIndex >= numberOfCommandsToSend || atMostOneLeaderCommitment.isCompleted then net.netSequencer.Task_unit
					else if retriesCounter > MAX_RETRIES then net.netSequencer.Task_failed(new AssertionError("The cluster got stuck unable to progress"))
					else for {
						wasProcessed <- client.sendsCommand(s"command#$commandIndex")
						_ <- {
							if wasProcessed then sendCommandLoop(commandIndex + 1, 0)
							else sendCommandLoop(commandIndex, retriesCounter + 1)
						}
					} yield ()
				}

				atMostOneLeaderCommitment.completeWith(sendCommandLoop(1, 0))()
			}

			checks.triggerAndForget(false)

			atMostOneLeaderCommitment.toFuture(false)
		}
	}


	test("Leader Append-Only - a leader never overwrites or deletes entries in its log") {
		PropF.forAllNoShrinkF(
			Gen.choose(3, 5).flatMap(n => Gen.choose(0, n - 1).map(m => (n, m))),
			Gen.choose(1, 11),
			Gen.choose(2, 5)
		) { (t, latency, numCommands) =>
			val (clusterSize, receiverIndex) = t
			scribe.info(s"\nBegin: clusterSize=$clusterSize, receiverIndex=$receiverIndex, latency=$latency, numCommands=$numCommands")

			val net = new Net(latency, 10)
			val leaderNeverOverwritesCommitment = net.netSequencer.Commitment[Unit]
			val weakReferencesHolder = mutable.Buffer.empty[AnyRef]

			val initializes = createsAndInitializesNodes(net, clusterSize, weakReferencesHolder) { node =>
				node.appendRecordListener = new node.AppendRecordListener() {
					override def onLogOverwrite(index: RecordIndex, firstReplacedRecord: Record, firstReplacingRecord: Record, behaviorOrdinal: BehaviorOrdinal): Unit = {
						if behaviorOrdinal == LEADER then leaderNeverOverwritesCommitment.break(new AssertionError(s"The participant ${node.myId} broke the \"append only rule\" at index $index. Removed record: $firstReplacedRecord, replacing record: $firstReplacingRecord."))()
					}
				}
				node.DefaultNotificationListener()
			}

			val checks = for _ <- initializes yield {

				val client = Client[net.type](net, receiverIndex)

				inline val MAX_RETRIES = 9

				def sendCommandLoop(commandIndex: Int, retriesCounter: Int): net.netSequencer.Task[Unit] = {
					if commandIndex >= 10 || leaderNeverOverwritesCommitment.isCompleted then net.netSequencer.Task_unit
					else if retriesCounter > MAX_RETRIES then net.netSequencer.Task_failed(new AssertionError("The cluster got stuck unable to progress"))
					else for {
						wasProcessed <- client.sendsCommand(s"command#$commandIndex")
						_ <- {
							if wasProcessed then sendCommandLoop(commandIndex + 1, 0)
							else sendCommandLoop(commandIndex, retriesCounter + 1)
						}
					} yield ()
				}

				leaderNeverOverwritesCommitment.completeWith(sendCommandLoop(1, 0))()
			}

			checks.triggerAndForget(false)
			leaderNeverOverwritesCommitment.toFuture(false)
		}
	}
}