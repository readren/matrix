package readren.consensus

import ConsensusParticipantSdm.*

import munit.ScalaCheckEffectSuite
import org.scalacheck.Gen
import org.scalacheck.effect.PropF
import readren.sequencer.MilliDuration
import readren.sequencer.providers.CooperativeWorkersWithAsyncSchedulerDp

import java.util.concurrent.Executors
import scala.collection.mutable.ArrayBuffer
import scala.collection.{mutable, IndexedSeq as GenIndexedSeq}
import scala.compiletime.uninitialized
import scala.concurrent.{ExecutionContext, Promise}
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

class ConsensusParticipantSdmTest extends ScalaCheckEffectSuite {

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
			case LEADER => "stopped"
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

	/** Knows the set of [[Node]]s involved in the test. */
	private class Net(latency: MilliDuration, timeout: MilliDuration) {
		val netSequencer: ScheduSequen = sharedDap.provide("net-sequencer")
		private var nodeById: Map[Id, Node] = Map.empty
		private var indexById: Map[Id, Int] = Map.empty
		private val nodeByIndex: ArrayBuffer[Node] = ArrayBuffer.empty

		def addNode(node: Node): Unit = synchronized {
			assert(!nodeById.contains(node.myId))
			nodeById += node.myId -> node
			indexById += node.myId -> nodeByIndex.size
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
				netSequencer.Task_mine[Node] { () =>
					val node = nodeById(id)
					node

				}.scheduled(netSequencer.newDelaySchedule(latency))
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

		import net.accessNode

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

			extension (destinationId: ParticipantId) {
				def asksHowAreYou(inquirerTerm: Term, aLeaderMaybeMissing: Boolean): sequencer.Task[StateInfo] = {
					val result =
						for {
							replier <- boundParticipantId.accessNode(destinationId).onBehalfOf(sequencer)
							si <- replier.sequencer.Task_mine { () =>
								replier.clusterParticipant.messagesListener.onHowAreYou(boundParticipantId, inquirerTerm)
							}.onBehalfOf(sequencer)
						} yield si

					result.andThen { response =>
						scribe.info(s"$boundParticipantId: $destinationId.asksHowAreYou(inquirerTerm:$inquirerTerm, leaderMissing:$aLeaderMaybeMissing) yielded $response")
					}
				}

				def askToChooseForLeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.Task[Vote[ParticipantId]] = {
					val result =
						for {
							replier <- boundParticipantId.accessNode(destinationId).onBehalfOf(sequencer)
							vote <- replier.sequencer.Task_ownFlat { () =>
								replier.clusterParticipant.messagesListener.onChooseALeader(destinationId, inquirerInfo)
							}.onBehalfOf(sequencer)
						} yield vote
					result.andThen { response =>
						scribe.info(s"$boundParticipantId: $destinationId.askToChooseForLeader(inquirerId:$inquirerId, inquirerInfo:$inquirerInfo) yielded $response")
					}
				}

				def asksToAppendRecords(inquirerTerm: Term, prevLogIndex: RecordIndex, prevLogTerm: Term, records: GenIndexedSeq[Record], leaderCommit: RecordIndex): sequencer.Task[AppendResult] = {
					scribe.info(s"$myId: Asking $destinationId to append the records: inquirerTerm=$inquirerTerm, prevLogIndex=$prevLogIndex, prevLogTerm=$prevLogTerm, records=$records, leaderCommit=$leaderCommit")
					val result =
						for {
							replier <- boundParticipantId.accessNode(destinationId).onBehalfOf(sequencer)
							ar <- replier.sequencer.Task_ownFlat { () =>
								replier.clusterParticipant.messagesListener.onAppendRecords(boundParticipantId, inquirerTerm, prevLogIndex, prevLogTerm, records, leaderCommit)
							}.onBehalfOf(sequencer)
						} yield ar
					result.andThen { response =>
						scribe.info(s"$myId: $destinationId.asksToAppendRecords(inquirerTerm:$inquirerTerm, previousLogIndex:$prevLogIndex, previousLogTerm;$prevLogTerm, records:$records, leaderCommit:$leaderCommit) yielded $response")
					}
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
				logBuffer += record
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
		(0 until size).map(i => s"participant-$i")
	}


	/** Helper to create and initialize [[Node]]s.
	 * @param net the [[Net]] where the created [[Node]] instances will be added.
	 * @param clusterSize the number of [[Node]]s to create.
	 * @param weakReferencesHolder a collection to which the created instances of [[NotificationListener]] are added in order to avoid being garbage-collected.
	 * @param notificationListenerBuilder a function that takes the new [[Node]] and builds the [[NotificationListener]] to be passed its [[ConsensusParticipantSdm.ConsensusParticipant]] service constructor. */
	private def createsAndInitializesNodes[N <: Net](net: N, clusterSize: Int, weakReferencesHolder: mutable.Buffer[AnyRef])(notificationListenerBuilder: (node: Node) => node.NotificationListener): Unit = {
		val ids = createIds(clusterSize).toSet
		for id <- ids do {
			val node = Node(id, ids, net)
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
			Gen.choose(1, timeout + 1 + timeout / 10)
		) { (t, latency) =>
			val (clusterSize, receiverIndex) = t
			scribe.info(s"\nBegin: clusterSize=$clusterSize, receiverIndex=$receiverIndex, latency=$latency")

			val promise = Promise[Unit]()
			val net = new Net(latency, timeout)
			val weakReferencesHolder = mutable.Buffer.empty[AnyRef]
			val leaderNodeByTerm: Array[Node] = new Array(numberOfCommandsToSend + 1)

			createsAndInitializesNodes(net, clusterSize, weakReferencesHolder) { node =>
				new node.DefaultNotificationListener() {
					override def onBecameLeader(previous: BehaviorOrdinal, term: Term): Unit = {
						if leaderNodeByTerm(term) eq null then leaderNodeByTerm(term) = node
						else promise.tryFailure(new AssertionError(s"Node $node became leader at term $t despite node ${leaderNodeByTerm(term)} was a leader of the same term before"))
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
			Gen.choose(1, 11),
		) { (clusterConf, latency) =>
			val (clusterSize, receiverIndex) = clusterConf
			scribe.info(s"\nBegin: clusterSize=$clusterSize, receiverIndex=$receiverIndex, latency=$latency")

			val promise = Promise[Unit]()
			val net = new Net(latency, 10)
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
			Gen.choose(1, 11),
		) { (clusterConf, latency) =>
			val (clusterSize, receiverIndex) = clusterConf
			scribe.info(s"\nBegin: clusterSize=$clusterSize, receiverIndex=$receiverIndex, latency=$latency")

			val promise = Promise[Unit]()
			val net = new Net(latency, 10)
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
			Gen.choose(1, 11),
		) { (clusterConf, latency) =>
			val (clusterSize, receiverIndex) = clusterConf
			scribe.info(s"\nBegin: clusterSize=$clusterSize, receiverIndex=$receiverIndex, latency=$latency")

			val promise = Promise[Unit]()
			val net = new Net(latency, 10)
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
			Gen.choose(1, 11),
		) { (clusterConf, latency) =>
			val (clusterSize, receiverIndex) = clusterConf
			scribe.info(s"\nBegin: clusterSize=$clusterSize, receiverIndex=$receiverIndex, latency=$latency")

			val promise = Promise[Unit]()
			val net = new Net(latency, 10)
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