package readren.matrix
package consensus

import consensus.Conciliator.{BehaviorOrdinal, LEADER, RecordIndex, Term}
import providers.assistant.{CooperativeWorkersDap, DoerAssistantProvider, SchedulingDap}

import munit.ScalaCheckEffectSuite
import org.scalacheck.Gen
import org.scalacheck.effect.PropF
import readren.taskflow.SchedulingExtension.MilliDuration
import readren.taskflow.{Doer, Maybe, SchedulingExtension}

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap, Executors}
import scala.collection.mutable.ArrayBuffer
import scala.collection.{mutable, IndexedSeq as GenIndexedSeq}
import scala.compiletime.uninitialized
import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

class ConciliatorTest extends ScalaCheckEffectSuite {

	private given ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(4))

	/** Id of a [[Node]]. Implements [[Conciliator.ParticipantId]]. */
	private type Id = String

	// Test command type
	private case class TestCommand(value: String)

	private val schedulingDap = new SchedulingDap(failureReporter = scribe.error(s"Unhandled exception in a task executed by the sequencer tagged with ${CooperativeWorkersDap.currentAssistant.id}", _))

	private class Sequencer(tag: DoerAssistantProvider.Tag) extends Doer, SchedulingExtension {
		override type Assistant = SchedulingDap.SchedulingAssistant
		override val assistant: Assistant = schedulingDap.provide(tag)
	}

	private class Net(latency: MilliDuration, timeout: MilliDuration) {
		val sequencer = Sequencer(0)
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
			def accessNode(id: Id): sequencer.Task[Node] = {
				sequencer.Task.ownFlat { () =>
					getNode(id).fold {
						sequencer.Task.failed(new RuntimeException(s"$inquirerId: no response from $id")).appointed(sequencer.newDelaySchedule(timeout))
					} { node =>
						sequencer.Task.successful(node).appointed(sequencer.newDelaySchedule(latency))
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
		 * Sends a command to a node of the net.
		 * Initially, the target node is the node at the `initialReceiverIndex`.
		 * If the target node responds with a [[RedirectTo]] the target node is updated to the redirected node and following commands are sent to the new target node.
		 * If the target node responds with an [[Unable]] the command is retried on the next node of the net.
		 * If the target node responds with a [[Processed]] the command is considered processed and the target node is not updated.
		 * @param command The command to send.
		 * @param isFallback Whether the command is a fallback command.
		 * @return A task that completes with true if the command was processed; false if the command was not processed despite all nodes were tried or the net is empty. */
		def sendsCommand(command: String, isFallback: Boolean = false, retriesCounter: Int = 0): net.sequencer.Task[Boolean] = {
			if net.size == 0 then net.sequencer.Task.successful(false)
			if receiverIndex < 0 then receiverIndex = net.size - 1
			val receiverNode = net.getNode(receiverIndex)

			def retry(): receiverNode.sequencer.Task[Boolean] = {
				receiverIndex -= 1
				if retriesCounter > net.size then receiverNode.sequencer.Task.successful(false)
				else sendsCommand(command, true, retriesCounter + 1).onBehalfOf(receiverNode.sequencer)
			}

			receiverNode.sequencer.Task.ownFlat { () =>
				val receiverBehavior = receiverNode.participant.getBehaviorOrdinal
				receiverNode.cluster.messagesListener.onCommandFromClient(TestCommand(command), isFallback)
					.transformWith[Boolean] {
						case Success(receiverNode.Processed(index, content)) =>
							scribe.info(s"Client: command `$command` was processed @$index by ${receiverNode.myId} which replied with `$content`.")
							receiverNode.sequencer.Task.successful(true)
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
			}.onBehalfOf(net.sequencer)
		}

	}

	/**
	 * Test implementation of [[Conciliator]]
	 */
	private class Node(val myId: Id, participantsIds: Set[Id], net: Net, stateMachineNeedsRestart: Boolean = false) extends Conciliator { thisNode =>
		assert(participantsIds.contains(myId))

		import net.accessNode

		override type ParticipantId = Id
		override type ClientCommand = TestCommand
		override type StateMachineResponse = String

		var logListener: LogListener = new LogListener() {
			override def onConflict(index: RecordIndex, term: Term, behaviorOrdinal: BehaviorOrdinal): Unit = ()
		}

		private var _participant: ConsensusParticipant = uninitialized

		/** @return the [[ConsensusParticipant]] instance associated to this node. */
		inline def participant: ConsensusParticipant = _participant

		/** Initializes this node, adding it to the `net` and creating the associated [[ConsensusParticipant]] instance. */
		def initializes(initialNotificationListener: NotificationListener = DefaultNotificationListener()): sequencer.Duty[Unit] = {
			net.addNode(myId, thisNode)
			sequencer.Duty.mine { () =>
				_participant = ConsensusParticipant(cluster, storage, machine, List(initialNotificationListener, notificationScribe), stateMachineNeedsRestart)
			}
		}


		/** Initializes this node and waits it's associated [[ConsensusParticipant]] instance to be started. */
		def starts(initialNotificationListener: NotificationListener = DefaultNotificationListener()): sequencer.Duty[Unit] = {
			net.addNode(myId, thisNode)
			sequencer.Duty.mineFlat { () =>
				val startedCovenant = sequencer.Covenant[Unit]
				val startedListener = new DefaultNotificationListener() {
					override def onStarted(previous: BehaviorOrdinal, term: Term, isRestart: Boolean): Unit = if !isRestart then startedCovenant.fulfill(())(_ => ())
				}
				_participant = ConsensusParticipant(cluster, storage, machine, List(startedListener, initialNotificationListener, notificationScribe), stateMachineNeedsRestart)
				startedCovenant
			}
		}

		// Real sequencer implementation for testing
		override val sequencer: Sequencer = new Sequencer(1)

		object machine extends StateMachine {
			override def appliesClientCommand(index: RecordIndex, command: ClientCommand): sequencer.Task[StateMachineResponse] = {
				sequencer.Task.successful(command.value) // TODO add delay
			}
		}

		override def isEager: Boolean = false

		/**
		 * Test instance of  [[Cluster]]
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
							si <- replier.sequencer.Task.mine { () =>
								replier.cluster.messagesListener.onHowAreYou(hostId, inquirerTerm)
							}.onBehalfOf(sequencer)
						} yield StateInfo(si.currentTerm, si.ordinal, si.lastRecordTerm, si.lastRecordIndex)

					result.andThen { response =>
						scribe.info(s"$hostId: $destinationId.asksHowAreYou(inquirerTerm:$inquirerTerm, leaderMissing:$aLeaderMaybeMissing) returned $response")
					}
				}

				def askToChooseForLeader(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.Task[Vote] = {
					val result =
						for {
							replier <- hostId.accessNode(destinationId).onBehalfOf(sequencer)
							v <- replier.sequencer.Task.ownFlat { () =>
								replier.cluster.messagesListener
									.onChooseALeader(destinationId, replier.StateInfo(inquirerInfo.currentTerm, inquirerInfo.ordinal, inquirerInfo.lastRecordTerm, inquirerInfo.lastRecordIndex))
							}.onBehalfOf(sequencer)

						} yield {
							Vote(v.term, v.candidateId, v.reachableCandidateCount, v.behaviorOrdinal)
						}
					result.andThen { response =>
						scribe.info(s"$hostId: $destinationId.askToChooseForLeader(inquirerId:$inquirerId, inquirerInfo:$inquirerInfo) returned $response")
					}
				}

				def asksToAppendRecords(inquirerTerm: Term, prevLogIndex: RecordIndex, prevLogTerm: Term, records: GenIndexedSeq[Record], leaderCommit: RecordIndex): sequencer.Task[AppendResult] = {
					inline def migrateRecordsTo(replier: Node): GenIndexedSeq[replier.Record] = {
						for record <- records yield record match {
							case c: Command => replier.Command(c.term, c.command)
							case lt: LeaderTransition => replier.LeaderTransition(lt.term)
							case sp: SnapshotPoint => replier.SnapshotPoint(sp.term)
							case cc1: ConfigurationChangeOldNew => replier.ConfigurationChangeOldNew(cc1.term, cc1.oldParticipants, cc1.newParticipants)
							case cc2: ConfigurationChangeNew => replier.ConfigurationChangeNew(cc2.term, cc2.newParticipants)
						}
					}

					scribe.info(s"$myId: Asking $destinationId to append the records: inquirerTerm=$inquirerTerm, prevLogIndex=$prevLogIndex, prevLogTerm=$prevLogTerm, records=$records, leaderCommit=$leaderCommit")
					val result =
						for {
							replier <- hostId.accessNode(destinationId).onBehalfOf(sequencer)
							ar <- replier.sequencer.Task.ownFlat { () =>
								replier.cluster.messagesListener
									.onAppendRecords(hostId, inquirerTerm, prevLogIndex, prevLogTerm, migrateRecordsTo(replier), leaderCommit)
							}.onBehalfOf(sequencer)
						} yield AppendResult(ar.term, ar.success, ar.behaviorOrdinal)
					result.andThen { response =>
						scribe.info(s"$myId: $destinationId.asksToAppendRecords(inquirerTerm:$inquirerTerm, previousLogIndex:$prevLogIndex, previousLogTerm;$prevLogTerm, records:$records, leaderCommit:$leaderCommit) returned $response")
					}
				}
			}
		}

		/**
		 * Test instance of [[Storage]]
		 */
		object storage extends Storage {
			override def invalidWorkspace(): WS = new TestWorkspace {
				override def isBrandNew: Boolean = ???

				override def getCurrentParticipants: IndexedSeq[ParticipantId] = ???

				override def setCurrentParticipants(participants: IndexedSeq[ParticipantId]): Unit = ???

				/** The current term according to this participant.
				 * Zero means "before the first election". */
				override def getCurrentTerm: Term = ???

				override def setCurrentTerm(term: Term): Unit = ???

				/** The index of the oldest [[Record]] stored in the log buffer. The initial value is 1. */
				override def logBufferOffset: RecordIndex = ???

				/** The index of the first empty entry in the log. The initial value is 1. */
				override def firstEmptyRecordIndex: RecordIndex = ???

				/** The index of the last appended record. The initial value is 0, which means that the log is empty. */
				override def lastAppendedRecord: Record = ???

				override def getRecordAt(index: RecordIndex): Record = ???

				/** Returns the records in the log starting at `from` and up to `until` exclusive.
				 * // TODO: consider returning an iterator instead of an iterable.
				 */
				override def getRecordsBetween(from: RecordIndex, until: RecordIndex): GenIndexedSeq[Record] = ???

				/** Appends a record and returns it index. */
				override def appendRecord(record: Record): Unit = ???

				/** Appends any new record not already in the log starting at the specified index.
				 * If a conflict is detected (i.e., a stored record and a new record at the same index have different terms), all stored records from that index onward are removed before appending the new records.
				 * @return The index of the last record appended. */
				override def appendResolvingConflicts(records: GenIndexedSeq[Record], from: RecordIndex): RecordIndex = ???

				/** Should be called whenever the commit index changed to allow this [[Workspace]] to release the storage used to memorize already commited records. */
				override def informCommitIndex(commitIndex: RecordIndex): Unit = ???

				override def release(): Unit = ()
			}

			private var memory: WS = TestWorkspace()

			override val loads: sequencer.Task[WS] = sequencer.Task.successful(memory) // TODO add a delay

			override def saves(workspace: WS): sequencer.Task[Unit] = {
				memory = workspace
				sequencer.Task.successful(()) // TODO add a delay
			}
		}

		override type WS = TestWorkspace

		/**
		 * Test implementation of [[Workspace]]
		 */
		class TestWorkspace extends Workspace {
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
					logListener.onConflict(storedIndex + logBufferOffset, participant.getTerm, participant.getBehaviorOrdinal)
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

		trait LogListener {
			def onConflict(index: RecordIndex, term: Term, behaviorOrdinal: BehaviorOrdinal): Unit
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


	/** Helper to create and initialize the nodes. */
	private def createsAndInitializesNodes[N <: Net](net: N, clusterSize: Int)(notificationListenerBuilder: (node: Node) => node.NotificationListener): net.sequencer.Duty[Array[Unit]] = {
		val ids = createIds(clusterSize)
		val nodes = for id <- ids yield Node(id, ids.toSet, net)
		var notificationsListenersHolder: List[Any] = Nil
		val nodeInitializerByIndex =
			for node <- nodes yield {
				val nl = notificationListenerBuilder(node)
				notificationsListenersHolder = nl :: notificationsListenersHolder
				node.initializes(initialNotificationListener = nl).onBehalfOf(net.sequencer)
			}
		net.sequencer.Duty.sequenceToArray(nodeInitializerByIndex)
	}


	test("Election Safety - at most one leader can be elected in a given term") {
		PropF.forAllNoShrinkF(
			Gen.choose(1, 5).flatMap(n => Gen.choose(0, n - 1).map(m => (n, m))),
			Gen.choose(1, 11)
		) { (t, latency) =>
			val (clusterSize, receiverIndex) = t
			scribe.info(s"\nBegin: clusterSize=$clusterSize, receiverIndex=$receiverIndex, latency=$latency")
			val net = new Net(latency, 10)
			val leaderNodeByTerm: ConcurrentMap[Term, Node] = new ConcurrentHashMap()
			val atMostOneLeaderCommitment = net.sequencer.Commitment[Unit]

			val initializes = createsAndInitializesNodes(net, clusterSize) { node =>
				new node.DefaultNotificationListener() {
					override def onBecameLeader(previous: BehaviorOrdinal, term: Term): Unit = {
						leaderNodeByTerm.compute(term, (t, previousLeader) =>
							if previousLeader ne null then atMostOneLeaderCommitment.break(new AssertionError(s"Node $node became leader at term $t despite node $previousLeader was a leader of the same term before"))()
							node
						)
					}
				}
			}
			val checks = for _ <- initializes yield {

				val client = Client[net.type](net, receiverIndex)

				inline val MAX_RETRIES = 9

				def sendCommandLoop(commandIndex: Int, retriesCounter: Int): net.sequencer.Task[Unit] = {
					if commandIndex >= 10 || atMostOneLeaderCommitment.isCompleted then net.sequencer.Task.unit
					else if retriesCounter > MAX_RETRIES then net.sequencer.Task.failed(new AssertionError("The cluster got stuck unable to progress"))
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
			val leaderNeverOverwritesCommitment = net.sequencer.Commitment[Unit]
			val initializes = createsAndInitializesNodes(net, clusterSize) { node =>
				node.logListener = new node.LogListener() {
					override def onConflict(index: RecordIndex, term: Term, behaviorOrdinal: BehaviorOrdinal): Unit =
						if behaviorOrdinal == LEADER then leaderNeverOverwritesCommitment.break(new AssertionError(s"The participant ${node.myId} broke the \"append only rule\" at index $index in term $term."))()
				}
				node.DefaultNotificationListener()
			}

			val checks = for _ <- initializes yield {

				val client = Client[net.type](net, receiverIndex)

				inline val MAX_RETRIES = 9

				def sendCommandLoop(commandIndex: Int, retriesCounter: Int): net.sequencer.Task[Unit] = {
					if commandIndex >= 10 || leaderNeverOverwritesCommitment.isCompleted then net.sequencer.Task.unit
					else if retriesCounter > MAX_RETRIES then net.sequencer.Task.failed(new AssertionError("The cluster got stuck unable to progress"))
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