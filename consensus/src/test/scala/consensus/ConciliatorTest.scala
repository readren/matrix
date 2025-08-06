package readren.matrix
package consensus

import providers.assistant.{CooperativeWorkersDap, DoerAssistantProvider, SchedulingDap}

import munit.ScalaCheckEffectSuite
import org.scalacheck.Gen
import org.scalacheck.effect.PropF
import readren.taskflow.SchedulingExtension.MilliDuration
import readren.taskflow.{Doer, Maybe, SchedulingExtension}

import java.util.concurrent.Executors
import scala.collection.mutable.ArrayBuffer
import scala.collection.{mutable, IndexedSeq as GenIndexedSeq}
import scala.compiletime.uninitialized
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

class ConciliatorTest extends ScalaCheckEffectSuite {

	private given ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(4))

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
		private var nodeById: Map[Id, TC] = Map.empty
		private var indexById: Map[Id, Int] = Map.empty
		private val nodeByIndex: ArrayBuffer[TC] = ArrayBuffer.empty

		def addNode(id: Id, node: TC): Unit = synchronized {
			nodeById += id -> node
			indexById += id -> nodeByIndex.size
			nodeByIndex.addOne(node)
		}

		def size: Int = synchronized(nodeById.size)

		def getNode(index: Int): TC = synchronized { nodeByIndex(index) }
		def getNode(id: String): Option[TC] = synchronized { nodeById.get(id) }
		def indexOf(id: String): Int = synchronized { indexById(id) }

		extension (inquirerId: Id) {
			def accessNode(id: Id): sequencer.Task[TC] = {
				sequencer.Task.ownFlat { () =>
					getNode(id).fold {
						sequencer.Task.failed(new RuntimeException(s"$inquirerId: no response from $id")).appointed(sequencer.newDelaySchedule(timeout))
					} { tc =>
						sequencer.Task.successful(tc).appointed(sequencer.newDelaySchedule(latency))
					}
				}
			}
		}
	}

	/**
	 * Simulates a client.
	 * @param net The network to use.
	 * @param initialReceiverIndex The initial receiver index. */
	private class Client(val net: Net, initialReceiverIndex: Int) {
		var receiverIndex: Int = initialReceiverIndex

		/**
		 * Sends a command to a node of the net.
		 * Initially, the target node is the node at the `initialReceiverIndex`.
		 * If the node responds with a [[RedirectTo]] the target node is updated to the redirected node and following commands are sent to the new target node.
		 * If the node responds with an [[Unable]] the command is retried on the next node of the net.
		 * If the node responds with a [[Processed]] the command is considered processed and the target node is not updated.
		 * @param command The command to send.
		 * @param isFallback Whether the command is a fallback command. */
		def sendsCommand(command: String, isFallback: Boolean = false, retriesCounter: Int = 0): net.sequencer.Task[Unit] = {
			val receiverNode = net.getNode(receiverIndex)

			def retry(): receiverNode.sequencer.Task[Unit] = {
				receiverIndex += 1
				if receiverIndex == net.size then receiverIndex = 0
				if retriesCounter >= net.size then receiverNode.sequencer.Task.unit
				else sendsCommand(command, true, retriesCounter + 1).onBehalfOf(receiverNode.sequencer)
			}

			receiverNode.sequencer.Task.ownFlat { () =>
				val receiverBehavior = receiverNode.participant.getBehaviorOrdinal
				receiverNode.cluster.messagesListener.onCommandFromClient(TestCommand(command), isFallback)
					.transformWith[Unit] {
						case Success(receiverNode.Processed(index, content)) =>
							scribe.info(s"Client: command `$command` was processed @$index by ${receiverNode.myId} which replied with `$content`.")
							receiverNode.sequencer.Task.unit
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
	 * Test implementation of  [[Conciliator]]
	 */
	private class TC(val myId: Id, participantsIds: Set[Id], net: Net, stateMachineNeedsRestart: Boolean = false) extends Conciliator { thisTC =>
		assert(participantsIds.contains(myId))

		import net.accessNode

		override type ParticipantId = Id
		override type ClientCommand = TestCommand
		override type StateMachineResponse = String

		private var _participant: ConsensusParticipant = uninitialized

		inline def participant: ConsensusParticipant = _participant

		def start(initialNotificationListener: NotificationListener): sequencer.Task[Unit] = {
			sequencer.Task.ownFlat { () =>
				val startedCommitment = sequencer.Commitment[Unit]
				val startedListener = new AbstractNotificationListener() {
					override def onStarted(term: Term, isRestart: Boolean): Unit = if !isRestart then startedCommitment.fulfill(())(_ => ())
				}
				_participant = ConsensusParticipant(cluster, storage, machine, List(startedListener, initialNotificationListener), stateMachineNeedsRestart)
				net.addNode(myId, thisTC)
				startedCommitment
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
					inline def migrateRecordsTo(replier: TC): GenIndexedSeq[replier.Record] = {
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
				if conflictFound then logBuffer.takeInPlace(storedIndex)
				while newIndex < records.size do {
					logBuffer += records(newIndex)
					newIndex += 1
				}
				logBufferOffset + logBuffer.size - 1
			}

			override def informCommitIndex(commitIndex: RecordIndex): Unit = ()

			override def release(): Unit = ()
		}
	}


	/**
	 * Helper to create participant ids.
	 */
	def createIds(size: Int): IndexedSeq[Id] = {
		(1 to size).map(i => s"participant-$i")
	}

	test("Simple test to verify framework works") {
		PropF.forAllF(Gen.choose(1, 5)) { clusterSize =>
			Future(assert(clusterSize > 2))
		}
	}

	test("Election Safety - at most one leader can be elected in a given term") {
		PropF.forAllNoShrinkF(
			Gen.choose(1, 5).flatMap(n => Gen.choose(0, n - 1).map(m => (n, m))),
			Gen.choose(1, 11)
		) { (t, latency) =>
			val (clusterSize, receiverIndex) = t
			val net = new Net(latency, 10)
			val ids = createIds(clusterSize)
			val tcs = for id <- ids yield TC(id, ids.toSet, net)
			scribe.info(s"\nBegin: clusterSize=$clusterSize, receiverIndex=$receiverIndex, latency=$latency")
			var notificationsListenersHolder: List[Any] = Nil
			val initTasks: IndexedSeq[net.sequencer.Task[net.sequencer.Commitment[Id]]] =
				for tc <- tcs yield {
					val consensusCommitment = net.sequencer.Commitment[Id]
					val listener = new tc.NotificationListener {
						override def onStarting(isRestart: Boolean): Unit = scribe.info(s"${tc.myId}: ${if isRestart then "re" else ""}starting...")

						override def onStarted(term: tc.Term, isRestart: Boolean): Unit = scribe.info(s"${tc.myId}: ${if isRestart then "re" else ""}started.")

						override def onBecameStopped(term: tc.Term, motive: Throwable): Unit = scribe.info(s"${tc.myId}: became stoped during term $term.", motive)

						override def onBecameIsolated(term: tc.Term, previousBehavior: tc.BehaviorOrdinal): Unit = scribe.info(s"${tc.myId}: became isolated from $previousBehavior during term $term.")

						override def onBecameCandidate(term: tc.Term): Unit = scribe.info(s"${tc.myId}: became candidate during term $term.")

						override def onBecameFollower(term: tc.Term, leaderId: Id): Unit = {
							scribe.info(s"${tc.myId}: became follower of $leaderId during term $term")
							consensusCommitment.fulfill(leaderId)(_ => ())
						}

						override def onBecameLeader(term: tc.Term): Unit = {
							scribe.info(s"${tc.myId}: became leader of term $term")
							consensusCommitment.fulfill(tc.myId)(_ => ())
						}
					}
					// Next line is needed because ConsensusParticipant`s references to notification listeners are weak.
					notificationsListenersHolder = listener :: notificationsListenersHolder
					tc.start(listener).map(_ => consensusCommitment).onBehalfOf(net.sequencer)
				}
			val commandsReceiver = tcs(receiverIndex)
			val compares =
				for {
					commitments <- net.sequencer.Task.sequenceToArray(initTasks)
					client = Client(net, receiverIndex)
					rtc1 <- client.sendsCommand("First")
						.onBehalfOf(net.sequencer)
					rtc2 <- client.sendsCommand("Second")
						.onBehalfOf(net.sequencer)
					oLeaderByParticipant <- net.sequencer.Task.sequenceToArray(commitments)
						.timeBounded(net.sequencer.newDelaySchedule(1000))
				} yield {
					assert(oLeaderByParticipant.fold(false)(_.toSet.size == 1))
				}
			compares.toFuture(false)
		}
	}
}