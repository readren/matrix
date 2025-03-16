package readren.matrix
package cluster.service

import cluster.service.ClusterService.*
import cluster.service.Protocol.{Aspirant, ContactAddress, ContactCard, Member, MembershipState}
import cluster.service.ProtocolVersion

import readren.taskflow.SchedulingExtension.MilliDuration
import readren.taskflow.{Doer, Maybe, SchedulingExtension}

import java.net.SocketAddress
import java.nio.channels.{AsynchronousServerSocketChannel, AsynchronousSocketChannel, CompletionHandler}
import java.util.concurrent.TimeUnit
import scala.collection.{IterableFactory, MapView}
import scala.jdk.CollectionConverters.*
import scala.reflect.ClassTag

object ClusterService {

	abstract class TaskSequencer extends Doer, SchedulingExtension
	
	sealed trait RelationshipState
	class AspirantState(proposedClusterCreatorByAspirant: Map[ContactAddress, ContactAddress]) extends RelationshipState
	class MemberState() extends RelationshipState

	trait ClusterEventListener {
		def onJoined(participantDelegateByContactAddress: Map[ContactAddress, ParticipantDelegate]): Unit

		def onOtherMemberJoined(memberAddress: ContactAddress, memberDelegate: ParticipantDelegate): Unit

		def onPeerChannelClosed(peerAddress: ContactAddress, cause: Any): Unit

		def onPeerConnected(peerDelegate: ParticipantDelegate): Unit
	}

	class Config(val myAddress: ContactAddress, val participantDelegatesConfig: ParticipantDelegate.Config, val retryMinDelay: MilliDuration, val retryMaxDelay: MilliDuration, val maxAttempts: Int)

	def start(sequencer: TaskSequencer, serviceConfig: Config, seeds: Iterable[ContactAddress]): ClusterService = {

		val serverChannel = AsynchronousServerSocketChannel.open()
			.bind(serviceConfig.myAddress)

		val service = new ClusterService(sequencer, serviceConfig, serverChannel)
		// start the listening servers
		service.acceptClientsSequentially(serverChannel)
		// start connection to seeds
		service.startConnectionToSeeds(seeds)
		service
	}

	val ignorableErrorCatcher: PartialFunction[Throwable, Unit] = {
		case scala.util.control.NonFatal(t) => scribe.error(t)
	}
}

class ClusterService private(val sequencer: TaskSequencer, config: ClusterService.Config, val serverChannel: AsynchronousServerSocketChannel) { thisClusterService =>

	private var participantDelegateByContactAddress: Map[ContactAddress, ParticipantDelegate] = Map.empty

	var relationshipState: RelationshipState = AspirantState(Map.empty)

	private val clusterEventsListeners: java.util.WeakHashMap[ClusterEventListener, Unit] = new java.util.WeakHashMap()

	inline def getDelegateOrElse[D >: ParticipantDelegate](participant: ContactAddress)(default: => D): D = participantDelegateByContactAddress.getOrElse(participant, default)

	/** Must be called within the [[sequencer]]. */
	def doesAClusterExist: Boolean = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		participantDelegateByContactAddress.exists(_._2.peerMembershipStateAccordingToMe == Member)
	}

	/** Must be called within the [[sequencer]]. */
	def getKnownParticipantsCards: MapView[ContactAddress, Set[ProtocolVersion]] = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		participantDelegateByContactAddress.view.mapValues { delegate => delegate.versionsSupportedByPeer }
	}

	/** Must be called within the [[sequencer]]. */
	def getKnownMembersCards: MapView[ContactAddress, Set[ProtocolVersion]] = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		participantDelegateByContactAddress.view.filter(_._2.peerMembershipStateAccordingToMe == Member).mapValues { delegate => delegate.versionsSupportedByPeer }
	}

	private def acceptClientsSequentially(serverChannel: AsynchronousServerSocketChannel): Unit = {
		try serverChannel.accept(null, new CompletionHandler[AsynchronousSocketChannel, Null]() {
			override def completed(clientChannel: AsynchronousSocketChannel, attachment: Null): Unit = {
				// Note that this method body is executed sequentially by nature of the NIO2. Nevertheless, the `executeSequentially` is necessary because some of the member variables accessed here need to be accessed by procedures that are started from other handlers that are concurrent.
				sequencer.executeSequentially {
					try {
						val clientRemoteAddress = clientChannel.getRemoteAddress
						participantDelegateByContactAddress.getOrElse(clientRemoteAddress, null) match {
							case null =>
								val delegate = new ParticipantDelegate(thisClusterService, clientChannel, config.participantDelegatesConfig, clientRemoteAddress)
								participantDelegateByContactAddress += clientRemoteAddress -> delegate
								delegate.startAsServer()
							case delegate =>
								delegate.handleReconnection(clientChannel)
						}
					} catch ignorableErrorCatcher
					// Accept the next client. Note that the call is done within the `sequencer` to avoid flooding its task-queue when many clients try to connect simultaneously.
					acceptClientsSequentially(serverChannel)
				}
			}

			override def failed(exc: Throwable, attachment: Null): Unit = {
				scribe.error(s"Error while trying to accept a connection", exc)
				acceptClientsSequentially(serverChannel)
			}
		})
		catch ignorableErrorCatcher
	}

	/** Tries to join to the cluster. */
	private def startConnectionToSeeds(seeds: Iterable[ContactAddress]): Unit = {
		// Send join requests to seeds with retries
		for seed <- seeds do if config.myAddress != seed then {
			sequencer.executeSequentially {
				startConnectionTo(seed)
			}
		}
	}

	def startConnectionTo(contactAddress: ContactAddress): Unit = {
		if !participantDelegateByContactAddress.contains(contactAddress) then {
			object handler extends CompletionHandler[Void, Integer] { thisHandler =>
				@volatile private var channel: AsynchronousSocketChannel = null

				def connect(attemptNumber: Integer): Unit = {
					try {
						channel = AsynchronousSocketChannel.open()
						channel.connect[Integer](contactAddress, attemptNumber, handler)
					} catch ignorableErrorCatcher
				}

				override def completed(result: Void, attemptNumber: Integer): Unit = {
					sequencer.executeSequentially {
						try {
							val peerRemoteAddress = channel.getRemoteAddress
							val newPeerDelegate = new ParticipantDelegate(thisClusterService, channel, config.participantDelegatesConfig, peerRemoteAddress)
							participantDelegateByContactAddress += peerRemoteAddress -> newPeerDelegate
							newPeerDelegate.startAsClient(config.myAddress)
							clusterEventsListeners.forEach { (listener, _) => listener.onPeerConnected(newPeerDelegate) }
						} catch ignorableErrorCatcher
					}
				}

				override def failed(exc: Throwable, attemptNumber: Integer): Unit = {
					if attemptNumber <= config.maxAttempts then {
						val retryDelay = config.retryMinDelay * attemptNumber * attemptNumber
						val schedule: sequencer.Schedule = sequencer.newDelaySchedule(math.max(config.retryMaxDelay, retryDelay))
						sequencer.scheduleSequentially(schedule) { () =>
							connect(attemptNumber + 1)
						}
					}
				}
			}
			handler.connect(1)
		}
	}

	def subscribe(listener: ClusterEventListener): Unit = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		clusterEventsListeners.put(listener, ())
	}

	def unsubscribe(listener: ClusterEventListener): Boolean = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		clusterEventsListeners.remove(listener) == ()
	}

	def peerChannelClosed(peerAddress: ContactAddress, cause: Any): Unit = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		if participantDelegateByContactAddress.contains(peerAddress) then {
			participantDelegateByContactAddress -= peerAddress
			clusterEventsListeners.forEach { (listener, _) => listener.onPeerChannelClosed(peerAddress, cause) }
		}
		participantDelegateByContactAddress.foreach { (_, delegate) => delegate.onOtherPeerChannelClosed(peerAddress) }
	}

	/** Tell all the participants that the participant at the specified address and me can not communicate becase we don't support a common [[ProtocolVersion]]. */
	def notifyVersionIncompatibilityWith(participantAddress: ContactAddress): Unit = ???

	def notifyParticipantHasBeenRestarted(rebornParticipantAddress: ContactAddress): Unit = ???
	
	def proposeClusterCreator(): Unit = ???
	def solveClusterExistenceConflictWith(participantDelegate: ParticipantDelegate): Unit = ???
}
