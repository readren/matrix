package readren.matrix
package cluster.service

import cluster.service.ClusterService.*
import cluster.service.Protocol.{ContactAddress, ContactCard, Member}
import cluster.service.ProtocolVersion

import readren.taskflow.SchedulingExtension.MilliDuration
import readren.taskflow.{Doer, SchedulingExtension}

import java.net.SocketAddress
import java.nio.channels.{AsynchronousServerSocketChannel, AsynchronousSocketChannel, CompletionHandler}
import java.util.concurrent.TimeUnit
import scala.collection.MapView
import scala.jdk.CollectionConverters.*

object ClusterService {
	
	abstract class TaskSequencer extends Doer, SchedulingExtension

	sealed trait ClusterState

	case object Joining extends ClusterState

	case class ClusterStateForMember(
		myAddress: SocketAddress,
		otherMembers: Set[ContactAddress],

	) extends ClusterState


	trait ClusterEventListener {
		def onJoined(state: ClusterStateForMember): Unit

		def onOtherMemberJoined(memberAddress: ContactAddress, memberDelegate: ParticipantDelegate): Unit

		def onPeerChannelClosed(peerAddress: ContactAddress, cause: Any): Unit

		def onPeerConnected(peerDelegate: ParticipantDelegate): Unit
	}

	class Config(val myAddress: ContactAddress, val supportedVersions: Set[ProtocolVersion], val peerDelegateConfig: ParticipantDelegate.Config)

	def start(taskSequencer: TaskSequencer, serviceConfig: Config, seeds: Iterable[ContactAddress], retryMinDelay: MilliDuration, retryMaxDelay: MilliDuration, maxAttempts: Int): ClusterService = {

		val serverChannel = AsynchronousServerSocketChannel.open()
			.bind(serviceConfig.myAddress)

		val service = new ClusterService(taskSequencer, serviceConfig, serverChannel)
		// start the listening servers
		service.acceptClientsSequentially(serverChannel)
		service.connectToSeeds(seeds, retryMinDelay, retryMaxDelay, maxAttempts)
		service
	}
}

class ClusterService private(sequencer: TaskSequencer, config: ClusterService.Config, val serverChannel: AsynchronousServerSocketChannel) { thisClusterService =>

	private var participantDelegateByContactAddress: Map[ContactAddress, ParticipantDelegate] = Map.empty

	@volatile private var clusterState: ClusterState = Joining

	private val clusterEventsListeners: java.util.WeakHashMap[ClusterEventListener, Unit] = new java.util.WeakHashMap()

	def doesAClusterExist: Boolean = participantDelegateByContactAddress.exists(_._2.peerMembershipStateAccordingToMe == Member)
	
	def getKnownParticipantsCards: MapView[ContactAddress, Set[ProtocolVersion]] = {
		participantDelegateByContactAddress.view.mapValues { delegate => delegate.versionsSupportedByPeer }
	}
	
	def getKnownMembersCards: MapView[ContactAddress, Set[ProtocolVersion]] = {
		participantDelegateByContactAddress.view.filter(_._2.peerMembershipStateAccordingToMe == Member).mapValues { delegate => delegate.versionsSupportedByPeer }
	}

	private def acceptClientsSequentially(serverChannel: AsynchronousServerSocketChannel): Unit = {
		serverChannel.accept(null, new CompletionHandler[AsynchronousSocketChannel, Null]() {
			override def completed(clientChannel: AsynchronousSocketChannel, attachment: Null): Unit = {
				onConnectionAccepted(clientChannel)
				// Note that the order of the calls to `onConnectionAccepted` and `acceptClientsSequentially` determines if the calls to `onConnectionAccepted` are sequential or concurrent.
				// Accept the next client
				acceptClientsSequentially(serverChannel)
			}

			override def failed(exc: Throwable, attachment: Null): Unit = {
				scribe.error(s"Error while trying to accept a connection: clusterState=$clusterState", exc)
				acceptClientsSequentially(serverChannel)
			}
		})
	}

	/** Note that calls to this method are sequential. */
	private def onConnectionAccepted(clientChannel: AsynchronousSocketChannel): Unit = {
		val clientRemoteAddress = clientChannel.getRemoteAddress
		participantDelegateByContactAddress.getOrElse(clientRemoteAddress, null) match {
			case null =>
				val delegate = new ParticipantDelegate(thisClusterService, clientChannel, config.peerDelegateConfig, clientRemoteAddress)
				participantDelegateByContactAddress += clientRemoteAddress -> delegate
				delegate.startAsServer(config.supportedVersions)
			case delegate => delegate.handleReconnection(clientChannel)
		}
	}

	/** Tries to join to the cluster. */
	private def connectToSeeds(seeds: Iterable[ContactAddress], retryMinDelay: MilliDuration, retryMaxDelay: MilliDuration, maxAttempts: Int): Unit = {
		// Send join requests to seeds with retries

		for seed <- seeds do {
			if config.myAddress != seed && !participantDelegateByContactAddress.contains(seed) then {
				object handler extends CompletionHandler[Void, Integer] { thisHandler =>
					@volatile private var channel: AsynchronousSocketChannel = null

					def connect(attemptNumber: Integer): Unit = {
						channel = AsynchronousSocketChannel.open()
						channel.connect[Integer](seed, attemptNumber, handler)
					}

					override def completed(result: Void, attemptNumber: Integer): Unit = {
						val peerRemoteAddress = channel.getRemoteAddress
						val newPeerDelegate = new ParticipantDelegate(thisClusterService, channel, config.peerDelegateConfig, peerRemoteAddress)
						participantDelegateByContactAddress += peerRemoteAddress -> newPeerDelegate
						newPeerDelegate.startAsClient(config.myAddress, config.supportedVersions)
						clusterEventsListeners.forEach { (listener, _) => listener.onPeerConnected(newPeerDelegate) }
					}

					override def failed(exc: Throwable, attemptNumber: Integer): Unit = {
						if attemptNumber <= maxAttempts then {
							val retryDelay = retryMinDelay * attemptNumber * attemptNumber
							val schedule: sequencer.Schedule = sequencer.newDelaySchedule(math.max(retryMaxDelay, retryDelay))
							sequencer.scheduleSequentially(schedule) { () =>
								connect(attemptNumber + 1)
							}
						}
					}
				}
				handler.connect(1)
			}
		}
	}

	def subscribe(listener: ClusterEventListener): Unit = {
		clusterEventsListeners.put(listener, ())
	}

	def unsubscribe(listener: ClusterEventListener): Boolean = {
		clusterEventsListeners.remove(listener) == ()
	}

	def peerChannelClosed(peerAddress: ContactAddress, cause: Any): Unit = {
		if participantDelegateByContactAddress.contains(peerAddress) then {
			participantDelegateByContactAddress -= peerAddress
			clusterEventsListeners.forEach { (listener, _) => listener.onPeerChannelClosed(peerAddress, cause) }
		}
		participantDelegateByContactAddress.foreach { (_, delegate) => delegate.onOtherPeerChannelClosed(peerAddress) }
	}
	
	/** Tell all the participants that the participant at the specified address and me can not communicate becase we don't support a common [[ProtocolVersion]]. */
	def notifyVersionIncompatibilityWith(participantAddress: ContactAddress): Unit = ???

	def notifyParticipantHasBeenRestarted(rebornParticipantAddress: ContactAddress): Unit = ???
}
