package readren.matrix
package cluster.service

import cluster.service.ClusterService.*
import cluster.service.Protocol.{ContactAddress, ContactCard}
import cluster.service.ProtocolVersion
import pruebas.Scheduler

import java.net.SocketAddress
import java.nio.channels.{AsynchronousServerSocketChannel, AsynchronousSocketChannel, CompletionHandler}
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.*

object ClusterService {

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

	class Config(val myContactCard: ContactCard, val joinConfig: JoinConfig, val peerDelegateConfig: ParticipantDelegate.Config)

	class JoinConfig(val seeds: Iterable[SocketAddress], val retryMinDelay: Long, val retryMaxDelay: Long, val timeUnit: TimeUnit, val maxAttempts: Int)

	def start(config: Config): ClusterService = {

		val serverChannelByVersionId =
			for (version, socketAddress) <- config.myContactCard yield {
				val serverSocketChannel = AsynchronousServerSocketChannel.open()
					.bind(socketAddress)
				version -> serverSocketChannel
			}

		val service = new ClusterService(config, serverChannelByVersionId)
		// start the listening servers
		for (version, serverChannel) <- serverChannelByVersionId do service.acceptNextClient(serverChannel, version)
		service.connectToSeeds(config.myContactCard)
		service
	}
}

class ClusterService private(config: ClusterService.Config, var serverChannelByVersionId: Map[ProtocolVersion, AsynchronousServerSocketChannel]) { thisClusterService =>

	private var participantDelegateByContactAddress: Map[ContactAddress, ParticipantDelegate] = Map.empty

	@volatile private var clusterState: ClusterState = Joining

	private val clusterEventsListeners: java.util.WeakHashMap[ClusterEventListener, Unit] = new java.util.WeakHashMap()

	val scheduler: Scheduler = new Scheduler

	def knownParticipantsCards: Iterator[ContactCard] = {
		participantDelegateByContactAddress.iterator.map { (_, participantDelegate) => participantDelegate.contactCard }
	}


	private def acceptNextClient(serverChannel: AsynchronousServerSocketChannel, version: ProtocolVersion): Unit = {
		serverChannel.accept(null, new CompletionHandler[AsynchronousSocketChannel, Null]() {
			override def completed(clientChannel: AsynchronousSocketChannel, attachment: Null): Unit = {
				onConnectionAccepted(clientChannel, version)
				// Note that the order of the calls to `onConnectionAccepted` and `acceptNextClient` determines if the calls to `onConnectionAccepted` are sequential or concurrent.
				acceptNextClient(serverChannel, version)
			}

			override def failed(exc: Throwable, attachment: Null): Unit = {
				scribe.error(s"Error while trying to accept a connection: clusterState=$clusterState, versionId=$version", exc)
				acceptNextClient(serverChannel, version)
			}
		})
	}

	/** Note that calls to this method are sequential. */
	private def onConnectionAccepted(clientChannel: AsynchronousSocketChannel, versionId: ProtocolVersion): Unit = {
		val clientRemoteAddress = clientChannel.getRemoteAddress
		participantDelegateByContactAddress.getOrElse(clientRemoteAddress, null) match {
			case null =>
				val delegate = new ParticipantDelegate(thisClusterService, clientRemoteAddress, clientChannel, config.peerDelegateConfig, versionId)
				participantDelegateByContactAddress += clientRemoteAddress -> delegate
				delegate.startAsServer()
			case delegate => delegate.handleReconnection(clientChannel, versionId)
		}
	}

	/** Tries to join to the cluster. */
	private def connectToSeeds(myContactCard: ContactCard): Unit = {
		// Send join requests to seeds with retries
		config.joinConfig.seeds.foreach { seed =>
			object handler extends CompletionHandler[Void, Integer] { thisHandler =>
				@volatile private var channel: AsynchronousSocketChannel = null

				def connect(attemptNumber: Integer): Unit = {
					if !participantDelegateByContactAddress.contains(seed) && config.myContactCard.forall { (_, a) => a != seed } then {
						channel = AsynchronousSocketChannel.open()
						channel.connect[Integer](seed, attemptNumber, handler)
					}
				}

				override def completed(result: Void, attemptNumber: Integer): Unit = {
					val peerRemoteAddress = channel.getRemoteAddress
					val newPeerDelegate = new ParticipantDelegate(thisClusterService, peerRemoteAddress, channel, config.peerDelegateConfig, ProtocolVersion.OF_THIS_PROJECT)
					participantDelegateByContactAddress += peerRemoteAddress -> newPeerDelegate
					newPeerDelegate.startAsClient(myContactCard)
					clusterEventsListeners.forEach { (listener, _) => listener.onPeerConnected(newPeerDelegate) }
				}

				override def failed(exc: Throwable, attemptNumber: Integer): Unit = {
					if attemptNumber <= config.joinConfig.maxAttempts then {
						val retryDelay = config.joinConfig.retryMinDelay * attemptNumber * attemptNumber
						scheduler.schedule(math.max(config.joinConfig.retryMaxDelay, retryDelay), config.joinConfig.timeUnit) { () =>
							connect(attemptNumber + 1)
						}
					}
				}
			}

			handler.connect(1)
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

}
