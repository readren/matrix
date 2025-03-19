package readren.matrix
package cluster.service

import cluster.service.ClusterService.*
import cluster.service.Protocol.{ContactAddress, Member}
import cluster.service.ProtocolVersion

import readren.taskflow.SchedulingExtension.MilliDuration
import readren.taskflow.{Doer, Maybe, SchedulingExtension}

import java.nio.channels.{AsynchronousServerSocketChannel, AsynchronousSocketChannel, CompletionHandler}
import scala.collection.MapView

object ClusterService {

	abstract class TaskSequencer extends Doer, SchedulingExtension

	trait ClusterEventListener {

		def onJoined(participantDelegateByContactAddress: Map[ContactAddress, MemberCommunicableDelegate]): Unit

		def onOtherMemberJoined(memberAddress: ContactAddress, memberDelegate: MemberCommunicableDelegate): Unit

		def onPeerChannelClosed(peerAddress: ContactAddress, cause: Any): Unit

		def onPeerConnected(participantDelegate: ParticipantDelegate): Unit

		def beforeClosingAllChannels(): Unit
	}

	class Config(val myAddress: ContactAddress, val participantDelegatesConfig: ParticipantDelegate.Config, val retryMinDelay: MilliDuration, val retryMaxDelay: MilliDuration, val maxAttempts: Int)

	def start(sequencer: TaskSequencer, serviceConfig: Config, seeds: Iterable[ContactAddress]): ClusterService = {

		val serverChannel = AsynchronousServerSocketChannel.open()
			.bind(serviceConfig.myAddress)

		val service = new ClusterService(sequencer, serviceConfig, serverChannel)
		// start the listening servers
		service.acceptClientConnections(serverChannel)
		// start connection to seeds
		service.startConnectionToSeeds(seeds)
		service
	}

	val ignorableErrorCatcher: PartialFunction[Throwable, Unit] = {
		case scala.util.control.NonFatal(t) => scribe.error(t)
	}
}

class ClusterService private(val sequencer: TaskSequencer, val config: ClusterService.Config, val serverChannel: AsynchronousServerSocketChannel) { thisClusterService =>

	export config.myAddress

	abstract class Behavior {
		def addCommunicableParticipant(participantAddress: ContactAddress): CommunicableDelegate
		def addIncommunicableParticipant(participantAddress: ContactAddress): IncommunicableDelegate

		def removeParticipant(participantAddress: ContactAddress): Unit

		def participantByAddress: Map[ContactAddress, ParticipantDelegate]
	}

	class AspirantBehavior(var aspirantDelegateByContactAddress: Map[ContactAddress, AspirantDelegate]) extends Behavior {
		override def addCommunicableParticipant(participantAddress: ContactAddress): AspirantCommunicableDelegate = {
			val newParticipant = new AspirantCommunicableDelegate(thisClusterService, this, config.participantDelegatesConfig, participantAddress)
			aspirantDelegateByContactAddress += participantAddress -> newParticipant
			newParticipant
		}

		override def addIncommunicableParticipant(participantAddress: ContactAddress): AspirantIncommunicableDelegate = {
			val newParticipant = new AspirantIncommunicableDelegate(thisClusterService)
			aspirantDelegateByContactAddress += participantAddress -> newParticipant
			newParticipant
		}

		override def removeParticipant(participantAddress: ContactAddress): Unit =
			aspirantDelegateByContactAddress -= participantAddress

		override def participantByAddress: Map[ContactAddress, AspirantDelegate] =
			aspirantDelegateByContactAddress
	}

	class MemberBehavior(var memberDelegateByContactAddress: Map[ContactAddress, MemberDelegate]) extends Behavior {
		override def addCommunicableParticipant(participantAddress: ContactAddress): MemberCommunicableDelegate = {
			val newParticipant = MemberCommunicableDelegate(thisClusterService, this, config.participantDelegatesConfig, participantAddress)
			memberDelegateByContactAddress += participantAddress -> newParticipant
			newParticipant
		}
		override def addIncommunicableParticipant(participantAddress: ContactAddress): MemberIncommunicableDelegate = {
			val newParticipant = new MemberIncommunicableDelegate(thisClusterService)
			memberDelegateByContactAddress += participantAddress -> newParticipant
			newParticipant
		}

		override def removeParticipant(participantAddress: ContactAddress): Unit =
			memberDelegateByContactAddress -= participantAddress

		override def participantByAddress: Map[ContactAddress, MemberDelegate] =
			memberDelegateByContactAddress
	}

	private var behavior: Behavior = AspirantBehavior(Map.empty)

	private val clusterEventsListeners: java.util.WeakHashMap[ClusterEventListener, Unit] = new java.util.WeakHashMap()

	inline def participantByAddress: Map[ContactAddress, ParticipantDelegate] = behavior.participantByAddress

	inline def getDelegateOrElse[D >: ParticipantDelegate](address: ContactAddress)(default: => D): D = behavior.participantByAddress.getOrElse(address, default)

	/** Must be called within the [[sequencer]]. */
	def doesAClusterExist: Boolean = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		behavior.participantByAddress.exists(_._2.peerMembershipStatusAccordingToMe eq Member)
	}

	/**
	 * Gets the contact cards of all the participants known by this [[ClusterService]] such that the participant is incommunicable or the handshake has been already done.
	 * Participants whose communicability is being investigated (because they are still in the handshake stage) are not included. 
	 * Must be called within the [[sequencer]]. */
	def getKnownParticipantsCards: MapView[ContactAddress, Set[ProtocolVersion]] = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		behavior.participantByAddress.view.mapValues { delegate => delegate.versionsSupportedByPeer }.filter { _._2.nonEmpty }
	}

	/**
	 * Gets the contact cards of all the participants known by this [[ClusterService]] such that the participant is member of the cluster according to this [[ClusterService]].
	 * Assumes that all members are either incommunicable or communicable with the handshake already completed. 
	 * Must be called within the [[sequencer]]. */
	def getKnownMembersCards: MapView[ContactAddress, Set[ProtocolVersion]] = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		behavior.participantByAddress.view.filter(_._2.peerMembershipStatusAccordingToMe eq Member).mapValues { delegate => delegate.versionsSupportedByPeer }
	}

	/**
	 * Manages incoming client connections to the server component of this [[ClusterService]].
	 *
	 * @param serverChannel The [[AsynchronousServerSocketChannel]] used to listen for and accept client connections.
	 */
	private def acceptClientConnections(serverChannel: AsynchronousServerSocketChannel): Unit = {
		try serverChannel.accept(null, new CompletionHandler[AsynchronousSocketChannel, Null]() {
			override def completed(clientChannel: AsynchronousSocketChannel, attachment: Null): Unit = {
				// Note that this method body is executed sequentially by nature of the NIO2. Nevertheless, the `executeSequentially` is necessary because some of the member variables accessed here need to be accessed by procedures that are started from other handlers that are concurrent.
				sequencer.executeSequentially {
					try {
						val clientRemoteAddress = clientChannel.getRemoteAddress
						behavior.participantByAddress.getOrElse(clientRemoteAddress, null) match {
							case null =>
								val newParticipant = behavior.addCommunicableParticipant(clientRemoteAddress)
								newParticipant.startAsServer(clientChannel)
								
							case communicableParticipant: CommunicableDelegate =>
								communicableParticipant.handleReconnection(clientChannel)
								
							case incommunicableParticipant: IncommunicableDelegate =>
								// TODO analyze if a notification of the communicability change should be issued here.
								behavior.removeParticipant(clientRemoteAddress)
								scribe.info(s"A connection from the participant at $clientRemoteAddress, which was marked as incommunicable, has been accepted. Let's see if we can communicate with it this time." )
								val renewedParticipant = behavior.addCommunicableParticipant(clientRemoteAddress)
								renewedParticipant.startAsServer(clientChannel)
						}
					} catch ignorableErrorCatcher
					// Accept the next client connection. Note that the call is done within the `sequencer` to avoid flooding its task-queue when many clients try to connect simultaneously.
					acceptClientConnections(serverChannel)
				}
			}

			override def failed(exc: Throwable, attachment: Null): Unit = {
				scribe.error(s"Error while trying to accept a connection", exc)
				acceptClientConnections(serverChannel)
			}
		})
		catch {
			case scala.util.control.NonFatal(e) =>
				scribe.error("Closing the cluster service because it is unable to accept connections from peers. Cause:", e)
				close()
		}
	}

	/** Tries to join to the cluster. */
	private def startConnectionToSeeds(seeds: Iterable[ContactAddress]): Unit = {
		// Send join requests to seeds with retries
		for seed <- seeds do if config.myAddress != seed then {
			sequencer.executeSequentially {
				if !participantByAddress.contains(seed) then startConnectionTo(seed)
			}
		}
	}

	def startConnectionTo(contactAddress: ContactAddress): Unit = {
		object handler extends CompletionHandler[Void, Integer] { thisHandler =>
			@volatile private var channel: AsynchronousSocketChannel = null

			def connect(attemptNumber: Integer): Unit = {
				assert(channel == null)
				try {
					channel = AsynchronousSocketChannel.open()
					channel.connect[Integer](contactAddress, attemptNumber, thisHandler)
				} catch ignorableErrorCatcher
			}

			override def completed(result: Void, attemptNumber: Integer): Unit = {
				sequencer.executeSequentially {
					try {
						val peerRemoteAddress = channel.getRemoteAddress
						val newParticipantDelegate = behavior.addCommunicableParticipant(peerRemoteAddress)
						newParticipantDelegate.startAsClient(config.myAddress, channel)
						clusterEventsListeners.forEach { (listener, _) => listener.onPeerConnected(newParticipantDelegate) }
					} catch ignorableErrorCatcher
				}
			}

			override def failed(exc: Throwable, attemptNumber: Integer): Unit = {
				if attemptNumber <= config.maxAttempts then {
					if channel.isOpen then channel.close() // TODO Is this line necessary?
					channel = null
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

	def subscribe(listener: ClusterEventListener): Unit = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		clusterEventsListeners.put(listener, ())
	}

	def unsubscribe(listener: ClusterEventListener): Boolean = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		clusterEventsListeners.remove(listener) == ()
	}

	def onParticipantChannelClosed(participantAddress: ContactAddress, cause: Any): Unit = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		if behavior.participantByAddress.contains(participantAddress) then {
			behavior.removeParticipant(participantAddress)
			clusterEventsListeners.forEach { (listener, _) => listener.onPeerChannelClosed(participantAddress, cause) }
			behavior.participantByAddress.foreach { case (_, communicableDelegate: CommunicableDelegate) => communicableDelegate.notifyOtherParticipantChannelHasBeenClosed(participantAddress) }
		}
	}

	/** Tell all the participants that the participant at the specified address and me can not communicate becase we don't support a common [[ProtocolVersion]]. */
	def notifyVersionIncompatibilityWith(participantAddress: ContactAddress): Unit = ???

	def notifyParticipantHasBeenRestarted(rebornParticipantAddress: ContactAddress): Unit = ???

	def proposeClusterCreator(): Unit = ???

	def solveClusterExistenceConflictWith(participantDelegate: MemberCommunicableDelegate): Unit = ???

	def close(): Unit = {
		clusterEventsListeners.forEach { (listener, _) => listener.beforeClosingAllChannels() }
		serverChannel.close()
		for case (_, communicableDelegate: CommunicableDelegate) <- behavior.participantByAddress do {
			communicableDelegate.closeChannel()
		}
	}
}
