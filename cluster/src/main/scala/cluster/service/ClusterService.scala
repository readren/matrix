package readren.matrix
package cluster.service

import cluster.service.ClusterService.*
import cluster.service.Protocol.{ContactAddress, Member}
import cluster.service.ProtocolVersion

import readren.taskflow.SchedulingExtension.MilliDuration
import readren.taskflow.{Doer, SchedulingExtension}

import java.nio.channels.{AsynchronousServerSocketChannel, AsynchronousSocketChannel, CompletionHandler}
import scala.collection.MapView
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object ClusterService {

	abstract class TaskSequencer extends Doer, SchedulingExtension

	trait ClusterEventListener {

		def onStartingConnectionToNewParticipant(participantDelegate: ParticipantDelegate & Incommunicable): Unit

		def onPeerConnected(participantDelegate: ParticipantDelegate): Unit

		def onJoined(participantDelegateByContactAddress: Map[ContactAddress, MemberCommunicableDelegate]): Unit

		def onOtherMemberJoined(memberAddress: ContactAddress, memberDelegate: MemberCommunicableDelegate): Unit

		def onDelegateBecomeCommunicable(peerAddress: ContactAddress, cause: Any): Unit

		def onDelegateBecomeCommunicable(peerAddress: ContactAddress): Unit

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
		case NonFatal(t) => scribe.error(t)
	}
}

/**
 * A service that manages the propagation of its own existence and the awareness of other [[ClusterService]] instances
 * to support intercommunication between their users across different JVMs, which may be (and usually are) on different host machines.
 *
 * For brevity, instances of this class are referred to as "participants."
 * Depending on its membership status (from its own viewpoint), a participant behaves either as a "member" or as an "aspirant"
 * to become a member of the cluster.
 *
 * The [[ClusterService]] class delegates the knowledge about, and communication with, other participants to implementations
 * of the [[ParticipantDelegate]] trait: it creates one delegate per participant it is aware of.
 */
class ClusterService private(val sequencer: TaskSequencer, val config: ClusterService.Config, val serverChannel: AsynchronousServerSocketChannel) { thisClusterService =>

	export config.myAddress

	private[service] abstract class Behavior {
		def createAndAddACommunicableDelegate(participantAddress: ContactAddress, communicationChannel: AsynchronousSocketChannel): ParticipantDelegate & Communicable

		def createAndAddAnIncommunicableDelegate(participantAddress: ContactAddress, isConnecting: Boolean): ParticipantDelegate & Incommunicable

		def removeDelegate(participantAddress: ContactAddress): Unit

		def delegateByAddress: Map[ContactAddress, ParticipantDelegate & Communicability]
	}

	private[service] class AspirantBehavior(var aspirantDelegateByContactAddress: Map[ContactAddress, AspirantDelegate & Communicability]) extends Behavior {
		override def createAndAddACommunicableDelegate(participantAddress: ContactAddress, communicationChannel: AsynchronousSocketChannel): AspirantCommunicableDelegate = {
			val newParticipant = new AspirantCommunicableDelegate(thisClusterService, this, config.participantDelegatesConfig, participantAddress, communicationChannel)
			aspirantDelegateByContactAddress += participantAddress -> newParticipant
			newParticipant
		}

		override def createAndAddAnIncommunicableDelegate(participantAddress: ContactAddress, isConnecting: Boolean): AspirantIncommunicableDelegate = {
			val newParticipant = new AspirantIncommunicableDelegate(thisClusterService, participantAddress, isConnecting)
			aspirantDelegateByContactAddress += participantAddress -> newParticipant
			newParticipant
		}

		override def removeDelegate(participantAddress: ContactAddress): Unit =
			aspirantDelegateByContactAddress -= participantAddress

		override def delegateByAddress: Map[ContactAddress, AspirantDelegate & Communicability] =
			aspirantDelegateByContactAddress
	}

	private[service] class MemberBehavior(var memberDelegateByContactAddress: Map[ContactAddress, MemberDelegate & Communicability]) extends Behavior {
		override def createAndAddACommunicableDelegate(participantAddress: ContactAddress, communicationChannel: AsynchronousSocketChannel): MemberCommunicableDelegate = {
			val newParticipant = MemberCommunicableDelegate(thisClusterService, this, config.participantDelegatesConfig, participantAddress, communicationChannel)
			memberDelegateByContactAddress += participantAddress -> newParticipant
			newParticipant
		}

		override def createAndAddAnIncommunicableDelegate(participantAddress: ContactAddress, isConnecting: Boolean): MemberIncommunicableDelegate = {
			val newParticipant = new MemberIncommunicableDelegate(thisClusterService, participantAddress, isConnecting)
			memberDelegateByContactAddress += participantAddress -> newParticipant
			newParticipant
		}

		override def removeDelegate(participantAddress: ContactAddress): Unit =
			memberDelegateByContactAddress -= participantAddress

		override def delegateByAddress: Map[ContactAddress, MemberDelegate & Communicability] =
			memberDelegateByContactAddress
	}

	private var behavior: Behavior = AspirantBehavior(Map.empty)

	private val clusterEventsListeners: java.util.WeakHashMap[ClusterEventListener, Unit] = new java.util.WeakHashMap()

	private[service] inline def getBehavior: Behavior = behavior

	inline def participantByAddress: Map[ContactAddress, ParticipantDelegate & Communicability] = behavior.delegateByAddress

	inline def getDelegateOrElse[D >: ParticipantDelegate](address: ContactAddress, default: => D): D = behavior.delegateByAddress.getOrElse(address, default)

	/** Must be called within the [[sequencer]]. */
	def doesAClusterExist: Boolean = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		behavior.delegateByAddress.exists(_._2.peerMembershipStatusAccordingToMe eq Member)
	}

	/**
	 * Gets the contact cards of all the participants known by this [[ClusterService]] such that the participant is incommunicable or the handshake has been already done.
	 * Participants whose communicability is being investigated (because they are still in the handshake stage) are not included. 
	 * Must be called within the [[sequencer]]. */
	def getKnownParticipantsCards: MapView[ContactAddress, Set[ProtocolVersion]] = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		behavior.delegateByAddress.view.mapValues { delegate => delegate.versionsSupportedByPeer }.filter {
			_._2.nonEmpty
		}
	}

	/**
	 * Gets the contact cards of all the participants known by this [[ClusterService]] such that the participant is member of the cluster according to this [[ClusterService]].
	 * Assumes that all members are either incommunicable or communicable with the handshake already completed. 
	 * Must be called within the [[sequencer]]. */
	def getKnownMembersCards: MapView[ContactAddress, Set[ProtocolVersion]] = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		behavior.delegateByAddress.view.filter(_._2.peerMembershipStatusAccordingToMe eq Member).mapValues { delegate => delegate.versionsSupportedByPeer }
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
						behavior.delegateByAddress.getOrElse(clientRemoteAddress, null) match {
							case null =>
								val newParticipant = behavior.createAndAddACommunicableDelegate(clientRemoteAddress, clientChannel)
								newParticipant.startAsServer(clientChannel)

							case communicableParticipant: Communicable =>
								communicableParticipant.handleReconnection(clientChannel)

							case incommunicableParticipant: Incommunicable =>
								val communicableParticipant = incommunicableParticipant.replaceMyselfWithACommunicableDelegate(clientChannel)
								// TODO analyze if a notification of the communicability change should be issued here.
								scribe.info(s"A connection from the participant at $clientRemoteAddress, which was marked as incommunicable, has been accepted. Let's see if we can communicate with it this time.")
								communicableParticipant.startAsServer(clientChannel)
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
			case NonFatal(e) =>
				scribe.error("Closing the cluster service because it is unable to accept connections from peers. Cause:", e)
				release()
		}
	}

	/** Tries to join to the cluster. */
	private def startConnectionToSeeds(seeds: Iterable[ContactAddress]): Unit = {
		// Send join requests to seeds with retries
		for seed <- seeds do if config.myAddress != seed then {
			sequencer.executeSequentially {
				if !participantByAddress.contains(seed) then startConnectionToSeed(seed)
			}
		}
	}

	private def startConnectionToSeed(contactAddress: ContactAddress): Unit = {
		connectTo(contactAddress) {
			case Success(communicationChannel) =>
				sequencer.executeSequentially {
					val newParticipantDelegate = behavior.createAndAddACommunicableDelegate(contactAddress, communicationChannel)
					newParticipantDelegate.startAsClient(communicationChannel)
					clusterEventsListeners.forEach { (listener, _) => listener.onPeerConnected(newParticipantDelegate) }
				}
			case Failure(exc) =>
				scribe.error(s"The connection to the seed at $contactAddress has been aborted after many failed tries.")
		}
	}

	private[service] def createNewDelegateForAndStartConnectingTo(participantContactAddress: ContactAddress, versionsSupportedByPeer: Set[ProtocolVersion]): Unit = {
		val connectingDelegate = behavior.createAndAddAnIncommunicableDelegate(participantContactAddress, true)
		connectingDelegate.versionsSupportedByPeer = versionsSupportedByPeer
		clusterEventsListeners.forEach { (listener, _) => listener.onStartingConnectionToNewParticipant(connectingDelegate) }

		connectTo(participantContactAddress) {
			case Success(communicationChannel) =>
				sequencer.executeSequentially {
					if connectingDelegate eq behavior.delegateByAddress.getOrElse(participantContactAddress, null) then {
						val communicableDelegate = connectingDelegate.replaceMyselfWithACommunicableDelegate(communicationChannel)
						communicableDelegate.startAsClient(communicationChannel)
					}
				}
			case Failure(exc) =>
				connectingDelegate.connectionAborted()
				scribe.error(s"The connection to the participant at $participantContactAddress has been aborted after many failed tries.")
		}
	}

	private[service] def connectTo(contactAddress: ContactAddress)(onComplete: Try[AsynchronousSocketChannel] => Unit): Unit = {
		object handler extends CompletionHandler[Void, Integer] { thisHandler =>
			@volatile private var channel: AsynchronousSocketChannel = null

			def connect(attemptNumber: Integer): Unit = {
				assert(channel == null)
				try {
					channel = AsynchronousSocketChannel.open()
					channel.connect[Integer](contactAddress, attemptNumber, thisHandler)
				} catch {
					case NonFatal(exc) => onComplete(Failure(exc))
				}
			}

			override def completed(result: Void, attemptNumber: Integer): Unit = {
				onComplete(Success(channel))
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
				} else onComplete(Failure(exc))
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

	private[service] def onDelegateBecomeIncommunicable(participantAddress: ContactAddress, cause: Any): Unit = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		clusterEventsListeners.forEach { (listener, _) => listener.onDelegateBecomeCommunicable(participantAddress, cause) }
		for case (_, communicableDelegate: Communicable) <- behavior.delegateByAddress do
			communicableDelegate.notifyPeerThatILostCommunicationWithOtherPeer(participantAddress)
	}

	private[service] def onDelegateBecomeCommunicable(participantAddress: ContactAddress): Unit = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		clusterEventsListeners.forEach { (listener, _) => listener.onDelegateBecomeCommunicable(participantAddress) }
		for case (_, communicableDelegate: Communicable) <- behavior.delegateByAddress do
			communicableDelegate.notifyPeerThatIRecoveredCommunicationWithOtherPeer(participantAddress)
	}

	/** Tell all the participants that the participant at the specified address and me can not communicate becase we don't support a common [[ProtocolVersion]]. */
	private[service] def notifyVersionIncompatibilityWith(participantAddress: ContactAddress): Unit = ???

	private[service] def notifyParticipantHasBeenRestarted(rebornParticipantAddress: ContactAddress): Unit = ???

	private[service] def proposeClusterCreator(): Unit = ???

	private[service] def solveClusterExistenceConflictWith(participantDelegate: MemberCommunicableDelegate): Unit = ???

	private def release(): Unit = {
		clusterEventsListeners.forEach { (listener, _) => listener.beforeClosingAllChannels() }
		serverChannel.close()
		for case (_, communicableDelegate: Communicable) <- behavior.delegateByAddress do {
			communicableDelegate.release()
		}
	}
}
