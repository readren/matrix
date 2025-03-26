package readren.matrix
package cluster.service

import cluster.service.ClusterService.*
import cluster.service.Protocol.MembershipStatus.{MEMBER, UNKNOWN}
import cluster.service.Protocol.{ContactAddress, MembershipStatus}
import cluster.service.ProtocolVersion

import readren.taskflow.SchedulingExtension.MilliDuration
import readren.taskflow.{Doer, SchedulingExtension}

import java.net.InetSocketAddress
import java.nio.channels.{AsynchronousServerSocketChannel, AsynchronousSocketChannel, CompletionHandler}
import java.util.concurrent.TimeUnit
import scala.collection.MapView
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object ClusterService {

	abstract class TaskSequencer extends Doer, SchedulingExtension

	sealed trait ClusterEvent

	/** Tells that the participant at the specified address and me can not communicate becase we don't support a common [[ProtocolVersion]]. */
	case class VersionIncompatibilityWith(participantAddress: ContactAddress) extends ClusterEvent
	case class ParticipantHasBeenRestarted(rebornParticipantAddress: ContactAddress) extends ClusterEvent
	case class StartingConnectionToNewParticipant(participantDelegate: ParticipantDelegate & Incommunicable) extends ClusterEvent
	case class PeerConnected(participantDelegate: ParticipantDelegate) extends ClusterEvent
	case class Joined(participantDelegateByContactAddress: Map[ContactAddress, MemberCommunicableDelegate]) extends ClusterEvent
	case class OtherMemberJoined(memberAddress: ContactAddress, memberDelegate: MemberCommunicableDelegate) extends ClusterEvent
	case class DelegateBecomeIncommunicable(peerAddress: ContactAddress, cause: Any) extends ClusterEvent
	case class DelegateBecomeCommunicable(peerAddress: ContactAddress) extends ClusterEvent
	case class CommunicationChannelReplaced(peerAddress: ContactAddress) extends ClusterEvent
	case class BeforeClosingAllChannels() extends ClusterEvent

	trait ClusterEventListener {
		def handle(event: ClusterEvent): Unit
	}

	class Config(val myAddress: ContactAddress, val participantDelegatesConfig: DelegateConfig, val retryMinDelay: MilliDuration, val retryMaxDelay: MilliDuration, val maxAttempts: Int)

	class DelegateConfig(val versionsSupportedByMe: Set[ProtocolVersion], val receiverTimeout: Long = 1, val transmitterTimeout: Long = 1, val timeUnit: TimeUnit = TimeUnit.SECONDS)

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

		def createAndAddAnIncommunicableDelegate(participantAddress: ContactAddress, isConnectingAsClient: Boolean): ParticipantDelegate & Incommunicable

		def removeDelegate(participantAddress: ContactAddress): Unit

		def delegateByAddress: Map[ContactAddress, ParticipantDelegate & Communicability]
	}

	private[service] class AspirantBehavior(var aspirantDelegateByContactAddress: Map[ContactAddress, AspirantDelegate & Communicability]) extends Behavior {
		override def createAndAddACommunicableDelegate(participantAddress: ContactAddress, communicationChannel: AsynchronousSocketChannel): AspirantCommunicableDelegate = {
			val newParticipant = new AspirantCommunicableDelegate(thisClusterService, this, participantAddress, communicationChannel)
			aspirantDelegateByContactAddress += participantAddress -> newParticipant
			newParticipant
		}

		override def createAndAddAnIncommunicableDelegate(participantAddress: ContactAddress, isConnectingAsClient: Boolean): AspirantIncommunicableDelegate = {
			val newParticipant = new AspirantIncommunicableDelegate(thisClusterService, participantAddress, isConnectingAsClient)
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
			val newParticipant = MemberCommunicableDelegate(thisClusterService, this, participantAddress, communicationChannel)
			memberDelegateByContactAddress += participantAddress -> newParticipant
			newParticipant
		}

		override def createAndAddAnIncommunicableDelegate(participantAddress: ContactAddress, isConnectingAsClient: Boolean): MemberIncommunicableDelegate = {
			val newParticipant = new MemberIncommunicableDelegate(thisClusterService, participantAddress, isConnectingAsClient)
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
		behavior.delegateByAddress.exists(_._2.peerMembershipStatusAccordingToMe == MEMBER)
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
		behavior.delegateByAddress.view.filter(_._2.peerMembershipStatusAccordingToMe == MEMBER).mapValues { delegate => delegate.versionsSupportedByPeer }
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
								val newParticipantDelegate = behavior.createAndAddACommunicableDelegate(clientRemoteAddress, clientChannel)
								newParticipantDelegate.startConversationAsServer()

							case communicableParticipant: Communicable =>
								communicableParticipant.replaceMyselfWithACommunicableDelegate(clientChannel).startConversationAsServer()
								scribe.info(s"A connection from the participant at $clientRemoteAddress, whose delegate is marked as communicable, has been accepted. Probably he had to rest the channel.")

							case incommunicableParticipant: Incommunicable =>
								// If the current delegate is connecting as a client and the peer's address is greater than ours, discard the new connection (initiated by the peer) and keep the existing client connection (initiated by me).
								// The address comparison avoids a race condition where both sides' servers reject each other's client connections.
								if incommunicableParticipant.isConnectingAsClient && compareContactAddresses(clientRemoteAddress, config.myAddress) > 0 then {
									clientChannel.close()
									scribe.info(s"A connection from the participant at $clientRemoteAddress, which was marked as connecting as client, has been rejected in order to continue with the connection as a client.")
								} else {
									val communicableParticipant = incommunicableParticipant.replaceMyselfWithACommunicableDelegate(clientChannel)
									// TODO analyze if a notification of the communicability change should be issued here.
									scribe.info(s"A connection from the participant at $clientRemoteAddress, which was marked as incommunicable, has been accepted. Let's see if we can communicate with it this time.")
									communicableParticipant.startConversationAsServer()
								}
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
				if !participantByAddress.contains(seed) then createAndAddADelegateForAndThenConnectToParticipant(seed, Set.empty, UNKNOWN)
			}
		}
	}

	private[service] def createAndAddADelegateForAndThenConnectToParticipant(participantContactAddress: ContactAddress, versionsSupportedByPeer: Set[ProtocolVersion], membershipStatus: MembershipStatus): ParticipantDelegate & Incommunicable = {
		val connectingDelegate = behavior.createAndAddAnIncommunicableDelegate(participantContactAddress, true)
		connectingDelegate.initializeState(versionsSupportedByPeer, membershipStatus)
		notify(StartingConnectionToNewParticipant(connectingDelegate))

		connectTo(participantContactAddress) {
			case Success(communicationChannel) =>
				sequencer.executeSequentially {
					if connectingDelegate eq behavior.delegateByAddress.getOrElse(participantContactAddress, null) then {
						val communicableDelegate = connectingDelegate.replaceMyselfWithACommunicableDelegate(communicationChannel)
						communicableDelegate.startConversationAsClient()
					}
				}
			case Failure(exc) =>
				connectingDelegate.connectionAborted()
				scribe.error(s"The connection to the participant at $participantContactAddress has been aborted after many failed tries.")
		}
		connectingDelegate
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

	private[service] def proposeClusterCreator(): Unit = {
		???
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
		notify(DelegateBecomeIncommunicable(participantAddress, cause))
		for case (_, communicableDelegate: Communicable) <- behavior.delegateByAddress do
			communicableDelegate.notifyPeerThatILostCommunicationWithOtherPeer(participantAddress)
	}

	private[service] def onDelegateBecomeCommunicable(participantAddress: ContactAddress): Unit = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		notify(DelegateBecomeCommunicable(participantAddress))
		for case (_, communicableDelegate: Communicable) <- behavior.delegateByAddress do
			communicableDelegate.notifyPeerThatIRecoveredCommunicationWithOtherPeer(participantAddress)
	}

	private[service] def notify(event: ClusterEvent): Unit = {
		clusterEventsListeners.forEach { (listener, _) => listener.handle(event) }
	}

	private[service] def solveClusterExistenceConflictWith(participantDelegate: MemberCommunicableDelegate): Unit = ???

	private def release(): Unit = {
		notify(BeforeClosingAllChannels())
		serverChannel.close()
		for case (_, communicableDelegate: Communicable) <- behavior.delegateByAddress do {
			communicableDelegate.release()
		}
	}

	private def compareContactAddresses(ca1: ContactAddress, ca2: ContactAddress): Int = {
		(ca1, ca2) match {
			case (isa1: InetSocketAddress, isa2: InetSocketAddress) => compareInetSocketAddresses(isa1, isa2)
			case (o1, o2) => o1.toString.compareTo(o2.toString)
		}
	}

	private def compareInetSocketAddresses(isa1: InetSocketAddress, isa2: InetSocketAddress): Int = {
		// Get raw byte arrays once
		val bytes1 = isa1.getAddress.getAddress
		val bytes2 = isa2.getAddress.getAddress

		// Compare byte by byte without creating intermediate objects
		var i = 0
		val len = math.min(bytes1.length, bytes2.length)
		while (i < len) {
			val cmp = (bytes1(i) & 0xff) - (bytes2(i) & 0xff) // unsigned byte comparison
			if (cmp != 0) return cmp
			i += 1
		}

		// If all bytes equal up to common length, compare array lengths
		val lengthCmp = bytes1.length - bytes2.length
		if (lengthCmp != 0) return lengthCmp

		// Finally compare ports if IPs are equal
		isa1.getPort - isa2.getPort
	}

}
