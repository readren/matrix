package readren.matrix
package cluster.service

import cluster.channel.Transmitter
import cluster.misc.CommonExtensions.getAndTransformOrElse
import cluster.service.ClusterService.*
import cluster.service.ContactCard.*
import cluster.service.Protocol.MembershipStatus.{ASPIRANT, MEMBER, UNKNOWN}
import cluster.service.Protocol.{ContactAddress, MembershipStatus}
import cluster.service.ProtocolVersion

import readren.taskflow.SchedulingExtension.MilliDuration
import readren.taskflow.{Doer, SchedulingExtension}

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

	case class StartingConnectionToNewParticipant(participantAddress: ContactAddress) extends ClusterEvent

	case class PeerConnected(participantDelegate: ParticipantDelegate) extends ClusterEvent

	case class Joined(participantDelegateByContactAddress: Map[ContactAddress, MemberBehavior]) extends ClusterEvent

	case class OtherMemberJoined(memberAddress: ContactAddress, memberDelegate: MemberBehavior) extends ClusterEvent

	case class DelegateBecomeIncommunicable(peerAddress: ContactAddress, cause: Any) extends ClusterEvent

	case class DelegateBecomeCommunicable(peerAddress: ContactAddress) extends ClusterEvent

	case class CommunicationChannelReplaced(peerAddress: ContactAddress) extends ClusterEvent

	case class BeforeClosingAllChannels() extends ClusterEvent

	trait ClusterEventListener {
		def handle(event: ClusterEvent): Unit
	}

	class Config(val myAddress: ContactAddress, val versionsISupport: Set[ProtocolVersion], val seeds: Iterable[ContactAddress], val participantDelegatesConfig: DelegateConfig, val retryMinDelay: MilliDuration, val retryMaxDelay: MilliDuration, val maxAttempts: Int)

	class DelegateConfig(val versionsSupportedByMe: Set[ProtocolVersion], val receiverTimeout: Long = 1, val transmitterTimeout: Long = 1, val timeUnit: TimeUnit = TimeUnit.SECONDS, val closeDelay: MilliDuration = 200)

	def start(sequencer: TaskSequencer, serviceConfig: Config): ClusterService = {

		val serverChannel = AsynchronousServerSocketChannel.open()
			.bind(serviceConfig.myAddress)

		val service = new ClusterService(sequencer, serviceConfig, serverChannel)
		// start the listening servers
		service.acceptClientConnections(serverChannel)
		// start connection to seeds
		service.startConnectionToSeeds(serviceConfig.seeds)
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

	val myContactCard: ContactCard = (config.myAddress, config.versionsISupport)

	private val clusterEventsListeners: java.util.WeakHashMap[ClusterEventListener, Unit] = new java.util.WeakHashMap()

	private var participantDelegateByAddress: Map[ContactAddress, ParticipantDelegate] = Map.empty

	private var communicableDelegatesBehavior: CommunicableDelegate.Behavior = new AspirantBehavior(this)

	inline def getCommunicableDelegatesBehavior: CommunicableDelegate.Behavior = communicableDelegatesBehavior

	private[service] def createAndAddACommunicableDelegate(participantAddress: ContactAddress, communicationChannel: AsynchronousSocketChannel): CommunicableDelegate = {
		val newParticipant = new CommunicableDelegate(thisClusterService, participantAddress, communicationChannel)
		participantDelegateByAddress += participantAddress -> newParticipant
		newParticipant
	}

	private[service] def createAndAddAnIncommunicableDelegate(participantAddress: ContactAddress, isConnectingAsClient: Boolean): IncommunicableDelegate = {
		val newParticipant = new IncommunicableDelegate(thisClusterService, participantAddress, isConnectingAsClient)
		participantDelegateByAddress += participantAddress -> newParticipant
		newParticipant
	}

	private[service] inline def removeDelegate(participantAddress: ContactAddress): Unit =
		participantDelegateByAddress -= participantAddress

	inline def delegateByAddress: Map[ContactAddress, ParticipantDelegate] =
		participantDelegateByAddress

	private[service] def proposeClusterCreatorIfAppropriate(): Unit = {
		val iCanCommunicateToAllSeeds = config.seeds.forall { seed =>
			seed == config.myAddress || delegateByAddress.getAndTransformOrElse(seed, false)(_.isCommunicable)
		}
		// if I can communicate with all the seeds and all delegates are aspirants with stable communicability, then propose the cluster creator.
		if iCanCommunicateToAllSeeds && delegateByAddress.valuesIterator.forall(delegate => delegate.isStable && delegate.peerMembershipStatusAccordingToMe == ASPIRANT) then {

			// Determine the candidate from my viewpoint
			val candidateProposedByMe = delegateByAddress.valuesIterator
				.filter(_.isCommunicable)
				.foldLeft(myContactCard) { (min, delegate) =>
					ContactCard.ordering.min(delegate.contactCard, min)
				}

			// First check if I should be the candidate: if me and all the aspirant I can communicate with propose me to be the cluster creator, then create it.
			if candidateProposedByMe == myAddress && delegateByAddress.valuesIterator.forall {
				case communicableDelegate: CommunicableDelegate =>
					communicableDelegate.clusterCreatorProposedByPeer == myAddress
				case _ => true
			} then {
				// Creating the cluster consist of: changing the behavior of communicable delegates to `MemberBehavior`, their membership status to MEMBER, and sending the `ICreatedACluster` message to their peers.
				communicableDelegatesBehavior = new MemberBehavior(this)
				val communicableDelegatesByAddress = delegateByAddress.iterator.collect {
					case (address, delegate: CommunicableDelegate) =>
						delegate.peerMembershipStatusAccordingToMe = MEMBER
						(address, delegate)
				}.toMap
				for communicableDelegate <- communicableDelegatesByAddress.valuesIterator do {
					communicableDelegate.notifyPeerThatICreatedTheCluster(communicableDelegatesByAddress)
				}
			} else {
				for case communicable: CommunicableDelegate <- delegateByAddress.valuesIterator do {
					communicable.notifyPeerTheAspirantIProposeToBeTheClusterCreator(candidateProposedByMe.address, candidateProposedByMe.supportedVersions)
				}
			}
		}
		// if the condition for proposal are not meet, undo the old proposal if any.
		else {
			for case communicable: CommunicableDelegate <- delegateByAddress.valuesIterator do {
				communicable.notifyPeerTheAspirantIProposeToBeTheClusterCreator(null, Set.empty)
			}
		}
	}

	/** Must be called within the [[sequencer]]. */
	def doesAClusterExist: Boolean = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		delegateByAddress.valuesIterator.exists(_.peerMembershipStatusAccordingToMe == MEMBER)
	}

	/**
	 * Gets the contact cards of all the participants known by this [[ClusterService]] such that the participant is incommunicable or the handshake has been already done.
	 * Participants whose communicability is being investigated (because they are still in the handshake stage) are not included.
	 * Must be called within the [[sequencer]]. */
	def getKnownParticipantsCards: MapView[ContactAddress, Set[ProtocolVersion]] = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		delegateByAddress.view.mapValues { delegate => delegate.versionsSupportedByPeer }.filter {
			_._2.nonEmpty
		}
	}

	/**
	 * Gets the contact cards of all the participants known by this [[ClusterService]] such that the participant is member of the cluster according to this [[ClusterService]].
	 * Assumes that all members are either incommunicable or communicable with the handshake already completed.
	 * Must be called within the [[sequencer]]. */
	def getKnownMembersCards: MapView[ContactAddress, Set[ProtocolVersion]] = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		delegateByAddress.view.filter(_._2.peerMembershipStatusAccordingToMe == MEMBER).mapValues { delegate => delegate.versionsSupportedByPeer }
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
						delegateByAddress.getOrElse(clientRemoteAddress, null) match {
							case null =>
								val newParticipantDelegate = createAndAddACommunicableDelegate(clientRemoteAddress, clientChannel)
								newParticipantDelegate.startConversationAsServer()

							case communicable: CommunicableDelegate =>
								communicable.replaceMyselfWithACommunicableDelegate(clientChannel)
									.startConversationAsServer()
								scribe.info(s"A connection from the participant at $clientRemoteAddress, whose delegate is marked as communicable, has been accepted. Probably he had to restart the channel.")

							case incommunicable: IncommunicableDelegate =>
								// If the current delegate is connecting as a client and the peer's address is greater than ours, discard the new connection (initiated by the peer) and keep the existing client connection (initiated by me).
								// The address comparison avoids a race condition where both sides' servers reject each other's client connections.
								if incommunicable.isConnectingAsClient && ContactCard.compareContactAddresses(clientRemoteAddress, config.myAddress) > 0 then {
									closeUnusedChannelGracefully(clientChannel)
									scribe.info(s"A connection from the participant at $clientRemoteAddress, which was marked as connecting as client, has been rejected in order to continue with the connection as a client.")
								} else {
									val communicableParticipant = incommunicable.replaceMyselfWithACommunicableDelegate(clientChannel)
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
				if !delegateByAddress.contains(seed) then andAddANewDelegateForAndThenConnectToAndThenStartConversationWithParticipant(seed, Set.empty, UNKNOWN)
			}
		}
	}

	private[service] def andAddANewDelegateForAndThenConnectToAndThenStartConversationWithParticipant(participantContactAddress: ContactAddress, versionsSupportedByPeer: Set[ProtocolVersion], membershipStatus: MembershipStatus): IncommunicableDelegate = {
		val connectingDelegate = createAndAddAnIncommunicableDelegate(participantContactAddress, true)
		connectingDelegate.initializeState(versionsSupportedByPeer, membershipStatus)
		notify(StartingConnectionToNewParticipant(connectingDelegate.peerAddress))

		connectTo(participantContactAddress) {
			case Success(communicationChannel) =>
				sequencer.executeSequentially {
					if connectingDelegate eq delegateByAddress.getOrElse(participantContactAddress, null) then {
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

	private[service] def solveClusterExistenceConflictWith(communicableDelegate: CommunicableDelegate): Unit = ???

	private[service] def closeUnusedChannelGracefully(channel: AsynchronousSocketChannel): Unit = {
		val transmitter = new Transmitter(channel)
		channel.shutdownInput()
		transmitter.transmit[Protocol](IAmDeaf, ProtocolVersion.OF_THIS_PROJECT, config.participantDelegatesConfig.transmitterTimeout, config.participantDelegatesConfig.timeUnit) {
			case failure: Transmitter.NotDelivered =>
				channel.close()
			case Transmitter.Delivered =>
				channel.shutdownOutput()
				sequencer.scheduleSequentially(sequencer.newDelaySchedule(config.participantDelegatesConfig.closeDelay)) { () => channel.close() }
		}
	}

	private def release(): Unit = {
		notify(BeforeClosingAllChannels())
		serverChannel.close()
		for case communicableDelegate: CommunicableDelegate <- delegateByAddress.valuesIterator do {
			communicableDelegate.release()
		}
	}

	private[service] def onDelegateBecomeIncommunicable(participantAddress: ContactAddress, cause: Any): Unit = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		notify(DelegateBecomeIncommunicable(participantAddress, cause))
		for case communicableDelegate: CommunicableDelegate <- delegateByAddress.valuesIterator do
			communicableDelegate.notifyPeerThatILostCommunicationWithOtherPeer(participantAddress)
	}

	private[service] def onDelegateBecomeCommunicable(participantAddress: ContactAddress): Unit = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		notify(DelegateBecomeCommunicable(participantAddress))
		for case communicableDelegate: CommunicableDelegate <- delegateByAddress.valuesIterator do
			communicableDelegate.notifyPeerThatIRecoveredCommunicationWithOtherPeer(participantAddress)
	}

	def subscribe(listener: ClusterEventListener): Unit = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		clusterEventsListeners.put(listener, ())
	}

	def unsubscribe(listener: ClusterEventListener): Boolean = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		clusterEventsListeners.remove(listener) == ()
	}

	private[service] def notify(event: ClusterEvent): Unit = {
		clusterEventsListeners.forEach { (listener, _) => listener.handle(event) }
	}
}
