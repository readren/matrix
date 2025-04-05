package readren.matrix
package cluster.service

import cluster.channel.Transmitter
import cluster.channel.Transmitter.{Delivered, NotDelivered}
import cluster.misc.CommonExtensions.getAndTransformOrElse
import cluster.service.ClusterService.*
import cluster.service.ContactCard.*
import cluster.service.IncommunicableDelegate.Reason
import cluster.service.IncommunicableDelegate.Reason.IS_CONNECTING_AS_CLIENT
import cluster.service.Protocol.MembershipStatus.{ASPIRANT, MEMBER, UNKNOWN}
import cluster.service.Protocol.*
import cluster.service.ProtocolVersion
import cluster.service.RingSerial.*

import readren.taskflow.SchedulingExtension.MilliDuration
import readren.taskflow.{Doer, SchedulingExtension}

import java.nio.channels.{AsynchronousServerSocketChannel, AsynchronousSocketChannel, CompletionHandler}
import java.util.concurrent.TimeUnit
import scala.collection.MapView
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object ClusterService {

	abstract class TaskSequencer extends Doer, SchedulingExtension

	trait Clock {
		def getTime: Instant
	}

	sealed trait ClusterEvent

	/** Tells that the participant at the specified address and me can not communicate becase we don't support a common [[ProtocolVersion]]. */
	case class VersionIncompatibilityWith(participantAddress: ContactAddress) extends ClusterEvent

	case class ParticipantHasBeenRestarted(rebornParticipantAddress: ContactAddress) extends ClusterEvent

	case class StartingConnectionToNewParticipant(participantAddress: ContactAddress) extends ClusterEvent

	case class PeerConnected(participantDelegate: ParticipantDelegate) extends ClusterEvent

	case class Joined(participantDelegateByContactAddress: Map[ContactAddress, ParticipantDelegate]) extends ClusterEvent

	case class OtherMemberJoined(memberAddress: ContactAddress, memberDelegate: MemberBehavior) extends ClusterEvent

	case class DelegateBecomeIncommunicable(peerAddress: ContactAddress, reason: Reason, cause: Any) extends ClusterEvent

	case class DelegateBecomeUnreachable(peerAddress: ContactAddress, cause: Any) extends ClusterEvent

	case class DelegateBecomeCommunicable(peerAddress: ContactAddress) extends ClusterEvent

	case class CommunicationChannelReplaced(peerAddress: ContactAddress) extends ClusterEvent

	case class BeforeClosingAllChannels() extends ClusterEvent

	trait ClusterEventListener {
		def handle(event: ClusterEvent): Unit
	}

	class Config(val myAddress: ContactAddress, val versionsISupport: Set[ProtocolVersion], val seeds: Iterable[ContactAddress], val participantDelegatesConfig: DelegateConfig, val retryMinDelay: MilliDuration = 1000, val retryMaxDelay: MilliDuration = 60_000, val maxAttempts: Int = 8, val joinCheckDelay: MilliDuration = 2_000)

	class DelegateConfig(val versionsSupportedByMe: Set[ProtocolVersion], val receiverTimeout: Long = 500, val transmitterTimeout: Long = 500, val timeUnit: TimeUnit = TimeUnit.MILLISECONDS, val responseTimeout: MilliDuration = 1_000, val closeDelay: MilliDuration = 200)

	def start(sequencer: TaskSequencer, clock: Clock, serviceConfig: Config): ClusterService = {

		val serverChannel = AsynchronousServerSocketChannel.open()
			.bind(serviceConfig.myAddress)

		val service = new ClusterService(sequencer, clock, serviceConfig, serverChannel)
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
class ClusterService private(val sequencer: TaskSequencer, clock: Clock, val config: ClusterService.Config, val serverChannel: AsynchronousServerSocketChannel) { thisClusterService =>

	export config.myAddress

	val myContactCard: ContactCard = (config.myAddress, config.versionsISupport)

	private val clusterEventsListeners: java.util.WeakHashMap[ClusterEventListener, None.type] = new java.util.WeakHashMap()

	private var participantDelegateByAddress: Map[ContactAddress, ParticipantDelegate] = Map.empty

	private var communicableDelegatesBehavior: CommunicableDelegate.Behavior = new AspirantBehavior(this)

	private var stateSerial: RingSerial = RingSerial.create()

	inline def myMembershipStatus: MembershipStatus = communicableDelegatesBehavior.membershipStatus

	inline def getCommunicableDelegatesBehavior: CommunicableDelegate.Behavior = communicableDelegatesBehavior

	inline def delegateByAddress: Map[ContactAddress, ParticipantDelegate] = participantDelegateByAddress

	def communicationStatusByParticipant: MapView[ContactAddress, CommunicationStatus] = {
		delegateByAddress.view.mapValues(_.communicationStatus)
	}

	private[service] def addANewCommunicableDelegate(participantAddress: ContactAddress, communicationChannel: AsynchronousSocketChannel): CommunicableDelegate = {
		assert(participantAddress != myAddress)
		val newParticipant = new CommunicableDelegate(thisClusterService, participantAddress, communicationChannel)
		participantDelegateByAddress += participantAddress -> newParticipant
		newParticipant
	}

	private[service] def addANewIncommunicableDelegate(participantAddress: ContactAddress, reason: Reason): IncommunicableDelegate = {
		assert(participantAddress != myAddress)
		val newParticipant = new IncommunicableDelegate(thisClusterService, participantAddress, reason)
		participantDelegateByAddress += participantAddress -> newParticipant
		newParticipant
	}

	private[service] inline def removeDelegate(participantAddress: ContactAddress): Unit =
		participantDelegateByAddress -= participantAddress

	/** Must be called within the [[sequencer]]. */
	def doesAClusterExist: Boolean = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		delegateByAddress.exists(_._2.peerMembershipStatusAccordingToMe eq MEMBER)
	}

	/**
	 * Gets the contact cards of all the participants known by this [[ClusterService]] such that the participant is incommunicable or the handshake has been already done.
	 * Participants whose communicability is being investigated (because they are still in the handshake stage) are not included.
	 * Must be called within the [[sequencer]]. */
	def getKnownParticipantsCards: MapView[ContactAddress, Set[ProtocolVersion]] = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		delegateByAddress.view.mapValues(_.versionsSupportedByPeer).filter(_._2.nonEmpty)
	}

	/**
	 * Gets the contact cards of all the participants known by this [[ClusterService]] such that the participant is member of the cluster according to this [[ClusterService]].
	 * Assumes that all members are either incommunicable or communicable with the handshake already completed.
	 * Must be called within the [[sequencer]]. */
	def getKnownMembersCards: MapView[ContactAddress, Set[ProtocolVersion]] = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		delegateByAddress.view.filter(_._2.peerMembershipStatusAccordingToMe eq MEMBER).mapValues { delegate => delegate.versionsSupportedByPeer }
	}

	def getKnownParticipantsInfo: MapView[ContactAddress, ParticipantInfo] =
		delegateByAddress.view.mapValues(_.info)

	/**
	 * As long as this [[ClusterService]]'s membership status is [[ASPIRANT]], this method must be called whenever:
	 *  - a delegate is added to, or removed from, the [[ClusterService.delegateByAddress]] map.
	 *  - the communicability of a delegates changes, including changes in [[CommunicableDelegate.agreedVersion]] and [[ParticipantDelegate.communicationStatus]].
	 *  - the [[ParticipantDelegate.versionsSupportedByPeer]] changes, because on it depends the [[ContactCard.ordering]].
	 *  - the [[ParticipantDelegate.peerMembershipStatusAccordingToMe]] changes.
	 *  - the [[CommunicableDelegate.clusterCreatorProposedByPeer]] changes.
	 * IMPORTANT: This method may change the membership status of this service; therefore, protocol-message handlers that call this method should avoid doing anything after the call that assumes the previous membership status. */
	private[service] def updateClusterCreatorProposalIfAppropriate(): Unit = {
		val iCanCommunicateToAllSeeds = config.seeds.forall { seed =>
			seed == config.myAddress || delegateByAddress.getAndTransformOrElse(seed, false)(_.isCommunicable)
		}
		// if I can communicate with all the seeds and all delegates are aspirants with stable communicability, then propose the cluster creator.
		if iCanCommunicateToAllSeeds && delegateByAddress.valuesIterator.forall(delegate => delegate.isStable && (delegate.peerMembershipStatusAccordingToMe eq ASPIRANT)) then {

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
				// Creating the cluster consist of: changing the behavior of communicable delegates to `MemberBehavior` and sending the `ICreatedACluster` message to the participant I can communicate with.
				switchToMember()
				stateSerial = stateSerial.incremented
				val myViewpoint = ParticipantViewpoint(stateSerial, clock.getTime, MEMBER, communicationStatusByParticipant.toMap)
				for case communicable: CommunicableDelegate <- delegateByAddress.valuesIterator do {
					communicable.notifyPeerThatICreatedTheCluster(myViewpoint)
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

	private var aRequestToJoinIsOnTheWay: Boolean = false

	private[service] def sendRequestToJoinTheClusterIfAppropriate(): Unit = {
		// if a request isn't on the way, a cluster exists, and the communicability of all the delegates is stable, then take the communicable delegate of the member with the lowest [[ContactCard]] and send a request to join to it
		if !aRequestToJoinIsOnTheWay && doesAClusterExist && delegateByAddress.iterator.forall(_._2.isStable) then {
			delegateByAddress.iterator
				.collect { case (_, cd: CommunicableDelegate) if cd.peerMembershipStatusAccordingToMe eq MEMBER => cd }
				.minByOption(_.contactCard)(using ContactCard.ordering)
				.foreach { chosenMember =>
					aRequestToJoinIsOnTheWay = true
					val joinTokenByMember = delegateByAddress.iterator
						.collect { case (_, delegate) if delegate.peerMembershipStatusAccordingToMe eq MEMBER => delegate.peerAddress -> 0L } // TODO add token logic or remove them.
						.toMap

					chosenMember.sendRequestToJoin(joinTokenByMember) { report =>
						val tryAgainIfAppropriate: Runnable = { () =>
							aRequestToJoinIsOnTheWay = false
							if myMembershipStatus eq ASPIRANT then sendRequestToJoinTheClusterIfAppropriate()
						}
						report match {
							case Delivered =>
								sequencer.scheduleSequentially(sequencer.newDelaySchedule(config.joinCheckDelay))(tryAgainIfAppropriate)
							case nd: NotDelivered =>
								sequencer.executeSequentially {
									chosenMember.reportFailure(nd)
									chosenMember.restartChannel(nd)
									tryAgainIfAppropriate.run()
								}
						}
					}
				}
		}
	}

	private[service] def switchToMember(): Unit = {
		communicableDelegatesBehavior = new MemberBehavior(this)
		notify(Joined(delegateByAddress))
	}

	private[service] def solveClusterExistenceConflictWith(communicableDelegate: CommunicableDelegate): Boolean = ???

	private[service] def onDelegateBecomeIncommunicable(participantAddress: ContactAddress, reason: Reason, cause: Any): Unit = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		notify(DelegateBecomeIncommunicable(participantAddress, reason, cause))
		for case communicableDelegate: CommunicableDelegate <- delegateByAddress.valuesIterator do
			communicableDelegate.notifyPeerThatILostCommunicationWithOtherPeer(participantAddress)
	}

	private[service] def onDelegateBecomeCommunicable(participantAddress: ContactAddress): Unit = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		notify(DelegateBecomeCommunicable(participantAddress))
		for case communicableDelegate: CommunicableDelegate <- delegateByAddress.valuesIterator do
			communicableDelegate.notifyPeerThatIRecoveredCommunicationWithOtherPeer(participantAddress)
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
								val newParticipantDelegate = addANewCommunicableDelegate(clientRemoteAddress, clientChannel)
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
				if !delegateByAddress.contains(seed) then addANewDelegateForAndThenConnectToAndThenStartConversationWithParticipant(seed, Set.empty, UNKNOWN)
			}
		}
	}

	private[service] def addANewDelegateForAndThenConnectToAndThenStartConversationWithParticipant(participantContactAddress: ContactAddress, versionsSupportedByPeer: Set[ProtocolVersion], membershipStatus: MembershipStatus): IncommunicableDelegate = {
		val connectingDelegate = addANewIncommunicableDelegate(participantContactAddress, IS_CONNECTING_AS_CLIENT)
		connectingDelegate.initializeState(versionsSupportedByPeer, membershipStatus)
		communicableDelegatesBehavior.onDelegatedAdded(connectingDelegate)
		notify(StartingConnectionToNewParticipant(connectingDelegate.peerAddress))

		connectTo(participantContactAddress) {
			case Success(communicationChannel) =>
				sequencer.executeSequentially {
					if connectingDelegate eq delegateByAddress.getOrElse(participantContactAddress, null) then {
						val communicableDelegate = connectingDelegate.replaceMyselfWithACommunicableDelegate(communicationChannel)
						communicableDelegate.startConversationAsClient(false)
					}
				}
			case Failure(exc) =>
				connectingDelegate.onConnectionAborted(exc)
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

	def subscribe(listener: ClusterEventListener): Unit = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		clusterEventsListeners.put(listener, None)
	}

	def unsubscribe(listener: ClusterEventListener): Boolean = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		clusterEventsListeners.remove(listener) eq None
	}

	private[service] def notify(event: ClusterEvent): Unit = {
		clusterEventsListeners.forEach { (listener, _) => listener.handle(event) }
	}

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
}
