package readren.matrix
package cluster.service

import cluster.channel.Transmitter
import cluster.service.ClusterService.*
import cluster.service.ClusterService.ChannelOrigin.{CLIENT_INITIATED, SERVER_ACCEPTED}
import cluster.service.Protocol.*
import cluster.service.Protocol.IncommunicabilityReason.IS_CONNECTING_AS_CLIENT
import cluster.service.Protocol.MembershipStatus.MEMBER
import cluster.service.ProtocolVersion
import cluster.service.RingSerial.*

import readren.matrix.cluster.misc.TaskSequencer
import readren.taskflow.SchedulingExtension.MilliDuration
import readren.taskflow.{Doer, SchedulingExtension}

import java.nio.channels.{AsynchronousServerSocketChannel, AsynchronousSocketChannel, CompletionHandler}
import java.util.concurrent.TimeUnit
import scala.collection.MapView
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object ClusterService {

	trait Clock {
		def getTime: Instant
	}

	trait ClusterEventListener {
		def handle(event: ClusterEvent): Unit
	}

	class Config(val myAddress: ContactAddress, val versionsISupport: Set[ProtocolVersion], val seeds: Iterable[ContactAddress], val participantDelegatesConfig: DelegateConfig, val retryMinDelay: MilliDuration = 1000, val retryMaxDelay: MilliDuration = 60_000, val maxAttempts: Int = 8, val joinCheckDelay: MilliDuration = 2_000)

	/** @param requestsAttempts how many times a [[CommunicableDelegate.askPeer]] should be called in total when the previous try resulted in a response timeout.
	 * Since the [[AsynchronousSocketChannel]] uses TCP, it ensures loss-less in-order delivery. Therefore, the purpose to retry a request after a no-response timeout is to reveal any silent communication problem or to detect silent middlebox interference.
	 * If the peer is alive but not responding (e.g., deadlock, overload), retries would not help. In conclusion, no more than a single retry is needed. 
	 */
	class DelegateConfig(
		val receiverTimeout: Long = 60_000, 
		val transmitterTimeout: Long = 500, 
		val timeUnit: TimeUnit = TimeUnit.MILLISECONDS, 
		val responseTimeout: MilliDuration = 1_000,
		val requestRetryStartingDelay: MilliDuration = 2_000,
		val requestsAttempts: Int = 2,
		val closeDelay: MilliDuration = 200, 
		val heartbeatPeriod: MilliDuration = 10_000, 
		val heartbeatMargin: MilliDuration = 12_000,
	)

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

	/** The origins of an [[AsynchronousSocketChannel]] instance. */
	enum ChannelOrigin {
		case CLIENT_INITIATED, SERVER_ACCEPTED
	}
}

/**
 * A service that manages the propagation of its own existence and the awareness of other [[ClusterService]] instances
 * to support intercommunication between their users across different JVMs, which maybe (and usually are) on different host machines.
 *
 * For brevity, instances of this class are referred to as "participants."
 * Depending on its membership status (from its own viewpoint), a participant behaves either as a "member" or as an "aspirant"
 * to become a member of the cluster.
 *
 * The [[ClusterService]] class delegates the knowledge about, and communication with, other participants to implementations
 * of the [[ParticipantDelegate]] trait: it creates one delegate per participant it is aware of.
 */
class ClusterService private(val sequencer: TaskSequencer, val clock: Clock, val config: ClusterService.Config, val serverChannel: AsynchronousServerSocketChannel) { thisClusterService =>

	export config.myAddress

	val myContactCard: ContactCard = (config.myAddress, config.versionsISupport)

	private val clusterEventsListeners: java.util.WeakHashMap[ClusterEventListener, None.type] = new java.util.WeakHashMap()

	private val creationInstant: Instant = clock.getTime

	private var participantDelegateByAddress: Map[ContactAddress, ParticipantDelegate] = Map.empty

	private var membershipScopedBehavior: MembershipScopedBehavior = new AspirantBehavior(this)

	inline def myCreationInstant: Instant = creationInstant

	inline def myMembershipStatus: MembershipStatus = membershipScopedBehavior.membershipStatus

	inline def getMembershipScopedBehavior: MembershipScopedBehavior = membershipScopedBehavior

	inline def delegateByAddress: Map[ContactAddress, ParticipantDelegate] = participantDelegateByAddress

	private[service] def addANewCommunicableDelegate(participantAddress: ContactAddress, communicationChannel: AsynchronousSocketChannel, channelOrigin: ChannelOrigin): CommunicableDelegate = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		assert(participantAddress != myAddress)
		val newParticipant = new CommunicableDelegate(thisClusterService, participantAddress, communicationChannel, channelOrigin)
		participantDelegateByAddress += participantAddress -> newParticipant
		newParticipant
	}

	private[service] def addANewIncommunicableDelegate(participantAddress: ContactAddress, reason: IncommunicabilityReason): IncommunicableDelegate = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		assert(participantAddress != myAddress)
		val newParticipant = new IncommunicableDelegate(thisClusterService, participantAddress, reason)
		participantDelegateByAddress += participantAddress -> newParticipant
		newParticipant
	}

	private[service] inline def removeDelegate(delegate: ParticipantDelegate, notifyListeners: Boolean): Boolean = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		delegateByAddress.getOrElse(delegate.peerAddress, null) match {
			case referencedDelegate if referencedDelegate eq delegate =>
				participantDelegateByAddress -= delegate.peerAddress
				if notifyListeners then notifyListenersThat(ParticipantHasGone(delegate.peerAddress))
				true
			case _ => false
		}
	}

	/** Must be called within the [[sequencer]]. */
	def doesAClusterExist: Boolean = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		delegateByAddress.exists(_._2.peerMembershipStatusAccordingToMe eq MEMBER)
	}

	/**
	 * Gets the address of all the participants known by this [[ClusterService]]. */
	def getKnownParticipantsAddresses: Set[ContactAddress] = {
		participantDelegateByAddress.keySet
	}

	def getKnownParticipantsMembershipStatus: Map[ContactAddress, MembershipStatus] = {
		val mapBuilder = Map.newBuilder[ContactAddress, MembershipStatus]
		delegateByAddress.foreachEntry { (address, delegate) => mapBuilder.addOne(address, delegate.peerMembershipStatusAccordingToMe) }
		mapBuilder.result()
	}

	def getKnownParticipantsInfo: MapView[ContactAddress, ParticipantInfo] = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		delegateByAddress.view.mapValues(_.info)
	}

	def createADelegateForEachParticipantIDoNotKnowIn(participantsKnownByPeer: Set[ContactAddress]): Unit = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		// Create a delegate for each participant that I did not know.
		for participantAddress <- participantsKnownByPeer do {
			if participantAddress != myAddress && !delegateByAddress.contains(participantAddress) then {
				addANewConnectingDelegateAndStartAConnectionToThenAConversationWithParticipant(participantAddress)
			}
		}
	}

	private[service] def determineAgreedVersion(versionsSupportedByPeer: Set[ProtocolVersion]): Option[ProtocolVersion] = {
		val versionsSupportedByBoth = config.versionsISupport.intersect(versionsSupportedByPeer)
		versionsSupportedByBoth.minOption(using ProtocolVersion.newerFirstOrdering)
	}

	private[service] def switchToMember(clusterCreationInstant: Instant): MemberBehavior = {
		val memberBehavior = new MemberBehavior(this, clusterCreationInstant)
		membershipScopedBehavior = memberBehavior
		notifyListenersThat(IJoinedTheCluster(delegateByAddress))
		memberBehavior
	}

	private[service] def switchToResolvingBrainJoin(): Unit = ???

	private[service] def solveClusterExistenceConflictWith(communicableDelegate: CommunicableDelegate): Boolean = ???

	private[service] def onDelegateBecomeIncommunicable(participantAddress: ContactAddress, reason: IncommunicabilityReason, cause: Any): Unit = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		notifyListenersThat(DelegateBecomeIncommunicable(participantAddress, reason, cause))
		for case communicableDelegate: CommunicableDelegate <- delegateByAddress.valuesIterator do
			communicableDelegate.notifyPeerThatILostCommunicationWith(participantAddress)
	}

	private[service] def onConversationStarted(participantAddress: ContactAddress): Unit = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		notifyListenersThat(DelegateStartedConversationWith(participantAddress))
		for case communicableDelegate: CommunicableDelegate <- delegateByAddress.valuesIterator do
			communicableDelegate.notifyPeerThatAConversationStartedWith(participantAddress)
	}

	/**
	 * Manages incoming client connections to the server component of this [[ClusterService]].
	 *
	 * @param serverChannel The [[AsynchronousServerSocketChannel]] used to listen for and accept client connections.
	 */
	private def acceptClientConnections(serverChannel: AsynchronousServerSocketChannel): Unit = {
		try serverChannel.accept(null, new CompletionHandler[AsynchronousSocketChannel, Null]() {
			override def completed(channel: AsynchronousSocketChannel, attachment: Null): Unit = {
				// Note that this method body is executed sequentially by nature of the NIO2. Nevertheless, the `executeSequentially` is necessary because some of the member variables accessed here need to be accessed by procedures that are started from other handlers that are concurrent.
				sequencer.executeSequentially {
					try {
						val clientParticipantAddress = channel.getRemoteAddress
						delegateByAddress.getOrElse(clientParticipantAddress, null) match {
							case null =>
								val newParticipantDelegate = addANewCommunicableDelegate(clientParticipantAddress, channel, SERVER_ACCEPTED)
								newParticipantDelegate.startConversationAsServer()

							case communicable: CommunicableDelegate =>
								communicable.replaceMyselfWithACommunicableDelegate(channel, SERVER_ACCEPTED).get.startConversationAsServer()
								scribe.info(s"A connection from the participant at $clientParticipantAddress, whose delegate is marked as communicable, has been accepted. Probably he had to restart the channel.")


							case incommunicable: IncommunicableDelegate => // Happens if I knew the peer, but we are not fully connected.
								// If I am simultaneously initiating a connection to the peer as a client, then I have to decide which connection to keep: the one previously initiated by me as a client of the peer (which is not completed jet), or the one initiated by the peer (as a client of mine) that I am accepting now?
								if incommunicable.isConnectingAsClient then {
									// Note that the peer may be having the same problem right now. He has to decide which connection to keep: the one that he initiated as a client (and I am accepting now), or the one I previously initiated as a client (and he may be accepting now). So we (the peer and me) have to agree which connection to keep.
									// We can't just keep the connection that completed earlier because that is vulnerable to race condition because each side may see a different connection completing first.
									// We have to establish an absolute and global criteria for this decision which is the comparison of the addresses: the kept connection is the one in which the server has the highest address. The other connection is gracefully closed.
									// If the kept channel is the one with me at the client side, then I have to discard the connection accepted here (initiated by the peer) and let the connection with me at client side continue.
									if whichChannelShouldIKeepWhenMutualConnectionWith(clientParticipantAddress) eq CLIENT_INITIATED then {
										closeDiscardedChannelGracefully(channel, SERVER_ACCEPTED)
										scribe.info(s"A connection from the participant at $clientParticipantAddress, which was marked as connecting as client, has been rejected in order to continue with the connection as a client.")
									}
									// else I have to discard the channel with me as a client, and continue with the connection accepted here (with me as a server).
									else {
										val communicableParticipant = incommunicable.replaceMyselfWithACommunicableDelegate(channel, SERVER_ACCEPTED).get
										// TODO analyze if a notification of the communicability change should be issued here.
										scribe.info(s"A connection from the participant at $clientParticipantAddress was accepted despite I am simultaneously trying to connect to him as client. I assume I will close the connection in which I am the client as soon as it completes.") // TODO investigate if it is possible to cancel the connection while it is in progress.
										communicableParticipant.startConversationAsServer()
									}
								}
								// If I know the peer and I am not currently trying to connect
								else {
									val communicableParticipant = incommunicable.replaceMyselfWithACommunicableDelegate(channel, SERVER_ACCEPTED)
									// TODO analyze if a notification of the communicability change should be issued here.
									scribe.info(s"A connection from the participant at $clientParticipantAddress, which was marked as incommunicable ${incommunicable.communicationStatus}, has been accepted. Let's see if we can communicate with it this time.")
									communicableParticipant.get.startConversationAsServer()
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
				if !delegateByAddress.contains(seed) then addANewConnectingDelegateAndStartAConnectionToThenAConversationWithParticipant(seed)
			}
		}
	}

	/** @return a brand-new [[IncommunicableDelegate]] in connecting state. */
	private[service] def addANewConnectingDelegateAndStartAConnectionToThenAConversationWithParticipant(participantContactAddress: ContactAddress): IncommunicableDelegate = {
		val connectingDelegate = addANewIncommunicableDelegate(participantContactAddress, IS_CONNECTING_AS_CLIENT)
		membershipScopedBehavior.onDelegatedAdded(connectingDelegate)
		notifyListenersThat(IStartedAConnectionToANewParticipant(connectingDelegate.peerAddress))
		connectToAndThenStartConversationWithParticipant(connectingDelegate, false)
		connectingDelegate
	}

	private[service] def connectToAndThenStartConversationWithParticipant(participantConnectingDelegate: IncommunicableDelegate, isReconnection: Boolean): Unit = {
		assert(participantConnectingDelegate.isConnectingAsClient)
		connectTo(participantConnectingDelegate.peerAddress) {
			case Success(communicationChannel) =>
				sequencer.executeSequentially {
					val currentDelegate = delegateByAddress.getOrElse(participantConnectingDelegate.peerAddress, null)
					// if the `connectingDelegate` was not removed in the middle, asociate a communicable delegate to the peer (replacing the connecting one).
					if participantConnectingDelegate eq currentDelegate then {
						participantConnectingDelegate.replaceMyselfWithACommunicableDelegate(communicationChannel, CLIENT_INITIATED).get.startConversationAsClient(isReconnection)
					}
					// else (if the `connectingDelegate` was removed in the middle), close the communication channel gracefully.
					else {
						if participantConnectingDelegate.isConnectingAsClient && currentDelegate.isInstanceOf[CommunicableDelegate] && (whichChannelShouldIKeepWhenMutualConnectionWith(participantConnectingDelegate.peerAddress) eq SERVER_ACCEPTED) then {
							scribe.info(s"Closing the brand-new connection to ${participantConnectingDelegate.peerAddress} (with me as a client) because a connection to him with me as server was completed in the middle and my address is greater than the peer's")
						} else {
							scribe.warn(s"Closing a brand-new connection to ${participantConnectingDelegate.peerAddress} because the associated delegate was removed.")
						}
						closeDiscardedChannelGracefully(communicationChannel, CLIENT_INITIATED)
					}
				}
			case Failure(exc) =>
				participantConnectingDelegate.onConnectionAborted(exc)
				scribe.error(s"The connection to the participant at ${participantConnectingDelegate.peerAddress} has been aborted after many failed tries.")
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

	/** Criteria that determines which connection channel to keep (and which to discard) when two participants initiate a connection to each other simultaneously.
	 * The kept connection is the one in which the participant with the highest address is the server. The other connection should be gracefully closed.
	 * Form the participant perspective, it should keep the connection whose channel's origin is [[SERVER_ACCEPTED]] if it has a higher address than the peer, and [[CLIENT_INITIATED]] otherwise.
	 * @param peerAddress the [[ContactAddress]] of the participant I am mutually connected with.
	 * @return the [[ChannelOrigin]] that the kept channel should have.
	 */
	def whichChannelShouldIKeepWhenMutualConnectionWith(peerAddress: ContactAddress): ChannelOrigin = {
		if ContactCard.compareContactAddresses(peerAddress, config.myAddress) < 0 then SERVER_ACCEPTED else CLIENT_INITIATED
	}


	def subscribe(listener: ClusterEventListener): Unit = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		clusterEventsListeners.put(listener, None)
	}

	def unsubscribe(listener: ClusterEventListener): Boolean = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		clusterEventsListeners.remove(listener) eq None
	}

	private[service] def notifyListenersThat(event: ClusterEvent): Unit = {
		clusterEventsListeners.forEach { (listener, _) => listener.handle(event) }
	}

	private[service] def closeDiscardedChannelGracefully(discardedChannel: AsynchronousSocketChannel, channelOrigin: ChannelOrigin): Unit = {
		val transmitter = new Transmitter(discardedChannel)
		discardedChannel.shutdownInput()
		transmitter.transmit[Protocol](ChannelDiscarded(channelOrigin eq CLIENT_INITIATED), ProtocolVersion.OF_THIS_PROJECT, config.participantDelegatesConfig.transmitterTimeout, config.participantDelegatesConfig.timeUnit) {
			case failure: Transmitter.NotDelivered =>
				discardedChannel.close()
			case Transmitter.Delivered =>
				discardedChannel.shutdownOutput()
				sequencer.scheduleSequentially(sequencer.newDelaySchedule(config.participantDelegatesConfig.closeDelay)) { () => discardedChannel.close() }
		}
	}

	private def release(): Unit = {
		notifyListenersThat(IAmGoingToCloseAllChannels())
		serverChannel.close()
		for case communicableDelegate: CommunicableDelegate <- delegateByAddress.valuesIterator do {
			communicableDelegate.release()
		}
	}
}
