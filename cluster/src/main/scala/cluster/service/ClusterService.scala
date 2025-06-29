package readren.matrix
package cluster.service

import cluster.channel.Transmitter
import cluster.misc.TaskSequencer
import cluster.serialization.ProtocolVersion
import cluster.service.ClusterService.*
import cluster.service.ClusterService.ChannelOrigin.{CLIENT_INITIATED, SERVER_ACCEPTED}
import cluster.service.Protocol.*
import cluster.service.Protocol.CommunicationStatus.HANDSHOOK
import cluster.service.Protocol.IncommunicabilityReason.IS_CONNECTING_AS_CLIENT

import readren.taskflow.Maybe
import readren.taskflow.SchedulingExtension.MilliDuration

import java.net.SocketOption
import java.nio.channels.{AsynchronousServerSocketChannel, AsynchronousSocketChannel, CompletionHandler}
import java.util.concurrent.TimeUnit
import scala.annotation.tailrec
import scala.annotation.unchecked.uncheckedVariance
import scala.collection.MapView
import scala.language.implicitConversions
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}
import readren.matrix.cluster.channel.Receiver
import java.net.SocketAddress

object ClusterService {

	trait Clock {
		def getTime: Instant
	}

	trait EventListener {
		def handle(event: ClusterServiceEvent): Unit
	}

	class CovariantSocketOption[+T](val value: SocketOption[T @uncheckedVariance])

	type SocketOptionValue[+T] = (CovariantSocketOption[T], T)

	given [T] =>Conversion[SocketOption[T], CovariantSocketOption[T]] {
		def apply(so: SocketOption[T]): CovariantSocketOption[T] = so
	}
	given [T] =>Conversion[(SocketOption[T], T), SocketOptionValue[T]] {
		def apply(so: (SocketOption[T], T)): SocketOptionValue[T] = (CovariantSocketOption[T](so._1), so._2)
	}

	trait ContactAddressFilter {
		def test(ca: ContactAddress): Boolean
	}

	class Config(
		val myAddress: ContactAddress,
		val seeds: Iterable[ContactAddress],
		val versionsISupport: Set[ProtocolVersion] = Set(ProtocolVersion.OF_THIS_PROJECT),
		val participantDelegatesConfig: DelegateConfig = new DelegateConfig(),
		val acceptedConnectionsFilter: ContactAddressFilter = (ca: ContactAddress) => true,
		val connectionRetryMinDelay: MilliDuration = 1000,
		val connectionRetryMaxDelay: MilliDuration = 60_000,
		val connectionRetryMaxAttempts: Int = 8,
		val joinCheckDelay: MilliDuration = 2_000,
		val socketOptions: Iterable[(CovariantSocketOption[Any], Any)] = Iterable.empty
	)

	/** @param requestsAttempts how many times a [[CommunicableDelegate.askPeer]] should be called in total when the previous try resulted in a response timeout.
	 * Since the [[AsynchronousSocketChannel]] uses TCP, it ensures loss-less in-order delivery. Therefore, the purpose to retry a request after a no-response timeout is to reveal any silent communication problem or to detect silent middlebox interference.
	 * If the peer is alive but not responding (e.g., deadlock, overload), retries would not help. In conclusion, no more than a single retry is needed. 
	 */
	class DelegateConfig(
		val behindTransmissionEnabled: Boolean = true,
		val receiverTimeout: Long = 60_000,
		val transmitterTimeout: Long = 500,
		val timeUnit: TimeUnit = TimeUnit.MILLISECONDS,
		val frameCapacity: Integer = 8192,
		val responseTimeout: MilliDuration = 1_000,
		val requestRetryStartingDelay: MilliDuration = 2_000,
		val requestsAttempts: Int = 2,
		val closeDelay: MilliDuration = 200,
		val heartbeatPeriod: MilliDuration = 10_000,
		val heartbeatMargin: MilliDuration = 12_000,
	)

	def start(sequencer: TaskSequencer, clock: Clock, serviceConfig: Config, startingListeners: Iterable[EventListener] = None): ClusterService = {

		val serverChannel = AsynchronousServerSocketChannel.open()
		for option <- serviceConfig.socketOptions do {
			serverChannel.setOption(option._1.value, option._2)
		}
		serverChannel.bind(serviceConfig.myAddress)

		val eventListeners = new java.util.WeakHashMap[EventListener, None.type]()
		for eventListener <- startingListeners do eventListeners.put(eventListener, None)

		val service = new ClusterService(sequencer, clock, serviceConfig, serverChannel, eventListeners)
		// start the listening server
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
		/** Marks [[CommunicableDelegate]]s instances that are at the client side of the channel (were initiated by the [[ClusterService]] that owns the delegate). */
		case CLIENT_INITIATED
		/** Marks [[CommunicableDelegate]]s that are at the server side of the channel (were accepted by the [[ClusterService]] that owns the delegate). */
		case SERVER_ACCEPTED
	}
}

/**
 * A service that manages the propagation of its own existence and the awareness of other [[ClusterService]] instances
 * to support intercommunication between their users across different JVMs, which maybe (and usually are) on different host machines.
 *
 * For brevity, instances of this class are referred to as "participants."
 * Depending on its membership status (from its own viewpoint), a participant behaves either as a "member" or as an "aspirant" to become a member of the cluster.
 *
 * The [[ClusterService]] class delegates the knowledge about, and communication with, other participants, to implementations of the [[ParticipantDelegate]] trait: it creates one delegate per participant it is aware of (excluding itself).
 */
class ClusterService private(val sequencer: TaskSequencer, val clock: Clock, val config: ClusterService.Config, serverChannel: AsynchronousServerSocketChannel, eventListeners: java.util.WeakHashMap[EventListener, None.type]) { thisClusterService =>

	export config.myAddress

	val myContactCard: ContactCard = (config.myAddress, config.versionsISupport)

	private val creationInstant: Instant = clock.getTime

	private var participantDelegateByAddress: Map[ContactAddress, ParticipantDelegate] = Map.empty

	private[service] var membershipScopedBehavior: MembershipScopedBehavior = new AspirantBehavior(this)

	inline def myCreationInstant: Instant = creationInstant

	inline def myMembershipStatus: MembershipStatus = membershipScopedBehavior.membershipStatus

	inline def getMembershipScopedBehavior: MembershipScopedBehavior = membershipScopedBehavior

	inline def delegateByAddress: Map[ContactAddress, ParticipantDelegate] = participantDelegateByAddress

	def handshookDelegateByAddress: MapView[ContactAddress, CommunicableDelegate] =
		delegateByAddress.view.filter(_._2.communicationStatus eq HANDSHOOK).asInstanceOf[MapView[ContactAddress, CommunicableDelegate]]

	private[service] def addANewCommunicableDelegate(peerContactAddress: ContactAddress, channel: AsynchronousSocketChannel, receiverFromPeer: Receiver, oPeerRemoteAddress: Maybe[SocketAddress], channelOrigin: ChannelOrigin): CommunicableDelegate = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		assert(peerContactAddress != myAddress)
		val newParticipant = new CommunicableDelegate(thisClusterService, peerContactAddress, channel, receiverFromPeer, oPeerRemoteAddress, channelOrigin)
		participantDelegateByAddress += peerContactAddress -> newParticipant
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
		delegateByAddress.getOrElse(delegate.peerContactAddress, null) match {
			case referencedDelegate if referencedDelegate eq delegate =>
				participantDelegateByAddress -= delegate.peerContactAddress
				if notifyListeners then notifyListenersThat(ParticipantHasGone(delegate.peerContactAddress))
				true
			case _ => false
		}
	}

	/** Must be called within the [[sequencer]].
	 * @return `0` if no cluster exists among the know participants; `1` if only one cluster exists; and `2` if two or more exist. */
	def clustersExistenceArity: Int = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		val iterator = delegateByAddress.valuesIterator

		@tailrec
		def loop(creationInstant: Instant): Int = {
			if iterator.hasNext then {
				iterator.next().getPeerMembershipStatusAccordingToMe.fold(loop(creationInstant)) {
					case Member(creationInstantOfTheClusterTheMemberBelongsTo) =>
						if creationInstant == UNSPECIFIED_INSTANT then loop(creationInstantOfTheClusterTheMemberBelongsTo)
						else if creationInstantOfTheClusterTheMemberBelongsTo == creationInstant then loop(creationInstant)
						else 2
					case _ =>
						loop(creationInstant)
				}
			} else if creationInstant == UNSPECIFIED_INSTANT then 0 else 1
		}

		loop(UNSPECIFIED_INSTANT)
	}

	/**
	 * Gets the address of all the participants known by this [[ClusterService]]. */
	def getKnownParticipantsAddresses: Set[ContactAddress] = {
		participantDelegateByAddress.keySet
	}

	def getStableParticipantsMembershipStatus: MapView[ContactAddress, MembershipStatus] = {
		delegateByAddress.view.filter(_._2.isStable).mapValues(_.getPeerMembershipStatusAccordingToMe.get)
	}

	def getStableParticipantsInfo: MapView[ContactAddress, ParticipantInfo] = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		delegateByAddress.view.filter(_._2.info.isDefined).mapValues(_.info.get)
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

	/** @return the newest [[ProtocolVersion]] of the specified set that this [[ClusterService]] supports. */
	private[service] def determineAgreedVersion(versionsSupportedByPeer: Set[ProtocolVersion]): Option[ProtocolVersion] = {
		val versionsSupportedByBoth = config.versionsISupport.intersect(versionsSupportedByPeer)
		versionsSupportedByBoth.minOption(using ProtocolVersion.newerFirstOrdering)
	}

	private[service] def solveClusterExistenceConflictWith(communicableDelegate: CommunicableDelegate): Boolean = ???

	/** Notifies listeners and other participants that the specified participant has become incommunicable. */
	private[service] def notifyListenersAndOtherParticipantsThatAParticipantBecomeIncommunicable(participantContactAddress: ContactAddress, reason: IncommunicabilityReason, cause: Any): Unit = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		notifyListenersThat(DelegateBecomeIncommunicable(participantContactAddress, reason, cause))
		for case communicableDelegate: CommunicableDelegate <- delegateByAddress.valuesIterator do
			communicableDelegate.notifyPeerThatILostCommunicationWith(participantContactAddress)
	}

	/** Notifies listeners and other participants that a conversation with the specified participant has started. */
	private[service] def notifyListenersAndOtherParticipantsThatAConversationStartedWith(participantAddress: ContactAddress, isARestartAfterReconnection: Boolean): Unit = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		notifyListenersThat(DelegateStartedConversationWith(participantAddress, isARestartAfterReconnection))
		for case (address, communicableDelegate: CommunicableDelegate) <- delegateByAddress.iterator do
			if address != participantAddress then communicableDelegate.notifyPeerThatAConversationStartedWith(participantAddress, isARestartAfterReconnection)
	}

	/**
	 * Manages incoming client connections to the server component of this [[ClusterService]].
	 *
	 * @param serverChannel The [[AsynchronousServerSocketChannel]] used to listen for and accept client connections.
	 */
	private def acceptClientConnections(serverChannel: AsynchronousServerSocketChannel): Unit = {
		try serverChannel.accept(null, new CompletionHandler[AsynchronousSocketChannel, Null]() {
			override def completed(channel: AsynchronousSocketChannel, attachment: Null): Unit = {
				val oClientParticipantEphemeralAddress = Try(channel.getRemoteAddress).fold(_ => Maybe.empty, Maybe.some)
				scribe.debug(s"The server at `$myAddress` is accepting a connection from `${oClientParticipantEphemeralAddress.fold("unknown")(_.toString)}`.")
				// Note that this method body is executed sequentially by nature of the NIO2. Nevertheless, the `executeSequentially` is necessary because some of the member variables accessed here need to be accessed by procedures that are started from other handlers that are concurrent.

				new ParticipantDelegateEgg(thisClusterService, channel).incubate()
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

	private def startConnectionToSeeds(seeds: Iterable[ContactAddress]): Unit = {
		sequencer.executeSequentially {
			for seed <- seeds do if config.myAddress != seed then {
				if !delegateByAddress.contains(seed) then addANewConnectingDelegateAndStartAConnectionToThenAConversationWithParticipant(seed)
			}
		}
	}

	/** @return a brand-new [[IncommunicableDelegate]] in connecting state. */
	private[service] def addANewConnectingDelegateAndStartAConnectionToThenAConversationWithParticipant(participantContactAddress: ContactAddress): IncommunicableDelegate = {
		val connectingDelegate = addANewIncommunicableDelegate(participantContactAddress, IS_CONNECTING_AS_CLIENT)
		membershipScopedBehavior.onDelegatedAdded(connectingDelegate)
		notifyListenersThat(IStartedAConnectionToANewParticipant(participantContactAddress))
		connectToAndThenStartConversationWithParticipant(connectingDelegate, false)
		connectingDelegate
	}

	/** @param relievedConnectingDelegate the [[IncommunicableDelegate]] instance currently associated with the participant to connect to (which should have its "isConnecting" flag set); and will be relieved with a [[CommunicableDelegate]] if the connection succeeds. */
	private[service] def connectToAndThenStartConversationWithParticipant(relievedConnectingDelegate: IncommunicableDelegate, isReconnection: Boolean): Unit = {
		assert(relievedConnectingDelegate.isConnectingAsClient)
		val peerContactAddress = relievedConnectingDelegate.peerContactAddress
		assert(delegateByAddress.contains(peerContactAddress))
		connectTo(peerContactAddress) {
			case Success(channel) =>
				scribe.debug(s"The participant at `$myAddress` has successfully connected as client to the participant at `$peerContactAddress`.")
				sequencer.executeSequentially {
					val currentDelegate = delegateByAddress.getOrElse(peerContactAddress, null)
					// if the `relievedConnectingDelegate` was not removed in the middle, asociate a communicable delegate to the peer (replacing the connecting one).
					if relievedConnectingDelegate eq currentDelegate then {
						relievedConnectingDelegate.replaceMyselfWithACommunicableDelegate(channel, buildReceiverFor(channel), Maybe.some(peerContactAddress), CLIENT_INITIATED)
							.get.startConversationAsClient(isReconnection)
					}
					// else (if the `relievedConnectingDelegate` was removed in the middle), close the communication channel gracefully.
					else {
						if relievedConnectingDelegate.isConnectingAsClient && currentDelegate.isInstanceOf[CommunicableDelegate] && (whichChannelShouldIKeepWhenMutualConnectionWith(peerContactAddress) eq SERVER_ACCEPTED) then {
							scribe.info(s"Closing the brand-new connection to $peerContactAddress (with me as a client) because a connection to him with me as server was completed in the middle and my address is greater than the peer's")
						} else {
							scribe.warn(s"Closing a brand-new connection to $peerContactAddress because the associated delegate was removed.")
						}
						closeDiscardedChannelGracefully(channel, CLIENT_INITIATED)
					}
				}
			case Failure(exc) =>
				scribe.error(s"The connection that I ($myAddress) started to the participant at `$peerContactAddress` has been aborted after many failed tries.", exc)
				sequencer.executeSequentially(relievedConnectingDelegate.onConnectionAborted(exc))
		}
	}

	private def connectTo(contactAddress: ContactAddress)(onComplete: Try[AsynchronousSocketChannel] => Unit): Unit = {
		object handler extends CompletionHandler[Void, Integer] { thisHandler =>
			@volatile private var channel: AsynchronousSocketChannel = null

			def connect(attemptNumber: Integer): Unit = {
				assert(channel eq null)
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
				if attemptNumber <= config.connectionRetryMaxAttempts then {
					if channel.isOpen then channel.close() // TODO Is this line necessary?
					channel = null
					val retryDelay = config.connectionRetryMinDelay * attemptNumber * attemptNumber
					val schedule: sequencer.Schedule = sequencer.newDelaySchedule(math.max(config.connectionRetryMaxDelay, retryDelay))
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


	def subscribe(listener: EventListener): Unit = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		eventListeners.put(listener, None)
	}

	def unsubscribe(listener: EventListener): Boolean = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		eventListeners.remove(listener) eq None
	}

	private[service] def notifyListenersThat(event: ClusterServiceEvent): Unit = {
		eventListeners.forEach { (listener, _) => listener.handle(event) }
	}

	private[service] def closeDiscardedChannelGracefully(discardedChannel: AsynchronousSocketChannel, channelOrigin: ChannelOrigin): Unit = {
		val transmitter = new Transmitter(discardedChannel)
		discardedChannel.shutdownInput()
		transmitter.transmit[Protocol](ChannelDiscarded(channelOrigin eq CLIENT_INITIATED), ProtocolVersion.OF_THIS_PROJECT, false, config.participantDelegatesConfig.transmitterTimeout, config.participantDelegatesConfig.timeUnit) {
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
			communicableDelegate.startPeerChannelClosing()
		}
	}
	
	private[service] def buildReceiverFor(channel: AsynchronousSocketChannel): Receiver =
		new Receiver(channel, config.participantDelegatesConfig.frameCapacity)
}
