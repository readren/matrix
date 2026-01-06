package readren.nexus
package cluster.service

import cluster.channel.{Receiver, SequentialTransmitter, Transmitter}
import cluster.serialization.ProtocolVersion
import cluster.service.ChannelOrigin.{ACCEPTED, INITIATED}
import cluster.service.ParticipantService.*
import cluster.service.Protocol.*
import cluster.service.Protocol.CommunicationStatus.HANDSHOOK
import cluster.service.Protocol.IncommunicabilityReason.IS_CONNECTING_AS_CLIENT
import cluster.service.behavior.*

import readren.common.Maybe
import readren.sequencer.{AbstractDoer, MilliDuration, SchedulingExtension}

import java.net.SocketOption
import java.nio.channels.{AsynchronousServerSocketChannel, AsynchronousSocketChannel, CompletionHandler}
import java.util.concurrent.TimeUnit
import scala.annotation.tailrec
import scala.annotation.unchecked.uncheckedVariance
import scala.collection.MapView
import scala.language.implicitConversions
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object ParticipantService {
	
	type TaskSequencer = AbstractDoer & SchedulingExtension 

	trait Clock {
		def getTime: Instant
	}

	trait EventListener {
		def handle(event: ParticipantServiceEvent): Unit
	}

	trait IsolationDecider {
		def shouldIBeIsolated(colleaguesStatuses: Iterable[CommunicationStatus]): Boolean
	}

	object MajorityIsolationDecider extends IsolationDecider {
		override def shouldIBeIsolated(colleaguesStatuses: Iterable[CommunicationStatus]): Boolean = {
			colleaguesStatuses.count(_ eq HANDSHOOK) <= (colleaguesStatuses.size + 1) / 2
		}
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
		val isolationDecider: IsolationDecider = MajorityIsolationDecider,
		val acceptedConnectionsFilter: ContactAddressFilter = (ca: ContactAddress) => true,
		val connectionRetryMinDelay: MilliDuration = 1000,
		val connectionRetryMaxDelay: MilliDuration = 60_000,
		val connectionRetryMaxAttempts: Int = 8,
		val joinCheckDelay: MilliDuration = 2_000,
		val socketOptions: Iterable[(CovariantSocketOption[Any], Any)] = Iterable.empty
	)

	/**
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
		val requestRetryDelay: MilliDuration = 2_000,
		val closeDelay: MilliDuration = 200,
		val heartbeatPeriod: MilliDuration = 10_000,
		val heartbeatMargin: MilliDuration = 12_000,
	)

	def start(sequencer: TaskSequencer, clock: Clock, serviceConfig: Config, startingListeners: Iterable[EventListener] = None): ParticipantService = {

		val serverChannel = AsynchronousServerSocketChannel.open()
		for option <- serviceConfig.socketOptions do {
			serverChannel.setOption(option._1.value, option._2)
		}
		serverChannel.bind(serviceConfig.myAddress)

		val eventListeners = new java.util.WeakHashMap[EventListener, None.type]()
		for eventListener <- startingListeners do eventListeners.put(eventListener, None)

		val service = new ParticipantService(sequencer, clock, serviceConfig, serverChannel, eventListeners)
		// start the listening server
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
 * A cluster-participant-service that manages the propagation of its own existence and the awareness of other [[ParticipantService]] instances
 * to support intercommunication between their users across different JVMs, which maybe (and usually are) on different host machines.
 *
 * For brevity, instances of this class are referred to as "participants."
 * Depending on its membership status (from its own viewpoint), a participant behaves either, as a "member", or as an "aspirant" to become a member of the cluster.
 *
 * The [[ParticipantService]] class delegates the knowledge about, and communication with, other participants, to implementations of the [[ParticipantDelegate]] trait: it creates one delegate per participant it is aware of (excluding itself).
 */
class ParticipantService private(val sequencer: TaskSequencer, val clock: Clock, val config: ParticipantService.Config, serverChannel: AsynchronousServerSocketChannel, eventListeners: java.util.WeakHashMap[EventListener, None.type]) { thisParticipantService =>

	export config.myAddress

	val myContactCard: ContactCard = (config.myAddress, config.versionsISupport)

	private val creationInstant: Instant = clock.getTime

	private var participantDelegateByAddress: Map[ContactAddress, ParticipantDelegate] = Map.empty

	private var membershipScopedBehavior: MembershipScopedBehavior = new AspirantBehavior(this)

	private var _isShutDown: Boolean = false
	inline def isShutDown: Boolean = _isShutDown

	inline def myCreationInstant: Instant = creationInstant

	inline def myMembershipStatus: MembershipStatus = membershipScopedBehavior.membershipStatus

	inline def getMembershipScopedBehavior: MembershipScopedBehavior = membershipScopedBehavior

	inline def delegateByAddress: Map[ContactAddress, ParticipantDelegate] = participantDelegateByAddress

	def handshookDelegateByAddress: MapView[ContactAddress, CommunicableDelegate] =
		delegateByAddress.view.filter(_._2.communicationStatus eq HANDSHOOK).asInstanceOf[MapView[ContactAddress, CommunicableDelegate]]
		
	def colleagueDelegateByAddress: MapView[ContactAddress, ParticipantDelegate] =
		delegateByAddress.view.filter(_._2.getPeerMembershipStatusAccordingToMe.contains(myMembershipStatus))	

	/** Creates and adds (to the set of known participants delegates) a new [[CommunicableDelegate]] to manage the communication with the participant at the specified [[ContactAddress]]. */
	private[service] def addANewCommunicableDelegate(peerContactAddress: ContactAddress, channel: AsynchronousSocketChannel, receiverFromPeer: Receiver, channelId: ChannelId): CommunicableDelegate = {
		assert(sequencer.isInSequence)
		assert(peerContactAddress != myAddress)
		val newParticipant = new CommunicableDelegate(thisParticipantService, peerContactAddress, channel, receiverFromPeer, channelId)
		participantDelegateByAddress += peerContactAddress -> newParticipant
		newParticipant
	}


	/** Creates and adds (to the set of known participants delegates) a new [[IncommunicableDelegate]] to remember if this service is currently connecting to the participant at the specified [[ContactAddress]] or has desisted to communicate with it. */
	private[service] def addANewIncommunicableDelegate(participantAddress: ContactAddress, reason: IncommunicabilityReason): IncommunicableDelegate = {
		assert(sequencer.isInSequence)
		assert(participantAddress != myAddress)
		val newParticipant = new IncommunicableDelegate(thisParticipantService, participantAddress, reason)
		participantDelegateByAddress += participantAddress -> newParticipant
		newParticipant
	}

	private[service] inline def removeDelegate(delegate: ParticipantDelegate, notifyListeners: Boolean): Boolean = {
		assert(sequencer.isInSequence)
		delegateByAddress.getOrElse(delegate.peerContactAddress, null) match {
			case referencedDelegate if referencedDelegate eq delegate =>
				participantDelegateByAddress -= delegate.peerContactAddress
				if notifyListeners then notifyListenersThat(ParticipantHasGone(delegate.peerContactAddress))
				true
			case _ => false
		}
	}

	/** Must be called within the [[sequencer]].
	 * @return A set with as many elements as clusters among the know participants, with each element equal to the [[ClusterId]]. */
	def findCluster: Set[ClusterId] = {
		assert(sequencer.isInSequence)
		val iterator = delegateByAddress.valuesIterator

		@tailrec
		def loop(alreadyFound: Set[ClusterId]): Set[ClusterId] = {
			if iterator.hasNext then {
				iterator.next().getPeerMembershipStatusAccordingToMe.fold(loop(alreadyFound)) {
					case Functional(idOfTheClusterTheMemberBelongsTo) =>
						loop(alreadyFound + idOfTheClusterTheMemberBelongsTo)
					case _ =>
						loop(alreadyFound)
				}
			} else alreadyFound
		}

		loop(Set.empty)
	}

	/**
	 * Gets the address of all the participants known by this [[ParticipantService]]. */
	def getKnownParticipantsAddresses: Set[ContactAddress] = {
		participantDelegateByAddress.keySet
	}

	def getStableParticipantsMembershipStatus: MapView[ContactAddress, MembershipStatus] = {
		delegateByAddress.view.filter(_._2.isStable).mapValues(_.getPeerMembershipStatusAccordingToMe.get)
	}

	def getHandshookParticipantsMembershipStatus: MapView[ContactAddress, MembershipStatus] = {
		delegateByAddress.view.filter(_._2.communicationStatus eq HANDSHOOK).mapValues(_.getPeerMembershipStatusAccordingToMe.get)
	}

	def getStableParticipantsInfo: MapView[ContactAddress, ParticipantInfo] = {
		assert(sequencer.isInSequence)
		delegateByAddress.view.filter(_._2.info.isDefined).mapValues(_.info.get)
	}

	def createADelegateForEachParticipantIDoNotKnowIn(participantsKnownByPeer: Set[ContactAddress]): Unit = {
		assert(sequencer.isInSequence)
		// Create a delegate for each participant that I did not know.
		for participantAddress <- participantsKnownByPeer do {
			if participantAddress != myAddress && !delegateByAddress.contains(participantAddress) then {
				addANewConnectingDelegateAndStartAConnectionToThenAConversationWithParticipant(participantAddress)
			}
		}
	}

	/** @return the newest [[ProtocolVersion]] of the specified set that this [[ParticipantService]] supports. */
	private[service] def determineAgreedVersion(versionsSupportedByPeer: Set[ProtocolVersion]): Option[ProtocolVersion] = {
		val versionsSupportedByBoth = config.versionsISupport.intersect(versionsSupportedByPeer)
		versionsSupportedByBoth.minOption(using ProtocolVersion.newerFirstOrdering)
	}

	private[service] def solveClusterExistenceConflictWith(communicableDelegate: CommunicableDelegate): Boolean = ???

	/** Notifies listeners and other participants that the specified participant has become incommunicable. */
	private[service] def notifyListenersAndOtherParticipantsThatAParticipantBecomeIncommunicable(participantContactAddress: ContactAddress, reason: IncommunicabilityReason, cause: Any): Unit = {
		assert(sequencer.isInSequence)
		notifyListenersThat(CommunicationLostWith(participantContactAddress, reason, cause))
		for case communicableDelegate: CommunicableDelegate <- delegateByAddress.valuesIterator do
			communicableDelegate.notifyPeerThatILostCommunicationWith(participantContactAddress)
	}

	/** Notifies listeners and other participants that a conversation with the specified participant has started. */
	private[service] def notifyListenersAndOtherParticipantsThatAConversationStartedWith(participantAddress: ContactAddress, membershipStatus: MembershipStatus, isARestartAfterReconnection: Boolean): Unit = {
		assert(sequencer.isInSequence)
		notifyListenersThat(AConversationStartedWith(participantAddress, membershipStatus, isARestartAfterReconnection))
		for case (address, communicableDelegate: CommunicableDelegate) <- delegateByAddress.iterator do
			if address != participantAddress then communicableDelegate.notifyPeerThatAConversationStartedWith(participantAddress, isARestartAfterReconnection)
	}

	/**
	 * Manages incoming client connections to the server component of this [[ParticipantService]].
	 *
	 * @param serverChannel The [[AsynchronousServerSocketChannel]] used to listen for and accept client connections.
	 */
	private def acceptClientConnections(serverChannel: AsynchronousServerSocketChannel): Unit = {
		try serverChannel.accept(null, new CompletionHandler[AsynchronousSocketChannel, Null]() {
			override def completed(channel: AsynchronousSocketChannel, attachment: Null): Unit = {
				val oClientAddress = try Maybe(channel.getRemoteAddress) catch {
					case NonFatal(_) => Maybe.empty
				}
				val channelId = ChannelId(ACCEPTED, oClientAddress)
				scribe.debug(s"$myAddress: Accepting a connection from `${channelId.clientAddress}` with channel $channelId.")
				// Note that this method body is executed sequentially by nature of the NIO2. Nevertheless, the `executeSequentially` is necessary because some of the member variables accessed here need to be accessed by procedures that are started from other handlers that are concurrent.

				new ParticipantDelegateEgg(thisParticipantService, channel, oClientAddress, channelId).incubate()
				acceptClientConnections(serverChannel)
			}

			override def failed(exc: Throwable, attachment: Null): Unit = {
				scribe.error(s"$myAddress: Error while trying to accept a connection:", exc)
				acceptClientConnections(serverChannel)
			}
		})
		catch {
			case NonFatal(e) =>
				scribe.error(s"$myAddress: Closing the cluster service because it is unable to accept connections from peers. Cause:", e)
				release()
		}
	}

	private def startConnectionToSeeds(seeds: Iterable[ContactAddress]): Unit = {
		sequencer.execute {
			for seed <- seeds do if config.myAddress != seed then {
				if !delegateByAddress.contains(seed) then addANewConnectingDelegateAndStartAConnectionToThenAConversationWithParticipant(seed)
			}
		}
	}

	/** @return a brand-new [[IncommunicableDelegate]] in connecting state. */
	private[service] def addANewConnectingDelegateAndStartAConnectionToThenAConversationWithParticipant(participantContactAddress: ContactAddress): IncommunicableDelegate = {
		val connectingDelegate = addANewIncommunicableDelegate(participantContactAddress, IS_CONNECTING_AS_CLIENT)
		membershipScopedBehavior.onPeerAdded(connectingDelegate)
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
				val oChannelLocalAddress = try Maybe(channel.getLocalAddress) catch {
					case NonFatal(_) => Maybe.empty
				}
				val channelId = ChannelId(INITIATED, oChannelLocalAddress)
				scribe.trace(s"$myAddress: I have successfully initiated a connection to `$peerContactAddress` with channel $channelId.")
				sequencer.execute {
					val currentDelegate = delegateByAddress.getOrElse(peerContactAddress, null)
					// if the `relievedConnectingDelegate` was not removed in the middle, then no conflicting connection happened on the while. Therefore, I can replace the relieved delegate with a communicable one.
					if relievedConnectingDelegate eq currentDelegate then {
						val oPeerContactAddress = Maybe(peerContactAddress)
						val receiver = buildReceiverFor(channel, () => oPeerContactAddress, channelId)
						relievedConnectingDelegate.replaceMyselfWithACommunicableDelegate(channel, receiver, channelId)
							.get.startConversationAsClient(isReconnection)
					}
					// else (if the `relievedConnectingDelegate` was removed in the middle), close the communication channel gracefully.
					else {
						if relievedConnectingDelegate.isConnectingAsClient && currentDelegate.isInstanceOf[CommunicableDelegate] && (whichChannelShouldIKeepWhenMutualConnectionWith(peerContactAddress) eq ACCEPTED) then {
							scribe.info(s"$myAddress: Closing the brand-new connection to $peerContactAddress that I had initiated recently, because I received a Hello message through another connection initiated by the same peer which has priority because my address is greater than the peer's")
						} else {
							scribe.warn(s"$myAddress: Closing the brand-new connection to $peerContactAddress that I had initiated recently, because the situation changed.")
						}
						closeDiscardedChannelGracefully(channel, buildTransmitterFor(channel, Maybe(peerContactAddress), channelId), channelId)
					}
				}
			case Failure(exc) =>
				scribe.error(s"$myAddress: The connection that I started to `$peerContactAddress` has been aborted after many failed tries.", exc)
				sequencer.execute(relievedConnectingDelegate.onConnectionAborted(exc))
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
					sequencer.schedule(schedule) { _ =>
						connect(attemptNumber + 1)
					}
				} else onComplete(Failure(exc))
			}
		}
		handler.connect(1)
	}

	/** Criteria that determines which connection channel to keep (and which to discard) when two participants initiate a connection to each other simultaneously.
	 * The kept connection is the one in which the participant with the highest address is the server. The other connection should be gracefully closed.
	 * Form the participant perspective, it should keep the connection whose channel's origin is [[ACCEPTED]] if it has a higher address than the peer, and [[INITIATED]] otherwise.
	 * @param peerAddress the [[ContactAddress]] of the participant I am mutually connected with.
	 * @return the [[ChannelOrigin]] that the kept channel should have.
	 */
	def whichChannelShouldIKeepWhenMutualConnectionWith(peerAddress: ContactAddress): ChannelOrigin = {
		if ContactCard.compareContactAddresses(peerAddress, config.myAddress) < 0 then ACCEPTED else INITIATED
	}


	/** Switches the behavior from [[AspirantBehavior]] to [[FunctionalBehavior]]. */
	private[service] def switchToMember(currentBehavior: AspirantBehavior, clusterId: ClusterId): FunctionalBehavior = {
		val newBehavior = new FunctionalBehavior(thisParticipantService, RingSerial.create(), clusterId)
		membershipScopedBehavior = newBehavior
		val currentInstant = System.currentTimeMillis()
		for (_, delegate) <- handshookDelegateByAddress do {
			delegate.transmitToPeerOrRestartChannel(ClusterStateChanged(newBehavior.currentStateSerial, currentInstant, newBehavior.membershipStatus, getStableParticipantsInfo.toMap))
		}
		notifyListenersThat(IJoinedTheCluster(delegateByAddress))
		newBehavior
	}

	private[service] def switchToIsolated(currentBehavior: FunctionalBehavior): IsolatedBehavior = {
		val newBehavior = IsolatedBehavior(thisParticipantService, currentBehavior.currentStateSerial.nextSerial, currentBehavior.myClusterId)
		membershipScopedBehavior = newBehavior
		getMembershipScopedBehavior
		for (_, delegate) <- handshookDelegateByAddress do {
			delegate.transmitToPeerOrRestartChannel(WeAreIsolated(currentBehavior.myCurrentViewpoint))
		}
		notifyListenersThat(IBecomeIsolated(newBehavior.IsolatedOperatorImpl))
		newBehavior
	}

	private[service] def switchToConflicted(currentBehavior: MemberBehavior, otherClustersIds: Set[ClusterId], why: String): ConflictedBehavior = {
		val newBehavior = ConflictedBehavior(thisParticipantService, currentBehavior.currentStateSerial.nextSerial, currentBehavior.myClusterId, otherClustersIds, currentBehavior.isIsolated, why)
		membershipScopedBehavior = newBehavior

		for (_, delegate) <- handshookDelegateByAddress do {
			delegate.transmitToPeerOrRestartChannel(WeHaveToResolveClustersConflict(newBehavior.myCurrentViewpoint))
		}
		notifyListenersThat(IBecomeConflicted(newBehavior.ConflictedOperatorImpl, newBehavior.isIsolated, why))
		newBehavior
	}

	def subscribe(listener: EventListener): Unit = {
		assert(sequencer.isInSequence)
		eventListeners.put(listener, None)
	}

	def unsubscribe(listener: EventListener): Boolean = {
		assert(sequencer.isInSequence)
		eventListeners.remove(listener) eq None
	}

	private[service] def notifyListenersThat(event: ParticipantServiceEvent): Unit = {
		eventListeners.forEach { (listener, _) => listener.handle(event) }
	}

	private[service] def closeDiscardedChannelGracefully(discardedChannel: AsynchronousSocketChannel, transmitter: SequentialTransmitter[Protocol], channelId: ChannelId): Unit = {
		scribe.trace(s"$myAddress: About to gracefully close the connection to ${transmitter.context.showPeerAddress} (channel $channelId) in four steps: 1) shutdown input, 2) send a $ChannelDiscarded message, 3) shutdown output, 4) completely close the channel after a while.")
		transmitter.transmit(ChannelDiscarded, ProtocolVersion.OF_THIS_PROJECT, false, config.participantDelegatesConfig.transmitterTimeout, config.participantDelegatesConfig.timeUnit) {
			case failure: Transmitter.NotDelivered =>
				scribe.trace(s"$myAddress: The transmission of the $ChannelDiscarded message to ${transmitter.context.showPeerAddress} failed with `$failure`. Closing the channel immediately.")
				discardedChannel.close()
			case Transmitter.Delivered =>
				discardedChannel.shutdownOutput()
				sequencer.schedule(sequencer.newDelaySchedule(config.participantDelegatesConfig.closeDelay)) { _ => discardedChannel.close() }
		}
	}

	private def release(): Unit = {
		sequencer.execute {
			_isShutDown = true
			notifyListenersThat(IAmGoingToCloseAllChannels())
			serverChannel.close()
			for case communicableDelegate: CommunicableDelegate <- delegateByAddress.valuesIterator do {
				communicableDelegate.completeChannelClosing()
			}
			participantDelegateByAddress = Map.empty
		}
	}

	private[service] def buildTransmitterFor(channel: AsynchronousSocketChannel, oPeerContactAddress: Maybe[ContactAddress], aChannelId: ChannelId): SequentialTransmitter[Protocol] = {
		object context extends Transmitter.Context {
			override val myAddress: ContactAddress = thisParticipantService.myAddress
			override val oPeerAddress: Maybe[ContactAddress] = oPeerContactAddress
			override val channelId: ChannelId = aChannelId
		}
		new SequentialTransmitter[Protocol](channel, context, config.participantDelegatesConfig.frameCapacity)
	}

	private[service] def buildReceiverFor(channel: AsynchronousSocketChannel, peerContactAddressGetter: () => Maybe[ContactAddress], aChannelId: ChannelId): Receiver = {
		object context extends Receiver.Context {
			override val myAddress: ContactAddress = thisParticipantService.myAddress

			override def oPeerAddress: Maybe[ContactAddress] = peerContactAddressGetter()

			override val channelId: ChannelId = aChannelId
		}
		new Receiver(channel, context, config.participantDelegatesConfig.frameCapacity)
	}
}
