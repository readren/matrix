package readren.matrix
package cluster.service

import cluster.channel.Receiver.ChannelClosedByPeer
import cluster.channel.Transmitter.{Delivered, NotDelivered, Report}
import cluster.channel.{Receiver, SequentialTransmitter, Transmitter}
import cluster.serialization.ProtocolVersion
import cluster.service.ClusterService.{ChannelOrigin, DelegateConfig}
import cluster.service.Protocol.*
import cluster.service.Protocol.CommunicationStatus.{CONNECTED, HANDSHOOK}
import cluster.service.Protocol.IncommunicabilityReason.IS_CONNECTING_AS_CLIENT

import readren.taskflow.Maybe
import readren.taskflow.SchedulingExtension.MilliDuration
import scribe.LogFeature

import java.net.SocketAddress
import java.nio.channels.AsynchronousSocketChannel
import scala.collection.mutable
import scala.util.control.NonFatal

/** A [[ParticipantDelegate]] that is currently able to communicable with the participant. */
class CommunicableDelegate(
	override val clusterService: ClusterService,
	override val peerContactAddress: ContactAddress,
	val channel: AsynchronousSocketChannel,
	receiverFromPeer: Receiver,
	val oPeerRemoteAddress: Maybe[SocketAddress],
	val channelOrigin: ChannelOrigin
) extends ParticipantDelegate, BehaviorAspectOfACommunicableDelegate { thisCommunicableDelegate =>
	val config: DelegateConfig = clusterService.config.participantDelegatesConfig

	private val transmitterToPeer: SequentialTransmitter[Protocol] = clusterService.buildTransmitterFor(channel, Maybe.some(peerContactAddress))

	private var agreedVersion: ProtocolVersion = ProtocolVersion.NOT_SPECIFIED
	inline def getAgreedVersion: ProtocolVersion = agreedVersion
	private var versionsSupportedByPeer: Set[ProtocolVersion] = Set.empty

	/** The state of the peer according to it */
	private[service] var peerStatePhoto: Maybe[MemberViewpoint] = Maybe.empty

	/** Only Used when the cluster service is in aspirant state. */
	private[service] var clusterCreatorProposedByPeer: ContactAddress | Null = null
	/** Memorizes which was the cluster-creator proposal sent to the peer. 
	 * Only Used when the cluster service is in aspirant state. */
	private[service] var lastClusterCreatorProposalSentToPeer: ContactAddress | Null = null

	private var lastRequestId: Short = 0
	private val requestExchangeByRequestId: mutable.LongMap[OutgoingRequestExchange[?]] = mutable.LongMap.empty

	override def isCommunicable: Boolean = true

	override def isStable: Boolean = (agreedVersion != ProtocolVersion.NOT_SPECIFIED) && oPeerMembershipStatusAccordingToMe.isDefined

	override def communicationStatus: CommunicationStatus = if isStable then HANDSHOOK else CONNECTED

	inline def contactCard: ContactCard = (peerContactAddress, versionsSupportedByPeer)

	private[service] def updateState(peersNewMembershipStatus: MembershipStatus, peersNewSupportedVersions: Set[ProtocolVersion] = versionsSupportedByPeer, newCreationInstant: Instant = peerCreationInstant): Unit = {
		val previousPeersSupportedVersions = versionsSupportedByPeer
		val previousMembershipStatusOfPeerAccordingToMe = oPeerMembershipStatusAccordingToMe
		versionsSupportedByPeer = peersNewSupportedVersions
		agreedVersion = clusterService.determineAgreedVersion(peersNewSupportedVersions).getOrElse(ProtocolVersion.NOT_SPECIFIED)
		oPeerMembershipStatusAccordingToMe = Maybe.some(peersNewMembershipStatus)
		peerCreationInstant = newCreationInstant
		val behavior = clusterService.getMembershipScopedBehavior
		if peersNewSupportedVersions != previousPeersSupportedVersions then behavior.onDelegateCommunicabilityChange(this)
		if !previousMembershipStatusOfPeerAccordingToMe.contentEquals(peersNewMembershipStatus) then behavior.onDelegateMembershipChange(this)
	}

	/** Called just after a successfully connecting through a brand-new connection initiated by me and accepted by the peer (I am at the client side of the channel). */
	def startConversationAsClient(isReconnection: Boolean): Unit = {
		if isReconnection then sendPeerAHelloIAmBack()
		else sendPeerAHelloIExist()
		receiveNextMessages()
	}

	/** Called just after a successfully receiving the first message (which should be a [[Hello]]) through a brand-new connection initiated by the peer and accepted by me (I am at the server side of the channel). */
	def startConversationAsServer(hello: Hello): Unit = {
		if clusterService.getMembershipScopedBehavior.handleInitiatorMessageFrom(thisCommunicableDelegate, hello) then receiveNextMessages()
	}

	/** Receives the messages sent by the peer to me. The received messages are consumed sequentially and eagerly, but the handling of them is governed by the [[MembershipScopedBehavior.handleInitiatorMessageFrom]] which may defer some reactions. */
	private def receiveNextMessages(): Unit = {
		receiverFromPeer.receive[Protocol](agreedVersion, config.receiverTimeout, config.timeUnit) {
			case responseFromPeer: Response =>
				scribe.debug(s"I (`${clusterService.myAddress}`) have received the response `$responseFromPeer from participant at `$peerContactAddress`.")
				sequencer.executeSequentially {
					requestExchangeByRequestId.remove(responseFromPeer.toRequest).fold {
						scribe.warn(s"I have received a response from the participant at $peerContactAddress to a request I haven't made: response = $responseFromPeer")
					} { ore =>
						sequencer.cancel(ore.oTimeoutSchedule.get)
						val request = ore.oRequest.get
						if ore.asInstanceOf[OutgoingRequestExchange[request.type]].onResponse(request, responseFromPeer.asInstanceOf[request.ResponseType]) then receiveNextMessages()
						else onTerminatingMessageReceived(responseFromPeer)
					}
				}

			case initiatorMessageFromPeer: InitiationMsg =>
				scribe.debug(s"I (`${clusterService.myAddress}`) have received the message `$initiatorMessageFromPeer from participant at `$peerContactAddress`.")
				sequencer.executeSequentially {
					if clusterService.getMembershipScopedBehavior.handleInitiatorMessageFrom(thisCommunicableDelegate, initiatorMessageFromPeer) then receiveNextMessages()
					else onTerminatingMessageReceived(initiatorMessageFromPeer)
				}

			case fault: Receiver.Fault =>
				val errorMessage = s"Failure while I (${clusterService.myAddress}) was receiving a message from `$peerContactAddress`."
				scribe.error(fault.scribeContent(errorMessage)*)
				sequencer.executeSequentially(restartChannel(errorMessage + fault))
		}
	}

	private def onTerminatingMessageReceived(message: Protocol): Unit = {
		scribe.info(s"The conversation between me (`${clusterService.myAddress}`) and $peerContactAddress was terminated after I received the message `$message``")
		if clusterService.removeDelegate(thisCommunicableDelegate, true) then startChannelClosing()
		// TODO analyze if something else should be done here (or it is responsibility of the message handlers when they return `false`).
	}


	private[service] def notifyPeerThatAConversationStartedWith(otherParticipant: ContactAddress, isARestartAfterReconnection: Boolean): Unit = {
		assert(otherParticipant != peerContactAddress && otherParticipant != clusterService.myAddress)
		transmitToPeerOrRestartChannel(ConversationStartedWith(otherParticipant, isARestartAfterReconnection))
	}

	private[service] def notifyPeerThatILostCommunicationWith(otherParticipant: ContactAddress): Unit = {
		assert(otherParticipant != peerContactAddress && otherParticipant != clusterService.myAddress)
		transmitToPeerOrRestartChannel(ILostCommunicationWith(otherParticipant))
	}

	private[service] def reportTransmissionFailure(failure: Transmitter.NotDelivered): Unit = {
		failure match {
			case transmissionFailure: Transmitter.TransmissionFailure =>
				scribe.error(s"I (`${clusterService.myAddress}`) failed to transmit the message `${transmissionFailure.rootMessage}` to `$peerContactAddress` through the channel `$channel`.", transmissionFailure.cause)
			case serializationFailure: Transmitter.SerializationProblem =>
				scribe.error(s"I (`${clusterService.myAddress}`) failed to serialize the message `${serializationFailure.rootMessage}` at position ${serializationFailure.problem.position} while transmitting to `$peerContactAddress` through the channel `$channel` ${if serializationFailure.aFragmentWasTransmitted then "after some bytes were transmitted." else "before any byte was transmitted."}", serializationFailure.problem)
		}
	}

	private[service] def ifFailureReportItAndThen(consumer: Transmitter.NotDelivered => Unit)(report: Transmitter.Report): Unit = {
		report match {
			case Transmitter.Delivered => // do nothing

			case failure: Transmitter.NotDelivered =>
				reportTransmissionFailure(failure)
				sequencer.executeSequentially(consumer(failure))
		}
	}

	/** Specifies what the [[askPeer]] method requires:
	 * - to build the requesting message to be sent to the peer
	 * - and to handle each of the three mutually exclusive outcomes.
	 * Also maintains the state or the request.
	 * Instances of this class are single-use and should be disposed after its usage. To ensure this create the instance inline when calling the [[ask]] method.
	 * Exactly one of the `on*` methods is called once, and the call occurs within a [[sequencer.executeSequentially]] block. */
	private[service] abstract class OutgoingRequestExchange[Q <: Request](val responseTimeout: MilliDuration = config.responseTimeout) { thisOutgoingRequestExchange =>

		private[CommunicableDelegate] var oTimeoutSchedule: Maybe[sequencer.Schedule] = Maybe.empty
		private[CommunicableDelegate] var oRequest: Maybe[Q] = Maybe.empty

		/** The implementation should build a [[Request]] with the specified [[RequestId]].
		 * Called within a [[sequencer.executeSequentially]] block. */
		def buildRequest(requestId: RequestId): Q

		/** Called (within a [[sequencer.executeSequentially]] block) if a response to the request is received.
		 * Should return `true` for the conversation with the peer to continue. */
		def onResponse(request: Q, response: request.ResponseType): Boolean

		/** Called (within a [[sequencer.executeSequentially]] block) if the transmission of the request fails. */
		def onTransmissionError(request: Q, error: NotDelivered): Unit

		/** Called (within a [[sequencer.executeSequentially]] block) if no response to the request is received within the time passed to the `responseTimeout` parameter of the [[askPeer]] method. */
		def onTimeout(request: Q): Unit
	}

	/** Sends a [[Request]] to the peer and then calls once exactly one of the `on*` methods of the specified [[OutgoingRequestExchange]]. */
	private[service] def askPeer[Q <: Request](requestExchange: OutgoingRequestExchange[Q]): Unit = {
		val requestId = lastRequestId.incremented
		lastRequestId = requestId
		val request = requestExchange.buildRequest(requestId)
		requestExchange.oRequest = Maybe.some(request)
		transmitToPeer(request) {
			case Delivered =>
				sequencer.executeSequentially {
					val timeoutSchedule = sequencer.newDelaySchedule(requestExchange.responseTimeout)
					requestExchange.oTimeoutSchedule = Maybe.some(timeoutSchedule)
					requestExchangeByRequestId.put(requestId, requestExchange)
					sequencer.scheduleSequentially(timeoutSchedule) { () =>
						requestExchangeByRequestId.remove(requestId).foreach { removedExchange =>
							assert(removedExchange eq requestExchange)
							requestExchange.onTimeout(request)
						}
					}
				}
			case nd: NotDelivered =>
				reportTransmissionFailure(nd)
				sequencer.executeSequentially {
					requestExchange.onTransmissionError(request, nd)
				}
		}
	}


	/** A partially implemented [[OutgoingRequestExchange]] that:
	 *  - retries a single time if a non-response timeout occurs,
	 *  - and causes a channel restart if either the transmission fails or a non-response timeouts occurs twice. */
	private[service] abstract class SingleRetryOutgoingRequestExchange[Q <: Request](responseTimeout: MilliDuration = config.responseTimeout, retryDelay: MilliDuration = config.requestRetryDelay) extends OutgoingRequestExchange[Q](responseTimeout) {
		private[CommunicableDelegate] var isFirstTry: Boolean = true

		override def onTransmissionError(request: Q, error: NotDelivered): Unit = {
			restartChannel(s"Transmission failure while trying to send `$request` to participant at $peerContactAddress: $error")
		}

		/** Since the [[AsynchronousSocketChannel]] uses TCP, it ensures loss-less in-order delivery. Therefore, the purpose to retry a request after a no-response timeout is to reveal any silent communication problem or to detect silent middlebox interference.
		 * If the peer is alive but not responding (e.g., deadlock, overload), retries would not help.
		 * In conclusion, no more than a retry is needed. */
		override def onTimeout(request: Q): Unit = {
			if isFirstTry then {
				isFirstTry = false
				val schedule: sequencer.Schedule = sequencer.newDelaySchedule(retryDelay)
				sequencer.scheduleSequentially(schedule) { () => askPeer(this) }
			}
			else restartChannel(s"Non-response timeout: request ${oRequest.get} was sent to participant at $peerContactAddress twice (initial attempt got no response within response timeout), but no response was received.")
		}
	}

	private[service] def transmitToPeerOrRestartChannel(message: Protocol): Unit = {
		transmitToPeer(message)(ifFailureReportItAndThen(restartChannel))
	}
	
	private[service] def transmitToPeer(message: Protocol)(onTransmissionComplete: Transmitter.Report => Unit): Unit = {
		transmitterToPeer.transmit(message, agreedVersion, config.behindTransmissionEnabled, config.transmitterTimeout, config.timeUnit)(onTransmissionComplete)
	}

	private[service] def replaceMyselfWithAnIncommunicableDelegate(reason: IncommunicabilityReason, motive: Any): Maybe[IncommunicableDelegate] = {
		// release this delegate
		startChannelClosing()
		// replace me with an incommunicable delegate.
		if clusterService.removeDelegate(this, false) then {
			val myReplacement = clusterService.addANewIncommunicableDelegate(peerContactAddress, reason)
			myReplacement.initializeStateBasedOn(this)
			// notify
			clusterService.getMembershipScopedBehavior.onDelegateCommunicabilityChange(myReplacement) // TODO consider moving this line inside the `notify*` method called in the next line
			clusterService.notifyListenersAndOtherParticipantsThatAParticipantBecomeIncommunicable(peerContactAddress, reason, motive)
			Maybe.some(myReplacement)
		} else Maybe.empty
	}

	private[service] def replaceMyselfWithACommunicableDelegate(newChannel: AsynchronousSocketChannel, newFromPeerReceiver: Receiver, oPeerNewRemoteAddress: Maybe[SocketAddress], newChannelOrigin: ChannelOrigin): CommunicableDelegate = {
		// release this delegate
		startChannelClosing()
		// replace me with a communicable delegate.
		val wasRemoved = clusterService.removeDelegate(this, false)
		assert(wasRemoved)
		val myReplacement = clusterService.addANewCommunicableDelegate(peerContactAddress, newChannel, newFromPeerReceiver, oPeerNewRemoteAddress, newChannelOrigin)
		myReplacement.initializeStateBasedOn(this)
		// notify
		clusterService.getMembershipScopedBehavior.onDelegateCommunicabilityChange(myReplacement) // TODO this isn't exactly a communicability change. Analyze it. 
		clusterService.notifyListenersThat(CommunicationChannelReplaced(peerContactAddress))
		myReplacement
	}

	private[service] def restartChannel(notDeliveredCause: Transmitter.NotDelivered): Unit =
		restartChannel(s"A transmission to $peerContactAddress failed with $notDeliveredCause")

	private[service] def restartChannel(motive: String): Unit = {
		scribe.debug(s"The channel between me (`${clusterService.myAddress}`) and participant at`$channel` is restarting becase: $motive" )
		replaceMyselfWithAnIncommunicableDelegate(IS_CONNECTING_AS_CLIENT, s"To restart the channel because of: $motive").foreach { myReplacement =>
			clusterService.connectToAndThenStartConversationWithParticipant(myReplacement, true)
		}
	}

	/** Initiates a graceful closing of the [[channel]], if it is open. */
	private[service] def startChannelClosing(): Unit = {
		// if the channel is open, close it gracefully.
		if channel.isOpen then {
			scribe.trace(s"About to gracefully close the connection that I $channelOrigin between `$peerContactAddress` and me (`${clusterService.myAddress}`).")
			channel.shutdownInput()  // TODO handle exceptions
			transmitterToPeer.triggerOnIdle(() => channel.close()) // TODO handle exceptions
		}
	}	
}
