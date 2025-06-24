package readren.matrix
package cluster.service

import cluster.channel.Receiver.ChannelClosedByPeer
import cluster.channel.Transmitter.{Delivered, NotDelivered, Report}
import cluster.channel.{Receiver, Transmitter}
import cluster.serialization.ProtocolVersion
import cluster.service.ClusterService.{ChannelOrigin, DelegateConfig}
import cluster.service.Protocol.*
import cluster.service.Protocol.CommunicationStatus.{CONNECTED, HANDSHOOK}
import cluster.service.Protocol.IncommunicabilityReason.IS_CONNECTING_AS_CLIENT

import readren.taskflow.Maybe
import readren.taskflow.SchedulingExtension.MilliDuration

import java.net.SocketAddress
import java.nio.channels.AsynchronousSocketChannel
import scala.collection.mutable
import scala.util.control.NonFatal

/** A [[ParticipantDelegate]] that is currently able to communicable with the participant. */
class CommunicableDelegate(
	override val clusterService: ClusterService,
	override val peerAddress: SocketAddress,
	val peerChannel: AsynchronousSocketChannel,
	val channelOrigin: ChannelOrigin
) extends ParticipantDelegate, BehaviorAspectOfACommunicableDelegate { thisCommunicableDelegate =>
	val config: DelegateConfig = clusterService.config.participantDelegatesConfig

	private val transmitterToPeer: Transmitter = new Transmitter(peerChannel)
	private val receiverFromPeer: Receiver = new Receiver(peerChannel)
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
	private val requestExchangeStateByRequestId: mutable.LongMap[OutgoingRequestExchange[?]#Session] = mutable.LongMap.empty

	override def isCommunicable: Boolean = true

	override def isStable: Boolean = (agreedVersion != ProtocolVersion.NOT_SPECIFIED) && oPeerMembershipStatusAccordingToMe.isDefined

	override def communicationStatus: CommunicationStatus = if isStable then HANDSHOOK else CONNECTED

	override def info: Maybe[ParticipantInfo] =
		oPeerMembershipStatusAccordingToMe.map(ParticipantInfo(communicationStatus, _))

	inline def contactCard: ContactCard = (peerAddress, versionsSupportedByPeer)

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

	def startConversationAsServer(): Unit = {
		receiveNextMessages()
	}

	def startConversationAsClient(isReconnection: Boolean): Unit = {
		clusterService.getMembershipScopedBehavior.openConversationWith(this, isReconnection)
		receiveNextMessages()
	}

	/** Receives the messages sent by the peer to me. The received messages are consumed sequentially and eagerly, but the handling of them is governed by the [[MembershipScopedBehavior.handleInitiatorMessageFrom]] which may defer some reactions. */
	private def receiveNextMessages(): Unit = {
		receiverFromPeer.receive[Protocol](agreedVersion, config.receiverTimeout, config.timeUnit) {
			case responseFromPeer: Response =>
				sequencer.executeSequentially {
					requestExchangeStateByRequestId.remove(responseFromPeer.toRequest).fold {
						scribe.warn(s"I have received a response from the participant at $peerAddress to a request I haven't made: response = $responseFromPeer")
					} { state =>
						sequencer.cancel(state.timeoutSchedule)
						if state.exchange.asInstanceOf[OutgoingRequestExchange[Request]].onResponse(state.request, responseFromPeer.asInstanceOf[state.request.ResponseType]) then receiveNextMessages()
						else onTerminatingMessageReceived(responseFromPeer)
					}
				}

			case initiatorMessageFromPeer: InitiationMsg =>
				sequencer.executeSequentially {
					if clusterService.getMembershipScopedBehavior.handleInitiatorMessageFrom(thisCommunicableDelegate, initiatorMessageFromPeer) then receiveNextMessages()
					else onTerminatingMessageReceived(initiatorMessageFromPeer)
				}

			case fault: Receiver.Fault =>
				val errorMessage = s"Failure while receiving a message from the peer $peerChannel: $fault"
				scribe.error(errorMessage)
				sequencer.executeSequentially(restartChannel(errorMessage))
		}
	}

	private def onTerminatingMessageReceived(message: Protocol): Unit = {
		scribe.debug(s"The channel {$peerChannel} reception completed with the terminal message {$message}")
		peerChannel.shutdownInput()
		// TODO analyze if something else should be done here (or it is responsibility of the message handlers when they return `false`).
	}


	private[service] def notifyPeerThatAConversationStartedWith(otherParticipant: ContactAddress, isARestartAfterReconnection: Boolean): Unit = {
		assert(otherParticipant != peerAddress && otherParticipant != clusterService.myAddress)
		transmitToPeerOrRestartChannel(ConversationStartedWith(otherParticipant, isARestartAfterReconnection))
	}

	private[service] def notifyPeerThatILostCommunicationWith(otherParticipant: ContactAddress): Unit = {
		assert(otherParticipant != peerAddress && otherParticipant != clusterService.myAddress)
		transmitToPeerOrRestartChannel(ILostCommunicationWith(otherParticipant))
	}

	private[service] def reportTransmissionFailure(failure: Transmitter.NotDelivered): Unit = {
		failure match {
			case transmissionFailure: Transmitter.TransmissionFailure =>
				scribe.error(s"A transmission failure occurred while transmitting `${transmissionFailure.rootMessage}` to the channel `$peerChannel`", transmissionFailure.cause)
			case serializationFailure: Transmitter.SerializationProblem =>
				scribe.error(s"A serialization failure occurred at position ${serializationFailure.problem.position} while transmitting `${serializationFailure.rootMessage}` to the channel `$peerChannel` ${if serializationFailure.aFragmentWasTransmitted then "after some bytes were transmitted" else "before any byte was transmitted"}", serializationFailure.problem)
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
	 * Exactly one of the `on*` methods is called once, and the call occurs within a [[sequencer.executeSequentially]] block. */
	private[service] abstract class OutgoingRequestExchange[Q <: Request](val responseTimeout: MilliDuration = config.responseTimeout) { thisOutgoingRequestExchange =>

		class Session(val request: Q, val timeoutSchedule: sequencer.Schedule) {
			def exchange: thisOutgoingRequestExchange.type = thisOutgoingRequestExchange
		}

		def newSession(request: Q, timeoutSchedule: sequencer.Schedule): Session = new Session(request, timeoutSchedule)

		/** The implementation should build a [[Request]] with the specified [[RequestId]].
		 * Called within a [[sequencer.executeSequentially]] block. */
		def buildRequest(requestId: RequestId): Q

		/** Called (within a [[sequencer.executeSequentially]] block) if a response to the request is received.
		 * Should return `true` for the conversation with the peer to continue. */
		def onResponse(request: Q, response: request.ResponseType): Boolean

		/** Called (within a [[sequencer.executeSequentially]] block) if the transmission of the request fails. */
		def onTransmissionError(request: Q, error: NotDelivered): Unit

		/** Called (within a [[sequencer.executeSequentially]] block) if no response to the request is received within the time passed to the `responseTimeout` parameter of the [[askPeer]] method. */
		def onTimeout(state: Session): Unit
	}

	/** Sends a [[Request]] to the peer and then calls once exactly one of the `on*` methods of the specified [[OutgoingRequestExchange]]. */
	private[service] def askPeer[Q <: Request](requestExchange: OutgoingRequestExchange[Q]): Unit = {
		lastRequestId = lastRequestId.incremented
		val requestId = lastRequestId
		val request = requestExchange.buildRequest(requestId)
		transmitToPeer(request) {
			case Delivered =>
				sequencer.executeSequentially {
					val timeoutSchedule = sequencer.newDelaySchedule(requestExchange.responseTimeout)
					val exchangeSession = requestExchange.newSession(request, timeoutSchedule)
					requestExchangeStateByRequestId.put(requestId, exchangeSession)
					sequencer.scheduleSequentially(timeoutSchedule) { () =>
						requestExchangeStateByRequestId.remove(requestId).foreach { removedState =>
							assert(removedState eq exchangeSession)
							exchangeSession.exchange.onTimeout(exchangeSession)
						}
					}
				}
			case nd: NotDelivered =>
				sequencer.executeSequentially {
					requestExchange.onTransmissionError(request, nd)
				}
		}
	}


	/** A partially implemented [[OutgoingRequestExchange]] that:
	 *  - retries a single time if a non-response timeout occurs,
	 *  - and causes a channel restart if either the transmission fails or a non-response timeouts occurs twice. */
	private[service] abstract class SingleRetryOutgoingRequestExchange[Q <: Request](responseTimeout: MilliDuration = config.responseTimeout, retryDelay: MilliDuration = config.requestRetryStartingDelay) extends OutgoingRequestExchange[Q](responseTimeout) {

		override def newSession(request: Q, timeoutSchedule: sequencer.Schedule): Session = new SingleRetrySession(request, timeoutSchedule)

		private class SingleRetrySession(request: Q, timeoutSchedule: sequencer.Schedule) extends Session(request, timeoutSchedule) {
			var isFirstAttempt = true
		}

		override def onTransmissionError(request: Q, error: NotDelivered): Unit = {
			reportTransmissionFailure(error)
			restartChannel(s"Transmission failure while trying to send `$request` to participant at $peerAddress: $error")
		}

		/** Since the [[AsynchronousSocketChannel]] uses TCP, it ensures loss-less in-order delivery. Therefore, the purpose to retry a request after a no-response timeout is to reveal any silent communication problem or to detect silent middlebox interference.
		 * If the peer is alive but not responding (e.g., deadlock, overload), retries would not help.
		 * In conclusion, no more than a retry is needed. */
		override def onTimeout(session: Session): Unit = {
			val singleRetrySession = session.asInstanceOf[SingleRetrySession]
			if singleRetrySession.isFirstAttempt then {
				singleRetrySession.isFirstAttempt = false
				val schedule: sequencer.Schedule = sequencer.newDelaySchedule(retryDelay)
				sequencer.scheduleSequentially(schedule) { () => askPeer(this) }
			}
			else restartChannel(s"Non-response timeout after sending the request `${session.request}` to participant at $peerAddress.")
		}
	}

	private[service] def transmitToPeerOrRestartChannel(message: Protocol): Unit = {
		transmitToPeer(message)(ifFailureReportItAndThen(restartChannel))
	}
	
	private[service] def transmitToPeer(message: Protocol)(onComplete: Transmitter.Report => Unit): Unit = {
		transmitterToPeer.transmit[Protocol](message, agreedVersion, config.transmitterTimeout, config.timeUnit)(onComplete)
	}

	private[service] def replaceMyselfWithAnIncommunicableDelegate(reason: IncommunicabilityReason, motive: Any): Maybe[IncommunicableDelegate] = {
		// release this delegate
		startPeerChannelClosing()
		// replace me with an incommunicable delegate.
		if clusterService.removeDelegate(this, false) then {
			val myReplacement = clusterService.addANewIncommunicableDelegate(peerAddress, reason)
			myReplacement.initializeStateBasedOn(this)
			// notify
			clusterService.getMembershipScopedBehavior.onDelegateCommunicabilityChange(myReplacement) // TODO consider moving this line inside the `notify*` method called in the next line
			clusterService.notifyListenersAndOtherParticipantsThatAParticipantBecomeIncommunicable(peerAddress, reason, motive)
			Maybe.some(myReplacement)
		} else Maybe.empty
	}

	private[service] def replaceMyselfWithACommunicableDelegate(newChannel: AsynchronousSocketChannel, channelOrigin: ChannelOrigin): Maybe[CommunicableDelegate] = {
		// release this delegate
		startPeerChannelClosing()
		// replace me with a communicable delegate.
		if clusterService.removeDelegate(this, false) then {
			val myReplacement = clusterService.addANewCommunicableDelegate(peerAddress, newChannel, channelOrigin)
			myReplacement.initializeStateBasedOn(this)
			// notify
			clusterService.getMembershipScopedBehavior.onDelegateCommunicabilityChange(myReplacement) // TODO this isn't exactly a communicability change. Analyze it. 
			clusterService.notifyListenersThat(CommunicationChannelReplaced(peerAddress))
			Maybe.some(myReplacement)
		} else Maybe.empty
	}

	private[service] def restartChannel(motive: Any): Unit = {
		replaceMyselfWithAnIncommunicableDelegate(IS_CONNECTING_AS_CLIENT, s"To restart the channel because of: $motive").foreach { myReplacement =>
			clusterService.connectToAndThenStartConversationWithParticipant(myReplacement, true)
		}
	}

	/** Initiates a graceful closing of the [[peerChannel]], if it is open. */
	private[service] def startPeerChannelClosing(): Unit = {
		// if the channel is open, close it gracefully.
		if peerChannel.isOpen then {
			peerChannel.shutdownOutput()

			def loop(): Unit = {
				try {
					receiverFromPeer.receive[Protocol](agreedVersion, config.receiverTimeout, config.timeUnit) {
						case cbp: ChannelClosedByPeer if cbp.missingBytesAccordingToLastFrame == 0 =>
							peerChannel.close()
							scribe.info(s"Channel closed by peer while purging the channel's input connection of a released delegate of the participant at $peerAddress.")
						case fault: Receiver.Fault =>
							peerChannel.close()
							scribe.error(s"Failure while purging the channel's input connection of a released delegate of the participant at $peerAddress.", fault.toString)
						case cd: ChannelDiscarded =>
							scribe.info(s"The channel of the released delegate of the participant at $peerAddress was gracefully closed")
							peerChannel.close()
						case message: Protocol =>
							scribe.warn(s"The following message from the participant at $peerAddress was ignored because it was received after the delegate was released:", message.toString)
							loop()
					}
				} catch {
					case NonFatal(e) =>
						peerChannel.close()
						scribe.error(s"Failure when trying to purge the next ignored message from the channel's input connection of a released delegate of the participant at $peerAddress.", e)
				}
			}

			loop()
		}
	}
}
