package readren.matrix
package cluster.service

import cluster.channel.Receiver.ChannelClosedByPeer
import cluster.channel.Transmitter.{Delivered, NotDelivered, Report}
import cluster.channel.{Receiver, Transmitter}
import cluster.misc.DoNothing
import cluster.serialization.ProtocolVersion
import cluster.service.ClusterService.{ChannelOrigin, DelegateConfig}
import cluster.service.Protocol.*
import cluster.service.Protocol.CommunicationStatus.{CONNECTED, HANDSHOOK}
import cluster.service.Protocol.IncommunicabilityReason.{IS_CONNECTING_AS_CLIENT, IS_INCOMPATIBLE}
import cluster.service.Protocol.MembershipStatus.{ASPIRANT, MEMBER, UNKNOWN}
import common.CompileTime.getTypeName

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
) extends ParticipantDelegate { thisCommunicableDelegate =>
	val config: DelegateConfig = clusterService.config.participantDelegatesConfig

	private val transmitterToPeer: Transmitter = new Transmitter(peerChannel)
	private val receiverFromPeer: Receiver = new Receiver(peerChannel)
	private[service] var agreedVersion: ProtocolVersion = ProtocolVersion.NOT_SPECIFIED

	protected[service] var versionsSupportedByPeer: Set[ProtocolVersion] = Set.empty

	/** The state of the peer according to it */
	private[service] var peerStatePhoto: Maybe[MemberViewpoint] = Maybe.empty

	/** Only Used when the cluster service is in aspirant state. */
	private[service] var clusterCreatorProposedByPeer: ContactAddress | Null = null
	/** Memorizes which was the cluster-creator proposal sent to the peer. 
	 * Only Used when the cluster service is in aspirant state. */
	private var lastClusterCreatorProposalSentToPeer: ContactAddress | Null = null

	private var lastRequestId: RequestId = 0
	private val requestExchangeByRequestId: mutable.LongMap[(sequencer.Schedule, OutgoingRequestExchange[?])] = mutable.LongMap.empty

	override def isCommunicable: Boolean = true

	override def isStable: Boolean = agreedVersion != ProtocolVersion.NOT_SPECIFIED && peerMembershipStatusAccordingToMe != UNKNOWN

	override def communicationStatus: CommunicationStatus = if isStable then HANDSHOOK else CONNECTED

	override def info: ParticipantInfo =
		ParticipantInfo(communicationStatus, peerMembershipStatusAccordingToMe)

	inline def contactCard: ContactCard = (peerAddress, versionsSupportedByPeer)

	private[service] def updateState(peersNewMembershipStatus: MembershipStatus, peersNewSupportedVersions: Set[ProtocolVersion] = versionsSupportedByPeer, newCreationInstant: Instant = peerCreationInstant): Unit = {
		val previousPeersSupportedVersions = versionsSupportedByPeer
		val previousMembershipStatusOfPeerAccordingToMe = peerMembershipStatusAccordingToMe
		versionsSupportedByPeer = peersNewSupportedVersions
		agreedVersion = clusterService.determineAgreedVersion(peersNewSupportedVersions).getOrElse(ProtocolVersion.NOT_SPECIFIED)
		peerMembershipStatusAccordingToMe = peersNewMembershipStatus
		peerCreationInstant = newCreationInstant
		val behavior = clusterService.getMembershipScopedBehavior
		if peersNewSupportedVersions != previousPeersSupportedVersions then behavior.onDelegateCommunicabilityChange(this)
		if peersNewMembershipStatus ne previousMembershipStatusOfPeerAccordingToMe then behavior.onDelegateMembershipChange(this)
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
					requestExchangeByRequestId.remove(responseFromPeer.toRequest).fold {
						scribe.warn(s"I have received a response from the participant at $peerAddress to a request I haven't made: response = $responseFromPeer")
					} { (timeoutSchedule, requestExchange) =>
						sequencer.cancel(timeoutSchedule)
						if requestExchange.onResponse(responseFromPeer) then receiveNextMessages()
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
	private[service] abstract class OutgoingRequestExchange[R <: Request](val responseTimeout: MilliDuration = config.responseTimeout) {
		/** The implementation should build a [[Request]] with the specified [[RequestId]].
		 * Called within a [[sequencer.executeSequentially]] block. */
		def buildRequest(requestId: RequestId): R

		/** Called (within a [[sequencer.executeSequentially]] block) if a response to the request is received.
		 * Should return `true` for the conversation with the peer to continue. */
		def onResponse(response: Response): Boolean

		/** Called (within a [[sequencer.executeSequentially]] block) if the transmission of the request fails. */
		def onTransmissionError(request: R, error: NotDelivered): Unit

		/** Called (within a [[sequencer.executeSequentially]] block) if no response to the request is received within the time passed to the `responseTimeout` parameter of the [[askPeer]] method. */
		def onTimeout(request: R): Unit
	}

	/** Sends a [[Request]] to the peer and then calls once exactly one of the `on*` methods of the specified [[OutgoingRequestExchange]]. */
	private[service] def askPeer[R <: Request](requestExchange: OutgoingRequestExchange[R]): Unit = {
		val timeoutSchedule = sequencer.newDelaySchedule(requestExchange.responseTimeout)
		lastRequestId += 1
		val requestId = lastRequestId
		val request = requestExchange.buildRequest(requestId)
		transmitToPeer(request) {
			case Delivered =>
				sequencer.executeSequentially {
					requestExchangeByRequestId.put(requestId, (timeoutSchedule, requestExchange))
					sequencer.scheduleSequentially(timeoutSchedule) { () =>
						requestExchangeByRequestId.remove(requestId).foreach(_._2.asInstanceOf[OutgoingRequestExchange[R]].onTimeout(request))
					}
				}
			case nd: NotDelivered =>
				sequencer.executeSequentially {
					requestExchange.onTransmissionError(request, nd)
				}
		}
	}

	private[service] def handleMessage(message: HelloIExist): Boolean = {
		// Connect to participants I didn't know.
		clusterService.createADelegateForEachParticipantIDoNotKnowIn(message.otherParticipantsIKnow)

		if peerMembershipStatusAccordingToMe eq MEMBER then clusterService.notifyListenersThat(ParticipantHasBeenRestarted(peerAddress))

		// update my viewpoint of the peer's membership.
		updateState(ASPIRANT, message.versionsISupport, message.myCreationInstant)
		// transmit the response: `Welcome` or `SupportedVersionsMismatch` accordingly.
		if agreedVersion == ProtocolVersion.NOT_SPECIFIED then {
			handleHelloVersionMismatch(message.requestId, message.versionsISupport)
			false
		} else {
			sendPeerAWelcome(message.requestId)
			true
		}
	}

	private[service] def handleHelloVersionMismatch(helloRequestId: RequestId, versionsSupportedByPeer: Set[ProtocolVersion]): Unit = {
		transmitToPeer(SupportedVersionsMismatch(helloRequestId)) { report =>
			sequencer.executeSequentially {
				ifFailureReportItAndThen(DoNothing)(report)
				replaceMyselfWithAnIncommunicableDelegate(IS_INCOMPATIBLE, s"None of the peer's supported versions according to his hello message are supported by me. Versions supported by peer: $versionsSupportedByPeer")
				clusterService.notifyListenersThat(VersionIncompatibilityWith(peerAddress))
			}
		}
	}

	private[service] def handleMessage(message: ChannelDiscarded): false = {
		scribe.error(s"The participant at $peerAddress sent me a ${getTypeName[ChannelDiscarded]} message through a channel that I already started to use.")
		restartChannel("Channel unexpectedly discarded")
		false
	}

	private[service] def handleMessage(farewell: Farewell): Boolean = {
		if this.peerCreationInstant == UNSPECIFIED_INSTANT || this.peerCreationInstant == farewell.myCreationInstant then {
			if clusterService.removeDelegate(this, true) then {
				for case (address, delegate: CommunicableDelegate) <- clusterService.delegateByAddress do {
					delegate.notifyPeerThatAnotherParticipantIsGone(address, farewell.myCreationInstant)
				}
			}
			startPeerChannelClosing()
			false
		} else {
			scribe.warn(s"A ${getTypeName[Farewell]} from $peerCreationInstant was ignored because the creation instant does not match.")
			true
		}
	}

	private[service] def handleMessage(message: WaitMyMembershipStatusIs): Unit = {
		updateState(message.myMembershipStatus)
		val delegateByAddress = clusterService.delegateByAddress
		for (participantAddress, participantMembershipStatusAccordingToPeer) <- message.membershipStatusOfParticipantsIKnow do {
			delegateByAddress.getOrElse(participantAddress, null) match {
				case communicableDelegate: CommunicableDelegate if participantMembershipStatusAccordingToPeer ne communicableDelegate.peerMembershipStatusAccordingToMe =>
					communicableDelegate.checkSyncWithPeer(s"the participant at $peerAddress told me that the participant at $participantAddress has a different membership status ($participantMembershipStatusAccordingToPeer) than the one I remember (${communicableDelegate.peerMembershipStatusAccordingToMe}).")

				case null =>
					if participantAddress == clusterService.myAddress then {
						if participantMembershipStatusAccordingToPeer ne clusterService.myMembershipStatus then incitePeerToResolveMembershipConflict()
					} else {
						clusterService.addANewConnectingDelegateAndStartAConnectionToThenAConversationWithParticipant(participantAddress)
					}

				case _ => // do nothing
			}
		}
	}

	private[service] def handleMessage(message: AreYouInSyncWithMe): Unit = {
		if message.yourMembershipStatusAccordingToMe ne clusterService.myMembershipStatus then incitePeerToResolveMembershipConflict()
		transmitToPeer(AreWeInSyncResponse(message.requestId, message.myMembershipStatus eq peerMembershipStatusAccordingToMe))(ifFailureReportItAndThen(restartChannel))
	}

	private[service] def handleMessage(message: AnotherParticipantHasBeenRebooted): Unit = {
		clusterService.delegateByAddress.getOrElse(message.restartedParticipantAddress, null) match {
			case rebootedParticipantDelegate: CommunicableDelegate =>
				if rebootedParticipantDelegate.peerCreationInstant == message.restartedParticipantCreationInstant then {
					rebootedParticipantDelegate.checkSyncWithPeer(s"participant at $peerAddress told me that participant at ${message.restartedParticipantAddress} has been rebooted")
				} else {
					scribe.warn(s"I received the message `$message` from $peerAddress with an unmatching creation instant (expected: ${rebootedParticipantDelegate.peerCreationInstant}).")
				}
			case rebootedParticipantDelegate: IncommunicableDelegate =>
				if !rebootedParticipantDelegate.isConnectingAsClient then rebootedParticipantDelegate.tryToConnect()
			case null =>
				clusterService.addANewConnectingDelegateAndStartAConnectionToThenAConversationWithParticipant(message.restartedParticipantAddress)
		}
	}

	/** Checks if the peer knows my membership-status and incites him to update it otherwise. */
	private[service] def checkSyncWithPeer(why: String): Unit = {
		askPeer(new OutgoingRequestExchange[AreYouInSyncWithMe] {
			override def buildRequest(requestId: RequestId): AreYouInSyncWithMe = AreYouInSyncWithMe(requestId, clusterService.myMembershipStatus, peerMembershipStatusAccordingToMe)

			override def onResponse(response: Response): Boolean = response match {
				case AreWeInSyncResponse(_, myMembershipStatusAccordingToPeerMatches) =>
					if !myMembershipStatusAccordingToPeerMatches then incitePeerToResolveMembershipConflict()
					true
			}

			override def onTransmissionError(request: AreYouInSyncWithMe, error: NotDelivered): Unit = restartChannel(why + s" and when trying to ask it `$request` the transmission failed with: $error")

			override def onTimeout(request: AreYouInSyncWithMe): Unit = restartChannel(why + s" and when asked `$request` he didn't answer within $responseTimeout millis.")
		})
	}

	private[service] def handleMessage(message: AnotherParticipantGone): Unit = {
		clusterService.delegateByAddress.getOrElse(message.goneParticipantAddress, null) match {
			case goneParticipantDelegate: CommunicableDelegate =>
				if goneParticipantDelegate.peerCreationInstant == message.goneParticipantCreationInstant then {
					goneParticipantDelegate.removeMyselfIfNoAnswerFromPeer()
				} else {
					scribe.warn(s"I received the message `$message` from $peerAddress with an unmatching creation instant (expected: ${goneParticipantDelegate.peerCreationInstant}).")
				}
			case goneParticipantDelegate: IncommunicableDelegate =>
				clusterService.removeDelegate(goneParticipantDelegate, true)
		}
	}

	private def removeMyselfIfNoAnswerFromPeer(): Unit = {
		askPeer(new OutgoingRequestExchange[AreYouInSyncWithMe] {
			override def buildRequest(requestId: RequestId): AreYouInSyncWithMe =
				AreYouInSyncWithMe(requestId, clusterService.myMembershipStatus, peerMembershipStatusAccordingToMe)

			override def onResponse(response: Response): Boolean = response match {
				case AreWeInSyncResponse(_, myMembershipStatusAccordingToPeerMatches) =>
					if !myMembershipStatusAccordingToPeerMatches then incitePeerToResolveMembershipConflict()
					true
			}

			override def onTransmissionError(request: AreYouInSyncWithMe, error: NotDelivered): Unit =
				clusterService.removeDelegate(thisCommunicableDelegate, true)

			override def onTimeout(request: AreYouInSyncWithMe): Unit =
				clusterService.removeDelegate(thisCommunicableDelegate, true)
		})
	}

	/** A partially implemented [[OutgoingRequestExchange]] that:
	 *  - retries a single time if a non-response timeout occurs,
	 *  - and causes a channel restart if either the transmission fails or a non-response timeouts occurs twice. */
	private[service] abstract class SingleRetryOutgoingRequestExchange[R <: Request](responseTimeout: MilliDuration = config.responseTimeout, retryDelay: MilliDuration = config.requestRetryStartingDelay) extends OutgoingRequestExchange[R](responseTimeout) {
		private var isFirstAttempt = true

		override def onTransmissionError(request: R, error: NotDelivered): Unit = {
			reportTransmissionFailure(error)
			restartChannel(s"Transmission failure while trying to send `$request` to participant at $peerAddress: $error")
		}

		/** Since the [[AsynchronousSocketChannel]] uses TCP, it ensures loss-less in-order delivery. Therefore, the purpose to retry a request after a no-response timeout is to reveal any silent communication problem or to detect silent middlebox interference.
		 * If the peer is alive but not responding (e.g., deadlock, overload), retries would not help.
		 * In conclusion, no more than a retry is needed. */
		override def onTimeout(request: R): Unit = {
			if isFirstAttempt then {
				isFirstAttempt = false
				val schedule: sequencer.Schedule = sequencer.newDelaySchedule(retryDelay)
				sequencer.scheduleSequentially(schedule) { () => askPeer(this) }
			}
			else restartChannel(s"Non-response timeout after sending the request `$request` to participant at $peerAddress.")
		}
	}

	private[service] def sendPeerAHello(): Unit = {
		askPeer(new SingleRetryOutgoingRequestExchange[HelloIExist] {
			override def buildRequest(requestId: RequestId): HelloIExist =
				HelloIExist(requestId, clusterService.config.versionsISupport, clusterService.myCreationInstant, clusterService.getKnownParticipantsAddresses)

			override def onResponse(response: Response): Boolean = handleHelloResponse(response)
		})
	}

	private[service] def sendPeerAWelcome(requestId: RequestId): Unit = {
		transmitToPeer(Welcome(requestId, clusterService.myMembershipStatus, clusterService.config.versionsISupport, clusterService.myCreationInstant, clusterService.getKnownParticipantsAddresses))(ifFailureReportItAndThen(restartChannel))
	}

	private[service] def sendPeerAHelloIAmBack(): Unit = {
		askPeer(new SingleRetryOutgoingRequestExchange[HelloIAmBack] {
			override def buildRequest(requestId: RequestId): HelloIAmBack =
				HelloIAmBack(requestId, clusterService.config.versionsISupport, clusterService.myMembershipStatus, clusterService.myCreationInstant)

			override def onResponse(response: Response): Boolean =  handleHelloResponse(response)
		})
	}

	private def handleHelloResponse(response: Response): Boolean = response match {

		case Welcome(_, membershipStatus, versionsSupportedByPeer, peerCreationInstant, otherParticipantsKnowByPeer) =>
			// override the peer's membership status and supported versions with the values provided by the source of truth.
			updateState(membershipStatus, versionsSupportedByPeer, peerCreationInstant)
			assert(agreedVersion != ProtocolVersion.NOT_SPECIFIED)
			// Create a delegate for each participant that I did not know.
			clusterService.createADelegateForEachParticipantIDoNotKnowIn(otherParticipantsKnowByPeer)
			true

		case _: SupportedVersionsMismatch =>
			clusterService.notifyListenersThat(VersionIncompatibilityWith(peerAddress))
			replaceMyselfWithAnIncommunicableDelegate(IS_INCOMPATIBLE, s"The peer told me we are not compatible.")
			false
	}

	/** Incites the peer to update his memory of my membership-status and re-check his memory of the membership-status of other participants that differs from my memory. */
	private[service] def incitePeerToResolveMembershipConflict(): Unit = {
		transmitToPeer(WaitMyMembershipStatusIs(clusterService.myMembershipStatus, clusterService.getKnownParticipantsMembershipStatus.toMap))(ifFailureReportItAndThen(restartChannel))
	}

	private[service] def notifyPeerThatAnotherParticipantIsGone(goneParticipantAddress: ContactAddress, goneParticipantCreationInstant: Instant): Unit = {
		transmitToPeer(AnotherParticipantGone(goneParticipantAddress, goneParticipantCreationInstant))(ifFailureReportItAndThen(DoNothing))
	}

	private[service] def sendPeerAHeartbeat(): Unit = {
		transmitToPeer(Heartbeat(config.heartbeatPeriod))(ifFailureReportItAndThen(restartChannel))
	}

	private[service] def sendPeerAClusterCreatorProposal(proposedAspirantAddress: ContactAddress): Unit = {
		if proposedAspirantAddress != lastClusterCreatorProposalSentToPeer then {
			lastClusterCreatorProposalSentToPeer = proposedAspirantAddress
			transmitToPeer(ClusterCreatorProposal(proposedAspirantAddress))(ifFailureReportItAndThen(restartChannel))
		}
	}

	private[service] def notifyPeerThatILostCommunicationWith(otherParticipant: ContactAddress): Unit = {
		assert(otherParticipant != peerAddress && otherParticipant != clusterService.myAddress)
		transmitToPeer(ILostCommunicationWith(otherParticipant))(ifFailureReportItAndThen(restartChannel))
	}

	private[service] def notifyPeerThatAConversationStartedWith(otherParticipant: ContactAddress): Unit = {
		assert(otherParticipant != peerAddress && otherParticipant != clusterService.myAddress)
		transmitToPeer(ConversationStartedWith(otherParticipant))(ifFailureReportItAndThen(restartChannel))
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
			clusterService.getMembershipScopedBehavior.onDelegateCommunicabilityChange(myReplacement)
			clusterService.onDelegateBecomeIncommunicable(peerAddress, reason, motive)
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
