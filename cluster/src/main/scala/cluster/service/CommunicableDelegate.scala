package readren.matrix
package cluster.service

import cluster.channel.Receiver.ChannelClosedByPeer
import cluster.channel.Transmitter.{Delivered, NotDelivered, Report}
import cluster.channel.{Receiver, Transmitter}
import cluster.misc.DoNothing
import cluster.service.ClusterService.{ChannelOrigin, DelegateConfig}
import cluster.service.Protocol.*
import cluster.service.Protocol.CommunicationStatus.{CONNECTED, HANDSHOOK}
import cluster.service.Protocol.IncommunicabilityReason.{IS_CONNECTING_AS_CLIENT, IS_INCOMPATIBLE}
import cluster.service.Protocol.MembershipStatus.{ASPIRANT, MEMBER, UNKNOWN}
import cluster.service.ProtocolVersion
import common.CompileTime.getTypeName

import readren.taskflow.Maybe

import java.net.SocketAddress
import java.nio.channels.AsynchronousSocketChannel
import scala.util.control.NonFatal

object CommunicableDelegate {
	type TimerInstance = Long
	val NANOS_PER_MILLISECOND = 1_000_000L
}

/** A [[ParticipantDelegate]] that is currently able to communicable with the participant. */
class CommunicableDelegate(
	override val clusterService: ClusterService,
	override val peerAddress: SocketAddress,
	val peerChannel: AsynchronousSocketChannel,
	val channelOrigin: ChannelOrigin
) extends ParticipantDelegate {
	val config: DelegateConfig = clusterService.config.participantDelegatesConfig

	private val transmitterToPeer: Transmitter = new Transmitter(peerChannel)
	private val receiverFromPeer: Receiver = new Receiver(peerChannel)
	private[service] var agreedVersion: ProtocolVersion = ProtocolVersion.NOT_SPECIFIED

	/** The state of the peer according to it */
	private[service] var peerStatePhoto: Maybe[MemberViewpoint] = Maybe.empty

	/** Only Used when the cluster service is in aspirant state. */
	private[service] var clusterCreatorProposedByPeer: ContactAddress | Null = null
	/** Memorizes which was the cluster-creator proposal sent to the peer. 
	 * Only Used when the cluster service is in aspirant state. */
	private var lastClusterCreatorProposalSentToPeer: ContactAddress | Null = null

	// TODO remove the need of these two variables by implementing structured request/response mechanism. 
	private var isPotentiallyGone: Boolean = false
	private[service] var isPotentiallyOutOfSync: Boolean = false

	override def isCommunicable: Boolean = true

	override def isStable: Boolean = agreedVersion != ProtocolVersion.NOT_SPECIFIED && peerMembershipStatusAccordingToMe != UNKNOWN

	override def communicationStatus: CommunicationStatus = if isStable then HANDSHOOK else CONNECTED

	override def info: ParticipantInfo =
		ParticipantInfo(versionsSupportedByPeer, communicationStatus, peerMembershipStatusAccordingToMe)

	private[service] def updateState(newMembershipStatus: MembershipStatus, newSupportedVersions: Set[ProtocolVersion] = versionsSupportedByPeer, newCreationInstant: Instant = peerCreationInstant): Unit = {
		val previousAgreeVersion = agreedVersion
		val previousMembershipStatusOfPeerAccordingToMe = peerMembershipStatusAccordingToMe
		versionsSupportedByPeer = newSupportedVersions
		agreedVersion = clusterService.determineAgreedVersion(newSupportedVersions).getOrElse(ProtocolVersion.NOT_SPECIFIED)
		peerMembershipStatusAccordingToMe = newMembershipStatus
		peerCreationInstant = newCreationInstant
		val behavior = clusterService.getMembershipScopedBehavior
		if agreedVersion != previousAgreeVersion then behavior.onDelegateCommunicabilityChange(this)
		if newMembershipStatus ne previousMembershipStatusOfPeerAccordingToMe then behavior.onDelegateMembershipChange(this)
	}

	def startConversationAsServer(): Unit = {
		receiveNextMessages()
	}

	def startConversationAsClient(isReconnection: Boolean): Unit = {
		clusterService.getMembershipScopedBehavior.openConversationWith(this, isReconnection) {
			case Transmitter.Delivered =>
				receiveNextMessages()
			case failure: Transmitter.NotDelivered =>
				reportFailure(failure)
				sequencer.executeSequentially(restartChannel(failure))
		}
	}

	/** Receives the messages sent by the peer to me. The received messages are consumed sequentially and eagerly, but the handling of them is governed by the [[MembershipScopedBehavior.handleMessageFrom]] which may defer some reactions. */
	private def receiveNextMessages(): Unit = {
		receiverFromPeer.receive[Protocol](agreedVersion, config.receiverTimeout, config.timeUnit) {
			case messageFromPeer: Protocol =>
				sequencer.executeSequentially {
					isPotentiallyGone = false
					if clusterService.getMembershipScopedBehavior.handleMessageFrom(this, messageFromPeer) then receiveNextMessages()
					else scribe.debug(s"The channel {$peerChannel} reception completed with the terminal message {$messageFromPeer}")
				}

			case fault: Receiver.Fault =>
				val errorMessage = s"Failure while receiving a message from the peer $peerChannel: $fault"
				scribe.error(errorMessage)
				sequencer.executeSequentially(restartChannel(errorMessage))
		}
	}

	private[service] def reportFailure(failure: Transmitter.NotDelivered): Unit = {
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
				reportFailure(failure)
				sequencer.executeSequentially(consumer(failure))
		}
	}

	private[service] def handleMessage(hello: Hello): Boolean = {
		versionsSupportedByPeer = hello.versionsISupport
		val previousMembershipState = peerMembershipStatusAccordingToMe
		// update my viewpoint of the peer's membership.
		peerMembershipStatusAccordingToMe = ASPIRANT

		// Connect to participants I didn't know.
		clusterService.createADelegateForEachParticipantIDoNotKnowIn(hello.otherParticipantsIKnow)

		val previousAgreedVersion = agreedVersion
		// update the protocol-version to use when communicating with the peer, transmit the response: `Welcome` or `SupportedVersionsMismatch`.
		val haveToContinueReceivingMessages = clusterService.determineAgreedVersion(hello.versionsISupport) match {
			case Some(version) =>
				agreedVersion = version
				sendPeerAWelcome()
				true

			case None =>
				agreedVersion = ProtocolVersion.NOT_SPECIFIED
				transmitterToPeer.transmit[Protocol](SupportedVersionsMismatch, ProtocolVersion.OF_THIS_PROJECT, config.transmitterTimeout, config.timeUnit) { report =>
					ifFailureReportItAndThen(DoNothing)(report)
					replaceMyselfWithAnIncommunicableDelegate(IS_INCOMPATIBLE, s"None of the peer's supported versions according to his hello message are supported by me. Versions supported by peer: ${hello.versionsISupport}")
				}
				clusterService.notifyListenersThat(VersionIncompatibilityWith(peerAddress))
				false
		}

		// Notify changes
		if agreedVersion != previousAgreedVersion then clusterService.getMembershipScopedBehavior.onDelegateCommunicabilityChange(this)
		if previousMembershipState != ASPIRANT then clusterService.getMembershipScopedBehavior.onDelegateMembershipChange(this)
		if previousMembershipState eq MEMBER then clusterService.notifyListenersThat(ParticipantHasBeenRestarted(peerAddress))

		haveToContinueReceivingMessages
	}

	private[service] def handleMessageSupportedVersionsMismatch(): false = {
		clusterService.notifyListenersThat(VersionIncompatibilityWith(peerAddress))
		replaceMyselfWithAnIncommunicableDelegate(IS_INCOMPATIBLE, s"The peer told me we are not compatible.")
		false
	}

	private[service] def handleMessage(message: ChannelDiscarded): false = {
		scribe.error(s"The participant at ${peerAddress} sent me a ${getTypeName[ChannelDiscarded]} message through a channel that I already started to use.")
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
			release()
			false
		} else {
			scribe.warn(s"A ${getTypeName[Farewell]} from $peerCreationInstant was ignored because the creation instant does not match.")
			true
		}
	}

	private[service] def handleMessage(message: ResolveMembershipConflict): Unit = {
		isPotentiallyOutOfSync = false
		updateState(message.myMembershipStatus)
		val delegateByAddress = clusterService.delegateByAddress
		for (participantAddress, participantMembershipStatusAccordingToPeer) <- message.membershipStatusOfParticipantsIKnow do {
			delegateByAddress.getOrElse(participantAddress, null) match {
				case communicableDelegate: CommunicableDelegate if participantMembershipStatusAccordingToPeer ne communicableDelegate.peerMembershipStatusAccordingToMe =>
					communicableDelegate.requestPeerToResolveMembershipConflict()

				case null =>
					if participantAddress == clusterService.myAddress then {
						if participantMembershipStatusAccordingToPeer ne clusterService.myMembershipStatus then requestPeerToResolveMembershipConflict()
					} else {
						clusterService.addANewConnectingDelegateAndStartAConnectionToThenAConversationWithParticipant(participantAddress)
					}

				case _ => // do nothing
			}
		}
	}

	private[service] def handleMessage(message: AreYouInSyncWithMe): Unit = {
		updateState(message.myMembershipStatus)
		if message.yourMembershipStatusAccordingToMe ne clusterService.myMembershipStatus then {
			requestPeerToResolveMembershipConflict()
		} else {
			respondPeerYesIAmInSync()
		}
	}

	private[service] def handleMessage(phr: AnotherParticipantHasBeenRebooted): Unit = {
		clusterService.delegateByAddress.getOrElse(phr.restartedParticipantAddress, null) match {
			case communicableDelegate: CommunicableDelegate =>
				communicableDelegate.checkSyncWithPeer("Another participant told me that the peer participant has been rebooted")
			case incommunicableDelegate: IncommunicableDelegate =>
				if !incommunicableDelegate.isConnectingAsClient then incommunicableDelegate.tryToConnect()
			case null =>
				clusterService.addANewConnectingDelegateAndStartAConnectionToThenAConversationWithParticipant(phr.restartedParticipantAddress)
		}
	}

	private[service] def handleMessage(message: AnotherParticipantGone): Unit = {
		clusterService.delegateByAddress.getOrElse(message.goneParticipantAddress, null) match {
			case communicableDelegate: CommunicableDelegate =>
				if communicableDelegate.peerCreationInstant == message.goneParticipantCreationInstant then {
					if !communicableDelegate.isPotentiallyGone then {
						communicableDelegate.isPotentiallyGone = true
						communicableDelegate.removeMyselfIfNoAnswerFromPeer()
					}
				} else {
					scribe.warn(s"Received the message `$message` from $peerAddress with an unmatching creation instant (expected: ${communicableDelegate.peerCreationInstant}).")
				}
			case incommunicableDelegate: IncommunicableDelegate =>
				clusterService.removeDelegate(incommunicableDelegate, true)
		}
	}

	private def checkSyncWithPeer(why: String): Unit = {
		isPotentiallyOutOfSync = true
		askPeerIfHeIsInSyncWithMe {
			case Delivered =>
				sequencer.scheduleSequentially(sequencer.newDelaySchedule(config.responseTimeout)) { () =>
					if isPotentiallyOutOfSync then restartChannel(why + " and when asked he didn't answer in time.")
				}
			case nd: NotDelivered =>
				sequencer.executeSequentially {
					restartChannel(why + s" and when asked the transmission reported: $nd")
				}
		}
	}

	private def removeMyselfIfNoAnswerFromPeer(): Unit = {
		askPeerIfHeIsInSyncWithMe {
			case Transmitter.Delivered =>
				sequencer.scheduleSequentially(sequencer.newDelaySchedule(config.responseTimeout)) { () =>
					if this.isPotentiallyGone then clusterService.removeDelegate(this, true)
				}
			case _ =>
				sequencer.executeSequentially {
					clusterService.removeDelegate(this, true)
				}
		}
	}

	private[service] def sendPeerAHello(onComplete: Transmitter.Report => Unit): Unit = {
		transmitterToPeer.transmit[Protocol](Hello(clusterService.config.versionsISupport, clusterService.myCreationInstant, clusterService.getKnownParticipantsAddresses), ProtocolVersion.OF_THIS_PROJECT, config.transmitterTimeout, config.timeUnit)(onComplete)
	}

	private[service] def sendPeerAWelcome(): Unit = {
		transmitToPeer(Welcome(clusterService.getMembershipScopedBehavior.membershipStatus, clusterService.config.versionsISupport, clusterService.myCreationInstant, clusterService.getKnownParticipantsAddresses))(ifFailureReportItAndThen(restartChannel))
	}

	private[service] def requestPeerToResolveMembershipConflict(): Unit = {
		transmitToPeer(ResolveMembershipConflict(clusterService.myMembershipStatus, clusterService.getKnownParticipantsMembershipStatus))(ifFailureReportItAndThen(restartChannel))
	}

	private[service] def notifyPeerThatIHaveReconnected(onComplete: Transmitter.Report => Unit): Unit = {
		transmitToPeer(IHaveReconnected(clusterService.config.versionsISupport, clusterService.myMembershipStatus, clusterService.myCreationInstant))(onComplete)
	}

	private[service] def notifyPeerThatAnotherParticipantIsGone(goneParticipantAddress: ContactAddress, goneParticipantCreationInstant: Instant): Unit = {
		transmitToPeer(AnotherParticipantGone(goneParticipantAddress, goneParticipantCreationInstant))(ifFailureReportItAndThen(DoNothing))
	}

	private[service] def sendPeerARequestToJoin(joinTokenByMemberAddress: Map[ContactAddress, JoinToken])(onComplete: Transmitter.Report => Unit): Unit = {
		transmitToPeer(RequestToJoin(joinTokenByMemberAddress))(onComplete)
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

	private[service] def askPeerIfHeIsInSyncWithMe(onComplete: Transmitter.Report => Unit): Unit = {
		transmitToPeer(AreYouInSyncWithMe(clusterService.myMembershipStatus, peerMembershipStatusAccordingToMe))(onComplete)
	}

	private[service] def respondPeerYesIAmInSync(): Unit = {
		transmitToPeer(YesImAInSyncWithYou)(ifFailureReportItAndThen(restartChannel))
	}

	private[service] def transmitToPeer(message: Protocol)(onComplete: Transmitter.Report => Unit): Unit = {
		transmitterToPeer.transmit[Protocol](message, agreedVersion, config.transmitterTimeout, config.timeUnit)(onComplete)
	}

	private[service] def replaceMyselfWithAnIncommunicableDelegate(reason: IncommunicabilityReason, motive: Any): Maybe[IncommunicableDelegate] = {
		// release this delegate
		release()
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
		release()
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


	private[service] def release(): Unit = {
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
