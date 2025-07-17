package readren.matrix
package cluster.service

import cluster.channel.Transmitter.NotDelivered
import cluster.misc.CommonExtensions.*
import cluster.misc.DoNothing
import cluster.serialization.ProtocolVersion
import cluster.service.Protocol.IncommunicabilityReason.IS_INCOMPATIBLE
import cluster.service.Protocol.{Aspirant, MembershipStatus, RequestId, UNSPECIFIED_INSTANT}
import cluster.service.behavior.MembershipScopedBehavior

/** Defines participant behavior aspects for a [[CommunicableDelegate]].
 *
 * This trait contains methods and functionality related to the [[ParticipantService]]'s role as aspirant or member of a cluster, primarily serving as helper methods for [[MembershipScopedBehavior]] subclasses.
 *
 * ==Design Rationale==
 * While all these members could exist directly in [[CommunicableDelegate]], they've been separated into this trait to:
 *   - Clearly distinguish communication infrastructure concerns (remaining in [[CommunicableDelegate]])
 *   - From participant behavior concerns (contained here)
 *
 * This separation is purely organizational - the trait exists to improve code structure and maintainability, not because of technical necessity.
 */
trait BehaviorAspectOfACommunicableDelegate { thisCommunicableDelegate: CommunicableDelegate =>

	private[service] def sendPeerAHelloIExist(): Unit = {
		askPeer(new SingleRetryOutgoingRequestExchange[HelloIExist] {
			override def buildRequest(requestId: RequestId): HelloIExist =
				HelloIExist(requestId, participantService.myAddress, participantService.config.versionsISupport, participantService.myCreationInstant, participantService.myMembershipStatus, participantService.getKnownParticipantsAddresses)

			override def onResponse(request: HelloIExist, response: request.ResponseType): Boolean = response match {
				case welcome: Welcome =>
					handleWelcome(welcome, false)
					true

				case svm: SupportedVersionsMismatch =>
					handleSupportedVersionsMismatch(svm)
					false
			}
		})
	}
	
	private[service] def sendPeerAHelloIAmBack(): Unit = {
		askPeer(new SingleRetryOutgoingRequestExchange[HelloIAmBack] {
			override def buildRequest(requestId: RequestId): HelloIAmBack =
				HelloIAmBack(requestId, participantService.myAddress, participantService.config.versionsISupport, participantService.myCreationInstant, participantService.myMembershipStatus)

			override def onResponse(request: HelloIAmBack, response: request.ResponseType): Boolean = response match {
				case welcome: Welcome =>
					handleWelcome(welcome, true)
					true

				case svm: SupportedVersionsMismatch =>
					handleSupportedVersionsMismatch(svm)
					false
			}
		})
	}

	private [service] def handleHello(hello: Hello): Boolean = hello match {
		case message: HelloIExist =>
			val previousMembershipStatus = oPeerMembershipStatusAccordingToMe

			// update my viewpoint of the peer's membership.
			updateState(message.myMembershipStatus, message.versionsISupport, message.myCreationInstant)

			// If the HelloIExist message comes from a participant that, according to my memory, isn't an aspirant; surely it was rebooted, so inform that.
			if previousMembershipStatus.fold(false)(Aspirant ne _) && (message.myMembershipStatus eq Aspirant) then {
				participantService.notifyListenersThat(MemberHasBeenRebooted(peerContactAddress))
				for case (contactAddress, delegate: CommunicableDelegate) <- participantService.delegateByAddress do {
					if contactAddress != peerContactAddress then delegate.transmitToPeerOrRestartChannel(AMemberHasBeenRebooted(peerContactAddress, message.myCreationInstant))
				}
			}

			// Connect to participants I didn't know.
			participantService.createADelegateForEachParticipantIDoNotKnowIn(message.otherParticipantsIKnow)

			// transmit the response: `Welcome` or `SupportedVersionsMismatch` accordingly.
			if getAgreedVersion == ProtocolVersion.NOT_SPECIFIED then {
				notifyAboutTheSupportedVersionsMismatch(message.requestId, message.versionsISupport)
				false
			} else {
				sendPeerAWelcome(message.requestId)
				true
			}
			
		case message: HelloIAmBack =>
			assert(message.myCreationInstant == peerCreationInstant)
			updateState(message.myMembershipStatus, message.versionsISupport)
			if getAgreedVersion == ProtocolVersion.NOT_SPECIFIED then {
				notifyAboutTheSupportedVersionsMismatch(message.requestId, message.versionsISupport)
				false
			}
			else {
				sendPeerAWelcome(message.requestId)
				true
			}
	}

	private def sendPeerAWelcome(requestId: RequestId): Unit = {
		transmitToPeerOrRestartChannel(Welcome(requestId, participantService.myMembershipStatus, participantService.config.versionsISupport, participantService.myCreationInstant, participantService.getKnownParticipantsAddresses))
	}

	/** @param isARestart `false` when triggered by a [[HelloIExist]]; `true` when triggered by a [[HelloIAmBack]]. */
	private def handleWelcome(welcome: Welcome, isARestart: Boolean): Unit = {
		// override the peer's membership status and supported versions with the values provided by the source of truth.
		updateState(welcome.myMembershipStatus, welcome.versionsISupport, welcome.myCreationInstant)
		assert(getAgreedVersion != ProtocolVersion.NOT_SPECIFIED)
		// Create a delegate for each participant that I did not know.
		participantService.createADelegateForEachParticipantIDoNotKnowIn(welcome.otherParticipants)
		// notify listeners and other participants
		participantService.notifyListenersAndOtherParticipantsThatAConversationStartedWith(peerContactAddress, isARestart)
	}

	private def handleSupportedVersionsMismatch(svm: SupportedVersionsMismatch): Unit = {
		replaceMyselfWithAnIncommunicableDelegate(IS_INCOMPATIBLE, s"The peer told me we are not compatible.").foreach { _ =>
			participantService.notifyListenersThat(VersionIncompatibilityWith(peerContactAddress))
		}
	}

	// TODO correct this method, either the name or the behavior
	private def notifyAboutTheSupportedVersionsMismatch(helloRequestId: RequestId, versionsSupportedByPeer: Set[ProtocolVersion]): Unit = {
		replaceMyselfWithAnIncommunicableDelegate(IS_INCOMPATIBLE, s"None of the peer's supported versions according to his hello message are supported by me. Versions supported by peer: $versionsSupportedByPeer")
		participantService.notifyListenersThat(VersionIncompatibilityWith(peerContactAddress))
		transmitToPeer(SupportedVersionsMismatch(helloRequestId)) { report =>
			ifFailureReportItAndThen(DoNothing)(report)
		}
	}
	////

	private[service] def handleChannelDiscarded(): Boolean = {
		if thisCommunicableDelegate eq participantService.delegateByAddress.getOrElse(peerContactAddress, null) then {
			scribe.warn(s"${participantService.myAddress}: `$peerContactAddress` sent me a `$ChannelDiscarded` through channel $channelId and is not being closed.")
		} else {
			scribe.trace(s"${participantService.myAddress}: `$peerContactAddress` sent me a `$ChannelDiscarded` through channel $channelId and is already being closed.")
		}
		false
	}

	private[service] def handleMessage(farewell: Farewell): Boolean = {
		if this.peerCreationInstant == Protocol.UNSPECIFIED_INSTANT || this.peerCreationInstant == farewell.myCreationInstant then {
			for case (address, delegate: CommunicableDelegate) <- participantService.delegateByAddress if address != peerContactAddress do {
				delegate.transmitToPeerOrRestartChannel(AnotherParticipantGone(peerContactAddress, peerCreationInstant))
			}
			false
		} else {
			checkSyncWithPeer(s"`$peerContactAddress` sent me a `$farewell` message with a creation instant that does not match my memory ($peerCreationInstant)")
			true
		}
	}

	private[service] def handleMessage(message: AMemberHasBeenRebooted): Unit = {
		participantService.delegateByAddress.getOrElse(message.rebootedParticipantAddress, null) match {
			case rebootedParticipantDelegate: CommunicableDelegate =>
				if rebootedParticipantDelegate.getPeerCreationInstant == message.restartedParticipantCreationInstant then {
					rebootedParticipantDelegate.checkSyncWithPeer(s"participant at `$peerContactAddress` told me that participant at `${message.rebootedParticipantAddress}` has been rebooted")
				} else {
					scribe.warn(s"I received the message `$message` from `$peerContactAddress` with an unmatching creation instant (expected: ${rebootedParticipantDelegate.getPeerCreationInstant}).")
				}
			case rebootedParticipantDelegate: IncommunicableDelegate =>
				if !rebootedParticipantDelegate.isConnectingAsClient then rebootedParticipantDelegate.tryToConnect()
			case null =>
				participantService.addANewConnectingDelegateAndStartAConnectionToThenAConversationWithParticipant(message.rebootedParticipantAddress)
		}
	}

	private[service] def handleMessage(message: AnotherParticipantGone): Unit = {
		participantService.delegateByAddress.getAndApply(message.goneParticipantAddress) { goneParticipantDelegate =>
			if goneParticipantDelegate.getPeerCreationInstant == message.goneParticipantCreationInstant || goneParticipantDelegate.getPeerCreationInstant == UNSPECIFIED_INSTANT then {
				goneParticipantDelegate.removeByOther()
			} else {
				scribe.error(s"${participantService.myAddress}: I received the message `$message` from `$peerContactAddress` with an unmatching creation instant (expected: ${goneParticipantDelegate.getPeerCreationInstant}).")
			}
		}
	}

	/** Checks if the peer knows my membership-status and incites him to update it otherwise. */
	private[service] def checkSyncWithPeer(why: String): Unit = {
		for peerMembershipStatusAccordingToMe <- oPeerMembershipStatusAccordingToMe do {
			askPeer(new OutgoingRequestExchange[AreYouInSyncWithMe] {
				override def buildRequest(requestId: RequestId): AreYouInSyncWithMe = AreYouInSyncWithMe(requestId, participantService.myMembershipStatus, peerMembershipStatusAccordingToMe)

				override def onResponse(request: AreYouInSyncWithMe, response: AreWeInSyncResponse): Boolean = response match {
					case AreWeInSyncResponse(_, myMembershipStatusAccordingToPeerMatches) =>
						if !response.yourMembershipStatusAccordingToMeMatches then incitePeerToResolveMembershipConflict()
						true
				}

				override def onTransmissionError(request: AreYouInSyncWithMe, error: NotDelivered): Unit =
					restartChannel(why + s" and when trying to ask it `$request` the transmission failed with: $error")

				override def onTimeout(request: AreYouInSyncWithMe): Unit =
					restartChannel(why + s" and when asked `$request` he didn't answer within $responseTimeout millis.")
			})
		}
	}

	private[service] def handleMessage(message: AreYouInSyncWithMe): Unit = {
		if message.yourMembershipStatusAccordingToMe ne participantService.myMembershipStatus then incitePeerToResolveMembershipConflict()
		transmitToPeerOrRestartChannel(AreWeInSyncResponse(message.requestId, oPeerMembershipStatusAccordingToMe.contentEquals(message.myMembershipStatus)))
	}

	/** Incites the peer to update his memory of my membership-status and re-check his memory of the membership-status of other participants that differs from my memory. */
	private[service] def incitePeerToResolveMembershipConflict(): Unit = {
		transmitToPeerOrRestartChannel(WaitMyMembershipStatusIs(participantService.myMembershipStatus, participantService.getStableParticipantsMembershipStatus.toMap))
	}

	private[service] def handleMessage(message: WaitMyMembershipStatusIs): Unit = {
		updateState(message.myMembershipStatus)
		val delegateByAddress = participantService.delegateByAddress
		for (participantAddress, participantMembershipStatusAccordingToPeer) <- message.membershipStatusOfParticipantsIKnow do {
			delegateByAddress.getOrElse(participantAddress, null) match {
				case communicableDelegate: CommunicableDelegate if !communicableDelegate.getPeerMembershipStatusAccordingToMe.contentEquals(participantMembershipStatusAccordingToPeer) =>
					communicableDelegate.checkSyncWithPeer(s"the participant at `$peerContactAddress` told me that the participant at $participantAddress has a different membership status ($participantMembershipStatusAccordingToPeer) than the one I remember (${communicableDelegate.getPeerMembershipStatusAccordingToMe}).")

				case null =>
					if participantAddress == participantService.myAddress then {
						if participantMembershipStatusAccordingToPeer ne participantService.myMembershipStatus then incitePeerToResolveMembershipConflict()
					} else {
						participantService.addANewConnectingDelegateAndStartAConnectionToThenAConversationWithParticipant(participantAddress)
					}

				case _ => // do nothing
			}
		}
	}
	
	// TODO is not called yet
	private[service] def sendPeerAHeartbeat(): Unit = {
		transmitToPeerOrRestartChannel(Heartbeat(config.heartbeatPeriod))
	}
}
