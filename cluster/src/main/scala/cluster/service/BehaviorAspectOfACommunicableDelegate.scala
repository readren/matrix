package readren.matrix
package cluster.service

import cluster.channel.Transmitter.NotDelivered
import cluster.misc.DoNothing
import cluster.serialization.ProtocolVersion
import cluster.service.Protocol.IncommunicabilityReason.IS_INCOMPATIBLE
import cluster.service.Protocol.{ASPIRANT, MembershipStatus, RequestId}
import common.CompileTime.getTypeName

/** Defines participant behavior aspects for a [[CommunicableDelegate]].
 *
 * This trait contains methods and functionality related to the [[ClusterService]]'s role as a participant, primarily serving as helper methods for [[MembershipScopedBehavior]] subclasses.
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
				HelloIExist(requestId, clusterService.config.versionsISupport, clusterService.myCreationInstant, clusterService.myMembershipStatus, clusterService.getKnownParticipantsAddresses)

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

	private[service] def handleMessage(message: HelloIExist): Boolean = {
		val previousMembershipStatus = oPeerMembershipStatusAccordingToMe

		// Connect to participants I didn't know.
		clusterService.createADelegateForEachParticipantIDoNotKnowIn(message.otherParticipantsIKnow)

		// update my viewpoint of the peer's membership.
		updateState(message.myMembershipStatus, message.versionsISupport, message.myCreationInstant)

		// If the HelloIExist message comes from a participant that, according to my memory, isn't an aspirant; surely it was rebooted, so inform that.
		if (message.myMembershipStatus eq ASPIRANT) && !previousMembershipStatus.contentEquals(ASPIRANT) then {
			clusterService.notifyListenersThat(MemberHasBeenRebooted(peerAddress))
			for case (contactAddress, delegate: CommunicableDelegate) <- clusterService.delegateByAddress do {
				if contactAddress != peerAddress then delegate.transmitToPeer(AMemberHasBeenRebooted(peerAddress, message.myCreationInstant))(ifFailureReportItAndThen(restartChannel))
			}
		}

		// transmit the response: `Welcome` or `SupportedVersionsMismatch` accordingly.
		if getAgreedVersion == ProtocolVersion.NOT_SPECIFIED then {
			notifyAboutTheSupportedVersionsMismatch(message.requestId, message.versionsISupport)
			false
		} else {
			sendPeerAWelcome(message.requestId)
			true
		}
	}

	private[service] def sendPeerAHelloIAmBack(): Unit = {
		askPeer(new SingleRetryOutgoingRequestExchange[HelloIAmBack] {
			override def buildRequest(requestId: RequestId): HelloIAmBack =
				HelloIAmBack(requestId, clusterService.config.versionsISupport, clusterService.myCreationInstant, clusterService.myMembershipStatus)

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

	private[service] def handleMessage(message: HelloIAmBack): Boolean = {
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
		transmitToPeer(Welcome(requestId, clusterService.myMembershipStatus, clusterService.config.versionsISupport, clusterService.myCreationInstant, clusterService.getKnownParticipantsAddresses))(ifFailureReportItAndThen(restartChannel))
	}

	/** @param isARestart `false` when triggered by a [[HelloIExist]]; `true` when triggered by a [[HelloIAmBack]] */
	private def handleWelcome(welcome: Welcome, isARestart: Boolean): Unit = {
		// override the peer's membership status and supported versions with the values provided by the source of truth.
		updateState(welcome.myMembershipStatus, welcome.versionsISupport, welcome.myCreationInstant)
		assert(getAgreedVersion != ProtocolVersion.NOT_SPECIFIED)
		// Create a delegate for each participant that I did not know.
		clusterService.createADelegateForEachParticipantIDoNotKnowIn(welcome.otherParticipants)
		// notify
		clusterService.onConversationStarted(peerAddress, isARestart)
	}

	private def handleSupportedVersionsMismatch(svm: SupportedVersionsMismatch): Unit = {
		clusterService.notifyListenersThat(VersionIncompatibilityWith(peerAddress))
		replaceMyselfWithAnIncommunicableDelegate(IS_INCOMPATIBLE, s"The peer told me we are not compatible.")
	}

	private def notifyAboutTheSupportedVersionsMismatch(helloRequestId: RequestId, versionsSupportedByPeer: Set[ProtocolVersion]): Unit = {
		transmitToPeer(SupportedVersionsMismatch(helloRequestId)) { report =>
			sequencer.executeSequentially {
				ifFailureReportItAndThen(DoNothing)(report)
				replaceMyselfWithAnIncommunicableDelegate(IS_INCOMPATIBLE, s"None of the peer's supported versions according to his hello message are supported by me. Versions supported by peer: $versionsSupportedByPeer")
				clusterService.notifyListenersThat(VersionIncompatibilityWith(peerAddress))
			}
		}
	}

	////

	private[service] def handleMessage(message: ChannelDiscarded): false = {
		scribe.error(s"The participant at $peerAddress sent me a ${getTypeName[ChannelDiscarded]} message through a channel that I already started to use.")
		restartChannel("Channel unexpectedly discarded")
		false
	}

	private[service] def handleMessage(farewell: Farewell): Boolean = {
		if this.peerCreationInstant == Protocol.UNSPECIFIED_INSTANT || this.peerCreationInstant == farewell.myCreationInstant then {
			if clusterService.removeDelegate(this, true) then {
				for case (address, delegate: CommunicableDelegate) <- clusterService.delegateByAddress do {
					transmitToPeer(AnotherParticipantGone(peerAddress, peerCreationInstant))(ifFailureReportItAndThen(restartChannel))
				}
			}
			startPeerChannelClosing()
			false
		} else {
			scribe.warn(s"A ${getTypeName[Farewell]} from $peerCreationInstant was ignored because the creation instant does not match.")
			true
		}
	}

	private[service] def handleMessage(message: AMemberHasBeenRebooted): Unit = {
		clusterService.delegateByAddress.getOrElse(message.rebootedParticipantAddress, null) match {
			case rebootedParticipantDelegate: CommunicableDelegate =>
				if rebootedParticipantDelegate.peerCreationInstant == message.restartedParticipantCreationInstant then {
					rebootedParticipantDelegate.checkSyncWithPeer(s"participant at $peerAddress told me that participant at ${message.rebootedParticipantAddress} has been rebooted")
				} else {
					scribe.warn(s"I received the message `$message` from $peerAddress with an unmatching creation instant (expected: ${rebootedParticipantDelegate.peerCreationInstant}).")
				}
			case rebootedParticipantDelegate: IncommunicableDelegate =>
				if !rebootedParticipantDelegate.isConnectingAsClient then rebootedParticipantDelegate.tryToConnect()
			case null =>
				clusterService.addANewConnectingDelegateAndStartAConnectionToThenAConversationWithParticipant(message.rebootedParticipantAddress)
		}
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

	protected def removeMyselfIfNoAnswerFromPeer(): Unit = {
		for peerMembershipStatusAccordingToMe <- oPeerMembershipStatusAccordingToMe do {
			askPeer(new OutgoingRequestExchange[AreYouInSyncWithMe] {
				override def buildRequest(requestId: RequestId): AreYouInSyncWithMe =
					AreYouInSyncWithMe(requestId, clusterService.myMembershipStatus, peerMembershipStatusAccordingToMe)

				override def onResponse(request: AreYouInSyncWithMe, response: request.ResponseType): Boolean = response match {
					case AreWeInSyncResponse(_, myMembershipStatusAccordingToPeerMatches) =>
						if !myMembershipStatusAccordingToPeerMatches then incitePeerToResolveMembershipConflict()
						true
				}

				override def onTransmissionError(request: AreYouInSyncWithMe, error: NotDelivered): Unit =
					clusterService.removeDelegate(thisCommunicableDelegate, true)

				override def onTimeout(session: Session): Unit =
					clusterService.removeDelegate(thisCommunicableDelegate, true)
			})
		}
	}

	/** Checks if the peer knows my membership-status and incites him to update it otherwise. */
	private[service] def checkSyncWithPeer(why: String): Unit = {
		for peerMembershipStatusAccordingToMe <- oPeerMembershipStatusAccordingToMe do {
			askPeer(new OutgoingRequestExchange[AreYouInSyncWithMe] {
				override def buildRequest(requestId: RequestId): AreYouInSyncWithMe = AreYouInSyncWithMe(requestId, clusterService.myMembershipStatus, peerMembershipStatusAccordingToMe)

				override def onResponse(request: AreYouInSyncWithMe, response: request.ResponseType): Boolean = response match {
					case AreWeInSyncResponse(_, myMembershipStatusAccordingToPeerMatches) =>
						if !myMembershipStatusAccordingToPeerMatches then incitePeerToResolveMembershipConflict()
						true
				}

				override def onTransmissionError(request: AreYouInSyncWithMe, error: NotDelivered): Unit =
					restartChannel(why + s" and when trying to ask it `$request` the transmission failed with: $error")

				override def onTimeout(session: Session): Unit =
					restartChannel(why + s" and when asked `${session.request}` he didn't answer within $responseTimeout millis.")
			})
		}
	}

	private[service] def handleMessage(message: AreYouInSyncWithMe): Unit = {
		if message.yourMembershipStatusAccordingToMe ne clusterService.myMembershipStatus then incitePeerToResolveMembershipConflict()
		transmitToPeer(AreWeInSyncResponse(message.requestId, oPeerMembershipStatusAccordingToMe.contentEquals(message.myMembershipStatus)))(ifFailureReportItAndThen(restartChannel))
	}

	/** Incites the peer to update his memory of my membership-status and re-check his memory of the membership-status of other participants that differs from my memory. */
	private[service] def incitePeerToResolveMembershipConflict(): Unit = {
		transmitToPeer(WaitMyMembershipStatusIs(clusterService.myMembershipStatus, clusterService.getStableParticipantsMembershipStatus.toMap))(ifFailureReportItAndThen(restartChannel))
	}

	private[service] def handleMessage(message: WaitMyMembershipStatusIs): Unit = {
		updateState(message.myMembershipStatus)
		val delegateByAddress = clusterService.delegateByAddress
		for (participantAddress, participantMembershipStatusAccordingToPeer) <- message.membershipStatusOfParticipantsIKnow do {
			delegateByAddress.getOrElse(participantAddress, null) match {
				case communicableDelegate: CommunicableDelegate if !communicableDelegate.oPeerMembershipStatusAccordingToMe.contentEquals(participantMembershipStatusAccordingToPeer) =>
					communicableDelegate.checkSyncWithPeer(s"the participant at $peerAddress told me that the participant at $participantAddress has a different membership status ($participantMembershipStatusAccordingToPeer) than the one I remember (${communicableDelegate.oPeerMembershipStatusAccordingToMe}).")

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

	// TODO is not called yet
	private[service] def sendPeerAHeartbeat(): Unit = {
		transmitToPeer(Heartbeat(config.heartbeatPeriod))(ifFailureReportItAndThen(restartChannel))
	}
}
