package readren.matrix
package cluster.service

import cluster.channel.Transmitter.NotDelivered
import cluster.misc.CommonExtensions.*
import cluster.serialization.ProtocolVersion
import cluster.service.ContactCard.*
import cluster.service.Protocol.MembershipStatus.{ASPIRANT, MEMBER}
import cluster.service.Protocol.{CommunicationStatus, Instant, RequestId}
import common.CompileTime.getTypeName

import readren.taskflow.Maybe

class AspirantBehavior(clusterService: ClusterService) extends MembershipScopedBehavior {
	private val sequencer = clusterService.sequencer

	override val membershipStatus: Protocol.MembershipStatus = ASPIRANT

	override def onDelegatedAdded(delegate: ParticipantDelegate): Unit = {
		updateClusterCreatorProposalIfAppropriate()
	}

	override def onDelegateCommunicabilityChange(delegate: ParticipantDelegate): Unit = {
		updateClusterCreatorProposalIfAppropriate()
		sendRequestToJoinTheClusterIfAppropriate()
	}

	override def onDelegateMembershipChange(delegate: ParticipantDelegate): Unit = {
		updateClusterCreatorProposalIfAppropriate()
	}

	override def openConversationWith(delegate: CommunicableDelegate, isReconnection: Boolean): Unit = {
		if isReconnection then delegate.sendPeerAHelloIAmBack()
		else delegate.sendPeerAHello()
	}


	override def handleInitiatorMessageFrom(senderDelegate: CommunicableDelegate, initiationMsg: InitiationMsg): Boolean = initiationMsg match {
		case hie: HelloIExist =>
			senderDelegate.handleMessage(hie)

		case csw: ConversationStartedWith =>
			// TODO
			true

		case ClusterCreatorProposal(candidateProposedByPeer) =>
			senderDelegate.clusterCreatorProposedByPeer = candidateProposedByPeer
			// If I don't know the candidate, create a delegate for it.
			if candidateProposedByPeer != null && !clusterService.delegateByAddress.contains(candidateProposedByPeer) then {
				clusterService.addANewConnectingDelegateAndStartAConnectionToThenAConversationWithParticipant(candidateProposedByPeer)
			}
			if clusterService.doesAClusterExist then senderDelegate.incitePeerToResolveMembershipConflict()
			else updateClusterCreatorProposalIfAppropriate()
			true

		case icc: ICreatedACluster =>
			senderDelegate.peerMembershipStatusAccordingToMe = MEMBER
			senderDelegate.peerStatePhoto = Maybe.some(icc.myViewpoint)
			sendRequestToJoinTheClusterIfAppropriate()
			true

		case oi: RequestApprovalToJoin =>
			???

		case rtj: RequestToJoin =>
			scribe.warn(s"The aspirant at ${senderDelegate.peerAddress} sent me a ${getTypeName[RequestToJoin]} despite I am not a member of any cluster. ")
			senderDelegate.incitePeerToResolveMembershipConflict()
			true

		case rmc: WaitMyMembershipStatusIs =>
			senderDelegate.handleMessage(rmc)
			true

		case lcw: ILostCommunicationWith =>
			scribe.info(s"The participant at ${senderDelegate.peerAddress} told me that he lost communication with ${lcw.participantsAddress}.")
			// TODO
			true

		case fw: Farewell =>
			senderDelegate.handleMessage(fw)

		case apg: AnotherParticipantGone =>
			senderDelegate.handleMessage(apg)
			true

		case isw: AreYouInSyncWithMe =>
			senderDelegate.handleMessage(isw)
			true

		case cd: ChannelDiscarded =>
			senderDelegate.handleMessage(cd)

		case phr: AnotherParticipantHasBeenRebooted =>
			senderDelegate.handleMessage(phr)
			true

		case hib: HelloIAmBack =>
			senderDelegate.updateState(hib.myMembershipStatus, hib.versionsISupport, hib.myCreationInstant)
			if senderDelegate.agreedVersion == ProtocolVersion.NOT_SPECIFIED then {
				senderDelegate.handleHelloVersionMismatch(hib.requestId, hib.versionsISupport)
				false
			}
			else {
				senderDelegate.sendPeerAWelcome(hib.requestId)
				true
			}

		case WeHaveToResolveBrainSplit(peerViewPoint) =>
			???
			true

		case WeHaveToResolveBrainJoin(peerViewPoint) =>
			???
			true

		case hb: Heartbeat =>
			// TODO
			true

		case sc: ClusterStateChanged =>
			// TODO
			true

		case am: ApplicationMsg =>
			// TODO
			true
	}

	/**
	 * As long as this [[ClusterService]]'s membership status is [[ASPIRANT]], this method must be called whenever:
	 *  - a delegate is added to, or removed from, the [[ClusterService.delegateByAddress]] map.
	 *  - the communicability of a delegates changes, including changes in [[CommunicableDelegate.agreedVersion]] and [[ParticipantDelegate.communicationStatus]].
	 *  - the [[ParticipantDelegate.versionsSupportedByPeer]] changes, because on it depends the [[ContactCard.ordering]].
	 *  - the [[ParticipantDelegate.peerMembershipStatusAccordingToMe]] changes.
	 *  - the [[CommunicableDelegate.clusterCreatorProposedByPeer]] changes.
	 * IMPORTANT: This method may change the membership status of this service; therefore, protocol-message handlers that call this method should avoid doing anything after the call that assumes the previous membership status. */
	private def updateClusterCreatorProposalIfAppropriate(): Unit = {
		val config = clusterService.config
		val delegateByAddress = clusterService.delegateByAddress
		val iCanCommunicateToAllSeeds = config.seeds.forall { seed =>
			seed == config.myAddress || delegateByAddress.getAndTransformOrElse(seed, false)(_.isCommunicable)
		}
		// if I can communicate with all the seeds and all delegates are aspirants with stable communicability, then propose the cluster creator.
		if iCanCommunicateToAllSeeds && delegateByAddress.valuesIterator.forall(delegate => delegate.isStable && (delegate.peerMembershipStatusAccordingToMe eq ASPIRANT)) then {

			// Determine the candidate from my viewpoint
			val candidateProposedByMe = delegateByAddress.iterator
				.foldLeft(clusterService.myContactCard) { (min, entry) =>
					entry._2 match {
						case cd: CommunicableDelegate => ContactCard.ordering.min(cd.contactCard, min)
						case _ => min
					}
				}

			val myAddress = clusterService.myAddress
			// First check if I should be the candidate. If me, and all the aspirant that I can communicate with, propose me to be the cluster creator...
			if candidateProposedByMe == myAddress && delegateByAddress.valuesIterator.forall {
				case communicableDelegate: CommunicableDelegate =>
					communicableDelegate.clusterCreatorProposedByPeer == myAddress
				case _ => true
			} then { // ... then create it.
				// Creating the cluster consists of: changing the behavior of communicable delegates to `MemberBehavior` and sending the `ICreatedACluster` message to the participant I can communicate with.
				val clusterCreationInstant: Instant = clusterService.clock.getTime
				val memberBehavior = clusterService.switchToMember(clusterCreationInstant)
				val myViewpoint = memberBehavior.myCurrentViewpoint
				for case communicable: CommunicableDelegate <- delegateByAddress.valuesIterator do {
					communicable.transmitToPeer(ICreatedACluster(myViewpoint))(communicable.ifFailureReportItAndThen(communicable.restartChannel))
				}
			} else { // ... else, send my proposal to all the participants I can communicate with.
				for case communicable: CommunicableDelegate <- delegateByAddress.valuesIterator do {
					communicable.sendPeerAClusterCreatorProposal(candidateProposedByMe.address)
				}
			}
		}
		// if the conditions for the proposal are not meet, undo the old proposal if any.
		else {
			for case communicable: CommunicableDelegate <- delegateByAddress.valuesIterator do {
				communicable.sendPeerAClusterCreatorProposal(null)
			}
		}
	}

	private var aRequestToJoinIsOnTheWay: Boolean = false

	private[service] def sendRequestToJoinTheClusterIfAppropriate(): Unit = {
		// if a request isn't on the way, a cluster exists, and the communicability of all the delegates is stable, then send a request to join to the member with the lowest [[ContactCard]]
		if !aRequestToJoinIsOnTheWay && clusterService.doesAClusterExist && clusterService.delegateByAddress.iterator.forall(_._2.isStable) then {
			clusterService.delegateByAddress.iterator
				.collect { case (_, cd: CommunicableDelegate) if cd.peerMembershipStatusAccordingToMe eq MEMBER => cd }
				.minByOption(_.contactCard)(using ContactCard.ordering)
				.foreach { chosenMemberDelegate =>
					aRequestToJoinIsOnTheWay = true
					val joinTokenByMember = clusterService.delegateByAddress.iterator
						.collect { case (_, delegate) if delegate.peerMembershipStatusAccordingToMe eq MEMBER => delegate.peerAddress -> 0L } // TODO add token logic or remove them.
						.toMap

					// send to the chosen member a request to join the cluster
					chosenMemberDelegate.askPeer(new chosenMemberDelegate.OutgoingRequestExchange[RequestToJoin] {
						
						override def buildRequest(requestId: RequestId): RequestToJoin = RequestToJoin(requestId, joinTokenByMember)

						override def onResponse(request: RequestToJoin, response: request.ResponseType): Boolean = {
							aRequestToJoinIsOnTheWay = false
							clusterService.getMembershipScopedBehavior match {
								case ab: AspirantBehavior =>
									response match {
										case jg: JoinGranted =>
											// verify we are in sync and, if not, start actions to be so.
											var weAreInSync = true
											for (participantAddress, participantInfoAccordingToChosenMember) <- jg.participantInfoByItsAddress do {
												clusterService.delegateByAddress.getOrElse(participantAddress, null) match {
													case null =>
														if participantAddress == clusterService.myAddress then {
															if participantInfoAccordingToChosenMember.membershipStatus ne clusterService.myMembershipStatus then {
																weAreInSync = false
																chosenMemberDelegate.checkSyncWithPeer(s"the `$jg` response from the member at ${chosenMemberDelegate.peerAddress} does not match my membership state.")
															}
														} else {
															weAreInSync = false
															clusterService.addANewConnectingDelegateAndStartAConnectionToThenAConversationWithParticipant(participantAddress)
														}
													case participantDelegate: CommunicableDelegate =>
														if participantInfoAccordingToChosenMember.membershipStatus ne participantDelegate.peerMembershipStatusAccordingToMe then {
															weAreInSync = false
															participantDelegate.checkSyncWithPeer(s"the `$jg` response from the member at ${chosenMemberDelegate.peerAddress} does not match my memory about the membership-status of the participant at $participantAddress.")
														}
													case participantDelegate: IncommunicableDelegate =>
														if participantInfoAccordingToChosenMember.communicationStatus eq CommunicationStatus.HANDSHOOK then {
															weAreInSync = false
															clusterService.connectToAndThenStartConversationWithParticipant(participantDelegate, false)
														}
												}
											}
											if weAreInSync then clusterService.switchToMember(jg.clusterCreationInstant)
											true

										case jr: JoinRejected =>
											scribe.info(s"A request to join the cluster was rejected because: ${jr.reason}")
											if jr.youHaveToRetry then ab.sendRequestToJoinTheClusterIfAppropriate()
											true
									}
								case _ =>
									scribe.info(s"The response $response to a ${getTypeName[RequestToJoin]} was received but disregarded as I am not currently an aspirant.")
									true
							}
						}

						override def onTransmissionError(request: RequestToJoin, nd: NotDelivered): Unit = {
							aRequestToJoinIsOnTheWay = false
							chosenMemberDelegate.reportTransmissionFailure(nd)
							chosenMemberDelegate.restartChannel(s"Transmission failure while trying to send `$request` to participant at ${chosenMemberDelegate.peerAddress}: $nd")
							sendRequestToJoinTheClusterIfAppropriate()
						}

						override def onTimeout(session: Session): Unit = {
							aRequestToJoinIsOnTheWay = false
							chosenMemberDelegate.restartChannel(s"Non-response timeout after sending `${session.request}` to participant at ${chosenMemberDelegate.peerAddress}.")
							sendRequestToJoinTheClusterIfAppropriate()
						}
					})
				}
		}
	}
}
