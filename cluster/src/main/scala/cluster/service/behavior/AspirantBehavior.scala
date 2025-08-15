package readren.matrix
package cluster.service.behavior

import cluster.channel.Transmitter.{Delivered, NotDelivered}
import cluster.misc.CommonExtensions.*
import cluster.service.*
import cluster.service.ContactCard.*
import cluster.service.Protocol.*
import cluster.service.Protocol.CommunicationStatus.HANDSHOOK
import cluster.service.behavior.MembershipScopedBehavior

import readren.common.CompileTime.getTypeName
import readren.common.Maybe

class AspirantBehavior(override val host: ParticipantService) extends MembershipScopedBehavior { thisAspirantBehavior =>
	override type MS = Aspirant.type
	override val membershipStatus: MS = Aspirant

	override def onPeerAdded(delegate: ParticipantDelegate): Unit = {
		updateClusterCreatorProposalIfAppropriate()
	}

	override def onPeerCommunicabilityChange(delegate: ParticipantDelegate, previousStatus: CommunicationStatus): Unit = {
		updateClusterCreatorProposalIfAppropriate()
		sendRequestToJoinTheClusterIfAppropriate()
	}

	override def onPeerMembershipChange(delegate: ParticipantDelegate, previousStatus: Maybe[MembershipStatus]): Unit = {
		updateClusterCreatorProposalIfAppropriate()
	}

	override def handleInitiatorMessageFrom(senderDelegate: CommunicableDelegate, initiationMsg: NonResponse): Boolean = initiationMsg match {
		case hello: HelloIExist => senderDelegate.handleHelloCommonLogic(hello)
		// The previous and next match-cases could be merged, but they are kept separate so the compiler notices that it can optimize this whole match construct with a lookup table.
		case hello: HelloIAmBack => senderDelegate.handleHelloCommonLogic(hello)

		case csw: ConversationStartedWith =>
			updateClusterCreatorProposalIfAppropriate()
			// TODO
			true

		case ClusterCreatorProposal(candidateProposedByPeer) =>
			senderDelegate.clusterCreatorProposedByPeer = candidateProposedByPeer.getOrElse(null)
			// If I don't know the candidate, create a delegate for it.
			candidateProposedByPeer.foreach { c =>
				if c != host.myAddress && !host.delegateByAddress.contains(c) then
					host.addANewConnectingDelegateAndStartAConnectionToThenAConversationWithParticipant(c)
			}
			if host.findCluster.nonEmpty then senderDelegate.incitePeerToUpdateHisStateAboutMyStatus()
			else updateClusterCreatorProposalIfAppropriate()
			true

		case icc: ICreatedACluster =>
			senderDelegate.updateState(Functional(icc.myViewpoint.clusterId))
			senderDelegate.updateViewpointPhoto(icc.myViewpoint)
			sendRequestToJoinTheClusterIfAppropriate()
			true

		case oi: RequestApprovalToJoin =>
			???

		case rtj: RequestToJoin =>
			scribe.warn(s"The aspirant at ${senderDelegate.peerContactAddress} sent me a ${getTypeName[RequestToJoin]} despite I am not a member of any cluster. ")
			senderDelegate.incitePeerToUpdateHisStateAboutMyStatus()
			true

		case rmc: WaitMyMembershipStatusIs =>
			senderDelegate.handleMessage(rmc)
			true

		case lcw: ILostCommunicationWith =>
			senderDelegate.checkSyncWithPeer(s"the participant at ${senderDelegate.peerContactAddress} told me that he lost communication with ${lcw.participantsAddress}.")
			true

		case fw: Farewell =>
			senderDelegate.handleMessage(fw)

		case apg: AnotherParticipantGone =>
			senderDelegate.handleMessage(apg)
			true

		case isw: AreYouInSyncWithMe =>
			senderDelegate.handleMessage(isw)
			true

		case ChannelDiscarded =>
			senderDelegate.handleChannelDiscarded()

		case phr: AMemberHasBeenRebooted =>
			senderDelegate.handleMessage(phr)
			true

		case ignored: (AreWeIsolated | WeAreIsolated | WeHaveToResolveClustersConflict) =>
			scribe.info(s"I have received `$ignored` despite my status is $membershipStatus")
			true

		case hb: Heartbeat =>
			// TODO
			true

		case sc: ClusterStateChanged =>
			host.notifyListenersThat(AMemberStateChanged(senderDelegate.peerContactAddress, sc))
			true

		case am: ApplicationMsg =>
			// TODO
			true
	}

	/**
	 * As long as this [[ParticipantService]]'s membership status is [[Aspirant]], this method must be called whenever:
	 *  - a delegate is added to, or removed from, the [[ParticipantService.delegateByAddress]] map.
	 *  - the communicability of a delegates changes, including changes in [[CommunicableDelegate.agreedVersion]] and [[ParticipantDelegate.communicationStatus]].
	 *  - the [[ParticipantDelegate.versionsSupportedByPeer]] changes, because on it depends the [[ContactCard.ordering]].
	 *  - the [[ParticipantDelegate.oPeerMembershipStatusAccordingToMe]] changes.
	 *  - the [[CommunicableDelegate.clusterCreatorProposedByPeer]] changes.
	 * IMPORTANT: This method may change the membership status of this service; therefore, protocol-message handlers that call this method should avoid doing anything after the call that assumes the previous membership status. */
	private def updateClusterCreatorProposalIfAppropriate(): Unit = {
		val config = host.config
		val delegateByAddress = host.delegateByAddress
		// If all the participants I know are aspirants with stable communicability, then propose the cluster creator.
		if delegateByAddress.forall((_, delegate) => delegate.isStable && delegate.getPeerMembershipStatusAccordingToMe.contentEquals(Aspirant)) then {
			// if I have hand-shaken with all the seeds, then propose the cluster creator.
			if config.seeds.forall { seed =>
				seed == config.myAddress || delegateByAddress.getAndTransformOrElse(seed, false)(_.communicationStatus eq HANDSHOOK)
			} then {

				// Determine the candidate from my viewpoint
				val candidateProposedByMe = delegateByAddress.iterator
					.foldLeft(host.myContactCard) { (min, entry) =>
						entry._2 match {
							case cd: CommunicableDelegate => ContactCard.ordering.min(cd.contactCard, min)
							case _ => min
						}
					}

				val myAddress = host.myAddress
				// scribe.trace(s"$myAddress: candidateProposedByMe=$candidateProposedByMe, candidateProposedByOthers=${delegateByAddress.collect { case (a, d: CommunicableDelegate) => s"$a proposes ${d.clusterCreatorProposedByPeer} and my previous proposal sent to him was ${d.lastClusterCreatorProposalSentToPeer}" }.mkString(",")}")
				// First check if I should be the candidate. If me, and all the aspirant that I can communicate with, propose me to be the cluster creator...
				if candidateProposedByMe.address == myAddress && delegateByAddress.valuesIterator.forall {
					case communicableDelegate: CommunicableDelegate =>
						communicableDelegate.clusterCreatorProposedByPeer == myAddress
					case _ => true
				} then { // ... then create it.
					// Creating the cluster consists of: changing the behavior of communicable delegates to `FunctionalBehavior` and sending the `ICreatedACluster` message to the participant I can communicate with.
					val memberBehavior = host.switchToMember(thisAspirantBehavior, generateClusterId(host.clock.getTime))
					val myViewpoint = memberBehavior.myCurrentViewpoint
					for case communicable: CommunicableDelegate <- delegateByAddress.valuesIterator do {
						communicable.transmitToPeer(ICreatedACluster(myViewpoint))(communicable.ifFailureReportItAndThen(communicable.restartChannel))
					}
				} else { // ... else, send my proposal to all the participants I can communicate with.
					for case communicable: CommunicableDelegate <- delegateByAddress.valuesIterator do {
						sendAClusterCreatorProposalTo(communicable, candidateProposedByMe.address)
					}
				}
			}

			// else (if a handshake is not complete) undo the old proposal if any.
			else {
				for case communicable: CommunicableDelegate <- delegateByAddress.valuesIterator do {
					sendAClusterCreatorProposalTo(communicable, null)
				}
			}
		}
	}

	private def sendAClusterCreatorProposalTo(targetDelegate: CommunicableDelegate, proposedAspirantAddress: ContactAddress | Null): Unit = {
		if proposedAspirantAddress != targetDelegate.lastClusterCreatorProposalSentToPeer then {
			targetDelegate.lastClusterCreatorProposalSentToPeer = proposedAspirantAddress
			targetDelegate.transmitToPeerOrRestartChannel(ClusterCreatorProposal(Maybe.apply(proposedAspirantAddress)))
		}
	}


	private var aRequestToJoinIsOnTheWay: Boolean = false

	private def sendRequestToJoinTheClusterIfAppropriate(): Unit = {
		if aRequestToJoinIsOnTheWay then return
		val foundClusters = host.findCluster
		// if a request isn't on the way, a single cluster exists, and the communicability of all the delegates is stable, then send a request to join to the member with the lowest [[ContactCard]]
		if foundClusters.size == 1 && host.delegateByAddress.iterator.forall(_._2.isStable) then {
			val membershipStatus = Functional(foundClusters.head)
			host.delegateByAddress.iterator
				.collect { case (_, cd: CommunicableDelegate) if cd.getPeerMembershipStatusAccordingToMe.contentEquals(membershipStatus) => cd }
				.minByOption(_.contactCard)(using ContactCard.ordering)
				.foreach { chosenMemberDelegate =>
					aRequestToJoinIsOnTheWay = true

					// send to the chosen member a request to join the cluster
					chosenMemberDelegate.askPeer(new chosenMemberDelegate.OutgoingRequestExchange[RequestToJoin] {

						override def buildRequest(requestId: RequestId): RequestToJoin = {
							val joinTokenByMember = host.delegateByAddress.collect {
								case (_, delegate) if delegate.getPeerMembershipStatusAccordingToMe == chosenMemberDelegate.getPeerMembershipStatusAccordingToMe =>
									delegate.peerContactAddress -> delegate.getPeerCreationInstant
							}
							RequestToJoin(requestId, membershipStatus.clusterId, joinTokenByMember)
						}

						override def onResponse(request: RequestToJoin, response: request.ResponseType): Boolean = {
							aRequestToJoinIsOnTheWay = false
							host.getMembershipScopedBehavior match {
								case ab: AspirantBehavior =>
									response match {
										case jg: JoinGranted =>
											// verify I am in sync with the chosen member about the state of myself and of other participants he knows and, if not, start actions to be so.
											var inSyncWithChosenMember = true
											for (participantAddress, participantInfoAccordingToChosenMember) <- jg.participantInfoByItsAddress do {
												host.delegateByAddress.getOrElse(participantAddress, null) match {
													case null =>
														if participantAddress == host.myAddress then {
															if participantInfoAccordingToChosenMember.membershipStatus ne host.myMembershipStatus then {
																inSyncWithChosenMember = false
																chosenMemberDelegate.checkSyncWithPeer(s"the `$jg` response from the member at ${chosenMemberDelegate.peerContactAddress} does not match my membership state.")
															}
														} else {
															inSyncWithChosenMember = false
															host.addANewConnectingDelegateAndStartAConnectionToThenAConversationWithParticipant(participantAddress)
														}
													case participantDelegate: CommunicableDelegate =>
														if !participantDelegate.getPeerMembershipStatusAccordingToMe.contentEquals(participantInfoAccordingToChosenMember.membershipStatus) then {
															inSyncWithChosenMember = false
															participantDelegate.checkSyncWithPeer(s"the `$jg` response from the member at ${chosenMemberDelegate.peerContactAddress} does not match my memory about the membership-status of $participantAddress.")
														}
													case participantDelegate: IncommunicableDelegate =>
														if participantInfoAccordingToChosenMember.communicationStatus eq CommunicationStatus.HANDSHOOK then {
															inSyncWithChosenMember = false
															host.connectToAndThenStartConversationWithParticipant(participantDelegate, false)
														}
												}
											}

											// Send a response to the chosen member's confirming my decision to join.
											chosenMemberDelegate.transmitToPeer(JoinDecision(jg.requestId, inSyncWithChosenMember)) {
												case Delivered =>
													// If the confirmation was delivered and is affirmative (because we are in sync with the chosen member about the state of all the participants he knows), then switch to member.
													if inSyncWithChosenMember then host.sequencer.execute {
														host.switchToMember(thisAspirantBehavior, request.clusterId)
													}
												case nd: NotDelivered =>
													chosenMemberDelegate.reportTransmissionFailure(nd)
													host.sequencer.execute {
														chosenMemberDelegate.restartChannel(nd)
													}
											}
											true

										case jr: JoinRejected =>
											scribe.info(s"${host.myAddress}: A request to join the cluster was rejected by ${chosenMemberDelegate.peerContactAddress} with `$jr`.")
											if jr.youHaveToRetrySoon then ab.sendRequestToJoinTheClusterIfAppropriate()
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
							chosenMemberDelegate.restartChannel(nd)
							sendRequestToJoinTheClusterIfAppropriate()
						}

						override def onTimeout(request: RequestToJoin): Unit = {
							aRequestToJoinIsOnTheWay = false
							chosenMemberDelegate.restartChannel(s"Non-response timeout after sending `$request`.")
							sendRequestToJoinTheClusterIfAppropriate()
						}
					})
				}
		}
	}
}
