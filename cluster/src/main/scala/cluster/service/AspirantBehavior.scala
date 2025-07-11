package readren.matrix
package cluster.service

import cluster.channel.Transmitter.{Delivered, NotDelivered}
import cluster.misc.CommonExtensions.*
import cluster.service.ContactCard.*
import cluster.service.Protocol.*
import common.CompileTime.getTypeName

import readren.matrix.cluster.service.Protocol.CommunicationStatus.HANDSHOOK
import readren.taskflow.Maybe

class AspirantBehavior(participantService: ParticipantService) extends MembershipScopedBehavior {
	private val sequencer = participantService.sequencer

	override val membershipStatus: Protocol.MembershipStatus = Aspirant

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

	override def handleInitiatorMessageFrom(senderDelegate: CommunicableDelegate, initiationMsg: InitiationMsg): Boolean = initiationMsg match {
		case hie: HelloIExist =>
			senderDelegate.handleMessage(hie)

		case hib: HelloIAmBack =>
			senderDelegate.handleMessage(hib)

		case csw: ConversationStartedWith =>
			updateClusterCreatorProposalIfAppropriate()
			// TODO
			true

		case ClusterCreatorProposal(candidateProposedByPeer) =>
			senderDelegate.clusterCreatorProposedByPeer = candidateProposedByPeer.getOrElse(null)
			// If I don't know the candidate, create a delegate for it.
			candidateProposedByPeer.foreach { c =>
				if c != participantService.myAddress && !participantService.delegateByAddress.contains(c) then
					participantService.addANewConnectingDelegateAndStartAConnectionToThenAConversationWithParticipant(c)
			}
			if participantService.clustersExistenceArity > 0 then senderDelegate.incitePeerToResolveMembershipConflict()
			else updateClusterCreatorProposalIfAppropriate()
			true

		case icc: ICreatedACluster =>
			senderDelegate.updateState(Member(icc.myViewpoint.clusterCreationInstant))
			senderDelegate.peerStatePhoto = Maybe.some(icc.myViewpoint)
			sendRequestToJoinTheClusterIfAppropriate()
			true

		case oi: RequestApprovalToJoin =>
			???

		case rtj: RequestToJoin =>
			scribe.warn(s"The aspirant at ${senderDelegate.peerContactAddress} sent me a ${getTypeName[RequestToJoin]} despite I am not a member of any cluster. ")
			senderDelegate.incitePeerToResolveMembershipConflict()
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
			
		case _: Response =>
			throw new AssertionError("unreachable")
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
		val config = participantService.config
		val delegateByAddress = participantService.delegateByAddress
		// If all the participants I know are aspirants with stable communicability, then propose the cluster creator.
		if delegateByAddress.forall((_, delegate) => delegate.isStable && delegate.getPeerMembershipStatusAccordingToMe.contentEquals(Aspirant)) then {
			// if I have hand-shaken with all the seeds, then propose the cluster creator.
			if config.seeds.forall { seed =>
				seed == config.myAddress || delegateByAddress.getAndTransformOrElse(seed, false)(_.communicationStatus eq HANDSHOOK)
			} then {

				// Determine the candidate from my viewpoint
				val candidateProposedByMe = delegateByAddress.iterator
					.foldLeft(participantService.myContactCard) { (min, entry) =>
						entry._2 match {
							case cd: CommunicableDelegate => ContactCard.ordering.min(cd.contactCard, min)
							case _ => min
						}
					}

				val myAddress = participantService.myAddress
				// scribe.trace(s"$myAddress: candidateProposedByMe=$candidateProposedByMe, candidateProposedByOthers=${delegateByAddress.collect { case (a, d: CommunicableDelegate) => s"$a proposes ${d.clusterCreatorProposedByPeer} and my previous proposal sent to him was ${d.lastClusterCreatorProposalSentToPeer}" }.mkString(",")}")
				// First check if I should be the candidate. If me, and all the aspirant that I can communicate with, propose me to be the cluster creator...
				if candidateProposedByMe.address == myAddress && delegateByAddress.valuesIterator.forall {
					case communicableDelegate: CommunicableDelegate =>
						communicableDelegate.clusterCreatorProposedByPeer == myAddress
					case _ => true
				} then { // ... then create it.
					// Creating the cluster consists of: changing the behavior of communicable delegates to `MemberBehavior` and sending the `ICreatedACluster` message to the participant I can communicate with.
					val clusterCreationInstant: Instant = participantService.clock.getTime
					val memberBehavior = switchToMember(clusterCreationInstant)
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
		// if a request isn't on the way, a single cluster exists, and the communicability of all the delegates is stable, then send a request to join to the member with the lowest [[ContactCard]]
		if !aRequestToJoinIsOnTheWay && participantService.clustersExistenceArity == 1 && participantService.delegateByAddress.iterator.forall(_._2.isStable) then {
			participantService.delegateByAddress.iterator
				.collect { case (_, cd: CommunicableDelegate) if cd.getPeerMembershipStatusAccordingToMe.contains(_.isInstanceOf[Member]) => cd }
				.minByOption(_.contactCard)(using ContactCard.ordering)
				.foreach { chosenMemberDelegate =>
					aRequestToJoinIsOnTheWay = true

					// send to the chosen member a request to join the cluster
					chosenMemberDelegate.askPeer(new chosenMemberDelegate.OutgoingRequestExchange[RequestToJoin] {
						
						override def buildRequest(requestId: RequestId): RequestToJoin = {
							val joinTokenByMember = participantService.delegateByAddress.collect {
								case (_, delegate) if delegate.getPeerMembershipStatusAccordingToMe == chosenMemberDelegate.getPeerMembershipStatusAccordingToMe =>
									delegate.peerContactAddress -> delegate.getPeerCreationInstant
							}
							RequestToJoin(requestId, joinTokenByMember)
						}

						override def onResponse(request: RequestToJoin, response: request.ResponseType): Boolean = {
							aRequestToJoinIsOnTheWay = false
							participantService.getMembershipScopedBehavior match {
								case ab: AspirantBehavior =>
									response match {
										case jg: JoinGranted =>
											// verify I am in sync with the chosen member about the state of myself and of other participants he knows and, if not, start actions to be so.
											var inSyncWithChosenMember = true
											for (participantAddress, participantInfoAccordingToChosenMember) <- jg.participantInfoByItsAddress do {
												participantService.delegateByAddress.getOrElse(participantAddress, null) match {
													case null =>
														if participantAddress == participantService.myAddress then {
															if participantInfoAccordingToChosenMember.membershipStatus ne participantService.myMembershipStatus then {
																inSyncWithChosenMember = false
																chosenMemberDelegate.checkSyncWithPeer(s"the `$jg` response from the member at ${chosenMemberDelegate.peerContactAddress} does not match my membership state.")
															}
														} else {
															inSyncWithChosenMember = false
															participantService.addANewConnectingDelegateAndStartAConnectionToThenAConversationWithParticipant(participantAddress)
														}
													case participantDelegate: CommunicableDelegate =>
														if !participantDelegate.getPeerMembershipStatusAccordingToMe.contentEquals(participantInfoAccordingToChosenMember.membershipStatus) then {
															inSyncWithChosenMember = false
															participantDelegate.checkSyncWithPeer(s"the `$jg` response from the member at ${chosenMemberDelegate.peerContactAddress} does not match my memory about the membership-status of $participantAddress.")
														}
													case participantDelegate: IncommunicableDelegate =>
														if participantInfoAccordingToChosenMember.communicationStatus eq CommunicationStatus.HANDSHOOK then {
															inSyncWithChosenMember = false
															participantService.connectToAndThenStartConversationWithParticipant(participantDelegate, false)
														}
												}
											}
											
											// Send a response to the chosen member's confirming my decision to join.
											chosenMemberDelegate.transmitToPeer(JoinDecision(jg.requestId, inSyncWithChosenMember)) {
												case Delivered =>
													// If the confirmation was delivered and is affirmative (because we are in sync with the chosen member about the state of all the participants he knows), then switch to member.
													if inSyncWithChosenMember then sequencer.executeSequentially { switchToMember(jg.clusterCreationInstant) }
												case nd: NotDelivered =>
													chosenMemberDelegate.reportTransmissionFailure(nd)
													sequencer.executeSequentially {
														chosenMemberDelegate.restartChannel(nd)
													}
											}
											true

										case jr: JoinRejected =>
											scribe.info(s"${participantService.myAddress}: A request to join the cluster was rejected by ${chosenMemberDelegate.peerContactAddress} because ${jr.reason}.")
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

	/** Switches this [[ParticipantService]] behavior to [[MemberBehavior]]. */
	private def switchToMember(clusterCreationInstant: Instant): MemberBehavior = {
		val memberBehavior = new MemberBehavior(RingSerial.create(), participantService, clusterCreationInstant)
		participantService.membershipScopedBehavior = memberBehavior
		val currentInstant = System.currentTimeMillis()
		for (_, delegate) <- participantService.handshookDelegateByAddress do {
			delegate.transmitToPeerOrRestartChannel(ClusterStateChanged(memberBehavior.currentStateSerial, currentInstant, participantService.myMembershipStatus, participantService.getStableParticipantsInfo.toMap))
		}
		participantService.notifyListenersThat(IJoinedTheCluster(participantService.delegateByAddress))
		memberBehavior
	}


}
