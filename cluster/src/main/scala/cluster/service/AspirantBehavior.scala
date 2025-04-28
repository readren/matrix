package readren.matrix
package cluster.service

import cluster.channel.Transmitter
import cluster.channel.Transmitter.{Delivered, NotDelivered}
import cluster.misc.CommonExtensions.*
import cluster.service.ContactCard.*
import cluster.service.Protocol.MembershipStatus.{ASPIRANT, MEMBER}
import cluster.service.Protocol.{ContactAddress, Instant, MembershipStatus}
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

	override def openConversationWith(delegate: CommunicableDelegate, isReconnection: Boolean)(onComplete: Transmitter.Report => Unit): Unit = {
		if isReconnection then delegate.notifyPeerThatIHaveReconnected(onComplete)
		else delegate.sendPeerAHello(onComplete)
	}


	override def handleMessageFrom(senderDelegate: CommunicableDelegate, message: Protocol): Boolean = message match {
		case am: ApplicationMsg =>
			// TODO
			true

		case hb: Heartbeat =>
			// TODO
			true

		case sc: ClusterStateChanged =>
			// TODO
			true

		case hello: Hello =>
			senderDelegate.handleMessage(hello)

		case Welcome(membershipStatus, versionsSupportedByPeer, peerCreationInstant, otherParticipantsKnowByPeer) =>
			// override the peer's membership status and supported versions with the values provided by the source of truth. 
			senderDelegate.updateState(membershipStatus, versionsSupportedByPeer, peerCreationInstant)
			// Create a delegate for each participant that I did not know.
			clusterService.createADelegateForEachParticipantIDoNotKnowIn(otherParticipantsKnowByPeer)
			true

		case csw: ConversationStartedWith =>
			// TODO
			true

		case ClusterCreatorProposal(candidateProposedByPeer) =>
			senderDelegate.clusterCreatorProposedByPeer = candidateProposedByPeer
			// If I don't know the candidate, create a delegate for it.
			if candidateProposedByPeer != null && !clusterService.delegateByAddress.contains(candidateProposedByPeer) then {
				clusterService.addANewConnectingDelegateAndStartAConnectionToThenAConversationWithParticipant(candidateProposedByPeer)
			}
			if clusterService.doesAClusterExist then senderDelegate.sendPeerAWelcome()
			else updateClusterCreatorProposalIfAppropriate()
			true

		case icc: ICreatedACluster =>
			senderDelegate.peerMembershipStatusAccordingToMe = MEMBER
			senderDelegate.peerStatePhoto = Maybe.some(icc.myViewpoint)
			sendRequestToJoinTheClusterIfAppropriate()
			true

		case oi: RequestApprovalToJoin =>
			???

		case jag: JoinApprovalGranted =>
			???

		case rtj: RequestToJoin =>
			scribe.warn(s"The aspirant at ${senderDelegate.peerAddress} sent me a ${getTypeName[RequestToJoin]} despite I am not a member of any cluster. ")
			senderDelegate.requestPeerToResolveMembershipConflict()
			true

		case jg: JoinGranted =>
			for (participantAddress, participantInfo) <- jg.participantInfoByItsAddress do {
				if clusterService.delegateByAddress.contains(participantAddress) then {
					
				}
			}
			clusterService.switchToMember(jg.clusterCreationInstant)
			true

		case JoinRejected(haveToRetry, reason) =>
			scribe.info(s"A request to join the cluster was rejected because: $reason")
			if haveToRetry then sendRequestToJoinTheClusterIfAppropriate()
			true

		case rmc: ResolveMembershipConflict =>
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
			
		case YesImAInSyncWithYou =>
			senderDelegate.isPotentiallyOutOfSync = false
			true
			
		case cd: ChannelDiscarded =>
			senderDelegate.handleMessage(cd)

		case phr: AnotherParticipantHasBeenRebooted =>
			senderDelegate.handleMessage(phr)
			true

		case ihr: IHaveReconnected =>
			senderDelegate.updateState(ihr.myMembershipStatus, ihr.versionsISupport, ihr.myCreationInstant)
			true

		case WeHaveToResolveBrainSplit(peerViewPoint) =>
			???
			true

		case WeHaveToResolveBrainJoin(peerViewPoint) =>
			???
			true

		case SupportedVersionsMismatch =>
			senderDelegate.handleMessageSupportedVersionsMismatch()
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
			val candidateProposedByMe = delegateByAddress.valuesIterator
				.filter(_.isCommunicable)
				.foldLeft(clusterService.myContactCard) { (min, delegate) =>
					ContactCard.ordering.min(delegate.contactCard, min)
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

	private def sendRequestToJoinTheClusterIfAppropriate(): Unit = {
		// if a request isn't on the way, a cluster exists, and the communicability of all the delegates is stable, then take the communicable delegate of the member with the lowest [[ContactCard]] and send a request to join to it
		if !aRequestToJoinIsOnTheWay && clusterService.doesAClusterExist && clusterService.delegateByAddress.iterator.forall(_._2.isStable) then {
			clusterService.delegateByAddress.iterator
				.collect { case (_, cd: CommunicableDelegate) if cd.peerMembershipStatusAccordingToMe eq MEMBER => cd }
				.minByOption(_.contactCard)(using ContactCard.ordering)
				.foreach { chosenMember =>
					aRequestToJoinIsOnTheWay = true
					val joinTokenByMember = clusterService.delegateByAddress.iterator
						.collect { case (_, delegate) if delegate.peerMembershipStatusAccordingToMe eq MEMBER => delegate.peerAddress -> 0L } // TODO add token logic or remove them.
						.toMap

					chosenMember.sendPeerARequestToJoin(joinTokenByMember) { report =>
						val tryAgainIfAppropriate: Runnable = { () =>
							aRequestToJoinIsOnTheWay = false
							if clusterService.getMembershipScopedBehavior eq this then sendRequestToJoinTheClusterIfAppropriate()
						}
						report match {
							case Delivered =>
								sequencer.scheduleSequentially(sequencer.newDelaySchedule(clusterService.config.joinCheckDelay))(tryAgainIfAppropriate)
							case nd: NotDelivered =>
								sequencer.executeSequentially {
									chosenMember.reportFailure(nd)
									chosenMember.restartChannel(nd)
									tryAgainIfAppropriate.run()
								}
						}
					}
				}
		}
	}
}
