package readren.matrix
package cluster.service

import cluster.channel.Transmitter
import cluster.service.ClusterService.VersionIncompatibilityWith
import cluster.service.IncommunicableDelegate.Reason.IS_INCOMPATIBLE
import cluster.service.Protocol.MembershipStatus.{ASPIRANT, MEMBER}

import readren.taskflow.Maybe

class AspirantBehavior(clusterService: ClusterService) extends CommunicableDelegate.Behavior {

	override def membershipStatus: Protocol.MembershipStatus = ASPIRANT

	override def onDelegatedAdded(delegate: ParticipantDelegate): Unit = {
		clusterService.updateClusterCreatorProposalIfAppropriate()
	}

	override def onDelegateCommunicabilityChange(delegate: ParticipantDelegate): Unit = {
		clusterService.updateClusterCreatorProposalIfAppropriate()
		clusterService.sendRequestToJoinTheClusterIfAppropriate()
	}

	override def onDelegateMembershipChange(delegate: ParticipantDelegate): Unit = {
		clusterService.updateClusterCreatorProposalIfAppropriate()
	}

	override def onConnectedAsClient(delegate: CommunicableDelegate, isReconnection: Boolean)(onComplete: Transmitter.Report => Unit): Unit = {
		if isReconnection then delegate.sendIHaveReconnected(onComplete)
		else delegate.sendHello(onComplete)
	}

	override def handleMessage(delegate: CommunicableDelegate, message: Protocol): Boolean = message match {
		case hello: Hello =>
			delegate.handleMessage(hello)
			true

		case ihr: IHaveReconnected =>
			delegate.versionsSupportedByPeer = ihr.versionsISupport
			delegate.peerMembershipStatusAccordingToMe = ihr.myMembershipStatus
			delegate.agreedVersion = delegate.determineAgreedVersion(clusterService.config.versionsISupport, ihr.versionsISupport).getOrElse(ProtocolVersion.NOT_SPECIFIED)
			true
			
		case SupportedVersionsMismatch =>
			delegate.replaceMyselfWithAnIncommunicableDelegate(IS_INCOMPATIBLE, s"The peer told me that we are not compatible.")
			clusterService.notify(VersionIncompatibilityWith(delegate.peerAddress))
			false

		case Welcome(participantsKnownByPeer) =>
			// override the peer's membership status and supported versions with the values provided by the source of truth. 
			participantsKnownByPeer.get(clusterService.myAddress).foreach { peerInfo =>
				delegate.peerMembershipStatusAccordingToMe = peerInfo.membershipStatus
				delegate.versionsSupportedByPeer = peerInfo.supportedVersions
			}
			
			// Create a delegate for each participant that I did not know.
			for (participantAddress, participantInfo) <- participantsKnownByPeer do {
				if participantAddress != clusterService.myAddress && !clusterService.delegateByAddress.contains(participantAddress) then {
					if participantInfo.supportedVersions.isEmpty || delegate.determineAgreedVersion(delegate.config.versionsSupportedByMe, participantInfo.supportedVersions).isDefined then {
						clusterService.addANewDelegateForAndThenConnectToAndThenStartConversationWithParticipant(participantAddress, participantInfo.supportedVersions, participantInfo.membershipStatus)
					} else {
						val newDelegate = clusterService.addANewIncommunicableDelegate(participantAddress, IS_INCOMPATIBLE)
						newDelegate.versionsSupportedByPeer = participantInfo.supportedVersions
						newDelegate.peerMembershipStatusAccordingToMe = ASPIRANT
						clusterService.notify(VersionIncompatibilityWith(participantAddress))
					}
				}
			}
			
			clusterService.sendRequestToJoinTheClusterIfAppropriate()
			true

		case ClusterCreatorProposal(candidateProposedByPeer, versionsSupportedByCandidate) =>
			delegate.clusterCreatorProposedByPeer = candidateProposedByPeer
			// If I don't know the candidate, create a delegate for it.
			if candidateProposedByPeer != null && !clusterService.delegateByAddress.contains(candidateProposedByPeer) then {
				clusterService.addANewDelegateForAndThenConnectToAndThenStartConversationWithParticipant(candidateProposedByPeer, versionsSupportedByCandidate, ASPIRANT)
			}
			clusterService.updateClusterCreatorProposalIfAppropriate()
			true

		case icc: ICreatedACluster =>
			delegate.peerMembershipStatusAccordingToMe = MEMBER
			delegate.peerStatePhoto = Maybe.some(icc.myViewpoint)
			clusterService.sendRequestToJoinTheClusterIfAppropriate()
			true

		case oi: RequestApprovalToJoin =>
			???

		case jag: JoinApprovalGranted =>
			???

		case rtj: RequestToJoin =>
			clusterService.solveClusterExistenceConflictWith(delegate)

		case jg: JoinGranted =>
			clusterService.switchToMember()
			true

		case JoinRejected(haveToRetry, reason) =>
			scribe.info(s"A request to join the cluster was rejected because: $reason")
			if haveToRetry then clusterService.sendRequestToJoinTheClusterIfAppropriate()
			true

		case lcw: ILostCommunicationWith =>
			???

		case IAmLeaving =>
			???

		case hb: Heartbeat =>
			???

		case sc: StateChanged =>
			???

		case phr: ParticipantHasBeenRestarted =>
			???

	}
}
