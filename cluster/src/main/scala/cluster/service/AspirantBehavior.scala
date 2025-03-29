package readren.matrix
package cluster.service

import ContactCard.*
import cluster.service.ClusterService.VersionIncompatibilityWith
import cluster.service.Protocol.MembershipStatus.ASPIRANT

class AspirantBehavior(clusterService: ClusterService) extends CommunicableDelegate.Behavior {
	override def handleMessage(delegate: CommunicableDelegate, message: Protocol): Boolean = message match {
		case hello: Hello =>
			delegate.handle(hello)
			clusterService.proposeClusterCreatorIfAppropriate()
			true

		case SupportedVersionsMismatch =>
			delegate.replaceMyselfWithAnIncommunicableDelegate(false, s"The peer told me that we are not compatible.")
			clusterService.notify(VersionIncompatibilityWith(delegate.peerAddress))
			clusterService.proposeClusterCreatorIfAppropriate()
			false

		case NoClusterIAmAwareOf(aspirantsKnownByPeer) =>
			// Create an aspirant delegate for each aspirant that I did not know.
			for aspirantCard <- aspirantsKnownByPeer do {
				if aspirantCard.address != clusterService.myAddress && !clusterService.delegateByAddress.contains(aspirantCard.address) then {
					if delegate.determineAgreedVersion(delegate.config.versionsSupportedByMe, aspirantCard.supportedVersions).isDefined then {
						clusterService.andAddANewDelegateForAndThenConnectToAndThenStartConversationWithParticipant(aspirantCard.address, aspirantCard.supportedVersions, ASPIRANT)
					} else {
						val newDelegate = clusterService.createAndAddAnIncommunicableDelegate(aspirantCard.address, false)
						newDelegate.versionsSupportedByPeer = aspirantCard.supportedVersions
						newDelegate.peerMembershipStatusAccordingToMe = ASPIRANT
						clusterService.notify(VersionIncompatibilityWith(aspirantCard.address))
					}
				}
			}
			// Propose a cluster creator if apropiate.
			clusterService.proposeClusterCreatorIfAppropriate()
			true

		case ClusterCreatorProposal(candidateProposedByPeer, versionsSupportedByCandidate) =>
			delegate.clusterCreatorProposedByPeer = candidateProposedByPeer
			if candidateProposedByPeer != null && !clusterService.delegateByAddress.contains(candidateProposedByPeer) then {
				clusterService.andAddANewDelegateForAndThenConnectToAndThenStartConversationWithParticipant(candidateProposedByPeer, versionsSupportedByCandidate, ASPIRANT)
			}
			clusterService.proposeClusterCreatorIfAppropriate()
			true

		case icc: ICreatedACluster =>
			???

		case jam: JoinApprovalMembers =>
			???

		case oi: RequestApprovalToJoin =>
			???

		case jag: JoinApprovalGranted =>
			???

		case rtj: RequestToJoin =>
			???

		case jg: JoinGranted =>
			???

		case jr: JoinRejected =>
			???

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
