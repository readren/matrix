package readren.matrix
package cluster.service

import cluster.service.ClusterService.VersionIncompatibilityWith
import cluster.service.IncommunicableDelegate.Reason.IS_INCOMPATIBLE
import cluster.service.Protocol.*

import readren.matrix.cluster.channel.Transmitter
import readren.matrix.cluster.service.Protocol.MembershipStatus.MEMBER

/** A communicable participant's delegate suited for a [[ClusterService]] with a [[MemberBehavior]]. */
class MemberBehavior(clusterService: ClusterService) extends CommunicableDelegate.Behavior {

	override def membershipStatus: MembershipStatus = MEMBER

	override def onDelegatedAdded(delegate: ParticipantDelegate): Unit = ???

	override def onDelegateCommunicabilityChange(delegate: ParticipantDelegate): Unit = ???

	override def onDelegateMembershipChange(delegate: ParticipantDelegate): Unit = ???

	override def onConnectedAsClient(delegate: CommunicableDelegate, isReconnection: Boolean)(onComplete: Transmitter.Report => Unit): Unit = {
		???
	}

	override def handleMessage(delegate: CommunicableDelegate, message: Protocol): Boolean = message match {
		case hello: Hello =>
			delegate.handleMessage(hello)
			true

		case SupportedVersionsMismatch =>
			clusterService.notify(VersionIncompatibilityWith(delegate.peerAddress))
			delegate.replaceMyselfWithAnIncommunicableDelegate(IS_INCOMPATIBLE, s"The peer told me we are not compatible.")
			false

		case ClusterCreatorProposal(proposedCandidate, supportedVersions) =>
			clusterService.solveClusterExistenceConflictWith(delegate)

		case icc: ICreatedACluster =>
			clusterService.solveClusterExistenceConflictWith(delegate)

		case jam: Welcome =>
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
