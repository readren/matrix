package readren.matrix
package cluster.service

import cluster.channel.{Receiver, Transmitter}
import cluster.service.ClusterService.{DelegateConfig, VersionIncompatibilityWith}
import cluster.service.Protocol.*

import java.nio.channels.AsynchronousSocketChannel

/** A communicable participant's delegate suited for a [[ClusterService]] with a [[MemberBehavior]]. */
class MemberBehavior(clusterService: ClusterService) extends CommunicableDelegate.Behavior {
	
	override def handleMessage(delegate: CommunicableDelegate, message: Protocol): Boolean = message match {
		case hello: Hello =>
			delegate.handle(hello)
			true

		case SupportedVersionsMismatch =>
			clusterService.notify(VersionIncompatibilityWith(delegate.peerAddress))
			delegate.replaceMyselfWithAnIncommunicableDelegate(false, s"The peer told me we are not compatible.")
			false

		case NoClusterIAmAwareOf(knowAspirantsCards) =>
			clusterService.solveClusterExistenceConflictWith(delegate)
			true

		case ClusterCreatorProposal(proposedCandidate, supportedVersions) =>
			clusterService.solveClusterExistenceConflictWith(delegate)
			true

		case icc: ICreatedACluster =>
			clusterService.solveClusterExistenceConflictWith(delegate)
			true

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
