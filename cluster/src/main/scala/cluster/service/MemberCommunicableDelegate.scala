package readren.matrix
package cluster.service

import cluster.channel.Receiver
import cluster.service.Protocol.*

import java.net.SocketAddress

/** A communicable participant's delegate suited for a [[ClusterService]] with a [[MemberBehavior]]. */
class MemberCommunicableDelegate(clusterService: ClusterService, clusterServiceBehavior: clusterService.MemberBehavior, config: ParticipantDelegate.Config, peerRemoteAddress: SocketAddress) extends CommunicableDelegate(peerRemoteAddress, config), MemberDelegate(clusterService) {

	protected def startReceiving(): Unit = {
		receiverFromPeer.receive[Protocol](agreedVersion, config.receiverTimeout, config.timeUnit) {
			case fault: Receiver.Fault =>
				scribe.error(s"Failure while receiving a message from the peer $peerChannel: $fault")
				peerChannel.close()
				sequencer.executeSequentially(clusterService.onParticipantChannelClosed(peerRemoteAddress, fault))

			case messageFromPeer: Protocol => sequencer.executeSequentially(messageFromPeer match {
				case hello: Hello => handle(hello)

				case SupportedVersionsMismatch =>
					clusterService.notifyVersionIncompatibilityWith(peerRemoteAddress)
					closeChannel()

				case NoClusterIAmAwareOf(knowAspirantsCards) =>
					clusterService.solveClusterExistenceConflictWith(this)

				case ClusterCreatorProposal(knownAspirantsCards) =>
					clusterService.solveClusterExistenceConflictWith(this)

				case icc: ICreatedACluster =>
					clusterService.solveClusterExistenceConflictWith(this)

				case jam: JoinApprovalMembers =>

				case oi: RequestApprovalToJoin =>

				case jag: JoinApprovalGranted =>

				case rtj: RequestToJoin =>

				case jg: JoinGranted =>

				case jr: JoinRejected =>

				case lcw: ILostCommunicationWith =>

				case IAmLeaving =>

				case hb: Heartbeat =>

				case sc: StateChanged =>
					
				case phr: ParticipantHasBeenRestarted =>

			})
		}
	}
}
