package readren.matrix
package cluster.service

import cluster.channel.Receiver
import cluster.service.Protocol.*

import java.net.SocketAddress
import java.nio.channels.AsynchronousSocketChannel

/** A communicable participant's delegate suited for a [[ClusterService]] with a [[MemberBehavior]]. */
class MemberCommunicableDelegate(override val clusterService: ClusterService, clusterServiceBehavior: clusterService.MemberBehavior, config: ParticipantDelegate.Config, peerAddress: SocketAddress, peerChannel: AsynchronousSocketChannel) extends MemberDelegate, Communicable(peerAddress, peerChannel, config)  {

	override protected def startReceiving(): Unit = {
		receiverFromPeer.receive[Protocol](agreedVersion, config.receiverTimeout, config.timeUnit) {
			case fault: Receiver.Fault =>
				val errorMessage = s"Failure while receiving a message from the peer $peerChannel: $fault" 
				scribe.error(errorMessage)
				sequencer.executeSequentially(restartChannel(errorMessage))

			case messageFromPeer: Protocol => sequencer.executeSequentially(messageFromPeer match {
				case hello: Hello => handle(hello)

				case SupportedVersionsMismatch =>
					clusterService.notifyVersionIncompatibilityWith(peerAddress)
					replaceMyselfWithAnIncommunicableDelegate(false, s"The peer told me we are not compatible.")

				case NoClusterIAmAwareOf(knowAspirantsCards) =>
					clusterService.solveClusterExistenceConflictWith(this)

				case ClusterCreatorProposal(proposedCandidate, supportedVersions) =>
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
