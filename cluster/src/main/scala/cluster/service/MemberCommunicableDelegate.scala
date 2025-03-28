package readren.matrix
package cluster.service

import cluster.channel.{Receiver, Transmitter}
import cluster.service.ClusterService.{DelegateConfig, VersionIncompatibilityWith}
import cluster.service.Protocol.*

import java.nio.channels.AsynchronousSocketChannel

/** A communicable participant's delegate suited for a [[ClusterService]] with a [[MemberBehavior]]. */
class MemberCommunicableDelegate private(
	override val clusterService: ClusterService,
	serviceBehavior: clusterService.MemberBehavior,
	override val peerAddress: ContactAddress,
	override protected val peerChannel: AsynchronousSocketChannel,
	override protected val transmitterToPeer: Transmitter,
	override protected val receiverFromPeer: Receiver
) extends MemberDelegate, Communicable {
	
	def this(clusterService: ClusterService, serviceBehavior: clusterService.MemberBehavior,	peerAddress: ContactAddress, peerChannel: AsynchronousSocketChannel) =
		this(clusterService, serviceBehavior, peerAddress, peerChannel, new Transmitter(peerChannel), new Receiver(peerChannel))
	
	def this(clusterService: ClusterService, clusterServiceBehavior: clusterService.MemberBehavior, aspirant: AspirantCommunicableDelegate) =
		this(clusterService, clusterServiceBehavior, aspirant.peerAddress, aspirant.peerChannel, aspirant.transmitterToPeer, aspirant.receiverFromPeer)

	override val config: DelegateConfig = clusterService.config.participantDelegatesConfig

	override final def continueReceiving(onComplete: Receiver.Fault | Protocol => Unit): Unit = {
		receiverFromPeer.receive[Protocol](agreedVersion, config.receiverTimeout, config.timeUnit) {
			case fault: Receiver.Fault =>
				onComplete(fault)

			case messageFromPeer: Protocol => sequencer.executeSequentially(messageFromPeer match {
				case hello: Hello =>
					handle(hello)
					continueReceiving(onComplete)

				case SupportedVersionsMismatch =>
					clusterService.notify(VersionIncompatibilityWith(peerAddress))
					replaceMyselfWithAnIncommunicableDelegate(false, s"The peer told me we are not compatible.")
					onComplete(messageFromPeer)

				case NoClusterIAmAwareOf(knowAspirantsCards) =>
					clusterService.solveClusterExistenceConflictWith(this)
					continueReceiving(onComplete)

				case ClusterCreatorProposal(proposedCandidate, supportedVersions) =>
					clusterService.solveClusterExistenceConflictWith(this)
					continueReceiving(onComplete)

				case icc: ICreatedACluster =>
					clusterService.solveClusterExistenceConflictWith(this)
					continueReceiving(onComplete)

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
