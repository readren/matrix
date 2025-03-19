package readren.matrix
package cluster.service

import cluster.channel.Receiver
import cluster.service.Protocol.*
import cluster.service.ProtocolVersion

import java.net.SocketAddress

/** A communicable participant's delegate suited for a [[ClusterService]] with an [[AspirantBehavior]]. */
class AspirantCommunicableDelegate(clusterService: ClusterService, clusterServiceBehavior: clusterService.AspirantBehavior, config: ParticipantDelegate.Config, peerRemoteAddress: SocketAddress) extends CommunicableDelegate(peerRemoteAddress, config), AspirantDelegate(clusterService) {

	var clusterCreatorCandidateProposedByPeer: ContactAddress | Null = null


	override final def startReceiving(): Unit = {
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

				case NoClusterIAmAwareOf(aspirantsKnownByPeer) =>
					val iAmConnectedToAllAspirants =
						aspirantsKnownByPeer.foldLeft(true) { (accumulator, aspirantCard) =>
							if aspirantCard.address == clusterService.myAddress then true
							else if clusterServiceBehavior.participantByAddress.contains(aspirantCard.address) then true
							else {
								if determineAgreedVersion(config.versionsSupportedByMe, aspirantCard.supportedVersions).isDefined then {
									clusterService.startConnectionTo(aspirantCard.address)
								} else {
									clusterService.notifyVersionIncompatibilityWith(aspirantCard.address)
								}
								false
							}
						}
					if iAmConnectedToAllAspirants then clusterService.proposeClusterCreator()

				case ClusterCreatorProposal(candidateProposedByPeer) =>
					clusterCreatorCandidateProposedByPeer = candidateProposedByPeer
					if candidateProposedByPeer == clusterService.myAddress then {
						if clusterServiceBehavior.participantByAddress.view.collect {
							case (_, delegate: AspirantCommunicableDelegate) => delegate.clusterCreatorCandidateProposedByPeer == clusterService.myAddress
						}.forall(identity) then {
							clusterService.proposeClusterCreator()
						}
					} else if !clusterServiceBehavior.participantByAddress.contains(candidateProposedByPeer) then {
						clusterService.startConnectionTo(candidateProposedByPeer)
					}
					
				case icc: ICreatedACluster =>

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
