package readren.matrix
package cluster.service

import cluster.channel.Receiver
import cluster.service.ClusterService.DelegateConfig
import cluster.service.Protocol.*
import cluster.service.ProtocolVersion

import java.net.SocketAddress
import java.nio.channels.AsynchronousSocketChannel

/** A communicable participant's delegate suited for a [[ClusterService]] with an [[AspirantBehavior]]. */
class AspirantCommunicableDelegate(
	override val clusterService: ClusterService,
	clusterServiceBehavior: clusterService.AspirantBehavior,
	override val peerAddress: SocketAddress,
	override val peerChannel: AsynchronousSocketChannel,
) extends AspirantDelegate, Communicable {
	override val config: DelegateConfig = clusterService.config.participantDelegatesConfig

	protected var clusterCreatorCandidateProposedByPeer: ContactAddress | Null = null

	override final def startReceiving(): Unit = {
		receiverFromPeer.receive[Protocol](agreedVersion, config.receiverTimeout, config.timeUnit) {
			case fault: Receiver.Fault =>
				val errorMessage = s"Failure while receiving a message from the peer $peerChannel: $fault"
				scribe.error(errorMessage)
				sequencer.executeSequentially(restartChannel(errorMessage))

			case messageFromPeer: Protocol => sequencer.executeSequentially(messageFromPeer match {
				case hello: Hello => handle(hello)

				case SupportedVersionsMismatch =>
					replaceMyselfWithAnIncommunicableDelegate(false, s"The peer told me that we are not compatible.")
					clusterService.notifyVersionIncompatibilityWith(peerAddress)

				case NoClusterIAmAwareOf(aspirantsKnownByPeer) =>
					val iAmConnectedToAllAspirants =
						aspirantsKnownByPeer.foldLeft(true) { (accumulator, aspirantCard) =>
							if aspirantCard.address == clusterService.myAddress then true
							else if clusterService.participantByAddress.contains(aspirantCard.address) then true
							else {
								if determineAgreedVersion(config.versionsSupportedByMe, aspirantCard.supportedVersions).isDefined then {
									clusterService.createNewDelegateForAndStartConnectingTo(aspirantCard.address, aspirantCard.supportedVersions)
								} else {
									clusterService.notifyVersionIncompatibilityWith(aspirantCard.address)
								}
								false
							}
						}
					if iAmConnectedToAllAspirants then clusterService.proposeClusterCreator()

				case ClusterCreatorProposal(candidateProposedByPeer, versionsSupportedByCandidate) =>
					clusterCreatorCandidateProposedByPeer = candidateProposedByPeer
					if candidateProposedByPeer == clusterService.myAddress then {
						if clusterService.participantByAddress.view.collect {
							case (_, delegate: AspirantCommunicableDelegate) => delegate.clusterCreatorCandidateProposedByPeer == clusterService.myAddress
						}.forall(identity) then {
							clusterService.proposeClusterCreator()
						}
					} else if !clusterService.participantByAddress.contains(candidateProposedByPeer) then {
						clusterService.createNewDelegateForAndStartConnectingTo(candidateProposedByPeer, versionsSupportedByCandidate)
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
