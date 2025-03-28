package readren.matrix
package cluster.service

import cluster.channel.{Receiver, Transmitter}
import cluster.service.ClusterService.{DelegateConfig, VersionIncompatibilityWith}
import cluster.service.Protocol.*
import cluster.service.Protocol.MembershipStatus.ASPIRANT
import cluster.service.ProtocolVersion

import ContactCard.*

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
	override val transmitterToPeer: Transmitter = new Transmitter(peerChannel)
	override val receiverFromPeer: Receiver = new Receiver(peerChannel)

	private[service] var clusterCreatorCandidateProposedByPeer: ContactAddress | Null = null

	override final def continueReceiving(onComplete: Receiver.Fault | Protocol => Unit): Unit = {
		receiverFromPeer.receive[Protocol](agreedVersion, config.receiverTimeout, config.timeUnit) {
			case fault: Receiver.Fault =>
				onComplete(fault)

			case messageFromPeer: Protocol => sequencer.executeSequentially(messageFromPeer match {
				case hello: Hello =>
					handle(hello)
					continueReceiving(onComplete)

				case SupportedVersionsMismatch =>
					replaceMyselfWithAnIncommunicableDelegate(false, s"The peer told me that we are not compatible.")
					clusterService.notify(VersionIncompatibilityWith(peerAddress))
					onComplete(messageFromPeer)

				case NoClusterIAmAwareOf(aspirantsKnownByPeer) =>
					// Create an aspirant delegate for each aspirant that I did not know.
					for aspirantCard <- aspirantsKnownByPeer do {
						if aspirantCard.address != clusterService.myAddress && !clusterServiceBehavior.delegateByAddress.contains(aspirantCard.address) then {
							if determineAgreedVersion(config.versionsSupportedByMe, aspirantCard.supportedVersions).isDefined then {
								clusterService.createAndAddADelegateForAndThenConnectToParticipant(aspirantCard.address, aspirantCard.supportedVersions, ASPIRANT)
							} else {
								val newDelegate = clusterServiceBehavior.createAndAddAnIncommunicableDelegate(aspirantCard.address, false)
								newDelegate.versionsSupportedByPeer = aspirantCard.supportedVersions
								newDelegate.peerMembershipStatusAccordingToMe = ASPIRANT
								clusterService.notify(VersionIncompatibilityWith(aspirantCard.address))
							}
						}
					}
					// Propose a cluster creator if apropiate.
					clusterServiceBehavior.proposeClusterCreatorIfAppropriate()
					continueReceiving(onComplete)

				case ClusterCreatorProposal(candidateProposedByPeer, versionsSupportedByCandidate) =>
					clusterCreatorCandidateProposedByPeer = candidateProposedByPeer
					clusterServiceBehavior.proposeClusterCreatorIfAppropriate()
					continueReceiving(onComplete)

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
