package readren.matrix
package cluster.service

import cluster.channel.Transmitter
import cluster.service.Protocol.*
import cluster.service.Protocol.IncommunicabilityReason.IS_INCOMPATIBLE
import cluster.service.Protocol.MembershipStatus.MEMBER

import readren.matrix.common.CompileTime.getTypeName

/** A communicable participant's delegate suited for a [[ClusterService]] with a [[MemberBehavior]]. */
class MemberBehavior(clusterService: ClusterService, val clusterCreationInstant: Instant) extends MembershipScopedBehavior {

	private var stateSerial: RingSerial = RingSerial.create()

	inline def currentStateSerial: RingSerial = stateSerial
	
	def myCurrentViewpoint: MemberViewpoint = MemberViewpoint(stateSerial, clusterService.clock.getTime, clusterCreationInstant, clusterService.getKnownParticipantsInfo.toMap)

	override val membershipStatus: MembershipStatus = MEMBER

	override def onDelegatedAdded(delegate: ParticipantDelegate): Unit =
		() // TODO

	override def onDelegateCommunicabilityChange(delegate: ParticipantDelegate): Unit =
		() // TODO

	override def onDelegateMembershipChange(delegate: ParticipantDelegate): Unit =
		() // TODO

	override def openConversationWith(delegate: CommunicableDelegate, isReconnection: Boolean)(onComplete: Transmitter.Report => Unit): Unit =
		() // TODO

	override def handleMessageFrom(senderDelegate: CommunicableDelegate, message: Protocol): Boolean = message match {
		case am: ApplicationMsg =>
			// TODO
			true

		case hb: Heartbeat =>
			???

		case sc: StateChanged =>
			???

		case hello: Hello =>
			senderDelegate.handleMessage(hello)

		case w: Welcome =>
			scribe.warn(s"The participant at ${senderDelegate.peerAddress} sent me a ${getTypeName[Welcome]} despite I consider myself a member of a cluster.")
			true

		case ConversationStartedWith(peerAddress) =>
			// TODO
			true
			
		case ClusterCreatorProposal(proposedCandidate) =>
			scribe.warn(s"The aspirant at ${senderDelegate.peerAddress} sent me a ${getTypeName[ClusterCreatorProposal]} despite cluster already exists.")
			senderDelegate.sendResolveAspirantMembershipConflict()
			true

		case icc: ICreatedACluster =>
			if icc.myViewpoint.clusterCreationInstant != clusterCreationInstant then {
				clusterService.switchToResolvingBrainJoin()
				for case (_, delegate: CommunicableDelegate) <- clusterService.delegateByAddress do {
					delegate.transmitToPeer(WeHaveToResolveBrainJoin(myCurrentViewpoint))(delegate.ifFailureReportItAndThen(delegate.restartChannel))
				}
			}
			true

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
			
		case rsm: ResolveAspirantMembershipConflict =>
			???

		case lcw: ILostCommunicationWith =>
			???

		case IAmLeaving =>
			???

		case cd: ChannelDiscarded =>
			senderDelegate.handleMessage(cd)
			
		case phr: AnotherParticipantHasBeenRestarted =>
			???

		case ihr: IHaveReconnected =>
			???

		case WeHaveToResolveBrainSplit(peerViewPoint) =>
			???
			true

		case WeHaveToResolveBrainJoin(peerViewPoint) =>
			???
			true

		case SupportedVersionsMismatch =>
			senderDelegate.handleMessageSupportedVersionsMismatch()
	}
}
