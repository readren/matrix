package readren.matrix
package cluster.service

import cluster.service.Protocol.*
import common.CompileTime.getTypeName

/** A communicable participant's delegate suited for a [[ClusterService]] with a [[MemberBehavior]]. */
class MemberBehavior(clusterService: ClusterService, val clusterCreationInstant: Instant) extends MembershipScopedBehavior {

	private var stateSerial: RingSerial = RingSerial.create()

	inline def currentStateSerial: RingSerial = stateSerial
	
	def myCurrentViewpoint: MemberViewpoint = MemberViewpoint(stateSerial, clusterService.clock.getTime, clusterCreationInstant, clusterService.getStableParticipantsInfo.toMap)

	override val membershipStatus: MembershipStatus = MEMBER(clusterService.myCreationInstant)

	override def onDelegatedAdded(delegate: ParticipantDelegate): Unit =
		() // TODO

	override def onDelegateCommunicabilityChange(delegate: ParticipantDelegate): Unit =
		() // TODO

	override def onDelegateMembershipChange(delegate: ParticipantDelegate): Unit =
		() // TODO

	override def openConversationWith(delegate: CommunicableDelegate, isReconnection: Boolean): Unit = {
		if isReconnection then delegate.sendPeerAHelloIAmBack()
		else delegate.sendPeerAHelloIExist()
	}

	override def handleInitiatorMessageFrom(senderDelegate: CommunicableDelegate, initiationMsg: InitiationMsg): Boolean = initiationMsg match {
		case hello: HelloIExist =>
			senderDelegate.handleMessage(hello)

		case ihr: HelloIAmBack =>
			ihr.myMembershipStatus match {
				case ASPIRANT => senderDelegate.handleMessage(ihr)
				case MEMBER(clusterCreationInstantAccordingToPeer) => ???
			}

		case ConversationStartedWith(peerAddress, isARestartAfterReconnection) =>
			// TODO
			true
			
		case ClusterCreatorProposal(proposedCandidate) =>
			scribe.warn(s"The aspirant at ${senderDelegate.peerAddress} sent me a ${getTypeName[ClusterCreatorProposal]} despite cluster already exists.")
			senderDelegate.incitePeerToResolveMembershipConflict()
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

		case rtj: RequestToJoin =>
			???

		case rmc: WaitMyMembershipStatusIs =>
			senderDelegate.handleMessage(rmc)
			true

		case lcw: ILostCommunicationWith =>
			???

		case fw: Farewell =>
			senderDelegate.handleMessage(fw)

		case apg: AnotherParticipantGone =>
			senderDelegate.handleMessage(apg)
			true

		case isw: AreYouInSyncWithMe =>
			senderDelegate.handleMessage(isw)
			true

		case cd: ChannelDiscarded =>
			senderDelegate.handleMessage(cd)
			
		case phr: AMemberHasBeenRebooted =>
			senderDelegate.handleMessage(phr)
			true

		case WeHaveToResolveBrainSplit(peerViewPoint) =>
			???
			true

		case WeHaveToResolveBrainJoin(peerViewPoint) =>
			???
			true

		case hb: Heartbeat =>
			???

		case sc: ClusterStateChanged =>
			???

		case am: ApplicationMsg =>
			// TODO
			true
	}
}
