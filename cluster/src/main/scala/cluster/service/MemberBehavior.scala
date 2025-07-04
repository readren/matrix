package readren.matrix
package cluster.service

import cluster.service.Protocol.*
import cluster.service.Protocol.CommunicationStatus.HANDSHOOK
import common.CompileTime.getTypeName

import scala.collection.MapView

/** A communicable participant's delegate suited for a [[ClusterService]] with a [[MemberBehavior]]. */
class MemberBehavior(startingStateSerial: RingSerial, clusterService: ClusterService, val clusterCreationInstant: Instant) extends MembershipScopedBehavior {

	private var stateSerial: RingSerial = startingStateSerial

	inline def currentStateSerial: RingSerial = stateSerial

	def myCurrentViewpoint: MemberViewpoint = MemberViewpoint(stateSerial, clusterService.clock.getTime, clusterCreationInstant, clusterService.getStableParticipantsInfo.toMap)

	override val membershipStatus: MembershipStatus = Member(clusterService.myCreationInstant)

	override def onDelegatedAdded(delegate: ParticipantDelegate): Unit = {
		stateSerial = stateSerial.nextSerial
		// TODO
	}

	override def onDelegateCommunicabilityChange(delegate: ParticipantDelegate): Unit = {
		stateSerial = stateSerial.nextSerial
		// TODO
	}

	override def onDelegateMembershipChange(delegate: ParticipantDelegate): Unit = {
		stateSerial = stateSerial.nextSerial
		// TODO
	}

	override def handleInitiatorMessageFrom(senderDelegate: CommunicableDelegate, initiationMsg: InitiationMsg): Boolean = initiationMsg match {
		case hello: HelloIExist =>
			senderDelegate.handleMessage(hello)

		case ihr: HelloIAmBack =>
			ihr.myMembershipStatus match {
				case Aspirant =>
					senderDelegate.handleMessage(ihr)

				case Member(clusterCreationInstantAccordingToPeer) =>
					if clusterCreationInstantAccordingToPeer != clusterCreationInstant then {
						switchToResolvingBrainJoin(senderDelegate, clusterCreationInstantAccordingToPeer)
						true
					} else {
						senderDelegate.handleMessage(ihr)
					}

				case BrainSplit =>
					???

				case BrainJoin(creationInstantOfClusterThePeerBelongsTo, creationInstantOfClustersThePeerIsTryingToMerge) =>
					???
			}

		case ConversationStartedWith(peerAddress, isARestartAfterReconnection) =>
			// TODO
			true

		case ClusterCreatorProposal(proposedCandidate) =>
			scribe.warn(s"The aspirant at `${senderDelegate.peerContactAddress}` sent me a `${getTypeName[ClusterCreatorProposal]}` despite cluster already exists.")
			senderDelegate.incitePeerToResolveMembershipConflict()
			true

		case icc: ICreatedACluster =>
			if icc.myViewpoint.clusterCreationInstant != clusterCreationInstant then {
				switchToResolvingBrainJoin(senderDelegate, icc.myViewpoint.clusterCreationInstant)
			} else scribe.warn(s"I have ignored the message `$icc` from the participant at `${senderDelegate.contactCard}` because I already am a member of the cluster he created.")
			true

		case oi: RequestApprovalToJoin =>
			???

		case rtj: RequestToJoin =>
			val members = memberDelegateByAddress
			// if the sender knows the same members as me and all are stable
			if members.forall(_._2.communicationStatus eq HANDSHOOK) && members.mapValues(_.getPeerCreationInstant) == rtj.joinTokenByMemberAddress then {
				senderDelegate.askPeer(new senderDelegate.SingleRetryOutgoingRequestExchange[JoinGranted]() {
					override def buildRequest(requestId: RequestId): JoinGranted =
						JoinGranted(rtj.requestId, requestId, clusterService.myCreationInstant, clusterService.getStableParticipantsInfo.toMap)

					override def onResponse(request: JoinGranted, response: JoinDecision): Boolean = {
						if response.accepted then senderDelegate.updateState(membershipStatus)
						true
					}
				})
			}
			true

		case wms: WaitMyMembershipStatusIs =>
			senderDelegate.handleMessage(wms)
			true

		case lcw: ILostCommunicationWith =>
			senderDelegate.checkSyncWithPeer(s"the participant at `${senderDelegate.peerContactAddress}` told me that he lost communication with `${lcw.participantsAddress}`.")
			true

		case fw: Farewell =>
			senderDelegate.handleMessage(fw)

		case apg: AnotherParticipantGone =>
			senderDelegate.handleMessage(apg)
			true

		case isw: AreYouInSyncWithMe =>
			senderDelegate.handleMessage(isw)
			true

		case ChannelDiscarded =>
			senderDelegate.handleChannelDiscarded()

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

		case _: Response =>
			throw new AssertionError("unreachable")
	}

	private def memberDelegateByAddress: MapView[ContactAddress, ParticipantDelegate] =
		clusterService.delegateByAddress.view.filter { (_, delegate) => delegate.getPeerMembershipStatusAccordingToMe.contentEquals(membershipStatus) }


	private def switchToResolvingBrainJoin(delegateMemberOfForeignCluster: CommunicableDelegate, otherClusterCreationInstant: Instant): Unit = {

		val newBehavior = BrainJoinBehavior(clusterService, otherClusterCreationInstant)
		for (_, delegate) <- clusterService.handshookDelegateByAddress do {
			delegate.transmitToPeerOrRestartChannel(WeHaveToResolveBrainJoin(myCurrentViewpoint))
		}
		// TODO

	}

	private def switchToResolvingBrainSplit(): Unit = ???

}
