package readren.matrix
package cluster.service

import cluster.service.Protocol.*
import cluster.service.Protocol.CommunicationStatus.HANDSHOOK
import common.CompileTime.getTypeName

import scala.collection.MapView

/** A communicable participant's delegate suited for a [[ParticipantService]] with a [[MemberBehavior]]. */
class MemberBehavior(startingStateSerial: RingSerial, participantService: ParticipantService, val clusterCreationInstant: Instant) extends MembershipScopedBehavior {

	private var stateSerial: RingSerial = startingStateSerial

	inline def currentStateSerial: RingSerial = stateSerial

	def myCurrentViewpoint: MemberViewpoint = MemberViewpoint(stateSerial, participantService.clock.getTime, clusterCreationInstant, participantService.getStableParticipantsInfo.toMap)

	override val membershipStatus: MembershipStatus = Member(clusterCreationInstant)

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
						switchToResolvingBrainJoin(senderDelegate, clusterCreationInstantAccordingToPeer, s"I received a `$ihr` from a member of another cluster.")
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
				switchToResolvingBrainJoin(senderDelegate, icc.myViewpoint.clusterCreationInstant, s"I received a `$icc` when a cluster with a different creation instant already exists, of which I am a member of.")
			} else scribe.warn(s"I have ignored the message `$icc` from `${senderDelegate.contactCard}` because I already am a member of the cluster he created.")
			true

		case oi: RequestApprovalToJoin =>
			???

		case rtj: RequestToJoin =>
			val members = memberDelegateByAddress
			val allMembersHaveHandshookWithMe = members.forall(_._2.communicationStatus eq HANDSHOOK)
			val theSenderKnowsTheSameMembersAsMe = members.size + 1 == rtj.joinTokenByMemberAddress.size && members.forall { (memberAddress, delegate) => rtj.joinTokenByMemberAddress.get(memberAddress).contains(delegate.getPeerCreationInstant) }
			if allMembersHaveHandshookWithMe && theSenderKnowsTheSameMembersAsMe then {
				senderDelegate.askPeer(new senderDelegate.SingleRetryOutgoingRequestExchange[JoinGranted]() {
					override def buildRequest(requestId: RequestId): JoinGranted =
						JoinGranted(rtj.requestId, requestId, participantService.getStableParticipantsInfo.toMap)

					override def onResponse(request: JoinGranted, response: JoinDecision): Boolean = {
						if response.accepted then senderDelegate.updateState(membershipStatus)
						true
					}
				})
			} else {
				val aMemberIsNotStable = members.exists(!_._2.isStable)
				var reasons = List.empty[String]
				if aMemberIsNotStable then reasons = "A member is not stable" :: reasons
				if !allMembersHaveHandshookWithMe then reasons = "A member has not handshook with me" :: reasons
				if !theSenderKnowsTheSameMembersAsMe then reasons = "The members you know aren't the same as the ones I know." :: reasons
				senderDelegate.transmitToPeerOrRestartChannel(JoinRejected(rtj.requestId, aMemberIsNotStable, reasons.mkString(", ")))
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
			// TODO
			true

		case am: ApplicationMsg =>
			// TODO
			true

		case _: Response =>
			throw new AssertionError("unreachable")
	}

	private def memberDelegateByAddress: MapView[ContactAddress, ParticipantDelegate] =
		participantService.delegateByAddress.view.filter { (_, delegate) => delegate.getPeerMembershipStatusAccordingToMe.contentEquals(membershipStatus) }


	private def switchToResolvingBrainJoin(delegateMemberOfForeignCluster: CommunicableDelegate, otherClusterCreationInstant: Instant, why: String): Unit = {
		scribe.warn(s"${participantService.myAddress}: Switched to `BrainJoinBehavior` because: $why")

		val newBehavior = BrainJoinBehavior(participantService, otherClusterCreationInstant)
		for (_, delegate) <- participantService.handshookDelegateByAddress do {
			delegate.transmitToPeerOrRestartChannel(WeHaveToResolveBrainJoin(myCurrentViewpoint))
		}
		// TODO

	}

	private def switchToResolvingBrainSplit(): Unit = ???

}
