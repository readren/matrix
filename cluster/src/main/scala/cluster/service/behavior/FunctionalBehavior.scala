package readren.matrix
package cluster.service.behavior

import cluster.service.*
import cluster.service.Protocol.*
import cluster.service.Protocol.CommunicationStatus.HANDSHOOK
import cluster.service.behavior.MemberBehavior

class FunctionalBehavior(participantService: ParticipantService, startingStateSerial: RingSerial, clusterId: ClusterId) extends MemberBehavior(participantService, startingStateSerial, clusterId) {

	override val membershipStatus: MembershipStatus = Functional(clusterId)

	override protected def onHello(senderDelegate: CommunicableDelegate, hello: Hello): Boolean = {
		hello.myMembershipStatus match {
			case Aspirant =>
				senderDelegate.handleHelloCommonLogic(hello)

			case Functional(peerClusterId) =>
				if peerClusterId == clusterId then senderDelegate.handleHelloCommonLogic(hello)
				else switchToConflicted(senderDelegate, Set(peerClusterId), false, s"I received a `$hello` from a functional member of another cluster.")
					.handleInitiatorMessageFrom(senderDelegate, hello)

			case Isolated(peerClusterId) =>
				if peerClusterId == clusterId then senderDelegate.handleHelloCommonLogic(hello)
				else switchToConflicted(senderDelegate, Set(peerClusterId), true, s"I received a `$hello` from an isolated member of another cluster.")
					.handleInitiatorMessageFrom(senderDelegate, hello)


			case Conflicted(peerClusterId, otherClusterIdsKnownByThePeer, wasIsolated) =>
				val otherClustersIds =
					if peerClusterId == clusterId then otherClusterIdsKnownByThePeer
					else otherClusterIdsKnownByThePeer + peerClusterId

				switchToConflicted(senderDelegate, otherClustersIds, false, s"I received a `$hello` from a conflicted member.")
					.handleInitiatorMessageFrom(senderDelegate, hello)
		}
	}

	override protected def onMessage(senderDelegate: CommunicableDelegate, message: ICreatedACluster): Boolean = {
		if message.myViewpoint.clusterId == clusterId then scribe.warn(s"The participant at `${senderDelegate.peerContactAddress}` sent me a `$message` despite I already belong to that cluster.")
		else switchToConflicted(senderDelegate, Set(message.myViewpoint.clusterId), false, s"I received a `$message` despite I already belong to the cluster `$clusterId`.")
		true
	}

	override protected def onMessage(senderDelegate: CommunicableDelegate, rtj: RequestToJoin): Unit = {
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
			senderDelegate.transmitToPeerOrRestartChannel(JoinRejected(rtj.requestId, aMemberIsNotStable, aMemberIsNotStable, !theSenderKnowsTheSameMembersAsMe, false, false))
		}
	}
}
