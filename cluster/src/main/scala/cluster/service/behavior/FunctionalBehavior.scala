package readren.matrix
package cluster.service.behavior

import cluster.service.*
import cluster.service.Protocol.*
import cluster.service.Protocol.CommunicationStatus.HANDSHOOK
import cluster.service.behavior.MemberBehavior

class FunctionalBehavior(override val host: ParticipantService, startingStateSerial: RingSerial, override val myClusterId: ClusterId) extends MemberBehavior(host, startingStateSerial, myClusterId) {
	override val membershipStatus: Functional = Functional(myClusterId)

	override def isIsolated = false

	override def onPeerCommunicabilityChange(delegate: ParticipantDelegate, previousStatus: CommunicationStatus): Unit = {
		stateSerial = stateSerial.nextSerial
		scribe.trace(s"${host.myAddress}: ----------> 1 previousStatus=$previousStatus, newStatus=${delegate.communicationStatus}")
		if previousStatus eq HANDSHOOK then {
			assert(delegate.communicationStatus ne HANDSHOOK)
			scribe.trace(s"${host.myAddress}: ----------> 2")
			if delegate.getPeerMembershipStatusAccordingToMe.exists(membershipStatus.isColleagueOf) then {
				// get the communication status of the hots's colleagues.
				val colleaguesCommunicationStatuses = host.delegateByAddress.view.collect {
					case (_, delegate) if delegate.getPeerMembershipStatusAccordingToMe.exists(membershipStatus.isColleagueOf) => delegate.communicationStatus
				}
				scribe.trace(s"${host.myAddress}: ----------> 3 colleaguesStatuses=$colleaguesCommunicationStatuses")
				if host.config.isolationDecider.shouldIBeIsolated(colleaguesCommunicationStatuses) then host.switchToIsolated(this)
				scribe.trace(s"${host.myAddress}: ----------> 4 decision=colleaguesStatuses=${host.config.isolationDecider.shouldIBeIsolated(colleaguesCommunicationStatuses)}")

			}
		}
		// TODO
	}

	override protected def onHello(senderDelegate: CommunicableDelegate, hello: Hello): Boolean = {
		hello.myMembershipStatus match {
			case Aspirant =>
				senderDelegate.handleHelloCommonLogic(hello)

			case foi: (Functional | Isolated) =>
				if foi.clusterId == myClusterId then senderDelegate.handleHelloCommonLogic(hello)
				else host.switchToConflicted(this, Set(foi.clusterId), s"I received a `$hello` from a member of another cluster.")
					.handleInitiatorMessageFrom(senderDelegate, hello)

			case Conflicted(peerClusterId, otherClusterIdsKnownByThePeer, wasIsolated) =>
				val newBehavior =
					if peerClusterId == myClusterId then
						host.switchToConflicted(this, otherClusterIdsKnownByThePeer, s"I received a `$hello` from a conflicted member of my cluster.")
					else
						host.switchToConflicted(this, otherClusterIdsKnownByThePeer + peerClusterId, s"I received a `$hello` from a conflicted member of another cluster.")
				newBehavior.handleInitiatorMessageFrom(senderDelegate, hello)
		}
	}

	override protected def onMessage(senderDelegate: CommunicableDelegate, message: ICreatedACluster): Boolean = {
		if message.myViewpoint.clusterId == myClusterId then scribe.warn(s"The participant at `${senderDelegate.peerContactAddress}` sent me a `$message` despite I already belong to that cluster.")
		else host.switchToConflicted(this, Set(message.myViewpoint.clusterId), s"I received a `$message` despite I already belong to the cluster `$myClusterId`.")
		true
	}

	override protected def onMessage(senderDelegate: CommunicableDelegate, rtj: RequestToJoin): Unit = {
		val members = memberDelegateByAddress
		val allMembersHaveHandshookWithMe = members.forall(_._2.communicationStatus eq HANDSHOOK)
		val theSenderKnowsTheSameMembersAsMe = members.size + 1 == rtj.joinTokenByMemberAddress.size && members.forall { (memberAddress, delegate) => rtj.joinTokenByMemberAddress.get(memberAddress).contains(delegate.getPeerCreationInstant) }
		if allMembersHaveHandshookWithMe && theSenderKnowsTheSameMembersAsMe then {
			senderDelegate.askPeer(new senderDelegate.SingleRetryOutgoingRequestExchange[JoinGranted]() {
				override def buildRequest(requestId: RequestId): JoinGranted =
					JoinGranted(rtj.requestId, requestId, host.getStableParticipantsInfo.toMap)

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

	override protected def onMessage(senderDelegate: CommunicableDelegate, isolated: AreWeIsolated): Unit = {
		
		???
	}

	override protected def onMessage(senderDelegate: CommunicableDelegate, message: WeAreIsolated): Unit = {
		if senderDelegate.getViewpointPhoto.fold(true)(_.serial.isBehindOf(message.myViewpoint.serial)) then {
			senderDelegate.updateViewpointPhoto(message.myViewpoint)
			host.switchToIsolated(this)
		} else scribe.info(s"The message `$message` was ignored because its state serial isn't newer than the current one `${senderDelegate.getViewpointPhoto.get.serial}`.")
	}
}
