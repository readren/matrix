package readren.matrix
package cluster.service.behavior

import cluster.service.*
import cluster.service.Protocol.*
import common.CompileTime.getTypeName

import readren.sequencer.Maybe

import scala.collection.MapView

/** A communicable participant's delegate suited for a [[ParticipantService]] with a [[MemberBehavior]].
 * @param host the [[ParticipantService]] that hosts this behavior.
 * @param startingStateSerial the revision number of the state this instance starts with.
 * @param myClusterId the id [[ClusterId]] of the cluster thet the hosting [[ParticipantService]] belongs to. */
abstract class MemberBehavior(override val host: ParticipantService, startingStateSerial: RingSerial, val myClusterId: ClusterId) extends MembershipScopedBehavior {

	override type MS = Member
	protected var stateSerial: RingSerial = startingStateSerial

	inline def currentStateSerial: RingSerial = stateSerial

	def isIsolated: Boolean
	
	def myCurrentViewpoint: MemberViewpoint = MemberViewpoint(stateSerial, host.clock.getTime, myClusterId, host.getStableParticipantsInfo.toMap)

	def isColleague(delegate: ParticipantDelegate): Boolean = delegate.getPeerMembershipStatusAccordingToMe.contains {
		case member: Member if member.clusterId == myClusterId => true
		case _ => false	
	}


	override def onPeerAdded(delegate: ParticipantDelegate): Unit = {
		stateSerial = stateSerial.nextSerial
		// TODO
	}

	override def onPeerMembershipChange(delegate: ParticipantDelegate, previousStatus: Maybe[MembershipStatus]): Unit = {
		stateSerial = stateSerial.nextSerial
		// TODO
	}

	protected def onHello(senderDelegate: CommunicableDelegate, hello: Hello): Boolean
	
	protected def onMessage(senderDelegate: CommunicableDelegate, message: ICreatedACluster): Boolean
	
	protected def onMessage(senderDelegate: CommunicableDelegate, message: RequestToJoin): Unit
	
	protected def onMessage(senderDelegate: CommunicableDelegate, message: AreWeIsolated): Unit
	
	protected def onMessage(senderDelegate: CommunicableDelegate, message: WeAreIsolated): Unit
	
	override def handleInitiatorMessageFrom(senderDelegate: CommunicableDelegate, initiationMsg: NonResponse): Boolean = initiationMsg match {

		case hello: HelloIExist => onHello(senderDelegate, hello)
		// The previous and next match-cases could be merged, but they are kept separate so the compiler notices that it can optimize this whole match construct with a lookup table.
		case hello: HelloIAmBack => onHello(senderDelegate, hello)
	
		case ConversationStartedWith(peerAddress, isARestartAfterReconnection) =>
			// TODO
			true

		case ccp: ClusterCreatorProposal =>
			scribe.warn(s"The aspirant at `${senderDelegate.peerContactAddress}` sent me a `${getTypeName[ClusterCreatorProposal]}` despite I already belong to the cluster `$myClusterId`.")
			senderDelegate.incitePeerToUpdateHisStateAboutMyStatus()
			true

		case icc: ICreatedACluster => onMessage(senderDelegate, icc)

		case oi: RequestApprovalToJoin =>
			???

		case rtj: RequestToJoin =>
			onMessage(senderDelegate, rtj)
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

		case awi: AreWeIsolated =>
			onMessage(senderDelegate, awi)
			true
			
		case wai: WeAreIsolated =>
			onMessage(senderDelegate, wai)
			true

		case WeHaveToResolveClustersConflict(peerViewpoint) =>
			var otherClusters: Set[ClusterId] = if peerViewpoint.clusterId == myClusterId then Set.empty else Set(peerViewpoint.clusterId)
			for	pi <- peerViewpoint.participantsInfo do pi._2.membershipStatus match {
				case member: Member if member.clusterId != myClusterId => otherClusters += member.clusterId
				case _ => // do nothing
			}
			if otherClusters.nonEmpty then host.switchToConflicted(this, otherClusters, s"I received `$initiationMsg` from `${senderDelegate.peerContactAddress}`")
			else scribe.warn(s"I received `$initiationMsg` from `${senderDelegate.peerContactAddress}` which is contradictory because only one cluster is mentioned.")
			true

		case hb: Heartbeat =>
			// TODO
			true

		case sc: ClusterStateChanged =>
			// TODO
			true

		case am: ApplicationMsg =>
			// TODO
			true
	}

	protected def memberDelegateByAddress: MapView[ContactAddress, ParticipantDelegate] =
		host.delegateByAddress.view.filter { (_, delegate) => delegate.getPeerMembershipStatusAccordingToMe.contentEquals(membershipStatus) }

}
