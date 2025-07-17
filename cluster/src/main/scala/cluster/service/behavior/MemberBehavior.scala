package readren.matrix
package cluster.service.behavior

import cluster.service.*
import cluster.service.Protocol.*
import cluster.service.behavior.ConflictedBehavior
import common.CompileTime.getTypeName

import scala.collection.MapView

/** A communicable participant's delegate suited for a [[ParticipantService]] with a [[MemberBehavior]].
 * @param participantService the [[ParticipantService]] that hosts this behavior.
 * @param startingStateSerial the revision number of the state this instance starts with.
 * @param clusterId the id [[ClusterId]] of the cluster thet the hosting [[ParticipantService]] belongs to. */
abstract class MemberBehavior(participantService: ParticipantService, startingStateSerial: RingSerial, clusterId: ClusterId) extends MembershipScopedBehavior {

	protected var stateSerial: RingSerial = startingStateSerial

	inline def currentStateSerial: RingSerial = stateSerial

	def myCurrentViewpoint: MemberViewpoint = MemberViewpoint(stateSerial, participantService.clock.getTime, clusterId, participantService.getStableParticipantsInfo.toMap)


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

	protected def onHello(senderDelegate: CommunicableDelegate, hello: Hello): Boolean
	
	protected def onMessage(senderDelegate: CommunicableDelegate, message: ICreatedACluster): Boolean
	
	protected def onMessage(senderDelegate: CommunicableDelegate, message: RequestToJoin): Unit
	
	override def handleInitiatorMessageFrom(senderDelegate: CommunicableDelegate, initiationMsg: InitiationMsg): Boolean = initiationMsg match {

		case hello: HelloIExist => onHello(senderDelegate, hello)
		// the previous and next match-cases may be merged, but they are not in order allow the compiler to optimize this whole match construct with a lookup table.
		case hello: HelloIAmBack => onHello(senderDelegate, hello)
	
		case ConversationStartedWith(peerAddress, isARestartAfterReconnection) =>
			// TODO
			true

		case ccp: ClusterCreatorProposal =>
			scribe.warn(s"The aspirant at `${senderDelegate.peerContactAddress}` sent me a `${getTypeName[ClusterCreatorProposal]}` despite I already belong to the cluster `$clusterId`.")
			senderDelegate.incitePeerToResolveMembershipConflict()
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

		case _: Response => // TODO make Request and InitiationMsg be disjoint to avoid the need of this match-case.
			throw new AssertionError("unreachable")
	}

	protected def memberDelegateByAddress: MapView[ContactAddress, ParticipantDelegate] =
		participantService.delegateByAddress.view.filter { (_, delegate) => delegate.getPeerMembershipStatusAccordingToMe.contentEquals(membershipStatus) }


	protected def switchToConflicted(delegateMemberOfForeignCluster: CommunicableDelegate, otherClustersIds: Set[ClusterId], wasIsolated: Boolean, why: String): ConflictedBehavior = {
		val newBehavior = ConflictedBehavior(participantService, stateSerial.nextSerial, clusterId, otherClustersIds, wasIsolated, why)

		for (_, delegate) <- participantService.handshookDelegateByAddress do {
			delegate.transmitToPeerOrRestartChannel(WeHaveToResolveBrainJoin(myCurrentViewpoint))
		}
		// TODO
		newBehavior
	}
}
