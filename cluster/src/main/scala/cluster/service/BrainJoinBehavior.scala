package readren.matrix
package cluster.service

import scala.collection.mutable
import cluster.service.Protocol.{BrainJoin, ClusterId, Instant}

class BrainJoinBehavior(participantService: ParticipantService, startingStateSerial: RingSerial, clusterIBelongTo: ClusterId, otherClusterId: ClusterId) extends MembershipScopedBehavior {

	private var stateSerial: RingSerial = startingStateSerial
	
	class CompetingCluster
	
	var otherClustersById: mutable.Map[ClusterId, CompetingCluster] = mutable.Map(otherClusterId -> CompetingCluster())
	
	/** The [[MembershipStatus]] this behavior corresponds to. */
	override val membershipStatus: Protocol.MembershipStatus = BrainJoin(clusterIBelongTo, otherClustersById.keySet.toSet)

	override def onDelegatedAdded(delegate: ParticipantDelegate): Unit = ???

	override def onDelegateCommunicabilityChange(delegate: ParticipantDelegate): Unit = ???

	override def onDelegateMembershipChange(delegate: ParticipantDelegate): Unit = ???

	/**
	 * Handles an incoming `message` from the participant associated with the `delegate`, possibly deferring some reactions (e.g., queueing them for later sequential execution).
	 *
	 * The method must return *immediately* with a `Boolean` indicating whether the conversation:
	 * - Should continue (`true`), or
	 * - Should terminate (`false`).
	 *
	 * Note: While reactions to the message may be deferred (e.g., processed later in a strict order), the decision to continue/end the conversation must be made synchronously.
	 */
	override def handleInitiatorMessageFrom(senderDelegate: CommunicableDelegate, initiationMsg: InitiationMsg): Boolean = ???
}
