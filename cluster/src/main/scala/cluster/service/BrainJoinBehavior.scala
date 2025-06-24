package readren.matrix
package cluster.service

import scala.collection.mutable
import cluster.service.Protocol.{BrainJoin, Instant}

class BrainJoinBehavior(clusterService: ClusterService, otherClusterCreationInstant: Instant) extends MembershipScopedBehavior {
	
	class CompetingCluster
	
	var otherClustersByCreationInstant: mutable.Map[Instant, CompetingCluster] = mutable.Map(otherClusterCreationInstant -> CompetingCluster())
	
	/** The [[MembershipStatus]] this behavior corresponds to. */
	override val membershipStatus: Protocol.MembershipStatus = BrainJoin(clusterService.myCreationInstant, otherClustersByCreationInstant.keySet.toSet)

	override def onDelegatedAdded(delegate: ParticipantDelegate): Unit = ???

	override def onDelegateCommunicabilityChange(delegate: ParticipantDelegate): Unit = ???

	override def onDelegateMembershipChange(delegate: ParticipantDelegate): Unit = ???

	/** Called just after a successful connection to the participant corresponding to the specified `delegate` when I am at the client side of the delegate's channel.
	 * The implementation should transmit the conversation-opening message. */
	override def openConversationWith(participantDelegate: CommunicableDelegate, isReconnection: Boolean): Unit = ???

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
