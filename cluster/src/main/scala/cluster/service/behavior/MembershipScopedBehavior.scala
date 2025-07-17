package readren.matrix
package cluster.service.behavior

import cluster.service.Protocol.MembershipStatus
import cluster.service.{CommunicableDelegate, InitiationMsg, ParticipantDelegate, ParticipantService}

/** The aspect of a [[ParticipantService]]'s behavior that depends on the membership status.
 * Implementations may have member variables that are needed, or refer to object that exists, only when the [[ParticipantService]] is in the corresponding [[MembershipStatus]]. */
abstract class MembershipScopedBehavior {
	/** The [[MembershipStatus]] this behavior corresponds to. */
	val membershipStatus: MembershipStatus

	def onDelegatedAdded(delegate: ParticipantDelegate): Unit

	def onDelegateCommunicabilityChange(delegate: ParticipantDelegate): Unit

	def onDelegateMembershipChange(delegate: ParticipantDelegate): Unit

//	/** Called just after a successful connection to the participant corresponding to the specified `delegate` when I am at the client side of the channel.
//	 * The implementation should transmit the conversation-opening message. */
//	def openConversationWith(participantDelegate: CommunicableDelegate, isReconnection: Boolean): Unit

	/**
	 * Handles an incoming `message` from the participant associated with the `delegate`, possibly deferring some reactions (e.g., queueing them for later sequential execution).
	 *
	 * The method must return *immediately* with a `Boolean` indicating whether the conversation should continue (`true`), or terminate (`false`).
	 * Note: While reactions to the message may be deferred (e.g., processed later in a strict order), the decision to continue/end the conversation must be made synchronously.
	 */
	def handleInitiatorMessageFrom(senderDelegate: CommunicableDelegate, initiationMsg: InitiationMsg): Boolean
}
