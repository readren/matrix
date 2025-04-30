package readren.matrix
package cluster.service

import cluster.channel.Transmitter
import cluster.service.Protocol.MembershipStatus

/** The aspect of a [[ClusterService]]'s behavior that depends on the membership status.
 * Implementations may have member variables that are needed, or referer to object that exists, only when the [[ClusterService]] is in the corresponding [[MembershipStatus]]. */
abstract class MembershipScopedBehavior {
	/** The [[MembershipStatus]] this behavior corresponds to. */
	val membershipStatus: MembershipStatus

	def onDelegatedAdded(delegate: ParticipantDelegate): Unit

	def onDelegateCommunicabilityChange(delegate: ParticipantDelegate): Unit

	def onDelegateMembershipChange(delegate: ParticipantDelegate): Unit

	/** Called just after a successful connection to the participant corresponding to the specified `delegate` when I am at the client side of the delegate's channel.
	 * The implementation should transmit the conversation-opening message and call the provided `onComplete` call-back when the transmission completes. */
	def openConversationWith(participantDelegate: CommunicableDelegate, isReconnection: Boolean)(onComplete: Transmitter.Report => Unit): Unit

	/**
	 * Handles an incoming `message` from the participant associated to the `delegate`, possibly deferring some reactions (e.g., queueing them for later sequential execution).
	 *
	 * The method must return *immediately* with a `Boolean` indicating whether the conversation:
	 * - Should continue (`true`), or
	 * - Should terminate (`false`).
	 *
	 * Note: While reactions to the message may be deferred (e.g., processed later in a strict order), the decision to continue/end the conversation must be made synchronously.
	 */
	def handleInitiatorMessageFrom(senderDelegate: CommunicableDelegate, initiationMsg: InitiationMsg): Boolean
	
	
	def handleResponseMessageFrom(senderDelegate: CommunicableDelegate, response: Response, toRequestType: String): Boolean
	
	
}
