package readren.matrix
package cluster.service.behavior

import cluster.service.Protocol.{CommunicationStatus, MembershipStatus}
import cluster.service.{CommunicableDelegate, NonResponse, ParticipantDelegate, ParticipantService}

import readren.taskflow.Maybe

/** The aspect of a [[ParticipantService]]'s behavior that depends on the membership status.
 * Implementations may have member variables that are needed, or refer to object that exists, only when the [[ParticipantService]] is in the corresponding [[MembershipStatus]]. */
abstract class MembershipScopedBehavior {
	type MS <: MembershipStatus
	/** The [[MembershipStatus]] this behavior corresponds to. */
	def membershipStatus: MS
	
	/** The [[ParticipantService]] that hosts this behavior  */
	val host: ParticipantService

	/** @param delegate the delegate associated with the added peer. The implementation may assume that this delegate is associated. */
	def onPeerAdded(delegate: ParticipantDelegate): Unit

	/** @param delegate the delegate associated with the updated peer. The implementation may assume that this delegate is associated. */
	def onPeerCommunicabilityChange(delegate: ParticipantDelegate, previousStatus: CommunicationStatus): Unit

	/** @param delegate the delegate associated with the updated peer. The implementation may assume that this delegate is associated. */
	def onPeerMembershipChange(delegate: ParticipantDelegate, previousStatus: Maybe[MembershipStatus]): Unit

//	/** Called just after a successful connection to the participant corresponding to the specified `delegate` when I am at the client side of the channel.
//	 * The implementation should transmit the conversation-opening message. */
//	def openConversationWith(participantDelegate: CommunicableDelegate, isReconnection: Boolean): Unit

	/**
	 * Handles an incoming `message` from the participant associated with the `delegate`, possibly deferring some reactions (e.g., queueing them for later sequential execution).
	 *
	 * The method must return *immediately* with a `Boolean` indicating whether the conversation should continue (`true`), or terminate (`false`).
	 * Note: While reactions to the message may be deferred (e.g., processed later in a strict order), the decision to continue/end the conversation must be made synchronously.
	 */
	def handleInitiatorMessageFrom(senderDelegate: CommunicableDelegate, initiationMsg: NonResponse): Boolean
}
