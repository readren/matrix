package readren.matrix
package cluster.service

import cluster.service.Protocol.{ContactAddress, IncommunicabilityReason}

sealed trait ClusterEvent

/** Tells that the participant at the specified address and me can not communicate becase we don't support a common [[ProtocolVersion]]. */
case class VersionIncompatibilityWith(participantAddress: ContactAddress) extends ClusterEvent

case class ParticipantHasBeenRestarted(rebornParticipantAddress: ContactAddress) extends ClusterEvent

case class ParticipantHasGone(participant: ContactAddress) extends ClusterEvent

case class IStartedAConnectionToANewParticipant(participantAddress: ContactAddress) extends ClusterEvent

case class IJoinedTheCluster(participantDelegateByContactAddress: Map[ContactAddress, ParticipantDelegate]) extends ClusterEvent

case class OtherMemberJoined(memberAddress: ContactAddress, memberDelegate: MemberBehavior) extends ClusterEvent

case class DelegateBecomeIncommunicable(peerAddress: ContactAddress, reason: IncommunicabilityReason, cause: Any) extends ClusterEvent

case class DelegateBecomeUnreachable(peerAddress: ContactAddress, cause: Any) extends ClusterEvent

case class DelegateStartedConversationWith(peerAddress: ContactAddress) extends ClusterEvent

case class CommunicationChannelReplaced(peerAddress: ContactAddress) extends ClusterEvent

case class IAmGoingToCloseAllChannels() extends ClusterEvent
