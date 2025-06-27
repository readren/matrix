package readren.matrix
package cluster.service

import cluster.serialization.ProtocolVersion
import cluster.service.Protocol.{ContactAddress, IncommunicabilityReason}

sealed trait ClusterServiceEvent

/** Tells that the participant at the specified address and me cannot communicate becase we don't support a common [[ProtocolVersion]]. */
case class VersionIncompatibilityWith(participantAddress: ContactAddress) extends ClusterServiceEvent

case class MemberHasBeenRebooted(rebootedMemberAddress: ContactAddress) extends ClusterServiceEvent

case class ParticipantHasGone(participant: ContactAddress) extends ClusterServiceEvent

case class IStartedAConnectionToANewParticipant(participantAddress: ContactAddress) extends ClusterServiceEvent

case class IJoinedTheCluster(participantDelegateByContactAddress: Map[ContactAddress, ParticipantDelegate]) extends ClusterServiceEvent

case class OtherMemberJoined(memberAddress: ContactAddress, memberDelegate: MemberBehavior) extends ClusterServiceEvent

case class DelegateBecomeIncommunicable(peerAddress: ContactAddress, reason: IncommunicabilityReason, cause: Any) extends ClusterServiceEvent

case class DelegateBecomeUnreachable(peerAddress: ContactAddress, cause: Any) extends ClusterServiceEvent

case class DelegateStartedConversationWith(peerAddress: ContactAddress, isARestartAfterReconnection: Boolean) extends ClusterServiceEvent

case class CommunicationChannelReplaced(peerAddress: ContactAddress) extends ClusterServiceEvent

case class IAmGoingToCloseAllChannels() extends ClusterServiceEvent
