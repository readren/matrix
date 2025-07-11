package readren.matrix
package cluster.service

import cluster.serialization.ProtocolVersion
import cluster.service.Protocol.{ContactAddress, IncommunicabilityReason}

sealed trait ParticipantServiceEvent


/** Tells that the participant at the specified address and me cannot communicate becase we don't support a common [[ProtocolVersion]]. */
case class VersionIncompatibilityWith(participantAddress: ContactAddress) extends ParticipantServiceEvent

case class MemberHasBeenRebooted(rebootedMemberAddress: ContactAddress) extends ParticipantServiceEvent

case class ParticipantHasGone(participant: ContactAddress) extends ParticipantServiceEvent

case class IStartedAConnectionToANewParticipant(participantAddress: ContactAddress) extends ParticipantServiceEvent

case class IJoinedTheCluster(participantDelegateByContactAddress: Map[ContactAddress, ParticipantDelegate]) extends ParticipantServiceEvent

case class OtherMemberJoined(memberAddress: ContactAddress, memberDelegate: MemberBehavior) extends ParticipantServiceEvent

case class DelegateBecomeIncommunicable(peerAddress: ContactAddress, reason: IncommunicabilityReason, cause: Any) extends ParticipantServiceEvent

case class DelegateBecomeUnreachable(peerAddress: ContactAddress, cause: Any) extends ParticipantServiceEvent

case class DelegateStartedConversationWith(peerAddress: ContactAddress, isARestartAfterReconnection: Boolean) extends ParticipantServiceEvent

case class CommunicationChannelReplaced(peerAddress: ContactAddress) extends ParticipantServiceEvent

case class IAmGoingToCloseAllChannels() extends ParticipantServiceEvent
