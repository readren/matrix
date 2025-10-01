package readren.nexus
package cluster.service

import cluster.serialization.ProtocolVersion
import cluster.service.Protocol.{ClusterId, ContactAddress, IncommunicabilityReason, MembershipStatus}
import cluster.service.behavior.{AspirantBehavior, FunctionalBehavior, IsolatedBehavior, MemberBehavior}

sealed trait ParticipantServiceEvent


/** Tells that the participant at the specified address and me cannot communicate becase we don't support a common [[ProtocolVersion]]. */
case class VersionIncompatibilityWith(participantAddress: ContactAddress) extends ParticipantServiceEvent

case class MemberHasBeenRebooted(rebootedMemberAddress: ContactAddress) extends ParticipantServiceEvent

case class ParticipantHasGone(participant: ContactAddress) extends ParticipantServiceEvent

case class IStartedAConnectionToANewParticipant(participantAddress: ContactAddress) extends ParticipantServiceEvent

case class IJoinedTheCluster(participantDelegateByContactAddress: Map[ContactAddress, ParticipantDelegate]) extends ParticipantServiceEvent

case class OtherMemberJoined(memberAddress: ContactAddress, memberDelegate: MemberBehavior) extends ParticipantServiceEvent

case class CommunicationLostWith(peerAddress: ContactAddress, reason: IncommunicabilityReason, cause: Any) extends ParticipantServiceEvent

case class UnableToConnectTo(peerAddress: ContactAddress, cause: Any) extends ParticipantServiceEvent

case class AConversationStartedWith(peerAddress: ContactAddress, peerMembershipStatus: MembershipStatus, isARestartAfterReconnection: Boolean) extends ParticipantServiceEvent

case class CommunicationChannelReplaced(peerAddress: ContactAddress) extends ParticipantServiceEvent

case class IAmGoingToCloseAllChannels() extends ParticipantServiceEvent

case class IBecomeIsolated(operator: IsolatedOperator) extends ParticipantServiceEvent

case class IBecomeConflicted(operator: ConflictedOperator, isIsolated: Boolean, why: String) extends ParticipantServiceEvent

case class AMemberStateChanged(peerAddress: ContactAddress, what: ClusterStateChanged) extends ParticipantServiceEvent

trait IsolatedOperator {
	def becomeFunctional(): FunctionalBehavior
}

trait ConflictedOperator {
	def becomeAspirant(): AspirantBehavior
	def migrateTo(clusterId: ClusterId): FunctionalBehavior | IsolatedBehavior
}
