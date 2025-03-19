package readren.matrix
package cluster.service

import cluster.service.ClusterService.TaskSequencer
import cluster.service.Protocol.{ContactAddress, MembershipStatus}

import java.util.concurrent.TimeUnit

object ParticipantDelegate {
	class Config(val versionsSupportedByMe: Set[ProtocolVersion], val receiverTimeout: Long = 1, val transmitterTimeout: Long = 1, val timeUnit: TimeUnit = TimeUnit.SECONDS)
}


sealed trait ParticipantDelegate {
	val clusterService: ClusterService
	export clusterService.sequencer
	protected[service] var peerMembershipStatusAccordingToMe: MembershipStatus | Null = null
	protected[service] var versionsSupportedByPeer: Set[ProtocolVersion] = Set.empty
}

trait AspirantDelegate(override val clusterService: ClusterService) extends ParticipantDelegate

trait MemberDelegate(override val clusterService: ClusterService) extends ParticipantDelegate

abstract class CommunicableDelegate(override val peerRemoteAddress: ContactAddress, config: ParticipantDelegate.Config) extends Communicable(peerRemoteAddress, config), ParticipantDelegate

abstract class IncommunicableDelegate extends Incommunicable, ParticipantDelegate