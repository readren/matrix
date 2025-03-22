package readren.matrix
package cluster.service

import cluster.service.ClusterService.TaskSequencer
import cluster.service.Protocol.*

import java.util.concurrent.TimeUnit

object ParticipantDelegate {
	class Config(val versionsSupportedByMe: Set[ProtocolVersion], val receiverTimeout: Long = 1, val transmitterTimeout: Long = 1, val timeUnit: TimeUnit = TimeUnit.SECONDS)
}

/** A [[ClusterService]]'s delegate responsible to manage the interaction with other instance of [[ClusterService]] hosted by other JVMs.
 * We name "participant" to each instance of [[ClusterService]] */
sealed abstract class ParticipantDelegate {
	val clusterService: ClusterService
	val peerAddress: ContactAddress
	protected[service] var versionsSupportedByPeer: Set[ProtocolVersion] = Set.empty
	protected[service] var peerMembershipStatusAccordingToMe: MembershipStatus | Null = null
	export clusterService.sequencer
}

trait AspirantDelegate extends ParticipantDelegate

trait MemberDelegate extends ParticipantDelegate


