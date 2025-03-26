package readren.matrix
package cluster.service

import cluster.service.ClusterService.TaskSequencer
import cluster.service.Protocol.*
import cluster.service.Protocol.MembershipStatus.UNKNOWN

/** A [[ClusterService]]'s delegate responsible to manage the interaction with other instance of [[ClusterService]] hosted by other JVMs.
 * We name "participant" to each instance of [[ClusterService]] */
sealed abstract class ParticipantDelegate {
	val clusterService: ClusterService
	val peerAddress: ContactAddress
	protected[service] var versionsSupportedByPeer: Set[ProtocolVersion] = Set.empty
	protected[service] var peerMembershipStatusAccordingToMe: MembershipStatus = UNKNOWN
	export clusterService.sequencer

	inline def initializeState(versionsSupportedByPeer: Set[ProtocolVersion], peerMembershipStatusAccordingToMe: MembershipStatus): Unit = {
		this.versionsSupportedByPeer = versionsSupportedByPeer
		this.peerMembershipStatusAccordingToMe = peerMembershipStatusAccordingToMe
	}
	
	inline def initializeStateBasedOn(other: ParticipantDelegate): Unit = {
		this.versionsSupportedByPeer = other.versionsSupportedByPeer
		this.peerMembershipStatusAccordingToMe = other.peerMembershipStatusAccordingToMe
	}
}

trait AspirantDelegate extends ParticipantDelegate

trait MemberDelegate extends ParticipantDelegate


