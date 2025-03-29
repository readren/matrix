package readren.matrix
package cluster.service

import cluster.service.ClusterService.TaskSequencer
import cluster.service.Protocol.*
import cluster.service.Protocol.MembershipStatus.UNKNOWN

/** A [[ClusterService]]'s delegate responsible to manage the interaction with other instance of [[ClusterService]] hosted by other JVMs.
 * We name "participant" to each instance of [[ClusterService]] */
abstract class ParticipantDelegate {
	val clusterService: ClusterService
	val peerAddress: ContactAddress
	protected[service] var versionsSupportedByPeer: Set[ProtocolVersion] = Set.empty
	protected[service] var peerMembershipStatusAccordingToMe: MembershipStatus = UNKNOWN
	export clusterService.sequencer
	
	inline def contactCard: ContactCard = (peerAddress, versionsSupportedByPeer)

	def isCommunicable: Boolean

	/** The communicability is stable: connection and handshaking have completed (successfully or not). */
	def isStable: Boolean
	
	inline def initializeState(versionsSupportedByPeer: Set[ProtocolVersion], peerMembershipStatusAccordingToMe: MembershipStatus): Unit = {
		this.versionsSupportedByPeer = versionsSupportedByPeer
		this.peerMembershipStatusAccordingToMe = peerMembershipStatusAccordingToMe
	}
	
	inline def initializeStateBasedOn(other: ParticipantDelegate): Unit = {
		this.versionsSupportedByPeer = other.versionsSupportedByPeer
		this.peerMembershipStatusAccordingToMe = other.peerMembershipStatusAccordingToMe
	}
}

