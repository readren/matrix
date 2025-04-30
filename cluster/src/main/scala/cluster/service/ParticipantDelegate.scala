package readren.matrix
package cluster.service

import cluster.service.Protocol.*
import cluster.service.Protocol.MembershipStatus.UNKNOWN

import readren.taskflow.Maybe

/** A [[ClusterService]]'s delegate responsible to manage the interaction with other instance of [[ClusterService]] hosted by other JVMs.
 * We name "participant" to each instance of [[ClusterService]] */
abstract class ParticipantDelegate {
	val clusterService: ClusterService
	val peerAddress: ContactAddress
	def communicationStatus: CommunicationStatus
	def info: ParticipantInfo

	// TODO push down the following three variables into the CommunicableDelegate subclass to avoid remembering outdated state. 
	protected[service] var versionsSupportedByPeer: Set[ProtocolVersion] = Set.empty
	protected[service] var peerMembershipStatusAccordingToMe: MembershipStatus = UNKNOWN
	protected[service] var peerCreationInstant: Instant = UNSPECIFIED_INSTANT  
	
	export clusterService.sequencer
	
	def isCommunicable: Boolean

	/** The communicability is stable: connection and handshaking have completed (successfully or not). */
	def isStable: Boolean
	
	inline def initializeStateBasedOn(other: ParticipantDelegate): Unit = {
		this.versionsSupportedByPeer = other.versionsSupportedByPeer
		this.peerMembershipStatusAccordingToMe = other.peerMembershipStatusAccordingToMe
		this.peerCreationInstant = other.peerCreationInstant
	}
}

