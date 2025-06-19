package readren.matrix
package cluster.service

import cluster.service.Protocol.*

import readren.taskflow.Maybe

/** A [[ClusterService]]'s delegate responsible to manage the communication with other instances of [[ClusterService]].
 * We name "participant" to each instance of [[ClusterService]] */
abstract class ParticipantDelegate {
	/** The [[ClusterService]] that this instance is a delegate of. */
	val clusterService: ClusterService
	/** The [[ContactAddress]] of the participant this instance manages.  */
	val peerAddress: ContactAddress
	def communicationStatus: CommunicationStatus
	def info: Maybe[ParticipantInfo]

	// TODO consider pushing down the following three variables into the CommunicableDelegate subclass to avoid remembering outdated state. 
	protected[service] var oPeerMembershipStatusAccordingToMe: Maybe[MembershipStatus] = Maybe.empty
	protected[service] var peerCreationInstant: Instant = UNSPECIFIED_INSTANT  
	
	export clusterService.sequencer
	
	def isCommunicable: Boolean

	/** The communicability is stable: connection and handshaking have completed (successfully or not). */
	def isStable: Boolean
	
	inline def initializeStateBasedOn(other: ParticipantDelegate): Unit = {
		this.oPeerMembershipStatusAccordingToMe = other.oPeerMembershipStatusAccordingToMe
		this.peerCreationInstant = other.peerCreationInstant
	}
}

