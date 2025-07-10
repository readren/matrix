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
	val peerContactAddress: ContactAddress
	def communicationStatus: CommunicationStatus

	// TODO consider pushing down the following three variables into the CommunicableDelegate subclass to avoid remembering outdated state. 
	protected var oPeerMembershipStatusAccordingToMe: Maybe[MembershipStatus] = Maybe.empty
	inline def getPeerMembershipStatusAccordingToMe: Maybe[MembershipStatus] = oPeerMembershipStatusAccordingToMe
	
	protected var peerCreationInstant: Instant = UNSPECIFIED_INSTANT
	inline def getPeerCreationInstant: Instant = peerCreationInstant
	
	export clusterService.sequencer
	
	def isCommunicable: Boolean

	/** The communicability is stable: connection and handshaking have completed (successfully or not). */
	def isStable: Boolean

	def info: Maybe[ParticipantInfo] = oPeerMembershipStatusAccordingToMe.map(ParticipantInfo(communicationStatus, _))

	inline def initializeStateBasedOn(other: ParticipantDelegate): Unit = {
		this.oPeerMembershipStatusAccordingToMe = other.oPeerMembershipStatusAccordingToMe
		this.peerCreationInstant = other.peerCreationInstant
	}

	private[service] inline def isAssociated: Boolean = this eq clusterService.delegateByAddress.getOrElse(peerContactAddress, null)

	/** Removes this participant from outside this delegate's reception-cycle thread. */
	def removeByOther(): Unit
}

