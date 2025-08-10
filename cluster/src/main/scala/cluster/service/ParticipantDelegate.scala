package readren.matrix
package cluster.service

import cluster.service.Protocol.*

import readren.sequencer.Maybe

/** A [[ParticipantService]]'s delegate responsible to manage the communication with other instances of [[ParticipantService]].
 * We name "participant" to each instance of [[ParticipantService]] */
abstract class ParticipantDelegate {
	/** The [[ParticipantService]] that this instance is a delegate of. */
	val owner: ParticipantService
	/** The [[ContactAddress]] of the participant this instance manages.  */
	val peerContactAddress: ContactAddress
	def communicationStatus: CommunicationStatus

	// TODO consider pushing down the following three variables into the CommunicableDelegate subclass to avoid remembering outdated state. 
	protected var oPeerMembershipStatusAccordingToMe: Maybe[MembershipStatus] = Maybe.empty
	inline def getPeerMembershipStatusAccordingToMe: Maybe[MembershipStatus] = oPeerMembershipStatusAccordingToMe
	
	protected var peerCreationInstant: Instant = UNSPECIFIED_INSTANT
	inline def getPeerCreationInstant: Instant = peerCreationInstant
	
	export owner.sequencer
	
	def isCommunicable: Boolean

	/** The communicability is stable: connection and handshaking have completed (successfully or not). */
	def isStable: Boolean

	def info: Maybe[ParticipantInfo] = oPeerMembershipStatusAccordingToMe.map(ParticipantInfo(communicationStatus, _))

	inline def initializeStateBasedOn(other: ParticipantDelegate): Unit = {
		this.oPeerMembershipStatusAccordingToMe = other.oPeerMembershipStatusAccordingToMe
		this.peerCreationInstant = other.peerCreationInstant
	}

	private[service] inline def isAssociated: Boolean = this eq owner.delegateByAddress.getOrElse(peerContactAddress, null)
	
	/** Removes this participant from outside this delegate's reception-cycle thread. */
	def removeByOther(): Unit
}

