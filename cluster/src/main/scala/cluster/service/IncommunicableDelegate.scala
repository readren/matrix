package readren.matrix
package cluster.service

import cluster.service.Protocol.CommunicationStatus.{CONNECTING, INCOMPATIBLE, UNREACHABLE}
import cluster.service.Protocol.{CommunicationStatus, ContactAddress, IncommunicabilityReason, ParticipantInfo}

import readren.matrix.cluster.service.Protocol.IncommunicabilityReason.{IS_CONNECTING_AS_CLIENT, IS_INCOMPATIBLE}

import java.nio.channels.AsynchronousSocketChannel

/** A [[ParticipantDelegate]] that is currently not able to communicate with the participant.
 * Note that participants to which the [[ClusterService]] is connecting to are associated to a [[Incommunicable]] delegate with the [[isConnectingAsClient]] flag set, until the connection is completed; moment at which the delegate is replaced with a [[CommunicableDelegate]] one. */
class IncommunicableDelegate(override val clusterService: ClusterService, override val peerAddress: ContactAddress, reason: IncommunicabilityReason) extends ParticipantDelegate {
	val isIncompatible: Boolean = reason eq IS_INCOMPATIBLE
	private var isTryingToConnectAsClient = reason eq IS_CONNECTING_AS_CLIENT

	override def isCommunicable: Boolean = false

	override def isStable: Boolean = !isTryingToConnectAsClient
	
	inline def isUnreachable: Boolean = !isIncompatible && !isTryingToConnectAsClient

	/** Tells that this incommunicable delegate is associated to a participant to which the [[ClusterService]] is connecting to. */
	inline def isConnectingAsClient: Boolean = isTryingToConnectAsClient

	/** Clears the [[isConnectingAsClient]] flag. */
	inline def onConnectionAborted(cause: Throwable): Unit = {
		isTryingToConnectAsClient = false
		clusterService.getMembershipScopedBehavior.onDelegateCommunicabilityChange(this)
		clusterService.notifyListenersThat(DelegateBecomeUnreachable(peerAddress, cause))
	}

	override def communicationStatus: CommunicationStatus = {
		if isIncompatible then INCOMPATIBLE
		else if isTryingToConnectAsClient then CONNECTING
		else UNREACHABLE
	}

	override def info: ParticipantInfo = 
		ParticipantInfo(versionsSupportedByPeer, communicationStatus, peerMembershipStatusAccordingToMe)
	

	def replaceMyselfWithACommunicableDelegate(communicationChannel: AsynchronousSocketChannel): CommunicableDelegate = {
		// replace me with a communicable delegate.
		clusterService.removeDelegate(peerAddress)
		val myReplacement = clusterService.addANewCommunicableDelegate(peerAddress, communicationChannel)
		myReplacement.initializeStateBasedOn(this)
		// notify
		clusterService.getMembershipScopedBehavior.onDelegateCommunicabilityChange(myReplacement)
		myReplacement
	}
}
