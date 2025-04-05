package readren.matrix
package cluster.service

import cluster.service.ClusterService.DelegateBecomeUnreachable
import cluster.service.IncommunicableDelegate.Reason
import cluster.service.IncommunicableDelegate.Reason.{IS_CONNECTING_AS_CLIENT, IS_INCOMPATIBLE}
import cluster.service.Protocol.CommunicationStatus.{CONNECTING, INCOMPATIBLE, UNREACHABLE}
import cluster.service.Protocol.{CommunicationStatus, ContactAddress, ParticipantInfo}

import java.nio.channels.AsynchronousSocketChannel

object IncommunicableDelegate {
	enum Reason {
		case IS_CONNECTING_AS_CLIENT, IS_INCOMPATIBLE
	}
}

/** A [[ParticipantDelegate]] that is currently not able to communicate with the participant.
 * Note that participants to which the [[ClusterService]] is connecting to are associated to a [[Incommunicable]] delegate with the [[isConnectingAsClient]] flag set, until the connection is completed; moment at which the delegate is replaced with a [[CommunicableDelegate]] one. */
class IncommunicableDelegate(override val clusterService: ClusterService, override val peerAddress: ContactAddress, reason: Reason) extends ParticipantDelegate {
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
		clusterService.getCommunicableDelegatesBehavior.onDelegateCommunicabilityChange(this)
		clusterService.notify(DelegateBecomeUnreachable(peerAddress, cause))
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
		clusterService.getCommunicableDelegatesBehavior.onDelegateCommunicabilityChange(myReplacement)
		clusterService.onDelegateBecomeCommunicable(peerAddress)
		myReplacement
	}
}
