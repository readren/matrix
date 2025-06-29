package readren.matrix
package cluster.service

import cluster.service.ClusterService.ChannelOrigin
import cluster.service.Protocol.CommunicationStatus.{CONNECTING, INCOMPATIBLE, UNREACHABLE}
import cluster.service.Protocol.IncommunicabilityReason.{IS_CONNECTING_AS_CLIENT, IS_INCOMPATIBLE}
import cluster.service.Protocol.{CommunicationStatus, ContactAddress, IncommunicabilityReason, ParticipantInfo}

import readren.matrix.cluster.channel.Receiver
import readren.taskflow.Maybe

import java.net.SocketAddress
import java.nio.channels.AsynchronousSocketChannel

/** A [[ParticipantDelegate]] that is currently not able to communicate with the participant.
 * Note that participants to which the [[ClusterService]] is connecting to are associated to a [[Incommunicable]] delegate with the [[isConnectingAsClient]] flag set. When the connection is completed the [[ClusterService]] replaces it with a [[CommunicableDelegate]] one by calling the [[replaceMyselfWithACommunicableDelegate]] method. */
class IncommunicableDelegate(override val clusterService: ClusterService, override val peerContactAddress: ContactAddress, reason: IncommunicabilityReason) extends ParticipantDelegate {
	var isIncompatible: Boolean = reason eq IS_INCOMPATIBLE
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
		clusterService.notifyListenersThat(DelegateBecomeUnreachable(peerContactAddress, cause))
	}

	override def communicationStatus: CommunicationStatus = {
		if isIncompatible then INCOMPATIBLE
		else if isTryingToConnectAsClient then CONNECTING
		else UNREACHABLE
	}

	def tryToConnect(): Unit = {
		isIncompatible = false
		isTryingToConnectAsClient = true
		clusterService.connectToAndThenStartConversationWithParticipant(this, false)
	}
	
	def replaceMyselfWithACommunicableDelegate(channel: AsynchronousSocketChannel, receiverFromPeer: Receiver, oPeerRemoteAddress: Maybe[SocketAddress], channelOrigin: ChannelOrigin): Maybe[CommunicableDelegate] = {
		// replace me with a communicable delegate.
		if clusterService.removeDelegate(this, false) then {
			val myReplacement = clusterService.addANewCommunicableDelegate(peerContactAddress, channel, receiverFromPeer, oPeerRemoteAddress, channelOrigin)
			myReplacement.initializeStateBasedOn(this)
			// notify
			clusterService.getMembershipScopedBehavior.onDelegateCommunicabilityChange(myReplacement)
			Maybe.some(myReplacement)
		} else Maybe.empty
	}
}
