package readren.matrix
package cluster.service

import cluster.channel.Receiver
import cluster.service.Protocol.CommunicationStatus.{CONNECTING, INCOMPATIBLE, UNREACHABLE}
import cluster.service.Protocol.IncommunicabilityReason.{IS_CONNECTING_AS_CLIENT, IS_INCOMPATIBLE}
import cluster.service.Protocol.{CommunicationStatus, ContactAddress, IncommunicabilityReason}

import readren.taskflow.Maybe

import java.net.SocketAddress
import java.nio.channels.AsynchronousSocketChannel

/** A [[ParticipantDelegate]] that is currently not able to communicate with the participant.
 * Note that participants to which the [[ParticipantService]] is connecting to are associated to a [[Incommunicable]] delegate with the [[isConnectingAsClient]] flag set. When the connection is completed the [[ParticipantService]] replaces it with a [[CommunicableDelegate]] one by calling the [[replaceMyselfWithACommunicableDelegate]] method. */
class IncommunicableDelegate(override val participantService: ParticipantService, override val peerContactAddress: ContactAddress, reason: IncommunicabilityReason) extends ParticipantDelegate {
	var isIncompatible: Boolean = reason eq IS_INCOMPATIBLE
	private var isTryingToConnectAsClient = reason eq IS_CONNECTING_AS_CLIENT

	override def isCommunicable: Boolean = false

	override def isStable: Boolean = !isTryingToConnectAsClient
	
	inline def isUnreachable: Boolean = !isIncompatible && !isTryingToConnectAsClient

	/** Tells that this incommunicable delegate is associated to a participant to which the [[ParticipantService]] is connecting to. */
	inline def isConnectingAsClient: Boolean = isTryingToConnectAsClient

	/** Clears the [[isConnectingAsClient]] flag. */
	inline def onConnectionAborted(cause: Throwable): Unit = {
		isTryingToConnectAsClient = false
		participantService.getMembershipScopedBehavior.onDelegateCommunicabilityChange(this)
		participantService.notifyListenersThat(DelegateBecomeUnreachable(peerContactAddress, cause))
	}

	override def communicationStatus: CommunicationStatus = {
		if isIncompatible then INCOMPATIBLE
		else if isTryingToConnectAsClient then CONNECTING
		else UNREACHABLE
	}

	def tryToConnect(): Unit = {
		isIncompatible = false
		isTryingToConnectAsClient = true
		participantService.connectToAndThenStartConversationWithParticipant(this, false)
	}
	
	def replaceMyselfWithACommunicableDelegate(channel: AsynchronousSocketChannel, receiverFromPeer: Receiver, oPeerRemoteAddress: Maybe[SocketAddress], channelOrigin: ChannelOrigin): Maybe[CommunicableDelegate] = {
		// replace me with a communicable delegate.
		if participantService.removeDelegate(this, false) then {
			val myReplacement = participantService.addANewCommunicableDelegate(peerContactAddress, channel, receiverFromPeer, oPeerRemoteAddress, channelOrigin)
			myReplacement.initializeStateBasedOn(this)
			// notify
			participantService.getMembershipScopedBehavior.onDelegateCommunicabilityChange(myReplacement)
			Maybe.some(myReplacement)
		} else Maybe.empty
	}

	override def removeByOther(): Unit = {
		participantService.removeDelegate(this, true)
	}
}
