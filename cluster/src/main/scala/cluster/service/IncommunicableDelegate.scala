package readren.matrix
package cluster.service

import cluster.service.Protocol.ContactAddress

import java.nio.channels.AsynchronousSocketChannel

/** A [[ParticipantDelegate]] that is currently not able to communicate with the participant.
 * Note that participants to which the [[ClusterService]] is connecting to are associated to a [[Incommunicable]] delegate with the [[isConnectingAsClient]] flag set, until the connection is completed; moment at which the delegate is replaced with a [[CommunicableDelegate]] one. */
class IncommunicableDelegate(override val clusterService: ClusterService, override val peerAddress: ContactAddress, isConnectingInPrinciple: Boolean) extends ParticipantDelegate {
	private var isTryingToConnect = isConnectingInPrinciple

	override def isCommunicable: Boolean = false

	override def isStable: Boolean = !isConnectingAsClient

	/** Tells that this incommunicable delegate is associated to a participant to which the [[ClusterService]] is connecting to. */
	def isConnectingAsClient: Boolean = isTryingToConnect

	/** Clears the [[isConnectingAsClient]] flag. */
	def connectionAborted(): Unit = isTryingToConnect = false

	def replaceMyselfWithACommunicableDelegate(communicationChannel: AsynchronousSocketChannel): CommunicableDelegate = {
		// replace me with a communicable delegate.
		clusterService.removeDelegate(peerAddress)
		val myReplacement = clusterService.createAndAddACommunicableDelegate(peerAddress, communicationChannel)
		myReplacement.initializeStateBasedOn(this)
		// notify
		clusterService.onDelegateBecomeCommunicable(peerAddress)
		myReplacement
	}
}
