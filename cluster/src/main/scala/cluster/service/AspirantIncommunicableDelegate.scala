package readren.matrix
package cluster.service

import cluster.service.Protocol.ContactAddress

/** A incommunicable participant's delegate suited for a [[ClusterService]] with an [[AspirantBehavior]]. */
class AspirantIncommunicableDelegate(override val clusterService: ClusterService, peerAddress: ContactAddress, isConnectingInPrinciple: Boolean) extends AspirantDelegate, Incommunicable(peerAddress) {
	private var isTryingToConnect = isConnectingInPrinciple

	override def isConnecting: Boolean = isTryingToConnect

	override def connectionAborted(): Unit = isTryingToConnect = false
}
