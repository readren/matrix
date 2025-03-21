package readren.matrix
package cluster.service

import cluster.service.Protocol.ContactAddress

/** A incommunicable participant's delegate suited for a [[ClusterService]] with a [[MemberBehavior]]. */
class MemberIncommunicableDelegate(override val clusterService: ClusterService, peerAddress: ContactAddress, isConnectingInPrinciple: Boolean) extends MemberDelegate, Incommunicable(peerAddress) {
	private var isTryingToConnect = isConnectingInPrinciple

	override def isConnecting: Boolean = isTryingToConnect

	override def connectionAborted(): Unit = isTryingToConnect = false
}
