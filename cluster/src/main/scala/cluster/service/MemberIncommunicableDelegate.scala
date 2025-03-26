package readren.matrix
package cluster.service

import cluster.service.Protocol.ContactAddress

/** A incommunicable participant's delegate suited for a [[ClusterService]] with a [[MemberBehavior]]. */
class MemberIncommunicableDelegate(override val clusterService: ClusterService, override val peerAddress: ContactAddress, isConnectingInPrinciple: Boolean) extends MemberDelegate, Incommunicable {
	private var isTryingToConnect = isConnectingInPrinciple

	override def isConnectingAsClient: Boolean = isTryingToConnect

	override def connectionAborted(): Unit = isTryingToConnect = false
}
