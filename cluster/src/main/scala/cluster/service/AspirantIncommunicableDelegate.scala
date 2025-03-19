package readren.matrix
package cluster.service

/** A incommunicable participant's delegate suited for a [[ClusterService]] with an [[AspirantBehavior]]. */
class AspirantIncommunicableDelegate(clusterService: ClusterService) extends IncommunicableDelegate, AspirantDelegate(clusterService) {
	
}
