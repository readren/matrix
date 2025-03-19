package readren.matrix
package cluster.service

/** A incommunicable participant's delegate suited for a [[ClusterService]] with a [[MemberBehavior]]. */
class MemberIncommunicableDelegate(override val clusterService: ClusterService) extends MemberDelegate, Incommunicable
