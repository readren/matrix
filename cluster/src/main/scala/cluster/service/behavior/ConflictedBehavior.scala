package readren.matrix
package cluster.service.behavior

import cluster.service.Protocol.{ClusterId, Conflicted, Instant, Member}
import cluster.service.*

import scala.collection.mutable

class ConflictedBehavior(participantService: ParticipantService, startingStateSerial: RingSerial, clusterId: ClusterId, otherClustersIds: Set[ClusterId], wasIsolated: Boolean, why: String) extends MemberBehavior(participantService, startingStateSerial, clusterId) {

	class CompetingCluster

	private val otherClustersById: mutable.Map[ClusterId, CompetingCluster] = {
		val map = mutable.Map.empty[ClusterId, CompetingCluster]
		for foreignClusterId <- otherClustersIds do map.put(foreignClusterId, new CompetingCluster)
		map
	}

	override val membershipStatus: Protocol.MembershipStatus =
		Conflicted(clusterId, otherClustersById.keySet.toSet, wasIsolated)

	
	participantService.notifyListenersThat(IBecomeConflicted(ConflictedOperatorImpl, why))
	

	override def onDelegatedAdded(delegate: ParticipantDelegate): Unit = {
		stateSerial = stateSerial.nextSerial
		// TODO
	}

	override def onDelegateCommunicabilityChange(delegate: ParticipantDelegate): Unit = {
		stateSerial = stateSerial.nextSerial
		// TODO
	}

	override def onDelegateMembershipChange(delegate: ParticipantDelegate): Unit = {
		stateSerial = stateSerial.nextSerial
		// TODO
	}

	override protected def onHello(senderDelegate: CommunicableDelegate, hello: Hello): Boolean = ???

	override protected def onMessage(senderDelegate: CommunicableDelegate, message: ICreatedACluster): Boolean = ???

	override protected def onMessage(senderDelegate: CommunicableDelegate, message: RequestToJoin): Unit = {
		senderDelegate.transmitToPeerOrRestartChannel(JoinRejected(message.requestId, false, false, false, wasIsolated, true))
	}

	object ConflictedOperatorImpl extends ConflictedOperator {
		override def becomeAspirant(): AspirantBehavior = ???

		override def migrateTo(clusterId: ClusterId): FunctionalBehavior | IsolatedBehavior = ???
	}

}
