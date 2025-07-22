package readren.matrix
package cluster.service.behavior

import cluster.service.Protocol.{ClusterId, CommunicationStatus, Conflicted, Instant, Member, MembershipStatus}
import cluster.service.*

import readren.taskflow.Maybe

import scala.collection.mutable

class ConflictedBehavior(override val host: ParticipantService, startingStateSerial: RingSerial, override val myClusterId: ClusterId, otherClustersIds: Set[ClusterId], override val isIsolated: Boolean, why: String) extends MemberBehavior(host, startingStateSerial, myClusterId) {

	class CompetingCluster

	private val otherClustersById: mutable.Map[ClusterId, CompetingCluster] = {
		val map = mutable.Map.empty[ClusterId, CompetingCluster]
		for foreignClusterId <- otherClustersIds do map.put(foreignClusterId, new CompetingCluster)
		map
	}

	override def membershipStatus: Conflicted =	Conflicted(myClusterId, otherClustersById.keySet.toSet, isIsolated)
	
	override def onPeerAdded(delegate: ParticipantDelegate): Unit = {
		stateSerial = stateSerial.nextSerial
		// TODO
	}

	override def onPeerCommunicabilityChange(delegate: ParticipantDelegate, previousStatus: CommunicationStatus): Unit = {
		stateSerial = stateSerial.nextSerial
		// TODO
	}

	override def onPeerMembershipChange(delegate: ParticipantDelegate, previousStatus: Maybe[MembershipStatus]): Unit = {
		stateSerial = stateSerial.nextSerial
		// TODO
	}

	override protected def onHello(senderDelegate: CommunicableDelegate, hello: Hello): Boolean = ???

	override protected def onMessage(senderDelegate: CommunicableDelegate, message: ICreatedACluster): Boolean = ???

	override protected def onMessage(senderDelegate: CommunicableDelegate, message: RequestToJoin): Unit = {
		senderDelegate.transmitToPeerOrRestartChannel(JoinRejected(message.requestId, false, false, false, isIsolated, true))
	}

	override protected def onMessage(senderDelegate: CommunicableDelegate, message: WeAreIsolated): Unit = ???

	override protected def onMessage(senderDelegate: CommunicableDelegate, isolated: AreWeIsolated): Unit = ???

	object ConflictedOperatorImpl extends ConflictedOperator {
		override def becomeAspirant(): AspirantBehavior = ???

		override def migrateTo(clusterId: ClusterId): FunctionalBehavior | IsolatedBehavior = ???
	}

}
