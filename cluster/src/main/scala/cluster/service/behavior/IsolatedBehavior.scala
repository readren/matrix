package readren.matrix
package cluster.service.behavior

import cluster.service.Protocol.*
import cluster.service.*

class IsolatedBehavior(participantService: ParticipantService, startingStateSerial: RingSerial, clusterId: ClusterId) extends MemberBehavior(participantService, startingStateSerial, clusterId) {


	override val membershipStatus: Protocol.MembershipStatus =
		Isolated(clusterId)

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
		senderDelegate.transmitToPeerOrRestartChannel(JoinRejected(message.requestId, false, false, false, true, false))
	}

	object IsolatedOperatorImpl extends IsolatedOperator {
		override def becomeFunctional(): FunctionalBehavior = {
			// TODO
			???
		}
	}

}
