package readren.matrix
package cluster.service.behavior

import cluster.service.*
import cluster.service.Protocol.*

import readren.common.Maybe

class IsolatedBehavior(override val host: ParticipantService, startingStateSerial: RingSerial, override val myClusterId: ClusterId) extends MemberBehavior(host, startingStateSerial, myClusterId) {

	override val membershipStatus: Isolated = Isolated(myClusterId)

	override def isIsolated = true
	
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
		senderDelegate.transmitToPeerOrRestartChannel(JoinRejected(message.requestId, false, false, false, true, false))
	}

	override protected def onMessage(senderDelegate: CommunicableDelegate, isolated: AreWeIsolated): Unit = ???

	override protected def onMessage(senderDelegate: CommunicableDelegate, message: WeAreIsolated): Unit = ???

	object IsolatedOperatorImpl extends IsolatedOperator {
		override def becomeFunctional(): FunctionalBehavior = {
			// TODO
			???
		}
	}

}
