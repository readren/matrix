package readren.matrix
package cluster.service

import cluster.channel.{Receiver, Transmitter}
import cluster.service.Protocol.*

import java.net.SocketAddress
import java.nio.channels.AsynchronousSocketChannel

sealed trait Communicability {
	def isCommunicable: Boolean
}

abstract class Incommunicable extends Communicability { thisIncomunicable: ParticipantDelegate =>
	override def isCommunicable: Boolean = false
}

abstract class Communicable(val peerRemoteAddress: SocketAddress, config: ParticipantDelegate.Config) extends Communicability { thisCommunicable: ParticipantDelegate =>
	protected var peerChannel: AsynchronousSocketChannel = null
	protected var receiverFromPeer: Receiver = null
	protected var transmitterToPeer: Transmitter = null
	protected var agreedVersion: ProtocolVersion = ProtocolVersion.OF_THIS_PROJECT

	override def isCommunicable: Boolean = true
	
	protected def startReceiving(): Unit

	def startAsServer(peerChannel: AsynchronousSocketChannel): Unit = {
		assert(this.peerChannel == null)
		this.peerChannel = peerChannel
		receiverFromPeer = new Receiver(peerChannel)
		transmitterToPeer = new Transmitter(peerChannel)
		startReceiving()
	}

	def startAsClient(myAddress: ContactAddress, peerChannel: AsynchronousSocketChannel): Unit = {
		assert(this.peerChannel == null)
		this.peerChannel = peerChannel
		this.receiverFromPeer = new Receiver(peerChannel)
		this.transmitterToPeer = new Transmitter(peerChannel)
		this.transmitterToPeer.transmit[Protocol](Hello(myAddress, config.versionsSupportedByMe, clusterService.getKnownParticipantsCards.toMap), ProtocolVersion.OF_THIS_PROJECT, config.transmitterTimeout, config.timeUnit) {
			case Transmitter.Delivered =>
				startReceiving()
			case failure =>
				reportTransmissionFailure(failure)
		}
	}

	protected def determineAgreedVersion(versionsSupportedByMe: Set[ProtocolVersion], versionsSupportedByPeer: Set[ProtocolVersion]): Option[ProtocolVersion] = {
		val versionsSupportedByBoth = versionsSupportedByMe.intersect(versionsSupportedByPeer)
		versionsSupportedByBoth.find(candidate => versionsSupportedByBoth.forall(rival => candidate == rival || candidate.isNewerThan(rival)))
	}

	protected def reportTransmissionFailure(report: Transmitter.Report): Unit = {
		report match {
			case Transmitter.Delivered => // do nothing

			case transmissionFailure: Transmitter.TransmissionFailure =>
				scribe.error(s"A transmission failure occurred while transmitting `${transmissionFailure.rootMessage}` to the channel `$peerChannel`", transmissionFailure.cause)
				tryToReconnect()
			case serializationFailure: Transmitter.SerializationUnsupported =>
				scribe.error(s"A serialization failure occurred at position ${serializationFailure.position} while transmitting `${serializationFailure.rootMessage}` to the channel `$peerChannel` ${if serializationFailure.aFragmentWasTransmitted then "after some bytes were transmitted" else "before any byte was transmitted"}: ${serializationFailure.reason} ")
				tryToReconnect()
		}
	}

	protected def handle(hello: Hello): Unit = {
		// update my viewpoint of the peer's membership. 
		peerMembershipStatusAccordingToMe match {
			case null =>
				peerMembershipStatusAccordingToMe = Aspirant
			case Aspirant =>
				peerMembershipStatusAccordingToMe = Aspirant
				clusterService.notifyParticipantHasBeenRestarted(peerRemoteAddress)
			case Member =>
				peerMembershipStatusAccordingToMe = Aspirant
				clusterService.notifyParticipantHasBeenRestarted(peerRemoteAddress)
		}
		versionsSupportedByPeer = hello.versionsISupport

		// Connect to participants I didn't know.
		for participantCard <- hello.cardsOfOtherParticipantsIKnow do {
			if participantCard.address != clusterService.myAddress && !clusterService.participantByAddress.contains(participantCard.address) then {
				determineAgreedVersion(config.versionsSupportedByMe, participantCard.supportedVersions) match {
					case Some(version) => clusterService.startConnectionTo(participantCard.address)
					case None => clusterService.notifyVersionIncompatibilityWith(participantCard.address)
				}
			}
		}

		// update the protocol-version to use when communicating with the peer and respond the hello message.
		determineAgreedVersion(config.versionsSupportedByMe, hello.versionsISupport) match {
			case Some(version) =>
				agreedVersion = version

				if clusterService.doesAClusterExist then {
					transmitterToPeer.transmit[Protocol](NoClusterIAmAwareOf(clusterService.getKnownParticipantsCards.toMap), agreedVersion, config.transmitterTimeout, config.timeUnit)(reportTransmissionFailure)
				} else {
					transmitterToPeer.transmit[Protocol](JoinApprovalMembers(clusterService.getKnownMembersCards.toMap), agreedVersion, config.transmitterTimeout, config.timeUnit)(reportTransmissionFailure)
				}

			case None =>
				transmitterToPeer.transmit[Protocol](SupportedVersionsMismatch, ProtocolVersion.OF_THIS_PROJECT, config.transmitterTimeout, config.timeUnit) {
					case Transmitter.Delivered =>
						clusterService.notifyVersionIncompatibilityWith(peerRemoteAddress)
						closeChannel()
					case issue =>
						reportTransmissionFailure(issue)
						clusterService.notifyVersionIncompatibilityWith(peerRemoteAddress)
						closeChannel()
				}

		}
	}


	protected[service] def handleReconnection(newChannel: AsynchronousSocketChannel): MemberCommunicableDelegate = ???

	protected[service] def notifyOtherParticipantChannelHasBeenClosed(otherParticipantAddress: ContactAddress): Unit = ???

	protected def tryToReconnect(): Unit = ???

	protected[service] def closeChannel(): Unit = ???
	
}
