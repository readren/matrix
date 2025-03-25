package readren.matrix
package cluster.service

import cluster.channel.{Receiver, Transmitter}
import cluster.service.ClusterService.DelegateConfig
import cluster.service.Protocol.*

import java.nio.channels.AsynchronousSocketChannel
import java.util.function.Consumer
import scala.util.{Failure, Success}

/** Categorizes subtypes of [[ParticipantDelegate]] into two groups, the ones that represent delegates that communicate to the peer from the ones that don't. */
sealed trait Communicability {
	def isCommunicable: Boolean
}

trait Incommunicable extends Communicability { thisCommunicable: ParticipantDelegate =>
	override def isCommunicable: Boolean = false
	
	def isConnecting: Boolean
	def connectionAborted(): Unit
	
	def replaceMyselfWithACommunicableDelegate(communicationChannel: AsynchronousSocketChannel): ParticipantDelegate & Communicable = {
		// replace me with a communicable delegate.
		clusterService.getBehavior.removeDelegate(peerAddress)
		val myReplacement = clusterService.getBehavior.createAndAddACommunicableDelegate(peerAddress, communicationChannel)
		myReplacement.initializeStateBasedOn(thisCommunicable)
		// notify
		clusterService.onDelegateBecomeCommunicable(peerAddress)
		myReplacement
	}

}

trait Communicable extends Communicability { thisCommunicable: ParticipantDelegate =>
	protected val peerChannel: AsynchronousSocketChannel
	val config: DelegateConfig
	protected val receiverFromPeer: Receiver = new Receiver(peerChannel)
	protected val transmitterToPeer: Transmitter = new Transmitter(peerChannel)
	protected var agreedVersion: ProtocolVersion = ProtocolVersion.OF_THIS_PROJECT

	override def isCommunicable: Boolean = true

	protected def startReceiving(): Unit


	def startConversationAsServer(): Unit = {
		startReceiving()
	}

	def startConversationAsClient(): Unit = {
		this.transmitterToPeer.transmit[Protocol](Hello(clusterService.myAddress, config.versionsSupportedByMe, clusterService.getKnownParticipantsCards.toMap), ProtocolVersion.OF_THIS_PROJECT, config.transmitterTimeout, config.timeUnit) {
			case Transmitter.Delivered =>
				startReceiving()
			case failure: Transmitter.NotDelivered =>
				reportFailure(failure)
		}
	}

	protected def determineAgreedVersion(versionsSupportedByMe: Set[ProtocolVersion], versionsSupportedByPeer: Set[ProtocolVersion]): Option[ProtocolVersion] = {
		val versionsSupportedByBoth = versionsSupportedByMe.intersect(versionsSupportedByPeer)
		versionsSupportedByBoth.find(candidate => versionsSupportedByBoth.forall(rival => candidate == rival || candidate.isNewerThan(rival)))
	}

	protected def reportFailure(failure: Transmitter.NotDelivered): Unit = {
		failure match {
			case transmissionFailure: Transmitter.TransmissionFailure =>
				scribe.error(s"A transmission failure occurred while transmitting `${transmissionFailure.rootMessage}` to the channel `$peerChannel`", transmissionFailure.cause)
			case serializationFailure: Transmitter.SerializationProblem =>
				scribe.error(s"A serialization failure occurred at position ${serializationFailure.problem.position} while transmitting `${serializationFailure.rootMessage}` to the channel `$peerChannel` ${if serializationFailure.aFragmentWasTransmitted then "after some bytes were transmitted" else "before any byte was transmitted"}", serializationFailure.problem)
		}
	}

	protected def ifFailureReportItAndThen(consumer: Consumer[Transmitter.NotDelivered])(report: Transmitter.Report): Unit = {
		report match {
			case Transmitter.Delivered => // do nothing

			case failure: Transmitter.NotDelivered =>
				reportFailure(failure)
				sequencer.executeSequentially(consumer.accept(failure))
		}
	}

	protected def handle(hello: Hello): Unit = {
		// update my viewpoint of the peer's membership.
		peerMembershipStatusAccordingToMe match {
			case null =>
				peerMembershipStatusAccordingToMe = Aspirant
			case Aspirant =>
				peerMembershipStatusAccordingToMe = Aspirant
				clusterService.notifyParticipantHasBeenRestarted(peerAddress)
			case Member =>
				peerMembershipStatusAccordingToMe = Aspirant
				clusterService.notifyParticipantHasBeenRestarted(peerAddress)
		}
		versionsSupportedByPeer = hello.versionsISupport

		// Connect to participants I didn't know.
		for participantCard <- hello.cardsOfOtherParticipantsIKnow do {
			if participantCard.address != clusterService.myAddress && !clusterService.participantByAddress.contains(participantCard.address) then {
				determineAgreedVersion(config.versionsSupportedByMe, participantCard.supportedVersions) match {
					case Some(version) => clusterService.createAndAddADelegateForAndThenConnectToParticipant(participantCard.address, participantCard.supportedVersions, Aspirant)
					case None => clusterService.notifyVersionIncompatibilityWith(participantCard.address)
				}
			}
		}

		// update the protocol-version to use when communicating with the peer, and transmit the response.
		determineAgreedVersion(config.versionsSupportedByMe, hello.versionsISupport) match {
			case Some(version) =>
				agreedVersion = version

				if clusterService.doesAClusterExist then {
					transmitterToPeer.transmit[Protocol](JoinApprovalMembers(clusterService.getKnownMembersCards.toMap), agreedVersion, config.transmitterTimeout, config.timeUnit)(ifFailureReportItAndThen(restartChannel))
				} else {
					transmitterToPeer.transmit[Protocol](NoClusterIAmAwareOf(clusterService.getKnownParticipantsCards.toMap), agreedVersion, config.transmitterTimeout, config.timeUnit)(ifFailureReportItAndThen(restartChannel))
				}

			case None =>
				val cause = s"None of the peer's supported versions according to his hello message are supported by me. Versions supported by peer: ${hello.versionsISupport}"
				transmitterToPeer.transmit[Protocol](SupportedVersionsMismatch, ProtocolVersion.OF_THIS_PROJECT, config.transmitterTimeout, config.timeUnit) {
					case Transmitter.Delivered =>
						clusterService.notifyVersionIncompatibilityWith(peerAddress)
						replaceMyselfWithAnIncommunicableDelegate(false, cause)
					case issue: Transmitter.NotDelivered =>
						reportFailure(issue)
						clusterService.notifyVersionIncompatibilityWith(peerAddress)
						replaceMyselfWithAnIncommunicableDelegate(false, cause)
				}

		}
	}


	protected[service] def handleReconnection(newChannel: AsynchronousSocketChannel): MemberCommunicableDelegate = ???

	protected[service] def notifyPeerThatILostCommunicationWithOtherPeer(otherParticipantAddress: ContactAddress): Unit = ???

	protected[service] def notifyPeerThatIRecoveredCommunicationWithOtherPeer(otherParticipantAddress: ContactAddress): Unit = ???

	protected[service] def replaceMyselfWithAnIncommunicableDelegate(isConnecting: Boolean, motive: Any): ParticipantDelegate & Incommunicable = {
		// close the channel
		release()
		// replace me with an incommunicable delegate.
		clusterService.getBehavior.removeDelegate(peerAddress)
		val myReplacement = clusterService.getBehavior.createAndAddAnIncommunicableDelegate(peerAddress, isConnecting)
		myReplacement.initializeStateBasedOn(thisCommunicable)
		// notify
		clusterService.onDelegateBecomeIncommunicable(peerAddress, motive)
		myReplacement
	}

	private[service] def restartChannel(motive: Any): Unit = {
		val myReplacement = replaceMyselfWithAnIncommunicableDelegate(true, s"To restart the channel because of: $motive")

		clusterService.connectTo(peerAddress) {
			case Success(newChannel) =>
				sequencer.executeSequentially {
					if myReplacement eq clusterService.getDelegateOrElse(peerAddress, null) then {
						myReplacement.replaceMyselfWithACommunicableDelegate(newChannel)
					}
				}
			case Failure(exc) =>
				myReplacement.connectionAborted()
				scribe.error(s"The communication to $peerAddress has been aborted after many reconnection tries", exc)
		}
	}

	private[service] def release(): Unit = {
		if peerChannel.isOpen then peerChannel.close()
	}

}

