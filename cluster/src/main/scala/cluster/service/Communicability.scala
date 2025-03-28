package readren.matrix
package cluster.service

import cluster.channel.{Receiver, Transmitter}
import cluster.misc.DoNothing
import cluster.service.ClusterService.{CommunicationChannelReplaced, DelegateConfig, VersionIncompatibilityWith}
import cluster.service.ContactCard.*
import cluster.service.Protocol.*
import cluster.service.Protocol.MembershipStatus.{ASPIRANT, MEMBER, UNKNOWN}

import java.nio.channels.AsynchronousSocketChannel
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

/** Categorizes subtypes of [[ParticipantDelegate]] into two groups, the ones that represent delegates that communicate to the peer from the ones that don't. */
sealed trait Communicability {
	def isCommunicable: Boolean

	/** The communicability is stable: connection and handshaking have completed (successfully or not). */
	def isStable: Boolean
}

/** Mixin that defines the peculiarities of a [[ParticipantDelegate]] that is currently not able to communicate with the participant.
 * Note that participants to which the [[ClusterService]] is connecting to are associated to a [[Incommunicable]] delegate with the [[isConnectingAsClient]] flag set, until the connection is completed; moment at which the delegate is replaced with a [[Communicable]] one. */
trait Incommunicable extends Communicability { thisCommunicable: ParticipantDelegate =>
	override def isCommunicable: Boolean = false

	override def isStable: Boolean = !isConnectingAsClient

	/** Tells that this incommunicable delegate is associated to a participant to which the [[ClusterService]] is connecting to. */
	def isConnectingAsClient: Boolean

	/** The implementation must clear the [[isConnectingAsClient]] flag. */
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

/** Mixin that defines the peculiarities of a [[ParticipantDelegate]] that is able to communicate with the participant. */
trait Communicable extends Communicability { thisCommunicable: ParticipantDelegate =>
	protected val peerChannel: AsynchronousSocketChannel
	val config: DelegateConfig
	protected val receiverFromPeer: Receiver
	protected val transmitterToPeer: Transmitter
	protected var agreedVersion: ProtocolVersion = ProtocolVersion.NOT_SPECIFIED

	override def isCommunicable: Boolean = true

	override def isStable: Boolean = agreedVersion != ProtocolVersion.NOT_SPECIFIED

	protected def continueReceiving(onComplete: Receiver.Fault | Protocol => Unit): Unit

	def startConversationAsServer(): Unit = {
		continueReceiving(handleReceiverCompletion)
	}

	def startConversationAsClient(): Unit = {
		this.transmitterToPeer.transmit[Protocol](Hello(config.versionsSupportedByMe, clusterService.getKnownParticipantsCards.toMap), ProtocolVersion.OF_THIS_PROJECT, config.transmitterTimeout, config.timeUnit) {
			case Transmitter.Delivered =>
				continueReceiving(handleReceiverCompletion)
			case failure: Transmitter.NotDelivered =>
				reportFailure(failure)
				replaceMyselfWithAnIncommunicableDelegate(false, failure)
		}
	}
	
	private def handleReceiverCompletion(terminatingSignal: Receiver.Fault | Protocol): Unit = {
		terminatingSignal match {
			case fault: Receiver.Fault =>
				val errorMessage = s"Failure while receiving a message from the peer $peerChannel: $fault"
				scribe.error(errorMessage)
				sequencer.executeSequentially(restartChannel(errorMessage))
			case terminalMessage: Protocol =>
				scribe.debug(s"The channel {$peerChannel} reception completed with the terminal message {$terminalMessage}" )
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

	protected def ifFailureReportItAndThen(consumer: Transmitter.NotDelivered => Unit)(report: Transmitter.Report): Unit = {
		report match {
			case Transmitter.Delivered => // do nothing

			case failure: Transmitter.NotDelivered =>
				reportFailure(failure)
				sequencer.executeSequentially(consumer(failure))
		}
	}

	protected def handle(hello: Hello): Unit = {
		versionsSupportedByPeer = hello.versionsISupport

		// update my viewpoint of the peer's membership.
		if peerMembershipStatusAccordingToMe == MEMBER then {
			peerMembershipStatusAccordingToMe = ASPIRANT
			clusterService.notify(ClusterService.ParticipantHasBeenRestarted(peerAddress))
		} else {
			peerMembershipStatusAccordingToMe = ASPIRANT
		}

		// Connect to participants I didn't know.
		for cardKnownByPeer <- hello.cardsOfOtherParticipantsIKnow do {
			if cardKnownByPeer.address != clusterService.myAddress && !clusterService.participantByAddress.contains(cardKnownByPeer.address) then {
				determineAgreedVersion(config.versionsSupportedByMe, cardKnownByPeer.supportedVersions) match {
					case Some(version) =>
						clusterService.createAndAddADelegateForAndThenConnectToParticipant(cardKnownByPeer.address, cardKnownByPeer.supportedVersions, UNKNOWN)

					case None =>
						clusterService.notify(VersionIncompatibilityWith(cardKnownByPeer.address))
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
				replaceMyselfWithAnIncommunicableDelegate(false, cause)
				clusterService.notify(VersionIncompatibilityWith(peerAddress))
				transmitterToPeer.transmit[Protocol](SupportedVersionsMismatch, ProtocolVersion.OF_THIS_PROJECT, config.transmitterTimeout, config.timeUnit)(ifFailureReportItAndThen(DoNothing))

		}
	}


	protected[service] def notifyPeerThatILostCommunicationWithOtherPeer(otherParticipantAddress: ContactAddress): Unit = ???

	protected[service] def notifyPeerThatIRecoveredCommunicationWithOtherPeer(otherParticipantAddress: ContactAddress): Unit = ???

	protected[service] def replaceMyselfWithAnIncommunicableDelegate(isConnectingAsClient: Boolean, motive: Any): ParticipantDelegate & Incommunicable = {
		// release this delegate
		release()
		// replace me with an incommunicable delegate.
		clusterService.getBehavior.removeDelegate(peerAddress)
		val myReplacement = clusterService.getBehavior.createAndAddAnIncommunicableDelegate(peerAddress, isConnectingAsClient)
		myReplacement.initializeStateBasedOn(thisCommunicable)
		// notify
		clusterService.onDelegateBecomeIncommunicable(peerAddress, motive)
		myReplacement
	}

	protected[service] def replaceMyselfWithACommunicableDelegate(newChannel: AsynchronousSocketChannel): ParticipantDelegate & Communicable = {
		// release this delegate
		release()
		// replace me with an incommunicable delegate.
		clusterService.getBehavior.removeDelegate(peerAddress)
		val myReplacement = clusterService.getBehavior.createAndAddACommunicableDelegate(peerAddress, newChannel)
		myReplacement.initializeStateBasedOn(thisCommunicable)
		// notify
		clusterService.notify(CommunicationChannelReplaced(peerAddress))
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
				scribe.error(s"The communication to the participant at $peerAddress has been aborted after many reconnection tries", exc)
		}
	}

	private[service] def release(): Unit = {
		// if the channel is open, close it gracefully.
		if peerChannel.isOpen then {
			peerChannel.shutdownOutput()

			def loop(): Unit = {
				try {
					receiverFromPeer.receive[Protocol](agreedVersion, config.receiverTimeout, config.timeUnit) {
						case fault: Receiver.Fault =>
							peerChannel.close()
							scribe.error(s"Failure while purging the channel's input connection of a released delegate of the participant at $peerAddress.", fault.toString)
						case IAmDeaf =>
							scribe.info(s"The channel of the released delegate of the participant at $peerAddress was gracefully closed" )
							peerChannel.close()
						case message: Protocol =>
							scribe.warn(s"The following message from the participant at $peerAddress was discarded because it was received after the delegate was released:", message.toString)
							loop()
					}
				} catch {
					case NonFatal(e) =>
						peerChannel.close()
						scribe.error(s"Failure when trying to purge the next ignored message from the channel's input connection of a released delegate of the participant at $peerAddress.", e)
				}
			}

			loop()
		}
	}

}

