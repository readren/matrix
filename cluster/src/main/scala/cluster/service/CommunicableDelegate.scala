package readren.matrix
package cluster.service

import cluster.channel.{Receiver, Transmitter}
import cluster.misc.DoNothing
import cluster.service.ClusterService.{CommunicationChannelReplaced, DelegateConfig, VersionIncompatibilityWith}
import cluster.service.ContactCard.*
import cluster.service.Protocol.*
import cluster.service.Protocol.MembershipStatus.{ASPIRANT, MEMBER, UNKNOWN}
import cluster.service.ProtocolVersion

import java.net.SocketAddress
import java.nio.channels.AsynchronousSocketChannel
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

object CommunicableDelegate {
	trait Behavior {
		def handleMessage(delegate: CommunicableDelegate, message: Protocol): Boolean
	}
}

/** A [[ParticipantDelegate]] that is currently able to communicable with the participant. */
class CommunicableDelegate(
	override val clusterService: ClusterService,
	override val peerAddress: SocketAddress,
	val peerChannel: AsynchronousSocketChannel
) extends ParticipantDelegate {
	val config: DelegateConfig = clusterService.config.participantDelegatesConfig
	
	private val transmitterToPeer: Transmitter = new Transmitter(peerChannel)
	private val receiverFromPeer: Receiver = new Receiver(peerChannel)
	private var agreedVersion: ProtocolVersion = ProtocolVersion.NOT_SPECIFIED

	/** Only Used when the cluster service is in aspirant state. */
	private[service] var clusterCreatorProposedByPeer: ContactAddress | Null = null
	/** Only Used when the cluster service is in aspirant state. */
	private var clusterCreatorProposedByMe: ContactAddress | Null = null

	override def isCommunicable: Boolean = true

	override def isStable: Boolean = agreedVersion != ProtocolVersion.NOT_SPECIFIED

	def startConversationAsServer(): Unit = {
		receiveNextMessage()
	}

	def startConversationAsClient(): Unit = {
		this.transmitterToPeer.transmit[Protocol](Hello(config.versionsSupportedByMe, clusterService.getKnownParticipantsCards.toMap), ProtocolVersion.OF_THIS_PROJECT, config.transmitterTimeout, config.timeUnit) {
			case Transmitter.Delivered =>
				receiveNextMessage()
			case failure: Transmitter.NotDelivered =>
				reportFailure(failure)
				replaceMyselfWithAnIncommunicableDelegate(false, failure)
		}
	}

	private def receiveNextMessage(): Unit = {
		receiverFromPeer.receive[Protocol](agreedVersion, config.receiverTimeout, config.timeUnit) {
			case messageFromPeer: Protocol =>
				sequencer.executeSequentially {
					if clusterService.getCommunicableDelegatesBehavior.handleMessage(this, messageFromPeer) then receiveNextMessage()
					else scribe.debug(s"The channel {$peerChannel} reception completed with the terminal message {$messageFromPeer}")
				}
				
			case fault: Receiver.Fault =>
				val errorMessage = s"Failure while receiving a message from the peer $peerChannel: $fault"
				scribe.error(errorMessage)
				sequencer.executeSequentially(restartChannel(errorMessage))
		}
	}

	private[service] def determineAgreedVersion(versionsSupportedByMe: Set[ProtocolVersion], versionsSupportedByPeer: Set[ProtocolVersion]): Option[ProtocolVersion] = {
		val versionsSupportedByBoth = versionsSupportedByMe.intersect(versionsSupportedByPeer)
		versionsSupportedByBoth.find(candidate => versionsSupportedByBoth.forall(rival => candidate == rival || candidate.isNewerThan(rival)))
	}

	private def reportFailure(failure: Transmitter.NotDelivered): Unit = {
		failure match {
			case transmissionFailure: Transmitter.TransmissionFailure =>
				scribe.error(s"A transmission failure occurred while transmitting `${transmissionFailure.rootMessage}` to the channel `$peerChannel`", transmissionFailure.cause)
			case serializationFailure: Transmitter.SerializationProblem =>
				scribe.error(s"A serialization failure occurred at position ${serializationFailure.problem.position} while transmitting `${serializationFailure.rootMessage}` to the channel `$peerChannel` ${if serializationFailure.aFragmentWasTransmitted then "after some bytes were transmitted" else "before any byte was transmitted"}", serializationFailure.problem)
		}
	}

	private def ifFailureReportItAndThen(consumer: Transmitter.NotDelivered => Unit)(report: Transmitter.Report): Unit = {
		report match {
			case Transmitter.Delivered => // do nothing

			case failure: Transmitter.NotDelivered =>
				reportFailure(failure)
				sequencer.executeSequentially(consumer(failure))
		}
	}

	private[service] def handle(hello: Hello): Unit = {
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
			if cardKnownByPeer.address != clusterService.myAddress && !clusterService.delegateByAddress.contains(cardKnownByPeer.address) then {
				determineAgreedVersion(config.versionsSupportedByMe, cardKnownByPeer.supportedVersions) match {
					case Some(version) =>
						clusterService.andAddANewDelegateForAndThenConnectToAndThenStartConversationWithParticipant(cardKnownByPeer.address, cardKnownByPeer.supportedVersions, UNKNOWN)

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

	// TODO Agregar un parámetro que intruya que hacer en caso de falla de transmission. 
	private[service] def notifyPeerThatICreatedTheCluster(inauguratingMembers: Map[ContactAddress, CommunicableDelegate]): Unit = {
		val inauguratingMembersCards = inauguratingMembers.view.mapValues(_.versionsSupportedByPeer).toMap
		transmitterToPeer.transmit[Protocol](ICreatedACluster(inauguratingMembersCards), agreedVersion, config.transmitterTimeout, config.timeUnit)(ifFailureReportItAndThen(DoNothing))
	}

	// TODO Agregar un parámetro que intruya que hacer en caso de falla de transmission. 
	private[service] def notifyPeerTheAspirantIProposeToBeTheClusterCreator(proposedAspirantAddress: ContactAddress, supportedVersions: Set[ProtocolVersion]): Unit = {
		if proposedAspirantAddress != clusterCreatorProposedByMe then {
			transmitterToPeer.transmit[Protocol](ClusterCreatorProposal(proposedAspirantAddress, supportedVersions), agreedVersion, config.transmitterTimeout, config.timeUnit)(ifFailureReportItAndThen(DoNothing))
			clusterCreatorProposedByMe = proposedAspirantAddress
		}
	}
	// TODO Agregar un parámetro que intruya que hacer en caso de falla de transmission. 
	private[service] def notifyPeerThatILostCommunicationWithOtherPeer(otherParticipantAddress: ContactAddress): Unit = ???

	// TODO Agregar un parámetro que intruya que hacer en caso de falla de transmission. 
	private[service] def notifyPeerThatIRecoveredCommunicationWithOtherPeer(otherParticipantAddress: ContactAddress): Unit = ???


	private[service] def replaceMyselfWithAnIncommunicableDelegate(isConnectingAsClient: Boolean, motive: Any): IncommunicableDelegate = {
		// release this delegate
		release()
		// replace me with an incommunicable delegate.
		clusterService.removeDelegate(peerAddress)
		val myReplacement = clusterService.createAndAddAnIncommunicableDelegate(peerAddress, isConnectingAsClient)
		myReplacement.initializeStateBasedOn(this)
		// notify
		clusterService.onDelegateBecomeIncommunicable(peerAddress, motive)
		myReplacement
	}

	private[service] def replaceMyselfWithACommunicableDelegate(newChannel: AsynchronousSocketChannel): CommunicableDelegate = {
		// release this delegate
		release()
		// replace me with an incommunicable delegate.
		clusterService.removeDelegate(peerAddress)
		val myReplacement = clusterService.createAndAddACommunicableDelegate(peerAddress, newChannel)
		myReplacement.initializeStateBasedOn(this)
		// notify
		clusterService.notify(CommunicationChannelReplaced(peerAddress))
		myReplacement
	}

	private[service] def restartChannel(motive: Any): Unit = {
		val myReplacement = replaceMyselfWithAnIncommunicableDelegate(true, s"To restart the channel because of: $motive")

		clusterService.connectTo(peerAddress) {
			case Success(newChannel) =>
				sequencer.executeSequentially {
					if myReplacement eq clusterService.delegateByAddress.getOrElse(peerAddress, null) then {
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
							scribe.info(s"The channel of the released delegate of the participant at $peerAddress was gracefully closed")
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
