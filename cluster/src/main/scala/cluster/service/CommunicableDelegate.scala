package readren.matrix
package cluster.service

import cluster.channel.Transmitter.{Delivered, NotDelivered, Report}
import cluster.channel.{Receiver, Transmitter}
import cluster.misc.DoNothing
import cluster.service.ClusterService.{CommunicationChannelReplaced, DelegateConfig, VersionIncompatibilityWith}
import cluster.service.ContactCard.*
import cluster.service.IncommunicableDelegate.Reason
import cluster.service.IncommunicableDelegate.Reason.{IS_CONNECTING_AS_CLIENT, IS_INCOMPATIBLE}
import cluster.service.Protocol.*
import cluster.service.Protocol.CommunicationStatus.{CONNECTED, HANDSHOOK}
import cluster.service.Protocol.MembershipStatus.{ASPIRANT, MEMBER, UNKNOWN}
import cluster.service.ProtocolVersion

import readren.taskflow.Maybe

import java.net.SocketAddress
import java.nio.channels.AsynchronousSocketChannel
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

object CommunicableDelegate {
	trait Behavior {
		/** The [[MembershipStatus]] this behavior corresponds to. */
		def membershipStatus: MembershipStatus
		def onDelegatedAdded(delegate: ParticipantDelegate): Unit
		def onDelegateCommunicabilityChange(delegate: ParticipantDelegate): Unit
		def onDelegateMembershipChange(delegate: ParticipantDelegate): Unit
		/** Called just after a successful connection to the participant corresponding to the specified `delegate` when I am at the client side of the delegate's channel.
		 * The implementation should transmit the conversation-opening message and call the provided `onComplete` call-back when the transmission completes. */
		def onConnectedAsClient(delegate: CommunicableDelegate, isReconnection: Boolean)(onComplete: Transmitter.Report => Unit): Unit
		def handleMessage(delegate: CommunicableDelegate, message: Protocol): Boolean
	}
	type TimerInstance = Long
	val NANOS_PER_MILLISECOND = 1_000_000L
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
	private[service] var agreedVersion: ProtocolVersion = ProtocolVersion.NOT_SPECIFIED

	/** Only Used when the cluster service is in aspirant state. */
	private[service] var clusterCreatorProposedByPeer: ContactAddress | Null = null
	/** Only Used when the cluster service is in aspirant state. */
	private var clusterCreatorProposedByMe: ContactAddress | Null = null
	
	/** The state of the peer according to it */
	private[service] var peerStatePhoto: Maybe[ParticipantViewpoint] = Maybe.empty 
	
	override def isCommunicable: Boolean = true

	override def isStable: Boolean = agreedVersion != ProtocolVersion.NOT_SPECIFIED && peerMembershipStatusAccordingToMe != UNKNOWN

	override def communicationStatus: CommunicationStatus = if isStable then HANDSHOOK else CONNECTED
	
	override def info: ParticipantInfo =
		ParticipantInfo(versionsSupportedByPeer, communicationStatus, peerMembershipStatusAccordingToMe)

	def startConversationAsServer(): Unit = {
		receiveNextMessage()
	}

	def startConversationAsClient(isReconnection: Boolean): Unit = {
		clusterService.getCommunicableDelegatesBehavior.onConnectedAsClient(this, isReconnection) {
			case Transmitter.Delivered =>
				receiveNextMessage()
			case failure: Transmitter.NotDelivered =>
				reportFailure(failure)
				sequencer.executeSequentially(restartChannel(failure))
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
		versionsSupportedByBoth.minOption(using ProtocolVersion.newerFirstOrdering)
	}

	private[service] def reportFailure(failure: Transmitter.NotDelivered): Unit = {
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

	private[service] def handleMessage(hello: Hello): Unit = {
		versionsSupportedByPeer = hello.versionsISupport
		val previousMembershipState = peerMembershipStatusAccordingToMe
		// update my viewpoint of the peer's membership.
		peerMembershipStatusAccordingToMe = ASPIRANT

		// Connect to participants I didn't know.
		for cardKnownByPeer <- hello.cardsOfOtherParticipantsIKnow do {
			if cardKnownByPeer.address != clusterService.myAddress && !clusterService.delegateByAddress.contains(cardKnownByPeer.address) then {
				determineAgreedVersion(config.versionsSupportedByMe, cardKnownByPeer.supportedVersions) match {
					case Some(version) =>
						clusterService.addANewDelegateForAndThenConnectToAndThenStartConversationWithParticipant(cardKnownByPeer.address, cardKnownByPeer.supportedVersions, UNKNOWN)

					case None =>
						val newDelegate = clusterService.addANewIncommunicableDelegate(cardKnownByPeer.address, IS_INCOMPATIBLE)
						clusterService.getCommunicableDelegatesBehavior.onDelegatedAdded(newDelegate)
						clusterService.notify(VersionIncompatibilityWith(cardKnownByPeer.address))
				}
			}
		}

		val previousAgreedVersion = agreedVersion
		// update the protocol-version to use when communicating with the peer, transmit the response: `Welcome` or `SupportedVersionsMismatch`.
		determineAgreedVersion(config.versionsSupportedByMe, hello.versionsISupport) match {
			case Some(version) =>
				agreedVersion = version
				transmitToPeer(Welcome(clusterService.getKnownParticipantsInfo.toMap))(ifFailureReportItAndThen(restartChannel))

			case None =>
				agreedVersion = ProtocolVersion.NOT_SPECIFIED
				val cause = s"None of the peer's supported versions according to his hello message are supported by me. Versions supported by peer: ${hello.versionsISupport}"
				transmitterToPeer.transmit[Protocol](SupportedVersionsMismatch, ProtocolVersion.OF_THIS_PROJECT, config.transmitterTimeout, config.timeUnit)(ifFailureReportItAndThen(DoNothing))
				replaceMyselfWithAnIncommunicableDelegate(IS_INCOMPATIBLE, cause)
				clusterService.notify(VersionIncompatibilityWith(peerAddress))

		}

		// Notify changes
		if agreedVersion != previousAgreedVersion then clusterService.getCommunicableDelegatesBehavior.onDelegateCommunicabilityChange(this)
		if previousMembershipState != ASPIRANT then clusterService.getCommunicableDelegatesBehavior.onDelegateMembershipChange(this)
		if previousMembershipState eq MEMBER then clusterService.notify(ClusterService.ParticipantHasBeenRestarted(peerAddress))
	}

	inline private[service] def sendHello(onComplete: Transmitter.Report => Unit): Unit = {
		transmitterToPeer.transmit[Protocol](Hello(config.versionsSupportedByMe, clusterService.getKnownParticipantsCards.toMap), ProtocolVersion.OF_THIS_PROJECT, config.transmitterTimeout, config.timeUnit)(onComplete)
	}

	inline private[service] def sendIHaveReconnected(onComplete: Transmitter.Report => Unit): Unit = {
		transmitToPeer(IHaveReconnected(config.versionsSupportedByMe, clusterService.myMembershipStatus))(onComplete);
	}

	inline private[service] def sendRequestToJoin(joinTokenByMemberAddress: Map[ContactAddress, JoinToken])(onComplete: Transmitter.Report => Unit): Unit = {
		transmitToPeer(RequestToJoin(joinTokenByMemberAddress))(onComplete)
	}

	private def transmitToPeer(message: Protocol)(onComplete: Transmitter.Report => Unit): Unit = {
		transmitterToPeer.transmit[Protocol](message, agreedVersion, config.transmitterTimeout, config.timeUnit)(onComplete)
	}

	// TODO Agregar un parámetro que instruya que hacer en caso de falla de transmission.
	private[service] def notifyPeerThatICreatedTheCluster(myViewPoint: ParticipantViewpoint): Unit = {
		transmitToPeer(ICreatedACluster(myViewPoint))(ifFailureReportItAndThen { failure =>
			peerMembershipStatusAccordingToMe = ASPIRANT
			restartChannel(failure)	
		})
	}

	private[service] def notifyPeerTheAspirantIProposeToBeTheClusterCreator(proposedAspirantAddress: ContactAddress, supportedVersions: Set[ProtocolVersion]): Unit = {
		if proposedAspirantAddress != clusterCreatorProposedByMe then {
			clusterCreatorProposedByMe = proposedAspirantAddress
			transmitToPeer(ClusterCreatorProposal(proposedAspirantAddress, supportedVersions))(ifFailureReportItAndThen(restartChannel))
		}
	}
	// TODO Agregar un parámetro que instruya que hacer en caso de falla de transmission. 
	private[service] def notifyPeerThatILostCommunicationWithOtherPeer(otherParticipantAddress: ContactAddress): Unit = ???

	// TODO Agregar un parámetro que instruya que hacer en caso de falla de transmission. 
	private[service] def notifyPeerThatIRecoveredCommunicationWithOtherPeer(otherParticipantAddress: ContactAddress): Unit = ???


	private[service] def replaceMyselfWithAnIncommunicableDelegate(reason: Reason, motive: Any): IncommunicableDelegate = {
		// release this delegate
		release()
		// replace me with an incommunicable delegate.
		clusterService.removeDelegate(peerAddress)
		val myReplacement = clusterService.addANewIncommunicableDelegate(peerAddress, reason)
		myReplacement.initializeStateBasedOn(this)
		// notify
		clusterService.getCommunicableDelegatesBehavior.onDelegateCommunicabilityChange(myReplacement)
		clusterService.onDelegateBecomeIncommunicable(peerAddress, reason, motive)
		myReplacement
	}

	private[service] def replaceMyselfWithACommunicableDelegate(newChannel: AsynchronousSocketChannel): CommunicableDelegate = {
		// release this delegate
		release()
		// replace me with an communicable delegate.
		clusterService.removeDelegate(peerAddress)
		val myReplacement = clusterService.addANewCommunicableDelegate(peerAddress, newChannel)
		myReplacement.initializeStateBasedOn(this)
		// notify
		clusterService.getCommunicableDelegatesBehavior.onDelegateCommunicabilityChange(myReplacement) // TODO this isn't exactly a communicability change. Analyze it. 
		clusterService.notify(CommunicationChannelReplaced(peerAddress))
		myReplacement
	}

	private[service] def restartChannel(motive: Any): Unit = {
		val myReplacement = replaceMyselfWithAnIncommunicableDelegate(IS_CONNECTING_AS_CLIENT, s"To restart the channel because of: $motive")

		clusterService.connectTo(peerAddress) {
			case Success(newChannel) =>
				sequencer.executeSequentially {
					if myReplacement eq clusterService.delegateByAddress.getOrElse(peerAddress, null) then {
						myReplacement.replaceMyselfWithACommunicableDelegate(newChannel).startConversationAsClient(true)
					}
				}
			case Failure(exc) =>
				myReplacement.onConnectionAborted(exc)
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
