package readren.matrix
package cluster.service

import ParticipantDelegate.{Config, ResponseToJoinRequest}
import Protocol.*
import cluster.channel.{Receiver, Serializer, Transmitter}
import cluster.service.ProtocolVersion
import cluster.service.Protocol.*
import common.CompileTime.getTypeName

import readren.matrix.cluster.service.ClusterService.{AspirantState, MemberState}
import readren.taskflow.Maybe

import java.net.SocketAddress
import java.nio.channels.AsynchronousSocketChannel
import java.util.concurrent.TimeUnit

object ParticipantDelegate {

	val DISCRIMINATOR_BASE: Byte = 10

	case class ResponseToJoinRequest(members: java.util.Set[ContactAddress])

	import readren.matrix.cluster.channel.Nio2Serializers.given

	given Serializer[ResponseToJoinRequest] = new Serializer[ResponseToJoinRequest] {
		//		val socketAddressSerializer: Serializer[SocketAddress] = summon[Serializer[SocketAddress]]

		override def serialize(message: ResponseToJoinRequest, writer: Serializer.Writer): Serializer.Outcome = {

			writer.putByte((DISCRIMINATOR_BASE + 1).toByte)
			writer.putShort(message.members.size.toShort)

			val iterator = message.members.iterator
			while iterator.hasNext do {
				val element = iterator.next()
				writer.writeFull(element) match {
					case Serializer.Success => // do nothing
					case failed: Serializer.Unsupported =>
						return Serializer.Unsupported(writer.position, s"Failure while serializing an element of `${getTypeName[ResponseToJoinRequest]}.members`. Cause: ${failed.explanation}")
				}
			}
			Serializer.Success
		}
	}

	class Config(val versionsSupportedByMe: Set[ProtocolVersion], val receiverTimeout: Long = 1, val transmitterTimeout: Long = 1, val timeUnit: TimeUnit = TimeUnit.SECONDS)

}

class ParticipantDelegate(clusterService: ClusterService, peerChannel: AsynchronousSocketChannel, config: Config, peerRemoteAddress: SocketAddress) {

	private val sequencer = clusterService.sequencer
	private val receiverFromPeer = new Receiver(peerChannel)
	private val transmitterToPeer = new Transmitter(peerChannel)
	var peerMembershipStateAccordingToMe: MembershipState | Null = null
	var versionsSupportedByPeer: Set[ProtocolVersion] = Set.empty
	var agreedVersion: ProtocolVersion = ProtocolVersion.OF_THIS_PROJECT

	def startAsServer(): Unit = {
		startReceiving()
	}

	def startAsClient(myAddress: ContactAddress): Unit = {
		transmitterToPeer.transmit[Protocol](Hello(myAddress, config.versionsSupportedByMe), ProtocolVersion.OF_THIS_PROJECT, config.transmitterTimeout, config.timeUnit) {
			case Transmitter.Delivered =>
				startReceiving()
			case failure =>
				reportTransmissionFailure(failure)
		}
	}

	private def startReceiving(): Unit = {
		receiverFromPeer.receive[Protocol](agreedVersion, config.receiverTimeout, config.timeUnit) {
			case fault: Receiver.Fault =>
				scribe.error(s"Failure while receiving a message from the peer $peerChannel: $fault")
				peerChannel.close()
				sequencer.executeSequentially(clusterService.peerChannelClosed(peerRemoteAddress, fault))

			case messageFromPeer: Protocol => sequencer.executeSequentially(messageFromPeer match {
				case hello: Hello =>
					peerMembershipStateAccordingToMe match {
						case null =>
							peerMembershipStateAccordingToMe = Aspirant
						case Aspirant =>
							peerMembershipStateAccordingToMe = Aspirant
							clusterService.notifyParticipantHasBeenRestarted(peerRemoteAddress)
						case Member =>
							peerMembershipStateAccordingToMe = Aspirant
							clusterService.notifyParticipantHasBeenRestarted(peerRemoteAddress)
					}
					versionsSupportedByPeer = hello.versionsISupport

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

				case SupportedVersionsMismatch =>
					clusterService.notifyVersionIncompatibilityWith(peerRemoteAddress)
					closeChannel()

				case NoClusterIAmAwareOf(knowAspirantsCards) =>
					clusterService.relationshipState match {
						case member: MemberState =>
							clusterService.solveClusterExistenceConflictWith(this)
							
						case aspirant: AspirantState =>
							val iAmConnectedToAllAspirants = knowAspirantsCards.foldLeft(true) { (accumulator, aspirantCard) =>
								clusterService.getDelegateOrElse(aspirantCard.address) match {
									case null =>
										if determineAgreedVersion(config.versionsSupportedByMe, aspirantCard.supportedVersions).isDefined then {
											clusterService.startConnectionTo(aspirantCard.address)
										} else {
											clusterService.notifyVersionIncompatibilityWith(aspirantCard.address)
										}
										false

									case aspirantDelegate: ParticipantDelegate =>
										true
								}
							}
							if iAmConnectedToAllAspirants then clusterService.proposeClusterCreator() 
					}

				case YouShouldCreateTheCluster(knownAspirantsCards) =>
					clusterService.relationshipState match {
						case member: MemberState =>
							clusterService.solveClusterExistenceConflictWith(this)
						case aspirant: AspirantState =>
							
					}					

				case icc: ICreatedACluster =>

				case jam: JoinApprovalMembers =>

				case oi: RequestApprovalToJoin =>

				case jag: JoinApprovalGranted =>

				case rtj: RequestToJoin =>

				case jg: JoinGranted =>

				case jr: JoinRejected =>

				case lcw: ILostCommunicationWith =>

				case IAmLeaving =>

				case hb: Heartbeat =>

				case sc: StateChanged =>
				case phr: ParticipantHasBeenRestarted =>

			})
		}
	}

	def handleReconnection(newChannel: AsynchronousSocketChannel): ParticipantDelegate = ???

	def onOtherPeerChannelClosed(otherPeerAddress: ContactAddress): Unit = ???

	private def determineAgreedVersion(versionsSupportedByMe: Set[ProtocolVersion], versionsSupportedByPeer: Set[ProtocolVersion]): Option[ProtocolVersion] = {
		val versionsSupportedByBoth = versionsSupportedByMe.intersect(versionsSupportedByPeer)
		versionsSupportedByBoth.find(candidate => versionsSupportedByBoth.forall(rival => candidate.isNewerThan(rival)))
	}

	private def reportTransmissionFailure(report: Transmitter.Report): Unit = {
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

	private def tryToReconnect(): Unit = ???

	private def closeChannel(): Unit = ???

}
