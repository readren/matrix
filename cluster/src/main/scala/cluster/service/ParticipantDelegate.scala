package readren.matrix
package cluster.service

import ParticipantDelegate.{Config, ResponseToJoinRequest}
import Protocol.*
import cluster.channel.{Receiver, Serializer, Transmitter}
import cluster.service.ProtocolVersion
import cluster.service.Protocol.*
import utils.CompileTime.getTypeName

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

	sealed trait State
	case object WaitingJoinRequestResponse extends State

	class Config(val receiverTimeout: Long = 1, val transmitterTimeout: Long = 1, val timeUnit: TimeUnit = TimeUnit.SECONDS)
	
}

class ParticipantDelegate(clusterService: ClusterService, peerRemoteAddress: ContactAddress, peerChannel: AsynchronousSocketChannel, config: Config, peerVersion: ProtocolVersion) {

	private val receiverFromPeer = new Receiver(peerChannel, peerVersion)
	private val transmitterToPeer = new Transmitter(peerChannel, peerVersion)
	private var membershipState: MembershipState | Null = null
	var contactCard: ContactCard | Null = null


	def startAsServer(): Unit = {
		receiverFromPeer.receive[Protocol](config.receiverTimeout, config.timeUnit) {
			case fault: Receiver.Fault =>
				scribe.error(s"Failure while receiving a message from the peer $peerChannel: $fault")
				peerChannel.close()
				clusterService.peerChannelClosed(peerRemoteAddress, fault)

			case hello: Hello =>
				membershipState match {
					case null =>
						contactCard = hello.myCard
						membershipState = Aspirant
						transmitterToPeer.transmit(NoClusterIAmAwareOf(clusterService.knownParticipantsCards.toSet)) {
							case Transmitter.Delivered =>

							case failure: Transmitter.TransmissionFailure =>
								scribe.error(s"Failure while transmitting a ${getTypeName[ResponseToJoinRequest]} to $peerChannel")

						}
					case Aspirant =>
					case Member =>
				}


		}
	}
	
	def startAsClient(myContactCard: ContactCard): Unit = {
		transmitterToPeer.transmit[Protocol](Hello(myContactCard), config.transmitterTimeout, config.timeUnit) { report =>

		}

	}

	def handleReconnection(newChannel: AsynchronousSocketChannel, versionId: ProtocolVersion): ParticipantDelegate = ???
	def onOtherPeerChannelClosed(otherPeerAddress: ContactAddress): Unit = ???
}
