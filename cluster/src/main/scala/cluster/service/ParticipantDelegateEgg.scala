package readren.matrix
package cluster.service

import cluster.channel.Receiver
import cluster.serialization.ProtocolVersion
import cluster.service.ClusterService.ChannelOrigin.{CLIENT_INITIATED, SERVER_ACCEPTED}

import readren.matrix.cluster.service.Protocol.CommunicationStatus.HANDSHOOK
import readren.taskflow.Maybe

import java.net.SocketAddress
import java.nio.channels.AsynchronousSocketChannel
import scala.util.control.NonFatal

class ParticipantDelegateEgg(clusterService: ClusterService, channel: AsynchronousSocketChannel) {
	val oPeerRemoteAddress: Maybe[SocketAddress] = try Maybe.apply(channel.getRemoteAddress) catch { case NonFatal(_) => Maybe.empty }
	private val config = clusterService.config.participantDelegatesConfig


	val receiverFromPeer: Receiver = clusterService.buildReceiverFor(channel)
	
	private def peerRemoteAddress: String = oPeerRemoteAddress.fold("unknown")(_.toString)
	
	def incubate(): Unit = {
		receiverFromPeer.receive[Protocol](ProtocolVersion.OF_THIS_PROJECT, config.receiverTimeout, config.timeUnit) {
			case fault: Receiver.Fault =>
				scribe.info(fault.scribeContent(s"A connection to the participant at `${clusterService.myAddress}` initiated from `$peerRemoteAddress}` was aborted because:")*)
				abort()
			case message: Hello =>
				if clusterService.config.acceptedConnectionsFilter.test(message.myContactAddress) then clusterService.sequencer.executeSequentially(hatch(message))
				else {
					scribe.info(s"A connection to the participant at `${clusterService.myAddress}` initiated (from the ephemeral address `$peerRemoteAddress`) by the participant at `${message.myContactAddress}` was rejected by the contact-addresses' filter")
					abort()
				}
			case unexpectedMessage =>
				scribe.info(s"A connection to the participant at `${clusterService.myAddress}` initiated from the ephemeral address `$peerRemoteAddress` was aborted because the conversation was initiated with an unexpected message: $unexpectedMessage.")
				abort()
		}
	}
	
	private def hatch(hello: Hello): Unit = {
		val peerContactAddress = hello.myContactAddress
		clusterService.delegateByAddress.getOrElse(peerContactAddress, null) match {
			case null =>
				val newParticipantDelegate = clusterService.addANewCommunicableDelegate(peerContactAddress, channel, receiverFromPeer, oPeerRemoteAddress, SERVER_ACCEPTED)
				newParticipantDelegate.startConversationAsServer(hello)

			case delegate: CommunicableDelegate =>
				// If I already have a communicable delegate associated with the peer, there are two possibilities:
				// 1) If either, I am at the server side of the delegate's channel, or I already have sent a Hello and received a Welcome response from the peer through the delegate's channel; then I am certain that delegate's channel is stale and the peer is reconnecting to me (because participants don't initiate a connection to a target that they are connecting or already connected). Therefore, I have to keep the new accepted connection and discard the old.
				// 2) If the connection of the delegated was initiated by me and I still haven't received the Welcome response (to the Hello), then most probably the peer and me have initiated a connection to each other simultaneously. Therefore, we have to agree which to keep and which to discard.
				// Note that the peer may be having the same problem right now. He has to decide which connection to keep: the one that he initiated as a client (and I am accepting now), or the one I previously initiated as a client (and he may be accepting now). So we (the peer and me) have to agree which connection to keep.
				// We can't just keep the connection that completed earlier because that is vulnerable to race condition because each side may see a different connection completing first.
				// We have to establish an absolute and global criteria for this decision which is the comparison of the addresses: the kept connection is the one in which the server has the highest address. The other connection is gracefully closed.
				// That criteria is implemented in the `ClusterService.whichChannelShouldIKeepWhenMutualConnectionWith` method.
				if (delegate.communicationStatus eq HANDSHOOK) || (delegate.channelOrigin eq SERVER_ACCEPTED) || (clusterService.whichChannelShouldIKeepWhenMutualConnectionWith(peerContactAddress) eq SERVER_ACCEPTED)  then {
					// Keep the new accepted connection and replace the old delegate with a new one.
					delegate.replaceMyselfWithACommunicableDelegate(channel, receiverFromPeer, oPeerRemoteAddress, SERVER_ACCEPTED).get.startConversationAsServer(hello)
					scribe.info(s"A connection from the participant at $peerContactAddress, with which I was already connected as ${delegate.channelOrigin.productPrefix}, has been accepted. The previous communication channel will be gracefully closed. This happens only (if I am not mistaken) when the peer restarts the channel.")
				} else {
					// Keep the okd delegate and discard the brand-new accepted connection.
					clusterService.closeDiscardedChannelGracefully(channel, SERVER_ACCEPTED)
					scribe.info(s"The connection initiated (from the ephemeral address `$peerRemoteAddress`) by the participant at `$peerContactAddress` that I (`${clusterService.myAddress}`) have incubated (accepted), is being discarded to let me continue with the already established connection with the same participant but with me on the client side.")
				}

			case incommunicableDelegate: IncommunicableDelegate => // Happens if I knew the peer, but we are not fully connected.
				// If I am simultaneously initiating a connection to the peer as a client, then I have to cancel it and continue with accepted one (because it has already received the hello message).
				if incommunicableDelegate.isConnectingAsClient then {
					val communicableParticipant = incommunicableDelegate.replaceMyselfWithACommunicableDelegate(channel, receiverFromPeer, oPeerRemoteAddress, SERVER_ACCEPTED).get
					// TODO analyze if a notification of the communicability change should be issued here.
					scribe.info(s"A connection from the participant at $peerContactAddress was accepted despite I am simultaneously trying to connect to him as client. I assume I will close the connection in which I am the client as soon as it completes.") // TODO investigate if it is possible to cancel the connection while it is in progress.
					communicableParticipant.startConversationAsServer(hello)
				}
				// If I know the peer and I am not currently trying to connect to it
				else {
					val communicableDelegate = incommunicableDelegate.replaceMyselfWithACommunicableDelegate(channel, receiverFromPeer, oPeerRemoteAddress, SERVER_ACCEPTED).get
					// TODO analyze if a notification of the communicability change should be issued here.
					scribe.info(s"A connection from the participant at $peerContactAddress, which was marked as incommunicable ${incommunicableDelegate.communicationStatus}, has been accepted. Let's see if we can communicate with it this time.")
					communicableDelegate.startConversationAsServer(hello)
				}
		}
	}
	
	private def abort(): Unit = {
		try channel.close() catch {
			case NonFatal(e) => scribe.warn(s"Closing the channel of an aborted connection to `${clusterService.myAddress} initiated from `$peerRemoteAddress` caused:", e)
		}
	}

}
