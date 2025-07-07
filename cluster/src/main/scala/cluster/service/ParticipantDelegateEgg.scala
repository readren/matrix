package readren.matrix
package cluster.service

import cluster.channel.Receiver
import cluster.misc.DoNothing
import cluster.serialization.ProtocolVersion
import cluster.service.ClusterService.ChannelOrigin.ACCEPTED
import cluster.service.Protocol.CommunicationStatus.HANDSHOOK
import cluster.service.Protocol.ContactAddress

import readren.taskflow.Maybe

import java.net.SocketAddress
import java.nio.channels.AsynchronousSocketChannel
import scala.util.control.NonFatal

class ParticipantDelegateEgg(clusterService: ClusterService, channel: AsynchronousSocketChannel) {
	private val config = clusterService.config.participantDelegatesConfig
	private val oChannelRemoteAddress: Maybe[SocketAddress] = try Maybe.apply(channel.getRemoteAddress) catch { case NonFatal(_) => Maybe.empty }
	private var oPeerContactAddress: Maybe[ContactAddress] = Maybe.empty

	val receiverFromPeer: Receiver = clusterService.buildReceiverFor(channel, () => oPeerContactAddress.fold(oChannelRemoteAddress)(Maybe.some), oChannelRemoteAddress)
	
	private def showChannelRemoteAddress: String = oChannelRemoteAddress.fold("unknown")(_.toString)

	def incubate(): Unit = {
		receiverFromPeer.receive[Protocol](ProtocolVersion.OF_THIS_PROJECT, config.receiverTimeout, config.timeUnit) {
			case fault: Receiver.Fault =>
				scribe.info(fault.scribeContent(s"${clusterService.myAddress}: A connection to me initiated from `$showChannelRemoteAddress` was aborted because:")*)
				abortIncubated(oChannelRemoteAddress)
			case message: Hello =>
				if clusterService.config.acceptedConnectionsFilter.test(message.myContactAddress) then {
					scribe.debug(s"${clusterService.myAddress}: I have received the hello message `$message` from `${message.myContactAddress}`.")
					clusterService.sequencer.executeSequentially {
						oPeerContactAddress = Maybe.some(message.myContactAddress)
						hatch(message)
					}
				} else {
					scribe.info(s"${clusterService.myAddress}: A connection to me initiated by `${message.myContactAddress}` (with ephemeral address `$showChannelRemoteAddress`) was rejected by the contact-addresses' filter")
					abortIncubated(Maybe.some(message.myContactAddress))
				}
			case unexpectedMessage =>
				scribe.info(s"${clusterService.myAddress}: A connection to me initiated from `$showChannelRemoteAddress` was discarded because the conversation was initiated with an unexpected message: $unexpectedMessage.")
				abortIncubated(oChannelRemoteAddress)
		}
	}
	
	private def abortIncubated(thePeerAddress: Maybe[ContactAddress]): Unit = {
		val transmitter = clusterService.buildTransmitterFor(channel, thePeerAddress)
		clusterService.closeDiscardedChannelGracefully(channel, transmitter, ACCEPTED)
	}
	
	private def hatch(hello: Hello): Unit = {
		val peerContactAddress = hello.myContactAddress
		if clusterService.isShutDown then abortIncubated(Maybe.some(peerContactAddress))
		else {
			clusterService.delegateByAddress.getOrElse(peerContactAddress, null) match {
				case null =>
					scribe.info(s"${clusterService.myAddress}: I accepted a new connection initiated by `$peerContactAddress`.")
					val newParticipantDelegate = clusterService.addANewCommunicableDelegate(peerContactAddress, channel, receiverFromPeer, oChannelRemoteAddress, ACCEPTED)
					newParticipantDelegate.startConversationAsServer(hello)

				case delegate: CommunicableDelegate =>
					// If I already have a communicable delegate associated with the peer, there are two possibilities:
					// 1) If either, I am at the server side of the delegate's channel (I accepted the connection), or I previously have received a Welcome response through the delegate's channel; then I am certain that delegate's channel is stale and the peer is reconnecting to me (because participants don't initiate a connection to a target that they are connecting or already connected). Therefore, I have to keep the new accepted connection and discard the old.
					// 2) If the connection of the delegated was initiated by me and I still haven't received the Welcome response (to the Hello), then most probably the peer and me have initiated a connection to each other simultaneously.
					// Note that the peer may be having the same problem right now. He has to decide which connection to keep: the one that he initiated as a client (and I am accepting now), or the one I previously initiated as a client (and he may be accepting now).
					// So we (the peer and me) have to agree which connection to keep.
					// We can't just keep the connection that completed earlier because that is vulnerable to race condition because each side may see a different connection completing first.
					// We have to establish an absolute and global criteria for this decision which is the comparison of the addresses: the kept connection is the one in which the server has the highest address. The other connection is gracefully closed.
					// The `ClusterService.whichChannelShouldIKeepWhenMutualConnectionWith` method encapsulates said criteria.
					if (delegate.channelOrigin eq ACCEPTED) || (delegate.communicationStatus eq HANDSHOOK) then {
						scribe.info(s"${clusterService.myAddress}: I accepted a new connection initiated by `$peerContactAddress`, assuming that the old one is stale. The previous connection will be gracefully closed. This happens only (if I am not mistaken) when the peer restarts the channel.")
						// Replace the old stale connection with a new one that uses the brand-new accepted connection's channel.
						delegate.replaceMyselfWithACommunicableDelegate(channel, receiverFromPeer, oChannelRemoteAddress, ACCEPTED)
							.startConversationAsServer(hello)
					} else if clusterService.whichChannelShouldIKeepWhenMutualConnectionWith(peerContactAddress) eq ACCEPTED then {
						scribe.info(s"${clusterService.myAddress}: I accepted a new connection initiated by $peerContactAddress despite we are already connected with a connection recently initiated by me. The replacement is because the accepted (and helloed) one has higher priority than the initiated by me (but still not welcomed). The old delegates's channel will be gracefully closed.")
						// Replace the old lower priority connection with the brand-new accepted higher priority one.
						delegate.replaceMyselfWithACommunicableDelegate(channel, receiverFromPeer, oChannelRemoteAddress, ACCEPTED)
							.startConversationAsServer(hello)
					} else {
						scribe.info(s"${clusterService.myAddress}: The connection initiated by `$peerContactAddress` that I had accepted (incubated), is being discarded to let us continue with the already established connection (that I have initiated) because it has higher priority.")
						// Keep the current delegate's connection (initiated by me) and discard the brand-new accepted connection.
						abortIncubated(Maybe.some(peerContactAddress))
					}

				case incommunicableDelegate: IncommunicableDelegate => // Happens if I knew the peer, but we are not fully connected.
					// If I am simultaneously initiating a connection to the peer as a client, then I have to cancel it (by removing the connecting delegate) and continue with accepted one (because it has already received the hello message).
					if incommunicableDelegate.isConnectingAsClient then {
						scribe.info(s"${clusterService.myAddress}: A connection from `$peerContactAddress` was accepted despite I am simultaneously trying to connect to him as client. Given the connection I am initiating has lower priority, I will discard it as soon as it completes.") // TODO investigate if it is possible to cancel the connection while it is in progress.
					}
					// If I know the peer and I am not currently trying to connect to it
					else {
						scribe.info(s"${clusterService.myAddress}: A connection from `$peerContactAddress`, which I considered incommunicable ${incommunicableDelegate.communicationStatus}, has been accepted. Let's see if we can communicate with it this time.")
					}
					val communicableDelegate = incommunicableDelegate.replaceMyselfWithACommunicableDelegate(channel, receiverFromPeer, oChannelRemoteAddress, ACCEPTED).get
					// TODO analyze if a notification of the communicability change should be issued here.
					communicableDelegate.startConversationAsServer(hello)
			}
		}
	}
}
