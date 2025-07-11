package readren.matrix
package cluster.service

import cluster.channel.Receiver
import cluster.channel.Transmitter.{Delivered, NotDelivered}
import cluster.serialization.ProtocolVersion
import cluster.service.ChannelOrigin.ACCEPTED
import cluster.service.Protocol.CommunicationStatus.HANDSHOOK
import cluster.service.Protocol.ContactAddress

import readren.taskflow.Maybe

import java.net.SocketAddress
import java.nio.channels.AsynchronousSocketChannel
import scala.util.control.NonFatal

/** Manages an accepted connection until a hello message is received or a reception timeout occurs.  */
class ParticipantDelegateEgg(participantService: ParticipantService, eggChannel: AsynchronousSocketChannel) {
	private val config = participantService.config.participantDelegatesConfig
	private val sequencer = participantService.sequencer
	private val oEggChannelRemoteAddress: Maybe[SocketAddress] = try Maybe.apply(eggChannel.getRemoteAddress) catch { case NonFatal(_) => Maybe.empty }
	private var oPeerContactAddress: Maybe[ContactAddress] = Maybe.empty

	val eggReceiver: Receiver = participantService.buildReceiverFor(eggChannel, () => oPeerContactAddress.fold(oEggChannelRemoteAddress)(Maybe.some), oEggChannelRemoteAddress)
	
	private def showChannelRemoteAddress: String = oEggChannelRemoteAddress.fold("unknown")(_.toString)

	def incubate(): Unit = {
		eggReceiver.receive[Protocol](ProtocolVersion.OF_THIS_PROJECT, config.receiverTimeout, config.timeUnit) {
			case fault: Receiver.Fault =>
				scribe.info(fault.scribeContent(s"${participantService.myAddress}: A connection to me initiated from `$showChannelRemoteAddress` was aborted because:")*)
				abortIncubated(oEggChannelRemoteAddress)
			case message: Hello =>
				if participantService.config.acceptedConnectionsFilter.test(message.myContactAddress) then {
					scribe.debug(s"${participantService.myAddress}: I have received the hello message `$message` from `${message.myContactAddress}`.")
					participantService.sequencer.executeSequentially {
						oPeerContactAddress = Maybe.some(message.myContactAddress)
						hatch(message)
					}
				} else {
					scribe.info(s"${participantService.myAddress}: A connection to me initiated by `${message.myContactAddress}` (with ephemeral address `$showChannelRemoteAddress`) was rejected by the contact-addresses' filter")
					abortIncubated(Maybe.some(message.myContactAddress))
				}
			case unexpectedMessage =>
				scribe.info(s"${participantService.myAddress}: A connection to me initiated from `$showChannelRemoteAddress` was discarded because the conversation was initiated with an unexpected message: $unexpectedMessage.")
				abortIncubated(oEggChannelRemoteAddress)
		}
	}
	
	private def abortIncubated(thePeerAddress: Maybe[ContactAddress]): Unit = {
		val transmitter = participantService.buildTransmitterFor(eggChannel, thePeerAddress)
		participantService.closeDiscardedChannelGracefully(eggChannel, transmitter, ACCEPTED)
	}
	
	private def hatch(hello: Hello): Unit = {
		val peerContactAddress = hello.myContactAddress
		if participantService.isShutDown then abortIncubated(Maybe.some(peerContactAddress))
		else {
			participantService.delegateByAddress.getOrElse(peerContactAddress, null) match {
				case null =>
					scribe.info(s"${participantService.myAddress}: I accepted a new connection initiated by `$peerContactAddress`.")
					val newParticipantDelegate = participantService.addANewCommunicableDelegate(peerContactAddress, eggChannel, eggReceiver, oEggChannelRemoteAddress, ACCEPTED)
					newParticipantDelegate.startConversationAsServer(hello)

				case incumbentDelegate: CommunicableDelegate =>
					// If I already have a communicable delegate associated with the peer, there are two possibilities:
					// 1) If either, I am at the server side of the delegate's channel (I accepted the connection), or I previously have received a Welcome response through the delegate's channel; then I am certain that delegate's channel is stale and the peer is reconnecting to me (because participants don't initiate a connection to a target that they are connecting or already connected). Therefore, I have to keep the new accepted connection and discard the old.
					// 2) If the connection of the delegated was initiated by me and I still haven't received the Welcome response (to the Hello), then most probably the peer and me have initiated a connection to each other simultaneously.
					// Note that the peer may be having the same problem right now. He has to decide which connection to keep: the one that he initiated as a client (and I am accepting now), or the one I previously initiated as a client (and he may be accepting now).
					// So we (the peer and me) have to agree which connection to keep.
					// We can't just keep the connection that completed earlier because that is vulnerable to race condition because each side may see a different connection completing first.
					// We have to establish an absolute and global criteria for this decision which is the comparison of the addresses: the kept connection is the one in which the server has the highest address. The other connection is gracefully closed.
					// The `ParticipantService.whichChannelShouldIKeepWhenMutualConnectionWith` method encapsulates said criteria.
					if (incumbentDelegate.channelOrigin eq ACCEPTED) || (incumbentDelegate.communicationStatus eq HANDSHOOK) then {
						scribe.info(s"${participantService.myAddress}: I accepted a new connection initiated by `$peerContactAddress`, assuming that the old one is stale. The previous connection will be gracefully closed. This happens only (if I am not mistaken) when the peer restarts the channel.")
						// Replace the old stale connection with a new one that uses the brand-new accepted connection's channel.
						replace(incumbentDelegate)
							.startConversationAsServer(hello)
					} else if participantService.whichChannelShouldIKeepWhenMutualConnectionWith(peerContactAddress) eq ACCEPTED then {
						scribe.info(s"${participantService.myAddress}: I accepted a new connection initiated by $peerContactAddress despite we are already connected with a connection recently initiated by me. The replacement is because the accepted (and helloed) one has higher priority than the initiated by me (but still not welcomed). The old delegates's channel will be gracefully closed.")
						// Replace the old lower priority connection with the brand-new accepted higher priority one.
						replace(incumbentDelegate)
							.startConversationAsServer(hello)
					} else {
						scribe.info(s"${participantService.myAddress}: The connection initiated by `$peerContactAddress` that I had accepted (incubated), is being discarded to let us continue with the already established connection (that I have initiated) because it has higher priority.")
						// Keep the current delegate's connection (initiated by me) and discard the brand-new accepted connection.
						abortIncubated(Maybe.some(peerContactAddress))
					}

				case incommunicableDelegate: IncommunicableDelegate => // Happens if I knew the peer, but we are not fully connected.
					// If I am simultaneously initiating a connection to the peer as a client, then I have to cancel it (by removing the connecting delegate) and continue with accepted one (because it has already received the hello message).
					if incommunicableDelegate.isConnectingAsClient then {
						scribe.info(s"${participantService.myAddress}: A connection from `$peerContactAddress` was accepted despite I am simultaneously trying to connect to him as client. Given the connection I am initiating has lower priority, I will discard it as soon as it completes.") // TODO investigate if it is possible to cancel the connection while it is in progress.
					}
					// If I know the peer and I am not currently trying to connect to it
					else {
						scribe.info(s"${participantService.myAddress}: A connection from `$peerContactAddress`, which I considered incommunicable ${incommunicableDelegate.communicationStatus}, has been accepted. Let's see if we can communicate with it this time.")
					}
					val communicableDelegate = incommunicableDelegate.replaceMyselfWithACommunicableDelegate(eggChannel, eggReceiver, oEggChannelRemoteAddress, ACCEPTED).get
					// TODO analyze if a notification of the communicability change should be issued here.
					communicableDelegate.startConversationAsServer(hello)
			}
		}
	}

	/**
	 * Replaces the `incumbentDelegate` with a new one, and gracefully closes the incumbent delegate's channel.
	 * The new delegate is created with the same `peerContactAddress`, but using the new `eggChannel`, `eggReceiver`, `oEggChannelRemoteAddress`, and `ACCEPTED` channel origin.
	 *
	 * Steps to gracefully close the incumbent delegate's channel:
	 *   1. The incumbent delegate is removed from the cluster's delegate registry (but not fully closed yet).
	 *   2. A new delegate is created and initialized with the state of the incumbent.
	 *   3. The [[ChannelDiscarded]] message is sent to the peer through the old (incumbent) channel to inform the peer that this channel is being replaced.
	 *   4. The output of the incumbent's channel is shut down immediately, regardless of whether the [[ChannelDiscarded]] message is delivered successfully or not.
	 *   5. The channel closing is completed by calling `completeChannelClosing` in both cases:
	 *      - If the message is delivered successfully, the call to `completeChannelClosing` is delayed by a configurable amount to allow the peer to close its side first.
	 *      - If the message delivery fails, `completeChannelClosing` is called immediately.
	 *   6. Notifications are sent to the membership-scoped behavior and listeners about the replacement.
	 *
	 * @return the new delegate.
	 */
	private def replace(incumbentDelegate: CommunicableDelegate): CommunicableDelegate = {

		participantService.removeDelegate(incumbentDelegate, false)

		val replacement = participantService.addANewCommunicableDelegate(incumbentDelegate.peerContactAddress, eggChannel, eggReceiver, oEggChannelRemoteAddress, ACCEPTED)
		replacement.initializeStateBasedOn(incumbentDelegate)

		// Send the [[ChannelDiscarded]] message to the peer through the discarded channel.
		incumbentDelegate.transmitToPeer(ChannelDiscarded) { report =>
			incumbentDelegate.channel.shutdownOutput()
			report match {
				case Delivered =>
					// If delivery is successful, schedule the remaining closing steps to happen later to allow the peer to close it first.
					sequencer.scheduleSequentially(sequencer.newDelaySchedule(config.closeDelay)) { () =>
						incumbentDelegate.completeChannelClosing()
					}
				case nd: NotDelivered =>
					scribe.trace(s"${participantService.myAddress}: The transmission of the $ChannelDiscarded message to `${incumbentDelegate.peerContactAddress}` through the channel that I ${incumbentDelegate.channelOrigin} failed with $nd.")
					// If delivery fails, continue with the remaining closing steps with no delay.
					incumbentDelegate.completeChannelClosing()
			}
		}

		// notify
		participantService.getMembershipScopedBehavior.onDelegateCommunicabilityChange(replacement) // TODO this isn't exactly a communicability change. Analyze it.
		participantService.notifyListenersThat(CommunicationChannelReplaced(incumbentDelegate.peerContactAddress))
		replacement
	}
}
