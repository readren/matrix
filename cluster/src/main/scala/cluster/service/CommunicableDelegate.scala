package readren.matrix
package cluster.service

import cluster.channel.Transmitter.{Delivered, NotDelivered}
import cluster.channel.{Receiver, SequentialTransmitter, Transmitter}
import cluster.serialization.ProtocolVersion
import cluster.service.ParticipantService.DelegateConfig
import cluster.service.Protocol.*
import cluster.service.Protocol.CommunicationStatus.{CONNECTED, HANDSHOOK}
import cluster.service.Protocol.IncommunicabilityReason.IS_CONNECTING_AS_CLIENT

import readren.matrix.cluster.service.behavior.MembershipScopedBehavior
import readren.taskflow.Maybe
import readren.taskflow.SchedulingExtension.MilliDuration

import java.net.SocketAddress
import java.nio.channels.AsynchronousSocketChannel
import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable
import scala.util.control.NonFatal

/** A [[ParticipantDelegate]] that is currently able to communicable with the participant. */
class CommunicableDelegate(
	override val participantService: ParticipantService,
	override val peerContactAddress: ContactAddress,
	val channel: AsynchronousSocketChannel,
	receiverFromPeer: Receiver,
	val channelId: ChannelId
) extends ParticipantDelegate, BehaviorsHelper { thisCommunicableDelegate =>
	val config: DelegateConfig = participantService.config.participantDelegatesConfig

	private val transmitterToPeer: SequentialTransmitter[Protocol] = participantService.buildTransmitterFor(channel, Maybe.some(peerContactAddress), channelId)

	private var agreedVersion: ProtocolVersion = ProtocolVersion.NOT_SPECIFIED

	inline def getAgreedVersion: ProtocolVersion = agreedVersion

	private var versionsSupportedByPeer: Set[ProtocolVersion] = Set.empty

	/** The state of the peer according to it */
	private[service] var peerStatePhoto: Maybe[MemberViewpoint] = Maybe.empty

	/** Only Used when the cluster service is in aspirant state. */
	private[service] var clusterCreatorProposedByPeer: ContactAddress | Null = null
	/** Memorizes which was the cluster-creator proposal sent to the peer. 
	 * Only Used when the cluster service is in aspirant state. */
	private[service] var lastClusterCreatorProposalSentToPeer: ContactAddress | Null = null

	@volatile private var isWaitingForAMessage: Boolean = false
	private val onReceptionCompleted: AtomicReference[Null | (() => Unit)] = AtomicReference(null)

	private var lastRequestId: Short = 0
	private val requestExchangeByRequestId: mutable.LongMap[OutgoingRequestExchange[?]] = mutable.LongMap.empty

	override def isCommunicable: Boolean = true

	override def isStable: Boolean = (agreedVersion != ProtocolVersion.NOT_SPECIFIED) && oPeerMembershipStatusAccordingToMe.isDefined

	override def communicationStatus: CommunicationStatus = if isStable then HANDSHOOK else CONNECTED

	inline def contactCard: ContactCard = (peerContactAddress, versionsSupportedByPeer)

	/** Exposes changes of the [[versionsSupportedByPeer]] and the [[oPeerMembershipStatusAccordingToMe]] to the [[participantService.getMembershipScopedBehavior]].
	 * The calls to [[participantService.getMembershipScopedBehavior.onDelegateCommunicabilityChange]] and [[participantService.getMembershipScopedBehavior.onDelegateMembershipChange]] are deferred to the end of the block and called a single time, no matter how many changes occur within the block. */
	private[service] inline def exposingChangesDo[T](inline body: () => T): T = {
		val previousPeersSupportedVersions = this.versionsSupportedByPeer
		val previousMembershipStatusOfPeerAccordingToMe = this.oPeerMembershipStatusAccordingToMe
		val result = body()
		if previousPeersSupportedVersions != this.versionsSupportedByPeer then participantService.getMembershipScopedBehavior.onDelegateCommunicabilityChange(this)
		if !previousMembershipStatusOfPeerAccordingToMe.isEqualTo(this.oPeerMembershipStatusAccordingToMe) then participantService.getMembershipScopedBehavior.onDelegateMembershipChange(this)
		result
	}

	/** @note should be called within an [[exposingChangesDo]] block. */
	private[service] def updateState(peersNewMembershipStatus: MembershipStatus, peersNewSupportedVersions: Set[ProtocolVersion] = versionsSupportedByPeer, newCreationInstant: Instant = peerCreationInstant): Unit = {
		oPeerMembershipStatusAccordingToMe = Maybe.some(peersNewMembershipStatus)
		versionsSupportedByPeer = peersNewSupportedVersions
		agreedVersion = participantService.determineAgreedVersion(peersNewSupportedVersions).getOrElse(ProtocolVersion.NOT_SPECIFIED)
		peerCreationInstant = newCreationInstant
	}

	/** Called just after a successfully connecting through a brand-new connection initiated by me and accepted by the peer (I am at the client side of the channel). */
	private[service] def startConversationAsClient(isReconnection: Boolean): Unit = {
		if isReconnection then sendPeerAHelloIAmBack()
		else sendPeerAHelloIExist()
		receiveNextMessages()
	}

	/** Called just after a successfully receiving the first message (which should be a [[Hello]]) through a brand-new connection initiated by the peer and accepted by me (I am at the server side of the channel). */
	private[service] def startConversationAsServer(hello: Hello): Unit = {
		val continueReceiving = exposingChangesDo { () =>
			participantService.getMembershipScopedBehavior.handleInitiatorMessageFrom(thisCommunicableDelegate, hello)
		}
		if continueReceiving then receiveNextMessages()
	}

	/** Receives the messages sent by the peer to me. The received messages are consumed sequentially and eagerly, but the handling of them is governed by the [[MembershipScopedBehavior.handleInitiatorMessageFrom]] which may defer some reactions. */
	private def receiveNextMessages(): Unit = {
		isWaitingForAMessage = true
		receiverFromPeer.receive[Protocol](agreedVersion, config.receiverTimeout, config.timeUnit) { receptionResult =>
			// Clear the flag that informs if the channel is waiting for, or receiving, a message. Should be cleared before accessing the `onReceptionCompleted` reference to avoid race condition in [[completeChannelClosing]].
			isWaitingForAMessage = false
			// Call the "on reception completed" callback potentially set by the [[completeChannelClosing]] method. 
			val channelClosingCallback = onReceptionCompleted.getAndSet(null)
			if channelClosingCallback != null then channelClosingCallback()
			val gracefulClosingAmendment = if channelClosingCallback != null then " and is gracefully closing" else ""
			
			receptionResult match {
				case responseFromPeer: Response =>
					scribe.debug(s"${participantService.myAddress}: I have received the response `$responseFromPeer from `$peerContactAddress` through channel $channelId$gracefulClosingAmendment.")
					sequencer.executeSequentially {
						requestExchangeByRequestId.remove(responseFromPeer.toRequest).fold {
							scribe.warn(s"${participantService.myAddress}: I have received a response to a request that I haven't made. It arrived from $peerContactAddress through channel $channelId and contains `$responseFromPeer`.")
						} { ore =>
							sequencer.cancel(ore.oTimeoutSchedule.get)
							val request = ore.oRequest.get
							val thisDelegateIsAssociated = isAssociated
							if !thisDelegateIsAssociated then scribe.info(s"The response `$responseFromPeer` from `$peerContactAddress` was ignored because it was handled after the delegate was removed. It was received through channel $channelId.")
							val continueReceiving = thisDelegateIsAssociated && exposingChangesDo(
								() => ore.asInstanceOf[OutgoingRequestExchange[request.type]].onResponse(request, responseFromPeer.asInstanceOf[request.ResponseType])
							)
							if continueReceiving then receiveNextMessages()
							else if thisDelegateIsAssociated then onTerminatingMessageReceived(responseFromPeer)
						}
					}

				case initiatorMessageFromPeer: NonResponse =>
					scribe.debug(s"${participantService.myAddress}: I have received the message `$initiatorMessageFromPeer` from `$peerContactAddress` through channel $channelId$gracefulClosingAmendment.")
					sequencer.executeSequentially {
						val thisDelegateIsAssociated = isAssociated
						if !thisDelegateIsAssociated then scribe.info(s"${participantService.myAddress}: The message `$initiatorMessageFromPeer` from `$peerContactAddress` was ignored because it was handled after the delegate was removed. It was received through channel $channelId.")
						val continueReceiving = thisDelegateIsAssociated && exposingChangesDo {
							() => participantService.getMembershipScopedBehavior.handleInitiatorMessageFrom(thisCommunicableDelegate, initiatorMessageFromPeer)
						}
						if continueReceiving then receiveNextMessages()
						else if thisDelegateIsAssociated then onTerminatingMessageReceived(initiatorMessageFromPeer)
					}

				case fault: Receiver.Fault =>
					if channelClosingCallback eq null then scribe.error(fault.scribeContent(s"${participantService.myAddress}: Failure while I was waiting for, or receiving, a message from `$peerContactAddress` through channel $channelId.") *)
					else scribe.trace(s"${participantService.myAddress}: Expected receiver failure due to graceful closing of the connection to $peerContactAddress with channel $channelId.")
					sequencer.executeSequentially {
						if isAssociated then restartChannel(s"The reception failed with $fault")
					}
			}
		}
	}

	private def onTerminatingMessageReceived(message: Protocol): Unit = {
		if isAssociated then scribe.info(s"${participantService.myAddress}: The connection to `$peerContactAddress` with channel $channelId is being closed because I received the terminating message `$message`. {communicationStatus: $communicationStatus}")
		else scribe.trace(s"${participantService.myAddress}: I received the terminating message `$message` from `$peerContactAddress` through the channel $channelId which is already closed.")
		if participantService.removeDelegate(thisCommunicableDelegate, true) then completeChannelClosing()
		// TODO analyze if something else should be done here (or it is responsibility of the message handlers when they return `false`).
	}


	private[service] def notifyPeerThatAConversationStartedWith(otherParticipant: ContactAddress, isARestartAfterReconnection: Boolean): Unit = {
		assert(otherParticipant != peerContactAddress && otherParticipant != participantService.myAddress)
		transmitToPeerOrRestartChannel(ConversationStartedWith(otherParticipant, isARestartAfterReconnection))
	}

	private[service] def notifyPeerThatILostCommunicationWith(otherParticipant: ContactAddress): Unit = {
		assert(otherParticipant != peerContactAddress && otherParticipant != participantService.myAddress)
		transmitToPeerOrRestartChannel(ILostCommunicationWith(otherParticipant))
	}

	private[service] def reportTransmissionFailure(failure: Transmitter.NotDelivered): Unit = {
		failure match {
			case transmissionFailure: Transmitter.TransmissionFailure =>
				scribe.error(s"${participantService.myAddress}: I failed to transmit the message `${transmissionFailure.rootMessage}` to `$peerContactAddress` through channel $channelId.", transmissionFailure.cause)
			case serializationFailure: Transmitter.SerializationProblem =>
				scribe.error(s"${participantService.myAddress}: I failed to serialize the message `${serializationFailure.rootMessage}` (at position ${serializationFailure.problem.position}) for `$peerContactAddress` ${if serializationFailure.aFragmentWasTransmitted then "after" else "before"} the transmission started, through channel $channelId.", serializationFailure.problem)
		}
	}

	/** Reports the transmission failure and then calls the specified consumer.
	 * @note The consumer is called within a [[sequencer.executeSequentially]] block but not within a [[exposingChangesDo]] block.
	 */
	private[service] def ifFailureReportItAndThen(consumer: Transmitter.NotDelivered => Unit)(report: Transmitter.Report): Unit = {
		report match {
			case Transmitter.Delivered => // do nothing

			case failure: Transmitter.NotDelivered =>
				reportTransmissionFailure(failure)
				sequencer.executeSequentially(consumer(failure))
		}
	}

	/** Specifies what the [[askPeer]] method requires:
	 * - to build the requesting message to be sent to the peer
	 * - and to handle each of the three mutually exclusive outcomes.
	 * Also maintains the state or the request.
	 * Instances of this class are single-use and should be disposed after its usage. To ensure this create the instance inline when calling the [[ask]] method.
	 * Exactly one of the `on*` methods is called once, and the call occurs within a [[sequencer.executeSequentially]] block. */
	private[service] abstract class OutgoingRequestExchange[Q <: Request](val responseTimeout: MilliDuration = config.responseTimeout) { thisOutgoingRequestExchange =>

		private[CommunicableDelegate] var oTimeoutSchedule: Maybe[sequencer.Schedule] = Maybe.empty
		private[CommunicableDelegate] var oRequest: Maybe[Q] = Maybe.empty

		/** The implementation should build a [[Request]] with the specified [[RequestId]].
		 * Called within the thread that called [[askPeer]], which should be the [[sequencer]]'s thread. */
		def buildRequest(requestId: RequestId): Q

		/** Called (within a [[sequencer.executeSequentially]] block and within an [[exposingChangesDo]] block) if a response to the request is received.
		 * Should return `true` for the conversation with the peer to continue. */
		def onResponse(request: Q, response: request.ResponseType): Boolean

		/** Called (within a [[sequencer.executeSequentially]] block and within an [[exposingChangesDo]] block) if the transmission of the request fails. */
		def onTransmissionError(request: Q, error: NotDelivered): Unit

		/** Called (within a [[sequencer.executeSequentially]] block and within an [[exposingChangesDo]] block) if no response to the request is received within the time passed to the `responseTimeout` parameter of the [[askPeer]] method. */
		def onTimeout(request: Q): Unit
	}

	/** Sends a [[Request]] to the peer and then calls once exactly one of the `on*` methods of the specified [[OutgoingRequestExchange]]. */
	private[service] def askPeer[Q <: Request](requestExchange: OutgoingRequestExchange[Q]): Unit = {
		assert(sequencer.assistant.isWithinDoSiThEx)
		val requestId = lastRequestId.incremented
		lastRequestId = requestId
		val request = requestExchange.buildRequest(requestId)
		requestExchange.oRequest = Maybe.some(request)
		transmitToPeer(request) {
			case Delivered =>
				sequencer.executeSequentially {
					val timeoutSchedule = sequencer.newDelaySchedule(requestExchange.responseTimeout)
					requestExchange.oTimeoutSchedule = Maybe.some(timeoutSchedule)
					requestExchangeByRequestId.put(requestId, requestExchange)
					sequencer.scheduleSequentially(timeoutSchedule) { () =>
						requestExchangeByRequestId.remove(requestId).foreach { removedExchange =>
							assert(removedExchange eq requestExchange)
							if isAssociated then exposingChangesDo { () => requestExchange.onTimeout(request) }
						}
					}
				}
			case nd: NotDelivered =>
				reportTransmissionFailure(nd)
				sequencer.executeSequentially {
					exposingChangesDo { () =>
						requestExchange.onTransmissionError(request, nd)
					}
				}
		}
	}


	/** A partially implemented [[OutgoingRequestExchange]] that:
	 *  - retries a single time if a non-response timeout occurs,
	 *  - and causes a channel restart if either the transmission fails or a non-response timeouts occurs twice. */
	private[service] abstract class SingleRetryOutgoingRequestExchange[Q <: Request](responseTimeout: MilliDuration = config.responseTimeout, retryDelay: MilliDuration = config.requestRetryDelay) extends OutgoingRequestExchange[Q](responseTimeout) {
		private[CommunicableDelegate] var isFirstTry: Boolean = true

		override def onTransmissionError(request: Q, error: NotDelivered): Unit = {
			restartChannel(error)
		}

		/** Since the [[AsynchronousSocketChannel]] uses TCP, it ensures loss-less in-order delivery. Therefore, the purpose to retry a request after a no-response timeout is to reveal any silent communication problem or to detect silent middlebox interference.
		 * If the peer is alive but not responding (e.g., deadlock, overload), retries would not help.
		 * In conclusion, no more than a retry is needed. */
		override def onTimeout(request: Q): Unit = {
			if isFirstTry then {
				isFirstTry = false
				val schedule: sequencer.Schedule = sequencer.newDelaySchedule(retryDelay)
				sequencer.scheduleSequentially(schedule) { () => askPeer(this) }
			}
			else restartChannel(s"Non-response timeout: request ${oRequest.get} was sent twice (initial attempt got no response within response timeout), but no response was received.")
		}
	}

	private[service] def transmitToPeerOrRestartChannel(message: Protocol): Unit = {
		transmitToPeer(message)(ifFailureReportItAndThen(restartChannel))
	}

	private[service] def transmitToPeer(message: Protocol)(onTransmissionComplete: Transmitter.Report => Unit): Unit = {
		transmitterToPeer.transmit(message, agreedVersion, config.behindTransmissionEnabled, config.transmitterTimeout, config.timeUnit)(onTransmissionComplete)
		scribe.trace(s"${participantService.myAddress}: The message `$message` was queued for transmission to `$peerContactAddress` through channel $channelId.")
	}

	/** Replaces this communicable delegate with an incommunicable one if not already removed. */
	protected def replaceMyselfWithAnIncommunicableDelegate(reason: IncommunicabilityReason, motive: Any): Maybe[IncommunicableDelegate] = {
		// replace me with an incommunicable delegate.
		if participantService.removeDelegate(this, false) then {
			val myReplacement = participantService.addANewIncommunicableDelegate(peerContactAddress, reason)
			myReplacement.initializeStateBasedOn(this)

			// Delay channel closing to allow the peer to close the channel first.
			sequencer.scheduleSequentially(sequencer.newDelaySchedule(config.closeDelay)) { () =>
				completeChannelClosing()
			}

			// notify
			participantService.getMembershipScopedBehavior.onDelegateCommunicabilityChange(myReplacement) // TODO consider moving this line inside the `notify*` method called in the next line
			participantService.notifyListenersAndOtherParticipantsThatAParticipantBecomeIncommunicable(peerContactAddress, reason, motive)
			Maybe.some(myReplacement)
		} else Maybe.empty
	}

	override def removeByOther(): Unit = {
		if participantService.removeDelegate(this, true) then completeChannelClosing()
	}

	private[service] def restartChannel(notDeliveredCause: Transmitter.NotDelivered): Unit =
		restartChannel(s"A transmission failed with $notDeliveredCause.")

	private[service] def restartChannel(motive: String): Unit = {
		scribe.debug(s"${participantService.myAddress}: Restarting the connection to `$peerContactAddress` with channel $channelId because: $motive")
		replaceMyselfWithAnIncommunicableDelegate(IS_CONNECTING_AS_CLIENT, s"To restart channel $channelId because of: $motive").foreach { myReplacement =>
			participantService.connectToAndThenStartConversationWithParticipant(myReplacement, true)
		}
	}

	/** Initiates a graceful closing of the [[channel]] by waiting for both transmission and reception to be idle to do the actual closing.
	 *
	 * @note This method can be called from any thread.
	 */
	private[service] def completeChannelClosing(): Unit = {
		assert(!isAssociated)
		scribe.trace(s"${participantService.myAddress}: About to complete the closing of channel $channelId to `$peerContactAddress`.")

		// Set the "on reception complete" callback first (before checking the [[isWaitingForAMessage]] flag) to avoid race condition.
		onReceptionCompleted.set { () =>
			// Reception completed, now wait for transmission to be idle
			transmitterToPeer.triggerOnIdle { () =>
							// Both reception and transmission are now idle, safe to close
			scribe.trace(s"${participantService.myAddress}: Both reception and transmission are idle, closing channel $channelId to `$peerContactAddress`.")
				try {
					channel.shutdownInput()
					channel.close()
				} catch {
					case NonFatal(e) =>
						scribe.warn(s"${participantService.myAddress}: Exception while closing channel $channelId to `$peerContactAddress`:", e)
				}
			}
		}

		// Check if reception is currently in progress
		if isWaitingForAMessage then {
			// If reception is in progress, the callback referenced by `onReceptionCompleted` will be triggered when it completes.
			scribe.trace(s"${participantService.myAddress}: Waiting for current reception to complete before closing channel $channelId to `$peerContactAddress`.")
		} else {
			// If no reception in progress, trigger the callback immediately 
			val callback = onReceptionCompleted.getAndSet(null)
			if callback != null then callback()
		}
	}
}
