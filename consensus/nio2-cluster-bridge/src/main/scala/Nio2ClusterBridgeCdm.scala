package readren.consensus
package nio2

import readren.common.Maybe
import readren.consensus.ConsensusClusterBridgeCdm
import readren.consensus.protocol.*
import readren.nexus.cluster.channel.{PipelinedTransmitter, Receiver}
import readren.nexus.cluster.serialization.*
import readren.nexus.cluster.service.{ChannelId, ChannelOrigin}
import readren.sequencer.*

import java.io.IOException
import java.net.{InetSocketAddress, SocketAddress}
import java.nio.channels.{AsynchronousServerSocketChannel, AsynchronousSocketChannel, ClosedChannelException, CompletionHandler}
import java.util.concurrent.{TimeUnit, TimeoutException}
import scala.collection.immutable.ListSet
import scala.collection.mutable
import scala.compiletime.deferred
import scala.reflect.ClassTag
import scala.util.Try
import scala.util.control.NonFatal

trait Nio2ClusterBridgeCdm extends ConsensusClusterBridgeCdm { thisSdm =>

	override val sequencer: SchedulingDoer

	given participantIdSerializer: Serializer[ParticipantId] = deferred
	given participantIdDeserializer: Deserializer[ParticipantId] = deferred
	given clientCommandSerializer: Serializer[ClientCommand] = deferred
	given clientCommandDeserializer: Deserializer[ClientCommand] = deferred
	given stateMachineResponseSerializer: Serializer[StateMachineResponse] = deferred
	given stateMachineResponseDeserializer: Deserializer[StateMachineResponse] = deferred

	def contactAddressOf(id: ParticipantId): SocketAddress
	def localBindAddress: SocketAddress
	val bridgeConfig: ClusterBridgeConfig = ClusterBridgeConfig.Default

	val wireProtocol: ClusterWireProtocol[thisSdm.type] = new ClusterWireProtocol[thisSdm.type](thisSdm)
	import wireProtocol.{*, given}

	class Nio2ClusterBridge(override val boundParticipantId: ParticipantId) extends thisSdm.ClusterBridge { thisBridge =>

		private final class PendingRpc[R](val timeoutSchedule: sequencer.Schedule) extends sequencer.Captor[R]

		private var boundDelegate: thisBridge.Delegate | Null = null
		private var serverChannel: AsynchronousServerSocketChannel | Null = null
		@volatile private var isClosed: Boolean = false

		private var memorizedProbableParticipants: ListSet[ParticipantId] = ListSet.empty
		private val peerConnections: mutable.Map[ParticipantId, PeerConnection] = mutable.Map.empty
		private val clientConnections: mutable.Set[ClientConnection] = mutable.Set.empty

		override def getOtherProbableParticipants: ListSet[ParticipantId] = memorizedProbableParticipants

		override def setBound(delegate: thisBridge.Delegate): Unit = {
			boundDelegate = delegate
		}

		override def removeBound(): Unit = {
			boundDelegate = null
		}

		override def onActiveElectorateChanged(change: ElectorateChange[ParticipantId], changeIndex: RecordIndex, roleOrdinal: RoleOrdinal): Unit = {
			val activePeers = change match {
				case sole: SoleElectorateChange[ParticipantId] => sole.activeParticipants
				case joint: JointElectorateChange[ParticipantId] => joint.activeParticipants
			}
			for peer <- activePeers if peer != boundParticipantId do {
				memorizedProbableParticipants = memorizedProbableParticipants + peer
			}
		}

		override def onQuiesced(motive: Try[String]): Unit = close()

		override def requestWakeUp(reason: WakeUpReason, wakeUpsDone: Int, callback: () => Unit): WakeUpToken = {
			val baseDelay = reason match {
				case WakeUpReason.QuiescenceAuthorizationRetry => bridgeConfig.quiescenceAuthorizationRetryDelay
				case WakeUpReason.UnreachableFollowersRetry => math.min(bridgeConfig.unreachableFollowersRetryDelay * (1L << math.min(wakeUpsDone, 5)), 5000L)
				case WakeUpReason.RetirementDriveRetry => bridgeConfig.retirementDriveRetryDelay
				case WakeUpReason.ReplicationLoopRetry => bridgeConfig.replicationLoopRetryDelay
			}
			val schedule = sequencer.newDelaySchedule(baseDelay)
			sequencer.schedule(schedule) { _ =>
				callback()
			}
			new WakeUpToken {
				override def cancel(): Unit = sequencer.cancel(schedule)
			}
		}

		def start(): Unit = {
			if serverChannel == null then {
				val ssc = AsynchronousServerSocketChannel.open().bind(localBindAddress)
				serverChannel = ssc
				acceptNext(ssc)
			}
		}

		def localAddress: SocketAddress = {
			val ch = serverChannel
			if ch != null && ch.isOpen then ch.getLocalAddress else localBindAddress
		}

		def close(): Unit = {
			isClosed = true
			if serverChannel != null then {
				try serverChannel.close() catch { case _: Throwable => () }
				serverChannel = null
			}
			sequencer.run {
				for (_, conn) <- peerConnections do {
					conn.close()
				}
				peerConnections.clear()
				for client <- clientConnections do {
					client.close()
				}
				clientConnections.clear()
				memorizedProbableParticipants = ListSet.empty
			}
		}

		private def acceptNext(ssc: AsynchronousServerSocketChannel): Unit = {
			if !isClosed && ssc.isOpen then {
				ssc.accept(null, new CompletionHandler[AsynchronousSocketChannel, Null] {
					override def completed(socketChannel: AsynchronousSocketChannel, attachment: Null): Unit = {
						acceptNext(ssc)
						incubateAcceptedChannel(socketChannel)
					}

					override def failed(exc: Throwable, attachment: Null): Unit = {
						if !isClosed && ssc.isOpen then {
							acceptNext(ssc)
						}
					}
				})
			}
		}

		private def determineAgreedVersion(peerVersions: Set[ProtocolVersion]): Option[ProtocolVersion] = {
			bridgeConfig.supportedVersions.intersect(peerVersions).minOption(using ProtocolVersion.newerFirstOrdering)
		}

		private def incubateAcceptedChannel(socketChannel: AsynchronousSocketChannel): Unit = {
			val localAddr = try socketChannel.getLocalAddress catch { case _: Throwable => new InetSocketAddress("127.0.0.1", 0) }
			val remoteAddr = try Maybe(socketChannel.getRemoteAddress) catch { case _: Throwable => Maybe.empty }

			val transmitterContext = new PipelinedTransmitter.Context {
				override def myAddress: SocketAddress = localAddr
				override def oPeerAddress: Maybe[SocketAddress] = remoteAddr
				override def channelId: ChannelId = new ChannelId(ChannelOrigin.ACCEPTED, remoteAddr)
			}
			val receiverContext = new Receiver.Context {
				override def myAddress: SocketAddress = localAddr
				override def oPeerAddress: Maybe[SocketAddress] = remoteAddr
				override def channelId: ChannelId = new ChannelId(ChannelOrigin.ACCEPTED, remoteAddr)
			}

			val transmitter = new PipelinedTransmitter[ClusterWireMessage](
				socketChannel,
				transmitterContext,
				buffersCapacity = bridgeConfig.frameCapacity,
				maxBuffers = bridgeConfig.maxBuffers,
				minBuffersOnIdle = bridgeConfig.minBuffersOnIdle,
				streamTransferBufferCapacity = bridgeConfig.streamTransferBufferCapacity
			)
			val receiver = new Receiver(
				socketChannel,
				receiverContext,
				buffersCapacity = bridgeConfig.frameCapacity,
				maxBuffers = bridgeConfig.maxBuffers,
				minBuffersOnIdle = bridgeConfig.minBuffersOnIdle,
				allocationLimits = bridgeConfig.allocationLimits,
				streamTransferBufferCapacity = bridgeConfig.streamTransferBufferCapacity
			)

			receiver.receive[ClusterWireMessage](
				ProtocolVersion.OF_THIS_PROJECT,
				bridgeConfig.connectTimeout,
				TimeUnit.MILLISECONDS
			) {
				case PeerHandshake(remotePeerId, remoteVersions) =>
					determineAgreedVersion(remoteVersions) match {
						case None =>
							transmitter.transmit(
								HandshakeReject(s"Incompatible protocol version(s): $remoteVersions"),
								ProtocolVersion.OF_THIS_PROJECT
							) { _ =>
								try socketChannel.close() catch { case _: Throwable => () }
							}(using clusterWireMessageSerializer)
						case Some(agreedVersion) =>
							val ack = HandshakeAck(boundParticipantId, agreedVersion)
							transmitter.transmit(ack, agreedVersion) { _ => () }(using clusterWireMessageSerializer)
							sequencer.run {
								hatchAcceptedPeer(remotePeerId, socketChannel, receiver, transmitter, agreedVersion)
							}
					}

				case ClientHandshake(clientVersions) =>
					determineAgreedVersion(clientVersions) match {
						case None =>
							transmitter.transmit(
								HandshakeReject(s"Incompatible protocol version(s): $clientVersions"),
								ProtocolVersion.OF_THIS_PROJECT
							) { _ =>
								try socketChannel.close() catch { case _: Throwable => () }
							}(using clusterWireMessageSerializer)
						case Some(agreedVersion) =>
							val ack = HandshakeAck(boundParticipantId, agreedVersion)
							transmitter.transmit(ack, agreedVersion) { _ => () }(using clusterWireMessageSerializer)
							sequencer.run {
								hatchAcceptedClient(socketChannel, receiver, transmitter, agreedVersion)
							}
					}

				case fault: Receiver.Fault =>
					try socketChannel.close() catch { case _: Throwable => () }

				case other =>
					try socketChannel.close() catch { case _: Throwable => () }
			}(using clusterWireMessageDeserializer)
		}

		private def hatchAcceptedPeer(
			remotePeerId: ParticipantId,
			socketChannel: AsynchronousSocketChannel,
			receiver: Receiver,
			transmitter: PipelinedTransmitter[ClusterWireMessage],
			agreedVersion: ProtocolVersion
		): Unit = {
			assert(sequencer.isInSequence)
			if isClosed then {
				try socketChannel.close() catch { case _: Throwable => () }
			} else {
				peerConnections.get(remotePeerId) match {
					case Some(existing) if existing.isConnected =>
						val cmp = participantIdComparator.compare(boundParticipantId, remotePeerId)
						if cmp > 0 then {
							existing.closeSilently()
							val conn = new PeerConnection(remotePeerId, ChannelOrigin.ACCEPTED)
							conn.attach(socketChannel, receiver, transmitter, agreedVersion)
							peerConnections.put(remotePeerId, conn)
							memorizedProbableParticipants = memorizedProbableParticipants + remotePeerId
							conn.startReceiving()
						} else {
							try socketChannel.close() catch { case _: Throwable => () }
						}
					case Some(existing) =>
						existing.attach(socketChannel, receiver, transmitter, agreedVersion)
						memorizedProbableParticipants = memorizedProbableParticipants + remotePeerId
						existing.startReceiving()
					case None =>
						val conn = new PeerConnection(remotePeerId, ChannelOrigin.ACCEPTED)
						conn.attach(socketChannel, receiver, transmitter, agreedVersion)
						peerConnections.put(remotePeerId, conn)
						memorizedProbableParticipants = memorizedProbableParticipants + remotePeerId
						conn.startReceiving()
				}
			}
		}

		private def hatchAcceptedClient(
			socketChannel: AsynchronousSocketChannel,
			receiver: Receiver,
			transmitter: PipelinedTransmitter[ClusterWireMessage],
			agreedVersion: ProtocolVersion
		): Unit = {
			assert(sequencer.isInSequence)
			if isClosed then {
				try socketChannel.close() catch { case _: Throwable => () }
			} else {
				val client = new ClientConnection(socketChannel, receiver, transmitter, agreedVersion)
				clientConnections.add(client)
				client.startReceiving()
			}
		}

		private def getOrConnectPeer(peerId: ParticipantId): PeerConnection = {
			assert(sequencer.isInSequence)
			peerConnections.get(peerId) match {
				case Some(conn) => conn
				case None =>
					val conn = new PeerConnection(peerId, ChannelOrigin.INITIATED)
					peerConnections.put(peerId, conn)
					startOutboundConnect(peerId, conn)
					conn
			}
		}

		private def startOutboundConnect(peerId: ParticipantId, conn: PeerConnection): Unit = {
			assert(sequencer.isInSequence)
			if isClosed then {
				conn.failAllPending(new ClosedChannelException())
			} else {
				conn.isConnecting = true
				val targetAddress = contactAddressOf(peerId)
				val clientChannel = AsynchronousSocketChannel.open()
				clientChannel.connect(targetAddress, null, new CompletionHandler[Void, Null] {
					override def completed(result: Void, attachment: Null): Unit = {
						val localAddr = try clientChannel.getLocalAddress catch { case _: Throwable => new InetSocketAddress("127.0.0.1", 0) }
						val remoteAddr = try Maybe(clientChannel.getRemoteAddress) catch { case _: Throwable => Maybe.empty }

						val transmitterContext = new PipelinedTransmitter.Context {
							override def myAddress: SocketAddress = localAddr
							override def oPeerAddress: Maybe[SocketAddress] = remoteAddr
							override def channelId: ChannelId = new ChannelId(ChannelOrigin.INITIATED, remoteAddr)
						}
						val receiverContext = new Receiver.Context {
							override def myAddress: SocketAddress = localAddr
							override def oPeerAddress: Maybe[SocketAddress] = remoteAddr
							override def channelId: ChannelId = new ChannelId(ChannelOrigin.INITIATED, remoteAddr)
						}

						val transmitter = new PipelinedTransmitter[ClusterWireMessage](
							clientChannel,
							transmitterContext,
							buffersCapacity = bridgeConfig.frameCapacity,
							maxBuffers = bridgeConfig.maxBuffers,
							minBuffersOnIdle = bridgeConfig.minBuffersOnIdle,
							streamTransferBufferCapacity = bridgeConfig.streamTransferBufferCapacity
						)
						val receiver = new Receiver(
							clientChannel,
							receiverContext,
							buffersCapacity = bridgeConfig.frameCapacity,
							maxBuffers = bridgeConfig.maxBuffers,
							minBuffersOnIdle = bridgeConfig.minBuffersOnIdle,
							allocationLimits = bridgeConfig.allocationLimits,
							streamTransferBufferCapacity = bridgeConfig.streamTransferBufferCapacity
						)

						val handshakeMessage = PeerHandshake(boundParticipantId, bridgeConfig.supportedVersions)
						transmitter.transmit(handshakeMessage, ProtocolVersion.OF_THIS_PROJECT) { _ => () }(using clusterWireMessageSerializer)

						receiver.receive[ClusterWireMessage](
							ProtocolVersion.OF_THIS_PROJECT,
							bridgeConfig.connectTimeout,
							TimeUnit.MILLISECONDS
						) {
							case HandshakeAck(ackResponderId, agreedVersion) =>
								if !bridgeConfig.supportedVersions.contains(agreedVersion) then {
									try clientChannel.close() catch { case _: Throwable => () }
									sequencer.run {
										conn.failAllPending(new IOException(s"Peer $peerId agreed on unsupported protocol version: $agreedVersion"))
										peerConnections.remove(peerId)
									}
								} else {
									sequencer.run {
										if isClosed then {
											try clientChannel.close() catch { case _: Throwable => () }
											conn.failAllPending(new ClosedChannelException())
										} else {
											peerConnections.get(peerId) match {
												case Some(existing) if (existing ne conn) && existing.isConnected =>
													val cmp = participantIdComparator.compare(boundParticipantId, peerId)
													if cmp > 0 then {
														try clientChannel.close() catch { case _: Throwable => () }
														conn.transferPendingTo(existing)
													} else {
														existing.closeSilently()
														conn.attach(clientChannel, receiver, transmitter, agreedVersion)
														memorizedProbableParticipants = memorizedProbableParticipants + peerId
														conn.startReceiving()
													}
												case _ =>
													conn.attach(clientChannel, receiver, transmitter, agreedVersion)
													memorizedProbableParticipants = memorizedProbableParticipants + peerId
													conn.startReceiving()
											}
										}
									}
								}

							case HandshakeReject(reason) =>
								try clientChannel.close() catch { case _: Throwable => () }
								sequencer.run {
									conn.failAllPending(new IOException(s"Peer $peerId rejected handshake: $reason"))
									peerConnections.remove(peerId)
								}

							case fault: Receiver.Fault =>
								try clientChannel.close() catch { case _: Throwable => () }
								sequencer.run {
									conn.failAllPending(new IOException(s"Peer $peerId connection fault during handshake: $fault"))
									peerConnections.remove(peerId)
								}

							case other =>
								try clientChannel.close() catch { case _: Throwable => () }
								sequencer.run {
									conn.failAllPending(new IOException(s"Peer $peerId sent unexpected initial message: $other"))
									peerConnections.remove(peerId)
								}
						}(using clusterWireMessageDeserializer)
					}

					override def failed(exc: Throwable, attachment: Null): Unit = {
						sequencer.run {
							conn.failAllPending(exc)
							peerConnections.remove(peerId)
						}
					}
				})
			}
		}

		extension (destinationId: ParticipantId) {

			override def howAreYou(inquirerInfo: StateInfo): sequencer.Capture[StateInfo] = {
				executeOutboundRpc[StateInfo](destinationId, corrId => HowAreYou(corrId, inquirerInfo), bridgeConfig.fastRpcTimeout)
			}

			override def requestBallot(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.Capture[Ballot] = {
				executeOutboundRpc[Ballot](destinationId, corrId => RequestBallot(corrId, inquirerInfo), bridgeConfig.fastRpcTimeout)
			}

			override def appendRecords(
				inquirerTerm: Term,
				prevLogIndex: RecordIndex,
				prevLogTerm: Term,
				batch: IArray[Record],
				leaderCommit: RecordIndex,
				termAtLeaderCommit: Term
			): sequencer.Capture[AppendResult] = {
				executeOutboundRpc[AppendResult](
					destinationId,
					corrId => AppendRecords(corrId, inquirerTerm, prevLogIndex, prevLogTerm, batch, leaderCommit, termAtLeaderCommit),
					bridgeConfig.fastRpcTimeout
				)
			}

			override def installSnapshot(
				inquirerTerm: Term,
				snapshot: SnapshotData[ParticipantId],
				batch: IArray[Record],
				leaderCommit: RecordIndex,
				termAtLeaderCommit: Term
			): sequencer.Capture[AppendResult] = {
				executeOutboundRpc[AppendResult](
					destinationId,
					corrId => InstallSnapshot(corrId, inquirerTerm, snapshot, batch, leaderCommit, termAtLeaderCommit),
					bridgeConfig.snapshotRpcTimeout
				)
			}

			override def permitQuiescence(indexOfGrantedSec: RecordIndex): sequencer.Capture[Unit] = {
				executeOutboundRpc[Unit](
					destinationId,
					corrId => PermitQuiescence(corrId, indexOfGrantedSec),
					bridgeConfig.fastRpcTimeout
				)
			}
		}

		private def executeOutboundRpc[R](
			destinationId: ParticipantId,
			buildMessage: CorrelationId => ConsensusRpcRequest,
			timeoutDuration: MilliDuration
		)(using ClassTag[R]): sequencer.Capture[R] = {
			if isClosed then sequencer.Failed(new ClosedChannelException())
			else {
				val conn = getOrConnectPeer(destinationId)
				val corrId = conn.nextCorrelationId()

				val timeoutSchedule = sequencer.newDelaySchedule(timeoutDuration)
				val pendingRpc = new PendingRpc[R](timeoutSchedule)
				sequencer.schedule(timeoutSchedule) { _ =>
					conn.removePending(corrId).foreach { pending =>
						pending.asInstanceOf[sequencer.Captor[R]].trapSync(
							new TimeoutException(s"RPC to $destinationId with CorrelationId $corrId timed out after ${timeoutDuration}ms")
						)
					}
				}
				conn.registerPending(corrId, pendingRpc)

				val message = buildMessage(corrId)
				conn.transmitMessage(message)
				pendingRpc
			}
		}

		private final class PeerConnection(val peerId: ParticipantId, var origin: ChannelOrigin) {
			var channel: AsynchronousSocketChannel | Null = null
			var receiver: Receiver | Null = null
			var transmitter: PipelinedTransmitter[ClusterWireMessage] | Null = null
			var agreedVersion: ProtocolVersion = ProtocolVersion.OF_THIS_PROJECT
			var isConnected: Boolean = false
			var isConnecting: Boolean = false
			var lastCorrelationId: CorrelationId = CorrelationId.Initial

			val pendingRequests: mutable.Map[CorrelationId, PendingRpc[?]] = mutable.Map.empty
			val pendingOutboundMessages: mutable.Queue[ClusterWireMessage] = mutable.Queue.empty

			def nextCorrelationId(): CorrelationId = {
				val next = lastCorrelationId.next
				lastCorrelationId = next
				next
			}

			def registerPending(corrId: CorrelationId, pending: PendingRpc[?]): Unit = {
				pendingRequests.remove(corrId).foreach { stale =>
					sequencer.cancel(stale.timeoutSchedule)
					stale.asInstanceOf[sequencer.Captor[Any]].trapSync(
						new TimeoutException(s"CorrelationId $corrId lapsed upon circular sequence wrap-around")
					)
				}
				pendingRequests.put(corrId, pending)
			}

			def removePending(corrId: CorrelationId): Option[PendingRpc[?]] = {
				pendingRequests.remove(corrId)
			}

			def attach(
				newChannel: AsynchronousSocketChannel,
				newReceiver: Receiver,
				newTransmitter: PipelinedTransmitter[ClusterWireMessage],
				newAgreedVersion: ProtocolVersion
			): Unit = {
				channel = newChannel
				receiver = newReceiver
				transmitter = newTransmitter
				agreedVersion = newAgreedVersion
				isConnected = true
				isConnecting = false
				flushPendingOutbound()
			}

			def transmitMessage(msg: ClusterWireMessage): Unit = {
				if isConnected && transmitter != null then {
					transmitter.transmit(msg, agreedVersion) { _ => () }(using clusterWireMessageSerializer)
				} else {
					pendingOutboundMessages.enqueue(msg)
					if !isConnecting then {
						startOutboundConnect(peerId, this)
					}
				}
			}

			private def flushPendingOutbound(): Unit = {
				while pendingOutboundMessages.nonEmpty && isConnected && transmitter != null do {
					val msg = pendingOutboundMessages.dequeue()
					transmitter.transmit(msg, agreedVersion) { _ => () }(using clusterWireMessageSerializer)
				}
			}

			def transferPendingTo(target: PeerConnection): Unit = {
				for (corrId, pending) <- pendingRequests do {
					target.pendingRequests.put(corrId, pending)
				}
				pendingRequests.clear()
				while pendingOutboundMessages.nonEmpty do {
					target.transmitMessage(pendingOutboundMessages.dequeue())
				}
			}

			def startReceiving(): Unit = {
				val r = receiver
				if r != null && isConnected && !isClosed then {
					r.receive[ClusterWireMessage](
						agreedVersion,
						bridgeConfig.receiverTimeout,
						TimeUnit.MILLISECONDS
					) {
						case response: ConsensusRpcResponse =>
							sequencer.run {
								if !response.correlationId.isLapsedRelativeTo(lastCorrelationId) then {
									pendingRequests.remove(response.correlationId).foreach { pending =>
										sequencer.cancel(pending.timeoutSchedule)
										response match {
											case HowAreYouReply(_, stateInfo) =>
												pending.asInstanceOf[sequencer.Captor[StateInfo]].captureSync(stateInfo)
											case RequestBallotReply(_, ballot) =>
												pending.asInstanceOf[sequencer.Captor[Ballot]].captureSync(ballot)
											case AppendRecordsReply(_, result) =>
												pending.asInstanceOf[sequencer.Captor[AppendResult]].captureSync(result)
											case InstallSnapshotReply(_, result) =>
												pending.asInstanceOf[sequencer.Captor[AppendResult]].captureSync(result)
											case PermitQuiescenceReply(_) =>
												pending.asInstanceOf[sequencer.Captor[Unit]].captureSync(())
										}
									}
								}
								startReceiving()
							}

						case request: ConsensusRpcRequest =>
							sequencer.run {
								val delegate = boundDelegate
								if delegate != null then {
									request match {
										case HowAreYou(corrId, info) =>
											val cap = delegate.onHowAreYou(peerId, info)
											cap.andThen(
												stateInfo => transmitResponse(HowAreYouReply(corrId, stateInfo)),
												err => ()
											)

										case RequestBallot(corrId, info) =>
											val cap = delegate.onRequestBallot(peerId, info)
											cap.andThen(
												ballot => transmitResponse(RequestBallotReply(corrId, ballot)),
												err => ()
											)

										case AppendRecords(corrId, inqTerm, prevIdx, prevTerm, batch, lCommit, termAtCommit) =>
											val cap = delegate.onAppendRecords(peerId, inqTerm, prevIdx, prevTerm, batch, lCommit, termAtCommit)
											cap.andThen(
												res => transmitResponse(AppendRecordsReply(corrId, res)),
												err => ()
											)

										case InstallSnapshot(corrId, inqTerm, snapshot, batch, lCommit, termAtCommit) =>
											val cap = delegate.onInstallSnapshot(peerId, inqTerm, snapshot, batch, lCommit, termAtCommit)
											cap.andThen(
												res => transmitResponse(InstallSnapshotReply(corrId, res)),
												err => {
													try snapshot.stateMachineSnapshot.close() catch { case NonFatal(_) => () }
												}
											)

										case PermitQuiescence(corrId, secIdx) =>
											delegate.onQuiescencePermitted(peerId, secIdx)
											transmitResponse(PermitQuiescenceReply(corrId))
									}
								}
								startReceiving()
							}

						case fault: Receiver.Fault =>
							sequencer.run {
								failAllPending(new IOException(s"Peer $peerId channel fault: $fault"))
								closeSilently()
								peerConnections.remove(peerId)
								memorizedProbableParticipants = memorizedProbableParticipants - peerId
							}

						case other =>
							sequencer.run {
								startReceiving()
							}
					}(using clusterWireMessageDeserializer)
				}
			}

			private def transmitResponse(msg: ClusterWireMessage): Unit = {
				val t = transmitter
				if t != null && isConnected then {
					t.transmit(msg, agreedVersion) { _ => () }(using clusterWireMessageSerializer)
				}
			}

			def failAllPending(exc: Throwable): Unit = {
				for ((_, pending) <- pendingRequests) {
					sequencer.cancel(pending.timeoutSchedule)
					pending.asInstanceOf[sequencer.Captor[Any]].trapSync(exc)
				}
				pendingRequests.clear()
				pendingOutboundMessages.clear()
			}

			def closeSilently(): Unit = {
				isConnected = false
				isConnecting = false
				if channel != null then {
					try channel.close() catch { case _: Throwable => () }
					channel = null
				}
				receiver = null
				transmitter = null
			}

			def close(): Unit = {
				failAllPending(new ClosedChannelException())
				closeSilently()
			}
		}

		private final class ClientConnection(
			val channel: AsynchronousSocketChannel,
			val receiver: Receiver,
			val transmitter: PipelinedTransmitter[ClusterWireMessage],
			val agreedVersion: ProtocolVersion
		) {
			var isConnected: Boolean = true

			def startReceiving(): Unit = {
				if isConnected && !isClosed then {
					receiver.receive[ClusterWireMessage](
						agreedVersion,
						bridgeConfig.receiverTimeout,
						TimeUnit.MILLISECONDS
					) {
						case SubmitClientCommand(corrId, command, attemptFlag) =>
							sequencer.run {
								val delegate = boundDelegate
								if delegate != null then {
									val cap = delegate.onCommandFromClient(command, attemptFlag)
									cap.andThen(
										res => transmitter.transmit(SubmitClientCommandReply(corrId, res), agreedVersion) { _ => () }(using clusterWireMessageSerializer),
										err => ()
									)
								}
								startReceiving()
							}

						case SubmitElectorateChange(corrId, reqId, desiredSet) =>
							sequencer.run {
								val delegate = boundDelegate
								if delegate != null then {
									val cap = delegate.requestElectorateChange(reqId, desiredSet)
									cap.andThen(
										res => transmitter.transmit(SubmitElectorateChangeReply(corrId, res), agreedVersion) { _ => () }(using clusterWireMessageSerializer),
										err => ()
									)
								}
								startReceiving()
							}

						case fault: Receiver.Fault =>
							sequencer.run {
								close()
							}

						case other =>
							sequencer.run {
								startReceiving()
							}
					}(using clusterWireMessageDeserializer)
				}
			}

			def close(): Unit = {
				isConnected = false
				try channel.close() catch { case _: Throwable => () }
				clientConnections.remove(this)
			}
		}
	}
}
