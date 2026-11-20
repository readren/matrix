package readren.consensus
package nio2

import readren.common.{ByteSource, Maybe}
import readren.consensus.ConsensusClusterBridgeCdm
import readren.consensus.protocol.*
import readren.nexus.cluster.channel.{PipelinedTransmitter, Receiver}
import readren.nexus.cluster.misc.VLQ
import readren.nexus.cluster.serialization.*
import readren.nexus.cluster.service.{ChannelId, ChannelOrigin}
import readren.sequencer.*

import java.io.IOException
import java.net.{InetSocketAddress, SocketAddress}
import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousServerSocketChannel, AsynchronousSocketChannel, ClosedChannelException, CompletionHandler}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ExecutorService, Executors, ThreadFactory, TimeUnit, TimeoutException}
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

	/** Instantiates the background [[ExecutorService]] used by [[Nio2ClusterBridge]] to encode and decode record batches off the `sequencer` thread.
	 * Subclasses or test harnesses may override this factory to inject custom executors or deterministic scheduling.
	 * @param participantId the identifier of the participant owning the bridge instance.
	 * @return a newly created [[ExecutorService]] configured for batch codec work.*/
	protected def newBatchCodecExecutor(participantId: ParticipantId): ExecutorService = {
		val threadCounter = new AtomicInteger(0)
		val factory: ThreadFactory = (runnable: Runnable) => {
			val thread = new Thread(runnable, s"nio2-bridge-batch-codec-$participantId-${threadCounter.incrementAndGet()}")
			thread.setDaemon(true)
			thread
		}
		Executors.newFixedThreadPool(bridgeConfig.batchCodecParallelism, factory)
	}

	class Nio2ClusterBridge(override val boundParticipantId: ParticipantId) extends thisSdm.ClusterBridge { thisBridge =>

		private final class PendingRpc[R](
			val correlationId: CorrelationId,
			val timeoutSchedule: sequencer.Schedule
		) extends sequencer.Captor[R] {
			var transmittedOnGeneration: Int = 0
		}

		private final class OutboundSlot(
			val correlationId: CorrelationId,
			val records: IArray[Record],
			val requestedStrategy: RecordBatchEncodingStrategy,
			val snapshotOrNull: SnapshotData[ParticipantId] | Null,
			val buildMessage: RecordBatch => ConsensusRpcRequest
		) {
			var preparedMessage: ConsensusRpcRequest | Null = null
			var encodedBatch: EncodedRecordBatch | Null = null
			var encodingFailure: Throwable | Null = null
			var encodingStarted: Boolean = false
			var isAbandoned: Boolean = false
		}

		/** Sequencer-confined envelope byte-length measurer for `AppendRecords` and `InstallSnapshot`, replacing static headroom heuristics.
		 * Overrides `putByteSource` to encode only the `Long` size VLQ without opening or reading `SnapshotByteSource` payload channels. */
		private object envelopeMeasurer extends Serializer.Writer(new RecyclableIdentityIntMap()) {
			private inline val AppendRecordsNonBatchBytes = 1L + 2L + 4L + 8L + 4L + 1L + VLQ.INT_MAX_LENGTH.toLong + 8L + 4L
			private inline val InstallSnapshotFixedNonBatchBytes = 1L + 2L + 4L + 1L + VLQ.INT_MAX_LENGTH.toLong + 8L + 4L
			private inline val MaxPrimitiveAlignmentLoss = Serializer.BUFFER_SIZE_REQUIRED_BY_MOST_DEMANDING_OPERATION - 1

			private val scratchBuffer = ByteBuffer.allocate(64)
			private val snapshotValueOrReference: ValueOrReferenceTest[SnapshotData[ParticipantId]] =
				ValueOrReferenceTest.whenReference[SnapshotData[ParticipantId]]
			private var countedBytes: Long = 0L
			private var currentVersion: ProtocolVersion = ProtocolVersion.OF_THIS_PROJECT

			override def governingVersion: ProtocolVersion = currentVersion

			override def position: Int = math.min(Int.MaxValue.toLong, countedBytes + scratchBuffer.position().toLong).toInt

			override protected def getBuffer(minimumRemaining: Int): ByteBuffer = {
				if scratchBuffer.remaining() < minimumRemaining then {
					countedBytes += scratchBuffer.position().toLong
					scratchBuffer.clear()
				}
				scratchBuffer
			}

			override def putByteSource(source: ByteSource): Unit = {
				val size = source.size
				if size < 0 then throw new Serializer.SerializationException(position, s"ByteSource size cannot be negative ($size)")
				putUnsignedLongVlq(size)
			}

			private def withAlignmentSlack(nonBatchBytes: Long): Int = {
				val usablePerBuffer = math.max(1, bridgeConfig.sizeOfBuffersInRing - MaxPrimitiveAlignmentLoss).toLong
				val crossedBoundaries = nonBatchBytes / usablePerBuffer + 2L
				val total = nonBatchBytes + crossedBoundaries * MaxPrimitiveAlignmentLoss.toLong
				math.min(Int.MaxValue.toLong, total).toInt
			}

			def measureEnvelopeBytes(snapshotOrNull: SnapshotData[ParticipantId] | Null, version: ProtocolVersion): Int = {
				if snapshotOrNull eq null then withAlignmentSlack(AppendRecordsNonBatchBytes)
				else {
					resetDeduplication()
					scratchBuffer.clear()
					countedBytes = 0L
					currentVersion = version
					this.write(snapshotOrNull)(using snapshotDataSerializer[ParticipantId], snapshotValueOrReference)
					val snapshotMetaBytes = countedBytes + scratchBuffer.position().toLong
					withAlignmentSlack(InstallSnapshotFixedNonBatchBytes + snapshotMetaBytes)
				}
			}
		}

		private var boundDelegate: thisBridge.Delegate | Null = null
		private var serverChannel: AsynchronousServerSocketChannel | Null = null
		@volatile private var isClosed: Boolean = false

		private val batchCodecExecutor: ExecutorService = newBatchCodecExecutor(boundParticipantId)

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
				batchCodecExecutor.shutdown()
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

		private def closeInboundRequestResourcesQuietly(message: ClusterWireMessage): Unit = {
			message match {
				case AppendRecords(_, _, _, _, RecordBatch.EncodedOutOfLine(source), _, _) =>
					try source.close() catch { case NonFatal(_) => () }
				case InstallSnapshot(_, _, snapshot, batch, _, _) =>
					batch match {
						case RecordBatch.EncodedOutOfLine(source) =>
							try source.close() catch { case NonFatal(_) => () }
						case _ => ()
					}
					try snapshot.stateMachineSnapshot.close() catch { case NonFatal(_) => () }
				case _ => ()
			}
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
				capacityOfBufferInRing = bridgeConfig.sizeOfBuffersInRing,
				ringMaxLength = bridgeConfig.maxRingLengthForTransmission,
				ringMinLengthOnIdle = bridgeConfig.minRingLengthOnIdle,
				streamTransferBufferCapacity = bridgeConfig.streamTransferBufferCapacity,
				maxGatheringBuffers = bridgeConfig.maxGatheringBuffers
			)
			val receiver = new Receiver(
				socketChannel,
				receiverContext,
				capacityOfBufferInRing = bridgeConfig.sizeOfBuffersInRing,
				ringMaxLength = bridgeConfig.maxRingLengthForReception,
				ringMinLengthOnIdle = bridgeConfig.minRingLengthOnIdle,
				allocationLimits = bridgeConfig.allocationLimits,
				byteSinkAllocator = bridgeConfig.inboundStagingAllocator,
				streamTransferBufferCapacity = bridgeConfig.streamTransferBufferCapacity
			)

			receiver.receive[ClusterWireMessage](
				ProtocolVersion.OF_THIS_PROJECT,
				bridgeConfig.connectTimeout,
				TimeUnit.MILLISECONDS
			) {
				case PeerHandshake(remotePeerId, remoteVersions, remoteMaxInboundControlBytes) =>
					determineAgreedVersion(remoteVersions) match {
						case None =>
							transmitter.transmit(
								HandshakeReject(s"Incompatible protocol version(s): $remoteVersions"),
								ProtocolVersion.OF_THIS_PROJECT
							) { _ =>
								try socketChannel.close() catch { case _: Throwable => () }
							}(using clusterWireMessageSerializer)
						case Some(agreedVersion) =>
							val ack = HandshakeAck(boundParticipantId, agreedVersion, bridgeConfig.localReceiverControlCapacity)
							transmitter.transmit(ack, agreedVersion) { _ => () }(using clusterWireMessageSerializer)
							sequencer.run {
								hatchAcceptedPeer(remotePeerId, socketChannel, receiver, transmitter, agreedVersion, remoteMaxInboundControlBytes)
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
							val ack = HandshakeAck(boundParticipantId, agreedVersion, bridgeConfig.localReceiverControlCapacity)
							transmitter.transmit(ack, agreedVersion) { _ => () }(using clusterWireMessageSerializer)
							sequencer.run {
								hatchAcceptedClient(socketChannel, receiver, transmitter, agreedVersion)
							}
					}

				case fault: Receiver.Fault =>
					try socketChannel.close() catch { case _: Throwable => () }

				case other: ClusterWireMessage =>
					closeInboundRequestResourcesQuietly(other)
					try socketChannel.close() catch { case _: Throwable => () }
			}(using clusterWireMessageDeserializer)
		}

		private def hatchAcceptedPeer(
			remotePeerId: ParticipantId,
			socketChannel: AsynchronousSocketChannel,
			receiver: Receiver,
			transmitter: PipelinedTransmitter[ClusterWireMessage],
			agreedVersion: ProtocolVersion,
			remoteMaxInboundControlBytes: Int
		): Unit = {
			assert(sequencer.isInSequence)
			if isClosed then {
				try socketChannel.close() catch { case _: Throwable => () }
			} else {
				val conn = getOrCreatePeerConnection(remotePeerId)
				if conn.isConnected then {
					val cmp = participantIdComparator.compare(boundParticipantId, remotePeerId)
					if cmp > 0 then {
						conn.attach(socketChannel, receiver, transmitter, agreedVersion, remoteMaxInboundControlBytes, ChannelOrigin.ACCEPTED)
						memorizedProbableParticipants = memorizedProbableParticipants + remotePeerId
						conn.startReceiving(conn.connectionGeneration)
					} else {
						try socketChannel.close() catch { case _: Throwable => () }
					}
				} else {
					conn.attach(socketChannel, receiver, transmitter, agreedVersion, remoteMaxInboundControlBytes, ChannelOrigin.ACCEPTED)
					memorizedProbableParticipants = memorizedProbableParticipants + remotePeerId
					conn.startReceiving(conn.connectionGeneration)
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

		private def getOrCreatePeerConnection(peerId: ParticipantId): PeerConnection = {
			assert(sequencer.isInSequence)
			peerConnections.getOrElseUpdate(peerId, new PeerConnection(peerId, ChannelOrigin.INITIATED))
		}

		private def startOutboundConnect(peerId: ParticipantId, conn: PeerConnection, connectAttemptId: Int): Unit = {
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
							capacityOfBufferInRing = bridgeConfig.sizeOfBuffersInRing,
							ringMaxLength = bridgeConfig.maxRingLengthForTransmission,
							ringMinLengthOnIdle = bridgeConfig.minRingLengthOnIdle,
							streamTransferBufferCapacity = bridgeConfig.streamTransferBufferCapacity,
							maxGatheringBuffers = bridgeConfig.maxGatheringBuffers
						)
						val receiver = new Receiver(
							clientChannel,
							receiverContext,
							capacityOfBufferInRing = bridgeConfig.sizeOfBuffersInRing,
							ringMaxLength = bridgeConfig.maxRingLengthForReception,
							ringMinLengthOnIdle = bridgeConfig.minRingLengthOnIdle,
							allocationLimits = bridgeConfig.allocationLimits,
							byteSinkAllocator = bridgeConfig.inboundStagingAllocator,
							streamTransferBufferCapacity = bridgeConfig.streamTransferBufferCapacity
						)

						val handshakeMessage = PeerHandshake(boundParticipantId, bridgeConfig.supportedVersions, bridgeConfig.localReceiverControlCapacity)
						transmitter.transmit(handshakeMessage, ProtocolVersion.OF_THIS_PROJECT) { _ => () }(using clusterWireMessageSerializer)

						receiver.receive[ClusterWireMessage](
							ProtocolVersion.OF_THIS_PROJECT,
							bridgeConfig.connectTimeout,
							TimeUnit.MILLISECONDS
						) {
							case HandshakeAck(_, agreedVersion, remoteMaxInboundControlBytes) =>
								if !bridgeConfig.supportedVersions.contains(agreedVersion) then {
									try clientChannel.close() catch { case _: Throwable => () }
									sequencer.run {
										if !conn.isConnected && conn.activeConnectAttemptId == connectAttemptId then {
											conn.isConnecting = false
											conn.failAllPending(new IOException(s"Peer $peerId agreed on unsupported protocol version: $agreedVersion"))
											peerConnections.remove(peerId)
										}
									}
								} else {
									sequencer.run {
										if isClosed then {
											try clientChannel.close() catch { case _: Throwable => () }
											conn.failAllPending(new ClosedChannelException())
										} else if conn.isConnected then {
											val cmp = participantIdComparator.compare(boundParticipantId, peerId)
											if cmp > 0 then {
												// Existing accepted channel wins tie-breaker; close this redundant initiated channel and drain any pending slots on conn.
												try clientChannel.close() catch { case _: Throwable => () }
												conn.isConnecting = false
												conn.drainOutboundBacklog()
											} else {
												// Newly initiated channel wins tie-breaker over existing accepted channel.
												conn.attach(clientChannel, receiver, transmitter, agreedVersion, remoteMaxInboundControlBytes, ChannelOrigin.INITIATED)
												memorizedProbableParticipants = memorizedProbableParticipants + peerId
												conn.startReceiving(conn.connectionGeneration)
											}
										} else {
											conn.attach(clientChannel, receiver, transmitter, agreedVersion, remoteMaxInboundControlBytes, ChannelOrigin.INITIATED)
											memorizedProbableParticipants = memorizedProbableParticipants + peerId
											conn.startReceiving(conn.connectionGeneration)
										}
									}
								}

							case HandshakeReject(reason) =>
								try clientChannel.close() catch { case _: Throwable => () }
								sequencer.run {
									if !conn.isConnected && conn.activeConnectAttemptId == connectAttemptId then {
										conn.isConnecting = false
										conn.failAllPending(new IOException(s"Peer $peerId rejected handshake: $reason"))
										peerConnections.remove(peerId)
									}
								}

							case fault: Receiver.Fault =>
								try clientChannel.close() catch { case _: Throwable => () }
								sequencer.run {
									if !conn.isConnected && conn.activeConnectAttemptId == connectAttemptId then {
										conn.isConnecting = false
										conn.failAllPending(new IOException(s"Peer $peerId connection fault during handshake: $fault"))
										peerConnections.remove(peerId)
									}
								}

							case other: ClusterWireMessage =>
								closeInboundRequestResourcesQuietly(other)
								try clientChannel.close() catch { case _: Throwable => () }
								sequencer.run {
									if !conn.isConnected && conn.activeConnectAttemptId == connectAttemptId then {
										conn.isConnecting = false
										conn.failAllPending(new IOException(s"Peer $peerId sent unexpected initial message: $other"))
										peerConnections.remove(peerId)
									}
								}
						}(using clusterWireMessageDeserializer)
					}

					override def failed(exc: Throwable, attachment: Null): Unit = {
						try clientChannel.close() catch { case _: Throwable => () }
						sequencer.run {
							if !conn.isConnected && conn.activeConnectAttemptId == connectAttemptId then {
								conn.isConnecting = false
								conn.failAllPending(exc)
								peerConnections.remove(peerId)
							}
						}
					}
				})
			}
		}

		extension (destinationId: ParticipantId) {

			override def howAreYou(inquirerInfo: StateInfo): sequencer.Capture[StateInfo] = {
				executeOutboundPreparedRpc[StateInfo](destinationId, corrId => HowAreYou(corrId, inquirerInfo), bridgeConfig.fastRpcTimeout)
			}

			override def requestBallot(inquirerId: ParticipantId, inquirerInfo: StateInfo): sequencer.Capture[Ballot] = {
				executeOutboundPreparedRpc[Ballot](destinationId, corrId => RequestBallot(corrId, inquirerInfo), bridgeConfig.fastRpcTimeout)
			}

			override def appendRecords(
				inquirerTerm: Term,
				prevLogIndex: RecordIndex,
				prevLogTerm: Term,
				batch: IArray[Record],
				leaderCommit: RecordIndex,
				termAtLeaderCommit: Term
			): sequencer.Capture[AppendResult] = {
				executeOutboundBatchRpc[AppendResult](
					destinationId,
					batch,
					null,
					(corrId, recordBatch) => new AppendRecords(corrId, inquirerTerm, prevLogIndex, prevLogTerm, recordBatch, leaderCommit, termAtLeaderCommit),
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
				executeOutboundBatchRpc[AppendResult](
					destinationId,
					batch,
					snapshot,
					(corrId, recordBatch) => new InstallSnapshot(corrId, inquirerTerm, snapshot, recordBatch, leaderCommit, termAtLeaderCommit),
					bridgeConfig.snapshotRpcTimeout
				)
			}

			override def permitQuiescence(indexOfGrantedSec: RecordIndex): sequencer.Capture[Unit] = {
				executeOutboundPreparedRpc[Unit](
					destinationId,
					corrId => PermitQuiescence(corrId, indexOfGrantedSec),
					bridgeConfig.fastRpcTimeout
				)
			}
		}

		private def createAndRegisterPendingRpc[R](
			conn: PeerConnection,
			destinationId: ParticipantId,
			timeoutDuration: MilliDuration
		)(using ClassTag[R]): (CorrelationId, PendingRpc[R]) = {
			val corrId = conn.nextCorrelationId()
			val timeoutSchedule = sequencer.newDelaySchedule(timeoutDuration)
			val pendingRpc = new PendingRpc[R](corrId, timeoutSchedule)
			sequencer.schedule(timeoutSchedule) { _ =>
				conn.removePending(corrId).foreach { pending =>
					pending.asInstanceOf[sequencer.Captor[R]].trapSync(
						new TimeoutException(s"RPC to $destinationId with CorrelationId $corrId timed out after ${timeoutDuration}ms")
					)
				}
			}
			conn.registerPending(corrId, pendingRpc)
			(corrId, pendingRpc)
		}

		private def executeOutboundPreparedRpc[R](
			destinationId: ParticipantId,
			buildMessage: CorrelationId => ConsensusRpcRequest,
			timeoutDuration: MilliDuration
		)(using ClassTag[R]): sequencer.Capture[R] = {
			if isClosed then sequencer.Failed(new ClosedChannelException())
			else {
				val conn = getOrCreatePeerConnection(destinationId)
				val (corrId, pendingRpc) = createAndRegisterPendingRpc[R](conn, destinationId, timeoutDuration)
				conn.enqueueOrTransmitPrepared(corrId, buildMessage(corrId))
				pendingRpc
			}
		}

		private def executeOutboundBatchRpc[R](
			destinationId: ParticipantId,
			batch: IArray[Record],
			snapshotOrNull: SnapshotData[ParticipantId] | Null,
			buildMessage: (CorrelationId, RecordBatch) => ConsensusRpcRequest,
			timeoutDuration: MilliDuration
		)(using ClassTag[R]): sequencer.Capture[R] = {
			if isClosed then sequencer.Failed(new ClosedChannelException())
			else {
				val conn = getOrCreatePeerConnection(destinationId)
				val (corrId, pendingRpc) = createAndRegisterPendingRpc[R](conn, destinationId, timeoutDuration)
				conn.enqueueOrTransmitBatch(corrId, batch, snapshotOrNull, recordBatch => buildMessage(corrId, recordBatch))
				pendingRpc
			}
		}

		private final class PeerConnection(val peerId: ParticipantId, var origin: ChannelOrigin) {
			var channel: AsynchronousSocketChannel | Null = null
			var receiver: Receiver | Null = null
			var transmitter: PipelinedTransmitter[ClusterWireMessage] | Null = null
			var agreedVersion: ProtocolVersion = ProtocolVersion.OF_THIS_PROJECT
			var remoteMaxInboundControlPackageBytes: Int = bridgeConfig.localReceiverControlCapacity
			var isConnected: Boolean = false
			var isConnecting: Boolean = false
			var activeConnectAttemptId: Int = 0
			var connectionGeneration: Int = 0
			var lastCorrelationId: CorrelationId = CorrelationId.Initial

			val pendingRequests: mutable.Map[CorrelationId, PendingRpc[?]] = mutable.Map.empty
			val outboundBacklog: mutable.ArrayDeque[OutboundSlot] = mutable.ArrayDeque.empty

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

			def failPendingRequest(corrId: CorrelationId, exc: Throwable): Unit = {
				pendingRequests.remove(corrId).foreach { pending =>
					sequencer.cancel(pending.timeoutSchedule)
					pending.asInstanceOf[sequencer.Captor[Any]].trapSync(exc)
				}
			}

			private def effectiveStrategyFor(batch: IArray[Record]): RecordBatchEncodingStrategy = {
				if batch.isEmpty then RecordBatchEncodingStrategy.WithinControlPackage
				else bridgeConfig.recordBatchEncodingSelector(batch)
			}

			private def ensureConnecting(): Unit = {
				if !isConnected && !isConnecting then {
					activeConnectAttemptId += 1
					startOutboundConnect(peerId, this, activeConnectAttemptId)
				}
			}

			def attach(
				newChannel: AsynchronousSocketChannel,
				newReceiver: Receiver,
				newTransmitter: PipelinedTransmitter[ClusterWireMessage],
				newAgreedVersion: ProtocolVersion,
				newRemoteMaxInboundControlBytes: Int,
				newOrigin: ChannelOrigin
			): Unit = {
				if isConnected || channel != null then {
					val supersededGen = connectionGeneration
					val oldChannel = channel
					channel = null
					receiver = null
					transmitter = null
					if oldChannel != null then {
						try oldChannel.close() catch { case _: Throwable => () }
					}
					if supersededGen > 0 then {
						val supersededIds = pendingRequests.collect {
							case (corrId, pending) if pending.transmittedOnGeneration == supersededGen => corrId
						}.toList
						val supersededExc = new IOException(s"Connection to $peerId was superseded during simultaneous-connect tie-breaker")
						for corrId <- supersededIds do {
							failPendingRequest(corrId, supersededExc)
						}
					}
				}
				origin = newOrigin
				channel = newChannel
				receiver = newReceiver
				transmitter = newTransmitter
				agreedVersion = newAgreedVersion
				remoteMaxInboundControlPackageBytes = newRemoteMaxInboundControlBytes
				connectionGeneration += 1
				isConnected = true
				isConnecting = false

				for slot <- outboundBacklog do {
					if !slot.isAbandoned && slot.requestedStrategy == RecordBatchEncodingStrategy.StagedOnCodecExecutor && !slot.encodingStarted then {
						startAsyncEncoding(slot)
					}
				}
				drainOutboundBacklog()
			}

			def enqueueOrTransmitPrepared(corrId: CorrelationId, msg: ConsensusRpcRequest): Unit = {
				if isConnected && transmitter != null && outboundBacklog.isEmpty then {
					transmitRequestNow(corrId, msg)
				} else {
					val slot = new OutboundSlot(
						correlationId = corrId,
						records = IArray.empty[Record],
						requestedStrategy = RecordBatchEncodingStrategy.WithinControlPackage,
						snapshotOrNull = null,
						buildMessage = _ => msg
					)
					slot.preparedMessage = msg
					outboundBacklog.append(slot)
					if isConnected then drainOutboundBacklog()
					else ensureConnecting()
				}
			}

			def enqueueOrTransmitBatch(
				corrId: CorrelationId,
				batch: IArray[Record],
				snapshotOrNull: SnapshotData[ParticipantId] | Null,
				buildMessage: RecordBatch => ConsensusRpcRequest
			): Unit = {
				val strategy = effectiveStrategyFor(batch)
				if isConnected && transmitter != null && outboundBacklog.isEmpty then {
					strategy match {
						case RecordBatchEncodingStrategy.WithinControlPackage =>
							transmitRequestNow(corrId, buildMessage(RecordBatch.Records(batch)))
						case RecordBatchEncodingStrategy.StagedOnSequencer =>
							try {
								val encoded = RecordBatchCodec.encode(batch, agreedVersion, bridgeConfig.allocationLimits)(using wireRecordBatchArraySerializer)
								placeAndTransmitEncoded(corrId, encoded, snapshotOrNull, buildMessage)
							} catch {
								case NonFatal(e) =>
									failPendingRequest(corrId, e)
							}
						case RecordBatchEncodingStrategy.StagedOnCodecExecutor =>
							val slot = new OutboundSlot(corrId, batch, strategy, snapshotOrNull, buildMessage)
							outboundBacklog.append(slot)
							startAsyncEncoding(slot)
					}
				} else {
					val slot = new OutboundSlot(corrId, batch, strategy, snapshotOrNull, buildMessage)
					if strategy == RecordBatchEncodingStrategy.WithinControlPackage then {
						slot.preparedMessage = buildMessage(RecordBatch.Records(batch))
					}
					outboundBacklog.append(slot)
					if isConnected then {
						if strategy == RecordBatchEncodingStrategy.StagedOnCodecExecutor then {
							startAsyncEncoding(slot)
						}
						drainOutboundBacklog()
					} else {
						ensureConnecting()
					}
				}
			}

			private def startAsyncEncoding(slot: OutboundSlot): Unit = {
				if !slot.encodingStarted && !slot.isAbandoned && isConnected then {
					slot.encodingStarted = true
					val targetVersion = agreedVersion
					val limits = bridgeConfig.allocationLimits
					val recordsToEncode = slot.records
					try {
						batchCodecExecutor.execute(() => {
							if !isClosed && !slot.isAbandoned then {
								try {
									val encoded = RecordBatchCodec.encode(recordsToEncode, targetVersion, limits)(using wireRecordBatchArraySerializer)
									sequencer.run {
										if !isClosed && !slot.isAbandoned then {
											if isConnected && agreedVersion != targetVersion then {
												slot.encodingStarted = false
												startAsyncEncoding(slot)
											} else {
												slot.encodedBatch = encoded
												drainOutboundBacklog()
											}
										}
									}
								} catch {
									case NonFatal(e) =>
										sequencer.run {
											if !isClosed && !slot.isAbandoned then {
												slot.encodingFailure = e
												drainOutboundBacklog()
											}
										}
								}
							}
						})
					} catch {
						case NonFatal(e) =>
							slot.encodingFailure = e
					}
				}
			}

			def drainOutboundBacklog(): Unit = {
				var waitingForAsyncHead = false
				while isConnected && transmitter != null && outboundBacklog.nonEmpty && !waitingForAsyncHead do {
					val head = outboundBacklog.head
					if head.isAbandoned || !pendingRequests.contains(head.correlationId) then {
						outboundBacklog.removeHead()
					} else if head.encodingFailure != null then {
						outboundBacklog.removeHead()
						failPendingRequest(head.correlationId, head.encodingFailure.nn)
					} else if head.preparedMessage != null then {
						outboundBacklog.removeHead()
						transmitRequestNow(head.correlationId, head.preparedMessage.nn)
					} else if head.encodedBatch != null then {
						val encoded = head.encodedBatch.nn
						if encoded.governingVersion != agreedVersion then {
							head.encodedBatch = null
							head.encodingStarted = false
							startAsyncEncoding(head)
							waitingForAsyncHead = true
						} else {
							outboundBacklog.removeHead()
							placeAndTransmitEncoded(head.correlationId, encoded, head.snapshotOrNull, head.buildMessage)
						}
					} else if head.requestedStrategy == RecordBatchEncodingStrategy.StagedOnSequencer then {
						outboundBacklog.removeHead()
						try {
							val encoded = RecordBatchCodec.encode(head.records, agreedVersion, bridgeConfig.allocationLimits)(using wireRecordBatchArraySerializer)
							placeAndTransmitEncoded(head.correlationId, encoded, head.snapshotOrNull, head.buildMessage)
						} catch {
							case NonFatal(e) =>
								failPendingRequest(head.correlationId, e)
						}
					} else {
						if !head.encodingStarted then {
							startAsyncEncoding(head)
						}
						if head.encodingFailure != null then {
							outboundBacklog.removeHead()
							failPendingRequest(head.correlationId, head.encodingFailure.nn)
						} else {
							waitingForAsyncHead = true
						}
					}
				}
			}

			private def placeAndTransmitEncoded(
				corrId: CorrelationId,
				encoded: EncodedRecordBatch,
				snapshotOrNull: SnapshotData[ParticipantId] | Null,
				buildMessage: RecordBatch => ConsensusRpcRequest
			): Unit = {
				try {
					val envelopeBytes = envelopeMeasurer.measureEnvelopeBytes(snapshotOrNull, agreedVersion)
					val maxInlineBytes = bridgeConfig.maxInlineBatchBytesForPeer(remoteMaxInboundControlPackageBytes, envelopeBytes)
					val placed = RecordBatchCodec.place(encoded, maxInlineBytes)
					transmitRequestNow(corrId, buildMessage(placed))
				} catch {
					case NonFatal(e) =>
						failPendingRequest(corrId, e)
				}
			}

			private def extractNotDeliveredCause(report: PipelinedTransmitter.NotDelivered): Throwable = {
				report match {
					case PipelinedTransmitter.SerializationProblem(_, problem, _) => problem
					case PipelinedTransmitter.TransmissionFailure(_, cause) => cause
					case PipelinedTransmitter.ByteSourceProblem(_, cause) => cause
				}
			}

			private def transmitRequestNow(corrId: CorrelationId, msg: ConsensusRpcRequest): Unit = {
				val t = transmitter
				if t != null && isConnected then {
					val gen = connectionGeneration
					pendingRequests.get(corrId).foreach(_.transmittedOnGeneration = gen)
					try {
						t.transmit(msg, agreedVersion) {
							case PipelinedTransmitter.Delivered => ()
							case notDelivered: PipelinedTransmitter.NotDelivered =>
								val cause = extractNotDeliveredCause(notDelivered)
								sequencer.run {
									failPendingRequest(corrId, new IOException(s"Failed to transmit RPC $corrId to $peerId: $notDelivered", cause))
								}
						}(using clusterWireMessageSerializer)
					} catch {
						case NonFatal(e) =>
							failPendingRequest(corrId, e)
					}
				}
			}

			def startReceiving(expectedGeneration: Int): Unit = {
				val r = receiver
				if r != null && isConnected && !isClosed && connectionGeneration == expectedGeneration then {
					r.receive[ClusterWireMessage](
						agreedVersion,
						bridgeConfig.receiverTimeout,
						TimeUnit.MILLISECONDS
					) {
						case response: ConsensusRpcResponse =>
							sequencer.run {
								if !isClosed && isConnected && connectionGeneration == expectedGeneration then {
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
									startReceiving(expectedGeneration)
								}
							}

						case request: ConsensusRpcRequest =>
							extractOutOfLineBatchSource(request) match {
								case Some(batchSource) =>
									if isClosed then {
										try batchSource.close() catch { case NonFatal(_) => () }
										closeInboundSnapshotIfPresent(request)
									} else {
										val decodeVersion = agreedVersion
										val limits = bridgeConfig.allocationLimits
										try {
											batchCodecExecutor.execute(() => {
												if isClosed then {
													try batchSource.close() catch { case NonFatal(_) => () }
													closeInboundSnapshotIfPresent(request)
												} else {
													var decodedRecords: IArray[Record] | Null = null
													var decodeFailure: Throwable | Null = null
													try {
														decodedRecords = RecordBatchCodec.decodeOutOfLine(batchSource, decodeVersion, limits)(using wireRecordBatchArrayDeserializer)
													} catch {
														case NonFatal(e) =>
															decodeFailure = e
													} finally {
														try batchSource.close() catch { case NonFatal(_) => () }
													}
													if decodeFailure != null then {
														closeInboundSnapshotIfPresent(request)
														val err = decodeFailure.nn
														sequencer.run {
															handleInboundChannelFault(
																expectedGeneration,
																new IOException(s"Peer $peerId streamed record batch decode failed: ${err.getMessage}", err)
															)
														}
													} else {
														val records = decodedRecords.nn
														sequencer.run {
															if isClosed || !isConnected || connectionGeneration != expectedGeneration then {
																closeInboundSnapshotIfPresent(request)
															} else {
																dispatchDecodedInboundRequest(request, records)
																startReceiving(expectedGeneration)
															}
														}
													}
												}
											})
										} catch {
											case NonFatal(_) =>
												try batchSource.close() catch { case NonFatal(_) => () }
												closeInboundSnapshotIfPresent(request)
										}
									}

								case None =>
									sequencer.run {
										if isClosed || !isConnected || connectionGeneration != expectedGeneration then {
											closeInboundSnapshotIfPresent(request)
										} else {
											val inlineRecords = extractInlineRecords(request)
											dispatchDecodedInboundRequest(request, inlineRecords)
											startReceiving(expectedGeneration)
										}
									}
							}

						case fault: Receiver.Fault =>
							sequencer.run {
								handleInboundChannelFault(expectedGeneration, new IOException(s"Peer $peerId channel fault: $fault"))
							}

						case other: ClusterWireMessage =>
							closeInboundRequestResourcesQuietly(other)
							sequencer.run {
								if !isClosed && isConnected && connectionGeneration == expectedGeneration then {
									startReceiving(expectedGeneration)
								}
							}
					}(using clusterWireMessageDeserializer)
				}
			}

			private def extractOutOfLineBatchSource(request: ConsensusRpcRequest): Option[readren.common.ByteSource] = {
				request match {
					case AppendRecords(_, _, _, _, RecordBatch.EncodedOutOfLine(source), _, _) => Some(source)
					case InstallSnapshot(_, _, _, RecordBatch.EncodedOutOfLine(source), _, _) => Some(source)
					case _ => None
				}
			}

			private def extractInlineRecords(request: ConsensusRpcRequest): IArray[Record] = {
				request match {
					case AppendRecords(_, _, _, _, RecordBatch.Records(records), _, _) => records
					case InstallSnapshot(_, _, _, RecordBatch.Records(records), _, _) => records
					case _ => IArray.empty[Record]
				}
			}

			private def closeInboundSnapshotIfPresent(request: ConsensusRpcRequest): Unit = {
				request match {
					case InstallSnapshot(_, _, snapshot, _, _, _) =>
						try snapshot.stateMachineSnapshot.close() catch { case NonFatal(_) => () }
					case _ => ()
				}
			}

			private def dispatchDecodedInboundRequest(request: ConsensusRpcRequest, decodedBatch: IArray[Record]): Unit = {
				val delegate = boundDelegate
				if delegate == null then {
					closeInboundSnapshotIfPresent(request)
				} else {
					request match {
						case HowAreYou(corrId, info) =>
							val cap = delegate.onHowAreYou(peerId, info)
							cap.andThen(
								stateInfo => transmitResponse(HowAreYouReply(corrId, stateInfo)),
								_ => ()
							)

						case RequestBallot(corrId, info) =>
							val cap = delegate.onRequestBallot(peerId, info)
							cap.andThen(
								ballot => transmitResponse(RequestBallotReply(corrId, ballot)),
								_ => ()
							)

						case AppendRecords(corrId, inqTerm, prevIdx, prevTerm, _, lCommit, termAtCommit) =>
							val cap = delegate.onAppendRecords(peerId, inqTerm, prevIdx, prevTerm, decodedBatch, lCommit, termAtCommit)
							cap.andThen(
								res => transmitResponse(AppendRecordsReply(corrId, res)),
								_ => ()
							)

						case InstallSnapshot(corrId, inqTerm, snapshot, _, lCommit, termAtCommit) =>
							try {
								val cap = delegate.onInstallSnapshot(peerId, inqTerm, snapshot, decodedBatch, lCommit, termAtCommit)
								cap.andThen(
									res => transmitResponse(InstallSnapshotReply(corrId, res)),
									_ => {
										try snapshot.stateMachineSnapshot.close() catch { case NonFatal(_) => () }
									}
								)
							} catch {
								case NonFatal(e) =>
									try snapshot.stateMachineSnapshot.close() catch { case NonFatal(_) => () }
									throw e
							}

						case PermitQuiescence(corrId, secIdx) =>
							delegate.onQuiescencePermitted(peerId, secIdx)
							transmitResponse(PermitQuiescenceReply(corrId))
					}
				}
			}

			private def handleInboundChannelFault(expectedGeneration: Int, cause: IOException): Unit = {
				if connectionGeneration == expectedGeneration then {
					failAllPending(cause)
					closeSilently()
					peerConnections.remove(peerId)
					memorizedProbableParticipants = memorizedProbableParticipants - peerId
				}
			}

			private def transmitResponse(msg: ClusterWireMessage): Unit = {
				val t = transmitter
				if t != null && isConnected then {
					t.transmit(msg, agreedVersion) { _ => () }(using clusterWireMessageSerializer)
				}
			}

			def failAllPending(exc: Throwable): Unit = {
				for slot <- outboundBacklog do {
					slot.isAbandoned = true
				}
				outboundBacklog.clear()
				for ((_, pending) <- pendingRequests) {
					sequencer.cancel(pending.timeoutSchedule)
					pending.asInstanceOf[sequencer.Captor[Any]].trapSync(exc)
				}
				pendingRequests.clear()
			}

			def closeSilently(): Unit = {
				isConnected = false
				isConnecting = false
				connectionGeneration += 1
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
										_ => ()
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
										_ => ()
									)
								}
								startReceiving()
							}

						case fault: Receiver.Fault =>
							sequencer.run {
								close()
							}

						case other: ClusterWireMessage =>
							closeInboundRequestResourcesQuietly(other)
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
