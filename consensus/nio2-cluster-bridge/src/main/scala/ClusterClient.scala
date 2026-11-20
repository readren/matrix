package readren.consensus
package nio2

import readren.common.Maybe
import readren.consensus.protocol.*
import readren.nexus.cluster.channel.{PipelinedTransmitter, Receiver}
import readren.nexus.cluster.serialization.*
import readren.nexus.cluster.service.{ChannelId, ChannelOrigin}
import readren.sequencer.MilliDuration

import java.io.IOException
import java.net.{InetSocketAddress, SocketAddress}
import java.nio.channels.{AsynchronousSocketChannel, ClosedChannelException, CompletionHandler}
import java.util.concurrent.{ConcurrentHashMap, TimeUnit, TimeoutException}
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

class ClusterClient[S <: Nio2ClusterBridgeCdm](val sdm: S)(
	val targetAddress: SocketAddress,
	val config: ClusterBridgeConfig = ClusterBridgeConfig.Default
) {
	import sdm.wireProtocol.*
	import sdm.wireProtocol.given

	private var channel: AsynchronousSocketChannel | Null = null
	private var receiver: Receiver | Null = null
	private var transmitter: PipelinedTransmitter[ClusterWireMessage] | Null = null
	@volatile private var isClosed: Boolean = false

	private var lastCorrelationId: CorrelationId = CorrelationId.Initial
	private val pendingResponses: ConcurrentHashMap[Short, Promise[ClusterWireMessage]] = new ConcurrentHashMap()
	@volatile private var agreedVersion: ProtocolVersion = ProtocolVersion.NOT_SPECIFIED

	def isConnected: Boolean = (agreedVersion != ProtocolVersion.NOT_SPECIFIED) && !isClosed

	def connect(timeout: MilliDuration = 5000L): Future[Unit] = {
		val promise = Promise[Unit]()
		val asc = AsynchronousSocketChannel.open()
		channel = asc

		asc.connect(targetAddress, null, new CompletionHandler[Void, Null] {
			override def completed(result: Void, attachment: Null): Unit = {
				val localAddr = try asc.getLocalAddress catch { case _: Throwable => new InetSocketAddress("127.0.0.1", 0) }
				val remoteAddr = try Maybe(asc.getRemoteAddress) catch { case _: Throwable => Maybe.empty }

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

				val trans = new PipelinedTransmitter[ClusterWireMessage](
					asc,
					transmitterContext,
					capacityOfBufferInRing = config.sizeOfBuffersInRing,
					ringMaxLength = config.maxRingLengthForTransmission,
					ringMinLengthOnIdle = config.minRingLengthOnIdle,
					streamTransferBufferCapacity = config.streamTransferBufferCapacity,
					maxGatheringBuffers = config.maxGatheringBuffers
				)
				val recv = new Receiver(
					asc,
					receiverContext,
					capacityOfBufferInRing = config.sizeOfBuffersInRing,
					ringMaxLength = config.maxRingLengthForReception,
					ringMinLengthOnIdle = config.minRingLengthOnIdle,
					allocationLimits = config.allocationLimits,
					byteSinkAllocator = config.inboundStagingAllocator,
					streamTransferBufferCapacity = config.streamTransferBufferCapacity
				)

				transmitter = trans
				receiver = recv

				val handshake = ClientHandshake(config.supportedVersions)
				trans.transmit(handshake, ProtocolVersion.OF_THIS_PROJECT) { _ => () }(using clusterWireMessageSerializer)

				recv.receive[ClusterWireMessage](ProtocolVersion.OF_THIS_PROJECT, timeout, TimeUnit.MILLISECONDS) {
					case HandshakeAck(_, remoteAgreedVersion, _) =>
						if !config.supportedVersions.contains(remoteAgreedVersion) then {
							close()
							promise.failure(new IOException(s"Server agreed on unsupported protocol version: $remoteAgreedVersion"))
						} else {
							agreedVersion = remoteAgreedVersion
							startReceiving()
							promise.success(())
						}

					case HandshakeReject(reason) =>
						close()
						promise.failure(new IOException(s"Client handshake rejected: $reason"))

					case fault: Receiver.Fault =>
						close()
						promise.failure(new IOException(s"Client handshake fault: $fault"))

					case other =>
						close()
						promise.failure(new IOException(s"Unexpected initial message from server: $other"))
				}(using clusterWireMessageDeserializer)
			}

			override def failed(exc: Throwable, attachment: Null): Unit = {
				close()
				promise.failure(exc)
			}
		})

		promise.future
	}

	private def startReceiving(): Unit = {
		val r = receiver
		if r != null && isConnected then {
			r.receive[ClusterWireMessage](agreedVersion, config.receiverTimeout, TimeUnit.MILLISECONDS) {
				case resp: ClientRpcResponse =>
					val p = pendingResponses.remove(resp.correlationId.toShort)
					if p != null then {
						p.success(resp)
					}
					startReceiving()

				case fault: Receiver.Fault =>
					failAllPending(new IOException(s"Client connection fault: $fault"))
					close()

				case other =>
					startReceiving()
			}(using clusterWireMessageDeserializer)
		}
	}

	private def nextCorrelationId(): CorrelationId = synchronized {
		val next = lastCorrelationId.next
		lastCorrelationId = next
		next
	}

	def sendCommand(command: sdm.ClientCommand, attemptFlag: CommandAttemptFlag = FIRST_ATTEMPT): Future[sdm.ResponseToClient] = {
		if !isConnected then Future.failed(new ClosedChannelException())
		else {
			val corrId = nextCorrelationId()
			val promise = Promise[ClusterWireMessage]()
			pendingResponses.put(corrId.toShort, promise)

			val msg = SubmitClientCommand(corrId, command, attemptFlag)
			transmitter.nn.transmit(msg, agreedVersion) { _ => () }(using clusterWireMessageSerializer)

			promise.future.map {
				case SubmitClientCommandReply(_, response) => response
				case other => throw new IllegalStateException(s"Unexpected response message: $other")
			}(using scala.concurrent.ExecutionContext.parasitic)
		}
	}

	def requestElectorateChange(requestId: ElectorateChangeRequestId, desiredParticipants: Set[sdm.ParticipantId]): Future[ElectorateChangeResponse[sdm.ParticipantId]] = {
		if !isConnected then Future.failed(new ClosedChannelException())
		else {
			val corrId = nextCorrelationId()
			val promise = Promise[ClusterWireMessage]()
			pendingResponses.put(corrId.toShort, promise)

			val msg = SubmitElectorateChange(corrId, requestId, desiredParticipants)
			transmitter.nn.transmit(msg, agreedVersion) { _ => () }(using clusterWireMessageSerializer)

			promise.future.map {
				case SubmitElectorateChangeReply(_, response) => response
				case other => throw new IllegalStateException(s"Unexpected response message: $other")
			}(using scala.concurrent.ExecutionContext.parasitic)
		}
	}

	private def failAllPending(exc: Throwable): Unit = {
		val iterator = pendingResponses.values().iterator()
		while iterator.hasNext do {
			iterator.next().tryFailure(exc)
		}
		pendingResponses.clear()
	}

	def close(): Unit = {
		isClosed = true
		failAllPending(new ClosedChannelException())
		if channel != null then {
			try channel.nn.close() catch { case _: Throwable => () }
			channel = null
		}
		receiver = null
		transmitter = null
	}
}
