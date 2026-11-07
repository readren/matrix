package readren.consensus
package nio2

import munit.FunSuite
import readren.common.Maybe
import readren.consensus.protocol.*
import readren.nexus.cluster.channel.{Receiver, SequentialTransmitter, Transmitter}
import readren.nexus.cluster.serialization.*
import readren.nexus.cluster.serialization.CommonSerializers.given
import readren.nexus.cluster.service.{ChannelId, ChannelOrigin}
import readren.sequencer.SchedulingDoer
import readren.sequencer.providers.CooperativeThreadDrivenSchedulerDp

import java.net.{InetSocketAddress, SocketAddress}
import java.nio.channels.AsynchronousSocketChannel
import java.util.Comparator
import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.concurrent.{Await, Promise}
import scala.concurrent.duration.*
import scala.reflect.ClassTag

class ClusterBridgeHandshakeTest extends FunSuite {

	val schedulerDp = new CooperativeThreadDrivenSchedulerDp.Impl(unhandledExceptionReporter = (_, e) => e.printStackTrace())
	private val strSer: Serializer[String] = summon
	private val strDeser: Deserializer[String] = summon

	class TestSdm(val nodeId: String, val bindAddress: SocketAddress, val peerAddressMap: Map[String, SocketAddress] = Map.empty) extends Nio2ClusterBridgeCdm {
		override type ParticipantId = String
		override val participantIdComparator: Comparator[ParticipantId] = Ordering.String
		override type ClientId = String
		override type ClientCommand = String
		override type StateMachineResponse = String

		override val sequencer: SchedulingDoer = schedulerDp.provide(s"sequencer-$nodeId")

		override given participantIdSerializer: Serializer[ParticipantId] = strSer
		override given participantIdDeserializer: Deserializer[ParticipantId] = strDeser
		override given clientCommandSerializer: Serializer[ClientCommand] = strSer
		override given clientCommandDeserializer: Deserializer[ClientCommand] = strDeser
		override given stateMachineResponseSerializer: Serializer[StateMachineResponse] = strSer
		override given stateMachineResponseDeserializer: Deserializer[StateMachineResponse] = strDeser

		override def contactAddressOf(id: ParticipantId): SocketAddress = peerAddressMap.getOrElse(id, new InetSocketAddress("127.0.0.1", 0))
		override def localBindAddress: SocketAddress = bindAddress
	}

	test("Server returns HandshakeAck upon valid PeerHandshake") {
		val sdm = new TestSdm("server-node", new InetSocketAddress("127.0.0.1", 0))
		val bridge = new sdm.Nio2ClusterBridge("server-node")
		bridge.start()

		val serverAddress = bridge.localAddress
		val clientChannel = AsynchronousSocketChannel.open()
		try {
			clientChannel.connect(serverAddress).get(5, TimeUnit.SECONDS)

			val transmitterContext = new Transmitter.Context {
				override def myAddress: SocketAddress = clientChannel.getLocalAddress
				override def oPeerAddress: Maybe[SocketAddress] = Maybe.some(serverAddress)
				override def channelId: ChannelId = new ChannelId(ChannelOrigin.INITIATED, Maybe.some(serverAddress))
			}
			val receiverContext = new Receiver.Context {
				override def myAddress: SocketAddress = clientChannel.getLocalAddress
				override def oPeerAddress: Maybe[SocketAddress] = Maybe.some(serverAddress)
				override def channelId: ChannelId = new ChannelId(ChannelOrigin.INITIATED, Maybe.some(serverAddress))
			}

			val transmitter = new SequentialTransmitter[sdm.wireProtocol.ClusterWireMessage](clientChannel, transmitterContext)
			val receiver = new Receiver(clientChannel, receiverContext)

			val promise = Promise[sdm.wireProtocol.ClusterWireMessage]()
			receiver.receive[sdm.wireProtocol.ClusterWireMessage](ProtocolVersion(1), 5, TimeUnit.SECONDS) {
				case msg: sdm.wireProtocol.ClusterWireMessage => promise.success(msg)
				case fault: Receiver.Fault => promise.failure(new RuntimeException(s"Fault: $fault"))
			}(using sdm.wireProtocol.clusterWireMessageDeserializer)

			val handshake = sdm.wireProtocol.PeerHandshake("client-peer", Set(ProtocolVersion(1)))
			transmitter.transmit(handshake, ProtocolVersion(1)) { _ => () }(using sdm.wireProtocol.clusterWireMessageSerializer)

			val response = Await.result(promise.future, 5.seconds)
			response match {
				case ack: sdm.wireProtocol.HandshakeAck =>
					assertEquals(ack.responderId, "server-node")
					assertEquals(ack.agreedVersion, ProtocolVersion(1))
				case other =>
					fail(s"Expected HandshakeAck, got $other")
			}
		} finally {
			clientChannel.close()
			bridge.close()
		}
	}

	test("Server returns HandshakeReject upon incompatible protocol version") {
		val sdm = new TestSdm("server-node", new InetSocketAddress("127.0.0.1", 0))
		val bridge = new sdm.Nio2ClusterBridge("server-node")
		bridge.start()

		val serverAddress = bridge.localAddress
		val clientChannel = AsynchronousSocketChannel.open()
		try {
			clientChannel.connect(serverAddress).get(5, TimeUnit.SECONDS)

			val transmitterContext = new Transmitter.Context {
				override def myAddress: SocketAddress = clientChannel.getLocalAddress
				override def oPeerAddress: Maybe[SocketAddress] = Maybe.some(serverAddress)
				override def channelId: ChannelId = new ChannelId(ChannelOrigin.INITIATED, Maybe.some(serverAddress))
			}
			val receiverContext = new Receiver.Context {
				override def myAddress: SocketAddress = clientChannel.getLocalAddress
				override def oPeerAddress: Maybe[SocketAddress] = Maybe.some(serverAddress)
				override def channelId: ChannelId = new ChannelId(ChannelOrigin.INITIATED, Maybe.some(serverAddress))
			}

			val transmitter = new SequentialTransmitter[sdm.wireProtocol.ClusterWireMessage](clientChannel, transmitterContext)
			val receiver = new Receiver(clientChannel, receiverContext)

			val promise = Promise[sdm.wireProtocol.ClusterWireMessage]()
			receiver.receive[sdm.wireProtocol.ClusterWireMessage](ProtocolVersion(1), 5, TimeUnit.SECONDS) {
				case msg: sdm.wireProtocol.ClusterWireMessage => promise.success(msg)
				case fault: Receiver.Fault => promise.failure(new RuntimeException(s"Fault: $fault"))
			}(using sdm.wireProtocol.clusterWireMessageDeserializer)

			val handshake = sdm.wireProtocol.PeerHandshake("client-peer", Set(ProtocolVersion(999)))
			transmitter.transmit(handshake, ProtocolVersion(1)) { _ => () }(using sdm.wireProtocol.clusterWireMessageSerializer)

			val response = Await.result(promise.future, 5.seconds)
			response match {
				case reject: sdm.wireProtocol.HandshakeReject =>
					assert(reject.reason.contains("Incompatible protocol version"))
				case other =>
					fail(s"Expected HandshakeReject, got $other")
			}
		} finally {
			clientChannel.close()
			bridge.close()
		}
	}

	test("ClusterClient establishes connection via ClientHandshake") {
		val sdm = new TestSdm("server-node", new InetSocketAddress("127.0.0.1", 0))
		val bridge = new sdm.Nio2ClusterBridge("server-node")
		bridge.start()

		val client = new ClusterClient(sdm)(bridge.localAddress)
		try {
			val connectFuture = client.connect(5000L)
			Await.result(connectFuture, 5.seconds)
		} finally {
			client.close()
			bridge.close()
		}
	}

	test("Server selects highest common protocol version") {
		val sdm = new TestSdm("server-node", new InetSocketAddress("127.0.0.1", 0)) {
			override val bridgeConfig: ClusterBridgeConfig = ClusterBridgeConfig(
				supportedVersions = Set(ProtocolVersion(1), ProtocolVersion(2))
			)
		}
		val bridge = new sdm.Nio2ClusterBridge("server-node")
		bridge.start()

		val serverAddress = bridge.localAddress
		val clientChannel = AsynchronousSocketChannel.open()
		try {
			clientChannel.connect(serverAddress).get(5, TimeUnit.SECONDS)

			val transmitterContext = new Transmitter.Context {
				override def myAddress: SocketAddress = clientChannel.getLocalAddress
				override def oPeerAddress: Maybe[SocketAddress] = Maybe.some(serverAddress)
				override def channelId: ChannelId = new ChannelId(ChannelOrigin.INITIATED, Maybe.some(serverAddress))
			}
			val receiverContext = new Receiver.Context {
				override def myAddress: SocketAddress = clientChannel.getLocalAddress
				override def oPeerAddress: Maybe[SocketAddress] = Maybe.some(serverAddress)
				override def channelId: ChannelId = new ChannelId(ChannelOrigin.INITIATED, Maybe.some(serverAddress))
			}

			val transmitter = new SequentialTransmitter[sdm.wireProtocol.ClusterWireMessage](clientChannel, transmitterContext)
			val receiver = new Receiver(clientChannel, receiverContext)

			val promise = Promise[sdm.wireProtocol.ClusterWireMessage]()
			receiver.receive[sdm.wireProtocol.ClusterWireMessage](ProtocolVersion.OF_THIS_PROJECT, 5, TimeUnit.SECONDS) {
				case msg: sdm.wireProtocol.ClusterWireMessage => promise.success(msg)
				case fault: Receiver.Fault => promise.failure(new RuntimeException(s"Fault: $fault"))
			}(using sdm.wireProtocol.clusterWireMessageDeserializer)

			val handshake = sdm.wireProtocol.PeerHandshake("client-peer", Set(ProtocolVersion(1), ProtocolVersion(2)))
			transmitter.transmit(handshake, ProtocolVersion.OF_THIS_PROJECT) { _ => () }(using sdm.wireProtocol.clusterWireMessageSerializer)

			val response = Await.result(promise.future, 5.seconds)
			response match {
				case ack: sdm.wireProtocol.HandshakeAck =>
					assertEquals(ack.responderId, "server-node")
					assertEquals(ack.agreedVersion, ProtocolVersion(2))
				case other =>
					fail(s"Expected HandshakeAck with version 2, got $other")
			}
		} finally {
			clientChannel.close()
			bridge.close()
		}
	}

	test("Server rejects client when no common protocol version exists") {
		val sdm = new TestSdm("server-node", new InetSocketAddress("127.0.0.1", 0)) {
			override val bridgeConfig: ClusterBridgeConfig = ClusterBridgeConfig(
				supportedVersions = Set(ProtocolVersion(2))
			)
		}
		val bridge = new sdm.Nio2ClusterBridge("server-node")
		bridge.start()

		val serverAddress = bridge.localAddress
		val clientChannel = AsynchronousSocketChannel.open()
		try {
			clientChannel.connect(serverAddress).get(5, TimeUnit.SECONDS)

			val transmitterContext = new Transmitter.Context {
				override def myAddress: SocketAddress = clientChannel.getLocalAddress
				override def oPeerAddress: Maybe[SocketAddress] = Maybe.some(serverAddress)
				override def channelId: ChannelId = new ChannelId(ChannelOrigin.INITIATED, Maybe.some(serverAddress))
			}
			val receiverContext = new Receiver.Context {
				override def myAddress: SocketAddress = clientChannel.getLocalAddress
				override def oPeerAddress: Maybe[SocketAddress] = Maybe.some(serverAddress)
				override def channelId: ChannelId = new ChannelId(ChannelOrigin.INITIATED, Maybe.some(serverAddress))
			}

			val transmitter = new SequentialTransmitter[sdm.wireProtocol.ClusterWireMessage](clientChannel, transmitterContext)
			val receiver = new Receiver(clientChannel, receiverContext)

			val promise = Promise[sdm.wireProtocol.ClusterWireMessage]()
			receiver.receive[sdm.wireProtocol.ClusterWireMessage](ProtocolVersion.OF_THIS_PROJECT, 5, TimeUnit.SECONDS) {
				case msg: sdm.wireProtocol.ClusterWireMessage => promise.success(msg)
				case fault: Receiver.Fault => promise.failure(new RuntimeException(s"Fault: $fault"))
			}(using sdm.wireProtocol.clusterWireMessageDeserializer)

			val handshake = sdm.wireProtocol.PeerHandshake("client-peer", Set(ProtocolVersion(1)))
			transmitter.transmit(handshake, ProtocolVersion.OF_THIS_PROJECT) { _ => () }(using sdm.wireProtocol.clusterWireMessageSerializer)

			val response = Await.result(promise.future, 5.seconds)
			response match {
				case reject: sdm.wireProtocol.HandshakeReject =>
					assert(reject.reason.contains("Incompatible protocol version"))
				case other =>
					fail(s"Expected HandshakeReject, got $other")
			}
		} finally {
			clientChannel.close()
			bridge.close()
		}
	}
}
