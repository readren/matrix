package readren.nexus
package cluster.channel

import cluster.channel.Receiver.{Fault, ReceptionFailure}
import cluster.misc.{RingLength, VLQ}
import cluster.serialization.{Deserializer, ProtocolVersion}
import cluster.service.{ChannelId, ChannelOrigin}

import munit.FunSuite
import readren.common.{ByteSink, ByteSource, Maybe}

import java.io.{ByteArrayOutputStream, IOException}
import java.net.{InetSocketAddress, SocketAddress, SocketOption}
import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousSocketChannel, Channels, ClosedChannelException, CompletionHandler}
import java.util
import java.util.Collections
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{Future, TimeUnit}
import scala.collection.mutable.ArrayBuffer

class ReceiverSuite extends FunSuite {

	private case class WireFrameMessage(payload: Array[Byte], trailingStream: Option[ByteSource])

	private given Deserializer[WireFrameMessage] = (reader: Deserializer.Reader) => {
		val hasTrailingStream = reader.readByte() != 0
		val payloadLength = reader.readUnsignedIntVlq()
		val payload = reader.readBytes(payloadLength)
		val trailingStream = if hasTrailingStream then Some(reader.readByteSource()) else None
		WireFrameMessage(payload, trailingStream)
	}

	private final class ScriptedReadSocketChannel(
		initialChunks: List[Array[Byte]],
		closeFailure: Option[Throwable] = None
	) extends AsynchronousSocketChannel(null) {
		private val localAddress = new InetSocketAddress("127.0.0.1", 9101)
		private val remoteAddress = new InetSocketAddress("127.0.0.1", 9102)
		private val pendingChunks = new util.ArrayDeque[Array[Byte]]()
		private val closed = new AtomicBoolean(false)

		val closeInvocationCount = new AtomicInteger(0)
		val recordedReadBounds = new ArrayBuffer[(Int, Int)]()

		initialChunks.foreach { chunk =>
			if chunk.nonEmpty then pendingChunks.addLast(chunk.clone())
		}

		override def isOpen: Boolean = !closed.get()

		override def close(): Unit = {
			closeInvocationCount.incrementAndGet()
			closed.set(true)
			closeFailure.foreach(throw _)
		}

		override def getLocalAddress: SocketAddress = localAddress
		override def getRemoteAddress: SocketAddress = remoteAddress
		override def supportedOptions(): util.Set[SocketOption[?]] = Collections.emptySet()
		override def bind(local: SocketAddress): AsynchronousSocketChannel = this
		override def setOption[T](name: SocketOption[T], value: T): AsynchronousSocketChannel = this
		override def shutdownInput(): AsynchronousSocketChannel = this
		override def shutdownOutput(): AsynchronousSocketChannel = this
		override def getOption[T](name: SocketOption[T]): T = throw new UnsupportedOperationException()
		override def connect[A](remote: SocketAddress, attachment: A, handler: CompletionHandler[Void, ? >: A]): Unit = throw new UnsupportedOperationException()
		override def connect(remote: SocketAddress): Future[Void] = throw new UnsupportedOperationException()

		override def read[A](
			destination: ByteBuffer,
			timeout: Long,
			unit: TimeUnit,
			attachment: A,
			handler: CompletionHandler[Integer, ? >: A]
		): Unit = {
			recordedReadBounds.addOne((destination.position(), destination.limit()))
			val anyHandler = handler.asInstanceOf[CompletionHandler[Integer, Any]]
			if closed.get() then anyHandler.failed(new ClosedChannelException(), attachment)
			else if pendingChunks.isEmpty then anyHandler.completed(Integer.valueOf(-1), attachment)
			else {
				val chunk = pendingChunks.poll()
				val bytesToCopy = math.min(chunk.length, destination.remaining())
				assert(bytesToCopy > 0, s"Expected positive writable space in destination buffer, got position=${destination.position()}, limit=${destination.limit()}")
				destination.put(chunk, 0, bytesToCopy)
				if bytesToCopy < chunk.length then pendingChunks.addFirst(util.Arrays.copyOfRange(chunk, bytesToCopy, chunk.length))
				anyHandler.completed(Integer.valueOf(bytesToCopy), attachment)
			}
		}

		override def read(destination: ByteBuffer): Future[Integer] = throw new UnsupportedOperationException()
		override def read[A](destinations: Array[ByteBuffer], offset: Int, length: Int, timeout: Long, unit: TimeUnit, attachment: A, handler: CompletionHandler[java.lang.Long, ? >: A]): Unit = throw new UnsupportedOperationException()
		override def write[A](source: ByteBuffer, timeout: Long, unit: TimeUnit, attachment: A, handler: CompletionHandler[Integer, ? >: A]): Unit = throw new UnsupportedOperationException()
		override def write(source: ByteBuffer): Future[Integer] = throw new UnsupportedOperationException()
		override def write[A](sources: Array[ByteBuffer], offset: Int, length: Int, timeout: Long, unit: TimeUnit, attachment: A, handler: CompletionHandler[java.lang.Long, ? >: A]): Unit = throw new UnsupportedOperationException()
	}

	private def encodeVlqUnsignedInt(value: Int): Array[Byte] = {
		val output = new ByteArrayOutputStream()
		VLQ.encodeUnsignedInt(value, (byte: Byte) => output.write(byte & 0xff))
		output.toByteArray
	}

	private def encodePlainPayloadControlBytes(payload: Array[Byte]): Array[Byte] = {
		val output = new ByteArrayOutputStream()
		output.write(0)
		output.write(encodeVlqUnsignedInt(payload.length))
		output.write(payload)
		output.toByteArray
	}

	private def encodeFrames(frames: List[Array[Byte]], sentinel: Array[Byte]): Array[Byte] = {
		val output = new ByteArrayOutputStream()
		frames.foreach { frameContent =>
			output.write(encodeVlqUnsignedInt(frameContent.length))
			output.write(frameContent)
		}
		output.write(sentinel)
		output.toByteArray
	}

	private def readAllStreamBytes(byteSource: ByteSource): Array[Byte] = {
		val output = new ByteArrayOutputStream()
		val writableChannel = Channels.newChannel(output)
		try byteSource.transferTo(writableChannel) finally writableChannel.close()
		output.toByteArray
	}

	private def createContext(channel: AsynchronousSocketChannel): (SocketAddress, Maybe[SocketAddress], ChannelId, Receiver.Context) = {
		val localAddress = channel.getLocalAddress
		val remoteAddress = Maybe.some(channel.getRemoteAddress)
		val createdChannelId = new ChannelId(ChannelOrigin.ACCEPTED, Maybe.some(localAddress))
		val context = new Receiver.Context {
			override def myAddress: SocketAddress = localAddress
			override def oPeerAddress: Maybe[SocketAddress] = remoteAddress
			override def channelId: ChannelId = createdChannelId
		}
		(localAddress, remoteAddress, createdChannelId, context)
	}

	test("first reception enforces buffersInitialLimit before extending for a split multi-byte header") {
		val capacityOfBuferInRing = 28
		val expectedInitialLimit = capacityOfBuferInRing - VLQ.INT_MAX_LENGTH + 1
		val expectedPayload = Array.tabulate[Byte](150)(index => (index & 0x7f).toByte)
		val controlBytes = encodePlainPayloadControlBytes(expectedPayload)
		assertEquals(controlBytes.length, 153)

		val firstFrameContent = util.Arrays.copyOfRange(controlBytes, 0, 25)
		val secondFrameContent = util.Arrays.copyOfRange(controlBytes, 25, controlBytes.length)
		assertEquals(secondFrameContent.length, 128)

		val wireBytes = encodeFrames(List(firstFrameContent, secondFrameContent), Array(0.toByte))
		val firstChunkLength = 27
		val channel = new ScriptedReadSocketChannel(
			List(
				util.Arrays.copyOfRange(wireBytes, 0, firstChunkLength),
				util.Arrays.copyOfRange(wireBytes, firstChunkLength, wireBytes.length)
			)
		)
		val (_, _, _, context) = createContext(channel)
		val receiver = new Receiver(channel, context, capacityOfBufferInRing = capacityOfBuferInRing, ringMaxLength = RingLength(16))

		var recordedOutcome: WireFrameMessage | Fault | Null = null
		receiver.receive[WireFrameMessage](ProtocolVersion.OF_THIS_PROJECT, 1L, TimeUnit.SECONDS) { outcome =>
			recordedOutcome = outcome
		}

		assertEquals(channel.recordedReadBounds.head, (0, expectedInitialLimit))
		channel.recordedReadBounds.foreach {
			case (0, limit) => assertEquals(limit, expectedInitialLimit)
			case _ => ()
		}

		recordedOutcome match {
			case message: WireFrameMessage =>
				assertEquals(message.trailingStream, None)
				assert(util.Arrays.equals(message.payload, expectedPayload))
			case other =>
				fail(s"Expected WireFrameMessage, got $other")
		}
	}

	test("trailing stream Case 2 compaction resets limit to buffersInitialLimit before pipelined split header") {
		val capacityOfBufferInRing = 32
		val expectedInitialLimit = capacityOfBufferInRing - VLQ.INT_MAX_LENGTH + 1
		val expectedStreamPayload = Array(0x5a.toByte)
		val firstMessageControlBytes = Array[Byte](1, 0, expectedStreamPayload.length.toByte)
		val firstMessageWireBytes = {
			val output = new ByteArrayOutputStream()
			output.write(encodeFrames(List(firstMessageControlBytes), Transmitter.STREAM_FOLLOWS_SENTINEL_ENCODED))
			output.write(expectedStreamPayload.length)
			output.write(expectedStreamPayload)
			output.write(0)
			output.toByteArray
		}
		assertEquals(firstMessageWireBytes.length, 12)

		val expectedSecondPayload = Array.tabulate[Byte](148)(index => ((index + 7) & 0x7f).toByte)
		val secondControlBytes = encodePlainPayloadControlBytes(expectedSecondPayload)
		assertEquals(secondControlBytes.length, 151)

		val secondMessageFrameOne = util.Arrays.copyOfRange(secondControlBytes, 0, 13)
		val secondMessageFrameTwo = util.Arrays.copyOfRange(secondControlBytes, 13, 23)
		val secondMessageFrameThree = util.Arrays.copyOfRange(secondControlBytes, 23, secondControlBytes.length)
		assertEquals(secondMessageFrameThree.length, 128)
		val secondMessageWireBytes = encodeFrames(List(secondMessageFrameOne, secondMessageFrameTwo, secondMessageFrameThree), Array(0.toByte))

		val combinedWire = new Array[Byte](firstMessageWireBytes.length + secondMessageWireBytes.length)
		System.arraycopy(firstMessageWireBytes, 0, combinedWire, 0, firstMessageWireBytes.length)
		System.arraycopy(secondMessageWireBytes, 0, combinedWire, firstMessageWireBytes.length, secondMessageWireBytes.length)

		val firstChunkEnd = expectedInitialLimit
		val secondChunkEnd = firstChunkEnd + 10
		val channel = new ScriptedReadSocketChannel(
			List(
				util.Arrays.copyOfRange(combinedWire, 0, firstChunkEnd),
				util.Arrays.copyOfRange(combinedWire, firstChunkEnd, secondChunkEnd),
				util.Arrays.copyOfRange(combinedWire, secondChunkEnd, combinedWire.length)
			)
		)
		val (_, _, _, context) = createContext(channel)
		val receiver = new Receiver(
			channel,
			context,
			capacityOfBufferInRing = capacityOfBufferInRing,
			ringMaxLength = RingLength(16),
			byteSinkAllocator = new ByteSink.DefaultAllocator(inMemoryThreshold = 64)
		)

		var firstOutcome: WireFrameMessage | Fault | Null = null
		receiver.receive[WireFrameMessage](ProtocolVersion.OF_THIS_PROJECT, 1L, TimeUnit.SECONDS) { outcome =>
			firstOutcome = outcome
		}

		firstOutcome match {
			case message: WireFrameMessage =>
				assertEquals(message.payload.length, 0)
				val streamBytes = message.trailingStream.map(readAllStreamBytes).getOrElse(fail("Expected trailing stream on first message"))
				assert(util.Arrays.equals(streamBytes, expectedStreamPayload))
			case other =>
				fail(s"Expected first WireFrameMessage, got $other")
		}

		var secondOutcome: WireFrameMessage | Fault | Null = null
		receiver.receive[WireFrameMessage](ProtocolVersion.OF_THIS_PROJECT, 1L, TimeUnit.SECONDS) { outcome =>
			secondOutcome = outcome
		}

		assertEquals(channel.recordedReadBounds(0), (0, expectedInitialLimit))
		assertEquals(channel.recordedReadBounds(1), (16, expectedInitialLimit))
		assertEquals(channel.recordedReadBounds(2), (26, 25 + VLQ.INT_MAX_LENGTH))

		secondOutcome match {
			case message: WireFrameMessage =>
				assertEquals(message.trailingStream, None)
				assert(util.Arrays.equals(message.payload, expectedSecondPayload))
			case other =>
				fail(s"Expected second WireFrameMessage, got $other")
		}
	}

	test("framing scan overflow exception reports ReceptionFailure and closes channel") {
		val malformedFrameHeader = Array.fill[Byte](7)(0x81.toByte) :+ 0x01.toByte
		val channel = new ScriptedReadSocketChannel(List(malformedFrameHeader))
		val (localAddress, remoteAddress, channelId, context) = createContext(channel)
		val receiver = new Receiver(channel, context, capacityOfBufferInRing = 64, ringMaxLength = RingLength(4))

		var recordedOutcome: WireFrameMessage | Fault | Null = null
		receiver.receive[WireFrameMessage](ProtocolVersion.OF_THIS_PROJECT, 1L, TimeUnit.SECONDS) { outcome =>
			recordedOutcome = outcome
		}

		assertEquals(channel.closeInvocationCount.get(), 1)
		assert(!channel.isOpen)
		recordedOutcome match {
			case ReceptionFailure(actualMyAddress, actualPeerAddress, actualChannelId, cause) =>
				assertEquals(actualMyAddress, localAddress)
				assertEquals(actualPeerAddress, remoteAddress)
				assertEquals(actualChannelId, channelId)
				assert(cause.isInstanceOf[VLQ.IntegerOverflowException], s"Expected VLQ.IntegerOverflowException, got $cause")
			case other =>
				fail(s"Expected ReceptionFailure, got $other")
		}
	}

	test("framing scan failure delivers ReceptionFailure even if closing channel throws NonFatal IOException") {
		val malformedFrameHeader = Array.fill[Byte](7)(0x81.toByte) :+ 0x01.toByte
		val channel = new ScriptedReadSocketChannel(
			List(malformedFrameHeader),
			closeFailure = Some(new IOException("simulated close error"))
		)
		val (localAddress, remoteAddress, channelId, context) = createContext(channel)
		val receiver = new Receiver(channel, context, capacityOfBufferInRing = 64, ringMaxLength = RingLength(4))

		var recordedOutcome: WireFrameMessage | Fault | Null = null
		receiver.receive[WireFrameMessage](ProtocolVersion.OF_THIS_PROJECT, 1L, TimeUnit.SECONDS) { outcome =>
			recordedOutcome = outcome
		}

		assertEquals(channel.closeInvocationCount.get(), 1)
		recordedOutcome match {
			case ReceptionFailure(actualMyAddress, actualPeerAddress, actualChannelId, cause) =>
				assertEquals(actualMyAddress, localAddress)
				assertEquals(actualPeerAddress, remoteAddress)
				assertEquals(actualChannelId, channelId)
				assert(cause.isInstanceOf[VLQ.IntegerOverflowException], s"Expected VLQ.IntegerOverflowException, got $cause")
			case other =>
				fail(s"Expected ReceptionFailure, got $other")
		}
	}
}
