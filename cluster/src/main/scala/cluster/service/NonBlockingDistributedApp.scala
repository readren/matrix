package readren.matrix
package cluster.service

import cluster.service.ClusterService.DelegateConfig
import cluster.service.ProtocolVersion
import providers.assistant.SchedulingDap

import java.net.{InetSocketAddress, SocketAddress}
import java.nio.ByteBuffer
import java.nio.channels.*
import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object NonBlockingDistributedApp {
	def main(args: Array[String]): Unit = {

		if (args.length < 3) {
			println("Usage: NonBlockingDistributedApp <my-ip> <my-port> <peer-ip:peer-port> ...")
			System.exit(1)
		}

		val myIp = args(0)
		val myPort = args(1).toInt
		val seedsAddresses = args.drop(2).map { addr =>
			val parts = addr.split(":")
			InetSocketAddress(parts(0), parts(1).toInt)
		}


		// Start the server to listen for incoming messages
		val myAddress = new InetSocketAddress(myIp, myPort)


		val schedulingDap = new SchedulingDap()
		val sequencer = new ClusterService.TaskSequencer {
			override type Assistant = SchedulingDap.SchedulingAssistant
			override val assistant: Assistant = schedulingDap.provide(0)
		}

		val sayHelloToEveryone = ClusterService.start(
			sequencer,
			new ClusterService.Config(
				myAddress,
				new DelegateConfig(Set(ProtocolVersion.OF_THIS_PROJECT), 1, 1, TimeUnit.SECONDS),
				500, 60_000, 5
			),
			seedsAddresses
		)

		// Connect to peers and send messages with retries
		seedsAddresses.foreach { contact =>
			sendMessageWithRetries(contact, s"Hello from $myIp:$myPort")
		}
	}


	def onConnectionAccepted(clientChannel: AsynchronousSocketChannel): Unit = {
		val buffer = ByteBuffer.allocateDirect(1024)
		clientChannel.read(buffer, null, new CompletionHandler[Integer, Null] {
			override def completed(bytesRead: Integer, attachment: Null): Unit = {
				buffer.flip()
				val message = new String(buffer.array(), 0, bytesRead)
				println(s"Received message: $message")
				val ackBuffer = ByteBuffer.wrap("Ack".getBytes)
				ackBuffer.get
				clientChannel.write(ackBuffer, 1, TimeUnit.SECONDS, null, new CompletionHandler[Integer, Null] {
					override def completed(result: Integer, attachment: Null): Unit = {
						println(s"Sent ACK to ${clientChannel.getRemoteAddress}")
						clientChannel.close()
					}

					override def failed(exc: Throwable, attachment: Null): Unit = {
						System.err.print(s"Something went wrong while trying to write the read-acknowledgment to the accepted connection:")
						exc.printStackTrace()
					}
				})

			}

			override def failed(exc: Throwable, attachment: Null): Unit = {
				System.err.print(s"Something went wrong while trying to read from the accepted connection:")
				exc.printStackTrace()
			}
		})
	}

	def startServer(ip: String, port: Int): Unit = {
		Future {
			val selector = Selector.open()
			val serverChannel = ServerSocketChannel.open()
			serverChannel.bind(new InetSocketAddress(ip, port))
			serverChannel.configureBlocking(false)
			serverChannel.register(selector, java.nio.channels.SelectionKey.OP_ACCEPT)

			println(s"Server started on $ip:$port")

			while (true) {
				selector.select()
				val selectedKeys = selector.selectedKeys().iterator()

				while (selectedKeys.hasNext) {
					val key = selectedKeys.next()
					selectedKeys.remove()

					if (key.isAcceptable) {
						val clientChannel = serverChannel.accept()
						clientChannel.configureBlocking(false)
						clientChannel.register(selector, java.nio.channels.SelectionKey.OP_READ)
						println(s"Accepted connection from ${clientChannel.getRemoteAddress}")
					}

					if (key.isReadable) {
						val clientChannel = key.channel().asInstanceOf[SocketChannel]
						val buffer = ByteBuffer.allocateDirect(1024)
						val bytesRead = clientChannel.read(buffer)

						if (bytesRead > 0) {
							buffer.flip()
							val message = new String(buffer.array(), 0, bytesRead)
							println(s"Received message: $message")

							// Send acknowledgment (ACK) back to the sender
							val ackMessage = "ACK"
							val ackBuffer = ByteBuffer.wrap(ackMessage.getBytes)
							while (ackBuffer.hasRemaining) {
								clientChannel.write(ackBuffer)
							}
							println(s"Sent ACK to ${clientChannel.getRemoteAddress}")
						} else if (bytesRead == -1) {
							clientChannel.close()
						}
					}
				}
			}
		}
	}

	def sendMessageWithRetries(address: SocketAddress, message: String): Unit = {
		var retryCount = 0
		val maxRetries = 5
		val initialDelay = 1000 // Initial delay in milliseconds
		val maxDelay = 10000 // Maximum delay in milliseconds

		while (retryCount < maxRetries) {
			Try {
				val clientChannel = SocketChannel.open()
				clientChannel.configureBlocking(false)

				// Connect to the peer (non-blocking)
				clientChannel.connect(address)

				// Use a Selector to wait for the connection to complete
				val selector = Selector.open()
				clientChannel.register(selector, SelectionKey.OP_CONNECT)

				var connected = false
				while (!connected) {
					selector.select() // Wait for an event
					val selectedKeys = selector.selectedKeys().iterator()

					while (selectedKeys.hasNext) {
						val key = selectedKeys.next()
						selectedKeys.remove()

						if (key.isConnectable) {
							// Finish the connection
							connected = clientChannel.finishConnect()
							if (connected) {
								println(s"Connected to $address")
							}
						}
					}
				}

				// Send the message
				val buffer = ByteBuffer.wrap(message.getBytes())
				while (buffer.hasRemaining) {
					clientChannel.write(buffer)
				}
				println(s"Sent message to $address: $message")

				// Wait for acknowledgment (ACK)
				val ackBuffer = ByteBuffer.allocate(1024)
				clientChannel.register(selector, SelectionKey.OP_READ)

				var ackReceived = false
				while (!ackReceived) {
					selector.select() // Wait for an event
					val selectedKeys = selector.selectedKeys().iterator()

					while (selectedKeys.hasNext) {
						val key = selectedKeys.next()
						selectedKeys.remove()

						if (key.isReadable) {
							val bytesRead = clientChannel.read(ackBuffer)
							if (bytesRead > 0) {
								ackBuffer.flip()
								val ackMessage = new String(ackBuffer.array(), 0, bytesRead)
								if (ackMessage == "ACK") {
									println(s"Received ACK from $address")
									ackReceived = true
								}
							} else if (bytesRead == -1) {
								throw new Exception("Connection closed by peer")
							}
						}
					}
				}

				clientChannel.close()
				selector.close()
			} match {
				case Success(_) => return // Exit the retry loop on success
				case Failure(e) =>
					retryCount += 1
					if (retryCount < maxRetries) {
						val delay = Math.min(initialDelay * (1 << retryCount), maxDelay)
						println(s"Failed to send message to $address (attempt $retryCount/$maxRetries). Retrying in ${delay}ms...")
						Thread.sleep(delay)
					} else {
						println(s"Failed to send message to $address after $maxRetries attempts.")
					}
			}
		}
	}
}