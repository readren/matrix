package readren.matrix
package cluster.misc

import java.net.SocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousServerSocketChannel, AsynchronousSocketChannel, CompletionHandler}

object BasicNio2 {

	trait ConnectionsAcceptanceHandler[A] {
		def onConnectionAccepted(clientChannel: AsynchronousSocketChannel, attachment: A): A

		def onConnectionFailed(exc: Throwable, attachment: A): A
	}
	
	def startAsyncServer[A](socketAddress: SocketAddress, attachment: A, connectionAcceptanceHandler: ConnectionsAcceptanceHandler[A]): AsynchronousServerSocketChannel = {

		val serverSocketChannel = AsynchronousServerSocketChannel.open()
			.bind(socketAddress)

		def acceptNext(attachment: A): Unit = {
			serverSocketChannel.accept(attachment, new CompletionHandler[AsynchronousSocketChannel, A]() {
				override def completed(clientChannel: AsynchronousSocketChannel, attachment: A): Unit = {
					acceptNext(connectionAcceptanceHandler.onConnectionAccepted(clientChannel, attachment))
				}

				override def failed(exc: Throwable, attachment: A): Unit = {
					acceptNext(connectionAcceptanceHandler.onConnectionFailed(exc, attachment))
				}
			})
		}

		acceptNext(attachment)
		serverSocketChannel
	}


	trait ReceptionHandler[S] {
		val receiveBuffer: ByteBuffer

		def onReceived(numOfBytes: Int, state: S): S

		def onReceivingFailure(exc: Throwable, state: S): S
	}

	class AsyncReceiver[S](channel: AsynchronousSocketChannel, receptionHandler: ReceptionHandler[S]) extends CompletionHandler[Integer, S] {

		inline def startReception(initialState: S): Unit = receiveNext(initialState)

		private def receiveNext(currentState: S): Unit = {
			channel.read(receptionHandler.receiveBuffer, currentState, this)
		}

		override final def completed(bytesReceived: Integer, newState: S): Unit = {
			receiveNext(receptionHandler.onReceived(bytesReceived, newState))
		}

		override final def failed(exc: Throwable, attachment: S): Unit = {
			receiveNext(receptionHandler.onReceivingFailure(exc, attachment))
		}
	}

	trait TransmissionHandler[S] {
		val sendBuffer: ByteBuffer

		def onSent(numOfBytes: Int, state: S): S

		def onSendingFailure(exc: Throwable, state: S): S
	}

	class AsyncTransmitter[S](channel: AsynchronousSocketChannel, transmissionHandler: TransmissionHandler[S]) extends CompletionHandler[Integer, S] {
		
		inline def startTransmission(initialState: S): Unit = transmitNext(initialState)

		private def transmitNext(currentState: S): Unit = {
			channel.write(transmissionHandler.sendBuffer, currentState, this)
		}

		override def completed(bytesSent: Integer, newState: S): Unit = {
			transmitNext(transmissionHandler.onSent(bytesSent, newState))
		}

		override def failed(exc: Throwable, newState: S): Unit = {
			transmitNext(transmissionHandler.onSendingFailure(exc, newState))
		}
	}

}
