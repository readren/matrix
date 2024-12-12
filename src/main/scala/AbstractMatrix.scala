package readren.matrix

import readren.taskflow.Maybe

import java.net.{InetAddress, URI}

abstract class AbstractMatrix(val name: String) { thisMatrix =>
	
	val doer: MatrixDoer
	
	protected val spawner: Spawner[doer.type]

	val logger: Logger

	val uri: URI = {
		val host = InetAddress.getLocalHost.getHostAddress // Or use getHostAddress for IP

		// Define the URI components
		val scheme = "http" // You can change this to "https" if needed
		val port = 8080 // Replace with the desired port, or omit it if not needed
		val path = s"/$name/" // Replace with your specific path

		// Construct the URI
		new URI(scheme, null, host, port, path, null, null)
	}

	def pickDoer(): MatrixDoer

	/** thread-safe */
	def spawn[U](reactantFactory: ReactantFactory)(initialBehaviorBuilder: ReactantRelay[U] => Behavior[U])(using isSignalTest: IsSignalTest[U]): doer.Duty[ReactantRelay[U]] =
		doer.Duty.mineFlat { () =>
			spawner.createReactant[U](reactantFactory, isSignalTest, initialBehaviorBuilder)
		}

	def buildEndpointProvider[A](callback: A => Unit): EndpointProvider[A] = {
		val receiver = new Receiver[A] {
			override def submit(message: A): Unit = callback(message)

			override def uri: _root_.java.net.URI = thisMatrix.uri
		}
		// TODO register the receiver in order to be accessible from remote JVMs.
		new EndpointProvider[A](receiver)
	}

	def buildEndpoint[A](receiver: A => Unit): Endpoint[A] = buildEndpointProvider[A](receiver).local
}
