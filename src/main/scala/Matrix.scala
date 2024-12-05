package readren.matrix

import readren.taskflow.{Doer, Maybe}

import java.net.{InetAddress, URI}


object Matrix {
	trait DoerProvider {
		def pick(serial: Int): MatrixDoer
	}

	trait Aide[DP <: DoerProvider] {
		def buildDoerProvider(owner: Matrix[DP]): DP
	}
}

class Matrix[+DP <: Matrix.DoerProvider](val name: String, aide: Matrix.Aide[DP]) { thisMatrix =>

	import Matrix.*

	val doerProvider: DP = aide.buildDoerProvider(thisMatrix)

	val doer: MatrixDoer = pickDoer(0)

	private val spawner: Spawner[doer.type] = new Spawner(Maybe.empty, doer, 0)

	val uri: URI = {
		val host = InetAddress.getLocalHost.getHostAddress // Or use getHostAddress for IP

		// Define the URI components
		val scheme = "http" // You can change this to "https" if needed
		val port = 8080 // Replace with the desired port, or omit it if not needed
		val path = s"/$name/" // Replace with your specific path

		// Construct the URI
		new URI(scheme, null, host, port, path, null, null)
	}

	inline def pickDoer(serialNumber: Reactant.SerialNumber): MatrixDoer = doerProvider.pick(serialNumber)

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
