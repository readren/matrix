package readren.matrix

import readren.taskflow.{Doer, Maybe}

import java.net.{InetAddress, URI}


object Matrix {
	trait Aide {
		def reportFailure(cause: Throwable): Unit

		def buildDoerAssistant(doerId: Int): Doer.Assistant
	}
}

class Matrix(val name: String, aide: Matrix.Aide) { thisMatrix =>

	import Matrix.*

	/**
	 * The array with all the [[MatrixDoer]] instances of this [[Matrix]]. */
	private val matrixDoers: IArray[MatrixDoer] = {
		val availableProcessors = Runtime.getRuntime.availableProcessors()
		IArray.tabulate(availableProcessors) { index =>
			val doerId = index + 1
			new MatrixDoer(doerId, aide.buildDoerAssistant(doerId), thisMatrix)
		}
	}

	val doer: MatrixDoer = matrixDoers(0)

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

	inline def pickDoer(serialNumber: Reactant.SerialNumber): MatrixDoer =
		matrixDoers(serialNumber % matrixDoers.length)

	/** thread-safe */
	def spawn[U](reactantFactory: ReactantFactory)(initialBehaviorBuilder: ReactantRelay[U] => Behavior[U])(using isSignalTest: IsSignalTest[U]): doer.Duty[ReactantRelay[U]] =
		doer.Duty.mineFlat { () =>
			spawner.createReactant[U](reactantFactory, isSignalTest, initialBehaviorBuilder)
		}

	def buildEndpointProvider[A](callback: A => Unit): EndpointProvider[A] = {
		val receiver = new Receiver[A]{
			override def submit(message: A): Unit = callback(message)
			override def uri: _root_.java.net.URI = thisMatrix.uri
		}
		// TODO register the receiver in order to be accessible from remote JVMs.
		new EndpointProvider[A](receiver)
	}

	def buildEndpoint[A](receiver: A => Unit): Endpoint[A] = buildEndpointProvider[A](receiver).local
}
