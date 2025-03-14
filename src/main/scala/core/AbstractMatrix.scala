package readren.matrix
package core

import core.Matrix.DoerProviderDescriptor
import providers.assistant.DoerAssistantProvider.Tag

import java.net.{InetAddress, URI}
import java.util.concurrent.atomic.AtomicLong

abstract class AbstractMatrix(val name: String) { thisMatrix =>

	type DefaultDoer <: MatrixDoer
	
	val doer: DefaultDoer

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

	private val matrixDoerIdSequencer = new AtomicLong(0)
	
	def genTag(): Tag =
		matrixDoerIdSequencer.getAndIncrement()

	def provideDoer[D <: MatrixDoer](descriptor: DoerProviderDescriptor[D]): D
	
	def provideDefaultDoer: MatrixDoer

	/** thread-safe */
	def spawns[U](
		childFactory: ReactantFactory,
		childDoer: MatrixDoer = provideDefaultDoer
	)(
		initialBehaviorBuilder: ReactantRelay[U] => Behavior[U]
	)(
		using isSignalTest: IsSignalTest[U]
	): doer.Duty[ReactantRelay[U]] = {
		doer.Duty.mineFlat { () =>
			spawner.createsReactant[U](childFactory, childDoer, isSignalTest, initialBehaviorBuilder)
		}
	}

	def buildEndpointProvider[A](callback: A => Unit): EndpointProvider[A] = {
		val receiver = new Receiver[A] {
			override def submit(message: A): Unit = callback(message)

			override def uri: _root_.java.net.URI = thisMatrix.uri
		}
		// TODO register the receiver in order to be accessible from remote JVMs.
		new EndpointProvider[A](receiver)
	}

	def buildEndpoint[A](receiver: A => Unit): Endpoint[A] =
		buildEndpointProvider[A](receiver).local

}
