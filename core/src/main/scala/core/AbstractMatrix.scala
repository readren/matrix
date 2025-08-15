package readren.matrix
package core

import core.Matrix.DoerProviderDescriptor
import providers.DoerProvider
import providers.DoerProvider.Tag

import readren.sequencer.Doer

import java.net.URI
import java.util.concurrent.atomic.AtomicLong

abstract class AbstractMatrix(val name: String) extends Procreative { thisMatrix =>

	type DefaultDoer <: Doer
	
	val doer: DefaultDoer
	
	protected val spawner: Spawner[doer.type]

	val logger: Logger

	override val path: String = s"/$name/"
	
	val uri: URI

	private val doerTagSequencer = new AtomicLong(0)
	
	def genTag(): Tag =
		doerTagSequencer.getAndIncrement().toString

	def provideDoer[T <: Doer](tag: DoerProvider.Tag, descriptor: DoerProviderDescriptor[T]): T
	
	/** TODO: add parenthesis because it mutates the [[Matrix.DoerProvider]] */
	def provideDefaultDoer(tag: DoerProvider.Tag): DefaultDoer

	/** thread-safe */
	def spawns[U](
		childFactory: ReactantFactory,
		childDoer: Doer
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
