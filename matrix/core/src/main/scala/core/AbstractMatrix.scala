package readren.matrix
package core

import readren.sequencer.Doer
import readren.sequencer.manager.DoerProviderDescriptor

import java.net.URI

abstract class AbstractMatrix(val name: String) extends Procreative { thisMatrix =>

	type MatrixDoerType <: Doer

	val doer: MatrixDoerType
	
	protected val spawner: Spawner[doer.type]

	val logger: Logger

	override val path: String = s"/$name/"
	
	val uri: URI

	def provideDoer[D <: Doer](descriptor: DoerProviderDescriptor[D], tag: descriptor.Tag): D

	def provideDoer[D <: Doer](text: String, descriptor: DoerProviderDescriptor[D]): D


	/** thread-safe */
	def spawns[U, CD <: Doer](
		childFactory: ReactantFactory,
		childDoer: CD
	)(
		initialBehaviorBuilder: ReactantRelay[U, CD] => Behavior[U]
	)(
		using isSignalTest: IsSignalTest[U]
	): doer.Duty[ReactantRelay[U, CD]] = {
		doer.Duty_mineFlat { () =>
			spawner.createsReactant[U, CD](childFactory, childDoer, isSignalTest, initialBehaviorBuilder)
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
