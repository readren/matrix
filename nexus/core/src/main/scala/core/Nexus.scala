package readren.nexus
package core

import readren.sequencer.Doer
import readren.sequencer.manager.DoerProviderDescriptor

import java.net.URI

/** A facade of [[NexusTyped]] that exposes what the contained [[Spuron]] need to know about.
 *  Nexus is an invented word that comes from nexus + orchestrator.
 * */
abstract class Nexus(val name: String) extends Procreative { thisNexus =>

	val doer: Doer
	
	protected val spawner: Spawner[doer.type]

	val logger: Logger

	override val path: String = s"/$name/"
	
	val uri: URI

	def provideDoer[D <: Doer](descriptor: DoerProviderDescriptor[D], tag: descriptor.Tag): D

	def provideDoer[D <: Doer](text: String, descriptor: DoerProviderDescriptor[D]): D


	/** thread-safe */
	def spawns[U, CD <: Doer](
		childFactory: SpuronFactory,
		childDoer: CD
	)(
		initialBehaviorBuilder: Spuron[U, CD] => Behavior[U]
	)(
		using isSignalTest: IsSignalTest[U]
	): doer.Duty[Spuron[U, CD]] = {
		doer.Duty_mineFlat { () =>
			spawner.createsSpuron[U, CD](childFactory, childDoer, isSignalTest, initialBehaviorBuilder)
		}
	}

	def buildEndpointProvider[A](callback: A => Unit): EndpointProvider[A] = {
		val receiver = new Receiver[A] {
			override def submit(message: A): Unit = callback(message)

			override def uri: _root_.java.net.URI = thisNexus.uri
		}
		// TODO register the receiver in order to be accessible from remote JVMs.
		new EndpointProvider[A](receiver)
	}

	def buildEndpoint[A](receiver: A => Unit): Endpoint[A] =
		buildEndpointProvider[A](receiver).local

}
