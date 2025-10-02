package readren.nexus
package core

import readren.sequencer.Doer
import readren.sequencer.manager.DoerProviderDescriptor

import java.net.URI

/** A facade of [[NexusTyped]] that exposes what the an [[Actant]] needs to know about the [[Nexus]] that contain it.
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
	def createsActant[U, CD <: Doer](
		childFactory: ActantFactory,
		childDoer: CD
	)(
		initialBehaviorBuilder: Actant[U, CD] => Behavior[U]
	)(
		using isSignalTest: IsSignalTest[U]
	): doer.Duty[Actant[U, CD]] = {
		doer.Duty_mineFlat { () =>
			spawner.createsActant[U, CD](childFactory, childDoer, isSignalTest, initialBehaviorBuilder)
		}
	}

	def buildReceptorProviderFor[A](consumer: A => Unit): ReceptorProvider[A] = {
		val receiver = new Inqueue[A] {
			override def submit(message: A): Unit = consumer(message)

			override def uri: _root_.java.net.URI = thisNexus.uri
		}
		// TODO register the receiver in order to be accessible from remote JVMs.
		new ReceptorProvider[A](receiver)
	}

	def buildReceptorFor[A](consumer: A => Unit): Receptor[A] =
		buildReceptorProviderFor[A](consumer).local

}
