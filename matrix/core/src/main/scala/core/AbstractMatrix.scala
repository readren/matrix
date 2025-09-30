package readren.matrix
package core

import readren.sequencer.{Doer, DoerProvider}
import DoerProvider.Tag
import readren.sequencer.manager.DoerProviderDescriptor

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

	def provideDoer[D <: Doer](tag: DoerProvider.Tag, descriptor: DoerProviderDescriptor[D]): D

	/** Provides a [[Doer]] of the default type. */
	def provideDefaultDoer(tag: DoerProvider.Tag): DefaultDoer

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

	def spawns[U](
		childFactory: ReactantFactory,
		childDoerTag: DoerProvider.Tag
	)(
		initialBehaviorBuilder: ReactantRelay[U, DefaultDoer] => Behavior[U]
	)(
		using isSignalTest: IsSignalTest[U]
	): doer.Duty[ReactantRelay[U, DefaultDoer]] =
		spawns[U, DefaultDoer](childFactory, provideDefaultDoer(childDoerTag))(initialBehaviorBuilder)


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
